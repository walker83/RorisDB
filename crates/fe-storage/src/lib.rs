pub mod catalog;
pub mod information_schema;
pub mod table_provider;

pub use catalog::{ParquetCatalogProvider, ParquetSchemaProvider};
pub use information_schema::InformationSchemaProvider;
pub use table_provider::ParquetTableProvider;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::{Mutex, MutexGuard, RwLock};

use arrow_array::RecordBatch;
use arrow_array::{ArrayRef, new_null_array};
use arrow_schema::{Field, Schema as ArrowSchema};
use thiserror::Error;
use tracing::debug;

/// Guard that holds a per-table write lock.
/// Drops the MutexGuard before the Arc to satisfy borrow checker.
pub struct TableWriteGuard {
    guard: Option<MutexGuard<'static, ()>>,
    _lock: Arc<Mutex<()>>,
}

impl Drop for TableWriteGuard {
    fn drop(&mut self) {
        // Drop guard FIRST (it borrows from _lock's Mutex)
        self.guard.take();
        // Then _lock (Arc) is dropped, potentially freeing the Mutex
    }
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Parquet error: {0}")]
    Parquet(String),
    #[error("Arrow error: {0}")]
    Arrow(String),
    #[error("Table not found: {0}.{1}")]
    TableNotFound(String, String),
    #[error("{0}")]
    Other(String),
}

impl From<String> for StorageError {
    fn from(s: String) -> Self {
        StorageError::Other(s)
    }
}

pub type Result<T> = std::result::Result<T, StorageError>;

/// Lightweight Parquet storage backend.
///
/// Data layout: `{data_dir}/{database}/{table}/data.parquet`
///
/// Write operations (insert/update/delete/truncate) are serialized per-table
/// using internal write locks to prevent concurrent write data loss.
pub struct ParquetStorage {
    data_dir: PathBuf,
    max_rows: Option<u64>,
    /// Per-table write locks: key = "{db}.{table}"
    write_locks: RwLock<HashMap<String, Arc<Mutex<()>>>>,
}

impl ParquetStorage {
    pub fn open(data_dir: impl Into<PathBuf>) -> Result<Self> {
        let data_dir = data_dir.into();
        std::fs::create_dir_all(&data_dir)?;
        debug!("ParquetStorage opened at {}", data_dir.display());
        Ok(Self {
            data_dir,
            max_rows: None,
            write_locks: RwLock::new(HashMap::new()),
        })
    }

    /// Set the maximum row count for DML operations (UPDATE/DELETE/INSERT).
    /// Operations on tables exceeding this limit will be rejected to prevent OOM.
    pub fn with_max_rows(mut self, max_rows: u64) -> Self {
        self.max_rows = Some(max_rows);
        self
    }

    pub fn table_dir(&self, db: &str, table: &str) -> PathBuf {
        self.data_dir.join(db).join(table)
    }

    fn parquet_path(&self, db: &str, table: &str) -> PathBuf {
        self.table_dir(db, table).join("data.parquet")
    }

    pub fn table_exists(&self, db: &str, table: &str) -> bool {
        self.parquet_path(db, table).exists()
    }

    /// Acquire the per-table write lock. Returns a guard that releases on drop.
    fn lock_table(&self, db: &str, table: &str) -> TableWriteGuard {
        let key = format!("{}.{}", db, table);
        // Fast path: read lock to get the Arc, then drop the read lock.
        // Use poisoning-aware access so a panicked writer elsewhere does not
        // cascade into crashing every subsequent write to any table; the
        // connection-level catch_unwind boundary contains the blast radius
        // but the map itself stays usable.
        let lock = {
            let locks = self.write_locks.read().unwrap_or_else(|e| e.into_inner());
            locks.get(&key).cloned()
        };
        let lock = match lock {
            Some(l) => l,
            None => {
                // Slow path: create under write lock
                let mut locks = self.write_locks.write().unwrap_or_else(|e| e.into_inner());
                locks
                    .entry(key)
                    .or_insert_with(|| Arc::new(Mutex::new(())))
                    .clone()
            }
        };
        // Acquire the per-table mutex (blocks if another thread holds it).
        // Safety: we transmute the guard to 'static because we keep the Arc
        // alive in TableWriteGuard._lock, ensuring the Mutex outlives the guard.
        let raw_guard = lock.lock().unwrap_or_else(|e| e.into_inner());
        let static_guard = unsafe {
            std::mem::transmute::<MutexGuard<'_, ()>, MutexGuard<'static, ()>>(raw_guard)
        };
        TableWriteGuard {
            guard: Some(static_guard),
            _lock: lock,
        }
    }

    /// Read row count from Parquet footer metadata without loading data.
    fn parquet_row_count(&self, db: &str, table: &str) -> Result<u64> {
        let path = self.parquet_path(db, table);
        if !path.exists() {
            return Err(StorageError::TableNotFound(
                db.to_string(),
                table.to_string(),
            ));
        }
        let file = std::fs::File::open(&path)?;
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| StorageError::Parquet(e.to_string()))?;
        Ok(builder.metadata().file_metadata().num_rows() as u64)
    }

    /// Check if the table row count exceeds the DML limit. Returns error if exceeded.
    fn check_dml_limit(&self, db: &str, table: &str) -> Result<()> {
        if let Some(max) = self.max_rows {
            let row_count = self.parquet_row_count(db, table)?;
            if row_count > max {
                return Err(StorageError::Other(format!(
                    "Table {}.{} has {} rows, exceeds DML limit of {}. \
                     Operation rejected to prevent excessive memory usage. \
                     Adjust max_dml_rows to increase the limit.",
                    db, table, row_count, max
                )));
            }
        }
        Ok(())
    }

    /// Create a new table: creates directory and writes an empty schema-only Parquet file.
    pub fn create_table(&self, db: &str, table: &str, schema: Arc<ArrowSchema>) -> Result<()> {
        let _guard = self.lock_table(db, table);
        let dir = self.table_dir(db, table);
        std::fs::create_dir_all(&dir)?;

        let path = dir.join("data.parquet");
        // Write an empty RecordBatch to establish schema (idempotent — write lock serializes
        // concurrent creates and write_parquet_atomic is atomic).
        let empty = self::write::empty_batch(&schema);
        self::write::write_parquet_atomic(&path, &empty)?;
        debug!("Created table {}.{}", db, table);
        Ok(())
    }

    /// Drop a table: removes the table directory.
    pub fn drop_table(&self, db: &str, table: &str) -> Result<()> {
        let _guard = self.lock_table(db, table);
        let dir = self.table_dir(db, table);
        if dir.exists() {
            std::fs::remove_dir_all(&dir)?;
        }
        debug!("Dropped table {}.{}", db, table);
        Ok(())
    }

    /// Insert rows: read existing data, concatenate, write back atomically.
    pub fn insert(&self, db: &str, table: &str, new_batch: RecordBatch) -> Result<()> {
        let _guard = self.lock_table(db, table);
        let path = self.parquet_path(db, table);
        if !path.exists() {
            return Err(StorageError::TableNotFound(
                db.to_string(),
                table.to_string(),
            ));
        }
        self.check_dml_limit(db, table)?;

        let num_new_rows = new_batch.num_rows();
        let existing = self::read::read_parquet(&path)?;
        let combined = if existing.num_rows() == 0 {
            new_batch
        } else {
            arrow_select::concat::concat_batches(&existing.schema(), &[existing, new_batch])
                .map_err(|e| StorageError::Arrow(e.to_string()))?
        };

        self::write::write_parquet_atomic(&path, &combined)?;
        debug!("Inserted {} rows into {}.{}", num_new_rows, db, table);
        Ok(())
    }

    /// Read all data from a table as a single RecordBatch.
    pub fn read(&self, db: &str, table: &str) -> Result<RecordBatch> {
        let path = self.parquet_path(db, table);
        if !path.exists() {
            return Err(StorageError::TableNotFound(
                db.to_string(),
                table.to_string(),
            ));
        }
        self::read::read_parquet(&path)
    }

    /// Read with optional projection and limit.
    pub fn read_with_options(
        &self,
        db: &str,
        table: &str,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<RecordBatch> {
        let path = self.parquet_path(db, table);
        if !path.exists() {
            return Err(StorageError::TableNotFound(
                db.to_string(),
                table.to_string(),
            ));
        }
        self::read::read_parquet_with_options(&path, projection, limit)
    }

    /// Update rows: read, apply update function, write back.
    pub fn update<F>(&self, db: &str, table: &str, update_fn: F) -> Result<usize>
    where
        F: FnOnce(RecordBatch) -> Result<(RecordBatch, usize)>,
    {
        let _guard = self.lock_table(db, table);
        let path = self.parquet_path(db, table);
        if !path.exists() {
            return Err(StorageError::TableNotFound(
                db.to_string(),
                table.to_string(),
            ));
        }
        self.check_dml_limit(db, table)?;

        let existing = self::read::read_parquet(&path)?;
        let (updated, count) = update_fn(existing)?;
        self::write::write_parquet_atomic(&path, &updated)?;
        debug!("Updated {} rows in {}.{}", count, db, table);
        Ok(count)
    }

    /// Delete rows: read, filter, write back.
    pub fn delete<F>(&self, db: &str, table: &str, filter_fn: F) -> Result<usize>
    where
        F: FnOnce(RecordBatch) -> Result<(RecordBatch, usize)>,
    {
        let _guard = self.lock_table(db, table);
        let path = self.parquet_path(db, table);
        if !path.exists() {
            return Err(StorageError::TableNotFound(
                db.to_string(),
                table.to_string(),
            ));
        }
        self.check_dml_limit(db, table)?;

        let existing = self::read::read_parquet(&path)?;
        let (kept, deleted_count) = filter_fn(existing)?;
        self::write::write_parquet_atomic(&path, &kept)?;
        debug!("Deleted {} rows from {}.{}", deleted_count, db, table);
        Ok(deleted_count)
    }

    /// Rewrite the Parquet file by dropping a column at `col_index`.
    pub fn rewrite_parquet_drop_column(
        &self,
        db: &str,
        table: &str,
        col_index: usize,
    ) -> Result<()> {
        let _guard = self.lock_table(db, table);
        let path = self.parquet_path(db, table);
        if !path.exists() {
            return Ok(());
        }
        let existing = self::read::read_parquet(&path)?;
        // Don't skip empty tables - schema still needs to be modified
        // Project out the column
        let mut indices: Vec<usize> = (0..existing.num_columns()).collect();
        if col_index >= indices.len() {
            return Err(StorageError::Other(format!(
                "Column index {} out of range (num_columns={})",
                col_index,
                indices.len()
            )));
        }
        indices.remove(col_index);
        let projected = existing
            .project(&indices)
            .map_err(|e| StorageError::Arrow(e.to_string()))?;
        self::write::write_parquet_atomic(&path, &projected)?;
        Ok(())
    }

    /// Rewrite the Parquet file by appending a NULL column for existing rows.
    pub fn rewrite_parquet_add_column(&self, db: &str, table: &str, field: &Field) -> Result<()> {
        let _guard = self.lock_table(db, table);
        let path = self.parquet_path(db, table);
        if !path.exists() {
            return Ok(());
        }
        let existing = self::read::read_parquet(&path)?;
        if existing.num_rows() == 0 {
            return Ok(());
        }
        // Create null array for new column
        let null_array = new_null_array(field.data_type(), existing.num_rows());
        let mut fields: Vec<Field> = existing
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(field.clone());
        let mut columns: Vec<ArrayRef> = existing.columns().to_vec();
        columns.push(null_array);
        let new_batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns)
            .map_err(|e| StorageError::Arrow(e.to_string()))?;
        self::write::write_parquet_atomic(&path, &new_batch)?;
        Ok(())
    }

    /// Truncate a table: atomically replace with an empty Parquet file (write temp + rename).
    pub fn truncate(&self, db: &str, table: &str, schema: Arc<ArrowSchema>) -> Result<()> {
        let _guard = self.lock_table(db, table);
        let path = self.parquet_path(db, table);
        // Write empty batch atomically — no separate delete step to avoid data loss on crash.
        let empty = self::write::empty_batch(&schema);
        self::write::write_parquet_atomic(&path, &empty)?;
        debug!("Truncated table {}.{}", db, table);
        Ok(())
    }
}

mod write {
    use super::*;

    /// Create an empty RecordBatch with the given schema.
    pub fn empty_batch(schema: &Arc<ArrowSchema>) -> RecordBatch {
        let cols: Vec<Arc<dyn arrow_array::Array>> = schema
            .fields()
            .iter()
            .map(|f| arrow_array::new_empty_array(f.data_type()))
            .collect();
        RecordBatch::try_new(schema.clone(), cols)
            .expect("schema should be valid for empty batch (need at least one column)")
    }

    /// Write a RecordBatch to a Parquet file atomically (write temp + rename).
    pub fn write_parquet_atomic(path: &Path, batch: &RecordBatch) -> Result<()> {
        use parquet::arrow::ArrowWriter;
        use parquet::basic::Compression;
        use parquet::file::properties::{EnabledStatistics, WriterProperties};
        use std::sync::atomic::{AtomicU64, Ordering};

        static SEQ: AtomicU64 = AtomicU64::new(0);

        let dir = path.parent().ok_or_else(|| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "path has no parent",
            ))
        })?;

        // Unique temp file: PID + thread-ID + monotonic counter
        let pid = std::process::id();
        let tid = {
            // Use a thread-local counter as a lightweight thread identifier
            std::thread_local! { static TID: u64 = {
                static NEXT: AtomicU64 = AtomicU64::new(0);
                NEXT.fetch_add(1, Ordering::Relaxed)
            }};
            TID.with(|t| *t)
        };
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let temp_path = dir.join(format!(
            ".tmp_{}_{}_{}_{}",
            pid,
            tid,
            seq,
            path.file_name().unwrap_or_default().to_string_lossy()
        ));

        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(parquet::basic::ZstdLevel::default()))
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();

        let file = std::fs::File::create(&temp_path)?;
        let write_result = (|| -> Result<()> {
            let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
                .map_err(|e| StorageError::Parquet(e.to_string()))?;

            writer
                .write(batch)
                .map_err(|e| StorageError::Parquet(e.to_string()))?;
            writer
                .close()
                .map_err(|e| StorageError::Parquet(e.to_string()))?;
            Ok(())
        })();

        if let Err(e) = write_result {
            let _ = std::fs::remove_file(&temp_path);
            return Err(e);
        }

        // fsync before rename for crash safety
        if let Err(e) = std::fs::File::open(&temp_path).and_then(|f| f.sync_all()) {
            let _ = std::fs::remove_file(&temp_path);
            return Err(e.into());
        }
        std::fs::rename(&temp_path, path)?;
        // fsync parent directory for crash safety
        if let Some(parent) = path.parent() {
            let _ = std::fs::File::open(parent).and_then(|f| f.sync_all());
        }
        debug!("Written {} rows to {}", batch.num_rows(), path.display());
        Ok(())
    }
}

mod read {
    use super::*;

    /// Read a Parquet file into a single RecordBatch.
    pub fn read_parquet(path: &Path) -> Result<RecordBatch> {
        read_parquet_with_options(path, None, None)
    }

    /// Read with optional column projection (by index) and row limit.
    pub fn read_parquet_with_options(
        path: &Path,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<RecordBatch> {
        use parquet::arrow::ProjectionMask;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let file = std::fs::File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| StorageError::Parquet(e.to_string()))?;

        let schema = builder.schema().clone();
        let parquet_schema = builder.metadata().file_metadata().schema_descr();

        let builder = if let Some(indices) = projection {
            let mask = ProjectionMask::leaves(parquet_schema, indices.iter().copied());
            builder.with_projection(mask)
        } else {
            builder
        };

        let reader = builder
            .build()
            .map_err(|e| StorageError::Parquet(e.to_string()))?;

        let mut batches = Vec::new();
        let mut total_rows = 0;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| StorageError::Arrow(e.to_string()))?;
            if let Some(limit) = limit {
                if total_rows >= limit {
                    break;
                }
                let remaining = limit - total_rows;
                if batch.num_rows() > remaining {
                    batches.push(batch.slice(0, remaining));
                    break;
                }
            }
            total_rows += batch.num_rows();
            batches.push(batch);
        }

        if batches.is_empty() {
            let cols: Vec<Arc<dyn arrow_array::Array>> = schema
                .fields()
                .iter()
                .map(|f| arrow_array::new_empty_array(f.data_type()))
                .collect();
            return RecordBatch::try_new(schema, cols)
                .map_err(|e| StorageError::Arrow(e.to_string()));
        }

        if batches.len() == 1 {
            return Ok(batches.into_iter().next().unwrap());
        }

        arrow_select::concat::concat_batches(&batches[0].schema(), &batches)
            .map_err(|e| StorageError::Arrow(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    use std::thread;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn test_create_table_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let storage = ParquetStorage::open(dir.path()).unwrap();
        let schema = test_schema();

        // Creating the same table twice should not fail (idempotent)
        storage.create_table("db1", "t1", schema.clone()).unwrap();
        storage.create_table("db1", "t1", schema).unwrap();

        assert!(storage.table_exists("db1", "t1"));
        // Cleanup
        storage.drop_table("db1", "t1").unwrap();
    }

    #[test]
    fn test_drop_table_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let storage = ParquetStorage::open(dir.path()).unwrap();

        // Dropping a non-existent table should not fail
        storage.drop_table("db1", "no_such_table").unwrap();
    }

    #[test]
    fn test_create_and_drop_table_concurrent() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(ParquetStorage::open(dir.path()).unwrap());
        let schema = test_schema();

        // Pre-create the table so both threads race on create + drop
        storage.create_table("db1", "t1", schema.clone()).unwrap();

        let mut handles = vec![];
        for _ in 0..4 {
            let s = Arc::clone(&storage);
            let sch = schema.clone();
            handles.push(thread::spawn(move || {
                // Each thread: drop then re-create
                s.drop_table("db1", "t1").unwrap();
                s.create_table("db1", "t1", sch).unwrap();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // Table should still exist and be readable
        assert!(storage.table_exists("db1", "t1"));
        let rb = storage.read("db1", "t1").unwrap();
        assert_eq!(rb.num_rows(), 0);
        assert_eq!(rb.num_columns(), 2);

        storage.drop_table("db1", "t1").unwrap();
    }

    #[test]
    fn test_insert_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let storage = ParquetStorage::open(dir.path()).unwrap();
        let schema = test_schema();

        storage.create_table("db1", "t1", schema.clone()).unwrap();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        storage.insert("db1", "t1", batch).unwrap();

        let result = storage.read("db1", "t1").unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 2);

        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
        assert_eq!(ids.value(2), 3);

        storage.drop_table("db1", "t1").unwrap();
    }

    #[test]
    fn test_read_nonexistent_table() {
        let dir = tempfile::tempdir().unwrap();
        let storage = ParquetStorage::open(dir.path()).unwrap();

        let result = storage.read("db1", "no_such_table");
        assert!(result.is_err());
        match result.unwrap_err() {
            StorageError::TableNotFound(db, tbl) => {
                assert_eq!(db, "db1");
                assert_eq!(tbl, "no_such_table");
            }
            other => panic!("Expected TableNotFound, got: {:?}", other),
        }
    }

    #[test]
    fn test_insert_concurrent_serialized() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(ParquetStorage::open(dir.path()).unwrap());
        let schema = test_schema();

        storage.create_table("db1", "t1", schema.clone()).unwrap();

        let mut handles = vec![];
        for i in 0..4 {
            let s = Arc::clone(&storage);
            let sch = schema.clone();
            handles.push(thread::spawn(move || {
                let batch = RecordBatch::try_new(
                    sch,
                    vec![
                        Arc::new(Int32Array::from(vec![i])),
                        Arc::new(StringArray::from(vec![format!("row_{}", i)])),
                    ],
                )
                .unwrap();
                s.insert("db1", "t1", batch).unwrap();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let result = storage.read("db1", "t1").unwrap();
        assert_eq!(result.num_rows(), 4);

        storage.drop_table("db1", "t1").unwrap();
    }
}
