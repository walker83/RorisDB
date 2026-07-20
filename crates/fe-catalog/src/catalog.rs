use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock as AsyncRwLock;

use crate::database::Database;
use crate::materialized_view::MaterializedView;
use crate::procedure::StoredProcedure;
use crate::table::Table;
use common::{CatalogError, DharnessError, Result};

/// Configuration for CatalogManager backend selection.
#[derive(Debug, Clone)]
pub struct CatalogConfig {
    /// Path for metadata storage (JSON file or RocksDB directory)
    pub catalog_path: String,
    /// Use RocksDB backend instead of JSON
    pub use_rocks_meta: bool,
    /// Enable dual-write mode (write to both backends during transition)
    pub dual_write: bool,
}

impl Default for CatalogConfig {
    fn default() -> Self {
        Self {
            catalog_path: "data/fe/doris-meta".to_string(),
            use_rocks_meta: false,
            dual_write: false,
        }
    }
}

/// Trait for metadata backend implementations.
/// Allows switching between JSON files and RocksDB storage.
pub trait MetaBackend: Send + Sync {
    /// Store a database
    fn put_database(&self, name: &str, db: &Database) -> Result<()>;

    /// Retrieve a database by name
    fn get_database(&self, name: &str) -> Result<Option<Database>>;

    /// Delete a database by name
    fn delete_database(&self, name: &str) -> Result<()>;

    /// List all database names
    fn list_databases(&self) -> Result<Vec<String>>;

    /// Store a table
    fn put_table(&self, db_name: &str, table_name: &str, table: &Table) -> Result<()>;

    /// Retrieve a table by database and name
    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Option<Table>>;

    /// Delete a table by database and name
    fn delete_table(&self, db_name: &str, table_name: &str) -> Result<()>;

    /// List all table names in a database
    fn list_tables(&self, db_name: &str) -> Result<Vec<String>>;

    /// Get the next unique ID atomically
    fn next_id(&self) -> Result<u64>;

    /// Set the ID counter (for recovery/migration)
    fn set_next_id(&self, value: u64) -> Result<()>;

    /// Get current ID counter value
    fn get_next_id(&self) -> Result<u64>;

    /// Store a materialized view
    fn put_materialized_view(&self, db_name: &str, name: &str, mv: &MaterializedView)
    -> Result<()>;

    /// Get a materialized view
    fn get_materialized_view(&self, db_name: &str, name: &str) -> Result<Option<MaterializedView>>;

    /// Delete a materialized view
    fn delete_materialized_view(&self, db_name: &str, name: &str) -> Result<()>;

    /// List all materialized views in a database
    fn list_materialized_views(&self, db_name: &str) -> Result<Vec<MaterializedView>>;

    /// Store a stored procedure
    fn put_procedure(&self, db_name: &str, name: &str, proc: &StoredProcedure) -> Result<()>;

    /// Get a stored procedure
    fn get_procedure(&self, db_name: &str, name: &str) -> Result<Option<StoredProcedure>>;

    /// Delete a stored procedure
    fn delete_procedure(&self, db_name: &str, name: &str) -> Result<()>;

    /// List all stored procedures in a database
    fn list_procedures(&self, db_name: &str) -> Result<Vec<StoredProcedure>>;

    /// Flush data to persistent storage
    fn flush(&self) -> Result<()>;

    /// Load data from persistent storage
    fn load(&self) -> Result<()>;
}

/// JSON-based metadata backend (legacy).
/// Stores metadata in a single JSON file.
pub struct JsonMetaBackend {
    catalog_path: String,
    databases: DashMap<String, Database>,
    materialized_views: DashMap<String, MaterializedView>,
    procedures: DashMap<String, StoredProcedure>,
    next_id: AtomicU64,
}

impl JsonMetaBackend {
    pub fn new(catalog_path: impl Into<String>) -> Self {
        Self {
            catalog_path: catalog_path.into(),
            databases: DashMap::new(),
            materialized_views: DashMap::new(),
            procedures: DashMap::new(),
            next_id: AtomicU64::new(1),
        }
    }

    fn catalog_file(&self) -> String {
        format!("{}/catalog.json", self.catalog_path)
    }
}

impl MetaBackend for JsonMetaBackend {
    fn put_database(&self, name: &str, db: &Database) -> Result<()> {
        self.databases.insert(name.to_string(), db.clone());
        Ok(())
    }

    fn get_database(&self, name: &str) -> Result<Option<Database>> {
        Ok(self.databases.get(name).map(|r| r.value().clone()))
    }

    fn delete_database(&self, name: &str) -> Result<()> {
        self.databases.remove(name);
        Ok(())
    }

    fn list_databases(&self) -> Result<Vec<String>> {
        Ok(self.databases.iter().map(|r| r.key().clone()).collect())
    }

    fn put_table(&self, db_name: &str, _table_name: &str, table: &Table) -> Result<()> {
        if let Some(mut db) = self.databases.get_mut(db_name) {
            db.add_table(table.clone());
        }
        Ok(())
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Option<Table>> {
        Ok(self
            .databases
            .get(db_name)
            .and_then(|db| db.get_table(table_name).cloned()))
    }

    fn delete_table(&self, db_name: &str, table_name: &str) -> Result<()> {
        if let Some(mut db) = self.databases.get_mut(db_name) {
            db.drop_table(table_name);
        }
        Ok(())
    }

    fn list_tables(&self, db_name: &str) -> Result<Vec<String>> {
        Ok(self
            .databases
            .get(db_name)
            .map(|db| {
                db.table_names()
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect()
            })
            .unwrap_or_default())
    }

    fn next_id(&self) -> Result<u64> {
        Ok(self.next_id.fetch_add(1, Ordering::Relaxed))
    }

    fn set_next_id(&self, value: u64) -> Result<()> {
        self.next_id.store(value, Ordering::SeqCst);
        Ok(())
    }

    fn get_next_id(&self) -> Result<u64> {
        Ok(self.next_id.load(Ordering::SeqCst))
    }

    fn put_materialized_view(
        &self,
        db_name: &str,
        name: &str,
        mv: &MaterializedView,
    ) -> Result<()> {
        let key = format!("{}.{}", db_name, name);
        self.materialized_views.insert(key, mv.clone());
        Ok(())
    }

    fn get_materialized_view(&self, db_name: &str, name: &str) -> Result<Option<MaterializedView>> {
        let key = format!("{}.{}", db_name, name);
        Ok(self.materialized_views.get(&key).map(|r| r.value().clone()))
    }

    fn delete_materialized_view(&self, db_name: &str, name: &str) -> Result<()> {
        let key = format!("{}.{}", db_name, name);
        self.materialized_views.remove(&key);
        Ok(())
    }

    fn list_materialized_views(&self, db_name: &str) -> Result<Vec<MaterializedView>> {
        let prefix = format!("{}.", db_name);
        Ok(self
            .materialized_views
            .iter()
            .filter(|r| r.key().starts_with(&prefix))
            .map(|r| r.value().clone())
            .collect())
    }

    fn put_procedure(&self, db_name: &str, name: &str, proc: &StoredProcedure) -> Result<()> {
        let key = format!("{}.{}", db_name, name);
        self.procedures.insert(key, proc.clone());
        Ok(())
    }

    fn get_procedure(&self, db_name: &str, name: &str) -> Result<Option<StoredProcedure>> {
        let key = format!("{}.{}", db_name, name);
        Ok(self.procedures.get(&key).map(|r| r.value().clone()))
    }

    fn delete_procedure(&self, db_name: &str, name: &str) -> Result<()> {
        let key = format!("{}.{}", db_name, name);
        self.procedures.remove(&key);
        Ok(())
    }

    fn list_procedures(&self, db_name: &str) -> Result<Vec<StoredProcedure>> {
        let prefix = format!("{}.", db_name);
        Ok(self
            .procedures
            .iter()
            .filter(|r| r.key().starts_with(&prefix))
            .map(|r| r.value().clone())
            .collect())
    }

    fn flush(&self) -> Result<()> {
        Ok(())
    }

    fn load(&self) -> Result<()> {
        use std::fs;

        let path = self.catalog_file();
        if !std::path::Path::new(&path).exists() {
            return Ok(());
        }
        let contents = fs::read_to_string(&path)?;
        let state: CatalogState =
            serde_json::from_str(&contents).map_err(|e| DharnessError::Internal(e.to_string()))?;
        for (key, value) in state.databases {
            self.databases.insert(key, value);
        }
        for (key, value) in state.materialized_views {
            self.materialized_views.insert(key, value);
        }
        for (key, value) in state.procedures {
            self.procedures.insert(key, value);
        }
        self.next_id.store(state.next_id, Ordering::SeqCst);
        Ok(())
    }
}

impl JsonMetaBackend {
    /// Save catalog state to JSON file
    pub fn save(&self) -> Result<()> {
        use std::fs;

        let catalog_state = CatalogState {
            databases: self
                .databases
                .iter()
                .map(|r| (r.key().clone(), r.value().clone()))
                .collect(),
            materialized_views: self
                .materialized_views
                .iter()
                .map(|r| (r.key().clone(), r.value().clone()))
                .collect(),
            procedures: self
                .procedures
                .iter()
                .map(|r| (r.key().clone(), r.value().clone()))
                .collect(),
            next_id: self.next_id.load(Ordering::Relaxed),
        };
        let json = serde_json::to_string(&catalog_state)
            .map_err(|e| DharnessError::Internal(e.to_string()))?;
        let path = self.catalog_file();
        fs::create_dir_all(&self.catalog_path)?;
        fs::write(&path, json.as_bytes())?;
        Ok(())
    }

    /// Load catalog state from JSON file
    pub fn load(&self) -> Result<()> {
        use std::fs;

        let path = self.catalog_file();
        if !std::path::Path::new(&path).exists() {
            return Ok(());
        }
        let contents = fs::read_to_string(&path)?;
        let state: CatalogState =
            serde_json::from_str(&contents).map_err(|e| DharnessError::Internal(e.to_string()))?;
        for (key, value) in state.databases {
            self.databases.insert(key, value);
        }
        for (key, value) in state.materialized_views {
            self.materialized_views.insert(key, value);
        }
        for (key, value) in state.procedures {
            self.procedures.insert(key, value);
        }
        self.next_id.store(state.next_id, Ordering::SeqCst);
        Ok(())
    }
}

/// redb-backed metadata backend.
/// Uses be-kv::CatalogStore for persistent storage.
pub struct KvMetaBackend {
    catalog_store: be_kv::CatalogStore,
}

impl KvMetaBackend {
    pub fn new(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let meta_store = be_kv::MetaStore::open(path)
            .map_err(|e| DharnessError::Internal(format!("Failed to open KV store: {}", e)))?;
        let catalog_store = be_kv::CatalogStore::new(Arc::new(meta_store));
        Ok(Self { catalog_store })
    }
}

impl MetaBackend for KvMetaBackend {
    fn put_database(&self, name: &str, db: &Database) -> Result<()> {
        // Serialize fe_catalog::Database to JSON bytes and store raw
        let data = serde_json::to_vec(db)
            .map_err(|e| DharnessError::Internal(format!("Serialization error: {}", e)))?;
        self.catalog_store
            .put_database_raw(name, &data)
            .map_err(|e| DharnessError::Internal(format!("KV store error: {}", e)))
    }

    fn get_database(&self, name: &str) -> Result<Option<Database>> {
        // Get raw bytes and deserialize into fe_catalog::Database
        let data = self
            .catalog_store
            .get_database_raw(name)
            .map_err(|e| DharnessError::Internal(format!("KV store error: {}", e)))?;
        data.map(|d| serde_json::from_slice(&d))
            .transpose()
            .map_err(|e| DharnessError::Internal(format!("Deserialization error: {}", e)))
    }

    fn delete_database(&self, name: &str) -> Result<()> {
        self.catalog_store
            .delete_database(name)
            .map_err(|e| DharnessError::Internal(format!("KV store error: {}", e)))
    }

    fn list_databases(&self) -> Result<Vec<String>> {
        self.catalog_store
            .list_databases()
            .map_err(|e| DharnessError::Internal(format!("KV store error: {}", e)))
    }

    fn put_table(&self, db_name: &str, table_name: &str, table: &Table) -> Result<()> {
        // Serialize fe_catalog::Table to JSON bytes and store raw
        let data = serde_json::to_vec(table)
            .map_err(|e| DharnessError::Internal(format!("Serialization error: {}", e)))?;
        self.catalog_store
            .put_table_raw(db_name, table_name, &data)
            .map_err(|e| DharnessError::Internal(format!("KV store error: {}", e)))
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Option<Table>> {
        // Get raw bytes and deserialize into fe_catalog::Table
        let data = self
            .catalog_store
            .get_table_raw(db_name, table_name)
            .map_err(|e| DharnessError::Internal(format!("KV store error: {}", e)))?;
        data.map(|d| serde_json::from_slice(&d))
            .transpose()
            .map_err(|e| DharnessError::Internal(format!("Deserialization error: {}", e)))
    }

    fn delete_table(&self, db_name: &str, table_name: &str) -> Result<()> {
        self.catalog_store
            .delete_table(db_name, table_name)
            .map_err(|e| DharnessError::Internal(format!("KV store error: {}", e)))
    }

    fn list_tables(&self, db_name: &str) -> Result<Vec<String>> {
        self.catalog_store
            .list_tables(db_name)
            .map_err(|e| DharnessError::Internal(format!("KV store error: {}", e)))
    }

    fn next_id(&self) -> Result<u64> {
        self.catalog_store
            .next_id()
            .map_err(|e| DharnessError::Internal(format!("KV store error: {}", e)))
    }

    fn set_next_id(&self, value: u64) -> Result<()> {
        self.catalog_store
            .set_next_id(value)
            .map_err(|e| DharnessError::Internal(format!("KV store error: {}", e)))
    }

    fn get_next_id(&self) -> Result<u64> {
        self.catalog_store
            .get_next_id()
            .map_err(|e| DharnessError::Internal(format!("KV store error: {}", e)))
    }

    fn put_materialized_view(
        &self,
        db_name: &str,
        name: &str,
        mv: &MaterializedView,
    ) -> Result<()> {
        // Store materialized views with a special prefix
        let key = format!("mv:{}.{}", db_name, name);
        let value = serde_json::to_vec(mv)
            .map_err(|e| DharnessError::Internal(format!("Serialization error: {}", e)))?;
        self.catalog_store
            .put_raw(&key, &value)
            .map_err(|e| DharnessError::Internal(format!("KV store error: {}", e)))
    }

    fn get_materialized_view(&self, db_name: &str, name: &str) -> Result<Option<MaterializedView>> {
        let key = format!("mv:{}.{}", db_name, name);
        let data = self
            .catalog_store
            .get_raw(&key)
            .map_err(|e| DharnessError::Internal(format!("KV store error: {}", e)))?;
        data.map(|d| serde_json::from_slice(&d))
            .transpose()
            .map_err(|e| DharnessError::Internal(format!("Deserialization error: {}", e)))
    }

    fn delete_materialized_view(&self, db_name: &str, name: &str) -> Result<()> {
        let key = format!("mv:{}.{}", db_name, name);
        self.catalog_store
            .delete_raw(&key)
            .map_err(|e| DharnessError::Internal(format!("KV store error: {}", e)))
    }

    fn list_materialized_views(&self, db_name: &str) -> Result<Vec<MaterializedView>> {
        let prefix = format!("mv:{}.", db_name);
        let keys = self
            .catalog_store
            .list_keys_with_prefix_str(&prefix)
            .map_err(|e| DharnessError::Internal(format!("KV store error: {}", e)))?;
        let mut mvs = Vec::new();
        for key in keys {
            if let Some(data) = self
                .catalog_store
                .get_raw(&key)
                .map_err(|e| DharnessError::Internal(format!("KV store error: {}", e)))?
            {
                if let Ok(mv) = serde_json::from_slice::<MaterializedView>(&data) {
                    mvs.push(mv);
                }
            }
        }
        Ok(mvs)
    }

    fn put_procedure(
        &self,
        db_name: &str,
        name: &str,
        proc: &StoredProcedure,
    ) -> Result<()> {
        let key = format!("proc:{}.{}", db_name, name);
        let value = serde_json::to_vec(proc)
            .map_err(|e| DharnessError::Internal(format!("Serialization error: {}", e)))?;
        self.catalog_store
            .put_raw(&key, &value)
            .map_err(|e| DharnessError::Internal(format!("KV store error: {}", e)))
    }

    fn get_procedure(&self, db_name: &str, name: &str) -> Result<Option<StoredProcedure>> {
        let key = format!("proc:{}.{}", db_name, name);
        let data = self
            .catalog_store
            .get_raw(&key)
            .map_err(|e| DharnessError::Internal(format!("KV store error: {}", e)))?;
        data.map(|d| serde_json::from_slice(&d))
            .transpose()
            .map_err(|e| DharnessError::Internal(format!("Deserialization error: {}", e)))
    }

    fn delete_procedure(&self, db_name: &str, name: &str) -> Result<()> {
        let key = format!("proc:{}.{}", db_name, name);
        self.catalog_store
            .delete_raw(&key)
            .map_err(|e| DharnessError::Internal(format!("KV store error: {}", e)))
    }

    fn list_procedures(&self, db_name: &str) -> Result<Vec<StoredProcedure>> {
        let prefix = format!("proc:{}.", db_name);
        let keys = self
            .catalog_store
            .list_keys_with_prefix_str(&prefix)
            .map_err(|e| DharnessError::Internal(format!("KV store error: {}", e)))?;
        let mut procs = Vec::new();
        for key in keys {
            if let Some(data) = self
                .catalog_store
                .get_raw(&key)
                .map_err(|e| DharnessError::Internal(format!("KV store error: {}", e)))?
            {
                if let Ok(proc) = serde_json::from_slice::<StoredProcedure>(&data) {
                    procs.push(proc);
                }
            }
        }
        Ok(procs)
    }

    fn flush(&self) -> Result<()> {
        self.catalog_store
            .flush()
            .map_err(|e| DharnessError::Internal(format!("KV store error: {}", e)))
    }

    fn load(&self) -> Result<()> {
        // KV store data is already persisted, no explicit load needed
        Ok(())
    }
}

/// Dual-write backend that writes to both backends simultaneously.
/// Used during transition from JSON to RocksDB.
pub struct DualWriteBackend {
    primary: Arc<dyn MetaBackend>,
    secondary: Arc<dyn MetaBackend>,
}

impl DualWriteBackend {
    pub fn new(primary: Arc<dyn MetaBackend>, secondary: Arc<dyn MetaBackend>) -> Self {
        Self { primary, secondary }
    }
}

impl MetaBackend for DualWriteBackend {
    fn put_database(&self, name: &str, db: &Database) -> Result<()> {
        self.primary.put_database(name, db)?;
        if let Err(e) = self.secondary.put_database(name, db) {
            tracing::warn!("dual-write: secondary put_database({name}) failed: {e}");
        }
        Ok(())
    }

    fn get_database(&self, name: &str) -> Result<Option<Database>> {
        self.primary.get_database(name)
    }

    fn delete_database(&self, name: &str) -> Result<()> {
        self.primary.delete_database(name)?;
        if let Err(e) = self.secondary.delete_database(name) {
            tracing::warn!("dual-write: secondary delete_database({name}) failed: {e}");
        }
        Ok(())
    }

    fn list_databases(&self) -> Result<Vec<String>> {
        self.primary.list_databases()
    }

    fn put_table(&self, db_name: &str, table_name: &str, table: &Table) -> Result<()> {
        self.primary.put_table(db_name, table_name, table)?;
        if let Err(e) = self.secondary.put_table(db_name, table_name, table) {
            tracing::warn!("dual-write: secondary put_table({db_name}.{table_name}) failed: {e}");
        }
        Ok(())
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Option<Table>> {
        self.primary.get_table(db_name, table_name)
    }

    fn delete_table(&self, db_name: &str, table_name: &str) -> Result<()> {
        self.primary.delete_table(db_name, table_name)?;
        if let Err(e) = self.secondary.delete_table(db_name, table_name) {
            tracing::warn!(
                "dual-write: secondary delete_table({db_name}.{table_name}) failed: {e}"
            );
        }
        Ok(())
    }

    fn list_tables(&self, db_name: &str) -> Result<Vec<String>> {
        self.primary.list_tables(db_name)
    }

    fn next_id(&self) -> Result<u64> {
        let id = self.primary.next_id()?;
        if let Err(e) = self.secondary.set_next_id(id + 1) {
            tracing::warn!("dual-write: secondary set_next_id({}) failed: {e}", id + 1);
        }
        Ok(id)
    }

    fn set_next_id(&self, value: u64) -> Result<()> {
        self.primary.set_next_id(value)?;
        if let Err(e) = self.secondary.set_next_id(value) {
            tracing::warn!("dual-write: secondary set_next_id({value}) failed: {e}");
        }
        Ok(())
    }

    fn get_next_id(&self) -> Result<u64> {
        self.primary.get_next_id()
    }

    fn put_materialized_view(
        &self,
        db_name: &str,
        name: &str,
        mv: &MaterializedView,
    ) -> Result<()> {
        self.primary.put_materialized_view(db_name, name, mv)?;
        if let Err(e) = self.secondary.put_materialized_view(db_name, name, mv) {
            tracing::warn!(
                "dual-write: secondary put_materialized_view({db_name}.{name}) failed: {e}"
            );
        }
        Ok(())
    }

    fn get_materialized_view(&self, db_name: &str, name: &str) -> Result<Option<MaterializedView>> {
        self.primary.get_materialized_view(db_name, name)
    }

    fn delete_materialized_view(&self, db_name: &str, name: &str) -> Result<()> {
        self.primary.delete_materialized_view(db_name, name)?;
        if let Err(e) = self.secondary.delete_materialized_view(db_name, name) {
            tracing::warn!(
                "dual-write: secondary delete_materialized_view({db_name}.{name}) failed: {e}"
            );
        }
        Ok(())
    }

    fn list_materialized_views(&self, db_name: &str) -> Result<Vec<MaterializedView>> {
        self.primary.list_materialized_views(db_name)
    }

    fn put_procedure(
        &self,
        db_name: &str,
        name: &str,
        proc: &StoredProcedure,
    ) -> Result<()> {
        self.primary.put_procedure(db_name, name, proc)?;
        if let Err(e) = self.secondary.put_procedure(db_name, name, proc) {
            tracing::warn!(
                "dual-write: secondary put_procedure({db_name}.{name}) failed: {e}"
            );
        }
        Ok(())
    }

    fn get_procedure(&self, db_name: &str, name: &str) -> Result<Option<StoredProcedure>> {
        self.primary.get_procedure(db_name, name)
    }

    fn delete_procedure(&self, db_name: &str, name: &str) -> Result<()> {
        self.primary.delete_procedure(db_name, name)?;
        if let Err(e) = self.secondary.delete_procedure(db_name, name) {
            tracing::warn!(
                "dual-write: secondary delete_procedure({db_name}.{name}) failed: {e}"
            );
        }
        Ok(())
    }

    fn list_procedures(&self, db_name: &str) -> Result<Vec<StoredProcedure>> {
        self.primary.list_procedures(db_name)
    }

    fn flush(&self) -> Result<()> {
        self.primary.flush()?;
        if let Err(e) = self.secondary.flush() {
            tracing::warn!("dual-write: secondary flush failed: {e}");
        }
        Ok(())
    }

    fn load(&self) -> Result<()> {
        self.primary.load()?;
        if let Err(e) = self.secondary.load() {
            tracing::warn!("dual-write: secondary load failed: {e}");
        }
        Ok(())
    }
}

/// Catalog manager with pluggable backend.
/// Supports JSON files, RocksDB, and dual-write mode.
pub struct CatalogManager {
    databases: DashMap<String, Database>,
    materialized_views: DashMap<String, MaterializedView>,
    procedures: DashMap<String, StoredProcedure>,
    next_id: AtomicU64,
    catalog_path: String,
    backend: Arc<dyn MetaBackend>,
    backend_type_name: &'static str,
    /// Optional secondary backend for dual-write mode
    #[allow(dead_code)]
    secondary_backend: Option<Arc<dyn MetaBackend>>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
enum CatalogOp {
    CreateDatabase(String),
    DropDatabase(String),
    CreateTable { db: String, table: Table },
    DropTable { db: String, table: String },
    AlterDatabase { name: String },
    AlterTable { db: String, table: String },
}

pub struct CatalogWriter {
    catalog: Arc<AsyncRwLock<CatalogManager>>,
    edit_log: Arc<AsyncRwLock<fe_common::edit_log::EditLog>>,
}

impl CatalogManager {
    /// Create a new CatalogManager with default JSON backend.
    pub fn new() -> Self {
        Self::with_config(CatalogConfig::default())
    }

    /// Create a CatalogManager with a specific path (JSON backend).
    pub fn with_path(path: impl Into<String>) -> Self {
        Self::with_config(CatalogConfig {
            catalog_path: path.into(),
            use_rocks_meta: false,
            dual_write: false,
        })
    }

    /// Create a CatalogManager with the specified configuration.
    pub fn with_config(config: CatalogConfig) -> Self {
        let (backend, backend_type_name): (Arc<dyn MetaBackend>, &'static str) =
            if config.use_rocks_meta {
                let kv_path = format!("{}/kvstore", config.catalog_path);
                (
                    Arc::new(
                        KvMetaBackend::new(&kv_path)
                            .expect("Failed to initialize KV store backend"),
                    ),
                    "kvstore",
                )
            } else {
                (Arc::new(JsonMetaBackend::new(&config.catalog_path)), "json")
            };

        let dbs = DashMap::new();
        dbs.insert(
            "information_schema".into(),
            Database::new(0, "information_schema"),
        );

        Self {
            databases: dbs,
            materialized_views: DashMap::new(),
            procedures: DashMap::new(),
            next_id: AtomicU64::new(1),
            catalog_path: config.catalog_path,
            backend,
            backend_type_name,
            secondary_backend: None,
        }
    }

    /// Create a CatalogManager with dual-write mode.
    /// Writes go to both JSON and KV (redb) backends.
    pub fn with_dual_write(path: impl Into<String>) -> Self {
        let path = path.into();
        let json_backend = Arc::new(JsonMetaBackend::new(&path));
        let kv_path = format!("{}/kvstore", path);
        let kv_backend = Arc::new(
            KvMetaBackend::new(&kv_path).expect("Failed to initialize KV store backend"),
        );

        let dual_backend = Arc::new(DualWriteBackend::new(
            json_backend.clone(),
            kv_backend.clone(),
        ));

        let dbs = DashMap::new();
        dbs.insert(
            "information_schema".into(),
            Database::new(0, "information_schema"),
        );

        Self {
            databases: dbs,
            materialized_views: DashMap::new(),
            procedures: DashMap::new(),
            next_id: AtomicU64::new(1),
            catalog_path: path,
            backend: dual_backend,
            backend_type_name: "dual-write",
            secondary_backend: None,
        }
    }

    /// Get the backend type name for logging/debugging.
    pub fn backend_type(&self) -> &'static str {
        self.backend_type_name
    }

    pub fn create_database(&self, name: &str) -> Result<()> {
        if self.databases.contains_key(name) {
            return Err(DharnessError::catalog(
                CatalogError::DatabaseAlreadyExists,
                format!("database '{}' already exists", name),
            ));
        }
        // Use create_dir_all (idempotent) to avoid TOCTOU race between checking
        // and creating the metadata directory.
        std::fs::create_dir_all(&self.catalog_path).map_err(|e| {
            DharnessError::Internal(format!("Failed to create catalog directory: {}", e))
        })?;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let db = Database::new(id, name);
        self.backend.put_database(name, &db)?;
        self.databases.insert(name.to_string(), db);
        Ok(())
    }

    pub fn drop_database(&self, name: &str) -> Result<()> {
        self.databases.remove(name).ok_or_else(|| {
            DharnessError::catalog(
                CatalogError::DatabaseNotFound,
                format!("database '{}' not found", name),
            )
        })?;
        self.backend.delete_database(name)?;
        Ok(())
    }

    pub fn list_databases(&self) -> Vec<String> {
        self.databases.iter().map(|r| r.key().clone()).collect()
    }

    pub fn get_database(&self, name: &str) -> Option<Database> {
        self.databases.get(name).map(|r| r.value().clone())
    }

    pub fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn create_table(&self, db_name: &str, table: Table) -> Result<()> {
        let mut db_ref = self.databases.get_mut(db_name).ok_or_else(|| {
            DharnessError::catalog(
                CatalogError::DatabaseNotFound,
                format!("database '{}' not found", db_name),
            )
        })?;
        // Check for duplicate to prevent silent overwrite
        if db_ref.get_table(&table.name).is_some() {
            return Err(DharnessError::catalog(
                CatalogError::TableAlreadyExists,
                format!("table '{}.{}' already exists", db_name, table.name),
            ));
        }
        self.backend.put_table(db_name, &table.name, &table)?;
        db_ref.add_table(table);
        Ok(())
    }

    /// Replaces an existing table in the catalog (used by ALTER TABLE).
    pub fn update_table(&self, db_name: &str, table: Table) -> Result<()> {
        let mut db_ref = self.databases.get_mut(db_name).ok_or_else(|| {
            DharnessError::catalog(
                CatalogError::DatabaseNotFound,
                format!("database '{}' not found", db_name),
            )
        })?;
        self.backend.put_table(db_name, &table.name, &table)?;
        db_ref.drop_table(&table.name);
        db_ref.add_table(table);
        Ok(())
    }

    pub fn drop_table(&self, db_name: &str, table_name: &str) -> Result<()> {
        let mut db_ref = self.databases.get_mut(db_name).ok_or_else(|| {
            DharnessError::catalog(
                CatalogError::DatabaseNotFound,
                format!("database '{}' not found", db_name),
            )
        })?;
        db_ref.drop_table(table_name).ok_or_else(|| {
            DharnessError::catalog(
                CatalogError::TableNotFound,
                format!("table '{}' not found", table_name),
            )
        })?;
        self.backend.delete_table(db_name, table_name)?;
        Ok(())
    }

    pub fn get_table(&self, db_name: &str, table_name: &str) -> Option<Table> {
        self.databases
            .get(db_name)
            .and_then(|db| db.get_table(table_name).cloned())
    }

    pub fn list_tables(&self, db_name: &str) -> Option<Vec<String>> {
        self.databases.get(db_name).map(|db| {
            db.table_names()
                .into_iter()
                .map(|s| s.to_string())
                .collect()
        })
    }

    pub fn create_materialized_view(&self, mv: MaterializedView) -> common::Result<()> {
        let key = format!("{}.{}", mv.database, mv.name);
        self.backend
            .put_materialized_view(&mv.database, &mv.name, &mv)?;
        self.materialized_views.insert(key, mv);
        Ok(())
    }

    pub fn drop_materialized_view(&self, db_name: &str, name: &str) -> common::Result<()> {
        let key = format!("{}.{}", db_name, name);
        self.materialized_views.remove(&key).ok_or_else(|| {
            DharnessError::catalog(
                CatalogError::TableNotFound,
                format!("materialized view '{}' not found", name),
            )
        })?;
        self.backend.delete_materialized_view(db_name, name)?;
        Ok(())
    }

    pub fn get_materialized_view(&self, db_name: &str, name: &str) -> Option<MaterializedView> {
        let key = format!("{}.{}", db_name, name);
        self.materialized_views.get(&key).map(|r| r.value().clone())
    }

    pub fn list_materialized_views(&self, db_name: &str) -> Vec<MaterializedView> {
        let prefix = format!("{}.", db_name);
        self.materialized_views
            .iter()
            .filter(|r| r.key().starts_with(&prefix))
            .map(|r| r.value().clone())
            .collect()
    }

    pub fn all_materialized_views(&self) -> Vec<MaterializedView> {
        self.materialized_views
            .iter()
            .map(|r| r.value().clone())
            .collect()
    }

    pub fn create_procedure(&self, db_name: &str, proc: StoredProcedure) -> Result<()> {
        let key = format!("{}.{}", db_name, proc.name);
        self.backend.put_procedure(db_name, &proc.name, &proc)?;
        self.procedures.insert(key, proc);
        Ok(())
    }

    pub fn drop_procedure(&self, db_name: &str, name: &str) -> Result<()> {
        let key = format!("{}.{}", db_name, name);
        self.procedures.remove(&key);
        self.backend.delete_procedure(db_name, name)
    }

    pub fn get_procedure(&self, db_name: &str, name: &str) -> Result<Option<StoredProcedure>> {
        let key = format!("{}.{}", db_name, name);
        if let Some(p) = self.procedures.get(&key) {
            return Ok(Some(p.value().clone()));
        }
        self.backend.get_procedure(db_name, name)
    }

    pub fn list_procedures(&self, db_name: &str) -> Result<Vec<StoredProcedure>> {
        self.backend.list_procedures(db_name)
    }

    /// Serialize catalog state to JSON file (for backward compatibility)
    pub fn save(&self) -> common::Result<()> {
        use std::fs;

        let catalog_state = CatalogState {
            databases: self
                .databases
                .iter()
                .map(|r| (r.key().clone(), r.value().clone()))
                .collect(),
            materialized_views: self
                .materialized_views
                .iter()
                .map(|r| (r.key().clone(), r.value().clone()))
                .collect(),
            procedures: self
                .procedures
                .iter()
                .map(|r| (r.key().clone(), r.value().clone()))
                .collect(),
            next_id: self.next_id.load(Ordering::Relaxed),
        };
        let json = serde_json::to_string(&catalog_state)
            .map_err(|e| DharnessError::Internal(e.to_string()))?;
        let path = format!("{}/catalog.json", self.catalog_path);
        fs::create_dir_all(&self.catalog_path)?;
        fs::write(&path, json.as_bytes())?;

        // Also flush the backend
        self.backend.flush()?;
        Ok(())
    }

    /// Load catalog state from backend
    pub fn load(&self) -> common::Result<()> {
        // First, load the backend's internal state (e.g., from JSON file for JsonMetaBackend)
        self.backend.load()?;

        // Then populate CatalogManager's DashMaps from backend
        let databases = self.backend.list_databases()?;
        for db_name in databases {
            if let Some(db) = self.backend.get_database(&db_name)? {
                self.databases.insert(db_name, db);
            }
        }

        // Load tables for each database - collect updates first to avoid DashMap deadlock
        let updates: Vec<(String, Database)> = self
            .databases
            .iter()
            .filter_map(|entry| {
                let db_name = entry.key();
                let db = entry.value();
                if let Ok(tables) = self.backend.list_tables(db_name) {
                    let mut updated_db = db.clone();
                    for table_name in tables {
                        if let Ok(Some(table)) = self.backend.get_table(db_name, &table_name) {
                            updated_db.add_table(table);
                        }
                    }
                    Some((db_name.clone(), updated_db))
                } else {
                    None
                }
            })
            .collect();

        // Apply updates after iteration completes
        for (db_name, db) in updates {
            self.databases.insert(db_name, db);
        }

        // Load procedures
        for db_name in self.databases.iter().map(|r| r.key().clone()).collect::<Vec<_>>() {
            if let Ok(procs) = self.backend.list_procedures(&db_name) {
                for proc in procs {
                    let key = format!("{}.{}", db_name, proc.name);
                    self.procedures.insert(key, proc);
                }
            }
        }

        // Load next_id from backend
        if let Ok(id) = self.backend.get_next_id() {
            self.next_id.store(id, Ordering::SeqCst);
        }

        Ok(())
    }

    /// Replay edit log entries into the catalog
    pub fn replay_edit_log(&self, log: &fe_common::edit_log::EditLog) -> common::Result<()> {
        use fe_common::edit_log::OpType;

        for entry in log.entries() {
            match entry.op_type {
                OpType::CreateDatabase => {
                    if let Ok(op) = serde_json::from_slice::<CatalogOp>(&entry.data)
                        && let CatalogOp::CreateDatabase(name) = op
                    {
                        self.create_database(&name)?;
                    }
                }
                OpType::DropDatabase => {
                    if let Ok(op) = serde_json::from_slice::<CatalogOp>(&entry.data)
                        && let CatalogOp::DropDatabase(name) = op
                    {
                        self.drop_database(&name)?;
                    }
                }
                OpType::CreateTable => {
                    if let Ok(op) = serde_json::from_slice::<CatalogOp>(&entry.data)
                        && let CatalogOp::CreateTable { db, table } = op
                    {
                        self.create_table(&db, table)?;
                    }
                }
                OpType::DropTable => {
                    if let Ok(op) = serde_json::from_slice::<CatalogOp>(&entry.data)
                        && let CatalogOp::DropTable { db, table } = op
                    {
                        self.drop_table(&db, &table)?;
                    }
                }
                _ => { /* ignore other op types for now */ }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct CatalogState {
    databases: HashMap<String, Database>,
    materialized_views: HashMap<String, MaterializedView>,
    #[serde(default)]
    procedures: HashMap<String, StoredProcedure>,
    next_id: u64,
}

impl Default for CatalogManager {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogWriter {
    pub fn new(
        catalog: Arc<AsyncRwLock<CatalogManager>>,
        edit_log: Arc<AsyncRwLock<fe_common::edit_log::EditLog>>,
    ) -> Self {
        Self { catalog, edit_log }
    }

    pub async fn create_database(&self, name: &str) -> common::Result<()> {
        let op = CatalogOp::CreateDatabase(name.to_string());
        let data = serde_json::to_vec(&op).map_err(|e| DharnessError::Internal(e.to_string()))?;
        let _index = self
            .edit_log
            .write()
            .await
            .append(fe_common::edit_log::OpType::CreateDatabase, data);
        self.catalog.write().await.create_database(name)?;
        Ok(())
    }

    pub async fn drop_database(&self, name: &str) -> common::Result<()> {
        let op = CatalogOp::DropDatabase(name.to_string());
        let data = serde_json::to_vec(&op).map_err(|e| DharnessError::Internal(e.to_string()))?;
        let _index = self
            .edit_log
            .write()
            .await
            .append(fe_common::edit_log::OpType::DropDatabase, data);
        self.catalog.write().await.drop_database(name)?;
        Ok(())
    }

    pub async fn create_table(&self, db_name: &str, table: Table) -> common::Result<()> {
        let op = CatalogOp::CreateTable {
            db: db_name.to_string(),
            table: table.clone(),
        };
        let data = serde_json::to_vec(&op).map_err(|e| DharnessError::Internal(e.to_string()))?;
        let _index = self
            .edit_log
            .write()
            .await
            .append(fe_common::edit_log::OpType::CreateTable, data);
        self.catalog.write().await.create_table(db_name, table)?;
        Ok(())
    }

    pub async fn drop_table(&self, db_name: &str, table_name: &str) -> common::Result<()> {
        let op = CatalogOp::DropTable {
            db: db_name.to_string(),
            table: db_name.to_string(),
        };
        let data = serde_json::to_vec(&op).map_err(|e| DharnessError::Internal(e.to_string()))?;
        let _index = self
            .edit_log
            .write()
            .await
            .append(fe_common::edit_log::OpType::DropTable, data);
        self.catalog.write().await.drop_table(db_name, table_name)?;
        Ok(())
    }
}

/// Helper trait for downcasting MetaBackend implementations
#[allow(dead_code)]
trait AsAny {
    fn as_any(&self) -> &dyn std::any::Any;
}

impl<T: 'static> AsAny for T {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl MetaBackend for Arc<dyn MetaBackend> {
    fn put_database(&self, name: &str, db: &Database) -> Result<()> {
        (**self).put_database(name, db)
    }
    fn get_database(&self, name: &str) -> Result<Option<Database>> {
        (**self).get_database(name)
    }
    fn delete_database(&self, name: &str) -> Result<()> {
        (**self).delete_database(name)
    }
    fn list_databases(&self) -> Result<Vec<String>> {
        (**self).list_databases()
    }
    fn put_table(&self, db_name: &str, table_name: &str, table: &Table) -> Result<()> {
        (**self).put_table(db_name, table_name, table)
    }
    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Option<Table>> {
        (**self).get_table(db_name, table_name)
    }
    fn delete_table(&self, db_name: &str, table_name: &str) -> Result<()> {
        (**self).delete_table(db_name, table_name)
    }
    fn list_tables(&self, db_name: &str) -> Result<Vec<String>> {
        (**self).list_tables(db_name)
    }
    fn next_id(&self) -> Result<u64> {
        (**self).next_id()
    }
    fn set_next_id(&self, value: u64) -> Result<()> {
        (**self).set_next_id(value)
    }
    fn get_next_id(&self) -> Result<u64> {
        (**self).get_next_id()
    }
    fn put_materialized_view(
        &self,
        db_name: &str,
        name: &str,
        mv: &MaterializedView,
    ) -> Result<()> {
        (**self).put_materialized_view(db_name, name, mv)
    }
    fn get_materialized_view(&self, db_name: &str, name: &str) -> Result<Option<MaterializedView>> {
        (**self).get_materialized_view(db_name, name)
    }
    fn delete_materialized_view(&self, db_name: &str, name: &str) -> Result<()> {
        (**self).delete_materialized_view(db_name, name)
    }
    fn list_materialized_views(&self, db_name: &str) -> Result<Vec<MaterializedView>> {
        (**self).list_materialized_views(db_name)
    }
    fn put_procedure(
        &self,
        db_name: &str,
        name: &str,
        proc: &StoredProcedure,
    ) -> Result<()> {
        (**self).put_procedure(db_name, name, proc)
    }
    fn get_procedure(&self, db_name: &str, name: &str) -> Result<Option<StoredProcedure>> {
        (**self).get_procedure(db_name, name)
    }
    fn delete_procedure(&self, db_name: &str, name: &str) -> Result<()> {
        (**self).delete_procedure(db_name, name)
    }
    fn list_procedures(&self, db_name: &str) -> Result<Vec<StoredProcedure>> {
        (**self).list_procedures(db_name)
    }
    fn flush(&self) -> Result<()> {
        (**self).flush()
    }
    fn load(&self) -> Result<()> {
        (**self).load()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{KeysType, Table, TableColumn};
    use types::DataType;

    fn make_table(id: u64, name: &str) -> Table {
        Table {
            id,
            tablet_id: 0, // TODO: 创建table时分配真实的tablet_id
            name: name.to_string(),
            database: "testdb".to_string(),
            columns: vec![TableColumn {
                name: "id".into(),
                data_type: DataType::Int64,
                nullable: false,
                default_value: None,
                agg_type: None,
                comment: String::new(),
            }],
            keys_type: KeysType::Duplicate,
            unique_keys: vec![],
            partition_info: None,
            distribution_info: None,
            replication_num: 1,
            properties: HashMap::new(),
            row_count: 0,
            data_size: 0,
            stats: None,
            view_definition: None,
        }
    }

    #[test]
    fn test_create_database() {
        let mgr = CatalogManager::new();
        assert!(mgr.create_database("db1").is_ok());
        assert!(mgr.list_databases().contains(&"db1".to_string()));
    }

    #[test]
    fn test_create_database_duplicate() {
        let mgr = CatalogManager::new();
        mgr.create_database("db1").unwrap();
        let result = mgr.create_database("db1");
        assert!(result.is_err());
    }

    #[test]
    fn test_drop_database() {
        let mgr = CatalogManager::new();
        mgr.create_database("db1").unwrap();
        assert!(mgr.drop_database("db1").is_ok());
        assert!(!mgr.list_databases().contains(&"db1".to_string()));
    }

    #[test]
    fn test_drop_database_nonexistent() {
        let mgr = CatalogManager::new();
        assert!(mgr.drop_database("no_such_db").is_err());
    }

    #[test]
    fn test_create_table() {
        let mgr = CatalogManager::new();
        mgr.create_database("mydb").unwrap();
        let table = make_table(1, "users");
        assert!(mgr.create_table("mydb", table).is_ok());
        let t = mgr.get_table("mydb", "users");
        assert!(t.is_some());
        assert_eq!(t.unwrap().name, "users");
    }

    #[test]
    fn test_create_table_wrong_db() {
        let mgr = CatalogManager::new();
        let table = make_table(1, "users");
        assert!(mgr.create_table("no_db", table).is_err());
    }

    #[test]
    fn test_drop_table() {
        let mgr = CatalogManager::new();
        mgr.create_database("mydb").unwrap();
        let table = make_table(1, "users");
        mgr.create_table("mydb", table).unwrap();
        assert!(mgr.drop_table("mydb", "users").is_ok());
        assert!(mgr.get_table("mydb", "users").is_none());
    }

    #[test]
    fn test_list_tables() {
        let mgr = CatalogManager::new();
        mgr.create_database("mydb").unwrap();
        mgr.create_table("mydb", make_table(1, "t1")).unwrap();
        mgr.create_table("mydb", make_table(2, "t2")).unwrap();
        let tables = mgr.list_tables("mydb").unwrap();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"t1".to_string()));
        assert!(tables.contains(&"t2".to_string()));
    }

    #[test]
    fn test_information_schema_exists() {
        let mgr = CatalogManager::new();
        assert!(
            mgr.list_databases()
                .contains(&"information_schema".to_string())
        );
    }

    #[test]
    fn test_catalog_save_and_load() {
        let dir = format!("/tmp/rovisdb_test_catalog_{}", std::process::id());
        let mgr = CatalogManager::with_path(&dir);
        mgr.create_database("saved_db").unwrap();
        mgr.create_table("saved_db", make_table(1, "saved_table"))
            .unwrap();
        mgr.save().unwrap();

        let mgr2 = CatalogManager::with_path(&dir);
        mgr2.load().unwrap();
        assert!(mgr2.get_database("saved_db").is_some());
        assert!(mgr2.get_table("saved_db", "saved_table").is_some());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_rocks_backend() {
        let dir = format!("/tmp/rovisdb_test_rocks_{}", std::process::id());
        let _ = std::fs::remove_dir_all(&dir); // Clean any stale data
        let config = CatalogConfig {
            catalog_path: dir.clone(),
            use_rocks_meta: true,
            dual_write: false,
        };
        let mgr = CatalogManager::with_config(config);
        assert_eq!(mgr.backend_type(), "kvstore");

        mgr.create_database("rocks_db").unwrap();
        mgr.create_table("rocks_db", make_table(1, "rocks_table"))
            .unwrap();

        mgr.save().unwrap();

        // Drop mgr to release the KV store lock before reopening
        drop(mgr);

        // Reopen and verify
        let config2 = CatalogConfig {
            catalog_path: dir.clone(),
            use_rocks_meta: true,
            dual_write: false,
        };
        let mgr2 = CatalogManager::with_config(config2);
        mgr2.load().unwrap();
        assert!(mgr2.get_database("rocks_db").is_some());
        assert!(mgr2.get_table("rocks_db", "rocks_table").is_some());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_dual_write_backend() {
        let dir = format!("/tmp/rovisdb_test_dual_{}", std::process::id());
        let mgr = CatalogManager::with_dual_write(&dir);
        assert_eq!(mgr.backend_type(), "dual-write");

        mgr.create_database("dual_db").unwrap();
        mgr.create_table("dual_db", make_table(1, "dual_table"))
            .unwrap();
        mgr.save().unwrap();

        let _ = std::fs::remove_dir_all(&dir);
    }
}
