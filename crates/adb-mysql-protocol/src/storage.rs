//! AnalyticDB MySQL in-memory storage backend with column-type awareness

use dashmap::DashMap;
use std::sync::Arc;
use tracing::debug;

/// Column type enum matching common SQL types
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    Int,
    VarChar,
    Double,
    Unknown,
}

impl ColumnType {
    pub fn from_sql_type(sql_type: &str) -> Self {
        let upper = sql_type.to_uppercase();
        if upper.contains("INT") {
            ColumnType::Int
        } else if upper.contains("VARCHAR") || upper.contains("CHAR") || upper.contains("TEXT") || upper.contains("STRING") {
            ColumnType::VarChar
        } else if upper.contains("DOUBLE") || upper.contains("FLOAT") || upper.contains("DECIMAL") || upper.contains("NUMERIC") {
            ColumnType::Double
        } else {
            ColumnType::Unknown
        }
    }
}

/// Column definition with name and type
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub col_type: ColumnType,
}

/// Column-oriented storage for analytical queries
pub struct AdbMysqlStorage {
    databases: DashMap<String, Arc<AdbMysqlDatabase>>,
}

pub struct AdbMysqlDatabase {
    tables: DashMap<String, Arc<AdbMysqlTable>>,
}

pub struct AdbMysqlTable {
    pub columns: Vec<ColumnDef>,
    /// Rows stored as Vec<String> (all values stringified for simplicity)
    rows: DashMap<u64, Vec<String>>,
    next_row_id: std::sync::atomic::AtomicU64,
}

impl AdbMysqlStorage {
    pub fn new() -> Self {
        Self {
            databases: DashMap::new(),
        }
    }

    pub fn create_database(&self, name: &str) {
        debug!("ADB storage: creating database '{}'", name);
        self.databases
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(AdbMysqlDatabase::new()));
    }

    pub fn drop_database(&self, name: &str) -> bool {
        debug!("ADB storage: dropping database '{}'", name);
        self.databases.remove(name).is_some()
    }

    pub fn get_database(&self, name: &str) -> Option<Arc<AdbMysqlDatabase>> {
        self.databases.get(name).map(|d| d.value().clone())
    }

    pub fn list_databases(&self) -> Vec<String> {
        self.databases.iter().map(|d| d.key().clone()).collect()
    }
}

impl AdbMysqlDatabase {
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
        }
    }

    pub fn create_table(&self, name: &str, columns: Vec<ColumnDef>) {
        self.tables
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(AdbMysqlTable::new(columns)));
    }

    pub fn drop_table(&self, name: &str) -> bool {
        self.tables.remove(name).is_some()
    }

    pub fn get_table(&self, name: &str) -> Option<Arc<AdbMysqlTable>> {
        self.tables.get(name).map(|t| t.value().clone())
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.tables.iter().map(|t| t.key().clone()).collect()
    }
}

impl AdbMysqlTable {
    pub fn new(columns: Vec<ColumnDef>) -> Self {
        Self {
            columns,
            rows: DashMap::new(),
            next_row_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    pub fn insert(&self, values: Vec<String>) {
        let row_id = self
            .next_row_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.rows.insert(row_id, values);
    }

    /// Remove all rows from the table (TRUNCATE).
    pub fn truncate(&self) {
        self.rows.clear();
    }

    pub fn select_all(&self) -> Vec<Vec<String>> {
        let mut ids: Vec<u64> = self.rows.iter().map(|r| *r.key()).collect();
        ids.sort();
        ids.iter()
            .filter_map(|id| self.rows.get(id).map(|r| r.value().clone()))
            .collect::<Vec<_>>()
    }

    /// Get all rows with their IDs for ordered iteration
    pub fn select_all_ordered(&self) -> Vec<(u64, Vec<String>)> {
        let mut ids: Vec<u64> = self.rows.iter().map(|r| *r.key()).collect();
        ids.sort();
        ids.iter()
            .filter_map(|id| self.rows.get(id).map(|r| (*id, r.value().clone())))
            .collect()
    }

    pub fn update_where(&self, col_indices: &[usize], set_values: &[String], where_col: usize, where_op: &str, where_val: &str) -> u64 {
        let mut count = 0u64;
        let ids: Vec<u64> = self.rows.iter().map(|r| *r.key()).collect();
        for id in ids {
            if let Some(mut row) = self.rows.get_mut(&id) {
                if self.matches_where(row.as_slice(), where_col, where_op, where_val) {
                    for (&ci, sv) in col_indices.iter().zip(set_values.iter()) {
                        if ci < row.len() {
                            row[ci] = sv.clone();
                        }
                    }
                    count += 1;
                }
            }
        }
        count
    }

    pub fn delete_where(&self, where_col: usize, where_op: &str, where_val: &str) -> u64 {
        let mut count = 0u64;
        let ids: Vec<u64> = self.rows.iter().map(|r| *r.key()).collect();
        for id in ids {
            if let Some(row) = self.rows.get(&id) {
                if self.matches_where(row.as_slice(), where_col, where_op, where_val) {
                    drop(row);
                    self.rows.remove(&id);
                    count += 1;
                }
            }
        }
        count
    }

    pub fn update_row(&self, row_id: u64, col_idx: usize, value: &str) {
        if let Some(mut row) = self.rows.get_mut(&row_id) {
            if col_idx < row.len() {
                row[col_idx] = value.to_string();
            }
        }
    }

    pub fn delete_row(&self, row_id: u64) {
        self.rows.remove(&row_id);
    }

    pub fn count(&self) -> usize {
        self.rows.len()
    }

    fn matches_where(&self, row: &[String], col_idx: usize, op: &str, val: &str) -> bool {
        if col_idx >= row.len() {
            return false;
        }
        let cell = &row[col_idx];
        match op {
            "=" => cell == val,
            "!=" | "<>" => cell != val,
            ">" => self.compare_values(cell, val) == Some(std::cmp::Ordering::Greater),
            ">=" => self.compare_values(cell, val) != Some(std::cmp::Ordering::Less),
            "<" => self.compare_values(cell, val) == Some(std::cmp::Ordering::Less),
            "<=" => self.compare_values(cell, val) != Some(std::cmp::Ordering::Greater),
            _ => false,
        }
    }

    fn compare_values(&self, a: &str, b: &str) -> Option<std::cmp::Ordering> {
        // Try numeric comparison first
        if let (Ok(na), Ok(nb)) = (a.parse::<f64>(), b.parse::<f64>()) {
            return na.partial_cmp(&nb);
        }
        // Fall back to string comparison
        Some(a.cmp(b))
    }
}
