//! Cassandra storage backend

use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;

/// Cassandra column family (table)
pub struct ColumnFamily {
    rows: DashMap<String, HashMap<String, String>>, // partition_key -> columns
}

impl ColumnFamily {
    pub fn new() -> Self {
        Self {
            rows: DashMap::new(),
        }
    }

    pub fn insert(&self, key: String, columns: HashMap<String, String>) {
        self.rows.insert(key, columns);
    }

    pub fn select(&self, key: Option<&str>) -> Vec<HashMap<String, String>> {
        if let Some(k) = key {
            self.rows.get(k).map(|r| vec![r.clone()]).unwrap_or_default()
        } else {
            self.rows.iter().map(|r| r.value().clone()).collect()
        }
    }

    pub fn delete(&self, key: &str) -> bool {
        self.rows.remove(key).is_some()
    }

    pub fn count(&self) -> usize {
        self.rows.len()
    }
}

impl Default for ColumnFamily {
    fn default() -> Self {
        Self::new()
    }
}

/// Cassandra keyspace
pub struct Keyspace {
    tables: DashMap<String, Arc<ColumnFamily>>,
}

impl Keyspace {
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
        }
    }

    pub fn create_table(&self, name: &str) {
        self.tables.entry(name.to_string()).or_insert_with(|| Arc::new(ColumnFamily::new()));
    }

    pub fn get_table(&self, name: &str) -> Option<Arc<ColumnFamily>> {
        self.tables.get(name).map(|t| t.clone())
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.tables.iter().map(|entry| entry.key().clone()).collect()
    }
}

impl Default for Keyspace {
    fn default() -> Self {
        Self::new()
    }
}

/// Cassandra cluster
pub struct CassandraStorage {
    keyspaces: DashMap<String, Arc<Keyspace>>,
}

impl CassandraStorage {
    pub fn new() -> Self {
        let storage = Self {
            keyspaces: DashMap::new(),
        };

        // Create default system keyspaces
        storage.keyspaces.insert("system".to_string(), Arc::new(Keyspace::new()));
        storage.keyspaces.insert("system_schema".to_string(), Arc::new(Keyspace::new()));

        storage
    }

    pub fn get_keyspace(&self, name: &str) -> Option<Arc<Keyspace>> {
        self.keyspaces.get(name).map(|r| r.value().clone())
    }

    pub fn list_keyspaces(&self) -> Vec<String> {
        self.keyspaces.iter().map(|entry| entry.key().clone()).collect()
    }

    pub fn create_keyspace(&self, name: &str) {
        self.keyspaces.entry(name.to_string()).or_insert_with(|| Arc::new(Keyspace::new()));
    }
}

impl Default for CassandraStorage {
    fn default() -> Self {
        Self::new()
    }
}
