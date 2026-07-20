//! Lindorm wide-column storage backend

use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;

pub struct LindormStorage {
    tables: DashMap<String, Arc<LindormTable>>,
}

pub struct LindormTable {
    // rowkey -> (column_family -> (qualifier -> value))
    rows: DashMap<String, HashMap<String, HashMap<String, String>>>,
}

impl LindormStorage {
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
        }
    }

    pub fn create_table(&self, name: &str) -> bool {
        use dashmap::mapref::entry::Entry;
        match self.tables.entry(name.to_string()) {
            Entry::Occupied(_) => false, // already exists
            Entry::Vacant(v) => {
                v.insert(Arc::new(LindormTable::new()));
                true
            }
        }
    }

    pub fn get_table(&self, name: &str) -> Option<Arc<LindormTable>> {
        self.tables.get(name).map(|t| t.value().clone())
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.tables.iter().map(|t| t.key().clone()).collect()
    }
}

impl LindormTable {
    pub fn new() -> Self {
        Self {
            rows: DashMap::new(),
        }
    }

    pub fn put(&self, rowkey: &str, family: &str, qualifier: &str, value: &str) {
        let mut row = self.rows.entry(rowkey.to_string()).or_insert_with(HashMap::new);
        let family_map = row.entry(family.to_string()).or_insert_with(HashMap::new);
        family_map.insert(qualifier.to_string(), value.to_string());
    }

    pub fn get(&self, rowkey: &str) -> Option<HashMap<String, HashMap<String, String>>> {
        self.rows.get(rowkey).map(|r| r.clone())
    }

    pub fn delete(&self, rowkey: &str) -> bool {
        self.rows.remove(rowkey).is_some()
    }

    pub fn scan(&self, start_row: &str, end_row: &str) -> Vec<(String, HashMap<String, HashMap<String, String>>)> {
        self.rows
            .iter()
            .filter(|entry| {
                let key = entry.key();
                key.as_str() >= start_row && key.as_str() < end_row
            })
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    pub fn count(&self) -> usize {
        self.rows.len()
    }
}
