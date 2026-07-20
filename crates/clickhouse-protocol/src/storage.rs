//! ClickHouse column storage backend

use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;

/// ClickHouse table with columnar storage
pub struct Table {
    pub columns: HashMap<String, Vec<String>>,
    pub column_order: Vec<String>,
    pub column_types: HashMap<String, String>,
}

impl Table {
    pub fn new() -> Self {
        Self {
            columns: HashMap::new(),
            column_order: Vec::new(),
            column_types: HashMap::new(),
        }
    }

    pub fn create_column(&mut self, name: String, type_name: String) {
        self.columns.insert(name.clone(), Vec::new());
        self.column_order.push(name.clone());
        self.column_types.insert(name, type_name);
    }

    pub fn insert_row(&mut self, values: Vec<String>) {
        for (i, value) in values.into_iter().enumerate() {
            if i < self.column_order.len() {
                let col_name = &self.column_order[i];
                if let Some(column) = self.columns.get_mut(col_name) {
                    column.push(value);
                }
            }
        }
    }

    pub fn select_all(&self) -> Vec<HashMap<String, String>> {
        let mut rows = Vec::new();

        if self.columns.is_empty() {
            return rows;
        }

        let row_count = self.columns.values().next().map(|c| c.len()).unwrap_or(0);

        for i in 0..row_count {
            let mut row = HashMap::new();
            for (col_name, values) in &self.columns {
                if i < values.len() {
                    row.insert(col_name.clone(), values[i].clone());
                }
            }
            rows.push(row);
        }

        rows
    }

    /// Delete rows matching a predicate. Returns number of rows deleted.
    pub fn delete_where<F>(&mut self, predicate: F) -> usize
    where
        F: Fn(&HashMap<String, String>) -> bool,
    {
        // Validate all columns have the same length to prevent index panics
        let lengths: Vec<usize> = self.columns.values().map(|c| c.len()).collect();
        if lengths.is_empty() {
            return 0;
        }
        let row_count = lengths[0];
        if lengths.iter().any(|&l| l != row_count) {
            return 0; // column length mismatch, skip deletion
        }

        let mut keep = Vec::with_capacity(row_count);

        for i in 0..row_count {
            let mut row = HashMap::new();
            for (col_name, values) in &self.columns {
                if i < values.len() {
                    row.insert(col_name.clone(), values[i].clone());
                }
            }
            keep.push(!predicate(&row));
        }

        let mut deleted = 0;
        for col in self.columns.values_mut() {
            let mut new_col = Vec::new();
            for (i, val) in col.drain(..).enumerate() {
                if keep[i] {
                    new_col.push(val);
                } else {
                    deleted += 1;
                }
            }
            *col = new_col;
        }
        // delete_where counts per-column, divide by number of columns
        if self.columns.is_empty() {
            0
        } else {
            deleted / self.columns.len()
        }
    }

    /// Update rows matching a predicate. Returns number of rows updated.
    pub fn update_where<F>(
        &mut self,
        predicate: F,
        updates: &HashMap<String, String>,
    ) -> usize
    where
        F: Fn(&HashMap<String, String>) -> bool,
    {
        let row_count = self.columns.values().next().map(|c| c.len()).unwrap_or(0);
        let mut indices_to_update = Vec::new();
        for i in 0..row_count {
            let mut row = HashMap::new();
            for (col_name, values) in &self.columns {
                if i < values.len() {
                    row.insert(col_name.clone(), values[i].clone());
                }
            }
            if predicate(&row) {
                indices_to_update.push(i);
            }
        }

        // Apply updates
        for (col_name, new_value) in updates {
            if let Some(col) = self.columns.get_mut(col_name) {
                for &idx in &indices_to_update {
                    if idx < col.len() {
                        col[idx] = new_value.clone();
                    }
                }
            }
        }

        indices_to_update.len()
    }

    pub fn count(&self) -> usize {
        self.columns.values().next().map(|c| c.len()).unwrap_or(0)
    }

    pub fn columns(&self) -> &HashMap<String, Vec<String>> {
        &self.columns
    }

    pub fn column_types(&self) -> &HashMap<String, String> {
        &self.column_types
    }
}

impl Default for Table {
    fn default() -> Self {
        Self::new()
    }
}

/// ClickHouse database
pub struct Database {
    tables: DashMap<String, Table>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
        }
    }

    pub fn create_table(&self, name: &str) {
        self.tables.entry(name.to_string()).or_insert_with(Table::new);
    }

    pub fn get_table(&self, name: &str) -> Option<Table> {
        self.tables.get(name).map(|t| {
            let mut new_table = Table::new();
            new_table.columns = t.columns.clone();
            new_table.column_order = t.column_order.clone();
            new_table.column_types = t.column_types.clone();
            new_table
        })
    }

    pub fn with_table_mut<F, R>(&self, name: &str, f: F) -> Option<R>
    where
        F: FnOnce(&mut Table) -> R,
    {
        self.tables.get_mut(name).map(|mut t| f(&mut t))
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.tables.iter().map(|entry| entry.key().clone()).collect()
    }

    pub fn drop_table(&self, name: &str) -> bool {
        self.tables.remove(name).is_some()
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

/// ClickHouse storage backend
pub struct ClickHouseStorage {
    databases: DashMap<String, Arc<Database>>,
}

impl ClickHouseStorage {
    pub fn new() -> Self {
        Self {
            databases: DashMap::new(),
        }
    }

    pub fn create_database(&self, name: &str) {
        self.databases
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Database::new()));
    }

    pub fn get_database(&self, name: &str) -> Arc<Database> {
        self.databases
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Database::new()))
            .clone()
    }

    pub fn drop_database(&self, name: &str) -> bool {
        self.databases.remove(name).is_some()
    }

    pub fn list_databases(&self) -> Vec<String> {
        self.databases.iter().map(|entry| entry.key().clone()).collect()
    }
}

impl Default for ClickHouseStorage {
    fn default() -> Self {
        Self::new()
    }
}
