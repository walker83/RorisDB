//! MongoDB document storage backend

use bson::{Bson, Document};
use dashmap::DashMap;
use std::sync::Arc;

/// MongoDB collection storage
pub struct Collection {
    documents: DashMap<String, Document>,
}

impl Collection {
    pub fn new() -> Self {
        Self {
            documents: DashMap::new(),
        }
    }

    pub fn insert(&self, id: String, doc: Document) {
        self.documents.insert(id, doc);
    }

    pub fn find(&self, filter: Option<&Document>) -> Vec<Document> {
        self.documents
            .iter()
            .filter(|entry| {
                if let Some(f) = filter {
                    Self::matches_filter(entry.value(), f)
                } else {
                    true
                }
            })
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub fn update(&self, id: &str, update: &Document) -> bool {
        if let Some(mut entry) = self.documents.get_mut(id) {
            Self::apply_update(&mut entry, update);
            true
        } else {
            false
        }
    }

    pub fn delete(&self, id: &str) -> bool {
        self.documents.remove(id).is_some()
    }

    pub fn count(&self) -> usize {
        self.documents.len()
    }

    /// Get a field value from a document supporting dot notation (public API)
    pub fn get_field<'a>(doc: &'a Document, field: &str) -> Option<&'a Bson> {
        Self::get_field_value(doc, field)
    }

    /// Get a field value supporting dot notation (e.g. "address.city")
    fn get_field_value<'a>(doc: &'a Document, field: &str) -> Option<&'a Bson> {
        if let Some(val) = doc.get(field) {
            return Some(val);
        }
        if field.contains('.') {
            let parts: Vec<&str> = field.splitn(2, '.').collect();
            if let Some(sub_doc) = doc.get_document(parts[0]).ok() {
                return Self::get_field_value(sub_doc, parts[1]);
            }
        }
        None
    }

    /// Compare two BSON values for ordering (manual since Bson lacks PartialOrd)
    fn bson_cmp(a: &Bson, b: &Bson) -> Option<std::cmp::Ordering> {
        match (a, b) {
            (Bson::Int32(a), Bson::Int32(b)) => a.partial_cmp(b),
            (Bson::Int64(a), Bson::Int64(b)) => a.partial_cmp(b),
            (Bson::Double(a), Bson::Double(b)) => a.partial_cmp(b),
            (Bson::String(a), Bson::String(b)) => a.partial_cmp(b),
            (Bson::Boolean(a), Bson::Boolean(b)) => a.partial_cmp(b),
            // Cross-numeric comparisons
            (Bson::Int32(a), Bson::Int64(b)) => (*a as i64).partial_cmp(b),
            (Bson::Int64(a), Bson::Int32(b)) => a.partial_cmp(&(*b as i64)),
            (Bson::Int32(a), Bson::Double(b)) => (*a as f64).partial_cmp(b),
            (Bson::Double(a), Bson::Int32(b)) => a.partial_cmp(&(*b as f64)),
            (Bson::Int64(a), Bson::Double(b)) => (*a as f64).partial_cmp(b),
            (Bson::Double(a), Bson::Int64(b)) => a.partial_cmp(&(*b as f64)),
            _ => None,
        }
    }

    /// Check if a document matches a filter, supporting query operators
    pub fn matches_filter(doc: &Document, filter: &Document) -> bool {
        for (key, filter_value) in filter {
            let doc_value = Self::get_field_value(doc, key);
            if let Some(op_doc) = filter_value.as_document() {
                if !Self::matches_operators(doc_value, op_doc) {
                    return false;
                }
            } else {
                match doc_value {
                    Some(v) if v == filter_value => {}
                    _ => return false,
                }
            }
        }
        true
    }

    /// Evaluate query operators ($eq, $ne, $gt, $gte, $lt, $lte, $in, $regex, $exists)
    fn matches_operators(doc_value: Option<&Bson>, op_doc: &Document) -> bool {
        for (op, op_val) in op_doc {
            let result = match op.as_str() {
                "$exists" => {
                    let exists = doc_value.is_some();
                    match op_val.as_bool() {
                        Some(b) => exists == b,
                        None => false,
                    }
                }
                "$eq" => doc_value.map_or(false, |v| v == op_val),
                "$ne" => doc_value.map_or(true, |v| v != op_val),
                "$gt" => doc_value
                    .and_then(|v| Self::bson_cmp(v, op_val).map(|o| o.is_gt()))
                    .unwrap_or(false),
                "$gte" => doc_value
                    .and_then(|v| Self::bson_cmp(v, op_val).map(|o| o.is_ge()))
                    .unwrap_or(false),
                "$lt" => doc_value
                    .and_then(|v| Self::bson_cmp(v, op_val).map(|o| o.is_lt()))
                    .unwrap_or(false),
                "$lte" => doc_value
                    .and_then(|v| Self::bson_cmp(v, op_val).map(|o| o.is_le()))
                    .unwrap_or(false),
                "$in" => {
                    if let (Some(v), Some(arr)) = (doc_value, op_val.as_array()) {
                        arr.contains(v)
                    } else {
                        false
                    }
                }
                "$regex" => {
                    if let (Some(v), Some(pattern_str)) =
                        (doc_value.and_then(|v| v.as_str()), op_val.as_str())
                    {
                        regex::Regex::new(pattern_str)
                            .map(|re| re.is_match(v))
                            .unwrap_or(false)
                    } else {
                        false
                    }
                }
                _ => false, // Unknown operator – reject
            };
            if !result {
                return false;
            }
        }
        true
    }

    /// Apply an update document ($set, $inc) to a target document
    fn apply_update(target: &mut Document, update: &Document) {
        if let Ok(set) = update.get_document("$set") {
            let set_ops: Vec<(String, Bson)> =
                set.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            for (key, value) in set_ops {
                if key.contains('.') {
                    Self::set_nested_field(target, &key, value);
                } else {
                    target.insert(key, value);
                }
            }
        }

        if let Ok(inc) = update.get_document("$inc") {
            let inc_ops: Vec<(String, Bson)> =
                inc.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            for (key, inc_value) in inc_ops {
                let current = target
                    .get(&key)
                    .cloned()
                    .unwrap_or(Bson::Int32(0));
                let new_value = match (&current, &inc_value) {
                    (Bson::Int32(a), Bson::Int32(b)) => Bson::Int32(a + b),
                    (Bson::Int64(a), Bson::Int64(b)) => Bson::Int64(a + b),
                    (Bson::Int32(a), Bson::Int64(b)) => Bson::Int64(*a as i64 + b),
                    (Bson::Int64(a), Bson::Int32(b)) => Bson::Int64(a + *b as i64),
                    (Bson::Double(a), Bson::Double(b)) => Bson::Double(a + b),
                    (Bson::Int32(a), Bson::Double(b)) => Bson::Double(*a as f64 + b),
                    (Bson::Int64(a), Bson::Double(b)) => Bson::Double(*a as f64 + b),
                    (Bson::Double(a), Bson::Int32(b)) => Bson::Double(a + *b as f64),
                    (Bson::Double(a), Bson::Int64(b)) => Bson::Double(a + *b as f64),
                    _ => current,
                };
                target.insert(key, new_value);
            }
        }
    }

    /// Set a value at a dot-notation path, creating intermediate documents as needed
    fn set_nested_field(doc: &mut Document, path: &str, value: Bson) {
        let parts: Vec<&str> = path.split('.').collect();
        if parts.len() == 1 {
            doc.insert(parts[0].to_string(), value);
            return;
        }

        let first = parts[0];
        let rest = parts[1..].join(".");

        // Get or create intermediate document
        if doc.get_document(first).is_err() {
            doc.insert(first.to_string(), Bson::Document(Document::new()));
        }
        if let Ok(sub_doc) = doc.get_document_mut(first) {
            Self::set_nested_field(sub_doc, &rest, value);
        }
    }
}

impl Default for Collection {
    fn default() -> Self {
        Self::new()
    }
}

/// MongoDB database
pub struct Database {
    collections: DashMap<String, Arc<Collection>>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            collections: DashMap::new(),
        }
    }

    pub fn get_collection(&self, name: &str) -> Arc<Collection> {
        self.collections
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Collection::new()))
            .clone()
    }

    pub fn list_collections(&self) -> Vec<String> {
        self.collections
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    pub fn drop_collection(&self, name: &str) -> bool {
        self.collections.remove(name).is_some()
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

/// MongoDB storage backend with multiple databases
pub struct MongoDBStorage {
    databases: DashMap<String, Arc<Database>>,
}

impl MongoDBStorage {
    pub fn new() -> Self {
        Self {
            databases: DashMap::new(),
        }
    }

    pub fn get_database(&self, name: &str) -> Arc<Database> {
        self.databases
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Database::new()))
            .clone()
    }

    pub fn list_databases(&self) -> Vec<String> {
        self.databases
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    pub fn drop_database(&self, name: &str) -> bool {
        self.databases.remove(name).is_some()
    }
}

impl Default for MongoDBStorage {
    fn default() -> Self {
        Self::new()
    }
}
