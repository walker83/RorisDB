//! Vector database command handler

use crate::storage::VectorStorage;
use serde_json::{json, Value};
use std::sync::Arc;

pub struct VectorHandler {
    storage: Arc<VectorStorage>,
}

impl VectorHandler {
    pub fn new(storage: Arc<VectorStorage>) -> Self {
        Self { storage }
    }

    pub fn handle_request(&self, method: &str, path: &str, body: &str) -> String {
        match (method, path) {
            ("POST", "/collections") => self.create_collection(body),
            ("GET", "/collections") => self.list_collections(),
            ("DELETE", "/collections") => self.delete_collection(body),
            ("POST", "/vectors") => self.insert_vector(body),
            ("DELETE", "/vectors") => self.delete_vector(body),
            ("POST", "/search") => self.search_vectors(body),
            ("GET", "/collections/count") => self.count_vectors(),
            _ => json!({"error": "Unknown endpoint"}).to_string(),
        }
    }

    fn create_collection(&self, body: &str) -> String {
        if let Ok(req) = serde_json::from_str::<Value>(body) {
            let name = req["name"].as_str().unwrap_or("default");
            let dimension = req["dimension"].as_u64().unwrap_or(128) as usize;
            self.storage.create_collection(name, dimension);
            json!({"status": "created", "name": name}).to_string()
        } else {
            json!({"error": "Invalid JSON"}).to_string()
        }
    }

    fn list_collections(&self) -> String {
        let collections = self.storage.list_collections();
        json!({"collections": collections}).to_string()
    }

    fn delete_collection(&self, body: &str) -> String {
        if let Ok(req) = serde_json::from_str::<Value>(body) {
            let name = req["name"].as_str().unwrap_or("default");
            if self.storage.drop_collection(name) {
                json!({"status": "deleted", "name": name}).to_string()
            } else {
                json!({"error": "Collection not found"}).to_string()
            }
        } else {
            json!({"error": "Invalid JSON"}).to_string()
        }
    }

    fn insert_vector(&self, body: &str) -> String {
        if let Ok(req) = serde_json::from_str::<Value>(body) {
            let collection = req["collection"].as_str().unwrap_or("default");
            let id = req["id"].as_str().unwrap_or("unknown");
            let vector: Vec<f32> = req["vector"]
                .as_array()
                .unwrap_or(&vec![])
                .iter()
                .filter_map(|v| v.as_f64().map(|f| f as f32))
                .collect();
            let metadata = req["metadata"].to_string();

            if let Some(coll) = self.storage.get_collection(collection) {
                coll.insert(id, vector, metadata);
                json!({"status": "inserted", "id": id}).to_string()
            } else {
                json!({"error": "Collection not found"}).to_string()
            }
        } else {
            json!({"error": "Invalid JSON"}).to_string()
        }
    }

    fn delete_vector(&self, body: &str) -> String {
        if let Ok(req) = serde_json::from_str::<Value>(body) {
            let collection = req["collection"].as_str().unwrap_or("default");
            let id = req["id"].as_str().unwrap_or("unknown");

            if let Some(coll) = self.storage.get_collection(collection) {
                if coll.delete(id) {
                    json!({"status": "deleted", "id": id}).to_string()
                } else {
                    json!({"error": "Vector not found"}).to_string()
                }
            } else {
                json!({"error": "Collection not found"}).to_string()
            }
        } else {
            json!({"error": "Invalid JSON"}).to_string()
        }
    }

    fn search_vectors(&self, body: &str) -> String {
        if let Ok(req) = serde_json::from_str::<Value>(body) {
            let collection = req["collection"].as_str().unwrap_or("default");
            let vector: Vec<f32> = req["vector"]
                .as_array()
                .unwrap_or(&vec![])
                .iter()
                .filter_map(|v| v.as_f64().map(|f| f as f32))
                .collect();
            let top_k = req["top_k"].as_u64().unwrap_or(10) as usize;

            if let Some(coll) = self.storage.get_collection(collection) {
                let results = coll.search(&vector, top_k);
                let response: Vec<Value> = results
                    .iter()
                    .map(|(id, score, metadata)| {
                        json!({
                            "id": id,
                            "score": score,
                            "metadata": metadata
                        })
                    })
                    .collect();
                json!({"results": response}).to_string()
            } else {
                json!({"error": "Collection not found"}).to_string()
            }
        } else {
            json!({"error": "Invalid JSON"}).to_string()
        }
    }

    fn count_vectors(&self) -> String {
        let total: usize = self
            .storage
            .list_collections()
            .iter()
            .filter_map(|name| self.storage.get_collection(name).map(|c| c.count()))
            .sum();
        json!({"count": total}).to_string()
    }
}
