//! Elasticsearch REST API command handler

use crate::storage::{Document, ElasticsearchStorage};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

/// Trait for handling Elasticsearch commands
pub trait ElasticsearchCommandHandler: Send + Sync {
    fn handle_request(&self, method: &str, path: &str, body: Option<&str>) -> serde_json::Value;
}

/// Default Elasticsearch command handler
pub struct DefaultElasticsearchHandler {
    storage: Arc<ElasticsearchStorage>,
}

impl DefaultElasticsearchHandler {
    pub fn new(storage: Arc<ElasticsearchStorage>) -> Self {
        Self { storage }
    }
}

impl ElasticsearchCommandHandler for DefaultElasticsearchHandler {
    fn handle_request(&self, method: &str, path: &str, body: Option<&str>) -> serde_json::Value {
        // Helper: parse path parts
        let path_parts: Vec<&str> = path.trim_matches('/').split('/').filter(|s| !s.is_empty()).collect();

        // Strip query string from body if accidentally included
        let body = body.map(|b| {
            if b.starts_with('?') { None } else { Some(b) }
        }).flatten();

        match (method, path_parts.as_slice()) {
            // GET / - Cluster root
            ("GET", []) => {
                json!({
                    "name": "harness",
                    "cluster_name": "harness-cluster",
                    "cluster_uuid": "harness-uuid",
                    "version": {
                        "number": "7.10.2",
                        "build_flavor": "default",
                        "build_type": "docker",
                        "build_hash": "harness",
                        "build_date": "2024-01-01",
                        "build_snapshot": false,
                        "lucene_version": "8.7.0",
                        "minimum_wire_compatibility_version": "6.8.0",
                        "minimum_index_compatibility_version": "6.0.0-beta1"
                    },
                    "tagline": "You Know, for Search"
                })
            }

            // GET /_cluster/health
            ("GET", ["_cluster", "health"]) => {
                json!({
                    "cluster_name": "harness-cluster",
                    "status": "green",
                    "timed_out": false,
                    "number_of_nodes": 1,
                    "number_of_data_nodes": 1,
                    "active_primary_shards": 0,
                    "active_shards": 0,
                    "relocating_shards": 0,
                    "initializing_shards": 0,
                    "unassigned_shards": 0
                })
            }

            // GET /_cat/health
            ("GET", ["_cat", "health"]) => {
                json!([{
                    "cluster": "harness-cluster",
                    "status": "green",
                    "node.total": "1",
                    "node.data": "1",
                    "shards": "0",
                    "pri": "0",
                    "relo": "0",
                    "init": "0",
                    "unassign": "0",
                    "pending_tasks": "0",
                    "max_task_wait_time": "0s",
                    "active_shards_percent": "100.0%"
                }])
            }

            // GET /_cat/indices (all)
            ("GET", ["_cat", "indices"]) => {
                let indices = self.storage.list_indices();
                let result: Vec<serde_json::Value> = indices
                    .iter()
                    .map(|name| {
                        let index = self.storage.get_index(&name).unwrap();
                        json!({
                            "health": "green",
                            "status": "open",
                            "index": name,
                            "uuid": "harness-uuid",
                            "pri": "1",
                            "rep": "0",
                            "docs.count": index.count().to_string(),
                            "docs.deleted": "0",
                            "store.size": "0b",
                            "pri.store.size": "0b"
                        })
                    })
                    .collect();
                json!(result)
            }

            // GET /_cat/indices/{index}
            ("GET", ["_cat", "indices", index_name]) => {
                if let Some(index) = self.storage.get_index(index_name) {
                    json!([{
                        "health": "green",
                        "status": "open",
                        "index": index_name,
                        "uuid": "harness-uuid",
                        "pri": "1",
                        "rep": "0",
                        "docs.count": index.count().to_string(),
                        "docs.deleted": "0",
                        "store.size": "0b",
                        "pri.store.size": "0b"
                    }])
                } else {
                    json!({
                        "error": {
                            "root_cause": [{
                                "type": "index_not_found_exception",
                                "reason": "no such index"
                            }],
                            "type": "index_not_found_exception",
                            "reason": "no such index"
                        },
                        "status": 404
                    })
                }
            }

            // PUT /{index} - Create index
            ("PUT", [index_name]) if path_parts.len() == 1 => {
                self.storage.create_index(index_name);
                json!({
                    "acknowledged": true,
                    "shards_acknowledged": true,
                    "index": index_name
                })
            }

            // DELETE /{index} - Delete index
            ("DELETE", [index_name]) if path_parts.len() == 1 => {
                let deleted = self.storage.delete_index(index_name);
                if deleted {
                    json!({"acknowledged": true})
                } else {
                    json!({
                        "error": {
                            "root_cause": [{
                                "type": "index_not_found_exception",
                                "reason": "no such index"
                            }],
                            "type": "index_not_found_exception",
                            "reason": "no such index"
                        },
                        "status": 404
                    })
                }
            }

            // GET /{index} - Get index info
            ("GET", [index_name]) if path_parts.len() == 1 => {
                if self.storage.index_exists(index_name) {
                    let mut result = serde_json::Map::new();
                    result.insert(
                        index_name.to_string(),
                        json!({
                            "aliases": {},
                            "mappings": {},
                            "settings": {
                                "index": {
                                    "number_of_shards": "1",
                                    "number_of_replicas": "0"
                                }
                            }
                        }),
                    );
                    json!(result)
                } else {
                    json!({
                        "error": {
                            "root_cause": [{
                                "type": "index_not_found_exception",
                                "reason": "no such index"
                            }],
                            "type": "index_not_found_exception",
                            "reason": "no such index"
                        },
                        "status": 404
                    })
                }
            }

            // GET /{index}/_settings - Get index settings
            ("GET", [index_name, "_settings"]) => {
                if self.storage.index_exists(index_name) {
                    let mut result = serde_json::Map::new();
                    result.insert(
                        index_name.to_string(),
                        json!({
                            "settings": {
                                "index": {
                                    "number_of_shards": "1",
                                    "number_of_replicas": "0",
                                    "creation_date": "1700000000000",
                                    "uuid": "harness-uuid",
                                    "version": { "created": "7100299" },
                                    "provided_name": index_name
                                }
                            }
                        }),
                    );
                    json!(result)
                } else {
                    json!({
                        "error": {
                            "root_cause": [{
                                "type": "index_not_found_exception",
                                "reason": "no such index"
                            }],
                            "type": "index_not_found_exception",
                            "reason": "no such index"
                        },
                        "status": 404
                    })
                }
            }

            // POST /{index}/_refresh - Refresh index (no-op)
            ("POST", [index_name, "_refresh"]) => {
                if self.storage.index_exists(index_name) {
                    json!({
                        "_shards": {
                            "total": 2,
                            "successful": 1,
                            "failed": 0
                        }
                    })
                } else {
                    json!({
                        "error": {
                            "root_cause": [{
                                "type": "index_not_found_exception",
                                "reason": "no such index"
                            }],
                            "type": "index_not_found_exception",
                            "reason": "no such index"
                        },
                        "status": 404
                    })
                }
            }

            // POST /{index}/_bulk - Bulk operations
            ("POST", [index_name, "_bulk"]) | ("PUT", [index_name, "_bulk"]) => {
                if let Some(index) = self.storage.get_index(index_name) {
                    let mut items = Vec::new();
                    if let Some(body_str) = body {
                        let lines: Vec<&str> = body_str.lines().collect();
                        let mut i = 0;
                        while i + 1 < lines.len() {
                            let action_line = lines[i].trim();
                            let source_line = lines[i + 1].trim();
                            if action_line.is_empty() {
                                i += 1;
                                continue;
                            }
                            if let Ok(action_val) = serde_json::from_str::<serde_json::Value>(action_line) {
                                if let Some(action_obj) = action_val.as_object() {
                                    for (action_type, action_meta) in action_obj {
                                        let bulk_index = action_meta.get("_index").and_then(|v| v.as_str()).unwrap_or(index_name);
                                        let doc_id = action_meta.get("_id").and_then(|v| v.as_str()).unwrap_or("");
                                        match action_type.as_str() {
                                            "index" | "create" => {
                                                if let Ok(doc_map) = serde_json::from_str::<HashMap<String, serde_json::Value>>(source_line) {
                                                    let document = Document { fields: doc_map };
                                                    let id = if doc_id.is_empty() {
                                                        uuid::Uuid::new_v4().to_string()
                                                    } else {
                                                        doc_id.to_string()
                                                    };
                                                    index.index_document(id.clone(), document);
                                                    items.push(json!({
                                                        action_type: {
                                                            "_index": bulk_index,
                                                            "_type": "_doc",
                                                            "_id": id,
                                                            "_version": 1,
                                                            "result": "created",
                                                            "_shards": {"total": 2, "successful": 1, "failed": 0},
                                                            "_seq_no": 0,
                                                            "_primary_term": 1,
                                                            "status": 201
                                                        }
                                                    }));
                                                }
                                            }
                                            "delete" => {
                                                let id = doc_id.to_string();
                                                let deleted = index.delete_document(&id);
                                                items.push(json!({
                                                    "delete": {
                                                        "_index": bulk_index,
                                                        "_type": "_doc",
                                                        "_id": id,
                                                        "_version": 2,
                                                        "result": if deleted { "deleted" } else { "not_found" },
                                                        "_shards": {"total": 2, "successful": 1, "failed": 0},
                                                        "_seq_no": 1,
                                                        "_primary_term": 1,
                                                        "status": if deleted { 200 } else { 404 }
                                                    }
                                                }));
                                                // delete has no source line
                                                i += 1;
                                                continue;
                                            }
                                            "update" => {
                                                if let Ok(update_val) = serde_json::from_str::<serde_json::Value>(source_line) {
                                                    let id = doc_id.to_string();
                                                    if let Some(doc_obj) = update_val.get("doc").and_then(|v| v.as_object()) {
                                                        if let Some(existing) = index.get_document(&id) {
                                                            let mut updated_fields = existing.fields.clone();
                                                            for (k, v) in doc_obj {
                                                                updated_fields.insert(k.clone(), v.clone());
                                                            }
                                                            let updated_doc = Document { fields: updated_fields };
                                                            index.index_document(id.clone(), updated_doc);
                                                        }
                                                        items.push(json!({
                                                            "update": {
                                                                "_index": bulk_index,
                                                                "_type": "_doc",
                                                                "_id": id,
                                                                "_version": 2,
                                                                "result": "updated",
                                                                "_shards": {"total": 2, "successful": 1, "failed": 0},
                                                                "_seq_no": 1,
                                                                "_primary_term": 1,
                                                                "status": 200
                                                            }
                                                        }));
                                                    }
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            }
                            i += 2;
                        }
                    }
                    let has_errors = items.iter().any(|item| {
                        item.get("index").and_then(|i| i.get("status")).and_then(|s| s.as_u64()).unwrap_or(200) >= 400
                        || item.get("update").and_then(|i| i.get("status")).and_then(|s| s.as_u64()).unwrap_or(200) >= 400
                        || item.get("delete").and_then(|i| i.get("status")).and_then(|s| s.as_u64()).unwrap_or(200) >= 400
                    });
                    json!({
                        "took": 1,
                        "errors": has_errors,
                        "items": items
                    })
                } else {
                    json!({"error": "Index not found"})
                }
            }

            // PUT /{index}/_doc/{id} - Index document with explicit ID
            ("PUT", [index_name, "_doc", id]) if path_parts.len() == 3 => {
                let index = self.storage.get_index(index_name);
                if let Some(idx) = index {
                    if let Some(body_str) = body {
                        if let Ok(doc) = serde_json::from_str::<HashMap<String, serde_json::Value>>(body_str) {
                            let document = Document { fields: doc };
                            idx.index_document(id.to_string(), document);
                            return json!({
                                "_index": index_name,
                                "_type": "_doc",
                                "_id": id,
                                "_version": 1,
                                "result": "created",
                                "_shards": {
                                    "total": 2,
                                    "successful": 1,
                                    "failed": 0
                                },
                                "_seq_no": 0,
                                "_primary_term": 1
                            });
                        }
                    }
                }
                json!({"error": "Invalid request"})
            }

            // POST /{index}/_doc/{id} - Index document with explicit ID (POST variant)
            ("POST", [index_name, "_doc", id]) if path_parts.len() == 3 => {
                let index = self.storage.get_index(index_name);
                if let Some(idx) = index {
                    if let Some(body_str) = body {
                        if let Ok(doc) = serde_json::from_str::<HashMap<String, serde_json::Value>>(body_str) {
                            let document = Document { fields: doc };
                            idx.index_document(id.to_string(), document);
                            return json!({
                                "_index": index_name,
                                "_type": "_doc",
                                "_id": id,
                                "_version": 1,
                                "result": "created",
                                "_shards": {
                                    "total": 2,
                                    "successful": 1,
                                    "failed": 0
                                },
                                "_seq_no": 0,
                                "_primary_term": 1
                            });
                        }
                    }
                }
                json!({"error": "Invalid request"})
            }

            // POST /{index}/_doc - Index document with auto-generated ID
            ("POST", [index_name, "_doc"]) if path_parts.len() == 2 => {
                let index = self.storage.get_index(index_name);
                if let Some(idx) = index {
                    if let Some(body_str) = body {
                        if let Ok(doc) = serde_json::from_str::<HashMap<String, serde_json::Value>>(body_str) {
                            let document = Document { fields: doc };
                            let id = uuid::Uuid::new_v4().to_string();
                            idx.index_document(id.clone(), document);
                            return json!({
                                "_index": index_name,
                                "_type": "_doc",
                                "_id": id,
                                "_version": 1,
                                "result": "created",
                                "_shards": {
                                    "total": 2,
                                    "successful": 1,
                                    "failed": 0
                                },
                                "_seq_no": 0,
                                "_primary_term": 1
                            });
                        }
                    }
                }
                json!({"error": "Invalid request"})
            }

            // POST /{index}/_update/{id} - Update document
            ("POST", [index_name, "_update", id]) if path_parts.len() == 3 => {
                if let Some(index) = self.storage.get_index(index_name) {
                    if let Some(body_str) = body {
                        if let Ok(update_val) = serde_json::from_str::<serde_json::Value>(body_str) {
                            if let Some(doc_obj) = update_val.get("doc").and_then(|v| v.as_object()) {
                                if let Some(existing) = index.get_document(id) {
                                    let mut updated_fields = existing.fields.clone();
                                    for (k, v) in doc_obj {
                                        updated_fields.insert(k.clone(), v.clone());
                                    }
                                    let updated_doc = Document { fields: updated_fields };
                                    index.index_document(id.to_string(), updated_doc);
                                    return json!({
                                        "_index": index_name,
                                        "_type": "_doc",
                                        "_id": id,
                                        "_version": 2,
                                        "result": "updated",
                                        "_shards": {
                                            "total": 2,
                                            "successful": 1,
                                            "failed": 0
                                        },
                                        "_seq_no": 1,
                                        "_primary_term": 1
                                    });
                                }
                            }
                        }
                    }
                    return json!({
                        "error": {
                            "root_cause": [{
                                "type": "document_missing_exception",
                                "reason": format!("[{}]: document missing", id)
                            }],
                            "type": "document_missing_exception",
                            "reason": format!("[{}]: document missing", id)
                        },
                        "status": 404
                    });
                }
                json!({"error": "Index not found"})
            }

            // GET /{index}/_doc/{id} - Get document
            ("GET", [index_name, "_doc", id]) if path_parts.len() == 3 => {
                if let Some(index) = self.storage.get_index(index_name) {
                    if let Some(doc) = index.get_document(id) {
                        return json!({
                            "_index": index_name,
                            "_type": "_doc",
                            "_id": id,
                            "_version": 1,
                            "_seq_no": 0,
                            "_primary_term": 1,
                            "found": true,
                            "_source": doc.fields
                        });
                    }
                }
                json!({
                    "_index": index_name,
                    "_type": "_doc",
                    "_id": id,
                    "found": false
                })
            }

            // DELETE /{index}/_doc/{id} - Delete document
            ("DELETE", [index_name, "_doc", id]) if path_parts.len() == 3 => {
                if let Some(index) = self.storage.get_index(index_name) {
                    let deleted = index.delete_document(id);
                    return json!({
                        "_index": index_name,
                        "_type": "_doc",
                        "_id": id,
                        "_version": 2,
                        "result": if deleted { "deleted" } else { "not_found" },
                        "_shards": {
                            "total": 2,
                            "successful": 1,
                            "failed": 0
                        },
                        "_seq_no": 1,
                        "_primary_term": 1
                    });
                }
                json!({"error": "Index not found"})
            }

            // POST /{index}/_search - Search
            ("POST" | "GET", [index_name, "_search"]) if path_parts.len() == 2 => {
                if let Some(index) = self.storage.get_index(index_name) {
                    let query = body
                        .and_then(|b| serde_json::from_str(b).ok())
                        .unwrap_or(json!({"query": {"match_all": {}}}));

                    let results = index.search(&query);
                    let hits: Vec<serde_json::Value> = results
                        .iter()
                        .map(|(id, doc)| {
                            json!({
                                "_index": index_name,
                                "_type": "_doc",
                                "_id": id,
                                "_score": 1.0,
                                "_source": doc.fields
                            })
                        })
                        .collect();

                    return json!({
                        "took": 1,
                        "timed_out": false,
                        "_shards": {
                            "total": 1,
                            "successful": 1,
                            "skipped": 0,
                            "failed": 0
                        },
                        "hits": {
                            "total": {
                                "value": hits.len(),
                                "relation": "eq"
                            },
                            "max_score": 1.0,
                            "hits": hits
                        }
                    });
                }
                json!({"error": "Index not found"})
            }

            _ => {
                json!({
                    "error": {
                        "root_cause": [{
                            "type": "invalid_index_name_exception",
                            "reason": format!("Invalid index name - unmatched route: {} {}", method, path)
                        }],
                        "type": "invalid_index_name_exception",
                        "reason": format!("Invalid index name - unmatched route: {} {}", method, path)
                    },
                    "status": 400
                })
            }
        }
    }
}
