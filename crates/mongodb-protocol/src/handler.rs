//! MongoDB command handler

use crate::storage::MongoDBStorage;
use crate::wire::{Message, MessagePayload, OpMsg, OpReply, Section};
use bson::{doc, Bson, Document};
use std::sync::Arc;

/// Trait for handling MongoDB commands
pub trait MongoDBCommandHandler: Send + Sync {
    fn handle_message(&self, message: &Message) -> Message;
}

/// Default MongoDB command handler
pub struct DefaultMongoDBHandler {
    storage: Arc<MongoDBStorage>,
}

impl DefaultMongoDBHandler {
    pub fn new(storage: Arc<MongoDBStorage>) -> Self {
        Self { storage }
    }

    fn handle_command(&self, db_name: &str, command: &Document, sections: &[Section]) -> Document {
        // Extract command name (first field of the body document)
        let cmd_name = command.keys().next().map(|s| s.as_str()).unwrap_or("");

        match cmd_name {
            "ping" => doc! { "ok": 1 },
            "ismaster" | "hello" => self.cmd_ismaster(),
            "buildInfo" => self.cmd_build_info(),
            "serverStatus" => self.cmd_server_status(),
            "listDatabases" => self.cmd_list_databases(),
            "listCollections" => self.cmd_list_collections(db_name),
            "insert" => self.cmd_insert(db_name, command, sections),
            "find" => self.cmd_find(db_name, command),
            "update" => self.cmd_update(db_name, command, sections),
            "delete" => self.cmd_delete(db_name, command, sections),
            "count" => self.cmd_count(db_name, command),
            "create" => self.cmd_create(db_name, command),
            "drop" => self.cmd_drop(db_name, command),
            "dropDatabase" => self.cmd_drop_database(db_name),
            "getLog" => self.cmd_get_log(command),
            "getMore" => doc! { "ok": 1, "cursor": { "id": 0, "ns": "", "nextBatch": [] } },
            "aggregate" => self.cmd_aggregate(db_name, command, sections),
            _ => doc! { "ok": 0, "errmsg": format!("Unknown command: {}", cmd_name) },
        }
    }

    fn cmd_ismaster(&self) -> Document {
        doc! {
            "ismaster": true,
            "maxBsonObjectSize": 16777216,
            "maxMessageSizeBytes": 48000000,
            "maxWriteBatchSize": 100000,
            "localTime": bson::DateTime::now(),
            "maxWireVersion": 17,
            "minWireVersion": 0,
            "ok": 1
        }
    }

    fn cmd_build_info(&self) -> Document {
        doc! {
            "version": "7.0.0",
            "gitVersion": "harness",
            "modules": [],
            "allocator": "system",
            "bits": 64,
            "debug": false,
            "maxBsonObjectSize": 16777216,
            "ok": 1
        }
    }

    fn cmd_server_status(&self) -> Document {
        doc! {
            "host": "harness",
            "version": "7.0.0",
            "process": "harness",
            "pid": std::process::id() as i64,
            "uptime": 1i64,
            "ok": 1
        }
    }

    fn cmd_list_databases(&self) -> Document {
        let db_names = self.storage.list_databases();
        let databases: Vec<Document> = db_names
            .into_iter()
            .map(|name| {
                doc! {
                    "name": name,
                    "sizeOnDisk": 0i64,
                    "empty": true
                }
            })
            .collect();

        doc! {
            "databases": databases,
            "totalSize": 0i64,
            "ok": 1
        }
    }

    fn cmd_list_collections(&self, db_name: &str) -> Document {
        let db = self.storage.get_database(db_name);
        let collections = db.list_collections();

        let cursor_docs: Vec<Document> = collections
            .into_iter()
            .map(|name| doc! { "name": name, "type": "collection" })
            .collect();

        doc! {
            "cursor": {
                "id": 0i64,
                "ns": format!("{}.$cmd.listCollections", db_name),
                "firstBatch": cursor_docs
            },
            "ok": 1
        }
    }

    /// Extract document sequence from OP_MSG sections by identifier name.
    /// In MongoDB OP_MSG format, insert/update/delete send their document arrays
    /// as kind-1 sections with identifiers "documents", "updates", "deletes".
    fn get_doc_sequence<'a>(sections: &'a [Section], identifier: &str) -> Vec<&'a Document> {
        for section in sections {
            if let Section::DocumentSequence {
                identifier: id,
                documents,
            } = section
            {
                if id == identifier {
                    return documents.iter().collect();
                }
            }
        }
        vec![]
    }

    fn cmd_insert(&self, db_name: &str, command: &Document, sections: &[Section]) -> Document {
        let collection_name = match command.get_str("insert") {
            Ok(name) => name,
            Err(_) => return doc! { "ok": 0, "errmsg": "Missing collection name" },
        };

        // Documents come from the "documents" DocumentSequence section
        let docs = Self::get_doc_sequence(sections, "documents");

        let db = self.storage.get_database(db_name);
        let collection = db.get_collection(collection_name);

        let mut inserted = 0;
        for doc in docs {
            let mut doc = (*doc).clone();
            let id = doc
                .get_object_id("_id")
                .map(|oid| oid.to_hex())
                .unwrap_or_else(|_| {
                    let oid = bson::oid::ObjectId::new();
                    doc.insert("_id", Bson::ObjectId(oid));
                    oid.to_hex()
                });
            collection.insert(id, doc);
            inserted += 1;
        }

        doc! { "n": inserted, "ok": 1 }
    }

    fn cmd_find(&self, db_name: &str, command: &Document) -> Document {
        let collection_name = match command.get_str("find") {
            Ok(name) => name,
            Err(_) => return doc! { "ok": 0, "errmsg": "Missing collection name" },
        };

        let filter = command.get_document("filter").ok();

        let db = self.storage.get_database(db_name);
        let collection = db.get_collection(collection_name);
        let documents = collection.find(filter);

        doc! {
            "cursor": {
                "id": 0i64,
                "ns": format!("{}.{}", db_name, collection_name),
                "firstBatch": documents
            },
            "ok": 1
        }
    }

    fn cmd_update(&self, db_name: &str, command: &Document, sections: &[Section]) -> Document {
        let collection_name = match command.get_str("update") {
            Ok(name) => name,
            Err(_) => return doc! { "ok": 0, "errmsg": "Missing collection name" },
        };

        // Updates come from the "updates" DocumentSequence section
        let updates = Self::get_doc_sequence(sections, "updates");

        let db = self.storage.get_database(db_name);
        let collection = db.get_collection(collection_name);

        let mut modified = 0;
        for update_doc in updates {
            if let (Ok(filter), Ok(update)) = (
                update_doc.get_document("q"),
                update_doc.get_document("u"),
            ) {
                // Collect matching IDs first to avoid borrow issues with DashMap
                let matching_ids: Vec<String> = collection
                    .find(Some(filter))
                    .iter()
                    .filter_map(|d| d.get_object_id("_id").ok().map(|oid| oid.to_hex()))
                    .collect();
                for id in matching_ids {
                    if collection.update(&id, update) {
                        modified += 1;
                    }
                }
            }
        }

        doc! { "n": modified, "nModified": modified, "ok": 1 }
    }

    fn cmd_delete(&self, db_name: &str, command: &Document, sections: &[Section]) -> Document {
        let collection_name = match command.get_str("delete") {
            Ok(name) => name,
            Err(_) => return doc! { "ok": 0, "errmsg": "Missing collection name" },
        };

        // Deletes come from the "deletes" DocumentSequence section
        let deletes = Self::get_doc_sequence(sections, "deletes");

        let db = self.storage.get_database(db_name);
        let collection = db.get_collection(collection_name);

        let mut deleted = 0;
        for delete_doc in deletes {
            if let Ok(filter) = delete_doc.get_document("q") {
                // Collect matching IDs first to avoid borrow issues with DashMap
                let matching_ids: Vec<String> = collection
                    .find(Some(filter))
                    .iter()
                    .filter_map(|d| d.get_object_id("_id").ok().map(|oid| oid.to_hex()))
                    .collect();
                for id in matching_ids {
                    if collection.delete(&id) {
                        deleted += 1;
                    }
                }
            }
        }

        doc! { "n": deleted, "ok": 1 }
    }

    fn cmd_count(&self, db_name: &str, command: &Document) -> Document {
        let collection_name = match command.get_str("count") {
            Ok(name) => name,
            Err(_) => return doc! { "ok": 0, "errmsg": "Missing collection name" },
        };

        let db = self.storage.get_database(db_name);
        let collection = db.get_collection(collection_name);
        let count = collection.count() as i64;

        doc! { "n": count, "ok": 1 }
    }

    fn cmd_create(&self, db_name: &str, command: &Document) -> Document {
        let collection_name = match command.get_str("create") {
            Ok(name) => name,
            Err(_) => return doc! { "ok": 0, "errmsg": "Missing collection name" },
        };

        let db = self.storage.get_database(db_name);
        db.get_collection(collection_name); // Creates if doesn't exist

        doc! { "ok": 1 }
    }

    fn cmd_drop(&self, db_name: &str, command: &Document) -> Document {
        let collection_name = match command.get_str("drop") {
            Ok(name) => name,
            Err(_) => return doc! { "ok": 0, "errmsg": "Missing collection name" },
        };

        let db = self.storage.get_database(db_name);
        db.drop_collection(collection_name);

        doc! { "ok": 1 }
    }

    fn cmd_drop_database(&self, db_name: &str) -> Document {
        self.storage.drop_database(db_name);
        doc! { "ok": 1 }
    }

    /// Handle the "aggregate" command (used by count_documents, etc.)
    fn cmd_aggregate(
        &self,
        db_name: &str,
        command: &Document,
        sections: &[Section],
    ) -> Document {
        let collection_name = match command.get_str("aggregate") {
            Ok(name) => name,
            Err(_) => return doc! { "ok": 0, "errmsg": "Missing collection name" },
        };

        let pipeline = match command.get_array("pipeline") {
            Ok(p) => p,
            Err(_) => return doc! { "ok": 0, "errmsg": "Missing pipeline array" },
        };

        let db = self.storage.get_database(db_name);
        let collection = db.get_collection(collection_name);

        // Start with all documents
        let mut current_docs: Vec<Document> = collection.find(None);

        // Execute each pipeline stage sequentially
        for stage_value in pipeline {
            if let Some(stage) = stage_value.as_document() {
                if let Some(match_filter) = stage.get_document("$match").ok() {
                    let filter = match_filter.clone();
                    current_docs.retain(|doc| crate::storage::Collection::matches_filter(doc, &filter));
                } else if let Some(group_spec) = stage.get_document("$group").ok() {
                    current_docs = Self::apply_group_stage(&current_docs, group_spec);
                } else if let Ok(count_field) = stage.get_str("$count") {
                    let count = current_docs.len() as i64;
                    current_docs = vec![doc! { count_field: count }];
                } else if let Some(skip_val) = stage.get("$skip") {
                    let skip = Self::bson_to_i64(skip_val).unwrap_or(0) as usize;
                    if skip < current_docs.len() {
                        current_docs = current_docs[skip..].to_vec();
                    } else {
                        current_docs = vec![];
                    }
                } else if let Some(limit_val) = stage.get("$limit") {
                    let limit = Self::bson_to_i64(limit_val).unwrap_or(0) as usize;
                    current_docs.truncate(limit);
                }
                // Other stages ($project, $sort) not yet implemented
            }
        }

        doc! {
            "cursor": {
                "id": 0i64,
                "ns": format!("{}.{}", db_name, collection_name),
                "firstBatch": current_docs
            },
            "ok": 1
        }
    }

    /// Apply a $group stage to a set of documents
    fn apply_group_stage(docs: &[Document], group_spec: &Document) -> Vec<Document> {
        // Group documents by the _id expression
        let mut groups: std::collections::HashMap<String, Vec<&Document>> =
            std::collections::HashMap::new();

        let id_expr = group_spec.get("_id");

        for doc in docs {
            let group_key = match id_expr {
                // Field reference (e.g. _id: "$field_name")
                Some(Bson::String(field_ref)) if field_ref.starts_with('$') => {
                    let field_name = &field_ref[1..];
                    match crate::storage::Collection::get_field(doc, field_name) {
                        Some(v) => format!("{:?}", v),
                        None => "null".to_string(),
                    }
                }
                // Constant value (e.g. _id: 1 or _id: "constant") - all docs in one group
                Some(Bson::Int32(v)) => v.to_string(),
                Some(Bson::Int64(v)) => v.to_string(),
                Some(Bson::String(v)) => v.clone(),
                Some(Bson::Null) | None => "null".to_string(),
                _ => "default".to_string(),
            };
            groups.entry(group_key).or_default().push(doc);
        }

        // If no docs at all, still need to produce a result for $group
        if docs.is_empty() {
            // For $group with constant _id, empty input → empty output
            return vec![];
        }

        let mut result = Vec::new();
        for (_key, group_docs) in &groups {
            let mut output = Document::new();

            // Set the _id field
            match id_expr {
                Some(v) => output.insert("_id", v.clone()),
                None => output.insert("_id", Bson::Null),
            };

            // Process accumulator fields
            for (field, spec) in group_spec {
                if field == "_id" {
                    continue;
                }
                if let Some(acc_doc) = spec.as_document() {
                    if let Some(sum_spec) = acc_doc.get("$sum") {
                        let sum_value = match sum_spec {
                            // $sum: 1 → count documents
                            Bson::Int32(n) => Bson::Int64(*n as i64 * group_docs.len() as i64),
                            Bson::Int64(n) => Bson::Int64(*n * group_docs.len() as i64),
                            Bson::Double(n) => Bson::Double(*n * group_docs.len() as f64),
                            // $sum: "$field" → sum field values
                            Bson::String(field_ref) if field_ref.starts_with('$') => {
                                let field_name = &field_ref[1..];
                                let mut total: i64 = 0;
                                for d in group_docs {
                                    if let Some(Bson::Int32(v)) = d.get(field_name) {
                                        total += *v as i64;
                                    } else if let Some(Bson::Int64(v)) = d.get(field_name) {
                                        total += *v;
                                    }
                                }
                                Bson::Int64(total)
                            }
                            _ => Bson::Int32(0),
                        };
                        output.insert(field.clone(), sum_value);
                    }
                    // $avg, $min, $max, $push, $first, $last not yet implemented
                }
            }
            result.push(output);
        }

        result
    }

    /// Helper to convert Bson to i64
    fn bson_to_i64(val: &Bson) -> Option<i64> {
        match val {
            Bson::Int32(v) => Some(*v as i64),
            Bson::Int64(v) => Some(*v),
            Bson::Double(v) => Some(*v as i64),
            _ => None,
        }
    }

    fn cmd_get_log(&self, command: &Document) -> Document {
        let log_name = command.get_str("getLog").unwrap_or("global");

        let log_lines = match log_name {
            "*" => vec!["global", "startupWarnings"],
            _ => vec![],
        };

        doc! {
            "totalLinesWritten": log_lines.len() as i64,
            "log": log_lines,
            "ok": 1
        }
    }
}

impl MongoDBCommandHandler for DefaultMongoDBHandler {
    fn handle_message(&self, message: &Message) -> Message {
        let response_payload = match &message.payload {
            MessagePayload::OpMsg(op_msg) => {
                // Find the body (kind 0) section – it contains the command document
                let body_opt = op_msg.sections.iter().find_map(|s| match s {
                    Section::Body(doc) => Some(doc),
                    _ => None,
                });

                if let Some(command) = body_opt {
                    // Get database name from $db field
                    let db_name = command.get_str("$db").unwrap_or("test");

                    // Pass all sections so CRUD commands can extract document sequences
                    let response_doc =
                        self.handle_command(db_name, command, &op_msg.sections);
                    MessagePayload::OpMsg(OpMsg::new_body(response_doc))
                } else {
                    MessagePayload::OpMsg(OpMsg::new_body(
                        doc! { "ok": 0, "errmsg": "Invalid message" },
                    ))
                }
            }
            MessagePayload::OpQuery(query) => {
                // Legacy OP_QUERY – extract command from query document
                let db_and_collection = &query.full_collection_name;
                let parts: Vec<&str> = db_and_collection.splitn(2, '.').collect();
                let db_name = parts.first().copied().unwrap_or("test");

                // Check if it's a command (collection is $cmd)
                if parts.get(1) == Some(&"$cmd") {
                    let response_doc =
                        self.handle_command(db_name, &query.query, &[]);
                    MessagePayload::OpReply(OpReply::new_ok(0, vec![response_doc]))
                } else {
                    // Regular query
                    let collection_name = parts.get(1).copied().unwrap_or("");
                    let db = self.storage.get_database(db_name);
                    let collection = db.get_collection(collection_name);
                    let filter = query
                        .query
                        .get_document("query")
                        .ok()
                        .or_else(|| query.query.get_document("$query").ok());
                    let documents = collection.find(filter);
                    MessagePayload::OpReply(OpReply::new_ok(0, documents))
                }
            }
            _ => MessagePayload::OpMsg(OpMsg::new_body(
                doc! { "ok": 0, "errmsg": "Unsupported operation" },
            )),
        };

        Message::encode_reply(
            message.header.request_id.wrapping_add(1),
            message.header.request_id,
            response_payload,
        )
    }
}
