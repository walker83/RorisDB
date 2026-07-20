//! Cassandra command handler

use crate::frame::{Frame, Opcode};
use crate::storage::CassandraStorage;
use std::sync::Arc;

/// Trait for handling Cassandra commands
pub trait CassandraCommandHandler: Send + Sync {
    fn handle_startup(&self) -> Vec<u8>;
    fn handle_query(&self, keyspace: &mut String, cql: &str) -> Vec<u8>;
}

/// Build a RESULT frame with VOID result kind (for DDL, INSERT, UPDATE, DELETE)
fn build_void_result(stream: i16) -> Vec<u8> {
    let mut body = Vec::with_capacity(4);
    body.extend_from_slice(&0x0000_0001u32.to_be_bytes()); // Void
    let frame = Frame::new(0x84, stream, Opcode::Result, body);
    let mut buf = bytes::BytesMut::new();
    frame.encode(&mut buf);
    buf.to_vec()
}

/// Build a RESULT frame with ROWS result kind (for SELECT)
fn build_rows_result(stream: i16, columns: &[&str], rows: &[Vec<String>]) -> Vec<u8> {
    let mut body = Vec::new();
    // Result kind = ROWS (0x0002)
    body.extend_from_slice(&0x0000_0002u32.to_be_bytes());
    // Column count (i32)
    body.extend_from_slice(&(columns.len() as i32).to_be_bytes());
    // Row count (i32)
    body.extend_from_slice(&(rows.len() as i32).to_be_bytes());
    // Row data: each cell is [i32 length][bytes]
    for row in rows {
        for val in row {
            let val_bytes = val.as_bytes();
            body.extend_from_slice(&(val_bytes.len() as i32).to_be_bytes());
            body.extend_from_slice(val_bytes);
        }
    }
    let frame = Frame::new(0x84, stream, Opcode::Result, body);
    let mut buf = bytes::BytesMut::new();
    frame.encode(&mut buf);
    buf.to_vec()
}

/// Build a RESULT frame with SET_KEYSPACE result kind (for USE)
fn build_set_keyspace_result(stream: i16, keyspace: &str) -> Vec<u8> {
    let mut body = Vec::new();
    // Result kind = SET_KEYSPACE (0x0003)
    body.extend_from_slice(&0x0000_0003u32.to_be_bytes());
    // [string] keyspace name: [u16 len][bytes]
    let ks_bytes = keyspace.as_bytes();
    body.extend_from_slice(&(ks_bytes.len() as u16).to_be_bytes());
    body.extend_from_slice(ks_bytes);
    let frame = Frame::new(0x84, stream, Opcode::Result, body);
    let mut buf = bytes::BytesMut::new();
    frame.encode(&mut buf);
    buf.to_vec()
}

/// Build an ERROR frame
fn build_error_frame(stream: i16, code: i32, message: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&code.to_be_bytes());
    let msg_bytes = message.as_bytes();
    body.extend_from_slice(&(msg_bytes.len() as u16).to_be_bytes());
    body.extend_from_slice(msg_bytes);
    let frame = Frame::new(0x84, stream, Opcode::Error, body);
    let mut buf = bytes::BytesMut::new();
    frame.encode(&mut buf);
    buf.to_vec()
}

/// Default Cassandra command handler
pub struct DefaultCassandraHandler {
    storage: Arc<CassandraStorage>,
}

impl DefaultCassandraHandler {
    pub fn new(storage: Arc<CassandraStorage>) -> Self {
        Self { storage }
    }
}

impl CassandraCommandHandler for DefaultCassandraHandler {
    fn handle_startup(&self) -> Vec<u8> {
        // Return READY frame (version 0x84 = response v4)
        let frame = Frame::new(0x84, 0, Opcode::Ready, vec![]);
        let mut buf = bytes::BytesMut::new();
        frame.encode(&mut buf);
        buf.to_vec()
    }

    fn handle_query(&self, keyspace: &mut String, cql: &str) -> Vec<u8> {
        let cql_trimmed = cql.trim().trim_end_matches(';');
        let upper = cql_trimmed.to_uppercase();
        let stream: i16 = 0;

        // SELECT queries
        if upper.starts_with("SELECT") {
            return self.handle_select(stream, keyspace, cql_trimmed, &upper);
        }

        // USE keyspace
        if upper.starts_with("USE ") {
            let parts: Vec<&str> = cql_trimmed.split_whitespace().collect();
            if parts.len() >= 2 {
                let ks = parts[1].to_string();
                *keyspace = ks.clone();
                return build_set_keyspace_result(stream, &ks);
            }
            return build_error_frame(stream, 0x2200, "Invalid USE statement");
        }

        // CREATE KEYSPACE
        if upper.starts_with("CREATE KEYSPACE") {
            let parts: Vec<&str> = cql_trimmed.split_whitespace().collect();
            // CREATE KEYSPACE [IF NOT EXISTS] name ...
            let ks_name = if upper.contains("IF NOT EXISTS") {
                // parts: CREATE KEYSPACE IF NOT EXISTS name ...
                parts.get(4).copied().unwrap_or("unknown")
            } else {
                parts.get(2).copied().unwrap_or("unknown")
            };
            self.storage.create_keyspace(ks_name);
            return build_void_result(stream);
        }

        // CREATE TABLE
        if upper.starts_with("CREATE TABLE") {
            // Parse table name (may be keyspace.table)
            let parts: Vec<&str> = cql_trimmed.split_whitespace().collect();
            let table_part = if upper.contains("IF NOT EXISTS") {
                parts.get(4).copied().unwrap_or("unknown")
            } else {
                parts.get(2).copied().unwrap_or("unknown")
            };
            let (ks, table) = parse_table_name(table_part, keyspace);
            let ks_obj = match self.storage.get_keyspace(&ks) { Some(ks) => ks, None => return build_error_frame(0, 0x2200, &format!("Keyspace {} not found", ks)) };
            ks_obj.create_table(&table);
            return build_void_result(stream);
        }

        // DROP TABLE
        if upper.starts_with("DROP TABLE") {
            // Simplified: just return VOID
            return build_void_result(stream);
        }

        // DROP KEYSPACE
        if upper.starts_with("DROP KEYSPACE") {
            return build_void_result(stream);
        }

        // INSERT INTO ks.table (col1, col2) VALUES (val1, val2)
        if upper.starts_with("INSERT") {
            if let Some(result) = self.handle_insert(keyspace, &upper, cql) {
                return result;
            }
            return build_void_result(stream);
        }

        // UPDATE ks.table SET col = val WHERE key = 'value'
        if upper.starts_with("UPDATE") {
            if let Some(result) = self.handle_update(keyspace, &upper, cql) {
                return result;
            }
            return build_void_result(stream);
        }

        // DELETE FROM ks.table WHERE key = 'value'
        if upper.starts_with("DELETE") {
            if let Some(result) = self.handle_delete(keyspace, &upper, cql) {
                return result;
            }
            return build_void_result(stream);
        }

        // DESCRIBE
        if upper.starts_with("DESCRIBE") || upper.starts_with("DESC") {
            return self.handle_describe(stream, keyspace, &upper);
        }

        // Default: VOID
        build_void_result(stream)
    }
}

impl DefaultCassandraHandler {
    fn handle_select(&self, stream: i16, keyspace: &str, cql: &str, upper: &str) -> Vec<u8> {
        // system.local
        if upper.contains("FROM SYSTEM.LOCAL") || upper.contains("FROM SYSTEM.LOCAL ") {
            let columns = &["key", "cluster_name", "cql_version", "release_version"];
            let rows = vec![vec![
                "local".to_string(),
                "HarnessDB".to_string(),
                "3.4.5".to_string(),
                "HarnessDB-1.1.0".to_string(),
            ]];
            return build_rows_result(stream, columns, &rows);
        }

        // system.peers
        if upper.contains("FROM SYSTEM.PEERS") {
            let columns = &["peer", "data_center", "rack", "release_version"];
            return build_rows_result(stream, columns, &[]);
        }

        // SELECT COUNT(*)
        if upper.contains("COUNT(*)") && upper.contains("FROM ") {
            let from_idx = upper.find("FROM ").unwrap();
            let after_from = &cql[from_idx + 5..].trim();
            let table_part = after_from.split_whitespace().next().unwrap_or("unknown");
            let (ks, table) = parse_table_name(table_part, keyspace);
            if let Some(ks_obj) = self.storage.get_keyspace(&ks) {
                if let Some(cf) = ks_obj.get_table(&table) {
                    let count = cf.count();
                    let columns = &["count"];
                    let rows = vec![vec![count.to_string()]];
                    return build_rows_result(stream, columns, &rows);
                }
            }
            let columns = &["count"];
            let rows = vec![vec!["0".to_string()]];
            return build_rows_result(stream, columns, &rows);
        }

        // Generic SELECT from user tables
        if upper.contains("FROM ") {
            let from_idx = upper.find("FROM ").unwrap();
            let after_from = &cql[from_idx + 5..].trim();
            let table_part = after_from
                .split_whitespace()
                .next()
                .unwrap_or("unknown");

            let (ks, table) = parse_table_name(table_part, keyspace);
            let ks_obj = match self.storage.get_keyspace(&ks) { Some(ks) => ks, None => return build_error_frame(stream, 0x2200, &format!("Keyspace '{}' not found", ks)) };

            if let Some(cf) = ks_obj.get_table(&table) {
                let all_rows = cf.select(None);
                let columns = extract_select_columns(cql, upper);
                let string_rows: Vec<Vec<String>> = all_rows.iter().map(|row| {
                    columns.iter().map(|col| row.get(*col).cloned().unwrap_or_default()).collect()
                }).collect();
                return build_rows_result(stream, &columns, &string_rows);
            }

            let columns = extract_select_columns(cql, upper);
            return build_rows_result(stream, &columns, &[]);
        }

        // Fallback: empty rows
        build_rows_result(stream, &["result"], &[])
    }

    fn handle_describe(&self, stream: i16, _keyspace: &str, upper: &str) -> Vec<u8> {
        if upper.contains("KEYSPACES") {
            let columns = &["keyspace_name"];
            let keyspaces = self.storage.list_keyspaces();
            let rows: Vec<Vec<String>> = keyspaces
                .iter()
                .map(|k| vec![k.clone()])
                .collect();
            return build_rows_result(stream, columns, &rows);
        }

        if upper.contains("TABLES") {
            let columns = &["table_name"];
            return build_rows_result(stream, columns, &[]);
        }

        // DESCRIBE TABLE
        let columns = &["column_name", "type"];
        return build_rows_result(stream, columns, &[]);
    }

    fn handle_insert(&self, keyspace: &str, upper: &str, cql: &str) -> Option<Vec<u8>> {
        // INSERT INTO ks.table (col1, col2) VALUES (val1, val2)
        let into_pos = upper.find("INTO ")?;
        let after_into = &cql[into_pos + 5..].trim();
        let paren_pos = after_into.find('(')?;
        let table_part = after_into[..paren_pos].trim();
        let (ks, table) = parse_table_name(table_part, keyspace);
        let ks_obj = self.storage.get_keyspace(&ks)?;
        let cf = ks_obj.get_table(&table)?;

        // Parse column names
        let cols_end = after_into.find(')')?;
        let cols_str = &after_into[1..cols_end];
        let columns: Vec<&str> = cols_str.split(',').map(|s| s.trim()).collect();

        // Parse VALUES
        let values_start = after_into.find("VALUES")?;
        let after_values = &after_into[values_start + 6..].trim();
        let vals_start = after_values.find('(')?;
        let vals_end = after_values.find(')')?;
        let vals_str = &after_values[vals_start + 1..vals_end];
        let values: Vec<&str> = vals_str.split(',').map(|s| s.trim().trim_matches('\'')).collect();

        // Build row and insert
        let mut row = std::collections::HashMap::new();
        for (i, col) in columns.iter().enumerate() {
            if let Some(val) = values.get(i) {
                row.insert(col.to_string(), val.to_string());
            }
        }
        // Use first column as key (simplified)
        let key = values.first()?.to_string();
        cf.insert(key, row);
        Some(build_void_result(0))
    }

    fn handle_delete(&self, keyspace: &str, upper: &str, cql: &str) -> Option<Vec<u8>> {
        // DELETE FROM ks.table WHERE key = 'value'
        let from_pos = upper.find("FROM ")?;
        let after_from = &cql[from_pos + 5..].trim();
        let where_pos = after_from.to_uppercase().find(" WHERE ")?;
        let table_part = after_from[..where_pos].trim();
        let (ks, table) = parse_table_name(table_part, keyspace);
        let ks_obj = self.storage.get_keyspace(&ks)?;
        let cf = ks_obj.get_table(&table)?;

        // Parse WHERE key = 'value'
        let where_clause = &after_from[where_pos + 7..].trim();
        if let Some(eq_pos) = where_clause.find('=') {
            let val = where_clause[eq_pos + 1..].trim().trim_matches('\'').trim();
            cf.delete(val);
        }
        Some(build_void_result(0))
    }

    fn handle_update(&self, keyspace: &str, upper: &str, cql: &str) -> Option<Vec<u8>> {
        // UPDATE ks.table SET col = val WHERE key = 'value'
        let table_start = upper.find("UPDATE ")? + 7;
        let set_pos = upper.find(" SET ")?;
        let table_part = cql[table_start..set_pos].trim();
        let (ks, table) = parse_table_name(table_part, keyspace);
        let ks_obj = self.storage.get_keyspace(&ks)?;
        let cf = ks_obj.get_table(&table)?;

        let where_pos = upper.find(" WHERE ")?;
        let set_clause = &cql[set_pos + 5..where_pos].trim();
        let where_clause = &cql[where_pos + 7..].trim();

        // Parse WHERE key = 'value'
        if let Some(eq_pos) = where_clause.find('=') {
            let key = where_clause[eq_pos + 1..].trim().trim_matches('\'').trim();
            // Get existing row and update
            if let Some(mut row) = cf.select(Some(key)).into_iter().next() {
                // Parse SET col = val
                if let Some(set_eq) = set_clause.find('=') {
                    let col = set_clause[..set_eq].trim();
                    let val = set_clause[set_eq + 1..].trim().trim_matches('\'').trim();
                    row.insert(col.to_string(), val.to_string());
                    cf.insert(key.to_string(), row);
                }
            }
        }
        Some(build_void_result(0))
    }
}

/// Parse "keyspace.table" or just "table" (using current keyspace)
fn parse_table_name<'a>(name: &'a str, default_ks: &'a str) -> (String, String) {
    if let Some(dot_pos) = name.find('.') {
        let ks = &name[..dot_pos];
        let table = &name[dot_pos + 1..];
        (ks.to_string(), table.to_string())
    } else {
        (default_ks.to_string(), name.to_string())
    }
}

/// Extract column names from a SELECT clause
fn extract_select_columns<'a>(cql: &'a str, upper: &str) -> Vec<&'a str> {
    // Simple parser: find text between SELECT and FROM
    let select_start = if upper.starts_with("SELECT ") { 7 } else { return vec!["col"] };
    let from_pos = match upper.find(" FROM ") {
        Some(p) => p,
        None => return vec!["col"],
    };

    let col_part = &cql[select_start..from_pos];
    let cols: Vec<&str> = col_part
        .split(',')
        .map(|c| c.trim())
        .filter(|c| !c.is_empty())
        .collect();

    if cols.is_empty() {
        vec!["col"]
    } else {
        cols
    }
}
