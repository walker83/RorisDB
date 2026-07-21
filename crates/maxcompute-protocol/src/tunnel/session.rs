//! MaxCompute Tunnel session management.
//!
//! Manages upload and download sessions with capacity limits, TTL eviction,
//! and integration with the QueryHandler for data ingestion/retrieval.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use mysql_protocol::server::QueryHandler;
use tracing::{info, warn};

use crate::tunnel::schema::{TunnelColumn, TunnelSchema};

// ============================================================================
// Constants
// ============================================================================

const MAX_SESSIONS: usize = 1000;
const EVICTION_TARGET: usize = 800;
const SESSION_TTL_SECS: i64 = 3600;

// ============================================================================
// Upload Session
// ============================================================================

/// Lifecycle status of an upload session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadStatus {
    Normal,
    Committed,
    Expired,
}

/// A single upload session tracking received data blocks.
#[derive(Debug, Clone)]
pub struct UploadSession {
    pub upload_id: String,
    pub project: String,
    pub table: String,
    pub schema: TunnelSchema,
    /// Block ID → rows (each row is Vec<Option<String>>)
    pub blocks: BTreeMap<u64, Vec<Vec<Option<String>>>>,
    pub status: UploadStatus,
    pub created_at: DateTime<Utc>,
    pub overwrite: bool,
}

impl UploadSession {
    /// Total records across all blocks.
    pub fn total_records(&self) -> usize {
        self.blocks.values().map(|v| v.len()).sum()
    }

    /// All block IDs that have been uploaded.
    pub fn block_ids(&self) -> Vec<u64> {
        self.blocks.keys().copied().collect()
    }

    /// Concatenate all blocks in order into a flat list of rows.
    pub fn all_rows(&self) -> Vec<Vec<Option<String>>> {
        self.blocks.values().flat_map(|v| v.clone()).collect()
    }
}

// ============================================================================
// Download Session
// ============================================================================

/// Lifecycle status of a download session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownloadStatus {
    Normal,
    Expired,
}

/// A download session with cached query results.
#[derive(Debug, Clone)]
pub struct DownloadSession {
    pub download_id: String,
    pub project: String,
    pub table: String,
    pub schema: TunnelSchema,
    pub record_count: u64,
    /// Cached SELECT * result (all rows)
    pub cached_data: Vec<Vec<Option<String>>>,
    pub status: DownloadStatus,
    pub created_at: DateTime<Utc>,
}

// ============================================================================
// Session Manager
// ============================================================================

/// Thread-safe registry of upload and download sessions.
pub struct TunnelSessionManager {
    upload_sessions: DashMap<String, UploadSession>,
    download_sessions: DashMap<String, DownloadSession>,
}

impl TunnelSessionManager {
    pub fn new() -> Self {
        Self {
            upload_sessions: DashMap::new(),
            download_sessions: DashMap::new(),
        }
    }

    // --- Upload sessions ---

    pub fn create_upload_session(
        &self,
        project: &str,
        table: &str,
        handler: &Arc<dyn QueryHandler>,
        conn_id: u32,
    ) -> Result<UploadSession, String> {
        // Evict if at capacity
        self.evict_if_needed();

        let schema = self.fetch_table_schema(handler, conn_id, project, table)?;

        let session = UploadSession {
            upload_id: uuid::Uuid::new_v4().to_string(),
            project: project.to_string(),
            table: table.to_string(),
            schema: schema.clone(),
            blocks: BTreeMap::new(),
            status: UploadStatus::Normal,
            created_at: Utc::now(),
            overwrite: false,
        };

        let upload_id = session.upload_id.clone();
        self.upload_sessions.insert(upload_id.clone(), session);
        info!("Created upload session {} for {}.{}", upload_id, project, table);

        // Look up to return a clone (DashMap guard doesn't impl Clone for entry)
        self.upload_sessions
            .get(&upload_id)
            .map(|r| r.value().clone())
            .ok_or_else(|| "failed to retrieve created session".to_string())
    }

    pub fn upload_block(
        &self,
        upload_id: &str,
        block_id: u64,
        rows: Vec<Vec<Option<String>>>,
    ) -> Result<(), String> {
        if let Some(mut entry) = self.upload_sessions.get_mut(upload_id) {
            if entry.status != UploadStatus::Normal {
                return Err(format!(
                    "Upload session {} is not in Normal state (current: {:?})",
                    upload_id, entry.status
                ));
            }
            entry.blocks.insert(block_id, rows);
            Ok(())
        } else {
            Err(format!("Upload session not found: {}", upload_id))
        }
    }

    pub fn commit_upload(
        &self,
        upload_id: &str,
        handler: &Arc<dyn QueryHandler>,
        conn_id: u32,
    ) -> Result<Vec<u64>, String> {
        let session = self
            .upload_sessions
            .get(upload_id)
            .map(|r| r.clone())
            .ok_or_else(|| format!("Upload session not found: {}", upload_id))?;

        if session.status != UploadStatus::Normal {
            return Err(format!(
                "Upload session {} cannot be committed (status: {:?})",
                upload_id, session.status
            ));
        }

        let block_ids = session.block_ids();
        let all_rows = session.all_rows();

        if all_rows.is_empty() {
            // Nothing to commit, just mark as committed
            if let Some(mut entry) = self.upload_sessions.get_mut(upload_id) {
                entry.status = UploadStatus::Committed;
            }
            return Ok(block_ids);
        }

        // Build INSERT SQL from rows
        let insert_sql = self.build_insert_sql(&session, &all_rows)?;
        tracing::debug!(
            "Tunnel commit: inserting {} rows into {}.{} via SQL",
            all_rows.len(),
            session.project,
            session.table
        );

        // Set database context
        handler.set_database(conn_id, &session.project);

        // Execute the INSERT
        let result = handler.handle_query(conn_id, &insert_sql);

        // Check for error in result
        let has_error = result.columns.iter().any(|c| {
            c.name.to_uppercase().contains("ERROR")
                || c.name.to_uppercase().contains("PARSE")
        });

        if has_error {
            let error_msg = result
                .rows
                .first()
                .and_then(|r| r.first())
                .and_then(|v| v.clone())
                .unwrap_or_else(|| "unknown error".to_string());
            warn!("Tunnel commit failed for {}: {}", upload_id, error_msg);
            return Err(error_msg);
        }

        // Mark as committed
        if let Some(mut entry) = self.upload_sessions.get_mut(upload_id) {
            entry.status = UploadStatus::Committed;
        }

        info!(
            "Committed upload session {}: {} rows inserted into {}.{}",
            upload_id,
            all_rows.len(),
            session.project,
            session.table
        );

        Ok(block_ids)
    }

    /// Build an INSERT SQL statement from session data.
    fn build_insert_sql(&self, session: &UploadSession, rows: &[Vec<Option<String>>]) -> Result<String, String> {
        let columns: Vec<&TunnelColumn> = session.schema.all_columns();
        let col_names: Vec<String> = columns.iter().map(|c| format!("`{}`", c.name)).collect();

        let mut values_rows: Vec<String> = Vec::with_capacity(rows.len());

        for row in rows {
            let mut field_values: Vec<String> = Vec::with_capacity(row.len());
            for (col_idx, val) in row.iter().enumerate() {
                let col = &columns[col_idx];
                let sql_val = match val {
                    None => "NULL".to_string(),
                    Some(v) => self.format_sql_value(v, &col.odps_type),
                };
                field_values.push(sql_val);
            }
            values_rows.push(format!("({})", field_values.join(", ")));
        }

        let table_name = format!("`{}`.`{}`", session.project, session.table);
        let values_clause = values_rows.join(", ");

        Ok(format!(
            "INSERT INTO {} ({}) VALUES {}",
            table_name,
            col_names.join(", "),
            values_clause
        ))
    }

    /// Format a string value as an SQL literal based on the ODPS type.
    fn format_sql_value(&self, value: &str, odps_type: &str) -> String {
        match odps_type.to_uppercase().as_str() {
            "BIGINT" | "INT" | "SMALLINT" | "TINYINT" | "FLOAT" | "DOUBLE" | "REAL" | "DECIMAL" | "NUMERIC" => {
                value.to_string()
            }
            "BOOLEAN" => {
                if value == "1" || value.eq_ignore_ascii_case("true") {
                    "1".to_string()
                } else {
                    "0".to_string()
                }
            }
            "DATETIME" | "TIMESTAMP" | "DATE" => {
                format!("'{}'", value)
            }
            "STRING" | "VARCHAR" | "CHAR" | "TEXT" => {
                // Escape single quotes
                format!("'{}'", value.replace('\'', "''"))
            }
            _ => {
                format!("'{}'", value.replace('\'', "''"))
            }
        }
    }

    /// Fetch table schema by executing DESCRIBE.
    fn fetch_table_schema(
        &self,
        handler: &Arc<dyn QueryHandler>,
        conn_id: u32,
        project: &str,
        table: &str,
    ) -> Result<TunnelSchema, String> {
        handler.set_database(conn_id, project);
        let result = handler.handle_query(conn_id, &format!("DESCRIBE `{}`", table.replace('`', "``")));

        if result.columns.len() < 2 {
            return Err(format!(
                "DESCRIBE {} returned {} columns (expected >= 2)",
                table,
                result.columns.len()
            ));
        }

        // Check for error
        if result.columns[0].name.to_uppercase().contains("ERROR") {
            let error_msg = result
                .rows
                .first()
                .and_then(|r| r.first())
                .and_then(|v| v.clone())
                .unwrap_or_else(|| "unknown error".to_string());
            return Err(error_msg);
        }

        let mut columns: Vec<TunnelColumn> = Vec::new();
        let mut partition_keys: Vec<TunnelColumn> = Vec::new();

        for row in &result.rows {
            if row.len() < 2 {
                continue;
            }
            let col_name = row[0].clone().unwrap_or_default();
            let col_type_raw = row[1].clone().unwrap_or_else(|| "STRING".to_string());
            let odps_type = crate::tunnel::schema::mysql_to_odps_type(&col_type_raw);

            let col = TunnelColumn {
                name: col_name.clone(),
                odps_type: odps_type.to_string(),
                nullable: true,
                comment: if row.len() > 3 { row.get(3).and_then(|v| v.clone()) } else { None },
            };

            // Check if this is a partition key
            let is_partition = row.len() > 2
                && row[2].as_ref().map(|v| v.to_uppercase().contains("PRI")).unwrap_or(false);

            if is_partition {
                partition_keys.push(col);
            } else {
                columns.push(col);
            }
        }

        Ok(TunnelSchema {
            columns,
            partition_keys,
        })
    }

    // --- Download sessions ---

    pub fn create_download_session(
        &self,
        project: &str,
        table: &str,
        handler: &Arc<dyn QueryHandler>,
        conn_id: u32,
    ) -> Result<DownloadSession, String> {
        self.evict_if_needed();

        let schema = self.fetch_table_schema(handler, conn_id, project, table)?;

        // Fetch all data
        handler.set_database(conn_id, project);
        let result = handler.handle_query(conn_id, &format!("SELECT * FROM `{}`", table.replace('`', "``")));

        // Check for error
        if !result.columns.is_empty()
            && result.columns[0].name.to_uppercase().contains("ERROR")
        {
            let error_msg = result
                .rows
                .first()
                .and_then(|r| r.first())
                .and_then(|v| v.clone())
                .unwrap_or_else(|| "unknown error".to_string());
            return Err(error_msg);
        }

        let record_count = result.rows.len() as u64;
        let cached_data = result.rows;

        let session = DownloadSession {
            download_id: uuid::Uuid::new_v4().to_string(),
            project: project.to_string(),
            table: table.to_string(),
            schema,
            record_count,
            cached_data,
            status: DownloadStatus::Normal,
            created_at: Utc::now(),
        };

        let download_id = session.download_id.clone();
        self.download_sessions.insert(download_id.clone(), session);
        info!(
            "Created download session {} for {}.{} ({} records)",
            download_id, project, table, record_count
        );

        self.download_sessions
            .get(&download_id)
            .map(|r| r.value().clone())
            .ok_or_else(|| "failed to retrieve created session".to_string())
    }

    pub fn get_download_data(
        &self,
        download_id: &str,
        row_start: u64,
        row_count: u64,
    ) -> Result<Vec<Vec<Option<String>>>, String> {
        let session = self
            .download_sessions
            .get(download_id)
            .map(|r| r.value().clone())
            .ok_or_else(|| format!("Download session not found: {}", download_id))?;

        if session.status != DownloadStatus::Normal {
            return Err(format!(
                "Download session {} is not in Normal state",
                download_id
            ));
        }

        let start = row_start as usize;
        let end = (start + row_count as usize).min(session.cached_data.len());

        if start >= session.cached_data.len() {
            return Ok(vec![]);
        }

        Ok(session.cached_data[start..end].to_vec())
    }

    // --- Session reload (status queries) ---

    pub fn reload_upload_session(
        &self,
        upload_id: &str,
    ) -> Option<(UploadSession, Vec<u64>)> {
        self.upload_sessions
            .get(upload_id)
            .map(|r| {
                let session = r.value().clone();
                let block_ids = session.block_ids();
                (session, block_ids)
            })
    }

    /// Get the schema for an upload session (used by upload_block handler).
    pub fn get_upload_session_schema(&self, upload_id: &str) -> Option<TunnelSchema> {
        self.upload_sessions
            .get(upload_id)
            .map(|r| r.value().schema.clone())
    }

    /// Get a download session by ID.
    pub fn get_download_session(&self, download_id: &str) -> Option<DownloadSession> {
        self.download_sessions
            .get(download_id)
            .map(|r| r.value().clone())
    }

    /// Reload download session status (same as get_download_session, for API symmetry).
    pub fn reload_download_session(&self, download_id: &str) -> Option<DownloadSession> {
        self.get_download_session(download_id)
    }

    // --- Capacity management ---

    fn evict_if_needed(&self) {
        if self.total_sessions() >= MAX_SESSIONS {
            self.evict_oldest(EVICTION_TARGET);
        }
    }

    fn total_sessions(&self) -> usize {
        self.upload_sessions.len() + self.download_sessions.len()
    }

    fn evict_oldest(&self, target: usize) {
        if self.total_sessions() <= target {
            return;
        }

        // Collect all sessions with their creation times
        let mut all_sessions: Vec<(String, DateTime<Utc>, bool)> = Vec::new(); // (id, created_at, is_upload)

        for entry in self.upload_sessions.iter() {
            all_sessions.push((entry.key().clone(), entry.value().created_at, true));
        }
        for entry in self.download_sessions.iter() {
            all_sessions.push((entry.key().clone(), entry.value().created_at, false));
        }

        all_sessions.sort_by(|a, b| a.1.cmp(&b.1));

        let to_remove = self.total_sessions() - target;
        for (id, _, is_upload) in all_sessions.iter().take(to_remove) {
            if *is_upload {
                self.upload_sessions.remove(id);
            } else {
                self.download_sessions.remove(id);
            }
        }
    }

    /// Remove expired sessions (older than TTL).
    pub fn cleanup(&self) {
        let cutoff = Utc::now() - Duration::seconds(SESSION_TTL_SECS);

        let expired_upload_ids: Vec<String> = self
            .upload_sessions
            .iter()
            .filter(|r| r.value().created_at < cutoff)
            .map(|r| r.key().clone())
            .collect();

        for id in expired_upload_ids {
            self.upload_sessions.remove(&id);
        }

        let expired_download_ids: Vec<String> = self
            .download_sessions
            .iter()
            .filter(|r| r.value().created_at < cutoff)
            .map(|r| r.key().clone())
            .collect();

        for id in expired_download_ids {
            self.download_sessions.remove(&id);
        }
    }

    pub fn len(&self) -> usize {
        self.total_sessions()
    }

    pub fn is_empty(&self) -> bool {
        self.total_sessions() == 0
    }
}

impl Default for TunnelSessionManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upload_session_total_records() {
        let session = UploadSession {
            upload_id: "test".into(),
            project: "default".into(),
            table: "t1".into(),
            schema: TunnelSchema::empty(),
            blocks: BTreeMap::from([
                (0, vec![vec![Some("1".into())], vec![Some("2".into())]]),
                (1, vec![vec![Some("3".into())]]),
            ]),
            status: UploadStatus::Normal,
            created_at: Utc::now(),
            overwrite: false,
        };
        assert_eq!(session.total_records(), 3);
    }

    #[test]
    fn test_upload_session_all_rows() {
        let session = UploadSession {
            upload_id: "test".into(),
            project: "default".into(),
            table: "t1".into(),
            schema: TunnelSchema::empty(),
            blocks: BTreeMap::from([
                (1, vec![vec![Some("b".into())]]),
                (0, vec![vec![Some("a".into())]]),
            ]),
            status: UploadStatus::Normal,
            created_at: Utc::now(),
            overwrite: false,
        };
        let rows = session.all_rows();
        // BTreeMap iterates in key order
        assert_eq!(rows[0][0].as_deref(), Some("a"));
        assert_eq!(rows[1][0].as_deref(), Some("b"));
    }

    #[test]
    fn test_upload_session_block_ids_sorted() {
        let session = UploadSession {
            upload_id: "test".into(),
            project: "default".into(),
            table: "t1".into(),
            schema: TunnelSchema::empty(),
            blocks: BTreeMap::from([
                (5, vec![]),
                (2, vec![]),
                (8, vec![]),
            ]),
            status: UploadStatus::Normal,
            created_at: Utc::now(),
            overwrite: false,
        };
        let ids = session.block_ids();
        assert_eq!(ids, vec![2, 5, 8]);
    }

    #[test]
    fn test_session_manager_new_is_empty() {
        let mgr = TunnelSessionManager::new();
        assert!(mgr.is_empty());
        assert_eq!(mgr.len(), 0);
    }

    #[test]
    fn test_session_manager_default() {
        let mgr = TunnelSessionManager::default();
        assert!(mgr.is_empty());
    }

    #[test]
    fn test_sql_value_formatting() {
        let mgr = TunnelSessionManager::new();
        assert_eq!(mgr.format_sql_value("42", "BIGINT"), "42");
        assert_eq!(mgr.format_sql_value("3.14", "DOUBLE"), "3.14");
        assert_eq!(mgr.format_sql_value("1", "BOOLEAN"), "1");
        assert_eq!(mgr.format_sql_value("0", "BOOLEAN"), "0");
        assert_eq!(mgr.format_sql_value("TRUE", "BOOLEAN"), "1");
        assert_eq!(mgr.format_sql_value("hello", "STRING"), "'hello'");
        assert_eq!(mgr.format_sql_value("it's", "STRING"), "'it''s'");
        assert_eq!(mgr.format_sql_value("2024-01-01", "DATE"), "'2024-01-01'");
    }
}
