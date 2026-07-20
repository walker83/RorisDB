//! PostgreSQL wire protocol connection state machine.
//!
//! Implements the full connection lifecycle: startup, authentication,
//! simple query, and extended query protocols.

use bytes::{Buf, BufMut, BytesMut};
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, error, info, warn};

use crate::auth::{AuthConfig, generate_salt, validate_password};
use crate::message::{
    BackendMessage, CANCEL_REQUEST_CODE, DescribeTarget, FieldDescription, FrontendMessage,
    OID_BOOL, OID_DATE, OID_FLOAT4, OID_FLOAT8, OID_INT4, OID_TEXT, OID_TIMESTAMP,
    PG_PROTOCOL_VERSION_3, PgProtocolError, SSL_REQUEST_CODE, TransactionStatus,
    create_error_response, sqlstate,
};
use mysql_protocol::server::{ColumnType, QueryHandler, QueryResult};

/// Maximum size of a single PG protocol message (16 MB).
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// A stored prepared statement.
struct PreparedStatement {
    sql: String,
    param_types: Vec<u32>,
    /// Column fields, populated lazily after the first execute.
    fields: Vec<FieldDescription>,
}

/// Session state for a single PG connection.
struct SessionState {
    database: String,
    username: String,
    prepared_statements: HashMap<String, PreparedStatement>,
    portals: HashMap<String, String>,
    _start_time: Instant,
}

/// A single PostgreSQL wire protocol connection.
pub struct PgConnection {
    stream: TcpStream,
    conn_id: u32,
    handler: Arc<dyn QueryHandler>,
    read_buf: BytesMut,
    write_buf: BytesMut,
    auth_config: AuthConfig,
    session: SessionState,
    process_id: i32,
    secret_key: i32,
}

impl PgConnection {
    pub fn new(
        stream: TcpStream,
        conn_id: u32,
        handler: Arc<dyn QueryHandler>,
        auth_config: AuthConfig,
    ) -> Self {
        let mut rng = rand::thread_rng();
        // TODO: set TCP keepalive (requires platform-specific APIs)
        stream.set_nodelay(true).ok();
        Self {
            stream,
            conn_id,
            handler,
            read_buf: BytesMut::with_capacity(8192),
            write_buf: BytesMut::with_capacity(8192),
            auth_config,
            session: SessionState {
                database: String::new(),
                username: String::new(),
                prepared_statements: HashMap::new(),
                portals: HashMap::new(),
                _start_time: Instant::now(),
            },
            process_id: rng.gen_range(10000..99999),
            secret_key: rng.gen_range(100000..999999),
        }
    }

    pub async fn run(&mut self) -> Result<(), PgProtocolError> {
        let result = self.run_inner().await;
        self.handler.on_disconnect(self.conn_id);
        result
    }

    async fn run_inner(&mut self) -> Result<(), PgProtocolError> {
        self.handle_startup().await?;
        // Call on_connect after startup so the actual username is available
        let peer_addr = self
            .stream
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        self.handler
            .on_connect(self.conn_id, &self.session.username, &peer_addr);
        self.handle_auth().await?;
        self.send_parameter_status().await?;
        self.send_backend_key_data().await?;
        self.send_ready_for_query().await?;
        self.handle_messages().await
    }

    // ======================================================================
    // Startup
    // ======================================================================

    async fn handle_startup(&mut self) -> Result<(), PgProtocolError> {
        loop {
            self.read_buf_ensure(4).await?;
            let len = (&self.read_buf[..4]).get_i32() as usize;
            if len < 4 {
                return Err(PgProtocolError::ProtocolViolation(
                    "startup message length too small".to_string(),
                ));
            }
            self.read_buf_ensure(len).await?;
            if len < 8 {
                return Err(PgProtocolError::ProtocolViolation(
                    "startup message too short".to_string(),
                ));
            }
            let version = (&self.read_buf[4..8]).get_i32();

            match version {
                PG_PROTOCOL_VERSION_3 => {
                    let msg =
                        FrontendMessage::decode_startup(&mut self.read_buf)?.ok_or_else(|| {
                            PgProtocolError::ProtocolViolation(
                                "incomplete startup message".to_string(),
                            )
                        })?;
                    match msg {
                        FrontendMessage::StartupMessage { params, .. } => {
                            self.session.username = params
                                .get("user")
                                .cloned()
                                .unwrap_or_else(|| "root".to_string());
                            self.session.database =
                                params.get("database").cloned().unwrap_or_default();
                            info!(
                                "PG conn {}: startup user='{}' database='{}'",
                                self.conn_id, self.session.username, self.session.database
                            );
                            return Ok(());
                        }
                        _ => unreachable!(),
                    }
                }
                SSL_REQUEST_CODE => {
                    debug!("PG conn {}: SSL declined", self.conn_id);
                    self.read_buf.advance(len);
                    self.write_buf.put_u8(b'N');
                    self.flush_write().await?;
                    continue;
                }
                CANCEL_REQUEST_CODE => {
                    if len >= 16 {
                        let pid = (&self.read_buf[8..12]).get_i32();
                        let key = (&self.read_buf[12..16]).get_i32();
                        warn!(
                            "PG conn {}: cancel request pid={} key={}",
                            self.conn_id, pid, key
                        );
                    }
                    return Err(PgProtocolError::CancelRequest);
                }
                _ => {
                    return Err(PgProtocolError::ProtocolViolation(format!(
                        "unknown protocol version: {}",
                        version
                    )));
                }
            }
        }
    }

    // ======================================================================
    // Authentication
    // ======================================================================

    async fn handle_auth(&mut self) -> Result<(), PgProtocolError> {
        let salt = generate_salt();

        BackendMessage::AuthenticationMD5Password { salt }.encode(&mut self.write_buf);
        self.flush_write().await?;

        self.read_buf_ensure(5).await?;
        let msg = FrontendMessage::decode(&mut self.read_buf)?.ok_or_else(|| {
            PgProtocolError::ProtocolViolation("incomplete password message".to_string())
        })?;

        let password = match msg {
            FrontendMessage::PasswordMessage { password } => password,
            other => {
                return Err(PgProtocolError::ProtocolViolation(format!(
                    "expected PasswordMessage, got {:?}",
                    other
                )));
            }
        };

        if !validate_password(&self.auth_config, &self.session.username, &password, &salt) {
            warn!(
                "PG conn {}: auth failed for user '{}'",
                self.conn_id, self.session.username
            );
            create_error_response(
                "FATAL",
                sqlstate::INVALID_PASSWORD,
                "password authentication failed for user",
            )
            .encode(&mut self.write_buf);
            self.flush_write().await?;
            return Err(PgProtocolError::AuthenticationFailed(
                "password authentication failed".to_string(),
            ));
        }

        info!(
            "PG conn {}: auth OK for user '{}'",
            self.conn_id, self.session.username
        );
        BackendMessage::AuthenticationOk.encode(&mut self.write_buf);
        self.flush_write().await
    }

    // ======================================================================
    // Post-auth initialization
    // ======================================================================

    async fn send_parameter_status(&mut self) -> Result<(), PgProtocolError> {
        let params = [
            ("server_version", "15.0"),
            ("server_encoding", "UTF8"),
            ("client_encoding", "UTF8"),
            ("DateStyle", "ISO, MDY"),
            ("TimeZone", "UTC"),
            ("integer_datetimes", "on"),
            ("standard_conforming_strings", "on"),
            ("application_name", ""),
            ("default_transaction_read_only", "off"),
            ("in_hot_standby", "off"),
            ("is_superuser", "on"),
            ("session_authorization", &self.session.username),
        ];
        for (key, value) in &params {
            BackendMessage::ParameterStatus {
                key: key.to_string(),
                value: value.to_string(),
            }
            .encode(&mut self.write_buf);
        }
        self.flush_write().await
    }

    async fn send_backend_key_data(&mut self) -> Result<(), PgProtocolError> {
        BackendMessage::BackendKeyData {
            pid: self.process_id,
            secret_key: self.secret_key,
        }
        .encode(&mut self.write_buf);
        self.flush_write().await
    }

    async fn send_ready_for_query(&mut self) -> Result<(), PgProtocolError> {
        BackendMessage::ReadyForQuery {
            status: TransactionStatus::Idle,
        }
        .encode(&mut self.write_buf);
        self.flush_write().await
    }

    // ======================================================================
    // Message loop
    // ======================================================================

    async fn handle_messages(&mut self) -> Result<(), PgProtocolError> {
        loop {
            self.read_buf_ensure(5).await?;
            let msg_len = (&self.read_buf[1..5]).get_i32() as usize;
            if msg_len > MAX_MESSAGE_SIZE || msg_len < 4 {
                return Err(PgProtocolError::ProtocolViolation(format!(
                    "invalid message length: {}",
                    msg_len
                )));
            }
            self.read_buf_ensure(1 + msg_len).await?;

            let msg = match FrontendMessage::decode(&mut self.read_buf) {
                Ok(Some(m)) => m,
                Ok(None) => continue,
                Err(e) => {
                    error!("PG conn {}: decode error: {:?}", self.conn_id, e);
                    return Err(e);
                }
            };

            match msg {
                FrontendMessage::Query { sql } => self.handle_query(&sql).await?,
                FrontendMessage::Terminate => {
                    info!("PG conn {}: Terminate", self.conn_id);
                    return Ok(());
                }
                FrontendMessage::Parse {
                    name,
                    query,
                    param_types,
                } => {
                    self.handle_parse(&name, &query, &param_types).await;
                }
                FrontendMessage::Bind {
                    portal, statement, ..
                } => {
                    self.handle_bind(&portal, &statement).await;
                }
                FrontendMessage::Describe { target, name } => {
                    self.handle_describe(target, &name).await;
                }
                FrontendMessage::Execute { portal, max_rows } => {
                    self.handle_execute(&portal, max_rows).await
                }
                FrontendMessage::Close { target, name } => self.handle_close(target, &name).await,
                FrontendMessage::Sync => self.send_ready_for_query().await?,
                other => {
                    debug!("PG conn {}: unhandled: {:?}", self.conn_id, other);
                    self.send_ready_for_query().await?;
                }
            }
        }
    }

    // ======================================================================
    // Simple Query
    // ======================================================================

    async fn handle_query(&mut self, sql: &str) -> Result<(), PgProtocolError> {
        let trimmed = sql.trim().trim_end_matches(';');
        if trimmed.is_empty() {
            BackendMessage::EmptyQueryResponse.encode(&mut self.write_buf);
            self.send_ready_for_query().await?;
            return Ok(());
        }

        let result = self.handler.handle_query(self.conn_id, trimmed);

        if is_row_returning_query(trimmed) {
            // Row-returning query: send RowDescription + DataRows + CommandComplete
            self.send_query_result(&result, trimmed).await?;
        } else {
            // DML (INSERT/UPDATE/DELETE) or DDL: send CommandComplete only.
            // Extract affected row count from result if available.
            let row_count = extract_affected_rows(&result);
            let tag = infer_command_tag(trimmed, row_count);
            BackendMessage::CommandComplete { tag }.encode(&mut self.write_buf);
        }

        self.send_ready_for_query().await
    }

    async fn send_query_result(
        &mut self,
        result: &QueryResult,
        sql: &str,
    ) -> Result<(), PgProtocolError> {
        let fields: Vec<FieldDescription> = result
            .columns
            .iter()
            .map(|col| {
                let type_oid = map_column_type_to_oid(col.col_type);
                let type_size = map_column_type_to_size(col.col_type);
                FieldDescription::new(&col.name, type_oid, type_size)
            })
            .collect();

        BackendMessage::RowDescription {
            fields: fields.clone(),
        }
        .encode(&mut self.write_buf);

        for row in &result.rows {
            let values: Vec<Option<Vec<u8>>> = row
                .iter()
                .zip(fields.iter())
                .map(|(val, field)| {
                    val.as_ref().map(|s| {
                        if field.type_oid == OID_BOOL {
                            match s.to_lowercase().as_str() {
                                "true" | "1" | "yes" => b"t".to_vec(),
                                _ => b"f".to_vec(),
                            }
                        } else {
                            s.as_bytes().to_vec()
                        }
                    })
                })
                .collect();
            BackendMessage::DataRow { values }.encode(&mut self.write_buf);
        }

        let tag = infer_command_tag(sql, result.rows.len() as i64);
        BackendMessage::CommandComplete { tag }.encode(&mut self.write_buf);
        self.flush_write().await
    }

    // ======================================================================
    // Extended Query
    // ======================================================================

    async fn handle_parse(&mut self, name: &str, query: &str, param_types: &[u32]) {
        let stmt_name = if name.is_empty() {
            format!("_pg3_{}", self.conn_id)
        } else {
            name.to_string()
        };

        self.session.prepared_statements.insert(
            stmt_name,
            PreparedStatement {
                sql: query.to_string(),
                param_types: param_types.to_vec(),
                fields: Vec::new(),
            },
        );
        BackendMessage::ParseComplete.encode(&mut self.write_buf);
        if let Err(e) = self.flush_write().await {
            error!(
                "PG conn {}: flush error in handle_parse: {}",
                self.conn_id, e
            );
        }
    }

    async fn handle_bind(&mut self, portal: &str, statement: &str) {
        let portal_name = if portal.is_empty() {
            format!("_pg3_portal_{}", self.conn_id)
        } else {
            portal.to_string()
        };
        let stmt_name = if statement.is_empty() {
            format!("_pg3_{}", self.conn_id)
        } else {
            statement.to_string()
        };

        // Validate that the referenced prepared statement exists
        if !self.session.prepared_statements.contains_key(&stmt_name) {
            error!(
                "PG conn {}: bind to non-existent prepared statement '{}'",
                self.conn_id, stmt_name
            );
            create_error_response(
                "ERROR",
                sqlstate::INVALID_SQL_STATEMENT_NAME,
                &format!("prepared statement '{}' does not exist", statement),
            )
            .encode(&mut self.write_buf);
            if let Err(e) = self.flush_write().await {
                error!(
                    "PG conn {}: flush error in handle_bind: {}",
                    self.conn_id, e
                );
            }
            return;
        }

        self.session.portals.insert(portal_name, stmt_name);
        BackendMessage::BindComplete.encode(&mut self.write_buf);
        if let Err(e) = self.flush_write().await {
            error!(
                "PG conn {}: flush error in handle_bind: {}",
                self.conn_id, e
            );
        }
    }

    async fn handle_describe(&mut self, target: DescribeTarget, name: &str) {
        match target {
            DescribeTarget::Statement => {
                let stmt_name = if name.is_empty() {
                    format!("_pg3_{}", self.conn_id)
                } else {
                    name.to_string()
                };
                // Extract SQL and cached state (drop borrow before mutating)
                let cached = self
                    .session
                    .prepared_statements
                    .get(&stmt_name)
                    .map(|stmt| (stmt.sql.clone(), stmt.param_types.clone(), stmt.fields.clone()));

                if let Some((sql, param_types, cached_fields)) = cached {
                    if !param_types.is_empty() {
                        BackendMessage::ParameterDescription {
                            type_oids: param_types,
                        }
                        .encode(&mut self.write_buf);
                    }
                    if !cached_fields.is_empty() {
                        BackendMessage::RowDescription {
                            fields: cached_fields,
                        }
                        .encode(&mut self.write_buf);
                    } else {
                        // Fields not yet cached — execute query to determine columns
                        let trimmed = sql.trim().trim_end_matches(';');
                        if trimmed.is_empty() {
                            BackendMessage::NoData.encode(&mut self.write_buf);
                        } else if is_row_returning_query(trimmed) {
                            let result = self.handler.handle_query(self.conn_id, trimmed);
                            if !result.columns.is_empty() {
                                let fields: Vec<FieldDescription> = result
                                    .columns
                                    .iter()
                                    .map(|col| {
                                        let type_oid = map_column_type_to_oid(col.col_type);
                                        let type_size = map_column_type_to_size(col.col_type);
                                        FieldDescription::new(&col.name, type_oid, type_size)
                                    })
                                    .collect();
                                // Cache for future Describe calls
                                if let Some(stmt) =
                                    self.session.prepared_statements.get_mut(&stmt_name)
                                {
                                    stmt.fields = fields.clone();
                                }
                                BackendMessage::RowDescription { fields }
                                    .encode(&mut self.write_buf);
                            } else {
                                BackendMessage::NoData.encode(&mut self.write_buf);
                            }
                        }
                    }
                } else {
                    BackendMessage::NoData.encode(&mut self.write_buf);
                }
            }
            DescribeTarget::Portal => {
                // For portal describe, look up the portal's statement and determine fields
                let portal_name = if name.is_empty() {
                    format!("_pg3_portal_{}", self.conn_id)
                } else {
                    name.to_string()
                };
                let stmt_name = self.session.portals.get(&portal_name).cloned();
                let cached_fields = stmt_name
                    .as_ref()
                    .and_then(|sn| self.session.prepared_statements.get(sn))
                    .map(|s| s.fields.clone());

                match cached_fields {
                    Some(fields) if !fields.is_empty() => {
                        BackendMessage::RowDescription { fields }
                            .encode(&mut self.write_buf);
                    }
                    _ => {
                        // Try to determine fields by executing the query
                        let mut found_fields = false;
                        if let Some(ref sn) = stmt_name {
                            let sql = self
                                .session
                                .prepared_statements
                                .get(sn)
                                .map(|s| s.sql.clone());
                            if let Some(sql) = sql {
                                let trimmed = sql.trim().trim_end_matches(';');
                                if !trimmed.is_empty() && is_row_returning_query(trimmed) {
                                    let result = self.handler.handle_query(self.conn_id, trimmed);
                                    if !result.columns.is_empty() {
                                        let fields: Vec<FieldDescription> = result
                                            .columns
                                            .iter()
                                            .map(|col| {
                                                let type_oid = map_column_type_to_oid(col.col_type);
                                                let type_size =
                                                    map_column_type_to_size(col.col_type);
                                                FieldDescription::new(
                                                    &col.name,
                                                    type_oid,
                                                    type_size,
                                                )
                                            })
                                            .collect();
                                        // Cache for future use
                                        if let Some(stmt) =
                                            self.session.prepared_statements.get_mut(sn)
                                        {
                                            stmt.fields = fields.clone();
                                        }
                                        BackendMessage::RowDescription { fields }
                                            .encode(&mut self.write_buf);
                                        found_fields = true;
                                    }
                                }
                            }
                        }
                        if !found_fields {
                            BackendMessage::NoData.encode(&mut self.write_buf);
                        }
                    }
                }
            }
        }
        if let Err(e) = self.flush_write().await {
            error!(
                "PG conn {}: flush error in handle_describe: {}",
                self.conn_id, e
            );
        }
    }

    async fn handle_execute(&mut self, portal: &str, max_rows: i32) {
        // TODO: max_rows support - when max_rows > 0, limit the returned rows
        // and send PortalSuspended instead of CommandComplete if more rows exist.
        // For Phase 1, we return all rows regardless of max_rows.
        let _ = max_rows;
        let portal_name = if portal.is_empty() {
            format!("_pg3_portal_{}", self.conn_id)
        } else {
            portal.to_string()
        };

        // Look up the statement name for this portal
        let stmt_name = match self.session.portals.get(&portal_name) {
            Some(name) => name.clone(),
            None => {
                error!(
                    "PG conn {}: portal '{}' not found",
                    self.conn_id, portal_name
                );
                create_error_response(
                    "ERROR",
                    sqlstate::INVALID_CURSOR_STATE,
                    &format!("portal '{}' not found", portal),
                )
                .encode(&mut self.write_buf);
                self.flush_write().await.ok();
                return;
            }
        };

        // Look up the prepared statement
        let sql = match self.session.prepared_statements.get(&stmt_name) {
            Some(stmt) => stmt.sql.clone(),
            None => {
                error!(
                    "PG conn {}: prepared statement '{}' not found",
                    self.conn_id, stmt_name
                );
                create_error_response(
                    "ERROR",
                    sqlstate::INVALID_SQL_STATEMENT_NAME,
                    &format!("prepared statement '{}' not found", stmt_name),
                )
                .encode(&mut self.write_buf);
                self.flush_write().await.ok();
                return;
            }
        };

        // Execute the query
        let trimmed = sql.trim().trim_end_matches(';');
        if trimmed.is_empty() {
            BackendMessage::EmptyQueryResponse.encode(&mut self.write_buf);
            self.send_ready_for_query().await.ok();
            return;
        }

        let result = self.handler.handle_query(self.conn_id, trimmed);

        if is_row_returning_query(trimmed) {
            // Row-returning query: cache fields for Describe, then send DataRows + CommandComplete.
            // NOTE: Do NOT send RowDescription here — that's only sent by handle_describe.
            let fields: Vec<FieldDescription> = result
                .columns
                .iter()
                .map(|col| {
                    let type_oid = map_column_type_to_oid(col.col_type);
                    let type_size = map_column_type_to_size(col.col_type);
                    FieldDescription::new(&col.name, type_oid, type_size)
                })
                .collect();

            // Cache fields for future Describe calls
            if let Some(stmt) = self.session.prepared_statements.get_mut(&stmt_name) {
                stmt.fields = fields.clone();
            }

            // Send DataRows (no RowDescription — Execute must not send it)
            for row in &result.rows {
                let values: Vec<Option<Vec<u8>>> = row
                    .iter()
                    .zip(fields.iter())
                    .map(|(val, field)| {
                        val.as_ref().map(|s| {
                            if field.type_oid == OID_BOOL {
                                match s.to_lowercase().as_str() {
                                    "true" | "1" | "yes" => b"t".to_vec(),
                                    _ => b"f".to_vec(),
                                }
                            } else {
                                s.as_bytes().to_vec()
                            }
                        })
                    })
                    .collect();
                BackendMessage::DataRow { values }.encode(&mut self.write_buf);
            }

            let tag = infer_command_tag(trimmed, result.rows.len() as i64);
            BackendMessage::CommandComplete { tag }.encode(&mut self.write_buf);
        } else {
            // DML (INSERT/UPDATE/DELETE) or DDL: send CommandComplete only.
            // Still cache fields if the result has columns (for Describe).
            if !result.columns.is_empty() {
                let fields: Vec<FieldDescription> = result
                    .columns
                    .iter()
                    .map(|col| {
                        let type_oid = map_column_type_to_oid(col.col_type);
                        let type_size = map_column_type_to_size(col.col_type);
                        FieldDescription::new(&col.name, type_oid, type_size)
                    })
                    .collect();
                if let Some(stmt) = self.session.prepared_statements.get_mut(&stmt_name) {
                    stmt.fields = fields;
                }
            }
            let row_count = extract_affected_rows(&result);
            let tag = infer_command_tag(trimmed, row_count);
            BackendMessage::CommandComplete { tag }.encode(&mut self.write_buf);
        }

        if let Err(e) = self.flush_write().await {
            error!(
                "PG conn {}: flush error in handle_execute: {}",
                self.conn_id, e
            );
        }
    }

    async fn handle_close(&mut self, target: DescribeTarget, name: &str) {
        match target {
            DescribeTarget::Statement => {
                let stmt_name = if name.is_empty() {
                    format!("_pg3_{}", self.conn_id)
                } else {
                    name.to_string()
                };
                self.session.prepared_statements.remove(&stmt_name);
            }
            DescribeTarget::Portal => {
                let portal_name = if name.is_empty() {
                    format!("_pg3_portal_{}", self.conn_id)
                } else {
                    name.to_string()
                };
                self.session.portals.remove(&portal_name);
            }
        }
        BackendMessage::CloseComplete.encode(&mut self.write_buf);
        if let Err(e) = self.flush_write().await {
            error!(
                "PG conn {}: flush error in handle_close: {}",
                self.conn_id, e
            );
        }
    }

    // ======================================================================
    // I/O
    // ======================================================================

    async fn read_buf_ensure(&mut self, n: usize) -> Result<(), PgProtocolError> {
        while self.read_buf.len() < n {
            let mut tmp = vec![0u8; 8192];
            let nread = tokio::time::timeout(
                std::time::Duration::from_secs(300), // 5-minute idle timeout
                self.stream.read(&mut tmp),
            )
            .await
            .map_err(|_| {
                PgProtocolError::ProtocolViolation("read timeout after 300s idle".to_string())
            })??;
            if nread == 0 {
                return Err(PgProtocolError::ConnectionClosed);
            }
            self.read_buf.extend_from_slice(&tmp[..nread]);
        }
        Ok(())
    }

    async fn flush_write(&mut self) -> Result<(), PgProtocolError> {
        if !self.write_buf.is_empty() {
            self.stream.write_all(&self.write_buf).await?;
            self.write_buf.clear();
        }
        Ok(())
    }
}

// ============================================================================
// Standalone functions
// ============================================================================

/// Returns true if the SQL statement is expected to return rows in PostgreSQL.
///
/// In PG protocol, only row-returning queries get RowDescription + DataRow.
/// DML (INSERT/UPDATE/DELETE) and DDL only get CommandComplete.
fn is_row_returning_query(sql: &str) -> bool {
    let upper = sql.trim().to_uppercase();
    upper.starts_with("SELECT")
        || upper.starts_with("WITH")
        || upper.starts_with("VALUES")
        || upper.starts_with("SHOW")
        || upper.starts_with("EXPLAIN")
        || upper.starts_with("DESCRIBE")
        || upper.starts_with("DESC ")
}

/// Extract the affected row count from a QueryResult for DML commands.
/// The handler may return a single-row result with an "affected_rows" column.
fn extract_affected_rows(result: &QueryResult) -> i64 {
    if result.columns.is_empty() || result.rows.is_empty() {
        return 0;
    }
    // Try to parse the first column of the first row as a row count
    if let Some(Some(val)) = result.rows.first().and_then(|r| r.first()) {
        val.parse::<i64>().unwrap_or(result.rows.len() as i64)
    } else {
        result.rows.len() as i64
    }
}

/// Infer a PostgreSQL command tag from SQL text (e.g., "SELECT 5", "INSERT 0 1").
fn infer_command_tag(sql: &str, row_count: i64) -> String {
    let upper = sql.trim().to_uppercase();
    if upper.starts_with("SELECT") || upper.starts_with("WITH") || upper.starts_with("VALUES") {
        format!("SELECT {}", row_count)
    } else if upper.starts_with("INSERT") {
        format!("INSERT 0 {}", row_count)
    } else if upper.starts_with("UPDATE") {
        format!("UPDATE {}", row_count)
    } else if upper.starts_with("DELETE") {
        format!("DELETE {}", row_count)
    } else if upper.starts_with("CREATE TABLE") {
        "CREATE TABLE".to_string()
    } else if upper.starts_with("CREATE DATABASE") {
        "CREATE DATABASE".to_string()
    } else if upper.starts_with("CREATE") {
        "CREATE".to_string()
    } else if upper.starts_with("DROP TABLE") {
        "DROP TABLE".to_string()
    } else if upper.starts_with("DROP DATABASE") {
        "DROP DATABASE".to_string()
    } else if upper.starts_with("DROP") {
        "DROP".to_string()
    } else if upper.starts_with("ALTER TABLE") {
        "ALTER TABLE".to_string()
    } else if upper.starts_with("ALTER DATABASE") {
        "ALTER DATABASE".to_string()
    } else if upper.starts_with("ALTER") {
        "ALTER".to_string()
    } else if upper.starts_with("TRUNCATE") {
        "TRUNCATE TABLE".to_string()
    } else if upper.starts_with("BEGIN") {
        "BEGIN".to_string()
    } else if upper.starts_with("COMMIT") {
        "COMMIT".to_string()
    } else if upper.starts_with("ROLLBACK") {
        "ROLLBACK".to_string()
    } else if upper.starts_with("SET") {
        "SET".to_string()
    } else if upper.starts_with("SHOW") {
        "SHOW".to_string()
    } else if upper.starts_with("USE") {
        "SET".to_string()
    } else if upper.starts_with("EXPLAIN")
        || upper.starts_with("DESCRIBE")
        || upper.starts_with("DESC")
    {
        "EXPLAIN".to_string()
    } else {
        format!("OK {}", row_count)
    }
}

/// Map MySQL ColumnType to PostgreSQL type OID.
fn map_column_type_to_oid(col_type: ColumnType) -> i32 {
    match col_type {
        ColumnType::String => OID_TEXT,
        ColumnType::Int => OID_INT4,
        ColumnType::Float => OID_FLOAT4,
        ColumnType::Double => OID_FLOAT8,
        ColumnType::Date => OID_DATE,
        ColumnType::DateTime => OID_TIMESTAMP,
        ColumnType::Blob => OID_TEXT,
    }
}

/// Map MySQL ColumnType to PG type size (-1 = variable length).
fn map_column_type_to_size(col_type: ColumnType) -> i16 {
    match col_type {
        ColumnType::String => -1,
        ColumnType::Int => 4,
        ColumnType::Float => 4,
        ColumnType::Double => 8,
        ColumnType::Date => 4,
        ColumnType::DateTime => 8,
        ColumnType::Blob => -1,
    }
}

/// Standalone entry point: create and run a PG connection.
pub async fn run_connection(
    stream: TcpStream,
    conn_id: u32,
    handler: Arc<dyn QueryHandler>,
    auth_config: AuthConfig,
) -> Result<(), PgProtocolError> {
    let mut conn = PgConnection::new(stream, conn_id, handler, auth_config);
    conn.run().await
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::compute_md5_password;
    use mysql_protocol::server::ColumnDef;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn test_infer_command_tag_select() {
        assert_eq!(infer_command_tag("SELECT * FROM t", 5), "SELECT 5");
    }

    #[test]
    fn test_infer_command_tag_insert() {
        assert_eq!(
            infer_command_tag("INSERT INTO t VALUES (1)", 1),
            "INSERT 0 1"
        );
    }

    #[test]
    fn test_infer_command_tag_update() {
        assert_eq!(infer_command_tag("UPDATE t SET x=1", 3), "UPDATE 3");
    }

    #[test]
    fn test_infer_command_tag_delete() {
        assert_eq!(infer_command_tag("DELETE FROM t", 2), "DELETE 2");
    }

    #[test]
    fn test_infer_command_tag_create_table() {
        assert_eq!(
            infer_command_tag("CREATE TABLE t (id INT)", 0),
            "CREATE TABLE"
        );
    }

    #[test]
    fn test_infer_command_tag_drop() {
        assert_eq!(infer_command_tag("DROP TABLE t", 0), "DROP TABLE");
    }

    #[test]
    fn test_infer_command_tag_begin() {
        assert_eq!(infer_command_tag("BEGIN", 0), "BEGIN");
    }

    #[test]
    fn test_infer_command_tag_commit() {
        assert_eq!(infer_command_tag("COMMIT", 0), "COMMIT");
    }

    #[test]
    fn test_infer_command_tag_set() {
        assert_eq!(infer_command_tag("SET x = 1", 0), "SET");
    }

    #[test]
    fn test_infer_command_tag_show() {
        assert_eq!(infer_command_tag("SHOW x", 0), "SHOW");
    }

    #[test]
    fn test_infer_command_tag_use() {
        assert_eq!(infer_command_tag("USE mydb", 0), "SET");
    }

    #[test]
    fn test_infer_command_tag_with_cte() {
        assert_eq!(
            infer_command_tag("WITH t AS (SELECT 1) SELECT * FROM t", 3),
            "SELECT 3"
        );
    }

    #[test]
    fn test_infer_command_tag_values() {
        assert_eq!(infer_command_tag("VALUES (1), (2)", 2), "SELECT 2");
    }

    #[test]
    fn test_infer_command_tag_unknown() {
        assert_eq!(infer_command_tag("SOME_COMMAND args", 0), "OK 0");
    }

    #[test]
    fn test_map_column_type_oid() {
        assert_eq!(map_column_type_to_oid(ColumnType::String), OID_TEXT);
        assert_eq!(map_column_type_to_oid(ColumnType::Int), OID_INT4);
        assert_eq!(map_column_type_to_oid(ColumnType::Float), OID_FLOAT4);
        assert_eq!(map_column_type_to_oid(ColumnType::Double), OID_FLOAT8);
        assert_eq!(map_column_type_to_oid(ColumnType::Date), OID_DATE);
        assert_eq!(map_column_type_to_oid(ColumnType::DateTime), OID_TIMESTAMP);
        assert_eq!(map_column_type_to_oid(ColumnType::Blob), OID_TEXT);
    }

    #[test]
    fn test_map_column_type_size() {
        assert_eq!(map_column_type_to_size(ColumnType::String), -1);
        assert_eq!(map_column_type_to_size(ColumnType::Int), 4);
        assert_eq!(map_column_type_to_size(ColumnType::Float), 4);
        assert_eq!(map_column_type_to_size(ColumnType::Double), 8);
        assert_eq!(map_column_type_to_size(ColumnType::Date), 4);
        assert_eq!(map_column_type_to_size(ColumnType::DateTime), 8);
        assert_eq!(map_column_type_to_size(ColumnType::Blob), -1);
    }

    // ======================================================================
    // Mock handler for integration tests
    // ======================================================================

    struct MockHandler;

    impl QueryHandler for MockHandler {
        fn handle_query(&self, _conn_id: u32, sql: &str) -> QueryResult {
            match sql.trim().to_uppercase().as_str() {
                "SELECT 1" => QueryResult::with_rows(
                    vec![ColumnDef {
                        name: "?column?".to_string(),
                        col_type: ColumnType::Int,
                    }],
                    vec![vec![Some("1".to_string())]],
                ),
                "SELECT 2 AS VAL" => QueryResult::with_rows(
                    vec![ColumnDef {
                        name: "val".to_string(),
                        col_type: ColumnType::Int,
                    }],
                    vec![vec![Some("2".to_string())]],
                ),
                "CREATE TABLE T (ID INT)" => QueryResult::ok(),
                _ => QueryResult::ok(),
            }
        }
    }

    // Helper function to read a PG message type byte
    async fn read_pg_type<R: AsyncReadExt + Unpin>(client: &mut R) -> u8 {
        let mut buf = [0u8; 1];
        client.read_exact(&mut buf).await.unwrap();
        buf[0]
    }

    // Helper function to read and discard a PG message body (type already consumed)
    async fn read_pg_body<R: AsyncReadExt + Unpin>(client: &mut R) -> Vec<u8> {
        let mut len_buf = [0u8; 4];
        client.read_exact(&mut len_buf).await.unwrap();
        let len = i32::from_be_bytes(len_buf) as usize;
        let mut body = vec![0u8; len - 4];
        if !body.is_empty() {
            client.read_exact(&mut body).await.unwrap();
        }
        body
    }

    // Helper: read a complete PG message (type + length + body)
    async fn read_pg_message<R: AsyncReadExt + Unpin>(client: &mut R) -> (u8, Vec<u8>) {
        let msg_type = read_pg_type(client).await;
        let body = read_pg_body(client).await;
        (msg_type, body)
    }

    #[allow(dead_code)]
    // Helper: write a complete PG frontend message
    fn write_frontend_message(buf: &mut bytes::BytesMut, type_byte: u8, body: &[u8]) {
        buf.put_u8(type_byte);
        buf.put_i32((body.len() + 4) as i32);
        buf.put_slice(body);
    }

    // Helper: build a startup message
    fn build_startup_message(user: &str, database: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        // version
        buf.extend_from_slice(&(PG_PROTOCOL_VERSION_3 as i32).to_be_bytes());
        // params
        buf.extend_from_slice(b"user\0");
        buf.extend_from_slice(user.as_bytes());
        buf.push(0);
        buf.extend_from_slice(b"database\0");
        buf.extend_from_slice(database.as_bytes());
        buf.push(0);
        buf.push(0); // final \0

        let mut msg = Vec::new();
        msg.extend_from_slice(&(buf.len() as i32 + 4).to_be_bytes());
        msg.extend_from_slice(&buf);
        msg
    }

    // Helper: build a Parse message
    fn build_parse_message(name: &str, query: &str, param_types: &[u32]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(query.as_bytes());
        body.push(0);
        body.extend_from_slice(&(param_types.len() as u16).to_be_bytes());
        for pt in param_types {
            body.extend_from_slice(&pt.to_be_bytes());
        }
        let mut msg = vec![b'P'];
        msg.extend_from_slice(&(body.len() as i32 + 4).to_be_bytes());
        msg.extend_from_slice(&body);
        msg
    }

    // Helper: build a Bind message
    fn build_bind_message(portal: &str, statement: &str, values: &[Option<&[u8]>]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(portal.as_bytes());
        body.push(0);
        body.extend_from_slice(statement.as_bytes());
        body.push(0);
        // num_formats (0 = all text)
        body.extend_from_slice(&0u16.to_be_bytes());
        // num_values
        body.extend_from_slice(&(values.len() as u16).to_be_bytes());
        for val in values {
            match val {
                Some(data) => {
                    body.extend_from_slice(&(data.len() as i32).to_be_bytes());
                    body.extend_from_slice(data);
                }
                None => {
                    body.extend_from_slice(&(-1i32).to_be_bytes());
                }
            }
        }
        // num_result_formats (0 = all text)
        body.extend_from_slice(&0u16.to_be_bytes());

        let mut msg = vec![b'B'];
        msg.extend_from_slice(&(body.len() as i32 + 4).to_be_bytes());
        msg.extend_from_slice(&body);
        msg
    }

    // Helper: build a Describe message
    fn build_describe_message(target: u8, name: &str) -> Vec<u8> {
        let mut body = Vec::new();
        body.push(target); // 'S' for Statement, 'P' for Portal
        body.extend_from_slice(name.as_bytes());
        body.push(0);

        let mut msg = vec![b'D'];
        msg.extend_from_slice(&(body.len() as i32 + 4).to_be_bytes());
        msg.extend_from_slice(&body);
        msg
    }

    // Helper: build an Execute message
    fn build_execute_message(portal: &str, max_rows: i32) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(portal.as_bytes());
        body.push(0);
        body.extend_from_slice(&max_rows.to_be_bytes());

        let mut msg = vec![b'E'];
        msg.extend_from_slice(&(body.len() as i32 + 4).to_be_bytes());
        msg.extend_from_slice(&body);
        msg
    }

    // Helper: build a Sync message
    fn build_sync_message() -> Vec<u8> {
        let mut msg = vec![b'S'];
        msg.extend_from_slice(&4i32.to_be_bytes());
        msg
    }

    // Helper: perform startup + auth handshake for test
    async fn startup_and_auth<R: AsyncReadExt + Unpin, W: AsyncWriteExt + Unpin>(
        client: &mut R,
        writer: &mut W,
        user: &str,
        database: &str,
    ) {
        // Send startup message
        let startup = build_startup_message(user, database);
        writer.write_all(&startup).await.unwrap();

        // Read AuthenticationMD5Password (type 'R')
        let (msg_type, body) = read_pg_message(client).await;
        assert_eq!(msg_type, b'R', "expected auth response");
        let auth_type = i32::from_be_bytes(body[..4].try_into().unwrap());
        assert_eq!(auth_type, 5, "expected MD5 password auth");

        // Send password (accept_any_password is true, so send anything)
        let salt = &body[4..8];
        let password = "any_password";
        // Compute MD5 response
        let md5_resp = compute_md5_password(user, password, salt.try_into().unwrap());
        let mut pwd_body = Vec::new();
        pwd_body.extend_from_slice(md5_resp.as_bytes());
        pwd_body.push(0);
        let mut pwd_msg = vec![b'p'];
        pwd_msg.extend_from_slice(&(pwd_body.len() as i32 + 4).to_be_bytes());
        pwd_msg.extend_from_slice(&pwd_body);
        writer.write_all(&pwd_msg).await.unwrap();

        // Read AuthenticationOk ('R' with auth_type=0)
        loop {
            let (msg_type, body) = read_pg_message(client).await;
            if msg_type == b'R' {
                let auth_type = i32::from_be_bytes(body[..4].try_into().unwrap());
                if auth_type == 0 {
                    break; // AuthenticationOk
                }
            }
            if msg_type == b'Z' {
                // ReadyForQuery - shouldn't happen before auth ok
                panic!("got ReadyForQuery before AuthenticationOk");
            }
            // Continue reading other messages (ParameterStatus, BackendKeyData)
        }

        // Read ParameterStatus messages
        loop {
            let (msg_type, _body) = read_pg_message(client).await;
            if msg_type == b'Z' {
                break; // ReadyForQuery completes the startup sequence
            }
            // Accept 'S' (ParameterStatus) and 'K' (BackendKeyData)
            assert!(
                msg_type == b'S' || msg_type == b'K',
                "unexpected message type during startup: {}",
                msg_type as char
            );
        }
    }

    #[tokio::test]
    async fn test_extended_query_full_flow() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handler = Arc::new(MockHandler);
        let auth_config = AuthConfig {
            accept_any_password: true,
            ..Default::default()
        };

        // Spawn server
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut conn = PgConnection::new(stream, 1, handler, auth_config);
            let _ = conn.run().await;
        });

        // Connect client
        let client_stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut reader, mut writer) = tokio::io::split(client_stream);

        // Startup + auth
        startup_and_auth(&mut reader, &mut writer, "testuser", "testdb").await;

        // === Extended Query: Parse "SELECT 1" ===
        let parse_msg = build_parse_message("", "SELECT 1", &[]);
        writer.write_all(&parse_msg).await.unwrap();

        // Expect ParameterDescription? No params, so no. Expect ParseComplete.
        loop {
            let (msg_type, _body) = read_pg_message(&mut reader).await;
            // Skip NoData if sent
            if msg_type == b'1' {
                break;
            }
            // 't' = ParameterDescription, 'n' = NoData - both are valid to receive before ParseComplete
            assert!(
                msg_type == b't' || msg_type == b'n',
                "expected ParseComplete, got type byte 0x{:02x}",
                msg_type
            );
        }

        // === Bind ===
        let bind_msg = build_bind_message("", "", &[]);
        writer.write_all(&bind_msg).await.unwrap();

        let (msg_type, _body) = read_pg_message(&mut reader).await;
        assert_eq!(
            msg_type, b'2',
            "expected BindComplete, got 0x{:02x}",
            msg_type
        );

        // === Describe(Portal) ===
        // After fix: Describe executes query to determine columns, returns RowDescription
        let describe_msg = build_describe_message(b'P', "");
        writer.write_all(&describe_msg).await.unwrap();

        let (msg_type, body) = read_pg_message(&mut reader).await;
        assert_eq!(
            msg_type, b'T',
            "expected RowDescription from Describe(Portal), got 0x{:02x}",
            msg_type
        );
        let num_fields = u16::from_be_bytes(body[..2].try_into().unwrap());
        assert_eq!(num_fields, 1, "expected 1 column in RowDescription");

        // === Execute ===
        let execute_msg = build_execute_message("", 0);
        writer.write_all(&execute_msg).await.unwrap();

        // Execute sends DataRow + CommandComplete (NOT RowDescription)
        // DataRow ('D')
        let (msg_type, body) = read_pg_message(&mut reader).await;
        assert_eq!(msg_type, b'D', "expected DataRow, got 0x{:02x}", msg_type);
        let num_cols = u16::from_be_bytes(body[..2].try_into().unwrap());
        assert_eq!(num_cols, 1);
        let col_len = i32::from_be_bytes(body[2..6].try_into().unwrap());
        assert_eq!(col_len, 1);
        assert_eq!(&body[6..7], b"1");

        // CommandComplete ('C')
        let (msg_type, body) = read_pg_message(&mut reader).await;
        assert_eq!(
            msg_type, b'C',
            "expected CommandComplete, got 0x{:02x}",
            msg_type
        );
        let tag = String::from_utf8_lossy(&body[..body.len() - 1]);
        assert_eq!(tag, "SELECT 1");

        // === Sync ===
        let sync_msg = build_sync_message();
        writer.write_all(&sync_msg).await.unwrap();

        // ReadyForQuery ('Z')
        let (msg_type, body) = read_pg_message(&mut reader).await;
        assert_eq!(
            msg_type, b'Z',
            "expected ReadyForQuery, got 0x{:02x}",
            msg_type
        );
        assert_eq!(body[0], b'I', "expected idle status");
    }

    #[tokio::test]
    async fn test_describe_statement_after_execute_returns_row_description() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handler = Arc::new(MockHandler);
        let auth_config = AuthConfig {
            accept_any_password: true,
            ..Default::default()
        };

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut conn = PgConnection::new(stream, 1, handler, auth_config);
            let _ = conn.run().await;
        });

        let client_stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut reader, mut writer) = tokio::io::split(client_stream);

        startup_and_auth(&mut reader, &mut writer, "user", "db").await;

        // Parse "SELECT 2 AS VAL"
        writer
            .write_all(&build_parse_message("mystmt", "SELECT 2 AS VAL", &[]))
            .await
            .unwrap();
        // Read until ParseComplete
        loop {
            let (msg_type, _body) = read_pg_message(&mut reader).await;
            if msg_type == b'1' {
                break;
            }
        }

        // Bind portal to mystmt
        writer
            .write_all(&build_bind_message("", "mystmt", &[]))
            .await
            .unwrap();
        let (msg_type, _body) = read_pg_message(&mut reader).await;
        assert_eq!(msg_type, b'2');

        // Execute portal (to cache column metadata)
        writer
            .write_all(&build_execute_message("", 0))
            .await
            .unwrap();
        // Read DataRow + CommandComplete (Execute does NOT send RowDescription)
        loop {
            let (msg_type, _body) = read_pg_message(&mut reader).await;
            if msg_type == b'C' {
                break; // CommandComplete ends the execute response
            }
        }

        // Now Describe(Statement) for mystmt should return RowDescription
        writer
            .write_all(&build_describe_message(b'S', "mystmt"))
            .await
            .unwrap();
        let (msg_type, body) = read_pg_message(&mut reader).await;
        assert_eq!(
            msg_type, b'T',
            "expected RowDescription for statement after execute, got 0x{:02x}",
            msg_type
        );
        let num_fields = u16::from_be_bytes(body[..2].try_into().unwrap());
        assert_eq!(num_fields, 1);
        // Check field name
        let name_end = body[2..].iter().position(|&b| b == 0).unwrap();
        let field_name = String::from_utf8_lossy(&body[2..2 + name_end]);
        assert_eq!(field_name, "val");

        // Sync
        writer.write_all(&build_sync_message()).await.unwrap();
        let (msg_type, _body) = read_pg_message(&mut reader).await;
        assert_eq!(msg_type, b'Z');
    }

    #[tokio::test]
    async fn test_on_connect_called_with_actual_username() {
        use std::sync::Mutex;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Use a shared state to verify on_connect was called with correct username
        let connected_user = Arc::new(Mutex::new(String::new()));
        let connected_user_clone = connected_user.clone();

        struct UsernameCapturingHandler {
            captured: Arc<Mutex<String>>,
        }

        impl QueryHandler for UsernameCapturingHandler {
            fn handle_query(&self, _conn_id: u32, _sql: &str) -> QueryResult {
                QueryResult::ok()
            }

            fn on_connect(&self, _conn_id: u32, user: &str, _host: &str) {
                let mut captured = self.captured.lock().unwrap();
                *captured = user.to_string();
            }
        }

        let handler = Arc::new(UsernameCapturingHandler {
            captured: connected_user_clone,
        });
        let auth_config = AuthConfig {
            accept_any_password: true,
            ..Default::default()
        };

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut conn = PgConnection::new(stream, 1, handler, auth_config);
            let _ = conn.run().await;
        });

        let client_stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut reader, mut writer) = tokio::io::split(client_stream);

        // Use a specific username
        startup_and_auth(&mut reader, &mut writer, "mycustomuser", "testdb").await;

        // Do a simple query + sync to ensure on_connect was called
        let query_buf = build_sync_message();
        writer.write_all(&query_buf).await.unwrap();
        let (msg_type, _body) = read_pg_message(&mut reader).await;
        assert_eq!(msg_type, b'Z');

        // Drop reader/writer to close connection, then check
        drop(reader);
        drop(writer);

        // Give the server a moment to process
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let user = connected_user.lock().unwrap().clone();
        assert_eq!(
            user, "mycustomuser",
            "on_connect should be called with actual username, not 'root'"
        );
    }

    #[tokio::test]
    async fn test_decode_error_closes_connection() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handler = Arc::new(MockHandler);
        let auth_config = AuthConfig {
            accept_any_password: true,
            ..Default::default()
        };

        let server_finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let server_finished_clone = server_finished.clone();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut conn = PgConnection::new(stream, 1, handler, auth_config);
            let result = conn.run().await;
            // After a decode error, run() should return an Err
            assert!(
                result.is_err(),
                "connection should close after decode error, got: {:?}",
                result
            );
            server_finished_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        });

        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut reader, mut writer) = tokio::io::split(client);

        // Complete startup + auth normally
        startup_and_auth(&mut reader, &mut writer, "user", "db").await;

        // Send an invalid message type byte (0x00 is not a valid PG message type)
        let invalid_msg = vec![0x00, 0x00, 0x00, 0x00, 0x04]; // type=0x00, len=4
        writer.write_all(&invalid_msg).await.unwrap();

        // The server should close the connection. Reading should return 0 bytes.
        let mut buf = [0u8; 1];
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(2), reader.read(&mut buf)).await;

        match result {
            Ok(Ok(0)) => {} // Good - connection closed
            Ok(Ok(n)) => {
                panic!(
                    "expected connection closed (read 0), but read {} bytes: {:?}",
                    n, buf
                );
            }
            Ok(Err(e)) => {
                // Connection reset is also acceptable
                assert!(
                    e.kind() == std::io::ErrorKind::ConnectionReset
                        || e.kind() == std::io::ErrorKind::BrokenPipe,
                    "unexpected error: {}",
                    e
                );
            }
            Err(_) => {
                panic!("timeout waiting for connection to close after decode error");
            }
        }

        // Wait a bit for server_finished flag
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(
            server_finished.load(std::sync::atomic::Ordering::SeqCst),
            "server should have finished after decode error"
        );
    }

    // ======================================================================
    // Extended query error path tests
    // ======================================================================

    #[tokio::test]
    async fn test_bind_to_nonexistent_statement_returns_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handler = Arc::new(MockHandler);
        let auth_config = AuthConfig {
            accept_any_password: true,
            ..Default::default()
        };

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut conn = PgConnection::new(stream, 1, handler, auth_config);
            let _ = conn.run().await;
        });

        let client_stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut reader, mut writer) = tokio::io::split(client_stream);

        startup_and_auth(&mut reader, &mut writer, "user", "db").await;

        // Bind to a statement that was never parsed
        let bind_msg = build_bind_message("", "nonexistent_stmt", &[]);
        writer.write_all(&bind_msg).await.unwrap();

        // Should get ErrorResponse ('E'), NOT BindComplete ('2')
        let (msg_type, body) = read_pg_message(&mut reader).await;
        assert_eq!(
            msg_type, b'E',
            "expected ErrorResponse for bind to nonexistent statement, got 0x{:02x}",
            msg_type
        );
        let error_str = String::from_utf8_lossy(&body);
        assert!(
            error_str.contains("does not exist"),
            "Error should mention nonexistent statement: {}",
            error_str
        );

        // Sync and expect ReadyForQuery (error does not break connection)
        writer.write_all(&build_sync_message()).await.unwrap();
        let (msg_type, _body) = read_pg_message(&mut reader).await;
        assert_eq!(msg_type, b'Z', "expected ReadyForQuery after error + Sync");
    }

    #[tokio::test]
    async fn test_execute_unknown_portal_returns_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handler = Arc::new(MockHandler);
        let auth_config = AuthConfig {
            accept_any_password: true,
            ..Default::default()
        };

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut conn = PgConnection::new(stream, 1, handler, auth_config);
            let _ = conn.run().await;
        });

        let client_stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut reader, mut writer) = tokio::io::split(client_stream);

        startup_and_auth(&mut reader, &mut writer, "user", "db").await;

        // Execute with a portal that was never bound
        let execute_msg = build_execute_message("unknown_portal", 0);
        writer.write_all(&execute_msg).await.unwrap();

        // Should get ErrorResponse ('E')
        let (msg_type, body) = read_pg_message(&mut reader).await;
        assert_eq!(
            msg_type, b'E',
            "expected ErrorResponse for execute unknown portal, got 0x{:02x}",
            msg_type
        );
        let error_str = String::from_utf8_lossy(&body);
        assert!(
            error_str.contains("not found"),
            "Error should mention portal not found: {}",
            error_str
        );

        // ReadyForQuery should follow (handle_execute sends it after error)
        let (msg_type, _body) = read_pg_message(&mut reader).await;
        assert_eq!(msg_type, b'Z', "expected ReadyForQuery after execute error");
    }

    #[tokio::test]
    async fn test_execute_portal_after_close_returns_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handler = Arc::new(MockHandler);
        let auth_config = AuthConfig {
            accept_any_password: true,
            ..Default::default()
        };

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut conn = PgConnection::new(stream, 1, handler, auth_config);
            let _ = conn.run().await;
        });

        let client_stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut reader, mut writer) = tokio::io::split(client_stream);

        startup_and_auth(&mut reader, &mut writer, "user", "db").await;

        // Parse a statement
        writer
            .write_all(&build_parse_message("mystmt", "SELECT 1", &[]))
            .await
            .unwrap();
        loop {
            let (msg_type, _body) = read_pg_message(&mut reader).await;
            if msg_type == b'1' {
                break;
            }
        }

        // Bind to create portal for mystmt
        writer
            .write_all(&build_bind_message("", "mystmt", &[]))
            .await
            .unwrap();
        let (msg_type, _body) = read_pg_message(&mut reader).await;
        assert_eq!(msg_type, b'2');

        // Close the unnamed portal
        let mut close_body = vec![b'P', 0x00]; // target: Portal + empty name + null
        let mut close_msg = vec![b'C'];
        close_msg.extend_from_slice(&(close_body.len() as i32 + 4).to_be_bytes());
        close_msg.extend_from_slice(&close_body);
        writer.write_all(&close_msg).await.unwrap();
        let (msg_type, _body) = read_pg_message(&mut reader).await;
        assert_eq!(
            msg_type, b'3',
            "expected CloseComplete after closing portal"
        );

        // Execute the now-closed portal -> should get error
        let execute_msg = build_execute_message("", 0);
        writer.write_all(&execute_msg).await.unwrap();

        let (msg_type, _body) = read_pg_message(&mut reader).await;
        assert_eq!(
            msg_type, b'E',
            "expected ErrorResponse for execute closed portal, got 0x{:02x}",
            msg_type
        );

        let (msg_type, _body) = read_pg_message(&mut reader).await;
        assert_eq!(msg_type, b'Z');
    }

    #[tokio::test]
    async fn test_close_nonexistent_statement() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handler = Arc::new(MockHandler);
        let auth_config = AuthConfig {
            accept_any_password: true,
            ..Default::default()
        };

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut conn = PgConnection::new(stream, 1, handler, auth_config);
            let _ = conn.run().await;
        });

        let client_stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut reader, mut writer) = tokio::io::split(client_stream);

        startup_and_auth(&mut reader, &mut writer, "user", "db").await;

        // Close a statement that was never parsed — should still get CloseComplete
        let mut close_body = vec![b'S'];
        close_body.extend_from_slice(b"nonexistent_stmt");
        close_body.push(0);
        let mut close_msg = vec![b'C'];
        close_msg.extend_from_slice(&(close_body.len() as i32 + 4).to_be_bytes());
        close_msg.extend_from_slice(&close_body);
        writer.write_all(&close_msg).await.unwrap();

        let (msg_type, _body) = read_pg_message(&mut reader).await;
        assert_eq!(
            msg_type, b'3',
            "expected CloseComplete even for nonexistent statement, got 0x{:02x}",
            msg_type
        );

        writer.write_all(&build_sync_message()).await.unwrap();
        let (msg_type, _body) = read_pg_message(&mut reader).await;
        assert_eq!(msg_type, b'Z');
    }

    #[tokio::test]
    async fn test_close_nonexistent_portal() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handler = Arc::new(MockHandler);
        let auth_config = AuthConfig {
            accept_any_password: true,
            ..Default::default()
        };

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut conn = PgConnection::new(stream, 1, handler, auth_config);
            let _ = conn.run().await;
        });

        let client_stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut reader, mut writer) = tokio::io::split(client_stream);

        startup_and_auth(&mut reader, &mut writer, "user", "db").await;

        // Close a portal that was never bound — should still get CloseComplete
        let mut close_body = vec![b'P'];
        close_body.extend_from_slice(b"nonexistent_portal");
        close_body.push(0);
        let mut close_msg = vec![b'C'];
        close_msg.extend_from_slice(&(close_body.len() as i32 + 4).to_be_bytes());
        close_msg.extend_from_slice(&close_body);
        writer.write_all(&close_msg).await.unwrap();

        let (msg_type, _body) = read_pg_message(&mut reader).await;
        assert_eq!(
            msg_type, b'3',
            "expected CloseComplete even for nonexistent portal, got 0x{:02x}",
            msg_type
        );

        writer.write_all(&build_sync_message()).await.unwrap();
        let (msg_type, _body) = read_pg_message(&mut reader).await;
        assert_eq!(msg_type, b'Z');
    }

    #[tokio::test]
    async fn test_describe_nonexistent_statement_returns_nodata() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handler = Arc::new(MockHandler);
        let auth_config = AuthConfig {
            accept_any_password: true,
            ..Default::default()
        };

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut conn = PgConnection::new(stream, 1, handler, auth_config);
            let _ = conn.run().await;
        });

        let client_stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut reader, mut writer) = tokio::io::split(client_stream);

        startup_and_auth(&mut reader, &mut writer, "user", "db").await;

        // Describe a statement that was never parsed
        writer
            .write_all(&build_describe_message(b'S', "nonexistent_stmt"))
            .await
            .unwrap();
        let (msg_type, _body) = read_pg_message(&mut reader).await;
        assert_eq!(
            msg_type, b'n',
            "expected NoData for nonexistent statement, got 0x{:02x}",
            msg_type
        );

        writer.write_all(&build_sync_message()).await.unwrap();
        let (msg_type, _body) = read_pg_message(&mut reader).await;
        assert_eq!(msg_type, b'Z');
    }

    #[tokio::test]
    async fn test_describe_unnamed_portal_returns_nodata() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handler = Arc::new(MockHandler);
        let auth_config = AuthConfig {
            accept_any_password: true,
            ..Default::default()
        };

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut conn = PgConnection::new(stream, 1, handler, auth_config);
            let _ = conn.run().await;
        });

        let client_stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut reader, mut writer) = tokio::io::split(client_stream);

        startup_and_auth(&mut reader, &mut writer, "user", "db").await;

        // Describe the unnamed portal without binding first — should return NoData
        writer
            .write_all(&build_describe_message(b'P', ""))
            .await
            .unwrap();
        let (msg_type, _body) = read_pg_message(&mut reader).await;
        assert_eq!(
            msg_type, b'n',
            "expected NoData for unnamed portal, got 0x{:02x}",
            msg_type
        );

        writer.write_all(&build_sync_message()).await.unwrap();
        let (msg_type, _body) = read_pg_message(&mut reader).await;
        assert_eq!(msg_type, b'Z');
    }
}
