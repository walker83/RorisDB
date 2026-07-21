use bytes::{Buf, Bytes, BytesMut};
use rand::RngCore;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use crate::auth::{AuthError, AuthUser, Credentials};
use crate::packet::{
    self, CapabilityFlags, Column, HandshakeResponse, HandshakeV10, column_type, command,
};
use crate::server::{ColumnDef, ColumnType, QueryHandler, QueryResult};

/// The connection state machine phases.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Phase {
    Handshake,
    #[allow(dead_code)]
    Auth,
    Command,
    Closed,
}

/// Represents a single MySQL client connection.
pub struct Connection {
    stream: TcpStream,
    conn_id: u32,
    handler: Arc<dyn QueryHandler>,
    seq_id: u8,
    phase: Phase,
    capability_flags: u32,
    charset: u8,
    username: String,
    database: Option<String>,
    auth_user: Option<AuthUser>,
    /// The 20-byte auth salt sent in handshake (used for AuthSwitchRequest)
    auth_salt: [u8; 20],
    /// Prepared statements: stmt_id -> (sql, num_params, num_columns)
    prepared_statements: Vec<(String, u16, u16)>,
    next_stmt_id: u32,
    read_buf: BytesMut,
    /// Write buffer for batching TCP writes (avoids flush per row)
    write_buf: BytesMut,
    /// Authentication timeout in seconds
    auth_timeout_secs: u64,
    /// User credentials for password verification
    credentials: Credentials,
}

impl Connection {
    pub fn new(
        stream: TcpStream,
        conn_id: u32,
        handler: Arc<dyn QueryHandler>,
        auth_timeout_secs: u64,
        credentials: Credentials,
    ) -> Self {
        Self {
            stream,
            conn_id,
            handler,
            seq_id: 0,
            phase: Phase::Handshake,
            capability_flags: 0,
            charset: 0,
            username: String::new(),
            database: None,
            auth_user: None,
            auth_salt: [0u8; 20],
            prepared_statements: Vec::new(),
            next_stmt_id: 1,
            read_buf: BytesMut::with_capacity(16 * 1024),
            write_buf: BytesMut::with_capacity(64 * 1024),
            auth_timeout_secs,
            credentials,
        }
    }

    /// Run the connection through all phases until closed.
    pub async fn run(&mut self) -> std::io::Result<()> {
        info!("Connection {} starting handshake phase", self.conn_id);
        // Phase 1: Send handshake
        self.send_handshake().await?;
        info!(
            "Connection {} handshake sent, waiting for auth response (timeout={}s)",
            self.conn_id, self.auth_timeout_secs
        );

        // Phase 2: Receive auth response (with configurable timeout)
        let auth_result = timeout(
            Duration::from_secs(self.auth_timeout_secs),
            self.handle_auth_response(),
        )
        .await;
        match auth_result {
            Ok(Ok(())) => {
                info!(
                    "Connection {} auth successful, entering command loop",
                    self.conn_id
                );
            }
            Ok(Err(e)) => {
                warn!("Auth failed for conn {}: {}", self.conn_id, e);
                return Err(e);
            }
            Err(_) => {
                warn!(
                    "Auth timeout for conn {} after {}s",
                    self.conn_id, self.auth_timeout_secs
                );
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "auth timeout",
                ));
            }
        }

        // Phase 3: Command loop
        self.command_loop().await
    }

    // -----------------------------------------------------------------------
    // Handshake phase
    // -----------------------------------------------------------------------

    async fn send_handshake(&mut self) -> std::io::Result<()> {
        let mut salt = [0u8; 20];
        rand::thread_rng().fill_bytes(&mut salt);
        self.auth_salt = salt; // Save for potential AuthSwitchRequest
        let handshake = HandshakeV10::new(self.conn_id, self.auth_salt);
        let packet = handshake.encode(self.seq_id);
        self.seq_id = self.seq_id.wrapping_add(1);
        self.write_all(&packet).await
    }

    // -----------------------------------------------------------------------
    // Auth phase
    // -----------------------------------------------------------------------

    async fn handle_auth_response(&mut self) -> std::io::Result<()> {
        let seq_before = self.seq_id;
        debug!(
            "handle_auth_response: reading auth packet, current seq_id={}",
            seq_before
        );
        let payload = self.read_packet().await?;
        info!(
            "Connection {} received auth response: {} bytes, seq_id={}",
            self.conn_id,
            payload.len(),
            self.seq_id
        );

        let response = match HandshakeResponse::parse(&payload) {
            Ok(r) => r,
            Err(e) => {
                error!(
                    "Failed to parse handshake response ({} bytes): {}",
                    payload.len(),
                    e
                );
                debug!("Raw payload: {:02x?}", &payload[..payload.len().min(128)]);
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e));
            }
        };

        self.capability_flags = response.capability_flags & packet::DEFAULT_CAPABILITIES;
        self.charset = response.charset;
        self.username = response.username.clone();
        self.database = response.database.clone();

        info!(
            "Connection {} auth details: user={}, db={:?}, charset={}, client_caps=0x{:x}, server_caps=0x{:x}, auth_plugin={:?}",
            self.conn_id,
            self.username,
            self.database,
            crate::charset::charset_name(self.charset),
            response.capability_flags,
            self.capability_flags,
            response.auth_plugin_name
        );

        let auth_result = self
            .authenticate_user(
                &self.username,
                &response.auth_response,
                response.auth_plugin_name.as_deref(),
            )
            .await;

        match auth_result {
            Ok(auth_user) => {
                self.auth_user = Some(auth_user);
                // If database was specified in handshake, set it
                if let Some(ref db) = self.database {
                    if !db.is_empty() {
                        self.handler.set_database(self.conn_id, db);
                    }
                }
                self.send_ok(0, 0).await?;
                debug!("handle_auth_response: sent OK, seq_id now={}", self.seq_id);
                self.phase = Phase::Command;
                Ok(())
            }
            Err(auth_err) => {
                let err_msg = auth_err.to_string();
                debug!("Auth failed for user {}: {}", self.username, err_msg);
                self.send_auth_failure(1045, &err_msg).await
            }
        }
    }

    async fn authenticate_user(
        &self,
        username: &str,
        auth_response: &[u8],
        auth_plugin_name: Option<&str>,
    ) -> Result<AuthUser, AuthError> {
        use crate::auth::{AuthPlugin, NativePasswordAuth, TokenAuth, TokenConfig};

        if username.is_empty() {
            return Err(AuthError::Failed("Empty username".to_string()));
        }

        let plugin_name = auth_plugin_name.unwrap_or("mysql_native_password");

        match plugin_name {
            "mysql_native_password" => {
                let auth = NativePasswordAuth::with_credentials(self.credentials.clone());
                auth.authenticate(username, auth_response, &self.auth_salt)
                    .await
            }
            "auth_token" => {
                let jwt_secret = match std::env::var("RORIS_JWT_SECRET") {
                    Ok(s) if !s.is_empty() => s,
                    _ => {
                        tracing::error!("RORIS_JWT_SECRET not set — rejecting token auth");
                        return Err(AuthError::InvalidCredentials);
                    }
                };
                let config = TokenConfig::new(jwt_secret, 3600, "harnessdb".to_string());
                let auth = TokenAuth::new(config);
                auth.authenticate(username, auth_response, &self.auth_salt)
                    .await
            }
            _ => Err(AuthError::PluginNotSupported(plugin_name.to_string())),
        }
    }

    async fn send_auth_failure(&mut self, error_code: u16, message: &str) -> std::io::Result<()> {
        let pkt = packet::make_general_err(self.seq_id, error_code, message);
        self.write_all(&pkt).await?;
        self.seq_id = self.seq_id.wrapping_add(1);
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            message,
        ))
    }

    #[allow(dead_code)]
    async fn send_auth_switch_request(&mut self) -> std::io::Result<()> {
        // Build AuthSwitchRequest packet
        // Format: 0xFE (status) + plugin name + null + auth plugin data
        let mut pb = packet::PacketBuilder::new(self.seq_id);
        pb.put_u8(0xFE); // Status byte for auth switch
        pb.put_slice(b"mysql_native_password");
        pb.put_u8(0); // null terminator
        // Auth plugin data (20 bytes auth salt)
        pb.put_slice(&self.auth_salt);
        pb.put_u8(0); // null terminator

        let (pkt, next) = pb.finish();
        self.write_all(&pkt).await?;
        self.seq_id = next;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Command loop
    // -----------------------------------------------------------------------

    async fn command_loop(&mut self) -> std::io::Result<()> {
        info!("Connection {} entering command loop", self.conn_id);
        loop {
            let payload = match self.read_packet().await {
                Ok(p) => p,
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        info!("Connection {} client disconnected", self.conn_id);
                        break;
                    }
                    error!("Connection {} read error: {}", self.conn_id, e);
                    break;
                }
            };

            if payload.is_empty() {
                warn!("Connection {} empty command packet", self.conn_id);
                continue;
            }

            let cmd = payload[0];
            let data = &payload[1..];

            let cmd_str = match cmd {
                0x00 => "COM_SLEEP",
                0x01 => "COM_QUIT",
                0x02 => "COM_INIT_DB",
                0x03 => "COM_QUERY",
                0x04 => "COM_FIELD_LIST",
                0x05 => "COM_CREATE_DB",
                0x06 => "COM_DROP_DB",
                0x07 => "COM_REFRESH",
                0x08 => "COM_SHUTDOWN",
                0x09 => "COM_STATISTICS",
                0x0a => "COM_PROCESS_INFO",
                0x0b => "COM_CONNECT",
                0x0c => "COM_PROCESS_KILL",
                0x0d => "COM_DEBUG",
                0x0e => "COM_PING",
                0x0f => "COM_TIME",
                0x10 => "COM_DELAYED_INSERT",
                0x11 => "COM_CHANGE_USER",
                0x12 => "COM_BINLOG_DUMP",
                0x13 => "COM_TABLE_DUMP",
                0x14 => "COM_CONNECT_OUT",
                0x15 => "COM_REGISTER_SLAVE",
                0x16 => "COM_STMT_PREPARE",
                0x17 => "COM_STMT_EXECUTE",
                0x18 => "COM_STMT_SEND_LONG_DATA",
                0x19 => "COM_STMT_CLOSE",
                0x1a => "COM_STMT_RESET",
                0x1b => "COM_SET_OPTION",
                0x1c => "COM_STMT_FETCH",
                0x1d => "COM_DAEMON",
                0x1e => "COM_BINLOG_DUMP_GTID",
                0x1f => "COM_RESET_CONNECTION",
                _ => "UNKNOWN",
            };
            if cmd == 0x03 {
                info!(
                    "Connection {} command: {} ({} bytes) - SQL: {:?}",
                    self.conn_id,
                    cmd_str,
                    data.len(),
                    String::from_utf8_lossy(data)
                );
            } else {
                info!(
                    "Connection {} command: {} ({} bytes)",
                    self.conn_id,
                    cmd_str,
                    data.len()
                );
            }

            match cmd {
                command::COM_QUIT => {
                    info!("COM_QUIT");
                    let _ = self.send_ok(0, 0).await;
                    break;
                }
                command::COM_QUERY => {
                    let sql = String::from_utf8_lossy(data).to_string();
                    debug!("COM_QUERY: {}", sql);
                    if let Err(e) = self.handle_query(&sql).await {
                        error!("Query handler error: {}", e);
                        return Err(e);
                    }
                }
                command::COM_PING => {
                    debug!("COM_PING");
                    self.send_ok(0, 0).await?;
                }
                command::COM_INIT_DB => {
                    let db = String::from_utf8_lossy(data).to_string();
                    debug!("COM_INIT_DB: {}", db);
                    self.database = Some(db.clone());
                    self.handler.set_database(self.conn_id, &db);
                    self.send_ok(0, 0).await?;
                }
                command::COM_FIELD_LIST => {
                    // Parse table name from data (format: table_name\0[field_name\0])
                    let raw_name = String::from_utf8_lossy(data).trim_end_matches('\0').to_string();
                    let table_name = raw_name.split('\0').next().unwrap_or(&raw_name);
                    debug!("COM_FIELD_LIST: {}", table_name);
                    // Validate: reject empty or obviously malicious table names
                    if table_name.is_empty() {
                        self.send_general_err(1045, "Empty table name in COM_FIELD_LIST".to_string()).await?;
                    } else {
                        // Use backtick quoting and escape internal backticks to prevent SQL injection
                        let safe_table = table_name.replace('`', "``");
                        let result = self.handler.handle_query(
                            self.conn_id,
                            &format!("SELECT * FROM `{}` LIMIT 0", safe_table),
                        );
                        for col in &result.columns {
                            let col_def = packet::Column::new(&col.name, col.col_type as u8);
                            self.write_all(&col_def.encode(self.seq_id)).await?;
                            self.seq_id = self.seq_id.wrapping_add(1);
                        }
                        // Send EOF
                        self.write_all(&packet::make_eof_packet(
                            self.seq_id,
                            0,
                            packet::SERVER_STATUS_AUTOCOMMIT,
                        ))
                        .await?;
                        self.seq_id = self.seq_id.wrapping_add(1);
                    }
                }
                command::COM_STMT_PREPARE => {
                    let sql = String::from_utf8_lossy(data).to_string();
                    debug!("COM_STMT_PREPARE: {}", sql);
                    self.handle_stmt_prepare(&sql).await?;
                }
                command::COM_STMT_EXECUTE => {
                    debug!("COM_STMT_EXECUTE");
                    self.handle_stmt_execute(data).await?;
                }
                command::COM_STMT_CLOSE => {
                    debug!("COM_STMT_CLOSE");
                    self.handle_stmt_close(data);
                }
                command::COM_STMT_SEND_LONG_DATA => {
                    debug!("COM_STMT_SEND_LONG_DATA (ignored)");
                }
                command::COM_STMT_RESET => {
                    debug!("COM_STMT_RESET");
                    self.send_ok(0, 0).await?;
                }
                command::COM_STMT_FETCH => {
                    debug!("COM_STMT_FETCH (no more rows)");
                    // No cursor-based fetch support; send empty EOF
                    self.write_all(&packet::make_eof_packet(
                        self.seq_id,
                        0,
                        packet::SERVER_STATUS_AUTOCOMMIT,
                    ))
                    .await?;
                    self.seq_id = self.seq_id.wrapping_add(1);
                }
                command::COM_STATISTICS => {
                    debug!("COM_STATISTICS");
                    let stats = b"Uptime: 0  Threads: 1  Questions: 0  Slow queries: 0  Opens: 0  Flush tables: 0  Open tables: 0  Queries per second avg: 0.000";
                    let mut pb = packet::PacketBuilder::new(self.seq_id);
                    pb.put_slice(stats);
                    let (pkt, next) = pb.finish();
                    self.write_all(&pkt).await?;
                    self.seq_id = next;
                }
                command::COM_SET_OPTION => {
                    debug!("COM_SET_OPTION (ignored)");
                    self.send_ok(0, 0).await?;
                }
                command::COM_RESET_CONNECTION => {
                    debug!("COM_RESET_CONNECTION");
                    self.send_ok(0, 0).await?;
                }
                _ => {
                    warn!("Unsupported command: {:#x}", cmd);
                    self.send_general_err(1047, format!("Unknown command {:#x}", cmd))
                        .await?;
                }
            }
        }

        self.phase = Phase::Closed;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Query handling
    // -----------------------------------------------------------------------

    async fn handle_query(&mut self, sql: &str) -> std::io::Result<()> {
        // Check for special commands that some clients send
        let trimmed = sql.trim().to_lowercase();

        // Handle common MySQL client initialization queries
        // These are sent automatically by the mysql CLI on connect
        if trimmed.starts_with("select") {
            // select @@version_comment limit 1
            if trimmed.contains("@@version_comment") {
                let result = QueryResult::with_rows(
                    vec![ColumnDef {
                        name: "@@version_comment".to_string(),
                        col_type: ColumnType::String,
                    }],
                    vec![vec![Some("HarnessDB".to_string())]],
                );
                return self.send_result_set(result).await;
            }
            // select database()
            if trimmed.contains("database()") {
                let db = self.database.clone().unwrap_or_default();
                let result = QueryResult::with_rows(
                    vec![ColumnDef {
                        name: "database()".to_string(),
                        col_type: ColumnType::String,
                    }],
                    vec![vec![if db.is_empty() { None } else { Some(db) }]],
                );
                return self.send_result_set(result).await;
            }
            // select @@variable (various system variables)
            if trimmed.contains("@@") {
                // Extract variable name from query like "SELECT @@max_allowed_packet"
                // or "SELECT @@session.lower_case_table_names"
                let var_name = trimmed
                    .split("@@")
                    .nth(1)
                    .map(|s| {
                        s.trim()
                            .trim_end_matches(';')
                            .trim()
                    })
                    .unwrap_or("");
                // Strip session./global. prefix if present
                let clean_var = var_name
                    .strip_prefix("session.")
                    .or_else(|| var_name.strip_prefix("global."))
                    .unwrap_or(var_name);

                let (value, col_type) = match clean_var {
                    "max_allowed_packet" => (4194304.to_string(), ColumnType::Int), // 4MB default
                    "version" => ("8.0.33".to_string(), ColumnType::String),
                    "version_comment" => ("HarnessDB".to_string(), ColumnType::String),
                    "character_set_client"
                    | "character_set_connection"
                    | "character_set_results"
                    | "character_set_server" => ("utf8mb4".to_string(), ColumnType::String),
                    "collation_connection" | "collation_server" => {
                        ("utf8mb4_general_ci".to_string(), ColumnType::String)
                    }
                    "autocommit" => ("1".to_string(), ColumnType::Int),
                    "sql_mode" => ("".to_string(), ColumnType::String),
                    "time_zone" => ("SYSTEM".to_string(), ColumnType::String),
                    "wait_timeout" => (28800.to_string(), ColumnType::Int),
                    "interactive_timeout" => (28800.to_string(), ColumnType::Int),
                    "net_buffer_length" => (16384.to_string(), ColumnType::Int),
                    "lower_case_table_names" => ("0".to_string(), ColumnType::Int),
                    "have_ssl" => ("NO".to_string(), ColumnType::String),
                    "have_query_cache" => ("NO".to_string(), ColumnType::String),
                    "license" => ("Apache 2.0".to_string(), ColumnType::String),
                    "innodb_version" => (env!("CARGO_PKG_VERSION").to_string(), ColumnType::String),
                    "protocol_version" => ("10".to_string(), ColumnType::Int),
                    "tmpdir" => ("/tmp".to_string(), ColumnType::String),
                    "datadir" => ("".to_string(), ColumnType::String),
                    // Variables required by MySQL Connector/J 8.0.x (Spark JDBC compatibility)
                    "auto_increment_increment" => ("1".to_string(), ColumnType::Int),
                    "auto_increment_offset" => ("1".to_string(), ColumnType::Int),
                    "tx_read_only" | "transaction_read_only" => {
                        ("0".to_string(), ColumnType::Int)
                    }
                    "tx_isolation" | "transaction_isolation" => {
                        ("REPEATABLE-READ".to_string(), ColumnType::String)
                    }
                    "version_compile_os" => ("Linux".to_string(), ColumnType::String),
                    "version_compile_machine" => ("x86_64".to_string(), ColumnType::String),
                    "init_connect" => ("".to_string(), ColumnType::String),
                    "character_set_database" => ("utf8mb4".to_string(), ColumnType::String),
                    "collation_database" => {
                        ("utf8mb4_general_ci".to_string(), ColumnType::String)
                    }
                    _ => ("".to_string(), ColumnType::String),
                };

                let result = QueryResult::with_rows(
                    vec![ColumnDef {
                        name: format!("@@{}", var_name),
                        col_type,
                    }],
                    vec![vec![Some(value)]],
                );
                return self.send_result_set(result).await;
            }
        }

        // Handle transaction commands
        if trimmed.starts_with("begin") || trimmed.starts_with("start transaction") {
            return self.send_ok(0, 0).await;
        }
        if trimmed.starts_with("commit") || trimmed.starts_with("rollback") {
            // Accept these silently (ignore errors about no transaction)
            return self.send_ok(0, 0).await;
        }

        // Handle SET commands (autocommit, names, etc.)
        if trimmed.starts_with("set ") {
            // SET AUTOCOMMIT = 0|1, SET @@autocommit = 0|1, etc.
            if trimmed.contains("autocommit")
                || trimmed.contains("names ")
                || trimmed.contains("character_set")
            {
                return self.send_ok(0, 0).await;
            }
        }

        // Handle SHOW commands
        if trimmed.starts_with("show ") {
            if trimmed.contains("warnings") || trimmed.contains("errors") {
                let result = QueryResult::with_rows(
                    vec![
                        ColumnDef {
                            name: "Level".to_string(),
                            col_type: ColumnType::String,
                        },
                        ColumnDef {
                            name: "Code".to_string(),
                            col_type: ColumnType::Int,
                        },
                        ColumnDef {
                            name: "Message".to_string(),
                            col_type: ColumnType::String,
                        },
                    ],
                    vec![],
                );
                return self.send_result_set(result).await;
            }
            // SHOW VARIABLES, SHOW STATUS, SHOW PROCESSLIST are now forwarded to the handler
        }

        let result = self.handler.handle_query(self.conn_id, sql);
        // Sync connection database state for USE commands (reuse trimmed, no double lowercase)
        if trimmed.starts_with("use ") {
            // Extract database name from USE statement
            if let Some(pos) = trimmed.find("use ") {
                let after_use = &trimmed[pos + 4..].trim().trim_end_matches(';').trim();
                let db_name = after_use.split_whitespace().next().unwrap_or(after_use);
                if !db_name.is_empty() {
                    self.database = Some(db_name.to_string());
                    self.handler.set_database(self.conn_id, db_name);
                }
            }
        }
        self.send_result_set(result).await
    }

    // -----------------------------------------------------------------------
    // Result set encoding
    // -----------------------------------------------------------------------

    async fn send_result_set(&mut self, result: QueryResult) -> std::io::Result<()> {
        use std::time::Instant;
        let send_start = Instant::now();

        if result.columns.is_empty() {
            self.send_ok(result.rows.len() as u64, 0).await?;
            return Ok(());
        }

        let use_eof = (self.capability_flags & CapabilityFlags::DEPRECATE_EOF) == 0;
        let row_count = if result.pre_encoded_rows.is_some() {
            result.pre_encoded_row_count
        } else {
            result.rows.len()
        };

        self.write_buf.clear();

        let encode_start = Instant::now();

        let mut pb = packet::PacketBuilder::new(self.seq_id);
        pb.lenenc_int(result.columns.len() as u64);
        let (pkt, next) = pb.finish();
        self.write_buf.extend_from_slice(&pkt);
        self.seq_id = next;

        let columns: Vec<Column> = result.columns.iter().map(|c| c.into()).collect();
        for col in &columns {
            let pkt = col.encode(self.seq_id);
            self.write_buf.extend_from_slice(&pkt);
            self.seq_id = self.seq_id.wrapping_add(1);
        }

        if use_eof {
            let eof = packet::make_eof_packet(self.seq_id, 0, packet::SERVER_STATUS_AUTOCOMMIT);
            self.write_buf.extend_from_slice(&eof);
            self.seq_id = self.seq_id.wrapping_add(1);
        }

        self.stream.write_all(&self.write_buf).await?;
        self.write_buf.clear();

        let encode_ms = encode_start.elapsed().as_millis();
        let write_start = Instant::now();

        if let Some(mut encoded_rows) = result.pre_encoded_rows {
            let mut seq = self.seq_id;
            let mut offset = 0;
            while offset + 4 <= encoded_rows.len() {
                let payload_len = encoded_rows[offset] as usize
                    | ((encoded_rows[offset + 1] as usize) << 8)
                    | ((encoded_rows[offset + 2] as usize) << 16);
                encoded_rows[offset + 3] = seq;
                seq = seq.wrapping_add(1);
                offset += 4 + payload_len;
            }
            self.seq_id = seq;

            // Write all encoded rows in one shot to minimize syscall overhead.
            // The OS TCP stack handles segmentation; we shouldn't artificially chunk.
            self.stream.write_all(&encoded_rows).await?;
        } else {
            let estimated_size = result.rows.len() * result.columns.len() * 20 + 4096;
            self.write_buf.reserve(estimated_size.min(16 * 1024 * 1024));
            for row in &result.rows {
                self.seq_id =
                    packet::encode_text_row_strings_into(self.seq_id, row, &mut self.write_buf);
            }
            if !self.write_buf.is_empty() {
                self.stream.write_all(&self.write_buf).await?;
                self.write_buf.clear();
            }
        }

        if use_eof {
            let eof = packet::make_eof_packet(self.seq_id, 0, packet::SERVER_STATUS_AUTOCOMMIT);
            self.write_buf.extend_from_slice(&eof);
            self.seq_id = self.seq_id.wrapping_add(1);
        } else {
            let ok = packet::make_result_set_eof_ok_packet(
                self.seq_id,
                row_count as u64,
                0,
                packet::SERVER_STATUS_AUTOCOMMIT,
                0,
            );
            self.write_buf.extend_from_slice(&ok);
            self.seq_id = self.seq_id.wrapping_add(1);
        }
        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;
        self.write_buf.clear();

        let write_ms = write_start.elapsed().as_millis();
        let total_ms = send_start.elapsed().as_millis();

        if row_count > 1000 {
            tracing::info!(
                "send_result_set timing: encode={}ms, write+flush={}ms, total={}ms, rows={}",
                encode_ms,
                write_ms,
                total_ms,
                row_count
            );
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Prepared statement handling
    // -----------------------------------------------------------------------

    async fn handle_stmt_prepare(&mut self, sql: &str) -> std::io::Result<()> {
        info!("Connection {} COM_STMT_PREPARE: {}", self.conn_id, sql);
        // For now, parse the statement but don't actually bind parameters.
        // We store it and assign a statement ID.
        let stmt_id = self.next_stmt_id;
        self.next_stmt_id += 1;

        // Parse placeholders (?) for parameter count
        let num_params = count_placeholders(sql) as u16;

        // Check if this is a DML statement (INSERT/UPDATE/DELETE)
        // For DML, we don't execute during PREPARE - just validate syntax
        let upper_sql = sql.trim().to_uppercase();
        let is_dml = upper_sql.starts_with("INSERT")
            || upper_sql.starts_with("UPDATE")
            || upper_sql.starts_with("DELETE");

        let (num_columns, result_columns) = if is_dml {
            // DML statements don't return columns during PREPARE
            (0, Vec::new())
        } else {
            // Execute the query to determine columns
            // For queries with placeholders, replace ? with dummy values to get schema
            let exec_sql = if num_params > 0 {
                self.replace_placeholders_with_dummy(sql, num_params)
            } else {
                sql.to_string()
            };
            info!(
                "Connection {} COM_STMT_PREPARE exec_sql: {}",
                self.conn_id, exec_sql
            );

            let result = self.handler.handle_query(self.conn_id, &exec_sql);
            (result.columns.len() as u16, result.columns)
        };
        info!(
            "Connection {} COM_STMT_PREPARE result: {} columns, {} params",
            self.conn_id, num_columns, num_params
        );

        // Store the statement
        self.prepared_statements
            .push((sql.to_string(), num_params, num_columns));

        let use_eof = (self.capability_flags & CapabilityFlags::DEPRECATE_EOF) == 0;

        // Batch all prepare response packets into write_buf, single flush
        self.write_buf.clear();

        // Send COM_STMT_PREPARE_OK
        let pkt = packet::make_stmt_prepare_ok(self.seq_id, stmt_id, num_columns, num_params, 0);
        self.write_buf.extend_from_slice(&pkt);
        self.seq_id = self.seq_id.wrapping_add(1);

        // Send parameter column definitions (if any)
        for i in 0..num_params {
            let col = Column::new(&format!("param_{}", i), column_type::VAR_STRING);
            let pkt = col.encode(self.seq_id);
            self.write_buf.extend_from_slice(&pkt);
            self.seq_id = self.seq_id.wrapping_add(1);
        }

        if num_params > 0 && use_eof {
            let eof = packet::make_eof_packet(self.seq_id, 0, packet::SERVER_STATUS_AUTOCOMMIT);
            self.write_buf.extend_from_slice(&eof);
            self.seq_id = self.seq_id.wrapping_add(1);
        }

        // Send result column definitions (if any)
        for col_def in &result_columns {
            let col: Column = col_def.into();
            let pkt = col.encode(self.seq_id);
            self.write_buf.extend_from_slice(&pkt);
            self.seq_id = self.seq_id.wrapping_add(1);
        }

        if num_columns > 0 && use_eof {
            let eof = packet::make_eof_packet(self.seq_id, 0, packet::SERVER_STATUS_AUTOCOMMIT);
            self.write_buf.extend_from_slice(&eof);
            self.seq_id = self.seq_id.wrapping_add(1);
        }

        // Single write + flush for entire prepare response
        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;
        self.write_buf.clear();

        Ok(())
    }

    async fn handle_stmt_execute(&mut self, data: &[u8]) -> std::io::Result<()> {
        if data.len() < 9 {
            self.send_general_err(1045, "Malformed COM_STMT_EXECUTE".to_string())
                .await?;
            return Ok(());
        }

        // Parse statement ID (4 bytes LE)
        let stmt_id = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;

        if self.is_stmt_closed(stmt_id) {
            self.send_general_err(1045, "Unknown prepared statement".to_string())
                .await?;
            return Ok(());
        }

        let (sql, num_params, _stored_num_cols) = &self.prepared_statements[stmt_id - 1];
        let num_params = *num_params;

        // Parse and bind parameters if any
        let bound_sql = if num_params > 0 && data.len() > 9 {
            self.bind_params(sql, data, num_params)
        } else {
            sql.clone()
        };

        info!(
            "Connection {} COM_STMT_EXECUTE: {} (bound: {})",
            self.conn_id, sql, bound_sql
        );

        // Execute the SQL with bound parameters
        let result = self.handler.handle_query(self.conn_id, &bound_sql);

        // Use actual column count from result, not stored value
        let actual_num_cols = result.columns.len() as u16;
        info!(
            "Connection {} COM_STMT_EXECUTE result: {} columns, {} rows",
            self.conn_id,
            actual_num_cols,
            result.rows.len()
        );

        // Send result set using binary protocol
        self.send_binary_result_set(result, actual_num_cols).await
    }

    /// Replace ? placeholders with dummy values for PREPARE phase.
    /// This allows us to execute the query to get schema even with placeholders.
    fn replace_placeholders_with_dummy(&self, sql: &str, num_params: u16) -> String {
        // Use NULL as dummy value - most compatible with different column types
        let dummy_values: Vec<String> = (0..num_params).map(|_| "NULL".to_string()).collect();
        self.replace_placeholders(sql, &dummy_values)
    }

    /// Bind parameters from COM_STMT_EXECUTE packet to SQL placeholders.
    fn bind_params(&self, sql: &str, data: &[u8], num_params: u16) -> String {
        // COM_STMT_EXECUTE packet format after stmt_id:
        // 1 byte: flags
        // 4 bytes: iteration count
        // (num_params + 7) / 8 bytes: NULL bitmap
        // 1 byte: new_params_bound_flag (if 1, followed by type info for each param)
        // If new_params_bound_flag == 1:
        //   For each param: 2 bytes type (1 byte type_code, 1 byte unsigned flag)
        // Then: parameter values (for non-NULL params)

        if data.len() < 10 {
            return sql.to_string();
        }

        let _flags = data[4];
        let _iteration = u32::from_le_bytes([data[5], data[6], data[7], data[8]]);

        // NULL bitmap starts at offset 9
        let null_bitmap_size = ((num_params as usize + 7) / 8).max(1);
        if data.len() < 9 + null_bitmap_size {
            return sql.to_string();
        }

        let null_bitmap = &data[9..9 + null_bitmap_size];

        // Check if new params are bound
        let new_params_bound = if data.len() > 9 + null_bitmap_size {
            data[9 + null_bitmap_size] == 1
        } else {
            false
        };

        // First, read ALL parameter types (if new_params_bound)
        // MySQL protocol sends all type info first, then all values
        let mut offset = if new_params_bound {
            9 + null_bitmap_size + 1
        } else {
            9 + null_bitmap_size
        };

        let param_types: Vec<(u8, bool)> = if new_params_bound {
            let mut types = Vec::new();
            for _ in 0..num_params {
                if offset + 2 <= data.len() {
                    let type_byte = data[offset];
                    let unsigned_flag = data[offset + 1];
                    types.push((type_byte, unsigned_flag != 0));
                    offset += 2;
                } else {
                    types.push((column_type::VAR_STRING, false));
                }
            }
            types
        } else {
            // Default all to string type
            (0..num_params)
                .map(|_| (column_type::VAR_STRING, false))
                .collect()
        };

        // Now read ALL parameter values
        let mut param_values: Vec<String> = Vec::new();
        for (i, (param_type, is_unsigned)) in param_types.iter().enumerate() {
            // Check NULL bitmap
            let is_null = (null_bitmap[i / 8] & (1 << (i % 8))) != 0;

            if is_null {
                param_values.push("NULL".to_string());
                continue;
            }

            // Read value based on type - returns (value, bytes_consumed)
            let (value, size) = self.read_param_value(data, offset, *param_type, *is_unsigned);
            param_values.push(value);

            // Advance offset based on actual bytes consumed
            offset += size;
        }

        // Replace ? placeholders with values
        self.replace_placeholders(sql, &param_values)
    }

    /// Read a parameter value from the packet data.
    /// Returns (value_string, bytes_consumed).
    fn read_param_value(
        &self,
        data: &[u8],
        offset: usize,
        param_type: u8,
        _is_unsigned: bool,
    ) -> (String, usize) {
        if offset >= data.len() {
            return ("NULL".to_string(), 0);
        }

        match param_type {
            column_type::TINY => {
                if offset + 1 <= data.len() {
                    (data[offset].to_string(), 1)
                } else {
                    ("0".to_string(), 0)
                }
            }
            column_type::SHORT => {
                if offset + 2 <= data.len() {
                    (
                        i16::from_le_bytes([data[offset], data[offset + 1]]).to_string(),
                        2,
                    )
                } else {
                    ("0".to_string(), 0)
                }
            }
            column_type::LONG => {
                if offset + 4 <= data.len() {
                    (
                        i32::from_le_bytes([
                            data[offset],
                            data[offset + 1],
                            data[offset + 2],
                            data[offset + 3],
                        ])
                        .to_string(),
                        4,
                    )
                } else {
                    ("0".to_string(), 0)
                }
            }
            column_type::LONGLONG => {
                if offset + 8 <= data.len() {
                    (
                        i64::from_le_bytes([
                            data[offset],
                            data[offset + 1],
                            data[offset + 2],
                            data[offset + 3],
                            data[offset + 4],
                            data[offset + 5],
                            data[offset + 6],
                            data[offset + 7],
                        ])
                        .to_string(),
                        8,
                    )
                } else {
                    ("0".to_string(), 0)
                }
            }
            column_type::FLOAT => {
                if offset + 4 <= data.len() {
                    (
                        f32::from_le_bytes([
                            data[offset],
                            data[offset + 1],
                            data[offset + 2],
                            data[offset + 3],
                        ])
                        .to_string(),
                        4,
                    )
                } else {
                    ("0.0".to_string(), 0)
                }
            }
            column_type::DOUBLE => {
                if offset + 8 <= data.len() {
                    (
                        f64::from_le_bytes([
                            data[offset],
                            data[offset + 1],
                            data[offset + 2],
                            data[offset + 3],
                            data[offset + 4],
                            data[offset + 5],
                            data[offset + 6],
                            data[offset + 7],
                        ])
                        .to_string(),
                        8,
                    )
                } else {
                    ("0.0".to_string(), 0)
                }
            }
            column_type::VAR_STRING | column_type::VARCHAR | column_type::BLOB => {
                // Length-encoded string
                if offset >= data.len() {
                    return ("''".to_string(), 0);
                }
                let len_byte = data[offset];
                if len_byte < 0xFB {
                    // 1-byte length
                    let len = len_byte as usize;
                    if offset + 1 + len <= data.len() {
                        let s = String::from_utf8_lossy(&data[offset + 1..offset + 1 + len]);
                        // Escape single quotes for SQL
                        (format!("'{}'", s.replace("'", "''")), 1 + len)
                    } else {
                        ("''".to_string(), 0)
                    }
                } else if len_byte == 0xFC {
                    // 2-byte length
                    if offset + 3 <= data.len() {
                        let len = u16::from_le_bytes([data[offset + 1], data[offset + 2]]) as usize;
                        if offset + 3 + len <= data.len() {
                            let s = String::from_utf8_lossy(&data[offset + 3..offset + 3 + len]);
                            (format!("'{}'", s.replace("'", "''")), 3 + len)
                        } else {
                            ("''".to_string(), 0)
                        }
                    } else {
                        ("''".to_string(), 0)
                    }
                } else {
                    ("''".to_string(), 0)
                }
            }
            _ => ("NULL".to_string(), 0),
        }
    }

    /// Replace ? placeholders in SQL with actual values.
    fn replace_placeholders(&self, sql: &str, values: &[String]) -> String {
        let mut result = String::new();
        let mut in_string = false;
        let mut string_char = b'\0';
        let mut param_idx = 0;
        let sql_bytes = sql.as_bytes();

        for (i, &b) in sql_bytes.iter().enumerate() {
            if in_string {
                if b == string_char {
                    // Count consecutive preceding backslashes to detect escaped quotes
                    let mut backslash_count = 0usize;
                    let mut j = i;
                    while j > 0 {
                        j -= 1;
                        if sql_bytes[j] == b'\\' {
                            backslash_count += 1;
                        } else {
                            break;
                        }
                    }
                    if backslash_count % 2 == 0 {
                        // Even backslashes: real quote boundary
                        in_string = false;
                    }
                    // Odd backslashes: escaped quote, stay in string
                }
                result.push(b as char);
            } else if b == b'\'' || b == b'"' {
                in_string = true;
                string_char = b;
                result.push(b as char);
            } else if b == b'?' {
                if param_idx < values.len() {
                    result.push_str(&values[param_idx]);
                    param_idx += 1;
                } else {
                    result.push('?');
                }
            } else {
                result.push(b as char);
            }
        }
        result
    }

    /// Send result set in binary protocol format (for COM_STMT_EXECUTE).
    /// Optimized: batches all packets into write_buf, single flush at end.
    async fn send_binary_result_set(
        &mut self,
        result: QueryResult,
        num_columns: u16,
    ) -> std::io::Result<()> {
        use std::time::Instant;
        let send_start = Instant::now();

        info!(
            "Connection {} send_binary_result_set: {} columns, {} rows, columns={:?}",
            self.conn_id,
            num_columns,
            result.rows.len(),
            result
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>()
        );

        // For prepared statements, always send column count and definitions
        // even if result.columns is empty (DDL) or num_columns is 0
        if num_columns == 0 {
            // This was a DDL or non-select statement
            info!(
                "Connection {} send_binary_result_set: num_columns=0, sending OK",
                self.conn_id
            );
            self.send_ok(0, 0).await?;
            return Ok(());
        }

        let use_eof = (self.capability_flags & CapabilityFlags::DEPRECATE_EOF) == 0;
        let row_count = result.rows.len();

        self.write_buf.clear();

        // 1. Column count packet (lenenc int)
        let mut pb = packet::PacketBuilder::new(self.seq_id);
        pb.lenenc_int(num_columns as u64);
        let (pkt, next) = pb.finish();
        self.write_buf.extend_from_slice(&pkt);
        self.seq_id = next;

        // 2. Column definition packets (same as text protocol)
        let columns: Vec<Column> = result.columns.iter().map(|c| c.into()).collect();
        for col in &columns {
            let pkt = col.encode(self.seq_id);
            self.write_buf.extend_from_slice(&pkt);
            self.seq_id = self.seq_id.wrapping_add(1);
        }

        // 3. EOF packet (if not deprecated)
        if use_eof {
            let eof = packet::make_eof_packet(self.seq_id, 0, packet::SERVER_STATUS_AUTOCOMMIT);
            self.write_buf.extend_from_slice(&eof);
            self.seq_id = self.seq_id.wrapping_add(1);
        }

        // Send column definitions + EOF in one shot
        self.stream.write_all(&self.write_buf).await?;
        self.write_buf.clear();

        let write_start = Instant::now();

        // 4. Binary row data packets — batch into write_buf, flush periodically
        let estimated_row_size = num_columns as usize * 40 + 20;
        let batch_capacity = (row_count * estimated_row_size).min(4 * 1024 * 1024);
        self.write_buf.reserve(batch_capacity);

        for row in &result.rows {
            // Convert text values to binary values based on column types
            let binary_values: Vec<Option<packet::BinaryValue>> = row
                .iter()
                .zip(columns.iter())
                .map(|(val, col)| {
                    packet::text_to_binary(val.as_ref().map(|s| s.as_str()), col.column_type)
                })
                .collect();

            self.seq_id = packet::encode_binary_row_into(self.seq_id, &binary_values, num_columns, &mut self.write_buf);

            // Flush when buffer gets large (4MB threshold)
            if self.write_buf.len() >= 4 * 1024 * 1024 {
                self.stream.write_all(&self.write_buf).await?;
                self.write_buf.clear();
            }
        }

        // Send remaining row data
        if !self.write_buf.is_empty() {
            self.stream.write_all(&self.write_buf).await?;
            self.write_buf.clear();
        }

        // 5. Final EOF or OK
        if use_eof {
            let eof = packet::make_eof_packet(self.seq_id, 0, packet::SERVER_STATUS_AUTOCOMMIT);
            self.write_buf.extend_from_slice(&eof);
            self.seq_id = self.seq_id.wrapping_add(1);
        } else {
            // DEPRECATE_EOF: result-set terminator uses 0xFE header (not 0x00)
            let ok = packet::make_result_set_eof_ok_packet(
                self.seq_id,
                row_count as u64,
                0,
                packet::SERVER_STATUS_AUTOCOMMIT,
                0,
            );
            self.write_buf.extend_from_slice(&ok);
            self.seq_id = self.seq_id.wrapping_add(1);
        }
        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;
        self.write_buf.clear();

        let write_ms = write_start.elapsed().as_millis();
        let total_ms = send_start.elapsed().as_millis();

        if row_count > 1000 {
            tracing::info!(
                "send_binary_result_set timing: write+flush={}ms, total={}ms, rows={}",
                write_ms,
                total_ms,
                row_count
            );
        }

        Ok(())
    }

    fn handle_stmt_close(&mut self, data: &[u8]) {
        if data.len() >= 4 {
            let stmt_id = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
            if stmt_id > 0 && stmt_id <= self.prepared_statements.len() {
                // Mark as closed by setting empty SQL and invalid param count
                self.prepared_statements[stmt_id - 1] = (String::new(), 0, 0);
            }
        }
        // COM_STMT_CLOSE has no response
    }

    fn is_stmt_closed(&self, stmt_id: usize) -> bool {
        if stmt_id == 0 || stmt_id > self.prepared_statements.len() {
            return true;
        }
        self.prepared_statements[stmt_id - 1].0.is_empty()
    }

    // -----------------------------------------------------------------------
    // Packet I/O helpers
    // -----------------------------------------------------------------------

    /// Read one complete MySQL packet from the stream.
    /// Returns just the payload (without header).
    /// Updates self.seq_id so the next write uses the correct sequence ID.
    async fn read_packet(&mut self) -> std::io::Result<Bytes> {
        loop {
            // Try to parse a complete packet from the buffer
            if self.read_buf.len() >= 4 {
                let (payload_len, _seq) =
                    packet::read_packet_header(&self.read_buf).ok_or_else(|| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, "bad header")
                    })?;

                let total = 4 + payload_len;
                if self.read_buf.len() >= total {
                    let recv_seq = self.read_buf[3];
                    // Sync sequence ID: next packet we send should be recv_seq + 1
                    self.seq_id = recv_seq.wrapping_add(1);
                    // Extract payload bytes
                    let payload: Bytes = self.read_buf[4..total].to_vec().into();
                    // Remove consumed bytes from the buffer: advance past the full packet
                    self.read_buf.advance(total);
                    return Ok(payload);
                }
            }

            // Need more data
            let n = self.stream.read_buf(&mut self.read_buf).await?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed",
                ));
            }
        }
    }

    /// Send an OK packet.
    async fn send_ok(&mut self, affected_rows: u64, last_insert_id: u64) -> std::io::Result<()> {
        let pkt = packet::make_ok_packet(
            self.seq_id,
            affected_rows,
            last_insert_id,
            packet::SERVER_STATUS_AUTOCOMMIT,
            0,
        );
        self.write_all(&pkt).await?;
        self.seq_id = self.seq_id.wrapping_add(1);
        Ok(())
    }

    /// Send a general error packet.
    async fn send_general_err(&mut self, error_code: u16, message: String) -> std::io::Result<()> {
        let pkt = packet::make_general_err(self.seq_id, error_code, &message);
        self.write_all(&pkt).await?;
        self.seq_id = self.seq_id.wrapping_add(1);
        Ok(())
    }

    /// Write all bytes to the stream (with flush — for single-packet responses).
    async fn write_all(&mut self, data: &[u8]) -> std::io::Result<()> {
        self.stream.write_all(data).await?;
        self.stream.flush().await
    }

    /// Write bytes to the stream WITHOUT flushing (for batching multiple packets).
    async fn write_no_flush(&mut self, data: &[u8]) -> std::io::Result<()> {
        self.stream.write_all(data).await
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Count `?` placeholders in a SQL string (simple, non-recursive).
fn count_placeholders(sql: &str) -> usize {
    let mut count = 0;
    let mut in_string = false;
    let mut string_char = b'\0';
    let sql_bytes = sql.as_bytes();

    for (i, &b) in sql_bytes.iter().enumerate() {
        if in_string {
            if b == string_char {
                // Count consecutive preceding backslashes
                let mut backslash_count = 0usize;
                let mut j = i;
                while j > 0 {
                    j -= 1;
                    if sql_bytes[j] == b'\\' {
                        backslash_count += 1;
                    } else {
                        break;
                    }
                }
                if backslash_count % 2 == 0 {
                    in_string = false;
                }
            }
        } else if b == b'\'' || b == b'"' {
            in_string = true;
            string_char = b;
        } else if b == b'?' {
            count += 1;
        }
    }
    count
}
