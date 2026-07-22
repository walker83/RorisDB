use bytes::BytesMut;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{Instrument, error, info, info_span, warn};

use crate::auth::{AuthPluginType, Credentials, default_credentials};
use crate::connection::Connection;
use crate::packet::Column;

/// Configuration for the MySQL server.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind_addr: String,
    pub port: u16,
    pub default_auth_plugin: AuthPluginType,
    /// Authentication timeout in seconds. Default: 30 seconds.
    /// Connection pools often need more time for handshake.
    pub auth_timeout_secs: u64,
    /// Maximum concurrent connections. Default: 100.
    pub max_connections: u32,
    /// User credentials: username → SHA1(SHA1(password)).
    pub credentials: Credentials,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1".to_string(),
            port: 9030,
            default_auth_plugin: AuthPluginType::NativePassword,
            auth_timeout_secs: 30,
            max_connections: 100,
            credentials: default_credentials(),
        }
    }
}

/// Result of a query execution, returned by the QueryHandler.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<ColumnDef>,
    pub rows: Vec<Vec<Option<String>>>,
    /// Pre-encoded MySQL row packets (bypasses String materialization).
    /// Contains all row packets with seq_id=0 placeholders; Connection patches seq_ids in-place.
    pub pre_encoded_rows: Option<BytesMut>,
    pub pre_encoded_row_count: usize,
    /// Explicit affected-row count for DML statements that do not return a
    /// result set. When `Some`, the protocol layer uses this value for the
    /// OK packet's affected_rows field instead of `rows.len()`. This lets a
    /// handler report how many rows an INSERT/UPDATE/DELETE touched without
    /// fabricating dummy rows.
    pub affected_rows: Option<u64>,
}

/// Column definition for query results.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub col_type: ColumnType,
}

/// Simplified column type for query result descriptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    String,
    Int,
    Float,
    Double,
    Date,
    DateTime,
    Blob,
}

impl QueryResult {
    /// Create an empty result set with the given columns.
    pub fn new(columns: Vec<ColumnDef>) -> Self {
        Self {
            columns,
            rows: Vec::new(),
            pre_encoded_rows: None,
            pre_encoded_row_count: 0,
            affected_rows: None,
        }
    }

    /// Create a result from columns and rows.
    pub fn with_rows(columns: Vec<ColumnDef>, rows: Vec<Vec<Option<String>>>) -> Self {
        Self {
            columns,
            rows,
            pre_encoded_rows: None,
            pre_encoded_row_count: 0,
            affected_rows: None,
        }
    }

    /// Create an empty OK-style result (e.g. for DDL statements).
    pub fn ok() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            pre_encoded_rows: None,
            pre_encoded_row_count: 0,
            affected_rows: None,
        }
    }

    /// Create an OK-style result carrying an explicit affected-row count for a
    /// DML statement (INSERT/UPDATE/DELETE). The protocol layer reports this
    /// count to the client in the OK packet.
    pub fn ok_with_affected(affected_rows: u64) -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            pre_encoded_rows: None,
            pre_encoded_row_count: 0,
            affected_rows: Some(affected_rows),
        }
    }

    /// Create a result with pre-encoded row data (zero-allocation Arrow path).
    pub fn with_encoded_rows(
        columns: Vec<ColumnDef>,
        encoded_rows: BytesMut,
        row_count: usize,
    ) -> Self {
        Self {
            columns,
            rows: Vec::new(),
            pre_encoded_rows: Some(encoded_rows),
            pre_encoded_row_count: row_count,
            affected_rows: None,
        }
    }
}

/// Trait that callers implement to handle SQL queries from MySQL clients.
pub trait QueryHandler: Send + Sync + 'static {
    fn handle_query(&self, conn_id: u32, sql: &str) -> QueryResult;
    /// Called when client changes database (USE command). Default: do nothing.
    fn set_database(&self, _conn_id: u32, _db: &str) {}
    /// Called when a new client connection is established.
    fn on_connect(&self, _conn_id: u32, _user: &str, _host: &str) {}
    /// Called when a client connection is closed.
    fn on_disconnect(&self, _conn_id: u32) {}
}

/// The MySQL protocol server with connection-level concurrency control.
pub struct MysqlServer {
    config: ServerConfig,
    handler: Arc<dyn QueryHandler>,
    connection_counter: AtomicU32,
    connection_semaphore: Arc<Semaphore>,
}

impl MysqlServer {
    pub fn new(config: ServerConfig, handler: Arc<dyn QueryHandler>) -> Self {
        let max_connections = config.max_connections.max(1) as usize;
        Self {
            config,
            handler,
            connection_counter: AtomicU32::new(1),
            connection_semaphore: Arc::new(Semaphore::new(max_connections)),
        }
    }

    /// Start accepting connections. Runs until the server is shut down.
    /// Uses a semaphore to limit concurrent client connections.
    pub async fn run(&self) -> std::io::Result<()> {
        let addr = format!("{}:{}", self.config.bind_addr, self.config.port);
        let listener = TcpListener::bind(&addr).await?;
        info!(
            "MySQL server listening on {} (max_connections={})",
            addr, self.config.max_connections
        );

        let auth_timeout_secs = self.config.auth_timeout_secs;
        let semaphore = self.connection_semaphore.clone();
        let credentials = self.config.credentials.clone();

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let conn_id = self.connection_counter.fetch_add(1, Ordering::Relaxed);
            let handler = self.handler.clone();
            let creds = credentials.clone();

            // Try to acquire a connection semaphore permit
            match semaphore.clone().try_acquire_owned() {
                Ok(permit) => {
                    info!("Accepted connection {} from {}", conn_id, peer_addr);

                    tokio::spawn(
                        async move {
                            if let Err(e) = handle_connection(
                                stream,
                                conn_id,
                                handler,
                                auth_timeout_secs,
                                permit,
                                creds,
                            )
                            .await
                            {
                                error!("Connection {} error: {}", conn_id, e);
                            }
                            info!("Connection {} closed", conn_id);
                        }
                        .instrument(info_span!("mysql_conn", cid = conn_id)),
                    );
                }
                Err(_) => {
                    warn!(
                        "Connection {} from {} rejected: max connections ({}) reached",
                        conn_id, peer_addr, self.config.max_connections
                    );
                    // Drop the stream — client will see a connection reset
                    drop(stream);
                }
            }
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    conn_id: u32,
    handler: Arc<dyn QueryHandler>,
    auth_timeout_secs: u64,
    _permit: OwnedSemaphorePermit,
    credentials: Credentials,
) -> std::io::Result<()> {
    let peer_addr = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    // NOTE: Do NOT set TCP_NODELAY here. Our send_result_set already batches all
    // column packets into a single write_all and row packets into chunked writes
    // with a single flush. Nagle's algorithm helps coalesce these TCP segments
    // for bulk data transfer. TCP_NODELAY would force immediate send of each
    // write_all call, hurting throughput for large result sets.


    handler.on_connect(conn_id, "root", &peer_addr);
    let mut conn = Connection::new(
        stream,
        conn_id,
        handler.clone(),
        auth_timeout_secs,
        credentials,
    );
    let result = conn.run().await;
    handler.on_disconnect(conn_id);
    result
}

/// Convert a ColumnDef (from QueryResult) to a Column (for packet encoding).
impl From<&ColumnDef> for Column {
    fn from(def: &ColumnDef) -> Self {
        let col_type = match def.col_type {
            ColumnType::String => crate::packet::column_type::VAR_STRING,
            ColumnType::Int => crate::packet::column_type::LONGLONG,
            ColumnType::Float => crate::packet::column_type::FLOAT,
            ColumnType::Double => crate::packet::column_type::DOUBLE,
            ColumnType::Date => crate::packet::column_type::DATE,
            ColumnType::DateTime => crate::packet::column_type::DATETIME,
            ColumnType::Blob => crate::packet::column_type::BLOB,
        };
        Column::new(&def.name, col_type)
    }
}
