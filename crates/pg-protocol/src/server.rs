//! PostgreSQL wire protocol v3 TCP server.
//!
//! This module implements the TCP server that accepts PostgreSQL client
//! connections and hands them off to the connection state machine.
//!
//! # Usage
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use pg_protocol::{PgServer, PgServerConfig};
//! use mysql_protocol::QueryHandler;
//!
//! async fn start(handler: Arc<dyn QueryHandler>) -> anyhow::Result<()> {
//!     let config = PgServerConfig {
//!         port: 5432,
//!         username: "admin".to_string(),
//!         password: "secret".to_string(),
//!         ..Default::default()
//!     };
//!     let server = PgServer::new(config, handler);
//!     server.run().await?;
//!     Ok(())
//! }
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{Instrument, error, info, info_span, warn};

use crate::auth::AuthConfig;
use crate::connection::run_connection;
use mysql_protocol::server::QueryHandler;

/// Configuration for the PostgreSQL protocol server.
#[derive(Debug, Clone)]
pub struct PgServerConfig {
    /// Bind address for the server. Default: "127.0.0.1"
    pub bind_addr: String,
    /// Port for the PostgreSQL server. Default: 5432
    pub port: u16,
    /// Maximum concurrent connections. Default: 100
    pub max_connections: u32,
    /// Expected username (AccessKey ID for Hologres). Default: "admin"
    pub username: String,
    /// Expected password (AccessKey Secret for Hologres). Default: "admin"
    pub password: String,
    /// If true, accept any username/password. Default: false (for dev only)
    pub accept_any_password: bool,
}

impl Default for PgServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1".to_string(),
            port: 5432,
            max_connections: 100,
            username: "admin".to_string(),
            password: "admin".to_string(),
            accept_any_password: false,
        }
    }
}

/// The PostgreSQL wire protocol v3 server.
///
/// Accepts TCP connections, authenticates clients, and dispatches queries
/// to the shared `QueryHandler`. Each connection runs in its own Tokio task.
pub struct PgServer {
    config: PgServerConfig,
    handler: Arc<dyn QueryHandler>,
    connection_counter: AtomicU32,
    connection_semaphore: Arc<Semaphore>,
}

impl PgServer {
    /// Create a new PostgreSQL server with the given config and query handler.
    pub fn new(config: PgServerConfig, handler: Arc<dyn QueryHandler>) -> Self {
        let max_connections = config.max_connections.max(1) as usize;
        Self {
            config,
            handler,
            connection_counter: AtomicU32::new(1),
            connection_semaphore: Arc::new(Semaphore::new(max_connections)),
        }
    }

    /// Start the server and begin accepting connections.
    ///
    /// This function runs indefinitely until the server is shut down or
    /// encounters a fatal error.
    pub async fn run(&self) -> anyhow::Result<()> {
        let addr = format!("{}:{}", self.config.bind_addr, self.config.port);
        let listener = TcpListener::bind(&addr).await?;
        info!(
            "PostgreSQL server listening on {} (max_connections={})",
            addr, self.config.max_connections
        );

        let auth_config = AuthConfig {
            username: self.config.username.clone(),
            password: self.config.password.clone(),
            accept_any_password: self.config.accept_any_password,
        };

        let semaphore = self.connection_semaphore.clone();

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let conn_id = self.connection_counter.fetch_add(1, Ordering::Relaxed);
            let handler = self.handler.clone();
            let auth_config = auth_config.clone();

            match semaphore.clone().try_acquire_owned() {
                Ok(permit) => {
                    info!("PG connection {} accepted from {}", conn_id, peer_addr);
                    tokio::spawn(
                        async move {
                            // Panic-recovery boundary: a panic during PG
                            // request handling closes only this connection.
                            if let Err(payload) =
                                common::panic_recovery::catch_unwind(handle_pg_connection(
                                    stream,
                                    conn_id,
                                    handler,
                                    auth_config,
                                    permit,
                                ))
                                .await
                            {
                                error!(
                                    "PG connection {} panicked: {}",
                                    conn_id,
                                    common::panic_recovery::payload_to_string(&payload)
                                );
                            }
                        }
                        .instrument(info_span!("pg_conn", cid = conn_id)),
                    );
                }
                Err(_) => {
                    warn!(
                        "PG connection {} from {} rejected: max connections ({}) reached",
                        conn_id, peer_addr, self.config.max_connections
                    );
                    drop(stream);
                }
            }
        }
    }
}

/// Handle a single PG connection, including cleanup.
async fn handle_pg_connection(
    stream: TcpStream,
    conn_id: u32,
    handler: Arc<dyn QueryHandler>,
    auth_config: AuthConfig,
    _permit: OwnedSemaphorePermit,
) {
    let peer_addr = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    info!("PG connection {}: starting from {}", conn_id, peer_addr);

    if let Err(e) = run_connection(stream, conn_id, handler, auth_config).await {
        // CancelRequest is not an error — client intentionally disconnected
        if matches!(&e, crate::message::PgProtocolError::CancelRequest) {
            info!("PG connection {}: cancelled by client", conn_id);
        } else {
            error!("PG connection {} error: {}", conn_id, e);
        }
    }

    info!("PG connection {} closed", conn_id);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_server_config_default() {
        let config = PgServerConfig::default();
        assert_eq!(config.bind_addr, "127.0.0.1");
        assert_eq!(config.port, 5432);
        assert_eq!(config.max_connections, 100);
        assert_eq!(config.username, "admin");
        assert_eq!(config.password, "admin");
        assert!(!config.accept_any_password);
    }

    #[test]
    fn test_pg_server_config_custom() {
        let config = PgServerConfig {
            bind_addr: "0.0.0.0".to_string(),
            port: 15432,
            max_connections: 50,
            username: "myuser".to_string(),
            password: "mypass".to_string(),
            accept_any_password: false,
        };
        assert_eq!(config.bind_addr, "0.0.0.0");
        assert_eq!(config.port, 15432);
        assert_eq!(config.max_connections, 50);
    }

    #[test]
    fn test_pg_server_new() {
        use mysql_protocol::server::QueryResult;
        struct DummyHandler;
        impl QueryHandler for DummyHandler {
            fn handle_query(&self, _conn_id: u32, _sql: &str) -> QueryResult {
                QueryResult::ok()
            }
        }

        let config = PgServerConfig::default();
        let handler = Arc::new(DummyHandler);
        let _server = PgServer::new(config, handler);

        // Verify the server was created successfully by checking it's Send + Sync
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<PgServer>();
        assert_sync::<PgServer>();
    }
}
