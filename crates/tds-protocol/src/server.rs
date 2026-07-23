use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tracing::error;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use mysql_protocol::server::QueryHandler;
use crate::connection;

#[derive(Debug, Clone)]
pub struct TdsServerConfig {
    pub bind_addr: String,
    pub port: u16,
    pub max_connections: u32,
}

impl Default for TdsServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0".to_string(),
            port: 5000,
            max_connections: 100,
        }
    }
}

pub struct TdsServer {
    config: TdsServerConfig,
    handler: Arc<dyn QueryHandler>,
    conn_counter: AtomicU32,
    semaphore: Arc<Semaphore>,
}

impl TdsServer {
    pub fn new(config: TdsServerConfig, handler: Arc<dyn QueryHandler>) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_connections as usize));
        Self {
            config,
            handler,
            conn_counter: AtomicU32::new(1),
            semaphore,
        }
    }

    pub async fn run(&self) -> std::io::Result<()> {
        let addr = format!("{}:{}", self.config.bind_addr, self.config.port);
        let listener = TcpListener::bind(&addr).await?;
        tracing::info!("TDS server listening on {}", addr);

        loop {
            let (stream, addr) = listener.accept().await?;
            let permit = match self.semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    tracing::warn!("TDS: max connections reached, rejecting {}", addr);
                    continue;
                }
            };

            let conn_id = self.conn_counter.fetch_add(1, Ordering::Relaxed);
            let handler = self.handler.clone();
            tokio::spawn(async move {
                // Panic-recovery boundary: a panic during TDS request handling
                // closes only this connection. `permit` is dropped after, so
                // the connection slot is always released.
                if let Err(payload) = common::panic_recovery::catch_unwind(
                    connection::run_connection(stream, handler, conn_id),
                )
                .await
                {
                    error!(
                        "TDS connection {} panicked: {}",
                        conn_id,
                        common::panic_recovery::payload_to_string(&payload)
                    );
                }
                drop(permit);
            });
        }
    }
}
