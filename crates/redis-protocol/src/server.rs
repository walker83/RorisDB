//! Redis TCP server

use crate::connection::RedisConnection;
use crate::handler::{DefaultRedisHandler, RedisCommandHandler};
use crate::storage::RedisStorage;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};

/// Redis server configuration
#[derive(Debug, Clone)]
pub struct RedisServerConfig {
    pub port: u16,
    pub password: Option<String>,
    pub num_databases: usize,
}

impl Default for RedisServerConfig {
    fn default() -> Self {
        Self {
            port: 6379,
            password: None,
            num_databases: 16,
        }
    }
}

/// Redis server instance
pub struct RedisServer {
    config: RedisServerConfig,
    storage: Arc<RedisStorage>,
    handler: Arc<dyn RedisCommandHandler>,
}

impl RedisServer {
    /// Create a new Redis server
    pub fn new(config: RedisServerConfig) -> Self {
        let storage = Arc::new(RedisStorage::new(config.num_databases));
        let handler = Arc::new(DefaultRedisHandler::new(
            storage.clone(),
            config.password.clone(),
        ));
        Self {
            config,
            storage,
            handler,
        }
    }

    /// Start the Redis server
    pub async fn start(&self) -> anyhow::Result<()> {
        let addr = format!("0.0.0.0:{}", self.config.port);
        let listener = TcpListener::bind(&addr).await?;
        info!("Redis server listening on {}", addr);

        let mut conn_id = 0u32;

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("Redis client connected from {}", addr);
                    conn_id += 1;

                    let handler = self.handler.clone();
                    tokio::spawn(async move {
                        // Panic-recovery boundary: a panic during Redis request
                        // handling closes only this connection.
                        let outcome = common::panic_recovery::catch_unwind(async {
                            let conn = RedisConnection::new(stream, handler, conn_id);
                            conn.run().await
                        })
                        .await;
                        match outcome {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => error!("Redis connection error: {}", e),
                            Err(payload) => error!(
                                "Redis connection {} panicked: {}",
                                conn_id,
                                common::panic_recovery::payload_to_string(&payload)
                            ),
                        }
                    });
                }
                Err(e) => {
                    error!("Redis accept error: {}", e);
                }
            }
        }
    }

    /// Get storage reference
    pub fn storage(&self) -> &Arc<RedisStorage> {
        &self.storage
    }
}

/// Start Redis server with default configuration
pub async fn start_redis_server() -> anyhow::Result<()> {
    let config = RedisServerConfig::default();
    let server = RedisServer::new(config);
    server.start().await
}

/// Start Redis server with custom configuration
pub async fn start_redis_server_with_config(config: RedisServerConfig) -> anyhow::Result<()> {
    let server = RedisServer::new(config);
    server.start().await
}
