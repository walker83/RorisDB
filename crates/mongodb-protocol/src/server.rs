//! MongoDB TCP server

use crate::connection::MongoDBConnection;
use crate::handler::{DefaultMongoDBHandler, MongoDBCommandHandler};
use crate::storage::MongoDBStorage;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};

/// MongoDB server configuration
#[derive(Debug, Clone)]
pub struct MongoDBServerConfig {
    pub port: u16,
}

impl Default for MongoDBServerConfig {
    fn default() -> Self {
        Self { port: 27017 }
    }
}

/// MongoDB server instance
pub struct MongoDBServer {
    config: MongoDBServerConfig,
    storage: Arc<MongoDBStorage>,
    handler: Arc<dyn MongoDBCommandHandler>,
}

impl MongoDBServer {
    /// Create a new MongoDB server
    pub fn new(config: MongoDBServerConfig) -> Self {
        let storage = Arc::new(MongoDBStorage::new());
        let handler = Arc::new(DefaultMongoDBHandler::new(storage.clone()));
        Self {
            config,
            storage,
            handler,
        }
    }

    /// Start the MongoDB server
    pub async fn start(&self) -> anyhow::Result<()> {
        let addr = format!("0.0.0.0:{}", self.config.port);
        let listener = TcpListener::bind(&addr).await?;
        info!("MongoDB server listening on {}", addr);

        let mut conn_id = 0u32;

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("MongoDB client connected from {}", addr);
                    conn_id += 1;

                    let handler = self.handler.clone();
                    tokio::spawn(async move {
                        // Panic-recovery boundary: a panic during MongoDB
                        // request handling closes only this connection.
                        let outcome = common::panic_recovery::catch_unwind(async {
                            let conn = MongoDBConnection::new(stream, handler, conn_id);
                            conn.run().await
                        })
                        .await;
                        match outcome {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => error!("MongoDB connection error: {}", e),
                            Err(payload) => error!(
                                "MongoDB connection {} panicked: {}",
                                conn_id,
                                common::panic_recovery::payload_to_string(&payload)
                            ),
                        }
                    });
                }
                Err(e) => {
                    error!("MongoDB accept error: {}", e);
                }
            }
        }
    }

    /// Get storage reference
    pub fn storage(&self) -> &Arc<MongoDBStorage> {
        &self.storage
    }
}

/// Start MongoDB server with default configuration
pub async fn start_mongodb_server() -> anyhow::Result<()> {
    let config = MongoDBServerConfig::default();
    let server = MongoDBServer::new(config);
    server.start().await
}

/// Start MongoDB server with custom configuration
pub async fn start_mongodb_server_with_config(config: MongoDBServerConfig) -> anyhow::Result<()> {
    let server = MongoDBServer::new(config);
    server.start().await
}
