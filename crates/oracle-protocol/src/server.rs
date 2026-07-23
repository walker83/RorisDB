//! Oracle TNS server

use crate::handler::{DefaultOracleHandler, OracleCommandHandler};
use crate::storage::OracleStorage;
use crate::tns::{ConnectData, TnsPacket, TnsPacketType};
use bytes::BytesMut;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

/// Oracle server configuration
#[derive(Debug, Clone)]
pub struct OracleServerConfig {
    pub port: u16,
}

impl Default for OracleServerConfig {
    fn default() -> Self {
        Self { port: 1521 }
    }
}

/// Oracle TNS server
pub struct OracleServer {
    config: OracleServerConfig,
    storage: Arc<OracleStorage>,
    handler: Arc<dyn OracleCommandHandler>,
}

impl OracleServer {
    /// Create a new Oracle server
    pub fn new(config: OracleServerConfig) -> Self {
        let storage = Arc::new(OracleStorage::new());
        let handler = Arc::new(DefaultOracleHandler::new(storage.clone()));
        Self {
            config,
            storage,
            handler,
        }
    }

    /// Start the Oracle TNS server
    pub async fn start(&self) -> anyhow::Result<()> {
        let addr: SocketAddr = format!("0.0.0.0:{}", self.config.port).parse()?;
        let listener = TcpListener::bind(addr).await?;
        info!("Oracle TNS server listening on {}", addr);

        let handler = self.handler.clone();

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("Oracle client connected from {}", addr);

                    let handler = handler.clone();
                    tokio::spawn(async move {
                        // Panic-recovery boundary: a panic during Oracle
                        // request handling closes only this connection.
                        match common::panic_recovery::catch_unwind(handle_connection(
                            stream, handler,
                        ))
                        .await
                        {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => error!("Oracle connection error: {}", e),
                            Err(payload) => error!(
                                "Oracle connection panicked: {}",
                                common::panic_recovery::payload_to_string(&payload)
                            ),
                        }
                    });
                }
                Err(e) => {
                    error!("Oracle accept error: {}", e);
                }
            }
        }
    }

    /// Get storage reference
    pub fn storage(&self) -> &Arc<OracleStorage> {
        &self.storage
    }
}

/// Handle a single Oracle connection
async fn handle_connection(
    mut stream: TcpStream,
    handler: Arc<dyn OracleCommandHandler>,
) -> anyhow::Result<()> {
    let mut read_buf = BytesMut::with_capacity(8192);
    let mut schema = "SYSTEM".to_string();

    loop {
        let n = stream.read_buf(&mut read_buf).await?;
        if n == 0 {
            info!("Oracle client disconnected");
            return Ok(());
        }

        // Parse TNS packet
        while let Some(packet) = TnsPacket::parse(&mut read_buf) {
            match packet.header.packet_type {
                TnsPacketType::Connect => {
                    info!("Received CONNECT packet");

                    // Parse connect data
                    if let Some(connect_data) = ConnectData::parse(&packet.data) {
                        info!("Connecting to service: {}", connect_data.service);
                        schema = connect_data.service.to_uppercase();
                    }

                    // Send ACCEPT
                    let accept = handler.handle_connect(&schema);
                    stream.write_all(&accept).await?;
                }

                TnsPacketType::Data => {
                    // Parse SQL query (simplified)
                    let sql = String::from_utf8_lossy(&packet.data).to_string();
                    info!("Received SQL: {}", sql);

                    // Execute query
                    let response = handler.handle_query(&schema, &sql);
                    stream.write_all(&response).await?;
                }

                _ => {
                    info!("Received packet type: {:?}", packet.header.packet_type);
                }
            }
        }
    }
}

/// Start Oracle server with default configuration
pub async fn start_oracle_server() -> anyhow::Result<()> {
    let config = OracleServerConfig::default();
    let server = OracleServer::new(config);
    server.start().await
}

/// Start Oracle server with custom configuration
pub async fn start_oracle_server_with_config(config: OracleServerConfig) -> anyhow::Result<()> {
    let server = OracleServer::new(config);
    server.start().await
}
