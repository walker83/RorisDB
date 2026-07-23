//! Cassandra native protocol server

use crate::frame::{Frame, Opcode};
use crate::handler::{CassandraCommandHandler, DefaultCassandraHandler};
use crate::storage::CassandraStorage;
use bytes::BytesMut;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

/// Cassandra server configuration
#[derive(Debug, Clone)]
pub struct CassandraServerConfig {
    pub port: u16,
}

impl Default for CassandraServerConfig {
    fn default() -> Self {
        Self { port: 9042 }
    }
}

/// Cassandra native protocol server
pub struct CassandraServer {
    config: CassandraServerConfig,
    storage: Arc<CassandraStorage>,
    handler: Arc<dyn CassandraCommandHandler>,
}

impl CassandraServer {
    /// Create a new Cassandra server
    pub fn new(config: CassandraServerConfig) -> Self {
        let storage = Arc::new(CassandraStorage::new());
        let handler = Arc::new(DefaultCassandraHandler::new(storage.clone()));
        Self {
            config,
            storage,
            handler,
        }
    }

    /// Start the Cassandra server
    pub async fn start(&self) -> anyhow::Result<()> {
        let addr: SocketAddr = format!("0.0.0.0:{}", self.config.port).parse()?;
        let listener = TcpListener::bind(addr).await?;
        info!("Cassandra native protocol server listening on {}", addr);

        let handler = self.handler.clone();

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("Cassandra client connected from {}", addr);

                    let handler = handler.clone();
                    tokio::spawn(async move {
                        // Panic-recovery boundary: a panic during Cassandra
                        // request handling closes only this connection.
                        match common::panic_recovery::catch_unwind(handle_connection(
                            stream, handler,
                        ))
                        .await
                        {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => error!("Cassandra connection error: {}", e),
                            Err(payload) => error!(
                                "Cassandra connection panicked: {}",
                                common::panic_recovery::payload_to_string(&payload)
                            ),
                        }
                    });
                }
                Err(e) => {
                    error!("Cassandra accept error: {}", e);
                }
            }
        }
    }

    /// Get storage reference
    pub fn storage(&self) -> &Arc<CassandraStorage> {
        &self.storage
    }
}

/// Handle a single Cassandra connection
async fn handle_connection(
    mut stream: TcpStream,
    handler: Arc<dyn CassandraCommandHandler>,
) -> anyhow::Result<()> {
    let mut read_buf = BytesMut::with_capacity(8192);
    let mut keyspace = "system".to_string();

    loop {
        let n = stream.read_buf(&mut read_buf).await?;
        if n == 0 {
            info!("Cassandra client disconnected");
            return Ok(());
        }

        // Parse frames
        while let Some(frame) = Frame::parse(&mut read_buf) {
            let opcode = Opcode::from_u8(frame.header.opcode);

            match opcode {
                Some(Opcode::Startup) => {
                    info!("Received STARTUP");
                    let response = handler.handle_startup();
                    stream.write_all(&response).await?;
                    stream.flush().await?;
                }
                Some(Opcode::Options) => {
                    info!("Received OPTIONS");
                    // Return SUPPORTED with CQL versions and compression options
                    let supported_body = build_supported_body();
                    let frame = Frame::new(0x84, frame.header.stream, Opcode::Supported, supported_body);
                    let mut buf = bytes::BytesMut::new();
                    frame.encode(&mut buf);
                    stream.write_all(&buf).await?;
                    stream.flush().await?;
                }

                Some(Opcode::Query) => {
                    // Parse CQL query: body is [long string] query + [short] consistency + [byte] flags
                    let cql = if frame.body.len() >= 4 {
                        let cql_len = ((frame.body[0] as usize) << 24)
                            | ((frame.body[1] as usize) << 16)
                            | ((frame.body[2] as usize) << 8)
                            | (frame.body[3] as usize);
                        let end = (4 + cql_len).min(frame.body.len());
                        String::from_utf8_lossy(&frame.body[4..end]).to_string()
                    } else {
                        String::from_utf8_lossy(&frame.body).to_string()
                    };
                    info!("Received CQL: {}", cql);

                    // Execute query
                    let response = handler.handle_query(&mut keyspace, &cql);
                    stream.write_all(&response).await?;
                    stream.flush().await?;
                }

                _ => {
                    info!("Received opcode: {:?}", opcode);
                }
            }
        }
    }
}

/// Build SUPPORTED response body with CQL version and compression options
fn build_supported_body() -> Vec<u8> {
    let mut body = Vec::new();
    // Number of options (2: CQL_VERSION and COMPRESSION)
    body.extend_from_slice(&2u32.to_be_bytes());
    // CQL_VERSION
    let cql_ver_key = b"CQL_VERSION";
    body.extend_from_slice(&(cql_ver_key.len() as u16).to_be_bytes());
    body.extend_from_slice(cql_ver_key);
    // List of supported versions (1 entry)
    body.extend_from_slice(&1u32.to_be_bytes());
    let ver = b"3.4.5";
    body.extend_from_slice(&(ver.len() as u16).to_be_bytes());
    body.extend_from_slice(ver);
    // COMPRESSION
    let comp_key = b"COMPRESSION";
    body.extend_from_slice(&(comp_key.len() as u16).to_be_bytes());
    body.extend_from_slice(comp_key);
    // List of supported compressions (1 entry: none)
    body.extend_from_slice(&1u32.to_be_bytes());
    let comp = b"none";
    body.extend_from_slice(&(comp.len() as u16).to_be_bytes());
    body.extend_from_slice(comp);
    body
}

/// Start Cassandra server with default configuration
pub async fn start_cassandra_server() -> anyhow::Result<()> {
    let config = CassandraServerConfig::default();
    let server = CassandraServer::new(config);
    server.start().await
}

/// Start Cassandra server with custom configuration
pub async fn start_cassandra_server_with_config(config: CassandraServerConfig) -> anyhow::Result<()> {
    let server = CassandraServer::new(config);
    server.start().await
}
