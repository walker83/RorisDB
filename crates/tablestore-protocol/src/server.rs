//! TableStore HTTP server

use crate::handler::{DefaultTableStoreHandler, TableStoreCommandHandler};
use crate::storage::TableStoreStorage;
use bytes::Bytes;
use http_body_util::Full;
use hyper::{body::Incoming, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};

/// TableStore server configuration
#[derive(Debug, Clone)]
pub struct TableStoreServerConfig {
    pub port: u16,
}

impl Default for TableStoreServerConfig {
    fn default() -> Self {
        Self { port: 8087 }
    }
}

/// TableStore HTTP server
pub struct TableStoreServer {
    config: TableStoreServerConfig,
    storage: Arc<TableStoreStorage>,
    handler: Arc<dyn TableStoreCommandHandler>,
}

impl TableStoreServer {
    /// Create a new TableStore server
    pub fn new(config: TableStoreServerConfig) -> Self {
        let storage = Arc::new(TableStoreStorage::new());
        let handler = Arc::new(DefaultTableStoreHandler::new(storage.clone()));
        Self {
            config,
            storage,
            handler,
        }
    }

    /// Start the TableStore HTTP server
    pub async fn start(&self) -> anyhow::Result<()> {
        let addr: SocketAddr = format!("0.0.0.0:{}", self.config.port).parse()?;
        let listener = TcpListener::bind(addr).await?;
        info!("TableStore HTTP server listening on http://{}", addr);

        let handler = self.handler.clone();

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("TableStore client connected from {}", addr);

                    let handler = handler.clone();
                    tokio::spawn(async move {
                        // Panic-recovery boundary: a panic in a request handler
                        // closes only this connection.
                        let outcome = common::panic_recovery::catch_unwind(async {
                            let io = TokioIo::new(stream);

                            let service = hyper::service::service_fn(
                                move |req: Request<Incoming>| {
                                    let handler = handler.clone();
                                    async move { handle_request(req, handler).await }
                                },
                            );

                            hyper::server::conn::http1::Builder::new()
                                .serve_connection(io, service)
                                .await
                        })
                        .await;
                        if let Err(payload) = outcome {
                            error!(
                                "TableStore connection panicked: {}",
                                common::panic_recovery::payload_to_string(&payload)
                            );
                        }
                    });
                }
                Err(e) => {
                    error!("TableStore accept error: {}", e);
                }
            }
        }
    }

    /// Get storage reference
    pub fn storage(&self) -> &Arc<TableStoreStorage> {
        &self.storage
    }
}

/// Handle HTTP request
async fn handle_request(
    req: Request<Incoming>,
    handler: Arc<dyn TableStoreCommandHandler>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let (parts, body) = req.into_parts();

    let method = parts.method.as_str();
    let path = parts.uri.path();
    let query = parts.uri.query().unwrap_or("");

    // Read body
    let body_str = {
        use http_body_util::BodyExt;
        let body_bytes = body.collect().await?.to_bytes();
        if body_bytes.is_empty() {
            None
        } else {
            Some(String::from_utf8_lossy(&body_bytes).to_string())
        }
    };

    info!("TableStore {} {} query={}", method, path, query);

    // Handle request
    let (status_code, response_body) = handler.handle_request(method, path, query, body_str.as_deref());

    let status = StatusCode::from_u16(status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

    let response = Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(response_body)))
        .unwrap();

    Ok(response)
}

/// Start TableStore server with default configuration
pub async fn start_tablestore_server() -> anyhow::Result<()> {
    let config = TableStoreServerConfig::default();
    let server = TableStoreServer::new(config);
    server.start().await
}

/// Start TableStore server with custom configuration
pub async fn start_tablestore_server_with_config(config: TableStoreServerConfig) -> anyhow::Result<()> {
    let server = TableStoreServer::new(config);
    server.start().await
}
