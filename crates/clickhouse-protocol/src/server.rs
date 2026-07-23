//! ClickHouse HTTP server

use crate::handler::{ClickHouseCommandHandler, DefaultClickHouseHandler};
use crate::storage::ClickHouseStorage;
use bytes::Bytes;
use http_body_util::Full;
use hyper::{body::Incoming, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};

/// ClickHouse server configuration
#[derive(Debug, Clone)]
pub struct ClickHouseServerConfig {
    pub port: u16,
}

impl Default for ClickHouseServerConfig {
    fn default() -> Self {
        Self { port: 8123 }
    }
}

/// ClickHouse HTTP server
pub struct ClickHouseServer {
    config: ClickHouseServerConfig,
    storage: Arc<ClickHouseStorage>,
    handler: Arc<dyn ClickHouseCommandHandler>,
}

impl ClickHouseServer {
    /// Create a new ClickHouse server
    pub fn new(config: ClickHouseServerConfig) -> Self {
        let storage = Arc::new(ClickHouseStorage::new());
        let handler = Arc::new(DefaultClickHouseHandler::new(storage.clone()));
        Self {
            config,
            storage,
            handler,
        }
    }

    /// Start the ClickHouse HTTP server
    pub async fn start(&self) -> anyhow::Result<()> {
        let addr: SocketAddr = format!("0.0.0.0:{}", self.config.port).parse()?;
        let listener = TcpListener::bind(addr).await?;
        info!("ClickHouse HTTP server listening on http://{}", addr);

        let handler = self.handler.clone();

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("ClickHouse client connected from {}", addr);

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
                                "ClickHouse connection panicked: {}",
                                common::panic_recovery::payload_to_string(&payload)
                            );
                        }
                    });
                }
                Err(e) => {
                    error!("ClickHouse accept error: {}", e);
                }
            }
        }
    }

    /// Get storage reference
    pub fn storage(&self) -> &Arc<ClickHouseStorage> {
        &self.storage
    }
}

/// Handle HTTP request
async fn handle_request(
    req: Request<Incoming>,
    handler: Arc<dyn ClickHouseCommandHandler>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let (parts, body) = req.into_parts();

    // Extract query parameters
    let query_params: HashMap<String, String> = parts
        .uri
        .query()
        .map(|q| {
            url::form_urlencoded::parse(q.as_bytes())
                .into_owned()
                .collect()
        })
        .unwrap_or_default();

    // Get database (default to "default")
    let database = query_params
        .get("database")
        .cloned()
        .unwrap_or_else(|| "default".to_string());

    // Get query from query param or body
    let query = if let Some(q) = query_params.get("query") {
        q.clone()
    } else {
        // Read body
        use http_body_util::BodyExt;
        let body_bytes = body.collect().await?.to_bytes();
        String::from_utf8_lossy(&body_bytes).to_string()
    };

    if query.is_empty() {
        // Health check endpoint
        return Ok(Response::new(Full::new(Bytes::from("Ok.\n"))));
    }

    info!("ClickHouse query: {}", query);

    // Execute query
    let result = handler.handle_query(&database, &query);

    let status = if result.starts_with("Error:") {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::OK
    };

    let response = Response::builder()
        .status(status)
        .header("Content-Type", "text/tab-separated-values; charset=UTF-8")
        .body(Full::new(Bytes::from(result)))
        .unwrap();

    Ok(response)
}

/// Start ClickHouse server with default configuration
pub async fn start_clickhouse_server() -> anyhow::Result<()> {
    let config = ClickHouseServerConfig::default();
    let server = ClickHouseServer::new(config);
    server.start().await
}

/// Start ClickHouse server with custom configuration
pub async fn start_clickhouse_server_with_config(config: ClickHouseServerConfig) -> anyhow::Result<()> {
    let server = ClickHouseServer::new(config);
    server.start().await
}
