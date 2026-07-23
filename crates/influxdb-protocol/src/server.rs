//! InfluxDB HTTP server

use crate::handler::{DefaultInfluxDBHandler, InfluxDBCommandHandler};
use crate::storage::InfluxDBStorage;
use bytes::Bytes;
use http_body_util::Full;
use hyper::{body::Incoming, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};

/// InfluxDB server configuration
#[derive(Debug, Clone)]
pub struct InfluxDBServerConfig {
    pub port: u16,
}

impl Default for InfluxDBServerConfig {
    fn default() -> Self {
        Self { port: 8086 }
    }
}

/// InfluxDB HTTP server
pub struct InfluxDBServer {
    config: InfluxDBServerConfig,
    storage: Arc<InfluxDBStorage>,
    handler: Arc<dyn InfluxDBCommandHandler>,
}

impl InfluxDBServer {
    /// Create a new InfluxDB server
    pub fn new(config: InfluxDBServerConfig) -> Self {
        let storage = Arc::new(InfluxDBStorage::new());
        let handler = Arc::new(DefaultInfluxDBHandler::new(storage.clone()));
        Self {
            config,
            storage,
            handler,
        }
    }

    /// Start the InfluxDB HTTP server
    pub async fn start(&self) -> anyhow::Result<()> {
        let addr: SocketAddr = format!("0.0.0.0:{}", self.config.port).parse()?;
        let listener = TcpListener::bind(addr).await?;
        info!("InfluxDB HTTP server listening on http://{}", addr);

        let handler = self.handler.clone();

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("InfluxDB client connected from {}", addr);

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
                                "InfluxDB connection panicked: {}",
                                common::panic_recovery::payload_to_string(&payload)
                            );
                        }
                    });
                }
                Err(e) => {
                    error!("InfluxDB accept error: {}", e);
                }
            }
        }
    }

    /// Get storage reference
    pub fn storage(&self) -> &Arc<InfluxDBStorage> {
        &self.storage
    }
}

/// Handle HTTP request
async fn handle_request(
    req: Request<Incoming>,
    handler: Arc<dyn InfluxDBCommandHandler>,
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

    let path = parts.uri.path();
    let method = parts.method.as_str();

    // Get database
    let database = query_params
        .get("db")
        .cloned()
        .unwrap_or_else(|| "default".to_string());

    // Read body
    let body_str = {
        use http_body_util::BodyExt;
        let body_bytes = body.collect().await?.to_bytes();
        String::from_utf8_lossy(&body_bytes).to_string()
    };

    info!("InfluxDB {} {}", method, path);

    let (status, response_body) = match (method, path) {
        // POST /write - Write data
        ("POST", "/write") => {
            match handler.handle_write(&database, &body_str) {
                Ok(_) => (StatusCode::NO_CONTENT, String::new()),
                Err(e) => (StatusCode::BAD_REQUEST, e),
            }
        }

        // GET /query - Query data
        ("GET", "/query") => {
            if let Some(q) = query_params.get("q") {
                let result = handler.handle_query(&database, q);
                (StatusCode::OK, result)
            } else {
                (StatusCode::BAD_REQUEST, "Missing query parameter 'q'".to_string())
            }
        }

        // POST /query - Query data (alternative)
        ("POST", "/query") => {
            if let Some(q) = query_params.get("q") {
                let result = handler.handle_query(&database, q);
                (StatusCode::OK, result)
            } else if !body_str.is_empty() {
                let result = handler.handle_query(&database, &body_str);
                (StatusCode::OK, result)
            } else {
                (StatusCode::BAD_REQUEST, "Missing query parameter 'q'".to_string())
            }
        }

        // GET /ping - Health check
        ("GET", "/ping") | ("HEAD", "/ping") => {
            (StatusCode::NO_CONTENT, String::new())
        }

        _ => {
            (StatusCode::NOT_FOUND, "Not found".to_string())
        }
    };

    let response = Response::builder()
        .status(status)
        .header("Content-Type", "text/plain")
        .body(Full::new(Bytes::from(response_body)))
        .unwrap();

    Ok(response)
}

/// Start InfluxDB server with default configuration
pub async fn start_influxdb_server() -> anyhow::Result<()> {
    let config = InfluxDBServerConfig::default();
    let server = InfluxDBServer::new(config);
    server.start().await
}

/// Start InfluxDB server with custom configuration
pub async fn start_influxdb_server_with_config(config: InfluxDBServerConfig) -> anyhow::Result<()> {
    let server = InfluxDBServer::new(config);
    server.start().await
}
