//! Elasticsearch HTTP server

use crate::handler::{DefaultElasticsearchHandler, ElasticsearchCommandHandler};
use crate::storage::ElasticsearchStorage;
use bytes::Bytes;
use http_body_util::Full;
use hyper::{body::Incoming, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};

/// Elasticsearch server configuration
#[derive(Debug, Clone)]
pub struct ElasticsearchServerConfig {
    pub port: u16,
}

impl Default for ElasticsearchServerConfig {
    fn default() -> Self {
        Self { port: 9200 }
    }
}

/// Elasticsearch HTTP server
pub struct ElasticsearchServer {
    config: ElasticsearchServerConfig,
    storage: Arc<ElasticsearchStorage>,
    handler: Arc<dyn ElasticsearchCommandHandler>,
}

impl ElasticsearchServer {
    /// Create a new Elasticsearch server
    pub fn new(config: ElasticsearchServerConfig) -> Self {
        let storage = Arc::new(ElasticsearchStorage::new());
        let handler = Arc::new(DefaultElasticsearchHandler::new(storage.clone()));
        Self {
            config,
            storage,
            handler,
        }
    }

    /// Start the Elasticsearch HTTP server
    pub async fn start(&self) -> anyhow::Result<()> {
        let addr: SocketAddr = format!("0.0.0.0:{}", self.config.port).parse()?;
        let listener = TcpListener::bind(addr).await?;
        info!("Elasticsearch HTTP server listening on http://{}", addr);

        let handler = self.handler.clone();

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("Elasticsearch client connected from {}", addr);

                    let handler = handler.clone();
                    tokio::spawn(async move {
                        let io = TokioIo::new(stream);

                        let service = hyper::service::service_fn(move |req: Request<Incoming>| {
                            let handler = handler.clone();
                            async move {
                                handle_request(req, handler).await
                            }
                        });

                        if let Err(err) = hyper::server::conn::http1::Builder::new()
                            .serve_connection(io, service)
                            .await
                        {
                            error!("Error serving connection: {:?}", err);
                        }
                    });
                }
                Err(e) => {
                    error!("Elasticsearch accept error: {}", e);
                }
            }
        }
    }

    /// Get storage reference
    pub fn storage(&self) -> &Arc<ElasticsearchStorage> {
        &self.storage
    }
}

/// Handle HTTP request
async fn handle_request(
    req: Request<Incoming>,
    handler: Arc<dyn ElasticsearchCommandHandler>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let (parts, body) = req.into_parts();

    let method = parts.method.as_str();
    let path = parts.uri.path();

    // Read body if present
    let body_str = {
        use http_body_util::BodyExt;
        let body_bytes = body.collect().await?.to_bytes();
        if body_bytes.is_empty() {
            None
        } else {
            Some(String::from_utf8_lossy(&body_bytes).to_string())
        }
    };

    info!("Elasticsearch {} {}", method, path);

    // Handle request
    let result = handler.handle_request(method, path, body_str.as_deref());

    let response_body = serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string());

    // Extract HTTP status from result JSON if available
    let status_code = result
        .get("status")
        .and_then(|v| v.as_u64())
        .and_then(|c| StatusCode::from_u16(c as u16).ok())
        .unwrap_or(StatusCode::OK);

    let response = Response::builder()
        .status(status_code)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(response_body)))
        .unwrap();

    Ok(response)
}

/// Start Elasticsearch server with default configuration
pub async fn start_elasticsearch_server() -> anyhow::Result<()> {
    let config = ElasticsearchServerConfig::default();
    let server = ElasticsearchServer::new(config);
    server.start().await
}

/// Start Elasticsearch server with custom configuration
pub async fn start_elasticsearch_server_with_config(config: ElasticsearchServerConfig) -> anyhow::Result<()> {
    let server = ElasticsearchServer::new(config);
    server.start().await
}
