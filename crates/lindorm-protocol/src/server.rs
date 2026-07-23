//! Lindorm TCP server (HBase-like protocol)

use crate::handler::LindormHandler;
use crate::storage::LindormStorage;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, error};

pub struct LindormServer {
    port: u16,
    storage: Arc<LindormStorage>,
}

impl LindormServer {
    pub fn new(port: u16) -> Self {
        Self {
            port,
            storage: Arc::new(LindormStorage::new()),
        }
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        let addr = format!("0.0.0.0:{}", self.port);
        let listener = TcpListener::bind(&addr).await?;
        info!("Lindorm server listening on {}", addr);

        loop {
            let (stream, addr) = listener.accept().await?;
            info!("Lindorm client connected: {}", addr);

            let storage = self.storage.clone();
            tokio::spawn(async move {
                // Panic-recovery boundary: a panic during Lindorm request
                // handling closes only this connection.
                match common::panic_recovery::catch_unwind(Self::handle_connection(
                    stream, storage,
                ))
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => error!("Lindorm connection error: {}", e),
                    Err(payload) => error!(
                        "Lindorm connection panicked: {}",
                        common::panic_recovery::payload_to_string(&payload)
                    ),
                }
            });
        }
    }

    async fn handle_connection(stream: TcpStream, storage: Arc<LindormStorage>) -> anyhow::Result<()> {
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let handler = LindormHandler::new(storage);

        writer.write_all(b"Lindorm Shell v1.0\nType 'help' for commands\n\n").await?;
        writer.flush().await?;

        let mut line = String::new();
        loop {
            writer.write_all(b"> ").await?;
            writer.flush().await?;

            line.clear();
            let bytes_read = reader.read_line(&mut line).await?;
            if bytes_read == 0 {
                break;
            }

            let command = line.trim();
            if command.is_empty() {
                continue;
            }

            if command.to_lowercase() == "quit" || command.to_lowercase() == "exit" {
                break;
            }

            let response = handler.handle_command(command);
            writer.write_all(response.as_bytes()).await?;
            writer.write_all(b"\n").await?;
            writer.flush().await?;
        }

        Ok(())
    }
}
