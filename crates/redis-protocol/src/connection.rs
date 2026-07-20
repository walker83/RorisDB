//! Redis TCP connection handler

use crate::resp::{RespParser, RespEncoder, RespValue};
use crate::handler::RedisCommandHandler;
use bytes::BytesMut;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, error, info};

/// Redis connection handler
pub struct RedisConnection {
    stream: TcpStream,
    read_buf: BytesMut,
    handler: Arc<dyn RedisCommandHandler>,
    db_index: usize,
    conn_id: u32,
}

impl RedisConnection {
    pub fn new(
        stream: TcpStream,
        handler: Arc<dyn RedisCommandHandler>,
        conn_id: u32,
    ) -> Self {
        Self {
            stream,
            read_buf: BytesMut::with_capacity(4096),
            handler,
            db_index: 0,
            conn_id,
        }
    }

    /// Run the connection loop
    pub async fn run(mut self) -> std::io::Result<()> {
        info!("Redis connection {} established", self.conn_id);

        loop {
            // Read data from socket
            let n = match self.stream.read_buf(&mut self.read_buf).await {
                Ok(0) => {
                    info!("Redis connection {} closed", self.conn_id);
                    return Ok(());
                }
                Ok(n) => n,
                Err(e) => {
                    error!("Redis connection {} read error: {}", self.conn_id, e);
                    return Err(e);
                }
            };

            debug!("Redis connection {} read {} bytes", self.conn_id, n);

            // Parse and handle commands
            while !self.read_buf.is_empty() {
                match RespParser::parse(&mut self.read_buf) {
                    Ok(Some(command)) => {
                        debug!("Redis connection {} command: {:?}", self.conn_id, command);

                        // Check for SELECT command to update db_index
                        if let RespValue::Array(ref arr) = command {
                            if !arr.is_empty() {
                                if let Some(cmd_str) = match &arr[0] {
                                    RespValue::BulkString(s) => std::str::from_utf8(s).ok(),
                                    RespValue::SimpleString(s) => Some(s.as_str()),
                                    _ => None,
                                } {
                                    if cmd_str.to_uppercase() == "SELECT" && arr.len() > 1 {
                                        if let Some(db) = match &arr[1] {
                                            RespValue::Integer(n) => Some(*n as usize),
                                            RespValue::BulkString(s) => {
                                                std::str::from_utf8(s).ok().and_then(|s| s.parse().ok())
                                            }
                                            _ => None,
                                        } {
                                            if db < 16 {
                                                self.db_index = db;
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Handle command
                        let cmd_array = match &command {
                            RespValue::Array(arr) => arr.clone(),
                            _ => vec![command.clone()],
                        };
                        let response = self.handler.handle_command(self.db_index, &cmd_array);

                        // Write response
                        self.stream.write_all(&response).await?;
                    }
                    Ok(None) => {
                        // Incomplete command, need more data
                        break;
                    }
                    Err(e) => {
                        error!("Redis connection {} parse error: {}", self.conn_id, e);
                        let err_response = RespEncoder::error("ERR protocol error");
                        self.stream.write_all(&err_response).await?;
                        break;
                    }
                }
            }
        }
    }
}
