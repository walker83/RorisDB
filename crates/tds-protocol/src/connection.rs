use std::sync::Arc;
use tokio::net::TcpStream;
use mysql_protocol::server::{QueryHandler, QueryResult};
use crate::packet::*;
use crate::token::*;
use crate::types::*;

pub async fn run_connection(
    mut stream: TcpStream,
    handler: Arc<dyn QueryHandler>,
    conn_id: u32,
) {
    // 1. Read TDS Login packet
    let _login_packet = match read_tds_packet(&mut stream).await {
        Ok(p) if p.packet_type == TDS_LOGIN => p,
        Ok(_) => {
            tracing::error!("Expected TDS login packet");
            return;
        }
        Err(e) => {
            tracing::error!("Error reading TDS login: {}", e);
            return;
        }
    };

    // 2. Send LoginAck + EnvChange
    let mut reply_data = Vec::new();
    reply_data.extend_from_slice(&encode_login_ack());
    reply_data.extend_from_slice(&encode_env_change_db("master"));

    let login_ack = TdsPacket::new(TDS_REPLY, reply_data);
    if let Err(e) = write_tds_packet(&mut stream, &login_ack).await {
        tracing::error!("Error writing login ack: {}", e);
        return;
    }

    // 3. Command loop
    loop {
        let packet = match read_tds_packet(&mut stream).await {
            Ok(p) => p,
            Err(e) => {
                tracing::debug!("Connection {} closed: {}", conn_id, e);
                break;
            }
        };

        match packet.packet_type {
            TDS_LANGUAGE => {
                // First byte is status indicator, rest is SQL text (UTF-16LE)
                let sql_data = if packet.data.len() > 1 {
                    &packet.data[1..]
                } else {
                    &packet.data
                };
                let sql = decode_tds_string(sql_data);
                tracing::debug!("TDS Language: {}", sql);

                let result = handler.handle_query(conn_id, &sql);
                let reply = encode_query_result(&result);
                let tds_reply = TdsPacket::new(TDS_REPLY, reply);
                if let Err(e) = write_tds_packet(&mut stream, &tds_reply).await {
                    tracing::error!("Error writing reply: {}", e);
                    break;
                }
            }
            TDS_RPC => {
                // RPC has different format than LANGUAGE: skip RPC header bytes
                // TDS RPC format: [procedure_name_len(2)] [procedure_name(UTF-16LE)] [parameters...]
                // For simplicity, extract SQL from the data portion after the RPC header
                let sql_data = if packet.data.len() > 2 {
                    let name_len = u16::from_le_bytes([packet.data[0], packet.data[1]]) as usize;
                    let header_end = 2 + name_len * 2;
                    if header_end < packet.data.len() {
                        &packet.data[header_end..]
                    } else {
                        &packet.data
                    }
                } else {
                    &packet.data
                };
                let sql = decode_tds_string(sql_data);
                let result = handler.handle_query(conn_id, &sql);
                let reply = encode_query_result(&result);
                let tds_reply = TdsPacket::new(TDS_REPLY, reply);
                if let Err(e) = write_tds_packet(&mut stream, &tds_reply).await {
                    tracing::error!("Error writing reply: {}", e);
                    break;
                }
            }
            TDS_ATTENTION => {
                // Cancel — send DONE
                let reply = encode_done(0, 0x0020); // attention ack
                let tds_reply = TdsPacket::new(TDS_REPLY, reply);
                let _ = write_tds_packet(&mut stream, &tds_reply).await;
            }
            _ => {
                tracing::warn!("Unknown TDS packet type: 0x{:02x}", packet.packet_type);
                // Send error response for unhandled opcodes
                let err = encode_error(0, 0, 1, &format!("Unsupported packet type 0x{:02x}", packet.packet_type), "HarnessDB");
                let err_reply = TdsPacket::new(TDS_REPLY, err);
                let _ = write_tds_packet(&mut stream, &err_reply).await;
            }
        }
    }
}

fn decode_tds_string(data: &[u8]) -> String {
    // TDS strings may be UTF-16LE or ASCII depending on context
    // Try UTF-16LE first, fall back to UTF-8
    if data.len() >= 2 && data.len() % 2 == 0 {
        let chars: Vec<u16> = data.chunks_exact(2)
            .map(|c| u16::from_le_bytes([c[0], c[1]]))
            .collect();
        if let Some(s) = String::from_utf16(&chars).ok().filter(|s| s.chars().all(|c| !c.is_control() || c == '\n' || c == '\r' || c == '\t')) {
            return s;
        }
    }
    String::from_utf8_lossy(data).to_string()
}

fn encode_query_result(result: &QueryResult) -> Vec<u8> {
    let mut buf = Vec::new();

    if !result.columns.is_empty() {
        // Column metadata
        let cols: Vec<TdsColumnDef> = result.columns.iter().map(|c| TdsColumnDef {
            name: c.name.clone(),
            tds_type: TDS_TYPE_VARCHAR,
            max_length: 8000,
        }).collect();
        buf.extend_from_slice(&encode_colmetadata(&cols));

        // Rows
        for row in &result.rows {
            buf.extend_from_slice(&encode_row(row));
        }
    }

    // Done token
    let row_count = result.rows.len() as u64;
    buf.extend_from_slice(&encode_done(row_count, 0x0010)); // DONE_FINAL
    buf
}
