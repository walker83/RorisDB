use bytes::{BufMut, BytesMut};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub const TDS_LANGUAGE: u8 = 0x01;
pub const TDS_RPC: u8 = 0x03;
pub const TDS_REPLY: u8 = 0x04;
pub const TDS_BULK: u8 = 0x07;
pub const TDS_LOGIN: u8 = 0x10;
pub const TDS_ATTENTION: u8 = 0x12;

pub const STATUS_EOM: u8 = 0x01;
pub const STATUS_NORMAL: u8 = 0x00;

#[derive(Debug, Clone)]
pub struct TdsPacket {
    pub packet_type: u8,
    pub status: u8,
    pub data: Vec<u8>,
}

impl TdsPacket {
    pub fn new(packet_type: u8, data: Vec<u8>) -> Self {
        Self { packet_type, status: STATUS_EOM, data }
    }

    pub fn is_eom(&self) -> bool { self.status & STATUS_EOM != 0 }
}

pub async fn read_tds_packet(stream: &mut TcpStream) -> io::Result<TdsPacket> {
    let mut header = [0u8; 8];
    stream.read_exact(&mut header).await?;
    let packet_type = header[0];
    let status = header[1];
    let length = u16::from_le_bytes([header[2], header[3]]) as usize;
    if length < 8 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid TDS packet length"));
    }
    let data_len = length - 8;
    let mut data = vec![0u8; data_len];
    if data_len > 0 {
        stream.read_exact(&mut data).await?;
    }
    Ok(TdsPacket { packet_type, status, data })
}

pub async fn write_tds_packet(stream: &mut TcpStream, packet: &TdsPacket) -> io::Result<()> {
    let total_len = packet.data.len() + 8;
    if total_len > u16::MAX as usize {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "TDS packet too large"));
    }
    let length = total_len as u16;
    let mut buf = BytesMut::with_capacity(total_len);
    buf.put_u8(packet.packet_type);
    buf.put_u8(packet.status);
    buf.put_u16_le(length);
    buf.put_u16(0); // spid
    buf.put_u8(0); // packet number
    buf.put_u8(0); // window
    buf.put_slice(&packet.data);
    stream.write_all(&buf).await?;
    stream.flush().await?;
    Ok(())
}
