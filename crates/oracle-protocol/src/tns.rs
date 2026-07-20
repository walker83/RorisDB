//! Oracle TNS protocol message formats
//! This is a simplified simulation of Oracle TNS protocol

use bytes::{Buf, BufMut, BytesMut};

/// TNS packet types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TnsPacketType {
    Connect = 1,
    Accept = 2,
    Reject = 4,
    Data = 8,
    Response = 9,
    Redirect = 11,
    Marker = 12,
}

impl TnsPacketType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Connect),
            2 => Some(Self::Accept),
            4 => Some(Self::Reject),
            8 => Some(Self::Data),
            9 => Some(Self::Response),
            11 => Some(Self::Redirect),
            12 => Some(Self::Marker),
            _ => None,
        }
    }
}

/// TNS packet header (8 bytes)
#[derive(Debug, Clone)]
pub struct TnsHeader {
    pub packet_length: u16,
    pub header_checksum: u16,
    pub packet_type: TnsPacketType,
    pub flags: u8,
    pub header_checksum2: u16,
}

impl TnsHeader {
    pub const SIZE: usize = 8;

    pub fn parse(buf: &mut BytesMut) -> Option<Self> {
        if buf.len() < Self::SIZE {
            return None;
        }

        let packet_length = buf.get_u16();
        let header_checksum = buf.get_u16();
        let packet_type = TnsPacketType::from_u8(buf.get_u8())?;
        let flags = buf.get_u8();
        let header_checksum2 = buf.get_u16();

        Some(Self {
            packet_length,
            header_checksum,
            packet_type,
            flags,
            header_checksum2,
        })
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u16(self.packet_length);
        buf.put_u16(self.header_checksum);
        buf.put_u8(self.packet_type as u8);
        buf.put_u8(self.flags);
        buf.put_u16(self.header_checksum2);
    }
}

/// TNS packet
#[derive(Debug, Clone)]
pub struct TnsPacket {
    pub header: TnsHeader,
    pub data: Vec<u8>,
}

impl TnsPacket {
    pub fn parse(buf: &mut BytesMut) -> Option<Self> {
        let header = TnsHeader::parse(buf)?;

        if (header.packet_length as usize) < TnsHeader::SIZE {
            return None; // Invalid packet length
        }
        let data_length = header.packet_length as usize - TnsHeader::SIZE;
        if data_length > 16 * 1024 * 1024 {
            return None; // Max 16MB
        }
        if buf.len() < data_length {
            return None;
        }

        let data = buf.split_to(data_length).to_vec();

        Some(Self { header, data })
    }

    pub fn new(packet_type: TnsPacketType, data: Vec<u8>) -> Self {
        let packet_length = (TnsHeader::SIZE + data.len()) as u16;
        Self {
            header: TnsHeader {
                packet_length,
                header_checksum: 0,
                packet_type,
                flags: 0,
                header_checksum2: 0,
            },
            data,
        }
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        self.header.encode(buf);
        buf.put_slice(&self.data);
    }
}

/// Oracle TNS Connect packet data
#[derive(Debug, Clone)]
pub struct ConnectData {
    pub version: u16,
    pub compatible: u16,
    pub service: String,
}

impl ConnectData {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 24 {
            return None;
        }

        let mut buf = BytesMut::from(&data[..]);
        // Read 6 x u32 connect options: version, compatible, options, flags, facility, reserved
        let version = buf.get_u32() as u16;
        let compatible = buf.get_u32() as u16;
        let _ns_options = buf.get_u32();
        let _flags = buf.get_u32();
        let _facility = buf.get_u32();
        let _reserved = buf.get_u32();

        // Remaining bytes are the connect descriptor string
        let connect_string = String::from_utf8_lossy(&buf[..]).to_string();
        let service = connect_string
            .split("SERVICE_NAME=")
            .nth(1)
            .and_then(|s| s.split(')').next())
            .unwrap_or("ORCL")
            .to_string();

        Some(Self {
            version,
            compatible,
            service,
        })
    }
}
