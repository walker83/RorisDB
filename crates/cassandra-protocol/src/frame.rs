//! Cassandra native protocol frame format

use bytes::{Buf, BufMut, BytesMut};

/// Cassandra frame header (9 bytes for v4)
#[derive(Debug, Clone)]
pub struct FrameHeader {
    pub version: u8,
    pub flags: u8,
    pub stream: i16,
    pub opcode: u8,
    pub length: i32,
}

impl FrameHeader {
    pub const SIZE: usize = 9;

    pub fn parse(buf: &mut BytesMut) -> Option<Self> {
        if buf.len() < Self::SIZE {
            return None;
        }

        let version = buf[0];
        let flags = buf[1];
        let stream = i16::from_be_bytes([buf[2], buf[3]]);
        let opcode = buf[4];
        let length = i32::from_be_bytes([buf[5], buf[6], buf[7], buf[8]]);

        Some(Self {
            version,
            flags,
            stream,
            opcode,
            length,
        })
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.version);
        buf.put_u8(self.flags);
        buf.put_i16(self.stream);
        buf.put_u8(self.opcode);
        buf.put_i32(self.length);
    }
}

/// Cassandra opcodes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    Error = 0x00,
    Startup = 0x01,
    Ready = 0x02,
    Authenticate = 0x03,
    Options = 0x05,
    Supported = 0x06,
    Query = 0x07,
    Result = 0x08,
    Prepare = 0x09,
    Execute = 0x0A,
    Register = 0x0B,
    Event = 0x0C,
    Batch = 0x0D,
    AuthChallenge = 0x0E,
    AuthResponse = 0x0F,
    AuthSuccess = 0x10,
}

impl Opcode {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x00 => Some(Self::Error),
            0x01 => Some(Self::Startup),
            0x02 => Some(Self::Ready),
            0x03 => Some(Self::Authenticate),
            0x05 => Some(Self::Options),
            0x06 => Some(Self::Supported),
            0x07 => Some(Self::Query),
            0x08 => Some(Self::Result),
            0x09 => Some(Self::Prepare),
            0x0A => Some(Self::Execute),
            0x0B => Some(Self::Register),
            0x0C => Some(Self::Event),
            0x0D => Some(Self::Batch),
            0x0E => Some(Self::AuthChallenge),
            0x0F => Some(Self::AuthResponse),
            0x10 => Some(Self::AuthSuccess),
            _ => None,
        }
    }
}

/// Cassandra frame
#[derive(Debug, Clone)]
pub struct Frame {
    pub header: FrameHeader,
    pub body: Vec<u8>,
}

impl Frame {
    pub fn parse(buf: &mut BytesMut) -> Option<Self> {
        let header = FrameHeader::parse(buf)?;

        if header.length < 0 {
            return None; // Invalid negative length
        }
        let length = header.length as usize;
        if length > 16 * 1024 * 1024 {
            return None; // Max 16MB body
        }
        // Check if we have the full frame (header + body) before consuming anything
        if buf.len() < FrameHeader::SIZE + length {
            return None;
        }

        // Now safe to consume: skip header, take body
        buf.advance(FrameHeader::SIZE);
        let body = buf.split_to(length).to_vec();

        Some(Self { header, body })
    }

    pub fn new(version: u8, stream: i16, opcode: Opcode, body: Vec<u8>) -> Self {
        let length = body.len() as i32;
        Self {
            header: FrameHeader {
                version,
                flags: 0,
                stream,
                opcode: opcode as u8,
                length,
            },
            body,
        }
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        self.header.encode(buf);
        buf.put_slice(&self.body);
    }
}
