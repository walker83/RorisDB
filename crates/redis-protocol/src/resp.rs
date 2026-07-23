//! RESP2/RESP3 protocol parser and encoder
//!
//! RESP (REdis Serialization Protocol) format:
//! - Simple String: +OK\r\n
//! - Error: -ERR message\r\n
//! - Integer: :1000\r\n
//! - Bulk String: $6\r\nfoobar\r\n (or $-1\r\n for null)
//! - Array: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n (or *-1\r\n for null)

use bytes::{Buf, BytesMut, BufMut};
use std::io;

/// RESP value types
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    /// Simple string (+OK\r\n)
    SimpleString(String),
    /// Error (-ERR message\r\n)
    Error(String),
    /// Integer (:1000\r\n)
    Integer(i64),
    /// Bulk string ($6\r\nfoobar\r\n)
    BulkString(Vec<u8>),
    /// Null bulk string ($-1\r\n)
    Null,
    /// Array (*2\r\n...)
    Array(Vec<RespValue>),
    /// Null array (*-1\r\n)
    NullArray,
}

impl RespValue {
    /// Create a bulk string from a string
    pub fn bulk_string(s: impl Into<String>) -> Self {
        RespValue::BulkString(s.into().into_bytes())
    }

    /// Create a bulk string from bytes
    pub fn bulk_bytes(b: Vec<u8>) -> Self {
        RespValue::BulkString(b)
    }

    /// Try to convert to string
    pub fn as_str(&self) -> Option<&str> {
        match self {
            RespValue::BulkString(b) => std::str::from_utf8(b).ok(),
            RespValue::SimpleString(s) => Some(s),
            _ => None,
        }
    }

    /// Try to convert to integer
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            RespValue::Integer(n) => Some(*n),
            RespValue::BulkString(b) => {
                std::str::from_utf8(b).ok()?.parse().ok()
            }
            _ => None,
        }
    }

    /// Check if null
    pub fn is_null(&self) -> bool {
        matches!(self, RespValue::Null | RespValue::NullArray)
    }
}

/// RESP parser
pub struct RespParser;

impl RespParser {
    /// Parse a complete RESP value from buffer
    /// Returns None if buffer doesn't contain a complete value
    pub fn parse(buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
        if buf.is_empty() {
            return Ok(None);
        }

        let prefix = buf[0];
        match prefix {
            b'+' => Self::parse_simple_string(buf),
            b'-' => Self::parse_error(buf),
            b':' => Self::parse_integer(buf),
            b'$' => Self::parse_bulk_string(buf),
            b'*' => Self::parse_array(buf),
            _ => {
                // Inline command (e.g., "PING\r\n")
                Self::parse_inline(buf)
            }
        }
    }

    fn parse_simple_string(buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
        Self::parse_line(buf).map(|opt| opt.map(RespValue::SimpleString))
    }

    fn parse_error(buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
        Self::parse_line(buf).map(|opt| opt.map(RespValue::Error))
    }

    fn parse_integer(buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
        Self::parse_line(buf).and_then(|opt| {
            match opt {
                Some(line) => match line.parse::<i64>() {
                    Ok(n) => Ok(Some(RespValue::Integer(n))),
                    Err(_) => Err(RespError::InvalidInteger(line)),
                },
                None => Ok(None),
            }
        })
    }

    fn parse_bulk_string(buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
        // Find the length line
        let newline_pos = match Self::find_crlf(buf, 1) {
            Some(pos) => pos,
            None => return Ok(None), // Incomplete
        };

        // Parse length
        let len_str = std::str::from_utf8(&buf[1..newline_pos])
            .map_err(|_| RespError::InvalidData)?;
        let len: i64 = len_str.parse()
            .map_err(|_| RespError::InvalidInteger(len_str.to_string()))?;

        // Null bulk string (only -1 is valid per RESP spec)
        if len == -1 {
            buf.advance(newline_pos + 2);
            return Ok(Some(RespValue::Null));
        }
        if len < -1 {
            return Err(RespError::InvalidData);
        }

        let len = len as usize;
        // Limit bulk string size to 512MB (Redis default)
        if len > 512 * 1024 * 1024 {
            return Err(RespError::InvalidData);
        }
        let total_len = newline_pos + 2 + len + 2; // header + data + CRLF

        if buf.len() < total_len {
            return Ok(None); // Incomplete
        }

        // Extract data
        let data_start = newline_pos + 2;
        let data = buf[data_start..data_start + len].to_vec();
        buf.advance(total_len);

        Ok(Some(RespValue::BulkString(data)))
    }

    fn parse_array(buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
        // First, check if the complete array is available without consuming anything
        match Self::measure_value(buf, 0, 0) {
            Some(total_size) if total_size <= buf.len() => {
                // We have all the data, safe to parse
            }
            Some(_) => return Ok(None), // Incomplete
            None => return Ok(None),    // Can't even measure yet
        }

        // Now parse for real (all data is guaranteed to be available).
        // Find the count line. `find_crlf` should always succeed here because
        // `measure_value` already verified the frame is complete, but a bug in
        // that function must not turn a malformed packet into a panic.
        let newline_pos = Self::find_crlf(buf, 1)
            .ok_or(RespError::InvalidData)?;

        let count_str = std::str::from_utf8(&buf[1..newline_pos])
            .map_err(|_| RespError::InvalidData)?;
        let count: i64 = count_str.parse()
            .map_err(|_| RespError::InvalidInteger(count_str.to_string()))?;

        if count < 0 {
            buf.advance(newline_pos + 2);
            return Ok(Some(RespValue::NullArray));
        }

        let count = count as usize;
        // Limit array size to 1M elements
        if count > 1_000_000 {
            return Err(RespError::InvalidData);
        }
        buf.advance(newline_pos + 2);

        let mut elements = Vec::with_capacity(count);
        for _ in 0..count {
            match Self::parse(buf)? {
                Some(val) => elements.push(val),
                None => {
                    // Should not happen since we verified completeness above
                    return Ok(None);
                }
            }
        }

        Ok(Some(RespValue::Array(elements)))
    }

    /// Measure the total byte size of a RESP value starting at `offset` in the buffer.
    /// Returns None if the data is incomplete or invalid.
    /// Does NOT modify the buffer.
    ///
    /// `depth` tracks the array nesting depth to prevent stack overflow on
    /// malicious deeply-nested payloads (e.g. `*1\r\n*1\r\n*1\r\n...`).
    fn measure_value(buf: &[u8], offset: usize, depth: usize) -> Option<usize> {
        const MAX_DEPTH: usize = 128;
        if offset >= buf.len() {
            return None;
        }

        match buf[offset] {
            b'+' | b'-' | b':' => {
                // Simple string, error, or integer - find CRLF
                let start = offset + 1;
                for i in start..buf.len().saturating_sub(1) {
                    if buf[i] == b'\r' && buf[i + 1] == b'\n' {
                        return Some(i + 2 - offset);
                    }
                }
                None // Incomplete
            }
            b'$' => {
                // Bulk string
                let newline_pos = Self::find_crlf_in(buf, offset + 1)?;
                let len_str = std::str::from_utf8(&buf[offset + 1..newline_pos]).ok()?;
                let len: i64 = len_str.parse().ok()?;
                if len < 0 {
                    Some(newline_pos + 2 - offset) // $-1\r\n
                } else {
                    let total = newline_pos + 2 + (len as usize) + 2;
                    if total <= buf.len() {
                        Some(total - offset)
                    } else {
                        None
                    }
                }
            }
            b'*' => {
                // Array — reject nesting beyond MAX_DEPTH to avoid stack
                // overflow from a pathological payload.
                if depth >= MAX_DEPTH {
                    return None;
                }
                let newline_pos = Self::find_crlf_in(buf, offset + 1)?;
                let count_str = std::str::from_utf8(&buf[offset + 1..newline_pos]).ok()?;
                let count: i64 = count_str.parse().ok()?;
                if count < 0 {
                    Some(newline_pos + 2 - offset)
                } else {
                    let mut pos = newline_pos + 2;
                    for _ in 0..count {
                        let elem_size = Self::measure_value(buf, pos, depth + 1)?;
                        pos += elem_size;
                    }
                    if pos <= buf.len() {
                        Some(pos - offset)
                    } else {
                        None
                    }
                }
            }
            _ => {
                // Inline command - find end of line
                for i in offset..buf.len().saturating_sub(1) {
                    if buf[i] == b'\r' && buf[i + 1] == b'\n' {
                        return Some(i + 2 - offset);
                    }
                }
                None
            }
        }
    }

    /// Like find_crlf but works on a slice with an arbitrary start offset
    fn find_crlf_in(buf: &[u8], start: usize) -> Option<usize> {
        for i in start..buf.len().saturating_sub(1) {
            if buf[i] == b'\r' && buf[i + 1] == b'\n' {
                return Some(i);
            }
        }
        None
    }

    fn parse_inline(buf: &mut BytesMut) -> Result<Option<RespValue>, RespError> {
        match Self::parse_line_inline(buf) {
            Ok(Some(line)) => {
                // Split by spaces into array
                let parts: Vec<RespValue> = line
                    .split_whitespace()
                    .map(|s| RespValue::BulkString(s.as_bytes().to_vec()))
                    .collect();
                Ok(Some(RespValue::Array(parts)))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn parse_line(buf: &mut BytesMut) -> Result<Option<String>, RespError> {
        match Self::find_crlf(buf, 1) {
            Some(pos) => {
                let line = std::str::from_utf8(&buf[1..pos])
                    .map_err(|_| RespError::InvalidData)?
                    .to_string();
                buf.advance(pos + 2);
                Ok(Some(line))
            }
            None => Ok(None),
        }
    }

    fn parse_line_inline(buf: &mut BytesMut) -> Result<Option<String>, RespError> {
        match Self::find_crlf(buf, 0) {
            Some(pos) => {
                let line = std::str::from_utf8(&buf[0..pos])
                    .map_err(|_| RespError::InvalidData)?
                    .to_string();
                buf.advance(pos + 2);
                Ok(Some(line))
            }
            None => Ok(None),
        }
    }

    fn find_crlf(buf: &[u8], start: usize) -> Option<usize> {
        for i in start..buf.len().saturating_sub(1) {
            if buf[i] == b'\r' && buf[i + 1] == b'\n' {
                return Some(i);
            }
        }
        None
    }
}

/// RESP encoder
pub struct RespEncoder;

impl RespEncoder {
    /// Encode a RESP value into buffer
    pub fn encode(val: &RespValue, buf: &mut BytesMut) {
        match val {
            RespValue::SimpleString(s) => {
                buf.put_u8(b'+');
                buf.put_slice(s.as_bytes());
                buf.put_slice(b"\r\n");
            }
            RespValue::Error(e) => {
                buf.put_u8(b'-');
                buf.put_slice(e.as_bytes());
                buf.put_slice(b"\r\n");
            }
            RespValue::Integer(n) => {
                buf.put_u8(b':');
                buf.put_slice(n.to_string().as_bytes());
                buf.put_slice(b"\r\n");
            }
            RespValue::BulkString(b) => {
                buf.put_u8(b'$');
                buf.put_slice(b.len().to_string().as_bytes());
                buf.put_slice(b"\r\n");
                buf.put_slice(b);
                buf.put_slice(b"\r\n");
            }
            RespValue::Null => {
                buf.put_slice(b"$-1\r\n");
            }
            RespValue::Array(arr) => {
                buf.put_u8(b'*');
                buf.put_slice(arr.len().to_string().as_bytes());
                buf.put_slice(b"\r\n");
                for elem in arr {
                    Self::encode(elem, buf);
                }
            }
            RespValue::NullArray => {
                buf.put_slice(b"*-1\r\n");
            }
        }
    }

    /// Encode to a new BytesMut
    pub fn encode_to_bytes(val: &RespValue) -> BytesMut {
        let mut buf = BytesMut::with_capacity(256);
        Self::encode(val, &mut buf);
        buf
    }

    /// Helper: OK response
    pub fn ok() -> BytesMut {
        Self::encode_to_bytes(&RespValue::SimpleString("OK".to_string()))
    }

    /// Helper: PONG response
    pub fn pong() -> BytesMut {
        Self::encode_to_bytes(&RespValue::SimpleString("PONG".to_string()))
    }

    /// Helper: Error response
    pub fn error(msg: impl Into<String>) -> BytesMut {
        Self::encode_to_bytes(&RespValue::Error(msg.into()))
    }

    /// Helper: Integer response
    pub fn integer(n: i64) -> BytesMut {
        Self::encode_to_bytes(&RespValue::Integer(n))
    }

    /// Helper: Bulk string response
    pub fn bulk_string(s: impl Into<Vec<u8>>) -> BytesMut {
        Self::encode_to_bytes(&RespValue::BulkString(s.into()))
    }

    /// Helper: Null response
    pub fn null() -> BytesMut {
        Self::encode_to_bytes(&RespValue::Null)
    }
}

/// RESP protocol errors
#[derive(Debug)]
pub enum RespError {
    InvalidData,
    InvalidInteger(String),
    Io(io::Error),
}

impl std::fmt::Display for RespError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RespError::InvalidData => write!(f, "Invalid RESP data"),
            RespError::InvalidInteger(s) => write!(f, "Invalid integer: {}", s),
            RespError::Io(e) => write!(f, "IO error: {}", e),
        }
    }
}

impl std::error::Error for RespError {}

impl From<io::Error> for RespError {
    fn from(e: io::Error) -> Self {
        RespError::Io(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_string() {
        let mut buf = BytesMut::from("+OK\r\n");
        let val = RespParser::parse(&mut buf).unwrap().unwrap();
        assert_eq!(val, RespValue::SimpleString("OK".to_string()));
    }

    #[test]
    fn test_parse_error() {
        let mut buf = BytesMut::from("-ERR unknown command\r\n");
        let val = RespParser::parse(&mut buf).unwrap().unwrap();
        assert_eq!(val, RespValue::Error("ERR unknown command".to_string()));
    }

    #[test]
    fn test_parse_integer() {
        let mut buf = BytesMut::from(":1000\r\n");
        let val = RespParser::parse(&mut buf).unwrap().unwrap();
        assert_eq!(val, RespValue::Integer(1000));
    }

    #[test]
    fn test_parse_bulk_string() {
        let mut buf = BytesMut::from("$6\r\nfoobar\r\n");
        let val = RespParser::parse(&mut buf).unwrap().unwrap();
        assert_eq!(val, RespValue::BulkString(b"foobar".to_vec()));
    }

    #[test]
    fn test_parse_null_bulk_string() {
        let mut buf = BytesMut::from("$-1\r\n");
        let val = RespParser::parse(&mut buf).unwrap().unwrap();
        assert_eq!(val, RespValue::Null);
    }

    #[test]
    fn test_parse_array() {
        let mut buf = BytesMut::from("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let val = RespParser::parse(&mut buf).unwrap().unwrap();
        assert_eq!(val, RespValue::Array(vec![
            RespValue::BulkString(b"foo".to_vec()),
            RespValue::BulkString(b"bar".to_vec()),
        ]));
    }

    #[test]
    fn test_parse_deeply_nested_array_does_not_overflow() {
        // A pathological payload of 500 nested single-element arrays must not
        // crash the parser with a stack overflow. Before the depth limit it
        // recursed ~500 frames; now it bails out at depth 128 and returns
        // Incomplete (None) instead.
        let mut payload = Vec::new();
        for _ in 0..500 {
            payload.extend_from_slice(b"*1\r\n");
        }
        payload.extend_from_slice(b"$1\r\na\r\n");
        let mut buf = BytesMut::from(&payload[..]);
        // Must not panic. Either Ok(None) (incomplete) or an error is fine.
        let _ = RespParser::parse(&mut buf);
    }

    #[test]
    fn test_encode_ok() {
        let buf = RespEncoder::ok();
        assert_eq!(&buf[..], b"+OK\r\n");
    }

    #[test]
    fn test_encode_error() {
        let buf = RespEncoder::error("ERR test");
        assert_eq!(&buf[..], b"-ERR test\r\n");
    }

    #[test]
    fn test_encode_integer() {
        let buf = RespEncoder::integer(42);
        assert_eq!(&buf[..], b":42\r\n");
    }

    #[test]
    fn test_encode_bulk_string() {
        let buf = RespEncoder::bulk_string(b"hello");
        assert_eq!(&buf[..], b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_roundtrip() {
        let original = RespValue::Array(vec![
            RespValue::BulkString(b"SET".to_vec()),
            RespValue::BulkString(b"key".to_vec()),
            RespValue::BulkString(b"value".to_vec()),
        ]);

        let encoded = RespEncoder::encode_to_bytes(&original);
        let mut buf = BytesMut::from(&encoded[..]);
        let parsed = RespParser::parse(&mut buf).unwrap().unwrap();

        assert_eq!(original, parsed);
    }
}
