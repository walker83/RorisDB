use bytes::{BufMut, BytesMut};
use types::ScalarValue;

use crate::charset::{self, DEFAULT_CHARSET};
use crate::value::{encode_lenenc_int, encode_lenenc_str};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum size of a single MySQL packet payload.
pub const MAX_PACKET_SIZE: usize = 0x00FF_FFFF;

/// Default server capability flags.
pub const DEFAULT_CAPABILITIES: u32 = CapabilityFlags::PROTOCOL_41
    | CapabilityFlags::PLUGIN_AUTH
    | CapabilityFlags::SECURE_CONNECTION
    | CapabilityFlags::CONNECT_WITH_DB
    | CapabilityFlags::LONG_FLAG
    | CapabilityFlags::TRANSACTIONS
    | CapabilityFlags::MULTI_STATEMENTS
    | CapabilityFlags::MULTI_RESULTS
    | CapabilityFlags::PS_MULTI_RESULTS
    | CapabilityFlags::PLUGIN_AUTH_LENENC_CLIENT_DATA
    | CapabilityFlags::DEPRECATE_EOF
    | CapabilityFlags::CONNECT_ATTRS;

/// Server status flags sent in OK packets.
pub const SERVER_STATUS_AUTOCOMMIT: u16 = 0x0002;

/// MySQL native password auth plugin name.
pub const AUTH_PLUGIN_NAME: &[u8] = b"mysql_native_password";

/// Column type constants (MYSQL_TYPE_*).
pub mod column_type {
    pub const DECIMAL: u8 = 0x00;
    pub const TINY: u8 = 0x01;
    pub const SHORT: u8 = 0x02;
    pub const LONG: u8 = 0x03;
    pub const FLOAT: u8 = 0x04;
    pub const DOUBLE: u8 = 0x05;
    pub const NULL: u8 = 0x06;
    pub const TIMESTAMP: u8 = 0x07;
    pub const LONGLONG: u8 = 0x08;
    pub const INT24: u8 = 0x09;
    pub const DATE: u8 = 0x0A;
    pub const TIME: u8 = 0x0B;
    pub const DATETIME: u8 = 0x0C;
    pub const YEAR: u8 = 0x0D;
    pub const VARCHAR: u8 = 0x0F;
    pub const BIT: u8 = 0x10;
    pub const NEWDECIMAL: u8 = 0xF6;
    pub const ENUM: u8 = 0xF7;
    pub const SET: u8 = 0xF8;
    pub const TINY_BLOB: u8 = 0xF9;
    pub const MEDIUM_BLOB: u8 = 0xFA;
    pub const LONG_BLOB: u8 = 0xFB;
    pub const BLOB: u8 = 0xFC;
    pub const VAR_STRING: u8 = 0xFD;
    pub const STRING: u8 = 0xFE;
    pub const GEOMETRY: u8 = 0xFF;
}

// ---------------------------------------------------------------------------
// Capability flags
// ---------------------------------------------------------------------------

#[allow(non_snake_case)]
pub mod CapabilityFlags {
    pub const LONG_PASSWORD: u32 = 0x00000001;
    pub const FOUND_ROWS: u32 = 0x00000002;
    pub const LONG_FLAG: u32 = 0x00000004;
    pub const CONNECT_WITH_DB: u32 = 0x00000008;
    pub const NO_SCHEMA: u32 = 0x00000010;
    pub const COMPRESS: u32 = 0x00000020;
    pub const ODBC: u32 = 0x00000040;
    pub const LOCAL_FILES: u32 = 0x00000080;
    pub const IGNORE_SPACE: u32 = 0x00000100;
    pub const PROTOCOL_41: u32 = 0x00000200;
    pub const INTERACTIVE: u32 = 0x00000400;
    pub const SSL: u32 = 0x00000800;
    pub const IGNORE_SIGPIPE: u32 = 0x00001000;
    pub const TRANSACTIONS: u32 = 0x00002000;
    pub const RESERVED: u32 = 0x00004000;
    pub const SECURE_CONNECTION: u32 = 0x00008000;
    pub const MULTI_STATEMENTS: u32 = 0x00010000;
    pub const MULTI_RESULTS: u32 = 0x00020000;
    pub const PS_MULTI_RESULTS: u32 = 0x00040000;
    pub const PLUGIN_AUTH: u32 = 0x00080000;
    pub const CONNECT_ATTRS: u32 = 0x00100000;
    pub const PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 0x00200000;
    pub const CAN_HANDLE_EXPIRED_PASSWORDS: u32 = 0x00400000;
    pub const SESSION_TRACK: u32 = 0x00800000;
    pub const DEPRECATE_EOF: u32 = 0x01000000;
}

// ---------------------------------------------------------------------------
// Command types
// ---------------------------------------------------------------------------

pub mod command {
    pub const COM_SLEEP: u8 = 0x00;
    pub const COM_QUIT: u8 = 0x01;
    pub const COM_INIT_DB: u8 = 0x02;
    pub const COM_QUERY: u8 = 0x03;
    pub const COM_FIELD_LIST: u8 = 0x04;
    pub const COM_CREATE_DB: u8 = 0x05;
    pub const COM_DROP_DB: u8 = 0x06;
    pub const COM_REFRESH: u8 = 0x07;
    pub const COM_SHUTDOWN: u8 = 0x08;
    pub const COM_STATISTICS: u8 = 0x09;
    pub const COM_PROCESS_INFO: u8 = 0x0A;
    pub const COM_CONNECT: u8 = 0x0B;
    pub const COM_PROCESS_KILL: u8 = 0x0C;
    pub const COM_DEBUG: u8 = 0x0D;
    pub const COM_PING: u8 = 0x0E;
    pub const COM_TIME: u8 = 0x0F;
    pub const COM_DELAYED_INSERT: u8 = 0x10;
    pub const COM_CHANGE_USER: u8 = 0x11;
    pub const COM_BINLOG_DUMP: u8 = 0x12;
    pub const COM_TABLE_DUMP: u8 = 0x13;
    pub const COM_CONNECT_OUT: u8 = 0x14;
    pub const COM_REGISTER_SLAVE: u8 = 0x15;
    pub const COM_STMT_PREPARE: u8 = 0x16;
    pub const COM_STMT_EXECUTE: u8 = 0x17;
    pub const COM_STMT_SEND_LONG_DATA: u8 = 0x18;
    pub const COM_STMT_CLOSE: u8 = 0x19;
    pub const COM_STMT_RESET: u8 = 0x1A;
    pub const COM_SET_OPTION: u8 = 0x1B;
    pub const COM_STMT_FETCH: u8 = 0x1C;
    pub const COM_DAEMON: u8 = 0x1D;
    pub const COM_BINLOG_DUMP_GTID: u8 = 0x1E;
    pub const COM_RESET_CONNECTION: u8 = 0x1F;
}

// ---------------------------------------------------------------------------
// Packet header helpers
// ---------------------------------------------------------------------------

/// Write a MySQL packet header: 3-byte length + 1-byte sequence id.
pub fn write_packet_header(buf: &mut BytesMut, length: usize, seq_id: u8) {
    buf.put_u8((length & 0xFF) as u8);
    buf.put_u8(((length >> 8) & 0xFF) as u8);
    buf.put_u8(((length >> 16) & 0xFF) as u8);
    buf.put_u8(seq_id);
}

/// Read a MySQL packet header. Returns (payload_length, sequence_id).
pub fn read_packet_header(buf: &[u8]) -> Option<(usize, u8)> {
    if buf.len() < 4 {
        return None;
    }
    let length = (buf[0] as usize) | ((buf[1] as usize) << 8) | ((buf[2] as usize) << 16);
    let seq_id = buf[3];
    Some((length, seq_id))
}

// ---------------------------------------------------------------------------
// Packet builder (accumulates payload, wraps with header on finish)
// ---------------------------------------------------------------------------

pub struct PacketBuilder {
    payload: BytesMut,
    seq_id: u8,
}

impl PacketBuilder {
    pub fn new(seq_id: u8) -> Self {
        Self {
            payload: BytesMut::with_capacity(256),
            seq_id,
        }
    }

    pub fn put_u8(&mut self, v: u8) -> &mut Self {
        self.payload.put_u8(v);
        self
    }

    pub fn put_u16_le(&mut self, v: u16) -> &mut Self {
        self.payload.put_u16_le(v);
        self
    }

    pub fn put_u32_le(&mut self, v: u32) -> &mut Self {
        self.payload.put_u32_le(v);
        self
    }

    pub fn put_u64_le(&mut self, v: u64) -> &mut Self {
        self.payload.put_u64_le(v);
        self
    }

    pub fn put_slice(&mut self, s: &[u8]) -> &mut Self {
        self.payload.put_slice(s);
        self
    }

    pub fn lenenc_int(&mut self, n: u64) -> &mut Self {
        encode_lenenc_int(&mut self.payload, n);
        self
    }

    pub fn lenenc_str(&mut self, s: &str) -> &mut Self {
        encode_lenenc_str(&mut self.payload, s);
        self
    }

    pub fn lenenc_bytes(&mut self, b: &[u8]) -> &mut Self {
        encode_lenenc_int(&mut self.payload, b.len() as u64);
        self.payload.put_slice(b);
        self
    }

    /// Build the final packet with header prepended.
    /// Returns the encoded packet and the next sequence id.
    pub fn finish(self) -> (BytesMut, u8) {
        let len = self.payload.len();
        let mut buf = BytesMut::with_capacity(4 + len);
        write_packet_header(&mut buf, len, self.seq_id);
        buf.put(self.payload);
        (buf, self.seq_id.wrapping_add(1))
    }

    /// Write the packet directly into an existing buffer, avoiding an extra allocation.
    /// Returns the next sequence id.
    pub fn write_to(self, buf: &mut BytesMut) -> u8 {
        let len = self.payload.len();
        write_packet_header(buf, len, self.seq_id);
        buf.put(self.payload);
        self.seq_id.wrapping_add(1)
    }
}

// ---------------------------------------------------------------------------
// Server handshake v10 packet
// ---------------------------------------------------------------------------

pub struct HandshakeV10 {
    pub server_version: String,
    pub connection_id: u32,
    pub auth_salt: [u8; 20],
    pub capability_flags: u32,
    pub charset: u8,
    pub status_flags: u16,
    pub auth_plugin_name: &'static [u8],
}

impl HandshakeV10 {
    pub fn new(connection_id: u32, auth_salt: [u8; 20]) -> Self {
        Self {
            server_version: "8.0.33-HarnessDB".to_string(),
            connection_id,
            auth_salt,
            capability_flags: DEFAULT_CAPABILITIES,
            charset: DEFAULT_CHARSET,
            status_flags: SERVER_STATUS_AUTOCOMMIT,
            auth_plugin_name: AUTH_PLUGIN_NAME,
        }
    }

    /// Encode into a single packet with the given sequence id.
    pub fn encode(&self, seq_id: u8) -> BytesMut {
        let mut pb = PacketBuilder::new(seq_id);

        // Protocol version
        pb.put_u8(10); // protocol v10

        // Server version (null-terminated)
        pb.put_slice(self.server_version.as_bytes());
        pb.put_u8(0);

        // Connection ID (thread id)
        pb.put_u32_le(self.connection_id);

        // Auth salt part 1 (first 8 bytes)
        pb.put_slice(&self.auth_salt[..8]);

        // Filler
        pb.put_u8(0);

        // Capability flags lower 16 bits
        pb.put_u16_le((self.capability_flags & 0xFFFF) as u16);

        // Character set
        pb.put_u8(self.charset);

        // Status flags
        pb.put_u16_le(self.status_flags);

        // Capability flags upper 16 bits
        pb.put_u16_le(((self.capability_flags >> 16) & 0xFFFF) as u16);

        // Length of auth plugin data (always 21 for mysql_native_password: 20 bytes + null)
        if (self.capability_flags & CapabilityFlags::SECURE_CONNECTION) != 0 {
            pb.put_u8(21);
        } else {
            pb.put_u8(0);
        }

        // Reserved 10 bytes of zeroes
        pb.put_slice(&[0u8; 10]);

        // Auth salt part 2 (remaining 12 bytes)
        if (self.capability_flags & CapabilityFlags::SECURE_CONNECTION) != 0 {
            pb.put_slice(&self.auth_salt[8..]);
            pb.put_u8(0);
        }

        // Auth plugin name (null-terminated)
        if (self.capability_flags & CapabilityFlags::PLUGIN_AUTH) != 0 {
            pb.put_slice(self.auth_plugin_name);
            pb.put_u8(0);
        }

        let (packet, _) = pb.finish();
        packet
    }
}

// ---------------------------------------------------------------------------
// Handshake response (parsed from client)
// ---------------------------------------------------------------------------

pub struct HandshakeResponse {
    pub capability_flags: u32,
    pub max_packet_size: u32,
    pub charset: u8,
    pub username: String,
    pub auth_response: Vec<u8>,
    pub database: Option<String>,
    pub auth_plugin_name: Option<String>,
}

impl HandshakeResponse {
    /// Parse a handshake response from raw packet payload (after header).
    pub fn parse(payload: &[u8]) -> Result<Self, String> {
        // MySQL HandshakeResponse41 format:
        // offset 0-3:   capability_flags (4 bytes LE)
        // offset 4-7:   max_packet_size (4 bytes LE)
        // offset 8:     charset (1 byte)
        // offset 9-31:  23 bytes reserved
        // offset 32+:  username (null-terminated string)
        // after username: auth_response (length-encoded or null-terminated)
        // after auth_response: database name (null-terminated, if CONNECT_WITH_DB)
        // after database: auth_plugin_name (null-terminated, if PLUGIN_AUTH)

        if payload.len() < 33 {
            return Err("handshake response too short".to_string());
        }

        let capability_flags = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let max_packet_size = u32::from_le_bytes([payload[4], payload[5], payload[6], payload[7]]);
        let charset = payload[8];

        // Username starts at offset 32
        let username_start = 32;
        let username_end = payload[username_start..]
            .iter()
            .position(|&b| b == 0)
            .ok_or("username not null-terminated")?
            + username_start;
        let username = String::from_utf8_lossy(&payload[username_start..username_end]).to_string();

        // Auth response - starts right after username null terminator
        let auth_start = username_end + 1;
        let (auth_response, auth_total_len) =
            if (capability_flags & CapabilityFlags::PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0 {
                // Length-encoded auth response
                let (len, n) = read_lenenc_int(&payload[auth_start..])?;
                let data = payload[auth_start + n..auth_start + n + len].to_vec();
                (data, n + len)
            } else if (capability_flags & CapabilityFlags::SECURE_CONNECTION) != 0 {
                let len = payload[auth_start] as usize;
                let data = payload[auth_start + 1..auth_start + 1 + len].to_vec();
                (data, 1 + len)
            } else {
                // Null-terminated
                let end = payload[auth_start..]
                    .iter()
                    .position(|&b| b == 0)
                    .unwrap_or(payload.len() - auth_start);
                let data = payload[auth_start..auth_start + end].to_vec();
                (data, end + 1) // +1 for null terminator
            };

        // Database - starts after auth response
        let db_start = auth_start + auth_total_len;
        let database = if (capability_flags & CapabilityFlags::CONNECT_WITH_DB) != 0
            && db_start < payload.len()
        {
            let end = payload[db_start..]
                .iter()
                .position(|&b| b == 0)
                .unwrap_or(payload.len() - db_start);
            let db = String::from_utf8_lossy(&payload[db_start..db_start + end]).to_string();
            Some(db)
        } else {
            None
        };

        // Auth plugin name - starts after database (if present)
        let plugin_start = if let Some(db) = &database {
            db_start + db.len() + 1
        } else {
            db_start
        };
        let auth_plugin_name = if (capability_flags & CapabilityFlags::PLUGIN_AUTH) != 0
            && plugin_start < payload.len()
        {
            let end = payload[plugin_start..]
                .iter()
                .position(|&b| b == 0)
                .unwrap_or(payload.len() - plugin_start);
            let name =
                String::from_utf8_lossy(&payload[plugin_start..plugin_start + end]).to_string();
            Some(name)
        } else {
            None
        };

        Ok(Self {
            capability_flags,
            max_packet_size,
            charset,
            username,
            auth_response,
            database,
            auth_plugin_name,
        })
    }
}

/// Read a length-encoded integer from a byte slice.
/// Returns (value, bytes_consumed).
fn read_lenenc_int(buf: &[u8]) -> Result<(usize, usize), String> {
    if buf.is_empty() {
        return Err("buffer empty".to_string());
    }
    match buf[0] {
        0..=0xFA => Ok((buf[0] as usize, 1)),
        0xFC => {
            if buf.len() < 3 {
                return Err("buffer too short for lenenc int FC".to_string());
            }
            let v = u16::from_le_bytes([buf[1], buf[2]]) as usize;
            Ok((v, 3))
        }
        0xFD => {
            if buf.len() < 4 {
                return Err("buffer too short for lenenc int FD".to_string());
            }
            let v = (buf[1] as usize) | ((buf[2] as usize) << 8) | ((buf[3] as usize) << 16);
            Ok((v, 4))
        }
        0xFE => {
            if buf.len() < 9 {
                return Err("buffer too short for lenenc int FE".to_string());
            }
            let v = u64::from_le_bytes([
                buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8],
            ]);
            Ok((v as usize, 9))
        }
        _ => Err(format!("invalid lenenc int prefix: {:#x}", buf[0])),
    }
}

// ---------------------------------------------------------------------------
// OK packet
// ---------------------------------------------------------------------------

/// Build an OK response packet.
pub fn make_ok_packet(
    seq_id: u8,
    affected_rows: u64,
    last_insert_id: u64,
    status_flags: u16,
    warning_count: u16,
) -> BytesMut {
    let mut pb = PacketBuilder::new(seq_id);
    pb.put_u8(0x00); // OK header byte
    pb.lenenc_int(affected_rows);
    pb.lenenc_int(last_insert_id);
    pb.put_u16_le(status_flags);
    pb.put_u16_le(warning_count);
    let (packet, _) = pb.finish();
    packet
}

// ---------------------------------------------------------------------------
// ERR packet
// ---------------------------------------------------------------------------

/// Build an ERR response packet.
pub fn make_err_packet(
    seq_id: u8,
    error_code: u16,
    sql_state: &[u8; 5],
    message: &str,
) -> BytesMut {
    let mut pb = PacketBuilder::new(seq_id);
    pb.put_u8(0xFF); // ERR header byte
    pb.put_u16_le(error_code);
    pb.put_u8(b'#'); // SQL state marker
    pb.put_slice(sql_state);
    pb.put_slice(message.as_bytes());
    let (packet, _) = pb.finish();
    packet
}

/// Convenience: build an ERR packet with a HY000 general error.
pub fn make_general_err(seq_id: u8, error_code: u16, message: &str) -> BytesMut {
    make_err_packet(seq_id, error_code, b"HY000", message)
}

// ---------------------------------------------------------------------------
// EOF packet
// ---------------------------------------------------------------------------

/// Build an EOF packet (used when CLIENT_DEPRECATE_EOF is NOT set).
pub fn make_eof_packet(seq_id: u8, warning_count: u16, status_flags: u16) -> BytesMut {
    let mut pb = PacketBuilder::new(seq_id);
    pb.put_u8(0xFE); // EOF header byte
    pb.put_u16_le(warning_count);
    pb.put_u16_le(status_flags);
    let (packet, _) = pb.finish();
    packet
}

/// Build the result-set terminator when CLIENT_DEPRECATE_EOF IS set.
/// This is an OK-style packet but with 0xFE header byte (not 0x00).
pub fn make_result_set_eof_ok_packet(
    seq_id: u8,
    affected_rows: u64,
    last_insert_id: u64,
    status_flags: u16,
    warning_count: u16,
) -> BytesMut {
    let mut pb = PacketBuilder::new(seq_id);
    pb.put_u8(0xFE); // EOF/OK header byte (distinguished from old 5-byte EOF by length)
    pb.lenenc_int(affected_rows);
    pb.lenenc_int(last_insert_id);
    pb.put_u16_le(status_flags);
    pb.put_u16_le(warning_count);
    let (packet, _) = pb.finish();
    packet
}

// ---------------------------------------------------------------------------
// Column definition packet (COM_QUERY response column)
// ---------------------------------------------------------------------------

/// A column descriptor used in result set responses.
#[derive(Debug, Clone)]
pub struct Column {
    pub schema: String,
    pub table: String,
    pub org_table: String,
    pub name: String,
    pub org_name: String,
    pub charset: u8,
    pub column_length: u32,
    pub column_type: u8,
    pub flags: u16,
    pub decimals: u8,
}

impl Column {
    pub fn new(name: &str, column_type: u8) -> Self {
        Self {
            schema: "def".to_string(),
            table: String::new(),
            org_table: String::new(),
            name: name.to_string(),
            org_name: name.to_string(),
            charset: charset::DEFAULT_CHARSET,
            column_length: 0,
            column_type,
            flags: 0,
            decimals: 0,
        }
    }

    pub fn with_schema(mut self, schema: &str) -> Self {
        self.schema = schema.to_string();
        self
    }

    pub fn with_table(mut self, table: &str) -> Self {
        self.table = table.to_string();
        self.org_table = table.to_string();
        self
    }

    pub fn with_charset(mut self, charset: u8) -> Self {
        self.charset = charset;
        self
    }

    pub fn with_length(mut self, len: u32) -> Self {
        self.column_length = len;
        self
    }

    pub fn with_flags(mut self, flags: u16) -> Self {
        self.flags = flags;
        self
    }

    pub fn with_decimals(mut self, decimals: u8) -> Self {
        self.decimals = decimals;
        self
    }

    /// Encode this column definition as a MySQL packet.
    pub fn encode(&self, seq_id: u8) -> BytesMut {
        let mut pb = PacketBuilder::new(seq_id);

        // For Protocol::ColumnDefinition41, we use lenenc encoding
        pb.lenenc_str("def"); // catalog (always "def")
        pb.lenenc_str(&self.schema);
        pb.lenenc_str(&self.table);
        pb.lenenc_str(&self.org_table);
        pb.lenenc_str(&self.name);
        pb.lenenc_str(&self.org_name);
        pb.lenenc_int(0x0C); // length of fixed-length fields (always 0x0C)
        pb.put_u16_le(self.charset as u16);
        pb.put_u32_le(self.column_length);
        pb.put_u8(self.column_type);
        pb.put_u16_le(self.flags);
        pb.put_u8(self.decimals);
        pb.put_u16_le(0x0000); // filler

        let (packet, _) = pb.finish();
        packet
    }
}

// ---------------------------------------------------------------------------
// Column type mapping from ScalarValue
// ---------------------------------------------------------------------------

/// Map a ScalarValue to a MySQL column type.
pub fn scalar_to_column_type(val: &ScalarValue) -> u8 {
    match val {
        ScalarValue::Null => column_type::NULL,
        ScalarValue::Boolean(_) => column_type::TINY,
        ScalarValue::Int8(_) => column_type::TINY,
        ScalarValue::Int16(_) => column_type::SHORT,
        ScalarValue::Int32(_) => column_type::LONG,
        ScalarValue::Int64(_) => column_type::LONGLONG,
        ScalarValue::Int128(_) => column_type::NEWDECIMAL,
        ScalarValue::Float32(_) => column_type::FLOAT,
        ScalarValue::Float64(_) => column_type::DOUBLE,
        ScalarValue::Date(_) => column_type::DATE,
        ScalarValue::DateTime(_) => column_type::DATETIME,
        ScalarValue::String(_) => column_type::VAR_STRING,
        ScalarValue::Binary(_) => column_type::BLOB,
        ScalarValue::Array(_) => column_type::BLOB,
        ScalarValue::Json(_) => column_type::VAR_STRING,
        ScalarValue::Float32Array(_) => column_type::BLOB,
    }
}

/// Map a DataType to a MySQL column type.
pub fn data_type_to_column_type(dt: &types::DataType) -> u8 {
    match dt {
        types::DataType::Null => column_type::NULL,
        types::DataType::Boolean => column_type::TINY,
        types::DataType::Int8 => column_type::TINY,
        types::DataType::Int16 => column_type::SHORT,
        types::DataType::Int32 => column_type::LONG,
        types::DataType::Int64 => column_type::LONGLONG,
        types::DataType::Int128 => column_type::NEWDECIMAL,
        types::DataType::Float32 => column_type::FLOAT,
        types::DataType::Float64 => column_type::DOUBLE,
        types::DataType::Decimal(_) => column_type::NEWDECIMAL,
        types::DataType::Date => column_type::DATE,
        types::DataType::DateTime => column_type::DATETIME,
        types::DataType::Varchar(_) | types::DataType::Char(_) | types::DataType::String => {
            column_type::VAR_STRING
        }
        types::DataType::Binary => column_type::BLOB,
        types::DataType::Json => column_type::VAR_STRING,
        _ => column_type::BLOB,
    }
}

// ---------------------------------------------------------------------------
// Row encoding (text protocol for COM_QUERY)
// ---------------------------------------------------------------------------

/// Encode a single text-protocol row. Each value is a length-encoded string (or 0xFB for NULL).
pub fn encode_text_row(seq_id: u8, values: &[Option<Vec<u8>>]) -> BytesMut {
    let mut pb = PacketBuilder::new(seq_id);
    for val in values {
        match val {
            Some(data) => {
                pb.lenenc_bytes(data);
            }
            None => {
                pb.put_u8(0xFB); // NULL
            }
        }
    }
    let (packet, _) = pb.finish();
    packet
}

/// Encode a text-protocol row directly into an existing buffer.
/// Single-pass: writes payload first, then inserts header at the beginning.
/// Returns next seq_id.
pub fn encode_text_row_into(seq_id: u8, values: &[Option<&[u8]>], buf: &mut BytesMut) -> u8 {
    // Remember where this packet starts
    let packet_start = buf.len();

    // Reserve space for 4-byte header (will fill in later)
    buf.reserve(4 + values.len() * 32); // rough estimate
    buf.extend_from_slice(&[0, 0, 0, seq_id]);

    // Write payload directly
    for val in values {
        match val {
            Some(data) => {
                encode_lenenc_int(buf, data.len() as u64);
                buf.put_slice(data);
            }
            None => {
                buf.put_u8(0xFB);
            }
        }
    }

    // Calculate payload length and fill in header
    let payload_len = buf.len() - packet_start - 4;
    buf[packet_start] = (payload_len & 0xFF) as u8;
    buf[packet_start + 1] = ((payload_len >> 8) & 0xFF) as u8;
    buf[packet_start + 2] = ((payload_len >> 16) & 0xFF) as u8;

    seq_id.wrapping_add(1)
}

/// Encode a text-protocol row from String slice directly into buffer.
/// Avoids intermediate conversion to byte slices.
/// Returns next seq_id.
pub fn encode_text_row_strings_into(seq_id: u8, values: &[Option<String>], buf: &mut BytesMut) -> u8 {
    // Remember where this packet starts
    let packet_start = buf.len();

    // Reserve space for 4-byte header + rough estimate of payload
    buf.reserve(4 + values.len() * 32);
    buf.extend_from_slice(&[0, 0, 0, seq_id]);

    // Write payload directly from String refs
    for val in values {
        match val {
            Some(s) => {
                let data = s.as_bytes();
                encode_lenenc_int(buf, data.len() as u64);
                buf.put_slice(data);
            }
            None => {
                buf.put_u8(0xFB);
            }
        }
    }

    // Calculate payload length and fill in header
    let payload_len = buf.len() - packet_start - 4;
    buf[packet_start] = (payload_len & 0xFF) as u8;
    buf[packet_start + 1] = ((payload_len >> 8) & 0xFF) as u8;
    buf[packet_start + 2] = ((payload_len >> 16) & 0xFF) as u8;

    seq_id.wrapping_add(1)
}

/// Encode a ScalarValue to text protocol bytes.
pub fn scalar_to_text_bytes(val: &ScalarValue) -> Option<Vec<u8>> {
    match val {
        ScalarValue::Null => None,
        ScalarValue::Boolean(b) => Some(if *b { b"1".to_vec() } else { b"0".to_vec() }),
        ScalarValue::Int8(n) => Some(n.to_string().into_bytes()),
        ScalarValue::Int16(n) => Some(n.to_string().into_bytes()),
        ScalarValue::Int32(n) => Some(n.to_string().into_bytes()),
        ScalarValue::Int64(n) => Some(n.to_string().into_bytes()),
        ScalarValue::Int128(n) => Some(n.to_string().into_bytes()),
        ScalarValue::Float32(f) => Some(format!("{f:.prec$}", f = f, prec = 6).into_bytes()),
        ScalarValue::Float64(f) => Some({
            let s = format!("{f}");
            s.into_bytes()
        }),
        ScalarValue::Date(days) => {
            let base = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let date = base + chrono::Duration::days(*days as i64);
            Some(date.format("%Y-%m-%d").to_string().into_bytes())
        }
        ScalarValue::DateTime(micros) => {
            let secs = *micros / 1_000_000;
            let nsecs = ((*micros % 1_000_000).abs() * 1000) as u32;
            match chrono::DateTime::from_timestamp(secs, nsecs) {
                Some(dt) => Some(dt.format("%Y-%m-%d %H:%M:%S").to_string().into_bytes()),
                None => Some(b"1970-01-01 00:00:00".to_vec()),
            }
        }
        ScalarValue::String(s) => Some(s.clone().into_bytes()),
        ScalarValue::Binary(b) => Some(b.clone()),
        ScalarValue::Array(_) => Some(b"[]".to_vec()),
        ScalarValue::Json(j) => Some(
            serde_json::to_string(j)
                .unwrap_or_else(|_| "null".to_string())
                .into_bytes(),
        ),
        ScalarValue::Float32Array(arr) => {
            let items: Vec<String> = arr.iter().map(|f| f.to_string()).collect();
            Some(format!("[{}]", items.join(",")).into_bytes())
        }
    }
}

// ---------------------------------------------------------------------------
// Statement (prepared statement) helpers
// ---------------------------------------------------------------------------

/// Build a COM_STMT_PREPARE response.
pub fn make_stmt_prepare_ok(
    seq_id: u8,
    stmt_id: u32,
    num_columns: u16,
    num_params: u16,
    warning_count: u16,
) -> BytesMut {
    let mut pb = PacketBuilder::new(seq_id);
    pb.put_u8(0x00); // OK header for prepared statement
    pb.put_u32_le(stmt_id);
    pb.put_u16_le(num_columns);
    pb.put_u16_le(num_params);
    pb.put_u8(0x00); // filler
    pb.put_u16_le(warning_count);
    let (packet, _) = pb.finish();
    packet
}

// ---------------------------------------------------------------------------
// Auth switch request
// ---------------------------------------------------------------------------

/// Build an auth switch request packet (if we need to switch auth plugins).
pub fn make_auth_switch_request(seq_id: u8, plugin_name: &[u8], plugin_data: &[u8]) -> BytesMut {
    let mut pb = PacketBuilder::new(seq_id);
    pb.put_u8(0xFE); // status byte for auth switch
    pb.put_slice(plugin_name);
    pb.put_u8(0); // null terminator
    pb.put_slice(plugin_data);
    let (packet, _) = pb.finish();
    packet
}

// ---------------------------------------------------------------------------
// Binary protocol row encoding (for COM_STMT_EXECUTE)
// ---------------------------------------------------------------------------

/// Encode a single binary-protocol row.
/// Format: 0x00 header + NULL bitmap + values
/// NULL bitmap: bits 0-1 are padding, then each column has one bit
/// Values are encoded in binary format based on column type.
pub fn encode_binary_row(seq_id: u8, values: &[Option<BinaryValue>], num_columns: u16) -> BytesMut {
    let mut pb = PacketBuilder::new(seq_id);

    // Header byte (0x00 for binary row)
    pb.put_u8(0x00);

    // NULL bitmap: (num_columns + 7 + 2) / 8 bytes
    // Bits 0-1 are padding, bit 2 = column 0, bit 3 = column 1, etc.
    let null_bitmap_size = ((num_columns as usize + 9) / 8).max(1);
    let mut null_bitmap = vec![0u8; null_bitmap_size];

    for (i, val) in values.iter().enumerate() {
        if val.is_none() {
            // Set bit at position (i + 2)
            let bit_pos = i + 2;
            null_bitmap[bit_pos / 8] |= 1 << (bit_pos % 8);
        }
    }
    pb.put_slice(&null_bitmap);

    // Encode non-NULL values
    for val in values.iter() {
        if let Some(v) = val {
            encode_binary_value(&mut pb, v);
        }
    }

    let (packet, _) = pb.finish();
    packet
}

/// Encode a single binary-protocol row directly into an existing buffer.
/// Zero-alloc: writes packet header + payload directly into `buf`.
/// Returns next seq_id.
pub fn encode_binary_row_into(
    seq_id: u8,
    values: &[Option<BinaryValue>],
    num_columns: u16,
    buf: &mut BytesMut,
) -> u8 {
    let packet_start = buf.len();

    // Reserve space for 4-byte header + estimated payload
    buf.reserve(4 + num_columns as usize * 20 + 10);

    // 4-byte packet header (payload length filled in later)
    buf.extend_from_slice(&[0, 0, 0, seq_id]);

    // Header byte (0x00 for binary row)
    buf.put_u8(0x00);

    // NULL bitmap: (num_columns + 7 + 2) / 8 bytes
    let null_bitmap_size = ((num_columns as usize + 9) / 8).max(1);
    let bitmap_start = buf.len();
    buf.extend_from_slice(&vec![0u8; null_bitmap_size]);

    // First pass: mark NULL positions in bitmap
    for (i, val) in values.iter().enumerate() {
        if val.is_none() {
            let bit_pos = i + 2;
            buf[bitmap_start + bit_pos / 8] |= 1 << (bit_pos % 8);
        }
    }

    // Second pass: encode non-NULL values directly into buf
    for val in values.iter() {
        if let Some(v) = val {
            encode_binary_value_into(buf, v);
        }
    }

    // Fill in packet header with payload length
    let payload_len = buf.len() - packet_start - 4;
    buf[packet_start] = (payload_len & 0xFF) as u8;
    buf[packet_start + 1] = ((payload_len >> 8) & 0xFF) as u8;
    buf[packet_start + 2] = ((payload_len >> 16) & 0xFF) as u8;

    seq_id.wrapping_add(1)
}

/// Encode a binary value directly into a BytesMut buffer (zero-alloc variant).
fn encode_binary_value_into(buf: &mut BytesMut, val: &BinaryValue) {
    match val {
        BinaryValue::Int8(n) => buf.put_i8(*n),
        BinaryValue::UInt8(n) => buf.put_u8(*n),
        BinaryValue::Int16(n) => buf.put_i16_le(*n),
        BinaryValue::UInt16(n) => buf.put_u16_le(*n),
        BinaryValue::Int32(n) => buf.put_i32_le(*n),
        BinaryValue::UInt32(n) => buf.put_u32_le(*n),
        BinaryValue::Int64(n) => buf.put_i64_le(*n),
        BinaryValue::UInt64(n) => buf.put_u64_le(*n),
        BinaryValue::Float(f) => buf.put_slice(&f.to_le_bytes()),
        BinaryValue::Double(d) => buf.put_slice(&d.to_le_bytes()),
        BinaryValue::String(s) => {
            encode_lenenc_int(buf, s.len() as u64);
            buf.put_slice(s);
        }
        BinaryValue::Date { year, month, day } => {
            buf.put_u8(4); // length
            buf.put_u16_le(*year);
            buf.put_u8(*month);
            buf.put_u8(*day);
        }
        BinaryValue::DateTime {
            year, month, day,
            hour, minute, second, micros,
        } => {
            if *micros == 0 {
                buf.put_u8(7); // length without micros
                buf.put_u16_le(*year);
                buf.put_u8(*month);
                buf.put_u8(*day);
                buf.put_u8(*hour);
                buf.put_u8(*minute);
                buf.put_u8(*second);
            } else {
                buf.put_u8(11); // length with micros
                buf.put_u16_le(*year);
                buf.put_u8(*month);
                buf.put_u8(*day);
                buf.put_u8(*hour);
                buf.put_u8(*minute);
                buf.put_u8(*second);
                buf.put_u32_le(*micros);
            }
        }
        BinaryValue::Time {
            is_neg, days, hours, minutes, seconds, micros,
        } => {
            if *micros == 0 {
                buf.put_u8(8); // length
                buf.put_u8(if *is_neg { 1 } else { 0 });
                buf.put_u32_le(*days);
                buf.put_u8(*hours);
                buf.put_u8(*minutes);
                buf.put_u8(*seconds);
            } else {
                buf.put_u8(12); // length with micros
                buf.put_u8(if *is_neg { 1 } else { 0 });
                buf.put_u32_le(*days);
                buf.put_u8(*hours);
                buf.put_u8(*minutes);
                buf.put_u8(*seconds);
                buf.put_u32_le(*micros);
            }
        }
    }
}

/// Binary value representation for MySQL binary protocol.
#[derive(Debug, Clone)]
pub enum BinaryValue {
    /// Signed 8-bit integer (MYSQL_TYPE_TINY)
    Int8(i8),
    /// Unsigned 8-bit integer
    UInt8(u8),
    /// Signed 16-bit integer (MYSQL_TYPE_SHORT)
    Int16(i16),
    /// Unsigned 16-bit integer
    UInt16(u16),
    /// Signed 32-bit integer (MYSQL_TYPE_LONG)
    Int32(i32),
    /// Unsigned 32-bit integer
    UInt32(u32),
    /// Signed 64-bit integer (MYSQL_TYPE_LONGLONG)
    Int64(i64),
    /// Unsigned 64-bit integer
    UInt64(u64),
    /// 32-bit float (MYSQL_TYPE_FLOAT)
    Float(f32),
    /// 64-bit double (MYSQL_TYPE_DOUBLE)
    Double(f64),
    /// String/binary (MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_BLOB)
    String(Vec<u8>),
    /// Date (MYSQL_TYPE_DATE)
    Date { year: u16, month: u8, day: u8 },
    /// DateTime (MYSQL_TYPE_DATETIME)
    DateTime {
        year: u16,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        micros: u32,
    },
    /// Time (MYSQL_TYPE_TIME)
    Time {
        is_neg: bool,
        days: u32,
        hours: u8,
        minutes: u8,
        seconds: u8,
        micros: u32,
    },
}

/// Encode a binary value into the packet builder.
fn encode_binary_value(pb: &mut PacketBuilder, val: &BinaryValue) {
    match val {
        BinaryValue::Int8(n) => {
            pb.put_u8(*n as u8);
        }
        BinaryValue::UInt8(n) => {
            pb.put_u8(*n);
        }
        BinaryValue::Int16(n) => {
            pb.put_u16_le(*n as u16);
        }
        BinaryValue::UInt16(n) => {
            pb.put_u16_le(*n);
        }
        BinaryValue::Int32(n) => {
            pb.put_u32_le(*n as u32);
        }
        BinaryValue::UInt32(n) => {
            pb.put_u32_le(*n);
        }
        BinaryValue::Int64(n) => {
            pb.put_u64_le(*n as u64);
        }
        BinaryValue::UInt64(n) => {
            pb.put_u64_le(*n);
        }
        BinaryValue::Float(f) => {
            pb.put_slice(&f.to_le_bytes());
        }
        BinaryValue::Double(d) => {
            pb.put_slice(&d.to_le_bytes());
        }
        BinaryValue::String(s) => {
            pb.lenenc_bytes(s);
        }
        BinaryValue::Date { year, month, day } => {
            pb.put_u8(4); // length
            pb.put_u16_le(*year);
            pb.put_u8(*month);
            pb.put_u8(*day);
        }
        BinaryValue::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            micros,
        } => {
            if *micros == 0 {
                pb.put_u8(7); // length without micros
                pb.put_u16_le(*year);
                pb.put_u8(*month);
                pb.put_u8(*day);
                pb.put_u8(*hour);
                pb.put_u8(*minute);
                pb.put_u8(*second);
            } else {
                pb.put_u8(11); // length with micros
                pb.put_u16_le(*year);
                pb.put_u8(*month);
                pb.put_u8(*day);
                pb.put_u8(*hour);
                pb.put_u8(*minute);
                pb.put_u8(*second);
                pb.put_u32_le(*micros);
            }
        }
        BinaryValue::Time {
            is_neg,
            days,
            hours,
            minutes,
            seconds,
            micros,
        } => {
            if *micros == 0 {
                pb.put_u8(8); // length without micros
                pb.put_u8(if *is_neg { 1 } else { 0 });
                pb.put_u32_le(*days);
                pb.put_u8(*hours);
                pb.put_u8(*minutes);
                pb.put_u8(*seconds);
            } else {
                pb.put_u8(12); // length with micros
                pb.put_u8(if *is_neg { 1 } else { 0 });
                pb.put_u32_le(*days);
                pb.put_u8(*hours);
                pb.put_u8(*minutes);
                pb.put_u8(*seconds);
                pb.put_u32_le(*micros);
            }
        }
    }
}

/// Convert a string value to BinaryValue based on column type.
pub fn text_to_binary(text: Option<&str>, col_type: u8) -> Option<BinaryValue> {
    let text = text?;
    match col_type {
        column_type::TINY => text.parse::<i8>().ok().map(|n| BinaryValue::Int8(n)),
        column_type::SHORT => text.parse::<i16>().ok().map(|n| BinaryValue::Int16(n)),
        column_type::LONG => text.parse::<i32>().ok().map(|n| BinaryValue::Int32(n)),
        column_type::LONGLONG => text.parse::<i64>().ok().map(|n| BinaryValue::Int64(n)),
        column_type::FLOAT => text.parse::<f64>().ok().map(|f| BinaryValue::Double(f)),
        column_type::DOUBLE => text.parse::<f64>().ok().map(|f| BinaryValue::Double(f)),
        column_type::VAR_STRING | column_type::VARCHAR | column_type::BLOB => {
            Some(BinaryValue::String(text.as_bytes().to_vec()))
        }
        column_type::DATE => {
            // Parse YYYY-MM-DD
            let parts: Vec<&str> = text.split('-').collect();
            if parts.len() == 3 {
                Some(BinaryValue::Date {
                    year: parts[0].parse().ok()?,
                    month: parts[1].parse().ok()?,
                    day: parts[2].parse().ok()?,
                })
            } else {
                Some(BinaryValue::String(text.as_bytes().to_vec()))
            }
        }
        column_type::DATETIME => {
            // Parse YYYY-MM-DD HH:MM:SS
            let parts: Vec<&str> = text.split_whitespace().collect();
            if parts.len() >= 2 {
                let date_parts: Vec<&str> = parts[0].split('-').collect();
                let time_parts: Vec<&str> = parts[1].split(':').collect();
                if date_parts.len() == 3 && time_parts.len() >= 3 {
                    let (sec, micros) = if let Some((s, f)) = time_parts[2].split_once('.') {
                        let sec: u8 = s.parse().ok()?;
                        let frac: u32 = f.parse().ok()?;
                        let micros = frac * 10u32.pow(6 - f.len() as u32);
                        (sec, micros)
                    } else {
                        (time_parts[2].parse().ok()?, 0u32)
                    };
                    Some(BinaryValue::DateTime {
                        year: date_parts[0].parse().ok()?,
                        month: date_parts[1].parse().ok()?,
                        day: date_parts[2].parse().ok()?,
                        hour: time_parts[0].parse().ok()?,
                        minute: time_parts[1].parse().ok()?,
                        second: sec,
                        micros,
                    })
                } else {
                    Some(BinaryValue::String(text.as_bytes().to_vec()))
                }
            } else {
                Some(BinaryValue::String(text.as_bytes().to_vec()))
            }
        }
        _ => {
            // Default: treat as string
            Some(BinaryValue::String(text.as_bytes().to_vec()))
        }
    }
}
