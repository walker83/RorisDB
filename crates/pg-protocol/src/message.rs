//! PostgreSQL wire protocol v3 message encoding and decoding.
//!
//! This module implements the PostgreSQL wire protocol v3 message format.
//! All integers are in network byte order (big-endian).
//! All strings are null-terminated in text format.
//!
//! # Protocol Overview
//!
//! ## Frontend Messages (client → server)
//! - StartupMessage: no type byte, starts with length(4) + version(4) + key=value pairs
//! - Query: type 'Q', length, SQL string
//! - Parse: type 'P', length, statement name, query, param types
//! - Bind: type 'B', length, portal, statement, formats, values, result formats
//! - Describe: type 'D', length, target ('S'/'P'), name
//! - Execute: type 'E', length, portal, max_rows
//! - Close: type 'C', length, target, name
//! - Sync: type 'S', length
//! - Terminate: type 'X', length
//! - PasswordMessage: type 'p', length, password
//!
//! ## Backend Messages (server → client)
//! All have a type byte prefix, length field, and message-specific body.

use bytes::{Buf, BufMut, BytesMut};
use std::collections::HashMap;
use thiserror::Error;

// ============================================================================
// Error Types
// ============================================================================

/// Errors that can occur during PG protocol message processing.
#[derive(Error, Debug)]
pub enum PgProtocolError {
    #[error("unexpected end of data")]
    UnexpectedEof,

    #[error("invalid message type byte: 0x{0:02x}")]
    InvalidMessageType(u8),

    #[error("invalid protocol version: {0}")]
    InvalidVersion(i32),

    #[error("encoding error: {0}")]
    EncodingError(String),

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("protocol violation: {0}")]
    ProtocolViolation(String),

    #[error("connection closed")]
    ConnectionClosed,

    #[error("cancel request received")]
    CancelRequest,

    #[error("authentication failed: {0}")]
    AuthenticationFailed(String),
}

// ============================================================================
// Constants
// ============================================================================

/// PostgreSQL protocol version 3.0 (encoded as 196608 = 3 << 16 | 0).
pub const PG_PROTOCOL_VERSION_3: i32 = 196608;

/// Special version number for SSL request.
pub const SSL_REQUEST_CODE: i32 = 80877102;

/// Special version number for cancel request.
pub const CANCEL_REQUEST_CODE: i32 = 80877103;

// Type OIDs commonly used in PostgreSQL wire protocol
pub const OID_INT2: i32 = 21;
pub const OID_INT4: i32 = 23;
pub const OID_INT8: i32 = 20;
pub const OID_FLOAT4: i32 = 700;
pub const OID_FLOAT8: i32 = 701;
pub const OID_TEXT: i32 = 25;
pub const OID_VARCHAR: i32 = 1043;
pub const OID_BOOL: i32 = 16;
pub const OID_DATE: i32 = 1082;
pub const OID_TIMESTAMP: i32 = 1114;
pub const OID_NUMERIC: i32 = 1700;
pub const OID_BPCHAR: i32 = 1042;
pub const OID_BYTEA: i32 = 17;
pub const OID_OID: i32 = 26;
pub const OID_NAME: i32 = 19;
pub const OID_XID: i32 = 28;
pub const OID_INT4_ARRAY: i32 = 1007;
pub const OID_TEXT_ARRAY: i32 = 1009;
pub const OID_VARCHAR_ARRAY: i32 = 1015;
pub const OID_BPCHAR_ARRAY: i32 = 1014;

// ============================================================================
// Enums
// ============================================================================

/// Target type for Describe and Close messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeTarget {
    Statement,
    Portal,
}

/// Transaction status for ReadyForQuery messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    /// Not in a transaction (idle)
    Idle,
    /// In a transaction block
    InTransaction,
    /// In a failed transaction block
    Failed,
}

impl TransactionStatus {
    /// Convert to the single-byte protocol representation.
    pub fn to_byte(self) -> u8 {
        match self {
            TransactionStatus::Idle => b'I',
            TransactionStatus::InTransaction => b'T',
            TransactionStatus::Failed => b'E',
        }
    }

    /// Convert from the single-byte protocol representation.
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            b'I' => Some(TransactionStatus::Idle),
            b'T' => Some(TransactionStatus::InTransaction),
            b'E' => Some(TransactionStatus::Failed),
            _ => None,
        }
    }
}

// ============================================================================
// Data Structures
// ============================================================================

/// Description of a single field/column in a RowDescription message.
#[derive(Debug, Clone)]
pub struct FieldDescription {
    pub name: String,
    pub table_oid: i32,
    pub column_number: i16,
    pub type_oid: i32,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format_code: i16,
}

impl FieldDescription {
    /// Create a new text-format field description.
    pub fn new(name: &str, type_oid: i32, type_size: i16) -> Self {
        Self {
            name: name.to_string(),
            table_oid: 0,
            column_number: 0,
            type_oid,
            type_size,
            type_modifier: -1,
            format_code: 0, // text format
        }
    }
}

/// Row description with column names and types.
/// Used by RowDescription, DataRow, and CommandComplete messages.
#[derive(Debug, Clone)]
pub struct RowSetDescription {
    pub fields: Vec<FieldDescription>,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
    pub command_tag: String,
}

// ============================================================================
// Frontend Messages (client → server)
// ============================================================================

/// Messages sent from the client (frontend) to the server (backend).
#[derive(Debug, Clone)]
pub enum FrontendMessage {
    /// StartupMessage: no type byte, begins a connection.
    StartupMessage {
        version: i32,
        params: HashMap<String, String>,
    },
    /// Simple Query: 'Q'
    Query { sql: String },
    /// Extended Query Parse: 'P'
    Parse {
        name: String,
        query: String,
        param_types: Vec<u32>,
    },
    /// Extended Query Bind: 'B'
    Bind {
        portal: String,
        statement: String,
        formats: Vec<i16>,
        values: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    },
    /// Describe: 'D'
    Describe {
        target: DescribeTarget,
        name: String,
    },
    /// Execute: 'E'
    Execute { portal: String, max_rows: i32 },
    /// Close: 'C'
    Close {
        target: DescribeTarget,
        name: String,
    },
    /// Sync: 'S'
    Sync,
    /// Terminate: 'X'
    Terminate,
    /// PasswordMessage: 'p'
    PasswordMessage { password: String },
}

impl FrontendMessage {
    /// Decode a StartupMessage from the buffer (no type byte prefix).
    ///
    /// The buffer should contain at least 4 bytes (length).
    /// Returns `None` if the buffer doesn't contain a complete message.
    pub fn decode_startup(buf: &mut BytesMut) -> Result<Option<Self>, PgProtocolError> {
        if buf.len() < 4 {
            return Ok(None);
        }
        let len_i32 = (&buf[..4]).get_i32();
        if len_i32 < 4 {
            return Err(PgProtocolError::UnexpectedEof); // Invalid negative or too-small length
        }
        let len = len_i32 as usize;
        if len > 10 * 1024 * 1024 {
            return Err(PgProtocolError::UnexpectedEof); // Max 10MB startup message
        }
        if buf.len() < len {
            return Ok(None);
        }
        buf.advance(4);

        if len < 8 {
            return Err(PgProtocolError::UnexpectedEof);
        }

        let version = buf.get_i32();
        let mut params = HashMap::new();

        // Read key=value pairs terminated by an empty string (double \0)
        while buf.has_remaining() {
            let key = read_cstring(buf)?;
            if key.is_empty() {
                break;
            }
            let value = read_cstring(buf)?;
            params.insert(key, value);
        }

        Ok(Some(FrontendMessage::StartupMessage { version, params }))
    }

    /// Decode a regular frontend message (with type byte prefix).
    ///
    /// Reads the type byte and length, then decodes the message body.
    /// Returns `None` if the buffer doesn't contain a complete message.
    pub fn decode(buf: &mut BytesMut) -> Result<Option<Self>, PgProtocolError> {
        if buf.len() < 5 {
            // type(1) + length(4)
            return Ok(None);
        }

        let type_byte = buf[0];
        let msg_len = (&buf[1..5]).get_i32() as usize;

        // length field includes itself (4 bytes) but not the type byte
        let total_needed = 1 + msg_len; // type byte + message body
        if buf.len() < total_needed {
            return Ok(None);
        }

        buf.advance(1); // consume type byte
        buf.advance(4); // consume length field

        match type_byte {
            b'Q' => {
                // Query: sql\0
                let sql = read_cstring(buf)?;
                Ok(Some(FrontendMessage::Query { sql }))
            }
            b'P' => {
                // Parse: name\0 + query\0 + num_param_types(2) + param_types(4 each)
                let name = read_cstring(buf)?;
                let query = read_cstring(buf)?;
                let num_params = buf.get_u16() as usize;
                let mut param_types = Vec::with_capacity(num_params);
                for _ in 0..num_params {
                    param_types.push(buf.get_u32());
                }
                Ok(Some(FrontendMessage::Parse {
                    name,
                    query,
                    param_types,
                }))
            }
            b'B' => {
                // Bind: portal\0 + statement\0 + num_formats(2) + formats(2 each)
                // + num_values(2) + values + num_result_formats(2) + result_formats(2 each)
                let portal = read_cstring(buf)?;
                let statement = read_cstring(buf)?;

                let num_formats = buf.get_u16() as usize;
                let mut formats = Vec::with_capacity(num_formats);
                for _ in 0..num_formats {
                    formats.push(buf.get_i16());
                }

                let num_values = buf.get_u16() as usize;
                let mut values = Vec::with_capacity(num_values);
                for _ in 0..num_values {
                    let value_len = buf.get_i32();
                    if value_len == -1 {
                        // NULL value
                        values.push(None);
                    } else if value_len < 0 {
                        return Err(PgProtocolError::ProtocolViolation(format!(
                            "bind value length {} is negative (expected -1 for NULL or >= 0)",
                            value_len
                        )));
                    } else if value_len > buf.remaining() as i32 {
                        return Err(PgProtocolError::ProtocolViolation(format!(
                            "bind value length {} exceeds remaining buffer {}",
                            value_len,
                            buf.remaining()
                        )));
                    } else {
                        let mut data = vec![0u8; value_len as usize];
                        buf.copy_to_slice(&mut data);
                        values.push(Some(data));
                    }
                }

                let num_result_formats = buf.get_u16() as usize;
                let mut result_formats = Vec::with_capacity(num_result_formats);
                for _ in 0..num_result_formats {
                    result_formats.push(buf.get_i16());
                }

                Ok(Some(FrontendMessage::Bind {
                    portal,
                    statement,
                    formats,
                    values,
                    result_formats,
                }))
            }
            b'D' => {
                // Describe: target_type(1) + name\0
                let target_byte = buf.get_u8();
                let target = match target_byte {
                    b'S' => DescribeTarget::Statement,
                    b'P' => DescribeTarget::Portal,
                    _ => {
                        return Err(PgProtocolError::InvalidMessageType(target_byte));
                    }
                };
                let name = read_cstring(buf)?;
                Ok(Some(FrontendMessage::Describe { target, name }))
            }
            b'E' => {
                // Execute: portal\0 + max_rows(4)
                let portal = read_cstring(buf)?;
                let max_rows = buf.get_i32();
                Ok(Some(FrontendMessage::Execute { portal, max_rows }))
            }
            b'C' => {
                // Close: target_type(1) + name\0
                let target_byte = buf.get_u8();
                let target = match target_byte {
                    b'S' => DescribeTarget::Statement,
                    b'P' => DescribeTarget::Portal,
                    _ => {
                        return Err(PgProtocolError::InvalidMessageType(target_byte));
                    }
                };
                let name = read_cstring(buf)?;
                Ok(Some(FrontendMessage::Close { target, name }))
            }
            b'S' => {
                // Sync
                Ok(Some(FrontendMessage::Sync))
            }
            b'X' => {
                // Terminate
                Ok(Some(FrontendMessage::Terminate))
            }
            b'p' => {
                // PasswordMessage: password\0
                let password = read_cstring(buf)?;
                Ok(Some(FrontendMessage::PasswordMessage { password }))
            }
            _ => Err(PgProtocolError::InvalidMessageType(type_byte)),
        }
    }
}

// ============================================================================
// Backend Messages (server → client)
// ============================================================================

/// Messages sent from the server (backend) to the client (frontend).
#[derive(Debug, Clone)]
pub enum BackendMessage {
    /// AuthenticationOk: 'R', length=8, status=0
    AuthenticationOk,
    /// AuthenticationMD5Password: 'R', length=12, status=5, salt(4)
    AuthenticationMD5Password { salt: [u8; 4] },
    /// AuthenticationCleartextPassword: 'R', length=8, status=3
    AuthenticationCleartextPassword,
    /// ParameterStatus: 'S', key\0, value\0
    ParameterStatus { key: String, value: String },
    /// BackendKeyData: 'K', pid(4), secret_key(4)
    BackendKeyData { pid: i32, secret_key: i32 },
    /// ReadyForQuery: 'Z', status(1)
    ReadyForQuery { status: TransactionStatus },
    /// RowDescription: 'T', field descriptions
    RowDescription { fields: Vec<FieldDescription> },
    /// DataRow: 'D', column values
    DataRow { values: Vec<Option<Vec<u8>>> },
    /// CommandComplete: 'C', tag\0
    CommandComplete { tag: String },
    /// ErrorResponse: 'E', severity + code + message fields
    ErrorResponse { fields: Vec<(u8, String)> },
    /// NoticeResponse: 'N', severity + code + message fields
    NoticeResponse { fields: Vec<(u8, String)> },
    /// EmptyQueryResponse: 'I', length=4
    EmptyQueryResponse,
    /// ParseComplete: '1', length=4
    ParseComplete,
    /// BindComplete: '2', length=4
    BindComplete,
    /// CloseComplete: '3', length=4
    CloseComplete,
    /// NoData: 'n', length=4
    NoData,
    /// PortalSuspended: 's', length=4
    PortalSuspended,
    /// ParameterDescription: 't', num_params(2), type_oids(4 each)
    ParameterDescription { type_oids: Vec<u32> },
}

impl BackendMessage {
    /// Encode this backend message into the given buffer.
    pub fn encode(&self, buf: &mut BytesMut) {
        match self {
            BackendMessage::AuthenticationOk => {
                buf.put_u8(b'R');
                buf.put_i32(8); // length
                buf.put_i32(0); // auth ok
            }
            BackendMessage::AuthenticationMD5Password { salt } => {
                buf.put_u8(b'R');
                buf.put_i32(12); // length
                buf.put_i32(5); // MD5 password
                buf.put_slice(salt);
            }
            BackendMessage::AuthenticationCleartextPassword => {
                buf.put_u8(b'R');
                buf.put_i32(8); // length
                buf.put_i32(3); // cleartext password
            }
            BackendMessage::ParameterStatus { key, value } => {
                let body_len = key.len() + 1 + value.len() + 1;
                buf.put_u8(b'S');
                buf.put_i32((body_len + 4) as i32); // total length (incl. self)
                put_cstring(buf, key);
                put_cstring(buf, value);
            }
            BackendMessage::BackendKeyData { pid, secret_key } => {
                buf.put_u8(b'K');
                buf.put_i32(12); // length
                buf.put_i32(*pid);
                buf.put_i32(*secret_key);
            }
            BackendMessage::ReadyForQuery { status } => {
                buf.put_u8(b'Z');
                buf.put_i32(5); // length
                buf.put_u8(status.to_byte());
            }
            BackendMessage::RowDescription { fields } => {
                let mut body = BytesMut::new();
                let field_count = fields.len().min(65535) as u16;
                body.put_u16(field_count);
                for field in fields {
                    put_cstring(&mut body, &field.name);
                    body.put_i32(field.table_oid);
                    body.put_i16(field.column_number);
                    body.put_i32(field.type_oid);
                    body.put_i16(field.type_size);
                    body.put_i32(field.type_modifier);
                    body.put_i16(field.format_code);
                }
                buf.put_u8(b'T');
                buf.put_i32((body.len() + 4) as i32); // length incl. self
                buf.put_slice(&body);
            }
            BackendMessage::DataRow { values } => {
                let mut body = BytesMut::new();
                body.put_u16(values.len() as u16);
                for val in values {
                    match val {
                        Some(data) => {
                            body.put_i32(data.len() as i32);
                            body.put_slice(data);
                        }
                        None => {
                            body.put_i32(-1); // NULL indicator
                        }
                    }
                }
                buf.put_u8(b'D');
                buf.put_i32((body.len() + 4) as i32);
                buf.put_slice(&body);
            }
            BackendMessage::CommandComplete { tag } => {
                let body_len = tag.len() + 1; // +1 for null terminator
                buf.put_u8(b'C');
                buf.put_i32((body_len + 4) as i32);
                put_cstring(buf, tag);
            }
            BackendMessage::ErrorResponse { fields } => {
                let mut body = BytesMut::new();
                for (field_type, value) in fields {
                    body.put_u8(*field_type);
                    put_cstring(&mut body, value);
                }
                body.put_u8(0); // terminator
                buf.put_u8(b'E');
                buf.put_i32((body.len() + 4) as i32);
                buf.put_slice(&body);
            }
            BackendMessage::NoticeResponse { fields } => {
                let mut body = BytesMut::new();
                for (field_type, value) in fields {
                    body.put_u8(*field_type);
                    put_cstring(&mut body, value);
                }
                body.put_u8(0); // terminator
                buf.put_u8(b'N');
                buf.put_i32((body.len() + 4) as i32);
                buf.put_slice(&body);
            }
            BackendMessage::EmptyQueryResponse => {
                buf.put_u8(b'I');
                buf.put_i32(4);
            }
            BackendMessage::ParseComplete => {
                buf.put_u8(b'1');
                buf.put_i32(4);
            }
            BackendMessage::BindComplete => {
                buf.put_u8(b'2');
                buf.put_i32(4);
            }
            BackendMessage::CloseComplete => {
                buf.put_u8(b'3');
                buf.put_i32(4);
            }
            BackendMessage::NoData => {
                buf.put_u8(b'n');
                buf.put_i32(4);
            }
            BackendMessage::PortalSuspended => {
                buf.put_u8(b's');
                buf.put_i32(4);
            }
            BackendMessage::ParameterDescription { type_oids } => {
                let mut body = BytesMut::new();
                body.put_u16(type_oids.len() as u16);
                for oid in type_oids {
                    body.put_u32(*oid);
                }
                buf.put_u8(b't');
                buf.put_i32((body.len() + 4) as i32);
                buf.put_slice(&body);
            }
        }
    }
}

// ============================================================================
// Error Response Helpers
// ============================================================================

/// Error field type constants for ErrorResponse and NoticeResponse.
pub mod error_fields {
    /// Severity (localized): "ERROR", "FATAL", "PANIC"
    pub const SEVERITY: u8 = b'S';
    /// Severity (non-localized): "ERROR", "FATAL", "PANIC"
    pub const SEVERITY_NONLOCALIZED: u8 = b'V';
    /// SQLSTATE code: "XXXXX"
    pub const SQLSTATE: u8 = b'C';
    /// Message: human-readable primary message
    pub const MESSAGE: u8 = b'M';
    /// Detail: secondary message with details
    pub const DETAIL: u8 = b'D';
    /// Hint: suggestion about what to do
    pub const HINT: u8 = b'H';
    /// Position: character position of error in query
    pub const POSITION: u8 = b'P';
    /// Internal position: position in internal query
    pub const INTERNAL_POSITION: u8 = b'p';
    /// Internal query: text of internal query
    pub const INTERNAL_QUERY: u8 = b'q';
    /// Where: context of error
    pub const WHERE: u8 = b'W';
    /// Schema name
    pub const SCHEMA_NAME: u8 = b's';
    /// Table name
    pub const TABLE_NAME: u8 = b't';
    /// Column name
    pub const COLUMN_NAME: u8 = b'c';
    /// Data type name
    pub const DATATYPE_NAME: u8 = b'd';
    /// Constraint name
    pub const CONSTRAINT_NAME: u8 = b'n';
    /// File: source file of error
    pub const FILE: u8 = b'F';
    /// Line: source line of error
    pub const LINE: u8 = b'L';
    /// Routine: source routine of error
    pub const ROUTINE: u8 = b'R';
}

/// Common SQLSTATE error codes.
pub mod sqlstate {
    pub const SUCCESSFUL_COMPLETION: &str = "00000";
    pub const WARNING: &str = "01000";
    pub const DYNAMIC_RESULT_SETS_RETURNED: &str = "0100C";
    pub const NO_DATA: &str = "02000";
    pub const CONNECTION_EXCEPTION: &str = "08000";
    pub const CONNECTION_DOES_NOT_EXIST: &str = "08003";
    pub const CONNECTION_FAILURE: &str = "08006";
    pub const SQL_CLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION: &str = "08001";
    pub const CONNECTION_REJECTED: &str = "08004";
    pub const PROTOCOL_VIOLATION: &str = "08P01";
    pub const FEATURE_NOT_SUPPORTED: &str = "0A000";
    pub const CARDINALITY_VIOLATION: &str = "21000";
    pub const DATA_EXCEPTION: &str = "22000";
    pub const STRING_DATA_RIGHT_TRUNCATION: &str = "22001";
    pub const NULL_VALUE_NO_INDICATOR_PARAMETER: &str = "22002";
    pub const NUMERIC_VALUE_OUT_OF_RANGE: &str = "22003";
    pub const INVALID_DATETIME_FORMAT: &str = "22007";
    pub const DATETIME_FIELD_OVERFLOW: &str = "22008";
    pub const DIVISION_BY_ZERO: &str = "22012";
    pub const INVALID_PARAMETER_VALUE: &str = "22023";
    pub const NOT_NULL_VIOLATION: &str = "23502";
    pub const FOREIGN_KEY_VIOLATION: &str = "23503";
    pub const UNIQUE_VIOLATION: &str = "23505";
    pub const INVALID_CURSOR_STATE: &str = "24000";
    pub const INVALID_TRANSACTION_STATE: &str = "25000";
    pub const ACTIVE_SQL_TRANSACTION: &str = "25001";
    pub const INVALID_SQL_STATEMENT_NAME: &str = "26000";
    pub const INSUFFICIENT_RESOURCES: &str = "53000";
    pub const PROGRAM_LIMIT_EXCEEDED: &str = "54000";
    pub const OBJECT_NOT_IN_PREREQUISITE_STATE: &str = "55000";
    pub const OBJECT_IN_USE: &str = "55006";
    pub const CANNOT_CHANGE_RUNTIME_PARAM: &str = "55P02";
    pub const LOCK_NOT_AVAILABLE: &str = "55P03";
    pub const OPERATOR_INTERVENTION: &str = "57000";
    pub const QUERY_CANCELED: &str = "57014";
    pub const ADMIN_SHUTDOWN: &str = "57P01";
    pub const CRASH_SHUTDOWN: &str = "57P02";
    pub const CANNOT_CONNECT_NOW: &str = "57P03";
    pub const SYSTEM_ERROR: &str = "58000";
    pub const IO_ERROR: &str = "58030";
    pub const UNDEFINED_FUNCTION: &str = "42883";
    pub const UNDEFINED_TABLE: &str = "42P01";
    pub const UNDEFINED_PARAMETER: &str = "42P02";
    pub const UNDEFINED_OBJECT: &str = "42704";
    pub const DUPLICATE_OBJECT: &str = "42710";
    pub const DUPLICATE_COLUMN: &str = "42701";
    pub const DUPLICATE_TABLE: &str = "42P07";
    pub const DUPLICATE_CURSOR: &str = "42P03";
    pub const DUPLICATE_DATABASE: &str = "42P04";
    pub const DUPLICATE_FUNCTION: &str = "42723";
    pub const DUPLICATE_PREPARED_STATEMENT: &str = "42P05";
    pub const DUPLICATE_SCHEMA: &str = "42P06";
    pub const INVALID_COLUMN_REFERENCE: &str = "42P10";
    pub const INVALID_CURSOR_DEFINITION: &str = "42P11";
    pub const INVALID_DATABASE_DEFINITION: &str = "42P12";
    pub const INVALID_FUNCTION_DEFINITION: &str = "42P13";
    pub const INVALID_PREPARED_STATEMENT_DEFINITION: &str = "42P14";
    pub const INVALID_SCHEMA_DEFINITION: &str = "42P15";
    pub const INVALID_TABLE_DEFINITION: &str = "42P16";
    pub const INVALID_OBJECT_DEFINITION: &str = "42P17";
    pub const SYNTAX_ERROR: &str = "42601";
    pub const INSUFFICIENT_PRIVILEGE: &str = "42501";
    pub const INVALID_AUTHORIZATION_SPECIFICATION: &str = "28000";
    pub const INVALID_PASSWORD: &str = "28P01";
}

/// Create a simple error response for SQL errors.
pub fn create_error_response(severity: &str, sqlstate_code: &str, message: &str) -> BackendMessage {
    BackendMessage::ErrorResponse {
        fields: vec![
            (error_fields::SEVERITY, severity.to_string()),
            (error_fields::SQLSTATE, sqlstate_code.to_string()),
            (error_fields::MESSAGE, message.to_string()),
        ],
    }
}

// ============================================================================
// Internal Helper Functions
// ============================================================================

/// Read a null-terminated string from the buffer.
fn read_cstring(buf: &mut BytesMut) -> Result<String, PgProtocolError> {
    let null_pos = buf.iter().position(|&b| b == 0);
    match null_pos {
        Some(pos) => {
            let s = String::from_utf8_lossy(&buf[..pos]).to_string();
            buf.advance(pos + 1); // consume string + null terminator
            Ok(s)
        }
        None => {
            if buf.is_empty() {
                Err(PgProtocolError::UnexpectedEof)
            } else {
                Err(PgProtocolError::UnexpectedEof) // No null terminator = malformed
            }
        }
    }
}

/// Write a null-terminated string to the buffer.
fn put_cstring(buf: &mut BytesMut, s: &str) {
    buf.put_slice(s.as_bytes());
    buf.put_u8(0);
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_authentication_ok() {
        let mut buf = BytesMut::new();
        let msg = BackendMessage::AuthenticationOk;
        msg.encode(&mut buf);

        assert_eq!(buf[0], b'R');
        let len = (&buf[1..5]).get_i32();
        assert_eq!(len, 8);
        let status = (&buf[5..9]).get_i32();
        assert_eq!(status, 0);
    }

    #[test]
    fn test_encode_authentication_md5() {
        let mut buf = BytesMut::new();
        let salt = [0x12, 0x34, 0x56, 0x78];
        let msg = BackendMessage::AuthenticationMD5Password { salt };
        msg.encode(&mut buf);

        assert_eq!(buf[0], b'R');
        let len = (&buf[1..5]).get_i32();
        assert_eq!(len, 12);
        let status = (&buf[5..9]).get_i32();
        assert_eq!(status, 5);
        assert_eq!(&buf[9..13], &salt[..]);
    }

    #[test]
    fn test_encode_ready_for_query() {
        let mut buf = BytesMut::new();
        let msg = BackendMessage::ReadyForQuery {
            status: TransactionStatus::Idle,
        };
        msg.encode(&mut buf);

        assert_eq!(buf[0], b'Z');
        assert_eq!((&buf[1..5]).get_i32(), 5);
        assert_eq!(buf[5], b'I');
    }

    #[test]
    fn test_encode_command_complete() {
        let mut buf = BytesMut::new();
        let msg = BackendMessage::CommandComplete {
            tag: "SELECT 5".to_string(),
        };
        msg.encode(&mut buf);

        assert_eq!(buf[0], b'C');
        let tag = String::from_utf8_lossy(&buf[5..buf.len() - 1]);
        assert_eq!(tag, "SELECT 5");
        assert_eq!(buf[buf.len() - 1], 0);
    }

    #[test]
    fn test_encode_row_description() {
        let mut buf = BytesMut::new();
        let fields = vec![
            FieldDescription::new("id", OID_INT4, 4),
            FieldDescription::new("name", OID_TEXT, -1),
        ];
        let msg = BackendMessage::RowDescription { fields };
        msg.encode(&mut buf);

        assert_eq!(buf[0], b'T');
        let num_fields = (&buf[5..7]).get_u16();
        assert_eq!(num_fields, 2);
    }

    #[test]
    fn test_encode_data_row() {
        let mut buf = BytesMut::new();
        let values = vec![Some(b"42".to_vec()), Some(b"hello".to_vec()), None];
        let msg = BackendMessage::DataRow { values };
        msg.encode(&mut buf);

        assert_eq!(buf[0], b'D');
        let num_cols = (&buf[5..7]).get_u16();
        assert_eq!(num_cols, 3);

        // First value: length 2, "42"
        let v1_len = (&buf[7..11]).get_i32();
        assert_eq!(v1_len, 2);
        assert_eq!(&buf[11..13], b"42");

        // Second value: length 5, "hello"
        let v2_len = (&buf[13..17]).get_i32();
        assert_eq!(v2_len, 5);
        assert_eq!(&buf[17..22], b"hello");

        // Third value: NULL
        let v3_len = (&buf[22..26]).get_i32();
        assert_eq!(v3_len, -1);
    }

    #[test]
    fn test_encode_error_response() {
        let mut buf = BytesMut::new();
        let msg = create_error_response("ERROR", "42P01", "table not found");
        msg.encode(&mut buf);

        assert_eq!(buf[0], b'E');
        assert!(buf.len() > 5);
    }

    #[test]
    fn test_encode_simple_messages() {
        for (msg, expected_type) in [
            (BackendMessage::ParseComplete, b'1'),
            (BackendMessage::BindComplete, b'2'),
            (BackendMessage::CloseComplete, b'3'),
            (BackendMessage::EmptyQueryResponse, b'I'),
            (BackendMessage::NoData, b'n'),
            (BackendMessage::PortalSuspended, b's'),
        ] {
            let mut buf = BytesMut::new();
            msg.encode(&mut buf);
            assert_eq!(buf[0], expected_type, "type byte mismatch");
            assert_eq!((&buf[1..5]).get_i32(), 4, "length should be 4");
        }
    }

    #[test]
    fn test_decode_query_message() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q');
        let sql = "SELECT 1";
        buf.put_i32((sql.len() + 1 + 4) as i32); // length = self(4) + string + null
        put_cstring(&mut buf, sql);

        let msg = FrontendMessage::decode(&mut buf).unwrap().unwrap();
        match msg {
            FrontendMessage::Query { sql: s } => {
                assert_eq!(s, "SELECT 1");
            }
            _ => panic!("Expected Query message"),
        }
    }

    #[test]
    fn test_decode_terminate_message() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'X');
        buf.put_i32(4); // length only

        let msg = FrontendMessage::decode(&mut buf).unwrap().unwrap();
        assert!(matches!(msg, FrontendMessage::Terminate));
    }

    #[test]
    fn test_decode_password_message() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'p');
        let password = "secret123";
        buf.put_i32((password.len() + 1 + 4) as i32);
        put_cstring(&mut buf, password);

        let msg = FrontendMessage::decode(&mut buf).unwrap().unwrap();
        match msg {
            FrontendMessage::PasswordMessage { password: p } => {
                assert_eq!(p, "secret123");
            }
            _ => panic!("Expected PasswordMessage"),
        }
    }

    #[test]
    fn test_decode_sync_message() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'S');
        buf.put_i32(4);

        let msg = FrontendMessage::decode(&mut buf).unwrap().unwrap();
        assert!(matches!(msg, FrontendMessage::Sync));
    }

    #[test]
    fn test_decode_startup_message() {
        let mut buf = BytesMut::new();
        // Startup message: no type byte, just length + version + params
        let version = PG_PROTOCOL_VERSION_3; // 3 << 16 = 196608
        let params = [("user", "testuser"), ("database", "testdb")];

        // Calculate total length: self(4) + version(4) + each pair (key\0 + value\0) + final \0
        let mut body_len = 4; // version
        for (k, v) in &params {
            body_len += k.len() + 1 + v.len() + 1;
        }
        body_len += 1; // final \0
        buf.put_i32((body_len + 4) as i32); // total length includes itself
        buf.put_i32(version);
        for (k, v) in &params {
            put_cstring(&mut buf, k);
            put_cstring(&mut buf, v);
        }
        buf.put_u8(0); // final \0

        let msg = FrontendMessage::decode_startup(&mut buf).unwrap().unwrap();
        match msg {
            FrontendMessage::StartupMessage {
                version: v,
                params: p,
            } => {
                assert_eq!(v, PG_PROTOCOL_VERSION_3);
                assert_eq!(p.get("user").unwrap(), "testuser");
                assert_eq!(p.get("database").unwrap(), "testdb");
            }
            _ => panic!("Expected StartupMessage"),
        }
    }

    #[test]
    fn test_transaction_status_conversion() {
        assert_eq!(TransactionStatus::Idle.to_byte(), b'I');
        assert_eq!(TransactionStatus::InTransaction.to_byte(), b'T');
        assert_eq!(TransactionStatus::Failed.to_byte(), b'E');

        assert_eq!(
            TransactionStatus::from_byte(b'I'),
            Some(TransactionStatus::Idle)
        );
        assert_eq!(
            TransactionStatus::from_byte(b'T'),
            Some(TransactionStatus::InTransaction)
        );
        assert_eq!(
            TransactionStatus::from_byte(b'E'),
            Some(TransactionStatus::Failed)
        );
        assert_eq!(TransactionStatus::from_byte(b'X'), None);
    }

    #[test]
    fn test_decode_describe_message() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'D');
        let name = "mystmt";
        buf.put_i32((1 + name.len() + 1 + 4) as i32); // target(1) + name + null + length(4)
        buf.put_u8(b'S');
        put_cstring(&mut buf, name);

        let msg = FrontendMessage::decode(&mut buf).unwrap().unwrap();
        match msg {
            FrontendMessage::Describe { target, name: n } => {
                assert_eq!(target, DescribeTarget::Statement);
                assert_eq!(n, "mystmt");
            }
            _ => panic!("Expected Describe message"),
        }
    }

    #[test]
    fn test_encode_parameter_status() {
        let mut buf = BytesMut::new();
        let msg = BackendMessage::ParameterStatus {
            key: "server_version".to_string(),
            value: "15.0".to_string(),
        };
        msg.encode(&mut buf);

        assert_eq!(buf[0], b'S');
        let pos = 5;
        let key_end = buf[pos..].iter().position(|&b| b == 0).unwrap();
        assert_eq!(&buf[pos..pos + key_end], b"server_version");
        let val_start = pos + key_end + 1;
        let val_end = buf[val_start..].iter().position(|&b| b == 0).unwrap();
        assert_eq!(&buf[val_start..val_start + val_end], b"15.0");
    }

    #[test]
    fn test_encode_backend_key_data() {
        let mut buf = BytesMut::new();
        let msg = BackendMessage::BackendKeyData {
            pid: 12345,
            secret_key: 98765,
        };
        msg.encode(&mut buf);

        assert_eq!(buf[0], b'K');
        assert_eq!((&buf[1..5]).get_i32(), 12);
        assert_eq!((&buf[5..9]).get_i32(), 12345);
        assert_eq!((&buf[9..13]).get_i32(), 98765);
    }

    #[test]
    fn test_decode_parse_message() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'P');
        let stmt_name = "";
        let query = "SELECT $1::int";
        let param_types: Vec<u32> = vec![23]; // INT4

        // body = name\0 + query\0 + num_params(2) + param_types(4 each)
        let body_len = stmt_name.len() + 1 + query.len() + 1 + 2 + (param_types.len() * 4);
        buf.put_i32((body_len + 4) as i32);
        put_cstring(&mut buf, stmt_name);
        put_cstring(&mut buf, query);
        buf.put_u16(param_types.len() as u16);
        for pt in &param_types {
            buf.put_u32(*pt);
        }

        let msg = FrontendMessage::decode(&mut buf).unwrap().unwrap();
        match msg {
            FrontendMessage::Parse {
                name,
                query: q,
                param_types: pts,
            } => {
                assert_eq!(name, "");
                assert_eq!(q, "SELECT $1::int");
                assert_eq!(pts, vec![23]);
            }
            _ => panic!("Expected Parse message"),
        }
    }

    #[test]
    fn test_decode_execute_message() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'E');
        let portal = "";
        let max_rows = 0;
        let body_len = portal.len() + 1 + 4;
        buf.put_i32((body_len + 4) as i32);
        put_cstring(&mut buf, portal);
        buf.put_i32(max_rows);

        let msg = FrontendMessage::decode(&mut buf).unwrap().unwrap();
        match msg {
            FrontendMessage::Execute {
                portal: p,
                max_rows: m,
            } => {
                assert_eq!(p, "");
                assert_eq!(m, 0);
            }
            _ => panic!("Expected Execute message"),
        }
    }

    #[test]
    fn test_decode_close_message() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'C');
        let name = "mystmt";
        let body_len = 1 + name.len() + 1; // target(1) + name\0
        buf.put_i32((body_len + 4) as i32);
        buf.put_u8(b'S');
        put_cstring(&mut buf, name);

        let msg = FrontendMessage::decode(&mut buf).unwrap().unwrap();
        match msg {
            FrontendMessage::Close { target, name: n } => {
                assert_eq!(target, DescribeTarget::Statement);
                assert_eq!(n, "mystmt");
            }
            _ => panic!("Expected Close message"),
        }
    }

    #[test]
    fn test_decode_bind_message() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'B');
        let portal = "";
        let statement = "";
        let values = vec![Some(b"42".to_vec())];

        // Calculate body length
        let mut body_len = portal.len() + 1 + statement.len() + 1; // portal\0 + statement\0
        body_len += 2; // num_formats
        // no formats
        body_len += 2; // num_values
        body_len += 4 + 2; // value length + "42"
        body_len += 2; // num_result_formats
        // no result formats

        buf.put_i32((body_len + 4) as i32);
        put_cstring(&mut buf, portal);
        put_cstring(&mut buf, statement);
        buf.put_u16(0); // num_formats
        buf.put_u16(values.len() as u16);
        for val in &values {
            match val {
                Some(data) => {
                    buf.put_i32(data.len() as i32);
                    buf.put_slice(data);
                }
                None => {
                    buf.put_i32(-1);
                }
            }
        }
        buf.put_u16(0); // num_result_formats

        let msg = FrontendMessage::decode(&mut buf).unwrap().unwrap();
        match msg {
            FrontendMessage::Bind {
                portal: p,
                statement: s,
                formats,
                values: vals,
                result_formats,
            } => {
                assert_eq!(p, "");
                assert_eq!(s, "");
                assert!(formats.is_empty());
                assert_eq!(vals.len(), 1);
                assert_eq!(vals[0].as_ref().unwrap(), &b"42"[..]);
                assert!(result_formats.is_empty());
            }
            _ => panic!("Expected Bind message"),
        }
    }

    #[test]
    fn test_encode_parameter_description() {
        let mut buf = BytesMut::new();
        let msg = BackendMessage::ParameterDescription {
            type_oids: vec![23, 25],
        };
        msg.encode(&mut buf);

        assert_eq!(buf[0], b't');
        assert_eq!((&buf[5..7]).get_u16(), 2);
        assert_eq!((&buf[7..11]).get_u32(), 23);
        assert_eq!((&buf[11..15]).get_u32(), 25);
    }

    #[test]
    fn test_decode_partial_message_returns_none() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q');
        buf.put_i32(100); // claim we're bigger than we are

        let result = FrontendMessage::decode(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_empty_buffer_returns_none() {
        let mut buf = BytesMut::new();
        let result = FrontendMessage::decode(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_startup_partial_returns_none() {
        let mut buf = BytesMut::new();
        buf.put_i32(100); // claim to be big
        let result = FrontendMessage::decode_startup(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_encode_data_row_with_null() {
        let mut buf = BytesMut::new();
        let values = vec![Some(b"hello".to_vec()), None, Some(b"world".to_vec())];
        let msg = BackendMessage::DataRow { values };
        msg.encode(&mut buf);

        assert_eq!(buf[0], b'D');
        let num_cols = (&buf[5..7]).get_u16();
        assert_eq!(num_cols, 3);

        let mut pos = 7;
        // value 1: "hello"
        assert_eq!((&buf[pos..pos + 4]).get_i32(), 5);
        pos += 4;
        assert_eq!(&buf[pos..pos + 5], b"hello");
        pos += 5;
        // value 2: NULL
        assert_eq!((&buf[pos..pos + 4]).get_i32(), -1);
        pos += 4;
        // value 3: "world"
        assert_eq!((&buf[pos..pos + 4]).get_i32(), 5);
        pos += 4;
        assert_eq!(&buf[pos..pos + 5], b"world");
    }

    #[test]
    fn test_encode_complex_error_response() {
        let mut buf = BytesMut::new();
        let msg = BackendMessage::ErrorResponse {
            fields: vec![
                (error_fields::SEVERITY, "ERROR".to_string()),
                (error_fields::SQLSTATE, "42P01".to_string()),
                (
                    error_fields::MESSAGE,
                    "relation \"users\" does not exist".to_string(),
                ),
                (error_fields::HINT, "check the table name".to_string()),
            ],
        };
        msg.encode(&mut buf);

        assert_eq!(buf[0], b'E');
        assert!(buf.last() == Some(&0)); // terminated by \0
    }

    #[test]
    fn test_field_description_defaults() {
        let f = FieldDescription::new("col1", OID_INT4, 4);
        assert_eq!(f.name, "col1");
        assert_eq!(f.table_oid, 0);
        assert_eq!(f.column_number, 0);
        assert_eq!(f.type_oid, OID_INT4);
        assert_eq!(f.type_size, 4);
        assert_eq!(f.type_modifier, -1);
        assert_eq!(f.format_code, 0);
    }

    #[test]
    fn test_decode_bind_value_len_exceeds_buffer() {
        // Build a Bind message with 2 values where the second value's value_len claims
        // more data than is actually present in the buffer, but the outer message length
        // passes the initial check because it was calculated from the actual data size.
        let mut buf = BytesMut::new();
        buf.put_u8(b'B');

        // Two values: first is small (1 byte), second claims 999 bytes but only has 1 byte
        let portal = "";
        let statement = "";
        let num_values: u16 = 2;

        // Calculate actual body length based on real data:
        // portal\0(1) + statement\0(1) + num_formats(2) + num_values(2)
        // + value0_len(4) + value0_data(1)
        // + value1_len(4) + value1_data(1) (actual 1 byte, but value_len claims 999)
        // + num_result_formats(2)
        let body_len = 1 + 1 + 2 + 2 // headers
            + 4 + 1 // first value
            + 4 + 1 // second value (actual data is 1 byte, but value_len will claim 999)
            + 2; // result formats
        buf.put_i32((body_len + 4) as i32); // msg_len = 4 + body_len
        put_cstring(&mut buf, portal);
        put_cstring(&mut buf, statement);
        buf.put_u16(0); // num_formats
        buf.put_u16(num_values);

        // First value: small
        buf.put_i32(1); // value_len = 1
        buf.put_u8(b'X'); // 1 byte of data

        // Second value: value_len claims 999 but only 1 byte of actual data follows
        buf.put_i32(999); // value_len = 999 (corrupted)
        buf.put_u8(b'Y'); // only 1 byte of data

        buf.put_u16(0); // num_result_formats

        // The total buffer is ~23 bytes, msg_len = body_len + 4 = ~22
        // total_needed = 1 + msg_len = ~23 = buf.len(), outer length check passes
        // But when decoding the second value, value_len(999) > remaining(~3 bytes)
        // which triggers the ProtocolViolation error
        let result = FrontendMessage::decode(&mut buf);
        assert!(
            result.is_err(),
            "Bind with value_len exceeding buffer should return error, got: {:?}",
            result
        );
    }

    #[test]
    fn test_decode_empty_query_message() {
        // Query message with empty SQL string
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q');
        let sql = "";
        buf.put_i32((sql.len() + 1 + 4) as i32); // length = self(4) + string + null
        put_cstring(&mut buf, sql);

        let msg = FrontendMessage::decode(&mut buf).unwrap().unwrap();
        match msg {
            FrontendMessage::Query { sql: s } => {
                assert_eq!(s, "");
            }
            _ => panic!("Expected Query message"),
        }
    }

    #[test]
    fn test_encode_error_response_with_multiple_fields() {
        // ErrorResponse with Severity, Code, Message, Detail, Hint
        let mut buf = BytesMut::new();
        let msg = BackendMessage::ErrorResponse {
            fields: vec![
                (error_fields::SEVERITY, "ERROR".to_string()),
                (error_fields::SQLSTATE, "42P01".to_string()),
                (error_fields::MESSAGE, "table not found".to_string()),
                (
                    error_fields::DETAIL,
                    "relation \"users\" does not exist".to_string(),
                ),
                (error_fields::HINT, "check the table name".to_string()),
            ],
        };
        msg.encode(&mut buf);

        assert_eq!(buf[0], b'E');
        assert!(buf.last() == Some(&0)); // terminated by \0

        // Verify all field types are present in the encoded output
        let body = &buf[5..buf.len() - 1]; // skip type(1) + length(4) + terminator(1)
        let mut pos = 0;
        let expected_fields = [b'S', b'C', b'M', b'D', b'H'];
        for &expected_type in &expected_fields {
            assert_eq!(
                body[pos], expected_type,
                "Expected field type '{}' at position {}",
                expected_type as char, pos
            );
            // Skip field value to next null terminator
            while body[pos] != 0 {
                pos += 1;
            }
            pos += 1; // skip null
        }
    }

    #[test]
    fn test_startup_message_with_extra_params() {
        // StartupMessage with application_name, client_encoding, etc.
        let mut buf = BytesMut::new();
        let version = PG_PROTOCOL_VERSION_3;
        let params = [
            ("user", "testuser"),
            ("database", "testdb"),
            ("application_name", "psql"),
            ("client_encoding", "UTF8"),
            ("extra_float_digits", "3"),
        ];

        // Calculate total length: self(4) + version(4) + each pair (key\0 + value\0) + final \0
        let mut body_len = 4; // version
        for (k, v) in &params {
            body_len += k.len() + 1 + v.len() + 1;
        }
        body_len += 1; // final \0
        buf.put_i32((body_len + 4) as i32); // total length includes itself
        buf.put_i32(version);
        for (k, v) in &params {
            put_cstring(&mut buf, k);
            put_cstring(&mut buf, v);
        }
        buf.put_u8(0); // final \0

        let msg = FrontendMessage::decode_startup(&mut buf).unwrap().unwrap();
        match msg {
            FrontendMessage::StartupMessage {
                version: v,
                params: p,
            } => {
                assert_eq!(v, PG_PROTOCOL_VERSION_3);
                assert_eq!(p.get("user").unwrap(), "testuser");
                assert_eq!(p.get("database").unwrap(), "testdb");
                assert_eq!(p.get("application_name").unwrap(), "psql");
                assert_eq!(p.get("client_encoding").unwrap(), "UTF8");
                assert_eq!(p.get("extra_float_digits").unwrap(), "3");
                assert_eq!(p.len(), 5, "expected exactly 5 parameters");
            }
            _ => panic!("Expected StartupMessage"),
        }
    }
}
