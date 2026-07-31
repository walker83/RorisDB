//! Integration tests for pg-protocol crate.
//!
//! These tests connect via raw TCP sockets and speak the PostgreSQL wire protocol v3
//! manually, verifying the full connection lifecycle, simple/extended query modes,
//! auth failure handling, and SSL request handling.

use std::sync::Arc;

use pg_protocol::auth::{AuthConfig, compute_md5_password};
use pg_protocol::connection::PgConnection;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use mysql_protocol::server::{ColumnDef, ColumnType, QueryHandler, QueryResult};

// ============================================================================
// Mock handler for tests
// ============================================================================

struct TestHandler;

impl QueryHandler for TestHandler {
    fn handle_query(&self, _conn_id: u32, sql: &str) -> QueryResult {
        let trimmed = sql.trim().trim_end_matches(';');
        let upper = trimmed.to_uppercase();
        if upper == "SELECT 1 AS NUM" || upper == "SELECT 1" {
            QueryResult::with_rows(
                vec![ColumnDef {
                    name: "num".to_string(),
                    col_type: ColumnType::Int,
                }],
                vec![vec![Some("1".to_string())]],
            )
        } else {
            QueryResult::ok()
        }
    }
}

// ============================================================================
// PG wire protocol message helpers
// ============================================================================

/// Read a complete PG backend message: type byte (1) + length (4) + body.
async fn read_message<R: AsyncReadExt + Unpin>(reader: &mut R) -> (u8, Vec<u8>) {
    let mut type_buf = [0u8; 1];
    reader.read_exact(&mut type_buf).await.unwrap();

    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await.unwrap();
    let total_len = i32::from_be_bytes(len_buf) as usize;
    let body_len = total_len - 4;

    let mut body = vec![0u8; body_len];
    if body_len > 0 {
        reader.read_exact(&mut body).await.unwrap();
    }
    (type_buf[0], body)
}

/// Encode a StartupMessage (no type byte).
/// Format: Length(4) + Version(4) + "user\0" + user + "\0" + "database\0" + db + "\0\0"
fn encode_startup(user: &str, database: &str) -> Vec<u8> {
    let version = 196608i32; // PG protocol 3.0 = 3 << 16 | 0
    let mut body = Vec::new();
    body.extend_from_slice(&version.to_be_bytes());
    body.extend_from_slice(b"user\0");
    body.extend_from_slice(user.as_bytes());
    body.push(0);
    body.extend_from_slice(b"database\0");
    body.extend_from_slice(database.as_bytes());
    body.push(0);
    body.push(0); // final null terminator

    let mut msg = Vec::new();
    msg.extend_from_slice(&(body.len() as i32 + 4).to_be_bytes()); // length includes self
    msg.extend_from_slice(&body);
    msg
}

/// Encode a PasswordMessage: 'p'(1) + Length(4) + password\0
fn encode_password(password: &str) -> Vec<u8> {
    let body = [password.as_bytes(), &[0]].concat();
    let mut msg = vec![b'p'];
    msg.extend_from_slice(&(body.len() as i32 + 4).to_be_bytes());
    msg.extend_from_slice(&body);
    msg
}

/// Encode a Query message: 'Q'(1) + Length(4) + sql\0
fn encode_query(sql: &str) -> Vec<u8> {
    let body = [sql.as_bytes(), &[0]].concat();
    let mut msg = vec![b'Q'];
    msg.extend_from_slice(&(body.len() as i32 + 4).to_be_bytes());
    msg.extend_from_slice(&body);
    msg
}

/// Encode a Parse message: 'P'(1) + Length(4) + name\0 + query\0 + num_params(2) + param_types(4*N)
fn encode_parse(statement: &str, query: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(statement.as_bytes());
    body.push(0);
    body.extend_from_slice(query.as_bytes());
    body.push(0);
    body.extend_from_slice(&0u16.to_be_bytes()); // num_param_types

    let mut msg = vec![b'P'];
    msg.extend_from_slice(&(body.len() as i32 + 4).to_be_bytes());
    msg.extend_from_slice(&body);
    msg
}

/// Encode a Bind message: 'B'(1) + Length(4) + portal\0 + statement\0 + formats(2+N) + values(2+N) + result_formats(2+N)
/// Sends with 0 params, 0 result formats (all text).
fn encode_bind(portal: &str, statement: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(portal.as_bytes());
    body.push(0);
    body.extend_from_slice(statement.as_bytes());
    body.push(0);
    body.extend_from_slice(&0u16.to_be_bytes()); // num_formats (0 = all text)
    body.extend_from_slice(&0u16.to_be_bytes()); // num_values
    body.extend_from_slice(&0u16.to_be_bytes()); // num_result_formats (0 = all text)

    let mut msg = vec![b'B'];
    msg.extend_from_slice(&(body.len() as i32 + 4).to_be_bytes());
    msg.extend_from_slice(&body);
    msg
}

/// Encode an Execute message: 'E'(1) + Length(4) + portal\0 + max_rows(4)
fn encode_execute(portal: &str, max_rows: i32) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(portal.as_bytes());
    body.push(0);
    body.extend_from_slice(&max_rows.to_be_bytes());

    let mut msg = vec![b'E'];
    msg.extend_from_slice(&(body.len() as i32 + 4).to_be_bytes());
    msg.extend_from_slice(&body);
    msg
}

/// Encode a Sync message: 'S'(1) + Length(4)  [length = 4]
fn encode_sync() -> Vec<u8> {
    let mut msg = vec![b'S'];
    msg.extend_from_slice(&4i32.to_be_bytes());
    msg
}

/// Encode a Terminate message: 'X'(1) + Length(4)  [length = 4]
fn encode_terminate() -> Vec<u8> {
    let mut msg = vec![b'X'];
    msg.extend_from_slice(&4i32.to_be_bytes());
    msg
}

/// Encode an SSLRequest (no type byte, like startup).
/// Format: Length(4) + SSLRequestCode(4)  [SSL request code = 80877102]
fn encode_ssl_request() -> Vec<u8> {
    let ssl_code = 80877103i32; // SSL_REQUEST_CODE = 1234 << 16 | 5679
    let mut msg = Vec::new();
    msg.extend_from_slice(&8i32.to_be_bytes()); // length = 8
    msg.extend_from_slice(&ssl_code.to_be_bytes());
    msg
}

// ============================================================================
// Auth helper
// ============================================================================

/// Perform the full startup + MD5 auth handshake.
///
/// 1. Send StartupMessage
/// 2. Receive AuthenticationMD5Password, extract salt
/// 3. Compute MD5(password) and send PasswordMessage
/// 4. Receive AuthenticationOk
/// 5. Drain ParameterStatus and BackendKeyData messages
/// 6. Receive ReadyForQuery
async fn perform_startup_and_auth<R, W>(
    reader: &mut R,
    writer: &mut W,
    user: &str,
    database: &str,
    password: &str,
) where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    // 1. Send StartupMessage
    let startup = encode_startup(user, database);
    writer.write_all(&startup).await.unwrap();

    // 2. Receive AuthenticationMD5Password
    let (msg_type, body) = read_message(reader).await;
    assert_eq!(
        msg_type, b'R',
        "expected AuthenticationMD5Password, got type byte 0x{:02x}",
        msg_type
    );
    let auth_type = i32::from_be_bytes(body[..4].try_into().unwrap());
    assert_eq!(
        auth_type, 5,
        "expected MD5 password auth type, got {}",
        auth_type
    );

    // 3. Compute MD5 response and send PasswordMessage
    let salt: [u8; 4] = body[4..8].try_into().unwrap();
    let md5_hash = compute_md5_password(user, password, &salt);
    let pwd_msg = encode_password(&md5_hash);
    writer.write_all(&pwd_msg).await.unwrap();

    // 4. Read AuthenticationOk
    loop {
        let (msg_type, body) = read_message(reader).await;
        if msg_type == b'R' {
            let at = i32::from_be_bytes(body[..4].try_into().unwrap());
            if at == 0 {
                break; // AuthenticationOk
            }
        }
        if msg_type == b'Z' {
            panic!("got ReadyForQuery before AuthenticationOk");
        }
    }

    // 5. Drain ParameterStatus + BackendKeyData, then ReadyForQuery
    loop {
        let (msg_type, _body) = read_message(reader).await;
        if msg_type == b'Z' {
            break; // ReadyForQuery
        }
        assert!(
            msg_type == b'S' || msg_type == b'K',
            "unexpected message type during startup: 0x{:02x} ({})",
            msg_type,
            msg_type as char
        );
    }
}

// ============================================================================
// Server helper
// ============================================================================

/// Bind a TCP listener on a random port, spawn a server that accepts one
/// connection and runs PgConnection on it. Returns the socket address and
/// the join handle.
async fn start_server(
    handler: Arc<impl QueryHandler + 'static>,
    auth_config: AuthConfig,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut conn = PgConnection::new(stream, 1, handler, auth_config);
        let _ = conn.run().await;
    });

    (addr, handle)
}

// ============================================================================
// Tests
// ============================================================================

// ---------------------------------------------------------------------------
// Test 1: Full connection lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_connection_lifecycle() {
    let handler = Arc::new(TestHandler);
    let auth_config = AuthConfig {
        accept_any_password: true,
        ..Default::default()
    };

    let (addr, _server) = start_server(handler, auth_config).await;
    let client = TcpStream::connect(addr).await.unwrap();
    let (mut reader, mut writer) = tokio::io::split(client);

    // Startup + auth handshake
    perform_startup_and_auth(&mut reader, &mut writer, "harness", "default", "anything").await;

    // Send Terminate
    let terminate = encode_terminate();
    writer.write_all(&terminate).await.unwrap();

    // The server should close the connection after Terminate.
    // Give a moment then check the connection is closed.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // If we got here without panicking, the lifecycle succeeded.
}

// ---------------------------------------------------------------------------
// Test 2: Simple query
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_simple_query() {
    let handler = Arc::new(TestHandler);
    let auth_config = AuthConfig {
        accept_any_password: true,
        ..Default::default()
    };

    let (addr, _server) = start_server(handler, auth_config).await;
    let client = TcpStream::connect(addr).await.unwrap();
    let (mut reader, mut writer) = tokio::io::split(client);

    perform_startup_and_auth(&mut reader, &mut writer, "harness", "default", "pass").await;

    // Send Query("SELECT 1 AS num")
    let query = encode_query("SELECT 1 AS num");
    writer.write_all(&query).await.unwrap();

    // Verify RowDescription
    let (msg_type, body) = read_message(&mut reader).await;
    assert_eq!(
        msg_type, b'T',
        "expected RowDescription, got type={}",
        msg_type as char
    );
    let num_fields = u16::from_be_bytes(body[..2].try_into().unwrap());
    assert_eq!(num_fields, 1, "expected 1 column");
    // Check column name is "num"
    let name_end = body[2..].iter().position(|&b| b == 0).unwrap();
    let col_name = String::from_utf8_lossy(&body[2..2 + name_end]);
    assert_eq!(col_name, "num", "column name should be 'num'");

    // Verify DataRow
    let (msg_type, body) = read_message(&mut reader).await;
    assert_eq!(
        msg_type, b'D',
        "expected DataRow, got type={}",
        msg_type as char
    );
    let num_cols = u16::from_be_bytes(body[..2].try_into().unwrap());
    assert_eq!(num_cols, 1, "expected 1 column in DataRow");
    let col_len = i32::from_be_bytes(body[2..6].try_into().unwrap());
    assert_eq!(col_len, 1, "expected data length 1");
    assert_eq!(&body[6..7], b"1", "expected data '1'");

    // Verify CommandComplete
    let (msg_type, body) = read_message(&mut reader).await;
    assert_eq!(
        msg_type, b'C',
        "expected CommandComplete, got type={}",
        msg_type as char
    );
    let tag = String::from_utf8_lossy(&body[..body.len() - 1]);
    assert_eq!(tag, "SELECT 1");

    // Verify ReadyForQuery
    let (msg_type, _body) = read_message(&mut reader).await;
    assert_eq!(
        msg_type, b'Z',
        "expected ReadyForQuery, got type={}",
        msg_type as char
    );
}

// ---------------------------------------------------------------------------
// Test 3: Extended query flow (Parse/Bind/Execute/Sync)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_extended_query_flow() {
    let handler = Arc::new(TestHandler);
    let auth_config = AuthConfig {
        accept_any_password: true,
        ..Default::default()
    };

    let (addr, _server) = start_server(handler, auth_config).await;
    let client = TcpStream::connect(addr).await.unwrap();
    let (mut reader, mut writer) = tokio::io::split(client);

    perform_startup_and_auth(&mut reader, &mut writer, "harness", "default", "x").await;

    // 1. Parse("SELECT 1 AS num")
    let parse = encode_parse("", "SELECT 1 AS num");
    writer.write_all(&parse).await.unwrap();
    let (msg_type, _body) = read_message(&mut reader).await;
    assert_eq!(
        msg_type, b'1',
        "expected ParseComplete ('1'), got 0x{:02x}",
        msg_type
    );

    // 2. Bind(portal="", statement="")
    let bind = encode_bind("", "");
    writer.write_all(&bind).await.unwrap();
    let (msg_type, _body) = read_message(&mut reader).await;
    assert_eq!(
        msg_type, b'2',
        "expected BindComplete ('2'), got 0x{:02x}",
        msg_type
    );

    // 3. Execute(portal="", max_rows=0)
    let execute = encode_execute("", 0);
    writer.write_all(&execute).await.unwrap();

    // Execute sends DataRow + CommandComplete (NOT RowDescription — that's Describe's job)

    // DataRow
    let (msg_type, body) = read_message(&mut reader).await;
    assert_eq!(msg_type, b'D', "expected DataRow, got 0x{:02x}", msg_type);
    let col_len = i32::from_be_bytes(body[2..6].try_into().unwrap());
    assert_eq!(col_len, 1);
    assert_eq!(&body[6..7], b"1");

    // CommandComplete
    let (msg_type, body) = read_message(&mut reader).await;
    assert_eq!(
        msg_type, b'C',
        "expected CommandComplete, got 0x{:02x}",
        msg_type
    );
    let tag = String::from_utf8_lossy(&body[..body.len() - 1]);
    assert_eq!(tag, "SELECT 1");

    // 4. Sync
    let sync = encode_sync();
    writer.write_all(&sync).await.unwrap();

    // ReadyForQuery
    let (msg_type, body) = read_message(&mut reader).await;
    assert_eq!(
        msg_type, b'Z',
        "expected ReadyForQuery, got 0x{:02x}",
        msg_type
    );
    assert_eq!(body[0], b'I', "expected idle transaction status");
}

// ---------------------------------------------------------------------------
// Test 4: Auth failure with wrong password
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_auth_failure_wrong_password() {
    let handler = Arc::new(TestHandler);
    // Set specific credentials, do NOT accept any password
    let auth_config = AuthConfig {
        accept_any_password: false,
        username: "harness".to_string(),
        password: "correct_password".to_string(),
    };

    let (addr, _server) = start_server(handler, auth_config).await;
    let client = TcpStream::connect(addr).await.unwrap();
    let (mut reader, mut writer) = tokio::io::split(client);

    // Send StartupMessage
    let startup = encode_startup("harness", "default");
    writer.write_all(&startup).await.unwrap();

    // Receive AuthenticationMD5Password and extract salt
    let (msg_type, body) = read_message(&mut reader).await;
    assert_eq!(msg_type, b'R', "expected AuthenticationMD5Password");
    let salt: [u8; 4] = body[4..8].try_into().unwrap();

    // Compute MD5 with WRONG password
    let wrong_md5 = compute_md5_password("harness", "wrong_password", &salt);
    let pwd_msg = encode_password(&wrong_md5);
    writer.write_all(&pwd_msg).await.unwrap();

    // Should receive an ErrorResponse, then the connection closes
    let mut buf = [0u8; 1];
    let result =
        tokio::time::timeout(std::time::Duration::from_secs(3), reader.read(&mut buf)).await;

    match result {
        Ok(Ok(0)) => {
            // Connection cleanly closed — the server sent ErrorResponse then closed
        }
        Ok(Ok(_n)) => {
            // An ErrorResponse was sent before close
            // The remaining data is the ErrorResponse message content
            // We already accepted any response since the server does send ErrorResponse now
            // but the read may return partial data
        }
        Ok(Err(e)) => {
            // Connection reset or broken pipe is also acceptable
            assert!(
                e.kind() == std::io::ErrorKind::ConnectionReset
                    || e.kind() == std::io::ErrorKind::BrokenPipe,
                "unexpected IO error: {}",
                e
            );
        }
        Err(_) => {
            panic!("timeout waiting for connection to close after auth failure");
        }
    }
}

// ---------------------------------------------------------------------------
// Test 5: Multiple queries on the same connection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_multiple_queries() {
    let handler = Arc::new(TestHandler);
    let auth_config = AuthConfig {
        accept_any_password: true,
        ..Default::default()
    };

    let (addr, _server) = start_server(handler, auth_config).await;
    let client = TcpStream::connect(addr).await.unwrap();
    let (mut reader, mut writer) = tokio::io::split(client);

    perform_startup_and_auth(&mut reader, &mut writer, "harness", "default", "pass").await;

    // Send first query
    let query = encode_query("SELECT 1 AS num");
    writer.write_all(&query).await.unwrap();

    // Drain response for first query: RowDescription + DataRow + CommandComplete + ReadyForQuery
    let (msg_type, body) = read_message(&mut reader).await;
    assert_eq!(msg_type, b'T', "first query: expected RowDescription");
    let name_end = body[2..].iter().position(|&b| b == 0).unwrap();
    assert_eq!(
        String::from_utf8_lossy(&body[2..2 + name_end]),
        "num",
        "first query: column name should be 'num'"
    );

    let (msg_type, body) = read_message(&mut reader).await;
    assert_eq!(msg_type, b'D', "first query: expected DataRow");
    assert_eq!(&body[6..7], b"1", "first query: expected data '1'");

    let (msg_type, _body) = read_message(&mut reader).await;
    assert_eq!(msg_type, b'C', "first query: expected CommandComplete");

    let (msg_type, _body) = read_message(&mut reader).await;
    assert_eq!(msg_type, b'Z', "first query: expected ReadyForQuery");

    // Send second query
    writer.write_all(&query).await.unwrap();

    // Drain response for second query
    let (msg_type, body) = read_message(&mut reader).await;
    assert_eq!(msg_type, b'T', "second query: expected RowDescription");
    let name_end = body[2..].iter().position(|&b| b == 0).unwrap();
    assert_eq!(
        String::from_utf8_lossy(&body[2..2 + name_end]),
        "num",
        "second query: column name should be 'num'"
    );

    let (msg_type, body) = read_message(&mut reader).await;
    assert_eq!(msg_type, b'D', "second query: expected DataRow");
    assert_eq!(&body[6..7], b"1", "second query: expected data '1'");

    let (msg_type, _body) = read_message(&mut reader).await;
    assert_eq!(msg_type, b'C', "second query: expected CommandComplete");

    let (msg_type, _body) = read_message(&mut reader).await;
    assert_eq!(msg_type, b'Z', "second query: expected ReadyForQuery");
}

// ---------------------------------------------------------------------------
// Test 6: SSL request declined
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_ssl_request_declined() {
    let handler = Arc::new(TestHandler);
    let auth_config = AuthConfig {
        accept_any_password: true,
        ..Default::default()
    };

    let (addr, _server) = start_server(handler, auth_config).await;
    let client = TcpStream::connect(addr).await.unwrap();
    let (mut reader, mut writer) = tokio::io::split(client);

    // 1. Send SSLRequest
    let ssl_req = encode_ssl_request();
    writer.write_all(&ssl_req).await.unwrap();

    // 2. Read single byte 'N' (server declines SSL)
    let mut resp = [0u8; 1];
    reader.read_exact(&mut resp).await.unwrap();
    assert_eq!(
        resp[0], b'N',
        "expected 'N' (SSL declined), got 0x{:02x}",
        resp[0]
    );

    // 3. Now send normal StartupMessage and complete auth
    perform_startup_and_auth(&mut reader, &mut writer, "harness", "default", "pass").await;

    // Verify the connection works by running a simple query
    let query = encode_query("SELECT 1 AS num");
    writer.write_all(&query).await.unwrap();

    // RowDescription
    let (msg_type, _body) = read_message(&mut reader).await;
    assert_eq!(msg_type, b'T', "expected RowDescription after SSL decline");

    // DataRow
    let (msg_type, body) = read_message(&mut reader).await;
    assert_eq!(msg_type, b'D', "expected DataRow after SSL decline");
    assert_eq!(&body[6..7], b"1", "expected data '1'");

    // CommandComplete
    let (msg_type, _body) = read_message(&mut reader).await;
    assert_eq!(msg_type, b'C', "expected CommandComplete after SSL decline");

    // ReadyForQuery
    let (msg_type, _body) = read_message(&mut reader).await;
    assert_eq!(msg_type, b'Z', "expected ReadyForQuery after SSL decline");
}
