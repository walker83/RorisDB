use mysql::prelude::*;
use mysql::{Conn, Row, Value};

// ===========================================================================
// Test Configuration
// ===========================================================================
const MYSQL_PORT: u16 = 19930;

// ===========================================================================
// Server lifecycle management
// ===========================================================================

// E2eServer, find_binary and make_conn live in the shared
// `integration_tests::harness` module so a server-side spawn change (e.g.
// passing --dev for auth) only needs to be made in one place.
use integration_tests::harness;

fn make_conn() -> mysql::Conn {
    harness::make_conn(MYSQL_PORT)
}

fn exec_sql(conn: &mut Conn, sql: &str) {
    conn.query_drop(sql)
        .unwrap_or_else(|e| panic!("Query failed: '{}' -- {}", sql, e));
}

fn query_rows(conn: &mut Conn, sql: &str) -> Vec<Row> {
    conn.query(sql)
        .unwrap_or_else(|e| panic!("Query failed: '{}' -- {}", sql, e))
}

fn get_i64(row: &Row, idx: usize) -> i64 {
    match &row[idx] {
        Value::Int(n) => *n,
        Value::UInt(n) => *n as i64,
        Value::Bytes(b) => {
            let s = String::from_utf8_lossy(b);
            s.parse::<i64>()
                .unwrap_or_else(|e| panic!("Cannot parse Bytes({:?}) as i64: {}", s, e))
        }
        v => panic!("Expected integer at column {}, got {:?}", idx, v),
    }
}

fn get_f64(row: &Row, idx: usize) -> f64 {
    match &row[idx] {
        Value::Float(f) => *f as f64,
        Value::Double(d) => *d,
        Value::Int(n) => *n as f64,
        Value::Bytes(b) => {
            let s = String::from_utf8_lossy(b);
            s.parse::<f64>()
                .unwrap_or_else(|e| panic!("Cannot parse Bytes({:?}) as f64: {}", s, e))
        }
        v => panic!("Expected float at column {}, got {:?}", idx, v),
    }
}

fn get_string(row: &Row, idx: usize) -> String {
    match &row[idx] {
        Value::Bytes(b) => String::from_utf8_lossy(b).to_string(),
        Value::NULL => String::new(),
        v => format!("{:?}", v),
    }
}

// ===========================================================================
// The E2E test
// ===========================================================================

#[test]
fn test_doris_compat_e2e() {
    // Keep the server alive for the duration of the test (it is dropped on exit).
    let _server = harness::shared_server(MYSQL_PORT);

    let mut conn = make_conn();

    // a. CREATE DATABASE
    exec_sql(&mut conn, "CREATE DATABASE test_e2e");

    // b. USE database
    exec_sql(&mut conn, "USE test_e2e");

    // c. CREATE TABLE with Doris syntax
    exec_sql(
        &mut conn,
        "CREATE TABLE users (
            id INT,
            name VARCHAR(100),
            age INT,
            salary DOUBLE
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1",
    );

    // d. INSERT single row
    exec_sql(
        &mut conn,
        "INSERT INTO users VALUES (1, 'Alice', 30, 50000.0)",
    );

    // e. INSERT multiple rows
    exec_sql(
        &mut conn,
        "INSERT INTO users VALUES (2, 'Bob', 25, 45000.0), (3, 'Charlie', 35, 60000.0)",
    );

    // f. SELECT with WHERE
    let rows = query_rows(&mut conn, "SELECT * FROM users WHERE age > 28");
    assert_eq!(rows.len(), 2, "WHERE age > 28 should return 2 rows");

    // g. SELECT with ORDER BY
    let rows = query_rows(
        &mut conn,
        "SELECT name, salary FROM users ORDER BY salary DESC",
    );
    assert_eq!(rows.len(), 3);
    assert_eq!(get_string(&rows[0], 0), "Charlie");
    assert_eq!(get_string(&rows[1], 0), "Alice");
    assert_eq!(get_string(&rows[2], 0), "Bob");

    // h. Aggregation
    let rows = query_rows(&mut conn, "SELECT COUNT(*), AVG(salary) FROM users");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 3);
    let avg = get_f64(&rows[0], 1);
    assert!(
        (avg - 51666.67).abs() < 100.0,
        "AVG should be ~51666, got {}",
        avg
    );

    // i. UPDATE
    exec_sql(
        &mut conn,
        "UPDATE users SET salary = 55000.0 WHERE name = 'Alice'",
    );

    // j. Verify UPDATE
    let rows = query_rows(&mut conn, "SELECT salary FROM users WHERE name = 'Alice'");
    assert_eq!(rows.len(), 1);
    assert!((get_f64(&rows[0], 0) - 55000.0).abs() < 0.01);

    // k. DELETE
    exec_sql(&mut conn, "DELETE FROM users WHERE age < 30");

    // l. Verify DELETE
    let rows = query_rows(&mut conn, "SELECT COUNT(*) FROM users");
    assert_eq!(get_i64(&rows[0], 0), 2);

    // m. DROP TABLE
    exec_sql(&mut conn, "DROP TABLE users");

    // n. DROP DATABASE
    exec_sql(&mut conn, "DROP DATABASE test_e2e");
}
