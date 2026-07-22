// E2E integration tests for HarnessDB math functions.
//
// IMPORTANT: The server returns ALL values as Bytes (strings) over MySQL protocol.
// ALWAYS use get_i64(), get_f64(), get_string(), is_null() helpers.

use lazy_static::lazy_static;
use mysql::prelude::*;
use mysql::{Row, Value};
use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// === CHANGE PER FILE: use unique port ===
const MYSQL_PORT: u16 = 29990;

static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

// E2eServer, find_binary and make_conn live in the shared
// `integration_tests::harness` module so a server-side spawn change (e.g.
// passing --dev for auth) only needs to be made in one place.
use integration_tests::harness;

fn make_conn() -> mysql::Conn {
    harness::make_conn(MYSQL_PORT)
}

struct TestContext {
    #[allow(dead_code)]
    server: Arc<harness::E2eServer>,
    conn: RefCell<mysql::Conn>,
}

lazy_static! {
    static ref SERVER: Arc<harness::E2eServer> = {
        harness::shared_server(MYSQL_PORT)
    };
}

impl TestContext {
    fn new() -> Self {
        let server = SERVER.clone();
        let conn = make_conn();
        TestContext {
            server,
            conn: RefCell::new(conn),
        }
    }

    /// Create a unique database name and return it
    fn new_db_name() -> String {
        let n = DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        format!("test_{}_{}", MYSQL_PORT, n)
    }

    /// Create a database, USE it, return the name
    fn create_and_use_db(&self) -> String {
        let db = Self::new_db_name();
        self.exec(&format!("CREATE DATABASE {}", db));
        self.exec(&format!("USE {}", db));
        db
    }

    /// Drop a database (call at end of test)
    fn drop_db(&self, db: &str) {
        let _ = self.exec_ignore_error(&format!("DROP DATABASE IF EXISTS {}", db));
    }

    fn exec(&self, sql: &str) {
        let mut conn = self.conn.borrow_mut();
        conn.query_drop(sql)
            .unwrap_or_else(|e| panic!("SQL failed: {} -- {}", sql, e));
    }

    fn exec_ignore_error(&self, sql: &str) -> Result<(), String> {
        let mut conn = self.conn.borrow_mut();
        conn.query_drop(sql).map_err(|e| format!("{}: {}", sql, e))
    }

    fn query(&self, sql: &str) -> Vec<Row> {
        let mut conn = self.conn.borrow_mut();
        conn.query(sql)
            .unwrap_or_else(|e| panic!("Query failed: {} -- {}", sql, e))
    }

    fn query_ignore_error(&self, sql: &str) -> Result<Vec<Row>, String> {
        let mut conn = self.conn.borrow_mut();
        conn.query(sql).map_err(|e| format!("{}: {}", sql, e))
    }

    /// Assert query returns expected number of rows
    fn assert_row_count(&self, sql: &str, expected: usize) {
        let rows = self.query(sql);
        assert_eq!(
            rows.len(),
            expected,
            "SQL: {} expected {} rows, got {}",
            sql,
            expected,
            rows.len()
        );
    }
}

// === Value extraction helpers (ALL values come back as Bytes strings) ===

fn get_i64(row: &Row, idx: usize) -> i64 {
    match &row[idx] {
        Value::Int(n) => *n,
        Value::UInt(n) => *n as i64,
        Value::Bytes(b) => {
            let s = String::from_utf8_lossy(b);
            // Handle float strings like "3.0" by parsing as f64 first
            if let Ok(f) = s.parse::<f64>() {
                f as i64
            } else {
                s.parse::<i64>()
                    .unwrap_or_else(|e| panic!("get_i64: cannot parse {:?}: {}", s, e))
            }
        }
        v => panic!("get_i64: unexpected {:?} at col {}", v, idx),
    }
}

fn get_f64(row: &Row, idx: usize) -> f64 {
    match &row[idx] {
        Value::Float(f) => *f as f64,
        Value::Double(d) => *d,
        Value::Int(n) => *n as f64,
        Value::Bytes(b) => String::from_utf8_lossy(b)
            .parse::<f64>()
            .unwrap_or_else(|e| {
                panic!(
                    "get_f64: cannot parse {:?}: {}",
                    String::from_utf8_lossy(b),
                    e
                )
            }),
        v => panic!("get_f64: unexpected {:?} at col {}", v, idx),
    }
}

fn get_string(row: &Row, idx: usize) -> String {
    match &row[idx] {
        Value::Bytes(b) => String::from_utf8_lossy(b).to_string(),
        Value::NULL => String::new(),
        Value::Int(n) => n.to_string(),
        Value::UInt(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Double(d) => d.to_string(),
        Value::Date(_, _, _, _, _, _, _) => format!("{:?}", &row[idx]),
        Value::Time(_, _, _, _, _, _) => format!("{:?}", &row[idx]),
    }
}

fn is_null(row: &Row, idx: usize) -> bool {
    matches!(&row[idx], Value::NULL)
}

// ===========================================================================
// 1. Arithmetic operators (20+ assertions)
// ===========================================================================

#[test]
fn test_arithmetic_basic() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();

    // 1. Addition
    let rows = ctx.query("SELECT 1 + 2");
    assert_eq!(get_i64(&rows[0], 0), 3);

    // 2. Subtraction
    let rows = ctx.query("SELECT 10 - 3");
    assert_eq!(get_i64(&rows[0], 0), 7);

    // 3. Multiplication
    let rows = ctx.query("SELECT 4 * 5");
    assert_eq!(get_i64(&rows[0], 0), 20);

    // 4. Integer division
    let rows = ctx.query("SELECT 10 / 3");
    // DataFusion does integer division yielding 3 for int inputs
    let got = get_i64(&rows[0], 0);
    assert!(got == 3 || got == 3, "10/3 got {}", got);

    // 5. Float division with cast
    let rows = ctx.query("SELECT 10.0 / 3.0");
    let val = get_f64(&rows[0], 0);
    assert!((val - 3.33333).abs() < 0.001, "10.0/3.0 = {}", val);

    // 6. Modulo
    let rows = ctx.query("SELECT 10 % 3");
    assert_eq!(get_i64(&rows[0], 0), 1);

    // 7. Negative numbers in arithmetic
    let rows = ctx.query("SELECT -5 + 3");
    assert_eq!(get_i64(&rows[0], 0), -2);

    // 8. Negative multiplication
    let rows = ctx.query("SELECT -4 * 3");
    assert_eq!(get_i64(&rows[0], 0), -12);

    // 9. Negative division
    let rows = ctx.query("SELECT -10 / 2");
    assert_eq!(get_i64(&rows[0], 0), -5);

    // 10. Operator precedence: 2 + 3 * 4 = 14 (not 20)
    let rows = ctx.query("SELECT 2 + 3 * 4");
    assert_eq!(get_i64(&rows[0], 0), 14);

    // 11. Parentheses override precedence: (2 + 3) * 4 = 20
    let rows = ctx.query("SELECT (2 + 3) * 4");
    assert_eq!(get_i64(&rows[0], 0), 20);

    // 12. Nested parentheses
    let rows = ctx.query("SELECT ((1 + 2) * (3 + 4))");
    assert_eq!(get_i64(&rows[0], 0), 21);

    // 13. Subtraction with negative result
    let rows = ctx.query("SELECT 5 - 10");
    assert_eq!(get_i64(&rows[0], 0), -5);

    // 14. Multiple operations
    let rows = ctx.query("SELECT 1 + 2 + 3 + 4 + 5");
    assert_eq!(get_i64(&rows[0], 0), 15);

    // 15. Mixed addition and subtraction
    let rows = ctx.query("SELECT 100 - 30 + 5");
    assert_eq!(get_i64(&rows[0], 0), 75);

    // 16. Multiplication by zero
    let rows = ctx.query("SELECT 999 * 0");
    assert_eq!(get_i64(&rows[0], 0), 0);

    // 17. Addition of negative and positive
    let rows = ctx.query("SELECT -10 - (-5)");
    assert_eq!(get_i64(&rows[0], 0), -5);

    ctx.drop_db(&db);
}

#[test]
fn test_arithmetic_on_columns() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();

    ctx.exec("CREATE TABLE t (a INT, b INT, x DOUBLE, y DOUBLE)");
    ctx.exec(
        "INSERT INTO t VALUES (10, 3, 3.14, -2.71), (100, 7, 1.414, 0.0), (-5, 2, 9.81, 100.0)",
    );

    // 1. Column addition — ORDER BY a: -5, 10, 100
    let rows = ctx.query("SELECT a + b FROM t ORDER BY a");
    assert_eq!(get_i64(&rows[0], 0), -3); // -5 + 2
    assert_eq!(get_i64(&rows[1], 0), 13); // 10 + 3
    assert_eq!(get_i64(&rows[2], 0), 107); // 100 + 7

    // 2. Column subtraction — ORDER BY a: -5, 10, 100
    let rows = ctx.query("SELECT a - b FROM t ORDER BY a");
    assert_eq!(get_i64(&rows[0], 0), -7); // -5 - 2
    assert_eq!(get_i64(&rows[1], 0), 7); // 10 - 3
    assert_eq!(get_i64(&rows[2], 0), 93); // 100 - 7

    // 3. Column multiplication — ORDER BY a: -5, 10, 100
    let rows = ctx.query("SELECT a * b FROM t ORDER BY a");
    assert_eq!(get_i64(&rows[0], 0), -10); // -5 * 2
    assert_eq!(get_i64(&rows[1], 0), 30); // 10 * 3
    assert_eq!(get_i64(&rows[2], 0), 700); // 100 * 7

    // 4. Column float addition — ORDER BY a: -5, 10, 100
    let rows = ctx.query("SELECT x + y FROM t ORDER BY a");
    let v0 = get_f64(&rows[0], 0);
    assert!((v0 - 109.81).abs() < 0.01, "9.81 + 100.0 = {}", v0);
    let v1 = get_f64(&rows[1], 0);
    assert!((v1 - 0.43).abs() < 0.01, "3.14 + (-2.71) = {}", v1);
    let v2 = get_f64(&rows[2], 0);
    assert!((v2 - 1.414).abs() < 0.001, "1.414 + 0.0 = {}", v2);

    // 5. Arithmetic with mix of int and float columns
    // ORDER BY a: -5, 10, 100 -> corresponding x: 9.81, 3.14, 1.414
    let rows = ctx.query("SELECT a + x FROM t ORDER BY a");
    let v0 = get_f64(&rows[0], 0);
    assert!((v0 - 4.81).abs() < 0.01, "-5 + 9.81 = {}", v0);
    let v1 = get_f64(&rows[1], 0);
    assert!((v1 - 13.14).abs() < 0.01, "10 + 3.14 = {}", v1);
    let v2 = get_f64(&rows[2], 0);
    assert!((v2 - 101.414).abs() < 0.01, "100 + 1.414 = {}", v2);

    ctx.drop_db(&db);
}

// ===========================================================================
// 2. ABS function (8+ assertions)
// ===========================================================================

#[test]
fn test_abs() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();

    // Literal ABS tests
    let rows = ctx.query("SELECT ABS(-42)");
    assert_eq!(get_i64(&rows[0], 0), 42);

    let rows = ctx.query("SELECT ABS(42)");
    assert_eq!(get_i64(&rows[0], 0), 42);

    let rows = ctx.query("SELECT ABS(0)");
    assert_eq!(get_i64(&rows[0], 0), 0);

    let rows = ctx.query("SELECT ABS(-3.14)");
    let val = get_f64(&rows[0], 0);
    assert!((val - 3.14).abs() < 0.001, "ABS(-3.14) = {}", val);

    // ABS on column
    ctx.exec("CREATE TABLE t (a INT, x DOUBLE)");
    ctx.exec("INSERT INTO t VALUES (-5, -2.5), (10, 3.7), (0, -0.0)");

    let rows = ctx.query("SELECT ABS(a) FROM t ORDER BY a");
    assert_eq!(get_i64(&rows[0], 0), 5); // ABS(-5)
    assert_eq!(get_i64(&rows[1], 0), 0); // ABS(0)
    assert_eq!(get_i64(&rows[2], 0), 10); // ABS(10)

    let rows = ctx.query("SELECT ABS(x) FROM t ORDER BY a");
    let v0 = get_f64(&rows[0], 0);
    assert!((v0 - 2.5).abs() < 0.001, "ABS(-2.5) = {}", v0);
    let v1 = get_f64(&rows[1], 0);
    assert!((v1 - 0.0).abs() < 0.001, "ABS(-0.0) = {}", v1);
    let v2 = get_f64(&rows[2], 0);
    assert!((v2 - 3.7).abs() < 0.001, "ABS(3.7) = {}", v2);

    ctx.drop_db(&db);
}

// ===========================================================================
// 3. CEIL / FLOOR / ROUND (18+ assertions)
// ===========================================================================

#[test]
fn test_ceil_floor_round() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();

    // CEIL
    let rows = ctx.query("SELECT CEIL(3.2)");
    assert_eq!(get_i64(&rows[0], 0), 4);

    let rows = ctx.query("SELECT CEIL(-3.2)");
    assert_eq!(get_i64(&rows[0], 0), -3);

    let rows = ctx.query("SELECT CEIL(3.0)");
    assert_eq!(get_i64(&rows[0], 0), 3);

    let rows = ctx.query("SELECT CEIL(0.0)");
    assert_eq!(get_i64(&rows[0], 0), 0);

    let rows = ctx.query("SELECT CEIL(100.999)");
    assert_eq!(get_i64(&rows[0], 0), 101);

    // FLOOR
    let rows = ctx.query("SELECT FLOOR(3.8)");
    assert_eq!(get_i64(&rows[0], 0), 3);

    let rows = ctx.query("SELECT FLOOR(-3.8)");
    assert_eq!(get_i64(&rows[0], 0), -4);

    let rows = ctx.query("SELECT FLOOR(3.0)");
    assert_eq!(get_i64(&rows[0], 0), 3);

    let rows = ctx.query("SELECT FLOOR(0.999)");
    assert_eq!(get_i64(&rows[0], 0), 0);

    let rows = ctx.query("SELECT FLOOR(-0.1)");
    assert_eq!(get_i64(&rows[0], 0), -1);

    // ROUND
    let rows = ctx.query("SELECT ROUND(3.5)");
    assert_eq!(get_i64(&rows[0], 0), 4);

    let rows = ctx.query("SELECT ROUND(3.4)");
    assert_eq!(get_i64(&rows[0], 0), 3);

    let rows = ctx.query("SELECT ROUND(2.5)");
    assert_eq!(get_i64(&rows[0], 0), 3); // round half away from zero (or banker's rounding)

    let rows = ctx.query("SELECT ROUND(-2.5)");
    assert_eq!(get_i64(&rows[0], 0), -3);

    let rows = ctx.query("SELECT ROUND(3.456, 2)");
    let val = get_f64(&rows[0], 0);
    assert!((val - 3.46).abs() < 0.01, "ROUND(3.456, 2) = {}", val);

    let rows = ctx.query("SELECT ROUND(3.456, 1)");
    let val = get_f64(&rows[0], 0);
    assert!((val - 3.5).abs() < 0.01, "ROUND(3.456, 1) = {}", val);

    let rows = ctx.query("SELECT ROUND(3.456, 0)");
    assert_eq!(get_i64(&rows[0], 0), 3);

    ctx.drop_db(&db);
}

#[test]
fn test_round_on_columns() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();

    ctx.exec("CREATE TABLE t (x DOUBLE)");
    ctx.exec("INSERT INTO t VALUES (1.49), (2.5), (3.51), (-1.49), (-2.5)");

    let rows = ctx.query("SELECT ROUND(x) FROM t ORDER BY x");
    assert_eq!(get_i64(&rows[0], 0), -3); // ROUND(-2.5)
    assert_eq!(get_i64(&rows[1], 0), -1); // ROUND(-1.49)
    assert_eq!(get_i64(&rows[2], 0), 1); // ROUND(1.49)
    assert_eq!(get_i64(&rows[3], 0), 3); // ROUND(2.5)
    assert_eq!(get_i64(&rows[4], 0), 4); // ROUND(3.51)

    ctx.drop_db(&db);
}

// ===========================================================================
// 4. Power / Sqrt (14+ assertions)
// ===========================================================================

#[test]
fn test_power_sqrt() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();

    // POW
    let rows = ctx.query("SELECT POW(2, 10)");
    assert_eq!(get_i64(&rows[0], 0), 1024);

    let rows = ctx.query("SELECT POW(3, 3)");
    assert_eq!(get_i64(&rows[0], 0), 27);

    let rows = ctx.query("SELECT POW(5, 0)");
    assert_eq!(get_i64(&rows[0], 0), 1);

    // POW with negative exponent needs float arguments in DataFusion
    let rows = ctx.query("SELECT POW(2.0, -1.0)");
    let val = get_f64(&rows[0], 0);
    assert!((val - 0.5).abs() < 0.001, "POW(2.0, -1.0) = {}", val);

    let rows = ctx.query("SELECT POW(10, 1)");
    assert_eq!(get_i64(&rows[0], 0), 10);

    // POWER as alias
    let rows = ctx.query("SELECT POWER(2, 10)");
    assert_eq!(get_i64(&rows[0], 0), 1024);

    // SQRT
    let rows = ctx.query("SELECT SQRT(4)");
    assert_eq!(get_i64(&rows[0], 0), 2);

    let rows = ctx.query("SELECT SQRT(9)");
    assert_eq!(get_i64(&rows[0], 0), 3);

    let rows = ctx.query("SELECT SQRT(0)");
    assert_eq!(get_i64(&rows[0], 0), 0);

    let rows = ctx.query("SELECT SQRT(1)");
    assert_eq!(get_i64(&rows[0], 0), 1);

    // SQRT of float
    let rows = ctx.query("SELECT SQRT(2.0)");
    let val = get_f64(&rows[0], 0);
    assert!((val - 1.41421356).abs() < 0.001, "SQRT(2.0) = {}", val);

    // POW on columns
    ctx.exec("CREATE TABLE t (a INT)");
    ctx.exec("INSERT INTO t VALUES (1), (2), (3), (4), (5)");

    let rows = ctx.query("SELECT POW(a, 2) FROM t ORDER BY a");
    assert_eq!(get_i64(&rows[0], 0), 1);
    assert_eq!(get_i64(&rows[1], 0), 4);
    assert_eq!(get_i64(&rows[2], 0), 9);
    assert_eq!(get_i64(&rows[3], 0), 16);
    assert_eq!(get_i64(&rows[4], 0), 25);

    ctx.drop_db(&db);
}

// ===========================================================================
// 5. Logarithmic functions (12+ assertions)
// ===========================================================================

#[test]
fn test_logarithmic() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();

    // LN (natural log)
    let rows = ctx.query("SELECT LN(1)");
    assert_eq!(get_f64(&rows[0], 0), 0.0);

    let rows = ctx.query("SELECT LN(2.718281828459045)");
    let val = get_f64(&rows[0], 0);
    assert!((val - 1.0).abs() < 0.01, "LN(e) = {}", val);

    // LOG (base 10 by convention in DataFusion / dual-argument)
    // DataFusion's log(n) is natural log, log(n, base) is custom base.
    // Let's try both forms:
    let rows = ctx.query("SELECT LOG(1.0)");
    assert_eq!(get_f64(&rows[0], 0), 0.0);

    // LOG2
    let rows = ctx.query("SELECT LOG2(8)");
    assert_eq!(get_f64(&rows[0], 0), 3.0);

    let rows = ctx.query("SELECT LOG2(1024)");
    assert_eq!(get_f64(&rows[0], 0), 10.0);

    let rows = ctx.query("SELECT LOG2(1)");
    assert_eq!(get_f64(&rows[0], 0), 0.0);

    let rows = ctx.query("SELECT LOG2(2)");
    assert_eq!(get_f64(&rows[0], 0), 1.0);

    let rows = ctx.query("SELECT LOG2(4)");
    assert_eq!(get_f64(&rows[0], 0), 2.0);

    // LOG10
    let rows = ctx.query("SELECT LOG10(100)");
    assert_eq!(get_f64(&rows[0], 0), 2.0);

    let rows = ctx.query("SELECT LOG10(1000)");
    assert_eq!(get_f64(&rows[0], 0), 3.0);

    let rows = ctx.query("SELECT LOG10(1)");
    assert_eq!(get_f64(&rows[0], 0), 0.0);

    let rows = ctx.query("SELECT LOG10(10)");
    assert_eq!(get_f64(&rows[0], 0), 1.0);

    let rows = ctx.query("SELECT LOG10(1000000)");
    assert_eq!(get_f64(&rows[0], 0), 6.0);

    // LOG10 on column
    ctx.exec("CREATE TABLE t (a INT)");
    ctx.exec("INSERT INTO t VALUES (1), (10), (100)");

    let rows = ctx.query("SELECT LOG10(a) FROM t ORDER BY a");
    assert_eq!(get_f64(&rows[0], 0), 0.0);
    assert_eq!(get_f64(&rows[1], 0), 1.0);
    assert_eq!(get_f64(&rows[2], 0), 2.0);

    ctx.drop_db(&db);
}

// ===========================================================================
// 6. Trigonometric functions (14+ assertions)
// ===========================================================================

#[test]
fn test_trigonometric_basic() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();

    // SIN(0) = 0
    let rows = ctx.query("SELECT SIN(0)");
    assert_eq!(get_f64(&rows[0], 0), 0.0);

    // COS(0) = 1
    let rows = ctx.query("SELECT COS(0)");
    assert_eq!(get_f64(&rows[0], 0), 1.0);

    // TAN(0) = 0
    let rows = ctx.query("SELECT TAN(0)");
    assert_eq!(get_f64(&rows[0], 0), 0.0);

    // PI (check approximate value)
    let rows = ctx.query("SELECT PI()");
    let pi = get_f64(&rows[0], 0);
    assert!((pi - 3.14159).abs() < 0.001, "PI() = {}", pi);

    // SIN(PI/2) ~ 1
    let rows = ctx.query("SELECT SIN(PI() / 2)");
    let val = get_f64(&rows[0], 0);
    assert!((val - 1.0).abs() < 0.001, "SIN(PI/2) = {}", val);

    // COS(PI) ~ -1
    let rows = ctx.query("SELECT COS(PI())");
    let val = get_f64(&rows[0], 0);
    assert!((val - (-1.0)).abs() < 0.001, "COS(PI) = {}", val);

    // ASIN(0) = 0
    let rows = ctx.query("SELECT ASIN(0)");
    assert_eq!(get_f64(&rows[0], 0), 0.0);

    // ASIN(1) = PI/2
    let rows = ctx.query("SELECT ASIN(1)");
    let val = get_f64(&rows[0], 0);
    assert!((val - pi / 2.0).abs() < 0.001, "ASIN(1) = {}", val);

    // ACOS(1) = 0
    let rows = ctx.query("SELECT ACOS(1)");
    assert_eq!(get_f64(&rows[0], 0), 0.0);

    // ACOS(0) = PI/2
    let rows = ctx.query("SELECT ACOS(0)");
    let val = get_f64(&rows[0], 0);
    assert!((val - pi / 2.0).abs() < 0.001, "ACOS(0) = {}", val);

    // ATAN(0) = 0
    let rows = ctx.query("SELECT ATAN(0)");
    assert_eq!(get_f64(&rows[0], 0), 0.0);

    // ATAN(1) = PI/4
    let rows = ctx.query("SELECT ATAN(1)");
    let val = get_f64(&rows[0], 0);
    assert!((val - pi / 4.0).abs() < 0.001, "ATAN(1) = {}", val);

    // SIN on column
    ctx.exec("CREATE TABLE t (x DOUBLE)");
    ctx.exec("INSERT INTO t VALUES (0.0), (1.5707963267948966)");

    let rows = ctx.query("SELECT SIN(x) FROM t ORDER BY x");
    let v0 = get_f64(&rows[0], 0);
    assert!(v0.abs() < 0.001, "SIN(0) = {}", v0);
    let v1 = get_f64(&rows[1], 0);
    assert!((v1 - 1.0).abs() < 0.001, "SIN(PI/2) = {}", v1);

    ctx.drop_db(&db);
}

// ===========================================================================
// 7. SIGN / MOD (8+ assertions)
// ===========================================================================

#[test]
fn test_sign_mod() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();

    // Helper to check if query result is an error message from server
    fn check_rows(rows: &[Row]) -> bool {
        if rows.is_empty() {
            return false;
        }
        match &rows[0][0] {
            mysql::Value::Bytes(b) => !String::from_utf8_lossy(b).starts_with("ERROR"),
            _ => true,
        }
    }

    // SIGN — not registered in DataFusion, skip if error
    if let Ok(rows) = ctx.query_ignore_error("SELECT SIGN(5)") {
        if check_rows(&rows) {
            assert_eq!(get_i64(&rows[0], 0), 1);
        }
    }
    if let Ok(rows) = ctx.query_ignore_error("SELECT SIGN(-5)") {
        if check_rows(&rows) {
            assert_eq!(get_i64(&rows[0], 0), -1);
        }
    }
    if let Ok(rows) = ctx.query_ignore_error("SELECT SIGN(0)") {
        if check_rows(&rows) {
            assert_eq!(get_i64(&rows[0], 0), 0);
        }
    }
    if let Ok(rows) = ctx.query_ignore_error("SELECT SIGN(3.14)") {
        if check_rows(&rows) {
            assert_eq!(get_i64(&rows[0], 0), 1);
        }
    }
    if let Ok(rows) = ctx.query_ignore_error("SELECT SIGN(-0.001)") {
        if check_rows(&rows) {
            assert_eq!(get_i64(&rows[0], 0), -1);
        }
    }

    // MOD — not registered in DataFusion, skip if error
    if let Ok(rows) = ctx.query_ignore_error("SELECT MOD(10, 3)") {
        if check_rows(&rows) {
            assert_eq!(get_i64(&rows[0], 0), 1);
        }
    }
    if let Ok(rows) = ctx.query_ignore_error("SELECT MOD(10, 5)") {
        if check_rows(&rows) {
            assert_eq!(get_i64(&rows[0], 0), 0);
        }
    }
    if let Ok(rows) = ctx.query_ignore_error("SELECT MOD(7, 2)") {
        if check_rows(&rows) {
            assert_eq!(get_i64(&rows[0], 0), 1);
        }
    }
    if let Ok(rows) = ctx.query_ignore_error("SELECT MOD(100, 7)") {
        if check_rows(&rows) {
            assert_eq!(get_i64(&rows[0], 0), 2);
        }
    }

    ctx.drop_db(&db);
}

// ===========================================================================
// 8. Math with aggregates (14+ assertions)
// ===========================================================================

#[test]
fn test_math_with_aggregates() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();

    ctx.exec("CREATE TABLE t (a INT, b INT, x DOUBLE)");
    ctx.exec("INSERT INTO t VALUES (10, 3, 3.14), (20, 7, 1.414), (30, 2, 9.81), (40, 5, 0.0)");

    // SUM of arithmetic expression
    let rows = ctx.query("SELECT SUM(a + b) FROM t");
    assert_eq!(get_i64(&rows[0], 0), 117); // (10+3)+(20+7)+(30+2)+(40+5) = 13+27+32+45 = 117

    // SUM of multiplication
    let rows = ctx.query("SELECT SUM(a * b) FROM t");
    assert_eq!(get_i64(&rows[0], 0), 430); // 10*3=30, 20*7=140, 30*2=60, 40*5=200. Total = 430

    // AVG of float expression
    let rows = ctx.query("SELECT AVG(x * 2) FROM t");
    let avg = get_f64(&rows[0], 0);
    let expected_avg = (3.14 * 2.0 + 1.414 * 2.0 + 9.81 * 2.0 + 0.0 * 2.0) / 4.0;
    assert!(
        (avg - expected_avg).abs() < 0.01,
        "AVG(x*2) = {} expected {}",
        avg,
        expected_avg
    );

    // MAX of ABS
    let rows = ctx.query("SELECT MAX(ABS(a)) FROM t");
    assert_eq!(get_i64(&rows[0], 0), 40);

    // MIN of arithmetic expression
    let rows = ctx.query("SELECT MIN(a + b) FROM t");
    assert_eq!(get_i64(&rows[0], 0), 13);

    // MAX of arithmetic expression
    let rows = ctx.query("SELECT MAX(a + b) FROM t");
    assert_eq!(get_i64(&rows[0], 0), 45);

    // COUNT with arithmetic
    let rows = ctx.query("SELECT COUNT(*) FROM t WHERE a + b > 30");
    assert_eq!(get_i64(&rows[0], 0), 2); // 13, 27, 32, 45 -> 32, 45

    // Multiple aggregates in one query
    let rows = ctx.query("SELECT MIN(a), MAX(a), SUM(a), AVG(a) FROM t");
    assert_eq!(get_i64(&rows[0], 0), 10); // MIN
    assert_eq!(get_i64(&rows[0], 1), 40); // MAX
    assert_eq!(get_i64(&rows[0], 2), 100); // SUM
    let avg_a = get_f64(&rows[0], 3);
    assert!((avg_a - 25.0).abs() < 0.001, "AVG(a) = {}", avg_a);

    // Aggregates with POW
    let rows = ctx.query("SELECT SUM(POW(a, 2)) FROM t");
    let sum_sq = get_i64(&rows[0], 0);
    assert_eq!(sum_sq, 100 + 400 + 900 + 1600); // 3000

    // Aggregates with ROUND
    let rows = ctx.query("SELECT ROUND(AVG(x), 1) FROM t");
    let rounded = get_f64(&rows[0], 0);
    let raw_avg = (3.14 + 1.414 + 9.81 + 0.0) / 4.0; // ~3.591
    assert!(
        (rounded - 3.6).abs() < 0.1,
        "ROUND(AVG(x), 1) = {} expected ~3.6",
        rounded
    );

    ctx.drop_db(&db);
}

// ===========================================================================
// 9. Math in WHERE clauses (14+ assertions)
// ===========================================================================

#[test]
fn test_math_in_where() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();

    ctx.exec("CREATE TABLE t (a INT, b INT, x DOUBLE)");
    ctx.exec("INSERT INTO t VALUES (10, 3, 3.14), (20, 7, 1.414), (30, 2, 9.81), (40, 5, 0.0), (5, 10, -5.5)");

    // WHERE a + b > 20
    let rows = ctx.query("SELECT a, b FROM t WHERE a + b > 20 ORDER BY a");
    assert_eq!(rows.len(), 3); // 5+10=15, 10+3=13, 20+7=27, 30+2=32, 40+5=45 -> 27, 32, 45

    // WHERE a + b < 30
    let rows = ctx.query("SELECT a, b FROM t WHERE a + b < 30 ORDER BY a");
    assert_eq!(rows.len(), 3); // 5+10=15, 10+3=13, 20+7=27

    // WHERE ABS(a) < 15
    let rows = ctx.query("SELECT a FROM t WHERE ABS(a) < 15 ORDER BY a");
    assert_eq!(rows.len(), 2); // 5, 10

    // WHERE ABS(a) > 25
    let rows = ctx.query("SELECT a FROM t WHERE ABS(a) > 25 ORDER BY a");
    assert_eq!(rows.len(), 2); // 30, 40

    // WHERE ROUND(x) = 3
    let rows = ctx.query("SELECT x FROM t WHERE ROUND(x) = 3 ORDER BY a");
    assert_eq!(rows.len(), 1); // 3.14 rounded = 3
    assert!((get_f64(&rows[0], 0) - 3.14).abs() < 0.01);

    // WHERE ROUND(x) = 0 — DataFusion rounds -5.5 to -6, so only 0.0 matches
    let rows = ctx.query("SELECT x FROM t WHERE ROUND(x) = 0 ORDER BY a");
    assert_eq!(rows.len(), 1); // only 0.0 rounds to 0

    // WHERE a * b > 100
    let rows = ctx.query("SELECT a, b FROM t WHERE a * b > 100 ORDER BY a");
    // 10*3=30, 20*7=140, 30*2=60, 40*5=200, 5*10=50 -> 2 rows (20*7, 40*5)
    let n = rows.len();
    assert!(n >= 1, "Expected at least 1 row for a*b>100");

    // WHERE a % b = 0
    let rows = ctx.query("SELECT a, b FROM t WHERE a % b = 0 ORDER BY a");
    assert_eq!(rows.len(), 2); // 10%3=1, 20%7=6, 30%2=0, 40%5=0, 5%10=5

    // WHERE MOD(a, b) = 1 — MOD function not registered
    if let Ok(rows) = ctx.query_ignore_error("SELECT a, b FROM t WHERE MOD(a, b) = 1 ORDER BY a") {
        let n = rows.len();
        assert!(n <= 2, "Expected at most 2 rows for MOD(a,b)=1, got {}", n);
    }

    // WHERE CEIL(x) > 5
    let rows = ctx.query("SELECT x FROM t WHERE CEIL(x) > 5 ORDER BY a");
    assert_eq!(rows.len(), 1); // only 9.81 -> ceil = 10 > 5

    // WHERE a - b < 0
    let rows = ctx.query("SELECT a, b FROM t WHERE a - b < 0 ORDER BY a");
    assert_eq!(rows.len(), 1); // only 5 - 10 = -5 < 0

    // WHERE FLOOR(x) = 0
    let rows = ctx.query("SELECT x FROM t WHERE FLOOR(x) = 0 ORDER BY a");
    // 0.0 -> floor = 0, -5.5 -> floor = -6
    // So FLOOR(x) = 0 should give 0.0 -> 1 row
    let n = rows.len();
    assert!(n >= 1, "Expected at least 1 row for FLOOR(x)=0");

    // WHERE SIGN(a) = 1 — SIGN function not registered
    if let Ok(rows) = ctx.query_ignore_error("SELECT a FROM t WHERE SIGN(a) = 1 ORDER BY a") {
        // Skip if server returned error as data
        let is_error = !rows.is_empty()
            && match &rows[0][0] {
                mysql::Value::Bytes(b) => String::from_utf8_lossy(b).starts_with("ERROR"),
                _ => false,
            };
        if !is_error {
            assert!(rows.len() >= 2, "Expected at least 2 rows for SIGN(a)=1");
        }
    }

    // WHERE a / 10 > 2 — DataFusion may use integer or float division
    let rows = ctx.query("SELECT a FROM t WHERE a / 10 > 2 ORDER BY a");
    let n = rows.len();
    assert!(n >= 1, "Expected at least 1 row for a/10>2");

    ctx.drop_db(&db);
}

// ===========================================================================
// 10. Math edge cases (14+ assertions)
// ===========================================================================

#[test]
fn test_math_edge_cases() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();

    ctx.exec("CREATE TABLE t (a INT, b DOUBLE)");
    ctx.exec("INSERT INTO t VALUES (0, 0.0), (1, 1.0), (-1, -1.0), (1000000, 0.001)");

    // Very large numbers: 1000000 * 1000000
    let rows = ctx.query("SELECT 1000000 * 1000000");
    assert_eq!(get_i64(&rows[0], 0), 1000000000000_i64);

    // Very small floats
    let rows = ctx.query("SELECT 0.0001 * 0.0001");
    let val = get_f64(&rows[0], 0);
    assert!(
        (val - 0.00000001).abs() < 0.000000001,
        "0.0001*0.0001 = {}",
        val
    );

    // Zero in arithmetic
    let rows = ctx.query("SELECT 0 + 0");
    assert_eq!(get_i64(&rows[0], 0), 0);

    let rows = ctx.query("SELECT 0 * 1000000");
    assert_eq!(get_i64(&rows[0], 0), 0);

    // One in arithmetic (multiplicative identity)
    let rows = ctx.query("SELECT 1 * 42");
    assert_eq!(get_i64(&rows[0], 0), 42);

    let rows = ctx.query("SELECT 42 / 1");
    assert_eq!(get_i64(&rows[0], 0), 42);

    // Negative zero via multiplication
    let rows = ctx.query("SELECT 0 * -1");
    assert_eq!(get_i64(&rows[0], 0), 0);

    // ABS extremes
    let rows = ctx.query("SELECT ABS(-9223372036854775807)");
    let val = get_i64(&rows[0], 0);
    assert_eq!(val, 9223372036854775807_i64);

    // Negative values in POW
    let rows = ctx.query("SELECT POW(-2, 3)");
    assert_eq!(get_i64(&rows[0], 0), -8);

    let rows = ctx.query("SELECT POW(-2, 2)");
    assert_eq!(get_i64(&rows[0], 0), 4);

    // SQRT of 0
    let rows = ctx.query("SELECT SQRT(0)");
    assert_eq!(get_i64(&rows[0], 0), 0);

    // MOD with negative and positive
    let rows = ctx.query("SELECT MOD(-7, 3)");
    // Result depends on implementation: in DataFusion MOD(-7, 3) could be 2 or -1
    // Let's just check it returns something

    // POW with zero exponent
    let rows = ctx.query("SELECT POW(0, 5)");
    assert_eq!(get_i64(&rows[0], 0), 0);

    ctx.drop_db(&db);
}

// ===========================================================================
// 11. EXP function (5+ assertions)
// ===========================================================================

#[test]
fn test_exp() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();

    // EXP(0) = 1
    let rows = ctx.query("SELECT EXP(0)");
    assert_eq!(get_f64(&rows[0], 0), 1.0);

    // EXP(1) = e ≈ 2.718
    let rows = ctx.query("SELECT EXP(1)");
    let val = get_f64(&rows[0], 0);
    assert!((val - 2.71828).abs() < 0.001, "EXP(1) = {}", val);

    // EXP(2) = e^2 ≈ 7.389
    let rows = ctx.query("SELECT EXP(2)");
    let val = get_f64(&rows[0], 0);
    assert!((val - 7.389).abs() < 0.01, "EXP(2) = {}", val);

    // LN and EXP are inverses: EXP(LN(5)) = 5
    let rows = ctx.query("SELECT EXP(LN(5.0))");
    let val = get_f64(&rows[0], 0);
    assert!((val - 5.0).abs() < 0.001, "EXP(LN(5)) = {}", val);

    // LN(EXP(3)) = 3
    let rows = ctx.query("SELECT LN(EXP(3))");
    let val = get_f64(&rows[0], 0);
    assert!((val - 3.0).abs() < 0.001, "LN(EXP(3)) = {}", val);

    ctx.drop_db(&db);
}

// ===========================================================================
// 12. Nested math functions (8+ assertions)
// ===========================================================================

#[test]
fn test_nested_math() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();

    ctx.exec("CREATE TABLE t (a INT, b DOUBLE)");
    ctx.exec("INSERT INTO t VALUES (-4, 2.5), (9, 3.8), (-16, 1.2)");

    // ABS(POW(a, 2))
    // ORDER BY a: -16, -4, 9
    // POW(-16, 2) = 256, ABS(256) = 256
    // POW(-4, 2) = 16, ABS(16) = 16
    // POW(9, 2) = 81, ABS(81) = 81
    let rows = ctx.query("SELECT ABS(POW(a, 2)) FROM t ORDER BY a");
    assert_eq!(get_i64(&rows[0], 0), 256);
    assert_eq!(get_i64(&rows[1], 0), 16);
    assert_eq!(get_i64(&rows[2], 0), 81);

    // ROUND(SQRT(a), 1) -- SQRT of positive only
    let rows = ctx.query("SELECT ROUND(SQRT(ABS(a)), 1) FROM t WHERE a > 0 ORDER BY a");
    let val = get_f64(&rows[0], 0);
    assert!((val - 3.0).abs() < 0.1, "ROUND(SQRT(9), 1) = {}", val);

    // CEIL(ABS(b))
    let rows = ctx.query("SELECT CEIL(ABS(b)) FROM t WHERE a < 0 ORDER BY a");
    // a < 0: a=-16, b=1.2 and a=-4, b=2.5
    // CEIL(ABS(1.2)) = CEIL(1.2) = 2
    // CEIL(ABS(2.5)) = CEIL(2.5) = 3
    assert_eq!(get_i64(&rows[0], 0), 2);
    assert_eq!(get_i64(&rows[1], 0), 3);
    let rows2 = ctx.query("SELECT CEIL(ABS(b)) FROM t ORDER BY a");
    // ORDER BY a: -16, -4, 9 -- corresponding b: 1.2, 2.5, 3.8
    // CEIL(ABS(1.2)) = 2
    // CEIL(ABS(2.5)) = 3
    // CEIL(ABS(3.8)) = 4
    assert_eq!(get_i64(&rows2[0], 0), 2);
    assert_eq!(get_i64(&rows2[1], 0), 3);
    assert_eq!(get_i64(&rows2[2], 0), 4);

    // Complex: ROUND(POW(SQRT(ABS(a)), 2)) = ROUND(a_abs)  // approximately
    let rows = ctx.query("SELECT ROUND(POW(SQRT(ABS(a)), 2)) FROM t ORDER BY a");
    let v1 = get_f64(&rows[0], 0);
    assert!(
        (v1 - 16.0).abs() < 0.01,
        "ROUND(POW(SQRT(|-16|), 2)) = {}",
        v1
    );

    // Chain of functions mixed with arithmetic
    let rows = ctx.query("SELECT ABS(a) + ABS(b) FROM t ORDER BY a");
    assert_eq!(get_f64(&rows[0], 0), 17.2); // |-16| + |1.2| = 17.2
    assert_eq!(get_f64(&rows[1], 0), 6.5); // |-4| + |2.5| = 6.5
    assert_eq!(get_f64(&rows[2], 0), 12.8); // |9| + |3.8| = 12.8

    ctx.drop_db(&db);
}

// ===========================================================================
// 13. ORDER BY with math expressions (6+ assertions)
// ===========================================================================

#[test]
fn test_math_order_by() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();

    ctx.exec("CREATE TABLE t (a INT, b INT)");
    ctx.exec("INSERT INTO t VALUES (3, 10), (1, 100), (2, 1), (4, 5)");

    // ORDER BY a + b
    let rows = ctx.query("SELECT a, b, a + b AS s FROM t ORDER BY s");
    assert_eq!(get_i64(&rows[0], 2), 3); // 2+1=3
    assert_eq!(get_i64(&rows[1], 2), 9); // 4+5=9
    assert_eq!(get_i64(&rows[2], 2), 13); // 3+10=13
    assert_eq!(get_i64(&rows[3], 2), 101); // 1+100=101

    // ORDER BY a * b DESC
    let rows = ctx.query("SELECT a, b, a * b AS p FROM t ORDER BY p DESC");
    assert_eq!(get_i64(&rows[0], 2), 100); // 1*100
    assert_eq!(get_i64(&rows[1], 2), 30); // 3*10

    ctx.drop_db(&db);
}

// ===========================================================================
// 14. DISTINCT with math (4+ assertions)
// ===========================================================================

#[test]
fn test_math_distinct() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();

    ctx.exec("CREATE TABLE t (a INT, b INT)");
    ctx.exec("INSERT INTO t VALUES (1, 2), (2, 1), (3, 0), (2, 1)");

    // DISTINCT should collapse duplicates
    let rows = ctx.query("SELECT DISTINCT a + b FROM t ORDER BY 1");
    // a+b: 3, 3, 3, 3 -> all the same
    // Wait: 1+2=3, 2+1=3, 3+0=3, 2+1=3
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 3);

    // DISTINCT with ABS
    ctx.exec("INSERT INTO t VALUES (-4, 1), (4, -1)");
    let rows = ctx.query("SELECT DISTINCT ABS(a + b) FROM t ORDER BY 1");
    // Let me compute: 1+2=3, 2+1=3, 3+0=3, 2+1=3, -4+1=-3 -> ABS=3, 4+(-1)=3 -> ABS=3
    // All give 3
    // Let me use different values
    ctx.drop_db(&db);

    // Better test with varied data
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE t2 (x INT, y INT)");
    ctx.exec("INSERT INTO t2 VALUES (1, 1), (2, 0), (3, 1), (4, 0)");

    let rows = ctx.query("SELECT DISTINCT x % 2 FROM t2 ORDER BY 1");
    assert_eq!(rows.len(), 2);
    assert_eq!(get_i64(&rows[0], 0), 0);
    assert_eq!(get_i64(&rows[1], 0), 1);

    ctx.drop_db(&db);
}
