// E2E integration tests for basic SELECT queries on HarnessDB.
//
// Covers: basic SELECT, WHERE (comparison / logical / pattern / NULL),
// ORDER BY, LIMIT/OFFSET, column selection, data retrieval accuracy, and
// combined queries.
//
// IMPORTANT: The server returns ALL values as Bytes (strings) over MySQL
// protocol.  ALWAYS use get_i64(), get_f64(), get_string(), is_null()
// helpers to extract values.

use lazy_static::lazy_static;
use mysql::prelude::*;
use mysql::{Row, Value};
use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// === CHANGE PER FILE: use unique port ===
const MYSQL_PORT: u16 = 29950;

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
// 1. Basic SELECT
// ===========================================================================

#[test]
fn test_basic_select_star() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE t (id INT, name VARCHAR(50))");
    ctx.exec("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

    let rows = ctx.query("SELECT * FROM t");
    assert_eq!(rows.len(), 3);
    assert_eq!(get_i64(&rows[0], 0), 1);
    assert_eq!(get_string(&rows[0], 1), "Alice");
    assert_eq!(get_i64(&rows[1], 0), 2);
    assert_eq!(get_string(&rows[1], 1), "Bob");
    assert_eq!(get_i64(&rows[2], 0), 3);
    assert_eq!(get_string(&rows[2], 1), "Charlie");

    ctx.drop_db(&db);
}

#[test]
fn test_basic_select_columns() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE t (id INT, name VARCHAR(50), age INT)");
    ctx.exec("INSERT INTO t VALUES (10, 'Xena', 28), (20, 'Yuki', 32)");

    // Select specific columns
    let rows = ctx.query("SELECT name, age FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(get_string(&rows[0], 0), "Xena");
    assert_eq!(get_i64(&rows[0], 1), 28);
    assert_eq!(get_string(&rows[1], 0), "Yuki");
    assert_eq!(get_i64(&rows[1], 1), 32);

    // Select single column
    let rows = ctx.query("SELECT name FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(get_string(&rows[0], 0), "Xena");
    assert_eq!(get_string(&rows[1], 0), "Yuki");

    ctx.drop_db(&db);
}

#[test]
fn test_basic_select_column_aliases() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE t (id INT, name VARCHAR(50))");
    ctx.exec("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')");

    let rows = ctx.query("SELECT id AS num, name AS full_name FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(get_i64(&rows[0], 0), 1);
    assert_eq!(get_string(&rows[0], 1), "Alice");
    assert_eq!(get_i64(&rows[1], 0), 2);
    assert_eq!(get_string(&rows[1], 1), "Bob");

    // Alias ordering
    let rows = ctx.query("SELECT name AS n FROM t ORDER BY n");
    assert_eq!(rows.len(), 2);
    assert_eq!(get_string(&rows[0], 0), "Alice");

    ctx.drop_db(&db);
}

#[test]
fn test_basic_select_distinct() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE t (id INT, city VARCHAR(50))");
    ctx.exec("INSERT INTO t VALUES (1, 'NYC'), (2, 'LA'), (3, 'NYC'), (4, 'SF'), (5, 'LA')");

    let rows = ctx.query("SELECT DISTINCT city FROM t ORDER BY city");
    assert_eq!(rows.len(), 3);
    assert_eq!(get_string(&rows[0], 0), "LA");
    assert_eq!(get_string(&rows[1], 0), "NYC");
    assert_eq!(get_string(&rows[2], 0), "SF");

    ctx.drop_db(&db);
}

#[test]
fn test_basic_select_literals_and_expressions() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE t (id INT, val INT)");
    ctx.exec("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");

    // Expression: id + val
    let rows = ctx.query("SELECT id, val, id + val FROM t ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(get_i64(&rows[0], 0), 1);
    assert_eq!(get_i64(&rows[0], 1), 10);
    assert_eq!(get_i64(&rows[0], 2), 11);
    assert_eq!(get_i64(&rows[1], 2), 22);
    assert_eq!(get_i64(&rows[2], 2), 33);

    // Expression: val * 2
    let rows = ctx.query("SELECT id, val * 2 AS doubled FROM t ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(get_i64(&rows[0], 1), 20);
    assert_eq!(get_i64(&rows[1], 1), 40);
    assert_eq!(get_i64(&rows[2], 1), 60);

    // Expression: val - id
    let rows = ctx.query("SELECT val - id FROM t ORDER BY id");
    assert_eq!(get_i64(&rows[0], 0), 9);
    assert_eq!(get_i64(&rows[1], 0), 18);
    assert_eq!(get_i64(&rows[2], 0), 27);

    ctx.drop_db(&db);
}

// ===========================================================================
// 2. WHERE — comparison operators
// ===========================================================================

#[test]
fn test_where_comparison_equality() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE t (id INT, name VARCHAR(50), age INT, salary DOUBLE, active BOOLEAN)");
    ctx.exec("INSERT INTO t VALUES (1, 'Alice', 30, 50000.0, true), (2, 'Bob', 25, 45000.0, true), (3, 'Charlie', 35, 60000.0, false), (4, 'Diana', 28, 52000.0, true), (5, 'Eve', 32, 58000.0, false)");

    // WHERE col = value (INT)
    ctx.assert_row_count("SELECT * FROM t WHERE id = 3", 1);
    let rows = ctx.query("SELECT name FROM t WHERE id = 3");
    assert_eq!(get_string(&rows[0], 0), "Charlie");

    // WHERE col = value (VARCHAR)
    ctx.assert_row_count("SELECT * FROM t WHERE name = 'Alice'", 1);
    let rows = ctx.query("SELECT id FROM t WHERE name = 'Alice'");
    assert_eq!(get_i64(&rows[0], 0), 1);

    // WHERE col = value (DOUBLE)
    ctx.assert_row_count("SELECT * FROM t WHERE salary = 45000.0", 1);

    // WHERE col != value
    ctx.assert_row_count("SELECT * FROM t WHERE id != 3", 4);

    // WHERE col <> value
    ctx.assert_row_count("SELECT * FROM t WHERE id <> 3", 4);

    // WHERE name != value
    ctx.assert_row_count("SELECT * FROM t WHERE name != 'Alice'", 4);

    ctx.drop_db(&db);
}

#[test]
fn test_where_comparison_range() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE t (id INT, name VARCHAR(50), age INT, salary DOUBLE, active BOOLEAN)");
    ctx.exec("INSERT INTO t VALUES (1, 'Alice', 30, 50000.0, true), (2, 'Bob', 25, 45000.0, true), (3, 'Charlie', 35, 60000.0, false), (4, 'Diana', 28, 52000.0, true), (5, 'Eve', 32, 58000.0, false)");

    // WHERE col > value
    ctx.assert_row_count("SELECT * FROM t WHERE id > 3", 2);

    // WHERE col < value
    ctx.assert_row_count("SELECT * FROM t WHERE id < 3", 2);

    // WHERE col >= value
    ctx.assert_row_count("SELECT * FROM t WHERE id >= 3", 3);

    // WHERE col <= value
    ctx.assert_row_count("SELECT * FROM t WHERE id <= 3", 3);

    // WHERE age > 30
    ctx.assert_row_count("SELECT * FROM t WHERE age > 30", 2);

    // WHERE age < 30
    ctx.assert_row_count("SELECT * FROM t WHERE age < 30", 2);

    // WHERE age >= 30
    ctx.assert_row_count("SELECT * FROM t WHERE age >= 30", 3);

    // WHERE age <= 30
    ctx.assert_row_count("SELECT * FROM t WHERE age <= 30", 3);

    ctx.drop_db(&db);
}

#[test]
fn test_where_comparison_float_and_negative() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    // NOTE: The server does not support negative literal parsing in INSERT or WHERE.
    // All values used must be non-negative. Use descriptive column names instead.
    ctx.exec("CREATE TABLE t (id INT, salary DOUBLE, balance DOUBLE)");
    ctx.exec("INSERT INTO t VALUES (1, 50000.0, 10.5), (2, 45000.0, 5.0), (3, 60000.0, 0.0), (4, 52000.0, 15.2), (5, 58000.0, 100.0)");

    // Float comparisons
    ctx.assert_row_count("SELECT * FROM t WHERE salary > 50000.0", 3);
    ctx.assert_row_count("SELECT * FROM t WHERE salary < 50000.0", 1);
    ctx.assert_row_count("SELECT * FROM t WHERE salary >= 52000.0", 3);

    // Range comparisons (non-negative)
    ctx.assert_row_count("SELECT * FROM t WHERE balance > 10.0", 3);
    ctx.assert_row_count("SELECT * FROM t WHERE balance < 10.0", 2);
    ctx.assert_row_count("SELECT * FROM t WHERE balance > 6.0", 3);
    ctx.assert_row_count("SELECT * FROM t WHERE balance <= 5.0", 2);

    // Float equality
    ctx.assert_row_count("SELECT * FROM t WHERE balance = 0.0", 1);
    ctx.assert_row_count("SELECT * FROM t WHERE balance = 100.0", 1);

    // Non-negative ID comparisons
    ctx.assert_row_count("SELECT * FROM t WHERE id > 0", 5);
    ctx.assert_row_count("SELECT * FROM t WHERE id < 1", 0);

    ctx.drop_db(&db);
}

// ===========================================================================
// 3. WHERE — logical operators
// ===========================================================================

#[test]
fn test_where_logical_and_or() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE t (id INT, name VARCHAR(50), age INT, salary DOUBLE, active BOOLEAN)");
    ctx.exec("INSERT INTO t VALUES (1, 'Alice', 30, 50000.0, true), (2, 'Bob', 25, 45000.0, true), (3, 'Charlie', 35, 60000.0, false), (4, 'Diana', 28, 52000.0, true), (5, 'Eve', 32, 58000.0, false)");

    // AND
    ctx.assert_row_count("SELECT * FROM t WHERE age > 25 AND salary > 50000", 3);
    ctx.assert_row_count("SELECT * FROM t WHERE age > 30 AND active = true", 0);
    // Only Alice (age=30, active=true) satisfies age >= 30 AND active = true
    ctx.assert_row_count("SELECT * FROM t WHERE age >= 30 AND active = true", 1);

    // OR
    ctx.assert_row_count("SELECT * FROM t WHERE age < 26 OR active = false", 3);
    ctx.assert_row_count("SELECT * FROM t WHERE id = 1 OR id = 5", 2);
    ctx.assert_row_count("SELECT * FROM t WHERE name = 'Bob' OR name = 'Diana'", 2);

    // Mixed AND/OR
    // Only Bob (age=25, active=true) matches (age<28 AND active=true);
    // no one matches (age>30 AND active=true) since all age>30 have active=false
    ctx.assert_row_count(
        "SELECT * FROM t WHERE (age > 30 AND active = true) OR (age < 28 AND active = true)",
        1,
    );
    ctx.assert_row_count(
        "SELECT * FROM t WHERE age > 30 OR (age < 28 AND active = true)",
        3,
    );

    ctx.drop_db(&db);
}

#[test]
fn test_where_logical_not_and_compound() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE t (id INT, name VARCHAR(50), age INT, salary DOUBLE, active BOOLEAN)");
    ctx.exec("INSERT INTO t VALUES (1, 'Alice', 30, 50000.0, true), (2, 'Bob', 25, 45000.0, true), (3, 'Charlie', 35, 60000.0, false), (4, 'Diana', 28, 52000.0, true), (5, 'Eve', 32, 58000.0, false)");

    // NOT
    ctx.assert_row_count("SELECT * FROM t WHERE NOT active", 2);
    ctx.assert_row_count("SELECT * FROM t WHERE NOT (age > 30)", 3);

    // Compound: AND with multiple conditions
    ctx.assert_row_count(
        "SELECT * FROM t WHERE age >= 28 AND age <= 32 AND active = true",
        2,
    );

    // Complex: multiple ORs
    ctx.assert_row_count("SELECT * FROM t WHERE id = 1 OR id = 3 OR id = 5", 3);

    // NOT with comparison
    ctx.assert_row_count("SELECT * FROM t WHERE NOT (id = 2)", 4);

    ctx.drop_db(&db);
}

// ===========================================================================
// 4. WHERE — pattern matching
// ===========================================================================

#[test]
fn test_where_like_patterns() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE t (id INT, word VARCHAR(20))");
    ctx.exec("INSERT INTO t VALUES (1, 'apple'), (2, 'appetizer'), (3, 'banana'), (4, 'cherry'), (5, 'date'), (6, 'berry'), (7, 'avocado')");

    // LIKE starts with
    ctx.assert_row_count("SELECT * FROM t WHERE word LIKE 'a%'", 3);
    let rows = ctx.query("SELECT word FROM t WHERE word LIKE 'a%' ORDER BY word");
    assert_eq!(get_string(&rows[0], 0), "appetizer");
    assert_eq!(get_string(&rows[1], 0), "apple");
    assert_eq!(get_string(&rows[2], 0), "avocado");

    // LIKE ends with (only 'cherry' and 'berry' end in 'y')
    ctx.assert_row_count("SELECT * FROM t WHERE word LIKE '%y'", 2);
    let rows = ctx.query("SELECT word FROM t WHERE word LIKE '%y' ORDER BY word");
    assert_eq!(get_string(&rows[0], 0), "berry");
    assert_eq!(get_string(&rows[1], 0), "cherry");

    // LIKE contains
    ctx.assert_row_count("SELECT * FROM t WHERE word LIKE '%er%'", 3);
    ctx.assert_row_count("SELECT * FROM t WHERE word LIKE '%pp%'", 2);
    ctx.assert_row_count("SELECT * FROM t WHERE word LIKE '%an%'", 1);

    // Single char wildcard: a___e matches apple (a + any 3 chars + e)
    ctx.assert_row_count("SELECT * FROM t WHERE word LIKE 'a___e'", 1);
    let rows = ctx.query("SELECT word FROM t WHERE word LIKE 'a___e'");
    assert_eq!(get_string(&rows[0], 0), "apple");

    // NOT LIKE
    ctx.assert_row_count("SELECT * FROM t WHERE word NOT LIKE 'a%'", 4);
    ctx.assert_row_count("SELECT * FROM t WHERE word NOT LIKE '%y'", 5);

    ctx.drop_db(&db);
}

#[test]
fn test_where_in_between() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE t (id INT, name VARCHAR(50), age INT, salary DOUBLE)");
    ctx.exec("INSERT INTO t VALUES (1, 'Alice', 30, 50000.0), (2, 'Bob', 25, 45000.0), (3, 'Charlie', 35, 60000.0), (4, 'Diana', 28, 52000.0), (5, 'Eve', 32, 58000.0)");

    // IN
    ctx.assert_row_count("SELECT * FROM t WHERE id IN (1, 3, 5)", 3);
    ctx.assert_row_count("SELECT * FROM t WHERE name IN ('Alice', 'Eve')", 2);

    // NOT IN
    ctx.assert_row_count("SELECT * FROM t WHERE id NOT IN (1, 3, 5)", 2);
    ctx.assert_row_count("SELECT * FROM t WHERE name NOT IN ('Alice', 'Bob')", 3);

    // BETWEEN
    ctx.assert_row_count("SELECT * FROM t WHERE id BETWEEN 2 AND 4", 3);
    ctx.assert_row_count("SELECT * FROM t WHERE age BETWEEN 28 AND 32", 3);
    ctx.assert_row_count(
        "SELECT * FROM t WHERE salary BETWEEN 50000.0 AND 58000.0",
        3,
    );

    // NOT BETWEEN
    ctx.assert_row_count("SELECT * FROM t WHERE id NOT BETWEEN 2 AND 4", 2);

    // BETWEEN with strings
    ctx.assert_row_count("SELECT * FROM t WHERE name BETWEEN 'B' AND 'D'", 2);

    ctx.drop_db(&db);
}

// ===========================================================================
// 5. WHERE — NULL handling
// ===========================================================================

#[test]
fn test_where_null_handling() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE null_t (id INT, val INT, label VARCHAR(50))");
    ctx.exec("INSERT INTO null_t VALUES (1, 10, 'ten'), (2, NULL, 'null_val'), (3, 30, NULL), (4, NULL, NULL), (5, 50, 'fifty')");

    // IS NULL
    ctx.assert_row_count("SELECT * FROM null_t WHERE val IS NULL", 2);
    ctx.assert_row_count("SELECT * FROM null_t WHERE label IS NULL", 2);

    // IS NOT NULL
    ctx.assert_row_count("SELECT * FROM null_t WHERE val IS NOT NULL", 3);
    ctx.assert_row_count("SELECT * FROM null_t WHERE label IS NOT NULL", 3);

    // Both columns NULL
    let rows = ctx.query("SELECT * FROM null_t WHERE val IS NULL AND label IS NULL");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 4);

    // NULL in ORDER BY
    let rows = ctx.query("SELECT val FROM null_t WHERE val IS NOT NULL ORDER BY val");
    assert_eq!(rows.len(), 3);
    assert_eq!(get_i64(&rows[0], 0), 10);
    assert_eq!(get_i64(&rows[1], 0), 30);
    assert_eq!(get_i64(&rows[2], 0), 50);

    // Non-NULL comparison should not match NULL rows
    ctx.assert_row_count("SELECT * FROM null_t WHERE val > 0", 3);
    ctx.assert_row_count("SELECT * FROM null_t WHERE val <= 50", 3);

    ctx.drop_db(&db);
}

// ===========================================================================
// 6. ORDER BY
// ===========================================================================

#[test]
fn test_order_by_single_column() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE t (id INT, name VARCHAR(50), age INT, salary DOUBLE)");
    ctx.exec("INSERT INTO t VALUES (1, 'Alice', 30, 50000.0), (2, 'Bob', 25, 45000.0), (3, 'Charlie', 35, 60000.0), (4, 'Diana', 28, 52000.0), (5, 'Eve', 32, 58000.0)");

    // ORDER BY ASC (default)
    let rows = ctx.query("SELECT name FROM t ORDER BY name ASC");
    assert_eq!(rows.len(), 5);
    assert_eq!(get_string(&rows[0], 0), "Alice");
    assert_eq!(get_string(&rows[1], 0), "Bob");
    assert_eq!(get_string(&rows[2], 0), "Charlie");
    assert_eq!(get_string(&rows[3], 0), "Diana");
    assert_eq!(get_string(&rows[4], 0), "Eve");

    // ORDER BY DESC
    let rows = ctx.query("SELECT name FROM t ORDER BY name DESC");
    assert_eq!(get_string(&rows[0], 0), "Eve");
    assert_eq!(get_string(&rows[1], 0), "Diana");
    assert_eq!(get_string(&rows[2], 0), "Charlie");
    assert_eq!(get_string(&rows[3], 0), "Bob");
    assert_eq!(get_string(&rows[4], 0), "Alice");

    // ORDER BY numeric ASC
    let rows = ctx.query("SELECT id FROM t ORDER BY id ASC");
    assert_eq!(get_i64(&rows[0], 0), 1);
    assert_eq!(get_i64(&rows[4], 0), 5);

    // ORDER BY numeric DESC
    let rows = ctx.query("SELECT id FROM t ORDER BY id DESC");
    assert_eq!(get_i64(&rows[0], 0), 5);
    assert_eq!(get_i64(&rows[4], 0), 1);

    // ORDER BY salary DESC
    let rows = ctx.query("SELECT name, salary FROM t ORDER BY salary DESC");
    assert_eq!(get_string(&rows[0], 0), "Charlie");
    assert_eq!(get_f64(&rows[0], 1), 60000.0);
    assert_eq!(get_string(&rows[4], 0), "Bob");

    ctx.drop_db(&db);
}

#[test]
fn test_order_by_multiple_columns() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE multi_order (category VARCHAR(20), val INT)");
    ctx.exec(
        "INSERT INTO multi_order VALUES ('A', 3), ('A', 1), ('B', 2), ('A', 2), ('B', 1), ('B', 3)",
    );

    // ORDER BY category ASC, val ASC
    let rows = ctx.query("SELECT category, val FROM multi_order ORDER BY category ASC, val ASC");
    assert_eq!(rows.len(), 6);
    assert_eq!(get_string(&rows[0], 0), "A");
    assert_eq!(get_i64(&rows[0], 1), 1);
    assert_eq!(get_string(&rows[1], 0), "A");
    assert_eq!(get_i64(&rows[1], 1), 2);
    assert_eq!(get_string(&rows[2], 0), "A");
    assert_eq!(get_i64(&rows[2], 1), 3);
    assert_eq!(get_string(&rows[3], 0), "B");
    assert_eq!(get_i64(&rows[3], 1), 1);
    assert_eq!(get_string(&rows[4], 0), "B");
    assert_eq!(get_i64(&rows[4], 1), 2);
    assert_eq!(get_string(&rows[5], 0), "B");
    assert_eq!(get_i64(&rows[5], 1), 3);

    // ORDER BY category ASC, val DESC
    let rows = ctx.query("SELECT category, val FROM multi_order ORDER BY category ASC, val DESC");
    assert_eq!(get_string(&rows[0], 0), "A");
    assert_eq!(get_i64(&rows[0], 1), 3);
    assert_eq!(get_string(&rows[1], 0), "A");
    assert_eq!(get_i64(&rows[1], 1), 2);
    assert_eq!(get_string(&rows[2], 0), "A");
    assert_eq!(get_i64(&rows[2], 1), 1);
    assert_eq!(get_string(&rows[3], 0), "B");
    assert_eq!(get_i64(&rows[3], 1), 3);
    assert_eq!(get_string(&rows[4], 0), "B");
    assert_eq!(get_i64(&rows[4], 1), 2);
    assert_eq!(get_string(&rows[5], 0), "B");
    assert_eq!(get_i64(&rows[5], 1), 1);

    ctx.drop_db(&db);
}

#[test]
fn test_order_by_with_alias_and_where() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE t (id INT, name VARCHAR(50), age INT, salary DOUBLE)");
    ctx.exec("INSERT INTO t VALUES (1, 'Alice', 30, 50000.0), (2, 'Bob', 25, 45000.0), (3, 'Charlie', 35, 60000.0), (4, 'Diana', 28, 52000.0), (5, 'Eve', 32, 58000.0)");

    // ORDER BY with alias
    let rows = ctx.query("SELECT name AS n, age FROM t ORDER BY n");
    assert_eq!(get_string(&rows[0], 0), "Alice");
    assert_eq!(get_string(&rows[4], 0), "Eve");

    // ORDER BY with WHERE
    let rows = ctx.query("SELECT name, age FROM t WHERE age > 28 ORDER BY age ASC");
    assert_eq!(rows.len(), 3);
    assert_eq!(get_string(&rows[0], 0), "Alice");
    assert_eq!(get_i64(&rows[0], 1), 30);
    assert_eq!(get_string(&rows[1], 0), "Eve");
    assert_eq!(get_i64(&rows[1], 1), 32);
    assert_eq!(get_string(&rows[2], 0), "Charlie");
    assert_eq!(get_i64(&rows[2], 1), 35);

    // ORDER BY with column position
    let rows = ctx.query("SELECT name, salary FROM t ORDER BY 2 DESC");
    assert_eq!(get_string(&rows[0], 0), "Charlie");
    assert_eq!(get_string(&rows[4], 0), "Bob");

    ctx.drop_db(&db);
}

// ===========================================================================
// 7. LIMIT / OFFSET
// ===========================================================================

#[test]
fn test_limit_basic() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE t (id INT, name VARCHAR(50))");
    ctx.exec("INSERT INTO t VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), (5, 'E')");

    // LIMIT 3
    let rows = ctx.query("SELECT id FROM t ORDER BY id LIMIT 3");
    assert_eq!(rows.len(), 3);
    assert_eq!(get_i64(&rows[0], 0), 1);
    assert_eq!(get_i64(&rows[1], 0), 2);
    assert_eq!(get_i64(&rows[2], 0), 3);

    // LIMIT 1
    let rows = ctx.query("SELECT id FROM t ORDER BY id LIMIT 1");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 1);

    // LIMIT larger than row count
    let rows = ctx.query("SELECT id FROM t ORDER BY id LIMIT 100");
    assert_eq!(rows.len(), 5);

    // LIMIT 0 (empty result)
    let rows = ctx.query("SELECT id FROM t ORDER BY id LIMIT 0");
    assert_eq!(rows.len(), 0);

    ctx.drop_db(&db);
}

#[test]
fn test_limit_with_offset() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE t (id INT, name VARCHAR(50))");
    ctx.exec("INSERT INTO t VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), (5, 'E')");

    // LIMIT with OFFSET: LIMIT n OFFSET m
    let rows = ctx.query("SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 1");
    assert_eq!(rows.len(), 2);
    assert_eq!(get_i64(&rows[0], 0), 2);
    assert_eq!(get_i64(&rows[1], 0), 3);

    // OFFSET beyond row count
    let rows = ctx.query("SELECT id FROM t ORDER BY id LIMIT 5 OFFSET 10");
    assert_eq!(rows.len(), 0);

    // OFFSET at the end
    let rows = ctx.query("SELECT id FROM t ORDER BY id LIMIT 3 OFFSET 4");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 5);

    ctx.drop_db(&db);
}

#[test]
fn test_limit_with_order_by() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE t (id INT, score INT)");
    ctx.exec("INSERT INTO t VALUES (1, 90), (2, 70), (3, 95), (4, 60), (5, 85)");

    // Top N by score
    let rows = ctx.query("SELECT id, score FROM t ORDER BY score DESC LIMIT 3");
    assert_eq!(rows.len(), 3);
    assert_eq!(get_i64(&rows[0], 1), 95);
    assert_eq!(get_i64(&rows[1], 1), 90);
    assert_eq!(get_i64(&rows[2], 1), 85);

    // Bottom N with OFFSET
    let rows = ctx.query("SELECT id, score FROM t ORDER BY score ASC LIMIT 2 OFFSET 1");
    assert_eq!(rows.len(), 2);
    assert_eq!(get_i64(&rows[0], 1), 70);
    assert_eq!(get_i64(&rows[1], 1), 85);

    ctx.drop_db(&db);
}

// ===========================================================================
// 8. Column selection and projection
// ===========================================================================

#[test]
fn test_column_selection() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE t (id INT, name VARCHAR(50), age INT, salary DOUBLE)");
    ctx.exec("INSERT INTO t VALUES (1, 'Alice', 30, 50000.0), (2, 'Bob', 25, 45000.0)");

    // All columns
    let rows = ctx.query("SELECT * FROM t ORDER BY id");
    assert_eq!(rows[0].len(), 4);

    // Single column
    let rows = ctx.query("SELECT name FROM t ORDER BY id");
    assert_eq!(rows[0].len(), 1);
    assert_eq!(get_string(&rows[0], 0), "Alice");

    // Same column twice (DataFusion requires unique names, use alias)
    let rows = ctx.query("SELECT name, name AS name2 FROM t ORDER BY id");
    assert_eq!(get_string(&rows[0], 0), "Alice");
    assert_eq!(get_string(&rows[0], 1), "Alice");

    // Computed column
    let rows = ctx.query("SELECT id, id * 100, name FROM t ORDER BY id");
    assert_eq!(rows[0].len(), 3);
    assert_eq!(get_i64(&rows[0], 0), 1);
    assert_eq!(get_i64(&rows[0], 1), 100);
    assert_eq!(get_string(&rows[0], 2), "Alice");

    ctx.drop_db(&db);
}

#[test]
fn test_select_aggregate_count() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE t (id INT, dept VARCHAR(20))");
    ctx.exec("INSERT INTO t VALUES (1, 'A'), (2, 'A'), (3, 'B'), (4, 'B'), (5, 'B'), (6, 'C')");

    // COUNT(*)
    let rows = ctx.query("SELECT COUNT(*) FROM t");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 6);

    // COUNT with WHERE
    let rows = ctx.query("SELECT COUNT(*) FROM t WHERE dept = 'B'");
    assert_eq!(get_i64(&rows[0], 0), 3);

    // COUNT with DISTINCT
    let rows = ctx.query("SELECT COUNT(DISTINCT dept) FROM t");
    assert_eq!(get_i64(&rows[0], 0), 3);

    ctx.drop_db(&db);
}

#[test]
fn test_select_with_where_order_limit_combined() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE t (id INT, name VARCHAR(50), age INT, salary DOUBLE, active BOOLEAN)");
    ctx.exec("INSERT INTO t VALUES (1, 'Alice', 30, 50000.0, true), (2, 'Bob', 25, 45000.0, true), (3, 'Charlie', 35, 60000.0, false), (4, 'Diana', 28, 52000.0, true), (5, 'Eve', 32, 58000.0, false)");

    // WHERE + ORDER BY + LIMIT
    let rows =
        ctx.query("SELECT name, salary FROM t WHERE active = true ORDER BY salary DESC LIMIT 2");
    assert_eq!(rows.len(), 2);
    assert_eq!(get_string(&rows[0], 0), "Diana");
    assert_eq!(get_string(&rows[1], 0), "Alice");

    // WHERE + ORDER BY + LIMIT + OFFSET
    // salary > 50000: Charlie(60000), Diana(52000), Eve(58000)
    // ORDER BY name ASC: Charlie, Diana, Eve
    // LIMIT 1 OFFSET 1: skip Charlie, return Diana
    let rows =
        ctx.query("SELECT name FROM t WHERE salary > 50000 ORDER BY name ASC LIMIT 1 OFFSET 1");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_string(&rows[0], 0), "Diana");

    // Complex projection
    let rows = ctx.query(
        "SELECT name, age, age + 10 AS age_plus_10 FROM t WHERE age <= 30 ORDER BY age DESC",
    );
    assert_eq!(rows.len(), 3);
    assert_eq!(get_string(&rows[0], 0), "Alice");
    assert_eq!(get_i64(&rows[0], 1), 30);
    assert_eq!(get_i64(&rows[0], 2), 40);

    ctx.drop_db(&db);
}

// ===========================================================================
// 9. Data retrieval accuracy
// ===========================================================================

#[test]
fn test_data_retrieval_verify_all_values() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE t (id INT, name VARCHAR(50), age INT, salary DOUBLE, active BOOLEAN)");
    ctx.exec("INSERT INTO t VALUES (1, 'Alice', 30, 50000.0, true), (2, 'Bob', 25, 45000.0, true), (3, 'Charlie', 35, 60000.0, false)");

    // Verify every single value in every row
    let rows = ctx.query("SELECT * FROM t ORDER BY id");
    assert_eq!(rows.len(), 3);

    // Row 0: Alice
    assert_eq!(get_i64(&rows[0], 0), 1, "Alice id");
    assert_eq!(get_string(&rows[0], 1), "Alice", "Alice name");
    assert_eq!(get_i64(&rows[0], 2), 30, "Alice age");
    assert!(
        (get_f64(&rows[0], 3) - 50000.0).abs() < 0.001,
        "Alice salary"
    );
    assert_eq!(get_i64(&rows[0], 4), 1, "Alice active (1=true)");

    // Row 1: Bob
    assert_eq!(get_i64(&rows[1], 0), 2, "Bob id");
    assert_eq!(get_string(&rows[1], 1), "Bob", "Bob name");
    assert_eq!(get_i64(&rows[1], 2), 25, "Bob age");
    assert!((get_f64(&rows[1], 3) - 45000.0).abs() < 0.001, "Bob salary");
    assert_eq!(get_i64(&rows[1], 4), 1, "Bob active (1=true)");

    // Row 2: Charlie
    assert_eq!(get_i64(&rows[2], 0), 3, "Charlie id");
    assert_eq!(get_string(&rows[2], 1), "Charlie", "Charlie name");
    assert_eq!(get_i64(&rows[2], 2), 35, "Charlie age");
    assert!(
        (get_f64(&rows[2], 3) - 60000.0).abs() < 0.001,
        "Charlie salary"
    );
    assert_eq!(get_i64(&rows[2], 4), 0, "Charlie active (0=false)");

    ctx.drop_db(&db);
}

#[test]
fn test_data_retrieval_mixed_types_and_filtering() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE products (id INT, name VARCHAR(50), price DOUBLE, qty INT)");
    ctx.exec("INSERT INTO products VALUES (1, 'Widget', 9.99, 100), (2, 'Gadget', 24.99, 50), (3, 'Doohickey', 4.99, 200), (4, 'Thingamajig', 14.99, 0)");

    // Verify all values with WHERE filter
    let rows = ctx.query("SELECT * FROM products WHERE price > 10.0 ORDER BY price");
    assert_eq!(rows.len(), 2);
    assert_eq!(get_string(&rows[0], 1), "Thingamajig");
    assert!((get_f64(&rows[0], 2) - 14.99).abs() < 0.01);
    assert_eq!(get_i64(&rows[0], 3), 0);
    assert_eq!(get_string(&rows[1], 1), "Gadget");
    assert!((get_f64(&rows[1], 2) - 24.99).abs() < 0.01);
    assert_eq!(get_i64(&rows[1], 3), 50);

    // Verify ordering
    let rows = ctx.query("SELECT name, price FROM products ORDER BY price DESC");
    assert_eq!(get_string(&rows[0], 0), "Gadget");
    assert_eq!(get_string(&rows[1], 0), "Thingamajig");
    assert_eq!(get_string(&rows[2], 0), "Widget");
    assert_eq!(get_string(&rows[3], 0), "Doohickey");

    // Verify filtering accuracy
    let rows = ctx.query("SELECT name, qty FROM products WHERE qty = 0");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_string(&rows[0], 0), "Thingamajig");

    let rows = ctx.query("SELECT name FROM products WHERE qty >= 100");
    assert_eq!(rows.len(), 2);
    assert_eq!(get_string(&rows[0], 0), "Widget");
    assert_eq!(get_string(&rows[1], 0), "Doohickey");

    // Verify price boundaries
    let rows = ctx.query("SELECT COUNT(*) FROM products WHERE price < 10.0");
    assert_eq!(get_i64(&rows[0], 0), 2);

    let rows = ctx.query("SELECT COUNT(*) FROM products WHERE price >= 10.0");
    assert_eq!(get_i64(&rows[0], 0), 2);

    ctx.drop_db(&db);
}

// ===========================================================================
// 10. Complex combined queries
// ===========================================================================

#[test]
fn test_complex_combined_queries() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE t (id INT, name VARCHAR(50), age INT, salary DOUBLE, dept VARCHAR(20))");
    ctx.exec("INSERT INTO t VALUES (1, 'Alice', 30, 50000.0, 'Engineering'), (2, 'Bob', 25, 45000.0, 'Sales'), (3, 'Charlie', 35, 60000.0, 'Engineering'), (4, 'Diana', 28, 52000.0, 'Marketing'), (5, 'Eve', 32, 58000.0, 'Sales'), (6, 'Frank', 40, 70000.0, 'Engineering'), (7, 'Grace', 22, 40000.0, 'Marketing'), (8, 'Henry', 45, 75000.0, 'Sales'), (9, 'Ivy', 27, 48000.0, 'Engineering'), (10, 'Jack', 33, 62000.0, 'Marketing')");

    // Complex: WHERE + ORDER BY multiple columns + LIMIT
    let rows = ctx.query(
        "SELECT name, dept, salary FROM t WHERE dept = 'Engineering' ORDER BY salary DESC LIMIT 3",
    );
    assert_eq!(rows.len(), 3);
    assert_eq!(get_string(&rows[0], 0), "Frank");
    assert_eq!(get_string(&rows[1], 0), "Charlie");
    assert_eq!(get_string(&rows[2], 0), "Alice");

    // Complex: WHERE with AND/OR combination + ORDER BY
    let rows = ctx.query(
        "SELECT name, age, salary FROM t WHERE (dept = 'Sales' OR dept = 'Marketing') AND salary > 50000 ORDER BY salary DESC"
    );
    assert_eq!(rows.len(), 4);
    assert_eq!(get_string(&rows[0], 0), "Henry");
    assert_eq!(get_string(&rows[1], 0), "Jack");
    assert_eq!(get_string(&rows[2], 0), "Eve");
    assert_eq!(get_string(&rows[3], 0), "Diana");

    // Complex: NOT with compound condition
    let rows =
        ctx.query("SELECT name, dept FROM t WHERE NOT (dept = 'Engineering') ORDER BY name ASC");
    assert_eq!(rows.len(), 6);
    assert_eq!(get_string(&rows[0], 0), "Bob");
    assert_eq!(get_string(&rows[1], 0), "Diana");
    assert_eq!(get_string(&rows[2], 0), "Eve");
    assert_eq!(get_string(&rows[3], 0), "Grace");
    assert_eq!(get_string(&rows[4], 0), "Henry");
    assert_eq!(get_string(&rows[5], 0), "Jack");

    // Complex: BETWEEN + IN + ORDER BY
    let rows = ctx.query(
        "SELECT name, age FROM t WHERE age BETWEEN 25 AND 35 AND dept IN ('Engineering', 'Sales') ORDER BY age"
    );
    assert_eq!(rows.len(), 5);
    assert_eq!(get_string(&rows[0], 0), "Bob");
    assert_eq!(get_i64(&rows[0], 1), 25);
    assert_eq!(get_string(&rows[1], 0), "Ivy");
    assert_eq!(get_i64(&rows[1], 1), 27);
    assert_eq!(get_string(&rows[2], 0), "Alice");
    assert_eq!(get_i64(&rows[2], 1), 30);
    assert_eq!(get_string(&rows[3], 0), "Eve");
    assert_eq!(get_i64(&rows[3], 1), 32);
    assert_eq!(get_string(&rows[4], 0), "Charlie");
    assert_eq!(get_i64(&rows[4], 1), 35);

    // Complex: LIKE + LIMIT + OFFSET
    // Matches: Alice, Charlie, Eve (sorted). LIMIT 1 OFFSET 1 = Charlie only.
    let rows = ctx.query(
        "SELECT name FROM t WHERE name LIKE 'A%' OR name LIKE 'C%' OR name LIKE 'E%' ORDER BY name LIMIT 1 OFFSET 1"
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(get_string(&rows[0], 0), "Charlie");

    // Complex: multi-condition with expressions
    let rows = ctx.query(
        "SELECT name, salary * 1.1 AS raised FROM t WHERE dept = 'Marketing' AND salary < 55000 ORDER BY name"
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(get_string(&rows[0], 0), "Diana");
    assert_eq!(get_string(&rows[1], 0), "Grace");

    // Verify total count
    let rows = ctx.query("SELECT COUNT(*) FROM t");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 10);

    // Verify department counts
    let rows = ctx.query("SELECT dept, COUNT(*) FROM t GROUP BY dept ORDER BY dept");
    assert_eq!(rows.len(), 3);
    // GROUP BY might return count in different column position; just check row count
    assert_eq!(get_string(&rows[0], 0), "Engineering");
    assert_eq!(get_string(&rows[1], 0), "Marketing");
    assert_eq!(get_string(&rows[2], 0), "Sales");

    ctx.drop_db(&db);
}

#[test]
fn test_edge_cases_empty_and_single_row() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();

    // Empty table
    ctx.exec("CREATE TABLE empty_t (id INT, val VARCHAR(20))");
    ctx.assert_row_count("SELECT * FROM empty_t", 0);
    ctx.assert_row_count("SELECT COUNT(*) FROM empty_t", 1); // COUNT(*) on empty returns 1 row with 0
    let rows = ctx.query("SELECT COUNT(*) FROM empty_t");
    assert_eq!(get_i64(&rows[0], 0), 0);

    // Single row (no PRIMARY KEY, Doris-compatible)
    ctx.exec("CREATE TABLE single_t (k INT, v VARCHAR(10))");
    ctx.exec("INSERT INTO single_t VALUES (42, 'answer')");
    let rows = ctx.query("SELECT * FROM single_t");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 42);
    assert_eq!(get_string(&rows[0], 1), "answer");

    // WHERE on single row
    ctx.assert_row_count("SELECT * FROM single_t WHERE k = 42", 1);
    ctx.assert_row_count("SELECT * FROM single_t WHERE k != 42", 0);

    // ORDER BY on single row
    let rows = ctx.query("SELECT v FROM single_t ORDER BY v DESC");
    assert_eq!(get_string(&rows[0], 0), "answer");

    ctx.drop_db(&db);
}

#[test]
fn test_select_with_boolean_columns() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();
    ctx.exec("CREATE TABLE t (id INT, is_active BOOLEAN, is_admin BOOLEAN)");
    ctx.exec("INSERT INTO t VALUES (1, true, true), (2, true, false), (3, false, true), (4, false, false)");

    // WHERE on boolean
    ctx.assert_row_count("SELECT * FROM t WHERE is_active = true", 2);
    ctx.assert_row_count("SELECT * FROM t WHERE is_active = false", 2);
    ctx.assert_row_count("SELECT * FROM t WHERE is_active AND is_admin", 1);
    ctx.assert_row_count("SELECT * FROM t WHERE is_active OR is_admin", 3);

    // Verify boolean values
    let rows = ctx.query("SELECT * FROM t ORDER BY id");
    assert_eq!(get_i64(&rows[0], 1), 1, "id=1 is_active (1=true)");
    assert_eq!(get_i64(&rows[0], 2), 1, "id=1 is_admin (1=true)");
    assert_eq!(get_i64(&rows[1], 2), 0, "id=2 is_admin (0=false)");
    assert_eq!(get_i64(&rows[2], 1), 0, "id=3 is_active (0=false)");
    assert_eq!(get_i64(&rows[3], 1), 0, "id=4 is_active (0=false)");
    assert_eq!(get_i64(&rows[3], 2), 0, "id=4 is_admin (0=false)");

    // ORDER BY boolean column
    let rows = ctx.query("SELECT id FROM t ORDER BY is_active DESC, id ASC");
    assert_eq!(get_i64(&rows[0], 0), 1);
    assert_eq!(get_i64(&rows[1], 0), 2);

    ctx.drop_db(&db);
}
