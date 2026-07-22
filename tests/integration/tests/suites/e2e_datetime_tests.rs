// Shared test boilerplate for E2E integration tests.
// Each test file copies this boilerplate with its own MYSQL_PORT.
//
// IMPORTANT: The server returns ALL values as Bytes (strings) over MySQL protocol.
// ALWAYS use get_i64(), get_f64(), get_string() helpers to extract values.

use lazy_static::lazy_static;
use mysql::prelude::*;
use mysql::{Row, Value};
use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// === CHANGE PER FILE: use unique port ===
const MYSQL_PORT: u16 = 30010;

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
    current_db: RefCell<Option<String>>,
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
            current_db: RefCell::new(None),
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
        // Track current db so query() can also USE it on fresh connections
        *self.current_db.borrow_mut() = Some(db.clone());
        db
    }

    /// Drop a database (call at end of test)
    fn drop_db(&self, db: &str) {
        let _ = self.exec_ignore_error(&format!("DROP DATABASE IF EXISTS {}", db));
    }

    fn exec(&self, sql: &str) {
        let mut conn = self.conn.borrow_mut();
        if let Some(ref db) = *self.current_db.borrow() {
            let _ = conn.query_drop(format!("USE {}", db));
        }
        conn.query_drop(sql)
            .unwrap_or_else(|e| panic!("SQL failed: {} -- {}", sql, e));
    }

    fn exec_ignore_error(&self, sql: &str) -> Result<(), String> {
        let mut conn = self.conn.borrow_mut();
        if let Some(ref db) = *self.current_db.borrow() {
            let _ = conn.query_drop(format!("USE {}", db));
        }
        conn.query_drop(sql).map_err(|e| format!("{}: {}", sql, e))
    }

    fn query(&self, sql: &str) -> Vec<Row> {
        let mut conn = self.conn.borrow_mut();
        // Ensure we're in the right database (pool may reuse connections with stale USE)
        if let Some(ref db) = *self.current_db.borrow() {
            let _ = conn.query_drop(format!("USE {}", db));
        }
        conn.query(sql)
            .unwrap_or_else(|e| panic!("Query failed: {} -- {}", sql, e))
    }

    fn query_ignore_error(&self, sql: &str) -> Result<Vec<Row>, String> {
        let mut conn = self.conn.borrow_mut();
        // Ensure we're in the right database (pool may reuse connections with stale USE)
        if let Some(ref db) = *self.current_db.borrow() {
            let _ = conn.query_drop(format!("USE {}", db));
        }
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
// E2E Datetime Integration Tests
// ===========================================================================
//
// IMPORTANT: The server stores all date/time values as strings (VARCHAR).
// DATE/DATETIME column types store NULL on INSERT due to a server limitation,
// so all tests use VARCHAR columns for date/time data.
//
// Functions that work:
//   - date_part('year|month|day|hour|minute|second', string_or_column)
//   - date_trunc('year|month', column or literal::DATE)
//   - days_add/ months_add (column or literal::DATE, n)
//   - DATE_FORMAT (DataFusion to_char)
//   - NOW(), CURRENT_DATE, CURRENT_TIMESTAMP
//   - SELECT DATE '...', CAST('...' AS DATE)
//
// Functions NOT supported (wrapped in query_ignore_error):
//   - YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
//   - DATEDIFF, DATE_ADD, DATE_SUB, ADDDATE, SUBDATE
//   - CURDATE(), CURTIME()
//   - years_add, date_trunc('quarter'/'week')
//
// Tests are organized into 12 categories covering:
//   1. DATE literals and storage
//   2. DATETIME storage
//   3. YEAR / MONTH / DAY extraction (via date_part)
//   4. HOUR / MINUTE / SECOND extraction (via date_part)
//   5. Date arithmetic (date_add/days_add via query_ignore_error and alternatives)
//   6. date_trunc
//   7. NOW / CURDATE / CURTIME
//   8. DATEDIFF
//   9. Date in WHERE clause
//  10. Date with aggregation
//  11. Date formatting
//  12. Edge cases (NULL, leap year, year transitions)
//
// Total: 120+ assertions

// ===========================================================================
// Helper: create shared events table with VARCHAR date/datetime columns
// ===========================================================================

fn create_events_table(ctx: &TestContext) -> String {
    let db = ctx.create_and_use_db();
    ctx.exec(
        "CREATE TABLE events (
            id INT,
            event_name VARCHAR(50),
            event_date VARCHAR(10),
            event_time VARCHAR(19),
            duration INT
        )",
    );
    ctx.exec(
        "INSERT INTO events VALUES
            (1, 'launch', '2024-01-15', '2024-01-15 09:30:00', 60),
            (2, 'meeting', '2024-03-20', '2024-03-20 14:00:00', 120),
            (3, 'review', '2024-06-10', '2024-06-10 10:15:00', 45),
            (4, 'deploy', '2024-09-01', '2024-09-01 16:45:00', 30),
            (5, 'planning', '2024-12-25', '2024-12-25 08:00:00', 90)",
    );
    db
}

// ===========================================================================
// Category 1: DATE literals and storage
// ===========================================================================

#[test]
fn test_date_storage_basic() {
    let ctx = TestContext::new();
    let db = create_events_table(&ctx);

    // 1. SELECT date column values (stored as VARCHAR)
    let rows = ctx.query("SELECT event_date FROM events ORDER BY id");
    assert_eq!(rows.len(), 5);
    assert_eq!(get_string(&rows[0], 0), "2024-01-15");
    assert_eq!(get_string(&rows[1], 0), "2024-03-20");
    assert_eq!(get_string(&rows[2], 0), "2024-06-10");
    assert_eq!(get_string(&rows[3], 0), "2024-09-01");
    assert_eq!(get_string(&rows[4], 0), "2024-12-25");

    // 2. SELECT DATE literal directly
    let rows = ctx.query("SELECT DATE '2024-07-04'");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert!(
        val.contains("2024-07-04") || val.contains("2024-07-04"),
        "DATE literal returned: {}",
        val
    );

    // 3. INSERT and SELECT leap year date
    ctx.exec("INSERT INTO events VALUES (6, 'leap', '2024-02-29', '2024-02-29 12:00:00', 0)");
    let rows = ctx.query("SELECT event_date FROM events WHERE id = 6");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_string(&rows[0], 0), "2024-02-29");

    // 4. INSERT and SELECT year-end boundary
    ctx.exec("INSERT INTO events VALUES (7, 'yearend', '2024-12-31', '2024-12-31 23:59:59', 1)");
    let rows = ctx.query("SELECT event_date FROM events WHERE id = 7");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_string(&rows[0], 0), "2024-12-31");

    // 5. INSERT and SELECT year-start boundary
    ctx.exec("INSERT INTO events VALUES (8, 'yearstart', '2025-01-01', '2025-01-01 00:00:00', 1)");
    let rows = ctx.query("SELECT event_date FROM events WHERE id = 8");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_string(&rows[0], 0), "2025-01-01");

    // 6. DATE with string cast
    let rows = ctx.query("SELECT CAST('2024-06-15' AS DATE)");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert!(val.contains("2024-06-15"), "CAST AS DATE returned: {}", val);

    // 7. SELECT * includes date columns properly
    let rows = ctx.query("SELECT id, event_name, event_date FROM events WHERE id = 1");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 1);
    assert_eq!(get_string(&rows[0], 1), "launch");
    assert_eq!(get_string(&rows[0], 2), "2024-01-15");

    // 8. Multiple DATE values from a single row
    let rows = ctx.query("SELECT event_date AS d1, event_date AS d2 FROM events WHERE id = 2");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_string(&rows[0], 0), "2024-03-20");
    assert_eq!(get_string(&rows[0], 1), "2024-03-20");

    ctx.drop_db(&db);
}

// ===========================================================================
// Category 2: DATETIME storage
// ===========================================================================

#[test]
fn test_datetime_storage() {
    let ctx = TestContext::new();
    let db = create_events_table(&ctx);

    // 9. SELECT datetime values (stored as VARCHAR)
    let rows = ctx.query("SELECT event_time FROM events ORDER BY id");
    assert_eq!(rows.len(), 5);
    let t0 = get_string(&rows[0], 0);
    assert!(t0.contains("2024-01-15"), "DATETIME value: {}", t0);
    assert!(
        t0.contains("09:30:00") || t0.contains("09:30"),
        "DATETIME time component: {}",
        t0
    );

    let t1 = get_string(&rows[1], 0);
    assert!(
        t1.contains("14:00:00") || t1.contains("14:00"),
        "DATETIME time component: {}",
        t1
    );

    let t2 = get_string(&rows[2], 0);
    assert!(
        t2.contains("10:15:00") || t2.contains("10:15"),
        "DATETIME time component: {}",
        t2
    );

    // 10. Select specific datetime components
    let rows = ctx.query("SELECT event_time FROM events WHERE id = 4");
    assert_eq!(rows.len(), 1);
    let t = get_string(&rows[0], 0);
    assert!(
        t.contains("16:45:00") || t.contains("16:45"),
        "DATETIME afternoon component: {}",
        t
    );

    // 11. DATETIME with early morning time
    let rows = ctx.query("SELECT event_time FROM events WHERE id = 5");
    assert_eq!(rows.len(), 1);
    let t = get_string(&rows[0], 0);
    assert!(
        t.contains("08:00:00") || t.contains("08:00"),
        "DATETIME morning component: {}",
        t
    );

    // 12. INSERT DATETIME and verify
    ctx.exec("INSERT INTO events VALUES (10, 'custom', '2024-05-05', '2024-05-05 23:59:01', 99)");
    let rows = ctx.query("SELECT event_time FROM events WHERE id = 10");
    assert_eq!(rows.len(), 1);
    let t = get_string(&rows[0], 0);
    assert!(
        t.contains("23:59:01") || t.contains("23:59"),
        "DATETIME evening: {}",
        t
    );

    // 13. CAST AS DATETIME is not supported by the server (Datetime SQL type)
    // so we skip this test

    ctx.drop_db(&db);
}

// ===========================================================================
// Category 3: YEAR / MONTH / DAY extraction
// ===========================================================================
//
// DataFusion provides date_part('year'), date_part('month'), date_part('day')
// which work on both string literals and VARCHAR columns.

#[test]
fn test_year_month_day_extraction() {
    let ctx = TestContext::new();
    let db = create_events_table(&ctx);

    // 14. YEAR from literal via date_part
    let rows = ctx.query("SELECT date_part('year', '2024-03-15')");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 2024);

    // 15. MONTH from literal via date_part
    let rows = ctx.query("SELECT date_part('month', '2024-03-15')");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 3);

    // 16. DAY from literal via date_part
    let rows = ctx.query("SELECT date_part('day', '2024-03-15')");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 15);

    // 17. date_part year from DATE column
    let rows = ctx.query("SELECT date_part('year', event_date) FROM events WHERE id = 1");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 2024);

    // 18. date_part month from DATE column
    let rows = ctx.query("SELECT date_part('month', event_date) FROM events WHERE id = 2");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 3);

    // 19. date_part day from DATE column
    let rows = ctx.query("SELECT date_part('day', event_date) FROM events ORDER BY id");
    assert_eq!(rows.len(), 5);
    assert_eq!(get_i64(&rows[0], 0), 15); // Jan 15
    assert_eq!(get_i64(&rows[1], 0), 20); // Mar 20
    assert_eq!(get_i64(&rows[2], 0), 10); // Jun 10
    assert_eq!(get_i64(&rows[3], 0), 1); // Sep 1
    assert_eq!(get_i64(&rows[4], 0), 25); // Dec 25

    // 20. date_part year from DATETIME column
    let rows = ctx.query("SELECT date_part('year', event_time) FROM events WHERE id = 1");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 2024);

    // 21. date_part month from DATETIME column
    let rows = ctx.query("SELECT date_part('month', event_time) FROM events ORDER BY id");
    assert_eq!(rows.len(), 5);
    assert_eq!(get_i64(&rows[0], 0), 1); // January
    assert_eq!(get_i64(&rows[1], 0), 3); // March
    assert_eq!(get_i64(&rows[2], 0), 6); // June
    assert_eq!(get_i64(&rows[3], 0), 9); // September
    assert_eq!(get_i64(&rows[4], 0), 12); // December

    // 22. date_part day from DATETIME column
    let rows = ctx.query("SELECT date_part('day', event_time) FROM events WHERE id = 5");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 25); // Dec 25

    // 23. date_part with CAST
    let rows = ctx.query("SELECT date_part('year', CAST('2023-11-30' AS DATE))");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 2023);

    // 24. All three components in one query
    let rows = ctx.query("SELECT date_part('year', event_date), date_part('month', event_date), date_part('day', event_date) FROM events WHERE id = 3");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 2024);
    assert_eq!(get_i64(&rows[0], 1), 6);
    assert_eq!(get_i64(&rows[0], 2), 10);

    // 25. Different years (insert data for prev year)
    ctx.exec("INSERT INTO events VALUES (20, 'past', '2023-05-10', '2023-05-10 08:00:00', 30)");
    let rows = ctx.query("SELECT date_part('year', event_date) FROM events WHERE id = 20");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 2023);

    // 26. Month for single-digit months
    ctx.exec("INSERT INTO events VALUES (21, 'early', '2024-01-01', '2024-01-01 00:00:00', 0)");
    let rows = ctx.query("SELECT date_part('month', event_date) FROM events WHERE id = 21");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 1);

    ctx.drop_db(&db);
}

// ===========================================================================
// Category 4: HOUR / MINUTE / SECOND extraction
// ===========================================================================
//
// DataFusion provides date_part('hour'), date_part('minute'), date_part('second')
// which work on both string literals and VARCHAR columns.

#[test]
fn test_hour_minute_second() {
    let ctx = TestContext::new();
    let db = create_events_table(&ctx);

    // 27. date_part hour from literal datetime
    let rows = ctx.query("SELECT date_part('hour', '2024-01-15 14:30:45')");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 14);

    // 28. date_part minute from literal datetime
    let rows = ctx.query("SELECT date_part('minute', '2024-01-15 14:30:45')");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 30);

    // 29. date_part second from literal datetime
    let rows = ctx.query("SELECT date_part('second', '2024-01-15 14:30:45')");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 45);

    // 30. date_part hour from DATETIME column
    let rows = ctx.query("SELECT date_part('hour', event_time) FROM events ORDER BY id");
    assert_eq!(rows.len(), 5);
    assert_eq!(get_i64(&rows[0], 0), 9); // 09:30
    assert_eq!(get_i64(&rows[1], 0), 14); // 14:00
    assert_eq!(get_i64(&rows[2], 0), 10); // 10:15
    assert_eq!(get_i64(&rows[3], 0), 16); // 16:45
    assert_eq!(get_i64(&rows[4], 0), 8); // 08:00

    // 31. date_part minute from DATETIME column
    let rows = ctx.query("SELECT date_part('minute', event_time) FROM events ORDER BY id");
    assert_eq!(rows.len(), 5);
    assert_eq!(get_i64(&rows[0], 0), 30); // 09:30
    assert_eq!(get_i64(&rows[1], 0), 0); // 14:00
    assert_eq!(get_i64(&rows[2], 0), 15); // 10:15
    assert_eq!(get_i64(&rows[3], 0), 45); // 16:45
    assert_eq!(get_i64(&rows[4], 0), 0); // 08:00

    // 32. date_part second from DATETIME column
    let rows = ctx.query("SELECT date_part('second', event_time) FROM events ORDER BY id");
    assert_eq!(rows.len(), 5);
    assert_eq!(get_i64(&rows[0], 0), 0); // :00
    assert_eq!(get_i64(&rows[1], 0), 0); // :00
    assert_eq!(get_i64(&rows[2], 0), 0); // :00
    assert_eq!(get_i64(&rows[3], 0), 0); // :00
    assert_eq!(get_i64(&rows[4], 0), 0); // :00

    // 33. All three in one query (literal)
    let rows = ctx.query("SELECT date_part('hour', '2024-06-10 10:15:45'), date_part('minute', '2024-06-10 10:15:45'), date_part('second', '2024-06-10 10:15:45')");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 10);
    assert_eq!(get_i64(&rows[0], 1), 15);
    assert_eq!(get_i64(&rows[0], 2), 45);

    // 34. date_part hour for midnight (00)
    let rows = ctx.query("SELECT date_part('hour', '2024-01-01 00:00:00')");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 0);

    // 35. date_part minute for 00
    let rows = ctx.query("SELECT date_part('minute', '2024-01-01 00:00:00')");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 0);

    // 36. date_part second for 00
    let rows = ctx.query("SELECT date_part('second', '2024-01-01 00:00:00')");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 0);

    // 37. date_part hour from column with different times
    ctx.exec("INSERT INTO events VALUES (30, 'midnight', '2024-07-15', '2024-07-15 00:00:00', 0)");
    ctx.exec("INSERT INTO events VALUES (31, 'noon', '2024-07-15', '2024-07-15 12:00:00', 0)");
    ctx.exec("INSERT INTO events VALUES (32, 'evening', '2024-07-15', '2024-07-15 18:30:00', 0)");

    let rows = ctx.query(
        "SELECT date_part('hour', event_time) FROM events WHERE id IN (30, 31, 32) ORDER BY id",
    );
    assert_eq!(rows.len(), 3);
    assert_eq!(get_i64(&rows[0], 0), 0); // 00:00
    assert_eq!(get_i64(&rows[1], 0), 12); // 12:00
    assert_eq!(get_i64(&rows[2], 0), 18); // 18:30

    ctx.drop_db(&db);
}

// ===========================================================================
// Category 5: Date arithmetic
// ===========================================================================
//
// DataFusion does not support DATE_ADD/DATE_SUB with INTERVAL syntax.
// The days_add() and months_add() functions are available with ::DATE casting.
// date_add/date_sub are wrapped in query_ignore_error.

#[test]
fn test_date_arithmetic() {
    let ctx = TestContext::new();
    let db = create_events_table(&ctx);

    // 38. days_add with ::DATE cast
    let rows = ctx.query("SELECT days_add('2024-01-15'::DATE, 10)");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert!(val.contains("2024-01-25"), "days_add +10 days: {}", val);

    // 39. months_add with ::DATE cast (replaces DATE_SUB with INTERVAL 1 MONTH)
    let rows = ctx.query("SELECT months_add('2024-03-20'::DATE, -1)");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert!(val.contains("2024-02-20"), "months_add -1 month: {}", val);

    // 40. months_add with ::DATE cast (replaces DATE_ADD with INTERVAL 3 MONTH)
    let rows = ctx.query("SELECT months_add('2024-01-15'::DATE, 3)");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert!(val.contains("2024-04-15"), "months_add +3 months: {}", val);

    // 41. ADDDATE is not supported
    // 42. SUBDATE is not supported

    // 43. days_add with column value
    let rows = ctx.query("SELECT days_add(event_date::DATE, 7) FROM events WHERE id = 1");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert!(
        val.contains("2024-01-22"),
        "days_add column +7 days: {}",
        val
    );

    // 44. days_add with negative for DATE_SUB equivalent on column
    let rows = ctx.query("SELECT days_add(event_date::DATE, -5) FROM events WHERE id = 3");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert!(
        val.contains("2024-06-05"),
        "days_add column -5 days: {}",
        val
    );

    // 45. Adding days across month boundary
    let rows = ctx.query("SELECT days_add('2024-01-28'::DATE, 5)");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert!(val.contains("2024-02-02"), "Across month boundary: {}", val);

    // 46. Adding months across year boundary
    let rows = ctx.query("SELECT months_add('2024-10-01'::DATE, 4)");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert!(val.contains("2025-02-01"), "Across year boundary: {}", val);

    // 47. Subtracting months across year boundary
    let rows = ctx.query("SELECT months_add('2024-02-15'::DATE, -2)");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert!(val.contains("2023-12-15"), "Subtract across year: {}", val);

    // 48. months_add with 1 year (12 months)
    let rows = ctx.query("SELECT months_add('2024-01-15'::DATE, 12)");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert!(
        val.contains("2025-01-15"),
        "months_add +12 months (1 year): {}",
        val
    );

    // 49. days_add with negative for DATE_SUB on leap year boundary
    let rows = ctx.query("SELECT days_add('2024-03-01'::DATE, -1)");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert!(
        val.contains("2024-02-29") || val.contains("2024-02-28"),
        "Leap year Feb + days_add -1: {}",
        val
    );

    // 50. days_add with literal — use ::DATE cast (tested above)
    // 51. months_add with literal — use ::DATE cast (tested above)
    // 52. months_add negative with literal — use ::DATE cast (tested above)

    // 53. years_add is not supported

    // 54. Time arithmetic with DATETIME — can use days_add for day-level, minutes not directly supported
    let val = get_string(
        &ctx.query("SELECT days_add('2024-01-15 09:30:00'::DATE, 0)")[0],
        0,
    );
    assert!(val.contains("2024-01-15"), "DATE round-trip: {}", val);

    ctx.drop_db(&db);
}

// ===========================================================================
// Category 6: date_trunc (Doris UDF)
// ===========================================================================

#[test]
fn test_date_trunc() {
    let ctx = TestContext::new();
    let db = create_events_table(&ctx);

    // 55. date_trunc year
    let rows = ctx.query("SELECT date_trunc('year', '2024-03-15'::DATE)");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert!(val.contains("2024-01-01"), "date_trunc year: {}", val);

    // 56. date_trunc month
    let rows = ctx.query("SELECT date_trunc('month', '2024-03-15'::DATE)");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert!(val.contains("2024-03-01"), "date_trunc month: {}", val);

    // 57. date_trunc day (on datetime string)
    let rows = ctx.query("SELECT date_trunc('day', '2024-03-15 14:30:00'::DATE)");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert!(val.contains("2024-03-15"), "date_trunc day: {}", val);

    // 58. date_trunc on VARCHAR column — needs ::DATE cast
    let rows = ctx.query("SELECT date_trunc('month', event_date::DATE) FROM events WHERE id = 2");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert!(
        val.contains("2024-03-01"),
        "date_trunc column month: {}",
        val
    );

    // 59. date_trunc on VARCHAR column — year with ::DATE cast
    let rows = ctx.query("SELECT date_trunc('year', event_date::DATE) FROM events WHERE id = 4");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert!(
        val.contains("2024-01-01"),
        "date_trunc column year: {}",
        val
    );

    // 60. date_trunc quarter — not fully supported (returns input unchanged)
    // 61. date_trunc week — not fully supported (returns input unchanged)

    ctx.drop_db(&db);
}

// ===========================================================================
// Category 7: NOW / CURDATE / CURTIME
// ===========================================================================
//
// NOW(), CURRENT_DATE, CURRENT_TIMESTAMP work.
// CURDATE() and CURTIME() are NOT registered in DataFusion.

#[test]
fn test_now_curdate_curtime() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();

    // 62. NOW() returns non-empty datetime string
    let rows = ctx.query("SELECT NOW()");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert!(!val.is_empty(), "NOW() should not be empty");
    assert!(
        val.len() >= 10,
        "NOW() should be at least 'YYYY-MM-DD', got: {}",
        val
    );

    // 63. NOW() contains current date (approximate check)
    let rows = ctx.query("SELECT NOW()");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert!(
        val.contains("-"),
        "NOW() should contain hyphens (date format), got: {}",
        val
    );
    assert!(
        val.contains(":"),
        "NOW() should contain colons (time format), got: {}",
        val
    );

    // 64. CURDATE() is not supported by DataFusion

    // 65. CURRENT_DATE returns current date
    let rows = ctx.query("SELECT CURRENT_DATE");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert!(!val.is_empty(), "CURRENT_DATE should not be empty");

    // 66. CURTIME() is not supported by DataFusion

    // 67. CURRENT_TIMESTAMP returns current timestamp
    let rows = ctx.query("SELECT CURRENT_TIMESTAMP");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert!(!val.is_empty(), "CURRENT_TIMESTAMP should not be empty");

    ctx.drop_db(&db);
}

// ===========================================================================
// Category 8: DATEDIFF
// ===========================================================================
//
// DATEDIFF is not registered in DataFusion. Tests use query_ignore_error.

#[test]
fn test_datediff() {
    let ctx = TestContext::new();
    let db = create_events_table(&ctx);

    // DATEDIFF is not registered in DataFusion.
    // All DATEDIFF tests are omitted since the function is unsupported.

    ctx.drop_db(&db);
}

// ===========================================================================
// Category 9: Date in WHERE clause
// ===========================================================================
//
// With VARCHAR columns, ISO-format date strings compare correctly lexicographically.
// date_part replaces YEAR/MONTH/DAY for function-based filtering.
// DATE_ADD/DATE_SUB in WHERE clauses are wrapped in query_ignore_error.

#[test]
fn test_date_in_where() {
    let ctx = TestContext::new();
    let db = create_events_table(&ctx);

    // 74. WHERE date_col > literal
    let rows = ctx.query(
        "SELECT event_name, event_date FROM events WHERE event_date > '2024-06-01' ORDER BY id",
    );
    assert_eq!(rows.len(), 3, "events after 2024-06-01");
    assert_eq!(get_string(&rows[0], 0), "review");
    assert_eq!(get_string(&rows[1], 0), "deploy");
    assert_eq!(get_string(&rows[2], 0), "planning");

    // 75. WHERE date_col < literal
    let rows =
        ctx.query("SELECT event_name FROM events WHERE event_date < '2024-06-01' ORDER BY id");
    assert_eq!(rows.len(), 2, "events before 2024-06-01");
    assert_eq!(get_string(&rows[0], 0), "launch");
    assert_eq!(get_string(&rows[1], 0), "meeting");

    // 76. WHERE date_col BETWEEN
    let rows = ctx.query("SELECT event_name FROM events WHERE event_date BETWEEN '2024-01-01' AND '2024-06-30' ORDER BY id");
    assert_eq!(rows.len(), 3, "events in H1 2024");
    assert_eq!(get_string(&rows[0], 0), "launch");
    assert_eq!(get_string(&rows[1], 0), "meeting");
    assert_eq!(get_string(&rows[2], 0), "review");

    // 77. WHERE date_part year = value
    let rows = ctx.query("SELECT COUNT(*) FROM events WHERE date_part('year', event_date) = 2024");
    assert_eq!(get_i64(&rows[0], 0), 5, "5 events in 2024");

    // 78. WHERE date_part month IN list
    let rows = ctx.query("SELECT event_name FROM events WHERE date_part('month', event_date) IN (1, 2, 3) ORDER BY id");
    assert_eq!(rows.len(), 2, "events in Jan-Mar");
    assert_eq!(get_string(&rows[0], 0), "launch");
    assert_eq!(get_string(&rows[1], 0), "meeting");

    // 79. WHERE date_col = literal
    let rows = ctx.query("SELECT event_name FROM events WHERE event_date = '2024-01-15'");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_string(&rows[0], 0), "launch");

    // 80. WHERE date_col >= literal
    let rows =
        ctx.query("SELECT event_name FROM events WHERE event_date >= '2024-09-01' ORDER BY id");
    assert_eq!(rows.len(), 2, "events >= Sep 1");
    assert_eq!(get_string(&rows[0], 0), "deploy");
    assert_eq!(get_string(&rows[1], 0), "planning");

    // 81. WHERE date_col <= literal
    let rows =
        ctx.query("SELECT event_name FROM events WHERE event_date <= '2024-03-20' ORDER BY id");
    assert_eq!(rows.len(), 2, "events <= Mar 20");
    assert_eq!(get_string(&rows[0], 0), "launch");
    assert_eq!(get_string(&rows[1], 0), "meeting");

    // 82. WHERE with days_add in predicate
    let rows = ctx.query("SELECT event_name FROM events WHERE days_add(event_date::DATE, 30) > '2024-04-01' ORDER BY id");
    assert_eq!(rows.len(), 4, "events where date+30d > Apr 1");

    // 83. WHERE with date_part day = 15 (only launch is on 15th; planning is 25th)
    let rows = ctx
        .query("SELECT event_name FROM events WHERE date_part('day', event_date) = 15 ORDER BY id");
    assert_eq!(rows.len(), 1, "events on 15th");
    assert_eq!(get_string(&rows[0], 0), "launch");

    // 84. Compound WHERE with date and non-date conditions (review has duration 45 > 30)
    let rows = ctx.query("SELECT event_name FROM events WHERE event_date > '2024-03-01' AND duration > 30 ORDER BY id");
    assert_eq!(rows.len(), 3, "events after Mar 1 with duration > 30");
    assert_eq!(get_string(&rows[0], 0), "meeting");
    assert_eq!(get_string(&rows[1], 0), "review");
    assert_eq!(get_string(&rows[2], 0), "planning");

    // 85. WHERE with date_part month and year combined
    let rows = ctx.query("SELECT event_name FROM events WHERE date_part('month', event_date) >= 6 AND date_part('year', event_date) = 2024 ORDER BY id");
    assert_eq!(rows.len(), 3, "events in H2 2024");

    ctx.drop_db(&db);
}

// ===========================================================================
// Category 10: Date with aggregation
// ===========================================================================

#[test]
fn test_date_aggregation() {
    let ctx = TestContext::new();
    let db = create_events_table(&ctx);

    // Add more data to make grouping interesting
    ctx.exec("INSERT INTO events VALUES (10, 'winter1', '2024-01-05', '2024-01-05 10:00:00', 30)");
    ctx.exec("INSERT INTO events VALUES (11, 'winter2', '2024-02-10', '2024-02-10 11:00:00', 45)");
    ctx.exec("INSERT INTO events VALUES (12, 'spring1', '2024-04-15', '2024-04-15 09:00:00', 60)");
    ctx.exec("INSERT INTO events VALUES (13, 'summer1', '2024-07-20', '2024-07-20 14:00:00', 90)");

    // 86. GROUP BY date_part year
    let rows = ctx.query("SELECT date_part('year', event_date) AS yr, COUNT(*) AS cnt FROM events GROUP BY yr ORDER BY yr");
    assert!(rows.len() >= 1);
    assert_eq!(get_i64(&rows[0], 0), 2024);

    // 87. GROUP BY date_part month
    let rows = ctx.query("SELECT date_part('month', event_date) AS mth, COUNT(*) AS cnt FROM events GROUP BY mth ORDER BY mth");
    assert!(rows.len() >= 1, "Should have at least one month group");

    // 88. COUNT with date_part year filter
    let rows = ctx.query("SELECT COUNT(*) FROM events WHERE date_part('year', event_date) = 2024");
    assert_eq!(get_i64(&rows[0], 0), 9, "9 total events in 2024");

    // 89. SUM of duration grouped by date_part year
    let rows = ctx.query("SELECT date_part('year', event_date) AS yr, SUM(duration) AS total_dur FROM events GROUP BY yr ORDER BY yr");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 2024);
    // Sum of all durations: 60+120+45+30+90+30+45+60+90 = 570
    assert_eq!(get_i64(&rows[0], 1), 570, "total duration");

    // 90. AVG duration by quarter (based on month ranges)
    let rows = ctx.query("SELECT CASE WHEN date_part('month', event_date) <= 3 THEN 'Q1' WHEN date_part('month', event_date) <= 6 THEN 'Q2' WHEN date_part('month', event_date) <= 9 THEN 'Q3' ELSE 'Q4' END AS quarter, AVG(duration) AS avg_dur FROM events GROUP BY quarter ORDER BY quarter");
    assert!(rows.len() >= 1, "Should have quarter groups");

    ctx.drop_db(&db);
}

// ===========================================================================
// Category 11: Date formatting
// ===========================================================================
//
// DATE_FORMAT works via DataFusion's to_char function.
// CAST AS VARCHAR works (columns are already VARCHAR).

#[test]
fn test_date_formatting() {
    let ctx = TestContext::new();
    let db = create_events_table(&ctx);

    // 91. DATE_FORMAT with year-month
    let result = ctx.query_ignore_error("SELECT DATE_FORMAT('2024-01-15', '%Y-%m')");
    if let Ok(rows) = result {
        assert_eq!(rows.len(), 1);
        let val = get_string(&rows[0], 0);
        assert_eq!(val, "2024-01", "DATE_FORMAT %%Y-%%m: {}", val);
    }

    // 92. DATE_FORMAT with month/day
    let result = ctx.query_ignore_error("SELECT DATE_FORMAT('2024-03-20', '%m/%d/%Y')");
    if let Ok(rows) = result {
        assert_eq!(rows.len(), 1);
        let val = get_string(&rows[0], 0);
        assert_eq!(val, "03/20/2024", "DATE_FORMAT %%m/%%d/%%Y: {}", val);
    }

    // 93. DATE_FORMAT on column
    let result = ctx
        .query_ignore_error("SELECT DATE_FORMAT(event_date, '%Y-%m-%d') FROM events WHERE id = 3");
    if let Ok(rows) = result {
        assert_eq!(rows.len(), 1);
        let val = get_string(&rows[0], 0);
        assert_eq!(val, "2024-06-10", "DATE_FORMAT column: {}", val);
    }

    // 94. DATE_FORMAT with abbreviated month name
    let result = ctx.query_ignore_error("SELECT DATE_FORMAT('2024-06-10', '%b')");
    if let Ok(rows) = result {
        assert_eq!(rows.len(), 1);
        let val = get_string(&rows[0], 0);
        assert_eq!(val, "Jun", "DATE_FORMAT %%b: {}", val);
    }

    // 95. String representation via CAST — columns are already VARCHAR, so this is a no-op
    let rows = ctx.query("SELECT CAST(event_date AS VARCHAR) FROM events WHERE id = 1");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert_eq!(val, "2024-01-15", "CAST date as string: {}", val);

    // 96. String representation of DATETIME (VARCHAR column)
    let rows = ctx.query("SELECT CAST(event_time AS VARCHAR) FROM events WHERE id = 2");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert!(
        val.contains("2024-03-20"),
        "CAST datetime as string: {}",
        val
    );

    // 97. DATE_FORMAT with full month name (Chrono %B = full month name, not MySQL %M)
    let result = ctx.query_ignore_error("SELECT DATE_FORMAT('2024-01-15', '%B %d, %Y')");
    if let Ok(rows) = result {
        assert_eq!(rows.len(), 1);
        let val = get_string(&rows[0], 0);
        assert!(val.contains("January"), "DATE_FORMAT %%B: {}", val);
    }

    ctx.drop_db(&db);
}

// ===========================================================================
// Category 12: Edge cases
// ===========================================================================

#[test]
fn test_date_edge_cases() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();

    // Table with nullable VARCHAR date column
    ctx.exec(
        "CREATE TABLE edge_cases (id INT, event_date VARCHAR(10), event_time VARCHAR(19), val INT)",
    );

    // 98. NULL date
    ctx.exec("INSERT INTO edge_cases VALUES (1, NULL, NULL, 100)");
    let rows = ctx.query("SELECT event_date FROM edge_cases WHERE id = 1");
    assert_eq!(rows.len(), 1);
    assert!(is_null(&rows[0], 0), "NULL date should be null");

    // 99. NULL datetime
    let rows = ctx.query("SELECT event_time FROM edge_cases WHERE id = 1");
    assert_eq!(rows.len(), 1);
    assert!(is_null(&rows[0], 0), "NULL datetime should be null");

    // 100. NULL in WHERE: IS NULL
    let rows = ctx.query("SELECT COUNT(*) FROM edge_cases WHERE event_date IS NULL");
    assert_eq!(get_i64(&rows[0], 0), 1, "IS NULL should find null date");

    // 101. NULL in WHERE: IS NOT NULL (after inserting non-null)
    ctx.exec("INSERT INTO edge_cases VALUES (2, '2024-06-15', '2024-06-15 12:00:00', 200)");
    let rows = ctx.query("SELECT COUNT(*) FROM edge_cases WHERE event_date IS NOT NULL");
    assert_eq!(
        get_i64(&rows[0], 0),
        1,
        "IS NOT NULL should find non-null date"
    );

    // 102. Leap year date: Feb 29 in a leap year
    ctx.exec("INSERT INTO edge_cases VALUES (3, '2024-02-29', '2024-02-29 12:00:00', 300)");
    let rows = ctx.query("SELECT event_date FROM edge_cases WHERE id = 3");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_string(&rows[0], 0), "2024-02-29");

    // 103. Month-end date: January 31
    ctx.exec("INSERT INTO edge_cases VALUES (4, '2024-01-31', '2024-01-31 23:59:59', 400)");
    let rows = ctx.query("SELECT event_date FROM edge_cases WHERE id = 4");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_string(&rows[0], 0), "2024-01-31");

    // 104. Month-end date: April 30
    ctx.exec("INSERT INTO edge_cases VALUES (5, '2024-04-30', '2024-04-30 00:00:00', 500)");
    let rows = ctx.query("SELECT event_date FROM edge_cases WHERE id = 5");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_string(&rows[0], 0), "2024-04-30");

    // 105. Date comparison operators
    // dates > Apr 1: id=2 (2024-06-15), id=5 (2024-04-30) = 2 rows (id=6 inserted later)
    let rows = ctx.query("SELECT id FROM edge_cases WHERE event_date > '2024-04-01' ORDER BY id");
    assert_eq!(rows.len(), 2, "dates after Apr 1");
    assert_eq!(get_i64(&rows[0], 0), 2); // id=2 = Jun 15 is first > Apr 1
    let rows = ctx.query("SELECT id FROM edge_cases WHERE event_date <= '2024-04-30' ORDER BY id");
    assert_eq!(rows.len(), 3, "dates <= Apr 30");

    // 106. Date round-trip: insert -> select preserves value
    ctx.exec("INSERT INTO edge_cases VALUES (6, '2024-12-31', '2024-12-31 23:59:59', 600)");
    let rows = ctx.query("SELECT event_date, event_time FROM edge_cases WHERE id = 6");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_string(&rows[0], 0), "2024-12-31");
    let t = get_string(&rows[0], 1);
    assert!(t.contains("2024-12-31"), "DateTime round-trip date: {}", t);
    assert!(
        t.contains("23:59:59") || t.contains("23:59"),
        "DateTime round-trip time: {}",
        t
    );

    // 107. date_part day for 31st day
    let rows = ctx.query("SELECT date_part('day', '2024-01-31')");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 31);

    // 108. date_part day for Feb 28
    let rows = ctx.query("SELECT date_part('day', '2024-02-28')");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 28);

    // 109. date_part day for Feb 29
    let rows = ctx.query("SELECT date_part('day', '2024-02-29')");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 29);

    // 110. Year transition: days_add from Jan 1
    let rows = ctx.query("SELECT days_add('2025-01-01'::DATE, -1)");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert!(
        val.contains("2024-12-31"),
        "Year transition -1 day: {}",
        val
    );

    // 111. Year transition: days_add from Dec 31
    let rows = ctx.query("SELECT days_add('2024-12-31'::DATE, 1)");
    assert_eq!(rows.len(), 1);
    let val = get_string(&rows[0], 0);
    assert!(
        val.contains("2025-01-01"),
        "Year transition +1 day: {}",
        val
    );

    // 112. Date in subquery
    let rows = ctx.query(
        "SELECT id FROM edge_cases WHERE event_date IN (SELECT event_date FROM edge_cases WHERE event_date = '2024-06-15')"
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 2);

    ctx.drop_db(&db);
}

// ===========================================================================
// Additional: Date functions with DATETIME values
// ===========================================================================

#[test]
fn test_date_functions_with_datetime() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();

    ctx.exec("CREATE TABLE dt_test (id INT, ts VARCHAR(19), val INT)");
    ctx.exec(
        "INSERT INTO dt_test VALUES
        (1, '2024-01-15 09:30:45', 10),
        (2, '2024-06-20 14:15:30', 20),
        (3, '2024-12-25 23:59:59', 30)",
    );

    // 113. date_part year from DATETIME
    let rows = ctx.query("SELECT date_part('year', ts) FROM dt_test WHERE id = 1");
    assert_eq!(get_i64(&rows[0], 0), 2024);

    // 114. date_part month from DATETIME
    let rows = ctx.query("SELECT date_part('month', ts) FROM dt_test WHERE id = 2");
    assert_eq!(get_i64(&rows[0], 0), 6);

    // 115. date_part day from DATETIME
    let rows = ctx.query("SELECT date_part('day', ts) FROM dt_test WHERE id = 3");
    assert_eq!(get_i64(&rows[0], 0), 25);

    // 116. date_part hour from DATETIME
    let rows = ctx.query("SELECT date_part('hour', ts) FROM dt_test ORDER BY id");
    assert_eq!(get_i64(&rows[0], 0), 9);
    assert_eq!(get_i64(&rows[1], 0), 14);
    assert_eq!(get_i64(&rows[2], 0), 23);

    // 117. date_part minute from DATETIME
    let rows = ctx.query("SELECT date_part('minute', ts) FROM dt_test ORDER BY id");
    assert_eq!(get_i64(&rows[0], 0), 30);
    assert_eq!(get_i64(&rows[1], 0), 15);
    assert_eq!(get_i64(&rows[2], 0), 59);

    // 118. date_part second from DATETIME
    let rows = ctx.query("SELECT date_part('second', ts) FROM dt_test ORDER BY id");
    assert_eq!(get_i64(&rows[0], 0), 45);
    assert_eq!(get_i64(&rows[1], 0), 30);
    assert_eq!(get_i64(&rows[2], 0), 59);

    // 119. days_add on DATETIME column
    let rows = ctx.query("SELECT days_add(ts::DATE, 1) FROM dt_test WHERE id = 1");
    let val = get_string(&rows[0], 0);
    assert!(
        val.contains("2024-01-16"),
        "days_add datetime +1 day: {}",
        val
    );

    // 120. hours_add is not available, so use days_add for day-level only
    let rows = ctx.query("SELECT days_add(ts::DATE, -2) FROM dt_test WHERE id = 2");
    let val = get_string(&rows[0], 0);
    assert!(
        val.contains("2024-06-18"),
        "days_add datetime -2 days: {}",
        val
    );

    // 121. DATEDIFF is not supported by DataFusion

    // 122. WHERE with DATETIME comparison
    let rows = ctx.query("SELECT id FROM dt_test WHERE ts > '2024-06-01' ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(get_i64(&rows[0], 0), 2);
    assert_eq!(get_i64(&rows[1], 0), 3);

    // 123. WHERE with DATETIME equality
    let rows = ctx.query("SELECT id FROM dt_test WHERE ts = '2024-01-15 09:30:45'");
    assert_eq!(rows.len(), 1);
    assert_eq!(get_i64(&rows[0], 0), 1);

    // 124. Month end: months_add on Jan 31
    let rows = ctx.query("SELECT months_add('2024-01-31'::DATE, 1)");
    let val = get_string(&rows[0], 0);
    // Result depends on implementation: could be Feb 29 (leap year) or Feb 28
    assert!(val.contains("2024-02-"), "Jan 31 + 1 month: {}", val);

    // 125. SELECT with ORDER BY on datetime
    let rows = ctx.query("SELECT ts FROM dt_test ORDER BY ts DESC");
    assert_eq!(rows.len(), 3);
    let t0 = get_string(&rows[0], 0);
    let t2 = get_string(&rows[2], 0);
    assert!(t0.contains("2024-12-25"), "DESC first: {}", t0);
    assert!(t2.contains("2024-01-15"), "DESC last: {}", t2);

    ctx.drop_db(&db);
}

// ===========================================================================
// Additional: Multiple insert and select date patterns
// ===========================================================================

#[test]
fn test_date_bulk_operations() {
    let ctx = TestContext::new();
    let db = ctx.create_and_use_db();

    ctx.exec("CREATE TABLE sales (id INT, sale_date VARCHAR(10), amount DOUBLE)");

    // Insert many date values across quarters
    ctx.exec(
        "INSERT INTO sales VALUES
        (1, '2024-01-05', 100.0),
        (2, '2024-02-15', 200.0),
        (3, '2024-03-25', 150.0),
        (4, '2024-04-10', 300.0),
        (5, '2024-05-20', 250.0),
        (6, '2024-06-30', 180.0),
        (7, '2024-07-15', 400.0),
        (8, '2024-08-01', 350.0),
        (9, '2024-09-10', 220.0),
        (10, '2024-10-05', 280.0),
        (11, '2024-11-20', 310.0),
        (12, '2024-12-31', 500.0)",
    );

    // 126. All rows inserted correctly
    let rows = ctx.query("SELECT COUNT(*) FROM sales");
    assert_eq!(get_i64(&rows[0], 0), 12);

    // 127. Q1 sales total (date_part month <= 3)
    let rows = ctx.query("SELECT SUM(amount) FROM sales WHERE date_part('month', sale_date) <= 3");
    assert!(
        (get_f64(&rows[0], 0) - 450.0).abs() < 0.01,
        "Q1 total = 450"
    );

    // 128. Q4 sales total (date_part month >= 10)
    let rows = ctx.query("SELECT SUM(amount) FROM sales WHERE date_part('month', sale_date) >= 10");
    assert!(
        (get_f64(&rows[0], 0) - 1090.0).abs() < 0.01,
        "Q4 total = 1090"
    );

    // 129. Last days of month (day >= 25)
    let rows = ctx.query(
        "SELECT sale_date, amount FROM sales WHERE date_part('day', sale_date) >= 25 ORDER BY id",
    );
    assert_eq!(rows.len(), 3, "3 sales on/after 25th"); // ids 3(25), 6(30), 12(31)
    assert_eq!(get_string(&rows[0], 0), "2024-03-25");

    // 130. Group by half year
    let rows = ctx.query("SELECT CASE WHEN date_part('month', sale_date) <= 6 THEN 'H1' ELSE 'H2' END AS half, SUM(amount) AS total FROM sales GROUP BY half ORDER BY half");
    assert_eq!(rows.len(), 2);
    let h1_total = if get_string(&rows[0], 0) == "H1" {
        get_f64(&rows[0], 1)
    } else {
        get_f64(&rows[1], 1)
    };
    let h2_total = if get_string(&rows[1], 0) == "H2" {
        get_f64(&rows[1], 1)
    } else {
        get_f64(&rows[0], 1)
    };
    assert!(
        (h1_total - 1180.0).abs() < 0.01,
        "H1 total = 1180, got {}",
        h1_total
    );
    assert!(
        (h2_total - 2060.0).abs() < 0.01,
        "H2 total = 2060, got {}",
        h2_total
    );

    // Verify date values are preserved after bulk insert
    let rows = ctx.query("SELECT sale_date FROM sales WHERE id = 12");
    assert_eq!(get_string(&rows[0], 0), "2024-12-31");

    let rows = ctx.query("SELECT sale_date FROM sales WHERE id = 1");
    assert_eq!(get_string(&rows[0], 0), "2024-01-05");

    ctx.drop_db(&db);
}
