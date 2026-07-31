# Test Investigation Report

## Problem Statement

5 integration tests fail when run together but pass individually:
- `test_insert_100_rows_update_all`
- `test_insert_200_rows_and_count`
- `test_insert_delete_half_verify`
- `test_insert_update_all_verify`
- `test_sequential_insert_select_cycles`

## Failure Pattern

All failures show data accumulation across tests:
- Expected 100 rows, got 1900 rows
- Expected 200 rows, got 1680 rows  
- Expected 10 rows, got 450 rows
- Expected 10 rows, got 183 rows
- Expected 1 row, got 28 rows

## Root Cause Analysis

### Hypothesis 1: Database Isolation Issue ❌
- Tested database isolation manually
- Each database has its own `tables` DashMap
- Tables with same name in different databases are properly isolated
- DROP DATABASE correctly removes databases from storage

### Hypothesis 2: Connection ID Reuse ❌
- Connection IDs generated with atomic counter
- IDs increment monotonically (1, 2, 3, ...)
- No ID reuse across connections

### Hypothesis 3: Missing on_disconnect Cleanup ⚠️ 
**CURRENT INVESTIGATION**

The `AdbMysqlHandler` tracks current database per connection:
```rust
current_databases: DashMap<u32, String>
```

**Flow:**
1. Test creates connection, gets conn_id (e.g., 42)
2. Test executes `USE test_db_XYZ`
3. Handler sets `current_databases[42] = "test_db_XYZ"`
4. Test inserts data
5. Test ends, connection dropped
6. Server calls `on_disconnect(42)`
7. Handler removes `current_databases[42]`
8. Next test creates connection with conn_id=43...

**Potential Issues:**
- Asynchronous connection cleanup (race condition)
- Connection not properly closed by test harness
- Drop implementation needed for Connection

### Hypothesis 4: Shared Server State ✅ CONFIRMED

All tests share the SAME server instance:
```rust
lazy_static! {
    static ref SERVER: Arc<harness::E2eServer> = {
        harness::shared_server(MYSQL_PORT)
    };
}
```

This means:
- Same `AdbMysqlStorage` instance
- Same `AdbMysqlHandler` instance
- Same `current_databases` DashMap

## Tests Performed

1. ✅ Individual tests all pass
2. ✅ Database isolation works correctly
3. ✅ DROP DATABASE properly removes data
4. ✅ Connection IDs are unique
5. ⚠️ Added Drop implementation for Connection (didn't fix)
6. 🔄 Added debug logging (build in progress)

## Next Steps

1. Capture connection lifecycle logs
2. Verify `on_disconnect` is being called
3. Check if connections are properly closed
4. Consider alternative: explicit connection cleanup in test harness
5. Consider alternative: ensure each test uses fully qualified table names

## Code Locations

- Test file: `tests/integration/tests/suites/e2e_edge_case_tests.rs`
- Handler: `crates/adb-mysql-protocol/src/handler.rs`
- Storage: `crates/adb-mysql-protocol/src/storage.rs`
- Connection: `crates/mysql-protocol/src/connection.rs`
- Server: `crates/mysql-protocol/src/server.rs`
- Test harness: `tests/integration/src/harness.rs`

## Workaround Options

1. Run tests with `--test-threads=1` (doesn't fix the issue)
2. Add explicit delay between tests
3. Add connection close in test harness
4. Use fully qualified table names in queries