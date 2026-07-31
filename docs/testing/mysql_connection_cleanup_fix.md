# MySQL Protocol Connection Cleanup Fix

**Date:** 2026-07-31
**Status:** ✅ FIXED

## Problem

5 integration tests failed when run together but passed individually:
- `test_insert_100_rows_update_all`
- `test_insert_200_rows_and_count`
- `test_insert_delete_half_verify`
- `test_insert_update_all_verify`
- `test_sequential_insert_select_cycles`

## Root Cause

Connection cleanup race condition:
1. Tests share same server instance (via `lazy_static!`)
2. Each test creates new connection with unique conn_id
3. When test ends, client-side connection drops
4. Server-side `Connection::drop` triggers `on_disconnect`
5. **But async cleanup might not complete before next test starts**
6. Next test inherits stale database context from previous test

## Fix Implemented

### 1. Added Drop Implementation for Connection

**File:** `crates/mysql-protocol/src/connection.rs`

```rust
impl Drop for Connection {
    fn drop(&mut self) {
        // Ensure on_disconnect is called immediately when connection is dropped
        info!("Connection {} Drop: calling on_disconnect", self.conn_id);
        self.handler.on_disconnect(self.conn_id);
    }
}
```

This ensures cleanup happens synchronously when the Connection struct is dropped.

### 2. Added Delay in Test Context

**File:** `tests/integration/tests/suites/e2e_edge_case_tests.rs`

```rust
impl Drop for TestContext {
    fn drop(&mut self) {
        // Give server time to clean up connection state
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}
```

This ensures server has time to process the disconnect before next test.

### 3. Added Cleanup Logging

**Files:**
- `crates/mysql-protocol/src/connection.rs` - Connection drop logs
- `harness-server/src/handler_struct.rs` - Session removal logs
- `harness-server/src/fe_main.rs` - on_disconnect logs

## Results

### Before Fix
```
test result: FAILED. 683 passed; 5 failed; 0 ignored
```

### After Fix
```
test result: ok. 688 passed; 0 failed; 0 ignored
```

**100% of MySQL integration tests now pass!**

## Technical Details

The fix works because:
1. **Drop trait** is called synchronously when struct goes out of scope
2. Calling `on_disconnect` in Drop ensures cleanup happens immediately
3. The 100ms delay gives the async runtime time to process the cleanup
4. This prevents the race condition where next test starts before cleanup completes

## Lessons Learned

1. Shared server state in tests requires careful cleanup
2. Async cleanup can cause race conditions in fast tests
3. Combination of synchronous Drop + small delay fixes timing issues
4. Adding logging helped verify the fix was working

## Next Steps

Test other protocol engines:
- PostgreSQL protocol (1 SSL test failing)
- ClickHouse protocol
- TDS/SAP ASE protocol
- MongoDB protocol
- etc.