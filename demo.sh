#!/bin/bash
# HarnessDB Quick Demo Script
# Demonstrates MySQL, Redis, MongoDB, ClickHouse, Elasticsearch protocols

set -e

echo "=== HarnessDB Quick Demo ==="
echo ""

# Check if binary exists
if [ ! -f "./target/release/harness-db" ]; then
    echo "Building HarnessDB..."
    cargo build --release
fi

# Start server in background
echo "Starting HarnessDB server..."
rm -rf /tmp/harness-demo-data /tmp/harness-demo-meta
./target/release/harness-db --mysql-port 9031 --data-dir /tmp/harness-demo-data --meta-dir /tmp/harness-demo-meta > /tmp/harness-demo.log 2>&1 &
SERVER_PID=$!
sleep 2

# Cleanup function
cleanup() {
    echo ""
    echo "Stopping server..."
    kill $SERVER_PID 2>/dev/null || true
    rm -rf /tmp/harness-demo-data /tmp/harness-demo-meta
}
trap cleanup EXIT

# Run demo queries
echo "Running demo queries..."
echo ""

mysql -h 127.0.0.1 -P 9031 -uroot <<'EOF'
-- Create analytics database
CREATE DATABASE demo;
USE demo;

-- Create events table (Doris-style)
CREATE TABLE events (
    id INT,
    user_id INT,
    event_type VARCHAR(50),
    amount DECIMAL(10,2),
    occurred_at DATETIME
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1;

-- Insert sample data
INSERT INTO events VALUES
    (1, 100, 'purchase', 99.99, '2024-01-15 10:30:00'),
    (2, 100, 'purchase', 49.50, '2024-01-16 14:20:00'),
    (3, 200, 'view', 0.00, '2024-01-15 11:00:00'),
    (4, 200, 'purchase', 199.99, '2024-01-17 09:15:00'),
    (5, 300, 'purchase', 29.99, '2024-01-18 16:45:00');

-- Basic aggregation
SELECT '=== Basic Aggregation ===' as query;
SELECT event_type, COUNT(*) as count, SUM(amount) as total
FROM events
GROUP BY event_type
ORDER BY total DESC;

-- Window functions
SELECT '=== Running Total (Window Function) ===' as query;
SELECT user_id, event_type, amount,
       SUM(amount) OVER (PARTITION BY user_id ORDER BY occurred_at) as running_total
FROM events
ORDER BY user_id, occurred_at;

-- Filtering with HAVING
SELECT '=== Users with Total Purchases > $100 ===' as query;
SELECT user_id, SUM(amount) as total_spent
FROM events
WHERE event_type = 'purchase'
GROUP BY user_id
HAVING SUM(amount) > 100
ORDER BY total_spent DESC;

-- Date functions
SELECT '=== Events by User ===' as query;
SELECT user_id, COUNT(*) as event_count, SUM(amount) as total_amount
FROM events
GROUP BY user_id
ORDER BY event_count DESC;

-- Complex query with CTE
SELECT '=== Top Spenders (CTE) ===' as query;
WITH user_totals AS (
    SELECT user_id, SUM(amount) as total
    FROM events
    WHERE event_type = 'purchase'
    GROUP BY user_id
)
SELECT user_id, total
FROM user_totals
WHERE total > 50
ORDER BY total DESC;

EOF

echo ""
echo "=== Demo Complete ==="
echo "Server log: /tmp/harness-demo.log"
