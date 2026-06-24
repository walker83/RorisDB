# Contributing to HarnessDB

Thanks for your interest in contributing! Here's how to get started.

## Development Setup

```bash
git clone https://github.com/walker83/HarnessDB.git
cd HarnessDB
cargo build
cargo test --workspace
```

## Project Structure

```
crates/
  mysql-protocol/      # MySQL wire protocol server
  pg-protocol/         # PostgreSQL wire protocol
  redis-protocol/      # Redis RESP protocol
  mongodb-protocol/    # MongoDB wire protocol
  clickhouse-protocol/ # ClickHouse HTTP protocol
  elasticsearch-protocol/ # ES REST API
  maxcompute-protocol/ # Alibaba Cloud MaxCompute
  ...
  fe-sql-parser/       # SQL parsing
  fe-catalog/          # Database/table metadata
  fe-storage/          # Parquet storage layer
  fe-datafusion/       # DataFusion integration
  types/               # Shared types (Block, Schema, etc.)
```

## Good First Issues

- Implement Cassandra INSERT/UPDATE/DELETE (currently no-ops)
- Add InfluxDB time-range filtering in SELECT
- Improve Oracle DML support
- Add batch operations to TableStore protocol
- Add more integration tests for any protocol

## Code Style

- Rust 2024 edition
- Run `cargo clippy` before submitting
- Run `cargo test --workspace` to verify nothing breaks
- Keep protocol implementations self-contained in their own crates

## Submitting Changes

1. Fork the repo
2. Create a feature branch (`git checkout -b my-feature`)
3. Make your changes
4. Run `cargo test --workspace` and `cargo clippy`
5. Submit a pull request

## Reporting Issues

- Include your OS, Rust version, and which protocol you're testing
- Include the exact client command/connection string you used
- Include error messages or unexpected behavior
