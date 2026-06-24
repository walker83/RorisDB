<div align="center">

# 🦎 HarnessDB

### LocalStack for Databases — 14 Protocols, 1 Binary

**One binary. Fourteen protocols. Zero infrastructure.**

[![License](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-2024--edition-orange.svg)](https://www.rust-lang.org)
[![Protocols](https://img.shields.io/badge/Protocols-14-blue.svg)](#-compatibility-matrix)

[English](README.md) · [中文文档](docs/zh/README.md) · [Quick Start](#-quick-start) · [Compatibility](#-compatibility-matrix) · [Architecture](#-architecture)

---

**Stop spinning up 14 different containers just to run integration tests.**

</div>

---

## What is HarnessDB?

HarnessDB is a **local development and CI testing platform** that speaks **14 different database protocols** from a single Rust binary. Think of it as [LocalStack](https://localstack.cloud/) but for databases — instead of simulating AWS services, it simulates MySQL, Redis, MongoDB, PostgreSQL, ClickHouse, Elasticsearch, and more.

**Use it for:**
- Local development without installing 14 database servers
- CI/CD pipelines that need a database stack in seconds
- Integration testing across multiple database protocols
- Alibaba Cloud local development (MaxCompute, Hologres, TableStore)

**Don't use it for:**
- Production workloads — it's a simulation layer, not a replacement for real databases
- High-concurrency, low-latency Redis scenarios
- MongoDB aggregation pipelines in production
- ACID transaction guarantees

> HarnessDB stores everything in Parquet files via Apache DataFusion. It's fast enough for dev/test, but it's not a drop-in production replacement for specialized databases.

## Quick Demo

```bash
# Start HarnessDB — all 14 protocols listen on their default ports
./target/release/harness-db

# Terminal 1: MySQL
mysql -h 127.0.0.1 -P 9030 -uroot -e "CREATE TABLE users (id INT, name VARCHAR(50)); INSERT INTO users VALUES (1, 'Alice'); SELECT * FROM users;"

# Terminal 2: Redis
redis-cli -h 127.0.0.1 -p 6379 SET mykey "hello" && redis-cli -h 127.0.0.1 -p 6379 GET mykey

# Terminal 3: MongoDB
mongosh --host 127.0.0.1 --port 27017 --eval "db.users.insertOne({name: 'Bob', age: 30}); db.users.find()"

# Terminal 4: Elasticsearch
curl -s -X PUT "http://127.0.0.1:9200/my-index/_doc/1" -H 'Content-Type: application/json' -d '{"title": "Hello"}'
curl -s "http://127.0.0.1:9200/my-index/_search" -H 'Content-Type: application/json' -d '{"query": {"match_all": {}}}'

# Terminal 5: ClickHouse
curl -s "http://127.0.0.1:8123/" -d "CREATE TABLE test (id Int32, name String) ENGINE=Memory"
curl -s "http://127.0.0.1:8123/" -d "INSERT INTO test VALUES (1, 'Charlie')"
curl -s "http://127.0.0.1:8123/" -d "SELECT * FROM test"
```

> 📹 **TODO**: Replace this with an asciinema recording or GIF showing all protocols in action.

## 🚀 Quick Start

### Build

```bash
git clone https://github.com/walker83/HarnessDB.git
cd HarnessDB
cargo build --release
```

### Run

```bash
./target/release/harness-db
```

All 14 protocols start listening immediately. No config files needed.

### Use in CI/CD

```yaml
# .github/workflows/test.yml
name: Integration Tests
on: [push]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Build HarnessDB
        run: cargo build --release

      - name: Start database stack
        run: ./target/release/harness-db &
        # MySQL on :9030, Redis on :6379, MongoDB on :27017, etc.

      - name: Wait for HarnessDB
        run: |
          for i in $(seq 1 30); do
            mysql -h 127.0.0.1 -P 9030 -uroot -e "SELECT 1" 2>/dev/null && break
            sleep 1
          done

      - name: Run your tests
        run: |
          # Your app can now connect to MySQL, Redis, MongoDB, etc.
          # No need for services: containers in your workflow!
          npm test
```

This replaces the typical CI setup that requires multiple `services:` containers:

```yaml
# ❌ Before: heavy, slow, complex
services:
  mysql:
    image: mysql:8.0
    ports: ['3306:3306']
    env:
      MYSQL_ROOT_PASSWORD: test
  redis:
    image: redis:7
    ports: ['6379:6379']
  mongo:
    image: mongo:6
    ports: ['27017:27017']
  elasticsearch:
    image: elasticsearch:7.17
    ports: ['9200:9200']

# ✅ After: one binary, starts in <1s
steps:
  - run: ./harness-db &
```

## 📊 Compatibility Matrix

This is the honest truth about what each protocol supports. We believe transparency earns more trust than inflated claims.

### ✅ Full Implementation — Ready for Local Dev & Testing

| Protocol | Port | Commands | Storage | Notes |
|----------|------|----------|---------|-------|
| **MySQL** | 9030 | CREATE/DROP TABLE/DB, INSERT, UPDATE, DELETE, SELECT (JOIN, WHERE, GROUP BY, ORDER BY, LIMIT, aggregates, window functions) | Parquet via DataFusion | MySQL 5.7/8.0 compatible wire protocol |
| **PostgreSQL** | 15432 | Full wire protocol v3, auth (md5/scram-sha-256), extended query, 20+ pg_catalog tables, information_schema | Parquet via DataFusion | Hologres-compatible, works with psql/JDBC/psycopg2 |
| **Redis** | 6379 | 50+ commands: String (GET/SET/MGET/INCR...), Hash (HGET/HSET/HGETALL...), List (LPUSH/RPOP/LRANGE...), Set (SADD/SMEMBERS...), Sorted Set (ZADD/ZRANGE...) | In-memory (DashMap) | RESP2/RESP3, 16 databases, TTL support |
| **MongoDB** | 27017 | insert, find, update, delete, count, aggregate ($match/$group/$sum/$count/$skip/$limit), ismaster/hello | In-memory (DashMap) | OP_MSG + legacy OP_QUERY wire protocol |
| **ClickHouse** | 8123 | SELECT (WHERE, GROUP BY, ORDER BY, LIMIT, LIKE), INSERT, CREATE/DROP TABLE/DB, ALTER TABLE UPDATE/DELETE, SHOW/DESCRIBE | Parquet via DataFusion | HTTP interface, TSV output |
| **Elasticsearch** | 9200 | Document CRUD, bulk API, search (match_all), index create/delete/info, _cat APIs, _cluster/health | Parquet via DataFusion | REST API, proper ES-style JSON responses |
| **MaxCompute** | 9031 | Full REST API, SQL instances (submit/status/result), tunnel upload/download, Aliyun auth (v1/v3/STS) | Parquet via DataFusion | pyodps SDK compatible, SQL translator included |
| **AnalyticDB MySQL** | 3307 | Full SQL (same as MySQL protocol) | Parquet via DataFusion | Uses mysql-protocol under the hood |

### ⚠️ Partial Implementation — Basic Operations Work, Advanced Features Missing

| Protocol | Port | Works | Missing | Notes |
|----------|------|-------|---------|-------|
| **Cassandra** | 9042 | Frame codec v4, startup handshake, SELECT from system tables, CREATE/DROP keyspace/table | INSERT/UPDATE/DELETE are no-ops (return success without persisting) | Good for testing CQL connection logic, not data operations |
| **InfluxDB** | 8086 | Line protocol write, SHOW DATABASES/MEASUREMENTS, CREATE/DROP DATABASE, basic SELECT | WHERE time filtering, InfluxQL, Flux | Write path works, query path is basic |
| **TableStore** | 8087 | Table CRUD, row put/get/update/delete, range queries | Batch operations, conditional updates, atomic counters, TTL | REST API with JSON |
| **Oracle** | 1521 | TNS connect/handshake, SELECT USER/SYSDATE/v$version, basic scalar expressions | DML, table access, PL/SQL, storage | Good for testing Oracle driver connectivity |
| **TDS (SAP ASE)** | 5000 | TDS 5.0 login, SQL text queries, basic result encoding | Prepared statements, cursors, transaction control, proper type mapping | Wire protocol framing works |

### 🔧 Minimal — Proof of Concept

| Protocol | Port | What Works | Status |
|----------|------|------------|--------|
| **Lindorm** | 30030 | 7 text commands (CREATE TABLE, PUT, GET, DELETE, SCAN, LIST, COUNT) | Text-based HBase-like interface, no wire protocol |
| **Vector DB** | 19530 | 5 HTTP endpoints (create collection, insert, search, list, count) | Minimal REST API, no delete/update/filtering |
| **Sybase** | 5000 | Delegates to TDS protocol | Thin wrapper, no Sybase-specific logic |

## 🔧 Configuration

All protocols can be independently enabled/disabled via `config/server.toml`:

```toml
[servers.mysql]
enabled = true
port = 9030

[servers.redis]
enabled = true
port = 6379

[servers.mongodb]
enabled = true
port = 27017

# Disable protocols you don't need
[servers.cassandra]
enabled = false
port = 9042
```

Or via command-line flags:

```bash
./harness-db --mysql-port 9030 --redis-port 6379 --mongodb-port 27017
```

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Client Applications                   │
│  mysql | psql | redis-cli | mongo | curl | clickhouse   │
└────────────────┬────────────────────────────────────────┘
                 │
    ┌────────────┼────────────────────────────────┐
    │            │                                │
    ▼            ▼                                ▼
┌────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐
│ MySQL  │  │  Redis   │  │ MongoDB  │  │ ClickHouse   │
│ :9030  │  │  :6379   │  │ :27017   │  │   :8123      │
└───┬────┘  └────┬─────┘  └────┬─────┘  └──────┬───────┘
    │            │              │                │
    └────────────┴──────────────┴────────────────┘
                 │
                 ▼
        ┌────────────────┐
        │ Protocol Layer │
        │ (14 Protocols) │
        └────────┬───────┘
                 │
                 ▼
        ┌────────────────┐
        │  Query Engine  │
        │  (DataFusion)  │
        └────────┬───────┘
                 │
                 ▼
        ┌────────────────┐
        │ Storage Engine │
        │   (Parquet)    │
        └────────────────┘
```

All SQL-speaking protocols (MySQL, PostgreSQL, ClickHouse, MaxCompute, etc.) share the same DataFusion query engine and Parquet storage. NoSQL protocols (Redis, MongoDB) use in-memory storage optimized for their access patterns.

## 📈 Performance

| Metric | Value |
|--------|-------|
| Binary size | ~50MB |
| Memory (idle) | ~100MB |
| Startup time | <1 second |
| MySQL query latency | 10-50ms |
| Redis operations | In-memory, sub-ms |

## 🧪 Testing

```bash
# Run all tests
cargo test --workspace

# Run integration tests
cargo test -p integration-tests
```

## 🎓 Use Cases

### 1. Local Development

Replace `docker-compose.yml` with 14 database containers:

```bash
# Instead of:
# docker-compose up mysql redis mongo elasticsearch clickhouse

# Just run:
./harness-db
```

Your app connects to the same ports, same protocols. No Docker Desktop eating 8GB RAM.

### 2. CI/CD Pipeline

One step in your GitHub Actions, zero container setup:

```yaml
- name: Start HarnessDB
  run: ./target/release/harness-db &

- name: Run tests
  run: cargo test
  # Tests can use MySQL, Redis, MongoDB, ES, ClickHouse...
```

### 3. Alibaba Cloud Local Development

Test MaxCompute (ODPS), Hologres, and TableStore queries locally without cloud costs:

```python
from odps import ODPS
o = ODPS('harness', 'harness-secret', 'default',
         endpoint='http://localhost:9031/api')
o.execute_sql('SELECT * FROM my_table').wait_for_success()
```

### 4. Database Protocol Testing

Verify your application's database driver compatibility:

```bash
# Test your app against MySQL protocol
./harness-db --only-mysql &
cargo test --features mysql-tests

# Test against PostgreSQL protocol
./harness-db --only-postgres &
cargo test --features pg-tests
```

## 📖 Documentation

- [SQL Reference](docs/en/sql-reference.md)
- [Configuration Guide](docs/en/configuration.md)
- [Architecture](docs/en/architecture.md)
- [Alibaba Cloud Compatibility](docs/alibaba-cloud-compatibility.md)
- [Roadmap](docs/roadmap/README.md)

## 🤝 Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

Good first issues:
- Implement missing Cassandra DML operations
- Add InfluxDB time-range filtering
- Improve Oracle DML support
- Add more protocol integration tests

## 📜 License

Apache License 2.0. See [LICENSE](LICENSE).

## 🙏 Acknowledgments

- **[Apache DataFusion](https://github.com/apache/arrow-datafusion)** — Query engine
- **[Apache Arrow](https://arrow.apache.org)** — Columnar format
- **[Apache Parquet](https://parquet.apache.org)** — Storage format
- **[sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs)** — SQL parsing
