<div align="center">

# 🦎 HarnessDB

### 数据库界的 LocalStack — 14 种协议，1 个二进制文件

**一个二进制文件。十四种协议。零基础设施。**

[![License](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-2024--edition-orange.svg)](https://www.rust-lang.org)
[![Protocols](https://img.shields.io/badge/Protocols-14-blue.svg)](#-兼容性矩阵)

[English](../../README.md) · [中文文档](README.md) · [快速开始](#-快速开始) · [兼容性矩阵](#-兼容性矩阵) · [架构](#-架构)

---

**别再为了跑集成测试就启动 14 个 Docker 容器了。**

</div>

---

## HarnessDB 是什么？

HarnessDB 是一个**本地开发与 CI 测试平台**，用一个 Rust 二进制文件同时模拟 **14 种数据库协议**。可以把它理解为数据库界的 [LocalStack](https://localstack.cloud/) — 不是模拟 AWS 服务，而是模拟 MySQL、Redis、MongoDB、PostgreSQL、ClickHouse、Elasticsearch 等等。

**适合用来做：**
- 本地开发，不用装 14 个数据库服务器
- CI/CD 流水线，几秒钟启动完整数据库栈
- 跨数据库协议的集成测试
- 阿里云本地开发（MaxCompute、Hologres、TableStore）

**不适合用来做：**
- 生产环境 — 它是仿真层，不是真实数据库的替代品
- 高并发低延迟的 Redis 场景
- 生产级 MongoDB 聚合管道
- ACID 事务保证

> HarnessDB 通过 Apache DataFusion 把所有数据存储在 Parquet 文件中。对开发/测试来说速度足够快，但它不是专业数据库的生产替代品。

## 快速演示

```bash
# 启动 HarnessDB — 所有 14 种协议在默认端口监听
./target/release/harness-db

# 终端 1: MySQL
mysql -h 127.0.0.1 -P 9030 -uroot -e "CREATE TABLE users (id INT, name VARCHAR(50)); INSERT INTO users VALUES (1, 'Alice'); SELECT * FROM users;"

# 终端 2: Redis
redis-cli -h 127.0.0.1 -p 6379 SET mykey "hello" && redis-cli -h 127.0.0.1 -p 6379 GET mykey

# 终端 3: MongoDB
mongosh --host 127.0.0.1 --port 27017 --eval "db.users.insertOne({name: 'Bob', age: 30}); db.users.find()"

# 终端 4: Elasticsearch
curl -s -X PUT "http://127.0.0.1:9200/my-index/_doc/1" -H 'Content-Type: application/json' -d '{"title": "Hello"}'
curl -s "http://127.0.0.1:9200/my-index/_search" -H 'Content-Type: application/json' -d '{"query": {"match_all": {}}}'

# 终端 5: ClickHouse
curl -s "http://127.0.0.1:8123/" -d "CREATE TABLE test (id Int32, name String) ENGINE=Memory"
curl -s "http://127.0.0.1:8123/" -d "INSERT INTO test VALUES (1, 'Charlie')"
curl -s "http://127.0.0.1:8123/" -d "SELECT * FROM test"
```

> 📹 **TODO**: 用 asciinema 录制或 GIF 替换此段，展示所有协议的实际运行效果。

## 🚀 快速开始

### 构建

```bash
git clone https://github.com/walker83/HarnessDB.git
cd HarnessDB
cargo build --release
```

### 运行

```bash
./target/release/harness-db
```

所有 14 种协议立即开始监听，无需配置文件。

### 在 CI/CD 中使用

```yaml
# .github/workflows/test.yml
name: 集成测试
on: [push]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: 构建 HarnessDB
        run: cargo build --release

      - name: 启动数据库栈
        run: ./target/release/harness-db &
        # MySQL :9030, Redis :6379, MongoDB :27017 等

      - name: 等待 HarnessDB 就绪
        run: |
          for i in $(seq 1 30); do
            mysql -h 127.0.0.1 -P 9030 -uroot -e "SELECT 1" 2>/dev/null && break
            sleep 1
          done

      - name: 运行测试
        run: |
          # 你的应用现在可以连接 MySQL、Redis、MongoDB 等
          # 不需要在 workflow 里配置 services: 容器！
          npm test
```

这替代了典型的需要多个 `services:` 容器的 CI 配置：

```yaml
# ❌ 之前：重、慢、复杂
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

# ✅ 之后：一个二进制文件，<1 秒启动
steps:
  - run: ./harness-db &
```

## 📊 兼容性矩阵

以下是每个协议实际支持情况的诚实说明。我们相信透明比夸大其词更能赢得信任。

### ✅ 完整实现 — 可用于本地开发与测试

| 协议 | 端口 | 支持的命令 | 存储 | 备注 |
|------|------|-----------|------|------|
| **MySQL** | 9030 | CREATE/DROP TABLE/DB, INSERT, UPDATE, DELETE, SELECT (JOIN, WHERE, GROUP BY, ORDER BY, LIMIT, 聚合, 窗口函数) | Parquet + DataFusion | MySQL 5.7/8.0 兼容线协议 |
| **PostgreSQL** | 15432 | 完整 wire protocol v3, 认证 (md5/scram-sha-256), 扩展查询, 20+ pg_catalog 表, information_schema | Parquet + DataFusion | Hologres 兼容, 支持 psql/JDBC/psycopg2 |
| **Redis** | 6379 | 50+ 命令: String (GET/SET/MGET/INCR...), Hash (HGET/HSET/HGETALL...), List (LPUSH/RPOP/LRANGE...), Set (SADD/SMEMBERS...), Sorted Set (ZADD/ZRANGE...) | 内存 (DashMap) | RESP2/RESP3, 16 个数据库, TTL 支持 |
| **MongoDB** | 27017 | insert, find, update, delete, count, aggregate ($match/$group/$sum/$count/$skip/$limit), ismaster/hello | 内存 (DashMap) | OP_MSG + 旧版 OP_QUERY 线协议 |
| **ClickHouse** | 8123 | SELECT (WHERE, GROUP BY, ORDER BY, LIMIT, LIKE), INSERT, CREATE/DROP TABLE/DB, ALTER TABLE UPDATE/DELETE, SHOW/DESCRIBE | Parquet + DataFusion | HTTP 接口, TSV 输出 |
| **Elasticsearch** | 9200 | 文档 CRUD, bulk API, search (match_all), 索引创建/删除/信息, _cat API, _cluster/health | Parquet + DataFusion | REST API, 标准 ES 风格 JSON 响应 |
| **MaxCompute** | 9031 | 完整 REST API, SQL 实例 (提交/状态/结果), tunnel 上传/下载, 阿里云签名认证 (v1/v3/STS) | Parquet + DataFusion | pyodps SDK 兼容, 内置 SQL 转换器 |
| **AnalyticDB MySQL** | 3307 | 完整 SQL (同 MySQL 协议) | Parquet + DataFusion | 底层使用 mysql-protocol |

### ⚠️ 部分实现 — 基本操作可用，高级功能缺失

| 协议 | 端口 | 可用 | 缺失 | 备注 |
|------|------|------|------|------|
| **Cassandra** | 9042 | Frame codec v4, 握手, SELECT 系统表, CREATE/DROP keyspace/table | INSERT/UPDATE/DELETE 是空操作（返回成功但不持久化） | 适合测试 CQL 连接逻辑 |
| **InfluxDB** | 8086 | Line protocol 写入, SHOW DATABASES/MEASUREMENTS, CREATE/DROP DATABASE, 基本 SELECT | WHERE 时间过滤, InfluxQL, Flux | 写入路径可用，查询路径基础 |
| **TableStore** | 8087 | 表 CRUD, 行 put/get/update/delete, 范围查询 | 批量操作, 条件更新, 原子计数器, TTL | REST API + JSON |
| **Oracle** | 1521 | TNS 连接/握手, SELECT USER/SYSDATE/v$version, 基本标量表达式 | DML, 表访问, PL/SQL, 存储 | 适合测试 Oracle 驱动连通性 |
| **TDS (SAP ASE)** | 5000 | TDS 5.0 登录, SQL 文本查询, 基本结果编码 | 预处理语句, 游标, 事务控制 | 线协议帧可用 |

### 🔧 最小实现 — 概念验证

| 协议 | 端口 | 已实现 | 状态 |
|------|------|--------|------|
| **Lindorm** | 30030 | 7 个文本命令 (CREATE TABLE, PUT, GET, DELETE, SCAN, LIST, COUNT) | 文本式 HBase 接口，无线协议 |
| **Vector DB** | 19530 | 5 个 HTTP 端点 (创建集合, 插入, 搜索, 列表, 计数) | 最小 REST API，无删除/更新/过滤 |
| **Sybase** | 5000 | 委托给 TDS 协议 | 薄封装，无 Sybase 特有逻辑 |

## 🔧 配置

所有协议都可以通过 `config/server.toml` 独立启用/禁用：

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

# 禁用不需要的协议
[servers.cassandra]
enabled = false
port = 9042
```

或通过命令行参数：

```bash
./harness-db --mysql-port 9030 --redis-port 6379 --mongodb-port 27017
```

## 🏗️ 架构

```
┌─────────────────────────────────────────────────────────┐
│                      客户端应用                          │
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
        │   协议层       │
        │ (14种协议)     │
        └────────┬───────┘
                 │
                 ▼
        ┌────────────────┐
        │   查询引擎     │
        │  (DataFusion)  │
        └────────┬───────┘
                 │
                 ▼
        ┌────────────────┐
        │   存储引擎     │
        │   (Parquet)    │
        └────────────────┘
```

所有 SQL 类协议（MySQL、PostgreSQL、ClickHouse、MaxCompute 等）共享同一个 DataFusion 查询引擎和 Parquet 存储。NoSQL 协议（Redis、MongoDB）使用针对其访问模式优化的内存存储。

## 📈 性能

| 指标 | 数值 |
|------|------|
| 二进制大小 | ~50MB |
| 内存（空闲） | ~100MB |
| 启动时间 | <1 秒 |
| MySQL 查询延迟 | 10-50ms |
| Redis 操作 | 内存级，亚毫秒 |

## 🧪 测试

```bash
# 运行所有测试
cargo test --workspace

# 运行集成测试
cargo test -p integration-tests
```

## 🎓 使用场景

### 1. 本地开发

用一个二进制文件替代 `docker-compose.yml` 里的 14 个数据库容器：

```bash
# 不再需要：
# docker-compose up mysql redis mongo elasticsearch clickhouse

# 只需运行：
./harness-db
```

你的应用连接相同的端口、相同的协议。不需要 Docker Desktop 吃 8GB 内存。

### 2. CI/CD 流水线

GitHub Actions 里一步搞定，零容器配置：

```yaml
- name: 启动 HarnessDB
  run: ./target/release/harness-db &

- name: 运行测试
  run: cargo test
  # 测试可以使用 MySQL、Redis、MongoDB、ES、ClickHouse...
```

### 3. 阿里云本地开发

本地测试 MaxCompute (ODPS)、Hologres、TableStore 查询，无需云成本：

```python
from odps import ODPS
o = ODPS('harness', 'harness-secret', 'default',
         endpoint='http://localhost:9031/api')
o.execute_sql('SELECT * FROM my_table').wait_for_success()
```

### 4. 数据库协议测试

验证你的应用的数据库驱动兼容性：

```bash
# 测试 MySQL 协议
./harness-db --only-mysql &
cargo test --features mysql-tests

# 测试 PostgreSQL 协议
./harness-db --only-postgres &
cargo test --features pg-tests
```

## 📖 文档

- [SQL 参考](../en/sql-reference.md)
- [配置指南](../en/configuration.md)
- [架构](../en/architecture.md)
- [阿里云兼容性](../alibaba-cloud-compatibility.md)
- [路线图](../roadmap/README.md)

## 🤝 贡献

欢迎贡献！请参阅 [CONTRIBUTING.md](../../CONTRIBUTING.md)。

适合新手的 Issue：
- 实现 Cassandra INSERT/UPDATE/DELETE（目前是空操作）
- 添加 InfluxDB 时间范围过滤
- 改进 Oracle DML 支持
- 为任何协议添加更多集成测试

## 📜 许可证

Apache License 2.0。详见 [LICENSE](../../LICENSE)。

## 🙏 致谢

- **[Apache DataFusion](https://github.com/apache/arrow-datafusion)** — 查询引擎
- **[Apache Arrow](https://arrow.apache.org)** — 列式格式
- **[Apache Parquet](https://parquet.apache.org)** — 存储格式
- **[sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs)** — SQL 解析
