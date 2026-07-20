# HarnessDB 全面代码审查报告

**审查日期**: 2026-07-20
**审查范围**: 全部30个crate, 215个Rust源文件
**审查方法**: 22个并行agent + 手动分析
**测试执行**: `cargo test --workspace` (298+ 测试用例)

---

## 修复状态 (2026-07-20)

**已修复: ~55个bug** (28个提交, 12个crate)
**剩余: ~315个bug** (主要是需要大规模架构改动的骨架实现)

### 已修复的关键Bug

| # | Crate | Bug | 修复 |
|---|-------|-----|------|
| 1 | MySQL | 二进制协议整数类型大小错误 | ✅ |
| 2 | MySQL | USE命令大小写敏感 | ✅ |
| 3 | MySQL | JWT回退密钥硬编码 | ✅ |
| 4 | MySQL | COM_INIT_DB返回结果集而非OK包 | ✅ |
| 5 | MySQL | DML affected_rows始终返回0 | ✅ |
| 6 | PG | Parse发送ParameterDescription | ✅ |
| 7 | PG | Describe执行DML副作用 | ✅ |
| 8 | PG | Execute双重ReadyForQuery | ✅ |
| 9 | PG | ALTER/EXPLAIN命令标签错误 | ✅ |
| 10 | PG | information_schema路由contains | ✅ |
| 11 | PG | 版本字符串过时 | ✅ |
| 12 | PG | startup消息负数长度DoS | ✅ |
| 13 | Redis | inline命令首字符丢失 | ✅ |
| 14 | Redis | 负TTL wrapping | ✅ |
| 15 | Redis | ZADD返回值错误 | ✅ |
| 16 | Redis | bulk string/array无大小限制 | ✅ |
| 17 | Redis | invalid integer静默挂起 | ✅ |
| 18 | Redis | dead code write_buf | ✅ |
| 19 | ClickHouse | delete_where列长度panic | ✅ |
| 20 | ClickHouse | HTTP状态码始终200 | ✅ |
| 21 | ClickHouse | SELECT无列头 | ✅ |
| 22 | ClickHouse | get_database自动创建 | ✅ |
| 23 | MongoDB | $group字段引用不可达 | ✅ |
| 24 | MongoDB | $match管道重新查询 | ✅ |
| 25 | MongoDB | $exists null混淆 | ✅ |
| 26 | MongoDB | 未知操作符静默通过 | ✅ |
| 27 | ES | HTTP状态码始终200 | ✅ |
| 28 | Cassandra | 负数body length panic | ✅ |
| 29 | Cassandra | get_keyspace自动创建 | ✅ |
| 30 | Lindorm | create_table覆盖已存在表 | ✅ |
| 31 | fe-storage | drop_table/create_table缺写锁 | ✅ |
| 32 | fe-storage | filter pushdown静默丢弃 | ✅ |
| 33 | fe-storage | 目录fsync缺失 | ✅ |
| 34 | fe-storage | ALTER TABLE空表不生效 | ✅ |
| 35 | DataFusion | UInt64→Int64截断 | ✅ |
| 36 | DataFusion | date_add i64→i32截断 | ✅ |
| 37 | DataFusion | unix_timestamp volatility | ✅ |
| 38 | DataFusion | substring_index count=0 | ✅ |
| 39 | SQL Parser | COMMIT;解析失败 | ✅ |
| 40 | SQL Parser | CATALOG大小写敏感 | ✅ |
| 41 | SQL Parser | INSERT SET大小写敏感 | ✅ |
| 42 | SQL Parser | materialized view AS替换 | ✅ |
| 43 | SQL Parser | isolation level默认值 | ✅ |
| 44 | SQL Parser | 三段式名称 | ✅ |
| 45 | fe-catalog | test_rocks_backend测试 | ✅ |
| 46 | Security | JWT回退密钥 | ✅ |

### 剩余需要大规模重构的Bug

| Crate | Bug数 | 说明 |
|-------|-------|------|
| Oracle/TDS/Sybase/TSQL | 95 | 骨架实现，需要完整协议重写 |
| Cassandra | ~20 | INSERT/UPDATE/DELETE是空操作 |
| ADB MySQL | ~15 | 聚合函数、NaN处理 |
| 其余 | ~190 | 中低优先级协议合规性、代码质量 |

---

## 执行摘要

| 指标 | 数值 |
|------|------|
| **总Bug数** | **370+** |
| CRITICAL | 40+ |
| HIGH | 75+ |
| MEDIUM | 80+ |
| LOW | 35+ |
| 零测试覆盖的crate | 15/30 (50%) |
| 生产代码unwrap()调用 | 586 |
| unsafe代码块 | 1 |
| 测试失败 | 1 (test_rocks_backend) |

---

## 一、协议层Bug汇总

### 1.1 MySQL协议 (12 bugs)

| # | 严重程度 | 位置 | 描述 |
|---|---------|------|------|
| 1 | CRITICAL | packet.rs:1167 | 二进制协议整数类型大小错误 - TINY/SHORT/LONG全部发送Int64(8字节) |
| 2 | CRITICAL | connection.rs:1100 | 2字节长度编码字符串越界检查off-by-one |
| 3 | CRITICAL | connection.rs:896 | 预处理语句参数绑定使用字符串插值(注入风险) |
| 4 | HIGH | connection.rs:618 | USE命令大小写敏感bug - 小写SQL无法切换数据库 |
| 5 | HIGH | connection.rs:374 | COM_INIT_DB返回结果集而非OK包(协议违规) |
| 6 | HIGH | connection.rs:1299 | 关闭后执行预处理语句返回空SQL |
| 7 | HIGH | packet.rs:1189 | 带微秒的日期时间解析失败 |
| 8 | MEDIUM | token.rs:150 | JWT使用非标准base64编码 |
| 9 | MEDIUM | connection.rs:498 | @@variable查询解析脆弱 |
| 10 | MEDIUM | connection.rs:640 | DML affected_rows始终返回0 |
| 11 | MEDIUM | connection.rs:389 | COM_FIELD_LIST不返回列定义 |
| 12 | MEDIUM | connection.rs:281 | 认证后无空闲超时 |

### 1.2 PostgreSQL协议 (17 bugs)

| # | 严重程度 | 位置 | 描述 |
|---|---------|------|------|
| 1 | HIGH | connection.rs:521 | Describe执行DML作为副作用(INSERT执行两次) |
| 2 | HIGH | connection.rs:478 | 参数化查询($1,$2)完全不工作 |
| 3 | HIGH | connection.rs:648 | Execute错误路径发送双重ReadyForQuery |
| 4 | MEDIUM | connection.rs:430 | Parse错误发送ParameterDescription(协议违规) |
| 5 | MEDIUM | message.rs:313 | 二进制格式参数被静默忽略 |
| 6 | MEDIUM | message.rs:238 | 负数启动消息长度导致5分钟DoS |
| 7 | MEDIUM | connection.rs:343 | 多语句查询未处理 |
| 8 | MEDIUM | connection.rs | 预处理语句缓存内存泄漏 |
| 9 | MEDIUM | connection.rs:162 | 取消请求不取消正在运行的查询 |
| 10 | MEDIUM | message.rs:733 | read_cstring静默接受无终止符字符串 |
| 11 | LOW | catalog.rs:386 | 版本字符串过时(0.3.0 vs 1.2.0) |
| 12 | LOW | catalog.rs:253 | information_schema路由使用contains而非starts_with |
| 13 | LOW | connection.rs:822 | SELECT INTO被错误分类为返回行 |
| 14 | LOW | connection.rs:869 | ALTER始终返回"ALTER TABLE" |
| 15 | LOW | connection.rs:885 | EXPLAIN命令标签包含行数 |
| 16 | LOW | message.rs:493 | 字段数无验证 |
| 17 | LOW | connection.rs:545 | Describe不存在的语句返回NoData而非Error |

### 1.3 MongoDB协议 (13 bugs)

| # | 严重程度 | 位置 | 描述 |
|---|---------|------|------|
| 1 | CRITICAL | handler.rs:157 | 插入时_id未注入文档 |
| 2 | CRITICAL | handler.rs:370 | $group字段引用分支不可达(match arm顺序错误) |
| 3 | HIGH | handler.rs:325 | $match管道阶段重新查询存储(丢弃前序结果) |
| 4 | HIGH | handler.rs:210 | update/delete仅支持ObjectId类型的_id |
| 5 | HIGH | storage.rs:151 | $exists将null值与缺失字段混为一谈 |
| 6 | MEDIUM | wire.rs:156 | 校验和从未解析或验证 |
| 7 | MEDIUM | wire.rs:133 | Section kind-1大小可能下溢 |
| 8 | MEDIUM | wire.rs:350 | Unknown负载编码产生空body |
| 9 | MEDIUM | wire.rs:20 | 无message_length验证 |
| 10 | MEDIUM | handler.rs:449 | cmd_get_log对特定日志名返回空 |
| 11 | MEDIUM | handler.rs:304 | $aggregate忽略sections参数 |
| 12 | MEDIUM | storage.rs:158 | 未知查询操作符静默通过所有文档 |
| 13 | MEDIUM | storage.rs:189 | $inc不处理混合数值类型 |

### 1.4 Redis协议 (17 bugs)

| # | 严重程度 | 位置 | 描述 |
|---|---------|------|------|
| 1 | CRITICAL | resp.rs:262 | parse_inline丢失inline命令首字符(PING→ING) |
| 2 | HIGH | resp.rs:129 | Bulk string无大小限制(OOM风险) |
| 3 | HIGH | resp.rs:168 | Array无元素数量限制(OOM风险) |
| 4 | HIGH | resp.rs:144 | 递归解析无深度限制(栈溢出) |
| 5 | HIGH | connection.rs:43 | 读缓冲区无大小限制(OOM) |
| 6 | HIGH | connection.rs:38 | 无认证强制执行 |
| 7 | MEDIUM | resp.rs:102 | 无效整数静默视为"数据不完整"(连接挂起) |
| 8 | MEDIUM | resp.rs:124 | 负数bulk string长度被当作null处理 |
| 9 | MEDIUM | handler.rs:1312 | ZADD对已存在成员更新也计入返回值 |
| 10 | MEDIUM | handler.rs:316 | EXPIRE负数秒数转为超大正数 |
| 11 | MEDIUM | handler.rs:463 | SET EX/PX负数值wrapping问题 |
| 12 | MEDIUM | storage.rs:68 | 过期key清理TOCTOU竞态条件 |
| 13 | LOW | storage.rs:29 | OrderedFloat的Eq/Ord不一致 |
| 14 | LOW | handler.rs:392 | RENAME非原子且丢失TTL |
| 15 | LOW | storage.rs:135 | KEYS命令不清理过期key |
| 16 | LOW | storage.rs:155 | glob_to_regex不支持转义序列 |
| 17 | LOW | connection.rs:15 | write_buf字段从未使用(dead code) |

### 1.5 ClickHouse协议 (12 bugs)

| # | 严重程度 | 位置 | 描述 |
|---|---------|------|------|
| 1 | CRITICAL | storage.rs:83 | delete_where列长度不等时panic |
| 2 | CRITICAL | handler.rs:256 | evaluate_single_condition匹配错误操作符(字符串中的=) |
| 3 | CRITICAL | handler.rs:696 | INSERT静默丢弃多余值/不填充缺失列 |
| 4 | HIGH | handler.rs:683 | SELECT从不输出列头 |
| 5 | HIGH | handler.rs:1092 | format_tsv忽略headers参数 |
| 6 | HIGH | server.rs:135 | HTTP错误始终返回200 OK |
| 7 | MEDIUM | handler.rs:884 | DROP TABLE IF EXISTS跳过错误数量的token |
| 8 | MEDIUM | handler.rs:777 | CREATE TABLE覆盖已存在的表数据 |
| 9 | MEDIUM | storage.rs:222 | get_database自动创建不存在的数据库 |
| 10 | MEDIUM | handler.rs:617 | COUNT检测匹配名为"count"的列 |
| 11 | LOW | storage.rs:29 | INSERT不填充缺失列 |
| 12 | LOW | - | 无Native协议支持(仅HTTP) |

### 1.6 Cassandra协议 (22 bugs)

| # | 严重程度 | 位置 | 描述 |
|---|---------|------|------|
| 1 | CRITICAL | handler.rs:27 | ROWS结果缺少CQL协议必需的Metadata段 |
| 2 | HIGH | frame.rs:104 | 负数body length导致panic |
| 3 | HIGH | handler.rs:156 | INSERT/UPDATE/DELETE全部是空操作 |
| 4 | MEDIUM | handler.rs:17 | 响应版本硬编码为0x84(v4) |
| 5 | MEDIUM | server.rs:104 | Stream ID硬编码为0 |
| 6 | MEDIUM | server.rs:105 | QUERY body解析不完整,忽略Consistency Level |
| 7 | MEDIUM | server.rs:96 | Batch操作码未处理 |
| 8 | MEDIUM | server.rs:96 | Prepare/Execute操作码未处理 |
| 9 | MEDIUM | storage.rs:95 | get_keyspace自动创建不存在的keyspace |
| 10 | MEDIUM | handler.rs:86 | OPTIONS返回READY而非SUPPORTED |
| 11 | MEDIUM | storage.rs:9 | 所有数据存储为String(无类型系统) |
| 12-22 | LOW | 各处 | 无连接超时、大小写混用、未知opcode静默忽略等 |

### 1.7 Elasticsearch协议 (8 bugs)

| # | 严重程度 | 位置 | 描述 |
|---|---------|------|------|
| 1 | CRITICAL | server.rs:118 | HTTP状态码始终返回200 |
| 2 | HIGH | handler.rs:369 | Bulk响应始终报告errors=false |
| 3 | HIGH | handler.rs:331 | Bulk Update对不存在的文档返回200 |
| 4 | HIGH | storage.rs:45 | Search完全忽略查询DSL |
| 5 | MEDIUM | handler.rs:328 | Bulk Delete索引推进错误 |
| 6 | MEDIUM | handler.rs:385 | PUT/POST文档始终返回"created" |
| 7 | MEDIUM | handler.rs:145 | 系统索引名未验证 |
| 8 | LOW | handler.rs:268 | _bulk不验证Content-Type |

### 1.8 InfluxDB协议 (8 bugs)

| # | 严重程度 | 位置 | 描述 |
|---|---------|------|------|
| 1 | CRITICAL | storage.rs:18 | 同时间戳数据点被静默覆盖(数据丢失) |
| 2 | HIGH | line_protocol.rs:58 | 行协议解析器不处理转义字符 |
| 3 | MEDIUM | line_protocol.rs:102 | 布尔解析缺少TRUE/FALSE |
| 4 | MEDIUM | line_protocol.rs:96 | 非数字字段值静默丢弃 |
| 5 | MEDIUM | handler.rs:123 | SELECT输出使用错误的measurement名 |
| 6 | MEDIUM | server.rs:147 | POST /query不解析表单参数 |
| 7 | MEDIUM | handler.rs:43 | 查询结果格式不符合JSON或行协议 |
| 8 | LOW | handler.rs:48 | SHOW DATABASES格式错误 |

### 1.9 Lindorm协议 (7 bugs)

| # | 严重程度 | 位置 | 描述 |
|---|---------|------|------|
| 1 | HIGH | storage.rs:57 | Scan返回无序结果(DashMap迭代顺序) |
| 2 | MEDIUM | handler.rs:43 | PUT命令不验证表存在性 |
| 3 | MEDIUM | storage.rs:23 | CREATE TABLE覆盖已存在的表 |
| 4 | LOW | handler.rs:4 | 未使用的导入 |
| 5 | LOW | handler.rs:132 | 未使用的参数 |
| 6 | LOW | handler.rs:43 | 无列族验证 |
| 7 | LOW | handler.rs:103 | Scan无范围key验证 |

### 1.10 Oracle/TDS/Sybase协议 (95 bugs)

**CRITICAL (5)**:
- Oracle TNS整数下溢(tns.rs:87)
- Oracle header字节丢失(tns.rs:47)
- TDS u16溢出(packet.rs:49)
- TDS LOGINACK长度off by 8(token.rs:71)
- 存储过程body永不执行(interpreter.rs:159)

**HIGH (22)**: 无认证、多包消息未重组、RPC包解析为SQL、结果集>64KB崩溃、错误静默吞掉等

**MEDIUM (38)**: 无能力交换、无RPC参数处理、UTF-16/UTF-8启发式脆弱、硬编码"master"数据库等

**LOW (30)**: 18个TDS类型定义仅使用VARCHAR、无TCP超时、行数截断等

### 1.11 ADB MySQL协议 (20 bugs)

| # | 严重程度 | 位置 | 描述 |
|---|---------|------|------|
| 1 | CRITICAL | handler.rs:833 | format_f64 as i64溢出 |
| 2 | CRITICAL | handler.rs:824 | NaN比较返回None导致错误WHERE过滤 |
| 3 | CRITICAL | handler.rs:776 | 解析错误返回OK |
| 4 | HIGH | handler.rs:782 | 多语句USE+DML使用错误数据库 |
| 5 | HIGH | handler.rs:396 | COUNT(col)被当作COUNT(*) |
| 6 | HIGH | handler.rs:398 | SUM/AVG/MIN/MAX静默返回"0" |
| 7 | HIGH | handler.rs:550 | GtEq/LtEq对NaN/不可解析字符串错误 |
| 8 | HIGH | handler.rs:141 | USE不更新current_databases |
| 9 | HIGH | handler.rs:565 | 未知WHERE操作符(LIKE/IN/BETWEEN)通过所有行 |
| 10 | HIGH | handler.rs:659 | INSERT表达式存储debug格式字符串 |

---

## 二、存储层Bug (19 bugs)

| # | 严重程度 | 位置 | 描述 |
|---|---------|------|------|
| 1 | CRITICAL | lib.rs:179 | drop_table未获取写锁(与并发INSERT竞争) |
| 2 | CRITICAL | lib.rs:165 | create_table未获取写锁 |
| 3 | CRITICAL | block_convert.rs:429 | UInt64→Int64静默截断(数据损坏) |
| 4 | CRITICAL | date_udf.rs:1823 | date_add/date_sub i64→i32截断 |
| 5 | HIGH | table_provider.rs:96 | Filter pushdown静默丢弃过滤条件 |
| 6 | HIGH | date_udf.rs:1305 | unix_timestamp声明Immutable但返回当前时间 |
| 7 | HIGH | misc_udf.rs:348 | group_concat DISTINCT多分区合并时重复 |
| 8 | HIGH | misc_udf.rs:134 | unhex返回类型错误且丢失非UTF-8数据 |
| 9 | HIGH | doris_udf.rs:549 | substring_index count=0返回全文而非空串 |
| 10 | MEDIUM | lib.rs:427 | 原子写入缺少目录fsync |
| 11 | MEDIUM | lib.rs:415 | 写入失败时临时文件泄漏 |
| 12 | MEDIUM | lib.rs:201 | 每次DML全表加载到内存 |
| 13 | MEDIUM | lib.rs:301 | ALTER TABLE DROP COLUMN在空表上不生效 |
| 14 | MEDIUM | block_convert.rs:149 | Timestamp带时区类型round-trip丢失 |
| 15-19 | LOW | 各处 | UUID不符合RFC4122、Decimal精度丢失等 |

---

## 三、SQL Parser Bug (38 bugs)

### SQL Parser深度分析 (20 bugs)

| # | 严重程度 | 位置 | 描述 |
|---|---------|------|------|
| 19 | CRITICAL | parser.rs:906 | try_parse_set_variable使用大写rest的偏移量切原始sql(多字节UTF-8错误) |
| 20 | CRITICAL | parser.rs:3177 | parse_create_catalog大小写敏感strip_prefix |
| 21 | CRITICAL | parser.rs:3230 | parse_drop_catalog大小写敏感strip_prefix |
| 22 | CRITICAL | parser.rs:3254 | parse_refresh_catalog大小写敏感strip_prefix |
| 23 | CRITICAL | parser.rs:325 | DROP ANALYZE JOB大小写敏感strip_prefix |
| 24 | HIGH | parser.rs:944 | parse_set_value转义替换顺序错误 |
| 25 | HIGH | parser.rs:2332 | 多个CTE静默丢弃，仅使用第一个 |
| 26 | HIGH | parser.rs:2499 | 多表FROM静默丢弃，仅使用第一个 |
| 27 | HIGH | parser.rs:2527 | LIMIT/OFFSET非数字表达式静默丢弃 |
| 28 | HIGH | parser.rs:2705 | \x十六进制转义可产生无效UTF-8 |
| 29 | MEDIUM | parser.rs:1836 | extract_identifier不支持反引号标识符 |
| 30 | MEDIUM | parser.rs:1845 | extract_identifier不处理转义引号 |
| 31 | MEDIUM | parser.rs:2739 | 未知转义序列静默丢弃反斜杠 |
| 32 | MEDIUM | parser.rs:1416 | parse_create_materialized_view.replace("AS ")破坏视图定义 |
| 33 | MEDIUM | parser.rs:465 | SET TRANSACTION ISOLATION LEVEL未知级别默认REPEATABLE READ |
| 34 | MEDIUM | parser.rs:1044 | SHOW STATUS默认global=true(MySQL应为session) |
| 35 | MEDIUM | parser.rs:671 | split_qualified_name三段式名称丢失database |
| 36 | LOW | parser.rs:547 | split_on_comma不支持反引号标识符 |
| 37 | LOW | parser.rs:3061 | parse_create_user解析IF NOT EXISTS但丢弃 |
| 38 | LOW | parser.rs:4334 | parse_drop_job等解析if_exists但丢弃 |

### SQL Parser初始分析 (18 bugs)

| # | 严重程度 | 位置 | 描述 |
|---|---------|------|------|
| 1 | HIGH | parser.rs:428 | COMMIT;等带分号事务语句解析失败 |
| 2 | HIGH | parser.rs:602 | INSERT SET大小写敏感 |
| 3 | HIGH | parser.rs:3180 | CREATE/DROP/REFRESH CATALOG大小写敏感 |
| 4 | MEDIUM | parser.rs | 错误位置始终为0 |
| 5 | MEDIUM | parser.rs:2345 | 子查询解析失败静默降级 |
| 6 | MEDIUM | parser.rs:1842 | 不支持反引号标识符 |
| 7 | MEDIUM | parser.rs:553 | SQL标准转义引号''处理不正确 |
| 8 | MEDIUM | parser.rs:2332 | 只支持第一个CTE |
| 9 | MEDIUM | parser.rs:2500 | 多表FROM只取第一个表 |
| 10 | MEDIUM | parser.rs | 空SQL返回Ok([])而非错误 |
| 11 | MEDIUM | parser.rs:2761 | 数字溢出静默降为0 |
| 12 | LOW | lib.rs:22 | test_alter_table_parsing吞掉错误 |
| 13 | LOW | parser.rs:2709 | 字节转字符处理不当 |
| 14 | LOW | parser.rs:671 | 三段式名称处理不正确 |
| 15-18 | LOW | 各处 | 标识符消毒、引号处理等 |

---

## 四、安全漏洞 (15 issues)

| # | 严重程度 | 位置 | 描述 |
|---|---------|------|------|
| 1 | CRITICAL | fe_main.rs:484 | 硬编码凭证"harness"/"harness-secret" |
| 2 | CRITICAL | connection.rs:238 | JWT回退密钥"harnessdb_dev_fallback_key" |
| 3 | CRITICAL | auth.rs:128 | accept_any_password标志无防护 |
| 4 | CRITICAL | native_password.rs:18 | root用户空密码 |
| 5 | HIGH | lib.rs:124 | unsafe transmute扩展MutexGuard生命周期 |
| 6 | HIGH | lib.rs:89 | 存储层无路径遍历防护 |
| 7 | HIGH | fe_main.rs:549 | Redis服务器无认证 |
| 8 | HIGH | packet.rs:12 | 无查询复杂度/大小限制 |
| 9 | MEDIUM | dml_handler.rs:617 | 动态SQL构造(DML处理器) |
| 10 | MEDIUM | native_password.rs:77 | 错误消息泄露用户名(用户枚举) |
| 11 | MEDIUM | connection.rs:345 | SQL明文日志记录 |
| 12 | MEDIUM | parser.rs:3013 | 生产代码panic!() |
| 13 | LOW | 各处 | 835+ unwrap()调用 |
| 14 | LOW | 各处 | 39+ from_utf8_lossy静默丢弃无效字节 |
| 15 | LOW | 各处 | 过度clone/to_string |

---

## 五、并发安全问题 (5 issues)

| # | 严重程度 | 位置 | 描述 |
|---|---------|------|------|
| 1 | HIGH | audit_log.rs | 审计日志锁顺序不一致(死锁风险) |
| 2 | HIGH | dml_handler.rs:156 | spawn_blocking内嵌套block_on(死锁风险) |
| 3 | MEDIUM | catalog.rs | 原子操作内存顺序不一致 |
| 4 | MEDIUM | lib.rs:124 | unsafe transmute脆弱安全不变量 |
| 5 | LOW | audit_log.rs | 异步I/O期间持锁(延迟尖峰) |

---

## 六、性能问题 (关键发现)

| 严重程度 | 问题 | 影响 |
|---------|------|------|
| P0 | INSERT全表读写 | 大表插入性能灾难 |
| P0 | UPDATE/DELETE全表读写 | 同上 |
| P1 | catalog深拷贝(30处clone) | 元数据操作慢,内存浪费 |
| P1 | 锁内I/O | 并发写入阻塞 |
| P2 | Statement clone(76处) | CPU浪费 |
| P2 | query_history.remove(0) O(n) | 累积性能退化 |
| P2 | 审计日志三锁合一 | 锁开销 |
| P3 | parser String分配(380+ to_string) | CPU+内存 |

---

## 七、测试失败

```
test catalog::tests::test_rocks_backend ... FAILED
assertion `left == right` failed
  left: "kvstore"
 right: "rocksdb"
```

**原因**: test_rocks_backend测试期望后端名为"rocksdb"但实际返回"kvstore"。可能是从RocksDB迁移到redb后测试未更新。

---

## 八、SQL Translators Bug (6 bugs)

| # | 严重程度 | 位置 | 描述 |
|---|---------|------|------|
| 1 | MEDIUM | lib.rs:103 | mask_string_literals不支持dollar-quoted字符串 |
| 2 | MEDIUM | maxcompute.rs:142 | strip_tblproperties不mask字符串字面量 |
| 3 | LOW | maxcompute.rs:61 | strip_stored_as扩展正则在未mask的SQL上操作 |
| 4 | LOW | maxcompute.rs:565 | is_noop_set_statement正则过于严格 |
| 5 | LOW | maxcompute.rs:1526 | 缺少#[test]注解(测试从未运行) |
| 6 | LOW | hologres.rs:487 | handle_ctas_with正则可被嵌套括号击败 |

---

## 九、错误处理问题

| 指标 | 数量 |
|------|------|
| unwrap()调用(生产代码) | 586 |
| expect()调用 | ~14 |
| panic!/unreachable! | ~25 |
| 无catch_unwind保护 | 所有协议连接 |

**关键风险**:
- Arrow downcast unwrap() (utils.rs 21处) - 类型不匹配时服务器崩溃
- RwLock poisoning (lib.rs:106,113) - 线程panic导致级联失败
- 无连接级panic恢复机制

---

## 十、文档覆盖

| Crate | 公开项 | 有文档 | 覆盖率 |
|-------|--------|--------|--------|
| fe-storage | 39 | 24 | 61.5% |
| fe-catalog | 164 | 25 | 15.2% |
| types | 166 | 9 | 5.4% |
| common | 25 | 0 | 0.0% |
| fe-sql-parser | 76 | 0 | 0.0% |

---

## 修复优先级建议

### P0 - 立即修复 (影响数据正确性/安全)
1. MySQL二进制协议整数类型大小错误
2. PG参数化查询完全不工作
3. MongoDB $group字段引用不可达
4. 存储层drop_table/create_table未获取写锁
5. 硬编码凭证和JWT回退密钥
6. ClickHouse delete_where列长度不等时panic
7. Redis inline命令首字符丢失
8. Oracle TNS整数下溢
9. TDS LOGINACK长度off by 8
10. 存储过程body永不执行

### P1 - 高优先级 (影响功能正确性)
1. ES HTTP始终返回200
2. InfluxDB同时间戳数据覆盖
3. Cassandra ROWS结果格式不符合协议
4. ADB MySQL SUM/AVG/MIN/MAX返回"0"
5. Filter pushdown静默丢弃条件
6. unix_timestamp volatility错误
7. UInt64→Int64静默截断

### P2 - 中优先级 (影响兼容性/可靠性)
1. 各协议缺失测试覆盖(15个crate零测试)
2. 586处unwrap()在生产代码中
3. 无连接级panic恢复
4. 全表内存模型不适合大表

### P3 - 低优先级 (代码质量)
1. 文档覆盖率不足(整体12.3%)
2. 718处clone()调用
3. 2636处to_string()调用
4. 死代码清理

---

## 测试覆盖状况

| 状态 | Crate数量 | 说明 |
|------|----------|------|
| 有测试 | 15 | fe-catalog(18), be-kv(15), pg-protocol(137), mysql-protocol(10), redis-protocol(11), sql-translators(275), tsql-parser(25), maxcompute-protocol(246)等 |
| 零测试 | 15 | cassandra, clickhouse, elasticsearch, influxdb, lindorm, oracle, tds, sybase, adb-mysql, vector, tablestore, fe-storage, fe-datafusion, tsql-executor等 |

---

*报告由18个并行分析agent生成,覆盖全部30个crate的源代码审查和测试执行。*
