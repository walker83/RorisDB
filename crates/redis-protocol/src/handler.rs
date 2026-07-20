//! Redis command handler
//! Implements all core Redis commands

use crate::resp::{RespValue, RespEncoder};
use crate::storage::{RedisStorage, RedisValue, OrderedFloat};
use bytes::BytesMut;
use std::sync::Arc;
use std::time::Duration;

/// Trait for handling Redis commands
pub trait RedisCommandHandler: Send + Sync {
    /// Handle a Redis command and return the response
    fn handle_command(&self, db_index: usize, command: &[RespValue]) -> BytesMut;
}

/// Default Redis command handler implementation
pub struct DefaultRedisHandler {
    storage: Arc<RedisStorage>,
    password: Option<String>,
}

impl DefaultRedisHandler {
    pub fn new(storage: Arc<RedisStorage>, password: Option<String>) -> Self {
        Self { storage, password }
    }
}

impl RedisCommandHandler for DefaultRedisHandler {
    fn handle_command(&self, db_index: usize, command: &[RespValue]) -> BytesMut {
        if command.is_empty() {
            return RespEncoder::error("ERR empty command");
        }

        let cmd = match &command[0] {
            RespValue::BulkString(s) => {
                String::from_utf8_lossy(s).to_uppercase()
            }
            RespValue::SimpleString(s) => s.to_uppercase(),
            _ => return RespEncoder::error("ERR invalid command format"),
        };

        let args = &command[1..];

        match cmd.as_str() {
            // Connection commands
            "PING" => self.cmd_ping(args),
            "ECHO" => self.cmd_echo(args),
            "QUIT" => RespEncoder::ok(),
            "AUTH" => self.cmd_auth(args),
            "SELECT" => self.cmd_select(args),

            // Server commands
            "INFO" => self.cmd_info(args),
            "DBSIZE" => self.cmd_dbsize(db_index),
            "FLUSHDB" => self.cmd_flushdb(db_index),
            "FLUSHALL" => self.cmd_flushall(),
            "COMMAND" => RespEncoder::ok(), // Simplified
            "CONFIG" => self.cmd_config(args),
            "CLIENT" => self.cmd_client(args),

            // Key commands
            "DEL" => self.cmd_del(db_index, args),
            "EXISTS" => self.cmd_exists(db_index, args),
            "EXPIRE" => self.cmd_expire(db_index, args),
            "TTL" => self.cmd_ttl(db_index, args),
            "TYPE" => self.cmd_type(db_index, args),
            "KEYS" => self.cmd_keys(db_index, args),
            "RENAME" => self.cmd_rename(db_index, args),

            // String commands
            "GET" => self.cmd_get(db_index, args),
            "SET" => self.cmd_set(db_index, args),
            "MGET" => self.cmd_mget(db_index, args),
            "MSET" => self.cmd_mset(db_index, args),
            "INCR" => self.cmd_incr(db_index, args),
            "DECR" => self.cmd_decr(db_index, args),
            "INCRBY" => self.cmd_incrby(db_index, args),
            "DECRBY" => self.cmd_decrby(db_index, args),
            "APPEND" => self.cmd_append(db_index, args),
            "STRLEN" => self.cmd_strlen(db_index, args),

            // Hash commands
            "HGET" => self.cmd_hget(db_index, args),
            "HSET" => self.cmd_hset(db_index, args),
            "HMGET" => self.cmd_hmget(db_index, args),
            "HMSET" => self.cmd_hmset(db_index, args),
            "HDEL" => self.cmd_hdel(db_index, args),
            "HGETALL" => self.cmd_hgetall(db_index, args),
            "HKEYS" => self.cmd_hkeys(db_index, args),
            "HVALS" => self.cmd_hvals(db_index, args),
            "HLEN" => self.cmd_hlen(db_index, args),
            "HEXISTS" => self.cmd_hexists(db_index, args),

            // List commands
            "LPUSH" => self.cmd_lpush(db_index, args),
            "RPUSH" => self.cmd_rpush(db_index, args),
            "LPOP" => self.cmd_lpop(db_index, args),
            "RPOP" => self.cmd_rpop(db_index, args),
            "LRANGE" => self.cmd_lrange(db_index, args),
            "LLEN" => self.cmd_llen(db_index, args),
            "LINDEX" => self.cmd_lindex(db_index, args),

            // Set commands
            "SADD" => self.cmd_sadd(db_index, args),
            "SREM" => self.cmd_srem(db_index, args),
            "SMEMBERS" => self.cmd_smembers(db_index, args),
            "SCARD" => self.cmd_scard(db_index, args),
            "SISMEMBER" => self.cmd_sismember(db_index, args),

            // Sorted set commands
            "ZADD" => self.cmd_zadd(db_index, args),
            "ZREM" => self.cmd_zrem(db_index, args),
            "ZRANGE" => self.cmd_zrange(db_index, args),
            "ZCARD" => self.cmd_zcard(db_index, args),
            "ZSCORE" => self.cmd_zscore(db_index, args),

            _ => RespEncoder::error(format!("ERR unknown command '{}'", cmd)),
        }
    }
}

impl DefaultRedisHandler {
    // Helper to get argument as string
    fn get_arg_str<'a>(&self, arg: &'a RespValue) -> Option<&'a str> {
        match arg {
            RespValue::BulkString(s) => std::str::from_utf8(s).ok(),
            RespValue::SimpleString(s) => Some(s),
            _ => None,
        }
    }

    // Helper to get argument as integer
    fn get_arg_int(&self, arg: &RespValue) -> Option<i64> {
        match arg {
            RespValue::Integer(n) => Some(*n),
            RespValue::BulkString(s) => {
                std::str::from_utf8(s).ok()?.parse().ok()
            }
            _ => None,
        }
    }

    // Connection commands
    fn cmd_ping(&self, args: &[RespValue]) -> BytesMut {
        if args.is_empty() {
            RespEncoder::pong()
        } else if let Some(msg) = self.get_arg_str(&args[0]) {
            RespEncoder::bulk_string(msg.as_bytes())
        } else {
            RespEncoder::pong()
        }
    }

    fn cmd_echo(&self, args: &[RespValue]) -> BytesMut {
        if args.is_empty() {
            return RespEncoder::error("ERR wrong number of arguments for 'echo' command");
        }
        if let Some(msg) = self.get_arg_str(&args[0]) {
            RespEncoder::bulk_string(msg.as_bytes())
        } else {
            RespEncoder::error("ERR invalid argument")
        }
    }

    fn cmd_auth(&self, args: &[RespValue]) -> BytesMut {
        if let Some(ref expected) = self.password {
            if args.is_empty() {
                return RespEncoder::error("ERR wrong number of arguments for 'auth' command");
            }
            if let Some(provided) = self.get_arg_str(&args[0]) {
                if provided == expected {
                    RespEncoder::ok()
                } else {
                    RespEncoder::error("ERR invalid password")
                }
            } else {
                RespEncoder::error("ERR invalid argument")
            }
        } else {
            RespEncoder::error("ERR Client sent AUTH, but no password is set")
        }
    }

    fn cmd_select(&self, args: &[RespValue]) -> BytesMut {
        if args.is_empty() {
            return RespEncoder::error("ERR wrong number of arguments for 'select' command");
        }
        if let Some(db) = self.get_arg_int(&args[0]) {
            if db >= 0 && db < 16 {
                RespEncoder::ok()
            } else {
                RespEncoder::error("ERR DB index out of range")
            }
        } else {
            RespEncoder::error("ERR invalid DB index")
        }
    }

    // Server commands
    fn cmd_info(&self, _args: &[RespValue]) -> BytesMut {
        let info = format!(
            "# Server\r\n\
             redis_version:7.0.0\r\n\
             harness_version:1.1.0\r\n\
             redis_mode:standalone\r\n\
             os:Linux\r\n\
             tcp_port:6379\r\n\
             \r\n\
             # Clients\r\n\
             connected_clients:1\r\n\
             \r\n\
             # Memory\r\n\
             used_memory:0\r\n\
             used_memory_human:0B\r\n\
             \r\n\
             # Stats\r\n\
             total_connections_received:1\r\n\
             total_commands_processed:0\r\n\
             \r\n\
             # Keyspace\r\n\
             db0:keys={},expires=0,avg_ttl=0\r\n",
            self.storage.total_keys()
        );
        RespEncoder::bulk_string(info.as_bytes())
    }

    fn cmd_dbsize(&self, db_index: usize) -> BytesMut {
        if let Some(db) = self.storage.get_db(db_index) {
            RespEncoder::integer(db.len() as i64)
        } else {
            RespEncoder::integer(0)
        }
    }

    fn cmd_flushdb(&self, db_index: usize) -> BytesMut {
        if let Some(db) = self.storage.get_db(db_index) {
            db.clear();
        }
        RespEncoder::ok()
    }

    fn cmd_flushall(&self) -> BytesMut {
        self.storage.clear_all();
        RespEncoder::ok()
    }

    fn cmd_config(&self, args: &[RespValue]) -> BytesMut {
        if args.is_empty() {
            return RespEncoder::error("ERR wrong number of arguments for 'config' command");
        }
        let subcmd = self.get_arg_str(&args[0]).unwrap_or("").to_uppercase();
        match subcmd.as_str() {
            "GET" => {
                // Return empty array for simplicity
                RespEncoder::encode_to_bytes(&RespValue::Array(vec![]))
            }
            "SET" => RespEncoder::ok(),
            _ => RespEncoder::error("ERR Unknown CONFIG subcommand"),
        }
    }

    fn cmd_client(&self, args: &[RespValue]) -> BytesMut {
        if args.is_empty() {
            return RespEncoder::error("ERR wrong number of arguments for 'client' command");
        }
        let subcmd = self.get_arg_str(&args[0]).unwrap_or("").to_uppercase();
        match subcmd.as_str() {
            "SETNAME" => RespEncoder::ok(),
            "GETNAME" => RespEncoder::null(),
            "LIST" => RespEncoder::bulk_string(b"id=1 addr=127.0.0.1:12345 fd=5 name= db=0\n"),
            _ => RespEncoder::ok(),
        }
    }

    // Key commands
    fn cmd_del(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if let Some(db) = self.storage.get_db(db_index) {
            let mut count = 0;
            for arg in args {
                if let Some(key) = self.get_arg_str(arg) {
                    if db.del(key) {
                        count += 1;
                    }
                }
            }
            RespEncoder::integer(count)
        } else {
            RespEncoder::integer(0)
        }
    }

    fn cmd_exists(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if let Some(db) = self.storage.get_db(db_index) {
            let mut count = 0;
            for arg in args {
                if let Some(key) = self.get_arg_str(arg) {
                    if db.exists(key) {
                        count += 1;
                    }
                }
            }
            RespEncoder::integer(count)
        } else {
            RespEncoder::integer(0)
        }
    }

    fn cmd_expire(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.len() < 2 {
            return RespEncoder::error("ERR wrong number of arguments for 'expire' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k,
            None => return RespEncoder::error("ERR invalid key"),
        };
        let seconds = match self.get_arg_int(&args[1]) {
            Some(s) if s < 0 => return RespEncoder::error("ERR invalid expire time"),
            Some(s) => s,
            None => return RespEncoder::error("ERR invalid expire time"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            if db.expire(key, Duration::from_secs(seconds as u64)) {
                RespEncoder::integer(1)
            } else {
                RespEncoder::integer(0)
            }
        } else {
            RespEncoder::integer(0)
        }
    }

    fn cmd_ttl(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.is_empty() {
            return RespEncoder::error("ERR wrong number of arguments for 'ttl' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k,
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            RespEncoder::integer(db.ttl(key))
        } else {
            RespEncoder::integer(-2)
        }
    }

    fn cmd_type(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.is_empty() {
            return RespEncoder::error("ERR wrong number of arguments for 'type' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k,
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            match db.get(key) {
                Some(RedisValue::String(_)) => RespEncoder::encode_to_bytes(&RespValue::SimpleString("string".to_string())),
                Some(RedisValue::Hash(_)) => RespEncoder::encode_to_bytes(&RespValue::SimpleString("hash".to_string())),
                Some(RedisValue::List(_)) => RespEncoder::encode_to_bytes(&RespValue::SimpleString("list".to_string())),
                Some(RedisValue::Set(_)) => RespEncoder::encode_to_bytes(&RespValue::SimpleString("set".to_string())),
                Some(RedisValue::SortedSet(_)) => RespEncoder::encode_to_bytes(&RespValue::SimpleString("zset".to_string())),
                None => RespEncoder::encode_to_bytes(&RespValue::SimpleString("none".to_string())),
            }
        } else {
            RespEncoder::encode_to_bytes(&RespValue::SimpleString("none".to_string()))
        }
    }

    fn cmd_keys(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.is_empty() {
            return RespEncoder::error("ERR wrong number of arguments for 'keys' command");
        }
        let pattern = match self.get_arg_str(&args[0]) {
            Some(p) => p,
            None => return RespEncoder::error("ERR invalid pattern"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            let keys: Vec<RespValue> = db
                .keys(pattern)
                .into_iter()
                .map(|k| RespValue::BulkString(k.into_bytes()))
                .collect();
            RespEncoder::encode_to_bytes(&RespValue::Array(keys))
        } else {
            RespEncoder::encode_to_bytes(&RespValue::Array(vec![]))
        }
    }

    fn cmd_rename(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.len() < 2 {
            return RespEncoder::error("ERR wrong number of arguments for 'rename' command");
        }
        let old_key = match self.get_arg_str(&args[0]) {
            Some(k) => k.to_string(),
            None => return RespEncoder::error("ERR invalid key"),
        };
        let new_key = match self.get_arg_str(&args[1]) {
            Some(k) => k,
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            if let Some(val) = db.get(&old_key) {
                db.set(new_key.to_string(), val, None);
                db.del(&old_key);
                RespEncoder::ok()
            } else {
                RespEncoder::error("ERR no such key")
            }
        } else {
            RespEncoder::error("ERR no such key")
        }
    }

    // String commands
    fn cmd_get(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.is_empty() {
            return RespEncoder::error("ERR wrong number of arguments for 'get' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k,
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            match db.get(key) {
                Some(RedisValue::String(s)) => RespEncoder::bulk_string(s.as_bytes()),
                Some(_) => RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => RespEncoder::null(),
            }
        } else {
            RespEncoder::null()
        }
    }

    fn cmd_set(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.len() < 2 {
            return RespEncoder::error("ERR wrong number of arguments for 'set' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k.to_string(),
            None => return RespEncoder::error("ERR invalid key"),
        };
        let value = match self.get_arg_str(&args[1]) {
            Some(v) => v.to_string(),
            None => return RespEncoder::error("ERR invalid value"),
        };

        // Parse optional arguments: EX seconds, PX milliseconds, NX, XX
        let mut ttl = None;
        let mut nx = false;
        let mut xx = false;

        let mut i = 2;
        while i < args.len() {
            if let Some(opt) = self.get_arg_str(&args[i]) {
                match opt.to_uppercase().as_str() {
                    "EX" => {
                        if i + 1 < args.len() {
                            if let Some(secs) = self.get_arg_int(&args[i + 1]) {
                                if secs <= 0 {
                                    return RespEncoder::error("ERR invalid expire time in 'set' command");
                                }
                                ttl = Some(Duration::from_secs(secs as u64));
                            }
                            i += 2;
                        } else {
                            return RespEncoder::error("ERR syntax error");
                        }
                    }
                    "PX" => {
                        if i + 1 < args.len() {
                            if let Some(ms) = self.get_arg_int(&args[i + 1]) {
                                if ms <= 0 {
                                    return RespEncoder::error("ERR invalid expire time in 'set' command");
                                }
                                ttl = Some(Duration::from_millis(ms as u64));
                            }
                            i += 2;
                        } else {
                            return RespEncoder::error("ERR syntax error");
                        }
                    }
                    "NX" => {
                        nx = true;
                        i += 1;
                    }
                    "XX" => {
                        xx = true;
                        i += 1;
                    }
                    _ => {
                        i += 1;
                    }
                }
            } else {
                i += 1;
            }
        }

        if let Some(db) = self.storage.get_db(db_index) {
            // Check NX/XX conditions
            if nx && db.exists(&key) {
                return RespEncoder::null();
            }
            if xx && !db.exists(&key) {
                return RespEncoder::null();
            }

            db.set(key, RedisValue::String(value), ttl);
            RespEncoder::ok()
        } else {
            RespEncoder::error("ERR invalid database")
        }
    }

    fn cmd_mget(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if let Some(db) = self.storage.get_db(db_index) {
            let values: Vec<RespValue> = args
                .iter()
                .map(|arg| {
                    if let Some(key) = self.get_arg_str(arg) {
                        match db.get(key) {
                            Some(RedisValue::String(s)) => RespValue::BulkString(s.into_bytes()),
                            _ => RespValue::Null,
                        }
                    } else {
                        RespValue::Null
                    }
                })
                .collect();
            RespEncoder::encode_to_bytes(&RespValue::Array(values))
        } else {
            let values: Vec<RespValue> = args.iter().map(|_| RespValue::Null).collect();
            RespEncoder::encode_to_bytes(&RespValue::Array(values))
        }
    }

    fn cmd_mset(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.len() % 2 != 0 {
            return RespEncoder::error("ERR wrong number of arguments for MSET");
        }

        if let Some(db) = self.storage.get_db(db_index) {
            for chunk in args.chunks(2) {
                if let (Some(key), Some(value)) = (self.get_arg_str(&chunk[0]), self.get_arg_str(&chunk[1])) {
                    db.set(key.to_string(), RedisValue::String(value.to_string()), None);
                }
            }
            RespEncoder::ok()
        } else {
            RespEncoder::error("ERR invalid database")
        }
    }

    fn cmd_incr(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        self.cmd_incrby_internal(db_index, args, 1)
    }

    fn cmd_decr(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        self.cmd_incrby_internal(db_index, args, -1)
    }

    fn cmd_incrby(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.len() < 2 {
            return RespEncoder::error("ERR wrong number of arguments for 'incrby' command");
        }
        let increment = match self.get_arg_int(&args[1]) {
            Some(n) => n,
            None => return RespEncoder::error("ERR value is not an integer or out of range"),
        };
        self.cmd_incrby_internal(db_index, &args[0..1], increment)
    }

    fn cmd_decrby(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.len() < 2 {
            return RespEncoder::error("ERR wrong number of arguments for 'decrby' command");
        }
        let decrement = match self.get_arg_int(&args[1]) {
            Some(n) => n,
            None => return RespEncoder::error("ERR value is not an integer or out of range"),
        };
        self.cmd_incrby_internal(db_index, &args[0..1], -decrement)
    }

    fn cmd_incrby_internal(&self, db_index: usize, args: &[RespValue], increment: i64) -> BytesMut {
        if args.is_empty() {
            return RespEncoder::error("ERR wrong number of arguments");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k.to_string(),
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            let current = match db.get(&key) {
                Some(RedisValue::String(s)) => {
                    match s.parse::<i64>() {
                        Ok(n) => n,
                        Err(_) => return RespEncoder::error("ERR value is not an integer or out of range"),
                    }
                }
                Some(_) => return RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => 0,
            };

            let new_value = current + increment;
            db.set(key, RedisValue::String(new_value.to_string()), None);
            RespEncoder::integer(new_value)
        } else {
            RespEncoder::error("ERR invalid database")
        }
    }

    fn cmd_append(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.len() < 2 {
            return RespEncoder::error("ERR wrong number of arguments for 'append' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k.to_string(),
            None => return RespEncoder::error("ERR invalid key"),
        };
        let append_value = match self.get_arg_str(&args[1]) {
            Some(v) => v.to_string(),
            None => return RespEncoder::error("ERR invalid value"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            let current = match db.get(&key) {
                Some(RedisValue::String(s)) => s,
                Some(_) => return RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => String::new(),
            };

            let new_value = format!("{}{}", current, append_value);
            let len = new_value.len();
            db.set(key, RedisValue::String(new_value), None);
            RespEncoder::integer(len as i64)
        } else {
            RespEncoder::error("ERR invalid database")
        }
    }

    fn cmd_strlen(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.is_empty() {
            return RespEncoder::error("ERR wrong number of arguments for 'strlen' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k,
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            match db.get(key) {
                Some(RedisValue::String(s)) => RespEncoder::integer(s.len() as i64),
                Some(_) => RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => RespEncoder::integer(0),
            }
        } else {
            RespEncoder::integer(0)
        }
    }

    // Hash commands (simplified - implement most important ones)
    fn cmd_hget(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.len() < 2 {
            return RespEncoder::error("ERR wrong number of arguments for 'hget' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k,
            None => return RespEncoder::error("ERR invalid key"),
        };
        let field = match self.get_arg_str(&args[1]) {
            Some(f) => f,
            None => return RespEncoder::error("ERR invalid field"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            match db.get(key) {
                Some(RedisValue::Hash(h)) => {
                    match h.get(field) {
                        Some(v) => RespEncoder::bulk_string(v.as_bytes()),
                        None => RespEncoder::null(),
                    }
                }
                Some(_) => RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => RespEncoder::null(),
            }
        } else {
            RespEncoder::null()
        }
    }

    fn cmd_hset(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.len() < 3 || args.len() % 2 == 0 {
            return RespEncoder::error("ERR wrong number of arguments for 'hset' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k.to_string(),
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            let mut hash = match db.get(&key) {
                Some(RedisValue::Hash(h)) => h,
                Some(_) => return RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => std::collections::HashMap::new(),
            };

            let mut added = 0;
            for chunk in args[1..].chunks(2) {
                if chunk.len() == 2 {
                    if let (Some(field), Some(value)) = (self.get_arg_str(&chunk[0]), self.get_arg_str(&chunk[1])) {
                        if !hash.contains_key(field) {
                            added += 1;
                        }
                        hash.insert(field.to_string(), value.to_string());
                    }
                }
            }

            db.set(key, RedisValue::Hash(hash), None);
            RespEncoder::integer(added)
        } else {
            RespEncoder::error("ERR invalid database")
        }
    }

    fn cmd_hmget(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.len() < 2 {
            return RespEncoder::error("ERR wrong number of arguments for 'hmget' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k,
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            let hash = match db.get(key) {
                Some(RedisValue::Hash(h)) => h,
                Some(_) => return RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => std::collections::HashMap::new(),
            };

            let values: Vec<RespValue> = args[1..]
                .iter()
                .map(|arg| {
                    if let Some(field) = self.get_arg_str(arg) {
                        match hash.get(field) {
                            Some(v) => RespValue::BulkString(v.as_bytes().to_vec()),
                            None => RespValue::Null,
                        }
                    } else {
                        RespValue::Null
                    }
                })
                .collect();
            RespEncoder::encode_to_bytes(&RespValue::Array(values))
        } else {
            let values: Vec<RespValue> = args[1..].iter().map(|_| RespValue::Null).collect();
            RespEncoder::encode_to_bytes(&RespValue::Array(values))
        }
    }

    fn cmd_hmset(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        self.cmd_hset(db_index, args)
    }

    fn cmd_hdel(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.len() < 2 {
            return RespEncoder::error("ERR wrong number of arguments for 'hdel' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k.to_string(),
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            let mut hash = match db.get(&key) {
                Some(RedisValue::Hash(h)) => h,
                Some(_) => return RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => return RespEncoder::integer(0),
            };

            let mut removed = 0;
            for arg in &args[1..] {
                if let Some(field) = self.get_arg_str(arg) {
                    if hash.remove(field).is_some() {
                        removed += 1;
                    }
                }
            }

            if hash.is_empty() {
                db.del(&key);
            } else {
                db.set(key, RedisValue::Hash(hash), None);
            }
            RespEncoder::integer(removed)
        } else {
            RespEncoder::integer(0)
        }
    }

    fn cmd_hgetall(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.is_empty() {
            return RespEncoder::error("ERR wrong number of arguments for 'hgetall' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k,
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            match db.get(key) {
                Some(RedisValue::Hash(h)) => {
                    let mut result = Vec::with_capacity(h.len() * 2);
                    for (field, value) in h {
                        result.push(RespValue::BulkString(field.as_bytes().to_vec()));
                        result.push(RespValue::BulkString(value.as_bytes().to_vec()));
                    }
                    RespEncoder::encode_to_bytes(&RespValue::Array(result))
                }
                Some(_) => RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => RespEncoder::encode_to_bytes(&RespValue::Array(vec![])),
            }
        } else {
            RespEncoder::encode_to_bytes(&RespValue::Array(vec![]))
        }
    }

    fn cmd_hkeys(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.is_empty() {
            return RespEncoder::error("ERR wrong number of arguments for 'hkeys' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k,
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            match db.get(key) {
                Some(RedisValue::Hash(h)) => {
                    let keys: Vec<RespValue> = h
                        .keys()
                        .map(|k| RespValue::BulkString(k.as_bytes().to_vec()))
                        .collect();
                    RespEncoder::encode_to_bytes(&RespValue::Array(keys))
                }
                Some(_) => RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => RespEncoder::encode_to_bytes(&RespValue::Array(vec![])),
            }
        } else {
            RespEncoder::encode_to_bytes(&RespValue::Array(vec![]))
        }
    }

    fn cmd_hvals(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.is_empty() {
            return RespEncoder::error("ERR wrong number of arguments for 'hvals' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k,
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            match db.get(key) {
                Some(RedisValue::Hash(h)) => {
                    let vals: Vec<RespValue> = h
                        .values()
                        .map(|v| RespValue::BulkString(v.as_bytes().to_vec()))
                        .collect();
                    RespEncoder::encode_to_bytes(&RespValue::Array(vals))
                }
                Some(_) => RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => RespEncoder::encode_to_bytes(&RespValue::Array(vec![])),
            }
        } else {
            RespEncoder::encode_to_bytes(&RespValue::Array(vec![]))
        }
    }

    fn cmd_hlen(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.is_empty() {
            return RespEncoder::error("ERR wrong number of arguments for 'hlen' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k,
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            match db.get(key) {
                Some(RedisValue::Hash(h)) => RespEncoder::integer(h.len() as i64),
                Some(_) => RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => RespEncoder::integer(0),
            }
        } else {
            RespEncoder::integer(0)
        }
    }

    fn cmd_hexists(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.len() < 2 {
            return RespEncoder::error("ERR wrong number of arguments for 'hexists' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k,
            None => return RespEncoder::error("ERR invalid key"),
        };
        let field = match self.get_arg_str(&args[1]) {
            Some(f) => f,
            None => return RespEncoder::error("ERR invalid field"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            match db.get(key) {
                Some(RedisValue::Hash(h)) => {
                    if h.contains_key(field) {
                        RespEncoder::integer(1)
                    } else {
                        RespEncoder::integer(0)
                    }
                }
                Some(_) => RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => RespEncoder::integer(0),
            }
        } else {
            RespEncoder::integer(0)
        }
    }

    // List commands (implement most important ones)
    fn cmd_lpush(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.len() < 2 {
            return RespEncoder::error("ERR wrong number of arguments for 'lpush' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k.to_string(),
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            let mut list = match db.get(&key) {
                Some(RedisValue::List(l)) => l,
                Some(_) => return RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => std::collections::VecDeque::new(),
            };

            for arg in &args[1..] {
                if let Some(value) = self.get_arg_str(arg) {
                    list.push_front(value.to_string());
                }
            }

            let len = list.len();
            db.set(key, RedisValue::List(list), None);
            RespEncoder::integer(len as i64)
        } else {
            RespEncoder::error("ERR invalid database")
        }
    }

    fn cmd_rpush(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.len() < 2 {
            return RespEncoder::error("ERR wrong number of arguments for 'rpush' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k.to_string(),
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            let mut list = match db.get(&key) {
                Some(RedisValue::List(l)) => l,
                Some(_) => return RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => std::collections::VecDeque::new(),
            };

            for arg in &args[1..] {
                if let Some(value) = self.get_arg_str(arg) {
                    list.push_back(value.to_string());
                }
            }

            let len = list.len();
            db.set(key, RedisValue::List(list), None);
            RespEncoder::integer(len as i64)
        } else {
            RespEncoder::error("ERR invalid database")
        }
    }

    fn cmd_lpop(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.is_empty() {
            return RespEncoder::error("ERR wrong number of arguments for 'lpop' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k.to_string(),
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            let mut list = match db.get(&key) {
                Some(RedisValue::List(l)) => l,
                Some(_) => return RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => return RespEncoder::null(),
            };

            match list.pop_front() {
                Some(value) => {
                    if list.is_empty() {
                        db.del(&key);
                    } else {
                        db.set(key, RedisValue::List(list), None);
                    }
                    RespEncoder::bulk_string(value.as_bytes())
                }
                None => RespEncoder::null(),
            }
        } else {
            RespEncoder::null()
        }
    }

    fn cmd_rpop(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.is_empty() {
            return RespEncoder::error("ERR wrong number of arguments for 'rpop' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k.to_string(),
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            let mut list = match db.get(&key) {
                Some(RedisValue::List(l)) => l,
                Some(_) => return RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => return RespEncoder::null(),
            };

            match list.pop_back() {
                Some(value) => {
                    if list.is_empty() {
                        db.del(&key);
                    } else {
                        db.set(key, RedisValue::List(list), None);
                    }
                    RespEncoder::bulk_string(value.as_bytes())
                }
                None => RespEncoder::null(),
            }
        } else {
            RespEncoder::null()
        }
    }

    fn cmd_lrange(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.len() < 3 {
            return RespEncoder::error("ERR wrong number of arguments for 'lrange' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k,
            None => return RespEncoder::error("ERR invalid key"),
        };
        let start = match self.get_arg_int(&args[1]) {
            Some(n) => n,
            None => return RespEncoder::error("ERR invalid start index"),
        };
        let stop = match self.get_arg_int(&args[2]) {
            Some(n) => n,
            None => return RespEncoder::error("ERR invalid stop index"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            match db.get(key) {
                Some(RedisValue::List(l)) => {
                    let len = l.len() as i64;
                    let start = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
                    let stop = if stop < 0 { (len + stop).max(0) } else { stop.min(len - 1) } as usize;

                    if start > stop || start >= l.len() {
                        return RespEncoder::encode_to_bytes(&RespValue::Array(vec![]));
                    }

                    let values: Vec<RespValue> = l
                        .iter()
                        .skip(start)
                        .take(stop - start + 1)
                        .map(|v| RespValue::BulkString(v.as_bytes().to_vec()))
                        .collect();
                    RespEncoder::encode_to_bytes(&RespValue::Array(values))
                }
                Some(_) => RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => RespEncoder::encode_to_bytes(&RespValue::Array(vec![])),
            }
        } else {
            RespEncoder::encode_to_bytes(&RespValue::Array(vec![]))
        }
    }

    fn cmd_llen(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.is_empty() {
            return RespEncoder::error("ERR wrong number of arguments for 'llen' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k,
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            match db.get(key) {
                Some(RedisValue::List(l)) => RespEncoder::integer(l.len() as i64),
                Some(_) => RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => RespEncoder::integer(0),
            }
        } else {
            RespEncoder::integer(0)
        }
    }

    fn cmd_lindex(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.len() < 2 {
            return RespEncoder::error("ERR wrong number of arguments for 'lindex' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k,
            None => return RespEncoder::error("ERR invalid key"),
        };
        let index = match self.get_arg_int(&args[1]) {
            Some(n) => n,
            None => return RespEncoder::error("ERR invalid index"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            match db.get(key) {
                Some(RedisValue::List(l)) => {
                    let idx = if index < 0 { (l.len() as i64 + index) as usize } else { index as usize };
                    match l.get(idx) {
                        Some(v) => RespEncoder::bulk_string(v.as_bytes()),
                        None => RespEncoder::null(),
                    }
                }
                Some(_) => RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => RespEncoder::null(),
            }
        } else {
            RespEncoder::null()
        }
    }

    // Set commands (implement most important ones)
    fn cmd_sadd(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.len() < 2 {
            return RespEncoder::error("ERR wrong number of arguments for 'sadd' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k.to_string(),
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            let mut set = match db.get(&key) {
                Some(RedisValue::Set(s)) => s,
                Some(_) => return RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => std::collections::HashSet::new(),
            };

            let mut added = 0;
            for arg in &args[1..] {
                if let Some(member) = self.get_arg_str(arg) {
                    if set.insert(member.to_string()) {
                        added += 1;
                    }
                }
            }

            db.set(key, RedisValue::Set(set), None);
            RespEncoder::integer(added)
        } else {
            RespEncoder::error("ERR invalid database")
        }
    }

    fn cmd_srem(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.len() < 2 {
            return RespEncoder::error("ERR wrong number of arguments for 'srem' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k.to_string(),
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            let mut set = match db.get(&key) {
                Some(RedisValue::Set(s)) => s,
                Some(_) => return RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => return RespEncoder::integer(0),
            };

            let mut removed = 0;
            for arg in &args[1..] {
                if let Some(member) = self.get_arg_str(arg) {
                    if set.remove(member) {
                        removed += 1;
                    }
                }
            }

            if set.is_empty() {
                db.del(&key);
            } else {
                db.set(key, RedisValue::Set(set), None);
            }
            RespEncoder::integer(removed)
        } else {
            RespEncoder::integer(0)
        }
    }

    fn cmd_smembers(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.is_empty() {
            return RespEncoder::error("ERR wrong number of arguments for 'smembers' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k,
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            match db.get(key) {
                Some(RedisValue::Set(s)) => {
                    let members: Vec<RespValue> = s
                        .iter()
                        .map(|m| RespValue::BulkString(m.as_bytes().to_vec()))
                        .collect();
                    RespEncoder::encode_to_bytes(&RespValue::Array(members))
                }
                Some(_) => RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => RespEncoder::encode_to_bytes(&RespValue::Array(vec![])),
            }
        } else {
            RespEncoder::encode_to_bytes(&RespValue::Array(vec![]))
        }
    }

    fn cmd_scard(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.is_empty() {
            return RespEncoder::error("ERR wrong number of arguments for 'scard' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k,
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            match db.get(key) {
                Some(RedisValue::Set(s)) => RespEncoder::integer(s.len() as i64),
                Some(_) => RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => RespEncoder::integer(0),
            }
        } else {
            RespEncoder::integer(0)
        }
    }

    fn cmd_sismember(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.len() < 2 {
            return RespEncoder::error("ERR wrong number of arguments for 'sismember' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k,
            None => return RespEncoder::error("ERR invalid key"),
        };
        let member = match self.get_arg_str(&args[1]) {
            Some(m) => m,
            None => return RespEncoder::error("ERR invalid member"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            match db.get(key) {
                Some(RedisValue::Set(s)) => {
                    if s.contains(member) {
                        RespEncoder::integer(1)
                    } else {
                        RespEncoder::integer(0)
                    }
                }
                Some(_) => RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => RespEncoder::integer(0),
            }
        } else {
            RespEncoder::integer(0)
        }
    }

    // Sorted set commands (implement most important ones)
    fn cmd_zadd(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.len() < 3 {
            return RespEncoder::error("ERR wrong number of arguments for 'zadd' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k.to_string(),
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            let mut zset = match db.get(&key) {
                Some(RedisValue::SortedSet(z)) => z,
                Some(_) => return RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => std::collections::BTreeSet::new(),
            };

            let mut added = 0;
            for chunk in args[1..].chunks(2) {
                if chunk.len() == 2 {
                    if let (Some(score_str), Some(member)) = (self.get_arg_str(&chunk[0]), self.get_arg_str(&chunk[1])) {
                        if let Ok(score) = score_str.parse::<f64>() {
                            // Remove old entry if exists
                            zset.retain(|(_, m)| m != member);
                            // Add new entry
                            zset.insert((OrderedFloat(score), member.to_string()));
                            added += 1;
                        }
                    }
                }
            }

            db.set(key, RedisValue::SortedSet(zset), None);
            RespEncoder::integer(added)
        } else {
            RespEncoder::error("ERR invalid database")
        }
    }

    fn cmd_zrem(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.len() < 2 {
            return RespEncoder::error("ERR wrong number of arguments for 'zrem' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k.to_string(),
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            let mut zset = match db.get(&key) {
                Some(RedisValue::SortedSet(z)) => z,
                Some(_) => return RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => return RespEncoder::integer(0),
            };

            let mut removed = 0;
            for arg in &args[1..] {
                if let Some(member) = self.get_arg_str(arg) {
                    let before = zset.len();
                    zset.retain(|(_, m)| m != member);
                    if zset.len() < before {
                        removed += 1;
                    }
                }
            }

            if zset.is_empty() {
                db.del(&key);
            } else {
                db.set(key, RedisValue::SortedSet(zset), None);
            }
            RespEncoder::integer(removed)
        } else {
            RespEncoder::integer(0)
        }
    }

    fn cmd_zrange(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.len() < 3 {
            return RespEncoder::error("ERR wrong number of arguments for 'zrange' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k,
            None => return RespEncoder::error("ERR invalid key"),
        };
        let start = match self.get_arg_int(&args[1]) {
            Some(n) => n,
            None => return RespEncoder::error("ERR invalid start index"),
        };
        let stop = match self.get_arg_int(&args[2]) {
            Some(n) => n,
            None => return RespEncoder::error("ERR invalid stop index"),
        };
        let withscores = args.len() > 3 && self.get_arg_str(&args[3]).map_or(false, |s| s.to_uppercase() == "WITHSCORES");

        if let Some(db) = self.storage.get_db(db_index) {
            match db.get(key) {
                Some(RedisValue::SortedSet(z)) => {
                    let len = z.len() as i64;
                    let start = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
                    let stop = if stop < 0 { (len + stop).max(0) } else { stop.min(len - 1) } as usize;

                    if start > stop || start >= z.len() {
                        return RespEncoder::encode_to_bytes(&RespValue::Array(vec![]));
                    }

                    let mut result = Vec::new();
                    for (i, (score, member)) in z.iter().skip(start).take(stop - start + 1).enumerate() {
                        result.push(RespValue::BulkString(member.as_bytes().to_vec()));
                        if withscores {
                            result.push(RespValue::BulkString(score.0.to_string().into_bytes()));
                        }
                    }
                    RespEncoder::encode_to_bytes(&RespValue::Array(result))
                }
                Some(_) => RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => RespEncoder::encode_to_bytes(&RespValue::Array(vec![])),
            }
        } else {
            RespEncoder::encode_to_bytes(&RespValue::Array(vec![]))
        }
    }

    fn cmd_zcard(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.is_empty() {
            return RespEncoder::error("ERR wrong number of arguments for 'zcard' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k,
            None => return RespEncoder::error("ERR invalid key"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            match db.get(key) {
                Some(RedisValue::SortedSet(z)) => RespEncoder::integer(z.len() as i64),
                Some(_) => RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => RespEncoder::integer(0),
            }
        } else {
            RespEncoder::integer(0)
        }
    }

    fn cmd_zscore(&self, db_index: usize, args: &[RespValue]) -> BytesMut {
        if args.len() < 2 {
            return RespEncoder::error("ERR wrong number of arguments for 'zscore' command");
        }
        let key = match self.get_arg_str(&args[0]) {
            Some(k) => k,
            None => return RespEncoder::error("ERR invalid key"),
        };
        let member = match self.get_arg_str(&args[1]) {
            Some(m) => m,
            None => return RespEncoder::error("ERR invalid member"),
        };

        if let Some(db) = self.storage.get_db(db_index) {
            match db.get(key) {
                Some(RedisValue::SortedSet(z)) => {
                    match z.iter().find(|(_, m)| m == member) {
                        Some((score, _)) => RespEncoder::bulk_string(score.0.to_string().as_bytes()),
                        None => RespEncoder::null(),
                    }
                }
                Some(_) => RespEncoder::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
                None => RespEncoder::null(),
            }
        } else {
            RespEncoder::null()
        }
    }
}
