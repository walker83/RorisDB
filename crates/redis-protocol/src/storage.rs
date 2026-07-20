//! Redis in-memory storage backend
//! Supports 16 databases (0-15), key expiration, and all Redis data types

use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Redis value types
#[derive(Debug, Clone)]
pub enum RedisValue {
    /// Simple string
    String(String),
    /// Hash map
    Hash(HashMap<String, String>),
    /// List (double-ended queue)
    List(VecDeque<String>),
    /// Set (unordered unique strings)
    Set(HashSet<String>),
    /// Sorted set (score + member)
    SortedSet(BTreeSet<(OrderedFloat, String)>),
}

/// Wrapper for f64 that implements Ord (for BTreeSet)
#[derive(Debug, Clone, Copy)]
pub struct OrderedFloat(pub f64);

impl PartialEq for OrderedFloat {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for OrderedFloat {}

impl PartialOrd for OrderedFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Entry with optional expiration
#[derive(Debug, Clone)]
struct Entry {
    value: RedisValue,
    expires_at: Option<Instant>,
}

/// Single Redis database
pub struct Database {
    data: DashMap<String, Entry>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            data: DashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<RedisValue> {
        self.data.get(key).and_then(|entry| {
            if let Some(expires_at) = entry.expires_at {
                if Instant::now() > expires_at {
                    let key_owned = entry.key().clone();
                    drop(entry);
                    // Use remove_if to avoid TOCTOU: only remove if still expired
                    self.data.remove_if(&key_owned, |_, v| {
                        v.expires_at.is_some_and(|t| Instant::now() > t)
                    });
                    return None;
                }
            }
            Some(entry.value.clone())
        })
    }

    pub fn set(&self, key: String, value: RedisValue, ttl: Option<Duration>) {
        let expires_at = ttl.map(|d| Instant::now() + d);
        self.data.insert(key, Entry { value, expires_at });
    }

    pub fn del(&self, key: &str) -> bool {
        self.data.remove(key).is_some()
    }

    pub fn exists(&self, key: &str) -> bool {
        if let Some(entry) = self.data.get(key) {
            if let Some(expires_at) = entry.expires_at {
                if Instant::now() > expires_at {
                    drop(entry);
                    self.data.remove(key);
                    return false;
                }
            }
            true
        } else {
            false
        }
    }

    pub fn expire(&self, key: &str, ttl: Duration) -> bool {
        if let Some(mut entry) = self.data.get_mut(key) {
            entry.expires_at = Some(Instant::now() + ttl);
            true
        } else {
            false
        }
    }

    pub fn ttl(&self, key: &str) -> i64 {
        if let Some(entry) = self.data.get(key) {
            match entry.expires_at {
                Some(expires_at) => {
                    let now = Instant::now();
                    if now > expires_at {
                        drop(entry);
                        self.data.remove(key);
                        -2 // Key doesn't exist or expired
                    } else {
                        expires_at.duration_since(now).as_secs() as i64
                    }
                }
                None => -1, // Key exists but no expiration
            }
        } else {
            -2 // Key doesn't exist
        }
    }

    pub fn keys(&self, pattern: &str) -> Vec<String> {
        let pattern = Self::glob_to_regex(pattern);
        let regex = regex::Regex::new(&pattern).ok();

        let now = Instant::now();
        let expired_keys: Vec<String> = self.data
            .iter()
            .filter(|entry| entry.expires_at.is_some_and(|t| now > t))
            .map(|entry| entry.key().clone())
            .collect();
        for key in expired_keys {
            self.data.remove(&key);
        }

        self.data
            .iter()
            .filter(|entry| {
                // Check pattern
                regex.as_ref().map_or(true, |re| re.is_match(entry.key()))
            })
            .map(|entry| entry.key().clone())
            .collect()
    }

    fn glob_to_regex(pattern: &str) -> String {
        let mut regex = String::from("^");
        let mut chars = pattern.chars().peekable();
        while let Some(ch) = chars.next() {
            match ch {
                '\\' => {
                    // Escaped character: \* matches literal *, \? matches literal ?
                    if let Some(&next) = chars.peek() {
                        regex.push('\\');
                        regex.push(next);
                        chars.next();
                    } else {
                        regex.push('\\');
                        regex.push('\\');
                    }
                }
                '*' => regex.push_str(".*"),
                '?' => regex.push('.'),
                '.' | '+' | '(' | ')' | '{' | '}' | '[' | ']' | '^' | '$' | '|' => {
                    regex.push('\\');
                    regex.push(ch);
                }
                _ => regex.push(ch),
            }
        }
        regex.push('$');
        regex
    }

    pub fn clear(&self) {
        self.data.clear();
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}

/// Redis storage backend with multiple databases
pub struct RedisStorage {
    databases: Vec<Database>,
}

impl RedisStorage {
    pub fn new(num_databases: usize) -> Self {
        let mut databases = Vec::with_capacity(num_databases);
        for _ in 0..num_databases {
            databases.push(Database::new());
        }
        Self { databases }
    }

    pub fn get_db(&self, db_index: usize) -> Option<&Database> {
        self.databases.get(db_index)
    }

    pub fn clear_all(&self) {
        for db in &self.databases {
            db.clear();
        }
    }

    pub fn total_keys(&self) -> usize {
        self.databases.iter().map(|db| db.len()).sum()
    }
}

impl Default for RedisStorage {
    fn default() -> Self {
        Self::new(16) // Redis default: 16 databases
    }
}
