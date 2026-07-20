//! InfluxDB time series storage backend

use crate::line_protocol::{FieldValue, Point};
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;

/// Time series data point
#[derive(Debug, Clone)]
pub struct TimeSeriesPoint {
    pub timestamp: i64,
    pub fields: HashMap<String, FieldValue>,
    pub tags: HashMap<String, String>,
}

/// InfluxDB measurement (table)
pub struct Measurement {
    points: DashMap<String, TimeSeriesPoint>, // composite key -> point
    counter: std::sync::atomic::AtomicU64,   // for unique keys
}

impl Measurement {
    pub fn new() -> Self {
        Self {
            points: DashMap::new(),
            counter: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn write(&self, point: Point) {
        let timestamp = point.timestamp.unwrap_or_else(|| {
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        });

        // Use composite key to prevent timestamp collisions
        let seq = self.counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let key = format!("{}_{:016x}", timestamp, seq);

        let ts_point = TimeSeriesPoint {
            timestamp,
            fields: point.fields,
            tags: point.tags,
        };

        self.points.insert(key, ts_point);
    }

    pub fn query(&self, start: Option<i64>, end: Option<i64>) -> Vec<TimeSeriesPoint> {
        let mut results: Vec<TimeSeriesPoint> = self.points
            .iter()
            .filter(|entry| {
                let ts = entry.value().timestamp;
                if let Some(s) = start {
                    if ts < s {
                        return false;
                    }
                }
                if let Some(e) = end {
                    if ts > e {
                        return false;
                    }
                }
                true
            })
            .map(|entry| entry.value().clone())
            .collect();

        results.sort_by_key(|p| p.timestamp);
        results
    }

    pub fn count(&self) -> usize {
        self.points.len()
    }
}

impl Default for Measurement {
    fn default() -> Self {
        Self::new()
    }
}

/// InfluxDB database
pub struct Database {
    measurements: DashMap<String, Arc<Measurement>>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            measurements: DashMap::new(),
        }
    }

    pub fn get_measurement(&self, name: &str) -> Arc<Measurement> {
        self.measurements
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Measurement::new()))
            .clone()
    }

    pub fn list_measurements(&self) -> Vec<String> {
        self.measurements.iter().map(|entry| entry.key().clone()).collect()
    }

    pub fn drop_measurement(&self, name: &str) -> bool {
        self.measurements.remove(name).is_some()
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

/// InfluxDB storage backend
pub struct InfluxDBStorage {
    databases: DashMap<String, Arc<Database>>,
}

impl InfluxDBStorage {
    pub fn new() -> Self {
        Self {
            databases: DashMap::new(),
        }
    }

    pub fn get_database(&self, name: &str) -> Arc<Database> {
        self.databases
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Database::new()))
            .clone()
    }

    pub fn list_databases(&self) -> Vec<String> {
        self.databases.iter().map(|entry| entry.key().clone()).collect()
    }

    pub fn create_database(&self, name: &str) {
        self.databases.entry(name.to_string()).or_insert_with(|| Arc::new(Database::new()));
    }

    pub fn drop_database(&self, name: &str) -> bool {
        self.databases.remove(name).is_some()
    }
}

impl Default for InfluxDBStorage {
    fn default() -> Self {
        Self::new()
    }
}
