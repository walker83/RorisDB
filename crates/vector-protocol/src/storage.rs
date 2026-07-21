//! Vector storage backend with HNSW index simulation

use dashmap::DashMap;
use std::sync::Arc;

pub struct VectorStorage {
    collections: DashMap<String, Arc<VectorCollection>>,
}

pub struct VectorCollection {
    vectors: DashMap<String, Vec<f32>>, // id -> vector
    metadata: DashMap<String, String>,   // id -> metadata JSON
}

impl VectorStorage {
    pub fn new() -> Self {
        Self {
            collections: DashMap::new(),
        }
    }

    pub fn create_collection(&self, name: &str, dimension: usize) {
        self.collections.insert(
            name.to_string(),
            Arc::new(VectorCollection::new(dimension)),
        );
    }

    pub fn get_collection(&self, name: &str) -> Option<Arc<VectorCollection>> {
        self.collections.get(name).map(|c| c.value().clone())
    }

    pub fn list_collections(&self) -> Vec<String> {
        self.collections.iter().map(|c| c.key().clone()).collect()
    }

    pub fn drop_collection(&self, name: &str) -> bool {
        self.collections.remove(name).is_some()
    }
}

impl VectorCollection {
    pub fn new(dimension: usize) -> Self {
        Self {
            vectors: DashMap::new(),
            metadata: DashMap::new(),
        }
    }

    pub fn insert(&self, id: &str, vector: Vec<f32>, metadata: String) {
        self.vectors.insert(id.to_string(), vector);
        self.metadata.insert(id.to_string(), metadata);
    }

    pub fn delete(&self, id: &str) -> bool {
        self.vectors.remove(id).is_some() && self.metadata.remove(id).is_some()
    }

    pub fn search(&self, query_vector: &[f32], top_k: usize) -> Vec<(String, f32, String)> {
        // Brute-force cosine similarity search (simplified HNSW)
        let mut results: Vec<(String, f32, String)> = self
            .vectors
            .iter()
            .map(|entry| {
                let id = entry.key().clone();
                let vector = entry.value();
                let similarity = cosine_similarity(query_vector, vector);
                let metadata = self.metadata.get(&id).map(|m| m.value().clone()).unwrap_or_default();
                (id, similarity, metadata)
            })
            .collect();

        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        results.truncate(top_k);
        results
    }

    pub fn count(&self) -> usize {
        self.vectors.len()
    }
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return 0.0;
    }

    let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }

    dot_product / (norm_a * norm_b)
}
