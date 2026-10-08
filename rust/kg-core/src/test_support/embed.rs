use async_trait::async_trait;
use xxhash_rust::xxh3::xxh3_64;

use crate::errors::BackendError;
use crate::traits::EmbedBackend;

/// Deterministic embedder: the same text always yields the same unit vector.
#[derive(Debug)]
pub struct MockEmbedBackend {
    dimension: usize,
}

impl MockEmbedBackend {
    /// Mock embedder producing vectors of the given dimension.
    pub fn new(dimension: usize) -> Self {
        Self { dimension }
    }

    /// Default 1536-dimension (matching OpenAI text-embedding-3-small).
    pub fn default_dimension() -> Self {
        Self { dimension: 1536 }
    }

    /// Generate a deterministic embedding from text using xxHash3.
    fn deterministic_embedding(&self, text: &str) -> Vec<f32> {
        let seed = xxh3_64(text.as_bytes());
        let mut embedding = Vec::with_capacity(self.dimension);

        let mut state = seed;
        for _ in 0..self.dimension {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            // All 32 upper bits are needed to cover both signs.
            let val = ((state >> 32) as f32) / (u32::MAX as f32) * 2.0 - 1.0;
            embedding.push(val);
        }

        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for v in &mut embedding {
                *v /= norm;
            }
        }

        embedding
    }
}

#[async_trait]
impl EmbedBackend for MockEmbedBackend {
    async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, BackendError> {
        Ok(texts
            .iter()
            .map(|t| self.deterministic_embedding(t))
            .collect())
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    fn max_batch_size(&self) -> usize {
        100
    }

    fn model_id(&self) -> &str {
        "mock-embed"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn deterministic_same_text() {
        let backend = MockEmbedBackend::new(128);
        let e1 = backend.embed_batch(&["hello world"]).await.unwrap();
        let e2 = backend.embed_batch(&["hello world"]).await.unwrap();
        assert_eq!(e1[0], e2[0], "Same text must produce same embedding");
    }

    #[tokio::test]
    async fn different_text_different_embedding() {
        let backend = MockEmbedBackend::new(128);
        let results = backend.embed_batch(&["hello", "world"]).await.unwrap();
        assert_ne!(results[0], results[1]);
    }

    #[tokio::test]
    async fn correct_dimension() {
        let backend = MockEmbedBackend::new(256);
        let results = backend.embed_batch(&["test"]).await.unwrap();
        assert_eq!(results[0].len(), 256);
    }

    #[tokio::test]
    async fn unit_normalized() {
        let backend = MockEmbedBackend::new(128);
        let results = backend.embed_batch(&["test"]).await.unwrap();
        let norm: f32 = results[0].iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(
            (norm - 1.0).abs() < 0.001,
            "Embedding should be unit-normalized, got {norm}"
        );
    }

    #[tokio::test]
    async fn unrelated_mock_vectors_are_not_biased_toward_each_other() {
        let vectors = MockEmbedBackend::new(1536)
            .embed_batch(&["unrelated-resource-a", "unrelated-resource-b"])
            .await
            .unwrap();
        for vector in &vectors {
            assert!(vector.iter().any(|v| *v > 0.0));
            assert!(vector.iter().any(|v| *v < 0.0));
        }
        let cosine: f32 = vectors[0].iter().zip(&vectors[1]).map(|(a, b)| a * b).sum();
        assert!(
            cosine.abs() < 0.1,
            "unrelated mock vectors have cosine {cosine}"
        );
    }
}
