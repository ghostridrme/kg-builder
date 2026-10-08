//! Greedy maximal marginal relevance over hydrated entity hits.
use kg_core::search::SearchHit;
use std::collections::HashSet;

/// Per-candidate features computed once: the embedding norm when a usable
/// vector exists, and lowercase name tokens for the fallback similarity.
struct Prepared {
    norm: Option<f64>,
    tokens: HashSet<String>,
}

impl Prepared {
    async fn new(hit: &SearchHit) -> Self {
        let mut norm = None;
        if let Some(vector) = &hit.embedding {
            if !vector.model.trim().is_empty() {
                let mut squares = 0.0;
                for chunk in vector.values.chunks(1024) {
                    tokio::task::yield_now().await;
                    squares += chunk.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>();
                }
                if squares.is_finite() && squares > 0.0 {
                    norm = Some(squares.sqrt());
                }
            }
        }
        Self {
            norm,
            tokens: hit
                .name
                .split(|c: char| !c.is_alphanumeric())
                .filter(|s| !s.is_empty())
                .map(str::to_lowercase)
                .collect(),
        }
    }
}

/// Cosine similarity when both hits carry vectors from the same model with the
/// same dimension; otherwise Jaccard overlap of name tokens. Same chain is 1.0.
async fn similarity(a: &SearchHit, b: &SearchHit, pa: &Prepared, pb: &Prepared) -> f32 {
    tokio::task::yield_now().await;
    if a.chain_id == b.chain_id {
        return 1.0;
    }
    if let (Some(a), Some(b), Some(na), Some(nb)) = (&a.embedding, &b.embedding, pa.norm, pb.norm) {
        if a.model == b.model && a.values.len() == b.values.len() {
            let mut dot = 0.0;
            for (a, b) in a.values.chunks(1024).zip(b.values.chunks(1024)) {
                dot += a
                    .iter()
                    .zip(b)
                    .map(|(a, b)| f64::from(*a) * f64::from(*b))
                    .sum::<f64>();
                tokio::task::yield_now().await;
            }
            return (dot / (na * nb)).clamp(0.0, 1.0) as f32;
        }
    }
    if pa.tokens.is_empty() || pb.tokens.is_empty() {
        0.0
    } else {
        pa.tokens.intersection(&pb.tokens).count() as f32
            / pa.tokens.union(&pb.tokens).count() as f32
    }
}
/// Greedy MMR: penalize similarity only to already-selected entities.
/// Supply one version per chain, finite relevance scores in 0–1, and lambda in 0–1.
/// Candidates need hydrated names/embeddings; selected scores become MMR scores.
/// Missing or incompatible embeddings use name-token overlap. Norms and tokens
/// are prepared once; bounded chunks yield so request cancellation can stop ranking.
pub async fn maximal_marginal_relevance(
    mut candidates: Vec<SearchHit>,
    lambda: f32,
    limit: usize,
) -> Vec<SearchHit> {
    let mut prepared = Vec::with_capacity(candidates.len());
    for hit in &candidates {
        tokio::task::yield_now().await;
        prepared.push(Prepared::new(hit).await);
    }
    let mut selected = Vec::new();
    let mut penalties = vec![0.0f32; candidates.len()];
    while !candidates.is_empty() && selected.len() < limit {
        let score = |i: usize| lambda * candidates[i].score - (1.0 - lambda) * penalties[i];
        let best = (0..candidates.len())
            .max_by(|a, b| {
                score(*a)
                    .total_cmp(&score(*b))
                    .then(candidates[*b].chain_id.cmp(&candidates[*a].chain_id))
            })
            .unwrap();
        let mmr = score(best);
        let mut hit = candidates.remove(best);
        penalties.remove(best);
        let selected_features = prepared.remove(best);
        for (i, candidate) in candidates.iter().enumerate() {
            penalties[i] = penalties[i]
                .max(similarity(&hit, candidate, &selected_features, &prepared[i]).await);
        }
        hit.score = mmr;
        hit.score_breakdown.insert("mmr".into(), mmr);
        selected.push(hit);
    }
    selected
}

#[cfg(test)]
mod tests {
    use super::*;
    use kg_core::traits::graph_backend::GraphEmbedding;
    use uuid::Uuid;

    fn hit(n: u128, name: &str, score: f32, embedding: Option<(&str, Vec<f32>)>) -> SearchHit {
        SearchHit {
            derived_summary: None,
            uuid: Uuid::from_u128(n),
            chain_id: Uuid::from_u128(n),
            entity_type: "Service".into(),
            namespace: "prod".into(),
            name: name.into(),
            score,
            score_breakdown: Default::default(),
            properties: serde_json::json!({}),
            graph_distance: None,
            embedding: embedding.map(|(model, values)| GraphEmbedding {
                model: model.into(),
                values,
            }),
            observation_count: None,
            dependent_count: None,
            last_changed_at: None,
            owner: None,
        }
    }
    fn chains(hits: &[SearchHit]) -> Vec<u128> {
        hits.iter().map(|h| h.chain_id.as_u128()).collect()
    }

    #[tokio::test]
    async fn name_tokens_drive_diversity_when_vectors_are_missing_or_incompatible() {
        let missing = vec![
            hit(1, "checkout api", 1.0, None),
            hit(2, "checkout api", 0.9, None),
            hit(3, "billing db", 0.8, None),
        ];
        assert_eq!(
            chains(&maximal_marginal_relevance(missing, 0.3, 2).await),
            [1, 3]
        );
        // Chain 2's vector is orthogonal to chain 1's but comes from another model,
        // so only its identical name may be compared, and it is penalised.
        let mixed = vec![
            hit(1, "checkout api", 1.0, Some(("a", vec![1.0, 0.0]))),
            hit(2, "checkout api", 0.9, Some(("b", vec![0.0, 1.0]))),
            hit(3, "billing db", 0.8, Some(("a", vec![0.0, 1.0]))),
        ];
        assert_eq!(
            chains(&maximal_marginal_relevance(mixed, 0.3, 2).await),
            [1, 3]
        );
    }

    #[tokio::test]
    async fn lambda_one_keeps_relevance_order_and_lambda_zero_maximises_diversity() {
        let candidates = vec![
            hit(1, "a", 0.5, Some(("m", vec![1.0, 0.0]))),
            hit(2, "b", 0.9, Some(("m", vec![1.0, 0.0]))),
            hit(3, "c", 0.7, Some(("m", vec![0.0, 1.0]))),
        ];
        let relevance = maximal_marginal_relevance(candidates.clone(), 1.0, 3).await;
        assert_eq!(chains(&relevance), [2, 3, 1]);
        assert!((relevance[0].score - 0.9).abs() < 1e-6);
        assert_eq!(relevance[0].score_breakdown["mmr"], relevance[0].score);
        // Every initial score ties at zero, so the lowest chain wins, then the orthogonal one.
        let diverse = maximal_marginal_relevance(candidates, 0.0, 2).await;
        assert_eq!(chains(&diverse), [1, 3]);
    }

    #[tokio::test]
    async fn limit_and_empty_input_are_respected() {
        assert!(maximal_marginal_relevance(vec![], 0.5, 3).await.is_empty());
        let candidates = (1..=5)
            .map(|n| hit(n, &format!("n{n}"), 0.5, None))
            .collect();
        assert_eq!(
            maximal_marginal_relevance(candidates, 0.5, 2).await.len(),
            2
        );
    }
}
