//! Search configuration and supported recipes.

use super::{RerankMethod, SearchMethod};
use crate::errors::BackendError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Independently searchable record kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchScope {
    /// Entity versions.
    Nodes,
    /// Relationship facts, with independent keyword and semantic recall.
    Relationships,
    /// Original source records.
    Snapshots,
    /// Published, time-valid communities.
    Communities,
}

/// Evidence ranking is independent of entity ranking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceReranker {
    Retrieval,
    Model,
}

/// Exact scans are an oracle for evaluation; indexed retrieval is approximate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorRetrieval {
    Exact,
    Indexed,
}

/// Bounded search request. Unknown fields are rejected to expose configuration errors.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SearchConfig {
    /// Candidate retrieval strategy. Indexed results disclose approximation.
    pub vector_retrieval: VectorRetrieval,
    /// Entity retrieval methods. Relationship methods are configured separately.
    pub methods: Vec<SearchMethod>,
    pub scopes: Vec<SearchScope>,
    /// Independent fulltext/vector fact recall for an explicit relationship scope.
    /// Attached facts are fetched by endpoint chains instead.
    pub relationship_methods: Vec<SearchMethod>,
    pub community_methods: Vec<SearchMethod>,
    pub community_reranker: RerankMethod,
    pub community_model_min_score: Option<f32>,
    /// Ranking strategy for entity candidates.
    pub reranker: RerankMethod,
    /// Maximum returned records per selected scope, from 1 to 100.
    pub limit: usize,
    /// Node recall and fused candidate budget, from limit to 500; traversal uses a 500-chain cap.
    pub prefetch: usize,
    /// Cosine cutoff for semantic retrieval, from -1 to 1; not a final ranking cutoff.
    pub min_score: f32,
    /// Minimum model relevance for entities; requires model ranking. No cutoff by default.
    /// A failed model call is an error when a cutoff is required.
    pub model_min_score: Option<f32>,
    /// Independent minimum model relevance for relationship and snapshot evidence.
    pub evidence_model_min_score: Option<f32>,
    /// MMR relevance weight, from 0 to 1.
    pub mmr_lambda: f32,
    /// Reciprocal rank fusion smoothing constant, from 1 to 1000. Sixty (Cormack et al.)
    /// lets lower ranks keep weight; one makes the top rank of each list dominate.
    pub rrf_k: f32,
    /// Rank exact identifier and name matches (a retrieval method reporting a perfect
    /// score) ahead of fused votes, so a literal id is never outranked by a neighbour
    /// another method happened to rank first.
    pub exact_match_first: bool,
    /// Maximum graph hops, from 1 to 5.
    pub bfs_max_depth: usize,
    /// Optional stable chain identity to seed traversal or anchored search.
    pub seed_chain_id: Option<Uuid>,
    /// Stable chain identity used by distance ranking; defaults to the seed.
    pub center_chain_id: Option<Uuid>,
    /// Automatic traversal seeds from initial rankings, from 1 to 20.
    pub expansion_seeds: usize,
    /// Namespace boundary for results, traversal intermediates, and evidence.
    pub namespaces: Vec<String>,
    /// Entity result types. Other types may be traversal intermediates.
    pub entity_types: Vec<String>,
    /// Relationship names allowed in traversal, relationship evidence, and dependency counts.
    pub relationship_types: Vec<String>,
    /// Source-valid time; None selects current entities/relationships and all capture times.
    pub as_of: Option<DateTime<Utc>>,
    /// Restrict results to snapshots that belong to one Saga. Accepted only with
    /// the snapshot scope alone and exactly one namespace; storage applies it
    /// before any candidate limit.
    pub saga_uuid: Option<Uuid>,
    /// Attach bounded relationship and snapshot evidence to returned entities.
    pub include_evidence: bool,
    /// Recall separately indexed, time-valid generated summaries.
    pub include_summaries: bool,
    /// Return observation and dependency counts. Frequency ranking always loads observations.
    pub include_signals: bool,
    /// Maximum attached records per evidence kind across all hits, from 1 to 100.
    /// An explicitly selected evidence scope uses `limit` and keyword matching instead.
    pub evidence_limit: usize,
    /// Candidate budget per evidence kind, from the effective output limit to 500.
    pub evidence_prefetch: usize,
    pub evidence_reranker: EvidenceReranker,
    /// Timeout per graph, index, embedding, or ranking operation, in milliseconds.
    pub operation_timeout_ms: u64,
    /// Execution deadline in milliseconds, from 1 to 120,000; expiry returns an error.
    /// Includes embedding and reranking; operation timeouts cannot exceed it.
    pub timeout_ms: u64,
}
impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            vector_retrieval: VectorRetrieval::Indexed,
            methods: vec![SearchMethod::Fulltext, SearchMethod::Vector],
            scopes: vec![SearchScope::Nodes],
            community_methods: vec![SearchMethod::Fulltext],
            community_reranker: RerankMethod::Rrf,
            community_model_min_score: None,
            relationship_methods: vec![SearchMethod::Fulltext],
            reranker: RerankMethod::Rrf,
            limit: 10,
            prefetch: 50,
            min_score: 0.0,
            model_min_score: None,
            evidence_model_min_score: None,
            mmr_lambda: 0.7,
            rrf_k: 60.0,
            exact_match_first: true,
            bfs_max_depth: 2,
            seed_chain_id: None,
            center_chain_id: None,
            expansion_seeds: 3,
            namespaces: vec![],
            entity_types: vec![],
            relationship_types: vec![],
            as_of: None,
            saga_uuid: None,
            include_evidence: true,
            include_summaries: false,
            include_signals: false,
            evidence_limit: 20,
            evidence_prefetch: 50,
            evidence_reranker: EvidenceReranker::Retrieval,
            operation_timeout_ms: 5_000,
            timeout_ms: 15_000,
        }
    }
}
impl SearchConfig {
    /// Keyword and semantic retrieval fused by rank. The fusion constant is 1: on the
    /// enterprise gold set (2026-09-27) it beat or tied the classic 60 on every cohort
    /// (paraphrase hit@1 0.65 vs 0.55, MRR 0.79 vs 0.74) with exact matches pinned
    /// first either way.
    pub fn hybrid_rrf() -> Self {
        Self {
            rrf_k: 1.0,
            ..Self::default()
        }
    }
    /// Keyword and semantic fact recall, independent of endpoint name matches.
    pub fn relationship_hybrid() -> Self {
        Self {
            scopes: vec![SearchScope::Relationships],
            relationship_methods: vec![SearchMethod::Fulltext, SearchMethod::Vector],
            include_evidence: false,
            ..Self::default()
        }
    }
    /// Literal keyword retrieval, requiring no embedding model.
    pub fn keyword_only() -> Self {
        Self {
            methods: vec![SearchMethod::Fulltext],
            ..Self::default()
        }
    }
    /// Semantic entity retrieval. Cosine cutoffs are model specific: with
    /// text-embedding-3-small a 0.5 cutoff kept only half of the relevant vectors on
    /// the enterprise gold set (2026-09-27); 0.35 kept 84 % and dropped 57 % of the
    /// irrelevant ones (paraphrase hit@1 0.75 vs 0.65).
    pub fn semantic_only() -> Self {
        Self {
            methods: vec![SearchMethod::Vector],
            min_score: 0.35,
            ..Self::default()
        }
    }
    /// Hybrid recall followed by greedy semantic diversity selection.
    pub fn hybrid_mmr() -> Self {
        Self {
            reranker: RerankMethod::Mmr,
            ..Self::default()
        }
    }
    /// Hybrid recall, automatic graph expansion, then a configured relevance model.
    pub fn hybrid_model() -> Self {
        Self {
            methods: vec![
                SearchMethod::Fulltext,
                SearchMethod::Vector,
                SearchMethod::Bfs,
            ],
            reranker: RerankMethod::Model,
            ..Self::default()
        }
    }
    /// Traverse live scoped relationships from a stable entity chain.
    pub fn graph_traversal(seed: Uuid, depth: usize) -> Self {
        Self {
            methods: vec![SearchMethod::Bfs],
            seed_chain_id: Some(seed),
            bfs_max_depth: depth,
            ..Self::default()
        }
    }
    /// Rank semantic matches inside a bounded graph neighborhood, including its seed.
    pub fn graph_anchored(seed: Uuid) -> Self {
        Self {
            methods: vec![SearchMethod::GraphAnchored],
            seed_chain_id: Some(seed),
            ..Self::default()
        }
    }
    /// Validate numeric limits and method combinations; the engine also validates filters and models.
    pub fn validate(&self) -> Result<(), BackendError> {
        if let Some(saga) = self.saga_uuid {
            if saga.is_nil()
                || self.scopes != [SearchScope::Snapshots]
                || self.namespaces.len() != 1
            {
                return Err(BackendError::Query(
                    "saga filter requires the snapshot scope alone and exactly one namespace"
                        .into(),
                ));
            }
        }
        if self
            .community_model_min_score
            .is_some_and(|score| !score.is_finite())
            || (self.community_model_min_score.is_some()
                && (self.community_reranker != RerankMethod::Model
                    || !self.scopes.contains(&SearchScope::Communities)))
        {
            return Err(BackendError::Query(
                "community model cutoff requires community model ranking".into(),
            ));
        }
        if self.community_methods.is_empty()
            || self
                .community_methods
                .iter()
                .any(|method| !matches!(method, SearchMethod::Fulltext | SearchMethod::Vector))
            || self
                .community_methods
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != self.community_methods.len()
            || !matches!(
                self.community_reranker,
                RerankMethod::Rrf | RerankMethod::Mmr | RerankMethod::Model
            )
            || (!self.scopes.contains(&SearchScope::Communities)
                && (self.community_methods != [SearchMethod::Fulltext]
                    || self.community_reranker != RerankMethod::Rrf))
        {
            return Err(BackendError::Query(
                "invalid community retrieval or ranking configuration".into(),
            ));
        }
        if self.model_min_score.is_some_and(|s| !s.is_finite())
            || self
                .evidence_model_min_score
                .is_some_and(|s| !s.is_finite())
            || (self.model_min_score.is_some() && self.reranker != RerankMethod::Model)
            || (self.evidence_model_min_score.is_some()
                && self.evidence_reranker != EvidenceReranker::Model)
        {
            return Err(BackendError::Query(
                "model cutoffs require model ranking and finite scores".into(),
            ));
        }
        if self.evidence_model_min_score.is_some()
            && !self
                .scopes
                .iter()
                .any(|scope| matches!(scope, SearchScope::Relationships | SearchScope::Snapshots))
            && !self.include_evidence
        {
            return Err(BackendError::Query(
                "evidence model cutoff requires an evidence scope or attached evidence".into(),
            ));
        }
        if self.relationship_methods.is_empty()
            || self
                .relationship_methods
                .iter()
                .any(|m| !matches!(m, SearchMethod::Fulltext | SearchMethod::Vector))
            || self
                .relationship_methods
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != self.relationship_methods.len()
            || (!self.scopes.contains(&SearchScope::Relationships)
                && self.relationship_methods != [SearchMethod::Fulltext])
        {
            return Err(BackendError::Query(
                "invalid relationship retrieval methods".into(),
            ));
        }
        if self.scopes.is_empty()
            || self
                .scopes
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != self.scopes.len()
            || self
                .methods
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != self.methods.len()
            || !(1..=100).contains(&self.limit)
            || !(self.limit..=500).contains(&self.prefetch)
            || !(1..=5).contains(&self.bfs_max_depth)
            || !(1..=20).contains(&self.expansion_seeds)
            || !(1..=100).contains(&self.evidence_limit)
            || !(1..=120_000).contains(&self.timeout_ms)
            || self.operation_timeout_ms == 0
            || self.operation_timeout_ms > self.timeout_ms
            || !self.min_score.is_finite()
            || !(-1.0..=1.0).contains(&self.min_score)
            || !self.mmr_lambda.is_finite()
            || !(0.0..=1.0).contains(&self.mmr_lambda)
            || !self.rrf_k.is_finite()
            || !(1.0..=1000.0).contains(&self.rrf_k)
        {
            return Err(BackendError::Query("invalid search configuration".into()));
        }
        for scope in [SearchScope::Relationships, SearchScope::Snapshots] {
            let output = if self.scopes.contains(&scope) {
                self.limit
            } else if self.include_evidence && self.scopes.contains(&SearchScope::Nodes) {
                self.evidence_limit
            } else {
                1
            };
            if !(output..=500).contains(&self.evidence_prefetch) {
                return Err(BackendError::Query(
                    "invalid evidence candidate budget".into(),
                ));
            }
        }
        if !self.scopes.contains(&SearchScope::Nodes)
            && (self.reranker != RerankMethod::Rrf
                || self.seed_chain_id.is_some()
                || self.center_chain_id.is_some()
                || self
                    .methods
                    .iter()
                    .any(|m| matches!(m, SearchMethod::Bfs | SearchMethod::GraphAnchored)))
        {
            return Err(BackendError::Query(
                "entity ranking and traversal require node scope".into(),
            ));
        }
        if self.scopes.contains(&SearchScope::Nodes) {
            if self.methods.is_empty() {
                return Err(BackendError::Query(
                    "node search requires a retrieval method".into(),
                ));
            }
            if self.methods.contains(&SearchMethod::GraphAnchored) && self.seed_chain_id.is_none() {
                return Err(BackendError::Query(
                    "anchored search requires a seed chain".into(),
                ));
            }
            if self.reranker == RerankMethod::NodeDistance
                && self.center_chain_id.or(self.seed_chain_id).is_none()
            {
                return Err(BackendError::Query(
                    "distance ranking requires a center chain".into(),
                ));
            }
            if self.methods == [SearchMethod::Bfs] && self.seed_chain_id.is_none() {
                return Err(BackendError::Query(
                    "standalone traversal requires a seed chain".into(),
                ));
            }
        }
        Ok(())
    }
}
