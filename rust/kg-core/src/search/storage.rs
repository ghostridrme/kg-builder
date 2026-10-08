//! Typed search operations implemented by graph storage adapters.

use super::SearchHit;
use crate::{errors::BackendError, traits::graph_backend::GraphEmbedding};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Scope shared by every retrieval operation. Empty lists impose no restriction.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchFilter {
    /// Mandatory organization boundary.
    pub org_id: String,
    /// Allowed namespaces for every entity endpoint and snapshot.
    pub namespaces: Vec<String>,
    /// Entity result types; relationships need one matching endpoint and snapshots a matching
    /// observed chain. Traversal may cross other entity types.
    pub entity_types: Vec<String>,
    /// Relationship names for traversal, relationship evidence, and dependency counts;
    /// does not filter standalone nodes or snapshots.
    pub relationship_types: Vec<String>,
    /// Fixed clock for current relationship reads within one search, including
    /// traversal and counts. Internal execution context; `as_of` takes precedence.
    #[serde(skip)]
    pub relationship_now: Option<DateTime<Utc>>,
    /// Entity/relationship visibility time; None selects current entity heads and
    /// relationship intervals containing the search's evaluation time.
    /// Standalone snapshots and observation counts use this as their capture cutoff.
    /// Relationship provenance may be learned later. Before an endpoint's recorded
    /// history, fact results use its earliest classification and return chain IDs;
    /// they do not claim a historical entity version exists.
    pub as_of: Option<DateTime<Utc>>,
    /// Restrict snapshot reads to members of one Saga. Only snapshot retrieval
    /// honors it; relationship and entity reads reject a filter that carries it,
    /// so a caller can never have the constraint silently dropped.
    #[serde(default)]
    pub saga_uuid: Option<Uuid>,
}
impl SearchFilter {
    /// Validate filter sizes and the required organization boundary.
    pub fn validate(&self) -> Result<(), BackendError> {
        if self.org_id.trim().is_empty()
            || self.org_id.len() > 1024
            || [
                &self.namespaces,
                &self.entity_types,
                &self.relationship_types,
            ]
            .iter()
            .any(|v| v.len() > 100 || v.iter().any(|s| s.trim().is_empty() || s.len() > 1024))
        {
            return Err(BackendError::Query("invalid search scope".into()));
        }
        Ok(())
    }
}

/// Bounded adapter result. Truncation means known matches were cut by a limit;
/// approximation means an index chose the candidates and neighbors may be missing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchPage<T> {
    /// Records in deterministic ranking order.
    pub items: Vec<T>,
    /// The adapter saw more matching records than the limit allowed.
    pub truncated: bool,
    /// Candidates came from an approximate index rather than an exhaustive scan.
    pub approximate: bool,
}
impl<T> SearchPage<T> {
    /// Preserve input order and trim to `limit`; callers fetch an extra record to detect overflow.
    pub fn bounded(mut items: Vec<T>, limit: usize) -> Self {
        let truncated = items.len() > limit;
        items.truncate(limit);
        Self {
            items,
            truncated,
            approximate: false,
        }
    }
    /// Mark the page as produced by an approximate index.
    pub fn approximate(mut self) -> Self {
        self.approximate = true;
        self
    }
}

/// Node retrieval operation. Chain constraints apply before similarity ranking.
#[derive(Debug, Clone)]
pub enum NodeQuery {
    /// Literal fulltext query.
    Fulltext(String),
    /// Exact cosine over embeddings from the same model.
    Similarity(GraphEmbedding),
    /// Resolve chain identities to versions visible under the filter.
    ByChain,
}
/// Counts requested independently to avoid unnecessary graph expansion.
#[derive(Debug, Clone, Copy, Default)]
pub struct NodeSignals {
    pub observations: bool,
    pub dependents: bool,
}

/// Candidate reads omit properties and embeddings; hydration loads the complete entity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeProjection {
    Candidate,
    Full,
}

/// Typed node retrieval request.
#[derive(Debug, Clone)]
pub struct NodeSearch {
    pub embedding_text_version: String,
    /// Organization, namespace, type, and temporal scope.
    pub filter: SearchFilter,
    pub query: NodeQuery,
    /// Optional neighborhood or candidate constraint. Some(empty) matches nothing.
    pub chain_ids: Option<Vec<Uuid>>,
    /// Maximum returned candidates, at most 500.
    pub limit: usize,
    /// Cosine cutoff in [-1, 1], applied only to similarity retrieval.
    pub min_score: f32,
    /// Unrequested counts are returned as None.
    pub signals: NodeSignals,
    pub projection: NodeProjection,
}
impl NodeSearch {
    /// Check bounded inputs before any database work.
    pub fn validate(&self) -> Result<(), BackendError> {
        self.filter.validate()?;
        crate::embedding::validate_entity_text_version(&self.embedding_text_version)?;
        if self.limit == 0
            || self.limit > 500
            || !self.min_score.is_finite()
            || !(-1.0..=1.0).contains(&self.min_score)
            || self.chain_ids.as_ref().is_some_and(|v| v.len() > 500)
        {
            return Err(BackendError::Query(
                "invalid node search budget or score".into(),
            ));
        }
        if let NodeQuery::Fulltext(text) = &self.query {
            if text.len() > 8192 || text.split_whitespace().next().is_none() {
                return Err(BackendError::Query(
                    "fulltext query must be 1 to 8192 bytes with at least one term".into(),
                ));
            }
        }
        if let NodeQuery::Similarity(embedding) = &self.query {
            embedding.validate()?;
        }
        if matches!(self.query, NodeQuery::ByChain) && self.chain_ids.is_none() {
            return Err(BackendError::Query(
                "chain lookup requires chain IDs".into(),
            ));
        }
        Ok(())
    }
}

/// Relationship/snapshot retrieval, optionally attached to entity chains.
#[derive(Debug, Clone)]
pub struct EvidenceSearch {
    /// Mandatory scope and source-valid time.
    pub filter: SearchFilter,
    /// Literal keyword query; None fetches evidence for the supplied chains.
    pub query: Option<String>,
    /// Text used only to select a snapshot passage; does not filter attached evidence.
    pub passage_query: Option<String>,
    /// Match a relationship endpoint or observed chain; Some(empty) matches nothing.
    pub chain_ids: Option<Vec<Uuid>>,
    /// Maximum records, at most 500.
    pub limit: usize,
}
impl EvidenceSearch {
    /// Validate request bounds and prevent accidental unbounded evidence scans.
    pub fn validate(&self) -> Result<(), BackendError> {
        self.filter.validate()?;
        if self.limit == 0
            || self.limit > 500
            || self.chain_ids.as_ref().is_some_and(|v| v.len() > 500)
            || self
                .query
                .as_ref()
                .is_some_and(|q| q.len() > 8192 || q.split_whitespace().next().is_none())
            || self.passage_query.as_ref().is_some_and(|q| q.len() > 8192)
            || (self.query.is_none() && self.chain_ids.is_none())
        {
            return Err(BackendError::Query("invalid evidence search".into()));
        }
        Ok(())
    }
}

/// Evidence attached to ranked entity chains: one storage call per record kind.
/// Each anchor's records come back in recency order, newest first, with the
/// record UUID as the only tie-breaker.
#[derive(Debug, Clone)]
pub struct AttachedEvidence {
    /// Mandatory scope and source-valid time.
    pub filter: SearchFilter,
    /// Ranked anchor chains, at most 100; duplicates are rejected.
    pub anchors: Vec<Uuid>,
    /// Maximum records per anchor, at most 500. Adapters read one extra record
    /// per anchor to report truncation.
    pub per_anchor: usize,
    /// Text used only to select a snapshot passage.
    pub passage_query: Option<String>,
}
impl AttachedEvidence {
    /// Bound the anchors, the per-anchor budget, and the total rows one call can return.
    pub fn validate(&self) -> Result<(), BackendError> {
        self.filter.validate()?;
        let unique = self
            .anchors
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len();
        if self.anchors.is_empty()
            || self.anchors.len() > 100
            || unique != self.anchors.len()
            || !(1..=500).contains(&self.per_anchor)
            || self.anchors.len() * self.per_anchor > 2_000
            || self.passage_query.as_ref().is_some_and(|q| q.len() > 8192)
        {
            return Err(BackendError::Query(
                "invalid attached evidence request".into(),
            ));
        }
        Ok(())
    }
}

/// One attached record and the anchor chain it was fetched for. A record shared
/// by several anchors appears once per anchor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attached<T> {
    pub anchor: Uuid,
    pub record: T,
}

/// Relationship fact with stable endpoints and source-valid timestamps.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationshipHit {
    pub uuid: Uuid,
    /// Stable source entity identity.
    pub source_chain_id: Uuid,
    /// Stable target entity identity.
    pub target_chain_id: Uuid,
    pub name: String,
    /// Source-grounded description.
    pub description: String,
    /// Beginning of the fact's validity window.
    pub valid_from: Option<String>,
    /// Earliest recorded validity end, invalidation, or deletion; exclusive.
    pub valid_to: Option<String>,
    /// First supporting snapshot, when visible in the requested namespace/time scope.
    pub snapshot_id: Option<Uuid>,
    /// Original retrieval score. Model scores use a separate scale.
    pub score: f32,
    pub model_score: Option<f32>,
}
/// Source evidence returned with its original provenance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotHit {
    pub uuid: Uuid,
    pub name: String,
    /// Original connector/source label.
    pub source: String,
    pub namespace: String,
    /// Original source capture time.
    pub captured_at: Option<String>,
    /// Up to 4,096 original source characters, without rewriting.
    pub content: String,
    /// True if the source text exceeds the returned excerpt.
    pub content_truncated: bool,
    /// Zero-based Unicode scalar offsets in stored content; end is exclusive.
    pub content_start: usize,
    pub content_end: usize,
    pub selection_kind: ExcerptSelection,
    /// Passage selection could not inspect the entire source.
    pub selection_limited: bool,
    /// Original retrieval score. Model scores use a separate scale.
    pub score: f32,
    pub model_score: Option<f32>,
}

pub type NodePage = SearchPage<SearchHit>;

/// Whether the excerpt contains a literal query match or a prefix fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExcerptSelection {
    Matched,
    Fallback,
}

/// Exact semantic fact recall, independent of entity matches.
#[derive(Debug, Clone)]
pub struct RelationshipSimilarity {
    pub filter: SearchFilter,
    pub embedding: GraphEmbedding,
    pub limit: usize,
    pub min_score: f32,
    /// Restrict to facts touching these chains (an anchored ranking of the facts
    /// attached to ranked entities); None searches the whole scope.
    pub anchor_chains: Option<Vec<Uuid>>,
}
impl RelationshipSimilarity {
    /// Bound recall and reject invalid vectors before storage work.
    pub fn validate(&self) -> Result<(), BackendError> {
        self.filter.validate()?;
        self.embedding.validate()?;
        if !(1..=500).contains(&self.limit)
            || !self.min_score.is_finite()
            || !(-1.0..=1.0).contains(&self.min_score)
            || self
                .anchor_chains
                .as_ref()
                .is_some_and(|chains| chains.is_empty() || chains.len() > 500)
        {
            return Err(BackendError::Query(
                "invalid semantic relationship search".into(),
            ));
        }
        Ok(())
    }
}

/// Explicit corpus readiness check; run at startup or after rebuilding embeddings.
/// This can scan the requested scope and is not performed for every search.
#[derive(Debug, Clone)]
pub struct EmbeddingReadinessRequest {
    pub entity_text_version: String,
    pub filter: SearchFilter,
    pub scope: super::SearchScope,
    pub model: String,
    pub dimensions: usize,
}
impl EmbeddingReadinessRequest {
    pub fn validate(&self) -> Result<(), BackendError> {
        self.filter.validate()?;
        crate::embedding::validate_entity_text_version(&self.entity_text_version)?;
        if self.model.trim().is_empty()
            || self.dimensions == 0
            || i64::try_from(self.dimensions).is_err()
            || self.scope == super::SearchScope::Snapshots
        {
            return Err(BackendError::Query(
                "invalid embedding readiness request".into(),
            ));
        }
        Ok(())
    }
}
/// Coverage for visible records. Missing and incompatible vectors are not search-ready.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EmbeddingReadiness {
    pub eligible: usize,
    pub compatible: usize,
    pub missing: usize,
    pub incompatible: usize,
}

#[cfg(test)]
mod clock_tests {
    use super::*;

    #[test]
    fn relationship_clock_is_execution_context_not_serialized_input() {
        let filter = SearchFilter {
            org_id: "test".into(),
            relationship_now: Some(Utc::now()),
            ..Default::default()
        };
        let mut value = serde_json::to_value(&filter).unwrap();
        assert!(value.get("relationship_now").is_none());
        assert!(serde_json::from_value::<SearchFilter>(value.clone())
            .unwrap()
            .relationship_now
            .is_none());
        value["relationship_now"] = serde_json::json!("2026-01-01T00:00:00Z");
        assert!(serde_json::from_value::<SearchFilter>(value).is_err());
    }
}
