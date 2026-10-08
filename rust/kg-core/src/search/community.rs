//! Community retrieval retains generation, revision and bounded member provenance.
use super::{NodeQuery, SearchFilter};
use crate::{errors::BackendError, traits::graph_backend::GraphEmbedding};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Entity version that supports a published community.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityEvidence {
    pub entity_uuid: Uuid,
    pub chain_id: Uuid,
    pub name: String,
    pub entity_type: String,
}
/// A time-valid community with stable publication provenance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityHit {
    pub uuid: Uuid,
    pub generation: Uuid,
    pub revision: Uuid,
    pub namespace: String,
    pub name: String,
    pub summary: String,
    pub source_hash: String,
    pub projected_at: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
    pub member_count: usize,
    pub members: Vec<CommunityEvidence>,
    pub members_truncated: bool,
    pub score: f32,
    pub score_breakdown: HashMap<String, f32>,
    #[serde(skip)]
    pub embedding: Option<GraphEmbedding>,
}
/// Storage recall or exact hydration within one organization.
#[derive(Debug, Clone)]
pub struct CommunitySearch {
    pub filter: SearchFilter,
    pub query: NodeQuery,
    /// Exact community identities for authoritative hydration.
    pub uuids: Option<Vec<Uuid>>,
    pub limit: usize,
    pub min_score: f32,
    /// Zero for candidate recall; hydrate only final member evidence.
    pub member_limit: usize,
}
impl CommunitySearch {
    pub fn validate(&self) -> Result<(), BackendError> {
        self.filter.validate()?;
        if !(1..=500).contains(&self.limit)
            || self.member_limit > 100
            || !self.min_score.is_finite()
            || !(-1.0..=1.0).contains(&self.min_score)
            || self.uuids.as_ref().is_some_and(|ids| {
                ids.is_empty()
                    || ids.len() > 500
                    || ids.iter().any(Uuid::is_nil)
                    || ids.iter().collect::<std::collections::HashSet<_>>().len() != ids.len()
            })
        {
            return Err(BackendError::Query(
                "invalid community search bounds".into(),
            ));
        }
        match &self.query {
            NodeQuery::ByChain if self.uuids.is_none() => Err(BackendError::Query(
                "community hydration requires identities".into(),
            )),
            NodeQuery::Fulltext(text) if text.trim().is_empty() || text.len() > 8192 => {
                Err(BackendError::Query("invalid community query".into()))
            }
            NodeQuery::Similarity(vector) => vector.validate(),
            _ => Ok(()),
        }
    }
}
