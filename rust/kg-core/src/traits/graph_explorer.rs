//! Bounded, read-only graph navigation for interactive clients.
use crate::{errors::BackendError, search::SearchPage};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExplorerDirection {
    #[default]
    Both,
    Out,
    In,
}

#[derive(Clone, Debug)]
pub enum ExplorerQuery {
    /// Bounded organization-wide commit markers for interactive refresh.
    GraphRevision,
    Catalog,
    EntityVersion {
        uuid: Uuid,
    },
    VersionHeaders {
        entity_type: String,
        chain_id: Uuid,
    },
    /// Canvas-only reads omit property bodies in storage, not after transport.
    CanvasEntity {
        entity_type: String,
        chain_id: Uuid,
    },
    CanvasNeighbors {
        entity_type: String,
        chain_id: Uuid,
        direction: ExplorerDirection,
        entity_types: Vec<String>,
    },
    /// Published community detail and a bounded page of its membership.
    Community {
        uuid: Uuid,
        member_offset: usize,
        member_limit: usize,
    },
    /// Effective-time events, including intermediate versions and relationship closures.
    Changes {
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        chains: Vec<Uuid>,
        event_kinds: Vec<String>,
    },
    /// Source records observing exact entity versions, never repointed by chain.
    SnapshotObservations {
        versions: Vec<Uuid>,
    },
    Snapshot {
        uuid: Uuid,
    },
    /// Compact, scope-checked endpoint hydration for search results.
    EntitiesByChains {
        chains: Vec<Uuid>,
    },
    CanvasRelationship {
        edge_id: Uuid,
    },
    Relationship {
        edge_id: Uuid,
    },
    /// Visible entities in a scope, including disconnected nodes.
    Entities {
        entity_types: Vec<String>,
    },
    NamespaceRelationships {
        chains: Vec<Uuid>,
    },
    /// Paged filter options, aggregated in storage before limiting.
    Filters {
        namespace_dimension: bool,
        search: String,
    },
    Entity {
        entity_type: String,
        chain_id: Uuid,
    },
    VersionHistory {
        entity_type: String,
        chain_id: Uuid,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
        newest_first: bool,
    },
    Versions {
        entity_type: String,
        chain_id: Uuid,
    },
    Neighbors {
        entity_type: String,
        chain_id: Uuid,
        direction: ExplorerDirection,
        /// Only neighbours of these entity types; empty means every type.
        entity_types: Vec<String>,
    },
}

/// The most entity types one read may filter by.
pub const MAX_ENTITY_TYPE_FILTERS: usize = 32;

#[derive(Clone, Debug)]
pub struct ExplorerRequest {
    pub org_id: String,
    pub namespace: Option<String>,
    pub as_of: Option<DateTime<Utc>>,
    pub limit: usize,
    pub offset: usize,
    pub query: ExplorerQuery,
}
impl ExplorerRequest {
    pub fn validate(&self) -> Result<(), BackendError> {
        let text =
            |v: &str| !v.trim().is_empty() && v.len() <= 256 && !v.chars().any(char::is_control);
        let identity_valid = match &self.query {
            ExplorerQuery::Changes {
                from,
                to,
                chains,
                event_kinds,
            } => {
                from < to
                    && (*to - *from).num_days() <= 366
                    && event_kinds.len() <= 7
                    && event_kinds.iter().all(|k| {
                        matches!(
                            k.as_str(),
                            "entity_version"
                                | "entity_deleted"
                                | "relationship_opened"
                                | "relationship_version"
                                | "relationship_closed"
                                | "relationship_deleted"
                                | "relationship_cancelled"
                        )
                    })
                    && chains.len() <= 32
                    && chains.iter().all(|id| !id.is_nil())
            }
            ExplorerQuery::GraphRevision
            | ExplorerQuery::Catalog
            | ExplorerQuery::Entities { .. } => true,
            ExplorerQuery::Community {
                uuid,
                member_offset,
                member_limit,
            } => !uuid.is_nil() && *member_offset <= 100_000 && (1..=20).contains(member_limit),
            ExplorerQuery::EntityVersion { uuid } | ExplorerQuery::Snapshot { uuid } => {
                !uuid.is_nil()
            }
            ExplorerQuery::SnapshotObservations { versions } => {
                !versions.is_empty()
                    && versions.len() <= 2000
                    && versions.iter().all(|id| !id.is_nil())
            }
            ExplorerQuery::EntitiesByChains { chains } => {
                !chains.is_empty() && chains.len() <= 200 && chains.iter().all(|id| !id.is_nil())
            }
            ExplorerQuery::CanvasRelationship { edge_id }
            | ExplorerQuery::Relationship { edge_id } => !edge_id.is_nil(),
            ExplorerQuery::NamespaceRelationships { chains } => {
                !chains.is_empty() && chains.len() <= 2000 && chains.iter().all(|id| !id.is_nil())
            }
            ExplorerQuery::Filters { search, .. } => {
                search.len() <= 256 && !search.chars().any(char::is_control)
            }
            ExplorerQuery::VersionHeaders {
                entity_type,
                chain_id,
            }
            | ExplorerQuery::CanvasEntity {
                entity_type,
                chain_id,
            }
            | ExplorerQuery::CanvasNeighbors {
                entity_type,
                chain_id,
                ..
            }
            | ExplorerQuery::Entity {
                entity_type,
                chain_id,
            }
            | ExplorerQuery::VersionHistory {
                entity_type,
                chain_id,
                ..
            }
            | ExplorerQuery::Versions {
                entity_type,
                chain_id,
            }
            | ExplorerQuery::Neighbors {
                entity_type,
                chain_id,
                ..
            } => text(entity_type) && !chain_id.is_nil(),
        };
        let types_valid = match &self.query {
            ExplorerQuery::CanvasNeighbors { entity_types, .. }
            | ExplorerQuery::Neighbors { entity_types, .. }
            | ExplorerQuery::Entities { entity_types } => {
                entity_types.len() <= MAX_ENTITY_TYPE_FILTERS
                    && entity_types.iter().all(|t| text(t))
            }
            _ => true,
        };
        let history_valid = match &self.query {
            ExplorerQuery::VersionHistory {
                from: Some(from),
                to: Some(to),
                ..
            } => from <= to,
            _ => true,
        };
        if !history_valid
            || !text(&self.org_id)
            || self.namespace.as_deref().is_some_and(|n| !text(n))
            || !(1..=200).contains(&self.limit)
            || self.offset > 100_000
            || !identity_valid
            || !types_valid
        {
            return Err(BackendError::Query("invalid graph explorer request".into()));
        }
        Ok(())
    }
}

#[async_trait]
pub trait GraphExplorerBackend: Send + Sync {
    /// Wait for cancellation cleanup already scheduled by dropped reads. No new reads.
    async fn settle_cancelled_reads(&self) {}

    /// Each item contains source properties and bounded metadata, never embedding arrays.
    async fn explore(&self, request: &ExplorerRequest) -> Result<SearchPage<Value>, BackendError>;

    /// Batch homogeneous explorer reads in one storage round trip (at most 32).
    async fn explore_batch(
        &self,
        requests: &[ExplorerRequest],
    ) -> Result<Vec<(Uuid, SearchPage<Value>)>, BackendError> {
        let _ = requests;
        Err(BackendError::NotConfigured(
            "Batched explorer reads are unsupported".into(),
        ))
    }

    /// Scoped Saga state, listing and membership reads for interactive clients.
    /// Adapters without Saga storage fail explicitly rather than returning nothing.
    async fn read_saga(
        &self,
        org: &str,
        request: &crate::saga::SagaRead,
    ) -> Result<crate::saga::SagaReadResult, BackendError> {
        request.validate(org)?;
        Err(BackendError::NotConfigured(
            "Saga reads are unsupported".into(),
        ))
    }
}
