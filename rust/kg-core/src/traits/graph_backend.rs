//! Scoped graph reads, atomic commits, and adapter lifecycle.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::errors::BackendError;

/// A model-specific vector stored on an entity or relationship version.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphEmbedding {
    /// Model identifier; different models are never compared.
    pub model: String,
    /// Finite vector values with at least one nonzero component.
    pub values: Vec<f32>,
}

impl GraphEmbedding {
    /// Reject empty model identifiers, empty/zero vectors, and non-finite values.
    pub fn validate(&self) -> Result<(), BackendError> {
        validate_embedding(&self.model, &self.values)
    }
}

/// Validate a model-specific embedding before storage or similarity search.
pub fn validate_embedding(model: &str, values: &[f32]) -> Result<(), BackendError> {
    if model.trim().is_empty()
        || values.is_empty()
        || values.iter().any(|v| !v.is_finite())
        || values.iter().all(|v| *v == 0.0)
    {
        return Err(BackendError::Query(
            "embedding requires a model and finite, nonzero values".into(),
        ));
    }
    Ok(())
}

/// An entity version matched by similarity of its graph-stored embedding.
#[derive(Debug, Clone)]
pub struct EntityEmbeddingHit {
    pub uuid: uuid::Uuid,
    pub chain_id: uuid::Uuid,
    /// Cosine similarity in [-1, 1].
    pub score: f32,
    pub payload: Option<serde_json::Value>,
}

/// Graph operations. Every read and write is organization scoped; query text
/// and row decoding stay inside adapters. See [`super::SearchBackend`] for search reads.
#[async_trait]
pub trait GraphBackend: super::search_backend::SearchBackend + Send + Sync + 'static {
    fn supports_paged_commits(&self) -> bool {
        false
    }
    /// Resume a previously frozen plan before re-running extraction or resolution.
    async fn resume_commit(
        &self,
        _org: &str,
        _batch: super::BatchIdentity,
        _fingerprint: &super::RequestFingerprint,
        _cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<Option<super::CommittedBatch>, BackendError> {
        Ok(None)
    }

    /// Cancellation is checked between durable pages, never by dropping an in-flight commit.
    async fn commit_batch_cancellable(
        &self,
        batch: &super::MutationBatch,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<super::CommittedBatch, BackendError> {
        if cancel.is_cancelled() {
            return Err(BackendError::Other(
                "commit cancelled before dispatch".into(),
            ));
        }
        self.commit_batch(batch).await
    }

    /// Read one learned-rule revision for lifecycle repair fencing.
    async fn learned_rule(
        &self,
        _org: &str,
        _id: uuid::Uuid,
    ) -> Result<Option<super::LearnedRule>, BackendError> {
        Err(BackendError::NotConfigured(
            "learned-rule reads are unsupported".into(),
        ))
    }

    async fn active_learned_rules(
        &self,
        _org: &str,
    ) -> Result<Vec<super::LearnedRule>, BackendError> {
        Ok(Vec::new())
    }
    async fn read_community(
        &self,
        org: &str,
        request: &crate::community::CommunityRead,
    ) -> Result<crate::community::CommunityReadResult, BackendError> {
        request.validate(org)?;
        Err(BackendError::NotConfigured(
            "Community reads are unsupported".into(),
        ))
    }
    /// Scoped Saga state and bounded membership reads.
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
    /// Scoped identity candidates, filtered and deduplicated before the page limit.
    /// Records and scores must describe the same read; omitted matches set truncated.
    async fn identity_candidates(
        &self,
        org: &str,
        request: &super::IdentityCandidateRequest,
    ) -> Result<super::IdentityCandidatePage, BackendError> {
        let _ = (org, request);
        Err(BackendError::NotConfigured(
            "identity candidate reads are unsupported".into(),
        ))
    }

    /// Unresolved reference applications waiting on any of `tokens`: the
    /// sources to refresh when a target carrying such a token appears or changes.
    async fn unresolved_references(
        &self,
        org: &str,
        request: &super::UnresolvedReferenceQuery,
    ) -> Result<super::UnresolvedReferencePage, BackendError> {
        let _ = (org, request);
        Err(BackendError::NotConfigured(
            "unresolved reference reads are unsupported".into(),
        ))
    }

    /// The newest original (non-reused) persisted decision per reuse
    /// fingerprint, for cross-run reuse. Missing keys are simply absent.
    async fn reference_decisions_by_reuse_key(
        &self,
        org: &str,
        keys: &[String],
    ) -> Result<Vec<crate::runtime::reference_resolution::PersistedDecision>, BackendError> {
        let _ = (org, keys);
        Err(BackendError::NotConfigured(
            "persisted reference decisions are unsupported".into(),
        ))
    }

    /// Persisted model decisions of one producer source whose source version
    /// is still the live latest, newest per (source, slot, value): the labelled
    /// cases rule learning validates against.
    async fn reference_decision_labels(
        &self,
        org: &str,
        producer_source: &str,
        limit: usize,
    ) -> Result<Vec<crate::runtime::reference_resolution::PersistedDecision>, BackendError> {
        let _ = (org, producer_source, limit);
        Err(BackendError::NotConfigured(
            "persisted reference decisions are unsupported".into(),
        ))
    }

    /// Confirmed reference slots whose decision used any requested typed token.
    /// Target changes use this reverse dependency together with unresolved
    /// records so an arriving competitor revalidates an earlier unique match.
    async fn confirmed_reference_dependencies(
        &self,
        org: &str,
        request: &super::UnresolvedReferenceQuery,
    ) -> Result<super::UnresolvedReferencePage, BackendError> {
        let _ = (org, request);
        Ok(super::UnresolvedReferencePage::default())
    }

    /// Live sources covered by a learned rule. Lifecycle maintenance pages
    /// these records through the normal reference and commit path.
    async fn reference_rule_sources(
        &self,
        org: &str,
        request: &super::ReferenceRuleSourceQuery,
    ) -> Result<super::ReferenceRuleSourcePage, BackendError> {
        let _ = (org, request);
        Err(BackendError::NotConfigured(
            "reference-rule source reads are unsupported".into(),
        ))
    }

    /// Read identity revisions without creating records. An absent scope has revision zero.
    async fn identity_revisions(
        &self,
        org: &str,
        scopes: &[super::IdentityScope],
    ) -> Result<Vec<super::IdentityRevision>, BackendError> {
        let _ = (org, scopes);
        Err(BackendError::NotConfigured(
            "identity revision reads are unsupported".into(),
        ))
    }

    /// Apply ordered mutations atomically within one organization without a
    /// receipt or preconditions. Missing or foreign mandatory references fail
    /// the whole batch. An absent merge loser is allowed. For seeding test and
    /// administrative data; ingestion commits use [`Self::commit_batch`].
    async fn apply_mutations(
        &self,
        org_id: &str,
        mutations: &[super::GraphMutation],
    ) -> Result<(), BackendError>;

    /// Read a scoped registration without consulting mutable processing configuration.
    async fn read_run(
        &self,
        org: &str,
        run_id: uuid::Uuid,
    ) -> Result<Option<super::RunHeader>, BackendError> {
        let _ = (org, run_id);
        Err(BackendError::NotConfigured(
            "run schema recovery is unsupported".into(),
        ))
    }

    /// Persist the run header before any batch commit. Registering the same
    /// run id with the same fingerprint returns the batches already committed;
    /// a different fingerprint is a [`BackendError::Conflict`].
    async fn register_run(
        &self,
        header: &super::RunHeader,
    ) -> Result<super::RunRegistration, BackendError>;

    /// Commit one logical batch: verify the run header, check the receipt,
    /// evaluate preconditions after locking the checked records, apply mutations,
    /// and publish the receipt only after all required work commits. Batches within
    /// the atomic budgets use one transaction. Adapters advertising paged commits
    /// may persist bounded atomic components across transactions; interrupted pages
    /// can be visible, but never have a completed parent receipt until finished.
    ///
    /// A batch whose receipt already exists with the same fingerprint returns
    /// the stored result with `replayed = true` and writes nothing. A failed
    /// precondition, a stale plan, or a reused identity with different content
    /// is a [`BackendError::Conflict`]. A rejected collection generation/owner is
    /// [`BackendError::CollectionOwnershipConflict`]; graph replanning cannot fix it.
    /// When the commit acknowledgement is lost
    /// the adapter reads the receipt before reporting; it returns the committed
    /// receipt when found. An absent or unreadable receipt after a lost
    /// acknowledgement is [`BackendError::UnknownCommit`], not permission to retry.
    async fn commit_batch(
        &self,
        batch: &super::MutationBatch,
    ) -> Result<super::CommittedBatch, BackendError>;

    /// Receipts committed for a run, ordered by batch kind then index.
    async fn committed_batches(
        &self,
        org_id: &str,
        run_id: uuid::Uuid,
    ) -> Result<Vec<super::CommittedBatch>, BackendError>;

    /// Typed entity reads for identity matching, candidates, reference targets,
    /// version history, and reconciliation. An empty lookup returns no records.
    async fn find_entities(
        &self,
        org_id: &str,
        lookup: &super::EntityLookup,
    ) -> Result<Vec<super::EntityVersionRecord>, BackendError>;

    /// Reference candidates by exact typed key-value token, bounded per value and
    /// reporting which values overflowed instead of silently truncating. The
    /// default is one `LiveByKeyValue` read; adapters may override with an index.
    async fn find_reference_candidates(
        &self,
        org_id: &str,
        wanted: Vec<super::graph_reads::WantedKeyValue>,
    ) -> Result<super::graph_reads::ReferenceCandidates, BackendError> {
        let records = self
            .find_entities(
                org_id,
                &super::EntityLookup::LiveByKeyValue {
                    wanted: wanted.clone(),
                },
            )
            .await?;
        Ok(super::graph_reads::ReferenceCandidates::bounded(
            &wanted, records,
        ))
    }

    /// Exact scoped source evidence; unavailable records must not be silently dropped.
    async fn snapshot_evidence(
        &self,
        _org: &str,
        _request: &crate::runtime::history::SnapshotEvidenceRequest,
    ) -> Result<Vec<crate::runtime::history::SnapshotEvidence>, BackendError> {
        Err(BackendError::NotConfigured(
            "snapshot context reads are unsupported".into(),
        ))
    }

    /// Typed relationship reads for edge resolution and reconciliation.
    async fn find_edges(
        &self,
        org_id: &str,
        lookup: &super::EdgeLookup,
    ) -> Result<Vec<super::EdgeRecord>, BackendError>;

    /// Bounded UUID-ordered maintenance scan over current and historical records.
    async fn embedding_records(
        &self,
        _org: &str,
        _kind: crate::embedding_rebuild::EmbeddingKind,
        _after: Option<uuid::Uuid>,
        _limit: usize,
    ) -> Result<Vec<crate::embedding_rebuild::EmbeddingRecord>, BackendError> {
        Err(BackendError::Unavailable(
            "embedding maintenance is unsupported".into(),
        ))
    }
    /// Compare read properties and replace vectors atomically; return the number applied.
    async fn refresh_embeddings(
        &self,
        _org: &str,
        _kind: crate::embedding_rebuild::EmbeddingKind,
        _updates: &[crate::embedding_rebuild::EmbeddingRefresh],
    ) -> Result<usize, BackendError> {
        Err(BackendError::Unavailable(
            "embedding maintenance is unsupported".into(),
        ))
    }
    /// Count missing/incompatible vectors across both kinds, including history.
    async fn incompatible_embeddings(
        &self,
        _org: &str,
        _settings: &crate::embedding::EmbeddingSettings,
    ) -> Result<usize, BackendError> {
        Err(BackendError::Unavailable(
            "embedding maintenance is unsupported".into(),
        ))
    }

    /// Store an embedding on an existing, live entity version. Never creates nodes.
    /// Missing, deleted, superseded, or foreign-organization versions fail explicitly.
    async fn set_entity_embedding(
        &self,
        _org_id: &str,
        _uuid: uuid::Uuid,
        _embedding: &GraphEmbedding,
        _text_version: &str,
        _content_hash: &str,
        _fields: &crate::embedding::EntityEmbeddingFields,
    ) -> Result<(), BackendError> {
        Err(BackendError::Unavailable(
            "entity embedding writes are unsupported by this graph adapter".into(),
        ))
    }

    /// Read an embedding from a live entity version, scoped to its organization.
    async fn get_entity_embedding(
        &self,
        _org_id: &str,
        _uuid: uuid::Uuid,
    ) -> Result<Option<GraphEmbedding>, BackendError> {
        Err(BackendError::Unavailable(
            "entity embedding reads are unsupported by this graph adapter".into(),
        ))
    }

    /// Search live entity versions with matching model, dimensions, and text policy.
    /// Namespace/type filters apply before ranking. Empty filters mean no restriction.
    #[allow(clippy::too_many_arguments)]
    async fn search_entity_embeddings(
        &self,
        _query: &[f32],
        _model: &str,
        _org_id: &str,
        _namespaces: Option<&[&str]>,
        _entity_types: Option<&[&str]>,
        _limit: usize,
        _min_score: f32,
        _text_version: &str,
    ) -> Result<Vec<EntityEmbeddingHit>, BackendError> {
        Err(BackendError::Unavailable(
            "entity similarity search is unsupported by this graph adapter".into(),
        ))
    }

    /// Check backend connectivity; schema and embedding readiness are separate.
    async fn health(&self) -> Result<(), BackendError>;
    /// Establish the connection (idempotent).
    async fn connect(&self) -> Result<(), BackendError>;
    /// Stop accepting search reads and drain their cleanup in the Neo4j adapter.
    /// Callers stop ingestion first; shared handles retain connection resources.
    async fn close(&self) -> Result<(), BackendError>;
}
