//! Organization-scoped graph writes, independent of query language.

use chrono::{DateTime, Utc};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::errors::BackendError;
use crate::models::{CollectionMembership, CollectionRef};
use crate::traits::graph_backend::GraphEmbedding;

/// Prefix isolates source properties from graph identity and bookkeeping.
pub const USER_PROPERTY_PREFIX: &str = "prop_";
/// Prefix for entity and snapshot tags, kept apart from source properties.
pub const TAG_PROPERTY_PREFIX: &str = "tag_";

pub type GraphProperties = Map<String, Value>;

/// Canonical semantic state used to fence a vector against a changed entity version.
/// Nulls are omitted because Neo4j stores them as absent properties.
pub fn entity_embedding_state(properties: &GraphProperties) -> GraphProperties {
    properties
        .iter()
        .filter(|(key, value)| {
            !value.is_null()
                && (matches!(
                    key.as_str(),
                    "name"
                        | "entity_type"
                        | "namespace"
                        | "version"
                        | "summary"
                        | "structural_hash"
                ) || key.starts_with(USER_PROPERTY_PREFIX)
                    || key.starts_with(crate::traits::property_codec::TYPE_PREFIX))
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

/// Validate the complete content projection used to guard an entity vector write.
pub fn validate_entity_embedding_state(properties: &GraphProperties) -> Result<(), BackendError> {
    let invalid = || BackendError::Query("invalid entity embedding content baseline".into());
    if entity_embedding_state(properties) != *properties
        || properties.values().any(Value::is_object)
        || ["name", "entity_type", "namespace"].iter().any(|key| {
            properties
                .get(*key)
                .and_then(Value::as_str)
                .is_none_or(|value| value.trim().is_empty())
        })
        || properties
            .get("version")
            .and_then(Value::as_u64)
            .is_none_or(|version| version == 0 || version > u32::MAX as u64)
        || ["summary", "structural_hash"]
            .iter()
            .any(|key| properties.get(*key).is_some_and(|value| !value.is_string()))
    {
        return Err(invalid());
    }
    crate::traits::property_codec::read_properties(properties).map_err(|_| invalid())?;
    if serde_json::to_vec(properties).map_err(|_| invalid())?.len() > 8 * 1024 * 1024 {
        return Err(invalid());
    }
    Ok(())
}

/// Entity and snapshot nodes share a UUID namespace. Domain relationships and
/// provenance relationships each have their own UUID namespace.
///
/// Operations execute in order within one atomic batch. Receipt replay returns
/// the stored result; it does not reapply old mutations over newer state.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum GraphMutation {
    AssertCommunityState {
        state: Box<crate::community::CommunityState>,
    },
    BeginCommunityGeneration {
        generation: Box<crate::community::BeginCommunityGeneration>,
    },
    StageCommunityPartition {
        partition: Box<crate::community::StageCommunityPartition>,
    },
    PublishCommunityGeneration {
        publication: Box<crate::community::PublishCommunityGeneration>,
    },
    UpdateCommunities {
        update: Box<crate::community::UpdateCommunities>,
    },
    AssociateSagaSnapshot {
        association: Box<crate::saga::ThreadAssociationWrite>,
    },
    SetSagaSummary {
        summary: Box<crate::saga::SagaSummaryWrite>,
    },
    SetDerivedSummary {
        guard: crate::entity_summary::SummaryEvidenceGuard,
        summary: Box<crate::entity_summary::DerivedSummary>,
        embedding: GraphEmbedding,
    },
    ClearDerivedSummary {
        guard: crate::entity_summary::SummaryEvidenceGuard,
    },
    /// Create or update a version. Storage checks restoration against the
    /// chain's latest prior version: a tombstone requires a distinct UUID,
    /// its exact `previous_version_uuid`, and a strictly newer `valid_from`.
    /// An existing tombstone cannot be cleared or made latest in place.
    /// These history-dependent checks supplement structural `validate` checks.
    UpsertEntity {
        uuid: Uuid,
        properties: GraphProperties,
    },
    UpsertSnapshot {
        uuid: Uuid,
        properties: GraphProperties,
    },
    SupersedeEntity {
        uuid: Uuid,
        chain_id: Uuid,
        valid_to: DateTime<Utc>,
    },
    /// Replace the unresolved applications recorded for one source slot. An
    /// empty list clears them. Never an edge: a durable dependency record so that
    /// a target appearing later can refresh the sources waiting on its token.
    RecordUnresolvedReferences {
        source_chain_id: Uuid,
        slot: String,
        decided_at: DateTime<Utc>,
        decision_id: Uuid,
        entries: Vec<super::UnresolvedReferenceEntry>,
    },
    /// Persist original model decisions on ambiguous references (hashes and
    /// identifiers only) and note reuses of earlier ones. Compiled as packed
    /// statements of at most `MAX_DECISIONS_PER_STATEMENT` rows each;
    /// idempotent under receipt replay.
    RecordReferenceDecisions {
        decisions: Vec<crate::runtime::reference_resolution::PersistedDecision>,
        reuses: Vec<crate::runtime::reference_resolution::DecisionReuse>,
    },
    /// Patch content or metadata. Storage preserves `deleted_at` and permits
    /// `is_latest` only to remain unchanged or change from true to false;
    /// restoration requires a new version through `UpsertEntity`.
    UpdateEntity {
        uuid: Uuid,
        properties: GraphProperties,
    },
    /// Apply classification metadata after creating or observing a live version.
    /// Partial input inherits the actual predecessor, then overlays supplied tags
    /// and unions labels. Full input replaces both. Equal-time conflicts fail.
    ApplyEntityMetadata {
        /// Frozen profile/effective-schema digest; absent for legacy observations.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        profile_contract: Option<String>,
        reference_exclusions: Vec<String>,
        uuid: Uuid,
        previous_uuid: Option<Uuid>,
        tags: indexmap::IndexMap<String, String>,
        labels: Vec<String>,
        replace: bool,
        observed_at: DateTime<Utc>,
    },
    /// Record source absence at the supplied capture time. The first deletion
    /// ends the entity's effective lifetime; a newer confirmed absence keeps
    /// that original `deleted_at` and advances the transition/freshness clock.
    /// Restoring the chain must be strictly newer than that known absence.
    DeleteEntity {
        chain_id: Uuid,
        deleted_at: DateTime<Utc>,
        deleted_by: Option<String>,
        reason: Option<String>,
    },
    /// Record a re-observation on the chain's live version: observation
    /// time, generation, snapshot, and the observing collection's membership
    /// (added, or its generation replaced).
    ObserveEntity {
        chain_id: Uuid,
        observed_at: DateTime<Utc>,
        sync_generation: Option<u64>,
        snapshot_id: Option<Uuid>,
        collection: Option<CollectionMembership>,
    },
    UpsertEdge {
        uuid: Uuid,
        source_chain_id: Uuid,
        target_chain_id: Uuid,
        properties: GraphProperties,
    },
    /// Cancel an interval before activation without rewriting its original validity bounds.
    CancelEdge {
        uuid: Uuid,
        cancelled_at: DateTime<Utc>,
        cancellation_snapshot_id: Option<Uuid>,
        cancellation_context: Option<crate::models::CancellationContext>,
        observed_at: DateTime<Utc>,
    },
    UpdateEdge {
        uuid: Uuid,
        properties: GraphProperties,
    },
    /// Move domain edges and copy aliases; provenance stays on the observed version.
    RepointEntity {
        previous_uuid: Uuid,
        new_uuid: Uuid,
        chain_id: Uuid,
    },
    /// Hide the loser from `effective_at` and close its incident facts. Fuzzy
    /// adoption can name a loser that has never been materialized. Nested active
    /// merges and transitions older than stored evidence are rejected.
    MergeChains {
        effective_at: DateTime<Utc>,
        loser_chain_id: Uuid,
        winner_chain_id: Uuid,
        identity_hashes: Vec<String>,
    },
    /// End the merge interval without reviving closed facts or deleted versions.
    SplitChain {
        effective_at: DateTime<Utc>,
        split_chain_id: Uuid,
        from_chain_id: Uuid,
        identity_hashes: Vec<String>,
    },
    /// A `MENTIONS` link to the exact observed entity version, retained after
    /// supersession or deletion. The chain ID guards against a mismatched endpoint.
    RecordObservation {
        uuid: Uuid,
        snapshot_uuid: Uuid,
        entity_uuid: Uuid,
        entity_chain_id: Uuid,
        observed_at: DateTime<Utc>,
        /// Accepted attribute adjudications made for this observation,
        /// stored on the link as auditable provenance.
        reconciliations: Vec<crate::runtime::stage_output::AttributeReconciliation>,
    },
    /// Remove one collection's membership from a live entity version that
    /// another collection still owns. Only that entry changes; concurrent
    /// membership writes for other collections are preserved.
    ReleaseMembership {
        uuid: Uuid,
        collection: CollectionRef,
    },
    /// Store an embedding on a live entity version in the same transaction as
    /// the version itself, with the text representation version and content
    /// hash it was computed from. Missing, deleted, or superseded versions
    /// fail the batch.
    SetEmbedding {
        uuid: Uuid,
        embedding: GraphEmbedding,
        text_version: String,
        content_hash: String,
    },
    /// Store a vector on an exact version, including history, only while its semantic state agrees.
    SetEntityVersionEmbedding {
        uuid: Uuid,
        expected_properties: GraphProperties,
        embedding: GraphEmbedding,
        text_version: String,
        content_hash: String,
    },
    /// Store a vector on a live relationship version in the same transaction.
    SetRelationshipEmbedding {
        uuid: Uuid,
        embedding: GraphEmbedding,
        text_version: String,
        content_hash: String,
    },
}

/// Leading independent writes that adapters may execute in one bulk statement.
/// Admission and compilation share this rule so safe bulk writes are not counted
/// as individual statements. Repeated keys break the group to preserve ordering.
pub fn mutation_group_length(mutations: &[GraphMutation]) -> usize {
    let Some(first) = mutations.first() else {
        return 0;
    };
    let Some(first_key) = group_key(first) else {
        return 1;
    };
    let mut keys = std::collections::HashSet::new();
    keys.insert(first_key);
    let mut length = 1;
    for mutation in &mutations[1..] {
        if matches!(first, GraphMutation::RecordUnresolvedReferences { .. })
            && length == super::graph_commit::MAX_UNRESOLVED_SLOTS_PER_STATEMENT
        {
            break;
        }
        match group_key(mutation) {
            Some(key)
                if std::mem::discriminant(mutation) == std::mem::discriminant(first)
                    && keys.insert(key) =>
            {
                length += 1
            }
            _ => break,
        }
    }
    length
}

fn group_key(mutation: &GraphMutation) -> Option<(Uuid, Option<&str>)> {
    use GraphMutation::*;
    match mutation {
        UpsertEntity { uuid, .. }
        | UpsertSnapshot { uuid, .. }
        | RecordObservation { uuid, .. }
        | SetEmbedding { uuid, .. }
        | SetEntityVersionEmbedding { uuid, .. } => Some((*uuid, None)),
        ApplyEntityMetadata {
            uuid,
            previous_uuid: None,
            ..
        } => Some((*uuid, None)),
        ObserveEntity { chain_id, .. } => Some((*chain_id, None)),
        RecordUnresolvedReferences {
            source_chain_id,
            slot,
            ..
        } => Some((*source_chain_id, Some(slot))),
        _ => None,
    }
}

impl GraphMutation {
    /// Property maps cannot change identifiers or organization ownership.
    pub fn validate(&self, org_id: &str) -> Result<(), BackendError> {
        if org_id.trim().is_empty() {
            return Err(BackendError::Query(
                "graph mutation requires an organization".into(),
            ));
        }
        let ids: &[Uuid] = match self {
            Self::UpsertEntity { uuid, .. }
            | Self::UpsertSnapshot { uuid, .. }
            | Self::UpdateEntity { uuid, .. }
            | Self::UpdateEdge { uuid, .. }
            | Self::CancelEdge { uuid, .. }
            | Self::ApplyEntityMetadata { uuid, .. }
            | Self::ReleaseMembership { uuid, .. }
            | Self::SetEmbedding { uuid, .. }
            | Self::SetEntityVersionEmbedding { uuid, .. }
            | Self::SetRelationshipEmbedding { uuid, .. } => std::slice::from_ref(uuid),
            Self::DeleteEntity { chain_id, .. }
            | Self::RecordUnresolvedReferences {
                source_chain_id: chain_id,
                ..
            } => std::slice::from_ref(chain_id),
            Self::ObserveEntity {
                chain_id,
                snapshot_id,
                ..
            } => {
                if snapshot_id.is_some_and(|id| id.is_nil()) {
                    return Err(BackendError::Query(
                        "observation requires a valid snapshot identifier".into(),
                    ));
                }
                std::slice::from_ref(chain_id)
            }
            Self::SupersedeEntity { uuid, chain_id, .. } => &[*uuid, *chain_id],
            Self::UpsertEdge {
                uuid,
                source_chain_id,
                target_chain_id,
                ..
            } => &[*uuid, *source_chain_id, *target_chain_id],
            Self::RepointEntity {
                previous_uuid,
                new_uuid,
                chain_id,
            } => &[*previous_uuid, *new_uuid, *chain_id],
            Self::MergeChains {
                loser_chain_id,
                winner_chain_id,
                ..
            } => &[*loser_chain_id, *winner_chain_id],
            Self::SplitChain {
                split_chain_id,
                from_chain_id,
                ..
            } => &[*split_chain_id, *from_chain_id],
            Self::RecordObservation {
                uuid,
                snapshot_uuid,
                entity_uuid,
                entity_chain_id,
                ..
            } => &[*uuid, *snapshot_uuid, *entity_uuid, *entity_chain_id],
            // These contracts validate their own nested identities below.
            Self::AssertCommunityState { .. }
            | Self::BeginCommunityGeneration { .. }
            | Self::StageCommunityPartition { .. }
            | Self::PublishCommunityGeneration { .. }
            | Self::UpdateCommunities { .. }
            | Self::AssociateSagaSnapshot { .. }
            | Self::SetSagaSummary { .. }
            | Self::SetDerivedSummary { .. }
            | Self::ClearDerivedSummary { .. } => &[],
            Self::RecordReferenceDecisions { decisions, reuses } => {
                use crate::runtime::reference_resolution::MAX_DECISIONS_PER_STATEMENT;
                if decisions.is_empty() && reuses.is_empty() {
                    return Err(BackendError::Query(
                        "reference decision record without content".into(),
                    ));
                }
                if decisions.len() > MAX_DECISIONS_PER_STATEMENT
                    || reuses.len() > MAX_DECISIONS_PER_STATEMENT
                {
                    return Err(BackendError::Query(
                        "reference decision record exceeds one statement".into(),
                    ));
                }
                for decision in decisions {
                    decision.validate(org_id)?;
                }
                if reuses.iter().any(|reuse| {
                    reuse.original.is_nil()
                        || reuse.decision_id.is_nil()
                        || reuse.original == reuse.decision_id
                }) {
                    return Err(BackendError::Query(
                        "reference decision reuse requires distinct identifiers".into(),
                    ));
                }
                &[]
            }
        };
        if ids.iter().any(Uuid::is_nil) {
            return Err(BackendError::Query(
                "graph mutation requires nonnil identifiers".into(),
            ));
        }
        let (uuid, properties, upsert) = match self {
            Self::UpsertEntity { uuid, properties }
            | Self::UpsertSnapshot { uuid, properties }
            | Self::UpsertEdge {
                uuid, properties, ..
            } => (uuid, properties, true),
            Self::UpdateEntity { uuid, properties } | Self::UpdateEdge { uuid, properties } => {
                (uuid, properties, false)
            }
            Self::CancelEdge {
                uuid,
                cancellation_snapshot_id,
                cancellation_context,
                cancelled_at,
                ..
            } => {
                if uuid.is_nil()
                    || cancellation_snapshot_id.is_some_and(|id| id.is_nil())
                    || (cancellation_snapshot_id.is_some() == cancellation_context.is_some())
                    || cancellation_context.as_ref().is_some_and(|context| context.validate().is_err()
                        || matches!(context, crate::models::CancellationContext::Merge { effective_at, .. } if effective_at != cancelled_at))
                {
                    return Err(BackendError::Query(
                        "cancellation requires valid identifiers".into(),
                    ));
                }
                return Ok(());
            }
            Self::RecordUnresolvedReferences {
                slot,
                decision_id,
                entries,
                ..
            } => {
                if slot.trim().is_empty()
                    || slot.len() > 512
                    || decision_id.is_nil()
                    || entries.len() > 256
                {
                    return Err(BackendError::Query(
                        "invalid unresolved reference slot".into(),
                    ));
                }
                for entry in entries {
                    entry.validate()?;
                }
                return Ok(());
            }
            Self::RepointEntity {
                previous_uuid,
                new_uuid,
                ..
            } if previous_uuid == new_uuid => {
                return Err(BackendError::Query(
                    "successor must differ from predecessor".into(),
                ));
            }
            Self::MergeChains {
                loser_chain_id,
                winner_chain_id,
                ..
            } if loser_chain_id == winner_chain_id => {
                return Err(BackendError::Query("merged chains must differ".into()));
            }
            Self::SplitChain {
                split_chain_id,
                from_chain_id,
                ..
            } if split_chain_id == from_chain_id => {
                return Err(BackendError::Query("split chains must differ".into()));
            }
            Self::AssertCommunityState { state } => return state.validate(org_id),
            Self::BeginCommunityGeneration { generation } => return generation.validate(org_id),
            Self::StageCommunityPartition { partition } => return partition.validate(org_id),
            Self::PublishCommunityGeneration { publication } => {
                return publication.validate(org_id);
            }
            Self::UpdateCommunities { update } => return update.validate(org_id),
            Self::AssociateSagaSnapshot { association } => return association.validate(org_id),
            Self::SetSagaSummary { summary } => return summary.validate(org_id),
            Self::SetDerivedSummary {
                guard,
                summary,
                embedding,
            } => {
                guard.validate()?;
                summary.validate()?;
                if summary.total_evidence > guard.incident_versions.len()
                    || summary.evidence_ids.iter().any(|id| {
                        !guard.incident_versions.iter().any(|version| {
                            version.properties.get("uuid").and_then(Value::as_str)
                                == Some(id.to_string().as_str())
                        })
                    })
                {
                    return Err(BackendError::Query(
                        "summary cites evidence outside its complete guard".into(),
                    ));
                }
                let boundary = crate::entity_summary::coverage_end(guard, summary.as_of)?;
                if boundary.is_some_and(|end| summary.valid_until.is_none_or(|until| until > end)) {
                    return Err(BackendError::Query(
                        "summary coverage exceeds evidence boundary".into(),
                    ));
                }
                if guard.entity_versions.values().flatten().any(|properties| {
                    properties
                        .get("org_id")
                        .and_then(Value::as_str)
                        .is_some_and(|scope| scope != org_id)
                }) {
                    return Err(BackendError::Query(
                        "summary evidence crosses organization".into(),
                    ));
                }
                return embedding.validate();
            }
            Self::ClearDerivedSummary { guard } => {
                guard.validate()?;
                if guard.entity_versions.values().flatten().any(|properties| {
                    properties
                        .get("org_id")
                        .and_then(Value::as_str)
                        .is_some_and(|scope| scope != org_id)
                }) {
                    return Err(BackendError::Query(
                        "summary evidence crosses organization".into(),
                    ));
                }
                return Ok(());
            }
            Self::SetEntityVersionEmbedding {
                uuid,
                expected_properties,
                embedding,
                text_version,
                content_hash,
            } => {
                if uuid.is_nil() || text_version.trim().is_empty() || content_hash.trim().is_empty()
                {
                    return Err(BackendError::Query(
                        "embedding requires valid version and text metadata".into(),
                    ));
                }
                crate::embedding::validate_entity_text_version(text_version)?;
                validate_entity_embedding_state(expected_properties)?;
                return embedding.validate();
            }
            Self::SetEmbedding {
                embedding,
                text_version,
                content_hash,
                ..
            }
            | Self::SetRelationshipEmbedding {
                embedding,
                text_version,
                content_hash,
                ..
            } => {
                if text_version.trim().is_empty() || content_hash.trim().is_empty() {
                    return Err(BackendError::Query(
                        "embedding requires its text version and content hash".into(),
                    ));
                }
                if matches!(self, Self::SetEmbedding { .. }) {
                    crate::embedding::validate_entity_text_version(text_version)?;
                }
                return embedding.validate();
            }
            Self::ObserveEntity {
                sync_generation,
                collection,
                ..
            } => {
                if let Some(generation) = sync_generation {
                    validate_generation(*generation)?;
                }
                if let Some(membership) = collection {
                    validate_generation(membership.generation)?;
                    membership
                        .collection
                        .validate()
                        .map_err(BackendError::Query)?;
                }
                return Ok(());
            }
            Self::ApplyEntityMetadata {
                profile_contract,
                uuid,
                previous_uuid,
                tags,
                labels,
                ..
            } => {
                if profile_contract
                    .as_ref()
                    .is_some_and(|v| v.len() != 64 || !v.bytes().all(|b| b.is_ascii_hexdigit()))
                {
                    return Err(BackendError::Query(
                        "invalid profile contract digest".into(),
                    ));
                }
                if uuid.is_nil()
                    || previous_uuid.is_some_and(|previous| previous.is_nil() || previous == *uuid)
                {
                    return Err(BackendError::Query(
                        "metadata requires distinct valid version identifiers".into(),
                    ));
                }
                crate::models::validate_metadata(tags, labels).map_err(BackendError::Query)?;
                return Ok(());
            }
            Self::ReleaseMembership { collection, .. } => {
                return collection
                    .validate()
                    .map_err(|message| BackendError::Query(format!("release {message}")));
            }
            _ => return Ok(()),
        };
        super::property_codec::validate_patch(properties).map_err(BackendError::Query)?;
        for value in properties.values() {
            validate_property(value)?;
        }
        for (key, expected) in [("uuid", uuid.to_string()), ("org_id", org_id.to_owned())] {
            if let Some(value) = properties.get(key) {
                if !upsert || value.as_str() != Some(expected.as_str()) {
                    return Err(BackendError::Query(format!(
                        "protected graph property: {key}"
                    )));
                }
            }
        }
        if properties.keys().any(|key| {
            key.starts_with('_')
                || crate::entity_summary::DERIVED_PROPERTIES.contains(&key.as_str())
                || (key == "last_transition_at"
                    && !matches!(self, Self::UpsertEdge { .. } | Self::UpdateEdge { .. }))
        }) {
            return Err(BackendError::Query("reserved graph property".into()));
        }
        if let Some(value) = properties.get("last_transition_at") {
            if value
                .as_str()
                .and_then(|text| chrono::DateTime::parse_from_rfc3339(text).ok())
                .is_none()
            {
                return Err(BackendError::Query(
                    "relationship transition time must be an RFC3339 timestamp".into(),
                ));
            }
        }
        if !upsert
            && [
                "chain_id",
                "source_chain_id",
                "target_chain_id",
                "hash_version",
                "identity_hash",
            ]
            .iter()
            .any(|k| properties.contains_key(*k))
        {
            return Err(BackendError::Query("cannot update graph identity".into()));
        }
        if matches!(self, Self::UpdateEntity { .. })
            && ["namespace", "entity_type"]
                .iter()
                .any(|key| properties.contains_key(*key))
        {
            return Err(BackendError::Query(
                "cannot move an entity identity scope".into(),
            ));
        }
        if let Self::UpsertEntity { .. } = self {
            for key in ["namespace", "entity_type"] {
                if properties
                    .get(key)
                    .and_then(Value::as_str)
                    .is_none_or(|v| v.trim().is_empty())
                {
                    return Err(BackendError::Query(
                        "entity requires namespace and entity type".into(),
                    ));
                }
            }
            properties
                .get("chain_id")
                .and_then(Value::as_str)
                .and_then(|s| Uuid::parse_str(s).ok())
                .filter(|id| !id.is_nil())
                .ok_or_else(|| BackendError::Query("entity requires a chain UUID".into()))?;
        }
        if let Self::UpsertEdge {
            source_chain_id,
            target_chain_id,
            ..
        } = self
        {
            for (key, id) in [
                ("source_chain_id", source_chain_id),
                ("target_chain_id", target_chain_id),
            ] {
                if properties
                    .get(key)
                    .is_some_and(|v| v.as_str() != Some(id.to_string().as_str()))
                {
                    return Err(BackendError::Query(format!(
                        "protected graph property: {key}"
                    )));
                }
            }
        }
        Ok(())
    }
}

pub(crate) fn validate_generation(generation: u64) -> Result<(), BackendError> {
    if generation > i64::MAX as u64 {
        return Err(BackendError::Query(
            "generation exceeds the supported signed 64-bit range".into(),
        ));
    }
    Ok(())
}

// Graph properties support scalars and homogeneous scalar arrays. Reject values
// that Neo4j cannot store before they reach the adapter.
fn validate_property(value: &Value) -> Result<(), BackendError> {
    fn scalar_kind(value: &Value) -> Option<u8> {
        match value {
            Value::Bool(_) => Some(1),
            Value::String(_) => Some(2),
            Value::Number(n) if n.is_f64() || n.as_i64().is_some() => Some(3),
            _ => None,
        }
    }
    let valid = match value {
        Value::Null => true,
        Value::Array(values) => values.first().is_none_or(|first| {
            let kind = scalar_kind(first);
            kind.is_some() && values.iter().all(|value| scalar_kind(value) == kind)
        }),
        scalar => scalar_kind(scalar).is_some(),
    };
    if valid {
        Ok(())
    } else {
        Err(BackendError::Query(
            "graph property must be a scalar or homogeneous scalar array".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_accepts_one_durable_context_and_rejects_ambiguous_provenance() {
        use crate::{
            models::CancellationContext,
            traits::{BatchIdentity, BatchKind},
        };
        let at = "2026-01-02T00:00:00Z".parse().unwrap();
        let batch = CancellationContext::Batch {
            batch: BatchIdentity {
                run_id: Uuid::new_v4(),
                kind: BatchKind::Reconciliation,
                index: 0,
            },
        };
        let mutation = |snapshot, context| GraphMutation::CancelEdge {
            uuid: Uuid::new_v4(),
            cancellation_snapshot_id: snapshot,
            cancellation_context: context,
            cancelled_at: at,
            observed_at: at,
        };
        assert!(mutation(None, Some(batch.clone())).validate("org").is_ok());
        assert!(mutation(Some(Uuid::new_v4()), Some(batch))
            .validate("org")
            .is_err());
        assert!(mutation(None, None).validate("org").is_err());
        for effective_at in [at, at + chrono::Duration::seconds(1)] {
            let context = CancellationContext::Merge {
                loser_chain_id: Uuid::new_v4(),
                winner_chain_id: Uuid::new_v4(),
                effective_at,
            };
            assert_eq!(
                mutation(None, Some(context)).validate("org").is_ok(),
                effective_at == at
            );
        }
    }

    #[test]
    fn cancellation_requires_identifiers_but_does_not_order_source_and_capture_clocks() {
        let mutation = |uuid, cancellation_snapshot_id| GraphMutation::CancelEdge {
            uuid,
            cancellation_snapshot_id: Some(cancellation_snapshot_id),
            cancellation_context: None,
            cancelled_at: "2026-01-02T00:00:00Z".parse().unwrap(),
            observed_at: "2026-01-01T00:00:00Z".parse().unwrap(),
        };
        assert!(mutation(Uuid::new_v4(), Uuid::new_v4())
            .validate("org")
            .is_ok());
        assert!(mutation(Uuid::nil(), Uuid::new_v4())
            .validate("org")
            .is_err());
        assert!(mutation(Uuid::new_v4(), Uuid::nil())
            .validate("org")
            .is_err());
    }

    #[test]
    fn relationship_transition_clock_is_typed_and_entity_clock_stays_protected() {
        let uuid = Uuid::new_v4();
        let properties: GraphProperties = [(
            "last_transition_at".into(),
            serde_json::json!("2026-09-19T00:00:00Z"),
        )]
        .into_iter()
        .collect();
        assert!(GraphMutation::UpdateEdge {
            uuid,
            properties: properties.clone()
        }
        .validate("org")
        .is_ok());
        assert!(GraphMutation::UpdateEntity { uuid, properties }
            .validate("org")
            .is_err());
        for value in [
            serde_json::Value::Null,
            serde_json::json!(17),
            serde_json::json!("yesterday"),
        ] {
            let properties = [("last_transition_at".into(), value)].into_iter().collect();
            assert!(GraphMutation::UpdateEdge { uuid, properties }
                .validate("org")
                .is_err());
        }
    }

    // ---- merged from `mod historical_embedding_tests`

    use serde_json::json;

    #[test]
    fn historical_embedding_requires_a_complete_canonical_typed_projection() {
        let mut full =
            json!({"name":"api", "entity_type":"Service", "namespace":"prod", "version":2,
            "summary":null,"structural_hash":"hash", "prop_port":5432,"property_type_port":"i",
            "prop_missing":null,"property_type_missing":"n", "is_latest":false,"embedding":[0.1]})
            .as_object()
            .unwrap()
            .clone();
        let expected = entity_embedding_state(&full);
        assert!(!expected.contains_key("summary"));
        assert!(!expected.contains_key("is_latest"));
        assert!(!expected.contains_key("prop_missing"));
        assert_eq!(expected["property_type_missing"], "n");
        let mutation = |properties| GraphMutation::SetEntityVersionEmbedding {
            uuid: Uuid::new_v4(),
            expected_properties: properties,
            embedding: GraphEmbedding {
                model: "m".into(),
                values: vec![1.0],
            },
            text_version: crate::embedding::TEXT_VERSION.into(),
            content_hash: "h".into(),
        };
        assert!(mutation(expected.clone()).validate("org").is_ok());
        for key in [
            "name",
            "namespace",
            "entity_type",
            "version",
            "property_type_port",
        ] {
            let mut bad = expected.clone();
            bad.remove(key);
            assert!(mutation(bad).validate("org").is_err(), "{key}");
        }
        for (key, value) in [
            ("summary", Value::Null),
            ("prop_port", json!("5432")),
            ("is_latest", json!(false)),
            ("name", json!({"nested":true})),
            ("version", json!(0)),
        ] {
            let mut bad = expected.clone();
            bad.insert(key.into(), value);
            assert!(mutation(bad).validate("org").is_err(), "{key}");
        }
        full.insert("summary".into(), json!("new summary"));
        assert_ne!(entity_embedding_state(&full), expected);
    }
}
