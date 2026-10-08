//! Atomic commits and receipts. Replay returns the committed result without
//! repeating writes; run fingerprints prevent reuse with different input.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::errors::BackendError;
use crate::models::CollectionRef;
use crate::models::SnapshotInput;
use crate::traits::graph_mutation::GraphMutation;

/// Compiled mutation and precondition statements allowed in one transaction.
/// Adapter receipt, run, and revision-lock protocol statements are additional.
/// Core estimates work at admission; adapters enforce the compiled cap.
pub const MAX_STATEMENTS_PER_BATCH: usize = 5_000;
/// Maximum distinct unresolved slots compiled into one parameterized statement.
pub const MAX_UNRESOLVED_SLOTS_PER_STATEMENT: usize = 500;
/// Embedding values allowed in one transaction, in bytes of `f32`.
pub const MAX_EMBEDDING_BYTES_PER_BATCH: usize = 8 * 1024 * 1024;

/// Receipt categories for ingestion and configured follow-up work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchKind {
    Node,
    Relationship,
    Reconciliation,
    Summary,
    SagaSummary,
    Community,
}

impl BatchKind {
    /// Stable storage label.
    pub fn label(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Relationship => "relationship",
            Self::Reconciliation => "reconciliation",
            Self::Summary => "summary",
            Self::SagaSummary => "saga_summary",
            Self::Community => "community",
        }
    }

    /// Parse a stored label.
    pub fn parse(label: &str) -> Option<Self> {
        match label {
            "node" => Some(Self::Node),
            "relationship" => Some(Self::Relationship),
            "reconciliation" => Some(Self::Reconciliation),
            "summary" => Some(Self::Summary),
            "saga_summary" => Some(Self::SagaSummary),
            "community" => Some(Self::Community),
            _ => None,
        }
    }
}

/// Deterministic batch identity within a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchIdentity {
    pub run_id: Uuid,
    pub kind: BatchKind,
    pub index: u32,
}

impl BatchIdentity {
    /// `UUIDv5(run_id, "batch:{kind}:{index}")`; the receipt key.
    pub fn batch_id(&self) -> Uuid {
        Uuid::new_v5(
            &self.run_id,
            format!("batch:{}:{}", self.kind.label(), self.index).as_bytes(),
        )
    }
}

/// xxh3-128 over the organization, the ordered canonical snapshot JSON, and the
/// effective processing settings. Same run id with a different fingerprint is a conflict.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RequestFingerprint(pub String);

impl RequestFingerprint {
    /// Compute the fingerprint. Object keys are sorted so serialization order
    /// never changes the result.
    pub fn compute(
        org_id: &str,
        snapshots: &[SnapshotInput],
        settings: &impl Serialize,
    ) -> Result<Self, BackendError> {
        Self::compute_value(org_id, &snapshots, settings)
    }

    pub fn compute_inputs(
        org_id: &str,
        inputs: &[crate::models::IngestionInput],
        settings: &impl Serialize,
    ) -> Result<Self, BackendError> {
        Self::compute_value(org_id, &inputs, settings)
    }

    fn compute_value(
        org_id: &str,
        snapshots: &impl Serialize,
        settings: &impl Serialize,
    ) -> Result<Self, BackendError> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(org_id.len() as u64).to_le_bytes());
        bytes.extend_from_slice(org_id.as_bytes());
        let snapshots = serde_json::to_value(snapshots)
            .map_err(|e| BackendError::Serialization(e.to_string()))?;
        canonical(&snapshots, &mut bytes);
        bytes.push(0);
        let settings = serde_json::to_value(settings)
            .map_err(|e| BackendError::Serialization(e.to_string()))?;
        canonical(&settings, &mut bytes);
        Ok(Self(format!(
            "{:032x}",
            xxhash_rust::xxh3::xxh3_128(&bytes)
        )))
    }

    /// Reject values that are not 32 lowercase hex characters.
    pub fn validate(&self) -> Result<(), BackendError> {
        if self.0.len() != 32
            || !self
                .0
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(BackendError::Query("invalid request fingerprint".into()));
        }
        Ok(())
    }
}

fn canonical(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            out.push(b'{');
            for (i, (key, value)) in entries.into_iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                out.extend_from_slice(serde_json::to_string(key).unwrap_or_default().as_bytes());
                out.push(b':');
                canonical(value, out);
            }
            out.push(b'}');
        }
        Value::Array(values) => {
            out.push(b'[');
            for (i, value) in values.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                canonical(value, out);
            }
            out.push(b']');
        }
        scalar => out.extend_from_slice(scalar.to_string().as_bytes()),
    }
}

/// One planned batch, persisted with the run header before processing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannedBatch {
    pub kind: BatchKind,
    pub index: u32,
    /// Input items the batch covers (snapshots, relationship pairs, or scopes).
    pub items: u32,
}

pub const MAX_RULE_FREEZE_SOURCES: usize = 256;

/// Run contract persisted before graph data writes; excludes source payloads and credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunHeader {
    pub observation_manifest: crate::runtime::saga::RunObservationManifest,
    pub schema_manifest: crate::runtime::schemas::RunSchemaManifest,
    #[serde(default)]
    pub rule_freezes: Vec<crate::runtime::rule_learning::materialize::RuleFreeze>,
    pub org_id: String,
    pub run_id: Uuid,
    pub fingerprint: RequestFingerprint,
    /// Version of the effective settings and text representation in use.
    pub settings_version: String,
    /// Capture time applied to snapshots that omit one, frozen at first attempt.
    pub capture_default: DateTime<Utc>,
    pub batch_plan: Vec<PlannedBatch>,
}

impl RunHeader {
    /// Validate run identity, unique receipt keys, and frozen manifests.
    pub fn validate(&self) -> Result<(), BackendError> {
        if self.org_id.trim().is_empty() {
            return Err(BackendError::Query(
                "run header requires an organization".into(),
            ));
        }
        if self.settings_version.trim().is_empty() {
            return Err(BackendError::Query(
                "run header requires a settings version".into(),
            ));
        }
        let mut batches = std::collections::HashSet::new();
        if self.run_id.is_nil()
            || self
                .batch_plan
                .iter()
                .any(|batch| !batches.insert((batch.kind, batch.index)))
        {
            return Err(BackendError::Query(
                "run requires a valid identifier and unique batch keys".into(),
            ));
        }
        self.schema_manifest
            .validate(&self.org_id)
            .map_err(BackendError::Query)?;
        if self.rule_freezes.len() > MAX_RULE_FREEZE_SOURCES
            || self.rule_freezes.iter().any(|freeze| {
                freeze.source.trim().is_empty()
                    || freeze.rules.len() != freeze.mappings.len()
                    || freeze
                        .rules
                        .iter()
                        .any(|(id, revision)| id.is_nil() || *revision == 0)
                    || freeze
                        .mappings
                        .iter()
                        .any(|mapping| mapping.validate().is_err())
            })
        {
            return Err(BackendError::Query("invalid frozen learned rules".into()));
        }
        self.observation_manifest
            .validate()
            .map_err(BackendError::Query)?;
        self.fingerprint.validate()
    }
}

/// Outcome of registering a run header.
#[derive(Debug, Clone, PartialEq)]
pub enum RunRegistration {
    /// First registration of this run id.
    Registered,
    /// Resume with the stored schemas and capture default, including when another
    /// caller won concurrent registration. Committed batches need no recomputation.
    Resumed {
        observation_manifest: crate::runtime::saga::RunObservationManifest,
        schema_manifest: crate::runtime::schemas::RunSchemaManifest,
        capture_default: DateTime<Utc>,
        committed: Vec<CommittedBatch>,
    },
}

/// Expected database state checked inside the commit transaction, after
/// locking the checked record where one exists. A failed precondition aborts
/// the batch with [`BackendError::Conflict`], or [`BackendError::IdentityRevisionChanged`]
/// when semantic evidence needs to be read again. Rejected collection ownership
/// returns [`BackendError::CollectionOwnershipConflict`] and cannot be replanned.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Precondition {
    /// A learned-rule lifecycle revision still has the status being repaired.
    RuleRevisionIs {
        id: Uuid,
        revision: u64,
        status: super::RuleStatus,
    },
    IncidentHistoryIs {
        chain_id: Uuid,
        versions: Vec<super::relationship_timeline::IncidentVersionState>,
    },

    /// Semantic evidence was read at this scoped revision.
    IdentityRevisionIs(super::IdentityRevision),

    /// The chain's only live latest version is `uuid` at `version`.
    LatestVersionIs {
        chain_id: Uuid,
        uuid: Uuid,
        version: u32,
    },
    /// No live version answers to any of these identity hashes.
    NoLiveVersionFor {
        hashes: Vec<String>,
    },
    /// The chain has no live latest version, its newest version is the
    /// tombstone `uuid`, and that tombstone was deleted before `restored_at`:
    /// a deletion or confirmed absence recorded at or after the restoring
    /// observation wins. This also fences a newer observation of absence.
    LatestDeletedVersionIs {
        chain_id: Uuid,
        uuid: Uuid,
        restored_at: DateTime<Utc>,
    },
    /// The live version `uuid` has no observation newer than `observed_at`.
    NotObservedAfter {
        uuid: Uuid,
        observed_at: DateTime<Utc>,
    },
    /// The latest historical head is unchanged and no newer capture was stored.
    EdgeHeadIs {
        observed_at: DateTime<Utc>,
        source_chain_id: Uuid,
        target_chain_id: Uuid,
        chain_id: Uuid,
        uuid: Uuid,
        version: u32,
    },
    EdgeIsLatest {
        uuid: Uuid,
        version: u32,
    },
    /// Closing at this effective time cannot create a negative validity interval.
    EdgeStartsNoLaterThan {
        uuid: Uuid,
        effective_end: DateTime<Utc>,
    },
    /// The relationship has no observation newer than this input.
    EdgeNotObservedAfter {
        uuid: Uuid,
        observed_at: DateTime<Utc>,
    },
    /// Every observation of the relationship is strictly
    /// before `observed_at`: an observation at that time contradicts the
    /// closure and rejects the commit.
    EdgeObservedBefore {
        uuid: Uuid,
        observed_at: DateTime<Utc>,
    },
    /// Exact state of every version on a directed pair, including ended and pending
    /// intervals. Embeddings are excluded so background vector refreshes do not conflict.
    RelationshipTimelineIs {
        source_chain_id: Uuid,
        target_chain_id: Uuid,
        versions: Vec<super::GraphProperties>,
    },
    /// Complete source/name history, including other targets and producer scopes.
    RelationTimelineIs {
        source_chain_id: Uuid,
        name: String,
        versions: Vec<super::relationship_timeline::VersionState>,
    },
    /// Complete history selected by the observation owner and reference slot.
    ReferenceOwnerTimelineIs {
        owner: crate::runtime::stage_output::ReferenceOwnerSelector,
        versions: Vec<super::relationship_timeline::IncidentVersionState>,
    },
    /// Complete incident history, including ended and scheduled intervals.
    IncidentTimelineIs {
        chain_id: Uuid,
        versions: Vec<super::relationship_timeline::IncidentVersionState>,
    },
    /// Exact live relationship set on the directed pair, checked under the source lock.
    LiveEdgesForPairAre {
        source_chain_id: Uuid,
        target_chain_id: Uuid,
        uuids: Vec<Uuid>,
    },
    /// The membership set read by reconciliation, checked under the entity lock.
    CollectionMembershipsAre {
        uuid: Uuid,
        memberships: Vec<crate::models::CollectionMembership>,
    },
    /// Deletion still has exactly one collection owner.
    SoleCollectionOwnerIs {
        uuid: Uuid,
        collection: CollectionRef,
    },
    /// All current relationships touching a live chain, checked while locking it.
    LiveIncidentEdgesAre {
        chain_id: Uuid,
        uuids: Vec<Uuid>,
    },
    /// Current relationships with this source and name must match the resolved set.
    LiveEdgesForRelationAre {
        source_chain_id: Uuid,
        name: String,
        uuids: Vec<Uuid>,
    },
    /// Current live set for an observing entity's reference slot.
    LiveEdgesForReferenceOwnerAre {
        owner: crate::runtime::stage_output::ReferenceOwnerSelector,
        uuids: Vec<Uuid>,
    },
    /// This run owns the collection's newest scan. An unclaimed collection or
    /// one whose recorded generation is older is claimed inside the
    /// transaction; a newer recorded generation, or the same generation
    /// claimed by another run, fails. Every batch of a run that declares the
    /// collection carries this check, so a superseded run stops at its next
    /// commit and a stale scan is rejected before its first write.
    OwnsCollection {
        collection: CollectionRef,
        generation: u64,
        run_id: Uuid,
    },
}

impl Precondition {
    /// Reject preconditions that could never be evaluated.
    pub fn validate(&self) -> Result<(), BackendError> {
        let ids: &[Uuid] = match self {
            Self::LatestVersionIs { chain_id, uuid, .. }
            | Self::LatestDeletedVersionIs { chain_id, uuid, .. } => &[*chain_id, *uuid],
            Self::NotObservedAfter { uuid, .. }
            | Self::EdgeIsLatest { uuid, .. }
            | Self::EdgeStartsNoLaterThan { uuid, .. }
            | Self::EdgeNotObservedAfter { uuid, .. }
            | Self::EdgeObservedBefore { uuid, .. }
            | Self::CollectionMembershipsAre { uuid, .. }
            | Self::SoleCollectionOwnerIs { uuid, .. } => std::slice::from_ref(uuid),
            Self::OwnsCollection { run_id, .. } => std::slice::from_ref(run_id),
            Self::RuleRevisionIs { id, .. } => std::slice::from_ref(id),
            Self::LiveIncidentEdgesAre { chain_id, uuids }
            | Self::LiveEdgesForRelationAre {
                source_chain_id: chain_id,
                uuids,
                ..
            } => {
                if uuids.iter().any(Uuid::is_nil)
                    || uuids.iter().collect::<std::collections::HashSet<_>>().len() != uuids.len()
                {
                    return Err(BackendError::Query(
                        "relationship precondition requires unique valid version ids".into(),
                    ));
                }
                std::slice::from_ref(chain_id)
            }
            Self::LiveEdgesForReferenceOwnerAre { owner, uuids } => {
                if owner.chain_id.is_nil()
                    || owner.namespace.trim().is_empty()
                    || owner.slot.trim().is_empty()
                    || uuids.iter().any(Uuid::is_nil)
                    || uuids.iter().collect::<std::collections::HashSet<_>>().len() != uuids.len()
                {
                    return Err(BackendError::Query(
                        "reference owner precondition requires a valid owner and unique versions"
                            .into(),
                    ));
                }
                std::slice::from_ref(&owner.chain_id)
            }
            Self::IdentityRevisionIs(_)
            | Self::IncidentHistoryIs { .. }
            | Self::IncidentTimelineIs { .. }
            | Self::RelationTimelineIs { .. }
            | Self::ReferenceOwnerTimelineIs { .. }
            | Self::RelationshipTimelineIs { .. }
            | Self::EdgeHeadIs { .. }
            | Self::LiveEdgesForPairAre { .. }
            | Self::NoLiveVersionFor { .. } => &[],
        };
        if ids.iter().any(Uuid::is_nil) {
            return Err(BackendError::Query(
                "precondition requires nonnil identifiers".into(),
            ));
        }
        match self {
            Self::RuleRevisionIs {
                revision, status, ..
            } => {
                if *revision == 0
                    || !matches!(
                        status,
                        super::RuleStatus::Active
                            | super::RuleStatus::Stale
                            | super::RuleStatus::Revoked
                    )
                {
                    return Err(BackendError::Query(
                        "rule revision precondition requires an effective lifecycle revision"
                            .into(),
                    ));
                }
                Ok(())
            }
            Self::IdentityRevisionIs(expected) => expected.validate(),
            Self::IncidentHistoryIs { chain_id, versions }
            | Self::IncidentTimelineIs { chain_id, versions } => {
                super::relationship_timeline::validate_incident(*chain_id, versions)
            }
            Self::RelationTimelineIs {
                source_chain_id,
                name,
                versions,
            } => super::relationship_timeline::validate_relation(*source_chain_id, name, versions),
            Self::ReferenceOwnerTimelineIs { owner, versions } => {
                if owner.chain_id.is_nil()
                    || owner.namespace.trim().is_empty()
                    || owner.slot.trim().is_empty()
                {
                    return Err(BackendError::Query(
                        "invalid reference owner timeline".into(),
                    ));
                }
                super::relationship_timeline::validate_incident(owner.chain_id, versions)
            }
            Self::RelationshipTimelineIs {
                source_chain_id,
                target_chain_id,
                versions,
            } => {
                super::relationship_timeline::validate(*source_chain_id, *target_chain_id, versions)
            }
            Self::EdgeHeadIs {
                source_chain_id,
                target_chain_id,
                chain_id,
                uuid,
                version,
                ..
            } => {
                if [source_chain_id, target_chain_id, chain_id, uuid]
                    .iter()
                    .any(|id| id.is_nil())
                    || *version == 0
                {
                    return Err(BackendError::Query(
                        "relationship head requires valid ids and version".into(),
                    ));
                }
                Ok(())
            }
            Self::LiveEdgesForPairAre {
                source_chain_id,
                target_chain_id,
                uuids,
            } => {
                let unique: std::collections::HashSet<_> = uuids.iter().collect();
                if source_chain_id.is_nil()
                    || target_chain_id.is_nil()
                    || uuids.iter().any(Uuid::is_nil)
                    || unique.len() != uuids.len()
                {
                    return Err(BackendError::Query("relationship pair precondition requires valid endpoints and unique version ids".into()));
                }
                Ok(())
            }
            Self::OwnsCollection {
                collection,
                generation,
                ..
            } => {
                super::graph_mutation::validate_generation(*generation)?;
                collection.validate().map_err(BackendError::Query)
            }
            Self::CollectionMembershipsAre { memberships, .. } => {
                for membership in memberships {
                    membership
                        .collection
                        .validate()
                        .map_err(BackendError::Query)?;
                    super::graph_mutation::validate_generation(membership.generation)?;
                }
                Ok(())
            }
            Self::SoleCollectionOwnerIs { collection, .. } => collection
                .validate()
                .map_err(|message| BackendError::Query(format!("ownership {message}"))),
            Self::NoLiveVersionFor { hashes }
                if hashes.is_empty() || hashes.iter().any(|h| h.trim().is_empty()) =>
            {
                Err(BackendError::Query(
                    "identity precondition requires nonblank hashes".into(),
                ))
            }
            Self::LatestVersionIs { version, .. } | Self::EdgeIsLatest { version, .. }
                if *version == 0 =>
            {
                Err(BackendError::Query("versions start at 1".into()))
            }
            Self::LiveEdgesForRelationAre { name, .. } if name.trim().is_empty() => Err(
                BackendError::Query("relationship precondition requires a name".into()),
            ),
            _ => Ok(()),
        }
    }
}

/// One atomic unit of work: preconditions, ordered mutations, and the result
/// the receipt returns on replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationBatch {
    pub org_id: String,
    pub batch: BatchIdentity,
    /// Fingerprint of the run this batch belongs to.
    pub fingerprint: RequestFingerprint,
    pub preconditions: Vec<Precondition>,
    pub mutations: Vec<GraphMutation>,
    /// Caller-defined committed outcome, stored verbatim in the receipt.
    /// Must be a JSON object without payloads or credentials.
    pub result: Value,
}

impl MutationBatch {
    /// Count compiled work using the same rules as transaction admission.
    /// Safe consecutive writes use the adapter's bulk grouping contract.
    pub fn estimated_statement_count(&self) -> usize {
        let mut statements = self.preconditions.len()
            + usize::from(self.mutations.iter().any(|mutation| {
                matches!(
                    mutation,
                    GraphMutation::AssociateSagaSnapshot { .. }
                        | GraphMutation::SetSagaSummary { .. }
                )
            }));
        let mut remaining = self.mutations.as_slice();
        while !remaining.is_empty() {
            let count = super::graph_mutation::mutation_group_length(remaining);
            let mutation = &remaining[0];
            statements += if count > 1 {
                if matches!(mutation, GraphMutation::RecordObservation { .. }) {
                    2
                } else {
                    1
                }
            } else {
                match mutation {
                    GraphMutation::RecordUnresolvedReferences { .. } => 1,
                    GraphMutation::RepointEntity { .. } => 5,
                    GraphMutation::MergeChains { .. } | GraphMutation::SplitChain { .. } => 8,
                    GraphMutation::UpsertEdge { .. } | GraphMutation::RecordObservation { .. } => 2,
                    GraphMutation::BeginCommunityGeneration { .. }
                    | GraphMutation::PublishCommunityGeneration { .. } => 3,
                    GraphMutation::StageCommunityPartition { partition } => {
                        3 + partition.partition.definitions.len()
                            + partition.partition.memberships.len()
                    }
                    GraphMutation::UpdateCommunities { update } => 3 + update.communities.len() * 3,
                    GraphMutation::SetDerivedSummary { guard, .. }
                    | GraphMutation::ClearDerivedSummary { guard } => {
                        guard.entity_versions.len() + 2
                    }
                    GraphMutation::AssociateSagaSnapshot { .. }
                    | GraphMutation::SetSagaSummary { .. } => 2,
                    GraphMutation::RecordReferenceDecisions { decisions, reuses } => {
                        use crate::runtime::reference_resolution::MAX_DECISIONS_PER_STATEMENT;
                        decisions.len().div_ceil(MAX_DECISIONS_PER_STATEMENT)
                            + reuses.len().div_ceil(MAX_DECISIONS_PER_STATEMENT)
                    }
                    _ => 1,
                }
            };
            remaining = &remaining[count..];
        }
        statements
    }

    /// Enforce organization scope, mutation validity, and transaction budgets.
    /// An oversized batch is rejected before any write; it is never split here.
    pub fn validate(&self) -> Result<(), BackendError> {
        if self.org_id.trim().is_empty() {
            return Err(BackendError::Query("batch requires an organization".into()));
        }
        if self.batch.run_id.is_nil() {
            return Err(BackendError::Query(
                "batch requires a valid run identifier".into(),
            ));
        }
        self.fingerprint.validate()?;
        if !self.result.is_object() {
            return Err(BackendError::Query(
                "batch result must be a JSON object".into(),
            ));
        }
        let result_bytes = serde_json::to_vec(&self.result)
            .map_err(|e| BackendError::Serialization(e.to_string()))?
            .len();
        if result_bytes > 8 * 1024 * 1024 {
            return Err(BackendError::Query("batch receipt exceeds 8 MiB".into()));
        }
        let payload_bytes = serde_json::to_vec(&(&self.preconditions, &self.mutations))
            .map_err(|error| BackendError::Serialization(error.to_string()))?
            .len();
        if payload_bytes > 16 * 1024 * 1024 {
            return Err(BackendError::Query(
                "transaction guards and mutations exceed 16 MiB".into(),
            ));
        }
        let statements = self.estimated_statement_count();
        if statements > MAX_STATEMENTS_PER_BATCH {
            return Err(BackendError::Query(format!(
                "batch has {statements} statements; the limit is {MAX_STATEMENTS_PER_BATCH}"
            )));
        }
        for precondition in &self.preconditions {
            precondition.validate()?;
            if let Precondition::OwnsCollection { run_id, .. } = precondition {
                if *run_id != self.batch.run_id {
                    return Err(BackendError::Query(
                        "collection ownership must belong to the committing run".into(),
                    ));
                }
            }
        }
        let mut embedding_bytes = 0usize;
        for mutation in &self.mutations {
            mutation.validate(&self.org_id)?;
            let saga_bytes = match mutation {
                GraphMutation::AssertCommunityState { state } => Some(serde_json::to_vec(state)),
                GraphMutation::BeginCommunityGeneration { generation } => {
                    Some(serde_json::to_vec(generation))
                }
                GraphMutation::StageCommunityPartition { partition } => {
                    Some(serde_json::to_vec(partition))
                }
                GraphMutation::PublishCommunityGeneration { publication } => {
                    Some(serde_json::to_vec(publication))
                }
                GraphMutation::UpdateCommunities { update } => Some(serde_json::to_vec(update)),
                GraphMutation::AssociateSagaSnapshot { association } => {
                    Some(serde_json::to_vec(association))
                }
                GraphMutation::SetSagaSummary { summary } => Some(serde_json::to_vec(summary)),
                _ => None,
            };
            if let Some(bytes) = saga_bytes {
                embedding_bytes = embedding_bytes.saturating_add(
                    bytes
                        .map_err(|e| BackendError::Serialization(e.to_string()))?
                        .len(),
                );
            }

            if let GraphMutation::SetEntityVersionEmbedding { embedding, .. }
            | GraphMutation::SetEmbedding { embedding, .. }
            | GraphMutation::SetRelationshipEmbedding { embedding, .. }
            | GraphMutation::SetDerivedSummary { embedding, .. } = mutation
            {
                embedding_bytes += embedding.values.len() * std::mem::size_of::<f32>();
            }
            if let GraphMutation::ClearDerivedSummary { guard } = mutation {
                embedding_bytes = embedding_bytes.saturating_add(
                    serde_json::to_vec(guard)
                        .map_err(|error| BackendError::Serialization(error.to_string()))?
                        .len(),
                );
            }
            if let GraphMutation::SetDerivedSummary { guard, summary, .. } = mutation {
                embedding_bytes = embedding_bytes.saturating_add(
                    serde_json::to_vec(&(guard, summary))
                        .map_err(|error| BackendError::Serialization(error.to_string()))?
                        .len(),
                );
            }
            if let GraphMutation::SetEntityVersionEmbedding {
                expected_properties,
                ..
            } = mutation
            {
                embedding_bytes += serde_json::to_vec(expected_properties)
                    .map_err(|error| BackendError::Serialization(error.to_string()))?
                    .len();
            }
        }
        if embedding_bytes > MAX_EMBEDDING_BYTES_PER_BATCH {
            return Err(BackendError::Query(format!(
                "batch carries {embedding_bytes} embedding payload bytes; the limit is {MAX_EMBEDDING_BYTES_PER_BATCH}"
            )));
        }
        Ok(())
    }

    /// Receipt key for this batch.
    pub fn batch_id(&self) -> Uuid {
        self.batch.batch_id()
    }
}

/// A receipt read back from storage.
#[derive(Debug, Clone, PartialEq)]
pub struct CommittedBatch {
    pub batch_id: Uuid,
    pub run_id: Uuid,
    pub kind: BatchKind,
    pub index: u32,
    pub committed_at: DateTime<Utc>,
    /// The result stored when the batch first committed.
    pub result: Value,
    /// True when this call found an existing receipt instead of writing.
    pub replayed: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn snapshots() -> Vec<SnapshotInput> {
        vec![SnapshotInput {
            relationship_changes: Default::default(),
            saga: None,
            previous_snapshot_uuids: vec![],
            labels: Vec::new(),
            tags: Default::default(),
            namespace: "prod".into(),
            name: "s".into(),
            source_description: None,
            data_type: crate::models::SnapshotDataType::Entities,
            snapshot_kind: Default::default(),
            sync_generation: None,
            complete: false,
            org_id: None,
            source: "svc".into(),
            entities: vec![],
            content: Some("{\"b\":1,\"a\":[1,2]}".into()),
            entity_types: None,
            edge_types: None,
            edge_type_map: None,
            exclude_fk_properties: vec![],
            ignore_change_properties: vec![],
            captured_at: None,
            collection: None,
        }]
    }

    #[test]
    fn batch_ids_are_deterministic_per_kind_and_index() {
        let run = Uuid::new_v4();
        let a = BatchIdentity {
            run_id: run,
            kind: BatchKind::Node,
            index: 0,
        };
        let b = BatchIdentity {
            run_id: run,
            kind: BatchKind::Relationship,
            index: 0,
        };
        assert_eq!(a.batch_id(), a.batch_id());
        assert_ne!(a.batch_id(), b.batch_id());
        assert_eq!(
            a.batch_id(),
            Uuid::new_v5(&run, b"batch:node:0"),
            "matches the documented derivation"
        );
    }

    #[test]
    fn fingerprint_depends_on_org_input_order_and_settings() {
        let base = RequestFingerprint::compute("org", &snapshots(), &json!({"x": 1})).unwrap();
        base.validate().unwrap();
        assert_eq!(base.0.len(), 32);
        assert_eq!(
            base,
            RequestFingerprint::compute("org", &snapshots(), &json!({"x": 1})).unwrap()
        );
        assert_ne!(
            base,
            RequestFingerprint::compute("other", &snapshots(), &json!({"x": 1})).unwrap()
        );
        assert_ne!(
            base,
            RequestFingerprint::compute("org", &snapshots(), &json!({"x": 2})).unwrap()
        );
        let mut two = snapshots();
        two.push(snapshots().remove(0));
        two[1].name = "t".into();
        let mut reversed = two.clone();
        reversed.reverse();
        assert_ne!(
            RequestFingerprint::compute("org", &two, &json!({})).unwrap(),
            RequestFingerprint::compute("org", &reversed, &json!({})).unwrap(),
            "input order is part of the identity"
        );
        assert!(RequestFingerprint("short".into()).validate().is_err());
    }

    #[test]
    fn canonical_form_sorts_object_keys() {
        let mut a = Vec::new();
        canonical(&json!({"z": {"b": 1, "a": 2}, "a": [true, null]}), &mut a);
        assert_eq!(
            String::from_utf8(a).unwrap(),
            r#"{"a":[true,null],"z":{"a":2,"b":1}}"#
        );
    }

    #[test]
    fn batch_validation_enforces_budgets_and_shape() {
        let identity = BatchIdentity {
            run_id: Uuid::new_v4(),
            kind: BatchKind::Node,
            index: 0,
        };
        let fingerprint = RequestFingerprint("0".repeat(32));
        let ok = MutationBatch {
            org_id: "org".into(),
            batch: identity,
            fingerprint: fingerprint.clone(),
            preconditions: vec![Precondition::NoLiveVersionFor {
                hashes: vec!["h".into()],
            }],
            mutations: vec![],
            result: json!({"entities_created": 1}),
        };
        ok.validate().unwrap();

        let mut bad = ok.clone();
        bad.result = json!([]);
        assert!(bad.validate().is_err(), "result must be an object");

        let mut bad = ok.clone();
        bad.preconditions = vec![Precondition::NoLiveVersionFor { hashes: vec![] }];
        assert!(bad.validate().is_err(), "empty identity precondition");

        let mut bad = ok.clone();
        bad.preconditions = vec![Precondition::OwnsCollection {
            collection: CollectionRef {
                namespace: "prod".into(),
                source: "aws".into(),
                key: String::new(),
            },
            generation: 1,
            run_id: Uuid::nil(),
        }];
        assert!(bad.validate().is_err(), "blank collection key");

        let mut bad = ok.clone();
        bad.preconditions = vec![
            Precondition::EdgeIsLatest {
                uuid: Uuid::new_v4(),
                version: 1
            };
            MAX_STATEMENTS_PER_BATCH + 1
        ];
        assert!(bad.validate().is_err(), "statement budget");

        let mut bad = ok.clone();
        bad.mutations = vec![GraphMutation::SetEmbedding {
            uuid: Uuid::new_v4(),
            embedding: crate::traits::graph_backend::GraphEmbedding {
                model: "m".into(),
                values: vec![1.0; MAX_EMBEDDING_BYTES_PER_BATCH / 4 + 1],
            },
            text_version: crate::embedding::TEXT_VERSION.into(),
            content_hash: "h".into(),
        }];
        assert!(bad.validate().is_err(), "embedding budget");

        let mut bad = ok.clone();
        let historical = GraphMutation::SetEntityVersionEmbedding {
            uuid: Uuid::new_v4(),
            expected_properties: serde_json::json!({"name":"api","entity_type":"Service","namespace":"prod","version":1}).as_object().unwrap().clone(),
            embedding: crate::traits::graph_backend::GraphEmbedding {
                model: "m".into(),
                values: vec![1.0; MAX_EMBEDDING_BYTES_PER_BATCH / 8],
            },
            text_version: crate::embedding::TEXT_VERSION.into(),
            content_hash: "h".into(),
        };
        bad.mutations = vec![historical.clone(), historical];
        assert!(
            bad.validate().is_err(),
            "content baselines count toward embedding payload budget"
        );

        let mut bad = ok;
        bad.org_id = " ".into();
        assert!(bad.validate().is_err(), "blank organization");
    }

    #[test]
    fn unresolved_slots_use_compiled_statement_count_for_large_batches() {
        let run_id = Uuid::new_v4();
        let batch = MutationBatch {
            org_id: "org".into(),
            batch: BatchIdentity {
                run_id,
                kind: BatchKind::Relationship,
                index: 0,
            },
            fingerprint: RequestFingerprint("0".repeat(32)),
            preconditions: vec![],
            mutations: (0..5_001)
                .map(|index| GraphMutation::RecordUnresolvedReferences {
                    source_chain_id: run_id,
                    slot: format!("slot-{index}"),
                    decided_at: Utc::now(),
                    decision_id: run_id,
                    entries: vec![],
                })
                .collect(),
            result: json!({}),
        };
        assert_eq!(batch.estimated_statement_count(), 11);
        batch.validate().unwrap();
    }
}
