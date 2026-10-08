//! Typed stage handoffs and bounded batch recovery records.
//! Only node resolutions are durable; other stage summaries omit their collections.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;

use crate::enums::versioning::PropertyChange;
use crate::models::CollectionRef;
use crate::models::{EntityEdge, EntityNode, SnapshotInput, SnapshotNode};
use crate::pipeline::CommittedCounts;
use crate::traits::{BatchIdentity, RequestFingerprint};

/// Fuzzy adoption persisted as a chain merge; the winner inherits identity aliases.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainsMerged {
    pub effective_at: DateTime<Utc>,
    pub winner_chain_id: Uuid,
    pub loser_chain_id: Uuid,
    /// Identity hashes the winner now also answers to.
    pub merged_identity_hashes: Vec<String>,
    /// Stage or model that decided the merge.
    pub merged_by: String,
    pub reason: Option<String>,
}

/// Typed stage hand-offs; large collections are shared through `Arc`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StageOutput {
    CommunityRequest(super::community::CommunityRequestOutput),
    CommunityClusters(super::community::CommunityClustersOutput),
    CommunityDrafts(super::community::CommunityDraftsOutput),
    CommunityPrepared(super::community::CommunityPreparedOutput),
    SagaSummaryBatch(SagaSummaryBatchOutput),
    ReusedSnapshot(ReusedSnapshotOutput),
    Empty,
    Input(Box<SnapshotInput>),
    /// Transient validated input; durable recovery starts after resolution.
    #[serde(skip)]
    ValidatedInput(Box<crate::models::ValidatedSnapshotInput>),
    /// Local observation record and its original input; recovery uses resolved checkpoints.
    #[serde(skip)]
    PreparedSnapshot(Box<PreparedSnapshotInput>),
    /// Declared source observations before schema inference or normalization.
    #[serde(skip)]
    StructuredDrafts(Box<StructuredEntityDrafts>),
    /// Transient evidence-backed mentions; graph identity belongs to preparation.
    #[serde(skip)]
    TextDrafts(Box<super::entity_drafts::TextEntityDrafts>),
    NodeExtraction(NodeExtractionOutput),
    /// Transient identity decisions; version classification follows matching.
    #[serde(skip)]
    NodeIdentity(NodeIdentityOutput),
    NodeResolution(NodeResolutionOutput),
    EdgeExtraction(EdgeExtractionOutput),
    EdgeResolution(EdgeResolutionOutput),

    /// Runner-aggregated batch with its commit identity, ready to persist.
    FlushBatch(FlushBatchOutput),
    SummaryBatch(SummaryBatchOutput),

    /// Read-only mutation plan; provider work has not started.
    #[serde(skip)]
    PlannedBatch(PlannedBatchOutput),

    /// All required vectors are validated and included in the atomic batch.
    #[serde(skip)]
    PreparedBatch(PreparedBatchOutput),

    /// Acknowledged commit of one batch.
    Committed(CommitOutput),
}

/// Exact version content selected by planning, never a latest-chain lookup.
#[derive(Debug, Clone)]
pub struct PlannedEmbedding {
    pub write: PlannedEmbeddingWrite,
    pub uuid: Uuid,
    pub namespace: String,
    pub text: String,
    pub content_hash: String,
    pub text_version: String,
    pub reuse: Option<crate::embedding::ComputedEmbedding>,
}

#[derive(Debug, Clone)]
pub enum PlannedEmbeddingWrite {
    EntityVersion {
        /// The semantic content the storage precondition compares.
        expected_properties: crate::traits::GraphProperties,
        /// Inputs of the embedding text that storage does not guard (labels are
        /// merged by snapshot metadata, key declarations are written apart);
        /// the planning guard re-renders the text from them.
        labels: Vec<String>,
        primary_key_properties: Vec<String>,
        additional_key_properties: Vec<Vec<String>>,
    },
    RelationshipVersion,
    DerivedSummary {
        summary: Box<crate::entity_summary::DerivedSummary>,
        guard: crate::entity_summary::SummaryEvidenceGuard,
    },
}
impl PlannedEmbedding {
    pub fn kind(&self) -> crate::embedding_rebuild::EmbeddingKind {
        use crate::embedding_rebuild::EmbeddingKind;
        match self.write {
            PlannedEmbeddingWrite::EntityVersion { .. } => EmbeddingKind::Entity,
            PlannedEmbeddingWrite::RelationshipVersion => EmbeddingKind::Relationship,
            PlannedEmbeddingWrite::DerivedSummary { .. } => EmbeddingKind::DerivedSummary,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PlannedBatchOutput {
    pub batch: crate::traits::MutationBatch,
    pub embeddings: Vec<PlannedEmbedding>,
}

#[derive(Debug, Clone)]
pub struct PreparedBatchOutput {
    pub batch: crate::traits::MutationBatch,
}

/// One prepared observation paired with its immutable validated input.
#[derive(Debug, Clone)]
pub struct PreparedSnapshotInput {
    input: crate::models::ValidatedSnapshotInput,
    snapshot: SnapshotNode,
    history: super::history::SnapshotHistory,
    schemas: Option<super::schemas::ObservationSchemas>,
}

impl PreparedSnapshotInput {
    /// Create the observation record once; does not read or write external state.
    pub fn new(
        input: crate::models::ValidatedSnapshotInput,
        org_id: &str,
    ) -> Result<Self, crate::models::InputValidationError> {
        input.check_org(org_id)?;
        let original = input.input();
        let now = Utc::now();
        let snapshot = SnapshotNode {
            uuid: Uuid::new_v4(),
            org_id: org_id.into(),
            namespace: original.namespace.clone(),
            name: original.name.clone(),
            source_description: original.source_description.clone(),
            data_type: original.data_type,
            snapshot_kind: original.snapshot_kind,
            sync_generation: original.sync_generation,
            complete: original.complete,
            collection: original.collection.clone(),
            source: original.source.clone(),
            content: super::history::source_content(original).map_err(|_| {
                crate::models::InputValidationError {
                    field: "content".into(),
                    reason: crate::models::InputValidationReason::WrongShape,
                }
            })?,
            captured_at: original.captured_at.unwrap_or(now),
            created_at: now,
            entities: vec![],
            entity_edges: vec![],
            labels: original.labels.clone(),
            tags: original.tags.clone(),
        };
        Ok(Self {
            input,
            snapshot,
            history: Default::default(),
            schemas: None,
        })
    }

    pub fn apply_frozen_identity(
        &mut self,
        entry: &super::saga::FrozenObservation,
    ) -> Result<(), String> {
        if !matches!(entry.kind, super::saga::FrozenObservationKind::Fresh)
            || entry.snapshot_uuid.is_nil()
            || entry.namespace != self.snapshot.namespace
            || self
                .input()
                .captured_at
                .is_some_and(|at| at != entry.captured_at)
        {
            return Err("prepared input disagrees with frozen identity".into());
        }
        self.snapshot.uuid = entry.snapshot_uuid;
        self.snapshot.created_at = entry.created_at;
        self.snapshot.captured_at = entry.captured_at;
        Ok(())
    }

    pub fn schemas(&self) -> Option<&super::schemas::ObservationSchemas> {
        self.schemas.as_ref()
    }

    /// Freeze effective definitions without reading a provider or changing evidence.
    pub fn attach_schemas(
        &mut self,
        org: &str,
        manifest: &super::schemas::RunSchemaManifest,
    ) -> Result<(), String> {
        self.check_org(org).map_err(|_| "schema scope mismatch")?;
        self.schemas = Some(manifest.observation(org, self.input())?);
        Ok(())
    }

    pub fn history(&self) -> &super::history::SnapshotHistory {
        &self.history
    }

    /// Attach scoped evidence without changing the prepared observation.
    pub fn attach_history(
        &mut self,
        org: &str,
        records: Vec<super::history::SnapshotEvidence>,
        max_bytes: usize,
    ) -> Result<(), crate::errors::BackendError> {
        let ids = self.input().previous_snapshot_uuids.clone();
        self.attach_selected_history(org, ids, records, max_bytes)
    }

    pub fn attach_selected_history(
        &mut self,
        org: &str,
        ids: Vec<Uuid>,
        mut records: Vec<super::history::SnapshotEvidence>,
        max_bytes: usize,
    ) -> Result<(), crate::errors::BackendError> {
        self.check_org(org)
            .map_err(|_| crate::errors::BackendError::Query("context scope mismatch".into()))?;
        let request = super::history::SnapshotEvidenceRequest {
            namespace: self.snapshot.namespace.clone(),
            ids,
            captured_before: self.snapshot.captured_at,
            max_bytes,
        };
        super::history::validate_evidence(org, &request, &mut records)?;
        self.history = super::history::SnapshotHistory::new(records);
        Ok(())
    }

    /// Original source payload and processing hints; never reconstructed from the node.
    pub fn input(&self) -> &SnapshotInput {
        self.input.input()
    }

    /// The observation all extraction branches must reuse.
    pub fn snapshot(&self) -> &SnapshotNode {
        &self.snapshot
    }

    /// Verify scope before inspecting or forwarding this handoff.
    pub fn check_org(&self, org_id: &str) -> Result<(), crate::models::InputValidationError> {
        self.input.check_org(org_id)
    }

    /// Move the paired input and observation into extraction in their original scope.
    pub fn into_parts(
        self,
        org_id: &str,
    ) -> Result<(SnapshotInput, SnapshotNode), crate::models::InputValidationError> {
        Ok((self.input.into_input(org_id)?, self.snapshot))
    }
}

/// A declared observation, with effective scope and capture metadata, not graph identity.
#[derive(Debug, Clone)]
pub struct DeclaredEntityDraft {
    entity: crate::models::ConnectorEntity,
    snapshot_id: Uuid,
    captured_at: DateTime<Utc>,
    sync_generation: Option<u64>,
}

impl DeclaredEntityDraft {
    /// Source values are unchanged except that the effective namespace is explicit.
    pub fn entity(&self) -> &crate::models::ConnectorEntity {
        &self.entity
    }

    pub fn snapshot_id(&self) -> Uuid {
        self.snapshot_id
    }

    pub fn captured_at(&self) -> DateTime<Utc> {
        self.captured_at
    }

    pub fn sync_generation(&self) -> Option<u64> {
        self.sync_generation
    }

    /// Deletion observations need identity preparation but must never create children.
    pub fn is_deleted(&self) -> bool {
        matches!(
            self.entity.lifecycle,
            crate::enums::EntityLifecycle::Deleted | crate::enums::EntityLifecycle::Deleting
        )
    }
}

/// Private construction keeps drafts paired with their validated source observation.
#[derive(Debug, Clone)]
pub struct StructuredEntityDrafts {
    prepared: PreparedSnapshotInput,
    drafts: Vec<DeclaredEntityDraft>,
}

impl StructuredEntityDrafts {
    pub fn history(&self) -> &super::history::SnapshotHistory {
        self.prepared.history()
    }

    pub fn schemas(&self) -> Option<&super::schemas::ObservationSchemas> {
        self.prepared.schemas()
    }

    pub fn new(
        prepared: PreparedSnapshotInput,
        org: &str,
    ) -> Result<Self, crate::errors::StageError> {
        prepared
            .check_org(org)
            .map_err(|error| crate::errors::StageError::StateValidation {
                stage: "direct_extraction".into(),
                message: error.to_string(),
            })?;
        if prepared.input().entities.is_empty() && prepared.input().content.is_some() {
            return Err(crate::errors::StageError::StateValidation {
                stage: "direct_extraction".into(),
                message: "content-only input requires LLM extraction".into(),
            });
        }
        let snapshot = prepared.snapshot();
        let drafts = prepared
            .input()
            .entities
            .iter()
            .map(|entity| {
                let mut entity = entity.clone();
                entity.namespace = Some(
                    entity
                        .namespace
                        .clone()
                        .unwrap_or_else(|| snapshot.namespace.clone()),
                );
                DeclaredEntityDraft {
                    entity,
                    snapshot_id: snapshot.uuid,
                    captured_at: snapshot.captured_at,
                    sync_generation: snapshot.sync_generation,
                }
            })
            .collect();
        Ok(Self { prepared, drafts })
    }

    pub fn prepared(&self) -> &PreparedSnapshotInput {
        &self.prepared
    }

    pub fn drafts(&self) -> &[DeclaredEntityDraft] {
        &self.drafts
    }

    pub fn into_parts(
        self,
        org: &str,
    ) -> Result<
        (SnapshotInput, SnapshotNode, Vec<DeclaredEntityDraft>),
        crate::models::InputValidationError,
    > {
        let (input, snapshot) = self.prepared.into_parts(org)?;
        Ok((input, snapshot, self.drafts))
    }
}

/// Incomplete extraction that suppresses full-sync deletion for its `(namespace, source)`.
/// Missing facts from failed, skipped, or truncated extraction must not be swept.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncompleteExtraction {
    pub snapshot_id: Uuid,
    pub namespace: String,
    pub source: String,
    pub reason: String,
}

/// Output of validation/LLM extraction: entities ready for resolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeExtractionOutput {
    pub raw_text_drafts: Arc<Vec<super::entity_drafts::RawTextDraft>>,
    pub relationship_changes: Arc<HashMap<Uuid, Vec<crate::models::RelationshipChange>>>,
    /// Effective non-versioning properties keyed by original observation UUID.
    pub version_exclusions: Arc<HashMap<Uuid, Vec<String>>>,
    /// Text-derived observations eligible for evidence-backed attribute enrichment.
    pub text_observation_ids: Arc<HashSet<Uuid>>,
    /// Reference-scan exclusions from each original source observation.
    pub fk_exclusions: Arc<HashMap<Uuid, Vec<String>>>,
    pub schemas: Arc<HashMap<Uuid, super::schemas::ObservationSchemas>>,
    pub history: Arc<HashMap<Uuid, super::history::SnapshotHistory>>,
    #[serde(skip)]
    pub snapshot_nodes: Arc<Vec<SnapshotNode>>,
    #[serde(skip)]
    pub entities_by_snapshot: Arc<Vec<(Uuid, Vec<EntityNode>)>>,
    /// Source-reported deletions bypass creation.
    #[serde(skip)]
    pub source_deleted: Arc<Vec<EntityNode>>,
    #[serde(skip)]
    pub sub_edges: Arc<Vec<EntityEdge>>,
    /// Incomplete scopes excluded from deletion sweeps; empty means extraction completed.
    #[serde(skip)]
    pub incomplete_extractions: Arc<Vec<IncompleteExtraction>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityOutcome {
    Matched,
    New,
    Unresolved,
}

/// A chain selected without deciding whether its properties need a new version.
#[derive(Debug, Clone)]
pub struct IdentityMatch {
    pub outcome: IdentityOutcome,
    pub chain_id: Uuid,
    pub existing: Option<crate::traits::EntityVersionRecord>,
}

/// Every observation survives identity matching, including repeated mentions.
#[derive(Debug, Clone)]
pub struct NodeIdentityOutput {
    pub identity_revisions: Arc<Vec<crate::traits::IdentityRevision>>,
    pub extraction: NodeExtractionOutput,
    pub observations: HashMap<Uuid, ObservedEntityProperties>,
    pub matches: HashMap<Uuid, IdentityMatch>,
    pub methods: HashMap<Uuid, String>,
    pub chains_merged: Vec<ChainsMerged>,
}

/// An entity that resolved against an existing version, with what changed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedChange {
    pub entity: EntityNode,
    pub changes: Vec<PropertyChange>,
}

/// One component of a complete key group on a target: its property, stored type
/// marker and canonical identity text. Components are key-equal only when the
/// marker and the text both match; the integer `443` never equals `"443"`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TargetKeyComponent {
    pub property: String,
    pub type_tag: String,
    pub value: String,
}

/// A complete primary or alternative key group: every component present with a
/// valid identity value. Incomplete groups are never carried — a lone component
/// can discover candidates but cannot prove a composite identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TargetKeyGroup {
    pub components: Vec<TargetKeyComponent>,
}

/// Separator inside a key-value token: `<type_tag>:<canonical text>`.
pub const KEY_VALUE_SEPARATOR: char = ':';

impl TargetKeyComponent {
    /// The exact typed token stored on nodes as `key_values` and looked up by
    /// `EntityLookup::LiveByKeyValue`. `"i:443"` never equals `"s:443"`.
    pub fn token(&self) -> String {
        format!("{}{KEY_VALUE_SEPARATOR}{}", self.type_tag, self.value)
    }
}

impl TargetKeyGroup {
    /// The component of a single-field group.
    pub fn single(&self) -> Option<&TargetKeyComponent> {
        match self.components.as_slice() {
            [component] => Some(component),
            _ => None,
        }
    }
}

/// Committed fields needed for relationship target matching. Vectors, full
/// source properties, and version history are not retained in the run index.
#[derive(Debug, Clone)]
pub struct RelationshipTarget {
    pub chain_id: Uuid,
    pub name: String,
    pub entity_type: String,
    pub namespace: String,
    /// The exact version this candidate was read as (R1 read set); a later
    /// version invalidates decisions that depended on it.
    pub version_uuid: Uuid,
    pub version: u32,
    /// Every complete key group, primary first then alternatives, with typed
    /// canonical values. Composite and non-string keys are first-class targets.
    pub key_groups: Vec<TargetKeyGroup>,
}

impl RelationshipTarget {
    /// A target for an entity observed in this run, from its typed properties.
    pub fn from_node(node: &EntityNode) -> Self {
        Self {
            chain_id: node.chain_id,
            name: node.name.clone(),
            entity_type: node.entity_type.clone(),
            namespace: node.namespace.clone(),
            version_uuid: node.uuid,
            version: node.version,
            key_groups: complete_key_groups(
                &node.primary_key_properties,
                &node.additional_key_properties,
                // A declared `name` key is carried by the node's display name, not
                // a source property: resolve it so name-keyed targets are indexed
                // and stored with their typed name token.
                |key| {
                    node.all_properties.get(key).cloned().or_else(|| {
                        (key == "name")
                            .then(|| crate::models::PropertyValue::String(node.name.clone()))
                    })
                },
            ),
        }
    }

    /// Every key component's typed token across all complete groups, deduplicated
    /// and sorted. Written as a node's `key_values`; a component token is a
    /// discovery handle, never proof of a composite identity.
    pub fn key_value_tokens(&self) -> Vec<String> {
        let mut tokens: Vec<String> = self
            .key_groups
            .iter()
            .flat_map(|group| group.components.iter().map(TargetKeyComponent::token))
            .collect();
        tokens.sort();
        tokens.dedup();
        tokens
    }

    /// The single-field string key other than `name`, as the pre-typed reference
    /// extractor indexes it. A bridge until extraction matches whole key groups.
    pub fn single_string_identity(&self) -> Option<(&str, &str)> {
        self.key_groups
            .iter()
            .filter_map(TargetKeyGroup::single)
            .find(|component| component.type_tag == "s" && component.property != "name")
            .map(|component| (component.property.as_str(), component.value.as_str()))
    }
}

/// Complete groups only. A group is dropped when any component is missing or is
/// not a valid identity value (lists, JSON, blobs, nulls, blank strings).
fn complete_key_groups(
    primary: &[String],
    additional: &[Vec<String>],
    value_of: impl Fn(&str) -> Option<crate::models::PropertyValue>,
) -> Vec<TargetKeyGroup> {
    std::iter::once(primary)
        .chain(additional.iter().map(Vec::as_slice))
        .filter(|group| !group.is_empty())
        .filter_map(|group| {
            group
                .iter()
                .map(|property| {
                    let value = value_of(property)?;
                    let text = value.as_identity_key()?;
                    Some(TargetKeyComponent {
                        property: property.clone(),
                        type_tag: crate::traits::property_codec::type_tag(&value).to_owned(),
                        value: text,
                    })
                })
                .collect::<Option<Vec<_>>>()
                .map(|components| TargetKeyGroup { components })
        })
        .collect()
}

impl From<crate::traits::EntityVersionRecord> for RelationshipTarget {
    fn from(record: crate::traits::EntityVersionRecord) -> Self {
        // A malformed primary key list (non-string entry) yields no primary group
        // rather than a partial one.
        let primary: Vec<String> = record
            .stored
            .get("primary_key_properties")
            .and_then(serde_json::Value::as_array)
            .and_then(|keys| {
                keys.iter()
                    .map(|key| key.as_str().map(str::to_owned))
                    .collect::<Option<Vec<_>>>()
            })
            .unwrap_or_default();
        // Alternative groups are stored as a JSON string on the node.
        let additional: Vec<Vec<String>> = record
            .stored
            .get("additional_key_properties")
            .and_then(|value| match value {
                serde_json::Value::String(text) => serde_json::from_str(text).ok(),
                serde_json::Value::Array(_) => serde_json::from_value(value.clone()).ok(),
                _ => None,
            })
            .unwrap_or_default();
        // The display name backs a declared `name` key when no typed source
        // property carries it (a name-keyed entity with no `name` in its payload).
        let display_name = record.name.clone();
        let key_groups = complete_key_groups(&primary, &additional, |key| {
            crate::traits::property_codec::read_property(&record.stored, key)
                .ok()
                .flatten()
                .or_else(|| {
                    (key == "name")
                        .then(|| crate::models::PropertyValue::String(display_name.clone()))
                })
        });
        Self {
            chain_id: record.chain_id,
            name: record.name,
            entity_type: record.entity_type,
            namespace: record.namespace,
            version_uuid: record.uuid,
            version: record.version,
            key_groups,
        }
    }
}

/// Original observation identity and properties, before adopting stored version fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservedEntityProperties {
    /// Original model mention; absent for structured observations and derived children.
    pub raw_mention_id: Option<super::entity_drafts::MentionId>,
    /// Accepted descriptive-attribute adjudications for this observation,
    /// persisted with its observation link so the decision stays auditable
    /// even when no new version is written.
    #[serde(default)]
    pub reconciliations: Vec<AttributeReconciliation>,
    /// Canonical classification accepted by identity matching for an unconstrained mention.
    #[serde(default)]
    pub resolved_entity_type: Option<String>,
    /// Rules frozen with this observation, including source-specific exclusions.
    pub version_exclusions: Vec<String>,
    /// Reserved for a new version if ordered observations require one.
    pub observation_uuid: Uuid,
    pub snapshot_uuid: Uuid,
    pub identity_hash: crate::identity::IdentityHash,
    pub properties: indexmap::IndexMap<String, crate::models::PropertyValue>,
    pub structural_hash: u64,
}

/// One value a simultaneous state reported for an adjudicated property.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReconciliationAlternative {
    pub value: crate::models::PropertyValue,
    /// The observation that reported it, or `None` for the committed version.
    pub observation_uuid: Option<Uuid>,
    /// The snapshot it came from, or the stored version's UUID when committed.
    pub source_uuid: Uuid,
    pub committed: bool,
}

/// An exact excerpt the adjudication cited from one snapshot's content.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReconciliationEvidence {
    pub snapshot_uuid: Uuid,
    pub quote: String,
}

/// A durable record of one accepted descriptive-attribute adjudication:
/// the alternatives that were weighed, the value accepted, the evidence
/// cited, and the model that decided. Bounded: quotes are limited by the
/// stage, alternatives and properties by its request limits.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AttributeReconciliation {
    pub path: String,
    pub accepted: crate::models::PropertyValue,
    pub alternatives: Vec<ReconciliationAlternative>,
    pub evidence: Vec<ReconciliationEvidence>,
    pub model: String,
    pub decided_at: DateTime<Utc>,
}

impl ObservedEntityProperties {
    pub fn from_entity(entity: &EntityNode, snapshot_uuid: Uuid) -> Self {
        Self {
            raw_mention_id: None,
            reconciliations: Vec::new(),
            resolved_entity_type: None,
            version_exclusions: Vec::new(),
            observation_uuid: entity.uuid,
            snapshot_uuid,
            identity_hash: entity.identity_hash,
            properties: entity.all_properties.clone(),
            structural_hash: entity.structural_hash,
        }
    }
}

/// Keeps a source observation attached when classification adopts a stored version UUID.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Observed<T> {
    pub observation_uuid: Uuid,
    pub value: T,
}

impl<T> Observed<T> {
    pub fn new(observation_uuid: Uuid, value: T) -> Self {
        Self {
            observation_uuid,
            value,
        }
    }
}

impl<T> std::ops::Deref for Observed<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> std::ops::DerefMut for Observed<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

/// Resolution buckets. Each entity carries the snapshot that observed it in
/// `last_seen_snapshot_id`; persistence links evidence from that field.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeResolutionOutput {
    /// Frozen stored-source reads for reverse repair only; ordinary historical
    /// ingestion observations do not require their version to remain latest.
    #[serde(default)]
    pub reference_source_reads: Arc<Vec<ReferenceSourceRead>>,
    pub raw_text_drafts: Arc<Vec<super::entity_drafts::RawTextDraft>>,
    pub relationship_changes: Arc<HashMap<Uuid, Vec<crate::models::RelationshipChange>>>,
    pub fk_exclusions: Arc<HashMap<Uuid, Vec<String>>>,
    pub identity_revisions: Arc<Vec<crate::traits::IdentityRevision>>,
    pub observed_properties: Arc<Vec<ObservedEntityProperties>>,
    /// Effective definitions by observation, retained in node checkpoints.
    pub schemas: Arc<HashMap<Uuid, super::schemas::ObservationSchemas>>,
    pub history: Arc<HashMap<Uuid, super::history::SnapshotHistory>>,
    pub snapshot_nodes: Arc<Vec<SnapshotNode>>,
    pub nodes_to_create: Arc<Vec<Observed<EntityNode>>>,
    /// Successor versions; `previous_version_uuid` names the superseded version.
    pub nodes_new_version: Arc<Vec<Observed<ResolvedChange>>>,
    /// In-place patches retain the existing version UUID.
    pub nodes_volatile: Arc<Vec<Observed<ResolvedChange>>>,
    /// Re-observations carrying the existing version UUID and number.
    pub nodes_unchanged: Arc<Vec<Observed<EntityNode>>>,
    /// Older observations record provenance without changing stored state.
    /// Tombstoned entries cannot serve as relationship endpoints.
    pub nodes_stale: Arc<Vec<Observed<EntityNode>>>,
    /// Tombstoned chains continued; `previous_version_uuid` names the tombstone.
    pub nodes_recreated: Arc<Vec<Observed<EntityNode>>>,
    /// Source-reported deletions carrying the live version UUID and number.
    pub nodes_deleted: Arc<Vec<Observed<EntityNode>>>,
    pub sub_edges: Arc<Vec<EntityEdge>>,
    /// Owner slots that target-side repair must re-evaluate even when fresh
    /// extraction produces no replacement edge.
    #[serde(skip)]
    pub reference_owner_refresh: Arc<Vec<ReferenceOwnerSelector>>,
    /// Incomplete scopes excluded from deletion sweeps; empty means extraction completed.
    pub incomplete_extractions: Arc<Vec<IncompleteExtraction>>,
    /// Authoritative live targets, sorted by chain ID, after all node commits. `None` means this
    /// stage was called directly without the runner's target lookup.
    #[serde(skip)]
    pub chunk_entities: Option<Arc<Vec<RelationshipTarget>>>,
    pub chains_merged: Arc<Vec<ChainsMerged>>,
}

impl NodeResolutionOutput {
    /// Every entity chain explicitly observed by this request, including stale
    /// observations and deletions. Target-change repair must not reprocess one
    /// of these chains from older durable evidence in the same run.
    pub fn observed_chain_ids(&self) -> impl Iterator<Item = Uuid> + '_ {
        self.nodes_to_create
            .iter()
            .map(|entry| entry.chain_id)
            .chain(
                self.nodes_new_version
                    .iter()
                    .map(|entry| entry.entity.chain_id),
            )
            .chain(
                self.nodes_volatile
                    .iter()
                    .map(|entry| entry.entity.chain_id),
            )
            .chain(self.nodes_unchanged.iter().map(|entry| entry.chain_id))
            .chain(self.nodes_stale.iter().map(|entry| entry.chain_id))
            .chain(self.nodes_recreated.iter().map(|entry| entry.chain_id))
            .chain(self.nodes_deleted.iter().map(|entry| entry.chain_id))
    }

    /// Live relationship sources with their original observation UUIDs.
    pub fn live_observations(&self) -> Vec<(Uuid, &EntityNode)> {
        self.nodes_to_create
            .iter()
            .map(|e| (e.observation_uuid, &e.value))
            .chain(
                self.nodes_new_version
                    .iter()
                    .map(|r| (r.observation_uuid, &r.entity)),
            )
            .chain(
                self.nodes_volatile
                    .iter()
                    .map(|r| (r.observation_uuid, &r.entity)),
            )
            .chain(
                self.nodes_unchanged
                    .iter()
                    .map(|e| (e.observation_uuid, &e.value)),
            )
            .chain(
                self.nodes_stale
                    .iter()
                    .filter(|e| e.deleted_at.is_none())
                    .map(|e| (e.observation_uuid, &e.value)),
            )
            .chain(
                self.nodes_recreated
                    .iter()
                    .map(|e| (e.observation_uuid, &e.value)),
            )
            .filter(|(_, node)| {
                self.chunk_entities.as_ref().is_none_or(|targets| {
                    targets
                        .binary_search_by_key(&node.chain_id, |record| record.chain_id)
                        .is_ok()
                })
            })
            .collect()
    }

    pub fn live_entities(&self) -> std::sync::Arc<Vec<EntityNode>> {
        std::sync::Arc::new(
            self.live_observations()
                .into_iter()
                .map(|(_, node)| node.clone())
                .collect(),
        )
    }
}

/// One eligible endpoint of an ambiguous, source-observed reference.
#[derive(Debug, Clone)]
pub struct ReferenceCandidate {
    pub target: RelationshipTarget,
    /// Complete declared key group satisfied by this candidate.
    pub matched_key_group: Vec<String>,
}

/// Relationship semantics and evidence that candidate selection must preserve.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceIntent {
    pub observing_chain_id: Uuid,
    pub observing_namespace: String,
    pub observing_entity_type: String,
    pub producer_source: String,
    pub location: String,
    pub slot: String,
    pub relationship_name: String,
    pub direction: super::extraction::ReferenceDirection,
    pub cardinality: super::extraction::ReferenceCardinality,
    pub target_key_group: Vec<String>,
    pub target_type: String,
    pub components: Vec<(String, String)>,
    pub allowed_namespaces: Option<Vec<String>>,
    pub lookup_complete: bool,
    /// Stable digest of the effective mapping/policy that produced this intent.
    pub policy_fingerprint: String,
}

/// Reference extraction has found several eligible targets; resolution decides or abstains.
#[derive(Debug, Clone)]
pub struct PendingReference {
    pub source: EntityNode,
    pub intent: ReferenceIntent,
    pub value: String,
    /// Canonical typed components supplied to the target key group.
    pub components: Vec<(String, String)>,
    pub candidates: Vec<ReferenceCandidate>,
}

/// A relationship command or discovery result intentionally not applied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationshipDecline {
    pub snapshot_id: Uuid,
    pub source_chain_id: Option<Uuid>,
    pub target_chain_id: Option<Uuid>,
    pub name: Option<String>,
    pub operation: String,
    pub reason: RelationshipDeclineReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationshipDeclineReason {
    NamespacePolicy,
    DeclaredDisabled,
    UnresolvedEndingTime,
    InvalidTimestampEvidence,
}

/// Durable per-batch reference-discovery decisions: how many references were
/// attempted and how they ended, plus the sources whose discovery was
/// truncated. Replayed with the receipt so diagnostics survive restarts.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceReport {
    /// In-memory uncertainty even when durable generic records are disabled.
    /// Model confirmation removes only the tokens it actually resolved.
    #[serde(default)]
    pub retirement_decisions: Vec<UnresolvedSlot>,
    /// Original property coverage; omission in a partial input is never absence.
    #[serde(default)]
    pub source_coverage: Vec<ReferenceSourceCoverage>,
    /// Complete incident reads used to discover old owner slots, including absence.
    #[serde(default)]
    pub source_histories: Vec<ReferenceSourceHistory>,
    /// Latest stored source version and observation clock used by repair.
    #[serde(default)]
    pub source_reads: Vec<ReferenceSourceRead>,
    #[serde(default)]
    pub relationship_declines: Vec<RelationshipDecline>,
    pub attempted: usize,
    pub confirmed: usize,
    pub unresolved: usize,
    pub excluded: usize,
    /// Source chains whose traversal or lookup was truncated; absence of an
    /// edge for them must never retire an established edge.
    pub incomplete_sources: Vec<Uuid>,
    /// Scoped entity-membership revisions read with candidate discovery. They
    /// fence both selected candidates and the absence of another match.
    #[serde(default)]
    pub candidate_revisions: Vec<crate::traits::IdentityRevision>,
    /// Durable unresolved decisions: one per source slot this run could not
    /// confirm (`target-not-found`, `partial-key`), plus empty-entry clears for
    /// slots it confirmed. The flush turns each into a
    /// `GraphMutation::RecordUnresolvedReferences` in the same batch as the edges.
    #[serde(default)]
    pub unresolved_slots: Vec<UnresolvedSlot>,
    /// Evidence-backed decisions on ambiguous references made for this batch:
    /// accepted, rejected and unsure alike, including host refusals made
    /// without a model call. Replayed with the receipt; deduplicated on their
    /// deterministic decision id.
    #[serde(default)]
    pub decisions: Vec<super::reference_resolution::ReferenceDecisionAudit>,
    /// Persistence context for the original decisions above, by decision id.
    #[serde(default)]
    pub decision_contexts: Vec<super::reference_resolution::DecisionContext>,
}

/// Property coverage at one source capture, independent of inferred relationships.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceSourceCoverage {
    pub chain_id: Uuid,
    pub namespace: String,
    pub captured_at: DateTime<Utc>,
    pub complete: bool,
    /// Flattened top-level paths; array/object values replace their entire subtree.
    pub paths: Vec<String>,
    pub excluded_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceSourceHistory {
    pub chain_id: Uuid,
    pub versions: Vec<crate::traits::relationship_timeline::IncidentVersionState>,
}

/// Source evidence fenced at a repair commit, including volatile updates that
/// keep the same entity version. Never use repair time as the observation clock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceSourceRead {
    pub chain_id: Uuid,
    pub version_uuid: Uuid,
    pub version: u32,
    pub observed_at: DateTime<Utc>,
}

/// One source slot this run could not confirm to a target. A target
/// appearing later finds the sources waiting on its typed token; empty `entries`
/// clears the slot (e.g. a slot that has since become confirmed).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnresolvedSlot {
    pub source_chain_id: Uuid,
    pub slot: String,
    pub decided_at: DateTime<Utc>,
    /// Stable tie-breaker when two observations share a capture timestamp.
    pub decision_id: Uuid,
    pub entries: Vec<crate::traits::UnresolvedReferenceEntry>,
}

impl ReferenceReport {
    pub fn merge(&mut self, other: &ReferenceReport) {
        for decision in &other.retirement_decisions {
            if !self.retirement_decisions.contains(decision) {
                self.retirement_decisions.push(decision.clone());
            }
        }
        for coverage in &other.source_coverage {
            if !self.source_coverage.contains(coverage) {
                self.source_coverage.push(coverage.clone());
            }
        }
        for history in &other.source_histories {
            if !self.source_histories.contains(history) {
                self.source_histories.push(history.clone());
            }
        }
        for read in &other.source_reads {
            if !self.source_reads.contains(read) {
                self.source_reads.push(read.clone());
            }
        }
        for decline in &other.relationship_declines {
            if !self.relationship_declines.contains(decline) {
                self.relationship_declines.push(decline.clone());
            }
        }
        self.attempted += other.attempted;
        self.confirmed += other.confirmed;
        self.unresolved += other.unresolved;
        self.excluded += other.excluded;
        for source in &other.incomplete_sources {
            if !self.incomplete_sources.contains(source) {
                self.incomplete_sources.push(*source);
            }
        }
        for revision in &other.candidate_revisions {
            match self
                .candidate_revisions
                .iter()
                .find(|existing| existing.scope == revision.scope)
            {
                Some(existing) if existing.revision != revision.revision => {
                    // The merged batch cannot commit inconsistent evidence; the
                    // downstream duplicate preconditions reject it deterministically.
                    self.candidate_revisions.push(revision.clone());
                }
                Some(_) => {}
                None => self.candidate_revisions.push(revision.clone()),
            }
        }
        for slot in &other.unresolved_slots {
            match self.unresolved_slots.iter_mut().find(|existing| {
                existing.source_chain_id == slot.source_chain_id && existing.slot == slot.slot
            }) {
                Some(existing) => {
                    if (slot.decided_at, slot.decision_id)
                        > (existing.decided_at, existing.decision_id)
                    {
                        *existing = slot.clone();
                    }
                }
                None => self.unresolved_slots.push(slot.clone()),
            }
        }
        for context in &other.decision_contexts {
            if !self
                .decision_contexts
                .iter()
                .any(|known| known.decision_id == context.decision_id)
            {
                self.decision_contexts.push(context.clone());
            }
        }
        for audit in &other.decisions {
            // Identical repeats of one occurrence dedupe here. Two different
            // records with one decision id both survive so relationship
            // planning (`flush/relationship_mutation_planning.rs`) sees the
            // conflict and rejects the batch; nothing picks a winner.
            if !self.decisions.contains(audit) {
                self.decisions.push(audit.clone());
            }
        }
    }

    pub fn incomplete(&self) -> bool {
        !self.incomplete_sources.is_empty()
    }
}

/// Output of the edge-discovery stages (heuristic + LLM), pre-resolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeExtractionOutput {
    /// Effective-time evidence keyed by the incoming observation UUID, frozen before replans.
    #[serde(default)]
    pub relationship_times: Arc<HashMap<Uuid, crate::models::RelationshipTimeEvidence>>,
    #[serde(default)]
    pub reference_report: ReferenceReport,
    pub relationship_directives: Arc<Vec<PendingRelationshipDirective>>,
    /// Transient work regenerated from the durable node checkpoint when retrying.
    #[serde(skip)]
    pub pending_references: Arc<Vec<PendingReference>>,
    #[serde(skip)]
    pub snapshot_nodes: Arc<Vec<SnapshotNode>>,
    #[serde(skip)]
    pub resolution: Arc<NodeResolutionOutput>,
    #[serde(skip)]
    pub resolved_nodes: Arc<Vec<EntityNode>>,
    #[serde(skip)]
    pub edges: Arc<Vec<EntityEdge>>,
}

/// A connector scope: the namespace and source of an observing snapshot.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ConnectorScope {
    pub namespace: String,
    pub source: String,
}

impl ConnectorScope {
    pub fn of(snapshot: &SnapshotNode) -> Self {
        Self {
            namespace: snapshot.namespace.clone(),
            source: snapshot.source.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RelationshipDirectiveAction {
    Cancel,
    Replace { replacement_edge_uuid: Uuid },
}

/// Validated source command retained independently from ordinary observations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingRelationshipDirective {
    pub target: crate::models::RelationshipVersionRef,
    pub action: RelationshipDirectiveAction,
    pub effective_at: DateTime<Utc>,
    pub snapshot_id: Uuid,
    pub captured_at: DateTime<Utc>,
    pub scope: ConnectorScope,
}

/// A stored relationship lineage head as resolution read it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredRelationship {
    #[serde(default)]
    pub time_evidence: Option<crate::models::RelationshipTimeEvidence>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub cancellation_snapshot_id: Option<Uuid>,
    pub cancellation_context: Option<crate::models::CancellationContext>,
    pub valid_from: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub uuid: Uuid,
    pub chain_id: Uuid,
    pub identity_hash: Option<String>,
    pub cardinality_key: Option<String>,
    /// Durable owner of a reference-derived relationship. Required for inverse
    /// mappings, where the observing entity is the graph target.
    #[serde(default)]
    pub reference_evidence: Option<crate::models::edges::ReferenceEvidence>,
    pub origin: crate::models::RelationshipOrigin,
    pub all_properties: indexmap::IndexMap<String, crate::models::PropertyValue>,
    pub first_seen_snapshot_id: Option<Uuid>,
    pub source_chain_id: Uuid,
    pub target_chain_id: Uuid,
    pub name: String,
    pub version: u32,
    pub confidence: f32,
    pub description: String,
    /// Latest source capture time; independent of the effective validity interval.
    pub latest_observation: Option<DateTime<Utc>>,
    /// Only observations from the source's connector scope may close this
    /// relationship by contradiction. `None` still participates in commit checks.
    pub scope: Option<ConnectorScope>,
}

impl StoredRelationship {
    /// Whether two reads saw the same stored state; the scope is derived
    /// per snapshot and is not part of the state.
    pub fn same_state(&self, other: &Self) -> bool {
        self.time_evidence == other.time_evidence
            && self.cancelled_at == other.cancelled_at
            && self.cancellation_snapshot_id == other.cancellation_snapshot_id
            && self.cancellation_context == other.cancellation_context
            && self.valid_from == other.valid_from
            && self.ended_at == other.ended_at
            && self.uuid == other.uuid
            && self.chain_id == other.chain_id
            && self.identity_hash == other.identity_hash
            && self.cardinality_key == other.cardinality_key
            && self.origin == other.origin
            && self.all_properties == other.all_properties
            && self.first_seen_snapshot_id == other.first_seen_snapshot_id
            && self.source_chain_id == other.source_chain_id
            && self.target_chain_id == other.target_chain_id
            && self.name == other.name
            && self.version == other.version
            && self.confidence == other.confidence
            && self.description == other.description
            && self.latest_observation == other.latest_observation
    }

    /// Different nonempty scopes indicate ownership changed between reads.
    fn merge_scope(&mut self, other: &Self) -> Result<(), String> {
        match (&self.scope, &other.scope) {
            (Some(mine), Some(theirs)) if mine != theirs => Err(format!(
                "relationship {} is in scope {}/{} for one snapshot and {}/{} for another",
                self.uuid, mine.namespace, mine.source, theirs.namespace, theirs.source
            )),
            (None, Some(theirs)) => {
                self.scope = Some(theirs.clone());
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// All live relationship lineages on one directed endpoint pair.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PairBaseline {
    /// Complete version state, excluding embeddings, ordered by UUID.
    pub versions: Vec<crate::traits::GraphProperties>,
    pub source_chain_id: Uuid,
    pub target_chain_id: Uuid,
    pub live: Vec<StoredRelationship>,
}

/// History and live membership of one single-target relation across producer
/// scopes. A concurrent target or history change requires replanning.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelationBaseline {
    /// Complete history ordered by UUID; live membership remains a separate check.
    pub versions: Vec<crate::traits::relationship_timeline::VersionState>,
    pub source_chain_id: Uuid,
    pub name: String,
    /// Ordered by uuid.
    pub live: Vec<StoredRelationship>,
}

/// Complete relationship history owned by one observing entity reference slot.
/// The stored graph direction is deliberately absent from this selector.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ReferenceOwnerSelector {
    pub chain_id: Uuid,
    pub namespace: String,
    pub slot: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReferenceOwnerBaseline {
    pub selector: ReferenceOwnerSelector,
    pub versions: Vec<crate::traits::relationship_timeline::IncidentVersionState>,
    pub live: Vec<StoredRelationship>,
}

/// Stored state used to plan relationships. A chunk must agree on the state
/// read by every snapshot before it can commit.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RelationshipBaseline {
    pub ended: Vec<StoredRelationship>,
    pub pairs: Vec<PairBaseline>,
    pub relations: Vec<RelationBaseline>,
    #[serde(default)]
    pub reference_owners: Vec<ReferenceOwnerBaseline>,
    /// Relation-slot target chains that no longer have a live entity head
    /// (deleted or merged away). An open edge to such a target is a legacy
    /// orphan: it must not take part in single-target supersession, so a
    /// retarget leaves it untouched and reconciliation closes it later.
    #[serde(default)]
    pub orphan_targets: Vec<Uuid>,
}

impl RelationshipBaseline {
    /// Combine a chunk's baselines in input order. Conflicting stored state
    /// requires replanning; scopes found by different snapshots are combined.
    /// After an error the partially merged result must be discarded.
    pub fn merge_all<'a>(
        &mut self,
        baselines: impl IntoIterator<Item = &'a Self>,
    ) -> Result<(), String> {
        let mut pairs: HashMap<_, _> = self
            .pairs
            .iter()
            .enumerate()
            .map(|(index, pair)| ((pair.source_chain_id, pair.target_chain_id), index))
            .collect();
        let mut relations: HashMap<_, _> = self
            .relations
            .iter()
            .enumerate()
            .map(|(index, relation)| ((relation.source_chain_id, relation.name.clone()), index))
            .collect();
        let mut reference_owners: HashMap<_, _> = self
            .reference_owners
            .iter()
            .enumerate()
            .map(|(index, owner)| (owner.selector.clone(), index))
            .collect();
        for other in baselines {
            for edge in &other.ended {
                if edge.chain_id.is_nil()
                    || edge.uuid.is_nil()
                    || (edge.ended_at.is_none() && edge.cancelled_at.is_none())
                {
                    return Err("invalid ended relationship head".into());
                }
                if let Some(existing) = self
                    .ended
                    .iter_mut()
                    .find(|stored| stored.chain_id == edge.chain_id)
                {
                    if !existing.same_state(edge) {
                        return Err(
                            "ended relationship head changed while the chunk resolved".into()
                        );
                    }
                    existing.merge_scope(edge)?;
                } else {
                    self.ended.push(edge.clone());
                }
            }
            for pair in &other.pairs {
                let ids: HashSet<_> = pair.live.iter().map(|edge| edge.uuid).collect();
                let chains: HashSet<_> = pair.live.iter().map(|edge| edge.chain_id).collect();
                if ids.len() != pair.live.len()
                    || chains.len() != pair.live.len()
                    || pair.live.iter().any(|edge| {
                        edge.uuid.is_nil()
                            || edge.chain_id.is_nil()
                            || edge.source_chain_id != pair.source_chain_id
                            || edge.target_chain_id != pair.target_chain_id
                    })
                {
                    return Err("invalid relationship pair baseline".into());
                }
                let key = (pair.source_chain_id, pair.target_chain_id);
                match pairs.get(&key).copied().map(|index| &mut self.pairs[index]) {
                    Some(existing) => {
                        if existing.versions != pair.versions
                            || existing.live.len() != pair.live.len()
                        {
                            return Err(format!(
                                "relationship pair {} -> {} changed while the chunk resolved",
                                key.0, key.1
                            ));
                        }
                        for mine in &mut existing.live {
                            let theirs = pair.live.iter().find(|edge| edge.uuid == mine.uuid)
                                .filter(|edge| mine.same_state(edge))
                                .ok_or_else(|| format!("relationship pair {} -> {} changed while the chunk resolved", key.0, key.1))?;
                            mine.merge_scope(theirs)?;
                        }
                    }
                    None => {
                        pairs.insert(key, self.pairs.len());
                        self.pairs.push(pair.clone());
                    }
                }
            }
            for relation in &other.relations {
                let key = (relation.source_chain_id, relation.name.clone());
                match relations
                    .get(&key)
                    .copied()
                    .map(|index| &mut self.relations[index])
                {
                    Some(existing) => {
                        let same = existing.versions == relation.versions
                            && existing.live.len() == relation.live.len()
                            && existing
                                .live
                                .iter()
                                .zip(&relation.live)
                                .all(|(mine, theirs)| mine.same_state(theirs));
                        if !same {
                            return Err(format!(
                                "relation {} of chain {} changed while the chunk resolved",
                                relation.name, relation.source_chain_id
                            ));
                        }
                        for (mine, theirs) in existing.live.iter_mut().zip(&relation.live) {
                            mine.merge_scope(theirs)?;
                        }
                    }
                    None => {
                        relations.insert(key, self.relations.len());
                        self.relations.push(relation.clone());
                    }
                }
            }
            for owner in &other.reference_owners {
                match reference_owners
                    .get(&owner.selector)
                    .copied()
                    .map(|index| &mut self.reference_owners[index])
                {
                    Some(existing) => {
                        let same = existing.versions == owner.versions
                            && existing.live.len() == owner.live.len()
                            && existing
                                .live
                                .iter()
                                .zip(&owner.live)
                                .all(|(mine, theirs)| mine.same_state(theirs));
                        if !same {
                            return Err(format!(
                                "reference owner {} of chain {} changed while the chunk resolved",
                                owner.selector.slot, owner.selector.chain_id
                            ));
                        }
                        for (mine, theirs) in existing.live.iter_mut().zip(&owner.live) {
                            mine.merge_scope(theirs)?;
                        }
                    }
                    None => {
                        reference_owners
                            .insert(owner.selector.clone(), self.reference_owners.len());
                        self.reference_owners.push(owner.clone());
                    }
                }
            }
            for target in &other.orphan_targets {
                if !self.orphan_targets.contains(target) {
                    self.orphan_targets.push(*target);
                }
            }
        }
        self.orphan_targets.sort_unstable();
        self.orphan_targets.dedup();
        Ok(())
    }
}

/// Model assessments are graph-dependent and are recomputed after a rejected commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationshipAssessment {
    pub observation_uuid: Uuid,
    pub candidate: crate::models::RelationshipTarget,
    pub protected_properties: Vec<String>,
    pub decision: RelationshipAssessmentDecision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationshipAssessmentDecision {
    Compatible,
    Contradiction,
    Unsure,
}

/// Per-snapshot output of the relationship pass: what this snapshot
/// observed and the stored state it read. Decisions are made per chunk by
/// persistence, in capture order across snapshots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeResolutionOutput {
    pub relationship_assessments: Arc<Vec<RelationshipAssessment>>,
    pub contradiction_timelines:
        BTreeMap<Uuid, Vec<crate::traits::relationship_timeline::IncidentVersionState>>,
    #[serde(default)]
    pub reference_report: ReferenceReport,
    pub relationship_directives: Arc<Vec<PendingRelationshipDirective>>,
    #[serde(skip)]
    pub snapshot_nodes: Arc<Vec<SnapshotNode>>,
    #[serde(skip)]
    pub resolution: Arc<NodeResolutionOutput>,
    #[serde(skip)]
    pub resolved_nodes: Arc<Vec<EntityNode>>,
    /// Relationship observations at the snapshot's capture time.
    #[serde(skip)]
    pub observed: Arc<Vec<EntityEdge>>,
    #[serde(skip)]
    pub baseline: Arc<RelationshipBaseline>,
}

/// Durable node handoff for the pending relationship pass. Snapshot content is
/// restored from the fingerprint-checked input; vectors and run-wide indexes are omitted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCheckpoint {
    pub snapshot_index: usize,
    pub resolution: NodeResolutionOutput,
}

/// Committed processing decisions. Failed inputs stay failed on replay; retrying
/// them with changed processing requires a new run id.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BatchRecovery {
    #[serde(
        default,
        skip_serializing_if = "crate::profiles::ProfileDiagnostics::is_empty"
    )]
    pub profile_diagnostics: crate::profiles::ProfileDiagnostics,
    /// Final empty receipt of an organization-scoped reference rebuild.
    #[serde(default)]
    pub reference_rebuild_complete: bool,
    /// Stored owners already processed by a reverse-repair batch. Replay uses
    /// these identities rather than a hash of a potentially regrouped page.
    #[serde(default)]
    pub reference_repair_sources: Vec<Uuid>,
    #[serde(default)]
    pub relationship_declines: Vec<RelationshipDecline>,
    #[serde(default)]
    pub incomplete_saga_summaries: Vec<crate::saga::IncompleteSagaSummary>,
    #[serde(default)]
    pub skipped_summaries: Vec<crate::pipeline::output::SkippedSummary>,
    pub community_checkpoint: Option<super::community::CommunityCheckpoint>,
    pub saga_summary_manifest: Option<SagaSummaryManifest>,
    pub saga_associations: Vec<ThreadAssociationEffect>,
    pub reused_snapshots: Vec<ReusedSnapshotOutput>,
    pub summary_affected_chains: Vec<Uuid>,
    pub summary_manifest: Option<SummaryManifest>,
    pub nodes: Vec<NodeCheckpoint>,
    pub failures: Vec<crate::pipeline::SnapshotFailure>,
    pub observed_relationships: Vec<Uuid>,
    /// Sources whose reference discovery was truncated in this batch; absence of
    /// their edges is not evidence and never authorizes retirement.
    #[serde(default)]
    pub incomplete_reference_sources: Vec<Uuid>,
    /// Evidence-backed ambiguity decisions committed with this batch, including
    /// rejected and unsure occurrences that wrote no edge. A replayed receipt
    /// restores them without any model call.
    #[serde(default)]
    pub reference_decisions: Vec<super::reference_resolution::ReferenceDecisionAudit>,
}

/// One batch handed to the persistence stage with its commit identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlushBatchOutput {
    pub batch: BatchIdentity,
    pub fingerprint: RequestFingerprint,
    /// Every collection the request declares. Each commit of the run checks
    /// that the run still owns these scans, claiming them on the first commit.
    #[serde(default)]
    pub scans: Vec<CollectionScan>,
    pub work: FlushWork,
    pub recovery: Option<BatchRecovery>,
}

/// One declared complete scan of a collection at a generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionScan {
    pub collection: CollectionRef,
    pub generation: u64,
}

/// The work of one batch; its variant must match `BatchIdentity::kind`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FlushWork {
    Nodes(NodeBatch),
    Relationships(RelationshipBatch),
    Reconciliation(ReconciliationBatch),
}

/// Every snapshot's node resolution in one chunk. Buckets are concatenated in
/// snapshot order and may hold several observations of one chain; persistence
/// orders them by capture time, then snapshot position.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeBatch {
    /// Per-observation exclusions persisted with live source metadata for repair.
    #[serde(default)]
    pub fk_exclusions: Arc<HashMap<Uuid, Vec<String>>>,
    #[serde(default)]
    pub pending_child_edges: Arc<Vec<EntityEdge>>,
    #[serde(skip)]
    pub identity_revisions: Arc<Vec<crate::traits::IdentityRevision>>,
    #[serde(skip)]
    pub observed_properties: Arc<Vec<ObservedEntityProperties>>,
    /// Snapshots in input order; the position breaks capture-time ties.
    #[serde(skip)]
    pub snapshot_nodes: Arc<Vec<SnapshotNode>>,
    #[serde(skip)]
    pub nodes_to_create: Arc<Vec<Observed<EntityNode>>>,
    #[serde(skip)]
    pub nodes_new_version: Arc<Vec<Observed<ResolvedChange>>>,
    #[serde(skip)]
    pub nodes_volatile: Arc<Vec<Observed<ResolvedChange>>>,
    #[serde(skip)]
    pub nodes_unchanged: Arc<Vec<Observed<EntityNode>>>,
    #[serde(skip)]
    pub nodes_stale: Arc<Vec<Observed<EntityNode>>>,
    #[serde(skip)]
    pub nodes_recreated: Arc<Vec<Observed<EntityNode>>>,
    #[serde(skip)]
    pub nodes_deleted: Arc<Vec<Observed<EntityNode>>>,
    #[serde(skip)]
    pub chains_merged: Arc<Vec<ChainsMerged>>,
}

/// Every snapshot's relationship observations in one chunk, against one
/// merged baseline. Persistence orders a pair's observations by capture
/// time, then snapshot position, and keeps one current target per
/// single-target relation.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RelationshipBatch {
    pub relationship_assessments: Arc<Vec<RelationshipAssessment>>,
    pub contradiction_timelines:
        BTreeMap<Uuid, Vec<crate::traits::relationship_timeline::IncidentVersionState>>,
    #[serde(default)]
    pub reference_report: ReferenceReport,
    pub relationship_directives: Arc<Vec<PendingRelationshipDirective>>,
    /// Snapshots in input order; the position breaks capture-time ties.
    #[serde(skip)]
    pub snapshot_nodes: Arc<Vec<SnapshotNode>>,
    #[serde(skip)]
    pub observed: Arc<Vec<EntityEdge>>,
    #[serde(skip)]
    pub baseline: Arc<RelationshipBaseline>,
}

/// A live version that a complete full sync did not re-observe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaleEntity {
    pub chain_id: Uuid,
    pub uuid: Uuid,
    pub version: u32,
    pub entity_type: String,
    pub name: String,
    pub collections: Vec<crate::models::CollectionMembership>,
}

/// An active or pending relationship selected for retirement by a complete collection scan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaleEdge {
    pub uuid: Uuid,
    pub version: u32,
    pub source_chain_id: Uuid,
    pub target_chain_id: Uuid,
    pub name: String,
    pub properties: crate::traits::GraphProperties,
}

/// Deletions, membership releases, and invalidations for one collection a
/// complete scan owns, effective at the scan's capture time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconciliationBatch {
    /// Latest source owners for relationship-only absence decisions.
    pub relationship_owners: std::collections::BTreeMap<Uuid, StaleEntity>,
    /// Open latest relationships used by the deletion live-set fence.
    pub live_incident: std::collections::BTreeMap<Uuid, Vec<Uuid>>,
    pub scan: CollectionScan,
    pub captured_at: DateTime<Utc>,
    /// Members no other observer still owns: tombstoned, with active intervals
    /// closed and pending intervals cancelled.
    pub entities: Vec<StaleEntity>,
    /// Members another collection or this run still observes: only the
    /// scanned collection's membership is released.
    pub released: Vec<StaleEntity>,
    pub edges: Vec<StaleEdge>,
    /// Complete history for deletion chains and standalone relationship source anchors.
    pub incident_timelines: std::collections::BTreeMap<
        Uuid,
        Vec<crate::traits::relationship_timeline::IncidentVersionState>,
    >,
}

/// Acknowledged commit of one batch. Counts come from the receipt when the
/// batch was replayed, otherwise from the work just committed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitOutput {
    pub batch: BatchIdentity,
    /// True when an earlier attempt had already committed this batch.
    pub replayed: bool,
    pub committed_at: DateTime<Utc>,
    pub counts: CommittedCounts,
    pub recovery: Option<BatchRecovery>,
}

/// Immutable reuse has no extraction or entity-resolution result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReusedSnapshotOutput {
    pub snapshot_index: usize,
    pub snapshot_uuid: Uuid,
    pub namespace: String,
    pub evidence_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadAssociationEffect {
    pub saga_uuid: Uuid,
    pub namespace: String,
    pub membership_ordinal: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SagaSummaryTarget {
    pub namespace: String,
    pub saga_uuid: Uuid,
    pub after_ordinal: u64,
    pub through_ordinal: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaSummaryManifest {
    pub targets: Vec<SagaSummaryTarget>,
    pub page_size: usize,
    pub collections: Vec<crate::pipeline::CollectionOutcome>,
}
impl SagaSummaryManifest {
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=super::history::MAX_CONTEXT_RECORDS).contains(&self.page_size)
            || self.targets.len() > super::saga::MAX_OBSERVATIONS
        {
            return Err("invalid Saga summary manifest bounds".into());
        }
        let mut previous = None;
        for target in &self.targets {
            let key = (&target.namespace, target.saga_uuid);
            if target.namespace.trim().is_empty()
                || target.saga_uuid.is_nil()
                || target.after_ordinal >= target.through_ordinal
                || target.through_ordinal > i64::MAX as u64
                || previous.is_some_and(|previous| previous >= key)
            {
                return Err("invalid or unsorted Saga summary target".into());
            }
            previous = Some(key);
        }
        self.page_count()?;
        if serde_json::to_vec(self)
            .map_err(|_| "invalid Saga summary manifest")?
            .len()
            > super::saga::MAX_MANIFEST_BYTES
        {
            return Err("Saga summary manifest exceeds byte bound".into());
        }
        Ok(())
    }
    pub fn page_count(&self) -> Result<u32, String> {
        if self.page_size == 0 {
            return Err("Saga summary page size is zero".into());
        }
        let total = self
            .targets
            .iter()
            .try_fold(0u64, |total, target| {
                let count = target
                    .through_ordinal
                    .checked_sub(target.after_ordinal)?
                    .div_ceil(self.page_size as u64);
                total.checked_add(count)
            })
            .ok_or("Saga summary page count overflow")?;
        u32::try_from(total).map_err(|_| "too many Saga summary pages".into())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SagaSummaryWork {
    Manifest {
        targets: Vec<SagaSummaryTarget>,
        collections: Vec<crate::pipeline::CollectionOutcome>,
    },
    Page {
        target: SagaSummaryTarget,
        after_ordinal: u64,
        through_ordinal: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaSummaryBatchOutput {
    pub batch: BatchIdentity,
    pub fingerprint: RequestFingerprint,
    pub scans: Vec<CollectionScan>,
    pub work: SagaSummaryWork,
}

#[cfg(test)]
mod target_tests {
    use super::*;
    use crate::traits::EntityVersionRecord;
    use serde_json::json;

    fn baseline(scope: Option<ConnectorScope>) -> RelationshipBaseline {
        let edge = StoredRelationship {
            time_evidence: None,
            cancelled_at: None,
            cancellation_snapshot_id: None,
            cancellation_context: None,
            valid_from: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            ended_at: None,
            uuid: Uuid::from_u128(1),
            chain_id: Uuid::from_u128(10),
            identity_hash: None,
            cardinality_key: None,
            origin: crate::models::RelationshipOrigin::Declared,
            all_properties: Default::default(),
            first_seen_snapshot_id: None,
            source_chain_id: Uuid::from_u128(2),
            target_chain_id: Uuid::from_u128(3),
            name: "uses".into(),
            version: 1,
            confidence: 1.0,
            description: "uses service".into(),
            latest_observation: None,
            scope,
            reference_evidence: None,
        };
        RelationshipBaseline {
            ended: vec![],
            pairs: vec![PairBaseline {
                versions: vec![],
                source_chain_id: edge.source_chain_id,
                target_chain_id: edge.target_chain_id,
                live: vec![edge.clone()],
            }],
            relations: vec![RelationBaseline {
                versions: vec![],
                source_chain_id: edge.source_chain_id,
                name: edge.name.clone(),
                live: vec![edge],
            }],
            orphan_targets: vec![],
            reference_owners: vec![],
        }
    }

    #[test]
    fn pair_baselines_compare_all_lineages_and_properties() {
        let mut original = baseline(None);
        let mut second = original.pairs[0].live[0].clone();
        second.uuid = Uuid::from_u128(11);
        second.chain_id = Uuid::from_u128(12);
        original.pairs[0].live.push(second);
        let mut reordered = original.clone();
        reordered.pairs[0].live.reverse();
        RelationshipBaseline::default()
            .merge_all([&original, &reordered])
            .unwrap();
        reordered.pairs[0].live[0]
            .all_properties
            .insert("port".into(), crate::models::PropertyValue::Integer(5432));
        assert!(RelationshipBaseline::default()
            .merge_all([&original, &reordered])
            .is_err());
        let mut duplicate = original.clone();
        duplicate.pairs[0].live[1] = duplicate.pairs[0].live[0].clone();
        assert!(RelationshipBaseline::default()
            .merge_all([&duplicate])
            .is_err());
    }

    #[test]
    fn baseline_merge_deduplicates_and_combines_scopes_in_input_order() {
        let unscoped = baseline(None);
        let scoped = baseline(Some(ConnectorScope {
            namespace: "prod".into(),
            source: "aws".into(),
        }));
        let mut other = RelationshipBaseline::default();
        other.pairs.push(PairBaseline {
            versions: vec![],
            source_chain_id: Uuid::from_u128(4),
            target_chain_id: Uuid::from_u128(5),
            live: vec![],
        });
        let mut merged = RelationshipBaseline::default();
        merged
            .merge_all([&unscoped, &other, &scoped, &scoped])
            .unwrap();
        assert_eq!(merged.pairs.len(), 2);
        assert_eq!(merged.pairs[0], scoped.pairs[0]);
        assert_eq!(merged.pairs[1], other.pairs[0]);
        assert_eq!(merged.relations, scoped.relations);
    }

    #[test]
    fn baseline_merge_rejects_changed_state_and_conflicting_scopes() {
        let original = baseline(Some(ConnectorScope {
            namespace: "prod".into(),
            source: "aws".into(),
        }));
        for case in 0..6 {
            let mut changed = original.clone();
            match case {
                0 => changed.pairs[0].live.clear(),
                1 => changed.relations[0].live[0].version += 1,
                2 => changed.pairs[0].live[0].scope.as_mut().unwrap().source = "gcp".into(),
                3 => changed.relations[0].live[0].scope.as_mut().unwrap().source = "gcp".into(),
                5 => changed.relations[0].versions.push(
                    crate::traits::relationship_timeline::VersionState {
                        target_chain_id: Uuid::new_v4(),
                        properties: serde_json::Map::new(),
                    },
                ),
                _ => changed.pairs[0].versions.push(serde_json::Map::new()),
            }
            assert!(RelationshipBaseline::default()
                .merge_all([&original, &changed])
                .is_err());
        }
    }

    fn record() -> EntityVersionRecord {
        EntityVersionRecord {
            uuid: Uuid::new_v4(),
            chain_id: Uuid::new_v4(),
            version: 1,
            is_latest: true,
            entity_type: "Service".into(),
            name: "api".into(),
            namespace: "prod".into(),
            source: None,
            identity_hash: None,
            identity_hashes: vec![],
            structural_hash: None,
            valid_from: None,
            valid_to: None,
            deleted_at: None,
            last_seen_at: None,
            last_transition_at: None,
            sync_generation: None,
            collections: vec![],
            merged_into: None,
            embedding: Some(crate::traits::graph_backend::GraphEmbedding {
                model: "unneeded-vector-model".into(),
                values: vec![1.0; 1536],
            }),
            stored: json!({
                "primary_key_properties": ["id"], "prop_id": "service-123", "property_type_id":"s",
                "prop_payload": "unneeded-source-payload",
            })
            .as_object()
            .unwrap()
            .clone(),
        }
    }

    #[test]
    fn target_keeps_identity_without_vectors_or_unrelated_properties() {
        let record = record();
        let chain = record.chain_id;
        let target = RelationshipTarget::from(record);
        assert_eq!(target.chain_id, chain);
        assert_eq!(target.name, "api");
        assert_eq!(target.namespace, "prod");
        assert_eq!(target.entity_type, "Service");
        assert_eq!(target.single_string_identity(), Some(("id", "service-123")));
        assert_eq!(target.key_groups.len(), 1);
        assert_eq!(target.key_groups[0].components[0].type_tag, "s");
        assert!(!format!("{target:?}").contains("unneeded"));
    }

    #[test]
    fn target_never_treats_a_composite_or_malformed_key_as_scalar_identity() {
        for keys in [json!(["id", "region"]), json!([]), json!("id"), json!([3])] {
            let mut record = record();
            record.stored.insert("primary_key_properties".into(), keys);
            assert!(RelationshipTarget::from(record)
                .single_string_identity()
                .is_none());
        }
        for kind in ["j", "bl", "u", "ts", "unknown"] {
            let mut record = record();
            record.stored.insert("property_type_id".into(), json!(kind));
            assert!(RelationshipTarget::from(record)
                .single_string_identity()
                .is_none());
        }
        for value in [json!(null), json!(123), json!(["service-123"])] {
            let mut record = record();
            record.stored.insert("prop_id".into(), value);
            assert!(RelationshipTarget::from(record)
                .single_string_identity()
                .is_none());
        }
    }

    #[test]
    fn target_carries_an_integer_key_as_a_typed_group() {
        let mut record = record();
        record.stored.insert("prop_id".into(), json!(443));
        record.stored.insert("property_type_id".into(), json!("i"));
        let target = RelationshipTarget::from(record);
        assert!(target.single_string_identity().is_none());
        let component = target.key_groups[0].single().unwrap();
        assert_eq!(
            (component.type_tag.as_str(), component.value.as_str()),
            ("i", "443")
        );
    }

    #[test]
    fn target_carries_a_complete_composite_and_alternative_groups() {
        let mut record = record();
        record
            .stored
            .insert("primary_key_properties".into(), json!(["id", "region"]));
        record
            .stored
            .insert("prop_region".into(), json!("us-east-1"));
        record
            .stored
            .insert("property_type_region".into(), json!("s"));
        record.stored.insert("prop_arn".into(), json!("arn:aws:x"));
        record.stored.insert("property_type_arn".into(), json!("s"));
        record.stored.insert("prop_number".into(), json!(7));
        record
            .stored
            .insert("property_type_number".into(), json!("i"));
        record.stored.insert(
            "additional_key_properties".into(),
            json!(
                serde_json::to_string(&vec![vec!["arn"], vec!["number"], vec!["missing"]]).unwrap()
            ),
        );
        let target = RelationshipTarget::from(record);
        let groups: Vec<Vec<&str>> = target
            .key_groups
            .iter()
            .map(|g| g.components.iter().map(|c| c.property.as_str()).collect())
            .collect();
        assert_eq!(
            groups,
            vec![vec!["id", "region"], vec!["arn"], vec!["number"]]
        );
        // The composite is never presented as a scalar identity; the string alternative is.
        assert_eq!(target.single_string_identity(), Some(("arn", "arn:aws:x")));
    }
}

/// Frozen target partition, committed before summary evidence is read.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryManifest {
    pub collections: Vec<crate::pipeline::CollectionOutcome>,
    pub as_of: DateTime<Utc>,
    pub chain_ids: Vec<Uuid>,
    pub batch_size: usize,
}
impl SummaryManifest {
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=256).contains(&self.batch_size)
            || self.chain_ids.iter().any(Uuid::is_nil)
            || self.chain_ids.windows(2).any(|ids| ids[0] >= ids[1])
            || self.chain_ids.len().div_ceil(self.batch_size) >= u32::MAX as usize
        {
            return Err("invalid summary target manifest".into());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SummaryWork {
    Manifest {
        chain_ids: Vec<Uuid>,
        batch_size: usize,
        collections: Vec<crate::pipeline::CollectionOutcome>,
    },
    Refresh {
        chain_ids: Vec<Uuid>,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryBatchOutput {
    pub batch: BatchIdentity,
    pub fingerprint: RequestFingerprint,
    pub scans: Vec<CollectionScan>,
    pub as_of: DateTime<Utc>,
    pub work: SummaryWork,
}
