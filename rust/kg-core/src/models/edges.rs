//! Semantic relationships and structural links between graph records.

use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::models::property_value::PropertyValue;

/// Confidence defaults and the LLM ceiling; not enforced by this model.
pub const CONFIDENCE_DECLARED: f32 = 1.0;
pub const CONFIDENCE_HEURISTIC: f32 = 0.9;
pub const CONFIDENCE_LLM_MAX: f32 = 0.7;

/// The generic relationship name carried by deterministically discovered edges
/// (reference and heuristic) that no source, profile or model gave a meaning.
/// The physical Neo4j type is always `RELATES_TO`; for a generic edge the `name`
/// property equals it too. Optional semantic naming replaces only the `name`
/// property, never the physical type, and never this edge's trusted identity.
pub const GENERIC_RELATIONSHIP_NAME: &str = "RELATES_TO";

/// How a relationship observation was obtained, independent of its model/provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationshipOrigin {
    Declared,
    Reference,
    Fact,
}

/// Durable cause when cancellation is performed without a source snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CancellationContext {
    Batch {
        batch: crate::traits::BatchIdentity,
    },
    Merge {
        loser_chain_id: Uuid,
        winner_chain_id: Uuid,
        effective_at: DateTime<Utc>,
    },
}

impl CancellationContext {
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Batch { batch } if batch.run_id.is_nil() => {
                Err("cancellation batch requires a run identifier".into())
            }
            Self::Merge {
                loser_chain_id,
                winner_chain_id,
                ..
            } if loser_chain_id.is_nil()
                || winner_chain_id.is_nil()
                || loser_chain_id == winner_chain_id =>
            {
                Err("cancellation merge requires distinct chain identifiers".into())
            }
            _ => Ok(()),
        }
    }
}

/// One version of a relationship; endpoint pairs may contain independent lineages.
/// Where a reference-discovered relationship came from and what it proved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceEvidence {
    /// Proven target-component to exact source-path correspondence. Absent when
    /// the producer cannot establish a reusable mapping (e.g. inherited scope).
    pub component_paths: Option<std::collections::BTreeMap<String, String>>,
    /// Entity observation that owns this reference. This differs from the
    /// stored graph source when guidance reverses the relationship direction.
    pub observing_chain_id: Uuid,
    /// Namespace of the observing entity. Ownership is scoped independently
    /// from a cross-namespace target allowed by policy.
    pub observing_namespace: String,
    /// `EntityType.path.without.indexes` — the slot whose cardinality and
    /// retirement this edge follows.
    pub slot: String,
    /// Exact source path including array indexes; evidence, never identity.
    pub location: String,
    /// The complete primary or alternative key group of the target that matched.
    pub target_key_group: Vec<String>,
    /// Canonical typed tokens whose candidate set justified this decision.
    /// Storage uses them as a reverse dependency index when another target
    /// later begins carrying the same identity value.
    #[serde(default)]
    pub reference_tokens: Vec<String>,
    /// Every candidate version this confirmation depended on — the selected
    /// target first, then rejected competitors. A commit requires each to
    /// still be the latest version; otherwise the evidence is refreshed.
    pub read_set: Vec<ReadVersion>,
    /// The evidence-backed model decision that confirmed a reference the
    /// deterministic pass could not. Audit provenance only: never part of the
    /// relationship's semantic content, hash or embedding. Absent for
    /// deterministic confirmations and for edges stored before decisions were recorded.
    #[serde(default)]
    pub decision: Option<crate::runtime::reference_resolution::ReferenceDecisionAudit>,
}

/// One version a reference decision was read against.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadVersion {
    pub chain_id: Uuid,
    pub version_uuid: Uuid,
    pub version: u32,
    /// The version's observation clock when its properties were read as
    /// evidence. Volatile properties can change without a new version, so a
    /// commit that used them also fences later observations of this version.
    /// Absent for deterministic confirmations, which use identity keys only.
    #[serde(default)]
    pub observed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityEdge {
    #[serde(default)]
    pub time_evidence: Option<super::RelationshipTimeEvidence>,
    pub uuid: Uuid,
    pub chain_id: Uuid,
    /// Trusted directed identity, including configured typed keys and reference role.
    pub identity_hash: Option<String>,
    /// Explicit single-target slot; absent unless the schema requests exclusivity.
    pub cardinality_key: Option<String>,
    pub origin: RelationshipOrigin,
    /// Source that supplied this observation and its relationship schema.
    pub producer_source: String,
    pub org_id: String,
    pub source_chain_id: Uuid,
    pub target_chain_id: Uuid,
    pub name: String,
    /// The name this edge was discovered with, set only when optional semantic
    /// naming replaced `name` in this run. Relationship resolution computes the
    /// identity hash, cardinality key and identifying properties from it, so a
    /// labelled edge keeps the lineage of the generic edge it came from. Never
    /// serialized or stored: a later observation is generic again and inherits
    /// the stored label through that unchanged identity.
    #[serde(default, skip)]
    pub identity_name: Option<String>,
    pub description: String,
    pub all_properties: IndexMap<String, PropertyValue>,
    /// Absent for a relationship explicitly declared by the source.
    pub discovered_by: Option<String>,
    pub resolved_by: Option<String>,
    pub source_property: Option<String>,
    pub target_identity_field: Option<String>,
    /// Stable owner slot, exact evidence location and the complete target key
    /// group a reference-discovered edge was confirmed against (R2/R3). Several
    /// locations of one slot support one edge; an index locates evidence only.
    /// `None` for declared, child and free-text relationships.
    pub reference_evidence: Option<ReferenceEvidence>,
    pub confidence: f32,
    pub justification: Option<String>,
    pub first_seen_snapshot_id: Option<Uuid>,
    pub last_seen_snapshot_id: Option<Uuid>,
    /// Latest source capture time; independent of when the relationship became true.
    pub last_seen_at: Option<DateTime<Utc>>,
    /// Last observation generation used by deletion sweeps.
    pub sync_generation: Option<u64>,
    /// Source-effective start; `created_at` records ingestion time.
    pub valid_from: DateTime<Utc>,
    /// Exclusive end when superseded or invalidated.
    pub valid_to: Option<DateTime<Utc>>,
    /// A cancelled future interval never becomes effective; its original bounds remain intact.
    pub cancelled_at: Option<DateTime<Utc>>,
    pub cancellation_snapshot_id: Option<Uuid>,
    pub cancellation_context: Option<CancellationContext>,
    pub version: u32,
    pub is_latest: bool,
    pub previous_version_uuid: Option<Uuid>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub deleted_by: Option<String>,
    pub deletion_reason: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl EntityEdge {
    /// Checks for no end, deletion or cancellation; does not consult the clock.
    pub fn is_valid(&self) -> bool {
        self.cancelled_at.is_none() && self.deleted_at.is_none() && self.valid_to.is_none()
    }

    /// Start-inclusive with exclusive ends; cancelled schedules are never effective.
    pub fn is_valid_at(&self, as_of: DateTime<Utc>) -> bool {
        self.cancelled_at.is_none()
            && self.valid_from <= as_of
            && self.valid_to.is_none_or(|vt| as_of < vt)
            && self.deleted_at.is_none_or(|dt| as_of < dt)
    }
}

/// MENTIONS provenance from a snapshot to the exact observed entity version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEdge {
    pub uuid: Uuid,
    pub org_id: String,
    pub snapshot_uuid: Uuid,
    pub entity_uuid: Uuid,
    pub entity_chain_id: Uuid,
    pub observed_at: DateTime<Utc>,
}

impl SnapshotEdge {
    pub fn validate(&self) -> Result<(), String> {
        validate_link(
            self.uuid,
            &self.org_id,
            self.snapshot_uuid,
            self.entity_uuid,
        )?;
        if self.entity_chain_id.is_nil() || self.entity_chain_id == self.snapshot_uuid {
            return Err("snapshot evidence requires a valid entity chain".into());
        }
        Ok(())
    }
}

/// HAS_MEMBER from a community to an entity version or another community.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityEdge {
    pub uuid: Uuid,
    pub org_id: String,
    pub community_uuid: Uuid,
    pub member_uuid: Uuid,
    pub created_at: DateTime<Utc>,
}

impl CommunityEdge {
    pub fn validate(&self) -> Result<(), String> {
        validate_link(
            self.uuid,
            &self.org_id,
            self.community_uuid,
            self.member_uuid,
        )?;
        Ok(())
    }
}

/// HAS_EPISODE from a saga to a source snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HasSnapshotEdge {
    pub ordinal: u64,
    pub uuid: Uuid,
    pub org_id: String,
    pub saga_uuid: Uuid,
    pub snapshot_uuid: Uuid,
    pub created_at: DateTime<Utc>,
}

impl HasSnapshotEdge {
    pub fn validate(&self) -> Result<(), String> {
        if self.ordinal == 0 || self.ordinal > i64::MAX as u64 {
            return Err("invalid saga membership ordinal".into());
        }
        validate_link(self.uuid, &self.org_id, self.saga_uuid, self.snapshot_uuid)?;
        Ok(())
    }
}

/// NEXT_EPISODE from a preceding snapshot to its successor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NextSnapshotEdge {
    pub saga_uuid: Uuid,
    pub uuid: Uuid,
    pub org_id: String,
    pub source_snapshot_uuid: Uuid,
    pub target_snapshot_uuid: Uuid,
    pub created_at: DateTime<Utc>,
}

impl NextSnapshotEdge {
    pub fn validate(&self) -> Result<(), String> {
        if self.saga_uuid.is_nil()
            || self.saga_uuid == self.source_snapshot_uuid
            || self.saga_uuid == self.target_snapshot_uuid
        {
            return Err("invalid saga identifier".into());
        }
        validate_link(
            self.uuid,
            &self.org_id,
            self.source_snapshot_uuid,
            self.target_snapshot_uuid,
        )?;
        Ok(())
    }
}

// Endpoint existence, kind, namespace policy and cycles require graph reads.
fn validate_link(uuid: Uuid, org: &str, source: Uuid, target: Uuid) -> Result<(), String> {
    if uuid.is_nil()
        || source.is_nil()
        || target.is_nil()
        || source == target
        || org.trim().is_empty()
    {
        return Err(
            "structural links require an organization and distinct, non-nil endpoints".into(),
        );
    }
    Ok(())
}
