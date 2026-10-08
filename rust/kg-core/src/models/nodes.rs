//! Stored entity versions and source observations.

use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use super::collection::{CollectionMembership, CollectionScope};
use super::input::{SnapshotDataType, SnapshotKind};
use crate::embedding::ComputedEmbedding;
use crate::enums::EntityLifecycle;
use crate::identity::IdentityHash;
use crate::models::property_value::PropertyValue;

/// Versions share a `chain_id`; each version has its own `uuid`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityNode {
    pub uuid: Uuid,
    pub chain_id: Uuid,
    pub org_id: String,
    pub namespace: String,
    pub entity_type: String,
    pub name: String,
    /// Flattened properties after configured field drops.
    pub all_properties: IndexMap<String, PropertyValue>,
    /// Empty for keyless observations; names alone are not authoritative identity.
    pub primary_key_properties: Vec<String>,
    /// Alternative key groups for resolving an existing chain.
    pub additional_key_properties: Vec<Vec<String>>,
    /// Source-key digest, or an opaque token when no authoritative keys exist.
    pub identity_hash: IdentityHash,
    /// Source-reported state; distinct from the system tombstone in `deleted_at`.
    pub lifecycle: EntityLifecycle,
    pub version: u32,
    /// Historical queries use validity windows instead.
    pub is_latest: bool,
    pub previous_version_uuid: Option<Uuid>,
    /// A vector already computed for this entity's content, or the one stored
    /// on the version it resolved against. Persistence reuses it when its
    /// settings and content still match; it is never serialized.
    #[serde(skip)]
    pub embedding: Option<Arc<ComputedEmbedding>>,
    /// Source-effective start, normally the snapshot capture time.
    pub valid_from: DateTime<Utc>,
    pub valid_to: Option<DateTime<Utc>>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub deleted_by: Option<String>,
    pub deletion_reason: Option<String>,
    pub source: String,
    pub extracted_by: String,
    pub resolved_by: Option<String>,
    pub first_seen_snapshot_id: Option<Uuid>,
    pub last_seen_snapshot_id: Option<Uuid>,
    pub last_seen_at: Option<DateTime<Utc>>,
    /// Generation of the last scan that observed the chain, any collection.
    pub sync_generation: Option<u64>,
    /// Collections the continued version belongs to, carried from the stored
    /// version; persistence adds the observing snapshot's own collection.
    #[serde(default)]
    pub collections: Vec<CollectionMembership>,
    #[serde(default)]
    pub labels: Vec<String>,
    /// Labels of the stored version a partial observation continues. Storage keeps
    /// them (a partial payload adds labels, it does not replace them), so the
    /// embedding text renders `labels` merged onto these; the metadata write still
    /// receives only `labels`, the observation itself. Empty for full observations
    /// and new chains.
    #[serde(default)]
    pub inherited_labels: Vec<String>,
    pub tags: IndexMap<String, String>,
    pub summary: Option<String>,
    /// Hash of normalized properties, excluding configured fields.
    pub structural_hash: u64,
    /// Marks degraded LLM processing; this flag alone does not schedule recovery.
    #[serde(default)]
    pub needs_llm_review: bool,
}

impl EntityNode {
    pub fn has_authoritative_keys(&self) -> bool {
        !self.primary_key_properties.is_empty()
    }

    /// Complete alternative groups use the same key encoding as primary identity.
    pub fn additional_identity_hashes(&self) -> Result<Vec<IdentityHash>, String> {
        let mut result = Vec::new();
        let mut groups = std::collections::HashSet::new();
        for group in &self.additional_key_properties {
            let mut ordered = group.clone();
            ordered.sort();
            if ordered.is_empty()
                || ordered.windows(2).any(|pair| pair[0] == pair[1])
                || !groups.insert(ordered)
            {
                return Err("invalid or repeated additional key group".into());
            }
            let pairs: Result<Vec<_>, String> = group
                .iter()
                .map(|key| {
                    if key.trim().is_empty() {
                        return Err("blank additional key property".into());
                    }
                    let value = if key == "name" {
                        Some(PropertyValue::String(self.name.clone()))
                    } else {
                        self.all_properties
                            .get(key)
                            .filter(|v| v.as_identity_key().is_some())
                            .cloned()
                    };
                    value
                        .map(|value| (key.clone(), value))
                        .ok_or_else(|| "incomplete or invalid additional key".into())
                })
                .collect();
            result.push(IdentityHash::compute_values(
                &self.org_id,
                &self.namespace,
                &self.entity_type,
                &pairs?,
            )?);
        }
        Ok(result)
    }

    /// Validates names and key declarations, not key values or the identity hash.
    pub fn validate(&self) -> Result<(), String> {
        super::validate_metadata(&self.tags, &self.labels)?;
        if self.org_id.trim().is_empty() {
            return Err("org_id must not be empty".into());
        }
        if self.namespace.trim().is_empty() {
            return Err("namespace must not be empty".into());
        }
        if self.name.trim().is_empty() {
            return Err("name must not be empty".into());
        }
        if self.entity_type.trim().is_empty() {
            return Err("entity_type must not be empty".into());
        }
        if !self.has_authoritative_keys() && !self.additional_key_properties.is_empty() {
            return Err("keyless entities cannot declare alternative keys".into());
        }
        let mut seen = std::collections::HashSet::new();
        for pk in &self.primary_key_properties {
            if pk.trim().is_empty() {
                return Err("blank primary key property name".into());
            }
            if !seen.insert(pk.as_str()) {
                return Err(format!("duplicate primary key property `{pk}`"));
            }
        }
        Ok(())
    }

    /// Checks for no end or deletion; does not check `is_latest` or the clock.
    pub fn is_valid(&self) -> bool {
        self.deleted_at.is_none() && self.valid_to.is_none()
    }

    /// Start-inclusive; validity end and deletion time are exclusive.
    pub fn is_valid_at(&self, as_of: DateTime<Utc>) -> bool {
        self.valid_from <= as_of
            && self.valid_to.is_none_or(|vt| as_of < vt)
            && self.deleted_at.is_none_or(|dt| as_of < dt)
    }
}

/// Recorded snapshot metadata. `captured_at` is source time; `created_at` is ingestion time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotNode {
    pub uuid: Uuid,
    pub org_id: String,
    pub namespace: String,
    pub name: String,
    /// Human-readable source context, treated as untrusted data.
    #[serde(default)]
    pub source_description: Option<String>,
    pub data_type: SnapshotDataType,
    pub snapshot_kind: SnapshotKind,
    /// Sweep generation scoped by organization, namespace, and source.
    pub sync_generation: Option<u64>,
    /// Connector assertion of full scope coverage; extraction completeness is checked separately.
    pub complete: bool,
    /// Owned collection of a complete full scan; see [`CollectionScope`].
    pub collection: Option<CollectionScope>,
    pub source: String,
    pub content: Option<String>,
    pub captured_at: DateTime<Utc>,
    /// Entity version UUIDs, not chain IDs.
    pub entities: Vec<Uuid>,
    pub entity_edges: Vec<Uuid>,
    pub labels: Vec<String>,
    pub tags: IndexMap<String, String>,
    pub created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests;

/// A group of related entities or communities with a shared summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityNode {
    pub uuid: Uuid,
    pub org_id: String,
    pub namespace: String,
    pub name: String,
    pub labels: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub summary: String,
    /// The community name is embedded; the model tag prevents incompatible comparisons.
    pub name_embedding: Option<crate::traits::graph_backend::GraphEmbedding>,
}

impl CommunityNode {
    pub fn validate(&self) -> Result<(), String> {
        super::validate_group_node(
            self.uuid,
            &self.org_id,
            &self.namespace,
            &self.name,
            &self.labels,
        )?;
        if let Some(embedding) = &self.name_embedding {
            embedding.validate().map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

/// A sequence of source snapshots. Membership and ordering are stored as edges.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadNode {
    #[serde(default)]
    pub summary_incomplete_reason: Option<crate::saga::SummaryIncompleteReason>,
    #[serde(default)]
    pub summary_incomplete_from_ordinal: Option<u64>,
    pub summary_supporting_snapshot_uuids: Vec<Uuid>,
    pub revision: u64,
    pub last_membership_ordinal: u64,
    pub summary_revision: Option<Uuid>,
    pub summary_cursor: u64,
    pub uuid: Uuid,
    pub org_id: String,
    pub namespace: String,
    pub name: String,
    pub labels: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub summary: String,
    pub first_snapshot_uuid: Option<Uuid>,
    pub last_snapshot_uuid: Option<Uuid>,
    /// Ingestion-time cutoff covered by the summary.
    pub last_summarized_at: Option<DateTime<Utc>>,
    /// Maximum source capture time covered; independent of the ingestion clock.
    pub last_summarized_snapshot_captured_at: Option<DateTime<Utc>>,
    /// Earliest capture time among every member, including backfills captured
    /// before the Thread was created. Historical reads treat the Thread as existing
    /// from this instant; absent on Threads stored before it was tracked.
    #[serde(default)]
    pub first_captured_at: Option<DateTime<Utc>>,
}

impl ThreadNode {
    pub fn validate(&self) -> Result<(), String> {
        if self.summary_incomplete_reason.is_some()
            != self.summary_incomplete_from_ordinal.is_some()
            || self.summary_incomplete_from_ordinal.is_some_and(|ordinal| {
                Some(ordinal) != self.summary_cursor.checked_add(1)
                    || ordinal > self.last_membership_ordinal
            })
        {
            return Err("invalid incomplete Saga summary interval".into());
        }
        if self.summary_supporting_snapshot_uuids.len() > crate::saga::MAX_PAGE_SIZE
            || self
                .summary_supporting_snapshot_uuids
                .iter()
                .any(Uuid::is_nil)
            || self
                .summary_supporting_snapshot_uuids
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != self.summary_supporting_snapshot_uuids.len()
            || (self.summary_cursor > 0 && self.summary_revision.is_none())
            || self.revision > i64::MAX as u64
            || self.last_membership_ordinal > i64::MAX as u64
            || self.summary_cursor > self.last_membership_ordinal
            || self.summary_revision.is_some_and(|id| id.is_nil())
        {
            return Err("invalid saga revision or summary coverage".into());
        }
        // A stored summary without its coverage watermark cannot be placed on the
        // observation timeline, so historical reads could leak later evidence.
        if !self.summary.trim().is_empty()
            && (self.summary_revision.is_none()
                || self.last_summarized_at.is_none()
                || self.last_summarized_snapshot_captured_at.is_none())
        {
            return Err("saga summary lacks its coverage watermark".into());
        }
        super::validate_group_node(
            self.uuid,
            &self.org_id,
            &self.namespace,
            &self.name,
            &self.labels,
        )?;
        if self.first_snapshot_uuid.is_some() != self.last_snapshot_uuid.is_some()
            || self
                .first_snapshot_uuid
                .is_some_and(|id| id.is_nil() || id == self.uuid)
            || self
                .last_snapshot_uuid
                .is_some_and(|id| id.is_nil() || id == self.uuid)
        {
            return Err("saga endpoints must both be absent or valid snapshot identifiers".into());
        }
        Ok(())
    }
}

impl EntityNode {
    /// The labels the stored version carries after this observation: inherited
    /// labels first, then the observed ones not already present (the order storage
    /// produces when it unions them).
    pub fn effective_labels(&self) -> Vec<String> {
        let mut labels = self.inherited_labels.clone();
        for label in &self.labels {
            if !labels.contains(label) {
                labels.push(label.clone());
            }
        }
        labels
    }
}
