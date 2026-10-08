//! Ownership and scan generations used by scoped absence-based deletion.

use super::input::SnapshotInput;
use super::nodes::SnapshotNode;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The collection a complete full scan owns. Absence-based deletion requires it:
/// a source name alone never authorizes deleting unseen data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectionScope {
    /// Stable, source-independent key of the owned collection, scoped by
    /// organization, namespace, and source. A connector may encode account,
    /// region, and resource kind; core treats the key as opaque.
    pub key: String,
    /// Connector assertion that the request holds every current relationship
    /// this collection owns. Entity coverage does not imply relationship coverage.
    #[serde(default)]
    pub relationships_complete: bool,
}

/// Identity of one owned collection within an organization: the scope a
/// complete scan declares and the unit of absence-based deletion.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CollectionRef {
    pub namespace: String,
    pub source: String,
    pub key: String,
}

impl CollectionRef {
    /// The collection a snapshot declares, if any.
    pub fn of(snapshot: &SnapshotInput) -> Option<Self> {
        snapshot.collection.as_ref().map(|scope| Self {
            namespace: snapshot.namespace.clone(),
            source: snapshot.source.clone(),
            key: scope.key.clone(),
        })
    }

    /// Reject blank parts.
    pub fn validate(&self) -> Result<(), String> {
        for (field, value) in [
            ("namespace", &self.namespace),
            ("source", &self.source),
            ("key", &self.key),
        ] {
            if value.trim().is_empty() {
                return Err(format!("collection {field} must not be blank"));
            }
        }
        Ok(())
    }

    /// Stored membership identifier: the JSON array `[namespace, source, key]`,
    /// unambiguous for any characters the parts contain.
    pub fn member_id(&self) -> String {
        serde_json::to_string(&[&self.namespace, &self.source, &self.key])
            .expect("three strings serialize")
    }

    /// Parse a stored membership identifier.
    pub fn parse_member_id(id: &str) -> Option<Self> {
        let [namespace, source, key]: [String; 3] = serde_json::from_str(id).ok()?;
        Some(Self {
            namespace,
            source,
            key,
        })
    }

    /// Stable identity of the collection's scan record within an organization.
    pub fn scope_id(&self, org_id: &str) -> Uuid {
        Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{org_id}\u{0}{}", self.member_id()).as_bytes(),
        )
    }
}

impl std::fmt::Display for CollectionRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}/{}", self.namespace, self.source, self.key)
    }
}

/// One collection an entity version belongs to, with the generation of the
/// last scan that observed it there. Membership is persisted per version and
/// carried forward; a sweep removes a member only through its own collection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionMembership {
    pub collection: CollectionRef,
    pub generation: u64,
}

/// Graph property holding a version's membership identifiers.
pub const MEMBERS_PROPERTY: &str = "collection_members";
/// Graph property holding the generations aligned with [`MEMBERS_PROPERTY`].
pub const GENERATIONS_PROPERTY: &str = "collection_generations";

impl CollectionMembership {
    /// The membership an observation grants: the snapshot's collection at its
    /// generation. `None` for snapshots that declare no collection.
    pub fn of(snapshot: &SnapshotInput) -> Option<Self> {
        Some(Self {
            collection: CollectionRef::of(snapshot)?,
            generation: snapshot.sync_generation?,
        })
    }

    /// The membership a recorded snapshot grants.
    pub fn of_node(snapshot: &SnapshotNode) -> Option<Self> {
        Some(Self {
            collection: CollectionRef {
                namespace: snapshot.namespace.clone(),
                source: snapshot.source.clone(),
                key: snapshot.collection.as_ref()?.key.clone(),
            },
            generation: snapshot.sync_generation?,
        })
    }

    /// The two aligned list properties that store a membership set.
    /// Generations are stored as signed integers; larger values saturate.
    pub fn to_properties(memberships: &[Self]) -> (serde_json::Value, serde_json::Value) {
        (
            serde_json::Value::Array(
                memberships
                    .iter()
                    .map(|m| serde_json::Value::String(m.collection.member_id()))
                    .collect(),
            ),
            serde_json::Value::Array(
                memberships
                    .iter()
                    .map(|m| {
                        serde_json::Value::from(i64::try_from(m.generation).unwrap_or(i64::MAX))
                    })
                    .collect(),
            ),
        )
    }

    /// Decode the aligned list properties; unreadable identifiers are skipped
    /// and a member without a readable generation counts as generation 0.
    pub fn from_properties(
        members: Option<&serde_json::Value>,
        generations: Option<&serde_json::Value>,
    ) -> Vec<Self> {
        let generations: Vec<u64> = generations
            .and_then(serde_json::Value::as_array)
            .map(|g| g.iter().map(|v| v.as_u64().unwrap_or(0)).collect())
            .unwrap_or_default();
        members
            .and_then(serde_json::Value::as_array)
            .map(|ids| {
                ids.iter()
                    .enumerate()
                    .filter_map(|(i, id)| {
                        Some(Self {
                            collection: CollectionRef::parse_member_id(id.as_str()?)?,
                            generation: generations.get(i).copied().unwrap_or(0),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Merge an observation's membership into a stored set: the collection is
    /// added or its generation advanced; a generation never goes backwards.
    pub fn merge(stored: &[Self], observed: Option<&Self>) -> Vec<Self> {
        let mut merged = stored.to_vec();
        if let Some(observed) = observed {
            match merged
                .iter_mut()
                .find(|m| m.collection == observed.collection)
            {
                Some(existing) => {
                    existing.generation = existing.generation.max(observed.generation)
                }
                None => merged.push(observed.clone()),
            }
        }
        merged
    }
}
