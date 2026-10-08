//! Durable records of reference applications that could not be resolved, so a
//! target that appears later can find the sources waiting on its typed token.
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::errors::BackendError;
use crate::traits::EntityVersionRecord;

/// One unresolved application of a source slot to a typed key token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnresolvedReferenceEntry {
    /// `<type_tag>:<canonical text>` the source referenced.
    pub token: String,
    /// Why it stayed unresolved, such as `target-not-found` or `partial-key`.
    pub reason: String,
    pub snapshot_id: Option<Uuid>,
    pub recorded_at: DateTime<Utc>,
}

impl UnresolvedReferenceEntry {
    pub fn validate(&self) -> Result<(), BackendError> {
        if self.token.trim().is_empty()
            || self.token.len() > 4096
            || self.reason.trim().is_empty()
            || self.reason.len() > 64
            || self.snapshot_id.is_some_and(|id| id.is_nil())
        {
            return Err(BackendError::Query(
                "invalid unresolved reference entry".into(),
            ));
        }
        Ok(())
    }
}

/// A stored unresolved application, read back by token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnresolvedReferenceRecord {
    pub source_chain_id: Uuid,
    /// Stable owner slot (`EntityType.path`), the unit refresh works on.
    pub slot: String,
    pub entry: UnresolvedReferenceEntry,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct UnresolvedReferenceCursor {
    pub source_chain_id: Uuid,
    pub slot: String,
    pub token: String,
}

impl From<&UnresolvedReferenceRecord> for UnresolvedReferenceCursor {
    fn from(record: &UnresolvedReferenceRecord) -> Self {
        Self {
            source_chain_id: record.source_chain_id,
            slot: record.slot.clone(),
            token: record.entry.token.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnresolvedReferenceQuery {
    pub tokens: Vec<String>,
    pub after: Option<UnresolvedReferenceCursor>,
    pub limit: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnresolvedReferencePage {
    pub records: Vec<UnresolvedReferenceRecord>,
    pub next_after: Option<UnresolvedReferenceCursor>,
}

/// Bounded scan of live source entities affected by one reference rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceRuleSourceQuery {
    /// Connector or producer that owns the source observations.
    pub source: String,
    /// Optional namespace applicability of the rule.
    pub namespace: Option<String>,
    /// Source entity type named by the mapping.
    pub entity_type: String,
    /// Exclusive stable chain cursor.
    pub after_chain: Option<Uuid>,
    /// Maximum records in the returned page.
    pub limit: usize,
}

impl ReferenceRuleSourceQuery {
    pub fn validate(&self, org_id: &str) -> Result<(), BackendError> {
        if org_id.trim().is_empty()
            || self.source.trim().is_empty()
            || self.entity_type.trim().is_empty()
            || self
                .namespace
                .as_ref()
                .is_some_and(|value| value.trim().is_empty())
            || self.after_chain.is_some_and(|value| value.is_nil())
            || !(1..=crate::traits::graph_reads::MAX_LOOKUP_KEYS).contains(&self.limit)
        {
            return Err(BackendError::Query(
                "invalid reference-rule source query".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ReferenceRuleSourcePage {
    /// Live source entity heads in chain order.
    pub records: Vec<EntityVersionRecord>,
    /// Last returned chain when another page exists.
    pub next_after: Option<Uuid>,
}
