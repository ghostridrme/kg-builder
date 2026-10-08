//! Scoped Saga association, ordered membership and incremental summary contracts.
use crate::{errors::BackendError, models::ThreadNode, runtime::history::SnapshotEvidence};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use uuid::Uuid;

pub const MAX_PAGE_SIZE: usize = 200;
pub const MAX_SUMMARY_BYTES: usize = 64 * 1024;
/// Deepest offset a listing may skip to; matches the graph explorer bound.
pub const MAX_LIST_OFFSET: usize = 100_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ThreadReference {
    Name { name: String },
    Uuid { uuid: Uuid },
}
impl ThreadReference {
    pub fn validate(&self) -> Result<(), BackendError> {
        match self {
            Self::Name { name } if !name.trim().is_empty() => Ok(()),
            Self::Uuid { uuid } if !uuid.is_nil() => Ok(()),
            _ => Err(invalid()),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SagaRead {
    State {
        namespace: String,
        reference: ThreadReference,
    },
    Members {
        namespace: String,
        saga_uuid: Uuid,
        after_ordinal: u64,
        through_ordinal: Option<u64>,
        limit: usize,
        /// Keep only members captured at or before this instant, for historical reads.
        /// Ordinals are unchanged, so pagination stays stable.
        captured_through: Option<DateTime<Utc>>,
    },
    /// Sagas in one namespace, ordered by name then identity, for interactive listing.
    List {
        namespace: String,
        offset: usize,
        limit: usize,
        /// Keep only Sagas whose earliest observation was captured at or before
        /// this instant, so historical paging happens in storage.
        as_of: Option<DateTime<Utc>>,
    },
    Member {
        namespace: String,
        saga_uuid: Uuid,
        snapshot_uuid: Uuid,
    },
    Latest {
        namespace: String,
        saga_uuid: Uuid,
        excluding_snapshot_uuid: Option<Uuid>,
    },
    Context {
        namespace: String,
        saga_uuid: Uuid,
        captured_before: DateTime<Utc>,
        limit: usize,
    },
}
impl SagaRead {
    pub fn namespace(&self) -> &str {
        match self {
            Self::State { namespace, .. }
            | Self::Members { namespace, .. }
            | Self::Member { namespace, .. }
            | Self::Latest { namespace, .. }
            | Self::Context { namespace, .. }
            | Self::List { namespace, .. } => namespace,
        }
    }
    pub fn validate(&self, org: &str) -> Result<(), BackendError> {
        if org.trim().is_empty() || self.namespace().trim().is_empty() {
            return Err(invalid());
        }
        match self {
            Self::State { reference, .. } => reference.validate()?,
            Self::Members {
                saga_uuid,
                after_ordinal,
                through_ordinal,
                limit,
                ..
            } => {
                valid_uuid(*saga_uuid)?;
                valid_limit(*limit)?;
                if *after_ordinal > i64::MAX as u64
                    || through_ordinal.is_some_and(|v| v < *after_ordinal || v > i64::MAX as u64)
                {
                    return Err(invalid());
                }
            }
            Self::Context {
                saga_uuid, limit, ..
            } => {
                valid_uuid(*saga_uuid)?;
                valid_limit(*limit)?;
            }
            Self::List { offset, limit, .. } => {
                valid_limit(*limit)?;
                if *offset > MAX_LIST_OFFSET {
                    return Err(invalid());
                }
            }
            Self::Member {
                saga_uuid,
                snapshot_uuid,
                ..
            } => {
                valid_uuid(*saga_uuid)?;
                valid_uuid(*snapshot_uuid)?;
            }
            Self::Latest {
                saga_uuid,
                excluding_snapshot_uuid,
                ..
            } => {
                valid_uuid(*saga_uuid)?;
                if let Some(id) = excluding_snapshot_uuid {
                    valid_uuid(*id)?;
                }
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SagaMember {
    pub snapshot_uuid: Uuid,
    pub captured_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub ordinal: u64,
    pub previous_snapshot_uuid: Option<Uuid>,
    /// Display name of the member snapshot, when the store returns one; absent
    /// for memberships planned before the snapshot is persisted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_name: Option<String>,
    /// Connector or source label of the member snapshot, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_source: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaMemberPage {
    pub members: Vec<SagaMember>,
    pub truncated: bool,
}
/// One page of a namespace listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaPage {
    pub sagas: Vec<ThreadNode>,
    pub truncated: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SagaReadResult {
    State(Option<ThreadNode>),
    Member(Option<SagaMember>),
    Members(SagaMemberPage),
    Sagas(SagaPage),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadAssociationWrite {
    pub namespace: String,
    pub saga_uuid: Uuid,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub expected_revision: u64,
    pub snapshot_uuid: Uuid,
    pub previous_snapshot_uuid: Option<Uuid>,
    pub membership_uuid: Uuid,
    pub next_uuid: Option<Uuid>,
}
impl ThreadAssociationWrite {
    pub fn validate(&self, org: &str) -> Result<(), BackendError> {
        if org.trim().is_empty()
            || self.namespace.trim().is_empty()
            || self.name.trim().is_empty()
            || self.expected_revision >= i64::MAX as u64
        {
            return Err(invalid());
        }
        for id in [self.saga_uuid, self.snapshot_uuid, self.membership_uuid] {
            valid_uuid(id)?;
        }
        if self.saga_uuid == self.snapshot_uuid
            || self.previous_snapshot_uuid.is_some() != self.next_uuid.is_some()
        {
            return Err(invalid());
        }
        if let Some(id) = self.previous_snapshot_uuid {
            valid_uuid(id)?;
            if id == self.snapshot_uuid || id == self.saga_uuid {
                return Err(invalid());
            }
        }
        if let Some(id) = self.next_uuid {
            valid_uuid(id)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SummaryIncompleteReason {
    DeterministicBudgetExceeded,
    MemberExceedsModelContext,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncompleteSagaSummary {
    pub namespace: String,
    pub saga_uuid: Uuid,
    pub reason: SummaryIncompleteReason,
    pub from_ordinal: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaSummaryWrite {
    /// An incomplete page preserves the accepted brief and coverage under the same guard.
    #[serde(default)]
    pub incomplete_reason: Option<SummaryIncompleteReason>,
    pub namespace: String,
    pub saga_uuid: Uuid,
    pub expected_summary_revision: Option<Uuid>,
    pub previous_summary: String,
    pub previous_supporting_snapshot_uuids: Vec<Uuid>,
    pub revision: Uuid,
    pub after_ordinal: u64,
    pub through_ordinal: u64,
    pub summary: String,
    pub supporting_snapshot_uuids: Vec<Uuid>,
    /// Every snapshot consumed by this page, in membership ordinal order.
    pub evidence: Vec<SnapshotEvidence>,
    pub summarized_at: DateTime<Utc>,
    pub max_captured_at: DateTime<Utc>,
}
impl SagaSummaryWrite {
    pub fn validate(&self, org: &str) -> Result<(), BackendError> {
        if self.incomplete_reason.is_some()
            && (self.summary != self.previous_summary
                || self.supporting_snapshot_uuids != self.previous_supporting_snapshot_uuids)
        {
            return Err(invalid());
        }
        valid_uuid(self.saga_uuid)?;
        valid_uuid(self.revision)?;
        if let Some(id) = self.expected_summary_revision {
            valid_uuid(id)?;
        }
        if org.trim().is_empty()
            || self.namespace.trim().is_empty()
            || (self.incomplete_reason.is_none() && self.summary.trim().is_empty())
            || self.summary.len() > MAX_SUMMARY_BYTES
            || self.through_ordinal <= self.after_ordinal
            || self.through_ordinal > i64::MAX as u64
            || self.through_ordinal - self.after_ordinal != self.evidence.len() as u64
            || self.evidence.len() > MAX_PAGE_SIZE
        {
            return Err(invalid());
        }
        let ids: BTreeSet<_> = self.evidence.iter().map(|v| v.uuid).collect();
        let allowed: BTreeSet<_> = ids
            .iter()
            .copied()
            .chain(self.previous_supporting_snapshot_uuids.iter().copied())
            .collect();
        if self.previous_summary.len() > MAX_SUMMARY_BYTES
            || self.previous_supporting_snapshot_uuids.len() > MAX_PAGE_SIZE
            || (self.incomplete_reason.is_none() && self.supporting_snapshot_uuids.is_empty())
            || self.supporting_snapshot_uuids.len() > MAX_PAGE_SIZE
            || self
                .previous_supporting_snapshot_uuids
                .iter()
                .any(Uuid::is_nil)
        {
            return Err(invalid());
        }
        if ids.len() != self.evidence.len()
            || self.evidence.iter().any(|v| {
                v.uuid.is_nil()
                    || v.org_id != org
                    || v.namespace != self.namespace
                    || v.content.trim().is_empty()
            })
            || self.evidence.iter().map(|v| v.captured_at).max() != Some(self.max_captured_at)
            || self
                .supporting_snapshot_uuids
                .iter()
                .any(|id| !allowed.contains(id))
            || self
                .supporting_snapshot_uuids
                .iter()
                .collect::<BTreeSet<_>>()
                .len()
                != self.supporting_snapshot_uuids.len()
        {
            return Err(invalid());
        }
        if self
            .evidence
            .iter()
            .map(SnapshotEvidence::byte_len)
            .sum::<usize>()
            > 8 * 1024 * 1024
        {
            return Err(invalid());
        }
        Ok(())
    }
}
fn valid_uuid(id: Uuid) -> Result<(), BackendError> {
    if id.is_nil() {
        Err(invalid())
    } else {
        Ok(())
    }
}
fn valid_limit(limit: usize) -> Result<(), BackendError> {
    if limit == 0 || limit > MAX_PAGE_SIZE {
        Err(invalid())
    } else {
        Ok(())
    }
}
fn invalid() -> BackendError {
    BackendError::Query("invalid scoped Saga request or mutation".into())
}

/// Stable identity for a named Saga, shared by admission and scoped creation.
pub fn saga_uuid(org: &str, namespace: &str, name: &str) -> Uuid {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    // Frozen storage identity: public package and Thread naming must not change existing UUIDs.
    hash.update(b"astrolabe-saga-v1\0");
    for part in [org, namespace, name] {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part.as_bytes());
    }
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hash.finalize()[..16]);
    bytes[6] = (bytes[6] & 15) | 0x80;
    bytes[8] = (bytes[8] & 63) | 0x80;
    Uuid::from_bytes(bytes)
}

/// Legacy reference name retained for existing Rust integrations.
pub type SagaReference = ThreadReference;
/// Canonical public name; keep the stable UUID salt for existing stored threads.
pub use saga_uuid as thread_uuid;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn named_identity_is_scoped_and_length_framed() {
        assert_eq!(
            saga_uuid("org", "prod", "incident"),
            saga_uuid("org", "prod", "incident")
        );
        assert_ne!(saga_uuid("a", "bc", "d"), saga_uuid("ab", "c", "d"));
        assert_ne!(
            saga_uuid("org", "prod", "incident"),
            saga_uuid("other", "prod", "incident")
        );
    }
    #[test]
    fn reads_reject_unbounded_pages_and_invalid_scope() {
        for limit in [0, MAX_PAGE_SIZE + 1] {
            assert!(SagaRead::Context {
                namespace: "prod".into(),
                saga_uuid: Uuid::new_v4(),
                captured_before: Utc::now(),
                limit
            }
            .validate("org")
            .is_err());
        }
        assert!(SagaRead::Members {
            namespace: "prod".into(),
            saga_uuid: Uuid::new_v4(),
            after_ordinal: 3,
            through_ordinal: Some(2),
            limit: 1,
            captured_through: None,
        }
        .validate("org")
        .is_err());
        assert!(ThreadReference::Name { name: " ".into() }
            .validate()
            .is_err());
        let list = |offset, limit| SagaRead::List {
            namespace: "prod".into(),
            offset,
            limit,
            as_of: None,
        };
        assert!(list(0, MAX_PAGE_SIZE).validate("org").is_ok());
        assert!(list(MAX_LIST_OFFSET, 1).validate("org").is_ok());
        assert!(list(MAX_LIST_OFFSET + 1, 1).validate("org").is_err());
        assert!(list(0, 0).validate("org").is_err());
        assert!(list(0, MAX_PAGE_SIZE + 1).validate("org").is_err());
        assert!(SagaRead::List {
            namespace: " ".into(),
            offset: 0,
            limit: 1,
            as_of: None,
        }
        .validate("org")
        .is_err());
    }
    #[test]
    fn association_requires_matching_predecessor_link_and_finite_revision() {
        let mut a = ThreadAssociationWrite {
            namespace: "prod".into(),
            saga_uuid: Uuid::new_v4(),
            name: "incident".into(),
            created_at: Utc::now(),
            expected_revision: 0,
            snapshot_uuid: Uuid::new_v4(),
            previous_snapshot_uuid: None,
            membership_uuid: Uuid::new_v4(),
            next_uuid: None,
        };
        a.validate("org").unwrap();
        a.previous_snapshot_uuid = Some(a.snapshot_uuid);
        assert!(a.validate("org").is_err());
        a.previous_snapshot_uuid = Some(Uuid::new_v4());
        a.next_uuid = Some(Uuid::new_v4());
        a.validate("org").unwrap();
        a.expected_revision = i64::MAX as u64;
        assert!(a.validate("org").is_err());
    }
}
