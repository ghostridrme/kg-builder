//! Complete scoped projections and guarded Community publication.
use crate::{errors::BackendError, models::CommunityNode};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use uuid::Uuid;

pub const REVISION_STRIPES: usize = 32;
pub const MAX_PAGE_SIZE: usize = 1000;
pub const MAX_ENTITY_TEXT_BYTES: usize = 16 * 1024;
pub const MAX_SUMMARY_BYTES: usize = 64 * 1024;
pub const MAX_PARTITIONS: usize = 100_000;
pub const NAME_TEXT_VERSION: &str = "community-name-v1";

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommunityRevision(pub [u64; REVISION_STRIPES]);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommunityState {
    pub namespace: String,
    pub source_revision: CommunityRevision,
    pub active_generation: Option<Uuid>,
    pub publication_revision: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityEntity {
    pub uuid: Uuid,
    pub chain_id: Uuid,
    pub name: String,
    pub entity_type: String,
    pub text: String,
    /// Hash of the exact bounded factual text, never the embedding.
    pub text_hash: String,
    pub valid_until: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityRelationship {
    pub uuid: Uuid,
    pub source_chain_id: Uuid,
    pub target_chain_id: Uuid,
    pub valid_until: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityScope {
    pub chain_id: Uuid,
    pub namespace: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityProjection {
    pub state: CommunityState,
    pub at: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
    pub entities: Vec<CommunityEntity>,
    pub relationships: Vec<CommunityRelationship>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct RelationshipCursor {
    pub source_uuid: Uuid,
    pub edge_uuid: Uuid,
}

/// Cursors advance over examined records, including currently invisible history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityPage<T, C> {
    pub records: Vec<T>,
    pub next: Option<C>,
    pub exhausted: bool,
    pub scanned_rows: usize,
    pub auxiliary_scanned_rows: usize,
    pub next_temporal_boundary: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommunityMember {
    pub entity_uuid: Uuid,
    pub chain_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityMembership {
    pub community_uuid: Uuid,
    pub community_revision: Uuid,
    pub member: CommunityMember,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommunityRead {
    Scopes {
        chain_ids: Vec<Uuid>,
    },
    State {
        namespace: String,
    },
    EntitiesByChains {
        namespace: String,
        at: DateTime<Utc>,
        chain_ids: Vec<Uuid>,
        after_uuid: Option<Uuid>,
        limit: usize,
    },
    Neighbors {
        namespace: String,
        at: DateTime<Utc>,
        chain_ids: Vec<Uuid>,
        limit: usize,
        after_edge_uuid: Option<Uuid>,
    },
    EntityPage {
        namespace: String,
        at: DateTime<Utc>,
        after_uuid: Option<Uuid>,
        limit: usize,
    },
    RelationshipPage {
        namespace: String,
        at: DateTime<Utc>,
        after: Option<RelationshipCursor>,
        limit: usize,
    },
    Memberships {
        namespace: String,
        generation: Uuid,
        chain_ids: Vec<Uuid>,
        limit: usize,
    },
    Members {
        namespace: String,
        generation: Uuid,
        community_uuid: Uuid,
        after_chain_id: Option<Uuid>,
        limit: usize,
    },
    Community {
        namespace: String,
        generation: Uuid,
        community_uuid: Uuid,
    },
}
impl CommunityRead {
    pub fn namespace(&self) -> &str {
        match self {
            Self::Scopes { .. } => "",
            Self::State { namespace }
            | Self::EntitiesByChains { namespace, .. }
            | Self::Neighbors { namespace, .. }
            | Self::EntityPage { namespace, .. }
            | Self::RelationshipPage { namespace, .. }
            | Self::Memberships { namespace, .. }
            | Self::Members { namespace, .. }
            | Self::Community { namespace, .. } => namespace,
        }
    }
    pub fn validate(&self, org: &str) -> Result<(), BackendError> {
        if let Self::Scopes { chain_ids } = self {
            if org.trim().is_empty() {
                return Err(invalid("organization is required"));
            }
            return unique_ids(chain_ids, MAX_PAGE_SIZE);
        }
        scope(org, self.namespace())?;
        match self {
            Self::Scopes { chain_ids } => unique_ids(chain_ids, MAX_PAGE_SIZE)?,
            Self::State { .. } => {}
            Self::EntitiesByChains {
                chain_ids,
                limit,
                after_uuid,
                ..
            } => {
                unique_ids(chain_ids, MAX_PAGE_SIZE)?;
                page_limit(*limit)?;
                optional_uuid(*after_uuid)?;
            }
            Self::Neighbors {
                chain_ids,
                limit,
                after_edge_uuid,
                ..
            } => {
                unique_ids(chain_ids, MAX_PAGE_SIZE)?;
                page_limit(*limit)?;
                optional_uuid(*after_edge_uuid)?;
            }
            Self::EntityPage {
                after_uuid, limit, ..
            } => {
                page_limit(*limit)?;
                optional_uuid(*after_uuid)?;
            }
            Self::RelationshipPage { after, limit, .. } => {
                page_limit(*limit)?;
                if let Some(cursor) = after {
                    uuid(cursor.source_uuid)?;
                    uuid(cursor.edge_uuid)?;
                }
            }
            Self::Memberships {
                generation,
                chain_ids,
                limit,
                ..
            } => {
                uuid(*generation)?;
                page_limit(*limit)?;
                unique_ids(chain_ids, MAX_PAGE_SIZE)?;
            }
            Self::Members {
                generation,
                community_uuid,
                after_chain_id,
                limit,
                ..
            } => {
                uuid(*generation)?;
                uuid(*community_uuid)?;
                optional_uuid(*after_chain_id)?;
                page_limit(*limit)?;
            }
            Self::Community {
                generation,
                community_uuid,
                ..
            } => {
                uuid(*generation)?;
                uuid(*community_uuid)?;
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommunityReadResult {
    Scopes(Vec<CommunityScope>),
    State(CommunityState),
    Entities(CommunityPage<CommunityEntity, Uuid>),
    Relationships(CommunityPage<CommunityRelationship, RelationshipCursor>),
    Memberships(CommunityPage<CommunityMembership, Uuid>),
    Members(CommunityPage<CommunityMember, Uuid>),
    Community(Option<StoredCommunity>),
    Neighbors(CommunityNeighborhood),
}

/// Definitions and membership fragments are independently bounded; a single
/// community can span many receipted partitions without one giant transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityNeighborhood {
    pub next: Option<Uuid>,
    pub entities: Vec<CommunityEntity>,
    pub relationships: Vec<CommunityRelationship>,
    pub scanned_rows: usize,
    pub auxiliary_scanned_rows: usize,
    pub exhausted: bool,
    pub next_temporal_boundary: Option<DateTime<Utc>>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityDefinition {
    pub node: CommunityNode,
    pub revision: Uuid,
    pub expected_member_count: u64,
    pub source_hash: String,
    pub projected_at: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredCommunity {
    pub generation: Uuid,
    pub dirty: bool,
    pub definition: CommunityDefinition,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityMembershipChunk {
    pub community_uuid: Uuid,
    pub members: Vec<CommunityMember>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityPartition {
    pub definitions: Vec<CommunityDefinition>,
    pub memberships: Vec<CommunityMembershipChunk>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BeginCommunityGeneration {
    pub generation: Uuid,
    pub expected_state: CommunityState,
    pub projected_at: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
    /// Exact hashes of ordered partition payloads, checked on each staged write.
    pub partition_hashes: Vec<String>,
    pub community_count: u64,
    pub member_count: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageCommunityPartition {
    pub namespace: String,
    pub generation: Uuid,
    pub index: usize,
    pub partition: CommunityPartition,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishCommunityGeneration {
    pub generation: Uuid,
    pub expected_state: CommunityState,
    pub publication_revision: Uuid,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuardedCommunityWrite {
    pub expected_revision: Uuid,
    pub definition: CommunityDefinition,
    pub members: Vec<CommunityMember>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateCommunities {
    pub publication_revision: Uuid,
    pub generation: Uuid,
    pub expected_state: CommunityState,
    pub communities: Vec<GuardedCommunityWrite>,
}

/// Complete typed source values participate in factual evidence and its cache key.
pub fn entity_text(
    name: &str,
    entity_type: &str,
    summary: &str,
    stored: &crate::traits::GraphProperties,
) -> Result<String, BackendError> {
    let properties = crate::traits::property_codec::read_properties(stored)
        .map_err(BackendError::Deserialization)?;
    let ordered: std::collections::BTreeMap<_, _> = properties.into_iter().collect();
    let mut text = format!("{entity_type}: {name}\n{summary}");
    if !ordered.is_empty() {
        text.push_str("\nproperties: ");
        text.push_str(
            &serde_json::to_string(&ordered)
                .map_err(|_| invalid("invalid Community source properties"))?,
        );
    }
    if name.trim().is_empty() || entity_type.trim().is_empty() || text.len() > MAX_ENTITY_TEXT_BYTES
    {
        return Err(invalid("Community entity factual text exceeds its bounds"));
    }
    Ok(text)
}

pub fn text_hash(text: &str) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

pub fn partition_hash(partition: &CommunityPartition) -> Result<String, BackendError> {
    let bytes =
        serde_json::to_vec(partition).map_err(|_| invalid("invalid community partition"))?;
    use sha2::{Digest, Sha256};
    Ok(format!("{:x}", Sha256::digest(bytes)))
}
impl CommunityState {
    pub fn validate(&self, org: &str) -> Result<(), BackendError> {
        scope(org, &self.namespace)?;
        optional_uuid(self.active_generation)?;
        optional_uuid(self.publication_revision)?;
        if self.active_generation.is_some() != self.publication_revision.is_some()
            || self.source_revision.0.iter().any(|n| *n > i64::MAX as u64)
        {
            return Err(invalid("invalid community state"));
        }
        Ok(())
    }
}
impl CommunityDefinition {
    pub fn validate(&self, org: &str, namespace: &str) -> Result<(), BackendError> {
        self.node
            .validate()
            .map_err(|_| invalid("invalid community node"))?;
        if self.node.org_id != org
            || self.node.namespace != namespace
            || self.node.summary.trim().is_empty()
            || self.node.summary.len() > MAX_SUMMARY_BYTES
            || self.node.name.len() > MAX_ENTITY_TEXT_BYTES
            || self.expected_member_count == 0
            || self.expected_member_count > i64::MAX as u64
            || self.node.name_embedding.is_none()
        {
            return Err(invalid("invalid community definition"));
        }
        uuid(self.revision)?;
        hash(&self.source_hash)?;
        interval(self.projected_at, self.valid_until)
    }
}
impl BeginCommunityGeneration {
    pub fn validate(&self, org: &str) -> Result<(), BackendError> {
        self.expected_state.validate(org)?;
        uuid(self.generation)?;
        interval(self.projected_at, self.valid_until)?;
        if self.partition_hashes.len() > MAX_PARTITIONS
            || self.community_count > i64::MAX as u64
            || self.member_count > i64::MAX as u64
            || (self.community_count == 0) != (self.member_count == 0)
            || (self.community_count > 0 && self.partition_hashes.is_empty())
        {
            return Err(invalid("invalid community generation manifest"));
        }
        for value in &self.partition_hashes {
            hash(value)?;
        }
        Ok(())
    }
}
impl StageCommunityPartition {
    pub fn validate(&self, org: &str) -> Result<(), BackendError> {
        scope(org, &self.namespace)?;
        uuid(self.generation)?;
        if self.index >= MAX_PARTITIONS
            || self.partition.definitions.len() + self.partition.memberships.len() > MAX_PAGE_SIZE
            || (self.partition.definitions.is_empty() && self.partition.memberships.is_empty())
        {
            return Err(invalid("invalid community partition"));
        }
        let mut ids = BTreeSet::new();
        for definition in &self.partition.definitions {
            definition.validate(org, &self.namespace)?;
            if !ids.insert(definition.node.uuid) {
                return Err(invalid("duplicate community definition"));
            }
        }
        let mut members = BTreeSet::new();
        for chunk in &self.partition.memberships {
            uuid(chunk.community_uuid)?;
            validate_members(&chunk.members)?;
            for member in &chunk.members {
                if !members.insert((chunk.community_uuid, member.chain_id)) {
                    return Err(invalid("duplicate community membership"));
                }
            }
        }
        if members.len() > MAX_PAGE_SIZE {
            return Err(invalid("community membership partition exceeds limit"));
        }
        Ok(())
    }
}
impl PublishCommunityGeneration {
    pub fn validate(&self, org: &str) -> Result<(), BackendError> {
        self.expected_state.validate(org)?;
        uuid(self.generation)?;
        uuid(self.publication_revision)
    }
}
impl UpdateCommunities {
    pub fn validate(&self, org: &str) -> Result<(), BackendError> {
        self.expected_state.validate(org)?;
        uuid(self.publication_revision)?;
        if Some(self.publication_revision) == self.expected_state.publication_revision {
            return Err(invalid("Community publication revision must advance"));
        }
        uuid(self.generation)?;
        if self.expected_state.active_generation != Some(self.generation)
            || self.communities.is_empty()
            || self.communities.len() > MAX_PAGE_SIZE
        {
            return Err(invalid("invalid incremental community update"));
        }
        let mut ids = BTreeSet::new();
        let mut total = 0;
        for write in &self.communities {
            uuid(write.expected_revision)?;
            write
                .definition
                .validate(org, &self.expected_state.namespace)?;
            validate_members(&write.members)?;
            if !ids.insert(write.definition.node.uuid)
                || write.members.len() as u64 != write.definition.expected_member_count
            {
                return Err(invalid("invalid complete community membership"));
            }
            total += write.members.len();
        }
        if total > MAX_PAGE_SIZE {
            return Err(invalid(
                "incremental community membership exceeds atomic limit",
            ));
        }
        Ok(())
    }
}
fn validate_members(members: &[CommunityMember]) -> Result<(), BackendError> {
    if members.is_empty() || members.len() > MAX_PAGE_SIZE {
        return Err(invalid("invalid community member count"));
    }
    let mut chains = BTreeSet::new();
    let mut versions = BTreeSet::new();
    for member in members {
        uuid(member.entity_uuid)?;
        uuid(member.chain_id)?;
        if !chains.insert(member.chain_id) || !versions.insert(member.entity_uuid) {
            return Err(invalid("duplicate community member"));
        }
    }
    Ok(())
}
fn unique_ids(values: &[Uuid], max: usize) -> Result<(), BackendError> {
    if values.is_empty()
        || values.len() > max
        || values.iter().any(Uuid::is_nil)
        || values.iter().collect::<BTreeSet<_>>().len() != values.len()
    {
        Err(invalid("invalid community identifiers"))
    } else {
        Ok(())
    }
}
fn scope(org: &str, namespace: &str) -> Result<(), BackendError> {
    if org.trim().is_empty() || namespace.trim().is_empty() {
        Err(invalid("community scope is required"))
    } else {
        Ok(())
    }
}
fn page_limit(value: usize) -> Result<(), BackendError> {
    if value == 0 || value > MAX_PAGE_SIZE {
        Err(invalid("community page exceeds limit"))
    } else {
        Ok(())
    }
}
fn uuid(value: Uuid) -> Result<(), BackendError> {
    if value.is_nil() {
        Err(invalid("nil community identifier"))
    } else {
        Ok(())
    }
}
fn optional_uuid(value: Option<Uuid>) -> Result<(), BackendError> {
    value.map_or(Ok(()), uuid)
}
fn hash(value: &str) -> Result<(), BackendError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        Err(invalid("invalid community content hash"))
    } else {
        Ok(())
    }
}
fn interval(at: DateTime<Utc>, until: Option<DateTime<Utc>>) -> Result<(), BackendError> {
    if until.is_some_and(|end| end <= at) {
        Err(invalid("empty community coverage"))
    } else {
        Ok(())
    }
}
fn invalid(message: &str) -> BackendError {
    BackendError::Query(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn revision_and_scope_requests_reject_ambiguous_or_unbounded_input() {
        assert!(CommunityRead::Scopes {
            chain_ids: vec![Uuid::new_v4(); 2]
        }
        .validate("org")
        .is_err());
        let mut state = CommunityState {
            namespace: "prod".into(),
            source_revision: CommunityRevision::default(),
            active_generation: Some(Uuid::new_v4()),
            publication_revision: None,
        };
        assert!(state.validate("org").is_err());
        state.publication_revision = Some(Uuid::new_v4());
        assert!(state.validate("org").is_ok());
        state.source_revision.0[31] = u64::MAX;
        assert!(state.validate("org").is_err());
    }
    #[test]
    fn complete_generation_manifest_supports_giant_membership_without_giant_partition() {
        let state = CommunityState {
            namespace: "prod".into(),
            source_revision: CommunityRevision::default(),
            active_generation: None,
            publication_revision: None,
        };
        let mut generation = BeginCommunityGeneration {
            generation: Uuid::new_v4(),
            expected_state: state,
            projected_at: Utc::now(),
            valid_until: None,
            partition_hashes: vec![text_hash("part"); 101],
            community_count: 1,
            member_count: 100_000,
        };
        assert!(generation.validate("org").is_ok());
        generation.partition_hashes[100] = "bad".into();
        assert!(generation.validate("org").is_err());
        let uuid = Uuid::new_v4();
        assert!(validate_members(&vec![
            CommunityMember {
                entity_uuid: uuid,
                chain_id: uuid
            };
            MAX_PAGE_SIZE + 1
        ])
        .is_err());
    }
    #[test]
    fn partition_hash_covers_ordered_members_and_exact_version() {
        let member = CommunityMember {
            entity_uuid: Uuid::new_v4(),
            chain_id: Uuid::new_v4(),
        };
        let mut partition = CommunityPartition {
            definitions: vec![],
            memberships: vec![CommunityMembershipChunk {
                community_uuid: Uuid::new_v4(),
                members: vec![member],
            }],
        };
        let hash = partition_hash(&partition).unwrap();
        partition.memberships[0].members[0].entity_uuid = Uuid::new_v4();
        assert_ne!(hash, partition_hash(&partition).unwrap());
        assert_eq!(
            text_hash("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
