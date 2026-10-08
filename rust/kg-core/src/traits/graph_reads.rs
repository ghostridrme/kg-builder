//! Typed ingestion reads. Adapters own query text and row decoding; callers
//! never see database rows.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::errors::BackendError;
use crate::models::{CollectionMembership, CollectionRef};
use crate::traits::graph_backend::GraphEmbedding;
use crate::traits::graph_mutation::USER_PROPERTY_PREFIX;

/// Upper bound on keys in one lookup; larger requests must be split by the caller.
pub const MAX_LOOKUP_KEYS: usize = 10_000;

/// Carriers returned per value by `LiveByIdentifyingValue`. Two already
/// mean ambiguity; the margin lets the ownership rule discount members an
/// authoritative scan is about to delete without missing the rest.
pub const MAX_CARRIERS_PER_IDENTIFYING_VALUE: usize = 8;

/// One typed key-value token to look up, with the target namespaces its source
/// observation may link to (`None` under an open namespace policy).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WantedKeyValue {
    pub token: String,
    pub namespaces: Option<Vec<String>>,
    /// Optional target types, applied before the carrier cap.
    #[serde(default)]
    pub target_types: Option<Vec<String>>,
    /// Fold only string-token text for a mapping that explicitly allows it.
    #[serde(default)]
    pub case_insensitive_string: bool,
}

impl WantedKeyValue {
    pub fn exact(token: String, namespaces: Option<Vec<String>>) -> Self {
        Self {
            token,
            namespaces,
            target_types: None,
            case_insensitive_string: false,
        }
    }

    /// Canonical identity used for per-query limits and completeness evidence.
    pub fn request_id(&self) -> String {
        let mut normalized = self.clone();
        if let Some(values) = &mut normalized.namespaces {
            values.sort();
            values.dedup();
        }
        if let Some(values) = &mut normalized.target_types {
            values.sort();
            values.dedup();
        }
        serde_json::to_string(&normalized).expect("wanted key value serializes")
    }

    pub fn matches_target(
        &self,
        target: &crate::runtime::stage_output::RelationshipTarget,
    ) -> bool {
        if self
            .namespaces
            .as_ref()
            .is_some_and(|namespaces| !namespaces.contains(&target.namespace))
            || self
                .target_types
                .as_ref()
                .is_some_and(|types| !types.contains(&target.entity_type))
        {
            return false;
        }
        target.key_value_tokens().into_iter().any(|candidate| {
            candidate == self.token
                || (self.case_insensitive_string
                    && candidate.starts_with("s:")
                    && self.token.starts_with("s:")
                    && candidate[2..].to_lowercase() == self.token[2..].to_lowercase())
        })
    }
}

/// Reference candidates for a batch of typed key-value tokens, with the tokens
/// whose carriers exceeded the per-value cap. Enumeration for those tokens is
/// incomplete: callers report `lookup-truncated` and never treat a capped page
/// as unique.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReferenceCandidates {
    pub records: Vec<EntityVersionRecord>,
    pub truncated_requests: Vec<String>,
}

impl ReferenceCandidates {
    /// Detect overflow from a read bounded to `MAX_CARRIERS_PER_IDENTIFYING_VALUE + 1`
    /// carriers per value: any wanted token carried by more than the cap is truncated.
    pub fn bounded(wanted: &[WantedKeyValue], records: Vec<EntityVersionRecord>) -> Self {
        // Index requests once. Decode each record's key tokens once, then count
        // only matching requests instead of rebuilding tokens for every pair.
        let mut requests = std::collections::HashMap::new();
        for request in wanted {
            requests.entry(request.request_id()).or_insert(request);
        }
        let mut exact: std::collections::HashMap<String, Vec<&str>> =
            std::collections::HashMap::new();
        let mut folded: std::collections::HashMap<String, Vec<&str>> =
            std::collections::HashMap::new();
        for (id, request) in &requests {
            exact.entry(request.token.clone()).or_default().push(id);
            if request.case_insensitive_string && request.token.starts_with("s:") {
                folded
                    .entry(request.token.to_lowercase())
                    .or_default()
                    .push(id);
            }
        }
        let mut carriers: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for record in &records {
            let target = crate::runtime::stage_output::RelationshipTarget::from(record.clone());
            let mut matched = std::collections::HashSet::new();
            for token in target.key_value_tokens() {
                if let Some(ids) = exact.get(&token) {
                    matched.extend(ids.iter().copied());
                }
                if token.starts_with("s:") {
                    if let Some(ids) = folded.get(&token.to_lowercase()) {
                        matched.extend(ids.iter().copied());
                    }
                }
            }
            for id in matched {
                let request = requests[id];
                if request
                    .namespaces
                    .as_ref()
                    .is_some_and(|values| !values.contains(&target.namespace))
                    || request
                        .target_types
                        .as_ref()
                        .is_some_and(|values| !values.contains(&target.entity_type))
                {
                    continue;
                }
                *carriers.entry(id.to_owned()).or_default() += 1;
            }
        }
        let mut truncated_requests: Vec<String> = carriers
            .into_iter()
            .filter(|(_, count)| *count > MAX_CARRIERS_PER_IDENTIFYING_VALUE)
            .map(|(request, _)| request)
            .collect();
        truncated_requests.sort();
        Self {
            records,
            truncated_requests,
        }
    }
}

/// Which versions an identity lookup returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionState {
    /// Latest versions that are not deleted.
    Live,
    /// Tombstoned versions, newest version first.
    Deleted,
}

/// Entity reads used by identity matching, fuzzy candidates, reference
/// targets, and reconciliation. Every lookup is organization scoped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntityLookup {
    /// Versions answering to any hash as primary identity or accumulated alias.
    ByIdentity {
        hashes: Vec<String>,
        state: VersionState,
    },
    /// The live latest version of each chain.
    LatestByChain { chain_ids: Vec<Uuid> },
    /// Live latest versions whose lowercased name equals one of `names`
    /// (callers pass lowercased values).
    LiveByName { names: Vec<String> },
    /// Live latest versions whose lowercased name, or whose lowercased
    /// single-property identity value, equals one of `values` (callers
    /// pass lowercased values). A composite key never matches by one part.
    /// One read resolves every reference of a snapshot; adapters bound it
    /// to the organization's live versions and return at most
    /// [`MAX_CARRIERS_PER_IDENTIFYING_VALUE`] carriers per value, in chain
    /// order, so a widely shared value is reported ambiguous, not listed.
    LiveByIdentifyingValue { values: Vec<String> },
    /// Live latest versions whose stored `key_values` contain one of `values`:
    /// exact, case-sensitive typed tokens (`<type_tag>:<canonical text>`, see
    /// `TargetKeyComponent::token`) over every primary and alternative key
    /// component, so composite parts and integer keys are found and nothing is
    /// lowercased. A component match is a candidate, never an identity; callers
    /// confirm a complete group. Adapters return at most
    /// [`MAX_CARRIERS_PER_IDENTIFYING_VALUE`] + 1 carriers per value so overflow
    /// is detectable (see `GraphBackend::find_reference_candidates`). Each wanted
    /// token carries the namespaces its source may link to, applied before the
    /// cap, so a capped page never hides an eligible target.
    LiveByKeyValue { wanted: Vec<WantedKeyValue> },
    /// Organization-scoped live source heads for a receipted reference rebuild.
    LiveReferenceSources {
        after_chain: Option<Uuid>,
        limit: usize,
    },
    /// Live latest versions missing `key_values` or derived component links, in chain order
    /// after `after_chain`, at most `limit` per page. Drives the awaited,
    /// resumable backfill; an empty page means the organization is ready.
    LiveMissingKeyValues {
        after_chain: Option<Uuid>,
        limit: usize,
    },
    /// Every version of each chain, including superseded and deleted ones,
    /// ordered by chain then version.
    VersionsByChain { chain_ids: Vec<Uuid> },
    /// Live latest versions that are members of one collection whose
    /// membership generation is older than `before_generation`, ordered by
    /// chain. Versions without that membership are never returned, whatever
    /// their source string says.
    StaleInCollection {
        collection: CollectionRef,
        before_generation: u64,
    },
}

impl EntityLookup {
    /// A lookup with no keys matches nothing; adapters return empty without querying.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::ByIdentity { hashes, .. } => hashes.is_empty(),
            Self::LatestByChain { chain_ids } | Self::VersionsByChain { chain_ids } => {
                chain_ids.is_empty()
            }
            Self::LiveByName { names } | Self::LiveByIdentifyingValue { values: names } => {
                names.is_empty()
            }
            Self::LiveByKeyValue { wanted } => wanted.is_empty(),
            Self::LiveMissingKeyValues { limit, .. } | Self::LiveReferenceSources { limit, .. } => {
                *limit == 0
            }
            Self::StaleInCollection { .. } => false,
        }
    }

    /// Reject blank scope values, blank keys, and oversized key lists.
    pub fn validate(&self, org_id: &str) -> Result<(), BackendError> {
        validate_org(org_id)?;
        if let Self::StaleInCollection {
            before_generation, ..
        } = self
        {
            super::graph_mutation::validate_generation(*before_generation)?;
        }
        let (count, blank) = match self {
            Self::ByIdentity { hashes, .. } => {
                (hashes.len(), hashes.iter().any(|h| h.trim().is_empty()))
            }
            Self::LatestByChain { chain_ids } | Self::VersionsByChain { chain_ids } => {
                (chain_ids.len(), false)
            }
            Self::LiveByName { names } | Self::LiveByIdentifyingValue { values: names } => {
                (names.len(), names.iter().any(|n| n.trim().is_empty()))
            }
            Self::LiveByKeyValue { wanted } => (
                wanted.len(),
                wanted.iter().any(|w| {
                    w.token.trim().is_empty()
                        || w.namespaces.as_ref().is_some_and(|namespaces| {
                            namespaces.is_empty()
                                || namespaces.iter().any(|ns| ns.trim().is_empty())
                        })
                        || w.target_types.as_ref().is_some_and(|types| {
                            types.is_empty() || types.iter().any(|ty| ty.trim().is_empty())
                        })
                }),
            ),
            Self::LiveMissingKeyValues { limit, after_chain }
            | Self::LiveReferenceSources { limit, after_chain } => {
                (*limit, after_chain.is_some_and(|id| id.is_nil()))
            }
            Self::StaleInCollection { collection, .. } => (0, collection.validate().is_err()),
        };
        if blank {
            return Err(BackendError::Query("lookup contains a blank key".into()));
        }
        if count > MAX_LOOKUP_KEYS {
            return Err(BackendError::Query(format!(
                "lookup exceeds {MAX_LOOKUP_KEYS} keys"
            )));
        }
        Ok(())
    }
}

/// Relationship reads used by edge resolution and reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeLookup {
    /// Current live relationships between live latest endpoints for each
    /// `(source chain, target chain)` pair.
    LiveByChainPairs { pairs: Vec<(Uuid, Uuid)> },
    /// Newest version of every relationship lineage, including ended heads.
    HeadsByChainPairs { pairs: Vec<(Uuid, Uuid)> },
    /// Every version on each pair, including historical and scheduled intervals.
    /// Embedding properties are omitted; the remaining properties form an exact commit baseline.
    VersionsByChainPairs { pairs: Vec<(Uuid, Uuid)> },
    /// Complete history for each source/name, across targets and producer scopes.
    VersionsByRelations { relations: Vec<(Uuid, String)> },
    /// Complete histories owned by observation slots, independent of stored direction.
    VersionsByReferenceOwners {
        owners: Vec<crate::runtime::stage_output::ReferenceOwnerSelector>,
    },
    /// Complete history touching any selected chain, in either direction.
    VersionsByEndpointChains { chain_ids: Vec<Uuid> },
    /// Every live relationship whose source entity is a live latest version
    /// in one connector scope, ordered by uuid. Supersession reads it to find
    /// contradicted relationships of the same connector.
    LiveBySourceScope { namespace: String, source: String },
    /// Every live relationship touching a live latest version of one of the
    /// chains, in either direction, ordered by uuid. Deleting a chain closes them.
    LiveByEndpointChains { chain_ids: Vec<Uuid> },
    /// Live relationships whose source entity is a member of one collection
    /// and whose generation is absent or older than `before_generation`,
    /// ordered by uuid.
    StaleInCollection {
        collection: CollectionRef,
        before_generation: u64,
    },
    /// Active or pending source-owned intervals absent from a complete collection.
    ScheduledStaleInCollection {
        collection: CollectionRef,
        before_generation: u64,
        effective_at: DateTime<Utc>,
    },
}

impl EdgeLookup {
    /// A lookup with no keys matches nothing; adapters return empty without querying.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::LiveByChainPairs { pairs }
            | Self::HeadsByChainPairs { pairs }
            | Self::VersionsByChainPairs { pairs } => pairs.is_empty(),
            Self::VersionsByRelations { relations } => relations.is_empty(),
            Self::VersionsByReferenceOwners { owners } => owners.is_empty(),
            Self::LiveByEndpointChains { chain_ids }
            | Self::VersionsByEndpointChains { chain_ids } => chain_ids.is_empty(),
            Self::LiveBySourceScope { .. }
            | Self::StaleInCollection { .. }
            | Self::ScheduledStaleInCollection { .. } => false,
        }
    }

    /// Reject blank scope values and oversized key lists.
    pub fn validate(&self, org_id: &str) -> Result<(), BackendError> {
        validate_org(org_id)?;
        if let Self::StaleInCollection {
            before_generation, ..
        }
        | Self::ScheduledStaleInCollection {
            before_generation, ..
        } = self
        {
            super::graph_mutation::validate_generation(*before_generation)?;
        }
        if let Self::VersionsByChainPairs { pairs } = self {
            let unique: std::collections::HashSet<_> = pairs.iter().collect();
            if unique.len() != pairs.len()
                || pairs
                    .iter()
                    .any(|(source, target)| source.is_nil() || target.is_nil())
            {
                return Err(BackendError::Query(
                    "timeline lookup requires unique nonnil pairs".into(),
                ));
            }
        }
        if let Self::VersionsByEndpointChains { chain_ids } = self {
            let unique: std::collections::HashSet<_> = chain_ids.iter().collect();
            if chain_ids.len() > MAX_LOOKUP_KEYS
                || unique.len() != chain_ids.len()
                || chain_ids.iter().any(Uuid::is_nil)
            {
                return Err(BackendError::Query(
                    "timeline lookup requires unique nonnil chains within the key budget".into(),
                ));
            }
        }
        if let Self::VersionsByRelations { relations } = self {
            let unique: std::collections::HashSet<_> = relations.iter().collect();
            if relations.len() > MAX_LOOKUP_KEYS
                || unique.len() != relations.len()
                || relations
                    .iter()
                    .any(|(source, name)| source.is_nil() || name.trim().is_empty())
            {
                return Err(BackendError::Query(
                    "invalid relationship timeline selectors".into(),
                ));
            }
        }
        if let Self::VersionsByReferenceOwners { owners } = self {
            let unique: std::collections::HashSet<_> = owners.iter().collect();
            if owners.len() > MAX_LOOKUP_KEYS
                || unique.len() != owners.len()
                || owners.iter().any(|owner| {
                    owner.chain_id.is_nil()
                        || owner.namespace.trim().is_empty()
                        || owner.slot.trim().is_empty()
                })
            {
                return Err(BackendError::Query(
                    "invalid reference owner selectors".into(),
                ));
            }
        }
        match self {
            Self::LiveByChainPairs { pairs }
            | Self::HeadsByChainPairs { pairs }
            | Self::VersionsByChainPairs { pairs }
                if pairs.len() > MAX_LOOKUP_KEYS =>
            {
                Err(BackendError::Query(format!(
                    "lookup exceeds {MAX_LOOKUP_KEYS} keys"
                )))
            }
            Self::LiveByEndpointChains { chain_ids } if chain_ids.len() > MAX_LOOKUP_KEYS => Err(
                BackendError::Query(format!("lookup exceeds {MAX_LOOKUP_KEYS} keys")),
            ),
            Self::LiveBySourceScope { namespace, source }
                if namespace.trim().is_empty() || source.trim().is_empty() =>
            {
                Err(BackendError::Query("lookup scope is blank".into()))
            }
            Self::StaleInCollection { collection, .. }
            | Self::ScheduledStaleInCollection { collection, .. } => collection
                .validate()
                .map_err(|message| BackendError::Query(format!("lookup {message}"))),
            _ => Ok(()),
        }
    }
}

fn validate_org(org_id: &str) -> Result<(), BackendError> {
    if org_id.trim().is_empty() {
        return Err(BackendError::Query(
            "lookup requires an organization".into(),
        ));
    }
    Ok(())
}

/// One stored entity version. Typed fields are decoded by the adapter;
/// `stored` keeps every graph property, including prefixed source properties.
#[derive(Debug, Clone, PartialEq)]
pub struct EntityVersionRecord {
    pub uuid: Uuid,
    pub chain_id: Uuid,
    pub version: u32,
    pub is_latest: bool,
    pub entity_type: String,
    pub name: String,
    pub namespace: String,
    pub source: Option<String>,
    pub identity_hash: Option<String>,
    /// Aliases accumulated by merges and renames.
    pub identity_hashes: Vec<String>,
    pub structural_hash: Option<String>,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_to: Option<DateTime<Utc>>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub last_seen_at: Option<DateTime<Utc>>,
    /// Latest chain transition; source observations must not precede it.
    pub last_transition_at: Option<DateTime<Utc>>,
    pub sync_generation: Option<u64>,
    /// Collections this version belongs to, each with its last observation
    /// generation there.
    pub collections: Vec<CollectionMembership>,
    pub merged_into: Option<Uuid>,
    pub embedding: Option<GraphEmbedding>,
    /// Every stored property in adapter-primitive form.
    pub stored: Map<String, Value>,
}

impl EntityVersionRecord {
    /// Whether another collection than `collection` also owns this version.
    pub fn is_shared_beyond(&self, collection: &CollectionRef) -> bool {
        self.collections
            .iter()
            .any(|membership| membership.collection != *collection)
    }

    /// Every hash this version answers to: primary identity plus aliases.
    pub fn answering_hashes(&self) -> impl Iterator<Item = &str> {
        self.identity_hash
            .iter()
            .map(String::as_str)
            .chain(self.identity_hashes.iter().map(String::as_str))
    }

    pub fn typed_source_properties(
        &self,
    ) -> Result<indexmap::IndexMap<String, crate::models::PropertyValue>, String> {
        super::property_codec::read_properties(&self.stored)
    }

    /// Native query projections with the storage prefix removed.
    pub fn source_properties(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.stored
            .iter()
            .filter_map(|(key, value)| Some((key.strip_prefix(USER_PROPERTY_PREFIX)?, value)))
    }
}

/// One stored domain relationship with its stable endpoint chains.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeRecord {
    pub uuid: Uuid,
    pub source_chain_id: Uuid,
    pub target_chain_id: Uuid,
    pub name: String,
    pub version: u32,
    pub is_latest: bool,
    pub confidence: f32,
    pub valid_from: Option<DateTime<Utc>>,
    pub invalid_at: Option<DateTime<Utc>>,
    pub sync_generation: Option<u64>,
    /// Collections the source entity's live version belongs to.
    pub source_collections: Vec<CollectionRef>,
    /// Every stored property in adapter-primitive form.
    pub stored: Map<String, Value>,
}

impl EdgeRecord {
    /// Whether a collection other than `collection` also owns the source entity.
    pub fn source_shared_beyond(&self, collection: &CollectionRef) -> bool {
        self.source_collections
            .iter()
            .any(|owner| owner != collection)
    }

    /// Relationships derived from source data (declared, foreign-key, or
    /// sub-entity rules) are owned by the observing collection; relationships
    /// an LLM inferred from text are never swept by absence.
    pub fn is_source_owned(&self) -> bool {
        matches!(
            self.stored.get("origin").and_then(Value::as_str),
            Some("declared" | "reference")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_lookups_are_detected_and_validated() {
        let lookup = EntityLookup::ByIdentity {
            hashes: vec![],
            state: VersionState::Live,
        };
        assert!(lookup.is_empty());
        assert!(lookup.validate("org").is_ok());
        assert!(lookup.validate(" ").is_err());
        let blank = EntityLookup::LiveByName {
            names: vec![" ".into()],
        };
        assert!(blank.validate("org").is_err());
        let scope = EdgeLookup::LiveBySourceScope {
            namespace: "prod".into(),
            source: String::new(),
        };
        assert!(scope.validate("org").is_err());
        let collection = EntityLookup::StaleInCollection {
            collection: CollectionRef {
                namespace: "prod".into(),
                source: "aws".into(),
                key: " ".into(),
            },
            before_generation: 1,
        };
        assert!(collection.validate("org").is_err());
        let big = EntityLookup::LatestByChain {
            chain_ids: vec![Uuid::nil(); MAX_LOOKUP_KEYS + 1],
        };
        assert!(big.validate("org").is_err());
    }

    #[test]
    fn record_exposes_hashes_and_source_properties() {
        let mut stored = Map::new();
        stored.insert("prop_owner".into(), Value::String("payments".into()));
        stored.insert("name".into(), Value::String("api".into()));
        let record = EntityVersionRecord {
            uuid: Uuid::nil(),
            chain_id: Uuid::nil(),
            version: 1,
            is_latest: true,
            entity_type: "Service".into(),
            name: "api".into(),
            namespace: "prod".into(),
            source: None,
            identity_hash: Some("primary".into()),
            identity_hashes: vec!["alias".into()],
            structural_hash: None,
            valid_from: None,
            valid_to: None,
            deleted_at: None,
            last_seen_at: None,
            last_transition_at: None,
            sync_generation: None,
            collections: vec![CollectionMembership {
                collection: CollectionRef {
                    namespace: "prod".into(),
                    source: "aws".into(),
                    key: "k".into(),
                },
                generation: 3,
            }],
            merged_into: None,
            embedding: None,
            stored,
        };
        assert_eq!(
            record.answering_hashes().collect::<Vec<_>>(),
            vec!["primary", "alias"]
        );
        let own = CollectionRef {
            namespace: "prod".into(),
            source: "aws".into(),
            key: "k".into(),
        };
        assert!(!record.is_shared_beyond(&own));
        assert!(record.is_shared_beyond(&CollectionRef {
            key: "j".into(),
            ..own.clone()
        }));
        let mut stored = Map::new();
        stored.insert("origin".into(), Value::String("fact".into()));
        let inferred = EdgeRecord {
            uuid: Uuid::nil(),
            source_chain_id: Uuid::nil(),
            target_chain_id: Uuid::nil(),
            name: "USES".into(),
            version: 1,
            is_latest: true,
            confidence: 0.5,
            valid_from: None,
            invalid_at: None,
            sync_generation: None,
            source_collections: vec![own.clone()],
            stored,
        };
        assert!(!inferred.is_source_owned());
        assert!(!inferred.source_shared_beyond(&own));
        let mut declared = inferred.clone();
        declared
            .stored
            .insert("origin".into(), Value::String("declared".into()));
        declared.source_collections.push(CollectionRef {
            key: "j".into(),
            ..own.clone()
        });
        assert!(declared.is_source_owned());
        assert!(declared.source_shared_beyond(&own));
        assert_eq!(
            record.source_properties().collect::<Vec<_>>(),
            vec![("owner", &Value::String("payments".into()))]
        );
    }

    /// A record whose single primary key `id` carries the given string value,
    /// so its typed token is `s:<value>`.
    fn carrier(id: &str) -> EntityVersionRecord {
        let mut stored = Map::new();
        stored.insert(
            "primary_key_properties".into(),
            Value::Array(vec![Value::String("id".into())]),
        );
        stored.insert("prop_id".into(), Value::String(id.into()));
        stored.insert("property_type_id".into(), Value::String("s".into()));
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
            embedding: None,
            stored,
        }
    }

    #[test]
    fn indexed_carriers_preserve_scope_type_and_case_contract() {
        let mut folded = WantedKeyValue::exact("s:PROD".into(), Some(vec!["prod".into()]));
        folded.case_insensitive_string = true;
        folded.target_types = Some(vec!["Service".into()]);
        let mut wrong_scope = folded.clone();
        wrong_scope.namespaces = Some(vec!["other".into()]);
        let mut wrong_type = folded.clone();
        wrong_type.target_types = Some(vec!["Database".into()]);
        let exact = WantedKeyValue::exact("s:PROD".into(), None);
        let wanted = vec![folded.clone(), folded, wrong_scope, wrong_type, exact];
        let records: Vec<_> = (0..=MAX_CARRIERS_PER_IDENTIFYING_VALUE)
            .map(|_| carrier("prod"))
            .collect();
        let result = ReferenceCandidates::bounded(&wanted, records);
        assert_eq!(result.truncated_requests, vec![wanted[0].request_id()]);
    }

    #[test]
    fn bounded_reports_only_values_over_the_carrier_cap() {
        let wanted = vec![
            WantedKeyValue::exact("s:common".into(), None),
            WantedKeyValue::exact("s:rare".into(), None),
        ];

        // Exactly the cap carriers for a value is not truncation.
        let at_cap: Vec<_> = (0..MAX_CARRIERS_PER_IDENTIFYING_VALUE)
            .map(|_| carrier("common"))
            .collect();
        let candidates = ReferenceCandidates::bounded(&wanted, at_cap);
        assert!(candidates.truncated_requests.is_empty());
        assert_eq!(candidates.records.len(), MAX_CARRIERS_PER_IDENTIFYING_VALUE);

        // One over the cap (the extra carrier the bounded read fetches) is
        // reported truncated; the rare value alongside it stays complete and the
        // records are never silently dropped.
        let mut over_cap: Vec<_> = (0..=MAX_CARRIERS_PER_IDENTIFYING_VALUE)
            .map(|_| carrier("common"))
            .collect();
        over_cap.push(carrier("rare"));
        let total = over_cap.len();
        let candidates = ReferenceCandidates::bounded(&wanted, over_cap);
        assert_eq!(candidates.truncated_requests, vec![wanted[0].request_id()]);
        assert_eq!(candidates.records.len(), total);

        // A token nobody asked for never counts toward truncation.
        let unwanted: Vec<_> = (0..MAX_CARRIERS_PER_IDENTIFYING_VALUE + 5)
            .map(|_| carrier("unlisted"))
            .collect();
        assert!(ReferenceCandidates::bounded(&wanted, unwanted)
            .truncated_requests
            .is_empty());
    }
}
