//! Strict input envelopes and extraction hints; resource properties remain open.

use super::collection::CollectionScope;
use crate::enums::EntityLifecycle;
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display, EnumString)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum SnapshotDataType {
    Message,
    Json,
    Text,
    Entities,
}

/// Deletion sweeps require a complete full observation, a generation, and successful extraction.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize, Display, EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum SnapshotKind {
    Full,
    /// Missing data does not prove deletion.
    #[default]
    Incremental,
}

/// Unflattened source input for normalization and child extraction.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorEntity {
    pub entity_type: String,
    pub name: String,
    pub primary_key_properties: Vec<String>,
    /// Each complete group is another authoritative way to identify this entity.
    #[serde(default)]
    pub additional_key_properties: Vec<Vec<String>>,
    pub raw_properties: serde_json::Value,
    /// Overrides the snapshot namespace when present.
    pub namespace: Option<String>,
    pub lifecycle: EntityLifecycle,
    #[serde(default)]
    pub labels: Vec<String>,
    pub tags: IndexMap<String, String>,
    pub source: String,
    pub org_id: String,
}

/// A complete, exact entity key or an already known lineage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RelationshipEndpoint {
    Chain {
        chain_id: uuid::Uuid,
    },
    Identity {
        namespace: String,
        entity_type: String,
        key_values: IndexMap<String, super::PropertyValue>,
    },
}

/// A source-declared fact. Scope and provenance come from its snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationshipObservation {
    pub source: RelationshipEndpoint,
    pub target: RelationshipEndpoint,
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub properties: IndexMap<String, super::PropertyValue>,
    pub valid_from: DateTime<Utc>,
    pub valid_to: Option<DateTime<Utc>>,
}

/// Exact target of a scheduled amendment; no semantic matching is permitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationshipVersionRef {
    pub source_chain_id: uuid::Uuid,
    pub target_chain_id: uuid::Uuid,
    pub chain_id: uuid::Uuid,
    pub version_uuid: uuid::Uuid,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum RelationshipChange {
    Observe {
        relationship: RelationshipObservation,
    },
    Replace {
        target: RelationshipVersionRef,
        replacement: RelationshipObservation,
        effective_at: DateTime<Utc>,
    },
    Cancel {
        target: RelationshipVersionRef,
        effective_at: DateTime<Utc>,
    },
}

/// Thread membership. New inputs use a flat reference; serialization retains the
/// legacy envelope so existing ingestion fingerprints and receipts stay replayable.
#[derive(Debug, Clone, Serialize)]
pub struct ThreadAssociation {
    pub saga: crate::saga::ThreadReference,
    pub previous_snapshot_uuid: Option<uuid::Uuid>,
}

impl<'de> Deserialize<'de> for ThreadAssociation {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            #[serde(alias = "thread")]
            saga: Option<crate::saga::ThreadReference>,
            kind: Option<String>,
            name: Option<String>,
            uuid: Option<uuid::Uuid>,
            #[serde(alias = "predecessor_snapshot_uuid")]
            previous_snapshot_uuid: Option<uuid::Uuid>,
        }
        let wire = Wire::deserialize(deserializer)?;
        let saga = match (wire.saga, wire.kind.as_deref(), wire.name, wire.uuid) {
            (Some(reference), None, None, None) => reference,
            (None, Some("name"), Some(name), None) => crate::saga::ThreadReference::Name { name },
            (None, Some("uuid"), None, Some(uuid)) => crate::saga::ThreadReference::Uuid { uuid },
            _ => {
                return Err(serde::de::Error::custom(
                    "thread requires exactly one name or UUID reference",
                ))
            }
        };
        Ok(Self {
            saga,
            previous_snapshot_uuid: wire.previous_snapshot_uuid,
        })
    }
}
impl ThreadAssociation {
    pub fn validate(&self) -> Result<(), InputValidationError> {
        match &self.saga {
            crate::saga::ThreadReference::Name { name } => {
                require_input_text(name, "saga.name".into())?
            }
            crate::saga::ThreadReference::Uuid { uuid } if uuid.is_nil() => {
                return Err(InputValidationError {
                    field: "saga.uuid".into(),
                    reason: InputValidationReason::WrongShape,
                });
            }
            _ => {}
        }
        if self
            .previous_snapshot_uuid
            .is_some_and(|uuid| uuid.is_nil())
        {
            return Err(InputValidationError {
                field: "saga.previous_snapshot_uuid".into(),
                reason: InputValidationReason::WrongShape,
            });
        }
        Ok(())
    }
}

/// Attach an immutable stored snapshot without extracting or rewriting its source data.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExistingSnapshotInput {
    pub namespace: String,
    pub snapshot_uuid: uuid::Uuid,
    #[serde(alias = "thread")]
    pub saga: ThreadAssociation,
}
impl ExistingSnapshotInput {
    pub fn validate_request(&self, org_id: &str) -> Result<(), InputValidationError> {
        require_input_text(org_id, "org_id".into())?;
        require_input_text(&self.namespace, "namespace".into())?;
        self.saga.validate()?;
        if self.snapshot_uuid.is_nil()
            || self.saga.previous_snapshot_uuid == Some(self.snapshot_uuid)
        {
            return Err(InputValidationError {
                field: "snapshot_uuid".into(),
                reason: InputValidationReason::WrongShape,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "input",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum IngestionInput {
    Fresh(Box<SnapshotInput>),
    Existing(ExistingSnapshotInput),
}
impl IngestionInput {
    pub fn validate_request(&self, org_id: &str) -> Result<(), InputValidationError> {
        match self {
            Self::Fresh(input) => input.validate_request(org_id),
            Self::Existing(input) => input.validate_request(org_id),
        }
    }
    pub fn namespace(&self) -> &str {
        match self {
            Self::Fresh(input) => &input.namespace,
            Self::Existing(input) => &input.namespace,
        }
    }
    pub fn saga(&self) -> Option<&ThreadAssociation> {
        match self {
            Self::Fresh(input) => input.saga.as_ref(),
            Self::Existing(input) => Some(&input.saga),
        }
    }
}
impl From<SnapshotInput> for IngestionInput {
    fn from(input: SnapshotInput) -> Self {
        Self::Fresh(Box::new(input))
    }
}

/// One observation scope; individual entities may override its namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotInput {
    #[serde(default)]
    #[serde(alias = "thread")]
    pub saga: Option<ThreadAssociation>,
    #[serde(default)]
    pub relationship_changes: Vec<RelationshipChange>,
    /// Previously committed observations selected as context, never new evidence.
    #[serde(default)]
    pub previous_snapshot_uuids: Vec<uuid::Uuid>,
    #[serde(default)]
    pub tags: IndexMap<String, String>,
    #[serde(default)]
    pub labels: Vec<String>,
    pub namespace: String,
    pub name: String,
    /// Human-readable source context, treated as untrusted data.
    #[serde(default)]
    pub source_description: Option<String>,
    pub data_type: SnapshotDataType,
    #[serde(default)]
    pub snapshot_kind: SnapshotKind,
    #[serde(default)]
    pub sync_generation: Option<u64>,
    #[serde(default)]
    pub complete: bool,
    /// When supplied, must match the runtime organization.
    #[serde(default)]
    pub org_id: Option<String>,
    pub source: String,
    pub entities: Vec<ConnectorEntity>,
    pub content: Option<String>,
    pub entity_types: Option<Vec<EntityTypeSchema>>,
    pub edge_types: Option<Vec<EdgeTypeSchema>>,
    pub edge_type_map: Option<Vec<EdgeTypeMapEntry>>,
    #[serde(default)]
    pub exclude_fk_properties: Vec<String>,
    /// Additional hash exclusions; applied even to per-type forced-version properties.
    #[serde(default)]
    pub ignore_change_properties: Vec<String>,
    /// Source observation time; the pipeline supplies ingestion time when absent.
    pub captured_at: Option<DateTime<Utc>>,
    /// Owned collection of a complete full scan. Requires `snapshot_kind: full`,
    /// `complete: true`, and `sync_generation`; see [`CollectionScope`].
    #[serde(default)]
    pub collection: Option<CollectionScope>,
}

impl SnapshotInput {
    /// A declared collection must be a fully specified deletion authorization.
    pub fn validate_scope(&self) -> Result<(), String> {
        if self
            .sync_generation
            .is_some_and(|generation| generation > i64::MAX as u64)
        {
            return Err("sync_generation exceeds the supported signed 64-bit range".into());
        }
        let Some(scope) = &self.collection else {
            return Ok(());
        };
        if scope.key.trim().is_empty() {
            return Err("collection.key must not be blank".into());
        }
        if self.snapshot_kind != SnapshotKind::Full {
            return Err("collection requires snapshot_kind `full`".into());
        }
        if !self.complete {
            return Err("collection requires complete = true".into());
        }
        if self.sync_generation.is_none() {
            return Err("collection requires sync_generation".into());
        }
        Ok(())
    }
}

fn validate_relationship_target(target: &RelationshipVersionRef) -> Result<(), ()> {
    if [
        target.source_chain_id,
        target.target_chain_id,
        target.chain_id,
        target.version_uuid,
    ]
    .iter()
    .any(uuid::Uuid::is_nil)
    {
        Err(())
    } else {
        Ok(())
    }
}

/// Why a request field failed validation. Values are never included in errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InputValidationReason {
    /// A required identifier contains only whitespace.
    #[error("blank")]
    Blank,
    /// An explicit organization differs from the request organization.
    #[error("scope_mismatch")]
    ScopeMismatch,
    /// Entity properties must be an object.
    #[error("expected object")]
    WrongShape,
    /// A declared key name occurs more than once.
    #[error("duplicate")]
    Duplicate,
    #[error("invalid schema")]
    InvalidSchema,
}

/// Location and reason for an invalid field, without the caller's data.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{field}: {reason}")]
pub struct InputValidationError {
    /// Structural field path, including indexes where applicable.
    pub field: String,
    /// Stable validation category.
    pub reason: InputValidationReason,
}

fn require_input_text(value: &str, field: String) -> Result<(), InputValidationError> {
    if value.trim().is_empty() {
        return Err(InputValidationError {
            field,
            reason: InputValidationReason::Blank,
        });
    }
    Ok(())
}

fn validate_input_metadata(
    tags: &IndexMap<String, String>,
    labels: &[String],
    prefix: &str,
) -> Result<(), InputValidationError> {
    for (index, key) in tags.keys().enumerate() {
        require_input_text(key, format!("{prefix}.tags[{index}].key"))?;
    }
    for (index, label) in labels.iter().enumerate() {
        require_input_text(label, format!("{prefix}.labels[{index}]"))?;
    }
    Ok(())
}

impl SnapshotInput {
    fn validate_relationship_changes(&self) -> Result<(), InputValidationError> {
        let invalid = || InputValidationError {
            field: "relationship_changes".into(),
            reason: InputValidationReason::WrongShape,
        };
        if self.relationship_changes.len() > 1_000
            || !super::attribute_schema::within_json_limit(&self.relationship_changes, 1_048_576)
        {
            return Err(invalid());
        }
        let mut targets = std::collections::HashSet::new();
        for change in &self.relationship_changes {
            let observation = match change {
                RelationshipChange::Observe { relationship } => Some(relationship),
                RelationshipChange::Replace {
                    target,
                    replacement,
                    ..
                } => {
                    validate_relationship_target(target).map_err(|_| invalid())?;
                    if !targets.insert(target.chain_id) {
                        return Err(invalid());
                    }
                    Some(replacement)
                }
                RelationshipChange::Cancel { target, .. } => {
                    validate_relationship_target(target).map_err(|_| invalid())?;
                    if !targets.insert(target.chain_id) {
                        return Err(invalid());
                    }
                    None
                }
            };
            if let Some(observation) = observation {
                super::PropertyValue::validate_flat_paths(&observation.properties)
                    .map_err(|_| invalid())?;
                for value in observation.properties.values() {
                    value.to_source().map_err(|_| invalid())?;
                }
                if observation.name.trim().is_empty()
                    || observation.description.trim().is_empty()
                    || observation
                        .valid_to
                        .is_some_and(|end| end < observation.valid_from)
                    || observation.properties.len() > 256
                    || observation
                        .properties
                        .keys()
                        .any(|key| key.trim().is_empty())
                {
                    return Err(invalid());
                }
                for endpoint in [&observation.source, &observation.target] {
                    match endpoint {
                        RelationshipEndpoint::Chain { chain_id } if chain_id.is_nil() => {
                            return Err(invalid());
                        }
                        RelationshipEndpoint::Identity {
                            namespace,
                            entity_type,
                            key_values,
                        } => {
                            if namespace.trim().is_empty()
                                || entity_type.trim().is_empty()
                                || key_values.is_empty()
                                || key_values.len() > 32
                                || key_values.iter().any(|(key, value)| {
                                    key.trim().is_empty()
                                        || match value {
                                            super::PropertyValue::String(value) => {
                                                value.trim().is_empty()
                                            }
                                            super::PropertyValue::Integer(_)
                                            | super::PropertyValue::Bool(_) => false,
                                            super::PropertyValue::Float(value) => {
                                                !value.is_finite()
                                            }
                                            _ => true,
                                        }
                                })
                            {
                                return Err(invalid());
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }

    /// Validate generic input shape and scope without interpreting collection state.
    pub fn validate_request(&self, org_id: &str) -> Result<(), InputValidationError> {
        require_input_text(org_id, "org_id".into())?;
        if let Some(saga) = &self.saga {
            saga.validate()?;
        }
        self.validate_relationship_changes()?;
        let ids = &self.previous_snapshot_uuids;
        if ids.len() > crate::runtime::history::MAX_CONTEXT_RECORDS
            || ids.iter().any(uuid::Uuid::is_nil)
            || (!ids.is_empty()
                && (!self.entities.is_empty()
                    || self.content.as_ref().is_none_or(|s| s.trim().is_empty())))
        {
            return Err(InputValidationError {
                field: "previous_snapshot_uuids".into(),
                reason: InputValidationReason::WrongShape,
            });
        }
        if ids.iter().collect::<std::collections::HashSet<_>>().len() != ids.len() {
            return Err(InputValidationError {
                field: "previous_snapshot_uuids".into(),
                reason: InputValidationReason::Duplicate,
            });
        }
        for (field, value) in [
            ("namespace", &self.namespace),
            ("source", &self.source),
            ("name", &self.name),
        ] {
            require_input_text(value, field.into())?;
        }
        if let Some(description) = &self.source_description {
            require_input_text(description, "source_description".into())?;
        }
        if self
            .org_id
            .as_deref()
            .is_some_and(|declared| declared != org_id)
        {
            return Err(InputValidationError {
                field: "org_id".into(),
                reason: InputValidationReason::ScopeMismatch,
            });
        }
        crate::runtime::schemas::validate_input_schemas(self).map_err(|_| {
            InputValidationError {
                field: "schemas".into(),
                reason: InputValidationReason::InvalidSchema,
            }
        })?;
        validate_input_metadata(&self.tags, &self.labels, "snapshot")?;
        for (index, entity) in self.entities.iter().enumerate() {
            let prefix = format!("entities[{index}]");
            if entity.org_id != org_id {
                return Err(InputValidationError {
                    field: format!("{prefix}.org_id"),
                    reason: InputValidationReason::ScopeMismatch,
                });
            }
            for (field, value) in [
                ("name", &entity.name),
                ("entity_type", &entity.entity_type),
                ("source", &entity.source),
            ] {
                require_input_text(value, format!("{prefix}.{field}"))?;
            }
            if let Some(namespace) = &entity.namespace {
                require_input_text(namespace, format!("{prefix}.namespace"))?;
            }
            if !entity.raw_properties.is_object() {
                return Err(InputValidationError {
                    field: format!("{prefix}.raw_properties"),
                    reason: InputValidationReason::WrongShape,
                });
            }
            validate_input_metadata(&entity.tags, &entity.labels, &prefix)?;
            let mut groups = std::collections::HashSet::new();
            for (index, group) in entity.additional_key_properties.iter().enumerate() {
                let field = format!("{prefix}.additional_key_properties[{index}]");
                if group.is_empty() {
                    return Err(InputValidationError {
                        field,
                        reason: InputValidationReason::WrongShape,
                    });
                }
                let mut unique = std::collections::HashSet::new();
                for key in group {
                    require_input_text(key, field.clone())?;
                    if !unique.insert(key) {
                        return Err(InputValidationError {
                            field,
                            reason: InputValidationReason::Duplicate,
                        });
                    }
                }
                let mut ordered = group.clone();
                ordered.sort();
                let mut primary = entity.primary_key_properties.clone();
                primary.sort();
                if ordered == primary || !groups.insert(ordered) {
                    return Err(InputValidationError {
                        field,
                        reason: InputValidationReason::Duplicate,
                    });
                }
            }
            let mut keys = std::collections::HashSet::new();
            for (key_index, key) in entity.primary_key_properties.iter().enumerate() {
                let field = format!("{prefix}.primary_key_properties[{key_index}]");
                require_input_text(key, field.clone())?;
                if !keys.insert(key) {
                    return Err(InputValidationError {
                        field,
                        reason: InputValidationReason::Duplicate,
                    });
                }
            }
        }
        Ok(())
    }
}

/// Check all snapshots before processing any of them. No I/O or collection decisions.
pub fn validate_request(
    org_id: &str,
    snapshots: &[SnapshotInput],
) -> Result<(), InputValidationError> {
    require_input_text(org_id, "org_id".into())?;
    for (index, snapshot) in snapshots.iter().enumerate() {
        snapshot.validate_request(org_id).map_err(|mut error| {
            let field = error
                .field
                .strip_prefix("snapshot.")
                .unwrap_or(&error.field);
            error.field = format!("snapshots[{index}].{field}");
            error
        })?;
    }
    Ok(())
}

/// Validated, immutable input bound to one request organization.
/// This transient handoff cannot be deserialized or constructed unchecked.
#[derive(Debug, Clone)]
pub struct ValidatedSnapshotInput {
    input: SnapshotInput,
    org_id: String,
}

impl ValidatedSnapshotInput {
    /// Validate an input without changing any caller values.
    pub fn new(input: SnapshotInput, org_id: &str) -> Result<Self, InputValidationError> {
        input.validate_request(org_id)?;
        Ok(Self {
            input,
            org_id: org_id.into(),
        })
    }

    /// Access the original input without permitting mutation.
    pub fn input(&self) -> &SnapshotInput {
        &self.input
    }

    /// Check scope before a stage inspects or forwards this handoff.
    pub fn check_org(&self, org_id: &str) -> Result<(), InputValidationError> {
        if self.org_id != org_id {
            return Err(InputValidationError {
                field: "org_id".into(),
                reason: InputValidationReason::ScopeMismatch,
            });
        }
        Ok(())
    }

    /// Consume the handoff only within its validated organization.
    pub fn into_input(self, org_id: &str) -> Result<SnapshotInput, InputValidationError> {
        self.check_org(org_id)?;
        Ok(self.input)
    }
}

/// Pages of one collection in a request must agree on generation and coverage.
/// The request must hold the whole collection; multi-call scans are not supported.
pub fn validate_collection_scopes(snapshots: &[SnapshotInput]) -> Result<(), String> {
    let mut seen: std::collections::HashMap<(&str, &str, &str), (Option<u64>, bool)> =
        std::collections::HashMap::new();
    for (index, snapshot) in snapshots.iter().enumerate() {
        snapshot
            .validate_scope()
            .map_err(|e| format!("snapshot {index}: {e}"))?;
        let Some(scope) = &snapshot.collection else {
            continue;
        };
        let key = (
            snapshot.namespace.as_str(),
            snapshot.source.as_str(),
            scope.key.as_str(),
        );
        let declared = (snapshot.sync_generation, scope.relationships_complete);
        match seen.get(&key) {
            Some(previous) if *previous != declared => {
                return Err(format!(
                    "snapshot {index}: collection `{}` declares generation {:?} \
                     (relationships_complete = {}) but an earlier page declared {:?} \
                     (relationships_complete = {})",
                    scope.key, declared.0, declared.1, previous.0, previous.1
                ));
            }
            Some(_) => {}
            None => {
                seen.insert(key, declared);
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntityTypeSchema {
    #[serde(default)]
    pub attributes: Option<super::AttributeSchema>,
    pub name: String,
    pub description: Option<String>,
    #[serde(default)]
    pub properties: Vec<PropertySchema>,
    /// Complete source-backed scalar coordinates that identify this text entity.
    #[serde(default)]
    pub identity_properties: Vec<String>,
}

/// Extraction hints with optional endpoint-type restrictions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeTypeSchema {
    /// Whether each identifying-property combination permits only one live target.
    #[serde(default)]
    pub single_target: bool,
    /// Property paths that distinguish relationships with the same endpoints and name.
    #[serde(default)]
    pub identifying_properties: Vec<String>,
    #[serde(default)]
    pub attributes: Option<super::AttributeSchema>,
    pub name: String,
    pub description: Option<String>,
    pub source_type: Option<String>,
    pub target_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeTypeMapEntry {
    pub source_type: String,
    pub target_type: String,
    pub edge_name: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PropertySchema {
    pub name: String,
    pub description: Option<String>,
    pub required: bool,
}

/// Legacy membership name; new public inputs use `thread`.
pub type SagaAssociation = ThreadAssociation;

#[cfg(test)]
mod tests;
