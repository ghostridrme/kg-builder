//! Text mentions retain evidence and provenance before graph identity is assigned.
use super::{
    extraction::ExtractionSettings, schemas::ObservationSchemas,
    stage_output::PreparedSnapshotInput,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntityMention {
    pub entity_type: String,
    pub name: String,
    pub properties: Map<String, Value>,
    pub extracted_by: String,
}
impl EntityMention {
    /// Different serving models do not make an otherwise identical observation new.
    pub fn same_observation(&self, other: &Self) -> bool {
        self.entity_type == other.entity_type
            && self.name == other.name
            && self.properties == other.properties
    }
}

#[derive(Debug, Clone)]
pub struct TextEntityDrafts {
    prepared: PreparedSnapshotInput,
    schemas: ObservationSchemas,
    mentions: Vec<EntityMention>,
    settings: ExtractionSettings,
    omission_checked: bool,
}

impl TextEntityDrafts {
    pub fn new(
        prepared: PreparedSnapshotInput,
        schemas: ObservationSchemas,
        settings: ExtractionSettings,
        mentions: Vec<EntityMention>,
    ) -> Result<Self, &'static str> {
        settings
            .validate()
            .map_err(|_| "invalid extraction settings")?;
        validate_mentions(&mentions, &settings)?;
        Ok(Self {
            prepared,
            schemas,
            mentions,
            settings,
            omission_checked: false,
        })
    }
    pub fn prepared(&self) -> &PreparedSnapshotInput {
        &self.prepared
    }
    pub fn schemas(&self) -> &ObservationSchemas {
        &self.schemas
    }
    pub fn mentions(&self) -> &[EntityMention] {
        &self.mentions
    }
    pub fn settings(&self) -> &ExtractionSettings {
        &self.settings
    }
    pub fn omission_checked(&self) -> bool {
        self.omission_checked
    }

    /// An enabled check must explicitly supply its additional observations, including an empty answer.
    pub fn complete_omission(
        &mut self,
        additional: Option<Vec<EntityMention>>,
    ) -> Result<(), &'static str> {
        if self.omission_checked {
            return Err("omission check already completed");
        }
        if additional.is_some() != self.settings.omission_check {
            return Err("omission result does not match configured policy");
        }
        let mut merged = self.mentions.clone();
        if let Some(additional) = additional {
            validate_mentions(&additional, &self.settings)?;
            for mention in additional {
                if !merged.iter().any(|old| old.same_observation(&mention)) {
                    merged.push(mention);
                }
            }
        }
        validate_mentions(&merged, &self.settings)?;
        self.mentions = merged;
        self.omission_checked = true;
        Ok(())
    }
    pub fn into_parts(
        self,
    ) -> Result<
        (
            PreparedSnapshotInput,
            ObservationSchemas,
            Vec<EntityMention>,
        ),
        &'static str,
    > {
        if !self.omission_checked {
            return Err("omission policy must be applied before preparation");
        }
        Ok((self.prepared, self.schemas, self.mentions))
    }
}

pub fn validate_mentions(
    mentions: &[EntityMention],
    settings: &ExtractionSettings,
) -> Result<(), &'static str> {
    let bad = "invalid or oversized entity discovery";
    if mentions.len() > settings.max_entities {
        return Err(bad);
    }
    struct Budget(usize);
    impl std::io::Write for Budget {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_sub(buf.len())
                .ok_or_else(|| std::io::Error::other("response too large"))?;
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Budget(settings.max_response_bytes), mentions).map_err(|_| bad)?;
    for mention in mentions {
        crate::sanitize::validate_extracted_name(&mention.name).map_err(|_| bad)?;
        crate::sanitize::validate_extracted_name(&mention.entity_type).map_err(|_| bad)?;
        let model = mention.extracted_by.strip_prefix("llm:").ok_or(bad)?;
        if model.trim().is_empty()
            || model.len() > 256
            || model.chars().any(char::is_control)
            || settings
                .excluded_entity_types
                .contains(&mention.entity_type)
            || !properties_within_limits(&mention.properties, settings.max_property_depth)
        {
            return Err(bad);
        }
    }
    Ok(())
}

/// Bound source property structure independently of entity discovery or model identity.
pub fn properties_within_limits(properties: &Map<String, Value>, max_depth: usize) -> bool {
    fn bounded(value: &Value, depth: usize, max: usize) -> bool {
        depth <= max
            && match value {
                Value::Object(m) => {
                    m.len() <= 512
                        && m.iter().all(|(k, v)| {
                            !k.trim().is_empty() && k.len() <= 256 && bounded(v, depth + 1, max)
                        })
                }
                Value::Array(a) => a.len() <= 4096 && a.iter().all(|v| bounded(v, depth + 1, max)),
                _ => true,
            }
    }
    properties.len() <= 512
        && properties.iter().all(|(key, value)| {
            !key.trim().is_empty() && key.len() <= 256 && bounded(value, 1, max_depth)
        })
}

/// Local extraction identity, never a graph identity or a model-supplied UUID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MentionId(pub uuid::Uuid);

impl MentionId {
    pub fn for_mention(
        snapshot: uuid::Uuid,
        mention: &EntityMention,
    ) -> Result<Self, &'static str> {
        // Serving model and order do not change the identity of source evidence.
        let mut value =
            serde_json::to_value((&mention.entity_type, &mention.name, &mention.properties))
                .map_err(|_| "invalid raw mention")?;
        value.sort_all_objects();
        let bytes = serde_json::to_vec(&value).map_err(|_| "invalid raw mention")?;
        Ok(Self(uuid::Uuid::new_v5(&snapshot, &bytes)))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawMention {
    pub id: MentionId,
    pub observation_uuid: uuid::Uuid,
    pub evidence: EntityMention,
}

/// Frozen source evidence; canonical names and enrichment never overwrite it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawTextDraft {
    pub schema_version: u32,
    pub snapshot_uuid: uuid::Uuid,
    pub org_id: String,
    pub namespace: String,
    pub settings_digest: String,
    pub schema_digest: String,
    pub mentions: Vec<RawMention>,
}

pub fn evidence_digest(value: &impl Serialize) -> Result<String, &'static str> {
    use sha2::{Digest, Sha256};
    // Value canonicalizes map keys before hashing, including nested source properties.
    let mut value =
        serde_json::to_value(value).map_err(|_| "cannot serialize extraction evidence")?;
    value.sort_all_objects();
    let bytes = serde_json::to_vec(&value).map_err(|_| "cannot serialize extraction evidence")?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

impl RawTextDraft {
    pub fn new(
        snapshot: &crate::models::SnapshotNode,
        schemas: &ObservationSchemas,
        settings: &ExtractionSettings,
        mentions: Vec<RawMention>,
    ) -> Result<Self, &'static str> {
        let draft = Self {
            schema_version: 1,
            snapshot_uuid: snapshot.uuid,
            org_id: snapshot.org_id.clone(),
            namespace: snapshot.namespace.clone(),
            settings_digest: evidence_digest(settings)?,
            schema_digest: evidence_digest(schemas)?,
            mentions,
        };
        draft.validate(snapshot, schemas, settings)?;
        Ok(draft)
    }

    pub fn validate(
        &self,
        snapshot: &crate::models::SnapshotNode,
        schemas: &ObservationSchemas,
        settings: &ExtractionSettings,
    ) -> Result<(), &'static str> {
        settings
            .validate()
            .map_err(|_| "invalid raw draft settings")?;
        if self.schema_version != 1
            || self.snapshot_uuid.is_nil()
            || self.snapshot_uuid != snapshot.uuid
            || self.org_id != snapshot.org_id
            || self.namespace != snapshot.namespace
            || schemas.for_snapshot(snapshot, &self.org_id).is_err()
            || self.settings_digest != evidence_digest(settings)?
            || self.schema_digest != evidence_digest(schemas)?
        {
            return Err("raw draft scope, schema or settings mismatch");
        }
        let mut ids = std::collections::HashSet::new();
        let mut observations = std::collections::HashSet::new();
        for mention in &self.mentions {
            if mention.observation_uuid.is_nil()
                || !observations.insert(mention.observation_uuid)
                || !ids.insert(mention.id)
                || mention.id != MentionId::for_mention(self.snapshot_uuid, &mention.evidence)?
            {
                return Err("invalid or duplicate raw mention identity");
            }
        }
        validate_mentions(
            &self
                .mentions
                .iter()
                .map(|m| m.evidence.clone())
                .collect::<Vec<_>>(),
            settings,
        )?;
        if !crate::models::attribute_schema::within_json_limit(
            self,
            settings
                .max_response_bytes
                .saturating_add(settings.max_entities.saturating_mul(256))
                .saturating_add(4096),
        ) {
            return Err("raw draft exceeds byte limit");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MentionOutcome {
    Current,
    Stale,
    Deleted,
    Unresolved,
    Excluded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MentionMapping {
    pub snapshot_uuid: uuid::Uuid,
    pub mention_id: MentionId,
    pub observation_uuid: uuid::Uuid,
    pub chain_id: uuid::Uuid,
    pub version_uuid: uuid::Uuid,
    pub outcome: MentionOutcome,
}

/// Endpoint eligibility is separate from the semantic decision to create a fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MentionPairOutcome {
    Eligible,
    CollapsedChain,
    Unavailable(MentionOutcome),
}

impl MentionMapping {
    pub fn pair_outcome(&self, target: &Self) -> Result<MentionPairOutcome, &'static str> {
        if self.snapshot_uuid != target.snapshot_uuid {
            return Err("raw fact endpoints belong to different snapshots");
        }
        for outcome in [self.outcome, target.outcome] {
            if outcome != MentionOutcome::Current {
                return Ok(MentionPairOutcome::Unavailable(outcome));
            }
        }
        if self.chain_id == target.chain_id {
            return Ok(MentionPairOutcome::CollapsedChain);
        }
        Ok(MentionPairOutcome::Eligible)
    }
}

impl super::stage_output::NodeResolutionOutput {
    /// Derive mappings from the persisted observation buckets, not a second identity lookup.
    /// A completed node pass cannot contain an unresolved or missing endpoint decision.
    pub fn raw_mention_mappings(&self) -> Result<Vec<MentionMapping>, &'static str> {
        use std::collections::{HashMap, HashSet};
        let mut endpoints = HashMap::new();
        let mut add = |id, node, outcome| {
            if endpoints.insert(id, (node, outcome)).is_some() {
                Err("duplicate observation endpoint")
            } else {
                Ok(())
            }
        };
        for node in self
            .nodes_to_create
            .iter()
            .chain(self.nodes_unchanged.iter())
            .chain(self.nodes_recreated.iter())
        {
            add(node.observation_uuid, &node.value, MentionOutcome::Current)?;
        }
        for node in self
            .nodes_new_version
            .iter()
            .chain(self.nodes_volatile.iter())
        {
            add(node.observation_uuid, &node.entity, MentionOutcome::Current)?;
        }
        for node in self.nodes_stale.iter() {
            add(
                node.observation_uuid,
                &node.value,
                if node.deleted_at.is_some() {
                    MentionOutcome::Deleted
                } else {
                    MentionOutcome::Stale
                },
            )?;
        }
        for node in self.nodes_deleted.iter() {
            add(node.observation_uuid, &node.value, MentionOutcome::Deleted)?;
        }
        let mut snapshots = HashSet::new();
        let mut mappings = Vec::new();
        for draft in self.raw_text_drafts.iter() {
            if !snapshots.insert(draft.snapshot_uuid) {
                return Err("duplicate raw draft snapshot");
            }
            let mut ids = HashSet::new();
            for mention in &draft.mentions {
                if !ids.insert(mention.id) {
                    return Err("duplicate raw mention ID");
                }
                let (node, outcome) = endpoints
                    .get(&mention.observation_uuid)
                    .ok_or("raw mention has no endpoint decision")?;
                let mut originals = self
                    .observed_properties
                    .iter()
                    .filter(|p| p.observation_uuid == mention.observation_uuid);
                let original = originals.next().ok_or("raw mention has no observation")?;
                if originals.next().is_some()
                    || original.raw_mention_id != Some(mention.id)
                    || original.snapshot_uuid != draft.snapshot_uuid
                    || node.org_id != draft.org_id
                    || node.namespace != draft.namespace
                    || node.uuid.is_nil()
                    || node.chain_id.is_nil()
                {
                    return Err("raw mention endpoint scope mismatch");
                }
                if let Some(resolved_type) = &original.resolved_entity_type {
                    let schema = self
                        .schemas
                        .get(&draft.snapshot_uuid)
                        .and_then(|schemas| schemas.definitions.get(&schemas.source))
                        .ok_or("resolved classification has no observation schema")?;
                    if resolved_type != &node.entity_type
                        || schema
                            .entity_types
                            .iter()
                            .any(|declared| declared.name == mention.evidence.entity_type)
                    {
                        return Err("resolved classification conflicts with canonical type or supplied schema");
                    }
                }
                mappings.push(MentionMapping {
                    snapshot_uuid: draft.snapshot_uuid,
                    mention_id: mention.id,
                    observation_uuid: mention.observation_uuid,
                    chain_id: node.chain_id,
                    version_uuid: node.uuid,
                    outcome: *outcome,
                });
            }
        }
        Ok(mappings)
    }

    pub fn validate_raw_text_drafts(
        &self,
        settings: &ExtractionSettings,
    ) -> Result<(), &'static str> {
        for draft in self.raw_text_drafts.iter() {
            let mut snapshots = self
                .snapshot_nodes
                .iter()
                .filter(|s| s.uuid == draft.snapshot_uuid);
            let snapshot = snapshots.next().ok_or("raw draft has no snapshot")?;
            if snapshots.next().is_some() {
                return Err("duplicate raw draft snapshot");
            }
            let schemas = self
                .schemas
                .get(&draft.snapshot_uuid)
                .ok_or("raw draft has no schemas")?;
            draft.validate(snapshot, schemas, &settings.for_source(&snapshot.source))?;
        }
        let mappings = self.raw_mention_mappings()?;
        for observed in self.observed_properties.iter() {
            if let Some(id) = observed.raw_mention_id {
                if !mappings.iter().any(|mapping| {
                    mapping.observation_uuid == observed.observation_uuid
                        && mapping.snapshot_uuid == observed.snapshot_uuid
                        && mapping.mention_id == id
                }) {
                    return Err("original observation has no raw mention");
                }
            }
        }
        Ok(())
    }
}
