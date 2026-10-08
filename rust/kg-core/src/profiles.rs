//! Versioned, caller-selected domain definitions. Profiles grant no provider or tenant authority.
use crate::{
    errors::BackendError,
    runtime::{extraction::SourceExtractionGuidance, schemas::validate_effective},
    traits::Ontology,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub type ProfileBindings = BTreeMap<String, ProfileRef>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileRef {
    pub profile_id: String,
    pub revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileMode {
    Open,
    Strict,
}

/// Only prose may augment host extraction settings. Provider policy and reference mappings remain host owned.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProfileGuidance {
    pub shared_instructions: Option<String>,
    pub instructions: Option<String>,
    pub relationship_instructions: Option<String>,
    pub identity_instructions: Option<String>,
}
impl ProfileGuidance {
    pub fn as_source_guidance(&self) -> SourceExtractionGuidance {
        SourceExtractionGuidance {
            shared_instructions: self.shared_instructions.clone(),
            instructions: self.instructions.clone(),
            relationship_instructions: self.relationship_instructions.clone(),
            identity_instructions: self.identity_instructions.clone(),
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub format_version: u32,
    pub profile_id: String,
    pub revision: u64,
    pub mode: ProfileMode,
    pub ontology: Ontology,
    #[serde(default)]
    pub source_guidance: BTreeMap<String, ProfileGuidance>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenProfile {
    pub document: Profile,
    pub digest: String,
}

pub fn invalid(message: &str) -> BackendError {
    BackendError::Query(format!("profile: {message}"))
}
pub fn validate_org(org: &str) -> Result<(), BackendError> {
    if org.trim().is_empty() || org.len() > 4096 {
        return Err(invalid("invalid organization"));
    }
    Ok(())
}
impl ProfileRef {
    pub fn validate(&self) -> Result<(), BackendError> {
        if self.profile_id.is_empty()
            || self.profile_id.len() > 128
            || !self
                .profile_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            || self.revision == 0
            || self.revision > i64::MAX as u64
        {
            return Err(invalid("invalid id or revision"));
        }
        Ok(())
    }
}
pub fn validate_bindings(bindings: &ProfileBindings) -> Result<(), BackendError> {
    if bindings.len() > 256 {
        return Err(invalid("too many source bindings"));
    }
    for (source, reference) in bindings {
        if source.trim().is_empty() || source.len() > 4096 {
            return Err(invalid("invalid source"));
        }
        reference.validate()?;
    }
    Ok(())
}
impl Profile {
    pub fn reference(&self) -> ProfileRef {
        ProfileRef {
            profile_id: self.profile_id.clone(),
            revision: self.revision,
        }
    }
    pub fn validate(&self) -> Result<(), BackendError> {
        self.reference().validate()?;
        if self.format_version != 1
            || !crate::models::attribute_schema::within_json_limit(self, 262_144)
            || self.source_guidance.len() > 256
        {
            return Err(invalid("unsupported format or oversized document"));
        }
        validate_effective(&canonical_patterns(&self.ontology)?)
            .map_err(|_| invalid("invalid ontology definitions"))?;
        for (source, guidance) in &self.source_guidance {
            if source.trim().is_empty() || source.len() > 4096 {
                return Err(invalid("invalid guidance source"));
            }
            for text in [
                &guidance.shared_instructions,
                &guidance.instructions,
                &guidance.relationship_instructions,
                &guidance.identity_instructions,
            ]
            .into_iter()
            .flatten()
            {
                if text.trim().is_empty() || text.len() > 16_384 {
                    return Err(invalid("empty or oversized guidance"));
                }
            }
        }
        for entity in &self.ontology.entity_types {
            for hint in &entity.properties {
                if hint.required
                    && !entity.attributes.as_ref().is_some_and(|schema| {
                        schema
                            .0
                            .get("required")
                            .and_then(|v| v.as_array())
                            .is_some_and(|required| {
                                required.iter().any(|v| v.as_str() == Some(&hint.name))
                            })
                    })
                {
                    return Err(invalid(
                        "required property hints must have a required attribute schema",
                    ));
                }
            }
        }
        // Canonical names must not alias away a declared name or have ambiguous normalized aliases.
        let mut aliases = BTreeMap::new();
        for (alias, target) in &self.ontology.relation_aliases {
            let key = crate::traits::ontology_store::normalize_relationship(alias);
            if key.is_empty()
                || aliases
                    .insert(key, target)
                    .is_some_and(|prior| prior != target)
                || (self.ontology.edge_types.iter().any(|e| e.name == *alias) && alias != target)
            {
                return Err(invalid("conflicting relationship aliases"));
            }
        }
        Ok(())
    }
    pub fn freeze(&self) -> Result<FrozenProfile, BackendError> {
        self.validate()?;

        let value =
            canonical(serde_json::to_value(self).map_err(|_| invalid("serialization failed"))?);
        let bytes = serde_json::to_vec(&value).map_err(|_| invalid("serialization failed"))?;
        Ok(FrozenProfile {
            document: self.clone(),
            digest: format!("{:x}", Sha256::digest(bytes)),
        })
    }
    /// Reject replacement of host definitions; overlay only compatible additions and intersect restrictions.
    pub fn compose(&self, host: &Ontology) -> Result<Ontology, BackendError> {
        self.validate()?;
        for entity in &self.ontology.entity_types {
            if host
                .entity_types
                .iter()
                .any(|e| e.name == entity.name && e != entity)
            {
                return Err(invalid("entity definition conflicts with host"));
            }
        }
        for edge in &self.ontology.edge_types {
            if host
                .edge_types
                .iter()
                .any(|e| e.name == edge.name && e != edge)
            {
                return Err(invalid("relationship definition conflicts with host"));
            }
        }
        for (alias, target) in &self.ontology.relation_aliases {
            if host.relation_aliases.iter().any(|(a, t)| {
                crate::traits::ontology_store::normalize_relationship(a)
                    == crate::traits::ontology_store::normalize_relationship(alias)
                    && t != target
            }) {
                return Err(invalid("alias conflicts with host"));
            }
        }
        let mut overlay = canonical_patterns(&self.ontology)?;
        let host = canonical_patterns(host)?;
        if self.mode == ProfileMode::Strict {
            overlay.allowed_relationships = crate::traits::ontology_store::intersect_signatures(
                Some(&overlay.edge_type_map),
                overlay.allowed_relationships.as_deref(),
            );
        }
        let effective = host.overlaid_with(&overlay);
        for definition in &effective.edge_types {
            if effective
                .canonical_relationship(&definition.name)
                .is_some_and(|name| name != definition.name)
            {
                return Err(invalid(
                    "alias or vocabulary contradicts a declared relationship name",
                ));
            }
        }
        validate_effective(&effective).map_err(|_| invalid("invalid composed ontology"))?;
        Ok(effective)
    }
    pub fn permits_entity(&self, entity_type: &str) -> bool {
        self.mode == ProfileMode::Open
            || self
                .ontology
                .entity_types
                .iter()
                .any(|e| e.name == entity_type)
    }
}
impl FrozenProfile {
    pub fn validate(&self) -> Result<(), BackendError> {
        if self.document.freeze()?.digest != self.digest {
            return Err(invalid("digest mismatch"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn profile() -> Profile {
        serde_json::from_value(serde_json::json!({"format_version":1,"profile_id":"example","revision":1,"mode":"strict","ontology":{"entity_types":[{"name":"Service"}]}})).unwrap()
    }
    #[test]
    fn strict_empty_patterns_and_host_conflicts() {
        let p = profile();
        let schema = p.compose(&Ontology::default()).unwrap();
        assert!(!schema.permits_signature("Service", "Service", "CALLS"));
        assert!(p.permits_entity("Service"));
        assert!(!p.permits_entity("Other"));
        let mut host = p.ontology.clone();
        host.entity_types[0].description = Some("different".into());
        assert!(p.compose(&host).is_err());
    }
    #[test]
    fn profile_example_and_alias_patterns_validate() {
        let sample: Profile = serde_json::from_str(include_str!(
            "../../../python/kg-sdk/tests/fixtures/profiles/operations.json"
        ))
        .unwrap();
        sample.validate().unwrap();
        sample.compose(&Ontology::default()).unwrap();
        let aliases:Profile=serde_json::from_value(serde_json::json!({"format_version":1,"profile_id":"alias","revision":1,"mode":"strict","ontology":{"entity_types":[{"name":"Service"}],"edge_types":[{"name":"CALLS"}],"relation_aliases":{"invokes":"CALLS"},"edge_type_map":[{"source_type":"Service","target_type":"Service","edge_name":"invokes"}]}})).unwrap();
        let effective = aliases.compose(&Ontology::default()).unwrap();
        assert!(effective.permits_signature("Service", "Service", "CALLS"));
        assert!(!effective.permits_signature("Service", "Other", "CALLS"));
    }

    #[test]
    fn profile_invalid_definitions_are_rejected_before_registry_access() {
        for bad in [
            serde_json::json!({"entity_types":[{"name":"Service","properties":[{"name":"id","required":true}]}]}),
            serde_json::json!({"entity_types":[{"name":"Service","identity_properties":["id","id"]}]}),
            serde_json::json!({"entity_types":[{"name":"Service","attributes":{"type":"object","$ref":"https://example.com/schema"}}]}),
            serde_json::json!({"entity_types":[{"name":"Service"}],"edge_type_map":[{"source_type":"Missing","target_type":"Service","edge_name":"CALLS"}]}),
        ] {
            let mut candidate = serde_json::to_value(profile()).unwrap();
            candidate["ontology"] = bad;
            let result = serde_json::from_value::<Profile>(candidate);
            assert!(result.is_err() || result.unwrap().validate().is_err());
        }
        let mut oversized = profile();
        oversized.source_guidance.insert(
            "source".into(),
            ProfileGuidance {
                instructions: Some("x".repeat(16_385)),
                ..Default::default()
            },
        );
        assert!(oversized.validate().is_err());
    }

    #[test]
    fn immutable_digest_and_bounded_contract() {
        let mut frozen = profile().freeze().unwrap();
        frozen.validate().unwrap();
        frozen.document.revision = 2;
        assert!(frozen.validate().is_err());
        let mut p = profile();
        p.revision = 0;
        assert!(p.validate().is_err());
        assert!(serde_json::from_value::<Profile>(serde_json::json!({"format_version":1,"profile_id":"x","revision":1,"mode":"open","ontology":{},"models":{}})).is_err());
    }
}

/// Fingerprint the bound evidence contract used by stored-reference follow-ups.
/// A different contract requires reingesting the source, not silently reinterpreting it.
pub fn evidence_contract(
    manifest: Option<&crate::runtime::schemas::RunSchemaManifest>,
    source: &str,
) -> Result<Option<String>, BackendError> {
    let Some(manifest) = manifest else {
        return Ok(None);
    };
    let Some(profile) = manifest.profiles.get(source) else {
        return Ok(None);
    };
    let schema = manifest
        .sources
        .get(source)
        .ok_or_else(|| invalid("missing source schema"))?;
    let bytes = serde_json::to_vec(&canonical(
        serde_json::to_value((profile, schema))
            .map_err(|_| invalid("invalid evidence contract"))?,
    ))
    .map_err(|_| invalid("invalid evidence contract"))?;
    Ok(Some(format!("{:x}", Sha256::digest(bytes))))
}

/// Bounded names/identifiers only; never source payloads or extracted values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileDiagnostic {
    pub source: String,
    pub snapshot_id: uuid::Uuid,
    pub profile: ProfileRef,
    pub reason: String,
    pub type_name: String,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProfileDiagnostics {
    pub items: Vec<ProfileDiagnostic>,
    pub omitted: usize,
}
impl ProfileDiagnostics {
    pub fn is_empty(&self) -> bool {
        self.items.is_empty() && self.omitted == 0
    }
    pub fn push(&mut self, item: ProfileDiagnostic) {
        if self.items.len() < 128 {
            self.items.push(item);
        } else {
            self.omitted = self.omitted.saturating_add(1);
        }
    }
    pub fn extend(&mut self, other: &Self) {
        for item in &other.items {
            self.push(item.clone());
        }
        self.omitted = self.omitted.saturating_add(other.omitted);
    }
    pub fn undeclared(
        &mut self,
        manifest: Option<&crate::runtime::schemas::RunSchemaManifest>,
        source: &str,
        snapshot_id: uuid::Uuid,
        type_name: &str,
        relationship: bool,
    ) {
        let Some(profile) = manifest.and_then(|m| m.profiles.get(source)) else {
            return;
        };
        if profile.document.mode != ProfileMode::Open {
            return;
        }
        let declared = if relationship {
            profile
                .document
                .ontology
                .edge_types
                .iter()
                .any(|t| t.name == type_name)
        } else {
            profile
                .document
                .ontology
                .entity_types
                .iter()
                .any(|t| t.name == type_name)
        };
        if !declared {
            self.push(ProfileDiagnostic {
                source: source.into(),
                snapshot_id,
                profile: profile.document.reference(),
                reason: if relationship {
                    "undeclared_relationship"
                } else {
                    "undeclared_entity"
                }
                .into(),
                type_name: type_name.chars().take(256).collect(),
            });
        }
    }
}

fn canonical(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.into_iter()
                .map(|(k, v)| (k, canonical(v)))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(canonical).collect())
        }
        other => other,
    }
}

fn canonical_patterns(schema: &Ontology) -> Result<Ontology, BackendError> {
    let mut result = schema.clone();
    for entry in result
        .edge_type_map
        .iter_mut()
        .chain(result.allowed_relationships.iter_mut().flatten())
    {
        entry.edge_name = schema
            .canonical_relationship(&entry.edge_name)
            .unwrap_or_else(|| entry.edge_name.clone());
    }
    Ok(result)
}

/// Safe validation detail carried beside a failed snapshot's original input index.
/// Contains schema names/paths, never rejected values or source content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileViolation {
    pub source: String,
    pub profile: ProfileRef,
    pub reason: String,
    pub property_path: String,
    pub type_name: String,
}
