//! Immutable source schemas selected for one ingestion run.

use crate::models::SnapshotInput;
use crate::traits::Ontology;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Complete effective source definitions; request overrides remain in fingerprinted input.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunSchemaManifest {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub profiles: BTreeMap<String, crate::profiles::FrozenProfile>,
    pub org_id: String,
    pub sources: BTreeMap<String, Ontology>,
}

impl RunSchemaManifest {
    pub fn validate(&self, org: &str) -> Result<(), String> {
        if org.trim().is_empty() || self.org_id != org {
            return Err("schema manifest scope mismatch".into());
        }
        if self.sources.len() > 256
            || !crate::models::attribute_schema::within_json_limit(self, 1_048_576)
        {
            return Err("schema manifest exceeds limits".into());
        }
        for (source, profile) in &self.profiles {
            if !self.sources.contains_key(source) {
                return Err("profile source missing from manifest".into());
            }
            profile.validate().map_err(|e| e.to_string())?;
        }
        for (source, schema) in &self.sources {
            text(source)?;
            validate_definitions(schema)?;
        }
        Ok(())
    }

    pub fn effective(&self, input: &SnapshotInput, source: &str) -> Result<Ontology, String> {
        if self.profiles.contains_key(source)
            && (input.entity_types.is_some()
                || input.edge_types.is_some()
                || input.edge_type_map.is_some())
        {
            return Err("profile-bound input cannot override schemas".into());
        }
        let base = self
            .sources
            .get(source)
            .ok_or("source missing from frozen schema manifest")?;
        let mut effective = base.overlaid_with(&input_definitions(input));
        if let Some(map) = &input.edge_type_map {
            effective.edge_type_map = map.clone();
            if effective.allowed_relationships.is_some() {
                effective.allowed_relationships =
                    crate::traits::ontology_store::intersect_signatures(
                        effective.allowed_relationships.as_deref(),
                        Some(map),
                    );
            }
        }
        validate_effective(&effective)?;
        Ok(effective)
    }

    pub fn validate_inputs(&self, org: &str, inputs: &[SnapshotInput]) -> Result<(), String> {
        self.validate(org)?;
        if self.sources.keys().cloned().collect::<BTreeSet<_>>() != sources(inputs) {
            return Err("schema manifest sources do not match request".into());
        }
        for input in inputs {
            for source in sources(std::slice::from_ref(input)) {
                self.effective(input, &source)?;
            }
        }
        Ok(())
    }
}

pub fn sources(inputs: &[SnapshotInput]) -> BTreeSet<String> {
    inputs
        .iter()
        .flat_map(|input| {
            std::iter::once(input.source.clone())
                .chain(input.entities.iter().map(|entity| entity.source.clone()))
        })
        .collect()
}

pub fn input_definitions(input: &SnapshotInput) -> Ontology {
    Ontology {
        entity_types: input.entity_types.clone().unwrap_or_default(),
        edge_types: input.edge_types.clone().unwrap_or_default(),
        edge_type_map: input.edge_type_map.clone().unwrap_or_default(),
        ..Default::default()
    }
}

pub fn validate_input_schemas(input: &SnapshotInput) -> Result<(), String> {
    if !crate::models::attribute_schema::within_json_limit(
        &(&input.entity_types, &input.edge_types, &input.edge_type_map),
        262_144,
    ) {
        return Err("input schemas exceed byte limit".into());
    }
    validate_definitions(&input_definitions(input))
}

fn text(value: &str) -> Result<(), String> {
    if value.trim().is_empty() || value.len() > 4096 {
        Err("invalid schema identifier or description".into())
    } else {
        Ok(())
    }
}

/// Local validation; references to inherited types are checked after composition.
pub fn validate_definitions(schema: &Ontology) -> Result<(), String> {
    if !crate::models::attribute_schema::within_json_limit(schema, 262_144) {
        return Err("source schema exceeds byte limit".into());
    }
    let mut entities = BTreeSet::new();
    for entity in &schema.entity_types {
        text(&entity.name)?;
        if !entities.insert(&entity.name) {
            return Err("duplicate entity definition".into());
        }
        if let Some(d) = &entity.description {
            text(d)?;
        }
        if let Some(attributes) = &entity.attributes {
            attributes.validate()?;
        }
        let mut properties = BTreeSet::new();
        for property in &entity.properties {
            text(&property.name)?;
            if !properties.insert(&property.name) {
                return Err("duplicate property hint".into());
            }
            if let Some(d) = &property.description {
                text(d)?;
            }
        }
        if entity.identity_properties.is_empty() {
            continue;
        }
        if entity.identity_properties.len() > 4 {
            return Err("too many entity identifying properties".into());
        }
        let mut keys = BTreeSet::new();
        for key in &entity.identity_properties {
            text(key)?;
            if !keys.insert(key) || (key != "name" && !properties.contains(key)) {
                return Err("invalid entity identifying property".into());
            }
        }
    }
    let mut edges = BTreeSet::new();
    for edge in &schema.edge_types {
        let mut keys = BTreeSet::new();
        if edge.identifying_properties.len() > 64 {
            return Err("too many relationship identifying properties".into());
        }
        for key in &edge.identifying_properties {
            text(key)?;
            if key.split('.').any(|part| part.trim().is_empty()) || !keys.insert(key) {
                return Err("invalid or duplicate relationship identifying property".into());
            }
        }
        text(&edge.name)?;
        if !edges.insert(&edge.name) {
            return Err("duplicate relationship definition".into());
        }
        for value in [&edge.description, &edge.source_type, &edge.target_type]
            .into_iter()
            .flatten()
        {
            text(value)?;
        }
        if let Some(attributes) = &edge.attributes {
            attributes.validate()?;
        }
    }
    for entries in std::iter::once(&schema.edge_type_map).chain(schema.allowed_relationships.iter())
    {
        let mut signatures = BTreeSet::new();
        for entry in entries {
            for name in [&entry.source_type, &entry.target_type, &entry.edge_name] {
                text(name)?;
            }
            if let Some(d) = &entry.description {
                text(d)?;
            }
            if !signatures.insert((&entry.source_type, &entry.target_type, &entry.edge_name)) {
                return Err("duplicate relationship mapping".into());
            }
        }
        if entities.len() > 128 || edges.len() > 128 || signatures.len() > 512 {
            return Err("too many schema definitions".into());
        }
    }
    let mut vocabulary = BTreeSet::new();
    for name in &schema.relationship_vocabulary {
        text(name)?;
        let key = crate::traits::ontology_store::normalize_relationship(name);
        if !vocabulary.insert(if key.is_empty() { name.clone() } else { key }) {
            return Err("ambiguous relationship vocabulary".into());
        }
    }
    let mut aliases = BTreeMap::new();
    for (alias, target) in &schema.relation_aliases {
        text(alias)?;
        text(target)?;
        let normalized = crate::traits::ontology_store::normalize_relationship(alias);
        if normalized.is_empty() {
            return Err("invalid relationship alias".into());
        }
        if aliases
            .insert(normalized, target)
            .is_some_and(|old| old != target)
        {
            return Err("conflicting normalized aliases".into());
        }
    }
    Ok(())
}

pub fn validate_effective(schema: &Ontology) -> Result<(), String> {
    validate_definitions(schema)?;
    let entity_exists =
        |name: &str| name == "Entity" || schema.entity_types.iter().any(|e| e.name == name);
    for edge in &schema.edge_types {
        if edge
            .source_type
            .as_deref()
            .is_some_and(|s| !entity_exists(s))
            || edge
                .target_type
                .as_deref()
                .is_some_and(|s| !entity_exists(s))
        {
            return Err("relationship definition references an unknown entity type".into());
        }
    }
    for entry in schema
        .edge_type_map
        .iter()
        .chain(schema.allowed_relationships.iter().flatten())
    {
        if !entity_exists(&entry.source_type) || !entity_exists(&entry.target_type) {
            return Err("mapping references an unknown entity type".into());
        }
        let edge = schema
            .edge_types
            .iter()
            .find(|edge| edge.name == entry.edge_name)
            .ok_or("mapping references an unknown relationship type")?;
        if edge
            .source_type
            .as_deref()
            .is_some_and(|s| s != "Entity" && s != entry.source_type)
            || edge
                .target_type
                .as_deref()
                .is_some_and(|s| s != "Entity" && s != entry.target_type)
        {
            return Err("mapping contradicts relationship endpoints".into());
        }
    }
    Ok(())
}

/// Effective definitions bound to the observation that produced entities and relationships.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationSchemas {
    pub org_id: String,
    pub source: String,
    pub definitions: BTreeMap<String, Ontology>,
}

impl RunSchemaManifest {
    pub fn observation(
        &self,
        org: &str,
        input: &SnapshotInput,
    ) -> Result<ObservationSchemas, String> {
        if org.trim().is_empty() || self.org_id != org {
            return Err("schema manifest scope mismatch".into());
        }
        let definitions = sources(std::slice::from_ref(input))
            .into_iter()
            .map(|source| {
                self.effective(input, &source)
                    .map(|schema| (source, schema))
            })
            .collect::<Result<_, _>>()?;
        Ok(ObservationSchemas {
            org_id: org.into(),
            source: input.source.clone(),
            definitions,
        })
    }
}

impl ObservationSchemas {
    pub fn for_snapshot(
        &self,
        snapshot: &crate::models::SnapshotNode,
        org: &str,
    ) -> Result<&Ontology, String> {
        if self.org_id != org || snapshot.org_id != org || self.source != snapshot.source {
            return Err("observation schema scope mismatch".into());
        }
        let schema = self
            .definitions
            .get(&self.source)
            .ok_or("observation schema source is missing")?;
        validate_effective(schema)?;
        Ok(schema)
    }
}

#[cfg(test)]
mod tests;
