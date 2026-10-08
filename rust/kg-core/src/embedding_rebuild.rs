//! Bounded, awaited vector refresh. Entity and relationship history is retained;
//! derived views keep their own visibility and evidence guards.
use std::time::Duration;

use serde_json::{Map, Value};
use uuid::Uuid;

use crate::{
    embedding::*,
    errors::BackendError,
    traits::{EmbedBackend, GraphBackend},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddingKind {
    Entity,
    Relationship,
    DerivedSummary,
    CommunityName,
}
impl EmbeddingKind {
    pub fn storage_properties(self) -> [&'static str; 4] {
        match self {
            Self::Entity | Self::Relationship => [
                "embedding",
                "embedding_model",
                "embedding_text_version",
                "embedding_content_hash",
            ],
            Self::CommunityName => [
                "name_embedding",
                "name_embedding_model",
                "name_embedding_text_version",
                "name_embedding_content_hash",
            ],
            Self::DerivedSummary => [
                "summary_embedding",
                "summary_embedding_model",
                "summary_embedding_text_version",
                "summary_embedding_content_hash",
            ],
        }
    }
    pub fn text_version(self, settings: &EmbeddingSettings) -> &str {
        match self {
            Self::Entity => &settings.text_version,
            Self::Relationship => RELATIONSHIP_TEXT_VERSION,
            Self::DerivedSummary => crate::entity_summary::SUMMARY_TEXT_VERSION,
            Self::CommunityName => crate::community::NAME_TEXT_VERSION,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EmbeddingRecord {
    pub uuid: Uuid,
    pub properties: Map<String, Value>,
}
impl EmbeddingRecord {
    pub fn text(
        &self,
        kind: EmbeddingKind,
        fields: &EntityEmbeddingFields,
    ) -> Result<String, BackendError> {
        let string = |key: &str| {
            self.properties
                .get(key)
                .and_then(Value::as_str)
                .unwrap_or("")
        };
        Ok(match kind {
            EmbeddingKind::CommunityName => {
                let name = string("name");
                if name.trim().is_empty()
                    || name.len() > crate::community::MAX_ENTITY_TEXT_BYTES
                    || string("generation_uuid").parse::<Uuid>().is_err()
                    || string("revision").parse::<Uuid>().is_err()
                    || string("namespace").trim().is_empty()
                    || self.properties.get("dirty") != Some(&Value::Bool(false))
                    || string("projected_at")
                        .parse::<chrono::DateTime<chrono::Utc>>()
                        .is_err()
                    || self.properties.get("valid_until").is_some_and(|value| {
                        !value.is_null()
                            && value.as_str().is_none_or(|text| {
                                text.parse::<chrono::DateTime<chrono::Utc>>().is_err()
                            })
                    })
                {
                    return Err(BackendError::Deserialization(
                        "invalid stored Community name evidence".into(),
                    ));
                }
                name.to_owned()
            }
            EmbeddingKind::DerivedSummary => {
                let summary = string("derived_summary");
                if summary.trim().is_empty()
                    || string("summary_revision").parse::<Uuid>().is_err()
                    || string("summary_evidence_hash").is_empty()
                    || string("summary_as_of")
                        .parse::<chrono::DateTime<chrono::Utc>>()
                        .is_err()
                {
                    return Err(BackendError::Deserialization(
                        "invalid stored derived summary".into(),
                    ));
                }
                crate::entity_summary::embedding_text(summary)
            }
            EmbeddingKind::Relationship => {
                relationship_representation(string("name"), string("description"))
            }
            EmbeddingKind::Entity => {
                let props = crate::traits::property_codec::read_properties(&self.properties)
                    .map_err(BackendError::Deserialization)?;
                let strings = |key: &str| -> Vec<String> {
                    self.properties
                        .get(key)
                        .and_then(Value::as_array)
                        .map(|values| {
                            values
                                .iter()
                                .filter_map(Value::as_str)
                                .map(str::to_owned)
                                .collect()
                        })
                        .unwrap_or_default()
                };
                let primary = strings("primary_key_properties");
                let additional: Vec<Vec<String>> = self
                    .properties
                    .get("additional_key_properties")
                    .and_then(Value::as_str)
                    .and_then(|raw| serde_json::from_str(raw).ok())
                    .unwrap_or_default();
                let keys = crate::embedding::key_properties(&primary, &additional);
                let labels = strings("labels");
                representation(
                    &crate::embedding::EntityText {
                        entity_type: string("entity_type"),
                        name: string("name"),
                        summary: self.properties.get("summary").and_then(Value::as_str),
                        properties: &props,
                        key_properties: &keys,
                        labels: &labels,
                    },
                    fields,
                )
            }
        })
    }
    fn compatible(&self, kind: EmbeddingKind, settings: &EmbeddingSettings, hash: &str) -> bool {
        let [vector, model, version, hash_property] = kind.storage_properties();
        self.properties.get(model).and_then(Value::as_str) == Some(&settings.model)
            && self.properties.get(version).and_then(Value::as_str)
                == Some(kind.text_version(settings))
            && self.properties.get(hash_property).and_then(Value::as_str) == Some(hash)
            && self
                .properties
                .get(vector)
                .and_then(Value::as_array)
                .is_some_and(|values| {
                    values.len() == settings.dimension
                        && values.iter().all(|v| {
                            v.as_f64()
                                .is_some_and(|f| f.is_finite() && (f as f32).is_finite())
                        })
                        && values
                            .iter()
                            .any(|v| v.as_f64().is_some_and(|f| (f as f32) != 0.0))
                })
    }
}

#[derive(Debug, Clone)]
pub struct EmbeddingRefresh {
    pub entity_fields: EntityEmbeddingFields,
    pub record: EmbeddingRecord,
    pub embedding: ComputedEmbedding,
}

#[derive(Debug, Clone)]
pub struct RebuildOptions {
    pub entity_fields: EntityEmbeddingFields,
    pub batch_size: usize,
    pub max_passes: usize,
    pub timeout: Duration,
}
impl Default for RebuildOptions {
    fn default() -> Self {
        Self {
            entity_fields: EntityEmbeddingFields::default(),
            batch_size: 64,
            max_passes: 3,
            timeout: Duration::from_secs(300),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RebuildReport {
    pub scanned: usize,
    pub updated: usize,
    pub reused: usize,
    pub conflicts: usize,
    pub passes: usize,
}

/// Configure all writers with the new provider first, then await this maintenance
/// operation before declaring semantic search ready. Each write compares the read
/// properties under lock; concurrent changes are revisited on the next bounded pass.
/// Dropping the future cancels work. Retrying reuses already compatible vectors.
#[tracing::instrument(name = "embedding.rebuild", skip_all)]
pub async fn rebuild_embeddings(
    graph: &dyn GraphBackend,
    provider: &dyn EmbedBackend,
    org: &str,
    options: &RebuildOptions,
) -> Result<RebuildReport, BackendError> {
    if org.trim().is_empty()
        || options.batch_size == 0
        || options.batch_size > 256
        || options.max_passes == 0
        || options.max_passes > 10
        || options.timeout.is_zero()
        || provider.max_batch_size() == 0
    {
        return Err(BackendError::Query(
            "invalid embedding rebuild scope or budget".into(),
        ));
    }
    let settings = EmbeddingSettings::of(provider)
        .map_err(|e| BackendError::Query(e.to_string()))?
        .with_entity_fields(options.entity_fields.clone())?;
    let size = options.batch_size.min(provider.max_batch_size());
    let work = async {
        let mut report = RebuildReport::default();
        for pass in 1..=options.max_passes {
            report.passes = pass;
            let mut pass_conflicts = 0;
            for kind in [
                EmbeddingKind::Entity,
                EmbeddingKind::Relationship,
                EmbeddingKind::DerivedSummary,
                EmbeddingKind::CommunityName,
            ] {
                let mut after = None;
                loop {
                    let records = graph.embedding_records(org, kind, after, size).await?;
                    if records.is_empty() {
                        break;
                    }
                    if records.len() > size
                        || records.windows(2).any(|pair| pair[0].uuid >= pair[1].uuid)
                        || after.is_some_and(|id| records[0].uuid <= id)
                    {
                        return Err(BackendError::Deserialization(
                            "invalid embedding maintenance page".into(),
                        ));
                    }
                    after = records.last().map(|r| r.uuid);
                    report.scanned += records.len();
                    let mut pending = Vec::new();
                    for record in records {
                        let text = record.text(kind, &settings.entity_fields)?;
                        let hash = content_hash(&text);
                        if record.compatible(kind, &settings, &hash) {
                            report.reused += 1;
                        } else {
                            pending.push((record, text, hash));
                        }
                    }
                    if pending.is_empty() {
                        continue;
                    }
                    let texts: Vec<_> = pending.iter().map(|(_, text, _)| text.as_str()).collect();
                    let vectors = provider.embed_batch(&texts).await?;
                    validate_vectors(&settings, pending.len(), &vectors)?;
                    let updates: Vec<_> = pending
                        .into_iter()
                        .zip(vectors)
                        .map(|((record, _, hash), values)| EmbeddingRefresh {
                            entity_fields: settings.entity_fields.clone(),
                            record,
                            embedding: ComputedEmbedding {
                                model: settings.model.clone(),
                                text_version: kind.text_version(&settings).into(),
                                content_hash: hash,
                                values,
                            },
                        })
                        .collect();
                    let written = graph.refresh_embeddings(org, kind, &updates).await?;
                    if written > updates.len() {
                        return Err(BackendError::Deserialization(
                            "invalid embedding refresh count".into(),
                        ));
                    }
                    report.updated += written;
                    pass_conflicts += updates.len() - written;
                }
            }
            report.conflicts += pass_conflicts;
            let mut remaining = graph.incompatible_embeddings(org, &settings).await?;
            // Recheck canonical content after writes; metadata alone cannot detect
            // content changed after an earlier page was scanned.
            for kind in [
                EmbeddingKind::Entity,
                EmbeddingKind::Relationship,
                EmbeddingKind::DerivedSummary,
                EmbeddingKind::CommunityName,
            ] {
                let mut after = None;
                loop {
                    let records = graph.embedding_records(org, kind, after, size).await?;
                    if records.is_empty() {
                        break;
                    }
                    if records.len() > size
                        || records.windows(2).any(|p| p[0].uuid >= p[1].uuid)
                        || after.is_some_and(|id| records[0].uuid <= id)
                    {
                        return Err(BackendError::Deserialization(
                            "invalid embedding verification page".into(),
                        ));
                    }
                    after = records.last().map(|r| r.uuid);
                    for record in &records {
                        if !record.compatible(
                            kind,
                            &settings,
                            &content_hash(&record.text(kind, &settings.entity_fields)?),
                        ) {
                            remaining += 1;
                        }
                    }
                }
            }
            tracing::info!(
                pass,
                updated = report.updated,
                reused = report.reused,
                conflicts = pass_conflicts,
                remaining,
                "embedding rebuild pass finished"
            );
            if remaining == 0 && pass_conflicts == 0 {
                return Ok(report);
            }
        }
        Err(BackendError::Conflict("embedding rebuild could not converge within its pass budget; retry after concurrent changes settle".into()))
    };
    tokio::time::timeout(options.timeout, work)
        .await
        .map_err(|_| {
            BackendError::Timeout(options.timeout.as_millis().min(u64::MAX as u128) as u64)
        })?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compatibility_requires_a_nonzero_f32_vector() {
        let settings = EmbeddingSettings {
            entity_fields: Default::default(),
            model: "model".into(),
            dimension: 1,
            text_version: TEXT_VERSION.into(),
        };
        let mut record = EmbeddingRecord {
            uuid: Uuid::new_v4(),
            properties: serde_json::json!({
                "embedding_model": "model", "embedding_text_version": TEXT_VERSION,
                "embedding_content_hash": "hash", "embedding": [1e-300]
            })
            .as_object()
            .unwrap()
            .clone(),
        };
        assert!(!record.compatible(EmbeddingKind::Entity, &settings, "hash"));
        record
            .properties
            .insert("embedding".into(), serde_json::json!([1.0]));
        assert!(record.compatible(EmbeddingKind::Entity, &settings, "hash"));
    }
}
