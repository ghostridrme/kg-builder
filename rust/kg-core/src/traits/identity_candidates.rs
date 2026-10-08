//! Bounded identity evidence; retrieval scores are clues, never identity proof.

use super::{
    graph_backend::{validate_embedding, GraphEmbedding},
    EntityVersionRecord, IdentityScope,
};
use crate::{errors::BackendError, models::PropertyValue};
use uuid::Uuid;

pub const MAX_IDENTITY_CANDIDATES: usize = 100;
pub const MAX_IDENTITY_QUERY_NAMES: usize = 64;
pub const MAX_IDENTITY_PROPERTY_PAIRS: usize = 128;
pub const MAX_IDENTITY_PROPERTY_BYTES: usize = 32_768;

#[derive(Debug, Clone)]
pub enum IdentityCandidateQuery {
    /// Literal names only; callers can require exhaustive coverage independently of keyword recall.
    ExactNames(Vec<String>),
    /// Ranked name recall, including fulltext token matches.
    Names(Vec<String>),
    /// Exact typed agreements rank candidates without establishing their identity.
    PropertyOverlap(Vec<(String, PropertyValue)>),
    Similarity {
        embedding: GraphEmbedding,
        text_version: String,
        min_score: f32,
    },
}

#[derive(Debug, Clone)]
pub struct IdentityCandidateRequest {
    /// A type of `*` searches the namespace across inferred classifications.
    pub scope: IdentityScope,
    pub query: IdentityCandidateQuery,
    pub exclude_chains: Vec<Uuid>,
    pub limit: usize,
}

impl IdentityCandidateRequest {
    pub fn validate(&self, org: &str) -> Result<(), BackendError> {
        self.scope.key(org)?;
        if !(1..=MAX_IDENTITY_CANDIDATES).contains(&self.limit)
            || self.exclude_chains.len() > MAX_IDENTITY_CANDIDATES
            || self.exclude_chains.iter().any(Uuid::is_nil)
        {
            return Err(BackendError::Query(
                "invalid identity candidate limits".into(),
            ));
        }
        match &self.query {
            IdentityCandidateQuery::ExactNames(names) | IdentityCandidateQuery::Names(names) => {
                if names.is_empty()
                    || names.len() > MAX_IDENTITY_QUERY_NAMES
                    || names
                        .iter()
                        .map(|s| s.split_whitespace().count())
                        .sum::<usize>()
                        > 256
                    || names.iter().any(|s| s.trim().is_empty() || s.len() > 4096)
                {
                    return Err(BackendError::Query(
                        "invalid identity candidate names".into(),
                    ));
                }
            }
            IdentityCandidateQuery::PropertyOverlap(properties) => {
                if properties.is_empty() || properties.len() > MAX_IDENTITY_PROPERTY_PAIRS {
                    return Err(BackendError::Query(
                        "property evidence exceeds candidate query budget".into(),
                    ));
                }
                let mut seen = std::collections::HashSet::new();
                let mut bytes = 0;
                for (key, value) in properties {
                    let supported = match value {
                        PropertyValue::String(value) => {
                            !value.trim().is_empty() && value.len() <= 4096
                        }
                        PropertyValue::Integer(_) | PropertyValue::Bool(_) => true,
                        PropertyValue::Float(value) => value.is_finite(),
                        _ => false,
                    };
                    if key.trim().is_empty() || key.len() > 1024 || !supported {
                        return Err(BackendError::Query("invalid property evidence".into()));
                    }
                    let encoded = serde_json::to_string(value)
                        .map_err(|_| BackendError::Query("invalid property evidence".into()))?;
                    bytes += key.len() + encoded.len();
                    if !seen.insert((key, encoded)) {
                        return Err(BackendError::Query("invalid property evidence".into()));
                    }
                }
                if bytes > MAX_IDENTITY_PROPERTY_BYTES {
                    return Err(BackendError::Query(
                        "property evidence exceeds candidate query budget".into(),
                    ));
                }
            }
            IdentityCandidateQuery::Similarity {
                embedding,
                text_version,
                min_score,
            } => {
                validate_embedding(&embedding.model, &embedding.values)?;
                if embedding.values.len() > 16_384 {
                    return Err(BackendError::Query(
                        "identity candidate vector exceeds dimension limit".into(),
                    ));
                }
                if text_version.trim().is_empty()
                    || !min_score.is_finite()
                    || !(-1.0..=1.0).contains(min_score)
                {
                    return Err(BackendError::Query(
                        "invalid identity candidate embedding".into(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct IdentityCandidate {
    pub record: EntityVersionRecord,
    pub score: f64,
}

#[derive(Debug, Clone, Default)]
pub struct IdentityCandidatePage {
    pub items: Vec<IdentityCandidate>,
    /// At least one eligible chain was omitted. An empty page is not proof of novelty.
    pub truncated: bool,
}

impl IdentityCandidatePage {
    /// Validate adapter responses again at the stage boundary, including scope and versions.
    pub fn validate(
        &self,
        org: &str,
        request: &IdentityCandidateRequest,
    ) -> Result<(), BackendError> {
        request.validate(org)?;
        let mut seen = std::collections::HashSet::new();
        if self.items.len() > request.limit || (self.truncated && self.items.len() != request.limit)
        {
            return Err(BackendError::Deserialization(
                "invalid identity candidate page size".into(),
            ));
        }
        for item in &self.items {
            let r = &item.record;
            if !item.score.is_finite()
                || !(-1.0..=1.0).contains(&item.score)
                || r.uuid.is_nil()
                || r.chain_id.is_nil()
                || r.version == 0
                || r.name.trim().is_empty()
                || r.stored.get("org_id").and_then(|v| v.as_str()) != Some(org)
                || r.namespace != request.scope.namespace
                || (request.scope.entity_type != "*" && r.entity_type != request.scope.entity_type)
                || !r.is_latest
                || r.valid_to.is_some()
                || r.deleted_at.is_some()
                || r.merged_into.is_some()
                || request.exclude_chains.contains(&r.chain_id)
                || !seen.insert(r.chain_id)
            {
                return Err(BackendError::Deserialization(
                    "invalid identity candidate record".into(),
                ));
            }
            if let IdentityCandidateQuery::PropertyOverlap(properties) = &request.query {
                let stored = r
                    .typed_source_properties()
                    .map_err(BackendError::Deserialization)?;
                let keys: std::collections::HashSet<_> =
                    properties.iter().map(|(key, _)| key).collect();
                let matches: std::collections::HashSet<_> = properties
                    .iter()
                    .filter(|(key, value)| stored.get(key) == Some(value))
                    .map(|(key, _)| key)
                    .collect();
                let expected = matches.len() as f64 / keys.len() as f64;
                if matches.is_empty() || (item.score - expected).abs() > 1e-9 {
                    return Err(BackendError::Deserialization(
                        "invalid property agreement score".into(),
                    ));
                }
            }
            if let IdentityCandidateQuery::Similarity {
                embedding,
                text_version,
                min_score,
            } = &request.query
            {
                if item.score < f64::from(*min_score)
                    || r.stored
                        .get(crate::embedding::TEXT_VERSION_PROPERTY)
                        .and_then(|v| v.as_str())
                        != Some(text_version.as_str())
                    || r.embedding.as_ref().is_none_or(|v| {
                        v.model != embedding.model || v.values.len() != embedding.values.len()
                    })
                {
                    return Err(BackendError::Deserialization(
                        "incompatible identity candidate embedding".into(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request(properties: Vec<(String, PropertyValue)>) -> IdentityCandidateRequest {
        IdentityCandidateRequest {
            scope: IdentityScope {
                namespace: "prod".into(),
                entity_type: "Service".into(),
            },
            query: IdentityCandidateQuery::PropertyOverlap(properties),
            exclude_chains: vec![],
            limit: 15,
        }
    }
    #[test]
    fn property_evidence_is_bounded_typed_and_not_duplicated() {
        assert!(request(vec![
            ("id".into(), PropertyValue::Integer(42)),
            ("id".into(), PropertyValue::String("42".into()))
        ])
        .validate("org")
        .is_ok());
        for properties in [
            vec![],
            vec![("x".into(), PropertyValue::Null)],
            vec![("x".into(), PropertyValue::Float(f64::NAN))],
            vec![("x".into(), PropertyValue::String(" ".into()))],
            vec![("x".into(), PropertyValue::Bool(true)); 2],
            (0..129)
                .map(|n| (n.to_string(), PropertyValue::Integer(n)))
                .collect(),
            (0..9)
                .map(|n| (n.to_string(), PropertyValue::String("x".repeat(4096))))
                .collect(),
        ] {
            assert!(request(properties).validate("org").is_err());
        }
    }
}
