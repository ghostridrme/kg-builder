//! Pipeline processing choices resolved per snapshot source.
//!
//! The caller supplies engine defaults, organization-level overrides, and optional
//! per-source overrides. Stages read the resulting policy through `for_source`.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// How entities are extracted from snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ExtractionMode {
    /// Structured `entities[]` only; raw-`content` snapshots are skipped
    /// (logged) instead of LLM-extracted.
    Heuristic,
    /// Same as [`ExtractionMode::Auto`] — kept for explicitness in config.
    Llm,
    /// Validate structured entities; extract raw content with the LLM.
    #[default]
    Auto,
}

/// How identity schemas (primary keys) are decided for entity types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SchemaMode {
    /// Connectors must declare `primary_key_properties`; entities without
    /// them are rejected.
    #[default]
    Declared,
    /// Infer missing primary keys once per organization, source, and type.
    /// Requires a schema store; subsequent observations reuse the adopted schema.
    InferOnce,
}

/// How an entity that matches no identity hash is matched to existing chains.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EntityMatching {
    /// Identity hashes only; no candidates and no model call.
    #[default]
    Exact,
    /// Name, embedding, and property candidates within the same organization,
    /// namespace, and type, confirmed by the disambiguation model. Similarity alone
    /// never merges, and a conflicting authoritative key disqualifies a
    /// candidate. Requires a configured model.
    Semantic,
}

/// How relationship edges are discovered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EdgeDiscoveryMode {
    /// FK/name matching only — the LLM edge stage is a no-op.
    Heuristic,
    /// Heuristics first, then model discovery of additional evidence-backed facts.
    /// An existing outgoing edge does not establish relationship completeness.
    HeuristicThenLlm,
    /// Both heuristic and LLM discovery stages run.
    #[default]
    Llm,
}

/// What happens when heuristic FK matching is ambiguous (several entities
/// share the referenced name).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EdgeAmbiguityMode {
    /// Skip ambiguous matches.
    #[default]
    Skip,
    /// Ask the LLM to select a candidate or abstain; cap confidence below heuristics.
    Llm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StaleDeletionPolicy {
    #[default]
    Fail,
    Record,
}

/// Extraction, deduplication, and relationship-discovery policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct PipelinePolicy {
    pub declared_relationships: bool,
    pub record_generic_unresolved: bool,
    pub stale_deletions: StaleDeletionPolicy,
    pub extraction: ExtractionMode,
    pub schema: SchemaMode,
    pub matching: EntityMatching,
    pub edge_discovery: EdgeDiscoveryMode,
    pub edge_ambiguity: EdgeAmbiguityMode,
}

impl Default for PipelinePolicy {
    fn default() -> Self {
        Self {
            declared_relationships: true,
            record_generic_unresolved: true,
            stale_deletions: StaleDeletionPolicy::Fail,
            extraction: Default::default(),
            schema: Default::default(),
            matching: Default::default(),
            edge_discovery: Default::default(),
            edge_ambiguity: Default::default(),
        }
    }
}

/// Absent choices inherit from the layer below.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct PolicyOverride {
    pub declared_relationships: Option<bool>,
    pub record_generic_unresolved: Option<bool>,
    pub stale_deletions: Option<StaleDeletionPolicy>,
    pub extraction: Option<ExtractionMode>,
    pub schema: Option<SchemaMode>,
    pub matching: Option<EntityMatching>,
    pub edge_discovery: Option<EdgeDiscoveryMode>,
    pub edge_ambiguity: Option<EdgeAmbiguityMode>,
}

impl PolicyOverride {
    /// Apply this override on top of `base`.
    pub fn apply(&self, base: PipelinePolicy) -> PipelinePolicy {
        PipelinePolicy {
            declared_relationships: self
                .declared_relationships
                .unwrap_or(base.declared_relationships),
            record_generic_unresolved: self
                .record_generic_unresolved
                .unwrap_or(base.record_generic_unresolved),
            stale_deletions: self.stale_deletions.unwrap_or(base.stale_deletions),
            extraction: self.extraction.unwrap_or(base.extraction),
            schema: self.schema.unwrap_or(base.schema),
            matching: self.matching.unwrap_or(base.matching),
            edge_discovery: self.edge_discovery.unwrap_or(base.edge_discovery),
            edge_ambiguity: self.edge_ambiguity.unwrap_or(base.edge_ambiguity),
        }
    }

    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

/// Organization-level defaults and per-source overrides supplied by the caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct TenantPipelinePolicy {
    /// Organization-wide override of the engine default.
    pub default: PolicyOverride,
    /// Per-source overrides (keyed by `SnapshotInput.source`), applied on
    /// top of the organization default.
    pub per_source: HashMap<String, PolicyOverride>,
}

/// Resolves engine and organization policy for each snapshot source.
#[derive(Debug, Clone, Default)]
pub struct PolicyResolver {
    engine_default: PipelinePolicy,
    tenant: TenantPipelinePolicy,
}

impl PolicyResolver {
    pub fn new(engine_default: PipelinePolicy) -> Self {
        Self {
            engine_default,
            tenant: TenantPipelinePolicy::default(),
        }
    }

    pub fn with_tenant(engine_default: PipelinePolicy, tenant: TenantPipelinePolicy) -> Self {
        Self {
            engine_default,
            tenant,
        }
    }

    /// Engine default before organization overrides.
    pub fn engine_default(&self) -> PipelinePolicy {
        self.engine_default
    }

    /// Organization-level overrides supplied by the caller.
    pub fn tenant(&self) -> &TenantPipelinePolicy {
        &self.tenant
    }

    /// The effective policy for one snapshot source:
    /// `per_source[source]` → organization default → engine default.
    pub fn for_source(&self, source: &str) -> PipelinePolicy {
        let base = self.tenant.default.apply(self.engine_default);
        match self.tenant.per_source.get(source) {
            Some(o) => o.apply(base),
            None => base,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_uses_exact_matching_and_skips_ambiguous_edges() {
        let p = PipelinePolicy::default();
        assert_eq!(p.extraction, ExtractionMode::Auto);
        assert_eq!(p.schema, SchemaMode::Declared);
        assert_eq!(p.matching, EntityMatching::Exact);
        assert_eq!(p.edge_discovery, EdgeDiscoveryMode::Llm);
        assert_eq!(p.edge_ambiguity, EdgeAmbiguityMode::Skip);
    }

    #[test]
    fn per_source_resolution_github_vs_aws() {
        let tenant = TenantPipelinePolicy {
            default: PolicyOverride {
                edge_discovery: Some(EdgeDiscoveryMode::Heuristic),
                ..Default::default()
            },
            per_source: HashMap::from([
                (
                    "github".to_string(),
                    PolicyOverride {
                        extraction: Some(ExtractionMode::Llm),
                        edge_discovery: Some(EdgeDiscoveryMode::Llm),
                        ..Default::default()
                    },
                ),
                (
                    "aws".to_string(),
                    PolicyOverride {
                        schema: Some(SchemaMode::InferOnce),
                        ..Default::default()
                    },
                ),
            ]),
        };
        let r = PolicyResolver::with_tenant(PipelinePolicy::default(), tenant);

        let github = r.for_source("github");
        assert_eq!(github.edge_discovery, EdgeDiscoveryMode::Llm);
        assert_eq!(github.extraction, ExtractionMode::Llm);
        assert_eq!(github.schema, SchemaMode::Declared, "inherits tenant base");

        let aws = r.for_source("aws");
        assert_eq!(
            aws.edge_discovery,
            EdgeDiscoveryMode::Heuristic,
            "tenant default"
        );
        assert_eq!(aws.schema, SchemaMode::InferOnce);

        let other = r.for_source("k8s");
        assert_eq!(other.edge_discovery, EdgeDiscoveryMode::Heuristic);
        assert_eq!(other.schema, SchemaMode::Declared);
    }

    #[test]
    fn unknown_knob_value_is_a_parse_error() {
        let bad = r#"{ "extraction": "magic" }"#;
        assert!(serde_json::from_str::<PipelinePolicy>(bad).is_err());
        let unknown_field = r#"{ "extractoin": "auto" }"#;
        assert!(serde_json::from_str::<PipelinePolicy>(unknown_field).is_err());
        // The removed dedup knobs are rejected, never aliased to a behavior.
        for removed in [
            r#"{ "entity_dedup": "heuristic_then_llm" }"#,
            r#"{ "dedup_candidates": "lsh" }"#,
            r#"{ "matching": "lsh" }"#,
            r#"{ "matching": "both" }"#,
        ] {
            assert!(
                serde_json::from_str::<PipelinePolicy>(removed).is_err(),
                "{removed}"
            );
            assert!(
                serde_json::from_str::<PolicyOverride>(removed).is_err(),
                "{removed}"
            );
        }
        let parsed: PipelinePolicy = serde_json::from_str(r#"{ "matching": "semantic" }"#).unwrap();
        assert_eq!(parsed.matching, EntityMatching::Semantic);
    }
}
