//! Bounded text discovery settings, included in the run fingerprint.
use crate::errors::BackendError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Trusted additions for one source; exclusions always extend the global set.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SourceExtractionGuidance {
    pub relationship_contradictions: Option<RelationshipContradictionSettings>,
    pub relationship_timestamps: Option<RelationshipTimestampSettings>,
    /// Domain context shared by entity and relationship discovery only.
    pub shared_instructions: Option<String>,
    pub instructions: Option<String>,
    /// Prose for the model relationship producer only (never a mapping, never
    /// edges); appended to the global relationship instructions.
    pub relationship_instructions: Option<String>,
    /// Prose for the model identity decision (what does and does not make two
    /// observations the same entity in this domain); appended to the global
    /// identity instructions. Never names entities or forces a match.
    pub identity_instructions: Option<String>,
    /// Deterministic reference mappings for this producer.
    pub reference_guidance: Vec<ReferenceMapping>,
    pub excluded_entity_types: Vec<String>,
    /// Extra generic kind words for this source (see
    /// [`ExtractionSettings::entity_kind_words`]); always extend the global set.
    pub entity_kind_words: Vec<String>,
}

/// Caller guidance mapping one reference path of a source entity type to one
/// complete key group of a target type. Guidance never carries edge instances
/// and never bypasses identity, type, scope, freshness or completeness checks;
/// conflicting positive mappings are rejected at validation, not resolved by
/// last-write-wins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceMapping {
    /// Source entity type this mapping applies to.
    pub source_entity_type: String,
    /// Optional observing namespace. Authored mappings omit it to apply to all
    /// namespaces; learned mappings are always scoped to their evidence.
    #[serde(default)]
    pub source_namespace: Option<String>,
    /// Path to the reference value(s); see [`ReferencePath::parse`].
    pub reference_path: String,
    /// Extra target key components supplied by other paths of the same source
    /// object (`component -> path`), completing a composite group.
    #[serde(default)]
    pub context_paths: BTreeMap<String, String>,
    pub target_type: String,
    /// The complete primary or alternative group of `target_type` this satisfies.
    pub target_key_group: Vec<String>,
    #[serde(default)]
    pub shape: ReferenceShape,
    #[serde(default)]
    pub direction: ReferenceDirection,
    /// Semantic name kept in the existing relationship name/property representation.
    pub relationship_name: String,
    #[serde(default)]
    pub qualifiers: Option<String>,
    #[serde(default)]
    pub cardinality: ReferenceCardinality,
    /// Target types whose string key components compare case-insensitively under
    /// this provider's contract (the only way case-folding enters).
    #[serde(default)]
    pub case_insensitive_types: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceShape {
    #[default]
    Scalar,
    List,
    Object,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceDirection {
    #[default]
    SourceToTarget,
    /// The edge points target→source but stays owned by the observing source's slot.
    Inverse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceCardinality {
    #[default]
    One,
    Many,
}

/// One segment of a parsed reference path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathSegment {
    pub key: String,
    /// `Some(None)` binds every element (`seg[]`); `Some(Some(n))` one element.
    pub index: Option<Option<usize>>,
}

/// A parsed reference path. Segments split on unescaped `.`; `\.` and `\\` are
/// literal; `seg[n]` selects one element and `seg[]` every element; a `raw:`
/// prefix binds original properties instead of the flattened form. Malformed
/// paths are rejected before any ingestion write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferencePath {
    pub raw: bool,
    pub segments: Vec<PathSegment>,
}

impl ReferencePath {
    pub fn parse(text: &str) -> Result<Self, String> {
        let (raw, body) = match text.strip_prefix("raw:") {
            Some(rest) => (true, rest),
            None => (false, text),
        };
        if body.is_empty() {
            return Err("empty reference path".into());
        }
        let mut segments = Vec::new();
        let mut current = String::new();
        let mut index: Option<Option<usize>> = None;
        let mut chars = body.chars().peekable();
        fn flush(
            current: &mut String,
            index: &mut Option<Option<usize>>,
            segments: &mut Vec<PathSegment>,
        ) -> Result<(), String> {
            if current.is_empty() {
                return Err("empty path segment".into());
            }
            segments.push(PathSegment {
                key: std::mem::take(current),
                index: index.take(),
            });
            Ok(())
        }
        while let Some(c) = chars.next() {
            match c {
                '\\' => match chars.next() {
                    Some(escaped @ ('.' | '\\' | '[' | ']')) => current.push(escaped),
                    _ => return Err("unknown escape in reference path".into()),
                },
                '.' => flush(&mut current, &mut index, &mut segments)?,
                '[' => {
                    if index.is_some() {
                        return Err("repeated index in reference path".into());
                    }
                    let mut digits = String::new();
                    loop {
                        match chars.next() {
                            Some(']') => break,
                            Some(d) if d.is_ascii_digit() => digits.push(d),
                            _ => return Err("unclosed or non-numeric index".into()),
                        }
                    }
                    index = Some(if digits.is_empty() {
                        None
                    } else {
                        Some(digits.parse().map_err(|_| "index out of range")?)
                    });
                    if let Some(next) = chars.peek() {
                        if *next != '.' {
                            return Err("index must end a segment".into());
                        }
                    }
                }
                ']' => return Err("unexpected ']' in reference path".into()),
                other => current.push(other),
            }
        }
        flush(&mut current, &mut index, &mut segments)?;
        Ok(Self { raw, segments })
    }
}

impl ReferenceMapping {
    /// Structural validity, reused by learned-rule proposal so a proposed
    /// mapping is held to the same shape as an authored one.
    pub fn validate(&self) -> Result<(), String> {
        let name_ok = |s: &str, max: usize| !s.trim().is_empty() && s.len() <= max;
        if !name_ok(&self.source_entity_type, 256)
            || !name_ok(&self.target_type, 256)
            || !name_ok(&self.relationship_name, 128)
            || self.target_key_group.is_empty()
            || self.target_key_group.len() > 16
            || self.target_key_group.iter().any(|k| !name_ok(k, 256))
            || self.qualifiers.is_some()
            || self.case_insensitive_types.len() > 64
            || self.case_insensitive_types.iter().any(|t| !name_ok(t, 256))
            || self.context_paths.len() > 16
        {
            return Err("invalid or unsupported reference mapping".into());
        }
        let key_set: std::collections::BTreeSet<_> = self.target_key_group.iter().collect();
        let fold_set: std::collections::BTreeSet<_> = self.case_insensitive_types.iter().collect();
        if key_set.len() != self.target_key_group.len()
            || fold_set.len() != self.case_insensitive_types.len()
            || self
                .case_insensitive_types
                .iter()
                .any(|target| target != &self.target_type)
        {
            return Err("invalid or unsupported reference mapping".into());
        }
        ReferencePath::parse(&self.reference_path)?;
        for (component, path) in &self.context_paths {
            if !self.target_key_group.contains(component) {
                return Err("context path names a component outside the target key group".into());
            }
            ReferencePath::parse(path)?;
        }
        Ok(())
    }

    /// Two positive mappings for the same source path that disagree are a conflict.
    pub fn conflicts_with(&self, other: &Self) -> bool {
        let same_path = ReferencePath::parse(&self.reference_path).ok()
            == ReferencePath::parse(&other.reference_path).ok();
        let same_context = self.context_paths.len() == other.context_paths.len()
            && self.context_paths.iter().all(|(component, path)| {
                other
                    .context_paths
                    .get(component)
                    .is_some_and(|other_path| {
                        ReferencePath::parse(path).ok() == ReferencePath::parse(other_path).ok()
                    })
            });
        let overlapping_namespace = self.source_namespace.is_none()
            || other.source_namespace.is_none()
            || self.source_namespace == other.source_namespace;
        self.source_entity_type == other.source_entity_type
            && overlapping_namespace
            && same_path
            && (self.target_type != other.target_type
                || as_set(&self.target_key_group) != as_set(&other.target_key_group)
                || !same_context
                || self.shape != other.shape
                || self.direction != other.direction
                || self.relationship_name != other.relationship_name
                || self.qualifiers != other.qualifiers
                || self.cardinality != other.cardinality
                || as_set(&self.case_insensitive_types) != as_set(&other.case_insensitive_types))
    }

    fn semantically_eq(&self, other: &Self) -> bool {
        let same_context = self.context_paths.len() == other.context_paths.len()
            && self.context_paths.iter().all(|(component, path)| {
                other
                    .context_paths
                    .get(component)
                    .is_some_and(|other_path| {
                        ReferencePath::parse(path).ok() == ReferencePath::parse(other_path).ok()
                    })
            });
        !self.conflicts_with(other)
            && self.source_entity_type == other.source_entity_type
            && self.source_namespace == other.source_namespace
            && ReferencePath::parse(&self.reference_path).ok()
                == ReferencePath::parse(&other.reference_path).ok()
            && self.target_type == other.target_type
            && as_set(&self.target_key_group) == as_set(&other.target_key_group)
            && same_context
            && self.shape == other.shape
            && self.direction == other.direction
            && self.relationship_name == other.relationship_name
            && self.qualifiers == other.qualifiers
            && self.cardinality == other.cardinality
            && as_set(&self.case_insensitive_types) == as_set(&other.case_insensitive_types)
    }
}

fn as_set(values: &[String]) -> std::collections::BTreeSet<&str> {
    values.iter().map(String::as_str).collect()
}

/// Semantic assessment is opt-in and bounded for each producer observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RelationshipContradictionSettings {
    pub enabled: bool,
    pub batch_size: usize,
    /// Maximum calls for one producer within one snapshot.
    pub max_batches: usize,
    /// Maximum eligible comparisons for one relationship observation.
    pub max_candidates: usize,
}
impl Default for RelationshipContradictionSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            batch_size: 16,
            max_batches: 16,
            max_candidates: 1024,
        }
    }
}

/// Additional timestamp inference is opt-in; explicit dates never require a model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RelationshipTimestampSettings {
    pub enabled: bool,
    pub batch_size: usize,
    /// Maximum model calls, including one possible correction per batch, for one producer observation.
    pub max_batches: usize,
}
impl Default for RelationshipTimestampSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            batch_size: 16,
            max_batches: 16,
        }
    }
}

/// Opt-in semantic naming of already-discovered generic relationships. Default
/// OFF: the naming stage then makes zero model calls and returns its input
/// unchanged, so every existing path is preserved. When enabled, a configured
/// relationship-discovery model labels only generic (`RELATES_TO`) confirmed
/// reference edges; the physical Neo4j type and the edge's trusted identity
/// never change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RelationshipNamingSettings {
    /// Master switch. `false` is the default and makes zero model calls.
    pub enabled: bool,
    /// Maximum relationships put to the model in one request; larger eligible
    /// sets are split into several bounded requests.
    pub max_batch: usize,
    /// Upper bound on one request's full character size (instruction, schema,
    /// deduplicated entity records, evidence and an output allowance), further
    /// limited by the model's context window. A single
    /// relationship whose complete evidence cannot fit stays generic with a
    /// diagnostic rather than having its properties stripped to fit.
    pub max_request_chars: usize,
    /// Characters of `max_request_chars` reserved for the model's reply, so a
    /// full request still leaves room for every per-edge name.
    pub output_reserve_chars: usize,
    pub timeout_ms: u64,
    pub max_output_tokens: u32,
    pub max_response_bytes: usize,
}

impl Default for RelationshipNamingSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            max_batch: 32,
            max_request_chars: 131_072,
            output_reserve_chars: 16_384,
            timeout_ms: 60_000,
            max_output_tokens: 4_096,
            max_response_bytes: 262_144,
        }
    }
}

impl RelationshipNamingSettings {
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=1000).contains(&self.max_batch)
            || !(1_024..=4 * 1024 * 1024).contains(&self.max_request_chars)
            || self.output_reserve_chars == 0
            || self.output_reserve_chars >= self.max_request_chars
            || self.timeout_ms == 0
            || self.timeout_ms > 300_000
            || self.max_output_tokens == 0
            || self.max_output_tokens > 32_000
            || self.max_response_bytes == 0
            || self.max_response_bytes > 4 * 1024 * 1024
        {
            return Err("invalid relationship naming settings".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ExtractionSettings {
    pub relationship_contradictions: RelationshipContradictionSettings,
    pub relationship_timestamps: RelationshipTimestampSettings,
    /// Opt-in semantic naming of generic relationships; off by default.
    pub relationship_naming: RelationshipNamingSettings,
    pub omission_check: bool,
    pub source_guidance: BTreeMap<String, SourceExtractionGuidance>,
    /// Domain context shared by entity and relationship discovery only.
    pub shared_instructions: Option<String>,
    pub instructions: Option<String>,
    /// Prose for the model relationship producer, separate from entity
    /// instructions and from deterministic mappings; schema and isolation win
    /// over its wording. Fingerprinted with every other setting.
    pub relationship_instructions: Option<String>,
    /// Domain prose for the model identity decision (for example which naming
    /// conventions distinguish or unify resources). The decision rules, key
    /// authority and evidence requirements win over its wording. Fingerprinted
    /// with every other setting.
    pub identity_instructions: Option<String>,
    /// Domain prose for the model summary producers (entity, Saga and Community
    /// briefs): what to keep, what wording the domain uses. Summaries span
    /// sources, so this is global only. Fingerprinted with every other setting.
    pub summary_instructions: Option<String>,
    /// Deterministic reference mappings by producer source (`for_source` keeps
    /// only the applicable entry). Never edge instances.
    pub reference_guidance: BTreeMap<String, Vec<ReferenceMapping>>,
    /// Per-entity generic scan bounds. A bound hit is reported as incomplete.
    pub reference_max_values: usize,
    pub reference_max_depth: usize,
    pub excluded_entity_types: Vec<String>,
    /// Generic words naming what KIND of thing an entity is in the caller's
    /// domain (for a cloud estate: database, cluster, bucket, queue; for a
    /// catalogue: album, edition). When a model keeps one as the trailing word
    /// of a distinctive identifier ("orders-store database"), the extractor drops
    /// it deterministically: the kind is the entity's type, not its name. Only an
    /// identifier-shaped head is cut, so a plain product name ("App Store") never
    /// is. Empty by default: the engine carries no domain vocabulary; a
    /// connector or caller supplies it. Compared case-insensitively.
    pub entity_kind_words: Vec<String>,
    pub timeout_ms: u64,
    pub max_output_tokens: u32,
    pub max_response_bytes: usize,
    pub max_entities: usize,
    pub max_property_depth: usize,
}
impl Default for ExtractionSettings {
    fn default() -> Self {
        Self {
            relationship_contradictions: RelationshipContradictionSettings::default(),
            relationship_timestamps: RelationshipTimestampSettings::default(),
            relationship_naming: RelationshipNamingSettings::default(),
            omission_check: true,
            source_guidance: BTreeMap::new(),
            shared_instructions: None,
            instructions: None,
            relationship_instructions: None,
            identity_instructions: None,
            summary_instructions: None,
            reference_guidance: BTreeMap::new(),
            reference_max_values: 128,
            reference_max_depth: 5,
            excluded_entity_types: vec![],
            entity_kind_words: vec![],
            timeout_ms: 60_000,
            max_output_tokens: 16_000,
            max_response_bytes: 1_048_576,
            max_entities: 256,
            max_property_depth: 16,
        }
    }
}
/// A kind word is one lowercase-comparable token: nonblank, no whitespace, at
/// most 64 bytes, at most 128 of them.
fn kind_words_valid(words: &[String]) -> bool {
    words.len() <= 128
        && words.iter().all(|word| {
            !word.is_empty() && word.len() <= 64 && !word.chars().any(char::is_whitespace)
        })
}

impl ExtractionSettings {
    /// Freeze applicable source guidance while retaining global restrictions.
    pub fn for_source(&self, source: &str) -> Self {
        let mut effective = Self {
            relationship_contradictions: self.relationship_contradictions.clone(),
            relationship_timestamps: self.relationship_timestamps.clone(),
            relationship_naming: self.relationship_naming.clone(),
            omission_check: self.omission_check,
            source_guidance: BTreeMap::new(),
            shared_instructions: self.shared_instructions.clone(),
            instructions: self.instructions.clone(),
            relationship_instructions: self.relationship_instructions.clone(),
            identity_instructions: self.identity_instructions.clone(),
            summary_instructions: self.summary_instructions.clone(),
            entity_kind_words: self.entity_kind_words.clone(),
            reference_guidance: self
                .reference_guidance
                .get(source)
                .map(|mappings| BTreeMap::from([(source.to_owned(), mappings.clone())]))
                .unwrap_or_default(),
            reference_max_values: self.reference_max_values,
            reference_max_depth: self.reference_max_depth,
            excluded_entity_types: self.excluded_entity_types.clone(),
            timeout_ms: self.timeout_ms,
            max_output_tokens: self.max_output_tokens,
            max_response_bytes: self.max_response_bytes,
            max_entities: self.max_entities,
            max_property_depth: self.max_property_depth,
        };
        if let Some(extra) = self.source_guidance.get(source) {
            if let Some(contradictions) = &extra.relationship_contradictions {
                effective.relationship_contradictions = contradictions.clone();
            }
            if let Some(timestamps) = &extra.relationship_timestamps {
                effective.relationship_timestamps = timestamps.clone();
            }
            if let Some(instructions) = &extra.shared_instructions {
                effective.shared_instructions = Some(match &self.shared_instructions {
                    Some(global) => format!("{global}\n{instructions}"),
                    None => instructions.clone(),
                });
            }
            if let Some(instructions) = &extra.instructions {
                effective.instructions = Some(match &self.instructions {
                    Some(global) => format!("{global}\n{instructions}"),
                    None => instructions.clone(),
                });
            }
            if let Some(instructions) = &extra.relationship_instructions {
                effective.relationship_instructions = Some(match &self.relationship_instructions {
                    Some(global) => format!("{global}\n{instructions}"),
                    None => instructions.clone(),
                });
            }
            if let Some(instructions) = &extra.identity_instructions {
                effective.identity_instructions = Some(match &self.identity_instructions {
                    Some(global) => format!("{global}\n{instructions}"),
                    None => instructions.clone(),
                });
            }
            if !extra.reference_guidance.is_empty() {
                effective
                    .reference_guidance
                    .entry(source.to_owned())
                    .or_default()
                    .extend(extra.reference_guidance.iter().cloned());
            }
            for name in &extra.excluded_entity_types {
                if !effective.excluded_entity_types.contains(name) {
                    effective.excluded_entity_types.push(name.clone());
                }
            }
            for word in &extra.entity_kind_words {
                if !effective
                    .entity_kind_words
                    .iter()
                    .any(|have| have.eq_ignore_ascii_case(word))
                {
                    effective.entity_kind_words.push(word.clone());
                }
            }
        }
        effective
    }

    pub fn validate(&self) -> Result<(), BackendError> {
        if !(1..=64).contains(&self.relationship_contradictions.batch_size)
            || !(1..=64).contains(&self.relationship_contradictions.max_batches)
            || !(1..=4096).contains(&self.relationship_contradictions.max_candidates)
            || !(1..=64).contains(&self.relationship_timestamps.batch_size)
            || !(1..=64).contains(&self.relationship_timestamps.max_batches)
            || !(1..=4096).contains(&self.reference_max_values)
            || !(1..=32).contains(&self.reference_max_depth)
            || self.timeout_ms == 0
            || self.timeout_ms > 300_000
            || self.max_output_tokens == 0
            || self.max_output_tokens > 32_000
            || self.max_response_bytes == 0
            || self.max_response_bytes > 4 * 1024 * 1024
            || self.max_entities == 0
            || self.max_entities > 1024
            || self.max_property_depth == 0
            || self.max_property_depth > 32
            || self
                .shared_instructions
                .as_ref()
                .is_some_and(|v| v.trim().is_empty() || v.len() > 16_384)
            || self
                .instructions
                .as_ref()
                .is_some_and(|v| v.trim().is_empty() || v.len() > 16_384)
            || self
                .relationship_instructions
                .as_ref()
                .is_some_and(|v| v.trim().is_empty() || v.len() > 16_384)
            || self
                .identity_instructions
                .as_ref()
                .is_some_and(|v| v.trim().is_empty() || v.len() > 16_384)
            || self
                .summary_instructions
                .as_ref()
                .is_some_and(|v| v.trim().is_empty() || v.len() > 16_384)
            || !kind_words_valid(&self.entity_kind_words)
            || self.excluded_entity_types.len() > 128
            || self
                .excluded_entity_types
                .iter()
                .any(|v| v.trim().is_empty() || v.len() > 256)
        {
            return Err(BackendError::Query("invalid extraction settings".into()));
        }
        self.relationship_naming
            .validate()
            .map_err(BackendError::Query)?;
        if self.source_guidance.len() > 128
            || self.source_guidance.iter().any(|(source, g)| {
                source.trim().is_empty()
                    || source.len() > 256
                    || g.shared_instructions
                        .as_ref()
                        .is_some_and(|s| s.trim().is_empty() || s.len() > 16_384)
                    || g.instructions
                        .as_ref()
                        .is_some_and(|s| s.trim().is_empty() || s.len() > 16_384)
                    || g.relationship_instructions
                        .as_ref()
                        .is_some_and(|s| s.trim().is_empty() || s.len() > 16_384)
                    || g.identity_instructions
                        .as_ref()
                        .is_some_and(|s| s.trim().is_empty() || s.len() > 16_384)
                    || !kind_words_valid(&g.entity_kind_words)
                    || g.reference_guidance.len() > 128
                    || g.excluded_entity_types.len() > 128
                    || g.excluded_entity_types
                        .iter()
                        .any(|s| s.trim().is_empty() || s.len() > 256)
                    || self.for_source(source).validate().is_err()
            })
        {
            return Err(BackendError::Query(
                "invalid source extraction guidance".into(),
            ));
        }
        for (source, mappings) in self.reference_guidance.iter().chain(
            self.source_guidance
                .iter()
                .map(|(source, g)| (source, &g.reference_guidance)),
        ) {
            if source.trim().is_empty() || source.len() > 256 || mappings.len() > 128 {
                return Err(BackendError::Query("invalid reference guidance".into()));
            }
            for (i, mapping) in mappings.iter().enumerate() {
                mapping
                    .validate()
                    .map_err(|e| BackendError::Query(format!("invalid reference guidance: {e}")))?;
                if mappings[..i]
                    .iter()
                    .any(|other| other.semantically_eq(mapping))
                {
                    return Err(BackendError::Query("duplicate reference guidance".into()));
                }
                if mappings[..i]
                    .iter()
                    .any(|other| other.conflicts_with(mapping))
                {
                    return Err(BackendError::Query(
                        "conflicting reference guidance for one source path".into(),
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

    fn mapping(path: &str, target: &str) -> ReferenceMapping {
        ReferenceMapping {
            source_namespace: None,
            source_entity_type: "GcpInstance".into(),
            reference_path: path.into(),
            context_paths: BTreeMap::new(),
            target_type: target.into(),
            target_key_group: vec!["self_link".into()],
            shape: ReferenceShape::List,
            direction: ReferenceDirection::SourceToTarget,
            relationship_name: "USES_SUBNETWORK".into(),
            qualifiers: None,
            cardinality: ReferenceCardinality::Many,
            case_insensitive_types: vec![],
        }
    }

    #[test]
    fn shared_guidance_is_bounded_scoped_and_serialized() {
        let mut settings = ExtractionSettings {
            shared_instructions: Some("GLOBAL".into()),
            ..Default::default()
        };
        settings.source_guidance.insert(
            "logs".into(),
            SourceExtractionGuidance {
                shared_instructions: Some("SOURCE".into()),
                ..Default::default()
            },
        );
        settings.validate().unwrap();
        let effective = settings.for_source("logs");
        assert_eq!(
            effective.shared_instructions.as_deref(),
            Some("GLOBAL\nSOURCE")
        );
        assert_eq!(effective.for_source("logs"), effective);
        assert_eq!(
            settings.for_source("code").shared_instructions.as_deref(),
            Some("GLOBAL")
        );
        assert_eq!(
            serde_json::from_value::<ExtractionSettings>(serde_json::to_value(&settings).unwrap())
                .unwrap(),
            settings
        );
        settings.shared_instructions = Some("x".repeat(16_384));
        assert!(
            settings.validate().is_err(),
            "combined source guidance must fit the limit"
        );
        settings.shared_instructions = Some(" ".into());
        assert!(settings.validate().is_err());
        settings.shared_instructions = None;
        settings
            .source_guidance
            .get_mut("logs")
            .unwrap()
            .shared_instructions = Some(" ".into());
        assert!(settings.validate().is_err());
    }

    #[test]
    fn reference_paths_follow_the_frozen_grammar() {
        let parsed = ReferencePath::parse("network_interfaces[0].subnetwork").unwrap();
        assert!(!parsed.raw);
        assert_eq!(parsed.segments[0].key, "network_interfaces");
        assert_eq!(parsed.segments[0].index, Some(Some(0)));
        assert_eq!(parsed.segments[1].key, "subnetwork");
        let literal = ReferencePath::parse(r"raw:a\.b.items[]").unwrap();
        assert!(literal.raw);
        assert_eq!(literal.segments[0].key, "a.b");
        assert_eq!(literal.segments[1].index, Some(None));
        for bad in [
            "", "a..b", ".a", "a.", r"a\x", "a[", "a[x]", "a]", "a[1]b", "a[1][2]",
        ] {
            assert!(
                ReferencePath::parse(bad).is_err(),
                "{bad:?} must be rejected"
            );
        }
    }

    #[test]
    fn reference_guidance_is_validated_scoped_conflict_checked_and_fingerprinted() {
        let mut settings = ExtractionSettings::default();
        let before = serde_json::to_value(&settings).unwrap();
        settings.reference_guidance.insert(
            "gcp".into(),
            vec![mapping("network_interfaces[].subnetwork", "GcpSubnetwork")],
        );
        settings.relationship_instructions = Some("Name relations as verbs.".into());
        settings.validate().unwrap();
        assert_ne!(
            before,
            serde_json::to_value(&settings).unwrap(),
            "guidance must change the fingerprint"
        );
        assert_eq!(
            settings.for_source("gcp").reference_guidance["gcp"].len(),
            1
        );
        assert!(
            settings.for_source("aws").reference_guidance.is_empty(),
            "guidance is producer scoped"
        );
        settings
            .reference_guidance
            .get_mut("gcp")
            .unwrap()
            .push(mapping("network_interfaces[].subnetwork", "GcpNetwork"));
        assert!(
            settings.validate().is_err(),
            "conflicting positives are rejected"
        );
        settings.reference_guidance.get_mut("gcp").unwrap().pop();
        settings.reference_guidance.get_mut("gcp").unwrap()[0].reference_path = "a..b".into();
        assert!(
            settings.validate().is_err(),
            "malformed paths are rejected before writes"
        );
        settings.reference_guidance.get_mut("gcp").unwrap()[0].reference_path = "x".into();
        settings.reference_guidance.get_mut("gcp").unwrap()[0]
            .context_paths
            .insert("region".into(), "raw:region".into());
        assert!(
            settings.validate().is_err(),
            "context component must belong to the target key group"
        );
        settings.reference_guidance.get_mut("gcp").unwrap()[0]
            .context_paths
            .clear();
        settings.validate().unwrap();
        settings.source_guidance.insert(
            "gcp".into(),
            SourceExtractionGuidance {
                relationship_instructions: Some("Prefer ARM ids.".into()),
                ..Default::default()
            },
        );
        settings.validate().unwrap();
        assert_eq!(
            settings
                .for_source("gcp")
                .relationship_instructions
                .as_deref(),
            Some("Name relations as verbs.\nPrefer ARM ids.")
        );
        settings.relationship_instructions = Some(" ".into());
        assert!(settings.validate().is_err());
        settings.relationship_instructions = None;
        settings.reference_guidance.get_mut("gcp").unwrap()[0].qualifiers =
            Some("environment=prod".into());
        assert!(
            settings.validate().is_err(),
            "qualifiers are rejected until their grammar is enforced"
        );
        settings.reference_guidance.get_mut("gcp").unwrap()[0].qualifiers = None;
        // Identity guidance merges the same way and is bounded the same way.
        settings.identity_instructions = Some("Pod names carry a random suffix.".into());
        settings
            .source_guidance
            .get_mut("gcp")
            .unwrap()
            .identity_instructions = Some("Project id is identity.".into());
        settings.validate().unwrap();
        assert_eq!(
            settings.for_source("gcp").identity_instructions.as_deref(),
            Some("Pod names carry a random suffix.\nProject id is identity.")
        );
        assert_eq!(
            settings.for_source("aws").identity_instructions.as_deref(),
            Some("Pod names carry a random suffix.")
        );
        settings
            .source_guidance
            .get_mut("gcp")
            .unwrap()
            .identity_instructions = Some("x".repeat(16_385));
        assert!(settings.validate().is_err());
    }
    #[test]
    fn contradiction_assessment_is_opt_in_bounded_and_source_scoped() {
        let mut settings = ExtractionSettings::default();
        assert!(!settings.relationship_contradictions.enabled);
        let before = serde_json::to_value(&settings).unwrap();
        settings.source_guidance.insert(
            "logs".into(),
            SourceExtractionGuidance {
                relationship_contradictions: Some(RelationshipContradictionSettings {
                    enabled: true,
                    batch_size: 4,
                    max_batches: 2,
                    max_candidates: 64,
                }),
                ..Default::default()
            },
        );
        settings.validate().unwrap();
        assert!(
            settings
                .for_source("logs")
                .relationship_contradictions
                .enabled
        );
        assert!(
            !settings
                .for_source("aws")
                .relationship_contradictions
                .enabled
        );
        assert_ne!(before, serde_json::to_value(&settings).unwrap());
        settings
            .source_guidance
            .get_mut("logs")
            .unwrap()
            .relationship_contradictions
            .as_mut()
            .unwrap()
            .max_candidates = 0;
        assert!(settings.validate().is_err());
    }

    #[test]
    fn timestamp_calls_are_opt_in_bounded_and_source_overrides_are_fingerprinted() {
        let mut settings = ExtractionSettings::default();
        assert!(!settings.relationship_timestamps.enabled);
        let before = serde_json::to_value(&settings).unwrap();
        settings.source_guidance.insert(
            "logs".into(),
            SourceExtractionGuidance {
                relationship_timestamps: Some(RelationshipTimestampSettings {
                    enabled: true,
                    batch_size: 4,
                    max_batches: 2,
                }),
                ..Default::default()
            },
        );
        settings.validate().unwrap();
        assert!(settings.for_source("logs").relationship_timestamps.enabled);
        assert!(!settings.for_source("aws").relationship_timestamps.enabled);
        assert_ne!(before, serde_json::to_value(&settings).unwrap());
        settings.relationship_timestamps.batch_size = 0;
        assert!(settings.validate().is_err());
        settings.relationship_timestamps.batch_size = 16;
        settings
            .source_guidance
            .get_mut("logs")
            .unwrap()
            .relationship_timestamps
            .as_mut()
            .unwrap()
            .max_batches = 0;
        assert!(settings.validate().is_err());
    }

    #[test]
    fn source_guidance_adds_to_global_policy_without_leaking_to_other_sources() {
        let mut settings = ExtractionSettings {
            instructions: Some("GLOBAL".into()),
            excluded_entity_types: vec!["Person".into()],
            ..Default::default()
        };
        settings.source_guidance.insert(
            "github".into(),
            SourceExtractionGuidance {
                relationship_contradictions: None,
                relationship_timestamps: None,
                shared_instructions: None,
                instructions: Some("SOURCE".into()),
                excluded_entity_types: vec!["Person".into(), "Comment".into()],
                relationship_instructions: None,
                identity_instructions: None,
                reference_guidance: Vec::new(),
                entity_kind_words: vec!["Repo".into()],
            },
        );
        settings.entity_kind_words = vec!["database".into(), "repo".into()];
        settings.validate().unwrap();
        let effective = settings.for_source("github");
        // Kind words extend the global set once, case-insensitively.
        assert_eq!(effective.entity_kind_words, vec!["database", "repo"]);
        assert_eq!(
            settings.for_source("logs").entity_kind_words,
            vec!["database", "repo"]
        );
        settings.entity_kind_words.push("two words".into());
        assert!(settings.validate().is_err());
        settings.entity_kind_words.pop();
        settings.summary_instructions = Some(" ".into());
        assert!(settings.validate().is_err());
        settings.summary_instructions = None;
        assert_eq!(effective.instructions.as_deref(), Some("GLOBAL\nSOURCE"));
        assert_eq!(effective.excluded_entity_types, vec!["Person", "Comment"]);
        assert!(effective.source_guidance.is_empty());
        assert_eq!(
            settings.for_source("logs").instructions.as_deref(),
            Some("GLOBAL")
        );
        assert_eq!(
            settings.for_source("logs").excluded_entity_types,
            vec!["Person"]
        );
        settings
            .source_guidance
            .get_mut("github")
            .unwrap()
            .instructions = Some("x".repeat(16384));
        assert!(settings.validate().is_err());
        let raw = settings.source_guidance.get_mut("github").unwrap();
        raw.instructions = None;
        raw.excluded_entity_types = vec!["Person".into(); 129];
        assert!(settings.validate().is_err());
    }

    // ---- merged from `mod reference_bound_tests`

    use super::ExtractionSettings;
    #[test]
    fn reference_limits_are_bounded_and_survive_source_projection() {
        let mut settings = ExtractionSettings {
            reference_max_values: 512,
            reference_max_depth: 8,
            ..Default::default()
        };
        settings.validate().unwrap();
        let effective = settings.for_source("github");
        assert_eq!(effective.reference_max_values, 512);
        assert_eq!(effective.reference_max_depth, 8);
        settings.reference_max_values = 0;
        assert!(settings.validate().is_err());
        settings.reference_max_values = 4097;
        assert!(settings.validate().is_err());
        settings.reference_max_values = 128;
        settings.reference_max_depth = 33;
        assert!(settings.validate().is_err());
    }
}
