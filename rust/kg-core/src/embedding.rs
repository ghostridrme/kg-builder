//! Graph embedding text, compatibility settings, and provider-output validation.

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use xxhash_rust::xxh3::xxh3_64;

use crate::errors::{BackendError, ConfigError};
use crate::models::{EntityNode, PropertyValue};
use crate::runtime::RuntimeContext;
use crate::traits::graph_backend::GraphEmbedding;
use crate::traits::{EmbedBackend, EntityVersionRecord};

/// Version of [`representation`]. Bump it when the text changes shape: stored
/// vectors of another version are recomputed and never compared.
pub const TEXT_VERSION: &str = "3";
/// Property lines an automatic representation keeps (after sorting by path).
pub const MAX_PROPERTY_LINES: usize = 48;
/// Tag lines an automatic representation keeps.
pub const MAX_TAG_LINES: usize = 32;
/// Labels the labels line keeps.
pub const MAX_LABELS: usize = 32;
/// Last path segments that name credentials; never embedded under automatic
/// selection. This is not redaction: a source with sensitive scalars under other
/// names lists them in `EntityEmbeddingFields::exclude`.
const SENSITIVE_NAME_PARTS: &[&str] = &[
    "password",
    "passwd",
    "secret",
    "token",
    "credential",
    "api_key",
    "apikey",
    "private_key",
    "privatekey",
    "authorization",
    "cookie",
    "session",
    "passphrase",
    "pwd",
    "access_key",
    "accesskey",
    "client_secret",
    "connection_string",
    "connectionstring",
    "dsn",
    "bearer",
];
/// Upper bound on one embedding text, in characters.
pub const MAX_TEXT_CHARS: usize = 4096;
/// Upper bound on one field's contribution, in characters.
pub const MAX_FIELD_CHARS: usize = 512;
/// Graph properties stored next to `embedding` and `embedding_model`.
pub const TEXT_VERSION_PROPERTY: &str = "embedding_text_version";
pub const CONTENT_HASH_PROPERTY: &str = "embedding_content_hash";

// Descriptive properties always embedded when present (older sources rely on
// them) and read by relevance ranking. Identifiers such as ARNs or resource ids
// are deliberately not here: they are exact-indexed, and a vector should carry
// meaning, not identity. This is not redaction.
/// Ordered descriptive properties shared by embedding and relevance ranking.
pub const DESCRIPTIVE_FIELDS: &[&str] = &[
    "summary",
    "description",
    "repository",
    "path",
    "language",
    "service",
    "resource_type",
];

/// Which properties an entity embeds. Type and name always lead the text.
///
/// Without a per-type list the selection is automatic: every scalar property
/// that is not a declared key value, not identifier-shaped, not credential-named,
/// not volatile by type and not excluded here, sorted by path; `default` names
/// properties that are always included when present (the descriptive fields
/// older sources rely on). A per-type list in `by_entity_type` switches that type
/// to explicit mode: exactly those properties, in that order. Tag-like lists and
/// the entity's labels follow in both modes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EntityEmbeddingFields {
    pub default: Vec<String>,
    pub by_entity_type: BTreeMap<String, Vec<String>>,
    /// Property paths never embedded under automatic selection.
    pub exclude: Vec<String>,
}
impl Default for EntityEmbeddingFields {
    fn default() -> Self {
        Self {
            default: DESCRIPTIVE_FIELDS.iter().map(|s| (*s).into()).collect(),
            by_entity_type: BTreeMap::new(),
            exclude: Vec::new(),
        }
    }
}
impl EntityEmbeddingFields {
    pub fn validate(&self) -> Result<(), BackendError> {
        let valid = |fields: &[String]| {
            let mut seen = HashSet::new();
            fields.len() <= 64
                && fields.iter().all(|field| {
                    field.len() <= 256
                        && !field.trim().is_empty()
                        && field.trim() == field
                        && !field.chars().any(char::is_control)
                        && field.split('.').all(|part| !part.is_empty())
                        && seen.insert(field)
                        && !matches!(field.as_str(), "type" | "name" | "entity_type")
                })
        };
        if !valid(&self.default)
            || !valid(&self.exclude)
            || self.by_entity_type.len() > 256
            || self.by_entity_type.iter().any(|(name, fields)| {
                name.trim().is_empty()
                    || name.trim() != name
                    || name.len() > 256
                    || name.chars().any(char::is_control)
                    || !valid(fields)
            })
        {
            return Err(BackendError::Query(
                "invalid entity embedding field selection".into(),
            ));
        }
        Ok(())
    }
    pub fn fields_for(&self, entity_type: &str) -> &[String] {
        self.by_entity_type
            .get(entity_type)
            .unwrap_or(&self.default)
    }
    pub fn text_version(&self) -> String {
        if self == &Self::default() {
            return TEXT_VERSION.into();
        }
        let mut hash = Sha256::new();
        let mut add = |value: &str| {
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(value.as_bytes());
        };
        add("entity-fields.v2");
        add(&self.default.len().to_string());
        for field in &self.default {
            add(field);
        }
        add(&self.exclude.len().to_string());
        for field in &self.exclude {
            add(field);
        }
        for (name, fields) in &self.by_entity_type {
            add(name);
            add(&fields.len().to_string());
            for field in fields {
                add(field);
            }
        }
        format!("{TEXT_VERSION}:{:x}", hash.finalize())
    }
}

/// Check syntax at storage boundaries; the writer validates its complete field policy.
pub fn validate_entity_text_version(version: &str) -> Result<(), BackendError> {
    if version == TEXT_VERSION
        || version
            .strip_prefix(&format!("{TEXT_VERSION}:"))
            .is_some_and(|hash| {
                hash.len() == 64
                    && hash
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            })
    {
        Ok(())
    } else {
        Err(BackendError::Query(
            "invalid entity embedding text version".into(),
        ))
    }
}

/// Relationship descriptions use a separate representation version from entities.
pub const RELATIONSHIP_TEXT_VERSION: &str = "1";

/// Embed the relation name and grounded fact, excluding volatile provenance metadata.
pub fn relationship_representation(name: &str, description: &str) -> String {
    let mut text = String::from("relationship: ");
    text.extend(name.chars().take(MAX_FIELD_CHARS));
    text.push_str("\nfact: ");
    let remaining = MAX_TEXT_CHARS.saturating_sub(text.chars().count());
    text.extend(description.chars().take(remaining));
    text
}

/// The run's shared embedding settings; part of the request fingerprint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EmbeddingSettings {
    pub entity_fields: EntityEmbeddingFields,
    pub model: String,
    pub dimension: usize,
    pub text_version: String,
}

impl EmbeddingSettings {
    pub fn with_entity_fields(
        mut self,
        fields: EntityEmbeddingFields,
    ) -> Result<Self, BackendError> {
        fields.validate()?;
        self.text_version = fields.text_version();
        self.entity_fields = fields;
        Ok(self)
    }

    /// Settings of a configured provider. A blank model or zero dimension is
    /// a configuration error before any work starts. The disabled placeholder
    /// is accepted as-is: it identifies itself in the fingerprint and fails
    /// the run at the first embedding request instead.
    pub fn of(backend: &dyn EmbedBackend) -> Result<Self, ConfigError> {
        let model = backend.model_id().trim();
        if model.is_empty() {
            return Err(ConfigError::InvalidValue {
                field: "embedder".into(),
                message: "model identifier is blank".into(),
            });
        }
        if backend.dimension() == 0 && backend.is_configured() {
            return Err(ConfigError::InvalidValue {
                field: "embedder".into(),
                message: "dimension must be positive".into(),
            });
        }
        Ok(Self {
            entity_fields: EntityEmbeddingFields::default(),
            model: model.to_string(),
            dimension: backend.dimension(),
            text_version: TEXT_VERSION.into(),
        })
    }
}

/// Type and name followed by selected scalar properties in policy order.
/// Each field and the complete text are bounded.
/// Everything the entity text is built from. Ingest (`entity_text`) and the
/// stored-row rebuild construct the same input, so both render the same text.
pub struct EntityText<'a> {
    pub entity_type: &'a str,
    pub name: &'a str,
    pub summary: Option<&'a str>,
    pub properties: &'a IndexMap<String, PropertyValue>,
    /// Every declared key property: primary and every additional group component.
    pub key_properties: &'a [String],
    pub labels: &'a [String],
}

/// Whether a string is an identifier rather than a word: URI-like (`arn:`,
/// `urn:`, `https://`), host:port, an email, a UUID, an IPv4/IPv6 address or CIDR
/// block, a MAC address, an ISO date or date-time, a hexadecimal digest of 16+
/// characters, or an opaque token (no whitespace; digits dominate a run of 8+
/// alphanumerics, or letters and digits mix over 12+ with at least 40 % digits, or
/// a `prefix-hex…` shape with a hexadecimal tail of 6+). Provider-neutral by
/// construction: no vocabulary, only shape.
pub fn identifier_like(value: &str) -> bool {
    let v = value.trim();
    if v.is_empty() {
        return false;
    }
    // ISO date or date-time, with `T` or a single space between date and time.
    let bytes = v.as_bytes();
    if bytes.len() >= 10
        && bytes[..4].iter().all(u8::is_ascii_digit)
        && bytes[4] == b'-'
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[7] == b'-'
        && bytes[8..10].iter().all(u8::is_ascii_digit)
        && (bytes.len() == 10 || matches!(bytes[10], b'T' | b't' | b' '))
    {
        return true;
    }
    if v.chars().any(char::is_whitespace) {
        return false;
    }
    // A list of identifiers is an identifier (`subnet-a,subnet-b`).
    if v.contains([',', ';']) {
        let pieces: Vec<&str> = v
            .split([',', ';'])
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .collect();
        if !pieces.is_empty() && pieces.iter().all(|p| identifier_like(p)) {
            return true;
        }
    }
    if v.contains('@') {
        return true;
    }
    // URI-like: `scheme:` followed by more structure; `A::B::C` is a path of
    // words (a Rust path, a resource type), not a URI.
    if let Some((scheme, rest)) = v.split_once(':') {
        if (2..=12).contains(&scheme.len())
            && scheme.chars().all(|c| c.is_ascii_alphabetic())
            && !rest.starts_with(':')
            && (rest.starts_with("//") || rest.contains(':') || rest.contains('/'))
        {
            return true;
        }
    }
    if let Some((host, port)) = v.rsplit_once(':') {
        if !port.is_empty()
            && port.chars().all(|c| c.is_ascii_digit())
            && (host.contains('.') || host == "localhost")
            && !host.contains(':')
        {
            return true;
        }
    }
    // A fully qualified hostname of four or more labels.
    let host_labels: Vec<&str> = v.split('.').collect();
    if host_labels.len() >= 4
        && host_labels
            .iter()
            .all(|l| !l.is_empty() && l.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'))
        && host_labels
            .last()
            .is_some_and(|tld| tld.chars().all(|c| c.is_ascii_alphabetic()))
    {
        return true;
    }
    let hex = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_ascii_hexdigit());
    // UUID.
    let parts: Vec<&str> = v.split('-').collect();
    if parts.len() == 5
        && [8, 4, 4, 4, 12]
            == [
                parts[0].len(),
                parts[1].len(),
                parts[2].len(),
                parts[3].len(),
                parts[4].len(),
            ]
        && parts.iter().all(|p| hex(p))
    {
        return true;
    }
    // IPv4, optionally CIDR.
    let (address, mask) = v.split_once('/').unwrap_or((v, ""));
    if mask.is_empty() || mask.chars().all(|c| c.is_ascii_digit()) {
        let octets: Vec<&str> = address.split('.').collect();
        if octets.len() == 4
            && octets
                .iter()
                .all(|o| !o.is_empty() && o.len() <= 3 && o.chars().all(|c| c.is_ascii_digit()))
        {
            return true;
        }
    }
    // IPv6: hexadecimal groups with at least two colons, optional zone or prefix.
    let ipv6 = v.split(['%', '/']).next().unwrap_or(v);
    if ipv6.matches(':').count() >= 2
        && ipv6
            .split(':')
            .all(|g| g.is_empty() || (g.len() <= 4 && hex(g)))
    {
        return true;
    }
    // MAC address.
    for separator in [':', '-'] {
        let groups: Vec<&str> = v.split(separator).collect();
        if groups.len() == 6 && groups.iter().all(|g| g.len() == 2 && hex(g)) {
            return true;
        }
    }
    // Hexadecimal digest, or an unbroken alphanumeric token of 16+ carrying
    // digits in one case only or a quarter digits (access keys, session ids,
    // base32/base62 handles); `Windows2019Datacenter` stays a word.
    if v.len() >= 16 && v.chars().all(|c| c.is_ascii_alphanumeric()) {
        let digits = v.chars().filter(|c| c.is_ascii_digit()).count();
        let uppers = v.chars().filter(|c| c.is_ascii_uppercase()).count();
        let lowers = v.chars().filter(|c| c.is_ascii_lowercase()).count();
        if hex(v) || (digits > 0 && (uppers == 0 || lowers == 0)) || digits * 4 >= v.len() {
            return true;
        }
    }
    // Base64-like material: 20+ characters of letters, digits and `+/=`, mixed
    // case with digits (keys, JWT segments).
    if v.len() >= 20
        && v.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '/' | '=' | '.' | '_'))
        && v.chars().any(|c| c.is_ascii_uppercase())
        && v.chars().any(|c| c.is_ascii_lowercase())
        && (v.contains(['+', '/', '='])
            || v.chars().filter(|c| c.is_ascii_digit()).count() * 4 >= v.len())
    {
        return true;
    }
    // Opaque tokens.
    let alnum: Vec<char> = v.chars().filter(char::is_ascii_alphanumeric).collect();
    let digits = alnum.iter().filter(|c| c.is_ascii_digit()).count();
    let letters = alnum.len() - digits;
    if alnum.len() >= 8 && digits * 10 >= alnum.len() * 6 {
        return true;
    }
    if alnum.len() >= 12 && letters > 0 && digits * 10 >= alnum.len() * 4 {
        return true;
    }
    if let Some((prefix, tail)) = v.rsplit_once(['-', '_']) {
        let prefix_ok = !prefix.is_empty()
            && prefix
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
        // `i-0abc123`, `vol-0acme…`: a hexadecimal tail with a digit.
        if prefix_ok && tail.len() >= 6 && hex(tail) && tail.chars().any(|c| c.is_ascii_digit()) {
            return true;
        }
        // `subneta-0acme0219b9ec00`, `eni-attach0-0acme08…`: a digit-led
        // alphanumeric tail of ten or more.
        if prefix_ok
            && tail.len() >= 10
            && tail.starts_with(|c: char| c.is_ascii_digit())
            && tail.chars().all(|c| c.is_ascii_alphanumeric())
        {
            return true;
        }
    }
    false
}

/// Whether any segment of a property path names a credential
/// (`Credentials.Username`, `db.password`). Free text under other names embeds
/// verbatim: this is a naming rule, not redaction.
fn sensitive_path(path: &str) -> bool {
    path.split('.').any(|segment| {
        let segment = segment.to_ascii_lowercase();
        SENSITIVE_NAME_PARTS
            .iter()
            .any(|part| segment.contains(part))
    })
}

/// Tag lines of a tag-like list: a JSON array of objects each carrying a
/// key-like (`Key`, `key`, `Name`, `name`) and a value-like (`Value`, `value`)
/// string field. Anything else is not a tag list.
fn tag_pairs(value: &PropertyValue) -> Vec<(String, String)> {
    let PropertyValue::Json(raw) = value else {
        return Vec::new();
    };
    let Ok(Value::Array(items)) = serde_json::from_str::<Value>(raw) else {
        return Vec::new();
    };
    if items.is_empty() {
        return Vec::new();
    }
    let mut pairs = Vec::with_capacity(items.len());
    for item in &items {
        let Value::Object(fields) = item else {
            return Vec::new();
        };
        let key = ["Key", "key", "Name", "name"]
            .iter()
            .find_map(|k| fields.get(*k).and_then(Value::as_str));
        let tag_value = ["Value", "value"]
            .iter()
            .find_map(|k| fields.get(*k).and_then(Value::as_str));
        match (key, tag_value) {
            (Some(key), Some(tag_value)) if !key.trim().is_empty() => {
                pairs.push((key.trim().to_owned(), tag_value.trim().to_owned()));
            }
            _ => return Vec::new(),
        }
    }
    pairs
}

fn scalar_for_text(value: &PropertyValue) -> Option<String> {
    match value {
        PropertyValue::String(v) => Some(v.clone()),
        // Capacities and counts carry meaning; epoch millis and counters do not.
        PropertyValue::Integer(v) if v.unsigned_abs() < 1_000_000_000 => Some(v.to_string()),
        PropertyValue::Bool(v) => Some(v.to_string()),
        _ => None,
    }
}

/// The embedding text of one entity: see [`EntityEmbeddingFields`].
pub fn representation(input: &EntityText<'_>, fields: &EntityEmbeddingFields) -> String {
    let mut text = String::new();
    let mut remaining = MAX_TEXT_CHARS;
    append_field(&mut text, &mut remaining, "type", input.entity_type);
    append_field(&mut text, &mut remaining, "name", input.name);
    let summary_of = |field: &str| {
        if field == "summary" {
            input
                .summary
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned)
                .or_else(|| scalar_property(input.properties, field))
        } else {
            scalar_property(input.properties, field)
        }
    };
    let mut lines: Vec<(String, String)> = Vec::new();
    if let Some(explicit) = fields.by_entity_type.get(input.entity_type) {
        for field in explicit {
            if let Some(value) = summary_of(field) {
                lines.push((field.clone(), value));
            }
        }
    } else {
        for field in &fields.default {
            if let Some(value) = summary_of(field) {
                lines.push((field.clone(), value));
            }
        }
        let forced: HashSet<&str> = fields.default.iter().map(String::as_str).collect();
        let excluded: HashSet<&str> = fields.exclude.iter().map(String::as_str).collect();
        let keys: HashSet<&str> = input.key_properties.iter().map(String::as_str).collect();
        let mut automatic: Vec<(String, String)> = input
            .properties
            .iter()
            .filter(|(path, _)| {
                !forced.contains(path.as_str())
                    && !excluded.contains(path.as_str())
                    && !keys.contains(path.as_str())
                    && !path.starts_with("_astrolabe_")
                    && !sensitive_path(path)
            })
            .filter_map(|(path, value)| scalar_for_text(value).map(|v| (path.clone(), v)))
            .filter(|(_, value)| {
                let value = value.trim();
                !value.is_empty() && value != input.name && !identifier_like(value)
            })
            .collect();
        automatic.sort();
        lines.extend(automatic);
    }
    for (field, value) in lines.iter().take(MAX_PROPERTY_LINES) {
        append_field(&mut text, &mut remaining, field, value);
    }
    // Tag-like lists, by property path.
    let mut tag_paths: Vec<&String> = input.properties.keys().collect();
    tag_paths.sort();
    let mut tag_lines = 0usize;
    for path in tag_paths {
        if path.starts_with("_astrolabe_")
            || sensitive_path(path)
            || fields.exclude.iter().any(|e| e == path)
        {
            continue;
        }
        for (key, value) in tag_pairs(&input.properties[path]) {
            if tag_lines >= MAX_TAG_LINES {
                break;
            }
            if value.is_empty()
                || value == input.name
                || identifier_like(&value)
                || sensitive_path(&key)
            {
                continue;
            }
            append_field(&mut text, &mut remaining, &format!("tag {key}"), &value);
            tag_lines += 1;
        }
    }
    if !input.labels.is_empty() {
        let mut labels: Vec<&str> = input
            .labels
            .iter()
            .map(String::as_str)
            .filter(|l| !l.trim().is_empty())
            .collect();
        labels.sort_unstable();
        labels.dedup();
        let joined = labels
            .iter()
            .take(MAX_LABELS)
            .copied()
            .collect::<Vec<_>>()
            .join(", ");
        append_field(&mut text, &mut remaining, "labels", &joined);
    }
    text
}

/// Every declared key property of an entity: primary keys and each component of
/// each additional key group.
pub fn key_properties(primary: &[String], additional: &[Vec<String>]) -> Vec<String> {
    let mut keys: Vec<String> = primary.to_vec();
    keys.extend(additional.iter().flatten().cloned());
    keys.sort();
    keys.dedup();
    keys
}

/// The representation of one resolved entity version.
pub fn entity_text(entity: &EntityNode, settings: &EmbeddingSettings) -> String {
    let keys = key_properties(
        &entity.primary_key_properties,
        &entity.additional_key_properties,
    );
    let labels = entity.effective_labels();
    representation(
        &EntityText {
            entity_type: &entity.entity_type,
            name: &entity.name,
            summary: entity.summary.as_deref(),
            properties: &entity.all_properties,
            key_properties: &keys,
            labels: &labels,
        },
        &settings.entity_fields,
    )
}

/// Content identity of an embedding text, stored with the vector so reuse
/// never needs the text again.
pub fn content_hash(text: &str) -> String {
    format!("{:016x}", xxh3_64(text.as_bytes()))
}

fn scalar_property(properties: &IndexMap<String, PropertyValue>, key: &str) -> Option<String> {
    match properties.get(key)? {
        PropertyValue::String(value) => Some(value.clone()),
        PropertyValue::Integer(value) => Some(value.to_string()),
        PropertyValue::Float(value) if value.is_finite() => Some(value.to_string()),
        PropertyValue::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn append_field(text: &mut String, remaining: &mut usize, label: &str, value: &str) {
    let value: String = value.chars().take(MAX_FIELD_CHARS).collect();
    let value = value.trim();
    let overhead = label.len() + 2 + usize::from(!text.is_empty());
    if value.is_empty() || *remaining <= overhead {
        return;
    }
    if !text.is_empty() {
        text.push('\n');
    }
    text.push_str(label);
    text.push_str(": ");
    *remaining -= overhead;
    for ch in value.chars().take(*remaining) {
        text.push(ch);
        *remaining -= 1;
    }
}

/// A vector together with the settings and content it was computed from.
#[derive(Debug, Clone, PartialEq)]
pub struct ComputedEmbedding {
    pub model: String,
    pub text_version: String,
    pub content_hash: String,
    pub values: Vec<f32>,
}

impl ComputedEmbedding {
    pub fn new(settings: &EmbeddingSettings, content_hash: String, values: Vec<f32>) -> Self {
        Self {
            model: settings.model.clone(),
            text_version: settings.text_version.clone(),
            content_hash,
            values,
        }
    }

    /// Whether this vector is the expected embedding of `content_hash` under
    /// `settings`: same model, text version, dimension, and content.
    pub fn matches(&self, settings: &EmbeddingSettings, content_hash: &str) -> bool {
        self.model == settings.model
            && self.text_version == settings.text_version
            && self.values.len() == settings.dimension
            && self.content_hash == content_hash
            && self.values.iter().all(|value| value.is_finite())
            && self.values.iter().any(|value| *value != 0.0)
    }

    /// The embedding stored on a version, when it carries its provenance.
    pub fn from_stored(record: &EntityVersionRecord) -> Option<Self> {
        let embedding = record.embedding.as_ref()?;
        let text = |key: &str| {
            record
                .stored
                .get(key)
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
        };
        Some(Self {
            model: embedding.model.clone(),
            text_version: text(TEXT_VERSION_PROPERTY)?,
            content_hash: text(CONTENT_HASH_PROPERTY)?,
            values: embedding.values.clone(),
        })
    }

    /// The model-tagged vector as the graph stores it.
    pub fn stored(&self) -> GraphEmbedding {
        GraphEmbedding {
            model: self.model.clone(),
            values: self.values.clone(),
        }
    }
}

/// Check a provider response against its request: one vector per input, in
/// order, of the configured dimension, finite, and nonzero.
pub fn validate_vectors(
    settings: &EmbeddingSettings,
    expected: usize,
    vectors: &[Vec<f32>],
) -> Result<(), BackendError> {
    if vectors.len() != expected {
        return Err(BackendError::Deserialization(format!(
            "embedding provider returned {} vectors for {expected} inputs",
            vectors.len()
        )));
    }
    for (index, values) in vectors.iter().enumerate() {
        if values.len() != settings.dimension {
            return Err(BackendError::Deserialization(format!(
                "embedding {index} has {} dimensions; model `{}` is configured for {}",
                values.len(),
                settings.model,
                settings.dimension
            )));
        }
        if values.iter().any(|v| !v.is_finite()) || values.iter().all(|v| *v == 0.0) {
            return Err(BackendError::Deserialization(format!(
                "embedding {index} is not a finite, nonzero vector"
            )));
        }
    }
    Ok(())
}

/// Embed `texts` with the run's shared model under the embedding semaphore
/// and verify the response. Inputs must be non-empty and within
/// [`MAX_TEXT_CHARS`]. Provider batches respect capacity and cancellation; adapters
/// own per-attempt deadlines and retries. No partial response is published.
pub async fn embed(ctx: &RuntimeContext, texts: &[&str]) -> Result<Vec<Vec<f32>>, BackendError> {
    if texts.is_empty() {
        return Ok(vec![]);
    }
    for (index, text) in texts.iter().enumerate() {
        if text.trim().is_empty() {
            return Err(BackendError::Query(format!(
                "embedding input {index} is empty"
            )));
        }
        if text.chars().count() > MAX_TEXT_CHARS {
            return Err(BackendError::Query(format!(
                "embedding input {index} exceeds {MAX_TEXT_CHARS} characters"
            )));
        }
    }
    let capacity = ctx.embedder.max_batch_size();
    if capacity == 0 {
        return Err(BackendError::Query(
            "embedding provider has zero batch capacity".into(),
        ));
    }
    let _permit = tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => return Err(BackendError::Unavailable("embedding cancelled".into())),
        permit = crate::telemetry::acquire(&ctx.embed_semaphore, crate::telemetry::OperationKind::Embedding) => permit.map_err(|_| BackendError::Unavailable("embedding pool is closed".into()))?,
    };
    let mut all = Vec::with_capacity(texts.len());
    for batch in texts.chunks(capacity) {
        let vectors = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => return Err(BackendError::Unavailable("embedding cancelled".into())),
            response = ctx.embedder.embed_batch(batch) => response?,
        };
        validate_vectors(&ctx.embedding, batch.len(), &vectors)?;
        all.extend(vectors);
    }
    Ok(all)
}

#[cfg(test)]
mod tests;
