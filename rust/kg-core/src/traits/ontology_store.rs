//! Mutable extraction guidance and relationship naming. Vocabulary can reject edges;
//! updates do not change adopted identity schemas or rewrite existing graph data.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::errors::BackendError;
use crate::models::{EdgeTypeMapEntry, EdgeTypeSchema, EntityTypeSchema};

/// Organization guidance, optionally source-scoped, with a
/// controlled relationship vocabulary and alias map.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Ontology {
    /// Keep an empty vocabulary restrictive after intersecting policies.
    #[serde(default)]
    pub restricted_relationships: bool,
    /// None is open; an empty list denies every directed type/name signature.
    #[serde(default)]
    pub allowed_relationships: Option<Vec<EdgeTypeMapEntry>>,
    #[serde(default)]
    pub entity_types: Vec<EntityTypeSchema>,
    #[serde(default)]
    pub edge_types: Vec<EdgeTypeSchema>,
    /// Discovery guidance for directed `(source_type, target_type) -> edge_name` mappings.
    #[serde(default)]
    pub edge_type_map: Vec<EdgeTypeMapEntry>,
    /// Nonempty vocabulary restricts accepted relationship names.
    #[serde(default)]
    pub relationship_vocabulary: Vec<String>,
    /// Alias-to-canonical names applied during edge creation.
    #[serde(default)]
    pub relation_aliases: BTreeMap<String, String>,
}

impl Ontology {
    /// True when the ontology carries no guidance at all.
    pub fn is_empty(&self) -> bool {
        !self.restricted_relationships
            && self.allowed_relationships.is_none()
            && self.entity_types.is_empty()
            && self.edge_types.is_empty()
            && self.edge_type_map.is_empty()
            && self.relationship_vocabulary.is_empty()
            && self.relation_aliases.is_empty()
    }

    /// Source entries override matching type names, mapping tuples, and aliases.
    /// Vocabulary restrictions intersect; an empty intersection denies every name.
    pub fn overlaid_with(&self, other: &Ontology) -> Ontology {
        fn merge_named<T, F: Fn(&T) -> &str>(over: &[T], base: &[T], name: F) -> Vec<T>
        where
            T: Clone,
        {
            let taken: std::collections::HashSet<String> =
                over.iter().map(|t| name(t).to_string()).collect();
            over.iter()
                .cloned()
                .chain(base.iter().filter(|t| !taken.contains(name(t))).cloned())
                .collect()
        }
        let base_restricted =
            self.restricted_relationships || !self.relationship_vocabulary.is_empty();
        let over_restricted =
            other.restricted_relationships || !other.relationship_vocabulary.is_empty();
        let vocab = match (base_restricted, over_restricted) {
            (true, true) => self
                .relationship_vocabulary
                .iter()
                .filter(|name| {
                    other.relationship_vocabulary.iter().any(|other| {
                        *name == other
                            || (!normalize_relationship(name).is_empty()
                                && normalize_relationship(name) == normalize_relationship(other))
                    })
                })
                .cloned()
                .collect(),
            (true, false) => self.relationship_vocabulary.clone(),
            _ => other.relationship_vocabulary.clone(),
        };
        let mut aliases = self.relation_aliases.clone();
        aliases.extend(other.relation_aliases.clone());
        Ontology {
            restricted_relationships: base_restricted || over_restricted,
            allowed_relationships: intersect_signatures(
                self.allowed_relationships.as_deref(),
                other.allowed_relationships.as_deref(),
            ),
            entity_types: merge_named(&other.entity_types, &self.entity_types, |t| &t.name),
            edge_types: merge_named(&other.edge_types, &self.edge_types, |t| &t.name),
            edge_type_map: {
                let key = |e: &EdgeTypeMapEntry| {
                    (
                        e.source_type.clone(),
                        e.target_type.clone(),
                        e.edge_name.clone(),
                    )
                };
                let taken: std::collections::HashSet<_> =
                    other.edge_type_map.iter().map(key).collect();
                other
                    .edge_type_map
                    .iter()
                    .cloned()
                    .chain(
                        self.edge_type_map
                            .iter()
                            .filter(|e| !taken.contains(&key(e)))
                            .cloned(),
                    )
                    .collect()
            },
            relationship_vocabulary: vocab,
            relation_aliases: aliases,
        }
    }

    /// `Entity` is a wildcard endpoint; relationship names remain declared names.
    pub fn permits_signature(&self, source: &str, target: &str, name: &str) -> bool {
        self.allowed_relationships.as_ref().is_none_or(|allowed| {
            allowed.iter().any(|entry| {
                entry.edge_name == name
                    && (entry.source_type == "Entity" || entry.source_type == source)
                    && (entry.target_type == "Entity" || entry.target_type == target)
            })
        })
    }

    /// Apply aliases and match controlled vocabulary, if configured.
    /// Return `None` for unmatched vocabulary entries so callers can drop the edge.
    pub fn canonical_relationship(&self, name: &str) -> Option<String> {
        // Exact aliases take precedence over normalized spelling variants.
        let aliased = self
            .relation_aliases
            .get(name)
            .cloned()
            .or_else(|| {
                let n = normalize_relationship(name);
                if n.is_empty() {
                    return None;
                }
                self.relation_aliases
                    .iter()
                    .find(|(k, _)| normalize_relationship(k) == n)
                    .map(|(_, v)| v.clone())
            })
            .unwrap_or_else(|| name.to_string());

        if self.relationship_vocabulary.is_empty() && !self.restricted_relationships {
            return Some(aliased);
        }
        if self.relationship_vocabulary.contains(&aliased) {
            return Some(aliased);
        }
        let target = normalize_relationship(&aliased);
        if target.is_empty() {
            return None;
        }
        self.relationship_vocabulary
            .iter()
            .find(|v| normalize_relationship(v) == target)
            .cloned()
    }
}

/// Intersect directed signatures, retaining the more specific wildcard endpoint.
pub(crate) fn intersect_signatures(
    base: Option<&[EdgeTypeMapEntry]>,
    over: Option<&[EdgeTypeMapEntry]>,
) -> Option<Vec<EdgeTypeMapEntry>> {
    let (Some(base), Some(over)) = (base, over) else {
        return base.or(over).map(<[_]>::to_vec);
    };
    let endpoint = |a: &str, b: &str| {
        if a == b || b == "Entity" {
            Some(a.to_owned())
        } else if a == "Entity" {
            Some(b.to_owned())
        } else {
            None
        }
    };
    let mut matches = BTreeMap::new();
    for a in base {
        for b in over {
            if a.edge_name != b.edge_name {
                continue;
            }
            let (Some(source_type), Some(target_type)) = (
                endpoint(&a.source_type, &b.source_type),
                endpoint(&a.target_type, &b.target_type),
            ) else {
                continue;
            };
            matches.insert(
                (
                    source_type.clone(),
                    target_type.clone(),
                    a.edge_name.clone(),
                ),
                EdgeTypeMapEntry {
                    source_type,
                    target_type,
                    edge_name: a.edge_name.clone(),
                    description: None,
                },
            );
            // The final validator rejects this sentinel count instead of allocating an unbounded cross product.
            if matches.len() > 512 {
                return Some(matches.into_values().collect());
            }
        }
    }
    Some(matches.into_values().collect())
}

/// Uppercase Unicode alphanumerics; collapse separators to `_` and trim the ends.
pub fn normalize_relationship(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut prev_us = false;
    for ch in name.trim().chars() {
        if ch.is_alphanumeric() {
            out.extend(ch.to_uppercase());
            prev_us = false;
        } else if !prev_us {
            out.push('_');
            prev_us = true;
        }
    }
    out.trim_matches('_').to_string()
}

/// Group names by normalized form; choose the most frequent raw name, then alphabetically.
/// Return aliases only for names differing from their group's canonical name.
pub fn propose_relation_aliases(names: &[String]) -> BTreeMap<String, String> {
    use std::collections::HashMap;
    let mut clusters: HashMap<String, HashMap<String, usize>> = HashMap::new();
    for raw in names {
        let norm = normalize_relationship(raw);
        if norm.is_empty() {
            continue;
        }
        *clusters
            .entry(norm)
            .or_default()
            .entry(raw.clone())
            .or_insert(0) += 1;
    }
    let mut out = BTreeMap::new();
    for variants in clusters.values() {
        if variants.len() < 2 {
            continue;
        }
        let canonical = variants
            .iter()
            .max_by(|a, b| a.1.cmp(b.1).then_with(|| b.0.cmp(a.0)))
            .map(|(name, _)| name.clone())
            .unwrap();
        for raw in variants.keys() {
            if *raw != canonical {
                out.insert(raw.clone(), canonical.clone());
            }
        }
    }
    out
}

/// Ontology storage keyed by organization and optional source.
/// The in-process store is process-local and treats `None` or empty source as defaults.
#[async_trait]
pub trait OntologyStore: Send + Sync + 'static {
    /// The stored ontology for an exact `(org_id, source)` key, if any.
    async fn get(
        &self,
        org_id: &str,
        source: Option<&str>,
    ) -> Result<Option<Ontology>, BackendError>;

    /// Create or replace the ontology for `(org_id, source)`.
    async fn put(
        &self,
        org_id: &str,
        source: Option<&str>,
        ontology: Ontology,
    ) -> Result<(), BackendError>;
}

/// Process-local [`OntologyStore`]: ontologies loaded from configuration for
/// one process, keyed by organization and optional source. Not durable.
#[derive(Debug, Default)]
pub struct InMemoryOntologyStore {
    ontologies: std::sync::RwLock<BTreeMap<(String, String), Ontology>>,
}

impl InMemoryOntologyStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl OntologyStore for InMemoryOntologyStore {
    async fn get(
        &self,
        org_id: &str,
        source: Option<&str>,
    ) -> Result<Option<Ontology>, BackendError> {
        Ok(self
            .ontologies
            .read()
            .map_err(|_| BackendError::Unavailable("ontology store lock is poisoned".into()))?
            .get(&(org_id.into(), source.unwrap_or("").into()))
            .cloned())
    }

    async fn put(
        &self,
        org_id: &str,
        source: Option<&str>,
        ontology: Ontology,
    ) -> Result<(), BackendError> {
        self.ontologies
            .write()
            .map_err(|_| BackendError::Unavailable("ontology store lock is poisoned".into()))?
            .insert((org_id.into(), source.unwrap_or("").into()), ontology);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn poisoned_store_returns_errors_instead_of_panicking() {
        let store = InMemoryOntologyStore::new();
        let _ = std::panic::catch_unwind(|| {
            let _guard = store.ontologies.write().unwrap();
            panic!("poison test store");
        });
        assert!(matches!(
            store.get("org", None).await,
            Err(BackendError::Unavailable(_))
        ));
        assert!(matches!(
            store.put("org", None, Ontology::default()).await,
            Err(BackendError::Unavailable(_))
        ));
    }

    #[test]
    fn normalize_collapses_separators_and_case() {
        assert_eq!(normalize_relationship("depends_on"), "DEPENDS_ON");
        assert_eq!(normalize_relationship("Depends On"), "DEPENDS_ON");
        assert_eq!(normalize_relationship("depends-on"), "DEPENDS_ON");
        assert_eq!(normalize_relationship("  uses  "), "USES");
        assert_eq!(normalize_relationship("RUNS//ON"), "RUNS_ON");
    }

    #[test]
    fn distinct_unicode_and_punctuation_names_do_not_collapse() {
        let ontology = Ontology {
            relationship_vocabulary: vec!["拥有".into(), "!!!".into()],
            relation_aliases: BTreeMap::from([("使用".into(), "拥有".into())]),
            ..Default::default()
        };
        assert_eq!(
            ontology.canonical_relationship("拥有").as_deref(),
            Some("拥有")
        );
        assert_eq!(
            ontology.canonical_relationship("使用").as_deref(),
            Some("拥有")
        );
        assert_eq!(ontology.canonical_relationship("依赖"), None);
        assert_eq!(ontology.canonical_relationship("???"), None);
        assert_eq!(
            ontology.canonical_relationship("!!!").as_deref(),
            Some("!!!")
        );
        assert_eq!(ontology.canonical_relationship(""), None);
        assert_ne!(
            normalize_relationship("USES中"),
            normalize_relationship("USES日")
        );
    }

    #[test]
    fn alias_then_vocabulary_canonicalization() {
        let onto = Ontology {
            relationship_vocabulary: vec!["DEPENDS_ON".into(), "USES".into()],
            relation_aliases: BTreeMap::from([("REQUIRES".to_string(), "DEPENDS_ON".to_string())]),
            ..Default::default()
        };
        assert_eq!(
            onto.canonical_relationship("REQUIRES").as_deref(),
            Some("DEPENDS_ON")
        );
        assert_eq!(onto.canonical_relationship("uses").as_deref(), Some("USES"));
        assert_eq!(onto.canonical_relationship("FROBNICATES"), None);
    }

    #[test]
    fn no_vocabulary_passes_names_through_after_alias() {
        let onto = Ontology {
            relation_aliases: BTreeMap::from([("USED_BY".to_string(), "USES".to_string())]),
            ..Default::default()
        };
        assert_eq!(
            onto.canonical_relationship("USED_BY").as_deref(),
            Some("USES")
        );
        assert_eq!(
            onto.canonical_relationship("ANYTHING").as_deref(),
            Some("ANYTHING")
        );
    }

    #[test]
    fn propose_clusters_surface_variants() {
        let names = vec![
            "DEPENDS_ON".to_string(),
            "depends_on".to_string(),
            "Depends On".to_string(),
            "USES".to_string(),
        ];
        let map = propose_relation_aliases(&names);
        assert_eq!(
            map.get("depends_on").map(String::as_str),
            Some("DEPENDS_ON")
        );
        assert_eq!(
            map.get("Depends On").map(String::as_str),
            Some("DEPENDS_ON")
        );
        assert!(!map.contains_key("USES"));
        assert!(!map.contains_key("DEPENDS_ON"));
    }

    #[test]
    fn overlay_preserves_distinct_mapping_tuples() {
        let entry = |source: &str, target: &str| EdgeTypeMapEntry {
            source_type: source.into(),
            target_type: target.into(),
            edge_name: "USES".into(),
            description: None,
        };
        let base = Ontology {
            edge_type_map: vec![entry("A|B", "C")],
            ..Default::default()
        };
        let override_ = Ontology {
            edge_type_map: vec![entry("A", "B|C")],
            ..Default::default()
        };
        assert_eq!(base.overlaid_with(&override_).edge_type_map.len(), 2);
        let mut replacement = entry("A|B", "C");
        replacement.description = Some("source-specific guidance".into());
        let override_ = Ontology {
            edge_type_map: vec![replacement.clone(), entry("A", "B|C")],
            relation_aliases: BTreeMap::from([("OWNS".into(), "USES".into())]),
            ..Default::default()
        };
        let base = Ontology {
            relation_aliases: BTreeMap::from([("OWNS".into(), "HAS".into())]),
            ..base
        };
        let merged = base.overlaid_with(&override_);
        assert_eq!(merged.edge_type_map.len(), 2);
        assert_eq!(merged.edge_type_map[0], replacement);
        assert_eq!(merged.edge_type_map[1], entry("A", "B|C"));
        assert_eq!(merged.relation_aliases["OWNS"], "USES");
    }

    #[test]
    fn overlay_source_over_org_default() {
        let base = Ontology {
            relationship_vocabulary: vec!["HAS".into()],
            relation_aliases: BTreeMap::from([("OWNS".to_string(), "HAS".to_string())]),
            ..Default::default()
        };
        let over = Ontology {
            relationship_vocabulary: vec!["USES".into()],
            relation_aliases: BTreeMap::from([("CONSUMES".to_string(), "USES".to_string())]),
            ..Default::default()
        };
        let merged = base.overlaid_with(&over);
        assert!(merged.relationship_vocabulary.is_empty());
        assert!(merged.restricted_relationships);
        assert_eq!(merged.canonical_relationship("USES"), None);
        assert_eq!(
            merged.relation_aliases.get("OWNS").map(String::as_str),
            Some("HAS")
        );
        assert_eq!(
            merged.relation_aliases.get("CONSUMES").map(String::as_str),
            Some("USES")
        );
    }
}
