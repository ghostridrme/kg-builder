use serde::{Deserialize, Serialize};

use super::sub_entity::SubEntityRule;

/// Rules for flattened properties. Normalization drops keys before JSON conversion,
/// child extraction, and hashing. Forced versioning overrides type-level exclusions,
/// but cannot restore dropped keys or override snapshot ignore rules.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntityTypeConfig {
    #[serde(default)]
    pub sub_entity_rules: Vec<SubEntityRule>,
    /// Properties whose changes can be merged in place without creating a version.
    #[serde(default)]
    pub volatile_properties: Vec<String>,
    /// Properties included in the hash despite volatile or explicit hash exclusions.
    #[serde(default)]
    pub always_version_on_change: Vec<String>,
    /// Stored properties excluded from the hash unless forced to participate.
    #[serde(default)]
    pub exclude_from_hash: Vec<String>,
    /// Paths removed after child extraction and before hashing. Identity paths cannot be dropped.
    #[serde(default)]
    pub drop_properties: Vec<String>,
    /// Existing property values serialized into the JSON property representation.
    #[serde(default)]
    pub force_json_properties: Vec<String>,
}

impl EntityTypeConfig {
    /// Volatile properties remaining after forced-version overrides.
    pub fn effective_volatile(&self) -> Vec<&str> {
        self.volatile_properties
            .iter()
            .filter(|p| !self.always_version_on_change.contains(p))
            .map(String::as_str)
            .collect()
    }

    /// Explicit and volatile hash exclusions after forced-version overrides.
    /// Snapshot-specific exclusions are added by the caller.
    pub fn hash_exclusions(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .exclude_from_hash
            .iter()
            .filter(|p| !self.always_version_on_change.contains(p))
            .cloned()
            .collect();
        for v in self.effective_volatile() {
            if !out.iter().any(|e| e == v) {
                out.push(v.to_string());
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn always_version_overrides_explicit_and_volatile_exclusions() {
        let config = EntityTypeConfig {
            exclude_from_hash: vec!["status".into(), "noise".into()],
            volatile_properties: vec!["status".into(), "heartbeat".into()],
            always_version_on_change: vec!["status".into()],
            ..Default::default()
        };
        assert_eq!(config.hash_exclusions(), vec!["noise", "heartbeat"]);
        assert_eq!(config.effective_volatile(), vec!["heartbeat"]);
    }
    #[test]
    fn misspelled_rules_are_rejected_instead_of_disabling_versioning() {
        assert!(
            serde_json::from_value::<EntityTypeConfig>(serde_json::json!({
                "always_version_on_chagne": ["status"]
            }))
            .is_err()
        );
        let rule = serde_json::json!({
            "source_path": "containers", "target_entity_type": "Container",
            "pk_properties": ["id"], "flat_properties": ["id"], "blob_properties": [],
            "edge_name": "contains", "edge_direction": "outgoing",
            "promote_to_parent": [], "keep_raw_blob": false
        });
        assert!(serde_json::from_value::<SubEntityRule>(rule.clone()).is_ok());
        let mut invalid = rule.clone();
        invalid["keep_raw_blbo"] = true.into();
        assert!(serde_json::from_value::<SubEntityRule>(invalid).is_err());
        let mut invalid = rule;
        invalid["promote_to_parent"] = serde_json::json!([{
            "source_field": "id", "target_field": "ids", "target_feild": "wrong"
        }]);
        assert!(serde_json::from_value::<SubEntityRule>(invalid).is_err());
    }
}
