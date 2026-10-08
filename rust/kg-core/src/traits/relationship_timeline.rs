//! Exact relationship state used to reject plans made against an older timeline.

use std::collections::HashSet;

use chrono::DateTime;
use serde_json::Value;
use uuid::Uuid;

use super::GraphProperties;
use crate::errors::BackendError;

/// Oversized histories fail explicitly; no partial timeline may authorize a write.
pub const MAX_VERSIONS: usize = 10_000;
pub const EMBEDDING_PROPERTIES: [&str; 4] = [
    "embedding",
    "embedding_model",
    "embedding_text_version",
    "embedding_content_hash",
];

/// Endpoint identity is part of the snapshot even when a relationship is rewired.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VersionState {
    pub target_chain_id: Uuid,
    pub properties: GraphProperties,
}

/// Both endpoint chains are retained when a deletion captures incident history.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IncidentVersionState {
    pub source_chain_id: Uuid,
    pub target_chain_id: Uuid,
    pub properties: GraphProperties,
}

pub fn validate_incident(
    anchor: Uuid,
    versions: &[IncidentVersionState],
) -> Result<(), BackendError> {
    let invalid = || BackendError::Query("invalid incident relationship timeline baseline".into());
    if anchor.is_nil() || versions.len() > MAX_VERSIONS {
        return Err(invalid());
    }
    let mut ids = HashSet::new();
    for version in versions {
        validate(
            version.source_chain_id,
            version.target_chain_id,
            std::slice::from_ref(&version.properties),
        )?;
        if (version.source_chain_id != anchor && version.target_chain_id != anchor)
            || !ids.insert(version.properties["uuid"].as_str().unwrap_or_default())
        {
            return Err(invalid());
        }
    }
    Ok(())
}

pub fn validate_relation(
    source: Uuid,
    name: &str,
    versions: &[VersionState],
) -> Result<(), BackendError> {
    if name.trim().is_empty() || source.is_nil() || versions.len() > MAX_VERSIONS {
        return Err(BackendError::Query(
            "invalid relationship timeline baseline".into(),
        ));
    }
    let mut ids = HashSet::new();
    for version in versions {
        validate(
            source,
            version.target_chain_id,
            std::slice::from_ref(&version.properties),
        )?;
        if version.properties.get("name").and_then(Value::as_str) != Some(name)
            || !ids.insert(version.properties["uuid"].as_str().unwrap_or_default())
        {
            return Err(BackendError::Query(
                "invalid relationship timeline baseline".into(),
            ));
        }
    }
    Ok(())
}

pub fn state(properties: &GraphProperties) -> GraphProperties {
    properties
        .iter()
        .filter(|(key, value)| !value.is_null() && !EMBEDDING_PROPERTIES.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

pub fn validate(
    source: Uuid,
    target: Uuid,
    versions: &[GraphProperties],
) -> Result<(), BackendError> {
    let invalid = || BackendError::Query("invalid relationship timeline baseline".into());
    if source.is_nil() || target.is_nil() || versions.len() > MAX_VERSIONS {
        return Err(invalid());
    }
    let mut ids = HashSet::new();
    for properties in versions {
        let id = |key| {
            properties
                .get(key)
                .and_then(Value::as_str)
                .and_then(|text| Uuid::parse_str(text).ok())
                .filter(|id| !id.is_nil())
        };
        let uuid = id("uuid").ok_or_else(invalid)?;
        if !ids.insert(uuid)
            || id("chain_id").is_none()
            || properties
                .get("version")
                .and_then(Value::as_u64)
                .is_none_or(|version| version == 0 || version > u32::MAX as u64)
            || properties
                .get("is_latest")
                .and_then(Value::as_bool)
                .is_none()
            || properties
                .keys()
                .any(|key| EMBEDDING_PROPERTIES.contains(&key.as_str()))
            || properties
                .values()
                .any(|value| value.is_null() || value.is_object())
        {
            return Err(invalid());
        }
        let snapshot = properties.contains_key("cancellation_snapshot_id");
        let context = properties.get("cancellation_context");
        if properties.contains_key("cancelled_at") != (snapshot || context.is_some())
            || (snapshot && context.is_some())
            || (snapshot && id("cancellation_snapshot_id").is_none())
        {
            return Err(invalid());
        }
        if let Some(context) = context {
            let context: crate::models::CancellationContext =
                serde_json::from_str(context.as_str().ok_or_else(invalid)?)
                    .map_err(|_| invalid())?;
            context.validate().map_err(|_| invalid())?;
            if let crate::models::CancellationContext::Merge { effective_at, .. } = context {
                let cancelled = properties
                    .get("cancelled_at")
                    .and_then(Value::as_str)
                    .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                    .ok_or_else(invalid)?;
                if effective_at != cancelled {
                    return Err(invalid());
                }
            }
        }
        for key in [
            "valid_from",
            "valid_to",
            "invalid_at",
            "deleted_at",
            "last_seen_at",
            "last_transition_at",
            "cancelled_at",
        ] {
            if let Some(value) = properties.get(key) {
                if value
                    .as_str()
                    .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
                    .is_none()
                {
                    return Err(invalid());
                }
            } else if key == "valid_from" {
                return Err(invalid());
            }
        }
        if let Some(cancelled) = properties.get("cancelled_at") {
            let cancelled = DateTime::parse_from_rfc3339(cancelled.as_str().ok_or_else(invalid)?)
                .map_err(|_| invalid())?;
            let start = DateTime::parse_from_rfc3339(
                properties["valid_from"].as_str().ok_or_else(invalid)?,
            )
            .map_err(|_| invalid())?;
            if cancelled >= start || properties["is_latest"] != Value::Bool(false) {
                return Err(invalid());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn version() -> GraphProperties {
        json!({"uuid":Uuid::from_u128(1),"chain_id":Uuid::from_u128(2),
            "version":1,"is_latest":false,"valid_from":"2026-01-01T00:00:00Z",
            "valid_to":"2027-01-01T00:00:00Z","last_seen_at":"2026-02-01T00:00:00Z",
            "last_transition_at":"2026-03-01T00:00:00Z","prop_region":"east"})
        .as_object()
        .unwrap()
        .clone()
    }

    #[test]
    fn cancellation_is_retained_and_requires_provenance_before_activation() {
        let mut cancelled = version();
        cancelled.insert("cancelled_at".into(), json!("2025-12-01T00:00:00Z"));
        cancelled.insert("cancellation_snapshot_id".into(), json!(Uuid::new_v4()));
        let source = Uuid::from_u128(3);
        let target = Uuid::from_u128(4);
        assert!(validate(source, target, std::slice::from_ref(&cancelled)).is_ok());
        assert_eq!(state(&cancelled), cancelled);
        for (key, value) in [
            ("cancelled_at", json!("2026-01-01T00:00:00Z")),
            ("cancellation_snapshot_id", json!(Uuid::nil())),
            ("is_latest", json!(true)),
        ] {
            let mut invalid = cancelled.clone();
            invalid.insert(key.into(), value);
            assert!(validate(source, target, &[invalid]).is_err());
        }
        cancelled.remove("cancellation_snapshot_id");
        assert!(validate(source, target, &[cancelled]).is_err());
    }

    #[test]
    fn cancellation_context_is_typed_and_exclusive_with_snapshot_provenance() {
        use crate::{
            models::CancellationContext,
            traits::{BatchIdentity, BatchKind},
        };
        let source = Uuid::from_u128(3);
        let target = Uuid::from_u128(4);
        let mut cancelled = version();
        cancelled.insert("cancelled_at".into(), json!("2025-12-01T00:00:00Z"));
        for context in [
            CancellationContext::Batch {
                batch: BatchIdentity {
                    run_id: Uuid::new_v4(),
                    kind: BatchKind::Reconciliation,
                    index: 1,
                },
            },
            CancellationContext::Merge {
                loser_chain_id: source,
                winner_chain_id: target,
                effective_at: "2025-12-01T00:00:00Z".parse().unwrap(),
            },
        ] {
            cancelled.insert(
                "cancellation_context".into(),
                json!(serde_json::to_string(&context).unwrap()),
            );
            assert!(validate(source, target, &[cancelled.clone()]).is_ok());
            cancelled.insert("cancellation_snapshot_id".into(), json!(Uuid::new_v4()));
            assert!(validate(source, target, &[cancelled.clone()]).is_err());
            cancelled.remove("cancellation_snapshot_id");
        }
        for context in [
            json!({"kind":"batch","batch":{"run_id":Uuid::nil(),"kind":"reconciliation","index":1}}),
            json!({"kind":"batch","batch":{"run_id":Uuid::new_v4(),"kind":"reconciliation","index":1,"surprise":true}}),
            json!({"kind":"merge","loser_chain_id":source,"winner_chain_id":source,"effective_at":"2025-12-01T00:00:00Z"}),
            json!({"kind":"merge","loser_chain_id":source,"winner_chain_id":target,"effective_at":"2025-12-01T00:00:00Z","surprise":true}),
            json!({"kind":"made_up"}),
        ] {
            cancelled.insert("cancellation_context".into(), json!(context.to_string()));
            assert!(validate(source, target, &[cancelled.clone()]).is_err());
        }
    }

    #[test]
    fn snapshot_keeps_independent_clocks_and_content_but_ignores_vectors() {
        let original = version();
        let mut refreshed = original.clone();
        for key in EMBEDDING_PROPERTIES {
            refreshed.insert(key.into(), json!("changed"));
        }
        assert_eq!(state(&refreshed), original);
        for key in [
            "valid_from",
            "valid_to",
            "invalid_at",
            "deleted_at",
            "last_seen_at",
            "last_transition_at",
            "cancelled_at",
            "prop_region",
        ] {
            let mut changed = original.clone();
            changed.insert(key.into(), json!("2028-01-01T00:00:00Z"));
            assert_ne!(state(&changed), original, "{key}");
        }
    }

    #[test]
    fn malformed_or_duplicate_snapshots_cannot_authorize_a_commit() {
        let source = Uuid::from_u128(3);
        let target = Uuid::from_u128(4);
        let original = version();
        assert!(validate(source, target, std::slice::from_ref(&original)).is_ok());
        assert!(validate(source, target, &[original.clone(), original.clone()]).is_err());
        assert!(validate(Uuid::nil(), target, &[]).is_err());
        for (key, value) in [
            ("uuid", json!(Uuid::nil())),
            ("chain_id", json!("bad")),
            ("version", json!(0)),
            ("is_latest", json!("true")),
            ("valid_to", json!("bad")),
            ("valid_from", Value::Null),
            ("embedding", json!([0.1])),
        ] {
            let mut changed = original.clone();
            changed.insert(key.into(), value);
            assert!(validate(source, target, &[changed]).is_err(), "{key}");
        }
    }

    #[test]
    fn relation_snapshots_require_matching_names_unique_ids_and_valid_targets() {
        let source = Uuid::from_u128(3);
        let mut properties = version();
        properties.insert("name".into(), json!("USES"));
        let mut entry = VersionState {
            target_chain_id: Uuid::from_u128(4),
            properties,
        };
        assert!(validate_relation(source, "USES", std::slice::from_ref(&entry)).is_ok());
        assert!(validate_relation(source, "USES", &[entry.clone(), entry.clone()]).is_err());
        assert!(validate_relation(source, "OTHER", std::slice::from_ref(&entry)).is_err());
        entry.target_chain_id = Uuid::nil();
        assert!(validate_relation(source, "USES", &[entry]).is_err());
        for relations in [
            vec![(source, "".into())],
            vec![(source, "USES".into()), (source, "USES".into())],
            vec![(Uuid::nil(), "USES".into())],
        ] {
            assert!(super::super::EdgeLookup::VersionsByRelations { relations }
                .validate("org")
                .is_err());
        }
    }

    #[test]
    fn incident_snapshots_require_unique_ids_and_the_selected_endpoint() {
        let anchor = Uuid::from_u128(3);
        let mut entry = IncidentVersionState {
            source_chain_id: anchor,
            target_chain_id: Uuid::from_u128(4),
            properties: version(),
        };
        assert!(validate_incident(anchor, std::slice::from_ref(&entry)).is_ok());
        assert!(validate_incident(anchor, &[entry.clone(), entry.clone()]).is_err());
        assert!(validate_incident(Uuid::nil(), &[]).is_err());
        entry.source_chain_id = entry.target_chain_id;
        assert!(validate_incident(anchor, std::slice::from_ref(&entry)).is_err());
        entry.target_chain_id = anchor;
        assert!(validate_incident(anchor, std::slice::from_ref(&entry)).is_ok());
        entry.source_chain_id = Uuid::nil();
        assert!(validate_incident(anchor, &[entry]).is_err());
        for chain_ids in [
            vec![anchor, anchor],
            vec![Uuid::nil()],
            vec![anchor; super::super::graph_reads::MAX_LOOKUP_KEYS + 1],
        ] {
            assert!(
                super::super::EdgeLookup::VersionsByEndpointChains { chain_ids }
                    .validate("org")
                    .is_err()
            );
        }
    }

    #[test]
    fn timeline_requests_reject_duplicate_and_nil_pairs() {
        let pair = (Uuid::from_u128(1), Uuid::from_u128(2));
        for pairs in [vec![pair, pair], vec![(Uuid::nil(), pair.1)]] {
            assert!(super::super::EdgeLookup::VersionsByChainPairs { pairs }
                .validate("org")
                .is_err());
        }
    }
}
