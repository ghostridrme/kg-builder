//! Property tests for pure core rules: property-value codec round trips,
//! flattening, namespace policy consistency, sanitization, and edge validity.
use std::collections::HashSet;

use chrono::{DateTime, TimeZone, Utc};
use kg_core::models::{EntityEdge, PropertyValue, RelationshipOrigin};
use kg_core::sanitize::{fence_untrusted, validate_extracted_name, MAX_EXTRACTED_NAME_LEN};
use kg_core::tenant::namespace::{CrossNamespaceRule, EnvironmentTier, NamespacePolicy};
use proptest::prelude::*;
use serde_json::{json, Value};
use uuid::Uuid;

fn json_leaf() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|i| json!(i)),
        any::<u64>().prop_map(|u| json!(u)),
        (-1.0e12f64..1.0e12f64).prop_map(|f| json!(f)),
        "[ -~]{0,12}".prop_map(Value::String),
    ]
}

fn json_value() -> impl Strategy<Value = Value> {
    json_leaf().prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::btree_map("[a-z]{1,4}", inner, 0..4)
                .prop_map(|m| Value::Object(m.into_iter().collect())),
        ]
    })
}

proptest! {
    /// Decoding a source value and re-encoding it never changes it: scalars keep
    /// their type, everything else stays an opaque JSON document.
    #[test]
    fn property_value_source_round_trip(value in json_value()) {
        let decoded = PropertyValue::from_source(&value);
        let back = decoded.to_source().expect("finite JSON always re-encodes");
        prop_assert_eq!(back, value);
    }

    /// Flattening an object without dotted keys always succeeds and yields
    /// paths where no key is the parent of another.
    #[test]
    fn flatten_source_yields_prefix_free_paths(value in json_value()) {
        let object = json!({"root": value});
        let flat = PropertyValue::flatten_source(&object, &[]).expect("dot-free keys never collide");
        prop_assert!(PropertyValue::validate_flat_paths(&flat).is_ok());
        let keys: HashSet<&String> = flat.keys().collect();
        for key in &keys {
            for (index, _) in key.match_indices('.') {
                prop_assert!(!keys.contains(&key[..index].to_string()), "{} has a flattened ancestor", key);
            }
        }
        // Opaque paths never introduce collisions either.
        let opaque = PropertyValue::flatten_source(&object, &["root".to_string()]).unwrap();
        prop_assert_eq!(opaque.len(), 1);
    }
}

fn policy() -> impl Strategy<Value = NamespacePolicy> {
    let ns = "[a-d]";
    let tier = ("[t-w]", prop::collection::vec(ns, 0..3))
        .prop_map(|(name, namespaces)| EnvironmentTier { name, namespaces });
    let rule =
        ("[t-w]", prop::collection::vec("[t-w]", 0..3)).prop_map(|(source_tier, target_tiers)| {
            CrossNamespaceRule {
                source_tier,
                target_tiers,
            }
        });
    (
        prop::collection::vec(tier, 0..4),
        prop::collection::vec(rule, 0..4),
        any::<bool>(),
    )
        .prop_map(
            |(environment_tiers, cross_namespace_rules, open_policy)| NamespacePolicy {
                environment_tiers,
                cross_namespace_rules,
                open_policy,
            },
        )
}

proptest! {
    /// The pre-filter list and the per-pair decision are one rule: a target is
    /// allowed iff it is in the allowed list (or the policy is open).
    #[test]
    fn namespace_policy_allowed_targets_agree_with_allows(
        policy in policy(), source in "[a-e]", target in "[a-e]"
    ) {
        let decision = policy.allows(&source, &target);
        match policy.allowed_targets(&source) {
            None => prop_assert!(policy.open_policy && decision),
            Some(allowed) => {
                prop_assert!(allowed.windows(2).all(|w| w[0] < w[1]), "sorted and deduplicated");
                prop_assert!(allowed.contains(&source), "the source namespace is always allowed");
                prop_assert_eq!(decision, allowed.contains(&target));
            }
        }
    }

    /// Fencing keeps the source text intact in one JSON string and
    /// name validation never panics, whatever the model returned.
    #[test]
    fn sanitizers_hold_for_arbitrary_text(content in "\\PC{0,64}", name in "\\PC{0,400}") {
        let fenced = fence_untrusted(&content);
        let framed: serde_json::Value = serde_json::from_str(&fenced).unwrap();
        prop_assert_eq!(framed.as_object().unwrap().len(), 1);
        prop_assert_eq!(framed["source_data"].as_str(), Some(content.as_str()));

        let verdict = validate_extracted_name(&name);
        if name.len() > MAX_EXTRACTED_NAME_LEN {
            prop_assert!(verdict.is_err(), "over-long names are rejected");
        }
        if name.trim().is_empty() || name.chars().any(char::is_control) {
            prop_assert!(verdict.is_err());
        }
    }
}

fn edge(
    valid_from: i64,
    valid_to: Option<i64>,
    cancelled: Option<i64>,
    deleted: Option<i64>,
) -> EntityEdge {
    let at = |s: i64| Utc.timestamp_opt(s, 0).unwrap();
    EntityEdge {
        time_evidence: None,
        uuid: Uuid::from_u128(1),
        chain_id: Uuid::from_u128(2),
        identity_hash: None,
        cardinality_key: None,
        origin: RelationshipOrigin::Declared,
        producer_source: "test".into(),
        org_id: "org".into(),
        source_chain_id: Uuid::from_u128(3),
        target_chain_id: Uuid::from_u128(4),
        name: "USES".into(),
        identity_name: None,
        description: String::new(),
        all_properties: Default::default(),
        discovered_by: None,
        resolved_by: None,
        source_property: None,
        target_identity_field: None,
        reference_evidence: None,
        confidence: 1.0,
        justification: None,
        first_seen_snapshot_id: None,
        last_seen_snapshot_id: None,
        last_seen_at: None,
        sync_generation: None,
        valid_from: at(valid_from),
        valid_to: valid_to.map(at),
        cancelled_at: cancelled.map(at),
        cancellation_snapshot_id: None,
        cancellation_context: None,
        version: 1,
        is_latest: true,
        previous_version_uuid: None,
        deleted_at: deleted.map(at),
        deleted_by: None,
        deletion_reason: None,
        created_at: at(0),
    }
}

proptest! {
    /// Start-inclusive, end-exclusive, deletion-exclusive, and never effective once cancelled.
    #[test]
    fn edge_validity_is_a_half_open_interval(
        valid_from in 0i64..1000, valid_to in prop::option::of(0i64..1000),
        cancelled in prop::option::of(0i64..1000), deleted in prop::option::of(0i64..1000),
        as_of in 0i64..1000,
    ) {
        let edge = edge(valid_from, valid_to, cancelled, deleted);
        let expected = cancelled.is_none()
            && valid_from <= as_of
            && valid_to.is_none_or(|end| as_of < end)
            && deleted.is_none_or(|end| as_of < end);
        let at: DateTime<Utc> = Utc.timestamp_opt(as_of, 0).unwrap();
        prop_assert_eq!(edge.is_valid_at(at), expected);
        // `is_valid` is the clock-free view: open, undeleted, not cancelled.
        prop_assert_eq!(edge.is_valid(), cancelled.is_none() && valid_to.is_none() && deleted.is_none());
    }
}
