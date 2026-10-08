//! Trusted relationship keys; fact wording is never a lineage identifier.

use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::identity_hash::push_framed;
use crate::models::{PropertyValue, RelationshipOrigin};

/// Ownership boundary for trusted relationship identities.
#[derive(Debug, Clone, Copy)]
pub struct RelationshipIdentityScope<'a> {
    pub org_id: &'a str,
    pub namespace: &'a str,
    pub source: &'a str,
    pub origin: RelationshipOrigin,
}

/// Hash an ordered endpoint pair, canonical name and explicitly selected typed keys.
/// The caller decides whether these keys establish identity; an empty key list
/// identifies a trusted relationship by endpoints and name alone.
pub fn relationship_identity_hash(
    scope: RelationshipIdentityScope<'_>,
    source_chain_id: Uuid,
    target_chain_id: Uuid,
    name: &str,
    reference_role: Option<&str>,
    keys: &[(String, PropertyValue)],
) -> Result<String, String> {
    if [scope.org_id, scope.namespace, scope.source]
        .iter()
        .any(|value| value.trim().is_empty())
        || name.trim().is_empty()
        || source_chain_id.is_nil()
        || target_chain_id.is_nil()
    {
        return Err("relationship identity requires scope, endpoints and name".into());
    }
    let mut input = String::from("relationship-identity-v1:");
    for field in [
        scope.org_id,
        scope.namespace,
        scope.source,
        match scope.origin {
            RelationshipOrigin::Declared => "declared",
            RelationshipOrigin::Reference => "reference",
            RelationshipOrigin::Fact => "fact",
        },
        &source_chain_id.to_string(),
        &target_chain_id.to_string(),
        name,
    ] {
        push_framed(&mut input, field);
    }
    append_keys(&mut input, reference_role, keys)?;
    Ok(format!("{:x}", Sha256::digest(input.as_bytes())))
}

/// Scope a single-target slot by source, name and trusted qualifiers, never target.
pub fn relationship_cardinality_key(
    org_id: &str,
    source_chain_id: Uuid,
    name: &str,
    reference_role: Option<&str>,
    keys: &[(String, PropertyValue)],
) -> Result<String, String> {
    if org_id.trim().is_empty() || name.trim().is_empty() || source_chain_id.is_nil() {
        return Err("relationship cardinality requires scope, source and name".into());
    }
    let mut input = String::from("relationship-cardinality-v1:");
    for field in [org_id, &source_chain_id.to_string(), name] {
        push_framed(&mut input, field);
    }
    append_keys(&mut input, reference_role, keys)?;
    Ok(format!("{:x}", Sha256::digest(input.as_bytes())))
}

fn append_keys(
    input: &mut String,
    reference_role: Option<&str>,
    keys: &[(String, PropertyValue)],
) -> Result<(), String> {
    push_framed(
        input,
        if reference_role.is_some() {
            "reference"
        } else {
            "declared-keys"
        },
    );
    if let Some(role) = reference_role {
        if role.trim().is_empty() {
            return Err("reference identity requires a nonblank role".into());
        }
        push_framed(input, role);
    }
    let mut keys: Vec<_> = keys.iter().collect();
    keys.sort_by(|a, b| a.0.cmp(&b.0));
    let mut previous = None;
    for (key, value) in keys {
        if key.trim().is_empty() || previous == Some(key) || value.as_identity_key().is_none() {
            return Err(
                "relationship identity requires unique keys and finite scalar values".into(),
            );
        }
        previous = Some(key);
        let normalized;
        let value = if matches!(value, PropertyValue::Float(v) if *v == 0.0) {
            normalized = PropertyValue::Float(0.0);
            &normalized
        } else {
            value
        };
        push_framed(input, key);
        push_framed(
            input,
            &serde_json::to_string(value).map_err(|_| "invalid relationship identity value")?,
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn scope(org_id: &str) -> RelationshipIdentityScope<'_> {
        RelationshipIdentityScope {
            org_id,
            namespace: "prod",
            source: "aws",
            origin: RelationshipOrigin::Declared,
        }
    }
    #[test]
    fn trusted_identity_isolates_producer_namespace_and_origin() {
        let hash = |scope| {
            relationship_identity_hash(
                scope,
                Uuid::from_u128(1),
                Uuid::from_u128(2),
                "USES",
                None,
                &[],
            )
            .unwrap()
        };
        let original = scope("org");
        let baseline = hash(original);
        for other in [
            RelationshipIdentityScope {
                namespace: "dev",
                ..original
            },
            RelationshipIdentityScope {
                source: "github",
                ..original
            },
            RelationshipIdentityScope {
                origin: RelationshipOrigin::Reference,
                ..original
            },
            RelationshipIdentityScope {
                origin: RelationshipOrigin::Fact,
                ..original
            },
        ] {
            assert_ne!(baseline, hash(other));
        }
        assert!(relationship_identity_hash(
            RelationshipIdentityScope {
                source: " ",
                ..original
            },
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            "USES",
            None,
            &[]
        )
        .is_err());
    }
    #[test]
    fn cardinality_slots_preserve_qualifiers_and_have_a_separate_domain() {
        let source = Uuid::from_u128(1);
        let key = |org, name, keys: &[(String, PropertyValue)]| {
            relationship_cardinality_key(org, source, name, None, keys).unwrap()
        };
        let a = vec![("deployment".into(), PropertyValue::String("a".into()))];
        let b = vec![("deployment".into(), PropertyValue::String("b".into()))];
        assert_ne!(key("o", "DEPLOYED_IN", &a), key("o", "DEPLOYED_IN", &b));
        assert_ne!(key("o", "DEPLOYED_IN", &a), key("other", "DEPLOYED_IN", &a));
        assert_ne!(
            key("o", "DEPLOYED_IN", &a),
            relationship_identity_hash(scope("o"), source, source, "DEPLOYED_IN", None, &a)
                .unwrap()
        );
        assert!(relationship_cardinality_key(
            "o",
            source,
            "USES",
            None,
            &[("k".into(), PropertyValue::Null)]
        )
        .is_err());
    }

    #[test]
    fn identity_preserves_scope_direction_name_and_scalar_types() {
        let a = Uuid::from_u128(1);
        let b = Uuid::from_u128(2);
        let hash = |org, name, source, target, keys: &[(String, PropertyValue)]| {
            relationship_identity_hash(scope(org), source, target, name, None, keys).unwrap()
        };
        let keys = vec![("port".into(), PropertyValue::Integer(5432))];
        let baseline = hash("o", "USES", a, b, &keys);
        assert_ne!(baseline, hash("other", "USES", a, b, &keys));
        assert_ne!(baseline, hash("o", "OWNS", a, b, &keys));
        assert_ne!(baseline, hash("o", "USES", b, a, &keys));
        assert_ne!(
            baseline,
            hash(
                "o",
                "USES",
                a,
                b,
                &[("port".into(), PropertyValue::String("5432".into()))]
            )
        );
        assert!(relationship_identity_hash(
            scope("o"),
            a,
            b,
            "USES",
            None,
            &[("port".into(), PropertyValue::Null)]
        )
        .is_err());
        assert!(relationship_identity_hash(
            scope("o"),
            a,
            b,
            "USES",
            None,
            &[keys[0].clone(), keys[0].clone()]
        )
        .is_err());
        assert_ne!(
            relationship_identity_hash(scope("o"), a, b, "USES", Some("port"), &[]).unwrap(),
            hash(
                "o",
                "USES",
                a,
                b,
                &[(
                    "source_property".into(),
                    PropertyValue::String("port".into())
                )]
            )
        );
        let x = ("a".into(), PropertyValue::Bool(true));
        let y = ("b".into(), PropertyValue::Integer(2));
        assert_eq!(
            hash("o", "USES", a, b, &[x.clone(), y.clone()]),
            hash("o", "USES", a, b, &[y, x])
        );
    }
}
