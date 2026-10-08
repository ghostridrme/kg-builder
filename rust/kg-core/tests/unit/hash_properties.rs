//! Property-based tests for hash framing and serde robustness.
//! Sampled collision checks detect regressions, not proof of collision freedom.

use indexmap::IndexMap;
use proptest::prelude::*;

use kg_core::identity::{compute_structural_hash, IdentityHash};
use kg_core::models::PropertyValue;

#[derive(Debug, Clone, PartialEq, Eq)]
struct IdentityInput {
    org: String,
    ns: String,
    entity_type: String,
    pks: Vec<(String, String)>,
}

impl IdentityInput {
    fn canonical(&self) -> (String, String, String, Vec<(String, String)>) {
        let mut pks = self.pks.clone();
        pks.sort();
        (
            self.org.clone(),
            self.ns.clone(),
            self.entity_type.clone(),
            pks,
        )
    }

    fn hash(&self) -> IdentityHash {
        IdentityHash::compute(&self.org, &self.ns, &self.entity_type, &self.pks)
    }
}

// Include delimiters, length-prefix lookalikes, and multibyte text.
fn sep_string() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[a-c0-9=,:\u{0}\n\u{e9}]{0,8}").unwrap()
}

fn identity_input() -> impl Strategy<Value = IdentityInput> {
    (
        sep_string(),
        sep_string(),
        sep_string(),
        proptest::collection::vec((sep_string(), sep_string()), 0..4),
    )
        .prop_map(|(org, ns, entity_type, pks)| IdentityInput {
            org,
            ns,
            entity_type,
            pks,
        })
}

proptest! {

    #[test]
    fn identity_hash_no_collisions(a in identity_input(), b in identity_input()) {
        if a.canonical() != b.canonical() {
            prop_assert_ne!(
                a.hash(), b.hash(),
                "distinct canonical inputs collided: {:?} vs {:?}", a, b
            );
        } else {
            prop_assert_eq!(a.hash(), b.hash());
        }
    }

    #[test]
    fn identity_hash_pk_order_invariant(mut input in identity_input()) {
        let h1 = input.hash();
        input.pks.reverse();
        prop_assert_eq!(h1, input.hash());
    }


    #[test]
    fn identity_hash_deserialize_never_panics(s in "\\PC*") {
        let json = serde_json::to_string(&s).unwrap();
        let _ = serde_json::from_str::<IdentityHash>(&json);
    }

    #[test]
    fn identity_hash_serde_round_trip(input in identity_input()) {
        let h = input.hash();
        let json = serde_json::to_string(&h).unwrap();
        let back: IdentityHash = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(h, back);
    }


    #[test]
    fn structural_hash_no_collisions(
        a in proptest::collection::btree_map(sep_string(), sep_string(), 0..4),
        b in proptest::collection::btree_map(sep_string(), sep_string(), 0..4),
    ) {
        let to_props = |m: &std::collections::BTreeMap<String, String>| {
            m.iter()
                .map(|(k, v)| (k.clone(), PropertyValue::String(v.clone())))
                .collect::<IndexMap<String, PropertyValue>>()
        };
        let h1 = compute_structural_hash(&to_props(&a), &[]);
        let h2 = compute_structural_hash(&to_props(&b), &[]);
        if a != b {
            prop_assert_ne!(h1, h2, "distinct property maps collided: {:?} vs {:?}", a, b);
        } else {
            prop_assert_eq!(h1, h2);
        }
    }

    #[test]
    fn structural_hash_list_boundaries(
        list_a in proptest::collection::vec(sep_string(), 0..4),
        list_b in proptest::collection::vec(sep_string(), 0..4),
    ) {
        let to_props = |l: &Vec<String>| {
            let mut m = IndexMap::new();
            m.insert("tags".to_string(), PropertyValue::StringList(l.clone()));
            m
        };
        let canon = |l: &Vec<String>| { let mut s = l.clone(); s.sort(); s };
        let h1 = compute_structural_hash(&to_props(&list_a), &[]);
        let h2 = compute_structural_hash(&to_props(&list_b), &[]);
        if canon(&list_a) != canon(&list_b) {
            prop_assert_ne!(h1, h2);
        } else {
            prop_assert_eq!(h1, h2);
        }
    }

    #[test]
    fn structural_hash_insertion_order_invariant(
        entries in proptest::collection::btree_map(sep_string(), sep_string(), 0..6),
    ) {
        let forward: IndexMap<String, PropertyValue> = entries
            .iter()
            .map(|(k, v)| (k.clone(), PropertyValue::String(v.clone())))
            .collect();
        let reverse: IndexMap<String, PropertyValue> = entries
            .iter()
            .rev()
            .map(|(k, v)| (k.clone(), PropertyValue::String(v.clone())))
            .collect();
        prop_assert_eq!(
            compute_structural_hash(&forward, &[]),
            compute_structural_hash(&reverse, &[])
        );
    }
}
