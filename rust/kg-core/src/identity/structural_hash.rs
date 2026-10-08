use indexmap::IndexMap;
use xxhash_rust::xxh3::xxh3_64;

use crate::identity::identity_hash::push_framed;
use crate::models::property_value::PropertyValue;

/// Non-cryptographic xxHash3 digest after explicit exclusions; callers choose versioning rules.
/// Sorts keys and typed lists (retaining duplicates), normalizes signed zero,
/// and recursively sorts JSON object keys while preserving JSON array order.
/// Invalid JSON/blobs retain raw strings. Type tags and byte framing separate values.
/// Equal hashes do not prove equal properties.
pub fn compute_structural_hash(
    properties: &IndexMap<String, PropertyValue>,
    exclude_from_hash: &[String],
) -> u64 {
    let mut buf = String::with_capacity(1024);

    let mut keys: Vec<&String> = properties
        .keys()
        .filter(|k| !exclude_from_hash.contains(k))
        .collect();
    keys.sort();

    for key in keys {
        let Some(value) = properties.get(key) else {
            continue;
        };

        push_framed(&mut buf, key);
        write_canonical_value(&mut buf, value);
    }

    xxh3_64(buf.as_bytes())
}

fn write_canonical_value(buf: &mut String, value: &PropertyValue) {
    match value {
        PropertyValue::String(s) => {
            buf.push_str("s:");
            push_framed(buf, s);
        }
        PropertyValue::Integer(i) => {
            buf.push_str("i:");
            push_framed(buf, &i.to_string());
        }
        PropertyValue::Float(f) => {
            buf.push_str("f:");
            // Signed zero must not create a new version.
            let f = if *f == 0.0 { 0.0 } else { *f };
            push_framed(buf, &format!("{f:.17e}"));
        }
        PropertyValue::Bool(b) => {
            buf.push_str("b:");
            push_framed(buf, if *b { "true" } else { "false" });
        }
        PropertyValue::Timestamp(ts) => {
            buf.push_str("ts:");
            push_framed(buf, &ts.to_rfc3339());
        }
        PropertyValue::Duration(d) => {
            buf.push_str("d:");
            push_framed(buf, &d.to_string());
        }
        PropertyValue::UuidRef(u) => {
            buf.push_str("u:");
            push_framed(buf, &u.to_string());
        }
        PropertyValue::StringList(list) => {
            buf.push_str("sl:");
            let mut sorted = list.clone();
            sorted.sort();
            for s in &sorted {
                push_framed(buf, s);
            }
        }
        PropertyValue::IntegerList(list) => {
            buf.push_str("il:");
            let mut sorted = list.clone();
            sorted.sort();
            for v in &sorted {
                push_framed(buf, &v.to_string());
            }
        }
        PropertyValue::FloatList(list) => {
            buf.push_str("fl:");
            let mut sorted = list.clone();
            sorted.sort_by(|a, b| a.total_cmp(b));
            for v in &sorted {
                let v = if *v == 0.0 { 0.0 } else { *v };
                push_framed(buf, &format!("{v:.17e}"));
            }
        }
        PropertyValue::Json(raw) => {
            buf.push_str("j:");
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(raw) {
                let sorted = sort_json_keys(&parsed);
                push_framed(buf, &sorted.to_string());
            } else {
                push_framed(buf, raw);
            }
        }
        PropertyValue::Blob(b) => {
            buf.push_str("bl:");
            push_framed(buf, b);
        }
        PropertyValue::Null => {
            buf.push_str("n:");
        }
    }
}

fn sort_json_keys(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut sorted: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            for key in keys {
                sorted.insert(key.clone(), sort_json_keys(&map[key]));
            }
            serde_json::Value::Object(sorted)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(sort_json_keys).collect())
        }
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn property_types_and_list_multiplicity_affect_versions() {
        let hash = |value| compute_structural_hash(&IndexMap::from([("key".into(), value)]), &[]);
        let values = [
            PropertyValue::String("1".into()),
            PropertyValue::Integer(1),
            PropertyValue::Float(1.0),
            PropertyValue::Bool(true),
            PropertyValue::Duration(1),
            PropertyValue::Null,
            PropertyValue::StringList(vec!["1".into()]),
            PropertyValue::StringList(vec!["1".into(), "1".into()]),
        ];
        for (index, value) in values.iter().enumerate() {
            for other in &values[index + 1..] {
                assert_ne!(
                    hash(value.clone()),
                    hash(other.clone()),
                    "{value:?} / {other:?}"
                );
            }
        }
    }

    #[test]
    fn signed_zero_is_equal_for_scalars_and_lists() {
        for (left, right) in [
            (PropertyValue::Float(-0.0), PropertyValue::Float(0.0)),
            (
                PropertyValue::FloatList(vec![-0.0, 1.0]),
                PropertyValue::FloatList(vec![1.0, 0.0]),
            ),
        ] {
            assert_eq!(
                compute_structural_hash(&IndexMap::from([("value".into(), left)]), &[]),
                compute_structural_hash(&IndexMap::from([("value".into(), right)]), &[]),
            );
        }
    }

    #[test]
    fn nested_json_objects_are_canonical_but_arrays_keep_order() {
        let hash = |raw: &str| {
            compute_structural_hash(
                &IndexMap::from([("json".into(), PropertyValue::Json(raw.into()))]),
                &[],
            )
        };
        assert_eq!(
            hash(r#"{"outer":[{"b":2,"a":1},3]}"#),
            hash(r#"{"outer":[{"a":1,"b":2},3]}"#)
        );
        assert_ne!(hash(r#"{"outer":[1,2]}"#), hash(r#"{"outer":[2,1]}"#));
    }

    #[test]
    fn list_order_does_not_affect_hash() {
        let mut props1 = IndexMap::new();
        props1.insert(
            "images".into(),
            PropertyValue::StringList(vec!["b".into(), "a".into()]),
        );

        let mut props2 = IndexMap::new();
        props2.insert(
            "images".into(),
            PropertyValue::StringList(vec!["a".into(), "b".into()]),
        );

        let h1 = compute_structural_hash(&props1, &[]);
        let h2 = compute_structural_hash(&props2, &[]);
        assert_eq!(h1, h2, "Sorted lists should produce the same hash");
    }

    #[test]
    fn json_key_order_does_not_affect_hash() {
        let mut props1 = IndexMap::new();
        props1.insert(
            "config".into(),
            PropertyValue::Json(r#"{"b":2,"a":1}"#.into()),
        );

        let mut props2 = IndexMap::new();
        props2.insert(
            "config".into(),
            PropertyValue::Json(r#"{"a":1,"b":2}"#.into()),
        );

        let h1 = compute_structural_hash(&props1, &[]);
        let h2 = compute_structural_hash(&props2, &[]);
        assert_eq!(h1, h2, "Key-sorted JSON should produce the same hash");
    }

    #[test]
    fn exclude_from_hash_works() {
        let mut props = IndexMap::new();
        props.insert("name".into(), PropertyValue::String("api".into()));
        props.insert("status".into(), PropertyValue::String("Running".into()));

        let h1 = compute_structural_hash(&props, &[]);
        let h2 = compute_structural_hash(&props, &["status".into()]);
        assert_ne!(h1, h2, "Excluding a property should change the hash");
    }

    #[test]
    fn different_values_different_hash() {
        let mut props1 = IndexMap::new();
        props1.insert("replicas".into(), PropertyValue::Integer(3));

        let mut props2 = IndexMap::new();
        props2.insert("replicas".into(), PropertyValue::Integer(5));

        let h1 = compute_structural_hash(&props1, &[]);
        let h2 = compute_structural_hash(&props2, &[]);
        assert_ne!(h1, h2);
    }

    #[test]
    fn record_separator_injection_does_not_collide() {
        let mut props1 = IndexMap::new();
        props1.insert("a".into(), PropertyValue::String("x\n1:b=s:1:y".into()));

        let mut props2 = IndexMap::new();
        props2.insert("a".into(), PropertyValue::String("x".into()));
        props2.insert("b".into(), PropertyValue::String("y".into()));

        let h1 = compute_structural_hash(&props1, &[]);
        let h2 = compute_structural_hash(&props2, &[]);
        assert_ne!(h1, h2, "newline in value must not split records");
    }

    #[test]
    fn list_element_separator_injection_does_not_collide() {
        let mut props1 = IndexMap::new();
        props1.insert("tags".into(), PropertyValue::StringList(vec!["a,b".into()]));

        let mut props2 = IndexMap::new();
        props2.insert(
            "tags".into(),
            PropertyValue::StringList(vec!["a".into(), "b".into()]),
        );

        let h1 = compute_structural_hash(&props1, &[]);
        let h2 = compute_structural_hash(&props2, &[]);
        assert_ne!(h1, h2, "comma in list element must not split elements");
    }
}
