use serde::{Deserialize, Serialize};
use std::fmt;
use xxhash_rust::xxh3::xxh3_128;

/// Non-cryptographic scoped identity digest or opaque keyless-observation token.
/// Length framing prevents field-boundary ambiguity; source provenance is excluded.
/// Digest collisions remain possible; this is not an authorization token.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IdentityHash(#[serde(with = "hex_bytes")] pub [u8; 16]);

/// Append a field as `{byte_len}:{bytes}`; unambiguous framing regardless of content.
pub(crate) fn push_framed(buf: &mut String, field: &str) {
    buf.push_str(&field.len().to_string());
    buf.push(':');
    buf.push_str(field);
}

impl IdentityHash {
    /// A stable token for one keyless observation, never evidence of shared identity.
    pub fn for_observation(
        org_id: &str,
        namespace: &str,
        entity_type: &str,
        snapshot_uuid: uuid::Uuid,
        ordinal: usize,
    ) -> Self {
        let mut input = String::from("keyless-observation:");
        for field in [
            org_id,
            namespace,
            entity_type,
            &snapshot_uuid.to_string(),
            &ordinal.to_string(),
        ] {
            push_framed(&mut input, field);
        }
        Self(xxh3_128(input.as_bytes()).to_be_bytes())
    }

    /// Hash string-valued identifying keys. Scalar types are encoded explicitly.
    pub fn compute(
        org_id: &str,
        namespace: &str,
        entity_type: &str,
        pk_values: &[(impl AsRef<str>, impl AsRef<str>)],
    ) -> Self {
        let encoded: Vec<_> = pk_values
            .iter()
            .map(|(key, value)| {
                (
                    key.as_ref().to_owned(),
                    serde_json::to_string(&crate::models::PropertyValue::String(
                        value.as_ref().to_owned(),
                    ))
                    .expect("string serialization is infallible"),
                )
            })
            .collect();
        Self::compute_encoded(org_id, namespace, entity_type, &encoded)
    }

    /// Primary and alternative keys share this encoding; display text is not identity.
    pub fn compute_values(
        org_id: &str,
        namespace: &str,
        entity_type: &str,
        pk_values: &[(String, crate::models::PropertyValue)],
    ) -> Result<Self, String> {
        let mut encoded = Vec::with_capacity(pk_values.len());
        for (key, value) in pk_values {
            if value.as_identity_key().is_none() {
                return Err("identity requires finite scalar values".into());
            }
            let normalized;
            let value = if matches!(value, crate::models::PropertyValue::Float(v) if *v == 0.0) {
                normalized = crate::models::PropertyValue::Float(0.0);
                &normalized
            } else {
                value
            };
            encoded.push((
                key.clone(),
                serde_json::to_string(value).map_err(|_| "invalid identity value")?,
            ));
        }
        Ok(Self::compute_encoded(
            org_id,
            namespace,
            entity_type,
            &encoded,
        ))
    }

    fn compute_encoded(
        org_id: &str,
        namespace: &str,
        entity_type: &str,
        pk_values: &[(String, String)],
    ) -> Self {
        let mut sorted_pks: Vec<_> = pk_values.iter().collect();
        sorted_pks.sort_unstable();
        let mut input = String::with_capacity(256);
        push_framed(&mut input, org_id);
        push_framed(&mut input, namespace);
        push_framed(&mut input, entity_type);
        for (key, value) in sorted_pks {
            push_framed(&mut input, key);
            push_framed(&mut input, value);
        }
        Self(xxh3_128(input.as_bytes()).to_be_bytes())
    }

    pub fn to_hex(&self) -> String {
        hex::encode(&self.0)
    }
}

impl fmt::Debug for IdentityHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IdentityHash({})", self.to_hex())
    }
}

impl fmt::Display for IdentityHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// Serialize a digest as a hex string and reject malformed or wrong-length input.
mod hex_bytes {
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &[u8; 16], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&super::hex::encode(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<[u8; 16], D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let bytes = super::hex::decode(&s).map_err(serde::de::Error::custom)?;
        let arr: [u8; 16] = bytes
            .try_into()
            .map_err(|_| serde::de::Error::custom("expected 16 bytes"))?;
        Ok(arr)
    }
}

mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        let mut s = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            s.push_str(&format!("{b:02x}"));
        }
        s
    }

    /// Reject malformed hex, including non-ASCII input, without panicking.
    pub fn decode(s: &str) -> Result<Vec<u8>, String> {
        if !s.len().is_multiple_of(2) {
            return Err("odd length hex string".into());
        }
        if !s.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err("non-hex character in hex string".into());
        }
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| e.to_string()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyless_tokens_are_scoped_stable_and_separate_from_source_keys() {
        let snapshot = uuid::Uuid::new_v4();
        let token = IdentityHash::for_observation("org", "prod", "Service", snapshot, 0);
        assert_eq!(
            token,
            IdentityHash::for_observation("org", "prod", "Service", snapshot, 0)
        );
        for other in [
            IdentityHash::for_observation("other", "prod", "Service", snapshot, 0),
            IdentityHash::for_observation("org", "dev", "Service", snapshot, 0),
            IdentityHash::for_observation("org", "prod", "Database", snapshot, 0),
            IdentityHash::for_observation("org", "prod", "Service", snapshot, 1),
            IdentityHash::for_observation("org", "prod", "Service", uuid::Uuid::new_v4(), 0),
            IdentityHash::compute("org", "prod", "Service", &[("name", "api")]),
        ] {
            assert_ne!(token, other);
        }
    }

    #[test]
    fn deterministic_same_input() {
        let h1 = IdentityHash::compute(
            "org-1",
            "production",
            "Deployment",
            &[("name", "payment-api"), ("namespace", "default")],
        );
        let h2 = IdentityHash::compute(
            "org-1",
            "production",
            "Deployment",
            &[("name", "payment-api"), ("namespace", "default")],
        );
        assert_eq!(h1, h2);
    }

    #[test]
    fn duplicate_pk_keys_order_irrelevant() {
        let h1 = IdentityHash::compute("o", "n", "T", &[("k", "1"), ("k", "2")]);
        let h2 = IdentityHash::compute("o", "n", "T", &[("k", "2"), ("k", "1")]);
        assert_eq!(
            h1, h2,
            "duplicate PK keys must not make hash order-dependent"
        );
    }

    #[test]
    fn separator_injection_does_not_collide() {
        let h1 = IdentityHash::compute("o", "n", "T", &[("a=b", "c")]);
        let h2 = IdentityHash::compute("o", "n", "T", &[("a", "b=c")]);
        assert_ne!(h1, h2, "'=' in key/value must not shift field boundaries");

        let h3 = IdentityHash::compute("o", "n", "T", &[("a", "b\0c=d")]);
        let h4 = IdentityHash::compute("o", "n", "T", &[("a", "b"), ("c", "d")]);
        assert_ne!(h3, h4, "NUL in value must not split into extra PK pairs");

        let h5 = IdentityHash::compute("ab", "c", "T", &[("k", "v")]);
        let h6 = IdentityHash::compute("a", "bc", "T", &[("k", "v")]);
        assert_ne!(h5, h6, "field content must not bleed across segments");
    }

    #[test]
    fn hex_display() {
        let h = IdentityHash::compute("o", "n", "T", &[("k", "v")]);
        let hex = h.to_hex();
        assert_eq!(hex.len(), 32);
    }
    #[test]
    fn scalar_types_cannot_collide_and_string_api_matches_typed_keys() {
        use crate::models::PropertyValue;
        let values = [
            PropertyValue::String("123".into()),
            PropertyValue::Integer(123),
            PropertyValue::Float(123.0),
            PropertyValue::Bool(true),
            PropertyValue::String("true".into()),
        ];
        let hashes: std::collections::HashSet<_> = values
            .iter()
            .map(|v| {
                IdentityHash::compute_values("org", "ns", "Type", &[("id".into(), v.clone())])
                    .unwrap()
            })
            .collect();
        assert_eq!(hashes.len(), values.len());
        assert_eq!(
            IdentityHash::compute("org", "ns", "Type", &[("id", "123")]),
            IdentityHash::compute_values("org", "ns", "Type", &[("id".into(), values[0].clone())])
                .unwrap()
        );
    }
    #[test]
    fn signed_zero_has_one_identity_encoding() {
        let hash = |value| {
            IdentityHash::compute_values(
                "org",
                "ns",
                "Type",
                &[("id".into(), crate::models::PropertyValue::Float(value))],
            )
            .unwrap()
        };
        assert_eq!(hash(-0.0), hash(0.0));
    }
}
