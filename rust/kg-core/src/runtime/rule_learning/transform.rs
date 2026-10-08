//! Identifier transforms supported by learned-rule proposals.
//!
//! A transform maps a source value to the target key value when the two are
//! related by a known, safe rewrite (a case fold, a fixed prefix/suffix strip).
//! Only this fixed, typed set is ever executed: model-generated code, regex or
//! Cypher is never run. An unsupported rewrite yields [`None`] and the caller
//! keeps the rule uncertain with a reason. A supported transform is still only
//! trusted after it is validated on real examples: it must complete the target
//! key on every positive and never collide two distinct sources onto one target.
use serde::{Deserialize, Serialize};

/// The fixed, safe set of supported value transforms. Each has typed string
/// input and output and bounded arguments; none executes arbitrary logic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ValueTransform {
    /// The value already equals the target key.
    Identity,
    /// ASCII lowercase fold.
    Lowercase,
    /// Remove a fixed leading string when present.
    StripPrefix { prefix: String },
    /// Remove a fixed trailing string when present.
    StripSuffix { suffix: String },
    /// Trim surrounding ASCII whitespace.
    TrimWhitespace,
}

/// Longest argument a transform may carry.
pub const MAX_TRANSFORM_ARG: usize = 128;

impl ValueTransform {
    /// Parse a model- or human-proposed transform into the supported set.
    /// An unknown name, or an over-long argument, is unsupported (`None`).
    pub fn parse_supported(name: &str, arg: Option<&str>) -> Option<Self> {
        if arg.is_some_and(|a| a.len() > MAX_TRANSFORM_ARG) {
            return None;
        }
        match (name, arg) {
            ("identity", _) => Some(Self::Identity),
            ("lowercase", _) => Some(Self::Lowercase),
            ("trim_whitespace", _) => Some(Self::TrimWhitespace),
            ("strip_prefix", Some(p)) if !p.is_empty() => {
                Some(Self::StripPrefix { prefix: p.into() })
            }
            ("strip_suffix", Some(s)) if !s.is_empty() => {
                Some(Self::StripSuffix { suffix: s.into() })
            }
            _ => None,
        }
    }

    /// Apply the transform. Pure and total; never fails or executes input.
    pub fn apply(&self, input: &str) -> String {
        match self {
            Self::Identity => input.to_string(),
            Self::Lowercase => input.to_ascii_lowercase(),
            Self::TrimWhitespace => input.trim().to_string(),
            Self::StripPrefix { prefix } => input
                .strip_prefix(prefix.as_str())
                .unwrap_or(input)
                .to_string(),
            Self::StripSuffix { suffix } => input
                .strip_suffix(suffix.as_str())
                .unwrap_or(input)
                .to_string(),
        }
    }
}

/// One labeled example for transform validation: a source value and the target
/// key value it should produce.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformExample {
    pub input: String,
    pub expected_key: String,
}

/// The result of validating a transform against examples.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransformValidation {
    /// Examples where the transform's output completed the expected key.
    pub completed: usize,
    /// Examples where it produced the wrong key (does not complete).
    pub misses: usize,
    /// Distinct inputs that collapsed onto one output mapped to different
    /// expected keys — the transform is not identity-preserving here.
    pub collisions: usize,
}

impl TransformValidation {
    /// A transform is trustworthy only with no misses, no collisions and at
    /// least one completed example.
    pub fn is_valid(&self) -> bool {
        self.completed > 0 && self.misses == 0 && self.collisions == 0
    }
}

/// Validate a transform on examples: it must complete every expected key and
/// never map two inputs bound for different keys onto one output.
pub fn validate_transform(
    transform: &ValueTransform,
    examples: &[TransformExample],
) -> TransformValidation {
    use std::collections::HashMap;
    let mut result = TransformValidation::default();
    // output -> the expected key it should map to; a second, different key is a
    // collision.
    let mut seen: HashMap<String, String> = HashMap::new();
    for example in examples {
        let output = transform.apply(&example.input);
        if output == example.expected_key {
            result.completed += 1;
        } else {
            result.misses += 1;
        }
        if let Some(prev) = seen.get(&output) {
            if prev != &example.expected_key {
                result.collisions += 1;
            }
        } else {
            seen.insert(output, example.expected_key.clone());
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ex(input: &str, key: &str) -> TransformExample {
        TransformExample {
            input: input.into(),
            expected_key: key.into(),
        }
    }

    #[test]
    fn only_the_safe_set_is_supported() {
        assert_eq!(
            ValueTransform::parse_supported("lowercase", None),
            Some(ValueTransform::Lowercase)
        );
        assert_eq!(
            ValueTransform::parse_supported("strip_prefix", Some("arn:")),
            Some(ValueTransform::StripPrefix {
                prefix: "arn:".into()
            })
        );
        // Unsupported: regex, cypher, code, empty arg, over-long arg.
        assert_eq!(ValueTransform::parse_supported("regex", Some(".*")), None);
        assert_eq!(
            ValueTransform::parse_supported("cypher", Some("MATCH")),
            None
        );
        assert_eq!(ValueTransform::parse_supported("strip_prefix", None), None);
        assert_eq!(
            ValueTransform::parse_supported("strip_prefix", Some("")),
            None
        );
        let long = "x".repeat(MAX_TRANSFORM_ARG + 1);
        assert_eq!(
            ValueTransform::parse_supported("strip_prefix", Some(&long)),
            None
        );
    }

    #[test]
    fn a_clean_transform_completes_every_key() {
        let t = ValueTransform::StripPrefix {
            prefix: "arn:aws:".into(),
        };
        let examples = vec![
            ex("arn:aws:i-1", "i-1"),
            ex("arn:aws:i-2", "i-2"),
            ex("arn:aws:i-3", "i-3"),
        ];
        let v = validate_transform(&t, &examples);
        assert_eq!(v.completed, 3);
        assert_eq!(v.misses, 0);
        assert_eq!(v.collisions, 0);
        assert!(v.is_valid());
    }

    #[test]
    fn a_miss_or_collision_invalidates_the_transform() {
        let t = ValueTransform::Lowercase;
        // "A" and "a" both fold to "a" but are bound to different keys: collision.
        let examples = vec![ex("A", "keyA"), ex("a", "keya")];
        let v = validate_transform(&t, &examples);
        assert!(v.collisions >= 1 || v.misses >= 1);
        assert!(!v.is_valid());

        // A transform that does not produce the expected key is a miss.
        let miss = validate_transform(&ValueTransform::Identity, &[ex("arn:x", "x")]);
        assert_eq!(miss.misses, 1);
        assert!(!miss.is_valid());
    }
}
