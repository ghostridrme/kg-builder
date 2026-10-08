//! Bounded entity text for the relevance model. Shares the embedding field
//! policy and size limits from `kg_core::embedding` so ranking and
//! embedding see the same descriptive fields.
use kg_core::embedding::{DESCRIPTIVE_FIELDS, MAX_FIELD_CHARS, MAX_TEXT_CHARS};
use serde_json::Value;

/// `name`, `type`, then each present descriptive field as `label: value`.
/// Tags, URLs, nested objects and unlisted properties never reach the model.
pub(crate) fn entity_text(name: &str, entity_type: &str, properties: &Value) -> String {
    let mut text = String::new();
    let mut remaining = MAX_TEXT_CHARS;
    append_field(&mut text, &mut remaining, "name", name);
    append_field(&mut text, &mut remaining, "type", entity_type);
    for field in DESCRIPTIVE_FIELDS {
        // Stored user properties have a prop_ prefix. A present stored value is
        // authoritative even when it is empty or has an unsupported type.
        let value = properties
            .get(format!("prop_{field}"))
            .or_else(|| properties.get(*field))
            .and_then(Value::as_str);
        if let Some(value) = value {
            append_field(&mut text, &mut remaining, field, value);
        }
    }
    text
}

fn append_field(text: &mut String, remaining: &mut usize, label: &str, value: &str) {
    // Inspect only the allowed prefix, including for blank fields, rather than
    // scanning an arbitrarily large value just to trim it.
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

/// Only hydrated, coverage-checked derived evidence enters relevance ranking.
pub(crate) fn hit_text(hit: &kg_core::search::SearchHit) -> String {
    let mut text = entity_text(&hit.name, &hit.entity_type, &hit.properties);
    if let Some(summary) = &hit.derived_summary {
        let mut remaining = MAX_TEXT_CHARS.saturating_sub(text.chars().count());
        append_field(
            &mut text,
            &mut remaining,
            "accepted evidence",
            &summary.text,
        );
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn descriptive_context_survives_without_a_summary() {
        let properties = json!({
            "prop_path": "src/retry.rs",
            "prop_description": "Retries transient failures with exponential backoff",
            "prop_language": "Rust",
            "prop_password": "SECRET",
            "prop_tags": {"token": "SECRET"},
            "prop_url": "https://user:SECRET@example.com",
            "embedding": [1, 2],
            "last_seen": "VOLATILE"
        });
        let text = entity_text("retry", "Function", &properties);
        assert!(text.contains("path: src/retry.rs"));
        assert!(text.contains("description: Retries transient failures"));
        assert!(!text.contains("SECRET"));
        assert!(!text.contains("VOLATILE"));
        assert!(!text.contains("embedding"));
        assert_eq!(text, entity_text("retry", "Function", &properties));
    }

    #[test]
    fn stored_fields_take_precedence_and_nested_values_are_omitted() {
        let text = entity_text(
            "checkout",
            "Service",
            &json!({
                "summary": "stale", "prop_summary": "current",
                "path": "stale-path", "prop_path": {"password": "SECRET"},
                "description": "plain properties also work"
            }),
        );
        assert!(text.contains("summary: current"));
        assert!(text.contains("description: plain properties also work"));
        assert!(!text.contains("stale"));
        assert!(!text.contains("SECRET"));
    }

    #[test]
    fn unicode_fields_and_total_text_are_bounded() {
        let large = "🌍".repeat(10_000);
        let properties: serde_json::Map<String, Value> = DESCRIPTIVE_FIELDS
            .iter()
            .map(|field| (format!("prop_{field}"), json!(large)))
            .collect();
        let text = entity_text(&large, &large, &Value::Object(properties));
        assert_eq!(text.chars().count(), MAX_TEXT_CHARS);
        assert!(text.starts_with(&format!("name: {}\ntype: ", "🌍".repeat(512))));
        assert!(text.contains("description: "));
        assert!(text.contains("summary: "));
    }
}
