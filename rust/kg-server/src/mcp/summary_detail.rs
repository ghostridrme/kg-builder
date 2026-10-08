//! Exact summary ranges and revision-bound supporting-evidence pages.
use super::*;

pub(super) fn validate(
    start: Option<usize>,
    limit: Option<usize>,
    offset: Option<usize>,
    count: Option<usize>,
    revision: Option<&str>,
) -> Result<(), CallToolResult> {
    if start.unwrap_or(0) > 2_000_000
        || !(1..=2000).contains(&limit.unwrap_or(300))
        || offset.unwrap_or(0) > 100_000
        || !(1..=20).contains(&count.unwrap_or(5))
    {
        return Err(tool_error(
            "invalid_input",
            "Summary limit must be 1..2000 and evidence limit 1..20",
        ));
    }
    if (start.unwrap_or(0) > 0 || offset.unwrap_or(0) > 0) && revision.is_none() {
        return Err(tool_error(
            "invalid_input",
            "Continuation requires expected_revision from the first response",
        ));
    }
    if let Some(revision) = revision {
        parse_chain(revision)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn page(
    value: &mut Value,
    revision_field: &str,
    evidence_field: &str,
    start: Option<usize>,
    limit: Option<usize>,
    offset: Option<usize>,
    count: Option<usize>,
    expected: Option<&str>,
    evidence_paged: bool,
) -> Result<(), CallToolResult> {
    validate(start, limit, offset, count, expected)?;
    if expected.is_some_and(|r| {
        value[revision_field]
            .as_str()
            .and_then(|v| Uuid::parse_str(v).ok())
            != Uuid::parse_str(r).ok()
    }) {
        return Err(tool_error(
            "revision_changed",
            "Summary is changed or unavailable; restart from offset zero",
        ));
    }
    value["revision"] = value[revision_field].clone();
    let start = start.unwrap_or(0);
    let text = value["summary"].as_str();
    let total = text.map_or(0, |s| s.chars().count());
    if start > total {
        return Err(tool_error(
            "invalid_input",
            "Summary range starts beyond available text",
        ));
    }
    let summary = text.map(|s| projection::excerpt(s, start, limit.unwrap_or(300)));
    let end = start + summary.as_ref().map_or(0, |s| s.chars().count());
    value["summary"] = json!(summary);
    value["summary_start"] = json!(start);
    value["summary_end"] = json!(end);
    value["summary_total_characters"] = json!(total);
    value["summary_truncated"] = json!(end < total);
    value["next_summary_start"] = json!((end < total).then_some(end));
    value["offset_unit"] = json!("unicode_scalar");
    let offset = offset.unwrap_or(0);
    let count = count.unwrap_or(5);
    let all = value[evidence_field]
        .as_array()
        .cloned()
        .unwrap_or_default();
    if !evidence_paged && offset > all.len() {
        return Err(tool_error(
            "invalid_input",
            "Evidence offset exceeds available evidence",
        ));
    }
    let available = if evidence_paged {
        all.len()
    } else {
        all.len() - offset
    };
    let selected: Vec<_> = all
        .into_iter()
        .skip(if evidence_paged { 0 } else { offset })
        .take(count)
        .collect();
    let next = offset + selected.len();
    value[evidence_field] = json!(selected);
    value["evidence_offset"] = json!(offset);
    value["next_evidence_offset"] = json!((available > count).then_some(next));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_unicode_ranges_and_revision_guard() {
        let revision = Uuid::new_v4().to_string();
        let raw = json!({"summary":"ab🦀cd", "summary_revision":revision,"ids":[1,2,3]});
        let mut first = raw.clone();
        page(
            &mut first,
            "summary_revision",
            "ids",
            None,
            Some(3),
            None,
            Some(2),
            None,
            false,
        )
        .unwrap();
        assert_eq!(first["summary"], "ab🦀");
        assert_eq!(first["next_summary_start"], 3);
        assert_eq!(first["next_evidence_offset"], 2);
        let mut next = raw.clone();
        page(
            &mut next,
            "summary_revision",
            "ids",
            Some(3),
            Some(3),
            Some(2),
            Some(2),
            Some(&revision),
            false,
        )
        .unwrap();
        assert_eq!(next["summary"], "cd");
        assert_eq!(next["ids"], json!([3]));
        assert!(page(
            &mut raw.clone(),
            "summary_revision",
            "ids",
            Some(3),
            None,
            None,
            None,
            None,
            false
        )
        .is_err());
        assert!(page(
            &mut raw.clone(),
            "summary_revision",
            "ids",
            None,
            None,
            None,
            None,
            Some(&Uuid::new_v4().to_string()),
            false
        )
        .is_err());
    }
}
