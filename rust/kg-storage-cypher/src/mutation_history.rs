//! Existing versions retain identity and source-time history. New rows receive
//! their initial fields; subsequent patches may observe or shorten, never rewind.
fn unchanged(record: &str, props: &str, fields: &[&str]) -> String {
    let keys = fields
        .iter()
        .map(|key| format!("'{key}'"))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "all(k IN [{keys}] WHERE NOT k IN keys({props}) OR {props}[k]={record}[k] OR ({props}[k] IS NULL AND {record}[k] IS NULL))"
    )
}
fn same_time(record: &str, props: &str, fields: &[&str]) -> String {
    let keys = fields
        .iter()
        .map(|key| format!("'{key}'"))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "all(k IN [{keys}] WHERE NOT k IN keys({props}) OR datetime({props}[k])=datetime({record}[k]) OR ({props}[k] IS NULL AND {record}[k] IS NULL))"
    )
}
fn monotone(record: &str, props: &str, fields: &[&str], increasing: bool) -> String {
    let keys = fields
        .iter()
        .map(|key| format!("'{key}'"))
        .collect::<Vec<_>>()
        .join(",");
    let order = if increasing { ">=" } else { "<=" };
    let interval = if increasing {
        String::new()
    } else {
        format!(
            " AND ({props}[k] IS NULL OR {record}.valid_from IS NULL OR datetime({props}[k])>=datetime({record}.valid_from))"
        )
    };
    format!(
        "all(k IN [{keys}] WHERE NOT k IN keys({props}) OR (({record}[k] IS NULL OR ({props}[k] IS NOT NULL AND datetime({props}[k]){order}datetime({record}[k]))){interval}))"
    )
}
pub(super) fn entity(props: &str) -> String {
    [
        unchanged("n",props,&["chain_id","version","previous_version_uuid","identity_hash","hash_version","namespace","entity_type"]),
        same_time("n",props,&["valid_from","deleted_at"]),
        monotone("n",props,&["last_seen_at"],true),
        monotone("n",props,&["valid_to","invalid_at"],false),
        format!("(NOT 'is_latest' IN keys({props}) OR {props}.is_latest=n.is_latest OR (n.is_latest=true AND {props}.is_latest=false))"),
    ].join(" AND ")
}
pub(super) fn edge() -> String {
    [
        unchanged("r","$props",&["chain_id","version","previous_version_uuid","origin","identity_hash","cardinality_key","producer_source","producer_namespace","source_chain_id","target_chain_id","cancellation_snapshot_id","cancellation_context"]),
        same_time("r","$props",&["valid_from","deleted_at","cancelled_at"]),
        monotone("r","$props",&["last_seen_at","last_transition_at"],true),
        monotone("r","$props",&["valid_to","invalid_at"],false),
        "(NOT 'is_latest' IN keys($props) OR $props.is_latest=r.is_latest OR (r.is_latest=true AND $props.is_latest=false))".into(),
    ].join(" AND ")
}
