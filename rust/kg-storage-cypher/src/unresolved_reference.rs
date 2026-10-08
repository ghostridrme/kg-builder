//! Read the unresolved applications waiting on typed key tokens (R4 refresh).
use crate::PreparedQuery;
use kg_core::{
    errors::BackendError,
    traits::{
        graph_reads::MAX_LOOKUP_KEYS, UnresolvedReferenceEntry, UnresolvedReferenceQuery,
        UnresolvedReferenceRecord,
    },
};
use serde_json::{json, Map, Value};
use uuid::Uuid;

pub fn read(org: &str, request: &UnresolvedReferenceQuery) -> Result<PreparedQuery, BackendError> {
    let tokens = &request.tokens;
    if org.trim().is_empty()
        || tokens.is_empty()
        || tokens.len() > MAX_LOOKUP_KEYS
        || request.limit == 0
        || request.limit > MAX_LOOKUP_KEYS
        || tokens.iter().any(|token| token.trim().is_empty())
        || request.after.as_ref().is_some_and(|cursor| {
            cursor.source_chain_id.is_nil()
                || cursor.slot.trim().is_empty()
                || cursor.token.trim().is_empty()
        })
    {
        return Err(BackendError::Query(
            "invalid unresolved reference request".into(),
        ));
    }
    Ok(PreparedQuery {
        statement: "UNWIND $tokens AS token MATCH (n:UnresolvedReference {org_id:$org, token:token}) WITH DISTINCT n WHERE NOT $has_after OR n.source_chain_id > $after_source OR (n.source_chain_id = $after_source AND n.slot > $after_slot) OR (n.source_chain_id = $after_source AND n.slot = $after_slot AND n.token > $after_token) RETURN n.source_chain_id AS source_chain_id, n.slot AS slot, n.token AS token, n.reason AS reason, n.snapshot_id AS snapshot_id, n.recorded_at AS recorded_at ORDER BY n.source_chain_id, n.slot, n.token LIMIT $limit".into(),
        parameters: json!({
            "org": org,
            "tokens": tokens,
            "limit": request.limit + 1,
            "has_after": request.after.is_some(),
            "after_source": request.after.as_ref().map(|c| c.source_chain_id.to_string()).unwrap_or_default(),
            "after_slot": request.after.as_ref().map(|c| c.slot.as_str()).unwrap_or(""),
            "after_token": request.after.as_ref().map(|c| c.token.as_str()).unwrap_or(""),
        }),
    })
}

pub fn decode(row: &Map<String, Value>) -> Result<UnresolvedReferenceRecord, BackendError> {
    let invalid = || BackendError::Deserialization("invalid unresolved reference".into());
    let text = |key: &str| row.get(key).and_then(Value::as_str).ok_or_else(invalid);
    let record = UnresolvedReferenceRecord {
        source_chain_id: Uuid::parse_str(text("source_chain_id")?).map_err(|_| invalid())?,
        slot: text("slot")?.to_owned(),
        entry: UnresolvedReferenceEntry {
            token: text("token")?.to_owned(),
            reason: text("reason")?.to_owned(),
            snapshot_id: row
                .get("snapshot_id")
                .and_then(Value::as_str)
                .map(Uuid::parse_str)
                .transpose()
                .map_err(|_| invalid())?,
            recorded_at: text("recorded_at")?.parse().map_err(|_| invalid())?,
        },
    };
    record.entry.validate()?;
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_is_parameterized_and_bounded() {
        let q = read(
            "org",
            &UnresolvedReferenceQuery {
                tokens: vec!["i:443".into()],
                after: None,
                limit: 10,
            },
        )
        .unwrap();
        assert_eq!(q.parameters["tokens"], json!(["i:443"]));
        assert!(q.statement.contains("LIMIT $limit"));
        assert!(
            !q.statement.contains("443"),
            "caller values never enter the statement"
        );
        assert!(read(
            "org",
            &UnresolvedReferenceQuery {
                tokens: vec![],
                after: None,
                limit: 10
            }
        )
        .is_err());
        assert!(read(
            "org",
            &UnresolvedReferenceQuery {
                tokens: vec![" ".into()],
                after: None,
                limit: 10
            }
        )
        .is_err());
    }

    #[test]
    fn rows_decode_to_validated_records() {
        let source = Uuid::new_v4();
        let row = json!({"source_chain_id": source.to_string(), "slot": "AwsInstance.subnet_id", "token": "s:subnet-1", "reason": "target-not-found", "snapshot_id": null, "recorded_at": "2026-09-21T00:00:00+00:00"});
        let record = decode(row.as_object().unwrap()).unwrap();
        assert_eq!(record.source_chain_id, source);
        assert_eq!(record.entry.reason, "target-not-found");
        let mut bad = row.clone();
        bad["reason"] = json!("");
        assert!(decode(bad.as_object().unwrap()).is_err());
    }
}
