//! Maintenance reads include historical versions. Refreshes compare all properties
//! under lock so provider latency cannot overwrite concurrent source changes.
use crate::PreparedQuery;
use kg_core::{
    embedding::{EmbeddingSettings, RELATIONSHIP_TEXT_VERSION},
    embedding_rebuild::{EmbeddingKind, EmbeddingRecord, EmbeddingRefresh},
    errors::BackendError,
};
use serde_json::{json, Map, Value};
use uuid::Uuid;

fn scope(org: &str) -> Result<(), BackendError> {
    if org.trim().is_empty() {
        Err(BackendError::Query(
            "embedding maintenance requires an organization".into(),
        ))
    } else {
        Ok(())
    }
}
fn source(kind: EmbeddingKind) -> &'static str {
    match kind {
        EmbeddingKind::CommunityName => {
            "MATCH (n:Community {org_id:$org}) MATCH (scope:CommunityScope {org_id:$org,namespace:n.namespace})"
        }
        EmbeddingKind::Entity | EmbeddingKind::DerivedSummary => "MATCH (n:Entity {org_id:$org})",
        EmbeddingKind::Relationship => {
            "MATCH (s:Entity {org_id:$org})-[n:RELATES_TO {org_id:$org}]->(t:Entity {org_id:$org})"
        }
    }
}
const COMMUNITY_ELIGIBLE: &str = "scope.namespace=n.namespace AND scope.active_generation=n.generation_uuid AND n.dirty=false AND n.revision IS NOT NULL AND datetime(n.projected_at)<=datetime.realtime() AND (n.valid_until IS NULL OR datetime.realtime()<datetime(n.valid_until))";
fn eligibility(kind: EmbeddingKind) -> String {
    match kind {
        EmbeddingKind::CommunityName => format!(" AND {COMMUNITY_ELIGIBLE}"),
        EmbeddingKind::DerivedSummary => {
            " AND n.derived_summary IS NOT NULL AND n.summary_revision IS NOT NULL".into()
        }
        _ => String::new(),
    }
}
pub fn page(
    org: &str,
    kind: EmbeddingKind,
    after: Option<Uuid>,
    limit: usize,
) -> Result<PreparedQuery, BackendError> {
    scope(org)?;
    if limit == 0 || limit > 256 {
        return Err(BackendError::Query("invalid embedding page size".into()));
    }
    let seek = if after.is_some() {
        "n.uuid>$after"
    } else {
        "n.uuid IS NOT NULL"
    };
    let eligibility = eligibility(kind);
    if kind == EmbeddingKind::CommunityName {
        return Ok(PreparedQuery{statement:format!("MATCH (n:Community {{org_id:$org}}) USING INDEX n:Community(org_id,uuid) WHERE {seek} MATCH (scope:CommunityScope {{org_id:$org,namespace:n.namespace}}) WHERE {COMMUNITY_ELIGIBLE} RETURN properties(n) AS record ORDER BY n.org_id,n.uuid LIMIT $limit"),parameters:json!({"org":org,"after":after,"limit":limit})});
    }
    Ok(PreparedQuery {
        statement: format!(
            "{} WHERE {seek}{eligibility} RETURN properties(n) AS record ORDER BY n.uuid LIMIT $limit",
            source(kind)
        ),
        parameters: json!({"org":org,"after":after,"limit":limit}),
    })
}
pub fn decode_record(mut row: Map<String, Value>) -> Result<EmbeddingRecord, BackendError> {
    let properties = row
        .remove("record")
        .and_then(|v| v.as_object().cloned())
        .ok_or_else(|| {
            BackendError::Deserialization("embedding maintenance record missing".into())
        })?;
    let uuid = properties
        .get("uuid")
        .and_then(Value::as_str)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| {
            BackendError::Deserialization("embedding maintenance UUID missing".into())
        })?;
    Ok(EmbeddingRecord { uuid, properties })
}
pub fn refresh(
    org: &str,
    kind: EmbeddingKind,
    updates: &[EmbeddingRefresh],
) -> Result<PreparedQuery, BackendError> {
    scope(org)?;
    if updates.len() > 256 {
        return Err(BackendError::Query(
            "embedding refresh batch exceeds 256".into(),
        ));
    }
    let mut rows = Vec::with_capacity(updates.len());
    let mut ids = std::collections::HashSet::new();
    for update in updates {
        update.embedding.stored().validate()?;
        update.entity_fields.validate()?;
        let expected_version = match kind {
            EmbeddingKind::CommunityName => kg_core::community::NAME_TEXT_VERSION.into(),
            EmbeddingKind::Entity => update.entity_fields.text_version(),
            EmbeddingKind::Relationship => RELATIONSHIP_TEXT_VERSION.into(),
            EmbeddingKind::DerivedSummary => kg_core::entity_summary::SUMMARY_TEXT_VERSION.into(),
        };
        if !ids.insert(update.record.uuid)
            || update.embedding.text_version != expected_version
            || update.embedding.content_hash
                != kg_core::embedding::content_hash(
                    &update.record.text(kind, &update.entity_fields)?,
                )
        {
            return Err(BackendError::Query(
                "embedding refresh has duplicate targets or incompatible text provenance".into(),
            ));
        }
        rows.push(json!({"uuid":update.record.uuid,"expected":update.record.properties,"values":update.embedding.values,"model":update.embedding.model,"text_version":update.embedding.text_version,"hash":update.embedding.content_hash}));
    }
    rows.sort_by(|a, b| a["uuid"].as_str().cmp(&b["uuid"].as_str()));
    let [vector, model, version, hash] = kind.storage_properties();
    if kind == EmbeddingKind::CommunityName {
        return Ok(PreparedQuery {statement:format!("UNWIND $rows AS row MATCH (scope:CommunityScope {{org_id:$org,namespace:row.expected.namespace}}) MATCH (n:Community {{org_id:$org,uuid:row.uuid}}) WITH scope,n,row ORDER BY n.uuid SET n.uuid=n.uuid WITH scope,n,row WHERE properties(n)=row.expected AND {COMMUNITY_ELIGIBLE} SET n.{vector}=row.values,n.{model}=row.model,n.{version}=row.text_version,n.{hash}=row.hash RETURN count(n) AS updated"),parameters:json!({"org":org,"rows":rows})});
    }
    Ok(PreparedQuery {statement:format!("UNWIND $rows AS row {} WHERE n.uuid=row.uuid SET n.uuid=n.uuid WITH n,row WHERE properties(n)=row.expected SET n.{vector}=row.values,n.{model}=row.model,n.{version}=row.text_version,n.{hash}=row.hash RETURN count(n) AS updated",source(kind)),parameters:json!({"org":org,"rows":rows})})
}
pub fn incompatible(
    org: &str,
    settings: &EmbeddingSettings,
) -> Result<PreparedQuery, BackendError> {
    scope(org)?;
    let count = |kind: EmbeddingKind, version: &str| {
        let [vector, model, text_version, hash] = kind.storage_properties();
        let eligible = match kind {
            EmbeddingKind::CommunityName => format!("{COMMUNITY_ELIGIBLE} AND "),
            EmbeddingKind::DerivedSummary => {
                "n.derived_summary IS NOT NULL AND n.summary_revision IS NOT NULL AND ".into()
            }
            _ => String::new(),
        };
        format!("{} WHERE {eligible}(n.{vector} IS NULL OR n.{model} IS NULL OR n.{model}<>$model OR n.{text_version} IS NULL OR n.{text_version}<>{version} OR n.{hash} IS NULL OR size(n.{vector})<>$dimension OR NOT all(x IN n.{vector} WHERE x IS NOT NULL AND x*0=0) OR NOT any(x IN n.{vector} WHERE x<>0)) RETURN count(n) AS count",source(kind))
    };
    Ok(PreparedQuery {
        statement: format!(
            "CALL {{ {} UNION ALL {} UNION ALL {} UNION ALL {} }} RETURN sum(count) AS count",
            count(EmbeddingKind::Entity, "$entity_version"),
            count(EmbeddingKind::Relationship, "$relationship_version"),
            count(EmbeddingKind::DerivedSummary, "$summary_version"),
            count(EmbeddingKind::CommunityName, "$community_version")
        ),
        parameters: json!({"org":org,"model":settings.model,"dimension":settings.dimension,"entity_version":settings.text_version,"relationship_version":RELATIONSHIP_TEXT_VERSION,"summary_version":kg_core::entity_summary::SUMMARY_TEXT_VERSION,"community_version":kg_core::community::NAME_TEXT_VERSION}),
    })
}
pub fn count(rows: &[Map<String, Value>], key: &str) -> Result<usize, BackendError> {
    if rows.len() != 1 {
        return Err(BackendError::Deserialization(
            "invalid maintenance count".into(),
        ));
    }
    rows[0]
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|v| usize::try_from(v).ok())
        .ok_or_else(|| BackendError::Deserialization("invalid maintenance count".into()))
}
