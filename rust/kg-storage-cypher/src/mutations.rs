//! Parameterized mutations and their required match checks. Execute the whole
//! result in one transaction and require exactly `expected_rows` rows of
//! `ok = true` from every statement.
//!
//! Consecutive node upserts, snapshot upserts, metadata updates, observations, observation links,
//! and embedding writes with distinct keys are grouped into one `UNWIND`
//! statement each; a row that fails its match drops out and the row count
//! check fails the batch, exactly as the single-statement form would.
//! Relationship, supersession, merge, split, and repoint statements keep their
//! ordered single-statement form because their checks read state written by
//! earlier statements of the same kind.

#[path = "mutation_history.rs"]
mod history;

#[path = "entity_version_embedding.rs"]
mod entity_version_embedding;

use crate::PreparedWrite;
use kg_core::{errors::BackendError, traits::GraphMutation};
use serde_json::{json, Value};
#[cfg(test)]
use uuid::Uuid;

/// Prepare an ordered batch with grouped statements.
pub fn mutations(
    org: &str,
    mutations: &[GraphMutation],
) -> Result<Vec<PreparedWrite>, BackendError> {
    prepare(org, mutations, true)
}

/// Prepare one statement sequence per mutation; the reference semantics that
/// grouping must preserve.
pub fn mutations_ungrouped(
    org: &str,
    mutations: &[GraphMutation],
) -> Result<Vec<PreparedWrite>, BackendError> {
    prepare(org, mutations, false)
}

fn prepare(
    org: &str,
    mutations: &[GraphMutation],
    grouped: bool,
) -> Result<Vec<PreparedWrite>, BackendError> {
    for mutation in mutations {
        mutation.validate(org)?;
    }
    let community_targets = crate::community_revision::Targets::from_mutations(mutations);
    if community_targets.has_source() && !community_targets.publications.is_empty() {
        return Err(BackendError::Query(
            "Community publication must not share a transaction with source mutations".into(),
        ));
    }
    let mut result = Vec::new();
    if let Some(lock) = crate::saga::lock_sagas(org, mutations) {
        result.push(lock);
    }
    let mut index = 0;
    while index < mutations.len() {
        let run = if grouped {
            kg_core::traits::graph_mutation::mutation_group_length(&mutations[index..])
        } else {
            1
        };
        if run > 1 {
            push_group(org, &mutations[index..index + run], &mut result);
        } else if let GraphMutation::RecordReferenceDecisions { decisions, reuses } =
            &mutations[index]
        {
            for (statement, parameters) in
                crate::reference_decision::writes(org, decisions, reuses)?
            {
                result.push(PreparedWrite {
                    statement,
                    parameters,
                    expected_rows: 1,
                });
            }
        } else {
            push_single(org, &mutations[index], &mut result);
        }
        index += run;
    }
    Ok(result)
}

fn entity_text_fields() -> Vec<String> {
    ["entity_type", "name", "summary"]
        .into_iter()
        .map(str::to_owned)
        .chain(
            kg_core::embedding::DESCRIPTIVE_FIELDS
                .iter()
                .flat_map(|f| [format!("prop_{f}"), format!("property_type_{f}")]),
        )
        .collect()
}

fn push_group(org: &str, run: &[GraphMutation], out: &mut Vec<PreparedWrite>) {
    use GraphMutation::*;
    let rows: Vec<Value> = run
        .iter()
        .map(|mutation| match mutation {
            UpsertEntity { uuid, properties } => json!({
                "uuid": uuid,
                "chain": properties["chain_id"],
                "state": json!([org, properties["chain_id"]]).to_string(),
                "props": properties,
            }),
            ApplyEntityMetadata { .. } => metadata_parameters(mutation),
            RecordUnresolvedReferences { .. } => unresolved_parameters(mutation),
            UpsertSnapshot { uuid, properties } => json!({"uuid": uuid, "props": properties}),
            ObserveEntity {
                chain_id,
                observed_at,
                sync_generation,
                snapshot_id,
                collection,
            } => json!({
                "chain": chain_id,
                "at": observed_at.to_rfc3339(),
                "generation": sync_generation,
                "snapshot": snapshot_id,
                "member": collection.as_ref().map(|m| m.collection.member_id()),
                "member_generation": collection
                    .as_ref()
                    .map(|m| crate::bolt_generation(m.generation)),
            }),
            RecordObservation {
                uuid,
                snapshot_uuid,
                entity_uuid,
                entity_chain_id,
                observed_at,
                reconciliations,
            } => json!({
                "uuid": uuid,
                "snapshot": snapshot_uuid,
                "entity": entity_uuid,
                "chain": entity_chain_id,
                "at": observed_at.to_rfc3339(),
                "reconciliations": reconciliations_property(reconciliations),
            }),
            SetEntityVersionEmbedding { .. } => entity_version_embedding::parameters(mutation),
            SetEmbedding {
                uuid,
                embedding,
                text_version,
                content_hash,
            } => json!({
                "uuid": uuid,
                "values": embedding.values,
                "model": embedding.model,
                "text_version": text_version,
                "content_hash": content_hash,
            }),
            _ => unreachable!("only groupable mutations form a run"),
        })
        .collect();
    let expected_rows = rows.len();
    let parameters = json!({"org": org, "rows": rows, "text_fields":entity_text_fields()});
    if matches!(run[0], UpsertEntity { .. }) {
        out.push(PreparedWrite {
            statement: crate::identity::sync(&upsert_entities()),
            parameters,
            expected_rows,
        });
        return;
    }

    if matches!(run[0], RecordUnresolvedReferences { .. }) {
        let mut statement = RECORD_UNRESOLVED_REFERENCES.to_owned();
        for key in ["source", "slot", "decided_at", "decision_id", "entries"] {
            statement = statement.replace(&format!("${key}"), &format!("row.{key}"));
        }
        out.push(PreparedWrite {
            statement: format!("UNWIND $rows AS row WITH row ORDER BY row.source,row.slot CALL (row) {{ {statement} }} RETURN ok"),
            parameters, expected_rows,
        });
        return;
    }

    if matches!(run[0], ApplyEntityMetadata { .. }) {
        let mut statement = APPLY_ENTITY_METADATA.to_string();
        for key in [
            "uuid",
            "previous",
            "tags",
            "labels",
            "replace",
            "signature",
            "reference_exclusions",
            "profile_contract",
            "at",
        ] {
            statement = statement.replace(&format!("${key}"), &format!("row.{key}"));
        }
        out.push(PreparedWrite {
            statement: format!(
                "UNWIND $rows AS row CALL (row) {{ {statement} }} RETURN ok, metadata_conflict"
            ),
            parameters,
            expected_rows,
        });
        return;
    }

    let statements: &[&str] = match run[0] {
        UpsertSnapshot { .. } => &[UPSERT_SNAPSHOTS],
        ObserveEntity { .. } => &[OBSERVE_ENTITIES],
        RecordObservation { .. } => &[CHECK_OBSERVATION_IDENTITIES, RECORD_OBSERVATIONS],
        SetEmbedding { .. } => &[SET_EMBEDDINGS],
        SetEntityVersionEmbedding { .. } => &[entity_version_embedding::WRITE],
        _ => unreachable!("only groupable mutations form a run"),
    };
    for statement in statements {
        out.push(PreparedWrite {
            statement: statement.to_string(),
            parameters: parameters.clone(),
            expected_rows,
        });
    }
}

fn metadata_parameters(mutation: &GraphMutation) -> Value {
    let GraphMutation::ApplyEntityMetadata {
        profile_contract,
        reference_exclusions,
        uuid,
        previous_uuid,
        tags,
        labels,
        replace,
        observed_at,
    } = mutation
    else {
        unreachable!("only metadata mutations use this serializer")
    };
    // Empty metadata is compatible across modes: a full clear wins
    // regardless of whether the partial no-op comes before or after it.
    let signature = serde_json::to_vec(&(
        *replace && (!tags.is_empty() || !labels.is_empty()),
        tags.iter().collect::<std::collections::BTreeMap<_, _>>(),
        labels.iter().collect::<std::collections::BTreeSet<_>>(),
    ))
    .expect("classification strings serialize");
    let signature = format!("{:032x}", xxhash_rust::xxh3::xxh3_128(&signature));
    let tags: serde_json::Map<String, Value> = tags
        .iter()
        .map(|(key, value)| (format!("tag_{key}"), Value::String(value.clone())))
        .collect();
    let reference_exclusions = reference_exclusions
        .iter()
        .collect::<std::collections::BTreeSet<_>>();
    json!({"uuid": uuid, "previous": previous_uuid, "tags": tags, "labels": labels,"reference_exclusions":reference_exclusions,"profile_contract":profile_contract,
        "replace": replace, "signature": signature, "at": observed_at.to_rfc3339()})
}

fn push_single(org: &str, mutation: &GraphMutation, out: &mut Vec<PreparedWrite>) {
    let mut push = |statement: &str, mut parameters: Value| {
        parameters["org"] = json!(org);
        parameters["text_fields"] = json!(entity_text_fields());
        out.push(PreparedWrite {
            statement: statement.to_string(),
            parameters,
            expected_rows: 1,
        });
    };
    use GraphMutation::*;
    match mutation {
        AssertCommunityState { state } => out.push(crate::community::guard(org, state)),

        BeginCommunityGeneration { generation } => {
            out.extend(crate::community::begin(org, generation))
        }
        StageCommunityPartition { partition } => {
            out.extend(crate::community::stage(org, partition))
        }
        PublishCommunityGeneration { publication } => {
            out.extend(crate::community::publish(org, publication))
        }
        UpdateCommunities { update } => out.extend(crate::community::update(org, update)),

        AssociateSagaSnapshot { association } => {
            out.extend(crate::saga::associate(org, association))
        }
        SetSagaSummary { summary } => out.extend(crate::saga::summary(org, summary)),
        SetDerivedSummary { .. } => {
            out.extend(crate::entity_summary::publication(org, mutation));
        }
        ClearDerivedSummary { guard } => {
            out.extend(crate::entity_summary::clear(org, guard));
        }
        UpsertEntity { uuid, properties } => push(
            &crate::identity::sync(&upsert_entity()),
            json!({
                "uuid": uuid,
                "chain": properties["chain_id"],
                "state": json!([org, properties["chain_id"]]).to_string(),
                "props": properties,
            }),
        ),
        RecordUnresolvedReferences { .. } => push(
            RECORD_UNRESOLVED_REFERENCES,
            unresolved_parameters(mutation),
        ),
        // Prepared by `prepare` itself, where serialization failures propagate.
        RecordReferenceDecisions { .. } => {}
        UpsertSnapshot { uuid, properties } => push(
            UPSERT_SNAPSHOT,
            json!({
                "uuid": uuid,
                "props": properties,
            }),
        ),
        SupersedeEntity {
            uuid,
            chain_id,
            valid_to,
        } => push(
            SUPERSEDE_ENTITY,
            json!({
                "uuid": uuid,
                "chain": chain_id,
                "at": valid_to.to_rfc3339(),
            }),
        ),
        ApplyEntityMetadata { .. } => push(APPLY_ENTITY_METADATA, metadata_parameters(mutation)),
        UpdateEntity { uuid, properties } => push(
            &crate::identity::sync(&update_entity()),
            json!({
                "uuid": uuid,
                "props": properties,
                "text_fields": entity_text_fields(),
            }),
        ),
        DeleteEntity {
            chain_id,
            deleted_at,
            deleted_by,
            reason,
        } => push(
            DELETE_ENTITY,
            json!({
                "chain": chain_id,
                "state": json!([org, chain_id]).to_string(),
                "at": deleted_at.to_rfc3339(),
                "by": deleted_by,
                "reason": reason,
            }),
        ),
        ObserveEntity {
            chain_id,
            observed_at,
            sync_generation,
            snapshot_id,
            collection,
        } => push(
            OBSERVE_ENTITY,
            json!({
                "chain": chain_id,
                "at": observed_at.to_rfc3339(),
                "generation": sync_generation,
                "snapshot": snapshot_id,
                "member": collection.as_ref().map(|m| m.collection.member_id()),
                "member_generation": collection
                    .as_ref()
                    .map(|m| crate::bolt_generation(m.generation)),
            }),
        ),
        UpsertEdge {
            uuid,
            source_chain_id,
            target_chain_id,
            properties,
        } => {
            push(
                CHECK_EDGE_IDENTITY,
                json!({
                    "uuid": uuid,
                    "source": source_chain_id,
                    "target": target_chain_id,
                }),
            );
            push(
                &crate::reference_dependency::sync(&upsert_edge()),
                json!({
                    "uuid": uuid,
                    "source": source_chain_id,
                    "target": target_chain_id,
                    "props": properties,
                }),
            );
        }
        CancelEdge {
            uuid,
            cancelled_at,
            cancellation_snapshot_id,
            cancellation_context,
            observed_at,
        } => push(
            CANCEL_EDGE,
            json!({"uuid":uuid,"at":cancelled_at.to_rfc3339(),"snapshot":cancellation_snapshot_id,"context":cancellation_context.as_ref().map(|context| serde_json::to_string(context).expect("typed cancellation context serializes")),"observed":observed_at.to_rfc3339()}),
        ),
        UpdateEdge { uuid, properties } => push(
            &crate::reference_dependency::sync(&update_edge()),
            json!({
                "uuid": uuid,
                "props": properties,
            }),
        ),
        ReleaseMembership { uuid, collection } => push(
            RELEASE_MEMBERSHIP,
            json!({
                "uuid": uuid,
                "member": collection.member_id(),
            }),
        ),
        RepointEntity {
            previous_uuid,
            new_uuid,
            chain_id,
        } => {
            let p = json!({
                "old": previous_uuid,
                "new": new_uuid,
                "chain": chain_id,
            });
            push(CHECK_REPOINT, p.clone());
            // Delete before recreating so relationship UUID uniqueness remains enforceable.
            push(REPOINT_OUTGOING, p.clone());
            push(REPOINT_INCOMING, p.clone());
            push(CARRY_ALIASES, p.clone());
            push(
                &crate::identity::sync("MATCH (n:Entity {org_id:$org,uuid:$new})"),
                p,
            );
        }
        MergeChains {
            loser_chain_id,
            winner_chain_id,
            identity_hashes,
            effective_at,
        } => {
            use crate::chain_transitions::*;
            let mut hashes = identity_hashes.clone();
            hashes.sort();
            hashes.dedup();
            let p = json!({
                "loser": loser_chain_id, "winner": winner_chain_id, "hashes": hashes,
                "at": effective_at.to_rfc3339(),
                "state": json!([org, loser_chain_id]).to_string(),
                "winner_state": json!([org, winner_chain_id]).to_string(),
                "period": json!([org, loser_chain_id, effective_at]).to_string(),
                "cancellation_context": serde_json::to_string(&kg_core::models::CancellationContext::Merge { loser_chain_id: *loser_chain_id, winner_chain_id: *winner_chain_id, effective_at: *effective_at }).expect("typed cancellation context serializes"),
            });
            for statement in [
                LOCK_STATE,
                LOCK_WINNER,
                LOCK_LOSER,
                CHECK_MERGE,
                CLOSE_FACTS,
                GRANT_ALIASES,
                HIDE_LOSER,
            ] {
                push(statement, p.clone());
            }
            push(
                &crate::identity::sync(
                    "MATCH (n:Entity {org_id:$org,chain_id:$winner,is_latest:true})",
                ),
                p.clone(),
            );
        }
        SplitChain {
            split_chain_id,
            from_chain_id,
            identity_hashes,
            effective_at,
        } => {
            use crate::chain_transitions::*;
            let mut hashes = identity_hashes.clone();
            hashes.sort();
            hashes.dedup();
            let p = json!({
                "loser": split_chain_id, "winner": from_chain_id, "hashes": hashes,
                "at": effective_at.to_rfc3339(),
                "state": json!([org, split_chain_id]).to_string(),
                "winner_state": json!([org, from_chain_id]).to_string(),
            });
            for statement in [
                LOCK_STATE,
                LOCK_WINNER,
                LOCK_LOSER,
                CHECK_SPLIT,
                RELEASE_ALIASES,
                RESTORE_LOSER,
            ] {
                push(statement, p.clone());
            }
            push(
                &crate::identity::sync(
                    "MATCH (n:Entity {org_id:$org,chain_id:$winner,is_latest:true})",
                ),
                p.clone(),
            );
            let loser_sync = crate::identity::sync(
                "MATCH (n:Entity {org_id:$org,chain_id:$loser,is_latest:true})",
            );
            push(
                &format!(
                    "CALL {{ {loser_sync} }} RETURN all(value IN collect(ok) WHERE value) AS ok, any(value IN collect(identity_conflict) WHERE value) AS identity_conflict"
                ),
                p,
            );
        }
        RecordObservation {
            uuid,
            snapshot_uuid,
            entity_uuid,
            entity_chain_id,
            observed_at,
            reconciliations,
        } => {
            let p = json!({
                "uuid": uuid,
                "snapshot": snapshot_uuid,
                "entity": entity_uuid,
                "chain": entity_chain_id,
                "at": observed_at.to_rfc3339(),
                "reconciliations": reconciliations_property(reconciliations),
            });
            push(CHECK_OBSERVATION_IDENTITY, p.clone());
            push(RECORD_OBSERVATION, p);
        }
        SetEmbedding {
            uuid,
            embedding,
            text_version,
            content_hash,
        } => push(
            SET_EMBEDDING,
            json!({
                "uuid": uuid,
                "values": embedding.values,
                "model": embedding.model,
                "text_version": text_version,
                "content_hash": content_hash,
            }),
        ),
        SetEntityVersionEmbedding { .. } => push(
            entity_version_embedding::WRITE,
            json!({"rows": [entity_version_embedding::parameters(mutation)]}),
        ),
        SetRelationshipEmbedding {
            uuid,
            embedding,
            text_version,
            content_hash,
        } => push(
            SET_RELATIONSHIP_EMBEDDING,
            json!({
                "uuid": uuid,
                "values": embedding.values,
                "model": embedding.model,
                "text_version": text_version,
                "content_hash": content_hash,
            }),
        ),
    }
}

fn upsert_entity() -> String {
    format!("MERGE (state:ChainMergeState {{scope_id:$state}})
    ON CREATE SET state.org_id=$org,state.chain_id=$chain
    SET state.scope_id=state.scope_id
    WITH state WHERE (state.action IS NULL OR state.action='split')
        AND (state.watermark IS NULL OR datetime(state.watermark)<=datetime(coalesce($props.last_seen_at,$props.valid_from)))
    CALL (state) {{
        OPTIONAL MATCH (previous:Entity {{org_id:$org,chain_id:$chain}}) WHERE previous.uuid<>$uuid
        RETURN previous ORDER BY previous.version DESC,previous.uuid LIMIT 1
    }}
    WITH state,previous WHERE previous.deleted_at IS NULL OR
        (previous.uuid=$props.previous_version_uuid AND datetime(previous.deleted_at)<datetime($props.valid_from)
         AND (previous.last_transition_at IS NULL OR datetime(previous.last_transition_at)<datetime($props.valid_from)))
    FOREACH (_ IN CASE WHEN previous.deleted_at IS NOT NULL THEN [1] ELSE [] END |
        SET state.watermark=CASE WHEN state.watermark IS NULL OR datetime(state.watermark)<datetime($props.valid_from) THEN $props.valid_from ELSE state.watermark END)
    WITH state
    OPTIONAL MATCH (existing:GraphNode {{uuid:$uuid}})
    WITH state,existing IS NULL AS creating
    MERGE (n:GraphNode {{uuid:$uuid}})
    ON CREATE SET n:Entity,n.org_id=$org,n.chain_id=$chain,n.namespace=$props.namespace,n.entity_type=$props.entity_type
    SET n.uuid=n.uuid
    WITH n,state,creating
    WHERE (creating OR ({guard})) AND n:Entity AND n.org_id=$org AND n.chain_id=$chain AND n.namespace=$props.namespace AND n.entity_type=$props.entity_type
        AND (n.is_latest IS NULL OR n.is_latest=true OR coalesce($props.is_latest,false)=false)
        AND (n.deleted_at IS NULL OR
            (coalesce($props.is_latest,false)=false AND
             (NOT 'deleted_at' IN keys($props) OR datetime($props.deleted_at)=datetime(n.deleted_at))))
    SET n.uuid=n.uuid
    WITH n,state,any(k IN $text_fields WHERE k IN keys($props) AND coalesce(n[k]<>$props[k],(n[k] IS NULL)<>($props[k] IS NULL))) AS changed
    FOREACH (_ IN CASE WHEN changed THEN [1] ELSE [] END | REMOVE n.embedding,n.embedding_model,n.embedding_text_version,n.embedding_content_hash)
    SET n += $props, n.uuid=$uuid,n.org_id=$org,n.last_transition_at=CASE WHEN n.last_transition_at IS NULL OR (state.watermark IS NOT NULL AND datetime(n.last_transition_at)<datetime(state.watermark)) THEN state.watermark ELSE n.last_transition_at END",guard=history::entity("$props"))
}

fn upsert_entities() -> String {
    format!("UNWIND $rows AS row
    MERGE (state:ChainMergeState {{scope_id:row.state}})
    ON CREATE SET state.org_id=$org,state.chain_id=row.chain
    SET state.scope_id=state.scope_id
    WITH state,row WHERE (state.action IS NULL OR state.action='split')
        AND (state.watermark IS NULL OR datetime(state.watermark)<=datetime(coalesce(row.props.last_seen_at,row.props.valid_from)))
    CALL (state,row) {{
        OPTIONAL MATCH (previous:Entity {{org_id:$org,chain_id:row.chain}}) WHERE previous.uuid<>row.uuid
        RETURN previous ORDER BY previous.version DESC,previous.uuid LIMIT 1
    }}
    WITH state,row,previous WHERE previous.deleted_at IS NULL OR
        (previous.uuid=row.props.previous_version_uuid AND datetime(previous.deleted_at)<datetime(row.props.valid_from)
         AND (previous.last_transition_at IS NULL OR datetime(previous.last_transition_at)<datetime(row.props.valid_from)))
    FOREACH (_ IN CASE WHEN previous.deleted_at IS NOT NULL THEN [1] ELSE [] END |
        SET state.watermark=CASE WHEN state.watermark IS NULL OR datetime(state.watermark)<datetime(row.props.valid_from) THEN row.props.valid_from ELSE state.watermark END)
    WITH state,row
    OPTIONAL MATCH (existing:GraphNode {{uuid:row.uuid}})
    WITH state,row,existing IS NULL AS creating
    MERGE (n:GraphNode {{uuid:row.uuid}})
    ON CREATE SET n:Entity,n.org_id=$org,n.chain_id=row.chain,n.namespace=row.props.namespace,n.entity_type=row.props.entity_type
    SET n.uuid=n.uuid
    WITH n,row,state,creating
    WHERE (creating OR ({guard})) AND n:Entity AND n.org_id=$org AND n.chain_id=row.chain AND n.namespace=row.props.namespace AND n.entity_type=row.props.entity_type
        AND (n.is_latest IS NULL OR n.is_latest=true OR coalesce(row.props.is_latest,false)=false)
        AND (n.deleted_at IS NULL OR
            (coalesce(row.props.is_latest,false)=false AND
             (NOT 'deleted_at' IN keys(row.props) OR datetime(row.props.deleted_at)=datetime(n.deleted_at))))
    SET n.uuid=n.uuid
    WITH n,row,state,any(k IN $text_fields WHERE k IN keys(row.props) AND coalesce(n[k]<>row.props[k],(n[k] IS NULL)<>(row.props[k] IS NULL))) AS changed
    FOREACH (_ IN CASE WHEN changed THEN [1] ELSE [] END | REMOVE n.embedding,n.embedding_model,n.embedding_text_version,n.embedding_content_hash)
    SET n += row.props,n.uuid=row.uuid,n.org_id=$org,n.last_transition_at=CASE WHEN n.last_transition_at IS NULL OR (state.watermark IS NOT NULL AND datetime(n.last_transition_at)<datetime(state.watermark)) THEN state.watermark ELSE n.last_transition_at END",guard=history::entity("row.props"))
}

const UPSERT_SNAPSHOT: &str = "MERGE (n:GraphNode {uuid:$uuid})
    ON CREATE SET n:Snapshot,n.org_id=$org,n += $props
    ON MATCH SET n.uuid=n.uuid
    WITH n
    WHERE n:Snapshot AND n.org_id=$org
      AND all(k IN ['namespace','source','data_type','source_description','captured_at','created_at','content'] WHERE (n[k] IS NULL AND $props[k] IS NULL) OR n[k]=$props[k])
    SET n += $props,n.uuid=$uuid,n.org_id=$org
    RETURN true AS ok";

fn unresolved_parameters(mutation: &GraphMutation) -> Value {
    let GraphMutation::RecordUnresolvedReferences {
        source_chain_id,
        slot,
        decided_at,
        decision_id,
        entries,
    } = mutation
    else {
        unreachable!("unresolved mutation")
    };
    let mut entries: Vec<_> = entries
        .iter()
        .map(|entry| {
            json!({
                "token": entry.token, "reason": entry.reason, "snapshot_id": entry.snapshot_id,
                "at": decided_at.to_rfc3339(),
            })
        })
        .collect();
    entries.sort_by_key(|entry| {
        (
            entry["token"].to_string(),
            entry["reason"].to_string(),
            entry["snapshot_id"].to_string(),
        )
    });
    entries.dedup_by(|a, b| a["token"] == b["token"] && a["reason"] == b["reason"]);
    if entries.is_empty() {
        entries.push(json!({"token":null,"reason":"cleared","snapshot_id":null,"at":decided_at.to_rfc3339()}));
    }
    json!({"source":source_chain_id,"slot":slot,"decided_at":decided_at.to_rfc3339(),"decision_id":decision_id,"entries":entries})
}

// The anchor serializes even an absent slot. Older databases initialize its
// watermark from the existing records before deciding whether an update is fresh.
const RECORD_UNRESOLVED_REFERENCES: &str = "MERGE (anchor:UnresolvedSlot {org_id:$org,source_chain_id:$source,slot:$slot})
 SET anchor.slot=anchor.slot
 WITH anchor OPTIONAL MATCH (old:UnresolvedReference {org_id:$org,source_chain_id:$source,slot:$slot})
 WITH anchor,collect(old) AS previous
 WITH anchor,previous,
 ((anchor.decided_at IS NULL OR datetime(anchor.decided_at)<datetime($decided_at) OR (datetime(anchor.decided_at)=datetime($decided_at) AND anchor.decision_id<=toString($decision_id)))
 AND all(old IN previous WHERE datetime(old.recorded_at)<datetime($decided_at) OR (datetime(old.recorded_at)=datetime($decided_at) AND coalesce(old.decision_id,'')<=toString($decision_id)))) AS apply,
 size(previous)=size($entries) AND all(old IN previous WHERE any(e IN $entries WHERE ((old.token IS NULL AND e.token IS NULL) OR old.token=e.token) AND old.reason=e.reason)) AS unchanged
 FOREACH (old IN CASE WHEN apply AND NOT unchanged THEN previous ELSE [] END | DELETE old)
 FOREACH (e IN CASE WHEN apply AND NOT unchanged THEN $entries ELSE [] END |
 CREATE (:UnresolvedReference {org_id:$org,source_chain_id:$source,slot:$slot,token:e.token,reason:e.reason,snapshot_id:e.snapshot_id,recorded_at:$decided_at,decision_id:toString($decision_id)}))
 FOREACH (old IN CASE WHEN apply AND unchanged THEN previous ELSE [] END |
 SET old.recorded_at=$decided_at,old.decision_id=toString($decision_id),old.snapshot_id=head([e IN $entries WHERE ((old.token IS NULL AND e.token IS NULL) OR old.token=e.token) AND old.reason=e.reason | e.snapshot_id]))
 FOREACH (_ IN CASE WHEN apply THEN [1] ELSE [] END | SET anchor.decided_at=$decided_at,anchor.decision_id=toString($decision_id))
 RETURN true AS ok";

const UPSERT_SNAPSHOTS: &str = "UNWIND $rows AS row
    MERGE (n:GraphNode {uuid:row.uuid})
    ON CREATE SET n:Snapshot,n.org_id=$org,n += row.props
    ON MATCH SET n.uuid=n.uuid
    WITH n,row
    WHERE n:Snapshot AND n.org_id=$org
      AND all(k IN ['namespace','source','data_type','source_description','captured_at','created_at','content'] WHERE (n[k] IS NULL AND row.props[k] IS NULL) OR n[k]=row.props[k])
    SET n += row.props,n.uuid=row.uuid,n.org_id=$org
    RETURN true AS ok";

const SUPERSEDE_ENTITY: &str = "MATCH (n:Entity {org_id:$org,uuid:$uuid,chain_id:$chain})
    SET n.uuid=n.uuid
    WITH n WHERE (n.last_transition_at IS NULL OR datetime(n.last_transition_at)<=datetime($at))
        AND (n.valid_from IS NULL OR datetime(n.valid_from)<=datetime($at))
        AND (n.valid_to IS NULL OR datetime($at)<=datetime(n.valid_to))
    SET n.is_latest=false,n.valid_to=$at
    RETURN true AS ok";

fn update_entity() -> String {
    format!("MATCH (n:Entity {{org_id:$org,uuid:$uuid}})
    SET n.uuid=n.uuid
    WITH n WHERE n.merged_into IS NULL AND ({guard})
        AND (NOT 'deleted_at' IN keys($props) OR datetime($props.deleted_at)=datetime(n.deleted_at)
             OR ($props.deleted_at IS NULL AND n.deleted_at IS NULL))
        AND (NOT 'is_latest' IN keys($props) OR $props.is_latest=n.is_latest
             OR (n.is_latest=true AND $props.is_latest=false))
    WITH n,coalesce(n.identity_hashes,[]) AS old_hashes,any(k IN $text_fields WHERE k IN keys($props) AND coalesce(n[k]<>$props[k],(n[k] IS NULL)<>($props[k] IS NULL))) AS changed
    SET n += $props
    FOREACH (_ IN CASE WHEN 'identity_hashes' IN keys($props) THEN [1] ELSE [] END |
        SET n.identity_hashes=reduce(hashes=old_hashes,h IN $props.identity_hashes | CASE WHEN h IN hashes THEN hashes ELSE hashes+[h] END))
    FOREACH (_ IN CASE WHEN changed THEN [1] ELSE [] END | REMOVE n.embedding,n.embedding_model,n.embedding_text_version,n.embedding_content_hash)",guard=history::entity("$props"))
}

// Deletion is a lineage transition just like merge/split. Keep its watermark
// after restoration so old relationship captures cannot attach to a new life.
const DELETE_ENTITY: &str = "MERGE (state:ChainMergeState {scope_id:$state})
    ON CREATE SET state.org_id=$org,state.chain_id=$chain
    SET state.scope_id=state.scope_id
    WITH state WHERE state.watermark IS NULL OR datetime(state.watermark)<=datetime($at)
    MATCH (n:Entity {org_id:$org,chain_id:$chain})
    SET n.uuid=n.uuid
    WITH state,n WHERE (n.is_latest=true OR (n.deleted_at IS NOT NULL AND datetime(n.deleted_at)<=datetime($at)
        AND NOT EXISTS { MATCH (newer:Entity {org_id:$org,chain_id:$chain}) WHERE newer.is_latest=true OR newer.version>n.version }))
        AND (n.last_transition_at IS NULL OR datetime(n.last_transition_at)<=datetime($at))
    SET n.deleted_at=coalesce(n.deleted_at,$at),n.deleted_by=coalesce(n.deleted_by,$by),
        n.deletion_reason=coalesce(n.deletion_reason,$reason),n.is_latest=false,
        n.last_transition_at=$at,state.watermark=$at
    RETURN count(n)>0 AS ok";

// Membership lists are index aligned; the observing collection's entry is
// replaced (or appended) so the rest of the set is untouched.
const OBSERVE_ENTITY: &str = "MATCH (n:Entity {org_id:$org,chain_id:$chain,is_latest:true})
    SET n.uuid=n.uuid
    WITH n WHERE n.is_latest=true AND n.deleted_at IS NULL
        AND (n.last_transition_at IS NULL OR datetime(n.last_transition_at)<=datetime($at))
        AND (n.last_seen_at IS NULL OR datetime(n.last_seen_at)<=datetime($at))
        AND (n.valid_from IS NULL OR datetime(n.valid_from)<=datetime($at))
    WITH n,coalesce(n.collection_members,[]) AS ms,coalesce(n.collection_generations,[]) AS gs
    WITH n,ms,gs,[i IN range(0,size(ms)-1) WHERE ms[i]<>$member] AS keep
    SET n.last_seen_at=$at,n.sync_generation=coalesce($generation,n.sync_generation),n.last_seen_snapshot_id=$snapshot,
        n.collection_members=CASE WHEN $member IS NULL THEN n.collection_members ELSE [i IN keep|ms[i]]+[$member] END,
        n.collection_generations=CASE WHEN $member IS NULL THEN n.collection_generations ELSE [i IN keep|gs[i]]+[$member_generation] END
    RETURN true AS ok";

const RELEASE_MEMBERSHIP: &str = "MATCH (n:Entity {org_id:$org,uuid:$uuid,is_latest:true})
    WHERE n.deleted_at IS NULL
    WITH n,coalesce(n.collection_members,[]) AS ms,coalesce(n.collection_generations,[]) AS gs
    WITH n,ms,gs,[i IN range(0,size(ms)-1) WHERE ms[i]<>$member] AS keep
    SET n.collection_members=[i IN keep|ms[i]],n.collection_generations=[i IN keep|gs[i]]
    RETURN true AS ok";

const OBSERVE_ENTITIES: &str = "UNWIND $rows AS row
    OPTIONAL MATCH (n:Entity {org_id:$org,chain_id:row.chain,is_latest:true})
    WHERE n.deleted_at IS NULL
    WITH row,collect(n) AS ns
    WHERE size(ns)=1
    UNWIND ns AS n
    SET n.uuid=n.uuid
    WITH row,n WHERE n.is_latest=true AND n.deleted_at IS NULL
        AND (n.last_transition_at IS NULL OR datetime(n.last_transition_at)<=datetime(row.at))
        AND (n.last_seen_at IS NULL OR datetime(n.last_seen_at)<=datetime(row.at))
        AND (n.valid_from IS NULL OR datetime(n.valid_from)<=datetime(row.at))
    WITH row,n,coalesce(n.collection_members,[]) AS ms,coalesce(n.collection_generations,[]) AS gs
    WITH row,n,ms,gs,[i IN range(0,size(ms)-1) WHERE ms[i]<>row.member] AS keep
    SET n.last_seen_at=row.at,n.sync_generation=coalesce(row.generation,n.sync_generation),n.last_seen_snapshot_id=row.snapshot,
        n.collection_members=CASE WHEN row.member IS NULL THEN n.collection_members ELSE [i IN keep|ms[i]]+[row.member] END,
        n.collection_generations=CASE WHEN row.member IS NULL THEN n.collection_generations ELSE [i IN keep|gs[i]]+[row.member_generation] END
    RETURN true AS ok";

const CHECK_EDGE_IDENTITY: &str = "OPTIONAL MATCH (s)-[r:RELATES_TO {uuid:$uuid}]->(t)
    WITH collect({r:r,s:s,t:t}) AS rs
    WHERE all(x IN rs
    WHERE x.r IS NULL OR (type(x.r)='RELATES_TO' AND x.r.org_id=$org AND x.s.org_id=$org AND x.t.org_id=$org AND x.s.chain_id=$source AND x.t.chain_id=$target))
    RETURN true AS ok";

// Lock both endpoints before rechecking liveness: MATCH may have read them
// before waiting for a concurrent deletion or version transition to commit.
fn upsert_edge() -> String {
    format!("MATCH (s:Entity {{org_id:$org,chain_id:$source,is_latest:true}}), (t:Entity {{org_id:$org,chain_id:$target,is_latest:true}})
    SET s.uuid=s.uuid,t.uuid=t.uuid
    WITH s,t
    WHERE s.is_latest=true AND t.is_latest=true AND s.deleted_at IS NULL AND t.deleted_at IS NULL
        AND (s.last_transition_at IS NULL OR datetime(s.last_transition_at)<=datetime(coalesce($props.last_seen_at,$props.valid_from)))
        AND (t.last_transition_at IS NULL OR datetime(t.last_transition_at)<=datetime(coalesce($props.last_seen_at,$props.valid_from)))
    OPTIONAL MATCH (s)-[existing:RELATES_TO {{uuid:$uuid}}]->(t)
    WITH s,t,existing IS NULL AS creating
    MERGE (s)-[r:RELATES_TO {{uuid:$uuid}}]->(t)
    SET r.uuid=r.uuid
    WITH r,creating WHERE creating OR ({guard})
    WITH r,any(k IN ['name','description'] WHERE k IN keys($props) AND coalesce(r[k]<>$props[k],(r[k] IS NULL)<>($props[k] IS NULL))) AS changed
    FOREACH (_ IN CASE WHEN changed THEN [1] ELSE [] END | REMOVE r.embedding,r.embedding_model,r.embedding_text_version,r.embedding_content_hash)
    SET r += $props,r.uuid=$uuid,r.org_id=$org,r.source_chain_id=$source,r.target_chain_id=$target
    WITH r",guard=history::edge())
}

const CANCEL_EDGE: &str = "MATCH (s:Entity {org_id:$org})-[r:RELATES_TO {org_id:$org,uuid:$uuid}]->(t:Entity {org_id:$org})
    SET r.uuid=r.uuid
    WITH r WHERE r.cancelled_at IS NULL AND r.deleted_at IS NULL
      AND (r.invalid_at IS NULL OR datetime(r.invalid_at)>=datetime(r.valid_from))
      AND (r.valid_to IS NULL OR datetime(r.valid_to)>=datetime(r.valid_from))
      AND datetime($at)<datetime(r.valid_from)
      AND (r.last_seen_at IS NULL OR datetime(r.last_seen_at)<=datetime($observed))
      AND (r.last_transition_at IS NULL OR datetime(r.last_transition_at)<=datetime($observed))
    SET r.cancelled_at=$at,r.cancellation_snapshot_id=$snapshot,r.cancellation_context=$context,r.is_latest=false,r.last_transition_at=$observed
    RETURN true AS ok";

fn update_edge() -> String {
    format!("MATCH (s:Entity {{org_id:$org}})-[r:RELATES_TO {{org_id:$org,uuid:$uuid}}]->(t:Entity {{org_id:$org}})
    SET r.uuid=r.uuid
    WITH r WHERE {guard}
    WITH r,any(k IN ['name','description'] WHERE k IN keys($props) AND coalesce(r[k]<>$props[k],(r[k] IS NULL)<>($props[k] IS NULL))) AS changed
    SET r += $props
    FOREACH (_ IN CASE WHEN changed THEN [1] ELSE [] END | REMOVE r.embedding,r.embedding_model,r.embedding_text_version,r.embedding_content_hash)
    WITH r",guard=history::edge())
}

const CHECK_REPOINT: &str = "MATCH (old:Entity {org_id:$org,uuid:$old,chain_id:$chain}), (new:Entity {org_id:$org,uuid:$new,chain_id:$chain})
    SET old.uuid=old.uuid,new.uuid=new.uuid
    WITH old,new WHERE new.previous_version_uuid=old.uuid AND new.version>old.version
        AND datetime(new.valid_from)>=datetime(old.valid_from)
    OPTIONAL MATCH (old)-[r:RELATES_TO]-(other)
    WITH old,new,collect({r:r,other:other}) AS rs
    WHERE all(x IN rs
    WHERE x.r IS NULL OR (x.r.org_id=$org AND x.other:Entity AND x.other.org_id=$org))
    RETURN true AS ok";

const REPOINT_OUTGOING: &str = "MATCH (old:Entity {org_id:$org,uuid:$old})-[r:RELATES_TO {org_id:$org}]->(t:Entity {org_id:$org}), (new:Entity {org_id:$org,uuid:$new})
    WITH new,t,properties(r) AS props,r
    DELETE r
    CREATE (new)-[replacement:RELATES_TO]->(t)
    SET replacement=props
    RETURN count(*) >= 0 AS ok";

const REPOINT_INCOMING: &str = "MATCH (s:Entity {org_id:$org})-[r:RELATES_TO {org_id:$org}]->(old:Entity {org_id:$org,uuid:$old}), (new:Entity {org_id:$org,uuid:$new})
    WITH s,new,properties(r) AS props,r
    DELETE r
    CREATE (s)-[replacement:RELATES_TO]->(new)
    SET replacement=props
    RETURN count(*) >= 0 AS ok";

const CARRY_ALIASES: &str =
    "MATCH (old:Entity {org_id:$org,uuid:$old}), (new:Entity {org_id:$org,uuid:$new})
    SET new.identity_hashes=reduce(hashes=coalesce(new.identity_hashes,[]),h IN [old.identity_hash]+coalesce(old.identity_hashes,[]) | CASE WHEN h IS NULL OR h IN hashes THEN hashes ELSE hashes+[h] END),
        new.additional_key_properties=coalesce(new.additional_key_properties,old.additional_key_properties)
    RETURN true AS ok";

const CHECK_OBSERVATION_IDENTITY: &str = "OPTIONAL MATCH (s)-[r:MENTIONS {uuid:$uuid}]->(t)
    WITH collect({r:r,s:s,t:t}) AS rs
    WHERE all(x IN rs
    WHERE x.r IS NULL OR (type(x.r)='MENTIONS' AND x.r.org_id=$org AND x.s.uuid=$snapshot AND x.t.uuid=$entity))
    RETURN true AS ok";

const CHECK_OBSERVATION_IDENTITIES: &str = "UNWIND $rows AS row
    OPTIONAL MATCH (s)-[r:MENTIONS {uuid:row.uuid}]->(t)
    WITH row,collect({r:r,s:s,t:t}) AS rs
    WHERE all(x IN rs
    WHERE x.r IS NULL OR (type(x.r)='MENTIONS' AND x.r.org_id=$org AND x.s.uuid=row.snapshot AND x.t.uuid=row.entity))
    RETURN true AS ok";

/// Accepted attribute adjudications travel on the observation link as one
/// JSON string; a link without any keeps no property (null removes it).
fn reconciliations_property(
    reconciliations: &[kg_core::runtime::stage_output::AttributeReconciliation],
) -> serde_json::Value {
    if reconciliations.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::Value::String(
            serde_json::to_string(reconciliations).expect("reconciliation record serializes"),
        )
    }
}

const RECORD_OBSERVATION: &str = "MATCH (s:Snapshot {org_id:$org,uuid:$snapshot}), (n:Entity {org_id:$org,uuid:$entity,chain_id:$chain})
    MERGE (s)-[r:MENTIONS {uuid:$uuid}]->(n)
    SET r.org_id=$org,r.observed_at=$at,r.reconciliations=$reconciliations
    RETURN true AS ok";

const RECORD_OBSERVATIONS: &str = "UNWIND $rows AS row
    MATCH (s:Snapshot {org_id:$org,uuid:row.snapshot}), (n:Entity {org_id:$org,uuid:row.entity,chain_id:row.chain})
    MERGE (s)-[r:MENTIONS {uuid:row.uuid}]->(n)
    SET r.org_id=$org,r.observed_at=row.at,r.reconciliations=row.reconciliations
    RETURN true AS ok";

const SET_RELATIONSHIP_EMBEDDING: &str =
    "MATCH (:Entity {org_id:$org})-[r:RELATES_TO {org_id:$org,uuid:$uuid}]->(:Entity {org_id:$org})
    SET r.embedding=$values,r.embedding_model=$model,
        r.embedding_text_version=$text_version,r.embedding_content_hash=$content_hash
    RETURN true AS ok";

const SET_EMBEDDING: &str = "MATCH (n:Entity {org_id:$org,uuid:$uuid,is_latest:true})
    WHERE n.deleted_at IS NULL
    SET n.embedding=$values,n.embedding_model=$model,
        n.embedding_text_version=$text_version,n.embedding_content_hash=$content_hash
    RETURN true AS ok";

const SET_EMBEDDINGS: &str = "UNWIND $rows AS row
    OPTIONAL MATCH (n:Entity {org_id:$org,uuid:row.uuid,is_latest:true})
    WHERE n.deleted_at IS NULL
    WITH row,collect(n) AS ns
    WHERE size(ns)=1
    UNWIND ns AS n
    SET n.embedding=row.values,n.embedding_model=row.model,
        n.embedding_text_version=row.text_version,n.embedding_content_hash=row.content_hash
    RETURN true AS ok";

// Empty partial metadata asserts nothing; version inheritance keeps the prior watermark.
const APPLY_ENTITY_METADATA: &str = "MATCH (n:Entity {org_id:$org,uuid:$uuid})
SET n.uuid=n.uuid
WITH n WHERE n.is_latest=true AND n.deleted_at IS NULL
OPTIONAL MATCH (p:Entity {org_id:$org,uuid:$previous})
WITH n,p WHERE $previous IS NULL OR (p.chain_id=n.chain_id AND n.previous_version_uuid=p.uuid)
WITH n, CASE WHEN $previous IS NULL OR n.metadata_observed_at IS NOT NULL THEN properties(n) ELSE properties(p) END AS base
WITH n,base,reduce(result=[], label IN (CASE WHEN $replace THEN [] ELSE coalesce(base.labels,[]) END + $labels) |
    CASE WHEN label IN result THEN result ELSE result+[label] END) AS labels
WITH n,base,labels,NOT $replace AND size(keys($tags))=0 AND size($labels)=0 AS noop
WITH n,base,labels,noop,(noop OR coalesce(n.metadata_observed_at IS NULL OR datetime(n.metadata_observed_at)<datetime($at)
   OR (datetime(n.metadata_observed_at)=datetime($at) AND n.metadata_signature=$signature),false))
   AND coalesce(n.reference_exclusions_observed_at IS NULL OR datetime(n.reference_exclusions_observed_at)<datetime($at)
   OR (datetime(n.reference_exclusions_observed_at)=datetime($at) AND n.reference_exclusions=$reference_exclusions AND coalesce(n.profile_contract,'')=coalesce($profile_contract,'')),false) AS compatible
FOREACH (apply IN CASE WHEN compatible THEN [1] ELSE [] END |
FOREACH (key IN CASE WHEN $replace THEN [key IN keys(n) WHERE key STARTS WITH 'tag_'] ELSE [] END | SET n[key]=null)
FOREACH (key IN CASE WHEN NOT $replace AND $previous IS NOT NULL AND n.metadata_observed_at IS NULL THEN [key IN keys(base) WHERE key STARTS WITH 'tag_'] ELSE [] END | SET n[key]=base[key])
SET n += $tags, n.labels=labels,n.reference_exclusions=$reference_exclusions,n.reference_exclusions_observed_at=$at,n.profile_contract=$profile_contract
FOREACH (write IN CASE WHEN NOT noop THEN [1] ELSE [] END |
SET n.metadata_observed_at=$at, n.metadata_signature=$signature)
FOREACH (inherit IN CASE WHEN noop AND $previous IS NOT NULL AND n.metadata_observed_at IS NULL THEN [1] ELSE [] END |
SET n.metadata_observed_at=base.metadata_observed_at, n.metadata_signature=base.metadata_signature))
RETURN compatible AS ok, NOT compatible AS metadata_conflict";

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use kg_core::traits::graph_backend::GraphEmbedding;

    fn entity(uuid: Uuid, chain: Uuid) -> GraphMutation {
        GraphMutation::UpsertEntity {
            uuid,
            properties: json!({"chain_id": chain, "name": "api", "namespace":"prod", "entity_type":"Service", "is_latest": true})
                .as_object()
                .unwrap()
                .clone(),
        }
    }

    fn observation(snapshot: Uuid, entity: Uuid) -> GraphMutation {
        GraphMutation::RecordObservation {
            uuid: Uuid::new_v5(&snapshot, entity.as_bytes()),
            snapshot_uuid: snapshot,
            entity_uuid: entity,
            entity_chain_id: entity,
            observed_at: Utc::now(),
            reconciliations: Vec::new(),
        }
    }

    /// A representative node batch: 500 created entities, each observed in
    /// one of 10 snapshots and embedded, plus 200 relationships.
    fn representative() -> Vec<GraphMutation> {
        let mut batch = Vec::new();
        let snapshots: Vec<Uuid> = (0..10).map(|_| Uuid::new_v4()).collect();
        for snapshot in &snapshots {
            batch.push(GraphMutation::UpsertSnapshot {
                uuid: *snapshot,
                properties: Default::default(),
            });
        }
        let entities: Vec<Uuid> = (0..500).map(|_| Uuid::new_v4()).collect();
        for uuid in &entities {
            batch.push(entity(*uuid, *uuid));
        }
        for uuid in &entities {
            batch.push(GraphMutation::ObserveEntity {
                chain_id: *uuid,
                observed_at: Utc::now(),
                sync_generation: Some(1),
                snapshot_id: None,
                collection: None,
            });
        }
        for (i, uuid) in entities.iter().enumerate() {
            batch.push(observation(snapshots[i % 10], *uuid));
        }
        for uuid in &entities {
            batch.push(GraphMutation::SetEmbedding {
                uuid: *uuid,
                embedding: GraphEmbedding {
                    model: "m".into(),
                    values: vec![0.5, 0.25],
                },
                text_version: kg_core::embedding::TEXT_VERSION.into(),
                content_hash: "h".into(),
            });
        }
        for pair in entities.windows(2).take(200) {
            batch.push(GraphMutation::UpsertEdge {
                uuid: Uuid::new_v4(),
                source_chain_id: pair[0],
                target_chain_id: pair[1],
                properties: json!({"name": "DEPENDS_ON", "is_latest": true})
                    .as_object()
                    .unwrap()
                    .clone(),
            });
        }
        batch
    }

    #[test]
    fn unresolved_slots_are_bounded_and_grouped_without_changing_order_for_repeated_slots() {
        let source = Uuid::new_v4();
        let writes: Vec<_> = (0..6400)
            .map(|index| GraphMutation::RecordUnresolvedReferences {
                source_chain_id: source,
                slot: format!("field-{index}"),
                decided_at: chrono::Utc::now(),
                decision_id: Uuid::new_v4(),
                entries: vec![],
            })
            .collect();
        let compiled = mutations("org", &writes).unwrap();
        assert_eq!(compiled.len(), 13);
        assert_eq!(
            compiled
                .iter()
                .map(|query| query.expected_rows)
                .sum::<usize>(),
            6400
        );
        assert!(compiled.iter().all(|query| query.expected_rows <= 500));
        assert_eq!(
            mutations("org", &[writes[0].clone(), writes[0].clone()])
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn cancellation_is_scoped_fenced_and_keeps_validity_bounds() {
        let writes = mutations(
            "org",
            &[GraphMutation::CancelEdge {
                uuid: Uuid::new_v4(),
                cancelled_at: "2026-01-01T00:00:00Z".parse().unwrap(),
                cancellation_snapshot_id: Some(Uuid::new_v4()),
                cancellation_context: None,
                observed_at: "2026-01-02T00:00:00Z".parse().unwrap(),
            }],
        )
        .unwrap();
        assert_eq!(writes.len(), 1);
        let query = &writes[0].statement;
        assert!(query.contains("datetime($at)<datetime(r.valid_from)"));
        assert!(query.contains("r.cancelled_at IS NULL"));
        assert!(query.contains("r.last_transition_at)<=datetime($observed)"));
        assert!(!query.contains("r.valid_from="));
        assert!(!query.contains("r.valid_to="));
    }

    #[test]
    fn relationship_embedding_write_targets_exact_scoped_version_even_after_ending() {
        assert!(SET_RELATIONSHIP_EMBEDDING.contains("r:RELATES_TO {org_id:$org,uuid:$uuid}"));
        assert_eq!(
            SET_RELATIONSHIP_EMBEDDING
                .matches(":Entity {org_id:$org}")
                .count(),
            2
        );
        for visibility_field in ["is_latest", "valid_to", "invalid_at", "deleted_at"] {
            assert!(!SET_RELATIONSHIP_EMBEDDING.contains(visibility_field));
        }
    }

    #[test]
    fn grouping_cuts_statement_count_and_keeps_row_expectations() {
        let batch = representative();
        let single = mutations_ungrouped("org", &batch).unwrap();
        let grouped = mutations("org", &batch).unwrap();
        // 10 snapshots + 500 entities + 500 observes + 2*500 links + 500 embeddings + 2*200 edges.
        assert_eq!(single.len(), 10 + 500 + 500 + 1000 + 500 + 400);
        // 1 + 1 + 1 + 2 + 1 grouped statements, then 400 ordered edge statements.
        assert_eq!(grouped.len(), 6 + 400);
        assert_eq!(grouped[0].expected_rows, 10);
        assert_eq!(grouped[1].expected_rows, 500);
        assert!(grouped[1].statement.starts_with("UNWIND $rows"));
        assert_eq!(grouped[1].parameters["rows"].as_array().unwrap().len(), 500);
        assert!(grouped[6..].iter().all(|w| w.expected_rows == 1));
        eprintln!(
            "representative batch: {} single statements, {} grouped statements",
            single.len(),
            grouped.len()
        );
    }

    #[test]
    fn repeated_keys_and_kind_changes_break_groups() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let batch = [
            entity(a, a),
            entity(a, a),
            entity(b, b),
            GraphMutation::ObserveEntity {
                chain_id: a,
                observed_at: Utc::now(),
                sync_generation: None,
                snapshot_id: None,
                collection: None,
            },
            entity(b, b),
        ];
        let grouped = mutations("org", &batch).unwrap();
        // [a] then [a, b] then observe then [b]: a duplicate key starts a new group.
        assert_eq!(grouped.len(), 4);
        assert_eq!(grouped[0].expected_rows, 1);
        assert_eq!(
            grouped[0].statement,
            crate::identity::sync(&upsert_entity())
        );
        assert_eq!(grouped[1].expected_rows, 2);
        assert_eq!(grouped[2].statement, OBSERVE_ENTITY);
        assert_eq!(
            grouped[3].statement,
            crate::identity::sync(&upsert_entity())
        );
    }

    #[test]
    fn invalid_mutations_are_rejected_before_preparation() {
        let bad = GraphMutation::SetEmbedding {
            uuid: Uuid::nil(),
            embedding: GraphEmbedding {
                model: String::new(),
                values: vec![1.0],
            },
            text_version: kg_core::embedding::TEXT_VERSION.into(),
            content_hash: "h".into(),
        };
        assert!(mutations("org", &[bad]).is_err());
        assert!(mutations(" ", &[entity(Uuid::nil(), Uuid::nil())]).is_err());
    }
    #[test]
    fn metadata_groups_independent_versions_but_preserves_inheritance_and_repeats() {
        let make = |uuid, previous_uuid| GraphMutation::ApplyEntityMetadata {
            profile_contract: None,
            reference_exclusions: vec![],
            uuid,
            previous_uuid,
            tags: Default::default(),
            labels: vec![],
            replace: false,
            observed_at: chrono::Utc::now(),
        };
        let first = make(Uuid::new_v4(), None);
        let other = make(Uuid::new_v4(), None);
        let inherited = make(Uuid::new_v4(), Some(Uuid::new_v4()));
        let grouped = mutations("org", &[first.clone(), other.clone()]).unwrap();
        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped[0].expected_rows, 2);
        assert!(!grouped[0].statement.contains("$reference_exclusions"));
        assert!(grouped[0].statement.contains("row.reference_exclusions"));
        for query in &grouped {
            for suffix in query.statement.split('$').skip(1) {
                let name: String = suffix
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                    .collect();
                assert!(
                    query.parameters.get(&name).is_some(),
                    "unbound parameter {name} in grouped metadata"
                );
            }
        }
        assert_eq!(
            mutations_ungrouped("org", &[first.clone(), other.clone()])
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            mutations("org", &[first.clone(), first.clone()])
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            mutations("org", &[first, inherited, other]).unwrap().len(),
            3
        );
    }

    #[test]
    fn metadata_signature_ignores_map_and_label_order_but_preserves_intent() {
        let uuid = Uuid::new_v4();
        let make = |tags, labels, replace| GraphMutation::ApplyEntityMetadata {
            profile_contract: None,
            reference_exclusions: vec![],
            uuid,
            previous_uuid: None,
            tags,
            labels,
            replace,
            observed_at: chrono::Utc::now(),
        };
        let a = make(
            [("a".into(), "1".into()), ("b".into(), "2".into())].into(),
            vec!["infra".into(), "compute".into()],
            false,
        );
        let b = make(
            [("b".into(), "2".into()), ("a".into(), "1".into())].into(),
            vec!["compute".into(), "infra".into(), "infra".into()],
            false,
        );
        let c = make(
            [("a".into(), "1".into()), ("b".into(), "2".into())].into(),
            vec!["infra".into(), "compute".into()],
            true,
        );
        let sig =
            |mutation| mutations("org", &[mutation]).unwrap()[0].parameters["signature"].clone();
        assert_eq!(sig(a.clone()), sig(b));
        assert_ne!(sig(a), sig(c));
    }
}
