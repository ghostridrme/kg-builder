//! Creation revisions and effective intervals are independent orders.
use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use kg_core::{
    errors::StageError,
    runtime::stage_output::{ConnectorScope, PairBaseline, StoredRelationship},
    traits::{relationship_timeline, GraphProperties},
};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug)]
pub(crate) struct TimelineVersion {
    pub relationship: StoredRelationship,
}

#[derive(Debug)]
pub(crate) struct ChainTimeline {
    /// Sorted by creation revision, not effective start.
    pub versions: Vec<TimelineVersion>,
}

impl ChainTimeline {
    pub fn revision_head(&self) -> &TimelineVersion {
        // Construction rejects empty chains.
        &self.versions[self.versions.len() - 1]
    }

    pub fn effective_at(&self, at: DateTime<Utc>) -> Option<&TimelineVersion> {
        self.versions.iter().find(|version| {
            let value = &version.relationship;
            value.cancelled_at.is_none()
                && value.valid_from <= at
                && value.ended_at.is_none_or(|end| at < end)
        })
    }

    fn identity_candidate(&self, at: DateTime<Utc>) -> &StoredRelationship {
        if let Some(version) = self.effective_at(at) {
            return &version.relationship;
        }
        // Gaps retain lineage identity for explicit restoration. A future-only
        // lineage remains identifiable, but the planner must validate its write.
        self.versions
            .iter()
            .filter(|v| v.relationship.cancelled_at.is_none() && v.relationship.valid_from <= at)
            .max_by_key(|v| (v.relationship.valid_from, v.relationship.version))
            .or_else(|| {
                self.versions
                    .iter()
                    .filter(|v| v.relationship.cancelled_at.is_none())
                    .min_by_key(|v| (v.relationship.valid_from, v.relationship.version))
            })
            .map(|v| &v.relationship)
            .unwrap_or(&self.revision_head().relationship)
    }
}

#[derive(Debug)]
pub(crate) struct RelationshipTimeline {
    pub chains: BTreeMap<Uuid, ChainTimeline>,
}

impl RelationshipTimeline {
    pub fn from_pair(pair: &PairBaseline) -> Result<Self, StageError> {
        relationship_timeline::validate(pair.source_chain_id, pair.target_chain_id, &pair.versions)
            .map_err(|_| invalid("invalid stored relationship timeline"))?;
        let mut chains: BTreeMap<Uuid, ChainTimeline> = BTreeMap::new();
        let mut scopes = BTreeMap::new();
        for properties in &pair.versions {
            let producer = (
                text(properties, "producer_namespace")?.to_owned(),
                text(properties, "producer_source")?.to_owned(),
            );
            let relationship = decode(
                pair.source_chain_id,
                pair.target_chain_id,
                properties,
                Some(ConnectorScope {
                    namespace: producer.0.clone(),
                    source: producer.1.clone(),
                }),
            )?;
            if scopes
                .insert(relationship.chain_id, producer.clone())
                .is_some_and(|prior| prior != producer)
            {
                return Err(invalid("relationship lineage changes producer scope"));
            }
            let is_latest = properties
                .get("is_latest")
                .and_then(Value::as_bool)
                .ok_or_else(|| invalid("relationship has no latest flag"))?;
            if is_latest != (relationship.ended_at.is_none() && relationship.cancelled_at.is_none())
            {
                return Err(invalid(
                    "relationship latest flag disagrees with its open interval",
                ));
            }
            chains
                .entry(relationship.chain_id)
                .or_insert_with(|| ChainTimeline {
                    versions: Vec::new(),
                })
                .versions
                .push(TimelineVersion { relationship });
        }
        let states: BTreeMap<_, _> = pair
            .versions
            .iter()
            .filter_map(|properties| {
                properties
                    .get("uuid")
                    .and_then(Value::as_str)
                    .map(|uuid| (uuid, properties))
            })
            .collect();
        if pair
            .live
            .iter()
            .any(|head| !states.contains_key(head.uuid.to_string().as_str()))
        {
            return Err(invalid(
                "live relationship is missing from its complete timeline",
            ));
        }
        for properties in &pair.versions {
            if let Some(previous) = id(properties, "previous_version_uuid")? {
                let own =
                    id(properties, "uuid")?.ok_or_else(|| invalid("missing relationship UUID"))?;
                if previous == own {
                    return Err(invalid("relationship revision points to itself"));
                }
                if let Some(prior) = states.get(previous.to_string().as_str()) {
                    if prior.get("chain_id") != properties.get("chain_id")
                        || prior.get("version").and_then(Value::as_u64)
                            >= properties.get("version").and_then(Value::as_u64)
                    {
                        return Err(invalid(
                            "relationship predecessor is not an earlier creation revision",
                        ));
                    }
                }
            }
        }
        for chain in chains.values_mut() {
            chain.versions.sort_by_key(|v| v.relationship.version);
            let mut revisions = BTreeSet::new();
            let first = &chain.versions[0].relationship;
            for version in &chain.versions {
                let value = &version.relationship;
                if !revisions.insert(value.version) {
                    return Err(invalid(
                        "relationship lineage has duplicate creation revisions",
                    ));
                }
                if value.origin != first.origin || value.identity_hash != first.identity_hash {
                    return Err(invalid("relationship lineage changes trusted identity"));
                }
            }
            let mut effective: Vec<_> = chain
                .versions
                .iter()
                .filter(|v| {
                    v.relationship.cancelled_at.is_none()
                        && v.relationship.ended_at != Some(v.relationship.valid_from)
                })
                .collect();
            effective.sort_by_key(|v| v.relationship.valid_from);
            for pair in effective.windows(2) {
                if pair[0]
                    .relationship
                    .ended_at
                    .is_none_or(|end| end > pair[1].relationship.valid_from)
                {
                    return Err(invalid(
                        "relationship lineage has overlapping effective intervals",
                    ));
                }
            }
        }
        Ok(Self { chains })
    }

    pub fn candidates_at(&self, at: DateTime<Utc>) -> Vec<StoredRelationship> {
        self.chains
            .values()
            .map(|chain| chain.identity_candidate(at).clone())
            .collect()
    }
}

fn invalid(message: &str) -> StageError {
    StageError::StateValidation {
        stage: "edge_resolution".into(),
        message: message.into(),
    }
}

fn text<'a>(properties: &'a GraphProperties, key: &str) -> Result<&'a str, StageError> {
    properties
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| invalid("stored relationship has missing or invalid text metadata"))
}

fn id(properties: &GraphProperties, key: &str) -> Result<Option<Uuid>, StageError> {
    properties
        .get(key)
        .filter(|v| !v.is_null())
        .map(|value| {
            value
                .as_str()
                .and_then(|v| Uuid::parse_str(v).ok())
                .filter(|v| !v.is_nil())
                .ok_or_else(|| invalid("stored relationship has invalid UUID metadata"))
        })
        .transpose()
}

pub(super) fn time(
    properties: &GraphProperties,
    key: &str,
) -> Result<Option<DateTime<Utc>>, StageError> {
    properties
        .get(key)
        .filter(|v| !v.is_null())
        .map(|value| {
            value
                .as_str()
                .and_then(|v| DateTime::parse_from_rfc3339(v).ok())
                .map(|v| v.with_timezone(&Utc))
                .ok_or_else(|| invalid("stored relationship has invalid temporal metadata"))
        })
        .transpose()
}

pub(crate) fn decode(
    source: Uuid,
    target: Uuid,
    properties: &GraphProperties,
    scope: Option<ConnectorScope>,
) -> Result<StoredRelationship, StageError> {
    let valid_from = time(properties, "valid_from")?
        .ok_or_else(|| invalid("stored relationship has no effective start"))?;
    let ended_at = ["valid_to", "invalid_at", "deleted_at"]
        .into_iter()
        .map(|key| time(properties, key))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .min();
    if ended_at.is_some_and(|end| end < valid_from) {
        return Err(invalid(
            "stored relationship ends before its effective start",
        ));
    }
    let confidence = properties
        .get("confidence")
        .and_then(Value::as_f64)
        .filter(|v| v.is_finite() && (0.0..=1.0).contains(v))
        .ok_or_else(|| invalid("stored relationship has invalid confidence"))?
        as f32;
    let optional_text = |key: &str| -> Result<Option<String>, StageError> {
        properties
            .get(key)
            .filter(|v| !v.is_null())
            .map(|value| {
                value
                    .as_str()
                    .filter(|v| !v.trim().is_empty())
                    .map(str::to_owned)
                    .ok_or_else(|| invalid("stored relationship has invalid identity metadata"))
            })
            .transpose()
    };
    let cancelled_at = time(properties, "cancelled_at")?;
    let cancellation_snapshot_id = id(properties, "cancellation_snapshot_id")?;
    let cancellation_context: Option<kg_core::models::CancellationContext> = properties
        .get("cancellation_context")
        .map(|value| {
            serde_json::from_str(
                value
                    .as_str()
                    .ok_or_else(|| invalid("invalid cancellation context"))?,
            )
            .map_err(|_| invalid("invalid cancellation context"))
        })
        .transpose()?;
    if let Some(context) = &cancellation_context {
        context
            .validate()
            .map_err(|_| invalid("invalid cancellation context"))?;
        if let kg_core::models::CancellationContext::Merge { effective_at, .. } = context {
            if Some(*effective_at) != cancelled_at {
                return Err(invalid(
                    "merge cancellation time disagrees with its evidence",
                ));
            }
        }
    }
    if cancelled_at.is_some()
        != (cancellation_snapshot_id.is_some() || cancellation_context.is_some())
        || (cancellation_snapshot_id.is_some() && cancellation_context.is_some())
        || cancelled_at.is_some_and(|at| at >= valid_from)
    {
        return Err(invalid(
            "stored relationship has invalid cancellation evidence",
        ));
    }
    let time_evidence: Option<kg_core::models::relationship_time::RelationshipTimeEvidence> =
        properties
            .get("time_evidence")
            .filter(|value| !value.is_null())
            .map(|value| {
                serde_json::from_str(
                    value
                        .as_str()
                        .ok_or_else(|| invalid("invalid relationship time evidence"))?,
                )
                .map_err(|_| invalid("invalid relationship time evidence"))
            })
            .transpose()?;
    if let Some(evidence) = &time_evidence {
        evidence
            .validate()
            .map_err(|_| invalid("invalid relationship time evidence"))?;
        if matches!(
            evidence.resolved_target,
            Some(kg_core::models::RelationshipTarget::PriorObservation { .. })
        ) {
            return Err(invalid(
                "stored termination has an unresolved observation target",
            ));
        }
    }
    Ok(StoredRelationship {
        time_evidence,
        cancelled_at,
        cancellation_snapshot_id,
        cancellation_context,
        valid_from,
        ended_at,
        uuid: id(properties, "uuid")?.ok_or_else(|| invalid("stored relationship has no UUID"))?,
        chain_id: id(properties, "chain_id")?
            .ok_or_else(|| invalid("stored relationship has no chain UUID"))?,
        identity_hash: optional_text("identity_hash")?,
        cardinality_key: optional_text("cardinality_key")?,
        reference_evidence: match (
            id(properties, "reference_owner_chain_id")?,
            optional_text("reference_owner_namespace")?,
            optional_text("reference_slot")?,
            optional_text("evidence_location")?,
        ) {
            (Some(observing_chain_id), Some(observing_namespace), Some(slot), Some(location)) => {
                let target_key_group = properties
                    .get("target_key_group")
                    .and_then(serde_json::Value::as_array)
                    .ok_or_else(|| invalid("stored reference has invalid target key group"))?
                    .iter()
                    .map(|value| {
                        value
                            .as_str()
                            .map(str::to_owned)
                            .ok_or_else(|| invalid("stored reference has invalid target key group"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let reference_tokens = properties
                    .get("reference_tokens")
                    .and_then(serde_json::Value::as_array)
                    .map(|values| {
                        values
                            .iter()
                            .map(|value| {
                                value.as_str().map(str::to_owned).ok_or_else(|| {
                                    invalid("stored reference has invalid dependency tokens")
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .transpose()?
                    .unwrap_or_default();
                Some(kg_core::models::edges::ReferenceEvidence {
                    component_paths: properties
                        .get("reference_component_paths")
                        .map(|value| {
                            value
                                .as_str()
                                .ok_or_else(|| invalid("invalid reference component paths"))
                                .and_then(|raw| {
                                    serde_json::from_str(raw)
                                        .map_err(|_| invalid("invalid reference component paths"))
                                })
                        })
                        .transpose()?,
                    observing_chain_id,
                    observing_namespace,
                    slot,
                    location,
                    target_key_group,
                    reference_tokens,
                    read_set: Vec::new(),
                    decision: properties
                        .get("reference_decision")
                        .filter(|value| !value.is_null())
                        .map(|value| {
                            value
                                .as_str()
                                .ok_or_else(|| invalid("invalid stored reference decision"))
                                .and_then(|raw| {
                                    serde_json::from_str(raw)
                                        .map_err(|_| invalid("invalid stored reference decision"))
                                })
                        })
                        .transpose()?,
                })
            }
            (None, None, None, None) => None,
            _ => return Err(invalid("stored reference has incomplete owner evidence")),
        },
        origin: serde_json::from_value(
            properties
                .get("origin")
                .cloned()
                .ok_or_else(|| invalid("stored relationship has no origin"))?,
        )
        .map_err(|_| invalid("stored relationship has invalid origin"))?,
        all_properties: kg_core::traits::property_codec::read_properties(properties)
            .map_err(|_| invalid("stored relationship has invalid typed properties"))?,
        first_seen_snapshot_id: id(properties, "first_seen_snapshot_id")?,
        source_chain_id: source,
        target_chain_id: target,
        name: text(properties, "name")?.to_owned(),
        version: properties
            .get("version")
            .and_then(Value::as_u64)
            .filter(|v| *v > 0 && *v <= u32::MAX as u64)
            .ok_or_else(|| invalid("stored relationship has invalid revision"))?
            as u32,
        confidence,
        description: properties
            .get("description")
            .map(|v| {
                v.as_str()
                    .ok_or_else(|| invalid("stored relationship has invalid description"))
            })
            .transpose()?
            .unwrap_or("")
            .to_owned(),
        latest_observation: time(properties, "last_seen_at")?
            .into_iter()
            .chain(time(properties, "last_transition_at")?)
            .max(),
        scope,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn date(day: u32) -> DateTime<Utc> {
        format!("2026-09-{day:02}T00:00:00Z").parse().unwrap()
    }

    fn state(revision: u32, start: u32, end: Option<u32>) -> GraphProperties {
        let mut value = json!({
            "uuid": Uuid::from_u128(u128::from(revision)), "chain_id": Uuid::from_u128(100),
            "version": revision, "is_latest": end.is_none(), "valid_from": date(start),
            "name": "USES", "description": format!("revision {revision}"), "confidence": 1.0,
            "origin": "declared", "identity_hash": "identity", "producer_source": "aws",
            "producer_namespace": "prod", "last_seen_at": date(1)
        })
        .as_object()
        .unwrap()
        .clone();
        if let Some(end) = end {
            value.insert("valid_to".into(), json!(date(end)));
        }
        value
    }

    fn pair(versions: Vec<GraphProperties>) -> PairBaseline {
        PairBaseline {
            source_chain_id: Uuid::from_u128(200),
            target_chain_id: Uuid::from_u128(201),
            live: vec![],
            versions,
        }
    }

    #[test]
    fn temporal_evidence_roundtrips_without_becoming_identity_properties() {
        use kg_core::models::relationship_time::{
            RelationshipTimeBound, RelationshipTimeEvidence, RelationshipTimeOutcome, TimeBasis,
            TimePrecision,
        };
        let evidence = RelationshipTimeEvidence {
            resolved_target: None,
            snapshot_id: Uuid::new_v4(),
            captured_at: date(19),
            outcome: RelationshipTimeOutcome::Inferred,
            start: Some(RelationshipTimeBound {
                at: date(2),
                precision: TimePrecision::Date,
                basis: TimeBasis::Absolute,
                quote: Some("since 2026-09-02".into()),
            }),
            end: None,
        };
        let mut properties = state(1, 2, None);
        properties.insert(
            "time_evidence".into(),
            json!(serde_json::to_string(&evidence).unwrap()),
        );
        let decoded = decode(
            Uuid::from_u128(200),
            Uuid::from_u128(201),
            &properties,
            None,
        )
        .unwrap();
        assert_eq!(decoded.time_evidence, Some(evidence.clone()));
        assert!(!decoded.all_properties.contains_key("time_evidence"));

        let mut pending = evidence;
        pending.end = pending.start.take();
        pending.resolved_target = Some(kg_core::models::RelationshipTarget::PriorObservation {
            observation_uuid: Uuid::new_v4(),
        });
        properties.insert(
            "time_evidence".into(),
            json!(serde_json::to_string(&pending).unwrap()),
        );
        assert!(decode(
            Uuid::from_u128(200),
            Uuid::from_u128(201),
            &properties,
            None
        )
        .is_err());

        properties.insert("time_evidence".into(), json!("{\"snapshot_id\":null}"));
        assert!(decode(
            Uuid::from_u128(200),
            Uuid::from_u128(201),
            &properties,
            None
        )
        .is_err());
    }

    #[test]
    fn selects_effective_interval_separately_from_revision_and_open_tail() {
        let timeline = RelationshipTimeline::from_pair(&pair(vec![
            state(2, 5, None),
            state(3, 4, Some(5)),
            state(1, 1, Some(4)),
        ]))
        .unwrap();
        let chain = &timeline.chains[&Uuid::from_u128(100)];
        assert_eq!(
            chain.revision_head().relationship.scope,
            Some(ConnectorScope {
                namespace: "prod".into(),
                source: "aws".into()
            })
        );
        assert_eq!(chain.revision_head().relationship.version, 3);
        assert_eq!(chain.effective_at(date(6)).unwrap().relationship.version, 2);
        assert_eq!(chain.effective_at(date(3)).unwrap().relationship.version, 1);
        assert_eq!(chain.effective_at(date(4)).unwrap().relationship.version, 3);
        assert_eq!(chain.effective_at(date(5)).unwrap().relationship.version, 2);
        assert_eq!(timeline.candidates_at(date(3))[0].ended_at, Some(date(4)));
    }

    #[test]
    fn lifecycle_cancellation_preserves_typed_evidence() {
        use kg_core::{
            models::CancellationContext,
            traits::{BatchIdentity, BatchKind},
        };
        let context = CancellationContext::Batch {
            batch: BatchIdentity {
                run_id: Uuid::new_v4(),
                kind: BatchKind::Reconciliation,
                index: 2,
            },
        };
        let mut cancelled = state(2, 5, None);
        cancelled.insert("cancelled_at".into(), json!(date(3)));
        cancelled.insert(
            "cancellation_context".into(),
            json!(serde_json::to_string(&context).unwrap()),
        );
        cancelled.insert("is_latest".into(), json!(false));
        let timeline = RelationshipTimeline::from_pair(&pair(vec![cancelled.clone()])).unwrap();
        let chain = &timeline.chains[&Uuid::from_u128(100)];
        assert_eq!(
            chain.revision_head().relationship.cancellation_context,
            Some(context)
        );
        assert!(chain.effective_at(date(6)).is_none());
        cancelled.insert("cancellation_snapshot_id".into(), json!(Uuid::new_v4()));
        assert!(RelationshipTimeline::from_pair(&pair(vec![cancelled])).is_err());
    }

    #[test]
    fn cancelled_schedule_retains_revision_without_visibility_or_boundary() {
        let mut cancelled = state(2, 5, None);
        cancelled.insert("cancelled_at".into(), json!(date(3)));
        cancelled.insert(
            "cancellation_snapshot_id".into(),
            json!(Uuid::from_u128(900)),
        );
        cancelled.insert("is_latest".into(), json!(false));
        let timeline =
            RelationshipTimeline::from_pair(&pair(vec![state(1, 1, Some(4)), cancelled.clone()]))
                .unwrap();
        let chain = &timeline.chains[&Uuid::from_u128(100)];
        assert_eq!(chain.revision_head().relationship.version, 2);
        assert!(chain.effective_at(date(5)).is_none());
        assert_eq!(timeline.candidates_at(date(5))[0].version, 1);
        cancelled.remove("cancellation_snapshot_id");
        assert!(RelationshipTimeline::from_pair(&pair(vec![cancelled])).is_err());
    }

    #[test]
    fn gaps_and_zero_length_versions_keep_identity_without_false_visibility() {
        let timeline = RelationshipTimeline::from_pair(&pair(vec![
            state(1, 1, Some(2)),
            state(2, 3, Some(3)),
            state(3, 5, None),
        ]))
        .unwrap();
        let chain = &timeline.chains[&Uuid::from_u128(100)];
        assert!(chain.effective_at(date(3)).is_none());
        assert!(chain.effective_at(date(4)).is_none());
        assert_eq!(timeline.candidates_at(date(4))[0].version, 2);
        assert_eq!(timeline.candidates_at(date(1))[0].version, 1);
    }

    #[test]
    fn rejects_ambiguous_or_malformed_history() {
        let cases = [
            vec![state(1, 1, Some(5)), state(2, 4, None)],
            vec![state(1, 1, None), state(2, 4, None)],
            vec![state(1, 1, Some(5)), state(1, 5, None)],
            vec![state(1, 5, Some(4))],
        ];
        for versions in cases {
            assert!(RelationshipTimeline::from_pair(&pair(versions)).is_err());
        }
        for (key, value) in [
            ("version", json!(0)),
            ("is_latest", json!(false)),
            ("confidence", json!(2.0)),
            ("producer_source", json!("")),
            ("first_seen_snapshot_id", json!("bad")),
        ] {
            let mut version = state(1, 1, None);
            version.insert(key.into(), value);
            assert!(
                RelationshipTimeline::from_pair(&pair(vec![version])).is_err(),
                "{key}"
            );
        }
        let mut duplicate_revision = state(1, 5, None);
        duplicate_revision.insert("uuid".into(), json!(Uuid::from_u128(99)));
        assert!(RelationshipTimeline::from_pair(&pair(vec![
            state(1, 1, Some(5)),
            duplicate_revision
        ]))
        .is_err());
        let mut predecessor = state(2, 5, None);
        predecessor.insert("previous_version_uuid".into(), json!(Uuid::from_u128(2)));
        assert!(
            RelationshipTimeline::from_pair(&pair(vec![state(1, 1, Some(5)), predecessor]))
                .is_err()
        );
        let mut changed = state(2, 5, None);
        changed.insert("producer_source".into(), json!("github"));
        assert!(
            RelationshipTimeline::from_pair(&pair(vec![state(1, 1, Some(5)), changed])).is_err()
        );
    }

    #[test]
    fn transition_clock_is_independent_of_validity_and_earliest_end_wins() {
        let mut version = state(1, 5, Some(9));
        version.insert("last_transition_at".into(), json!(date(3)));
        version.insert("invalid_at".into(), json!(date(8)));
        let timeline = RelationshipTimeline::from_pair(&pair(vec![version.clone()])).unwrap();
        let stored = &timeline.chains[&Uuid::from_u128(100)]
            .revision_head()
            .relationship;
        assert_eq!(stored.latest_observation, Some(date(3)));
        assert_eq!(stored.ended_at, Some(date(8)));
        version.insert("invalid_at".into(), json!("bad"));
        assert!(RelationshipTimeline::from_pair(&pair(vec![version])).is_err());
    }
}
