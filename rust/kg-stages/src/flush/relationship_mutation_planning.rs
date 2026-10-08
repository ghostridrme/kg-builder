//! Apply resolved relationship lineages in capture order, fencing every read candidate set.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use chrono::{DateTime, Utc};
use uuid::Uuid;

use kg_core::errors::StageError;
use kg_core::models::EntityEdge;
use kg_core::runtime::stage_output::{
    ConnectorScope, PairBaseline, PendingRelationshipDirective, RelationshipBatch,
    RelationshipDirectiveAction, StoredRelationship,
};
use kg_core::traits::{GraphMutation, GraphProperties, Precondition};

use crate::edge::relationship_timeline::RelationshipTimeline;

use super::mutation_plan::{closed_edge, invalid, set_string, set_uuid, Plan, STAGE};

#[path = "relationship_contradictions.rs"]
mod semantic;

#[path = "reference_retirement.rs"]
mod reference_retirement;

/// A persisted relationship version whose content needs an embedding.
#[derive(Debug)]
pub(crate) struct RelationshipEmbeddingTarget {
    pub namespace: String,
    pub uuid: Uuid,
    pub name: String,
    pub description: String,
    /// The pair whose stored relationship this is, unchanged by the batch:
    /// its stored vector may be reusable.
    pub stored_pair: Option<(Uuid, Uuid)>,
}

/// One resolved relationship lineage's current version.
#[derive(Clone, PartialEq)]
struct Version {
    cancelled_at: Option<DateTime<Utc>>,
    valid_from: DateTime<Utc>,
    identity_hash: Option<String>,
    cardinality_key: Option<String>,
    reference_evidence: Option<kg_core::models::edges::ReferenceEvidence>,
    origin: kg_core::models::RelationshipOrigin,
    source_chain_id: Uuid,
    target_chain_id: Uuid,
    all_properties: indexmap::IndexMap<String, kg_core::models::PropertyValue>,
    first_seen_snapshot_id: Option<Uuid>,
    uuid: Uuid,
    version: u32,
    name: String,
    confidence: f32,
    description: String,
    latest_observation: Option<DateTime<Utc>>,
    ended_at: Option<DateTime<Utc>>,
    stored: bool,
    /// The connector scope whose observations may close it, when known.
    scope: Option<ConnectorScope>,
    /// An ended lineage can reopen only after its effective end.
    closed: bool,
}

impl Version {
    fn embedding_target(&self) -> RelationshipEmbeddingTarget {
        RelationshipEmbeddingTarget {
            namespace: self
                .scope
                .as_ref()
                .map(|scope| scope.namespace.clone())
                .unwrap_or_default(),
            uuid: self.uuid,
            name: self.name.clone(),
            description: self.description.clone(),
            stored_pair: self
                .stored
                .then_some((self.source_chain_id, self.target_chain_id)),
        }
    }

    fn stored(stored: &StoredRelationship) -> Self {
        Self {
            cancelled_at: stored.cancelled_at,
            valid_from: stored.valid_from,
            identity_hash: stored.identity_hash.clone(),
            cardinality_key: stored.cardinality_key.clone(),
            reference_evidence: stored.reference_evidence.clone(),
            origin: stored.origin,
            source_chain_id: stored.source_chain_id,
            target_chain_id: stored.target_chain_id,
            all_properties: stored.all_properties.clone(),
            first_seen_snapshot_id: stored.first_seen_snapshot_id,
            uuid: stored.uuid,
            version: stored.version,
            name: stored.name.clone(),
            confidence: stored.confidence,
            description: stored.description.clone(),
            latest_observation: stored.latest_observation,
            ended_at: stored.ended_at,
            stored: true,
            scope: stored.scope.clone(),
            closed: stored.ended_at.is_some(),
        }
    }

    fn written(
        edge: &EntityEdge,
        version: u32,
        scope: &ConnectorScope,
        observed_at: DateTime<Utc>,
    ) -> Self {
        Self {
            cancelled_at: edge.cancelled_at,
            valid_from: edge.valid_from,
            identity_hash: edge.identity_hash.clone(),
            cardinality_key: edge.cardinality_key.clone(),
            reference_evidence: edge.reference_evidence.clone(),
            origin: edge.origin,
            source_chain_id: edge.source_chain_id,
            target_chain_id: edge.target_chain_id,
            all_properties: edge.all_properties.clone(),
            first_seen_snapshot_id: edge.first_seen_snapshot_id,
            uuid: edge.uuid,
            version,
            name: edge.name.clone(),
            confidence: edge.confidence,
            description: edge.description.clone(),
            latest_observation: Some(observed_at),
            ended_at: edge.valid_to,
            stored: false,
            scope: Some(scope.clone()),
            closed: edge.valid_to.is_some(),
        }
    }

    fn same_content(&self, edge: &EntityEdge) -> bool {
        self.name == edge.name
            && (self.confidence - edge.confidence).abs() < 1e-6
            && self.description == edge.description
            && self.all_properties == edge.all_properties
    }
}

/// One observation with the position and scope of its snapshot.
struct Observed<'a> {
    captured_at: DateTime<Utc>,
    edge: &'a EntityEdge,
    position: usize,
    scope: ConnectorScope,
}

/// Plan one relationship batch: preconditions, ordered mutations, counts,
/// and every new version plus unchanged observed heads for embedding planning.
#[cfg(test)]
pub(crate) fn plan_relationships(
    batch: &RelationshipBatch,
) -> Result<(Plan, Vec<RelationshipEmbeddingTarget>), StageError> {
    plan_relationships_for_rule_maintenance(batch, None)
}

pub(crate) fn plan_relationships_for_rule_maintenance(
    batch: &RelationshipBatch,
    retire_reference_owners_at: Option<DateTime<Utc>>,
) -> Result<(Plan, Vec<RelationshipEmbeddingTarget>), StageError> {
    let snapshots: HashMap<_, _> = batch
        .snapshot_nodes
        .iter()
        .enumerate()
        .map(|(position, snapshot)| (snapshot.uuid, (position, snapshot)))
        .collect();
    let mut observations = Vec::new();
    for edge in batch.observed.iter() {
        let snapshot_id = edge
            .last_seen_snapshot_id
            .ok_or_else(|| invalid("relationship has no observing snapshot".into()))?;
        let (position, snapshot) = snapshots.get(&snapshot_id).ok_or_else(|| {
            invalid("relationship observing snapshot is outside this batch".into())
        })?;
        if edge.chain_id.is_nil() || edge.valid_to.is_some_and(|end| end < edge.valid_from) {
            return Err(invalid(
                "relationship has unresolved identity or invalid validity bounds".into(),
            ));
        }
        observations.push(Observed {
            captured_at: snapshot.captured_at,
            edge,
            position: *position,
            scope: ConnectorScope {
                namespace: snapshot.namespace.clone(),
                source: edge.producer_source.clone(),
            },
        });
    }
    observations.sort_by(|a, b| {
        a.captured_at
            .cmp(&b.captured_at)
            .then_with(|| b.edge.confidence.total_cmp(&a.edge.confidence))
            .then_with(|| a.position.cmp(&b.position))
            .then_with(|| a.edge.uuid.cmp(&b.edge.uuid))
    });

    let mut plan = Plan::default();
    // Repair's synthetic effective time cannot authorize stale stored source
    // properties. Fence both replacement versions and same-version observations,
    // including batches that only record uncertainty or retire an old reference.
    for source in &batch.reference_report.source_reads {
        plan.require(Precondition::LatestVersionIs {
            chain_id: source.chain_id,
            uuid: source.version_uuid,
            version: source.version,
        });
        plan.require(Precondition::NotObservedAfter {
            uuid: source.version_uuid,
            observed_at: source.observed_at,
        });
    }
    plan.counts.relationships_declined = batch.reference_report.relationship_declines.len();
    plan.counts.references_attempted = batch.reference_report.attempted;
    plan.counts.references_confirmed = batch.reference_report.confirmed;
    plan.counts.references_unresolved = batch.reference_report.unresolved;
    plan.counts.references_excluded = batch.reference_report.excluded;
    plan.counts.references_incomplete = batch.reference_report.incomplete_sources.len();
    validate_decision_audits(&batch.reference_report.decisions)?;
    let mut guarded_scopes = BTreeMap::new();
    for revision in &batch.reference_report.candidate_revisions {
        if guarded_scopes
            .insert(revision.scope.clone(), revision.revision)
            .is_some_and(|prior| prior != revision.revision)
        {
            return Err(kg_core::errors::StageError::IdentityRevisionChanged);
        }
        plan.require(Precondition::IdentityRevisionIs(revision.clone()));
    }
    // Decision memory: original model decisions become persisted records and
    // reuses bump their originals, in the same batch and receipt as the edges.
    {
        use kg_core::runtime::reference_resolution::{
            DecisionReuse, PersistedDecision, MAX_DECISIONS_PER_STATEMENT,
        };
        let contexts = &batch.reference_report.decision_contexts;
        let mut records = Vec::new();
        let mut reuses = Vec::new();
        for audit in &batch.reference_report.decisions {
            if audit.reason.is_host_refusal() {
                continue;
            }
            if let Some(original) = audit.reused_from.filter(|_| audit.reused) {
                reuses.push(DecisionReuse {
                    original,
                    decision_id: audit.decision_id,
                    at: audit.decided_at,
                    source_version_uuid: Some(audit.source_version_uuid),
                });
                continue;
            }
            let Some(context) = contexts.iter().find(|c| c.decision_id == audit.decision_id) else {
                continue; // carried from a replayed receipt without context: nothing to persist
            };
            records.push(PersistedDecision {
                audit: audit.clone(),
                producer_source: context.producer_source.clone(),
                observing_namespace: context.observing_namespace.clone(),
                source_entity_type: context.source_entity_type.clone(),
                target_type: context.target_type.clone(),
                components: context.components.clone(),
                reference_tokens: context.reference_tokens.clone(),
                candidates: context.candidates.clone(),
                reuse_count: 0,
                last_reused_at: None,
            });
        }
        // Every record page before any reuse page: a reuse in this batch may
        // name an original recorded by this same batch, and a note for a
        // missing original is a silent no-op.
        for page in records.chunks(MAX_DECISIONS_PER_STATEMENT) {
            plan.mutations
                .push(GraphMutation::RecordReferenceDecisions {
                    decisions: page.to_vec(),
                    reuses: Vec::new(),
                });
        }
        for page in reuses.chunks(MAX_DECISIONS_PER_STATEMENT) {
            plan.mutations
                .push(GraphMutation::RecordReferenceDecisions {
                    decisions: Vec::new(),
                    reuses: page.to_vec(),
                });
        }
    }
    // R4: durable unresolved decisions travel with the edges in this batch. Each
    // (source, slot) replaces the source's prior record for that slot so a target
    // appearing later finds the sources waiting on its typed token.
    for slot in &batch.reference_report.unresolved_slots {
        plan.mutations
            .push(GraphMutation::RecordUnresolvedReferences {
                source_chain_id: slot.source_chain_id,
                slot: slot.slot.clone(),
                decided_at: slot.decided_at,
                decision_id: slot.decision_id,
                entries: slot.entries.clone(),
            });
    }
    let mut chains: BTreeMap<Uuid, Vec<Version>> = BTreeMap::new();
    let mut owners = BTreeMap::new();
    // Targets that no longer have a live entity head: an open edge to one is a
    // legacy orphan that supersession must leave alone for reconciliation.
    let orphan_targets: HashSet<Uuid> = batch.baseline.orphan_targets.iter().copied().collect();
    let mut pairs = HashSet::new();
    for pair in &batch.baseline.pairs {
        let key = (pair.source_chain_id, pair.target_chain_id);
        if !pairs.insert(key) {
            return Err(invalid("duplicate pair baseline".into()));
        }
        load_timeline(pair, &mut chains, &mut owners)?;
        let uuids: BTreeSet<_> = pair.live.iter().map(|value| value.uuid).collect();
        if uuids.len() != pair.live.len() {
            return Err(invalid("duplicate live relationship".into()));
        }
        plan.require(Precondition::RelationshipTimelineIs {
            source_chain_id: key.0,
            target_chain_id: key.1,
            versions: pair.versions.clone(),
        });
        plan.require(Precondition::LiveEdgesForPairAre {
            source_chain_id: key.0,
            target_chain_id: key.1,
            uuids: uuids.into_iter().collect(),
        });
    }
    let mut relations = HashSet::new();
    for relation in &batch.baseline.relations {
        if !relations.insert((relation.source_chain_id, relation.name.as_str())) {
            return Err(invalid("duplicate relation baseline".into()));
        }
        plan.require(Precondition::RelationTimelineIs {
            source_chain_id: relation.source_chain_id,
            name: relation.name.clone(),
            versions: relation.versions.clone(),
        });
        plan.require(Precondition::LiveEdgesForRelationAre {
            source_chain_id: relation.source_chain_id,
            name: relation.name.clone(),
            uuids: relation.live.iter().map(|v| v.uuid).collect(),
        });
        let mut grouped = BTreeMap::<Uuid, Vec<GraphProperties>>::new();
        for version in &relation.versions {
            grouped
                .entry(version.target_chain_id)
                .or_default()
                .push(version.properties.clone());
        }
        for (target_chain_id, versions) in grouped {
            load_timeline(
                &PairBaseline {
                    source_chain_id: relation.source_chain_id,
                    target_chain_id,
                    versions,
                    live: vec![],
                },
                &mut chains,
                &mut owners,
            )?;
        }
    }
    for history in &batch.reference_report.source_histories {
        plan.require(Precondition::IncidentHistoryIs {
            chain_id: history.chain_id,
            versions: history.versions.clone(),
        });
    }
    let mut reference_owners = HashSet::new();
    for owner in &batch.baseline.reference_owners {
        if !reference_owners.insert(owner.selector.clone()) {
            return Err(invalid("duplicate reference owner baseline".into()));
        }
        plan.require(Precondition::ReferenceOwnerTimelineIs {
            owner: owner.selector.clone(),
            versions: owner.versions.clone(),
        });
        plan.require(Precondition::LiveEdgesForReferenceOwnerAre {
            owner: owner.selector.clone(),
            uuids: owner.live.iter().map(|edge| edge.uuid).collect(),
        });
        let mut grouped = BTreeMap::<(Uuid, Uuid), Vec<GraphProperties>>::new();
        for version in &owner.versions {
            grouped
                .entry((version.source_chain_id, version.target_chain_id))
                .or_default()
                .push(version.properties.clone());
        }
        for ((source_chain_id, target_chain_id), versions) in grouped {
            let live = owner
                .live
                .iter()
                .filter(|edge| {
                    edge.source_chain_id == source_chain_id
                        && edge.target_chain_id == target_chain_id
                })
                .cloned()
                .collect();
            load_timeline(
                &PairBaseline {
                    source_chain_id,
                    target_chain_id,
                    versions,
                    live,
                },
                &mut chains,
                &mut owners,
            )?;
        }
    }
    semantic::load_candidates(batch, &mut plan, &mut chains, &mut owners)?;
    semantic::validate_assessments(batch)?;
    for observed in &observations {
        let edge = observed.edge;
        if !pairs.contains(&(edge.source_chain_id, edge.target_chain_id)) {
            return Err(invalid(
                "relationship observed without pair baseline".into(),
            ));
        }
        if edge.cardinality_key.is_some()
            && !relations.contains(&(edge.source_chain_id, edge.name.as_str()))
        {
            return Err(invalid(
                "single-target relationship observed without live-set baseline".into(),
            ));
        }
        if let Some(evidence) = &edge.reference_evidence {
            let owner = kg_core::runtime::stage_output::ReferenceOwnerSelector {
                chain_id: evidence.observing_chain_id,
                namespace: evidence.observing_namespace.clone(),
                slot: evidence.slot.clone(),
            };
            if !reference_owners.contains(&owner) {
                return Err(invalid(
                    "reference relationship observed without owner baseline".into(),
                ));
            }
        }
        // R1: a reference decision holds only while every version it was read
        // against is still latest; a changed target or competitor rejects the commit
        // so the runner refreshes eligibility instead of committing stale evidence.
        if let Some(evidence) = &edge.reference_evidence {
            for read in &evidence.read_set {
                plan.require(Precondition::LatestVersionIs {
                    chain_id: read.chain_id,
                    uuid: read.version_uuid,
                    version: read.version,
                });
                // Descriptive properties used as decision evidence can change
                // without a new version (volatile updates), so the version must
                // also not have been observed after the evidence was read.
                if let Some(observed_at) = read.observed_at {
                    plan.require(Precondition::NotObservedAfter {
                        uuid: read.version_uuid,
                        observed_at,
                    });
                }
            }
        }
    }
    let mut directives: Vec<_> = batch.relationship_directives.iter().collect();
    directives.sort_by_key(|directive| {
        (
            directive.captured_at,
            directive.snapshot_id,
            directive.target.version_uuid,
        )
    });
    let mut replacement_ids = HashSet::new();
    for directive in &directives {
        let (_, snapshot) = snapshots
            .get(&directive.snapshot_id)
            .ok_or_else(|| invalid("scheduled amendment snapshot is outside this batch".into()))?;
        if directive.captured_at != snapshot.captured_at
            || directive.scope != ConnectorScope::of(snapshot)
        {
            return Err(invalid(
                "scheduled amendment has inconsistent capture or scope".into(),
            ));
        }
        if !pairs.contains(&(
            directive.target.source_chain_id,
            directive.target.target_chain_id,
        )) {
            return Err(invalid("scheduled amendment has no target timeline".into()));
        }
        if let RelationshipDirectiveAction::Replace {
            replacement_edge_uuid,
        } = directive.action
        {
            if !replacement_ids.insert(replacement_edge_uuid) {
                return Err(invalid(
                    "replacement observation belongs to multiple commands".into(),
                ));
            }
            let replacement = observations
                .iter()
                .find(|o| o.edge.uuid == replacement_edge_uuid)
                .ok_or_else(|| {
                    invalid("scheduled replacement has no replacement observation".into())
                })?;
            if replacement.edge.last_seen_snapshot_id != Some(directive.snapshot_id)
                || replacement.scope != directive.scope
            {
                return Err(invalid(
                    "scheduled replacement crosses observation scope".into(),
                ));
            }
        }
    }
    let mut embeddings = BTreeMap::new();
    let mut directive_index = 0;
    let mut planned_observations = HashMap::<Uuid, Uuid>::new();
    let observed_reference_owners: HashSet<_> = observations
        .iter()
        .filter_map(|observed| observed.edge.reference_evidence.as_ref())
        .map(
            |evidence| kg_core::runtime::stage_output::ReferenceOwnerSelector {
                chain_id: evidence.observing_chain_id,
                namespace: evidence.observing_namespace.clone(),
                slot: evidence.slot.clone(),
            },
        )
        .collect();
    let mut coverage: Vec<_> = batch.reference_report.source_coverage.iter().collect();
    coverage.sort_by_key(|c| c.captured_at);
    let mut coverage_index = 0;
    for observed in observations {
        while coverage_index < coverage.len()
            && coverage[coverage_index].captured_at < observed.captured_at
        {
            reference_retirement::apply(&mut plan, &mut chains, batch, coverage[coverage_index])?;
            coverage_index += 1;
        }
        while directive_index < directives.len()
            && directives[directive_index].captured_at <= observed.captured_at
        {
            cancel_scheduled(&mut plan, &mut chains, &owners, directives[directive_index])?;
            directive_index += 1;
        }
        let mut normalized = observed.edge.clone();
        if let Some(kg_core::models::RelationshipTarget::PriorObservation { observation_uuid }) =
            normalized
                .time_evidence
                .as_ref()
                .and_then(|time| time.resolved_target.clone())
        {
            let uuid = planned_observations
                .get(&observation_uuid)
                .ok_or_else(|| invalid("termination references an unapplied observation".into()))?;
            let target = chains
                .get(&normalized.chain_id)
                .into_iter()
                .flatten()
                .find(|version| version.uuid == *uuid)
                .ok_or_else(|| invalid("planned termination target is missing".into()))?;
            if target.scope.as_ref() != Some(&observed.scope)
                || !target.same_content(&normalized)
                || target.source_chain_id != normalized.source_chain_id
                || target.target_chain_id != normalized.target_chain_id
            {
                return Err(invalid(
                    "planned termination target changed identity or content".into(),
                ));
            }
            normalized.valid_from = target.valid_from;
            normalized.time_evidence.as_mut().unwrap().resolved_target =
                Some(kg_core::models::RelationshipTarget::StoredVersion { uuid: *uuid });
        }
        let semantic = semantic::prepare(
            batch,
            &mut normalized,
            &observed.scope,
            &chains,
            &owners,
            &planned_observations,
        )?;
        let captured_at = observed.captured_at;
        let observed = Observed {
            edge: &normalized,
            ..observed
        };
        let observation_uuid = observed.edge.uuid;
        let replacement = replacement_ids.contains(&observation_uuid);
        let before = plan.mutations.len();
        let applied = apply_observation(
            &mut plan,
            &mut chains,
            &mut owners,
            &mut embeddings,
            observed,
            replacement,
            &orphan_targets,
        )?;
        if let Some(uuid) = applied {
            semantic::apply(
                &mut plan,
                &mut chains,
                semantic,
                uuid,
                &normalized,
                captured_at,
            )?;
            planned_observations.insert(observation_uuid, uuid);
        }
        if replacement && plan.mutations.len() == before {
            return Err(contradiction(
                "replacement observation is stale and cannot be applied".into(),
            ));
        }
    }
    for remaining in &coverage[coverage_index..] {
        reference_retirement::apply(&mut plan, &mut chains, batch, remaining)?;
    }
    for directive in &directives[directive_index..] {
        cancel_scheduled(&mut plan, &mut chains, &owners, directive)?;
    }
    for owner in &batch.baseline.reference_owners {
        if let Some(retire_at) = retire_reference_owners_at {
            for stored in &owner.live {
                close(&mut plan, &Version::stored(stored), retire_at, retire_at)?;
            }
            continue;
        }
        if batch
            .reference_report
            .source_coverage
            .iter()
            .any(|coverage| coverage.chain_id == owner.selector.chain_id)
            || observed_reference_owners.contains(&owner.selector)
            || batch
                .reference_report
                .incomplete_sources
                .contains(&owner.selector.chain_id)
        {
            continue;
        }
        // Compatibility for custom edge producers that supply slot decisions without
        // ReferenceSourceCoverage. The built-in extractor always takes the coverage
        // path above; external producers still require explicit all-not-found proof.
        let Some(decision) = batch
            .reference_report
            .unresolved_slots
            .iter()
            .find(|decision| {
                decision.source_chain_id == owner.selector.chain_id
                    && decision.slot == owner.selector.slot
            })
        else {
            continue;
        };
        let disproved = !decision.entries.is_empty()
            && decision
                .entries
                .iter()
                .all(|entry| entry.reason == "target-not-found");
        if !disproved {
            continue;
        }
        for stored in &owner.live {
            close(
                &mut plan,
                &Version::stored(stored),
                decision.decided_at,
                decision.decided_at,
            )?;
        }
    }
    Ok((plan, embeddings.into_values().collect()))
}

/// Audits committed with a batch: bounded in number and bytes so they cannot
/// break the receipt budget, and never two different records under one
/// deterministic decision id (a conflict is rejected, not resolved by order).
use kg_core::runtime::reference_resolution::{
    MAX_DECISION_AUDITS_PER_BATCH, MAX_DECISION_AUDIT_BYTES_PER_BATCH,
};

fn validate_decision_audits(
    decisions: &[kg_core::runtime::reference_resolution::ReferenceDecisionAudit],
) -> Result<(), StageError> {
    if decisions.len() > MAX_DECISION_AUDITS_PER_BATCH {
        return Err(invalid(format!(
            "batch carries {} reference decision audits; the limit is {MAX_DECISION_AUDITS_PER_BATCH}",
            decisions.len()
        )));
    }
    let mut seen: HashMap<Uuid, &kg_core::runtime::reference_resolution::ReferenceDecisionAudit> =
        HashMap::new();
    let mut bytes = 0usize;
    for audit in decisions {
        match seen.insert(audit.decision_id, audit) {
            Some(previous) if previous != audit => {
                return Err(invalid(format!(
                    "conflicting reference decision audits for {}",
                    audit.decision_id
                )));
            }
            _ => {}
        }
        bytes = bytes.saturating_add(
            serde_json::to_vec(audit)
                .map_err(|_| invalid("reference decision audit cannot be serialized".into()))?
                .len(),
        );
    }
    if bytes > MAX_DECISION_AUDIT_BYTES_PER_BATCH {
        return Err(invalid(format!(
            "reference decision audits total {bytes} bytes; the limit is {MAX_DECISION_AUDIT_BYTES_PER_BATCH}"
        )));
    }
    Ok(())
}

fn load_timeline(
    pair: &PairBaseline,
    chains: &mut BTreeMap<Uuid, Vec<Version>>,
    owners: &mut BTreeMap<Uuid, ConnectorScope>,
) -> Result<(), StageError> {
    let timeline = RelationshipTimeline::from_pair(pair)?;
    let raw_by_uuid: HashMap<_, _> = pair
        .versions
        .iter()
        .filter_map(|properties| {
            properties
                .get("uuid")
                .and_then(serde_json::Value::as_str)
                .map(|uuid| (uuid, properties))
        })
        .collect();
    for (chain_id, chain) in timeline.chains {
        let versions = chains.entry(chain_id).or_default();
        let mut positions: HashMap<_, _> = versions
            .iter()
            .enumerate()
            .map(|(position, version)| (version.uuid, position))
            .collect();
        let mut revisions: HashSet<_> = versions.iter().map(|version| version.version).collect();
        for stored in chain.versions {
            let properties = raw_by_uuid
                .get(stored.relationship.uuid.to_string().as_str())
                .ok_or_else(|| invalid("relationship version has no raw state".into()))?;
            let owner = ConnectorScope {
                source: properties
                    .get("producer_source")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| invalid("relationship has no producer".into()))?
                    .into(),
                namespace: properties
                    .get("producer_namespace")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| invalid("relationship has no producer namespace".into()))?
                    .into(),
            };
            if owners
                .insert(chain_id, owner.clone())
                .is_some_and(|previous| previous != owner)
            {
                return Err(invalid("relationship lineage changes owner".into()));
            }
            let mut version = Version::stored(&stored.relationship);
            version.scope = Some(owner);
            if let Some(position) = positions.get(&version.uuid) {
                if versions[*position] != version {
                    return Err(invalid("conflicting relationship baseline".into()));
                }
            } else {
                if !revisions.insert(version.version)
                    || versions.first().is_some_and(|v| {
                        (v.source_chain_id, v.target_chain_id)
                            != (version.source_chain_id, version.target_chain_id)
                    })
                {
                    return Err(invalid(
                        "relationship revision collision or endpoint mismatch".into(),
                    ));
                }
                positions.insert(version.uuid, versions.len());
                versions.push(version);
            }
        }
        versions.sort_by_key(|version| version.version);
    }
    Ok(())
}

fn cancel_scheduled(
    plan: &mut Plan,
    chains: &mut BTreeMap<Uuid, Vec<Version>>,
    owners: &BTreeMap<Uuid, ConnectorScope>,
    directive: &PendingRelationshipDirective,
) -> Result<(), StageError> {
    let target = &directive.target;
    let version = chains
        .get_mut(&target.chain_id)
        .and_then(|versions| versions.iter_mut().find(|v| v.uuid == target.version_uuid))
        .ok_or_else(|| invalid("scheduled amendment target does not exist".into()))?;
    if version.source_chain_id != target.source_chain_id
        || version.target_chain_id != target.target_chain_id
        || owners.get(&target.chain_id) != Some(&directive.scope)
    {
        return Err(invalid(
            "scheduled amendment target or owner does not match".into(),
        ));
    }
    if directive.effective_at >= version.valid_from {
        return Err(invalid(
            "only a pending relationship can be cancelled".into(),
        ));
    }
    if version.cancelled_at.is_some()
        || version
            .latest_observation
            .is_some_and(|at| at >= directive.captured_at)
    {
        return Err(contradiction(
            "scheduled amendment is stale or conflicts at the same capture time".into(),
        ));
    }
    plan.mutations.push(GraphMutation::CancelEdge {
        uuid: version.uuid,
        cancelled_at: directive.effective_at,
        cancellation_snapshot_id: Some(directive.snapshot_id),
        cancellation_context: None,
        observed_at: directive.captured_at,
    });
    version.cancelled_at = Some(directive.effective_at);
    version.latest_observation = Some(directive.captured_at);
    version.closed = true;
    plan.counts.edges_invalidated += 1;
    Ok(())
}

fn overlaps(start: DateTime<Utc>, end: Option<DateTime<Utc>>, version: &Version) -> bool {
    version.cancelled_at.is_none()
        && end.is_none_or(|end| start < end)
        && version.ended_at.is_none_or(|end| version.valid_from < end)
        && end.is_none_or(|end| version.valid_from < end)
        && version.ended_at.is_none_or(|end| start < end)
}

fn apply_observation(
    plan: &mut Plan,
    chains: &mut BTreeMap<Uuid, Vec<Version>>,
    owners: &mut BTreeMap<Uuid, ConnectorScope>,
    embeddings: &mut BTreeMap<Uuid, RelationshipEmbeddingTarget>,
    observed: Observed<'_>,
    explicit_replacement: bool,
    orphan_targets: &HashSet<Uuid>,
) -> Result<Option<Uuid>, StageError> {
    let edge = observed.edge;
    let at = observed.captured_at;
    let mut successor = edge.clone();
    let termination = edge.time_evidence.as_ref().filter(|time| time.end_only());
    let termination_target = termination.and_then(|time| match time.resolved_target {
        Some(kg_core::models::RelationshipTarget::StoredVersion { uuid }) => Some(uuid),
        _ => None,
    });
    if termination.is_some() && termination_target.is_none() {
        return Err(invalid(
            "termination has no resolved existing target".into(),
        ));
    }
    let selected = chains
        .get(&edge.chain_id)
        .and_then(|versions| {
            versions.iter().find(|v| {
                (termination_target == Some(v.uuid) || edge.valid_to != Some(edge.valid_from))
                    && termination_target.is_none_or(|target| target == v.uuid)
                    && v.cancelled_at.is_none()
                    && v.valid_from <= edge.valid_from
                    && v.ended_at.is_none_or(|end| edge.valid_from < end)
            })
        })
        .cloned();
    if termination_target.is_some()
        && selected.as_ref().is_none_or(|version| {
            version.valid_from != edge.valid_from
                || !version.same_content(edge)
                || edge.valid_to.is_none_or(|end| {
                    end < version.valid_from || version.ended_at.is_some_and(|prior| end > prior)
                })
        })
    {
        return Err(invalid(
            "termination target no longer matches its resolved interval".into(),
        ));
    }
    // A no-op observation can still identify an unchanged stored fact for a later
    // end-only statement. It must not claim that the observation itself was applied.
    let unchanged_target = selected
        .as_ref()
        .filter(|version| {
            version.same_content(edge) && version.scope.as_ref() == Some(&observed.scope)
        })
        .map(|version| version.uuid);
    let explicit_disjoint = edge.valid_to.is_some()
        && chains
            .get(&edge.chain_id)
            .into_iter()
            .flatten()
            .all(|version| !overlaps(edge.valid_from, edge.valid_to, version));
    let revision = chains
        .get(&edge.chain_id)
        .and_then(|versions| versions.last())
        .cloned();
    if let Some(revision) = &revision {
        if (revision.source_chain_id, revision.target_chain_id)
            != (edge.source_chain_id, edge.target_chain_id)
        {
            return Err(invalid("relationship lineage changed endpoints".into()));
        }
        if termination_target.is_none()
            && !explicit_replacement
            && !explicit_disjoint
            && revision.cancelled_at.is_none()
            && revision
                .ended_at
                .is_some_and(|end| end > revision.valid_from && end <= at && edge.valid_from < end)
            && !(revision.valid_from == edge.valid_from
                && revision.ended_at == edge.valid_to
                && revision.same_content(edge))
            && selected
                .as_ref()
                .is_none_or(|selected| selected.uuid == revision.uuid)
        {
            return Ok(unchanged_target);
        }
        let latest = selected.as_ref().unwrap_or(revision).latest_observation;
        if latest.is_some_and(|latest| at < latest) {
            return Ok(unchanged_target);
        }
        if !explicit_disjoint
            && selected.is_none()
            && edge.valid_from < revision.valid_from
            && revision.valid_from <= at
        {
            return Err(invalid(
                "relationship update precedes the current version's effective start".into(),
            ));
        }
    }
    if termination_target.is_none()
        && selected.as_ref().is_some_and(|version| {
            version.valid_from > at
                && version.valid_from == edge.valid_from
                && (!version.same_content(edge)
                    || edge
                        .valid_to
                        .is_some_and(|end| Some(end) != version.ended_at))
        })
    {
        return Err(invalid(
            "changing a pending interval at its scheduled start requires explicit replacement"
                .into(),
        ));
    }
    // A new effective interval may fill a gap or shorten its predecessor, but
    // never overwrites a later scheduled interval without an explicit command.
    if let Some(next) = chains
        .get(&edge.chain_id)
        .into_iter()
        .flatten()
        .filter(|v| {
            v.cancelled_at.is_none()
                && v.valid_from > edge.valid_from
                && v.ended_at.is_none_or(|end| v.valid_from < end)
        })
        .map(|v| v.valid_from)
        .min()
    {
        successor.valid_to = Some(successor.valid_to.map_or(next, |end| end.min(next)));
    }
    if let Some(end) = selected.as_ref().and_then(|version| version.ended_at) {
        successor.valid_to = Some(successor.valid_to.map_or(end, |incoming| incoming.min(end)));
    }
    if termination_target.is_none() && edge.cardinality_key.is_some() {
        let mut others: Vec<_> = chains
            .iter()
            .flat_map(|(chain, versions)| versions.iter().map(move |v| (*chain, v)))
            .filter(|(_, v)| {
                v.reference_evidence
                    .as_ref()
                    .zip(edge.reference_evidence.as_ref())
                    .map_or(v.source_chain_id == edge.source_chain_id, |(prior, current)| {
                        prior.observing_chain_id == current.observing_chain_id
                            && prior.observing_namespace == current.observing_namespace
                            && prior.slot == current.slot
                    })
                    && (v.source_chain_id, v.target_chain_id)
                        != (edge.source_chain_id, edge.target_chain_id)
                    && v.name == edge.name
                    && v.cardinality_key == edge.cardinality_key
                    && v.origin == edge.origin
                    && v.scope.as_ref() == Some(&observed.scope)
                    && overlaps(edge.valid_from, successor.valid_to, v)
                    // A legacy orphan edge to a deleted target is not a live
                    // competitor for this slot; reconciliation closes it.
                    && !orphan_targets.contains(&v.target_chain_id)
            })
            .map(|(chain, version)| (chain, version.clone()))
            .collect();
        if let Some(next) = others
            .iter()
            .map(|(_, version)| version.valid_from)
            .filter(|start| *start > edge.valid_from)
            .min()
        {
            successor.valid_to = Some(successor.valid_to.map_or(next, |end| end.min(next)));
        }
        others.retain(|(_, version)| overlaps(edge.valid_from, successor.valid_to, version));
        if others
            .iter()
            .any(|(_, v)| v.latest_observation.is_none_or(|latest| latest > at))
        {
            return Ok(unchanged_target);
        }
        if others.iter().any(|(_, v)| v.latest_observation == Some(at)) {
            return Err(contradiction(format!(
                "chain {} has {} to two targets captured at {at}",
                edge.source_chain_id, edge.name
            )));
        }
        for (chain_id, version) in others {
            close(plan, &version, edge.valid_from, at)?;
            let stored = chains
                .get_mut(&chain_id)
                .and_then(|versions| versions.iter_mut().find(|v| v.uuid == version.uuid))
                .ok_or_else(|| invalid("cardinality interval vanished".into()))?;
            stored.ended_at = Some(edge.valid_from);
            stored.latest_observation = Some(at);
            stored.closed = true;
            plan.counts.edges_invalidated += 1;
        }
    }

    if !explicit_replacement
        && chains
            .get(&edge.chain_id)
            .into_iter()
            .flatten()
            .any(|version| {
                version.cancelled_at.is_some()
                    && version.latest_observation == Some(at)
                    && successor
                        .valid_to
                        .is_none_or(|end| edge.valid_from < end && version.valid_from < end)
                    && version
                        .ended_at
                        .is_none_or(|end| version.valid_from < end && edge.valid_from < end)
            })
    {
        return Err(contradiction(
            "ordinary observation conflicts with a cancellation at the same capture time".into(),
        ));
    }
    if let Some(version) = selected.as_ref().or_else(|| {
        revision.as_ref().filter(|v| {
            v.cancelled_at.is_none()
                && v.valid_from == edge.valid_from
                && v.ended_at == edge.valid_to
                && v.same_content(edge)
        })
    }) {
        let same_interval = successor.valid_to == version.ended_at;
        if (version.same_content(edge) && same_interval) || version.latest_observation == Some(at) {
            if version.latest_observation == Some(at)
                && (version.name != edge.name
                    || version.all_properties != edge.all_properties
                    || !same_interval)
            {
                return Err(contradiction("one relationship identity has conflicting attributes or meaning at one capture time".into()));
            }
            plan.mutations.push(GraphMutation::UpdateEdge {
                uuid: version.uuid,
                properties: observed_properties(edge, at),
            });
            plan.counts.edges_unchanged += 1;
            let stored = chains
                .get_mut(&edge.chain_id)
                .and_then(|versions| versions.iter_mut().find(|v| v.uuid == version.uuid))
                .ok_or_else(|| invalid("relationship interval vanished".into()))?;
            stored.latest_observation = Some(at);
            embeddings.insert(stored.uuid, stored.embedding_target());
            return Ok(Some(stored.uuid));
        }
    }
    if let Some(version) = &selected {
        if version.same_content(edge) && successor.valid_to.is_some() {
            let end = successor
                .valid_to
                .ok_or_else(|| invalid("missing effective end".into()))?;
            if version.ended_at.is_some_and(|prior| end > prior) {
                return Err(invalid(
                    "observation cannot extend a closed relationship interval".into(),
                ));
            }
            close(plan, version, end, at)?;
            let mut properties = observed_properties(edge, at);
            properties.insert("valid_to".into(), end.to_rfc3339().into());
            plan.mutations.push(GraphMutation::UpdateEdge {
                uuid: version.uuid,
                properties,
            });
            let stored = chains
                .get_mut(&edge.chain_id)
                .and_then(|versions| versions.iter_mut().find(|v| v.uuid == version.uuid))
                .ok_or_else(|| invalid("relationship interval vanished".into()))?;
            stored.ended_at = Some(end);
            stored.closed = true;
            stored.latest_observation = Some(at);
            embeddings.insert(stored.uuid, stored.embedding_target());
            plan.counts.edges_invalidated += 1;
            return Ok(Some(stored.uuid));
        }
        close(plan, version, edge.valid_from, at)?;
        let stored = chains
            .get_mut(&edge.chain_id)
            .and_then(|versions| versions.iter_mut().find(|v| v.uuid == version.uuid))
            .ok_or_else(|| invalid("relationship interval vanished".into()))?;
        stored.ended_at = Some(edge.valid_from);
        stored.closed = true;
        stored.latest_observation = Some(at);
    }
    let owner = owners
        .entry(edge.chain_id)
        .or_insert_with(|| observed.scope.clone())
        .clone();
    let (next, previous) = if let Some(revision) = revision {
        successor.first_seen_snapshot_id = revision
            .first_seen_snapshot_id
            .or(edge.first_seen_snapshot_id);
        plan.counts.edges_updated += 1;
        (
            crate::next_version(revision.version, "persist")?,
            Some(revision.uuid),
        )
    } else {
        plan.counts.edges_created += 1;
        (1, None)
    };
    if chains
        .values()
        .flatten()
        .any(|version| version.uuid == successor.uuid)
    {
        return Err(invalid(
            "new relationship revision reuses an existing UUID".into(),
        ));
    }
    successor.producer_source = owner.source.clone();
    plan.mutations
        .push(upsert(&successor, next, previous, &owner, at));
    let version = Version::written(&successor, next, &owner, at);
    embeddings.insert(version.uuid, version.embedding_target());
    chains.entry(edge.chain_id).or_default().push(version);
    Ok(Some(successor.uuid))
}

/// Complete timeline fences protect finite and historical intervals as well as
/// open tails. A latest-only precondition would guard the wrong revision.
fn close(
    plan: &mut Plan,
    version: &Version,
    effective_end: DateTime<Utc>,
    observed_at: DateTime<Utc>,
) -> Result<(), StageError> {
    if effective_end < version.valid_from || version.ended_at.is_some_and(|end| effective_end > end)
    {
        return Err(invalid(
            "relationship update would extend or invert an interval".into(),
        ));
    }
    if version
        .latest_observation
        .is_some_and(|latest| latest >= observed_at)
    {
        return Err(contradiction(
            "relationship closure is not newer than its last observation".into(),
        ));
    }
    plan.mutations.push(GraphMutation::UpdateEdge {
        uuid: version.uuid,
        properties: {
            let mut properties = closed_edge(effective_end);
            properties.insert("last_transition_at".into(), observed_at.to_rfc3339().into());
            properties
        },
    });
    Ok(())
}

fn upsert(
    edge: &EntityEdge,
    version: u32,
    previous: Option<Uuid>,
    scope: &ConnectorScope,
    observed_at: DateTime<Utc>,
) -> GraphMutation {
    GraphMutation::UpsertEdge {
        uuid: edge.uuid,
        source_chain_id: edge.source_chain_id,
        target_chain_id: edge.target_chain_id,
        properties: {
            let mut props = edge_properties(edge, version, previous, scope);
            props.insert("last_seen_at".into(), observed_at.to_rfc3339().into());
            props
        },
    }
}

fn contradiction(message: String) -> StageError {
    StageError::StateValidation {
        stage: STAGE.into(),
        message: format!("contradictory observations: {message}"),
    }
}

/// Bookkeeping of a re-observation that changed nothing.
fn observed_properties(edge: &EntityEdge, observed_at: DateTime<Utc>) -> GraphProperties {
    let mut props = GraphProperties::new();
    if let Some(evidence) = &edge.time_evidence {
        props.insert(
            "time_evidence".into(),
            serde_json::json!(evidence).to_string().into(),
        );
    }
    if let Some(evidence) = &edge.reference_evidence {
        props.insert(
            "reference_component_paths".into(),
            evidence
                .component_paths
                .as_ref()
                .map(|paths| serde_json::Value::String(serde_json::json!(paths).to_string()))
                .unwrap_or(serde_json::Value::Null),
        );
        props.insert(
            "reference_tokens".into(),
            serde_json::json!(evidence.reference_tokens),
        );
        props.insert("evidence_location".into(), evidence.location.clone().into());
        // A re-observation rebinds the decision audit to the current capture
        // without a new version: provenance bookkeeping, never semantic content.
        // An occurrence that became deterministic clears the stale audit.
        props.insert(
            "reference_decision".into(),
            evidence
                .decision
                .as_ref()
                .map(|decision| serde_json::Value::String(serde_json::json!(decision).to_string()))
                .unwrap_or(serde_json::Value::Null),
        );
    }
    props.insert("last_seen_at".into(), observed_at.to_rfc3339().into());
    if let Some(generation) = edge.sync_generation {
        props.insert("sync_generation".into(), generation.into());
    }
    set_uuid(
        &mut props,
        "last_seen_snapshot_id",
        edge.last_seen_snapshot_id,
    );
    props
}

pub(crate) fn edge_properties(
    edge: &EntityEdge,
    version: u32,
    previous: Option<Uuid>,
    scope: &ConnectorScope,
) -> GraphProperties {
    let mut props = GraphProperties::new();
    props.insert(
        "producer_source".into(),
        edge.producer_source.clone().into(),
    );
    props.insert("producer_namespace".into(), scope.namespace.clone().into());
    props.insert("chain_id".into(), edge.chain_id.to_string().into());
    props.insert(
        "origin".into(),
        match &edge.origin {
            kg_core::models::RelationshipOrigin::Declared => "declared",
            kg_core::models::RelationshipOrigin::Reference => "reference",
            kg_core::models::RelationshipOrigin::Fact => "fact",
        }
        .into(),
    );
    if let Some(key) = &edge.cardinality_key {
        props.insert("cardinality_key".into(), key.clone().into());
    }
    if let Some(hash) = &edge.identity_hash {
        props.insert("identity_hash".into(), hash.clone().into());
    }
    props.insert("name".into(), edge.name.as_str().into());
    props.insert("description".into(), edge.description.as_str().into());
    props.insert("confidence".into(), edge.confidence.into());
    if let Some(evidence) = &edge.time_evidence {
        props.insert(
            "time_evidence".into(),
            serde_json::json!(evidence).to_string().into(),
        );
    }
    set_string(
        &mut props,
        "discovered_by",
        edge.discovered_by.as_deref().unwrap_or(""),
    );
    set_string(
        &mut props,
        "resolved_by",
        edge.resolved_by.as_deref().unwrap_or(""),
    );
    set_string(
        &mut props,
        "source_property",
        edge.source_property.as_deref().unwrap_or(""),
    );
    set_string(
        &mut props,
        "target_identity_field",
        edge.target_identity_field.as_deref().unwrap_or(""),
    );
    if let Some(evidence) = &edge.reference_evidence {
        if let Some(paths) = &evidence.component_paths {
            props.insert(
                "reference_component_paths".into(),
                serde_json::json!(paths).to_string().into(),
            );
        }
        props.insert(
            "reference_owner_chain_id".into(),
            evidence.observing_chain_id.to_string().into(),
        );
        props.insert(
            "reference_owner_namespace".into(),
            evidence.observing_namespace.as_str().into(),
        );
        props.insert("reference_slot".into(), evidence.slot.as_str().into());
        props.insert(
            "evidence_location".into(),
            evidence.location.as_str().into(),
        );
        props.insert(
            "target_key_group".into(),
            serde_json::json!(evidence.target_key_group),
        );
        props.insert(
            "reference_tokens".into(),
            serde_json::json!(evidence.reference_tokens),
        );
        if let Some(decision) = &evidence.decision {
            props.insert(
                "reference_decision".into(),
                serde_json::json!(decision).to_string().into(),
            );
        }
    }
    set_string(
        &mut props,
        "justification",
        edge.justification.as_deref().unwrap_or(""),
    );
    props.insert("version".into(), version.into());
    props.insert(
        "is_latest".into(),
        (edge.valid_to.is_none() && edge.cancelled_at.is_none()).into(),
    );
    if let Some(at) = edge.cancelled_at {
        props.insert("cancelled_at".into(), at.to_rfc3339().into());
    }
    set_uuid(
        &mut props,
        "cancellation_snapshot_id",
        edge.cancellation_snapshot_id,
    );
    if let Some(context) = &edge.cancellation_context {
        // The typed context contains only UUIDs, timestamps and integer batch fields.
        props.insert(
            "cancellation_context".into(),
            serde_json::json!(context).to_string().into(),
        );
    }
    if let Some(end) = edge.valid_to {
        props.insert("valid_to".into(), end.to_rfc3339().into());
    }
    set_uuid(&mut props, "previous_version_uuid", previous);
    props.insert("valid_from".into(), edge.valid_from.to_rfc3339().into());
    if let Some(at) = edge.last_seen_at {
        props.insert("last_seen_at".into(), at.to_rfc3339().into());
    }
    if let Some(generation) = edge.sync_generation {
        props.insert("sync_generation".into(), generation.into());
    }
    set_uuid(
        &mut props,
        "first_seen_snapshot_id",
        edge.first_seen_snapshot_id,
    );
    set_uuid(
        &mut props,
        "last_seen_snapshot_id",
        edge.last_seen_snapshot_id,
    );
    props.insert("created_at".into(), edge.created_at.to_rfc3339().into());
    for (key, value) in &edge.all_properties {
        kg_core::traits::property_codec::write_property(&mut props, key, Some(value));
    }
    props
}

#[cfg(test)]
mod tests;
