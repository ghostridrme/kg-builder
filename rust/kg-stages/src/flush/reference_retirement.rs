//! Close only reference-owned facts disproved by an authoritative property observation.
//! Missing partial properties, exclusions and incomplete/ambiguous discovery preserve facts.
use super::{close, Plan, Version};
use kg_core::{
    errors::StageError,
    runtime::stage_output::{ReferenceSourceCoverage, RelationshipBatch},
};
use std::collections::BTreeMap;
use uuid::Uuid;

pub(super) fn apply(
    plan: &mut Plan,
    chains: &mut BTreeMap<Uuid, Vec<Version>>,
    batch: &RelationshipBatch,
    coverage: &ReferenceSourceCoverage,
) -> Result<(), StageError> {
    if batch
        .reference_report
        .incomplete_sources
        .contains(&coverage.chain_id)
    {
        return Ok(());
    }
    let under =
        |path: &str, prefix: &str| path == prefix || path.starts_with(&format!("{prefix}."));
    for (chain_id, versions) in chains {
        for version in versions {
            let Some(evidence) = &version.reference_evidence else {
                continue;
            };
            if evidence.observing_chain_id != coverage.chain_id
                || evidence.observing_namespace != coverage.namespace
                || version.closed
                || version.cancelled_at.is_some()
                || version.valid_from > coverage.captured_at
            {
                continue;
            }
            let path = crate::edge::reference_extraction::path_without_indexes(&evidence.location);
            if coverage
                .excluded_paths
                .iter()
                .any(|excluded| under(&path, excluded))
                || (!coverage.complete
                    && !coverage.paths.iter().any(|present| under(&path, present)))
            {
                continue;
            }
            // One uncertain member protects the stored fact whose identity value
            // it carries: no partial candidate set can prove absence. A member
            // with another value (a sibling tag naming something else) says
            // nothing about this fact, so it cannot keep it alive. Edges stored
            // before tokens were recorded keep the slot-wide protection.
            if batch
                .reference_report
                .retirement_decisions
                .iter()
                .any(|decision| {
                    decision.source_chain_id == coverage.chain_id
                        && decision.slot == evidence.slot
                        && decision.decided_at == coverage.captured_at
                        && decision.entries.iter().any(|entry| {
                            entry.reason != "target-not-found"
                                && (evidence.reference_tokens.is_empty()
                                    || evidence.reference_tokens.contains(&entry.token))
                        })
                })
            {
                continue;
            }
            if batch.observed.iter().any(|edge| {
                edge.chain_id == *chain_id
                    && edge.last_seen_at == Some(coverage.captured_at)
                    && edge.reference_evidence.as_ref().is_some_and(|observed| {
                        observed.observing_chain_id == coverage.chain_id
                            && observed.slot == evidence.slot
                    })
            }) {
                continue;
            }
            close(plan, version, coverage.captured_at, coverage.captured_at)?;
            plan.counts.edges_invalidated += 1;
            version.ended_at = Some(coverage.captured_at);
            version.latest_observation = Some(coverage.captured_at);
            version.closed = true;
        }
    }
    Ok(())
}
