//! Breadth-first neighbourhood expansion within the request scope.
use crate::engine::{observe, page_diagnostic, selection_diagnostic, truncation_diagnostic};
use kg_core::{errors::BackendError, search::*, traits::SearchBackend};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// Total chains one traversal may visit, seeds included, and the relationship
/// candidates read per hop.
const TRAVERSAL_BUDGET: usize = 500;

/// Visited chains with their hop distance from the nearest seed.
pub(crate) struct Neighborhood {
    pub hits: Vec<SearchHit>,
    pub distances: HashMap<Uuid, usize>,
    pub truncated: bool,
}

/// Breadth-first expansion over stable chains. Every intermediate must be visible
/// in the organization, namespace, and time scope. Entity type restricts results,
/// not intermediates, so a service can reach a database through a queue.
/// Follow either edge direction, with at most 500 chains and 500 edges per hop.
pub(crate) async fn neighborhood(
    embedding_text_version: &str,
    graph: &dyn SearchBackend,
    filter: &SearchFilter,
    seeds: Vec<Uuid>,
    expected_versions: Option<&HashMap<Uuid, Uuid>>,
    config: &SearchConfig,
    diagnostics: &mut Vec<SearchDiagnostic>,
) -> Result<Neighborhood, BackendError> {
    let mut traversal_filter = filter.clone();
    traversal_filter.entity_types.clear();
    let seed_request = NodeSearch {
        embedding_text_version: embedding_text_version.into(),
        filter: traversal_filter.clone(),
        query: NodeQuery::ByChain,
        chain_ids: Some(seeds),
        limit: TRAVERSAL_BUDGET,
        min_score: 0.0,
        projection: NodeProjection::Candidate,
        signals: NodeSignals::default(),
    };
    let mut initial = observe(
        "traversal_seeds",
        config.operation_timeout_ms,
        graph.search_nodes(&seed_request),
        diagnostics,
    )
    .await?;
    page_diagnostic(diagnostics, &initial);
    // Automatic seeds must still be the versions that matched the query.
    if let Some(expected) = expected_versions {
        initial
            .items
            .retain(|hit| expected.get(&hit.chain_id) == Some(&hit.uuid));
        let retained: HashSet<_> = initial.items.iter().map(|hit| hit.chain_id).collect();
        let rejected = expected.len() - retained.len();
        if rejected > 0 {
            initial.truncated = true;
            diagnostics.push(truncation_diagnostic(
                "traversal_seed_version_changed",
                rejected,
            ));
        }
    }
    let mut distances = HashMap::new();
    let mut hits = Vec::new();
    for mut hit in initial.items {
        distances.insert(hit.chain_id, 0);
        hit.graph_distance = Some(0);
        hits.push(hit);
    }
    let mut frontier: Vec<Uuid> = distances.keys().copied().collect();
    frontier.sort();
    let mut truncated = initial.truncated;
    for depth in 1..=config.bfs_max_depth {
        if frontier.is_empty() {
            break;
        }
        let request = EvidenceSearch {
            passage_query: None,
            filter: traversal_filter.clone(),
            query: None,
            chain_ids: Some(frontier),
            limit: TRAVERSAL_BUDGET,
        };
        let edges = observe(
            "traversal_edges",
            config.operation_timeout_ms,
            graph.search_relationships(&request),
            diagnostics,
        )
        .await?;
        page_diagnostic(diagnostics, &edges);
        truncated |= edges.truncated;
        let mut next: Vec<Uuid> = edges
            .items
            .into_iter()
            .flat_map(|e| [e.source_chain_id, e.target_chain_id])
            .filter(|id| !distances.contains_key(id))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        next.sort();
        let budget_start = tokio::time::Instant::now();
        let before_budget = next.len();
        let remaining = TRAVERSAL_BUDGET.saturating_sub(distances.len());
        if next.len() > remaining {
            truncated = true;
            next.truncate(remaining);
            selection_diagnostic(
                diagnostics,
                "traversal_budget",
                budget_start,
                before_budget,
                next.len(),
            );
        }
        if next.is_empty() {
            break;
        }
        let request = NodeSearch {
            embedding_text_version: embedding_text_version.into(),
            filter: traversal_filter.clone(),
            query: NodeQuery::ByChain,
            chain_ids: Some(next),
            limit: TRAVERSAL_BUDGET,
            min_score: 0.0,
            projection: NodeProjection::Candidate,
            signals: NodeSignals::default(),
        };
        let nodes = observe(
            "traversal_nodes",
            config.operation_timeout_ms,
            graph.search_nodes(&request),
            diagnostics,
        )
        .await?;
        page_diagnostic(diagnostics, &nodes);
        truncated |= nodes.truncated;
        frontier = vec![];
        for mut hit in nodes.items {
            if distances.contains_key(&hit.chain_id) {
                continue;
            }
            distances.insert(hit.chain_id, depth);
            frontier.push(hit.chain_id);
            hit.graph_distance = Some(depth);
            // Closer chains score higher; seeds are 1.0, one hop 0.5, two hops 0.33.
            hit.score = 1.0 / (1.0 + depth as f32);
            hit.score_breakdown.insert("bfs".into(), hit.score);
            hits.push(hit);
        }
    }
    hits.retain(|h| filter.entity_types.is_empty() || filter.entity_types.contains(&h.entity_type));
    hits.sort_by(|a, b| {
        a.graph_distance
            .cmp(&b.graph_distance)
            .then(a.chain_id.cmp(&b.chain_id))
    });
    Ok(Neighborhood {
        hits,
        distances,
        truncated,
    })
}
