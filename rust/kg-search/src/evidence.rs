//! Bounded evidence recall, model ranking, and per-entity coverage.
use crate::{
    engine::{
        observe, page_diagnostic, selection_diagnostic, truncation_diagnostic, QueryEmbedding,
    },
    rerank::rrf_weight,
};
use futures::{stream, StreamExt};
use kg_core::{
    embedding::{MAX_FIELD_CHARS, MAX_TEXT_CHARS},
    errors::BackendError,
    search::*,
    traits::{rerank_backend::MAX_CANDIDATE_BYTES, RankCandidate, RerankBackend, SearchBackend},
};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// One evidence record of either kind, so budgets, ranking and selection share code.
enum Evidence {
    Relationship(RelationshipHit),
    Snapshot(SnapshotHit),
}
impl Evidence {
    fn id(&self) -> Uuid {
        match self {
            Self::Relationship(h) => h.uuid,
            Self::Snapshot(h) => h.uuid,
        }
    }
    fn score(&self) -> f32 {
        match self {
            Self::Relationship(h) => h.model_score.unwrap_or(h.score),
            Self::Snapshot(h) => h.model_score.unwrap_or(h.score),
        }
    }
    fn set_model_score(&mut self, score: f32) {
        match self {
            Self::Relationship(h) => h.model_score = Some(score),
            Self::Snapshot(h) => h.model_score = Some(score),
        }
    }
    /// Bounded text for the relevance model. Long passages and descriptions get
    /// the full text budget; labels and names get the field budget.
    fn candidate(&self) -> RankCandidate {
        let fields: Vec<(&str, &str)> = match self {
            Self::Relationship(h) => vec![
                ("kind", "relationship"),
                ("name", &h.name),
                ("description", &h.description),
            ],
            Self::Snapshot(h) => vec![
                ("kind", "snapshot"),
                ("name", &h.name),
                ("source", &h.source),
                ("passage", &h.content),
            ],
        };
        let mut text = String::new();
        for (label, value) in fields {
            text.push_str(label);
            text.push_str(": ");
            text.extend(
                value
                    .chars()
                    .take(if label == "passage" || label == "description" {
                        MAX_TEXT_CHARS
                    } else {
                        MAX_FIELD_CHARS
                    }),
            );
            text.push('\n');
        }
        // The provider budget includes labels and metadata, not just the passage.
        let mut end = text.len().min(MAX_CANDIDATE_BYTES);
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
        RankCandidate {
            id: self.id(),
            text,
        }
    }
}

/// Split the attached-evidence budget evenly across ranked chains. Chains beyond
/// the budget get nothing; earlier chains absorb the remainder.
fn allocations(chains: &[Uuid], budget: usize) -> Vec<(Uuid, usize)> {
    let count = chains.len().min(budget);
    chains
        .iter()
        .take(count)
        .enumerate()
        .map(|(i, &id)| (id, budget / count + usize::from(i < budget % count)))
        .collect()
}

/// Round-robin selection across per-entity groups so a hub cannot crowd out the
/// evidence of quieter entities. Within a group, `items` order (already ranked)
/// decides which record is taken first.
fn select(items: &[Evidence], groups: &[Vec<Uuid>], limit: usize) -> HashSet<Uuid> {
    let positions: HashMap<_, _> = items.iter().enumerate().map(|(i, h)| (h.id(), i)).collect();
    let groups: Vec<_> = groups
        .iter()
        .map(|ids| {
            let mut ids: Vec<_> = ids
                .iter()
                .copied()
                .filter(|id| positions.contains_key(id))
                .collect();
            ids.sort_by_key(|id| positions[id]);
            ids
        })
        .collect();
    let mut selected = HashSet::new();
    for round in 1..=limit {
        for group in &groups {
            if selected.len() == limit {
                return selected;
            }
            // Shared evidence already satisfies this round for every group containing it.
            if group.iter().filter(|id| selected.contains(*id)).count() >= round {
                continue;
            }
            if let Some(id) = group.iter().find(|id| !selected.contains(*id)) {
                selected.insert(*id);
            }
        }
    }
    selected
}

/// Everything evidence retrieval borrows from the engine for one request.
pub(crate) struct EvidenceContext<'a> {
    pub graph: &'a dyn SearchBackend,
    pub model: Option<&'a dyn RerankBackend>,
    pub query: &'a str,
    pub filter: &'a SearchFilter,
    pub config: &'a SearchConfig,
    pub query_embedding: Option<QueryEmbedding<'a>>,
}

/// Attached rows grouped per anchor in storage order.
struct AttachedPage {
    rows: HashMap<Uuid, Vec<Evidence>>,
    truncated: bool,
}

/// One storage call fetches evidence for every anchor; each anchor's rows are
/// then cut to its own allocation so a hub cannot consume a quiet entity's share.
async fn attached(
    graph: &dyn SearchBackend,
    request: &AttachedEvidence,
    relationships: bool,
) -> Result<AttachedPage, BackendError> {
    let (items, truncated): (Vec<(Uuid, Evidence)>, bool) = if relationships {
        let page = graph.attached_relationships(request).await?;
        (
            page.items
                .into_iter()
                .map(|a| (a.anchor, Evidence::Relationship(a.record)))
                .collect(),
            page.truncated,
        )
    } else {
        let page = graph.attached_snapshots(request).await?;
        (
            page.items
                .into_iter()
                .map(|a| (a.anchor, Evidence::Snapshot(a.record)))
                .collect(),
            page.truncated,
        )
    };
    let anchors: HashSet<_> = request.anchors.iter().copied().collect();
    let mut identities = HashSet::new();
    let mut rows = HashMap::<Uuid, Vec<Evidence>>::new();
    for (anchor, item) in items {
        if !anchors.contains(&anchor)
            || !identities.insert((anchor, item.id()))
            || !item.score().is_finite()
        {
            return Err(BackendError::Deserialization(
                "invalid attached evidence page".into(),
            ));
        }
        let group = rows.entry(anchor).or_default();
        group.push(item);
        if group.len() > request.per_anchor {
            return Err(BackendError::Deserialization(
                "invalid attached evidence page".into(),
            ));
        }
    }
    Ok(AttachedPage { rows, truncated })
}

/// The typed failure of a required ranking call, from its recorded diagnostic.
fn required_ranking_failure(
    diagnostics: &[SearchDiagnostic],
    message: &str,
    timeout_ms: u64,
) -> BackendError {
    crate::engine::typed_failure(diagnostics.last(), message, timeout_ms)
}

async fn retrieve(
    context: &EvidenceContext<'_>,
    result: &mut SearchResult,
    scope: SearchScope,
) -> Result<bool, BackendError> {
    let EvidenceContext {
        graph,
        model,
        query,
        filter,
        config,
        query_embedding,
    } = context;
    let (graph, model, query, filter, config) = (*graph, *model, *query, *filter, *config);
    let explicit = config.scopes.contains(&scope);
    let relationships = scope == SearchScope::Relationships;
    let operation = if relationships {
        "relationships"
    } else {
        "snapshots"
    };
    let mut items = Vec::new();
    let mut groups = Vec::new();
    let mut seen = HashSet::new();
    let mut succeeded = false;
    let mut retrieved = 0;
    if !explicit {
        let chains: Vec<_> = result.hits.iter().map(|h| h.chain_id).collect();
        let allocated = allocations(&chains, config.evidence_prefetch);
        if allocated.len() < chains.len() {
            result.truncated = true;
            result.diagnostics.push(truncation_diagnostic(
                &format!("{operation}_coverage"),
                chains.len() - allocated.len(),
            ));
        }
        let request = AttachedEvidence {
            filter: filter.clone(),
            anchors: allocated.iter().map(|(id, _)| *id).collect(),
            per_anchor: allocated.iter().map(|(_, n)| *n).max().unwrap_or(1),
            passage_query: Some(query.to_owned()),
        };
        let page = observe(
            operation,
            config.operation_timeout_ms,
            attached(graph, &request, relationships),
            &mut result.diagnostics,
        )
        .await;
        if let Ok(mut page) = page {
            succeeded = true;
            let mut returned = 0;
            for (anchor, budget) in allocated {
                let rows = page.rows.remove(&anchor).unwrap_or_default();
                returned += rows.len();
                page.truncated |= rows.len() > budget;
                let rows: Vec<_> = rows.into_iter().take(budget).collect();
                retrieved += rows.len();
                groups.push(rows.iter().map(Evidence::id).collect());
                for item in rows {
                    if seen.insert(item.id()) {
                        items.push(item);
                    }
                }
            }
            if let Some(d) = result.diagnostics.last_mut() {
                d.count = returned;
                if page.truncated {
                    d.status = "truncated".into();
                }
            }
            result.truncated |= page.truncated;
        }
        // Attached facts arrive newest first. When the request carries a query
        // embedding, rank each anchor's facts by their cosine to the question
        // instead (unembedded or unmatched facts keep their recency order after),
        // so a question about one fact of a busy entity surfaces that fact first.
        if relationships && succeeded && !items.is_empty() {
            if let Some(future) = query_embedding.clone() {
                if let (Some(embedding), _) = future.await {
                    let anchors: Vec<_> = result.hits.iter().map(|h| h.chain_id).collect();
                    if !anchors.is_empty() && anchors.len() <= 500 {
                        let request = RelationshipSimilarity {
                            filter: filter.clone(),
                            embedding,
                            limit: config.evidence_prefetch.clamp(1, 500),
                            min_score: -1.0,
                            anchor_chains: Some(anchors),
                        };
                        let ranked = observe(
                            "relationships_anchored_vector",
                            config.operation_timeout_ms,
                            async {
                                if config.vector_retrieval == VectorRetrieval::Indexed {
                                    graph.search_relationships_indexed(&request).await
                                } else {
                                    graph.search_relationship_similarity(&request).await
                                }
                            },
                            &mut result.diagnostics,
                        )
                        .await;
                        if let Ok(page) = ranked {
                            page_diagnostic(&mut result.diagnostics, &page);
                            let scores: HashMap<Uuid, f32> =
                                page.items.iter().map(|h| (h.uuid, h.score)).collect();
                            for item in &mut items {
                                if let Evidence::Relationship(hit) = item {
                                    // Ranked facts carry their cosine; facts the
                                    // anchored vector pass did not rank are set to
                                    // 0.0 rather than keeping the storage query's
                                    // 1.0 ordering placeholder, which would print a
                                    // perfect score next to real lower cosines.
                                    hit.score = scores.get(&hit.uuid).copied().unwrap_or(0.0);
                                }
                            }
                            // Stable: unranked facts keep their recency order at the end.
                            items.sort_by(|a, b| {
                                let ranked = |e: &Evidence| scores.contains_key(&e.id());
                                ranked(b)
                                    .cmp(&ranked(a))
                                    .then_with(|| b.score().total_cmp(&a.score()))
                            });
                        }
                    }
                }
            }
        }
    }
    let requests: Vec<_> = if !explicit {
        vec![]
    } else if relationships {
        config
            .relationship_methods
            .iter()
            .map(|method| (config.evidence_prefetch, *method == SearchMethod::Vector))
            .collect()
    } else {
        vec![(config.evidence_prefetch, false)]
    };
    let outcomes = stream::iter(requests.into_iter().map(|(limit, semantic)| {
        let query_embedding = query_embedding.clone();
        async move {
            let mut diagnostics = Vec::new();
            let request = EvidenceSearch {
                filter: filter.clone(),
                query: Some(query.to_owned()),
                passage_query: Some(query.to_owned()),
                chain_ids: None,
                limit,
            };
            // Embedding has its own operation timeout; the database budget starts
            // once its input is ready. The outer request deadline covers both.
            let embedding = if semantic {
                query_embedding.expect("semantic query future").await.0
            } else {
                None
            };
            let page = observe(
                if semantic {
                    "relationships_vector"
                } else {
                    operation
                },
                config.operation_timeout_ms,
                async {
                    let page = if relationships {
                        let page = if semantic {
                            let embedding = embedding.ok_or_else(|| {
                                BackendError::Unavailable("query embedding failed".into())
                            })?;
                            let request = RelationshipSimilarity {
                                filter: filter.clone(),
                                embedding,
                                limit,
                                min_score: config.min_score,
                                anchor_chains: None,
                            };
                            if config.vector_retrieval == VectorRetrieval::Indexed {
                                graph.search_relationships_indexed(&request).await?
                            } else {
                                graph.search_relationship_similarity(&request).await?
                            }
                        } else {
                            graph.search_relationships(&request).await?
                        };
                        SearchPage {
                            items: page.items.into_iter().map(Evidence::Relationship).collect(),
                            truncated: page.truncated,
                            approximate: page.approximate,
                        }
                    } else {
                        let page = graph.search_snapshots(&request).await?;
                        SearchPage {
                            items: page.items.into_iter().map(Evidence::Snapshot).collect(),
                            truncated: page.truncated,
                            approximate: page.approximate,
                        }
                    };
                    let unique: HashSet<_> = page.items.iter().map(Evidence::id).collect();
                    if page.items.len() > limit
                        || unique.len() != page.items.len()
                        || page.items.iter().any(|h| !h.score().is_finite())
                    {
                        return Err(BackendError::Deserialization(
                            "invalid evidence page".into(),
                        ));
                    }
                    Ok(page)
                },
                &mut diagnostics,
            )
            .await;
            if let Ok(page) = &page {
                page_diagnostic(&mut diagnostics, page);
            }
            (page, diagnostics)
        }
    }))
    .buffered(8)
    .collect::<Vec<_>>()
    .await;
    let merge_start = tokio::time::Instant::now();
    let fuse = explicit && relationships && config.relationship_methods.len() > 1;
    let methods = outcomes
        .iter()
        .filter(|(page, _)| page.is_ok())
        .count()
        .max(1) as f32;
    let mut votes = HashMap::<Uuid, f32>::new();
    for (page, diagnostics) in outcomes {
        result.diagnostics.extend(diagnostics);
        if let Ok(page) = page {
            succeeded = true;
            retrieved += page.items.len();
            result.truncated |= page.truncated;
            result.approximate |= page.approximate;
            groups.push(page.items.iter().map(Evidence::id).collect());
            for (rank, item) in page.items.into_iter().enumerate() {
                if fuse {
                    *votes.entry(item.id()).or_default() += rrf_weight(rank, methods, config.rrf_k);
                }
                if seen.insert(item.id()) {
                    items.push(item);
                }
            }
        }
    }
    if fuse {
        for item in &mut items {
            if let Evidence::Relationship(hit) = item {
                hit.score = votes[&hit.uuid];
            }
        }
        items.sort_by(|a, b| b.score().total_cmp(&a.score()).then(a.id().cmp(&b.id())));
    }
    if succeeded {
        selection_diagnostic(
            &mut result.diagnostics,
            &format!("{operation}_merge"),
            merge_start,
            retrieved,
            items.len(),
        );
    }
    let budget_start = tokio::time::Instant::now();
    let before_budget = items.len();
    if before_budget > config.evidence_prefetch {
        result.truncated = true;
        items.truncate(config.evidence_prefetch);
        seen = items.iter().map(Evidence::id).collect();
        selection_diagnostic(
            &mut result.diagnostics,
            &format!("{operation}_budget"),
            budget_start,
            before_budget,
            items.len(),
        );
    }
    if config.evidence_reranker == EvidenceReranker::Model && !items.is_empty() {
        let candidates: Vec<_> = items.iter().map(Evidence::candidate).collect();
        let ranked = observe(
            &format!("{operation}_model"),
            config.operation_timeout_ms,
            async {
                let scores = model
                    .expect("validated evidence model")
                    .rank(query, &candidates)
                    .await?;
                let actual: HashSet<_> = scores.iter().map(|s| s.id).collect();
                if scores.len() != seen.len()
                    || actual != seen
                    || scores.iter().any(|s| !s.score.is_finite())
                {
                    return Err(BackendError::Deserialization(
                        "reranker must return one finite score per evidence candidate".into(),
                    ));
                }
                Ok(scores)
            },
            &mut result.diagnostics,
        )
        .await;
        // A required relevance floor cannot be applied without scores: the
        // request fails with the ranking call's own typed cause, as for entities.
        if ranked.is_err() && config.evidence_model_min_score.is_some() {
            return Err(required_ranking_failure(
                &result.diagnostics,
                &format!("required {operation} relevance ranking failed"),
                config.operation_timeout_ms,
            ));
        }
        if let Ok(scores) = ranked {
            result
                .diagnostics
                .last_mut()
                .expect("recorded model call")
                .count = scores.len();
            let scores: HashMap<_, _> = scores.into_iter().map(|s| (s.id, s.score)).collect();
            for item in &mut items {
                item.set_model_score(scores[&item.id()]);
            }
            if let Some(minimum) = config.evidence_model_min_score {
                let filter_start = tokio::time::Instant::now();
                let before_filter = items.len();
                items.retain(|item| item.score() >= minimum);
                selection_diagnostic(
                    &mut result.diagnostics,
                    &format!("{operation}_relevance"),
                    filter_start,
                    before_filter,
                    items.len(),
                );
            }
            items.sort_by(|a, b| b.score().total_cmp(&a.score()).then(a.id().cmp(&b.id())));
        }
    }
    let selection_start = tokio::time::Instant::now();
    let before_selection = items.len();
    let limit = if explicit {
        config.limit
    } else {
        config.evidence_limit
    };
    if explicit {
        items.truncate(limit);
    } else {
        let selected = select(&items, &groups, limit);
        items.retain(|h| selected.contains(&h.id()));
    }
    if succeeded {
        selection_diagnostic(
            &mut result.diagnostics,
            &format!("{operation}_selection"),
            selection_start,
            before_selection,
            items.len(),
        );
    }
    if !relationships && succeeded {
        // Passage selection runs inside the storage read; count only returned excerpts.
        let mut matched = 0;
        let mut fallback = 0;
        let mut limited = 0;
        for item in &items {
            if let Evidence::Snapshot(snapshot) = item {
                match snapshot.selection_kind {
                    ExcerptSelection::Matched => matched += 1,
                    ExcerptSelection::Fallback => fallback += 1,
                }
                limited += usize::from(snapshot.selection_limited);
            }
        }
        for (name, count, truncated) in [
            ("excerpt_matched", matched, false),
            ("excerpt_fallback", fallback, false),
            ("excerpt_scan_limited", limited, limited > 0),
        ] {
            result.diagnostics.push(SearchDiagnostic {
                operation: name.into(),
                status: if truncated { "truncated" } else { "success" }.into(),
                duration_ms: 0,
                count,
                dropped_count: 0,
                failure: None,
                timeout_ms: None,
                retry_after_ms: None,
            });
        }
    }
    for item in items {
        match item {
            Evidence::Relationship(h) => result.relationships.push(h),
            Evidence::Snapshot(h) => result.snapshots.push(h),
        }
    }
    Ok(succeeded)
}

/// Explicit scopes or attached evidence for `hits`. An error means a required
/// ranking floor could not be applied; degraded retrieval is reported per scope.
pub(crate) async fn retrieve_scopes(
    context: EvidenceContext<'_>,
    hits: &[SearchHit],
    explicit: bool,
) -> Result<(SearchResult, usize), BackendError> {
    let config = context.config;
    let query = context.query;
    let outcomes = futures::future::try_join_all(
        [SearchScope::Relationships, SearchScope::Snapshots]
            .into_iter()
            .map(|scope| {
                let context = &context;
                async move {
                    let mut result = SearchResult::default();
                    if config.scopes.contains(&scope) != explicit
                        || (explicit && query.trim().is_empty())
                        || (!explicit && (!config.include_evidence || hits.is_empty()))
                    {
                        return Ok((result, false));
                    }
                    result.hits = hits.to_vec();
                    let success = retrieve(context, &mut result, scope).await?;
                    if explicit && success {
                        result.methods_used.push(
                            if scope == SearchScope::Relationships {
                                "relationships"
                            } else {
                                "snapshots"
                            }
                            .into(),
                        );
                    }
                    Ok((result, success))
                }
            }),
    )
    .await?;
    let mut result = SearchResult::default();
    let mut successes = 0;
    for (part, success) in outcomes {
        successes += usize::from(success);
        result.relationships.extend(part.relationships);
        result.snapshots.extend(part.snapshots);
        result.methods_used.extend(part.methods_used);
        result.diagnostics.extend(part.diagnostics);
        result.truncated |= part.truncated;
        result.approximate |= part.approximate;
    }
    Ok((result, successes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unicode_evidence_fits_the_complete_model_candidate_budget() {
        let hit = Evidence::Relationship(RelationshipHit {
            uuid: Uuid::from_u128(1),
            source_chain_id: Uuid::from_u128(2),
            target_chain_id: Uuid::from_u128(3),
            name: "🌍".repeat(512),
            description: "🌍".repeat(4096),
            valid_from: None,
            valid_to: None,
            snapshot_id: None,
            score: 1.0,
            model_score: None,
        });
        let candidate = hit.candidate();
        assert!(candidate.text.len() <= MAX_CANDIDATE_BYTES);
        assert!(candidate.text.contains("description: "));
        assert!(candidate.text.ends_with('🌍'));
        let snapshot = Evidence::Snapshot(SnapshotHit {
            uuid: Uuid::from_u128(4),
            name: "🌍".repeat(512),
            source: "🌍".repeat(512),
            namespace: "prod".into(),
            captured_at: None,
            content: "🌍".repeat(4096),
            content_truncated: false,
            content_start: 0,
            content_end: 4096,
            selection_kind: ExcerptSelection::Matched,
            selection_limited: false,
            score: 1.0,
            model_score: None,
        });
        let candidate = snapshot.candidate();
        assert!(candidate.text.len() <= MAX_CANDIDATE_BYTES);
        assert!(candidate.text.ends_with('🌍'));
    }

    #[test]
    fn candidate_allocations_are_bounded_and_keep_ranked_chain_order() {
        let chains: Vec<_> = (1..=3).map(Uuid::from_u128).collect();
        assert_eq!(
            allocations(&chains, 8),
            vec![(chains[0], 3), (chains[1], 3), (chains[2], 2)]
        );
        assert_eq!(
            allocations(&chains, 2),
            vec![(chains[0], 1), (chains[1], 1)]
        );
        assert!(allocations(&[], 50).is_empty());
    }

    // ---- merged from `mod selection_tests`

    fn fact(n: u128) -> Evidence {
        Evidence::Relationship(RelationshipHit {
            uuid: Uuid::from_u128(n),
            source_chain_id: Uuid::from_u128(100),
            target_chain_id: Uuid::from_u128(101),
            name: "uses".into(),
            description: String::new(),
            valid_from: None,
            valid_to: None,
            snapshot_id: None,
            score: 1.0,
            model_score: None,
        })
    }
    fn ids(ns: &[u128]) -> Vec<Uuid> {
        ns.iter().map(|n| Uuid::from_u128(*n)).collect()
    }
    fn set(ns: &[u128]) -> HashSet<Uuid> {
        ids(ns).into_iter().collect()
    }

    #[test]
    fn selection_alternates_between_groups_before_taking_second_records() {
        let items: Vec<_> = (1..=6).map(fact).collect();
        let groups = vec![ids(&[1, 2, 3]), ids(&[4, 5, 6])];
        assert_eq!(select(&items, &groups, 3), set(&[1, 4, 2]));
    }

    #[test]
    fn shared_evidence_satisfies_every_group_that_retrieved_it() {
        let items: Vec<_> = (1..=4).map(fact).collect();
        let groups = vec![ids(&[1, 2]), ids(&[1, 3])];
        // Record 1 counts for both entities, so round one takes nothing else for group two.
        assert_eq!(select(&items, &groups, 3), set(&[1, 2, 3]));
    }

    #[test]
    fn selection_ignores_unknown_ids_and_respects_the_limit() {
        let items: Vec<_> = (1..=2).map(fact).collect();
        let groups = vec![ids(&[9, 1]), ids(&[2, 8])];
        assert_eq!(select(&items, &groups, 1), set(&[1]));
        assert_eq!(select(&items, &groups, 10), set(&[1, 2]));
        assert!(select(&items, &[], 5).is_empty());
    }
}
