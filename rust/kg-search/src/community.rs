//! Community recall and ranking share entity algorithms without changing public result identities.
use crate::{
    engine::{observe, page_diagnostic, search_clock, truncation_diagnostic, QueryEmbedding},
    rerank::{fuse_versions, maximal_marginal_relevance},
};
use kg_core::{
    errors::BackendError,
    search::*,
    traits::{RankCandidate, RerankBackend, SearchBackend},
};
use std::collections::{HashMap, HashSet};

/// Project a community onto the entity hit shape so rank fusion and MMR can be
/// reused. The community identity becomes the chain and its revision the version.
fn candidate(hit: &CommunityHit) -> SearchHit {
    SearchHit {
        derived_summary: None,
        uuid: hit.revision,
        chain_id: hit.uuid,
        entity_type: "Community".into(),
        namespace: hit.namespace.clone(),
        name: hit.name.clone(),
        score: hit.score,
        score_breakdown: hit.score_breakdown.clone(),
        properties: serde_json::json!({"summary":hit.summary}),
        graph_distance: None,
        embedding: hit.embedding.clone(),
        observation_count: None,
        dependent_count: None,
        last_changed_at: Some(hit.projected_at.to_rfc3339()),
        owner: None,
    }
}
/// Reject adapter rows that violate the request scope or the evaluation instant.
/// Storage is trusted for organization, but namespace and time are rechecked.
fn valid(hit: &CommunityHit, filter: &SearchFilter) -> bool {
    let at = search_clock(filter);
    !hit.uuid.is_nil()
        && !hit.generation.is_nil()
        && !hit.revision.is_nil()
        && hit.score.is_finite()
        && (filter.namespaces.is_empty() || filter.namespaces.contains(&hit.namespace))
        && hit.projected_at <= at
        && hit.valid_until.is_none_or(|until| at < until)
}
/// Community recall, revision validation, fusion, optional ranking and member
/// hydration. Returns the partial result and the number of retrieval methods
/// that succeeded, which the engine sums across scopes.
pub(crate) async fn retrieve(
    graph: &dyn SearchBackend,
    model: Option<&dyn RerankBackend>,
    query: &str,
    filter: &SearchFilter,
    config: &SearchConfig,
    embedding: QueryEmbedding<'_>,
) -> Result<(SearchResult, usize), BackendError> {
    let mut result = SearchResult::default();
    if !config.scopes.contains(&SearchScope::Communities) {
        return Ok((result, 0));
    }
    let mut lists = Vec::new();
    let mut identities = HashMap::new();
    let mut successes = 0;
    // The node path records the shared `embed_query` diagnostic; recording it
    // here as well would report one embedding call twice.
    let vector = if config.community_methods.contains(&SearchMethod::Vector) {
        embedding.await.0
    } else {
        None
    };
    for method in &config.community_methods {
        let (mode, label) = match method {
            SearchMethod::Fulltext => (NodeQuery::Fulltext(query.into()), "community_fulltext"),
            SearchMethod::Vector => {
                let Some(vector) = &vector else {
                    continue;
                };
                (NodeQuery::Similarity(vector.clone()), "community_vector")
            }
            _ => unreachable!("validated community method"),
        };
        let request = CommunitySearch {
            filter: filter.clone(),
            query: mode,
            uuids: None,
            limit: config.prefetch,
            min_score: config.min_score,
            member_limit: 0,
        };
        if let Ok(page) = observe(
            label,
            config.operation_timeout_ms,
            graph.search_communities(
                &request,
                config.vector_retrieval == VectorRetrieval::Indexed,
            ),
            &mut result.diagnostics,
        )
        .await
        {
            page_diagnostic(&mut result.diagnostics, &page);
            result.approximate |= page.approximate;
            result.truncated |= page.truncated;
            successes += 1;
            result.methods_used.push(label.into());
            let mut list = Vec::new();
            for mut hit in page.items {
                if !valid(&hit, filter) {
                    return Err(BackendError::Deserialization(
                        "community result violates scope or temporal coverage".into(),
                    ));
                }
                hit.score_breakdown.insert(label.into(), hit.score);
                identities.insert((hit.uuid, hit.revision), hit.clone());
                list.push(candidate(&hit));
            }
            lists.push(list);
        }
    }
    let ids: Vec<_> = identities
        .keys()
        .map(|(uuid, _)| *uuid)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    result.total_candidates = ids.len();
    let mut fresh = HashMap::new();
    for batch in ids.chunks(500) {
        let request = CommunitySearch {
            filter: filter.clone(),
            query: NodeQuery::ByChain,
            uuids: Some(batch.to_vec()),
            limit: 500,
            min_score: 0.0,
            member_limit: 0,
        };
        let page = observe(
            "community_hydrate",
            config.operation_timeout_ms,
            graph.search_communities(&request, false),
            &mut result.diagnostics,
        )
        .await?;
        page_diagnostic(&mut result.diagnostics, &page);
        for hit in page.items {
            if !valid(&hit, filter)
                || !batch.contains(&hit.uuid)
                || fresh.insert(hit.uuid, hit).is_some()
            {
                return Err(BackendError::Deserialization(
                    "invalid community hydration".into(),
                ));
            }
        }
    }
    // Metadata is checked before assigning votes; stale generations never influence scores.
    let visible = |hit: &SearchHit| {
        identities
            .get(&(hit.chain_id, hit.uuid))
            .is_some_and(|old| {
                fresh.get(&hit.chain_id).is_some_and(|current| {
                    current.generation == old.generation
                        && current.revision == old.revision
                        && current.source_hash == old.source_hash
                })
            })
    };
    let stale = lists.iter().flatten().filter(|hit| !visible(hit)).count();
    if stale > 0 {
        result.truncated = true;
        result
            .diagnostics
            .push(truncation_diagnostic("community_visibility_changed", stale));
    }
    let mut ranked = fuse_versions(
        lists,
        usize::MAX,
        crate::rerank::Fusion {
            k: config.rrf_k,
            exact_match_first: false,
        },
        visible,
    );
    if ranked.len() > config.prefetch {
        result.truncated = true;
        ranked.truncate(config.prefetch);
    }
    for hit in &mut ranked {
        let current = &fresh[&hit.chain_id];
        hit.embedding = current.embedding.clone();
        hit.properties = serde_json::json!({"summary":current.summary});
        hit.name = current.name.clone();
    }
    match config.community_reranker {
        RerankMethod::Mmr => {
            ranked = maximal_marginal_relevance(ranked, config.mmr_lambda, config.limit).await
        }
        RerankMethod::Model if !ranked.is_empty() => {
            let candidates: Vec<_> = ranked
                .iter()
                .map(|hit| RankCandidate {
                    id: hit.chain_id,
                    text: crate::rerank::hit_text(hit),
                })
                .collect();
            let outcome = observe(
                "community_rerank",
                config.operation_timeout_ms,
                async {
                    let scores = model
                        .ok_or_else(|| {
                            BackendError::NotConfigured("community reranker unavailable".into())
                        })?
                        .rank(query, &candidates)
                        .await?;
                    let expected: HashSet<_> =
                        candidates.iter().map(|candidate| candidate.id).collect();
                    let actual: HashSet<_> = scores.iter().map(|score| score.id).collect();
                    if scores.len() != expected.len()
                        || actual != expected
                        || scores.iter().any(|score| !score.score.is_finite())
                    {
                        return Err(BackendError::Deserialization(
                            "invalid community relevance scores".into(),
                        ));
                    }
                    Ok(scores)
                },
                &mut result.diagnostics,
            )
            .await;
            if outcome.is_err() && config.community_model_min_score.is_some() {
                return Err(crate::engine::typed_failure(
                    result.diagnostics.last(),
                    "required community relevance ranking failed",
                    config.operation_timeout_ms,
                ));
            }
            if let Ok(scores) = outcome {
                let scores: HashMap<_, _> = scores
                    .into_iter()
                    .map(|score| (score.id, score.score))
                    .collect();
                for hit in &mut ranked {
                    hit.score = scores[&hit.chain_id];
                    hit.score_breakdown.insert("model".into(), hit.score);
                }
                ranked.retain(|hit| {
                    config
                        .community_model_min_score
                        .is_none_or(|minimum| hit.score >= minimum)
                });
                ranked.sort_by(|a, b| {
                    b.score
                        .total_cmp(&a.score)
                        .then(a.chain_id.cmp(&b.chain_id))
                });
            }
        }
        _ => {}
    }
    ranked.truncate(config.limit);
    if !ranked.is_empty() {
        // Final hydration loads member evidence for the selected communities only.
        // The member budget keeps one page under 2,000 rows regardless of `limit`.
        let selected: Vec<_> = ranked.iter().map(|hit| hit.chain_id).collect();
        let request = CommunitySearch {
            filter: filter.clone(),
            query: NodeQuery::ByChain,
            uuids: Some(selected.clone()),
            limit: config.limit,
            min_score: 0.0,
            member_limit: if config.include_evidence {
                config.evidence_limit.min(2000 / config.limit)
            } else {
                0
            },
        };
        let page = observe(
            "community_evidence",
            config.operation_timeout_ms,
            graph.search_communities(&request, false),
            &mut result.diagnostics,
        )
        .await?;
        page_diagnostic(&mut result.diagnostics, &page);
        let mut final_hits = HashMap::new();
        for hit in page.items {
            if !valid(&hit, filter)
                || !selected.contains(&hit.uuid)
                || final_hits.insert(hit.uuid, hit).is_some()
            {
                return Err(BackendError::Deserialization(
                    "invalid community evidence hydration".into(),
                ));
            }
        }
        for candidate in ranked {
            if let Some(mut hit) = final_hits.remove(&candidate.chain_id) {
                // A community republished between hydration and member loading is
                // dropped rather than returned with scores from an older revision.
                let prior = &fresh[&candidate.chain_id];
                if prior.revision != hit.revision
                    || prior.generation != hit.generation
                    || prior.source_hash != hit.source_hash
                {
                    result.truncated = true;
                    continue;
                }
                hit.score = candidate.score;
                hit.score_breakdown = candidate.score_breakdown;
                result.truncated |= config.include_evidence && hit.members_truncated;
                result.communities.push(hit);
            } else {
                result.truncated = true;
            }
        }
    }
    Ok((result, successes))
}
