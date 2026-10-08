//! Reciprocal rank fusion over independent candidate lists.
use kg_core::search::SearchHit;
use std::collections::{HashMap, HashSet};

/// Default RRF smoothing constant. Sixty is the value from the original Cormack
/// et al. formulation; it keeps top ranks from dominating the fused score.
pub const RRF_K: f32 = 60.0;

/// The retrieval method whose perfect score marks an exact identifier or name match.
const EXACT_METHOD: &str = "fulltext";

/// Fusion parameters: the smoothing constant and whether exact matches rank first.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Fusion {
    /// Reciprocal rank fusion smoothing constant (`SearchConfig::rrf_k`).
    pub k: f32,
    /// Rank exact identifier and name matches ahead of fused votes.
    pub exact_match_first: bool,
}
impl Default for Fusion {
    fn default() -> Self {
        Self {
            k: RRF_K,
            exact_match_first: true,
        }
    }
}

/// Vote weight of a zero-based `rank` in one of `lists` fused lists with constant
/// `k`: `1 / (k + rank + 1)`, normalised so the maximum fused score is 1.0 when
/// every list ranks the same record first.
pub(crate) fn rrf_weight(rank: usize, lists: f32, k: f32) -> f32 {
    (k + 1.0) / (k + 1.0 + rank as f32) / lists
}

/// An exact identifier or display-name match: storage reports it with a perfect
/// score, which analysed text relevance (normalised strictly below 1.0) never reaches.
pub(crate) fn exact_match(hit: &SearchHit) -> bool {
    hit.score_breakdown
        .get(EXACT_METHOD)
        .is_some_and(|score| *score >= 1.0)
}

/// Fuse independent rankings by entity version. Each method gets one vote per version.
/// Uses RRF constant 60 with one-based ranks, normalized to 0–1 across input lists,
/// exact matches first. Callers must validate version visibility before using
/// these scores as search results.
pub fn reciprocal_rank_fusion(lists: Vec<Vec<SearchHit>>, limit: usize) -> Vec<SearchHit> {
    fuse_versions(lists, limit, Fusion::default(), |_| true)
}

/// Validate after assigning ranks so discarded versions cannot promote later votes.
pub(crate) fn fuse_versions(
    lists: Vec<Vec<SearchHit>>,
    limit: usize,
    fusion: Fusion,
    visible: impl Fn(&SearchHit) -> bool,
) -> Vec<SearchHit> {
    let count = lists.len().max(1) as f32;
    let mut hits = HashMap::new();
    for list in lists {
        let mut seen = HashSet::new();
        let mut rank = 0;
        for hit in list {
            if !hit.score.is_finite() || !seen.insert((hit.chain_id, hit.uuid)) {
                continue;
            }
            let contribution = rrf_weight(rank, count, fusion.k);
            rank += 1;
            if !visible(&hit) {
                continue;
            }
            let entry = hits.entry((hit.chain_id, hit.uuid)).or_insert_with(|| {
                let mut h = hit.clone();
                h.score = 0.0;
                h
            });
            entry.graph_distance = match (entry.graph_distance, hit.graph_distance) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            entry.score += contribution;
            entry.score_breakdown.extend(hit.score_breakdown);
        }
    }
    let mut hits: Vec<SearchHit> = hits.into_values().collect();
    hits.sort_by(|a, b| {
        let pinned = if fusion.exact_match_first {
            exact_match(b).cmp(&exact_match(a))
        } else {
            std::cmp::Ordering::Equal
        };
        pinned
            .then(b.score.total_cmp(&a.score))
            .then(a.chain_id.cmp(&b.chain_id))
            .then(a.uuid.cmp(&b.uuid))
    });
    hits.truncate(limit);
    hits
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn hit(chain: u128, version: u128, score: f32) -> SearchHit {
        SearchHit {
            derived_summary: None,
            uuid: Uuid::from_u128(version),
            chain_id: Uuid::from_u128(chain),
            entity_type: "Service".into(),
            namespace: "prod".into(),
            name: "n".into(),
            score,
            score_breakdown: Default::default(),
            properties: serde_json::json!({}),
            graph_distance: None,
            embedding: None,
            observation_count: None,
            dependent_count: None,
            last_changed_at: None,
            owner: None,
        }
    }
    fn chains(hits: &[SearchHit]) -> Vec<u128> {
        hits.iter().map(|h| h.chain_id.as_u128()).collect()
    }

    #[test]
    fn weights_start_at_one_over_lists_and_decay_with_rank() {
        assert!((rrf_weight(0, 1.0, RRF_K) - 1.0).abs() < 1e-6);
        assert!((rrf_weight(0, 2.0, RRF_K) - 0.5).abs() < 1e-6);
        assert!((rrf_weight(1, 1.0, RRF_K) - 61.0 / 62.0).abs() < 1e-6);
        assert!(rrf_weight(10, 1.0, RRF_K) > rrf_weight(11, 1.0, RRF_K));
        // A small constant makes the top rank dominate: rank two is worth two thirds of rank one.
        assert!((rrf_weight(1, 1.0, 1.0) - 2.0 / 3.0).abs() < 1e-6);
    }

    #[test]
    fn exact_matches_rank_ahead_of_fused_votes_unless_disabled() {
        // The keyword list ranks the exact identifier first; the vector list ranks a
        // neighbour first. With the default constant both tie at 0.5 and the lower
        // chain id would win; the exact match must still come first.
        let mut exact = hit(9, 9, 1.0);
        exact.score_breakdown.insert("fulltext".into(), 1.0);
        let mut near = hit(1, 1, 0.9);
        near.score_breakdown.insert("vector".into(), 0.9);
        let mut also = hit(9, 9, 0.8);
        also.score_breakdown.insert("vector".into(), 0.8);
        let fused = reciprocal_rank_fusion(
            vec![vec![exact.clone()], vec![near.clone(), also.clone()]],
            10,
        );
        assert_eq!(chains(&fused), [9, 1]);
        let plain = fuse_versions(
            vec![vec![exact.clone()], vec![near.clone(), also.clone()]],
            10,
            Fusion {
                k: RRF_K,
                exact_match_first: false,
            },
            |_| true,
        );
        // Chain 9 collects votes from both lists and wins on score alone here; a
        // fulltext relevance below 1.0 is not an exact match and gets no pin.
        assert_eq!(chains(&plain), [9, 1]);
        let mut fuzzy = hit(7, 7, 0.99);
        fuzzy.score_breakdown.insert("fulltext".into(), 0.99);
        let fused = reciprocal_rank_fusion(vec![vec![fuzzy], vec![near]], 10);
        assert_eq!(chains(&fused), [1, 7]);
    }

    #[test]
    fn unanimous_first_place_scores_one_and_ties_break_by_chain_then_version() {
        let fused = reciprocal_rank_fusion(
            vec![
                vec![hit(2, 2, 0.9), hit(1, 1, 0.1)],
                vec![hit(2, 2, 0.5), hit(1, 1, 0.4)],
            ],
            10,
        );
        assert_eq!(chains(&fused), [2, 1]);
        assert!((fused[0].score - 1.0).abs() < 1e-6);
        let tied = reciprocal_rank_fusion(vec![vec![hit(3, 1, 1.0)], vec![hit(1, 9, 1.0)]], 10);
        assert_eq!(chains(&tied), [1, 3]);
        let versions = reciprocal_rank_fusion(vec![vec![hit(1, 5, 1.0)], vec![hit(1, 2, 1.0)]], 10);
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].uuid, Uuid::from_u128(2));
    }

    #[test]
    fn non_finite_and_repeated_votes_do_not_consume_ranks() {
        let fused = reciprocal_rank_fusion(
            vec![vec![
                hit(1, 1, f32::NAN),
                hit(2, 2, 1.0),
                hit(2, 2, 1.0),
                hit(3, 3, 0.5),
            ]],
            10,
        );
        assert_eq!(chains(&fused), [2, 3]);
        assert!((fused[1].score - rrf_weight(1, 1.0, RRF_K)).abs() < 1e-6);
    }

    #[test]
    fn invisible_hits_keep_their_rank_position_without_scoring() {
        let hidden = Uuid::from_u128(1);
        let fused = fuse_versions(
            vec![vec![hit(1, 1, 1.0), hit(2, 2, 0.5)]],
            10,
            Fusion::default(),
            |h| h.chain_id != hidden,
        );
        assert_eq!(chains(&fused), [2]);
        // Chain 2 stays at rank one: the rejected hit did not promote it.
        assert!((fused[0].score - rrf_weight(1, 1.0, RRF_K)).abs() < 1e-6);
    }

    #[test]
    fn fusion_keeps_the_shortest_distance_and_merges_score_breakdowns() {
        let mut a = hit(1, 1, 1.0);
        a.graph_distance = Some(2);
        a.score_breakdown.insert("bfs".into(), 0.3);
        let mut b = hit(1, 1, 1.0);
        b.graph_distance = Some(1);
        b.score_breakdown.insert("vector".into(), 0.8);
        let fused = reciprocal_rank_fusion(vec![vec![a], vec![b]], 1);
        assert_eq!(fused[0].graph_distance, Some(1));
        assert!(fused[0].score_breakdown.contains_key("bfs"));
        assert!(fused[0].score_breakdown.contains_key("vector"));
    }
}
