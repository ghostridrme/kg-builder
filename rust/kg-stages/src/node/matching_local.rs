//! Incoming candidate retrieval mirrors stored retrieval: exhaustive names and
//! bounded property/vector windows. Ranking offers evidence; it never decides identity.
use super::matching_decision::LocalCandidate;
use kg_core::{models::EntityNode, runtime::matching::MatchingSettings};
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

fn cosine(a: &[f32], b: &[f32]) -> Result<f64, String> {
    if a.is_empty() || a.len() != b.len() {
        return Err("incompatible incoming candidate vectors".into());
    }
    let (mut dot, mut na, mut nb) = (0.0, 0.0, 0.0);
    for (&a, &b) in a.iter().zip(b) {
        let (a, b) = (f64::from(a), f64::from(b));
        dot += a * b;
        na += a * a;
        nb += b * b;
    }
    if !dot.is_finite() || !na.is_finite() || !nb.is_finite() || na == 0.0 || nb == 0.0 {
        return Err("invalid incoming candidate vectors".into());
    }
    Ok(dot / (na * nb).sqrt())
}

/// Offer an incoming component to a text mention only on evidence: the same
/// name, an identifying property in common, or a vector above the similarity
/// floor. A mention with no similar candidate is new. Frontiers over the limit are then ranked.
pub(super) fn select(
    members: &[(usize, EntityNode)],
    locals: Vec<LocalCandidate>,
    embedded: &HashMap<Uuid, EntityNode>,
    settings: &MatchingSettings,
) -> Result<Vec<LocalCandidate>, String> {
    // Only text mentions (no authoritative keys) are gated on evidence; keyed
    // components keep their complete incoming frontier, where a model may
    // still link aliases declared by different connectors.
    let gated = members.iter().all(|(_, e)| !e.has_authoritative_keys());
    let floor = if gated {
        f64::from(settings.candidate_min_similarity)
    } else {
        -1.0
    };
    let mut names = Vec::new();
    let mut properties = Vec::new();
    let mut vectors = Vec::new();
    let mut evidenced = Vec::new();
    for (index, candidate) in locals.iter().enumerate() {
        let (mut name, mut overlap, mut similarity) = (false, 0, -1.0_f64);
        for (_, source) in members {
            // Keyed components are not embedded for retrieval; without a vector
            // on either side the pair has no vector evidence, only names and
            // properties.
            let source_vector = embedded
                .get(&source.uuid)
                .and_then(|e| e.embedding.as_ref());
            for (_, target) in &candidate.members {
                name |= source.name.trim().eq_ignore_ascii_case(target.name.trim());
                overlap = overlap.max(
                    source
                        .all_properties
                        .iter()
                        .filter(|(key, value)| {
                            value.as_identity_key().is_some()
                                && target.all_properties.get(*key) == Some(*value)
                        })
                        .count(),
                );
                let target_vector = embedded
                    .get(&target.uuid)
                    .and_then(|e| e.embedding.as_ref());
                if let (Some(source_vector), Some(target_vector)) = (source_vector, target_vector) {
                    similarity =
                        similarity.max(cosine(&source_vector.values, &target_vector.values)?);
                }
            }
        }
        if gated && !(name || overlap > 0 || similarity >= floor) {
            continue;
        }
        evidenced.push(index);
        if name {
            names.push((index, 1.0));
        }
        if overlap > 0 {
            properties.push((index, overlap as f64));
        }
        if similarity >= floor {
            vectors.push((index, similarity));
        }
    }
    if evidenced.len() <= settings.candidate_limit && names.len() <= settings.max_candidate_limit {
        let mut by_index: HashMap<_, _> = locals.into_iter().enumerate().collect();
        return Ok(evidenced
            .into_iter()
            .map(|index| by_index.remove(&index).expect("evidenced index exists"))
            .collect());
    }
    // Names are exhaustive, including hubs. Never turn an incomplete exact set into uniqueness.
    if names.len() > settings.max_candidate_limit {
        return Err("incoming name candidates exceed budget; evidence is incomplete".into());
    }
    let mut fused = BTreeMap::<usize, f64>::new();
    for (source, mut ranked) in [names, properties, vectors].into_iter().enumerate() {
        ranked.sort_by(|a, b| {
            b.1.total_cmp(&a.1)
                .then(locals[a.0].component_id.cmp(&locals[b.0].component_id))
        });
        if source != 0 {
            ranked.truncate(settings.candidate_limit);
        }
        for (rank, (index, _)) in ranked.into_iter().enumerate() {
            *fused.entry(index).or_default() += 1.0 / (61.0 + rank as f64);
        }
    }
    if fused.len() > settings.max_candidate_limit {
        return Err("combined incoming candidates exceed budget; evidence is incomplete".into());
    }
    let mut selected: Vec<_> = fused.into_iter().collect();
    selected.sort_by(|a, b| {
        b.1.total_cmp(&a.1)
            .then(locals[a.0].component_id.cmp(&locals[b.0].component_id))
    });
    tracing::debug!(
        available = locals.len(),
        selected = selected.len(),
        coverage = "ranked_window",
        "incoming identity candidate frontier"
    );
    let mut by_index: HashMap<_, _> = locals.into_iter().enumerate().collect();
    Ok(selected
        .into_iter()
        .map(|(index, _)| by_index.remove(&index).expect("selected index exists"))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kg_core::embedding::ComputedEmbedding;
    use std::sync::Arc;

    fn entity(id: u128, name: &str, vector: Vec<f32>) -> EntityNode {
        let mut e = crate::node::entity_versioning::tests::test_entity(name);
        e.uuid = Uuid::from_u128(id);
        e.chain_id = e.uuid;
        e.embedding = Some(Arc::new(ComputedEmbedding {
            model: "test".into(),
            text_version: "test".into(),
            content_hash: "test".into(),
            values: vector,
        }));
        e
    }

    type Fixture = (
        Vec<(usize, EntityNode)>,
        Vec<LocalCandidate>,
        HashMap<Uuid, EntityNode>,
    );

    fn fixture(same_names: bool) -> Fixture {
        let source = entity(1000, "orders", vec![1.0, 0.0]);
        let mut embedded = HashMap::from([(source.uuid, source.clone())]);
        let locals = (1..=150)
            .map(|id| {
                let e = entity(
                    id,
                    if same_names || id == 150 {
                        "orders"
                    } else {
                        "unrelated"
                    },
                    if id == 150 {
                        vec![1.0, 0.0]
                    } else {
                        vec![0.0, 1.0]
                    },
                );
                embedded.insert(e.uuid, e.clone());
                LocalCandidate {
                    component_id: e.chain_id,
                    members: vec![(0, e)],
                    stored: None,
                }
            })
            .collect();
        (vec![(0, source)], locals, embedded)
    }

    #[test]
    fn large_mixed_frontier_keeps_late_exact_and_semantic_candidate() {
        let (members, locals, embedded) = fixture(false);
        let selected = select(&members, locals, &embedded, &MatchingSettings::default()).unwrap();
        assert!(selected.len() <= 30);
        assert_eq!(selected[0].component_id, Uuid::from_u128(150));
    }

    #[test]
    fn medium_frontiers_use_the_same_ranked_windows_as_large_frontiers() {
        let (members, mut locals, embedded) = fixture(false);
        locals.drain(0..70);
        let selected = select(&members, locals, &embedded, &MatchingSettings::default()).unwrap();
        assert!(selected.len() <= 30);
        assert_eq!(selected[0].component_id, Uuid::from_u128(150));
    }

    #[test]
    fn many_exact_names_still_fail_closed() {
        let (members, locals, embedded) = fixture(true);
        assert!(
            select(&members, locals, &embedded, &MatchingSettings::default())
                .err()
                .unwrap()
                .contains("name candidates")
        );
    }

    #[test]
    fn ranked_windows_keep_aliases_and_typed_property_matches_without_name_equality() {
        let (mut members, mut locals, embedded) = fixture(false);
        members[0].1.name = "logical checkout service".into();
        members[0].1.all_properties.insert(
            "resource_id".into(),
            kg_core::models::PropertyValue::Integer(77),
        );
        locals[148].members[0].1.all_properties.insert(
            "resource_id".into(),
            kg_core::models::PropertyValue::Integer(77),
        );
        let selected = select(&members, locals, &embedded, &MatchingSettings::default()).unwrap();
        assert!(selected
            .iter()
            .any(|c| c.component_id == Uuid::from_u128(149)));
        assert!(selected
            .iter()
            .any(|c| c.component_id == Uuid::from_u128(150)));
    }

    #[test]
    fn retrieval_order_does_not_change_the_selected_frontier() {
        let (members, locals, embedded) = fixture(false);
        let expected: Vec<_> = select(&members, locals, &embedded, &MatchingSettings::default())
            .unwrap()
            .into_iter()
            .map(|c| c.component_id)
            .collect();
        let (_, mut locals, _) = fixture(false);
        locals.reverse();
        let actual: Vec<_> = select(&members, locals, &embedded, &MatchingSettings::default())
            .unwrap()
            .into_iter()
            .map(|c| c.component_id)
            .collect();
        assert_eq!(actual, expected);
    }

    fn keyless((mut members, mut locals, mut embedded): Fixture) -> Fixture {
        for (_, e) in &mut members {
            e.primary_key_properties.clear();
            e.additional_key_properties.clear();
        }
        for local in &mut locals {
            for (_, e) in &mut local.members {
                e.primary_key_properties.clear();
                e.additional_key_properties.clear();
            }
        }
        for e in embedded.values_mut() {
            e.primary_key_properties.clear();
            e.additional_key_properties.clear();
        }
        (members, locals, embedded)
    }

    #[test]
    fn unrelated_incoming_components_are_not_candidates() {
        // 149 "unrelated" components with orthogonal vectors and no shared
        // property: only the same-name, similar component is offered.
        let (members, locals, embedded) = keyless(fixture(false));
        let selected = select(&members, locals, &embedded, &MatchingSettings::default()).unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].component_id, Uuid::from_u128(150));
        // Keyed components are not gated: every compatible incoming component stays offered.
        let (members, locals, embedded) = fixture(false);
        assert!(
            select(&members, locals, &embedded, &MatchingSettings::default())
                .unwrap()
                .len()
                > 1
        );
        // Below the floor a same-name component still qualifies on its name.
        let (members, mut locals, mut embedded) = keyless(fixture(false));
        let renamed = &mut locals[0].members[0].1;
        renamed.name = "Orders".into();
        embedded.get_mut(&renamed.uuid).unwrap().name = "Orders".into();
        let selected = select(&members, locals, &embedded, &MatchingSettings::default()).unwrap();
        let ids: Vec<_> = selected.iter().map(|c| c.component_id).collect();
        assert!(ids.contains(&Uuid::from_u128(1)) && ids.contains(&Uuid::from_u128(150)));
        assert_eq!(ids.len(), 2);
    }

    #[test]
    fn invalid_vectors_never_silently_drop_candidates() {
        assert!(cosine(&[0.0], &[1.0]).is_err());
        assert!(cosine(&[1.0], &[1.0, 2.0]).is_err());
        assert!(cosine(&[f32::NAN], &[1.0]).is_err());
    }
}
