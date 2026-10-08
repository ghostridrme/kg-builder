//! Rule proposals derived from aggregated reference evidence.
//!
//! A proposal turns a well-supported candidate pattern into a structurally
//! valid [`ReferenceMapping`] plus its owner slot and support counts. It never
//! activates anything: proposing is deliberately cheap and frequency-driven,
//! while activation is gated separately by held-out validation. A pattern with
//! too little distinct evidence produces no proposal, but individual reference
//! resolution for that pattern is unaffected — a rare valid edge still resolves.
use crate::runtime::extraction::{ReferenceDirection, ReferenceMapping};
use crate::runtime::rule_learning::evidence::EvidenceAggregate;
use serde::{Deserialize, Serialize};

/// Minimum distinct positive examples before a pattern is worth proposing.
pub const MIN_PROPOSAL_POSITIVES: usize = 3;
/// Minimum distinct positive *values*; guards against one value repeated.
pub const MIN_PROPOSAL_DISTINCT_VALUES: usize = 2;

/// A proposed learned rule awaiting validation and a decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleProposal {
    pub mapping: ReferenceMapping,
    pub namespace: String,
    /// The source field whose observation owns the rule's relationships.
    pub owner_slot: String,
    pub positives: usize,
    pub negatives: usize,
    pub distinct_values: usize,
}

/// Learned reference rules use the same stable generic name as direct
/// references. Their source path and target key group carry the distinction.
pub fn default_relationship_name(_target_type: &str) -> String {
    "RELATES_TO".into()
}

/// Build a proposal from one aggregate, or `None` when the distinct evidence is
/// below the proposal floor. Frequency alone here only *proposes*; it never
/// activates or rejects, and never suppresses individual resolution.
pub fn propose(aggregate: &EvidenceAggregate) -> Option<RuleProposal> {
    if aggregate.positives < MIN_PROPOSAL_POSITIVES
        || aggregate.distinct_values < MIN_PROPOSAL_DISTINCT_VALUES
    {
        return None;
    }
    let key = &aggregate.key;
    // Scope inheritance cannot be learned from a group name without paths.
    if key.target_key_group.len() > 1 && key.component_paths.is_none() {
        return None;
    }
    let mut context_paths = key.component_paths.clone().unwrap_or_default();
    let is_list = key.reference_path.contains("[]");
    if is_list {
        // The varying component comes from each list element; siblings are
        // completed using wildcard paths bound to that same occurrence.
        context_paths.retain(|_, path| path != &key.reference_path);
        if key
            .target_key_group
            .len()
            .saturating_sub(context_paths.len())
            != 1
        {
            return None;
        }
    }
    let mapping = ReferenceMapping {
        source_entity_type: key.source_entity_type.clone(),
        source_namespace: Some(key.source_namespace.clone()),
        reference_path: key.reference_path.clone(),
        context_paths,
        target_type: key.target_type.clone(),
        target_key_group: key.target_key_group.clone(),
        shape: if is_list {
            crate::runtime::extraction::ReferenceShape::List
        } else {
            Default::default()
        },
        direction: ReferenceDirection::default(),
        relationship_name: default_relationship_name(&key.target_type),
        qualifiers: None,
        cardinality: if is_list {
            crate::runtime::extraction::ReferenceCardinality::Many
        } else {
            Default::default()
        },
        case_insensitive_types: Vec::new(),
    };
    // A malformed pattern (e.g. an empty key group) is never proposed.
    mapping.validate().ok()?;
    Some(RuleProposal {
        mapping,
        namespace: key.source_namespace.clone(),
        owner_slot: key.owner_slot(),
        positives: aggregate.positives,
        negatives: aggregate.negatives,
        distinct_values: aggregate.distinct_values,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::rule_learning::evidence::PatternKey;

    fn agg(positives: usize, distinct_values: usize, negatives: usize) -> EvidenceAggregate {
        EvidenceAggregate {
            key: PatternKey {
                component_paths: None,
                source_entity_type: "CmdbChange".into(),
                source_namespace: "prod".into(),
                reference_path: "owning_group".into(),
                target_type: "CmdbGroup".into(),
                target_key_group: vec!["group_id".into()],
            },
            positives,
            negatives,
            distinct_values,
        }
    }

    #[test]
    fn well_supported_pattern_yields_a_valid_mapping() {
        let proposal = propose(&agg(5, 4, 1)).unwrap();
        assert_eq!(proposal.mapping.source_entity_type, "CmdbChange");
        assert_eq!(proposal.mapping.target_type, "CmdbGroup");
        assert_eq!(proposal.mapping.relationship_name, "RELATES_TO");
        assert_eq!(proposal.owner_slot, "CmdbChange.owning_group");
        assert!(proposal.mapping.validate().is_ok());
    }

    #[test]
    fn thin_or_single_valued_evidence_does_not_propose() {
        assert!(propose(&agg(2, 2, 0)).is_none(), "too few positives");
        assert!(
            propose(&agg(9, 1, 0)).is_none(),
            "one value repeated is not distinct evidence"
        );
    }

    #[test]
    fn default_name_is_stable_across_target_types() {
        assert_eq!(default_relationship_name("Gcp.Project"), "RELATES_TO");
    }
    #[test]
    fn composite_proposals_require_real_paths_and_retain_them() {
        let mut a = agg(5, 4, 0);
        a.key.target_key_group = vec!["account".into(), "disk_id".into()];
        assert!(propose(&a).is_none());
        a.key.component_paths = Some(
            [
                ("account".into(), "attachment.OwnerId".into()),
                ("disk_id".into(), "attachment.VolumeId".into()),
            ]
            .into(),
        );
        assert_eq!(
            propose(&a).unwrap().mapping.context_paths,
            a.key.component_paths.unwrap()
        );
    }

    #[test]
    fn array_proposal_binds_siblings_to_each_element() {
        let mut a = agg(5, 4, 0);
        a.key.reference_path = "attachments[].VolumeId".into();
        a.key.target_key_group = vec!["account".into(), "disk_id".into()];
        a.key.component_paths = Some(
            [
                ("account".into(), "attachments[].OwnerId".into()),
                ("disk_id".into(), "attachments[].VolumeId".into()),
            ]
            .into(),
        );
        let mapping = propose(&a).unwrap().mapping;
        assert_eq!(
            mapping.shape,
            crate::runtime::extraction::ReferenceShape::List
        );
        assert_eq!(mapping.context_paths.len(), 1);
        assert_eq!(mapping.context_paths["account"], "attachments[].OwnerId");
    }
}
