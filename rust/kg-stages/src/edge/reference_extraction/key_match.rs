//! Complete target keys from one reference object, independent of field spelling.
//!
//! Equal values discover correspondence; the caller still confirms relationship
//! meaning. Repeated equal values cannot assign different key roles by accident.
use std::collections::{BTreeMap, BTreeSet};

use super::{Observation, RelationshipTarget};

pub(super) fn complete_group(
    target: &RelationshipTarget,
    observation: &Observation,
) -> Option<Vec<String>> {
    complete_mapping(target, observation).map(|mapping| mapping.into_keys().collect())
}

pub(super) fn complete_mapping(
    target: &RelationshipTarget,
    observation: &Observation,
) -> Option<BTreeMap<String, String>> {
    let mut by_value: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for (field, token) in observation.context.iter() {
        by_value.entry(token).or_default().insert(field);
    }
    by_value
        .entry(&observation.token)
        .or_default()
        .insert(&observation.component);
    // Canonical ordering makes equivalent declared groups independent of their
    // input order. Each group must stand on its own; components never mix.
    let mut complete = BTreeSet::new();
    for group in &target.key_groups {
        if !group
            .components
            .iter()
            .any(|c| c.token() == observation.token)
        {
            continue;
        }
        let mut used = BTreeSet::new();
        let mut mapping = BTreeMap::new();
        let mut anchored = false;
        let matched = group.components.iter().all(|component| {
            let token = component.token();
            let Some(fields) = by_value.get(token.as_str()) else {
                return false;
            };
            // An explicitly named role with a conflicting value cannot be
            // replaced by an unrelated sibling containing the desired value.
            if observation
                .context
                .iter()
                .any(|(field, value)| field == &component.property && value != &token)
            {
                return false;
            }
            let field = if fields.contains(component.property.as_str()) {
                component.property.as_str()
            } else if fields.len() == 1 {
                *fields.first().expect("nonempty value bucket")
            } else {
                return false;
            };
            anchored |= field == observation.component && token == observation.token;
            let path = if field == observation.component {
                observation.location.clone()
            } else {
                observation.location.rsplit_once('.').map_or_else(
                    || field.to_owned(),
                    |(parent, _)| format!("{parent}.{field}"),
                )
            };
            mapping.insert(component.property.clone(), path);
            used.insert(field)
        });
        if matched && anchored {
            complete.insert(mapping);
        }
    }
    complete.into_iter().next()
}

/// Distinguish missing values from complete values with uncertain field roles.
pub(super) fn ambiguous_correspondence(
    target: &RelationshipTarget,
    observation: &Observation,
) -> bool {
    let mut available: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for (field, token) in observation.context.iter() {
        available.entry(token).or_default().insert(field);
    }
    available
        .entry(&observation.token)
        .or_default()
        .insert(&observation.component);
    target.key_groups.iter().any(|group| {
        let mut required: BTreeMap<String, usize> = BTreeMap::new();
        for component in &group.components {
            *required.entry(component.token()).or_default() += 1;
        }
        required.contains_key(&observation.token)
            && required.iter().all(|(token, count)| {
                available
                    .get(token.as_str())
                    .is_some_and(|fields| fields.len() >= *count)
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use kg_core::runtime::stage_output::{TargetKeyComponent, TargetKeyGroup};
    use std::sync::Arc;
    use uuid::Uuid;

    fn target(groups: &[&[(&str, &str)]]) -> RelationshipTarget {
        RelationshipTarget {
            chain_id: Uuid::new_v4(),
            version_uuid: Uuid::new_v4(),
            version: 1,
            name: "disk".into(),
            entity_type: "Disk".into(),
            namespace: "shared".into(),
            key_groups: groups
                .iter()
                .map(|group| TargetKeyGroup {
                    components: group
                        .iter()
                        .map(|(property, value)| TargetKeyComponent {
                            property: (*property).into(),
                            type_tag: "s".into(),
                            value: (*value).into(),
                        })
                        .collect(),
                })
                .collect(),
        }
    }
    fn observation(fields: &[(&str, &str)]) -> Observation {
        Observation {
            location: "disks[0].VolumeId".into(),
            component: "VolumeId".into(),
            token: "s:vol-1".into(),
            canonical: "vol-1".into(),
            from_collection: true,
            context: Arc::new(
                fields
                    .iter()
                    .map(|(k, v)| ((*k).into(), format!("s:{v}")))
                    .collect(),
            ),
        }
    }
    #[test]
    fn complete_cross_account_key_matches_differently_named_siblings() {
        let t = target(&[&[("account", "B"), ("region", "west"), ("disk_id", "vol-1")]]);
        let o = observation(&[("OwnerId", "B"), ("Region", "west"), ("VolumeId", "vol-1")]);
        assert_eq!(
            complete_group(&t, &o),
            Some(vec!["account".into(), "disk_id".into(), "region".into()])
        );
    }
    #[test]
    fn missing_scope_is_not_filled_from_target() {
        let t = target(&[&[("account", "B"), ("disk_id", "vol-1")]]);
        assert!(complete_group(&t, &observation(&[])).is_none());
    }
    #[test]
    fn unrelated_equal_siblings_do_not_override_explicit_conflict() {
        let t = target(&[&[("account", "B"), ("disk_id", "vol-1")]]);
        assert!(complete_group(&t, &observation(&[("account", "A"), ("tag", "B")])).is_none());
    }
    #[test]
    fn one_occurrence_cannot_fill_two_key_roles() {
        let t = target(&[&[("account", "B"), ("project", "B"), ("disk_id", "vol-1")]]);
        assert!(complete_group(&t, &observation(&[("OwnerId", "B")])).is_none());
    }
    #[test]
    fn ambiguous_correspondence_does_not_choose_a_sibling() {
        let t = target(&[&[("account", "B"), ("disk_id", "vol-1")]]);
        assert!(complete_group(&t, &observation(&[("OwnerId", "B"), ("tag", "B")])).is_none());
    }
    #[test]
    fn complete_alternative_does_not_need_partial_primary() {
        let t = target(&[
            &[("account", "B"), ("disk_id", "vol-1")],
            &[("unique_disk", "vol-1")],
        ]);
        assert_eq!(
            complete_group(&t, &observation(&[])),
            Some(vec!["unique_disk".into()])
        );
    }
}
