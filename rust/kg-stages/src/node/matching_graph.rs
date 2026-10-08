//! Validate every local identity decision before applying any mapping.
use kg_core::{errors::StageError, models::EntityNode};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Decision {
    Stored(Uuid),
    New,
    GroundedNew,
    MatchLocal(Uuid),
    Insufficient,
}

#[derive(Debug, Clone)]
pub(super) struct Component {
    pub id: Uuid,
    pub members: Vec<EntityNode>,
    pub decision: Decision,
    pub inferred_type: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Anchor {
    Stored(Uuid),
    New(Uuid),
}

fn invalid(message: &str) -> StageError {
    StageError::StateValidation {
        stage: "fuzzy_match".into(),
        message: message.into(),
    }
}

fn scope(component: &Component) -> (&str, &str, &str) {
    let member = &component.members[0];
    (&member.org_id, &member.namespace, &member.entity_type)
}

/// Return only cycle members; dependent paths are handled by decline propagation.
pub(super) fn cycles(components: &BTreeMap<Uuid, Component>) -> Vec<Vec<Uuid>> {
    let mut visited = BTreeSet::new();
    let mut cycles = Vec::new();
    for start in components.keys() {
        let mut positions = HashMap::new();
        let mut path = Vec::new();
        let mut current = *start;
        loop {
            if let Some(&position) = positions.get(&current) {
                let mut cycle = path[position..].to_vec();
                cycle.sort_unstable();
                cycles.push(cycle);
                break;
            }
            if !visited.insert(current) {
                break;
            }
            positions.insert(current, path.len());
            path.push(current);
            match components
                .get(&current)
                .map(|component| &component.decision)
            {
                Some(Decision::MatchLocal(target)) => current = *target,
                _ => break,
            }
        }
    }
    cycles.sort();
    cycles
}

/// Clarification cannot anchor to a cycle, a declined component, or their dependents.
pub(super) fn unanchored_components(components: &BTreeMap<Uuid, Component>) -> Vec<Uuid> {
    let mut reverse: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    let mut blocked: BTreeSet<_> = cycles(components).into_iter().flatten().collect();
    for (&id, component) in components {
        match component.decision {
            Decision::Insufficient => {
                blocked.insert(id);
            }
            Decision::MatchLocal(target) => {
                reverse.entry(target).or_default().push(id);
                if !components.contains_key(&target) {
                    blocked.insert(id);
                }
            }
            _ => {}
        }
    }
    let mut pending: Vec<_> = blocked.iter().copied().collect();
    while let Some(target) = pending.pop() {
        for &dependent in reverse.get(&target).into_iter().flatten() {
            if blocked.insert(dependent) {
                pending.push(dependent);
            }
        }
    }
    blocked.into_iter().collect()
}

/// Validate mappings and incoming key consistency. The caller checks final stored
/// targets against their stored keys; model identity judgments still require evaluation.
pub(super) fn validate(components: &[Component]) -> Result<HashMap<Uuid, Anchor>, StageError> {
    let mut by_id = BTreeMap::new();
    let mut observation_ids = BTreeSet::new();
    for component in components {
        if component.id.is_nil()
            || component.members.is_empty()
            || by_id.insert(component.id, component).is_some()
        {
            return Err(invalid("missing or duplicate identity component"));
        }
        let expected = scope(component);
        for member in &component.members {
            member
                .validate()
                .map_err(|_| invalid("invalid identity component member"))?;
            if member.uuid.is_nil()
                || !observation_ids.insert(member.uuid)
                || (&*member.org_id, &*member.namespace, &*member.entity_type) != expected
            {
                return Err(invalid(
                    "duplicate observation or mixed identity component scope",
                ));
            }
        }
    }
    let mut resolved = HashMap::new();
    for start in by_id.keys() {
        let mut path = Vec::new();
        let mut seen = BTreeSet::new();
        let mut current = *start;
        let anchor = loop {
            if let Some(anchor) = resolved.get(&current) {
                break *anchor;
            }
            if !seen.insert(current) {
                return Err(invalid("cyclic local identity decisions"));
            }
            let component = by_id
                .get(&current)
                .ok_or_else(|| invalid("unknown local identity target"))?;
            path.push(current);
            match component.decision {
                Decision::Stored(id) if !id.is_nil() => break Anchor::Stored(id),
                Decision::GroundedNew => break Anchor::New(current),
                Decision::New => {
                    if !component
                        .members
                        .iter()
                        .any(EntityNode::has_authoritative_keys)
                    {
                        return Err(invalid("keyless new identity requires an accepted anchor"));
                    }
                    break Anchor::New(current);
                }
                Decision::MatchLocal(target) => {
                    let next = by_id
                        .get(&target)
                        .ok_or_else(|| invalid("unknown local identity target"))?;
                    if current == target {
                        return Err(invalid("identity component cannot match itself"));
                    }
                    if scope(component).0 != scope(next).0
                        || scope(component).1 != scope(next).1
                        || (!component.inferred_type && scope(component).2 != scope(next).2)
                    {
                        return Err(invalid("local identity target crosses scope"));
                    }
                    current = target;
                }
                Decision::Stored(_) => return Err(invalid("invalid stored identity target")),
                Decision::Insufficient => {
                    return Err(invalid("insufficient evidence for entity identity"))
                }
            }
        };
        for id in path {
            resolved.insert(id, anchor);
        }
    }
    let mut groups = BTreeMap::<Anchor, Vec<&Component>>::new();
    for component in components {
        groups
            .entry(resolved[&component.id])
            .or_default()
            .push(component);
    }
    for group in groups.values() {
        let expected = scope(group[0]);
        let declared_type = group.iter().find(|c| !c.inferred_type).map(|c| scope(c).2);
        if group.iter().any(|component| {
            let actual = scope(component);
            actual.0 != expected.0
                || actual.1 != expected.1
                || (!component.inferred_type && Some(actual.2) != declared_type)
        }) {
            return Err(invalid("identity anchor crosses scope"));
        }
        validate_keys(group)?;
    }
    Ok(resolved)
}

/// Declared keys override semantic similarity; undeclared names remain aliases.
pub(super) fn compatible(left: &EntityNode, right: &EntityNode) -> bool {
    let declares_name = |entity: &EntityNode| {
        entity
            .primary_key_properties
            .iter()
            .chain(entity.additional_key_properties.iter().flatten())
            .any(|key| key == "name")
    };
    if declares_name(left) && declares_name(right) && left.name != right.name {
        return false;
    }
    left.primary_key_properties
        .iter()
        .chain(left.additional_key_properties.iter().flatten())
        .chain(right.primary_key_properties.iter())
        .chain(right.additional_key_properties.iter().flatten())
        .filter(|key| key.as_str() != "name")
        .all(
            |key| match (left.all_properties.get(key), right.all_properties.get(key)) {
                (Some(a), Some(b)) => a == b,
                _ => true,
            },
        )
}

fn validate_keys(components: &[&Component]) -> Result<(), StageError> {
    // Authoritative components already describe one identity across time. Check
    // newly joined components without forbidding changes within their own history.
    for (index, left) in components.iter().enumerate() {
        for right in &components[index + 1..] {
            if left
                .members
                .iter()
                .any(|a| right.members.iter().any(|b| !compatible(a, b)))
            {
                return Err(invalid(
                    "transitive identity decisions conflict with authoritative keys",
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kg_core::models::PropertyValue;
    fn component(name: &str, decision: Decision) -> Component {
        let mut node = super::super::entity_versioning::tests::test_entity(name);
        node.primary_key_properties.clear();
        Component {
            id: Uuid::new_v4(),
            members: vec![node],
            inferred_type: false,
            decision,
        }
    }
    #[test]
    fn only_inferred_types_can_cross_classifications_and_never_scope() {
        let anchor = component("api", Decision::GroundedNew);
        let mut alias = component("the api", Decision::MatchLocal(anchor.id));
        alias.members[0].entity_type = "System".into();
        assert!(validate(&[anchor.clone(), alias.clone()]).is_err());
        alias.inferred_type = true;
        assert_eq!(
            validate(&[anchor.clone(), alias.clone()]).unwrap()[&alias.id],
            Anchor::New(anchor.id)
        );
        alias.members[0].namespace = "elsewhere".into();
        assert!(validate(&[anchor.clone(), alias.clone()]).is_err());
        alias.members[0].namespace = anchor.members[0].namespace.clone();
        alias.members[0].org_id = "another-org".into();
        assert!(validate(&[anchor, alias]).is_err());
    }

    fn keyed(component: &mut Component, key: &str, value: PropertyValue) {
        component.members[0].primary_key_properties = vec![key.into()];
        component.members[0]
            .all_properties
            .insert(key.into(), value);
    }
    #[test]
    fn grounded_keyless_anchor_accepts_local_aliases_without_inventing_keys() {
        let anchor = component("deployment", Decision::GroundedNew);
        let alias = component("the deployment", Decision::MatchLocal(anchor.id));
        let expected = Anchor::New(anchor.id);
        let original = vec![anchor, alias];
        let resolved = validate(&original).unwrap();
        assert!(resolved.values().all(|value| *value == expected));
        assert!(original
            .iter()
            .flat_map(|component| &component.members)
            .all(|member| member.primary_key_properties.is_empty()
                && member.additional_key_properties.is_empty()));
    }

    #[test]
    fn chains_resolve_identically_regardless_of_input_order() {
        let mut a = component("anchor", Decision::New);
        keyed(&mut a, "id", PropertyValue::String("one".into()));
        let b = component("alias", Decision::MatchLocal(a.id));
        let c = component("another alias", Decision::MatchLocal(b.id));
        let input = vec![a.clone(), b.clone(), c.clone()];
        let expected = validate(&input).unwrap();
        assert!(expected.values().all(|anchor| *anchor == Anchor::New(a.id)));
        assert_eq!(validate(&[c, b, a]).unwrap(), expected);
    }
    #[test]
    fn cycles_exclude_dependents_and_valid_anchors() {
        let mut a = component("a", Decision::New);
        let b = component("b", Decision::MatchLocal(a.id));
        a.decision = Decision::MatchLocal(b.id);
        let dependent = component("dependent", Decision::MatchLocal(a.id));
        let good = component("good", Decision::GroundedNew);
        let alias = component("alias", Decision::MatchLocal(good.id));
        let mut expected = vec![a.id, b.id];
        expected.sort();
        let graph = [a, b, dependent, good, alias]
            .into_iter()
            .map(|c| (c.id, c))
            .collect();
        assert_eq!(cycles(&graph), vec![expected]);
    }

    #[test]
    fn clarification_blocks_transitive_dependents_but_keeps_grounded_targets() {
        let mut a = component("cycle-a", Decision::New);
        let b = component("cycle-b", Decision::MatchLocal(a.id));
        a.decision = Decision::MatchLocal(b.id);
        let c = component("off-cycle-alias", Decision::MatchLocal(a.id));
        let d = component("transitive-alias", Decision::MatchLocal(c.id));
        let rejected = component("rejected", Decision::Insufficient);
        let dependent = component("rejected-alias", Decision::MatchLocal(rejected.id));
        let missing = component("missing-target", Decision::MatchLocal(Uuid::new_v4()));
        let stored = component("stored", Decision::Stored(Uuid::new_v4()));
        let grounded = component("grounded", Decision::GroundedNew);
        let grounded_alias = component("grounded-alias", Decision::MatchLocal(grounded.id));
        let mut expected = vec![
            a.id,
            b.id,
            c.id,
            d.id,
            rejected.id,
            dependent.id,
            missing.id,
        ];
        expected.sort_unstable();
        let graph = [
            a,
            b,
            c,
            d,
            rejected,
            dependent,
            missing,
            stored,
            grounded,
            grounded_alias,
        ]
        .into_iter()
        .map(|c| (c.id, c))
        .collect();
        assert_eq!(unanchored_components(&graph), expected);
    }

    #[test]
    fn dangling_self_cyclic_and_insufficient_targets_reject() {
        let mut a = component("a", Decision::MatchLocal(Uuid::new_v4()));
        assert!(validate(&[a.clone()]).is_err());
        a.decision = Decision::MatchLocal(a.id);
        assert!(validate(&[a.clone()]).is_err());
        let b = component("b", Decision::MatchLocal(a.id));
        a.decision = Decision::MatchLocal(b.id);
        assert!(validate(&[a.clone(), b.clone()]).is_err());
        a.decision = Decision::Insufficient;
        assert!(validate(&[a, b]).is_err());
    }
    #[test]
    fn transitive_and_converging_stored_matches_check_all_declared_keys() {
        let stored = Uuid::new_v4();
        let mut a = component("a", Decision::Stored(stored));
        keyed(&mut a, "id", PropertyValue::String("one".into()));
        let b = component("b", Decision::MatchLocal(a.id));
        let mut c = component("c", Decision::MatchLocal(b.id));
        keyed(&mut c, "id", PropertyValue::String("two".into()));
        assert!(validate(&[a.clone(), b, c.clone()]).is_err());
        c.decision = Decision::Stored(stored);
        assert!(validate(&[a, c]).is_err());
    }
    #[test]
    fn keyless_aliases_do_not_invent_name_keys_but_declared_names_conflict() {
        let mut a = component("canonical", Decision::New);
        a.members[0].primary_key_properties = vec!["name".into()];
        let mut b = component("friendly alias", Decision::MatchLocal(a.id));
        assert!(validate(&[a.clone(), b.clone()]).is_ok());
        b.members[0].primary_key_properties = vec!["name".into()];
        assert!(validate(&[a, b]).is_err());
    }
    #[test]
    fn prejoined_history_is_preserved_and_keyless_new_is_rejected() {
        let keyless = component("alias", Decision::New);
        assert!(validate(&[keyless]).is_err());
        let mut authoritative = component("api", Decision::New);
        keyed(
            &mut authoritative,
            "id",
            PropertyValue::String("one".into()),
        );
        let mut later = authoritative.members[0].clone();
        later.uuid = Uuid::new_v4();
        later
            .all_properties
            .insert("id".into(), PropertyValue::String("two".into()));
        authoritative.members.push(later);
        assert!(validate(&[authoritative]).is_ok());
    }

    #[test]
    fn scopes_duplicates_and_typed_key_conflicts_reject() {
        let a = component("a", Decision::Stored(Uuid::new_v4()));
        let mut b = component("b", Decision::MatchLocal(a.id));
        b.members[0].namespace = "elsewhere".into();
        assert!(validate(&[a.clone(), b]).is_err());
        assert!(validate(&[a.clone(), a]).is_err());
        let mut a = component("a", Decision::New);
        keyed(&mut a, "id", PropertyValue::Integer(1));
        let mut b = component("b", Decision::MatchLocal(a.id));
        keyed(&mut b, "id", PropertyValue::String("1".into()));
        assert!(validate(&[a, b]).is_err());
    }
}
