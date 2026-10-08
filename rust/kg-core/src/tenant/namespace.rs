use serde::{Deserialize, Serialize};

/// Discovered relationship linking policy, not search authorization or organization isolation.
/// Closed mode permits same namespace, shared tier, or a directional tier grant;
/// any grant suffices when namespaces belong to multiple tiers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespacePolicy {
    pub environment_tiers: Vec<EnvironmentTier>,
    pub cross_namespace_rules: Vec<CrossNamespaceRule>,
    /// Cross-namespace discovery requires an explicit grant or open policy.
    #[serde(default)]
    pub open_policy: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentTier {
    pub name: String,
    pub namespaces: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrossNamespaceRule {
    pub source_tier: String,
    pub target_tiers: Vec<String>,
}

impl NamespacePolicy {
    /// Reject ambiguous tier names and grants that reference missing tiers.
    pub fn validate(&self) -> Result<(), String> {
        let mut names = std::collections::HashSet::new();
        for tier in &self.environment_tiers {
            if tier.name.trim().is_empty() || !names.insert(tier.name.as_str()) {
                return Err("namespace tier names must be nonblank and unique".into());
            }
            if tier.namespaces.iter().any(|name| name.trim().is_empty()) {
                return Err("namespace tier entries must not be blank".into());
            }
        }
        for rule in &self.cross_namespace_rules {
            if !names.contains(rule.source_tier.as_str())
                || rule
                    .target_tiers
                    .iter()
                    .any(|name| !names.contains(name.as_str()))
            {
                return Err("namespace grants must reference declared tiers".into());
            }
        }
        Ok(())
    }

    /// Every namespace a discovered edge from `source` may target, sorted and
    /// deduplicated; `None` under an open policy (no restriction). Lets a bounded
    /// candidate read filter by scope *before* its per-value cap, so a capped page
    /// can never hide an eligible target behind ineligible ones.
    pub fn allowed_targets(&self, source: &str) -> Option<Vec<String>> {
        if self.open_policy {
            return None;
        }
        let mut allowed = vec![source.to_owned()];
        for source_tier in self
            .environment_tiers
            .iter()
            .filter(|tier| tier.namespaces.iter().any(|ns| ns == source))
        {
            allowed.extend(source_tier.namespaces.iter().cloned());
            for rule in self
                .cross_namespace_rules
                .iter()
                .filter(|rule| rule.source_tier == source_tier.name)
            {
                for tier in &self.environment_tiers {
                    if rule.target_tiers.contains(&tier.name) {
                        allowed.extend(tier.namespaces.iter().cloned());
                    }
                }
            }
        }
        allowed.sort();
        allowed.dedup();
        Some(allowed)
    }

    /// Same-namespace edges are always allowed. An open policy needs no tier registry.
    pub fn allows(&self, source: &str, target: &str) -> bool {
        if self.open_policy || source == target {
            return true;
        }
        self.environment_tiers
            .iter()
            .filter(|tier| tier.namespaces.iter().any(|ns| ns == source))
            .any(|source_tier| {
                source_tier.namespaces.iter().any(|ns| ns == target)
                    || self
                        .cross_namespace_rules
                        .iter()
                        .filter(|rule| rule.source_tier == source_tier.name)
                        .any(|rule| {
                            self.environment_tiers.iter().any(|tier| {
                                rule.target_tiers.contains(&tier.name)
                                    && tier.namespaces.iter().any(|ns| ns == target)
                            })
                        })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowed_targets_agrees_with_allows_and_is_none_when_open() {
        let policy = NamespacePolicy {
            open_policy: false,
            environment_tiers: vec![
                EnvironmentTier {
                    name: "prod".into(),
                    namespaces: vec!["api".into(), "db".into()],
                },
                EnvironmentTier {
                    name: "shared".into(),
                    namespaces: vec!["dns".into()],
                },
                EnvironmentTier {
                    name: "dev".into(),
                    namespaces: vec!["sandbox".into()],
                },
            ],
            cross_namespace_rules: vec![CrossNamespaceRule {
                source_tier: "prod".into(),
                target_tiers: vec!["shared".into()],
            }],
        };
        let allowed = policy.allowed_targets("api").unwrap();
        assert_eq!(allowed, vec!["api", "db", "dns"]);
        for target in ["api", "db", "dns", "sandbox", "unknown"] {
            assert_eq!(
                policy.allows("api", target),
                allowed.iter().any(|ns| ns == target),
                "{target}"
            );
        }
        assert_eq!(policy.allowed_targets("orphan").unwrap(), vec!["orphan"]);
        let open = NamespacePolicy {
            open_policy: true,
            ..policy
        };
        assert!(open.allowed_targets("api").is_none());
    }

    #[test]
    fn policy_validation_rejects_ambiguous_names_and_dangling_grants() {
        let base = NamespacePolicy {
            open_policy: false,
            environment_tiers: vec![EnvironmentTier {
                name: "prod".into(),
                namespaces: vec!["api".into()],
            }],
            cross_namespace_rules: vec![],
        };
        base.validate().unwrap();
        let mut duplicate = base.clone();
        duplicate
            .environment_tiers
            .push(base.environment_tiers[0].clone());
        assert!(duplicate.validate().is_err());
        assert!(
            matches!(crate::runtime::RuntimeContextBuilder::new("org").namespace_policy(duplicate).build(), Err(crate::errors::ConfigError::InvalidValue { field, .. }) if field == "namespace_policy")
        );
        for (source, target) in [("unknown", "prod"), ("prod", "unknown")] {
            let mut policy = base.clone();
            policy.cross_namespace_rules.push(CrossNamespaceRule {
                source_tier: source.into(),
                target_tiers: vec![target.into()],
            });
            assert!(policy.validate().is_err());
        }
        let mut overlap = base;
        overlap.environment_tiers.push(EnvironmentTier {
            name: "shared".into(),
            namespaces: vec!["api".into(), "auth".into()],
        });
        overlap.validate().unwrap();
        assert!(overlap.allows("api", "auth"));
    }

    #[test]
    fn open_policy_allows_unregistered_namespaces() {
        let policy = NamespacePolicy {
            environment_tiers: vec![],
            cross_namespace_rules: vec![],
            open_policy: true,
        };
        assert!(policy.allows("production", "shared"));
        let closed = NamespacePolicy {
            open_policy: false,
            ..policy
        };
        assert!(closed.allows("production", "production"));
        assert!(!closed.allows("production", "shared"));
    }

    #[test]
    fn closed_policy_honors_directional_tier_rules() {
        let policy = NamespacePolicy {
            open_policy: false,
            environment_tiers: vec![
                EnvironmentTier {
                    name: "prod".into(),
                    namespaces: vec!["api".into(), "db".into()],
                },
                EnvironmentTier {
                    name: "shared".into(),
                    namespaces: vec!["auth".into()],
                },
            ],
            cross_namespace_rules: vec![CrossNamespaceRule {
                source_tier: "prod".into(),
                target_tiers: vec!["shared".into()],
            }],
        };
        assert!(policy.allows("api", "db"));
        assert!(policy.allows("api", "auth"));
        assert!(!policy.allows("auth", "api"));
        assert!(!policy.allows("api", "unknown"));
    }

    #[test]
    fn omitted_or_misspelled_policy_cannot_enable_open_linking() {
        let mut input = serde_json::json!({
            "environment_tiers": [], "cross_namespace_rules": []
        });
        assert!(
            !serde_json::from_value::<NamespacePolicy>(input.clone())
                .unwrap()
                .open_policy
        );
        input["open_polciy"] = false.into();
        assert!(serde_json::from_value::<NamespacePolicy>(input).is_err());
        assert!(
            serde_json::from_value::<EnvironmentTier>(serde_json::json!({
                "name": "prod", "namespaces": [], "namespace": "sensitive"
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<CrossNamespaceRule>(serde_json::json!({
                "source_tier": "prod", "target_tiers": [], "target_tier": "shared"
            }))
            .is_err()
        );
    }
}
