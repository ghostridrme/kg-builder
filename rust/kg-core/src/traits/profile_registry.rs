//! Immutable organization-scoped profile registry. Revisions are never overwritten.
use crate::{
    errors::BackendError,
    profiles::{validate_org, FrozenProfile, Profile, ProfileRef},
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::RwLock};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileEntry {
    pub reference: ProfileRef,
    pub digest: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfilePage {
    pub items: Vec<ProfileEntry>,
    pub next: Option<ProfileRef>,
}

pub fn validate_page(
    org: &str,
    after: Option<&ProfileRef>,
    limit: usize,
) -> Result<(), BackendError> {
    validate_org(org)?;
    if !(1..=100).contains(&limit) {
        return Err(crate::profiles::invalid("page limit must be 1..100"));
    }
    if let Some(after) = after {
        after.validate()?;
    }
    Ok(())
}
#[async_trait]
pub trait ProfileRegistry: Send + Sync {
    async fn put(&self, org: &str, document: Profile) -> Result<FrozenProfile, BackendError>;
    async fn get(
        &self,
        org: &str,
        reference: &ProfileRef,
    ) -> Result<Option<FrozenProfile>, BackendError>;
    async fn list(
        &self,
        org: &str,
        after: Option<&ProfileRef>,
        limit: usize,
    ) -> Result<ProfilePage, BackendError>;
}
#[derive(Default)]
pub struct InMemoryProfileRegistry {
    entries: RwLock<BTreeMap<(String, ProfileRef), FrozenProfile>>,
}
#[async_trait]
impl ProfileRegistry for InMemoryProfileRegistry {
    async fn put(&self, org: &str, document: Profile) -> Result<FrozenProfile, BackendError> {
        validate_org(org)?;
        let frozen = document.freeze()?;
        let mut entries = self
            .entries
            .write()
            .map_err(|_| BackendError::Unavailable("profile lock poisoned".into()))?;
        let stored = entries
            .entry((org.into(), document.reference()))
            .or_insert_with(|| frozen.clone());
        if stored != &frozen {
            return Err(BackendError::Conflict(
                "profile revision already has different content".into(),
            ));
        }
        Ok(stored.clone())
    }
    async fn get(
        &self,
        org: &str,
        reference: &ProfileRef,
    ) -> Result<Option<FrozenProfile>, BackendError> {
        validate_org(org)?;
        reference.validate()?;
        Ok(self
            .entries
            .read()
            .map_err(|_| BackendError::Unavailable("profile lock poisoned".into()))?
            .get(&(org.into(), reference.clone()))
            .cloned())
    }
    async fn list(
        &self,
        org: &str,
        after: Option<&ProfileRef>,
        limit: usize,
    ) -> Result<ProfilePage, BackendError> {
        validate_page(org, after, limit)?;
        let entries = self
            .entries
            .read()
            .map_err(|_| BackendError::Unavailable("profile lock poisoned".into()))?;
        let mut items: Vec<_> = entries
            .iter()
            .filter(|((scope, reference), _)| scope == org && after.is_none_or(|a| reference > a))
            .take(limit + 1)
            .map(|((_, reference), value)| ProfileEntry {
                reference: reference.clone(),
                digest: value.digest.clone(),
            })
            .collect();
        let more = items.len() > limit;
        items.truncate(limit);
        Ok(ProfilePage {
            next: if more {
                items.last().map(|e| e.reference.clone())
            } else {
                None
            },
            items,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn profile(revision: u64) -> Profile {
        serde_json::from_value(serde_json::json!({"format_version":1,"profile_id":"service","revision":revision,"mode":"open","ontology":{}})).unwrap()
    }
    #[tokio::test]
    async fn profile_registry_is_immutable_scoped_and_paged() {
        let registry = std::sync::Arc::new(InMemoryProfileRegistry::default());
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let r = registry.clone();
            tasks.push(tokio::spawn(async move { r.put("org", profile(1)).await }));
        }
        for task in tasks {
            task.await.unwrap().unwrap();
        }
        let mut changed = profile(1);
        changed.mode = crate::profiles::ProfileMode::Strict;
        assert!(matches!(
            registry.put("org", changed).await,
            Err(BackendError::Conflict(_))
        ));
        assert!(registry
            .get("other", &profile(1).reference())
            .await
            .unwrap()
            .is_none());
        registry.put("org", profile(2)).await.unwrap();
        let first = registry.list("org", None, 1).await.unwrap();
        assert_eq!(first.items.len(), 1);
        let last = registry.list("org", first.next.as_ref(), 1).await.unwrap();
        assert_eq!(last.items[0].reference.revision, 2);
        assert!(last.next.is_none());
        assert!(registry.list("org", None, 101).await.is_err());
    }
}
