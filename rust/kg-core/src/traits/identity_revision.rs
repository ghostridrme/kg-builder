//! Optimistic concurrency for graph-dependent identity decisions.

use crate::errors::BackendError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityScope {
    pub namespace: String,
    pub entity_type: String,
}

impl IdentityScope {
    pub fn validate(&self) -> Result<(), BackendError> {
        if [&self.namespace, &self.entity_type]
            .iter()
            .any(|s| s.trim().is_empty())
        {
            return Err(BackendError::Query(
                "identity scope must be nonblank".into(),
            ));
        }
        Ok(())
    }

    pub fn key(&self, org: &str) -> Result<String, BackendError> {
        self.validate()?;
        if org.trim().is_empty() {
            return Err(BackendError::Query(
                "identity scope requires organization".into(),
            ));
        }
        serde_json::to_string(&(org, &self.namespace, &self.entity_type))
            .map_err(|_| BackendError::Serialization("identity scope".into()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityRevision {
    pub scope: IdentityScope,
    pub revision: u64,
}

impl IdentityRevision {
    pub fn validate(&self) -> Result<(), BackendError> {
        self.scope.validate()?;
        if self.revision >= i64::MAX as u64 {
            return Err(BackendError::Query(
                "identity revision is out of range".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_keys_preserve_boundaries_and_revision_bounds() {
        let first = IdentityScope {
            namespace: "a:b".into(),
            entity_type: "c".into(),
        };
        let other = IdentityScope {
            namespace: "a".into(),
            entity_type: "b:c".into(),
        };
        assert_ne!(first.key("org").unwrap(), other.key("org").unwrap());
        assert_ne!(first.key("org").unwrap(), first.key("other").unwrap());
        assert!(first.key(" ").is_err());
        assert!(IdentityRevision {
            scope: first,
            revision: i64::MAX as u64
        }
        .validate()
        .is_err());
    }
}
