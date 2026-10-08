//! Process-local adopt-once schema store for tests: on conflict the
//! existing schema wins, as a durable store must behave.

use std::collections::HashMap;
use std::sync::RwLock;

use async_trait::async_trait;

use crate::errors::BackendError;
use crate::traits::schema_store::{InferredSchema, SchemaStore};

/// Process-local [`SchemaStore`] for tests.
#[derive(Debug, Default)]
pub struct InMemorySchemaStore {
    schemas: RwLock<HashMap<(String, String, String), InferredSchema>>,
}

impl InMemorySchemaStore {
    /// Empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl SchemaStore for InMemorySchemaStore {
    async fn get(
        &self,
        org_id: &str,
        source: &str,
        entity_type: &str,
    ) -> Result<Option<InferredSchema>, BackendError> {
        Ok(self
            .schemas
            .read()
            .unwrap()
            .get(&(org_id.into(), source.into(), entity_type.into()))
            .cloned())
    }

    async fn adopt(&self, schema: InferredSchema) -> Result<InferredSchema, BackendError> {
        let key = (
            schema.org_id.clone(),
            schema.source.clone(),
            schema.entity_type.clone(),
        );
        let mut map = self.schemas.write().unwrap();
        Ok(map.entry(key).or_insert(schema).clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema(pks: &[&str]) -> InferredSchema {
        InferredSchema {
            org_id: "org-1".into(),
            source: "aws".into(),
            entity_type: "NatGateway".into(),
            primary_key_properties: pks.iter().map(|s| s.to_string()).collect(),
            fk_property_hints: vec![],
            volatile_property_hints: vec![],
            inferred_by: "test".into(),
            degraded: false,
        }
    }

    #[tokio::test]
    async fn adopt_is_insert_once() {
        let store = InMemorySchemaStore::new();
        assert!(store
            .get("org-1", "aws", "NatGateway")
            .await
            .unwrap()
            .is_none());

        let winner = store.adopt(schema(&["arn"])).await.unwrap();
        assert_eq!(winner.primary_key_properties, ["arn"]);

        let second = store.adopt(schema(&["name"])).await.unwrap();
        assert_eq!(
            second.primary_key_properties,
            ["arn"],
            "first adoption wins"
        );
        let stored = store
            .get("org-1", "aws", "NatGateway")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.primary_key_properties, ["arn"]);
    }

    #[tokio::test]
    async fn scoping_is_org_source_type() {
        let store = InMemorySchemaStore::new();
        store.adopt(schema(&["arn"])).await.unwrap();
        let mut other_org = schema(&["name"]);
        other_org.org_id = "org-2".into();
        let adopted = store.adopt(other_org).await.unwrap();
        assert_eq!(
            adopted.primary_key_properties,
            ["name"],
            "org-2 adopts independently"
        );
        assert!(store
            .get("org-1", "gcp", "NatGateway")
            .await
            .unwrap()
            .is_none());
    }
}
