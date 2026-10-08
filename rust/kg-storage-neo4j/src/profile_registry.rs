//! Immutable profile publication uses a unique-key MERGE and verifies content before commit.
use crate::{
    driver::{build_query, row_to_json, CallError},
    Neo4jGraphBackend,
};
use async_trait::async_trait;
use kg_core::{
    errors::BackendError,
    profiles::{FrozenProfile, Profile, ProfileRef},
    traits::profile_registry::{ProfilePage, ProfileRegistry},
};
use kg_storage_cypher::profile_registry as queries;
#[async_trait]
impl ProfileRegistry for Neo4jGraphBackend {
    async fn put(&self, org: &str, document: Profile) -> Result<FrozenProfile, BackendError> {
        let frozen = document.freeze()?;
        let query = queries::put(org, &frozen)?;
        let mut txn = tokio::time::timeout(
            self.options.timeout,
            self.graph.start_txn_with_timeout(self.options.timeout),
        )
        .await
        .map_err(|_| BackendError::Timeout(self.options.timeout.as_millis() as u64))?
        .map_err(|e| CallError::Driver(e).into_backend("profile transaction start"))?;
        let read = async {
            let mut stream = txn
                .execute(build_query(&query.statement, &query.parameters))
                .await
                .map_err(|e| CallError::Driver(e).into_backend("profile publication"))?;
            let row = stream
                .next(&mut txn)
                .await
                .map_err(|e| CallError::Driver(e).into_backend("profile publication"))?
                .ok_or_else(|| BackendError::Conflict("profile scope conflict".into()))?;
            queries::decode(&row_to_json(&row).map_err(|e| e.into_backend("profile decode"))?)
        };
        let result = tokio::time::timeout(self.options.timeout, read)
            .await
            .map_err(|_| BackendError::Timeout(self.options.timeout.as_millis() as u64))
            .and_then(|r| r);
        match result {
            Ok(stored) if stored == frozen => {}
            Ok(_) => {
                let _ = tokio::time::timeout(self.options.timeout, txn.rollback()).await;
                return Err(BackendError::Conflict(
                    "profile revision already has different content".into(),
                ));
            }
            Err(e) => {
                let _ = tokio::time::timeout(self.options.timeout, txn.rollback()).await;
                return Err(e);
            }
        }
        match tokio::time::timeout(self.options.timeout, txn.commit()).await {
            Ok(Ok(())) => Ok(frozen),
            Ok(Err(e)) if matches!(e, neo4rs::Error::Neo4j(_)) => {
                Err(CallError::Driver(e).into_backend("profile commit rejected"))
            }
            _ => Err(BackendError::UnknownCommit(
                "profile publication: read this revision before retrying".into(),
            )),
        }
    }
    async fn get(
        &self,
        org: &str,
        reference: &ProfileRef,
    ) -> Result<Option<FrozenProfile>, BackendError> {
        let query = queries::get(org, reference)?;
        let rows = self
            .execute_read(&query.statement, &query.parameters)
            .await?;
        let value = rows.first().map(queries::decode).transpose()?;
        if value
            .as_ref()
            .is_some_and(|v| v.document.reference() != *reference)
        {
            return Err(BackendError::Deserialization(
                "profile reference mismatch".into(),
            ));
        }
        Ok(value)
    }
    async fn list(
        &self,
        org: &str,
        after: Option<&ProfileRef>,
        limit: usize,
    ) -> Result<ProfilePage, BackendError> {
        let query = queries::list(org, after, limit)?;
        let rows = self
            .execute_read(&query.statement, &query.parameters)
            .await?;
        let mut items = rows
            .iter()
            .map(queries::decode_entry)
            .collect::<Result<Vec<_>, _>>()?;
        let more = items.len() > limit;
        items.truncate(limit);
        Ok(ProfilePage {
            next: if more {
                items.last().map(|p| p.reference.clone())
            } else {
                None
            },
            items,
        })
    }
}
