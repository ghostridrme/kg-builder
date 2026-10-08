use crate::Neo4jGraphBackend;
use async_trait::async_trait;
use kg_core::{
    errors::BackendError,
    saga::{SagaRead, SagaReadResult},
    search::SearchPage,
    traits::{
        graph_explorer::{ExplorerRequest, GraphExplorerBackend},
        GraphBackend,
    },
};
use serde_json::Value;

#[async_trait]
impl GraphExplorerBackend for Neo4jGraphBackend {
    async fn settle_cancelled_reads(&self) {
        self.cleanup.wait_idle().await;
    }

    async fn explore(&self, request: &ExplorerRequest) -> Result<SearchPage<Value>, BackendError> {
        let query = kg_storage_cypher::explorer::prepare(request)?;
        let rows = self
            .execute_search_read(&query.statement, &query.parameters)
            .await?;
        let items = rows
            .into_iter()
            .map(|mut row| {
                row.remove("item")
                    .ok_or_else(|| BackendError::Deserialization("missing explorer item".into()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SearchPage::bounded(items, request.limit))
    }

    async fn explore_batch(
        &self,
        requests: &[ExplorerRequest],
    ) -> Result<Vec<(uuid::Uuid, SearchPage<Value>)>, BackendError> {
        let query = kg_storage_cypher::explorer::prepare_batch(requests)?;
        let rows = self
            .execute_search_read(&query.statement, &query.parameters)
            .await?;
        rows.into_iter()
            .map(|row| {
                let id = row
                    .get("anchor")
                    .and_then(Value::as_str)
                    .and_then(|v| v.parse().ok())
                    .ok_or_else(|| BackendError::Deserialization("invalid batch anchor".into()))?;
                let items = row
                    .get("items")
                    .and_then(Value::as_array)
                    .cloned()
                    .ok_or_else(|| BackendError::Deserialization("invalid batch items".into()))?;
                Ok((id, SearchPage::bounded(items, requests[0].limit)))
            })
            .collect()
    }

    async fn read_saga(
        &self,
        org: &str,
        request: &SagaRead,
    ) -> Result<SagaReadResult, BackendError> {
        GraphBackend::read_saga(self, org, request).await
    }
}

impl Neo4jGraphBackend {
    /// Wait for cancelled administrator reads before releasing MCP admission capacity.
    pub async fn settle_mcp_reads(&self) {
        self.cleanup.wait_idle().await;
    }

    /// Classify without executing caller work. Missing metadata fails closed.
    pub async fn verify_read_query(
        &self,
        statement: &str,
        parameters: &Value,
    ) -> Result<(), BackendError> {
        tokio::time::timeout(self.options.timeout, async {
            let mut stream = self
                .graph
                .execute_once(crate::driver::build_query(
                    &format!("EXPLAIN {statement}"),
                    parameters,
                ))
                .await
                .map_err(|_| BackendError::Query("Unable to classify Cypher".into()))?;
            while stream
                .next()
                .await
                .map_err(|_| BackendError::Query("Unable to classify Cypher".into()))?
                .is_some()
            {}
            if stream.query_type() != Some("r") {
                return Err(BackendError::Query(
                    "Cypher is not a classified read".into(),
                ));
            }
            Ok(())
        })
        .await
        .map_err(|_| BackendError::Timeout(self.options.timeout.as_millis() as u64))?
    }
}

#[cfg(test)]
mod classification_tests {
    #[cfg(feature = "live-tests")]
    use super::*;
    #[cfg(feature = "live-tests")]
    #[tokio::test]
    #[ignore = "live: Neo4j"]
    async fn explain_rejects_mutation_without_executing_it() {
        let settings = crate::Neo4jSettings::new(
            std::env::var("NEO4J_TEST_URI").expect("NEO4J_TEST_URI"),
            std::env::var("NEO4J_TEST_USER").unwrap_or_else(|_| "neo4j".into()),
            std::env::var("NEO4J_TEST_PASSWORD").expect("NEO4J_TEST_PASSWORD"),
        );
        let backend = Neo4jGraphBackend::connect(&settings).await.unwrap();
        backend
            .verify_read_query(
                "MATCH (n:Entity) RETURN n.uuid LIMIT 1",
                &serde_json::json!({}),
            )
            .await
            .unwrap();
        assert!(backend
            .verify_read_query(
                "CREATE (n:ShouldNeverExist) RETURN n",
                &serde_json::json!({})
            )
            .await
            .is_err());
        assert!(backend
            .verify_read_query("MATCH (n) DELETE n", &serde_json::json!({}))
            .await
            .is_err());
    }
}
