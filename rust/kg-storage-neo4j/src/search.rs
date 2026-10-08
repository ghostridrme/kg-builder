//! Execute shared typed search queries through the Neo4j connection.
use crate::Neo4jGraphBackend;
use kg_core::traits::SearchBackend;
use kg_core::{errors::BackendError, search::*};
use kg_storage_cypher as queries;

/// Candidate budgets tried in order before the exact fallback.
const BUDGETS: [usize; 2] = [4096, queries::MAX_VECTOR_CANDIDATES];

/// Whether widening from `budget` to `next` can plausibly fill a page of
/// `limit + 1` when only `survivors` of the current candidates passed the
/// filters. Underfilled scopes go straight to the exact scan instead.
fn worth_widening(survivors: usize, budget: usize, next: usize, limit: usize) -> bool {
    survivors.saturating_mul(next) / budget > limit
}

impl Neo4jGraphBackend {
    pub(crate) async fn retrieve_communities(
        &self,
        request: &CommunitySearch,
        indexed: bool,
    ) -> Result<SearchPage<CommunityHit>, BackendError> {
        let mut pinned = request.clone();
        pinned
            .filter
            .relationship_now
            .get_or_insert_with(chrono::Utc::now);
        let request = &pinned;
        request.validate()?;
        if indexed
            && matches!(request.query, NodeQuery::Similarity(_))
            && self
                .population(queries::community_search::population(request)?)
                .await?
                > self.options.exact_vector_population
        {
            for (step, budget) in BUDGETS.into_iter().enumerate() {
                let query = queries::community_search::communities(request, Some(budget))?;
                let rows = self
                    .execute_search_read(&query.statement, &query.parameters)
                    .await?;
                let (rows, stats) = queries::vector_stats(rows, "community")?;
                let survivors = rows.len();
                if survivors > request.limit || stats.exhausted(request.min_score) {
                    return Ok(SearchPage::bounded(
                        rows.into_iter()
                            .map(queries::community_search::decode)
                            .collect::<Result<Vec<_>, _>>()?,
                        request.limit,
                    )
                    .approximate());
                }
                if let Some(next) = BUDGETS.get(step + 1) {
                    if !worth_widening(survivors, budget, *next, request.limit) {
                        break;
                    }
                }
            }
        }
        let query = queries::community_search::communities(request, None)?;
        let rows = self
            .execute_search_read(&query.statement, &query.parameters)
            .await?;
        Ok(SearchPage::bounded(
            rows.into_iter()
                .map(queries::community_search::decode)
                .collect::<Result<Vec<_>, _>>()?,
            request.limit,
        ))
    }

    async fn population(&self, query: queries::PreparedQuery) -> Result<u64, BackendError> {
        let rows = self
            .execute_search_read(&query.statement, &query.parameters)
            .await?;
        rows.first()
            .and_then(|r| r.get("count"))
            .and_then(|v| v.as_u64())
            .ok_or_else(|| BackendError::Deserialization("invalid vector population".into()))
    }

    pub(crate) async fn retrieve_nodes_indexed(
        &self,
        request: &NodeSearch,
    ) -> Result<NodePage, BackendError> {
        self.retrieve_nodes_source_indexed(request, false).await
    }
    pub(crate) async fn retrieve_summary_nodes(
        &self,
        request: &SummarySearch,
        indexed: bool,
    ) -> Result<NodePage, BackendError> {
        if indexed {
            self.retrieve_nodes_source_indexed(&request.node, true)
                .await
        } else {
            self.retrieve_nodes_source(&request.node, true).await
        }
    }
    async fn retrieve_nodes_source_indexed(
        &self,
        request: &NodeSearch,
        summary: bool,
    ) -> Result<NodePage, BackendError> {
        let mut pinned = request.clone();
        pinned
            .filter
            .relationship_now
            .get_or_insert_with(chrono::Utc::now);
        let request = &pinned;
        request.validate()?;
        let NodeQuery::Similarity(embedding) = &request.query else {
            return self.retrieve_nodes_source(request, summary).await;
        };
        let population = self
            .population(if summary {
                queries::summary_population(&SummarySearch {
                    node: request.clone(),
                })?
            } else {
                queries::vector_population(
                    &request.filter,
                    &embedding.model,
                    false,
                    &request.embedding_text_version,
                    &request.chain_ids,
                )
            })
            .await?;
        if population <= self.options.exact_vector_population {
            return self.retrieve_nodes_source(request, summary).await;
        }
        for (step, budget) in BUDGETS.into_iter().enumerate() {
            let query = if summary {
                queries::summary_nodes(
                    &SummarySearch {
                        node: request.clone(),
                    },
                    Some(budget),
                )?
            } else {
                queries::indexed_nodes(request, budget)?
            };
            let rows = self
                .execute_search_read(&query.statement, &query.parameters)
                .await?;
            let (rows, stats) = queries::vector_stats(rows, "n")?;
            let survivors = rows.len();
            if survivors > request.limit || stats.exhausted(request.min_score) {
                let items = rows
                    .into_iter()
                    .map(|row| decode_source_node(row, request, summary))
                    .collect::<Result<Vec<_>, _>>()?;
                return Ok(NodePage::bounded(items, request.limit).approximate());
            }
            if let Some(next) = BUDGETS.get(step + 1) {
                if !worth_widening(survivors, budget, *next, request.limit) {
                    break;
                }
            }
        }
        self.retrieve_nodes_source(request, summary).await
    }

    pub(crate) async fn retrieve_relationships_indexed(
        &self,
        request: &RelationshipSimilarity,
    ) -> Result<SearchPage<RelationshipHit>, BackendError> {
        let mut pinned = request.clone();
        pinned
            .filter
            .relationship_now
            .get_or_insert_with(chrono::Utc::now);
        let request = &pinned;
        request.validate()?;
        // Facts of a few anchors are a small scope; the exact scan over them is cheaper
        // than an index probe over the whole scope that is then filtered down.
        if request.anchor_chains.is_some() {
            return self.search_relationship_similarity(request).await;
        }
        let population = self
            .population(queries::vector_population(
                &request.filter,
                &request.embedding.model,
                true,
                kg_core::embedding::TEXT_VERSION,
                &None,
            ))
            .await?;
        if population <= self.options.exact_vector_population {
            return self.search_relationship_similarity(request).await;
        }
        for (step, budget) in BUDGETS.into_iter().enumerate() {
            let query = queries::indexed_relationships(request, budget)?;
            let rows = self
                .execute_search_read(&query.statement, &query.parameters)
                .await?;
            let (rows, stats) = queries::vector_stats(rows, "uuid")?;
            let survivors = rows.len();
            if survivors > request.limit || stats.exhausted(request.min_score) {
                return Ok(queries::decode_relationships(rows, request.limit)?.approximate());
            }
            if let Some(next) = BUDGETS.get(step + 1) {
                if !worth_widening(survivors, budget, *next, request.limit) {
                    break;
                }
            }
        }
        self.search_relationship_similarity(request).await
    }

    pub(crate) async fn retrieve_nodes(
        &self,
        request: &NodeSearch,
    ) -> Result<NodePage, BackendError> {
        self.retrieve_nodes_source(request, false).await
    }
    async fn retrieve_nodes_source(
        &self,
        request: &NodeSearch,
        summary: bool,
    ) -> Result<NodePage, BackendError> {
        let query = if summary {
            queries::summary_nodes(
                &SummarySearch {
                    node: request.clone(),
                },
                None,
            )?
        } else {
            queries::nodes(request)?
        };
        let rows = self
            .execute_search_read(&query.statement, &query.parameters)
            .await?;
        Ok(SearchPage::bounded(
            rows.into_iter()
                .map(|row| decode_source_node(row, request, summary))
                .collect::<Result<Vec<_>, _>>()?,
            request.limit,
        ))
    }
    pub(crate) async fn retrieve_relationships(
        &self,
        request: &EvidenceSearch,
    ) -> Result<SearchPage<RelationshipHit>, BackendError> {
        let query = queries::relationships(request)?;
        queries::decode_relationships(
            self.execute_search_read(&query.statement, &query.parameters)
                .await?,
            request.limit,
        )
    }
    pub(crate) async fn retrieve_snapshots(
        &self,
        request: &EvidenceSearch,
    ) -> Result<SearchPage<SnapshotHit>, BackendError> {
        let query = queries::snapshots(request)?;
        queries::decode_snapshots(
            self.execute_search_read(&query.statement, &query.parameters)
                .await?,
            request.limit,
            request
                .passage_query
                .as_deref()
                .or(request.query.as_deref()),
        )
    }
    pub(crate) async fn retrieve_attached_relationships(
        &self,
        request: &AttachedEvidence,
    ) -> Result<SearchPage<Attached<RelationshipHit>>, BackendError> {
        let query = queries::attached_relationships(request)?;
        queries::decode_attached_relationships(
            self.execute_search_read(&query.statement, &query.parameters)
                .await?,
            request.per_anchor,
        )
    }
    pub(crate) async fn retrieve_attached_snapshots(
        &self,
        request: &AttachedEvidence,
    ) -> Result<SearchPage<Attached<SnapshotHit>>, BackendError> {
        let query = queries::attached_snapshots(request)?;
        queries::decode_attached_snapshots(
            self.execute_search_read(&query.statement, &query.parameters)
                .await?,
            request.per_anchor,
            request.passage_query.as_deref(),
        )
    }
}

fn decode_source_node(
    row: serde_json::Map<String, serde_json::Value>,
    request: &NodeSearch,
    summary: bool,
) -> Result<SearchHit, BackendError> {
    let mut hit = queries::decode_node(row, &request.embedding_text_version)?;
    if summary {
        let view = hit.derived_summary.as_mut().ok_or_else(|| {
            BackendError::Deserialization("summary candidate has no valid summary".into())
        })?;
        view.contributed = true;
    }
    Ok(hit)
}

#[cfg(test)]
mod tests {
    use super::worth_widening;

    #[test]
    fn widening_is_skipped_when_the_filtered_share_cannot_fill_the_page() {
        // 3 of 4,096 survived: 16,384 would yield about 12, not the 51 needed.
        assert!(!worth_widening(3, 4096, 16384, 50));
        assert!(worth_widening(20, 4096, 16384, 50));
        assert!(!worth_widening(0, 4096, 16384, 0));
        assert!(worth_widening(1, 4096, 16384, 0));
    }
}
