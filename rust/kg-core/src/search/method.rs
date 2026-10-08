//! Supported retrieval and ranking strategies.
use serde::{Deserialize, Serialize};

/// Entity candidate retrieval strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchMethod {
    /// Literal keyword search through the graph adapter.
    Fulltext,
    /// Similarity of graph-stored entity embeddings.
    Vector,
    /// Similarity ranked inside a graph neighborhood.
    GraphAnchored,
    /// Scoped traversal from explicit or automatically discovered seeds.
    Bfs,
}
/// Strategy applied to fused entity candidates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RerankMethod {
    /// Reciprocal rank fusion.
    Rrf,
    /// Greedy diversity selection using stored embeddings where available.
    Mmr,
    /// Relevance scores from an explicitly configured ranking model.
    Model,
    /// Minimum hop distance from a stable center chain.
    NodeDistance,
    /// Scoped source snapshot count, descending; fused score breaks ties.
    ObservationFrequency,
}
