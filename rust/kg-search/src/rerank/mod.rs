//! Entity rank fusion and diversity selection, plus a model adapter for nodes and evidence.
mod mmr;
mod text;
pub(crate) use text::hit_text;
mod rrf;
pub use mmr::maximal_marginal_relevance;
pub(crate) use rrf::{exact_match, fuse_versions, rrf_weight};
pub use rrf::{reciprocal_rank_fusion, Fusion, RRF_K};
/// Optional structured-output relevance model adapter.
pub mod model;
pub use model::ModelReranker;
