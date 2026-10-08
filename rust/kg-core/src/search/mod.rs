//! Shared search configuration, requests, and results.

pub mod config;
pub mod method;
pub mod result;
pub mod storage;
pub mod summary;
pub use summary::{SummaryReadiness, SummarySearch, SummaryView};

pub use config::{EvidenceReranker, SearchConfig, SearchScope, VectorRetrieval};
pub use method::{RerankMethod, SearchMethod};
pub use result::{SearchDiagnostic, SearchFailure, SearchHit, SearchResult};

pub use storage::*;

#[cfg(test)]
mod tests;

pub mod community;
pub use community::{CommunityEvidence, CommunityHit, CommunitySearch};
