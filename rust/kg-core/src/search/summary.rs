//! Derived summary recall is separate from source text and base entity vectors.
use super::NodeSearch;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SummaryView {
    #[serde(rename = "summary_revision")]
    pub revision: Uuid,
    #[serde(rename = "derived_summary")]
    pub text: String,
    #[serde(rename = "summary_evidence_hash")]
    pub evidence_hash: String,
    #[serde(rename = "summary_as_of")]
    pub as_of: DateTime<Utc>,
    #[serde(rename = "summary_valid_until")]
    pub valid_until: Option<DateTime<Utc>>,
    #[serde(rename = "summary_policy_version")]
    pub policy_version: String,
    #[serde(rename = "summary_evidence_ids")]
    pub evidence_ids: Vec<Uuid>,
    #[serde(rename = "summary_total_evidence")]
    pub total_evidence: usize,
    /// Only retrieval from the summary index contributes a summary vote.
    #[serde(skip)]
    pub contributed: bool,
}
impl SummaryView {
    pub fn valid_at(&self, at: DateTime<Utc>) -> bool {
        !self.revision.is_nil()
            && !self.text.trim().is_empty()
            && !self.evidence_hash.is_empty()
            && self.policy_version == crate::entity_summary::POLICY_VERSION
            && self.as_of <= at
            && self.valid_until.is_none_or(|until| at < until)
    }
}
#[derive(Debug, Clone)]
pub struct SummarySearch {
    pub node: NodeSearch,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SummaryReadiness {
    pub valid: u64,
    pub expired: u64,
    pub missing: u64,
    pub compatible: u64,
    pub incompatible: u64,
}
