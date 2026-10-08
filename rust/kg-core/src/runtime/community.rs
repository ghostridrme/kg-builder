//! Bounded Community maintenance and recoverable stage handoffs.
use crate::community::{CommunityDefinition, CommunityEntity, CommunityMember, CommunityState};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CommunitySettings {
    pub incremental_enabled: bool,
    pub max_namespaces: usize,
    pub max_run_ms: u64,
    pub max_model_calls_per_run: usize,
    pub max_scanned_rows: usize,
    pub max_clustering_work: usize,
    pub mode: super::entity_summary::SummaryMode,
    pub page_size: usize,
    pub max_entities: usize,
    pub max_relationships: usize,
    pub max_projection_bytes: usize,
    pub max_iterations: usize,
    pub max_clusters: usize,
    pub max_summary_bytes: usize,
    pub max_name_bytes: usize,
    pub max_evidence_bytes: usize,
    pub max_output_tokens: u32,
    pub max_model_calls_per_cluster: usize,
    pub max_concurrent_batches: usize,
    pub max_replans: u32,
    pub timeout_ms: u64,
    pub partition_members: usize,
    pub max_checkpoint_bytes: usize,
}
impl Default for CommunitySettings {
    fn default() -> Self {
        Self {
            incremental_enabled: false,
            max_namespaces: 64,
            max_run_ms: 900_000,
            max_model_calls_per_run: 1024,
            max_scanned_rows: 2_000_000,
            max_clustering_work: 100_000_000,
            mode: super::entity_summary::SummaryMode::Deterministic,
            page_size: 1000,
            max_entities: 100_000,
            max_relationships: 1_000_000,
            max_projection_bytes: 64 * 1024 * 1024,
            max_iterations: 100,
            max_clusters: 10_000,
            max_summary_bytes: 16 * 1024,
            max_name_bytes: 512,
            max_evidence_bytes: 256 * 1024,
            max_output_tokens: 4096,
            max_model_calls_per_cluster: 1024,
            max_concurrent_batches: 2,
            max_replans: 3,
            timeout_ms: 120_000,
            partition_members: 1000,
            max_checkpoint_bytes: 4 * 1024 * 1024,
        }
    }
}
impl CommunitySettings {
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=3_600_000).contains(&self.max_run_ms)
            || !(1..=100_000).contains(&self.max_model_calls_per_run)
            || !(1..=64).contains(&self.max_namespaces)
            || !(1..=100_000_000).contains(&self.max_scanned_rows)
            || !(1..=1_000_000_000).contains(&self.max_clustering_work)
            || !(1..=1000).contains(&self.page_size)
            || !(1..=1_000_000).contains(&self.max_entities)
            || !(1..=10_000_000).contains(&self.max_relationships)
            || !(1024..=256 * 1024 * 1024).contains(&self.max_projection_bytes)
            || !(1..=1000).contains(&self.max_iterations)
            || !(1..=10_000).contains(&self.max_clusters)
            || !(1..=64 * 1024).contains(&self.max_summary_bytes)
            || !(1..=crate::embedding::MAX_TEXT_CHARS).contains(&self.max_name_bytes)
            || !(1024..=8 * 1024 * 1024).contains(&self.max_evidence_bytes)
            || !(1..=32768).contains(&self.max_output_tokens)
            || !(1..=100_000).contains(&self.max_model_calls_per_cluster)
            || !(1..=16).contains(&self.max_concurrent_batches)
            || self.max_replans > 16
            || !(1..=3_600_000).contains(&self.timeout_ms)
            || !(1..=1000).contains(&self.partition_members)
            || !(128 * 1024..=4 * 1024 * 1024).contains(&self.max_checkpoint_bytes)
        {
            return Err("invalid Community processing bounds".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommunityOperation {
    Full,
    Incremental { chain_ids: Vec<Uuid> },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityRequestOutput {
    pub namespace: String,
    /// Full-build identity; incremental detection replaces it with the active generation.
    pub generation: Uuid,
    pub operation: CommunityOperation,
    pub as_of: DateTime<Utc>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityCluster {
    pub uuid: Uuid,
    pub expected_revision: Option<Uuid>,
    pub members: Vec<CommunityEntity>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityClustersOutput {
    pub request: CommunityRequestOutput,
    pub state: CommunityState,
    pub valid_until: Option<DateTime<Utc>>,
    pub clusters: Vec<CommunityCluster>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityDraft {
    /// Name vectors are absent until the embedding stage completes.
    pub definition: CommunityDefinition,
    pub expected_revision: Option<Uuid>,
    pub members: Vec<CommunityMember>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityDraftsOutput {
    pub request: CommunityRequestOutput,
    pub state: CommunityState,
    pub valid_until: Option<DateTime<Utc>>,
    pub drafts: Vec<CommunityDraft>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityPreparedOutput {
    pub request: CommunityRequestOutput,
    pub state: CommunityState,
    pub valid_until: Option<DateTime<Utc>>,
    pub drafts: Vec<CommunityDraft>,
}

/// Frozen plan metadata; source text travels in separately bounded receipts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityPlan {
    pub request: CommunityRequestOutput,
    pub state: CommunityState,
    pub valid_until: Option<DateTime<Utc>>,
    pub clusters: Vec<CommunityClusterHeader>,
    pub fragment_hashes: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityClusterHeader {
    pub uuid: Uuid,
    pub expected_revision: Option<Uuid>,
    pub member_count: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityMemberFragment {
    pub cluster_index: usize,
    pub members: Vec<CommunityEntity>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityTargetManifest {
    pub requests: Vec<CommunityRequestOutput>,
    pub collections: Vec<crate::pipeline::CollectionOutcome>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunityCheckpoint {
    pub namespace: String,
    pub attempt: u32,
    pub step: CommunityCheckpointStep,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommunityCheckpointStep {
    Targets(CommunityTargetManifest),
    Plan(CommunityPlan),
    Members(CommunityMemberFragment),
    Summary {
        cluster_index: usize,
        definition: CommunityDefinition,
    },
    Vector {
        cluster_index: usize,
        definition: CommunityDefinition,
    },
    Begun,
    Partition {
        index: usize,
    },
    Published,
}

pub fn checkpoint_hash(value: &impl Serialize) -> Result<String, String> {
    use sha2::{Digest, Sha256};
    let bytes = serde_json::to_vec(value)
        .map_err(|_| "cannot serialize Community checkpoint".to_owned())?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn name_and_checkpoint_bounds_match_downstream_capacity() {
        assert!(CommunitySettings::default().validate().is_ok());
        assert!(CommunitySettings {
            max_name_bytes: crate::embedding::MAX_TEXT_CHARS + 1,
            ..Default::default()
        }
        .validate()
        .is_err());
        assert!(CommunitySettings {
            max_checkpoint_bytes: 8 * 1024 * 1024,
            ..Default::default()
        }
        .validate()
        .is_err());
        assert!(CommunitySettings {
            max_namespaces: 65,
            ..Default::default()
        }
        .validate()
        .is_err());
    }
}
