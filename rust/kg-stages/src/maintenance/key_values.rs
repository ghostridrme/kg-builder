//! Backfill typed reference tokens and their indexed component links.
//!
//! Nodes lacking `key_values` match nothing in `EntityLookup::LiveByKeyValue`, so
//! typed reference discovery is not ready for an organization until this reports
//! `complete`. Pages in chain order, writes through revision-guarded commits, and stops at
//! the budget with a resume position; a partial pass is progress, never readiness.
use kg_core::errors::BackendError;
use kg_core::runtime::stage_output::RelationshipTarget;
use kg_core::traits::{
    BatchIdentity, BatchKind, EntityLookup, GraphBackend, GraphMutation, GraphProperties,
    IdentityScope, MutationBatch, Precondition, RequestFingerprint, RunHeader,
};
use uuid::Uuid;

/// Work bounds for one awaited call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyValuesBackfillBudget {
    /// Versions read and written per page.
    pub page: usize,
    /// Pages processed before returning with a resume position.
    pub max_pages: usize,
}

impl Default for KeyValuesBackfillBudget {
    fn default() -> Self {
        Self {
            page: 500,
            max_pages: 20,
        }
    }
}

/// Progress of one call; resume by passing `last_chain` back in.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KeyValuesBackfillProgress {
    /// Live versions read in this call.
    pub scanned: usize,
    /// Versions that received `key_values` in this call.
    pub updated: usize,
    /// Chain to resume after; `None` when nothing was scanned.
    pub last_chain: Option<Uuid>,
    /// True when a read returns no remaining live version without `key_values`.
    pub complete: bool,
}

/// Recompute typed key tokens from stored properties for every live version
/// still lacking `key_values` or their index links, with the same canonicalization as the reference
/// target index. Entities with no valid key components receive an empty list so
/// they are not revisited.
pub async fn backfill_key_values(
    graph: &dyn GraphBackend,
    org_id: &str,
    resume_after: Option<Uuid>,
    budget: KeyValuesBackfillBudget,
) -> Result<KeyValuesBackfillProgress, BackendError> {
    if org_id.trim().is_empty() || !(1..=500).contains(&budget.page) || budget.max_pages == 0 {
        return Err(BackendError::Query(
            "backfill requires an organization, page 1..=500 and positive max_pages".into(),
        ));
    }
    let mut progress = KeyValuesBackfillProgress {
        last_chain: resume_after,
        ..Default::default()
    };
    let mut verification_pass = resume_after.is_none();
    for _ in 0..budget.max_pages {
        // Freeze before reading: an in-place identity/key update must not let
        // this derived-index page overwrite tokens from newer source evidence.
        let revisions = graph
            .identity_revisions(
                org_id,
                &[IdentityScope {
                    namespace: "*".into(),
                    entity_type: "*".into(),
                }],
            )
            .await?;
        if revisions.len() != 1 {
            return Err(BackendError::Deserialization(
                "key backfill missing identity revision".into(),
            ));
        }
        let page = graph
            .find_entities(
                org_id,
                &EntityLookup::LiveMissingKeyValues {
                    after_chain: progress.last_chain,
                    limit: budget.page,
                },
            )
            .await?;
        if page.is_empty() {
            if verification_pass {
                progress.complete = true;
                progress.last_chain = None;
                return Ok(progress);
            }
            // A legacy writer may have inserted a row behind the cursor while
            // this pass was running. Restart once before declaring readiness.
            // Current writers always populate `key_values`.
            progress.last_chain = None;
            verification_pass = true;
            continue;
        }
        let mut mutations = Vec::with_capacity(page.len());
        for record in page {
            progress.scanned += 1;
            progress.last_chain = Some(record.chain_id);
            let uuid = record.uuid;
            let tokens = RelationshipTarget::from(record).key_value_tokens();
            let mut properties = GraphProperties::new();
            properties.insert("key_values".into(), serde_json::json!(tokens));
            mutations.push(GraphMutation::UpdateEntity { uuid, properties });
        }
        let run_id = Uuid::new_v4();
        let at = chrono::Utc::now();
        let fingerprint = RequestFingerprint::compute(
            org_id,
            &[],
            &serde_json::json!({"maintenance":"key_values_index_v1","last_chain":progress.last_chain,"revision":revisions[0]}),
        )?;
        graph
            .register_run(&RunHeader {
                org_id: org_id.into(),
                run_id,
                fingerprint: fingerprint.clone(),
                settings_version: "key-values-index-v1".into(),
                capture_default: at,
                batch_plan: vec![],
                observation_manifest: Default::default(),
                rule_freezes: vec![],
                schema_manifest: kg_core::runtime::schemas::RunSchemaManifest {
                    profiles: Default::default(),
                    org_id: org_id.into(),
                    sources: Default::default(),
                },
            })
            .await?;
        let updated = mutations.len();
        graph.commit_batch(&MutationBatch {
            org_id:org_id.into(),batch:BatchIdentity {run_id,kind:BatchKind::Node,index:0},fingerprint,
            preconditions:revisions.into_iter().map(Precondition::IdentityRevisionIs).collect(),mutations,
            result:serde_json::json!({"maintenance":"key_values_index_v1","updated":updated,"last_chain":progress.last_chain}),
        }).await?;
        progress.updated += updated;
    }
    Ok(progress)
}
