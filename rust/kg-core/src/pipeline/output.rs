//! Run results, committed graph counts, and recorded failures.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::models::CollectionRef;
use crate::runtime::stage_output::IncompleteExtraction;
use crate::traits::BatchKind;

/// A recorded snapshot failure. Run-level failures abort the run and are
/// reported as `PipelineError`, never as a failure row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotFailure {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<crate::profiles::ProfileViolation>,
    /// Original input index.
    pub snapshot_index: usize,
    pub stage: String,
    pub error: String,
    /// Whether the same input may succeed on a retry, as the stage reported.
    #[serde(default)]
    pub retriable: bool,
}

/// Policy skipped required extraction. The snapshot was not written and its
/// scope cannot be swept for deletions; the run succeeds but is incomplete.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncompleteSnapshot {
    /// Original input index.
    pub snapshot_index: usize,
    #[serde(flatten)]
    pub extraction: IncompleteExtraction,
}

/// A derived summary left unchanged because complete evidence exceeds its budget.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkippedSummary {
    pub chain_id: Uuid,
    pub reason: String,
}

/// Graph work committed by one batch. Stored verbatim in the batch receipt,
/// so a replayed batch reports what it committed the first time.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedCounts {
    pub relationships_declined: usize,
    /// Reference observations the discovery stage actually attempted to resolve
    /// (not every payload scalar).
    pub references_attempted: usize,
    /// References confirmed as edges (deterministic, guided, heuristic or model).
    pub references_confirmed: usize,
    /// Attempted references left unresolved for lack of evidence (no edge).
    pub references_unresolved: usize,
    /// References excluded by policy, self-reference or scope (deliberate no-edge).
    pub references_excluded: usize,
    /// Sources whose reference traversal or candidate lookup was truncated; the
    /// graph is not fully current for them and no retirement may rely on their absence.
    pub references_incomplete: usize,
    pub communities_updated: usize,
    pub community_memberships: usize,
    pub community_generations: usize,
    pub saga_summaries_updated: usize,
    pub saga_summaries_incomplete: usize,
    pub saga_memberships_summarized: usize,
    pub summaries_updated: usize,
    pub summaries_skipped: usize,
    pub snapshots: usize,
    /// Existing immutable observations attached without fresh extraction.
    pub snapshots_reused: usize,
    pub entities_created: usize,
    /// Tombstoned chains continued by a new live version.
    pub entities_recreated: usize,
    /// New versions plus in-place volatile updates.
    pub entities_updated: usize,
    /// Observations without versioned or volatile property changes, including
    /// stale input. Tags, labels and observation bookkeeping may still change.
    pub entities_unchanged: usize,
    pub entities_deleted: usize,
    pub chains_merged: usize,
    pub edges_created: usize,
    pub edges_updated: usize,
    pub edges_unchanged: usize,
    pub edges_invalidated: usize,
    /// Snapshot-to-version provenance links.
    pub observations: usize,
    /// Entity and relationship embeddings written with their versions.
    pub embeddings: usize,
    /// Collection memberships a sweep released from entities another
    /// collection still owns.
    pub memberships_released: usize,
}

/// A reported count cannot be represented; aggregation leaves the totals unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("committed count '{0}' exceeds the supported range")]
pub struct CountOverflow(pub &'static str);

impl CommittedCounts {
    /// Whether any source's reference discovery was truncated; a fully-current
    /// claim requires this to be false.
    pub fn reference_discovery_incomplete(&self) -> bool {
        self.references_incomplete > 0
    }

    /// Add acknowledged work atomically; invalid totals never wrap or partially apply.
    pub fn add(&mut self, other: &CommittedCounts) -> Result<(), CountOverflow> {
        let next = Self {
            summaries_skipped: self
                .summaries_skipped
                .checked_add(other.summaries_skipped)
                .ok_or(CountOverflow("summaries_skipped"))?,
            references_attempted: self
                .references_attempted
                .checked_add(other.references_attempted)
                .ok_or(CountOverflow("references_attempted"))?,
            references_confirmed: self
                .references_confirmed
                .checked_add(other.references_confirmed)
                .ok_or(CountOverflow("references_confirmed"))?,
            references_unresolved: self
                .references_unresolved
                .checked_add(other.references_unresolved)
                .ok_or(CountOverflow("references_unresolved"))?,
            references_excluded: self
                .references_excluded
                .checked_add(other.references_excluded)
                .ok_or(CountOverflow("references_excluded"))?,
            relationships_declined: self
                .relationships_declined
                .checked_add(other.relationships_declined)
                .ok_or(CountOverflow("relationships_declined"))?,
            references_incomplete: self
                .references_incomplete
                .checked_add(other.references_incomplete)
                .ok_or(CountOverflow("references_incomplete"))?,
            communities_updated: self
                .communities_updated
                .checked_add(other.communities_updated)
                .ok_or(CountOverflow("communities_updated"))?,
            community_memberships: self
                .community_memberships
                .checked_add(other.community_memberships)
                .ok_or(CountOverflow("community_memberships"))?,
            community_generations: self
                .community_generations
                .checked_add(other.community_generations)
                .ok_or(CountOverflow("community_generations"))?,
            saga_summaries_incomplete: self
                .saga_summaries_incomplete
                .checked_add(other.saga_summaries_incomplete)
                .ok_or(CountOverflow("saga_summaries_incomplete"))?,
            saga_summaries_updated: self
                .saga_summaries_updated
                .checked_add(other.saga_summaries_updated)
                .ok_or(CountOverflow("saga_summaries_updated"))?,
            saga_memberships_summarized: self
                .saga_memberships_summarized
                .checked_add(other.saga_memberships_summarized)
                .ok_or(CountOverflow("saga_memberships_summarized"))?,
            summaries_updated: self
                .summaries_updated
                .checked_add(other.summaries_updated)
                .ok_or(CountOverflow("summaries_updated"))?,
            snapshots: self
                .snapshots
                .checked_add(other.snapshots)
                .ok_or(CountOverflow("snapshots"))?,
            snapshots_reused: self
                .snapshots_reused
                .checked_add(other.snapshots_reused)
                .ok_or(CountOverflow("snapshots_reused"))?,
            entities_created: self
                .entities_created
                .checked_add(other.entities_created)
                .ok_or(CountOverflow("entities_created"))?,
            entities_recreated: self
                .entities_recreated
                .checked_add(other.entities_recreated)
                .ok_or(CountOverflow("entities_recreated"))?,
            entities_updated: self
                .entities_updated
                .checked_add(other.entities_updated)
                .ok_or(CountOverflow("entities_updated"))?,
            entities_unchanged: self
                .entities_unchanged
                .checked_add(other.entities_unchanged)
                .ok_or(CountOverflow("entities_unchanged"))?,
            entities_deleted: self
                .entities_deleted
                .checked_add(other.entities_deleted)
                .ok_or(CountOverflow("entities_deleted"))?,
            chains_merged: self
                .chains_merged
                .checked_add(other.chains_merged)
                .ok_or(CountOverflow("chains_merged"))?,
            edges_created: self
                .edges_created
                .checked_add(other.edges_created)
                .ok_or(CountOverflow("edges_created"))?,
            edges_updated: self
                .edges_updated
                .checked_add(other.edges_updated)
                .ok_or(CountOverflow("edges_updated"))?,
            edges_unchanged: self
                .edges_unchanged
                .checked_add(other.edges_unchanged)
                .ok_or(CountOverflow("edges_unchanged"))?,
            edges_invalidated: self
                .edges_invalidated
                .checked_add(other.edges_invalidated)
                .ok_or(CountOverflow("edges_invalidated"))?,
            observations: self
                .observations
                .checked_add(other.observations)
                .ok_or(CountOverflow("observations"))?,
            embeddings: self
                .embeddings
                .checked_add(other.embeddings)
                .ok_or(CountOverflow("embeddings"))?,
            memberships_released: self
                .memberships_released
                .checked_add(other.memberships_released)
                .ok_or(CountOverflow("memberships_released"))?,
        };
        *self = next;
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

/// One batch acknowledged by the storage adapter during this call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchOutcome {
    pub kind: BatchKind,
    pub index: u32,
    /// True when an earlier attempt had already committed this batch.
    pub replayed: bool,
    pub counts: CommittedCounts,
}

/// What reconciliation did for one declared collection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionOutcome {
    pub collection: CollectionRef,
    pub generation: u64,
    /// False when the sweep was suppressed; `reason` says why.
    pub swept: bool,
    pub reason: Option<String>,
    pub entities_deleted: usize,
    pub memberships_released: usize,
    pub edges_invalidated: usize,
    /// Stale members left alone because a later observation of another run
    /// already exists; that observer's next scan decides their fate.
    pub entities_protected: usize,
}

impl CollectionOutcome {
    /// Fold one reconciliation batch's committed counts into the outcome.
    pub fn add(&mut self, counts: &CommittedCounts) -> Result<(), CountOverflow> {
        let deleted = self
            .entities_deleted
            .checked_add(counts.entities_deleted)
            .ok_or(CountOverflow("entities_deleted"))?;
        let released = self
            .memberships_released
            .checked_add(counts.memberships_released)
            .ok_or(CountOverflow("memberships_released"))?;
        let invalidated = self
            .edges_invalidated
            .checked_add(counts.edges_invalidated)
            .ok_or(CountOverflow("edges_invalidated"))?;
        self.entities_deleted = deleted;
        self.memberships_released = released;
        self.edges_invalidated = invalidated;
        Ok(())
    }
}

/// Result of awaited ingestion. Graph counts describe acknowledged commits,
/// including re-observations; no reported graph work is pending.
///
/// A run that aborts returns [`crate::errors::PipelineError::Aborted`]
/// instead, carrying the progress committed before the failure.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PipelineOutput {
    #[serde(
        default,
        skip_serializing_if = "crate::profiles::ProfileDiagnostics::is_empty"
    )]
    pub profile_diagnostics: crate::profiles::ProfileDiagnostics,
    #[serde(default)]
    pub relationship_declines: Vec<crate::runtime::stage_output::RelationshipDecline>,
    /// Every evidence-backed ambiguous reference decision of the run, from
    /// committed and replayed relationship receipts alike.
    #[serde(default)]
    pub reference_decisions: Vec<crate::runtime::reference_resolution::ReferenceDecisionAudit>,
    #[serde(default)]
    pub incomplete_followups: Vec<crate::saga::IncompleteSagaSummary>,
    #[serde(default)]
    pub skipped_summaries: Vec<SkippedSummary>,
    /// Caller-supplied identity, or a fresh UUID when using `PipelineRunner::run`.
    pub run_id: Uuid,
    /// Run totals across every attempt, reconstructed from receipts. A batch
    /// replayed from an earlier attempt counts once.
    pub committed: CommittedCounts,
    /// Work committed by this attempt only; zero for a fully replayed retry.
    pub newly_committed: CommittedCounts,
    /// Every batch acknowledged in this call, in commit order.
    pub batches: Vec<BatchOutcome>,
    /// Inputs in the request.
    pub snapshots_total: usize,
    /// Inputs that were neither failed nor incomplete in any pass.
    pub snapshots_completed: usize,
    /// Recorded only when processing continues on error. Any snapshot failure
    /// suppresses the run's deletion reconciliation.
    pub failed_snapshots: Vec<SnapshotFailure>,
    /// Inputs whose extraction was skipped by policy. Their scopes'
    /// reconciliation is suppressed without failing the run.
    #[serde(default)]
    pub incomplete_extractions: Vec<IncompleteSnapshot>,
    /// Reconciliation outcome per declared collection, in scope order.
    #[serde(default)]
    pub collections: Vec<CollectionOutcome>,
    /// Elapsed time before completion hooks.
    pub duration_ms: u64,
}

impl PipelineOutput {
    /// Complete means every snapshot was processed and every required
    /// extraction finished. Only a complete run fires the completion hook.
    pub fn is_complete(&self) -> bool {
        self.snapshots_completed == self.snapshots_total
            && self.failed_snapshots.is_empty()
            && self.incomplete_extractions.is_empty()
            && self.incomplete_followups.is_empty()
            && self.skipped_summaries.is_empty()
    }

    /// Indexes of failed inputs.
    pub fn failed_indexes(&self) -> HashSet<usize> {
        self.failed_snapshots
            .iter()
            .map(|f| f.snapshot_index)
            .collect()
    }

    /// Indexes of incomplete inputs that did not also fail in a later pass.
    pub fn incomplete_indexes(&self) -> HashSet<usize> {
        let failed = self.failed_indexes();
        self.incomplete_extractions
            .iter()
            .map(|i| i.snapshot_index)
            .filter(|i| !failed.contains(i))
            .collect()
    }

    /// Inputs that neither failed nor were left incomplete; an input
    /// rejected in several places counts once.
    pub fn count_completed(
        total: usize,
        failed: &[SnapshotFailure],
        incomplete: &[IncompleteSnapshot],
    ) -> usize {
        let rejected: HashSet<usize> = failed
            .iter()
            .map(|f| f.snapshot_index)
            .chain(incomplete.iter().map(|i| i.snapshot_index))
            .collect();
        total.saturating_sub(rejected.len())
    }

    /// Batches this call found already committed by an earlier attempt.
    pub fn replayed_batches(&self) -> usize {
        self.batches.iter().filter(|b| b.replayed).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_overflow_never_wraps_or_partially_updates_totals() {
        let mut total = CommittedCounts {
            embeddings: usize::MAX,
            ..Default::default()
        };
        let before = total;
        assert_eq!(
            total.add(&CommittedCounts {
                snapshots: 1,
                embeddings: 1,
                ..Default::default()
            }),
            Err(CountOverflow("embeddings"))
        );
        assert_eq!(total, before);
    }

    #[test]
    fn collection_overflow_leaves_all_fields_unchanged() {
        let mut outcome = CollectionOutcome {
            collection: CollectionRef {
                namespace: "prod".into(),
                source: "aws".into(),
                key: "account".into(),
            },
            generation: 1,
            swept: true,
            reason: None,
            entities_deleted: 0,
            memberships_released: 0,
            edges_invalidated: usize::MAX,
            entities_protected: 0,
        };
        let before = outcome.clone();
        assert_eq!(
            outcome.add(&CommittedCounts {
                entities_deleted: 1,
                edges_invalidated: 1,
                ..Default::default()
            }),
            Err(CountOverflow("edges_invalidated"))
        );
        assert_eq!(outcome, before);
    }

    #[test]
    fn receipt_counts_require_every_field_but_allow_recovery_data() {
        let counts = CommittedCounts {
            entities_created: 2,
            observations: 3,
            ..Default::default()
        };
        let mut complete = serde_json::to_value(counts).unwrap();
        complete["recovery"] =
            serde_json::json!({"nodes": [], "failures": [], "observed_relationships": []});
        assert_eq!(
            serde_json::from_value::<CommittedCounts>(complete.clone()).unwrap(),
            counts
        );
        for field in serde_json::to_value(counts)
            .unwrap()
            .as_object()
            .unwrap()
            .keys()
        {
            let mut missing = complete.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<CommittedCounts>(missing).is_err(),
                "missing {field}"
            );
            let mut malformed = complete.clone();
            malformed[field] = serde_json::json!("invalid");
            assert!(
                serde_json::from_value::<CommittedCounts>(malformed).is_err(),
                "malformed {field}"
            );
        }
        assert!(serde_json::from_value::<CommittedCounts>(serde_json::json!({})).is_err());
    }

    #[test]
    fn completion_requires_all_inputs_to_be_accounted_for() {
        let mut output = PipelineOutput {
            snapshots_total: 2,
            ..Default::default()
        };
        assert!(!output.is_complete());
        output.snapshots_completed = 2;
        assert!(output.is_complete());
        output.snapshots_completed = 3;
        assert!(!output.is_complete());
        assert!(PipelineOutput::default().is_complete());
    }

    #[test]
    fn counts_add_and_completeness_follow_failures() {
        let mut total = CommittedCounts::default();
        total
            .add(&CommittedCounts {
                entities_created: 2,
                observations: 3,
                ..Default::default()
            })
            .unwrap();
        total
            .add(&CommittedCounts {
                entities_created: 1,
                edges_created: 4,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(total.entities_created, 3);
        assert_eq!(total.observations, 3);
        assert_eq!(total.edges_created, 4);
        assert!(!total.is_empty());

        let mut output = PipelineOutput::default();
        assert!(output.is_complete());
        output.incomplete_extractions.push(IncompleteSnapshot {
            snapshot_index: 1,
            extraction: IncompleteExtraction {
                snapshot_id: Uuid::nil(),
                namespace: "prod".into(),
                source: "aws".into(),
                reason: "skipped".into(),
            },
        });
        assert!(!output.is_complete());
        // The same input incomplete in the node pass and failed in the
        // relationship pass counts once, as failed.
        output.failed_snapshots.push(SnapshotFailure {
            profile: None,
            snapshot_index: 1,
            stage: "edge".into(),
            error: "x".into(),
            retriable: false,
        });
        assert_eq!(
            PipelineOutput::count_completed(
                3,
                &output.failed_snapshots,
                &output.incomplete_extractions
            ),
            2
        );
        assert_eq!(output.failed_indexes(), HashSet::from([1]));
        assert!(output.incomplete_indexes().is_empty());
        let json = serde_json::to_value(&output.incomplete_extractions[0]).unwrap();
        assert_eq!(json["snapshot_index"], 1);
        assert_eq!(json["namespace"], "prod");
    }
}
