//! Count accounting helpers of the runner.
use super::*;

#[test]
fn replay_deduplicates_counts_and_overflow_preserves_progress() {
    let mut progress = Progress::default();
    let mut commit = CommitOutput {
        batch: BatchIdentity {
            run_id: Uuid::new_v4(),
            kind: BatchKind::Node,
            index: 0,
        },
        replayed: true,
        committed_at: Utc::now(),
        counts: CommittedCounts {
            snapshots: usize::MAX,
            ..Default::default()
        },
        recovery: None,
    };
    progress.record(&commit).unwrap();
    progress.record(&commit).unwrap();
    assert_eq!(progress.batches.len(), 1);
    assert_eq!(progress.committed.snapshots, usize::MAX);
    assert!(progress.newly_committed.is_empty());
    commit.batch.index = 1;
    commit.replayed = false;
    commit.counts.snapshots = 1;
    assert!(progress.record(&commit).is_err());
    assert_eq!(progress.batches.len(), 2);
    assert!(!progress.commit_unknown);
    assert_eq!(progress.committed.snapshots, usize::MAX);
    assert!(progress.newly_committed.is_empty());
}
