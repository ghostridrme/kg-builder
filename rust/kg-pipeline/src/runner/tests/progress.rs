//! `Progress::record` is idempotent per (kind, index) and separates replayed
//! from newly committed work.
use super::*;
use kg_core::pipeline::CommittedCounts;
use kg_core::traits::{BatchIdentity, BatchKind};

fn commit(kind: BatchKind, index: u32, entities_created: usize, replayed: bool) -> CommitOutput {
    CommitOutput {
        batch: BatchIdentity {
            run_id: Uuid::from_u128(7),
            kind,
            index,
        },
        replayed,
        committed_at: Utc::now(),
        counts: CommittedCounts {
            entities_created,
            ..Default::default()
        },
        recovery: None,
    }
}

#[test]
fn recording_the_same_batch_twice_counts_it_once() {
    let mut progress = Progress::default();
    progress
        .record(&commit(BatchKind::Node, 0, 3, false))
        .unwrap();
    progress
        .record(&commit(BatchKind::Node, 0, 3, false))
        .unwrap();
    progress
        .record(&commit(BatchKind::Node, 0, 99, false))
        .unwrap();
    assert_eq!(progress.batches.len(), 1, "one outcome per (kind, index)");
    assert_eq!(progress.committed.entities_created, 3);
    assert_eq!(progress.newly_committed.entities_created, 3);
}

#[test]
fn different_kinds_or_indexes_accumulate_and_replays_do_not_count_as_new_work() {
    let mut progress = Progress::default();
    progress
        .record(&commit(BatchKind::Node, 0, 2, false))
        .unwrap();
    progress
        .record(&commit(BatchKind::Node, 1, 5, true))
        .unwrap();
    progress
        .record(&commit(BatchKind::Relationship, 0, 1, false))
        .unwrap();
    assert_eq!(progress.batches.len(), 3);
    assert_eq!(
        progress.committed.entities_created, 8,
        "committed totals include replays"
    );
    assert_eq!(
        progress.newly_committed.entities_created, 3,
        "replays add nothing new"
    );
    assert!(progress.batches.iter().any(|b| b.replayed));
}
