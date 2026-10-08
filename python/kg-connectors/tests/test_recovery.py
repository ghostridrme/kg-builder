"""Linked follow-up recovery cannot hide failures or release unrelated checkpoints."""

from connector_test_support.recovery import Store
import copy
import json
from uuid import uuid4

import pytest

from kg_sdk import Engine, RunJournal, JournalError
from connector_test_support.collection import local, context, native


def settled(journal, prepared, **overrides):
    outcome = dict(run_id=str(prepared.run_id), complete=False, snapshots_total=1, snapshots_completed=1,
        failed_snapshots=[], incomplete_extractions=[], skipped_summaries=[],
        incomplete_followups=[{"namespace": "prod", "saga_uuid": str(uuid4()), "from_ordinal": 1,
            "reason": "summary_too_large"}])
    outcome.update(overrides)
    journal._record(prepared, outcome, success=True)
    return outcome


def test_recovery_calls_only_maintenance_and_releases_original_cursor(native, tmp_path):
    journal, store = RunJournal(tmp_path / "journal"), Store()
    with Engine({}, connectors=[local()]) as old:
        original = old.prepare("local-aws", context(), journal=journal)
        before = settled(journal, original)
    with Engine({"processing": {"saga_summary": {"enabled": True, "max_summary_bytes": 64000}}}) as new:
        recovery = new.prepare_saga_recovery(original, journal=journal)
        with pytest.raises(JournalError):
            journal.acknowledge(original.run_id, checkpoint_store=store, recovery_run_id=recovery.run_id)
        assert not store.calls
        result = new.replay(recovery, journal=journal)
        assert result.complete and result["original_run_id"] == str(original.run_id)
        assert [call[0] for call in new._engine.calls] == ["saga"]
        first_id = new._engine.calls[0][3]
        assert new.replay(journal.load(recovery.run_id), journal=journal).complete
        assert new._engine.calls[1][3] == first_id
        journal.acknowledge(original.run_id, checkpoint_store=store, recovery_run_id=recovery.run_id)
        assert store.calls[0][1]["run_id"] == original.run_id
        assert journal._read(journal._path(original.run_id, "settled-incomplete"))["outcome"] == before
        assert not journal._path(original.run_id, "complete").exists()
        with pytest.raises(JournalError):
            journal.acknowledge(recovery.run_id, checkpoint_store=store)


@pytest.mark.parametrize("changes", [
    {"snapshots_completed": 0}, {"failed_snapshots": [{"snapshot_index": 0}]},
    {"incomplete_extractions": [{"snapshot_index": 0}]}, {"skipped_summaries": [{"reason": "large"}]},
    {"commit_unknown": True}, {"incomplete_followups": []},
    {"incomplete_followups": [{"namespace": "wrong", "saga_uuid": str(uuid4()), "from_ordinal": 1}]},
    {"incomplete_followups": [{"namespace": "prod", "saga_uuid": str(uuid4()), "from_ordinal": 0}]},
])
def test_other_incompleteness_cannot_be_relabelled_as_recovered(native, tmp_path, changes):
    journal = RunJournal(tmp_path / "journal")
    with Engine({}, connectors=[local()]) as engine:
        original = engine.prepare("local-aws", context(), journal=journal)
        settled(journal, original, **changes)
        with pytest.raises(JournalError):
            engine.prepare_saga_recovery(original, journal=journal)
        assert not engine._engine.calls


def test_wrong_database_and_missing_settled_evidence_fail(native, tmp_path):
    journal = RunJournal(tmp_path / "journal")
    with Engine({"graph": {"uri": "bolt://one"}}, connectors=[local()]) as engine:
        original = engine.prepare("local-aws", context(), journal=journal)
        with pytest.raises(JournalError):
            engine.prepare_saga_recovery(original, journal=journal)
        settled(journal, original)
    with Engine({"graph": {"uri": "bolt://two"}}) as engine:
        with pytest.raises(JournalError, match="original database"):
            engine.prepare_saga_recovery(original, journal=journal)


def test_incomplete_maintenance_never_releases_cursor(native, tmp_path):
    journal = RunJournal(tmp_path / "journal")
    with Engine({}, connectors=[local()]) as engine:
        original = engine.prepare("local-aws", context(), journal=journal)
        settled(journal, original)
        recovery = engine.prepare_saga_recovery(original, journal=journal)
        engine._engine.complete = False
        assert not engine.replay(recovery, journal=journal).complete
        with pytest.raises(JournalError):
            journal.acknowledge(original.run_id, checkpoint_store=Store(), recovery_run_id=recovery.run_id)


def test_unrelated_original_cannot_use_recovery_evidence(native, tmp_path):
    journal = RunJournal(tmp_path / "journal")
    with Engine({}, connectors=[local()]) as engine:
        a = engine.prepare("local-aws", context(), journal=journal)
        settled(journal, a)
        recovery = engine.prepare_saga_recovery(a, journal=journal)
        assert engine.replay(recovery, journal=journal).complete
        b = engine.prepare("local-aws", context(), journal=journal)
        settled(journal, b)
        with pytest.raises(JournalError, match="lineage"):
            journal.acknowledge(b.run_id, checkpoint_store=Store(), recovery_run_id=recovery.run_id)


def test_recovery_cancellation_and_unknown_commit_stay_incomplete(native, tmp_path):
    from kg_sdk import IngestionError
    journal = RunJournal(tmp_path / "journal")
    with Engine({}, connectors=[local()]) as engine:
        original = engine.prepare("local-aws", context(), journal=journal)
        settled(journal, original)
        recovery = engine.prepare_saga_recovery(original, journal=journal)
        def fail(op, value, org, run_id, *args):
            return json.dumps({"ok": False, "result": {"run_id": run_id, "cause": "cancelled", "commit_unknown": True}})
        engine._engine.run = fail
        with pytest.raises(IngestionError) as caught:
            engine.replay(recovery, journal=journal)
        assert caught.value.run_id == str(recovery.run_id) and caught.value.outcome["commit_unknown"]
        with pytest.raises(JournalError):
            journal.acknowledge(original.run_id, checkpoint_store=Store(), recovery_run_id=recovery.run_id)


def test_multiple_sagas_all_required_and_stable_child_ids(native, tmp_path):
    journal, store = RunJournal(tmp_path / "journal"), Store()
    targets = [{"namespace": "prod", "saga_uuid": str(uuid4()), "from_ordinal": n + 1} for n in range(3)]
    with Engine({}, connectors=[local()]) as engine:
        original = engine.prepare("local-aws", context(), journal=journal)
        settled(journal, original, incomplete_followups=targets)
        recovery = engine.prepare_saga_recovery(original, journal=journal)
        result = engine.replay(recovery, journal=journal)
        assert len(result["saga_recoveries"]) == 3
        assert len({call[3] for call in engine._engine.calls}) == 3
        journal.acknowledge(original.run_id, checkpoint_store=store, recovery_run_id=recovery.run_id)
        record = journal._read(journal._path(recovery.run_id, "complete"))
        record["outcome"]["saga_recoveries"].pop()
        path = journal._path(recovery.run_id, "complete")
        path.write_text(json.dumps(record))
        with pytest.raises(JournalError, match="missing required"):
            journal.acknowledge(original.run_id, checkpoint_store=store, recovery_run_id=recovery.run_id)


def test_host_shutdown_between_children_retains_completed_progress(native, tmp_path):
    from kg_sdk import EngineClosedError, IngestionError
    journal = RunJournal(tmp_path / "journal")
    with Engine({}, connectors=[local()]) as engine:
        original = engine.prepare("local-aws", context(), journal=journal)
        settled(journal, original, incomplete_followups=[
            {"namespace": "prod", "saga_uuid": str(uuid4()), "from_ordinal": 1} for _ in range(2)])
        recovery = engine.prepare_saga_recovery(original, journal=journal)
        original_run, calls = engine._engine.run, []
        def interrupted(*args):
            calls.append(True)
            if len(calls) == 2:
                raise EngineClosedError("closed")
            return original_run(*args)
        engine._engine.run = interrupted
        with pytest.raises(IngestionError) as caught:
            engine.replay(recovery, journal=journal)
        assert len(caught.value.outcome["saga_recoveries"]) == 1
        assert caught.value.outcome["commit_unknown"]
        with pytest.raises(JournalError):
            journal.acknowledge(original.run_id, checkpoint_store=Store(), recovery_run_id=recovery.run_id)
        records = list(journal.directory.glob(str(recovery.run_id) + ".result-*.json"))
        assert records and any(len(journal._read(path)["outcome"]["saga_recoveries"]) == 1 for path in records)
