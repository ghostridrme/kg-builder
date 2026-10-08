from connector_test_support.collection import DATA, local, context, native
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from kg_sdk import (
    CancellationToken, CollectedBatch, CollectionContext, CollectionError, CollectionLimits,
    Engine, EngineClosedError, IngestionError, JournalError, Limits,
    RunJournal, SnapshotInput,
)
from kg_sdk.connector import collection_key
from kg_sdk.connector import connector_key
from kg_sdk.registration import configuration_fingerprint, register


def test_prepare_freezes_raw_payload_replay_does_not_collect(native, tmp_path):
    connector = local()
    journal = RunJournal(tmp_path / "journal")
    with Engine({}, connectors=[connector]) as engine:
        prepared = engine.prepare(connector.name, context(), journal=journal)
        assert not engine._engine.calls
        body = journal._body(prepared)
        snapshot = json.loads(body["inputs_json"])[0]["input"]
        file = sorted(DATA.rglob("*.json"))[0]
        exported = json.loads(file.read_text())
        raw = dict(snapshot["entities"][0]["raw_properties"])
        assert raw.pop("_astrolabe_scope") == {"account_id": exported["accountId"], "region": exported["awsRegion"]}
        assert raw.pop("_kg_identity") == {"arn": exported["resourceArn"], "resource_id": exported["resourceId"]}
        assert raw == exported["payload"]
        assert snapshot["snapshot_kind"] == "full" and snapshot["complete"] is True and snapshot["collection"] is None
        assert snapshot["relationship_changes"] == []
        assert snapshot["captured_at"] == exported["capturedAt"]
        assert body["original_envelopes"][0] == {"file": file.relative_to(DATA).as_posix(), "envelope": exported}
        connector.collect = lambda *a, **kw: pytest.fail("replay recollected")
        assert engine.replay(journal.load(prepared.run_id), journal=journal).complete
        assert engine.replay(prepared, journal=journal).complete
        assert engine._engine.calls[0] == engine._engine.calls[1]
        with pytest.raises(JournalError, match="already collected"):
            engine.prepare(connector.name, context(run_id=prepared.run_id), journal=journal)






class StaticConnector:
    name, source = "custom", "custom"

    def collect(self, context, **kwargs):
        return CollectedBatch(snapshots=[SnapshotInput(org_id=context.org_id, namespace=context.namespace,
                              source=self.source, name="test", data_type="text", content="hello")], complete=True)


@pytest.mark.parametrize("field,value", [("namespace", "other"), ("org_id", "other"), ("source", "other"),
                                         ("relationship_changes", [{"operation": "observe"}])])
def test_connector_cannot_override_scope_or_supply_edges(native, tmp_path, field, value):
    connector = StaticConnector()
    original = connector.collect
    def collect(*args, **kwargs):
        batch = original(*args, **kwargs)
        setattr(batch.snapshots[0], field, value)
        return batch
    connector.collect = collect
    with Engine({}, connectors=[connector]) as engine:
        with pytest.raises(CollectionError):
            engine.prepare(connector.name, context(), journal=RunJournal(tmp_path / "journal"))


def test_connector_registration_only_registers_collection_sources():
    from kg_sdk import ConfigurationError
    a, b = StaticConnector(), StaticConnector()
    b.name = "second"
    host = {"processing": {"source_policies": {"custom": {"extraction": "heuristic"}},
                           "entity_type_configs": {"Resource": {"b": 3}}}}
    config, _ = register(host, [b, a])
    assert config == host
    assert register({}, [a, b])[0] == register({}, [b, a])[0]
    with pytest.raises(ConfigurationError, match="duplicate"):
        register({}, [a, a])
    b.source = "different"
    assert register({}, [a, b])[0] == {}


def test_journal_integrity_permissions_and_configuration(native, tmp_path):
    journal = RunJournal(tmp_path / "journal")
    secret = "never-persist-this-key"
    config = {"models": {"default": {"api_key": secret, "model": "model-a"}}}
    with Engine(config, connectors=[local()]) as engine:
        prepared = engine.prepare("local-aws", context(), journal=journal)
        assert all(secret not in p.read_text() for p in journal.directory.glob("*.json"))
        assert all(p.stat().st_mode & 0o077 == 0 for p in journal.directory.glob("*.json"))
        config["models"]["default"]["api_key"] = "rotated"
        with Engine(config, connectors=[local()]) as rotated:
            assert rotated.replay(prepared, journal=journal).complete
        changed = json.loads(json.dumps(config))
        changed["processing"] = {"source_policies": {"aws": {"matching": "exact"}}}
        with Engine(changed) as rotated:
            with pytest.raises(JournalError, match="configuration differs"):
                rotated.replay(prepared, journal=journal)
        path = journal._path(prepared.run_id, "input")
        artifact = json.loads(path.read_text())
        artifact["body_json"] += " "
        path.write_text(json.dumps(artifact))
        with pytest.raises(JournalError, match="integrity"):
            journal.load(prepared.run_id)
    untrusted = tmp_path / "untrusted"
    untrusted.mkdir(mode=0o755)
    with pytest.raises(JournalError, match="owner-only"):
        RunJournal(untrusted)


def test_checkpoint_requires_durable_complete_result_and_uses_cas(native, tmp_path):
    class Store:
        def __init__(self):
            self.value = None
            self.runs = set()
        def compare_and_set(self, key, *, expected, proposed, run_id):
            if run_id in self.runs:
                return
            if self.value != expected:
                raise RuntimeError("cursor conflict")
            self.value = proposed
            self.runs.add(run_id)
    connector = StaticConnector()
    original = connector.collect
    def collect(*args, **kwargs):
        batch = original(*args, **kwargs)
        batch.proposed_cursor = "next"
        return batch
    connector.collect = collect
    store, journal = Store(), RunJournal(tmp_path / "journal")
    with Engine({}, connectors=[connector]) as engine:
        prepared = engine.prepare(connector.name, context(), journal=journal)
        for complete in (False, True):
            engine._engine.complete = complete
            engine.replay(prepared, journal=journal)
            if not complete:
                with pytest.raises(JournalError):
                    journal.acknowledge(prepared.run_id, checkpoint_store=store)
                assert store.value is None
        journal.acknowledge(prepared.run_id, checkpoint_store=store)
        journal.acknowledge(prepared.run_id, checkpoint_store=store)
        assert store.value == "next"
        next_run = engine.prepare(connector.name, context(), journal=journal)
        engine.replay(next_run, journal=journal)
        with pytest.raises(RuntimeError, match="cursor conflict"):
            journal.acknowledge(next_run.run_id, checkpoint_store=store)


def test_failed_commit_outcome_persists_and_journal_failure_keeps_progress(native, tmp_path, monkeypatch):
    journal = RunJournal(tmp_path / "journal")
    with Engine({}, connectors=[local()]) as engine:
        prepared = engine.prepare("local-aws", context(), journal=journal)
        engine._engine.ok = False
        with pytest.raises(IngestionError):
            engine.replay(prepared, journal=journal)
        assert list(journal.directory.glob("*.result-*.json"))
        assert not list(journal.directory.glob("*.complete.json"))
        engine._engine.ok = True
        def full_disk(*args):
            raise OSError("disk full")
        monkeypatch.setattr(journal, "_publish", full_disk)
        with pytest.raises(JournalError) as error:
            engine.replay(prepared, journal=journal)
        assert error.value.outcome["committed"]["entities_created"] == 1


def test_close_cancels_collection_and_waits_for_exit(native, tmp_path):
    connector, entered, exited = StaticConnector(), threading.Event(), threading.Event()
    def collect(context, *, cancellation, deadline):
        entered.set()
        while not cancellation.cancelled:
            if time.monotonic() >= deadline:
                pytest.fail("close never cancelled collection")
            time.sleep(0.005)
        exited.set()
        raise CollectionError("cancelled")
    connector.collect = collect
    engine = Engine({}, connectors=[connector])
    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(engine.prepare, connector.name, context(), journal=RunJournal(tmp_path / "journal"), timeout=5)
        assert entered.wait(2)
        engine.close()
        assert exited.is_set()
        with pytest.raises(CollectionError, match="cancelled"):
            future.result(timeout=2)
    with pytest.raises(EngineClosedError):
        engine.prepare(connector.name, context(), journal=RunJournal(tmp_path / "journal"))




def test_incomplete_collection_holds_checkpoint_even_if_rust_completes(native, tmp_path):
    connector = StaticConnector()
    original = connector.collect
    def collect(*args, **kwargs):
        batch = original(*args, **kwargs)
        batch.complete = False
        batch.proposed_cursor = "must-not-advance"
        return batch
    connector.collect = collect
    journal = RunJournal(tmp_path / "journal")
    with Engine({}, connectors=[connector]) as engine:
        prepared = engine.prepare(connector.name, context(), journal=journal)
        assert engine.replay(prepared, journal=journal).complete
        with pytest.raises(JournalError, match="complete source collection"):
            journal.acknowledge(prepared.run_id, checkpoint_store=None)


def test_concurrent_same_run_cannot_refetch(native, tmp_path):
    connector, entered, release = StaticConnector(), threading.Event(), threading.Event()
    original = connector.collect
    calls = []
    def collect(*args, **kwargs):
        calls.append(1)
        entered.set()
        assert release.wait(3)
        return original(*args, **kwargs)
    connector.collect = collect
    journal, ctx = RunJournal(tmp_path / "journal"), context()
    with Engine({}, connectors=[connector]) as engine, ThreadPoolExecutor(1) as pool:
        pending = pool.submit(engine.prepare, connector.name, ctx, journal=journal)
        try:
            assert entered.wait(2)
            with pytest.raises(JournalError, match="already collected"):
                engine.prepare(connector.name, ctx, journal=journal)
        finally:
            release.set()
        assert journal.load(pending.result(timeout=2).run_id)
    assert calls == [1]


def test_collection_admission_is_bounded_and_wait_counts_against_deadline(native, tmp_path):
    connector, entered, release = StaticConnector(), threading.Event(), threading.Event()
    original = connector.collect
    def collect(*args, **kwargs):
        entered.set()
        assert release.wait(3)
        return original(*args, **kwargs)
    connector.collect = collect
    journal = RunJournal(tmp_path / "journal")
    with Engine({}, connectors=[connector], limits=Limits(admitted_calls=1)) as engine, ThreadPoolExecutor(1) as pool:
        first = pool.submit(engine.prepare, connector.name, context(), journal=journal)
        try:
            assert entered.wait(2)
            with pytest.raises(CollectionError, match="deadline"):
                engine.prepare(connector.name, context(), journal=journal, timeout=0.02)
        finally:
            release.set()
        assert first.result(timeout=2)


def test_journal_rejects_symlink_artifacts(native, tmp_path):
    journal = RunJournal(tmp_path / "journal")
    with Engine({}, connectors=[local()]) as engine:
        prepared = engine.prepare("local-aws", context(), journal=journal)
    path = journal._path(prepared.run_id, "input")
    moved = path.with_suffix(".moved")
    path.rename(moved)
    path.symlink_to(moved)
    with pytest.raises(JournalError, match="missing or invalid"):
        journal.load(prepared.run_id)




def test_connector_cannot_mutate_previous_cursor(native, tmp_path):
    connector = StaticConnector()
    original = connector.collect
    def collect(context, **kwargs):
        context.previous_cursor["offset"] = 999
        return original(context, **kwargs)
    connector.collect = collect
    ctx = context(previous_cursor={"offset": 1})
    with Engine({}, connectors=[connector]) as engine:
        with pytest.raises(CollectionError, match="connector_changed_scope"):
            engine.prepare(connector.name, ctx, journal=RunJournal(tmp_path / "journal"))
    assert ctx.previous_cursor == {"offset": 1}


def test_interrupt_after_commit_keeps_outcome(native, tmp_path, monkeypatch):
    from kg_sdk import IngestionInterrupted
    journal = RunJournal(tmp_path / "journal")
    with Engine({}, connectors=[local()]) as engine:
        prepared = engine.prepare("local-aws", context(), journal=journal)
        def interrupt(*args, **kwargs):
            raise KeyboardInterrupt
        monkeypatch.setattr(journal, "_record", interrupt)
        with pytest.raises(IngestionInterrupted) as error:
            engine.replay(prepared, journal=journal)
        assert error.value.outcome["committed"]["entities_created"] == 1


def test_registration_is_closed_after_construction(native):
    connector = StaticConnector()
    with Engine({}, connectors=[connector]) as engine:
        with pytest.raises(TypeError):
            engine._connectors["late"] = ("late", connector)
