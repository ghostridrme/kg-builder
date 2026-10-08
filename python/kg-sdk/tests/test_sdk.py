import json
import os
import subprocess
import sys
from decimal import Decimal
from uuid import uuid4

import pytest
import kg_sdk
from kg_sdk.serialization import dumps, loads


def test_import_has_no_host_side_effects():
    code = '''
import os, threading, logging, socket
before = (dict(os.environ), len(threading.enumerate()), list(logging.getLogger().handlers))
socket.socket = lambda *a, **k: (_ for _ in ()).throw(AssertionError("import opened a socket"))
import kg_sdk
assert before == (dict(os.environ), len(threading.enumerate()), list(logging.getLogger().handlers))
assert kg_sdk.__version__ == kg_sdk._native.__version__
'''
    subprocess.run([sys.executable, "-c", code], check=True, timeout=15)


@pytest.mark.parametrize("value", [2**63, -(2**63)-1, float("nan"), float("inf"), Decimal("1"), {1: "x"}, object(), "\ud800"])
def test_lossy_values_rejected(value):
    with pytest.raises((ValueError, TypeError)):
        dumps(value)


def test_fidelity_and_limits():
    value = {"missing_is_not_null": None, "empty": [], "nested": {"日本": [True, -2**63, 2**63-1, 1.0]}}
    assert loads(dumps(value)) == value
    assert dumps({}) != dumps({"x": None})
    cycle = []; cycle.append(cycle)
    with pytest.raises(ValueError, match="circular"):
        dumps(cycle)
    with pytest.raises(ValueError, match="byte limit"):
        dumps("é" * 100, max_bytes=150)
    with pytest.raises(ValueError, match="nesting"):
        dumps([[[1]]], max_depth=2)
    with pytest.raises(ValueError, match="duplicate"):
        loads('{"x":1,"x":2}')
    with pytest.raises(ValueError):
        loads('1e400')


def test_bad_configuration_does_not_leak_values():
    secret = "secret-do-not-print"
    with pytest.raises(kg_sdk.ConfigurationError) as caught:
        kg_sdk.Engine({"password": secret})
    assert secret not in str(caught.value)


@pytest.mark.parametrize("protocol", [3, 999])
def test_native_version_mismatch_fails_import(protocol):
    code = f'''
import kg_sdk, importlib
kg_sdk._native.PROTOCOL_VERSION = {protocol}
try:
    importlib.reload(kg_sdk)
except ImportError as e:
    assert "reinstall" in str(e)
else:
    raise AssertionError("stale native accepted")
'''
    subprocess.run([sys.executable, "-c", code], check=True, timeout=15)


def test_tokens_are_independent():
    a, b = kg_sdk.CancellationToken(), kg_sdk.CancellationToken()
    a.cancel()
    assert a.cancelled and not b.cancelled


def test_incomplete_result_is_not_success():
    result = kg_sdk.IngestionResult(complete=False, run_id=str(uuid4()), batches_committed=1)
    with pytest.raises(kg_sdk.IngestionError) as caught:
        result.require_complete()
    assert caught.value.outcome["batches_committed"] == 1


def test_telemetry_is_explicit_and_cannot_replace_host_subscriber():
    code = '''
import kg_sdk
handle = kg_sdk.configure_telemetry({"filter": "warn"})
try:
    kg_sdk.configure_telemetry({"filter": "info"})
except kg_sdk.ConfigurationError:
    pass
else:
    raise AssertionError("host subscriber replaced")
handle.close()
handle.close()
'''
    subprocess.run([sys.executable, "-c", code], check=True, timeout=20)


def test_concurrent_telemetry_close_does_not_hold_gil_while_waiting():
    code = """
import kg_sdk, threading
handle = kg_sdk.configure_telemetry({"filter": "off"})
barrier = threading.Barrier(17)
def close():
    barrier.wait()
    handle.close()
threads = [threading.Thread(target=close) for _ in range(16)]
for thread in threads: thread.start()
barrier.wait()
for thread in threads: thread.join()
handle.close()
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=10)


def test_admission_precedes_encoding_and_waiters_cancel_timeout_and_close(monkeypatch):
    import threading
    from concurrent.futures import ThreadPoolExecutor
    from kg_sdk import Engine, Limits, IngestionError, EngineClosedError, CancellationToken

    entered, release = threading.Event(), threading.Event()
    encodes = []
    class Native:
        def __init__(self, *args): pass
        def run(self, operation, payload, org, run, *args):
            entered.set()
            assert release.wait(5)
            return json.dumps({"ok": True, "result": {"complete": True, "run_id": run}})
        def close(self): release.set()
    monkeypatch.setattr(kg_sdk._native, "NativeEngine", Native)
    engine = Engine({}, limits=Limits(admitted_calls=1))
    encode = engine._encode
    def counted(value, *args):
        encodes.append(value)
        return encode(value, *args)
    engine._encode = counted
    with ThreadPoolExecutor(4) as pool:
        active = pool.submit(engine.ingest, [], org_id="test")
        assert entered.wait(2)
        with pytest.raises(IngestionError) as caught:
            engine.ingest([], org_id="test", timeout=0.03)
        assert caught.value.outcome["batches_committed"] == 0
        assert len(encodes) == 1  # waiting request never reached encoding
        token = CancellationToken()
        waiting = pool.submit(engine.ingest, [], org_id="test", cancellation=token)
        token.cancel()
        with pytest.raises(IngestionError): waiting.result(timeout=2)
        assert len(encodes) == 1
        waiting = pool.submit(engine.ingest, [], org_id="test")
        engine.close()
        with pytest.raises(EngineClosedError): waiting.result(timeout=2)
        assert active.result(timeout=2).complete
        assert len(encodes) == 1


def test_maintenance_deadlines_forward_and_include_encoding(monkeypatch):
    import time
    from kg_sdk import Engine, InputValidationError, IngestionError
    calls = []
    class Native:
        def __init__(self, *args): pass
        def run(self, operation, payload, org, run, cancel, timeout, trace):
            calls.append((operation, timeout))
            return json.dumps({"ok": True, "result": {"complete": True, "run_id": run}})
        def close(self): pass
    monkeypatch.setattr(kg_sdk._native, "NativeEngine", Native)
    with Engine({}) as engine:
        assert engine.summarize_saga(org_id="org", namespace="ns", saga={"kind":"name", "name":"s"}, timeout=1).complete
        assert engine.rebuild_communities(org_id="org", namespace="ns", timeout=1).complete
        assert [op for op, _ in calls] == ["saga", "community"]
        assert all(0 < timeout <= 1 for _, timeout in calls)
        with pytest.raises(InputValidationError):
            engine.rebuild_communities(org_id="org", namespace="ns", timeout=float("nan"))
        encode = engine._encode
        def slow(value, *args):
            time.sleep(0.02)
            return encode(value, *args)
        engine._encode = slow
        with pytest.raises(IngestionError):
            engine.rebuild_communities(org_id="org", namespace="ns", timeout=0.005)
        assert len(calls) == 2


def test_transport_upgrade_preserves_prepared_run_fingerprints():
    from kg_sdk.registration import configuration_fingerprint
    from kg_sdk.connector import digest
    assert kg_sdk._native.PROTOCOL_VERSION == 4
    assert configuration_fingerprint({}, None, None) == digest({
        "protocol": 1, "config": {}, "ontology": None, "ontology_org_id": None,
    })


def test_thread_input_helpers_preserve_binding_and_reject_two_memberships():
    # SDK boundary: constructor arguments must survive serialization into native input.
    from kg_sdk.inputs import SnapshotInput, ExistingSnapshotInput
    ref = {"kind": "name", "name": "incident-1042", "predecessor_snapshot_uuid": None}
    snapshot = SnapshotInput(namespace="prod", name="alert", source="test", data_type="text",
                             content="incident reported", thread=ref)
    assert snapshot.to_dict()["thread"] == ref
    assert "saga" not in snapshot.to_dict()
    reused = ExistingSnapshotInput(namespace="prod", snapshot_uuid=str(uuid4()), thread=ref)
    assert reused.to_dict()["input"]["thread"] == ref
    snapshot.saga = {"saga": {"kind": "name", "name": "different"}}
    with pytest.raises(ValueError, match="not both"):
        snapshot.to_dict()
    reused.saga = snapshot.saga
    with pytest.raises(ValueError, match="exactly one"):
        reused.to_dict()


def test_null_disabled_embedder_has_same_fingerprint_as_omitted():
    from kg_sdk.registration import configuration_fingerprint
    assert configuration_fingerprint({"embedder": None}, None, None) == configuration_fingerprint({}, None, None)
