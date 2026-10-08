"""Shared SDK collection tests use the generic local-folder connector."""
import json
import time
from pathlib import Path
import pytest
from kg_sdk import CancellationToken, CollectionContext
from kg_connectors import LocalFolderConnector

DATA = Path(__file__).resolve().parents[2] / "data/aws"

def context(**kwargs):
    values = dict(org_id="sdk-unit", namespace="prod", selection={"dataset":"aws-local-demo"})
    values.update(kwargs)
    return CollectionContext(**values)

def local():
    return LocalFolderConnector(DATA)

@pytest.fixture
def native(monkeypatch):
    class FakeNative:
        def __init__(self, *args):
            self.calls = []
            self.complete = True
            self.ok = True

        def run(self, op, value, org, run_id, *args):
            self.calls.append((op, value, org, run_id))
            return json.dumps({"ok": self.ok, "result": {"run_id": run_id, "complete": self.complete,
                              "committed": {"entities_created": 1}}})

        def close(self):
            pass
    monkeypatch.setattr("kg_sdk._native.NativeEngine", FakeNative)


def collect(connector, context, seconds=5):
    """Run one bounded collection: fresh cancellation token, short deadline."""
    return connector.collect(context, cancellation=CancellationToken(), deadline=time.monotonic() + seconds)

