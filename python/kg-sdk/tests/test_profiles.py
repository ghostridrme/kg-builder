"""Profile transport and journal contracts. Fake native responses; no model calls."""
import json
from uuid import uuid4
import pytest
from kg_sdk import Engine, ProfileRef, load_profile, RunJournal, CollectionContext, CollectedBatch
from kg_sdk.profiles import bindings


def test_profile_references_and_lossless_loading(tmp_path):
    for value in [True, 0, -1, 2**63, "1"]:
        with pytest.raises(ValueError):
            ProfileRef("p", value)
    source = {"s": {"profile_id": "p", "revision": 1}}
    copied = bindings(source)
    source["s"]["revision"] = 2
    assert copied["s"]["revision"] == 1
    path = tmp_path / "p.json"
    path.write_text('{"revision":1,"revision":2}')
    with pytest.raises(ValueError, match="duplicate"):
        load_profile(path)
    path.write_text('{"revision":1,"ontology":{"properties":{"number":42}}}')
    assert load_profile(path)["ontology"]["properties"]["number"] == 42
    path = tmp_path / "p.yaml"
    pytest.importorskip("yaml")
    for text in ["revision: 1\nrevision: 2", "date: 2026-01-01", "a: !!python/object:dict {}"]:
        path.write_text(text)
        with pytest.raises(Exception):
            load_profile(path)
    path.write_text('revision: 1\nname: "001"')
    assert load_profile(path) == {"revision": 1, "name": "001"}


class Connector:
    name = "collector"
    source = "inventory"
    calls = 0
    def collect(self, context, **kwargs):
        self.calls += 1
        return CollectedBatch(snapshots=[{"name":"scan","namespace":context.namespace,"source":self.source,
            "org_id":context.org_id,"data_type":"text","content":"An operational service","entities":[]}], complete=False)


@pytest.fixture
def native(monkeypatch):
    events = []
    class Native:
        def __init__(self, *args):
            pass
        def close(self):
            pass
        def run(self, op, raw, org, run_id, *args):
            payload = json.loads(raw)
            events.append((op,payload))
            if op == "profiles":
                refs = payload["bindings"]
                result = {"manifest":{"org_id":org,"sources":{s:{} for s in refs},
                    "profiles":{s:{"document":ref,"digest":"test-digest"} for s,ref in refs.items()}}}
            elif op == "decide":
                result = {"model":"fake","answers":{}}
            else:
                result = {"run_id":run_id,"complete":True,"committed":{}}
            return json.dumps({"ok":True,"result":result})
    monkeypatch.setattr("kg_sdk._native.NativeEngine", Native)
    return events


def test_preparation_freezes_binding_replay_ignores_new_selection(native, tmp_path):
    connector = Connector()
    journal = RunJournal(tmp_path / "journal")
    v1 = ProfileRef("operations", 1)
    with Engine({}, connectors=[connector], connector_profiles={"collector":v1}) as engine:
        prepared = engine.prepare("collector", CollectionContext(org_id="o",namespace="p",selection={"scope":"repo"}),journal=journal)
        assert native[0][0] == "profiles"
        assert connector.calls == 1
        body = journal._body(prepared)
        assert body["profile_format"] == 1
        assert body["profiles"]["inventory"] == v1.to_dict()
    with Engine({}, profiles={"inventory":ProfileRef("operations",2)}) as engine:
        assert engine.replay(prepared,journal=journal).complete
    op, payload = native[-1]
    assert op == "ingest_profiles"
    assert payload["bindings"]["inventory"] == v1.to_dict()
    assert payload["expected"] == body["profile_manifest"]
    assert connector.calls == 1


def test_unbound_transport_and_decisions_have_no_ingestion_identity(native):
    with Engine({}) as engine:
        engine.ingest([],org_id="o")
        assert native[-1] == ("ingest", [])
        result = engine.decide({}, {})
        assert result == {"model":"fake", "answers":{}}
        assert "run_id" not in result
    with pytest.raises(Exception, match="conflicting profiles"):
        Engine({},connectors=[Connector()],profiles={"inventory":ProfileRef("p",1)},connector_profiles={"collector":ProfileRef("p",2)})
