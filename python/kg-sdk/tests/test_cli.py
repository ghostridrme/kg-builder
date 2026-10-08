"""CLI boundary tests; never construct a live database/provider client."""
import io
import json
import subprocess
import sys
from uuid import NAMESPACE_OID, UUID, uuid5

import pytest
from kg_sdk import cli
from kg_sdk.engine import IngestionError, IngestionInterrupted
from kg_sdk.journal import JournalError


@pytest.fixture
def host(tmp_path, monkeypatch):
    calls = []
    config = tmp_path / "config.json"
    config.write_text('{}')
    monkeypatch.delenv("KG_ORG_ID", raising=False)
    monkeypatch.setattr(sys, "stdin", io.StringIO('[{"name":"demo"}]'))

    class Engine:
        outcome = {"complete": True, "run_id": "known", "committed": {"entities": 2}}
        failure = None

        def __init__(self, config, **kwargs):
            calls.append(("init", config, kwargs))

        def __enter__(self):
            return self

        def __exit__(self, *args):
            calls.append(("close",))

        def invoke(self, method, *args, **kwargs):
            calls.append((method, args, kwargs))
            if self.failure:
                raise self.failure
            return self.outcome

        def ingest(self, *args, **kwargs):
            return self.invoke("ingest", *args, **kwargs)

        def rebuild_communities(self, *args, **kwargs):
            return self.invoke("community", *args, **kwargs)

        def replay(self, *args, **kwargs):
            return self.invoke("replay", *args, **kwargs)

        def learn_rules(self, *args, **kwargs):
            return self.invoke("learn", *args, **kwargs)

        def list_rules(self, *args, **kwargs):
            return self.invoke("list", *args, **kwargs)

    monkeypatch.setattr(cli, "Engine", Engine)
    return config, calls, Engine


def command(host, *extra):
    return ["ingest", "--config", str(host[0]), "--org", "test", *extra]


@pytest.mark.parametrize("payload", ['{"name":"demo"}', '[{"name":"demo"}]', '{"snapshots":[{"name":"demo"}]}', '{"inputs":[{"kind":"existing","input":{}}]}'])
def test_json_forms(host, monkeypatch, capsys, payload):
    monkeypatch.setattr(sys, "stdin", io.StringIO(payload))
    assert cli.main(command(host, "--run-id", "batch-one")) == 0
    assert host[1][1][2]["run_id"] == uuid5(NAMESPACE_OID, "batch-one")
    assert len(host[1][1][1][0]) == 1
    assert json.loads(capsys.readouterr().out)["committed"] == {"entities": 2}
    assert host[1][-1] == ("close",)


def test_jsonl_file(host, tmp_path, capsys):
    data = tmp_path / "input.jsonl"
    data.write_text('{"name":"日本"}\n\n{"name":"two"}\n')
    assert cli.main(command(host, "--file", str(data), "--format", "jsonl")) == 0
    assert host[1][1][1][0] == [{"name": "日本"}, {"name": "two"}]


@pytest.mark.parametrize("payload", ['{"x":1,"x":2}', '[NaN]', '[{"x":9223372036854775808}]', '{"snapshots":[],"extra":1}', '[1]', '{'])
def test_bad_input_rejected_before_engine(host, monkeypatch, capsys, payload):
    monkeypatch.setattr(sys, "stdin", io.StringIO(payload))
    assert cli.main(command(host)) == 2
    assert not host[1]
    assert UUID(json.loads(capsys.readouterr().out)["run_id"])


def test_incomplete_and_failures_keep_progress(host, capsys, monkeypatch):
    for error, status in ((None, 3), (IngestionError, 1), (IngestionInterrupted, 1)):
        outcome = {"run_id": "saved", "complete": False, "committed": {"entities": 9}, "commit_unknown": True}
        host[2].outcome = outcome
        host[2].failure = error(outcome) if error else None
        monkeypatch.setattr(sys, "stdin", io.StringIO('[]'))
        assert cli.main(command(host)) == status
        assert json.loads(capsys.readouterr().out) == outcome


def test_policy_and_ontology(host, tmp_path, capsys):
    ontology = tmp_path / "ontology.json"
    ontology.write_text('{"entity_types":{}}')
    assert cli.main(command(host, "--no-llm", "--ontology", str(ontology))) == 0
    assert host[1][0][1]["models"] == {"default": {"type": "disabled"}}
    assert host[1][0][1]["processing"]["policy"]["matching"] == "exact"
    assert host[1][0][2]["ontology_org_id"] == "test"


@pytest.mark.parametrize("flags", [("--no-llm", "--matching", "semantic"), ("--community", "x", "--ontology", "x"), ("--timeout", "nan"), ("--run-id", str(UUID(int=0))), ("--graph", "memory")])
def test_bad_flags(host, capsys, flags):
    assert cli.main(command(host, *flags)) == 2
    assert not host[1]
    assert json.loads(capsys.readouterr().out)["error"]


def test_community_and_telemetry_cleanup(host, tmp_path, monkeypatch, capsys):
    events = []
    class Handle:
        def close(self):
            events.append("closed")
    monkeypatch.setattr(cli, "configure_telemetry", lambda config: (events.append(config), Handle())[1])
    telemetry = tmp_path / "telemetry.json"
    telemetry.write_text('{"filter":"info"}')
    assert cli.main(command(host, "--community", "prod", "--telemetry-config", str(telemetry))) == 0
    assert host[1][1][0] == "community"
    assert events == [{"filter": "info"}, "closed"]


def test_replay_loads_and_preserves_run_id(host, monkeypatch, capsys):
    class Journal:
        def __init__(self, path):
            self.path = path
        def load(self, run_id):
            return ("saved", run_id)
    monkeypatch.setattr(cli, "RunJournal", Journal)
    assert cli.main(["replay", "--config", str(host[0]), "--journal", "journal", "--run-id", "saved"]) == 0
    assert host[1][1][1] == (("saved", uuid5(NAMESPACE_OID, "saved")),)


def test_journal_error_keeps_progress(host, capsys):
    host[2].failure = JournalError("private payload", outcome={"run_id": "saved", "committed": {"entities": 3}})
    assert cli.main(command(host)) == 1
    result = json.loads(capsys.readouterr().out)
    assert result["committed"] == {"entities": 3}
    assert "private payload" not in str(result)


@pytest.mark.parametrize("action,flags", [("learn", ["--auto-promote"]), ("list", ["--status", "revoked"])])
def test_rule_dispatch(host, capsys, action, flags):
    assert cli.main(["rules", action, "--config", str(host[0]), "--org", "test", "--source", "aws", *flags]) == 0
    assert host[1][1][0] == action
    assert host[1][1][1] == ("aws",)


def test_secrets_do_not_leak(host, capsys):
    host[2].failure = RuntimeError("secret-provider-token")
    assert cli.main(command(host)) == 1
    output = capsys.readouterr()
    assert "secret-provider-token" not in output.out + output.err
    assert json.loads(output.out)["commit_unknown"] is True


def test_module_help_without_dotenv_or_engine(tmp_path):
    (tmp_path / ".env").write_text("KG_ORG_ID=should-not-load\n")
    result = subprocess.run([sys.executable, "-m", "kg_sdk", "--help"], cwd=tmp_path, capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    assert "ingest" in result.stdout and "replay" in result.stdout


def test_missing_org_ignores_dotenv(host, tmp_path, monkeypatch, capsys):
    (tmp_path / ".env").write_text("KG_ORG_ID=should-not-load\n")
    monkeypatch.chdir(tmp_path)
    assert cli.main(["ingest", "--config", str(host[0])]) == 2
    assert not host[1]


def test_no_llm_auto_retains_legacy_heuristic_default(host, capsys):
    assert cli.main(command(host, "--no-llm", "--extraction", "auto")) == 0
    assert host[1][0][1]["processing"]["policy"]["extraction"] == "heuristic"


@pytest.fixture
def preparation(host, tmp_path, monkeypatch):
    from types import SimpleNamespace
    from kg_sdk.journal import PreparedRun

    events = []
    connector = SimpleNamespace(name="inventory", source="aws",
                                collect=lambda *a, **kw: None)
    def factory():
        events.append("factory")
        return connector
    module = SimpleNamespace(factory=factory)
    monkeypatch.setitem(sys.modules, "trusted_cli_test_connector", module)
    context = tmp_path / "context.json"
    context.write_text(json.dumps({"namespace": "prod", "selection": {"account": "123"},
                                  "previous_cursor": {"version": 1}, "generation": 2,
                                  "limits": {"max_pages": 4}}))

    def prepare(self, name, context, **kwargs):
        self.invoke("prepare", name, context, **kwargs)
        return PreparedRun(context.run_id, "saved-checksum")
    monkeypatch.setattr(host[2], "prepare", prepare, raising=False)
    args = ["prepare", "--config", str(host[0]), "--org", "test", "--run-id", "collection",
            "--connector", "trusted_cli_test_connector:factory", "--context", str(context),
            "--journal", str(tmp_path / "journal"), "--timeout", "12"]
    return args, context, connector, module, events


def test_prepare_collects_without_ingestion(host, preparation, capsys):
    args, _, connector, _, events = preparation
    assert cli.main(args) == 0
    assert events == ["factory"]
    assert host[1][0][2]["connectors"] == (connector,)
    assert [call[0] for call in host[1]] == ["init", "prepare", "close"]
    name, context = host[1][1][1]
    assert name == "inventory"
    assert context.org_id == "test" and context.namespace == "prod"
    assert context.previous_cursor == {"version": 1} and context.generation == 2
    assert context.limits.max_pages == 4
    assert host[1][1][2]["timeout"] == 12
    assert json.loads(capsys.readouterr().out) == {
        "operation": "prepare", "prepared": True, "ingested": False,
        "run_id": str(uuid5(NAMESPACE_OID, "collection")), "artifact_hash": "saved-checksum"}


@pytest.mark.parametrize("field,value", [("org_id", "other"), ("run_id", "other"), ("unknown", 1),
                                         ("limits", {"unknown": 1}), ("limits", {"max_pages": 0}),
                                         ("namespace", ""), ("selection", {})])
def test_prepare_rejects_context_before_factory(host, preparation, capsys, field, value):
    args, context, _, _, events = preparation
    data = json.loads(context.read_text())
    data[field] = value
    context.write_text(json.dumps(data))
    assert cli.main(args) == 2
    assert not events and not host[1]


@pytest.mark.parametrize("spec", ["missing-colon", "module:factory:extra", ":factory", "module:", "module:factory()"])
def test_prepare_rejects_factory_syntax(host, preparation, capsys, spec):
    args = preparation[0]
    args[args.index("--connector") + 1] = spec
    assert cli.main(args) == 2
    assert not preparation[4] and not host[1]


@pytest.mark.parametrize("factory", [None, lambda: object()])
def test_prepare_rejects_invalid_factory_result(host, preparation, capsys, factory):
    preparation[3].factory = factory
    assert cli.main(preparation[0]) == 2
    assert not host[1]


def test_prepare_factory_failure_is_sanitized(host, preparation, capsys):
    def factory():
        raise RuntimeError("private-credential")
    preparation[3].factory = factory
    assert cli.main(preparation[0]) == 1
    assert not host[1]
    assert "private-credential" not in capsys.readouterr().out


def test_prepare_collection_failure_has_no_ingestion(host, preparation, capsys):
    from kg_sdk import CollectionError
    host[2].failure = CollectionError("permission_denied")
    assert cli.main(preparation[0]) == 1
    result = json.loads(capsys.readouterr().out)
    assert result["prepared"] is False and result["ingested"] is False
    assert [call[0] for call in host[1]] == ["init", "prepare", "close"]


@pytest.mark.parametrize("label", ["", " ", "\t\n"])
def test_blank_run_label_rejected(host, capsys, label):
    assert cli.main(command(host, "--run-id", label)) == 2
    assert not host[1]


def test_replay_registers_original_connector_without_collecting(host, preparation, monkeypatch, capsys):
    from kg_sdk.journal import PreparedRun
    class Journal:
        def __init__(self, path):
            pass
        def load(self, run_id):
            return PreparedRun(run_id, "checksum")
    monkeypatch.setattr(cli, "RunJournal", Journal)
    assert cli.main(["replay", "--config", str(host[0]), "--journal", "journal", "--run-id", "collection",
                     "--connector", "trusted_cli_test_connector:factory"]) == 0
    assert host[1][0][2]["connectors"] == (preparation[2],)
    assert [call[0] for call in host[1]] == ["init", "replay", "close"]


@pytest.mark.parametrize("status", ["proposed", "active", "rejected", "uncertain", "stale", "revoked"])
def test_all_rule_statuses_are_forwarded(host, capsys, status):
    assert cli.main(["rules", "list", "--config", str(host[0]), "--org", "test",
                     "--source", "aws", "--status", status]) == 0
    assert host[1][1][2]["status"] == status


@pytest.mark.parametrize("mode", [("--extraction", "llm"), ("--matching", "semantic"),
                                   ("--edge-discovery", "llm"), ("--edge-discovery", "heuristic_then_llm"),
                                   ("--edge-ambiguity", "llm")])
def test_explicit_model_modes_reject_no_llm_before_bootstrap(host, capsys, mode):
    assert cli.main(command(host, "--no-llm", *mode)) == 2
    assert not host[1]


@pytest.mark.parametrize("filename", ["aws_snapshot.json", "aws_text_snapshot.json"])
def test_documented_snapshots_decode_bare_and_wrapped(host, monkeypatch, capsys, filename):
    from pathlib import Path
    snapshots = json.loads((Path(__file__).resolve().parent / "fixtures/ingestion" / filename).read_text())
    for payload in (snapshots, {"snapshots": snapshots}):
        host[1].clear()
        monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))
        assert cli.main(command(host)) == 0
        assert host[1][1][1][0] == snapshots


@pytest.mark.parametrize("change,status", [({}, 2), ({"context": {"stage": "input_validation"}}, 2),
    ({"context": {"stage": "rule_admission"}}, 2), ({"batches_committed": 1}, 1),
    ({"committed": {"entities_created": 1}}, 1), ({"commit_unknown": True}, 1),
    ({"context": {"stage": "entity_extraction"}}, 1)])
def test_native_request_rejection_exit_status_preserves_commit_evidence(host, capsys, change, status):
    outcome = {"run_id": "saved", "cause": "input_validation", "context": {"stage": "request"},
               "committed": {"entities_created": 0}, "batches_committed": 0, "commit_unknown": False}
    outcome.update(change)
    host[2].failure = IngestionError(outcome)
    assert cli.main(command(host)) == status
    assert json.loads(capsys.readouterr().out) == outcome
