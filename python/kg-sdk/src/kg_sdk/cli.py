"""Explicit file/stdin host for the synchronous SDK. Never loads dotenv files."""
import argparse
import importlib
import json
import math
import os
import sys
from pathlib import Path
from uuid import NAMESPACE_OID, UUID, uuid4, uuid5

from .serialization import loads
from .connector import CollectionContext, CollectionError, CollectionLimits
from .engine import (ConfigurationError, Engine, IngestionError, IngestionInterrupted,
                     InputValidationError, Limits, OperationError, configure_telemetry)
from .journal import JournalError, RunJournal


class UsageError(ValueError):
    pass


class Parser(argparse.ArgumentParser):
    def error(self, message):
        # Argument values can contain credentials; do not echo the parser's text.
        raise UsageError("invalid command arguments; use --help")


def _run_id(value):
    if not value.strip():
        raise ValueError("run ID label must not be blank")
    try:
        result = UUID(value)
    except ValueError:
        result = uuid5(NAMESPACE_OID, value)
    if not result.int:
        raise ValueError("run ID cannot be nil")
    return result


def _parser():
    parser = Parser(prog="kg", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    def common(command, *, org=True):
        command.add_argument("--config", required=True, help="explicit SDK configuration JSON file")
        command.add_argument("--run-id", type=_run_id, help="UUID or stable UUIDv5 label")
        if org:
            command.add_argument("--org", default=os.environ.get("KG_ORG_ID"))
        command.add_argument("--telemetry-config", help="explicit native telemetry JSON file")
        command.add_argument("--timeout", type=float, help="settled cancellation deadline in seconds")

    ingest = commands.add_parser("ingest", help="ingest JSON or JSONL from a file or stdin")
    common(ingest)
    source = ingest.add_mutually_exclusive_group()
    source.add_argument("--file", help="input file; omit or use - for stdin")
    source.add_argument("--community", metavar="NAMESPACE", help="rebuild namespace communities")
    ingest.add_argument("--format", choices=("json", "jsonl"), default="json")
    ingest.add_argument("--ontology", help="organization-specific ontology JSON file")
    ingest.add_argument("--trace-id")
    ingest.add_argument("--profiles", help="JSON source-to-exact-profile-reference map")
    ingest.add_argument("--no-llm", action="store_true")
    ingest.add_argument("--extraction", choices=("auto", "llm", "heuristic"))
    ingest.add_argument("--matching", choices=("semantic", "exact"))
    ingest.add_argument("--edge-discovery", choices=("llm", "heuristic_then_llm", "heuristic"))
    ingest.add_argument("--edge-ambiguity", choices=("llm", "skip"))
    prepare = commands.add_parser("prepare", help="collect durable input without ingestion or checkpoint acknowledgment")
    common(prepare)
    prepare.add_argument("--connector", required=True, metavar="MODULE:factory",
                         help="trusted Python factory callable returning a configured Connector")
    prepare.add_argument("--context", required=True, help="collection context JSON file")
    prepare.add_argument("--journal", required=True, type=Path)
    prepare.add_argument("--profiles", help="JSON source-to-exact-profile-reference map")
    profile = commands.add_parser("profiles", help="publish and read immutable domain profiles")
    common(profile)
    profile.add_argument("action", choices=("register", "get", "list"))
    profile.add_argument("--file", help="JSON or YAML profile document for registration")
    profile.add_argument("--id", help="exact profile ID for get")
    profile.add_argument("--revision", type=int, help="exact revision for get")
    profile.add_argument("--after", help="JSON reference cursor file for list")
    profile.add_argument("--limit", type=int, default=20)
    replay = commands.add_parser("replay", help="replay immutable journal input without recollecting")
    common(replay, org=False)
    replay.add_argument("--journal", required=True, type=Path)
    replay.add_argument("--connector", metavar="MODULE:factory",
                        help="register original recommendations without collecting")
    rules = commands.add_parser("rules", help="native rule learning and inspection")
    actions = rules.add_subparsers(dest="action", required=True)
    for action in ("learn", "list"):
        command = actions.add_parser(action)
        common(command)
        command.add_argument("--source", required=True)
        if action == "learn":
            command.add_argument("--auto-promote", action="store_true")
        else:
            command.add_argument("--status", choices=("proposed", "active", "rejected", "uncertain", "stale", "revoked"))
    return parser


def _read(path, *, stdin=False):
    limit = Limits().max_request_bytes
    if stdin and (path is None or path == "-"):
        stream = getattr(sys.stdin, "buffer", sys.stdin)
        raw = stream.read(limit + 1)
    else:
        with open(path, "rb") as stream:
            raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise UsageError("input exceeds byte limit")
    return raw


def _object(path):
    value = loads(_read(path))
    if type(value) is not dict:
        raise UsageError("configuration, context and ontology must be JSON objects")
    return value


def _snapshots(args):
    raw = _read(args.file, stdin=True)
    if args.format == "jsonl":
        result = [loads(line) for line in raw.splitlines() if line.strip()]
    else:
        result = loads(raw)
        if type(result) is dict:
            if "inputs" in result or "snapshots" in result:
                if len(result) != 1:
                    raise UsageError("input wrapper has unknown fields")
                result = result.get("inputs", result.get("snapshots"))
            else:
                result = [result]
    if type(result) is not list or any(type(item) is not dict for item in result):
        raise UsageError("input must contain snapshot objects")
    if len(result) > Limits().max_snapshots:
        raise UsageError("snapshot count limit exceeded")
    return result


def _policy(config, args):
    processing = config.get("processing", {})
    if type(processing) is not dict or type(processing.get("policy", {})) is not dict:
        raise UsageError("processing and policy must be JSON objects")
    overrides = {key: getattr(args, key) for key in
                 ("extraction", "matching", "edge_discovery", "edge_ambiguity")
                 if getattr(args, key) is not None}
    if args.no_llm:
        defaults = dict(extraction="heuristic", matching="exact", edge_discovery="heuristic", edge_ambiguity="skip")
        if any(value != defaults[key] and not (key == "extraction" and value == "auto")
               for key, value in overrides.items()):
            raise UsageError("requested policy needs a language model with --no-llm")
        config["models"] = {"default": {"type": "disabled"}}
        config.setdefault("processing", {})["policy"] = defaults
        return
    if overrides:
        config.setdefault("processing", {}).setdefault("policy", {}).update(overrides)



def _collection_context(path, org_id, run_id):
    value = _object(path)
    allowed = {"namespace", "selection", "previous_cursor", "generation", "limits"}
    if set(value) - allowed:
        raise UsageError("unknown context fields; organization and run ID belong in CLI flags")
    limits = value.pop("limits", {})
    if type(limits) is not dict:
        raise UsageError("collection limits must be an object")
    return CollectionContext(org_id=org_id, run_id=run_id, limits=CollectionLimits(**limits), **value)


def _factory_spec(spec):
    parts = spec.split(":")
    if (len(parts) != 2 or not all(part.isidentifier() for part in parts[0].split("."))
            or not parts[1].isidentifier()):
        raise UsageError("connector must name a trusted MODULE:factory")
    return parts


def _connector(parts):
    try:
        module = importlib.import_module(parts[0])
        factory = getattr(module, parts[1], None)
    except ImportError:
        raise UsageError("connector module unavailable; install its dependencies") from None
    if not callable(factory):
        raise UsageError("connector factory must be callable")
    connector = factory()
    if (not isinstance(getattr(connector, "name", None), str) or not connector.name.strip()
            or not isinstance(getattr(connector, "source", None), str) or not connector.source.strip()
            or not callable(getattr(connector, "collect", None))):
        raise UsageError("connector factory must return a configured Connector")
    return connector


def _execute(args, run_id):
    if args.timeout is not None and (not math.isfinite(args.timeout) or args.timeout <= 0):
        raise UsageError("timeout must be positive and finite")
    if args.command != "replay" and (not args.org or not args.org.strip()):
        raise UsageError("--org or KG_ORG_ID is required")
    config = _object(args.config)
    telemetry_config = _object(args.telemetry_config) if args.telemetry_config else None
    kwargs = {}
    snapshots = None
    prepared = journal = None
    if args.command == "ingest":
        if args.community is not None:
            if not args.community.strip() or args.ontology or args.timeout is not None or args.trace_id or args.format != "json":
                raise UsageError("community requires a namespace and rejects ontology, timeout, trace ID and JSONL")
        else:
            snapshots = _snapshots(args)
        _policy(config, args)
        if args.ontology:
            kwargs = dict(ontology=_object(args.ontology), ontology_org_id=args.org)
    elif args.command == "prepare":
        parts = _factory_spec(args.connector)
        context = _collection_context(args.context, args.org, run_id)
        journal = RunJournal(args.journal)
        connector = _connector(parts)
        kwargs["connectors"] = (connector,)
    elif args.command == "replay":
        if args.run_id is None:
            raise UsageError("replay requires --run-id")
        journal = RunJournal(args.journal)
        prepared = journal.load(run_id)
        if args.connector:
            kwargs["connectors"] = (_connector(_factory_spec(args.connector)),)
    elif args.command != "profiles" and not args.source.strip():
        raise UsageError("source must not be blank")
    if getattr(args, "profiles", None):
        kwargs["profiles"] = _object(args.profiles)
    telemetry = None
    try:
        if telemetry_config is not None:
            telemetry = configure_telemetry(telemetry_config)
        with Engine(config, **kwargs) as engine:
            if args.command == "profiles":
                from .profiles import ProfileRef, load_profile
                options = dict(org_id=args.org, timeout=args.timeout)
                if args.action == "register":
                    if not args.file:
                        raise UsageError("register requires --file")
                    return {"profile": engine.register_profile(load_profile(args.file), **options)}
                if args.action == "get":
                    return {"profile": engine.get_profile(ProfileRef(args.id, args.revision), **options)}
                return engine.list_profiles(after=_object(args.after) if args.after else None, limit=args.limit, **options)
            if args.command == "prepare":
                prepared = engine.prepare(connector.name, context, journal=journal, timeout=args.timeout)
                return {"operation": "prepare", "prepared": True, "ingested": False,
                        "run_id": str(prepared.run_id), "artifact_hash": prepared.artifact_hash}
            if args.command == "replay":
                return engine.replay(prepared, journal=journal, timeout=args.timeout)
            if args.command == "rules":
                options = dict(org_id=args.org, run_id=run_id, timeout=args.timeout)
                if args.action == "learn":
                    return engine.learn_rules(args.source, auto_promote=args.auto_promote, **options)
                return engine.list_rules(args.source, status=args.status, **options)
            if args.community is not None:
                return engine.rebuild_communities(org_id=args.org, namespace=args.community, run_id=run_id)
            return engine.ingest(snapshots, org_id=args.org, run_id=run_id,
                                 timeout=args.timeout, trace_id=args.trace_id)
    finally:
        if telemetry is not None:
            telemetry.close()


def main(argv=None):
    """Console entry point. 0 complete, 3 incomplete, 2 rejected, 1 aborted."""
    run_id = uuid4()
    try:
        args = _parser().parse_args(argv)
        run_id = args.run_id or run_id
        result = _execute(args, run_id)
        status = 3 if result.get("complete") is False else 0
    except SystemExit as error:
        return error.code
    except OperationError as error:
        result, status = error.outcome, 2
    except (IngestionError, IngestionInterrupted) as error:
        result, status = dict(error.outcome), 1
        # Native request validation is an explicit before-write rejection. Never
        # relabel partial/unknown commit outcomes or interrupts as usage failures.
        if (isinstance(error, IngestionError)
                and result.get("cause") == "input_validation"
                and result.get("context", {}).get("stage") in {"request", "input_validation", "rule_admission"}
                and result.get("batches_committed") == 0
                and result.get("commit_unknown") is False
                and isinstance(result.get("committed"), dict)
                and not any(result["committed"].values())):
            status = 2
    except CollectionError as error:
        result = {"operation": "prepare", "prepared": False, "ingested": False,
                  "run_id": str(run_id), "error": "collection failed"}
        status = 1
    except JournalError as error:
        result = dict(error.outcome) if error.outcome is not None else {"run_id": error.run_id or str(run_id)}
        result["error"] = "journal failure"
        status = 1 if error.outcome is not None else 2
    except (ConfigurationError, InputValidationError, ValueError, TypeError, OSError, RecursionError) as error:
        result = {"run_id": str(run_id), "error": str(error) if isinstance(error, UsageError) else "configuration or input rejected",
                  "cause": type(error).__name__}
        status = 2
    except (Exception, KeyboardInterrupt) as error:
        # Never expose provider errors or configuration secrets on stdout/stderr.
        result = {"run_id": str(run_id), "error": "operation aborted", "cause": type(error).__name__, "commit_unknown": True}
        status = 1
    print(json.dumps(result, ensure_ascii=False, allow_nan=False))
    return status
