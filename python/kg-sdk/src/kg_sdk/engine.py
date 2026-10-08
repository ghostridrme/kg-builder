"""Synchronous calls; Rust owns all graph processing and commit decisions."""
import json
import math
import os
import threading
import time
import logging
from dataclasses import dataclass
from uuid import UUID, uuid4

from . import _native
from .serialization import dumps
from .profiles import bindings as profile_bindings, reference as profile_reference
from .collection import CollectionCancellation, prepare
from .connector import CollectionContext, CollectionError, check_collection
from .inputs import input_dict
from .journal import JournalError
from .registration import configuration_fingerprint, register, storage_fingerprint

ConfigurationError = _native.ConfigurationError
InputValidationError = _native.InputValidationError
EngineClosedError = _native.EngineClosedError
ForkedEngineError = _native.ForkedEngineError
CancellationToken = _native.CancellationToken
TelemetryHandle = _native.TelemetryHandle
logger = logging.getLogger(__name__)


class OperationError(RuntimeError):
    """A registry or decision operation failed; it is not an ingestion run."""
    def __init__(self, outcome):
        self.outcome = {key: value for key, value in outcome.items() if key != "run_id"}
        super().__init__(f"KG Pipeline operation: {self.outcome.get('cause', 'failed')}")


class IngestionError(RuntimeError):
    def __init__(self, outcome):
        self.outcome = outcome
        self.run_id = outcome.get("run_id")
        super().__init__(f"KG Pipeline run {self.run_id}: {outcome.get('cause', 'incomplete')}")


class IngestionInterrupted(KeyboardInterrupt):
    def __init__(self, outcome):
        self.outcome = outcome
        self.run_id = outcome.get("run_id")
        super().__init__(f"KG Pipeline run {self.run_id} interrupted; inspect committed progress before replay")


class IngestionResult(dict):
    @property
    def complete(self):
        return self["complete"]

    @property
    def run_id(self):
        return UUID(self["run_id"])

    def require_complete(self):
        if not self.complete:
            raise IngestionError(self)
        return self


@dataclass(frozen=True)
class Limits:
    max_request_bytes: int = 64 * 1024 * 1024
    max_response_bytes: int = 64 * 1024 * 1024
    max_snapshot_bytes: int = 8 * 1024 * 1024
    max_depth: int = 64
    max_snapshots: int = 10_000
    max_entities: int = 100_000
    workers: int = 2
    admitted_calls: int = 4
    blocking_threads: int = 16

    def __post_init__(self):
        if any(type(v) is not int or v <= 0 for v in vars(self).values()):
            raise ValueError("limits must be positive integers")
        if self.max_depth > 100:
            raise ValueError("nesting exceeds Rust JSON decoder support")


class Engine:
    def __init__(self, config, *, connectors=(), ontology=None, ontology_org_id=None, limits=Limits(), profiles=None, connector_profiles=None):
        self._pid = os.getpid()
        self._limits = limits
        self._collection_condition = threading.Condition()
        self._run_slots = threading.BoundedSemaphore(limits.admitted_calls)
        self._collection_slots = threading.BoundedSemaphore(limits.admitted_calls)
        self._collections = set()
        self._closing = False
        try:
            config, self._connectors = register(config, connectors)
            self._profiles = profile_bindings(profiles)
            self._ontology_org = ontology_org_id
            for name, ref in (connector_profiles or {}).items():
                if name not in self._connectors:
                    raise ConfigurationError("profile connector is not registered")
                source = self._connectors[name][0]
                ref = profile_reference(ref)
                if source in self._profiles and self._profiles[source] != ref:
                    raise ConfigurationError("conflicting profiles for the same source; use separate runs")
                self._profiles[source] = ref
            self._fingerprint = configuration_fingerprint(config, ontology, ontology_org_id)
            self._storage_fingerprint = storage_fingerprint(config)
            configuration = self._encode(config)
            ontology_json = None if ontology is None else self._encode(ontology)
        except ConfigurationError:
            raise
        except (ValueError, TypeError) as error:
            raise ConfigurationError("configuration is not bounded JSON") from error
        self._engine = _native.NativeEngine(configuration, ontology_json, ontology_org_id,
                                             limits.workers, limits.admitted_calls, limits.blocking_threads, limits.max_response_bytes, limits.max_request_bytes)

    def _encode(self, value, max_bytes=None):
        return dumps(value, max_bytes=max_bytes or self._limits.max_request_bytes,
                     max_depth=self._limits.max_depth)

    @staticmethod
    def _not_started(run_id, *, interrupted=False):
        outcome = {"run_id": str(run_id), "cause": "cancelled", "committed": {},
                   "batches_committed": 0, "commit_unknown": False, "retriable": False}
        return IngestionInterrupted(outcome) if interrupted else IngestionError(outcome)

    def _run(self, operation, value, *, org_id, run_id=None, cancellation=None, timeout=None, trace_id=None):
        self._check_open()
        run_id = uuid4() if run_id is None else run_id
        if type(run_id) is not UUID or run_id.int == 0:
            error = InputValidationError("run_id must be a non-nil UUID")
            error.run_id = str(run_id)
            raise error
        if timeout is not None and (type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0):
            error = InputValidationError("timeout must be positive and finite")
            error.run_id = str(run_id)
            raise error
        deadline = None if timeout is None else time.monotonic() + timeout
        acquired = False
        try:
            try:
                while not acquired:
                    self._check_open()
                    if ((cancellation is not None and cancellation.cancelled)
                            or (deadline is not None and time.monotonic() >= deadline)):
                        raise self._not_started(run_id)
                    wait = 0.05 if deadline is None else min(0.05, max(0, deadline - time.monotonic()))
                    acquired = self._run_slots.acquire(timeout=wait)
                self._check_open()
            except KeyboardInterrupt as error:
                raise self._not_started(run_id, interrupted=True) from error
            return self._run_admitted(operation, value, org_id=org_id, run_id=run_id,
                                      cancellation=cancellation, deadline=deadline, trace_id=trace_id)
        finally:
            if acquired:
                self._run_slots.release()

    def _run_admitted(self, operation, value, *, org_id, run_id, cancellation=None, deadline=None, trace_id=None):
        timeout = None
        try:
            if type(org_id) is not str or not org_id.strip():
                raise ValueError("org_id is required")
            bound = value if operation == "ingest_profiles" else None
            if bound is not None:
                value = bound["inputs"]
            if operation in ("ingest", "ingest_profiles"):
                if type(value) not in (list, tuple) or len(value) > self._limits.max_snapshots:
                    raise ValueError("snapshots must be a bounded list or tuple")
                inputs = []
                entities = 0
                for snapshot in value:
                    snapshot = input_dict(snapshot)
                    if type(snapshot) is not dict:
                        raise ValueError("snapshot must be an object")
                    envelope = snapshot if "kind" in snapshot else {"kind": "fresh", "input": snapshot}
                    self._encode(envelope, self._limits.max_snapshot_bytes)
                    body = envelope.get("input", {})
                    if type(body) is not dict:
                        raise ValueError("snapshot input must be an object")
                    rows = body.get("entities", [])
                    if type(rows) is not list:
                        raise ValueError("entities must be a list")
                    entities += len(rows)
                    if entities > self._limits.max_entities:
                        raise ValueError("entity count limit exceeded")
                    inputs.append(envelope)
                value = inputs if bound is None else {**bound, "inputs": inputs}
            encoded = self._encode(value)
            if deadline is not None:
                timeout = deadline - time.monotonic()
            if ((cancellation is not None and cancellation.cancelled)
                    or (timeout is not None and timeout <= 0)):
                raise self._not_started(run_id)
            response = json.loads(self._engine.run(operation, encoded, org_id, str(run_id), cancellation, timeout, trace_id))
        except (ValueError, TypeError) as error:
            if hasattr(error, "outcome_json"):
                raise
            failure = InputValidationError(str(error))
            failure.run_id = str(run_id)
            raise failure from error
        outcome = response["result"]
        if response.get("interrupted"):
            raise IngestionInterrupted(outcome)
        if not response["ok"]:
            raise IngestionError(outcome)
        return IngestionResult(outcome)

    def ingest(self, snapshots, *, org_id, run_id=None, cancellation=None, timeout=None, trace_id=None, profiles=None, _expected_profiles=None):
        self._check_open()
        selected = profile_bindings(self._profiles if profiles is None else profiles)
        operation = "ingest_profiles" if selected or _expected_profiles is not None else "ingest"
        payload = {"inputs": snapshots, "bindings": selected, "expected": _expected_profiles} if operation == "ingest_profiles" else snapshots
        return self._run(operation, payload, org_id=org_id, run_id=run_id,
                         cancellation=cancellation, timeout=timeout, trace_id=trace_id)

    def _control(self, operation, payload, *, org_id, timeout=None, cancellation=None):
        try:
            result = self._run(operation, payload, org_id=org_id, timeout=timeout, cancellation=cancellation)
            return {key: value for key, value in result.items() if key != "run_id"}
        except IngestionError as error:
            raise OperationError(error.outcome) from None

    def register_profile(self, document, *, org_id, timeout=None, cancellation=None):
        return self._control("profiles", {"action": "put", "document": document}, org_id=org_id,
                             timeout=timeout, cancellation=cancellation)["profile"]

    def get_profile(self, profile, *, org_id, timeout=None, cancellation=None):
        return self._control("profiles", {"action": "get", "reference": profile_reference(profile)},
                             org_id=org_id, timeout=timeout, cancellation=cancellation)["profile"]

    def list_profiles(self, *, org_id, after=None, limit=20, timeout=None, cancellation=None):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("profile page limit must be 1..100")
        return self._control("profiles", {"action": "list", "after": None if after is None else profile_reference(after), "limit": limit},
                             org_id=org_id, timeout=timeout, cancellation=cancellation)["page"]

    def checkpoint_store(self, *, org_id, namespace):
        self._check_open()
        from .checkpoints import Neo4jCheckpointStore
        return Neo4jCheckpointStore(self, org_id=org_id, namespace=namespace)

    def _checkpoint_control(self, payload, *, org_id, timeout=None, cancellation=None):
        return self._control("checkpoints", payload, org_id=org_id, timeout=timeout, cancellation=cancellation)["state"]

    def _prepare_profiles(self, bindings, *, org_id, timeout, cancellation=None):
        return self._control("profiles", {"action": "prepare", "bindings": bindings},
                             org_id=org_id, timeout=timeout, cancellation=cancellation)["manifest"]

    def decide(self, state, questions, *, timeout=None):
        return self._control("decide", {"state": state, "questions": questions},
                             org_id=self._ontology_org or "decision", timeout=timeout)

    def _check_open(self):
        if os.getpid() != self._pid:
            raise ForkedEngineError("create a new engine after fork")
        with self._collection_condition:
            if self._closing:
                raise EngineClosedError("engine is closing")

    def prepare(self, connector, context, *, journal, cancellation=None, timeout=None, checkpoint_store=None):
        self._check_open()
        if type(context) is not CollectionContext:
            raise ValueError("context must be CollectionContext")
        if timeout is not None and (type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0):
            raise ValueError("timeout must be positive and finite")
        deadline = time.monotonic() + min(context.limits.timeout, timeout or context.limits.timeout)
        closing = CancellationToken()
        token = CollectionCancellation(cancellation, closing)
        with self._collection_condition:
            if self._closing:
                raise EngineClosedError("engine is closing")
            self._collections.add(closing)
        acquired = False
        try:
            while not acquired:
                check_collection(token, deadline)
                acquired = self._collection_slots.acquire(timeout=min(0.05, max(0, deadline - time.monotonic())))
            started = time.monotonic()
            prepared = prepare(self, connector, context, journal, token, deadline, checkpoint_store)
            logger.info("collection prepared", extra={"run_id": str(prepared.run_id), "connector": connector,
                        "duration_ms": int((time.monotonic() - started) * 1000)})
            return prepared
        except CollectionError as error:
            error.run_id = str(context.run_id)
            if not acquired:
                journal._failure(context.run_id, self._connectors.get(connector, ("unknown",))[0], error.reason)
            logger.warning("collection failed", extra={"run_id": str(context.run_id), "reason": error.reason})
            raise
        finally:
            if acquired:
                self._collection_slots.release()
            with self._collection_condition:
                self._collections.remove(closing)
                self._collection_condition.notify_all()

    def prepare_thread_recovery(self, original, *, journal, run_id=None):
        return self.prepare_saga_recovery(original, journal=journal, run_id=run_id)

    def prepare_saga_recovery(self, original, *, journal, run_id=None):
        self._check_open()
        from .recovery import prepare_sagas
        return prepare_sagas(self, original, journal, run_id)

    def replay(self, prepared, *, journal, cancellation=None, timeout=None):
        self._check_open()
        body = journal._body(prepared)
        if body["configuration_fingerprint"] != self._fingerprint:
            raise JournalError("engine configuration differs; changed processing requires a new run",
                               run_id=prepared.run_id)
        if timeout is not None and (type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0):
            raise ValueError("timeout must be positive and finite")
        if body.get("operation") == "recover_sagas":
            from .recovery import replay_sagas
            return replay_sagas(self, prepared, journal, cancellation, timeout)
        try:
            result = self.ingest(json.loads(body["inputs_json"]), org_id=body["org_id"], run_id=prepared.run_id,
                                 cancellation=cancellation, timeout=timeout, profiles=body.get("profiles", {}),
                                 _expected_profiles=body.get("profile_manifest"))
        except (IngestionError, IngestionInterrupted) as error:
            self._save_outcome(journal, prepared, error.outcome, success=False)
            raise
        self._save_outcome(journal, prepared, result, success=True)
        logger.info("prepared ingestion finished", extra={"run_id": str(prepared.run_id), "complete": result.complete})
        return result

    @staticmethod
    def _save_outcome(journal, prepared, outcome, *, success):
        try:
            journal._record(prepared, outcome, success=success)
        except KeyboardInterrupt:
            raise IngestionInterrupted(dict(outcome)) from None

    def learn_rules(self, source, *, org_id, auto_promote=False, run_id=None, cancellation=None, timeout=None):
        self._check_open()
        if type(source) is not str or not source.strip() or type(auto_promote) is not bool:
            raise InputValidationError("source and boolean auto_promote are required")
        return self._run("rules", {"action": "learn", "source": source, "auto_promote": auto_promote},
            org_id=org_id, run_id=run_id, cancellation=cancellation, timeout=timeout)

    def list_rules(self, source, *, org_id, status=None, run_id=None, cancellation=None, timeout=None):
        self._check_open()
        if type(source) is not str or not source.strip() or status not in (None, "proposed", "active", "rejected", "uncertain", "stale", "revoked"):
            raise InputValidationError("invalid rule source or status")
        return self._run("rules", {"action": "list", "source": source, "status": status},
            org_id=org_id, run_id=run_id, cancellation=cancellation, timeout=timeout)

    def rebuild_communities(self, *, org_id, namespace, run_id=None, cancellation=None, timeout=None):
        return self._run("community", namespace, org_id=org_id, run_id=run_id, cancellation=cancellation, timeout=timeout)

    def summarize_thread(self, *, org_id, namespace, thread, run_id=None, cancellation=None, timeout=None):
        return self._run("saga", {"namespace": namespace, "saga": thread}, org_id=org_id,
                         run_id=run_id, cancellation=cancellation, timeout=timeout)

    def summarize_saga(self, *, org_id, namespace, saga, run_id=None, cancellation=None, timeout=None):
        return self._run("saga", {"namespace": namespace, "saga": saga}, org_id=org_id,
                         run_id=run_id, cancellation=cancellation, timeout=timeout)

    def close(self):
        if os.getpid() != self._pid:
            raise ForkedEngineError("create a new engine after fork")
        with self._collection_condition:
            self._closing = True
            for token in self._collections:
                token.cancel()
        self._engine.close()
        with self._collection_condition:
            self._collection_condition.wait_for(lambda: not self._collections)

    def __enter__(self):
        if os.getpid() != self._pid:
            raise ForkedEngineError("create a new engine after fork")
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


def configure_telemetry(config):
    """Explicit process-wide subscriber installation; close the returned handle."""
    return _native.TelemetryHandle(dumps(config))
