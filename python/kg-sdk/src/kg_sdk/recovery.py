"""Recover committed Saga follow-ups without recreating source observations."""
from uuid import UUID, uuid4, uuid5

from .connector import digest
from .journal import JournalError


def saga_obligations(journal, prepared):
    body = journal._body(prepared)
    if body.get("operation", "ingest") != "ingest" or body.get("collection_complete") is not True:
        raise JournalError("recovery requires a complete original source collection", run_id=prepared.run_id)
    record = journal._read(journal._path(prepared.run_id, "settled-incomplete"))
    outcome = record.get("outcome", {})
    if (record.get("artifact_hash") != prepared.artifact_hash or record.get("success") is not True
            or outcome.get("run_id") != str(prepared.run_id) or outcome.get("complete") is not False
            or outcome.get("commit_unknown", False) is not False
            or type(outcome.get("snapshots_total")) is not int
            or outcome["snapshots_total"] != outcome.get("snapshots_completed")
            or any(outcome.get(key) != [] for key in ("failed_snapshots", "incomplete_extractions", "skipped_summaries"))):
        raise JournalError("only settled Saga-only incompleteness is recoverable here", run_id=prepared.run_id)
    followups = outcome.get("incomplete_followups")
    if type(followups) is not list or not followups:
        raise JournalError("original run has no recoverable Saga follow-ups", run_id=prepared.run_id)
    targets, seen = [], set()
    for item in followups:
        try:
            saga = UUID(item["saga_uuid"])
            ordinal = item["from_ordinal"]
            if (saga.int == 0 or item["namespace"] != body["namespace"] or type(ordinal) is not int
                    or ordinal < 1 or str(saga) in seen):
                raise ValueError
        except (KeyError, TypeError, ValueError, AttributeError):
            raise JournalError("invalid original Saga obligations", run_id=prepared.run_id) from None
        seen.add(str(saga))
        targets.append({"namespace": item["namespace"], "saga_uuid": str(saga), "from_ordinal": ordinal})
    return body, record, sorted(targets, key=lambda item: item["saga_uuid"])


def prepare_sagas(engine, original, journal, run_id):
    body, record, targets = saga_obligations(journal, original)
    if not body.get("storage_fingerprint") or body["storage_fingerprint"] != engine._storage_fingerprint:
        raise JournalError("Saga recovery must use the original database", run_id=original.run_id)
    run_id = uuid4() if run_id is None else run_id
    journal._reserve(run_id)
    return journal._prepare(run_id, {
        "protocol": 1, "operation": "recover_sagas", "run_id": str(run_id),
        "org_id": body["org_id"], "namespace": body["namespace"],
        "configuration_fingerprint": engine._fingerprint, "storage_fingerprint": engine._storage_fingerprint,
        "original_run_id": str(original.run_id), "original_artifact_hash": original.artifact_hash,
        "original_outcome_hash": digest(record), "targets": targets,
    })


def verify_link(journal, recovery, original):
    body = journal._body(recovery)
    original_body, record, targets = saga_obligations(journal, original)
    if (body.get("operation") != "recover_sagas" or body.get("original_run_id") != str(original.run_id)
            or body.get("original_artifact_hash") != original.artifact_hash
            or body.get("original_outcome_hash") != digest(record) or body.get("targets") != targets
            or body.get("org_id") != original_body["org_id"] or body.get("namespace") != original_body["namespace"]
            or not body.get("storage_fingerprint") or body["storage_fingerprint"] != original_body.get("storage_fingerprint")):
        raise JournalError("recovery lineage or obligations differ", run_id=recovery.run_id)
    return body


def replay_sagas(engine, prepared, journal, cancellation, timeout):
    import time
    from .engine import IngestionError, IngestionInterrupted, IngestionResult
    body = journal._body(prepared)
    original = journal.load(UUID(body["original_run_id"]))
    body = verify_link(journal, prepared, original)
    started = time.monotonic()
    outcomes = []
    try:
        for target in body["targets"]:
            remaining = None if timeout is None else timeout - (time.monotonic() - started)
            if remaining is not None and remaining <= 0:
                raise IngestionError({"run_id": str(prepared.run_id), "cause": "recovery_deadline", "commit_unknown": False})
            child_id = uuid5(prepared.run_id, target["namespace"] + "/" + target["saga_uuid"])
            result = engine._run("saga", {"namespace": target["namespace"], "saga": {"kind": "uuid", "uuid": target["saga_uuid"]}},
                org_id=body["org_id"], run_id=child_id, cancellation=cancellation, timeout=remaining)
            outcomes.append({"saga_uuid": target["saga_uuid"], "outcome": dict(result)})
        result = IngestionResult(run_id=str(prepared.run_id), original_run_id=str(original.run_id),
            complete=all(item["outcome"].get("complete") is True for item in outcomes),
            saga_recoveries=outcomes)
        engine._save_outcome(journal, prepared, result, success=True)
        return result
    except (IngestionError, IngestionInterrupted) as error:
        outcome = {"run_id": str(prepared.run_id), "original_run_id": str(original.run_id),
            "complete": False, "cause": error.outcome.get("cause", "incomplete"),
            "commit_unknown": error.outcome.get("commit_unknown", False),
            "saga_recoveries": outcomes, "failed_recovery": error.outcome}
        engine._save_outcome(journal, prepared, outcome, success=False)
        raise type(error)(outcome) from None
    except KeyboardInterrupt:
        outcome = {"run_id": str(prepared.run_id), "original_run_id": str(original.run_id),
            "complete": False, "cause": "interrupted", "commit_unknown": True, "saga_recoveries": outcomes}
        engine._save_outcome(journal, prepared, outcome, success=False)
        raise IngestionInterrupted(outcome) from None
    except JournalError:
        # A journal failure (for example while persisting the success outcome
        # after every saga has already committed) is not an uncertain commit.
        # Let it propagate as Engine.replay does, rather than recording a
        # commit_unknown host-interruption outcome that misreports certainty.
        raise
    except Exception as error:
        outcome = {"run_id": str(prepared.run_id), "original_run_id": str(original.run_id),
            "complete": False, "cause": "recovery_interrupted_by_host", "commit_unknown": True,
            "saga_recoveries": outcomes}
        engine._save_outcome(journal, prepared, outcome, success=False)
        raise IngestionError(outcome) from error


def verify_completion(body, completion):
    results = completion.get("outcome", {}).get("saga_recoveries")
    if type(results) is not list or len(results) != len(body["targets"]):
        raise JournalError("recovery completion is missing required Sagas")
    for target, result in zip(body["targets"], results):
        child = uuid5(UUID(body["run_id"]), target["namespace"] + "/" + target["saga_uuid"])
        outcome = result.get("outcome", {}) if type(result) is dict else {}
        if (type(result) is not dict or result.get("saga_uuid") != target["saga_uuid"]
                or outcome.get("run_id") != str(child) or outcome.get("complete") is not True
                or outcome.get("commit_unknown", False) is not False):
            raise JournalError("recovery completion differs from its obligations")
