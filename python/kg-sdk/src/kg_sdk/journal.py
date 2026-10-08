"""Durable local replay artifacts with explicit, completion-gated cursor acknowledgment."""
import hashlib
import os
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID, uuid4

from .serialization import dumps, loads


class JournalError(RuntimeError):
    def __init__(self, reason, *, run_id=None, outcome=None):
        self.reason = reason
        self.run_id = str(run_id) if run_id else None
        self.outcome = outcome
        super().__init__(f"replay journal: {reason}")


class CheckpointStore(Protocol):
    """CAS must be atomic, durable and idempotent for the same run and proposal."""
    def compare_and_set(self, key: str, *, expected: Any, proposed: Any, run_id: UUID) -> None: ...


@dataclass(frozen=True)
class PreparedRun:
    run_id: UUID
    artifact_hash: str


class RunJournal:
    def __init__(self, directory: Path, *, max_bytes=64 * 1024 * 1024):
        if type(max_bytes) is not int or max_bytes <= 0:
            raise ValueError("journal max_bytes must be a positive integer")
        if os.name != "posix":
            raise JournalError("durable journal requires POSIX file and directory fsync")
        self.directory = Path(directory)
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = self.directory.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise JournalError("directory must be owner-only and not a symlink")
        self.max_bytes = max_bytes

    def _path(self, run_id, suffix):
        if type(run_id) is not UUID or run_id.int == 0:
            raise JournalError("run_id must be a non-nil UUID")
        return self.directory / f"{run_id}.{suffix}.json"

    def _read(self, path):
        try:
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, "rb") as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
                    raise JournalError("artifact must be an owner-only regular file")
                if info.st_size > self.max_bytes:
                    raise JournalError("artifact exceeds byte limit")
                raw = stream.read(self.max_bytes + 1)
                if len(raw) > self.max_bytes:
                    raise JournalError("artifact exceeds byte limit")
                return loads(raw, max_bytes=self.max_bytes)
        except (OSError, ValueError, RecursionError) as error:
            raise JournalError("artifact missing or invalid") from error

    def _publish(self, path, value):
        raw = dumps(value, max_bytes=self.max_bytes).encode()
        temporary = None
        try:
            fd, temporary = tempfile.mkstemp(prefix=".pending-", dir=self.directory)
            with os.fdopen(fd, "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            # Linking publishes atomically without replacing a different artifact.
            os.link(temporary, path)
            self._sync_directory()
        finally:
            if temporary is not None:
                os.unlink(temporary)

    def _sync_directory(self):
        fd = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _prepare(self, run_id, body):
        encoded = dumps(body, max_bytes=self.max_bytes)
        checksum = hashlib.sha256(encoded.encode()).hexdigest()
        try:
            self._publish(self._path(run_id, "input"), {
                "format_version": 1, "run_id": str(run_id), "sha256": checksum, "body_json": encoded,
            })
        except FileExistsError as error:
            raise JournalError("run already prepared; load and replay without recollecting", run_id=run_id) from error
        except (OSError, ValueError) as error:
            raise JournalError("could not persist prepared input", run_id=run_id) from error
        return PreparedRun(run_id, checksum)

    def _reserve(self, run_id):
        try:
            if self._path(run_id, "input").exists():
                raise FileExistsError
            self._publish(self._path(run_id, "collection"), {"run_id": str(run_id)})
        except FileExistsError as error:
            raise JournalError("run already collected or reserved; replay it or collect under a new run_id",
                               run_id=run_id) from error
        except OSError as error:
            raise JournalError("could not reserve collection", run_id=run_id) from error

    def load(self, run_id):
        artifact, _ = self._load(run_id)
        return PreparedRun(run_id, artifact["sha256"])

    def _load(self, run_id):
        artifact = self._read(self._path(run_id, "input"))
        try:
            encoded = artifact["body_json"]
            if (artifact["format_version"] != 1 or artifact["run_id"] != str(run_id)
                    or hashlib.sha256(encoded.encode()).hexdigest() != artifact["sha256"]):
                raise ValueError("invalid artifact")
            body = loads(encoded, max_bytes=self.max_bytes)
            if body["protocol"] != 1 or body["run_id"] != str(run_id):
                raise ValueError("invalid protocol")
            if "profile_format" in body and (body["profile_format"] != 1 or type(body.get("profiles")) is not dict or type(body.get("profile_manifest")) is not dict):
                raise ValueError("invalid profile journal contract")
            if "profile_format" not in body and ("profiles" in body or "profile_manifest" in body):
                raise ValueError("unversioned profile contract")
            return artifact, body
        except (KeyError, TypeError, ValueError, AttributeError) as error:
            raise JournalError("artifact integrity or protocol mismatch", run_id=run_id) from error

    def _body(self, prepared):
        if type(prepared) is not PreparedRun:
            raise JournalError("expected PreparedRun")
        artifact, body = self._load(prepared.run_id)
        if artifact["sha256"] != prepared.artifact_hash:
            raise JournalError("prepared artifact changed", run_id=prepared.run_id)
        return body

    def _record(self, prepared, outcome, *, success):
        if outcome.get("run_id") != str(prepared.run_id):
            raise JournalError("outcome run mismatch", run_id=prepared.run_id, outcome=outcome)
        record = {"artifact_hash": prepared.artifact_hash, "outcome": dict(outcome), "success": success}
        try:
            self._publish(self._path(prepared.run_id, f"result-{uuid4()}"), record)
            if success and outcome.get("complete") is False:
                try:
                    self._publish(self._path(prepared.run_id, "settled-incomplete"), record)
                except FileExistsError:
                    previous = self._read(self._path(prepared.run_id, "settled-incomplete"))
                    if previous.get("artifact_hash") != prepared.artifact_hash:
                        raise JournalError("incomplete artifact mismatch")
                    self._sync_directory()
            if success and outcome.get("complete") is True:
                try:
                    self._publish(self._path(prepared.run_id, "complete"), record)
                except FileExistsError:
                    previous = self._read(self._path(prepared.run_id, "complete"))
                    if previous["artifact_hash"] != prepared.artifact_hash:
                        raise JournalError("completion artifact mismatch")
                    self._sync_directory()
        except (OSError, ValueError, JournalError) as error:
            raise JournalError("could not persist outcome; replay saved input before acknowledgment",
                               run_id=prepared.run_id, outcome=outcome) from error

    def _failure(self, run_id, source, reason):
        try:
            self._publish(self._path(run_id, f"collection-failure-{uuid4()}"),
                          {"run_id": str(run_id), "source": source, "reason": reason})
        except (OSError, ValueError) as error:
            raise JournalError("could not persist collection failure", run_id=run_id) from error

    def acknowledge(self, run_id, *, checkpoint_store: CheckpointStore, recovery_run_id=None):
        prepared = self.load(run_id)
        body = self._body(prepared)
        if body.get("collection_complete") is not True:
            raise JournalError("complete source collection required before acknowledgment", run_id=run_id)
        evidence = prepared
        if recovery_run_id is not None:
            from .recovery import verify_link
            evidence = self.load(recovery_run_id)
            recovery_body = verify_link(self, evidence, prepared)
        completion = self._read(self._path(evidence.run_id, "complete"))
        if (completion.get("artifact_hash") != evidence.artifact_hash or completion.get("success") is not True
                or completion.get("outcome", {}).get("run_id") != str(evidence.run_id)
                or completion["outcome"].get("complete") is not True):
            raise JournalError("complete ingestion evidence required", run_id=run_id)
        if recovery_run_id is not None:
            from .recovery import verify_completion
            verify_completion(recovery_body, completion)
        from .checkpoints import Neo4jCheckpointStore
        revision = body.get("checkpoint_revision")
        if isinstance(checkpoint_store, Neo4jCheckpointStore):
            checkpoint_store._validate_scope(body["org_id"], body["namespace"])
            if body["storage_fingerprint"] != checkpoint_store._engine._storage_fingerprint:
                raise JournalError("checkpoint database differs from prepared ingestion", run_id=run_id)
            if type(revision) is not int or not 0 <= revision < 2**63 - 1:
                raise JournalError("shared checkpoint requires a journaled revision", run_id=run_id)
            checkpoint_store.compare_and_set(body["checkpoint_key"], expected=body["previous_cursor"],
                proposed=body["proposed_cursor"], run_id=run_id, expected_revision=revision)
        else:
            if revision is not None:
                raise JournalError("revision-fenced collection requires the shared checkpoint store", run_id=run_id)
            checkpoint_store.compare_and_set(body["checkpoint_key"], expected=body["previous_cursor"],
                                             proposed=body["proposed_cursor"], run_id=run_id)
