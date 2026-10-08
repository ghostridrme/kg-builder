"""Shared Neo4j checkpoints, reached only through the SDK's Rust engine boundary."""
from dataclasses import replace


class Neo4jCheckpointStore:
    """Use with Engine.prepare(checkpoint_store=...) and RunJournal.acknowledge.

    Reads return both the provider cursor and its revision. The journal keeps that
    revision across restarts; equality of cursor values alone cannot fence writers.
    """
    def __init__(self, engine, *, org_id, namespace):
        if not isinstance(org_id, str) or not org_id.strip() or not isinstance(namespace, str) or not namespace.strip():
            raise ValueError("checkpoint organization and namespace are required")
        self._engine, self.org_id, self.namespace = engine, org_id, namespace

    def _validate_scope(self, org_id, namespace):
        if (org_id, namespace) != (self.org_id, self.namespace):
            raise ValueError("checkpoint store scope differs from collection")

    def read(self, key, *, timeout=None, cancellation=None):
        return self._engine._checkpoint_control(
            {"action": "get", "scope": {"namespace": self.namespace, "key": key}},
            org_id=self.org_id, timeout=timeout, cancellation=cancellation)

    def _bind(self, context, key, *, timeout, cancellation):
        self._validate_scope(context.org_id, context.namespace)
        if context.previous_cursor is not None or context.checkpoint_revision is not None:
            raise ValueError("shared checkpoints supply the previous cursor and revision")
        state = self.read(key, timeout=timeout, cancellation=cancellation)
        return replace(context, previous_cursor=state["cursor"], checkpoint_revision=state["revision"])

    def compare_and_set(self, key, *, expected, proposed, run_id, expected_revision=None):
        if type(expected_revision) is not int or not 0 <= expected_revision < 2**63 - 1:
            raise ValueError("a journaled checkpoint revision is required")
        self._engine._checkpoint_control({"action": "advance", "request": {
            "scope": {"namespace": self.namespace, "key": key},
            "expected": {"revision": expected_revision, "cursor": expected},
            "proposed": proposed, "run_id": str(run_id),
        }}, org_id=self.org_id)
