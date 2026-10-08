"""Prepare a bounded source observation without writing graph data."""
from dataclasses import replace
from datetime import datetime, timezone
import time

from .serialization import dumps, loads
from .connector import (
    CollectedBatch, CollectionContext, CollectionError, canonical, check_collection,
)
from .inputs import input_dict
from .connector import connector_key


class CollectionCancellation:
    def __init__(self, caller, closing):
        self.caller, self.closing = caller, closing

    @property
    def cancelled(self):
        return self.closing.cancelled or (self.caller is not None and self.caller.cancelled)


def prepare(engine, name, context, journal, cancellation, deadline, checkpoint_store=None):
    if type(context) is not CollectionContext:
        raise ValueError("context must be CollectionContext")
    if name not in engine._connectors:
        raise ValueError("connector is not registered")
    context = replace(context)  # Detach selection and cursor from the caller's mutable mappings.
    source, connector = engine._connectors[name]
    if checkpoint_store is not None:
        from .checkpoints import Neo4jCheckpointStore
        if not isinstance(checkpoint_store, Neo4jCheckpointStore) or checkpoint_store._engine is not engine:
            raise ValueError("prepare requires a checkpoint store from this engine")
        check_collection(cancellation, deadline)
        context = checkpoint_store._bind(context, connector_key(connector, context),
            timeout=max(0.001, deadline-time.monotonic()), cancellation=getattr(cancellation, "caller", None))
        check_collection(cancellation, deadline)
    original_selection = canonical(context.selection)
    original_cursor = canonical(context.previous_cursor)
    original_revision = context.checkpoint_revision
    # Resolve before collecting. The registry and host policy are authoritative.
    selected = dict(engine._profiles)
    manifest = None
    if selected:
        check_collection(cancellation, deadline)
        manifest = engine._prepare_profiles(selected, org_id=context.org_id, timeout=max(0.001, deadline-time.monotonic()), cancellation=getattr(cancellation, "caller", None))
        check_collection(cancellation, deadline)
    journal._reserve(context.run_id)
    try:
        key = connector_key(connector, context)
        check_collection(cancellation, deadline)
        batch = connector.collect(context, cancellation=cancellation, deadline=deadline)
        check_collection(cancellation, deadline)
        if (canonical(context.selection) != original_selection or connector_key(connector, context) != key
                or canonical(context.previous_cursor) != original_cursor
                or context.checkpoint_revision != original_revision):
            raise CollectionError("connector_changed_scope")
        if type(batch) is not CollectedBatch or type(batch.snapshots) is not list or type(batch.complete) is not bool:
            raise CollectionError("invalid_batch")
        snapshots = []
        if len(batch.snapshots) > engine._limits.max_snapshots:
            raise CollectionError("snapshot_limit")
        now = datetime.now(timezone.utc).isoformat()
        entities = 0
        provider_entities = 0
        for value in batch.snapshots:
            snapshot = loads(dumps(input_dict(value), max_bytes=engine._limits.max_snapshot_bytes))
            if type(snapshot) is not dict or "kind" in snapshot:
                raise CollectionError("connector_requires_fresh_snapshots")
            if (snapshot.get("source") != source or snapshot.get("org_id") != context.org_id
                    or snapshot.get("namespace") != context.namespace):
                raise CollectionError("snapshot_scope")
            if snapshot.get("relationship_changes"):
                raise CollectionError("connector_supplied_relationships")
            rows = snapshot.get("entities", [])
            if type(rows) is not list:
                raise CollectionError("invalid_entities")
            entities += len(rows)
            # Scan-scope observations (namespace, account, region...) carry the
            # astrolabe:scope label and do not count against the provider record limit.
            provider_entities += sum(
                type(entity) is dict and "astrolabe:scope" not in (entity.get("labels") or [])
                for entity in rows
            )
            if (entities > engine._limits.max_entities
                    or provider_entities > context.limits.max_records):
                raise CollectionError("entity_limit")
            for entity in rows:
                namespace_node = (type(entity) is dict
                    and entity.get("entity_type") == "Astrolabe::Namespace"
                    and entity.get("source") == "kg"
                    and entity.get("name") == context.namespace
                    and entity.get("primary_key_properties") == ["name"]
                    and entity.get("raw_properties") == {}
                    and entity.get("labels") == ["astrolabe:scope"])
                if (type(entity) is not dict or (entity.get("source") != source and not namespace_node)
                        or entity.get("org_id") != context.org_id
                        or entity.get("namespace") not in (None, context.namespace)):
                    raise CollectionError("entity_scope")
            authority = snapshot.get("collection")
            if authority is not None:
                if (not batch.complete or batch.diagnostics or context.generation is None
                        or snapshot.get("snapshot_kind") != "full" or snapshot.get("complete") is not True
                        or snapshot.get("sync_generation") != context.generation
                        or type(authority) is not dict or authority.get("key") != key
                        or authority.get("relationships_complete", False) is not False):
                    raise CollectionError("invalid_collection_authority")
            if snapshot.get("captured_at") is None:
                snapshot["captured_at"] = now
            snapshots.append({"kind": "fresh", "input": snapshot})
        if context.generation is not None and (not snapshots or any(s["input"].get("collection") is None for s in snapshots)):
            raise CollectionError("full_collection_incomplete")
        encoded = dumps(snapshots, max_bytes=engine._limits.max_request_bytes)
        check_collection(cancellation, deadline)
        profile_fields = {}
        if manifest is not None:
            sources = {source} | {entity["source"] for item in snapshots for entity in item["input"].get("entities", [])}
            selected = {s: ref for s, ref in selected.items() if s in sources}
            manifest = {**manifest, "profiles": {s: p for s, p in manifest["profiles"].items() if s in selected},
                        "sources": {s: p for s, p in manifest["sources"].items() if s in selected}}
            if selected:
                profile_fields = {"profile_format": 1, "profiles": selected, "profile_manifest": manifest}
        return journal._prepare(context.run_id, {
            **profile_fields,
            **({"checkpoint_revision": context.checkpoint_revision} if context.checkpoint_revision is not None else {}),
            "protocol": 1, "run_id": str(context.run_id), "org_id": context.org_id,
            "namespace": context.namespace, "source": source, "connector": name,
            "selection": context.selection, "checkpoint_key": key,
            "configuration_fingerprint": engine._fingerprint,
            "storage_fingerprint": engine._storage_fingerprint,
            "inputs_json": encoded, "original_envelopes": batch.original_envelopes,
            "previous_cursor": context.previous_cursor, "proposed_cursor": batch.proposed_cursor,
            "diagnostics": batch.diagnostics, "collection_complete": batch.complete,
        })
    except CollectionError as error:
        error.run_id = str(context.run_id)
        journal._failure(context.run_id, source, error.reason)
        raise
    except (ValueError, TypeError, RecursionError) as error:
        journal._failure(context.run_id, source, "invalid_source_data")
        failure = CollectionError("invalid_source_data")
        failure.run_id = str(context.run_id)
        raise failure from error
    except KeyboardInterrupt:
        journal._failure(context.run_id, source, "interrupted")
        raise
