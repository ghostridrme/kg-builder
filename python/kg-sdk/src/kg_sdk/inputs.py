"""Input helpers preserve Rust field names and unmodified provider properties."""
from dataclasses import dataclass, field, fields
from typing import Any


@dataclass(kw_only=True)
class ConnectorEntity:
    entity_type: str
    name: str
    primary_key_properties: list[str]
    raw_properties: dict[str, Any]
    source: str
    org_id: str
    additional_key_properties: list[list[str]] = field(default_factory=list)
    namespace: str | None = None
    lifecycle: str = "active"
    labels: list[str] = field(default_factory=list)
    tags: dict[str, str] = field(default_factory=dict)

    def to_dict(self):
        return {entry.name: getattr(self, entry.name) for entry in fields(self)}


@dataclass(kw_only=True)
class SnapshotInput:
    namespace: str
    name: str
    source: str
    data_type: str
    org_id: str | None = None
    entities: list[ConnectorEntity | dict[str, Any]] = field(default_factory=list)
    content: str | None = None
    captured_at: str | None = None
    source_description: str | None = None
    entity_types: list[dict[str, Any]] | None = None
    edge_types: list[dict[str, Any]] | None = None
    edge_type_map: list[dict[str, Any]] | None = None
    thread: dict[str, Any] | None = None
    saga: dict[str, Any] | None = None
    relationship_changes: list[dict[str, Any]] = field(default_factory=list)
    previous_snapshot_uuids: list[str] = field(default_factory=list)
    tags: dict[str, str] = field(default_factory=dict)
    labels: list[str] = field(default_factory=list)
    exclude_fk_properties: list[str] = field(default_factory=list)
    ignore_change_properties: list[str] = field(default_factory=list)
    snapshot_kind: str = "incremental"
    sync_generation: int | None = None
    complete: bool = False
    collection: dict[str, Any] | None = None

    def to_dict(self):
        value = {entry.name: getattr(self, entry.name) for entry in fields(self)}
        if self.thread is not None:
            if self.saga is not None:
                raise ValueError("supply thread or legacy saga, not both")
            value.pop("saga")
        else:
            value.pop("thread")
        if type(self.entities) is not list:
            raise ValueError("entities must be a list")
        value["entities"] = [input_dict(entity) for entity in self.entities]
        return value


@dataclass(kw_only=True)
class ExistingSnapshotInput:
    namespace: str
    snapshot_uuid: str
    thread: dict[str, Any] | None = None
    saga: dict[str, Any] | None = None

    def to_dict(self):
        if (self.thread is None) == (self.saga is None):
            raise ValueError("supply exactly one thread or legacy saga")
        key, value = ("thread", self.thread) if self.thread is not None else ("saga", self.saga)
        return {"kind": "existing", "input": {"namespace": self.namespace,
                "snapshot_uuid": self.snapshot_uuid, key: value}}


def input_dict(value):
    if type(value) in (ConnectorEntity, SnapshotInput, ExistingSnapshotInput):
        return value.to_dict()
    return value
