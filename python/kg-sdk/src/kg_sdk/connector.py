"""Provider collection contracts; graph processing remains in Rust."""
import hashlib
import json
import math
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID, uuid4

from .serialization import dumps, loads


class CollectionError(RuntimeError):
    def __init__(self, reason: str):
        self.reason = reason
        self.run_id = None
        super().__init__(f"collection failed: {reason}")


class ProviderError(CollectionError):
    """Sanitized status/code for operation-specific absence handling."""
    def __init__(self, status, code):
        super().__init__("provider_access_denied" if status in (401, 403) else "provider_failure")
        self.status = status
        self.code = code if type(code) is str and len(code) <= 128 and all(c.isascii() and (c.isalnum() or c in "._-") for c in code) else "unknown"


def canonical(value):
    # Validate before json.dumps so non-string keys and lossy values cannot sneak in.
    dumps(value)
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def require_text(value, field_name):
    if type(value) is not str or not value.strip():
        raise ValueError(f"{field_name} must be nonblank text")


@dataclass(frozen=True, kw_only=True)
class CollectionLimits:
    max_pages: int = 1_000
    max_bytes: int = 64 * 1024 * 1024
    max_records: int = 100_000
    timeout: float = 300
    attempts: int = 3
    connect_timeout: float = 5
    read_timeout: float = 30

    def __post_init__(self):
        for name in ("max_pages", "max_bytes", "max_records", "attempts"):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise ValueError(f"{name} must be a positive integer")
        for name in ("timeout", "connect_timeout", "read_timeout"):
            value = getattr(self, name)
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be positive and finite")


@dataclass(frozen=True, kw_only=True)
class CollectionContext:
    org_id: str
    namespace: str
    selection: dict[str, Any]
    previous_cursor: Any = field(default=None, repr=False)
    checkpoint_revision: int | None = None
    generation: int | None = None
    run_id: UUID = field(default_factory=uuid4)
    limits: CollectionLimits = field(default_factory=CollectionLimits)

    def __post_init__(self):
        require_text(self.org_id, "org_id")
        require_text(self.namespace, "namespace")
        if type(self.selection) is not dict or not self.selection:
            raise ValueError("selection must describe a nonempty provider scope")
        if type(self.run_id) is not UUID or self.run_id.int == 0:
            raise ValueError("run_id must be a non-nil UUID")
        if self.generation is not None and (type(self.generation) is not int or not 0 <= self.generation < 2**63):
            raise ValueError("generation must fit a nonnegative signed 64-bit integer")
        if self.checkpoint_revision is not None and (type(self.checkpoint_revision) is not int or not 0 <= self.checkpoint_revision < 2**63 - 1):
            raise ValueError("invalid checkpoint revision")
        if not isinstance(self.limits, CollectionLimits):
            raise ValueError("invalid collection limits")
        object.__setattr__(self, "selection", loads(dumps(self.selection)))
        object.__setattr__(self, "previous_cursor", loads(dumps(self.previous_cursor)))


@dataclass(kw_only=True)
class CollectedBatch:
    snapshots: list
    original_envelopes: list[dict[str, Any]] = field(default_factory=list, repr=False)
    proposed_cursor: Any = field(default=None, repr=False)
    complete: bool = False
    diagnostics: list[str] = field(default_factory=list)


class Connector(Protocol):
    name: str
    source: str

    def collect(self, context: CollectionContext, *, cancellation, deadline: float) -> CollectedBatch: ...


def check_collection(cancellation, deadline):
    if cancellation.cancelled:
        raise CollectionError("cancelled")
    if time.monotonic() >= deadline:
        raise CollectionError("deadline")


def collection_key(connector: str, source: str, context: CollectionContext, *, selection=None):
    return digest([context.org_id, context.namespace, connector, source,
                   context.selection if selection is None else selection])


def observed_time(value):
    try:
        if type(value) is not str:
            raise ValueError
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError
        return parsed
    except (ValueError, OverflowError):
        raise CollectionError("invalid_event_observation_time") from None


def connector_key(connector, context):
    selection = context.selection
    normalize = getattr(connector, "ownership_selection", None)
    if normalize is not None:
        selection = normalize(selection)
    return collection_key(connector.name, connector.source, context, selection=selection)

