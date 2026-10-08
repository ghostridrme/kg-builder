"""Read identity envelopes and unchanged resource payloads without AWS-specific parsers."""
from copy import deepcopy
import os
from pathlib import Path
import re
import stat

from kg_sdk.connector import (
    CollectedBatch, CollectionError, check_collection, observed_time, require_text,
)
from kg_sdk.inputs import ConnectorEntity, SnapshotInput
from kg_sdk.serialization import loads

_SCOPE = "_astrolabe_scope"
_IDENTITY = "_kg_identity"


def _identity_paths(value, expected, prefix=""):
    """Find object properties equal to the known ID, never guess from field names.

    Arrays are reference collections, not stable identity property paths. Keys
    containing literal dots are not representable unambiguously by the SDK's
    dotted identity paths, so they cannot become additional keys.
    """
    if type(value) is not dict:
        return
    for key, child in sorted(value.items()):
        if not key or "." in key or "[" in key or "]" in key:
            continue
        path = f"{prefix}.{key}" if prefix else key
        if type(child) is str and child == expected:
            yield path
        elif type(child) is dict:
            yield from _identity_paths(child, expected, path)


def _entity(envelope, context, source):
    required = {"resourceArn", "resourceId", "resourceType", "accountId", "awsRegion", "capturedAt", "payload"}
    if (type(envelope) is not dict or not required <= envelope.keys()
            or set(envelope) - required - {"resourceName", "lifecycle"}):
        raise CollectionError("invalid_resource_envelope")
    if any(type(envelope[k]) is not str or not envelope[k].strip()
           for k in ("resourceArn", "resourceId", "resourceType", "accountId")):
        raise CollectionError("invalid_resource_identity")
    if not re.fullmatch(r"[0-9]{12}", envelope["accountId"]):
        raise CollectionError("invalid_resource_account")
    region = envelope["awsRegion"]
    if type(region) is not str or (region and not re.fullmatch(r"[a-z0-9-]+", region)):
        raise CollectionError("invalid_resource_region")
    arn = envelope["resourceArn"].split(":", 5)
    if len(arn) != 6 or arn[0] != "arn" or not arn[1] or not arn[2] or not arn[5] or any(c.isspace() for c in envelope["resourceArn"]) or any(c in envelope["resourceArn"] for c in "*?"):
        raise CollectionError("invalid_resource_arn")
    # Empty ARN scope components are legitimate (e.g. S3 and global IAM).
    # A nonempty component must agree with the export; never rewrite it.
    if (arn[3] and arn[3] != region) or (arn[4] and arn[4] != envelope["accountId"]):
        raise CollectionError("resource_arn_scope_mismatch")
    observed_time(envelope["capturedAt"])
    lifecycle = envelope.get("lifecycle", "active")
    if lifecycle not in ("active", "deleted"):
        raise CollectionError("invalid_resource_lifecycle")
    name = envelope.get("resourceName", envelope["resourceId"])
    if type(name) is not str or not name.strip():
        raise CollectionError("invalid_resource_name")
    payload = envelope["payload"]
    if type(payload) is not dict or any(
        k == reserved or k.startswith(reserved + ".")
        for k in payload for reserved in (_SCOPE, _IDENTITY)
    ):
        raise CollectionError("invalid_resource_object")
    scope = {"account_id": envelope["accountId"]}
    if region:
        scope["region"] = region
    properties = deepcopy(payload)
    properties[_SCOPE] = scope
    properties[_IDENTITY] = {"arn": envelope["resourceArn"], "resource_id": envelope["resourceId"]}
    scope_keys = [f"{_SCOPE}.{key}" for key in scope]
    paths = list(_identity_paths(payload, envelope["resourceId"]))
    additional = [scope_keys + [path] for path in paths]
    # This common identity alias is available even when the native ID is not
    # repeated inside the response. Never derive a short ID by splitting an ARN.
    additional.append(scope_keys + [f"{_IDENTITY}.resource_id"])
    # Keep native own-ARN properties usable as exact references too.
    additional.extend([[path] for path in _identity_paths(payload, envelope["resourceArn"])])
    additional = [list(group) for group in dict.fromkeys(tuple(group) for group in additional)]
    return ConnectorEntity(entity_type=envelope["resourceType"], name=name,
        primary_key_properties=[f"{_IDENTITY}.arn"], additional_key_properties=additional,
        raw_properties=properties, source=source, org_id=context.org_id,
        namespace=context.namespace, lifecycle=lifecycle), bool(paths)


class LocalFolderConnector:
    """Discover JSON files recursively; no manifest and no AWS API calls.

    The root must be a trusted, immutable export during collection. Missing files
    never authorize deletion; tombstones require explicit lifecycle metadata.
    """

    def __init__(self, root, *, name="local-aws", source="aws", dataset="aws-local-demo"):
        for field, value in (("name", name), ("source", source), ("dataset", dataset)):
            require_text(value, field)
        self.root = Path(root).resolve(strict=True)
        if not self.root.is_dir():
            raise ValueError("root must be a directory")
        self.name, self.source, self.dataset = name, source, dataset

    def collect(self, context, *, cancellation, deadline):
        check_collection(cancellation, deadline)
        if context.selection != {"dataset": self.dataset}:
            raise CollectionError("invalid_dataset_scope")
        if context.previous_cursor is not None or context.generation is not None:
            raise CollectionError("local_export_requires_incremental_observations")
        consumed = 0
        snapshots, originals, diagnostics = [], [], []
        def files():
            result, visited = [], 0
            def walk_error(_):
                raise CollectionError("resource_directory_unreadable")
            for directory, dirs, names in os.walk(self.root, followlinks=False, onerror=walk_error):
                check_collection(cancellation, deadline)
                visited += len(dirs) + len(names) + 1
                if visited > context.limits.max_records + context.limits.max_pages:
                    raise CollectionError("collection_limit")
                dirs.sort()
                for name in dirs + names:
                    if (Path(directory) / name).is_symlink():
                        raise CollectionError("resource_symlink")
                for name in sorted(names):
                    if not name.endswith('.json'):
                        continue
                    path = Path(directory) / name
                    if len(path.relative_to(self.root).parts) < 2:
                        raise CollectionError("resource_file_requires_type_folder")
                    result.append(path)
                    if len(result) > min(context.limits.max_pages, context.limits.max_records):
                        raise CollectionError("collection_limit")
            return result
        paths = files()
        signatures = {}
        for path in paths:
            check_collection(cancellation, deadline)
            relative = path.relative_to(self.root).as_posix()
            try:
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                with os.fdopen(fd, 'rb') as handle:
                    before = os.fstat(handle.fileno())
                    if not stat.S_ISREG(before.st_mode):
                        raise CollectionError("resource_not_regular_file")
                    if before.st_size > context.limits.max_bytes - consumed:
                        raise CollectionError("collection_limit")
                    raw = handle.read(context.limits.max_bytes - consumed + 1)
                consumed += len(raw)
                if consumed > context.limits.max_bytes:
                    raise CollectionError("collection_limit")
                signatures[path] = (before.st_ino, before.st_size, before.st_mtime_ns)
                envelope = loads(raw)
            except (OSError, ValueError, RecursionError):
                raise CollectionError("invalid_resource_json") from None
            entity, has_native_id = _entity(envelope, context, self.source)
            if not has_native_id:
                diagnostics.append(f"native_id_property_not_found:{relative}")
            snapshots.append(SnapshotInput(org_id=context.org_id, namespace=context.namespace,
                name=relative, source=self.source, data_type="entities", entities=[entity],
                snapshot_kind="full", complete=True,
                captured_at=envelope["capturedAt"], source_description="Local exported resource object"))
            originals.append({"file": relative, "envelope": envelope})
        if paths != files():
            raise CollectionError("export_changed_during_read")
        for path, before in signatures.items():
            check_collection(cancellation, deadline)
            try:
                after = path.stat()
            except OSError:
                raise CollectionError("export_changed_during_read") from None
            if before != (after.st_ino, after.st_size, after.st_mtime_ns):
                raise CollectionError("export_changed_during_read")
        return CollectedBatch(snapshots=snapshots, original_envelopes=originals,
                              complete=True, diagnostics=diagnostics)
