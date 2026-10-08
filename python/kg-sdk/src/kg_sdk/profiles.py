"""Opt-in, exact profile revisions. Rust validates domain semantics."""
from dataclasses import dataclass
from pathlib import Path
from .serialization import dumps, loads

MAX_PROFILE_BYTES = 262_144

@dataclass(frozen=True)
class ProfileRef:
    profile_id: str
    revision: int

    def __post_init__(self):
        if (type(self.profile_id) is not str or not 1 <= len(self.profile_id) <= 128
                or not self.profile_id.isascii()
                or any(not (c.isalnum() or c in "-_.") for c in self.profile_id)
                or type(self.revision) is not int or not 1 <= self.revision < 2**63):
            raise ValueError("invalid profile id or revision")

    def to_dict(self):
        return {"profile_id": self.profile_id, "revision": self.revision}


def reference(value):
    if type(value) is ProfileRef:
        return value.to_dict()
    if type(value) is not dict or set(value) != {"profile_id", "revision"}:
        raise ValueError("profile reference needs profile_id and revision")
    return ProfileRef(**value).to_dict()


def bindings(value):
    if value is None:
        return {}
    if type(value) is not dict or len(value) > 256:
        raise ValueError("profiles must be a bounded source-to-reference map")
    result = {}
    for source, ref in value.items():
        if type(source) is not str or not source.strip() or len(source.encode()) > 4096:
            raise ValueError("invalid profile source")
        result[source] = reference(ref)
    return result


def load_profile(path):
    """Read bounded JSON or safe YAML; duplicate keys and non-JSON values fail."""
    path = Path(path)
    with path.open("rb") as stream:
        raw = stream.read(MAX_PROFILE_BYTES + 1)
    if len(raw) > MAX_PROFILE_BYTES:
        raise ValueError("profile exceeds byte limit")
    if path.suffix.lower() not in (".yaml", ".yml"):
        value = loads(raw, max_bytes=MAX_PROFILE_BYTES, max_depth=32)
    else:
        try:
            import yaml
        except ImportError as error:
            raise ValueError("install kg-sdk[profiles] for YAML; JSON needs no extra dependency") from error

        class Loader(yaml.SafeLoader):
            pass

        def mapping(loader, node):
            result = {}
            for key_node, value_node in node.value:
                key = loader.construct_object(key_node)
                if type(key) is not str or key in result:
                    raise ValueError("duplicate or non-string YAML key")
                result[key] = loader.construct_object(value_node)
            return result

        Loader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, mapping)
        value = yaml.load(raw.decode("utf-8"), Loader=Loader)
    if type(value) is not dict:
        raise ValueError("profile must be an object")
    return loads(dumps(value, max_bytes=MAX_PROFILE_BYTES, max_depth=32))
