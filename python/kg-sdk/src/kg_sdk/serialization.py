"""Lossless, bounded JSON at the Python/Rust boundary."""
import json
import math


def dumps(value, *, max_bytes=64 * 1024 * 1024, max_depth=64):
    ancestors = set()
    estimated = 0

    def visit(item, depth):
        nonlocal estimated
        if depth > max_depth:
            raise ValueError("JSON nesting limit exceeded")
        estimated += 1
        if item is None or type(item) is bool:
            pass
        elif type(item) is int:
            if not -(2**63) <= item < 2**63:
                raise ValueError("integer exceeds signed 64-bit range")
        elif type(item) is float:
            if not math.isfinite(item):
                raise ValueError("non-finite float")
        elif type(item) is str:
            # Lower bound before encoding a possibly enormous string.
            if len(item) > max_bytes - estimated:
                raise ValueError("JSON byte limit exceeded")
            for start in range(0, len(item), 4096):
                encoded = json.dumps(item[start:start + 4096], ensure_ascii=False).encode("utf-8", errors="strict")
                estimated += len(encoded) - 2
                if estimated > max_bytes:
                    raise ValueError("JSON byte limit exceeded")
        elif type(item) is dict or type(item) in (list, tuple):
            if id(item) in ancestors:
                raise ValueError("circular JSON input")
            ancestors.add(id(item))
            if type(item) is dict:
                for key, child in item.items():
                    if type(key) is not str:
                        raise ValueError("JSON object keys must be strings")
                    visit(key, depth + 1)
                    visit(child, depth + 1)
            else:
                for child in item:
                    visit(child, depth + 1)
            ancestors.remove(id(item))
        else:
            raise ValueError("unsupported JSON value; use explicit provider serialization")
        if estimated > max_bytes:
            raise ValueError("JSON byte limit exceeded")

    visit(value, 0)
    parts = []
    size = 0
    for part in json.JSONEncoder(ensure_ascii=False, allow_nan=False, separators=(",", ":")).iterencode(value):
        size += len(part.encode("utf-8"))
        if size > max_bytes:
            raise ValueError("JSON byte limit exceeded")
        parts.append(part)
    return "".join(parts)


def loads(raw, *, max_bytes=64 * 1024 * 1024, max_depth=64):
    if len(raw) > max_bytes or (isinstance(raw, str) and len(raw.encode("utf-8")) > max_bytes):
        raise ValueError("JSON byte limit exceeded")
    def object_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON object key")
            result[key] = value
        return result

    def constant(_):
        raise ValueError("non-finite JSON number")

    value = json.loads(raw, object_pairs_hook=object_pairs, parse_constant=constant)
    # Validate floats such as 1e400 and integers beyond i64 as well.
    dumps(value, max_bytes=max_bytes, max_depth=max_depth)
    return value
