"""Register connector names without letting connectors configure graph processing."""
from types import MappingProxyType

from . import _native
from .serialization import dumps, loads
from .connector import digest, require_text


def register(config, connectors):
    config = loads(dumps(config))
    if type(config) is not dict:
        raise _native.ConfigurationError("configuration must be an object")
    registry = {}
    for connector in connectors:
        require_text(connector.name, "connector name")
        require_text(connector.source, "connector source")
        if connector.name in registry:
            raise _native.ConfigurationError("duplicate connector name")
        if not callable(getattr(connector, "collect", None)):
            raise _native.ConfigurationError("connector must collect snapshots")
        registry[connector.name] = (connector.source, connector)
    return config, MappingProxyType(registry)


def configuration_fingerprint(config, ontology, ontology_org_id):
    config = loads(dumps(config))
    config.get("graph", {}).pop("password", None)
    # A malformed shape here must surface as a configuration error, not an
    # AttributeError that the engine would report as an aborted run with an
    # unknown commit. Both are mappings keyed by model/embedder role.
    models = config.get("models", {})
    if type(models) is not dict:
        raise TypeError("configuration 'models' must be a mapping")
    embedder = config.get("embedder")
    if embedder is None:
        config.pop("embedder", None)
        embedder = {}
    if type(embedder) is not dict:
        raise TypeError("configuration 'embedder' must be a mapping")
    providers = [embedder, *models.values()]
    for provider in providers:
        if type(provider) is dict:
            provider.pop("api_key", None)
    # This is the processing/journal contract version, not the native ABI.
    # Transport-only upgrades must not invalidate already prepared runs.
    # Only the digest is persisted, never connection URLs, credentials or configuration.
    return digest({"protocol": 1, "config": config,
                   "ontology": ontology, "ontology_org_id": ontology_org_id})


def storage_fingerprint(config):
    graph = loads(dumps(config.get("graph", {})))
    graph.pop("password", None)
    return digest(graph)
