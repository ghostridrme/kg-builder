"""Connectors emit source observations, never inferred graph relationships."""
from kg_sdk.connector import (
    CollectedBatch, CollectionContext, CollectionError, CollectionLimits,
    Connector,
)

__all__ = ["CollectedBatch", "CollectionContext", "CollectionError", "CollectionLimits",
           "Connector"]

from .local_folder import LocalFolderConnector

__all__.append("LocalFolderConnector")
