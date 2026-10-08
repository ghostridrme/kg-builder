"""Python input collection and synchronous Rust knowledge-graph ingestion."""
import sys
import sysconfig

__version__ = "0.1.0"
if sys.implementation.name != "cpython" or not (3, 11) <= sys.version_info[:2] < (3, 15) or sysconfig.get_config_var("Py_GIL_DISABLED"):
    raise ImportError("KG Pipeline requires standard GIL-enabled CPython 3.11–3.14")
from . import _native

if (_native.__version__ != __version__ or _native.PROTOCOL_VERSION != 4
        or getattr(_native, "BUILD_FLAVOR", None) != "deterministic"):
    raise ImportError("KG Pipeline Python/native versions differ; reinstall the same wheel")

from .engine import (  # noqa: E402
    CancellationToken, ConfigurationError, Engine, EngineClosedError, ForkedEngineError,
    OperationError, IngestionError, IngestionInterrupted, IngestionResult, InputValidationError, Limits,
    TelemetryHandle, configure_telemetry,
)
from .connector import (  # noqa: E402
    CollectedBatch, CollectionContext, CollectionError, CollectionLimits, Connector,
)
from .inputs import ConnectorEntity, ExistingSnapshotInput, SnapshotInput  # noqa: E402
from .journal import CheckpointStore, JournalError, PreparedRun, RunJournal  # noqa: E402

from .profiles import ProfileRef, load_profile  # noqa: E402

from .checkpoints import Neo4jCheckpointStore  # noqa: E402

__all__ = ["Neo4jCheckpointStore", "ProfileRef", "load_profile", "OperationError", "CancellationToken", "ConfigurationError", "Engine", "EngineClosedError",
           "ForkedEngineError", "IngestionError", "IngestionInterrupted", "IngestionResult",
           "InputValidationError", "Limits", "TelemetryHandle", "configure_telemetry",
           "CollectedBatch", "CollectionContext", "CollectionError", "CollectionLimits", "Connector",
           "ConnectorEntity", "ExistingSnapshotInput", "SnapshotInput",
           "CheckpointStore", "JournalError", "PreparedRun", "RunJournal"]
