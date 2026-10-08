"""Provider-free configuration and wheel isolation checks."""
import pytest
from kg_sdk import ConfigurationError, Engine, _native

def test_deterministic_wheel_marker():
    assert _native.BUILD_FLAVOR == "deterministic"

@pytest.mark.parametrize("extra", [
    {"processing":{"recipe":"full"}},
    {"processing":{"policy":{"matching":"semantic"}}},
    {"processing":{"source_policies":{"aws":{"edge_ambiguity":"llm"}}}},
    {"embedder":{"type":"openai","model":"text-embedding-3-small","dimension":1536,"api_key":"local-test-only"}},
])
def test_unsupported_settings_fail_before_database_connection(extra):
    config={"graph":{"type":"neo4j","uri":"bolt://127.0.0.1:1","username":"test","password":"local-test-only"},**extra}
    with pytest.raises(ConfigurationError):
        Engine(config)
