"""Shared setup extracted from test_live_engine; assertions stay in owner tests."""
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import pytest

@pytest.fixture
def service(request):
    class Service:
        gate = threading.Event()
        entered = threading.Event()
        calls = 0
        completion_calls = 0
    service = Service()
    service.gate.set()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            if not self.path.endswith('/embeddings'):
                service.completion_calls += 1
                self.send_error(500)
                return
            service.calls += 1
            service.entered.set()
            assert service.gate.wait(15), "test failed to release embedding gate"
            inputs = body['input']
            if isinstance(inputs, str):
                inputs = [inputs]
            response = {'object': 'list', 'model': 'sdk-transport-test', 'data': [
                {'object': 'embedding', 'index': i, 'embedding': [1.0] + [0.0] * 1535} for i, _ in enumerate(inputs)
            ], 'usage': {'prompt_tokens': 0, 'total_tokens': 0}}
            raw = json.dumps(response).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            try:
                self.wfile.write(raw)
            except (BrokenPipeError, ConnectionResetError):
                pass

    # Live tests write per-test organisations to a real database. Refuse to fall
    # back to the demo bolt URI and password (Taskfile forbids it); a missing
    # variable is an operator error, not a silent demo write. Validate the
    # environment before binding the HTTP server/thread so a setup failure does
    # not leak that thread and socket.
    try:
        neo4j_uri, neo4j_password = os.environ['NEO4J_URI'], os.environ['NEO4J_PASSWORD']
    except KeyError as missing:
        raise RuntimeError(
            f"live SDK tests require {missing.args[0]}; refusing to use the demo database"
        ) from None
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    service.config = {
        'graph': {'type': 'neo4j', 'uri': neo4j_uri,
                  'username': 'neo4j', 'password': neo4j_password,
                  'timeout_ms': 5000, 'max_retries': 1},
        'models': {'default': {'type': 'disabled'}},
        'embedder': {'type': 'openai_compatible', 'model': 'sdk-transport-test', 'dimension': 1536,
                     'api_key': 'local-test-only', 'endpoint': f'http://127.0.0.1:{server.server_port}/v1'},
        'processing': {'policy': {'extraction': 'heuristic', 'matching': 'exact', 'edge_discovery': 'heuristic', 'edge_ambiguity': 'skip'}}
    }
    yield service
    service.gate.set()
    server.shutdown()
    server.server_close()
    thread.join(5)
    request.node.user_properties.extend([("embedding_calls", service.calls), ("completion_calls", service.completion_calls)])
    assert service.completion_calls == 0


def observation(org, name):
    return {'org_id': org, 'namespace': 'native-bridge', 'source': 'sdk-test', 'name': name,
            'data_type': 'entities', 'entities': [{'entity_type': 'SdkTransportProbe', 'name': name,
            'primary_key_properties': ['id'], 'raw_properties': {'id': name, 'details': {'unicode': '日本', 'port': 5432}},
            'lifecycle': 'active', 'tags': {}, 'source': 'sdk-test', 'org_id': org}]}

