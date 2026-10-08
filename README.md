# kg-builder — Infrastructure Knowledge Graph Builder

Build a knowledge graph from structured resource data using a Rust engine, a Python SDK, and Neo4j. Discover relationships automatically, track changes over time, and explore dependencies through a graph UI or MCP tools—without an LLM.

## Included

- Rust ingestion, supplied entity keys and alternative keys, exact deduplication, deterministic references, scoped identities, snapshots and threads.
- Temporal versions, deletions, replay receipts, transaction guards and cancellation.
- Python SDK and a generic local-folder connector for exported AWS resource JSON.
- Neo4j, graph APIs, read-only MCP tools and the 2D graph frontend.
- Keyword search. No embedding service, model configuration or paid API is needed.

## Automatic relationship discovery

You supply entities, their primary and alternative identity keys, and their resource properties. The engine looks for property values that reference other entities and creates relationships when its identity and scope checks establish a unique target. You do not need to supply every relationship yourself.

For example, an EC2 instance's `SubnetId` can match a subnet's declared identity key. The engine can then create an instance-to-subnet relationship. Composite keys require all necessary components; ambiguous matches remain unresolved instead of becoming guessed edges. Source-declared relationships are also supported.

## Supported input and limits

- Structured entities and resource properties are supported; extracting entities from free-form text is not.
- Relationship discovery is automatic and deterministic. LLM-based discovery and semantic relationship naming are not enabled.
- Keyword search and graph traversal are available. Vector search, model-based identity matching, schema inference, generated summaries, and community maintenance are not enabled.
- Unsupported ingestion settings fail explicitly.

## Local setup

Requires Rust 1.92+, CPython 3.11–3.14, Node.js 20.9+ and Docker Compose (or compatible Podman Compose).

    cp .env.example .env
    # Edit NEO4J_PASSWORD before starting.
    docker compose up -d
    python3 -m venv .venv
    .venv/bin/python -m pip install maturin==1.15.0 pytest
    . .venv/bin/activate
    task build:python
    task install:python
    python -m pip install 'kg-sdk[profiles]==0.1.0'
    task server

In another terminal:

    task frontend:install
    task frontend:dev

Open http://127.0.0.1:3000. Backend: http://127.0.0.1:8080.
Install go-task to use these commands, or run the commands listed in Taskfile.yml directly.

## Ingest exported resource files

The [local-folder connector](python/kg-connectors/README.md) automatically discovers resource JSON files with a common ARN/ID envelope. Twenty synthetic AWS API-shaped resource files are included under `python/kg-connectors/data/aws/`. No AWS credentials or per-resource Python handlers are required.

```sh
python tools/ingest_local_folder.py python/kg-connectors/data/aws \
  --dataset aws-local-demo --namespace aws-local-demo
```

Export the Neo4j environment variables before running this command. Relationship discovery and versioning happen in Rust.

## Ingestion

    import os
    from kg_sdk import Engine

    config = {
        "graph": {
            "type": "neo4j",
            "uri": os.environ["NEO4J_URI"],
            "username": os.environ.get("NEO4J_USER", "neo4j"),
            "password": os.environ["NEO4J_PASSWORD"],
        }
    }
    with Engine(config) as engine:
        result = engine.ingest([{
            "org_id": "graph-demo", "namespace": "aws-dev", "source": "aws",
            "name": "describe-vpcs", "data_type": "entities",
            "captured_at": "2026-10-08T12:00:00Z",
            "entities": [{
                "entity_type": "AWS::EC2::VPC", "name": "vpc-0123456789abcdef0",
                "primary_key_properties": ["VpcId"], "additional_key_properties": [],
                "raw_properties": {
                    "VpcId": "vpc-0123456789abcdef0",
                    "CidrBlock": "10.0.0.0/16", "State": "available"
                },
                "org_id": "graph-demo", "namespace": "aws-dev",
                "source": "aws", "lifecycle": "active", "tags": {}
            }]
        }], org_id="graph-demo")
        result.require_complete()

Keys in this example are local to an isolated inventory namespace. For multiple accounts preserve each resource’s ARN, ID, account, and region in the common export envelope. Ambiguous references are not guessed.

Use task test:smoke after starting Neo4j to run ingestion, replay, unchanged-scan, versioning and deletion checks. It retains its unique test namespace for inspection. It never resets the database and needs no model services.

## Development

Use an isolated Python virtual environment. The SDK verifies that its native extension is compatible with this deterministic build.

Keep `.env`, `node_modules`, `target`, virtual environments, and generated results out of Git. The Neo4j volume contains graph data and is not part of the repository.

## Licensing

Licensed under the [Apache License 2.0](LICENSE). Bundled third-party code and fonts retain their own license notices.
