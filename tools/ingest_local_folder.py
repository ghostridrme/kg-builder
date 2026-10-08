"""Ingest a local export through the SDK journal and the Rust engine; no AWS calls."""
import argparse
import json
import os
from pathlib import Path

from kg_connectors import LocalFolderConnector
from kg_sdk import CollectionContext, Engine, RunJournal


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--namespace', required=True)
    parser.add_argument('--org', default=os.environ.get('KG_ORG_ID', 'graph-demo'))
    parser.add_argument('--journal', type=Path, default=Path('.local-journal'))
    args = parser.parse_args()
    connector = LocalFolderConnector(args.directory, dataset=args.dataset)
    config = {'graph': {'type': 'neo4j', 'uri': os.environ.get('NEO4J_URI', 'bolt://127.0.0.1:7687'),
        'username': os.environ.get('NEO4J_USER', 'neo4j'), 'password': os.environ['NEO4J_PASSWORD']}}
    context = CollectionContext(org_id=args.org, namespace=args.namespace, selection={'dataset': args.dataset})
    journal = RunJournal(args.journal)
    with Engine(config, connectors=[connector]) as engine:
        prepared = engine.prepare(connector.name, context, journal=journal)
        result = engine.replay(prepared, journal=journal).require_complete()
        print(json.dumps({'run_id': str(prepared.run_id), 'namespace': args.namespace,
                         'counts': result['newly_committed']}, indent=2))


if __name__ == '__main__':
    main()
