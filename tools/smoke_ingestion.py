"""Real Neo4j smoke check. No database reset, model, vectors, credentials or results in source."""
import base64
import copy
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import time
import urllib.request
from uuid import uuid4

from kg_sdk import Engine

ROOT = Path(__file__).resolve().parents[1]
def main():
    org = os.environ.get("KG_ORG_ID", "graph-demo")
    ns = "aws-builder-" + uuid4().hex[:10]
    username = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ["NEO4J_PASSWORD"]
    graph = {"type": "neo4j", "uri": os.environ.get("NEO4J_URI", "bolt://127.0.0.1:7687"),
             "username": username, "password": password}
    http = os.environ.get("NEO4J_HTTP", "http://127.0.0.1:7474")
    def read(query):
        req = urllib.request.Request(http + "/db/neo4j/tx/commit",
            data=json.dumps({"statements": [{"statement": query, "parameters": {"org": org, "ns": ns}}]}).encode(),
            headers={"Content-Type": "application/json", "Authorization": "Basic " +
                     base64.b64encode((username + ":" + password).encode()).decode()})
        with urllib.request.urlopen(req, timeout=30) as response:
            result = json.load(response)
        assert not result["errors"], result["errors"]
        return [row["row"] for row in result["results"][0]["data"]]
    inputs = json.loads((ROOT / "python/kg-sdk/tests/fixtures/ingestion/aws_builder_snapshot.json").read_text())
    start = datetime.now(timezone.utc) - timedelta(minutes=10)
    def observed(values, minute):
        values = copy.deepcopy(values)
        for snap in values:
            snap.update(org_id=org, namespace=ns, captured_at=(start + timedelta(minutes=minute)).isoformat())
            for entity in snap["entities"]:
                entity.update(org_id=org, namespace=ns)
        return values
    inputs = observed(inputs, 0)
    # A repeated identity in the same batch must not create a second chain.
    duplicate = copy.deepcopy(inputs[0]); duplicate["name"] += "-duplicate"
    inputs.append(duplicate)
    results = []
    def ingest(engine, label, payload, run=None):
        tick = time.monotonic()
        result = engine.ingest(payload, org_id=org, run_id=run, timeout=180).require_complete()
        counts = result["newly_committed"]
        results.append({"step": label, "seconds": round(time.monotonic()-tick, 3), "counts": counts})
        print(label, json.dumps({k: counts[k] for k in ("entities_created","entities_updated","entities_deleted","entities_unchanged","edges_created","embeddings")}), flush=True)
        assert counts["embeddings"] == 0
        return result
    with Engine({"graph": graph}) as engine:
        run = uuid4()
        first = ingest(engine, "initial and within-batch duplicate", inputs, run)
        assert first["newly_committed"]["entities_created"] == 8
        assert first["newly_committed"]["edges_created"] == 13
        replay = ingest(engine, "same-run replay", inputs, run)
        assert not any(replay["newly_committed"].values())
        unchanged = ingest(engine, "unchanged scan", observed(inputs[:8], 1))
        assert unchanged["newly_committed"]["entities_created"] == 0
        assert unchanged["newly_committed"]["entities_updated"] == 0
        updated = observed([inputs[3]], 2)
        updated[0]["entities"][0]["raw_properties"]["State"] = {"Code": 80, "Name": "stopped"}
        update = ingest(engine, "EC2 state change", updated)
        assert update["newly_committed"]["entities_updated"] == 1
        versions = read("MATCH (e:Entity {org_id:$org,namespace:$ns}) WHERE e.entity_type='AWS::EC2::Instance' RETURN e.version,e.is_latest,e.`prop_State.Name` ORDER BY e.version")
        assert versions == [[1,False,"running"],[2,True,"stopped"]], versions
        deleted = observed([inputs[5]], 3)
        deleted[0]["entities"][0]["lifecycle"] = "deleted"
        deletion = ingest(engine, "EIP deletion", deleted)
        assert deletion["newly_committed"]["entities_deleted"] == 1
    counts = read("MATCH (e:Entity {org_id:$org,namespace:$ns}) RETURN count(DISTINCT e.chain_id),sum(CASE WHEN e.is_latest AND e.deleted_at IS NULL THEN 1 ELSE 0 END),sum(CASE WHEN e.embedding IS NOT NULL THEN 1 ELSE 0 END)")
    assert counts == [[8,7,0]], counts
    edges = read("MATCH (s:Entity {org_id:$org,namespace:$ns})-[r:RELATES_TO]->(t:Entity {org_id:$org,namespace:$ns}) WHERE r.is_latest AND r.valid_to IS NULL AND r.deleted_at IS NULL RETURN DISTINCT s.name,t.name")
    assert len(edges)==12, edges
    assert not any(target.startswith("eipalloc-") for _,target in edges)
    print(json.dumps({"namespace":ns,"org":org,"chains":8,"live_resources":7,"live_relationships":12,
                      "historical_time":(start+timedelta(seconds=30)).isoformat(),"runs":results},indent=2))

if __name__ == "__main__":
    main()
