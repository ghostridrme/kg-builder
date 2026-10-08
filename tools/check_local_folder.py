"""Live dynamic-connector lifecycle checks. Retains a unique namespace; never resets Neo4j.

Run with NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD and optionally NEO4J_HTTP.
Fixtures and mutations are test data only, not connector resource mappings.
"""
import json,os,tempfile,shutil,urllib.request,base64,time
from pathlib import Path
from uuid import uuid4
from kg_sdk import Engine,CollectionContext,RunJournal
from kg_connectors import LocalFolderConnector
root=Path(__file__).resolve().parents[1]/'python/kg-connectors/data/aws';ns='aws-dynamic-'+uuid4().hex[:8];org=os.environ.get('KG_ORG_ID','graph-demo')
user=os.environ.get('NEO4J_USER','neo4j');pw=os.environ['NEO4J_PASSWORD']
config={'graph':{'type':'neo4j','uri':os.environ.get('NEO4J_URI','bolt://127.0.0.1:7687'),'username':user,'password':pw}}

def q(cypher,**params):
 req=urllib.request.Request(os.environ.get('NEO4J_HTTP','http://127.0.0.1:7474').rstrip('/')+'/db/neo4j/tx/commit',data=json.dumps({'statements':[{'statement':cypher,'parameters':dict(ns=ns,org=org,**params)}]}).encode(),headers={'Content-Type':'application/json','Authorization':'Basic '+base64.b64encode((user+':'+pw).encode()).decode()})
 v=json.load(urllib.request.urlopen(req,timeout=30));assert not v['errors'],v['errors'];return [x['row'] for x in v['results'][0]['data']]
def visible(alias):
 return ' AND '.join([f'datetime({alias}.valid_from)<=datetime($at)']+[f'({alias}.{field} IS NULL OR datetime($at)<datetime({alias}.{field}))' for field in ['valid_to','invalid_at','deleted_at']])

def historical(kind,prop,at):
 return q(f'MATCH (e:Entity {{org_id:$org,namespace:$ns,entity_type:$kind}}) WHERE e.`prop__astrolabe_scope.account_id`="111122223333" AND {visible("e")} RETURN e.version,e.`prop_{prop}` ORDER BY e.version',kind=kind,at=at)

def graph_counts():
 return q('MATCH (n {org_id:$org,namespace:$ns}) RETURN labels(n),count(n) ORDER BY labels(n)')

report=[]
with tempfile.TemporaryDirectory() as temp:
 data=Path(temp)/'data';shutil.copytree(root,data)
 # A second account deliberately reuses the same instance and volume short IDs.
 for folder in ['AWS_EC2_Instance','AWS_EC2_Volume']:
  p=next((data/folder).glob('*.json'));v=json.loads(p.read_text())
  v=json.loads(json.dumps(v).replace('111122223333','999999999999'));v['resourceName']='second-account-'+v['resourceId'];p.with_name('second-account.json').write_text(json.dumps(v))
 def load(folder):return next(p for p in (data/folder).glob('*.json') if p.name!='second-account.json')
 def mutate(folder,action):
  p=load(folder);v=json.loads(p.read_text());action(v);p.write_text(json.dumps(v))
 connector=LocalFolderConnector(data);journal=RunJournal(Path(temp)/'journal')
 with Engine(config,connectors=[connector]) as engine:
  def validate_graph(expected_live):
   rows=q('MATCH (e:Entity {org_id:$org,namespace:$ns}) WHERE e.is_latest AND e.deleted_at IS NULL RETURN e.chain_id,e.`prop__kg_identity.arn`')
   assert len(rows)==expected_live,(len(rows),expected_live)
   assert len({r[0] for r in rows})==len(rows) and len({r[1] for r in rows})==len(rows)
   edges=q('MATCH (s:Entity {org_id:$org,namespace:$ns})-[r:RELATES_TO]->(t:Entity {org_id:$org,namespace:$ns}) WHERE r.is_latest AND r.valid_to IS NULL AND r.deleted_at IS NULL RETURN s.name,t.name,s.`prop__astrolabe_scope.account_id`,t.`prop__astrolabe_scope.account_id`')
   disk=[r for r in edges if 'vol-0a' in r[0] and 'i-0a' in r[1]]
   assert len(disk)==2 and all(r[2]==r[3] for r in disk),disk
   return len(rows),len(edges)
  def run(label,round,created,updated,deleted,live):
   for p in data.rglob('*.json'):
    v=json.loads(p.read_text());v['capturedAt']=f'2026-10-08T13:{round:02d}:00Z';p.write_text(json.dumps(v))
   context=CollectionContext(org_id=org,namespace=ns,selection={'dataset':'aws-local-demo'})
   start=time.monotonic();prepared=engine.prepare(connector.name,context,journal=journal);result=engine.replay(prepared,journal=journal).require_complete();c=result['newly_committed']
   assert (c['entities_created'],c['entities_updated'],c['entities_deleted'])==(created,updated,deleted),c
   assert c['embeddings']==0,c
   nodes,edges=validate_graph(live)
   summary={'run':label,'input_files':len(list(data.rglob('*.json'))),'created':created,'updated':updated,'deleted':deleted,'unchanged':c['entities_unchanged'],'live_nodes':nodes,'live_edges':edges,'seconds':round_time(time.monotonic()-start)}
   print(json.dumps(summary),flush=True);report.append(summary);return prepared
  def round_time(x):return float(f'{x:.2f}')
  original_values={kind:json.loads(load(folder).read_text())['payload'][prop] for kind,folder,prop in [('AWS::EC2::Volume','AWS_EC2_Volume','Size'),('AWS::RDS::DBInstance','AWS_RDS_DBInstance','AllocatedStorage')]}
  first=run('initial: 20 types and two overlapping identities in second account',0,22,0,0,22)
  before_replay=graph_counts()
  c=engine.replay(first,journal=journal).require_complete()['newly_committed'];assert not any(c.values());validate_graph(22);assert graph_counts()==before_replay;print('PASS same-run replay: zero new commits',flush=True)
  run('unchanged rescan',1,0,0,0,22)
  mutate('AWS_EC2_Instance',lambda v:v['payload'].update(State={'Code':80,'Name':'stopped'}))
  mutate('AWS_EC2_Volume',lambda v:v['payload'].update(Size=200))
  mutate('AWS_RDS_DBInstance',lambda v:v['payload'].update(AllocatedStorage=150))
  run('three property changes',2,0,3,0,22)
  for kind,prop,expected in [('AWS::EC2::Instance','State.Name','stopped'),('AWS::EC2::Volume','Size',200),('AWS::RDS::DBInstance','AllocatedStorage',150)]:
   rows=q(f'MATCH (e:Entity {{org_id:$org,namespace:$ns,entity_type:$kind}}) WHERE e.`prop__astrolabe_scope.account_id`="111122223333" RETURN e.version,e.`prop_{prop}`,e.is_latest ORDER BY e.version',kind=kind)
   assert len(rows)==2 and rows[1]==[2,expected,True] and rows[0][2] is False,rows
  print('PASS stored version history for all three changed resources',flush=True)
  original=load('AWS_IAM_Role');v=json.loads(original.read_text());oldarn=v['resourceArn'];newarn=oldarn.replace('orders-app','orders-worker-v2')
  v=json.loads(json.dumps(v).replace('orders-app','orders-worker-v2'));v['payload']['RoleId']='AROAXAMPLE223456789012';(original.parent/'new-role.json').write_text(json.dumps(v))
  mutate('AWS_Lambda_Function',lambda v:v['payload'].update(Role=newarn))
  run('new role and changed Lambda dependency',3,1,1,0,23)
  deps=q('MATCH (s:Entity {org_id:$org,namespace:$ns,entity_type:"AWS::Lambda::Function"})-[r:RELATES_TO]->(t:Entity {entity_type:"AWS::IAM::Role"}) WHERE r.is_latest AND r.valid_to IS NULL RETURN t.`prop__kg_identity.arn`')
  assert deps==[[newarn]],deps
  print('PASS old Lambda role link retired; new role link active',flush=True)
  load('AWS_RDS_DBInstance').unlink()
  run('missing database file does not delete database',4,0,0,0,23)
  mutate('AWS_EC2_EIP',lambda v:v.update(lifecycle='deleted'))
  run('explicit EIP deletion',5,0,0,1,22)
  rows=q('MATCH (s:Entity {org_id:$org,namespace:$ns})-[r:RELATES_TO]->(t:Entity {entity_type:"AWS::EC2::EIP"}) WHERE r.is_latest AND r.valid_to IS NULL RETURN count(r)');assert rows==[[0]],rows
  print('PASS deleted EIP has no active incoming edges',flush=True)
  # Test source-time visibility after ALL later writes, at exact boundaries too.
  def validate_history():
   for kind,prop,old,new in [('AWS::EC2::Instance','State.Name','running','stopped'),('AWS::EC2::Volume','Size',original_values['AWS::EC2::Volume'],200),('AWS::RDS::DBInstance','AllocatedStorage',original_values['AWS::RDS::DBInstance'],150)]:
    for at,expected in [('12:59:59',[]),('13:00:00',[[1,old]]),('13:01:59',[[1,old]]),('13:02:00',[[2,new]]),('13:05:00',[[2,new]])]:
     actual=historical(kind,prop,'2026-10-08T'+at+'Z');assert actual==expected,(kind,at,actual,expected)
   for at,expected in [('13:02:59',oldarn),('13:03:00',newarn),('13:05:00',newarn)]:
    rows=q(f'MATCH (s:Entity {{org_id:$org,namespace:$ns,entity_type:"AWS::Lambda::Function"}})-[r:RELATES_TO]->(t:Entity {{org_id:$org,namespace:$ns,entity_type:"AWS::IAM::Role"}}) WHERE {visible("r")} AND r.cancelled_at IS NULL RETURN DISTINCT t.`prop__kg_identity.arn`',at='2026-10-08T'+at+'Z')
    assert rows==[[expected]],(at,rows,expected)
   assert len(historical('AWS::EC2::EIP','AllocationId','2026-10-08T13:04:59Z'))==1
   assert historical('AWS::EC2::EIP','AllocationId','2026-10-08T13:05:00Z')==[]
   for at,expected in [('13:04:59',1),('13:05:00',0)]:
    rows=q(f'MATCH (s:Entity {{org_id:$org,namespace:$ns}})-[r:RELATES_TO]->(t:Entity {{org_id:$org,namespace:$ns,entity_type:"AWS::EC2::EIP"}}) WHERE {visible("r")} AND r.cancelled_at IS NULL RETURN count(r)',at='2026-10-08T'+at+'Z')
    assert rows==[[expected]],(at,rows)
  validate_history()
  print('PASS historical values, exact time boundaries, old/new role links, and deletion history',flush=True)
  # Returning to an older value is a new version, never reuse of version 1.
  mutate('AWS_EC2_Instance',lambda v:v['payload'].update(State={'Code':16,'Name':'running'}))
  reverted=run('instance returns to running: version 3',6,0,1,0,22)
  assert historical('AWS::EC2::Instance','State.Name','2026-10-08T13:06:00Z')==[[3,'running']]
  validate_history()
  before_replay=graph_counts()
  assert not any(engine.replay(reverted,journal=journal).require_complete()['newly_committed'].values())
  assert graph_counts()==before_replay
  print('PASS version 3, prior history preserved, replay creates no extra nodes or snapshots',flush=True)
  def instance_links(kind,at):
   return q(f'MATCH (s:Entity {{org_id:$org,namespace:$ns,entity_type:"AWS::EC2::Instance"}})-[r:RELATES_TO]->(t:Entity {{org_id:$org,namespace:$ns,entity_type:$kind}}) WHERE s.`prop__astrolabe_scope.account_id`="111122223333" AND {visible("r")} AND r.cancelled_at IS NULL RETURN DISTINCT r.uuid',kind=kind,at=at)
  assert len(instance_links('AWS::EC2::SecurityGroup','2026-10-08T13:06:00Z'))==1
  assert len(instance_links('AWS::EC2::VPC','2026-10-08T13:06:00Z'))==1
  def remove_properties(v):
   del v['payload']['PrivateIpAddress']
   del v['payload']['VpcId']
   v['payload']['SecurityGroups']=[]
  mutate('AWS_EC2_Instance',remove_properties)
  removed=run('omitted properties and empty security groups',7,0,1,0,22)
  assert historical('AWS::EC2::Instance','PrivateIpAddress','2026-10-08T13:07:00Z')==[[4,None]]
  assert historical('AWS::EC2::Instance','PrivateIpAddress','2026-10-08T13:06:59Z')==[[3,'10.42.1.10']]
  for kind in ['AWS::EC2::SecurityGroup','AWS::EC2::VPC']:
   assert instance_links(kind,'2026-10-08T13:07:00Z')==[],kind
   assert len(instance_links(kind,'2026-10-08T13:06:59Z'))==1,kind
  validate_history()
  before_replay=graph_counts()
  assert not any(engine.replay(removed,journal=journal).require_complete()['newly_committed'].values())
  assert graph_counts()==before_replay
  print('PASS full-object omissions and empty-list retirement with >20 account/region carriers; prior history and replay preserved',flush=True)


print('NAMESPACE',ns);print(json.dumps({'namespace':ns,'runs':report},indent=2))

# Two existing VPCs plus one healthy new volume exercise chunk failure isolation.
from kg_sdk import IngestionError
for continuation in [True, False]:
 ns='aws-identity-'+uuid4().hex[:8]
 with tempfile.TemporaryDirectory() as temp:
  data=Path(temp)/'data';folder=data/'AWS_EC2_VPC';folder.mkdir(parents=True)
  a=json.loads(next((root/'AWS_EC2_VPC').glob('*.json')).read_text())
  b=json.loads(json.dumps(a).replace(a['resourceId'],'vpc-0b000000000000002'))
  for name,value in [('a',a),('b',b)]: (folder/(name+'.json')).write_text(json.dumps(value))
  connector=LocalFolderConnector(data);journal=RunJournal(Path(temp)/'journal')
  settings={**config,'processing':{'continue_on_step_error':continuation}}
  with Engine(settings,connectors=[connector]) as engine:
   def prepare():
    return engine.prepare(connector.name,CollectionContext(org_id=org,namespace=ns,selection={'dataset':'aws-local-demo'}),journal=journal)
   engine.replay(prepare(),journal=journal).require_complete()
   # B retains its own primary ARN but falsely claims A's native resource ID.
   b['resourceId']=a['resourceId'];b['payload']['VpcId']=a['resourceId']
   b['capturedAt']='2026-10-08T14:00:00Z'
   (folder/'b.json').write_text(json.dumps(b))
   volume=json.loads(next((root/'AWS_EC2_Volume').glob('*.json')).read_text())
   volume['capturedAt']='2026-10-08T14:00:00Z'
   (data/'AWS_EC2_Volume').mkdir();(data/'AWS_EC2_Volume'/'healthy.json').write_text(json.dumps(volume))
   prepared=prepare()
   if continuation:
    result=engine.replay(prepared,journal=journal)
    assert not result.complete and len(result['failed_snapshots'])==2,result
    assert result['newly_committed']['entities_created']==1,result
    counts=graph_counts()
    replay=engine.replay(prepared,journal=journal)
    assert not replay.complete and len(replay['failed_snapshots'])==2,replay
    assert not any(replay['newly_committed'].values()),replay
    assert graph_counts()==counts
   else:
    try: engine.replay(prepared,journal=journal)
    except IngestionError: pass
    else: raise AssertionError('fail-fast must reject the conflicting chunk')
   volumes=q('MATCH (e:Entity {org_id:$org,namespace:$ns,entity_type:"AWS::EC2::Volume"}) RETURN count(e)')
   assert volumes==[[int(continuation)]],volumes
   vpcs=q('MATCH (e:Entity {org_id:$org,namespace:$ns,entity_type:"AWS::EC2::VPC"}) RETURN count(e),collect(e.version)')
   assert vpcs==[[2,[1,1]]],vpcs
   print('PASS identity conflict:', 'healthy snapshot committed; failed receipts replayed' if continuation else 'fail-fast preserved; healthy snapshot not committed',ns,flush=True)
