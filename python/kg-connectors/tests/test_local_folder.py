import json
from pathlib import Path
import shutil
import time

import pytest
from kg_sdk import CancellationToken, CollectionContext, CollectionLimits, CollectionError
from kg_connectors.local_folder import LocalFolderConnector

DATA = Path(__file__).resolve().parents[1] / 'data/aws'


def collect(root=DATA, **kwargs):
    context = CollectionContext(org_id='test-org', namespace='local-test',
        selection={'dataset':'aws-local-demo'}, **kwargs)
    return LocalFolderConnector(root).collect(context, cancellation=CancellationToken(), deadline=time.monotonic()+10)


def test_raw_payloads_and_keys_generated_without_manifest():
    batch = collect()
    assert len(batch.snapshots) == 20 and batch.complete and len(batch.diagnostics) == 1
    assert not (DATA/'inventory.json').exists()
    for snapshot, original in zip(batch.snapshots, batch.original_envelopes):
        entity = snapshot.entities[0]; exported = json.loads((DATA/original['file']).read_text())
        assert original['envelope'] == exported
        raw = dict(entity.raw_properties)
        scope={'account_id':exported['accountId']}
        if exported['awsRegion']:scope['region']=exported['awsRegion']
        assert raw.pop('_astrolabe_scope') == scope
        assert raw.pop('_kg_identity') == {'arn':exported['resourceArn'],'resource_id':exported['resourceId']}
        assert raw == exported['payload']
        assert entity.primary_key_properties == ['_kg_identity.arn']
        assert snapshot.snapshot_kind == "full" and snapshot.complete
        assert snapshot.collection is None and snapshot.sync_generation is None
        assert not snapshot.relationship_changes
    instance=next(s.entities[0] for s in batch.snapshots if s.entities[0].entity_type=='AWS::EC2::Instance')
    volume=next(s.entities[0] for s in batch.snapshots if s.entities[0].entity_type=='AWS::EC2::Volume')
    assert ['_astrolabe_scope.account_id','_astrolabe_scope.region','InstanceId'] in instance.additional_key_properties
    assert ['_astrolabe_scope.account_id','_astrolabe_scope.region','VolumeId'] in volume.additional_key_properties
    assert not any('SubnetId' in g for g in instance.additional_key_properties)


def test_fake_objects_match_aws_sdk_response_shapes():
    # Validation-only mapping. Runtime collection never imports an AWS SDK.
    from botocore.session import Session
    from botocore.validate import validate_parameters
    mappings={'VPC':('ec2','Vpc'),'Subnet':('ec2','Subnet'),'SecurityGroup':('ec2','SecurityGroup'),
        'Instance':('ec2','Instance'),'Volume':('ec2','Volume'),'EIP':('ec2','Address'),
        'NatGateway':('ec2','NatGateway'),'DBInstance':('rds','DBInstance'),
        'Role':('iam','Role'),'InstanceProfile':('iam','InstanceProfile'),'Key':('kms','KeyMetadata'),
        'Function':('lambda','FunctionConfiguration'),'LogGroup':('logs','LogGroup'),
        'Repository':('ecr','Repository'),'Table':('dynamodb','TableDescription'),
        'Topic':('sns','GetTopicAttributesResponse'),'Queue':('sqs','GetQueueAttributesResult'),
        'Secret':('secretsmanager','DescribeSecretResponse'),
        'LoadBalancer':('elbv2','LoadBalancer'),'TargetGroup':('elbv2','TargetGroup')}
    for path in DATA.rglob('*.json'):
        envelope=json.loads(path.read_text());service,shape=mappings[envelope['resourceType'].split('::')[-1]]
        validate_parameters(envelope['payload'],Session().get_service_model(service).shape_for(shape))


@pytest.mark.parametrize('change,reason',[
    ('symlink','resource_symlink'),('malformed','invalid_resource_json'),
    ('account','resource_arn_scope_mismatch'),('region','resource_arn_scope_mismatch'),
    ('reserved','invalid_resource_object'),('missing_arn','invalid_resource_envelope'),
    ('bad_arn','invalid_resource_arn'),('wildcard','invalid_resource_arn'),
])
def test_reject_bad_exports(tmp_path, change, reason):
    root=tmp_path/'data';shutil.copytree(DATA,root);p=sorted(root.rglob('*.json'))[0]
    e=json.loads(p.read_text())
    if change=='account':e['accountId']='999999999999'
    if change=='region':e['awsRegion']='eu-west-1'
    if change=='reserved':e['payload']['_kg_identity']={}
    if change=='missing_arn':del e['resourceArn']
    if change=='bad_arn':e['resourceArn']='not-an-arn'
    if change=='wildcard':e['resourceArn']+='*'
    p.write_text(json.dumps(e))
    if change=='symlink':p.unlink();p.symlink_to(next(DATA.rglob('*.json')))
    if change=='malformed':p.write_text('{"x":1,"x":2}')
    with pytest.raises(CollectionError,match=reason):collect(root)


def test_bounds_cancellation_and_no_deletion_authority():
    with pytest.raises(CollectionError,match='collection_limit'):collect(limits=CollectionLimits(max_records=2))
    with pytest.raises(CollectionError,match='collection_limit'):collect(limits=CollectionLimits(max_bytes=10))
    with pytest.raises(CollectionError,match='incremental'):collect(generation=1)
    token=CancellationToken();token.cancel()
    with pytest.raises(CollectionError,match='cancelled'):
        LocalFolderConnector(DATA).collect(CollectionContext(org_id='o',namespace='n',selection={'dataset':'aws-local-demo'}),cancellation=token,deadline=time.monotonic()+10)


def test_new_type_nested_id_and_global_scope_need_no_customization(tmp_path):
    (tmp_path/'Any_Type').mkdir()
    envelope={'resourceArn':'arn:aws:custom::111122223333:asset/asset-1','resourceId':'asset-1',
        'resourceType':'Custom::NewType','accountId':'111122223333','awsRegion':'',
        'capturedAt':'2026-10-08T12:00:00Z','lifecycle':'deleted',
        'payload':{'identity':{'unusualKey':'asset-1'},'references':[{'other':'asset-1'}]}}
    (tmp_path/'Any_Type/item.json').write_text(json.dumps(envelope))
    entity=collect(tmp_path).snapshots[0].entities[0]
    assert ['_astrolabe_scope.account_id','identity.unusualKey'] in entity.additional_key_properties
    assert not any('references' in '.'.join(g) for g in entity.additional_key_properties)
    assert 'region' not in entity.raw_properties['_astrolabe_scope']
    assert entity.lifecycle=='deleted'


def test_missing_native_id_is_reported_and_removed_files_never_delete(tmp_path):
    shutil.copytree(DATA,tmp_path/'data');root=tmp_path/'data';paths=sorted(root.rglob('*.json'))
    for path in paths[1:]:path.unlink()
    e=json.loads(paths[0].read_text());e['payload']={'Description':'no native identity here'};paths[0].write_text(json.dumps(e))
    batch=collect(root)
    assert len(batch.snapshots)==1 and len(batch.diagnostics)==1
    assert batch.diagnostics[0].startswith('native_id_property_not_found:')
    entity=batch.snapshots[0].entities[0]
    assert entity.primary_key_properties==['_kg_identity.arn']
    assert len(entity.additional_key_properties)==1
    paths[0].unlink();batch=collect(root)
    assert batch.snapshots==[]


def test_mutated_export_is_rejected(tmp_path,monkeypatch):
    import kg_connectors.local_folder as module
    shutil.copytree(DATA,tmp_path/'data');root=tmp_path/'data';path=sorted(root.rglob('*.json'))[0]
    original=module._entity
    def change(envelope,*args):
        path.write_text(path.read_text()+' ')
        return original(envelope,*args)
    monkeypatch.setattr(module,'_entity',change)
    with pytest.raises(CollectionError,match='export_changed_during_read'):collect(root)
