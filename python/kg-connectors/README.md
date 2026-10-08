# Dynamic local resource connector

`LocalFolderConnector` discovers JSON files under a local export directory and sends their resource payloads to the Rust engine through `kg-sdk`. It makes no AWS API calls and contains no resource-specific API mappings, field-selection rules, ARN templates, or primary-key tables.

## One file format for every resource

```text
data/aws/
  AWS_EC2_Instance/i-0a000000000000001.json
  AWS_EC2_Volume/vol-0a000000000000001.json
  AWS_IAM_Role/orders-app.json
  ...
```

No manifest or file list is needed. The connector walks the folders automatically. The exporter provides the same identity envelope for every resource:

```json
{
  "resourceArn": "arn:aws:ec2:us-east-1:111122223333:instance/i-0a000000000000001",
  "resourceId": "i-0a000000000000001",
  "resourceType": "AWS::EC2::Instance",
  "accountId": "111122223333",
  "awsRegion": "us-east-1",
  "capturedAt": "2026-10-08T12:00:00Z",
  "payload": {
    "InstanceId": "i-0a000000000000001",
    "SubnetId": "subnet-0a000000000000001",
    "State": {"Code": 16, "Name": "running"}
  }
}
```

This envelope is our export contract, not a native EC2 response. The abbreviated payload above illustrates the format; actual files retain the complete individual resource object returned by the exporter. The connector does not truncate it or remove nested properties. Date/time values in JSON use ISO 8601 strings. Resource folders are organizational; `resourceType` determines the entity type.

Identity metadata must be preserved by your discovery/export script. Do not select an arbitrary ARN from inside a resource payload: it may identify a dependency rather than the resource itself. AWS Config's [common configuration fields](https://docs.aws.amazon.com/config/latest/APIReference/API_BaseConfigurationItem.html) are one possible source. AWS coverage and field availability vary; this reader requires the envelope fields above and never invents missing identifiers. It does not require Config to be enabled or make Config calls.

For account-global resources, use `"awsRegion": ""`. Optional `resourceName` controls the display name; otherwise it uses `resourceId`. Nonempty account and region components in the ARN must agree with the supplied scope. Local collection cannot authenticate metadata against an AWS account, so the export must be trusted.

## Automatic primary and additional keys

Every entity uses the same primary key: the supplied resource ARN, stored under the reserved `_kg_identity.arn` property.

The collector searches object properties for exact string equality with the supplied `resourceId`. Each matching property becomes an additional key with account and, when supplied, region scope:

- An instance ID matching `InstanceId` yields account + region + `InstanceId`.
- A volume ID matching `VolumeId` yields account + region + `VolumeId`.
- A new resource with its ID in `identity.customKey` works without a code change.

These are examples of one algorithm, not a mapping table. Arrays are preserved but not used for identity-key paths. Literal dotted/bracketed field names are not treated as identity paths. The common `_kg_identity.resource_id` plus scope is also retained as an additional key. Payload properties equal to the supplied own ARN become additional ARN keys.

If the resource ID is absent from the payload, ingestion retains the common ARN/ID identity and records `native_id_property_not_found` in collection diagnostics. It does not guess another field or split an ARN to manufacture an ID. Such resources can still be found by ARN; resolving native short references depends on the available identity evidence and the engine's matching rules.

The payload file stays unchanged. In memory, the connector adds `_kg_identity` and the existing `_astrolabe_scope` metadata to a copy of the payload. Existing properties with those reserved names are rejected rather than overwritten. Original export envelopes are preserved in the SDK journal.

Rust performs relationship discovery, deduplication, versioning, and persistence. No relationships are supplied by this connector. Ambiguous or unsupported matches remain unresolved.

## Run

Install the SDK and connectors using the root README. Export `NEO4J_URI`, `NEO4J_USER`, and `NEO4J_PASSWORD`, then run from the repository root:

```sh
python tools/ingest_local_folder.py python/kg-connectors/data/aws \
  --dataset aws-local-demo --namespace aws-local-demo
```

`dataset` is a shared collection label, not a resource configuration. This command registers the connector, collects files, writes the SDK journal, and invokes Rust ingestion. It retains the graph in Neo4j. `.local-journal/` is ignored by Git because it contains payloads. Each invocation gets a new run ID. SDK replay can reuse a prepared run without rereading files.

Exports must be immutable during collection. File/directory enumeration, resource count, file reads, total input bytes, cancellation, and deadlines are bounded. Symbolic links, invalid JSON, invalid envelopes, and observed file changes fail collection before ingestion. Empty folders produce no observations.

Each file must contain the complete current resource object. It is ingested as a full snapshot with complete property/reference coverage: an omitted property or reference is removed at that capture time, while earlier versions remain. Partial API projections must not be supplied as complete objects. No collection-generation authority is attached.

A complete folder read does **not** mean a complete AWS inventory. Missing files never authorize deletion. Explicit deletion requires `"lifecycle": "deleted"` in the envelope and the actual deletion observation time. Changed resource observations must have the correct later `capturedAt`. Merely rescanning an old export does not make its observation time newer.

## Synthetic AWS fixtures and validation

`data/aws/` contains 20 resource types: VPC, subnet, security group, instance, volume, Elastic IP, NAT gateway, RDS instance, IAM role/profile, KMS key, Lambda function, log group, ECR repository, DynamoDB table, SNS topic, SQS queue, secret metadata, load balancer, and target group.

IDs and contents are synthetic. Payloads follow AWS SDK response shapes, with timestamps serialized to strings. They are representative responses, not every optional field AWS could return. The secret fixture contains metadata only, never a secret value. The SQS example intentionally demonstrates a response without the short resource ID repeated in its payload.

The AWS model mapping in `tests/test_local_folder.py` exists only to validate fixtures; runtime collection does not import AWS SDKs or that mapping. Install the optional `kg-connectors[test]` dependencies to run those tests. Botocore is test-only.

AWS references:

- [EC2 response objects](https://docs.aws.amazon.com/boto3/latest/reference/services/ec2/client/describe_instances.html)
- [RDS DBInstance](https://docs.aws.amazon.com/AmazonRDS/latest/APIReference/API_DBInstance.html)
- [IAM Role](https://docs.aws.amazon.com/IAM/latest/APIReference/API_Role.html)
- [AWS SDK service reference](https://docs.aws.amazon.com/boto3/latest/reference/services/index.html)

## Repeatable live verification

With Neo4j environment variables exported, run `python tools/check_local_folder.py` from the repository root. It creates a unique namespace, then verifies repeated ingestion, replay, overlapping IDs across two accounts, three property versions, changed Lambda/IAM relationships, missing-file safety, and explicit deletion. Every run checks stored Neo4j state. Synthetic fixture copies and journals use a temporary directory; the graph remains available for inspection. No AWS or model calls are made.
