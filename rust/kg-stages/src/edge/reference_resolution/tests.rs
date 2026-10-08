use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use kg_core::{
    errors::{BackendError, StageError},
    models::{EntityNode, PropertyValue, SnapshotNode},
    runtime::{
        reference_resolution::{
            DecisionReason, EvidenceKind, EvidenceOrigin, ReferenceResolutionSettings,
        },
        stage_output::{PendingReference, ReferenceCandidate, ReferenceIntent, RelationshipTarget},
        RuntimeContext,
    },
    traits::*,
};
use serde_json::{json, Value};
use uuid::Uuid;

use super::decision::{
    answer_schema, parse_answer, system_prompt, Answer, PROMPT_VERSION, SCHEMA_VERSION,
};
use super::evidence::{
    hydrate_candidates, prepare, render, value_at, HydratedCandidate, Inputs, Preparation, Prepared,
};
use super::*;

// ---------------------------------------------------------------- fixtures

#[allow(clippy::too_many_arguments)]
fn stored_record(
    chain: Uuid,
    version_uuid: Uuid,
    version: u32,
    namespace: &str,
    entity_type: &str,
    name: &str,
    keys: &[&str],
    additional: &[&[&str]],
    properties: Value,
) -> EntityVersionRecord {
    let flat = PropertyValue::flatten_source(&properties, &[]).unwrap();
    let mut stored = serde_json::Map::new();
    for (key, value) in &flat {
        kg_core::traits::property_codec::write_property(&mut stored, key, Some(value));
    }
    stored.insert("primary_key_properties".into(), json!(keys));
    stored.insert(
        "additional_key_properties".into(),
        json!(serde_json::to_string(&additional).unwrap()),
    );
    stored.insert("labels".into(), json!(["fixture"]));
    stored.insert("tag_env".into(), json!("test"));
    EntityVersionRecord {
        uuid: version_uuid,
        chain_id: chain,
        version,
        is_latest: true,
        entity_type: entity_type.into(),
        name: name.into(),
        namespace: namespace.into(),
        source: Some("pilot".into()),
        identity_hash: None,
        identity_hashes: vec![],
        structural_hash: None,
        valid_from: None,
        valid_to: None,
        deleted_at: None,
        last_seen_at: Some(Utc.with_ymd_and_hms(2026, 9, 26, 12, 0, 0).unwrap()),
        last_transition_at: None,
        sync_generation: None,
        collections: vec![],
        merged_into: None,
        embedding: None,
        stored,
    }
}

fn bucket_record(chain: Uuid, version_uuid: Uuid) -> EntityVersionRecord {
    stored_record(
        chain,
        version_uuid,
        1,
        "ns",
        "AWS::S3::Bucket",
        "archive-bucket",
        &["Name"],
        &[],
        json!({"Name": "archive", "CreationDate": "2026-08-01T12:00:00Z", "BucketRegion": "us-east-1"}),
    )
}

fn role_record(chain: Uuid, version_uuid: Uuid) -> EntityVersionRecord {
    stored_record(
        chain,
        version_uuid,
        1,
        "ns",
        "AWS::IAM::Role",
        "archive-role",
        &["Arn"],
        &[&["RoleName"]],
        json!({"Arn": "arn:aws:iam::777788889999:role/archive", "RoleName": "archive", "Description": "control plane role", "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": [{"Effect": "Allow"}]}, "MaxSessionDuration": 3600}),
    )
}

fn candidate(record: &EntityVersionRecord, matched: &[&str]) -> ReferenceCandidate {
    ReferenceCandidate {
        target: RelationshipTarget::from(record.clone()),
        matched_key_group: matched.iter().map(|k| k.to_string()).collect(),
    }
}

fn source(properties: Value, extracted_by: &str, snapshot: Uuid) -> EntityNode {
    let mut entity = crate::node::entity_versioning::tests::test_entity("instance-a");
    entity.entity_type = "AWS::EC2::Instance".into();
    entity.primary_key_properties = vec!["InstanceId".into()];
    entity.all_properties = PropertyValue::flatten_source(&properties, &[]).unwrap();
    entity.extracted_by = extracted_by.into();
    entity.last_seen_snapshot_id = Some(snapshot);
    entity.last_seen_at = Some(Utc.with_ymd_and_hms(2026, 9, 26, 12, 23, 50).unwrap());
    entity.valid_from = entity.last_seen_at.unwrap();
    entity
}

fn instance_properties(tag_key: &str) -> Value {
    json!({
        "InstanceId": "i-0000000000aab0002",
        "State": {"Code": 16, "Name": "running"},
        "Tags": [{"Key": tag_key, "Value": "archive"}, {"Key": "Team", "Value": "archive"}],
        "Ports": [443, 8443],
        "Secret": {"Token": "do-not-send"},
        "weird.key": "literal-dot",
        "Nullable": null
    })
}

fn snapshot(
    uuid: Uuid,
    content: Option<&str>,
    data_type: kg_core::models::SnapshotDataType,
) -> SnapshotNode {
    SnapshotNode {
        uuid,
        org_id: "org".into(),
        namespace: "ns".into(),
        name: "observation".into(),
        source_description: None,
        data_type,
        snapshot_kind: kg_core::models::SnapshotKind::Incremental,
        sync_generation: None,
        complete: false,
        collection: None,
        source: "test".into(),
        content: content.map(str::to_owned),
        captured_at: Utc.with_ymd_and_hms(2026, 9, 26, 12, 23, 50).unwrap(),
        entities: vec![],
        entity_edges: vec![],
        labels: vec![],
        tags: Default::default(),
        created_at: Utc::now(),
    }
}

fn pending(
    source: EntityNode,
    location: &str,
    candidates: Vec<ReferenceCandidate>,
) -> PendingReference {
    let component = location
        .rsplit('.')
        .next()
        .unwrap()
        .split('[')
        .next()
        .unwrap();
    let components = vec![
        ("Key".into(), "s:BackupBucket".into()),
        (component.to_owned(), "s:archive".into()),
    ];
    let slot = format!(
        "{}.{}",
        source.entity_type,
        crate::edge::reference_extraction::path_without_indexes(location)
    );
    PendingReference {
        intent: ReferenceIntent {
            observing_chain_id: source.chain_id,
            observing_namespace: source.namespace.clone(),
            observing_entity_type: source.entity_type.clone(),
            producer_source: source.source.clone(),
            location: location.into(),
            slot,
            relationship_name: "RELATES_TO".into(),
            direction: kg_core::runtime::extraction::ReferenceDirection::SourceToTarget,
            cardinality: kg_core::runtime::extraction::ReferenceCardinality::Many,
            target_key_group: vec![],
            target_type: String::new(),
            components: components.clone(),
            allowed_namespaces: Some(vec!["ns".into()]),
            lookup_complete: true,
            policy_fingerprint: "generic-reference-v1".into(),
        },
        source,
        value: "archive".into(),
        components,
        candidates,
    }
}

/// A graph that answers exact-version reads for the records it holds and
/// refuses every other operation.
struct HydrationGraph {
    records: HashMap<Uuid, EntityVersionRecord>,
    failing: bool,
    snapshot_read: SnapshotRead,
    delay: Option<std::time::Duration>,
    /// Persisted decisions of "earlier runs", by reuse fingerprint.
    persisted: PersistedStore,
}

#[derive(Clone, Debug, Default)]
pub(super) enum PersistedStore {
    /// The backend has no decision store (trait default): reuse unavailable.
    #[default]
    Unsupported,
    Rows(Vec<kg_core::runtime::reference_resolution::PersistedDecision>),
    Failing,
}

/// How the graph answers a request for a snapshot's retained content.
#[derive(Clone, Debug)]
pub(super) enum SnapshotRead {
    /// The backend does not implement the read (trait default).
    Unsupported,
    /// Storage answered: nothing retained for that snapshot.
    Missing,
    Content(String),
    Denied,
}

fn no_graph<T>() -> Result<T, BackendError> {
    panic!("evidence assembly must not call storage beyond candidate hydration")
}

#[async_trait]
impl SearchBackend for HydrationGraph {}

#[async_trait]
impl GraphBackend for HydrationGraph {
    async fn identity_revisions(
        &self,
        _: &str,
        _: &[IdentityScope],
    ) -> Result<Vec<IdentityRevision>, BackendError> {
        no_graph()
    }
    async fn apply_mutations(&self, _: &str, _: &[GraphMutation]) -> Result<(), BackendError> {
        no_graph()
    }
    async fn register_run(&self, _: &RunHeader) -> Result<RunRegistration, BackendError> {
        no_graph()
    }
    async fn commit_batch(&self, _: &MutationBatch) -> Result<CommittedBatch, BackendError> {
        no_graph()
    }
    async fn committed_batches(
        &self,
        _: &str,
        _: Uuid,
    ) -> Result<Vec<CommittedBatch>, BackendError> {
        no_graph()
    }
    async fn snapshot_evidence(
        &self,
        org: &str,
        request: &kg_core::runtime::history::SnapshotEvidenceRequest,
    ) -> Result<Vec<kg_core::runtime::history::SnapshotEvidence>, BackendError> {
        assert_eq!(org, "org");
        assert_eq!(
            request.namespace, "ns",
            "content reads stay in the source namespace"
        );
        match &self.snapshot_read {
            SnapshotRead::Unsupported => {
                Err(BackendError::NotConfigured("no snapshot reads".into()))
            }
            SnapshotRead::Missing => Err(BackendError::Query(
                kg_core::runtime::history::EVIDENCE_UNAVAILABLE.into(),
            )),
            SnapshotRead::Denied => Err(BackendError::Auth("forbidden".into())),
            SnapshotRead::Content(text) => Ok(request
                .ids
                .iter()
                .map(|id| kg_core::runtime::history::SnapshotEvidence {
                    uuid: *id,
                    org_id: org.into(),
                    namespace: request.namespace.clone(),
                    source: "test".into(),
                    data_type: kg_core::models::SnapshotDataType::Text,
                    source_description: None,
                    captured_at: request.captured_before,
                    created_at: request.captured_before,
                    content: text.clone(),
                })
                .collect()),
        }
    }
    async fn reference_decisions_by_reuse_key(
        &self,
        org: &str,
        keys: &[String],
    ) -> Result<Vec<kg_core::runtime::reference_resolution::PersistedDecision>, BackendError> {
        assert_eq!(org, "org");
        match &self.persisted {
            PersistedStore::Unsupported => Err(BackendError::NotConfigured("no store".into())),
            PersistedStore::Failing => Err(BackendError::Unavailable("store down".into())),
            PersistedStore::Rows(rows) => Ok(rows
                .iter()
                .filter(|row| keys.contains(&row.audit.reuse_fingerprint))
                .cloned()
                .collect()),
        }
    }

    async fn find_entities(
        &self,
        org: &str,
        lookup: &EntityLookup,
    ) -> Result<Vec<EntityVersionRecord>, BackendError> {
        assert_eq!(org, "org", "reads stay organization scoped");
        if let Some(delay) = self.delay {
            tokio::time::sleep(delay).await;
        }
        if self.failing {
            return Err(BackendError::Timeout(5));
        }
        let EntityLookup::LatestByChain { chain_ids } = lookup else {
            panic!("hydration uses exact chain reads");
        };
        Ok(chain_ids
            .iter()
            .filter_map(|id| self.records.get(id).cloned())
            .collect())
    }
    async fn find_edges(&self, _: &str, _: &EdgeLookup) -> Result<Vec<EdgeRecord>, BackendError> {
        no_graph()
    }
    async fn health(&self) -> Result<(), BackendError> {
        Ok(())
    }
    async fn connect(&self) -> Result<(), BackendError> {
        Ok(())
    }
    async fn close(&self) -> Result<(), BackendError> {
        Ok(())
    }
}

fn context(records: Vec<EntityVersionRecord>, failing: bool) -> RuntimeContext {
    context_with(
        records,
        failing,
        SnapshotRead::Unsupported,
        None,
        PersistedStore::Unsupported,
    )
}

fn context_with(
    records: Vec<EntityVersionRecord>,
    failing: bool,
    snapshot_read: SnapshotRead,
    delay: Option<std::time::Duration>,
    persisted: PersistedStore,
) -> RuntimeContext {
    let llm = Arc::new(kg_core::test_support::MockLlmBackend::with_responses(
        vec![],
    ));
    kg_core::runtime::RuntimeContextBuilder::new("org")
        .graph(Arc::new(HydrationGraph {
            records: records.into_iter().map(|r| (r.chain_id, r)).collect(),
            failing,
            snapshot_read,
            delay,
            persisted,
        }))
        .llm_extraction(llm.clone())
        .llm_disambiguation(llm.clone())
        .llm_default(llm)
        .embedder(Arc::new(kg_core::test_support::MockEmbedBackend::new(4)))
        .build()
        .unwrap()
}

#[derive(Clone)]
pub(super) struct Fixture {
    pub(super) reference: PendingReference,
    pub(super) snapshot: SnapshotNode,
    hydrated: HashMap<Uuid, HydratedCandidate>,
    pub(super) settings: ReferenceResolutionSettings,
    guidance: Option<String>,
    prompt_budget: usize,
    context_window: usize,
    records: Vec<EntityVersionRecord>,
    pub(super) snapshot_read: SnapshotRead,
    hydration_delay: Option<std::time::Duration>,
    pub(super) persisted: PersistedStore,
}

impl Fixture {
    pub(super) async fn structured(tag_key: &str, location: &str) -> Self {
        let snapshot_id = Uuid::new_v4();
        let bucket = bucket_record(Uuid::from_u128(0xb), Uuid::from_u128(0xb1));
        let role = role_record(Uuid::from_u128(0xa), Uuid::from_u128(0xa1));
        let reference = pending(
            source(instance_properties(tag_key), "direct", snapshot_id),
            location,
            vec![
                candidate(&role, &["RoleName"]),
                candidate(&bucket, &["Name"]),
            ],
        );
        let ctx = context(vec![bucket.clone(), role.clone()], false);
        let hydrated = hydrate_candidates(
            &ctx,
            "reference_resolution",
            std::slice::from_ref(&reference),
        )
        .await
        .unwrap();
        Self {
            reference,
            snapshot: snapshot(
                snapshot_id,
                Some("{\"entities\":[]}"),
                kg_core::models::SnapshotDataType::Entities,
            ),
            hydrated,
            settings: ReferenceResolutionSettings::default(),
            guidance: None,
            prompt_budget: 64 * 1024,
            context_window: 128_000,
            records: vec![bucket, role],
            snapshot_read: SnapshotRead::Unsupported,
            hydration_delay: None,
            persisted: PersistedStore::Unsupported,
        }
    }

    /// The stage handoff for this fixture's single pending occurrence.
    pub(super) fn stage_input(&self) -> StageOutput {
        use kg_core::runtime::stage_output::{EdgeExtractionOutput, ReferenceReport};
        StageOutput::EdgeExtraction(EdgeExtractionOutput {
            relationship_times: Default::default(),
            reference_report: ReferenceReport {
                attempted: 1,
                unresolved: 1,
                ..Default::default()
            },
            relationship_directives: Default::default(),
            pending_references: Arc::new(vec![self.reference.clone()]),
            snapshot_nodes: Arc::new(vec![self.snapshot.clone()]),
            resolution: Default::default(),
            resolved_nodes: Default::default(),
            edges: Default::default(),
        })
    }

    /// A stage context that hydrates this fixture's candidates and consults `llm`.
    pub(super) fn stage_ctx(&self, llm: Arc<dyn LlmBackend>) -> RuntimeContext {
        use kg_core::policy::{EdgeAmbiguityMode, PipelinePolicy, PolicyResolver};
        let mut ctx = context_with(
            self.records.clone(),
            false,
            self.snapshot_read.clone(),
            self.hydration_delay,
            self.persisted.clone(),
        );
        ctx.llm_disambiguation = llm;
        ctx.reference_resolution_settings = self.settings.clone();
        ctx.policy = Arc::new(PolicyResolver::new(PipelinePolicy {
            edge_ambiguity: EdgeAmbiguityMode::Llm,
            ..Default::default()
        }));
        if let Some(guidance) = &self.guidance {
            ctx.extraction_settings.relationship_instructions = Some(guidance.clone());
        }
        ctx
    }

    fn prepare(&self) -> Preparation {
        self.prepare_with(
            json!({"provider": "fixture"}),
            "fixture-model",
            "reference-resolution-test",
        )
    }

    fn prepare_with(&self, descriptor: Value, model: &str, version: &str) -> Preparation {
        let schema = answer_schema();
        let prompt = system_prompt(&self.settings);
        prepare(
            &Inputs {
                reference: &self.reference,
                snapshot: &self.snapshot,
                hydrated: &self.hydrated,
                settings: &self.settings,
                guidance: self.guidance.as_deref(),
                model_id: model,
                provider_descriptor: &descriptor,
                processing_version: version,
                prompt_version: PROMPT_VERSION,
                schema_version: SCHEMA_VERSION,
                prompt_budget_bytes: self.prompt_budget,
                context_window: self.context_window,
                answer_schema: &schema,
                system_prompt: &prompt,
            },
            "reference_resolution",
        )
        .unwrap()
    }

    fn ready(&self) -> Box<Prepared> {
        match self.prepare() {
            Preparation::Ready(prepared) => prepared,
            Preparation::Refused { reason, .. } => panic!("unexpected refusal {reason:?}"),
        }
    }
}

fn item<'a>(prepared: &'a Prepared, id: &str) -> &'a Value {
    prepared.packet["evidence_items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == id)
        .unwrap_or_else(|| panic!("missing evidence item {id}"))
}

// ------------------------------------------------------------- evidence

#[tokio::test]
async fn structured_packet_is_complete_without_previous_content() {
    let fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    let prepared = fixture.ready();
    let packet = &prepared.packet;
    assert_eq!(prepared.origin, EvidenceOrigin::Structured);
    assert_eq!(
        packet["source_observation"]["evidence_origin"],
        "structured"
    );
    assert!(
        packet["source_observation"]["content"].is_null(),
        "structured input carries no text"
    );
    assert_eq!(
        packet["source_observation"]["snapshot_id"],
        json!(fixture.snapshot.uuid)
    );
    // Every flattened source property is present as an item, in flattened form.
    let properties = &packet["source_observation"]["entity"]["properties"];
    assert_eq!(properties["State.Code"], 16);
    assert_eq!(properties["Tags"][1]["Key"], "Team");
    assert_eq!(properties["Ports"], json!([443, 8443]));
    assert_eq!(properties["weird.key"], "literal-dot");
    assert!(
        properties.get("Nullable").is_some_and(Value::is_null),
        "an explicit null property is shown as null, not dropped"
    );
    assert!(prepared
        .manifest
        .iter()
        .any(|m| m.path.as_deref() == Some("Nullable")));
    assert_eq!(item(&prepared, "src:occ")["exact_path_resolved"], true);
    assert_eq!(item(&prepared, "src:occ")["value"], "archive");
    assert_eq!(item(&prepared, "src:occ")["path"], "Tags[0].Value");
    assert_eq!(
        item(&prepared, "src:ctx")["value"],
        json!({"Key": "BackupBucket", "Value": "archive"})
    );
    assert_eq!(item(&prepared, "src:ctx")["path"], "Tags[0]");
    assert_eq!(item(&prepared, "src:occ")["interpretation_only"], false);
    // Candidates come in chain order with complete stored properties and keys.
    let candidates = packet["candidates"].as_array().unwrap();
    assert_eq!(candidates.len(), 2);
    assert_eq!(candidates[0]["entity"]["entity_type"], "AWS::IAM::Role");
    assert_eq!(candidates[1]["entity"]["entity_type"], "AWS::S3::Bucket");
    assert_eq!(
        candidates[0]["entity"]["properties"]["Description"],
        "control plane role"
    );
    // Nested source objects stay in their flattened stored form; nothing is
    // rebuilt by splitting dotted names.
    assert_eq!(
        candidates[0]["entity"]["properties"]["AssumeRolePolicyDocument.Version"],
        "2012-10-17"
    );
    assert_eq!(
        candidates[0]["entity"]["properties"]["AssumeRolePolicyDocument.Statement"][0]["Effect"],
        "Allow"
    );
    assert_eq!(
        candidates[0]["entity"]["additional_key_properties"],
        json!([["RoleName"]])
    );
    assert_eq!(
        candidates[0]["entity"]["key_groups"][1][0]["property"],
        "RoleName"
    );
    assert_eq!(candidates[0]["entity"]["labels"], json!(["fixture"]));
    assert_eq!(candidates[0]["entity"]["tags"]["env"], "test");
    assert_eq!(candidates[1]["matched_key_group"], json!(["Name"]));
    let bucket_name = prepared
        .manifest
        .iter()
        .find(|m| m.id.starts_with("c2:p") && m.path.as_deref() == Some("Name"))
        .expect("bucket key property is an evidence item");
    assert_eq!(bucket_name.owner_chain_id, Uuid::from_u128(0xb));
    assert_eq!(item(&prepared, &bucket_name.id)["value"], "archive");
    assert!(prepared.manifest.iter().any(|m| m.id.starts_with("c1:p")
        && m.path.as_deref() == Some("Description")
        && m.owner_chain_id == Uuid::from_u128(0xa)));
    assert_eq!(packet["coverage"]["permitted_evidence_complete"], true);
    assert_eq!(packet["coverage"]["omitted_items"], json!([]));
    assert_eq!(packet["candidate_lookup"]["complete"], true);
    assert_eq!(packet["reference"]["value_type"], "s");
    assert_eq!(prepared.read_set.len(), 2);
    assert!(
        prepared
            .read_set
            .iter()
            .all(|read| read.observed_at.is_some()),
        "mutable-field fence captured"
    );
    assert!(prepared
        .manifest
        .iter()
        .all(|m| m.id.len() <= 32 && m.id.is_ascii()));
    assert!(
        !packet.to_string().contains("embedding\":["),
        "no vectors in evidence"
    );
}

#[tokio::test]
async fn text_source_needs_its_original_content_and_marks_interpretations() {
    let mut fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    fixture.reference.source.extracted_by = "llm:fixture".into();
    fixture.snapshot = snapshot(
        fixture.snapshot.uuid,
        Some("Instance i-0000000000aab0002 uses bucket archive for backups. The role named archive is unrelated."),
        kg_core::models::SnapshotDataType::Text,
    );
    let prepared = fixture.ready();
    assert_eq!(prepared.origin, EvidenceOrigin::TextExtracted);
    let text = item(&prepared, "src:text");
    assert_eq!(text["kind"], "text_excerpt");
    assert_eq!(text["start_char"], 0);
    assert_eq!(
        text["end_char"],
        fixture.snapshot.content.as_ref().unwrap().chars().count()
    );
    assert_eq!(text["interpretation_only"], false);
    assert_eq!(
        item(&prepared, "src:occ")["interpretation_only"],
        true,
        "extracted properties are interpretations"
    );
    assert_eq!(item(&prepared, "c1:p1")["interpretation_only"], false);
    let manifest = prepared.item("src:text").unwrap();
    assert_eq!(manifest.kind, EvidenceKind::TextExcerpt);
    assert_eq!(manifest.snapshot_uuid, Some(fixture.snapshot.uuid));

    // Same text-derived observation without retained content: refused, never fabricated.
    fixture.snapshot.content = None;
    match fixture.prepare() {
        Preparation::Refused { reason, prepared } => {
            assert_eq!(reason, DecisionReason::SourceContentUnavailable);
            assert!(prepared.item("src:text").is_none());
        }
        Preparation::Ready(_) => panic!("text origin without content must not be sent"),
    }

    // Unknown provenance (legacy repair) is neither structured nor text.
    fixture.reference.source.extracted_by = "stored".into();
    let prepared = fixture.ready();
    assert_eq!(prepared.origin, EvidenceOrigin::Unknown);
    assert_eq!(
        prepared.packet["source_observation"]["evidence_origin"],
        "unknown"
    );
    assert!(prepared.packet["source_observation"]["content"].is_null());
}

#[tokio::test]
async fn values_resolve_by_reference_path_grammar_including_escapes_and_arrays() {
    let properties =
        PropertyValue::flatten_source(&instance_properties("BackupBucket"), &[]).unwrap();
    assert_eq!(
        value_at(&properties, "Tags[0].Value"),
        Some(json!("archive"))
    );
    assert_eq!(
        value_at(&properties, "Tags[1]"),
        Some(json!({"Key": "Team", "Value": "archive"}))
    );
    assert_eq!(value_at(&properties, "State.Code"), Some(json!(16)));
    assert_eq!(value_at(&properties, "Ports[1]"), Some(json!(8443)));
    assert_eq!(
        value_at(&properties, "weird\\.key"),
        Some(json!("literal-dot"))
    );
    assert_eq!(
        value_at(&properties, "InstanceId"),
        Some(json!("i-0000000000aab0002"))
    );
    assert_eq!(value_at(&properties, "Tags[7].Value"), None);
    assert_eq!(
        value_at(&properties, "Tags[].Value"),
        None,
        "every-element paths are not one occurrence"
    );
    assert_eq!(value_at(&properties, "raw:Tags"), None);
    assert_eq!(value_at(&properties, "Missing.Path"), None);
}

#[tokio::test]
async fn array_members_are_isolated_and_change_the_decision_identity() {
    let first = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    let mut second = first.clone();
    second.reference.intent.location = "Tags[1].Value".into();
    let a = first.ready();
    let b = second.ready();
    assert_eq!(
        item(&b, "src:ctx")["value"],
        json!({"Key": "Team", "Value": "archive"})
    );
    assert_eq!(item(&b, "src:occ")["path"], "Tags[1].Value");
    assert_ne!(
        a.fingerprint, b.fingerprint,
        "a sibling element is different evidence"
    );
    let id = |p: &Prepared, location: &str| {
        kg_core::runtime::reference_resolution::decision_id(
            "org",
            first.reference.source.chain_id,
            p.snapshot_id,
            p.captured_at,
            "AWS::EC2::Instance.Tags.Value",
            location,
            "archive",
            &p.fingerprint,
        )
    };
    assert_ne!(id(&a, "Tags[0].Value"), id(&b, "Tags[1].Value"));
}

#[tokio::test]
async fn hydration_rejects_missing_stale_foreign_or_rekeyed_candidates() {
    let snapshot_id = Uuid::new_v4();
    let bucket = bucket_record(Uuid::from_u128(0xb), Uuid::from_u128(0xb1));
    let role = role_record(Uuid::from_u128(0xa), Uuid::from_u128(0xa1));
    let reference = pending(
        source(instance_properties("BackupBucket"), "direct", snapshot_id),
        "Tags[0].Value",
        vec![
            candidate(&role, &["RoleName"]),
            candidate(&bucket, &["Name"]),
        ],
    );
    let refs = std::slice::from_ref(&reference);
    let changed = |mutate: &dyn Fn(&mut EntityVersionRecord)| {
        let mut stored = bucket.clone();
        mutate(&mut stored);
        context(vec![stored, role.clone()], false)
    };
    // Missing candidate row.
    let ctx = context(vec![role.clone()], false);
    assert!(matches!(
        hydrate_candidates(&ctx, "s", refs).await,
        Err(StageError::IdentityRevisionChanged)
    ));
    // A newer version of the same chain.
    let ctx = changed(&|r| {
        r.uuid = Uuid::from_u128(0xb2);
        r.version = 2;
    });
    assert!(matches!(
        hydrate_candidates(&ctx, "s", refs).await,
        Err(StageError::IdentityRevisionChanged)
    ));
    // Same version identifiers in another namespace or organization scope.
    let ctx = changed(&|r| r.namespace = "other".into());
    assert!(matches!(
        hydrate_candidates(&ctx, "s", refs).await,
        Err(StageError::IdentityRevisionChanged)
    ));
    // Same version but its complete key group no longer matches discovery.
    let ctx = changed(&|r| {
        r.stored.insert("prop_Name".into(), json!("renamed"));
    });
    assert!(matches!(
        hydrate_candidates(&ctx, "s", refs).await,
        Err(StageError::IdentityRevisionChanged)
    ));
    // Deleted or merged rows are not live candidates.
    let ctx = changed(&|r| r.deleted_at = Some(Utc::now()));
    assert!(matches!(
        hydrate_candidates(&ctx, "s", refs).await,
        Err(StageError::IdentityRevisionChanged)
    ));
    let ctx = changed(&|r| r.merged_into = Some(Uuid::new_v4()));
    assert!(matches!(
        hydrate_candidates(&ctx, "s", refs).await,
        Err(StageError::IdentityRevisionChanged)
    ));
    // A read failure is a typed operational failure, never missing evidence.
    let ctx = context(vec![bucket.clone(), role.clone()], true);
    assert!(matches!(
        hydrate_candidates(&ctx, "s", refs).await,
        Err(StageError::StepFailed {
            retriable: true,
            ..
        })
    ));
    // The happy path keeps the observation clock for the mutable-field fence.
    let ctx = context(vec![bucket.clone(), role.clone()], false);
    let hydrated = hydrate_candidates(&ctx, "s", refs).await.unwrap();
    assert_eq!(hydrated.len(), 2);
    assert_eq!(hydrated[&bucket.chain_id].observed_at, bucket.last_seen_at);
    assert_eq!(
        hydrated[&bucket.chain_id].primary_key_properties,
        vec!["Name".to_string()]
    );
    assert_eq!(
        hydrated[&role.chain_id].additional_key_properties,
        vec![vec!["RoleName".to_string()]]
    );
}

#[tokio::test]
async fn same_version_with_changed_mutable_evidence_is_a_different_packet() {
    let fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    let before = fixture.ready();
    let mut mutated = fixture.clone();
    let role = mutated.hydrated.get_mut(&Uuid::from_u128(0xa)).unwrap();
    role.properties.insert(
        "Description".into(),
        PropertyValue::String("backup role".into()),
    );
    let after = mutated.ready();
    assert_eq!(before.read_set, after.read_set, "same versions read");
    assert_ne!(
        before.fingerprint, after.fingerprint,
        "changed descriptive evidence changes the decision identity"
    );
}

#[tokio::test]
async fn fingerprints_ignore_candidate_order_but_track_settings_and_guidance() {
    let fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    let a = fixture.ready();
    let mut reversed = fixture.clone();
    reversed.reference.candidates.reverse();
    let b = reversed.ready();
    assert_eq!(
        a.fingerprint, b.fingerprint,
        "candidate order is not evidence"
    );
    assert_eq!(
        a.packet, b.packet,
        "the rendered packet is order-independent too"
    );
    let first = render(&a, &system_prompt(&fixture.settings), None);
    let second = render(&a, &system_prompt(&fixture.settings), None);
    assert_eq!(
        first[1].content, second[1].content,
        "evidence framing is stable"
    );
    assert_eq!(
        kg_core::test_support::source_data_json(&first[1].content),
        a.packet
    );
    assert!(first[0].content.contains("Return only the four fields"));
    let mut guided = fixture.clone();
    guided.guidance = Some("Backup tags name destinations.".into());
    let g = guided.ready();
    assert_ne!(
        a.fingerprint, g.fingerprint,
        "guidance is decision evidence"
    );
    let rendered = render(
        &g,
        &system_prompt(&guided.settings),
        guided.guidance.as_deref(),
    );
    assert!(rendered[0]
        .content
        .contains("Backup tags name destinations."));
    assert!(
        rendered[0]
            .content
            .find("Return only the four fields")
            .unwrap()
            < rendered[0].content.find("Backup tags").unwrap(),
        "guidance stays subordinate"
    );
    let mut tighter = fixture.clone();
    tighter.settings.max_supporting_items = 2;
    assert_ne!(
        a.fingerprint,
        tighter.ready().fingerprint,
        "bounds shape the answer"
    );
    let mut other_tag = fixture.clone();
    other_tag.reference.source.all_properties =
        PropertyValue::flatten_source(&instance_properties("ReleaseLabel"), &[]).unwrap();
    assert_ne!(
        a.fingerprint,
        other_tag.ready().fingerprint,
        "source context is evidence"
    );
    assert_eq!(a.fingerprint.len(), 64);
}

#[tokio::test]
async fn oversized_evidence_is_refused_before_any_call_with_the_complete_packet_kept() {
    let mut fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    fixture.prompt_budget = 2048;
    match fixture.prepare() {
        Preparation::Refused { reason, prepared } => {
            assert_eq!(reason, DecisionReason::EvidenceBudgetExceeded);
            assert_eq!(
                prepared.packet["candidates"].as_array().unwrap().len(),
                2,
                "nothing was trimmed to fit"
            );
            assert_eq!(prepared.fingerprint.len(), 64);
        }
        Preparation::Ready(_) => panic!("a packet beyond the budget must not be sent"),
    }
    // A context window too small for the packet plus the output reserve refuses too.
    let mut fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    fixture.context_window = 3000;
    assert!(matches!(
        fixture.prepare(),
        Preparation::Refused {
            reason: DecisionReason::EvidenceBudgetExceeded,
            ..
        }
    ));
    // Many complete-key candidates overflow the same way.
    let mut crowded = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    crowded.prompt_budget = 6 * 1024;
    let base = role_record(Uuid::from_u128(0xa), Uuid::from_u128(0xa1));
    for n in 0..12u128 {
        let record = role_record(Uuid::from_u128(0x100 + n), Uuid::from_u128(0x200 + n));
        crowded
            .reference
            .candidates
            .push(candidate(&record, &["RoleName"]));
        let mut hydrated = crowded.hydrated[&base.chain_id].clone();
        hydrated.target = RelationshipTarget::from(record);
        crowded.hydrated.insert(hydrated.target.chain_id, hydrated);
    }
    assert!(matches!(
        crowded.prepare(),
        Preparation::Refused {
            reason: DecisionReason::EvidenceBudgetExceeded,
            ..
        }
    ));
}

#[tokio::test]
async fn restrictions_inside_whole_values_withhold_the_enclosing_property() {
    // The store keeps arrays whole, so a restriction that points inside an
    // element (`Tags.Key`) can only be honored by withholding the whole `Tags`
    // value and the enclosing element; the occurrence value itself stays.
    let mut fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    let open = fixture.ready();
    fixture.settings.disclosure_restricted_paths = vec!["Tags.Key".into()];
    let prepared = fixture.ready();
    let text = prepared.packet.to_string();
    assert!(
        !text.contains("BackupBucket"),
        "array-internal restriction leaked"
    );
    assert!(
        prepared.item("src:ctx").is_none(),
        "the enclosing element carries the restricted field"
    );
    assert_eq!(item(&prepared, "src:occ")["value"], "archive");
    let omitted = prepared.packet["coverage"]["omitted_items"]
        .as_array()
        .unwrap();
    assert!(omitted
        .iter()
        .any(|o| o["path"] == "Tags" && o["owner_id"] == json!(fixture.reference.source.chain_id)));
    assert_ne!(open.fingerprint, prepared.fingerprint);
    // A restriction inside a candidate's whole value withholds that value too.
    fixture.settings.disclosure_restricted_paths =
        vec!["AssumeRolePolicyDocument.Statement.Effect".into()];
    let prepared = fixture.ready();
    assert!(!prepared.packet.to_string().contains("Allow"));
    assert!(prepared
        .omitted
        .iter()
        .any(|o| o.path == "AssumeRolePolicyDocument.Statement"));
}

#[tokio::test]
async fn disclosure_restrictions_withhold_paths_and_refuse_a_restricted_occurrence() {
    let mut fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    fixture.settings.disclosure_restricted_paths =
        vec!["Secret".into(), "AssumeRolePolicyDocument".into()];
    let prepared = fixture.ready();
    let text = prepared.packet.to_string();
    assert!(
        !text.contains("do-not-send"),
        "restricted source property never rendered"
    );
    assert!(
        !text.contains("2012-10-17"),
        "restricted candidate property never rendered"
    );
    assert!(
        prepared.packet["source_observation"]["entity"]["properties"]
            .get("Secret.Token")
            .is_none()
    );
    let omitted = prepared.packet["coverage"]["omitted_items"]
        .as_array()
        .unwrap();
    // One restriction prefix covers every flattened path beneath it.
    assert_eq!(omitted.len(), 3);
    assert!(omitted
        .iter()
        .all(|o| o["reason"] == "disclosure_restricted"));
    assert!(omitted.iter().any(|o| o["path"] == "Secret.Token"));
    assert!(omitted
        .iter()
        .any(|o| o["path"] == "AssumeRolePolicyDocument.Version"));
    assert!(omitted
        .iter()
        .any(|o| o["path"] == "AssumeRolePolicyDocument.Statement"));
    assert!(prepared
        .manifest
        .iter()
        .all(|m| m.path.as_deref() != Some("Secret.Token")));
    assert_eq!(prepared.omitted.len(), 3);
    let unrestricted = {
        let mut open = fixture.clone();
        open.settings.disclosure_restricted_paths.clear();
        open.ready()
    };
    assert_ne!(
        unrestricted.fingerprint, prepared.fingerprint,
        "disclosure state is part of the identity"
    );
    // Discovery exclusions are not disclosure restrictions: nothing configured, nothing withheld.
    assert!(unrestricted.packet.to_string().contains("do-not-send"));

    // An unmatched identity key is still evidence: redact its alternative
    // representation in key_groups as well as the ordinary properties map.
    fixture.settings.disclosure_restricted_paths = vec!["Arn".into()];
    let redacted = fixture.ready();
    assert!(!redacted
        .packet
        .to_string()
        .contains("arn:aws:iam::777788889999:role/archive"));
    // A restriction on the matched identity produces explicit uncertainty,
    // never a stage error or a reduced candidate set.
    fixture.settings.disclosure_restricted_paths = vec!["RoleName".into()];
    assert!(matches!(
        fixture.prepare(),
        Preparation::Refused {
            reason: DecisionReason::EvidenceUnavailable,
            ..
        }
    ));

    // The occurrence itself under a restriction cannot be proven: refused, zero calls.
    fixture.settings.disclosure_restricted_paths = vec!["Tags".into()];
    match fixture.prepare() {
        Preparation::Refused { reason, prepared } => {
            assert_eq!(reason, DecisionReason::EvidenceUnavailable);
            assert!(prepared.item("src:occ").is_none());
            assert!(!prepared.packet.to_string().contains("BackupBucket"));
        }
        Preparation::Ready(_) => panic!("a restricted occurrence has no showable proof"),
    }
}

#[tokio::test]
async fn packets_bind_to_the_producing_snapshot_only() {
    let mut fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    fixture.snapshot.uuid = Uuid::new_v4();
    let schema = answer_schema();
    let prompt = system_prompt(&fixture.settings);
    let descriptor = json!({});
    let result = prepare(
        &Inputs {
            reference: &fixture.reference,
            snapshot: &fixture.snapshot,
            hydrated: &fixture.hydrated,
            settings: &fixture.settings,
            guidance: None,
            model_id: "m",
            provider_descriptor: &descriptor,
            processing_version: "v",
            prompt_version: PROMPT_VERSION,
            schema_version: SCHEMA_VERSION,
            prompt_budget_bytes: 65_536,
            context_window: 128_000,
            answer_schema: &schema,
            system_prompt: &prompt,
        },
        "reference_resolution",
    );
    assert!(
        matches!(result, Err(StageError::StateValidation { .. })),
        "unrelated content is never borrowed"
    );
    let bound = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    assert_eq!(
        bound.ready().source_version_uuid,
        bound.reference.source.uuid
    );
}

/// A chunk whose worst-case audits could not be committed by planning is
/// refused before any attempt: the caps planning enforces (2048 audits, 4 MiB)
/// are applied against the prepared occurrences with zero calls.
#[tokio::test]
async fn chunks_that_cannot_commit_their_audits_are_refused_before_any_call() {
    use kg_core::runtime::stage_output::EdgeExtractionOutput;
    let fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    let many = |n: usize| {
        let StageOutput::EdgeExtraction(base) = fixture.stage_input() else {
            unreachable!()
        };
        let mut pending = Vec::with_capacity(n);
        for i in 0..n {
            let mut reference = fixture.reference.clone();
            reference.intent.location = format!("Tags[{i}].Value");
            pending.push(reference);
        }
        StageOutput::EdgeExtraction(EdgeExtractionOutput {
            pending_references: Arc::new(pending),
            ..base
        })
    };
    // Too many audits for one batch.
    let llm = mock(&[accept_json(Uuid::from_u128(0xb), &["src:occ"])]);
    let error = ReferenceResolutionStage
        .process(many(2049), &fixture.stage_ctx(llm.clone()))
        .await
        .unwrap_err();
    assert!(
        matches!(&error, StageError::StateValidation { message, .. } if message.contains("2049 reference decisions") && message.contains("no provider attempt")),
        "{error}"
    );
    assert_eq!(llm.call_count(), 0);
    // Fewer audits, but their worst-case bytes exceed the batch byte cap.
    let error = ReferenceResolutionStage
        .process(many(1500), &fixture.stage_ctx(llm.clone()))
        .await
        .unwrap_err();
    assert!(
        matches!(&error, StageError::StateValidation { message, .. } if message.contains("worst-case audit size")),
        "{error}"
    );
    assert_eq!(llm.call_count(), 0);
    // A chunk that fits is decided normally.
    let llm = mock(&[REJECT_JSON.into()]);
    let StageOutput::EdgeExtraction(output) = ReferenceResolutionStage
        .process(many(40), &fixture.stage_ctx(llm.clone()))
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(output.reference_report.decisions.len(), 40);
    // Tags[0] and Tags[1] have distinct enclosing elements; Tags[2..] resolve
    // to no element at all, so they share one context and one verdict.
    assert_eq!(llm.call_count(), 3);
    assert_eq!(
        output
            .reference_report
            .decisions
            .iter()
            .filter(|d| d.reused)
            .count(),
        37
    );
}

#[tokio::test]
async fn bounded_calls_refuse_backends_without_a_declared_attempt_contract() {
    use kg_core::traits::llm_backend::{CallBudget, LlmMessage, MessageRole};
    struct Unbounded;
    #[async_trait]
    impl LlmBackend for Unbounded {
        async fn complete(
            &self,
            _: &[LlmMessage],
            _: Option<&Value>,
            _: Option<u32>,
        ) -> Result<kg_core::traits::llm_backend::LlmResponse, BackendError> {
            panic!("an undeclared backend must never be dispatched to")
        }
        fn model_id(&self) -> &str {
            "unbounded"
        }
        fn context_window(&self) -> usize {
            100_000
        }
    }
    let ctx = context(vec![], false);
    let messages = vec![LlmMessage {
        role: MessageRole::User,
        content: "x".into(),
    }];
    let budget = CallBudget::single();
    let semaphore = tokio::sync::Semaphore::new(1);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let error = crate::node::extraction_support::call_provider_bounded(
        &ctx,
        "reference_resolution",
        &messages,
        &answer_schema(),
        &Unbounded,
        &semaphore,
        deadline,
        64,
        &budget,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, StageError::StateValidation { .. }),
        "{error}"
    );
    assert_eq!(budget.consumed(), 0);
    // A declaring backend spends exactly one attempt and reports the served model.
    let mock = kg_core::test_support::MockLlmBackend::with_responses(vec![
        r#"{"decision":"unsure","target_id":null,"fact":null,"supporting_evidence_ids":[]}"#.into(),
    ]);
    let response = crate::node::extraction_support::call_provider_bounded(
        &ctx,
        "reference_resolution",
        &messages,
        &answer_schema(),
        &mock,
        &semaphore,
        deadline,
        64,
        &budget,
    )
    .await
    .unwrap();
    assert_eq!(response.model, "mock");
    assert_eq!((budget.consumed(), budget.remaining()), (1, 0));
    assert_eq!(mock.call_count(), 1);
    let spent = crate::node::extraction_support::call_provider_bounded(
        &ctx,
        "reference_resolution",
        &messages,
        &answer_schema(),
        &mock,
        &semaphore,
        deadline,
        64,
        &budget,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            spent,
            StageError::ModelCall {
                kind: kg_core::errors::stage::ModelFailureKind::Configuration,
                ..
            }
        ),
        "{spent}"
    );
    assert_eq!(mock.call_count(), 1, "a spent allowance dispatches nothing");
}

// -------------------------------------------------------------- answers

fn accept(target: Uuid, ids: &[&str]) -> String {
    json!({"decision": "accept", "target_id": target, "fact": "The instance tags bucket archive as its backup bucket.", "supporting_evidence_ids": ids}).to_string()
}

#[tokio::test]
async fn answers_are_validated_against_the_rendered_packet() {
    let fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    let prepared = fixture.ready();
    let source = fixture.reference.source.chain_id;
    let bucket = Uuid::from_u128(0xb);
    let parse = |raw: &str| parse_answer(raw, &prepared, source, &fixture.settings);
    let Answer::Accept {
        target,
        fact,
        citations,
    } = parse(&accept(bucket, &["src:ctx", "c2:p1"])).unwrap()
    else {
        panic!("valid acceptance")
    };
    assert_eq!(target, bucket);
    assert!(fact.starts_with("The instance"));
    assert_eq!(citations.len(), 2);
    assert_eq!(citations[0].owner_chain_id, source);
    assert_eq!(citations[0].path.as_deref(), Some("Tags[0]"));
    assert_eq!(citations[1].owner_chain_id, bucket);
    assert_eq!(
        parse(r#"{"decision":"reject","target_id":null,"fact":null,"supporting_evidence_ids":[]}"#)
            .unwrap(),
        Answer::Reject
    );
    assert_eq!(
        parse(r#"{"decision":"unsure","target_id":null,"fact":null,"supporting_evidence_ids":[]}"#)
            .unwrap(),
        Answer::Unsure
    );
    // The validator proves existence and ownership, not meaning: the bare
    // occurrence value is source-owned and therefore satisfies source support
    // on its own. Distinguishing that from a coincidental own-name match is the
    // prompt's job and is tested adversarially in Phase 3.
    assert!(parse(&accept(bucket, &["src:occ", "c2:p1"])).is_ok());
    for (case, raw) in [
        ("target outside the frozen set", accept(Uuid::from_u128(0x77), &["src:ctx", "c2:p1"])),
        ("target metadata alone", accept(bucket, &["c2:p1", "c2:p2"])),
        ("unknown evidence id", accept(bucket, &["src:ctx", "nope"])),
        ("repeated evidence id", accept(bucket, &["src:ctx", "src:ctx"])),
        ("too many citations", accept(bucket, &["src:ctx", "src:p1", "src:p2", "src:p3", "c2:p1"])),
        ("no citations", accept(bucket, &[])),
        ("accept without fact", json!({"decision":"accept","target_id":bucket,"fact":null,"supporting_evidence_ids":["src:ctx"]}).to_string()),
        ("fact too long", json!({"decision":"accept","target_id":bucket,"fact":"x".repeat(241),"supporting_evidence_ids":["src:ctx"]}).to_string()),
        ("reject with a target", json!({"decision":"reject","target_id":bucket,"fact":null,"supporting_evidence_ids":[]}).to_string()),
        ("unsure with evidence", json!({"decision":"unsure","target_id":null,"fact":null,"supporting_evidence_ids":["src:ctx"]}).to_string()),
        ("extra explanation field", json!({"decision":"reject","target_id":null,"fact":null,"supporting_evidence_ids":[],"reason":"because"}).to_string()),
        ("evidence request variant", json!({"decision":"needs_more_evidence","target_id":null,"fact":null,"supporting_evidence_ids":[]}).to_string()),
        ("missing field", json!({"decision":"reject","target_id":null,"fact":null}).to_string()),
        ("index instead of id", json!({"decision":"accept","target_id":"1","fact":"f","supporting_evidence_ids":["src:ctx"]}).to_string()),
        ("truncated", "{\"decision\":\"acc".into()),
        ("not json", "the bucket".into()),
    ] {
        assert!(parse(&raw).is_err(), "{case} must not become a decision");
    }
}

#[test]
fn answer_schema_is_strict_mode_friendly_and_prompt_is_generic() {
    let schema = answer_schema();
    assert_eq!(schema["additionalProperties"], false);
    assert_eq!(
        schema["required"],
        json!(["decision", "fact", "supporting_evidence_ids", "target_id"]),
        "property order, so strict sanitizers leave the schema unchanged"
    );
    assert_eq!(schema["properties"].as_object().unwrap().len(), 4);
    let text = schema.to_string();
    for keyword in [
        "maxLength",
        "maxItems",
        "minItems",
        "minLength",
        "pattern",
        "format",
    ] {
        assert!(
            !text.contains(keyword),
            "{keyword} is unsupported by strict providers; the host enforces it"
        );
    }
    assert_eq!(
        schema["properties"]["decision"]["enum"],
        json!(["accept", "reject", "unsure"])
    );
    let prompt = system_prompt(&ReferenceResolutionSettings::default());
    assert!(prompt.contains("at most 240 characters"));
    assert!(prompt.contains("between 1 and 4 distinct evidence ids"));
    for domain_word in [
        "AWS",
        "bucket",
        "RoleName",
        "BackupBucket",
        "Kubernetes",
        "GCP",
    ] {
        assert!(
            !prompt.contains(domain_word),
            "generic prompt must not carry {domain_word}"
        );
    }
    assert!(prompt.contains("no tools, follow-up questions or further evidence"));
    assert!(prompt.contains("membership record may genuinely reference"));
}

// ------------------------------------------------------- compatibility

#[test]
fn durable_contracts_default_for_data_written_before_decisions_existed() {
    use kg_core::runtime::stage_output::{BatchRecovery, ReferenceReport};
    let report: ReferenceReport = serde_json::from_value(json!({
        "attempted": 3, "confirmed": 1, "unresolved": 2, "excluded": 0, "incomplete_sources": []
    }))
    .unwrap();
    assert!(report.decisions.is_empty());
    let recovery: BatchRecovery = serde_json::from_value(json!({
        "community_checkpoint": null, "saga_summary_manifest": null, "saga_associations": [],
        "reused_snapshots": [], "summary_affected_chains": [], "summary_manifest": null,
        "nodes": [], "failures": [], "observed_relationships": []
    }))
    .unwrap();
    assert!(recovery.reference_decisions.is_empty());
    let output: kg_core::pipeline::PipelineOutput = serde_json::from_value(json!({
        "run_id": Uuid::nil(), "committed": serde_json::to_value(kg_core::pipeline::CommittedCounts::default()).unwrap(),
        "newly_committed": serde_json::to_value(kg_core::pipeline::CommittedCounts::default()).unwrap(),
        "batches": [], "snapshots_total": 0, "snapshots_completed": 0, "failed_snapshots": [], "duration_ms": 0
    }))
    .unwrap();
    assert!(output.reference_decisions.is_empty());
    let evidence: kg_core::models::edges::ReferenceEvidence = serde_json::from_value(json!({
        "component_paths": null, "observing_chain_id": Uuid::from_u128(1), "observing_namespace": "ns",
        "slot": "T.a", "location": "a", "target_key_group": ["id"],
        "read_set": [{"chain_id": Uuid::from_u128(2), "version_uuid": Uuid::from_u128(3), "version": 1}]
    }))
    .unwrap();
    assert!(evidence.decision.is_none());
    assert!(evidence.read_set[0].observed_at.is_none());
    // Settings absent from an older configuration take their defaults; unknown keys stay rejected.
    let settings: crate::IngestionSettings =
        serde_json::from_value(json!({"recipe": "declared_and_references"})).unwrap();
    assert_eq!(
        settings.reference_resolution,
        ReferenceResolutionSettings::default()
    );
    assert!(serde_json::from_value::<crate::IngestionSettings>(
        json!({"reference_resolution": {"bogus": 1}})
    )
    .is_err());
    assert!(crate::IngestionSettings {
        reference_resolution: ReferenceResolutionSettings {
            timeout_ms: 0,
            ..Default::default()
        },
        ..Default::default()
    }
    .validate()
    .is_err());
}

// --------------------------------------------- existing stage behaviour

#[test]
fn confirmation_preserves_context_tokens_and_other_observations() {
    use kg_core::{runtime::stage_output::UnresolvedSlot, traits::UnresolvedReferenceEntry};
    let source = uuid::Uuid::new_v4();
    let snapshot = uuid::Uuid::new_v4();
    let at = chrono::Utc::now();
    let entries = ["s:region", "s:target"]
        .into_iter()
        .map(|token| UnresolvedReferenceEntry {
            token: token.into(),
            reason: "multiple-candidates".into(),
            snapshot_id: Some(snapshot),
            recorded_at: at,
        })
        .collect();
    let current = UnresolvedSlot {
        source_chain_id: source,
        slot: "targets".into(),
        decided_at: at,
        decision_id: snapshot,
        entries,
    };
    let mut later = current.clone();
    later.decided_at += chrono::Duration::seconds(1);
    let mut decisions = vec![later.clone(), current];
    clear_confirmed_occurrence(&mut decisions, source, "targets", at, snapshot, "s:target");
    assert_eq!(decisions[0], later);
    assert_eq!(decisions[1].entries.len(), 1);
    assert_eq!(decisions[1].entries[0].token, "s:region");
}

// ------------------------------------------------------------ the stage

pub(super) fn mock(responses: &[String]) -> Arc<kg_core::test_support::MockLlmBackend> {
    Arc::new(kg_core::test_support::MockLlmBackend::with_responses(
        responses.to_vec(),
    ))
}

pub(super) fn accept_json(target: Uuid, ids: &[&str]) -> String {
    json!({"decision": "accept", "target_id": target, "fact": "The instance tags the bucket as its backup bucket.", "supporting_evidence_ids": ids}).to_string()
}
pub(super) const REJECT_JSON: &str =
    r#"{"decision":"reject","target_id":null,"fact":null,"supporting_evidence_ids":[]}"#;
pub(super) const UNSURE_JSON: &str =
    r#"{"decision":"unsure","target_id":null,"fact":null,"supporting_evidence_ids":[]}"#;

pub(super) async fn run_stage(
    fixture: &Fixture,
    ctx: &RuntimeContext,
) -> Result<kg_core::runtime::stage_output::EdgeExtractionOutput, StageError> {
    match ReferenceResolutionStage
        .process(fixture.stage_input(), ctx)
        .await?
    {
        StageOutput::EdgeExtraction(output) => Ok(output),
        _ => panic!("edge extraction output"),
    }
}

#[tokio::test]
async fn one_call_decides_accept_reject_or_unsure_and_leaves_one_audit() {
    use kg_core::runtime::reference_resolution::{DecisionOutcome, DecisionReason};
    let bucket = Uuid::from_u128(0xb);
    let fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    for (answer, outcome, reason) in [
        (
            accept_json(bucket, &["src:ctx", "c2:p3"]),
            DecisionOutcome::Accepted,
            DecisionReason::ModelAccepted,
        ),
        (
            REJECT_JSON.into(),
            DecisionOutcome::Rejected,
            DecisionReason::ModelRejected,
        ),
        (
            UNSURE_JSON.into(),
            DecisionOutcome::Unsure,
            DecisionReason::ModelUnsure,
        ),
    ] {
        let llm = mock(&[answer]);
        let recorded = crate::tests::prompt_contract::RecordingModel::new(llm.clone());
        let ctx = fixture.stage_ctx(recorded.clone());
        let output = run_stage(&fixture, &ctx).await.unwrap();
        assert_eq!(llm.call_count(), 1, "exactly one provider attempt");
        let requests = recorded.requests();
        let schema = requests[0].1.as_ref().unwrap();
        let targets = schema["properties"]["target_id"]["enum"]
            .as_array()
            .unwrap();
        assert!(targets.contains(&json!(bucket)) && targets.contains(&Value::Null));
        assert!(!targets.contains(&json!(Uuid::from_u128(0x77))));
        let ids = schema["properties"]["supporting_evidence_ids"]["items"]["enum"]
            .as_array()
            .unwrap();
        assert!(ids.contains(&json!("src:ctx")) && ids.contains(&json!("c2:p3")));
        assert!(!ids.contains(&json!("nope")));

        assert!(output.pending_references.is_empty());
        assert_eq!(output.reference_report.decisions.len(), 1);
        let audit = &output.reference_report.decisions[0];
        assert_eq!(audit.outcome, outcome);
        assert_eq!(audit.reason, reason);
        assert_eq!(audit.provider_attempts, 1);
        assert_eq!(audit.model_served.as_deref(), Some("mock"));
        assert_eq!(audit.model_configured, "mock-llm");
        assert_eq!(audit.source_snapshot_id, fixture.snapshot.uuid);
        assert_eq!(audit.candidate_read_set.len(), 2);
        assert!(audit
            .candidate_read_set
            .iter()
            .all(|r| r.observed_at.is_some()));
        assert!(!audit.reused);
        audit.validate(fixture.settings.max_audit_bytes).unwrap();
        let accepted = outcome == DecisionOutcome::Accepted;
        assert_eq!(output.edges.len(), usize::from(accepted));
        assert_eq!(output.reference_report.confirmed, usize::from(accepted));
        assert_eq!(output.reference_report.unresolved, usize::from(!accepted));
        if accepted {
            let edge = &output.edges[0];
            assert_eq!(edge.target_chain_id, bucket);
            assert_eq!(edge.discovered_by.as_deref(), Some("llm_fk_disambiguation"));
            let evidence = edge.reference_evidence.as_ref().unwrap();
            assert_eq!(evidence.decision.as_ref(), Some(audit));
            assert_eq!(evidence.read_set, audit.candidate_read_set);
            assert_eq!(audit.supporting_evidence[1].path.as_deref(), Some("Name"));
            assert!(
                !edge.description.contains("backup bucket") && edge.all_properties.is_empty(),
                "the fact stays audit provenance, never semantic content"
            );
        }
    }
}

#[tokio::test]
async fn host_refusals_leave_unsure_audits_with_zero_calls() {
    use kg_core::runtime::reference_resolution::{DecisionOutcome, DecisionReason};
    // Text-derived observation whose original content is not retained.
    let mut fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    fixture.reference.source.extracted_by = "llm:fixture".into();
    fixture.snapshot.content = None;
    fixture.snapshot_read = SnapshotRead::Missing;
    let llm = mock(&[accept_json(Uuid::from_u128(0xb), &["src:occ"])]);
    let output = run_stage(&fixture, &fixture.stage_ctx(llm.clone()))
        .await
        .unwrap();
    assert_eq!(llm.call_count(), 0);
    let audit = &output.reference_report.decisions[0];
    assert_eq!(
        (audit.outcome, audit.reason),
        (
            DecisionOutcome::Unsure,
            DecisionReason::SourceContentUnavailable
        )
    );
    assert_eq!(audit.provider_attempts, 0);
    assert!(output.edges.is_empty());
    assert_eq!(output.reference_report.unresolved, 1);
    // Complete evidence that does not fit the prompt budget.
    let mut fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    fixture.settings.max_prompt_bytes = 1024;
    let llm = mock(&[accept_json(Uuid::from_u128(0xb), &["src:occ"])]);
    let output = run_stage(&fixture, &fixture.stage_ctx(llm.clone()))
        .await
        .unwrap();
    assert_eq!(llm.call_count(), 0);
    assert_eq!(
        output.reference_report.decisions[0].reason,
        DecisionReason::EvidenceBudgetExceeded
    );
    // An audit that could not be recorded is refused before the attempt, not
    // after: the refusal record fits, the worst-case accepted record would not.
    let mut fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    fixture.settings.max_audit_bytes = 2048;
    let llm = mock(&[accept_json(Uuid::from_u128(0xb), &["src:occ"])]);
    let output = run_stage(&fixture, &fixture.stage_ctx(llm.clone()))
        .await
        .unwrap();
    assert_eq!(
        llm.call_count(),
        0,
        "no attempt when the audit cannot be recorded"
    );
    assert_eq!(
        output.reference_report.decisions[0].reason,
        DecisionReason::EvidenceBudgetExceeded
    );
    // When even the refusal record cannot be kept, the stage fails explicitly.
    fixture.settings.max_audit_bytes = 512;
    let error = run_stage(&fixture, &fixture.stage_ctx(llm.clone()))
        .await
        .unwrap_err();
    assert!(
        matches!(error, StageError::StateValidation { .. }),
        "{error}"
    );
    assert_eq!(llm.call_count(), 0);
    // The occurrence itself is disclosure-restricted: nothing to show.
    let mut fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    fixture.settings.disclosure_restricted_paths = vec!["Tags".into()];
    let llm = mock(&[accept_json(Uuid::from_u128(0xb), &["src:occ"])]);
    let output = run_stage(&fixture, &fixture.stage_ctx(llm.clone()))
        .await
        .unwrap();
    assert_eq!(llm.call_count(), 0);
    assert_eq!(
        output.reference_report.decisions[0].reason,
        DecisionReason::EvidenceUnavailable
    );
}

#[tokio::test]
async fn invalid_answers_fail_typed_without_correction_and_are_not_paid_twice() {
    let bucket = Uuid::from_u128(0xb);
    let fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    for (case, answer) in [
        ("malformed", "not json at all".to_owned()),
        ("evidence request", json!({"decision":"needs_more_evidence","target_id":null,"fact":null,"supporting_evidence_ids":[]}).to_string()),
        ("fabricated evidence", accept_json(bucket, &["src:ctx", "made-up"])),
        ("unoffered target", accept_json(Uuid::new_v4(), &["src:ctx"])),
        ("target metadata alone", accept_json(bucket, &["c2:p3", "c1:p1"])),
    ] {
        let llm = mock(&[answer, accept_json(bucket, &["src:ctx"])]);
        let ctx = fixture.stage_ctx(llm.clone());
        let error = run_stage(&fixture, &ctx).await.unwrap_err();
        assert_eq!(llm.call_count(), 1, "{case}: no correction call");
        assert!(matches!(error, StageError::StepFailed { .. }), "{case}: {error}");
        // Re-entering the stage with identical evidence in the same run must
        // not dispatch again: the consumed attempt is remembered.
        let again = run_stage(&fixture, &ctx).await.unwrap_err();
        assert_eq!(llm.call_count(), 1, "{case}: replan did not pay twice");
        assert!(matches!(again, StageError::StepFailed { retriable: false, .. }), "{case}: {again}");
    }
}

#[tokio::test]
async fn transport_failures_stay_typed_with_a_single_attempt() {
    use kg_core::errors::stage::ModelFailureKind;
    let fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    let llm = Arc::new(kg_core::test_support::MockLlmBackend::failing());
    let ctx = fixture.stage_ctx(llm.clone());
    let error = run_stage(&fixture, &ctx).await.unwrap_err();
    assert!(
        matches!(
            error,
            StageError::ModelCall {
                kind: ModelFailureKind::Unavailable,
                ..
            }
        ),
        "{error}"
    );
    assert_eq!(llm.call_count(), 1, "no retry or fallback");
    assert!(run_stage(&fixture, &ctx).await.is_err());
    assert_eq!(
        llm.call_count(),
        1,
        "a failed attempt is never repeated in the run"
    );
}

#[tokio::test]
async fn identical_evidence_reuses_the_decision_and_changed_evidence_pays_once_more() {
    let bucket = Uuid::from_u128(0xb);
    let fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    let llm = mock(&[
        accept_json(bucket, &["src:ctx", "c2:p3"]),
        REJECT_JSON.into(),
    ]);
    let ctx = fixture.stage_ctx(llm.clone());
    let first = run_stage(&fixture, &ctx).await.unwrap();
    // The same observation seen again under a new snapshot at the same capture time.
    let mut again = fixture.clone();
    again.snapshot.uuid = Uuid::new_v4();
    again.reference.source.last_seen_snapshot_id = Some(again.snapshot.uuid);
    let second = run_stage(&again, &ctx).await.unwrap();
    assert_eq!(
        llm.call_count(),
        1,
        "identical evidence is decided once per run"
    );
    let (a, b) = (
        &first.reference_report.decisions[0],
        &second.reference_report.decisions[0],
    );
    assert!(b.reused && !a.reused);
    assert_ne!(
        a.decision_id, b.decision_id,
        "rebound to the new observation"
    );
    assert_eq!(b.source_snapshot_id, again.snapshot.uuid);
    assert_eq!(b.evidence_fingerprint, a.evidence_fingerprint);
    assert_eq!(
        (b.model_served.clone(), b.provider_attempts),
        (a.model_served.clone(), 1)
    );
    assert_eq!(second.edges[0].target_chain_id, bucket);
    // Changed guidance is different evidence: the next answer (reject) is used.
    let mut guided = fixture.clone();
    guided.guidance = Some("Backup tags name destinations.".into());
    let ctx_guided = {
        let mut c = guided.stage_ctx(llm.clone());
        c.reference_decisions = ctx.reference_decisions.clone();
        c
    };
    let third = run_stage(&guided, &ctx_guided).await.unwrap();
    assert_eq!(llm.call_count(), 2);
    assert!(third.edges.is_empty());
    assert!(!third.reference_report.decisions[0].reused);
    // A changed candidate property the verdict did not cite is a different
    // packet but not evidence the verdict rested on: the run's own verdict is
    // rebound under its reuse key, no call.
    let mut mutated = fixture.clone();
    for record in &mut mutated.records {
        if record.chain_id == Uuid::from_u128(0xa) {
            record
                .stored
                .insert("prop_Description".into(), json!("backup role"));
        }
    }
    let uncited = mock(&[UNSURE_JSON.into()]);
    let ctx_mutated = {
        let mut c = mutated.stage_ctx(uncited.clone());
        c.reference_decisions = ctx.reference_decisions.clone();
        c
    };
    let fourth = run_stage(&mutated, &ctx_mutated).await.unwrap();
    assert_eq!(uncited.call_count(), 0);
    assert!(fourth.reference_report.decisions[0].reused);
    assert_eq!(
        fourth.reference_report.decisions[0].reused_from,
        Some(a.decision_id)
    );
    assert_ne!(
        fourth.reference_report.decisions[0].evidence_fingerprint,
        a.evidence_fingerprint
    );
    // A changed cited value (the enclosing tag element the model cited as
    // src:ctx) is different evidence: decided again.
    let mut recited = fixture.clone();
    let mut props = instance_properties("BackupBucket");
    props["Tags"][0]["Key"] = json!("BackupBucketRetired");
    recited.reference.source.all_properties = PropertyValue::flatten_source(&props, &[]).unwrap();
    let paying = mock(&[UNSURE_JSON.into()]);
    let ctx_recited = {
        let mut c = recited.stage_ctx(paying.clone());
        c.reference_decisions = ctx.reference_decisions.clone();
        c
    };
    let fifth = run_stage(&recited, &ctx_recited).await.unwrap();
    assert_eq!(paying.call_count(), 1);
    assert_eq!(
        fifth.reference_report.decisions[0].outcome,
        kg_core::runtime::reference_resolution::DecisionOutcome::Unsure
    );
}

#[tokio::test]
async fn refusals_before_dispatch_cover_policy_backend_and_cancellation() {
    use kg_core::policy::{PipelinePolicy, PolicyResolver};
    let fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    // Source policy without model ambiguity: a pending reference is a contract violation.
    let llm = mock(&[REJECT_JSON.into()]);
    let mut ctx = fixture.stage_ctx(llm.clone());
    ctx.policy = Arc::new(PolicyResolver::new(PipelinePolicy::default()));
    assert!(matches!(
        run_stage(&fixture, &ctx).await,
        Err(StageError::StateValidation { .. })
    ));
    // A backend that does not declare bounded attempts is refused before hydration.
    struct Unbounded;
    #[async_trait]
    impl LlmBackend for Unbounded {
        async fn complete(
            &self,
            _: &[llm_backend::LlmMessage],
            _: Option<&Value>,
            _: Option<u32>,
        ) -> Result<llm_backend::LlmResponse, BackendError> {
            panic!("never dispatched")
        }
        fn model_id(&self) -> &str {
            "unbounded"
        }
        fn context_window(&self) -> usize {
            100_000
        }
    }
    let ctx = fixture.stage_ctx(Arc::new(Unbounded));
    assert!(matches!(
        run_stage(&fixture, &ctx).await,
        Err(StageError::StateValidation { .. })
    ));
    // Cancellation before the call stops new work; nothing is dispatched or cached.
    let ctx = fixture.stage_ctx(llm.clone());
    ctx.cancel.cancel();
    assert!(matches!(
        run_stage(&fixture, &ctx).await,
        Err(StageError::Cancelled { .. })
    ));
    assert_eq!(llm.call_count(), 0);
    assert!(
        ctx.reference_decisions.is_empty(),
        "an abandoned key leaves no record"
    );
    // Nothing pending: no hydration, no model, no audit.
    let mut empty = fixture.stage_input();
    if let StageOutput::EdgeExtraction(output) = &mut empty {
        output.pending_references = Default::default();
    }
    let ctx = fixture.stage_ctx(llm.clone());
    let StageOutput::EdgeExtraction(output) =
        ReferenceResolutionStage.process(empty, &ctx).await.unwrap()
    else {
        panic!()
    };
    assert!(output.reference_report.decisions.is_empty());
    assert_eq!(llm.call_count(), 0);
}

#[tokio::test]
async fn concurrent_identical_occurrences_share_one_attempt() {
    let bucket = Uuid::from_u128(0xb);
    let fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    let llm = mock(&[accept_json(bucket, &["src:ctx"])]);
    let ctx = Arc::new(fixture.stage_ctx(llm.clone()));
    let fixture = Arc::new(fixture);
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let (fixture, ctx) = (fixture.clone(), ctx.clone());
        tasks.push(tokio::spawn(async move {
            match ReferenceResolutionStage
                .process(fixture.stage_input(), &ctx)
                .await
                .unwrap()
            {
                StageOutput::EdgeExtraction(output) => output.reference_report.decisions[0].clone(),
                _ => panic!(),
            }
        }));
    }
    let mut audits = Vec::new();
    for task in tasks {
        audits.push(task.await.unwrap());
    }
    assert_eq!(
        llm.call_count(),
        1,
        "single flight across concurrent identical packets"
    );
    assert!(audits
        .iter()
        .all(|a| a.outcome == kg_core::runtime::reference_resolution::DecisionOutcome::Accepted));
    assert_eq!(audits.iter().filter(|a| !a.reused).count(), 1);
}

#[tokio::test]
async fn a_hydrated_chain_that_is_not_offered_for_this_occurrence_is_never_accepted() {
    // The role is a stored chain the graph can hydrate, but discovery did not
    // offer it for this occurrence; naming it is an unoffered target.
    let mut fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    fixture
        .reference
        .candidates
        .retain(|c| c.target.chain_id == Uuid::from_u128(0xb));
    let llm = mock(&[accept_json(Uuid::from_u128(0xa), &["src:ctx"])]);
    let ctx = fixture.stage_ctx(llm.clone());
    let error = run_stage(&fixture, &ctx).await.unwrap_err();
    assert!(matches!(error, StageError::StepFailed { .. }), "{error}");
    assert_eq!(llm.call_count(), 1);
    assert!(run_stage(&fixture, &ctx).await.is_err());
    assert_eq!(llm.call_count(), 1, "the spent attempt is remembered");
}

#[tokio::test]
async fn original_content_reads_are_scoped_and_never_disguise_failures() {
    use kg_core::runtime::reference_resolution::DecisionReason;
    let bucket = Uuid::from_u128(0xb);
    let mut fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    fixture.reference.source.extracted_by = "llm:fixture".into();
    fixture.snapshot.content = None;
    // Storage says nothing is retained: explicit uncertainty, zero calls.
    fixture.snapshot_read = SnapshotRead::Missing;
    let llm = mock(&[accept_json(bucket, &["src:text"])]);
    let output = run_stage(&fixture, &fixture.stage_ctx(llm.clone()))
        .await
        .unwrap();
    assert_eq!(
        output.reference_report.decisions[0].reason,
        DecisionReason::SourceContentUnavailable
    );
    assert_eq!(llm.call_count(), 0);
    // A permission failure or an unsupported read is an operational failure, not missing evidence.
    for read in [SnapshotRead::Denied, SnapshotRead::Unsupported] {
        fixture.snapshot_read = read.clone();
        let error = run_stage(&fixture, &fixture.stage_ctx(llm.clone()))
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                StageError::StepFailed {
                    retriable: false,
                    ..
                }
            ),
            "{read:?}: {error}"
        );
        assert_eq!(llm.call_count(), 0);
    }
    // Retained content is read by the authorized snapshot identity and becomes the text item.
    fixture.snapshot_read =
        SnapshotRead::Content("instance i-0000000000aab0002 backs up to archive".into());
    let output = run_stage(&fixture, &fixture.stage_ctx(llm.clone()))
        .await
        .unwrap();
    assert_eq!(llm.call_count(), 1);
    let audit = &output.reference_report.decisions[0];
    assert_eq!(
        audit.outcome,
        kg_core::runtime::reference_resolution::DecisionOutcome::Accepted
    );
    assert_eq!(audit.supporting_evidence[0].kind, EvidenceKind::TextExcerpt);
    assert_eq!(
        audit.supporting_evidence[0].snapshot_uuid,
        Some(fixture.snapshot.uuid)
    );
    let mut without = fixture.clone();
    without.snapshot_read = SnapshotRead::Missing;
    let refused = run_stage(&without, &without.stage_ctx(mock(&[])))
        .await
        .unwrap();
    assert_ne!(
        refused.reference_report.decisions[0].evidence_fingerprint, audit.evidence_fingerprint,
        "content is part of the evidence identity"
    );
}

/// A backend that spends its attempt and then never answers.
struct Hanging {
    calls: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl LlmBackend for Hanging {
    async fn complete(
        &self,
        _: &[llm_backend::LlmMessage],
        _: Option<&Value>,
        _: Option<u32>,
    ) -> Result<llm_backend::LlmResponse, BackendError> {
        std::future::pending().await
    }
    async fn complete_bounded(
        &self,
        _: &[llm_backend::LlmMessage],
        _: Option<&Value>,
        _: Option<u32>,
        budget: &llm_backend::CallBudget,
    ) -> Result<llm_backend::LlmResponse, BackendError> {
        budget.consume()?;
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        std::future::pending().await
    }
    fn supports_bounded_attempts(&self) -> bool {
        true
    }
    fn model_id(&self) -> &str {
        "hanging"
    }
    fn context_window(&self) -> usize {
        1_000_000
    }
}

#[tokio::test]
async fn cancellation_and_deadlines_release_or_settle_the_key() {
    use std::sync::atomic::Ordering;
    let bucket = Uuid::from_u128(0xb);
    let fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    // Cancelled while waiting for a provider permit: nothing dispatched, key released.
    let llm = mock(&[accept_json(bucket, &["src:ctx"])]);
    let mut ctx = fixture.stage_ctx(llm.clone());
    ctx.llm_disambiguation_semaphore = Arc::new(tokio::sync::Semaphore::new(0));
    let cancel = ctx.cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        cancel.cancel();
    });
    assert!(matches!(
        run_stage(&fixture, &ctx).await,
        Err(StageError::Cancelled { .. })
    ));
    assert_eq!(llm.call_count(), 0);
    assert!(
        ctx.reference_decisions.is_empty(),
        "an undispatched key is released"
    );

    // Cancelled during the call: the attempt was spent, so the key settles as failed
    // and identical evidence never dispatches again in this run.
    let hanging = Arc::new(Hanging {
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let ctx = fixture.stage_ctx(hanging.clone());
    let cancel = ctx.cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        cancel.cancel();
    });
    assert!(matches!(
        run_stage(&fixture, &ctx).await,
        Err(StageError::Cancelled { .. })
    ));
    assert_eq!(hanging.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        ctx.reference_decisions.len(),
        1,
        "the consumed attempt is recorded"
    );
    let mut again = fixture.stage_ctx(hanging.clone());
    again.reference_decisions = ctx.reference_decisions.clone();
    assert!(matches!(
        run_stage(&fixture, &again).await,
        Err(StageError::StepFailed {
            retriable: false,
            ..
        })
    ));
    assert_eq!(
        hanging.calls.load(Ordering::SeqCst),
        1,
        "no second dispatch"
    );

    // Hydration deadline: a slow graph fails typed and retriable before any call.
    let mut slow = fixture.clone();
    slow.settings.timeout_ms = 20;
    slow.hydration_delay = Some(std::time::Duration::from_millis(500));
    let llm = mock(&[accept_json(bucket, &["src:ctx"])]);
    let error = run_stage(&slow, &slow.stage_ctx(llm.clone()))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            StageError::StepFailed {
                retriable: true,
                ..
            }
        ),
        "{error}"
    );
    assert_eq!(llm.call_count(), 0);

    // Model deadline shared by a leader and a waiter: one attempt, both fail typed.
    let mut short = fixture.clone();
    short.settings.timeout_ms = 200;
    let hanging = Arc::new(Hanging {
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let ctx = Arc::new(short.stage_ctx(hanging.clone()));
    let short = Arc::new(short);
    let (a, b) = tokio::join!(
        {
            let (f, c) = (short.clone(), ctx.clone());
            async move { ReferenceResolutionStage.process(f.stage_input(), &c).await }
        },
        {
            let (f, c) = (short.clone(), ctx.clone());
            async move { ReferenceResolutionStage.process(f.stage_input(), &c).await }
        }
    );
    assert!(a.is_err() && b.is_err());
    assert_eq!(
        hanging.calls.load(Ordering::SeqCst),
        1,
        "single flight under the deadline"
    );
}

#[tokio::test]
async fn a_failed_occurrence_stops_further_dispatch_in_the_snapshot() {
    let fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    let mut second = fixture.reference.clone();
    second.intent.location = "Tags[1].Value".into();
    let mut input = fixture.stage_input();
    if let StageOutput::EdgeExtraction(output) = &mut input {
        output.pending_references = Arc::new(vec![fixture.reference.clone(), second]);
        output.reference_report.unresolved = 2;
    }
    let llm = Arc::new(kg_core::test_support::MockLlmBackend::failing());
    let ctx = fixture.stage_ctx(llm.clone());
    assert!(ReferenceResolutionStage.process(input, &ctx).await.is_err());
    assert_eq!(
        llm.call_count(),
        1,
        "a provider failure stops the snapshot before the next occurrence"
    );
}

/// The persisted decisions a completed stage pass would write, from its report.
fn persisted_from(
    output: &kg_core::runtime::stage_output::EdgeExtractionOutput,
) -> Vec<kg_core::runtime::reference_resolution::PersistedDecision> {
    output
        .reference_report
        .decisions
        .iter()
        .filter(|a| !a.reused && !a.reason.is_host_refusal())
        .map(|audit| {
            let context = output
                .reference_report
                .decision_contexts
                .iter()
                .find(|c| c.decision_id == audit.decision_id)
                .expect("every original model decision carries its context");
            kg_core::runtime::reference_resolution::PersistedDecision {
                audit: audit.clone(),
                producer_source: context.producer_source.clone(),
                observing_namespace: context.observing_namespace.clone(),
                source_entity_type: context.source_entity_type.clone(),
                target_type: context.target_type.clone(),
                components: context.components.clone(),
                reference_tokens: context.reference_tokens.clone(),
                candidates: context.candidates.clone(),
                reuse_count: 0,
                last_reused_at: None,
            }
        })
        .collect()
}

/// Decision memory: a decision persisted by an earlier run is rebound to a new
/// occurrence with unchanged evidence without any provider attempt; volatile
/// properties do not break the key, changed context or cited values do, and an
/// opted-out or unavailable store dispatches as before.
#[tokio::test]
async fn persisted_decisions_are_rebound_across_runs_and_invalidated_by_changed_evidence() {
    use kg_core::runtime::reference_resolution::{DecisionOutcome, DecisionReason};
    let bucket = Uuid::from_u128(0xb);
    let fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    let llm = mock(&[accept_json(bucket, &["src:ctx", "c2:p3"])]);
    let first = run_stage(&fixture, &fixture.stage_ctx(llm.clone()))
        .await
        .unwrap();
    assert_eq!(llm.call_count(), 1);
    let original = first.reference_report.decisions[0].clone();
    assert_eq!(original.reuse_fingerprint.len(), 64);
    assert!(
        !original.cited_value_hashes.is_empty(),
        "an acceptance records the cited values"
    );
    let store = persisted_from(&first);
    assert_eq!(store.len(), 1);
    assert_eq!(store[0].target_type.as_deref(), Some("AWS::S3::Bucket"));
    assert_eq!(store[0].candidates.len(), 2);

    // A later run: new snapshot and capture, a volatile source property changed.
    let mut later = fixture.clone();
    later.snapshot.uuid = Uuid::new_v4();
    later.reference.source.last_seen_snapshot_id = Some(later.snapshot.uuid);
    later.reference.source.uuid = Uuid::new_v4();
    later
        .reference
        .source
        .all_properties
        .insert("State.Name".into(), PropertyValue::String("stopped".into()));
    later.persisted = PersistedStore::Rows(store.clone());
    let quiet = mock(&[]);
    let second = run_stage(&later, &later.stage_ctx(quiet.clone()))
        .await
        .unwrap();
    assert_eq!(quiet.call_count(), 0, "persisted hit: no provider attempt");
    let rebound = &second.reference_report.decisions[0];
    assert!(rebound.reused);
    assert_eq!(rebound.reused_from, Some(original.decision_id));
    assert_eq!(rebound.reuse_fingerprint, original.reuse_fingerprint);
    assert_ne!(rebound.decision_id, original.decision_id);
    assert_ne!(
        rebound.evidence_fingerprint, original.evidence_fingerprint,
        "volatile change: different packet, same key"
    );
    assert_eq!(rebound.source_version_uuid, later.reference.source.uuid);
    assert_eq!(rebound.model_served, original.model_served);
    assert_eq!(rebound.provider_attempts, 1);
    assert_eq!(second.edges.len(), 1);
    assert_eq!(second.edges[0].target_chain_id, bucket);
    assert!(
        second.reference_report.decision_contexts.is_empty(),
        "a rebinding is not persisted as a new record"
    );

    // A cited value changed since the decision: no reuse, one new attempt.
    let mut changed = store.clone();
    changed[0].audit.cited_value_hashes[0].sha256 = "0".repeat(64);
    let mut stale = later.clone();
    stale.persisted = PersistedStore::Rows(changed);
    let paying = mock(&[REJECT_JSON.into()]);
    let third = run_stage(&stale, &stale.stage_ctx(paying.clone()))
        .await
        .unwrap();
    assert_eq!(paying.call_count(), 1);
    assert!(!third.reference_report.decisions[0].reused);

    // Different occurrence context (another tag key): different key, no hit.
    let other = Fixture::structured("ReleaseLabel", "Tags[0].Value").await;
    let mut other = other;
    other.persisted = PersistedStore::Rows(store.clone());
    let paying = mock(&[REJECT_JSON.into()]);
    let fourth = run_stage(&other, &other.stage_ctx(paying.clone()))
        .await
        .unwrap();
    assert_eq!(paying.call_count(), 1);
    assert_ne!(
        fourth.reference_report.decisions[0].reuse_fingerprint,
        original.reuse_fingerprint
    );

    // Opted out: the store is not consulted.
    let mut opted_out = later.clone();
    opted_out.settings.persisted_reuse = false;
    let paying = mock(&[REJECT_JSON.into()]);
    run_stage(&opted_out, &opted_out.stage_ctx(paying.clone()))
        .await
        .unwrap();
    assert_eq!(paying.call_count(), 1);

    // A failing store is an operational failure before any attempt, never a
    // silent miss that pays.
    let mut broken = later.clone();
    broken.persisted = PersistedStore::Failing;
    let paying = mock(&[REJECT_JSON.into()]);
    let error = run_stage(&broken, &broken.stage_ctx(paying.clone()))
        .await
        .unwrap_err();
    assert!(
        matches!(error, StageError::StepFailed { ref step, .. } if step == "persisted_decisions"),
        "{error}"
    );
    assert_eq!(paying.call_count(), 0);

    // Unsure persisted verdicts are never frozen: they dispatch again.
    let mut unsure_store = store.clone();
    unsure_store[0].audit.outcome = DecisionOutcome::Unsure;
    unsure_store[0].audit.reason = DecisionReason::ModelUnsure;
    unsure_store[0].audit.target_chain_id = None;
    unsure_store[0].audit.fact = None;
    unsure_store[0].audit.supporting_evidence.clear();
    unsure_store[0].audit.cited_value_hashes.clear();
    let mut unsure = later.clone();
    unsure.persisted = PersistedStore::Rows(unsure_store);
    let paying = mock(&[REJECT_JSON.into()]);
    run_stage(&unsure, &unsure.stage_ctx(paying.clone()))
        .await
        .unwrap();
    assert_eq!(paying.call_count(), 1);
}

/// The reuse key ignores where a call was routed and every volatile property,
/// and changes with anything the answer depends on.
#[tokio::test]
async fn reuse_keys_ignore_routing_and_volatile_properties_but_track_evidence() {
    let fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;
    let a = fixture.ready();
    let ready_with = |descriptor: Value, model: &str, version: &str| match fixture
        .prepare_with(descriptor, model, version)
    {
        Preparation::Ready(p) => p,
        Preparation::Refused { .. } => panic!(),
    };
    let routed = ready_with(
        json!({"provider": "fixture", "route_identity": "http://127.0.0.1:54321/v1", "endpoint": "x", "context_window": 1, "default_max_tokens": 2}),
        "fixture-model",
        "reference-resolution-test",
    );
    assert_ne!(
        ready_with(
            json!({"provider": "fixture", "reasoning_effort": "high"}),
            "fixture-model",
            "reference-resolution-test"
        )
        .reuse_fingerprint,
        a.reuse_fingerprint,
        "reasoning effort decides"
    );
    assert_ne!(
        ready_with(
            json!({"provider": "fixture"}),
            "other-model",
            "reference-resolution-test"
        )
        .reuse_fingerprint,
        a.reuse_fingerprint,
        "the model decides"
    );
    assert_ne!(
        ready_with(
            json!({"provider": "fixture"}),
            "fixture-model",
            "reference-resolution-next"
        )
        .reuse_fingerprint,
        a.reuse_fingerprint,
        "the processing version decides"
    );
    let mut reordered = fixture.clone();
    reordered.reference.candidates.reverse();
    assert_eq!(
        reordered.ready().reuse_fingerprint,
        a.reuse_fingerprint,
        "candidate order is not evidence"
    );
    let mut restricted = fixture.clone();
    restricted.settings.disclosure_restricted_paths = vec!["Secret".into()];
    assert_ne!(
        restricted.ready().reuse_fingerprint,
        a.reuse_fingerprint,
        "restrictions decide what was shown"
    );
    let mut policy = fixture.clone();
    policy.reference.intent.policy_fingerprint = "other-policy".into();
    assert_ne!(policy.ready().reuse_fingerprint, a.reuse_fingerprint);
    let mut renamed = fixture.clone();
    renamed
        .hydrated
        .get_mut(&Uuid::from_u128(0xa))
        .unwrap()
        .target
        .name = "another display name".into();
    assert_ne!(
        renamed.ready().reuse_fingerprint,
        a.reuse_fingerprint,
        "a shown candidate name is evidence"
    );
    let mut rehomed = fixture.clone();
    rehomed
        .hydrated
        .get_mut(&Uuid::from_u128(0xa))
        .unwrap()
        .target
        .namespace = "staging".into();
    assert_ne!(rehomed.ready().reuse_fingerprint, a.reuse_fingerprint);
    assert_eq!(
        a.reuse_fingerprint, routed.reuse_fingerprint,
        "routing is not evidence"
    );
    assert_ne!(
        a.fingerprint, routed.fingerprint,
        "the exact packet fingerprint still tracks it"
    );
    let mut volatile = fixture.clone();
    volatile
        .reference
        .source
        .all_properties
        .insert("State.Name".into(), PropertyValue::String("stopped".into()));
    volatile.snapshot.uuid = Uuid::new_v4();
    volatile.reference.source.last_seen_snapshot_id = Some(volatile.snapshot.uuid);
    assert_eq!(volatile.ready().reuse_fingerprint, a.reuse_fingerprint);
    // The same tag element at another array position: same key. Another tag
    // element (a different key) with the same value at that position: another decision.
    let mut moved = fixture.clone();
    let mut swapped = instance_properties("BackupBucket");
    let tags = swapped["Tags"].as_array().unwrap().clone();
    swapped["Tags"] = json!([tags[1], tags[0]]);
    moved.reference.source.all_properties = PropertyValue::flatten_source(&swapped, &[]).unwrap();
    moved.reference.intent.location = "Tags[1].Value".into();
    assert_eq!(
        moved.ready().reuse_fingerprint,
        a.reuse_fingerprint,
        "array position is not evidence"
    );
    let mut sibling = fixture.clone();
    sibling.reference.intent.location = "Tags[1].Value".into();
    assert_ne!(
        sibling.ready().reuse_fingerprint,
        a.reuse_fingerprint,
        "the enclosing tag element is the occurrence's context"
    );
    let mut other_value = fixture.clone();
    other_value.reference.value = "other".into();
    assert_ne!(other_value.ready().reuse_fingerprint, a.reuse_fingerprint);
    let mut other_source = fixture.clone();
    other_source.reference.source.chain_id = Uuid::new_v4();
    other_source.reference.intent.observing_chain_id = other_source.reference.source.chain_id;
    assert_ne!(other_source.ready().reuse_fingerprint, a.reuse_fingerprint);
    let mut guided = fixture.clone();
    guided.guidance = Some("tags name destinations".into());
    assert_ne!(guided.ready().reuse_fingerprint, a.reuse_fingerprint);
    let mut fewer = fixture.clone();
    fewer.reference.candidates.truncate(1);
    assert_ne!(
        fewer.ready().reuse_fingerprint,
        a.reuse_fingerprint,
        "candidate set is evidence"
    );
}

/// Decision memory within one run and across array positions: a second
/// snapshot of the same source in the same run (a different packet, the same
/// key) is rebound to the run's own verdict; a persisted verdict is rebound when
/// the tag moved to another array position; a persisted rejection is reused
/// without an edge; a persisted target no longer offered dispatches.
#[tokio::test]
async fn run_local_key_hits_moved_tags_persisted_rejections_and_missing_targets() {
    use kg_core::runtime::reference_resolution::DecisionOutcome;
    let bucket = Uuid::from_u128(0xb);
    let fixture = Fixture::structured("BackupBucket", "Tags[0].Value").await;

    // Same run, second snapshot of the source with a volatile change: the
    // run-local original is rebound, the store is never needed.
    let llm = mock(&[accept_json(bucket, &["src:ctx", "c2:p3"])]);
    let ctx = fixture.stage_ctx(llm.clone());
    let first = run_stage(&fixture, &ctx).await.unwrap();
    assert_eq!(llm.call_count(), 1);
    let original = first.reference_report.decisions[0].clone();
    let mut later = fixture.clone();
    later.snapshot.uuid = Uuid::new_v4();
    later.reference.source.last_seen_snapshot_id = Some(later.snapshot.uuid);
    later.reference.source.uuid = Uuid::new_v4();
    later
        .reference
        .source
        .all_properties
        .insert("State.Name".into(), PropertyValue::String("stopped".into()));
    let second = run_stage(&later, &ctx).await.unwrap();
    assert_eq!(llm.call_count(), 1, "the run's own verdict is rebound");
    let rebound = &second.reference_report.decisions[0];
    assert!(rebound.reused);
    assert_eq!(rebound.reused_from, Some(original.decision_id));
    assert_ne!(rebound.evidence_fingerprint, original.evidence_fingerprint);
    assert_eq!(second.edges.len(), 1);

    // The persisted verdict, the tag moved from Tags[0] to Tags[1]: the reuse
    // key is index-free and the cited occurrence values compare by symbolic path.
    let store = persisted_from(&first);
    assert!(
        store[0]
            .audit
            .cited_value_hashes
            .iter()
            .any(|h| h.path == super::evidence::OCCURRENCE_PATH)
            && store[0]
                .audit
                .cited_value_hashes
                .iter()
                .any(|h| h.path == super::evidence::ENCLOSING_PATH),
        "{:?}",
        store[0].audit.cited_value_hashes
    );
    let mut moved = later.clone();
    let mut swapped = instance_properties("BackupBucket");
    swapped["State"]["Name"] = json!("stopped");
    let tags = swapped["Tags"].as_array().unwrap().clone();
    swapped["Tags"] = json!([tags[1], tags[0]]);
    moved.reference.source.all_properties = PropertyValue::flatten_source(&swapped, &[]).unwrap();
    moved.reference.intent.location = "Tags[1].Value".into();
    moved.persisted = PersistedStore::Rows(store.clone());
    let quiet = mock(&[]);
    let third = run_stage(&moved, &moved.stage_ctx(quiet.clone()))
        .await
        .unwrap();
    assert_eq!(
        quiet.call_count(),
        0,
        "{:?}",
        third.reference_report.decisions
    );
    assert!(third.reference_report.decisions[0].reused);
    assert_eq!(
        third.reference_report.decisions[0].location,
        "Tags[1].Value"
    );
    assert_eq!(third.edges.len(), 1);

    // The other tag (Team) now sits at Tags[0] with the same value: a different
    // enclosing element, so it is not this verdict.
    let mut other_tag = moved.clone();
    other_tag.reference.intent.location = "Tags[0].Value".into();
    let paying = mock(&[REJECT_JSON.into()]);
    run_stage(&other_tag, &other_tag.stage_ctx(paying.clone()))
        .await
        .unwrap();
    assert_eq!(
        paying.call_count(),
        1,
        "another tag key is another decision"
    );

    // A persisted rejection: reused without an edge and without a call.
    let rejecting = mock(&[REJECT_JSON.into()]);
    let rejected = run_stage(&fixture, &fixture.stage_ctx(rejecting.clone()))
        .await
        .unwrap();
    let reject_store = persisted_from(&rejected);
    assert_eq!(reject_store[0].audit.outcome, DecisionOutcome::Rejected);
    assert_eq!(
        reject_store[0].audit.cited_value_hashes.len(),
        2,
        "a rejection still pins the occurrence and its enclosing element"
    );
    let mut rejected_later = later.clone();
    rejected_later.persisted = PersistedStore::Rows(reject_store);
    let quiet = mock(&[]);
    let fourth = run_stage(&rejected_later, &rejected_later.stage_ctx(quiet.clone()))
        .await
        .unwrap();
    assert_eq!(quiet.call_count(), 0);
    assert!(fourth.reference_report.decisions[0].reused);
    assert_eq!(
        fourth.reference_report.decisions[0].outcome,
        DecisionOutcome::Rejected
    );
    assert!(fourth.edges.is_empty());

    // The persisted target is not among the offered candidates: decided again.
    let mut retargeted = store.clone();
    retargeted[0].audit.target_chain_id = Some(Uuid::from_u128(0xdead));
    let mut missing = later.clone();
    missing.persisted = PersistedStore::Rows(retargeted);
    let paying = mock(&[REJECT_JSON.into()]);
    run_stage(&missing, &missing.stage_ctx(paying.clone()))
        .await
        .unwrap();
    assert_eq!(paying.call_count(), 1);
}
