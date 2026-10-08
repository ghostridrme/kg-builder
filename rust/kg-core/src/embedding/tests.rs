//! Embedding text, field policy, stored summaries and the disabled placeholder.
use super::*;
use serde_json::json;

fn props(pairs: &[(&str, PropertyValue)]) -> IndexMap<String, PropertyValue> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn text(
    entity_type: &str,
    name: &str,
    summary: Option<&str>,
    properties: &IndexMap<String, PropertyValue>,
    keys: &[&str],
    labels: &[&str],
    fields: &EntityEmbeddingFields,
) -> String {
    let keys: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
    let labels: Vec<String> = labels.iter().map(|l| l.to_string()).collect();
    representation(
        &EntityText {
            entity_type,
            name,
            summary,
            properties,
            key_properties: &keys,
            labels: &labels,
        },
        fields,
    )
}

/// Shape, not vocabulary: identifiers of any provider are excluded, words of
/// any provider stay.
#[test]
fn identifier_grammar_separates_identifiers_from_words() {
    for id in [
        "arn:aws:iam::555555555555:role/acme-payments-app",
        "urn:isbn:0451450523",
        "https://api.example.com/v1",
        "payments-db.abc123xyz.eu-west-1.rds.amazonaws.com:5432",
        "ops@example.com",
        "6f2d2a1e-5c4b-4d3e-9a8f-7b6c5d4e3f2a",
        "10.20.30.40",
        "10.20.0.0/16",
        "fe80::1",
        "2001:db8::ff00:42:8329",
        "00:1a:2b:3c:4d:5e",
        "2026-09-27",
        "2026-09-27T01:59:00Z",
        "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
        "i-0abc123def456",
        "vol1-0acme075468c340",
        "sg-0acme071b53b680",
        "subneta-0acme004e69a950",
        "db-ACME0000000000000007",
        "555555555555",
        "ami-0123456789abcdef0",
        "AKIAIOSFODNN7EXAMPLE",
        "subneta-0acme0219b9ec00",
        "eni-attach0-0acme0858f01ea0",
        "subneta-0acme004e69a950,subnetb-0acme00cd4a3c20",
        "analytics-use1-db.acme.us-east-1.rds.amazonaws.com",
        "fe80::1%eth0",
        "localhost:8080",
        "0.0.0.0/0",
        "dGhpcyBpcyBhIHNlY3JldA1234==",
    ] {
        assert!(identifier_like(id), "{id} is an identifier");
    }
    for word in [
        "m7i.xlarge",
        "db.r6g.large",
        "eu-west-1b",
        "us-central1-a",
        "running",
        "postgres",
        "x86_64",
        "cc-2042",
        "production",
        "Standard_D4s_v5",
        "n2-standard-4",
        "linux/amd64",
        "2xlarge",
        "1.2.3",
        "Handles card payments",
        "payments-euw1-db",
        "gp3",
        "true",
        "AWS::EC2::Instance",
        "std::fmt::Display",
        "PAY_PER_REQUEST",
        "AWS/EC2",
        "$Latest",
        "Windows2019Datacenter",
        "checkout-7d9f8b6c5d-x2k9q",
        "Ubuntu 22.04",
        "v2.10.0",
        "16.4",
        "t3.micro",
        "arm64",
    ] {
        assert!(!identifier_like(word), "{word} is a word");
    }
}

/// Automatic selection: type and name lead; scalar properties that are neither
/// keys, identifiers, credentials nor volatile follow sorted by path; tag lists
/// and labels close. The same input renders the same text whatever the property
/// order, and every identifier and key value stays out.
#[test]
fn automatic_representation_keeps_words_and_drops_identifiers_keys_and_secrets() {
    let properties = props(&[
        ("VpcId", "vpc-0acme01d16c8e90".into()),
        ("State.Name", "running".into()),
        ("State.Code", PropertyValue::Integer(16)),
        ("InstanceType", "m7i.xlarge".into()),
        ("InstanceId", "i-0abc123def456".into()),
        ("Placement.AvailabilityZone", "eu-west-1b".into()),
        ("PrivateIpAddress", "10.20.30.40".into()),
        ("LaunchTime", PropertyValue::Timestamp("2026-09-01T00:00:00Z".parse().unwrap())),
        ("EbsOptimized", PropertyValue::Bool(true)),
        ("Score", PropertyValue::Float(0.5)),
        ("Size", PropertyValue::Integer(100)),
        ("Epoch", PropertyValue::Integer(1_758_368_412_000)),
        ("Metadata.Password", "hunter2".into()),
        ("api_key", "plain-words-here".into()),
        ("Credentials.Username", "svc-admin".into()),
        ("Arn", "arn:aws:ec2:eu-west-1:555555555555:instance/i-0abc123def456".into()),
        ("_astrolabe_scope.region", "eu-west-1".into()),
        ("Nested.Blob", PropertyValue::Blob("AAAA".into())),
        ("Ports", PropertyValue::IntegerList(vec![443])),
        (
            "Tags",
            PropertyValue::Json(
                r#"[{"Key":"Name","Value":"payments-euw1-app-1"},{"Key":"Environment","Value":"production"},{"Key":"DataVolume","Value":"vol1-0acme075468c340"},{"Key":"CostCenter","Value":"cc-2042","PropagateAtLaunch":true}]"#
                    .into(),
            ),
        ),
    ]);
    let fields = EntityEmbeddingFields::default();
    let rendered = text(
        "AWS::EC2::Instance",
        "payments-euw1-app-1",
        None,
        &properties,
        &["InstanceId", "Arn"],
        &["region:eu-west-1", "aws", "account:555555555555"],
        &fields,
    );
    assert_eq!(
        rendered,
        "type: AWS::EC2::Instance
name: payments-euw1-app-1
EbsOptimized: true
InstanceType: m7i.xlarge
Placement.AvailabilityZone: eu-west-1b
Size: 100
State.Code: 16
State.Name: running
tag Environment: production
tag CostCenter: cc-2042
labels: account:555555555555, aws, region:eu-west-1"
    );
    for absent in [
        "vpc-0acme",
        "i-0abc",
        "10.20.30.40",
        "2026-09",
        "0.5",
        "1758368412000",
        "hunter2",
        "plain-words-here",
        "svc-admin",
        "arn:",
        "_astrolabe",
        "AAAA",
        "443",
        "vol1-",
        "tag Name",
    ] {
        assert!(!rendered.contains(absent), "{absent} must not be embedded");
    }
    let mut reordered = properties.clone();
    reordered.reverse();
    assert_eq!(
        rendered,
        text(
            "AWS::EC2::Instance",
            "payments-euw1-app-1",
            None,
            &reordered,
            &["InstanceId", "Arn"],
            &["aws", "region:eu-west-1", "account:555555555555"],
            &fields,
        ),
        "property and label order never change the text"
    );
    // A descriptive field from the default list is kept even when its value
    // would be excluded by shape, and the native summary takes precedence.
    let described = props(&[
        ("description", "Handles card payments".into()),
        ("summary", "from property".into()),
        ("resource_id", "i-0abc123def456".into()),
        ("service", "cc-2042".into()),
    ]);
    let rendered = text(
        "Service",
        "payments",
        Some("from field"),
        &described,
        &[],
        &[],
        &fields,
    );
    // Descriptive defaults lead (summary, description, service); identifiers stay out.
    assert_eq!(
        rendered,
        "type: Service\nname: payments\nsummary: from field\ndescription: Handles card payments\nservice: cc-2042"
    );
    assert!(
        text("Service", "payments", None, &described, &[], &[], &fields)
            .contains("summary: from property")
    );
    // A per-source exclusion removes a property the rule would keep.
    let excluding = EntityEmbeddingFields {
        exclude: vec!["State.Code".into()],
        ..EntityEmbeddingFields::default()
    };
    excluding.validate().unwrap();
    assert!(!text(
        "AWS::EC2::Instance",
        "n",
        None,
        &properties,
        &[],
        &[],
        &excluding
    )
    .contains("State.Code"));
    assert_ne!(excluding.text_version(), fields.text_version());
    assert_eq!(fields.text_version(), TEXT_VERSION);
    validate_entity_text_version(&excluding.text_version()).unwrap();
}

/// Other record shapes: a Kubernetes pod (map labels flatten to dotted scalars,
/// the UID is a key and an identifier) and a CMDB server (free text kept,
/// timestamps and sys_id out).
#[test]
fn automatic_representation_works_for_kubernetes_and_cmdb_shapes() {
    let pod = props(&[
        (
            "metadata.uid",
            "6f2d2a1e-5c4b-4d3e-9a8f-7b6c5d4e3f2a".into(),
        ),
        ("metadata.labels.app", "checkout".into()),
        ("metadata.labels.tier", "web".into()),
        ("spec.nodeName", "node-1".into()),
        ("status.phase", "Running".into()),
        ("status.podIP", "10.1.2.3".into()),
        (
            "spec.containers",
            PropertyValue::Json("[{\"image\":\"x\"}]".into()),
        ),
    ]);
    assert_eq!(
        text(
            "Pod",
            "checkout-7d9f",
            None,
            &pod,
            &["metadata.uid"],
            &["cluster:prod-eu"],
            &EntityEmbeddingFields::default()
        ),
        "type: Pod
name: checkout-7d9f
metadata.labels.app: checkout
metadata.labels.tier: web
spec.nodeName: node-1
status.phase: Running
labels: cluster:prod-eu"
    );
    let server = props(&[
        ("sys_id", "3a1f9c8e7b6d5a4c3b2a1f0e9d8c7b6a".into()),
        ("description", "Primary database host for payments".into()),
        ("os", "Ubuntu 22.04".into()),
        ("cpu_count", PropertyValue::Integer(16)),
        ("sys_updated_on", "2026-09-27 01:59:00".into()),
        ("ip_address", "192.168.1.10".into()),
        ("environment", "production".into()),
    ]);
    assert_eq!(
        text(
            "cmdb_ci_server",
            "db-prod-01",
            None,
            &server,
            &["sys_id"],
            &[],
            &EntityEmbeddingFields::default()
        ),
        "type: cmdb_ci_server
name: db-prod-01
description: Primary database host for payments
cpu_count: 16
environment: production
os: Ubuntu 22.04"
    );
}

/// Bounds: property lines, tag lines, labels, characters.
#[test]
fn representation_is_bounded() {
    let mut many = IndexMap::new();
    for i in 0..60 {
        many.insert(
            format!("p{i:02}"),
            PropertyValue::String(format!("word{i}")),
        );
    }
    let tags: Vec<String> = (0..40)
        .map(|i| format!(r#"{{"Key":"k{i}","Value":"v{i}"}}"#))
        .collect();
    many.insert(
        "Tags".into(),
        PropertyValue::Json(format!("[{}]", tags.join(","))),
    );
    let labels: Vec<String> = (0..40).map(|i| format!("l{i:02}")).collect();
    let label_refs: Vec<&str> = labels.iter().map(String::as_str).collect();
    let rendered = text(
        "T",
        "n",
        None,
        &many,
        &[],
        &label_refs,
        &EntityEmbeddingFields::default(),
    );
    assert_eq!(
        rendered.lines().filter(|l| l.starts_with('p')).count(),
        MAX_PROPERTY_LINES
    );
    assert_eq!(
        rendered.lines().filter(|l| l.starts_with("tag ")).count(),
        MAX_TAG_LINES
    );
    assert_eq!(
        rendered.lines().last().unwrap().matches(", ").count() + 1,
        MAX_LABELS
    );

    let large = "x".repeat(10_000);
    let properties: IndexMap<String, PropertyValue> = DESCRIPTIVE_FIELDS
        .iter()
        .map(|field| (field.to_string(), PropertyValue::String(large.clone())))
        .collect();
    let rendered = text(
        &large,
        &large,
        Some(&large),
        &properties,
        &[],
        &[],
        &EntityEmbeddingFields::default(),
    );
    assert_eq!(rendered.chars().count(), MAX_TEXT_CHARS);
    assert!(rendered.starts_with(&format!(
        "type: {}
name: ",
        "x".repeat(MAX_FIELD_CHARS)
    )));
    assert_ne!(content_hash(&rendered), content_hash("other"));
}

#[test]
fn stored_embeddings_match_only_under_the_same_settings_and_content() {
    let settings = EmbeddingSettings {
        entity_fields: Default::default(),
        model: "m".into(),
        dimension: 2,
        text_version: TEXT_VERSION.into(),
    };
    let hash = content_hash("type: Service\nname: api");
    let mut record = EntityVersionRecord {
        uuid: uuid::Uuid::nil(),
        chain_id: uuid::Uuid::nil(),
        version: 1,
        is_latest: true,
        entity_type: "Service".into(),
        name: "api".into(),
        namespace: "prod".into(),
        source: None,
        identity_hash: None,
        identity_hashes: vec![],
        structural_hash: None,
        valid_from: None,
        valid_to: None,
        deleted_at: None,
        last_seen_at: None,
        last_transition_at: None,
        sync_generation: None,
        collections: vec![],
        merged_into: None,
        embedding: Some(GraphEmbedding {
            model: "m".into(),
            values: vec![0.5, 0.5],
        }),
        stored: serde_json::Map::new(),
    };
    assert!(
        ComputedEmbedding::from_stored(&record).is_none(),
        "a vector without provenance is never reused"
    );
    record
        .stored
        .insert(TEXT_VERSION_PROPERTY.into(), json!(TEXT_VERSION));
    record
        .stored
        .insert(CONTENT_HASH_PROPERTY.into(), json!(hash));
    let stored = ComputedEmbedding::from_stored(&record).unwrap();
    assert!(stored.matches(&settings, &hash));
    assert!(!stored.matches(&settings, &content_hash("changed")));
    for other in [
        EmbeddingSettings {
            model: "other".into(),
            ..settings.clone()
        },
        EmbeddingSettings {
            dimension: 3,
            ..settings.clone()
        },
        EmbeddingSettings {
            text_version: "different-policy".into(),
            ..settings.clone()
        },
    ] {
        assert!(!stored.matches(&other, &hash), "{other:?}");
    }
    assert_eq!(stored.stored().values, vec![0.5, 0.5]);
    // A vector stored under the previous text version is recomputed, never compared.
    let mut previous = settings.clone();
    previous.text_version = "2".into();
    let stale = ComputedEmbedding::new(&previous, content_hash("t"), vec![1.0, 0.0]);
    assert!(!stale.matches(&settings, &content_hash("t")));
    assert!(validate_entity_text_version("2").is_err());
}

#[test]
fn provider_output_is_checked_for_count_dimension_and_values() {
    let settings = EmbeddingSettings {
        entity_fields: Default::default(),
        model: "m".into(),
        dimension: 2,
        text_version: TEXT_VERSION.into(),
    };
    assert!(validate_vectors(&settings, 1, &[vec![1.0, 0.0]]).is_ok());
    for bad in [
        vec![],
        vec![vec![1.0, 0.0], vec![0.0, 1.0]],
        vec![vec![1.0]],
        vec![vec![0.0, 0.0]],
        vec![vec![f32::NAN, 1.0]],
    ] {
        assert!(validate_vectors(&settings, 1, &bad).is_err(), "{bad:?}");
    }
}

mod field_policy {
    use super::*;
    #[test]
    fn configured_fields_are_ordered_bounded_scalar_and_type_specific() {
        let mut fields = EntityEmbeddingFields::default();
        fields.by_entity_type.insert(
            "Deployment".into(),
            vec!["namespace".into(), "replicas".into(), "tls".into()],
        );
        fields.validate().unwrap();
        let properties = IndexMap::from([
            ("namespace".into(), PropertyValue::String("payments".into())),
            ("replicas".into(), PropertyValue::Integer(3)),
            ("tls".into(), PropertyValue::Bool(true)),
            (
                "password".into(),
                PropertyValue::String("unselected".into()),
            ),
            ("zone".into(), PropertyValue::String("eu-west-1b".into())),
        ]);
        // Explicit mode: exactly the listed properties, in the listed order.
        assert_eq!(
            text(
                "Deployment",
                "checkout",
                None,
                &properties,
                &[],
                &[],
                &fields
            ),
            "type: Deployment\nname: checkout\nnamespace: payments\nreplicas: 3\ntls: true"
        );
        // Another type stays automatic: sorted, credentials out.
        assert_eq!(
            text("Other", "checkout", None, &properties, &[], &[], &fields),
            "type: Other\nname: checkout\nnamespace: payments\nreplicas: 3\ntls: true\nzone: eu-west-1b"
        );
        let mut reordered = properties.clone();
        reordered.reverse();
        assert_eq!(
            text(
                "Deployment",
                "checkout",
                None,
                &properties,
                &[],
                &[],
                &fields
            ),
            text(
                "Deployment",
                "checkout",
                None,
                &reordered,
                &[],
                &[],
                &fields
            )
        );
        assert_ne!(fields.text_version(), TEXT_VERSION);
        validate_entity_text_version(&fields.text_version()).unwrap();
        let before = fields.text_version();
        fields
            .by_entity_type
            .get_mut("Deployment")
            .unwrap()
            .reverse();
        assert_ne!(fields.text_version(), before);
    }
    #[test]
    fn policies_reject_duplicate_unbounded_and_ambiguous_paths() {
        for paths in [
            vec!["name"],
            vec![""],
            vec!["a", "a"],
            vec!["a..b"],
            vec![" description"],
            vec!["a\nb"],
        ] {
            let fields = EntityEmbeddingFields {
                default: paths.into_iter().map(str::to_owned).collect(),
                ..Default::default()
            };
            assert!(fields.validate().is_err());
        }
        assert!(validate_entity_text_version("1").is_err());
        assert!(validate_entity_text_version("2").is_err());
        assert!(validate_entity_text_version("3:bad").is_err());
        let fields = EntityEmbeddingFields {
            default: vec![],
            ..Default::default()
        };
        fields.validate().unwrap();
        assert_eq!(
            text("Type", "name", None, &IndexMap::new(), &[], &[], &fields),
            "type: Type\nname: name"
        );
        let bad_exclude = EntityEmbeddingFields {
            exclude: vec!["name".into()],
            ..EntityEmbeddingFields::default()
        };
        assert!(bad_exclude.validate().is_err());
    }
}

mod stored_summary {
    use super::*;
    #[test]
    fn absent_and_blank_native_summaries_keep_the_source_summary_after_storage() {
        let fields = EntityEmbeddingFields::default();
        let source = IndexMap::from([(
            "summary".into(),
            PropertyValue::String("checkout API".into()),
        )]);
        let incoming = text("Service", "api", None, &source, &[], &[], &fields);
        for native_summary in ["", "   "] {
            let mut stored = serde_json::json!({"entity_type":"Service", "name":"api", "summary":native_summary}).as_object().unwrap().clone();
            crate::traits::property_codec::write_property(
                &mut stored,
                "summary",
                source.get("summary"),
            );
            let record = crate::embedding_rebuild::EmbeddingRecord {
                uuid: uuid::Uuid::new_v4(),
                properties: stored,
            };
            let recovered = record
                .text(crate::embedding_rebuild::EmbeddingKind::Entity, &fields)
                .unwrap();
            assert_eq!(recovered, incoming);
            assert_eq!(content_hash(&recovered), content_hash(&incoming));
            assert!(recovered.contains("summary: checkout API"));
        }
    }
}

mod disabled_embedder {
    use super::*;
    use crate::traits::EmbedDisabled;

    #[test]
    fn the_disabled_placeholder_is_accepted_and_identifies_itself() {
        let settings = EmbeddingSettings::of(&EmbedDisabled).unwrap();
        assert_eq!((settings.model.as_str(), settings.dimension), ("none", 0));
        assert!(!EmbedDisabled.is_configured());
    }
}

mod stored_round_trip {
    use super::*;
    /// A stored row (alphabetical `prop_` order, keys as stored arrays and JSON,
    /// labels as a list) renders exactly the ingest text.
    #[test]
    fn stored_rows_render_the_ingest_text() {
        let fields = EntityEmbeddingFields::default();
        let source = props(&[
            ("State.Name", "running".into()),
            ("InstanceType", "m7i.xlarge".into()),
            ("InstanceId", "i-0abc123def456".into()),
            (
                "Arn",
                "arn:aws:ec2:eu-west-1:1:instance/i-0abc123def456".into(),
            ),
            ("Size", PropertyValue::Integer(100)),
            (
                "Tags",
                PropertyValue::Json(r#"[{"Key":"Environment","Value":"production"}]"#.into()),
            ),
        ]);
        let incoming = text(
            "AWS::EC2::Instance",
            "web-1",
            None,
            &source,
            &["InstanceId", "Arn"],
            &["region:eu-west-1", "aws"],
            &fields,
        );
        assert!(incoming.contains("tag Environment: production"));
        assert!(!incoming.contains("i-0abc"));
        let mut stored = serde_json::json!({
            "entity_type": "AWS::EC2::Instance",
            "name": "web-1",
            "labels": ["aws", "region:eu-west-1"],
            "primary_key_properties": ["InstanceId"],
            "additional_key_properties": "[[\"Arn\"]]"
        })
        .as_object()
        .unwrap()
        .clone();
        let mut reversed: Vec<_> = source.iter().collect();
        reversed.reverse();
        for (key, value) in reversed {
            crate::traits::property_codec::write_property(&mut stored, key, Some(value));
        }
        let record = crate::embedding_rebuild::EmbeddingRecord {
            uuid: uuid::Uuid::new_v4(),
            properties: stored,
        };
        let recovered = record
            .text(crate::embedding_rebuild::EmbeddingKind::Entity, &fields)
            .unwrap();
        assert_eq!(recovered, incoming);
    }
}
