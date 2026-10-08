//! Neo4j schema definitions and validation of server/index metadata.

/// Required constraints and indexes: node and relationship identity, the
/// producer's version key, run headers and operation receipts, and the
/// fulltext and vector indexes used by typed search.
///
/// One latest version per chain cannot be a Neo4j constraint; commits enforce
/// it with locked transactional preconditions.
///
/// Run via `Neo4jGraphBackend::ensure_indexes` at startup.
/// Old evidence links cannot be ignored by the new search and ingestion schema.
pub const LEGACY_EVIDENCE: &str = "MATCH ()-[r:OBSERVED_IN]->() RETURN true AS legacy LIMIT 1";
pub const LEGACY_REFERENCE_OWNERS: &str = "MATCH ()-[r:RELATES_TO]->() WHERE r.reference_slot IS NOT NULL AND (r.reference_owner_chain_id IS NULL OR r.reference_owner_namespace IS NULL OR r.reference_tokens IS NULL) RETURN true AS legacy LIMIT 1";

pub const INDEXES: &[(&str, &str)] = &[
    ("commit_plan_unique", "CREATE CONSTRAINT commit_plan_unique IF NOT EXISTS FOR (n:CommitPlan) REQUIRE n.batch_id IS UNIQUE"),
    ("commit_plan_part_unique", "CREATE CONSTRAINT commit_plan_part_unique IF NOT EXISTS FOR (n:CommitPlanPart) REQUIRE (n.batch_id,n.ordinal) IS UNIQUE"),
    ("commit_page_receipt_unique", "CREATE CONSTRAINT commit_page_receipt_unique IF NOT EXISTS FOR (n:CommitPageReceipt) REQUIRE (n.batch_id,n.ordinal) IS UNIQUE"),
    ("connector_checkpoint_scope", "CREATE CONSTRAINT connector_checkpoint_scope IF NOT EXISTS FOR (n:ConnectorCheckpoint) REQUIRE (n.org_id,n.namespace,n.key) IS UNIQUE"),
    ("connector_checkpoint_receipt", "CREATE CONSTRAINT connector_checkpoint_receipt IF NOT EXISTS FOR (n:ConnectorCheckpointReceipt) REQUIRE (n.org_id,n.namespace,n.key,n.run_id) IS UNIQUE"),
    ("ingestion_profile_scope", "CREATE INDEX ingestion_profile_scope IF NOT EXISTS FOR (n:IngestionProfile) ON (n.org_id,n.profile_id,n.revision)"),
    ("ingestion_profile_unique", "CREATE CONSTRAINT ingestion_profile_unique IF NOT EXISTS FOR (n:IngestionProfile) REQUIRE n.scope_id IS UNIQUE"),
    ("graph_revision_unique", "CREATE CONSTRAINT graph_revision_unique IF NOT EXISTS FOR (n:GraphRevision) REQUIRE (n.org_id,n.stripe) IS UNIQUE"),
    ("explorer_entity_scope", "CREATE INDEX explorer_entity_scope IF NOT EXISTS FOR (n:Entity) ON (n.org_id,n.namespace)"),
    ("reference_dependency_unique", "CREATE CONSTRAINT reference_dependency_unique IF NOT EXISTS FOR (n:ReferenceDependency) REQUIRE (n.org_id,n.edge_uuid,n.token) IS UNIQUE"),
    ("reference_dependency_token", "CREATE INDEX reference_dependency_token IF NOT EXISTS FOR (n:ReferenceDependency) ON (n.org_id,n.token)"),
    ("identity_value_unique", "CREATE CONSTRAINT identity_value_unique IF NOT EXISTS FOR (n:IdentityValue) REQUIRE (n.org_id,n.token) IS UNIQUE"),
    ("identity_value_folded", "CREATE INDEX identity_value_folded IF NOT EXISTS FOR (n:IdentityValue) ON (n.org_id,n.folded_token)"),
    ("unresolved_slot_unique", "CREATE CONSTRAINT unresolved_slot_unique IF NOT EXISTS FOR (n:UnresolvedSlot) REQUIRE (n.org_id,n.source_chain_id,n.slot) IS UNIQUE"),
    ("community_maintenance_page", "CREATE INDEX community_maintenance_page IF NOT EXISTS FOR (n:Community) ON (n.org_id,n.uuid)"),
    ("search_communities", "CREATE FULLTEXT INDEX search_communities IF NOT EXISTS FOR (n:Community) ON EACH [n.org_id,n.name] OPTIONS {indexConfig: {`fulltext.analyzer`: 'standard-no-stop-words', `fulltext.eventually_consistent`: false}}"),
    ("search_community_name_vectors", "CREATE VECTOR INDEX search_community_name_vectors IF NOT EXISTS FOR (n:Community) ON (n.name_embedding) OPTIONS {indexConfig: {`vector.similarity_function`: 'cosine', `vector.quantization.enabled`: false}}"),
    ("community_vector_scope", "CREATE INDEX community_vector_scope IF NOT EXISTS FOR (n:Community) ON (n.org_id,n.namespace,n.name_embedding_model,n.name_embedding_text_version)"),
    ("community_incoming", "CREATE INDEX community_incoming IF NOT EXISTS FOR ()-[r:RELATES_TO]-() ON (r.org_id,r.target_chain_id,r.uuid)"),
    ("community_outgoing", "CREATE INDEX community_outgoing IF NOT EXISTS FOR ()-[r:RELATES_TO]-() ON (r.org_id,r.source_chain_id,r.uuid)"),
    ("community_scope_unique", "CREATE CONSTRAINT community_scope_unique IF NOT EXISTS FOR (n:CommunityScope) REQUIRE (n.org_id,n.namespace) IS UNIQUE"),
    ("community_revision_unique", "CREATE CONSTRAINT community_revision_unique IF NOT EXISTS FOR (n:CommunityRevision) REQUIRE (n.org_id,n.namespace,n.stripe) IS UNIQUE"),
    ("community_generation_unique", "CREATE CONSTRAINT community_generation_unique IF NOT EXISTS FOR (n:CommunityGeneration) REQUIRE n.uuid IS UNIQUE"),
    ("community_partition_unique", "CREATE CONSTRAINT community_partition_unique IF NOT EXISTS FOR (n:CommunityPartition) REQUIRE (n.generation_uuid,n.partition_index) IS UNIQUE"),
    ("community_uuid_unique", "CREATE CONSTRAINT community_uuid_unique IF NOT EXISTS FOR (n:Community) REQUIRE n.uuid IS UNIQUE"),
    ("community_membership_unique", "CREATE CONSTRAINT community_membership_unique IF NOT EXISTS FOR ()-[n:HAS_MEMBER]-() REQUIRE n.membership_key IS UNIQUE"),
    ("community_entity_scope", "CREATE INDEX community_entity_scope IF NOT EXISTS FOR (n:Entity) ON (n.org_id,n.namespace,n.uuid)"),
    ("community_generation_scope", "CREATE INDEX community_generation_scope IF NOT EXISTS FOR (n:Community) ON (n.org_id,n.namespace,n.generation_uuid,n.uuid)"),
    ("community_member_chain", "CREATE INDEX community_member_chain IF NOT EXISTS FOR ()-[n:HAS_MEMBER]-() ON (n.community_uuid,n.chain_id)"),
    ("community_generation_chain", "CREATE INDEX community_generation_chain IF NOT EXISTS FOR ()-[n:HAS_MEMBER]-() ON (n.generation_uuid,n.chain_id)"),
    ("saga_member_capture", "CREATE INDEX saga_member_capture IF NOT EXISTS FOR ()-[m:HAS_EPISODE]-() ON (m.saga_uuid,m.captured_at,m.snapshot_created_at,m.ordinal)"),
    ("saga_member_ordinal", "CREATE INDEX saga_member_ordinal IF NOT EXISTS FOR ()-[m:HAS_EPISODE]-() ON (m.saga_uuid,m.ordinal)"),
    ("saga_scope_unique", "CREATE CONSTRAINT saga_scope_unique IF NOT EXISTS FOR (n:Saga) REQUIRE n.scope_id IS UNIQUE"),
    ("saga_uuid_unique", "CREATE CONSTRAINT saga_uuid_unique IF NOT EXISTS FOR (n:Saga) REQUIRE n.uuid IS UNIQUE"),
    ("saga_membership_uuid_unique", "CREATE CONSTRAINT saga_membership_uuid_unique IF NOT EXISTS FOR ()-[n:HAS_EPISODE]-() REQUIRE n.uuid IS UNIQUE"),
    ("saga_membership_key_unique", "CREATE CONSTRAINT saga_membership_key_unique IF NOT EXISTS FOR ()-[n:HAS_EPISODE]-() REQUIRE n.membership_key IS UNIQUE"),
    ("saga_next_uuid_unique", "CREATE CONSTRAINT saga_next_uuid_unique IF NOT EXISTS FOR ()-[n:NEXT_EPISODE]-() REQUIRE n.uuid IS UNIQUE"),
    ("saga_next_target_unique", "CREATE CONSTRAINT saga_next_target_unique IF NOT EXISTS FOR ()-[n:NEXT_EPISODE]-() REQUIRE n.predecessor_key IS UNIQUE"),

    ("search_entity_summaries", "CREATE FULLTEXT INDEX search_entity_summaries IF NOT EXISTS FOR (n:Entity) ON EACH [n.org_id, n.derived_summary] OPTIONS {indexConfig: {`fulltext.analyzer`: 'standard-no-stop-words', `fulltext.eventually_consistent`: false}}"),
    ("search_entity_summary_vectors", "CREATE VECTOR INDEX search_entity_summary_vectors IF NOT EXISTS FOR (n:Entity) ON (n.summary_embedding) OPTIONS {indexConfig: {`vector.similarity_function`: 'cosine', `vector.quantization.enabled`: false}}"),
    ("search_entity_summary_scope", "CREATE INDEX search_entity_summary_scope IF NOT EXISTS FOR (n:Entity) ON (n.org_id,n.namespace,n.summary_embedding_model,n.summary_embedding_text_version)"),

    ("identity_candidate_live_scope", "CREATE INDEX identity_candidate_live_scope IF NOT EXISTS FOR (n:Entity) ON (n.org_id,n.namespace,n.is_latest)"),
    ("identity_candidate_live_vectors", "CREATE INDEX identity_candidate_live_vectors IF NOT EXISTS FOR (n:Entity) ON (n.org_id,n.namespace,n.is_latest,n.embedding_model,n.embedding_text_version)"),
    ("identity_candidate_names", "CREATE INDEX identity_candidate_names IF NOT EXISTS FOR (n:Entity) ON (n.org_id,n.namespace,n.entity_type,n.name)"),
    ("identity_candidate_vectors", "CREATE INDEX identity_candidate_vectors IF NOT EXISTS FOR (n:Entity) ON (n.org_id,n.namespace,n.entity_type,n.embedding_model,n.embedding_text_version)"),
    ("identity_revision_unique", "CREATE CONSTRAINT identity_revision_unique IF NOT EXISTS FOR (n:IdentityRevision) REQUIRE n.scope_id IS UNIQUE"),

    ("reference_decision_unique", "CREATE CONSTRAINT reference_decision_unique IF NOT EXISTS FOR (n:ReferenceDecision) REQUIRE (n.org_id,n.decision_id) IS UNIQUE"),
    ("reference_decision_reuse_key", "CREATE INDEX reference_decision_reuse_key IF NOT EXISTS FOR (n:ReferenceDecision) ON (n.org_id,n.reuse_fingerprint)"),
    ("reference_decision_source", "CREATE INDEX reference_decision_source IF NOT EXISTS FOR (n:ReferenceDecision) ON (n.org_id,n.producer_source,n.source_chain_id)"),
    ("unresolved_reference_token", "CREATE INDEX unresolved_reference_token IF NOT EXISTS FOR (n:UnresolvedReference) ON (n.org_id,n.token)"),
    ("unresolved_reference_source", "CREATE INDEX unresolved_reference_source IF NOT EXISTS FOR (n:UnresolvedReference) ON (n.org_id,n.source_chain_id,n.slot)"),
    ("identity_key_unique", "CREATE CONSTRAINT identity_key_unique IF NOT EXISTS FOR (n:IdentityKey) REQUIRE (n.org_id,n.hash) IS UNIQUE"),
    ("chain_merge_state_unique", "CREATE CONSTRAINT chain_merge_state_unique IF NOT EXISTS FOR (n:ChainMergeState) REQUIRE n.scope_id IS UNIQUE"),
    ("chain_merge_period_unique", "CREATE CONSTRAINT chain_merge_period_unique IF NOT EXISTS FOR (n:ChainMerge) REQUIRE n.period_id IS UNIQUE"),
    ("chain_merge_loser", "CREATE INDEX chain_merge_loser IF NOT EXISTS FOR (n:ChainMerge) ON (n.org_id, n.loser_chain_id)"),
    ("chain_merge_winner", "CREATE INDEX chain_merge_winner IF NOT EXISTS FOR (n:ChainMerge) ON (n.org_id, n.winner_chain_id)"),
    ("search_entity_vectors", "CREATE VECTOR INDEX search_entity_vectors IF NOT EXISTS FOR (n:Entity) ON (n.embedding) OPTIONS {indexConfig: {`vector.similarity_function`: 'cosine', `vector.quantization.enabled`: false}}"),
    ("search_relationship_vectors", "CREATE VECTOR INDEX search_relationship_vectors IF NOT EXISTS FOR ()-[r:RELATES_TO]-() ON (r.embedding) OPTIONS {indexConfig: {`vector.similarity_function`: 'cosine', `vector.quantization.enabled`: false}}"),
    ("ingestion_run_id_unique", "CREATE CONSTRAINT ingestion_run_id_unique IF NOT EXISTS FOR (n:IngestionRun) REQUIRE n.run_id IS UNIQUE"),
    ("operation_receipt_batch_unique", "CREATE CONSTRAINT operation_receipt_batch_unique IF NOT EXISTS FOR (n:OperationReceipt) REQUIRE n.batch_id IS UNIQUE"),
    ("operation_receipt_run_idx", "CREATE INDEX operation_receipt_run_idx IF NOT EXISTS FOR (n:OperationReceipt) ON (n.org_id, n.run_id)"),
    ("graph_node_uuid_unique", "CREATE CONSTRAINT graph_node_uuid_unique IF NOT EXISTS FOR (n:GraphNode) REQUIRE n.uuid IS UNIQUE"),
    ("snapshot_uuid_unique", "CREATE CONSTRAINT snapshot_uuid_unique IF NOT EXISTS FOR (n:Snapshot) REQUIRE n.uuid IS UNIQUE"),
    ("relationship_uuid_unique", "CREATE CONSTRAINT relationship_uuid_unique IF NOT EXISTS FOR ()-[r:RELATES_TO]-() REQUIRE r.uuid IS UNIQUE"),
    ("observation_uuid_unique", "CREATE CONSTRAINT observation_uuid_unique IF NOT EXISTS FOR ()-[r:MENTIONS]-() REQUIRE r.uuid IS UNIQUE"),
    // The organization is indexed so keyword queries can require it inside
    // Lucene; namespace and type are filters, not searchable text.
    ("search_entities", "CREATE FULLTEXT INDEX search_entities IF NOT EXISTS FOR (n:Entity) ON EACH [n.org_id, n.name, n.summary, n.prop_summary] OPTIONS {indexConfig: {`fulltext.analyzer`: 'standard-no-stop-words', `fulltext.eventually_consistent`: false}}"),
    ("search_relationships", "CREATE FULLTEXT INDEX search_relationships IF NOT EXISTS FOR ()-[r:RELATES_TO]-() ON EACH [r.org_id, r.name, r.description] OPTIONS {indexConfig: {`fulltext.analyzer`: 'standard-no-stop-words', `fulltext.eventually_consistent`: false}}"),
    ("search_snapshots", "CREATE FULLTEXT INDEX search_snapshots IF NOT EXISTS FOR (n:Snapshot) ON EACH [n.org_id, n.name, n.source, n.content] OPTIONS {indexConfig: {`fulltext.analyzer`: 'standard-no-stop-words', `fulltext.eventually_consistent`: false}}"),
    ("search_org_chain", "CREATE INDEX search_org_chain IF NOT EXISTS FOR (n:Entity) ON (n.org_id, n.chain_id)"),
    ("search_entity_embedding_scope", "CREATE INDEX search_entity_embedding_scope IF NOT EXISTS FOR (n:Entity) ON (n.org_id, n.namespace, n.embedding_model, n.embedding_text_version)"),
    ("search_relationship_org", "CREATE INDEX search_relationship_org IF NOT EXISTS FOR ()-[r:RELATES_TO]-() ON (r.org_id)"),
    ("search_relationship_embedding", "CREATE INDEX search_relationship_embedding IF NOT EXISTS FOR ()-[r:RELATES_TO]-() ON (r.org_id, r.embedding_model, r.embedding_text_version)"),
    ("search_snapshot_org", "CREATE INDEX search_snapshot_org IF NOT EXISTS FOR (n:Snapshot) ON (n.org_id)"),

    (
        "entity_hash_version_unique",
        // The producer's hash/version key arbitrates concurrent version creation.
        "CREATE CONSTRAINT entity_hash_version_unique IF NOT EXISTS FOR (n:Entity) REQUIRE n.hash_version IS UNIQUE",
    ),
    (
        "entity_uuid_unique",
        "CREATE CONSTRAINT entity_uuid_unique IF NOT EXISTS FOR (n:Entity) REQUIRE n.uuid IS UNIQUE",
    ),
    (
        "entity_identity_hash_idx",
        "CREATE INDEX entity_identity_hash_idx IF NOT EXISTS FOR (n:Entity) ON (n.identity_hash)",
    ),
    (
        "entity_chain_id_idx",
        "CREATE INDEX entity_chain_id_idx IF NOT EXISTS FOR (n:Entity) ON (n.chain_id)",
    ),
    (
        "entity_latest_idx",
        "CREATE INDEX entity_latest_idx IF NOT EXISTS FOR (n:Entity) ON (n.is_latest)",
    ),
    (
        "entity_org_id_idx",
        "CREATE INDEX entity_org_id_idx IF NOT EXISTS FOR (n:Entity) ON (n.org_id)",
    ),
    (
        // Reference lookups scan the organization's live versions only.
        "entity_org_latest_idx",
        "CREATE INDEX entity_org_latest_idx IF NOT EXISTS FOR (n:Entity) ON (n.org_id, n.is_latest)",
    ),
    // Learned-rule schema. Also installed by `install_schema` for the SDK/library
    // path, but anchored here so a server prepared only through `ensure_indexes`
    // still gets the uniqueness constraint (concurrent proposals cannot duplicate
    // a rule id) and the lookup index, and so startup verifies them.
    (
        "learned_rule_identity",
        "CREATE CONSTRAINT learned_rule_identity IF NOT EXISTS FOR (r:LearnedRule) REQUIRE (r.org_id, r.id) IS UNIQUE",
    ),
    (
        "learned_rule_active",
        "CREATE INDEX learned_rule_active IF NOT EXISTS FOR (r:LearnedRule) ON (r.org_id, r.source, r.status)",
    ),
];

/// Search indexes that `rebuild_search_indexes` drops and recreates.
pub const SEARCH_INDEXES: &[&str] = &[
    "search_communities",
    "search_community_name_vectors",
    "search_entity_summaries",
    "search_entity_summary_vectors",
    "search_entities",
    "search_relationships",
    "search_snapshots",
    "search_entity_vectors",
    "search_relationship_vectors",
];

/// Oldest server the search statements support: `OPTIONAL CALL` (5.24),
/// vector indexes without a fixed dimension (5.23), and the combined
/// `SHOW TRANSACTIONS ... TERMINATE TRANSACTIONS` command (5.9).
pub const MINIMUM_SERVER_VERSION: (u64, u64) = (5, 24);

pub fn validate_server_version(
    rows: &[serde_json::Map<String, serde_json::Value>],
) -> Result<(), kg_core::errors::BackendError> {
    use kg_core::errors::BackendError;
    let version = rows
        .iter()
        .find(|row| row.get("name").and_then(|v| v.as_str()) == Some("Neo4j Kernel"))
        .and_then(|row| row.get("versions"))
        .and_then(|v| v.as_array())
        .and_then(|v| v.first())
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            BackendError::Unavailable("Neo4j did not report its kernel version".into())
        })?;
    let mut parts = version.split(|c: char| !c.is_ascii_digit());
    let parsed = match (parts.next(), parts.next()) {
        (Some(major), Some(minor)) => major.parse::<u64>().ok().zip(minor.parse::<u64>().ok()),
        _ => None,
    };
    match parsed {
        Some(found) if found >= MINIMUM_SERVER_VERSION => Ok(()),
        _ => Err(BackendError::Unavailable(format!(
            "Neo4j {version} is not supported; search needs Neo4j {}.{} or newer",
            MINIMUM_SERVER_VERSION.0, MINIMUM_SERVER_VERSION.1
        ))),
    }
}

fn index_problem(
    rows: &[serde_json::Map<String, serde_json::Value>],
    name: &str,
    kind: &str,
    compatible: impl Fn(&serde_json::Map<String, serde_json::Value>) -> bool,
) -> Result<(), kg_core::errors::BackendError> {
    use serde_json::json;
    let Some(row) = rows
        .iter()
        .find(|row| row.get("name") == Some(&json!(name)))
    else {
        return Err(kg_core::errors::BackendError::Unavailable(format!(
            "{kind} index {name} is missing; run Neo4jGraphBackend::ensure_indexes() (prepare) to create it"
        )));
    };
    if !compatible(row) {
        return Err(kg_core::errors::BackendError::Unavailable(format!(
            "{kind} index {name} has an incompatible definition; rebuild it deliberately with \
             Neo4jGraphBackend::rebuild_search_indexes(), or run `DROP INDEX {name}` and then \
             ensure_indexes() during a maintenance window. Startup never drops indexes itself."
        )));
    }
    if row.get("state") != Some(&json!("ONLINE")) {
        return Err(kg_core::errors::BackendError::Unavailable(format!(
            "{kind} index {name} is not online (state {}); wait for population or inspect \
             `SHOW INDEXES YIELD name, state, populationPercent, failureMessage`",
            row.get("state").cloned().unwrap_or(json!(null))
        )));
    }
    Ok(())
}

pub fn validate_search_indexes(
    rows: &[serde_json::Map<String, serde_json::Value>],
) -> Result<(), kg_core::errors::BackendError> {
    use serde_json::json;

    for (name, kind, label, content) in [
        ("search_communities", "NODE", "Community", &["name"][..]),
        (
            "search_entity_summaries",
            "NODE",
            "Entity",
            &["derived_summary"][..],
        ),
        (
            "search_entities",
            "NODE",
            "Entity",
            crate::ENTITY_TEXT_FIELDS,
        ),
        (
            "search_relationships",
            "RELATIONSHIP",
            "RELATES_TO",
            crate::RELATIONSHIP_TEXT_FIELDS,
        ),
        (
            "search_snapshots",
            "NODE",
            "Snapshot",
            crate::SNAPSHOT_TEXT_FIELDS,
        ),
    ] {
        let properties: Vec<_> = std::iter::once("org_id")
            .chain(content.iter().copied())
            .collect();
        index_problem(rows, name, "fulltext", |row| {
            row.get("entityType") == Some(&json!(kind))
                && row.get("labelsOrTypes") == Some(&json!([label]))
                && row
                    .get("properties")
                    .and_then(|value| value.as_array())
                    .is_some_and(|actual| {
                        actual.len() == properties.len()
                            && properties
                                .iter()
                                .all(|property| actual.contains(&json!(property)))
                    })
                && row.get("options").is_some_and(|options| {
                    options["indexConfig"]["fulltext.analyzer"] == "standard-no-stop-words"
                        && options["indexConfig"]["fulltext.eventually_consistent"] == false
                })
        })?;
    }
    Ok(())
}

pub fn validate_vector_indexes(
    rows: &[serde_json::Map<String, serde_json::Value>],
) -> Result<(), kg_core::errors::BackendError> {
    use serde_json::json;
    for (name, kind, label, property) in [
        (
            "search_community_name_vectors",
            "NODE",
            "Community",
            "name_embedding",
        ),
        ("search_entity_vectors", "NODE", "Entity", "embedding"),
        (
            "search_relationship_vectors",
            "RELATIONSHIP",
            "RELATES_TO",
            "embedding",
        ),
        (
            "search_entity_summary_vectors",
            "NODE",
            "Entity",
            "summary_embedding",
        ),
    ] {
        index_problem(rows, name, "vector", |r| {
            r.get("entityType") == Some(&json!(kind))
                && r.get("labelsOrTypes") == Some(&json!([label]))
                && r.get("properties") == Some(&json!([property]))
                && r.get("options").is_some_and(|o| {
                    o["indexConfig"]["vector.similarity_function"]
                        .as_str()
                        .is_some_and(|s| s.eq_ignore_ascii_case("cosine"))
                        && o["indexConfig"]["vector.quantization.enabled"] == false
                        && o["indexConfig"]["vector.dimensions"].is_null()
                })
        })?;
    }
    Ok(())
}

/// The scan ledger: one record per collection, holding the newest claimed
/// generation and the run that claimed it. Requires the constraint below.
pub const COLLECTION_SCAN_CONSTRAINT: (&str, &str) = (
    "collection_scan_scope_unique",
    "CREATE CONSTRAINT collection_scan_scope_unique IF NOT EXISTS FOR (n:CollectionScan) REQUIRE n.scope_id IS UNIQUE",
);

const REQUIRED_CONSTRAINTS: &[(&str, &str, &str, &[&str])] = &[
    (
        "learned_rule_identity",
        "NODE",
        "LearnedRule",
        &["org_id", "id"],
    ),
    (
        "connector_checkpoint_scope",
        "NODE",
        "ConnectorCheckpoint",
        &["org_id", "namespace", "key"],
    ),
    (
        "connector_checkpoint_receipt",
        "NODE",
        "ConnectorCheckpointReceipt",
        &["org_id", "namespace", "key", "run_id"],
    ),
    (
        "ingestion_profile_unique",
        "NODE",
        "IngestionProfile",
        &["scope_id"],
    ),
    (
        "reference_dependency_unique",
        "NODE",
        "ReferenceDependency",
        &["org_id", "edge_uuid", "token"],
    ),
    (
        "identity_value_unique",
        "NODE",
        "IdentityValue",
        &["org_id", "token"],
    ),
    (
        "community_scope_unique",
        "NODE",
        "CommunityScope",
        &["org_id", "namespace"],
    ),
    (
        "community_revision_unique",
        "NODE",
        "CommunityRevision",
        &["org_id", "namespace", "stripe"],
    ),
    (
        "community_generation_unique",
        "NODE",
        "CommunityGeneration",
        &["uuid"],
    ),
    (
        "community_partition_unique",
        "NODE",
        "CommunityPartition",
        &["generation_uuid", "partition_index"],
    ),
    ("community_uuid_unique", "NODE", "Community", &["uuid"]),
    (
        "community_membership_unique",
        "RELATIONSHIP",
        "HAS_MEMBER",
        &["membership_key"],
    ),
    ("saga_scope_unique", "NODE", "Saga", &["scope_id"]),
    (
        "unresolved_slot_unique",
        "NODE",
        "UnresolvedSlot",
        &["org_id", "source_chain_id", "slot"],
    ),
    (
        "reference_decision_unique",
        "NODE",
        "ReferenceDecision",
        &["org_id", "decision_id"],
    ),
    ("saga_uuid_unique", "NODE", "Saga", &["uuid"]),
    (
        "saga_membership_uuid_unique",
        "RELATIONSHIP",
        "HAS_EPISODE",
        &["uuid"],
    ),
    (
        "saga_membership_key_unique",
        "RELATIONSHIP",
        "HAS_EPISODE",
        &["membership_key"],
    ),
    (
        "saga_next_uuid_unique",
        "RELATIONSHIP",
        "NEXT_EPISODE",
        &["uuid"],
    ),
    (
        "saga_next_target_unique",
        "RELATIONSHIP",
        "NEXT_EPISODE",
        &["predecessor_key"],
    ),
    (
        "chain_merge_state_unique",
        "NODE",
        "ChainMergeState",
        &["scope_id"],
    ),
    (
        "chain_merge_period_unique",
        "NODE",
        "ChainMerge",
        &["period_id"],
    ),
    (
        "ingestion_run_id_unique",
        "NODE",
        "IngestionRun",
        &["run_id"],
    ),
    (
        "operation_receipt_batch_unique",
        "NODE",
        "OperationReceipt",
        &["batch_id"],
    ),
    (
        "graph_revision_unique",
        "NODE",
        "GraphRevision",
        &["org_id", "stripe"],
    ),
    ("commit_plan_unique", "NODE", "CommitPlan", &["batch_id"]),
    (
        "commit_plan_part_unique",
        "NODE",
        "CommitPlanPart",
        &["batch_id", "ordinal"],
    ),
    (
        "commit_page_receipt_unique",
        "NODE",
        "CommitPageReceipt",
        &["batch_id", "ordinal"],
    ),
    ("graph_node_uuid_unique", "NODE", "GraphNode", &["uuid"]),
    ("snapshot_uuid_unique", "NODE", "Snapshot", &["uuid"]),
    (
        "relationship_uuid_unique",
        "RELATIONSHIP",
        "RELATES_TO",
        &["uuid"],
    ),
    (
        "observation_uuid_unique",
        "RELATIONSHIP",
        "MENTIONS",
        &["uuid"],
    ),
    (
        "entity_hash_version_unique",
        "NODE",
        "Entity",
        &["hash_version"],
    ),
    ("entity_uuid_unique", "NODE", "Entity", &["uuid"]),
    (
        "identity_revision_unique",
        "NODE",
        "IdentityRevision",
        &["scope_id"],
    ),
    (
        "collection_scan_scope_unique",
        "NODE",
        "CollectionScan",
        &["scope_id"],
    ),
];

/// IF NOT EXISTS checks names; startup must also verify what those names enforce.
pub fn validate_constraints(
    rows: &[serde_json::Map<String, serde_json::Value>],
) -> Result<(), kg_core::errors::BackendError> {
    use serde_json::json;
    let identity_valid = rows.iter().any(|row| {
        row.get("name") == Some(&json!("identity_key_unique"))
            && row.get("entityType") == Some(&json!("NODE"))
            && row.get("labelsOrTypes") == Some(&json!(["IdentityKey"]))
            && row.get("properties") == Some(&json!(["org_id", "hash"]))
            && row.get("type") == Some(&json!("UNIQUENESS"))
    });
    if !identity_valid {
        return Err(kg_core::errors::BackendError::Unavailable(
        "required constraint identity_key_unique is missing or incompatible; repair the constraint before startup".into()));
    }
    for (name, entity, label, property) in REQUIRED_CONSTRAINTS {
        let valid = rows.iter().any(|row| {
            row.get("name") == Some(&json!(name))
                && row.get("entityType") == Some(&json!(entity))
                && row.get("labelsOrTypes") == Some(&json!([label]))
                && row.get("properties") == Some(&json!(property))
                && row.get("type").and_then(|value| value.as_str())
                    == Some(if *entity == "NODE" {
                        "UNIQUENESS"
                    } else {
                        "RELATIONSHIP_UNIQUENESS"
                    })
        });
        if !valid {
            return Err(kg_core::errors::BackendError::Unavailable(format!(
                "required constraint {name} is missing or incompatible; inspect SHOW CONSTRAINTS, repair the named constraint during maintenance, then rerun ensure_indexes; startup never drops constraints"
            )));
        }
    }
    Ok(())
}

const RANGE_INDEXES: &[(&str, &str, &str, &[&str])] = &[
    (
        "identity_candidate_live_scope",
        "NODE",
        "Entity",
        &["org_id", "namespace", "is_latest"],
    ),
    (
        "identity_candidate_live_vectors",
        "NODE",
        "Entity",
        &[
            "org_id",
            "namespace",
            "is_latest",
            "embedding_model",
            "embedding_text_version",
        ],
    ),
    (
        "ingestion_profile_scope",
        "NODE",
        "IngestionProfile",
        &["org_id", "profile_id", "revision"],
    ),
    (
        "explorer_entity_scope",
        "NODE",
        "Entity",
        &["org_id", "namespace"],
    ),
    (
        "reference_dependency_token",
        "NODE",
        "ReferenceDependency",
        &["org_id", "token"],
    ),
    (
        "identity_value_folded",
        "NODE",
        "IdentityValue",
        &["org_id", "folded_token"],
    ),
    (
        "reference_decision_reuse_key",
        "NODE",
        "ReferenceDecision",
        &["org_id", "reuse_fingerprint"],
    ),
    (
        "reference_decision_source",
        "NODE",
        "ReferenceDecision",
        &["org_id", "producer_source", "source_chain_id"],
    ),
    (
        "unresolved_reference_token",
        "NODE",
        "UnresolvedReference",
        &["org_id", "token"],
    ),
    (
        "unresolved_reference_source",
        "NODE",
        "UnresolvedReference",
        &["org_id", "source_chain_id", "slot"],
    ),
    (
        "community_maintenance_page",
        "NODE",
        "Community",
        &["org_id", "uuid"],
    ),
    (
        "community_vector_scope",
        "NODE",
        "Community",
        &[
            "org_id",
            "namespace",
            "name_embedding_model",
            "name_embedding_text_version",
        ],
    ),
    (
        "community_incoming",
        "RELATIONSHIP",
        "RELATES_TO",
        &["org_id", "target_chain_id", "uuid"],
    ),
    (
        "community_outgoing",
        "RELATIONSHIP",
        "RELATES_TO",
        &["org_id", "source_chain_id", "uuid"],
    ),
    (
        "community_entity_scope",
        "NODE",
        "Entity",
        &["org_id", "namespace", "uuid"],
    ),
    (
        "community_generation_scope",
        "NODE",
        "Community",
        &["org_id", "namespace", "generation_uuid", "uuid"],
    ),
    (
        "community_member_chain",
        "RELATIONSHIP",
        "HAS_MEMBER",
        &["community_uuid", "chain_id"],
    ),
    (
        "community_generation_chain",
        "RELATIONSHIP",
        "HAS_MEMBER",
        &["generation_uuid", "chain_id"],
    ),
    (
        "saga_member_capture",
        "RELATIONSHIP",
        "HAS_EPISODE",
        &["saga_uuid", "captured_at", "snapshot_created_at", "ordinal"],
    ),
    (
        "saga_member_ordinal",
        "RELATIONSHIP",
        "HAS_EPISODE",
        &["saga_uuid", "ordinal"],
    ),
    (
        "search_entity_summary_scope",
        "NODE",
        "Entity",
        &[
            "org_id",
            "namespace",
            "summary_embedding_model",
            "summary_embedding_text_version",
        ],
    ),
    (
        "identity_candidate_names",
        "NODE",
        "Entity",
        &["org_id", "namespace", "entity_type", "name"],
    ),
    (
        "identity_candidate_vectors",
        "NODE",
        "Entity",
        &[
            "org_id",
            "namespace",
            "entity_type",
            "embedding_model",
            "embedding_text_version",
        ],
    ),
    (
        "chain_merge_loser",
        "NODE",
        "ChainMerge",
        &["org_id", "loser_chain_id"],
    ),
    (
        "chain_merge_winner",
        "NODE",
        "ChainMerge",
        &["org_id", "winner_chain_id"],
    ),
    (
        "operation_receipt_run_idx",
        "NODE",
        "OperationReceipt",
        &["org_id", "run_id"],
    ),
    (
        "search_org_chain",
        "NODE",
        "Entity",
        &["org_id", "chain_id"],
    ),
    (
        "search_entity_embedding_scope",
        "NODE",
        "Entity",
        &[
            "org_id",
            "namespace",
            "embedding_model",
            "embedding_text_version",
        ],
    ),
    (
        "search_relationship_org",
        "RELATIONSHIP",
        "RELATES_TO",
        &["org_id"],
    ),
    (
        "search_relationship_embedding",
        "RELATIONSHIP",
        "RELATES_TO",
        &["org_id", "embedding_model", "embedding_text_version"],
    ),
    ("search_snapshot_org", "NODE", "Snapshot", &["org_id"]),
    (
        "entity_identity_hash_idx",
        "NODE",
        "Entity",
        &["identity_hash"],
    ),
    (
        "learned_rule_active",
        "NODE",
        "LearnedRule",
        &["org_id", "source", "status"],
    ),
    ("entity_chain_id_idx", "NODE", "Entity", &["chain_id"]),
    ("entity_latest_idx", "NODE", "Entity", &["is_latest"]),
    ("entity_org_id_idx", "NODE", "Entity", &["org_id"]),
    (
        "entity_org_latest_idx",
        "NODE",
        "Entity",
        &["org_id", "is_latest"],
    ),
];

pub fn validate_range_indexes(
    rows: &[serde_json::Map<String, serde_json::Value>],
) -> Result<(), kg_core::errors::BackendError> {
    use serde_json::json;
    for (name, kind, label, properties) in RANGE_INDEXES {
        let valid = rows.iter().any(|r| {
            r.get("name") == Some(&json!(name))
                && r.get("type") == Some(&json!("RANGE"))
                && r.get("state") == Some(&json!("ONLINE"))
                && r.get("entityType") == Some(&json!(kind))
                && r.get("labelsOrTypes") == Some(&json!([label]))
                && r.get("properties") == Some(&json!(properties))
        });
        if !valid {
            return Err(kg_core::errors::BackendError::Unavailable(format!(
            "range index {name} is missing, offline, or incompatible; inspect SHOW INDEXES, repair only this index during maintenance, then rerun ensure_indexes")));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Map, Value};

    #[test]
    fn vector_readiness_checks_metric_dimensions_and_schema() {
        let rows: Vec<Map<String,Value>> = [("search_community_name_vectors","NODE","Community","name_embedding"),("search_entity_vectors","NODE","Entity","embedding"),("search_relationship_vectors","RELATIONSHIP","RELATES_TO","embedding"),("search_entity_summary_vectors","NODE","Entity","summary_embedding")].into_iter().map(|(name,kind,label,property)|json!({"name":name,"state":"ONLINE","entityType":kind,"labelsOrTypes":[label],"properties":[property],"options":{"indexConfig":{"vector.similarity_function":"COSINE","vector.quantization.enabled":false}}}).as_object().unwrap().clone()).collect();
        assert!(validate_vector_indexes(&rows).is_ok());
        assert!(validate_vector_indexes(&rows[..1]).is_err());
        for (key, value) in [
            ("vector.similarity_function", json!("euclidean")),
            ("vector.dimensions", json!(1536)),
            ("vector.quantization.enabled", json!(true)),
        ] {
            let mut invalid = rows.clone();
            invalid[0].get_mut("options").unwrap()["indexConfig"][key] = value;
            assert!(validate_vector_indexes(&invalid).is_err());
        }
    }

    fn ready_indexes() -> Vec<Map<String, Value>> {
        [
            (
                "search_entities",
                "NODE",
                "Entity",
                json!(["org_id", "name", "summary", "prop_summary"]),
            ),
            (
                "search_relationships",
                "RELATIONSHIP",
                "RELATES_TO",
                json!(["org_id", "name", "description"]),
            ),
            (
                "search_snapshots",
                "NODE",
                "Snapshot",
                json!(["org_id", "name", "source", "content"]),
            ),
            (
                "search_entity_summaries",
                "NODE",
                "Entity",
                json!(["org_id", "derived_summary"]),
            ),
            (
                "search_communities",
                "NODE",
                "Community",
                json!(["org_id", "name"]),
            ),
        ]
        .into_iter()
        .map(|(name, kind, label, properties)| {
            json!({
                "name": name, "state": "ONLINE", "entityType": kind,
                "labelsOrTypes": [label], "properties": properties,
                "options": {"indexConfig": {
                    "fulltext.analyzer": "standard-no-stop-words",
                    "fulltext.eventually_consistent": false
                }}
            })
            .as_object()
            .unwrap()
            .clone()
        })
        .collect()
    }

    #[test]
    fn search_index_readiness_accepts_equivalent_property_order() {
        let mut rows = ready_indexes();
        assert!(validate_search_indexes(&rows).is_ok());
        for row in &mut rows {
            row.get_mut("properties")
                .unwrap()
                .as_array_mut()
                .unwrap()
                .reverse();
        }
        assert!(validate_search_indexes(&rows).is_ok());
    }

    #[test]
    fn problems_name_the_index_and_the_remediation() {
        let rows = ready_indexes();
        let missing = validate_search_indexes(&rows[1..]).unwrap_err().to_string();
        assert!(missing.contains("search_entities is missing"));
        assert!(missing.contains("ensure_indexes"));
        let mut incompatible = ready_indexes();
        incompatible[0].insert("properties".into(), json!(["name"]));
        let message = validate_search_indexes(&incompatible)
            .unwrap_err()
            .to_string();
        assert!(message.contains("search_entities has an incompatible definition"));
        assert!(message.contains("rebuild_search_indexes"));
        assert!(message.contains("DROP INDEX search_entities"));
        let mut populating = ready_indexes();
        populating[2].insert("state".into(), json!("POPULATING"));
        let message = validate_search_indexes(&populating)
            .unwrap_err()
            .to_string();
        assert!(message.contains("search_snapshots is not online (state \"POPULATING\")"));
        assert!(!message.contains("DROP INDEX"));
    }

    #[test]
    fn server_version_gate_accepts_supported_kernels_only() {
        let components = |version: &str| {
            vec![
                json!({"name": "Neo4j Kernel", "versions": [version], "edition": "community"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ]
        };
        for ok in ["5.24.0", "5.26.27", "2025.01.0", "6.0.0-beta"] {
            assert!(validate_server_version(&components(ok)).is_ok(), "{ok}");
        }
        for old in ["5.23.1", "5.9.0", "4.4.40", "garbage"] {
            let message = validate_server_version(&components(old))
                .unwrap_err()
                .to_string();
            assert!(message.contains("5.24 or newer"), "{old}: {message}");
        }
        assert!(validate_server_version(&[]).is_err());
    }

    #[test]
    fn search_index_readiness_rejects_missing_unready_and_incompatible_indexes() {
        assert!(validate_search_indexes(&[]).is_err());
        for index in 0..ready_indexes().len() {
            let mut rows = ready_indexes();
            rows.remove(index);
            assert!(validate_search_indexes(&rows).is_err());
            for (field, value) in [
                ("state", json!("POPULATING")),
                ("state", json!("FAILED")),
                ("entityType", json!("WRONG")),
                ("labelsOrTypes", json!(["WrongLabel"])),
                ("properties", json!(["name"])),
                ("properties", json!(["name", "description", "unintended"])),
                ("properties", json!(["name", "name"])),
                (
                    "options",
                    json!({"indexConfig": {"fulltext.analyzer": "standard",
                    "fulltext.eventually_consistent": false}}),
                ),
                (
                    "options",
                    json!({"indexConfig": {"fulltext.analyzer": "standard-no-stop-words",
                    "fulltext.eventually_consistent": true}}),
                ),
            ] {
                let mut rows = ready_indexes();
                rows[index].insert(field.into(), value);
                assert!(validate_search_indexes(&rows).is_err(), "{index}: {field}");
            }
            for field in [
                "name",
                "state",
                "entityType",
                "labelsOrTypes",
                "properties",
                "options",
            ] {
                let mut rows = ready_indexes();
                rows[index].remove(field);
                assert!(validate_search_indexes(&rows).is_err(), "missing {field}");
            }
        }
    }

    // ---- merged from `mod constraint_tests`

    #[test]
    fn every_installed_constraint_is_checked_and_wrong_definitions_fail() {
        let mut rows: Vec<_> = REQUIRED_CONSTRAINTS.iter().map(|(name, entity, label, property)| {
            json!({"name":name, "entityType":entity, "labelsOrTypes":[label], "properties":property, "type": if *entity == "NODE" {"UNIQUENESS"} else {"RELATIONSHIP_UNIQUENESS"}}).as_object().unwrap().clone()
        }).collect();
        rows.push(json!({"name":"identity_key_unique","entityType":"NODE","labelsOrTypes":["IdentityKey"],"properties":["org_id","hash"],"type":"UNIQUENESS"}).as_object().unwrap().clone());
        validate_constraints(&rows).unwrap();
        for (name, ddl) in INDEXES
            .iter()
            .chain(std::iter::once(&COLLECTION_SCAN_CONSTRAINT))
        {
            if ddl.starts_with("CREATE CONSTRAINT") {
                assert!(
                    *name == "identity_key_unique"
                        || REQUIRED_CONSTRAINTS.iter().any(|spec| spec.0 == *name),
                    "{name}"
                );
            }
        }
        rows.iter_mut()
            .find(|row| row.get("name") == Some(&json!("operation_receipt_batch_unique")))
            .unwrap()
            .insert("properties".into(), json!(["wrong"]));
        assert!(validate_constraints(&rows)
            .unwrap_err()
            .to_string()
            .contains("operation_receipt_batch_unique"));
        assert!(validate_constraints(&[]).is_err());
    }

    // ---- merged from `mod range_tests`

    #[test]
    fn every_range_definition_is_verified_including_property_order() {
        let rows: Vec<_> = RANGE_INDEXES.iter().map(|(name,kind,label,properties)|json!({"name":name,"type":"RANGE","state":"ONLINE","entityType":kind,"labelsOrTypes":[label],"properties":properties}).as_object().unwrap().clone()).collect();
        validate_range_indexes(&rows).unwrap();
        for (name, ddl) in INDEXES {
            if ddl.starts_with("CREATE INDEX") {
                assert!(RANGE_INDEXES.iter().any(|r| r.0 == *name), "{name}");
            }
        }
        for index in 0..rows.len() {
            for (key, value) in [
                ("type", json!("TEXT")),
                ("state", json!("POPULATING")),
                ("entityType", json!("wrong")),
                ("labelsOrTypes", json!(["Wrong"])),
                ("properties", json!(["wrong"])),
            ] {
                let mut invalid = rows.clone();
                invalid[index].insert(key.into(), value);
                assert!(validate_range_indexes(&invalid)
                    .unwrap_err()
                    .to_string()
                    .contains(RANGE_INDEXES[index].0));
            }
        }
        assert!(validate_range_indexes(&[]).is_err());
    }
}
