use async_trait::async_trait;
use kg_core::{
    errors::StageError,
    runtime::{RuntimeContext, StageOutput},
    traits::Stage,
};

/// Match authoritative keys across the complete chunk before deciding versions.
pub struct ResolveNodesStage;

#[async_trait]
impl Stage for ResolveNodesStage {
    fn processing_version(&self) -> String {
        "authoritative-identity-conflicts-v3-isolated".into()
    }
    fn contract(&self) -> kg_core::traits::StageContract {
        use kg_core::traits::StageKind;
        &[(StageKind::NodeExtraction, StageKind::NodeIdentity)]
    }

    fn name(&self) -> &str {
        "resolve_nodes"
    }
    fn is_batch(&self) -> bool {
        true
    }

    async fn process(
        &self,
        input: StageOutput,
        ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        self.process_batch(vec![input], ctx)
            .await?
            .pop()
            .ok_or_else(|| StageError::StateValidation {
                stage: self.name().into(),
                message: "missing identity output".into(),
            })?
    }

    async fn process_batch(
        &self,
        inputs: Vec<StageOutput>,
        ctx: &RuntimeContext,
    ) -> Result<Vec<Result<StageOutput, kg_core::errors::StageError>>, StageError> {
        let extractions = inputs
            .into_iter()
            .map(|input| match input {
                StageOutput::NodeExtraction(extraction) => Ok(extraction),
                _ => Err(StageError::StateValidation {
                    stage: self.name().into(),
                    message: "expected entity extraction".into(),
                }),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(identity_batch::match_batch(extractions, ctx)
            .await?
            .into_iter()
            .map(|output| output.map(StageOutput::NodeIdentity))
            .collect())
    }
}

mod identity_batch {
    //! Authoritative identity matching over all observations in a pipeline chunk.
    use std::collections::{BTreeSet, HashMap, HashSet};
    use std::sync::Arc;

    use kg_core::{
        errors::StageError,
        identity::IdentityHash,
        models::EntityNode,
        runtime::{
            stage_output::{
                IdentityMatch, IdentityOutcome, NodeExtractionOutput, NodeIdentityOutput,
            },
            RuntimeContext,
        },
        traits::{
            graph_reads::MAX_LOOKUP_KEYS, EntityLookup, EntityVersionRecord, IdentityRevision,
            IdentityScope, VersionState,
        },
    };
    use uuid::Uuid;

    fn invalid(message: &str) -> StageError {
        StageError::StateValidation {
            stage: "resolve_nodes".into(),
            message: message.into(),
        }
    }

    fn keys(entity: &EntityNode, ctx: &RuntimeContext) -> Result<Vec<IdentityHash>, StageError> {
        if entity.org_id != ctx.org_id.as_ref() {
            return Err(invalid("entity scope mismatch"));
        }
        if !entity.has_authoritative_keys() {
            return Ok(Vec::new());
        }
        let mut keys = entity
            .additional_identity_hashes()
            .map_err(|_| invalid("invalid authoritative keys"))?;
        keys.push(entity.identity_hash);
        Ok(keys)
    }

    fn root(parents: &mut [usize], mut index: usize) -> usize {
        while parents[index] != index {
            parents[index] = parents[parents[index]];
            index = parents[index];
        }
        index
    }

    fn preferred<'a>(
        left: &'a EntityVersionRecord,
        right: &'a EntityVersionRecord,
    ) -> &'a EntityVersionRecord {
        if (right.deleted_at.is_none(), right.version, right.uuid)
            > (left.deleted_at.is_none(), left.version, left.uuid)
        {
            right
        } else {
            left
        }
    }

    /// Preserve observations while assigning a single identity to each connected key set.
    /// Deletions participate in stored lookup only: an unknown deletion cannot bridge creations.
    pub(crate) async fn match_batch(
        extractions: Vec<NodeExtractionOutput>,
        ctx: &RuntimeContext,
    ) -> Result<Vec<Result<NodeIdentityOutput, StageError>>, StageError> {
        if ctx.cancel.is_cancelled() {
            return Err(StageError::Cancelled {
                stage: "resolve_nodes".into(),
            });
        }

        let entities: Vec<_> = extractions
            .iter()
            .flat_map(|e| e.entities_by_snapshot.iter().flat_map(|(_, nodes)| nodes))
            .collect();
        let entity_keys: Vec<_> = entities
            .iter()
            .map(|entity| keys(entity, ctx))
            .collect::<Result<_, _>>()?;
        let mut wanted: Vec<_> = entity_keys.iter().flatten().copied().collect();
        for extraction in &extractions {
            for entity in extraction.source_deleted.iter() {
                wanted.extend(keys(entity, ctx)?);
            }
        }
        wanted.sort_by_key(IdentityHash::to_hex);
        wanted.dedup();
        let identity_revisions = Arc::new(capture_revisions(&extractions, ctx).await?);
        let mut records: HashMap<IdentityHash, Vec<EntityVersionRecord>> = HashMap::new();
        for state in [VersionState::Live, VersionState::Deleted] {
            for hashes in wanted.chunks(MAX_LOOKUP_KEYS) {
                let lookup = EntityLookup::ByIdentity {
                    hashes: hashes.iter().map(IdentityHash::to_hex).collect(),
                    state,
                };
                let found = tokio::select! {
                    biased;
                    _ = ctx.cancel.cancelled() => return Err(StageError::Cancelled { stage: "resolve_nodes".into() }),
                    result = tokio::time::timeout(std::time::Duration::from_millis(ctx.matching_settings.timeout_ms), ctx.graph.find_entities(ctx.org_id.as_ref(), &lookup)) => result
                    .map_err(|_| StageError::StepFailed {
                        stage: "resolve_nodes".into(), step: "identity_hash_match".into(),
                        cause: "authoritative identity lookup timed out".into(), retriable: true,
                    })?
                    .map_err(|error| StageError::StepFailed {
                        stage: "resolve_nodes".into(), step: "identity_hash_match".into(),
                        cause: error.to_string(), retriable: error.is_transient(),
                    })?,
                };
                for record in found {
                    for hash in record.answering_hashes() {
                        let hash: IdentityHash =
                            serde_json::from_value(serde_json::Value::String(hash.into()))
                                .map_err(|_| invalid("stored identity hash is invalid"))?;
                        records.entry(hash).or_default().push(record.clone());
                    }
                }
            }
        }
        let mut parents: Vec<_> = (0..entities.len()).collect();
        let mut owner = HashMap::new();
        for (index, hashes) in entity_keys.iter().enumerate() {
            for hash in hashes {
                if let Some(prior) = owner.insert(*hash, index) {
                    let a = root(&mut parents, prior);
                    let b = root(&mut parents, index);
                    parents[b] = a;
                }
            }
        }
        let mut component_errors = HashMap::new();
        let mut component_records: HashMap<usize, EntityVersionRecord> = HashMap::new();
        let mut component_chains: HashMap<usize, Uuid> = HashMap::new();
        for (index, entity) in entities.iter().enumerate() {
            let component = root(&mut parents, index);
            component_chains
                .entry(component)
                .and_modify(|id| *id = (*id).min(entity.chain_id))
                .or_insert(entity.chain_id);
            for hash in &entity_keys[index] {
                for record in records.get(hash).into_iter().flatten() {
                    if record.namespace != entity.namespace
                        || record.entity_type != entity.entity_type
                    {
                        return Err(invalid("stored identity scope mismatch"));
                    }
                    if record.deleted_at.is_some()
                        && record.identity_hash.as_deref()
                            != Some(entity.identity_hash.to_hex().as_str())
                    {
                        if let Some(candidate) =
                            super::super::matching_candidates::candidate_from_record(record)?
                        {
                            if entity.primary_key_properties.iter().chain(entity.additional_key_properties.iter().flatten())
                                .chain(candidate.primary_keys.iter()).chain(candidate.additional_keys.iter().flatten())
                                .filter(|key| key.as_str() != "name")
                                .any(|key| matches!((entity.all_properties.get(key), candidate.key_values.get(key)), (Some(value), Some(stored)) if serde_json::to_value(value).ok().as_ref() != Some(stored)))
                            {
                                if record.deleted_at.max(record.last_transition_at).is_some_and(|at| entity.valid_from <= at) {
                                    component_errors.insert(component, "reused identity precedes the previous resource deletion");
                                }
                                // A reused alternate key cannot resurrect a different primary resource.
                                // IdentityKey locking still prevents two live owners at commit.
                                continue;
                            }
                        }
                    }
                    if let Some(existing) = component_records.get(&component) {
                        if existing.chain_id != record.chain_id {
                            component_errors.insert(
                                component,
                                "authoritative keys identify different entities",
                            );
                            continue;
                        }
                        component_records.insert(component, preferred(existing, record).clone());
                    } else {
                        component_records.insert(component, record.clone());
                    }
                }
            }
        }
        // Validate deletion identities before publishing any surviving result.
        // Deletions never join otherwise independent new-identity components.
        let mut rejected = HashMap::new();
        let mut deletion_matches = HashMap::new();
        for (input_index, extraction) in extractions.iter().enumerate() {
            for entity in extraction.source_deleted.iter() {
                let deletion_keys = if records.contains_key(&entity.identity_hash) {
                    vec![entity.identity_hash]
                } else {
                    keys(entity, ctx)?
                };
                let mut existing: Option<&EntityVersionRecord> = None;
                for hash in deletion_keys {
                    for record in records.get(&hash).into_iter().flatten() {
                        if record.namespace != entity.namespace
                            || record.entity_type != entity.entity_type
                        {
                            return Err(invalid("stored identity scope mismatch"));
                        }
                        if let Some(prior) = existing {
                            if prior.chain_id != record.chain_id {
                                rejected.insert(
                                    input_index,
                                    "authoritative keys identify different entities",
                                );
                            }
                            existing = Some(preferred(prior, record));
                        } else {
                            existing = Some(record);
                        }
                    }
                }
                if let Some(record) = existing {
                    deletion_matches.insert(entity.uuid, record.clone());
                }
            }
        }
        // A rejected snapshot cannot supply identity evidence to another snapshot.
        // Propagate through all its key components until the survivor set is stable.
        let mut components_by_input = Vec::new();
        let mut entity_index = 0;
        for extraction in &extractions {
            let mut components = HashSet::new();
            for (_, nodes) in extraction.entities_by_snapshot.iter() {
                for _ in nodes {
                    components.insert(root(&mut parents, entity_index));
                    entity_index += 1;
                }
            }
            components_by_input.push(components);
        }
        loop {
            let before = (rejected.len(), component_errors.len());
            for (index, components) in components_by_input.iter().enumerate() {
                if let Some(reason) = components
                    .iter()
                    .find_map(|c| component_errors.get(c))
                    .copied()
                {
                    rejected.entry(index).or_insert(reason);
                }
                if rejected.contains_key(&index) {
                    for component in components {
                        component_errors
                            .entry(*component)
                            .or_insert("identity evidence belongs to a rejected snapshot");
                    }
                }
            }
            if before == (rejected.len(), component_errors.len()) {
                break;
            }
        }
        let mut matches = HashMap::new();
        let mut methods = HashMap::new();
        let mut ids = HashSet::new();
        for (index, entity) in entities.iter().enumerate() {
            if !ids.insert(entity.uuid) {
                return Err(invalid("duplicate observation UUID"));
            }
            let component = root(&mut parents, index);
            let existing = component_records.get(&component).cloned();
            let chain_id = existing
                .as_ref()
                .map_or(component_chains[&component], |record| record.chain_id);
            let method = if !entity.has_authoritative_keys() {
                "unresolved"
            } else if existing.is_some() {
                if records.contains_key(&entity.identity_hash) {
                    "identity_hash"
                } else if entity_keys[index]
                    .iter()
                    .any(|hash| records.contains_key(hash))
                {
                    "alt_key"
                } else {
                    "in_batch"
                }
            } else if chain_id != entity.chain_id {
                "in_batch"
            } else {
                "new"
            };
            matches.insert(
                entity.uuid,
                IdentityMatch {
                    outcome: if !entity.has_authoritative_keys() {
                        IdentityOutcome::Unresolved
                    } else if existing.is_some() {
                        IdentityOutcome::Matched
                    } else {
                        IdentityOutcome::New
                    },
                    chain_id,
                    existing,
                },
            );
            methods.insert(entity.uuid, method.into());
        }
        for entity in extractions.iter().flat_map(|e| e.source_deleted.iter()) {
            if !ids.insert(entity.uuid) {
                return Err(invalid("duplicate observation UUID"));
            }
        }
        let mut output = Vec::with_capacity(extractions.len());
        for (input_index, extraction) in extractions.into_iter().enumerate() {
            if let Some(reason) = rejected.get(&input_index) {
                output.push(Err(invalid(reason)));
                continue;
            }
            let mut local_matches = HashMap::new();
            let mut local_methods = HashMap::new();
            for (_, nodes) in extraction.entities_by_snapshot.iter() {
                for entity in nodes {
                    local_matches.insert(
                        entity.uuid,
                        matches.remove(&entity.uuid).expect("validated observation"),
                    );
                    local_methods.insert(
                        entity.uuid,
                        methods.remove(&entity.uuid).expect("validated observation"),
                    );
                }
            }
            for entity in extraction.source_deleted.iter() {
                if let Some(existing) = deletion_matches.get(&entity.uuid) {
                    local_matches.insert(
                        entity.uuid,
                        IdentityMatch {
                            outcome: IdentityOutcome::Matched,
                            chain_id: existing.chain_id,
                            existing: Some(existing.clone()),
                        },
                    );
                    local_methods.insert(
                        entity.uuid,
                        if records.contains_key(&entity.identity_hash) {
                            "identity_hash"
                        } else {
                            "alt_key"
                        }
                        .into(),
                    );
                }
            }
            let observations = extraction
                .entities_by_snapshot
                .iter()
                .flat_map(|(snapshot, nodes)| nodes.iter().map(move |entity| (*snapshot, entity)))
                .chain(extraction.source_deleted.iter().filter_map(|entity| {
                    entity
                        .last_seen_snapshot_id
                        .map(|snapshot| (snapshot, entity))
                }))
                .map(|(snapshot, entity)| {
                    let mut observed =
                        kg_core::runtime::stage_output::ObservedEntityProperties::from_entity(
                            entity, snapshot,
                        );
                    observed.raw_mention_id = extraction
                        .raw_text_drafts
                        .iter()
                        .flat_map(|draft| draft.mentions.iter())
                        .find(|mention| mention.observation_uuid == entity.uuid)
                        .map(|mention| mention.id);
                    observed.version_exclusions = extraction
                        .version_exclusions
                        .get(&entity.uuid)
                        .cloned()
                        .ok_or_else(|| StageError::StateValidation {
                            stage: "resolve_nodes".into(),
                            message: "missing observation version exclusions".into(),
                        })?;
                    Ok((entity.uuid, observed))
                })
                .collect::<Result<_, StageError>>()?;
            output.push(Ok(NodeIdentityOutput {
                identity_revisions: identity_revisions.clone(),
                observations,
                extraction,
                matches: local_matches,
                methods: local_methods,
                chains_merged: Vec::new(),
            }));
        }
        Ok(output)
    }

    /// Capture the whole chunk before any graph identity read, including exact matches
    /// that may later supply evidence for another observation.
    async fn capture_revisions(
        extractions: &[NodeExtractionOutput],
        ctx: &RuntimeContext,
    ) -> Result<Vec<IdentityRevision>, StageError> {
        let scopes: BTreeSet<_> = extractions
            .iter()
            .flat_map(|e| {
                e.entities_by_snapshot
                    .iter()
                    .flat_map(|(_, nodes)| nodes.iter())
                    .chain(e.source_deleted.iter())
            })
            .flat_map(|e| {
                [
                    IdentityScope {
                        namespace: e.namespace.clone(),
                        entity_type: e.entity_type.clone(),
                    },
                    IdentityScope {
                        namespace: e.namespace.clone(),
                        entity_type: "*".into(),
                    },
                ]
            })
            .collect();
        let scopes: Vec<_> = scopes.into_iter().collect();
        let mut revisions = Vec::with_capacity(scopes.len());
        for requested in scopes.chunks(MAX_LOOKUP_KEYS) {
            let response = tokio::select! {
                biased;
                _ = ctx.cancel.cancelled() => return Err(StageError::Cancelled { stage: "resolve_nodes".into() }),
                result = tokio::time::timeout(std::time::Duration::from_millis(ctx.matching_settings.timeout_ms), ctx.graph.identity_revisions(&ctx.org_id, requested)) => result
                    .map_err(|_| StageError::StepFailed { stage: "resolve_nodes".into(), step: "identity_revisions".into(), cause: "identity revision lookup timed out".into(), retriable: true })?
                    .map_err(|e| StageError::StepFailed { stage: "resolve_nodes".into(), step: "identity_revisions".into(), cause: "identity revision lookup failed".into(), retriable: e.is_transient() })?,
            };
            let mut seen = BTreeSet::new();
            for revision in &response {
                revision
                    .validate()
                    .map_err(|_| invalid("invalid identity revision response"))?;
                if requested.binary_search(&revision.scope).is_err()
                    || !seen.insert(revision.scope.clone())
                {
                    return Err(invalid("unexpected or duplicate identity revision scope"));
                }
            }
            if seen.len() != requested.len() {
                return Err(invalid("missing identity revision scope"));
            }
            revisions.extend(response);
        }
        revisions.sort_by(|a, b| a.scope.cmp(&b.scope));
        Ok(revisions)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use async_trait::async_trait;
        use chrono::Utc;
        use kg_core::{
            enums::EntityLifecycle,
            errors::BackendError,
            models::PropertyValue,
            runtime::RuntimeContextBuilder,
            test_support::{MockEmbedBackend, MockLlmBackend},
            traits::*,
        };
        use std::sync::Arc;
        async fn match_batch(
            inputs: Vec<NodeExtractionOutput>,
            ctx: &RuntimeContext,
        ) -> Result<Vec<NodeIdentityOutput>, StageError> {
            super::match_batch(inputs, ctx).await?.into_iter().collect()
        }
        struct RecordedGraph {
            records: Vec<EntityVersionRecord>,
            calls: std::sync::atomic::AtomicUsize,
            revision_response: std::sync::Mutex<Option<Vec<IdentityRevision>>>,
            revision_reads: std::sync::atomic::AtomicUsize,
        }

        fn no_graph<T>() -> Result<T, BackendError> {
            panic!("validation must not call storage")
        }

        #[async_trait]
        impl SearchBackend for RecordedGraph {}

        #[async_trait]
        impl GraphBackend for RecordedGraph {
            async fn identity_revisions(
                &self,
                _: &str,
                scopes: &[IdentityScope],
            ) -> Result<Vec<IdentityRevision>, BackendError> {
                self.revision_reads
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if let Some(response) = self.revision_response.lock().unwrap().clone() {
                    return Ok(response);
                }
                Ok(scopes
                    .iter()
                    .cloned()
                    .map(|scope| IdentityRevision { scope, revision: 0 })
                    .collect())
            }

            async fn apply_mutations(
                &self,
                _: &str,
                _: &[GraphMutation],
            ) -> Result<(), BackendError> {
                no_graph()
            }

            async fn register_run(&self, _: &RunHeader) -> Result<RunRegistration, BackendError> {
                no_graph()
            }

            async fn commit_batch(
                &self,
                _: &MutationBatch,
            ) -> Result<CommittedBatch, BackendError> {
                no_graph()
            }

            async fn committed_batches(
                &self,
                _: &str,
                _: Uuid,
            ) -> Result<Vec<CommittedBatch>, BackendError> {
                no_graph()
            }

            async fn find_entities(
                &self,
                org: &str,
                lookup: &EntityLookup,
            ) -> Result<Vec<EntityVersionRecord>, BackendError> {
                assert_eq!(org, "org");
                assert!(
                    self.revision_reads
                        .load(std::sync::atomic::Ordering::SeqCst)
                        > 0,
                    "revisions must precede graph lookups"
                );
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let EntityLookup::ByIdentity { hashes, state } = lookup else {
                    panic!("unexpected lookup")
                };
                Ok(self
                    .records
                    .iter()
                    .filter(|record| {
                        (record.deleted_at.is_some() == (*state == VersionState::Deleted))
                            && record
                                .answering_hashes()
                                .any(|hash| hashes.iter().any(|wanted| wanted == hash))
                    })
                    .cloned()
                    .collect())
            }

            async fn find_edges(
                &self,
                _: &str,
                _: &EdgeLookup,
            ) -> Result<Vec<EdgeRecord>, BackendError> {
                no_graph()
            }

            async fn health(&self) -> Result<(), BackendError> {
                no_graph()
            }

            async fn connect(&self) -> Result<(), BackendError> {
                no_graph()
            }

            async fn close(&self) -> Result<(), BackendError> {
                Ok(())
            }
        }

        fn entity(name: &str, replicas: i64) -> EntityNode {
            let mut all_properties = indexmap::IndexMap::new();
            all_properties.insert("replicas".to_string(), PropertyValue::Integer(replicas));
            EntityNode {
                labels: Vec::new(),
                inherited_labels: Vec::new(),
                uuid: Uuid::new_v4(),
                chain_id: Uuid::new_v4(),
                org_id: "org".into(),
                namespace: "ns".into(),
                entity_type: "Type".into(),
                name: name.into(),
                all_properties,
                primary_key_properties: vec!["name".into()],
                additional_key_properties: vec![],
                identity_hash: IdentityHash::compute("org", "ns", "Type", &[("name", name)]),
                lifecycle: EntityLifecycle::Active,
                version: 1,
                is_latest: true,
                previous_version_uuid: None,
                embedding: None,
                valid_from: Utc::now(),
                valid_to: None,
                deleted_at: None,
                deleted_by: None,
                deletion_reason: None,
                source: "test".into(),
                extracted_by: "test".into(),
                resolved_by: None,
                first_seen_snapshot_id: None,
                last_seen_snapshot_id: None,
                last_seen_at: None,
                sync_generation: None,
                tags: indexmap::IndexMap::new(),
                summary: None,
                // Distinct per content.
                structural_hash: replicas as u64 + 1000,
                needs_llm_review: false,
                collections: Vec::new(),
            }
        }

        fn extraction(entity: EntityNode) -> NodeExtractionOutput {
            NodeExtractionOutput {
                raw_text_drafts: Default::default(),
                relationship_changes: Default::default(),
                version_exclusions: Arc::new(HashMap::from([(entity.uuid, Vec::new())])),
                text_observation_ids: Default::default(),
                fk_exclusions: Default::default(),
                schemas: Default::default(),
                history: Default::default(),
                snapshot_nodes: Default::default(),
                entities_by_snapshot: Arc::new(vec![(Uuid::new_v4(), vec![entity])]),
                source_deleted: Default::default(),
                sub_edges: Default::default(),
                incomplete_extractions: Default::default(),
            }
        }
        fn context(records: Vec<EntityVersionRecord>) -> (RuntimeContext, Arc<RecordedGraph>) {
            let graph = Arc::new(RecordedGraph {
                records,
                calls: Default::default(),
                revision_response: Default::default(),
                revision_reads: Default::default(),
            });
            let ctx = RuntimeContextBuilder::new("org")
                .graph(graph.clone())
                .llm_default(Arc::new(MockLlmBackend::empty()))
                .llm_extraction(Arc::new(MockLlmBackend::empty()))
                .embedder(Arc::new(MockEmbedBackend::default_dimension()))
                .build()
                .unwrap();
            (ctx, graph)
        }
        fn stored(entity: &EntityNode) -> EntityVersionRecord {
            EntityVersionRecord {
                uuid: entity.uuid,
                chain_id: entity.chain_id,
                version: 1,
                is_latest: true,
                entity_type: entity.entity_type.clone(),
                name: entity.name.clone(),
                namespace: entity.namespace.clone(),
                source: Some(entity.source.clone()),
                identity_hash: Some(entity.identity_hash.to_hex()),
                identity_hashes: vec![],
                structural_hash: None,
                valid_from: Some(entity.valid_from),
                valid_to: None,
                deleted_at: None,
                last_seen_at: None,
                last_transition_at: None,
                sync_generation: None,
                collections: vec![],
                merged_into: None,
                embedding: None,
                stored: Default::default(),
            }
        }
        fn alias(entity: &mut EntityNode, value: &str) {
            entity
                .all_properties
                .insert("alias".into(), PropertyValue::String(value.into()));
            entity.additional_key_properties = vec![vec!["alias".into()]];
        }
        #[tokio::test]
        async fn stored_chain_propagates_new_alias_across_chunk_in_either_order() {
            let persisted = entity("A", 1);
            let mut a = entity("A", 2);
            alias(&mut a, "X");
            let mut b = entity("B", 3);
            alias(&mut b, "X");
            for incoming in [vec![a.clone(), b.clone()], vec![b.clone(), a.clone()]] {
                let (ctx, graph) = context(vec![stored(&persisted)]);
                let output = match_batch(incoming.iter().cloned().map(extraction).collect(), &ctx)
                    .await
                    .unwrap();
                assert_eq!(graph.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
                for (result, original) in output.iter().zip(&incoming) {
                    assert_eq!(result.matches[&original.uuid].chain_id, persisted.chain_id);
                    assert_eq!(
                        result.matches[&original.uuid]
                            .existing
                            .as_ref()
                            .unwrap()
                            .uuid,
                        persisted.uuid
                    );
                    assert_eq!(
                        result.extraction.entities_by_snapshot[0].1[0].uuid,
                        original.uuid
                    );
                    assert_eq!(
                        result.extraction.entities_by_snapshot[0].1[0].all_properties,
                        original.all_properties
                    );
                }
            }
        }
        #[tokio::test]
        async fn transitive_alias_bridge_between_two_stored_chains_is_rejected() {
            let mut a = entity("A", 1);
            let mut b = entity("B", 1);
            let records = vec![stored(&a), stored(&b)];
            alias(&mut a, "X");
            alias(&mut b, "X");
            for incoming in [vec![a.clone(), b.clone()], vec![b.clone(), a.clone()]] {
                let (ctx, _) = context(records.clone());
                let error = match_batch(incoming.into_iter().map(extraction).collect(), &ctx)
                    .await
                    .unwrap_err();
                assert!(error
                    .to_string()
                    .contains("authoritative keys identify different entities"));
            }
        }
        #[tokio::test]
        async fn conflicting_component_preserves_unrelated_inputs_and_rejects_dependents() {
            let mut a = entity("A", 1);
            let mut b = entity("B", 1);
            let records = vec![stored(&a), stored(&b)];
            alias(&mut a, "X");
            alias(&mut b, "X");
            let healthy = entity("healthy", 1);
            let dependent = entity("dependent", 1);
            let mut bad = extraction(a);
            Arc::make_mut(&mut bad.entities_by_snapshot)[0]
                .1
                .push(dependent.clone());
            Arc::make_mut(&mut bad.version_exclusions).insert(dependent.uuid, vec![]);
            let mut second = dependent.clone();
            second.uuid = Uuid::new_v4();
            for reverse in [false, true] {
                let mut inputs = vec![
                    bad.clone(),
                    extraction(b.clone()),
                    extraction(second.clone()),
                    extraction(healthy.clone()),
                ];
                if reverse {
                    inputs.reverse();
                }
                let (ctx, graph) = context(records.clone());
                let outcomes = super::match_batch(inputs, &ctx).await.unwrap();
                assert_eq!(outcomes.iter().filter(|r| r.is_err()).count(), 3);
                let survivor = outcomes.into_iter().find_map(Result::ok).unwrap();
                assert!(survivor.matches.contains_key(&healthy.uuid));
                assert_eq!(graph.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
            }
        }
        #[tokio::test]
        async fn same_name_keyless_observations_never_match_or_query_authoritative_keys() {
            let mut a = entity("same", 1);
            let mut b = entity("same", 1);
            a.primary_key_properties.clear();
            b.primary_key_properties.clear();
            // Even accidentally equal opaque hashes are not authoritative identity.
            assert_eq!(a.identity_hash, b.identity_hash);
            let (ctx, graph) = context(vec![stored(&a)]);
            let output = match_batch(vec![extraction(a.clone()), extraction(b.clone())], &ctx)
                .await
                .unwrap();
            assert_eq!(graph.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
            assert_eq!(output[0].matches[&a.uuid].chain_id, a.chain_id);
            assert_eq!(output[1].matches[&b.uuid].chain_id, b.chain_id);
            assert!(output.iter().all(|item| item
                .matches
                .values()
                .all(|matched| matched.existing.is_none())));
        }
        #[tokio::test]
        async fn unknown_delete_cannot_join_otherwise_distinct_new_entities() {
            let a = entity("A", 1);
            let mut b = entity("B", 1);
            alias(&mut b, "X");
            let mut deletion = entity("A", 1);
            alias(&mut deletion, "X");
            let mut input = extraction(a.clone());
            input.source_deleted = Arc::new(vec![deletion.clone()]);
            let (ctx, _) = context(vec![]);
            let output = match_batch(vec![input, extraction(b.clone())], &ctx)
                .await
                .unwrap();
            assert_eq!(output[0].matches[&a.uuid].chain_id, a.chain_id);
            assert_eq!(output[1].matches[&b.uuid].chain_id, b.chain_id);
            assert!(!output[0].matches.contains_key(&deletion.uuid));
        }
        #[tokio::test]
        async fn new_component_uses_minimum_original_chain_in_either_order() {
            let mut a = entity("A", 1);
            let mut b = entity("B", 2);
            alias(&mut a, "X");
            alias(&mut b, "X");
            let expected = a.chain_id.min(b.chain_id);
            for incoming in [vec![a.clone(), b.clone()], vec![b.clone(), a.clone()]] {
                let (ctx, _) = context(vec![]);
                let output = match_batch(incoming.into_iter().map(extraction).collect(), &ctx)
                    .await
                    .unwrap();
                assert!(output.iter().all(|item| item
                    .matches
                    .values()
                    .all(|matched| matched.chain_id == expected)));
            }
        }
        #[tokio::test]
        async fn malformed_revision_responses_fail_before_identity_lookup() {
            let valid = IdentityRevision {
                scope: IdentityScope {
                    namespace: "ns".into(),
                    entity_type: "Type".into(),
                },
                revision: 7,
            };
            let mut wrong = valid.clone();
            wrong.scope.namespace = "other".into();
            let mut overflow = valid.clone();
            overflow.revision = u64::MAX;
            for response in [
                vec![],
                vec![valid.clone(), valid.clone()],
                vec![wrong],
                vec![overflow],
            ] {
                let (ctx, graph) = context(vec![]);
                *graph.revision_response.lock().unwrap() = Some(response);
                assert!(matches!(
                    match_batch(vec![extraction(entity("api", 1))], &ctx).await,
                    Err(StageError::StateValidation { .. })
                ));
                assert_eq!(graph.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
            }
            let (ctx, graph) = context(vec![]);
            let mut namespace = valid.clone();
            namespace.scope.entity_type = "*".into();
            *graph.revision_response.lock().unwrap() = Some(vec![namespace.clone(), valid.clone()]);
            let output = match_batch(vec![extraction(entity("api", 1))], &ctx)
                .await
                .unwrap();
            assert_eq!(
                output[0].identity_revisions.as_ref(),
                &vec![namespace, valid]
            );
        }
    }
}
