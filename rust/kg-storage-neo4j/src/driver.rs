//! Neo4j graph adapter with bounded calls, typed reads, and receipted atomic commits.
mod paging;

use std::future::Future;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use neo4rs::{BoltMap, BoltType, Graph, Query, Row, Txn};

use kg_core::errors::BackendError;
use kg_core::traits::graph_backend::{EntityEmbeddingHit, GraphBackend, GraphEmbedding};
use kg_core::traits::{
    CommittedBatch, EdgeLookup, EdgeRecord, EntityLookup, EntityVersionRecord, MutationBatch,
    RunHeader, RunRegistration,
};
use kg_storage_cypher as cypher;
use kg_storage_cypher::{PreparedQuery, PreparedWrite, StoredReceipt};

/// Test-only transport faults, each consumed once per armed count. They
/// simulate a lost acknowledgement after the server committed, a dropped
/// connection before the commit request is sent, and receipt checks that
/// see nothing or fail while a commit is still in flight. Production leaves
/// them unset.
#[derive(Debug, Default)]
pub struct FaultInjection {
    pub lose_commit_ack: AtomicU32,
    pub drop_before_commit: AtomicU32,
    /// Outcome verification reads that find no receipt although one exists.
    pub hold_receipt: AtomicU32,
    /// Outcome verification reads that fail with a connection error.
    pub fail_receipt: AtomicU32,
}

impl FaultInjection {
    fn take(counter: &AtomicU32) -> bool {
        counter
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
    }
}

/// Tunables for every call this backend makes against Neo4j.
#[derive(Debug, Clone)]
pub struct Neo4jOptions {
    /// Client deadline for a call or complete mutation staging block (default 30s).
    /// The server lifetime covers all three client phases: begin, staging, commit.
    pub timeout: Duration,
    /// Maximum number of attempts (initial call + retries) for transient
    /// failures (default 3). Clamped to at least 1. Never applied to
    /// authentication, validation, precondition, or reference failures.
    pub max_retries: u32,
    /// Base backoff between retries; doubles per attempt (default 100ms).
    pub base_backoff: Duration,
    /// Receipt checks after a commit request whose outcome the transport did
    /// not report (default 5, at least 1), spaced by the doubling backoff. A
    /// receipt found within them is the commit; none found is reported as an
    /// unknown outcome, never retried, because the commit may still land.
    pub commit_verification_attempts: u32,
    /// Injected transport faults for tests.
    pub faults: Option<Arc<FaultInjection>>,
    /// Vector scopes with at most this many comparable vectors are scored by an
    /// exact cosine scan instead of the vector index (default 512). On a
    /// 300-entity namespace the exact scan costs about 40 ms per query
    /// (measured 2026-09-27); lower this to make small scopes use the index.
    pub exact_vector_population: u64,
}

impl Default for Neo4jOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            max_retries: 3,
            base_backoff: Duration::from_millis(100),
            commit_verification_attempts: 5,
            faults: None,
            exact_vector_population: 512,
        }
    }
}

/// Neo4j GraphBackend implementation using neo4rs.
pub struct Neo4jGraphBackend {
    pub(crate) graph: Arc<Graph>,
    pub(crate) cancellation_graph: Arc<Graph>,
    pub(crate) cleanup: Arc<crate::cancellation::CleanupTracker>,
    pub(crate) options: Neo4jOptions,
    pub(crate) search_reads: std::sync::atomic::AtomicU64,
}

impl Neo4jGraphBackend {
    /// Roles granted to the connected Neo4j account; an empty result is not a role assertion.
    pub async fn current_user_roles(&self) -> Result<Vec<String>, BackendError> {
        let rows = self
            .execute_read(
                cypher::neo4j::admin::CURRENT_USER_ROLES,
                &serde_json::json!({}),
            )
            .await?;
        let roles = rows
            .first()
            .and_then(|row| row.get("roles"))
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| BackendError::Deserialization("Neo4j returned no user roles".into()))?;
        roles
            .iter()
            .map(|role| {
                role.as_str().map(str::to_owned).ok_or_else(|| {
                    BackendError::Deserialization("Neo4j returned an invalid user role".into())
                })
            })
            .collect()
    }

    /// Adapter-specific administration and fixture writes. No automatic retries:
    /// arbitrary Cypher is not necessarily idempotent. Application writes use mutations.
    /// Run entity-changing maintenance only while ingestion is stopped: raw Cypher
    /// bypasses identity revision accounting.
    #[tracing::instrument(name = "neo4j.write", skip_all)]
    pub async fn execute_write(
        &self,
        query: &str,
        params: &serde_json::Value,
    ) -> Result<(), BackendError> {
        let mut observation = crate::telemetry::Operation::new("write");
        let result = match tokio::time::timeout(
            self.options.timeout,
            self.graph.run_once(build_query(query, params)),
        )
        .await
        {
            Ok(result) => {
                result.map_err(|error| CallError::Driver(error).into_backend("Neo4j write failed"))
            }
            Err(_) => Err(BackendError::Timeout(
                self.options.timeout.as_millis() as u64
            )),
        };
        observation.finish(match &result {
            Ok(()) => "success",
            Err(error) => crate::telemetry::backend_error_kind(error),
        });
        result
    }

    /// Adapter-specific raw read for administration, fixtures, and search
    /// statements prepared by the shared Cypher crate. Ingestion uses the typed
    /// reads on [`GraphBackend`].
    /// Callers must supply read-only statements; raw writes bypass revision guards
    /// and are unsafe to retry here.
    pub async fn execute_read(
        &self,
        query: &str,
        params: &serde_json::Value,
    ) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, BackendError> {
        self.execute_retryable_statement("read", query, params)
            .await
    }

    // Private: only reads and explicitly idempotent derived-index maintenance
    // may use automatic retries. Entity/relationship writes use commit_batch.
    async fn execute_retryable_statement(
        &self,
        operation: &'static str,
        query: &str,
        params: &serde_json::Value,
    ) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, BackendError> {
        let q = build_query(query, params);
        let graph = Arc::clone(&self.graph);
        with_retry(operation, &self.options, || {
            let graph = Arc::clone(&graph);
            let q = q.clone();
            async move {
                let mut stream = graph.execute_once(q).await.map_err(CallError::Driver)?;
                let mut rows = Vec::new();
                // Explicit match: a mid-stream error is an Err, never a
                // silently truncated Ok.
                loop {
                    match stream.next().await {
                        Ok(Some(row)) => rows.push(row_to_json(&row)?),
                        Ok(None) => break,
                        Err(e) => return Err(CallError::Driver(e)),
                    }
                }
                Ok(rows)
            }
        })
        .await
        .map_err(|e| e.into_backend("Neo4j read failed"))
    }

    pub async fn new(uri: &str, username: &str, password: &str) -> Result<Self, BackendError> {
        Self::with_options(uri, username, password, Neo4jOptions::default()).await
    }

    /// Connect with explicit timeout/retry options.
    pub async fn with_options(
        uri: &str,
        username: &str,
        password: &str,
        options: Neo4jOptions,
    ) -> Result<Self, BackendError> {
        crate::settings::validate_uri(uri)?;
        let graph =
            tokio::time::timeout(options.timeout, Graph::new(uri.trim(), username, password))
                .await
                .map_err(|_| BackendError::Timeout(options.timeout.as_millis() as u64))?
                .map_err(|e| BackendError::Connection(format!("Neo4j connection failed: {e}")))?;
        let control_config = neo4rs::ConfigBuilder::new()
            .uri(uri.trim())
            .user(username)
            .password(password)
            .max_connections(2)
            .build()
            .map_err(|e| BackendError::Connection(format!("Neo4j cancellation settings: {e}")))?;
        let cancellation_graph =
            tokio::time::timeout(options.timeout, Graph::connect(control_config))
                .await
                .map_err(|_| BackendError::Timeout(options.timeout.as_millis() as u64))?
                .map_err(|e| {
                    BackendError::Connection(format!("Neo4j cancellation connection: {e}"))
                })?;
        Ok(Self::from_graph_with_options(
            Arc::new(graph),
            Arc::new(cancellation_graph),
            options,
        ))
    }

    pub fn from_graph(graph: Arc<Graph>, cancellation_graph: Arc<Graph>) -> Self {
        Self::from_graph_with_options(graph, cancellation_graph, Neo4jOptions::default())
    }

    pub fn from_graph_with_options(
        graph: Arc<Graph>,
        cancellation_graph: Arc<Graph>,
        options: Neo4jOptions,
    ) -> Self {
        Self {
            graph,
            cancellation_graph,
            cleanup: Arc::new(crate::cancellation::CleanupTracker::default()),
            options,
            search_reads: std::sync::atomic::AtomicU64::new(0),
        }
    }

    async fn run_ddl(&self, name: &str, ddl: &str) -> Result<(), BackendError> {
        let graph = Arc::clone(&self.graph);
        // Telemetry labels DDL as "schema" (a Maintenance operation), not the
        // specific index name, which would fall back to an unrelated bucket and
        // export as GraphWrite. The index name stays on the tracing context.
        tracing::debug!(index = name, "installing schema element");
        match with_retry("schema", &self.options, || {
            let graph = Arc::clone(&graph);
            let q = Query::new(ddl.to_string());
            async move { graph.run_once(q).await.map_err(CallError::Driver) }
        })
        .await
        {
            Ok(()) => Ok(()),
            // Two instances starting together can both pass IF NOT EXISTS; the
            // schema rule then exists as requested, which is the goal.
            Err(CallError::Driver(neo4rs::Error::Neo4j(error)))
                if error.code() == "Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists" =>
            {
                Ok(())
            }
            Err(e) => Err(e.into_backend(&format!("schema statement `{name}` failed"))),
        }
    }

    /// Install every constraint and index ingestion and search require, wait
    /// once until they are online, and verify the search index definitions.
    /// Call at startup before accepting graph reads or writes. An existing
    /// index with an incompatible definition fails here with remediation
    /// instructions; nothing is dropped automatically.
    pub async fn ensure_indexes(&self) -> Result<(), BackendError> {
        let rows = self
            .execute_read(cypher::neo4j::admin::SERVER_VERSION, &serde_json::json!({}))
            .await?;
        cypher::neo4j::schema::validate_server_version(&rows)?;
        if !self
            .execute_read(
                cypher::neo4j::schema::LEGACY_EVIDENCE,
                &serde_json::json!({}),
            )
            .await?
            .is_empty()
        {
            return Err(BackendError::Unavailable(
                "legacy OBSERVED_IN evidence exists; re-ingest into a fresh development database using MENTIONS before accepting traffic".into(),
            ));
        }
        if !self
            .execute_read(
                cypher::neo4j::schema::LEGACY_REFERENCE_OWNERS,
                &serde_json::json!({}),
            )
            .await?
            .is_empty()
        {
            return Err(BackendError::Unavailable(
                "reference relationships lack owner or dependency evidence; re-ingest into a fresh development database before accepting traffic".into(),
            ));
        }
        for (name, ddl) in cypher::neo4j::schema::INDEXES {
            self.run_ddl(name, ddl).await?;
        }
        let (name, ddl) = kg_storage_cypher::COLLECTION_SCAN_CONSTRAINT;
        self.run_ddl(name, ddl).await?;
        self.execute_read(cypher::neo4j::admin::AWAIT_INDEXES, &serde_json::json!({}))
            .await?;
        let constraints = self
            .execute_read(cypher::neo4j::admin::CONSTRAINTS, &serde_json::json!({}))
            .await?;
        cypher::neo4j::schema::validate_constraints(&constraints)?;
        if !self
            .execute_read(cypher::IDENTITY_INDEX_READY, &serde_json::json!({}))
            .await?
            .is_empty()
        {
            return Err(BackendError::Unavailable("entity identities are not indexed; reimport this pre-index database through the mutation API before accepting traffic".into()));
        }
        if !self
            .execute_read(cypher::reference_dependency::READY, &serde_json::json!({}))
            .await?
            .is_empty()
        {
            return Err(BackendError::Unavailable("reference dependencies are not indexed; call backfill_reference_dependencies for each affected organization until it returns zero, then rerun ensure_indexes".into()));
        }
        if !self
            .execute_read(cypher::IDENTITY_VALUES_INDEX_READY, &serde_json::json!({}))
            .await?
            .is_empty()
        {
            return Err(BackendError::Unavailable("reference key components are not indexed; run the organization-scoped backfill_key_values maintenance until complete, then rerun ensure_indexes".into()));
        }
        let ranges = self
            .execute_read(cypher::neo4j::admin::RANGE_INDEXES, &serde_json::json!({}))
            .await?;
        cypher::neo4j::schema::validate_range_indexes(&ranges)?;
        self.validate_search_indexes().await
    }

    /// Backfill one bounded page of legacy confirmed-reference dependencies.
    /// Call after ensure_indexes has installed the schema (even if its readiness
    /// check failed). Repeat until zero rows; markers make restart idempotent.
    pub async fn backfill_reference_dependencies(
        &self,
        org: &str,
        limit: usize,
    ) -> Result<usize, BackendError> {
        let query = cypher::reference_dependency::backfill(org, limit)?;
        Ok(self
            .execute_retryable_statement("reference_backfill", &query.statement, &query.parameters)
            .await?
            .len())
    }

    async fn validate_search_indexes(&self) -> Result<(), BackendError> {
        let rows = self
            .execute_read(
                cypher::neo4j::admin::FULLTEXT_INDEXES,
                &serde_json::json!({}),
            )
            .await?;
        cypher::neo4j::schema::validate_search_indexes(&rows)?;
        let rows = self
            .execute_read(cypher::neo4j::admin::VECTOR_INDEXES, &serde_json::json!({}))
            .await?;
        cypher::neo4j::schema::validate_vector_indexes(&rows)
    }

    /// Deliberately drop and recreate the search indexes, then wait for them to
    /// come online. Run this during a maintenance window after an index
    /// definition changes; searches fail until population completes.
    pub async fn rebuild_search_indexes(&self) -> Result<(), BackendError> {
        for name in cypher::neo4j::schema::SEARCH_INDEXES {
            self.run_ddl(name, &cypher::neo4j::admin::drop_search_index(name)?)
                .await?;
        }
        self.ensure_indexes().await
    }

    async fn read_prepared(
        &self,
        query: &PreparedQuery,
    ) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, BackendError> {
        self.execute_read(&query.statement, &query.parameters).await
    }

    /// Read the receipt of one batch outside any transaction.
    async fn verify_receipt(
        &self,
        batch_id: uuid::Uuid,
    ) -> Result<Option<StoredReceipt>, BackendError> {
        let rows = self.read_prepared(&cypher::read_receipt(batch_id)).await?;
        unique_receipt(&rows)
    }

    async fn discover_community_scopes(
        txn: &mut Txn,
        org: &str,
        targets: &cypher::community_revision::Targets,
    ) -> Result<std::collections::BTreeSet<String>, CallError> {
        let mut scopes = targets.namespaces.clone();
        if !targets.has_source() {
            return Ok(scopes);
        }
        for row in run_read(txn, &cypher::community_revision::discover(org, targets)).await? {
            let namespace = row
                .get("namespace")
                .and_then(serde_json::Value::as_str)
                .filter(|v| !v.trim().is_empty())
                .ok_or_else(|| {
                    CallError::Backend(BackendError::Deserialization(
                        "invalid Community source namespace".into(),
                    ))
                })?;
            scopes.insert(namespace.to_owned());
        }
        Ok(scopes)
    }
    async fn lock_community_scopes(
        txn: &mut Txn,
        org: &str,
        targets: &cypher::community_revision::Targets,
        stripe: usize,
    ) -> Result<std::collections::BTreeSet<String>, CallError> {
        let mut scopes = Self::discover_community_scopes(txn, org, targets).await?;
        scopes.extend(targets.publications.iter().cloned());
        let mut locks = Vec::new();
        for namespace in &scopes {
            let stripes = if targets.publications.contains(namespace) {
                (0..kg_core::community::REVISION_STRIPES).collect::<Vec<_>>()
            } else {
                vec![stripe]
            };
            locks.extend(
                stripes
                    .into_iter()
                    .map(|selected| (namespace.clone(), selected)),
            );
        }
        if !locks.is_empty() {
            run_write(txn, &cypher::community_revision::locks(org, &locks)).await?;
        }
        Ok(scopes)
    }
    async fn check_community_scopes(
        txn: &mut Txn,
        org: &str,
        targets: &cypher::community_revision::Targets,
        locked: &std::collections::BTreeSet<String>,
        stripe: Option<usize>,
    ) -> Result<(), CallError> {
        let actual = Self::discover_community_scopes(txn, org, targets).await?;
        if !actual.is_subset(locked) {
            return Err(CallError::Backend(BackendError::Conflict(
                "Community source namespace changed".into(),
            )));
        }
        if let Some(stripe) = stripe {
            for namespace in actual {
                run_write(
                    txn,
                    &cypher::community_revision::advance(org, &namespace, stripe),
                )
                .await?;
            }
        }
        Ok(())
    }
    async fn discover_identity_scopes(
        txn: &mut Txn,
        org: &str,
        targets: &cypher::identity_revision::MutationScopes,
    ) -> Result<std::collections::BTreeSet<kg_core::traits::IdentityScope>, CallError> {
        let mut scopes = targets.declared.clone();
        if !targets.uuids.is_empty() || !targets.chains.is_empty() {
            let rows = run_read(
                txn,
                &cypher::identity_revision::existing_scopes(org, &targets.uuids, &targets.chains),
            )
            .await?;
            for row in rows {
                let scope =
                    cypher::identity_revision::decode_scope(&row).map_err(CallError::Backend)?;
                cypher::identity_revision::add_revision_scopes(&mut scopes, scope);
            }
        }
        Ok(scopes)
    }

    async fn lock_identity_scopes(
        txn: &mut Txn,
        org: &str,
        targets: &cypher::identity_revision::MutationScopes,
        expected: impl Iterator<Item = kg_core::traits::IdentityScope>,
    ) -> Result<std::collections::BTreeSet<kg_core::traits::IdentityScope>, CallError> {
        let mut scopes = Self::discover_identity_scopes(txn, org, targets).await?;
        scopes.extend(expected);
        for scope in &scopes {
            run_write(
                txn,
                &cypher::identity_revision::lock(org, scope).map_err(CallError::Backend)?,
            )
            .await?;
        }
        Ok(scopes)
    }

    async fn advance_identity_scopes(
        txn: &mut Txn,
        org: &str,
        targets: &cypher::identity_revision::MutationScopes,
        locked: &std::collections::BTreeSet<kg_core::traits::IdentityScope>,
    ) -> Result<(), CallError> {
        let written = Self::discover_identity_scopes(txn, org, targets).await?;
        // A previously absent target can appear concurrently. Never publish a
        // mutation in a scope whose revision this transaction did not lock.
        if !written.is_subset(locked) {
            return Err(CallError::Backend(BackendError::Conflict(
                "mutation targets changed identity scope".into(),
            )));
        }
        for scope in written {
            run_write(
                txn,
                &cypher::identity_revision::advance(org, &scope).map_err(CallError::Backend)?,
            )
            .await?;
        }
        Ok(())
    }

    /// One transaction attempt. `Retry` errors may be attempted again; `Final`
    /// errors are returned to the caller as they are.
    async fn commit_attempt(
        &self,
        batch: &MutationBatch,
        checks: &[PreparedWrite],
        writes: &[PreparedWrite],
        page: Option<&paging::PageCommit<'_>>,
    ) -> Result<CommittedBatch, AttemptError> {
        let targets = cypher::identity_revision::mutation_scopes(&batch.org_id, &batch.mutations)
            .map_err(AttemptError::Final)?;
        let batch_id = batch.batch_id();
        let timeout = self.options.timeout;
        let timeout_error = || BackendError::Timeout(timeout.as_millis() as u64);
        let mut txn = match tokio::time::timeout(
            timeout,
            self.graph
                .start_txn_with_timeout(self.options.server_transaction_timeout()),
        )
        .await
        {
            Ok(Ok(txn)) => txn,
            Ok(Err(e)) => return Err(classify_driver(e, "Neo4j transaction start failed")),
            Err(_) => return Err(AttemptError::Retry(timeout_error())),
        };

        let committed_at = Utc::now();
        let receipt = match page.map_or_else(
            || cypher::create_receipt(batch, committed_at),
            |page| {
                Ok(cypher::commit_pages::receipt(
                    batch,
                    page.ordinal,
                    committed_at,
                ))
            },
        ) {
            Ok(receipt) => receipt,
            Err(e) => return Err(AttemptError::Final(e)),
        };
        let staged = tokio::time::timeout(timeout, async {
            run_write(&mut txn, &checks[0])
                .await
                .map_err(StageError::RunHeader)?;
            let receipt_query = page.map_or_else(|| cypher::read_receipt(batch_id), |page| cypher::commit_pages::read_receipt(batch,page.ordinal));
            let rows = run_read(&mut txn, &receipt_query)
                .await
                .map_err(StageError::Transport)?;
            if let Some(stored) = unique_receipt(&rows).map_err(StageError::Decode)? {
                return Ok(Staged::Replayed(stored));
            }
            run_write(&mut txn, &cypher::commit_pages::lock_revision(&batch.org_id,batch_id,page.is_some()))
                .await.map_err(StageError::Transport)?;
            if let Some(page)=page {
                if let Err(error) = run_write(&mut txn,&cypher::commit_pages::guard(batch,page.ordinal)).await {
                    match error {
                        CallError::Reference => {
                            // A concurrent resume of the same run may have committed
                            // this exact page between our staged read and the guard.
                            // Re-read the page receipt in this transaction: if it is
                            // now present the page is done and we replay it, instead
                            // of reporting a false graph-changed conflict. If it is
                            // still absent the fence caught a genuine intervening
                            // graph write and the original error stands.
                            let rows = run_read(&mut txn, &cypher::commit_pages::read_receipt(batch, page.ordinal))
                                .await
                                .map_err(StageError::Transport)?;
                            if let Some(stored) = unique_receipt(&rows).map_err(StageError::Decode)? {
                                return Ok(Staged::Replayed(stored));
                            }
                            return Err(StageError::Transport(CallError::Backend(BackendError::Query("paged commit graph changed; completed pages are retained, parent is incomplete; a new run must replan remaining observations".into()))));
                        }
                        other => return Err(StageError::Transport(other)),
                    }
                }
            }
            let community_targets =
                cypher::community_revision::Targets::from_mutations(&batch.mutations);
            let community_stripe =
                cypher::community_revision::stripe(format!("{:?}", batch.batch).as_bytes());
            let community_locked = Self::lock_community_scopes(
                &mut txn,
                &batch.org_id,
                &community_targets,
                community_stripe,
            )
            .await
            .map_err(StageError::Transport)?;
            let expected = batch.preconditions.iter().filter_map(|check| match check {
                kg_core::traits::Precondition::IdentityRevisionIs(expected) => {
                    Some(expected.scope.clone())
                }
                _ => None,
            });
            let locked = Self::lock_identity_scopes(&mut txn, &batch.org_id, &targets, expected)
                .await
                .map_err(StageError::Transport)?;
            Self::check_community_scopes(
                &mut txn,
                &batch.org_id,
                &community_targets,
                &community_locked,
                None,
            )
            .await
            .map_err(StageError::Transport)?;
            if community_targets.has_source() {
                run_write(
                    &mut txn,
                    &cypher::community_revision::dirty(&batch.org_id, &community_targets),
                )
                .await
                .map_err(StageError::Mutation)?;
            }
            if let Some(invalidation) =
                cypher::entity_summary::invalidate(&batch.org_id, &batch.mutations)
            {
                run_write(&mut txn, &invalidation)
                    .await
                    .map_err(StageError::Mutation)?;
            }
            for (index, check) in checks[1..].iter().enumerate() {
                run_write(&mut txn, check)
                    .await
                    .map_err(|e| StageError::Precondition(index, e))?;
            }
            for write in writes {
                run_write(&mut txn, write)
                    .await
                    .map_err(StageError::Mutation)?;
            }
            Self::advance_identity_scopes(&mut txn, &batch.org_id, &targets, &locked)
                .await
                .map_err(StageError::Transport)?;
            Self::check_community_scopes(
                &mut txn,
                &batch.org_id,
                &community_targets,
                &community_locked,
                Some(community_stripe),
            )
            .await
            .map_err(StageError::Transport)?;
            run_write(&mut txn, &receipt)
                .await
                .map_err(StageError::Receipt)?;
            if let Some(page)=page.filter(|page|page.ordinal+1==page.total) {
                let receipt=cypher::create_receipt(page.parent,committed_at).map_err(StageError::Decode)?;
                run_write(&mut txn,&receipt).await.map_err(StageError::Receipt)?;
                run_write(&mut txn,&cypher::commit_pages::release_parts(page.parent)).await.map_err(StageError::Receipt)?;
            }
            Ok::<Staged, StageError>(Staged::Ready)
        })
        .await;

        match staged {
            Ok(Ok(Staged::Replayed(stored))) => {
                self.rollback(txn).await;
                return replayed(batch, stored).map_err(AttemptError::Final);
            }
            Ok(Ok(Staged::Ready)) => {}
            Ok(Err(stage)) => {
                self.rollback(txn).await;
                return self.classify_stage(batch, stage).await;
            }
            Err(_) => {
                self.rollback(txn).await;
                return Err(AttemptError::Retry(timeout_error()));
            }
        }

        if self
            .options
            .faults
            .as_ref()
            .is_some_and(|f| FaultInjection::take(&f.drop_before_commit))
        {
            self.rollback(txn).await;
            return Err(AttemptError::Retry(BackendError::Connection(
                "injected connection loss before commit".into(),
            )));
        }

        let lost = match tokio::time::timeout(timeout, txn.commit()).await {
            Ok(Ok(())) => {
                if self
                    .options
                    .faults
                    .as_ref()
                    .is_some_and(|f| FaultInjection::take(&f.lose_commit_ack))
                {
                    "injected acknowledgement loss after commit".to_string()
                } else {
                    return Ok(CommittedBatch {
                        batch_id,
                        run_id: batch.batch.run_id,
                        kind: batch.batch.kind,
                        index: batch.batch.index,
                        committed_at,
                        result: batch.result.clone(),
                        replayed: false,
                    });
                }
            }
            Ok(Err(e)) if matches!(e, neo4rs::Error::Neo4j(_)) => {
                // Rejected at commit, typically a constraint violation from a
                // concurrent write. The receipt decides whether it was ours.
                return match self.verify_receipt(batch_id).await {
                    Ok(Some(stored)) => replayed(batch, stored).map_err(AttemptError::Final),
                    Ok(None) => Err(classify_driver(e, "Neo4j commit rejected")),
                    Err(read) => Err(AttemptError::Final(BackendError::UnknownCommit(format!(
                        "commit rejected ({e}) and receipt read failed: {read}"
                    )))),
                };
            }
            Ok(Err(e)) => format!("commit acknowledgement could not be decoded: {e}"),
            Err(_) => "commit timed out".to_string(),
        };

        if page.is_some() {
            return Err(AttemptError::Final(BackendError::UnknownCommit(format!(
                "paged commit acknowledgment uncertain ({lost}); resume the same run to read its durable page receipt"))));
        }
        self.verify_commit_outcome(batch, batch_id, &lost).await
    }

    /// The commit request left this process, so the batch may be committed
    /// whatever the transport reported. Only a receipt proves it, and an
    /// absent receipt may still be in flight: after a bounded number of
    /// checks the outcome is reported unknown, never retried.
    async fn verify_commit_outcome(
        &self,
        batch: &MutationBatch,
        batch_id: uuid::Uuid,
        lost: &str,
    ) -> Result<CommittedBatch, AttemptError> {
        let attempts = self.options.commit_verification_attempts.max(1);
        let mut read_error = None;
        for check in 0..attempts {
            if check > 0 {
                tokio::time::sleep(retry_delay(self.options.base_backoff, check - 1)).await;
            }
            tracing::warn!(batch = %batch_id, check = check + 1, "verifying commit outcome through the receipt");
            let faults = self.options.faults.as_ref();
            let read = if faults.is_some_and(|f| FaultInjection::take(&f.hold_receipt)) {
                Ok(None)
            } else if faults.is_some_and(|f| FaultInjection::take(&f.fail_receipt)) {
                Err(BackendError::Connection(
                    "injected receipt read failure".into(),
                ))
            } else {
                self.verify_receipt(batch_id).await
            };
            match read {
                Ok(Some(stored)) => {
                    tracing::info!(batch = %batch_id, "commit recovered from receipt");
                    return replayed(batch, stored)
                        .map(|mut committed| {
                            committed.replayed = false;
                            committed
                        })
                        .map_err(AttemptError::Final);
                }
                Ok(None) => read_error = None,
                Err(error) => {
                    tracing::warn!(batch = %batch_id, error_kind = crate::telemetry::backend_error_kind(&error), "receipt check failed");
                    read_error = Some(error);
                }
            }
        }
        Err(AttemptError::Final(BackendError::UnknownCommit(
            match read_error {
                Some(error) => {
                    format!("{lost}; the receipt check failed {attempts} time(s), last: {error}")
                }
                None => format!(
                    "{lost}; no receipt after {attempts} check(s), the commit may still be in flight"
                ),
            },
        )))
    }

    async fn classify_stage(
        &self,
        batch: &MutationBatch,
        stage: StageError,
    ) -> Result<CommittedBatch, AttemptError> {
        use AttemptError::*;
        let (phase, precondition_index, error_kind) = stage.diagnostic();
        tracing::warn!(
            phase,
            precondition_index,
            error_kind,
            "graph transaction staging rejected"
        );
        Err(match stage {
            StageError::RunHeader(CallError::Reference) => Final(BackendError::Conflict(
                "run is not registered with this organization and fingerprint".into(),
            )),
            StageError::Precondition(index, CallError::Reference) => {
                // A concurrent identical batch may have changed the precondition
                // after our first receipt read. Its receipt takes precedence.
                match self.verify_receipt(batch.batch_id()).await {
                    Ok(Some(stored)) => return replayed(batch, stored).map_err(Final),
                    Ok(None)
                        if matches!(
                            batch.preconditions.get(index),
                            Some(kg_core::traits::Precondition::IdentityRevisionIs(_))
                        ) =>
                    {
                        Final(BackendError::IdentityRevisionChanged)
                    }
                    Ok(None) => {
                        let message = self.describe_rejection(batch, index).await;
                        if matches!(
                            batch.preconditions.get(index),
                            Some(kg_core::traits::Precondition::OwnsCollection { .. })
                        ) {
                            // Collection ownership only advances. Replanning identity
                            // or graph evidence cannot recover this generation/run.
                            Final(BackendError::CollectionOwnershipConflict(message))
                        } else {
                            Final(BackendError::Conflict(message))
                        }
                    }
                    Err(error) => Final(BackendError::UnknownCommit(format!(
                        "precondition rejected and concurrent receipt check failed: {error}"
                    ))),
                }
            }
            StageError::Mutation(CallError::Reference) => {
                Final(CallError::Reference.into_backend("Neo4j mutation failed"))
            }
            StageError::Receipt(error) => match self.verify_receipt(batch.batch_id()).await {
                // Committed concurrently by another attempt: its receipt is this batch's result.
                Ok(Some(stored)) => return replayed(batch, stored).map_err(Final),
                Ok(None) => classify_call(error, "Neo4j receipt write failed"),
                Err(read) => Final(BackendError::UnknownCommit(format!(
                    "receipt write failed and receipt read failed: {read}"
                ))),
            },
            StageError::Decode(error) => Final(error),
            StageError::RunHeader(error)
            | StageError::Precondition(_, error)
            | StageError::Mutation(error)
            | StageError::Transport(error) => classify_call(error, "Neo4j transaction failed"),
        })
    }

    /// Why the `index`-th precondition of `batch` failed, named so callers
    /// can tell a stale plan from a lost race. A collection claim
    /// names the generation and run that hold the scan; the read happens after
    /// the rollback, so it describes the state that rejected the commit unless
    /// another writer moved it since.
    async fn describe_rejection(&self, batch: &MutationBatch, index: usize) -> String {
        use kg_core::traits::Precondition;
        match batch.preconditions.get(index) {
            Some(Precondition::RuleRevisionIs { .. }) => {
                "learned-rule lifecycle revision changed before maintenance commit".into()
            }
            Some(Precondition::IncidentHistoryIs { .. }) => {
                "incident history changed before summary publication".into()
            }
            Some(Precondition::IdentityRevisionIs(_)) => {
                "identity evidence changed before commit".into()
            }
            Some(Precondition::OwnsCollection {
                collection,
                generation,
                ..
            }) => {
                let owner = self
                    .read_prepared(&cypher::collection_owner(
                        &batch.org_id,
                        &collection.scope_id(&batch.org_id),
                    ))
                    .await
                    .ok()
                    .and_then(|rows| rows.into_iter().next());
                match owner {
                    Some(row) => format!(
                        "collection {collection} is owned by generation {} of run {}",
                        row.get("generation")
                            .and_then(serde_json::Value::as_u64)
                            .unwrap_or(0),
                        row.get("run_id")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("?"),
                    ),
                    None => format!(
                        "collection {collection} could not be claimed at generation {generation}"
                    ),
                }
            }
            Some(Precondition::LatestVersionIs {
                chain_id,
                uuid,
                version,
            }) => format!(
                "expected latest version {uuid} (v{version}) of chain {chain_id} is not current"
            ),
            Some(Precondition::NoLiveVersionFor { hashes }) => format!(
                "a live version already answers to one of {} identity hashes",
                hashes.len()
            ),
            Some(Precondition::LatestDeletedVersionIs {
                chain_id,
                uuid,
                restored_at,
            }) => format!(
                "tombstone {uuid} of chain {chain_id} is no longer its newest version, or was deleted at or after {restored_at}"
            ),
            Some(Precondition::NotObservedAfter { uuid, observed_at }) => {
                format!("version {uuid} was observed after {observed_at}")
            }
            Some(
                Precondition::RelationshipTimelineIs { .. }
                | Precondition::RelationTimelineIs { .. }
                | Precondition::ReferenceOwnerTimelineIs { .. }
                | Precondition::IncidentTimelineIs { .. },
            ) => "relationship timeline changed since planning".into(),
            Some(Precondition::EdgeStartsNoLaterThan { uuid, .. }) => {
                format!("relationship {uuid} starts after the requested effective end")
            }
            Some(Precondition::EdgeHeadIs { chain_id, .. }) => {
                format!("relationship head of chain {chain_id} changed since planning")
            }
            Some(Precondition::EdgeIsLatest { uuid, version }) => {
                format!("relationship {uuid} (v{version}) is not current")
            }
            Some(Precondition::EdgeNotObservedAfter { uuid, observed_at }) => {
                format!("relationship {uuid} was observed after {observed_at}")
            }
            Some(Precondition::EdgeObservedBefore { uuid, observed_at }) => {
                format!("relationship {uuid} was observed at or after {observed_at}")
            }
            Some(Precondition::LiveEdgesForPairAre {
                source_chain_id,
                target_chain_id,
                ..
            }) => format!(
                "live relationship set from chain {source_chain_id} to chain {target_chain_id} changed since planning"
            ),
            Some(Precondition::CollectionMembershipsAre { uuid, .. }) => {
                format!("collection memberships of version {uuid} changed since planning")
            }
            Some(Precondition::SoleCollectionOwnerIs { uuid, collection }) => {
                format!("version {uuid} is no longer owned solely by collection {collection}")
            }
            Some(Precondition::LiveIncidentEdgesAre { chain_id, .. }) => {
                format!("the live relationships touching chain {chain_id} changed since planning")
            }
            Some(Precondition::LiveEdgesForRelationAre {
                source_chain_id,
                name,
                ..
            }) => format!(
                "the live {name} relationships of chain {source_chain_id} changed since planning"
            ),
            Some(Precondition::LiveEdgesForReferenceOwnerAre { owner, .. }) => format!(
                "the live relationships for reference slot {} of chain {} changed since planning",
                owner.slot, owner.chain_id
            ),
            None => "a transactional precondition failed; the graph changed since planning".into(),
        }
    }

    async fn rollback(&self, txn: Txn) {
        match tokio::time::timeout(self.options.timeout, txn.rollback()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(
                error_kind = crate::telemetry::driver_error_kind(&error),
                "graph transaction rollback failed"
            ),
            Err(_) => tracing::warn!("graph transaction rollback timed out"),
        }
    }
}

enum Staged {
    Replayed(StoredReceipt),
    Ready,
}

enum StageError {
    RunHeader(CallError),
    Transport(CallError),
    Decode(BackendError),
    Precondition(usize, CallError),
    Mutation(CallError),
    Receipt(CallError),
}

impl StageError {
    // Only bounded labels and a position: errors and graph evidence may contain private data.
    fn diagnostic(&self) -> (&'static str, Option<usize>, &'static str) {
        let (phase, index, error) = match self {
            Self::RunHeader(error) => ("run_header", None, error),
            Self::Transport(error) => ("transport", None, error),
            Self::Precondition(index, error) => ("precondition", Some(*index), error),
            Self::Mutation(error) => ("mutation", None, error),
            Self::Receipt(error) => ("receipt", None, error),
            Self::Decode(error) => {
                return ("decode", None, crate::telemetry::backend_error_kind(error))
            }
        };
        let kind = match error {
            CallError::IdentityConflict => "identity_conflict",
            CallError::MetadataConflict => "metadata_conflict",
            CallError::EmbeddingConflict => "embedding_conflict",
            CallError::SummaryConflict => "summary_conflict",
            CallError::SagaConflict => "saga_conflict",
            _ => error.telemetry_kind(),
        };
        (phase, index, kind)
    }
}

enum AttemptError {
    Retry(BackendError),
    Final(BackendError),
}

fn classify_driver(e: neo4rs::Error, what: &str) -> AttemptError {
    if is_transient(&e) {
        AttemptError::Retry(CallError::Driver(e).into_backend(what))
    } else {
        AttemptError::Final(CallError::Driver(e).into_backend(what))
    }
}

/// A stored receipt for this batch counts only when it belongs to the same
/// organization and request fingerprint.
fn replayed(batch: &MutationBatch, stored: StoredReceipt) -> Result<CommittedBatch, BackendError> {
    if stored.org_id != batch.org_id || stored.fingerprint != batch.fingerprint.0 {
        return Err(BackendError::Conflict(
            "batch identity was already committed for a different request".into(),
        ));
    }
    Ok(stored.batch)
}

async fn run_write(txn: &mut Txn, write: &PreparedWrite) -> Result<(), CallError> {
    let mut stream = txn
        .execute(build_query(&write.statement, &write.parameters))
        .await
        .map_err(CallError::Driver)?;
    let mut matched = 0;
    while let Some(row) = stream.next(&mut *txn).await.map_err(CallError::Driver)? {
        if row.get::<bool>("community_conflict").unwrap_or(false) {
            return Err(CallError::Backend(BackendError::Conflict(
                "Community projection or publication changed".into(),
            )));
        }
        if row.get::<bool>("saga_conflict").unwrap_or(false) {
            return Err(CallError::SagaConflict);
        }
        if row.get::<bool>("summary_conflict").unwrap_or(false) {
            return Err(CallError::SummaryConflict);
        }
        if row.get::<bool>("embedding_conflict").unwrap_or(false) {
            return Err(CallError::EmbeddingConflict);
        }
        if row.get::<bool>("metadata_conflict").unwrap_or(false) {
            return Err(CallError::MetadataConflict);
        }
        if row.get::<bool>("identity_conflict").unwrap_or(false) {
            return Err(CallError::IdentityConflict);
        }
        if !row.get::<bool>("ok").unwrap_or(false) {
            return Err(CallError::Reference);
        }
        matched += 1;
    }
    if matched != write.expected_rows {
        return Err(CallError::Reference);
    }
    Ok(())
}

async fn run_read(
    txn: &mut Txn,
    query: &PreparedQuery,
) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, CallError> {
    let mut stream = txn
        .execute(build_query(&query.statement, &query.parameters))
        .await
        .map_err(CallError::Driver)?;
    let mut rows = Vec::new();
    while let Some(row) = stream.next(&mut *txn).await.map_err(CallError::Driver)? {
        rows.push(row_to_json(&row)?);
    }
    Ok(rows)
}

#[async_trait]
impl GraphBackend for Neo4jGraphBackend {
    fn supports_paged_commits(&self) -> bool {
        true
    }
    async fn resume_commit(
        &self,
        org: &str,
        batch: kg_core::traits::BatchIdentity,
        fingerprint: &kg_core::traits::RequestFingerprint,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<Option<CommittedBatch>, BackendError> {
        self.resume_pages(org, batch, fingerprint, cancel).await
    }

    async fn learned_rule(
        &self,
        org: &str,
        id: uuid::Uuid,
    ) -> Result<Option<kg_core::traits::LearnedRule>, BackendError> {
        kg_core::traits::RuleStore::get(self, org, id).await
    }

    async fn active_learned_rules(
        &self,
        org: &str,
    ) -> Result<Vec<kg_core::traits::LearnedRule>, BackendError> {
        use kg_storage_cypher::rule_store::MAX_ACTIVE_RULES;
        let query = cypher::rule_store::list_all_active(org)?;
        let rows = self
            .execute_search_read(&query.statement, &query.parameters)
            .await?;
        if rows.len() > MAX_ACTIVE_RULES {
            return Err(BackendError::Query(
                "active learned-rule set exceeds the admission limit".into(),
            ));
        }
        rows.iter().map(cypher::rule_store::decode).collect()
    }

    async fn identity_candidates(
        &self,
        org: &str,
        request: &kg_core::traits::IdentityCandidateRequest,
    ) -> Result<kg_core::traits::IdentityCandidatePage, BackendError> {
        let query = cypher::identity_candidates::candidates(org, request)?;
        let rows = self
            .execute_search_read(&query.statement, &query.parameters)
            .await?;
        cypher::identity_candidates::decode(org, request, rows)
    }

    async fn reference_decisions_by_reuse_key(
        &self,
        org: &str,
        keys: &[String],
    ) -> Result<Vec<kg_core::runtime::reference_resolution::PersistedDecision>, BackendError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let query = cypher::reference_decision::by_reuse_key(org, keys)?;
        decode_persisted_decisions(org, &self.read_prepared(&query).await?)
    }

    async fn reference_decision_labels(
        &self,
        org: &str,
        producer_source: &str,
        limit: usize,
    ) -> Result<Vec<kg_core::runtime::reference_resolution::PersistedDecision>, BackendError> {
        let query = cypher::reference_decision::labels(org, producer_source, limit)?;
        decode_persisted_decisions(org, &self.read_prepared(&query).await?)
    }

    async fn unresolved_references(
        &self,
        org: &str,
        request: &kg_core::traits::UnresolvedReferenceQuery,
    ) -> Result<kg_core::traits::UnresolvedReferencePage, BackendError> {
        let query = cypher::unresolved_reference::read(org, request)?;
        let mut records = self
            .read_prepared(&query)
            .await?
            .iter()
            .map(cypher::unresolved_reference::decode)
            .collect::<Result<Vec<_>, _>>()?;
        let has_more = records.len() > request.limit;
        records.truncate(request.limit);
        let next_after = has_more
            .then(|| {
                records
                    .last()
                    .map(kg_core::traits::UnresolvedReferenceCursor::from)
            })
            .flatten();
        Ok(kg_core::traits::UnresolvedReferencePage {
            records,
            next_after,
        })
    }

    async fn confirmed_reference_dependencies(
        &self,
        org: &str,
        request: &kg_core::traits::UnresolvedReferenceQuery,
    ) -> Result<kg_core::traits::UnresolvedReferencePage, BackendError> {
        let query = cypher::reference_dependency::read(org, request)?;
        let mut records = self
            .read_prepared(&query)
            .await?
            .iter()
            .map(cypher::reference_dependency::decode)
            .collect::<Result<Vec<_>, _>>()?;
        let has_more = records.len() > request.limit;
        records.truncate(request.limit);
        let next_after = has_more
            .then(|| {
                records
                    .last()
                    .map(kg_core::traits::UnresolvedReferenceCursor::from)
            })
            .flatten();
        Ok(kg_core::traits::UnresolvedReferencePage {
            records,
            next_after,
        })
    }

    async fn reference_rule_sources(
        &self,
        org: &str,
        request: &kg_core::traits::ReferenceRuleSourceQuery,
    ) -> Result<kg_core::traits::ReferenceRuleSourcePage, BackendError> {
        let query = kg_storage_cypher::reference_dependency::rule_sources(org, request)?;
        let mut records = self
            .read_prepared(&query)
            .await?
            .into_iter()
            .map(cypher::decode_entity_version)
            .collect::<Result<Vec<_>, _>>()?;
        let next_after = if records.len() > request.limit {
            records.truncate(request.limit);
            records.last().map(|record| record.chain_id)
        } else {
            None
        };
        Ok(kg_core::traits::ReferenceRuleSourcePage {
            records,
            next_after,
        })
    }

    async fn identity_revisions(
        &self,
        org: &str,
        scopes: &[kg_core::traits::IdentityScope],
    ) -> Result<Vec<kg_core::traits::IdentityRevision>, BackendError> {
        let query = cypher::identity_revision::read(org, scopes)?;
        self.read_prepared(&query)
            .await?
            .iter()
            .map(cypher::identity_revision::decode)
            .collect()
    }

    async fn embedding_records(
        &self,
        org: &str,
        kind: kg_core::embedding_rebuild::EmbeddingKind,
        after: Option<uuid::Uuid>,
        limit: usize,
    ) -> Result<Vec<kg_core::embedding_rebuild::EmbeddingRecord>, BackendError> {
        let query = cypher::embedding_maintenance::page(org, kind, after, limit)?;
        self.read_prepared(&query)
            .await?
            .into_iter()
            .map(cypher::embedding_maintenance::decode_record)
            .collect()
    }
    async fn refresh_embeddings(
        &self,
        org: &str,
        kind: kg_core::embedding_rebuild::EmbeddingKind,
        updates: &[kg_core::embedding_rebuild::EmbeddingRefresh],
    ) -> Result<usize, BackendError> {
        let query = cypher::embedding_maintenance::refresh(org, kind, updates)?;
        let community = kind == kg_core::embedding_rebuild::EmbeddingKind::CommunityName;
        if updates.is_empty() {
            return cypher::embedding_maintenance::count(
                &self.read_prepared(&query).await?,
                "updated",
            );
        }
        let targets = cypher::identity_revision::MutationScopes {
            declared: Default::default(),
            uuids: updates.iter().map(|update| update.record.uuid).collect(),
            chains: Vec::new(),
        };
        let mut txn = tokio::time::timeout(
            self.options.timeout,
            self.graph
                .start_txn_with_timeout(self.options.server_transaction_timeout()),
        )
        .await
        .map_err(|_| BackendError::Timeout(self.options.timeout.as_millis() as u64))?
        .map_err(|error| CallError::Driver(error).into_backend("embedding refresh start failed"))?;
        let revision_token = uuid::Uuid::new_v4();
        let entity = kind == kg_core::embedding_rebuild::EmbeddingKind::Entity;
        let staged = tokio::time::timeout(self.options.timeout, async {
            run_write(
                &mut txn,
                &cypher::commit_pages::lock_revision(org, revision_token, false),
            )
            .await?;
            let locked = if community {
                let mut scope_targets = cypher::community_revision::Targets::default();
                for update in updates {
                    let namespace = update
                        .record
                        .properties
                        .get("namespace")
                        .and_then(serde_json::Value::as_str)
                        .filter(|v| !v.trim().is_empty())
                        .ok_or_else(|| {
                            CallError::Backend(BackendError::Query(
                                "Community embedding namespace is required".into(),
                            ))
                        })?;
                    scope_targets.publications.insert(namespace.to_owned());
                }
                Self::lock_community_scopes(&mut txn, org, &scope_targets, 0).await?;
                Default::default()
            } else if entity {
                Self::lock_identity_scopes(&mut txn, org, &targets, std::iter::empty()).await?
            } else {
                Default::default()
            };
            let rows = run_read(&mut txn, &query).await?;
            let count = cypher::embedding_maintenance::count(&rows, "updated")
                .map_err(CallError::Backend)?;
            if count > 0 {
                if entity {
                    Self::advance_identity_scopes(&mut txn, org, &targets, &locked).await?;
                }
                run_write(
                    &mut txn,
                    &cypher::commit_pages::advance_revision(org, revision_token),
                )
                .await?;
            }
            Ok::<usize, CallError>(count)
        })
        .await;
        let count = match staged {
            Ok(Ok(count)) => count,
            Ok(Err(error)) => {
                self.rollback(txn).await;
                return Err(error.into_backend("embedding refresh failed"));
            }
            Err(_) => {
                self.rollback(txn).await;
                return Err(BackendError::Timeout(
                    self.options.timeout.as_millis() as u64
                ));
            }
        };
        match tokio::time::timeout(self.options.timeout, txn.commit()).await {
            Ok(Ok(())) => Ok(count),
            Ok(Err(error)) if matches!(error, neo4rs::Error::Neo4j(_)) => {
                Err(CallError::Driver(error).into_backend("embedding refresh rejected"))
            }
            Ok(Err(_)) | Err(_) => Err(BackendError::UnknownCommit(
                "embedding refresh commit was not acknowledged".into(),
            )),
        }
    }
    async fn incompatible_embeddings(
        &self,
        org: &str,
        settings: &kg_core::embedding::EmbeddingSettings,
    ) -> Result<usize, BackendError> {
        let query = cypher::embedding_maintenance::incompatible(org, settings)?;
        cypher::embedding_maintenance::count(&self.read_prepared(&query).await?, "count")
    }

    async fn set_entity_embedding(
        &self,
        org_id: &str,
        uuid: uuid::Uuid,
        embedding: &GraphEmbedding,
        text_version: &str,
        content_hash: &str,
        fields: &kg_core::embedding::EntityEmbeddingFields,
    ) -> Result<(), BackendError> {
        use kg_core::{
            embedding::ComputedEmbedding,
            embedding_rebuild::{EmbeddingKind, EmbeddingRefresh},
        };
        embedding.validate()?;
        fields.validate()?;
        if text_version != fields.text_version() {
            return Err(BackendError::Query(
                "incompatible embedding text version".into(),
            ));
        }
        let query = cypher::entity_embedding_record(org_id, uuid);
        let rows = self.read_prepared(&query).await?;
        if rows.len() != 1 {
            return Err(BackendError::NotFound("live entity version".into()));
        }
        let record =
            cypher::embedding_maintenance::decode_record(rows.into_iter().next().unwrap())?;
        if kg_core::embedding::content_hash(&record.text(EmbeddingKind::Entity, fields)?)
            != content_hash
        {
            return Err(BackendError::Conflict(
                "entity changed since embedding text was prepared".into(),
            ));
        }
        let update = EmbeddingRefresh {
            entity_fields: fields.clone(),
            record,
            embedding: ComputedEmbedding {
                model: embedding.model.clone(),
                values: embedding.values.clone(),
                text_version: text_version.into(),
                content_hash: content_hash.into(),
            },
        };
        if self
            .refresh_embeddings(org_id, EmbeddingKind::Entity, &[update])
            .await?
            != 1
        {
            return Err(BackendError::Conflict(
                "entity changed while storing its embedding".into(),
            ));
        }
        Ok(())
    }

    async fn get_entity_embedding(
        &self,
        org_id: &str,
        uuid: uuid::Uuid,
    ) -> Result<Option<GraphEmbedding>, BackendError> {
        let query = cypher::get_entity_embedding(org_id, uuid);
        self.read_prepared(&query)
            .await?
            .into_iter()
            .next()
            .map(cypher::decode_entity_embedding)
            .transpose()
    }

    async fn search_entity_embeddings(
        &self,
        query: &[f32],
        model: &str,
        org_id: &str,
        namespaces: Option<&[&str]>,
        entity_types: Option<&[&str]>,
        limit: usize,
        min_score: f32,
        text_version: &str,
    ) -> Result<Vec<EntityEmbeddingHit>, BackendError> {
        let query = cypher::search_entity_embeddings(
            query,
            model,
            org_id,
            namespaces,
            entity_types,
            limit,
            min_score,
            text_version,
        )?;
        self.read_prepared(&query)
            .await?
            .into_iter()
            .map(cypher::decode_entity_embedding_hit)
            .collect()
    }

    async fn find_entities(
        &self,
        org_id: &str,
        lookup: &EntityLookup,
    ) -> Result<Vec<EntityVersionRecord>, BackendError> {
        lookup.validate(org_id)?;
        if lookup.is_empty() {
            return Ok(vec![]);
        }
        let query = cypher::entities(org_id, lookup)?;
        self.read_prepared(&query)
            .await?
            .into_iter()
            .map(cypher::decode_entity_version)
            .collect()
    }

    #[tracing::instrument(name = "neo4j.read_community", skip_all)]
    async fn read_community(
        &self,
        org: &str,
        request: &kg_core::community::CommunityRead,
    ) -> Result<kg_core::community::CommunityReadResult, BackendError> {
        let query = cypher::community::read(org, request)?;
        cypher::community::decode(request, self.read_prepared(&query).await?)
    }
    #[tracing::instrument(name = "neo4j.read_saga", skip_all)]
    async fn read_saga(
        &self,
        org: &str,
        request: &kg_core::saga::SagaRead,
    ) -> Result<kg_core::saga::SagaReadResult, BackendError> {
        let query = cypher::saga::read(org, request)?;
        cypher::saga::decode(request, self.read_prepared(&query).await?)
    }

    async fn snapshot_evidence(
        &self,
        org: &str,
        request: &kg_core::runtime::history::SnapshotEvidenceRequest,
    ) -> Result<Vec<kg_core::runtime::history::SnapshotEvidence>, BackendError> {
        request.validate(org)?;
        if request.ids.is_empty() {
            return Ok(vec![]);
        }
        let query = cypher::snapshot_evidence(org, request)?;
        let mut records = self
            .read_prepared(&query)
            .await?
            .into_iter()
            .map(|row| {
                serde_json::from_value(serde_json::Value::Object(row)).map_err(|_| {
                    BackendError::Deserialization("invalid snapshot evidence record".into())
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        kg_core::runtime::history::validate_evidence(org, request, &mut records)?;
        Ok(records)
    }

    async fn find_edges(
        &self,
        org_id: &str,
        lookup: &EdgeLookup,
    ) -> Result<Vec<EdgeRecord>, BackendError> {
        lookup.validate(org_id)?;
        if lookup.is_empty() {
            return Ok(vec![]);
        }
        let query = cypher::edges(org_id, lookup)?;
        let rows = self.read_prepared(&query).await?;
        if matches!(
            lookup,
            EdgeLookup::VersionsByChainPairs { .. }
                | EdgeLookup::VersionsByRelations { .. }
                | EdgeLookup::VersionsByReferenceOwners { .. }
                | EdgeLookup::VersionsByEndpointChains { .. }
        ) && rows.len() > kg_core::traits::relationship_timeline::MAX_VERSIONS
        {
            return Err(BackendError::RelationshipHistoryLimit {
                limit: kg_core::traits::relationship_timeline::MAX_VERSIONS,
            });
        }
        rows.into_iter().map(cypher::decode_edge).collect()
    }

    #[tracing::instrument(name = "neo4j.apply_mutations", skip_all)]
    async fn apply_mutations(
        &self,
        org: &str,
        mutations: &[kg_core::traits::GraphMutation],
    ) -> Result<(), BackendError> {
        let statements = cypher::mutations(org, mutations)?;
        let targets = cypher::identity_revision::mutation_scopes(org, mutations)?;
        if statements.is_empty() {
            return Ok(());
        }
        let mut observation = crate::telemetry::Operation::new("graph mutations");
        let result = async {
            let mut txn = tokio::time::timeout(self.options.timeout, self.graph.start_txn_with_timeout(self.options.server_transaction_timeout()))
                .await.map_err(|_| BackendError::Timeout(self.options.timeout.as_millis() as u64))?
                .map_err(|error| CallError::Driver(error).into_backend("Neo4j transaction failed"))?;
            let revision_token=uuid::Uuid::new_v4();
            let staged = tokio::time::timeout(self.options.timeout, async {
                run_write(&mut txn,&cypher::commit_pages::lock_revision(org,revision_token,false)).await?;
                let community_targets=cypher::community_revision::Targets::from_mutations(mutations);
                let community_stripe=cypher::community_revision::stripe(format!("{:?}{:?}{:?}",community_targets.entities,community_targets.chains,community_targets.edges).as_bytes());
                let community_locked=Self::lock_community_scopes(&mut txn,org,&community_targets,community_stripe).await?;
                let locked = Self::lock_identity_scopes(&mut txn, org, &targets, std::iter::empty()).await?;
                Self::check_community_scopes(&mut txn,org,&community_targets,&community_locked,None).await?;
                if community_targets.has_source() {run_write(&mut txn,&cypher::community_revision::dirty(org,&community_targets)).await?;}
                if let Some(invalidation) = cypher::entity_summary::invalidate(org, mutations) {
                    run_write(&mut txn, &invalidation).await?;
                }
                for statement in &statements {
                    run_write(&mut txn, statement).await?;
                }
                Self::advance_identity_scopes(&mut txn, org, &targets, &locked).await?;
                Self::check_community_scopes(&mut txn,org,&community_targets,&community_locked,Some(community_stripe)).await?;
                run_write(&mut txn,&cypher::commit_pages::advance_revision(org,revision_token)).await?;
                Ok::<(), CallError>(())
            }).await;
            match staged {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    self.rollback(txn).await;
                    return Err(error.into_backend("Neo4j mutation failed"));
                }
                Err(_) => {
                    self.rollback(txn).await;
                    return Err(BackendError::Timeout(self.options.timeout.as_millis() as u64));
                }
            }
            // This unreceipted API cannot prove a lost commit outcome. Never replay it.
            match tokio::time::timeout(self.options.timeout, txn.commit()).await {
                Ok(Ok(())) if self.options.faults.as_ref().is_some_and(|faults| FaultInjection::take(&faults.lose_commit_ack)) => Err(BackendError::UnknownCommit("injected acknowledgement loss after unreceipted commit".into())),
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) if matches!(error, neo4rs::Error::Neo4j(_)) =>
                    Err(CallError::Driver(error).into_backend("Neo4j commit rejected")),
                Ok(Err(_)) | Err(_) => Err(BackendError::UnknownCommit(
                    "unreceipted mutation commit was not acknowledged; inspect state before retrying".into()
                )),
            }
        }.await;
        observation.finish(match &result {
            Ok(()) => "committed",
            Err(error) => crate::telemetry::backend_error_kind(error),
        });
        result
    }

    async fn read_run(
        &self,
        org: &str,
        run_id: uuid::Uuid,
    ) -> Result<Option<RunHeader>, BackendError> {
        let query = cypher::read_run(org, run_id);
        let rows = self
            .execute_read(&query.statement, &query.parameters)
            .await?;
        if rows.len() > 1 {
            return Err(BackendError::Deserialization(
                "duplicate run headers".into(),
            ));
        }
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let stored = cypher::decode_run_header(row)?;
        let header = RunHeader {
            observation_manifest: stored.observation_manifest,
            rule_freezes: stored.rule_freezes,
            org_id: stored.org_id,
            run_id,
            fingerprint: kg_core::traits::RequestFingerprint(stored.fingerprint),
            settings_version: stored.settings_version,
            capture_default: stored.capture_default,
            batch_plan: stored.batch_plan,
            schema_manifest: stored.schema_manifest,
        };
        header.validate()?;
        Ok(Some(header))
    }

    async fn register_run(&self, header: &RunHeader) -> Result<RunRegistration, BackendError> {
        let query = cypher::register_run(header)?;
        let rows = self.read_prepared(&query).await?;
        if rows.len() > 1 {
            return Err(BackendError::Deserialization(
                "duplicate run headers".into(),
            ));
        }
        let stored = rows
            .first()
            .ok_or_else(|| BackendError::Transaction("run registration returned no row".into()))
            .and_then(cypher::decode_run_header)?;
        if stored.org_id != header.org_id
            || stored.fingerprint != header.fingerprint.0
            || stored.settings_version != header.settings_version
        {
            return Err(BackendError::Conflict(
                "run id is already registered for a different request or settings".into(),
            ));
        }
        stored
            .schema_manifest
            .validate(&header.org_id)
            .map_err(BackendError::Deserialization)?;
        stored
            .observation_manifest
            .validate()
            .map_err(BackendError::Deserialization)?;
        if stored.created {
            return Ok(RunRegistration::Registered);
        }
        let committed = self
            .committed_batches(&header.org_id, header.run_id)
            .await?;
        Ok(RunRegistration::Resumed {
            observation_manifest: stored.observation_manifest,
            schema_manifest: stored.schema_manifest,
            capture_default: stored.capture_default,
            committed,
        })
    }

    #[tracing::instrument(name = "neo4j.commit_batch", skip_all)]
    async fn commit_batch(&self, batch: &MutationBatch) -> Result<CommittedBatch, BackendError> {
        self.commit_batch_cancellable(batch, &tokio_util::sync::CancellationToken::new())
            .await
    }
    async fn commit_batch_cancellable(
        &self,
        batch: &MutationBatch,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<CommittedBatch, BackendError> {
        let mut observation = crate::telemetry::Operation::new("commit_batch");
        let result = async {
            if let Some(receipt) = self
                .resume_pages(&batch.org_id, batch.batch, &batch.fingerprint, cancel)
                .await?
            {
                return Ok(receipt);
            }
            if batch.validate().is_err() {
                let pages = kg_core::traits::commit_pages::partition(batch)?;
                return self.freeze_and_commit_pages(batch, pages, cancel).await;
            }
            let mut checks = vec![cypher::check_run(
                &batch.org_id,
                batch.batch.run_id,
                &batch.fingerprint.0,
            )];
            for precondition in &batch.preconditions {
                checks.push(cypher::precondition(&batch.org_id, precondition)?);
            }
            let writes = cypher::mutations(&batch.org_id, &batch.mutations)?;
            validate_compiled_batch_budget(batch.preconditions.len(), writes.len())?;
            let attempts = self.options.max_retries.max(1);
            let mut last = None;
            for attempt in 0..attempts {
                if attempt > 0 {
                    crate::telemetry::retry("commit_batch");
                    tokio::time::sleep(retry_delay(self.options.base_backoff, attempt - 1)).await;
                }
                match self.commit_attempt(batch, &checks, &writes, None).await {
                    Ok(committed) => return Ok(committed),
                    Err(AttemptError::Final(error)) => return Err(error),
                    Err(AttemptError::Retry(error)) => {
                        tracing::warn!(
                            attempt = attempt + 1,
                            error_kind = crate::telemetry::backend_error_kind(&error),
                            retrying = attempt + 1 < attempts,
                            "batch commit attempt failed"
                        );
                        last = Some(error);
                    }
                }
            }
            Err(last.expect("at least one attempt was made"))
        }
        .await;
        observation.finish(match &result {
            Ok(value) => {
                let _ = value;
                if value.replayed {
                    "replayed"
                } else {
                    "committed"
                }
            }
            Err(error) => crate::telemetry::backend_error_kind(error),
        });
        result
    }

    async fn committed_batches(
        &self,
        org_id: &str,
        run_id: uuid::Uuid,
    ) -> Result<Vec<CommittedBatch>, BackendError> {
        let rows = self
            .read_prepared(&cypher::run_receipts(org_id, run_id))
            .await?;
        let mut batches: Vec<CommittedBatch> = rows
            .iter()
            .map(cypher::decode_receipt)
            .map(|receipt| receipt.map(|stored| stored.batch))
            .collect::<Result<_, _>>()?;
        batches.sort_by_key(|b| (b.kind, b.index));
        Ok(batches)
    }

    async fn health(&self) -> Result<(), BackendError> {
        let graph = Arc::clone(&self.graph);
        with_retry("health", &self.options, || {
            let graph = Arc::clone(&graph);
            async move {
                graph
                    .run_once(Query::new(cypher::neo4j::admin::HEALTH.to_string()))
                    .await
                    .map_err(CallError::Driver)
            }
        })
        .await
        .map_err(|e| match e.into_backend("health check failed") {
            BackendError::Query(message) => BackendError::Connection(message),
            other => other,
        })
    }

    async fn connect(&self) -> Result<(), BackendError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), BackendError> {
        self.cleanup.drain(self.options.timeout).await
    }
}

// Timeout + retry

/// Error from a single call attempt.
pub(crate) enum CallError {
    Backend(BackendError),
    Reference,
    IdentityConflict,
    MetadataConflict,
    /// The exact entity version changed after its embedding was planned.
    EmbeddingConflict,
    SummaryConflict,
    SagaConflict,
    /// Driver-level error; retried only if [`is_transient`].
    Driver(neo4rs::Error),
    /// Conversion/deserialization error — never retried.
    Permanent(String),
    /// A single attempt exceeded the configured timeout; retried.
    Timeout(Duration),
}

impl CallError {
    fn telemetry_kind(&self) -> &'static str {
        match self {
            Self::Backend(error) => crate::telemetry::backend_error_kind(error),
            Self::Reference => "not_found",
            Self::IdentityConflict
            | Self::MetadataConflict
            | Self::EmbeddingConflict
            | Self::SummaryConflict
            | Self::SagaConflict => "conflict",
            Self::Driver(error) => crate::telemetry::driver_error_kind(error),
            Self::Permanent(_) => "deserialization",
            Self::Timeout(_) => "timeout",
        }
    }

    pub(crate) fn into_backend(self, what: &str) -> BackendError {
        match self {
            CallError::Backend(error) => error,
            CallError::SagaConflict => {
                BackendError::Conflict("Saga membership or summary evidence changed".into())
            }
            CallError::SummaryConflict => BackendError::Conflict(
                "summary evidence or accepted revision changed since planning".into(),
            ),
            CallError::EmbeddingConflict => {
                BackendError::Conflict("entity embedding content changed since planning".into())
            }
            CallError::MetadataConflict => BackendError::Conflict(
                "metadata observation is stale or conflicts at the same capture time".into(),
            ),
            CallError::IdentityConflict => {
                BackendError::Conflict("identity already belongs to another live chain".into())
            }
            CallError::Reference => BackendError::NotFound(
                "graph reference is missing, foreign, or inconsistent".into(),
            ),
            CallError::Driver(e) => match e {
                neo4rs::Error::IOError { .. } | neo4rs::Error::ConnectionError => {
                    BackendError::Connection(format!("{what}: {e}"))
                }
                neo4rs::Error::AuthenticationError(message) => {
                    BackendError::Auth(format!("{what}: {message}"))
                }
                neo4rs::Error::Neo4j(error)
                    if error.code().starts_with("Neo.ClientError.Security.") =>
                {
                    BackendError::Auth(format!("{what}: authentication or authorization failed"))
                }
                neo4rs::Error::Neo4j(error) if is_constraint_failure(error.code()) => {
                    BackendError::Conflict(format!(
                        "{what}: uniqueness constraint rejected the write"
                    ))
                }
                neo4rs::Error::Neo4j(error)
                    if error.code().starts_with("Neo.ClientError.Transaction.") =>
                {
                    BackendError::Transaction(format!(
                        "{what}: server terminated or rejected the transaction"
                    ))
                }
                other => BackendError::Query(format!("{what}: {other}")),
            },
            CallError::Permanent(msg) => BackendError::Deserialization(format!("{what}: {msg}")),
            CallError::Timeout(d) => BackendError::Timeout(d.as_millis() as u64),
        }
    }
}

/// Is this driver error worth retrying? Connection/IO failures and Neo4j
/// `TransientError`s are; query/syntax/auth/constraint errors are not.
fn is_transient(e: &neo4rs::Error) -> bool {
    match e {
        neo4rs::Error::IOError { .. } | neo4rs::Error::ConnectionError => true,
        neo4rs::Error::Neo4j(err) => matches!(
            err.kind(),
            neo4rs::Neo4jErrorKind::Transient
                | neo4rs::Neo4jErrorKind::Client(neo4rs::Neo4jClientErrorKind::SessionExpired)
        ),
        _ => false,
    }
}

/// Run `attempt` with a per-attempt timeout, retrying transient failures up
/// to `opts.max_retries` total attempts with exponential backoff.
#[tracing::instrument(name = "neo4j.op", skip_all, fields(operation = op))]
async fn with_retry<T, F, Fut>(
    op: &str,
    opts: &Neo4jOptions,
    mut attempt: F,
) -> Result<T, CallError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, CallError>>,
{
    let mut observation = crate::telemetry::Operation::new(op);
    let result = async {
        let attempts = opts.max_retries.max(1);
        let mut last: Option<CallError> = None;
        for n in 0..attempts {
            match tokio::time::timeout(opts.timeout, attempt()).await {
                Ok(Ok(v)) => return Ok(v),
                Ok(Err(error @ CallError::Backend(_))) => return Err(error),
                Ok(Err(CallError::Reference)) => return Err(CallError::Reference),
                Ok(Err(CallError::IdentityConflict)) => return Err(CallError::IdentityConflict),
                Ok(Err(CallError::MetadataConflict)) => return Err(CallError::MetadataConflict),
                Ok(Err(CallError::EmbeddingConflict)) => return Err(CallError::EmbeddingConflict),
                Ok(Err(CallError::SummaryConflict)) => return Err(CallError::SummaryConflict),
                Ok(Err(CallError::SagaConflict)) => return Err(CallError::SagaConflict),
                Ok(Err(CallError::Permanent(msg))) => return Err(CallError::Permanent(msg)),
                Ok(Err(timeout @ CallError::Timeout(_))) => {
                    last = Some(timeout);
                }
                Ok(Err(CallError::Driver(e))) => {
                    if !is_transient(&e) {
                        return Err(CallError::Driver(e));
                    }
                    tracing::warn!(
                        attempt = n + 1,
                        error_kind = crate::telemetry::driver_error_kind(&e),
                        retrying = n + 1 < attempts,
                        "transient Neo4j error"
                    );
                    last = Some(CallError::Driver(e));
                }
                Err(_) => {
                    tracing::warn!(
                        attempt = n + 1,
                        timeout_ms = opts.timeout.as_millis() as u64,
                        retrying = n + 1 < attempts,
                        "Neo4j call timed out"
                    );
                    last = Some(CallError::Timeout(opts.timeout));
                }
            }
            if n + 1 < attempts {
                crate::telemetry::retry(op);
                tokio::time::sleep(retry_delay(opts.base_backoff, n)).await;
            }
        }
        Err(last.expect("at least one attempt was made"))
    }
    .await;
    observation.finish(match &result {
        Ok(value) => {
            let _ = value;
            "success"
        }
        Err(error) => error.telemetry_kind(),
    });
    result
}

// JSON → Bolt parameter binding

/// Build a [`Query`] with every top-level key of `params` bound. Binding is
/// total: `null` binds as Bolt `Null` (it is NOT skipped), arrays and objects
/// convert recursively.
pub(crate) fn build_query(cypher: &str, params: &serde_json::Value) -> Query {
    let mut q = Query::new(cypher.to_string());
    if let Some(obj) = params.as_object() {
        for (key, value) in obj {
            q = q.param(key, json_to_bolt(value));
        }
    }
    q
}

/// Total, recursive JSON → Bolt conversion. Never drops values:
/// - `null` → `BoltNull`
/// - integers → `BoltInteger` (u64 above `i64::MAX` falls back to float)
/// - floats → `BoltFloat`
/// - arrays of ANY values (including numeric embeddings) → `BoltList`
/// - objects → nested `BoltMap`
pub(crate) fn json_to_bolt(value: &serde_json::Value) -> BoltType {
    match value {
        serde_json::Value::Null => BoltType::Null(neo4rs::BoltNull),
        serde_json::Value::Bool(b) => BoltType::Boolean(neo4rs::BoltBoolean { value: *b }),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                BoltType::Integer(neo4rs::BoltInteger { value: i })
            } else {
                // u64 > i64::MAX or a float — represent as f64 (lossy only for
                // huge u64s, which Bolt cannot represent as an integer anyway).
                BoltType::Float(neo4rs::BoltFloat {
                    value: n.as_f64().unwrap_or(f64::NAN),
                })
            }
        }
        serde_json::Value::String(s) => BoltType::String(neo4rs::BoltString { value: s.clone() }),
        serde_json::Value::Array(arr) => BoltType::List(neo4rs::BoltList {
            value: arr.iter().map(json_to_bolt).collect(),
        }),
        serde_json::Value::Object(map) => BoltType::Map(BoltMap {
            value: map
                .iter()
                .map(|(k, v)| (neo4rs::BoltString { value: k.clone() }, json_to_bolt(v)))
                .collect(),
        }),
    }
}

// Bolt → JSON row extraction

/// Convert a full result row to a JSON map:
/// - every column is inserted under its column name (nodes/relationships as
///   JSON objects of their properties, scalars/lists/maps converted
///   recursively, temporal values as RFC3339-style strings);
/// - additionally, node and relationship properties are flattened into the
///   top-level map so callers can do `row.get("uuid")` directly.
///
/// Flattening never clobbers an existing key; columns are processed in
/// lexicographic column-name order so multi-entity rows (e.g. `RETURN d, a`)
/// are deterministic — the full per-column objects are always available
/// under their column names.
pub(crate) fn row_to_json(
    row: &Row,
) -> Result<serde_json::Map<String, serde_json::Value>, CallError> {
    let attrs: BoltMap = row
        .to_strict()
        .map_err(|e| CallError::Permanent(format!("row decode failed: {e}")))?;

    let mut columns: Vec<(&str, &BoltType)> = attrs
        .value
        .iter()
        .map(|(k, v)| (k.value.as_str(), v))
        .collect();
    columns.sort_by(|a, b| a.0.cmp(b.0));

    let mut out = serde_json::Map::with_capacity(columns.len());
    // Pass 1: every column under its own name.
    for (name, value) in &columns {
        out.insert(
            (*name).to_string(),
            bolt_to_json(value).map_err(CallError::Permanent)?,
        );
    }
    // Pass 2: flatten node/relationship properties to the top level.
    for (_, value) in &columns {
        let props = match value {
            BoltType::Node(n) => &n.properties,
            BoltType::Relation(r) => &r.properties,
            BoltType::UnboundedRelation(r) => &r.properties,
            _ => continue,
        };
        flatten_props(props, &mut out).map_err(CallError::Permanent)?;
    }
    Ok(out)
}

/// Insert every property into `out` unless the key already exists.
fn flatten_props(
    props: &BoltMap,
    out: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<(), String> {
    let mut entries: Vec<(&str, &BoltType)> = props
        .value
        .iter()
        .map(|(k, v)| (k.value.as_str(), v))
        .collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    for (key, value) in entries {
        if !out.contains_key(key) {
            out.insert(key.to_string(), bolt_to_json(value)?);
        }
    }
    Ok(())
}

/// Recursive Bolt → JSON conversion supporting every Bolt property type:
/// strings, integers, floats, booleans, lists, maps, bytes, points, paths,
/// nodes/relationships (as property objects), durations (seconds as a
/// number), and all temporal types (RFC3339-style strings).
pub(crate) fn bolt_to_json(value: &BoltType) -> Result<serde_json::Value, String> {
    use serde_json::Value as J;
    Ok(match value {
        BoltType::Null(_) => J::Null,
        BoltType::Boolean(b) => J::Bool(b.value),
        BoltType::Integer(i) => J::from(i.value),
        BoltType::Float(f) => serde_json::Number::from_f64(f.value)
            .map(J::Number)
            .unwrap_or(J::Null),
        BoltType::String(s) => J::String(s.value.clone()),
        BoltType::List(list) => J::Array(
            list.value
                .iter()
                .map(bolt_to_json)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        BoltType::Map(map) => bolt_map_to_json(map)?,
        BoltType::Node(n) => bolt_map_to_json(&n.properties)?,
        BoltType::Relation(r) => bolt_map_to_json(&r.properties)?,
        BoltType::UnboundedRelation(r) => bolt_map_to_json(&r.properties)?,
        BoltType::Path(p) => {
            let nodes = p
                .nodes
                .value
                .iter()
                .map(bolt_to_json)
                .collect::<Result<Vec<_>, _>>()?;
            let rels = p
                .rels
                .value
                .iter()
                .map(bolt_to_json)
                .collect::<Result<Vec<_>, _>>()?;
            serde_json::json!({ "nodes": nodes, "relationships": rels })
        }
        BoltType::Bytes(b) => J::Array(b.value.iter().map(|byte| J::from(*byte)).collect()),
        BoltType::Point2D(p) => {
            serde_json::json!({ "srid": p.sr_id.value, "x": p.x.value, "y": p.y.value })
        }
        BoltType::Point3D(p) => serde_json::json!({
            "srid": p.sr_id.value, "x": p.x.value, "y": p.y.value, "z": p.z.value
        }),
        // Match the driver's average-month convention while retaining the sign.
        BoltType::Duration(d) => serde_json::Number::from_f64(d.as_seconds_f64())
            .map(J::Number)
            .unwrap_or(J::Null),
        BoltType::Date(d) => {
            let date = NaiveDate::try_from(d).map_err(|e| format!("invalid Bolt date: {e}"))?;
            J::String(date.format("%Y-%m-%d").to_string())
        }
        BoltType::Time(t) => {
            let (time, offset): (NaiveTime, FixedOffset) = t.into();
            J::String(format!("{}{}", time.format("%H:%M:%S%.f"), offset))
        }
        BoltType::LocalTime(t) => {
            let time: NaiveTime = t.into();
            J::String(time.format("%H:%M:%S%.f").to_string())
        }
        BoltType::DateTime(dt) => {
            let dt = DateTime::<FixedOffset>::try_from(dt)
                .map_err(|e| format!("invalid Bolt datetime: {e}"))?;
            J::String(dt.to_rfc3339())
        }
        BoltType::LocalDateTime(dt) => {
            let dt =
                NaiveDateTime::try_from(dt).map_err(|e| format!("invalid Bolt datetime: {e}"))?;
            J::String(dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string())
        }
        BoltType::DateTimeZoneId(dt) => {
            let parsed = DateTime::<FixedOffset>::try_from(dt)
                .map_err(|e| format!("invalid Bolt zoned datetime: {e}"))?;
            J::String(parsed.to_rfc3339())
        }
    })
}

/// Convert a `BoltMap` into a JSON object, recursing into values. Keys are
/// inserted in sorted order so output is deterministic.
fn bolt_map_to_json(map: &BoltMap) -> Result<serde_json::Value, String> {
    let mut entries: Vec<(&str, &BoltType)> = map
        .value
        .iter()
        .map(|(k, v)| (k.value.as_str(), v))
        .collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    let mut out = serde_json::Map::with_capacity(entries.len());
    for (key, value) in entries {
        out.insert(key.to_string(), bolt_to_json(value)?);
    }
    Ok(serde_json::Value::Object(out))
}

#[async_trait]
impl kg_core::traits::SearchBackend for Neo4jGraphBackend {
    async fn search_communities(
        &self,
        request: &kg_core::search::CommunitySearch,
        indexed: bool,
    ) -> Result<kg_core::search::SearchPage<kg_core::search::CommunityHit>, BackendError> {
        self.retrieve_communities(request, indexed).await
    }
    async fn search_entity_summaries(
        &self,
        request: &kg_core::search::SummarySearch,
        indexed: bool,
    ) -> Result<kg_core::search::NodePage, BackendError> {
        self.retrieve_summary_nodes(request, indexed).await
    }
    async fn summary_readiness(
        &self,
        request: &kg_core::search::EmbeddingReadinessRequest,
    ) -> Result<kg_core::search::SummaryReadiness, BackendError> {
        let query = kg_storage_cypher::summary_readiness(request)?;
        let rows = self
            .execute_search_read(&query.statement, &query.parameters)
            .await?;
        if rows.len() != 1 {
            return Err(BackendError::Deserialization(
                "invalid summary readiness row count".into(),
            ));
        }
        serde_json::from_value(serde_json::Value::Object(rows.into_iter().next().unwrap()))
            .map_err(|_| BackendError::Deserialization("invalid summary readiness counts".into()))
    }

    async fn search_nodes_indexed(
        &self,
        request: &kg_core::search::NodeSearch,
    ) -> Result<kg_core::search::NodePage, BackendError> {
        self.retrieve_nodes_indexed(request).await
    }
    async fn search_relationships_indexed(
        &self,
        request: &kg_core::search::RelationshipSimilarity,
    ) -> Result<kg_core::search::SearchPage<kg_core::search::RelationshipHit>, BackendError> {
        self.retrieve_relationships_indexed(request).await
    }
    async fn embedding_readiness(
        &self,
        request: &kg_core::search::EmbeddingReadinessRequest,
    ) -> Result<kg_core::search::EmbeddingReadiness, BackendError> {
        let query = kg_storage_cypher::embedding_readiness(request)?;
        let rows = self
            .execute_search_read(&query.statement, &query.parameters)
            .await?;
        kg_storage_cypher::decode_embedding_readiness(rows)
    }
    async fn search_nodes(
        &self,
        request: &kg_core::search::NodeSearch,
    ) -> Result<kg_core::search::NodePage, BackendError> {
        self.retrieve_nodes(request).await
    }
    async fn search_relationships(
        &self,
        request: &kg_core::search::EvidenceSearch,
    ) -> Result<kg_core::search::SearchPage<kg_core::search::RelationshipHit>, BackendError> {
        self.retrieve_relationships(request).await
    }
    async fn search_relationship_similarity(
        &self,
        request: &kg_core::search::RelationshipSimilarity,
    ) -> Result<kg_core::search::SearchPage<kg_core::search::RelationshipHit>, BackendError> {
        let query = kg_storage_cypher::relationship_similarity(request)?;
        let rows = self
            .execute_search_read(&query.statement, &query.parameters)
            .await?;
        kg_storage_cypher::decode_relationships(rows, request.limit)
    }
    async fn attached_relationships(
        &self,
        request: &kg_core::search::AttachedEvidence,
    ) -> Result<
        kg_core::search::SearchPage<kg_core::search::Attached<kg_core::search::RelationshipHit>>,
        BackendError,
    > {
        self.retrieve_attached_relationships(request).await
    }
    async fn attached_snapshots(
        &self,
        request: &kg_core::search::AttachedEvidence,
    ) -> Result<
        kg_core::search::SearchPage<kg_core::search::Attached<kg_core::search::SnapshotHit>>,
        BackendError,
    > {
        self.retrieve_attached_snapshots(request).await
    }
    async fn search_snapshots(
        &self,
        request: &kg_core::search::EvidenceSearch,
    ) -> Result<kg_core::search::SearchPage<kg_core::search::SnapshotHit>, BackendError> {
        self.retrieve_snapshots(request).await
    }
}

#[cfg(test)]
mod tests;

fn is_constraint_failure(code: &str) -> bool {
    matches!(
        code,
        "Neo.ClientError.Schema.ConstraintValidationFailed"
            | "Neo.ClientError.Schema.ConstraintVerificationFailed"
    )
}

fn retry_delay(base: Duration, exponent: u32) -> Duration {
    base.saturating_mul(2u32.saturating_pow(exponent))
        .min(Duration::from_secs(5))
}

impl Neo4jOptions {
    fn server_transaction_timeout(&self) -> Duration {
        self.timeout.saturating_mul(3)
    }
}

fn unique_receipt(
    rows: &[serde_json::Map<String, serde_json::Value>],
) -> Result<Option<StoredReceipt>, BackendError> {
    if rows.len() > 1 {
        return Err(BackendError::UnknownCommit(
            "multiple receipts for one batch identity; repair receipt integrity before replay"
                .into(),
        ));
    }
    rows.first().map(cypher::decode_receipt).transpose()
}

fn classify_call(error: CallError, context: &str) -> AttemptError {
    match error {
        CallError::Driver(error) => classify_driver(error, context),
        CallError::Timeout(duration) => {
            AttemptError::Retry(BackendError::Timeout(duration.as_millis() as u64))
        }
        other => AttemptError::Final(other.into_backend(context)),
    }
}

// Protocol queries (receipts, run checks, revision locks) are outside the caller-work cap.
fn validate_compiled_batch_budget(preconditions: usize, writes: usize) -> Result<(), BackendError> {
    let limit = kg_core::traits::graph_commit::MAX_STATEMENTS_PER_BATCH;
    if preconditions.saturating_add(writes) > limit {
        return Err(BackendError::Query(format!(
            "compiled batch exceeds the {limit} mutation and precondition statement limit"
        )));
    }
    Ok(())
}

/// Persisted decisions from rows. A row of another organization fails the
/// read; a malformed row (a record written by a build this one cannot read)
/// is skipped with a warning so one bad record never blocks every later run
/// of the same source.
fn decode_persisted_decisions(
    org: &str,
    rows: &[serde_json::Map<String, serde_json::Value>],
) -> Result<Vec<kg_core::runtime::reference_resolution::PersistedDecision>, BackendError> {
    let mut decisions = Vec::with_capacity(rows.len());
    for row in rows {
        cypher::reference_decision::row_organization(org, row)?;
        match cypher::reference_decision::decode(org, row) {
            Ok(decision) => decisions.push(decision),
            Err(error) => tracing::warn!(
                error = %error,
                "skipping a persisted reference decision this build cannot read"
            ),
        }
    }
    Ok(decisions)
}
