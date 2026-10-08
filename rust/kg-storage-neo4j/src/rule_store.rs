//! Neo4j-backed learned-rule storage.
//!
//! Reads run outside any transaction. Every mutation runs in its own
//! transaction as a single guarded statement so optimistic concurrency is
//! enforced by the database, not by a read-modify-write race: `propose` creates
//! only when the id is absent and `transition` writes only when the stored
//! revision matches the caller's expectation. A guard miss returns no row and
//! becomes [`BackendError::Conflict`]; a commit whose outcome the transport did
//! not report becomes [`BackendError::UnknownCommit`] and is never retried, so a
//! transition can never be silently applied twice.
use crate::driver::{build_query, row_to_json, CallError};
use crate::Neo4jGraphBackend;
use async_trait::async_trait;
use kg_core::errors::BackendError;
use kg_core::traits::rule_store::{LearnedRule, RuleStatus, RuleStore, RuleTransition};
use kg_storage_cypher::rule_store as cypher;
use neo4rs::Txn;
use serde_json::{Map, Value};
use uuid::Uuid;

impl Neo4jGraphBackend {
    async fn begin_rule_txn(&self) -> Result<Txn, BackendError> {
        tokio::time::timeout(
            self.options.timeout,
            self.graph.start_txn_with_timeout(self.options.timeout),
        )
        .await
        .map_err(|_| BackendError::Timeout(self.options.timeout.as_millis() as u64))?
        .map_err(|error| CallError::Driver(error).into_backend("rule transaction start failed"))
    }

    async fn rule_txn_rows(
        txn: &mut Txn,
        query: &kg_storage_cypher::PreparedQuery,
    ) -> Result<Vec<Map<String, Value>>, CallError> {
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

    async fn abort_rule_txn(txn: Txn) {
        // Best effort: a failed rollback still releases on transaction timeout.
        let _ = txn.rollback().await;
    }

    async fn commit_rule_txn(&self, txn: Txn, what: &str) -> Result<(), BackendError> {
        match tokio::time::timeout(self.options.timeout, txn.commit()).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) if matches!(error, neo4rs::Error::Neo4j(_)) => {
                Err(CallError::Driver(error).into_backend(what))
            }
            Ok(Err(_)) | Err(_) => Err(BackendError::UnknownCommit(format!(
                "{what}: commit was not acknowledged"
            ))),
        }
    }
}

#[async_trait]
impl RuleStore for Neo4jGraphBackend {
    async fn get(&self, org_id: &str, id: Uuid) -> Result<Option<LearnedRule>, BackendError> {
        let query = cypher::get(org_id, id)?;
        let rows = self
            .execute_read(&query.statement, &query.parameters)
            .await?;
        match rows.first() {
            Some(row) => Ok(Some(cypher::decode(row)?)),
            None => Ok(None),
        }
    }

    async fn list_active(
        &self,
        org_id: &str,
        source: &str,
    ) -> Result<Vec<LearnedRule>, BackendError> {
        let query = cypher::list_active(org_id, source)?;
        let rows = self
            .execute_read(&query.statement, &query.parameters)
            .await?;
        if rows.len() > cypher::MAX_ACTIVE_RULES {
            return Err(BackendError::Query(
                "active learned rules exceed the source budget".into(),
            ));
        }
        rows.iter().map(cypher::decode).collect()
    }

    async fn list_all(&self, org_id: &str, source: &str) -> Result<Vec<LearnedRule>, BackendError> {
        let query = cypher::list_all(org_id, source)?;
        let rows = self
            .execute_read(&query.statement, &query.parameters)
            .await?;
        if rows.len() > cypher::MAX_ACTIVE_RULES {
            return Err(BackendError::Query(
                "learned rules exceed the administrative listing budget".into(),
            ));
        }
        rows.iter().map(cypher::decode).collect()
    }

    async fn list_all_active(&self, org_id: &str) -> Result<Vec<LearnedRule>, BackendError> {
        let query = cypher::list_all_active(org_id)?;
        let rows = self
            .execute_read(&query.statement, &query.parameters)
            .await?;
        rows.iter().map(cypher::decode).collect()
    }

    async fn propose(&self, rule: LearnedRule) -> Result<LearnedRule, BackendError> {
        rule.validate().map_err(BackendError::Query)?;
        if rule.revision != 1 {
            return Err(BackendError::Query(
                "a newly proposed rule starts at revision 1".into(),
            ));
        }
        if !matches!(rule.status, RuleStatus::Proposed | RuleStatus::Uncertain) {
            return Err(BackendError::Query(
                "a new rule enters as proposed or uncertain".into(),
            ));
        }
        let query = cypher::propose(&rule)?;
        let mut txn = self.begin_rule_txn().await?;
        let rows = match Self::rule_txn_rows(&mut txn, &query).await {
            Ok(rows) => rows,
            Err(error) => {
                Self::abort_rule_txn(txn).await;
                return Err(error.into_backend("rule proposal failed"));
            }
        };
        let created = rows
            .first()
            .and_then(|row| row.get("created"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if !created {
            Self::abort_rule_txn(txn).await;
            return Err(BackendError::Conflict(
                "a learned rule with this id already exists".into(),
            ));
        }
        self.commit_rule_txn(txn, "rule proposal rejected").await?;
        Ok(rule)
    }

    async fn transition(
        &self,
        org_id: &str,
        id: Uuid,
        expected_revision: u64,
        transition: RuleTransition,
    ) -> Result<LearnedRule, BackendError> {
        let read = cypher::read_for_update(org_id, id)?;
        let mut txn = self.begin_rule_txn().await?;
        let rows = match Self::rule_txn_rows(&mut txn, &read).await {
            Ok(rows) => rows,
            Err(error) => {
                Self::abort_rule_txn(txn).await;
                return Err(error.into_backend("rule read for transition failed"));
            }
        };
        let current = match rows.first() {
            Some(row) => cypher::decode(row)?,
            None => {
                Self::abort_rule_txn(txn).await;
                return Err(BackendError::NotFound("learned rule".into()));
            }
        };
        if current.revision != expected_revision {
            Self::abort_rule_txn(txn).await;
            return Err(BackendError::Conflict(
                "learned rule was modified since it was read".into(),
            ));
        }
        let updated = match current.with_transition(expected_revision, &transition) {
            Ok(updated) => updated,
            Err(error) => {
                Self::abort_rule_txn(txn).await;
                return Err(error);
            }
        };
        let write = cypher::apply_transition(&updated, expected_revision)?;
        let written = match Self::rule_txn_rows(&mut txn, &write).await {
            Ok(rows) => rows,
            Err(error) => {
                Self::abort_rule_txn(txn).await;
                return Err(error.into_backend("rule transition failed"));
            }
        };
        if written.len() != 1 {
            // The revision changed between the in-transaction read and the
            // guarded write; report a conflict rather than a partial update.
            Self::abort_rule_txn(txn).await;
            return Err(BackendError::Conflict(
                "learned rule revision changed during transition".into(),
            ));
        }
        self.commit_rule_txn(txn, "rule transition rejected")
            .await?;
        Ok(updated)
    }

    async fn supersede(
        &self,
        org_id: &str,
        id: Uuid,
        expected_revision: u64,
        updated: LearnedRule,
    ) -> Result<LearnedRule, BackendError> {
        updated.validate().map_err(BackendError::Query)?;
        if updated.id != id || updated.org_id != org_id || updated.revision != expected_revision + 1
        {
            return Err(BackendError::Query(
                "superseding rule must keep id/org and bump to expected_revision + 1".into(),
            ));
        }
        // A single guarded statement writes the whole new body (mapping and
        // fingerprint included) only when the stored revision still matches.
        let write = cypher::apply_transition(&updated, expected_revision)?;
        let mut txn = self.begin_rule_txn().await?;
        let written = match Self::rule_txn_rows(&mut txn, &write).await {
            Ok(rows) => rows,
            Err(error) => {
                Self::abort_rule_txn(txn).await;
                return Err(error.into_backend("rule supersede failed"));
            }
        };
        if written.len() != 1 {
            Self::abort_rule_txn(txn).await;
            return Err(BackendError::Conflict(
                "learned rule was modified since it was read".into(),
            ));
        }
        self.commit_rule_txn(txn, "rule supersede rejected").await?;
        Ok(updated)
    }

    async fn install_schema(&self) -> Result<(), BackendError> {
        for (name, ddl) in cypher::SCHEMA {
            self.run_rule_ddl(name, ddl).await?;
        }
        Ok(())
    }
}

impl Neo4jGraphBackend {
    async fn run_rule_ddl(&self, name: &str, ddl: &str) -> Result<(), BackendError> {
        match self
            .graph
            .run_once(neo4rs::Query::new(ddl.to_string()))
            .await
        {
            Ok(()) => Ok(()),
            // Concurrent installers both pass `IF NOT EXISTS`; the rule then
            // exists as requested, which is the goal.
            Err(neo4rs::Error::Neo4j(error))
                if error.code() == "Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists" =>
            {
                Ok(())
            }
            Err(error) => {
                Err(CallError::Driver(error).into_backend(&format!("rule schema `{name}` failed")))
            }
        }
    }
}
