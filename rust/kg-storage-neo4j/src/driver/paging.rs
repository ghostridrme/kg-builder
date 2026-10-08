//! Durable plans are frozen before the first page. Parent receipts are published
//! only in the final transaction; replay reads this plan before invoking models.
use super::*;
use cypher::commit_pages as queries;
use kg_core::traits::{BatchIdentity, RequestFingerprint};
use tokio_util::sync::CancellationToken;

pub(super) struct PageCommit<'a> {
    pub parent: &'a MutationBatch,
    pub ordinal: usize,
    pub total: usize,
}
fn cancelled(cancel: &CancellationToken) -> Result<(), BackendError> {
    if cancel.is_cancelled() {
        Err(BackendError::Other(
            "commit cancelled between durable pages; resume the same run".into(),
        ))
    } else {
        Ok(())
    }
}
impl Neo4jGraphBackend {
    async fn plan_write(
        &self,
        batch: &MutationBatch,
        writes: &[PreparedWrite],
    ) -> Result<(), BackendError> {
        let timeout = self.options.timeout;
        let mut txn = tokio::time::timeout(
            timeout,
            self.graph
                .start_txn_with_timeout(self.options.server_transaction_timeout()),
        )
        .await
        .map_err(|_| BackendError::Timeout(timeout.as_millis() as u64))?
        .map_err(|e| CallError::Driver(e).into_backend("commit plan begin"))?;
        let staged = tokio::time::timeout(timeout, async {
            run_write(
                &mut txn,
                &cypher::check_run(&batch.org_id, batch.batch.run_id, &batch.fingerprint.0),
            )
            .await?;
            for write in writes {
                run_write(&mut txn, write).await?;
            }
            Ok::<_, CallError>(())
        })
        .await;
        match staged {
            Ok(Ok(())) => {}
            other => {
                self.rollback(txn).await;
                return Err(match other {
                    Ok(Err(e)) => e.into_backend("commit plan rejected"),
                    _ => BackendError::Timeout(timeout.as_millis() as u64),
                });
            }
        }
        match tokio::time::timeout(timeout, txn.commit()).await {
            Ok(Ok(())) => Ok(()),
            _ => Err(BackendError::UnknownCommit(
                "commit plan acknowledgment uncertain; resume the same run".into(),
            )),
        }
    }
    pub(super) async fn freeze_and_commit_pages(
        &self,
        batch: &MutationBatch,
        pages: Vec<MutationBatch>,
        cancel: &CancellationToken,
    ) -> Result<CommittedBatch, BackendError> {
        // Admission already validated these exact boundaries; freeze them once.
        let (digest, parts) = queries::encode(batch, pages)?;
        cancelled(cancel)?;
        let mut writes = vec![queries::begin(batch, digest, parts.len())];
        for (index, part) in parts.iter().enumerate() {
            writes.push(queries::save_part(batch, digest, index, part));
        }
        writes.push(queries::ready(batch, digest, parts.len()));
        // One metadata transaction: interrupted upload cannot leave an unusable
        // partial plan that would force different model results onto the same ID.
        self.plan_write(batch, &writes).await?;
        self.resume_pages(&batch.org_id, batch.batch, &batch.fingerprint, cancel)
            .await?
            .ok_or_else(|| BackendError::UnknownCommit("frozen commit plan unavailable".into()))
    }
    pub(super) async fn resume_pages(
        &self,
        org: &str,
        id: BatchIdentity,
        fingerprint: &RequestFingerprint,
        cancel: &CancellationToken,
    ) -> Result<Option<CommittedBatch>, BackendError> {
        let query = queries::read(org, id);
        let rows = self
            .execute_read(&query.statement, &query.parameters)
            .await?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        if rows.len() != 1
            || row.get("fingerprint").and_then(|v| v.as_str()) != Some(fingerprint.0.as_str())
        {
            return Err(BackendError::Conflict(
                "frozen commit plan belongs to another request".into(),
            ));
        }
        // Completed plans release their bulk parts atomically with this receipt.
        if let Some(stored) = self.verify_receipt(id.batch_id()).await? {
            if stored.org_id != org || stored.fingerprint != fingerprint.0 {
                return Err(BackendError::Conflict(
                    "paged receipt belongs to another request".into(),
                ));
            }
            return Ok(Some(stored.batch));
        }
        let invalid = || BackendError::Deserialization("invalid frozen commit plan".into());
        let count = row
            .get("parts")
            .and_then(|v| v.as_u64())
            .ok_or_else(invalid)? as usize;
        let next = row
            .get("next_page")
            .and_then(|v| v.as_u64())
            .ok_or_else(invalid)? as usize;
        if count == 0 || count > queries::MAX_PLAN_BYTES / queries::PART_BYTES + 1 {
            return Err(invalid());
        }
        let mut body = String::new();
        for index in 0..count {
            cancelled(cancel)?;
            let query = queries::part(org, id.batch_id(), index);
            let rows = self
                .execute_read(&query.statement, &query.parameters)
                .await?;
            let Some(part) = rows
                .first()
                .and_then(|r| r.get("body"))
                .and_then(|v| v.as_str())
            else {
                // Another resumer may have finished and released the parts while
                // this reader was loading them. Its parent receipt is decisive.
                if let Some(stored) = self.verify_receipt(id.batch_id()).await? {
                    if stored.org_id != org || stored.fingerprint != fingerprint.0 {
                        return Err(invalid());
                    }
                    return Ok(Some(stored.batch));
                }
                return Err(invalid());
            };
            if rows.len() != 1
                || part.len() > queries::PART_BYTES
                || body.len().saturating_add(part.len()) > queries::MAX_PLAN_BYTES
            {
                return Err(invalid());
            }
            body.push_str(part);
        }
        let digest = uuid::Uuid::new_v5(&id.batch_id(), body.as_bytes()).to_string();
        if row.get("digest").and_then(|v| v.as_str()) != Some(digest.as_str()) {
            return Err(invalid());
        }
        let plan: queries::FrozenPlan = serde_json::from_str(&body).map_err(|_| invalid())?;
        if plan.version != 1 {
            return Err(BackendError::Query(
                "unsupported frozen commit plan version".into(),
            ));
        }
        let parent = plan.parent;
        if parent.org_id != org || parent.batch != id || parent.fingerprint != *fingerprint {
            return Err(invalid());
        }
        parent.validate()?;
        if let Some(stored) = self.verify_receipt(id.batch_id()).await? {
            return replayed(&parent, stored).map(Some);
        }
        let pages = plan.pages;
        for page in &pages {
            if page.org_id != org || page.batch != id || page.fingerprint != *fingerprint {
                return Err(invalid());
            }
            page.validate()?;
        }
        if next >= pages.len() {
            return Err(invalid());
        }
        let mut completed_here = false;
        for (ordinal, page) in pages.iter().enumerate().skip(next) {
            cancelled(cancel)?;
            let mut checks = vec![cypher::check_run(org, id.run_id, &fingerprint.0)];
            for guard in &page.preconditions {
                checks.push(cypher::precondition(org, guard)?);
            }
            let writes = cypher::mutations(org, &page.mutations)?;
            validate_compiled_batch_budget(page.preconditions.len(), writes.len())?;
            let descriptor = PageCommit {
                parent: &parent,
                ordinal,
                total: pages.len(),
            };
            let attempts = self.options.max_retries.max(1);
            for attempt in 0..attempts {
                cancelled(cancel)?;
                if attempt > 0 {
                    tokio::time::sleep(retry_delay(self.options.base_backoff, attempt - 1)).await;
                    cancelled(cancel)?;
                }
                match self
                    .commit_attempt(page, &checks, &writes, Some(&descriptor))
                    .await
                {
                    Ok(receipt) => { if ordinal+1==pages.len() { completed_here = !receipt.replayed; } break; },
                    Err(AttemptError::Retry(_)) if attempt + 1 < attempts => continue,
                    Err(AttemptError::Final(BackendError::Conflict(message))) => return Err(BackendError::Query(format!("frozen commit page rejected: {message}; parent remains incomplete, use a new run to replan"))),
                    Err(AttemptError::Final(BackendError::IdentityRevisionChanged)) => return Err(BackendError::Query("identity changed before frozen page commit; parent remains incomplete, use a new run to replan".into())),
                    Err(AttemptError::Final(error) | AttemptError::Retry(error)) => {
                        return Err(error)
                    }
                }
            }
        }
        let stored = self.verify_receipt(id.batch_id()).await?.ok_or_else(|| {
            BackendError::UnknownCommit(
                "commit pages finished but parent receipt is unavailable".into(),
            )
        })?;
        replayed(&parent, stored).map(|mut receipt| {
            receipt.replayed = !completed_here;
            Some(receipt)
        })
    }
}
