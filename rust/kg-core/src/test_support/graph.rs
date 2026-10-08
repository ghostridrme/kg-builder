//! A graph backend that must never be reached. Stages whose contract is
//! "does not touch storage" run over it; any call panics with the operation.

use crate::errors::BackendError;
use crate::traits::*;
use async_trait::async_trait;
use uuid::Uuid;

/// Null-object `GraphBackend`: every operation panics.
#[derive(Debug, Default, Clone, Copy)]
pub struct UnreachableGraph;

fn no_graph<T>(operation: &str) -> Result<T, BackendError> {
    panic!("this test must not call storage, but `{operation}` was invoked")
}

#[async_trait]
impl SearchBackend for UnreachableGraph {}

#[async_trait]
impl GraphBackend for UnreachableGraph {
    async fn apply_mutations(&self, _: &str, _: &[GraphMutation]) -> Result<(), BackendError> {
        no_graph("apply_mutations")
    }

    async fn register_run(&self, _: &RunHeader) -> Result<RunRegistration, BackendError> {
        no_graph("register_run")
    }

    async fn commit_batch(&self, _: &MutationBatch) -> Result<CommittedBatch, BackendError> {
        no_graph("commit_batch")
    }

    async fn committed_batches(
        &self,
        _: &str,
        _: Uuid,
    ) -> Result<Vec<CommittedBatch>, BackendError> {
        no_graph("committed_batches")
    }

    async fn find_entities(
        &self,
        _: &str,
        _: &EntityLookup,
    ) -> Result<Vec<EntityVersionRecord>, BackendError> {
        no_graph("find_entities")
    }

    async fn find_edges(&self, _: &str, _: &EdgeLookup) -> Result<Vec<EdgeRecord>, BackendError> {
        no_graph("find_edges")
    }

    async fn health(&self) -> Result<(), BackendError> {
        no_graph("health")
    }

    async fn connect(&self) -> Result<(), BackendError> {
        no_graph("connect")
    }

    async fn close(&self) -> Result<(), BackendError> {
        Ok(())
    }
}
