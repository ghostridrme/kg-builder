//! Shared backend and pipeline interfaces; consumers own their backend handles.

pub mod decision_backend;
pub mod embed_backend;
pub mod graph_backend;
pub mod graph_commit;
pub mod graph_explorer;
pub mod graph_mutation;
pub mod graph_reads;
pub mod llm_backend;
pub mod ontology_store;
pub mod property_codec;
pub mod rerank_backend;
pub mod rule_store;
pub mod schema_store;
pub mod search_backend;
pub mod stage;
pub mod unresolved_reference;

pub use decision_backend::{Answer, Decided, DecisionBackend, DecisionLimits, Question};
pub use embed_backend::{EmbedBackend, EmbedDisabled};
pub use graph_backend::GraphBackend;
pub use graph_commit::{
    BatchIdentity, BatchKind, CommittedBatch, MutationBatch, PlannedBatch, Precondition,
    RequestFingerprint, RunHeader, RunRegistration,
};
pub use graph_mutation::{GraphMutation, GraphProperties};
pub use graph_reads::{
    EdgeLookup, EdgeRecord, EntityLookup, EntityVersionRecord, ReferenceCandidates, VersionState,
    WantedKeyValue,
};
pub use llm_backend::{complete_as, LlmBackend, LlmDisabled, LlmMessage, LlmResponse, MessageRole};
pub use ontology_store::{InMemoryOntologyStore, Ontology, OntologyStore};
pub use rerank_backend::{RankCandidate, RankScore, RerankBackend};
pub use rule_store::{
    LearnedRule, RuleDecision, RuleOrigin, RuleStatus, RuleStore, RuleTransition, RuleValidation,
};
pub use schema_store::{InferredSchema, SchemaStore};
pub use search_backend::SearchBackend;
pub use stage::{Stage, StageCapability, StageContract, StageKind};
pub use unresolved_reference::{
    ReferenceRuleSourcePage, ReferenceRuleSourceQuery, UnresolvedReferenceCursor,
    UnresolvedReferenceEntry, UnresolvedReferencePage, UnresolvedReferenceQuery,
    UnresolvedReferenceRecord,
};

#[cfg(test)]
mod tests;

pub mod identity_candidates;
pub use identity_candidates::{
    IdentityCandidate, IdentityCandidatePage, IdentityCandidateQuery, IdentityCandidateRequest,
};
pub mod identity_revision;
pub use identity_revision::{IdentityRevision, IdentityScope};

pub mod relationship_timeline;

pub mod profile_registry;
pub use profile_registry::{InMemoryProfileRegistry, ProfileRegistry};

pub mod connector_checkpoint;

pub mod commit_pages;
