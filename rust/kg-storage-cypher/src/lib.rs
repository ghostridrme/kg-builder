//! Parameterized openCypher statements shared by graph adapters.
//!
//! Only fixed schema names and provider expressions enter query text. Caller
//! input stays in bound parameters. Core, stages, and search depend on typed
//! contracts, never on these database-specific statements.
pub mod community;
pub mod community_revision;
mod decode;
pub mod entity_summary;
pub mod explorer;
mod filters;
pub mod saga;
pub use decode::{
    decode_attached_relationships, decode_attached_snapshots, decode_node, decode_relationships,
    decode_snapshots,
};
pub use neo4j::evidence::{
    attached_relationships, attached_snapshots, relationship_similarity, relationships, snapshots,
    RELATIONSHIP_TEXT_FIELDS, SNAPSHOT_TEXT_FIELDS,
};
pub use neo4j::nodes::{nodes, ENTITY_TEXT_FIELDS};

/// Prepared statement executed as one auto-commit call, with caller values
/// bound separately. Reads and the run-header upsert use this form.
pub struct PreparedQuery {
    /// Query text owned by this crate.
    pub statement: String,
    /// Named parameters.
    pub parameters: serde_json::Value,
}

/// Prepared write. Execute the whole batch in one transaction and require
/// exactly `expected_rows` rows of `ok = true` from every statement.
#[derive(Debug, Clone)]
pub struct PreparedWrite {
    pub statement: String,
    pub parameters: serde_json::Value,
    pub expected_rows: usize,
}

mod chain_transitions;
mod identity;
pub use identity::READY as IDENTITY_INDEX_READY;
pub use identity::VALUES_READY as IDENTITY_VALUES_INDEX_READY;
mod mutations;
pub use mutations::{mutations, mutations_ungrouped};

mod reads;
pub use reads::{
    bolt_generation, decode_edge, decode_entity_version, edges, entities, membership_properties,
    memberships, snapshot_evidence,
};

mod receipts;
pub use receipts::{
    check_run, collection_owner, create_receipt, decode_receipt, decode_run_header, precondition,
    read_receipt, read_run, register_run, run_receipts, StoredReceipt, StoredRunHeader,
};

mod excerpt;
pub use excerpt::{source_excerpt, SourceExcerpt, SOURCE_SCAN_CHARS};

mod readiness;
pub use readiness::{decode_embedding_readiness, embedding_readiness};

mod embeddings;
pub use embeddings::{
    decode_entity_embedding, decode_entity_embedding_hit, entity_embedding_record,
    get_entity_embedding, search_entity_embeddings,
};
pub mod neo4j;
pub use neo4j::vector::{
    indexed_nodes, indexed_relationships, vector_population, vector_stats, VectorStats,
    MAX_VECTOR_CANDIDATES,
};

pub use neo4j::schema::COLLECTION_SCAN_CONSTRAINT;

pub mod embedding_maintenance;

pub mod identity_candidates;
pub mod identity_revision;
pub mod reference_decision;
pub mod reference_dependency;
pub mod rule_evidence;
pub mod rule_store;
pub mod unresolved_reference;

mod summary_search;
pub use summary_search::{summary_nodes, summary_population, summary_readiness};

pub mod community_search;

pub mod profile_registry;

pub mod connector_checkpoint;

pub mod commit_pages;
