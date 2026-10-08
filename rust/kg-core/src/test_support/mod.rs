//! Test doubles for the provider and configuration traits, plus the paid
//! gate. Compiled for this crate's own tests and for consumers that enable
//! the `test-support` feature (dev-dependencies only). There is no test graph
//! here: live suites run against a disposable Neo4j through
//! `kg-neo4j-testkit`.

mod decisions;
mod embed;
#[cfg(feature = "test-support")]
pub mod env;
mod graph;
mod llm;
mod metered;
mod rule_store;
mod schema_store;

pub use decisions::MockDecisionBackend;
pub use embed::MockEmbedBackend;
pub use graph::UnreachableGraph;
pub use llm::MockLlmBackend;
pub use metered::MeteredLlm;
pub use rule_store::InMemoryRuleStore;
pub use schema_store::InMemorySchemaStore;

/// Decode a JSON evidence packet in a recorded model request. Test providers
/// inspect the public wire representation independently of the prompt writer.
pub fn source_data_json(prompt: &str) -> serde_json::Value {
    let start = prompt
        .find("{\"source_data\":")
        .expect("source data envelope");
    let envelope = serde_json::Deserializer::from_str(&prompt[start..])
        .into_iter::<serde_json::Value>()
        .next()
        .expect("source data envelope")
        .expect("valid JSON envelope");
    serde_json::from_str(envelope["source_data"].as_str().expect("source text"))
        .expect("JSON evidence packet")
}
