use crate::config::ApplicationConfig;
use kg_core::traits::ontology_store::{InMemoryOntologyStore, Ontology, OntologyStore};
use kg_stages::{Backends, Engine};
use kg_storage_neo4j::{Neo4jGraphBackend, Neo4jSettings};
use std::sync::Arc;

pub async fn build(
    configuration: ApplicationConfig,
    ontology: Option<Ontology>,
    org: Option<String>,
) -> Result<(Engine, Arc<Neo4jGraphBackend>), String> {
    let ontology_store = Arc::new(InMemoryOntologyStore::new());
    if let (Some(ontology), Some(org)) = (ontology, org) {
        ontology_store
            .put(&org, None, ontology)
            .await
            .map_err(|_| "provider or ontology initialization failed".to_owned())?;
    }
    let llm = Arc::new(kg_core::traits::LlmDisabled);
    let embedder = Arc::new(kg_core::traits::EmbedDisabled);
    let settings = Neo4jSettings::from_config(&configuration.graph)
        .map_err(|_| "provider or ontology initialization failed".to_owned())?;
    let graph = Neo4jGraphBackend::connect(&settings).await.map_err(|_| {
        "Neo4j connection failed; verify address, credentials and availability".to_owned()
    })?;
    graph.prepare().await.map_err(|_| {
        "Neo4j schema preparation failed; verify schema compatibility and permissions".to_owned()
    })?;
    let graph = Arc::new(graph);

    let rule_store: &dyn kg_core::traits::rule_store::RuleStore = graph.as_ref();
    rule_store
        .install_schema()
        .await
        .map_err(|_| "learned-rule schema installation failed".to_owned())?;
    let engine = Engine::new(
        Backends {
            graph: graph.clone(),
            llm_extraction: llm.clone(),
            llm_disambiguation: None,
            llm_edge_discovery: None,
            llm_default: llm,
            decisions: None,
            embedder: embedder.clone(),
            ontology_store: Some(ontology_store),
            schema_store: None,
        },
        configuration.processing,
    )
    .map_err(|_| "engine rejected provider/settings combination".to_owned())?;

    Ok((engine.with_profile_registry(graph.clone()), graph))
}
