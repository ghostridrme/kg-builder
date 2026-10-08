//! Pure schema handoffs; external definitions are loaded by run admission.
use kg_core::{
    errors::StageError,
    models::SnapshotInput,
    runtime::{
        schemas::{self, ObservationSchemas, RunSchemaManifest},
        RuntimeContext,
    },
};

pub(crate) fn observation(
    ctx: &RuntimeContext,
    input: &SnapshotInput,
    attached: Option<ObservationSchemas>,
) -> Result<ObservationSchemas, StageError> {
    let invalid = |message| StageError::StateValidation {
        stage: "schema_preparation".into(),
        message,
    };
    if let Some(attached) = attached {
        return Ok(attached);
    }
    if let Some(manifest) = &ctx.run_schemas {
        return manifest.observation(&ctx.org_id, input).map_err(invalid);
    }
    if ctx.ontology_store.is_some() {
        return Err(invalid(
            "configured ontology requires run schema admission before extraction".into(),
        ));
    }
    // Direct stage calls without external configuration use only supplied definitions.
    let manifest = RunSchemaManifest {
        profiles: Default::default(),
        org_id: ctx.org_id.to_string(),
        sources: schemas::sources(std::slice::from_ref(input))
            .into_iter()
            .map(|source| (source, Default::default()))
            .collect(),
    };
    manifest.validate(&ctx.org_id).map_err(invalid)?;
    manifest.observation(&ctx.org_id, input).map_err(invalid)
}
