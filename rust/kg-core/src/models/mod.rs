//! Graph records, connector input, and collection ownership contracts.

pub mod attribute_schema;
pub use attribute_schema::AttributeSchema;
pub mod collection;
pub mod edges;
pub mod input;
pub mod nodes;
pub mod property_value;
pub mod relationship_time;
pub use relationship_time::{
    RelationshipTarget, RelationshipTimeBound, RelationshipTimeEvidence, RelationshipTimeOutcome,
    TimeBasis, TimePrecision,
};

pub use collection::{
    CollectionMembership, CollectionRef, CollectionScope, GENERATIONS_PROPERTY, MEMBERS_PROPERTY,
};
pub use edges::{
    CancellationContext, CommunityEdge, EntityEdge, HasSnapshotEdge, NextSnapshotEdge,
    RelationshipOrigin, SnapshotEdge,
};
pub use input::{
    validate_collection_scopes, validate_request, ConnectorEntity, EdgeTypeMapEntry,
    EdgeTypeSchema, EntityTypeSchema, InputValidationError, InputValidationReason, PropertySchema,
    RelationshipChange, RelationshipEndpoint, RelationshipObservation, RelationshipVersionRef,
    SnapshotDataType, SnapshotInput, SnapshotKind, ValidatedSnapshotInput,
};
pub use nodes::{CommunityNode, EntityNode, SnapshotNode, ThreadNode};
pub use property_value::PropertyValue;

/// Validate classification names without rewriting caller values.
pub(crate) fn validate_metadata(
    tags: &indexmap::IndexMap<String, String>,
    labels: &[String],
) -> Result<(), String> {
    if tags.keys().any(|key| key.trim().is_empty())
        || labels.iter().any(|label| label.trim().is_empty())
    {
        return Err("metadata keys and labels must not be blank".into());
    }
    Ok(())
}

fn validate_group_node(
    uuid: uuid::Uuid,
    org: &str,
    namespace: &str,
    name: &str,
    labels: &[String],
) -> Result<(), String> {
    if uuid.is_nil()
        || [org, namespace, name]
            .iter()
            .any(|value| value.trim().is_empty())
        || labels.iter().any(|label| label.trim().is_empty())
    {
        return Err(
            "group nodes require a non-nil identifier and nonblank scope, name and labels".into(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests;

pub use input::{ExistingSnapshotInput, IngestionInput, SagaAssociation, ThreadAssociation};

/// Legacy Rust name; serialized node fields are unchanged.
pub type SagaNode = ThreadNode;
