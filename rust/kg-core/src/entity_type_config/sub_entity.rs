use serde::{Deserialize, Serialize};

use crate::enums::EdgeDirection;

/// Extract children from a JSON array without removing it from the parent hash.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubEntityRule {
    /// Delete the child only after every live owning parent is deleted.
    #[serde(default)]
    pub owned: bool,
    /// Dotted source path containing a JSON array (e.g. `spec.containers`).
    pub source_path: String,
    pub target_entity_type: String,
    /// Nonempty scalar identity keys, also selected by the property lists below.
    pub pk_properties: Vec<String>,
    /// Dotted paths to copy using the shared source-value types. Null stays null;
    /// arrays and empty objects remain JSON.
    pub flat_properties: Vec<String>,
    /// Subtrees to retain as JSON before flattening, including any subtree that
    /// the child type config forces to JSON.
    pub blob_properties: Vec<String>,
    pub edge_name: String,
    /// Direction of the child edge from the parent: outgoing, incoming, or one each way.
    pub edge_direction: EdgeDirection,
    pub promote_to_parent: Vec<PromotedList>,
    /// Whether to copy the source array to `raw_blob_field`; the source stays intact.
    pub keep_raw_blob: bool,
    /// Destination key for the optional copy; no copy is made when absent.
    pub raw_blob_field: Option<String>,
}

/// Child values promoted as strings in encounter order, retaining duplicates.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromotedList {
    pub source_field: String,
    pub target_field: String,
}
