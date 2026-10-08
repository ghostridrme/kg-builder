//! Entity identity and property change detection.

pub mod identity_hash;
pub mod structural_hash;

pub use identity_hash::IdentityHash;
pub use structural_hash::compute_structural_hash;

pub mod relationship_identity;
pub use relationship_identity::{
    relationship_cardinality_key, relationship_identity_hash, RelationshipIdentityScope,
};
