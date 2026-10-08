//! Lifecycle, versioning, and edge-direction enums.

pub mod direction;
pub mod lifecycle;
pub mod versioning;

pub use direction::EdgeDirection;
pub use lifecycle::EntityLifecycle;
pub use versioning::{PropertyChange, VersioningDecision};
