//! Entity normalization, versioning, and child-extraction rules.

pub mod config;
pub mod sub_entity;

pub use config::EntityTypeConfig;
pub use sub_entity::{PromotedList, SubEntityRule};
