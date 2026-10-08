use thiserror::Error;

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("entity missing required field: {0}")]
    MissingField(String),

    /// `primary_key_properties` is empty; no identity hash can be computed.
    #[error("entity has empty primary_key_properties")]
    EmptyPrimaryKeys,

    /// A declared PK property has no value in `all_properties`.
    #[error("primary key property '{0}' not found in all_properties")]
    PrimaryKeyNotInProperties(String),

    #[error("self-loop edge: source and target are the same ({0})")]
    SelfLoop(String),

    #[error("property key too long ({len} > {max}): {key}")]
    PropertyKeyTooLong { key: String, len: usize, max: usize },

    #[error("{0}")]
    Other(String),
}
