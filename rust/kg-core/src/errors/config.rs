use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("missing required field: {0}")]
    MissingField(String),

    #[error("invalid value for '{field}': {message}")]
    InvalidValue { field: String, message: String },

    #[error("failed to parse config: {0}")]
    Parse(String),

    #[error("environment variable '{0}' not set")]
    EnvVar(String),

    #[error("{0}")]
    Other(String),
}
