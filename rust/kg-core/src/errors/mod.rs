//! Backend, configuration, pipeline, stage, and validation errors.
//! Messages must omit secrets and sensitive payloads. Retry hints do not establish replay safety.

pub mod backend;
pub mod config;
pub mod pipeline;
pub mod stage;
pub mod validation;

pub use backend::BackendError;
pub use config::ConfigError;
pub use pipeline::PipelineError;
pub use stage::StageError;
pub use validation::ValidationError;
