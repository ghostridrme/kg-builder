//! Awaited ingestion with bounded concurrent stages and receipted graph commits.
//!
//! Node batches commit before relationship discovery. Complete source scans
//! then reconcile missing members. Retrying the same run id replays receipts
//! and finishes remaining work; earlier commits survive a later failure.

#![warn(missing_docs)]

/// The message envelope flowing between stage workers.
pub mod message;
/// One pipeline phase: N stage workers wired by bounded channels.
pub mod phase;
/// The two-pass runner: chunking, receipted commits, and reconciliation.
pub mod runner;
/// A single stage worker: bounded-concurrency processing with failure
/// attribution.
pub mod worker;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

pub use message::PipelineMessage;
pub use runner::{PipelineRunner, PipelineRunnerConfig, SETTINGS_VERSION};

mod schema_admission;
pub use schema_admission::resolve_profile_manifest;

mod observation_admission;
