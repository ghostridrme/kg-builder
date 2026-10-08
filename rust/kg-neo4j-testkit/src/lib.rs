//! Live-database support for the workspace test suites.
//!
//! One environment vocabulary (`NEO4J_TEST_URI`, `NEO4J_TEST_USER`,
//! `NEO4J_TEST_PASSWORD`), set by the `test:live:*`, `eval:*`, `load:*` and
//! `paid:*` Taskfile targets against a disposable container. Tests are
//! ignored by default; once selected, a missing prerequisite is a failure
//! naming the variable, never a silent skip.
//!
//! Whole-database operations (`wipe`) additionally require
//! `KG_EXCLUSIVE_DB=1`, which only targets that own their container set.

pub mod env;
mod fixture;
mod live;

pub use env::{EnvError, Neo4jTestEnv};
pub use fixture::Fixture;
pub use live::{connect, indexed_graph, LiveGraph, TestkitError};
