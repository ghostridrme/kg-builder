//! Provider-neutral learned-rule domain logic, split by responsibility so each stage is independently
//! testable and none reimplements reference matching.
//!
//! - [`evidence`] collects and aggregates observed reference examples into
//!   candidate mapping patterns over *distinct* examples.
//! - [`proposal`] turns a well-supported pattern into a structurally valid
//!   [`crate::runtime::extraction::ReferenceMapping`]; it proposes, never
//!   activates.
//! - [`validation`] measures a proposal against labeled held-out cases and
//!   produces a [`crate::traits::rule_store::RuleValidation`] for the promotion
//!   promotion gate.
//!
//! Persistence, model calls and graph reads live outside this module (the
//! learning service wires them to these functions), so the decision logic can
//! be exercised without a database or a model.
pub mod admin;
pub mod approximate;
pub mod drift;
pub mod evidence;
pub mod materialize;
pub mod proposal;
pub mod review;
pub mod service;
pub mod transform;
pub mod validation;

pub use evidence::{aggregate, EvidenceAggregate, ExampleOutcome, PatternKey, ReferenceExample};
pub use proposal::{propose, RuleProposal};
pub use review::{review_proposal, ModelReview, ModelVerdict};
pub use service::{
    learn, AdjudicatedEvidenceSource, EvidenceBatch, LearningBounds, LearningOptions,
    LearningReport, NoDerivedRuleState, ReferenceEvidenceSource, RuleLifecycleRepair,
};
pub use validation::{split, validate_against, HeldOutSplit, LabeledCase, ValidationExample};
