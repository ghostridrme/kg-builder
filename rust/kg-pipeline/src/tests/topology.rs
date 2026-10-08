use std::sync::Arc;

use async_trait::async_trait;
use kg_core::errors::StageError;
use kg_core::runtime::{RuntimeContext, StageOutput};
use kg_core::traits::{Stage, StageContract, StageKind};

use crate::PipelineRunner;

struct ContractStage(&'static str, StageContract);

#[async_trait]
impl Stage for ContractStage {
    fn name(&self) -> &str {
        self.0
    }
    fn contract(&self) -> StageContract {
        self.1
    }
    async fn process(&self, _: StageOutput, _: &RuntimeContext) -> Result<StageOutput, StageError> {
        panic!("topology validation must not execute stages")
    }
}

fn stage(name: &'static str, contract: StageContract) -> Arc<dyn Stage> {
    Arc::new(ContractStage(name, contract))
}

fn valid() -> PipelineRunner {
    use StageKind::*;
    PipelineRunner::new()
        .node_stage(stage("extract", &[(Input, NodeExtraction)]))
        .resolution_stage(stage("resolve", &[(NodeExtraction, NodeIdentity)]))
        .resolution_stage(stage("version", &[(NodeIdentity, NodeResolution)]))
        .flush(stage("persist", &[(FlushBatch, Committed)]))
}

#[test]
fn entity_only_and_relationship_recipes_have_complete_boundaries() {
    use StageKind::*;
    valid().validate_topology().unwrap();
    valid()
        .edge_stage(stage("relations", &[(NodeResolution, EdgeExtraction)]))
        .relationship_resolution_stage(stage(
            "match_relations",
            &[(EdgeExtraction, EdgeResolution)],
        ))
        .validate_topology()
        .unwrap();
    PipelineRunner::new()
        .node_stage(stage("combined", &[(Input, NodeResolution)]))
        .flush(stage("persist", &[(FlushBatch, Committed)]))
        .validate_topology()
        .unwrap();
}

#[test]
fn missing_or_reordered_dependencies_fail_before_execution() {
    let mut runner = valid();
    runner.resolution_stages.swap(0, 1);
    assert!(runner
        .validate_topology()
        .unwrap_err()
        .to_string()
        .contains("does not accept"));
    let mut runner = valid();
    runner.resolution_stages.pop();
    assert!(runner.validate_topology().is_err());
    let mut runner = valid();
    runner.flush_stages.clear();
    assert!(runner.validate_topology().is_err());
    let mut runner = valid();
    runner.relationship_resolution_stages.push(stage(
        "orphan",
        &[(StageKind::EdgeExtraction, StageKind::EdgeResolution)],
    ));
    assert!(runner.validate_topology().is_err());
}

#[test]
fn every_possible_output_must_be_accepted_and_names_are_unique() {
    use StageKind::*;
    let mut runner = valid();
    runner.node_stages[0] = stage("branch", &[(Input, NodeExtraction), (Input, Empty)]);
    assert!(runner.validate_topology().is_err());
    let mut runner = valid();
    runner.resolution_stages[0] = stage("extract", &[(NodeExtraction, NodeIdentity)]);
    assert!(runner
        .validate_topology()
        .unwrap_err()
        .to_string()
        .contains("unique"));
    let mut runner = valid();
    runner.flush_stages = vec![stage("persist", &[(FlushBatch, Empty)])];
    assert!(runner.validate_topology().is_err());
    let mut runner = valid();
    runner.node_stages[0] = stage("extract", &[]);
    assert!(runner.validate_topology().is_err());
}

struct RelationshipStage(ContractStage, kg_core::traits::StageCapability);
#[async_trait]
impl Stage for RelationshipStage {
    fn name(&self) -> &str {
        self.0.name()
    }
    fn contract(&self) -> StageContract {
        self.0.contract()
    }
    fn capabilities(&self) -> &'static [kg_core::traits::StageCapability] {
        use kg_core::traits::StageCapability::*;
        match self.1 {
            DeclaredRelationships => &[DeclaredRelationships],
            ReferenceExtraction => &[ReferenceExtraction],
            ReferenceResolution => &[ReferenceResolution],
            TextRelationships => &[TextRelationships],
            ThreadAssociation => &[ThreadAssociation],
        }
    }
    async fn process(
        &self,
        input: StageOutput,
        ctx: &RuntimeContext,
    ) -> Result<StageOutput, StageError> {
        self.0.process(input, ctx).await
    }
}

fn full_relationships() -> PipelineRunner {
    use kg_core::traits::StageCapability::*;
    use StageKind::*;
    let mut runner = valid().relationship_coverage(true);
    for (name, role, contract) in [
        (
            "declared",
            DeclaredRelationships,
            &[(NodeResolution, EdgeExtraction)][..],
        ),
        (
            "references",
            ReferenceExtraction,
            &[(EdgeExtraction, EdgeExtraction)][..],
        ),
        (
            "reference_targets",
            ReferenceResolution,
            &[(EdgeExtraction, EdgeExtraction)][..],
        ),
        (
            "text",
            TextRelationships,
            &[(EdgeExtraction, EdgeExtraction)][..],
        ),
    ] {
        runner.edge_stages.push(Arc::new(RelationshipStage(
            ContractStage(name, contract),
            role,
        )));
    }
    runner.relationship_resolution_stage(stage(
        "relationship_resolution",
        &[(EdgeExtraction, EdgeResolution)],
    ))
}

#[test]
fn coverage_requires_every_relationship_capability() {
    for included in 0u8..16 {
        let mut runner = full_relationships();
        runner.edge_stages = runner
            .edge_stages
            .into_iter()
            .enumerate()
            .filter_map(|(index, stage)| (included & (1 << index) != 0).then_some(stage))
            .collect();
        assert_eq!(
            runner.validate_topology().is_ok(),
            included == 15,
            "complete relationship coverage requires all four roles; subset {included:04b}"
        );
    }
}

#[test]
fn references_must_be_resolved_even_without_reconciliation_coverage() {
    let mut runner = full_relationships().relationship_coverage(false);
    runner.edge_stages.remove(2);
    assert!(runner.validate_topology().is_err());
    let mut runner = full_relationships().relationship_coverage(false);
    runner.edge_stages.swap(1, 2);
    assert!(runner.validate_topology().is_err());
}

#[test]
fn differently_named_stages_cannot_duplicate_relationship_ownership() {
    use kg_core::traits::StageCapability;
    use StageKind::*;
    let mut runner = full_relationships();
    runner.edge_stages.push(Arc::new(RelationshipStage(
        ContractStage("second_text_owner", &[(EdgeExtraction, EdgeExtraction)]),
        StageCapability::TextRelationships,
    )));
    assert!(runner
        .validate_topology()
        .unwrap_err()
        .to_string()
        .contains("multiple stages own"));
}

#[test]
fn batch_stages_append_and_validate_every_handoff() {
    use StageKind::*;
    let mut runner = valid();
    runner.flush_stages.clear();
    let runner = runner
        .flush(stage("plan", &[(FlushBatch, PlannedBatch)]))
        .flush(stage("embed", &[(PlannedBatch, PreparedBatch)]))
        .flush(stage("persist", &[(PreparedBatch, Committed)]));
    assert_eq!(runner.flush_stages.len(), 3);
    runner.validate_topology().unwrap();
    let mut reordered = runner;
    reordered.flush_stages.swap(0, 1);
    assert!(reordered.validate_topology().is_err());
    reordered.flush_stages.swap(0, 1);
    reordered.flush_stages.remove(1);
    assert!(reordered.validate_topology().is_err());
}

#[test]
fn summary_followup_has_its_own_complete_typed_boundary() {
    use StageKind::*;
    let runner = valid()
        .summary_stage(stage("entity_summary", &[(SummaryBatch, PlannedBatch)]))
        .summary_stage(stage("batch_embedding", &[(PlannedBatch, PreparedBatch)]))
        .summary_stage(stage("persist", &[(PreparedBatch, Committed)]));
    runner.validate_topology().unwrap();
    let mut invalid = runner;
    invalid.summary_stages.remove(1);
    assert!(invalid.validate_topology().is_err());
}

#[test]
fn saga_followup_requires_its_own_complete_typed_chain() {
    use StageKind::*;
    let mut runner = valid()
        .saga_summary_stage(stage("saga_summary", &[(SagaSummaryBatch, PlannedBatch)]))
        .saga_summary_stage(stage("batch_embedding", &[(PlannedBatch, PreparedBatch)]))
        .saga_summary_stage(stage("persist", &[(PreparedBatch, Committed)]));
    runner.validate_topology().unwrap();
    runner.saga_summary_stages.remove(1);
    assert!(runner.validate_topology().is_err());
}
