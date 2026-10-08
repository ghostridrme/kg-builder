//! Staging diagnostics name the guard or conflict without leaking error text.
use super::*;

#[test]
fn staging_diagnostics_identify_guard_or_conflict_without_error_text() {
    assert_eq!(
        StageError::Precondition(3, CallError::Reference).diagnostic(),
        ("precondition", Some(3), "not_found")
    );
    assert_eq!(
        StageError::Mutation(CallError::IdentityConflict).diagnostic(),
        ("mutation", None, "identity_conflict")
    );
    assert_eq!(
        StageError::Mutation(CallError::MetadataConflict).diagnostic(),
        ("mutation", None, "metadata_conflict")
    );
    let failure = StageError::Receipt(CallError::Permanent("private source data".into()));
    assert_eq!(failure.diagnostic(), ("receipt", None, "deserialization"));
}
