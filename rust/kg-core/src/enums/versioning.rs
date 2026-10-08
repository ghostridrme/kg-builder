use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertyChange {
    pub property: String,
    /// Previous serialized property value; `None` means the key was absent.
    pub old_value: Option<serde_json::Value>,
    /// New serialized property value; `None` means the key was removed.
    pub new_value: Option<serde_json::Value>,
}

/// Resolution outcome using hashes, type rules, and snapshot kind.
/// Incremental snapshots preserve omitted properties; persistence applies the matching change.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum VersioningDecision {
    /// Retain the current version and record the observation.
    Unchanged,
    /// Patch the current version without creating a successor.
    MergeInPlace { changed: Vec<PropertyChange> },
    /// Supersede the stored version with a new version.
    NewVersion { changed: Vec<PropertyChange> },
}
