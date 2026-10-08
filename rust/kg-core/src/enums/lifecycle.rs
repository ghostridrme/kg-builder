use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

/// Source state: `Deleting`/`Deleted` route to deletion; `Archived` resolves normally.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize, Display, EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum EntityLifecycle {
    #[default]
    Active,
    Deleting,
    Deleted,
    Archived,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_serde_round_trip() {
        let v = EntityLifecycle::Active;
        let json = serde_json::to_string(&v).unwrap();
        assert_eq!(json, "\"active\"");
        let back: EntityLifecycle = serde_json::from_str(&json).unwrap();
        assert_eq!(v, back);
    }
}
