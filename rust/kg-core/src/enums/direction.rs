use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

/// Sub-entity edge direction relative to its parent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display, EnumString)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum EdgeDirection {
    Outgoing,
    Incoming,
    Both,
}
