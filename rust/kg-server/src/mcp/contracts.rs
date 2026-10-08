use super::*;
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct SagaListArgs {
    pub namespace: Option<String>,
    pub as_of: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct SagaArgs {
    /// Exact summary range; offsets count Unicode scalars. Default 0, limit 300 (maximum 2000).
    pub summary_start: Option<usize>,
    pub summary_limit: Option<usize>,
    /// Supporting evidence page. Default offset 0, limit 5 (maximum 20).
    pub evidence_offset: Option<usize>,
    pub evidence_limit: Option<usize>,
    /// Repeat the returned revision for continuation; mandatory for nonzero offsets.
    pub expected_revision: Option<String>,

    /// Stable Thread UUID. Supply exactly one of `thread_uuid` and `name`.
    #[serde(rename = "thread_uuid", alias = "saga_uuid")]
    pub saga_uuid: Option<String>,
    /// Thread name, resolved inside the namespace.
    pub name: Option<String>,
    pub namespace: Option<String>,
    pub as_of: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct SagaSnapshotSearchArgs {
    pub query: String,
    /// Stable Thread UUID. Supply exactly one of `thread_uuid` and `name`.
    #[serde(rename = "thread_uuid", alias = "saga_uuid")]
    pub saga_uuid: Option<String>,
    /// Thread name, resolved inside the namespace.
    pub name: Option<String>,
    pub namespace: Option<String>,
    pub as_of: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct SummarizeSagaArgs {
    /// Stable Thread UUID. Supply exactly one of `thread_uuid` and `name`.
    #[serde(rename = "thread_uuid", alias = "saga_uuid")]
    pub saga_uuid: Option<String>,
    /// Thread name, resolved inside the namespace.
    pub name: Option<String>,
    pub namespace: Option<String>,
    /// Replay identity from an earlier interrupted call; omit to start a new run.
    pub run_id: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct SagaMemberArgs {
    #[serde(rename = "thread_uuid", alias = "saga_uuid")]
    pub saga_uuid: String,
    pub namespace: Option<String>,
    pub as_of: Option<String>,
    /// Return members after this ordinal; use `next_after_ordinal` from the previous page.
    pub after_ordinal: Option<u64>,
    pub limit: Option<usize>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct CatalogArgs {
    pub namespace: Option<String>,
    pub as_of: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct SearchArgs {
    pub query: String,
    pub namespace: Option<String>,
    pub as_of: Option<String>,
    pub limit: Option<usize>,
    pub semantic: Option<bool>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct EntityArgs {
    /// Default false: overview only. Set true for bounded properties, or use fields to request only needed keys.
    pub include_properties: Option<bool>,
    pub include_evidence: Option<bool>,
    pub fields: Option<Vec<String>>,
    pub property_path: Option<String>,
    pub value_start: Option<usize>,
    /// Characters of the exact JSON property value: 1..2000, default 1000.
    #[schemars(range(min = 1, max = 2000))]
    pub value_limit: Option<usize>,
    pub entity_type: String,
    pub chain_id: String,
    pub namespace: Option<String>,
    pub as_of: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct PageArgs {
    pub from: Option<String>,
    pub to: Option<String>,
    pub newest_first: Option<bool>,
    pub entity_type: String,
    pub chain_id: String,
    pub namespace: Option<String>,
    pub as_of: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct NeighborArgs {
    pub entity_type: String,
    pub chain_id: String,
    pub namespace: Option<String>,
    pub as_of: Option<String>,
    pub direction: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct CypherArgs {
    pub query: String,
    pub params: Option<Map<String, Value>>,
    pub offset: Option<usize>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct CommunityArgs {
    pub uuid: String,
    pub namespace: Option<String>,
    pub as_of: Option<String>,
    /// Exact summary range; offsets count Unicode scalars. Default 0, limit 300 (maximum 2000).
    pub summary_start: Option<usize>,
    pub summary_limit: Option<usize>,
    /// Supporting evidence page. Default offset 0, limit 5 (maximum 20).
    pub evidence_offset: Option<usize>,
    pub evidence_limit: Option<usize>,
    /// Repeat the returned revision for continuation; mandatory for nonzero offsets.
    pub expected_revision: Option<String>,
}
