//! Inputs for investigation tools. Scope always comes from authenticated identity.
use super::*;

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct ScopeArgs {
    pub namespace: Option<String>,
}
#[derive(Deserialize, serde::Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct UnifiedSearchArgs {
    /// Include execution timings and all diagnostic steps. Default false; warnings and dropped counts remain visible.
    pub include_diagnostics: Option<bool>,
    /// Continue the same ranked result; repeat other arguments unchanged.
    pub continuation: Option<String>,
    pub query: String,
    pub namespace: Option<String>,
    pub as_of: Option<String>,
    /// keyword (default), hybrid, semantic or diverse. Vector presets require configured embeddings.
    pub preset: Option<String>,
    /// nodes, relationships, snapshots, communities. Defaults to nodes and relationships.
    pub scopes: Option<Vec<String>>,
    /// Total results per page across all scopes: 1..10, default 5.
    #[schemars(range(min = 1, max = 10))]
    pub limit: Option<usize>,
    pub include_evidence: Option<bool>,
    pub entity_types: Option<Vec<String>>,
}
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct RecordArgs {
    /// Include bounded relationship properties. Default false; fields also selects properties.
    pub include_properties: Option<bool>,
    /// Include source text / relationship description. Default false; an explicit content range also enables it.
    pub include_content: Option<bool>,
    /// Exact relationship properties; ignored for source snapshots.
    pub fields: Option<Vec<String>>,
    pub property_path: Option<String>,
    /// Relationship or snapshot UUID, depending on the tool.
    pub uuid: String,
    pub namespace: Option<String>,
    pub as_of: Option<String>,
    /// Zero-based Unicode scalar offset for source content; default zero.
    pub content_start: Option<usize>,
    /// Maximum source characters (1..2000; relationship description max 1000), default 300.
    pub content_limit: Option<usize>,
}
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct AgentHandle {
    pub entity_type: String,
    pub chain_id: String,
}
impl AgentHandle {
    pub fn handle(self) -> Result<crate::graph::Handle, CallToolResult> {
        Ok(crate::graph::Handle {
            entity_type: self.entity_type,
            chain_id: parse_chain(&self.chain_id)?,
        })
    }
}
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct SubgraphArgs {
    /// Records per response, 1..20, default 5. Does not change exploration bounds.
    #[schemars(range(min = 1, max = 20))]
    pub page_size: Option<usize>,
    pub roots: Vec<AgentHandle>,
    pub namespace: Option<String>,
    pub as_of: Option<String>,
    /// in, out, both (default).
    #[schemars(extend("enum" = ["in", "out", "both", null]))]
    pub direction: Option<String>,
    /// 1..3, default 1.
    pub depth: Option<usize>,
    /// 1..200, default 50.
    pub node_limit: Option<usize>,
    /// 1..1000, default 250. Overflow is explicit.
    pub edge_limit: Option<usize>,
    pub entity_types: Option<Vec<String>>,
    /// Exact relationship names. An empty list includes all relationships.
    pub relationship_names: Option<Vec<String>>,
    /// Opaque continuation from this tool. Repeat all other arguments unchanged.
    pub continuation: Option<String>,
}
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct ChangeArgs {
    pub namespace: Option<String>,
    /// RFC3339, exclusive beginning of the effective-time interval.
    pub from: String,
    /// RFC3339, inclusive end; at most 366 days after from.
    pub to: String,
    /// Optional stable entity chain UUIDs, maximum 32.
    pub chains: Option<Vec<String>>,
    pub event_kinds: Option<Vec<String>>,
    /// Events per page: 1..50, default 5. Use offset for further pages.
    #[schemars(range(min = 1, max = 50))]
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct PathArgs {
    pub source: AgentHandle,
    pub target: AgentHandle,
    pub namespace: Option<String>,
    pub as_of: Option<String>,
    /// Traversal direction: in, out, or both (default).
    #[schemars(extend("enum" = ["in", "out", "both", null]))]
    pub direction: Option<String>,
    pub relationship_names: Option<Vec<String>>,
    /// 1..3, default 3.
    pub max_hops: Option<usize>,
    /// 1..10, default 5.
    pub max_paths: Option<usize>,
}

/// Unranked inventory enumeration, including resources with no relationships.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct InventoryArgs {
    pub namespace: Option<String>,
    /// Required fixed RFC3339 timestamp; repeat it on every page.
    pub as_of: String,
    pub entity_types: Option<Vec<String>>,
    /// 1..20 records; default 5.
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}
