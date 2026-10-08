//! Bounded graph operations shared by HTTP and other service callers.
//! Continuations page an immutable, short-lived read result, not live offsets.
pub mod presentation;
use crate::query::{GraphQueryService, QueryError};
use chrono::{DateTime, Utc};
use kg_core::{
    errors::BackendError,
    traits::graph_explorer::{ExplorerDirection, ExplorerQuery, ExplorerRequest},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Mutex,
    time::{Duration, Instant},
};
use uuid::Uuid;

const MAX_HANDLES: usize = 2000;
const MAX_WORK: usize = 24000;
const MAX_EDGES: usize = 12000;
const MAX_BYTES: usize = 2 * 1024 * 1024;
const PAGE_EDGES: usize = 500;
const TTL: Duration = Duration::from_secs(120);

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct Handle {
    pub entity_type: String,
    pub chain_id: Uuid,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphRequest {
    #[serde(default)]
    pub seeds: Vec<Handle>,
    #[serde(default)]
    pub namespace_view: bool,
    #[serde(default)]
    pub include_snapshots: bool,
    #[serde(default)]
    pub handles: Vec<Handle>,
    #[serde(default)]
    pub expanded: Vec<Handle>,
    pub namespace: Option<String>,
    pub as_of: Option<DateTime<Utc>>,
    #[serde(default)]
    pub direction: ExplorerDirection,
    #[serde(default)]
    pub entity_types: Vec<String>,
    #[serde(default)]
    pub relationship_names: Vec<String>,
    #[serde(default = "default_depth")]
    pub depth: usize,
    #[serde(default = "default_nodes")]
    pub node_limit: usize,
    #[serde(default = "default_edges")]
    pub edge_limit: usize,
    pub continuation: Option<String>,
}
fn default_depth() -> usize {
    1
}
fn default_edges() -> usize {
    6000
}
fn default_nodes() -> usize {
    200
}
#[derive(Clone, Serialize)]
pub struct GraphPage {
    pub snapshots: Vec<Value>,
    pub observations: Vec<Value>,
    pub nodes: Vec<Value>,
    pub relationships: Vec<Value>,
    pub effective_as_of: DateTime<Utc>,
    pub not_visible: Vec<Uuid>,
    pub has_more: bool,
    pub continuation: Option<String>,
    pub truncated: bool,
    pub limits: Vec<String>,
    pub storage_reads: usize,
}
struct Snapshot {
    org: String,
    key: String,
    created: Instant,
    accessed: Instant,
    completed: bool,
    result: GraphPage,
    _slot: Option<tokio::sync::OwnedSemaphorePermit>,
}
pub struct GraphCache(
    Mutex<HashMap<Uuid, Snapshot>>,
    std::sync::Arc<tokio::sync::Semaphore>,
);
impl Default for GraphCache {
    fn default() -> Self {
        Self(
            Mutex::new(HashMap::new()),
            std::sync::Arc::new(tokio::sync::Semaphore::new(32)),
        )
    }
}
impl GraphCache {
    fn reserve(&self, org: &str) -> Result<tokio::sync::OwnedSemaphorePermit, QueryError> {
        let mut cache = self.0.lock().map_err(|_| QueryError::Busy)?;
        cache.retain(|_, s| s.created.elapsed() < TTL);
        if self.1.available_permits() == 0 {
            let victim = cache
                .iter()
                .filter(|(_, s)| s.completed)
                .min_by_key(|(_, s)| (s.org != org, s.accessed))
                .map(|(id, _)| *id);
            if let Some(id) = victim {
                cache.remove(&id);
            }
        }
        self.1
            .clone()
            .try_acquire_owned()
            .map_err(|_| QueryError::Busy)
    }
}

impl GraphRequest {
    fn validate(&mut self, org: &str) -> Result<(), QueryError> {
        if self.seeds.len() + self.handles.len() + self.expanded.len() > MAX_HANDLES * 3
            || (self.seeds.is_empty() && !self.namespace_view)
            || (self.namespace_view && self.namespace.is_none())
            || !(1..=3).contains(&self.depth)
            || !(1..=MAX_EDGES).contains(&self.edge_limit)
            || !(1..=MAX_HANDLES).contains(&self.node_limit)
            || self.continuation.as_ref().is_some_and(|c| c.len() > 80)
        {
            return Err(QueryError::Invalid);
        }
        ExplorerRequest {
            org_id: org.into(),
            namespace: self.namespace.clone(),
            as_of: self.as_of,
            limit: 1,
            offset: 0,
            query: ExplorerQuery::Entities {
                entity_types: self.entity_types.clone(),
            },
        }
        .validate()
        .map_err(|_| QueryError::Invalid)?;
        for handles in [&mut self.seeds, &mut self.handles, &mut self.expanded] {
            handles.sort();
            handles.dedup();
        }
        if self
            .seeds
            .iter()
            .chain(&self.handles)
            .chain(&self.expanded)
            .map(|h| h.chain_id)
            .collect::<BTreeSet<_>>()
            .len()
            > MAX_HANDLES
        {
            return Err(QueryError::Invalid);
        }
        if self.relationship_names.len() > 32
            || self
                .relationship_names
                .iter()
                .any(|s| s.is_empty() || s.len() > 256 || s.chars().any(char::is_control))
        {
            return Err(QueryError::Invalid);
        }
        let mut types_by_chain = BTreeMap::new();
        for h in self.seeds.iter().chain(&self.handles).chain(&self.expanded) {
            if types_by_chain
                .insert(h.chain_id, &h.entity_type)
                .is_some_and(|previous| previous != &h.entity_type)
            {
                return Err(QueryError::Invalid);
            }
        }
        self.entity_types.sort();
        self.entity_types.dedup();
        for handle in self.seeds.iter().chain(&self.handles).chain(&self.expanded) {
            self.read(org, handle, true, 0)
                .validate()
                .map_err(|_| QueryError::Invalid)?;
        }
        Ok(())
    }
    fn read(&self, org: &str, h: &Handle, neighbors: bool, offset: usize) -> ExplorerRequest {
        ExplorerRequest {
            org_id: org.into(),
            namespace: self.namespace.clone(),
            as_of: self.as_of,
            limit: 60,
            offset,
            query: if neighbors {
                ExplorerQuery::CanvasNeighbors {
                    entity_type: h.entity_type.clone(),
                    chain_id: h.chain_id,
                    direction: self.direction,
                    entity_types: self.entity_types.clone(),
                }
            } else {
                ExplorerQuery::CanvasEntity {
                    entity_type: h.entity_type.clone(),
                    chain_id: h.chain_id,
                }
            },
        }
    }
    fn key(&self, view: bool) -> String {
        let mut request = self.clone();
        request.continuation = None;
        format!(
            "{view}:{}",
            serde_json::to_string(&request).expect("graph request serializes")
        )
    }
}
fn id(value: &Value, field: &str) -> Result<Uuid, QueryError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| BackendError::Deserialization("invalid graph identity".into()).into())
}
fn handle(value: &Value) -> Result<Handle, QueryError> {
    Ok(Handle {
        chain_id: id(value, "chain_id")?,
        entity_type: value["entity_type"]
            .as_str()
            .ok_or(QueryError::Invalid)?
            .into(),
    })
}
fn page(result: &GraphPage, token: Uuid, offset: usize) -> Result<GraphPage, QueryError> {
    if offset > result.relationships.len() || !offset.is_multiple_of(PAGE_EDGES) {
        return Err(QueryError::Invalid);
    }
    let end = (offset + PAGE_EDGES).min(result.relationships.len());
    let has_more = end < result.relationships.len();
    Ok(GraphPage {
        snapshots: if offset == 0 {
            result.snapshots.clone()
        } else {
            vec![]
        },
        observations: if offset == 0 {
            result.observations.clone()
        } else {
            vec![]
        },
        nodes: if offset == 0 {
            result.nodes.clone()
        } else {
            vec![]
        },
        relationships: result.relationships[offset..end].to_vec(),
        effective_as_of: result.effective_as_of,
        not_visible: if offset == 0 {
            result.not_visible.clone()
        } else {
            vec![]
        },
        has_more,
        continuation: has_more.then(|| format!("{token}:{end}")),
        truncated: result.truncated,
        limits: result.limits.clone(),
        storage_reads: result.storage_reads,
    })
}
impl GraphQueryService {
    pub async fn graph_view(
        &self,
        org: &str,
        mut request: GraphRequest,
        view: bool,
    ) -> Result<GraphPage, QueryError> {
        request.validate(org)?;
        let key = request.key(view);
        if let Some(cursor) = &request.continuation {
            let (token, offset) = cursor.split_once(':').ok_or(QueryError::Invalid)?;
            let token: Uuid = token.parse().map_err(|_| QueryError::Invalid)?;
            let offset = offset.parse().map_err(|_| QueryError::Invalid)?;
            let mut cache = self.graph_cache.0.lock().map_err(|_| QueryError::Busy)?;
            let saved = cache
                .get_mut(&token)
                .filter(|v| v.org == org && v.key == key && v.created.elapsed() < TTL)
                .ok_or(QueryError::RestartRequired)?;
            let response = page(&saved.result, token, offset)?;
            saved.accessed = Instant::now();
            saved.completed |= !response.has_more;
            return Ok(response);
        }
        let cache_slot = self.graph_cache.reserve(org)?;
        let _permit = self.admit_read()?;
        let started = Instant::now();
        request.as_of = Some(request.as_of.unwrap_or_else(Utc::now));
        let result = tokio::time::timeout(Duration::from_secs(30), async {
            let mut result = self.collect_requested_graph(org, &request, view).await?;
            if request.include_snapshots {
                self.attach_source_snapshots(org, &request, &mut result)
                    .await?;
            }
            Ok::<_, QueryError>(result)
        })
        .await
        .map_err(|_| BackendError::Timeout(30_000))??;
        _permit.complete().await;
        tracing::info!(
            nodes = result.nodes.len(),
            edges = result.relationships.len(),
            storage_reads = result.storage_reads,
            truncated = result.truncated,
            duration_ms = started.elapsed().as_millis() as u64,
            "graph view read"
        );
        if serde_json::to_vec(&result)
            .map_err(|_| QueryError::Invalid)?
            .len()
            > MAX_BYTES
        {
            return Err(QueryError::ViewLimit);
        }
        let token = Uuid::new_v4();
        let first = page(&result, token, 0)?;
        if first.has_more {
            let mut cache = self.graph_cache.0.lock().map_err(|_| QueryError::Busy)?;
            cache.retain(|_, v| v.created.elapsed() < TTL);
            cache.insert(
                token,
                Snapshot {
                    org: org.into(),
                    key,
                    created: Instant::now(),
                    accessed: Instant::now(),
                    completed: false,
                    result,
                    _slot: Some(cache_slot),
                },
            );
        }
        Ok(first)
    }
    async fn attach_source_snapshots(
        &self,
        org: &str,
        request: &GraphRequest,
        result: &mut GraphPage,
    ) -> Result<(), QueryError> {
        let versions = result
            .nodes
            .iter()
            .map(|n| id(n, "uuid"))
            .collect::<Result<Vec<_>, _>>()?;
        if versions.is_empty() {
            return Ok(());
        }
        // Evidence has its own budget: enabling it never evicts resources.
        let budget = 200;
        let mut seen = BTreeSet::new();
        let mut bytes = serde_json::to_vec(result)
            .map_err(|_| QueryError::Invalid)?
            .len();
        let mut offset = 0;
        'pages: loop {
            let rows = self
                .graph
                .explore(&ExplorerRequest {
                    org_id: org.into(),
                    namespace: request.namespace.clone(),
                    as_of: request.as_of,
                    limit: 200,
                    offset,
                    query: ExplorerQuery::SnapshotObservations {
                        versions: versions.clone(),
                    },
                })
                .await?;
            result.storage_reads += 1;
            for row in rows.items {
                let snapshot = &row["snapshot"];
                let uuid = id(snapshot, "uuid")?;
                let fresh = !seen.contains(&uuid);
                let cost = serde_json::to_vec(&row)
                    .map_err(|_| QueryError::Invalid)?
                    .len();
                let limit = if fresh && seen.len() >= budget {
                    Some("snapshot_node_limit")
                } else if result.observations.len() >= 2000 {
                    Some("snapshot_edge_limit")
                } else if bytes + cost > MAX_BYTES - 64 * 1024 {
                    Some("snapshot_byte_limit")
                } else {
                    None
                };
                if let Some(limit) = limit {
                    result.truncated = true;
                    result.limits.push(limit.into());
                    break 'pages;
                }
                bytes += cost;
                if fresh {
                    seen.insert(uuid);
                    result.snapshots.push(snapshot.clone());
                }
                result.observations.push(row["observation"].clone());
            }
            if !rows.truncated {
                break;
            }
            offset += 200;
        }
        Ok(())
    }
    async fn collect_requested_graph(
        &self,
        org: &str,
        request: &GraphRequest,
        view: bool,
    ) -> Result<GraphPage, QueryError> {
        if !request.namespace_view {
            return self.collect_graph(org, request, view).await;
        }
        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        let mut offset = 0;
        let mut reads = 0;
        let mut limits = Vec::new();
        loop {
            let rows = self
                .graph
                .explore(&ExplorerRequest {
                    org_id: org.into(),
                    namespace: request.namespace.clone(),
                    as_of: request.as_of,
                    limit: 200,
                    offset,
                    query: ExplorerQuery::Entities {
                        entity_types: request.entity_types.clone(),
                    },
                })
                .await?;
            reads += 1;
            for entity in rows.items {
                if nodes.len() == request.node_limit {
                    limits.push("node_limit".into());
                    break;
                }
                nodes.push(entity);
            }
            if !limits.is_empty() || !rows.truncated {
                break;
            }
            if nodes.len() == request.node_limit {
                limits.push("node_limit".into());
                break;
            }
            offset += 200;
        }
        let chains = nodes
            .iter()
            .map(|n| id(n, "chain_id"))
            .collect::<Result<Vec<_>, _>>()?;
        let mut bytes = serde_json::to_vec(&nodes)
            .map_err(|_| QueryError::Invalid)?
            .len();
        offset = 0;
        // Namespace overview is a set read, not one neighbor query per resource.
        // Both endpoints are restricted to the visible, authorized entity set.
        if !chains.is_empty() {
            'pages: loop {
                let rows = self
                    .graph
                    .explore(&ExplorerRequest {
                        org_id: org.into(),
                        namespace: request.namespace.clone(),
                        as_of: request.as_of,
                        limit: 200,
                        offset,
                        query: ExplorerQuery::NamespaceRelationships {
                            chains: chains.clone(),
                        },
                    })
                    .await?;
                reads += 1;
                for mut edge in rows.items {
                    if !request.relationship_names.is_empty()
                        && !request
                            .relationship_names
                            .iter()
                            .any(|name| edge["via"].as_str() == Some(name))
                    {
                        continue;
                    }
                    edge["entity"] = presentation::compact_entity(&edge["entity"]);
                    edge["relationship"] = presentation::typed_record(&edge["relationship"]);
                    let cost = serde_json::to_vec(&edge)
                        .map_err(|_| QueryError::Invalid)?
                        .len();
                    if bytes + cost > MAX_BYTES - 64 * 1024 {
                        limits.push("byte_limit".into());
                        break 'pages;
                    }
                    if edges.len() == request.edge_limit {
                        limits.push("edge_limit".into());
                        break 'pages;
                    }
                    bytes += cost;
                    edges.push(edge);
                }
                if !rows.truncated {
                    break;
                }
                offset += 200;
            }
        }
        Ok(GraphPage {
            snapshots: vec![],
            observations: vec![],
            nodes,
            relationships: edges,
            effective_as_of: request.as_of.unwrap(),
            not_visible: vec![],
            has_more: false,
            continuation: None,
            truncated: !limits.is_empty(),
            limits,
            storage_reads: reads,
        })
    }
    async fn collect_graph(
        &self,
        org: &str,
        r: &GraphRequest,
        view: bool,
    ) -> Result<GraphPage, QueryError> {
        let mut nodes = BTreeMap::new();
        let mut edges = BTreeMap::new();
        let mut limits = BTreeSet::new();
        let mut reads = 0;
        let roots: BTreeSet<_> = r.seeds.iter().map(|h| h.chain_id).collect();
        let mut requested = r.seeds.clone();
        if view {
            requested.extend(r.handles.clone());
            requested.extend(r.expanded.clone());
        }
        requested.sort();
        requested.dedup();
        let mut visible = BTreeSet::new();
        let mut remembered = BTreeMap::new();
        for batch in requested.chunks(32) {
            let requests: Vec<_> = batch.iter().map(|h| r.read(org, h, false, 0)).collect();
            reads += 1;
            for (_, row) in self.graph.explore_batch(&requests).await? {
                for value in row.items {
                    let chain = id(&value, "chain_id")?;
                    visible.insert(chain);
                    if r.entity_types.is_empty()
                        || r.entity_types
                            .iter()
                            .any(|t| value["entity_type"].as_str() == Some(t))
                    {
                        remembered.insert(chain, presentation::compact_entity(&value));
                    }
                    let allowed = roots.contains(&chain)
                        || (view
                            && r.expanded.iter().any(|h| h.chain_id == chain)
                            && (r.entity_types.is_empty()
                                || r.entity_types
                                    .iter()
                                    .any(|t| value["entity_type"].as_str() == Some(t))));
                    if allowed {
                        nodes.insert(chain, presentation::compact_entity(&value));
                    }
                }
            }
        }
        // Roots are never silently evicted by a smaller view budget.
        if nodes.len() > r.node_limit {
            return Err(QueryError::ViewLimit);
        }
        // A deleted/hidden root must not erase still-visible entities the user already
        // explored. With all roots visible, traversal alone controls depth narrowing.
        if view && roots.iter().any(|id| !visible.contains(id)) {
            for (chain, entity) in remembered {
                if nodes.contains_key(&chain) {
                    continue;
                }
                if nodes.len() >= r.node_limit {
                    limits.insert("node_limit".into());
                    break;
                }
                nodes.insert(chain, entity);
            }
        }
        let mut frontier: Vec<_> = r
            .seeds
            .iter()
            .chain(if view {
                r.expanded.iter()
            } else {
                r.seeds.iter()
            })
            .filter(|h| nodes.contains_key(&h.chain_id))
            .cloned()
            .collect();
        frontier.sort();
        frontier.dedup();
        let mut visited = BTreeSet::new();
        let mut work = 0;
        let mut bytes = serde_json::to_vec(&nodes)
            .map_err(|_| QueryError::Invalid)?
            .len();
        'traversal: for _ in 0..r.depth {
            let mut next = BTreeMap::new();
            for batch in frontier.chunks(32) {
                let mut pending: Vec<_> = batch
                    .iter()
                    .filter(|h| visited.insert(h.chain_id))
                    .map(|h| (h.clone(), 0))
                    .collect();
                while !pending.is_empty() {
                    if work >= MAX_WORK {
                        limits.insert("work_limit".to_owned());
                        break;
                    }
                    let requests: Vec<_> = pending
                        .iter()
                        .map(|(h, o)| r.read(org, h, true, *o))
                        .collect();
                    reads += 1;
                    let rows = self.graph.explore_batch(&requests).await?;
                    let mut again = Vec::new();
                    for (anchor, result) in rows {
                        let Some((h, offset)) = pending.iter().find(|(h, _)| h.chain_id == anchor)
                        else {
                            return Err(QueryError::Invalid);
                        };
                        for mut edge in result.items {
                            work += 1;
                            if work > MAX_WORK {
                                limits.insert("work_limit".into());
                                break;
                            }
                            if !r.relationship_names.is_empty()
                                && !r
                                    .relationship_names
                                    .iter()
                                    .any(|name| edge["via"].as_str() == Some(name))
                            {
                                continue;
                            }
                            let entity = presentation::compact_entity(&edge["entity"]);
                            let chain = id(&entity, "chain_id")?;
                            let eid = id(&edge, "edge_id")?;
                            if edges.contains_key(&eid) {
                                continue;
                            }
                            if edges.len() >= r.edge_limit {
                                limits.insert("edge_limit".into());
                                break 'traversal;
                            }
                            if !nodes.contains_key(&chain) && nodes.len() >= r.node_limit {
                                limits.insert("node_limit".into());
                                continue;
                            }
                            edge["entity"] = entity.clone();
                            edge["relationship"] =
                                presentation::typed_record(&edge["relationship"]);
                            let cost = serde_json::to_vec(&edge)
                                .map_err(|_| QueryError::Invalid)?
                                .len()
                                + if nodes.contains_key(&chain) {
                                    0
                                } else {
                                    serde_json::to_vec(&entity)
                                        .map_err(|_| QueryError::Invalid)?
                                        .len()
                                };
                            if bytes + cost > MAX_BYTES - 64 * 1024 {
                                limits.insert("byte_limit".into());
                                continue;
                            }
                            bytes += cost;
                            nodes.entry(chain).or_insert(entity.clone());
                            if !visited.contains(&chain) {
                                next.insert(chain, handle(&entity)?);
                            }
                            edges.insert(eid, edge);
                        }
                        if result.truncated && *offset < 100_000 {
                            again.push((h.clone(), offset + 60));
                        }
                    }
                    if limits.contains("byte_limit") || limits.contains("node_limit") {
                        break;
                    }
                    pending = again;
                }
                if work >= MAX_WORK || !limits.is_empty() {
                    break;
                }
            }
            if !limits.is_empty() {
                break;
            }
            frontier = next.into_values().collect();
            if frontier.is_empty() {
                break;
            }
        }
        Ok(GraphPage {
            snapshots: vec![],
            observations: vec![],
            nodes: nodes.into_values().collect(),
            relationships: edges.into_values().collect(),
            effective_as_of: r.as_of.unwrap(),
            not_visible: requested
                .iter()
                .filter(|h| !visible.contains(&h.chain_id))
                .map(|h| h.chain_id)
                .collect(),
            has_more: false,
            continuation: None,
            truncated: !limits.is_empty(),
            limits: limits.into_iter().collect(),
            storage_reads: reads,
        })
    }
    pub async fn graph_filters(
        &self,
        org: &str,
        namespace: Option<String>,
        as_of: Option<DateTime<Utc>>,
        dimension: &str,
        search: String,
        offset: usize,
    ) -> Result<Value, QueryError> {
        if !["namespace", "entity_type"].contains(&dimension) {
            return Err(QueryError::Invalid);
        }
        let result = self
            .explore(
                org,
                crate::query::QueryScope {
                    namespace,
                    as_of,
                    limit: Some(100),
                    offset: Some(offset),
                },
                ExplorerQuery::Filters {
                    namespace_dimension: dimension == "namespace",
                    search,
                },
            )
            .await?;
        Ok(
            json!({"items":result.items,"has_more":result.truncated,"next_offset":result.truncated.then_some(offset+100),"org_id":org,"semantic_available":self.semantic_available()}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kg_core::{
        search::SearchPage,
        traits::{graph_explorer::GraphExplorerBackend, SearchBackend},
    };
    use kg_search::SearchEngine;
    use std::sync::Arc;
    struct Graph;
    #[async_trait::async_trait]
    impl GraphExplorerBackend for Graph {
        async fn explore(&self, _: &ExplorerRequest) -> Result<SearchPage<Value>, BackendError> {
            unreachable!()
        }
        async fn explore_batch(
            &self,
            requests: &[ExplorerRequest],
        ) -> Result<Vec<(Uuid, SearchPage<Value>)>, BackendError> {
            assert!(requests.len() <= 32);
            Ok(requests.iter().map(|r|{
                assert_eq!(r.org_id,"org"); assert!(r.as_of.is_some());
                let (chain,rows)=match &r.query {
                    ExplorerQuery::CanvasEntity{chain_id,..}=>(*chain_id,vec![entity(*chain_id)]),
                    ExplorerQuery::CanvasNeighbors{chain_id,..}=>{
                        let target=if chain_id.as_u128()==1 {Uuid::from_u128(2)} else {Uuid::from_u128(1)};
                        (*chain_id,vec![json!({"edge_id":Uuid::from_u128(9),"src_chain":Uuid::from_u128(1),"dst_chain":Uuid::from_u128(2),"entity":entity(target),"via":"RELATES_TO","relationship":{"name":"RELATES_TO","embedding":[1,2]}})])
                    },_=>unreachable!(),
                };(chain,SearchPage::bounded(rows,60))
            }).collect())
        }
    }
    struct NoSearch;
    #[async_trait::async_trait]
    impl SearchBackend for NoSearch {}
    fn entity(id: Uuid) -> Value {
        json!({"chain_id":id,"entity_type":"Service","name":"service","namespace":"prod","prop_secret":"not needed by canvas"})
    }
    fn request() -> GraphRequest {
        serde_json::from_value(json!({"seeds":[{"entity_type":"Service","chain_id":Uuid::from_u128(1)}],"namespace":"prod","depth":3})).unwrap()
    }
    #[tokio::test]
    async fn traversal_is_batched_cyclic_bounded_and_compact() {
        let service = GraphQueryService::new(
            Arc::new(Graph),
            Arc::new(SearchEngine::new(Arc::new(NoSearch))),
            false,
        );
        let result = service.graph_view("org", request(), false).await.unwrap();
        assert_eq!(result.nodes.len(), 2);
        assert_eq!(result.relationships.len(), 1);
        assert_eq!(result.storage_reads, 3);
        assert!(result.nodes.iter().all(|n| n.get("prop_secret").is_none()));
        assert!(result.relationships[0]["relationship"]["metadata"]
            .get("embedding")
            .is_none());
        let mut small = request();
        small.node_limit = 1;
        let result = service.graph_view("org", small, false).await.unwrap();
        assert_eq!(result.nodes.len(), 1);
        assert!(result.truncated);
        assert_eq!(result.limits, ["node_limit"]);
    }
    #[tokio::test]
    async fn continuation_is_scoped_immutable_and_rejects_invalid_positions() {
        let service = GraphQueryService::new(
            Arc::new(Graph),
            Arc::new(SearchEngine::new(Arc::new(NoSearch))),
            false,
        );
        let token = Uuid::new_v4();
        let key = request().key(false);
        let result = GraphPage {
            snapshots: vec![],
            observations: vec![],
            nodes: vec![entity(Uuid::from_u128(1))],
            relationships: (0..1001).map(|i| json!({"edge_id":i})).collect(),
            effective_as_of: Utc::now(),
            not_visible: vec![],
            has_more: false,
            continuation: None,
            truncated: false,
            limits: vec![],
            storage_reads: 1,
        };
        service.graph_cache.0.lock().unwrap().insert(
            token,
            Snapshot {
                org: "org".into(),
                key,
                created: Instant::now(),
                accessed: Instant::now(),
                completed: false,
                result,
                _slot: None,
            },
        );
        let mut req = request();
        req.continuation = Some(format!("{token}:500"));
        let next = service.graph_view("org", req.clone(), false).await.unwrap();
        assert_eq!(next.relationships[0]["edge_id"], 500);
        assert!(next.has_more);
        assert!(matches!(
            service.graph_view("other", req.clone(), false).await,
            Err(QueryError::RestartRequired)
        ));
        req.depth = 2;
        assert!(matches!(
            service.graph_view("org", req.clone(), false).await,
            Err(QueryError::RestartRequired)
        ));
        req.depth = 3;
        req.continuation = Some(format!("{token}:501"));
        assert!(matches!(
            service.graph_view("org", req, false).await,
            Err(QueryError::Invalid)
        ));
    }
    #[tokio::test]
    async fn completed_graphs_release_capacity_under_repeated_browsing() {
        let service = GraphQueryService::new(
            Arc::new(Graph),
            Arc::new(SearchEngine::new(Arc::new(NoSearch))),
            false,
        );
        for _ in 0..40 {
            let slot = service.graph_cache.reserve("org").unwrap();
            let token = Uuid::new_v4();
            service.graph_cache.0.lock().unwrap().insert(
                token,
                Snapshot {
                    org: "org".into(),
                    key: request().key(false),
                    created: Instant::now(),
                    accessed: Instant::now(),
                    completed: false,
                    _slot: Some(slot),
                    result: GraphPage {
                        snapshots: vec![],
                        observations: vec![],
                        nodes: vec![],
                        relationships: (0..501).map(|i| json!({"edge_id":i})).collect(),
                        effective_as_of: Utc::now(),
                        not_visible: vec![],
                        has_more: false,
                        continuation: None,
                        truncated: false,
                        limits: vec![],
                        storage_reads: 0,
                    },
                },
            );
            let mut req = request();
            req.continuation = Some(format!("{token}:500"));
            assert!(
                !service
                    .graph_view("org", req.clone(), false)
                    .await
                    .unwrap()
                    .has_more
            );
            assert!(
                !service
                    .graph_view("org", req, false)
                    .await
                    .unwrap()
                    .has_more
            );
            assert!(service.graph_cache.0.lock().unwrap().len() <= 32);
        }
        // Every retained result is complete; reserving another slot must evict one.
        assert!(service.graph_cache.reserve("org").is_ok());
    }
    #[test]
    fn storage_values_are_decoded_only_when_marked() {
        let value = presentation::typed_record(
            &json!({"chain_id":"x","prop_a.b":"{\"n\":2}","property_type_a.b":"j","prop_literal":"[1]","prop_bad":"bad","property_type_bad":"j","prop_null":null,"embedding":[1],"version":2}),
        );
        assert_eq!(value["properties"]["a.b"]["n"], 2);
        assert_eq!(value["properties"]["literal"], "[1]");
        assert_eq!(value["properties"]["bad"], "bad");
        assert!(value["properties"]["null"].is_null());
        assert_eq!(value["diagnostics"].as_array().unwrap().len(), 1);
        assert!(value["metadata"].get("embedding").is_none());
        assert_eq!(value["metadata"]["version"], 2);
    }
}
