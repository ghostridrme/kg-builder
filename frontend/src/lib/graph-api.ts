import { api, type Entity, type Neighbor, type Scope } from "./api";
export const MAX_WORKSPACE = 2000;
export type GraphHandle = Pick<Entity, "entity_type" | "chain_id">;
export type GraphRequest = {
  namespace_view?: boolean;
  include_snapshots?: boolean;
  seeds: GraphHandle[];
  handles?: GraphHandle[];
  expanded?: GraphHandle[];
  namespace: string | null;
  as_of: string | null;
  direction: "both" | "in" | "out";
  entity_types: string[];
  depth: number;
  node_limit: number;
  continuation?: string;
};
export type GraphPage = {
  snapshots?: {
    uuid: string;
    name: string;
    source: string;
    namespace: string;
    captured_at: string;
  }[];
  observations?: {
    uuid: string;
    snapshot_uuid: string;
    entity_uuid: string;
    chain_id: string;
    entity_version: number;
    observed_at: string;
  }[];
  nodes: Entity[];
  relationships: Neighbor[];
  effective_as_of: string;
  not_visible: string[];
  has_more: boolean;
  continuation: string | null;
  truncated: boolean;
  limits: string[];
  storage_reads: number;
};
export const graphHandle = ({ entity_type, chain_id }: Entity): GraphHandle => ({
  entity_type,
  chain_id,
});
export function graphScope(scope: Scope) {
  return { namespace: scope.namespace || null, as_of: scope.as_of || null };
}
/** Consume bounded server result pages; graph discovery/ordering lives entirely in Rust. */
export async function loadGraph(
  operation: "view" | "expand",
  request: GraphRequest,
  signal: AbortSignal,
  onProgress?: (page: GraphPage) => void,
): Promise<GraphPage> {
  let page = await api<GraphPage>(`graph/${operation}`, signal, request);
  const first = page;
  const edges = new Map(page.relationships.map((edge) => [edge.edge_id, edge]));
  const seen = new Set<string>();
  let lastProgress = 0;
  const publish = () => {
    if (!signal.aborted && onProgress) {
      onProgress({ ...first, relationships: [...edges.values()] });
      lastProgress = performance.now();
    }
  };
  publish();
  while (page.has_more) {
    if (!page.continuation || seen.has(page.continuation) || seen.size >= 40)
      throw new Error("Invalid graph continuation; refresh the view.");
    seen.add(page.continuation);
    page = await api<GraphPage>(`graph/${operation}`, signal, {
      ...request,
      continuation: page.continuation,
    });
    if (page.effective_as_of !== first.effective_as_of)
      throw new Error("Graph changed during paging; refresh the view.");
    for (const edge of page.relationships) edges.set(edge.edge_id, edge);
    if (performance.now() - lastProgress > 500 && page.has_more) publish();
  }
  // Snapshot canvas IDs are UI identities, never entity chains sent to the engine.
  const snapshots: Entity[] = (first.snapshots ?? []).map((s) => ({
    chain_id: `snapshot:${s.uuid}`,
    snapshot_uuid: s.uuid,
    node_kind: "snapshot",
    uuid: s.uuid,
    entity_type: "Source snapshot",
    name: `${s.name} · ${s.captured_at}`,
    namespace: s.namespace,
  }));
  const entities = new Map(first.nodes.map((n) => [n.chain_id, n]));
  for (const observation of first.observations ?? []) {
    const target = entities.get(observation.chain_id);
    if (!target) continue;
    edges.set(observation.uuid, {
      entity: target,
      edge_id: observation.uuid,
      src_chain: `snapshot:${observation.snapshot_uuid}`,
      dst_chain: observation.chain_id,
      via: "MENTIONS",
      relationship: { metadata: { ...observation, origin: "observation" }, properties: {} },
    });
  }
  return {
    ...first,
    nodes: [...first.nodes, ...snapshots],
    relationships: [...edges.values()],
    has_more: false,
    continuation: null,
  };
}
