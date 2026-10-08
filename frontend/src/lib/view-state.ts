import type { Edge, Node, Viewport } from "@xyflow/react";
import type { Entity, Neighbor } from "./api";

export type DetailView = {
  tab: "properties" | "history" | "dependencies";
  offset: number;
  scrollTop: number;
  sections: Record<string, boolean>;
  records?: Record<string, { offset: number; trail: number[]; revision: string; open: string[] }>;
};
export function validDetailView(d: unknown): d is DetailView {
  return (
    record(d) &&
    ["properties", "history", "dependencies"].includes(String(d.tab)) &&
    Number.isInteger(d.offset) &&
    Number(d.offset) >= 0 &&
    Number(d.offset) <= 100000 &&
    finite(d.scrollTop) &&
    d.scrollTop >= 0 &&
    record(d.sections) &&
    Object.keys(d.sections).length <= 500 &&
    Object.values(d.sections).every((x) => typeof x === "boolean") &&
    (d.records === undefined ||
      (record(d.records) &&
        Object.keys(d.records).length <= 50 &&
        Object.values(d.records).every(
          (r) =>
            record(r) &&
            Number.isInteger(r.offset) &&
            Number(r.offset) >= 0 &&
            Number(r.offset) <= 10000000 &&
            Array.isArray(r.trail) &&
            r.trail.length <= 200 &&
            r.trail.every((n) => Number.isInteger(n) && n >= 0 && n <= 10000000) &&
            typeof r.revision === "string" &&
            r.revision.length <= 128 &&
            Array.isArray(r.open) &&
            r.open.length <= 200 &&
            r.open.every((p) => typeof p === "string" && p.length <= 2048),
        )))
  );
}
export const defaultDetailView: DetailView = {
  tab: "properties",
  offset: 0,
  scrollTop: 0,
  sections: {},
};
export type SavedView = {
  version: 1;
  nodeLimit?: number;
  focusTrail?: Entity[][];
  orgId: string;
  namespace: string;
  namespaceView?: boolean;
  includeSnapshots?: boolean;
  asOf: string;
  asOfInput: string;
  query: string;
  semantic: boolean;
  direction: "both" | "in" | "out";
  depth: number;
  /** Entity types the search and expansions are limited to; empty means every type. */
  types?: string[];
  labels: boolean;
  nodes: Node[];
  edges: Edge[];
  entities: Entity[];
  workspaceEntities?: Entity[];
  relations: Neighbor[];
  pages: [string, number | null][];
  workspacePages?: [string, number | null][];
  workspacePositions?: [string, { x: number; y: number }][];
  workspaceRoots?: string[];
  selected: string | null;
  selectedEdge: string | null;
  edgeDetailsOpen?: boolean;
  viewport: Viewport;
  detailViews: Record<string, DetailView>;
  message: string;
  /** Thread browser state; kept until Reset or a namespace change. */
  threadView?: ThreadView;
};
export type ThreadView = {
  open: boolean;
  selected: string | null;
  /** Keyword typed into the selected Thread's observation search. */
  query: string;
  /** Member page cursors visited (after_ordinal values); the last one is current. */
  cursors: number[];
  listOffset?: number;
  listScope?: string;
};
export const defaultThreadView: ThreadView = {
  open: false,
  selected: null,
  query: "",
  cursors: [0],
  listOffset: 0,
  listScope: "",
};
export const viewKey = (orgId: string) => `kg.graph-view.v1:${encodeURIComponent(orgId)}`;
const MAX_CHARS = 4_000_000;
const record = (v: unknown): v is Record<string, unknown> =>
  !!v && typeof v === "object" && !Array.isArray(v);
const text = (v: unknown): v is string => typeof v === "string" && v.length <= 2000;
const finite = (v: unknown): v is number => typeof v === "number" && Number.isFinite(v);
const entity = (v: unknown): v is Entity =>
  record(v) &&
  [v.chain_id, v.name, v.entity_type, v.namespace].every(text) &&
  (v.node_kind === undefined ||
    (v.node_kind === "snapshot" &&
      text(v.snapshot_uuid) &&
      v.chain_id === `snapshot:${v.snapshot_uuid}`));

// Browser data is untrusted and may belong to an older deployment.
export function decodeView(raw: string, orgId: string): SavedView | null {
  try {
    if (raw.length > MAX_CHARS) return null;
    const v = JSON.parse(raw);
    if (!record(v) || v.version !== 1 || v.orgId !== orgId) return null;
    // Retain existing saved selection, paging and search across the public rename.
    if (v.threadView === undefined && v.sagaView !== undefined) v.threadView = v.sagaView;
    delete v.sagaView;
    // Discard retired renderer settings while preserving the saved graph and filters.
    delete v.graphMode;
    delete v.scene3d;
    if (
      v.focusTrail !== undefined &&
      (!Array.isArray(v.focusTrail) ||
        v.focusTrail.length > 20 ||
        !v.focusTrail.every(
          (step: unknown) => Array.isArray(step) && step.length <= 10 && step.every(entity),
        ))
    )
      return null;
    if (v.nodeLimit !== undefined && ![200, 500, 1000, 2000].includes(v.nodeLimit as number))
      return null;
    if (v.includeSnapshots !== undefined && typeof v.includeSnapshots !== "boolean") return null;
    if (v.namespaceView !== undefined && typeof v.namespaceView !== "boolean") return null;
    if (
      ![v.namespace, v.asOf, v.asOfInput, v.query, v.message].every(text) ||
      ![v.semantic, v.labels].every((x) => typeof x === "boolean")
    )
      return null;
    if (
      !["both", "in", "out"].includes(String(v.direction)) ||
      !finite(v.depth) ||
      ![1, 2, 3].includes(v.depth)
    )
      return null;
    if (v.asOf && (typeof v.asOf !== "string" || Number.isNaN(Date.parse(v.asOf)))) return null;
    if (
      v.types !== undefined &&
      (!Array.isArray(v.types) ||
        v.types.length > 32 ||
        !v.types.every((t: unknown) => text(t) && (t as string).trim() !== "") ||
        new Set(v.types).size !== v.types.length)
    )
      return null;
    if (
      !record(v.viewport) ||
      ![v.viewport.x, v.viewport.y, v.viewport.zoom].every(finite) ||
      Number(v.viewport.zoom) < 0.08 ||
      Number(v.viewport.zoom) > 2
    )
      return null;
    if (!Array.isArray(v.entities) || v.entities.length > 2200 || !v.entities.every(entity))
      return null;
    const ids = new Set(v.entities.map((e) => e.chain_id));
    if (
      v.workspaceEntities !== undefined &&
      (!Array.isArray(v.workspaceEntities) ||
        v.workspaceEntities.length > 2200 ||
        !v.workspaceEntities.every(entity) ||
        new Set(v.workspaceEntities.map((e: Entity) => e.chain_id)).size !==
          v.workspaceEntities.length)
    )
      return null;
    const workspaceIds = new Set(
      (v.workspaceEntities ?? v.entities).map((e: Entity) => e.chain_id),
    );
    if ([...ids].some((id) => !workspaceIds.has(id))) return null;
    if (
      v.workspaceRoots !== undefined &&
      (!Array.isArray(v.workspaceRoots) ||
        v.workspaceRoots.length > 2200 ||
        !v.workspaceRoots.every((id: unknown) => typeof id === "string" && workspaceIds.has(id)))
    )
      return null;
    if (
      v.workspacePages !== undefined &&
      (!Array.isArray(v.workspacePages) ||
        v.workspacePages.length > 2200 ||
        !v.workspacePages.every(
          (p: unknown) =>
            Array.isArray(p) &&
            p.length === 2 &&
            workspaceIds.has(p[0]) &&
            (p[1] === null || (Number.isInteger(p[1]) && p[1] >= 0 && p[1] <= 100000)),
        ))
    )
      return null;
    if (
      v.workspacePositions !== undefined &&
      (!Array.isArray(v.workspacePositions) ||
        v.workspacePositions.length > 2200 ||
        !v.workspacePositions.every(
          (p: unknown) =>
            Array.isArray(p) &&
            p.length === 2 &&
            workspaceIds.has(p[0]) &&
            record(p[1]) &&
            finite(p[1].x) &&
            finite(p[1].y),
        ))
    )
      return null;
    const hasId = (id: unknown) => typeof id === "string" && ids.has(id);
    if (ids.size !== v.entities.length || !Array.isArray(v.nodes) || v.nodes.length !== ids.size)
      return null;
    if (
      !v.nodes.every(
        (n) =>
          record(n) &&
          hasId(n.id) &&
          record(n.position) &&
          finite(n.position.x) &&
          finite(n.position.y) &&
          record(n.data) &&
          text(n.data.label) &&
          text(n.data.type) &&
          n.type === "entity" &&
          (n.data.namespace === undefined || text(n.data.namespace)) &&
          (n.data.dependents === undefined || finite(n.data.dependents)) &&
          [n.data.expanded, n.data.isSeed].every((x) => x === undefined || typeof x === "boolean"),
      )
    )
      return null;
    if (new Set(v.nodes.map((n) => n.id)).size !== ids.size) return null;
    if (
      !Array.isArray(v.relations) ||
      v.relations.length > 20000 ||
      !v.relations.every(
        (r) =>
          record(r) &&
          text(r.edge_id) &&
          text(r.via) &&
          entity(r.entity) &&
          hasId(r.src_chain) &&
          hasId(r.dst_chain) &&
          record(r.relationship),
      )
    )
      return null;
    const edgeIds = new Set(v.relations.map((r) => r.edge_id));
    if (
      !Array.isArray(v.edges) ||
      v.edges.length !== edgeIds.size ||
      !v.edges.every(
        (e) =>
          record(e) &&
          edgeIds.has(e.id) &&
          hasId(e.source) &&
          hasId(e.target) &&
          record(e.data) &&
          e.type === "relationship" &&
          text(e.data.name) &&
          [e.data.offset, e.data.loopIndex].every((x) => x === undefined || finite(x)) &&
          (e.data.hideLabel === undefined || typeof e.data.hideLabel === "boolean"),
      )
    )
      return null;
    if (
      !Array.isArray(v.pages) ||
      v.pages.length > 2200 ||
      !v.pages.every(
        (p) =>
          Array.isArray(p) &&
          p.length === 2 &&
          ids.has(p[0]) &&
          (p[1] === null || (Number.isInteger(p[1]) && p[1] >= 0 && p[1] <= 100000)),
      )
    )
      return null;
    if (v.selected !== null && !hasId(v.selected)) return null;
    if (v.edgeDetailsOpen !== undefined && typeof v.edgeDetailsOpen !== "boolean") return null;
    if (v.selectedEdge !== null && !edgeIds.has(v.selectedEdge)) return null;
    // Roll back the removed details tab without discarding the saved graph.
    if (record(v.detailViews))
      for (const detail of Object.values(v.detailViews)) {
        if (record(detail) && detail.tab === "system") detail.tab = "properties";
      }
    if (
      !record(v.detailViews) ||
      Object.keys(v.detailViews).length > 20200 ||
      !Object.values(v.detailViews).every((d) => validDetailView(d))
    )
      return null;
    if (
      v.threadView !== undefined &&
      (!record(v.threadView) ||
        typeof v.threadView.open !== "boolean" ||
        (v.threadView.listOffset !== undefined &&
          (!Number.isInteger(v.threadView.listOffset) ||
            Number(v.threadView.listOffset) < 0 ||
            Number(v.threadView.listOffset) > 100000)) ||
        (v.threadView.listScope !== undefined && !text(v.threadView.listScope)) ||
        !(v.threadView.selected === null || text(v.threadView.selected)) ||
        !text(v.threadView.query) ||
        !Array.isArray(v.threadView.cursors) ||
        v.threadView.cursors.length === 0 ||
        v.threadView.cursors.length > 1000 ||
        v.threadView.cursors[0] !== 0 ||
        !v.threadView.cursors.every(
          (c: unknown) => Number.isInteger(c) && Number(c) >= 0 && Number(c) <= 100000,
        ))
    )
      return null;
    return v as SavedView;
  } catch {
    return null;
  }
}
export function readView(orgId: string): SavedView | null {
  try {
    const raw = localStorage.getItem(viewKey(orgId));
    return raw ? decodeView(raw, orgId) : null;
  } catch {
    return null;
  }
}
export function writeView(view: SavedView) {
  let raw = JSON.stringify(view);
  if (raw.length > MAX_CHARS) {
    // The relationship payload is the bulk of a large view and is refetched when
    // the view is restored, so drop it rather than refusing to save. Keep the
    // selected relationship and its edge record so the user's selection survives
    // the refresh (decode validates it against the retained entities and would
    // discard a selection with no matching edge). If the selection has no
    // matching record, clear it so the whole view is not rejected on decode.
    const keep =
      view.selectedEdge !== null &&
      view.relations.some((r) => r.edge_id === view.selectedEdge) &&
      view.edges.some((e) => e.id === view.selectedEdge);
    raw = JSON.stringify({
      ...view,
      relations: keep ? view.relations.filter((r) => r.edge_id === view.selectedEdge) : [],
      edges: keep ? view.edges.filter((e) => e.id === view.selectedEdge) : [],
      selectedEdge: keep ? view.selectedEdge : null,
    });
  }
  if (raw.length > MAX_CHARS)
    throw new Error("Saved graph is too large to store even without its relationships");
  localStorage.setItem(viewKey(view.orgId), raw);
}
