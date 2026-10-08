"use client";
import { formatUtc as formatAsOf } from "@/lib/utils";
import { usePanelAccessibility } from "@/lib/panel-accessibility";
import { FilterSelect } from "@/components/graph/filter-select";
import { GraphFinder } from "@/components/graph/graph-finder";
import {
  useState,
  useRef,
  useEffect,
  useCallback,
  useMemo,
  useSyncExternalStore,
  type SetStateAction,
} from "react";
import { QueryClient, QueryClientProvider, useQuery } from "@tanstack/react-query";
import {
  ReactFlow,
  ReactFlowProvider,
  Background,
  Controls,
  MarkerType,
  useNodesState,
  useEdgesState,
  useReactFlow,
  getNodesBounds,
  getViewportForBounds,
  type Node,
  type Edge,
  type Viewport,
} from "@xyflow/react";
import { Details, Properties, PanelView } from "@/components/graph/node-details-panel";
import { ThreadPanel } from "@/components/graph/thread-panel";
import { SnapshotDetails } from "@/components/graph/snapshot-details";
import { GRAPH_CARD_WIDTH, GRAPH_CARD_HEIGHT } from "@/components/graph/graph-style";
import { EntityNode } from "@/components/graph/entity-node";
import { CustomEdge } from "@/components/graph/custom-edge";
import { useGraphRefresh } from "@/lib/use-graph-refresh";
import { useCanvasBatches } from "@/lib/use-canvas-batches";
import { radialLayout } from "@/lib/graph-layout";
import { api, params, type Entity, type Neighbor, type Page } from "@/lib/api";
import {
  readView,
  writeView,
  defaultDetailView,
  defaultThreadView,
  type SavedView,
  type DetailView,
  type ThreadView,
} from "@/lib/view-state";
import { RecordDetails } from "@/components/graph/record-details";
import { loadGraph, graphHandle, graphScope, MAX_WORKSPACE } from "@/lib/graph-api";
import { useFilterOptions } from "@/lib/filter-options";
const nodeTypes = { entity: EntityNode };
const edgeTypes = { relationship: CustomEdge };

type Catalog = Page<{
  namespace: string;
  entity_type: string;
  count: number;
}> & { org_id: string; semantic_available: boolean };
function entityHandle({
  chain_id,
  name,
  entity_type,
  namespace,
  node_kind,
  snapshot_uuid,
}: Entity): Entity {
  return { chain_id, name, entity_type, namespace, node_kind, snapshot_uuid };
}
function Explorer({ orgId }: { orgId: string }) {
  const [saved] = useState(() => readView(orgId));
  const [namespace, setNamespace] = useState(saved?.namespace ?? "");
  const [nodeLimit, setNodeLimit] = useState(saved?.nodeLimit ?? 1000);
  const [includeSnapshots, setIncludeSnapshots] = useState(saved?.includeSnapshots ?? false);
  const [namespaceView, setNamespaceView] = useState(
    saved?.namespaceView ?? Boolean(saved?.namespace),
  );
  const [asOfInput, setAsOfInput] = useState(saved?.asOfInput ?? "");
  const [asOf, setAsOf] = useState(saved?.asOf ?? "");
  const scope = useMemo(() => ({ namespace, as_of: asOf }), [namespace, asOf]);
  const [effectiveTime, setEffectiveTime] = useState("");
  const detailScope = useMemo(
    () => ({ namespace, as_of: effectiveTime || asOf }),
    [namespace, effectiveTime, asOf],
  );
  const [filtersOpen, setFiltersOpen] = useState(false);
  const [focusTrail, setFocusTrail] = useState<Entity[][]>(saved?.focusTrail ?? []);
  const [query, setQuery] = useState(saved?.query ?? "");
  const [semantic, setSemantic] = useState(saved?.semantic ?? false);
  const [direction, setDirection] = useState<"both" | "in" | "out">(saved?.direction ?? "both");
  const [depth, setDepth] = useState(saved?.depth ?? 1);
  const [types, setTypes] = useState<string[]>(saved?.types ?? []);
  // The saved-view refresh reads the current filter without re-running on every change.

  const [labels, setLabels] = useState(true);
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>(saved?.nodes ?? []);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>(saved?.edges ?? []);
  const [selected, setSelected] = useState<Entity | null>(
    saved?.entities.find((e) => e.chain_id === saved.selected) ?? null,
  );
  const [selectedEdge, setSelectedEdge] = useState<Neighbor | null>(
    saved?.relations.find((e) => e.edge_id === saved.selectedEdge) ?? null,
  );
  const [edgeDetailsOpen, setEdgeDetailsOpen] = useState(saved?.edgeDetailsOpen ?? true);
  const isObservation =
    (selectedEdge?.relationship?.metadata as Record<string, unknown> | undefined)?.origin ===
    "observation";
  const edgeDetails = useQuery({
    queryKey: ["relationship", selectedEdge?.edge_id, detailScope],
    queryFn: ({ signal }) =>
      api<Record<string, unknown>>(
        `relationships/${selectedEdge!.edge_id}?${params(detailScope, { representation: "overview" })}`,
        signal,
      ),
    enabled: Boolean(selectedEdge) && edgeDetailsOpen && !isObservation,
    staleTime: 30_000,
  });
  const edgeRecord = isObservation ? selectedEdge!.relationship : (edgeDetails.data ?? {});
  const [hasWorkspace, setHasWorkspace] = useState(
    Boolean((saved?.workspaceEntities ?? saved?.entities)?.length),
  );
  const [message, setMessage] = useState(saved?.message ?? "");
  // The Thread browser survives refreshes and time changes; Reset clears it.
  const [threadView, setThreadView] = useState<ThreadView>(saved?.threadView ?? defaultThreadView);
  const showThreads = threadView.open;
  const setShowThreads = (open: boolean) => setThreadView((v) => ({ ...v, open }));
  const [error, setError] = useState("");
  const entities = useRef(new Map<string, Entity>(saved?.entities.map((e) => [e.chain_id, e])));
  const relations = useRef(new Map<string, Neighbor>(saved?.relations.map((r) => [r.edge_id, r])));
  const pages = useRef(new Map<string, number | null>(saved?.pages));
  const workspaceEntities = useRef(
    new Map<string, Entity>(
      (saved?.workspaceEntities ?? saved?.entities ?? []).map((e) => [e.chain_id, e]),
    ),
  );
  const workspacePages = useRef(
    new Map<string, number | null>(saved?.workspacePages ?? saved?.pages),
  );
  const workspaceRoots = useRef(
    new Set<string>(
      saved?.workspaceRoots ??
        saved?.nodes.filter((node) => node.data.isSeed).map((node) => node.id),
    ),
  );
  const workspacePositions = useRef(
    new Map<string, { x: number; y: number }>(
      saved?.workspacePositions ?? saved?.nodes.map((node) => [node.id, node.position]),
    ),
  );
  const entityClickTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  useEffect(
    () => () => clearTimeout(entityClickTimer.current),
    [namespace, asOf, direction, depth, types],
  );
  const refreshController = useRef<AbortController | null>(null);
  const { fitView, setViewport: setFlowViewport } = useReactFlow();
  const canvasElement = useRef<HTMLDivElement>(null);
  const [fitRequest, setFitRequest] = useState(0);
  const fittedRequest = useRef(0);
  const [viewport, setViewport] = useState<Viewport>(saved?.viewport ?? { x: 0, y: 0, zoom: 1 });
  const [detailViews, setDetailViewsRaw] = useState<Record<string, DetailView>>(
    saved?.detailViews ?? {},
  );
  const setDetailViews = useCallback(
    (change: SetStateAction<Record<string, DetailView>>) =>
      setDetailViewsRaw((previous) => {
        const next = typeof change === "function" ? change(previous) : change;
        const entries = Object.entries(next).sort(
          ([a, av], [b, bv]) => Number(av !== previous[a]) - Number(bv !== previous[b]),
        );
        return Object.fromEntries(
          entries.slice(-100).map(([key, value]) => [
            key,
            {
              ...value,
              sections: Object.fromEntries(Object.entries(value.sections).slice(-500)),
            },
          ]),
        );
      }),
    [],
  );
  const [storageError, setStorageError] = useState("");
  const [restoring, setRestoring] = useState(
    Boolean(saved?.namespace || (saved?.workspaceEntities ?? saved?.entities)?.length),
  );
  const [filtering, setFiltering] = useState(false);
  usePanelAccessibility(
    restoring && !filtering
      ? ""
      : showThreads
        ? `threads:${namespace}:${asOf}`
        : selected || (selectedEdge && edgeDetailsOpen)
          ? `${selected?.chain_id ?? selectedEdge?.edge_id}:${namespace}:${asOf}`
          : "",
    () => {
      setShowThreads(false);
      setSelected(null);
      setSelectedEdge(null);
    },
  );

  const [refreshSource, setRefreshSource] = useState<SavedView | null>(saved);
  const [restoreError, setRestoreError] = useState("");
  const [restoreAttempt, setRestoreAttempt] = useState(0);
  const latestView = useRef<SavedView | null>(null);
  const committedView = useRef<{ view: SavedView; at: string } | null>(
    saved ? { view: saved, at: saved.asOf } : null,
  );
  const captureView = useRef<() => void>(() => {});
  const topologyCache = useRef<{ nodes: Node[]; edges: Edge[]; view: SavedView } | null>(null);
  useEffect(() => {
    const capture = () => {
      if (filtering || restoring || restoreError) return;
      const cached =
        topologyCache.current?.nodes === nodes && topologyCache.current?.edges === edges
          ? topologyCache.current.view
          : null;
      for (const node of nodes) workspacePositions.current.set(node.id, node.position);
      const view: SavedView = {
        version: 1,
        orgId,
        namespace,
        namespaceView,
        includeSnapshots,
        nodeLimit,
        focusTrail,
        asOf,
        asOfInput,
        query,
        semantic,
        direction,
        depth,
        types,
        labels,
        nodes:
          cached?.nodes ??
          nodes.map(({ id, position, data, ariaLabel, type }) => ({
            id,
            position,
            data,
            ariaLabel,
            type,
          })),
        edges:
          cached?.edges ??
          edges.map(({ id, source, target, data, type, markerEnd }) => ({
            id,
            source,
            target,
            data: {
              name: data?.name,
              offset: data?.offset,
              loopIndex: data?.loopIndex,
              hideLabel: data?.hideLabel,
            },
            type,
            markerEnd,
          })),
        entities: [...entities.current.values()].map(entityHandle),
        workspaceEntities: [...workspaceEntities.current.values()].map(entityHandle),
        relations:
          cached?.relations ??
          [...relations.current.values()].map((relation) => ({
            ...relation,
            entity: entityHandle(relation.entity),
          })),
        pages: [...pages.current],
        workspacePages: [...workspacePages.current],
        // Only positions of entities still in the workspace: a stale position
        // would make the whole saved view undecodable.
        workspacePositions: [...workspacePositions.current].filter(([id]) =>
          workspaceEntities.current.has(id),
        ),
        workspaceRoots: [...workspaceRoots.current],
        selected: selected && entities.current.has(selected.chain_id) ? selected.chain_id : null,
        selectedEdge: selectedEdge?.edge_id ?? null,
        edgeDetailsOpen,
        viewport,
        detailViews,
        message,
        threadView,
      };
      topologyCache.current = { nodes, edges, view };
      latestView.current = view;
      committedView.current = { view, at: effectiveTime };
      try {
        writeView(view);
        setStorageError("");
      } catch {
        setStorageError(
          "This browser could not save your view. Free browser storage to retain changes across refreshes.",
        );
      }
    };
    captureView.current = capture;
    if (!latestView.current) capture();
    const timer = setTimeout(capture, 250);
    return () => clearTimeout(timer);
  }, [
    orgId,
    namespace,
    namespaceView,
    includeSnapshots,
    nodeLimit,
    focusTrail,
    asOf,
    asOfInput,
    query,
    semantic,
    direction,
    depth,
    types,
    labels,
    nodes,
    edges,
    selected,
    selectedEdge,
    edgeDetailsOpen,
    viewport,
    detailViews,
    message,
    threadView,
    filtering,
    restoring,
    restoreError,
    effectiveTime,
  ]);
  useEffect(() => {
    const save = () => {
      captureView.current();
      if (latestView.current) {
        try {
          writeView(latestView.current);
        } catch {
          /* The visible storage warning is handled during normal writes. */
        }
      }
    };
    const hide = () => {
      if (document.visibilityState === "hidden") save();
    };
    window.addEventListener("pagehide", save);
    document.addEventListener("visibilitychange", hide);
    return () => {
      save();
      window.removeEventListener("pagehide", save);
      document.removeEventListener("visibilitychange", hide);
    };
  }, []);

  const [typeSearch, setTypeSearch] = useState("");
  const [namespaceSearch, setNamespaceSearch] = useState("");
  const namespaceOptions = useFilterOptions(
    "namespace",
    { namespace: "", as_of: asOf },
    namespaceSearch,
  );
  const typeOptions = useFilterOptions("entity_type", scope, typeSearch);
  const catalog = { error: namespaceOptions.error || typeOptions.error };
  const reset = useCallback(() => {
    clearTimeout(entityClickTimer.current);
    setIncludeSnapshots(false);
    setNamespaceView(false);
    refreshController.current?.abort();
    refreshController.current = null;
    entities.current.clear();
    relations.current.clear();
    pages.current.clear();
    workspaceEntities.current.clear();
    workspacePages.current.clear();
    workspacePositions.current.clear();
    workspaceRoots.current.clear();
    setHasWorkspace(false);
    setNodes([]);
    setEdges([]);
    setSelected(null);
    setSelectedEdge(null);
    setRestoring(false);
    setFiltering(false);
    setMessage("");
    setError("");
    setQuery("");
    setFocusTrail([]);
    setEffectiveTime("");
    setNamespace("");
    setAsOf("");
    setAsOfInput("");
    setThreadView(defaultThreadView);
    setSemantic(false);
    setDirection("both");
    setDepth(1);
    setTypes([]);
    setLabels(true);
    setViewport({ x: 0, y: 0, zoom: 1 });
    setDetailViews({});
    if (latestView.current) {
      const cleared = {
        ...latestView.current,
        nodes: [],
        edges: [],
        entities: [],
        workspaceEntities: [],
        relations: [],
        pages: [],
        workspacePages: [],
        workspacePositions: [],
        workspaceRoots: [],
        selected: null,
        selectedEdge: null,
        query: "",
        namespace: "",
        includeSnapshots: false,
        asOf: "",
        asOfInput: "",
        semantic: false,
        direction: "both" as const,
        depth: 1,
        types: [],
        labels: true,
        viewport: { x: 0, y: 0, zoom: 1 },
        detailViews: {},
        message: "",
        threadView: defaultThreadView,
      };
      latestView.current = cleared;
      try {
        writeView(cleared);
      } catch {
        setStorageError("This browser could not reset its saved view.");
      }
    }
  }, [setNodes, setEdges, setDetailViews]);
  function scopeChange(ns: string, time: string, nextDirection = direction) {
    captureView.current();
    if (ns === namespace && time === asOf && nextDirection === direction) return;
    if (ns !== namespace) {
      setFocusTrail([]);
      setThreadView(defaultThreadView);
      workspaceEntities.current.clear();
      workspacePages.current.clear();
      workspaceRoots.current.clear();
      workspacePositions.current.clear();
      setSelected(null);
      setSelectedEdge(null);
    }
    refreshController.current?.abort();
    setError("");
    setRestoreError("");
    if (ns || workspaceEntities.current.size) {
      const current = latestView.current;
      if (current)
        setRefreshSource({
          ...current,
          workspaceEntities: [...workspaceEntities.current.values()].map(entityHandle),
          workspacePages: [...workspacePages.current],
          workspacePositions: [...workspacePositions.current],
          workspaceRoots: [...workspaceRoots.current],
        });
      setFiltering(true);
      setRestoring(true);
    }
    setNamespaceView(Boolean(ns));
    setNamespace(ns);
    setAsOf(time);
    setDirection(nextDirection);
  }
  // Filters are the user's until Reset. A scope may outlive the data it was
  // set on (a reseeded graph, a time before the first observation); the
  // toolbar keeps it and says so instead of silently switching scope.
  const catalogNamespaces = namespaceOptions.options.map((option) => option.value);
  const catalogTypes = [
    ...new Set([...typeOptions.options.map((option) => option.value), ...types]),
  ].sort();
  const scopeNotice =
    namespace && !typeSearch && typeOptions.isSuccess && typeOptions.options.length === 0
      ? `Namespace "${namespace}" has no entities${asOf ? ` as of ${formatAsOf(asOf)}` : ""}. Filters are kept; choose Current or Reset.`
      : "";
  const [draft, setDraft] = useState({
    time: saved?.asOf ?? "",
    direction: saved?.direction ?? "both",
    depth: saved?.depth ?? 1,
    types: saved?.types ?? [],
  });
  function applyFilters(defaults = false, changed = draft) {
    captureView.current();
    const next = defaults ? { time: "", direction: "both" as const, depth: 1, types: [] } : changed;
    const parsed = next.time
      ? new Date(next.time.endsWith("Z") ? next.time : `${next.time}Z`)
      : null;
    if (parsed && Number.isNaN(parsed.getTime())) {
      setError("Enter a valid UTC date and time.");
      return;
    }
    if (defaults) workspacePositions.current.clear();
    const roots = defaults ? [] : [...workspaceRoots.current];
    const focused = !defaults && !namespaceView && roots.length > 0;
    refreshController.current?.abort();
    setError("");
    setRestoreError("");
    setAsOf(parsed?.toISOString() ?? "");
    setAsOfInput(next.time);
    setDirection(next.direction);
    setDepth(next.depth);
    setTypes(next.types);
    setDraft(next);
    setNamespaceView(!focused);
    workspaceRoots.current = new Set(focused ? roots : []);
    if (latestView.current && (namespace || workspaceEntities.current.size)) {
      setRefreshSource({
        ...latestView.current,
        nodes: defaults ? [] : latestView.current.nodes,
        selected: defaults
          ? null
          : (selected?.chain_id ?? refreshSource?.selected ?? latestView.current.selected),
        selectedEdge: defaults
          ? null
          : (selectedEdge?.edge_id ??
            refreshSource?.selectedEdge ??
            latestView.current.selectedEdge),
        workspaceEntities: [...workspaceEntities.current.values()],
        workspaceRoots: focused ? roots : [],
        workspacePages: [],
        workspacePositions: [...workspacePositions.current],
      });
      setFiltering(true);
      setRestoring(true);
    }
    if (defaults) {
      setNodeLimit(1000);
      setIncludeSnapshots(false);
      setSelected(null);
      setSelectedEdge(null);
    }
  }
  // Start a new browser view with the first available namespace; never heal a saved empty scope.
  useEffect(() => {
    if (
      !namespace &&
      !workspaceEntities.current.size &&
      namespaceOptions.options.length &&
      latestView.current
    ) {
      const initial = namespaceOptions.options[0].value;
      setNamespace(initial);
      setNamespaceView(true);
      setRefreshSource(latestView.current);
      setFiltering(true);
      setRestoring(true);
    }
  }, [namespace, namespaceOptions.options]);
  const renderGraph = useCallback(
    (focal: string, positions?: Map<string, { x: number; y: number }>) => {
      const es: Edge[] = [...relations.current.values()].map((r) => ({
        id: r.edge_id,
        source: r.src_chain,
        target: r.dst_chain,
        type: "relationship",
        markerEnd: { type: MarkerType.ArrowClosed, color: "var(--color-muted)" },
        data: {
          name: r.via,
          hideLabel: !labels,
          onSelect: () => {
            setSelected(null);
            setSelectedEdge(r);
            setEdgeDetailsOpen(true);
          },
        },
      }));
      const groups = new Map<string, Edge[]>();
      for (const edge of es) {
        const key = [edge.source, edge.target].sort().join(":");
        groups.set(key, [...(groups.get(key) || []), edge]);
      }
      for (const group of groups.values())
        group.forEach((edge, i) => {
          edge.data!.offset = (i - (group.length - 1) / 2) * 160;
          edge.data!.loopIndex = i;
        });
      const ns: Node[] = [...entities.current.values()].map((e) => ({
        id: e.chain_id,
        ariaLabel: `${e.name}, ${e.entity_type}`,
        type: "entity",
        position: { x: 0, y: 0 },
        data: {
          label: e.name,
          type: e.entity_type,
          namespace: e.namespace,
          expanded: pages.current.has(e.chain_id),
          isSeed: e.chain_id === focal,
        },
      }));
      // Revert only the saved grid produced by the withdrawn 2D experiment.
      // Keep filters, roots and manually positioned/radial workspaces intact.
      if (
        positions &&
        positions.size > 60 &&
        [...positions.values()].every((p) => p.x >= 0 && p.y >= 0 && p.y % 244 === 0)
      )
        positions = undefined;
      const laidOut =
        positions && ns.every((node) => positions.has(node.id)) ? ns : radialLayout(ns, es, focal);
      if (positions?.size) {
        // Existing cards never move. Place newcomers in unoccupied cells; recomputing
        // a radial ring then overlaying old positions can stack new cards on old ones.
        const width = GRAPH_CARD_WIDTH + 80,
          height = GRAPH_CARD_HEIGHT + 80;
        const occupied = new Set<string>();
        for (const node of ns) {
          const p = positions.get(node.id);
          if (!p) continue;
          const x = Math.floor(p.x / width),
            y = Math.floor(p.y / height);
          for (let dx = -1; dx <= 1; dx++)
            for (let dy = -1; dy <= 1; dy++) occupied.add(`${x + dx}:${y + dy}`);
        }
        for (const node of laidOut) {
          if (positions.has(node.id)) continue;
          const x = Math.round(node.position.x / width),
            y = Math.round(node.position.y / height);
          let chosen: { x: number; y: number } | undefined;
          for (let ring = 0; !chosen; ring++)
            for (let dx = -ring; dx <= ring && !chosen; dx++) {
              for (const dy of ring ? [-ring, ring] : [0]) {
                if (!occupied.has(`${x + dx}:${y + dy}`)) {
                  chosen = { x: x + dx, y: y + dy };
                  break;
                }
              }
            }
          occupied.add(`${chosen.x}:${chosen.y}`);
          node.position = { x: chosen.x * width, y: chosen.y * height };
        }
      }
      if (!positions && laidOut.length > 500 && canvasElement.current) {
        // Fit from the complete topology before mounting cards. Otherwise every
        // card first builds its full-detail DOM at zoom1, then rebuilds at overview
        // zoom after measurement. Final fit still uses measured dimensions.
        const rect = canvasElement.current.getBoundingClientRect();
        const bounds = getNodesBounds(
          laidOut.map((n) => ({ ...n, width: GRAPH_CARD_WIDTH, height: GRAPH_CARD_HEIGHT })),
        );
        if (rect.width && rect.height)
          void setFlowViewport(getViewportForBounds(bounds, rect.width, rect.height, 0.08, 1, 0.2));
      }
      setNodes((previous) => {
        const existing = new Map(previous.map((node) => [node.id, node]));
        return laidOut.map((node) => {
          const position = positions?.get(node.id) ?? node.position;
          const old = existing.get(node.id);
          if (
            old &&
            old.position.x === position.x &&
            old.position.y === position.y &&
            old.ariaLabel === node.ariaLabel &&
            JSON.stringify(old.data) === JSON.stringify(node.data)
          )
            return old;
          return { ...node, position };
        });
      });
      setEdges(es);
      if (!positions) setFitRequest((n) => n + 1);
    },
    [labels, setNodes, setEdges, setFlowViewport],
  );
  useEffect(() => {
    if (!restoring || !refreshSource) return;
    const source = refreshSource;
    const previous = committedView.current;
    const workspaceHandles = (source.workspaceEntities ?? source.entities).filter(
      (e) => e.node_kind !== "snapshot",
    );
    const expanded = source.workspacePages ?? source.pages;
    const controller = new AbortController();
    refreshController.current = controller;
    async function refresh() {
      try {
        const handles = new Map(workspaceHandles.map((entity) => [entity.chain_id, entity]));
        const roots =
          source.workspaceRoots ??
          source.nodes.filter((node) => node.data.isSeed).map((node) => node.id);
        const seeds = roots.map((id) => handles.get(id)).filter((e): e is Entity => !!e);
        if (!seeds.length && workspaceHandles.length) seeds.push(workspaceHandles[0]);
        const result = await loadGraph(
          "view",
          {
            namespace_view: namespaceView,
            include_snapshots: includeSnapshots,
            seeds: namespaceView ? [] : seeds.map(graphHandle),
            handles: [],
            expanded: [],
            ...graphScope(scope),
            direction,
            depth,
            entity_types: types,
            node_limit: nodeLimit,
          },
          controller.signal,
          (partial) => {
            if (controller.signal.aborted) return;
            setEffectiveTime(partial.effective_as_of);
            // Publish compact transport pages; final coverage and overlays arrive below.
            entities.current = new Map(partial.nodes.map((entity) => [entity.chain_id, entity]));
            relations.current = new Map(partial.relationships.map((edge) => [edge.edge_id, edge]));
            const root = seeds[0]?.chain_id ?? partial.nodes[0]?.chain_id;
            if (root) renderGraph(root, new Map(source.workspacePositions ?? []));
          },
        );
        if (controller.signal.aborted) return;
        setEffectiveTime(result.effective_as_of);
        const nextEntities = new Map(result.nodes.map((entity) => [entity.chain_id, entity]));
        const nextRelations = new Map(result.relationships.map((edge) => [edge.edge_id, edge]));
        const nextPages = new Map<string, number | null>(
          expanded.filter(([id]) => nextEntities.has(id)).map(([id]) => [id, null]),
        );
        const bounded = result.truncated;
        // Source nodes are an overlay, not permanent entity exploration roots.
        for (const [id, entity] of workspaceEntities.current) {
          if (entity.node_kind === "snapshot" && !nextEntities.has(id)) {
            workspaceEntities.current.delete(id);
            workspacePositions.current.delete(id);
          }
        }
        if (
          new Set([...workspaceEntities.current.keys(), ...nextEntities.keys()]).size >
          MAX_WORKSPACE + 200
        )
          throw new Error("Saved workspace limit reached. Reset to start another workspace.");
        if (controller.signal.aborted) return;
        for (const entity of nextEntities.values())
          workspaceEntities.current.set(entity.chain_id, entityHandle(entity));
        setHasWorkspace(workspaceEntities.current.size > 0);
        for (const [chain, offset] of nextPages) workspacePages.current.set(chain, offset);
        entities.current = nextEntities;
        relations.current = nextRelations;
        pages.current = nextPages;
        const focal =
          source.nodes.find((node) => node.data.isSeed && nextEntities.has(node.id))?.id ??
          source.workspaceRoots?.find((id) => nextEntities.has(id)) ??
          (source.selected && nextEntities.has(source.selected) ? source.selected : null) ??
          nextEntities.keys().next().value;
        if (focal)
          renderGraph(
            focal,
            source.nodes.length
              ? new Map(
                  source.workspacePositions ?? source.nodes.map((node) => [node.id, node.position]),
                )
              : undefined,
          );
        else {
          setNodes([]);
          setEdges([]);
        }
        setSelected(source.selected ? (nextEntities.get(source.selected) ?? null) : null);
        setSelectedEdge(
          source.selectedEdge ? (nextRelations.get(source.selectedEdge) ?? null) : null,
        );
        setMessage(
          bounded
            ? `Partial graph: ${result.limits.map((limit) => limit.replaceAll("_", " ")).join(", ")}. Focus a resource or narrow the namespace/type filters to explore further.`
            : "",
        );
        setRestoreError("");
        setRestoring(false);
        setFiltering(false);
        refreshController.current = null;
      } catch (error) {
        if (!controller.signal.aborted) {
          // Failed pages never become a committed view. Keep new filters for retry.
          entities.current = new Map(
            (previous?.view ?? source).entities.map((e) => [e.chain_id, e]),
          );
          relations.current = new Map(
            (previous?.view ?? source).relations.map((e) => [e.edge_id, e]),
          );
          pages.current = new Map((previous?.view ?? source).pages);
          setNodes(previous?.view.nodes ?? []);
          setEdges(previous?.view.edges ?? []);
          setEffectiveTime(previous?.at ?? "");
          if (previous) void setFlowViewport(previous.view.viewport);
          setSelected(null);
          setSelectedEdge(null);
          setRestoring(false);
          setFiltering(false);
          refreshController.current = null;
          setRestoreError(
            `Could not apply filters. Previous view retained; retry before exploring. ${error instanceof Error ? error.message : "Unable to refresh graph"}`,
          );
        }
      }
    }
    void refresh();
    return () => {
      controller.abort();
      if (refreshController.current === controller) refreshController.current = null;
    };
  }, [
    refreshSource,
    scope,
    direction,
    depth,
    types,
    restoreAttempt,
    restoring,
    namespaceView,
    includeSnapshots,
    nodeLimit,
    renderGraph,
    setNodes,
    setEdges,
    setFlowViewport,
  ]);
  const backgroundRefresh = useGraphRefresh({
    viewKey: JSON.stringify([
      orgId,
      scope,
      direction,
      depth,
      types,
      nodeLimit,
      namespaceView,
      includeSnapshots,
      namespaceView ? [] : nodes.filter((n) => n.data.isSeed).map((n) => n.id),
    ]),
    enabled: !!namespace && !restoring && !filtering && !restoreError,
    current: !asOf,
    prepare: async (signal) => {
      const seeds = [...workspaceRoots.current]
        .map((id) => workspaceEntities.current.get(id))
        .filter((e): e is Entity => !!e);
      const result = await loadGraph(
        "view",
        {
          namespace_view: namespaceView,
          include_snapshots: includeSnapshots,
          seeds: namespaceView ? [] : seeds.map(graphHandle),
          handles: [],
          expanded: [],
          ...graphScope(scope),
          direction,
          depth,
          entity_types: types,
          node_limit: nodeLimit,
        },
        signal,
      );
      return () => {
        // Publish only a complete, successful read. Keep surviving positions and
        // the latest user selection (which may have changed during the request).
        captureView.current();
        const saved = latestView.current;
        const nextEntities = new Map(result.nodes.map((e) => [e.chain_id, e]));
        const nextRelations = new Map(result.relationships.map((e) => [e.edge_id, e]));
        const sorted = (values: Iterable<Entity | Neighbor>) =>
          JSON.stringify(
            [...values].sort((a, b) =>
              String("chain_id" in a ? a.chain_id : a.edge_id).localeCompare(
                String("chain_id" in b ? b.chain_id : b.edge_id),
              ),
            ),
          );
        const changed =
          sorted(entities.current.values()) !== sorted(nextEntities.values()) ||
          sorted(relations.current.values()) !== sorted(nextRelations.values());
        if (
          changed &&
          new Set([...nextEntities.keys(), ...workspaceRoots.current]).size > MAX_WORKSPACE + 200
        )
          throw new Error("Graph refresh exceeds the saved workspace budget");
        setEffectiveTime(result.effective_as_of);
        if (!changed) return;
        for (const [id, entity] of workspaceEntities.current) {
          if (entity.node_kind === "snapshot" && !nextEntities.has(id)) {
            workspaceEntities.current.delete(id);
            workspacePositions.current.delete(id);
            workspacePages.current.delete(id);
          }
        }
        // A long-lived page must not grow its saved workspace without a bound.
        const union = new Set([...workspaceEntities.current.keys(), ...nextEntities.keys()]);
        for (const id of union) {
          if (union.size <= MAX_WORKSPACE + 200) break;
          if (!nextEntities.has(id) && !workspaceRoots.current.has(id)) {
            union.delete(id);
            workspaceEntities.current.delete(id);
            workspacePositions.current.delete(id);
            workspacePages.current.delete(id);
          }
        }
        for (const entity of nextEntities.values())
          workspaceEntities.current.set(entity.chain_id, entityHandle(entity));
        setHasWorkspace(workspaceEntities.current.size > 0);
        entities.current = nextEntities;
        relations.current = nextRelations;
        pages.current = new Map([...pages.current].filter(([id]) => nextEntities.has(id)));
        const positions = new Map(saved?.nodes.map((n) => [n.id, n.position]) ?? []);
        const focal =
          saved?.nodes.find((n) => n.data.isSeed && nextEntities.has(n.id))?.id ??
          seeds[0]?.chain_id ??
          result.nodes[0]?.chain_id;
        if (focal) renderGraph(focal, positions);
        else {
          setNodes([]);
          setEdges([]);
        }
        setSelected((value) => (value ? (nextEntities.get(value.chain_id) ?? null) : null));
        setSelectedEdge((value) => (value ? (nextRelations.get(value.edge_id) ?? null) : null));
        setEffectiveTime(result.effective_as_of);
        setMessage(
          result.truncated
            ? `Partial graph: ${result.limits.map((value) => value.replaceAll("_", " ")).join(", ")}. Narrow the filters to explore further.`
            : "",
        );
      };
    },
  });
  // Selection replaces the visible neighborhood; every entry point uses the
  // same server traversal and retains the active filters.
  function focusGraph(
    members: Entity[],
    selectedEdge: string | null = null,
    remember = true,
    openDetails = true,
  ) {
    if (restoreError) return;
    clearTimeout(entityClickTimer.current);
    captureView.current();
    if (!latestView.current) return;
    if (remember) {
      const previous = [...workspaceRoots.current]
        .map((id) => workspaceEntities.current.get(id))
        .filter((e): e is Entity => !!e);
      setFocusTrail((trail) => [...trail.slice(-19), previous]);
    }
    refreshController.current?.abort();
    setError("");
    setRestoreError("");
    setShowThreads(false);
    setNamespaceView(false);
    workspaceRoots.current = new Set(members.map((e) => e.chain_id));
    workspaceEntities.current = new Map(members.map((e) => [e.chain_id, entityHandle(e)]));
    workspacePages.current.clear();
    workspacePositions.current.clear();
    setSelected(selectedEdge || !openDetails ? null : members[0]);
    setSelectedEdge(null);
    setRefreshSource({
      ...latestView.current,
      namespaceView: false,
      nodes: [],
      edges: [],
      workspacePositions: [],
      workspacePages: [],
      pages: [],
      workspaceEntities: members.map(entityHandle),
      entities: members.map(entityHandle),
      workspaceRoots: members.map((e) => e.chain_id),
      selected: selectedEdge || !openDetails ? null : members[0].chain_id,
      selectedEdge,
    });
    setFiltering(true);
    setRestoring(true);
  }
  function expand(entity: Entity, openDetails = true) {
    if (restoreError) return;
    // An observation is evidence, not a dependency traversal root.
    if (entity.node_kind === "snapshot") {
      setSelected(openDetails ? entity : null);
      setSelectedEdge(null);
      return;
    }
    focusGraph([entity], null, true, openDetails);
  }
  function clickEntity(entity: Entity) {
    clearTimeout(entityClickTimer.current);
    // Defer traversal so a double-click cannot move its target before the
    // second click. Keyboard activation and search results remain immediate.
    entityClickTimer.current = setTimeout(() => expand(entity, false), 350);
  }
  function inspectEntity(entity: Entity) {
    clearTimeout(entityClickTimer.current);
    expand(entity, true);
  }
  function activateEdge(id: string, inspect = false) {
    if (restoreError) return;
    clearTimeout(entityClickTimer.current);
    const edge = relations.current.get(id);
    if (edge) {
      setSelected(null);
      setSelectedEdge(edge);
      setEdgeDetailsOpen(inspect);
      setShowThreads(false);
    }
  }
  const renderedEdges = useMemo(
    () =>
      edges.map((edge) => ({
        ...edge,
        data: {
          ...edge.data,
          selected: edge.id === selectedEdge?.edge_id,
          hideLabel: edges.length > 80 && edge.id !== selectedEdge?.edge_id,
          onInspect: () => {
            const relation = relations.current.get(edge.id);
            if (relation && !restoreError) {
              setSelected(null);
              setSelectedEdge(relation);
              setEdgeDetailsOpen(true);
              setShowThreads(false);
            }
          },
          onSelect: () => {
            if (restoreError) return;
            const relation = relations.current.get(edge.id);
            if (relation) {
              setSelected(null);
              setSelectedEdge(relation);
              setEdgeDetailsOpen(false);
            }
          },
        },
      })),
    [edges, selectedEdge?.edge_id, restoreError],
  );
  const displayNodes = useMemo(() => {
    if (!selectedEdge) return nodes;
    return nodes.map((node) =>
      node.id === selectedEdge.src_chain || node.id === selectedEdge.dst_chain
        ? { ...node, data: { ...node.data, edgeEndpoint: true } }
        : node,
    );
  }, [nodes, selectedEdge]);
  const canvas = useCanvasBatches(displayNodes, renderedEdges, true);
  useEffect(() => {
    if (restoreError || !canvas.complete || fittedRequest.current === fitRequest) return;
    const timer = setTimeout(() => {
      fittedRequest.current = fitRequest;
      void fitView({
        padding: 0.2,
        duration: window.matchMedia("(prefers-reduced-motion: reduce)").matches ? 0 : 250,
        maxZoom: 1,
      });
    }, 60);
    return () => clearTimeout(timer);
  }, [restoreError, canvas.complete, fitRequest, fitView]);
  if (restoring && !filtering)
    return (
      <main className="flex h-dvh items-center justify-center p-6 text-center">
        <div role="status">
          <p>{restoreError || "Checking saved graph against current data…"}</p>
          {restoreError && (
            <div className="mt-4 flex justify-center gap-2">
              <button
                onClick={() => {
                  setRestoreError("");
                  setRestoreAttempt((attempt) => attempt + 1);
                }}
              >
                Retry
              </button>
              <button
                onClick={() => {
                  reset();
                  setRestoring(false);
                }}
              >
                Reset
              </button>
            </div>
          )}
        </div>
      </main>
    );
  return (
    <main className="flex h-dvh flex-col p-2 md:p-3">
      <section
        aria-label="Graph filters"
        className={`graph-toolbar core-filters border border-b-0 border-border bg-surface p-3 ${filtersOpen ? "filters-expanded" : ""}`}
      >
        <div className="control-field scope-field">
          <span>Namespace</span>
          <FilterSelect
            label="Namespace"
            value={namespace}
            items={catalogNamespaces}
            search={namespaceSearch}
            onSearch={setNamespaceSearch}
            onChange={(ns) => scopeChange(ns, asOf, direction)}
            hasMore={namespaceOptions.hasNextPage}
            busy={namespaceOptions.isFetching}
            onMore={() => void namespaceOptions.fetchNextPage()}
          />
        </div>
        <label className="control-field secondary-filter">
          Time
          <select
            aria-label="Time mode"
            value={draft.time ? "fixed" : "current"}
            onChange={(e) =>
              applyFilters(false, {
                ...draft,
                time: e.target.value === "current" ? "" : new Date().toISOString().slice(0, 16),
              })
            }
          >
            <option value="current">Current</option>
            <option value="fixed">As of (UTC)</option>
          </select>
          {draft.time && (
            <input
              aria-label="As of UTC"
              type="datetime-local"
              value={draft.time.replace(/Z$/, "").slice(0, 16)}
              onChange={(e) => applyFilters(false, { ...draft, time: e.target.value })}
            />
          )}
        </label>
        <label
          className="control-field secondary-filter"
          title={namespaceView ? "Select a resource to explore by direction" : ""}
        >
          Direction
          <select
            aria-label="Relationship direction"
            disabled={namespaceView}
            value={draft.direction}
            onChange={(e) =>
              applyFilters(false, { ...draft, direction: e.target.value as "both" | "in" | "out" })
            }
          >
            <option value="both">All directions</option>
            <option value="out">Outgoing</option>
            <option value="in">Incoming</option>
          </select>
        </label>
        <div className="control-field secondary-filter">
          <span>Entity type</span>
          <FilterSelect
            label="Entity types"
            value={draft.types[0] ?? ""}
            emptyLabel="All entity types"
            items={catalogTypes}
            search={typeSearch}
            onSearch={setTypeSearch}
            onChange={(type) => applyFilters(false, { ...draft, types: type ? [type] : [] })}
            hasMore={typeOptions.hasNextPage}
            busy={typeOptions.isFetching}
            onMore={() => void typeOptions.fetchNextPage()}
          />
        </div>
        <label
          className="control-field secondary-filter"
          title={namespaceView ? "Select a resource to explore by depth" : ""}
        >
          Depth
          <select
            aria-label="Expansion depth"
            disabled={namespaceView}
            value={draft.depth}
            onChange={(e) => applyFilters(false, { ...draft, depth: Number(e.target.value) })}
          >
            {[1, 2, 3].map((d) => (
              <option key={d}>{d}</option>
            ))}
          </select>
        </label>
        <div className="control-field finder-field">
          <span>Search</span>
          <GraphFinder
            query={query}
            onQueryChange={setQuery}
            orgId={orgId}
            scope={scope}
            types={types}
            semanticAvailable={namespaceOptions.semantic_available}
            refreshKey={effectiveTime}
            visibleChains={new Set(nodes.map((n) => n.id))}
            onRelationship={(relation, source, target) =>
              focusGraph([source, target], relation.uuid)
            }
            onSelect={(entity) => expand(entity)}
          />
        </div>
        <label className="checkbox-field snapshot-toggle secondary-filter">
          <input
            type="checkbox"
            checked={includeSnapshots}
            onChange={(e) => {
              setIncludeSnapshots(e.target.checked);
              refreshController.current?.abort();
              setRestoreError("");
              if (latestView.current && (namespace || workspaceEntities.current.size)) {
                setRefreshSource(latestView.current);
                setFiltering(true);
                setRestoring(true);
              }
            }}
          />
          Show source snapshots
        </label>
        <div className="filter-actions">
          <button
            className="mobile-filters"
            aria-expanded={filtersOpen}
            onClick={() => setFiltersOpen((v) => !v)}
          >
            Filters
          </button>
          <button
            disabled={!namespace}
            onClick={() => {
              setQuery("");
              setThreadView(defaultThreadView);
              setFocusTrail([]);
              setDetailViews({});
              applyFilters(true);
            }}
          >
            Reset
          </button>
        </div>
      </section>
      <div
        className="flex flex-wrap justify-between gap-2 border-x border-border px-3 py-1 text-xs text-muted"
        role="status"
      >
        <span>
          {filtering
            ? "Updating graph…"
            : `${nodes.filter((n) => !n.id.startsWith("snapshot:")).length} resources · ${edges.filter((e) => !e.source.startsWith("snapshot:")).length} relationships`}
          {includeSnapshots &&
            ` · ${nodes.filter((n) => n.id.startsWith("snapshot:")).length} snapshots · ${edges.filter((e) => e.source.startsWith("snapshot:")).length} observations`}
        </span>
        <div className="graph-context">
          {!namespaceView && (
            <>
              <button
                disabled={!focusTrail.length}
                onClick={() => {
                  const previous = focusTrail.at(-1);
                  setFocusTrail((t) => t.slice(0, -1));
                  if (previous?.length) focusGraph(previous, null, false);
                  else {
                    workspaceRoots.current.clear();
                    setNamespaceView(true);
                    setRefreshSource(latestView.current);
                    setFiltering(true);
                    setRestoring(true);
                  }
                }}
              >
                ← Back
              </button>
              <button
                onClick={() => {
                  workspaceRoots.current.clear();
                  setNamespaceView(true);
                  setRefreshSource(latestView.current);
                  setFiltering(true);
                  setRestoring(true);
                }}
              >
                Namespace overview
              </button>
            </>
          )}
          {effectiveTime && (
            <time
              dateTime={effectiveTime}
              title={`Graph and details read at ${formatAsOf(effectiveTime)}`}
            >
              {asOf ? "As of" : "Updated"} {formatAsOf(effectiveTime)}
            </time>
          )}
          <button
            disabled={filtering || restoring || backgroundRefresh.status === "checking"}
            onClick={backgroundRefresh.refresh}
          >
            Refresh
          </button>
          {backgroundRefresh.status === "delayed" && (
            <span role="status">Updates delayed · retrying</span>
          )}
        </div>
        <div className="graph-view-actions">
          <label className="canvas-budget">
            Resources{" "}
            <select
              aria-label="Canvas resource budget"
              value={nodeLimit}
              onChange={(e) => {
                setNodeLimit(Number(e.target.value));
                captureView.current();
                setRefreshSource(latestView.current);
                setFiltering(true);
                setRestoring(true);
              }}
            >
              {[200, 500, 1000, 2000].map((n) => (
                <option key={n} value={n}>
                  {n.toLocaleString()}
                </option>
              ))}
            </select>
          </label>
          <button aria-pressed={showThreads} onClick={() => setShowThreads(!showThreads)}>
            Threads
          </button>
        </div>
      </div>
      {storageError && (
        <p role="status" className="p-3 text-sm text-accent">
          {storageError}
        </p>
      )}
      {(error || restoreError || catalog.error) && (
        <p role="alert" className="bg-red-950 p-3">
          {error || restoreError || catalog.error?.message}
          {restoreError && (
            <button
              onClick={() => {
                setRestoreError("");
                setFiltering(true);
                setRestoring(true);
                setRestoreAttempt((n) => n + 1);
              }}
            >
              Retry graph
            </button>
          )}
        </p>
      )}
      {(scopeNotice || message) && (
        <p role="status" className="px-4 py-2 text-xs text-accent">
          {scopeNotice || message}
        </p>
      )}
      <div
        aria-busy={filtering}
        className="relative flex min-h-0 flex-1 overflow-hidden border border-border bg-surface"
      >
        <div
          className="relative min-w-0 flex-1"
          ref={canvasElement}
          data-graph-ready={canvas.complete}
          onKeyDownCapture={(event) => {
            if (event.key !== "Enter" && event.key !== " ") return;
            const target = event.target;
            if (target instanceof Element && target.classList.contains("react-flow__edge")) {
              event.preventDefault();
              event.stopPropagation();
              if (restoreError) return;
              const relationship = relations.current.get(target.getAttribute("data-id") ?? "");
              if (relationship) {
                setShowThreads(false);
                setSelected(null);
                setSelectedEdge(relationship);
                setEdgeDetailsOpen(true);
              }
              return;
            }
            if (!(target instanceof Element) || !target.classList.contains("react-flow__node"))
              return;
            const entity = entities.current.get(target.getAttribute("data-id") ?? "");
            if (entity) {
              event.preventDefault();
              event.stopPropagation();
              void expand(entity);
            }
          }}
        >
          <ReactFlow
            onlyRenderVisibleElements
            proOptions={{ hideAttribution: true }}
            defaultViewport={viewport}
            onMoveEnd={(_, nextViewport) => setViewport(nextViewport)}
            nodes={canvas.nodes}
            edges={canvas.edges}
            nodeTypes={nodeTypes}
            edgeTypes={edgeTypes}
            onEdgeClick={(_, edge) => activateEdge(edge.id)}
            onEdgeDoubleClick={(_, edge) => activateEdge(edge.id, true)}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            nodesConnectable={false}
            deleteKeyCode={null}
            minZoom={0.08}
            maxZoom={2}
            zoomOnDoubleClick={false}
            onNodeDoubleClick={(_, node) => {
              const entity = entities.current.get(node.id);
              if (entity) inspectEntity(entity);
            }}
            onNodeClick={(_, node) => {
              if (restoreError) return;
              const e = entities.current.get(node.id);
              if (e) {
                setShowThreads(false);
                clickEntity(e);
              }
            }}
            onPaneClick={() => {
              clearTimeout(entityClickTimer.current);
              setSelected(null);
              setSelectedEdge(null);
            }}
          >
            <Background gap={24} color="var(--color-border)" />
            <Controls showInteractive={false} />
          </ReactFlow>
          {!canvas.complete && (
            <div
              role="status"
              className="absolute bottom-4 right-4 rounded-none border border-border bg-surface px-3 py-2 text-xs text-muted"
            >
              Preparing graph · {canvas.nodes.length.toLocaleString()} /{" "}
              {nodes.length.toLocaleString()} resources
            </div>
          )}
          {!nodes.length && (
            <div className="absolute inset-0 flex items-center justify-center text-center">
              <div className="w-full max-w-xl px-6">
                <h1 className="font-display text-[26px] font-semibold">
                  {hasWorkspace || namespace
                    ? "No entities match these filters."
                    : "Explore the graph."}
                </h1>
                <p className="mt-3 text-muted">
                  {hasWorkspace ? (
                    "Widen the filters to bring your graph back."
                  ) : (
                    <>
                      Select a namespace to load its graph.
                      <br />
                      Click a node to explore its neighbors.
                    </>
                  )}
                </p>
              </div>
            </div>
          )}
        </div>
        {showThreads && (
          <ThreadPanel
            scope={detailScope}
            view={threadView}
            onChange={setThreadView}
            onClose={() => setShowThreads(false)}
          />
        )}
        {!showThreads && selected?.node_kind === "snapshot" && (
          <PanelView
            view={detailViews[selected.chain_id] ?? defaultDetailView}
            onChange={(view) =>
              setDetailViews((current) => ({ ...current, [selected.chain_id]: view }))
            }
          >
            <SnapshotDetails
              entity={selected}
              scope={detailScope}
              onClose={() => setSelected(null)}
            />
          </PanelView>
        )}
        {!showThreads && selected && selected.node_kind !== "snapshot" && (
          <>
            <Details
              key={`${selected.chain_id}:${namespace}:${asOf}`}
              entity={selected}
              view={detailViews[selected.chain_id] ?? defaultDetailView}
              onViewChange={(view) =>
                setDetailViews((current) => ({
                  ...current,
                  [selected.chain_id]: view,
                }))
              }
              scope={detailScope}
              onNavigate={(e) => void expand(e)}
              onClose={() => setSelected(null)}
            />
          </>
        )}
        {!showThreads && selectedEdge && edgeDetailsOpen && (
          <PanelView
            view={detailViews[selectedEdge.edge_id] ?? defaultDetailView}
            onChange={(view) =>
              setDetailViews((current) => ({
                ...current,
                [selectedEdge.edge_id]: view,
              }))
            }
          >
            <aside
              aria-label="Relationship details"
              className="graph-panel"
              ref={(element) => {
                if (element) element.scrollTop = detailViews[selectedEdge.edge_id]?.scrollTop ?? 0;
              }}
              onScroll={(event) => {
                const scrollTop = event.currentTarget.scrollTop;
                setDetailViews((current) => ({
                  ...current,
                  [selectedEdge.edge_id]: {
                    ...(current[selectedEdge.edge_id] ?? defaultDetailView),
                    scrollTop,
                  },
                }));
              }}
            >
              <button
                aria-label="Close relationship details"
                className="icon-button float-right"
                onClick={() => setSelectedEdge(null)}
              >
                ×
              </button>
              <div className="text-xs text-accent">RELATIONSHIP</div>
              <h2 className="my-3 text-xl">{selectedEdge.via}</h2>
              {[selectedEdge.src_chain, selectedEdge.dst_chain].map((id, i) => (
                <button
                  key={`${id}:${i}`}
                  className="entity-link"
                  onClick={() => {
                    const e = entities.current.get(id);
                    if (e) void expand(e);
                  }}
                >
                  {i === 0 ? "From" : "To"}:{" "}
                  {String(nodes.find((n) => n.id === id)?.data.label ?? id)}
                </button>
              ))}
              {!isObservation && edgeDetails.isPending ? (
                <p role="status">Loading relationship details…</p>
              ) : !isObservation && edgeDetails.error ? (
                <p role="alert">{edgeDetails.error.message}</p>
              ) : (
                <>
                  <div className="my-4 border border-border bg-raised p-3 text-xs leading-6">
                    <div>
                      Evidence:{" "}
                      {isObservation
                        ? "Snapshot observed this exact entity version"
                        : (edgeRecord.metadata as Record<string, unknown>)?.origin === "declared"
                          ? "Source declaration"
                          : (edgeRecord.metadata as Record<string, unknown>)?.origin === "reference"
                            ? "Matching source property"
                            : "Extracted fact"}
                    </div>
                    {typeof (edgeRecord.metadata as Record<string, unknown>)?.description ===
                      "string" && (
                      <div>
                        {String(
                          (edgeRecord.metadata as Record<string, unknown>)?.description ?? "",
                        )}
                      </div>
                    )}
                    {typeof (edgeRecord.metadata as Record<string, unknown>)?.source_property ===
                      "string" && (
                      <div>
                        Source field:{" "}
                        {String(
                          (edgeRecord.metadata as Record<string, unknown>)?.source_property ?? "",
                        )}
                      </div>
                    )}
                    {typeof (edgeRecord.metadata as Record<string, unknown>)
                      ?.target_identity_field === "string" && (
                      <div>
                        Matched key:{" "}
                        {String(
                          (edgeRecord.metadata as Record<string, unknown>)?.target_identity_field ??
                            "",
                        )}
                      </div>
                    )}
                    {typeof (edgeRecord.metadata as Record<string, unknown>)?.valid_from ===
                      "string" && (
                      <div>
                        Valid from:{" "}
                        {String((edgeRecord.metadata as Record<string, unknown>)?.valid_from ?? "")}
                      </div>
                    )}
                    {typeof (edgeRecord.metadata as Record<string, unknown>)?.invalid_at ===
                      "string" && (
                      <div>
                        Valid until:{" "}
                        {String((edgeRecord.metadata as Record<string, unknown>)?.invalid_at ?? "")}
                      </div>
                    )}
                  </div>
                  {isObservation ? (
                    <Properties value={edgeRecord} />
                  ) : (
                    <RecordDetails
                      scope={detailScope}
                      identity={{ kind: "relationship", uuid: selectedEdge.edge_id }}
                    />
                  )}
                </>
              )}
            </aside>
          </PanelView>
        )}
        {filtering && (
          <div className="absolute bottom-4 left-4 z-40 text-center pointer-events-none">
            <div
              role="status"
              className="border border-border bg-raised p-3 text-xs text-accent pointer-events-auto"
            >
              {restoreError || "Updating graph for filters…"}
              {restoreError && (
                <div className="mt-3">
                  <button onClick={() => setRestoreAttempt((attempt) => attempt + 1)}>Retry</button>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </main>
  );
}
const subscribe = () => () => {};
function OrganizationView() {
  const mounted = useSyncExternalStore(
    subscribe,
    () => true,
    () => false,
  );
  const identity = useQuery({
    queryKey: ["graph-organization"],
    queryFn: ({ signal }) => api<Catalog>("catalog?limit=1", signal),
  });
  if (identity.error)
    return (
      <main className="p-6">
        <p role="alert">Unable to connect to the graph API.</p>
        <button onClick={() => void identity.refetch()}>Retry</button>
      </main>
    );
  if (!mounted || !identity.data)
    return (
      <main className="p-6 text-sm text-muted" role="status">
        Restoring your workspace…
      </main>
    );
  return <Explorer key={identity.data.org_id} orgId={identity.data.org_id} />;
}
export default function Page() {
  const [client] = useState(
    () =>
      new QueryClient({
        defaultOptions: { queries: { staleTime: 15000, retry: 1 } },
      }),
  );
  return (
    <QueryClientProvider client={client}>
      <ReactFlowProvider>
        <OrganizationView />
      </ReactFlowProvider>
    </QueryClientProvider>
  );
}
