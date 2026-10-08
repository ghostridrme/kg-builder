"use client";
import { useEffect, useMemo, useState } from "react";
import type { Edge, Node } from "@xyflow/react";

const NODE_BATCH = 128;
const EDGE_BATCH = 256;

/** Presentation only: the workspace and its persisted topology stay complete.
 * Dimension/selection changes retain progress; a different topology cancels it.
 * Schedule after each committed batch so React cannot coalesce all batches into
 * one large update. Small focused graphs render immediately.
 */
export function useCanvasBatches(nodes: Node[], edges: Edge[], enabled: boolean) {
  const key = useMemo(
    () =>
      JSON.stringify([
        enabled,
        nodes.map((n) => n.id),
        edges.map((e) => [e.id, e.source, e.target]),
      ]),
    [nodes, edges, enabled],
  );
  const steps =
    enabled && (nodes.length > 500 || edges.length > 1000)
      ? Math.max(Math.ceil(nodes.length / NODE_BATCH), Math.ceil(edges.length / EDGE_BATCH))
      : 1;
  const [progress, setProgress] = useState({ key: "", step: 1 });
  const step = progress.key === key ? progress.step : 1;
  const complete = step >= steps;
  useEffect(() => {
    if (complete) return;
    const timer = setTimeout(() => setProgress({ key, step: step + 1 }), 16);
    return () => clearTimeout(timer);
  }, [key, step, complete]);
  const visibleNodes = useMemo(
    () => (complete ? nodes : nodes.slice(0, step * NODE_BATCH)),
    [nodes, complete, step],
  );
  const visibleEdges = useMemo(() => {
    if (complete) return edges;
    const ids = new Set(visibleNodes.map((n) => n.id));
    return edges.slice(0, step * EDGE_BATCH).filter((e) => ids.has(e.source) && ids.has(e.target));
  }, [edges, visibleNodes, complete, step]);
  return { nodes: visibleNodes, edges: visibleEdges, complete };
}
