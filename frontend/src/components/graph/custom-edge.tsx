import { GRAPH_EDGE_STYLE } from "./graph-style";
import { memo } from "react";
import {
  BaseEdge,
  EdgeLabelRenderer,
  getBezierPath,
  Position,
  useInternalNode,
  useStore,
  type EdgeProps,
  type InternalNode,
} from "@xyflow/react";

export type RelEdgeData = {
  /** Relationship name rendered as a clickable label. */
  name?: string;
  offset?: number;
  loopIndex?: number;
  /** True when this edge is the current selection. */
  selected?: boolean;
  /** Select this edge and highlight its endpoints. */
  onSelect?: () => void;
  onInspect?: () => void;
  /** Hide the relationship label (the "Labels" toggle) — declutters dense graphs. */
  hideLabel?: boolean;
};

// ── floating-edge geometry ──────────────────────────────────────────────────
// A knowledge graph isn't a left→right DAG: an edge should leave/enter whichever
// SIDE of a node faces its neighbor. We compute the point where the line between
// node centers crosses each node's box, so edges attach on all four sides.

function intersection(node: InternalNode, other: InternalNode) {
  const w = (node.measured.width ?? 0) / 2;
  const h = (node.measured.height ?? 0) / 2;
  const cx = node.internals.positionAbsolute.x + w;
  const cy = node.internals.positionAbsolute.y + h;
  const ox = other.internals.positionAbsolute.x + (other.measured.width ?? 0) / 2;
  const oy = other.internals.positionAbsolute.y + (other.measured.height ?? 0) / 2;
  if (w === 0 || h === 0) return { x: cx, y: cy };
  const xx = (ox - cx) / (2 * w) - (oy - cy) / (2 * h);
  const yy = (ox - cx) / (2 * w) + (oy - cy) / (2 * h);
  const a = 1 / (Math.abs(xx) + Math.abs(yy) || 1);
  const bx = a * xx;
  const by = a * yy;
  return { x: w * (bx + by) + cx, y: h * (-bx + by) + cy };
}

function sideOf(node: InternalNode, p: { x: number; y: number }) {
  const nx = node.internals.positionAbsolute.x;
  const ny = node.internals.positionAbsolute.y;
  const w = node.measured.width ?? 0;
  if (p.x <= nx + 1) return Position.Left;
  if (p.x >= nx + w - 1) return Position.Right;
  if (p.y <= ny + 1) return Position.Top;
  return Position.Bottom;
}

/** Labeled relationship that connects the sides facing its endpoints. */
function CustomEdgeInner({ id, source, target, markerEnd, data }: EdgeProps) {
  const d = (data ?? {}) as RelEdgeData;
  const readable = useStore((state) => state.transform[2] >= 0.45);
  const sourceNode = useInternalNode(source);
  const targetNode = useInternalNode(target);
  if (!sourceNode || !targetNode) return null;

  const sp = intersection(sourceNode, targetNode);
  const tp = intersection(targetNode, sourceNode);
  let [path, labelX, labelY] = getBezierPath({
    sourceX: sp.x,
    sourceY: sp.y,
    targetX: tp.x,
    targetY: tp.y,
    sourcePosition: sideOf(sourceNode, sp),
    targetPosition: sideOf(targetNode, tp),
  });

  const offset = d.offset ?? 0;
  if (source === target) {
    const x = sourceNode.internals.positionAbsolute.x;
    const y = sourceNode.internals.positionAbsolute.y;
    const w = sourceNode.measured.width ?? 240;
    const lift = 100 + (d.loopIndex ?? 0) * 80;
    path = `M ${x + w * 0.3} ${y} C ${x - w * 0.2} ${y - lift}, ${x + w * 1.2} ${y - lift}, ${x + w * 0.7} ${y}`;
    labelX = x + w * 0.5;
    labelY = y - lift * 0.75;
  } else if (offset) {
    const dx = tp.x - sp.x,
      dy = tp.y - sp.y,
      length = Math.hypot(dx, dy) || 1;
    const sign = source < target ? 1 : -1;
    const cx = (sp.x + tp.x) / 2 - (dy / length) * offset * sign;
    const cy = (sp.y + tp.y) / 2 + (dx / length) * offset * sign;
    path = `M ${sp.x} ${sp.y} Q ${cx} ${cy} ${tp.x} ${tp.y}`;
    labelX = (sp.x + 2 * cx + tp.x) / 4;
    labelY = (sp.y + 2 * cy + tp.y) / 4;
  }
  return (
    <>
      <BaseEdge
        id={id}
        path={path}
        markerEnd={markerEnd}
        style={{
          stroke: d.selected ? "var(--color-accent)" : "var(--color-border-strong)",
          strokeWidth: d.selected ? GRAPH_EDGE_STYLE.selectedWidth : GRAPH_EDGE_STYLE.width,
          opacity: d.selected ? 1 : d.hideLabel ? GRAPH_EDGE_STYLE.overviewOpacity : 1,
        }}
      />
      {d.name && (d.selected || (readable && !d.hideLabel)) && (
        <EdgeLabelRenderer>
          <button
            title={d.name}
            onClick={(e) => {
              e.stopPropagation();
              if (e.detail === 0) d.onInspect?.();
              else d.onSelect?.();
            }}
            onDoubleClick={(e) => {
              e.stopPropagation();
              d.onInspect?.();
            }}
            style={{
              transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`,
              pointerEvents: "all",
              fontSize: GRAPH_EDGE_STYLE.labelFontSize,
            }}
            className={`nodrag nopan absolute rounded-none bg-[var(--color-raised)]/90 px-1 py-px font-mono text-[10px] uppercase tracking-tight ring-1 ring-inset transition ${
              d.selected
                ? "text-[var(--color-fg)] ring-[var(--color-accent)]"
                : "text-[var(--color-faint)] ring-[var(--color-border)] hover:text-[var(--color-fg)] hover:ring-[color-mix(in_oklab,var(--color-accent)_45%,transparent)]"
            }`}
          >
            {d.name}
          </button>
        </EdgeLabelRenderer>
      )}
    </>
  );
}

export const CustomEdge = memo(CustomEdgeInner);
