import { GRAPH_CARD_WIDTH } from "./graph-style";
import { memo } from "react";
import { Handle, Position, useStore, type NodeProps } from "@xyflow/react";
import { cn, hueFor } from "@/lib/utils";

export type EntityNodeData = {
  label: string;
  type: string;
  namespace?: string;
  dependents?: number;
  isSeed?: boolean;
  expanded?: boolean;
  edgeEndpoint?: boolean;
};

// Invisible connection points — this is a read-only visualization, not an editor.
const HANDLE = "!h-2 !w-2 !min-w-0 !min-h-0 !border-0 !bg-transparent !opacity-0";

function EntityNodeInner({ data, selected }: NodeProps) {
  const d = data as EntityNodeData;
  const overview = useStore((state) => state.transform[2] < 0.35);
  const hue = hueFor(d.type || "node");
  return (
    <div
      data-edge-endpoint={d.edgeEndpoint || undefined}
      title={`${d.label} · ${d.type} · Click to explore · Double-click for properties`}
      className={cn(
        "group overflow-hidden rounded-none border bg-[var(--color-surface)] transition-colors",
        overview && "entity-overview",
        selected || d.isSeed || d.edgeEndpoint
          ? "border-accent"
          : "border-[var(--color-border-strong)] hover:border-[var(--color-fg)]/25",
      )}
      style={{
        width: GRAPH_CARD_WIDTH,
        boxShadow: d.isSeed
          ? "0 0 0 1px var(--color-accent), 0 0 24px -8px color-mix(in oklab, var(--color-accent) 55%, transparent)"
          : selected || d.edgeEndpoint
            ? "0 0 0 1px var(--color-accent), 0 8px 24px -12px color-mix(in oklab, var(--color-accent) 35%, transparent)"
            : undefined,
      }}
    >
      <Handle type="target" position={Position.Left} isConnectable={false} className={HANDLE} />
      <Handle type="source" position={Position.Right} isConnectable={false} className={HANDLE} />

      <div className="entity-card-header flex items-center gap-2 border-b border-[var(--color-border)] px-3 py-2.5">
        <span className="grid h-6 w-6 shrink-0 place-items-center rounded-none bg-[var(--color-raised)]">
          <span className="h-2 w-2 rounded-full" style={{ background: `hsl(${hue} 65% 60%)` }} />
        </span>
        <span
          className="min-w-0 flex-1 truncate text-[14px] font-medium text-[var(--color-fg)]"
          title={d.label}
        >
          {d.label}
        </span>
        {d.isSeed && (
          <span className="shrink-0 rounded-none border border-accent/40 px-1.5 py-0.5 text-[9px] font-medium uppercase tracking-wide text-accent">
            Focus
          </span>
        )}
      </div>

      {!overview && (
        <div className="entity-card-body space-y-2 px-3 py-3">
          <span
            title={d.type}
            className="inline-block max-w-full truncate rounded-none border border-[var(--color-border)] px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide text-[var(--color-muted)]"
          >
            {d.type}
          </span>
          {(d.namespace || (d.dependents != null && d.dependents > 0)) && (
            <div className="flex items-center gap-2 text-[11px] text-[var(--color-faint)]">
              {d.namespace && <span className="truncate">{d.namespace}</span>}
              {d.dependents != null && d.dependents > 0 && (
                <span className="ml-auto shrink-0">{d.dependents} dependents</span>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export const EntityNode = memo(EntityNodeInner);
