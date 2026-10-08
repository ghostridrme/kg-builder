"use client";
import { RecordDetails } from "./record-details";
import { Children, useContext, useEffect, useRef, type ReactNode } from "react";
import { type DetailView } from "@/lib/view-state";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import {
  api,
  params,
  entityPath,
  type Entity,
  type TypedEntity,
  type Neighbor,
  type Page,
  type Scope,
} from "@/lib/api";
export { PanelView } from "./detail-view";
import { PanelView, ViewContext } from "./detail-view";
function Disclosure({
  id,
  initialOpen = false,
  className,
  children,
}: {
  id: string;
  initialOpen?: boolean;
  className?: string;
  children: ReactNode;
}) {
  const { view, onChange } = useContext(ViewContext);
  const open = view.sections[id] ?? initialOpen;
  return (
    <details
      className={className}
      open={open}
      onToggle={(event) => {
        if (event.target !== event.currentTarget) return;
        const next = event.currentTarget.open;
        if (next !== open) onChange({ ...view, sections: { ...view.sections, [id]: next } });
      }}
    >
      {Children.toArray(children).map((child, index) => (index === 0 || open ? child : null))}
    </details>
  );
}
function PropertyValue({ value }: { value: unknown }) {
  return <pre>{typeof value === "object" ? JSON.stringify(value, null, 2) : String(value)}</pre>;
}
function PropertyRows({ entries, prefix }: { entries: [string, unknown][]; prefix: string }) {
  return (
    <>
      {entries.map(([key, value]) => (
        <Disclosure
          key={key}
          id={`property:${prefix}:${key}`}
          initialOpen={typeof value !== "object" && String(value).length < 160}
        >
          <summary className="break-words text-xs text-muted">{key}</summary>
          <PropertyValue value={value} />
        </Disclosure>
      ))}
    </>
  );
}
export function Properties({ value }: { value: Record<string, unknown> }) {
  const prefix = String(value.uuid ?? value.chain_id ?? "record");
  const properties = Object.entries((value.properties ?? {}) as Record<string, unknown>).sort(
    ([a], [b]) => a.localeCompare(b),
  );
  const metadata = Object.entries((value.metadata ?? {}) as Record<string, unknown>)
    .filter(([, value]) => value !== null)
    .sort(([a], [b]) => a.localeCompare(b));
  if (!properties.length) return <PropertyRows entries={metadata} prefix={prefix} />;
  return (
    <>
      <PropertyRows entries={properties} prefix={prefix} />
      <Disclosure id={`metadata:${prefix}`} className="mt-4">
        <summary>Record metadata</summary>
        <PropertyRows entries={metadata} prefix={prefix} />
      </Disclosure>
    </>
  );
}
export function Details({
  entity,
  scope,
  onNavigate,
  onClose,
  view,
  onViewChange,
}: {
  entity: Entity;
  scope: Scope;
  onNavigate: (e: Entity) => void;
  onClose: () => void;
  view: DetailView;
  onViewChange: (view: DetailView) => void;
}) {
  const { tab, offset } = view;
  const panel = useRef<HTMLElement | null>(null);
  const restored = useRef(false);
  const detail = useQuery({
    queryKey: ["entity", entity.chain_id, scope],
    queryFn: ({ signal }) =>
      api<TypedEntity>(
        `${entityPath(entity)}?${params(scope, { representation: "overview" })}`,
        signal,
      ),
    // Keep the open panel's current data on screen while a scope refresh (e.g. an
    // advanced as-of time) refetches, so the details do not blank out and reload.
    placeholderData: keepPreviousData,
  });
  const rows = useQuery({
    queryKey: ["detail", entity.chain_id, scope, tab, offset],
    queryFn: ({ signal }) =>
      api<Page<TypedEntity | Neighbor>>(
        `${entityPath(entity)}/${tab === "history" ? "versions" : "neighbors"}?${params(scope, { offset, limit: 50, representation: "overview" })}`,
        signal,
      ),
    enabled: tab !== "properties",
    // Only a refresh of this page may retain its rows. History and neighbors
    // have different shapes; reusing history as neighbors can crash rendering.
    placeholderData: (previous, query) =>
      query?.queryKey[1] === entity.chain_id &&
      query.queryKey[3] === tab &&
      query.queryKey[4] === offset
        ? previous
        : undefined,
  });
  useEffect(() => {
    if (
      !restored.current &&
      !detail.isPending &&
      (tab === "properties" || !rows.isPending) &&
      panel.current
    ) {
      panel.current.scrollTop = view.scrollTop;
      restored.current = true;
    }
  }, [detail.isPending, rows.isPending, tab, view.scrollTop]);
  return (
    <PanelView view={view} onChange={onViewChange}>
      <aside
        ref={panel}
        aria-label="Entity details"
        className="graph-panel"
        onScroll={(event) => {
          if (restored.current && Math.abs(view.scrollTop - event.currentTarget.scrollTop) > 1)
            onViewChange({ ...view, scrollTop: event.currentTarget.scrollTop });
        }}
      >
        <button className="icon-button float-right" aria-label="Close details" onClick={onClose}>
          ×
        </button>
        <div className="text-xs text-accent">ENTITY</div>
        <h2 className="my-3 pr-10 break-words text-xl font-medium">{entity.name}</h2>
        <p className="mb-4 text-xs text-muted">
          {entity.entity_type} · {entity.namespace}
        </p>
        {detail.data && (
          <div className="mb-4 border border-border bg-raised p-3 text-xs leading-6">
            <div>
              Source:{" "}
              {String((detail.data.metadata as Record<string, unknown>)?.source ?? "unknown")}
            </div>
            <details>
              <summary>Identifiers</summary>
              <div className="break-all">Chain: {detail.data.chain_id}</div>
              <button onClick={() => void navigator.clipboard.writeText(detail.data!.chain_id)}>
                Copy chain ID
              </button>
            </details>
            <div>Version: {String(detail.data.version ?? "unknown")}</div>
            <div className="break-all">Version ID: {String(detail.data.uuid ?? "unknown")}</div>
            <div>
              Valid from:{" "}
              {String((detail.data.metadata as Record<string, unknown>)?.valid_from ?? "unknown")}
            </div>
            {((detail.data.metadata as Record<string, unknown>)?.valid_to ||
              detail.data.deleted_at) && (
              <div>
                Valid until:{" "}
                {String(
                  (detail.data.metadata as Record<string, unknown>)?.valid_to ??
                    detail.data.deleted_at,
                )}
              </div>
            )}
          </div>
        )}
        <nav className="panel-tabs">
          {["properties", "history", "dependencies"].map((t) => (
            <button
              key={t}
              aria-pressed={tab === t}
              onClick={() => {
                onViewChange({
                  ...view,
                  tab: t as DetailView["tab"],
                  offset: 0,
                  scrollTop: 0,
                });
                if (panel.current) panel.current.scrollTop = 0;
              }}
              className={tab === t ? "text-accent" : ""}
            >
              {t === "dependencies" ? "relationships" : t}
            </button>
          ))}
        </nav>
        {(detail.error || rows.error) && (
          <p role="alert">{(detail.error || rows.error)?.message}</p>
        )}
        {tab === "properties" ? (
          detail.isPending ? (
            <p>Loading properties…</p>
          ) : (
            detail.data && (
              <RecordDetails
                key={`${entity.chain_id}:${scope.as_of}`}
                scope={scope}
                identity={{
                  kind: "entity",
                  chain_id: entity.chain_id,
                  entity_type: entity.entity_type,
                }}
              />
            )
          )
        ) : rows.isPending ? (
          <p>Loading {tab}…</p>
        ) : (
          <>
            {!rows.data?.items.length && (
              <p>No {tab === "dependencies" ? "relationships" : tab} in this scope.</p>
            )}
            {rows.data?.items.map((r, i) =>
              tab === "history" ? (
                <Disclosure
                  id={`version:${(r as Entity).uuid}`}
                  key={String((r as Entity).uuid) || i}
                >
                  <summary>
                    Version {(r as Entity).version}{" "}
                    {(r as Entity).deleted_at
                      ? "· deleted"
                      : (r as Entity).is_latest
                        ? "· latest"
                        : ""}
                  </summary>
                  <RecordDetails
                    scope={scope}
                    identity={{ kind: "version", uuid: String((r as Entity).uuid) }}
                  />
                </Disclosure>
              ) : (
                <button
                  key={(r as Neighbor).edge_id}
                  className="entity-link"
                  onClick={() => onNavigate((r as Neighbor).entity)}
                >
                  <small className="text-accent">
                    {(r as Neighbor).src_chain === entity.chain_id ? "→ " : "← "}
                    {(r as Neighbor).via}
                  </small>
                  <br />
                  {(r as Neighbor).entity.name}
                </button>
              ),
            )}
            <div className="mt-3 flex gap-2">
              {offset > 0 && (
                <button
                  onClick={() =>
                    onViewChange({
                      ...view,
                      offset: Math.max(0, offset - 50),
                      scrollTop: 0,
                    })
                  }
                >
                  Previous
                </button>
              )}
              {rows.data?.truncated && (
                <button
                  onClick={() =>
                    onViewChange({
                      ...view,
                      offset: rows.data!.next_offset!,
                      scrollTop: 0,
                    })
                  }
                >
                  Next page
                </button>
              )}
            </div>
          </>
        )}
      </aside>
    </PanelView>
  );
}
