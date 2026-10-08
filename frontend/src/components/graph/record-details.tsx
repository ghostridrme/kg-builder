"use client";
import { useState, useContext } from "react";
import { ViewContext } from "./detail-view";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api, type Scope, params } from "@/lib/api";
type Item = {
  name: string;
  group: string;
  path: string;
  deferred: boolean;
  value: unknown;
};
type Fields = {
  revision: string;
  items: Item[];
  total_fields: number;
  next_offset: number | null;
};
function useRecordPreferences(identity: Record<string, string>) {
  const { view, onChange } = useContext(ViewContext);
  const key = JSON.stringify(identity);
  const [local, setLocal] = useState({
    offset: 0,
    trail: [] as number[],
    revision: "",
    open: [] as string[],
  });
  const prefs = view.records?.[key] ?? local;
  const update = (patch: Partial<typeof prefs>) => {
    const next = { ...prefs, ...patch };
    setLocal(next);
    onChange({
      ...view,
      records: Object.fromEntries([
        ...Object.entries(view.records ?? {})
          .filter(([k]) => k !== key)
          .slice(-49),
        [key, next],
      ]),
    });
  };
  return [prefs, update] as const;
}
/** Exact values are fetched only after expansion; continuations pin record content. */
export function RecordDetails({
  scope,
  identity,
}: {
  scope: Scope;
  identity: Record<string, string>;
}) {
  const [prefs, update] = useRecordPreferences(identity);
  const { offset, revision } = prefs;
  const path = `details?${params(scope, { ...identity, offset, ...(revision ? { revision } : {}) })}`;
  const result = useQuery({
    queryKey: ["record-fields", path],
    queryFn: ({ signal }) => api<Fields>(path, signal),
  });
  if (result.error)
    return (
      <p role="alert">
        {result.error.message}{" "}
        <button
          onClick={() => {
            update({ offset: 0, trail: [], revision: "", open: [] });
            if (offset === 0 && !revision) void result.refetch();
          }}
        >
          Reload details
        </button>
      </p>
    );
  if (!result.data) return <p role="status">Loading fields…</p>;
  const page = result.data;
  return (
    <div className="record-fields">
      {page.items.map((item) => (
        <Field
          key={`${page.revision}:${item.path}`}
          item={item}
          scope={scope}
          identity={identity}
          revision={page.revision}
          open={prefs.open.includes(item.path)}
          onOpen={(open) =>
            update({
              open: open
                ? [...prefs.open.filter((p) => p !== item.path), item.path].slice(-200)
                : prefs.open.filter((p) => p !== item.path),
            })
          }
        />
      ))}
      <div className="flex gap-3 my-4">
        {offset > 0 && (
          <button
            onClick={() => {
              update({
                offset: prefs.trail.at(-1) ?? 0,
                trail: prefs.trail.slice(0, -1),
              });
            }}
          >
            Previous fields
          </button>
        )}
        {page.next_offset !== null && (
          <button
            onClick={() => {
              update({
                revision: page.revision,
                trail: [...prefs.trail, offset].slice(-200),
                offset: page.next_offset!,
              });
            }}
          >
            More fields
          </button>
        )}
      </div>
      <p className="text-xs text-muted">{page.total_fields} fields · exact record values</p>
    </div>
  );
}
function Field({
  item,
  scope,
  identity,
  revision,
  open,
  onOpen,
}: {
  item: Item;
  scope: Scope;
  identity: Record<string, string>;
  revision: string;
  open: boolean;
  onOpen: (open: boolean) => void;
}) {
  return (
    <details
      open={open}
      onToggle={(e) => {
        if (e.target === e.currentTarget && open !== e.currentTarget.open)
          onOpen(e.currentTarget.open);
      }}
    >
      <summary className="break-words text-xs text-muted">
        {item.name}
        <span className="ml-2 opacity-60">{item.group === "metadata" ? "metadata" : ""}</span>
      </summary>
      {open &&
        (item.deferred ? (
          <FieldRange scope={scope} identity={identity} revision={revision} path={item.path} />
        ) : (
          <pre className="max-h-80 overflow-auto whitespace-pre-wrap break-all text-xs">
            {typeof item.value === "object"
              ? JSON.stringify(item.value, null, 2)
              : String(item.value)}
          </pre>
        ))}
    </details>
  );
}
function FieldRange({
  scope,
  identity,
  revision,
  path,
}: {
  scope: Scope;
  identity: Record<string, string>;
  revision: string;
  path: string;
}) {
  const client = useQueryClient();
  const [prefs, update] = useRecordPreferences({
    ...identity,
    field: path,
    revision,
  });
  const { offset: start, trail } = prefs;
  const url = `details?${params(scope, { ...identity, revision, path, start })}`;
  const result = useQuery({
    queryKey: ["record-range", url],
    queryFn: ({ signal }) =>
      api<{
        content: string;
        next_start: number | null;
        total_characters: number;
      }>(url, signal),
  });
  return (
    <div>
      {result.error ? (
        <p role="alert">
          {result.error.message}{" "}
          <button
            onClick={() => {
              update({ offset: 0, trail: [] });
              void client.invalidateQueries({ queryKey: ["record-fields"] });
              void result.refetch();
            }}
          >
            Reload value
          </button>
        </p>
      ) : result.data ? (
        <>
          <pre className="max-h-80 overflow-auto whitespace-pre-wrap break-all text-xs">
            {result.data.content}
          </pre>
          <p className="text-xs text-muted">
            JSON characters {start + 1}–{start + [...result.data.content].length} of{" "}
            {result.data.total_characters}
          </p>
          <div className="flex gap-2">
            {trail.length > 0 && (
              <button
                onClick={() => {
                  update({ offset: trail.at(-1)!, trail: trail.slice(0, -1) });
                }}
              >
                Previous part
              </button>
            )}
            {result.data.next_start !== null && (
              <button
                onClick={() => {
                  update({
                    trail: [...trail, start].slice(-200),
                    offset: result.data!.next_start!,
                  });
                }}
              >
                Next part
              </button>
            )}
          </div>
        </>
      ) : (
        <p role="status">Loading exact value…</p>
      )}
    </div>
  );
}
