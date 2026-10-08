"use client";
import { formatUtc as utc } from "@/lib/utils";
import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  api,
  ApiRequestError,
  params,
  type Page,
  type Thread,
  type ThreadMemberPage,
  type Scope,
  type SnapshotHit,
} from "@/lib/api";
import { memberLabel, threadSearchBody, summaryState } from "@/lib/thread";
import { defaultThreadView, type ThreadView } from "@/lib/view-state";

const MEMBER_PAGE = 20;

/**
 * Browse the Threads of the selected namespace: membership timelines, the
 * summary the API allows for the selected time, and keyword search over one
 * Thread's member observations. Read-only; summaries are requested through the
 * API with the write token.
 */
export function ThreadPanel({
  scope,
  view,
  onChange,
  onClose,
}: {
  scope: Scope;
  /** Browser state owned by the page, so it survives refreshes and time changes. */
  view: ThreadView;
  onChange: (update: (view: ThreadView) => ThreadView) => void;
  onClose: () => void;
}) {
  const { selected } = view;
  // Selecting another Thread (or none) starts its search and paging afresh.
  const select = (uuid: string | null) =>
    onChange((v) => ({
      ...defaultThreadView,
      open: v.open,
      listOffset: v.listOffset,
      listScope: v.listScope,
      selected: uuid,
    }));
  return (
    <aside aria-label="Threads" className="graph-panel">
      <button className="icon-button float-right" aria-label="Close threads" onClick={onClose}>
        ×
      </button>
      <div className="text-xs text-accent">THREADS</div>
      {!scope.namespace ? (
        <p className="mt-3 text-sm text-muted">
          Select a namespace to browse its Threads. A Thread is a named timeline of source observations;
          its name is unique only inside a namespace.
        </p>
      ) : selected ? (
        <ThreadDetail
          uuid={selected}
          scope={scope}
          view={view}
          onChange={onChange}
          onBack={() => select(null)}
        />
      ) : (
        <ThreadList scope={scope} onSelect={select} view={view} onChange={onChange} />
      )}
    </aside>
  );
}

function ThreadList({
  scope,
  onSelect,
  view,
  onChange,
}: {
  scope: Scope;
  onSelect: (uuid: string) => void;
  view: ThreadView;
  onChange: (update: (v: ThreadView) => ThreadView) => void;
}) {
  const scopeKey = JSON.stringify(scope);
  const offset = view.listScope === scopeKey ? (view.listOffset ?? 0) : 0;
  const setOffset = (offset: number) =>
    onChange((v) => ({ ...v, listOffset: offset, listScope: scopeKey }));
  const list = useQuery({
    queryKey: ["threads", scope, offset],
    queryFn: ({ signal }) =>
      api<Page<Thread>>(`threads?${params(scope, { offset, limit: 50 })}`, signal),
  });
  return (
    <>
      <h2 className="my-3 pr-10 text-xl font-medium">{scope.namespace}</h2>
      <p className="mb-4 text-xs text-muted">
        {scope.as_of
          ? `Threads whose earliest observation was captured by ${utc(scope.as_of)}.`
          : "Every Thread in this namespace, by name."}
      </p>
      {list.isPending && <p>Loading Threads…</p>}
      {list.error && <p role="alert">{list.error.message}</p>}
      {list.data && !list.data.items.length && <p>No Threads in this scope.</p>}
      {list.data?.items.map((thread) => (
        <button
          key={thread.uuid}
          data-testid="thread-item"
          className="entity-link"
          onClick={() => onSelect(thread.uuid)}
        >
          {thread.name}
          <br />
          <small className="text-muted">
            {thread.total_members} member{thread.total_members === 1 ? "" : "s"} ·{" "}
            {summaryState(thread).label.toLowerCase()}
          </small>
        </button>
      ))}
      <div className="mt-3 flex gap-2">
        {offset > 0 && (
          <button onClick={() => setOffset(Math.max(0, offset - 50))}>Previous</button>
        )}
        {list.data?.truncated && list.data.next_offset !== null && (
          <button onClick={() => setOffset(list.data!.next_offset!)}>Next page</button>
        )}
      </div>
    </>
  );
}

function ThreadDetail({
  uuid,
  scope,
  view,
  onChange,
  onBack,
}: {
  uuid: string;
  scope: Scope;
  view: ThreadView;
  onChange: (update: (view: ThreadView) => ThreadView) => void;
  onBack: () => void;
}) {
  const detail = useQuery({
    queryKey: ["thread", uuid, scope],
    queryFn: ({ signal }) => api<Thread>(`threads/${uuid}?${params(scope)}`, signal),
  });
  // Member pages are keyed by the ordinal cursor the API returns; ordinals never
  // change, so the saved cursor trail stays valid across refreshes.
  const cursors = view.cursors.length ? view.cursors : [0];
  const setCursors = (next: number[]) => onChange((v) => ({ ...v, cursors: next }));
  const after = cursors[cursors.length - 1];
  const members = useQuery({
    queryKey: ["thread-members", uuid, scope, after],
    queryFn: ({ signal }) =>
      api<ThreadMemberPage>(
        `threads/${uuid}/members?${params(scope, { after_ordinal: after, limit: MEMBER_PAGE })}`,
        signal,
      ),
  });
  const query = view.query;
  const setQuery = (next: string) => onChange((v) => ({ ...v, query: next }));
  const [debounced, setDebounced] = useState(query.trim());
  useEffect(() => {
    const timer = setTimeout(() => setDebounced(query.trim()), 300);
    return () => clearTimeout(timer);
  }, [query]);
  const search = useQuery({
    queryKey: ["thread-search", uuid, scope, debounced],
    queryFn: ({ signal }) =>
      api<{ snapshots: SnapshotHit[]; truncated: boolean }>(
        "search",
        signal,
        threadSearchBody(debounced, scope.namespace, scope.as_of, uuid),
      ),
    enabled: debounced.length > 0,
    retry: false,
  });
  const thread = detail.data;
  const summary = thread ? summaryState(thread) : null;
  return (
    <>
      <button className="mt-2 text-xs" onClick={onBack}>
        ← All Threads
      </button>
      {detail.isPending && <p className="mt-3">Loading Thread…</p>}
      {detail.error && (
        <p role="alert" className="mt-3 text-sm">
          {detail.error instanceof ApiRequestError && detail.error.status === 404
            ? "This Thread has no observation captured by the selected time. Choose a later time or use Current."
            : detail.error.message}
        </p>
      )}
      {thread && summary && (
        <>
          <h2 className="my-3 pr-10 break-words text-xl font-medium">{thread.name}</h2>
          <div className="mb-4 border border-border bg-raised p-3 text-xs leading-6">
            <div className="break-all">Thread ID: {thread.uuid}</div>
            <div>Earliest observation: {utc(thread.earliest_captured_at)}</div>
            <div>Created by observation at: {utc(thread.created_at)}</div>
            <div>Members: {thread.total_members}</div>
          </div>
          <section aria-label="Thread summary" className="mb-4">
            <h3 className="text-xs text-accent">{summary.label.toUpperCase()}</h3>
            <p className="mt-1 text-xs text-muted">{summary.detail}</p>
            {summary.shown && thread.summary && (
              <details className="mt-2" open={thread.summary.length < 1200}>
                <summary className="text-xs text-muted">
                  {thread.summary.length.toLocaleString()} characters ·{" "}
                  {thread.summary_supporting_snapshot_uuids.length} supporting snapshot
                  {thread.summary_supporting_snapshot_uuids.length === 1 ? "" : "s"} · summarized{" "}
                  {utc(thread.summarized_at)}
                </summary>
                <pre className="mt-2 max-h-96 overflow-auto text-xs whitespace-pre-wrap">
                  {thread.summary}
                </pre>
              </details>
            )}
          </section>
          <section aria-label="Thread members">
            <h3 className="text-xs text-accent">MEMBERS IN ORDER</h3>
            {members.isPending && <p>Loading members…</p>}
            {members.error && <p role="alert">{members.error.message}</p>}
            {members.data && !members.data.items.length && (
              <p className="text-xs text-muted">
                {after > 0 ? "No more members." : "No members captured by the selected time."}
              </p>
            )}
            {members.data?.items.map((member) => (
              <div
                key={member.snapshot_uuid}
                data-testid="thread-member"
                className="my-2 border border-border bg-raised p-3 text-xs leading-6"
              >
                <div>
                  <span className="text-accent">#{member.ordinal}</span>{" "}
                  <span className="break-words">{memberLabel(member)}</span>
                </div>
                <div className="text-muted">Captured {utc(member.captured_at)}</div>
                {member.snapshot_source && (
                  <div className="text-muted">Source: {member.snapshot_source}</div>
                )}
                <div className="break-all text-faint">Snapshot {member.snapshot_uuid}</div>
                {member.previous_snapshot_uuid && (
                  <div className="break-all text-faint">
                    Follows {member.previous_snapshot_uuid}
                  </div>
                )}
              </div>
            ))}
            <div className="mt-3 flex gap-2">
              {cursors.length > 1 && (
                <button onClick={() => setCursors(cursors.slice(0, -1))}>Previous</button>
              )}
              {members.data?.truncated && members.data.next_after_ordinal !== null && (
                <button onClick={() => setCursors([...cursors, members.data!.next_after_ordinal!])}>
                  Next page
                </button>
              )}
            </div>
          </section>
          <section aria-label="Search this thread" className="mt-4">
            <h3 className="text-xs text-accent">SEARCH MEMBER OBSERVATIONS</h3>
            <input
              className="search-input mt-2"
              aria-label="Search thread observations"
              placeholder="Keyword in this Thread's observations…"
              maxLength={2000}
              value={query}
              onChange={(e) => setQuery(e.target.value)}
            />
            {search.isFetching && <p className="mt-2 text-xs">Searching…</p>}
            {search.error && (
              <p role="alert" className="mt-2 text-xs">
                {search.error.message}
              </p>
            )}
            {search.data && debounced && !search.data.snapshots.length && (
              <p className="mt-2 text-xs text-muted">No member observation matched.</p>
            )}
            {search.data?.snapshots.map((hit) => (
              <div
                key={hit.uuid}
                data-testid="thread-hit"
                className="my-2 border border-border bg-raised p-3 text-xs leading-6"
              >
                <div className="break-words">{hit.name}</div>
                <div className="text-muted">Captured {utc(hit.captured_at)}</div>
                <pre className="mt-1 max-h-40 overflow-auto whitespace-pre-wrap text-faint">
                  {hit.content.slice(0, 400)}
                  {hit.content.length > 400 || hit.content_truncated ? "…" : ""}
                </pre>
              </div>
            ))}
          </section>
        </>
      )}
    </>
  );
}
