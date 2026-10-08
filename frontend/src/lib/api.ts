export type Entity = {
  node_kind?: "snapshot";
  snapshot_uuid?: string;
  chain_id: string;
  uuid?: string;
  entity_type: string;
  name: string;
  namespace: string;
  version?: number;
  is_latest?: boolean;
  deleted_at?: string;
  [key: string]: unknown;
};
/** Explicit representation=typed detail/history contract. */
export type TypedEntity = Entity & {
  properties: Record<string, unknown>;
  metadata: Record<string, unknown>;
  diagnostics: { property: string; reason: string }[];
};
export type Neighbor = {
  entity: Entity;
  edge_id: string;
  src_chain: string;
  dst_chain: string;
  via: string;
  relationship: Record<string, unknown>;
};
export type Page<T> = {
  items: T[];
  truncated: boolean;
  next_offset: number | null;
};
export type Scope = { namespace: string; as_of: string };
export type SummaryWithheld = "not_summarized" | "covers_later_observations" | "coverage_unknown";
/** One Thread as the API reports it for the requested scope. */
export type Thread = {
  uuid: string;
  name: string;
  namespace: string;
  created_at: string;
  earliest_captured_at: string;
  total_members: number;
  first_snapshot_uuid: string | null;
  last_snapshot_uuid: string | null;
  summary: string | null;
  summary_withheld: SummaryWithheld | null;
  summary_revision: string | null;
  summary_supporting_snapshot_uuids: string[];
  summary_covers_members_through: number;
  summary_covers_captured_through: string | null;
  summarized_at: string | null;
};
export type ThreadMember = {
  snapshot_uuid: string;
  captured_at: string;
  created_at: string;
  ordinal: number;
  previous_snapshot_uuid: string | null;
  snapshot_name?: string;
  snapshot_source?: string;
};
export type ThreadMemberPage = {
  items: ThreadMember[];
  after_ordinal: number;
  truncated: boolean;
  next_after_ordinal: number | null;
};
export type SnapshotHit = {
  uuid: string;
  name: string;
  source: string;
  namespace: string;
  captured_at: string | null;
  content: string;
  content_truncated: boolean;
  score: number;
};
export class ApiRequestError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
  }
}
export async function api<T>(
  path: string,
  signal?: AbortSignal,
  body?: unknown,
  timeoutMs = 45_000,
): Promise<T> {
  const response = await fetch(`/api/v1/${path}`, {
    signal: signal
      ? AbortSignal.any([signal, AbortSignal.timeout(Math.min(130_000, Math.max(1000, timeoutMs)))])
      : AbortSignal.timeout(Math.min(130_000, Math.max(1000, timeoutMs))),
    headers: body ? { "Content-Type": "application/json" } : undefined,
    method: body ? "POST" : "GET",
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!response.ok) {
    const text = await response.text();
    let message = `Request failed (${response.status})`;
    try {
      message = JSON.parse(text).error || message;
    } catch {}
    throw new ApiRequestError(message, response.status);
  }
  return response.json();
}
export function params(scope: Scope, extra: Record<string, string | number> = {}) {
  const query = new URLSearchParams();
  if (scope.namespace) query.set("namespace", scope.namespace);
  if (scope.as_of) query.set("as_of", scope.as_of);
  for (const [key, value] of Object.entries(extra)) query.set(key, String(value));
  return query.toString();
}
export function entityPath(entity: Pick<Entity, "entity_type" | "chain_id">) {
  return `entities/${encodeURIComponent(entity.entity_type)}/${encodeURIComponent(entity.chain_id)}`;
}
