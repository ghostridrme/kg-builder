import type { Entity } from "./api";
export type SearchHit = Entity & {
  score: number;
  score_breakdown?: Record<string, number>;
  properties: Record<string, unknown>;
  observation_count: number | null;
  dependent_count: number | null;
  last_changed_at: string | null;
  derived_summary?: { summary?: string };
};
export type RelationshipHit = {
  uuid: string;
  source_chain_id: string;
  target_chain_id: string;
  name: string;
  description: string;
  score: number;
  model_score?: number | null;
  valid_from: string | null;
  valid_to: string | null;
};
export type SearchResult = {
  hits: SearchHit[];
  relationships: RelationshipHit[];
  relationship_entities?: Entity[];
  snapshots: Record<string, unknown>[];
  communities?: {
    uuid: string;
    namespace: string;
    name: string;
    summary: string;
    score: number;
    member_count: number;
  }[];
  diagnostics: { operation: string; status: string }[];
  truncated: boolean;
  approximate: boolean;
  total_candidates: number;
  duration_ms: number;
};
