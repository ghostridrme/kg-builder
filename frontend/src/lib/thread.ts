import { formatUtc as utc } from "@/lib/utils";
import type { Thread, ThreadMember } from "./api";

/** What the panel says about a Thread's summary in the current scope. */
export type SummaryState = {
  label: string;
  detail: string;
  /** The summary text is shown only when the API returned it. */
  shown: boolean;
};

/**
 * Explain the API's summary decision in operator terms. The API withholds a
 * summary for a fixed time when the summary accounts for observations captured
 * after that time, so the reader is told which time would show it.
 */
export function summaryState(
  thread: Pick<
    Thread,
    | "summary"
    | "summary_withheld"
    | "summary_covers_members_through"
    | "summary_covers_captured_through"
    | "total_members"
  >,
): SummaryState {
  switch (thread.summary_withheld) {
    case null:
    case undefined: {
      const pending = thread.total_members - thread.summary_covers_members_through;
      const coverage = thread.summary_covers_captured_through
        ? `Covers observations captured through ${utc(thread.summary_covers_captured_through)}.`
        : "";
      const behind =
        pending > 0
          ? ` ${pending} newer member${pending === 1 ? "" : "s"} not yet summarized.`
          : "";
      return { label: "Summary", detail: `${coverage}${behind}`.trim(), shown: true };
    }
    case "not_summarized":
      return {
        label: "No summary yet",
        detail:
          "Summaries are produced on request through the API (POST /api/v1/threads/{uuid}/summary with the write token).",
        shown: false,
      };
    case "covers_later_observations":
      return {
        label: "Summary withheld at this time",
        detail: thread.summary_covers_captured_through
          ? `The stored summary accounts for observations captured through ${utc(thread.summary_covers_captured_through)}, after the selected time. Set As of to that time or later, or use Current.`
          : "The stored summary accounts for observations captured after the selected time.",
        shown: false,
      };
    case "coverage_unknown":
      return {
        label: "Summary withheld",
        detail:
          "The stored summary has no coverage watermark, so it cannot be placed on the timeline for a fixed time. Use Current to read it.",
        shown: false,
      };
  }
}

/** A member's display name: the snapshot name when the store returns one. */
export function memberLabel(member: Pick<ThreadMember, "snapshot_name" | "snapshot_uuid">) {
  return member.snapshot_name?.trim() || member.snapshot_uuid;
}

/** Request body for keyword search restricted to one Thread's member snapshots. */
export function threadSearchBody(
  query: string,
  namespace: string,
  asOf: string,
  threadUuid: string,
  limit = 10,
) {
  return {
    query,
    namespace,
    ...(asOf ? { as_of: asOf } : {}),
    semantic: false,
    thread: { kind: "uuid", uuid: threadUuid },
    limit,
  };
}
