"use client";
import type { Entity, Scope } from "@/lib/api";
import { RecordDetails } from "./record-details";
export function SnapshotDetails({
  entity,
  scope,
  onClose,
}: {
  entity: Entity;
  scope: Scope;
  onClose: () => void;
}) {
  return (
    <aside className="graph-panel" aria-label="Snapshot details">
      <button
        className="icon-button float-right"
        aria-label="Close snapshot details"
        onClick={onClose}
      >
        ×
      </button>
      <div className="text-xs text-accent">SOURCE SNAPSHOT</div>
      <h2 className="my-3 text-xl">{entity.name}</h2>
      <p className="mb-4 text-sm text-muted">
        A source observation. Its links identify the exact resource versions it observed. Expand a
        field to read retained source content or metadata.
      </p>
      <RecordDetails
        key={`${entity.snapshot_uuid}:${scope.as_of}`}
        scope={scope}
        identity={{ kind: "snapshot", uuid: entity.snapshot_uuid! }}
      />
    </aside>
  );
}
