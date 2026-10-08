"use client";
import { useEffect, useRef, useState, useId, useMemo } from "react";
import { api, type Entity, type Scope } from "@/lib/api";
import type { SearchResult, RelationshipHit } from "@/lib/search";

type Props = {
  query: string;
  onQueryChange: (text: string) => void;
  orgId: string;
  scope: Scope;
  types: string[];
  semanticAvailable: boolean;
  refreshKey: string;
  onSelect: (entity: Entity) => void;
  visibleChains: Set<string>;
  onRelationship: (relation: RelationshipHit, source: Entity, target: Entity) => void;
};
/** Scoped typeahead for entities and relationships on the graph canvas. */
export function GraphFinder({
  query,
  onQueryChange,
  orgId,
  scope,
  types,
  semanticAvailable,
  refreshKey,
  onSelect,
  visibleChains,
  onRelationship,
}: Props) {
  const signature = JSON.stringify({ orgId, scope, types });
  const recipe = semanticAvailable ? "hybrid" : "keyword";
  const searchKey = JSON.stringify({
    query: query.trim(),
    recipe,
    signature,
    refreshKey: scope.as_of ? "" : refreshKey,
  });
  const [result, setResult] = useState<SearchResult | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [open, setOpen] = useState(false);
  const section = useRef<HTMLElement>(null);
  const input = useRef<HTMLInputElement>(null);
  const resultsId = useId();
  const completed = useRef<string | undefined>(undefined);
  const [previous, setPrevious] = useState(searchKey);
  if (previous !== searchKey) {
    setPrevious(searchKey);
    setBusy(false);
    setResult(null);
    setError("");
  }
  useEffect(() => {
    const dismiss = (event: PointerEvent) => {
      if (!section.current?.contains(event.target as Node)) setOpen(false);
    };
    document.addEventListener("pointerdown", dismiss);
    return () => document.removeEventListener("pointerdown", dismiss);
  }, []);
  useEffect(() => {
    if (query.trim().length < 2 || (completed.current === searchKey && result !== null)) return;
    const controller = new AbortController();
    const timer = setTimeout(async () => {
      setBusy(true);
      setError("");
      setOpen(true);
      try {
        const scoped = JSON.parse(signature);
        const found = await api<SearchResult>(
          "search",
          controller.signal,
          {
            query: query.trim(),
            namespace: scoped.scope.namespace || null,
            as_of: scoped.scope.as_of || null,
            entity_types: scoped.types,
            recipe,
            include_relationships: true,
            include_evidence: false,
            include_signals: false,
            limit: 10,
          },
          45_000,
        );
        if (!controller.signal.aborted) {
          completed.current = searchKey;
          setResult(found);
        }
      } catch (e) {
        if (!controller.signal.aborted) setError(e instanceof Error ? e.message : "Search failed");
      } finally {
        if (!controller.signal.aborted) setBusy(false);
      }
    }, 400);
    return () => {
      clearTimeout(timer);
      controller.abort();
    };
  }, [query, recipe, signature, searchKey, result]);
  const endpoints = useMemo(() => {
    const indexed = new Map<string, Entity>();
    // Preserve API endpoint precedence and the first record for each chain.
    for (const entity of [...(result?.relationship_entities ?? []), ...(result?.hits ?? [])]) {
      if (!indexed.has(entity.chain_id)) indexed.set(entity.chain_id, entity);
    }
    return indexed;
  }, [result]);
  return (
    <section
      ref={section}
      className="graph-finder"
      aria-label="Graph entity finder"
      onKeyDown={(e) => {
        if (e.key === "Escape") {
          e.stopPropagation();
          input.current?.focus();
          setOpen(false);
        }
        if (!(e.target instanceof HTMLButtonElement) || !["ArrowDown", "ArrowUp"].includes(e.key))
          return;
        const buttons = [
          ...e.currentTarget.querySelectorAll<HTMLButtonElement>(
            ".finder-result button:not(:disabled)",
          ),
        ];
        const i = buttons.indexOf(e.target);
        if (i < 0) return;
        e.preventDefault();
        buttons[(i + (e.key === "ArrowDown" ? 1 : buttons.length - 1)) % buttons.length]?.focus();
      }}
    >
      <form className="search-form" onSubmit={(e) => e.preventDefault()}>
        <div className="flex gap-2">
          <input
            ref={input}
            role="combobox"
            aria-expanded={open}
            aria-controls={resultsId}
            aria-autocomplete="list"
            aria-haspopup="dialog"
            aria-label="Find entities or relationships"
            className="search-input"
            maxLength={2000}
            placeholder="Find entities or relationships…"
            value={query}
            onFocus={() => setOpen(true)}
            onChange={(e) => {
              onQueryChange(e.target.value);
              setOpen(true);
            }}
            onKeyDown={(e) => {
              if (e.key === "Escape") setOpen(false);
              if (e.key === "ArrowDown") {
                e.preventDefault();
                section.current
                  ?.querySelector<HTMLButtonElement>(".finder-result button:not(:disabled)")
                  ?.focus();
              }
            }}
          />
        </div>
      </form>
      {open && (result || busy || error) && (
        <div
          id={resultsId}
          role="dialog"
          aria-label="Search suggestions"
          className="finder-dropdown"
        >
          <div className="flex justify-end">
            <button aria-label="Close search results" onClick={() => setOpen(false)}>
              ×
            </button>
          </div>
          {busy && (
            <p role="status" className="p-3 text-sm text-muted">
              Searching…
            </p>
          )}
          {error && (
            <p role="alert" className="p-3 text-sm text-red-400">
              {error}
            </p>
          )}
          {result && (
            <>
              {(result.truncated || result.diagnostics.some((d) => d.status === "failed")) && (
                <p role="status" className="p-3 text-sm text-accent">
                  Results may be incomplete.{" "}
                  {result.truncated
                    ? "Retrieval reached a limit."
                    : "One or more search operations failed."}
                </p>
              )}
              {!result.hits.length && !result.relationships.length && (
                <p className="p-3 text-sm text-muted">
                  No matching entities or relationships. Try different words or widen your filters.
                </p>
              )}
              {result.hits.map((hit) => (
                <article key={hit.chain_id} className="finder-result">
                  <button
                    className="w-full text-left"
                    onClick={() => {
                      onSelect?.(hit);
                      setOpen(false);
                    }}
                  >
                    <strong>{hit.name}</strong>
                    <span className="mt-1 block text-xs text-muted">
                      {hit.entity_type} · Score {hit.score.toPrecision(4)} ·{" "}
                      {visibleChains?.has(hit.chain_id) ? "Show in graph" : "Open neighborhood"}
                    </span>
                  </button>
                </article>
              ))}
              {result.relationships.length > 0 && (
                <h2 className="px-3 py-2 text-xs uppercase tracking-wide text-muted">
                  Relationships
                </h2>
              )}
              {result.relationships.map((relation) => {
                const source = endpoints.get(relation.source_chain_id),
                  target = endpoints.get(relation.target_chain_id);
                const text = `${source?.name ?? relation.source_chain_id} → ${relation.name} → ${target?.name ?? relation.target_chain_id}`;
                return (
                  <article key={relation.uuid} className="finder-result">
                    <button
                      className="w-full text-left"
                      disabled={!source || !target}
                      onClick={() => {
                        if (source && target) {
                          onRelationship?.(relation, source, target);
                          setOpen(false);
                        }
                      }}
                    >
                      <strong>{text}</strong>
                      <span className="mt-1 block text-xs text-muted">
                        Score {relation.score.toPrecision(4)} ·{" "}
                        {relation.description || "Open connection in graph"}
                      </span>
                    </button>

                    {(!source || !target) && (
                      <p className="text-xs text-muted">
                        An endpoint is no longer visible in this scope. Search again to refresh.
                      </p>
                    )}
                  </article>
                );
              })}
            </>
          )}
        </div>
      )}
    </section>
  );
}
