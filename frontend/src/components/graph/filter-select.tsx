"use client";
import { useEffect, useId, useRef, useState } from "react";
/** Searchable, paged choices. The selected value remains valid even in an empty scope. */
export function FilterSelect({
  label,
  value,
  items,
  search,
  onSearch,
  onChange,
  hasMore,
  busy,
  onMore,
  emptyLabel,
}: {
  label: string;
  value: string;
  items: string[];
  search: string;
  onSearch: (s: string) => void;
  onChange: (s: string) => void;
  hasMore: boolean;
  busy: boolean;
  onMore: () => void;
  emptyLabel?: string;
}) {
  const [open, setOpen] = useState(false);
  const root = useRef<HTMLDivElement>(null);
  const button = useRef<HTMLButtonElement>(null);
  const id = useId();
  useEffect(() => {
    const dismiss = (e: PointerEvent) => {
      if (!root.current?.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("pointerdown", dismiss);
    return () => document.removeEventListener("pointerdown", dismiss);
  }, []);
  const choose = (item: string) => {
    onChange(item);
    setOpen(false);
    onSearch("");
    button.current?.focus();
  };
  return (
    <div
      ref={root}
      className="filter-select"
      onKeyDown={(e) => {
        if (e.key === "Escape") {
          if (open) {
            // Close only this popover. Without stopping propagation the same
            // Escape reaches the document-level listener that closes the
            // details panel and clears the selection (as the finder does at
            // search-workspace.tsx). When the popover is already closed, let
            // Escape through so it can still close the panel.
            e.stopPropagation();
          }
          setOpen(false);
          button.current?.focus();
        }
        if ((e.key === "ArrowDown" || e.key === "ArrowUp") && open) {
          const choices = [...root.current!.querySelectorAll<HTMLButtonElement>("[role=option]")];
          const current = choices.indexOf(document.activeElement as HTMLButtonElement);
          e.preventDefault();
          choices[
            (current + (e.key === "ArrowDown" ? 1 : choices.length - 1) + choices.length) %
              choices.length
          ]?.focus();
        }
      }}
    >
      <button
        ref={button}
        type="button"
        aria-label={label}
        aria-haspopup="dialog"
        aria-expanded={open}
        aria-controls={id}
        title={value || emptyLabel}
        onClick={() => setOpen((v) => !v)}
      >
        {value || emptyLabel || "Select…"}
        <span aria-hidden>⌄</span>
      </button>
      {open && (
        <div id={id} role="dialog" aria-label={`${label} options`} className="filter-popover">
          <input
            autoFocus
            aria-label={`Find ${label.toLowerCase()}`}
            placeholder={`Filter ${label.toLowerCase()}…`}
            value={search}
            onChange={(e) => onSearch(e.target.value)}
          />
          <div role="listbox" aria-label={label}>
            {emptyLabel && (
              <button role="option" aria-selected={!value} onClick={() => choose("")}>
                {emptyLabel}
              </button>
            )}
            {items.map((item) => (
              <button
                key={item}
                role="option"
                title={item}
                aria-selected={item === value}
                onClick={() => choose(item)}
              >
                {item}
              </button>
            ))}
          </div>
          {busy && <p role="status">Loading…</p>}
          {hasMore && (
            <button disabled={busy} onClick={onMore}>
              More options
            </button>
          )}
          {!items.length && !busy && <p>No matching options.</p>}
        </div>
      )}
    </div>
  );
}
