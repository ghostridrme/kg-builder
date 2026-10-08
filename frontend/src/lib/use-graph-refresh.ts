"use client";
import { useCallback, useEffect, useRef, useState } from "react";
import { api } from "./api";

const REFRESH_INTERVAL = 15_000;
const REFRESH_FALLBACK = 60_000;

export function useGraphRefresh({
  viewKey,
  enabled,
  current,
  prepare,
}: {
  viewKey: string;
  enabled: boolean;
  current: boolean;
  prepare: (signal: AbortSignal) => Promise<() => void>;
}) {
  const prepareRef = useRef(prepare);
  useEffect(() => {
    prepareRef.current = prepare;
  }, [prepare]);
  const manual = useRef<() => void>(() => {});
  const [status, setStatus] = useState<"idle" | "checking" | "delayed">("idle");
  useEffect(() => {
    if (!enabled) {
      manual.current = () => {};
      return;
    }
    let disposed = false,
      running = false,
      revision: string | undefined;
    queueMicrotask(() => {
      if (!disposed) setStatus("idle");
    });
    let lastRead = 0,
      failures = 0,
      pointerDown = false,
      quietUntil = 0;
    let timer: ReturnType<typeof setTimeout>;
    let controller: AbortController | undefined;
    const down = () => {
      pointerDown = true;
    };
    const up = () => {
      pointerDown = false;
      quietUntil = Date.now() + 600;
    };
    const wheel = () => {
      quietUntil = Date.now() + 600;
    };
    const waitForIdle = async (signal: AbortSignal) => {
      while (pointerDown || Date.now() < quietUntil || document.hidden) {
        signal.throwIfAborted();
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      signal.throwIfAborted();
    };
    const schedule = () => {
      clearTimeout(timer);
      if (current && !disposed)
        timer = setTimeout(
          () => void run(false),
          Math.min(120_000, REFRESH_INTERVAL * 2 ** failures),
        );
    };
    const run = async (force: boolean) => {
      if (
        disposed ||
        running ||
        document.hidden ||
        (!force && (pointerDown || Date.now() < quietUntil))
      ) {
        schedule();
        return;
      }
      running = true;
      controller = new AbortController();
      const signal = AbortSignal.any([controller.signal, AbortSignal.timeout(60_000)]);
      setStatus("checking");
      try {
        // Read BEFORE the graph: a commit during the fetch is detected next poll.
        const next = JSON.stringify(
          (await api<{ revision: unknown }>("graph/revision", signal)).revision,
        );
        if (force || revision !== next || Date.now() - lastRead >= REFRESH_FALLBACK) {
          const commit = await prepareRef.current(signal);
          await waitForIdle(signal);
          if (disposed) return;
          commit();
          lastRead = Date.now();
        }
        revision = next;
        failures = 0;
        setStatus("idle");
      } catch {
        if (!disposed) {
          if (controller.signal.aborted) setStatus("idle");
          else {
            failures = Math.min(failures + 1, 3);
            setStatus("delayed");
          }
        }
      } finally {
        running = false;
        schedule();
      }
    };
    const visible = () => {
      if (document.hidden) controller?.abort();
      else if (current) void run(false);
    };
    document.addEventListener("visibilitychange", visible);
    window.addEventListener("pointerdown", down);
    window.addEventListener("pointerup", up);
    window.addEventListener("pointercancel", up);
    window.addEventListener("blur", up);
    window.addEventListener("wheel", wheel, { passive: true });
    manual.current = () => void run(true);
    schedule();
    return () => {
      disposed = true;
      clearTimeout(timer);
      controller?.abort();
      manual.current = () => {};
      document.removeEventListener("visibilitychange", visible);
      window.removeEventListener("pointerdown", down);
      window.removeEventListener("pointerup", up);
      window.removeEventListener("pointercancel", up);
      window.removeEventListener("blur", up);
      window.removeEventListener("wheel", wheel);
    };
  }, [viewKey, enabled, current]);
  return { status, refresh: useCallback(() => manual.current(), []) };
}
