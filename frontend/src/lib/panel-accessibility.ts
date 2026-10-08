"use client";
import { useEffect, useRef } from "react";
/** Narrow inspection panels behave as sheets; desktop panels remain nonmodal. */
export function usePanelAccessibility(identity: string, onClose: () => void) {
  const close = useRef(onClose);
  useEffect(() => {
    close.current = onClose;
  }, [onClose]);
  useEffect(() => {
    if (!identity) return;
    const panel = document.querySelector<HTMLElement>(".graph-panel");
    if (!panel) return;
    const previous = document.activeElement as HTMLElement | null;
    const narrow = window.matchMedia("(max-width: 767px)");
    const configure = () => {
      if (narrow.matches) {
        panel.setAttribute("role", "dialog");
        panel.setAttribute("aria-modal", "true");
        panel.tabIndex = -1;
        panel.focus();
      } else {
        panel.removeAttribute("role");
        panel.removeAttribute("aria-modal");
      }
    };
    const keyboard = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.preventDefault();
        close.current();
        return;
      }
      if (!narrow.matches || e.key !== "Tab") return;
      const focusable = [
        ...panel.querySelectorAll<HTMLElement>(
          'button:not(:disabled),a[href],input:not(:disabled),select:not(:disabled),summary,[tabindex="0"]',
        ),
      ].filter((el) => el.getClientRects().length > 0);
      const first = focusable[0],
        last = focusable.at(-1);
      if (!first) {
        e.preventDefault();
        panel.focus();
        return;
      }
      if (
        e.shiftKey &&
        (document.activeElement === first || !panel.contains(document.activeElement))
      ) {
        e.preventDefault();
        last?.focus();
      } else if (
        !e.shiftKey &&
        (document.activeElement === last ||
          !panel.contains(document.activeElement) ||
          document.activeElement === panel)
      ) {
        e.preventDefault();
        first.focus();
      }
    };
    configure();
    narrow.addEventListener("change", configure);
    document.addEventListener("keydown", keyboard);
    return () => {
      narrow.removeEventListener("change", configure);
      document.removeEventListener("keydown", keyboard);
      if (previous?.isConnected) previous.focus({ preventScroll: true });
    };
  }, [identity]);
}
