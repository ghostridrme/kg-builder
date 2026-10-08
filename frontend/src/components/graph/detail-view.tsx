"use client";
import { createContext, type ReactNode } from "react";
import { defaultDetailView, type DetailView } from "@/lib/view-state";
export const ViewContext = createContext<{
  view: DetailView;
  onChange: (view: DetailView) => void;
}>({ view: defaultDetailView, onChange: () => {} });
export function PanelView({
  view,
  onChange,
  children,
}: {
  view: DetailView;
  onChange: (view: DetailView) => void;
  children: ReactNode;
}) {
  return <ViewContext.Provider value={{ view, onChange }}>{children}</ViewContext.Provider>;
}
