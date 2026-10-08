"use client";
import { useEffect, useState } from "react";
import { useInfiniteQuery } from "@tanstack/react-query";
import { api, params, type Scope } from "./api";
export type FilterPage = {
  items: { value: string; count: number }[];
  has_more: boolean;
  next_offset: number | null;
  semantic_available: boolean;
  org_id: string;
};
export function useFilterOptions(
  dimension: "namespace" | "entity_type",
  scope: Scope,
  search: string,
) {
  const [term, setTerm] = useState(search);
  useEffect(() => {
    const timer = setTimeout(() => setTerm(search), 250);
    return () => clearTimeout(timer);
  }, [search]);
  const query = useInfiniteQuery({
    staleTime: 30_000,
    queryKey: ["graph-filters", dimension, scope, term],
    initialPageParam: 0,
    queryFn: ({ signal, pageParam }) =>
      api<FilterPage>(
        `graph/filters?${params(scope, { dimension, search: term, offset: pageParam })}`,
        signal,
      ),
    getNextPageParam: (page) => (page.has_more ? (page.next_offset ?? undefined) : undefined),
  });
  return {
    ...query,
    options: query.data?.pages.flatMap((p) => p.items) ?? [],
    semantic_available: query.data?.pages[0]?.semantic_available ?? false,
  };
}
