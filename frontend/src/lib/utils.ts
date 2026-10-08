import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";
export function cn(...values: ClassValue[]) {
  return twMerge(clsx(values));
}
export function hueFor(value: string) {
  return [...value].reduce((n, c) => (n * 31 + c.charCodeAt(0)) % 360, 0);
}

export function formatUtc(iso: string | null | undefined) {
  return iso ? iso.replace("T", " ").replace(/(\.\d+)?(Z|\+00:00)$/, " UTC") : "unknown";
}
