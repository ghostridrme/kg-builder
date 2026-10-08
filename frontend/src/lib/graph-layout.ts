import type { Edge, Node } from "@xyflow/react";

/**
 * Radial layout centered on a focal node: the entity in question sits at the
 * center and everything else fans out on concentric rings by hop-distance, so
 * the whole neighborhood is visible at a glance. Falls back to the most-connected
 * node when no focal is given.
 */
export function radialLayout(nodes: Node[], edges: Edge[], focalId?: string): Node[] {
  if (nodes.length === 0) return nodes;
  if (nodes.length === 1) return nodes.map((n) => ({ ...n, position: { x: 0, y: 0 } }));

  const adj = new Map<string, Set<string>>();
  nodes.forEach((n) => adj.set(n.id, new Set()));
  edges.forEach((e) => {
    adj.get(e.source)?.add(e.target);
    adj.get(e.target)?.add(e.source);
  });

  // Focal: requested node, else the highest-degree node.
  let focal = focalId && adj.has(focalId) ? focalId : nodes[0].id;
  if (!focalId) {
    let best = -1;
    for (const [id, nbrs] of adj) {
      if (nbrs.size > best) {
        best = nbrs.size;
        focal = id;
      }
    }
  }

  // BFS hop-distance from the focal.
  const dist = new Map<string, number>([[focal, 0]]);
  const queue = [focal];
  for (let cursor = 0; cursor < queue.length; cursor++) {
    const cur = queue[cursor];
    for (const nb of adj.get(cur) ?? []) {
      if (!dist.has(nb)) {
        dist.set(nb, (dist.get(cur) ?? 0) + 1);
        queue.push(nb);
      }
    }
  }
  const maxKnown = Math.max(0, ...Array.from(dist.values()));

  // Group by ring (unreachable nodes land one ring beyond the farthest).
  const rings = new Map<number, string[]>();
  nodes.forEach((n) => {
    const d = dist.get(n.id) ?? maxKnown + 1;
    if (!rings.has(d)) rings.set(d, []);
    rings.get(d)!.push(n.id);
  });

  const RING = 360;
  const pos = new Map<string, { x: number; y: number }>();
  let previousRadius = 0;
  for (const [d, ids] of [...rings].sort(([a], [b]) => a - b)) {
    if (d === 0) {
      pos.set(ids[0], { x: 0, y: 0 });
      continue;
    }
    // Dense hops occupy several concentric bands. One giant ring makes
    // overview radius linear in inventory size and renders every label tiny.
    let index = 0;
    while (index < ids.length) {
      const radius = Math.max(previousRadius + RING, RING * d);
      const count = Math.min(
        ids.length - index,
        Math.max(1, Math.floor((2 * Math.PI * radius) / 320)),
      );
      ids.slice(index, index + count).forEach((id, i) => {
        const angle = (2 * Math.PI * i) / count - Math.PI / 2 + (d % 2 ? Math.PI / count : 0);
        pos.set(id, {
          x: Math.round(Math.cos(angle) * radius),
          y: Math.round(Math.sin(angle) * radius),
        });
      });
      previousRadius = radius;
      index += count;
    }
  }

  return nodes.map((n) => ({
    ...n,
    position: pos.get(n.id) ?? { x: 0, y: 0 },
  }));
}
