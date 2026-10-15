/**
 * Embed-layer warning for chasm-trap shaped views: two or more independent FACT tables (roots with outgoing
 * relationships that nothing else points at, no relationship path between them) that both reach the same dimension tables.
 *
 * Snowflake/Cortex Analyst can combine metrics of such facts per shared dimension (aggregate each fact, join on the
 * dimension). A Sigma data model relationship only traverses N:1 from the many side, so no single element reaches another
 * fact: nothing is double counted by the conversion, but cross-fact metrics cannot be expressed in one element (the engine
 * drops them, EXPRESSION_NOT_REPRESENTABLE) and, if the view declares none, nothing else would tell the author. A workbook
 * author who later joins the raw facts on a dimension key would double count.
 *
 * Adds ONE finding per view; never changes the spec. Derived elements (`__joined`, `(via ...)`) fold back to their base
 * element through `source.elementId`. Roots-only avoids flagging snowflaked dimensions (CUSTOMER -> REGION, STORE -> REGION
 * reached from one fact). Must stay browser-safe.
 */
type Json = Record<string, any>;

export interface MultiFactFinding {
  code: 'MULTI_FACT_SHARED_DIMENSIONS';
  severity: 'warning';
  message: string;
  remediation?: string;
  object: { kind: string; name: string; owner?: string };
  source: 'embed';
}

export interface MultiFactEngineFinding {
  code: string;
  message: string;
  object: { kind: string; name: string; owner?: string };
}

const MAX_LISTED = 5;
const NOT_REACHABLE = /\b([A-Za-z0-9_$]+)\.[A-Za-z0-9_$"]+ is not reachable from the emitted element/;

const lastSegment = (e: Json): string => {
  const p = e.source?.path;
  return typeof e.name === 'string' && e.name !== '' ? e.name : Array.isArray(p) && p.length > 0 ? String(p[p.length - 1]) : String(e.id);
};

export function applyMultiFact(
  specText: string,
  viewName: string,
  engineFindings: MultiFactEngineFinding[] = [],
): { findings: MultiFactFinding[] } {
  const none = { findings: [] as MultiFactFinding[] };
  let spec: Json;
  try {
    spec = JSON.parse(specText) as Json;
  } catch {
    return none;
  }
  const elements: Json[] = spec.pages?.[0]?.elements ?? [];
  const byId = new Map<string, Json>(elements.map(e => [e.id, e]));

  const baseOf = (id: string): string => {
    let cur = byId.get(id);
    for (let i = 0; cur !== undefined && i < 50; i++) {
      const s = cur.source;
      if (s?.kind === 'table' && typeof s.elementId === 'string' && byId.has(s.elementId)) cur = byId.get(s.elementId);
      else break;
    }
    return cur?.id ?? id;
  };

  // Folded adjacency: base element -> base elements it relates to (self edges dropped).
  const adj = new Map<string, Set<string>>();
  const hasIncoming = new Set<string>();
  for (const e of elements) {
    const from = baseOf(e.id);
    for (const r of e.relationships ?? []) {
      if (typeof r.targetElementId !== 'string') continue;
      const to = baseOf(r.targetElementId);
      if (to === from) continue;
      if (!adj.has(from)) adj.set(from, new Set());
      adj.get(from)!.add(to);
      hasIncoming.add(to);
    }
  }
  const roots = [...adj.keys()].filter(id => !hasIncoming.has(id));
  if (roots.length < 2) return none;

  const reach = (start: string): Set<string> => {
    const seen = new Set<string>();
    const stack = [...(adj.get(start) ?? [])];
    while (stack.length > 0) {
      const id = stack.pop()!;
      if (seen.has(id)) continue;
      seen.add(id);
      for (const n of adj.get(id) ?? []) stack.push(n);
    }
    return seen;
  };
  const reachOf = new Map(roots.map(r => [r, reach(r)] as const));

  const nameOf = (id: string): string => lastSegment(byId.get(id) ?? { id });
  const pairs: { a: string; b: string; shared: string[] }[] = [];
  for (let i = 0; i < roots.length; i++) {
    for (let j = i + 1; j < roots.length; j++) {
      const a = roots[i]!;
      const b = roots[j]!;
      const ra = reachOf.get(a)!;
      const rb = reachOf.get(b)!;
      // Roots have no incoming relationships, so neither can reach the other: only the shared-dimension check remains.
      const shared = [...ra].filter(x => rb.has(x)).map(nameOf);
      if (shared.length > 0) pairs.push({ a, b, shared });
    }
  }
  if (pairs.length === 0) return none;

  const listPair = (p: { a: string; b: string; shared: string[] }): string => {
    const shown = p.shared.slice(0, MAX_LISTED).join(', ');
    const more = p.shared.length > MAX_LISTED ? `, and ${p.shared.length - MAX_LISTED} more` : '';
    return `${nameOf(p.a)} and ${nameOf(p.b)} share ${p.shared.length} dimension${p.shared.length === 1 ? '' : 's'} (${shown}${more}) but are not related to each other`;
  };

  // Dropped cross-fact metrics: engine said a column of ANOTHER root fact is not reachable from the emitted element.
  const rootKey = new Map<string, string>(); // lowercased alias/name -> root id
  for (const r of roots) {
    const el = byId.get(r)!;
    rootKey.set(String(r).toLowerCase(), r);
    rootKey.set(lastSegment(el).toLowerCase(), r);
  }
  const dropped: string[] = [];
  for (const f of engineFindings) {
    if (f.code !== 'EXPRESSION_NOT_REPRESENTABLE' || f.object.kind !== 'metric') continue;
    const m = NOT_REACHABLE.exec(f.message);
    const ownerRoot = f.object.owner !== undefined ? rootKey.get(f.object.owner.toLowerCase()) : undefined;
    let cross = false;
    if (m) {
      const other = rootKey.get(m[1]!.toLowerCase());
      cross = other !== undefined && other !== ownerRoot;
    } else if (ownerRoot === undefined) {
      // Unowned derived metric: dropped because a name could not be resolved; cross-fact if its expression names >= 2 root facts.
      const expr = /`([^`]*)`/.exec(f.message)?.[1] ?? '';
      const hit = new Set<string>();
      for (const t of expr.matchAll(/([A-Za-z0-9_$]+)\./g)) {
        const root = rootKey.get(t[1]!.toLowerCase());
        if (root !== undefined) hit.add(root);
      }
      cross = hit.size >= 2;
    }
    if (cross && !dropped.includes(f.object.name)) dropped.push(f.object.name);
  }

  const message =
    `${pairs.map(listPair).join('; ')}. ` +
    'Snowflake can combine their metrics per shared dimension; a Sigma element can only reach one of them. ' +
    'Build cross-fact metrics (a measure on one fact divided by a measure on another) by aggregating each fact to the shared dimension first, then joining those results; never join the raw facts on a dimension key (that double counts).' +
    (dropped.length > 0 ? ` Dropped cross-fact metrics: ${dropped.join(', ')}.` : '');

  return {
    findings: [
      {
        code: 'MULTI_FACT_SHARED_DIMENSIONS',
        severity: 'warning',
        message,
        remediation:
          'Aggregate each fact to the shared dimension (grouped element or custom SQL), then join the aggregates on that dimension; do not join the raw fact tables.',
        object: { kind: 'view', name: viewName },
        source: 'embed',
      },
    ],
  };
}
