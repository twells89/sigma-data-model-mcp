/**
 * Embed-layer correction for USING-pinned metrics (`usingPaths: 'bound'`, best effort only).
 *
 * A Snowflake metric pinned with `USING (rel, ...)` (DDL) / `using_relationships: [...]` (YAML) is refused by the
 * engine (METRIC_JOIN_PATH_SELECTOR). Under bestEffort the engine emits it as a PLAIN metric on the fact element
 * (e.g. ORDER_LINE_COUNT_BY_SHIP_DATE -> `Count()`, identical to the unpinned metric), losing the pin.
 *
 * Verified live (2026-10-12, AWS prod): a derived element `{kind:'table', source:{kind:'table', elementId:<fact>}}` whose
 * columns are the fact's passthroughs plus the target's columns through ONLY the pinned relationship
 * (`[FACT/<relationship NAME>/<target column DISPLAY NAME>]`, the same spelling crossTable.ts uses) reproduces
 * Snowflake's path. So each pinned metric moves to such an element, one per (fact element, distinct set of USING
 * relationships), keeping its id and referencing the element's local columns.
 *
 * Inputs come from the IR, not from re-parsing the source: the overridden finding's `details.relationships` carries the
 * metric's USING names exactly as the reader parsed them (DDL and YAML alike), and the IR relationship list lets each
 * name be mapped to the emitted Sigma relationship by (source element, target element, key columns) -- never by guess.
 *
 * Must run AFTER crossTable (it reads the final elements: relationships may have been retargeted at a `__joined` element,
 * and fact columns may have moved there). Must stay browser-safe.
 */
import { mkColumnId, mkElementId, mkMetricId, toDisplayName } from '@semantic-tools/sigma-data-model';

import { relDisplaySegment } from './relDisplay.js';

type Json = Record<string, any>;

export interface UsingPin {
  /** Table alias that owns the metric (the fact), as written in the source. */
  owner: string;
  /** Metric name as written in the source. */
  name: string;
  /** USING relationship names as parsed. */
  using: string[];
}

export interface UsingRelationship {
  /** Relationship name (derived `<SRC>_TO_<TGT>` when anonymous). */
  name: string;
  sourceAlias: string;
  targetAlias: string;
  sourceColumns: string[];
}

export interface UsingPathsInput {
  viewName: string;
  pinned: UsingPin[];
  relationships: UsingRelationship[];
}

export interface UsingPathsFinding {
  code: 'USING_METRICS_PATH_BOUND' | 'USING_PATH_UNRESOLVED';
  severity: 'info' | 'warning';
  message: string;
  remediation?: string;
  line: number;
  object: { kind: string; name: string; owner?: string };
  source: 'embed';
}

const REF3 = /\[([^\[\]/]+)\/([^\[\]/]+)\/([^\[\]/]+)\]/;
const PASSTHROUGH = /^\[([^\[\]/]+)\/([^\[\]]+)\]$/;
const MAX_LISTED = 12;

const slug = (x: string): string => x.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '') || 'x';
const ci = (x: string): string => x.toLowerCase();

/** Display name Sigma shows for a column: explicit `name`, else the display segment of its passthrough formula. */
function displayOf(col: Json): string | undefined {
  if (typeof col.name === 'string' && col.name !== '') return col.name;
  const m = typeof col.formula === 'string' ? PASSTHROUGH.exec(col.formula) : null;
  return m ? m[2] : undefined;
}

interface Group {
  fact: Json;
  rels: { rel: Json; snowflake: string }[]; // sorted by spec relationship id
  pins: { pin: UsingPin; metric: Json }[];
}

export function applyUsingPaths(specText: string, input: UsingPathsInput): { spec: string; findings: UsingPathsFinding[] } {
  const spec = JSON.parse(specText) as Json;
  const elements: Json[] = spec.pages?.[0]?.elements ?? [];
  const byId = new Map<string, Json>(elements.map(e => [e.id, e]));
  const findings: UsingPathsFinding[] = [];

  const unresolved = (pin: UsingPin, why: string): void => {
    findings.push({
      code: 'USING_PATH_UNRESOLVED',
      severity: 'warning',
      message: `Metric \`${pin.name}\` on \`${pin.owner}\` is pinned with USING (${pin.using.join(', ')}) but ${why}; it was left on its table, where it behaves exactly like an unpinned metric (Snowflake would follow only the pinned relationship path).`,
      remediation: 'Rebuild this metric in Sigma on a derived element over the table that joins only through the pinned relationship.',
      line: 1,
      object: { kind: 'metric', name: pin.name, owner: pin.owner },
      source: 'embed',
    });
  };

  /** The emitted Sigma relationship for an IR relationship, matched on source/target element and key columns. */
  const mapRelationship = (ir: UsingRelationship): { rel: Json; src: Json } | { why: string } => {
    const srcId = mkElementId(ir.sourceAlias);
    const tgtId = mkElementId(ir.targetAlias);
    const src = byId.get(srcId);
    if (!src) return { why: `table \`${ir.sourceAlias}\` is not in the converted model` };
    const wantKeys = ir.sourceColumns.map(c => mkColumnId(srcId, c));
    const hits = ((src.relationships ?? []) as Json[]).filter(r => {
      const t = byId.get(r.targetElementId);
      const targetOk = t !== undefined && (t.id === tgtId || (t.source?.kind === 'table' && t.source.elementId === tgtId));
      const keys: Json[] = Array.isArray(r.keys) ? r.keys : [];
      return targetOk && keys.length === wantKeys.length && keys.every((k, i) => k.sourceColumnId === wantKeys[i]);
    });
    const base = toDisplayName(ir.name);
    const named = hits.filter(r => r.name === base || (typeof r.name === 'string' && r.name.startsWith(`${base} `) && /^\d+$/.test(r.name.slice(base.length + 1))));
    const pool = named.length > 0 ? named : [];
    if (pool.length === 0) return { why: `relationship \`${ir.name}\` was not emitted into the model` };
    const exact = pool.filter(r => r.name === base);
    const one = pool.length === 1 ? pool[0] : exact.length === 1 ? exact[0] : undefined;
    if (one === undefined) return { why: `relationship \`${ir.name}\` matches several emitted relationships` };
    if (typeof one.name !== 'string' || one.name === '') return { why: `relationship \`${ir.name}\` has no name in the model` };
    return { rel: one, src };
  };

  // 1. Resolve every pin to (fact element, metric object, spec relationships).
  const groups = new Map<string, Group>();
  for (const pin of input.pinned) {
    const fact = byId.get(mkElementId(pin.owner));
    const metric = fact?.metrics?.find((m: Json) => m.id === mkMetricId(fact.id, pin.name));
    if (!fact || !metric) { unresolved(pin, `its metric is no longer on \`${mkElementId(pin.owner)}\``); continue; }
    if (pin.using.length === 0) { unresolved(pin, 'no USING relationship was reported'); continue; }
    const rels: Group['rels'] = [];
    let bad: string | undefined;
    for (const name of pin.using) {
      const candidates = input.relationships.filter(r => ci(r.name) === ci(name) && ci(r.sourceAlias) === ci(pin.owner));
      if (candidates.length !== 1) { bad = `relationship \`${name}\` ${candidates.length === 0 ? 'was not found on that table' : 'is ambiguous'}`; break; }
      const m = mapRelationship(candidates[0]!);
      if ('why' in m) { bad = m.why; break; }
      if (!rels.some(x => x.rel.id === m.rel.id)) rels.push({ rel: m.rel, snowflake: candidates[0]!.name });
    }
    if (bad !== undefined) { unresolved(pin, bad); continue; }
    rels.sort((a, b) => (a.rel.id < b.rel.id ? -1 : a.rel.id > b.rel.id ? 1 : 0));
    const key = `${fact.id}\u0000${rels.map(r => r.rel.id).join('\u0000')}`;
    const g = groups.get(key) ?? { fact, rels, pins: [] };
    g.pins.push({ pin, metric });
    groups.set(key, g);
  }
  if (groups.size === 0) return { spec: specText, findings };

  // 2. Build one derived element per group.
  const used = new Set<string>(elements.map(e => e.id));
  const built: { fact: Json; derived: Json; moved: Set<Json>; group: Group }[] = [];
  for (const key of [...groups.keys()].sort()) {
    const g = groups.get(key)!;
    const fact = g.fact;
    const tableName = ((fact.columns ?? []) as Json[]).map(c => PASSTHROUGH.exec(c.formula ?? '')?.[1]).find(Boolean);
    if (tableName === undefined) {
      for (const p of g.pins) unresolved(p.pin, `\`${fact.id}\` has no passthrough column to build the path-bound element from`);
      continue;
    }
    const path: unknown = fact.source?.path;
    const lastSeg = Array.isArray(path) && path.length > 0 ? String(path[path.length - 1]) : undefined;
    const baseName: string = typeof fact.name === 'string' && fact.name !== '' ? fact.name : (lastSeg ?? toDisplayName(fact.id));
    const relNames = g.rels.map(r => String(r.rel.name));

    let id = `${fact.id}__via__${g.rels.map(r => r.rel.id).join('__')}`;
    for (let n = 2; used.has(id); n++) id = `${fact.id}__via__${g.rels.map(r => r.rel.id).join('__')}_${n}`;
    used.add(id);

    const cols: Json[] = [];
    const names = new Set<string>();
    const colIds = new Set<string>();
    const add = (display: string, formula: string, from: Json, suffix: string): void => {
      let name = display;
      if (names.has(ci(name))) { name = `${display} (${suffix})`; for (let n = 2; names.has(ci(name)); n++) name = `${display} (${suffix}) ${n}`; }
      names.add(ci(name));
      let cid = `${id}__${slug(name)}`;
      for (let n = 2; colIds.has(cid); n++) cid = `${id}__${slug(name)}_${n}`;
      colIds.add(cid);
      cols.push({
        id: cid,
        formula,
        name,
        ...(from.description !== undefined && { description: from.description }),
        ...(from.format !== undefined && { format: from.format }),
      });
    };
    for (const c of (fact.columns ?? []) as Json[]) {
      const display = displayOf(c);
      if (display === undefined || REF3.test(String(c.formula ?? ''))) continue;
      add(display, `[${tableName}/${display}]`, c, '');
    }
    for (const { rel } of g.rels) {
      const target = byId.get(rel.targetElementId);
      for (const c of (target?.columns ?? []) as Json[]) {
        const display = displayOf(c);
        if (display === undefined || REF3.test(String(c.formula ?? ''))) continue;
        add(display, `[${tableName}/${rel.name}/${relDisplaySegment(display, typeof c.name === 'string' && c.name !== '')}]`, c, String(rel.name));
      }
    }

    // Metric refs must all be local columns (or another metric moving with it); anything else stays on the fact.
    let candidates = g.pins.slice();
    for (;;) {
      const metricNames = new Set(candidates.map(p => String(p.metric.name ?? '')));
      const keep = candidates.filter(p => {
        const f = String(p.metric.formula ?? '');
        const refs = [...f.matchAll(/\[([^\]]*)\]/g)].map(m => m[1]!);
        const bad = refs.find(r => r.includes('/') || (!cols.some(c => c.name === r) && !metricNames.has(r)));
        if (bad === undefined) return true;
        unresolved(p.pin, `its formula references \`[${bad}]\`, which is not a column of the path-bound element`);
        return false;
      });
      if (keep.length === candidates.length) break;
      candidates = keep;
    }
    if (candidates.length === 0) continue;

    const moved = new Set<Json>(candidates.map(p => p.metric));
    const derived: Json = {
      id,
      kind: 'table',
      name: `${baseName} (via ${relNames.join(' + ')})`,
      description: `${baseName} joined only through ${relNames.join(' + ')}. Metrics here follow Snowflake's USING (${g.rels.map(r => r.snowflake).join(', ')}) path — slice them by this element's columns.`,
      source: { kind: 'table', elementId: fact.id },
      columns: cols,
      metrics: candidates.map(p => p.metric),
      order: cols.map(c => c.id),
    };
    built.push({ fact, derived, moved, group: g });
  }
  if (built.length === 0) return { spec: specText, findings };

  // 3. Rewrite the element list: drop moved metrics from each fact; insert via-elements after it (and its `__joined`).
  const removed = new Set<Json>(built.flatMap(b => [...b.moved]));
  const insertAfter = new Map<string, Json[]>();
  for (const b of built) {
    const idx = elements.indexOf(b.fact);
    let at = idx;
    while (at + 1 < elements.length && String(elements[at + 1]!.id).startsWith(`${b.fact.id}__joined`)) at++;
    const anchor = elements[at]!.id as string;
    insertAfter.set(anchor, [...(insertAfter.get(anchor) ?? []), b.derived]);
  }
  const out: Json[] = [];
  for (const e of elements) {
    if (Array.isArray(e.metrics) && e.metrics.some((m: Json) => removed.has(m))) {
      const rest = e.metrics.filter((m: Json) => !removed.has(m));
      const copy: Json = { ...e };
      if (rest.length > 0) copy.metrics = rest; else delete copy.metrics;
      out.push(copy);
    } else out.push(e);
    out.push(...(insertAfter.get(e.id) ?? []));
  }
  spec.pages[0].elements = out;

  const lines = built.flatMap(b => b.group.pins.filter(p => b.moved.has(p.metric)).map(p => `${p.pin.name} -> "${b.derived.name}"`));
  const listed = lines.slice(0, MAX_LISTED).join('; ') + (lines.length > MAX_LISTED ? `; and ${lines.length - MAX_LISTED} more` : '');
  findings.push({
    code: 'USING_METRICS_PATH_BOUND',
    severity: 'info',
    message: `${lines.length} metric(s) pinned with USING were moved to path-bound derived elements that join only through the pinned relationship(s), so their numbers follow Snowflake's USING path instead of being plain metrics identical to the unpinned ones: ${listed}.`,
    remediation: 'Slice each of these metrics by the columns of the element it lives on; columns from a different element do not follow the pinned path.',
    line: 1,
    object: { kind: 'view', name: input.viewName },
    source: 'embed',
  });
  return { spec: JSON.stringify(spec, null, 2), findings };
}
