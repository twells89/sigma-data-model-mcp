/**
 * Embed-layer correction for cross-table references (`crossTableReferences: 'derived'`).
 *
 * The engine emits `[TABLE/relationship_id/column_id]` on the element that OWNS the relationship. Verified live
 * in Sigma: on that element every spelling fails ("Unknown name"). On a DERIVED element sourced from it,
 * ONLY `[TABLE/<relationship NAME>/<target column DISPLAY NAME>]` resolves (a real join). So the affected
 * columns/metrics (plus anything on the element that depends on them) move to a derived `<id>__joined` element.
 *
 * Multi-hop: the engine's resolver (emit.ts mkResolver) only builds single-hop, forward references
 * (`parts.length !== 2 -> null`), so 4+ segment references are never emitted at the pin; nothing to handle.
 * Must stay browser-safe.
 */
import { toDisplayName } from '@semantic-tools/sigma-data-model';

type Json = Record<string, any>;

export interface CrossTableFinding {
  code: 'CROSS_TABLE_REFERENCES_MOVED' | 'CROSS_TABLE_REFERENCE_UNRESOLVED';
  severity: 'info' | 'warning';
  message: string;
  remediation?: string;
  line: number;
  object: { kind: string; name: string; owner?: string };
  source: 'embed';
}

const REF3 = /\[([^\[\]/]+)\/([^\[\]/]+)\/([^\[\]/]+)\]/g;
const PASSTHROUGH = /^\[([^\[\]/]+)\/([^\[\]]+)\]$/;

interface Item {
  kind: 'column' | 'metric';
  obj: Json;
}

const refsOf = (formula: unknown): RegExpExecArray[] =>
  typeof formula === 'string' ? [...formula.matchAll(REF3)] : [];

/** Display name Sigma shows for a column: explicit `name`, else the display segment of its passthrough formula. */
function displayOf(col: Json): string | undefined {
  if (typeof col.name === 'string') return col.name;
  const m = typeof col.formula === 'string' ? PASSTHROUGH.exec(col.formula) : null;
  return m ? m[2] : undefined;
}

function mentions(formula: string, tableName: string | undefined, display: string | undefined, id: string): boolean {
  if (formula.includes(`[${id}]`)) return true;
  if (display === undefined) return false;
  return formula.includes(`[${display}]`) || (tableName !== undefined && formula.includes(`[${tableName}/${display}]`));
}

export function applyCrossTableReferences(
  specText: string,
  viewName: string,
): { spec: string; findings: CrossTableFinding[] } {
  const spec = JSON.parse(specText) as Json;
  const elements: Json[] = spec.pages?.[0]?.elements ?? [];
  const byId = new Map<string, Json>(elements.map(e => [e.id, e]));
  const findings: CrossTableFinding[] = [];

  const hasCross = (e: Json): boolean => {
    const rels = new Set<string>((e.relationships ?? []).map((r: Json) => r.id));
    return [...(e.columns ?? []), ...(e.metrics ?? [])].some(o => refsOf(o.formula).some(m => rels.has(m[2]!)));
  };
  if (!elements.some(hasCross)) return { spec: specText, findings };

  // Items blocked from moving because a reference could not be resolved (reported once, below).
  const blocked = new Map<Json, string>();
  let moved = new Map<string, Set<Json>>(); // element id -> moved column/metric objects

  const resolveRef = (e: Json, m: RegExpExecArray, targetMoved: Map<string, Set<Json>>): { text: string } | { why: string } => {
    const rel = (e.relationships ?? []).find((r: Json) => r.id === m[2]);
    if (!rel || typeof rel.name !== 'string' || rel.name === '') return { why: `relationship \`${m[2]}\` has no name` };
    const target = byId.get(rel.targetElementId);
    const col = target?.columns?.find((c: Json) => c.id === m[3]);
    if (!target || !col) return { why: `target column \`${m[3]}\` not found on element \`${rel.targetElementId}\`` };
    if (refsOf(col.formula).length > 0 || targetMoved.get(target.id)?.has(col)) {
      return { why: `target column \`${m[3]}\` is itself a cross-table column` };
    }
    const display = displayOf(col);
    if (display === undefined) return { why: `target column \`${m[3]}\` has no resolvable display name` };
    return { text: `[${m[1]}/${rel.name}/${display}]` };
  };

  // Fixpoint: blocking an item can change what moves, which can change what other refs may target.
  for (let pass = 0; pass < 20; pass++) {
    const next = new Map<string, Set<Json>>();
    const newBlocked = new Map<Json, string>();
    for (const e of elements) {
      const rels = new Set<string>((e.relationships ?? []).map((r: Json) => r.id));
      const items: Item[] = [
        ...(e.columns ?? []).map((obj: Json) => ({ kind: 'column' as const, obj })),
        ...(e.metrics ?? []).map((obj: Json) => ({ kind: 'metric' as const, obj })),
      ];
      const set = new Set<Json>();
      for (const it of items) {
        const mine = refsOf(it.obj.formula).filter(m => rels.has(m[2]!));
        if (mine.length === 0 || blocked.has(it.obj)) continue;
        let bad: string | undefined;
        for (const m of mine) {
          const r = resolveRef(e, m, moved);
          if ('why' in r) { bad = r.why; break; }
        }
        if (bad !== undefined) newBlocked.set(it.obj, bad);
        else set.add(it.obj);
      }
      if (set.size > 0) {
        // Transitive: anything on this element whose formula references a moved item.
        const tableName = [...set].map(o => refsOf(o.formula)[0]?.[1]).find(Boolean);
        let grew = true;
        while (grew) {
          grew = false;
          for (const it of items) {
            if (set.has(it.obj) || typeof it.obj.formula !== 'string' || blocked.has(it.obj)) continue;
            for (const mo of set) {
              const disp = mo.name ?? displayOf(mo);
              if (mentions(it.obj.formula, tableName, disp, mo.id)) { set.add(it.obj); grew = true; break; }
            }
          }
        }
        next.set(e.id, set);
      }
    }
    const changed = newBlocked.size > 0 && [...newBlocked.keys()].some(k => !blocked.has(k));
    for (const [k, v] of newBlocked) blocked.set(k, v);
    moved = next;
    if (!changed) break;
  }

  for (const e of elements) {
    for (const o of [...(e.columns ?? []), ...(e.metrics ?? [])]) {
      const why = blocked.get(o);
      if (why === undefined) continue;
      findings.push({
        code: 'CROSS_TABLE_REFERENCE_UNRESOLVED',
        severity: 'warning',
        message: `\`${o.name ?? o.id}\` on \`${e.id}\` uses a cross-table reference that could not be rewritten for Sigma (${why}); it was left as the engine emitted it and will not validate in Sigma.`,
        remediation: 'Rebuild this column in Sigma on a derived element over the table, using the relationship picker.',
        line: 1,
        object: { kind: (e.metrics ?? []).includes(o) ? 'metric' : 'column', name: String(o.name ?? o.id), owner: e.id },
        source: 'embed',
      });
    }
  }

  const used = new Set<string>(elements.map(e => e.id));
  const out: Json[] = [];
  const derivedInfo: { baseId: string; baseName: string; derivedId: string; derivedName: string; colIds: Set<string>; finding: CrossTableFinding }[] = [];
  for (const e of elements) {
    const set = moved.get(e.id);
    if (!set || set.size === 0) { out.push(e); continue; }
    let id = `${e.id}__joined`;
    for (let n = 2; used.has(id); n++) id = `${e.id}__joined_${n}`;
    used.add(id);

    const anyFormula = [...set].map(o => refsOf(o.formula)[0]?.[1]).find(Boolean);
    const tableName = anyFormula ?? (e.columns ?? []).map((c: Json) => PASSTHROUGH.exec(c.formula ?? '')?.[1]).find(Boolean);
    const rewrite = (formula: string): string =>
      formula.replace(REF3, (all, a, b, c) => {
        const m = [all, a, b, c] as unknown as RegExpExecArray;
        if (!(e.relationships ?? []).some((r: Json) => r.id === b)) return all;
        const r = resolveRef(e, m, new Map());
        return 'text' in r ? r.text : all;
      });

    const dCols: Json[] = [];
    for (const c of e.columns ?? []) {
      if (set.has(c)) { dCols.push({ ...c, formula: rewrite(c.formula) }); continue; }
      const display = displayOf(c);
      if (display === undefined || tableName === undefined) continue;
      dCols.push({
        id: c.id,
        formula: `[${tableName}/${display}]`,
        ...(typeof c.name === 'string' && { name: c.name }),
        ...(c.description !== undefined && { description: c.description }),
        ...(c.format !== undefined && { format: c.format }),
      });
    }
    // Metrics: Sigma does not accept a 3-segment ref in a metric (not in its metric catalog), so each cross ref is
    // replaced by a same-element ref to a derived column whose formula is exactly that cross ref (helper if needed).
    const slug = (x: string): string => x.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '') || 'x';
    const helpers: Json[] = [];
    const nameTaken = (n: string): boolean => [...dCols, ...helpers].some(c => (displayOf(c) ?? '').toLowerCase() === n.toLowerCase());
    const refName = (all: string, m: RegExpExecArray): string | undefined => {
      const r = resolveRef(e, m, new Map());
      if (!('text' in r)) return undefined;
      const hit = [...dCols, ...helpers].find(c => c.formula === r.text && typeof c.name === 'string');
      if (hit) return hit.name as string;
      const rel = (e.relationships ?? []).find((x: Json) => x.id === m[2])!;
      const display = r.text.slice(r.text.lastIndexOf('/') + 1, -1);
      let name = display;
      if (nameTaken(name)) { name = `${display} (${rel.name})`; for (let n = 2; nameTaken(name); n++) name = `${display} (${rel.name}) ${n}`; }
      let hid = `${id}__xref__${slug(rel.name)}__${slug(display)}`;
      const ids = new Set([...dCols, ...helpers].map(c => c.id));
      for (let n = 2; ids.has(hid); n++) hid = `${id}__xref__${slug(rel.name)}__${slug(display)}_${n}`;
      helpers.push({ id: hid, formula: r.text, name });
      return name;
    };
    const failedMetrics = new Set<Json>();
    const dMetrics: Json[] = [];
    for (const m of (e.metrics ?? []).filter((x: Json) => set.has(x))) {
      let ok = true;
      const formula = String(m.formula).replace(REF3, (all, a, b, c) => {
        if (!(e.relationships ?? []).some((r: Json) => r.id === b)) { ok = false; return all; }
        const nm = refName(all, [all, a, b, c] as unknown as RegExpExecArray);
        if (nm === undefined) { ok = false; return all; }
        return `[${nm}]`;
      });
      if (ok && refsOf(formula).length === 0) dMetrics.push({ ...m, formula });
      else failedMetrics.add(m);
    }
    for (const m of failedMetrics) {
      findings.push({
        code: 'CROSS_TABLE_REFERENCE_UNRESOLVED',
        severity: 'warning',
        message: `\`${m.name ?? m.id}\` on \`${e.id}\` uses a cross-table reference that could not be rewritten to a same-element reference; it was left on \`${e.id}\` as the engine emitted it and will not validate in Sigma.`,
        remediation: 'Rebuild this metric in Sigma on a derived element over the table, using the relationship picker.',
        line: 1,
        object: { kind: 'metric', name: String(m.name ?? m.id), owner: e.id },
        source: 'embed',
      });
    }
    dCols.push(...helpers);

    const base: Json = { ...e };
    for (const m of failedMetrics) set.delete(m);
    base.columns = (e.columns ?? []).filter((c: Json) => !set.has(c));
    if (e.metrics !== undefined) {
      const rest = e.metrics.filter((m: Json) => !set.has(m));
      if (rest.length > 0) base.metrics = rest; else delete base.metrics;
    }
    const movedIds = new Set([...set].map(o => o.id));
    if (Array.isArray(e.order)) base.order = e.order.filter((x: string) => !movedIds.has(x));

    // Sigma displays an unnamed warehouse-table element by its table name as written (e.g. ORDERS), so match that.
    const path: unknown = e.source?.path;
    const lastSeg = Array.isArray(path) && path.length > 0 ? String(path[path.length - 1]) : undefined;
    const baseName: string = typeof e.name === 'string' && e.name !== '' ? e.name : (lastSeg ?? toDisplayName(e.id));
    const names = [...set].map(o => String(o.name ?? displayOf(o) ?? o.id));
    const includes = `Includes columns from related tables: ${names.join(', ')}.`;
    const baseDesc = typeof e.description === 'string' ? e.description.trim() : '';
    const description = baseDesc !== ''
      ? `${baseDesc}${/[.!?]$/.test(baseDesc) ? ' ' : '. '}${includes}`
      : `${baseName} plus columns from related tables: ${names.join(', ')}.`;

    const derived: Json = {
      id,
      kind: 'table',
      name: `${baseName} (joined)`,
      description,
      source: { kind: 'table', elementId: e.id },
      columns: dCols,
      ...(dMetrics.length > 0 && { metrics: dMetrics }),
      ...(Array.isArray(e.order) && { order: dCols.map(c => c.id) }),
    };
    out.push(base, derived);

    const helperNote = helpers.length > 0 ? ` Helper column(s) added to \`${id}\` so metrics can reference joined columns by name: ${helpers.map(h => h.name).join(', ')}.` : '';
    const movedFinding: CrossTableFinding = {
      code: 'CROSS_TABLE_REFERENCES_MOVED',
      severity: 'info',
      message: `Sigma resolves cross-table references only on a derived element, so ${names.length} column(s)/metric(s) of \`${e.id}\` (${names.join(', ')}) now live on the derived element \`${id}\` ("${derived.name}"), which reads from \`${e.id}\` and joins through its relationships. \`${e.id}\` itself no longer carries them.${helperNote}`,
      remediation: `Use "${derived.name}" wherever you need these columns.`,
      line: 1,
      object: { kind: 'view', name: viewName },
      source: 'embed',
    };
    findings.push(movedFinding);
    derivedInfo.push({ baseId: e.id, baseName, derivedId: id, derivedName: derived.name, colIds: new Set(dCols.map(c => c.id)), finding: movedFinding });
  }

  // Retarget relationships INTO a moved-from element at its derived element (so related tables can reach the moved
  // columns). Never touches the element's own outgoing relationships or the derived elements. Only when every key's
  // target column exists on the derived element.
  for (const info of derivedInfo) {
    let retargeted = 0;
    for (let i = 0; i < out.length; i++) {
      const x = out[i]!;
      if (x.id === info.baseId || x.id === info.derivedId || !Array.isArray(x.relationships)) continue;
      let changed = false;
      const rels = x.relationships.map((r: Json) => {
        if (r.targetElementId !== info.baseId) return r;
        const keys: Json[] = Array.isArray(r.keys) ? r.keys : [];
        const missing = keys.find(k => !info.colIds.has(k.targetColumnId));
        if (missing !== undefined) {
          findings.push({
            code: 'CROSS_TABLE_REFERENCE_UNRESOLVED',
            severity: 'warning',
            message: `Relationship \`${r.name ?? r.id}\` on \`${x.id}\` was not retargeted at \`${info.derivedId}\` ("${info.derivedName}") because its key column \`${missing.targetColumnId}\` does not exist on that derived element; it still points at \`${info.baseId}\`, so related tables cannot reach the moved columns through it.`,
            remediation: `Point the relationship at "${info.derivedName}" in Sigma, or add the key column to that element.`,
            line: 1,
            object: { kind: 'relationship', name: String(r.name ?? r.id), owner: x.id },
            source: 'embed',
          });
          return r;
        }
        changed = true;
        retargeted++;
        return { ...r, targetElementId: info.derivedId };
      });
      if (changed) out[i] = { ...x, relationships: rels };
    }
    if (retargeted > 0) {
      info.finding.message += ` ${retargeted === 1 ? 'The relationship' : `${retargeted} relationships`} into ${info.baseName} now ${retargeted === 1 ? 'points' : 'point'} at ${info.derivedName} so related tables can reach these columns.`;
    }
  }
  spec.pages[0].elements = out;
  return { spec: JSON.stringify(spec, null, 2), findings };
}
