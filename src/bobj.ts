/**
 * SAP BusinessObjects Universe → Sigma Data Model converter.
 *
 * Phase 1 input: BI RESTful Web Service (RWS) universe JSON, as returned by
 *   GET /biprws/sl/v1/universes/{id}
 * on the customer's on-prem BO 4.x server. The ingest (`normalizeBobjUniverse`)
 * is tolerant of the common RWS shape variants — nested outline/items, class /
 * objects, or a flat objects[] array — and normalizes them all into a single
 * `BobjUniverse` IR.
 *
 * Phase 2 (planned): `ingestBobjSdkXml()` will parse a full Semantic-Layer-SDK
 * XML export (joins, contexts, derived tables, cardinalities) into the SAME
 * `BobjUniverse` IR, so `convertBobjIR()` below stays the single shared core.
 *
 * Mapping:
 *   physical table        → Sigma warehouse-table element
 *   dimension / detail    → column (business name preserved via `name`)
 *   measure               → metric (Sum/Count/... of the underlying column)
 *   object with an expr   → calculated column / computed metric formula
 *   join                  → relationship (FK keys parsed from the join SQL)
 *   predefined filter     → warning (report-time condition, no DM equivalent)
 */

import {
  resetIds, sigmaShortId, sigmaDisplayName,
  inferSigmaFormat,
  type SigmaElement, type SigmaColumn, type SigmaMetric, type ConversionResult,
} from './sigma-ids.js';
import { sqlCaseToIf } from './alteryx.js';

// ── Normalized intermediate representation (IR) ──────────────────────────────

export interface BobjObject {
  name: string;
  className?: string;
  kind: 'dimension' | 'measure' | 'detail';
  dataType?: string;
  select: string;
  aggregation?: string;
  description?: string;
}
export interface BobjTable { name: string; database?: string; schema?: string; }
export interface BobjJoin { left?: string; right?: string; expression?: string; cardinality?: string; }
export interface BobjFilter { name: string; where?: string; className?: string; }
export interface BobjUniverse {
  name: string;
  objects: BobjObject[];
  tables: BobjTable[];
  joins: BobjJoin[];
  filters: BobjFilter[];
}

export type BobjTableRemap = string | { table?: string; name?: string; database?: string; schema?: string };
export interface BobjConvertOptions {
  connectionId?: string;
  database?: string;
  schema?: string;
  modelName?: string;
  /** Restructured / platinum-layer remap: old universe table → new warehouse table.
   *  Value is a new table name, or { table, database?, schema? } to also relocate it.
   *  Many old tables may map to one new table (consolidation). */
  tableMap?: Record<string, BobjTableRemap>;
  /** Column remap keyed "OLD_TABLE.OLD_COL" (or "*.OLD_COL" for any table) → new column name. */
  columnMap?: Record<string, string>;
}

// ── Public entry point: RWS JSON → Sigma ─────────────────────────────────────

export function convertBobjToSigma(
  input: any,
  options: BobjConvertOptions = {},
): ConversionResult {
  const uni = normalizeBobjUniverse(input);
  return convertBobjIR(uni, options);
}

// ── Target-layer remap (old universe names → restructured / platinum names) ──
// tableMap: { OLD_TABLE: "NEW_TABLE" | { table, database?, schema? } }.
// columnMap: { "OLD_TABLE.OLD_COL": "NEW_COL" } or { "*.OLD_COL": "NEW_COL" } (any table).

interface RemapEntry { name: string; database?: string; schema?: string; }
function buildBobjRemap(
  tableMap: Record<string, BobjTableRemap> = {},
  columnMap: Record<string, string> = {},
): { tmap: Map<string, RemapEntry>; cmap: Map<string, string> } {
  const tmap = new Map<string, RemapEntry>();   // OLD TABLE KEY → entry
  for (const [k, v] of Object.entries(tableMap || {})) {
    const e: RemapEntry = typeof v === 'string'
      ? { name: v }
      : { name: (v.name || v.table) as string, database: v.database, schema: v.schema };
    if (e.name) tmap.set(tableKeyOf(k), e);
  }
  const cmap = new Map<string, string>();        // "OLD_TABLE_KEY.OLD_COL↑" | "*.OLD_COL↑" → NEW_COL
  for (const [k, v] of Object.entries(columnMap || {})) {
    if (!v) continue;
    const dot = k.indexOf('.');
    const t = dot >= 0 ? tableKeyOf(k.slice(0, dot)) : '*';
    const c = (dot >= 0 ? k.slice(dot + 1) : k).trim().toUpperCase();
    cmap.set(`${t}.${c}`, v);
  }
  return { tmap, cmap };
}

interface RemapUsed { tables: Set<string>; cols: Set<string>; }
function remapBobjSql(sql: string, tmap: Map<string, RemapEntry>, cmap: Map<string, string>, used: RemapUsed): string {
  if (!sql) return sql;
  const re = /"?([A-Za-z_][\w ]*?)"?\s*\.\s*"?([A-Za-z_]\w*)"?/g;
  return sql.replace(re, (_full, t: string, c: string) => {
    const tk = tableKeyOf(t);
    const te = tmap.get(tk);
    if (te) used.tables.add(tk);
    const newTable = te?.name ?? t;
    const cu = c.trim().toUpperCase();
    const newCol = cmap.get(`${tk}.${cu}`) ?? cmap.get(`*.${cu}`) ?? c;
    if (newCol !== c) used.cols.add(`${tk}.${cu}`);
    return `${newTable}.${newCol}`;
  });
}
function remapBobjBare(name: string | undefined, tmap: Map<string, RemapEntry>, used: RemapUsed): string | undefined {
  if (!name) return name;
  if (/\./.test(name)) return name;
  const e = tmap.get(tableKeyOf(name));
  if (e) { used.tables.add(tableKeyOf(name)); return e.name; }
  return name;
}

export function applyBobjRemap(
  uni: BobjUniverse,
  tableMap?: Record<string, BobjTableRemap>,
  columnMap?: Record<string, string>,
): { uni: BobjUniverse; warnings: string[] } {
  const hasT = tableMap && Object.keys(tableMap).length;
  const hasC = columnMap && Object.keys(columnMap).length;
  if (!hasT && !hasC) return { uni, warnings: [] };
  const { tmap, cmap } = buildBobjRemap(tableMap, columnMap);
  const used: RemapUsed = { tables: new Set(), cols: new Set() };
  const warnings: string[] = [];

  // Tables — rename/relocate, collapsing many-to-one consolidations by new key.
  const merged = new Map<string, BobjTable>();
  for (const t of uni.tables) {
    const tk = tableKeyOf(t.name);
    const e = tmap.get(tk);
    if (e) used.tables.add(tk);
    const name = e?.name ?? t.name;
    const key = tableKeyOf(name);
    const prev = merged.get(key);
    merged.set(key, {
      name,
      database: e?.database ?? t.database ?? prev?.database,
      schema: e?.schema ?? t.schema ?? prev?.schema,
    });
  }
  const consolidations = uni.tables.length - merged.size;
  const newUni: BobjUniverse = {
    ...uni,
    tables: [...merged.values()],
    objects: uni.objects.map(o => ({ ...o, select: remapBobjSql(o.select, tmap, cmap, used) })),
    joins: uni.joins.map(j => ({
      ...j,
      left: remapBobjBare(j.left, tmap, used),
      right: remapBobjBare(j.right, tmap, used),
      expression: j.expression ? remapBobjSql(j.expression, tmap, cmap, used) : j.expression,
    })),
  };

  warnings.push(`Target-layer remap applied: ${used.tables.size} table(s) + ${used.cols.size} column(s) repointed${consolidations > 0 ? `, ${consolidations} table(s) consolidated into one` : ''}.`);
  for (const k of tmap.keys()) if (!used.tables.has(k)) warnings.push(`Remap: tableMap key "${k}" matched no universe table — check the name.`);
  for (const k of cmap.keys()) if (!k.startsWith('*.') && !used.cols.has(k)) warnings.push(`Remap: columnMap key "${k.replace('.', '/')}" matched no universe column — check the name.`);

  return { uni: newUni, warnings };
}

// ── RWS-shape-tolerant ingest → IR ───────────────────────────────────────────

export function normalizeBobjUniverse(input: any): BobjUniverse {
  const root = input?.universe ?? input ?? {};
  const name: string = root.name || root.universeName || input?.name || 'BusinessObjects Universe';

  const objects: BobjObject[] = [];

  // Collect objects either from a flat objects[] array or by walking classes.
  const flat = root.objects || input?.objects;
  if (Array.isArray(flat)) {
    for (const o of flat) pushObject(objects, o, o.className || o.class);
  }
  const classRoots =
    root.classes || root.businessLayer?.classes ||
    root.outlines?.outline || root.outline || input?.classes;
  if (Array.isArray(classRoots)) walkClasses(classRoots, objects, undefined);

  // Tables: declared and/or inferred from object SELECTs.
  const tableMap = new Map<string, BobjTable>();
  const declaredTables = root.tables || root.dataFoundation?.tables || input?.tables;
  if (Array.isArray(declaredTables)) {
    for (const t of declaredTables) {
      const raw = typeof t === 'string' ? t : (t.name || t.tableName || '');
      if (!raw) continue;
      const key = tableKeyOf(raw);
      tableMap.set(key, {
        name: key,
        database: typeof t === 'object' ? (t.database || t.catalog) : undefined,
        schema: typeof t === 'object' ? (t.schema || t.owner) : undefined,
      });
    }
  }
  for (const o of objects) {
    for (const { table } of parseTableColTokens(o.select)) {
      const key = tableKeyOf(table);
      if (!tableMap.has(key)) tableMap.set(key, { name: key });
    }
  }

  // Joins.
  const joins: BobjJoin[] = [];
  const rawJoins = root.joins || root.dataFoundation?.joins || input?.joins;
  if (Array.isArray(rawJoins)) {
    for (const j of rawJoins) {
      if (typeof j === 'string') { joins.push({ expression: j }); continue; }
      joins.push({
        left: j.left || j.leftTable || j.table1,
        right: j.right || j.rightTable || j.table2,
        expression: j.expression || j.statement || j.sql || j.definition,
        cardinality: j.cardinality,
      });
    }
  }

  // Predefined filters / conditions.
  const filters: BobjFilter[] = [];
  const rawFilters = root.filters || root.conditions || root.predefinedFilters || input?.filters;
  if (Array.isArray(rawFilters)) {
    for (const f of rawFilters) {
      filters.push({
        name: f.name || 'Filter',
        where: f.where || f.expression || f.sql || f.definition,
        className: f.className || f.class,
      });
    }
  }

  return { name, objects, tables: [...tableMap.values()], joins, filters };
}

function walkClasses(nodes: any[], out: BobjObject[], parentClass?: string): void {
  for (const node of nodes) {
    const className = node.name || node.className || parentClass;
    const items = node.objects || node.items?.item || node.items || node.children;
    if (Array.isArray(items)) {
      for (const it of items) {
        // A nested folder (sub-class) vs a leaf object.
        const subItems = it.objects || it.items?.item || it.items;
        const isFolder = (it.type || it.kind || '').toString().toLowerCase() === 'folder' ||
                         (Array.isArray(subItems) && !(it.select || it.sql || it.definition));
        if (isFolder) walkClasses([it], out, className);
        else pushObject(out, it, className);
      }
    }
    // Sub-classes can also hang directly off `classes`.
    const sub = node.classes || node.subClasses;
    if (Array.isArray(sub)) walkClasses(sub, out, className);
  }
}

function pushObject(out: BobjObject[], o: any, className?: string): void {
  const select = o.select || o.sql || o.definition || o.expression || '';
  if (!select && !o.name) return;
  const rawKind = (o.qualification || o.type || o.kind || o.objectType || '').toString().toLowerCase();
  let kind: BobjObject['kind'] = 'dimension';
  if (/measure/.test(rawKind)) kind = 'measure';
  else if (/detail|attribute/.test(rawKind)) kind = 'detail';
  out.push({
    name: o.name || o.objectName || 'Object',
    className,
    kind,
    dataType: o.dataType || o.datatype || o.type,
    select,
    aggregation: o.aggregation || o.aggregationFunction || o.projectionFunction || o.function,
    description: o.description || o.help,
  });
}

// ── IR → Sigma ───────────────────────────────────────────────────────────────

interface ElemCtx {
  element: SigmaElement;
  columns: SigmaColumn[];
  metrics: SigmaMetric[];
  order: string[];
  // physical-col-display → column id (de-dupe raw passthrough columns)
  physColIds: Map<string, string>;
}

export function convertBobjIR(uni: BobjUniverse, options: BobjConvertOptions = {}): ConversionResult {
  resetIds();
  const { connectionId = '<CONNECTION_ID>', database: dbOverride = '', schema: schOverride = '',
          modelName, tableMap, columnMap } = options;
  const warnings: string[] = [];

  // Target-layer remap (restructured / platinum layer): rewrite the universe's old
  // physical table/column names to the new warehouse names BEFORE conversion, so the
  // output binds to the layer that actually exists. Many old tables may map to one
  // (consolidation). No-op when neither map is provided.
  if (tableMap || columnMap) {
    const r = applyBobjRemap(uni, tableMap, columnMap);
    uni = r.uni;
    warnings.push(...r.warnings);
  }

  // Pass 1 — one Sigma element per physical table.
  const ctxByKey = new Map<string, ElemCtx>();
  for (const t of uni.tables) {
    const key = tableKeyOf(t.name);
    const path: string[] = [];
    const db = dbOverride || t.database || '';
    const sch = schOverride || t.schema || '';
    if (db) path.push(db);
    if (sch) path.push(sch);
    path.push(key);
    const element: SigmaElement = {
      id: sigmaShortId(), kind: 'table', name: sigmaDisplayName(key),
      source: { connectionId, kind: 'warehouse-table', path },
      columns: [], order: [],
    };
    ctxByKey.set(key, { element, columns: [], metrics: [], order: [], physColIds: new Map() });
  }

  // Helper: ensure a raw passthrough column exists on a table element; return its id.
  const ensureRawCol = (ctx: ElemCtx, tableKey: string, physColRaw: string, hidden = false): string => {
    const disp = sigmaDisplayName(physColRaw);
    const existing = ctx.physColIds.get(disp);
    if (existing) return existing;
    const id = sigmaShortId();
    const col: SigmaColumn = { id, formula: `[${tableKey}/${disp}]` };
    if (hidden) col.hidden = true;
    ctx.columns.push(col);
    ctx.order.push(id);
    ctx.physColIds.set(disp, id);
    return id;
  };

  // Pass 2 — place each object on its home element.
  for (const obj of uni.objects) {
    const tokens = parseTableColTokens(obj.select);
    const tableKeys = [...new Set(tokens.map(t => tableKeyOf(t.table)))];
    const homeKey = tableKeys[0] || (uni.tables[0] ? tableKeyOf(uni.tables[0].name) : '');
    if (!homeKey) { warnings.push(`"${qual(obj)}": no table reference found in SELECT — skipped.`); continue; }
    let ctx = ctxByKey.get(homeKey);
    if (!ctx) { warnings.push(`"${qual(obj)}": table "${homeKey}" not in universe tables — skipped.`); continue; }

    const dispName = bobjDisplayName(obj.name);
    const isMeasure = obj.kind === 'measure' || !!obj.aggregation || !!detectOuterAgg(obj.select);
    const multiTable = tableKeys.length > 1;
    if (multiTable) warnings.push(`"${qual(obj)}": SELECT spans tables ${tableKeys.join(', ')} — placed on ${homeKey}; verify cross-table refs.`);

    if (isMeasure) {
      const { agg, inner } = splitAggregate(obj.select, obj.aggregation);
      const innerTokens = parseTableColTokens(inner);
      let formulaInner: string;
      if (innerTokens.length === 1 && isBareColumn(inner)) {
        // sum(Table.Col) → ensure raw col + Sum([Col])
        ensureRawCol(ctx, homeKey, innerTokens[0].col);
        formulaInner = `[${sigmaDisplayName(innerTokens[0].col)}]`;
      } else {
        const { formula, warnings: w } = translateBobjExpr(inner, ctx, ensureRawCol, homeKey);
        w.forEach(x => warnings.push(`"${qual(obj)}": ${x}`));
        formulaInner = formula;
        // surface bare cols used in the expr so they resolve
        for (const tk of innerTokens) if (tableKeyOf(tk.table) === homeKey) ensureRawCol(ctx, homeKey, tk.col);
      }
      const fn = aggFormula(agg, formulaInner);
      const metricId = sigmaShortId();
      const m: SigmaMetric = { id: metricId, name: dispName, formula: fn };
      const fmt = inferSigmaFormat(fn, dispName);
      if (fmt) (m as any).format = fmt;
      ctx.metrics.push(m);
    } else if (innerIsSimpleColumn(obj.select)) {
      // dimension / detail mapping straight to a physical column
      const tok = tokens[0];
      const physDisp = sigmaDisplayName(tok.col);
      const existing = ctx.physColIds.get(physDisp);
      const colId = existing ?? sigmaShortId();
      if (!existing) {
        const col: SigmaColumn = { id: colId, formula: `[${homeKey}/${physDisp}]` };
        if (dispName !== physDisp) col.name = dispName;       // preserve business name
        if (obj.description) col.description = obj.description;
        ctx.columns.push(col);
        ctx.order.push(colId);
        ctx.physColIds.set(physDisp, colId);
      }
    } else {
      // expression dimension → calculated column
      const { formula, warnings: w } = translateBobjExpr(obj.select, ctx, ensureRawCol, homeKey);
      w.forEach(x => warnings.push(`"${qual(obj)}": ${x}`));
      for (const tk of tokens) if (tableKeyOf(tk.table) === homeKey) ensureRawCol(ctx, homeKey, tk.col);
      const colId = sigmaShortId();
      const col: SigmaColumn = { id: colId, name: dispName, formula };
      if (obj.description) col.description = obj.description;
      ctx.columns.push(col);
      ctx.order.push(colId);
    }
  }

  // Pass 3 — relationships from joins.
  for (const join of uni.joins) {
    const parsed = parseJoinKeys(join);
    if (!parsed) { warnings.push(`Join "${join.expression || `${join.left}~${join.right}`}" not a simple equi-join — add manually in Sigma.`); continue; }
    let { leftTable, leftCol, rightTable, rightCol } = parsed;
    // Source = "many" side. If cardinality unknown, keep left as source.
    if (join.cardinality && /one-to-many|1-n|onetomany/i.test(join.cardinality)) {
      [leftTable, leftCol, rightTable, rightCol] = [rightTable, rightCol, leftTable, leftCol];
    }
    const srcKey = tableKeyOf(leftTable), tgtKey = tableKeyOf(rightTable);
    const srcCtx = ctxByKey.get(srcKey), tgtCtx = ctxByKey.get(tgtKey);
    if (!srcCtx || !tgtCtx) { warnings.push(`Join ${srcKey}→${tgtKey}: a table is missing from the universe — relationship skipped.`); continue; }
    const srcColId = ensureRawCol(srcCtx, srcKey, leftCol, true);
    const tgtColId = ensureRawCol(tgtCtx, tgtKey, rightCol, true);
    if (!srcCtx.element.relationships) srcCtx.element.relationships = [];
    srcCtx.element.relationships.push({
      id: sigmaShortId(),
      targetElementId: tgtCtx.element.id,
      keys: [{ sourceColumnId: srcColId, targetColumnId: tgtColId }],
      name: tgtKey,   // spec rule: rel name = uppercase target table key
    });
  }

  // Predefined filters → warnings only (report-time conditions, no DM equivalent).
  for (const f of uni.filters) {
    warnings.push(`Predefined filter "${f.name}" not migrated (report-time condition). WHERE: ${truncate(f.where)}`);
  }

  // Finalize elements.
  const elements: SigmaElement[] = [];
  for (const ctx of ctxByKey.values()) {
    ctx.element.columns = ctx.columns;
    ctx.element.order = ctx.order;
    if (ctx.metrics.length) (ctx.element as any).metrics = ctx.metrics;
    elements.push(ctx.element);
  }

  // Denormalized "View" elements (name-aware cross-element refs).
  for (const de of buildBobjDerivedElements(elements)) elements.push(de);

  const stats = {
    elements: elements.length,
    columns: elements.reduce((n, e) => n + (e.columns?.length || 0), 0),
    metrics: elements.reduce((n, e) => n + ((e as any).metrics?.length || 0), 0),
    relationships: elements.reduce((n, e) => n + (e.relationships?.length || 0), 0),
  };

  return {
    model: { name: modelName || uni.name, schemaVersion: 1, pages: [{ id: sigmaShortId(), name: 'Page 1', elements }] },
    warnings,
    stats,
  };
}

// ── Phase 2 stub: SL-SDK XML → IR (feeds the same convertBobjIR core) ────────
//
// export function ingestBobjSdkXml(xml: string): BobjUniverse { ... }
// Will parse <classes>/<tables>/<joins cardinality=...>/<contexts>/<derivedTables>
// into the BobjUniverse IR above, then callers run convertBobjIR().

// ── Derived elements (name-aware variant of buildDerivedElements) ────────────

function buildBobjDerivedElements(elements: SigmaElement[]): SigmaElement[] {
  const derived: SigmaElement[] = [];
  for (const srcEl of elements) {
    if (!srcEl.relationships?.length) continue;
    if (srcEl.source?.kind !== 'warehouse-table') continue;
    const baseName = srcEl.name || (srcEl.source.path || []).slice(-1)[0] || '';
    const viewCols: SigmaColumn[] = [];
    const viewOrder: string[] = [];

    // Set an explicit `name` on each View column = its business display, so the
    // denormalized element is referencable by clean business names (e.g.
    // "Customer Region", not Sigma's auto "Customer Region (CUSTOMER_DIM)").
    // This makes the View the single bindable element for a workbook/report.
    const pushView = (disp: string, ref: string) => {
      if (disp.includes('/')) return;
      const id = sigmaShortId();
      viewCols.push({ id, name: disp, formula: ref });
      viewOrder.push(id);
    };

    for (const col of (srcEl.columns || [])) {
      if (col.hidden) continue;
      const fm = col.formula?.match(/^\[([^\/\]]+)\/([^\]]+)\]$/);
      if (!fm) continue; // skip calc cols
      const disp = col.name || fm[2];
      pushView(disp, `[${baseName}/${disp}]`);
    }
    for (const rel of srcEl.relationships) {
      if (!rel.name) continue;
      const tgt = elements.find(e => e.id === rel.targetElementId);
      if (!tgt || tgt.source?.kind !== 'warehouse-table') continue;
      for (const col of (tgt.columns || [])) {
        if (col.hidden) continue;
        const fm = col.formula?.match(/^\[([^\]]+)\]$/);
        if (!fm) continue;
        const inner = fm[1];
        const s = inner.indexOf('/');
        const disp = col.name || (s >= 0 ? inner.slice(s + 1) : inner);
        pushView(disp, `[${baseName}/${rel.name}/${disp}]`);
      }
    }
    if (viewCols.length) {
      derived.push({
        id: sigmaShortId(), kind: 'table', name: `${baseName} View`,
        source: { kind: 'table', elementId: srcEl.id },
        columns: viewCols, order: viewOrder,
      });
    }
  }
  return derived;
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/** Business object names are human phrases ("Net Revenue"); title-case each word.
 *  Single-token identifiers fall through to sigmaDisplayName (underscore handling). */
function bobjDisplayName(s: string): string {
  if (!s) return '';
  if (/[ ]/.test(s)) return s.replace(/\b\w/g, c => c.toUpperCase());
  return sigmaDisplayName(s);
}

function qual(o: BobjObject): string { return o.className ? `${o.className}\\${o.name}` : o.name; }
function truncate(s?: string): string { return !s ? '(none)' : (s.length > 120 ? s.slice(0, 117) + '...' : s); }

/** Last path segment, uppercased — "dbo.Sales" → "SALES", "Customer" → "CUSTOMER". */
function tableKeyOf(raw: string): string {
  if (!raw) return '';
  return raw.replace(/["'`\[\]]/g, '').split('.').pop()!.trim().toUpperCase();
}

/** All `Table.Column` tokens in a SELECT (quotes stripped). */
function parseTableColTokens(sql: string): Array<{ table: string; col: string }> {
  if (!sql) return [];
  const out: Array<{ table: string; col: string }> = [];
  const re = /"?([A-Za-z_][\w ]*?)"?\s*\.\s*"?([A-Za-z_]\w*)"?/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(sql))) out.push({ table: m[1].trim(), col: m[2].trim() });
  return out;
}

/** True when the whole SELECT is a single bare `Table.Col` (no functions/ops). */
function innerIsSimpleColumn(sql: string): boolean {
  const s = (sql || '').trim();
  return /^"?[A-Za-z_][\w ]*?"?\s*\.\s*"?[A-Za-z_]\w*"?$/.test(s);
}
function isBareColumn(sql: string): boolean { return innerIsSimpleColumn(sql); }

const AGG_RE = /^\s*(count\s+distinct|distinct\s+count|sum|count|avg|average|min|minimum|max|maximum|stddev|variance)\s*\(\s*([\s\S]*)\)\s*$/i;

function detectOuterAgg(sql: string): string | null {
  const m = (sql || '').match(AGG_RE);
  return m ? m[1] : null;
}

/** Split `sum(expr)` → { agg:'sum', inner:'expr' }; honor an explicit aggregation hint. */
function splitAggregate(sql: string, aggHint?: string): { agg: string; inner: string } {
  const m = (sql || '').match(AGG_RE);
  if (m) return { agg: m[1].toLowerCase().replace(/\s+/g, ' '), inner: m[2].trim() };
  return { agg: (aggHint || 'sum').toLowerCase(), inner: (sql || '').trim() };
}

function aggFormula(agg: string, inner: string): string {
  const a = agg.toLowerCase().replace(/\s+/g, ' ');
  switch (a) {
    case 'sum': return `Sum(${inner})`;
    case 'count': return `Count(${inner})`;
    case 'count distinct': case 'distinct count': return `CountDistinct(${inner})`;
    case 'avg': case 'average': return `Avg(${inner})`;
    case 'min': case 'minimum': return `Min(${inner})`;
    case 'max': case 'maximum': return `Max(${inner})`;
    case 'stddev': return `StdDev(${inner})`;
    case 'variance': return `Variance(${inner})`;
    default: return `Sum(${inner})`;
  }
}

/** Parse equi-join keys from a join's expression (or its left/right hints). */
function parseJoinKeys(join: BobjJoin): { leftTable: string; leftCol: string; rightTable: string; rightCol: string } | null {
  const expr = join.expression || '';
  // First equality of two Table.Col tokens.
  const m = expr.match(/"?([A-Za-z_][\w ]*?)"?\s*\.\s*"?([A-Za-z_]\w*)"?\s*=\s*"?([A-Za-z_][\w ]*?)"?\s*\.\s*"?([A-Za-z_]\w*)"?/);
  if (m) return { leftTable: m[1].trim(), leftCol: m[2].trim(), rightTable: m[3].trim(), rightCol: m[4].trim() };
  return null;
}

/**
 * Translate a BOBJ SELECT expression to a Sigma formula. Same-element column
 * refs become bare `[Display]`; functions are mapped; CASE → If; `@`-functions
 * are flagged. `ensureRawCol` surfaces referenced physical columns so the bare
 * refs resolve at query time.
 */
function translateBobjExpr(
  expr: string,
  ctx: ElemCtx,
  ensureRawCol: (ctx: ElemCtx, tableKey: string, physColRaw: string, hidden?: boolean) => string,
  homeKey: string,
): { formula: string; warnings: string[] } {
  let f = (expr || '').trim();
  const warnings: string[] = [];

  // Universe @-functions have no DM equivalent — flag, then best-effort strip.
  const at = f.match(/@(\w+)\s*\(/);
  if (at) {
    const fn = at[1].toLowerCase();
    if (fn === 'select') warnings.push('uses @Select() (reference to another object) — inline the target object SELECT manually');
    else if (fn === 'prompt') warnings.push('uses @Prompt() (runtime prompt) — model it as a Sigma control/parameter');
    else if (fn === 'variable') warnings.push('uses @Variable() (session variable) — substitute a literal or control');
    else if (fn === 'aggregate_aware') warnings.push('uses @Aggregate_Aware() — kept the first aggregate branch; verify table routing');
    else if (fn === 'where') warnings.push('uses @Where() (embedded condition) — re-express as an If()/filter');
    else warnings.push(`uses @${at[1]}() — no Sigma equivalent; review manually`);
    // @Aggregate_Aware(a, b, c) → first branch
    f = f.replace(/@Aggregate_Aware\s*\(\s*([^,()]+)[\s\S]*?\)/gi, '$1');
  }

  // Table.Col → bare [Display] (only the column part survives in a Sigma element).
  f = f.replace(/"?([A-Za-z_][\w ]*?)"?\s*\.\s*"?([A-Za-z_]\w*)"?/g,
    (_full, _tbl, col) => `[${sigmaDisplayName(col)}]`);

  // Common SQL → Sigma function mappings.
  f = f.replace(/\bsubstr(?:ing)?\s*\(/gi, 'Mid(');
  f = f.replace(/\bnvl\s*\(/gi, 'Coalesce(');
  f = f.replace(/\bifnull\s*\(/gi, 'Coalesce(');
  f = f.replace(/\binstr\s*\(/gi, 'Search(');
  f = f.replace(/\b(?:char_)?length\s*\(/gi, 'Len(');
  f = f.replace(/\bupper\s*\(/gi, 'Upper(');
  f = f.replace(/\blower\s*\(/gi, 'Lower(');
  f = f.replace(/\btrim\s*\(/gi, 'Trim(');
  f = f.replace(/\bto_char\s*\(/gi, 'Text(');
  f = f.replace(/\bto_date\s*\(/gi, 'Date(');
  f = f.replace(/\bto_number\s*\(/gi, 'Number(');
  f = f.replace(/\bcurrent_date\b/gi, 'Today()');
  f = f.replace(/\bsysdate\b/gi, 'Today()');
  f = f.replace(/\|\|/g, '&');                 // SQL concat → Sigma concat
  f = f.replace(/'([^']*)'/g, '"$1"');         // string literals
  if (/\bcase\b/i.test(f)) f = sqlCaseToIf(f);

  return { formula: f, warnings };
}
