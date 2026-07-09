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
  /** Detected input format ('sdk-xml' | 'json-with-joins' | 'json-outline'); set by
   *  convertBobjToSigma so the conversion can tell the user whether the provided file
   *  can carry a join graph. */
  inputKind?: 'sdk-xml' | 'json-with-joins' | 'json-outline';
}

// ── Public entry point: RWS JSON *or* SL-SDK XML → Sigma ─────────────────────
//
// `input` may be:
//   • a parsed RWS universe object (the original Phase-1 path), or
//   • a raw string — auto-detected: a leading `<` routes to the Semantic-Layer
//     export ingest (`ingestBobjSdkXml`, the data-foundation + business-layer
//     XML that carries the physical columns and object SELECTs the RWS REST
//     endpoint does NOT expose); anything else is parsed as RWS JSON.
// Both paths normalize to the same `BobjUniverse` IR, so `convertBobjIR` is the
// single shared core regardless of how the universe was extracted.
export function convertBobjToSigma(
  input: any,
  options: BobjConvertOptions = {},
): ConversionResult {
  const uni = parseBobjInput(input);
  return convertBobjIR(uni, { ...options, inputKind: detectBobjInputKind(input) });
}

/** Classify the raw input so the conversion can tell the user, up front, whether the
 *  file they provided carries a join graph:
 *   - 'sdk-xml'         SL-SDK / IDT XML export → data foundation + joins expected
 *   - 'json-with-joins' full IR / extractor --json → joins present
 *   - 'json-outline'    RWS REST outline JSON → NO joins/columns by SAP design */
export function detectBobjInputKind(input: any): 'sdk-xml' | 'json-with-joins' | 'json-outline' {
  if (typeof input === 'string' && input.trim().startsWith('<')) return 'sdk-xml';
  let obj: any = input;
  if (typeof input === 'string') {
    try { obj = JSON.parse(input); } catch { obj = {}; }
  }
  const root = obj?.universe ?? obj ?? {};
  const rawJoins = root.joins || root.dataFoundation?.joins || obj?.joins;
  return Array.isArray(rawJoins) && rawJoins.length ? 'json-with-joins' : 'json-outline';
}

/** Route any accepted input shape to the `BobjUniverse` IR. */
export function parseBobjInput(input: any): BobjUniverse {
  if (typeof input === 'string') {
    const s = input.trim();
    if (s.startsWith('<')) return ingestBobjSdkXml(s);
    return normalizeBobjUniverse(JSON.parse(s));
  }
  return normalizeBobjUniverse(input);
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
          modelName, tableMap, columnMap, inputKind } = options;
  const warnings: string[] = [];

  // Tell the user, up front, whether the file they provided can carry joins — the #1
  // cause of a low-column workbook is an outline-only input silently producing a
  // joinless model.
  if (inputKind === 'json-outline')
    warnings.push(`Input format: RWS outline JSON — carries object names/types/folders ONLY, not the data foundation (physical tables, joins) or object SELECTs. Expect 0 relationships and outline-only columns. For a full model (joins + all columns) provide an SL-SDK / IDT XML export instead (scripts/extract-universe-sdk.groovy).`);
  else if (inputKind === 'sdk-xml')
    warnings.push(`Input format: SL-SDK / IDT XML export — carries the data foundation, so joins and physical columns are expected. If relationships still come out 0, check the per-join warnings below.`);

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
    // Direction matters: in Sigma a relationship is a many→one LOOKUP. The source
    // element (which owns the relationship and becomes the denormalized View base)
    // MUST be the "many" side — otherwise every column looked up across the join
    // fans out to "multiple values" in a workbook. So the source must be the many
    // side, the target the one side.
    const dir = joinDirection(join.cardinality); // 'left-many' | 'right-many' | null
    let leftIsSource: boolean;
    if (dir === 'left-many') leftIsSource = true;
    else if (dir === 'right-many') leftIsSource = false;
    else {
      // No usable cardinality in the input — infer from the star-schema shape:
      // the fact (measure-bearing) table is the many side. If exactly one side
      // carries measures, make that the source; otherwise keep left but warn,
      // because a wrong guess here is exactly what yields "multiple values".
      const lHasM = !!ctxByKey.get(tableKeyOf(leftTable))?.metrics.length;
      const rHasM = !!ctxByKey.get(tableKeyOf(rightTable))?.metrics.length;
      if (lHasM !== rHasM) leftIsSource = lHasM; // the measure-bearing (fact) side is the many side
      else {
        leftIsSource = true;
        warnings.push(`Join ${tableKeyOf(leftTable)}↔${tableKeyOf(rightTable)}: no usable join cardinality in the input and can't tell the fact from the dimension by measures — assumed "${tableKeyOf(leftTable)}" is the many side. If workbook columns come back as "multiple values", flip this relationship so its source is the many/fact table.`);
      }
    }
    if (!leftIsSource) {
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

  // Guard: a multi-table universe that produced NO relationships yields a low-fidelity
  // model — the denormalized View can only traverse to dimension columns via a
  // relationship, so a workbook bound to it sees just the one fact table's columns.
  // This used to happen silently; surface it loudly.
  const physicalCount = elements.filter(e => e.source?.kind === 'warehouse-table').length;
  if (physicalCount > 1 && stats.relationships === 0) {
    if (!uni.joins.length)
      warnings.push(`No joins in the universe input: ${physicalCount} tables produced 0 relationships, so a workbook can only reach ONE table's columns. If this came from the RWS outline (GET /sl/v1/universes/{id}), that REST path carries no join graph by design — re-extract with the SL-SDK / IDT XML path (scripts/extract-universe-sdk.groovy) so joins and the full column set come through.`);
    else
      warnings.push(`${uni.joins.length} join(s) present but 0 produced a relationship — a workbook can only reach ONE table's columns. See the per-join warnings above: joins whose expression isn't a simple equi-join, or whose tables/aliases aren't in the exported data foundation, are dropped. Verify each join expression and that every joined table is exported.`);
  }

  return {
    model: { name: modelName || uni.name, schemaVersion: 1, pages: [{ id: sigmaShortId(), name: 'Page 1', elements }] },
    warnings,
    stats,
  };
}

// ── SL-SDK / IDT export ingest: XML → IR (feeds the same convertBobjIR core) ──
//
// The RWS REST endpoint (GET /sl/v1/universes/{id}) returns only the business
// OUTLINE — object names, datatypes, folders — NOT the relational bindings
// (each object's SELECT/WHERE) nor the data foundation (physical tables,
// columns, joins). To migrate the actual warehouse columns and calculations you
// need a Semantic Layer SDK / Information Design Tool export of the data
// foundation + business layer. This ingest parses that XML into the SAME
// `BobjUniverse` IR `normalizeBobjUniverse` produces, so `convertBobjIR` is
// unchanged.
//
// It is deliberately SHAPE-TOLERANT (SAP publishes no schema for the .dfx/.blx
// XML, and SDK extractors vary): tables/joins are read from a <dataFoundation>
// subtree if present (else the whole doc), business objects + predefined filters
// from a <businessLayer> subtree (else the whole doc). Tag and attribute names
// are matched case-insensitively across the common variants.

interface XmlNode { tag: string; attrs: Record<string, string>; children: XmlNode[]; text: string; }

export function ingestBobjSdkXml(xml: string): BobjUniverse {
  const doc = parseXml(xml);
  const uni = findFirst(doc, ['universe', 'unx', 'businessuniverse']) || doc;
  const name =
    uni.attrs.name || textOf(findFirst(uni, ['name', 'universename'])) || 'BusinessObjects Universe';

  const dfScope = findFirst(uni, ['datafoundation', 'data-foundation', 'df']) || uni;
  const blScope = findFirst(uni, ['businesslayer', 'business-layer', 'bl']) || uni;

  // Data-foundation tables (catalog/schema preserved when present).
  const tableMap = new Map<string, BobjTable>();
  for (const t of findAll(dfScope, n => n.tag === 'table')) {
    const raw = t.attrs.name || t.attrs.tablename || textOf(findFirst(t, ['name']));
    if (!raw) continue;
    const key = tableKeyOf(raw);
    if (!tableMap.has(key)) {
      tableMap.set(key, {
        name: key,
        database: t.attrs.catalog || t.attrs.database || undefined,
        schema: t.attrs.schema || t.attrs.owner || undefined,
      });
    }
  }

  // Joins (+ cardinality).
  const joins: BobjJoin[] = [];
  for (const j of findAll(dfScope, n => n.tag === 'join')) {
    const expression =
      textOf(findFirst(j, ['expression', 'statement', 'sql', 'definition'])) ||
      j.attrs.expression || j.attrs.statement || textOf(j) || undefined;
    // Cardinality: a single string when present, else compose one from per-side
    // multiplicity (attrs or child tags) so joinDirection() can read direction.
    const lc = j.attrs.leftcardinality || j.attrs.leftmultiplicity ||
      textOf(findFirst(j, ['leftcardinality', 'leftmultiplicity']));
    const rc = j.attrs.rightcardinality || j.attrs.rightmultiplicity ||
      textOf(findFirst(j, ['rightcardinality', 'rightmultiplicity']));
    const cardinality = j.attrs.cardinality || textOf(findFirst(j, ['cardinality'])) ||
      (lc && rc ? `${lc}-to-${rc}` : undefined);
    joins.push({
      left: j.attrs.left || j.attrs.lefttable || j.attrs.table1 || undefined,
      right: j.attrs.right || j.attrs.righttable || j.attrs.table2 || undefined,
      expression,
      cardinality,
    });
  }

  // Business-layer objects (recursive, tracking the enclosing folder/class name).
  const objects: BobjObject[] = [];
  walkBlNodes(blScope.children, objects, undefined);

  // Predefined filters / conditions (a WHERE, no SELECT).
  const filters: BobjFilter[] = [];
  for (const f of findAll(blScope, n => /^(filter|condition|predefinedfilter)$/.test(n.tag))) {
    const where =
      textOf(findFirst(f, ['where', 'whereclause', 'expression', 'sql', 'definition'])) ||
      f.attrs.where || undefined;
    if (!where && !f.attrs.name) continue;
    filters.push({ name: f.attrs.name || textOf(findFirst(f, ['name'])) || 'Filter', where });
  }

  // Backfill any table referenced by an object SELECT but absent from the data
  // foundation (lenient exports may omit it).
  for (const o of objects) {
    for (const { table } of parseTableColTokens(o.select)) {
      const key = tableKeyOf(table);
      if (!tableMap.has(key)) tableMap.set(key, { name: key });
    }
  }

  return { name, objects, tables: [...tableMap.values()], joins, filters };
}

/** Walk a business-layer node list, emitting one BobjObject per leaf object. */
function walkBlNodes(nodes: XmlNode[], out: BobjObject[], parentClass?: string): void {
  for (const node of nodes) {
    if (/^(folder|class|businessfolder|subclass)$/.test(node.tag)) {
      walkBlNodes(node.children, out, node.attrs.name || parentClass);
      continue;
    }
    const binding = findFirst(node, ['relationalbinding', 'binding']);
    const selectNode =
      (binding && findFirst(binding, ['select', 'selectclause', 'expression'])) ||
      findFirst(node, ['select', 'selectclause']);
    const select = textOf(selectNode) || node.attrs.select || node.attrs.selectclause || '';
    const tagIsObj = /^(item|object|businessobject|dimension|measure|attribute|detail)$/.test(node.tag);

    if (select || (tagIsObj && node.attrs.name)) {
      let q = (node.attrs.qualification || node.attrs.type || node.attrs.kind || '').toLowerCase();
      if (!q && /^(dimension|measure|attribute|detail)$/.test(node.tag)) q = node.tag;
      let kind: BobjObject['kind'] = 'dimension';
      if (/measure/.test(q)) kind = 'measure';
      else if (/detail|attribute/.test(q)) kind = 'detail';
      out.push({
        name: node.attrs.name || node.attrs.objectname || textOf(findFirst(node, ['name'])) || 'Object',
        className: parentClass,
        kind,
        dataType: node.attrs.datatype || undefined,
        select,
        aggregation:
          node.attrs.aggregation || node.attrs.aggregationfunction ||
          node.attrs.projectionfunction || node.attrs.function || undefined,
        description:
          node.attrs.description || node.attrs.help ||
          textOf(findFirst(node, ['description', 'help'])) || undefined,
      });
      continue; // don't descend into a leaf object
    }
    if (node.children.length) walkBlNodes(node.children, out, parentClass);
  }
}

// ── Minimal dependency-free XML parser ───────────────────────────────────────
// Tolerant of declarations, comments, CDATA, self-closing tags, and `>` inside
// quoted attribute values. Tag/attribute names are lowercased; values keep case
// and are entity-decoded. Shared verbatim by the skill + browser mirrors.

function parseXml(xml: string): XmlNode {
  const s = xml.replace(/^﻿/, '');
  const root: XmlNode = { tag: '#root', attrs: {}, children: [], text: '' };
  const stack: XmlNode[] = [root];
  const top = () => stack[stack.length - 1];
  const n = s.length;
  let i = 0;
  while (i < n) {
    const lt = s.indexOf('<', i);
    if (lt < 0) { addText(top(), s.slice(i)); break; }
    if (lt > i) addText(top(), s.slice(i, lt));
    if (s.startsWith('<!--', lt)) { const e = s.indexOf('-->', lt + 4); i = e < 0 ? n : e + 3; continue; }
    if (s.startsWith('<![CDATA[', lt)) {
      const e = s.indexOf(']]>', lt + 9);
      top().text += s.slice(lt + 9, e < 0 ? n : e);   // raw, no entity decode
      i = e < 0 ? n : e + 3; continue;
    }
    if (s.startsWith('<?', lt)) { const e = s.indexOf('?>', lt + 2); i = e < 0 ? n : e + 2; continue; }
    if (s.startsWith('<!', lt)) { const e = s.indexOf('>', lt + 2); i = e < 0 ? n : e + 1; continue; }
    const gt = findTagEnd(s, lt + 1);
    if (gt < 0) break;
    let raw = s.slice(lt + 1, gt).trim();
    if (raw.startsWith('/')) { if (stack.length > 1) stack.pop(); i = gt + 1; continue; }
    const selfClose = raw.endsWith('/');
    if (selfClose) raw = raw.slice(0, -1).trim();
    const node = parseTag(raw);
    top().children.push(node);
    if (!selfClose) stack.push(node);
    i = gt + 1;
  }
  return root;
}

/** Index of the `>` that closes a tag opened at `from`, skipping quoted attrs. */
function findTagEnd(s: string, from: number): number {
  let q = '';
  for (let j = from; j < s.length; j++) {
    const c = s[j];
    if (q) { if (c === q) q = ''; }
    else if (c === '"' || c === "'") q = c;
    else if (c === '>') return j;
  }
  return -1;
}

function parseTag(raw: string): XmlNode {
  const m = raw.match(/^([^\s/>]+)\s*([\s\S]*)$/);
  const tag = (m ? m[1] : raw).toLowerCase();
  const attrs: Record<string, string> = {};
  const rest = m ? m[2] : '';
  const re = /([^\s=]+)\s*=\s*(?:"([^"]*)"|'([^']*)')/g;
  let a: RegExpExecArray | null;
  while ((a = re.exec(rest))) attrs[a[1].toLowerCase()] = decodeEntities(a[2] !== undefined ? a[2] : a[3]);
  return { tag, attrs, children: [], text: '' };
}

function addText(node: XmlNode, chunk: string): void {
  const t = decodeEntities(chunk);
  if (t.trim()) node.text += t;
}

function decodeEntities(s: string): string {
  return s
    .replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"').replace(/&apos;/g, "'")
    .replace(/&#x([0-9a-fA-F]+);/g, (_m, h) => String.fromCharCode(parseInt(h, 16)))
    .replace(/&#(\d+);/g, (_m, d) => String.fromCharCode(parseInt(d, 10)))
    .replace(/&amp;/g, '&');
}

/** First descendant (breadth-first within each level) whose tag is in `tags`. */
function findFirst(node: XmlNode, tags: string[]): XmlNode | null {
  for (const c of node.children) if (tags.includes(c.tag)) return c;
  for (const c of node.children) { const r = findFirst(c, tags); if (r) return r; }
  return null;
}

/** All descendants matching `pred` (does not descend into a match). */
function findAll(node: XmlNode, pred: (n: XmlNode) => boolean, out: XmlNode[] = []): XmlNode[] {
  for (const c of node.children) {
    if (pred(c)) out.push(c);
    else findAll(c, pred, out);
  }
  return out;
}

function textOf(node: XmlNode | null): string { return node ? (node.text || '').trim() : ''; }

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
  // First equality of two qualified column refs, each 2-or-more dotted, optionally
  // quoted segments — so schema/catalog-qualified 3-part names
  // ("DWH"."CUSTOMER"."ID" = "DWH"."SALES"."CUST_ID") resolve, not just bare
  // Table.Col. The last two segments of each ref are taken as table.col.
  // One identifier segment: a quoted/bracketed token (spaces allowed inside) OR a bare
  // unquoted identifier (no spaces — so it can't swallow " AND "/operators in a
  // multi-condition join).
  const seg = '(?:["\'`\\[][^"\'`\\]]+["\'`\\]]|[A-Za-z_]\\w*)';
  const ref = '((?:' + seg + '\\s*\\.\\s*)+' + seg + ')'; // 2+ dot-separated segments
  const m = expr.match(new RegExp(ref + '\\s*=\\s*' + ref));
  if (!m) return null;
  const l = splitDottedRef(m[1]), r = splitDottedRef(m[2]);
  if (!l || !r) return null;
  return { leftTable: l.table, leftCol: l.col, rightTable: r.table, rightCol: r.col };
}

/**
 * Read a SAP join cardinality string and say which side is the "many" side, so the
 * Sigma relationship (a many→one lookup) is built with the many side as source.
 * Tolerant of the many encodings BO/IDT emit: `one-to-many`, `many-to-one`,
 * `OneToMany`, `1-n`/`n-1`, `1:N`/`N:1`, `1..N`/`N..1`, `1..*`/`*..1`. Returns
 * 'left-many' | 'right-many', or null when absent or symmetric (1-1 / N-N).
 */
function joinDirection(card: string | undefined): 'left-many' | 'right-many' | null {
  if (!card) return null;
  // Normalize words/symbols to the two multiplicity tokens 1 (one) and N (many).
  const norm = String(card).toLowerCase()
    .replace(/many/g, 'N').replace(/one/g, '1').replace(/[*]/g, 'N');
  const toks = norm.toUpperCase().match(/[1N]/g);
  if (!toks || toks.length < 2) return null;
  const left = toks[0], right = toks[toks.length - 1];
  if (left === 'N' && right === '1') return 'left-many';
  if (left === '1' && right === 'N') return 'right-many';
  return null; // 1-1 or N-N — no directional signal
}

/** Split a dotted, optionally-quoted ref into { table, col } via its last two segments. */
function splitDottedRef(ref: string): { table: string; col: string } | null {
  const parts = String(ref).split('.').map(p => p.replace(/["'`\[\]]/g, '').trim()).filter(Boolean);
  if (parts.length < 2) return null;
  return { table: parts[parts.length - 2], col: parts[parts.length - 1] };
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
