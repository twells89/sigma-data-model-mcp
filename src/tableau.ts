/**
 * Tableau Workbook/Data Source XML → Sigma Data Model JSON converter.
 *
 * Handles .twb (workbook) and .tds (data source) XML content.
 * Parses data sources, joins, calculated fields, parameters, LOD expressions,
 * and relationships. Produces Sigma data model JSON.
 */

import { XMLParser } from 'fast-xml-parser';
import {
  resetIds, sigmaShortId, sigmaInodeId, sigmaDisplayName, inferSigmaFormat, buildDerivedElements, makeRlsSecurity,
  type SigmaElement, type ConversionResult, type SecurityRule,
} from './sigma-ids.js';
import { tableauFormulaToSigma, tableauIsAggregate, tableauFormulaIsRls } from './formulas.js';

// ── XML Parsing Helpers ──────────────────────────────────────────────────────

const xmlParser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '@_',
  isArray: (name) => ['datasource', 'relation', 'column', 'member', 'clause', 'expression',
    'metadata-record', 'relationship', 'object', 'worksheet', 'filter', 'rows', 'cols'].includes(name),
  trimValues: true,
});

function asArray(val: any): any[] {
  if (!val) return [];
  return Array.isArray(val) ? val : [val];
}

function attr(node: any, key: string): string {
  return (node && node[`@_${key}`]) || '';
}

// ── LOD Expression Parser ────────────────────────────────────────────────────

interface LODResult {
  _isLOD: true;
  lodType: 'FIXED' | 'INCLUDE' | 'EXCLUDE';
  dims: string[];           // dim refs as written in the formula (raw bracket contents)
  rawAgg: string;           // raw inner aggregate (e.g. "SUM([SALES])")
  aggFunc: string;          // canonical agg function for SQL emission: SUM/AVG/MIN/MAX/COUNT/COUNTD
  aggExpr: string;          // SQL-expression form of the inner expr (e.g. "PROFIT/NULLIF(SALES,0)")
  sigmaAgg: string;         // Sigma-formula form of the aggregate (legacy compatibility)
}

function _tableauInnerToSql(expr: string): string {
  // Convert Tableau bracket refs to bare warehouse identifiers.
  // [PROFIT]/[SALES] → PROFIT/SALES; ZN([Sales]) → SALES (we ignore safe-null wrappers in the helper SQL).
  let s = expr;
  // Strip ZN(...) wrapper (zero-if-null)
  s = s.replace(/\bZN\s*\(([^()]+)\)/gi, '$1');
  // Tableau IFNULL(x, 0) — keep as Snowflake-compatible IFNULL
  // Bracket refs → bare uppercase identifiers
  s = s.replace(/\[([^\]]+)\]/g, (_m, name) => name.replace(/[^A-Za-z0-9_]/g, '_').toUpperCase());
  // Division by potentially zero column → wrap denominator with NULLIF (best-effort heuristic for AVG ratios)
  s = s.replace(/\/\s*([A-Z][A-Z0-9_]*)\b/g, '/NULLIF($1,0)');
  return s;
}

function tableauParseLOD(formula: string): LODResult | null {
  const m = formula.match(/^\{\s*(FIXED|INCLUDE|EXCLUDE)\s*(.*?)\s*:\s*(.*?)\s*\}$/is);
  if (!m) return null;
  const lodType = m[1].toUpperCase() as 'FIXED' | 'INCLUDE' | 'EXCLUDE';
  const rawDims = m[2].trim();
  const rawAgg = m[3].trim();

  const dims: string[] = [];
  if (rawDims) {
    const dimRefs = rawDims.match(/\[([^\]]+)\]/g) || [];
    for (const ref of dimRefs) dims.push(ref.replace(/^\[|\]$/g, ''));
  }

  // Determine aggregate function and inner expression
  const aggMatch = rawAgg.match(/^(SUM|AVG|MIN|MAX|COUNTD|COUNT)\s*\(([\s\S]+)\)\s*$/i);
  let aggFunc = 'SUM';
  let innerExpr = rawAgg;
  if (aggMatch) {
    aggFunc = aggMatch[1].toUpperCase();
    innerExpr = aggMatch[2].trim();
  }
  const aggExpr = _tableauInnerToSql(innerExpr);

  // Sigma-formula form (legacy)
  let sigmaAgg = rawAgg;
  sigmaAgg = sigmaAgg.replace(/\bSUM\s*\(/gi, 'Sum(');
  sigmaAgg = sigmaAgg.replace(/\bAVG\s*\(/gi, 'Avg(');
  sigmaAgg = sigmaAgg.replace(/\bMIN\s*\(/gi, 'Min(');
  sigmaAgg = sigmaAgg.replace(/\bMAX\s*\(/gi, 'Max(');
  sigmaAgg = sigmaAgg.replace(/\bCOUNTD\s*\(/gi, 'CountDistinct(');
  sigmaAgg = sigmaAgg.replace(/\bCOUNT\s*\(([^)]+)\)/gi, 'CountIf(IsNotNull($1))');
  sigmaAgg = sigmaAgg.replace(/\[([A-Z][A-Z0-9_]{2,})\]/g, (_m, colName) => {
    if (colName.includes(' ')) return `[${colName}]`;
    return '[' + sigmaDisplayName(colName) + ']';
  });

  return { _isLOD: true, lodType, dims, rawAgg, aggFunc, aggExpr, sigmaAgg };
}

// ── Window/Table-calc parser ────────────────────────────────────────────────
// Parses Tableau table calcs (RUNNING_*, WINDOW_*, LOOKUP, RANK*, INDEX, FIRST,
// LAST) into a structured form so the converter can lower them to a kind:'sql'
// helper element with SQL OVER clauses (Sigma DM has no working partitioned/
// ordered window formulas, so SQL is the only path).
interface WindowResult {
  _isWindow: true;
  windowType: 'RUNNING_SUM' | 'RUNNING_AVG' | 'RUNNING_MIN' | 'RUNNING_MAX'
            | 'WINDOW_SUM' | 'WINDOW_AVG' | 'WINDOW_MIN' | 'WINDOW_MAX' | 'WINDOW_COUNT'
            | 'LOOKUP' | 'PREVIOUS_VALUE'
            | 'RANK' | 'RANK_DENSE' | 'RANK_UNIQUE' | 'INDEX'
            | 'FIRST' | 'LAST';
  innerAggFunc: string;       // SUM/AVG/MIN/MAX/COUNT — the inner aggregate (or '' for RANK()/INDEX())
  innerColRaw: string;        // raw inner column ref e.g. "[SALES]" — '' for RANK()/INDEX()
  innerExprSql: string;       // SQL form of inner expression (uppercased identifiers); '' for RANK()/INDEX()
  lookupOffset?: number;      // for LOOKUP — Tableau offset (negative = prior period)
  rankDirection?: 'asc' | 'desc';
}

function _windowInnerToSql(expr: string): string {
  let s = expr;
  s = s.replace(/\bZN\s*\(([^()]+)\)/gi, '$1');
  s = s.replace(/\[([^\]]+)\]/g, (_m, name) => name.replace(/[^A-Za-z0-9_]/g, '_').toUpperCase());
  return s;
}

function tableauParseWindow(formula: string): WindowResult | null {
  const f = formula.trim();
  if (!/^(WINDOW_|RUNNING_|LOOKUP\(|PREVIOUS_VALUE\(|RANK\b|RANK_DENSE\b|RANK_UNIQUE\b|INDEX\(|FIRST\(|LAST\()/i.test(f)) {
    return null;
  }
  // RUNNING_SUM(SUM([x])), RUNNING_AVG(AVG([x])), etc.
  let m = f.match(/^(RUNNING_(?:SUM|AVG|MIN|MAX))\s*\(\s*(SUM|AVG|MIN|MAX|COUNT)\s*\(\s*(\[[^\]]+\]|[A-Z0-9_]+)\s*\)\s*\)\s*$/i);
  if (m) return {
    _isWindow: true, windowType: m[1].toUpperCase() as any,
    innerAggFunc: m[2].toUpperCase(), innerColRaw: m[3],
    innerExprSql: _windowInnerToSql(m[3]),
  };
  // RUNNING_SUM([x]) — bare column form
  m = f.match(/^(RUNNING_(?:SUM|AVG|MIN|MAX))\s*\(\s*(\[[^\]]+\]|[A-Z0-9_]+)\s*\)\s*$/i);
  if (m) return {
    _isWindow: true, windowType: m[1].toUpperCase() as any,
    innerAggFunc: 'SUM', innerColRaw: m[2],
    innerExprSql: _windowInnerToSql(m[2]),
  };
  // WINDOW_SUM(SUM([x])), etc.
  m = f.match(/^(WINDOW_(?:SUM|AVG|MIN|MAX|COUNT))\s*\(\s*(SUM|AVG|MIN|MAX|COUNT)\s*\(\s*(\[[^\]]+\]|[A-Z0-9_]+)\s*\)\s*\)\s*$/i);
  if (m) return {
    _isWindow: true, windowType: m[1].toUpperCase() as any,
    innerAggFunc: m[2].toUpperCase(), innerColRaw: m[3],
    innerExprSql: _windowInnerToSql(m[3]),
  };
  // LOOKUP(SUM([x]), N)
  m = f.match(/^LOOKUP\s*\(\s*(SUM|AVG|MIN|MAX|COUNT)\s*\(\s*(\[[^\]]+\]|[A-Z0-9_]+)\s*\)\s*,\s*(-?\d+)\s*\)\s*$/i);
  if (m) return {
    _isWindow: true, windowType: 'LOOKUP',
    innerAggFunc: m[1].toUpperCase(), innerColRaw: m[2],
    innerExprSql: _windowInnerToSql(m[2]),
    lookupOffset: parseInt(m[3], 10),
  };
  // PREVIOUS_VALUE([x]) — best effort: treat as LAG-1 of inner
  m = f.match(/^PREVIOUS_VALUE\s*\(\s*(SUM|AVG|MIN|MAX|COUNT)\s*\(\s*(\[[^\]]+\]|[A-Z0-9_]+)\s*\)\s*\)\s*$/i);
  if (m) return {
    _isWindow: true, windowType: 'PREVIOUS_VALUE',
    innerAggFunc: m[1].toUpperCase(), innerColRaw: m[2],
    innerExprSql: _windowInnerToSql(m[2]),
    lookupOffset: -1,
  };
  // FIRST() / LAST() — emit constant 0/-1 placeholders as offsets vs current row;
  // We handle them as RANK-style offset-from-partition-start/end via FIRST_VALUE/LAST_VALUE.
  m = f.match(/^(FIRST|LAST)\s*\(\s*\)\s*$/i);
  if (m) return {
    _isWindow: true, windowType: m[1].toUpperCase() as any,
    innerAggFunc: '', innerColRaw: '', innerExprSql: '',
  };
  // RANK() / RANK_DENSE() / RANK_UNIQUE() — bare-arg or measure form
  m = f.match(/^(RANK|RANK_DENSE|RANK_UNIQUE)\s*\(\s*\)\s*$/i);
  if (m) return {
    _isWindow: true, windowType: m[1].toUpperCase() as any,
    innerAggFunc: '', innerColRaw: '', innerExprSql: '',
    rankDirection: 'desc',
  };
  m = f.match(/^(RANK|RANK_DENSE|RANK_UNIQUE)\s*\(\s*(SUM|AVG|MIN|MAX|COUNT)\s*\(\s*(\[[^\]]+\]|[A-Z0-9_]+)\s*\)\s*(?:,\s*['"]?(asc|desc)['"]?\s*)?\)\s*$/i);
  if (m) return {
    _isWindow: true, windowType: m[1].toUpperCase() as any,
    innerAggFunc: m[2].toUpperCase(), innerColRaw: m[3],
    innerExprSql: _windowInnerToSql(m[3]),
    rankDirection: (m[4] || 'desc').toLowerCase() as 'asc' | 'desc',
  };
  // INDEX()
  if (/^INDEX\s*\(\s*\)\s*$/i.test(f)) return {
    _isWindow: true, windowType: 'INDEX',
    innerAggFunc: '', innerColRaw: '', innerExprSql: '',
  };
  return null;
}

// Parse the <table-calculation> child of a <calculation> element to extract
// "Compute Using" addressing override config. Tableau encodes "Specific
// Dimensions" (and the Table/Pane axis presets) as either a `direction` attr or
// nested <address> children listing the order axis fields. Returns null when
// no addressing block is present (caller should fall back to rows/cols heuristic).
//
// Scope:
//  - Specific Dimensions:  <address ref-name='[X]' /> → orderFields = [X], partition = (rows+cols) - {X}
//  - Table (Across):       direction='right' (no <address>) → order = cols dims, partition = rows dims
//  - Table (Down):         direction='down'  (no <address>) → order = rows dims, partition = cols dims
//  - Pane / Cell variants are flagged and fall back to heuristic (out of scope).
interface WindowAddressing {
  mode: 'specific' | 'table-across' | 'table-down' | 'unknown';
  orderFields: string[];           // uppercase warehouse identifiers (no brackets)
  rawDirection?: string;           // for diagnostics
}
function _parseWindowAddressing(calcEl: any): WindowAddressing | null {
  if (!calcEl) return null;
  const tc = calcEl['table-calculation'];
  if (!tc) return null;
  const tcNode = Array.isArray(tc) ? tc[0] : tc;
  if (!tcNode) return null;

  const direction = (attr(tcNode, 'direction') || '').toLowerCase();
  const scope = (attr(tcNode, 'scope') || '').toLowerCase(); // 'table' | 'pane' | 'cell'
  const addresses = asArray(tcNode['address'] || tcNode.address || []);
  const orderFields: string[] = [];
  for (const a of addresses) {
    const refName = attr(a, 'ref-name') || '';
    const cleaned = refName.replace(/^\[|\]$/g, '');
    // Strip any "yr:FIELD:ok" style date prefix; we only need the bare field name.
    const colonStripped = cleaned.match(/^(?:yr|mn|qr|dy|wk|md):([^:]+)(?::[a-z]{2})?$/i);
    const bare = colonStripped ? colonStripped[1] : cleaned;
    if (bare) orderFields.push(bare.toUpperCase());
  }

  if (orderFields.length > 0) {
    return { mode: 'specific', orderFields, rawDirection: direction || undefined };
  }
  if (scope === 'pane' || scope === 'cell') {
    return { mode: 'unknown', orderFields: [], rawDirection: `${scope}/${direction}` };
  }
  if (direction === 'right' || direction === 'left') {
    return { mode: 'table-across', orderFields: [], rawDirection: direction };
  }
  if (direction === 'down' || direction === 'up') {
    return { mode: 'table-down', orderFields: [], rawDirection: direction };
  }
  // table-calculation block exists but didn't match any known mode — fall through.
  return null;
}

// Build a deterministic uppercase alias for window calcs (mirrors LOD path).
function _windowAlias(caption: string, used: Set<string>): string {
  let base = (caption || 'WIN_VAL')
    .toUpperCase()
    .replace(/[^A-Z0-9_]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .replace(/_+/g, '_');
  if (!base) base = 'WIN_VAL';
  let alias = base;
  let n = 2;
  while (used.has(alias)) { alias = `${base}_${n++}`; }
  used.add(alias);
  return alias;
}

// ── Tableau Top-N / Bottom-N Set parser ─────────────────────────────────────
// Parses <calculation class='categorical-set'> with a <groupfilter function='end'>
// child (Tableau Top-N/Bottom-N set) into a structured form so the converter can
// lower it to a kind:'sql' RANK helper element + relationship.
interface TopNResult {
  _isTopN: true;
  setName: string;        // e.g. "Top 10 Customers Set"
  caption: string;        // user-facing caption
  dimField: string;       // ranking key, uppercase warehouse identifier (e.g. CUSTOMER_NAME)
  byField: string;        // measure col, uppercase warehouse identifier (e.g. SALES)
  byAggFunc: string;      // SUM | AVG | MIN | MAX | COUNT | COUNTD
  count: number | null;   // literal N (when count attr is set)
  countControl: string | null; // Tableau parameter name (when count-control attr is set)
  direction: 'top' | 'bottom';
  partitionBy: string[];  // uppercase warehouse identifiers for PARTITION BY (empty = global)
}

function _stripBrackets(s: string): string { return (s || '').replace(/^\[|\]$/g, '').trim(); }

function tableauParseTopNSet(calcEl: any, caption: string, setName: string): TopNResult | null {
  if (!calcEl) return null;
  if (attr(calcEl, 'class') !== 'categorical-set') return null;
  const groupFilters = asArray(calcEl.groupfilter || []);
  // Find the function='end' filter (Top/Bottom-N marker)
  let endFilter: any = null;
  for (const gf of groupFilters) {
    if (attr(gf, 'function') === 'end') { endFilter = gf; break; }
  }
  if (!endFilter) return null;

  const rawDim = attr(endFilter, 'field');
  const rawCount = attr(endFilter, 'count');
  const rawCountCtl = attr(endFilter, 'count-control');
  const direction = (attr(endFilter, 'direction') || 'top').toLowerCase() as 'top' | 'bottom';

  const dimField = _stripBrackets(rawDim).toUpperCase();
  if (!dimField) return null;

  let count: number | null = null;
  let countControl: string | null = null;
  if (rawCount) count = parseInt(rawCount, 10);
  else if (rawCountCtl) countControl = _stripBrackets(rawCountCtl);
  if (count === null && countControl === null) return null;

  // Walk nested groupfilter children to find the aggregation node and any partition-by.
  // Structure: end → filter (user:op='TOP', user:partition-by=...) → aggregation (user:op='SUM', user:op-field='[SALES]')
  let byField = '';
  let byAggFunc = 'SUM';
  const partitionBy: string[] = [];
  function walk(node: any): void {
    if (!node || typeof node !== 'object') return;
    const fn = attr(node, 'function');
    if (fn === 'aggregation') {
      const opField = attr(node, 'user:op-field') || attr(node, 'op-field');
      const op = (attr(node, 'user:op') || attr(node, 'op') || 'SUM').toUpperCase();
      if (opField) byField = _stripBrackets(opField).toUpperCase();
      // Tableau aggregation ops: SUM | AVG | MIN | MAX | COUNT | COUNTD
      byAggFunc = op === 'COUNTD' ? 'COUNTD' : op;
    }
    if (fn === 'filter') {
      const partRaw = attr(node, 'user:partition-by') || attr(node, 'partition-by');
      if (partRaw) {
        // Comma- or space-separated list of [DIM] refs
        const matches = partRaw.match(/\[[^\]]+\]/g) || [];
        for (const m of matches) partitionBy.push(_stripBrackets(m).toUpperCase());
      }
    }
    for (const k of Object.keys(node)) {
      if (k.startsWith('@_')) continue;
      const v = (node as any)[k];
      if (Array.isArray(v)) for (const x of v) walk(x);
      else if (v && typeof v === 'object') walk(v);
    }
  }
  walk(endFilter);

  if (!byField) return null;
  return {
    _isTopN: true,
    setName,
    caption,
    dimField,
    byField,
    byAggFunc,
    count,
    countControl,
    direction,
    partitionBy,
  };
}

// Build a uppercase, identifier-safe alias from a caption (used for helper element names).
function _topNAlias(caption: string, used: Set<string>): string {
  let base = (caption || 'TOPN')
    .toUpperCase()
    .replace(/[^A-Z0-9_]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .replace(/_+/g, '_');
  if (!base) base = 'TOPN';
  let alias = base;
  let n = 2;
  while (used.has(alias)) { alias = `${base}_${n++}`; }
  used.add(alias);
  return alias;
}

// Extract per-worksheet rows/cols dim split from a parsed workbook XML so we can
// derive PARTITION BY / ORDER BY heuristically. Returns: byField (calc-key →
// list of contexts) AND each context records rowsDims / colsDims separately.
interface WindowViewContext { rowsDims: string[]; colsDims: string[]; allDims: string[]; dateDim?: string; dateGrain?: string | null; }
interface WindowWorksheetIndex { byField: Map<string, WindowViewContext[]>; }

// Map a Tableau cols-shelf bracket prefix to the matching DATE_TRUNC grain.
// 'md' (month-day) is treated as 'day' since Snowflake DATE_TRUNC has no md grain.
// Returns null when no time prefix was present (i.e. caller should NOT wrap in DATE_TRUNC).
function _tableauPrefixToDateTrunc(prefix: string | null | undefined): string | null {
  if (!prefix) return null;
  switch (prefix.toLowerCase()) {
    case 'yr': return 'year';
    case 'qr': return 'quarter';
    case 'mn': return 'month';
    case 'wk': return 'week';
    case 'dy': return 'day';
    case 'md': return 'day';
    default: return null;
  }
}

function _buildWindowWorksheetIndex(parsed: any): WindowWorksheetIndex {
  const byField = new Map<string, WindowViewContext[]>();
  const worksheets = asArray(parsed?.workbook?.worksheets?.worksheet || []);
  for (const ws of worksheets) {
    const tbl = ws.table || ws;
    const rowRefs: string[] = [];
    const colRefs: string[] = [];
    let dateDim: string | undefined;
    let dateGrain: string | null = null;

    for (const r of asArray(tbl?.rows || [])) {
      const text = typeof r === 'string' ? r : (r['#text'] || '');
      for (const ref of _extractFieldRefsFromShelf(text)) rowRefs.push(ref.toUpperCase());
    }
    for (const c of asArray(tbl?.cols || [])) {
      const text = typeof c === 'string' ? c : (c['#text'] || '');
      // Tableau encodes date truncations in the bracket prefix (yr:, mn:, qr:, dy:, wk:, md:).
      // Detect them and tag the underlying field as the date order dim plus its grain.
      const re = /\[[^\]]+\]\.\[([^\]]+)\]/g;
      let mm: RegExpExecArray | null;
      while ((mm = re.exec(text)) !== null) {
        const inner = mm[1];
        const colon = inner.match(/^(yr|mn|qr|dy|wk|md):([^:]+):[a-z]{2}$/i);
        if (colon) {
          dateDim = colon[2].toUpperCase();
          dateGrain = _tableauPrefixToDateTrunc(colon[1]);
          colRefs.push(dateDim);
        } else {
          const colon2 = inner.match(/^[a-z]{2,5}:([^:]+):[a-z]{2}$/i);
          colRefs.push((colon2 ? colon2[1] : inner).toUpperCase());
        }
      }
    }

    const view = tbl?.view || {};
    const deps = asArray(view['datasource-dependencies'] || []);
    const dimFields = new Set<string>();
    const usedFields = new Set<string>([...rowRefs, ...colRefs]);
    for (const d of deps) {
      for (const col of asArray(d.column || [])) {
        const role = attr(col, 'role');
        const name = attr(col, 'name').replace(/^\[|\]$/g, '');
        if (role === 'dimension') dimFields.add(name.toUpperCase());
      }
    }
    const rowsDims = rowRefs.filter(r => dimFields.has(r));
    const colsDims = colRefs.filter(c => dimFields.has(c));
    const allDims = Array.from(new Set([...rowsDims, ...colsDims]));

    for (const used of usedFields) {
      const list = byField.get(used) || [];
      list.push({ rowsDims: rowsDims.slice(), colsDims: colsDims.slice(), allDims: allDims.slice(), dateDim, dateGrain });
      byField.set(used, list);
    }
  }
  return { byField };
}

// ── LOD alias generation ─────────────────────────────────────────────────────
// Build a deterministic uppercase SQL alias for the LOD calc, e.g.
// "Sales per Customer" → "SALES_PER_CUSTOMER".
function _lodAlias(caption: string, used: Set<string>): string {
  let base = (caption || 'LOD_VAL')
    .toUpperCase()
    .replace(/[^A-Z0-9_]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .replace(/_+/g, '_');
  if (!base) base = 'LOD_VAL';
  let alias = base;
  let n = 2;
  while (used.has(alias)) {
    alias = `${base}_${n++}`;
  }
  used.add(alias);
  return alias;
}

// ── Worksheet view-dim extraction ────────────────────────────────────────────
// Returns a map: fieldKey → set of view-dim sets (each set is a sorted joined string).
// We also return view dim *details* (set per worksheet) so the LOD effective grouping
// can be computed once per worksheet a calc participates in.
interface ViewContext { dims: string[]; }
interface WorksheetIndex {
  byField: Map<string, ViewContext[]>;  // calcFieldKey (uppercase) → contexts where used
}

function _extractFieldRefsFromShelf(text: string): string[] {
  // Tableau shelf strings look like: "[ds].[REGION]" or "[ds].[avg:Calculation_X:qk]"
  // or "[ds].[yr:ORDER_DATE:ok] / [ds].[avg:Calc:qk]"
  const refs: string[] = [];
  const re = /\[[^\]]+\]\.\[([^\]]+)\]/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) {
    let inner = m[1];
    // Strip Tableau aggregation prefix: "yr:ORDER_DATE:ok" → "ORDER_DATE"
    const colon = inner.match(/^[a-z]{2,5}:([^:]+):[a-z]{2}$/i);
    if (colon) inner = colon[1];
    refs.push(inner);
  }
  return refs;
}

function _buildWorksheetIndex(parsed: any): WorksheetIndex {
  const byField = new Map<string, ViewContext[]>();
  const worksheets = asArray(parsed?.workbook?.worksheets?.worksheet || []);
  for (const ws of worksheets) {
    const tbl = ws.table || ws;
    const viewDims: string[] = [];
    const usedFields = new Set<string>();
    const shelves: string[] = [];

    // rows / cols
    for (const r of asArray(tbl?.rows || [])) shelves.push(typeof r === 'string' ? r : (r['#text'] || ''));
    for (const c of asArray(tbl?.cols || [])) shelves.push(typeof c === 'string' ? c : (c['#text'] || ''));

    for (const s of shelves) {
      for (const ref of _extractFieldRefsFromShelf(s)) {
        usedFields.add(ref.toUpperCase());
      }
    }

    // Dimensions in the view come from datasource-dependencies <column role='dimension'>
    // intersected with fields that appear on rows/cols shelves.
    const view = tbl?.view || {};
    const deps = asArray(view['datasource-dependencies'] || []);
    const dimFieldNames = new Set<string>();
    for (const d of deps) {
      for (const col of asArray(d.column || [])) {
        const role = attr(col, 'role');
        const name = attr(col, 'name').replace(/^\[|\]$/g, '');
        if (role === 'dimension') dimFieldNames.add(name.toUpperCase());
      }
    }

    for (const used of usedFields) {
      if (dimFieldNames.has(used)) viewDims.push(used);
    }

    // Index every used field (calc or column) → this view context
    for (const used of usedFields) {
      const list = byField.get(used) || [];
      list.push({ dims: viewDims.slice() });
      byField.set(used, list);
    }
  }
  return { byField };
}

// ── Column Name Normalization ────────────────────────────────────────────────

// Converts "Country/Region" → "COUNTRY_REGION", "Sub-Category" → "SUB_CATEGORY", etc.
function normalizeColumnName(name: string): string {
  return name.replace(/[^a-zA-Z0-9]+/g, '_').replace(/^_|_$/g, '').toUpperCase();
}

// ── Path Extraction ──────────────────────────────────────────────────────────

function extractPath(rel: any, dbOverride: string, schOverride: string): string[] {
  const rawTable = attr(rel, 'table') || attr(rel, 'name') || '';
  // Strip brackets, disambiguation suffixes (e.g. "(CSA.TABLE)"), then filter UUID path segments
  const cleaned = rawTable.replace(/[\[\]]/g, '').replace(/\s*\([^)]*\)/g, '');
  const parts = cleaned.split('.').filter(Boolean)
    .map((s: string) => s.toUpperCase().trim())
    .filter((p: string) => !/^[0-9A-F]{8}-[0-9A-F]{4}-/i.test(p));

  // Strip Tableau hex-hash suffix from the table name segment: "ORDER_FACT_A1B2C3D4E5F60718" → "ORDER_FACT"
  const stripHash = (s: string) => s.replace(/_[0-9A-Fa-f]{16,}$/, '');

  let path: string[];
  if (parts.length >= 2) {
    path = [...parts.slice(0, -1), stripHash(parts[parts.length - 1])];
  } else if (parts.length === 1) {
    path = [schOverride || 'SCHEMA', stripHash(parts[0])];
  } else {
    path = [attr(rel, 'name').toUpperCase() || 'UNKNOWN'];
  }

  if (dbOverride) {
    if (path.length >= 3) path[0] = dbOverride;
    else path = [dbOverride, ...path];
  }
  if (schOverride) {
    if (path.length >= 3) path[1] = schOverride;
    else if (path.length === 2) path[0] = schOverride;
  }

  return path;
}

// ── Collect Tables from Join Tree ────────────────────────────────────────────

interface TableEntry {
  rel: any;
  leftKey: string;
  rightKey: string;
  joinType: string;
}

function collectTables(rel: any, tables: TableEntry[]): void {
  const type = attr(rel, 'type') || 'table';

  if (type === 'table') {
    tables.push({ rel, leftKey: '', rightKey: '', joinType: '' });
    return;
  }

  if (type === 'join') {
    const joinType = attr(rel, 'join') || 'left';
    let leftKey = '', rightKey = '';

    // Extract join keys from clause
    const clauses = asArray(rel.clause);
    if (clauses.length > 0) {
      const exprs = asArray(clauses[0].expression);
      // Find the comparison expression (op='=')
      const eqExpr = exprs.find((e: any) => attr(e, 'op') === '=');
      if (eqExpr) {
        const innerExprs = asArray(eqExpr.expression);
        if (innerExprs.length >= 2) {
          leftKey = attr(innerExprs[0], 'op') || '';
          rightKey = attr(innerExprs[1], 'op') || '';
        }
      }
    }

    const childRels = asArray(rel.relation);
    if (childRels.length === 2) {
      collectTables(childRels[0], tables);
      const beforeRight = tables.length;
      collectTables(childRels[1], tables);
      for (let i = beforeRight; i < tables.length; i++) {
        if (!tables[i].leftKey) {
          tables[i].joinType = joinType;
          tables[i].leftKey = leftKey;
          tables[i].rightKey = rightKey;
        }
      }
    } else {
      for (const child of childRels) {
        collectTables(child, tables);
      }
    }
  }
}

// ── Main Conversion ──────────────────────────────────────────────────────────

export interface TableauConvertOptions {
  connectionId?: string;
  database?: string;
  schema?: string;
  datasourceIndex?: number;
  /** Map Tableau table names to warehouse names: { "Orders": "ORDERS", "People": "PEOPLE" } */
  tableMapping?: Record<string, string>;
}

export function convertTableauToSigma(
  xmlContent: string,
  options: TableauConvertOptions = {}
): ConversionResult {
  resetIds();

  const { connectionId = '', database = '', schema = '', datasourceIndex = 0 } = options;
  const dbOverride = (database || '').toUpperCase();
  const schOverride = (schema || '').toUpperCase();

  // Parse XML
  let parsed: any;
  try {
    parsed = xmlParser.parse(xmlContent);
  } catch (e: any) {
    throw new Error('XML parse error: ' + e.message);
  }

  // Support both .twb (root=<workbook>) and .tds (root=<datasource>)
  let allDs: any[];
  if (parsed.workbook) {
    allDs = asArray(parsed.workbook?.datasources?.datasource || []);
  } else if (parsed.datasource) {
    allDs = asArray(parsed.datasource);
  } else {
    throw new Error('Unrecognized XML — expected <workbook> or <datasource> root element');
  }

  const parameters: any[] = [];
  const datasources: any[] = [];
  // Maps Tableau parameter name → DM control descriptor; populated by Top-N
  // helpers so the parameter→control emit step can pick up the right shape.
  const topNParamControls: Record<string, { controlId: string; defaultVal: number }> = {};

  for (const ds of allDs) {
    if (attr(ds, 'hasconnection') === 'false' || attr(ds, 'name') === 'Parameters') {
      // Parse parameters
      for (const col of asArray(ds.column)) {
        const colName = attr(col, 'caption') || attr(col, 'name') || '';
        const rawName = attr(col, 'name') || '';
        const colType = attr(col, 'datatype') || 'string';
        const domainType = attr(col, 'param-domain-type') || 'all';
        const members = asArray(col.member).map((m: any) => attr(m, 'value')).filter(Boolean);
        const calcEl = col.calculation;
        parameters.push({
          name: colName.replace(/^\[|\]$/g, ''),
          rawName: rawName.replace(/^\[|\]$/g, ''),
          type: colType,
          domainType,
          members,
          defaultVal: calcEl ? attr(calcEl, 'formula') : ''
        });
      }
      continue;
    }

    const name = attr(ds, 'caption') || attr(ds, 'name') || 'Unnamed';
    const connection = ds.connection;
    const connClass = connection ? attr(connection, 'class') : '';
    const dbname = connection ? (attr(connection, 'dbname') || attr(connection, 'database')) : '';
    const schemaName = connection ? attr(connection, 'schema') : '';
    datasources.push({ name, ds, connection, connClass, dbname, schema: schemaName });
  }

  if (datasources.length === 0) {
    throw new Error('No data sources found in the Tableau file');
  }

  const dsIdx = Math.min(datasourceIndex, datasources.length - 1);
  const ds = datasources[dsIdx];
  const warnings: string[] = [];
  const security: SecurityRule[] = [];   // detected RLS — reported, not injected (architecture B)
  const elements: SigmaElement[] = [];
  const connId = connectionId || '<CONNECTION_ID>';

  // ── Virtual-connection GUID resolution index ─────────────────────────────
  // In a Tableau 2020.2+ "virtual connection" (relation type='collection') every
  // field is referenced internally by a UUID. The datasource carries the authoritative
  // GUID→(caption, owning-table) mapping in two places:
  //   <cols><map key='[GUID]' value='[REL_NAME].[GUID]'/>   → GUID → owning relation name
  //   <column caption='…' name='[GUID]'/> + <metadata-records> → GUID → display caption
  // We build a lookup so that (a) calc/metric formulas referencing bare GUIDs can be
  // rewritten to their display captions, and (b) flattened dimension columns the
  // virtual connection invents on the fact relation can be recognised and skipped
  // (they belong to a related DIM element, not the physical fact table).
  const GUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  const guidCaption: Record<string, string> = {};   // guid(lower) → display caption (suffix-stripped)
  const guidOwnerRel: Record<string, string> = {};   // guid(lower) → owning relation name (e.g. "ORDER_FACT (CSA.ORDER_FACT)")
  {
    // (1) <cols><map> — GUID → owning relation. Key/value carry the GUID; the value's
    // leading bracket segment is the owning relation name.
    for (const mp of asArray((ds.ds as any)?.cols?.map || [])) {
      const key = (attr(mp, 'key') || '').replace(/^\[|\]$/g, '');
      const val = (attr(mp, 'value') || '');
      const guid = (key.match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i) || [])[0];
      const ownerRel = (val.match(/^\[([^\]]+)\]/) || [])[1];
      if (guid && ownerRel) guidOwnerRel[guid.toLowerCase()] = ownerRel;
    }
    // (2) metadata-records — GUID → caption (most authoritative; carries <caption>).
    for (const mr of asArray((ds.connection as any)?.['metadata-records']?.['metadata-record'] || [])) {
      if (attr(mr, 'class') !== 'column') continue;
      const guid = ((mr['remote-name'] as string) || '').trim();
      const cap  = ((mr['caption'] as string) || '').trim();
      if (guid && GUID_RE.test(guid) && cap) guidCaption[guid.toLowerCase()] = cap;
    }
    // (3) datasource <column caption=… name='[GUID]'> defs — fill any gaps. Strip the
    // virtual-connection flatten suffix " (TABLE (schema.TABLE))" and role-playing tail.
    for (const col of asArray(ds.ds?.column || [])) {
      const nm = (attr(col, 'name') || '').replace(/^\[|\]$/g, '');
      const guid = (nm.match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i) || [])[0];
      if (!guid) continue;
      const cap = (attr(col, 'caption') || '').replace(/\s*\([^()]*\([^)]*\)\)\s*$/, '').trim();
      if (cap && !guidCaption[guid.toLowerCase()]) guidCaption[guid.toLowerCase()] = cap;
    }
  }

  // The relation name of the fact (the relation that carries physical measure columns,
  // i.e. the one whose <relation> child declares its own <columns>). Set in the
  // collection branch below; used to decide whether a GUID column is a genuine fact
  // column or a flattened dimension column.
  let factRelName: string | null = null;

  // GUIDs of relation columns that are Tableau-DERIVED, not physical warehouse columns —
  // e.g. a date-parsed field `<column date-parse-format='yyyyMMdd' name='guid'/>` inside
  // a <relation>. These have no physical counterpart in the warehouse table (the parse
  // is a Tableau transform), so emitting them as base columns invents phantoms.
  const derivedRelColGuids = new Set<string>();
  for (const rel of asArray((ds.connection as any)?.relation || [])) {
    const scanRel = (r: any) => {
      for (const col of asArray(r?.columns?.column || [])) {
        const nm = (attr(col, 'name') || '').replace(/^\[|\]$/g, '');
        const g = (nm.match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i) || [])[0];
        // date-parse-format / calculation marks a derived (non-physical) relation column.
        if (g && (attr(col, 'date-parse-format') || col.calculation)) derivedRelColGuids.add(g.toLowerCase());
      }
      for (const child of asArray(r?.relation || [])) scanRel(child);
    };
    scanRel(rel);
  }

  // Rewrite bare [GUID] references in a Tableau formula to [Caption] so the
  // downstream formula translator + cross-element placement work on display names.
  const rewriteGuidRefs = (formula: string): string =>
    formula.replace(/\[([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\]/gi,
      (m, g) => { const cap = guidCaption[g.toLowerCase()]; return cap ? `[${cap}]` : m; });

  // ── Build elements from relation structure ──────────────────────────────
  const rootRelation = ds.connection ? asArray(ds.connection.relation || [])[0] : null;

  if (rootRelation) {
    const relType = attr(rootRelation, 'type') || 'table';

    if (relType === 'table') {
      const path = extractPath(rootRelation, dbOverride, schOverride);
      const tableName = path[path.length - 1] || '';
      const columns: any[] = [], order: string[] = [];
      for (const col of asArray(rootRelation?.columns?.column || [])) {
        const key = attr(col, 'name').toUpperCase();
        if (!key) continue;
        const id = sigmaInodeId(key);
        columns.push({ id, formula: `[${tableName}/${sigmaDisplayName(key)}]` });
        order.push(id);
      }
      elements.push({ id: sigmaShortId(), kind: 'table',
        source: { connectionId: connId, kind: 'warehouse-table', path },
        columns, order } as any);

    } else if (relType === 'join') {
      const tables: TableEntry[] = [];
      collectTables(rootRelation, tables);

      if (tables.length === 0) {
        warnings.push('⚠ Could not parse join structure');
      } else {
        const elementMap: Record<string, { element: any; colIdMap: Record<string, string> }> = {};

        for (const t of tables) {
          const path = extractPath(t.rel, dbOverride, schOverride);
          const tableName = path[path.length - 1] || attr(t.rel, 'name') || '';
          if (elementMap[tableName]) continue;

          const columns: any[] = [], order: string[] = [];
          for (const col of asArray(t.rel?.columns?.column || [])) {
            const key = attr(col, 'name').toUpperCase();
            if (!key) continue;
            const id = sigmaInodeId(key);
            columns.push({ id, formula: `[${tableName}/${sigmaDisplayName(key)}]` });
            order.push(id);
          }

          const elemId = sigmaShortId();
          const el: any = { id: elemId, kind: 'table',
            source: { connectionId: connId, kind: 'warehouse-table', path },
            columns, order };
          const colIdMap: Record<string, string> = {};
          columns.forEach((c: any) => {
            const m = c.formula.match(/\/([^\]]+)\]$/);
            if (m) {
              colIdMap[m[1].toUpperCase()] = c.id;
              colIdMap[m[1].replace(/\s+/g, '_').toUpperCase()] = c.id;
            }
          });
          elementMap[tableName] = { element: el, colIdMap };
          elements.push(el);
        }

        // Wire relationships
        const primaryTableName = extractPath(tables[0].rel, dbOverride, schOverride).pop() || '';
        const primaryEntry = elementMap[primaryTableName];

        for (let i = 1; i < tables.length; i++) {
          const t = tables[i];
          if (!t.leftKey || !t.rightKey) continue;
          const leftKey = t.leftKey.replace(/^\[|\]$/g, '').split(/[\.\]]\[?/).pop()?.replace(/\]$/, '').toUpperCase() || '';
          const rightKey = t.rightKey.replace(/^\[|\]$/g, '').split(/[\.\]]\[?/).pop()?.replace(/\]$/, '').toUpperCase() || '';
          const tgtName = extractPath(t.rel, dbOverride, schOverride).pop() || '';
          const tgtEntry = elementMap[tgtName];
          if (!primaryEntry || !tgtEntry) continue;

          let srcColId = primaryEntry.colIdMap[leftKey] || primaryEntry.colIdMap[sigmaDisplayName(leftKey).toUpperCase()];
          if (!srcColId) {
            srcColId = sigmaInodeId(leftKey);
            primaryEntry.element.columns.push({ id: srcColId, formula: `[${primaryTableName}/${sigmaDisplayName(leftKey)}]` });
            primaryEntry.element.order.push(srcColId);
            primaryEntry.colIdMap[leftKey] = srcColId;
          }

          let tgtColId = tgtEntry.colIdMap[rightKey] || tgtEntry.colIdMap[sigmaDisplayName(rightKey).toUpperCase()];
          if (!tgtColId) {
            tgtColId = sigmaInodeId(rightKey);
            tgtEntry.element.columns.push({ id: tgtColId, formula: `[${tgtName}/${sigmaDisplayName(rightKey)}]` });
            tgtEntry.element.order.push(tgtColId);
            tgtEntry.colIdMap[rightKey] = tgtColId;
          }

          if (!primaryEntry.element.relationships) primaryEntry.element.relationships = [];
          primaryEntry.element.relationships.push({
            id: sigmaShortId(),
            targetElementId: tgtEntry.element.id,
            keys: [{ sourceColumnId: srcColId, targetColumnId: tgtColId }],
            name: tgtName
          });
          warnings.push(`ℹ Join ${primaryTableName} → ${tgtName} (${t.joinType || 'left'}) on ${leftKey} = ${rightKey}`);
        }

        // Sort: dims first, fact last
        elements.sort((a, b) => {
          const aR = !!((a as any).relationships?.length);
          const bR = !!((b as any).relationships?.length);
          return aR === bR ? 0 : aR ? 1 : -1;
        });
      }

    } else if (relType === 'collection') {
      // ── Tableau 2020.2+ relationship model (virtual connections) ─────────
      // Child <relation> elements have join-key columns only; all columns live
      // in <connection><metadata-records><metadata-record class='column'> grouped
      // by <object-id> (value = "[TABLE_NAME_HASH]"). Relationships are in
      // <datasource><object-graph><relationships>.

      const childRels = asArray(rootRelation.relation || []);
      if (childRels.length === 0) {
        warnings.push('⚠ Collection datasource has no child relations — skipped');
      } else {
        // Build UUID→caption map keyed by FULL object-id (with hex hash) so role-playing
        // dimensions (two instances of the same table) remain distinguishable.
        const metaByObjId: Record<string, Array<{ uuid: string; caption: string }>> = {};
        const metaRecords = asArray((ds.connection as any)?.['metadata-records']?.['metadata-record'] || []);
        for (const mr of metaRecords) {
          if (attr(mr, 'class') !== 'column') continue;
          const uuid     = ((mr['remote-name'] as string) || '').trim();
          const cap      = ((mr['caption']      as string) || '').trim();
          const objIdRaw = ((mr['object-id']    as string) || '').replace(/^\[|\]$/g, '');
          if (!uuid || !cap || !objIdRaw) continue;
          if (!metaByObjId[objIdRaw]) metaByObjId[objIdRaw] = [];
          metaByObjId[objIdRaw].push({ uuid, caption: cap });
        }

        type EntryType = { element: any; colIdMap: Record<string, string>; cleanName: string; objId?: string | null };
        const elementMap: Record<string, EntryType> = {};

        // The fact relation is the child <relation> that declares its own inline
        // <columns> (the physical fact table). Dimension relations are bare. We use
        // this to resolve which GUID columns are genuine fact columns vs flattened dims.
        const factChild = childRels.find((r: any) => asArray(r?.columns?.column || []).length > 0);
        factRelName = factChild ? (attr(factChild, 'name') || attr(factChild, 'table') || null) : null;

        for (const rel of childRels) {
          const fullName  = attr(rel, 'name') || attr(rel, 'table') || 'TABLE';
          const path      = extractPath(rel, dbOverride, schOverride);
          const cleanName = path[path.length - 1] || fullName;

          const columns: any[] = [], order: string[] = [], colIdMap: Record<string, string> = {};

          // Role-playing dimensions append a trailing digit to the relation name
          // (e.g. "DATE_DIM (CSA.DATE_DIM)1") but objIds share the same base prefix.
          // Fall back to sorted candidates and take the Nth one when no direct match.
          let matchingObjId = Object.keys(metaByObjId).find(k => k === fullName || k.startsWith(fullName + '_'));
          if (!matchingObjId) {
            const roleM = fullName.match(/^([\s\S]+?)(\d+)$/);
            if (roleM) {
              const base  = roleM[1];
              const idx   = parseInt(roleM[2], 10);
              const cands = Object.keys(metaByObjId).filter(k => k === base || k.startsWith(base + '_')).sort();
              matchingObjId = cands[idx];
            }
          }
          const metaCols   = matchingObjId ? metaByObjId[matchingObjId] : [];

          for (const { uuid, caption } of metaCols) {
            const cleanCaption = caption.replace(/\s*\(.*\)$/, '').trim(); // strip disambiguation suffix
            const idKey = uuid.toUpperCase();
            const id    = sigmaInodeId(idKey);
            columns.push({ id, formula: `[${cleanName}/${cleanCaption}]`, name: cleanCaption });
            order.push(id);
            colIdMap[idKey] = id;
            colIdMap[uuid.toUpperCase().replace(/-/g, '_')] = id;
            colIdMap[cleanCaption.toUpperCase()] = id;
            colIdMap[cleanCaption.toUpperCase().replace(/\s+/g, '_')] = id;
          }

          // Fallback: inline <column> elements (join keys only) when no metadata-records match
          if (metaCols.length === 0) {
            for (const col of asArray(rel?.columns?.column || [])) {
              const rawCol = attr(col, 'name') || '';
              const isUuid = /^[0-9a-f]{8}-[0-9a-f]{4}-/i.test(rawCol);
              const key    = isUuid ? rawCol.toUpperCase() : normalizeColumnName(rawCol);
              if (!key) continue;
              const id          = sigmaInodeId(key);
              const capAttr     = attr(col, 'caption');
              const displayName = isUuid ? (capAttr || rawCol) : sigmaDisplayName(key);
              columns.push({ id, formula: `[${cleanName}/${displayName}]` });
              order.push(id);
              colIdMap[rawCol.toUpperCase()] = id;
              colIdMap[key] = id;
            }
          }

          const el: any = { id: sigmaShortId(), kind: 'table',
            source: { connectionId: connId, kind: 'warehouse-table', path },
            columns, order };
          elementMap[fullName] = { element: el, colIdMap, cleanName, objId: matchingObjId || null };
          elements.push(el);
        }

        // Wire relationships from <object-graph><relationships>
        const objGraph = (ds.ds as any)?.['object-graph'];
        const relsList  = asArray(objGraph?.relationships?.relationship || []);

        // Resolve object-id to an elementMap entry.
        // Prefer exact stored objId match (handles role-playing dims with shared prefix),
        // then fall back to cleaned-segment heuristic.
        const getCleanSeg = (name: string) =>
          name.replace(/[\[\]]/g, '').split('.').pop()?.replace(/_[0-9A-Fa-f]{16,}$/, '').toUpperCase() || '';
        const findEntry = (objId: string): EntryType | undefined => {
          const exactKey = Object.keys(elementMap).find(k => (elementMap[k] as any).objId === objId);
          if (exactKey) return elementMap[exactKey];
          const cleanId = getCleanSeg(objId);
          const key = Object.keys(elementMap).find(k => getCleanSeg(k) === cleanId);
          return key ? elementMap[key] : undefined;
        };

        // Extract UUID from function-call expressions: DATE([uuid]) → uuid (uppercase)
        const extractOpUuid = (opAttr: string): string => {
          const fnWrap = opAttr.match(/^\w+\(\[?([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\]?\)$/i);
          return fnWrap ? fnWrap[1].toUpperCase() : '';
        };

        // Parse a relationship expression op to a colIdMap lookup key
        const parseOpRef = (opAttr: string): string => {
          const uuidInFn = extractOpUuid(opAttr);
          if (uuidInFn) return uuidInFn;
          // Strip brackets and greedy disambiguation suffix (handles nested parens)
          return opAttr.replace(/^\[|\]$/g, '').replace(/\s*\(.*\)$/, '').trim().toUpperCase();
        };

        for (const rel of relsList) {
          const firstEp  = rel['first-end-point'];
          const secondEp = rel['second-end-point'];
          if (!firstEp || !secondEp) continue;

          const firstEntry  = findEntry(attr(firstEp,  'object-id'));
          const secondEntry = findEntry(attr(secondEp, 'object-id'));
          if (!firstEntry || !secondEntry || firstEntry === secondEntry) continue;

          // Expression structure: expression[op="="] > expression[op="col"] x2
          const outerExprs = asArray(rel.expression || []);
          const eqExpr     = outerExprs.find((e: any) => attr(e, 'op') === '=') || outerExprs[0];
          if (!eqExpr) continue;
          const innerExprs = asArray(eqExpr.expression || []);
          if (innerExprs.length < 2) continue;

          const srcOpRaw = attr(innerExprs[0], 'op') || '';
          const tgtOpRaw = attr(innerExprs[1], 'op') || '';
          const srcKey = parseOpRef(srcOpRaw);
          const tgtKey = parseOpRef(tgtOpRaw);
          if (!srcKey || !tgtKey) continue;

          // A join key wrapped in a function (e.g. DATE([order_date])) is a computed
          // key, not a plain physical column. Sigma relationships join on physical
          // columns only; emitting one keyed on a derived value yields a dangling join
          // whose cross-element refs all error-type. Route to manual authoring instead.
          const isFnWrappedKey = (op: string) =>
            /^[A-Za-z_]\w*\(\s*\[?[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\]?\s*\)$/i.test(op.trim());
          if (isFnWrappedKey(srcOpRaw) || isFnWrappedKey(tgtOpRaw)) {
            warnings.push(`⚠ Relationship ${firstEntry.cleanName} → ${secondEntry.cleanName} joins on a computed key (${(srcOpRaw || tgtOpRaw).slice(0, 40)}); Sigma joins on physical columns only — skipped, needs manual authoring.`);
            continue;
          }

          const ensureCol = (entry: EntryType, key: string): string => {
            let id = entry.colIdMap[key] || entry.colIdMap[key.replace(/-/g, '_')];
            if (!id) {
              id = sigmaInodeId(key.replace(/\s+/g, '_'));
              const isUuid    = /^[0-9A-F]{8}-[0-9A-F]{4}-/i.test(key);
              // Resolve a relationship-key GUID to its display caption so the join-key
              // column carries a readable name instead of a bare UUID. The caption is
              // also tracked so a later <column> pass dedupes against this column.
              const cap       = isUuid ? guidCaption[key.toLowerCase()] : undefined;
              const dispName  = cap || (isUuid ? key : sigmaDisplayName(key));
              const colObj: any = { id, formula: `[${entry.cleanName}/${dispName}]` };
              if (cap) colObj.name = cap;
              entry.element.columns.push(colObj);
              entry.element.order.push(id);
              entry.colIdMap[key] = id;
              if (cap) {
                entry.colIdMap[cap.toUpperCase()] = id;
                entry.colIdMap[cap.toUpperCase().replace(/\s+/g, '_')] = id;
              }
            }
            return id;
          };

          const srcColId = ensureCol(firstEntry,  srcKey);
          const tgtColId = ensureCol(secondEntry, tgtKey);

          if (!firstEntry.element.relationships) firstEntry.element.relationships = [];
          firstEntry.element.relationships.push({
            id: sigmaShortId(),
            targetElementId: secondEntry.element.id,
            keys: [{ sourceColumnId: srcColId, targetColumnId: tgtColId }],
            name: secondEntry.cleanName,
          });
          warnings.push(`ℹ Relationship ${firstEntry.cleanName} → ${secondEntry.cleanName} on ${srcKey} = ${tgtKey}`);
        }

        // Sort: dims first, fact last
        elements.sort((a, b) => {
          const aR = !!((a as any).relationships?.length);
          const bR = !!((b as any).relationships?.length);
          return aR === bR ? 0 : aR ? 1 : -1;
        });

        if (!dbOverride || !schOverride) {
          warnings.push('⚠ Virtual connection: pass database and schema parameters to set the full warehouse path.');
        }
      }
    }
  }

  // ── Process calculated fields ───────────────────────────────────────────
  const factEl = elements.find(e => (e as any).relationships?.length > 0)
    || (elements.length > 0 ? elements.reduce((best, e) =>
      (e.columns?.length || 0) > (best.columns?.length || 0) ? e : best, elements[0]) : null);

  if (factEl) {
    // Build display name maps
    const globalColMap: Record<string, { elId: string; displayName: string }> = {};
    const displayNameMap: Record<string, { colId: string; el: any }> = {};

    for (const el of elements) {
      for (const c of (el.columns || [])) {
        const fm = c.formula.match(/\/([^\]]+)\]$/);
        if (fm) {
          const dn = fm[1];
          globalColMap[dn.toUpperCase()] = { elId: el.id, displayName: dn };
          displayNameMap[dn.toUpperCase()] = { colId: c.id, el };
          displayNameMap[dn.replace(/\s+/g, '_').toUpperCase()] = { colId: c.id, el };
        }
        if (c.name) displayNameMap[c.name.toUpperCase()] = { colId: c.id, el };
      }
    }

    const factTableName = (factEl.source?.path?.[factEl.source.path.length - 1]) || 'FACT';
    const lodChildElements: any[] = [];

    // ── LOD: build worksheet view-dim index and helper-element registry ───
    const wsIndex = _buildWorksheetIndex(parsed);
    // signatureKey (sorted UPPER dim names joined ',') → helper element record
    const lodHelpers: Record<string, {
      element: any;
      groupDimNames: string[];        // ordered upper-case dim names (matches SELECT order)
      groupDimColIds: string[];       // helper column ids for the dim columns
      aggsByExpr: Record<string, { alias: string; aggFunc: string; aggExpr: string; calcId: string; caption: string }>;
      relationshipName: string;
    }> = {};
    const usedAliases = new Set<string>();

    function _resolveDimDisplayName(dimNameRaw: string): { dimUpper: string; displayName: string; baseColId?: string; onFact: boolean } | null {
      const found = displayNameMap[dimNameRaw.toUpperCase()]
        || displayNameMap[sigmaDisplayName(dimNameRaw).toUpperCase()];
      if (!found) return null;
      const parentCol = found.el.columns?.find((c: any) => c.id === found.colId);
      const dn = parentCol?.name || (parentCol?.formula.match(/\/([^\]]+)\]$/)?.[1]) || dimNameRaw;
      // Determine the warehouse identifier (uppercase) from the formula tail
      const fm = parentCol?.formula.match(/\/([^\]]+)\]$/);
      const dispName: string = dn;
      const dimUpper = dispName.replace(/\s+/g, '_').toUpperCase();
      // Prefer the actual warehouse name if we can derive it from the parent col formula
      // (since Sigma display names are derived from SNAKE_CASE).
      const physicalUpper = (fm ? fm[1] : dispName).replace(/\s+/g, '_').toUpperCase();
      // onFact: the resolved column physically lives on the fact element. SQL helpers
      // (LOD / window / Top-N) query the fact table directly, so a dim that resolves
      // to a *related* dimension element cannot be expressed as a single-table helper.
      return { dimUpper: physicalUpper, displayName: dispName, baseColId: found.colId, onFact: found.el === factEl };
    }

    function _ensureHelper(
      effectiveDims: string[],          // ordered uppercase warehouse identifiers
      dimResolved: { dimUpper: string; displayName: string; baseColId?: string }[],
      relNameSuggestion: string
    ): { helper: any; signatureKey: string } | null {
      if (effectiveDims.length === 0) return null;
      const signatureKey = effectiveDims.slice().sort().join(',');
      const existing = lodHelpers[signatureKey];
      if (existing) return { helper: existing.element, signatureKey };

      const helperId = sigmaShortId();
      const helperCols: any[] = [];
      const helperOrder: string[] = [];
      const groupDimColIds: string[] = [];
      for (const d of dimResolved) {
        const colId = sigmaShortId();
        helperCols.push({ id: colId, formula: `[Custom SQL/${d.dimUpper}]`, name: d.displayName });
        helperOrder.push(colId);
        groupDimColIds.push(colId);
      }
      const helperEl: any = {
        id: helperId,
        kind: 'table',
        name: relNameSuggestion,
        source: {
          connectionId: connId,
          kind: 'sql',
          statement: '__PLACEHOLDER__'   // filled in below once aggs are known
        },
        columns: helperCols,
        order: helperOrder,
      };
      // Internal: defer SQL emission until we know all aggregates
      lodHelpers[signatureKey] = {
        element: helperEl,
        groupDimNames: effectiveDims.slice(),
        groupDimColIds,
        aggsByExpr: {},
        relationshipName: relNameSuggestion,
      };
      lodChildElements.push(helperEl);
      return { helper: helperEl, signatureKey };
    }

    function _ensureRelationship(
      sigKey: string,
      dimResolved: { dimUpper: string; displayName: string; baseColId?: string }[],
      relName: string
    ): void {
      const rec = lodHelpers[sigKey];
      if (!rec) return;
      // Check if relationship to this helper already exists on the base
      const existing = (factEl as any).relationships || [];
      if (existing.find((r: any) => r.targetElementId === rec.element.id)) return;
      // Build keys array using base column ids and helper dim col ids
      const keys: any[] = [];
      for (let i = 0; i < dimResolved.length; i++) {
        const baseColId = dimResolved[i].baseColId;
        const helperColId = rec.groupDimColIds[i];
        if (!baseColId || !helperColId) return;
        keys.push({ sourceColumnId: baseColId, targetColumnId: helperColId });
      }
      if (!(factEl as any).relationships) (factEl as any).relationships = [];
      (factEl as any).relationships.push({
        id: sigmaShortId(),
        targetElementId: rec.element.id,
        keys,
        name: relName,
      });
    }

    function _addAggToHelper(sigKey: string, alias: string, aggFunc: string, aggExpr: string, caption: string): { alias: string; caption: string } {
      const rec = lodHelpers[sigKey];
      if (!rec) return { alias, caption };
      // dedup by (aggFunc, aggExpr)
      const dedupKey = `${aggFunc}::${aggExpr}`;
      const ex = rec.aggsByExpr[dedupKey];
      if (ex) return { alias: ex.alias, caption: ex.caption };
      const calcId = sigmaShortId();
      rec.aggsByExpr[dedupKey] = { alias, aggFunc, aggExpr, calcId, caption };
      // Add the calc column referencing the SQL alias; column name = user-facing caption
      rec.element.columns.push({ id: calcId, formula: `[Custom SQL/${alias}]`, name: caption });
      rec.element.order.push(calcId);
      return { alias, caption };
    }

    function _finalizeHelpers(): void {
      const fe = factEl as any;
      const baseFqTable = (fe?.source?.path && fe.source.path.length >= 2)
        ? fe.source.path.join('.')
        : factTableName;
      for (const sigKey of Object.keys(lodHelpers)) {
        const rec = lodHelpers[sigKey];
        const dimList = rec.groupDimNames.join(', ');
        const aggParts: string[] = [];
        for (const k of Object.keys(rec.aggsByExpr)) {
          const a = rec.aggsByExpr[k];
          let sqlAggFunc = a.aggFunc;
          if (sqlAggFunc === 'COUNTD') sqlAggFunc = 'COUNT(DISTINCT ' + a.aggExpr + ')';
          else sqlAggFunc = `${sqlAggFunc}(${a.aggExpr})`;
          aggParts.push(`${sqlAggFunc} AS ${a.alias}`);
        }
        const groupByIdx = rec.groupDimNames.map((_d, i) => i + 1).join(', ');
        rec.element.source.statement =
          `SELECT ${dimList}, ${aggParts.join(', ')} FROM ${baseFqTable} GROUP BY ${groupByIdx}`;
      }
    }

    // ── Top-N / Bottom-N Set helper-element registry (kind:sql) ───────────
    // Each Tableau Top-N set becomes a kind:'sql' RANK helper element + a
    // relationship from the base on the dim key (and partition cols, if any).
    // The set's boolean is exposed as IS_TOP_N on the helper; calc formulas
    // referencing the set caption are rewritten to [<HelperRel>/IS_TOP_N].
    const topNHelpers: Array<{
      element: any;
      isTopNColId: string;
      relationshipName: string;
      setNames: string[];   // captions/setNames that resolve to this helper's IS_TOP_N
    }> = [];
    const topNUsedAliases = new Set<string>();
    // Maps caption (lowercased+stripped) → { helperEl, isTopNDisplayName, relName, isTopNColId }
    const topNSetIndex: Record<string, {
      helperEl: any;
      relName: string;
      isTopNDisplayName: string;
      helperElName: string;
      isTopNColId: string;
    }> = {};

    function _emitTopNHelper(top: TopNResult): boolean {
      // Resolve the dim key to a base column
      const keyResolved = _resolveDimDisplayName(top.dimField);
      if (!keyResolved || !keyResolved.baseColId) {
        warnings.push(`⚠ Set "${top.caption}": ranking key [${top.dimField}] not found on base; skipped.`);
        return false;
      }
      // Resolve partition dims, if any
      const partResolved: { dimUpper: string; displayName: string; baseColId?: string }[] = [];
      for (const p of top.partitionBy) {
        const r = _resolveDimDisplayName(p);
        if (!r || !r.baseColId) {
          warnings.push(`⚠ Set "${top.caption}": partition dim [${p}] not found on base; skipped.`);
          return false;
        }
        partResolved.push(r);
      }

      // Determine N expression — either a literal or a DM control reference
      let nLiteral: string;
      let controlId: string | null = null;
      if (top.count !== null) {
        nLiteral = String(top.count);
      } else if (top.countControl) {
        const param = parameters.find(p =>
          p.name.toUpperCase() === top.countControl!.toUpperCase()
          || (p as any).rawName?.toUpperCase() === top.countControl!.toUpperCase()
          || sigmaDisplayName(p.name).toUpperCase() === sigmaDisplayName(top.countControl!).toUpperCase()
        );
        // Use the parameter's user-facing name when known so the control id
        // matches the existing parameter→control emit step.
        const ctlSourceName = param?.name || top.countControl;
        const cidBase = sigmaDisplayName(ctlSourceName).replace(/\s+/g, '-');
        controlId = cidBase;
        const defaultVal = parseInt(param?.defaultVal || '10', 10) || 10;
        if (param) topNParamControls[param.name] = { controlId: cidBase, defaultVal };
        topNParamControls[top.countControl] = { controlId: cidBase, defaultVal };
        // The IS_TOP_N column lives on the helper element; reference the control as a Sigma column formula
        nLiteral = `[${cidBase}]`;
      } else {
        warnings.push(`⚠ Set "${top.caption}": no count or count-control; skipped.`);
        return false;
      }

      const dirSql = top.direction === 'bottom' ? 'ASC' : 'DESC';
      const aliasBase = _topNAlias(top.caption, topNUsedAliases);
      const helperId = sigmaShortId();
      const cols: any[] = [];
      const order: string[] = [];

      // Helper columns (in select order):
      //   <KEY>            (warehouse identifier)
      //   <PART>...        (one per partition col)
      //   TOTAL (numeric, the ranked aggregate value)
      //   RNK (rank int)
      //   IS_TOP_N (bool — uses RANK() result; conditional on N which may be a control ref)
      const keyColId = sigmaShortId();
      cols.push({ id: keyColId, formula: `[Custom SQL/${keyResolved.dimUpper}]`, name: keyResolved.displayName });
      order.push(keyColId);

      const partColIds: string[] = [];
      for (const p of partResolved) {
        const pid = sigmaShortId();
        cols.push({ id: pid, formula: `[Custom SQL/${p.dimUpper}]`, name: p.displayName });
        order.push(pid);
        partColIds.push(pid);
      }

      // TOTAL & RNK come straight from SQL aliases
      const totalColId = sigmaShortId();
      cols.push({ id: totalColId, formula: '[Custom SQL/TOTAL]', name: `${top.caption} Total` });
      order.push(totalColId);
      const rnkColId = sigmaShortId();
      cols.push({ id: rnkColId, formula: '[Custom SQL/RNK]', name: `${top.caption} Rank` });
      order.push(rnkColId);

      // IS_TOP_N column. Two emit modes:
      //   • literal N → put `(rnk <= N) AS IS_TOP_N` in the SQL, expose as
      //     [Custom SQL/IS_TOP_N] (matches spike Case 1 / Case 3).
      //   • parameterized N → emit a Sigma calc-col formula referencing the
      //     local rank column display name and the DM control by id (matches
      //     spike Case 2 — controls are not bindable inside SQL).
      const isTopNColId = sigmaShortId();
      const isTopNName = `${top.caption} ${top.direction === 'bottom' ? 'Bottom' : 'Top'} N`;
      const rankColName = `${top.caption} Rank`;
      let isTopNFormula: string;
      let emitIsTopNInSql = false;
      if (controlId) {
        // Parameterized — Sigma calc col referencing the control + local Rank col display name
        isTopNFormula = `[${rankColName}] <= [${controlId}]`;
      } else {
        // Literal — emit boolean in SQL and reference via [Custom SQL/IS_TOP_N]
        isTopNFormula = '[Custom SQL/IS_TOP_N]';
        emitIsTopNInSql = true;
      }
      cols.push({ id: isTopNColId, formula: isTopNFormula, name: isTopNName });
      order.push(isTopNColId);

      // Build the WITH agg / ranked / SELECT statement
      const fe = factEl as any;
      const baseFqTable = (fe?.source?.path && fe.source.path.length >= 2)
        ? fe.source.path.join('.')
        : factTableName;
      const groupCols = [keyResolved.dimUpper, ...partResolved.map(p => p.dimUpper)];
      const groupByIdx = groupCols.map((_g, i) => i + 1).join(', ');
      let aggSql = top.byAggFunc;
      if (aggSql === 'COUNTD') aggSql = `COUNT(DISTINCT ${top.byField})`;
      else aggSql = `${aggSql}(${top.byField})`;
      const partBy = top.partitionBy.length > 0
        ? `PARTITION BY ${top.partitionBy.join(', ')} `
        : '';
      const overClause = `RANK() OVER (${partBy}ORDER BY s ${dirSql})`;
      const innerSelect =
        `SELECT ${groupCols.join(', ')}, ${aggSql} AS s FROM ${baseFqTable} GROUP BY ${groupByIdx}`;
      const rankedSelect =
        `SELECT ${groupCols.join(', ')}, s, ${overClause} AS RNK FROM agg`;
      const outerCols = emitIsTopNInSql
        ? `${groupCols.join(', ')}, s AS TOTAL, RNK, (RNK <= ${nLiteral}) AS IS_TOP_N`
        : `${groupCols.join(', ')}, s AS TOTAL, RNK`;
      const outerSelect = `SELECT ${outerCols} FROM ranked`;
      const statement = `WITH agg AS (${innerSelect}), ranked AS (${rankedSelect}) ${outerSelect}`;

      const helperEl: any = {
        id: helperId,
        kind: 'table',
        // No element-level name field for kind:sql elements (per spec rule 3).
        source: { connectionId: connId, kind: 'sql', statement },
        columns: cols,
        order,
      };
      // Sigma data-model elements need *some* name to render in folders/UI.
      // Helper elements created elsewhere in this file use a `name` (e.g. window
      // helper). We follow the same convention.
      helperEl.name = `${top.caption} Top-N Helper`;

      // Wire relationship from base on the dim key (and partition cols)
      const relName = `${factTableName}_TOPN_${aliasBase}`;
      if (!fe.relationships) fe.relationships = [];
      const relKeys: any[] = [
        { sourceColumnId: keyResolved.baseColId, targetColumnId: keyColId },
      ];
      for (let i = 0; i < partResolved.length; i++) {
        const baseColId = partResolved[i].baseColId;
        if (baseColId) relKeys.push({ sourceColumnId: baseColId, targetColumnId: partColIds[i] });
      }
      fe.relationships.push({
        id: sigmaShortId(),
        targetElementId: helperEl.id,
        keys: relKeys,
        name: relName,
      });

      topNHelpers.push({
        element: helperEl,
        isTopNColId,
        relationshipName: relName,
        setNames: [top.setName, top.caption],
      });
      const idxEntry = {
        helperEl,
        relName,
        isTopNDisplayName: isTopNName,
        helperElName: helperEl.name,
        isTopNColId,
      };
      topNSetIndex[top.setName.toUpperCase()] = idxEntry;
      topNSetIndex[top.caption.toUpperCase()] = idxEntry;
      warnings.push(`✅ Set "${top.caption}" (${top.direction.toUpperCase()}-${top.count !== null ? top.count : '[' + top.countControl + ']'}${top.partitionBy.length ? ' per ' + top.partitionBy.join(',') : ''}) → kind:sql RANK helper`);
      return true;
    }

    // ── Window-calc helper element registry ───────────────────────────────
    // Window calcs (RUNNING_*, WINDOW_*, LOOKUP, RANK, INDEX, FIRST, LAST,
    // PREVIOUS_VALUE) cannot be expressed via Sigma DM formulas (the partitioned/
    // ordered window forms either don't exist or only accept a single arg). We
    // lower them to a kind:'sql' helper element with explicit OVER clauses, then
    // wire a relationship from the base on the partition dim columns so the
    // workbook can reference [<helper>/<rel>/<calc>] cross-element refs.
    const windowWsIndex = _buildWindowWorksheetIndex(parsed);
    const windowHelpers: Record<string, {
      element: any;
      partitionDimNames: string[];     // e.g. ['REGION']
      orderDimRaw: string | null;      // e.g. 'ORDER_DATE' (warehouse identifier) or null
      orderDimAlias: string | null;    // 'ORDER_MONTH' if truncated, else same as orderDimRaw
      orderDimDateTrunc: string | null;// 'month' | 'year' | etc, or null when orderDimRaw is not date-truncated
      partitionDimColIds: string[];    // helper col ids matching partitionDimNames
      orderDimColId: string | null;    // helper col id for the order dim (if present)
      innerAggs: Record<string, { alias: string }>;  // dedup base aggregates keyed by aggFunc::expr
      windowAliases: Set<string>;
      windowOverParts: string[];       // ALIAS_AS_OVER_CLAUSE strings, in emit order
      relationshipName: string;
    }> = {};
    const windowUsedAliases = new Set<string>();
    const windowChildElements: any[] = [];

    function _ensureWindowHelper(
      partitionDims: { dimUpper: string; displayName: string; baseColId?: string }[],
      orderDimRaw: string | null,
      orderDimDateTrunc: string | null,
      relName: string,
    ): { helper: any; key: string; rec: any } | null {
      const partKey = partitionDims.map(d => d.dimUpper).slice().sort().join(',');
      const orderKey = orderDimRaw ? `${orderDimRaw}|${orderDimDateTrunc || ''}` : '';
      const key = partKey + '||' + orderKey;
      const existing = windowHelpers[key];
      if (existing) return { helper: existing.element, key, rec: existing };
      if (partitionDims.length === 0 && !orderDimRaw) return null;

      const helperId = sigmaShortId();
      const cols: any[] = [];
      const order: string[] = [];
      const partitionDimColIds: string[] = [];
      for (const d of partitionDims) {
        const colId = sigmaShortId();
        cols.push({ id: colId, formula: `[Custom SQL/${d.dimUpper}]`, name: d.displayName });
        order.push(colId);
        partitionDimColIds.push(colId);
      }
      let orderDimColId: string | null = null;
      let orderDimAlias: string | null = null;
      if (orderDimRaw) {
        orderDimAlias = orderDimDateTrunc
          ? `${orderDimRaw.replace(/_DATE$/, '')}_${orderDimDateTrunc.toUpperCase()}`
          : orderDimRaw;
        // Avoid alias collision with partition dims
        if (partitionDims.find(p => p.dimUpper === orderDimAlias)) {
          orderDimAlias = `${orderDimAlias}_W`;
        }
        const oid = sigmaShortId();
        cols.push({
          id: oid,
          formula: `[Custom SQL/${orderDimAlias}]`,
          name: sigmaDisplayName(orderDimAlias),
        });
        order.push(oid);
        orderDimColId = oid;
      }

      const helperEl: any = {
        id: helperId,
        kind: 'table',
        name: relName,
        source: { connectionId: connId, kind: 'sql', statement: '__PLACEHOLDER__' },
        columns: cols,
        order,
      };
      const rec = {
        element: helperEl,
        partitionDimNames: partitionDims.map(d => d.dimUpper),
        orderDimRaw,
        orderDimAlias,
        orderDimDateTrunc,
        partitionDimColIds,
        orderDimColId,
        innerAggs: {},
        windowAliases: new Set<string>(),
        windowOverParts: [] as string[],
        relationshipName: relName,
      };
      windowHelpers[key] = rec;
      windowChildElements.push(helperEl);

      // Wire relationship from base on partition dim columns
      const baseRels = (factEl as any).relationships || [];
      const alreadyLinked = baseRels.find((r: any) => r.targetElementId === helperEl.id);
      if (!alreadyLinked && partitionDims.length > 0 && partitionDims.every(d => d.baseColId)) {
        if (!(factEl as any).relationships) (factEl as any).relationships = [];
        const keys: any[] = [];
        for (let i = 0; i < partitionDims.length; i++) {
          keys.push({ sourceColumnId: partitionDims[i].baseColId, targetColumnId: partitionDimColIds[i] });
        }
        (factEl as any).relationships.push({
          id: sigmaShortId(),
          targetElementId: helperEl.id,
          keys,
          name: relName,
        });
      }
      return { helper: helperEl, key, rec };
    }

    function _registerInnerAgg(rec: any, aggFunc: string, exprSql: string): string {
      // Deduplicates the base SUM(SALES) AS SALES so multiple OVER clauses share it.
      const key = `${aggFunc}::${exprSql}`;
      if (rec.innerAggs[key]) return rec.innerAggs[key].alias;
      // Pick a clean alias from the column expression: SALES, PROFIT, etc.
      const idMatch = exprSql.match(/[A-Z][A-Z0-9_]*/);
      let alias = idMatch ? idMatch[0] : 'VAL';
      let n = 2;
      while (rec.windowAliases.has(alias) || Object.values(rec.innerAggs).some((v: any) => v.alias === alias)) {
        alias = idMatch ? `${idMatch[0]}_${n++}` : `VAL_${n++}`;
      }
      rec.innerAggs[key] = { alias };
      return alias;
    }

    function _emitWindowOverClause(
      rec: any,
      win: WindowResult,
      windowAlias: string,
      innerAlias: string,
    ): { ok: boolean; reason?: string } {
      const partBy = rec.partitionDimNames.length > 0
        ? `PARTITION BY ${rec.partitionDimNames.join(', ')}`
        : '';
      const orderBy = rec.orderDimAlias ? `ORDER BY ${rec.orderDimAlias}` : '';
      const windowSpec = (parts: string[]) => parts.filter(Boolean).join(' ');

      let overSql = '';
      switch (win.windowType) {
        case 'RUNNING_SUM':
        case 'RUNNING_AVG':
        case 'RUNNING_MIN':
        case 'RUNNING_MAX': {
          if (!rec.orderDimAlias) return { ok: false, reason: 'no order dim' };
          const fn = win.windowType.replace('RUNNING_', '');
          overSql = `${fn}(${innerAlias}) OVER (${windowSpec([partBy, orderBy])} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`;
          break;
        }
        case 'WINDOW_SUM':
        case 'WINDOW_AVG':
        case 'WINDOW_MIN':
        case 'WINDOW_MAX':
        case 'WINDOW_COUNT': {
          const fn = win.windowType.replace('WINDOW_', '');
          // Full-partition agg — no ORDER BY
          overSql = `${fn}(${innerAlias}) OVER (${partBy})`;
          break;
        }
        case 'LOOKUP':
        case 'PREVIOUS_VALUE': {
          if (!rec.orderDimAlias) return { ok: false, reason: 'no order dim' };
          const offset = win.lookupOffset ?? -1;
          const fn = offset < 0 ? 'LAG' : (offset > 0 ? 'LEAD' : '');
          if (!fn) {
            overSql = `${innerAlias}`;  // LOOKUP(x, 0) is identity
          } else {
            overSql = `${fn}(${innerAlias}, ${Math.abs(offset)}) OVER (${windowSpec([partBy, orderBy])})`;
          }
          break;
        }
        case 'RANK':
        case 'RANK_DENSE':
        case 'RANK_UNIQUE': {
          // For RANK with no inner expr, rank by first inner agg DESC.
          let rankExpr = innerAlias;
          if (!rankExpr) {
            const firstInner = Object.values(rec.innerAggs)[0] as any;
            if (firstInner) rankExpr = firstInner.alias;
            else return { ok: false, reason: 'rank has no measure to order by' };
          }
          const dir = (win.rankDirection || 'desc').toUpperCase();
          const rankFn = win.windowType === 'RANK_DENSE' ? 'DENSE_RANK'
                       : win.windowType === 'RANK_UNIQUE' ? 'ROW_NUMBER'
                       : 'RANK';
          overSql = `${rankFn}() OVER (${windowSpec([partBy, `ORDER BY ${rankExpr} ${dir}`])})`;
          break;
        }
        case 'INDEX': {
          if (!rec.orderDimAlias) return { ok: false, reason: 'no order dim' };
          overSql = `ROW_NUMBER() OVER (${windowSpec([partBy, orderBy])})`;
          break;
        }
        case 'FIRST': {
          if (!rec.orderDimAlias) return { ok: false, reason: 'no order dim' };
          // Tableau FIRST() returns offset from partition start (negative number).
          // Approximate as ROW_NUMBER()-1 negated.
          overSql = `(1 - ROW_NUMBER() OVER (${windowSpec([partBy, orderBy])}))`;
          break;
        }
        case 'LAST': {
          if (!rec.orderDimAlias) return { ok: false, reason: 'no order dim' };
          overSql = `(COUNT(*) OVER (${partBy}) - ROW_NUMBER() OVER (${windowSpec([partBy, orderBy])}))`;
          break;
        }
        default:
          return { ok: false, reason: 'unsupported window type ' + win.windowType };
      }
      rec.windowOverParts.push(`${overSql} AS ${windowAlias}`);
      rec.windowAliases.add(windowAlias);
      // Add a calc column referencing the alias
      const calcId = sigmaShortId();
      rec.element.columns.push({ id: calcId, formula: `[Custom SQL/${windowAlias}]` });
      rec.element.order.push(calcId);
      return { ok: true };
    }

    function _finalizeWindowHelpers(): void {
      const fe = factEl as any;
      const baseFqTable = (fe?.source?.path && fe.source.path.length >= 2)
        ? fe.source.path.join('.')
        : factTableName;
      for (const key of Object.keys(windowHelpers)) {
        const rec = windowHelpers[key];
        const selectParts: string[] = [];
        // Partition dims (passed through bare)
        for (const d of rec.partitionDimNames) selectParts.push(d);
        // Order dim (with optional DATE_TRUNC)
        if (rec.orderDimRaw && rec.orderDimAlias) {
          if (rec.orderDimDateTrunc) {
            selectParts.push(`DATE_TRUNC('${rec.orderDimDateTrunc}', ${rec.orderDimRaw}) AS ${rec.orderDimAlias}`);
          } else {
            selectParts.push(`${rec.orderDimRaw} AS ${rec.orderDimAlias}`);
          }
        }
        // Inner aggregates (e.g. SUM(SALES) AS SALES)
        for (const k of Object.keys(rec.innerAggs)) {
          const [aggFunc, exprSql] = k.split('::');
          const a = rec.innerAggs[k];
          let sqlFn = aggFunc;
          if (sqlFn === 'COUNTD') sqlFn = `COUNT(DISTINCT ${exprSql})`;
          else sqlFn = `${sqlFn}(${exprSql})`;
          selectParts.push(`${sqlFn} AS ${a.alias}`);
        }
        // Pre-aggregate happens in an inner subquery so OVER clauses see clean aliases.
        const groupByCount = rec.partitionDimNames.length + (rec.orderDimRaw ? 1 : 0);
        const groupByIdx = Array.from({ length: groupByCount }, (_, i) => i + 1).join(', ');
        const baseSelect = `SELECT ${selectParts.join(', ')} FROM ${baseFqTable} GROUP BY ${groupByIdx}`;

        // Outer SELECT: pass through everything from the inner CTE plus the OVER aliases.
        const innerProjection: string[] = [
          ...rec.partitionDimNames,
          ...(rec.orderDimAlias ? [rec.orderDimAlias] : []),
          ...Object.values(rec.innerAggs).map((v: any) => v.alias),
        ];
        const outerProjection = innerProjection.concat(rec.windowOverParts);
        rec.element.source.statement =
          `WITH base AS (${baseSelect}) SELECT ${outerProjection.join(', ')} FROM base`;
      }
    }

    for (const col of asArray(ds.ds?.column || [])) {
      const rawName = attr(col, 'name') || '';
      let caption = attr(col, 'caption') || rawName.replace(/^\[|\]$/g, '');
      const hidden = attr(col, 'hidden') === 'true';
      const calcEl = col.calculation;
      // Resolve any internal-GUID field references in the calc formula to their display
      // captions up front, so LOD/window/set/regular-calc translation and cross-element
      // placement all operate on resolvable display names rather than opaque UUIDs.
      const formula = calcEl ? rewriteGuidRefs(attr(calcEl, 'formula') || '') : '';
      const fieldKey = rawName.replace(/^\[|\]$/g, '');

      // Skip Tableau-internal / derived columns that don't exist in the warehouse:
      //   datatype="table"  → object-graph internal reference
      //   name ends (group) → Tableau group set (e.g. "[Product Name (group)]")
      //   name ends (bin)   → Tableau bin column (e.g. "[Profit (bin)]")
      const colDatatype = attr(col, 'datatype') || '';
      if (
        hidden || !fieldKey ||
        fieldKey.startsWith('Number of Records') ||
        fieldKey.includes('__tableau_internal_object_id__') ||
        colDatatype === 'table' ||
        /\(group\)\s*$/i.test(fieldKey) ||
        /\(bin\)\s*$/i.test(fieldKey)
      ) continue;

      // ── Virtual-connection (collection) GUID / flattened-dimension handling ──
      // In a collection datasource, the physical fact + dimension columns are listed
      // here as GUID-named <column> entries (and dimension columns are flattened onto
      // the fact with a " (TABLE (schema.TABLE))" caption suffix). Emitting those as
      // [ORDER_FACT/…] columns invents columns that don't exist in the physical fact
      // table (phantoms) and leaves UUID display names. We:
      //   * SKIP a non-calc GUID column whose owning relation (per <cols><map>) is NOT
      //     the fact relation — it belongs to a related DIM element, reachable via the
      //     relationship, not a physical fact column.
      //   * SKIP a non-calc column whose caption carries the flatten suffix (a
      //     dimension column denormalised onto the fact).
      //   * For a genuine fact GUID column, resolve its display caption so it isn't
      //     emitted with a bare UUID name.
      const guidMatch = (fieldKey.match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i) || [])[0];
      if (factRelName && !formula) {
        const flattenSuffix = /\s\([^()]*\([^)]*\)\)\s*$/.test(caption);
        if (guidMatch) {
          // Tableau-derived relation column (e.g. date-parsed) → no physical column; skip.
          if (derivedRelColGuids.has(guidMatch.toLowerCase())) continue;
          const owner = guidOwnerRel[guidMatch.toLowerCase()];
          const cap   = guidCaption[guidMatch.toLowerCase()];
          // Owned by a non-fact relation, or caption is a flattened dim → skip.
          if ((owner && owner !== factRelName) || flattenSuffix) continue;
          // Genuine fact column: use the resolved display caption.
          if (cap) caption = cap.trim();
        } else if (flattenSuffix) {
          continue;
        }
      }

      // ── Top-N / Bottom-N Set → kind:sql RANK helper element ─────────
      if (calcEl && attr(calcEl, 'class') === 'categorical-set') {
        const topN = tableauParseTopNSet(calcEl, caption, fieldKey);
        if (topN) {
          _emitTopNHelper(topN);
          continue;
        }
        // Condition/member-based set → boolean calc column on factEl so other
        // calcs that reference it by name (e.g. `If([High Value Orders], ...)`)
        // resolve. Mirrors the smm browser tool's set-handling path.
        const setFormula = (attr(calcEl, 'formula') || '').trim();
        let sigmaSetFormula: string | null = null;
        if (setFormula) {
          // Condition-based set — formula attribute holds a row predicate
          sigmaSetFormula = tableauFormulaToSigma(setFormula, warnings);
        } else {
          // Member-based set — <groupfilter function="member"> children list values
          const memberFilters: any[] = [];
          const collectMembers = (node: any) => {
            if (!node || typeof node !== 'object') return;
            for (const k of Object.keys(node)) {
              if (k === 'groupfilter') {
                for (const gf of asArray(node[k])) {
                  if (attr(gf, 'function') === 'member') memberFilters.push(gf);
                  collectMembers(gf);
                }
              } else if (typeof node[k] === 'object') {
                collectMembers(node[k]);
              }
            }
          };
          collectMembers(calcEl);
          if (memberFilters.length > 0) {
            const membersByField: Record<string, string[]> = {};
            for (const gf of memberFilters) {
              const level = (attr(gf, 'level') || '').replace(/^\[|\]$/g, '');
              const val = (attr(gf, 'member') || '').replace(/^"|"$/g, '');
              if (!level || !val) continue;
              (membersByField[level] || (membersByField[level] = [])).push(val);
            }
            const conditions = Object.entries(membersByField).map(([f, vals]) => {
              const dn = sigmaDisplayName(f);
              return vals.length === 1
                ? `[${dn}] = "${vals[0]}"`
                : vals.map(v => `[${dn}] = "${v}"`).map(c => `(${c})`).join(' Or ');
            });
            sigmaSetFormula = conditions.length === 1
              ? conditions[0]
              : conditions.map(c => `(${c})`).join(' And ');
          }
        }
        if (!sigmaSetFormula) {
          warnings.push(`⚠ Set "${caption}": unrecognised set definition — skipped.`);
          continue;
        }
        const _setColId = sigmaShortId();
        const _setFmt = inferSigmaFormat(String(sigmaSetFormula), caption);
        const _setCol: any = { id: _setColId, formula: String(sigmaSetFormula), name: caption };
        if (_setFmt) _setCol.format = _setFmt;
        factEl.columns.push(_setCol);
        factEl.order.push(_setColId);
        // Track the set as a known display name so cross-element link rewrites
        // (and chained calcs) can resolve [<caption>] as a local column.
        displayNameMap[caption.toUpperCase()] = { colId: _setColId, el: factEl };
        globalColMap[caption.toUpperCase()] = { elId: factEl.id, displayName: caption };
        warnings.push(`✅ Set "${caption}" → boolean column: ${String(sigmaSetFormula).slice(0, 80)}`);
        continue;
      }

      if (!formula) {
        // Regular (non-calculated) source column — add to factEl if not already tracked
        const physCol = normalizeColumnName(fieldKey);
        const displayName = caption || sigmaDisplayName(physCol);
        if (!displayNameMap[displayName.toUpperCase()] && !displayNameMap[physCol]) {
          const colId = sigmaInodeId(physCol);
          factEl.columns.push({ id: colId, formula: `[${factTableName}/${displayName}]` });
          factEl.order.push(colId);
          displayNameMap[displayName.toUpperCase()] = { colId, el: factEl };
          displayNameMap[physCol] = { colId, el: factEl };
          globalColMap[displayName.toUpperCase()] = { elId: factEl.id, displayName };

          // Auto-generate Sum() metric for numeric measure columns
          const role = attr(col, 'role') || '';
          const dataType = attr(col, 'datatype') || '';
          const isNumeric = dataType === 'real' || dataType === 'integer' || dataType === 'decimal';
          if (role === 'measure' && isNumeric) {
            if (!(factEl as any).metrics) (factEl as any).metrics = [];
            const _autoFmt = inferSigmaFormat(`Sum([${displayName}])`, displayName);
            const _autoMetric: any = { id: sigmaShortId(), formula: `Sum([${displayName}])`, name: displayName };
            if (_autoFmt) _autoMetric.format = _autoFmt;
            (factEl as any).metrics.push(_autoMetric);
          }
        }
        continue;
      }

      // Calculated field
      {
        // Check for LOD expression
        const lod = tableauParseLOD(formula);
        if (lod) {
          // Resolve LOD dim names → display + warehouse identifiers
          const lodDimsResolved: { dimUpper: string; displayName: string; baseColId?: string }[] = [];
          let allFound = true;
          let dimOffFact = false;
          for (const dimName of lod.dims) {
            const r = _resolveDimDisplayName(dimName);
            if (r) {
              lodDimsResolved.push(r);
              // The LOD helper SELECT runs against the fact table. A declared dim that
              // physically lives on a related dimension element (virtual-connection
              // flattening) can't be projected from the fact alone — that needs a join,
              // not a single-table helper. Route to OPEN QUESTIONS rather than emit
              // SQL referencing a column the fact table doesn't have.
              if (r.onFact === false) dimOffFact = true;
            }
            else { allFound = false; warnings.push(`⚠ LOD "${caption}" dim [${dimName}] not found`); }
          }
          if (dimOffFact) {
            warnings.push(`⚠ LOD "${caption}" (${lod.lodType}) groups by a dimension-table column (cross-table grain); not mechanizable as a single-table helper — needs manual Sigma authoring. Skipped.`);
            continue;
          }

          // Determine view contexts where this calc field is used
          const fieldKeyUpper = fieldKey.toUpperCase();
          const viewContexts = wsIndex.byField.get(fieldKeyUpper) || [];
          const viewDimSets: string[][] = viewContexts.length > 0
            ? viewContexts.map(c => c.dims.slice())
            : [[]];   // fallback when no worksheet uses the calc

          // Compute effective grouping(s) per LOD type. Dedup by signature.
          const effectiveSets: string[][] = [];
          const seenSigs = new Set<string>();
          for (const viewDims of viewDimSets) {
            let effective: string[];
            if (lod.lodType === 'FIXED') {
              effective = lodDimsResolved.map(d => d.dimUpper);
            } else if (lod.lodType === 'INCLUDE') {
              const set = new Set<string>(viewDims);
              for (const d of lodDimsResolved) set.add(d.dimUpper);
              effective = Array.from(set);
            } else { // EXCLUDE
              const exclude = new Set(lodDimsResolved.map(d => d.dimUpper));
              effective = viewDims.filter(v => !exclude.has(v));
            }
            const sig = effective.slice().sort().join(',');
            if (sig && !seenSigs.has(sig)) {
              seenSigs.add(sig);
              effectiveSets.push(effective);
            }
          }

          if (lod.lodType === 'FIXED' && lod.dims.length === 0) {
            // Table-scoped FIXED → metric (existing behavior)
            if (!(factEl as any).metrics) (factEl as any).metrics = [];
            (factEl as any).metrics.push({ id: sigmaShortId(), formula: lod.sigmaAgg, name: caption });
            warnings.push(`✅ LOD "${caption}" (table-scoped FIXED) → metric: ${lod.sigmaAgg}`);
            continue;
          }

          if (!allFound) continue;

          if (effectiveSets.length === 0) {
            warnings.push(`⚠ LOD "${caption}" (${lod.lodType}) — no view context found and dims empty; skipped.`);
            continue;
          }

          // For INCLUDE/EXCLUDE we need real view dims. For FIXED, lodDimsResolved already
          // has the warehouse-identifier list and the matching baseColIds.
          for (const effective of effectiveSets) {
            // Build dimResolved aligned to `effective`. Each entry needs a baseColId on the fact element.
            const dimResolved: { dimUpper: string; displayName: string; baseColId?: string }[] = [];
            let ok = true;
            for (const dn of effective) {
              const m = _resolveDimDisplayName(dn);
              if (!m) { ok = false; warnings.push(`⚠ LOD "${caption}" view dim [${dn}] not found on base`); break; }
              dimResolved.push(m);
            }
            if (!ok) continue;

            const alias = _lodAlias(caption, usedAliases);
            const relName = `${lod.lodType} ${lod.dims.join(', ') || '(table)'}` + (effectiveSets.length > 1 ? ` @ ${effective.join('×')}` : '');
            const helperRes = _ensureHelper(effective, dimResolved, `${factTableName} ${lod.lodType} ${effective.join(', ')}`);
            if (!helperRes) continue;
            _ensureRelationship(helperRes.signatureKey, dimResolved, relName);
            _addAggToHelper(helperRes.signatureKey, alias, lod.aggFunc, lod.aggExpr, caption);
            // The LOD value is exposed via the helper element + relationship.
            // Workbook authors reference it as [HELPER_ELEMENT/REL_NAME/Caption]; we do not
            // emit a calc column on the base because cross-element formulas referencing a
            // related element's column don't validate at the data-model level.
            warnings.push(`✅ LOD "${caption}" (${lod.lodType}) → helper "${helperRes.helper.name}" alias ${alias}`);
          }
          continue;
        }

        // Check for table-calc / window expressions — lower to a kind:'sql' helper
        const win = tableauParseWindow(formula);
        if (win) {
          // Determine partition + order from the worksheet view-context heuristic.
          // Try the calc's own field-key first, then fall back to ANY worksheet
          // context (first found with rows-dim + time-dim) so all calcs in a
          // datasource share consistent partitioning even if they aren't all
          // pinned to a shelf.
          const fieldKeyUpper = fieldKey.toUpperCase();
          let ctxList = windowWsIndex.byField.get(fieldKeyUpper) || [];
          if (ctxList.length === 0) {
            // Fallback: walk all indexed contexts and collect any with rows + dateDim
            const all: WindowViewContext[] = [];
            for (const v of windowWsIndex.byField.values()) {
              for (const c of v) all.push(c);
            }
            ctxList = all;
          }
          let chosen: WindowViewContext | null = null;
          for (const c of ctxList) {
            if (c.rowsDims.length > 0 && c.dateDim) { chosen = c; break; }
          }
          if (!chosen) for (const c of ctxList) {
            if (c.rowsDims.length > 0) { chosen = c; break; }
          }
          if (!chosen && ctxList.length > 0) chosen = ctxList[0];

          // Default: rows/cols heuristic — partition = rows dims, order = first
          // cols dim (preferring a date-truncated dim if present).
          let partitionDimNames: string[] = chosen ? chosen.rowsDims.slice() : [];
          let orderDimRaw: string | null = chosen?.dateDim || null;
          // Use the cols-shelf prefix to derive the DATE_TRUNC grain. When the
          // chosen worksheet's cols carries a yr:/mn:/qr:/dy:/wk: prefix, we
          // captured the matching grain at index time. When no time prefix was
          // present the order dim should be projected as-is (no DATE_TRUNC).
          let orderDimDateTrunc: string | null = chosen?.dateGrain ?? null;
          if (!chosen?.dateDim && chosen && chosen.colsDims.length > 0) {
            orderDimRaw = chosen.colsDims[0];
            orderDimDateTrunc = null;
          }

          // Apply explicit "Compute Using" addressing override from
          // <calculation>'s <table-calculation> child if present. This trumps
          // the rows/cols heuristic for partition + order axes. We still keep
          // the cols-shelf grain mapping from above so DATE_TRUNC matches the
          // viz when the addressing field IS the date dim.
          const addressing = _parseWindowAddressing(calcEl);
          if (addressing) {
            if (addressing.mode === 'specific' && addressing.orderFields.length > 0) {
              const orderSet = new Set(addressing.orderFields.map(s => s.toUpperCase()));
              const allShelfDims = chosen
                ? Array.from(new Set([...chosen.rowsDims, ...chosen.colsDims]))
                : [];
              partitionDimNames = allShelfDims.filter(d => !orderSet.has(d.toUpperCase()));
              const firstOrder = addressing.orderFields[0];
              orderDimRaw = firstOrder;
              // If this addressing field matches the date-shelf dim, keep its grain;
              // otherwise project the field raw (no DATE_TRUNC).
              orderDimDateTrunc = (chosen?.dateDim && firstOrder.toUpperCase() === chosen.dateDim.toUpperCase())
                ? (chosen.dateGrain ?? null)
                : null;
              warnings.push(`✅ Window calc "${caption}" — addressing override: order=[${addressing.orderFields.join(',')}], partition=[${partitionDimNames.join(',')}]`);
            } else if (addressing.mode === 'table-across') {
              // Table (Across): partition = rows dims, order = cols dims (default).
              // Already matches rows/cols heuristic, no change needed.
            } else if (addressing.mode === 'table-down') {
              // Table (Down): partition = cols dims, order = rows dims (first).
              partitionDimNames = chosen ? chosen.colsDims.slice() : [];
              orderDimRaw = chosen && chosen.rowsDims.length > 0 ? chosen.rowsDims[0] : null;
              orderDimDateTrunc = null;
            } else if (addressing.mode === 'unknown') {
              warnings.push(`⚠ Window calc "${caption}" — Compute Using mode "${addressing.rawDirection}" is not yet supported; falling back to rows/cols heuristic`);
            }
          }

          // Resolve partition dims to baseColIds for the relationship
          const partitionResolved: { dimUpper: string; displayName: string; baseColId?: string }[] = [];
          let allP = true;
          for (const p of partitionDimNames) {
            const r = _resolveDimDisplayName(p);
            if (!r) { allP = false; break; }
            partitionResolved.push(r);
          }
          if (!allP || partitionResolved.length === 0) {
            warnings.push(`⚠ Window calc "${caption}" — no partition dims resolved on base; skipped`);
            continue;
          }

          const relName = `Window ${partitionDimNames.join(', ')}`
            + (orderDimRaw ? ` ORDER ${orderDimRaw}${orderDimDateTrunc ? `(${orderDimDateTrunc})` : ''}` : '');
          const helperRes = _ensureWindowHelper(partitionResolved, orderDimRaw, orderDimDateTrunc, relName);
          if (!helperRes) {
            warnings.push(`⚠ Window calc "${caption}" — could not create helper element; skipped`);
            continue;
          }
          // Register inner aggregate (e.g. SUM(SALES) → AS SALES) — even RANK()/INDEX() may not need one,
          // but we still register the SUM if a measure column is present.
          let innerAlias = '';
          if (win.innerExprSql && win.innerAggFunc) {
            innerAlias = _registerInnerAgg(helperRes.rec, win.innerAggFunc, win.innerExprSql);
          }
          const winAlias = _windowAlias(caption, windowUsedAliases);
          // Ensure column carries the user-facing caption as `name`
          const emitRes = _emitWindowOverClause(helperRes.rec, win, winAlias, innerAlias);
          if (!emitRes.ok) {
            warnings.push(`⚠ Window calc "${caption}" → ${win.windowType}: ${emitRes.reason}; skipped`);
            // Pop placeholder column we may have added
            continue;
          }
          // Patch the most recently-added column with the caption as name
          const lastCol = helperRes.rec.element.columns[helperRes.rec.element.columns.length - 1];
          if (lastCol && !lastCol.name) lastCol.name = caption;
          warnings.push(`✅ Window "${caption}" (${win.windowType}) → helper "${helperRes.rec.element.name}" alias ${winAlias}`);
          continue;
        }

        // Regular calculated field
        const sigmaFormula = tableauFormulaToSigma(formula, warnings);
        if (!sigmaFormula || sigmaFormula.startsWith('/*')) continue;

        // Row-level security: a calc that tests the viewer's identity/group
        // (USERNAME/ISMEMBEROF/USERATTRIBUTE/…). Emit a fail-closed RLS artifact —
        // a boolean calc column + an element filter keeping only True rows — instead
        // of a plain visible calc column. Mirrors the lookml access_filter pattern.
        if (tableauFormulaIsRls(formula)) {
          // Cross-element guard: a Sigma element filter must live on the same element
          // as its column. If the RLS calc references a related-table (non-fact) column,
          // the column would be moved to a derived view downstream, orphaning the filter —
          // so we don't auto-emit; we flag it for manual placement on the owning element.
          const rlsRefs = (sigmaFormula.match(/\[([^\]\/]+)\]/g) || []).map(r => r.replace(/^\[|\]$/g, ''));
          const offFact = rlsRefs.find(n => {
            if (/^(true|false|null)$/i.test(n)) return false;
            const hit = displayNameMap[n.toUpperCase()] || displayNameMap[n.replace(/\s+/g, '_').toUpperCase()];
            return hit && hit.el !== factEl;
          });
          if (offFact) {
            warnings.push(`⚠ "${caption}" is row-level security but references a related-table column [${offFact}] (cross-element). Sigma RLS filters apply per-element — re-apply this rule on the element that owns [${offFact}] (or its derived view): add a boolean calc column ${sigmaFormula.slice(0, 70)} and an element filter keeping only True.`);
            continue;
          }
          // REPORT (architecture B) — do not inject; the skill provisions + applies.
          const rlsName = /^RLS\b/i.test(caption) ? caption : `RLS: ${caption}`;
          security.push(makeRlsSecurity({ source: `Tableau calc "${caption}"`, element: factEl, name: rlsName, formula: sigmaFormula }));
          warnings.push(`🔐 "${caption}" → row-level security DETECTED (reported in result.security, not injected): ${sigmaFormula.slice(0, 80)}. The migration skill provisions the referenced attribute(s)/team(s) and applies the RLS calc + filter.`);
          continue;
        }

        if (tableauIsAggregate(formula)) {
          // A fact metric can only aggregate columns that physically live on the fact
          // element. In a virtual connection a calc may aggregate a flattened dimension
          // column (e.g. Sum([Promo Cost]) where Promo Cost is on PROMO_DIM). Sigma
          // metrics have no cross-element [SRC/REL/Field] form, so such a metric would
          // error-type the element. Detect any bare ref that resolves to a non-fact
          // (or unknown) column and route the whole metric to OPEN QUESTIONS instead.
          const refNames = (sigmaFormula.match(/\[([^\]\/]+)\]/g) || [])
            .map(r => r.replace(/^\[|\]$/g, ''));
          const offFactRef = refNames.find(n => {
            if (/^(true|false|null)$/i.test(n)) return false;
            const hit = displayNameMap[n.toUpperCase()]
              || displayNameMap[n.replace(/\s+/g, '_').toUpperCase()];
            // Unknown ref (not a tracked column) is left alone — could be a metric name
            // or constant; only drop when we KNOW it resolves to a non-fact element.
            return hit && hit.el !== factEl;
          });
          if (offFactRef) {
            warnings.push(`⚠ Metric "${caption}" aggregates a dimension-table column [${offFactRef}] (cross-element); Sigma metrics can't reference related-element columns — needs manual authoring. Skipped.`);
            continue;
          }
          if (!(factEl as any).metrics) (factEl as any).metrics = [];
          const _mFmt = inferSigmaFormat(sigmaFormula, caption);
          const _m: any = { id: sigmaShortId(), formula: sigmaFormula, name: caption };
          if (_mFmt) _m.format = _mFmt;
          (factEl as any).metrics.push(_m);
        } else {
          const colId = sigmaShortId();
          const _cFmt = inferSigmaFormat(sigmaFormula, caption);
          const _c: any = { id: colId, formula: sigmaFormula, name: caption };
          if (_cFmt) _c.format = _cFmt;
          factEl.columns.push(_c);
          factEl.order.push(colId);
          warnings.push(`ℹ "${caption}" → calculated column. Review: ${sigmaFormula.slice(0, 60)}`);
        }
      }
    }

    // Finalize LOD helper SQL statements now that all aggregates are registered
    _finalizeHelpers();
    // Finalize window helper SQL statements
    _finalizeWindowHelpers();

    // Add window helper child elements first (then LOD)
    for (const child of windowChildElements) {
      elements.push(child);
    }
    if (windowChildElements.length > 0) {
      warnings.push(`ℹ ${windowChildElements.length} window helper element(s) created (kind:sql)`);
    }

    // Add LOD helper child elements
    for (const child of lodChildElements) {
      delete child._dimKey;
      elements.push(child);
    }
    if (lodChildElements.length > 0) {
      warnings.push(`ℹ ${lodChildElements.length} LOD helper element(s) created`);
    }

    // Add Top-N helper child elements
    for (const rec of topNHelpers) {
      elements.push(rec.element);
    }
    if (topNHelpers.length > 0) {
      warnings.push(`ℹ ${topNHelpers.length} Top-N helper element(s) created (kind:sql)`);
    }
  }

  // ── Pull cross-element calc cols off source elements (moved to derived) ─
  // Calc cols whose formula references a related-table column by display name
  // cannot resolve on the source warehouse-table element — Sigma doesn't see
  // those names in scope. The smm browser tool's `buildDerivedElementsAndMoveCalcs`
  // pass moves them to the derived "<Table> View" element where the related
  // columns are surfaced via [SRC/REL/Field] formulas, then rewrites bare [X]
  // refs to that 3-segment form.
  const crossElCalcsByElId: Record<string, any[]> = {};
  for (const el of elements) {
    if (el.source?.kind !== 'warehouse-table') continue;
    if (!(el as any).relationships?.length) continue;

    const localNames = new Set<string>();
    for (const c of (el.columns || [])) {
      if (!c.formula) continue;
      const m = c.formula.match(/^\[[^\]\/]+\/([^\]]+)\]$/);
      if (m) localNames.add(m[1].toUpperCase());
      if (c.name) localNames.add(c.name.toUpperCase());
    }

    const crossEl: any[] = [];
    const keep: any[] = [];
    for (const c of (el.columns || [])) {
      if (!c.name || !c.formula) { keep.push(c); continue; }
      if (/^\[[^\]\/]+\/[^\]]+\]$/.test(c.formula)) { keep.push(c); continue; }
      const refs = c.formula.match(/\[([^\]\/]+)\]/g) || [];
      const hasCross = refs.some(ref => {
        const n = ref.replace(/^\[|\]$/g, '');
        return !/^(true|false|null)$/i.test(n) && !localNames.has(n.toUpperCase());
      });
      if (hasCross) {
        const oi = (el.order || []).indexOf(c.id);
        if (oi >= 0) (el.order as string[]).splice(oi, 1);
        crossEl.push(c);
      } else {
        keep.push(c);
      }
    }
    el.columns = keep;
    if (crossEl.length) crossElCalcsByElId[el.id] = crossEl;
  }

  // ── Parameters → Controls ───────────────────────────────────────────────
  const controls: any[] = [];
  for (const p of parameters) {
    const controlId = sigmaDisplayName(p.name).replace(/\s+/g, '-');
    // Top-N parameter: emit a single-number control with the Tableau default value.
    // (The Top-N IS_TOP_N calc col references this control by id.)
    if (topNParamControls[p.name]) {
      const def = topNParamControls[p.name];
      const defVal = parseInt(p.defaultVal || String(def.defaultVal), 10) || def.defaultVal;
      controls.push({
        kind: 'control',
        controlId: def.controlId,
        id: sigmaShortId() + 'con',
        controlType: 'number',
        mode: '<=',
        value: defVal,
        includeNulls: 'when-no-value-is-selected',
      });
      warnings.push(`ℹ Parameter "${p.name}" → number control (Top-N driver, default ${defVal})`);
      continue;
    }
    if (p.domainType === 'list' && p.members.length > 0) {
      controls.push({ kind: 'control', controlId, id: sigmaShortId() + 'con',
        controlType: 'list', mode: 'include', selectionMode: 'single', values: [],
        source: { kind: 'manual', valueType: 'text', values: p.members } });
      warnings.push(`ℹ Parameter "${p.name}" → list control`);
    } else if (p.type === 'date' || p.type === 'datetime') {
      controls.push({ kind: 'control', controlId, id: sigmaShortId() + 'con',
        controlType: 'date-range', mode: 'last', value: 90, unit: 'day', includeToday: true });
      warnings.push(`ℹ Parameter "${p.name}" → date-range control (default: last 90 days — adjust in Sigma UI)`);
    } else if (p.type === 'real' || p.type === 'integer' || p.domainType === 'range') {
      controls.push({ kind: 'control', controlId, id: sigmaShortId() + 'con',
        controlType: 'number-range' });
      warnings.push(`ℹ Parameter "${p.name}" → number-range control`);
    } else {
      controls.push({ kind: 'control', controlId, id: sigmaShortId() + 'con',
        controlType: 'text', mode: 'contains' });
      warnings.push(`ℹ Parameter "${p.name}" → text control`);
    }
  }

  // ── Derived elements (fact tables with relationships) ───────────────────
  const derivedEls = buildDerivedElements(elements);
  for (const de of derivedEls) elements.push(de);

  // Place cross-element calc cols (pulled from source above) onto their
  // matching derived element, rewriting bare [X] refs to [SRC/REL/X] form.
  const placedSrcElIds: Record<string, boolean> = {};
  for (const de of derivedEls) {
    if (de.source?.kind !== 'table' || !(de.source as any).elementId) continue;
    const srcElId = (de.source as any).elementId;
    const calcs = crossElCalcsByElId[srcElId];
    if (!calcs?.length) continue;
    const srcEl = elements.find(e => e.id === srcElId);
    if (!srcEl) continue;
    const srcBaseName = (srcEl as any).name || srcEl.source?.path?.[srcEl.source.path.length - 1] || '';
    const relatedNameMap: Record<string, string> = {};
    if (srcEl && (srcEl as any).relationships && srcBaseName) {
      for (const rel of ((srcEl as any).relationships || [])) {
        if (!rel.name) continue;
        const tgtEl = elements.find(e => e.id === rel.targetElementId);
        if (!tgtEl || tgtEl.source?.kind !== 'warehouse-table') continue;
        for (const tc of (tgtEl.columns || [])) {
          if (!tc.formula || tc.formula.startsWith('/*')) continue;
          const fm = tc.formula.match(/^\[([^\]]+)\]$/);
          if (!fm) continue;
          const inner = fm[1];
          const s = inner.lastIndexOf('/');
          const dispName = s >= 0 ? inner.slice(s + 1) : inner;
          if (!(dispName in relatedNameMap)) {
            relatedNameMap[dispName] = `${srcBaseName}/${rel.name}/${dispName}`;
          }
        }
      }
    }
    for (const c of calcs) {
      if (c.formula && Object.keys(relatedNameMap).length) {
        c.formula = c.formula.replace(/\[([^\]\/]+)\]/g, (match: string, refName: string) => {
          const rewritten = relatedNameMap[refName];
          return rewritten ? `[${rewritten}]` : match;
        });
      }
      (de.columns as any[]).push(c);
      (de.order as string[]).push(c.id);
    }
    warnings.push(`ℹ ${calcs.length} calc col(s) moved to derived "${(de as any).name}" (cross-element refs)`);
    placedSrcElIds[srcElId] = true;
  }
  // Drop calcs that referenced cross-element cols but had no derived element
  for (const elId of Object.keys(crossElCalcsByElId)) {
    if (placedSrcElIds[elId]) continue;
    for (const c of crossElCalcsByElId[elId]) {
      warnings.push(`⚠ "${c.name}" cross-element refs but no derived element — column dropped`);
    }
  }

  // ── Build output ────────────────────────────────────────────────────────
  if (!connectionId) warnings.unshift('⚠ Connection ID not set — update in JSON before saving to Sigma');

  const sigmaModel: any = {
    name: ds.name,
    schemaVersion: 1,
    pages: [{ id: sigmaShortId(), name: 'Page 1', elements: [...controls, ...elements] }]
  };

  const totalCols = elements.reduce((s, e) => s + (e.columns?.length || 0), 0);
  const totalMetrics = elements.reduce((s, e) => s + ((e as any).metrics?.length || 0), 0);
  const totalRels = elements.reduce((s, e) => s + ((e as any).relationships?.length || 0), 0);

  return {
    model: sigmaModel,
    warnings,
    ...(security.length ? { security } : {}),
    stats: {
      datasources: datasources.length,
      elements: elements.length,
      columns: totalCols,
      metrics: totalMetrics,
      relationships: totalRels,
      controls: controls.length,
      parameters: parameters.length,
      lodChildElements: elements.filter(e => e.source?.kind === 'table' && e.source?.elementId).length,
    }
  };
}
