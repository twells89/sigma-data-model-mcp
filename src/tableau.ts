/**
 * Tableau Workbook/Data Source XML → Sigma Data Model JSON converter.
 *
 * Handles .twb (workbook) and .tds (data source) XML content.
 * Parses data sources, joins, calculated fields, parameters, LOD expressions,
 * and relationships. Produces Sigma data model JSON.
 */

import { XMLParser } from 'fast-xml-parser';
import {
  resetIds, sigmaShortId, sigmaInodeId, sigmaDisplayName, inferSigmaFormat, buildDerivedElements,
  type SigmaElement, type ConversionResult,
} from './sigma-ids.js';
import { tableauFormulaToSigma, tableauIsAggregate } from './formulas.js';

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

// Extract per-worksheet rows/cols dim split from a parsed workbook XML so we can
// derive PARTITION BY / ORDER BY heuristically. Returns: byField (calc-key →
// list of contexts) AND each context records rowsDims / colsDims separately.
interface WindowViewContext { rowsDims: string[]; colsDims: string[]; allDims: string[]; dateDim?: string; }
interface WindowWorksheetIndex { byField: Map<string, WindowViewContext[]>; }

function _buildWindowWorksheetIndex(parsed: any): WindowWorksheetIndex {
  const byField = new Map<string, WindowViewContext[]>();
  const worksheets = asArray(parsed?.workbook?.worksheets?.worksheet || []);
  for (const ws of worksheets) {
    const tbl = ws.table || ws;
    const rowRefs: string[] = [];
    const colRefs: string[] = [];
    let dateDim: string | undefined;

    for (const r of asArray(tbl?.rows || [])) {
      const text = typeof r === 'string' ? r : (r['#text'] || '');
      for (const ref of _extractFieldRefsFromShelf(text)) rowRefs.push(ref.toUpperCase());
    }
    for (const c of asArray(tbl?.cols || [])) {
      const text = typeof c === 'string' ? c : (c['#text'] || '');
      // Tableau encodes date truncations in the bracket prefix (yr:, mn:, qr:, dy:, wk:).
      // Detect them and tag the underlying field as the date order dim.
      const re = /\[[^\]]+\]\.\[([^\]]+)\]/g;
      let mm: RegExpExecArray | null;
      while ((mm = re.exec(text)) !== null) {
        const inner = mm[1];
        const colon = inner.match(/^(yr|mn|qr|dy|wk|md):([^:]+):[a-z]{2}$/i);
        if (colon) {
          dateDim = colon[2].toUpperCase();
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
      list.push({ rowsDims: rowsDims.slice(), colsDims: colsDims.slice(), allDims: allDims.slice(), dateDim });
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

  for (const ds of allDs) {
    if (attr(ds, 'hasconnection') === 'false' || attr(ds, 'name') === 'Parameters') {
      // Parse parameters
      for (const col of asArray(ds.column)) {
        const colName = attr(col, 'caption') || attr(col, 'name') || '';
        const colType = attr(col, 'datatype') || 'string';
        const domainType = attr(col, 'param-domain-type') || 'all';
        const members = asArray(col.member).map((m: any) => attr(m, 'value')).filter(Boolean);
        const calcEl = col.calculation;
        parameters.push({
          name: colName.replace(/^\[|\]$/g, ''),
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
  const elements: SigmaElement[] = [];
  const connId = connectionId || '<CONNECTION_ID>';

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

          const srcKey = parseOpRef(attr(innerExprs[0], 'op') || '');
          const tgtKey = parseOpRef(attr(innerExprs[1], 'op') || '');
          if (!srcKey || !tgtKey) continue;

          const ensureCol = (entry: EntryType, key: string): string => {
            let id = entry.colIdMap[key] || entry.colIdMap[key.replace(/-/g, '_')];
            if (!id) {
              id = sigmaInodeId(key.replace(/\s+/g, '_'));
              const isUuid    = /^[0-9A-F]{8}-[0-9A-F]{4}-/i.test(key);
              const dispName  = isUuid ? key : sigmaDisplayName(key);
              entry.element.columns.push({ id, formula: `[${entry.cleanName}/${dispName}]` });
              entry.element.order.push(id);
              entry.colIdMap[key] = id;
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

    function _resolveDimDisplayName(dimNameRaw: string): { dimUpper: string; displayName: string; baseColId?: string } | null {
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
      return { dimUpper: physicalUpper, displayName: dispName, baseColId: found.colId };
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
      const caption = attr(col, 'caption') || rawName.replace(/^\[|\]$/g, '');
      const hidden = attr(col, 'hidden') === 'true';
      const calcEl = col.calculation;
      const formula = calcEl ? attr(calcEl, 'formula') : '';
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
          for (const dimName of lod.dims) {
            const r = _resolveDimDisplayName(dimName);
            if (r) lodDimsResolved.push(r);
            else { allFound = false; warnings.push(`⚠ LOD "${caption}" dim [${dimName}] not found`); }
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

          let partitionDimNames: string[] = chosen ? chosen.rowsDims.slice() : [];
          let orderDimRaw: string | null = chosen?.dateDim || null;
          let orderDimDateTrunc: string | null = null;
          if (chosen?.dateDim) {
            // Heuristic: monthly grain is the most common Tableau view default
            // when ORDER_DATE is on cols with mn: prefix. Use 'month'.
            orderDimDateTrunc = 'month';
          } else if (chosen && chosen.colsDims.length > 0) {
            orderDimRaw = chosen.colsDims[0];
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

        if (tableauIsAggregate(formula)) {
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
  }

  // ── Auto-fix cross-element column references with - link/ syntax ────────
  const gColMap: Record<string, { elId: string; displayName: string }> = {};
  for (const el of elements) {
    for (const c of (el.columns || [])) {
      const fm = c.formula.match(/\[([^\/\]]+)\/([^\]]+)\]$/);
      if (fm) gColMap[fm[2].toUpperCase()] = { elId: el.id, displayName: fm[2] };
    }
  }
  for (const el of elements) {
    const localNames = new Set<string>();
    for (const c of (el.columns || [])) {
      if (c.name) localNames.add(c.name.toUpperCase());
      const fm = c.formula.match(/\/([^\]]+)\]$/);
      if (fm) localNames.add(fm[1].toUpperCase());
    }
    const relFkLookup: Record<string, string> = {};
    const elTbl = el.source?.path?.[el.source.path.length - 1] || 'UNKNOWN';
    for (const rel of ((el as any).relationships || [])) {
      const fkCol = (el.columns || []).find(c => c.id === rel.keys[0]?.sourceColumnId);
      if (fkCol) {
        const fkM = fkCol.formula.match(/\/([^\]]+)\]$/);
        if (fkM) relFkLookup[rel.targetElementId] = fkM[1].replace(/\s+/g, '_').toUpperCase();
      }
    }
    for (const c of (el.columns || [])) {
      if (!c.name || !c.formula) continue;
      if (c.formula.match(/^\[[\w_]+\//)) continue;
      if (c.formula.includes('- link/')) continue;
      const refs = c.formula.match(/\[([^\]\/]+)\]/g) || [];
      let fixedFormula = c.formula;
      let wasFixed = false;
      for (const ref of refs) {
        const rn = ref.replace(/^\[|\]$/g, '');
        if (localNames.has(rn.toUpperCase()) || rn.toUpperCase() === 'TRUE' || rn.toUpperCase() === 'FALSE') continue;
        const ge = gColMap[rn.toUpperCase()];
        if (ge && relFkLookup[ge.elId]) {
          fixedFormula = fixedFormula.replace(ref, `[${elTbl}/${relFkLookup[ge.elId]} - link/${ge.displayName}]`);
          wasFixed = true;
        }
      }
      if (wasFixed) {
        c.formula = fixedFormula;
        warnings.push(`✅ "${c.name}" → linked column: ${fixedFormula.slice(0, 100)}`);
        warnings.push(`   ⚠ Note: Sigma API may not round-trip linked columns correctly yet.`);
      }
    }
  }

  // ── Parameters → Controls ───────────────────────────────────────────────
  const controls: any[] = [];
  for (const p of parameters) {
    const controlId = sigmaDisplayName(p.name).replace(/\s+/g, '-');
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
  for (const de of buildDerivedElements(elements)) elements.push(de);

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
