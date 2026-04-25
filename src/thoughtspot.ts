/**
 * ThoughtSpot TML (Thought Model Language) → Sigma Data Model converter.
 * Accepts a ThoughtSpot worksheet or model YAML string.
 */

import yaml from 'js-yaml';
import {
  resetIds, sigmaShortId, sigmaInodeId, sigmaDisplayName,
  sigmaColFormula, inferSigmaFormat, buildDerivedElements,
  type SigmaElement, type SigmaColumn, type SigmaMetric, type ConversionResult,
} from './sigma-ids.js';

export interface ThoughtSpotConvertOptions {
  connectionId?: string;
  database?: string;
  schema?: string;
}

export function convertThoughtSpotToSigma(
  yamlText: string,
  options: ThoughtSpotConvertOptions = {},
): ConversionResult {
  resetIds();
  const { connectionId, database: dbOverride, schema: schOverride } = options;
  const warnings: string[] = [];

  let tml: any;
  try {
    tml = yaml.load(yamlText);
  } catch (e: any) {
    throw new Error('YAML parse error: ' + e.message);
  }
  if (!tml || typeof tml !== 'object') throw new Error('Empty or invalid TML');

  const ws: any = tml.worksheet || tml.model || tml;
  const modelName: string = ws.name || 'ThoughtSpot Model';

  // Build table metadata map
  const tablesMeta: Record<string, { db: string; schema: string }> = {};
  for (const t of (ws.tables || [])) {
    tablesMeta[t.name] = { db: t.db || dbOverride || '', schema: t.schema || schOverride || '' };
  }

  // table_paths: alias → actual table name
  const tablePathMap: Record<string, string> = {};
  for (const tp of (ws.table_paths || [])) {
    tablePathMap[tp.id] = tp.table;
  }
  if (Object.keys(tablePathMap).length === 0) {
    for (const n of Object.keys(tablesMeta)) tablePathMap[n] = n;
  }

  // Build formula map
  const formulaMap: Record<string, string> = {};
  for (const f of (ws.formulas || [])) {
    formulaMap[f.id || f.name] = f.expr || f.expression || '';
  }

  // Group physical columns by resolved table name
  const colsByTable: Record<string, Array<{ col: any; physCol: string; tableName: string }>> = {};
  const formulaCols: Array<{ col: any; formulaExpr: string }> = [];

  for (const col of (ws.worksheet_columns || ws.columns || [])) {
    const colId: string = col.column_id || col.id || '';
    const sepIdx = colId.indexOf('::');
    if (sepIdx !== -1) {
      const alias = colId.slice(0, sepIdx);
      const physCol = colId.slice(sepIdx + 2);
      const tableName = tablePathMap[alias] || alias;
      if (!colsByTable[tableName]) colsByTable[tableName] = [];
      colsByTable[tableName].push({ col, physCol, tableName });
    } else if (colId && formulaMap[colId]) {
      formulaCols.push({ col, formulaExpr: formulaMap[colId] });
    } else if (col.formula_id && formulaMap[col.formula_id]) {
      formulaCols.push({ col, formulaExpr: formulaMap[col.formula_id] });
    } else {
      warnings.push(`Column "${col.name || colId}" has no resolvable source — skipped`);
    }
  }

  const allTableNames = Array.from(new Set([
    ...Object.keys(colsByTable),
    ...Object.keys(tablesMeta),
  ]));

  if (allTableNames.length === 0) {
    warnings.push('No tables found in TML — check table_paths and tables sections');
  }

  // Build Sigma elements
  const pageId = sigmaShortId();
  const elements: SigmaElement[] = [];
  const elementByTable: Record<string, SigmaElement & { _colPhysIdMap: Record<string, string> }> = {};

  for (const tableName of allTableNames) {
    const meta = tablesMeta[tableName] || { db: '', schema: '' };
    const db  = dbOverride || meta.db || '';
    const sch = schOverride || meta.schema || '';
    const elemId = sigmaShortId();
    const columns: SigmaColumn[] = [];
    const metrics: SigmaMetric[] = [];
    const colOrder: string[] = [];
    const colPhysIdMap: Record<string, string> = {};

    for (const { col, physCol } of (colsByTable[tableName] || [])) {
      const dispName: string = col.name || sigmaDisplayName(physCol);
      const isMeasure = (col.type || '').toUpperCase() === 'MEASURE';
      const isDate    = (col.type || '').toUpperCase() === 'DATE';

      let colId: string;
      let colObj: SigmaColumn;
      if (isDate) {
        colId = sigmaShortId();
        colObj = {
          id: colId,
          formula: `DateTrunc("day", ${sigmaColFormula(tableName, physCol)})`,
          name: dispName,
        };
      } else {
        colId = sigmaInodeId(physCol);
        colObj = { id: colId, formula: sigmaColFormula(tableName, physCol) };
      }
      colPhysIdMap[physCol.toUpperCase()] = colId;
      columns.push(colObj);
      colOrder.push(colId);

      if (isMeasure) {
        const agg = (col.aggregation || 'SUM').toUpperCase();
        const aggMap: Record<string, string> = {
          SUM: 'Sum', COUNT: 'Count', COUNT_DISTINCT: 'CountDistinct',
          AVERAGE: 'Avg', AVG: 'Avg', MAX: 'Max', MIN: 'Min',
          STD_DEVIATION: 'StdDev', VARIANCE: 'Variance',
        };
        const sigmaAgg = aggMap[agg] || 'Sum';
        const colDisplayName = sigmaDisplayName(physCol);
        const formula = `${sigmaAgg}([${colDisplayName}])`;
        let fmt: any = inferSigmaFormat(formula, dispName);
        if (fmt?.formatString === ',.2%') fmt = { kind: 'number', formatString: ',.2f', suffix: '%' };
        const metric: any = { id: sigmaShortId(), name: dispName, formula };
        if (fmt) metric.format = fmt;
        metrics.push(metric);
      }
    }

    const element: any = {
      id: elemId, kind: 'table',
      name: sigmaDisplayName(tableName),
      source: {
        connectionId,
        kind: 'warehouse-table',
        path: [db, sch, tableName].filter(Boolean),
      },
      columns, metrics, order: colOrder, relationships: [],
      _colPhysIdMap: colPhysIdMap,
    };
    elements.push(element);
    elementByTable[tableName] = element;
  }

  // Attach formula-derived columns to first element
  if (formulaCols.length > 0 && elements.length > 0) {
    const hostEl = elements[0];
    for (const { col, formulaExpr } of formulaCols) {
      const dispName: string = col.name || 'Calculated';
      const sigmaFormula = tsFormulaToSigma(formulaExpr, elementByTable);
      const colId = sigmaShortId();
      let fmt: any = inferSigmaFormat(sigmaFormula, dispName);
      if (fmt?.formatString === ',.2%') fmt = { kind: 'number', formatString: ',.2f', suffix: '%' };
      const colObj: any = { id: colId, name: dispName, formula: sigmaFormula };
      if (fmt) colObj.format = fmt;
      hostEl.columns.push(colObj);
    }
  }

  // Build relationships from joins
  const joinOnRe = /\[([^\]:]+)::([^\]]+)\]\s*=\s*\[([^\]:]+)::([^\]]+)\]/;
  for (const join of (ws.joins || [])) {
    const onStr: string = join.on || '';
    const m = joinOnRe.exec(onStr);
    if (!m) {
      warnings.push(`Join "${join.name || '?'}": could not parse ON clause — "${onStr}"`);
      continue;
    }
    const [, lAlias, lCol, rAlias, rCol] = m;
    const lTable = tablePathMap[lAlias] || lAlias;
    const rTable = tablePathMap[rAlias] || rAlias;
    const lEl = elementByTable[lTable];
    const rEl = elementByTable[rTable];
    if (!lEl || !rEl) {
      warnings.push(`Join "${join.name}": element not found for "${lTable}" or "${rTable}"`);
      continue;
    }
    const srcColId = (lEl._colPhysIdMap || {})[lCol.toUpperCase()] || null;
    const tgtColId = (rEl._colPhysIdMap || {})[rCol.toUpperCase()] || null;
    if (!srcColId || !tgtColId) {
      warnings.push(`Join "${join.name}": join key columns not found — "${lCol}" / "${rCol}"`);
      continue;
    }
    lEl.relationships!.push({
      id: sigmaShortId(),
      targetElementId: rEl.id,
      keys: [{ sourceColumnId: srcColId, targetColumnId: tgtColId }],
      name: join.name || rTable,
      relationshipType: 'N:1',
    });
  }

  if ((ws.joins || []).length === 0 && allTableNames.length > 1) {
    warnings.push('No joins defined in TML — relationships will need to be configured manually in Sigma');
  }

  // Strip transient helper maps
  for (const el of elements) { delete (el as any)._colPhysIdMap; }

  // Add derived join-view elements
  for (const de of buildDerivedElements(elements)) elements.push(de);

  const stats = {
    elements: elements.length,
    columns: elements.reduce((n, e) => n + (e.columns?.length || 0), 0),
    metrics: elements.reduce((n, e) => n + (e.metrics?.length || 0), 0),
    relationships: elements.reduce((n, e) => n + (e.relationships?.length || 0), 0),
  };

  return {
    model: { name: modelName, pages: [{ id: pageId, name: 'Page 1', elements }] },
    warnings,
    stats,
  };
}

// ── ThoughtSpot formula → Sigma formula ────────────────────────────────────

function tsFormulaToSigma(expr: string, _elementByTable: Record<string, any>): string {
  if (!expr) return '';
  let s = expr;
  s = tsConvertIfThenElse(s);
  s = s.replace(/(\w+)\s+in\s*\{([^}]+)\}/gi, (_, col, vals) => {
    const vlist = vals.split(',').map((v: string) => v.trim()).join(', ');
    return `In([${sigmaDisplayName(col.trim())}], ${vlist})`;
  });
  const tsAggMap: Record<string, string> = {
    sum: 'Sum', count: 'Count', count_distinct: 'CountDistinct',
    average: 'Avg', avg: 'Avg', max: 'Max', min: 'Min',
    std_deviation: 'StdDev', variance: 'Variance',
    count_not_null: 'CountDistinct', cumulative_sum: 'CumulativeSum',
  };
  s = s.replace(/\b(sum|count_distinct|count_not_null|count|average|avg|max|min|std_deviation|variance|cumulative_sum)\s*\(([^)]+)\)/gi,
    (_, fn, arg) => {
      const sigmaFn = tsAggMap[fn.toLowerCase()] || fn;
      return `${sigmaFn}(${tsWrapColumnRefs(arg.trim())})`;
    });
  s = s.replace(/\bsafe_divide\s*\(([^,)]+),([^)]+)\)/gi,
    (_, a, b) => `If(IsNull(${b.trim()}) or ${b.trim()} = 0, Null(), ${a.trim()} / ${b.trim()})`);
  s = s.replace(/\bisnull\s*\(/gi, 'IsNull(');
  s = s.replace(/\bnot\s*\(/gi, 'Not(');
  s = s.replace(/\btoday\s*\(\s*\)/gi, 'Today()');
  s = s.replace(/\bdate_diff\s*\(/gi, 'DateDiff(');
  s = s.replace(/\bdatediff\s*\(/gi, 'DateDiff(');
  s = tsWrapColumnRefs(s);
  return s;
}

function tsConvertIfThenElse(s: string): string {
  let maxPasses = 10;
  const re = /\bif\s*\(([^)]+)\)\s*then\s+(.+?)\s+else\s+/g;
  while (re.test(s) && maxPasses-- > 0) {
    re.lastIndex = 0;
    s = s.replace(/\bif\s*\(([^)]+)\)\s*then\s+(.+?)\s+else\s+(.+?)(?=\s*(?:$|\bif\b))/g,
      (_, cond, thenV, elseV) => `If(${cond}, ${thenV}, ${elseV})`);
  }
  return s;
}

function tsWrapColumnRefs(expr: string): string {
  const saved: string[] = [];
  let s = expr
    .replace(/\[[^\]]*\]/g, m => { saved.push(m); return `\x02${saved.length - 1}\x03`; })
    .replace(/"[^"]*"/g,    m => { saved.push(m); return `\x02${saved.length - 1}\x03`; });
  const skip = /^(if|then|else|and|or|not|in|null|true|false|today|IsNull|If|In|List|Sum|Count|Avg|Max|Min|CountDistinct|StdDev|Variance|DateDiff|Today|CumulativeSum|Not)$/;
  s = s.replace(/\b([A-Z_][A-Z0-9_]*)\b(?!\s*\()/gi, (match, ident) => {
    if (skip.test(ident)) return match;
    return `[${sigmaDisplayName(ident)}]`;
  });
  return s.replace(/\x02(\d+)\x03/g, (_, i) => saved[+i]);
}
