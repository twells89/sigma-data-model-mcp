/**
 * Oracle Analytics Cloud (OAC) Logical Tables JSON → Sigma Data Model converter.
 *
 * Input format: an array of logical table objects as exported from OAC SMML.
 * Each table has the structure from OAC's logical table JSON:
 *   { name, logicalColumns: [...], logicalTableSources: [...], joins: [...] }
 *
 * physicalMap: optional map of physical table metadata:
 *   { [tableNameUpper]: { database, schema } }
 */

import {
  resetIds, sigmaShortId, sigmaDisplayName,
  inferSigmaFormat, buildDerivedElements,
  type SigmaElement, type SigmaColumn, type SigmaMetric, type ConversionResult,
} from './sigma-ids.js';
import { sqlCaseToIf } from './alteryx.js';

export interface OacConvertOptions {
  connectionId?: string;
  database?: string;
  schema?: string;
  modelName?: string;
  physicalMap?: Record<string, { database?: string; schema?: string }>;
}

export function convertOacToSigma(
  tables: any[],
  options: OacConvertOptions = {},
): ConversionResult {
  resetIds();
  const { connectionId = '<CONNECTION_ID>', database: dbOverride = '', schema: schOverride = '',
          modelName = 'OAC Model', physicalMap = {} } = options;
  const warnings: string[] = [];

  const elements: SigmaElement[] = [];
  const tableElementMap = new Map<string, { elementId: string; colMap: Map<string, { colId: string; displayName: string }>; element: SigmaElement }>();

  // Pass 1: Build Sigma elements
  for (const table of tables) {
    const tableName: string = table.name || 'Unknown';
    const tableDisplay = oacDisplayName(tableName);
    const elementId = sigmaShortId();
    const columns: SigmaColumn[] = [];
    const metrics: SigmaMetric[] = [];
    const order: string[] = [];
    const colMap = new Map<string, { colId: string; displayName: string }>();
    const physColIds = new Map<string, string>();

    // Resolve physical table path
    const srcTables: string[] = table.logicalTableSources?.[0]?.tableMapping?.tables || [];
    const physRawName: string = srcTables[0] || tableName;
    const physKey = physRawName.split('.').pop()!.trim().toUpperCase();
    const physInfo = physicalMap[physKey] || {};

    const database = dbOverride || physInfo.database || '';
    const schema   = schOverride || physInfo.schema || '';
    const pathParts: string[] = [];
    if (database) pathParts.push(database);
    if (schema)   pathParts.push(schema);
    pathParts.push(physKey);

    for (const col of (table.logicalColumns || [])) {
      const colName: string = col.name || 'Column';
      const colDisp = oacDisplayName(colName);
      const rule: string | undefined = col.aggregation?.rule;
      const isMeasure = rule && rule !== 'NONE';

      const physExpr: string = col.logicalColumnSource?.physicalMappings?.[0]?.physicalExpression?.text || '';
      const physColRaw = oacExtractPhysColName(physExpr);
      const physColDisp = physColRaw ? oacDisplayName(physColRaw) : oacDisplayName(colName);

      const logExpr: string = col.logicalColumnSource?.logicalExpression?.text || '';
      const isDerived = !physExpr && !!logExpr;

      if (isDerived) {
        const { formula: converted, warnings: exprW } = oacExprToSigma(logExpr);
        exprW.forEach((w: string) => warnings.push(`"${colName}": ${w}`));
        const formula = converted || `[${colDisp}]`;
        if (isMeasure) {
          const fn = aggFormula(rule!, formula);
          if (!fn) { warnings.push(`"${colName}": rule "${rule}" unsupported — skipped.`); continue; }
          const metricId = sigmaShortId();
          const mObj: any = { id: metricId, name: colDisp, formula: fn };
          let mFmt = inferSigmaFormat(fn, colDisp);
          if (mFmt?.formatString === ',.2%') mFmt = { kind: 'number', formatString: ',.2f', suffix: '%' };
          if (mFmt) mObj.format = mFmt;
          metrics.push(mObj);
          colMap.set(colName.toUpperCase(), { colId: metricId, displayName: colDisp });
        } else {
          const colId = sigmaShortId();
          columns.push({ id: colId, formula });
          order.push(colId);
          colMap.set(colName.toUpperCase(), { colId, displayName: colDisp });
        }
      } else if (isMeasure) {
        const rawFormula = `[${physKey}/${physColDisp}]`;
        if (!physColIds.has(rawFormula)) {
          const rawId = sigmaShortId();
          columns.push({ id: rawId, formula: rawFormula });
          order.push(rawId);
          physColIds.set(rawFormula, rawId);
        }
        const fn = aggFormula(rule!, `[${physColDisp}]`);
        if (!fn) { warnings.push(`"${colName}": rule "${rule}" unsupported — skipped.`); continue; }
        const metricId = sigmaShortId();
        const mObj: any = { id: metricId, name: colDisp, formula: fn };
        let mFmt = inferSigmaFormat(fn, colDisp);
        if (mFmt?.formatString === ',.2%') mFmt = { kind: 'number', formatString: ',.2f', suffix: '%' };
        if (mFmt) mObj.format = mFmt;
        metrics.push(mObj);
        colMap.set(colName.toUpperCase(), { colId: metricId, displayName: colDisp });
      } else {
        const rawFormula = `[${physKey}/${physColDisp}]`;
        if (physColIds.has(rawFormula)) {
          colMap.set(colName.toUpperCase(), { colId: physColIds.get(rawFormula)!, displayName: physColDisp });
          continue;
        }
        const colId = sigmaShortId();
        columns.push({ id: colId, formula: rawFormula });
        order.push(colId);
        physColIds.set(rawFormula, colId);
        colMap.set(colName.toUpperCase(), { colId, displayName: physColDisp });
      }
    }

    const element: SigmaElement = {
      id: elementId, kind: 'table', name: tableDisplay,
      source: { connectionId, kind: 'warehouse-table', path: pathParts },
      columns, order,
    };
    if (metrics.length) (element as any).metrics = metrics;
    elements.push(element);
    tableElementMap.set(tableName.toUpperCase(), { elementId, colMap, element });
  }

  // Pass 2: Relationships from logical joins
  for (const table of tables) {
    const srcInfo = tableElementMap.get((table.name || '').toUpperCase());
    if (!srcInfo) continue;
    for (const join of (table.joins || [])) {
      const tgtName: string = join.rightTable;
      if (!tgtName) continue;
      const tgtInfo = tableElementMap.get(tgtName.toUpperCase());
      if (!tgtInfo) {
        warnings.push(`Join target "${tgtName}" not in tables array — relationship skipped.`);
        continue;
      }
      let srcEntry: { colId: string; displayName: string } | undefined;
      let tgtEntry: { colId: string; displayName: string } | undefined;
      for (const [key, info] of srcInfo.colMap) {
        if (tgtInfo.colMap.has(key)) {
          srcEntry = info;
          tgtEntry = tgtInfo.colMap.get(key);
          break;
        }
      }
      const oacRelName = tgtName.toUpperCase();
      if (!srcInfo.element.relationships) srcInfo.element.relationships = [];
      if (srcEntry && tgtEntry) {
        srcInfo.element.relationships.push({
          id: sigmaShortId(),
          targetElementId: tgtInfo.elementId,
          keys: [{ sourceColumnId: srcEntry.colId, targetColumnId: tgtEntry.colId }],
          name: oacRelName,
        });
      } else {
        warnings.push(`"${table.name}" → "${tgtName}": no shared key column found — add join keys manually in Sigma.`);
        srcInfo.element.relationships.push({
          id: sigmaShortId(),
          targetElementId: tgtInfo.elementId,
          keys: [],
          name: oacRelName,
        });
      }
    }
  }

  for (const de of buildDerivedElements(elements)) elements.push(de);

  const stats = {
    elements: elements.length,
    columns: elements.reduce((n, e) => n + (e.columns?.length || 0), 0),
    metrics: elements.reduce((n, e) => n + ((e as any).metrics?.length || 0), 0),
    relationships: elements.reduce((n, e) => n + (e.relationships?.length || 0), 0),
  };

  return {
    model: { name: modelName, pages: [{ id: sigmaShortId(), name: 'Page 1', elements }] },
    warnings,
    stats,
  };
}

// ── OAC helpers ──────────────────────────────────────────────────────────────

function oacDisplayName(s: string): string {
  if (!s) return '';
  if (s.includes(' ')) return s.replace(/\b\w/g, c => c.toUpperCase());
  return sigmaDisplayName(s);
}

function oacExtractPhysColName(expr: string): string {
  if (!expr) return '';
  const str = expr.trim();
  const quoted = str.match(/"([^"]+)"\s*$/);
  if (quoted) return quoted[1];
  return str.includes('.') ? str.split('.').pop()!.trim() : str;
}

function aggFormula(rule: string, colExpr: string): string | null {
  const map: Record<string, string> = {
    SUM:            `Sum(${colExpr})`,
    AVG:            `Avg(${colExpr})`,
    COUNT:          `Count(${colExpr})`,
    COUNT_DISTINCT: `CountDistinct(${colExpr})`,
    MIN:            `Min(${colExpr})`,
    MAX:            `Max(${colExpr})`,
    MEDIAN:         `Median(${colExpr})`,
    STD_DEV:        `StdDev(${colExpr})`,
    STD_DEV_POP:    `StdDevPop(${colExpr})`,
  };
  return map[rule] ?? null;
}

function oacExprToSigma(expr: string): { formula: string; warnings: string[] } {
  if (!expr) return { formula: '', warnings: [] };
  let f = expr.trim();
  const warnings: string[] = [];

  const unsupportedRe = /\b(AGO|TODATE|PERIODROLLING|FILTER|EVALUATE|EVALUATE_AGGR|MSUM|MCOUNT|MAVG|MMAX|MMIN|NTILE|TOPN|BOTTOMN|PERCENTRANK|NVL2|OBIEE_BIN)\s*\(/i;
  const unsupMatch = f.match(unsupportedRe);
  if (unsupMatch) warnings.push(`uses "${unsupMatch[1].toUpperCase()}()" — no direct Sigma equivalent; review manually`);

  const tsiMap: Record<string, string> = {
    SQL_TSI_SECOND: '"second"', SQL_TSI_MINUTE: '"minute"', SQL_TSI_HOUR: '"hour"',
    SQL_TSI_DAY: '"day"', SQL_TSI_WEEK: '"week"', SQL_TSI_MONTH: '"month"',
    SQL_TSI_QUARTER: '"quarter"', SQL_TSI_YEAR: '"year"',
  };
  f = f.replace(/\bSQL_TSI_\w+\b/gi, m => tsiMap[m.toUpperCase()] || `"${m.toLowerCase()}"`);

  f = f.replace(/\bNVL\s*\(/gi, 'Coalesce(');
  f = f.replace(/\bSUBSTR(?:ING)?\s*\(/gi, 'Mid(');
  f = f.replace(/\bINSTR\s*\(/gi, 'Search(');
  f = f.replace(/\bLENGTH\s*\(/gi, 'Len(');
  f = f.replace(/\bTO_CHAR\s*\(/gi, 'Text(');
  f = f.replace(/\bTO_DATE\s*\(/gi, 'Date(');
  f = f.replace(/\bTO_NUMBER\s*\(/gi, 'Number(');
  f = f.replace(/\bCEIL\s*\(/gi, 'Ceiling(');
  f = f.replace(/\bTIMESTAMPADD\s*\(/gi, 'DateAdd(');
  f = f.replace(/\bTIMESTAMPDIFF\s*\(/gi, 'DateDiff(');
  f = f.replace(/\bCURRENT_DATE\b/gi, 'Today()');
  f = f.replace(/\bCURRENT_TIMESTAMP\b/gi, 'Now()');
  f = f.replace(/\bCURRENT_TIME\b/gi, 'Now()');
  f = f.replace(/"[^"]+"\."([^"]+)"/g, (_, col) => `[${oacDisplayName(col)}]`);
  f = f.replace(/\b[A-Za-z_][A-Za-z0-9_ ]*\.[A-Za-z_][A-Za-z0-9_]+\b/g,
    m => `[${oacDisplayName(m.split('.').pop()!)}]`);
  f = f.replace(/'([^']*)'/g, '"$1"');
  f = f.replace(/(\w+(?:\([^)]*\))?|\[[^\]]+\])\s+IN\s+\(([^)]+)\)/gi,
    (_, lhs, items) => `In(${lhs}, ${items.split(',').map((v: string) => v.trim()).join(', ')})`);

  if (/\bCASE\b/i.test(f)) f = sqlCaseToIf(f);

  return { formula: f, warnings };
}
