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
          // Carry `name` so the cross-element calc post-pass can identify and
          // re-place this on the derived "<Table> View" element if its formula
          // references a related-table column.
          columns.push({ id: colId, name: colDisp, formula });
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

    // Don't set element `name` here. For a warehouse-table element, Sigma
    // resolves the element's identifier from the path-tail (physKey) — that's
    // also what raw column formulas `[physKey/Col]` reference. Setting `name`
    // to a different string (e.g. the OAC logical table name "Order Fact")
    // makes Sigma fail to resolve the element's own raw formulas. Cross-element
    // refs and derived elements correctly fall back to path-tail via
    // sigma-ids.buildDerivedElements (`srcEl.name || srcTableName`).
    void tableDisplay;
    const element: SigmaElement = {
      id: elementId, kind: 'table',
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
      // Rel name = uppercased target warehouse-table name (last segment of source path).
      // Using raw `tgtName` would let spaces/lowercase leak in (e.g., "customer dim").
      // Per spec rule, this name is the middle segment of cross-element refs and must
      // match what Sigma resolves the relationship as.
      const tgtPath = tgtInfo.element.source?.path;
      const oacRelName = (tgtPath ? tgtPath[tgtPath.length - 1] : tgtName).toUpperCase();
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

  // ── Pull cross-element calc cols off source elements (moved to derived) ─
  // Calc cols whose formula references a related-table column by display name
  // cannot resolve on the source warehouse-table element — Sigma doesn't see
  // those names in scope. Pull them off here, then place on the derived
  // "<Table> View" element after buildDerivedElements runs, rewriting bare
  // [X] refs to [BaseName/REL_NAME/X] triple form.
  // Pattern adapted from tableau.ts:1993-2129 (commit cca1341).
  const crossElCalcsByElId: Record<string, any[]> = {};
  for (const el of elements) {
    if (el.source?.kind !== 'warehouse-table') continue;
    if (!(el as any).relationships?.length) continue;

    const localNames = new Set<string>();
    for (const c of (el.columns || [])) {
      if (!c.formula) continue;
      const m = c.formula.match(/^\[[^\]\/]+\/([^\]]+)\]$/);
      if (m) localNames.add(m[1].toUpperCase());
      if ((c as any).name) localNames.add((c as any).name.toUpperCase());
    }

    const crossEl: any[] = [];
    const keep: any[] = [];
    for (const c of (el.columns || [])) {
      const cn: string | undefined = (c as any).name;
      if (!cn || !c.formula) { keep.push(c); continue; }
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

  const derivedEls = buildDerivedElements(elements);
  for (const de of derivedEls) elements.push(de);

  // Place the pulled calc cols on their matching derived element, rewriting
  // bare [X] refs to [BaseName/REL_NAME/X] triple form.
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
  for (const elId of Object.keys(crossElCalcsByElId)) {
    if (placedSrcElIds[elId]) continue;
    for (const c of crossElCalcsByElId[elId]) {
      warnings.push(`⚠ "${(c as any).name}" cross-element refs but no derived element — column dropped`);
    }
  }

  const stats = {
    elements: elements.length,
    columns: elements.reduce((n, e) => n + (e.columns?.length || 0), 0),
    metrics: elements.reduce((n, e) => n + ((e as any).metrics?.length || 0), 0),
    relationships: elements.reduce((n, e) => n + (e.relationships?.length || 0), 0),
  };

  return {
    model: { name: modelName, schemaVersion: 1, pages: [{ id: sigmaShortId(), name: 'Page 1', elements }] },
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

  // An OAC logical-column expression is user-written SQL-like text and CAN
  // contain single-quoted string literals (OAC's dialect follows Oracle
  // convention: `"Table"."Col"` for quoted identifiers, `'text'` for string
  // literals — e.g. `CASE WHEN "T"."STATUS" = 'When Due' THEN ... END`).
  // Every pass below — the unsupported-function check, SQL_TSI_ mapping,
  // the NVL/SUBSTR/.../CURRENT_DATE function-name rewrites, the dotted
  // "table"."col"/bare table.col field-ref substitution, and the IN-list
  // splitter — is a regex scan that cannot tell code from data. Left
  // unmasked, a literal containing one of these tokens is silently
  // rewritten — confirmed live via convertOacToSigma:
  //   'This uses SQL_TSI_MONTH label' → "This uses "month" label"
  //     (a SYNTACTICALLY INVALID Sigma string — unescaped nested quotes)
  //   'Contact ACME.Corp for details' → "[Corp] for details"
  //     (the literal text is destroyed, replaced with a bracket ref to a
  //     column that may not even exist)
  //
  // Mask every literal span ONCE, here, before any pass runs; every pass
  // below operates on the masked text; unmask at the very end, which is
  // also where a single-quoted literal becomes Sigma's double-quoted form
  // (this replaces the old separate, unmasked quote-conversion pass). Only
  // single-quoted spans are masked — OAC's `"double-quoted"` identifiers
  // are structural syntax the field-ref pass below still needs to see.
  const { masked, lits } = maskOacLiterals(f);
  f = masked;

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
  f = f.replace(/(\w+(?:\([^)]*\))?|\[[^\]]+\])\s+IN\s+\(([^)]+)\)/gi,
    (_, lhs, items) => `In(${lhs}, ${items.split(',').map((v: string) => v.trim()).join(', ')})`);

  if (/\bCASE\b/i.test(f)) f = sqlCaseToIf(f);

  return { formula: unmaskOacLiterals(f, lits), warnings };
}

// Masks every single-quoted string literal in `s` behind a sentinel built
// from NUL + digits + SOH — no letters, so none of the ALL-CAPS function-
// name/keyword regexes above can ever match inside one. Mirrors
// maskOmniLiterals (omni.ts) and maskAlteryxLiterals (alteryx.ts) — same
// proven shape, reproduced locally here since each of those files (and
// formulas.ts, the original reference) has a different owner.
//
// A `[bracketed identifier]` span is treated as atomic (an apostrophe
// inside one is part of the identifier, not a literal delimiter), and an
// unterminated `[` or `'` is kept as an ordinary character rather than
// swallowing the rest of the string.
const OAC_LIT_RE = /'(?:[^']|'')*'/g;

function maskOacLiterals(s: string): { masked: string; lits: string[] } {
  const lits: string[] = [];
  let out = '';
  let i = 0;
  while (i < s.length) {
    if (s[i] === '[') {
      const close = s.indexOf(']', i + 1);
      if (close !== -1) {
        out += s.slice(i, close + 1);
        i = close + 1;
        continue;
      }
    }
    if (s[i] === "'") {
      OAC_LIT_RE.lastIndex = i;
      const m = OAC_LIT_RE.exec(s);
      if (m && m.index === i) {
        out += `\u0000${lits.push(m[0]) - 1}\u0001`;
        i += m[0].length;
        continue;
      }
    }
    out += s[i];
    i++;
  }
  return { masked: out, lits };
}

// Restores literals in Sigma form: double-quoted, SQL's '' escape collapsed
// to a single apostrophe, and any embedded double quote backslash-escaped.
function unmaskOacLiterals(s: string, lits: string[]): string {
  return s.replace(/\u0000(\d+)\u0001/g, (_m, i) => {
    const inner = lits[Number(i)].slice(1, -1).replace(/''/g, "'").replace(/"/g, '\\"');
    return `"${inner}"`;
  });
}
