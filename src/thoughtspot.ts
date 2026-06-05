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

  // Build table metadata map. Worksheet TML lists tables under `tables:`;
  // model TML (the format ThoughtSpot actually exports) lists them under
  // `model_tables:` (name + fqn, no db/schema — those come from the overrides).
  const tablesMeta: Record<string, { db: string; schema: string }> = {};
  for (const t of (ws.tables || [])) {
    tablesMeta[t.name] = { db: t.db || dbOverride || '', schema: t.schema || schOverride || '' };
  }
  for (const mt of (ws.model_tables || [])) {
    if (mt?.name && !tablesMeta[mt.name]) {
      tablesMeta[mt.name] = { db: dbOverride || '', schema: schOverride || '' };
    }
  }

  // Column type / aggregation live on the column directly in worksheet TML
  // (`col.type`, `col.aggregation`) but under `col.properties` in model TML
  // (`col.properties.column_type`, `col.properties.aggregation`).
  const colType = (c: any): string => (c.type || c.properties?.column_type || '').toUpperCase();
  const colAgg  = (c: any): string => (c.aggregation || c.properties?.aggregation || 'SUM').toUpperCase();

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
      const isMeasure = colType(col) === 'MEASURE';
      const isDate    = colType(col) === 'DATE';

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
        const agg = colAgg(col);
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

  // Build a map: display name (uppercased) → owning element id. Used to
  // determine which element a formula's column refs belong to so we can place
  // each calc col on its proper host (and pull cross-element calcs onto a
  // derived view with rewritten [Base/REL/Field] refs).
  const dispNameToElId: Record<string, string> = {};
  for (const el of elements) {
    for (const c of (el.columns || [])) {
      // Physical column formula form: [TABLE/PHYS_COL] → display name = sigmaDisplayName(PHYS_COL).
      const fm = c.formula?.match(/^\[([^\/\]]+)\/([^\]]+)\]$/);
      if (fm) {
        const disp = sigmaDisplayName(fm[2]);
        if (!dispNameToElId[disp.toUpperCase()]) dispNameToElId[disp.toUpperCase()] = el.id;
      } else if (c.name) {
        if (!dispNameToElId[c.name.toUpperCase()]) dispNameToElId[c.name.toUpperCase()] = el.id;
      }
    }
  }

  // Bucketing of formula calcs by host element happens AFTER joins are
  // processed (so we can prefer hosts with outgoing relationships).
  type PendingCalc = { col: any; formulaExpr: string; sigmaFormula: string; dispName: string };
  const sameElCalcsByElId: Record<string, PendingCalc[]> = {};
  const crossElCalcsByElId: Record<string, PendingCalc[]> = {};

  // Collect joins. Worksheet TML has a top-level `joins:` list; model TML
  // (the exported format) defines them inline on each table via
  // `model_tables[].joins[]` = { with, on, type, cardinality }. The `on`
  // clause is the same `[T::col] = [T::col]` form in both, so we normalise
  // model-table joins into the worksheet shape and run one loop.
  let tmlJoins: any[] = Array.isArray(ws.joins) ? ws.joins : [];
  if (tmlJoins.length === 0 && Array.isArray(ws.model_tables)) {
    for (const mt of ws.model_tables) {
      for (const j of (mt.joins || [])) {
        tmlJoins.push({ name: j.name || `${mt.name}_to_${j.with}`, on: j.on, type: j.type });
      }
    }
  }

  // Build relationships from joins
  const joinOnRe = /\[([^\]:]+)::([^\]]+)\]\s*=\s*\[([^\]:]+)::([^\]]+)\]/;
  for (const join of tmlJoins) {
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
    // Rel name = uppercased target warehouse path-tail (matches the spec rule used by
    // OAC/Atlan/Qlik/Cube). The TML join.name is often a phrase like "orders_to_customer"
    // which doesn't match the canonical [SRC/REL_NAME/Field] convention.
    const tsTgtPath = rEl.source && (rEl.source as any).path;
    const tsRelName = (tsTgtPath ? tsTgtPath[tsTgtPath.length - 1] : rTable).toUpperCase();
    lEl.relationships!.push({
      id: sigmaShortId(),
      targetElementId: rEl.id,
      keys: [{ sourceColumnId: srcColId, targetColumnId: tgtColId }],
      name: tsRelName,
      relationshipType: 'N:1',
    });
  }

  if (tmlJoins.length === 0 && allTableNames.length > 1) {
    warnings.push('No joins defined in TML — relationships will need to be configured manually in Sigma');
  }

  // ── Bucket formula calc cols by host element ──────────────────────────────
  // A formula whose refs all resolve to a single element is placed there. A
  // formula referencing >1 element is "cross-element" — defer placement until
  // after derived elements are built, then attach to the host's derived view
  // with [Base/REL/Field] refs. Mirrors src/tableau.ts:1993-2129.
  if (formulaCols.length > 0 && elements.length > 0) {
    for (const { col, formulaExpr } of formulaCols) {
      const dispName: string = col.name || 'Calculated';
      const sigmaFormula = tsFormulaToSigma(formulaExpr, elementByTable);
      const refs = sigmaFormula.match(/\[([^\]\/]+)\]/g) || [];
      const refElIds = new Set<string>();
      for (const ref of refs) {
        const inner = ref.replace(/^\[|\]$/g, '');
        const elId = dispNameToElId[inner.toUpperCase()];
        if (elId) refElIds.add(elId);
      }
      // Choose host element. For cross-element formulas we prefer the
      // element that has outgoing relationships — that's the fact-table
      // host whose derived view can expose related-table columns via
      // [Base/REL/Field] refs. For single-element formulas, the only
      // referenced element. Fall back to elements[0] when nothing resolves.
      let hostElId: string;
      if (refElIds.size === 0) {
        hostElId = elements[0].id;
      } else if (refElIds.size === 1) {
        hostElId = Array.from(refElIds)[0];
      } else {
        const candidates = Array.from(refElIds);
        const withRels = candidates.find(id => {
          const el: any = elements.find(e => e.id === id);
          return el?.relationships?.length > 0;
        });
        hostElId = withRels || candidates[0];
      }
      const pending: PendingCalc = { col, formulaExpr, sigmaFormula, dispName };
      if (refElIds.size > 1) {
        (crossElCalcsByElId[hostElId] ||= []).push(pending);
      } else {
        (sameElCalcsByElId[hostElId] ||= []).push(pending);
      }
    }
  }

  // Place same-element calcs on their host element. A TML formula that is itself
  // aggregate-level (e.g. `sum(x)/sum(y)`, `sqrt(sum(x))`, `average(x)`) must be
  // a Sigma METRIC (evaluated in aggregate context) — as a row-level calc column
  // its inner Sum() collapses to the row value and the ratio-of-sums is lost.
  // Row-level formulas (if/then, safe_divide, concat, …) stay calc columns.
  for (const elId of Object.keys(sameElCalcsByElId)) {
    const hostEl = elements.find(e => e.id === elId);
    if (!hostEl) continue;
    for (const p of sameElCalcsByElId[elId]) {
      let fmt: any = inferSigmaFormat(p.sigmaFormula, p.dispName);
      if (fmt?.formatString === ',.2%') fmt = { kind: 'number', formatString: ',.2f', suffix: '%' };
      if (tsIsAggregateFormula(p.formulaExpr)) {
        const metric: any = { id: sigmaShortId(), name: p.dispName, formula: p.sigmaFormula };
        if (fmt) metric.format = fmt;
        (hostEl.metrics ||= []).push(metric);
      } else {
        const colObj: any = { id: sigmaShortId(), name: p.dispName, formula: p.sigmaFormula };
        if (fmt) colObj.format = fmt;
        hostEl.columns.push(colObj);
      }
    }
  }

  // Strip transient helper maps
  for (const el of elements) { delete (el as any)._colPhysIdMap; }

  // Add derived join-view elements
  const derivedEls = buildDerivedElements(elements);
  for (const de of derivedEls) elements.push(de);

  // Place cross-element calc cols onto their host's derived view, rewriting
  // bare [X] refs to the [Base/REL/X] triple-form Sigma needs to resolve a
  // related-table column. Mirrors tableau.ts:2080-2129. Calcs whose host has
  // no derived view fall back to the host element with a warning.
  for (const elId of Object.keys(crossElCalcsByElId)) {
    const calcs = crossElCalcsByElId[elId];
    if (!calcs?.length) continue;
    const srcEl = elements.find(e => e.id === elId);
    if (!srcEl) continue;
    const de = derivedEls.find(d => (d.source as any)?.elementId === elId);
    const srcBaseName = (srcEl as any).name
      || srcEl.source?.path?.[srcEl.source.path.length - 1] || '';

    // Build name → [Base/REL/Name] map from this host's relationships.
    const relatedNameMap: Record<string, string> = {};
    if (srcBaseName && (srcEl as any).relationships?.length) {
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
          // The trailing segment of a `[TABLE/Display Name]` warehouse-column
          // formula is already the display name — do NOT re-apply
          // sigmaDisplayName (it lowercases multi-word names like "Customer
          // Key" → "Customer key").
          const dispName = s >= 0 ? inner.slice(s + 1) : inner;
          if (!(dispName in relatedNameMap)) {
            relatedNameMap[dispName] = `${srcBaseName}/${rel.name}/${dispName}`;
          }
        }
      }
    }

    const placeOn = de || srcEl;
    for (const p of calcs) {
      let formula = p.sigmaFormula;
      if (Object.keys(relatedNameMap).length) {
        formula = formula.replace(/\[([^\]\/]+)\]/g, (match: string, refName: string) => {
          const rewritten = relatedNameMap[refName];
          return rewritten ? `[${rewritten}]` : match;
        });
      }
      const colId = sigmaShortId();
      let fmt: any = inferSigmaFormat(formula, p.dispName);
      if (fmt?.formatString === ',.2%') fmt = { kind: 'number', formatString: ',.2f', suffix: '%' };
      const colObj: any = { id: colId, name: p.dispName, formula };
      if (fmt) colObj.format = fmt;
      (placeOn.columns as any[]).push(colObj);
      if (placeOn.order) (placeOn.order as string[]).push(colId);
    }
    if (!de) {
      warnings.push(`⚠ ${calcs.length} cross-element calc(s) had no derived view for "${srcBaseName}" — placed on source element with bare refs (may error)`);
    }
  }

  const stats = {
    elements: elements.length,
    columns: elements.reduce((n, e) => n + (e.columns?.length || 0), 0),
    metrics: elements.reduce((n, e) => n + (e.metrics?.length || 0), 0),
    relationships: elements.reduce((n, e) => n + (e.relationships?.length || 0), 0),
  };

  return {
    model: { name: modelName, schemaVersion: 1, pages: [{ id: pageId, name: 'Page 1', elements }] },
    warnings,
    stats,
  };
}

// ── ThoughtSpot formula → Sigma formula ────────────────────────────────────

// A TML formula is "aggregate-level" if it applies an aggregate function to a
// column — its value is one number per group, not per row. Such formulas must
// become Sigma metrics, not row-level calc columns.
function tsIsAggregateFormula(expr: string): boolean {
  return /\b(sum|count|count_distinct|unique_count|count_not_null|average|avg|max|min|median|std_deviation|stddev|variance|cumulative_sum|running_total)\s*\(/i
    .test(expr || '');
}

function tsFormulaToSigma(expr: string, _elementByTable: Record<string, any>): string {
  if (!expr) return '';
  let s = expr;
  // Model TML formula refs are `[TABLE::COL]` (e.g. `[ORDER_FACT::GROSS_REVENUE]`).
  // Rewrite to bare `[Display Name]` so downstream column-ref handling and the
  // single-/cross-element bucketing (which key off display names) resolve them.
  // Worksheet TML uses bare identifiers, so this is a no-op there.
  s = s.replace(/\[([^\]:]+)::([^\]]+)\]/g, (_, _tbl, col) => `[${sigmaDisplayName(col.trim())}]`);
  s = tsConvertIfThenElse(s);
  // `<col> in { "a", "b" }` → `In(<col>, "a", "b")`. The left side may now be a
  // bracketed display-name ref (from the rewrite above) or a bare identifier.
  s = s.replace(/(\[[^\]]+\]|\w+)\s+in\s*\{([^}]+)\}/gi, (_, col, vals) => {
    const vlist = vals.split(',').map((v: string) => v.trim()).join(', ');
    const colRef = col.startsWith('[') ? col : `[${sigmaDisplayName(col.trim())}]`;
    return `In(${colRef}, ${vlist})`;
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
  s = tsRewriteSafeDivide(s);
  s = s.replace(/\bisnull\s*\(/gi, 'IsNull(');
  s = s.replace(/\bnot\s*\(/gi, 'Not(');
  s = s.replace(/\btoday\s*\(\s*\)/gi, 'Today()');
  s = s.replace(/\bdate_diff\s*\(/gi, 'DateDiff(');
  s = s.replace(/\bdatediff\s*\(/gi, 'DateDiff(');
  s = tsWrapColumnRefs(s);
  return s;
}

// Paren-balanced rewrite of `safe_divide(a, b)` → `If(IsNull(b) or b = 0, Null(), a / b)`.
// The simple regex form (`[^,)]+,[^)]+`) doesn't match when args contain nested
// parens like `safe_divide(sum(x), sum(y))`.
function tsRewriteSafeDivide(s: string): string {
  let out = '';
  let i = 0;
  const re = /\bsafe_divide\s*\(/gi;
  let m: RegExpExecArray | null;
  while ((m = re.exec(s)) !== null) {
    out += s.slice(i, m.index);
    let depth = 1;
    let j = re.lastIndex;
    let commaIdx = -1;
    while (j < s.length && depth > 0) {
      const ch = s[j];
      if (ch === '(') depth++;
      else if (ch === ')') { depth--; if (depth === 0) break; }
      else if (ch === ',' && depth === 1 && commaIdx === -1) commaIdx = j;
      j++;
    }
    if (depth !== 0 || commaIdx === -1) {
      // unbalanced or single-arg — bail out, leave the original token
      out += m[0];
      i = re.lastIndex;
      continue;
    }
    const a = s.slice(re.lastIndex, commaIdx).trim();
    const b = s.slice(commaIdx + 1, j).trim();
    out += `If(IsNull(${b}) or ${b} = 0, null, ${a} / ${b})`;
    i = j + 1;
    re.lastIndex = i;
  }
  out += s.slice(i);
  return out;
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
