/**
 * ThoughtSpot TML (Thought Model Language) → Sigma Data Model converter.
 * Accepts a ThoughtSpot worksheet or model YAML string.
 */

import yaml from 'js-yaml';
import {
  resetIds, sigmaShortId, sigmaInodeId, sigmaDisplayName,
  sigmaColFormula, inferSigmaFormat, buildDerivedElements,
  makeRlsSecurity,
  type SigmaElement, type SigmaColumn, type SigmaMetric, type ConversionResult, type SecurityRule,
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
  const security: SecurityRule[] = [];   // detected RLS — reported, not injected (architecture B)

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

  // Auto-include physical columns referenced by a JOIN key or a FORMULA but not
  // explicitly selected. Without this, joins drop ("join key columns not found")
  // and formulas referencing an unselected column resolve to `error` in Sigma —
  // real ThoughtSpot models routinely reference base columns they don't surface.
  const refRe = /\[([^\]:]+)::([^\]]+)\]/g;
  const referenced: Array<[string, string]> = [];
  const collectRefs = (s: string) => { let m; while ((m = refRe.exec(s || ''))) referenced.push([m[1], m[2]]); };
  for (const j of (ws.joins || [])) collectRefs(j.on || '');
  for (const mt of (ws.model_tables || [])) for (const j of (mt.joins || [])) collectRefs(j.on || '');
  for (const expr of Object.values(formulaMap)) collectRefs(expr);
  // RLS rule expressions reference base columns that must be surfaced too.
  const collectRlsRefs = (h: any) => {
    const raw = h?.rls_rules ?? h?.rls_rule;
    const rules = Array.isArray(raw) ? raw : (raw?.rules || (raw ? [raw] : []));
    for (const r of rules) collectRefs(String(r?.expr || ''));
  };
  collectRlsRefs(ws);
  for (const mt of (ws.model_tables || [])) collectRlsRefs(mt);
  for (const t of (ws.tables || [])) collectRlsRefs(t);
  for (const [alias, physCol] of referenced) {
    const tableName = tablePathMap[alias] || alias;
    if (!tablesMeta[tableName] && !colsByTable[tableName]) continue; // ref to an unknown table — skip
    const existing = (colsByTable[tableName] ||= []);
    if (!existing.some(e => e.physCol.toUpperCase() === physCol.toUpperCase())) {
      existing.push({ col: { column_id: `${tableName}::${physCol}`, name: sigmaDisplayName(physCol) }, physCol, tableName });
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
  const colAggByDisp: Record<string, string> = {};

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
        // Default aggregation by display name — used by the window-function
        // grouped-element emission (a TS window call takes the RAW measure,
        // e.g. cumulative_sum([Net Revenue], …), and inherits the column's
        // default aggregation).
        colAggByDisp[colDisplayName.toUpperCase()] = sigmaAgg;
        colAggByDisp[dispName.toUpperCase()] = sigmaAgg;
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
  const pendingWindowCalcs: Array<{ dispName: string; formulaExpr: string; call: TsWindowCall | null }> = [];

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
      // Window functions (rank / lead / lag / first / last / running_total /
      // cumulative_* / moving_*) silently error in DM element calc columns and
      // DM metrics — they only evaluate in GROUPED elements (verified; see
      // CLAUDE.md "Sigma window functions in DM elements"). Divert them: each
      // becomes a flagged Null column on its host element PLUS an auto-built
      // grouped child element carrying the Sigma window calc (same handoff the
      // Power BI converter uses for time-intel measures — beads-sigma-5d9k).
      if (TS_WINDOW_FN_RE.test(formulaExpr)) {
        pendingWindowCalcs.push({ dispName, formulaExpr, call: tsParseWindowCall(formulaExpr) });
        continue;
      }
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

  // ── Window-function formulas → grouped child elements ─────────────────────
  tsEmitWindowElements(pendingWindowCalcs, elements, derivedEls, dispNameToElId, colAggByDisp, warnings);

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

  // ── ThoughtSpot RLS rules → Sigma row-level security ───────────────────────
  // RLS rules live on a table (rls_rules / rls_rule): each {name, expr} is a
  // boolean over ts_username / ts_groups / columns. Rules combine with OR
  // (most-permissive). Emit one fail-closed boolean calc per table + element filter.
  const tsRlsByTable: Record<string, any[]> = {};
  const collectRls = (tableName: string, holder: any) => {
    const raw = holder?.rls_rules ?? holder?.rls_rule;
    const rules = Array.isArray(raw) ? raw : (raw?.rules || (raw ? [raw] : []));
    if (rules.length) (tsRlsByTable[tableName] ??= []).push(...rules);
  };
  collectRls((ws.model_tables?.[0]?.name || ws.tables?.[0]?.name || Object.keys(elementByTable)[0] || ''), ws);
  for (const mt of (ws.model_tables || [])) collectRls(mt.name, mt);
  for (const t of (ws.tables || [])) collectRls(t.name, t);
  for (const [tableName, rules] of Object.entries(tsRlsByTable)) {
    const el: any = elementByTable[tableName];
    if (!el || !rules.length) continue;
    const conds = rules.map((r: any) => tsRlsExprToSigma(String(r.expr || ''), elementByTable)).filter(Boolean);
    if (!conds.length) continue;
    const formula = conds.length === 1 ? conds[0] : conds.map(c => `(${c})`).join(' or ');
    security.push(makeRlsSecurity({ source: `ThoughtSpot rls_rules (table "${tableName}", ${rules.length} rule${rules.length > 1 ? 's OR-combined' : ''})`, element: el, name: 'RLS', formula }));
    warnings.push(`🔐 ThoughtSpot RLS on "${tableName}" (${rules.length} rule${rules.length > 1 ? 's, OR-combined' : ''}) → row-level security DETECTED (reported in result.security, not injected). The migration skill provisions the referenced teams/attributes and applies the RLS calc + filter.`);
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
    ...(security.length ? { security } : {}),
    stats,
  };
}

// ── ThoughtSpot formula → Sigma formula ────────────────────────────────────

// A TML formula is "aggregate-level" if it applies an aggregate function to a
// column — its value is one number per group, not per row. Such formulas must
// become Sigma metrics, not row-level calc columns.
function tsIsAggregateFormula(expr: string): boolean {
  return /\b(sum|count|count_distinct|unique_count|count_not_null|average|avg|max|min|median|std_deviation|stddev|variance|cumulative_sum|running_total|sum_if|count_if|average_if|max_if|min_if|unique_count_if)\s*\(/i
    .test(expr || '');
}

// ── ThoughtSpot-specific literal masking ────────────────────────────────────
// tsFormulaToSigma's keyword-mapping tail (sum/count/average/…, the date-part
// and math/string renames, safe_divide/cond-agg arg swaps) used to run plain
// regex .replace() over the formula text right after converting a TML
// '…' literal to a Sigma "…" string — with no further protection until
// tsWrapColumnRefs ran at the very END of the function. Live-reproduced:
// a formula that was NOTHING BUT the literal 'This report shows sum(x)
// trends' came back as "This report shows Sum(\x020\x03) trends" — RAW,
// UNRESTORED SENTINEL BYTES leaking into the final Sigma formula (not just
// mis-rewritten content). Root cause was two-fold:
//   1. The aggregate-rename regex (`sum|count|…`) matched "sum(" INSIDE the
//      already-double-quoted literal and rewrote it, exactly the qlik.ts
//      defect class.
//   2. tsWrapColumnRefs was called TWICE — once per-argument (nested, inside
//      the aggregate-rename replacer) and once on the WHOLE string at the
//      end — each with its OWN separate, local `saved` array and its OWN
//      \x02/\x03 sentinel scheme. The final call's bracket-protection step
//      re-masked the (still raw-double-quoted) literal, which by then
//      CONTAINED the inner call's leftover sentinel from bracketing "x"
//      inside the rewritten "sum(x)" — and String.replace() does not
//      re-scan a replacement value for further matches, so that inner
//      sentinel was never resolved. Two independent mask/restore cycles
//      nested inside one formula corrupt each other; mask ONCE, thread the
//      SAME `lits` through every pass, unmask ONCE.
//
// A `[bracketed field]` is atomic here too (a `[TABLE::COL]` ref, or the
// `[Display Name]` it's rewritten to, might itself contain an apostrophe) —
// an apostrophe inside one is not a quote opener. A pre-existing `"…"` span
// (TML literals are single-quoted; a raw double-quoted span is not TML's own
// convention, but tsWrapColumnRefs already defensively protected it) is
// masked the same way and restored verbatim. An unterminated `'`, `"`, or
// `[` is left as an ordinary character — scanning resumes normally and does
// NOT swallow the rest of the formula.
interface TsMaskedLit { kind: "'" | '"' | '['; raw: string; }
const TS_LIT_SENT = '\x02';
const TS_LIT_SENT_RE = new RegExp(`${TS_LIT_SENT}(\\d+)${TS_LIT_SENT}`, 'g');

function tsMaskLiterals(s: string, lits: TsMaskedLit[] = []): { masked: string; lits: TsMaskedLit[] } {
  let out = '';
  let i = 0;
  while (i < s.length) {
    const ch = s[i];
    if (ch === '[' || ch === '"' || ch === "'") {
      const closeChar = ch === '[' ? ']' : ch;
      const close = s.indexOf(closeChar, i + 1);
      if (close !== -1) {
        const raw = s.slice(i, close + 1);
        out += `${TS_LIT_SENT}${lits.push({ kind: ch as TsMaskedLit['kind'], raw }) - 1}${TS_LIT_SENT}`;
        i = close + 1;
        continue;
      }
    }
    out += ch;
    i++;
  }
  return { masked: out, lits };
}

/** Finalizes every remaining masked span for Sigma output. A `'…'` literal
 *  becomes a Sigma double-quoted string; a `"…"` or `[…]` span is restored
 *  verbatim (unchanged either way). */
function tsUnmaskLiterals(s: string, lits: TsMaskedLit[]): string {
  return s.replace(TS_LIT_SENT_RE, (_m, i) => {
    const lit = lits[Number(i)];
    if (!lit) return _m;
    return lit.kind === "'" ? `"${lit.raw.slice(1, -1)}"` : lit.raw;
  });
}

// Translate a ThoughtSpot RLS rule expression to a Sigma boolean.
// ts_username → CurrentUserEmail(); `<col> =|in ts_groups` → CurrentUserInTeam(<col>);
// the rest (column refs [T::COL]→[Display], operators, in {…}) via tsFormulaToSigma.
function tsRlsExprToSigma(expr: string, elementByTable: Record<string, any>): string {
  if (!expr?.trim()) return '';
  let e = expr.trim();
  // NOTE: the single-quote → double-quote conversion used to happen here,
  // before handing off to tsFormulaToSigma. That's exactly the hazard
  // tsMaskLiterals below exists to close — tsFormulaToSigma now does its own
  // mask-on-entry (which needs to see the ORIGINAL '…' delimiter to tell a
  // literal from a bracket/quoted ref), so this function no longer converts
  // quotes itself.
  e = e.replace(/\[([^\]]+)\]\s*(?:=|in)\s*ts_groups/gi, 'CurrentUserInTeam([$1])');
  e = e.replace(/ts_groups\s*(?:=|in)\s*\[([^\]]+)\]/gi, 'CurrentUserInTeam([$1])');
  e = e.replace(/\bts_username\b/gi, 'CurrentUserEmail()');
  return tsFormulaToSigma(e, elementByTable).trim();
}

function tsFormulaToSigma(expr: string, _elementByTable: Record<string, any>): string {
  if (!expr) return '';
  let s = expr;
  // Model TML formula refs are `[TABLE::COL]` (e.g. `[ORDER_FACT::GROSS_REVENUE]`).
  // Rewrite to bare `[Display Name]` so downstream column-ref handling and the
  // single-/cross-element bucketing (which key off display names) resolve them.
  // Worksheet TML uses bare identifiers, so this is a no-op there. Runs BEFORE
  // masking (below) — it needs to see and parse REAL bracket content, and this
  // narrow, structurally-anchored `alias::col` shape isn't something a
  // free-text TML literal would realistically contain.
  s = s.replace(/\[([^\]:]+)::([^\]]+)\]/g, (_, _tbl, col) => `[${sigmaDisplayName(col.trim())}]`);

  // Mask ONCE, here, for the rest of the function — see the block comment
  // above tsMaskLiterals (near tsIsAggregateFormula). Every keyword-mapping
  // pass below (if/then/else, in{…}, the aggregate map, safe_divide, the
  // cond-agg arg swaps, and the date/math/string/null renames) runs against
  // masked text, full stop — including the ONE bracket-identifier pass at the
  // end (formerly two separate tsWrapColumnRefs calls, each with its own
  // colliding mask/restore cycle).
  const { masked, lits } = tsMaskLiterals(s);
  s = masked;

  s = tsConvertIfThenElse(s);
  // `<col> in { "a", "b" }` → `In(<col>, "a", "b")`. The left side may now be
  // a masked bracket-ref sentinel (from the rewrite above, now masked) or a
  // bare identifier.
  s = s.replace(new RegExp(`(${TS_LIT_SENT}\\d+${TS_LIT_SENT}|\\w+)\\s+in\\s*\\{([^}]+)\\}`, 'gi'), (_, col, vals) => {
    const vlist = vals.split(',').map((v: string) => v.trim()).join(', ');
    const colRef = new RegExp(`^${TS_LIT_SENT}\\d+${TS_LIT_SENT}$`).test(col) ? col : `[${sigmaDisplayName(col.trim())}]`;
    return `In(${colRef}, ${vlist})`;
  });
  // ThoughtSpot's distinct count is the two-word keyword `unique count` —
  // normalise it (and the underscore form) before the aggregate-map pass.
  s = s.replace(/\bunique[\s_]+count\s*\(/gi, 'count_distinct(');
  const tsAggMap: Record<string, string> = {
    sum: 'Sum', count: 'Count', count_distinct: 'CountDistinct',
    average: 'Avg', avg: 'Avg', max: 'Max', min: 'Min', median: 'Median',
    std_deviation: 'StdDev', variance: 'Variance',
    count_not_null: 'CountDistinct', cumulative_sum: 'CumulativeSum',
  };
  // NOTE: the aggregate argument is intentionally left UN-bracketed here —
  // the single tsBracketIdents(s) pass on the whole formula, below, already
  // covers it. Bracketing it twice (once here, once there) re-wraps the
  // already-bracketed identifier the second time around — e.g.
  // `sum(revenue)` → `Sum([Revenue])` here, then the whole-string pass sees
  // `[Revenue]` and, having no bracket-awareness of its own, wraps it again
  // into `[[Revenue]]`; a multi-word display name like `net_revenue` →
  // `[Net Revenue]` gets split into TWO refs, `[[Net] [Revenue]]`, since the
  // second pass's identifier regex matches "Net" and "Revenue" separately
  // once they're space-separated inside the bracket. Live-reproduced.
  s = s.replace(/\b(sum|count_distinct|count_not_null|count|average|avg|max|min|median|std_deviation|variance|cumulative_sum)\s*\(([^)]+)\)/gi,
    (_, fn, arg) => {
      const sigmaFn = tsAggMap[fn.toLowerCase()] || fn;
      return `${sigmaFn}(${arg.trim()})`;
    });
  s = tsRewriteSafeDivide(s);
  // Conditional aggregates. ThoughtSpot puts the condition FIRST
  // (sum_if(cond, measure)); Sigma's SumIf(field, condition) puts it LAST —
  // so these two-arg forms are swapped. count_if(cond) maps directly.
  s = tsRewriteCondAgg(s, 'sum_if', 'SumIf');
  s = tsRewriteCondAgg(s, 'unique_count_if', 'CountDistinctIf');
  s = tsRewriteCondAgg(s, 'average_if', 'AvgIf');
  s = tsRewriteCondAgg(s, 'max_if', 'MaxIf');
  s = tsRewriteCondAgg(s, 'min_if', 'MinIf');
  s = s.replace(/\bcount_if\s*\(/gi, 'CountIf(');
  // Date truncation → DateTrunc("<unit>", …)
  s = s.replace(/\bstart_of_week\s*\(/gi, 'DateTrunc("week", ');
  s = s.replace(/\bstart_of_month\s*\(/gi, 'DateTrunc("month", ');
  s = s.replace(/\bstart_of_quarter\s*\(/gi, 'DateTrunc("quarter", ');
  s = s.replace(/\bstart_of_year\s*\(/gi, 'DateTrunc("year", ');
  s = s.replace(/\bstart_of_day\s*\(/gi, 'DateTrunc("day", ');
  // Date-part extraction
  s = s.replace(/\bmonth_number\s*\(/gi, 'Month(');
  s = s.replace(/\bquarter_number\s*\(/gi, 'Quarter(');
  s = s.replace(/\b(?:day_number_of_week|day_of_week)\s*\(/gi, 'Weekday(');
  s = s.replace(/\bday_of_month\s*\(/gi, 'Day(');
  s = s.replace(/\byear\s*\(/gi, 'Year(');
  s = s.replace(/\bmonth\s*\(/gi, 'Month(');
  s = s.replace(/\bday\s*\(/gi, 'Day(');
  s = s.replace(/\bhour\s*\(/gi, 'Hour(');
  // Math
  s = s.replace(/\bpow(?:er)?\s*\(/gi, 'Power(');
  s = s.replace(/\bsqrt\s*\(/gi, 'Sqrt(');
  s = s.replace(/\babs\s*\(/gi, 'Abs(');
  s = s.replace(/\bround\s*\(/gi, 'Round(');
  s = s.replace(/\bexp\s*\(/gi, 'Exp(');
  s = s.replace(/\bln\s*\(/gi, 'Ln(');
  s = s.replace(/\blog10\s*\(/gi, 'Log10(');
  s = s.replace(/\bceil\s*\(/gi, 'Ceiling(');
  s = s.replace(/\bfloor\s*\(/gi, 'Floor(');
  s = s.replace(/\bmod\s*\(/gi, 'Mod(');
  // String
  s = s.replace(/\bconcat\s*\(/gi, 'Concat(');
  s = s.replace(/\bsubstr(?:ing)?\s*\(/gi, 'Mid(');
  s = s.replace(/\bstrlen\s*\(/gi, 'Len(');
  s = s.replace(/\bupper\s*\(/gi, 'Upper(');
  s = s.replace(/\blower\s*\(/gi, 'Lower(');
  s = s.replace(/\bltrim\s*\(/gi, 'Ltrim(');
  s = s.replace(/\brtrim\s*\(/gi, 'Rtrim(');
  s = s.replace(/\btrim\s*\(/gi, 'Trim(');
  s = s.replace(/\breplace\s*\(/gi, 'Replace(');
  // Null handling
  s = s.replace(/\bifnull\s*\(/gi, 'Coalesce(');
  s = s.replace(/\bcoalesce\s*\(/gi, 'Coalesce(');
  s = s.replace(/\bisnull\s*\(/gi, 'IsNull(');
  s = s.replace(/\bnot\s*\(/gi, 'Not(');
  s = s.replace(/\btoday\s*\(\s*\)/gi, 'Today()');
  s = s.replace(/\bdate_diff\s*\(/gi, 'DateDiff(');
  s = s.replace(/\bdatediff\s*\(/gi, 'DateDiff(');
  s = tsBracketIdents(s);
  // The single unmask that closes out this function's mask-on-entry — a
  // masked `'…'` literal becomes a Sigma double-quoted string; a masked
  // `[…]`/`"…"` span is restored verbatim.
  return tsUnmaskLiterals(s, lits);
}

// Paren-balanced rewrite of a ThoughtSpot two-arg conditional aggregate
// `<tsFn>(condition, measure)` → `<sigmaFn>(measure, condition)` (arg swap).
function tsRewriteCondAgg(s: string, tsFn: string, sigmaFn: string): string {
  let out = '';
  let i = 0;
  const re = new RegExp(`\\b${tsFn}\\s*\\(`, 'gi');
  let m: RegExpExecArray | null;
  while ((m = re.exec(s)) !== null) {
    out += s.slice(i, m.index);
    let depth = 1, j = re.lastIndex, commaIdx = -1;
    while (j < s.length && depth > 0) {
      const ch = s[j];
      if (ch === '(') depth++;
      else if (ch === ')') { depth--; if (depth === 0) break; }
      else if (ch === ',' && depth === 1 && commaIdx === -1) commaIdx = j;
      j++;
    }
    if (depth !== 0 || commaIdx === -1) { out += m[0]; i = re.lastIndex; continue; }
    const cond = s.slice(re.lastIndex, commaIdx).trim();
    const measure = s.slice(commaIdx + 1, j).trim();
    out += `${sigmaFn}(${measure}, ${cond})`;
    i = j + 1;
    re.lastIndex = i;
  }
  out += s.slice(i);
  return out;
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

// Bracket bare identifiers so downstream column-ref resolution catches them.
// Formerly named tsWrapColumnRefs and called TWICE per formula (once nested,
// per-aggregate-argument; once on the whole string at the end) — each call
// did its OWN separate stash/restore of brackets and quotes with its own
// local `saved` array. Live-reproduced: those two independent mask cycles
// nested inside one formula corrupted each other — the outer call's bracket
// mask re-wrapped the (still-visible) inner call's leftover sentinel, and
// String.replace() doesn't re-scan a replacement value for further matches,
// so the inner sentinel was never resolved and RAW CONTROL BYTES leaked into
// the final Sigma formula. Both call sites now arrive PRE-MASKED (via
// tsMaskLiterals in tsFormulaToSigma, once, shared across the whole formula)
// so this function no longer needs — or does — any masking of its own; a
// bare identifier can only contain [A-Za-z0-9_], so wrapping one in a fresh
// `[…]` bracket here is always safe and never needs to be masked itself.
function tsBracketIdents(expr: string): string {
  const skip = /^(if|then|else|and|or|not|in|null|true|false|today|IsNull|If|In|List|Sum|Count|Avg|Max|Min|CountDistinct|StdDev|Variance|DateDiff|Today|CumulativeSum|Not)$/;
  return expr.replace(/\b([A-Z_][A-Z0-9_]*)\b(?!\s*\()/gi, (match, ident) => {
    if (skip.test(ident)) return match;
    return `[${sigmaDisplayName(ident)}]`;
  });
}

// ── ThoughtSpot window functions → grouped child elements ───────────────────
//
// HARD CONSTRAINT (live-verified; CLAUDE.md + feedback_sigma_window_functions):
// Sigma window functions (CumulativeSum, Rank, Lag, Lead, MovingAvg, First,
// Last, …) silently compile to error-type columns in data-model element calc
// columns and DM metrics. They only evaluate in a GROUPED element. The proven
// handoff (Power BI time-intel, exact-parity-verified) is to emit a grouped
// child table element whose innermost grouping level carries the aggregate and
// the window calc, and leave a flagged Null placeholder column on the host so
// the original column name stays discoverable with re-author instructions in
// its description (beads-sigma-5d9k).

const TS_WINDOW_SIGMA: Record<string, string> = {
  cumulative_sum: 'CumulativeSum', running_total: 'CumulativeSum', running_sum: 'CumulativeSum',
  cumulative_average: 'CumulativeAvg', cumulative_avg: 'CumulativeAvg',
  cumulative_max: 'CumulativeMax', cumulative_min: 'CumulativeMin',
  running_count: 'CumulativeCount',
  moving_average: 'MovingAvg', moving_avg: 'MovingAvg', moving_sum: 'MovingSum',
  moving_max: 'MovingMax', moving_min: 'MovingMin',
  rank: 'Rank', rank_desc: 'Rank', lead: 'Lead', lag: 'Lag',
  first: 'First', first_value: 'First', last: 'Last', last_value: 'Last',
};

const TS_WINDOW_FN_RE = new RegExp(
  `\\b(${Object.keys(TS_WINDOW_SIGMA).sort((a, b) => b.length - a.length).join('|')})\\s*\\(`, 'i');

interface TsWindowCall { fn: string; args: string[] }

// Parse a formula that IS a single window-function call (the whole expression).
// Returns null for embedded usage (`cumulative_sum(x, d) / 100`) — those can't
// be decomposed into one grouped element and degrade to flag-only.
function tsParseWindowCall(expr: string): TsWindowCall | null {
  const t = (expr || '').trim();
  const m = t.match(/^([a-z_]+)\s*\(/i);
  if (!m || !(m[1].toLowerCase() in TS_WINDOW_SIGMA)) return null;
  let depth = 1, j = m[0].length, cur = '';
  const args: string[] = [];
  for (; j < t.length; j++) {
    const ch = t[j];
    if (ch === '(') depth++;
    else if (ch === ')') { depth--; if (depth === 0) break; }
    if (ch === ',' && depth === 1) { args.push(cur.trim()); cur = ''; continue; }
    cur += ch;
  }
  if (depth !== 0 || j !== t.length - 1) return null;
  if (cur.trim()) args.push(cur.trim());
  return { fn: m[1].toLowerCase(), args };
}

function tsEmitWindowElements(
  pend: Array<{ dispName: string; formulaExpr: string; call: TsWindowCall | null }>,
  elements: SigmaElement[],
  derivedEls: SigmaElement[],
  dispNameToElId: Record<string, string>,
  colAggByDisp: Record<string, string>,
  warnings: string[],
): void {
  if (!pend.length || !elements.length) return;
  const refsOf = (sig: string): string[] =>
    (sig.match(/\[([^\]\/]+)\]/g) || []).map(r => r.slice(1, -1));
  // Display name of a host column ([TABLE/PHYS] → derived display; calc/date cols carry name).
  const hostColDisp = (c: any): string => {
    if (c.name) return c.name;
    const fm = (c.formula || '').match(/^\[([^\/\]]+)\/([^\]]+)\]$/);
    return fm ? sigmaDisplayName(fm[2]) : '';
  };

  for (const p of pend) {
    const flagOnly = (host: any, reason: string) => {
      const colId = sigmaShortId();
      (host.columns as any[]).push({
        id: colId, name: p.dispName, formula: 'Null',
        description: `ThoughtSpot window function (re-author as a grouped-element calc in Sigma — window functions error in DM calc columns): ${p.formulaExpr}`,
      });
      if (host.order) (host.order as string[]).push(colId);
      warnings.push(`⚠ "${p.dispName}": ${reason} Left as a flagged Null column on "${host.name}" with the original expression in its description — re-author as a calc in a grouped workbook element.`);
    };

    // Resolve host from the formula's refs (fall back to the first element).
    const allRefs = refsOf(tsFormulaToSigma(p.formulaExpr, {}));
    const hostElId = allRefs.map(r => dispNameToElId[r.toUpperCase()]).find(Boolean) || elements[0].id;
    const host: any = elements.find(e => e.id === hostElId) || elements[0];

    if (!p.call || !p.call.args.length) {
      flagOnly(host, 'window function is embedded in a larger expression (or has no arguments), which can\'t be decomposed into a single grouped element.');
      continue;
    }

    const fn = p.call.fn;
    const sigmaFn = TS_WINDOW_SIGMA[fn];
    const rawMeasure = p.call.args[0];
    const measureSigma = tsFormulaToSigma(rawMeasure, {});

    // Classify trailing args: integers → window offsets, the rest → dims.
    const nums: string[] = [];
    const dimSigs: string[] = [];
    for (const a of p.call.args.slice(1)) {
      if (/^-?\d+$/.test(a.trim())) nums.push(a.trim());
      else dimSigs.push(tsFormulaToSigma(a, {}));
    }

    // The grouped element's parent: the host itself when every ref lives there,
    // the host's derived join view when refs span elements (the view already
    // denormalizes related columns as "Col (REL)" — same pattern the PBI
    // time-intel emission uses).
    const offHost = [...refsOf(measureSigma), ...dimSigs.flatMap(refsOf)]
      .filter(r => { const id = dispNameToElId[r.toUpperCase()]; return id && id !== host.id; });
    let parent: any = host;
    let crossRel: Record<string, string> | null = null;   // ref display → "ref (REL)"
    if (offHost.length) {
      const view = derivedEls.find(d => (d.source as any)?.elementId === host.id);
      const rels: any[] = host.relationships || [];
      crossRel = {};
      let ok = !!view;
      for (const r of new Set(offHost)) {
        const tgtId = dispNameToElId[r.toUpperCase()];
        const rel = rels.find(rr => rr.targetElementId === tgtId && rr.name);
        if (!rel) { ok = false; break; }
        crossRel[r] = `${r} (${rel.name})`;
      }
      if (!ok) {
        flagOnly(host, 'window function references columns across elements with no derived join view to host the grouped calc.');
        continue;
      }
      parent = view;
    }
    const parentName: string = parent.name;
    // Qualify bare [X] refs against the parent element ([Parent/X], with the
    // derived-view "X (REL)" display for cross-element refs).
    const qualify = (sig: string): string =>
      sig.replace(/\[([^\]\/]+)\]/g, (mch, ref) => {
        const disp = crossRel && crossRel[ref] ? crossRel[ref] : ref;
        return `[${parentName}/${disp}]`;
      });

    // Grouping dims. Default (no dims in the call): the host's first date
    // column, else its first physical column.
    let dims: Array<{ name: string; formula: string }> = [];
    for (let i = 0; i < dimSigs.length; i++) {
      const sig = dimSigs[i];
      const bare = sig.match(/^\[([^\]\/]+)\]$/);
      if (bare) {
        const disp = bare[1];
        dims.push({ name: disp, formula: qualify(sig) });
      } else {
        dims.push({ name: `${p.dispName} Key ${i + 1}`, formula: qualify(sig) });
      }
    }
    if (!dims.length) {
      const hostCols: any[] = host.columns || [];
      const dateCol = hostCols.find(c => typeof c.formula === 'string' && c.formula.startsWith('DateTrunc('));
      const fallback = dateCol || hostCols.find(c => hostColDisp(c) && c.formula !== 'Null');
      if (!fallback) {
        flagOnly(host, `${fn}() has no ordering dimension and "${host.name}" has no usable column to group by.`);
        continue;
      }
      const disp = hostColDisp(fallback);
      dims.push({ name: disp, formula: qualify(`[${disp}]`) });
      warnings.push(`ℹ "${p.dispName}": ${fn}() had no explicit dimension — grouped by "${disp}" (host's ${dateCol ? 'date' : 'first'} column). Adjust the grouping in Sigma if a different grain is wanted.`);
    }

    // Aggregate value column. A raw measure ref inherits the column's default
    // aggregation (TS window calls take the raw measure — no sum() wrap); an
    // already-aggregated arg passes through; any other expression is Sum-wrapped.
    let valFormula: string;
    let valName: string;
    const bareMeasure = measureSigma.match(/^\[([^\]\/]+)\]$/);
    if (tsIsAggregateFormula(rawMeasure)) {
      valFormula = qualify(measureSigma);
      valName = `${p.dispName} Base`;
    } else if (bareMeasure) {
      valName = bareMeasure[1];
      const agg = colAggByDisp[valName.toUpperCase()] || 'Sum';
      valFormula = `${agg}(${qualify(measureSigma)})`;
    } else {
      valFormula = `Sum(${qualify(measureSigma)})`;
      valName = `${p.dispName} Base`;
    }

    // Window calc formula (Sigma signatures verified against the function docs:
    // Rank([col], "asc"|"desc"), Lag/Lead(value, offset), MovingAvg(col, above,
    // [below]), Cumulative*(col), First/Last(col)).
    let winFormula: string;
    if (sigmaFn === 'Rank') {
      winFormula = `Rank([${valName}], "${fn === 'rank_desc' ? 'desc' : 'asc'}")`;
    } else if (sigmaFn === 'Lead' || sigmaFn === 'Lag') {
      winFormula = `${sigmaFn}([${valName}], ${nums[0] || '1'})`;
    } else if (sigmaFn.startsWith('Moving')) {
      winFormula = `${sigmaFn}([${valName}], ${nums[0] || '1'}${nums.length > 1 ? `, ${nums[1]}` : ''})`;
    } else {
      winFormula = `${sigmaFn}([${valName}])`;
    }

    const dimCols = dims.map(d => ({ id: sigmaShortId(), formula: d.formula, name: d.name }));
    const vId = sigmaShortId();
    const wId = sigmaShortId();
    const cols: any[] = [
      ...dimCols,
      { id: vId, formula: valFormula, name: valName },
      { id: wId, formula: winFormula, name: p.dispName },
    ];
    elements.push({
      id: sigmaShortId(), kind: 'table', name: p.dispName,
      source: { kind: 'table', elementId: parent.id },
      columns: cols, order: cols.map(c => c.id),
      groupings: [{ id: sigmaShortId(), groupBy: dimCols.map(c => c.id), calculations: [vId, wId] }],
    } as any);

    // Flagged placeholder on the host so the original column name resolves and
    // carries a pointer to the grouped element.
    const flagId = sigmaShortId();
    (host.columns as any[]).push({
      id: flagId, name: p.dispName, formula: 'Null',
      description: `ThoughtSpot window function — auto-built as grouped element "${p.dispName}" in this data model (window functions error in DM calc columns): ${p.formulaExpr}`,
    });
    if (host.order) (host.order as string[]).push(flagId);
    warnings.push(`ℹ "${p.dispName}": ${fn}() → grouped ${sigmaFn} element "${p.dispName}" on "${parentName}" (group by ${dims.map(d => `"${d.name}"`).join(', ')}). The column on "${host.name}" is a flagged Null placeholder pointing at it.`);
  }
}
