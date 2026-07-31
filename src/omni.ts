/**
 * Omni Analytics YAML → Sigma Data Model JSON converter.
 *
 * Accepts .view.yaml files (dimensions + measures) and .model.yaml files
 * (explores + joins). Multiple files can be passed together.
 */

import yaml from 'js-yaml';
import {
  resetIds, sigmaShortId, sigmaInodeId, sigmaDisplayName,
  sigmaColFormula, sigmaAggFormula,
  type SigmaElement, type ConversionResult,
} from './sigma-ids.js';

// ── Public interface ─────────────────────────────────────────────────────────

export interface OmniFile {
  name: string;
  content: string;
}

export interface OmniConvertOptions {
  connectionId?: string;
  database?: string;
  schema?: string;
}

export function convertOmniToSigma(
  files: OmniFile[],
  options: OmniConvertOptions = {},
): ConversionResult {
  resetIds();

  const { connectionId = '<CONNECTION_ID>', database = '', schema = '' } = options;
  const dbOverride     = database.trim().toUpperCase();
  const schemaOverride = schema.trim().toUpperCase();

  const views: OmniView[]   = [];
  const explores: OmniExplore[] = [];
  const warnings: string[]  = [];

  // ── Parse all files ──────────────────────────────────────────────────────

  for (const file of files) {
    try {
      const docs: any[] = [];
      yaml.loadAll(file.content, (d) => { if (d) docs.push(d); });

      for (const doc of docs) {
        // View file: top-level `view:` key OR has dimensions/measures
        if (doc.view || doc.dimensions || doc.measures) {
          const v: OmniView = doc.view
            ? { name: String(doc.view), ...doc }
            : doc;
          views.push(v);
        }
        // Inline views list (some Omni exports use `views: [...]`)
        if (Array.isArray(doc.views)) {
          for (const v of doc.views) {
            if (v && v.name) views.push(v as OmniView);
          }
        }
        // Model file: has `explores:`
        if (Array.isArray(doc.explores)) {
          explores.push(...doc.explores as OmniExplore[]);
        }
      }
    } catch (e: any) {
      warnings.push(`${file.name}: parse error — ${e.message}`);
    }
  }

  if (views.length === 0) {
    return {
      model: { name: 'Omni Analytics Model', schemaVersion: 1, pages: [{ id: sigmaShortId(), name: 'Page 1', elements: [] }] },
      warnings: ['No views found in the provided files'],
      stats: {},
    };
  }

  // ── Convert views → elements ──────────────────────────────────────────────

  const elements: SigmaElement[] = [];
  // viewName (lowercase) → { elementId, pkColId, colIdMap, element, sourceTable }
  const viewRegistry = new Map<string, ViewEntry>();
  let totalDims = 0;
  let totalMeasures = 0;

  for (const view of views) {
    const viewName    = String(view.name || view.view || 'unknown');
    const displayName = sigmaDisplayName(viewName);
    const tableName   = viewName.toUpperCase();

    const elementId = sigmaShortId();
    let element: SigmaElement;
    let sourceTable: string;

    if (view.derived_table) {
      // Derived table → Custom SQL element (same as LookML converter)
      const rawSql = (view.derived_table.sql ?? '').trim();
      sourceTable = 'Custom SQL';
      element = {
        id:     elementId,
        kind:   'table',
        source: { connectionId, kind: 'sql', statement: rawSql },
        columns: [],
        metrics: [],
        order:   [],
      };
      if (rawSql) {
        warnings.push(`ℹ "${viewName}" → Custom SQL element. Review the SQL before saving.`);
      } else {
        warnings.push(`⚠ "${viewName}" derived_table has no sql — SQL statement left blank. Add SQL manually in the JSON editor before saving.`);
      }
    } else {
      // Warehouse table — parse sql_table_name into path segments
      let path: string[] = [];
      if (view.sql_table_name) {
        const raw = view.sql_table_name.replace(/"/g, '').trim();
        path = raw.split('.').map((s) => s.trim().toUpperCase()).filter(Boolean);
      } else {
        path = [tableName];
      }

      // Apply database/schema overrides for incomplete paths
      if (path.length === 1) {
        const table = path[0];
        if (dbOverride && schemaOverride) path = [dbOverride, schemaOverride, table];
        else if (schemaOverride)           path = [schemaOverride, table];
        else if (dbOverride)               path = [dbOverride, table];
      } else if (path.length === 2 && dbOverride) {
        path = [dbOverride, path[0], path[1]];
      }

      sourceTable = path[path.length - 1] || tableName;
      // No element.name — Sigma defaults to last path segment (e.g.
      // CUSTOMER_DIM), which matches `[CUSTOMER_DIM/Col]` formula refs.
      // Setting a display-cased name (e.g. "Customer Dim") breaks resolution.
      element = {
        id:     elementId,
        kind:   'table',
        source: { connectionId, kind: 'warehouse-table', path },
        columns: [],
        metrics: [],
        order:   [],
      };
    }

    const colIdMap: Record<string, string> = {};
    let pkColId: string | null = null;

    function addCol(fieldName: string, formula: string, label?: string): string {
      const id = sigmaInodeId(fieldName.toUpperCase());
      colIdMap[fieldName.toUpperCase()] = id;
      const col: any = { id, formula };
      if (label) col.name = label;
      element.columns.push(col);
      element.order.push(id);
      return id;
    }

    // ── Dimensions → columns ────────────────────────────────────────────────
    for (const dim of view.dimensions ?? []) {
      const name = dim.name || '';
      if (!name) continue;
      totalDims++;

      // Detect REGEXP functions — Sigma uses RegexpMatch([col], "pattern") but these
      // can't be auto-translated without knowing which argument is the pattern.
      if (/\b(?:REGEXP_LIKE|REGEXP_CONTAINS|REGEXP_SUBSTR|REGEXP_REPLACE|REGEXP_EXTRACT)\s*\(/i.test(dim.sql || '')) {
        warnings.push(`⚠ "${name}": uses REGEXP function — skipped. Sigma uses RegexpMatch([col], "pattern") syntax — add this column manually.`);
        totalDims--; // don't count skipped dims
        continue;
      }

      if (dim.primary_key) {
        const formula = dim.sql
          ? omniTranslateFormula(dim.sql, sourceTable)
          : sigmaColFormula(sourceTable, name);
        pkColId = addCol(name, formula ?? sigmaColFormula(sourceTable, name), dim.label);
        continue;
      }

      if (dim.type === 'time') {
        // Expand timeframes
        const baseFormula = dim.sql
          ? (omniTranslateFormula(dim.sql, sourceTable) ?? sigmaColFormula(sourceTable, name))
          : sigmaColFormula(sourceTable, name);

        const tfMap: Record<string, string> = {
          raw:           baseFormula,
          date:          `DateTrunc("day", ${baseFormula})`,
          week:          `DateTrunc("week", ${baseFormula})`,
          month:         `DateTrunc("month", ${baseFormula})`,
          quarter:       `DateTrunc("quarter", ${baseFormula})`,
          year:          `DateTrunc("year", ${baseFormula})`,
          day_of_week:   `DatePart("dayofweek", ${baseFormula})`,
          hour:          `DateTrunc("hour", ${baseFormula})`,
          hour_of_day:   `DatePart("hour", ${baseFormula})`,
          minute:        `DateTrunc("minute", ${baseFormula})`,
          month_name:    `Text(DateTrunc("month", ${baseFormula}), "MMMM")`,
          quarter_of_year: `DatePart("quarter", ${baseFormula})`,
          week_of_year:  `DatePart("week", ${baseFormula})`,
        };

        const tfs: string[] = Array.isArray(dim.timeframes) && dim.timeframes.length > 0
          ? dim.timeframes
          : ['raw', 'date', 'week', 'month', 'quarter', 'year'];

        let first = true;
        for (const tf of tfs) {
          if (tf === 'raw' || first) {
            addCol(name, baseFormula, dim.label);
            first = false;
            if (tf === 'raw') continue;
          }
          const tfFormula = tfMap[tf];
          if (tfFormula) {
            const tfLabel = dim.label ? `${dim.label} (${tf.replace(/_/g, ' ')})` : undefined;
            addCol(`${name}_${tf}`, tfFormula, tfLabel);
          }
        }
      } else {
        const formula = dim.sql
          ? (omniTranslateFormula(dim.sql, sourceTable) ?? sigmaColFormula(sourceTable, name))
          : sigmaColFormula(sourceTable, name);
        addCol(name, formula, dim.label);
      }
    }

    // ── Measures → metrics ──────────────────────────────────────────────────
    for (const measure of view.measures ?? []) {
      const name = measure.name || '';
      if (!name) continue;
      totalMeasures++;

      const type = (measure.type || 'count').toLowerCase();
      let formula: string;

      if (type === 'count') {
        // count has no sql — use PK column if available
        const pkKey = pkColId
          ? Object.keys(colIdMap).find((k) => colIdMap[k] === pkColId) || name
          : (element.columns[0]?.id?.split('/')[1]?.toLowerCase() || name);
        formula = `CountIf(IsNotNull([${sigmaDisplayName(pkKey)}]))`;
      } else if (measure.sql) {
        const rawExpr = omniTranslateFormula(measure.sql, sourceTable) ?? measure.sql;
        // Metrics reference columns by display name only — strip [TABLE/col] → [col]
        const metricExpr = rawExpr.replace(/\[([^/\]]+)\/([^\]]+)\]/g, '[$2]');
        const aggWrap: Record<string, (e: string) => string> = {
          sum:            (e) => `Sum(${e})`,
          average:        (e) => `Avg(${e})`,
          avg:            (e) => `Avg(${e})`,
          min:            (e) => `Min(${e})`,
          max:            (e) => `Max(${e})`,
          count_distinct: (e) => `CountDistinct(${e})`,
          median:         (e) => `Median(${e})`,
          sum_distinct:   (e) => `Sum(${e})`,
        };
        const wrap = aggWrap[type];
        if (!wrap) warnings.push(`⚠ measure "${name}": aggregate type "${type}" has no Sigma mapping — defaulted to Sum; verify the aggregation is correct.`);
        formula = (wrap ?? ((e: string) => `Sum(${e})`))(metricExpr);
      } else {
        formula = sigmaAggFormula(type, name, warnings);
      }

      const metricId = sigmaInodeId(name.toUpperCase());
      colIdMap[name.toUpperCase()] = metricId;
      (element.metrics ??= []).push({
        id:      metricId,
        name:    measure.label ?? sigmaDisplayName(name),
        formula,
      });
    }

    // For Custom SQL elements, wrap the user's SQL with an outer SELECT that
    // aliases each projected column to its display name, and rewrite each
    // column's bare `[Display]` formula to qualified `[Custom SQL/Display]`.
    // Without this, Sigma can't resolve the formulas at query time and every
    // column shows "Unknown column" errors.
    if (element.source.kind === 'sql' && sourceTable === 'Custom SQL') {
      const rawSql = String(element.source.statement || '').trim();
      if (rawSql) {
        const passthroughs: Array<{ phys: string; display: string }> = [];
        for (const col of element.columns) {
          const m = (col.formula || '').match(/^\[([^\/\]]+)\]$/);
          if (!m) continue;
          const display = m[1];
          const physMatches = Object.entries(colIdMap).find(([, id]) => id === col.id);
          const phys = physMatches ? physMatches[0] : display.toUpperCase().replace(/\s+/g, '_');
          passthroughs.push({ phys, display });
        }
        if (passthroughs.length) {
          const aliasList = passthroughs.map(p => `"${p.phys}" AS "${p.display}"`).join(', ');
          element.source.statement = `SELECT ${aliasList}\nFROM (\n${rawSql}\n) AS _src`;
          for (const col of element.columns) {
            const m = (col.formula || '').match(/^\[([^\/\]]+)\]$/);
            if (m) col.formula = `[Custom SQL/${m[1]}]`;
          }
        }
      }
    }

    elements.push(element);
    viewRegistry.set(viewName.toLowerCase(), { elementId, pkColId, colIdMap, element, sourceTable });
  }

  // ── Wire relationships from explores/joins ────────────────────────────────

  for (const explore of explores) {
    const fromViewName = (explore.from || explore.name || '').toLowerCase();
    const fromEntry = viewRegistry.get(fromViewName);

    for (const join of explore.joins ?? []) {
      const toViewName = (join.from || join.name || '').toLowerCase();
      const toEntry = viewRegistry.get(toViewName);

      if (!fromEntry || !toEntry) {
        if (fromEntry || toEntry) {
          warnings.push(`Join "${join.name}": view "${toViewName}" not found in loaded files — skipping relationship`);
        }
        continue;
      }

      let srcColId: string | null = null;
      let tgtColId: string | null = null;

      // Parse sql_on: ${from.col} = ${to.col}
      if (join.sql_on) {
        const m = join.sql_on.match(/\$\{(\w+)\.(\w+)\}\s*=\s*\$\{(\w+)\.(\w+)\}/);
        if (m) {
          const [, v1, c1, v2, c2] = m;
          const left  = { view: v1.toLowerCase(), col: c1.toUpperCase() };
          const right = { view: v2.toLowerCase(), col: c2.toUpperCase() };
          const srcSide = left.view  === fromViewName ? left  : right;
          const tgtSide = right.view === toViewName   ? right : left;
          srcColId = fromEntry.colIdMap[srcSide.col] ?? null;
          tgtColId = toEntry.colIdMap[tgtSide.col]   ?? null;
        }
      }

      // Fall back to foreign_key shorthand
      if (!srcColId && join.foreign_key) {
        const fkCol = join.foreign_key.includes('.')
          ? join.foreign_key.split('.')[1].toUpperCase()
          : join.foreign_key.toUpperCase();
        srcColId = fromEntry.colIdMap[fkCol] ?? null;
        tgtColId = toEntry.pkColId;
      }

      // Relationship name = uppercase target table name (matches dbt/derived
      // element convention: cross-element refs are [SRC/REL_NAME/Col]).
      const tgtTableName = toEntry.element.source?.kind === 'warehouse-table'
        ? (toEntry.element.source.path[toEntry.element.source.path.length - 1] || toViewName.toUpperCase())
        : toViewName.toUpperCase();
      const rel: any = {
        id:               sigmaShortId(),
        targetElementId:  toEntry.elementId,
        name:             tgtTableName,
        relationshipType: join.relationship === 'one_to_many' ? '1:N' : 'N:1',
      };
      if (srcColId && tgtColId) {
        rel.keys = [{ sourceColumnId: srcColId, targetColumnId: tgtColId }];
      } else {
        warnings.push(`Join "${join.name}": could not resolve column keys — relationship added without key mapping`);
      }

      (fromEntry.element.relationships ??= []).push(rel);
    }
  }

  // ── Pull cross-view calc cols off source elements (moved to derived) ────
  // A computed dim like `${other_view.field}` is translated to a bare [Field]
  // ref on the source warehouse-table element where Sigma can't resolve it.
  // Mirror the tableau converter: detect refs that don't match any local col,
  // pull them off the source, then place onto the derived element with
  // [SrcTable/REL_NAME/Field] triple-form refs.  (See tableau.ts:1993-2129.)
  const crossElCalcsByElId: Record<string, any[]> = {};
  for (const el of elements as any[]) {
    if (el.source?.kind !== 'warehouse-table') continue;
    if (!el.relationships?.length) continue;

    // Local names = display names of own physical pass-through cols. Calc
    // cols (those with `name`) are NOT counted because their bare [X] refs
    // could be cross-view themselves (the bug we're fixing).
    const localNames = new Set<string>();
    for (const c of (el.columns || [])) {
      if (!c.formula || c.name) continue;
      const m = c.formula.match(/^\[[^\]\/]+\/([^\]]+)\]$/);
      if (m) localNames.add(m[1].toUpperCase());
      const m2 = c.formula.match(/^\[([^\]\/]+)\]$/);
      if (m2) localNames.add(m2[1].toUpperCase());
    }

    const crossEl: any[] = [];
    const keep: any[] = [];
    for (const c of (el.columns || [])) {
      if (!c.name || !c.formula) { keep.push(c); continue; }
      // Pure [TABLE/Col] passthrough — keep on source
      if (/^\[[^\]\/]+\/[^\]]+\]$/.test(c.formula)) { keep.push(c); continue; }
      // Pure [Col] passthrough that resolves locally — keep
      const single = c.formula.match(/^\[([^\]\/]+)\]$/);
      if (single && localNames.has(single[1].toUpperCase())) { keep.push(c); continue; }
      const refs = c.formula.match(/\[([^\]\/]+)\]/g) || [];
      const hasCross = refs.some((ref: string) => {
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

  // Build derived elements per source element with relationships and surface
  // joined cols via [SrcTable/REL_NAME/Field] cross-element refs. Then place
  // pulled cross-view calcs onto matching derived element, rewriting bare
  // [X] refs to triple-form.
  const derivedEls = buildDerivedElementsForOmni(elements as any[]);
  for (const de of derivedEls) (elements as any[]).push(de);

  const placedSrcElIds: Record<string, boolean> = {};
  for (const de of derivedEls) {
    if (de.source?.kind !== 'table' || !de.source.elementId) continue;
    const srcElId = de.source.elementId;
    const calcs = crossElCalcsByElId[srcElId];
    if (!calcs?.length) continue;
    const srcEl: any = (elements as any[]).find(e => e.id === srcElId);
    if (!srcEl) continue;
    const srcBaseName: string = srcEl.name
      || (srcEl.source?.path?.[srcEl.source.path.length - 1] ?? '');
    const relatedNameMap: Record<string, string> = {};
    if (srcEl.relationships && srcBaseName) {
      for (const rel of (srcEl.relationships || [])) {
        if (!rel.name) continue;
        const tgtEl: any = (elements as any[]).find(e => e.id === rel.targetElementId);
        if (!tgtEl || tgtEl.source?.kind !== 'warehouse-table') continue;
        for (const tc of (tgtEl.columns || [])) {
          if (!tc.formula || tc.formula.startsWith('/*')) continue;
          const fm = tc.formula.match(/^\[([^\]]+)\]$/);
          if (!fm) continue;
          const inner = fm[1];
          const s = inner.lastIndexOf('/');
          const dispName = s >= 0 ? inner.slice(s + 1) : inner;
          if (tc.name && !(tc.name in relatedNameMap)) {
            relatedNameMap[tc.name] = `${srcBaseName}/${rel.name}/${dispName}`;
          }
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
    warnings.push(`ℹ ${calcs.length} calc col(s) moved to derived "${de.name}" (cross-view refs)`);
    placedSrcElIds[srcElId] = true;
  }
  for (const elId of Object.keys(crossElCalcsByElId)) {
    if (placedSrcElIds[elId]) continue;
    for (const c of crossElCalcsByElId[elId]) {
      warnings.push(`⚠ "${c.name}" cross-view refs but no derived element — column dropped`);
    }
  }

  // Remove empty metrics/relationships arrays to keep JSON clean
  for (const el of elements) {
    if (el.metrics?.length === 0)       delete el.metrics;
    if (el.relationships?.length === 0) delete el.relationships;
  }

  const modelName = views.length === 1
    ? sigmaDisplayName(String(views[0].name || views[0].view || 'Omni Model'))
    : 'Omni Analytics Model';

  return {
    model: {
      name:  modelName,
      schemaVersion: 1,
      pages: [{ id: sigmaShortId(), name: 'Page 1', elements }],
    },
    warnings,
    stats: {
      views:    views.length,
      explores: explores.length,
      elements: elements.length,
      columns:  elements.reduce((s, e) => s + (e.columns?.length ?? 0), 0),
      metrics:  elements.reduce((s, e) => s + (e.metrics?.length  ?? 0), 0),
      relationships: elements.reduce((s, e) => s + (e.relationships?.length ?? 0), 0),
      totalDims,
      totalMeasures,
    },
  };
}

/**
 * Build derived (browsable) elements for each source element with relationships.
 * Surfaces own warehouse cols + joined dim cols via [SrcTable/REL_NAME/Col] refs.
 * Mirrors lookml.ts:buildDerivedElements; required for cross-view calc col placement.
 */
function buildDerivedElementsForOmni(elements: any[]): any[] {
  const derived: any[] = [];
  for (const srcEl of elements) {
    if (!srcEl.relationships?.length) continue;
    if (srcEl.source?.kind !== 'warehouse-table') continue;

    const srcPath = srcEl.source.path as string[];
    const srcTableName = srcEl.name || srcPath[srcPath.length - 1];

    const viewCols: Array<{ id: string; formula: string }> = [];
    const viewOrder: string[] = [];

    // Own warehouse columns (no name = physical pass-through; computed cols
    // with bare [Col] refs won't resolve here).
    for (const col of srcEl.columns ?? []) {
      if (!col.formula || col.formula.startsWith('/*')) continue;
      if (col.name) continue;
      const cId = sigmaShortId();
      viewCols.push({ id: cId, formula: col.formula });
      viewOrder.push(cId);
    }

    // Joined dim cols via [SrcTable/REL_NAME/Col] cross-element refs.
    for (const rel of srcEl.relationships ?? []) {
      if (!rel.name) continue;
      const tgtEl = elements.find(e => e.id === rel.targetElementId);
      if (!tgtEl || tgtEl.source?.kind !== 'warehouse-table') continue;
      for (const col of tgtEl.columns ?? []) {
        if (!col.formula || col.formula.startsWith('/*')) continue;
        if (col.name) continue;
        const fm = col.formula.match(/^\[([^\]]+)\]$/);
        if (!fm) continue;
        const inner = fm[1];
        const slashIdx = inner.lastIndexOf('/');
        const dispName = slashIdx >= 0 ? inner.slice(slashIdx + 1) : inner;
        const cId = sigmaShortId();
        viewCols.push({ id: cId, formula: `[${srcTableName}/${rel.name}/${dispName}]` });
        viewOrder.push(cId);
      }
    }

    if (viewCols.length > 0) {
      derived.push({
        id: sigmaShortId(),
        kind: 'table',
        name: `${srcTableName} View`,
        source: { kind: 'table', elementId: srcEl.id },
        columns: viewCols,
        order: viewOrder,
      });
    }
  }
  return derived;
}

// ── Formula translation ──────────────────────────────────────────────────────

const OMNI_FUNC_MAP: Record<string, string> = {
  MONTH: 'Month', YEAR: 'Year', DAY: 'Day', HOUR: 'Hour',
  QUARTER: 'Quarter', WEEK: 'Week', MINUTE: 'Minute', SECOND: 'Second',
  CONCAT: 'Concat', ROUND: 'Round', ABS: 'Abs',
  CEIL: 'Ceiling', CEILING: 'Ceiling', FLOOR: 'Floor',
  SQRT: 'Sqrt', POWER: 'Power', MOD: 'Mod', LOG: 'Log',
  COALESCE: 'Coalesce', NVL: 'Coalesce', IFNULL: 'Coalesce',
  NULLIF: 'Nullif', IFF: 'If', IIF: 'If',
  LOWER: 'Lower', UPPER: 'Upper', TRIM: 'Trim',
  LTRIM: 'Ltrim', RTRIM: 'Rtrim', LENGTH: 'Len', LEN: 'Len',
  LEFT: 'Left', RIGHT: 'Right',
  SUBSTR: 'Mid', SUBSTRING: 'Mid',
  REPLACE: 'Replace', SPLIT_PART: 'SplitPart',
  CONTAINS: 'Contains',
  DATEDIFF: 'DateDiff', DATE_DIFF: 'DateDiff',
  DATEADD: 'DateAdd', DATE_ADD: 'DateAdd',
  DATE_TRUNC: 'DateTrunc', TRUNC: 'DateTrunc',
  TO_DATE: 'Date', TO_CHAR: 'Text', TO_NUMBER: 'Number',
  GETDATE: 'Now', CURRENT_DATE: 'Today', CURRENT_TIMESTAMP: 'Now',
  // NOTE: REGEXP_LIKE / REGEXP_CONTAINS intentionally omitted — they emit a skip warning instead
};

function omniTranslateFormula(sql: string, tableName: string): string | null {
  if (!sql || typeof sql !== 'string') return null;
  let expr = sql.trim();

  // A user-written Omni `sql:` expression is real SQL and CAN contain
  // single-quoted string literals (e.g. `CASE WHEN due_label = 'When Due'
  // THEN 'On Time' ELSE 'Late' END`). Every pass below — field substitution,
  // ::TYPE casts, the IN-list splitter, CASE→If() lowering, and SQL
  // function-name mapping — is a regex scan or depth-walk that cannot tell
  // code from data. Left unmasked, the word "When" inside that literal reads
  // as a live WHEN keyword and corrupts the whole CASE (confirmed live: the
  // condition is destroyed and the emitted If() binds the wrong value).
  //
  // Mask every literal span ONCE, here, before any pass runs; every pass
  // below operates on the masked text; unmask at the very end, which is
  // also where a single-quoted SQL literal becomes Sigma's double-quoted
  // form (this replaces what used to be a separate, unmasked quote-
  // conversion pass).
  const { masked, lits } = maskOmniLiterals(expr);
  expr = masked;

  // 1. Field reference substitution
  // For Custom SQL elements Sigma uses bare [Display Name] refs (no table prefix).
  const isCustomSql = tableName === 'Custom SQL';
  expr = expr.replace(/\$\{TABLE\}\.(\w+)/g, (_, col) =>
    isCustomSql
      ? `[${sigmaDisplayName(col)}]`
      : `[${tableName}/${sigmaDisplayName(col)}]`
  );
  expr = expr.replace(/\$\{(\w+)\.(\w+)\}/g, (_, _v, field) =>
    `[${sigmaDisplayName(field)}]`
  );
  expr = expr.replace(/\$\{(\w+)\}/g, (_, field) =>
    `[${sigmaDisplayName(field)}]`
  );

  // 1b. Snowflake/SQL ::TYPE casts → Sigma type functions
  expr = expr.replace(/(\[[^\]]+\]|\w+)\s*::\s*(\w+)/gi, (_, val, typ) => {
    const t = typ.toUpperCase();
    if (t === 'DATE') return `Date(${val})`;
    if (t.startsWith('TIMESTAMP') || t === 'DATETIME') return `Datetime(${val})`;
    if (t === 'VARCHAR' || t === 'STRING' || t === 'TEXT' || t === 'CHAR') return `Text(${val})`;
    if (t === 'INTEGER' || t === 'INT' || t === 'BIGINT' || t === 'SMALLINT') return `Int(${val})`;
    if (t === 'FLOAT' || t === 'DOUBLE' || t === 'NUMERIC' || t === 'DECIMAL' || t === 'NUMBER') return `Number(${val})`;
    if (t === 'BOOLEAN') return `Boolean(${val})`;
    return val;
  });

  // 2. expr IN (a, b, c) → In(expr, a, b, c)
  expr = expr.replace(
    /(\w+(?:\([^)]*\))?|\[[^\]]+\])\s+IN\s+\(([^)]+)\)/gi,
    (_, lhs, items) => {
      const vals = items.split(',').map((v: string) => v.trim());
      return `In(${lhs}, ${vals.join(', ')})`;
    }
  );

  // 3. CASE WHEN … END → nested If()
  expr = sqlCaseToIf(expr);

  // 4. SQL function names → Sigma equivalents
  expr = expr.replace(/\b([A-Za-z_][A-Za-z0-9_]*)\s*(?=\()/g, (match, fn) => {
    const mapped = OMNI_FUNC_MAP[fn.toUpperCase()];
    return mapped ?? match;
  });

  return unmaskOmniLiterals(expr, lits);
}

// Masks every single-quoted string literal in `s` behind a sentinel
// ( <index> — no letters, so pass 4's identifier-followed-by-"("
// function-name scan never matches it, and the bare digits alone don't
// satisfy that regex's `[A-Za-z_]` leading-character requirement either) so
// every later regex/depth-walk pass sees data, not code, where a literal used
// to be. `_maskLiterals` in formulas.ts is the proven reference for this
// shape; reproduced locally here since that file has a different owner.
//
// A `[bracketed identifier]` span is treated as atomic: an apostrophe inside
// one (e.g. `[Manager's Approval]`) is part of the identifier, not a string-
// literal delimiter, so bracketed spans are skipped whole before the quote
// scan ever sees them. An unterminated `[` or `'` (no matching close anywhere
// in the rest of the string) is NOT treated as an opening delimiter — it's
// kept as an ordinary character and scanning continues — so a stray quote
// can never swallow the remainder of the expression.
const OMNI_LIT_RE = /'(?:[^']|'')*'/g;

function maskOmniLiterals(s: string): { masked: string; lits: string[] } {
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
      OMNI_LIT_RE.lastIndex = i;
      const m = OMNI_LIT_RE.exec(s);
      if (m && m.index === i) {
        out += ` ${lits.push(m[0]) - 1}`;
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
function unmaskOmniLiterals(s: string, lits: string[]): string {
  return s.replace(/ (\d+)/g, (_m, i) => {
    const inner = lits[Number(i)].slice(1, -1).replace(/''/g, "'").replace(/"/g, '\\"');
    return `"${inner}"`;
  });
}

function sqlCaseToIf(expr: string): string {
  let prev = '';
  let safety = 0;
  while (expr !== prev && safety++ < 20) {
    prev = expr;
    expr = expr.replace(
      /\bCASE\b((?:(?!\bCASE\b).)*?)\bEND\b/is,
      (_, body) => parseCaseBody(body)
    );
  }
  return expr;
}

function parseCaseBody(body: string): string {
  interface Part { kw: string | null; val: string | null }
  const parts: Part[] = [];
  let current = '';
  let depth    = 0;
  let i        = 0;
  const up     = body.toUpperCase();

  while (i < body.length) {
    if (body[i] === '(' || body[i] === '[') depth++;
    else if (body[i] === ')' || body[i] === ']') depth--;

    if (depth === 0) {
      let matched = false;
      for (const kw of ['WHEN', 'THEN', 'ELSE'] as const) {
        if (
          up.startsWith(kw, i) &&
          (i === 0 || !/[A-Z0-9_]/i.test(body[i - 1])) &&
          (i + kw.length >= body.length || !/[A-Z0-9_]/i.test(body[i + kw.length]))
        ) {
          parts.push({ kw: null, val: current.trim() });
          parts.push({ kw, val: null });
          current = '';
          i += kw.length;
          matched = true;
          break;
        }
      }
      if (matched) continue;
    }
    current += body[i];
    i++;
  }
  if (current.trim()) parts.push({ kw: null, val: current.trim() });

  const conditions: Array<{ cond: string; then: string }> = [];
  let elseVal = 'null';
  let pi = 0;

  while (pi < parts.length) {
    if (parts[pi].kw === 'WHEN') {
      const cond = (parts[pi + 1]?.val ?? '').trim();
      if (parts[pi + 2]?.kw === 'THEN') {
        const then = (parts[pi + 3]?.val ?? '').trim();
        conditions.push({ cond, then });
        pi += 4;
      } else { pi++; }
    } else if (parts[pi].kw === 'ELSE') {
      elseVal = (parts[pi + 1]?.val ?? 'null').trim();
      pi += 2;
    } else { pi++; }
  }

  let result = elseVal;
  for (let k = conditions.length - 1; k >= 0; k--) {
    result = `If(${conditions[k].cond}, ${conditions[k].then}, ${result})`;
  }
  return result;
}

// ── Internal types ───────────────────────────────────────────────────────────

interface OmniDimension {
  name: string;
  type?: string;
  sql?: string;
  label?: string;
  primary_key?: boolean;
  hidden?: boolean;
  timeframes?: string[];
  description?: string;
}

interface OmniMeasure {
  name: string;
  type?: string;
  sql?: string;
  label?: string;
  filters?: Array<{ field: string; value: string }>;
  description?: string;
}

interface OmniView {
  name: string;
  view?: string;
  sql_table_name?: string;
  derived_table?: { sql?: string };
  dimensions?: OmniDimension[];
  measures?: OmniMeasure[];
  label?: string;
}

interface OmniJoin {
  name: string;
  from?: string;
  type?: string;
  relationship?: string;
  sql_on?: string;
  foreign_key?: string;
  view_label?: string;
}

interface OmniExplore {
  name: string;
  from?: string;
  label?: string;
  joins?: OmniJoin[];
}

interface ViewEntry {
  elementId: string;
  pkColId: string | null;
  colIdMap: Record<string, string>;
  element: SigmaElement;
  sourceTable: string;
}
