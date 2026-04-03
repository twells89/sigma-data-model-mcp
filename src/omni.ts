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
      model: { name: 'Omni Analytics Model', pages: [{ id: sigmaShortId(), name: 'Page 1', elements: [] }] },
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
        name:   displayName,
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
      element = {
        id:     elementId,
        kind:   'table',
        name:   displayName,
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
        formula = (aggWrap[type] ?? ((e: string) => `Sum(${e})`))(metricExpr);
      } else {
        formula = sigmaAggFormula(type, name);
      }

      const metricId = sigmaInodeId(name.toUpperCase());
      colIdMap[name.toUpperCase()] = metricId;
      (element.metrics ??= []).push({
        id:      metricId,
        name:    measure.label ?? sigmaDisplayName(name),
        formula,
      });
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

      const rel: any = {
        id:               sigmaShortId(),
        targetElementId:  toEntry.elementId,
        name:             `${sigmaDisplayName(fromViewName)} to ${sigmaDisplayName(toViewName)}`,
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
  REGEXP_LIKE: 'RegexpMatch', REGEXP_CONTAINS: 'RegexpMatch',
};

function omniTranslateFormula(sql: string, tableName: string): string | null {
  if (!sql || typeof sql !== 'string') return null;
  let expr = sql.trim();

  // 1. Field reference substitution
  // For Custom SQL elements Sigma uses the raw SQL alias (uppercase), not a display name
  const isCustomSql = tableName === 'Custom SQL';
  expr = expr.replace(/\$\{TABLE\}\.(\w+)/g, (_, col) =>
    isCustomSql
      ? `[Custom SQL/${col.toUpperCase()}]`
      : `[${tableName}/${sigmaDisplayName(col)}]`
  );
  expr = expr.replace(/\$\{(\w+)\.(\w+)\}/g, (_, _v, field) =>
    `[${sigmaDisplayName(field)}]`
  );
  expr = expr.replace(/\$\{(\w+)\}/g, (_, field) =>
    `[${sigmaDisplayName(field)}]`
  );

  // 2. Single-quoted strings → double-quoted
  expr = expr.replace(/'([^']*)'/g, '"$1"');

  // 3. expr IN (a, b, c) → (expr = a Or expr = b Or expr = c)
  expr = expr.replace(
    /(\w+(?:\([^)]*\))?|\[[^\]]+\])\s+IN\s+\(([^)]+)\)/gi,
    (_, lhs, items) => {
      const vals = items.split(',').map((v: string) => v.trim());
      return '(' + vals.map((v: string) => `${lhs} = ${v}`).join(' Or ') + ')';
    }
  );

  // 4. CASE WHEN … END → nested If()
  expr = sqlCaseToIf(expr);

  // 5. SQL function names → Sigma equivalents
  expr = expr.replace(/\b([A-Za-z_][A-Za-z0-9_]*)\s*(?=\()/g, (match, fn) => {
    const mapped = OMNI_FUNC_MAP[fn.toUpperCase()];
    return mapped ?? match;
  });

  return expr;
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
