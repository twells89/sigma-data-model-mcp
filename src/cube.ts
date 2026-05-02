/**
 * Cube.dev → Sigma Data Model JSON converter.
 *
 * Accepts Cube schema files in YAML (.yml, .yaml) and JavaScript (.js) form.
 * Handles cubes, dimensions (string/number/time/boolean), measures
 * (count/sum/avg/min/max/count_distinct/number/percent), joins
 * (one_to_one/one_to_many/many_to_one), and views with cubes[].includes.
 *
 * Pre-aggregations and segments are skipped with warnings.
 */

import yaml from 'js-yaml';
import {
  resetIds, sigmaShortId, sigmaInodeId, sigmaDisplayName,
  sigmaColFormula, sigmaAggFormula,
  type SigmaElement, type ConversionResult,
} from './sigma-ids.js';

// ── Public interface ─────────────────────────────────────────────────────────

export interface CubeFile {
  name: string;
  content: string;
}

export interface CubeConvertOptions {
  connectionId?: string;
  database?: string;
  schema?: string;
}

export function convertCubeToSigma(
  files: CubeFile[],
  options: CubeConvertOptions = {},
): ConversionResult {
  resetIds();

  const { connectionId = '<CONNECTION_ID>', database = '', schema = '' } = options;
  const dbOverride = database.trim().toUpperCase();
  const schemaOverride = schema.trim().toUpperCase();

  const cubes: CubeDef[] = [];
  const views: CubeView[] = [];
  const warnings: string[] = [];
  let preAggCount = 0;
  let segmentCount = 0;

  for (const file of files) {
    try {
      const lower = file.name.toLowerCase();
      const isJs = lower.endsWith('.js') || lower.endsWith('.cjs') || lower.endsWith('.mjs');
      const parsed = isJs ? parseCubeJs(file.content) : parseCubeYaml(file.content);
      cubes.push(...parsed.cubes);
      views.push(...parsed.views);
      preAggCount += parsed.preAggCount;
      segmentCount += parsed.segmentCount;
    } catch (e: any) {
      warnings.push(`${file.name}: parse error — ${e.message}`);
    }
  }

  if (cubes.length === 0 && views.length === 0) {
    return {
      model: { name: 'Cube Model', schemaVersion: 1, pages: [{ id: sigmaShortId(), name: 'Page 1', elements: [] }] },
      warnings: warnings.length ? warnings : ['No cubes or views found in the provided files'],
      stats: {},
    };
  }

  if (preAggCount > 0) warnings.push(`ℹ ${preAggCount} pre_aggregation${preAggCount !== 1 ? 's' : ''} skipped — Sigma uses warehouse-side caching, so these have no equivalent.`);
  if (segmentCount > 0) warnings.push(`ℹ ${segmentCount} segment${segmentCount !== 1 ? 's' : ''} skipped — Sigma has no direct equivalent for reusable segment filters.`);

  // ── Build elements from cubes ───────────────────────────────────────────────

  const elements: SigmaElement[] = [];
  const cubeRegistry = new Map<string, CubeRegistryEntry>();
  let totalDims = 0;
  let totalMeasures = 0;
  let totalCalcDims = 0;
  let totalCalcMeasures = 0;

  for (const cube of cubes) {
    const cubeName = cube.name;
    const displayName = sigmaDisplayName(cubeName);
    const elementId = sigmaShortId();

    let element: SigmaElement;
    let sourceTable: string;
    let isCustomSql = false;

    if (cube.sql_table) {
      const raw = cube.sql_table.replace(/"/g, '').trim();
      let path = raw.split('.').map(s => s.trim().toUpperCase()).filter(Boolean);
      if (path.length === 1) {
        if (dbOverride && schemaOverride) path = [dbOverride, schemaOverride, path[0]];
        else if (schemaOverride) path = [schemaOverride, path[0]];
        else if (dbOverride) path = [dbOverride, path[0]];
      } else if (path.length === 2 && dbOverride) {
        path = [dbOverride, path[0], path[1]];
      }
      sourceTable = path[path.length - 1] || cubeName.toUpperCase();
      element = {
        id: elementId,
        kind: 'table',
        name: displayName,
        source: { connectionId, kind: 'warehouse-table', path },
        columns: [],
        metrics: [],
        order: [],
      };
    } else if (cube.sql) {
      // Raw SQL → Custom SQL element. Translate ${CUBE}/${other.dim} refs first.
      isCustomSql = true;
      sourceTable = 'Custom SQL';
      const translatedSql = translateCubeSql(cube.sql, cubeName, true);
      element = {
        id: elementId,
        kind: 'table',
        source: { connectionId, kind: 'sql', statement: translatedSql },
        columns: [],
        metrics: [],
        order: [],
      };
      warnings.push(`ℹ "${cubeName}" → Custom SQL element. Review the SQL before saving.`);
    } else {
      // No source — assume table name from cube name
      const path = dbOverride && schemaOverride
        ? [dbOverride, schemaOverride, cubeName.toUpperCase()]
        : [cubeName.toUpperCase()];
      sourceTable = path[path.length - 1];
      element = {
        id: elementId,
        kind: 'table',
        name: displayName,
        source: { connectionId, kind: 'warehouse-table', path },
        columns: [],
        metrics: [],
        order: [],
      };
      warnings.push(`⚠ "${cubeName}" has no sql_table or sql — assuming table named ${sourceTable}.`);
    }

    const colIdMap: Record<string, string> = {};
    let pkColId: string | null = null;
    // For Custom SQL elements: track (physical column → display name) pairs so
    // we can wrap the user's SQL with aliasing afterward and rewrite formulas
    // to the qualified `[Custom SQL/Display]` form. Bare `[Display]` refs do
    // not resolve at query time when the SQL output uses snake_case names.
    const customSqlPassthroughs: Array<{ phys: string; display: string }> = [];

    function addCol(fieldName: string, formula: string, label?: string): string {
      const id = isCustomSql ? sigmaShortId() : sigmaInodeId(fieldName.toUpperCase());
      colIdMap[fieldName.toUpperCase()] = id;
      const col: any = { id, formula };
      if (label) col.name = label;
      element.columns.push(col);
      element.order.push(id);
      return id;
    }

    // Dimensions → columns
    for (const dim of cube.dimensions || []) {
      const dimName = dim.name;
      if (!dim.shown && dim.shown === false) continue;
      totalDims++;

      const isSimple = isSimpleColumnRef(dim.sql, cubeName);
      let formula: string;

      if (isSimple) {
        // ${CUBE}.col or bare col → passthrough
        const physCol = extractColumnName(dim.sql, cubeName) || dimName;
        formula = isCustomSql
          ? `[${sigmaDisplayName(physCol)}]`
          : sigmaColFormula(sourceTable, physCol);
        if (isCustomSql) customSqlPassthroughs.push({ phys: physCol, display: sigmaDisplayName(physCol) });
      } else if (dim.sql) {
        const translated = translateCubeFormula(dim.sql, cubeName, sourceTable, isCustomSql);
        if (translated === null) {
          warnings.push(`⚠ "${cubeName}.${dimName}": SQL could not be translated — skipped. Add manually in Sigma. SQL: ${dim.sql.slice(0, 80)}`);
          totalDims--;
          continue;
        }
        formula = translated;
        totalCalcDims++;
      } else {
        // No sql → use dimension name as physical column
        formula = isCustomSql
          ? `[${sigmaDisplayName(dimName)}]`
          : sigmaColFormula(sourceTable, dimName);
        if (isCustomSql) customSqlPassthroughs.push({ phys: dimName, display: sigmaDisplayName(dimName) });
      }

      const label = dim.title || undefined;
      const colId = addCol(dimName, formula, label);
      if (dim.primary_key) pkColId = colId;
    }

    // Measures → metrics
    for (const measure of cube.measures || []) {
      const measureName = measure.name;
      if (!measureName) continue;
      totalMeasures++;

      const type = (measure.type || 'count').toLowerCase();
      let formula: string;

      if (type === 'count' && !measure.sql) {
        // Bare count — count of rows. Use PK if available.
        if (pkColId) {
          const pkKey = Object.keys(colIdMap).find(k => colIdMap[k] === pkColId) || measureName;
          formula = `CountIf(IsNotNull([${sigmaDisplayName(pkKey)}]))`;
        } else if (element.columns[0]) {
          const firstColKey = Object.keys(colIdMap)[0] || measureName;
          formula = `CountIf(IsNotNull([${sigmaDisplayName(firstColKey)}]))`;
        } else {
          formula = `CountIf(IsNotNull([${sigmaDisplayName(measureName)}]))`;
        }
      } else if (type === 'number' || type === 'percent') {
        // Calculated measure — sql references other measures via ${measure_name}
        const translated = measure.sql
          ? translateCubeMeasureExpr(measure.sql, cubeName, cube)
          : null;
        if (translated === null) {
          warnings.push(`⚠ "${cubeName}.${measureName}": calculated measure SQL could not be translated — skipped.`);
          totalMeasures--;
          continue;
        }
        formula = translated;
        totalCalcMeasures++;
      } else if (measure.sql) {
        // Standard aggregate over a column. sql may be a column ref or expression.
        // Ensure any ${CUBE}.col refs exist on the element (auto-add hidden passthroughs)
        ensureFilterColumns(measure.sql, element, colIdMap, sourceTable, isCustomSql, addCol);
        const isCol = isSimpleColumnRef(measure.sql, cubeName);
        let inner: string;
        if (isCol) {
          const physCol = extractColumnName(measure.sql, cubeName) || measureName;
          inner = `[${sigmaDisplayName(physCol)}]`;
        } else {
          const translated = translateCubeFormula(measure.sql, cubeName, sourceTable, isCustomSql);
          // Strip [TABLE/col] → [col] for metric context
          inner = (translated || measure.sql).replace(/\[([^/\]]+)\/([^\]]+)\]/g, '[$2]');
        }
        formula = wrapAggregate(type, inner);

        // Apply filters if present
        if (measure.filters && measure.filters.length > 0) {
          // Auto-add columns referenced by the filter SQL — otherwise the filter resolves to
          // an unknown column and the metric returns null.
          for (const f of measure.filters) ensureFilterColumns(f.sql, element, colIdMap, sourceTable, isCustomSql, addCol);
          const condParts = measure.filters
            .map(f => translateCubeFormula(f.sql, cubeName, sourceTable, isCustomSql))
            .filter(Boolean) as string[];
          if (condParts.length > 0) {
            const cond = condParts.length === 1 ? condParts[0] : condParts.map(c => `(${c})`).join(' And ');
            const condStripped = cond.replace(/\[([^/\]]+)\/([^\]]+)\]/g, '[$2]');
            formula = wrapAggregateIf(type, inner, condStripped);
          }
        }
      } else {
        // count_distinct etc with no sql
        formula = sigmaAggFormula(type, measureName);
      }

      const metricId = sigmaShortId();
      colIdMap[measureName.toUpperCase()] = metricId;
      (element.metrics ??= []).push({
        id: metricId,
        name: measure.title || sigmaDisplayName(measureName),
        formula,
      });
    }

    // For Custom SQL elements: wrap the user's SQL with an outer SELECT that
    // aliases each passthrough column to its display name, and rewrite each
    // bare `[Display]` formula to qualified `[Custom SQL/Display]` form.
    if (isCustomSql && customSqlPassthroughs.length) {
      const rawSql = String(element.source.statement || '').trim();
      if (rawSql) {
        // Dedupe in case multiple dims share the same physical column
        const seen = new Set<string>();
        const uniq = customSqlPassthroughs.filter(p => {
          if (seen.has(p.phys)) return false;
          seen.add(p.phys);
          return true;
        });
        const aliasList = uniq.map(p => `"${p.phys.toUpperCase()}" AS "${p.display}"`).join(', ');
        element.source.statement = `SELECT ${aliasList}\nFROM (\n${rawSql}\n) AS _src`;
        for (const col of element.columns) {
          const m = (col.formula || '').match(/^\[([^\/\]]+)\]$/);
          if (m) col.formula = `[Custom SQL/${m[1]}]`;
        }
      }
    }

    elements.push(element);
    cubeRegistry.set(cubeName.toLowerCase(), {
      elementId, pkColId, colIdMap, element, sourceTable, cubeDef: cube, isCustomSql,
    });
  }

  // ── Wire joins → relationships ──────────────────────────────────────────────

  let totalRels = 0;
  for (const cube of cubes) {
    const fromEntry = cubeRegistry.get(cube.name.toLowerCase());
    if (!fromEntry) continue;

    for (const join of cube.joins || []) {
      const toName = join.name.toLowerCase();
      const toEntry = cubeRegistry.get(toName);
      if (!toEntry) {
        warnings.push(`Join "${cube.name} → ${join.name}": target cube not found — skipping`);
        continue;
      }

      // Parse sql: ${CUBE}.fk = ${OtherCube.pk}  (or full match in either direction)
      let srcColId: string | null = null;
      let tgtColId: string | null = null;
      const m = join.sql && join.sql.match(/\$\{(\w+)\}?\.?(\w+)?\s*=\s*\$\{(\w+)\}?\.?(\w+)?/);
      // Full pattern: ${CUBE}.col1 = ${OtherCube.col2} OR ${CUBE}.col1 = ${OtherCube}.col2
      if (join.sql) {
        const refs = parseJoinSqlRefs(join.sql, cube.name, join.name);
        if (refs) {
          srcColId = fromEntry.colIdMap[refs.fromCol.toUpperCase()] ?? null;
          tgtColId = toEntry.colIdMap[refs.toCol.toUpperCase()] ?? null;
          // Auto-add the source column if it's a real warehouse column not yet in the element
          if (!srcColId && fromEntry.element.source?.kind === 'warehouse-table') {
            const newId = sigmaInodeId(refs.fromCol.toUpperCase());
            fromEntry.colIdMap[refs.fromCol.toUpperCase()] = newId;
            fromEntry.element.columns.push({
              id: newId,
              formula: sigmaColFormula(fromEntry.sourceTable, refs.fromCol),
            });
            fromEntry.element.order.push(newId);
            srcColId = newId;
          }
          if (!tgtColId && toEntry.element.source?.kind === 'warehouse-table') {
            const newId = sigmaInodeId(refs.toCol.toUpperCase());
            toEntry.colIdMap[refs.toCol.toUpperCase()] = newId;
            toEntry.element.columns.push({
              id: newId,
              formula: sigmaColFormula(toEntry.sourceTable, refs.toCol),
            });
            toEntry.element.order.push(newId);
            tgtColId = newId;
          }
        }
      }

      const relType = join.relationship === 'one_to_many' ? '1:N'
        : join.relationship === 'one_to_one' ? '1:1'
        : 'N:1';

      const rel: any = {
        id: sigmaShortId(),
        targetElementId: toEntry.elementId,
        name: `${sigmaDisplayName(cube.name)} to ${sigmaDisplayName(join.name)}`,
        relationshipType: relType,
      };
      if (srcColId && tgtColId) {
        rel.keys = [{ sourceColumnId: srcColId, targetColumnId: tgtColId }];
      } else {
        warnings.push(`Join "${cube.name} → ${join.name}": could not resolve column keys from sql=${join.sql || '(missing)'} — relationship added without key mapping`);
      }
      (fromEntry.element.relationships ??= []).push(rel);
      totalRels++;
    }
  }

  // ── Build view elements ─────────────────────────────────────────────────────

  let viewElementCount = 0;
  for (const view of views) {
    const viewElement = buildViewElement(view, cubeRegistry, warnings);
    if (viewElement) {
      elements.push(viewElement);
      viewElementCount++;
    }
  }

  // Clean up empty arrays
  for (const el of elements) {
    if (el.metrics?.length === 0) delete el.metrics;
    if (el.relationships?.length === 0) delete el.relationships;
  }

  if (!options.connectionId) warnings.unshift('⚠ Connection ID not set — update in JSON before saving to Sigma');

  const modelName = cubes.length === 1 && views.length === 0
    ? sigmaDisplayName(cubes[0].name)
    : 'Cube Model';

  return {
    model: {
      name: modelName,
      schemaVersion: 1,
      pages: [{ id: sigmaShortId(), name: 'Page 1', elements }],
    },
    warnings,
    stats: {
      cubes: cubes.length,
      views: views.length,
      elements: elements.length,
      columns: elements.reduce((s, e) => s + (e.columns?.length ?? 0), 0),
      metrics: elements.reduce((s, e) => s + (e.metrics?.length ?? 0), 0),
      relationships: totalRels,
      viewElements: viewElementCount,
      totalDims,
      totalMeasures,
      calculatedDims: totalCalcDims,
      calculatedMeasures: totalCalcMeasures,
      preAggregationsSkipped: preAggCount,
      segmentsSkipped: segmentCount,
    },
  };
}

// ── View → derived element ───────────────────────────────────────────────────

function buildViewElement(
  view: CubeView,
  cubeRegistry: Map<string, CubeRegistryEntry>,
  warnings: string[],
): SigmaElement | null {
  const viewName = view.name;
  const displayName = sigmaDisplayName(viewName);

  // Find the head cube (first entry's join_path root)
  const cubeEntries = view.cubes || [];
  if (cubeEntries.length === 0) {
    warnings.push(`⚠ View "${viewName}" has no cubes[] — skipped.`);
    return null;
  }

  const headPath = cubeEntries[0].join_path || '';
  const headCubeName = headPath.split('.')[0];
  const headEntry = cubeRegistry.get(headCubeName.toLowerCase());
  if (!headEntry) {
    warnings.push(`⚠ View "${viewName}": head cube "${headCubeName}" not found — skipped.`);
    return null;
  }

  const elementId = sigmaShortId();
  const element: SigmaElement = {
    id: elementId,
    kind: 'table',
    name: displayName,
    source: { kind: 'table', elementId: headEntry.elementId },
    columns: [],
    metrics: [],
    order: [],
  };

  for (const entry of cubeEntries) {
    const path = (entry.join_path || '').split('.').filter(Boolean);
    if (path.length === 0) continue;

    // Determine the target cube and the relationship-name path for cross-element refs.
    const cubePath = path.map(p => cubeRegistry.get(p.toLowerCase())).filter(Boolean) as CubeRegistryEntry[];
    if (cubePath.length === 0) {
      warnings.push(`⚠ View "${viewName}": join_path "${entry.join_path}" — no cubes found, skipped.`);
      continue;
    }
    const targetEntry = cubePath[cubePath.length - 1];

    // Determine which dims/measures to include
    const includes = entry.includes;
    const excludes = new Set((entry.excludes || []).map(s => s.toLowerCase()));

    const dimsToInclude: CubeDimension[] = [];
    const measuresToInclude: CubeMeasure[] = [];

    if (includes === '*' || includes === undefined) {
      dimsToInclude.push(...(targetEntry.cubeDef.dimensions || []));
      measuresToInclude.push(...(targetEntry.cubeDef.measures || []));
    } else if (Array.isArray(includes)) {
      const incLower = new Set(includes.map(i => (typeof i === 'string' ? i.toLowerCase() : '')));
      for (const d of targetEntry.cubeDef.dimensions || []) {
        if (incLower.has(d.name.toLowerCase())) dimsToInclude.push(d);
      }
      for (const m of targetEntry.cubeDef.measures || []) {
        if (incLower.has(m.name.toLowerCase())) measuresToInclude.push(m);
      }
    }

    const prefix = entry.prefix === true ? `${path[path.length - 1]}_`
      : (typeof entry.prefix === 'string' ? entry.prefix : '');

    for (const d of dimsToInclude) {
      if (excludes.has(d.name.toLowerCase())) continue;
      const dispName = prefix ? sigmaDisplayName(prefix + d.name) : sigmaDisplayName(d.name);
      const refFormula = buildViewColumnRef(path, d.name, headEntry, cubeRegistry);
      if (!refFormula) continue;
      element.columns.push({
        id: sigmaShortId(),
        formula: refFormula,
        name: dispName,
      });
      element.order.push(element.columns[element.columns.length - 1].id);
    }
    for (const m of measuresToInclude) {
      if (excludes.has(m.name.toLowerCase())) continue;
      // Measures from joined cubes can't be re-bound on the view without cross-element
      // metric refs (which Sigma doesn't support). Surface a warning and skip.
      if (path.length > 1) {
        warnings.push(`ℹ View "${viewName}": measure "${m.name}" on joined cube "${targetEntry.cubeDef.name}" skipped — add manually in Sigma UI as a linked metric.`);
        continue;
      }
      // Head-cube measures: copy the metric's formula onto the view. The formula
      // references columns by display name, which resolve via the view's passthrough columns.
      const dispName = prefix ? sigmaDisplayName(prefix + m.name) : (m.title || sigmaDisplayName(m.name));
      const sourceMetric = (targetEntry.element.metrics || []).find(mm => mm.name === (m.title || sigmaDisplayName(m.name)));
      if (!sourceMetric) continue;
      (element.metrics ??= []).push({
        id: sigmaShortId(),
        name: dispName,
        formula: sourceMetric.formula,
      });
    }
  }

  if (element.columns.length === 0 && (element.metrics?.length ?? 0) === 0) {
    warnings.push(`⚠ View "${viewName}" produced no columns or metrics — check cubes[].includes and join_path.`);
    return null;
  }
  if (element.metrics?.length === 0) delete element.metrics;
  return element;
}

// Build the formula referencing a field on a (possibly remote) cube via its head cube.
// Single-cube path: use the head cube's element directly via [HeadCubeElement.Field] form.
// Multi-cube path: walk the head's relationships and emit a linked-column ref.
function buildViewColumnRef(
  path: string[],
  fieldName: string,
  headEntry: CubeRegistryEntry,
  cubeRegistry: Map<string, CubeRegistryEntry>,
): string | null {
  const dispField = sigmaDisplayName(fieldName);

  if (path.length === 1) {
    // Field is on the head cube itself
    return `[${headEntry.element.name || sigmaDisplayName(headEntry.cubeDef.name)}/${dispField}]`;
  }

  // Walk through joins to build a linked-column path.
  // Each hop uses [Source/FK_COL - link/target] form.
  let currentEntry = headEntry;
  const linkSegments: string[] = [];
  for (let i = 1; i < path.length; i++) {
    const nextName = path[i];
    const join = currentEntry.cubeDef.joins?.find(j => j.name.toLowerCase() === nextName.toLowerCase());
    if (!join) return null;
    const refs = parseJoinSqlRefs(join.sql, currentEntry.cubeDef.name, join.name);
    if (!refs) return null;
    const fkCol = sigmaDisplayName(refs.fromCol);
    if (i === 1) {
      const headTable = currentEntry.sourceTable;
      linkSegments.push(`${headTable}/${fkCol.toUpperCase()} - link`);
    } else {
      linkSegments.push(`${fkCol} - link`);
    }
    const next = cubeRegistry.get(nextName.toLowerCase());
    if (!next) return null;
    currentEntry = next;
  }
  return `[${linkSegments.join('/')}/${dispField}]`;
}

/**
 * Walk a SQL string for ${CUBE}.col / ${CUBE.col} refs and ensure each referenced
 * column exists on the element. Used for measure filters and measure expressions
 * that reference columns the user did not declare as dimensions — without this,
 * Sigma would resolve `[Is Active]` to nothing and the metric returns null.
 */
function ensureFilterColumns(
  sql: string | undefined,
  _element: SigmaElement,
  colIdMap: Record<string, string>,
  _sourceTable: string,
  _isCustomSql: boolean,
  addCol: (name: string, formula: string) => string,
): void {
  if (!sql) return;
  const cols = new Set<string>();
  for (const m of sql.matchAll(/\$\{CUBE\}\.(\w+)/g))    cols.add(m[1].toUpperCase());
  for (const m of sql.matchAll(/\$\{CUBE\.(\w+)\}/g))    cols.add(m[1].toUpperCase());
  for (const col of cols) {
    if (colIdMap[col]) continue;
    const formula = _isCustomSql
      ? `[${sigmaDisplayName(col)}]`
      : sigmaColFormula(_sourceTable, col);
    addCol(col, formula);
  }
}

// ── Cube formula translation ─────────────────────────────────────────────────

const CUBE_FUNC_MAP: Record<string, string> = {
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
  IS_NULL: 'IsNull', IS_NOT_NULL: 'IsNotNull',
  NOT: 'Not', AND: 'And', OR: 'Or',
  SUM: 'Sum', COUNT: 'Count', AVG: 'Avg', MIN: 'Min', MAX: 'Max',
};

/**
 * Returns true when the dim/measure sql is a bare column reference:
 *   "amount", "${CUBE}.amount", "${CUBE.amount}", or just "amount" (legacy)
 */
function isSimpleColumnRef(sql: string | undefined, _cubeName: string): boolean {
  if (!sql) return false;
  const s = sql.trim();
  if (/^\$\{CUBE\}\.\w+$/.test(s)) return true;
  if (/^\$\{CUBE\.\w+\}$/.test(s)) return true;
  if (/^\w+$/.test(s)) return true;
  return false;
}

function extractColumnName(sql: string | undefined, _cubeName: string): string | null {
  if (!sql) return null;
  const s = sql.trim();
  let m = s.match(/^\$\{CUBE\}\.(\w+)$/);
  if (m) return m[1];
  m = s.match(/^\$\{CUBE\.(\w+)\}$/);
  if (m) return m[1];
  m = s.match(/^(\w+)$/);
  if (m) return m[1];
  return null;
}

/**
 * Translate a Cube SQL expression to a Sigma formula.
 * Handles ${CUBE}.col, ${OtherCube.col}, ${OtherCube}.col, ${field_name}.
 * Uses a function map and CASE→If conversion shared with Omni.
 */
function translateCubeFormula(
  sql: string,
  cubeName: string,
  tableName: string,
  isCustomSql: boolean,
): string | null {
  if (!sql || typeof sql !== 'string') return null;
  let expr = sql.trim();

  // 1. ${CUBE}.col → table-qualified ref
  expr = expr.replace(/\$\{CUBE\}\.(\w+)/g, (_, col) =>
    isCustomSql ? `[${sigmaDisplayName(col)}]` : `[${tableName}/${sigmaDisplayName(col)}]`
  );
  // 1b. ${CUBE.col} (alternate inline form)
  expr = expr.replace(/\$\{CUBE\.(\w+)\}/g, (_, col) =>
    isCustomSql ? `[${sigmaDisplayName(col)}]` : `[${tableName}/${sigmaDisplayName(col)}]`
  );
  // 2. ${OtherCube.field} or ${OtherCube}.field → linked-column placeholder.
  //    For dim/measure context, calc-column formulas can't easily reach across joins, so emit
  //    the relative form [Field] and rely on Sigma's display-name resolution at the view level.
  expr = expr.replace(/\$\{(\w+)\.(\w+)\}/g, (_, _other, field) =>
    `[${sigmaDisplayName(field)}]`
  );
  expr = expr.replace(/\$\{(\w+)\}\.(\w+)/g, (_, _other, field) =>
    `[${sigmaDisplayName(field)}]`
  );
  // 3. ${field_name} (intra-cube measure ref) → [Display Field]
  expr = expr.replace(/\$\{(\w+)\}/g, (_, field) =>
    `[${sigmaDisplayName(field)}]`
  );

  // 4. ::TYPE casts → Sigma type funcs
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

  // 5. SQL string concat ' || ' → Sigma '&'
  expr = expr.replace(/\s*\|\|\s*/g, ' & ');

  // 6. Single-quoted strings → double-quoted
  expr = expr.replace(/'([^']*)'/g, '"$1"');

  // 7. expr IN (a, b, c) → In(expr, a, b, c)
  expr = expr.replace(
    /(\w+(?:\([^)]*\))?|\[[^\]]+\])\s+IN\s+\(([^)]+)\)/gi,
    (_, lhs, items) => {
      const vals = items.split(',').map((v: string) => v.trim());
      return `In(${lhs}, ${vals.join(', ')})`;
    }
  );

  // 8. CASE WHEN … END → nested If()
  expr = sqlCaseToIf(expr);

  // 9. SQL function names → Sigma equivalents
  expr = expr.replace(/\b([A-Za-z_][A-Za-z0-9_]*)\s*(?=\()/g, (match, fn) => {
    return CUBE_FUNC_MAP[fn.toUpperCase()] ?? match;
  });

  return expr;
}

/**
 * For Custom SQL element bodies — keep ${CUBE}.col → col (bare), not Sigma syntax,
 * since the SQL gets sent to the warehouse verbatim.
 */
function translateCubeSql(sql: string, cubeName: string, _isCustomSql: boolean): string {
  // For Custom SQL element source, we strip Cube template refs to produce plain SQL.
  // ${CUBE} → cubeName alias (so user can adjust); ${OtherCube} also normalized.
  let s = sql;
  s = s.replace(/\$\{CUBE\}/g, cubeName);
  s = s.replace(/\$\{CUBE\.(\w+)\}/g, `${cubeName}.$1`);
  s = s.replace(/\$\{(\w+)\.(\w+)\}/g, '$1.$2');
  s = s.replace(/\$\{(\w+)\}/g, '$1');
  return s;
}

/**
 * Translate a calculated-measure SQL like "${revenue} / NULLIF(${count}, 0)"
 * into a Sigma metric expression where ${m} → [Display Name of m].
 */
function translateCubeMeasureExpr(
  sql: string,
  _cubeName: string,
  _cube: CubeDef,
): string | null {
  let expr = sql.trim();
  expr = expr.replace(/\$\{CUBE\}\.(\w+)/g, (_, col) => `[${sigmaDisplayName(col)}]`);
  expr = expr.replace(/\$\{CUBE\.(\w+)\}/g, (_, col) => `[${sigmaDisplayName(col)}]`);
  expr = expr.replace(/\$\{(\w+)\.(\w+)\}/g, (_, _other, field) => `[${sigmaDisplayName(field)}]`);
  expr = expr.replace(/\$\{(\w+)\}\.(\w+)/g, (_, _other, field) => `[${sigmaDisplayName(field)}]`);
  expr = expr.replace(/\$\{(\w+)\}/g, (_, m) => `[${sigmaDisplayName(m)}]`);
  expr = expr.replace(/'([^']*)'/g, '"$1"');
  expr = sqlCaseToIf(expr);
  expr = expr.replace(/\b([A-Za-z_][A-Za-z0-9_]*)\s*(?=\()/g, (match, fn) =>
    CUBE_FUNC_MAP[fn.toUpperCase()] ?? match
  );
  return expr;
}

function wrapAggregate(type: string, inner: string): string {
  const map: Record<string, (e: string) => string> = {
    sum: e => `Sum(${e})`,
    avg: e => `Avg(${e})`,
    average: e => `Avg(${e})`,
    min: e => `Min(${e})`,
    max: e => `Max(${e})`,
    count: e => `CountIf(IsNotNull(${e}))`,
    count_distinct: e => `CountDistinct(${e})`,
    count_distinct_approx: e => `CountDistinct(${e})`,
    median: e => `Median(${e})`,
    sum_boolean: e => `CountIf(${e})`,
    runningtotal: e => `Sum(${e})`,
  };
  return (map[type] ?? ((e: string) => `Sum(${e})`))(inner);
}

function wrapAggregateIf(type: string, inner: string, cond: string): string {
  const map: Record<string, (e: string, c: string) => string> = {
    sum: (e, c) => `SumIf(${e}, ${c})`,
    avg: (e, c) => `AvgIf(${e}, ${c})`,
    average: (e, c) => `AvgIf(${e}, ${c})`,
    min: (e, c) => `MinIf(${e}, ${c})`,
    max: (e, c) => `MaxIf(${e}, ${c})`,
    count: (_e, c) => `CountIf(${c})`,
    count_distinct: (e, c) => `CountDistinctIf(${e}, ${c})`,
    count_distinct_approx: (e, c) => `CountDistinctIf(${e}, ${c})`,
  };
  return (map[type] ?? ((e: string, c: string) => `SumIf(${e}, ${c})`))(inner, cond);
}

// CASE WHEN converter — same shape as Omni's
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
  let depth = 0;
  let i = 0;
  const up = body.toUpperCase();
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

  const conditions: { cond: string; then: string }[] = [];
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

// ── Join SQL parser ──────────────────────────────────────────────────────────

/**
 * Parse "${CUBE}.fk = ${OtherCube.pk}" or "${CUBE}.fk = ${OtherCube}.pk"
 * (also reversed). Returns the column on the FROM cube and the column on the TO cube.
 */
function parseJoinSqlRefs(
  sql: string | undefined,
  fromCube: string,
  toCube: string,
): { fromCol: string; toCol: string } | null {
  if (!sql) return null;
  // Patterns of {ref}.{col} we need to extract:
  //   ${CUBE}.col          → cube=CUBE col=col
  //   ${name.col}          → cube=name col=col
  //   ${name}.col          → cube=name col=col
  const refPattern = /\$\{(\w+)(?:\.(\w+))?\}(?:\.(\w+))?/g;
  const refs: { cube: string; col: string }[] = [];
  let m: RegExpExecArray | null;
  while ((m = refPattern.exec(sql)) !== null) {
    const inner = m[1];
    const innerCol = m[2];
    const outerCol = m[3];
    const col = innerCol || outerCol;
    if (!col) continue;
    refs.push({ cube: inner, col });
  }
  if (refs.length < 2) return null;

  // Identify which ref belongs to fromCube vs toCube
  const fromLower = fromCube.toLowerCase();
  const toLower = toCube.toLowerCase();
  let fromRef: { cube: string; col: string } | null = null;
  let toRef: { cube: string; col: string } | null = null;
  for (const r of refs) {
    const cl = r.cube.toLowerCase();
    if (cl === 'cube' || cl === fromLower) fromRef ??= r;
    else if (cl === toLower) toRef ??= r;
  }
  if (!fromRef || !toRef) {
    // Fallback: assume order — first ref is from, second is to
    fromRef = refs[0];
    toRef = refs[1];
  }
  return { fromCol: fromRef.col, toCol: toRef.col };
}

// ── YAML parser ──────────────────────────────────────────────────────────────

function parseCubeYaml(text: string): ParsedFile {
  const out: ParsedFile = { cubes: [], views: [], preAggCount: 0, segmentCount: 0 };
  const docs: any[] = [];
  yaml.loadAll(text, (d) => { if (d) docs.push(d); });
  for (const doc of docs) {
    if (Array.isArray(doc.cubes)) {
      for (const c of doc.cubes) {
        const norm = normalizeCubeFromYaml(c);
        out.preAggCount += (c.pre_aggregations?.length || 0);
        out.segmentCount += (c.segments?.length || 0);
        out.cubes.push(norm);
      }
    }
    if (Array.isArray(doc.views)) {
      for (const v of doc.views) {
        out.views.push(normalizeViewFromYaml(v));
      }
    }
  }
  return out;
}

function normalizeCubeFromYaml(c: any): CubeDef {
  return {
    name: String(c.name || ''),
    sql_table: c.sql_table ? String(c.sql_table) : undefined,
    sql: c.sql ? String(c.sql) : undefined,
    title: c.title ? String(c.title) : undefined,
    description: c.description ? String(c.description) : undefined,
    dimensions: (c.dimensions || []).map((d: any): CubeDimension => ({
      name: String(d.name || ''),
      sql: d.sql !== undefined ? String(d.sql) : undefined,
      type: d.type ? String(d.type) : undefined,
      primary_key: !!d.primary_key,
      shown: d.shown !== false,
      title: d.title ? String(d.title) : undefined,
      description: d.description ? String(d.description) : undefined,
    })),
    measures: (c.measures || []).map((m: any): CubeMeasure => ({
      name: String(m.name || ''),
      sql: m.sql !== undefined ? String(m.sql) : undefined,
      type: String(m.type || 'count'),
      filters: Array.isArray(m.filters)
        ? m.filters.map((f: any) => ({ sql: String(f.sql || '') })).filter((f: any) => f.sql)
        : undefined,
      title: m.title ? String(m.title) : undefined,
      description: m.description ? String(m.description) : undefined,
    })),
    joins: (c.joins || []).map((j: any): CubeJoin => ({
      name: String(j.name || ''),
      relationship: String(j.relationship || 'many_to_one') as CubeJoin['relationship'],
      sql: String(j.sql || ''),
    })),
  };
}

function normalizeViewFromYaml(v: any): CubeView {
  return {
    name: String(v.name || ''),
    cubes: (v.cubes || []).map((entry: any): CubeViewCube => ({
      join_path: String(entry.join_path || ''),
      includes: entry.includes,
      excludes: Array.isArray(entry.excludes) ? entry.excludes.map(String) : undefined,
      prefix: entry.prefix,
      alias: entry.alias ? String(entry.alias) : undefined,
    })),
    description: v.description ? String(v.description) : undefined,
  };
}

// ── JS parser ────────────────────────────────────────────────────────────────

interface ParsedFile {
  cubes: CubeDef[];
  views: CubeView[];
  preAggCount: number;
  segmentCount: number;
}

/**
 * Parse a Cube .js file containing cube(`name`, { ... }) and view(`name`, { ... }) calls.
 *
 * Approach:
 *   - Strip /* *\/ block comments and // line comments.
 *   - Scan for top-level cube(...) / view(...) calls; capture name + body object literal.
 *   - Use a custom JS-object parser that preserves backtick template strings as raw text
 *     (so ${CUBE}.col is kept verbatim for our SQL translator to handle later).
 */
function parseCubeJs(text: string): ParsedFile {
  const out: ParsedFile = { cubes: [], views: [], preAggCount: 0, segmentCount: 0 };
  const stripped = stripJsComments(text);

  for (const call of findTopLevelCalls(stripped, ['cube', 'view'])) {
    try {
      if (call.kind === 'cube') {
        const obj = parseJsObjectLiteral(call.body);
        obj.name = call.name;
        out.preAggCount += Array.isArray(obj.pre_aggregations) ? obj.pre_aggregations.length
          : (obj.pre_aggregations && typeof obj.pre_aggregations === 'object' ? Object.keys(obj.pre_aggregations).length : 0);
        out.segmentCount += Array.isArray(obj.segments) ? obj.segments.length
          : (obj.segments && typeof obj.segments === 'object' ? Object.keys(obj.segments).length : 0);
        out.cubes.push(normalizeCubeFromJs(obj));
      } else {
        const obj = parseJsObjectLiteral(call.body);
        obj.name = call.name;
        out.views.push(normalizeViewFromJs(obj));
      }
    } catch (e: any) {
      throw new Error(`failed to parse ${call.kind}(${call.name}): ${e.message}`);
    }
  }
  return out;
}

function stripJsComments(text: string): string {
  let out = '';
  let i = 0;
  while (i < text.length) {
    const ch = text[i];
    const ch2 = text[i + 1];
    // Block comment
    if (ch === '/' && ch2 === '*') {
      const end = text.indexOf('*/', i + 2);
      if (end < 0) break;
      i = end + 2;
      continue;
    }
    // Line comment — but not inside a string or backtick. We only honor this at top level.
    if (ch === '/' && ch2 === '/') {
      const eol = text.indexOf('\n', i);
      i = eol < 0 ? text.length : eol;
      continue;
    }
    // Skip strings/backticks verbatim so embedded "//" or "/*" doesn't trip us up
    if (ch === '`' || ch === '\'' || ch === '"') {
      const quote = ch;
      out += ch;
      i++;
      while (i < text.length) {
        const c = text[i];
        out += c;
        if (c === '\\' && i + 1 < text.length) { out += text[i + 1]; i += 2; continue; }
        if (quote === '`' && c === '$' && text[i + 1] === '{') {
          // Walk through ${...} preserving content
          out += text[i + 1]; i += 2;
          let depth = 1;
          while (i < text.length && depth > 0) {
            const cc = text[i]; out += cc;
            if (cc === '{') depth++;
            else if (cc === '}') depth--;
            i++;
          }
          continue;
        }
        i++;
        if (c === quote) break;
      }
      continue;
    }
    out += ch;
    i++;
  }
  return out;
}

function findTopLevelCalls(
  text: string,
  fnNames: string[]
): { kind: 'cube' | 'view'; name: string; body: string }[] {
  const out: { kind: 'cube' | 'view'; name: string; body: string }[] = [];
  const fnRegex = new RegExp(`\\b(${fnNames.join('|')})\\s*\\(`, 'g');
  let match: RegExpExecArray | null;
  while ((match = fnRegex.exec(text)) !== null) {
    const fn = match[1] as 'cube' | 'view';
    let i = match.index + match[0].length;
    // Skip whitespace
    while (i < text.length && /\s/.test(text[i])) i++;
    // Read the name argument: backtick or single/double quote
    if (i >= text.length) continue;
    const q = text[i];
    if (q !== '`' && q !== '\'' && q !== '"') continue;
    i++;
    let name = '';
    while (i < text.length && text[i] !== q) {
      if (text[i] === '\\' && i + 1 < text.length) { name += text[i + 1]; i += 2; continue; }
      name += text[i++];
    }
    i++; // closing quote
    // Skip "," and whitespace
    while (i < text.length && /[\s,]/.test(text[i])) i++;
    if (text[i] !== '{') continue;
    // Capture balanced { ... }
    const start = i + 1;
    let depth = 1;
    i++;
    while (i < text.length && depth > 0) {
      const c = text[i];
      if (c === '`' || c === '\'' || c === '"') {
        i = skipString(text, i);
        continue;
      }
      if (c === '{') depth++;
      else if (c === '}') depth--;
      if (depth === 0) break;
      i++;
    }
    const body = text.slice(start, i);
    out.push({ kind: fn, name, body });
  }
  return out;
}

function skipString(text: string, start: number): number {
  const quote = text[start];
  let i = start + 1;
  while (i < text.length) {
    const c = text[i];
    if (c === '\\' && i + 1 < text.length) { i += 2; continue; }
    if (quote === '`' && c === '$' && text[i + 1] === '{') {
      i += 2;
      let depth = 1;
      while (i < text.length && depth > 0) {
        const cc = text[i];
        if (cc === '{') depth++;
        else if (cc === '}') depth--;
        i++;
      }
      continue;
    }
    if (c === quote) return i + 1;
    i++;
  }
  return i;
}

/**
 * Minimal JS-object-literal parser. Supports:
 *   { key: val, ... }, [ ... ], 'str', "str", `tpl`, numbers, booleans, null, identifiers
 *   Trailing commas. Computed keys not supported. Function values not supported.
 *
 * Backtick strings preserve their entire raw content (including ${...}).
 */
class JsObjParser {
  src: string;
  i: number;
  constructor(s: string) { this.src = s; this.i = 0; }

  parse(): any {
    this.skipWs();
    return this.value();
  }

  private skipWs(): void {
    while (this.i < this.src.length) {
      const c = this.src[this.i];
      if (/\s/.test(c)) { this.i++; continue; }
      if (c === '/' && this.src[this.i + 1] === '/') {
        const eol = this.src.indexOf('\n', this.i);
        this.i = eol < 0 ? this.src.length : eol;
        continue;
      }
      if (c === '/' && this.src[this.i + 1] === '*') {
        const end = this.src.indexOf('*/', this.i + 2);
        this.i = end < 0 ? this.src.length : end + 2;
        continue;
      }
      break;
    }
  }

  private value(): any {
    this.skipWs();
    const c = this.src[this.i];
    if (c === '{') return this.object();
    if (c === '[') return this.array();
    if (c === '`' || c === '\'' || c === '"') return this.string();
    if (c === '-' || /[0-9]/.test(c)) return this.number();
    return this.ident();
  }

  private object(): Record<string, any> {
    this.i++; // {
    const obj: Record<string, any> = {};
    this.skipWs();
    while (this.i < this.src.length && this.src[this.i] !== '}') {
      const key = this.key();
      this.skipWs();
      if (this.src[this.i] !== ':') throw new Error(`expected ":" after key "${key}" at ${this.i}`);
      this.i++; // :
      this.skipWs();
      const val = this.value();
      obj[key] = val;
      this.skipWs();
      if (this.src[this.i] === ',') { this.i++; this.skipWs(); }
    }
    if (this.src[this.i] !== '}') throw new Error(`expected "}" at ${this.i}`);
    this.i++; // }
    return obj;
  }

  private array(): any[] {
    this.i++; // [
    const out: any[] = [];
    this.skipWs();
    while (this.i < this.src.length && this.src[this.i] !== ']') {
      out.push(this.value());
      this.skipWs();
      if (this.src[this.i] === ',') { this.i++; this.skipWs(); }
    }
    if (this.src[this.i] !== ']') throw new Error(`expected "]" at ${this.i}`);
    this.i++;
    return out;
  }

  private key(): string {
    this.skipWs();
    const c = this.src[this.i];
    if (c === '\'' || c === '"' || c === '`') {
      const s = this.string();
      return String(s);
    }
    let key = '';
    while (this.i < this.src.length && /[A-Za-z0-9_$]/.test(this.src[this.i])) {
      key += this.src[this.i++];
    }
    if (!key) throw new Error(`expected key at ${this.i}`);
    return key;
  }

  // Returns the raw string contents. For backticks, preserves ${...} verbatim.
  private string(): string {
    const quote = this.src[this.i];
    this.i++;
    let s = '';
    while (this.i < this.src.length && this.src[this.i] !== quote) {
      const c = this.src[this.i];
      if (c === '\\' && this.i + 1 < this.src.length) {
        const next = this.src[this.i + 1];
        if (next === 'n') s += '\n';
        else if (next === 't') s += '\t';
        else if (next === 'r') s += '\r';
        else s += next;
        this.i += 2;
        continue;
      }
      if (quote === '`' && c === '$' && this.src[this.i + 1] === '{') {
        // Preserve template substitutions as-is so the SQL translator sees ${CUBE}.col
        s += '${';
        this.i += 2;
        let depth = 1;
        while (this.i < this.src.length && depth > 0) {
          const cc = this.src[this.i];
          if (cc === '{') depth++;
          else if (cc === '}') { depth--; if (depth === 0) { s += '}'; this.i++; break; } }
          s += cc;
          this.i++;
        }
        continue;
      }
      s += c;
      this.i++;
    }
    if (this.src[this.i] !== quote) throw new Error(`unterminated string starting near ${this.i}`);
    this.i++;
    return s;
  }

  private number(): number {
    let s = '';
    if (this.src[this.i] === '-') s += this.src[this.i++];
    while (this.i < this.src.length && /[0-9.eE+-]/.test(this.src[this.i])) s += this.src[this.i++];
    return Number(s);
  }

  private ident(): any {
    let s = '';
    while (this.i < this.src.length && /[A-Za-z0-9_$.]/.test(this.src[this.i])) {
      s += this.src[this.i++];
    }
    if (s === 'true') return true;
    if (s === 'false') return false;
    if (s === 'null') return null;
    if (s === 'undefined') return undefined;
    if (!s) throw new Error(`unexpected character "${this.src[this.i]}" at ${this.i}`);
    // Bare identifier — used for join_path: orders.customers etc.
    return s;
  }
}

function parseJsObjectLiteral(src: string): any {
  const p = new JsObjParser('{' + src + '}');
  return p.parse();
}

function normalizeCubeFromJs(obj: any): CubeDef {
  // dimensions/measures/joins are objects (key = name) in JS form
  const dims: CubeDimension[] = [];
  if (obj.dimensions && typeof obj.dimensions === 'object' && !Array.isArray(obj.dimensions)) {
    for (const [k, v] of Object.entries<any>(obj.dimensions)) {
      dims.push({
        name: k,
        sql: v.sql !== undefined ? String(v.sql) : undefined,
        type: v.type ? String(v.type) : undefined,
        primary_key: !!v.primary_key,
        shown: v.shown !== false,
        title: v.title ? String(v.title) : undefined,
        description: v.description ? String(v.description) : undefined,
      });
    }
  }
  const measures: CubeMeasure[] = [];
  if (obj.measures && typeof obj.measures === 'object' && !Array.isArray(obj.measures)) {
    for (const [k, v] of Object.entries<any>(obj.measures)) {
      measures.push({
        name: k,
        sql: v.sql !== undefined ? String(v.sql) : undefined,
        type: String(v.type || 'count'),
        filters: Array.isArray(v.filters)
          ? v.filters.map((f: any) => ({ sql: String(f.sql || '') })).filter((f: any) => f.sql)
          : undefined,
        title: v.title ? String(v.title) : undefined,
        description: v.description ? String(v.description) : undefined,
      });
    }
  }
  const joins: CubeJoin[] = [];
  if (obj.joins && typeof obj.joins === 'object' && !Array.isArray(obj.joins)) {
    for (const [k, v] of Object.entries<any>(obj.joins)) {
      joins.push({
        name: k,
        relationship: String(v.relationship || 'many_to_one') as CubeJoin['relationship'],
        sql: String(v.sql || ''),
      });
    }
  }
  return {
    name: obj.name,
    sql_table: obj.sql_table ? String(obj.sql_table) : undefined,
    sql: obj.sql ? String(obj.sql) : undefined,
    title: obj.title ? String(obj.title) : undefined,
    description: obj.description ? String(obj.description) : undefined,
    dimensions: dims,
    measures,
    joins,
  };
}

function normalizeViewFromJs(obj: any): CubeView {
  return {
    name: obj.name,
    cubes: Array.isArray(obj.cubes) ? obj.cubes.map((entry: any): CubeViewCube => ({
      // join_path is a bare identifier path like orders.customers — captured as a string by ident()
      join_path: typeof entry.join_path === 'string' ? entry.join_path : String(entry.join_path || ''),
      includes: entry.includes,
      excludes: Array.isArray(entry.excludes) ? entry.excludes.map(String) : undefined,
      prefix: entry.prefix,
      alias: entry.alias ? String(entry.alias) : undefined,
    })) : [],
    description: obj.description ? String(obj.description) : undefined,
  };
}

// ── Internal types ───────────────────────────────────────────────────────────

interface CubeDef {
  name: string;
  sql_table?: string;
  sql?: string;
  title?: string;
  description?: string;
  dimensions?: CubeDimension[];
  measures?: CubeMeasure[];
  joins?: CubeJoin[];
}

interface CubeDimension {
  name: string;
  sql?: string;
  type?: string;
  primary_key?: boolean;
  shown?: boolean;
  title?: string;
  description?: string;
}

interface CubeMeasure {
  name: string;
  sql?: string;
  type: string;
  filters?: { sql: string }[];
  title?: string;
  description?: string;
}

interface CubeJoin {
  name: string;
  relationship: 'one_to_one' | 'one_to_many' | 'many_to_one';
  sql: string;
}

interface CubeView {
  name: string;
  cubes: CubeViewCube[];
  description?: string;
}

interface CubeViewCube {
  join_path: string;
  includes?: string | string[];
  excludes?: string[];
  prefix?: boolean | string;
  alias?: string;
}

interface CubeRegistryEntry {
  elementId: string;
  pkColId: string | null;
  colIdMap: Record<string, string>;
  element: SigmaElement;
  sourceTable: string;
  cubeDef: CubeDef;
  isCustomSql: boolean;
}
