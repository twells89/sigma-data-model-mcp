/**
 * dbt Semantic Model YAML → Sigma Data Model JSON converter.
 */

import yaml from 'js-yaml';
import {
  resetIds, sigmaShortId, sigmaInodeId, sigmaDisplayName,
  sigmaColFormula, sigmaAggFormula, buildDerivedElements,
  type SigmaElement, type SigmaColumn, type SigmaMetric,
  type ConversionResult, type ElementResult
} from './sigma-ids.js';
import { lookIsComplexSql, lookSqlToSigmaRules, detectUnsupportedSigmaFunction, hasResidualCaseKeyword, hasResidualInfixOperator } from './formulas.js';

interface DbtEntity {
  name: string;
  type: string;
  expr?: string;
  description?: string;
  label?: string;
}

interface DbtDimension {
  name: string;
  expr?: string;
  type?: string;
  description?: string;
  label?: string;
}

interface DbtMeasure {
  name: string;
  expr?: string;
  agg: string;
  description?: string;
  label?: string;
}

interface DbtSemanticModel {
  name: string;
  model?: string;
  node_relation?: {
    database?: string;
    schema_name?: string;
    schema?: string;
    alias?: string;
    relation_name?: string;
  };
  entities?: DbtEntity[];
  dimensions?: DbtDimension[];
  measures?: DbtMeasure[];
}

interface DbtMetric {
  name: string;
  type: string;
  description?: string;
  label?: string;
  type_params?: {
    measure?: string | { name: string };
    numerator?: string | { name: string };
    denominator?: string | { name: string };
    expr?: string;
  };
}

interface DbtConvertConfig {
  database?: string;
  schema?: string;
  connectionId?: string;
}

/**
 * Uppercase any bare identifier in `expr` that matches a known column name (case-insensitive),
 * so that downstream lookSqlToSigmaRules / lookConvertExpression picks them up as references
 * (its bracketing regex requires ALL_CAPS) and turns them into [Display Name] form.
 *
 * Identifiers inside double-quoted strings are left alone (Sigma uses double quotes for string
 * literals — same as the dbt YAML expr like `customer_segment = "Pro"`).
 */
function preBracketKnownNames(expr: string, knownNames: Set<string>): string {
  if (!knownNames.size) return expr;
  // Tokenize: split on string literals (single OR double quotes) so we don't touch their contents
  const parts = expr.split(/("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')/);
  for (let i = 0; i < parts.length; i++) {
    if (i % 2 === 1) continue; // odd indices are string literals — leave alone
    parts[i] = parts[i].replace(/\b([A-Za-z_][A-Za-z0-9_]*)\b/g, (m) => {
      if (_SQL_KEYWORD_RE.test(m)) return m;
      return knownNames.has(m.toUpperCase()) ? m.toUpperCase() : m;
    });
  }
  return parts.join('');
}

/**
 * Normalize dbt "model-level YAML" (newer form: `models[].semantic_model` + `columns[]`
 * + nested `metrics[]`) into the standard `semantic_models[]` + top-level `metrics[]`
 * shape. No-op when the input is already in the standard form. Inline-aggregation
 * metrics (with `agg`+`expr` and no `type_params.measure`) are hoisted into the
 * model's `measures[]` so descriptions flow through correctly.
 */
function dbtNormalizeModelLevelYaml(parsed: any): any {
  if (!parsed || !Array.isArray(parsed.models)) return parsed;
  const semanticModels: any[] = [];
  const liftedMetrics: any[] = [];
  for (const model of parsed.models) {
    const sm = model.semantic_model;
    if (!sm) continue;
    const entities: any[] = [];
    const dimensions: any[] = [];
    const measures: any[] = [];
    for (const col of (model.columns || [])) {
      if (col.entity) {
        entities.push({
          name: col.entity.name || col.name,
          type: col.entity.type,
          expr: col.name,
          ...(col.description ? { description: col.description } : {}),
        });
      } else if (col.dimension) {
        dimensions.push({
          name: col.name,
          type: col.dimension.type,
          ...(col.dimension.type_params ? { type_params: col.dimension.type_params } : {}),
          expr: col.name,
          ...(col.description ? { description: col.description } : {}),
        });
      } else if (col.measure) {
        measures.push({
          name: col.measure.name || col.name,
          agg: col.measure.agg,
          expr: col.name,
          ...(col.description ? { description: col.description } : {}),
          ...(col.measure.filter ? { filter: col.measure.filter } : {}),
        });
      }
    }
    for (const m of (model.metrics || [])) {
      if (m.agg && (m.expr || m.measure)) {
        measures.push({
          name: m.name,
          agg: m.agg,
          expr: m.expr || m.measure,
          ...(m.description ? { description: m.description } : {}),
          ...(m.filter ? { filter: m.filter } : {}),
        });
      } else {
        liftedMetrics.push(m);
      }
    }
    semanticModels.push({
      name: sm.name || model.name,
      ...(model.description ? { description: model.description } : (sm.description ? { description: sm.description } : {})),
      model: model.model || `ref('${model.name}')`,
      ...(sm.agg_time_dimension ? { defaults: { agg_time_dimension: sm.agg_time_dimension } } : {}),
      entities,
      dimensions,
      measures,
    });
  }
  if (semanticModels.length) {
    parsed.semantic_models = (parsed.semantic_models || []).concat(semanticModels);
    if (liftedMetrics.length) parsed.metrics = (parsed.metrics || []).concat(liftedMetrics);
  }
  return parsed;
}

function convertDbtSemanticModel(
  model: DbtSemanticModel,
  config: DbtConvertConfig,
  allMeasuresByModel: Record<string, { agg: string; exprId: string }>,
  knownNames: Set<string> = new Set(),
  warnings?: string[]
): ElementResult {
  const connectionId = config.connectionId || '<CONNECTION_ID>';

  let db = config.database || null;
  let schema = config.schema || null;
  let tableName = model.name.toUpperCase();

  if (model.node_relation) {
    const nr = model.node_relation;
    db = nr.database || db;
    schema = nr.schema_name || nr.schema || schema;
    if (nr.alias) tableName = nr.alias.toUpperCase();
    else if (nr.relation_name)
      tableName = nr.relation_name.split('.').pop()!.replace(/"/g, '').toUpperCase();
  } else if (model.model) {
    const m = model.model.match(/ref\(['"]([^'"]+)['"]\)/);
    if (m) tableName = m[1].toUpperCase();
  }

  const path = [db, schema, tableName].filter(Boolean) as string[];
  const elementId = sigmaShortId();
  const element: SigmaElement = {
    id: elementId,
    kind: 'table',
    source: { connectionId, kind: 'warehouse-table', path },
    columns: [],
    metrics: [],
    order: []
  };

  const colIdMap: Record<string, string> = {};

  function addCol(identifier: string, description?: string): string {
    const id = sigmaInodeId(identifier.toUpperCase());
    colIdMap[identifier.toUpperCase()] = id;
    const col: SigmaColumn = { id, formula: sigmaColFormula(tableName, identifier) };
    if (description) col.description = description;
    element.columns.push(col);
    element.order.push(id);
    return id;
  }

  // Entities (primary/unique/natural) → columns
  for (const entity of model.entities || []) {
    if (entity.type === 'primary' || entity.type === 'unique') {
      addCol((entity.expr || entity.name).toUpperCase(), entity.description);
    } else if (entity.type === 'natural') {
      // natural entities are source columns only — no join relationship
      addCol((entity.expr || entity.name).toUpperCase(), entity.description);
    }
  }

  // Dimensions → columns
  for (const dim of model.dimensions || []) {
    const rawExpr = (dim.expr || dim.name || '').trim();
    if (lookIsComplexSql(rawExpr)) {
      const unsupported = detectUnsupportedSigmaFunction(rawExpr);
      if (unsupported) {
        // Skip with warning — emitting broken SQL would cause Sigma save errors
        // Collect on a skippedDims array attached to element for caller to surface
        (element as any)._skippedDims = (element as any)._skippedDims || [];
        (element as any)._skippedDims.push({ name: dim.name, reason: unsupported });
      } else {
        const preBracketed = preBracketKnownNames(rawExpr, knownNames);
        const formula = lookSqlToSigmaRules(preBracketed);
        // lookSqlToSigmaRules is not guaranteed residue-free: its CASE/ROUND/
        // arithmetic patterns can splice a converted sub-expression back into
        // a template that still carries an untranslated construct — a CASE
        // condition with a bare LIKE/BETWEEN (no Sigma equivalent), or an
        // unsupported "simple CASE" shape that survives raw. Drop and warn
        // rather than emit broken Sigma, same posture as the
        // detectUnsupportedSigmaFunction check just above and lookml.ts's
        // computed-measure guard.
        if (formula && (hasResidualCaseKeyword(formula) || hasResidualInfixOperator(formula))) {
          (element as any)._skippedDims = (element as any)._skippedDims || [];
          (element as any)._skippedDims.push({
            name: dim.name,
            reason: hasResidualInfixOperator(formula) ? 'LIKE/BETWEEN' : 'CASE',
          });
        } else if (formula) {
          const id = sigmaShortId();
          const semantic = dim.name.toUpperCase();
          colIdMap[semantic] = id;
          const col: SigmaColumn = { id, formula, name: sigmaDisplayName(dim.name) };
          if (dim.description) col.description = dim.description;
          element.columns.push(col);
          element.order.push(id);
        }
      }
    } else {
      const identifier = rawExpr.split('.').pop()!.replace(/"/g, '').toUpperCase() || dim.name.toUpperCase();
      addCol(identifier, dim.description);
    }
  }

  // Measures → source column + metric
  for (const measure of model.measures || []) {
    const rawExpr = (measure.expr || measure.name || '').trim();
    const exprId = rawExpr.split('.').pop()!.replace(/"/g, '').toUpperCase() || measure.name.toUpperCase();
    if (!colIdMap[exprId]) addCol(exprId);
    const metric: SigmaMetric = {
      id: sigmaShortId(),
      formula: sigmaAggFormula(measure.agg, exprId, warnings),
      name: sigmaDisplayName(measure.name),
    };
    if (measure.description) metric.description = measure.description;
    element.metrics!.push(metric);
    allMeasuresByModel[measure.name] = { agg: measure.agg, exprId };
  }

  if (element.metrics!.length === 0) delete element.metrics;
  return { element, elementId, colIdMap };
}

function convertDbtMetrics(
  metrics: DbtMetric[],
  allMeasuresByModel: Record<string, { agg: string; exprId: string }>,
  elements: SigmaElement[],
  warnings?: string[]
): { targetElementId: string; metric: { id: string; formula: string; name: string; description?: string } }[] {
  const result: { targetElementId: string; metric: { id: string; formula: string; name: string; description?: string } }[] = [];
  for (const metric of metrics || []) {
    const tp = metric.type_params || {};
    const name = sigmaDisplayName(metric.name);
    let formula = '';

    if (metric.type === 'simple') {
      const mName = typeof tp.measure === 'object' ? tp.measure?.name : tp.measure;
      const src = allMeasuresByModel[mName || ''];
      formula = src ? sigmaAggFormula(src.agg, src.exprId, warnings) : `/* measure: ${mName} */`;
    } else if (metric.type === 'ratio') {
      const num = typeof tp.numerator === 'object' ? tp.numerator?.name : tp.numerator;
      const den = typeof tp.denominator === 'object' ? tp.denominator?.name : tp.denominator;
      const ns = allMeasuresByModel[num || ''], ds = allMeasuresByModel[den || ''];
      const nf = ns ? sigmaAggFormula(ns.agg, ns.exprId, warnings) : `[${sigmaDisplayName(num || '')}]`;
      const df = ds ? sigmaAggFormula(ds.agg, ds.exprId, warnings) : `[${sigmaDisplayName(den || '')}]`;
      formula = `${nf} / NullIf(${df}, 0)`;
    } else if (metric.type === 'derived') {
      formula = (tp.expr || '').replace(
        /\{\{\s*metric\(['"]([^'"]+)['"]\)\s*\}\}/g,
        (_, m) => {
          const s = allMeasuresByModel[m];
          return s ? sigmaAggFormula(s.agg, s.exprId, warnings) : `[${sigmaDisplayName(m)}]`;
        }
      ) || `/* derived: ${metric.name} */`;
    } else {
      formula = `/* unsupported type: ${metric.type} */`;
    }

    if (formula && elements[0]) {
      const m: { id: string; formula: string; name: string; description?: string } = { id: sigmaShortId(), formula, name };
      if (metric.description) m.description = metric.description;
      result.push({ targetElementId: elements[0].id, metric: m });
    }
  }
  return result;
}

export interface DbtConvertOptions {
  connectionId?: string;
  database?: string;
  schema?: string;
}

export function convertDbtToSigma(
  yamlText: string,
  options: DbtConvertOptions = {}
): ConversionResult {
  resetIds();

  const { connectionId = '', database = '', schema = '' } = options;

  let parsed: any;
  try {
    parsed = dbtNormalizeModelLevelYaml(yaml.load(yamlText));
  } catch (e: any) {
    throw new Error('YAML parse error: ' + e.message);
  }

  let semanticModels: DbtSemanticModel[] = [];
  let dbtMetrics: DbtMetric[] = [];

  if (parsed.semantic_models) semanticModels = parsed.semantic_models;
  else if (Array.isArray(parsed)) semanticModels = parsed.filter((x: any) => x.entities || x.dimensions || x.measures);
  else if (parsed.name && (parsed.entities || parsed.dimensions || parsed.measures)) semanticModels = [parsed];
  if (parsed.metrics) dbtMetrics = parsed.metrics;

  if (!semanticModels.length) throw new Error('No semantic models found in the YAML');

  const config: DbtConvertConfig = { database, schema, connectionId };
  const allMeasuresByModel: Record<string, { agg: string; exprId: string }> = {};
  const elements: SigmaElement[] = [];
  const warnings: string[] = [];

  // Build a set of all known column identifiers across all semantic models so that
  // cross-element refs in calc-col exprs (e.g. order_fact.is_pro_segment referencing
  // customer_segment from customer_dim via foreign entity) survive as upper-case
  // identifiers that lookConvertExpression then brackets as [Display Name].
  const knownNames = new Set<string>();
  for (const m of semanticModels) {
    for (const e of m.entities || []) knownNames.add((e.expr || e.name || '').toUpperCase());
    for (const d of m.dimensions || []) {
      const expr = (d.expr || d.name || '').trim();
      if (lookIsComplexSql(expr)) knownNames.add(d.name.toUpperCase());
      else knownNames.add((expr.split('.').pop()!.replace(/"/g, '') || d.name).toUpperCase());
    }
    for (const me of m.measures || []) {
      const expr = (me.expr || me.name || '').trim();
      knownNames.add((expr.split('.').pop()!.replace(/"/g, '') || me.name).toUpperCase());
    }
  }
  knownNames.delete('');

  for (const model of semanticModels) {
    try {
      const { element } = convertDbtSemanticModel(model, config, allMeasuresByModel, knownNames, warnings);
      // Surface any skipped dimensions as warnings
      for (const { name, reason } of (element as any)._skippedDims || []) {
        warnings.push(`⚠ "${model.name}.${name}": skipped — contains ${reason}() which has no Sigma equivalent. Add this column manually in the Sigma UI.`);
      }
      delete (element as any)._skippedDims;
      elements.push(element);
    } catch (e: any) {
      warnings.push(`Failed to convert model "${model.name}": ${e.message}`);
    }
  }

  // Resolve foreign entity cross-references → relationships
  const elementColIdMaps = elements.map(el => {
    const map: Record<string, string> = {};
    (el.columns || []).forEach(c => {
      const parts = c.id.split('/');
      if (parts.length > 1) map[parts[parts.length - 1]] = c.id;
    });
    return map;
  });

  semanticModels.forEach((model, i) => {
    const element = elements[i];
    if (!element) return;
    for (const entity of model.entities || []) {
      if (entity.type !== 'foreign') continue;
      const logicalName = (entity.name || '').toUpperCase();
      const physicalFk = (entity.expr || entity.name || '').toUpperCase();

      let targetEntity: DbtEntity | undefined;
      const targetIdx = semanticModels.findIndex(m => {
        const found = m.entities?.find(en =>
          (en.name || '').toUpperCase() === logicalName &&
          (en.type === 'primary' || en.type === 'unique')
        );
        if (found) { targetEntity = found; return true; }
        return false;
      });

      // Materialize FK column on the source element unconditionally — even when
      // the target dim isn't loaded. The FK is a real warehouse column whether or
      // not a Sigma relationship can be wired; gating addCol on a found target
      // silently drops FK columns when users upload a single fact-table YAML
      // without its dim files.
      const srcTableName = (element.source?.path?.[2] || model.name).toUpperCase();
      let srcColId = elementColIdMaps[i][physicalFk];
      if (!srcColId) {
        srcColId = sigmaInodeId(physicalFk);
        element.columns.push({ id: srcColId, formula: sigmaColFormula(srcTableName, physicalFk) });
        element.order.push(srcColId);
        elementColIdMaps[i][physicalFk] = srcColId;
      }

      if (targetIdx < 0 || !elements[targetIdx]) {
        warnings.push(`Foreign entity "${entity.name}" on "${model.name}" — no matching primary entity found; FK column added without relationship wiring`);
        continue;
      }

      const targetEl = elements[targetIdx];
      const tgtTableName = (targetEl.source?.path?.[2] || semanticModels[targetIdx].name).toUpperCase();
      // Target column key: the target entity's physical expr (column on the dim table)
      const tgtColKey = (targetEntity?.expr || targetEntity?.name || logicalName).toUpperCase();

      const tgtColId = elementColIdMaps[targetIdx][tgtColKey];
      if (!tgtColId) {
        warnings.push(`Foreign entity "${entity.name}" on "${model.name}" — target column "${tgtColKey}" not found`);
        continue;
      }

      if (!element.relationships) element.relationships = [];
      element.relationships.push({
        id: sigmaShortId(),
        targetElementId: targetEl.id,
        keys: [{ sourceColumnId: srcColId, targetColumnId: tgtColId }],
        name: tgtTableName,
        relationshipType: 'N:1'
      });
    }
  });

  // Add derived/ratio metrics
  for (const { targetElementId, metric } of convertDbtMetrics(dbtMetrics, allMeasuresByModel, elements, warnings)) {
    const el = elements.find(e => e.id === targetElementId);
    if (!el) continue;
    // Never ship a placeholder formula (/* ... */) as a real metric — warn and skip.
    if (metric.formula.trim().startsWith('/*')) {
      warnings.push(`⚠ metric "${metric.name}": could not be translated (${metric.formula.replace(/\/\*|\*\//g, '').trim()}) — skipped. Add it manually in Sigma.`);
      continue;
    }
    if (!el.metrics) el.metrics = [];
    el.metrics.push(metric);
  }

  // ── Pull cross-element calc cols off source elements (moved to derived) ─
  // Calc cols whose formula references a related-table column by display name
  // (e.g. order_fact.is_pro_segment refs customer_segment via foreign entity)
  // cannot resolve on the source warehouse-table element — Sigma doesn't see
  // those names in scope. Mirror tableau.ts buildDerivedElementsAndMoveCalcs:
  // pull them off, build derived "<Table> View" elements, place calcs on the
  // derived element with bare [X] refs rewritten to [SRC/REL/X] form.
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

  // Build derived elements for fact tables with relationships
  const derivedEls = buildDerivedElements(elements);
  for (const de of derivedEls) elements.push(de);

  // Place pulled cross-element calc cols onto matching derived element,
  // rewriting bare [X] refs to [SRC/REL/X] form.
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

  if (!connectionId) warnings.unshift('⚠ Connection ID not set — update in JSON before saving to Sigma');

  const sigmaModel = {
    name: semanticModels.length === 1
      ? sigmaDisplayName(semanticModels[0].name)
      : 'Dbt Semantic Models',
    schemaVersion: 1,
    pages: [{ id: sigmaShortId(), name: 'Page 1', elements }]
  };

  const totalCols = elements.reduce((s, e) => s + (e.columns?.length || 0), 0);
  const totalMetrics = elements.reduce((s, e) => s + (e.metrics?.length || 0), 0);
  const totalRels = elements.reduce((s, e) => s + (e.relationships?.length || 0), 0);

  return {
    model: sigmaModel,
    warnings,
    stats: {
      models: semanticModels.length,
      elements: elements.length,
      columns: totalCols,
      metrics: totalMetrics,
      relationships: totalRels,
      dbtMetrics: dbtMetrics.length
    }
  };
}
