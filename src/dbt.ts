/**
 * dbt Semantic Model YAML → Sigma Data Model JSON converter.
 */

import yaml from 'js-yaml';
import {
  resetIds, sigmaShortId, sigmaInodeId, sigmaDisplayName,
  sigmaColFormula, sigmaAggFormula,
  type SigmaElement, type ConversionResult, type ElementResult
} from './sigma-ids.js';
import { lookIsComplexSql, lookSqlToSigmaRules } from './formulas.js';

interface DbtEntity {
  name: string;
  type: string;
  expr?: string;
}

interface DbtDimension {
  name: string;
  expr?: string;
  type?: string;
}

interface DbtMeasure {
  name: string;
  expr?: string;
  agg: string;
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

function convertDbtSemanticModel(
  model: DbtSemanticModel,
  config: DbtConvertConfig,
  allMeasuresByModel: Record<string, { agg: string; exprId: string }>
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

  function addCol(identifier: string): string {
    const id = sigmaInodeId(identifier.toUpperCase());
    colIdMap[identifier.toUpperCase()] = id;
    element.columns.push({ id, formula: sigmaColFormula(tableName, identifier) });
    element.order.push(id);
    return id;
  }

  // Entities (primary/unique) → columns
  for (const entity of model.entities || []) {
    if (entity.type === 'primary' || entity.type === 'unique') {
      addCol((entity.expr || entity.name).toUpperCase());
    }
  }

  // Dimensions → columns
  for (const dim of model.dimensions || []) {
    const rawExpr = (dim.expr || dim.name || '').trim();
    if (lookIsComplexSql(rawExpr)) {
      const formula = lookSqlToSigmaRules(rawExpr);
      if (formula) {
        const id = sigmaShortId();
        const semantic = dim.name.toUpperCase();
        colIdMap[semantic] = id;
        element.columns.push({ id, formula, name: sigmaDisplayName(dim.name) });
        element.order.push(id);
      }
    } else {
      const identifier = rawExpr.split('.').pop()!.replace(/"/g, '').toUpperCase() || dim.name.toUpperCase();
      addCol(identifier);
    }
  }

  // Measures → source column + metric
  for (const measure of model.measures || []) {
    const rawExpr = (measure.expr || measure.name || '').trim();
    const exprId = rawExpr.split('.').pop()!.replace(/"/g, '').toUpperCase() || measure.name.toUpperCase();
    if (!colIdMap[exprId]) addCol(exprId);
    element.metrics!.push({
      id: sigmaShortId(),
      formula: sigmaAggFormula(measure.agg, exprId),
      name: sigmaDisplayName(measure.name)
    });
    allMeasuresByModel[measure.name] = { agg: measure.agg, exprId };
  }

  if (element.metrics!.length === 0) delete element.metrics;
  return { element, elementId, colIdMap };
}

function convertDbtMetrics(
  metrics: DbtMetric[],
  allMeasuresByModel: Record<string, { agg: string; exprId: string }>,
  elements: SigmaElement[]
): { targetElementId: string; metric: { id: string; formula: string; name: string } }[] {
  const result: { targetElementId: string; metric: { id: string; formula: string; name: string } }[] = [];
  for (const metric of metrics || []) {
    const tp = metric.type_params || {};
    const name = sigmaDisplayName(metric.name);
    let formula = '';

    if (metric.type === 'simple') {
      const mName = typeof tp.measure === 'object' ? tp.measure?.name : tp.measure;
      const src = allMeasuresByModel[mName || ''];
      formula = src ? sigmaAggFormula(src.agg, src.exprId) : `/* measure: ${mName} */`;
    } else if (metric.type === 'ratio') {
      const num = typeof tp.numerator === 'object' ? tp.numerator?.name : tp.numerator;
      const den = typeof tp.denominator === 'object' ? tp.denominator?.name : tp.denominator;
      const ns = allMeasuresByModel[num || ''], ds = allMeasuresByModel[den || ''];
      const nf = ns ? sigmaAggFormula(ns.agg, ns.exprId) : `[${sigmaDisplayName(num || '')}]`;
      const df = ds ? sigmaAggFormula(ds.agg, ds.exprId) : `[${sigmaDisplayName(den || '')}]`;
      formula = `${nf} / NullIf(${df}, 0)`;
    } else if (metric.type === 'derived') {
      formula = (tp.expr || '').replace(
        /\{\{\s*metric\(['"]([^'"]+)['"]\)\s*\}\}/g,
        (_, m) => {
          const s = allMeasuresByModel[m];
          return s ? sigmaAggFormula(s.agg, s.exprId) : `[${sigmaDisplayName(m)}]`;
        }
      ) || `/* derived: ${metric.name} */`;
    } else {
      formula = `/* unsupported type: ${metric.type} */`;
    }

    if (formula && elements[0]) {
      result.push({ targetElementId: elements[0].id, metric: { id: sigmaShortId(), formula, name } });
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
    parsed = yaml.load(yamlText);
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

  for (const model of semanticModels) {
    try {
      const { element } = convertDbtSemanticModel(model, config, allMeasuresByModel);
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

      const targetIdx = semanticModels.findIndex(m =>
        m.entities?.some(en =>
          (en.name || '').toUpperCase() === logicalName &&
          (en.type === 'primary' || en.type === 'unique')
        )
      );
      if (targetIdx < 0 || !elements[targetIdx]) {
        warnings.push(`Foreign entity "${entity.name}" on "${model.name}" — no matching primary entity found`);
        continue;
      }

      const targetEl = elements[targetIdx];
      const srcTableName = (element.source?.path?.[2] || model.name).toUpperCase();

      let srcColId = elementColIdMaps[i][physicalFk];
      if (!srcColId) {
        srcColId = sigmaInodeId(physicalFk);
        element.columns.push({ id: srcColId, formula: sigmaColFormula(srcTableName, physicalFk) });
        element.order.push(srcColId);
        elementColIdMaps[i][physicalFk] = srcColId;
      }

      const tgtColId = elementColIdMaps[targetIdx][logicalName];
      if (!tgtColId) {
        warnings.push(`Foreign entity "${entity.name}" on "${model.name}" — target column "${logicalName}" not found`);
        continue;
      }

      if (!element.relationships) element.relationships = [];
      element.relationships.push({
        id: sigmaShortId(),
        targetElementId: targetEl.id,
        keys: [{ sourceColumnId: srcColId, targetColumnId: tgtColId }],
        name: `${sigmaDisplayName(model.name)} to ${sigmaDisplayName(semanticModels[targetIdx].name)}`
      });
    }
  });

  // Add derived/ratio metrics
  for (const { targetElementId, metric } of convertDbtMetrics(dbtMetrics, allMeasuresByModel, elements)) {
    const el = elements.find(e => e.id === targetElementId);
    if (el) {
      if (!el.metrics) el.metrics = [];
      el.metrics.push(metric);
    }
  }

  if (!connectionId) warnings.unshift('⚠ Connection ID not set — update in JSON before saving to Sigma');

  const sigmaModel = {
    name: semanticModels.length === 1
      ? sigmaDisplayName(semanticModels[0].name)
      : 'Dbt Semantic Models',
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
