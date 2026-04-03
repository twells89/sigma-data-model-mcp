/**
 * Snowflake Semantic View YAML → Sigma Data Model JSON converter.
 */

import yaml from 'js-yaml';
import {
  resetIds, sigmaShortId, sigmaInodeId, sigmaDisplayName,
  sigmaColFormula, sigmaAggFormula,
  type SigmaElement, type ConversionResult, type ElementResult
} from './sigma-ids.js';
import { lookIsComplexSql, lookSqlToSigmaRules, detectUnsupportedSigmaFunction } from './formulas.js';

interface SnowColumn {
  name: string;
  expr?: string;
  expression?: string;
  data_type?: string;
  description?: string;
  access_modifier?: string;
}

interface SnowRelationship {
  name: string;
  foreign_key?: string | string[];
  foreignKey?: string | string[];
  ref_table?: string;
  refTable?: string;
  ref_key?: string | string[];
  refKey?: string | string[];
}

interface SnowTable {
  name: string;
  description?: string;
  base_table?: { database?: string; schema?: string; table?: string };
  dimensions?: SnowColumn[];
  time_dimensions?: SnowColumn[];
  facts?: SnowColumn[];
  primary_key?: { columns: string[] };
  relationships?: SnowRelationship[];
}

interface SnowSemanticView {
  name: string;
  tables: SnowTable[];
  relationships?: any[];
}

function snowIsIndicator(col: SnowColumn): boolean {
  const dt = (col.data_type || '').toUpperCase();
  const n = (col.name || '').toUpperCase();
  if (/^NUMBER\(1[,\s]*[01]?\)$/.test(dt)) return true;
  if (n.endsWith('_INDICATOR') || n.endsWith('_IND') || n.endsWith('_FLAG')) return true;
  return false;
}

function snowConvertTable(
  table: SnowTable,
  connectionId: string,
  extraIdentifiers: Set<string>,
  autoMetrics: boolean
): ElementResult {
  const bt = table.base_table || {};
  const db = bt.database;
  const schema = bt.schema;
  const tableName = bt.table || table.name;

  const elementId = sigmaShortId();
  const element: SigmaElement = {
    id: elementId,
    kind: 'table',
    source: {
      connectionId: connectionId || '<CONNECTION_ID>',
      kind: 'warehouse-table',
      path: [db, schema, tableName].filter(Boolean)
    },
    columns: [],
    metrics: [],
    order: []
  };

  const colIdMap: Record<string, string> = {};
  const addedIds = new Set<string>();

  function addColById(physicalId: string, semanticAlias?: string): string {
    if (addedIds.has(physicalId)) {
      if (semanticAlias && semanticAlias !== physicalId) colIdMap[semanticAlias] = colIdMap[physicalId];
      return colIdMap[physicalId];
    }
    const id = sigmaInodeId(physicalId);
    colIdMap[physicalId] = id;
    addedIds.add(physicalId);
    element.columns.push({ id, formula: sigmaColFormula(tableName, physicalId) });
    element.order.push(id);
    if (semanticAlias && semanticAlias !== physicalId) colIdMap[semanticAlias] = id;
    return id;
  }

  function resolveCol(col: SnowColumn | string) {
    if (typeof col === 'string') return { physical: col.toUpperCase(), semantic: col.toUpperCase(), expr: null as string | null };
    const semantic = (col.name || '').toUpperCase();
    const expr = (col.expr || col.expression || '').trim();
    if (!expr) return { physical: semantic, semantic, expr: null as string | null };
    if (/^[\w."]+$/.test(expr)) {
      const physical = expr.split('.').pop()!.replace(/"/g, '').toUpperCase();
      return { physical, semantic, expr: null as string | null };
    }
    if (lookIsComplexSql(expr)) {
      return { physical: null as string | null, semantic, expr };
    }
    const m = expr.match(/^([A-Za-z_][A-Za-z0-9_]*)/);
    return { physical: m ? m[1].toUpperCase() : semantic, semantic, expr: null as string | null };
  }

  // 1. Dimensions
  for (const col of table.dimensions || []) {
    const { physical, semantic, expr } = resolveCol(col);
    if (physical) {
      addColById(physical, semantic);
    } else if (expr) {
      const unsupported = detectUnsupportedSigmaFunction(expr);
      if (unsupported) {
        (element as any)._skippedDims = (element as any)._skippedDims || [];
        (element as any)._skippedDims.push({ name: semantic, reason: unsupported });
      } else {
        const formula = lookSqlToSigmaRules(expr);
        if (formula) {
          const id = sigmaShortId();
          colIdMap[semantic] = id;
          element.columns.push({ id, formula, name: sigmaDisplayName(semantic) });
          element.order.push(id);
        }
      }
    }
  }

  // 2. Time dimensions
  for (const col of table.time_dimensions || []) {
    const { physical, semantic, expr } = resolveCol(col);
    if (physical) {
      addColById(physical, semantic);
    } else if (expr) {
      const unsupported = detectUnsupportedSigmaFunction(expr);
      if (unsupported) {
        (element as any)._skippedDims = (element as any)._skippedDims || [];
        (element as any)._skippedDims.push({ name: semantic, reason: unsupported });
      } else {
        const formula = lookSqlToSigmaRules(expr);
        if (formula) {
          const id = sigmaShortId();
          colIdMap[semantic] = id;
          element.columns.push({ id, formula, name: sigmaDisplayName(semantic) });
          element.order.push(id);
        }
      }
    }
  }

  // 3. Facts + auto-metrics
  for (const col of table.facts || []) {
    const { physical, semantic } = resolveCol(col);
    if (!physical) continue;
    addColById(physical, semantic);
    if (autoMetrics && !snowIsIndicator(col as SnowColumn)) {
      if (physical.endsWith('_KEY') || physical.endsWith('_CODE')) continue;
      element.metrics!.push({
        id: sigmaShortId(),
        formula: 'Sum([' + sigmaDisplayName(physical) + '])',
        name: sigmaDisplayName(semantic || physical)
      });
    }
  }

  // 4. FK join columns
  extraIdentifiers.forEach(id => {
    if (!colIdMap[id]) addColById(id, id);
  });

  if (element.metrics!.length === 0) delete element.metrics;
  return { element, elementId, colIdMap };
}

export interface SnowflakeConvertOptions {
  connectionId?: string;
  autoMetrics?: boolean;
}

export function convertSnowflakeSemanticView(
  yamlText: string,
  options: SnowflakeConvertOptions = {}
): ConversionResult {
  resetIds();

  const { connectionId = '', autoMetrics = true } = options;

  let parsed: any;
  try {
    parsed = yaml.load(yamlText);
  } catch (e: any) {
    throw new Error('YAML parse error: ' + e.message);
  }

  const views: SnowSemanticView[] = Array.isArray(parsed) ? parsed : [parsed];
  const allTables = views.reduce((a: SnowTable[], v) => a.concat(v.tables || []), []);
  const topLevelRels = views.reduce((a: any[], v) => a.concat(v.relationships || []), []);
  const viewName = (views[0] && views[0].name) || 'Snowflake Semantic View';

  if (!allTables.length) throw new Error('No tables found in YAML');

  // Pass 1: Pre-scan FK columns
  const fkColsByTable: Record<string, Set<string>> = {};
  allTables.forEach(table => {
    const tname = table.name.toUpperCase();
    (table.relationships || []).forEach((rel: any) => {
      const fks = ([] as string[]).concat(rel.foreign_key || rel.foreignKey || []);
      // Also support join_columns / relationship_columns format
      (rel.join_columns || rel.relationship_columns || []).forEach((jc: any) => {
        if (jc.left_column) fks.push(jc.left_column);
      });
      if (!fkColsByTable[tname]) fkColsByTable[tname] = new Set();
      fks.filter(Boolean).forEach(fk => fkColsByTable[tname].add(fk.toUpperCase()));
    });
  });

  // Pass 2: Convert tables
  const warnings: string[] = [];
  const elements: SigmaElement[] = [];
  const tableIndex: Record<string, ElementResult> = {};

  allTables.forEach(table => {
    try {
      const extra = fkColsByTable[table.name.toUpperCase()] || new Set();
      const result = snowConvertTable(table, connectionId, extra, autoMetrics);
      // Surface skipped dims as warnings
      for (const { name, reason } of (result.element as any)._skippedDims || []) {
        warnings.push(`⚠ "${table.name}.${name}": skipped — contains ${reason}() which has no Sigma equivalent. Add this column manually in the Sigma UI.`);
      }
      delete (result.element as any)._skippedDims;
      elements.push(result.element);
      tableIndex[table.name.toUpperCase()] = result;
    } catch (e: any) {
      warnings.push('Failed to convert table "' + table.name + '": ' + e.message);
    }
  });

  // Pass 3: Resolve inline relationships
  allTables.forEach(table => {
    const srcEntry = tableIndex[table.name.toUpperCase()];
    if (!srcEntry) return;

    (table.relationships || []).forEach((rel: SnowRelationship) => {
      const refTable = (rel.ref_table || rel.refTable || '').toUpperCase();
      const tgtEntry = tableIndex[refTable];

      if (!tgtEntry) {
        warnings.push('Relationship "' + rel.name + '": ref_table "' + (rel.ref_table || rel.refTable) + '" not found');
        return;
      }

      const fks = ([] as string[]).concat(rel.foreign_key || rel.foreignKey || []);
      const rks = ([] as string[]).concat(rel.ref_key || rel.refKey || []);

      // Also support join_columns / relationship_columns format
      const joinCols: any[] = (rel as any).join_columns || (rel as any).relationship_columns || [];
      if (joinCols.length > 0 && fks.length === 0) {
        joinCols.forEach((jc: any) => {
          if (jc.left_column) fks.push(jc.left_column);
          if (jc.right_column) rks.push(jc.right_column);
        });
      }

      const keys: { sourceColumnId: string; targetColumnId: string }[] = [];
      for (let i = 0; i < fks.length; i++) {
        const srcId = srcEntry.colIdMap[fks[i].toUpperCase()];
        const rk = (rks[i] || rks[0] || '');
        const tgtId = tgtEntry.colIdMap[rk.toUpperCase()];
        if (srcId && tgtId) {
          keys.push({ sourceColumnId: srcId, targetColumnId: tgtId });
        } else {
          warnings.push('Relationship "' + rel.name + '": column "' + (!srcId ? fks[i] : rk) + '" not found');
        }
      }

      if (keys.length) {
        if (!srcEntry.element.relationships) srcEntry.element.relationships = [];
        srcEntry.element.relationships.push({
          id: sigmaShortId(),
          targetElementId: tgtEntry.elementId,
          keys,
          name: (rel.name || '').replace(/_/g, ' '),
          relationshipType: 'N:1'
        });
      }
    });
  });

  // Pass 4: Resolve top-level relationships
  topLevelRels.forEach((rel: any) => {
    const leftKey = (rel.left_table || '').toUpperCase();
    const rightKey = (rel.right_table || '').toUpperCase();
    const left = tableIndex[leftKey];
    const right = tableIndex[rightKey];

    if (!left || !right) {
      warnings.push('Relationship "' + rel.name + '": table "' + (!left ? rel.left_table : rel.right_table) + '" not found');
      return;
    }

    const keys: { sourceColumnId: string; targetColumnId: string }[] = [];
    (rel.relationship_columns || []).forEach((rc: any) => {
      const srcId = left.colIdMap[(rc.left_column || '').toUpperCase()];
      const tgtId = right.colIdMap[(rc.right_column || '').toUpperCase()];
      if (srcId && tgtId) {
        keys.push({ sourceColumnId: srcId, targetColumnId: tgtId });
      } else {
        warnings.push('Relationship "' + rel.name + '": column "' + (!srcId ? rc.left_column : rc.right_column) + '" not found');
      }
    });

    if (keys.length) {
      if (!left.element.relationships) left.element.relationships = [];
      left.element.relationships.push({
        id: sigmaShortId(),
        targetElementId: right.elementId,
        keys,
        name: rel.name ? rel.name.replace(/_/g, ' ')
          : sigmaDisplayName(rel.left_table) + ' to ' + sigmaDisplayName(rel.right_table),
        relationshipType: 'N:1'
      });
    }
  });

  // Pass 5: Deduplicate same-physical-path elements
  elements.forEach(srcEl => {
    if (!srcEl.relationships) return;
    const usedPaths: Record<string, string> = {};
    srcEl.relationships = srcEl.relationships.filter(rel => {
      const tgtEl = elements.find(e => e.id === rel.targetElementId);
      if (!tgtEl) return true;
      const pathKey = (tgtEl.source?.path || []).join('|');
      if (usedPaths[pathKey]) {
        warnings.push('⚠ Duplicate path: "' + rel.name + '" → ' + pathKey +
          ' is already joined as "' + usedPaths[pathKey] + '".');
        return false;
      }
      usedPaths[pathKey] = rel.name;
      return true;
    });
    if (srcEl.relationships.length === 0) delete srcEl.relationships;
  });

  if (!connectionId) warnings.unshift('⚠ Connection ID not set — update in JSON before saving to Sigma');

  const sigmaModel = {
    name: sigmaDisplayName(viewName),
    pages: [{ id: sigmaShortId(), name: 'Page 1', elements }]
  };

  const totalCols = elements.reduce((s, e) => s + (e.columns?.length || 0), 0);
  const totalMetrics = elements.reduce((s, e) => s + (e.metrics?.length || 0), 0);
  const totalRels = elements.reduce((s, e) => s + (e.relationships?.length || 0), 0);

  return {
    model: sigmaModel,
    warnings,
    stats: {
      tables: allTables.length,
      elements: elements.length,
      columns: totalCols,
      metrics: totalMetrics,
      relationships: totalRels
    }
  };
}
