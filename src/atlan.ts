/**
 * Atlan Data Contract YAML/JSON → Sigma Data Model converter.
 * Accepts an Atlan data contract document (YAML or JSON string).
 */

import yaml from 'js-yaml';
import {
  resetIds, sigmaShortId, sigmaDisplayName,
  inferSigmaFormat, buildDerivedElements,
  type SigmaElement, type SigmaColumn, type SigmaMetric, type ConversionResult,
} from './sigma-ids.js';

export interface AtlanConvertOptions {
  connectionId?: string;
  database?: string;
  schema?: string;
}

export function convertAtlanToSigma(
  contractText: string,
  options: AtlanConvertOptions = {},
): ConversionResult {
  resetIds();
  const { connectionId = '<CONNECTION_ID>', database = '', schema = '' } = options;
  const warnings: string[] = [];

  const contract = parseContract(contractText);

  const elements: SigmaElement[] = [];
  const elementMap: Record<string, { elementId: string; colIdMap: Record<string, string> }> = {};

  // Parse models → elements
  for (const [modelName, modelDef] of Object.entries(contract.models || {})) {
    const tableName = modelName.toUpperCase();
    const elementId = sigmaShortId();
    const path = [database || 'DATABASE', schema || 'SCHEMA', tableName].filter(Boolean);

    const columns: SigmaColumn[] = [];
    const metrics: SigmaMetric[] = [];
    const order: string[] = [];
    const colIdMap: Record<string, string> = {};

    const fields = (modelDef as any).fields || (modelDef as any).columns || {};
    for (const [fieldName, fieldDef] of Object.entries(fields as Record<string, any>)) {
      const physCol = fieldName.toUpperCase();
      const displayName = sigmaDisplayName(physCol);
      const colId = sigmaShortId();
      colIdMap[physCol] = colId;

      const col: any = { id: colId, formula: `[${tableName}/${displayName}]` };
      if (fieldDef.description) col.description = fieldDef.description;
      columns.push(col);
      order.push(colId);

      const fType = (fieldDef.type || '').toLowerCase();
      if (['decimal','numeric','number','float','double','integer','int','bigint'].includes(fType)) {
        if (!physCol.endsWith('_KEY') && !physCol.endsWith('_ID') && !physCol.endsWith('_CODE')
            && !fieldDef.primaryKey && !fieldDef.primary_key) {
          const formula = `Sum([${displayName}])`;
          const metricName = `Total ${displayName}`;
          const metric: any = { id: sigmaShortId(), formula, name: metricName };
          const fmt = inferSigmaFormat(formula, metricName);
          if (fmt) metric.format = fmt;
          metrics.push(metric);
        }
      }
    }

    const element: SigmaElement = {
      id: elementId, kind: 'table',
      source: { connectionId, kind: 'warehouse-table', path },
      columns, order,
    };
    if (metrics.length > 0) (element as any).metrics = metrics;
    elements.push(element);
    elementMap[modelName] = { elementId, colIdMap };
  }

  // Parse references → relationships
  for (const [modelName, modelDef] of Object.entries(contract.models || {})) {
    const srcEntry = elementMap[modelName];
    if (!srcEntry) continue;
    const srcElement = elements.find(e => e.id === srcEntry.elementId);
    if (!srcElement) continue;

    const fields = (modelDef as any).fields || (modelDef as any).columns || {};
    for (const [fieldName, fieldDef] of Object.entries(fields as Record<string, any>)) {
      const ref: string | null = fieldDef.references || fieldDef.ref || fieldDef['$ref'] || null;
      if (!ref) continue;

      const parts = ref.split('.');
      if (parts.length < 2) {
        warnings.push(`Field "${fieldName}": reference "${ref}" not in model.column format`);
        continue;
      }
      const targetModel = parts[0];
      const targetCol = parts.slice(1).join('.').toUpperCase();
      const tgtEntry = elementMap[targetModel];
      if (!tgtEntry) {
        warnings.push(`Field "${fieldName}": referenced model "${targetModel}" not found`);
        continue;
      }
      const srcColId = srcEntry.colIdMap[fieldName.toUpperCase()];
      const tgtColId = tgtEntry.colIdMap[targetCol];
      if (!srcColId || !tgtColId) {
        warnings.push(`Relationship ${modelName}.${fieldName} → ${ref}: column ID not resolved`);
        continue;
      }
      if (!srcElement.relationships) srcElement.relationships = [];
      srcElement.relationships.push({
        id: sigmaShortId(),
        targetElementId: tgtEntry.elementId,
        keys: [{ sourceColumnId: srcColId, targetColumnId: tgtColId }],
        name: targetModel,
      });
    }
  }

  elements.sort((a, b) => {
    const aR = !!(a.relationships?.length);
    const bR = !!(b.relationships?.length);
    return aR === bR ? 0 : aR ? 1 : -1;
  });

  for (const de of buildDerivedElements(elements)) elements.push(de);

  const modelName: string = contract.id || contract.info?.title || contract.name || 'Data Contract Import';
  const stats = {
    elements: elements.length,
    columns: elements.reduce((n, e) => n + (e.columns?.length || 0), 0),
    metrics: elements.reduce((n, e) => n + ((e as any).metrics?.length || 0), 0),
    relationships: elements.reduce((n, e) => n + (e.relationships?.length || 0), 0),
  };

  return {
    model: {
      name: sigmaDisplayName(modelName.replace(/[-_]/g, ' ')),
      schemaVersion: 1,
      pages: [{ id: sigmaShortId(), name: 'Page 1', elements }],
    },
    warnings,
    stats,
  };
}

function parseContract(text: string): any {
  text = text.trim();
  if (text.startsWith('{')) return JSON.parse(text);
  return yaml.load(text) as any;
}
