/**
 * Tableau Workbook/Data Source XML → Sigma Data Model JSON converter.
 *
 * Handles .twb (workbook) and .tds (data source) XML content.
 * Parses data sources, joins, calculated fields, parameters, LOD expressions,
 * and relationships. Produces Sigma data model JSON.
 */

import { XMLParser } from 'fast-xml-parser';
import {
  resetIds, sigmaShortId, sigmaInodeId, sigmaDisplayName,
  type SigmaElement, type ConversionResult,
} from './sigma-ids.js';
import { tableauFormulaToSigma, tableauIsAggregate } from './formulas.js';

// ── XML Parsing Helpers ──────────────────────────────────────────────────────

const xmlParser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '@_',
  isArray: (name) => ['datasource', 'relation', 'column', 'member', 'clause', 'expression'].includes(name),
  trimValues: true,
});

function asArray(val: any): any[] {
  if (!val) return [];
  return Array.isArray(val) ? val : [val];
}

function attr(node: any, key: string): string {
  return (node && node[`@_${key}`]) || '';
}

// ── LOD Expression Parser ────────────────────────────────────────────────────

interface LODResult {
  _isLOD: true;
  lodType: string;
  dims: string[];
  sigmaAgg: string;
}

function tableauParseLOD(formula: string): LODResult | null {
  const m = formula.match(/^\{\s*(FIXED|INCLUDE|EXCLUDE)\s*(.*?)\s*:\s*(.*?)\s*\}$/is);
  if (!m) return null;
  const lodType = m[1].toUpperCase();
  const rawDims = m[2].trim();
  const rawAgg = m[3].trim();

  const dims: string[] = [];
  if (rawDims) {
    const dimRefs = rawDims.match(/\[([^\]]+)\]/g) || [];
    for (const ref of dimRefs) dims.push(ref.replace(/^\[|\]$/g, ''));
  }

  // Convert the aggregate part
  let sigmaAgg = rawAgg;
  sigmaAgg = sigmaAgg.replace(/\bSUM\s*\(/gi, 'Sum(');
  sigmaAgg = sigmaAgg.replace(/\bAVG\s*\(/gi, 'Avg(');
  sigmaAgg = sigmaAgg.replace(/\bMIN\s*\(/gi, 'Min(');
  sigmaAgg = sigmaAgg.replace(/\bMAX\s*\(/gi, 'Max(');
  sigmaAgg = sigmaAgg.replace(/\bCOUNTD\s*\(/gi, 'CountDistinct(');
  sigmaAgg = sigmaAgg.replace(/\bCOUNT\s*\(([^)]+)\)/gi, 'CountIf(IsNotNull($1))');

  // Convert column refs to display names
  sigmaAgg = sigmaAgg.replace(/\[([A-Z][A-Z0-9_]{2,})\]/g, (_m, colName) => {
    if (colName.includes(' ')) return `[${colName}]`;
    return '[' + sigmaDisplayName(colName) + ']';
  });

  return { _isLOD: true, lodType, dims, sigmaAgg };
}

// ── Path Extraction ──────────────────────────────────────────────────────────

function extractPath(rel: any, dbOverride: string, schOverride: string): string[] {
  const rawTable = attr(rel, 'table') || attr(rel, 'name') || '';
  const parts = rawTable.replace(/[\[\]]/g, '').split('.').filter(Boolean).map((s: string) => s.toUpperCase());

  let path: string[];
  if (parts.length >= 2) {
    path = parts;
  } else if (parts.length === 1) {
    path = [schOverride || 'SCHEMA', parts[0]];
  } else {
    path = [attr(rel, 'name').toUpperCase() || 'UNKNOWN'];
  }

  if (dbOverride) {
    if (path.length >= 3) path[0] = dbOverride;
    else path = [dbOverride, ...path];
  }
  if (schOverride) {
    if (path.length >= 3) path[1] = schOverride;
    else if (path.length === 2) path[0] = schOverride;
  }

  return path;
}

// ── Collect Tables from Join Tree ────────────────────────────────────────────

interface TableEntry {
  rel: any;
  leftKey: string;
  rightKey: string;
  joinType: string;
}

function collectTables(rel: any, tables: TableEntry[]): void {
  const type = attr(rel, 'type') || 'table';

  if (type === 'table') {
    tables.push({ rel, leftKey: '', rightKey: '', joinType: '' });
    return;
  }

  if (type === 'join') {
    const joinType = attr(rel, 'join') || 'left';
    let leftKey = '', rightKey = '';

    // Extract join keys from clause
    const clauses = asArray(rel.clause);
    if (clauses.length > 0) {
      const exprs = asArray(clauses[0].expression);
      // Find the comparison expression (op='=')
      const eqExpr = exprs.find((e: any) => attr(e, 'op') === '=');
      if (eqExpr) {
        const innerExprs = asArray(eqExpr.expression);
        if (innerExprs.length >= 2) {
          leftKey = attr(innerExprs[0], 'op') || '';
          rightKey = attr(innerExprs[1], 'op') || '';
        }
      }
    }

    const childRels = asArray(rel.relation);
    if (childRels.length === 2) {
      collectTables(childRels[0], tables);
      const beforeRight = tables.length;
      collectTables(childRels[1], tables);
      for (let i = beforeRight; i < tables.length; i++) {
        if (!tables[i].leftKey) {
          tables[i].joinType = joinType;
          tables[i].leftKey = leftKey;
          tables[i].rightKey = rightKey;
        }
      }
    } else {
      for (const child of childRels) {
        collectTables(child, tables);
      }
    }
  }
}

// ── Main Conversion ──────────────────────────────────────────────────────────

export interface TableauConvertOptions {
  connectionId?: string;
  database?: string;
  schema?: string;
  datasourceIndex?: number;
}

export function convertTableauToSigma(
  xmlContent: string,
  options: TableauConvertOptions = {}
): ConversionResult {
  resetIds();

  const { connectionId = '', database = '', schema = '', datasourceIndex = 0 } = options;
  const dbOverride = (database || '').toUpperCase();
  const schOverride = (schema || '').toUpperCase();

  // Parse XML
  let parsed: any;
  try {
    parsed = xmlParser.parse(xmlContent);
  } catch (e: any) {
    throw new Error('XML parse error: ' + e.message);
  }

  // Support both .twb (root=<workbook>) and .tds (root=<datasource>)
  let allDs: any[];
  if (parsed.workbook) {
    allDs = asArray(parsed.workbook?.datasources?.datasource || []);
  } else if (parsed.datasource) {
    allDs = asArray(parsed.datasource);
  } else {
    throw new Error('Unrecognized XML — expected <workbook> or <datasource> root element');
  }

  const parameters: any[] = [];
  const datasources: any[] = [];

  for (const ds of allDs) {
    if (attr(ds, 'hasconnection') === 'false' || attr(ds, 'name') === 'Parameters') {
      // Parse parameters
      for (const col of asArray(ds.column)) {
        const colName = attr(col, 'caption') || attr(col, 'name') || '';
        const colType = attr(col, 'datatype') || 'string';
        const domainType = attr(col, 'param-domain-type') || 'all';
        const members = asArray(col.member).map((m: any) => attr(m, 'value')).filter(Boolean);
        const calcEl = col.calculation;
        parameters.push({
          name: colName.replace(/^\[|\]$/g, ''),
          type: colType,
          domainType,
          members,
          defaultVal: calcEl ? attr(calcEl, 'formula') : ''
        });
      }
      continue;
    }

    const name = attr(ds, 'caption') || attr(ds, 'name') || 'Unnamed';
    const connection = ds.connection;
    const connClass = connection ? attr(connection, 'class') : '';
    const dbname = connection ? (attr(connection, 'dbname') || attr(connection, 'database')) : '';
    const schemaName = connection ? attr(connection, 'schema') : '';
    datasources.push({ name, ds, connection, connClass, dbname, schema: schemaName });
  }

  if (datasources.length === 0) {
    throw new Error('No data sources found in the Tableau file');
  }

  const dsIdx = Math.min(datasourceIndex, datasources.length - 1);
  const ds = datasources[dsIdx];
  const warnings: string[] = [];
  const elements: SigmaElement[] = [];
  const connId = connectionId || '<CONNECTION_ID>';

  // ── Build elements from relation structure ──────────────────────────────
  const rootRelation = ds.connection ? asArray(ds.connection.relation || [])[0] : null;

  if (rootRelation) {
    const relType = attr(rootRelation, 'type') || 'table';

    if (relType === 'table') {
      const path = extractPath(rootRelation, dbOverride, schOverride);
      const tableName = path[path.length - 1] || '';
      const columns: any[] = [], order: string[] = [];
      for (const col of asArray(rootRelation?.columns?.column || [])) {
        const key = attr(col, 'name').toUpperCase();
        if (!key) continue;
        const id = sigmaInodeId(key);
        columns.push({ id, formula: `[${tableName}/${sigmaDisplayName(key)}]` });
        order.push(id);
      }
      elements.push({ id: sigmaShortId(), kind: 'table',
        source: { connectionId: connId, kind: 'warehouse-table', path },
        columns, order } as any);

    } else if (relType === 'join') {
      const tables: TableEntry[] = [];
      collectTables(rootRelation, tables);

      if (tables.length === 0) {
        warnings.push('⚠ Could not parse join structure');
      } else {
        const elementMap: Record<string, { element: any; colIdMap: Record<string, string> }> = {};

        for (const t of tables) {
          const path = extractPath(t.rel, dbOverride, schOverride);
          const tableName = path[path.length - 1] || attr(t.rel, 'name') || '';
          if (elementMap[tableName]) continue;

          const columns: any[] = [], order: string[] = [];
          for (const col of asArray(t.rel?.columns?.column || [])) {
            const key = attr(col, 'name').toUpperCase();
            if (!key) continue;
            const id = sigmaInodeId(key);
            columns.push({ id, formula: `[${tableName}/${sigmaDisplayName(key)}]` });
            order.push(id);
          }

          const elemId = sigmaShortId();
          const el: any = { id: elemId, kind: 'table',
            source: { connectionId: connId, kind: 'warehouse-table', path },
            columns, order };
          const colIdMap: Record<string, string> = {};
          columns.forEach((c: any) => {
            const m = c.formula.match(/\/([^\]]+)\]$/);
            if (m) {
              colIdMap[m[1].toUpperCase()] = c.id;
              colIdMap[m[1].replace(/\s+/g, '_').toUpperCase()] = c.id;
            }
          });
          elementMap[tableName] = { element: el, colIdMap };
          elements.push(el);
        }

        // Wire relationships
        const primaryTableName = extractPath(tables[0].rel, dbOverride, schOverride).pop() || '';
        const primaryEntry = elementMap[primaryTableName];

        for (let i = 1; i < tables.length; i++) {
          const t = tables[i];
          if (!t.leftKey || !t.rightKey) continue;
          const leftKey = t.leftKey.replace(/^\[|\]$/g, '').split(/[\.\]]\[?/).pop()?.replace(/\]$/, '').toUpperCase() || '';
          const rightKey = t.rightKey.replace(/^\[|\]$/g, '').split(/[\.\]]\[?/).pop()?.replace(/\]$/, '').toUpperCase() || '';
          const tgtName = extractPath(t.rel, dbOverride, schOverride).pop() || '';
          const tgtEntry = elementMap[tgtName];
          if (!primaryEntry || !tgtEntry) continue;

          let srcColId = primaryEntry.colIdMap[leftKey] || primaryEntry.colIdMap[sigmaDisplayName(leftKey).toUpperCase()];
          if (!srcColId) {
            srcColId = sigmaInodeId(leftKey);
            primaryEntry.element.columns.push({ id: srcColId, formula: `[${primaryTableName}/${sigmaDisplayName(leftKey)}]` });
            primaryEntry.element.order.push(srcColId);
            primaryEntry.colIdMap[leftKey] = srcColId;
          }

          let tgtColId = tgtEntry.colIdMap[rightKey] || tgtEntry.colIdMap[sigmaDisplayName(rightKey).toUpperCase()];
          if (!tgtColId) {
            tgtColId = sigmaInodeId(rightKey);
            tgtEntry.element.columns.push({ id: tgtColId, formula: `[${tgtName}/${sigmaDisplayName(rightKey)}]` });
            tgtEntry.element.order.push(tgtColId);
            tgtEntry.colIdMap[rightKey] = tgtColId;
          }

          if (!primaryEntry.element.relationships) primaryEntry.element.relationships = [];
          primaryEntry.element.relationships.push({
            id: sigmaShortId(),
            targetElementId: tgtEntry.element.id,
            keys: [{ sourceColumnId: srcColId, targetColumnId: tgtColId }],
            name: tgtName
          });
          warnings.push(`ℹ Join ${primaryTableName} → ${tgtName} (${t.joinType || 'left'}) on ${leftKey} = ${rightKey}`);
        }

        // Sort: dims first, fact last
        elements.sort((a, b) => {
          const aR = !!((a as any).relationships?.length);
          const bR = !!((b as any).relationships?.length);
          return aR === bR ? 0 : aR ? 1 : -1;
        });
      }
    }
  }

  // ── Process calculated fields ───────────────────────────────────────────
  const factEl = elements.find(e => (e as any).relationships?.length > 0)
    || (elements.length > 0 ? elements.reduce((best, e) =>
      (e.columns?.length || 0) > (best.columns?.length || 0) ? e : best, elements[0]) : null);

  if (factEl) {
    // Build display name maps
    const globalColMap: Record<string, { elId: string; displayName: string }> = {};
    const displayNameMap: Record<string, { colId: string; el: any }> = {};

    for (const el of elements) {
      for (const c of (el.columns || [])) {
        const fm = c.formula.match(/\/([^\]]+)\]$/);
        if (fm) {
          const dn = fm[1];
          globalColMap[dn.toUpperCase()] = { elId: el.id, displayName: dn };
          displayNameMap[dn.toUpperCase()] = { colId: c.id, el };
          displayNameMap[dn.replace(/\s+/g, '_').toUpperCase()] = { colId: c.id, el };
        }
        if (c.name) displayNameMap[c.name.toUpperCase()] = { colId: c.id, el };
      }
    }

    const factTableName = (factEl.source?.path?.[factEl.source.path.length - 1]) || 'FACT';
    const lodChildElements: any[] = [];

    for (const col of asArray(ds.ds?.column || [])) {
      const rawName = attr(col, 'name') || '';
      const caption = attr(col, 'caption') || rawName.replace(/^\[|\]$/g, '');
      const hidden = attr(col, 'hidden') === 'true';
      const calcEl = col.calculation;
      const formula = calcEl ? attr(calcEl, 'formula') : '';
      const fieldKey = rawName.replace(/^\[|\]$/g, '');

      if (hidden || !fieldKey || fieldKey.startsWith('Number of Records')) continue;

      if (formula) {
        // Check for LOD expression
        const lod = tableauParseLOD(formula);
        if (lod) {
          if (lod.lodType === 'FIXED' && lod.dims.length > 0) {
            const dimInfos: { displayName: string }[] = [];
            let allFound = true;
            for (const dimName of lod.dims) {
              const found = displayNameMap[dimName.toUpperCase()]
                || displayNameMap[sigmaDisplayName(dimName).toUpperCase()];
              if (found) {
                const parentCol = found.el.columns?.find((c: any) => c.id === found.colId);
                const dn = parentCol?.name || (parentCol?.formula.match(/\/([^\]]+)\]$/)?.[1]) || dimName;
                dimInfos.push({ displayName: dn });
              } else {
                allFound = false;
                warnings.push(`⚠ LOD "${caption}": dimension [${dimName}] not found`);
              }
            }

            if (allFound && dimInfos.length > 0) {
              const dimKey = dimInfos.map(d => d.displayName).sort().join(',');
              let existingChild = lodChildElements.find((ce: any) => ce._dimKey === dimKey);

              if (existingChild) {
                // Add referenced columns
                const existingFormulas = new Set(existingChild.columns.map((c: any) => c.formula));
                for (const ref of (lod.sigmaAgg.match(/\[([^\]]+)\]/g) || [])) {
                  const colName = ref.replace(/^\[|\]$/g, '');
                  const refFormula = `[${factTableName}/${colName}]`;
                  if (!existingFormulas.has(refFormula)) {
                    existingFormulas.add(refFormula);
                    const refColId = sigmaShortId();
                    existingChild.columns.push({ id: refColId, formula: refFormula });
                    existingChild.order.push(refColId);
                  }
                }
                const calcId = sigmaShortId();
                existingChild.columns.push({ id: calcId, formula: lod.sigmaAgg, name: caption });
                existingChild.order.push(calcId);
                existingChild.groupings[0].calculations.push(calcId);
              } else {
                // Create new child element
                const childCols: any[] = [], childOrder: string[] = [], groupByIds: string[] = [];
                for (const di of dimInfos) {
                  const colId = sigmaShortId();
                  childCols.push({ id: colId, formula: `[${factTableName}/${di.displayName}]` });
                  childOrder.push(colId);
                  groupByIds.push(colId);
                }
                // Add referenced columns
                const addedNames = new Set(dimInfos.map(d => d.displayName.toUpperCase()));
                for (const ref of (lod.sigmaAgg.match(/\[([^\]]+)\]/g) || [])) {
                  const colName = ref.replace(/^\[|\]$/g, '');
                  if (addedNames.has(colName.toUpperCase())) continue;
                  addedNames.add(colName.toUpperCase());
                  const refColId = sigmaShortId();
                  childCols.push({ id: refColId, formula: `[${factTableName}/${colName}]` });
                  childOrder.push(refColId);
                }
                const calcId = sigmaShortId();
                childCols.push({ id: calcId, formula: lod.sigmaAgg, name: caption });
                childOrder.push(calcId);

                lodChildElements.push({
                  id: sigmaShortId(), kind: 'table',
                  name: `${factTableName} by ${dimInfos.map(d => d.displayName).join(', ')}`,
                  source: { elementId: factEl.id, kind: 'table' },
                  columns: childCols,
                  groupings: [{ id: sigmaShortId(), groupBy: groupByIds, calculations: [calcId] }],
                  order: childOrder,
                  _dimKey: dimKey
                });
              }
              warnings.push(`✅ LOD "${caption}" → child element with grouping: ${lod.sigmaAgg}`);
            }
          } else if (lod.lodType === 'FIXED' && lod.dims.length === 0) {
            // Table-scoped: add as metric
            if (!(factEl as any).metrics) (factEl as any).metrics = [];
            (factEl as any).metrics.push({ id: sigmaShortId(), formula: lod.sigmaAgg, name: caption });
            warnings.push(`ℹ LOD "${caption}" (table-scoped FIXED) → metric: ${lod.sigmaAgg}`);
          } else {
            warnings.push(`⚠ LOD "${caption}" (${lod.lodType}) → manual conversion needed. See: community.sigmacomputing.com/t/tableau-level-of-detail-or-lod-calculations-in-sigma/6427`);
          }
          continue;
        }

        // Regular calculated field
        const sigmaFormula = tableauFormulaToSigma(formula, warnings);
        if (!sigmaFormula || sigmaFormula.startsWith('/*')) continue;

        if (tableauIsAggregate(formula)) {
          if (!(factEl as any).metrics) (factEl as any).metrics = [];
          (factEl as any).metrics.push({ id: sigmaShortId(), formula: sigmaFormula, name: caption });
        } else {
          const colId = sigmaShortId();
          factEl.columns.push({ id: colId, formula: sigmaFormula, name: caption });
          factEl.order.push(colId);
          warnings.push(`ℹ "${caption}" → calculated column. Review: ${sigmaFormula.slice(0, 60)}`);
        }
      }
    }

    // Add LOD child elements
    for (const child of lodChildElements) {
      delete child._dimKey;
      elements.push(child);
    }
    if (lodChildElements.length > 0) {
      warnings.push(`ℹ ${lodChildElements.length} child element(s) created from LOD expressions`);
    }
  }

  // ── Auto-fix cross-element column references with - link/ syntax ────────
  const gColMap: Record<string, { elId: string; displayName: string }> = {};
  for (const el of elements) {
    for (const c of (el.columns || [])) {
      const fm = c.formula.match(/\[([^\/\]]+)\/([^\]]+)\]$/);
      if (fm) gColMap[fm[2].toUpperCase()] = { elId: el.id, displayName: fm[2] };
    }
  }
  for (const el of elements) {
    const localNames = new Set<string>();
    for (const c of (el.columns || [])) {
      if (c.name) localNames.add(c.name.toUpperCase());
      const fm = c.formula.match(/\/([^\]]+)\]$/);
      if (fm) localNames.add(fm[1].toUpperCase());
    }
    const relFkLookup: Record<string, string> = {};
    const elTbl = el.source?.path?.[el.source.path.length - 1] || 'UNKNOWN';
    for (const rel of ((el as any).relationships || [])) {
      const fkCol = (el.columns || []).find(c => c.id === rel.keys[0]?.sourceColumnId);
      if (fkCol) {
        const fkM = fkCol.formula.match(/\/([^\]]+)\]$/);
        if (fkM) relFkLookup[rel.targetElementId] = fkM[1].replace(/\s+/g, '_').toUpperCase();
      }
    }
    for (const c of (el.columns || [])) {
      if (!c.name || !c.formula) continue;
      if (c.formula.match(/^\[[\w_]+\//)) continue;
      if (c.formula.includes('- link/')) continue;
      const refs = c.formula.match(/\[([^\]\/]+)\]/g) || [];
      let fixedFormula = c.formula;
      let wasFixed = false;
      for (const ref of refs) {
        const rn = ref.replace(/^\[|\]$/g, '');
        if (localNames.has(rn.toUpperCase()) || rn.toUpperCase() === 'TRUE' || rn.toUpperCase() === 'FALSE') continue;
        const ge = gColMap[rn.toUpperCase()];
        if (ge && relFkLookup[ge.elId]) {
          fixedFormula = fixedFormula.replace(ref, `[${elTbl}/${relFkLookup[ge.elId]} - link/${ge.displayName}]`);
          wasFixed = true;
        }
      }
      if (wasFixed) {
        c.formula = fixedFormula;
        warnings.push(`✅ "${c.name}" → linked column: ${fixedFormula.slice(0, 100)}`);
        warnings.push(`   ⚠ Note: Sigma API may not round-trip linked columns correctly yet.`);
      }
    }
  }

  // ── Parameters → Controls ───────────────────────────────────────────────
  const controls: any[] = [];
  for (const p of parameters) {
    const controlId = sigmaDisplayName(p.name).replace(/\s+/g, '-');
    if (p.domainType === 'list' && p.members.length > 0) {
      controls.push({ kind: 'control', controlId, id: sigmaShortId() + 'con',
        controlType: 'list', mode: 'include', selectionMode: 'single', values: [],
        source: { kind: 'manual', valueType: 'text', values: p.members } });
      warnings.push(`ℹ Parameter "${p.name}" → list control (${p.members.length} values)`);
    } else if (p.type === 'date' || p.type === 'datetime') {
      controls.push({ kind: 'control', controlId, id: sigmaShortId() + 'con',
        controlType: 'date-range', mode: 'between', includeNulls: 'when-no-value-is-selected' });
      warnings.push(`ℹ Parameter "${p.name}" → date-range control`);
    } else {
      controls.push({ kind: 'control', controlId, id: sigmaShortId() + 'con',
        controlType: 'text', mode: 'include', values: [] });
      warnings.push(`ℹ Parameter "${p.name}" → text control`);
    }
  }

  // ── Build output ────────────────────────────────────────────────────────
  if (!connectionId) warnings.unshift('⚠ Connection ID not set — update in JSON before saving to Sigma');

  const sigmaModel = {
    name: ds.name,
    pages: [{ id: sigmaShortId(), name: 'Page 1', elements: [...controls, ...elements] }]
  };

  const totalCols = elements.reduce((s, e) => s + (e.columns?.length || 0), 0);
  const totalMetrics = elements.reduce((s, e) => s + ((e as any).metrics?.length || 0), 0);
  const totalRels = elements.reduce((s, e) => s + ((e as any).relationships?.length || 0), 0);

  return {
    model: sigmaModel,
    warnings,
    stats: {
      datasources: datasources.length,
      elements: elements.length,
      columns: totalCols,
      metrics: totalMetrics,
      relationships: totalRels,
      controls: controls.length,
      parameters: parameters.length,
      lodChildElements: elements.filter(e => e.source?.kind === 'table' && e.source?.elementId).length,
    }
  };
}
