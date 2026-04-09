/**
 * LookML → Sigma Data Model JSON converter.
 * Includes a full LookML parser and explore-based conversion.
 */

import {
  resetIds, sigmaShortId, sigmaInodeId, sigmaDisplayName,
  type SigmaElement, type ConversionResult, type ElementResult
} from './sigma-ids.js';
import { lookIsComplexSql, lookSqlToSigmaRules, lookStripSql, lookSigmaMetric, detectUnsupportedSigmaFunction } from './formulas.js';

// ── LookML Parser ────────────────────────────────────────────────────────────

interface LookMLParseResult {
  views: any[];
  explores: any[];
  connection: string | null;
  label: string | null;
}

function restoreSqlPlaceholders(obj: any, map: Record<string, string>): void {
  if (!obj || typeof obj !== 'object') return;
  for (const key of Object.keys(obj)) {
    const v = obj[key];
    if (typeof v === 'string' && map[v] !== undefined) {
      obj[key] = map[v];
    } else if (typeof v === 'object') {
      restoreSqlPlaceholders(v, map);
    }
  }
}

export function parseLookML(text: string): LookMLParseResult {
  // Strip line comments
  text = text.replace(/#[^\n]*/g, '');

  // Pre-extract raw sql: ... ;; blocks
  const sqlPlaceholders: Record<string, string> = {};
  let phIdx = 0;
  text = text.replace(/\bsql\s*:([\s\S]*?);;/g, (match, sqlContent) => {
    const key = `__SQLPH${phIdx++}__`;
    sqlPlaceholders[key] = sqlContent.trim();
    return `sql: "${key}" ;;`;
  });

  const tokens: string[] = [];
  const re = /;;;?|\$\{[^}]*\}|[\[\]{}]|"(?:[^"\\]|\\.)*"|[^\s\[\]{}:;,"]+|:/g;
  let m;
  while ((m = re.exec(text)) !== null) tokens.push(m[0]);

  let pos = 0;
  const peek = (n?: number) => tokens[pos + (n || 0)];
  const consume = () => tokens[pos++];

  const NAMED_BLOCK_KEYS = new Set([
    'dimension', 'measure', 'dimension_group', 'filter', 'parameter',
    'join', 'set', 'link', 'action', 'form_param', 'option'
  ]);

  const SQL_KEYS = new Set(['sql', 'sql_on', 'sql_where', 'sql_table_name',
    'sql_trigger_value', 'html', 'label_from_parameter']);

  function parseBlock(): any {
    const obj: any = {};
    while (pos < tokens.length) {
      const t = peek();
      if (t === '}') { consume(); break; }
      if (t === undefined) break;

      const key = consume();
      if (peek() !== ':') continue;
      consume(); // eat ':'

      const a0 = peek();
      const a1 = peek(1);

      if (SQL_KEYS.has(key)) {
        const parts: string[] = [];
        while (pos < tokens.length &&
          peek() !== ';;' && peek() !== ';;;' &&
          peek() !== '}') {
          parts.push(consume());
        }
        if (peek() === ';;' || peek() === ';;;') consume();
        let val = parts.join(' ').trim();
        if (val.startsWith('"') && val.endsWith('"')) val = val.slice(1, -1);
        obj[key] = val;

      } else if (NAMED_BLOCK_KEYS.has(key) && a0 && a0 !== '{' && a1 === '{') {
        const name = consume().replace(/"/g, '');
        consume(); // eat '{'
        const child = parseBlock();
        child._name = name;
        if (obj[key] !== undefined) {
          if (!Array.isArray(obj[key])) obj[key] = [obj[key]];
          obj[key].push(child);
        } else {
          obj[key] = [child];
        }

      } else if (a0 === '{') {
        consume();
        const child = parseBlock();
        if (obj[key] !== undefined) {
          if (!Array.isArray(obj[key])) obj[key] = [obj[key]];
          obj[key].push(child);
        } else {
          obj[key] = child;
        }

      } else if (a0 === ';;' || a0 === ';;;') {
        consume(); obj[key] = '';

      } else if (a0 === '[') {
        // Bracket array: [key: "val", ...] or ["val1", "val2"]
        consume(); // eat '['
        const items: any[] = [];
        while (pos < tokens.length && peek() !== ']') {
          const t1 = consume();
          if (t1 === undefined) break;
          if (peek() === ':') {
            consume(); // eat ':'
            let val = consume() || '';
            if (val.startsWith('"') && val.endsWith('"')) val = val.slice(1, -1);
            items.push({ field: t1.replace(/"/g, ''), value: val });
          } else {
            let val = t1;
            if (val.startsWith('"') && val.endsWith('"')) val = val.slice(1, -1);
            items.push(val);
          }
        }
        if (peek() === ']') consume();
        if (peek() === ';;' || peek() === ';;;') consume();
        obj[key] = items;

      } else {
        let val = consume() || '';
        if (val.startsWith('"') && val.endsWith('"')) val = val.slice(1, -1);
        if (peek() === ';;' || peek() === ';;;') consume();
        obj[key] = val;
      }
    }
    return obj;
  }

  const result: LookMLParseResult = { views: [], explores: [], connection: null, label: null };
  while (pos < tokens.length) {
    const keyword = consume();
    if (!keyword) continue;
    if ((keyword === 'view' || keyword === 'explore') && peek() === ':') {
      consume();
      const name = (consume() || '').replace(/"/g, '');
      if (peek() === '{') {
        consume();
        const block = parseBlock();
        block._name = name;
        result[keyword + 's' as 'views' | 'explores'].push(block);
      }
    } else if (keyword === 'connection' && peek() === ':') {
      consume(); result.connection = (consume() || '').replace(/"/g, '');
    } else if (keyword === 'label' && peek() === ':') {
      consume(); result.label = (consume() || '').replace(/"/g, '');
    } else if (peek() === ':') {
      consume();
      if (peek() === '{') { consume(); parseBlock(); }
      else { consume(); if (peek() === ';;' || peek() === ';;;') consume(); }
    }
  }
  restoreSqlPlaceholders(result, sqlPlaceholders);
  return result;
}

// ── LookML View → Sigma Element Conversion ───────────────────────────────────

function lookExtractPath(view: any): string[] {
  const raw = (view.sql_table_name || view.from || '').trim().replace(/`/g, '');
  if (!raw) return [];
  return raw.split('.').map((p: string) => p.trim().toUpperCase()).filter(Boolean);
}

function lookFindColId(elementResult: ElementResult, colName: string): string | null {
  if (!elementResult) return null;
  const upper = (colName || '').toUpperCase();
  return elementResult.colIdMap[upper] || null;
}

function lookConvertView(
  viewName: string,
  view: any,
  connectionId: string,
  warnings: string[]
): ElementResult {
  if (!view) {
    warnings.push(`⚠ View "${viewName}" not found — element will have no columns`);
    const id = sigmaShortId();
    return {
      element: { id, kind: 'table', source: { connectionId: connectionId || '<CONNECTION_ID>', kind: 'warehouse-table', path: [viewName.toUpperCase()] }, columns: [], order: [] },
      elementId: id,
      colIdMap: {}
    };
  }

  const elementId = sigmaShortId();
  let tableName: string, element: SigmaElement;

  if (view.derived_table !== undefined) {
    const rawSql = (view.derived_table.sql || '').replace(/;;\s*$/, '').trim();
    tableName = 'Custom SQL';
    element = {
      id: elementId,
      kind: 'table',
      source: {
        connectionId: connectionId || '<CONNECTION_ID>',
        statement: rawSql || '',
        kind: 'sql'
      },
      columns: [],
      metrics: [],
      order: []
    };
    if (rawSql) warnings.push(`ℹ View "${viewName}" → Custom SQL element. Review the SQL before saving.`);
    else warnings.push(`⚠ View "${viewName}" derived_table has no sql — SQL statement left blank. Add SQL manually in the JSON before saving.`);
  } else {
    const path = lookExtractPath(view);
    tableName = (path[path.length - 1] || viewName).toUpperCase();
    element = {
      id: elementId,
      kind: 'table',
      source: {
        connectionId: connectionId || '<CONNECTION_ID>',
        kind: 'warehouse-table',
        path: path.length > 0 ? path : [viewName.toUpperCase()]
      },
      columns: [],
      metrics: [],
      order: []
    };
  }

  const colIdMap: Record<string, string> = {};
  const isCustomSql = (tableName === 'Custom SQL');
  const colLabel = (physCol: string) => isCustomSql ? physCol : sigmaDisplayName(physCol);
  // Warehouse columns get deterministic inode- IDs; calculated/SQL columns get short random IDs
  const makeColId = (physCol: string) => isCustomSql ? sigmaShortId() : sigmaInodeId(physCol);

  // Dimensions
  const dims = view.dimension ? (Array.isArray(view.dimension) ? view.dimension : [view.dimension]) : [];
  for (const d of dims) {
    if (!d._name) continue;
    const colName = d._name.toUpperCase();

    if (lookIsComplexSql(d.sql)) {
      const cleanedSql = (d.sql || '').replace(/\$\{TABLE\}\./gi, '').replace(/\$\{[^.}]+\.([^}]+)\}/g, '$1').trim();
      const boolMatch = cleanedSql.match(/^([A-Z_][A-Z0-9_]*)\s*=\s*(\d+)$/i);

      if (boolMatch) {
        const physicalCol = boolMatch[1].toUpperCase();
        const val = boolMatch[2];
        let physColId = colIdMap[physicalCol];
        if (!physColId) {
          physColId = makeColId(physicalCol);
          colIdMap[physicalCol] = physColId;
          element.columns.push({ id: physColId, formula: `[${tableName}/${colLabel(physicalCol)}]` });
          element.order.push(physColId);
        }
        const calcId = sigmaShortId();
        colIdMap[colName] = calcId;
        const baseName = d.label || sigmaDisplayName(d._name);
        const displayName = baseName + ' (T/F)';
        element.columns.push({ id: calcId, formula: `[${colLabel(physicalCol)}] = ${val}`, name: displayName });
        element.order.push(calcId);
        continue;
      }

      const unsupported = detectUnsupportedSigmaFunction(d.sql || '');
      if (unsupported) {
        warnings.push(`⚠ "${d._name}": skipped — contains ${unsupported}() which has no Sigma equivalent. Add this column manually in the Sigma UI.`);
        continue;
      }
      const colId = sigmaShortId();
      colIdMap[colName] = colId;
      let sigmaFormula = lookSqlToSigmaRules(d.sql);
      if (sigmaFormula) {
        element.columns.push({ id: colId, formula: sigmaFormula, name: d.label || sigmaDisplayName(d._name) });
        element.order.push(colId);
        warnings.push(`ℹ "${d._name}" → calculated column: ${sigmaFormula}`);
      } else {
        element.columns.push({ id: colId, formula: `[${tableName}/${sigmaDisplayName(colName)}]`, name: d.label || sigmaDisplayName(d._name) });
        element.order.push(colId);
        warnings.push(`⚠ "${d._name}": could not auto-convert. Edit formula manually.`);
      }
      continue;
    }

    const sqlCol = lookStripSql(d.sql) || colName;
    const physicalCol = sqlCol.split('.').pop()!.replace(/"/g, '').toUpperCase();

    // Dedup: if physical column already exists, just map the dimension name
    if (colIdMap[physicalCol]) {
      colIdMap[colName] = colIdMap[physicalCol];
      continue;
    }

    const colId = makeColId(physicalCol);
    colIdMap[colName] = colId;
    colIdMap[physicalCol] = colId;
    element.columns.push({ id: colId, formula: `[${tableName}/${colLabel(physicalCol)}]` });
    element.order.push(colId);
  }

  // Dimension groups (time)
  const dimGroups = view.dimension_group ? (Array.isArray(view.dimension_group) ? view.dimension_group : [view.dimension_group]) : [];
  dimGroups.forEach((dg: any) => {
    if (!dg._name) return;
    const colName = dg._name.toUpperCase();
    if (lookIsComplexSql(dg.sql)) {
      warnings.push(`⚠ Dimension group "${dg._name}": complex expression — skipped.`);
      return;
    }
    const sqlCol = lookStripSql(dg.sql) || colName;
    const physicalCol = sqlCol.split('.').pop()!.replace(/"/g, '').toUpperCase();
    const colId = makeColId(physicalCol);
    colIdMap[colName] = colId;
    colIdMap[physicalCol] = colId;
    element.columns.push({ id: colId, formula: `[${tableName}/${colLabel(physicalCol)}]` });
    element.order.push(colId);
  });

  // Measures → metrics
  const measures = view.measure ? (Array.isArray(view.measure) ? view.measure : [view.measure]) : [];
  const CALC_COL_MEASURE_TYPES = new Set(['running_total', 'percent_of_total']);

  measures.forEach((ms: any) => {
    if (!ms._name) return;
    const msName = ms._name.toUpperCase();
    const sqlCol = lookStripSql(ms.sql) || msName;
    const physicalCol = sqlCol.split('.').pop()!.replace(/"/g, '').toUpperCase() || msName.replace(/"/g, '');
    const msType = (ms.type || 'count').toLowerCase();
    const msLabel = ms.label || sigmaDisplayName(msName);

    // running_total / percent_of_total → calculated columns
    if (CALC_COL_MEASURE_TYPES.has(msType)) {
      if (!colIdMap[physicalCol]) {
        const colId = makeColId(physicalCol);
        colIdMap[physicalCol] = colId;
        element.columns.push({ id: colId, formula: `[${tableName}/${colLabel(physicalCol)}]` });
        element.order.push(colId);
      }
      const dn = colLabel(physicalCol);
      const calcId = sigmaShortId();
      if (msType === 'running_total') {
        element.columns.push({ id: calcId, formula: `CumulativeSum([${dn}])`, name: msLabel });
        warnings.push(`✅ "${ms._name}" (running_total) → CumulativeSum([${dn}])`);
      } else {
        element.columns.push({ id: calcId, formula: `Sum([${dn}]) / GrandTotal(Sum([${dn}]))`, name: msLabel });
        warnings.push(`✅ "${ms._name}" (percent_of_total) → Sum/GrandTotal`);
      }
      element.order.push(calcId);
      return;
    }

    // Filtered measures → conditional aggregates
    if (ms.filters && (Array.isArray(ms.filters) ? ms.filters.length : false)) {
      const filters = Array.isArray(ms.filters) ? ms.filters : [];
      const conditions: string[] = [];
      for (const f of filters) {
        if (typeof f !== 'object' || !f) continue;
        const fField = f.field || f._name;
        const fVal = f.value;
        if (fField && fVal) {
          const cleanField = fField.replace(/^.*\./, '').toUpperCase();
          const dn = colLabel(cleanField);
          if (!colIdMap[cleanField]) {
            const colId = makeColId(cleanField);
            colIdMap[cleanField] = colId;
            element.columns.push({ id: colId, formula: `[${tableName}/${dn}]` });
            element.order.push(colId);
          }
          if (fVal === 'yes' || fVal === 'true') conditions.push(`[${dn}] = True`);
          else if (fVal === 'no' || fVal === 'false') conditions.push(`[${dn}] = False`);
          else conditions.push(`[${dn}] = "${fVal}"`);
        }
      }
      if (conditions.length > 0) {
        const condition = conditions.length === 1 ? conditions[0] : conditions.map(c => `(${c})`).join(' And ');
        if (!colIdMap[physicalCol]) {
          const colId = makeColId(physicalCol);
          colIdMap[physicalCol] = colId;
          element.columns.push({ id: colId, formula: `[${tableName}/${colLabel(physicalCol)}]` });
          element.order.push(colId);
        }
        const dn = colLabel(physicalCol);
        const condAggMap: Record<string, string> = {
          sum: `SumIf([${dn}], ${condition})`, count: `CountIf(${condition})`,
          count_distinct: `CountDistinctIf([${dn}], ${condition})`, average: `AvgIf([${dn}], ${condition})`,
          max: `MaxIf([${dn}], ${condition})`, min: `MinIf([${dn}], ${condition})`,
        };
        const formula = condAggMap[msType] || `SumIf([${dn}], ${condition})`;
        element.metrics!.push({ id: sigmaShortId(), formula, name: msLabel });
        warnings.push(`✅ Filtered "${ms._name}" → ${formula.slice(0, 60)}`);
        return;
      }
      warnings.push(`⚠ "${ms._name}": filters not parsed — metric created without filter`);
    }

    if (msType === 'count') {
      element.metrics!.push({ id: sigmaShortId(), formula: 'Count()', name: msLabel });
    } else if (msType === 'count_distinct') {
      const cdCol = physicalCol && physicalCol !== msName ? physicalCol : msName;
      if (!colIdMap[cdCol]) {
        const colId = makeColId(cdCol);
        colIdMap[cdCol] = colId;
        element.columns.push({ id: colId, formula: `[${tableName}/${colLabel(cdCol)}]` });
        element.order.push(colId);
      }
      element.metrics!.push({ id: sigmaShortId(), formula: `CountDistinct([${colLabel(cdCol)}])`, name: msLabel });
    } else {
      if (!colIdMap[physicalCol]) {
        const colId = makeColId(physicalCol);
        colIdMap[physicalCol] = colId;
        element.columns.push({ id: colId, formula: `[${tableName}/${colLabel(physicalCol)}]` });
        element.order.push(colId);
      }
      element.metrics!.push({ id: sigmaShortId(), formula: lookSigmaMetric(msType, colLabel(physicalCol)), name: msLabel });
    }
  });

  if (element.metrics!.length === 0) delete element.metrics;
  return { element, elementId, colIdMap };
}

// ── Main LookML Conversion ───────────────────────────────────────────────────

export interface LookMLConvertOptions {
  connectionId?: string;
  exploreName?: string;
  joinStrategy?: 'relationships' | 'joins' | 'auto';
}

export function convertLookMLToSigma(
  files: { name: string; content: string }[],
  options: LookMLConvertOptions = {}
): ConversionResult {
  resetIds();

  const { connectionId = '', joinStrategy = 'auto' } = options;

  // Parse all files
  const views: Record<string, any> = {};
  const explores: Record<string, any> = {};

  for (const file of files) {
    const isModel = file.name.endsWith('.model.lkml') || file.name.includes('.model.');
    try {
      const parsed = parseLookML(file.content);
      if (isModel) {
        parsed.explores.forEach((ex: any) => { explores[ex._name] = ex; });
      }
      parsed.views.forEach((v: any) => { views[v._name] = v; });
    } catch (e: any) {
      throw new Error(`Parse error in ${file.name}: ${e.message}`);
    }
  }

  // Determine which explore to convert
  let exploreName = options.exploreName;
  const exploreNames = Object.keys(explores);
  if (!exploreName) {
    if (exploreNames.length === 1) exploreName = exploreNames[0];
    else if (exploreNames.length === 0) throw new Error('No explores found in the LookML files. Upload a .model.lkml file.');
    else throw new Error(`Multiple explores found: ${exploreNames.join(', ')}. Specify exploreName.`);
  }

  const explore = explores[exploreName];
  if (!explore) throw new Error(`Explore "${exploreName}" not found. Available: ${exploreNames.join(', ')}`);

  const warnings: string[] = [];
  const strategy = joinStrategy;

  // Build join list
  const baseViewName = explore.from || exploreName;
  const baseAlias = exploreName;
  const isBaseView = (name: string) => name === baseAlias || name === baseViewName;

  const joinDefs: any[] = [];
  const joinsRaw = explore.join ? (Array.isArray(explore.join) ? explore.join : [explore.join]) : [];

  joinsRaw.forEach((j: any) => {
    const alias = j._name || j.join;
    const viewName = j.from || alias;
    const rel = (j.relationship || 'many_to_one').toLowerCase();
    const jType = (j.type || 'left_outer').toLowerCase().replace('_join', '').replace(' ', '_');

    const sqlOn = j.sql_on || '';
    const keyMatch = sqlOn.match(/\$\{(\w+)\.(\w+)\}\s*=\s*\$\{(\w+)\.(\w+)\}/);
    const keys = keyMatch ? [{
      leftView: keyMatch[1], leftCol: keyMatch[2].toUpperCase(),
      rightView: keyMatch[3], rightCol: keyMatch[4].toUpperCase()
    }] : [];

    if (!keyMatch && sqlOn) {
      warnings.push(`⚠ Join "${alias}": complex sql_on — keys may need manual review`);
    }

    joinDefs.push({ alias, viewName, rel, joinType: jType, keys });
  });

  const needsPhysical = (j: any): boolean => {
    if (strategy === 'joins') return true;
    if (strategy === 'relationships') return false;
    return j.rel === 'one_to_many' || j.rel === 'many_to_many' || j.joinType === 'full_outer';
  };

  const relJoins = joinDefs.filter(j => !needsPhysical(j));

  // Build elements
  const elementMap: Record<string, ElementResult> = {};
  const physViewMap: Record<string, ElementResult> = {};

  const baseResult = lookConvertView(baseViewName, views[baseViewName], connectionId, warnings);
  elementMap[baseAlias] = baseResult;
  physViewMap[baseViewName] = baseResult;
  if (baseAlias !== baseViewName) elementMap[baseViewName] = baseResult;

  for (const j of joinDefs) {
    if (!physViewMap[j.viewName]) {
      const res = lookConvertView(j.viewName, views[j.viewName], connectionId, warnings);
      physViewMap[j.viewName] = res;
    }
    elementMap[j.alias] = physViewMap[j.viewName];
    if (!elementMap[j.viewName]) elementMap[j.viewName] = physViewMap[j.viewName];
  }

  // Wire relationships
  const baseEl = elementMap[baseAlias].element;
  const usedTargetCols = new Set<string>();

  relJoins.forEach(j => {
    const targetRes = elementMap[j.alias] || elementMap[j.viewName];
    if (!targetRes) {
      warnings.push(`⚠ Relationship "${j.alias}": target not found`);
      return;
    }

    j.keys.forEach((k: any) => {
      const isTarget = (name: string) => name === j.alias || name === j.viewName;
      const srcColId = lookFindColId(elementMap[baseAlias], isBaseView(k.leftView) ? k.leftCol : k.rightCol);
      const tgtColId = lookFindColId(targetRes, isTarget(k.leftView) ? k.leftCol : k.rightCol);

      if (!srcColId || !tgtColId) {
        warnings.push(`⚠ Relationship "${j.alias}": could not resolve column IDs for keys (${k.leftCol} / ${k.rightCol})`);
        return;
      }

      const pairKey = `${targetRes.elementId}|${tgtColId}`;
      if (usedTargetCols.has(pairKey)) {
        warnings.push(`ℹ Role-playing join "${j.alias}" shares a physical table — add manually in Sigma.`);
        return;
      }
      usedTargetCols.add(pairKey);

      if (!baseEl.relationships) baseEl.relationships = [];
      baseEl.relationships.push({
        id: sigmaShortId(),
        targetElementId: targetRes.elementId,
        keys: [{ sourceColumnId: srcColId, targetColumnId: tgtColId }],
        name: j.alias,
        relationshipType: 'N:1'
      });
    });
  });

  // Collect unique elements — dims first, fact (with relationships) last
  const seenIds = new Set<string>();
  let allElements = Object.values(physViewMap).filter(r => {
    if (seenIds.has(r.elementId)) return false;
    seenIds.add(r.elementId);
    return true;
  }).map(r => r.element);

  allElements.sort((a, b) => {
    const aHasRel = !!(a.relationships && a.relationships.length > 0);
    const bHasRel = !!(b.relationships && b.relationships.length > 0);
    if (aHasRel === bHasRel) return 0;
    return aHasRel ? 1 : -1;
  });

  if (!connectionId) warnings.unshift('⚠ Connection ID not set — update in JSON before saving to Sigma');

  const sigmaModel = {
    name: sigmaDisplayName(exploreName),
    pages: [{ id: sigmaShortId(), name: 'Page 1', elements: allElements }]
  };

  const totalCols = allElements.reduce((s, e) => s + (e.columns?.length || 0), 0);
  const totalMetrics = allElements.reduce((s, e) => s + (e.metrics?.length || 0), 0);
  const totalRels = allElements.reduce((s, e) => s + (e.relationships?.length || 0), 0);

  return {
    model: sigmaModel,
    warnings,
    stats: {
      views: Object.keys(views).length,
      explores: Object.keys(explores).length,
      elements: allElements.length,
      columns: totalCols,
      metrics: totalMetrics,
      relationships: totalRels
    }
  };
}
