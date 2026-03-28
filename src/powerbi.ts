/**
 * Power BI Model JSON → Sigma Data Model JSON converter.
 * Handles .bim and DataModelSchema JSON (from .pbit extraction).
 *
 * DAX conversion tiers:
 *   Tier 1: Direct mappings (SUM→Sum, DIVIDE→safe division, etc.)
 *   Tier 2: Simple CALCULATE(AGG, filter) → SumIf/CountIf
 *   Tier 3: Complex CALCULATE+ALL, iterators, time intelligence → warnings
 *   Tier 4: VAR/RETURN → warnings
 */

import {
  resetIds, sigmaShortId, sigmaDisplayName,
  type SigmaElement, type ConversionResult,
} from './sigma-ids.js';

// ── Community article links for warnings ──────────────────────────────────────

const PBI_COMMUNITY_LINKS = {
  lod: 'community.sigmacomputing.com/t/tableau-level-of-detail-or-lod-calculations-in-sigma/6427',
  groupings: 'community.sigmacomputing.com/t/how-to-use-groupings-aggregate-calculations/2003',
  rollup: 'community.sigmacomputing.com/t/rollup-perform-aggregate-calculations-across-a-group-of-values-without-using-a-group-by/4367',
  biDiffs: 'community.sigmacomputing.com/t/sigma-differences-from-other-bi-tools-overview-for-new-sigma-creators/3285',
  leveled: 'community.sigmacomputing.com/t/how-to-implement-complex-leveled-aggregations-in-sigma-lods-dax/5203',
  pop: 'community.sigmacomputing.com/t/which-logic-to-use-for-period-over-period-comparisons/3206',
};

// ── DAX → Sigma formula converter ─────────────────────────────────────────────

export function pbiDaxToSigma(
  dax: string,
  warnings: string[],
  measureName: string
): string | null {
  if (!dax || !dax.trim()) return null;
  let f = dax.trim();

  // ── Tier 3/4: Structural patterns → warnings only ──────────────────────
  // CALCULATE with ALL/ALLEXCEPT/REMOVEFILTERS → grouping pattern
  if (/\bCALCULATE\s*\(/i.test(f) && /\b(ALL|ALLEXCEPT|REMOVEFILTERS|ALLSELECTED)\s*\(/i.test(f)) {
    warnings.push(`⚠ "${measureName}": uses CALCULATE with filter context manipulation. In Sigma, use groupings. See: ${PBI_COMMUNITY_LINKS.leveled}`);
    return null;
  }
  // Iterator functions
  if (/\b(SUMX|AVERAGEX|MINX|MAXX|COUNTAX|CONCATENATEX)\s*\(/i.test(f)) {
    const fn = f.match(/\b(SUMX|AVERAGEX|MINX|MAXX|COUNTAX|CONCATENATEX)/i)![1];
    warnings.push(`⚠ "${measureName}": uses DAX iterator (${fn}). Use groupings or calculated columns. See: ${PBI_COMMUNITY_LINKS.groupings}`);
    return null;
  }
  // Time intelligence
  if (/\b(TOTALYTD|TOTALQTD|TOTALMTD|SAMEPERIODLASTYEAR|DATEADD|DATESYTD|PARALLELPERIOD|PREVIOUSMONTH|PREVIOUSQUARTER|PREVIOUSYEAR)\s*\(/i.test(f)) {
    const fn = f.match(/\b(TOTALYTD|TOTALQTD|TOTALMTD|SAMEPERIODLASTYEAR|DATEADD|DATESYTD|PARALLELPERIOD|PREVIOUSMONTH|PREVIOUSQUARTER|PREVIOUSYEAR)/i)![1];
    warnings.push(`⚠ "${measureName}": uses DAX time intelligence (${fn}). Use Period over Period feature. See: ${PBI_COMMUNITY_LINKS.pop}`);
    return null;
  }
  // CALCULATE without ALL (simple filter) — try Tier 2 conversion
  if (/\bCALCULATE\s*\(/i.test(f)) {
    const simpleCalc = f.match(/\bCALCULATE\s*\(\s*(SUM|COUNT|COUNTROWS|AVERAGE|MIN|MAX|DISTINCTCOUNT)\s*\(\s*(\[[^\]]+\])\s*\)\s*,\s*(\[[^\]]+\])\s*=\s*"([^"]+)"\s*\)/i);
    if (simpleCalc) {
      const aggMap: Record<string, string> = {
        SUM: 'SumIf', AVERAGE: 'AvgIf', COUNT: 'CountIf',
        MIN: 'MinIf', MAX: 'MaxIf', DISTINCTCOUNT: 'CountDistinctIf'
      };
      const sigmaFn = aggMap[simpleCalc[1].toUpperCase()] || 'SumIf';
      const col = simpleCalc[2];
      const dimCol = simpleCalc[3];
      const val = simpleCalc[4];
      if (sigmaFn === 'CountIf') return `CountIf(${dimCol} = "${val}")`;
      return `${sigmaFn}(${col}, ${dimCol} = "${val}")`;
    }
    warnings.push(`⚠ "${measureName}": complex CALCULATE expression. Use groupings. See: ${PBI_COMMUNITY_LINKS.leveled}`);
    return null;
  }
  // VAR/RETURN blocks
  if (/\bVAR\b/i.test(f) && /\bRETURN\b/i.test(f)) {
    warnings.push(`⚠ "${measureName}": uses DAX VAR/RETURN. Break into multiple calculated columns. See: ${PBI_COMMUNITY_LINKS.biDiffs}`);
    return null;
  }

  // ── Tier 1: Direct mappings ────────────────────────────────────────────

  // DIVIDE(a, b, alt) — nested-paren-aware parser
  const divideMatch = f.match(/\bDIVIDE\s*\(/i);
  if (divideMatch) {
    const startIdx = divideMatch.index! + divideMatch[0].length;
    const divArgs: string[] = [];
    let depth = 1, argStart = startIdx;
    for (let i = startIdx; i < f.length && depth > 0; i++) {
      if (f[i] === '(') depth++;
      else if (f[i] === ')') {
        depth--;
        if (depth === 0) { divArgs.push(f.slice(argStart, i).trim()); break; }
      }
      else if (f[i] === ',' && depth === 1) {
        divArgs.push(f.slice(argStart, i).trim());
        argStart = i + 1;
      }
    }
    if (divArgs.length >= 2) {
      const num = divArgs[0], den = divArgs[1], alt = divArgs[2];
      let replacement: string;
      if (alt && alt.trim()) {
        replacement = `If(${den} = 0, ${alt.trim()}, ${num} / ${den})`;
      } else {
        replacement = `${num} / ${den}`;
      }
      let d2 = 1, endPos = startIdx;
      for (; endPos < f.length && d2 > 0; endPos++) {
        if (f[endPos] === '(') d2++;
        else if (f[endPos] === ')') d2--;
      }
      f = f.slice(0, divideMatch.index!) + replacement + f.slice(endPos);
    }
  }

  // Simple aggregates
  f = f.replace(/\bDISTINCTCOUNT\s*\(/gi, 'CountDistinct(');
  f = f.replace(/\bCOUNTROWS\s*\(\s*'?[^)]*'?\s*\)/gi, 'Count()');
  f = f.replace(/\bCOUNTA\s*\(/gi, 'CountIf(IsNotNull(');
  f = f.replace(/\bSUM\s*\(/gi, 'Sum(');
  f = f.replace(/\bAVERAGE\s*\(/gi, 'Avg(');
  f = f.replace(/\bMIN\s*\(/gi, 'Min(');
  f = f.replace(/\bMAX\s*\(/gi, 'Max(');
  f = f.replace(/\bCOUNT\s*\(/gi, 'Count(');

  // RELATED([Col]) → just [Col]
  f = f.replace(/\bRELATED\s*\(\s*(\[[^\]]+\])\s*\)/gi, '$1');
  f = f.replace(/\bRELATEDTABLE\s*\([^)]*\)/gi, '/* RELATEDTABLE - use relationship */');

  // Logical
  f = f.replace(/\bIF\s*\(/gi, 'If(');
  f = f.replace(/\bSWITCH\s*\(\s*TRUE\s*\(\s*\)\s*,/gi, 'If(');
  f = f.replace(/\bSWITCH\s*\(/gi, 'Switch(');
  f = f.replace(/\bISBLANK\s*\(/gi, 'IsNull(');
  f = f.replace(/\bCOALESCE\s*\(/gi, 'Coalesce(');
  f = f.replace(/\bBLANK\s*\(\s*\)/gi, 'null');
  f = f.replace(/\bNOT\s*\(/gi, 'Not(');
  f = f.replace(/\bTRUE\s*\(\s*\)/gi, 'True');
  f = f.replace(/\bFALSE\s*\(\s*\)/gi, 'False');
  f = f.replace(/&&/g, ' and ');
  f = f.replace(/\|\|/g, ' or ');

  // Text
  f = f.replace(/\bCONCATENATE\s*\(/gi, 'Concat(');
  f = f.replace(/\bLEN\s*\(/gi, 'Len(');
  f = f.replace(/\bUPPER\s*\(/gi, 'Upper(');
  f = f.replace(/\bLOWER\s*\(/gi, 'Lower(');
  f = f.replace(/\bTRIM\s*\(/gi, 'Trim(');
  f = f.replace(/\bLEFT\s*\(/gi, 'Left(');
  f = f.replace(/\bRIGHT\s*\(/gi, 'Right(');
  f = f.replace(/\bMID\s*\(/gi, 'Mid(');
  f = f.replace(/\bSUBSTITUTE\s*\(/gi, 'Replace(');
  f = f.replace(/\bFORMAT\s*\(/gi, 'DateFormat(');

  // Math
  f = f.replace(/\bABS\s*\(/gi, 'Abs(');
  f = f.replace(/\bROUND\s*\(/gi, 'Round(');
  f = f.replace(/\bINT\s*\(/gi, 'Int(');
  f = f.replace(/\bSQRT\s*\(/gi, 'Sqrt(');
  f = f.replace(/\bPOWER\s*\(/gi, 'Power(');

  // Date
  f = f.replace(/\bYEAR\s*\(/gi, 'Year(');
  f = f.replace(/\bMONTH\s*\(/gi, 'Month(');
  f = f.replace(/\bDAY\s*\(/gi, 'Day(');
  f = f.replace(/\bHOUR\s*\(/gi, 'Hour(');
  f = f.replace(/\bMINUTE\s*\(/gi, 'Minute(');
  f = f.replace(/\bSECOND\s*\(/gi, 'Second(');
  f = f.replace(/\bTODAY\s*\(\s*\)/gi, 'Today()');
  f = f.replace(/\bNOW\s*\(\s*\)/gi, 'Now()');
  f = f.replace(/\bDATE\s*\(/gi, 'MakeDate(');
  f = f.replace(/\bDATEDIFF\s*\(/gi, 'DateDiff(');

  // Clean up 'table'[column] → [column] (quoted DAX table qualifier)
  f = f.replace(/'[^']+'\[([^\]]+)\]/g, '[$1]');
  // Also handle unquoted: Table[Column] → [Column]
  f = f.replace(/\b[A-Za-z_]\w*\[([^\]]+)\]/g, '[$1]');

  return f.trim();
}

// ── Extract table path from Power Query M expression ──────────────────────────

function pbiExtractPathFromM(mExpr: string): string[] | null {
  if (!mExpr) return null;
  const dbMatch = mExpr.match(/Sql\.Database\s*\(\s*"[^"]*"\s*,\s*"([^"]+)"/i)
    || mExpr.match(/Snowflake\.Databases\s*\(\s*"[^"]*"\s*,\s*"([^"]+)"/i);
  const schemaMatch = mExpr.match(/\{[^}]*\[Schema\s*=\s*"([^"]+)"\]/i)
    || mExpr.match(/\{[^}]*\[Name\s*=\s*"([^"]+)"\s*,\s*\[Kind\s*=\s*"Schema"\]/i);
  const tableMatch = mExpr.match(/\{[^}]*\[Name\s*=\s*"([^"]+)"\s*,\s*\[Kind\s*=\s*"Table"\]/i)
    || mExpr.match(/\{[^}]*\[Name\s*=\s*"([^"]+)"\]\s*\}\s*\[\s*Data\s*\]/i);
  const db = dbMatch ? dbMatch[1] : null;
  const schema = schemaMatch ? schemaMatch[1] : null;
  const table = tableMatch ? tableMatch[1] : null;
  if (db && schema && table) return [db.toUpperCase(), schema.toUpperCase(), table.toUpperCase()];
  if (schema && table) return [schema.toUpperCase(), table.toUpperCase()];
  return null;
}

// ── Main conversion ───────────────────────────────────────────────────────────

export interface PowerBIConvertOptions {
  connectionId?: string;
  database?: string;
  schema?: string;
}

export function convertPowerBIToSigma(
  modelJson: any,
  options: PowerBIConvertOptions = {}
): ConversionResult {
  resetIds();

  const { connectionId = '', database = '', schema = '' } = options;
  const dbOverride = (database || '').toUpperCase();
  const schOverride = (schema || '').toUpperCase();

  const model = modelJson.model || modelJson;
  if (!model.tables || !Array.isArray(model.tables)) {
    throw new Error('Invalid model — no "tables" array found');
  }

  const warnings: string[] = [];
  const elements: SigmaElement[] = [];
  const tableIdMap: Record<string, string> = {};
  const tableColMap: Record<string, Record<string, string>> = {};
  const allPbiToSigmaNames: Record<string, string> = {};

  // ── Detect "measures only" tables ─────────────────────────────────────────
  const measureOnlyTables = new Set<string>();
  for (const t of model.tables) {
    const dataCols = (t.columns || []).filter((c: any) => c.type !== 'rowNumber' && !c.isGenerated);
    if (dataCols.length === 0 && (t.measures || []).length > 0) {
      measureOnlyTables.add(t.name);
    }
  }

  // ── Convert tables to Sigma elements ──────────────────────────────────────
  for (const t of model.tables) {
    if (measureOnlyTables.has(t.name)) continue;
    if (t.name.startsWith('LocalDateTable_') || t.name.startsWith('DateTableTemplate_')) continue;

    const elementId = sigmaShortId();
    const tableName: string = t.name;
    tableIdMap[tableName] = elementId;
    tableColMap[tableName] = {};

    // Determine source path from M expression
    let path: string[] | null = null;
    const partition = (t.partitions || [])[0];
    if (partition?.source) {
      if (partition.source.expression) {
        path = pbiExtractPathFromM(
          Array.isArray(partition.source.expression)
            ? partition.source.expression.join('\n')
            : partition.source.expression
        );
      }
      if (!path && partition.source.query) {
        const tblMatch = partition.source.query.match(
          /FROM\s+(?:\[?(\w+)\]?\.)?\[?(\w+)\]?\.\[?(\w+)\]?/i
        );
        if (tblMatch) {
          path = [tblMatch[1] || '', tblMatch[2], tblMatch[3]]
            .filter(Boolean)
            .map((s: string) => s.toUpperCase());
        }
      }
    }

    // Apply overrides
    if (path) {
      if (dbOverride && path.length >= 3) path[0] = dbOverride;
      if (schOverride && path.length >= 3) path[1] = schOverride;
      else if (schOverride && path.length === 2) path[0] = schOverride;
    } else {
      path = [dbOverride || 'DATABASE', schOverride || 'SCHEMA', tableName.toUpperCase()];
      warnings.push(`⚠ Table "${tableName}": could not extract source path — using default. Update manually.`);
    }

    // Columns
    const columns: any[] = [];
    const order: string[] = [];
    const pbiToSigmaName: Record<string, string> = {};

    for (const c of (t.columns || [])) {
      if (c.type === 'rowNumber' || c.isGenerated) continue;
      const sourceCol = c.sourceColumn || c.name;
      const displayName = sigmaDisplayName(sourceCol);
      const colId = sigmaShortId();
      tableColMap[tableName][c.name] = colId;
      pbiToSigmaName[c.name] = displayName;
      allPbiToSigmaNames[c.name] = displayName;

      const col: any = { id: colId, formula: `[${tableName.toUpperCase()}/${displayName}]` };
      if (c.isHidden) col.hidden = true;
      if (c.description) col.description = c.description;
      columns.push(col);
      order.push(colId);
    }

    // Calculated columns
    for (const c of (t.columns || [])) {
      if (c.type !== 'calculated' || !c.expression) continue;
      let sigmaFormula = pbiDaxToSigma(c.expression, warnings, c.name);
      if (sigmaFormula) {
        // Rewrite PBI column names → Sigma display names
        sigmaFormula = sigmaFormula.replace(/\[([^\]\/]+)\]/g, (m: string, colName: string) => {
          return pbiToSigmaName[colName] ? `[${pbiToSigmaName[colName]}]` : m;
        });
        const colId = sigmaShortId();
        tableColMap[tableName][c.name] = colId;
        pbiToSigmaName[c.name] = c.name;
        columns.push({ id: colId, formula: sigmaFormula, name: c.name });
        order.push(colId);
        warnings.push(`ℹ "${c.name}" → calculated column. Review: ${sigmaFormula.slice(0, 60)}`);
      } else if (!warnings.some((w: string) => w.includes(c.name))) {
        warnings.push(`⛔ "${c.name}": DAX expression could not be converted. Add manually.`);
      }
    }

    // Measures → metrics
    const metrics: any[] = [];
    for (const m of (t.measures || [])) {
      let sigmaFormula = pbiDaxToSigma(m.expression, warnings, m.name);
      if (sigmaFormula) {
        sigmaFormula = sigmaFormula.replace(/\[([^\]\/]+)\]/g, (m2: string, colName: string) => {
          return pbiToSigmaName[colName] ? `[${pbiToSigmaName[colName]}]` : m2;
        });
        const metric: any = { id: sigmaShortId(), formula: sigmaFormula, name: m.name };
        if (m.description) metric.description = m.description;
        metrics.push(metric);
      } else if (!warnings.some((w: string) => w.includes(`"${m.name}"`))) {
        warnings.push(`⛔ "${m.name}": DAX measure could not be auto-converted. Add manually.`);
      }
    }

    // Display folders
    const folders: any[] = [];
    const folderMap: Record<string, { id: string; name: string; items: string[] }> = {};
    for (const c of [...(t.columns || []), ...(t.measures || [])]) {
      if (c.displayFolder) {
        if (!folderMap[c.displayFolder]) {
          folderMap[c.displayFolder] = { id: sigmaShortId(), name: c.displayFolder, items: [] };
        }
        const colId = tableColMap[tableName][c.name];
        if (colId) folderMap[c.displayFolder].items.push(colId);
      }
    }
    for (const folder of Object.values(folderMap)) {
      if (folder.items.length > 0) folders.push(folder);
    }

    const element: any = {
      id: elementId, kind: 'table',
      source: { connectionId: connectionId || '<CONNECTION_ID>', kind: 'warehouse-table', path },
      columns, order
    };
    if (metrics.length > 0) element.metrics = metrics;
    if (folders.length > 0) element.folders = folders;
    if (t.isHidden) element.visibleAsSource = false;
    elements.push(element);
  }

  // ── Move measures from "measures only" tables to fact element ──────────────
  if (measureOnlyTables.size > 0) {
    const factEl = elements.reduce((best, e) =>
      (e.columns?.length || 0) > (best.columns?.length || 0) ? e : best, elements[0]);
    if (factEl) {
      for (const tName of measureOnlyTables) {
        const t = model.tables.find((tb: any) => tb.name === tName);
        if (!t) continue;
        for (const m of (t.measures || [])) {
          let sigmaFormula = pbiDaxToSigma(m.expression, warnings, m.name);
          if (sigmaFormula) {
            sigmaFormula = sigmaFormula.replace(/\[([^\]\/]+)\]/g, (m2: string, colName: string) => {
              return allPbiToSigmaNames[colName] ? `[${allPbiToSigmaNames[colName]}]` : m2;
            });
            if (!factEl.metrics) factEl.metrics = [];
            const metric: any = { id: sigmaShortId(), formula: sigmaFormula, name: m.name };
            if (m.description) metric.description = m.description;
            factEl.metrics.push(metric);
          }
        }
        warnings.push(`ℹ Measures table "${tName}" → measures moved to "${factEl.source?.path?.[factEl.source.path.length - 1]}"`);
      }
    }
  }

  // ── Relationships ─────────────────────────────────────────────────────────
  for (const rel of (model.relationships || [])) {
    const fromElId = tableIdMap[rel.fromTable];
    const toElId = tableIdMap[rel.toTable];
    if (!fromElId || !toElId) continue;

    const fromColId = tableColMap[rel.fromTable]?.[rel.fromColumn];
    const toColId = tableColMap[rel.toTable]?.[rel.toColumn];
    if (!fromColId || !toColId) {
      warnings.push(`⚠ Relationship ${rel.fromTable}[${rel.fromColumn}] → ${rel.toTable}[${rel.toColumn}]: columns not found`);
      continue;
    }

    // In PBI, fromTable is "many" side, toTable is "one" side
    const fromElement = elements.find(e => e.id === fromElId);
    if (fromElement) {
      if (!fromElement.relationships) fromElement.relationships = [];
      fromElement.relationships.push({
        id: sigmaShortId(),
        targetElementId: toElId,
        keys: [{ sourceColumnId: fromColId, targetColumnId: toColId }],
        name: rel.toTable
      });
    }
  }

  // ── Auto-fix cross-element column references with - link/ syntax ──────────
  const pbiGlobalColMap: Record<string, { elId: string; displayName: string }> = {};
  for (const el of elements) {
    for (const c of (el.columns || [])) {
      const fm = c.formula.match(/\[([^\/\]]+)\/([^\]]+)\]$/);
      if (fm) pbiGlobalColMap[fm[2].toUpperCase()] = { elId: el.id, displayName: fm[2] };
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
    for (const rel of (el.relationships || [])) {
      const fkCol = (el.columns || []).find((c: any) => c.id === rel.keys[0]?.sourceColumnId);
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
        const ge = pbiGlobalColMap[rn.toUpperCase()];
        if (ge && relFkLookup[ge.elId]) {
          fixedFormula = fixedFormula.replace(ref, `[${elTbl}/${relFkLookup[ge.elId]} - link/${ge.displayName}]`);
          wasFixed = true;
        } else {
          warnings.push(`⚠ "${c.name}" references [${rn}] — no matching relationship found. Fix manually.`);
        }
      }
      if (wasFixed) {
        c.formula = fixedFormula;
        warnings.push(`✅ "${c.name}" → linked column: ${fixedFormula.slice(0, 100)}`);
        warnings.push(`   ⚠ Note: Sigma API may not round-trip linked columns correctly yet.`);
      }
    }
  }

  // ── Build output ──────────────────────────────────────────────────────────
  if (!connectionId) warnings.unshift('⚠ Connection ID not set — update in JSON before saving to Sigma');

  const modelName = modelJson.name || model.name || 'Power BI Import';
  const sigmaModel = {
    name: modelName,
    pages: [{ id: sigmaShortId(), name: 'Page 1', elements }]
  };

  const totalCols = elements.reduce((s, e) => s + (e.columns?.length || 0), 0);
  const totalMetrics = elements.reduce((s, e) => s + (e.metrics?.length || 0), 0);
  const totalRels = elements.reduce((s, e) => s + (e.relationships?.length || 0), 0);

  return {
    model: sigmaModel,
    warnings,
    stats: {
      tables: model.tables.length,
      elements: elements.length,
      columns: totalCols,
      metrics: totalMetrics,
      relationships: totalRels,
      measureOnlyTables: measureOnlyTables.size
    }
  };
}
