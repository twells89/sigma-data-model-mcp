/**
 * Power BI Model (.bim / TOM JSON) → Sigma Data Model JSON converter.
 *
 * Handles:
 * - Tables → Sigma elements with warehouse paths (extracted from M expressions)
 * - DAX measures → Sigma metrics with formula conversion
 * - DAX calculated columns → Sigma calculated columns
 * - Relationships (fromTable=many → toTable=one) → Sigma relationships
 * - Measures-only tables → measures moved to fact element
 * - Display folders → Sigma folders
 * - Cross-element column references → auto-rewrite with [SRC/REL_NAME/Field] form
 * - Calculation groups → derived metric stubs per base measure × calc item
 */

import {
  resetIds, sigmaShortId, sigmaInodeId, sigmaDisplayName, inferSigmaFormat,
  buildDerivedElements,
  type SigmaElement, type SigmaColumn, type ConversionResult,
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

// ── DAX → Sigma Formula Converter ─────────────────────────────────────────────

export function pbiDaxToSigma(
  dax: string | string[],
  warnings: string[] | null,
  measureName: string
): string | null {
  // BIM/TMSL serializes multi-line DAX expressions as a string[] (one entry per line)
  if (Array.isArray(dax)) dax = dax.join('\n');
  if (typeof dax !== 'string' || !dax.trim()) return null;
  let f = dax.trim();

  // ── Tier 4: Structural patterns → warnings only ──
  // CALCULATE with ALL/ALLEXCEPT/REMOVEFILTERS
  if (/\bCALCULATE\s*\(/i.test(f) && /\b(ALL|ALLEXCEPT|REMOVEFILTERS|ALLSELECTED)\s*\(/i.test(f)) {
    if (warnings) warnings.push(`⚠ "${measureName}": uses CALCULATE with filter context manipulation. In Sigma, use groupings. See: ${PBI_COMMUNITY_LINKS.leveled}`);
    return null;
  }
  // Iterator functions
  if (/\b(SUMX|AVERAGEX|MINX|MAXX|COUNTAX|CONCATENATEX)\s*\(/i.test(f)) {
    const fn = f.match(/\b(SUMX|AVERAGEX|MINX|MAXX|COUNTAX|CONCATENATEX)/i)![1];
    if (warnings) warnings.push(`⚠ "${measureName}": uses DAX iterator (${fn}). Use groupings or calculated columns. See: ${PBI_COMMUNITY_LINKS.groupings}`);
    return null;
  }
  // Time intelligence
  if (/\b(TOTALYTD|TOTALQTD|TOTALMTD|SAMEPERIODLASTYEAR|DATEADD|DATESYTD|PARALLELPERIOD|PREVIOUSMONTH|PREVIOUSQUARTER|PREVIOUSYEAR)\s*\(/i.test(f)) {
    const fn = f.match(/\b(TOTALYTD|TOTALQTD|TOTALMTD|SAMEPERIODLASTYEAR|DATEADD|DATESYTD|PARALLELPERIOD|PREVIOUSMONTH|PREVIOUSQUARTER|PREVIOUSYEAR)/i)![1];
    if (warnings) warnings.push(`⚠ "${measureName}": uses DAX time intelligence (${fn}). Use Period over Period feature. See: ${PBI_COMMUNITY_LINKS.pop}`);
    return null;
  }
  // CALCULATE without ALL (simple filter)
  if (/\bCALCULATE\s*\(/i.test(f)) {
    const simpleCalc = f.match(/\bCALCULATE\s*\(\s*(SUM|COUNT|COUNTROWS|AVERAGE|MIN|MAX|DISTINCTCOUNT)\s*\(\s*(\[[^\]]+\])\s*\)\s*,\s*(\[[^\]]+\])\s*=\s*"([^"]+)"\s*\)/i);
    if (simpleCalc) {
      const aggMap: Record<string, string> = { 'SUM': 'SumIf', 'AVERAGE': 'AvgIf', 'COUNT': 'CountIf', 'MIN': 'MinIf', 'MAX': 'MaxIf', 'DISTINCTCOUNT': 'CountDistinctIf' };
      const sigmaFn = aggMap[simpleCalc[1].toUpperCase()] || 'SumIf';
      const col = simpleCalc[2];
      const dimCol = simpleCalc[3];
      const val = simpleCalc[4];
      if (sigmaFn === 'CountIf') return `CountIf(${dimCol} = "${val}")`;
      return `${sigmaFn}(${col}, ${dimCol} = "${val}")`;
    }
    if (warnings) warnings.push(`⚠ "${measureName}": complex CALCULATE expression. Use groupings. See: ${PBI_COMMUNITY_LINKS.leveled}`);
    return null;
  }
  // VAR/RETURN blocks
  if (/\bVAR\b/i.test(f) && /\bRETURN\b/i.test(f)) {
    if (warnings) warnings.push(`⚠ "${measureName}": uses DAX VAR/RETURN. Break into multiple calculated columns. See: ${PBI_COMMUNITY_LINKS.biDiffs}`);
    return null;
  }

  // ── Tier 1: Direct mappings ──

  // DIVIDE(a, b, alt) — nested-paren-aware parser
  const divideMatch = f.match(/\bDIVIDE\s*\(/i);
  if (divideMatch) {
    const startIdx = divideMatch.index! + divideMatch[0].length;
    const divArgs: string[] = [];
    let depth = 1, argStart = startIdx;
    for (let i = startIdx; i < f.length && depth > 0; i++) {
      if (f[i] === '(') depth++;
      else if (f[i] === ')') { depth--; if (depth === 0) { divArgs.push(f.slice(argStart, i).trim()); break; } }
      else if (f[i] === ',' && depth === 1) { divArgs.push(f.slice(argStart, i).trim()); argStart = i + 1; }
    }
    if (divArgs.length >= 2) {
      const num = divArgs[0], den = divArgs[1], alt = divArgs[2];
      let d2 = 1, endPos = startIdx;
      for (; endPos < f.length && d2 > 0; endPos++) {
        if (f[endPos] === '(') d2++;
        else if (f[endPos] === ')') d2--;
      }
      let replacement: string;
      if (alt && alt.trim()) {
        replacement = `If(${den} = 0, ${alt.trim()}, ${num} / ${den})`;
      } else {
        replacement = `${num} / ${den}`;
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
  // RELATED('table'[Col]) — the inner 'table'[Col] is normalized below to
  // [Col]; we strip the RELATED wrapper after that. The bare ref is
  // intentional: the post-conversion cross-element move pass detects calc
  // cols whose refs aren't local, pulls them off the source warehouse-table,
  // and places them on the derived "<Table> View" element with refs
  // rewritten to the triple form [SRC/REL/Col] — the only form Sigma
  // resolves for cross-element refs.
  const hadRelated = /\bRELATED\s*\(/i.test(f);
  if (hadRelated && warnings) {
    warnings.push(`ℹ Calculated column "${measureName}": uses RELATED() — column will be moved to a derived "<Table> View" element with cross-element refs rewritten to [SRC/REL/Col] form.`);
  }
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

  // Clean up 'table'[column] → [column] (quoted table qualifier)
  // Collect unique table prefixes before [ to detect multi-table references
  const quotedTablePrefixes = (f.match(/'([^']+)'\[/g) || []).map(m => m.replace(/'\[$/g, '').replace(/^'/g, ''));
  const unquotedTablePrefixes = (f.match(/\b([A-Za-z_]\w*)\[/g) || []).map(m => m.replace(/\[$/, ''));
  const allTablePrefixes = [...new Set([...quotedTablePrefixes, ...unquotedTablePrefixes])].filter(p =>
    !/^(If|Switch|Not|And|Or|Sum|Avg|Min|Max|Count|CountIf|CountDistinct|CumulativeSum|Coalesce|Nullif|Round|Floor|Ceiling|Abs|Upper|Lower|Trim|Left|Right|Mid|Replace|Find|Len|Year|Month|Day|Hour|Minute|Second|Today|Now|MakeDate|DateDiff|DateAdd|DateTrunc|DateFormat|IsNull|IsNotNull|Int|Number|Text|Sqrt|Power|Concat|In|GrandTotal|CumulativeAvg)$/.test(p)
  );
  if (allTablePrefixes.length > 1 && warnings) {
    const tableNames = allTablePrefixes.join(', ');
    warnings.push(`⚠ Calculated column "${measureName}": references columns from multiple tables (${tableNames}). Column context has been simplified — verify formula references the correct columns.`);
  }
  f = f.replace(/'[^']+'\[([^\]]+)\]/g, '[$1]');
  // Also handle unquoted: Table[Column] → [Column]
  f = f.replace(/\b[A-Za-z_]\w*\[([^\]]+)\]/g, '[$1]');

  // Strip RELATED([col]) → [col] AFTER table-prefix normalization, so that
  // RELATED('dim'[X]) (which the line 121 regex couldn't match because of
  // the quoted prefix) gets unwrapped here.
  f = f.replace(/\bRELATED\s*\(\s*(\[[^\]]+\])\s*\)/gi, '$1');

  return f.trim();
}

// ── Extract table path from Power Query M expression ──────────────────────────

function pbiExtractPathFromM(mExpr: string): string[] | null {
  if (!mExpr) return null;

  // Pattern 1: explicit SQL Server / Azure connector with db arg
  // Sql.Database("server", "DATABASE")
  const sqlDbMatch = mExpr.match(/Sql\.Database\s*\(\s*"[^"]*"\s*,\s*"([^"]+)"/i);
  const schemaMatch = mExpr.match(/\{[^}]*\[Schema\s*=\s*"([^"]+)"\]/i)
    || mExpr.match(/\{[^}]*\[Name\s*=\s*"([^"]+)"\s*,\s*Kind\s*=\s*"Schema"\]/i);
  const tableKindMatch = mExpr.match(/\{[^}]*\[Name\s*=\s*"([^"]+)"\s*,\s*Kind\s*=\s*"Table"\]/i);

  if (sqlDbMatch && tableKindMatch) {
    const db = sqlDbMatch[1];
    const table = tableKindMatch[1];
    const schema = schemaMatch ? schemaMatch[1] : null;
    if (schema) return [db.toUpperCase(), schema.toUpperCase(), table.toUpperCase()];
    return [db.toUpperCase(), table.toUpperCase()];
  }

  // Pattern 2a: Kind-tagged navigation — Snowflake / Databricks / BigQuery / others.
  // Power BI's Snowflake connector emits navigation steps that carry an explicit Kind:
  //   Source{[Name = "CSA", Kind = "Database"]}[Data]
  //   #"Navigation 1"{[Name = "TJ", Kind = "Schema"]}[Data]
  //   #"Navigation 2"{[Name = "EMPLOYEES", Kind = "Table"]}[Data]
  // Key on the Kind so each segment maps to the right path slot regardless of order
  // (and tolerate arbitrary whitespace inside the record). Caller overrides still apply
  // later in convertPowerBIToSigma.
  const kindNavMatches = [...mExpr.matchAll(
    /\[\s*Name\s*=\s*"([^"]+)"\s*,\s*Kind\s*=\s*"(Database|Schema|Table|View)"\s*\]/gi
  )];
  if (kindNavMatches.length) {
    let db: string | null = null, sch: string | null = null, tbl: string | null = null;
    for (const m of kindNavMatches) {
      const kind = m[2].toLowerCase();
      if (kind === 'database') db = m[1];
      else if (kind === 'schema') sch = m[1];
      else if (kind === 'table' || kind === 'view') tbl = m[1];
    }
    if (tbl) {
      const parts = [db, sch, tbl].filter((s): s is string => !!s);
      if (parts.length >= 2) return parts.map(s => s.toUpperCase());
    }
  }

  // Pattern 2b: plain navigation by {[Name="X"]}[Data] (no Kind tag) —
  // older Snowflake/Databricks/BigQuery M, positional DB/Schema/Table order.
  // let DB     = Source{[Name="ANALYTICS"]}[Data]
  // let Schema = DB{[Name="PROD"]}[Data]
  // let Table  = Schema{[Name="SALES"]}[Data]
  const nameNavMatches = [...mExpr.matchAll(/\{\s*\[Name\s*=\s*"([^"]+)"\s*\]\s*\}\s*\[\s*Data\s*\]/gi)];
  if (nameNavMatches.length >= 3) {
    return [
      nameNavMatches[0][1].toUpperCase(),
      nameNavMatches[1][1].toUpperCase(),
      nameNavMatches[2][1].toUpperCase(),
    ];
  }
  if (nameNavMatches.length === 2) {
    return [nameNavMatches[0][1].toUpperCase(), nameNavMatches[1][1].toUpperCase()];
  }

  // Pattern 3: SQL query fallback
  const tblMatch = mExpr.match(/FROM\s+(?:\[?(\w+)\]?\.)?\[?(\w+)\]?\.\[?(\w+)\]?/i);
  if (tblMatch) {
    return [tblMatch[1] || '', tblMatch[2], tblMatch[3]].filter(Boolean).map((s: string) => s.toUpperCase());
  }

  return null;
}

// ── Main conversion ───────────────────────────────────────────────────────────

export interface PowerBIConvertOptions {
  connectionId?: string;
  database?: string;
  schema?: string;
}

// ── Time-intelligence → grouped DM elements (DateLookback / CumulativeSum) ──
// Standalone time-intel measures (SAMEPERIODLASTYEAR / DATEADD / TOTALYTD /
// running-total / hand-rolled prior-year) can't be scalar metrics (they need a
// date grouping) — emit them as grouped/leveled elements on the fact's "<T> View"
// (denormalized join), which is DM-native and verified exact vs Power BI.
function classifyTimeIntel(dax: string): 'prior' | 'ytd' | null {
  const d = dax || '';
  if (/\bTOTALYTD\s*\(|\bDATESYTD\s*\(/i.test(d)) return 'ytd';
  if (/FILTER\s*\(\s*ALL\s*\([^)]*\)\s*,[^<]*<=\s*MAX\s*\(/i.test(d)) return 'ytd'; // running total
  if (/\bSAMEPERIODLASTYEAR\s*\(/i.test(d)) return 'prior';
  if (/\bDATEADD\s*\([^,]+,\s*-?\d+\s*,\s*(YEAR|QUARTER|MONTH|WEEK|DAY)/i.test(d)) return 'prior';
  // hand-rolled prior-year: SELECTEDVALUE(Date[Year]) … ALL(Date[Year]) … [Year]=cy-1
  if (/SELECTEDVALUE\s*\([^)]*\[Year\]/i.test(d) && /ALL\s*\([^)]*\[Year\]/i.test(d) && /-\s*1\b/.test(d)) return 'prior';
  return null;
}
// Sigma's display name for a View column: [A/Col]->"Col"; [A/DIM/Col]->"Col (DIM)".
function viewColDisplay(formula: string): string {
  const p = (formula || '').replace(/^\[|\]$/g, '').split('/');
  return p.length <= 2 ? p[p.length - 1] : `${p[p.length - 1]} (${p[p.length - 2]})`;
}
function emitTimeIntelElements(model: any, elements: any[], warnings: string[]): void {
  const AGG: Record<string, string> = { SUM: 'Sum', AVERAGE: 'Avg', AVG: 'Avg', MIN: 'Min',
    MAX: 'Max', COUNT: 'Count', COUNTA: 'Count', DISTINCTCOUNT: 'CountDistinct' };
  const views = elements.filter((e: any) => e.name && /View$/.test(e.name) && e.source?.kind === 'table');
  if (!views.length) return;
  const lastSeg = (f: string) => (f || '').replace(/^\[|\]$/g, '').split('/').pop() || '';
  for (const t of (model.tables || [])) {
    for (const m of (t.measures || [])) {
      const dax = Array.isArray(m.expression) ? m.expression.join(' ') : String(m.expression || '');
      const shape = classifyTimeIntel(dax);
      if (!shape) continue;
      const am = dax.match(/\b(SUM|AVERAGE|AVG|MIN|MAX|COUNT|DISTINCTCOUNT)\s*\(\s*'?[^'\[]*'?\[([^\]]+)\]/i);
      if (!am) continue;
      const agg = AGG[am[1].toUpperCase()]; const col = am[2];
      // find a View carrying both the value column and a date column
      let parent: any = null, valDisp = '', dateDisp = '';
      for (const v of views) {
        const vc = (v.columns || []).find((c: any) => lastSeg(c.formula).toUpperCase() === col.toUpperCase());
        const dc = (v.columns || []).find((c: any) => /full date/i.test(viewColDisplay(c.formula)))
                || (v.columns || []).find((c: any) => /date/i.test(lastSeg(c.formula)) && !/key/i.test(lastSeg(c.formula)));
        if (vc && dc) { parent = v; valDisp = viewColDisplay(vc.formula); dateDisp = viewColDisplay(dc.formula); break; }
      }
      if (!parent) continue;
      const pn = parent.name; const b = (m.name || 'TI').replace(/[^a-zA-Z0-9]/g, '').slice(0, 14);
      if (shape === 'prior') {
        const prior = `${valDisp} (Prior Year)`;
        const cols = [
          { id: `${b}_d`, formula: `DateTrunc("year", [${pn}/${dateDisp}])`, name: 'Year' },
          { id: `${b}_v`, formula: `${agg}([${pn}/${valDisp}])`, name: valDisp },
          { id: `${b}_p`, formula: `DateLookback([${valDisp}], [Year], 1, "year")`, name: prior },
          { id: `${b}_y`, formula: `([${valDisp}] - [${prior}]) / [${prior}]`, name: `${valDisp} YoY %`, format: { kind: 'number', formatString: ',.1%' } },
        ];
        elements.push({ id: `${b}PP`, kind: 'table', name: m.name, source: { kind: 'table', elementId: parent.id },
          columns: cols, order: cols.map(c => c.id), groupings: [{ id: `${b}_g`, groupBy: [`${b}_d`], calculations: [`${b}_v`, `${b}_p`, `${b}_y`] }] });
        warnings.push(`ℹ Time-intel measure "${m.name}" → grouped DateLookback element on "${pn}" (prior-year + YoY %).`);
      } else {
        const cols = [
          { id: `${b}_o`, formula: `DateTrunc("year", [${pn}/${dateDisp}])`, name: 'Year' },
          { id: `${b}_i`, formula: `DateTrunc("month", [${pn}/${dateDisp}])`, name: 'Month' },
          { id: `${b}_v`, formula: `${agg}([${pn}/${valDisp}])`, name: valDisp },
          { id: `${b}_c`, formula: `CumulativeSum([${valDisp}])`, name: `${valDisp} YTD` },
        ];
        // TWO grouping LEVELS so CumulativeSum resets per outer (year) period.
        elements.push({ id: `${b}YT`, kind: 'table', name: m.name, source: { kind: 'table', elementId: parent.id },
          columns: cols, order: cols.map(c => c.id), groupings: [
            { id: `${b}_go`, groupBy: [`${b}_o`] },
            { id: `${b}_gi`, groupBy: [`${b}_i`], calculations: [`${b}_v`, `${b}_c`] }] });
        warnings.push(`ℹ Time-intel measure "${m.name}" → grouped CumulativeSum (YTD, year-reset) element on "${pn}".`);
      }
    }
  }
}

export function convertPowerBIToSigma(
  modelJson: any,
  options: PowerBIConvertOptions = {}
): ConversionResult {
  resetIds();

  const { connectionId = '', database = '', schema = '' } = options;
  const model = modelJson.model || modelJson;

  if (!model.tables || !Array.isArray(model.tables)) {
    throw new Error('Invalid model — no "tables" array found');
  }

  const dbOverride = (database || '').toUpperCase();
  const schOverride = (schema || '').toUpperCase();
  const warnings: string[] = [];
  const elements: SigmaElement[] = [];
  const tableIdMap: Record<string, string> = {};
  const tableColMap: Record<string, Record<string, string>> = {};
  const allPbiToSigmaNames: Record<string, string> = {};

  // Detect "measures only" tables and calculation group tables
  const measureOnlyTables = new Set<string>();
  const calcGroupTables = new Set<string>();
  for (const t of model.tables) {
    if (t.calculationGroup) {
      calcGroupTables.add(t.name);
      continue;
    }
    const dataCols = (t.columns || []).filter((c: any) => c.type !== 'rowNumber' && !c.isGenerated);
    if (dataCols.length === 0 && (t.measures || []).length > 0) {
      measureOnlyTables.add(t.name);
    }
  }

  // Pre-pass: map every table's column PBI-name → Sigma display name across
  // the entire model so calc col formulas referencing related-table columns
  // (e.g. RELATED('dim'[COL_X]) → bare [COL_X]) can be normalized to the
  // display-name form before the cross-element move pass runs.
  for (const t of model.tables) {
    if (calcGroupTables.has(t.name)) continue;
    if (t.name.startsWith('LocalDateTable_') || t.name.startsWith('DateTableTemplate_')) continue;
    for (const c of (t.columns || [])) {
      if (c.type === 'rowNumber' || c.isGenerated) continue;
      const sourceCol = c.sourceColumn || c.name;
      if (!sourceCol) continue;
      if (!(c.name in allPbiToSigmaNames)) {
        allPbiToSigmaNames[c.name] = sigmaDisplayName(sourceCol);
      }
    }
  }

  // ── Convert tables to Sigma elements ────────────────────────────────────────
  for (const t of model.tables) {
    if (measureOnlyTables.has(t.name)) continue;
    if (calcGroupTables.has(t.name)) continue;
    if (t.name.startsWith('LocalDateTable_') || t.name.startsWith('DateTableTemplate_')) continue;

    const elementId = sigmaShortId();
    const tableName: string = t.name;
    tableIdMap[tableName] = elementId;
    tableColMap[tableName] = {};

    // Determine source path
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
        const tblMatch = partition.source.query.match(/FROM\s+(?:\[?(\w+)\]?\.)?\[?(\w+)\]?\.\[?(\w+)\]?/i);
        if (tblMatch) {
          path = [tblMatch[1] || '', tblMatch[2], tblMatch[3]].filter(Boolean).map((s: string) => s.toUpperCase());
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
      warnings.push(`⚠ Table "${tableName}": could not extract source path from M expression — using default.`);
    }

    // Columns
    const columns: SigmaColumn[] = [];
    const order: string[] = [];
    const pbiToSigmaName: Record<string, string> = {};

    for (const c of (t.columns || [])) {
      if (c.type === 'rowNumber' || c.isGenerated) continue;
      if (c.type === 'calculated') continue;
      const sourceCol = c.sourceColumn || c.name;
      const displayName = sigmaDisplayName(sourceCol);
      const colId = sigmaInodeId(sourceCol.toUpperCase().replace(/\s+/g, '_'));
      tableColMap[tableName][c.name] = colId;
      pbiToSigmaName[c.name] = displayName;
      allPbiToSigmaNames[c.name] = displayName;

      const col: SigmaColumn = { id: colId, formula: `[${tableName.toUpperCase()}/${displayName}]` };
      if (c.isHidden) (col as any).hidden = true;
      if (c.description) col.description = c.description;
      columns.push(col);
      order.push(colId);
    }

    // Calculated columns
    for (const c of (t.columns || [])) {
      if (c.type !== 'calculated') continue;
      let sigmaFormula = pbiDaxToSigma(c.expression, warnings, c.name);
      if (sigmaFormula) {
        // Rewrite PBI column names → Sigma display names. Try local table
        // first, fall back to the global map so cross-table refs (e.g. from
        // RELATED('dim'[COL])) get a usable display name that the post-pass
        // cross-element move can map back to a triple-form ref.
        sigmaFormula = sigmaFormula.replace(/\[([^\]\/]+)\]/g, (_m: string, colName: string) => {
          if (pbiToSigmaName[colName]) return `[${pbiToSigmaName[colName]}]`;
          if (allPbiToSigmaNames[colName]) return `[${allPbiToSigmaNames[colName]}]`;
          return `[${colName}]`;
        });
        const colId = sigmaShortId();
        tableColMap[tableName][c.name] = colId;
        pbiToSigmaName[c.name] = c.name;
        const _calcFmt = inferSigmaFormat(sigmaFormula, c.name);
        const _calcCol: any = { id: colId, formula: sigmaFormula, name: c.name };
        if (_calcFmt) _calcCol.format = _calcFmt;
        columns.push(_calcCol);
        order.push(colId);
        warnings.push(`ℹ "${c.name}" → calculated column. Review: ${sigmaFormula.slice(0, 60)}`);
      } else if (!warnings.some(w => w.includes(c.name))) {
        warnings.push(`⛔ "${c.name}": DAX expression could not be converted. Add manually.`);
      }
    }

    // Measures → metrics
    const metrics: any[] = [];
    for (const m of (t.measures || [])) {
      let sigmaFormula = pbiDaxToSigma(m.expression, warnings, m.name);
      if (sigmaFormula) {
        sigmaFormula = sigmaFormula.replace(/\[([^\]\/]+)\]/g, (_m2: string, colName: string) => {
          return pbiToSigmaName[colName] ? `[${pbiToSigmaName[colName]}]` : `[${colName}]`;
        });
        const _mFmt = inferSigmaFormat(sigmaFormula, m.name);
        const metric: any = { id: sigmaShortId(), formula: sigmaFormula, name: m.name };
        if (_mFmt) metric.format = _mFmt;
        if (m.description) metric.description = m.description;
        metrics.push(metric);
      } else if (!warnings.some(w => w.includes(`"${m.name}"`))) {
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

    // Name the base element after its warehouse table (last path segment) so
    // workbook masters can reference it as [TABLE/Col]. Without this, only the
    // derived "<Table> View" elements were named and unnamed base elements
    // were unaddressable. (Bug beads-sigma-tkd #1)
    const baseElementName = (path && path.length ? path[path.length - 1] : tableName.toUpperCase());
    const element: SigmaElement = {
      id: elementId, kind: 'table', name: baseElementName,
      source: { connectionId: connectionId || '<CONNECTION_ID>', kind: 'warehouse-table', path },
      columns, order
    };
    if (metrics.length > 0) (element as any).metrics = metrics;
    if (folders.length > 0) (element as any).folders = folders;
    if (t.isHidden) (element as any).visibleAsSource = false;
    elements.push(element);
  }

  // ── Move measures from "measures only" tables to fact element ──────────────
  if (measureOnlyTables.size > 0) {
    const factEl = elements.reduce((best, e) =>
      (e.columns || []).length > (best.columns || []).length ? e : best, elements[0]);
    if (factEl) {
      for (const tName of measureOnlyTables) {
        const t = model.tables.find((tb: any) => tb.name === tName);
        if (!t) continue;
        for (const m of (t.measures || [])) {
          let sigmaFormula = pbiDaxToSigma(m.expression, warnings, m.name);
          if (sigmaFormula) {
            sigmaFormula = sigmaFormula.replace(/\[([^\]\/]+)\]/g, (_m2: string, colName: string) => {
              return allPbiToSigmaNames[colName] ? `[${allPbiToSigmaNames[colName]}]` : `[${colName}]`;
            });
            if (!(factEl as any).metrics) (factEl as any).metrics = [];
            const _moFmt = inferSigmaFormat(sigmaFormula, m.name);
            const metric: any = { id: sigmaShortId(), formula: sigmaFormula, name: m.name };
            if (_moFmt) metric.format = _moFmt;
            if (m.description) metric.description = m.description;
            (factEl as any).metrics.push(metric);
          }
        }
        warnings.push(`ℹ Measures table "${tName}" → measures moved to "${factEl.source?.path?.[factEl.source.path.length - 1]}"`);
      }
    }
  }

  // ── Relationships ──────────────────────────────────────────────────────────
  for (const rel of (model.relationships || [])) {
    const fromTable = rel.fromTable;
    const toTable = rel.toTable;
    const fromCol = rel.fromColumn;
    const toCol = rel.toColumn;

    const fromElId = tableIdMap[fromTable];
    const toElId = tableIdMap[toTable];
    if (!fromElId || !toElId) continue;

    const fromColId = tableColMap[fromTable]?.[fromCol];
    const toColId = tableColMap[toTable]?.[toCol];
    if (!fromColId || !toColId) {
      warnings.push(`⚠ Relationship ${fromTable}[${fromCol}] → ${toTable}[${toCol}]: columns not found`);
      continue;
    }

    const fromElement = elements.find(e => e.id === fromElId);
    if (fromElement) {
      if (!fromElement.relationships) fromElement.relationships = [];
      fromElement.relationships.push({
        id: sigmaShortId(),
        targetElementId: toElId,
        keys: [{ sourceColumnId: fromColId, targetColumnId: toColId }],
        name: toTable
      });
    }
  }

  // ── Pull cross-element calc cols off source warehouse-table elements ─────
  // A calc col on a warehouse-table whose formula references columns that
  // aren't on that element (e.g. RELATED('dim'[Field]) DAX → bare [Field])
  // cannot resolve there — Sigma doesn't see the related-table columns in
  // scope. We pull these calcs off the source, build derived "<Table> View"
  // elements via buildDerivedElements (which surfaces related cols via
  // [SRC/REL/Field]), then place the calcs on the derived element with
  // their bare [X] refs rewritten to the same triple form.
  // Mirrors tableau.ts buildDerivedElementsAndMoveCalcs Steps 1+3.
  const pbiCrossElCalcsByElId: Record<string, any[]> = {};
  for (const el of elements) {
    if (el.source?.kind !== 'warehouse-table') continue;
    if (!(el as any).relationships?.length) continue;

    const localNames = new Set<string>();
    for (const c of (el.columns || [])) {
      if (c.name) localNames.add(c.name.toUpperCase());
      if (!c.formula) continue;
      const fm = c.formula.match(/^\[[^\]\/]+\/([^\]]+)\]$/);
      if (fm) localNames.add(fm[1].toUpperCase());
    }

    const cross: any[] = [];
    const keep: any[] = [];
    for (const c of (el.columns || []) as any[]) {
      if (!c.name || !c.formula) { keep.push(c); continue; }
      // already-rewritten triple-segment formula (single-ref view col)
      if (/^\[[^\]\/]+\/[^\]\/]+\/[^\]]+\]$/.test(c.formula)) { keep.push(c); continue; }
      // simple 2-seg [Table/Field] passthrough column — keep
      if (/^\[[^\]\/]+\/[^\]\/]+\]$/.test(c.formula)) { keep.push(c); continue; }
      const refs = c.formula.match(/\[([^\]\/]+)\]/g) || [];
      const hasCross = refs.some((ref: string) => {
        const rn = ref.replace(/^\[|\]$/g, '');
        return !/^(true|false|null)$/i.test(rn) && !localNames.has(rn.toUpperCase());
      });
      if (hasCross) {
        const oi = ((el as any).order || []).indexOf(c.id);
        if (oi >= 0) ((el as any).order as string[]).splice(oi, 1);
        cross.push(c);
      } else {
        keep.push(c);
      }
    }
    (el as any).columns = keep;
    if (cross.length) pbiCrossElCalcsByElId[el.id] = cross;
  }

  // ── Calculation groups → derived metric stubs ────────────────────────────
  // Build a flat index: metric name → element so we can attach derived metrics
  // to the same element as their base measure.
  interface MetricRef { elementIndex: number; sigmaFormula: string }
  const metricIndex: Record<string, MetricRef> = {};
  for (let ei = 0; ei < elements.length; ei++) {
    for (const m of ((elements[ei] as any).metrics || [])) {
      if (m.name && m.formula) metricIndex[m.name] = { elementIndex: ei, sigmaFormula: m.formula };
    }
  }

  for (const t of model.tables) {
    if (!calcGroupTables.has(t.name)) continue;
    const cg = t.calculationGroup;
    const items: any[] = cg?.calculationItems || [];
    if (items.length === 0) continue;

    const groupName = t.name;
    warnings.push(
      `ℹ Calculation group "${groupName}" (${items.length} item${items.length !== 1 ? 's' : ''}): ` +
      `${items.map((i: any) => i.name).join(', ')} — ` +
      `derived metric stubs generated. Implement time intelligence using Sigma's Period-over-Period: ${PBI_COMMUNITY_LINKS.pop}`
    );

    // Track which elements get new metrics so we can add/update their folder
    const newMetricsByElement: Record<number, any[]> = {};

    for (const item of items) {
      const itemName: string = item.name || 'Unknown';
      const itemExpr: string = (item.expression || '').trim();

      // Skip "Current" / pass-through items — they're identical to the base measure
      const isPassthrough =
        /^SELECTEDMEASURE\s*\(\s*\)\s*$/i.test(itemExpr) ||
        itemName.toLowerCase() === 'current' ||
        itemName.toLowerCase() === 'actual';
      if (isPassthrough) continue;

      // Classify the item's time intelligence pattern for the description
      let description = `Calculation group "${groupName}" — ${itemName}. `;
      if (/TOTALYTD|DATESYTD/i.test(itemExpr)) {
        description += `Year-to-date. Implement using DateTrunc + CumulativeSum or Sigma's Period-over-Period: ${PBI_COMMUNITY_LINKS.pop}`;
      } else if (/TOTALQTD/i.test(itemExpr)) {
        description += `Quarter-to-date. Use DateTrunc("quarter", …) + CumulativeSum.`;
      } else if (/TOTALMTD/i.test(itemExpr)) {
        description += `Month-to-date. Use DateTrunc("month", …) + CumulativeSum.`;
      } else if (/SAMEPERIODLASTYEAR|PREVIOUSYEAR/i.test(itemExpr)) {
        description += `Same period last year. Implement using Sigma's Period-over-Period: ${PBI_COMMUNITY_LINKS.pop}`;
      } else if (/PREVIOUSQUARTER|PREVIOUSMONTH/i.test(itemExpr)) {
        description += `Previous period. Implement using DateAdd / Sigma's Period-over-Period: ${PBI_COMMUNITY_LINKS.pop}`;
      } else if (/PARALLELPERIOD|DATEADD/i.test(itemExpr)) {
        description += `Date-shifted period. Implement using DateAdd + Sigma's Period-over-Period: ${PBI_COMMUNITY_LINKS.pop}`;
      } else if (/DIVIDE\s*\(/i.test(itemExpr)) {
        description += `Ratio/variance calculation. Implement as a derived metric using base period formulas.`;
      } else {
        description += `DAX expression: ${itemExpr.slice(0, 120)}`;
      }

      // Generate one derived metric per base measure
      for (const [baseName, ref] of Object.entries(metricIndex)) {
        const derivedName = `${baseName} (${itemName})`;
        const derivedMetric: any = {
          id: sigmaShortId(),
          name: derivedName,
          // Use base formula as placeholder so the metric is syntactically valid
          formula: ref.sigmaFormula,
          description,
        };

        if (!newMetricsByElement[ref.elementIndex]) newMetricsByElement[ref.elementIndex] = [];
        newMetricsByElement[ref.elementIndex].push(derivedMetric);
      }
    }

    // Attach derived metrics to their elements and group them in a display folder
    for (const [eiStr, newMetrics] of Object.entries(newMetricsByElement)) {
      const ei = Number(eiStr);
      const el = elements[ei] as any;
      if (!el.metrics) el.metrics = [];
      el.metrics.push(...newMetrics);

      // Add / update a display folder for this calc group
      if (!el.folders) el.folders = [];
      const existingFolder = el.folders.find((f: any) => f.name === groupName);
      const folderItems = newMetrics.map((m: any) => m.id);
      if (existingFolder) {
        existingFolder.items.push(...folderItems);
      } else {
        el.folders.push({ id: sigmaShortId(), name: groupName, items: folderItems });
      }
    }
  }

  // ── Derived "<Table> View" elements + place pulled-off calc cols ────────
  // buildDerivedElements creates a derived element per warehouse-table that
  // has outgoing relationships, exposing own + related cols via [SRC/REL/X]
  // formulas. We then rewrite any pulled-off calc col's bare [X] refs to
  // the same triple form (using the relationship.name as REL segment) and
  // append onto the derived element. Mirrors tableau.ts Step 3.
  const pbiDerivedEls = buildDerivedElements(elements);
  for (const de of pbiDerivedEls) elements.push(de);

  // Auto-emit grouped time-intel elements (DateLookback / CumulativeSum) for
  // standalone time-intel measures, now that the "<T> View" join elements exist.
  emitTimeIntelElements(model, elements, warnings);

  const pbiPlacedSrcElIds: Record<string, boolean> = {};
  for (const de of pbiDerivedEls) {
    if (de.source?.kind !== 'table' || !(de.source as any).elementId) continue;
    const srcElId = (de.source as any).elementId;
    const calcs = pbiCrossElCalcsByElId[srcElId];
    if (!calcs?.length) continue;
    const srcEl = elements.find(e => e.id === srcElId);
    if (!srcEl) continue;
    const srcBaseName = (srcEl as any).name
      || srcEl.source?.path?.[srcEl.source.path.length - 1]
      || '';

    // Build map: bare related-col display name → triple-form path.
    const relatedNameMap: Record<string, string> = {};
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

    for (const c of calcs) {
      if (c.formula && Object.keys(relatedNameMap).length) {
        c.formula = c.formula.replace(/\[([^\]\/]+)\]/g, (match: string, refName: string) => {
          const rewritten = relatedNameMap[refName];
          return rewritten ? `[${rewritten}]` : match;
        });
      }
      ((de as any).columns as any[]).push(c);
      ((de as any).order as string[]).push(c.id);
    }
    warnings.push(`ℹ ${calcs.length} calc col(s) moved to derived "${(de as any).name}" (cross-element refs)`);
    pbiPlacedSrcElIds[srcElId] = true;
  }
  for (const elId of Object.keys(pbiCrossElCalcsByElId)) {
    if (pbiPlacedSrcElIds[elId]) continue;
    for (const c of pbiCrossElCalcsByElId[elId]) {
      warnings.push(`⚠ "${c.name}" cross-element refs but no derived element — column dropped`);
    }
  }

  // ── Build output ──────────────────────────────────────────────────────────
  if (!connectionId) warnings.unshift('⚠ Connection ID not set — update in JSON before saving to Sigma');

  const modelName = modelJson.name || model.name || 'Power BI Import';
  const sigmaModel = {
    name: modelName,
    schemaVersion: 1,
    pages: [{ id: sigmaShortId(), name: 'Page 1', elements }]
  };

  const ec = elements.length;
  const mc = elements.reduce((n, e) => n + ((e as any).metrics?.length || 0), 0);
  const rc = elements.reduce((n, e) => n + (e.relationships?.length || 0), 0);
  const cgCount = calcGroupTables.size;

  return {
    model: sigmaModel,
    warnings,
    stats: {
      tables: model.tables.filter((t: any) => !calcGroupTables.has(t.name)).length,
      elements: ec,
      columns: elements.reduce((n, e) => n + (e.columns?.length || 0), 0),
      metrics: mc,
      relationships: rc,
      ...(cgCount > 0 ? { calculationGroups: cgCount } : {}),
    }
  };
}
