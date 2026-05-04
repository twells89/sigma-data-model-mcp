/**
 * Qlik Sense metadata JSON → Sigma Data Model converter.
 * Accepts the JSON from Qlik's Engine API GetTablesAndKeys or the REST metadata endpoint.
 */

import {
  resetIds, sigmaShortId, sigmaDisplayName,
  inferSigmaFormat, buildDerivedElements,
  type SigmaElement, type SigmaColumn, type SigmaMetric, type ConversionResult,
} from './sigma-ids.js';

export interface QlikConvertOptions {
  connectionId?: string;
  database?: string;
  schema?: string;
}

export function convertQlikToSigma(
  rawJson: unknown,
  options: QlikConvertOptions = {},
): ConversionResult {
  resetIds();
  const { connectionId = '<CONNECTION_ID>', database: dbOverride = '', schema: schOverride = '' } = options;
  const warnings: string[] = [];

  const { tables, masterMeasures, masterDimensions, appName } = qlikParseInput(rawJson);
  const modelName: string = (rawJson as any).appName || (rawJson as any).appId || appName || 'Qlik App';

  if (!tables.length) throw new Error('No tables found in input. Check the JSON format.');

  const userTables = tables.filter((t: any) =>
    t.name && !t.name.startsWith('$') && !/^%.*%$/.test(t.name)
  );
  if (userTables.length < tables.length) {
    warnings.push(`${tables.length - userTables.length} system table(s) skipped ($… and %%name%% synthetic key tables).`);
  }

  // Pass 1: Build elements
  const elements: SigmaElement[] = [];
  const tableElementMap: Record<string, { elementId: string; colMap: Record<string, { colId: string; displayName: string }>; element: SigmaElement; rowCount: number; fields: any[] }> = {};

  for (const t of userTables) {
    const elementId = sigmaShortId();
    const columns: SigmaColumn[] = [];
    const order: string[] = [];
    const colMap: Record<string, { colId: string; displayName: string }> = {};

    const visibleFields = (t.fields || []).filter((f: any) =>
      f.name && !f.isSystem && !f.isHidden && !f.name.startsWith('$')
    );

    for (const f of visibleFields) {
      const displayName = sigmaDisplayName(f.name);
      const colId = sigmaShortId();
      columns.push({ id: colId, formula: `[${t.name}/${displayName}]` });
      order.push(colId);
      colMap[f.name] = { colId, displayName };
    }

    const pathParts: string[] = [];
    if (dbOverride)  pathParts.push(dbOverride);
    if (schOverride) pathParts.push(schOverride);
    pathParts.push(t.name.toUpperCase());

    const element: SigmaElement = {
      id: elementId, kind: 'table',
      name: sigmaDisplayName(t.name),
      source: { connectionId, kind: 'warehouse-table', path: pathParts },
      columns, order,
    };
    elements.push(element);
    tableElementMap[t.name] = { elementId, colMap, element, rowCount: t.noOfRows || 0, fields: t.fields || [] };
  }

  // Display name lookup for rewriting metric formulas
  const qlikColToDisplayName: Record<string, string> = {};
  for (const info of Object.values(tableElementMap)) {
    for (const [fieldName, colInfo] of Object.entries(info.colMap)) {
      qlikColToDisplayName[fieldName] = colInfo.displayName;
    }
  }

  // Pass 2: Infer relationships from shared field names
  const fieldToTables: Record<string, string[]> = {};
  for (const t of userTables) {
    for (const f of (t.fields || []).filter((f: any) => f.name && !f.name.startsWith('$'))) {
      if (!fieldToTables[f.name]) fieldToTables[f.name] = [];
      fieldToTables[f.name].push(t.name);
    }
  }

  const createdRels = new Set<string>();
  for (const [fieldName, tableNames] of Object.entries(fieldToTables)) {
    if (tableNames.length < 2) continue;
    if (tableNames.length > 2) {
      warnings.push(`Field "${fieldName}" links ${tableNames.length} tables (${tableNames.join(', ')}). Complex association — review relationships in Sigma.`);
    }
    for (let i = 0; i < tableNames.length - 1; i++) {
      for (let j = i + 1; j < tableNames.length; j++) {
        const infoA = tableElementMap[tableNames[i]];
        const infoB = tableElementMap[tableNames[j]];
        if (!infoA || !infoB) continue;

        const relKey = [infoA.elementId, infoB.elementId].sort().join('|') + '|' + fieldName;
        if (createdRels.has(relKey)) continue;
        createdRels.add(relKey);

        const aField = infoA.fields.find((f: any) => f.name === fieldName);
        const bField = infoB.fields.find((f: any) => f.name === fieldName);
        const aDistinct = aField ? (aField.distinctValueCount || 0) : 0;
        const bDistinct = bField ? (bField.distinctValueCount || 0) : 0;
        const aRatio = infoA.rowCount > 0 && aDistinct > 0 ? aDistinct / infoA.rowCount : 0;
        const bRatio = infoB.rowCount > 0 && bDistinct > 0 ? bDistinct / infoB.rowCount : 0;

        const hasPkSide = aRatio >= 0.9 || bRatio >= 0.9;
        const noInfo    = aRatio === 0 && bRatio === 0;
        if (!hasPkSide && !noInfo) continue;

        const toInfo   = aRatio >= bRatio ? infoA : infoB;
        const fromInfo = aRatio >= bRatio ? infoB : infoA;
        const fromColInfo = fromInfo.colMap[fieldName];
        const toColInfo   = toInfo.colMap[fieldName];
        if (!fromColInfo || !toColInfo) continue;

        if (!fromInfo.element.relationships) fromInfo.element.relationships = [];
        const tgtPath = toInfo.element.source?.path;
        fromInfo.element.relationships.push({
          id: sigmaShortId(),
          targetElementId: toInfo.elementId,
          keys: [{ sourceColumnId: fromColInfo.colId, targetColumnId: toColInfo.colId }],
          name: tgtPath ? tgtPath[tgtPath.length - 1].toUpperCase() : fieldName.toUpperCase(),
        });
      }
    }
  }

  // Pass 2b: Explicit relationships
  for (const rel of ((rawJson as any).relationships || [])) {
    const fromInfo = tableElementMap[rel.fromTable];
    const toInfo   = tableElementMap[rel.toTable];
    if (!fromInfo || !toInfo) continue;
    const fromColInfo = fromInfo.colMap[rel.fromField];
    const toColInfo   = toInfo.colMap[rel.toField];
    if (!fromColInfo || !toColInfo) {
      warnings.push(`Explicit relationship ${rel.fromTable}.${rel.fromField} → ${rel.toTable}.${rel.toField}: column not found, skipped.`);
      continue;
    }
    const relKey = [fromInfo.elementId, toInfo.elementId].sort().join('|') + '|' + rel.fromField;
    if (createdRels.has(relKey)) continue;
    createdRels.add(relKey);
    if (!fromInfo.element.relationships) fromInfo.element.relationships = [];
    const expPath = toInfo.element.source?.path;
    fromInfo.element.relationships.push({
      id: sigmaShortId(),
      targetElementId: toInfo.elementId,
      keys: [{ sourceColumnId: fromColInfo.colId, targetColumnId: toColInfo.colId }],
      name: expPath ? expPath[expPath.length - 1].toUpperCase() : rel.toTable.toUpperCase(),
    });
  }

  // Pass 3: Master measures → metrics
  const measuresByElement: Record<string, SigmaMetric[]> = {};
  for (const el of elements) measuresByElement[el.id] = [];

  for (const m of masterMeasures) {
    const title: string = m.title || m.qTitle || 'Metric';
    const exprRaw: string = m.expr || m.qDef || m.expression || '';
    let sigmaFormula = qlikExprToSigma(exprRaw, warnings, title);
    if (!sigmaFormula) continue;
    sigmaFormula = sigmaFormula.replace(/\[([^\]\/]+)\]/g, (_m: string, colName: string) =>
      qlikColToDisplayName[colName] ? `[${qlikColToDisplayName[colName]}]` : _m
    );
    let bestElementId = elements[0]?.id;
    outer: for (const [, info] of Object.entries(tableElementMap)) {
      for (const [fn, dn] of Object.entries(info.colMap)) {
        if (sigmaFormula.includes(`[${(dn as any).displayName}]`) || sigmaFormula.includes(`[${fn}]`)) {
          bestElementId = info.elementId;
          break outer;
        }
      }
    }
    if (!measuresByElement[bestElementId]) measuresByElement[bestElementId] = [];
    const metric: any = { id: sigmaShortId(), formula: sigmaFormula, name: title };
    if (m.description || m.qDescription) metric.description = m.description || m.qDescription;
    const fmt = inferSigmaFormat(sigmaFormula, title);
    if (fmt) metric.format = fmt;
    measuresByElement[bestElementId].push(metric);
  }
  for (const el of elements) {
    const metrics = measuresByElement[el.id];
    if (metrics?.length) el.metrics = metrics;
  }

  // Pass 4: Calculated master dimensions → columns
  for (const d of masterDimensions) {
    const title: string = d.title || d.qTitle || 'Dimension';
    const exprRaw: string = d.fieldDef || d.qFieldDef || d.expr || d.expression || '';
    const isCalc = exprRaw.trim().startsWith('=') ||
      /\b(If|Sum|Count|Avg|Concat|Year|Month|Day|Left|Right|Upper|Lower|Trim)\s*\(/i.test(exprRaw);
    if (!isCalc) continue;
    let sigmaFormula = qlikExprToSigma(exprRaw, warnings, title);
    if (!sigmaFormula) continue;
    sigmaFormula = sigmaFormula.replace(/\[([^\]\/]+)\]/g, (_m: string, colName: string) =>
      qlikColToDisplayName[colName] ? `[${qlikColToDisplayName[colName]}]` : _m
    );
    const targetEl = elements[0];
    if (!targetEl) continue;
    const colId = sigmaShortId();
    const fmt: any = inferSigmaFormat(sigmaFormula, title);
    const col: any = { id: colId, formula: sigmaFormula, name: title };
    if (fmt) col.format = fmt;
    targetEl.columns.push(col);
    (targetEl.order as string[]).push(colId);
  }

  for (const de of buildDerivedElements(elements)) elements.push(de);

  const stats = {
    elements: elements.length,
    columns: elements.reduce((n, e) => n + (e.columns?.length || 0), 0),
    metrics: elements.reduce((n, e) => n + (e.metrics?.length || 0), 0),
    relationships: elements.reduce((n, e) => n + (e.relationships?.length || 0), 0),
  };

  return {
    model: { name: sigmaDisplayName(modelName), schemaVersion: 1, pages: [{ id: sigmaShortId(), name: 'Page 1', elements }] },
    warnings,
    stats,
  };
}

// ── QVD ingestion ───────────────────────────────────────────────────────────
// QVD (Qlik Data) files are Qlik's proprietary binary data extract format used
// by both QlikView and Qlik Sense. Each QVD is a single table.
//
// We only parse the XML header (everything before the first \0 byte after
// </QvdTableHeader>) to recover table name, fields, types, distinct counts.
// The binary symbol + index tables that follow are skipped — Sigma re-pulls
// data from the warehouse on save.
//
// Format spec: https://pyqvd.readthedocs.io/stable/guide/qvd-file-format.html

export interface QvdFile {
  /** filename, used as the table name fallback when <TableName> is empty */
  name: string;
  /** raw bytes of the .qvd file */
  buffer: Uint8Array | Buffer;
}

export interface QvdHeaderInfo {
  tableName: string;
  noOfRecords: number;
  fields: Array<{
    name: string;
    type: string;        // QVD NumberFormat.Type — UNKNOWN, INTEGER, REAL, DATE, etc.
    tags: string[];      // Qlik tags — $key, $numeric, $text, $timestamp, etc.
    noOfSymbols: number; // distinct value count
  }>;
}

function _decodeXmlEntity(s: string): string {
  return s.replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&apos;/g, "'");
}

/** Read tag text content. Returns '' if tag is absent or self-closing/empty. */
function _xmlText(scope: string, tag: string): string {
  const m = scope.match(new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`));
  return m ? _decodeXmlEntity(m[1].trim()) : '';
}

/** Slice between first <Tag> ... last </Tag> (single occurrence per scope). */
function _xmlSection(scope: string, tag: string): string {
  const m = scope.match(new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`));
  return m ? m[1] : '';
}

/** Parse a QVD file's XML header. Reads up to first \0 byte after </QvdTableHeader>. */
export function parseQvdHeader(buf: Uint8Array | Buffer): QvdHeaderInfo {
  // Find end of XML header — terminated by \r\n\0 or just \0 after the closing tag.
  const len = buf.length;
  let endIdx = -1;
  for (let i = 0; i < len; i++) {
    if (buf[i] === 0) { endIdx = i; break; }
  }
  if (endIdx < 0) throw new Error('QVD header: no NUL terminator found — not a valid QVD file');
  const xml = Buffer.from(buf.slice(0, endIdx)).toString('utf8');
  if (!xml.includes('<QvdTableHeader>')) throw new Error('QVD header: missing <QvdTableHeader> root element');

  const tableName = _xmlText(xml, 'TableName');
  const noOfRecords = parseInt(_xmlText(xml, 'NoOfRecords') || '0', 10);

  const fieldsScope = _xmlSection(xml, 'Fields');
  const fields: QvdHeaderInfo['fields'] = [];
  const fieldRe = /<QvdFieldHeader>([\s\S]*?)<\/QvdFieldHeader>/g;
  let m: RegExpExecArray | null;
  while ((m = fieldRe.exec(fieldsScope))) {
    const f = m[1];
    const name = _xmlText(f, 'FieldName');
    const numFmt = _xmlSection(f, 'NumberFormat');
    const type = _xmlText(numFmt, 'Type') || 'UNKNOWN';
    const tagsScope = _xmlSection(f, 'Tags');
    const tagRe = /<String>([^<]+)<\/String>/g;
    const tags: string[] = [];
    let tm: RegExpExecArray | null;
    while ((tm = tagRe.exec(tagsScope))) tags.push(tm[1]);
    const noOfSymbols = parseInt(_xmlText(f, 'NoOfSymbols') || '0', 10);
    if (name) fields.push({ name, type, tags, noOfSymbols });
  }
  return { tableName: tableName || '', noOfRecords, fields };
}

/** Convert QVD header info → qtr table entry that matches Qlik Engine API export shape. */
function _qvdHeaderToQtrTable(h: QvdHeaderInfo, fallbackName: string): any {
  return {
    qName: h.tableName || fallbackName,
    qNoOfRows: h.noOfRecords,
    qFields: h.fields.map(f => ({
      qName: f.name,
      qnTotalDistinctValues: f.noOfSymbols,
      qnRows: h.noOfRecords,
      qTags: f.tags,
    })),
  };
}

/**
 * Convert one or more QVD files to a Sigma data model spec.
 *
 * Each QVD is one table. Implicit Qlik associations across tables (shared
 * field names) are resolved by the existing `convertQlikToSigma` pipeline.
 *
 * The QVD format does not include the load script, so the converter cannot
 * recover database/schema/table paths — pass them via opts.
 */
export function convertQvdsToSigma(
  qvds: QvdFile[],
  options: QlikConvertOptions = {},
): ConversionResult {
  const headers: QvdHeaderInfo[] = [];
  const warnings: string[] = [];
  for (const qf of qvds) {
    try {
      const h = parseQvdHeader(qf.buffer);
      headers.push(h);
    } catch (e: any) {
      warnings.push(`${qf.name}: failed to parse QVD header — ${e.message}`);
    }
  }
  const qtr = headers.map((h, i) => {
    const qf = qvds[i];
    const fallback = (qf.name || '').replace(/\.qvd$/i, '').toUpperCase();
    return _qvdHeaderToQtrTable(h, fallback);
  });
  const synthetic = {
    appName: 'Qlik QVDs',
    qtr,
    masterMeasures: [],
    masterDimensions: [],
  };
  const result = convertQlikToSigma(synthetic, options);
  result.warnings = [...warnings, ...result.warnings];
  return result;
}

// ── Internal helpers ────────────────────────────────────────────────────────

function qlikParseInput(raw: any): { tables: any[]; masterMeasures: any[]; masterDimensions: any[]; appName: string } {
  let tables: any[] = [], masterMeasures: any[] = [], masterDimensions: any[] = [], appName = '';
  if (Array.isArray(raw?.qtr)) {
    appName = raw.appName || raw.qAppId || 'Qlik App';
    tables = raw.qtr.map((t: any) => ({
      name: t.qName || '',
      noOfRows: t.qNoOfRows || 0,
      fields: (t.qFields || []).map((f: any) => ({
        name: f.qName || '',
        distinctValueCount: f.qnTotalDistinctValues || f.qnPresentDistinctValues || 0,
        noOfRows: f.qnRows || t.qNoOfRows || 0,
        isSystem: (f.qName || '').startsWith('$'),
      })),
    }));
    masterMeasures = raw.masterMeasures || [];
    masterDimensions = raw.masterDimensions || [];
  } else if (Array.isArray(raw?.tables)) {
    appName = raw.appName || raw.appId || 'Qlik App';
    tables = raw.tables.map((t: any) => ({
      name: t.name || t.qName || '',
      noOfRows: t.noOfRows || t.qNoOfRows || 0,
      fields: (t.fields || t.qFields || []).map((f: any) => ({
        name: f.name || f.qName || '',
        distinctValueCount: f.distinctValueCount || f.qDistinctCount || f.qnTotalDistinctValues || 0,
        noOfRows: t.noOfRows || t.qNoOfRows || 0,
        isSystem: f.isSystem || (f.name || f.qName || '').startsWith('$') || false,
        isHidden: f.isHidden || false,
      })),
    }));
    masterMeasures = raw.masterMeasures || [];
    masterDimensions = raw.masterDimensions || [];
  }
  return { tables, masterMeasures, masterDimensions, appName };
}

function qlikExprToSigma(expr: string, warnings: string[], name: string): string | null {
  if (!expr?.trim()) return null;
  let f = expr.trim();
  if (f.startsWith('=')) f = f.slice(1).trim();

  if (/\{\s*[\$1][^}]*\}/.test(f)) {
    warnings?.push(`"${name}": uses Qlik Set Analysis. In Sigma, use SumIf/CountIf with a condition argument instead.`);
    return null;
  }
  if (/\bAggr\s*\(/i.test(f)) {
    warnings?.push(`"${name}": uses Aggr() — no direct Sigma equivalent.`);
    return null;
  }
  if (/\bDual\s*\(/i.test(f)) {
    warnings?.push(`"${name}": uses Dual() — Qlik-specific function.`);
    return null;
  }
  if (/\bGet(?:Field)?(?:Selections?|CurrentSelections?|PossibleCount|SelectedCount|AlternativeCount|ExcludedCount)\s*\(/i.test(f)) {
    warnings?.push(`"${name}": uses a Qlik selection-state function — no Sigma equivalent.`);
    return null;
  }
  if (/\bClass\s*\(/i.test(f)) {
    warnings?.push(`"${name}": uses Class() (Qlik data binning). Use If() ranges for bucketing in Sigma.`);
    return null;
  }
  if (/\bRange(?:Sum|Avg|Min|Max|Count|Stdev|Mode|Skew|Kurtosis|Correl|Fractile)\s*\(/i.test(f)) {
    warnings?.push(`"${name}": uses a Qlik Range aggregation function — no direct Sigma equivalent.`);
    return null;
  }

  f = f.replace(/\bOnly\s*\(\s*(\[[^\]]+\])\s*\)/gi, '$1');
  f = f.replace(/\bMinString\s*\(/gi, 'Min(').replace(/\bMaxString\s*\(/gi, 'Max(');
  f = f.replace(/\bFabs\s*\(/gi, 'Abs(');
  f = f.replace(/\bFrac\s*\(\s*([^)]+)\)/gi, '$1 - Trunc($1)');
  f = f.replace(/\bSqrt\s*\(/gi, 'Sqrt(');
  f = f.replace(/\bPow\s*\(\s*([^,]+),\s*([^)]+)\)/gi, 'Power($1, $2)');
  f = f.replace(/\bLog10\s*\(/gi, 'Log10(').replace(/\bLog\s*\(/gi, 'Ln(');
  f = f.replace(/\bExp\s*\(/gi, 'Exp(');
  f = f.replace(/\bCeil\s*\(/gi, 'Ceiling(');
  f = f.replace(/\bFmod\s*\(\s*([^,]+),\s*([^)]+)\)/gi, 'Mod($1, $2)');
  f = f.replace(/\bDiv\s*\(\s*([^,]+),\s*([^)]+)\)/gi, 'Trunc($1 / $2)');
  f = f.replace(/\bSubStringCount\s*\(/gi, 'RegexpCount(');
  f = f.replace(/\bIndex\s*\(\s*([^,]+),\s*([^,)]+)(?:,\s*([^)]+))?\)/gi,
    (_m, s, sub, occ) => occ ? `IndexOf(${s}, ${sub}, ${occ})` : `IndexOf(${s}, ${sub})`);
  f = f.replace(/\bLTrim\s*\(/gi, 'Ltrim(').replace(/\bRTrim\s*\(/gi, 'Rtrim(');
  f = f.replace(/\bRepeat\s*\(/gi, 'Repeat(');
  f = f.replace(/\bConcat\s*\(/gi, 'ListAgg(');
  f = f.replace(/\bNum\s*\(\s*([^,)]+)(,([^)]+))?\)/gi, (_m, val, hasComma, fmt) => {
    if (hasComma && warnings) warnings.push(`"${name}": Num() format argument "${(fmt||'').trim()}" stripped.`);
    return val.trim();
  });
  f = f.replace(/\bText\s*\(/gi, 'ToString(').replace(/\bDate\$\s*\(/gi, 'ToString(');
  f = f.replace(/\bIsNum\s*\(/gi, 'IsNumber(');
  f = f.replace(/\bIsText\s*\(\s*([^)]+)\)/gi, '!IsNumber($1)');
  f = f.replace(/\bNull\s*\(\s*\)/gi, 'null');
  f = f.replace(/\bWeekDay\s*\(/gi, 'Weekday(');
  f = f.replace(/\bYearToDate\s*\(\s*([^)]+)\)/gi, (_m, field) => {
    warnings?.push(`"${name}": YearToDate() approximated as Year(${field.trim()}) = Year(Today())`);
    return `Year(${field}) = Year(Today())`;
  });
  f = f.replace(/'([^']*)'/g, '"$1"');
  return f.trim();
}
