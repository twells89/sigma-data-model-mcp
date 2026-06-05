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
    // Sigma resolves a warehouse-table element's identifier from the path-tail
    // uppercase when no explicit `name` is set. We deliberately do NOT set
    // `name` on base elements so the prefix in column formulas (also path-tail
    // uppercase) matches the resolver.
    const tablePrefix = t.name.toUpperCase();

    const visibleFields = (t.fields || []).filter((f: any) =>
      f.name && !f.isSystem && !f.isHidden && !f.name.startsWith('$')
    );

    for (const f of visibleFields) {
      const displayName = sigmaDisplayName(f.name);
      const colId = sigmaShortId();
      columns.push({ id: colId, formula: `[${tablePrefix}/${displayName}]` });
      order.push(colId);
      colMap[f.name] = { colId, displayName };
    }

    const pathParts: string[] = [];
    if (dbOverride)  pathParts.push(dbOverride);
    if (schOverride) pathParts.push(schOverride);
    pathParts.push(tablePrefix);

    const element: SigmaElement = {
      id: elementId, kind: 'table',
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
  const _distinctRatio = (info: any, fieldName: string): number => {
    const f = info.fields.find((x: any) => x.name === fieldName);
    const d = f ? (f.distinctValueCount || f.cardinal || 0) : 0;
    return info.rowCount > 0 && d > 0 ? d / info.rowCount : 0;
  };
  // How PK-like (dimension-like) a table is for a shared key: real cardinality when
  // available, else a heuristic (non-fact tables win; fewer rows as a tiebreak).
  const _pkScore = (tableName: string, fieldName: string): number => {
    const info = tableElementMap[tableName];
    const r = _distinctRatio(info, fieldName);
    if (r > 0) return r;
    const isFact = /FACT|FACTS|_FCT|TRANSACTIONS?$/i.test(tableName);
    return (isFact ? 0 : 1) + 1 / (1 + (info.rowCount || 0));
  };
  const _addRel = (fromInfo: any, toInfo: any, fieldName: string) => {
    const relKey = [fromInfo.elementId, toInfo.elementId].sort().join('|') + '|' + fieldName;
    if (createdRels.has(relKey)) return;
    createdRels.add(relKey);
    const fromColInfo = fromInfo.colMap[fieldName];
    const toColInfo   = toInfo.colMap[fieldName];
    if (!fromColInfo || !toColInfo) return;
    if (!fromInfo.element.relationships) fromInfo.element.relationships = [];
    const tgtPath = toInfo.element.source?.path;
    fromInfo.element.relationships.push({
      id: sigmaShortId(),
      targetElementId: toInfo.elementId,
      keys: [{ sourceColumnId: fromColInfo.colId, targetColumnId: toColInfo.colId }],
      name: tgtPath ? tgtPath[tgtPath.length - 1].toUpperCase() : fieldName.toUpperCase(),
    });
  };

  for (const [fieldName, tableNames] of Object.entries(fieldToTables)) {
    if (tableNames.length < 2) continue;

    if (tableNames.length === 2) {
      // Two tables sharing a key → single relationship, directed toward the PK side.
      const infoA = tableElementMap[tableNames[0]];
      const infoB = tableElementMap[tableNames[1]];
      if (!infoA || !infoB) continue;
      const aRatio = _distinctRatio(infoA, fieldName);
      const bRatio = _distinctRatio(infoB, fieldName);
      const hasPkSide = aRatio >= 0.9 || bRatio >= 0.9;
      const noInfo    = aRatio === 0 && bRatio === 0;
      if (!hasPkSide && !noInfo) continue;
      // Direct toward the PK side. With cardinality, that's the higher ratio; without
      // it, the higher _pkScore (non-fact / smaller table) — so facts point at dims.
      const aIsPk = noInfo
        ? _pkScore(tableNames[0], fieldName) >= _pkScore(tableNames[1], fieldName)
        : aRatio >= bRatio;
      const toInfo   = aIsPk ? infoA : infoB;
      const fromInfo = aIsPk ? infoB : infoA;
      _addRel(fromInfo, toInfo, fieldName);
      continue;
    }

    // ≥3 tables share this key (e.g. two facts + a conformed dimension). Link each
    // table to the single inferred key owner (the dim), NEVER fact↔fact — a direct
    // fact-to-fact join on a dim key is a fan trap that double-counts.
    const pkTable = [...tableNames].sort((x, y) => _pkScore(y, fieldName) - _pkScore(x, fieldName))[0];
    const pkInfo = tableElementMap[pkTable];
    warnings.push(`Field "${fieldName}" links ${tableNames.length} tables (${tableNames.join(', ')}). Linked each to "${pkTable}" (inferred key owner); no fact-to-fact join created — review in Sigma.`);
    if (!pkInfo) continue;
    for (const other of tableNames) {
      if (other === pkTable) continue;
      const otherInfo = tableElementMap[other];
      if (otherInfo) _addRel(otherInfo, pkInfo, fieldName);
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

  // Field name (UPPER) → owning element ids, for placing a measure on the element
  // that actually owns the fields it references (matters for multi-fact models).
  const fieldToEl: Record<string, Set<string>> = {};
  for (const info of Object.values(tableElementMap)) {
    for (const [fn, dn] of Object.entries(info.colMap)) {
      for (const key of [fn.toUpperCase(), (dn as any).displayName.toUpperCase()]) {
        (fieldToEl[key] = fieldToEl[key] || new Set()).add(info.elementId);
      }
    }
  }
  // Fact-like elements = relationship targets (dims point at facts). Used to break
  // ties toward the measure's fact when a Set-Analysis condition references a dim.
  const factLike = new Set<string>();
  for (const el of elements) for (const rel of ((el as any).relationships || [])) {
    if (rel.targetElementId) factLike.add(rel.targetElementId);
  }
  const rowCountByEl: Record<string, number> = {};
  for (const info of Object.values(tableElementMap)) rowCountByEl[info.elementId] = info.rowCount || 0;

  for (const m of masterMeasures) {
    const title: string = m.title || m.qTitle || 'Metric';
    const exprRaw: string = m.expr || m.qDef || m.expression || '';
    let sigmaFormula = qlikExprToSigma(exprRaw, warnings, title);
    if (!sigmaFormula) continue;
    sigmaFormula = sigmaFormula.replace(/\[([^\]\/]+)\]/g, (_m: string, colName: string) =>
      qlikColToDisplayName[colName] ? `[${qlikColToDisplayName[colName]}]` : _m
    );
    // Score every element by how many of the measure's field references it owns;
    // resolve bare Qlik names (Sum(UNITS_ON_HAND)) as well as bracketed display names.
    const tokens = new Set<string>();
    for (const t of (exprRaw.match(/[A-Za-z_][A-Za-z0-9_]*/g) || [])) tokens.add(t.toUpperCase());
    for (const t of (sigmaFormula.match(/\[([^\]\/]+)\]/g) || [])) tokens.add(t.slice(1, -1).toUpperCase());
    const hits: Record<string, number> = {};
    for (const tok of tokens) for (const id of (fieldToEl[tok] || [])) hits[id] = (hits[id] || 0) + 1;
    let bestElementId = elements[0]?.id;
    const ranked = Object.keys(hits).sort((a, b) =>
      (hits[b] - hits[a]) ||
      ((factLike.has(b) ? 1 : 0) - (factLike.has(a) ? 1 : 0)) ||
      ((rowCountByEl[b] || 0) - (rowCountByEl[a] || 0)));
    if (ranked.length) bestElementId = ranked[0];
    if (ranked.length > 1) {
      warnings.push(`ℹ "${title}": references fields from ${ranked.length} elements — placed on the most-referenced one. If it errors as cross-element, host it on the denormalized element instead.`);
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

  // Build derived elements up front so calc dims with cross-element refs can
  // be placed on a derived "<Table> View" and rewritten to [SRC/REL/Field] form.
  const derivedEls = buildDerivedElements(elements);
  for (const de of derivedEls) elements.push(de);

  // Display-name → element-id reverse map (for warehouse-table elements only)
  // Built from each element's column formulas of form [TABLE/Display Name].
  const displayNameToElementIds: Record<string, Set<string>> = {};
  for (const el of elements) {
    if (el.source?.kind !== 'warehouse-table') continue;
    for (const c of (el.columns || [])) {
      if (!c.formula) continue;
      const m = c.formula.match(/^\[[^\]\/]+\/([^\]]+)\]$/);
      if (!m) continue;
      const dn = m[1].toUpperCase();
      if (!displayNameToElementIds[dn]) displayNameToElementIds[dn] = new Set();
      displayNameToElementIds[dn].add(el.id);
    }
  }
  // Also map raw qlik field names → element ids (parsing happens before display rewrite)
  const qlikNameToElementIds: Record<string, Set<string>> = {};
  for (const [tableName, info] of Object.entries(tableElementMap)) {
    void tableName;
    for (const fieldName of Object.keys(info.colMap)) {
      const k = fieldName.toUpperCase();
      if (!qlikNameToElementIds[k]) qlikNameToElementIds[k] = new Set();
      qlikNameToElementIds[k].add(info.elementId);
    }
  }

  // Build per-source-element maps of related-column display name → REL name,
  // for rewriting bare [X] refs to [SRC/REL/X] triple form.
  const relatedNameMapBySrc: Record<string, Record<string, string>> = {};
  for (const srcEl of elements) {
    if (srcEl.source?.kind !== 'warehouse-table') continue;
    if (!(srcEl as any).relationships?.length) continue;
    const srcPath = srcEl.source.path || [];
    const srcBaseName = (srcEl as any).name || srcPath[srcPath.length - 1] || '';
    if (!srcBaseName) continue;
    const map: Record<string, string> = {};
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
        if (!(dispName in map)) map[dispName] = `${srcBaseName}/${rel.name}/${dispName}`;
      }
    }
    relatedNameMapBySrc[srcEl.id] = map;
  }

  // Helper: find a derived element backed by a source element id.
  const derivedBySrc: Record<string, SigmaElement> = {};
  for (const de of derivedEls) {
    const srcId = (de.source as any)?.elementId;
    if (srcId) derivedBySrc[srcId] = de;
  }

  // Pass 4: Calculated master dimensions → columns (placed by ref scope)
  for (const d of masterDimensions) {
    const title: string = d.title || d.qTitle || 'Dimension';
    const exprRaw: string = d.fieldDef || d.qFieldDef || d.expr || d.expression || '';
    const isCalc = exprRaw.trim().startsWith('=') ||
      /\b(If|Sum|Count|Avg|Concat|Year|Month|Day|Left|Right|Upper|Lower|Trim|Class|Dual|Floor|Ceil|Round|Pick|Match)\s*\(/i.test(exprRaw);
    if (!isCalc) continue;
    let sigmaFormula = qlikExprToSigma(exprRaw, warnings, title);
    if (!sigmaFormula) continue;

    // Resolve which element each ref belongs to BEFORE rewriting names — we
    // need both the raw qlik names and post-rewrite display names to count.
    const refsRaw = (sigmaFormula.match(/\[([^\]\/]+)\]/g) || [])
      .map(r => r.slice(1, -1))
      .filter(r => !/^(true|false|null)$/i.test(r));
    const elementHits: Record<string, number> = {};
    for (const ref of refsRaw) {
      const upper = ref.toUpperCase();
      const ids = qlikNameToElementIds[upper] ||
        displayNameToElementIds[upper] || new Set<string>();
      for (const id of ids) elementHits[id] = (elementHits[id] || 0) + 1;
    }

    sigmaFormula = sigmaFormula.replace(/\[([^\]\/]+)\]/g, (_m: string, colName: string) =>
      qlikColToDisplayName[colName] ? `[${qlikColToDisplayName[colName]}]` : _m
    );

    const distinctElIds = Object.keys(elementHits);
    const colId = sigmaShortId();
    const fmt: any = inferSigmaFormat(sigmaFormula, title);
    const col: any = { id: colId, formula: sigmaFormula, name: title };
    if (fmt) col.format = fmt;

    if (distinctElIds.length === 1) {
      // All refs resolve to one element → place directly.
      const targetEl = elements.find(e => e.id === distinctElIds[0]);
      if (!targetEl) continue;
      targetEl.columns.push(col);
      (targetEl.order as string[]).push(colId);
    } else if (distinctElIds.length > 1) {
      // Refs span elements → place on derived view of the element with most refs
      // and rewrite cross-element refs to [SRC/REL/Field] triple form.
      const srcElId = distinctElIds.sort((a, b) =>
        (elementHits[b] || 0) - (elementHits[a] || 0))[0];
      const de = derivedBySrc[srcElId];
      const srcEl = elements.find(e => e.id === srcElId);
      const relMap = relatedNameMapBySrc[srcElId] || {};
      if (!de) {
        warnings.push(`⚠ Calc dimension "${title}" has cross-element refs but no derived element exists for ${srcEl ? (srcEl as any).name : srcElId} — column dropped`);
        continue;
      }
      // Rewrite refs: anything in relMap → triple form. Local refs stay bare.
      col.formula = (col.formula as string).replace(/\[([^\]\/]+)\]/g, (m: string, refName: string) => {
        return relMap[refName] ? `[${relMap[refName]}]` : m;
      });
      (de.columns as any[]).push(col);
      (de.order as string[]).push(colId);
      warnings.push(`ℹ Calc dimension "${title}" placed on derived "${(de as any).name}" (cross-element refs)`);
    } else {
      // No refs resolved (e.g. literal-only formula or unknown fields) → fall
      // back to elements[0] to preserve existing behaviour.
      const targetEl = elements.find(e => e.source?.kind === 'warehouse-table');
      if (!targetEl) continue;
      targetEl.columns.push(col);
      (targetEl.order as string[]).push(colId);
    }
  }

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

/** Split `s` on top-level occurrences of `sep`, respecting () [] {} and quotes. */
function _splitTop(s: string, sep = ','): string[] {
  const out: string[] = [];
  let depth = 0, start = 0, q = '';
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (q) { if (c === q) q = ''; continue; }
    if (c === '"' || c === "'") { q = c; continue; }
    else if (c === '(' || c === '[' || c === '{') depth++;
    else if (c === ')' || c === ']' || c === '}') depth--;
    else if (c === sep && depth === 0) { out.push(s.slice(start, i)); start = i + 1; }
  }
  out.push(s.slice(start));
  return out.map(x => x.trim());
}

/** Bracket a bare Qlik field token (so the caller's display-name rewrite catches it).
 *  Leaves already-bracketed refs, numbers, and quoted strings alone. */
function _maybeBracket(tok: string): string {
  const t = tok.trim();
  if (!t) return t;
  if (/^\[.*\]$/.test(t)) return t;                       // already bracketed
  if (/^-?\d+(\.\d+)?$/.test(t)) return t;                // numeric literal
  if (/^['"].*['"]$/.test(t)) return `"${t.slice(1, -1)}"`; // quoted string literal
  if (/^[A-Za-z_][A-Za-z0-9_.]*$/.test(t)) return `[${t}]`; // bare field name
  return t;                                               // expression — leave as-is
}

/** Translate ONE Qlik set-modifier field clause (`FIELD OP {values}`) to a Sigma condition.
 *  Returns null when too complex (search wildcards, $()-expansion, set operators). */
function _setClauseToCond(clause: string): string | null {
  const m = clause.match(/^\s*([A-Za-z_][\w.]*)\s*(-=|\+=|\*=|\/=|=)\s*\{([^}]*)\}\s*$/);
  if (!m) return null;
  const [, field, op, valuesRaw] = m;
  if (op === '+=' || op === '*=' || op === '/=') return null; // set arithmetic — too complex
  if (/[\$]/.test(valuesRaw)) return null;                    // dollar-expansion
  const vals = _splitTop(valuesRaw).map(v => v.trim()).filter(Boolean);
  if (!vals.length) return null;
  for (const v of vals) if (/[*?<>]/.test(v) && !/^['"]/.test(v)) return null; // search/range — too complex
  const eq = op === '-=' ? '<>' : '=';
  const join = op === '-=' ? ' and ' : ' or ';
  const parts = vals.map(v => `[${field}]${eq}${_maybeBracket(v)}`);
  return parts.length === 1 ? parts[0] : `(${parts.join(join)})`;
}

/** Translate a SIMPLE Qlik Set-Analysis aggregation to a Sigma If-wrapped aggregate.
 *  Returns null when the expression is not a translatable set agg (caller warns + drops). */
function _translateSetAnalysis(f: string): string | null {
  const head = f.match(/^(Sum|Count|Avg|Min|Max)\s*\(/i);
  if (!head) return null;
  const agg = head[1];
  // Balance the outer parens to isolate the aggregation argument.
  let depth = 0, argStart = -1, argEnd = -1;
  for (let i = head[0].length - 1; i < f.length; i++) {
    if (f[i] === '(') { if (depth === 0) argStart = i + 1; depth++; }
    else if (f[i] === ')') { depth--; if (depth === 0) { argEnd = i; break; } }
  }
  if (argStart < 0 || argEnd < 0 || f.slice(argEnd + 1).trim()) return null; // trailing → not a bare set agg
  let inner = f.slice(argStart, argEnd).trim();
  if (inner[0] !== '{') return null;                       // no set modifier
  // Balance the set braces.
  let bd = 0, setEnd = -1;
  for (let i = 0; i < inner.length; i++) {
    if (inner[i] === '{') bd++;
    else if (inner[i] === '}') { bd--; if (bd === 0) { setEnd = i; break; } }
  }
  if (setEnd < 0) return null;
  const setStr = inner.slice(1, setEnd).trim();
  let valueStr = inner.slice(setEnd + 1).trim();
  if (/[\$]/.test(setStr)) return null;                    // dollar-expansion in set

  // Parse the set modifier into a list of conditions.
  let conds: string[] = [];
  let setBody = setStr;
  // Leading set identifier: 1 (= all records, ignore selections) or $/named state → only 1 is handled.
  const idMatch = setBody.match(/^\s*(1|\$\d*|[A-Za-z_]\w*)\s*(<.*>)?\s*$/s);
  if (idMatch && idMatch[1] !== '1' && !setBody.startsWith('<')) return null; // bookmark/alt-state
  setBody = setBody.replace(/^\s*1\s*/, '');               // strip leading 1 (ignore-selection)
  if (setBody) {
    const fm = setBody.match(/^<(.*)>$/s);
    if (!fm) return null;
    for (const clause of _splitTop(fm[1])) {
      if (!clause.trim()) continue;
      const c = _setClauseToCond(clause);
      if (!c) return null;
      conds.push(c);
    }
  }
  const cond = conds.join(' and ');

  // Handle DISTINCT and bracket the value field.
  let distinct = false;
  const dm = valueStr.match(/^DISTINCT\s+(.*)$/is);
  if (dm) { distinct = true; valueStr = dm[1].trim(); }
  const value = _maybeBracket(valueStr);

  if (/^Count$/i.test(agg)) {
    const wrapped = cond ? `If(${cond}, ${value})` : value;
    return distinct ? `CountDistinct(${wrapped})` : `Count(${wrapped})`;
  }
  return cond ? `${agg}(If(${cond}, ${value}))` : `${agg}(${value})`;
}

/** Translate a row-wise (multi-arg) Qlik Range* function to Sigma arithmetic.
 *  Returns null for the single-arg inter-record form (e.g. RangeSum(Above(...))). */
function _translateRange(f: string): string | null {
  const m = f.match(/^Range(Sum|Avg|Max|Min)\s*\(/i);
  if (!m) return null;
  const fn = m[1];
  let depth = 0, start = -1, end = -1;
  for (let i = m[0].length - 1; i < f.length; i++) {
    if (f[i] === '(') { if (depth === 0) start = i + 1; depth++; }
    else if (f[i] === ')') { depth--; if (depth === 0) { end = i; break; } }
  }
  if (start < 0 || end < 0 || f.slice(end + 1).trim()) return null;
  const args = _splitTop(f.slice(start, end));
  if (args.length < 2) return null;                        // single-arg = inter-record → drop
  const wrapped = args.map(a => `(${a.trim()})`);
  if (/^Sum$/i.test(fn)) return wrapped.join(' + ');
  if (/^Avg$/i.test(fn)) return `(${wrapped.join(' + ')}) / ${args.length}`;
  if (/^Max$/i.test(fn)) return `Greatest(${args.map(a => a.trim()).join(', ')})`;
  if (/^Min$/i.test(fn)) return `Least(${args.map(a => a.trim()).join(', ')})`;
  return null;
}

function qlikExprToSigma(expr: string, warnings: string[], name: string): string | null {
  if (!expr?.trim()) return null;
  let f = expr.trim();
  if (f.startsWith('=')) f = f.slice(1).trim();

  // --- Variable expansion: $(...) cannot be resolved and POST-blocks the whole DM. ---
  if (/\$\s*\(/.test(f)) {
    warnings?.push(`"${name}": uses a Qlik variable/dollar-expansion $(...) — cannot resolve; measure dropped. Define it explicitly in Sigma.`);
    return null;
  }

  // --- Inter-record / chart-position / selection-state / ranking functions have no
  //     scalar Sigma equivalent. Emitting them verbatim produces a silently broken
  //     metric (or fails the POST), so drop + warn instead of passing through. ---
  const UNSUPPORTED: Array<[RegExp, string]> = [
    [/\b(Above|Below|Before|After|Top|Bottom)\s*\(/i, 'an inter-record/chart function (Above/Below/…)'],
    [/\b(Peek|Previous|Exists|FieldValue|FieldIndex|LookUp)\s*\(/i, 'a script/inter-record lookup function'],
    [/\b(RowNo|RecNo|NoOfRows|NoOfColumns|FirstSortedValue)\s*\(/i, 'a row-position/sorted-value function'],
    [/\b(Rank|HRank|VRank)\s*\(/i, 'Rank() (Sigma Rank is a window function with different semantics)'],
    [/\bAggr\s*\(/i, 'Aggr() — build a grouped/level data-model element instead'],
    [/(?<![A-Za-z_])[PE]\s*\(/, 'a set-element function P()/E()'],
    [/\bGet(?:Field)?(?:Selections?|CurrentSelections?|PossibleCount|SelectedCount|AlternativeCount|ExcludedCount)\s*\(/i, 'a selection-state function'],
  ];
  for (const [re, label] of UNSUPPORTED) {
    if (re.test(f)) {
      warnings?.push(`"${name}": uses ${label} — no direct Sigma equivalent; measure dropped (see gap-scout).`);
      return null;
    }
  }

  // --- Set Analysis: translate the simple single-/multi-flag forms; drop the rest. ---
  if (/\{.*\}/s.test(f)) {
    const t = _translateSetAnalysis(f);
    if (t) { f = t; }
    else {
      warnings?.push(`"${name}": uses Qlik Set Analysis Sigma can't auto-translate (search/$()/set operators) — measure dropped. Use SumIf/CountIf manually.`);
      return null;
    }
  }

  // --- Row-wise Range* (multiple args) → arithmetic / Greatest / Least. ---
  if (/^Range(?:Sum|Avg|Max|Min)\s*\(/i.test(f)) {
    const t = _translateRange(f);
    if (t) { f = t; }
    else {
      warnings?.push(`"${name}": uses an inter-record Range* aggregation (e.g. running total) — no direct Sigma equivalent; measure dropped.`);
      return null;
    }
  } else if (/\bRange(?:Count|Stdev|Mode|Skew|Kurtosis|Correl|Fractile)\s*\(/i.test(f)) {
    warnings?.push(`"${name}": uses a Qlik Range aggregation function — no direct Sigma equivalent; measure dropped.`);
    return null;
  }

  // --- Dual(text, num) → keep the numeric (2nd) argument; surface the label separately. ---
  f = f.replace(/\bDual\s*\(/gi, 'DUAL(');
  while (/DUAL\(/.test(f)) {
    const i = f.indexOf('DUAL(');
    const open = i + 4;
    let depth = 0, end = -1;
    for (let j = open; j < f.length; j++) { if (f[j] === '(') depth++; else if (f[j] === ')') { depth--; if (depth === 0) { end = j; break; } } }
    if (end < 0) { f = f.replace(/DUAL\(/g, 'Dual('); break; }
    const inner = f.slice(open + 1, end);
    const parts = _splitTop(inner);
    const numArg = parts.length >= 2 ? parts[parts.length - 1] : inner;
    warnings?.push(`"${name}": Dual() reduced to its numeric argument; the text label was dropped.`);
    f = f.slice(0, i) + numArg + f.slice(end + 1);
  }

  // --- Class(field, n) binning → Floor(field / n) * n (lower bin edge). ---
  f = f.replace(/\bClass\s*\(\s*([^,()]+?)\s*,\s*([^,()]+?)\s*(?:,[^()]*)?\)/gi,
    (_m, field, size) => `Floor(${_maybeBracket(field)} / ${size.trim()}) * ${size.trim()}`);

  // --- Count(DISTINCT x) → CountDistinct(x) (Sigma has no DISTINCT keyword). ---
  f = f.replace(/\bCount\s*\(\s*DISTINCT\s+(.+?)\s*\)/gi, 'CountDistinct($1)');

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
