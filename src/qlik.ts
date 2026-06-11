/**
 * Qlik Sense metadata JSON → Sigma Data Model converter.
 * Accepts the JSON from Qlik's Engine API GetTablesAndKeys or the REST metadata endpoint.
 */

import {
  resetIds, sigmaShortId, sigmaDisplayName,
  inferSigmaFormat, buildDerivedElements,
  makeRlsSecurity, makeClsSecurity,
  type SigmaElement, type SigmaColumn, type SigmaMetric, type ConversionResult, type SecurityRule,
  type WorkbookPattern,
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
  const security: SecurityRule[] = [];   // Section Access RLS/CLS — reported, not injected (architecture B)
  const workbookPatterns: WorkbookPattern[] = [];  // inter-record/window calcs — reported, not injected (same architecture)

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

  // Aggr() helper SQL elements built during Pass 3 (appended after the loop).
  const aggrElements: SigmaElement[] = [];

  for (const m of masterMeasures) {
    const title: string = m.title || m.qTitle || 'Metric';
    const exprRaw: string = m.expr || m.qDef || m.expression || '';
    const ctx: QlikExprCtx = { patterns: workbookPatterns };
    let sigmaFormula = qlikExprToSigma(exprRaw, warnings, title, ctx);
    if (!sigmaFormula) continue;

    // Aggr() lowering — qlikExprToSigma tags these with QLIK_AGGR_SENTINEL.
    if (sigmaFormula.startsWith(QLIK_AGGR_SENTINEL)) {
      const aggrExpr = sigmaFormula.slice(QLIK_AGGR_SENTINEL.length);
      const lowered = lowerQlikAggr(aggrExpr, title, tableElementMap, connectionId, warnings);
      if (!lowered) continue;            // degraded — warning already pushed
      aggrElements.push(lowered.element);
      const metric: any = { id: sigmaShortId(), formula: lowered.metricFormula, name: title };
      if (m.description || m.qDescription) metric.description = m.description || m.qDescription;
      const fmt = inferSigmaFormat(lowered.metricFormula, title);
      if (fmt) metric.format = fmt;
      if (!lowered.element.metrics) lowered.element.metrics = [];
      lowered.element.metrics.push(metric);
      continue;
    }

    // FirstSortedValue() lowering — qlikExprToSigma tags with QLIK_FSV_SENTINEL.
    // SQL QUALIFY helper element where the simple form resolves to one table;
    // otherwise degrade to the Rank=n-filter workbook pattern (verify-me).
    if (sigmaFormula.startsWith(QLIK_FSV_SENTINEL)) {
      const fsvExpr = sigmaFormula.slice(QLIK_FSV_SENTINEL.length);
      const lowered = lowerQlikFirstSortedValue(fsvExpr, title, tableElementMap, connectionId, warnings);
      if (lowered) {
        aggrElements.push(lowered.element);
        const metric: any = { id: sigmaShortId(), formula: lowered.metricFormula, name: title };
        if (m.description || m.qDescription) metric.description = m.description || m.qDescription;
        if (!lowered.element.metrics) lowered.element.metrics = [];
        lowered.element.metrics.push(metric);
      } else {
        const fp = fsvRankPattern(fsvExpr, warnings, title);
        if (fp.formula) {
          fp.formula = tidyFormula(bracketKnownBareFields(fp.formula, qlikColToDisplayName).replace(/\[([^\]\/]+)\]/g, (_m2: string, colName: string) =>
            qlikColToDisplayName[colName] ? `[${qlikColToDisplayName[colName]}]` : _m2));
        }
        workbookPatterns.push(fp);
        warnings.push(`ℹ "${title}": FirstSortedValue() → Rank=n-filter workbook pattern (result.workbookPatterns) — build it in a GROUPED workbook element and VERIFY values against Qlik.`);
      }
      continue;
    }

    // Bracket bare Qlik field tokens (real master items use unbracketed refs,
    // e.g. RangeSum(Sum(NET_REVENUE), …)) so the raw→display rewrite resolves them.
    sigmaFormula = bracketKnownBareFields(sigmaFormula, qlikColToDisplayName).replace(/\[([^\]\/]+)\]/g, (_m: string, colName: string) =>
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
    // Inter-record/window expression → workbook-pattern handoff, NOT a DM
    // metric: window functions (Rank/Lag/Lead) silently error in DM calc
    // columns/metrics and workbook master calc columns; they only work in
    // GROUPED workbook elements (live-verified 2026-06-11, exact vs warehouse
    // RANK/DENSE_RANK/LAG/LEAD). Mirrors the PBI time-intel handoff.
    if (ctx.window) {
      const bestEl = elements.find(e => e.id === bestElementId);
      const bestPath = bestEl?.source?.path;
      workbookPatterns.push({
        kind: ctx.kind || 'rank',
        name: title,
        source: exprRaw,
        formula: tidyFormula(sigmaFormula),
        requires: QLIK_GROUPED_REQUIRES,
        elementId: bestElementId,
        elementName: (bestEl as any)?.name || (bestPath ? bestPath[bestPath.length - 1] : undefined),
        ...(ctx.verify ? { verify: true } : {}),
        note: ctx.notes?.length ? ctx.notes.join(' ') : 'Translated Qlik inter-record expression.',
      });
      warnings.push(`ℹ "${title}": inter-record/window expression → ready Sigma formula in result.workbookPatterns — place as a calculation in a GROUPED workbook element (group by the chart's dimension); not emitted as a DM metric (window functions silently error there).`);
      continue;
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
  // Append Aggr() SQL helper elements (carry their own metric already attached).
  for (const ae of aggrElements) elements.push(ae);

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
      /\b(If|Sum|Count|Avg|Concat|Year|Month|Day|Left|Right|Upper|Lower|Trim|Class|Dual|Range\w+|Floor|Ceil|Round|Pick|Match|Mid|Replace|WeekDay|Date|Num|Rank|HRank|Above|Below|Peek|Previous|FirstSortedValue)\s*\(/i.test(exprRaw);
    if (!isCalc) continue;
    const ctx: QlikExprCtx = { patterns: workbookPatterns };
    let sigmaFormula = qlikExprToSigma(exprRaw, warnings, title, ctx);
    if (!sigmaFormula) continue;
    // FirstSortedValue() in a calc dimension → Rank=n-filter workbook pattern
    // (the SQL helper lowering is metric-shaped; a dim wants the picked value
    // per group, which is exactly the grouped Rank=n pattern).
    if (sigmaFormula.startsWith(QLIK_FSV_SENTINEL)) {
      const fp = fsvRankPattern(sigmaFormula.slice(QLIK_FSV_SENTINEL.length), warnings, title);
      if (fp.formula) {
        fp.formula = tidyFormula(bracketKnownBareFields(fp.formula, qlikColToDisplayName).replace(/\[([^\]\/]+)\]/g, (_m2: string, colName: string) =>
          qlikColToDisplayName[colName] ? `[${qlikColToDisplayName[colName]}]` : _m2));
      }
      fp.note += ' (Qlik master dimension.)';
      workbookPatterns.push(fp);
      warnings.push(`ℹ Calc dimension "${title}": FirstSortedValue() → Rank=n-filter workbook pattern (result.workbookPatterns) — build in a GROUPED workbook element and VERIFY.`);
      continue;
    }
    // Bracket bare Qlik field tokens (real master dims use unbracketed refs,
    // e.g. Class(UNIT_PRICE, 50)) so ref-resolution + the raw→display rewrite work.
    sigmaFormula = bracketKnownBareFields(sigmaFormula, qlikColToDisplayName);

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

    // Inter-record/window expression in a calc dimension → workbook-pattern
    // handoff, NOT a DM column (window functions silently error there).
    if (ctx.window) {
      workbookPatterns.push({
        kind: ctx.kind || 'rank',
        name: title,
        source: exprRaw,
        formula: tidyFormula(sigmaFormula),
        requires: QLIK_GROUPED_REQUIRES,
        ...(ctx.verify ? { verify: true } : {}),
        note: (ctx.notes?.length ? ctx.notes.join(' ') : 'Translated Qlik inter-record expression.') + ' (Qlik master dimension.)',
      });
      warnings.push(`ℹ Calc dimension "${title}": inter-record/window expression → ready Sigma formula in result.workbookPatterns — place in a GROUPED workbook element; not emitted as a DM column (window functions silently error there).`);
      continue;
    }

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

  // ── Qlik Section Access → Sigma RLS (row REDUCTION) + CLS (OMIT column) ────
  // Section Access lives in the load SCRIPT (not the model metadata), so it must
  // be supplied as a parsed `sectionAccess` object. REDUCTION = strict-exclusion
  // row reduction (unlisted ⇒ hidden) → maps cleanly to Sigma's fail-closed
  // include-True filter. GROUP-keyed → CurrentUserInTeam; USERID-keyed → user
  // attribute. OMIT = per-user column hide → Sigma CLS (flattened, flagged).
  const sa: any = (rawJson as any).sectionAccess;
  if (typeof sa === 'string') {
    warnings.push('⚠ Qlik SECTION ACCESS supplied as raw script — not auto-parsed. Pass a parsed { reductionFields[], omitFields[], keyedBy } object to port it.');
  } else if (sa && typeof sa === 'object') {
    const findField = (name: string) => {
      const up = (name || '').toUpperCase().replace(/\s+/g, '_');
      for (const info of Object.values(tableElementMap)) {
        for (const [fn, ci] of Object.entries(info.colMap)) {
          if (fn.toUpperCase() === up || ci.displayName.toUpperCase().replace(/\s+/g, '_') === up) return { el: info.element as any, disp: ci.displayName, colId: ci.colId };
        }
      }
      return null;
    };
    const keyedBy = (sa.keyedBy || 'group').toLowerCase();
    const reductions: string[] = sa.reductionFields || (sa.reductionField ? [sa.reductionField] : []);
    for (const rf of reductions) {
      const hit = findField(rf);
      if (!hit) { warnings.push(`⚠ Section Access REDUCTION field "${rf}" not found in the model — re-apply RLS manually.`); continue; }
      const formula = keyedBy === 'userid'
        ? `CurrentUserAttributeText("${sigmaDisplayName(rf)}") = [${hit.disp}]`
        : `CurrentUserInTeam([${hit.disp}])`;
      security.push(makeRlsSecurity({ source: `Qlik Section Access REDUCTION on [${hit.disp}]`, element: hit.el, name: `RLS: ${hit.disp}`, formula }));
      warnings.push(`🔐 Qlik Section Access REDUCTION on [${hit.disp}] → row-level security DETECTED (reported in result.security, not injected; strict-exclusion ≡ fail-closed). ${keyedBy === 'userid' ? `The skill provisions user attribute "${sigmaDisplayName(rf)}" per user (multi-value reductions need an or-chain)` : `The skill recreates the Qlik GROUP values as Sigma teams`} and applies the RLS calc + filter.`);
    }
    for (const om of (sa.omitFields || (sa.omitField ? [sa.omitField] : []))) {
      const hit = findField(om);
      if (!hit) continue;
      security.push(makeClsSecurity({ source: `Qlik Section Access OMIT [${hit.disp}]`, element: hit.el, columnIds: [hit.colId], columnNames: [hit.disp], note: 'Qlik OMIT is per-user/group; Sigma CLS is no-one-can-view (or re-scope to a team/attribute allowlist). The skill applies it — not injected.' }));
      warnings.push(`🔐 Qlik Section Access OMIT [${hit.disp}] → column-level security DETECTED (reported in result.security, not injected).`);
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
    ...(security.length ? { security } : {}),
    ...(workbookPatterns.length ? { workbookPatterns } : {}),
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

// ── Set Analysis translation ─────────────────────────────────────────────────
// Qlik Set Analysis filters an aggregation:  Sum({<A={1}, B={'x','y'}>} EXPR)
// We lower it to a conditional aggregation:   Sum(If(<conds>, EXPR, 0))
// with AND across clauses, OR-chains across a clause's element list (Sigma has
// no IsIn — see feedback_sigma_formula_isin), <> for exclusion (A-={x}), and
// comparison operators for numeric-range search strings ({">=2020"}).
//
// Set identifiers that aren't a plain field modifier — alternate states,
// $-expansions, P()/E() set functions, set operators (+/-/*) on the set itself —
// are left untranslated (the caller degrades+flags).

const QLIK_SET_AGGS = ['Sum', 'Count', 'Avg', 'Min', 'Max', 'Median', 'Only'] as const;

/** Find the index of the matching close brace/paren for the char at `open`. */
function matchClose(s: string, open: number, oc: string, cc: string): number {
  let depth = 0;
  for (let i = open; i < s.length; i++) {
    const ch = s[i];
    if (ch === "'" || ch === '"') {
      const q = ch; i++;
      while (i < s.length && s[i] !== q) i++;
      continue;
    }
    if (ch === oc) depth++;
    else if (ch === cc) { depth--; if (depth === 0) return i; }
  }
  return -1;
}

/** Split on a top-level delimiter (not inside quotes/braces/parens). */
function splitTopLevel(s: string, delim: string): string[] {
  const out: string[] = [];
  let depth = 0, start = 0;
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (ch === "'" || ch === '"') {
      const q = ch; i++;
      while (i < s.length && s[i] !== q) i++;
      continue;
    }
    if (ch === '{' || ch === '(' || ch === '[') depth++;
    else if (ch === '}' || ch === ')' || ch === ']') depth--;
    else if (depth === 0 && ch === delim) { out.push(s.slice(start, i)); start = i + 1; }
  }
  out.push(s.slice(start));
  return out;
}

/**
 * Lower the row-wise Range aggregations that have a direct Sigma equivalent.
 * RangeSum/RangeAvg fold over the scalar arg list; RangeMin/RangeMax map to
 * Least/Greatest. Null handling: Qlik treats nulls as absent — RangeSum maps
 * them to 0 (Coalesce); RangeAvg uses a fixed denominator (flagged). The
 * statistical variants are left for the residual reject in qlikExprToSigma.
 */
function lowerRangeFns(f: string, warnings: string[], name: string): string {
  const re = /\bRange(Sum|Avg|Min|Max)\s*\(/i;
  let guard = 0;
  let m: RegExpMatchArray | null;
  while ((m = f.match(re)) && guard++ < 50) {
    const fn = m[1].toLowerCase();
    const idx = m.index!;
    const open = f.indexOf('(', idx);
    const close = matchClose(f, open, '(', ')');
    if (close < 0) break;
    const args = splitTopLevel(f.slice(open + 1, close), ',')
      .map(a => a.trim()).filter(a => a.length);
    if (args.length === 0) break;
    let repl: string;
    if (fn === 'sum') {
      repl = '(' + args.map(a => `Coalesce(${a}, 0)`).join(' + ') + ')';
    } else if (fn === 'avg') {
      repl = '((' + args.map(a => `Coalesce(${a}, 0)`).join(' + ') + `) / ${args.length})`;
      warnings?.push(`"${name}": RangeAvg() translated with a fixed denominator (${args.length}); Qlik excludes nulls from the divisor.`);
    } else if (fn === 'min') {
      repl = `Least(${args.join(', ')})`;
    } else {
      repl = `Greatest(${args.join(', ')})`;
    }
    f = f.slice(0, idx) + repl + f.slice(close + 1);
  }
  return f;
}

/**
 * Lower Class(value, interval [, label [, start]]) — Qlik numeric binning — to
 * the numeric lower bound of each bucket. The textual "lo<=x<hi" dual label is
 * not reproduced (flagged); the numeric bound sorts and groups correctly, which
 * is how binned dimensions are consumed downstream.
 */
function lowerClass(f: string, warnings: string[], name: string): string {
  const re = /\bClass\s*\(/i;
  let guard = 0;
  let m: RegExpMatchArray | null;
  while ((m = f.match(re)) && guard++ < 50) {
    const idx = m.index!;
    const open = f.indexOf('(', idx);
    const close = matchClose(f, open, '(', ')');
    if (close < 0) break;
    const args = splitTopLevel(f.slice(open + 1, close), ',').map(a => a.trim());
    const val = args[0];
    const bs = args[1] || '1';
    const start = args[3]; // arg[2] is the optional text label (ignored for numeric output)
    const repl = start
      ? `(Floor((${val} - ${start}) / ${bs}) * ${bs} + ${start})`
      : `(Floor(${val} / ${bs}) * ${bs})`;
    warnings?.push(`"${name}": Class() lowered to each bin's numeric lower bound (Floor); the textual "lo<=x<hi" label is not reproduced.`);
    f = f.slice(0, idx) + repl + f.slice(close + 1);
  }
  return f;
}

/** A single Set Analysis element value → a Sigma condition for `field`. */
function setValueToCondition(field: string, rawVal: string, op: '=' | '<>'): string | null {
  let v = rawVal.trim();
  // Quoted string — may be a search/comparison expression like ">=2020" or a literal.
  const qm = v.match(/^['"](.*)['"]$/);
  if (qm) {
    const inner = qm[1].trim();
    const cmp = inner.match(/^(>=|<=|<>|>|<|=)\s*(.+)$/);
    if (cmp) {
      // numeric/comparison search string → comparison operator
      let cop = cmp[1];
      if (op === '<>') {
        // exclusion of a comparison — negate
        const neg: Record<string, string> = { '>=': '<', '<=': '>', '>': '<=', '<': '>=', '=': '<>', '<>': '=' };
        cop = neg[cop] || cop;
      }
      const rhs = cmp[2].trim();
      const rhsNum = /^-?\d+(\.\d+)?$/.test(rhs);
      return `[${field}]${cop}${rhsNum ? rhs : `"${rhs}"`}`;
    }
    // plain quoted literal
    return `[${field}]${op}"${inner}"`;
  }
  // bare numeric
  if (/^-?\d+(\.\d+)?$/.test(v)) return `[${field}]${op}${v}`;
  // bare token literal
  if (/^[A-Za-z0-9_]+$/.test(v)) return `[${field}]${op}"${v}"`;
  return null;
}

/** Translate one clause `FIELD = {v1, v2}` (or `-=`) → a Sigma boolean. */
function clauseToCondition(clause: string): string | null {
  // operators: =, -= (exclude), += (add — rare, treat as = for a fresh set)
  const m = clause.match(/^\s*\[?([A-Za-z0-9_ .]+?)\]?\s*(-=|\+=|=)\s*\{([\s\S]*)\}\s*$/);
  if (!m) return null;
  const field = m[1].trim();
  const setOp = m[2];
  const op: '=' | '<>' = setOp === '-=' ? '<>' : '=';
  const body = m[3].trim();
  if (body === '') return null;
  // nested set functions / P()/E() / element-set operators → untranslatable
  if (/[+\-*/](?![=\d])/.test(body) && /\}|\{/.test(body)) return null;
  if (/\b[PE]\s*\(/.test(body)) return null;
  const vals = splitTopLevel(body, ',').map(v => v.trim()).filter(Boolean);
  const conds: string[] = [];
  for (const v of vals) {
    const c = setValueToCondition(field, v, op);
    if (!c) return null;
    conds.push(c);
  }
  if (!conds.length) return null;
  if (conds.length === 1) return conds[0];
  // multi-value list: OR for inclusion, AND for exclusion (NOT in)
  const joiner = op === '<>' ? ' and ' : ' or ';
  return `(${conds.join(joiner)})`;
}

/**
 * Bracket bare Qlik field tokens in a measure expression so the downstream
 * display-name rewrite ([RAW_NAME] → [Display Name]) catches them. Leaves
 * already-bracketed refs, quoted strings, numeric literals and function names
 * (token immediately followed by `(`) alone.
 */
function bracketBareFields(expr: string): string {
  const tokens: string[] = [];
  const SENT = '\u0001';
  const stash = (mm: string) => { tokens.push(mm); return ` ${SENT}${tokens.length - 1}${SENT} `; };
  // protect quoted strings and already-bracketed refs
  let s = expr.replace(/'[^']*'|"[^"]*"|\[[^\]]+\]/g, stash);
  // bracket bare identifiers that are NOT function calls (not followed by `(`)
  s = s.replace(/\b([A-Za-z_][A-Za-z0-9_]*)\b(\s*\()?/g, (full, ident, call) => {
    if (call) return full;                       // function name -> leave
    if (/^(null|true|false)$/i.test(ident)) return full;
    return `[${ident}]`;
  });
  // restore protected tokens
  s = s.replace(new RegExp(SENT + '(\\d+)' + SENT, 'g'), (_m, i) => tokens[+i]);
  return s.replace(/ {2,}/g, ' ').trim();
}

/**
 * Bracket bare tokens that are KNOWN Qlik field names (present in `displayMap`),
 * leaving operators (and/or/not), function calls, literals, and already-bracketed
 * refs untouched. Real master items reference fields bare (Class(UNIT_PRICE, 50)),
 * but unlike bracketBareFields this won't mangle `and`/`or` into `[and]`/`[or]`.
 */
function bracketKnownBareFields(expr: string, displayMap: Record<string, string>): string {
  const tokens: string[] = [];
  const SENT = '\u0001';
  const stash = (mm: string) => { tokens.push(mm); return ` ${SENT}${tokens.length - 1}${SENT} `; };
  // protect quoted strings and already-bracketed refs
  let s = expr.replace(/'[^']*'|"[^"]*"|\[[^\]]+\]/g, stash);
  s = s.replace(/\b([A-Za-z_][A-Za-z0-9_]*)\b(\s*\()?/g, (full, ident, call) => {
    if (call) return full;                                  // function name -> leave
    return displayMap[ident] ? `[${ident}]` : full;         // only bracket known fields
  });
  s = s.replace(new RegExp(SENT + '(\\d+)' + SENT, 'g'), (_m, i) => tokens[+i]);
  return s.replace(/ {2,}/g, ' ').trim();
}

/**
 * Translate every `AGG({<set>} EXPR)` occurrence in `f` to `AGG(If(conds, EXPR, 0))`.
 * Returns null on any untranslatable set construct (caller degrades+flags).
 */
function translateSetAnalysis(f: string, warnings: string[], name: string): string | null {
  const aggRe = new RegExp(`\\b(${QLIK_SET_AGGS.join('|')})\\s*\\(\\s*\\{`, 'i');
  let guard = 0;
  while (aggRe.test(f) && guard++ < 50) {
    const m = aggRe.exec(f);
    if (!m) break;
    const aggFn = m[1];
    const parenOpen = f.indexOf('(', m.index);
    const parenClose = matchClose(f, parenOpen, '(', ')');
    if (parenClose < 0) return null;
    const argStr = f.slice(parenOpen + 1, parenClose);
    // set spec is the leading {<...>}
    const braceOpen = argStr.indexOf('{');
    const braceClose = matchClose(argStr, braceOpen, '{', '}');
    if (braceClose < 0) return null;
    const setSpec = argStr.slice(braceOpen, braceClose + 1);   // {<...>}
    const expr = argStr.slice(braceClose + 1).trim();          // measure expr after the set

    // Alternate state (set identifier other than $ / 1 before the <...>), or no modifier block.
    const inner = setSpec.replace(/^\{/, '').replace(/\}$/, '').trim();
    // strip leading set identifier ($, 1, or alternate-state name) up to the < modifier
    const ltIdx = inner.indexOf('<');
    const gtIdx = inner.lastIndexOf('>');
    if (ltIdx < 0 || gtIdx < 0) {
      // {$} or {1} with no modifier — full/current set; just the bare aggregation
      const ident = inner.replace(/[$1\s]/g, '');
      if (ident) { warnings?.push(`"${name}": Set Analysis uses alternate state "${ident}" — left untranslated.`); return null; }
      f = f.slice(0, m.index) + `${aggFn}(${bracketBareFields(expr)})` + f.slice(parenClose + 1);
      continue;
    }
    const setIdent = inner.slice(0, ltIdx).trim();
    if (setIdent && !/^[$1]$/.test(setIdent)) {
      warnings?.push(`"${name}": Set Analysis uses alternate state "${setIdent}" — left untranslated.`);
      return null;
    }
    const modifiers = inner.slice(ltIdx + 1, gtIdx);             // A={1}, B={'x','y'}
    // $-expansion of complex macros inside the set
    if (/\$\(/.test(modifiers)) {
      warnings?.push(`"${name}": Set Analysis contains a $-expansion macro — left untranslated.`);
      return null;
    }
    const clauses = splitTopLevel(modifiers, ',').map(c => c.trim()).filter(Boolean);
    const conds: string[] = [];
    for (const cl of clauses) {
      const c = clauseToCondition(cl);
      if (!c) {
        warnings?.push(`"${name}": Set Analysis clause "${cl}" could not be translated — left untranslated.`);
        return null;
      }
      conds.push(c);
    }
    if (!conds.length || !expr) return null;
    const condJoined = conds.length === 1 ? conds[0] : conds.join(' and ');
    // Bracket bare field tokens in the measure expr so the downstream
    // raw-name → display-name rewrite resolves them.
    const exprBracketed = bracketBareFields(expr);
    const replacement = `${aggFn}(If(${condJoined}, ${exprBracketed}, 0))`;
    f = f.slice(0, m.index) + replacement + f.slice(parenClose + 1);
  }
  // any residual set-spec we didn't handle (e.g. set operator on the measure) → degrade
  if (/\{\s*[\$1<][^}]*\}/.test(f) || /\{\s*<[^}]*>\s*\}/.test(f)) {
    warnings?.push(`"${name}": Set Analysis construct could not be fully translated — left untranslated.`);
    return null;
  }
  return f;
}

/** Sentinel marker for an Aggr() expression that Pass 3 lowers to a SQL element. */
export const QLIK_AGGR_SENTINEL = '__QLIK_AGGR__';

const QLIK_AGG_TO_SQL: Record<string, string> = {
  SUM: 'SUM', COUNT: 'COUNT', AVG: 'AVG', MIN: 'MIN', MAX: 'MAX',
  MEDIAN: 'MEDIAN', ONLY: 'MIN', COUNTDISTINCT: 'COUNT(DISTINCT', NODISTINCT: '',
};

interface QlikAggrLowering { element: SigmaElement; metricFormula: string; }

type QlikTableElementMap = Record<string, {
  elementId: string; colMap: Record<string, { colId: string; displayName: string }>;
  element: SigmaElement; rowCount: number; fields: any[];
}>;

/**
 * Lower a single-level Aggr() to a kind:'sql' helper element.
 *   Sum(Aggr(Sum(SALES), CUSTOMER))
 * → SQL element:  SELECT CUSTOMER, SUM(SALES) AS inner_agg FROM <db.schema.table> GROUP BY CUSTOMER
 *   metric:       Sum([Inner Agg])
 * Returns null (degrade) for nested Aggr, non-aggregate outer ops, or when the
 * grain/inner-agg field can't be resolved to one warehouse table.
 */
function lowerQlikAggr(
  expr: string, name: string, tableElementMap: QlikTableElementMap,
  connectionId: string, warnings: string[],
): QlikAggrLowering | null {
  // OUTER( Aggr( INNER(EXPR), DIM[, DIM...] ) )
  const m = expr.match(/^\s*([A-Za-z_]+)\s*\(\s*Aggr\s*\(/i);
  if (!m) { warnings?.push(`"${name}": Aggr() not wrapped in a single outer aggregation — left untranslated.`); return null; }
  const outerFn = m[1];
  const outerSql = QLIK_AGG_TO_SQL[outerFn.toUpperCase()];
  if (!outerSql || outerSql === '' || outerSql.includes('(')) {
    warnings?.push(`"${name}": Aggr() outer function "${outerFn}" not supported — left untranslated.`);
    return null;
  }
  const aggrOpen = expr.toLowerCase().indexOf('aggr(', m.index ?? 0);
  const aggrParen = expr.indexOf('(', aggrOpen);
  const aggrClose = matchClose(expr, aggrParen, '(', ')');
  if (aggrClose < 0) return null;
  const aggrArgs = splitTopLevel(expr.slice(aggrParen + 1, aggrClose), ',').map(s => s.trim());
  if (aggrArgs.length < 2) { warnings?.push(`"${name}": Aggr() missing grain dimension — left untranslated.`); return null; }
  const innerExpr = aggrArgs[0];
  const dims = aggrArgs.slice(1);

  // reject nested Aggr
  if (/\bAggr\s*\(/i.test(innerExpr)) {
    warnings?.push(`"${name}": nested Aggr() — left untranslated.`);
    return null;
  }
  // inner aggregation:  INNER( FIELD )
  const im = innerExpr.match(/^\s*([A-Za-z_]+)\s*\(\s*\[?([A-Za-z0-9_ .]+?)\]?\s*\)\s*$/);
  if (!im) { warnings?.push(`"${name}": Aggr() inner expression "${innerExpr}" too complex — left untranslated.`); return null; }
  const innerFn = im[1];
  const innerField = im[2].trim();
  const innerSql = QLIK_AGG_TO_SQL[innerFn.toUpperCase()];
  if (innerSql === undefined || innerSql === '') {
    warnings?.push(`"${name}": Aggr() inner function "${innerFn}" not supported — left untranslated.`);
    return null;
  }
  const dimFields = dims.map(d => d.replace(/^\[|\]$/g, '').trim());

  // Resolve a single warehouse table that owns the inner field + all dims.
  let owner: QlikTableElementMap[string] | null = null;
  for (const info of Object.values(tableElementMap)) {
    const has = (n: string) => Object.keys(info.colMap).some(k => k.toUpperCase() === n.toUpperCase());
    if (has(innerField) && dimFields.every(has)) { owner = info; break; }
  }
  if (!owner) {
    warnings?.push(`"${name}": Aggr() grain spans tables or fields not found in one table — left untranslated.`);
    return null;
  }
  const path = owner.element.source?.path || [];
  if (!path.length) { warnings?.push(`"${name}": Aggr() source table path unknown — left untranslated.`); return null; }
  const fromSql = path.map((p: string) => `"${p}"`).join('.');

  const realName = (n: string) =>
    Object.keys(owner!.colMap).find(k => k.toUpperCase() === n.toUpperCase()) || n;
  const dimCols = dimFields.map(realName);
  const innerCol = realName(innerField);
  const innerAlias = 'inner_agg';
  const innerAggSql = innerSql.includes('(')
    ? `${innerSql} "${innerCol}")`              // COUNT(DISTINCT col)
    : `${innerSql}("${innerCol}")`;

  const selectCols = [
    ...dimCols.map(c => `"${c}"`),
    `${innerAggSql} AS "${innerAlias}"`,
  ];
  const groupBy = dimCols.map((_c, i) => i + 1).join(', ');
  const statement = `SELECT ${selectCols.join(', ')} FROM ${fromSql} GROUP BY ${groupBy}`;

  // Build SQL element columns using the qualified [Custom SQL/<SQL_ALIAS>] form
  // — the exact alias the SELECT emits — mirroring the QuickSight window helper.
  // This is the form that resolves for kind:'sql' elements (bare display-name
  // refs do NOT resolve here; verified against the live API).
  const cols: SigmaColumn[] = [];
  const order: string[] = [];
  for (const dc of dimCols) {
    const id = sigmaShortId();
    cols.push({ id, name: sigmaDisplayName(dc), formula: `[Custom SQL/${dc}]` });
    order.push(id);
  }
  const innerColId = sigmaShortId();
  const innerDisplay = sigmaDisplayName(innerAlias);
  cols.push({ id: innerColId, name: innerDisplay, formula: `[Custom SQL/${innerAlias}]` });
  order.push(innerColId);

  const element: SigmaElement = {
    id: sigmaShortId(),
    kind: 'table',
    // SQL elements use the implicit "Custom SQL" element name for column-ref
    // prefixes; a descriptive element name is fine (matches QuickSight helper).
    name: `${name} (Aggr)`,
    source: { connectionId, kind: 'sql', statement },
    columns: cols,
    order,
  };
  const metricFormula = `${outerFn}([${innerDisplay}])`;
  return { element, metricFormula };
}

/** Sentinel marker for a FirstSortedValue() expression that Pass 3 lowers to a
 *  SQL QUALIFY helper element (or the Rank=n-filter workbook pattern). */
export const QLIK_FSV_SENTINEL = '__QLIK_FSV__';

/** Out-params for qlikExprToSigma: set when the translated formula contains
 *  window functions (Rank/Lag/Lead) and must be placed in a GROUPED workbook
 *  element, plus a sink for flag-not-drop pattern entries. */
export interface QlikExprCtx {
  window?: boolean;
  verify?: boolean;
  kind?: 'rank' | 'lag' | 'lead';
  notes?: string[];
  patterns?: WorkbookPattern[];
}

/** Cosmetic cleanup for pattern formulas: the bracketKnownBareFields stash/
 *  restore pads protected tokens with spaces (harmless to Sigma, ugly in a
 *  ready-to-paste formula). Only applied to workbookPatterns output. */
function tidyFormula(f: string): string {
  return f
    .replace(/\(\s+/g, '(')
    .replace(/\s+\)/g, ')')
    .replace(/\s+,/g, ',')
    .replace(/ {2,}/g, ' ')
    .trim();
}

export const QLIK_GROUPED_REQUIRES =
  "Place as a calculation in a GROUPED workbook element: group by the Qlik chart's dimension(s), sort the element to match the chart's sort order, and put this formula at the grouping level. (Spec gotcha, live-verified 2026-06-11: the element-level `sort` field 400s on grouped tables — 'Sort column not found' — so a grouped element computes Lag/Lead over the group key ASCENDING at POST time; apply any other sort in the UI afterwards, or pick the grouping key so ascending order matches the Qlik chart.) When placing in a workbook element, prefix base-column refs with the source element name ([Element/Col]). Window functions (Rank/RankDense/Lag/Lead) silently error in data-model calc columns/metrics and in workbook master calc columns — they only work in grouped workbook elements.";

// Qlik inter-record / chart-position functions. Rank/Above/Below/Previous/Peek
// translate to Sigma window formulas (grouped-element context, live-verified
// exact vs warehouse RANK/LAG/LEAD); HRank/VRank (pivot column-axis rank) and
// the column-segment walkers (Before/After/Top/Bottom) flag-not-drop.
const QLIK_IR_RE = /\b(Rank|HRank|VRank|Above|Below|Before|After|Top|Bottom|Previous|Peek)\s*\(/i;

/**
 * Lower Qlik inter-record functions in-place to Sigma window functions:
 *   Rank(total Expr[, mode[, fmt]])  → Rank(Expr', "desc")          (Qlik: rank 1 = largest)
 *   Above(Expr[, off[, n]])          → Lag(Expr', off)  — n>1 range form expands to a
 *                                      Lag list inside RangeSum/Avg/Min/Max (rolling window)
 *   Below(...)                       → Lead(...)        (negative offsets flip Lag/Lead)
 *   Previous(Expr)                   → Lag(Expr', 1)    (load-order ⇒ verify sort)
 *   Peek('F'[, -n])                  → Lag([F], n)      (row ≥ 0 / table arg = script-time ⇒ flag)
 * Emits __SIGMA_RANK__/__SIGMA_LAG__/__SIGMA_LEAD__ sentinels during the loop so
 * freshly-emitted functions are never re-matched, then restores them.
 * Returns null to DEGRADE (warning + workbookPatterns 'unsupported' entry pushed).
 */
function lowerInterRecordFns(
  f: string, warnings: string[], name: string, original: string, ctx?: QlikExprCtx,
): string | null {
  const flagUnsupported = (note: string): null => {
    warnings?.push(`⚠ "${name}": ${note} — left untranslated (flagged in workbookPatterns).`);
    ctx?.patterns?.push({ kind: 'unsupported', name, source: original, note });
    return null;
  };
  const setKind = (k: 'rank' | 'lag' | 'lead') => {
    if (!ctx) return;
    ctx.window = true;
    if (k === 'rank' || !ctx.kind) ctx.kind = k;   // rank is the headline kind
  };
  const note = (msg: string) => { if (ctx && !(ctx.notes || []).includes(msg)) (ctx.notes = ctx.notes || []).push(msg); };

  let guard = 0;
  let m: RegExpMatchArray | null;
  while ((m = f.match(QLIK_IR_RE)) && guard++ < 50) {
    const fn = m[1].toLowerCase();
    const idx = m.index!;
    const open = f.indexOf('(', idx);
    const close = matchClose(f, open, '(', ')');
    if (close < 0) return flagUnsupported(`unbalanced parentheses in ${m[1]}()`);
    const args = splitTopLevel(f.slice(open + 1, close), ',').map(a => a.trim());

    if (fn === 'hrank' || fn === 'vrank') {
      return flagUnsupported(`${m[1]}() ranks across a pivot table's COLUMN dimension (horizontal rank); Sigma pivots have no spec-level cross-column calculation (that axis is UI-only)`);
    }
    if (fn === 'before' || fn === 'after' || fn === 'top' || fn === 'bottom') {
      return flagUnsupported(`${m[1]}() walks a pivot's column segments (chart-position on the column axis); Sigma has no spec-level equivalent`);
    }

    let repl: string;
    if (fn === 'rank') {
      const inner = (args[0] || '').replace(/^total\b\s*/i, '');
      if (!inner) return flagUnsupported('Rank() missing expression argument');
      if (args.length > 1 && args[1] && args[1] !== '0') {
        warnings?.push(`"${name}": Rank() mode/fmt argument(s) (${args.slice(1).join(', ')}) ignored — Sigma Rank uses standard competition ranking (ties share the lowest rank, gap after); verify tie handling.`);
        if (ctx) ctx.verify = true;
      }
      repl = `__SIGMA_RANK__(${inner}, "desc")`;   // Qlik Rank: largest value gets rank 1
      setKind('rank');
      note("Qlik Rank(expr) ranks the chart's dimension values by expr descending — Sigma Rank(expr, \"desc\") at the grouping level.");
    } else if (fn === 'above' || fn === 'below') {
      let effFn = fn === 'above' ? 'LAG' : 'LEAD';
      const inner = (args[0] || '').replace(/^total\b\s*/i, '');
      if (!inner) return flagUnsupported(`${m[1]}() missing expression argument`);
      let offset = 1;
      if (args.length > 1 && args[1]) {
        if (!/^-?\d+$/.test(args[1])) return flagUnsupported(`${m[1]}() offset "${args[1]}" is not a literal integer`);
        offset = parseInt(args[1], 10);
      }
      if (offset < 0) { effFn = effFn === 'LAG' ? 'LEAD' : 'LAG'; offset = -offset; }
      let count = 1;
      if (args.length > 2 && args[2]) {
        if (!/^\d+$/.test(args[2])) return flagUnsupported(`${m[1]}() count "${args[2]}" is not a literal integer`);
        count = parseInt(args[2], 10);
      }
      if (count > 1) {
        // Range form: Above(expr, off, n) yields n values consumed by a Range
        // aggregation (rolling window). Expand to a Lag/Lead arg list so the
        // existing RangeSum/Avg/Min/Max folding produces the rolling window.
        if (!/\bRange(?:Sum|Avg|Min|Max)\s*\(\s*$/i.test(f.slice(0, idx))) {
          return flagUnsupported(`${m[1]}(expr, offset, ${count}) range form outside a RangeSum/Avg/Min/Max aggregation`);
        }
        if (count > 24) return flagUnsupported(`${m[1]}() range count ${count} too large to expand to a Lag/Lead list`);
        const items: string[] = [];
        for (let i = 0; i < count; i++) {
          const off = offset + i;
          items.push(off === 0 ? inner : `__SIGMA_${effFn}__(${inner}, ${off})`);
        }
        repl = items.join(', ');
      } else {
        repl = offset === 0 ? inner : `__SIGMA_${effFn}__(${inner}, ${offset})`;
      }
      setKind(effFn === 'LAG' ? 'lag' : 'lead');
      note(`Qlik ${m[1]}() reads a neighbouring chart row — Sigma ${effFn === 'LAG' ? 'Lag' : 'Lead'} follows the grouped element's sort order; sort it to match the Qlik chart.`);
      if (ctx) ctx.verify = true;
    } else if (fn === 'previous') {
      const inner = args[0] || '';
      if (!inner) return flagUnsupported('Previous() missing expression argument');
      repl = `__SIGMA_LAG__(${inner}, 1)`;
      setKind('lag');
      if (ctx) ctx.verify = true;
      warnings?.push(`"${name}": Previous() is a Qlik LOAD-ORDER (script) function — translated to Lag(expr, 1), which follows the grouped element's SORT order. Sort the element to reproduce load order, and verify.`);
    } else { // peek
      if (args.length > 2 && args[2]) {
        return flagUnsupported(`Peek() with a table argument reads another table's load buffer (script-time semantics, not chart semantics)`);
      }
      const fieldRaw = (args[0] || '').trim().replace(/^['"\[]/, '').replace(/['"\]]$/, '');
      if (!fieldRaw) return flagUnsupported('Peek() missing field argument');
      let row = -1;
      if (args.length > 1 && args[1]) {
        if (!/^-?\d+$/.test(args[1])) return flagUnsupported(`Peek() row argument "${args[1]}" is not a literal integer`);
        row = parseInt(args[1], 10);
      }
      if (row >= 0) {
        return flagUnsupported(`Peek('${fieldRaw}', ${row}) addresses an ABSOLUTE load-order row index (script-time semantics, not chart semantics)`);
      }
      repl = `__SIGMA_LAG__([${fieldRaw}], ${-row})`;
      setKind('lag');
      if (ctx) ctx.verify = true;
      warnings?.push(`"${name}": Peek() is a Qlik LOAD-ORDER (script) function — translated to Lag([${fieldRaw}], ${-row}), which follows the grouped element's SORT order. Sort the element to reproduce load order, and verify.`);
    }
    f = f.slice(0, idx) + repl + f.slice(close + 1);
  }
  // restore window-function sentinels (kept emitted Rank/Lag/Lead from re-matching)
  return f.replace(/__SIGMA_(RANK|LAG|LEAD)__/g, (_s, w) =>
    w === 'RANK' ? 'Rank' : w === 'LAG' ? 'Lag' : 'Lead');
}

/**
 * Lower a standalone FirstSortedValue([distinct] value, [-]weight[, n]) to a
 * kind:'sql' QUALIFY helper element (mirrors lowerQlikAggr / the validated
 * QuickSight/PBI topn QUALIFY pattern):
 *   FirstSortedValue(CUSTOMER, -Sum(SALES))
 * → SELECT "CUSTOMER" AS "fsv_value" FROM <db.schema.table>
 *   GROUP BY 1 QUALIFY ROW_NUMBER() OVER (ORDER BY SUM("SALES") DESC) = 1
 *   metric: Min([Fsv Value])
 * Returns null to DEGRADE to the Rank=n-filter workbook pattern (complex value
 * expr, set analysis in the weight, multi-table grain, non-literal n).
 */
function lowerQlikFirstSortedValue(
  expr: string, name: string, tableElementMap: QlikTableElementMap,
  connectionId: string, warnings: string[],
): QlikAggrLowering | null {
  const m = expr.match(/^\s*FirstSortedValue\s*\(/i);
  if (!m) return null;
  const open = expr.indexOf('(', m.index!);
  const close = matchClose(expr, open, '(', ')');
  if (close < 0) return null;
  const args = splitTopLevel(expr.slice(open + 1, close), ',').map(a => a.trim());
  if (args.length < 2) return null;

  let valueArg = args[0];
  const distinct = /^distinct\s+/i.test(valueArg);
  if (distinct) valueArg = valueArg.replace(/^distinct\s+/i, '');
  const vm = valueArg.match(/^\[?([A-Za-z0-9_ .]+?)\]?$/);
  if (!vm) return null;                       // complex value expr → pattern fallback
  const valueField = vm[1].trim();

  let weightArg = args[1];
  let dir: 'ASC' | 'DESC' = 'ASC';            // FirstSortedValue sorts ascending; -weight ⇒ descending
  if (/^-/.test(weightArg)) { dir = 'DESC'; weightArg = weightArg.slice(1).trim(); }
  if (/[{}]/.test(weightArg)) return null;    // set analysis in the weight → pattern fallback

  let n = 1;
  if (args.length > 2 && args[2]) {
    if (!/^\d+$/.test(args[2])) return null;
    n = parseInt(args[2], 10);
  }

  // weight: simple aggregate Agg(field) (grain = the value field) or a bare row-level field
  let weightField = '', weightAggSql = '';
  const am = weightArg.match(/^([A-Za-z_]+)\s*\(\s*\[?([A-Za-z0-9_ .]+?)\]?\s*\)$/);
  if (am) {
    const aggSql = QLIK_AGG_TO_SQL[am[1].toUpperCase()];
    if (!aggSql) return null;
    weightField = am[2].trim();
    weightAggSql = aggSql;
  } else {
    const wm = weightArg.match(/^\[?([A-Za-z0-9_ .]+?)\]?$/);
    if (!wm) return null;
    weightField = wm[1].trim();
  }

  // Resolve one warehouse table owning both fields (mirrors lowerQlikAggr).
  let owner: QlikTableElementMap[string] | null = null;
  for (const info of Object.values(tableElementMap)) {
    const has = (nm: string) => Object.keys(info.colMap).some(k => k.toUpperCase() === nm.toUpperCase());
    if (has(valueField) && has(weightField)) { owner = info; break; }
  }
  if (!owner) return null;
  const path = owner.element.source?.path || [];
  if (!path.length) return null;
  const fromSql = path.map((p: string) => `"${p}"`).join('.');
  const realName = (nm: string) => Object.keys(owner!.colMap).find(k => k.toUpperCase() === nm.toUpperCase()) || nm;
  const valCol = realName(valueField);
  const wCol = realName(weightField);
  const alias = 'fsv_value';

  let statement: string;
  if (weightAggSql) {
    const aggExpr = weightAggSql.includes('(') ? `${weightAggSql} "${wCol}")` : `${weightAggSql}("${wCol}")`;
    statement = `SELECT "${valCol}" AS "${alias}" FROM ${fromSql} GROUP BY 1 QUALIFY ROW_NUMBER() OVER (ORDER BY ${aggExpr} ${dir}) = ${n}`;
  } else {
    statement = `SELECT ${distinct ? 'DISTINCT ' : ''}"${valCol}" AS "${alias}" FROM ${fromSql} QUALIFY ROW_NUMBER() OVER (ORDER BY "${wCol}" ${dir}) = ${n}`;
  }

  const colId = sigmaShortId();
  const display = sigmaDisplayName(alias);    // "Fsv Value"
  const element: SigmaElement = {
    id: sigmaShortId(),
    kind: 'table',
    name: `${name} (FirstSortedValue)`,
    source: { connectionId, kind: 'sql', statement },
    columns: [{ id: colId, name: display, formula: `[Custom SQL/${alias}]` }],
    order: [colId],
  };
  warnings?.push(`ℹ "${name}": FirstSortedValue() lowered to a SQL QUALIFY helper element (ROW_NUMBER ${dir} = ${n}). Tie caveat: Qlik returns NULL on a tie at position ${n}; the SQL picks one row — verify.`);
  return { element, metricFormula: `Min([${display}])` };
}

/**
 * Rank=n-filter workbook pattern for FirstSortedValue() forms the SQL lowering
 * can't take (complex value expr / set-analysis weight / multi-table grain):
 *   If(Rank(<weight'>, "asc|desc") = n, <value>, Null)
 * placed in a grouped workbook element grouped by the value's dimension and
 * surfaced with Max()/Min(). Always verify-me.
 */
function fsvRankPattern(expr: string, warnings: string[], name: string): WorkbookPattern {
  const base: WorkbookPattern = {
    kind: 'first-sorted-value',
    name,
    source: expr,
    requires: QLIK_GROUPED_REQUIRES + " Group by the value field's dimension; aggregate the result with Max() (or Min()) to surface the single picked value.",
    verify: true,
    note: 'FirstSortedValue(value, weight[, n]) = the value at sorted-weight position n. Emitted as the Rank=n-filter pattern: rank the groups by the weight and keep rank = n. Tie caveat: Qlik returns NULL on a tie, this pattern picks one row — VERIFY against Qlik.',
  };
  const m = expr.match(/^\s*FirstSortedValue\s*\(/i);
  if (!m) return { ...base, kind: 'unsupported' };
  const open = expr.indexOf('(', m.index!);
  const close = matchClose(expr, open, '(', ')');
  if (close < 0) return { ...base, kind: 'unsupported' };
  const args = splitTopLevel(expr.slice(open + 1, close), ',').map(a => a.trim());
  if (args.length < 2) return { ...base, kind: 'unsupported' };
  const valueArg = args[0].replace(/^distinct\s+/i, '');
  let weightArg = args[1];
  let dir = 'asc';
  if (/^-/.test(weightArg)) { dir = 'desc'; weightArg = weightArg.slice(1).trim(); }
  let n = '1';
  if (args.length > 2 && /^\d+$/.test(args[2] || '')) n = args[2];
  const weightSigma = qlikExprToSigma(weightArg, warnings, `${name} (weight)`);
  if (!weightSigma || weightSigma.startsWith(QLIK_AGGR_SENTINEL) || weightSigma.startsWith(QLIK_FSV_SENTINEL)) {
    return { ...base, kind: 'unsupported' };
  }
  const valueRef = /^\[.*\]$/.test(valueArg) ? valueArg
    : (/^[A-Za-z_][A-Za-z0-9_]*$/.test(valueArg) ? `[${valueArg}]` : valueArg);
  return { ...base, formula: `If(Rank(${weightSigma}, "${dir}") = ${n}, ${valueRef}, Null)` };
}

function qlikExprToSigma(expr: string, warnings: string[], name: string, ctx?: QlikExprCtx): string | null {
  if (!expr?.trim()) return null;
  let f = expr.trim();
  if (f.startsWith('=')) f = f.slice(1).trim();

  // Dual(text, num): a literal dual value. Measures want the numeric part (2nd
  // arg), labels want the text part (1st). Lower to the numeric part by default
  // — it is the part that participates in aggregation — keeping the text only
  // when the numeric arg is itself non-numeric/absent.
  f = f.replace(/\bDual\s*\(/gi, (_m, off) => 'DUAL('); // tag, resolve below
  if (f.includes('DUAL(')) {
    let guard = 0;
    while (f.includes('DUAL(') && guard++ < 50) {
      const idx = f.indexOf('DUAL(');
      const open = f.indexOf('(', idx);
      const close = matchClose(f, open, '(', ')');
      if (close < 0) break;
      const args = splitTopLevel(f.slice(open + 1, close), ',');
      const textPart = (args[0] || '').trim();
      const numPart = (args[1] || '').trim();
      // pick numeric part if present, else fall back to the text part
      let chosen = numPart || textPart || '0';
      // bracket a bare field identifier so the downstream display-name rewrite resolves it
      if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(chosen) && !/^(null|true|false)$/i.test(chosen)) chosen = `[${chosen}]`;
      f = f.slice(0, idx) + chosen + f.slice(close + 1);
    }
  }

  // FirstSortedValue(): the standalone whole-expression form hands off to
  // Pass 3 for SQL QUALIFY lowering (or the Rank=n-filter workbook pattern).
  // Checked BEFORE set-analysis translation so the lowering sees raw args.
  const fsvM = f.match(/^FirstSortedValue\s*\(/i);
  if (fsvM && matchClose(f, f.indexOf('('), '(', ')') === f.length - 1) {
    return QLIK_FSV_SENTINEL + f;
  }
  if (/\bFirstSortedValue\s*\(/i.test(f)) {
    warnings?.push(`⚠ "${name}": FirstSortedValue() nested inside a larger expression — left untranslated (flagged in workbookPatterns).`);
    ctx?.patterns?.push({ kind: 'unsupported', name, source: expr, note: 'FirstSortedValue() nested inside a larger expression; only the standalone form is lowered (SQL QUALIFY helper element or the Rank=n-filter workbook pattern).' });
    return null;
  }

  // Set Analysis — translate to conditional aggregation, or degrade+flag.
  if (/\{\s*[\$1<][^}]*\}/.test(f) || /\{\s*<[^}]*>\s*\}/.test(f)) {
    const translated = translateSetAnalysis(f, warnings, name);
    if (translated === null) return null;
    f = translated;
  }

  // Aggr(): hand off to Pass 3 for SQL-element lowering. Only the simple
  // single-level form Sum(Aggr(<innerAgg>(<expr>), <dim>[, <dim>...])) is
  // attempted; genuinely nested Aggr or non-aggregate outer ops degrade+flag.
  if (/\bAggr\s*\(/i.test(f)) {
    return QLIK_AGGR_SENTINEL + f;
  }
  if (/\bGet(?:Field)?(?:Selections?|CurrentSelections?|PossibleCount|SelectedCount|AlternativeCount|ExcludedCount)\s*\(/i.test(f)) {
    warnings?.push(`"${name}": uses a Qlik selection-state function — no Sigma equivalent.`);
    return null;
  }
  // Inter-record / chart-position functions → Sigma window formulas (Rank/
  // Lag/Lead in grouped-element context) or flag-not-drop. Runs BEFORE the
  // Range lowering so Above(expr, off, n) rolling windows expand to Lag lists
  // that the RangeSum/Avg/Min/Max folding consumes. Set analysis inside the
  // inner expr was already translated above, so Rank(Sum({<…>} X)) wraps the
  // conditional-aggregation form.
  const ir = lowerInterRecordFns(f, warnings, name, expr, ctx);
  if (ir === null) return null;
  f = ir;
  // Row-wise Range aggregations over a scalar arg list translate directly;
  // the statistical variants (Count/Stdev/Mode/Skew/Kurtosis/Correl/Fractile)
  // have no row-wise Sigma equivalent and are flagged below after lowering.
  f = lowerRangeFns(f, warnings, name);
  // Class(): numeric binning -> the numeric lower bound of each bucket.
  f = lowerClass(f, warnings, name);
  if (/\bRange(?:Count|Stdev|Mode|Skew|Kurtosis|Correl|Fractile)\s*\(/i.test(f)) {
    warnings?.push(`"${name}": uses a Qlik Range statistical function — no direct Sigma equivalent.`);
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
