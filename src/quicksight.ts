/**
 * AWS QuickSight → Sigma Data Model JSON converter.
 *
 * Accepts a mix of files, auto-detected by content:
 *   - DescribeAnalysisDefinition JSON   (has `Definition.DataSetIdentifierDeclarations`)
 *   - DescribeDataSet JSON              (has `DataSet.PhysicalTableMap` or top-level `PhysicalTableMap`)
 *   - Asset-bundle wrapped variants     (object whose value is one of the above)
 *
 * Asset-bundle .zip extraction is handled by the caller (pass each extracted JSON file).
 *
 * Translation:
 *   PhysicalTable RelationalTable → warehouse-table element
 *   PhysicalTable CustomSql       → Custom SQL element
 *   PhysicalTable S3Source/SaaS   → Custom SQL placeholder (no direct warehouse path)
 *   LogicalTable JoinInstruction  → Sigma relationship on the left-operand element
 *   LogicalTable DataTransforms:
 *     CreateColumnsOperation      → calculated column (formula via quicksightFormulaToSigma)
 *     RenameColumnOperation       → column display name
 *     CastColumnTypeOperation     → cast wrapper formula
 *     FilterOperation             → boolean calc column "Filter: <expr>"
 *     ProjectOperation            → column order / hide non-projected
 *     TagColumnOperation          → informational warning (Sigma has no equivalent)
 *   AnalysisDefinition.CalculatedFields → calc columns on the element bound to DataSetIdentifier
 *   ParameterDeclarations         → Sigma controls (string/number/date)
 *   FilterGroups                  → informational warnings (analysis-level filters → UI page filters)
 */

import {
  resetIds, sigmaShortId, sigmaInodeId, sigmaDisplayName, sigmaColFormula,
  type SigmaElement, type SigmaColumn, type ConversionResult,
} from './sigma-ids.js';

// ── Public interface ────────────────────────────────────────────────────────

export interface QuickSightFile {
  name: string;
  content: string;
}

export interface QuickSightConvertOptions {
  connectionId?: string;
  database?: string;
  schema?: string;
}

// colMap value: id = Sigma column id, display = Title-Case display name,
// raw = original-case source alias (used for [Custom SQL/RAW] refs on sql
// elements), sql = true when the owning element is a Custom SQL / S3 / SaaS
// element (refs into it must use the [Custom SQL/RAW] qualified form).
interface ColMapEntry { id: string; display: string; raw: string; sql: boolean; }

/** Cross-element ref form for a column living in `entry`'s element. */
function colRefFor(entry: ColMapEntry): string {
  return entry.sql ? `[Custom SQL/${entry.raw}]` : `[${entry.display}]`;
}

/**
 * On a Custom SQL element, a bare display-name ref `[Net Profit]` does not
 * resolve — it must be the SQL-alias form `[Custom SQL/NET_PROFIT]`. Rewrite any
 * bare `[Display]` ref that matches a known base column (by display name) to
 * that form, using the column's raw original-case alias from `colMap`.
 */
function rewriteSqlRefs(formula: string, colMap: Map<string, ColMapEntry>): string {
  // Build a display-name (lowercased) → raw lookup of the element's own columns.
  const byDisplay = new Map<string, ColMapEntry>();
  for (const e of colMap.values()) byDisplay.set(e.display.toLowerCase(), e);
  return formula.replace(/\[([^\]\/]+)\]/g, (match, refName) => {
    const hit = byDisplay.get(String(refName).trim().toLowerCase());
    if (hit && hit.sql) return `[Custom SQL/${hit.raw}]`;
    return match;
  });
}

export function convertQuickSightToSigma(
  files: QuickSightFile[],
  options: QuickSightConvertOptions = {},
): ConversionResult {
  resetIds();

  const { connectionId = '<CONNECTION_ID>', database = '', schema = '' } = options;
  const dbOverride = database.trim().toUpperCase();
  const schemaOverride = schema.trim().toUpperCase();

  const analyses: QSAnalysisDefinition[] = [];
  const datasets: QSDataSet[] = [];
  const warnings: string[] = [];

  // ── Parse + classify input files ──────────────────────────────────────────
  for (const file of files) {
    let parsed: any;
    try {
      parsed = JSON.parse(file.content);
    } catch (e: any) {
      warnings.push(`${file.name}: JSON parse error — ${e.message}`);
      continue;
    }
    const classified = classifyQuickSightJson(parsed);
    if (classified.kind === 'analysis') {
      analyses.push(classified.value);
    } else if (classified.kind === 'dataset') {
      datasets.push(classified.value);
    } else {
      warnings.push(`${file.name}: unrecognized QuickSight JSON shape — skipped (no AnalysisDefinition or DataSet found)`);
    }
  }

  if (analyses.length === 0 && datasets.length === 0) {
    return {
      model: emptyModel('QuickSight Model'),
      warnings: warnings.length ? warnings : ['No QuickSight analysis or dataset JSON found in the provided files'],
      stats: {},
    };
  }

  // ── Datasets → elements ───────────────────────────────────────────────────
  // Maps a dataset ARN/Identifier to the registry entry built from its
  // PhysicalTableMap + LogicalTableMap.
  const datasetRegistry = new Map<string, DatasetEntry>();
  const elements: SigmaElement[] = [];

  // Shared window/table-calc lowering context: window calcs across all datasets
  // + analyses are lowered to kind:'sql' helper elements (SQL OVER clauses),
  // grouped by (source, partition, order) signature so calcs that share an axis
  // coexist in one element. Finalized into real SQL statements at the end.
  const winCtx: QSWindowContext = {
    helpers: new Map(),
    usedAliases: new Set(),
    extraElements: [],
    connectionId,
  };

  for (const ds of datasets) {
    const entry = buildElementsForDataset(ds, { connectionId, dbOverride, schemaOverride, winCtx }, warnings);
    elements.push(...entry.elements);
    if (ds.Arn) datasetRegistry.set(ds.Arn, entry);
    if (ds.DataSetId) datasetRegistry.set(ds.DataSetId, entry);
    if (ds.Name) datasetRegistry.set(ds.Name, entry);
  }

  // ── Pre-build derived views for every source element with relationships ──
  // Analysis-level CalculatedFields land on the derived view (they typically
  // reference joined dim cols, which can only be reached via [SRC/REL/Field]
  // cross-element refs from a derived element).
  const derivedViewBySrcId = new Map<string, SigmaElement>();
  const sourceEls = elements.filter(e => e.source?.kind === 'warehouse-table' && (e.relationships?.length ?? 0) > 0);
  for (const srcEl of sourceEls) {
    const view = buildDerivedView(srcEl, elements);
    if (view) {
      elements.push(view);
      derivedViewBySrcId.set(srcEl.id, view);
    }
  }

  // ── Analyses → calc-col / parameter wiring ────────────────────────────────
  const controls: any[] = [];
  let totalCalcFields = 0;
  let totalParams = 0;

  for (const analysis of analyses) {
    const def = analysis.Definition || {};
    // Per-dataset visual grouping grain: the set of dimension columns a window
    // calc's value must be computed AT (the visual's GroupBy/Category dims).
    // QuickSight table-calc partition/order axes describe how the calc resets
    // *within* that grain — so the helper SQL must aggregate to this grain first,
    // then apply the OVER. Without it, partition-only grain collapses rows and
    // breaks rank / percentOfTotal (always 1 / 100%).
    const visualDimsByDataset = collectVisualGroupingDims(def);
    const identifierMap = new Map<string, DatasetEntry>();
    for (const decl of def.DataSetIdentifierDeclarations || []) {
      const ident = decl.Identifier;
      const arn = decl.DataSetArn;
      let entry: DatasetEntry | undefined;
      if (arn && datasetRegistry.has(arn)) entry = datasetRegistry.get(arn);
      else if (ident && datasetRegistry.has(ident)) entry = datasetRegistry.get(ident);
      if (!entry) {
        entry = synthesizeStubDataset(ident || arn || 'Unknown', { connectionId, dbOverride, schemaOverride }, warnings);
        elements.push(...entry.elements);
        warnings.push(`ℹ Analysis references dataset "${ident}" (ARN ${arn || '?'}) — no DescribeDataSet JSON supplied; emitted a Custom-SQL stub element so calc fields have a home. Re-run with the dataset JSON to resolve warehouse columns.`);
      }
      if (ident) identifierMap.set(ident, entry);
    }

    for (const cf of def.CalculatedFields || []) {
      totalCalcFields++;
      const entry = cf.DataSetIdentifier ? identifierMap.get(cf.DataSetIdentifier) : undefined;
      if (!entry) {
        warnings.push(`⚠ Analysis calc field "${cf.Name}": DataSetIdentifier "${cf.DataSetIdentifier}" not in DataSetIdentifierDeclarations — skipped`);
        continue;
      }
      const grain = (cf.DataSetIdentifier && visualDimsByDataset.get(cf.DataSetIdentifier)) || [];
      addAnalysisCalcCol(entry, cf.Name, cf.Expression, derivedViewBySrcId, elements, winCtx, grain, warnings);
    }

    for (const decl of def.ParameterDeclarations || []) {
      const ctl = parameterDeclarationToControl(decl, warnings);
      if (ctl) {
        controls.push(ctl);
        totalParams++;
      }
    }

    const filterCount = (def.FilterGroups || []).length;
    if (filterCount > 0) {
      warnings.push(`ℹ ${filterCount} analysis-level FilterGroup(s) skipped — these are visual page filters in QuickSight. Re-create as workbook filters/page controls in Sigma.`);
    }
  }

  // ── Finalize window/table-calc SQL helper elements ───────────────────────
  // Each accumulated helper now gets its real WITH base AS (...) SELECT ... OVER
  // statement built; then the helper elements are appended to the model.
  for (const helper of winCtx.helpers.values()) finalizeQSWindowHelper(helper);
  if (winCtx.extraElements.length) {
    elements.push(...winCtx.extraElements);
  }

  // Every element must have a non-empty, model-unique name (beads-sigma-vy4k/nc6g).
  dedupeElementNames(elements);

  // Strip empty arrays
  for (const el of elements) {
    if (el.metrics?.length === 0) delete el.metrics;
    if (el.relationships?.length === 0) delete el.relationships;
  }

  // ── Build page ────────────────────────────────────────────────────────────
  const page: any = {
    id: sigmaShortId(),
    name: 'Page 1',
    elements,
  };
  if (controls.length) page.controls = controls;

  const modelName = analyses.length === 1
    ? sigmaDisplayName(String(analyses[0].Name || analyses[0].AnalysisId || 'QuickSight Analysis'))
    : datasets.length === 1
      ? sigmaDisplayName(String(datasets[0].Name || datasets[0].DataSetId || 'QuickSight DataSet'))
      : 'QuickSight Model';

  return {
    model: { name: modelName, schemaVersion: 1, pages: [page] },
    warnings,
    stats: {
      analyses: analyses.length,
      datasets: datasets.length,
      elements: elements.length,
      columns: elements.reduce((s, e) => s + (e.columns?.length ?? 0), 0),
      relationships: elements.reduce((s, e) => s + (e.relationships?.length ?? 0), 0),
      controls: controls.length,
      calcFields: totalCalcFields,
      params: totalParams,
    },
  };
}

// ── Classification ──────────────────────────────────────────────────────────

type Classification =
  | { kind: 'analysis'; value: QSAnalysisDefinition }
  | { kind: 'dataset'; value: QSDataSet }
  | { kind: 'unknown' };

function classifyQuickSightJson(obj: any): Classification {
  if (!obj || typeof obj !== 'object') return { kind: 'unknown' };

  // Analysis: DescribeAnalysisDefinition response shape OR asset-bundle wrapper
  if (obj.Definition?.DataSetIdentifierDeclarations) {
    return { kind: 'analysis', value: obj as QSAnalysisDefinition };
  }
  if (obj.AnalysisDefinition?.Definition?.DataSetIdentifierDeclarations) {
    return { kind: 'analysis', value: { ...obj, Definition: obj.AnalysisDefinition.Definition, Name: obj.AnalysisDefinition.Name } as QSAnalysisDefinition };
  }
  if (obj.Analysis?.Definition?.DataSetIdentifierDeclarations) {
    return { kind: 'analysis', value: { ...obj.Analysis } as QSAnalysisDefinition };
  }

  // Dataset: DescribeDataSet response OR raw DataSet
  if (obj.DataSet?.PhysicalTableMap) {
    return { kind: 'dataset', value: obj.DataSet as QSDataSet };
  }
  if (obj.PhysicalTableMap) {
    return { kind: 'dataset', value: obj as QSDataSet };
  }
  // Asset-bundle dataset wrapper (rare)
  if (obj.QuickSightDataSet?.PhysicalTableMap) {
    return { kind: 'dataset', value: obj.QuickSightDataSet as QSDataSet };
  }

  return { kind: 'unknown' };
}

// ── Dataset → elements ──────────────────────────────────────────────────────

interface DatasetBuildContext {
  connectionId: string;
  dbOverride: string;
  schemaOverride: string;
  // Window/table-calc lowering context (shared across datasets+analyses). Optional
  // so the stub/test paths that don't translate window calcs can omit it.
  winCtx?: QSWindowContext;
}

interface DatasetEntry {
  // Elements emitted for this dataset (one per logical table). For analyses,
  // calc fields land on the "primary" logical table (the last one with a
  // join, or the only one if no joins).
  elements: SigmaElement[];
  // logicalId → element (includes ones built from PhysicalTableId, JoinInstruction)
  byLogicalId: Map<string, SigmaElement>;
  // logicalId → physicalId map (only when the logical table directly references a physical table)
  logicalToPhysical: Map<string, string>;
  // The element that an analysis calc field should target by default
  primary: SigmaElement;
  // The primary element's colMap (for rewriting analysis calc refs into a SQL element)
  primaryColMap?: Map<string, ColMapEntry>;
}

function buildElementsForDataset(
  ds: QSDataSet,
  ctx: DatasetBuildContext,
  warnings: string[],
): DatasetEntry {
  const elementsByLogical = new Map<string, SigmaElement>();
  const logicalToPhysical = new Map<string, string>();
  const physicalToLogical = new Map<string, string>();
  // colNameMap[logicalId][lowercaseOriginalName] = { id, displayName }
  const colMaps = new Map<string, Map<string, ColMapEntry>>();

  const phys = ds.PhysicalTableMap || {};
  const logical = ds.LogicalTableMap || {};
  const dsName = ds.Name || ds.DataSetId || 'QuickSight DataSet';

  // First pass: build a SigmaElement for each logical table that points at a
  // physical table directly. JoinInstruction-backed logical tables are
  // resolved in a second pass.
  for (const [logicalId, lt] of Object.entries(logical)) {
    if (lt.Source?.PhysicalTableId) {
      const physId = lt.Source.PhysicalTableId;
      const phyTable = phys[physId];
      if (!phyTable) {
        warnings.push(`⚠ Dataset "${dsName}": logical table "${lt.Alias || logicalId}" references missing PhysicalTableId "${physId}" — skipped`);
        continue;
      }
      const { element, colMap } = buildElementFromPhysicalTable(phyTable, lt.Alias || logicalId, ctx, warnings);
      elementsByLogical.set(logicalId, element);
      logicalToPhysical.set(logicalId, physId);
      physicalToLogical.set(physId, logicalId);
      colMaps.set(logicalId, colMap);
      // Apply this logical table's DataTransforms
      applyTransformsToElement(element, lt.DataTransforms || [], colMap, dsName, lt.Alias || logicalId, warnings, ctx.winCtx);
    }
  }

  // If no LogicalTableMap entries were physical-backed (uncommon: dataset
  // omits LogicalTableMap altogether), generate one element per physical
  // table directly.
  if (elementsByLogical.size === 0) {
    for (const [physId, phyTable] of Object.entries(phys)) {
      const alias = inferPhysicalAlias(phyTable, physId);
      const { element, colMap } = buildElementFromPhysicalTable(phyTable, alias, ctx, warnings);
      elementsByLogical.set(physId, element);
      logicalToPhysical.set(physId, physId);
      physicalToLogical.set(physId, physId);
      colMaps.set(physId, colMap);
    }
  }

  // Resolve a join operand transitively to its left-most physical-backed
  // logical id. For a chained 3-way join `(A join B) join C`, the outer join's
  // LeftOperand is the inner join's logicalId, which has no element of its own —
  // follow it down to A so the C relationship attaches to the base fact element.
  const resolveOperand = (operandId: string, seen = new Set<string>()): string => {
    if (elementsByLogical.has(operandId)) return operandId;
    if (seen.has(operandId)) return operandId;
    seen.add(operandId);
    const inner = logical[operandId]?.Source?.JoinInstruction;
    if (inner) return resolveOperand(inner.LeftOperand, seen);
    return operandId;
  };

  // Second pass: JoinInstruction logical tables → relationships on the left
  // element. The "joined" logical table itself is not emitted as a separate
  // Sigma element; its role is captured by the relationship.
  for (const [logicalId, lt] of Object.entries(logical)) {
    const join = lt.Source?.JoinInstruction;
    if (!join) continue;
    const leftOperandId = resolveOperand(join.LeftOperand);
    const rightOperandId = resolveOperand(join.RightOperand);
    const leftEl = elementsByLogical.get(leftOperandId);
    const rightEl = elementsByLogical.get(rightOperandId);
    if (!leftEl || !rightEl) {
      warnings.push(`⚠ Dataset "${dsName}": join "${lt.Alias || logicalId}" left/right operand not resolvable — relationship skipped`);
      continue;
    }
    const leftColMap = colMaps.get(leftOperandId);
    const rightColMap = colMaps.get(rightOperandId);
    const parsed = parseJoinOnClause(join.OnClause || '', join.LeftOperand, join.RightOperand);
    let leftColId: string | undefined;
    let rightColId: string | undefined;
    if (parsed && leftColMap && rightColMap) {
      leftColId = leftColMap.get(parsed.leftCol.toLowerCase())?.id;
      rightColId = rightColMap.get(parsed.rightCol.toLowerCase())?.id;
    }
    // Relationship name = uppercase right-side alias (matches DM convention
    // of [SRC/REL_NAME/Col] cross-element refs). Prefer the right element's own
    // warehouse table name so the rel name lines up with the dim table.
    const rightPath: string[] = (rightEl.source?.path as string[]) || [];
    const rightAlias = (rightPath[rightPath.length - 1]
      || logical[rightOperandId]?.Alias || rightOperandId).toString().toUpperCase().replace(/\s+/g, '_');
    const rel: any = {
      id: sigmaShortId(),
      targetElementId: rightEl.id,
      name: rightAlias,
      relationshipType: join.Type === 'INNER' ? 'N:1' : (join.Type === 'RIGHT' ? '1:N' : 'N:1'),
    };
    if (leftColId && rightColId) {
      rel.keys = [{ sourceColumnId: leftColId, targetColumnId: rightColId }];
    } else {
      warnings.push(`ℹ Dataset "${dsName}": join "${lt.Alias || logicalId}" OnClause "${join.OnClause}" — could not resolve FK/PK column IDs; relationship added without keys`);
    }
    (leftEl.relationships ??= []).push(rel);

    // Apply transforms on the joined logical table to the LEFT element (the
    // join "produces" a combined row set in QuickSight, but in Sigma the
    // joined dim cols are reached via the relationship — so transforms on
    // the joined output that affect left-side columns still apply).
    applyTransformsToElement(leftEl, lt.DataTransforms || [], leftColMap || new Map(), dsName, lt.Alias || logicalId, warnings, ctx.winCtx);
  }

  // Ensure every relationship's join keys are projected on both sides. parseJoin
  // already resolves FK/PK column ids from the colMaps, so the columns exist;
  // this is a no-op guard kept for clarity. (Grouped dims are reached via the
  // [SRC/REL/Field] derived-view columns built later.)

  // Pick the primary element: prefer one with the most relationships
  // (analyses usually wire calc fields to the joined "facts" table).
  const allEls = Array.from(elementsByLogical.values());
  const primary = allEls.length === 1
    ? allEls[0]
    : allEls.slice().sort((a, b) => (b.relationships?.length ?? 0) - (a.relationships?.length ?? 0))[0];

  // Find the logicalId whose element is the primary, to recover its colMap.
  let primaryColMap: Map<string, ColMapEntry> | undefined;
  for (const [lid, el] of elementsByLogical.entries()) {
    if (el === primary) { primaryColMap = colMaps.get(lid); break; }
  }

  return {
    elements: allEls,
    byLogicalId: elementsByLogical,
    logicalToPhysical,
    primary,
    primaryColMap,
  };
}

function inferPhysicalAlias(phy: QSPhysicalTable, physId: string): string {
  if (phy.RelationalTable?.Name) return phy.RelationalTable.Name;
  if (phy.CustomSql?.Name) return phy.CustomSql.Name;
  if (phy.S3Source?.UploadSettings) return physId;
  return physId;
}

function buildElementFromPhysicalTable(
  phy: QSPhysicalTable,
  alias: string,
  ctx: DatasetBuildContext,
  warnings: string[],
): { element: SigmaElement; colMap: Map<string, ColMapEntry> } {
  const colMap = new Map<string, ColMapEntry>();

  if (phy.RelationalTable) {
    const rt = phy.RelationalTable;
    const tableName = (rt.Name || alias).toString();
    // Path: [catalog?, schema?, table]
    let path: string[] = [];
    if (rt.Catalog) path.push(rt.Catalog.toUpperCase());
    if (rt.Schema) path.push(rt.Schema.toUpperCase());
    path.push(tableName.toUpperCase());

    // Apply DB/schema overrides for incomplete paths (path.length === 1)
    if (path.length === 1) {
      const t = path[0];
      if (ctx.dbOverride && ctx.schemaOverride) path = [ctx.dbOverride, ctx.schemaOverride, t];
      else if (ctx.schemaOverride) path = [ctx.schemaOverride, t];
      else if (ctx.dbOverride) path = [ctx.dbOverride, t];
    } else if (path.length === 2 && ctx.dbOverride) {
      path = [ctx.dbOverride, path[0], path[1]];
    }

    const tablePathTail = path[path.length - 1];
    const element: SigmaElement = {
      id: sigmaShortId(),
      kind: 'table',
      name: stripParens(sigmaDisplayName(tableName)),
      source: { connectionId: ctx.connectionId, kind: 'warehouse-table', path },
      columns: [],
      metrics: [],
      order: [],
    };

    for (const ic of rt.InputColumns || []) {
      const id = sigmaInodeId(ic.Name.toUpperCase());
      const display = sigmaDisplayName(ic.Name);
      colMap.set(ic.Name.toLowerCase(), { id, display, raw: ic.Name, sql: false });
      element.columns.push({ id, formula: sigmaColFormula(tablePathTail, ic.Name) });
      element.order.push(id);
    }
    return { element, colMap };
  }

  if (phy.CustomSql) {
    const cs = phy.CustomSql;
    const element: SigmaElement = {
      id: sigmaShortId(),
      kind: 'table',
      name: stripParens(sigmaDisplayName(cs.Name || alias)),
      source: { connectionId: ctx.connectionId, kind: 'sql', statement: cs.SqlQuery || '' },
      columns: [],
      metrics: [],
      order: [],
    };
    // CustomSql.Columns is documented optional but the API rejects null —
    // always emit something. If the source bundle didn't supply column metadata,
    // fall back to InputColumns or warn.
    const cols = cs.Columns || [];
    if (cols.length === 0) {
      warnings.push(`⚠ CustomSql "${cs.Name}" has no Columns metadata — the SQL element will have no surfaced columns. Add them manually after save.`);
    }
    for (const ic of cols) {
      // QuickSight CustomSql.Columns ARE the literal SQL aliases — emit the
      // qualified [Custom SQL/RAW_ALIAS] passthrough form (raw, original case)
      // which resolves against the SQL element. (Diverges from CLAUDE.md rule #3
      // bare-ref default — see beads-sigma-vy4k/nc6g; the QS aliases match.)
      const id = sigmaInodeId(ic.Name.toUpperCase());
      const display = sigmaDisplayName(ic.Name);
      colMap.set(ic.Name.toLowerCase(), { id, display, raw: ic.Name, sql: true });
      element.columns.push({ id, name: display, formula: `[Custom SQL/${ic.Name}]` });
      element.order.push(id);
    }
    return { element, colMap };
  }

  if (phy.S3Source) {
    warnings.push(`ℹ S3Source "${alias}" — Sigma has no direct S3 file connection; emitted as Custom SQL stub. Replace with an external table or warehouse-loaded equivalent.`);
    const element: SigmaElement = {
      id: sigmaShortId(),
      kind: 'table',
      name: stripParens(sigmaDisplayName(alias)),
      source: { connectionId: ctx.connectionId, kind: 'sql', statement: `-- TODO: replace with warehouse SELECT for S3 source "${alias}"\nSELECT 1 AS _placeholder` },
      columns: [],
      metrics: [],
      order: [],
    };
    for (const ic of phy.S3Source.InputColumns || []) {
      const id = sigmaInodeId(ic.Name.toUpperCase());
      const display = sigmaDisplayName(ic.Name);
      colMap.set(ic.Name.toLowerCase(), { id, display, raw: ic.Name, sql: true });
      element.columns.push({ id, name: display, formula: `[Custom SQL/${ic.Name}]` });
      element.order.push(id);
    }
    return { element, colMap };
  }

  if ((phy as any).SaaSTable) {
    warnings.push(`ℹ SaaSTable "${alias}" — Sigma has no direct SaaS connector equivalent; emitted as Custom SQL stub.`);
    const sa = (phy as any).SaaSTable;
    const element: SigmaElement = {
      id: sigmaShortId(),
      kind: 'table',
      name: stripParens(sigmaDisplayName(alias)),
      source: { connectionId: ctx.connectionId, kind: 'sql', statement: `-- TODO: replace with warehouse SELECT for SaaS source "${alias}" (${(sa.TablePath || []).join('.')})\nSELECT 1 AS _placeholder` },
      columns: [],
      metrics: [],
      order: [],
    };
    for (const ic of sa.InputColumns || []) {
      const id = sigmaInodeId(ic.Name.toUpperCase());
      const display = sigmaDisplayName(ic.Name);
      colMap.set(ic.Name.toLowerCase(), { id, display, raw: ic.Name, sql: true });
      element.columns.push({ id, name: display, formula: `[Custom SQL/${ic.Name}]` });
      element.order.push(id);
    }
    return { element, colMap };
  }

  // Unknown physical-table variant: empty stub.
  warnings.push(`⚠ Physical table "${alias}" has no recognized variant (RelationalTable/CustomSql/S3Source/SaaSTable) — emitted empty stub`);
  return {
    element: {
      id: sigmaShortId(),
      kind: 'table',
      name: stripParens(sigmaDisplayName(alias)) || 'Stub',
      source: { connectionId: ctx.connectionId, kind: 'sql', statement: '-- empty placeholder' },
      columns: [],
      metrics: [],
      order: [],
    },
    colMap,
  };
}

function applyTransformsToElement(
  element: SigmaElement,
  transforms: QSDataTransform[],
  colMap: Map<string, ColMapEntry>,
  dsName: string,
  logicalAlias: string,
  warnings: string[],
  winCtx?: QSWindowContext,
): void {
  for (const tx of transforms) {
    if (tx.CastColumnTypeOperation) {
      const op = tx.CastColumnTypeOperation;
      const existing = colMap.get(op.ColumnName.toLowerCase());
      if (!existing) {
        warnings.push(`⚠ ${dsName}/${logicalAlias}: Cast on missing column "${op.ColumnName}" — skipped`);
        continue;
      }
      // Locate the column object on the element and wrap its formula with
      // a Sigma cast function. The original column stays in place.
      const col = element.columns.find(c => c.id === existing.id);
      if (!col) continue;
      const castFn = sigmaCastForType(op.NewColumnType);
      if (castFn) col.formula = `${castFn}(${col.formula})`;
    } else if (tx.CreateColumnsOperation) {
      const elemIsSql = element.source?.kind === 'sql';
      for (const newCol of tx.CreateColumnsOperation.Columns || []) {
        const id = sigmaInodeId(newCol.ColumnName.toUpperCase());
        const display = stripParens(sigmaDisplayName(newCol.ColumnName));
        // Window/table-calc functions silently error in Sigma DM calc columns.
        // Try to lower them to a kind:'sql' helper element (SQL OVER clause)
        // before falling back to the Null degrade in quicksightFormulaToSigmaEx.
        const win = winCtx ? quicksightParseWindow(newCol.Expression || '') : null;
        if (win && winCtx) {
          // Dataset-level calcs have no analysis visual grain available — the
          // helper grain falls back to (partition ∪ order) from the expression.
          const ok = lowerQSWindowCalc(win, newCol.ColumnName, element, colMap, winCtx.helpers, winCtx.usedAliases, winCtx.extraElements, winCtx.connectionId, [], warnings);
          if (ok) continue; // lowered to helper element; no calc col on this element
        }
        const { formula, description } = quicksightFormulaToSigmaEx(newCol.Expression || '', warnings);
        // On a SQL element, bare base-column refs must use [Custom SQL/RAW].
        const rewritten = elemIsSql ? rewriteSqlRefs(formula, colMap) : formula;
        const col: SigmaColumn = { id, formula: rewritten, name: display };
        if (description) col.description = description;
        colMap.set(newCol.ColumnName.toLowerCase(), { id, display, raw: newCol.ColumnName, sql: elemIsSql });
        element.columns.push(col);
        element.order.push(id);
      }
    } else if (tx.RenameColumnOperation) {
      const op = tx.RenameColumnOperation;
      const existing = colMap.get(op.ColumnName.toLowerCase());
      if (!existing) continue;
      const col = element.columns.find(c => c.id === existing.id);
      if (!col) continue;
      const newDisplay = stripParens(sigmaDisplayName(op.NewColumnName));
      col.name = newDisplay;
      colMap.delete(op.ColumnName.toLowerCase());
      // Preserve raw/sql provenance — the underlying SQL alias is unchanged.
      colMap.set(op.NewColumnName.toLowerCase(), { id: existing.id, display: newDisplay, raw: existing.raw, sql: existing.sql });
    } else if (tx.ProjectOperation) {
      // ProjectedColumns is the subset that survives — we reorder element.order
      // to match and drop the rest from order (Sigma still keeps them as data
      // model columns but they'll be off the default projected list).
      const projected = tx.ProjectOperation.ProjectedColumns || [];
      const projectedIds = projected
        .map(name => colMap.get(name.toLowerCase())?.id)
        .filter((id): id is string => !!id);
      if (projectedIds.length) {
        const projectedSet = new Set(projectedIds);
        // Keep projected first in order, others moved to end
        const others = element.order.filter(id => !projectedSet.has(id));
        element.order = [...projectedIds, ...others];
      }
    } else if (tx.FilterOperation) {
      const op = tx.FilterOperation;
      const elemIsSql = element.source?.kind === 'sql';
      let translated = quicksightFormulaToSigma(op.ConditionExpression || '', warnings);
      if (elemIsSql) translated = rewriteSqlRefs(translated, colMap);
      const id = sigmaShortId();
      const name = stripParens(`Filter: ${(op.ConditionExpression || '').slice(0, 40)}`);
      element.columns.push({ id, formula: translated, name });
      element.order.push(id);
      warnings.push(`⚠ ${dsName}/${logicalAlias}: FilterOperation "${(op.ConditionExpression || '').slice(0, 60)}" — a true row-filter genuinely cannot move into a warehouse-table data-model element; it is emitted as an UNAPPLIED boolean calc column "${name}". Downstream counts/aggregates stay UNFILTERED. Apply this as a workbook filter on the boolean column, or push it into the SQL element's WHERE clause.`);
    } else if (tx.TagColumnOperation) {
      warnings.push(`ℹ ${dsName}/${logicalAlias}: TagColumnOperation on "${tx.TagColumnOperation.ColumnName}" skipped (Sigma has no geo-role tagging)`);
    } else if (tx.UntagColumnOperation || tx.OverrideDatasetParameterOperation) {
      // No-op tags
    } else {
      const keys = Object.keys(tx);
      if (keys.length) warnings.push(`ℹ ${dsName}/${logicalAlias}: unsupported transform "${keys[0]}" skipped`);
    }
  }
}

function sigmaCastForType(t: string): string | null {
  switch (t.toUpperCase()) {
    case 'STRING': return 'Text';
    case 'INTEGER': return 'Int';
    case 'DECIMAL': return 'Number';
    case 'DATETIME': return 'Datetime';
    default: return null;
  }
}

function parseJoinOnClause(onClause: string, leftId: string, rightId: string): { leftCol: string; rightCol: string } | null {
  // QuickSight conventional form: `{leftCol} = {rightCol[rightOperandId]}`
  // Both columns may be qualified or unqualified.
  const m = onClause.match(/\{([^}\[]+?)(?:\[([^\]]+)\])?\}\s*=\s*\{([^}\[]+?)(?:\[([^\]]+)\])?\}/);
  if (!m) return null;
  const [, c1, q1, c2, q2] = m;
  // If qualifier is present, it tells us which operand owns the column.
  if (q1 && q2) {
    if (q1 === leftId) return { leftCol: c1.trim(), rightCol: c2.trim() };
    return { leftCol: c2.trim(), rightCol: c1.trim() };
  }
  if (q2 === rightId || q2 === leftId) {
    return q2 === rightId
      ? { leftCol: c1.trim(), rightCol: c2.trim() }
      : { leftCol: c2.trim(), rightCol: c1.trim() };
  }
  // Default assume left-first form
  return { leftCol: c1.trim(), rightCol: c2.trim() };
}

// ── QuickSight window / table-calc parser + SQL-window lowering ─────────────
// QuickSight window & table-calc functions (runningSum, percentOfTotal, rank,
// lag/lead, difference, windowSum, sumOver, period*, etc.) carry their PARTITION
// and ORDER axes inside the expression itself, e.g.
//   runningSum(sum({NET_REVENUE}), [{MONTH_NUMBER} ASC], [{ORDER_CHANNEL}])
//   percentOfTotal(sum({Sales}), [{Region}])
//   rank([sum({Sales}) DESC], [{Region}])
//   difference(sum({rev}), [{month} ASC], -1, [{channel}])
// Sigma DM calc columns silently error on window functions (see
// feedback_sigma_window_functions.md), so — mirroring src/tableau.ts — we lower
// each translatable window calc to a kind:'sql' helper element with an explicit
// SQL `OVER (PARTITION BY ... ORDER BY ...)` clause projected over the dataset's
// underlying physical source. Where the partition/order can't be determined we
// fall back to the existing Null+description degrade.

// Walk an analysis Definition and collect, per DataSetIdentifier, the union of
// dimension column names placed on any visual's grouping shelf (GroupBy /
// Category / Rows / Columns / etc.). These define the grain a window/table-calc
// is evaluated at. Returns datasetIdentifier → array of raw column names.
function collectVisualGroupingDims(def: any): Map<string, string[]> {
  const out = new Map<string, Set<string>>();
  const add = (col: any) => {
    if (!col || !col.DataSetIdentifier || !col.ColumnName) return;
    let s = out.get(col.DataSetIdentifier);
    if (!s) { s = new Set(); out.set(col.DataSetIdentifier, s); }
    s.add(col.ColumnName);
  };
  // CategoricalDimensionField / DateDimensionField / NumericalDimensionField
  // are the dimension wrappers QuickSight uses on grouping shelves.
  const DIM_KEYS = new Set(['CategoricalDimensionField', 'DateDimensionField', 'NumericalDimensionField']);
  const walk = (o: any) => {
    if (Array.isArray(o)) { for (const x of o) walk(x); return; }
    if (!o || typeof o !== 'object') return;
    for (const [k, v] of Object.entries(o)) {
      if (DIM_KEYS.has(k) && v && typeof v === 'object') add((v as any).Column);
      walk(v);
    }
  };
  walk(def?.Sheets || []);
  const result = new Map<string, string[]>();
  for (const [k, s] of out) result.set(k, Array.from(s));
  return result;
}

interface QSWindowResult {
  _isWindow: true;
  // canonical operation, mapped to a SQL window construction below
  op:
    | 'RUNNING_SUM' | 'RUNNING_AVG' | 'RUNNING_COUNT' | 'RUNNING_MAX' | 'RUNNING_MIN'
    | 'PERCENT_OF_TOTAL'
    | 'RANK' | 'DENSE_RANK'
    | 'LAG' | 'LEAD'
    | 'FIRST_VALUE' | 'LAST_VALUE'
    | 'DIFFERENCE' | 'PERCENT_DIFFERENCE'
    | 'WINDOW_SUM' | 'WINDOW_AVG' | 'WINDOW_COUNT' | 'WINDOW_MAX' | 'WINDOW_MIN'
    | 'OVER_SUM' | 'OVER_AVG' | 'OVER_COUNT' | 'OVER_DISTINCT_COUNT' | 'OVER_MAX' | 'OVER_MIN';
  innerAggFunc: string;      // SUM/AVG/COUNT/COUNT_DISTINCT/MIN/MAX — '' for RANK/DENSE_RANK
  innerColRaw: string;       // raw inner column name (original case), '' for RANK-by-measure handled via innerExprSql
  innerExprSql: string;      // SQL form of inner aggregate's *argument* (uppercased identifier), '' for bare rank
  sortFields: { col: string; dir: 'ASC' | 'DESC' }[];  // ORDER BY (raw col names)
  partitionFields: string[]; // PARTITION BY (raw col names)
  offset?: number;           // for LAG/LEAD/DIFFERENCE/PERCENT_DIFFERENCE
  rankSortExprSql?: string;  // for RANK/DENSE_RANK — SQL of the measure to ORDER BY (e.g. SUM(NET_REVENUE))
  rankDir?: 'ASC' | 'DESC';
}

// QS window fn name (lowercased) → canonical op.
const QS_WINDOW_OP: Record<string, QSWindowResult['op']> = {
  runningsum: 'RUNNING_SUM', runningavg: 'RUNNING_AVG', runningcount: 'RUNNING_COUNT',
  runningmax: 'RUNNING_MAX', runningmin: 'RUNNING_MIN',
  percentoftotal: 'PERCENT_OF_TOTAL',
  rank: 'RANK', denserank: 'DENSE_RANK',
  lag: 'LAG', lead: 'LEAD',
  firstvalue: 'FIRST_VALUE', lastvalue: 'LAST_VALUE',
  difference: 'DIFFERENCE', percentdifference: 'PERCENT_DIFFERENCE',
  // periodOverPeriod* are time-LAG variants — treat like difference/lag with offset -1
  periodoverperioddifference: 'DIFFERENCE',
  periodoverperiodpercentdifference: 'PERCENT_DIFFERENCE',
  periodoverperiodlastvalue: 'LAG',
  windowsum: 'WINDOW_SUM', windowavg: 'WINDOW_AVG', windowcount: 'WINDOW_COUNT',
  windowmax: 'WINDOW_MAX', windowmin: 'WINDOW_MIN',
  sumover: 'OVER_SUM', avgover: 'OVER_AVG', countover: 'OVER_COUNT',
  distinctcountover: 'OVER_DISTINCT_COUNT', maxover: 'OVER_MAX', minover: 'OVER_MIN',
};

const QS_AGG_TO_SQL: Record<string, string> = {
  sum: 'SUM', avg: 'AVG', count: 'COUNT', min: 'MIN', max: 'MAX',
  distinct_count: 'COUNT_DISTINCT', distinctcount: 'COUNT_DISTINCT',
};

// Strip the QS {brace} from a field token and return the bare original-case name.
function _qsStripBrace(tok: string): string {
  const m = tok.trim().match(/^\{([^{}]+)\}$/);
  let inner = m ? m[1] : tok.trim();
  // strip a trailing dataset qualifier [dsId]
  inner = inner.replace(/\[[^\]]+\]\s*$/, '').trim();
  return inner;
}

// Parse a sort-spec list token "[{COL} ASC, {COL2} DESC]" → fields with dir.
function _qsParseSortList(tok: string): { col: string; dir: 'ASC' | 'DESC' }[] {
  const inner = tok.trim().replace(/^\[/, '').replace(/\]$/, '');
  if (!inner.trim()) return [];
  return splitTopLevel(inner, ',').map(part => {
    const mm = part.trim().match(/^(.*?)(?:\s+(ASC|DESC))?$/i);
    const rawField = (mm ? mm[1] : part).trim();
    const dir = (mm && mm[2] ? mm[2].toUpperCase() : 'ASC') as 'ASC' | 'DESC';
    return { col: _qsStripBrace(rawField), dir };
  }).filter(f => f.col);
}

// Parse a partition-spec list token "[{COL}, {COL2}]" → bare field names.
function _qsParsePartitionList(tok: string): string[] {
  const inner = tok.trim().replace(/^\[/, '').replace(/\]$/, '');
  if (!inner.trim()) return [];
  return splitTopLevel(inner, ',').map(_qsStripBrace).filter(Boolean);
}

// Parse the inner aggregate measure "sum({NET_REVENUE})" → {func, col}.
function _qsParseInnerAgg(tok: string): { func: string; col: string } | null {
  const m = tok.trim().match(/^([A-Za-z_]+)\s*\(\s*\{([^{}]+)\}\s*\)$/);
  if (!m) return null;
  const func = QS_AGG_TO_SQL[m[1].toLowerCase()];
  if (!func) return null;
  return { func, col: m[2].replace(/\[[^\]]+\]\s*$/, '').trim() };
}

// Uppercase, SQL-identifier-safe form of a raw QS column name.
function _qsColToSql(raw: string): string {
  return raw.replace(/[^A-Za-z0-9_]/g, '_').toUpperCase();
}

/**
 * Parse a QuickSight window/table-calc expression into a structured form, or
 * return null when the expression is not a (recognized) window calc OR its
 * partition/order axes cannot be extracted (caller then degrades to Null).
 */
function quicksightParseWindow(expr: string): QSWindowResult | null {
  const s = (expr || '').trim();
  const m = s.match(/^([A-Za-z_]+)\s*\(([\s\S]*)\)\s*$/);
  if (!m) return null;
  const fn = m[1].toLowerCase();
  const op = QS_WINDOW_OP[fn];
  if (!op) return null;
  const args = splitTopLevel(m[2], ',');
  if (args.length === 0) return null;

  const base = (sortFields: QSWindowResult['sortFields'], partitionFields: string[],
                inner: { func: string; col: string } | null,
                extra: Partial<QSWindowResult> = {}): QSWindowResult => ({
    _isWindow: true, op,
    innerAggFunc: inner?.func || '',
    innerColRaw: inner?.col || '',
    innerExprSql: inner ? _qsColToSql(inner.col) : '',
    sortFields, partitionFields, ...extra,
  });

  switch (op) {
    // measure, [sort], [partition?]
    case 'RUNNING_SUM': case 'RUNNING_AVG': case 'RUNNING_COUNT':
    case 'RUNNING_MAX': case 'RUNNING_MIN': {
      const inner = _qsParseInnerAgg(args[0]);
      if (!inner) return null;
      const sort = args[1] ? _qsParseSortList(args[1]) : [];
      const part = args[2] ? _qsParsePartitionList(args[2]) : [];
      if (sort.length === 0) return null; // running needs an order
      return base(sort, part, inner);
    }
    // measure, [partition]
    case 'PERCENT_OF_TOTAL': {
      const inner = _qsParseInnerAgg(args[0]);
      if (!inner) return null;
      const part = args[1] ? _qsParsePartitionList(args[1]) : [];
      return base([], part, inner);
    }
    // sumOver(measure, [partition], calcLevel?) — partition list is the OVER scope
    case 'OVER_SUM': case 'OVER_AVG': case 'OVER_COUNT':
    case 'OVER_DISTINCT_COUNT': case 'OVER_MAX': case 'OVER_MIN': {
      const inner = _qsParseInnerAgg(args[0]);
      if (!inner) return null;
      const part = args[1] ? _qsParsePartitionList(args[1]) : [];
      return base([], part, inner);
    }
    // rank([sort], [partition]) — sort token may carry a measure expr with ASC/DESC
    case 'RANK': case 'DENSE_RANK': {
      const sortTok = args[0] || '';
      const inner = sortTok.replace(/^\[/, '').replace(/\]$/, '').trim();
      // try measure form: "sum({col}) DESC" or "{col} DESC"
      const dm = inner.match(/^([\s\S]*?)\s+(ASC|DESC)\s*$/i);
      const exprPart = dm ? dm[1].trim() : inner;
      const dir = (dm && dm[2] ? dm[2].toUpperCase() : 'DESC') as 'ASC' | 'DESC';
      let rankSortExprSql = '';
      const innerAgg = _qsParseInnerAgg(exprPart);
      if (innerAgg) {
        rankSortExprSql = innerAgg.func === 'COUNT_DISTINCT'
          ? `COUNT(DISTINCT ${_qsColToSql(innerAgg.col)})`
          : `${innerAgg.func}(${_qsColToSql(innerAgg.col)})`;
      } else {
        const fld = _qsStripBrace(exprPart);
        if (!fld) return null;
        rankSortExprSql = _qsColToSql(fld);
      }
      const part = args[1] ? _qsParsePartitionList(args[1]) : [];
      return base([], part, null, { rankSortExprSql, rankDir: dir });
    }
    // lag/lead(measure, [sort], offset?, [partition?])
    case 'LAG': case 'LEAD': {
      const inner = _qsParseInnerAgg(args[0]);
      if (!inner) return null;
      const sort = args[1] ? _qsParseSortList(args[1]) : [];
      if (sort.length === 0) return null;
      let offset = 1; let partIdx = 2;
      if (args[2] && /^-?\d+$/.test(args[2].trim())) { offset = Math.abs(parseInt(args[2], 10)) || 1; partIdx = 3; }
      const part = args[partIdx] ? _qsParsePartitionList(args[partIdx]) : [];
      return base(sort, part, inner, { offset });
    }
    case 'FIRST_VALUE': case 'LAST_VALUE': {
      const inner = _qsParseInnerAgg(args[0]);
      if (!inner) return null;
      const sort = args[1] ? _qsParseSortList(args[1]) : [];
      if (sort.length === 0) return null;
      const part = args[2] ? _qsParsePartitionList(args[2]) : [];
      return base(sort, part, inner);
    }
    // difference(measure, [sort], offset, [partition]) — LAG-based delta
    case 'DIFFERENCE': case 'PERCENT_DIFFERENCE': {
      const inner = _qsParseInnerAgg(args[0]);
      if (!inner) return null;
      const sort = args[1] ? _qsParseSortList(args[1]) : [];
      if (sort.length === 0) return null;
      let offset = 1; let partIdx = 2;
      if (args[2] && /^-?\d+$/.test(args[2].trim())) { offset = Math.abs(parseInt(args[2], 10)) || 1; partIdx = 3; }
      const part = args[partIdx] ? _qsParsePartitionList(args[partIdx]) : [];
      return base(sort, part, inner, { offset });
    }
    // windowSum(measure, startIndex, endIndex, [partition]) — full-partition agg
    case 'WINDOW_SUM': case 'WINDOW_AVG': case 'WINDOW_COUNT':
    case 'WINDOW_MAX': case 'WINDOW_MIN': {
      const inner = _qsParseInnerAgg(args[0]);
      if (!inner) return null;
      // last arg may be a partition list; numeric start/end indices are ignored
      const last = args[args.length - 1];
      const part = last && /^\[/.test(last.trim()) ? _qsParsePartitionList(last) : [];
      return base([], part, inner);
    }
  }
  return null;
}

// ── QS window helper-element registry ───────────────────────────────────────
// Groups window calcs that share the same (partition, order) signature onto one
// kind:'sql' helper element so multiple OVER columns coexist in a single SELECT.
interface QSWindowContext {
  helpers: Map<string, QSWindowHelper>;
  usedAliases: Set<string>;
  extraElements: SigmaElement[];
  connectionId: string;
}

interface QSWindowHelper {
  element: SigmaElement;
  grainRaw: string[];              // raw base-grain dim col names (visual grouping dims ∪ partition ∪ order)
  partitionRaw: string[];          // raw partition col names (original case)
  orderSpec: { col: string; dir: 'ASC' | 'DESC' }[]; // raw order col names + dir
  innerAggs: Record<string, { alias: string }>;       // dedup base aggregates keyed by func::sqlexpr
  windowAliases: Set<string>;
  overParts: string[];             // "<over sql> AS <ALIAS>" strings
  baseFromSql: string;             // the FROM source: a fq table or "(<custom sql>)"
}

function _qsWindowAlias(name: string, used: Set<string>): string {
  let b = (name || 'WIN_VAL').toUpperCase().replace(/[^A-Z0-9_]+/g, '_').replace(/^_+|_+$/g, '').replace(/_+/g, '_');
  if (!b) b = 'WIN_VAL';
  let a = b, n = 2;
  while (used.has(a)) a = `${b}_${n++}`;
  used.add(a);
  return a;
}

/**
 * Lower a parsed QS window calc to a column on a shared kind:'sql' helper element.
 * Returns true if it translated, false if it should degrade (caller emits Null).
 *
 *  - `primary` is the element the calc would otherwise have landed on; we read
 *    its underlying physical source (warehouse-table path or CustomSql statement)
 *    to build the helper's FROM.
 *  - `primaryColMap` maps lowercased col display-name → ColMapEntry (for raw names).
 *  - `helpers`/`usedAliases` accumulate across calls so calcs SHARE a helper.
 *  - `extraElements` collects newly-created helper elements (caller appends them).
 */
function lowerQSWindowCalc(
  win: QSWindowResult,
  calcName: string,
  primary: SigmaElement,
  primaryColMap: Map<string, ColMapEntry> | undefined,
  helpers: Map<string, QSWindowHelper>,
  usedAliases: Set<string>,
  extraElements: SigmaElement[],
  connectionId: string,
  visualGrainDims: string[],
  warnings: string[],
): boolean {
  // 1. Resolve the underlying FROM source for the helper SQL.
  const baseFromSql = qsResolveBaseFrom(primary);
  if (!baseFromSql) {
    warnings.push(`⚠ Window calc "${calcName}" (${win.op}) — could not resolve a warehouse FROM source for the primary element; degraded to Null.`);
    return false;
  }

  // 2. Validate partition / order field availability against the source.
  //    We require every partition + order field to be a known raw column so the
  //    generated SQL references real columns (else degrade).
  const knownRaw = qsKnownRawColumns(primary, primaryColMap);
  const resolveRaw = (name: string): string | null => {
    // direct case-insensitive match on raw col names
    const hit = knownRaw.find(r => r.toLowerCase() === name.toLowerCase());
    return hit || (knownRaw.length === 0 ? name : null); // if we have no col metadata, trust the name
  };
  const partitionRaw: string[] = [];
  for (const p of win.partitionFields) {
    const r = resolveRaw(p);
    if (!r) { warnings.push(`⚠ Window calc "${calcName}" (${win.op}) — partition field "${p}" not found in source columns; degraded to Null.`); return false; }
    partitionRaw.push(r);
  }
  const orderSpec: { col: string; dir: 'ASC' | 'DESC' }[] = [];
  for (const sf of win.sortFields) {
    const r = resolveRaw(sf.col);
    if (!r) { warnings.push(`⚠ Window calc "${calcName}" (${win.op}) — order field "${sf.col}" not found in source columns; degraded to Null.`); return false; }
    orderSpec.push({ col: r, dir: sf.dir });
  }

  // Ops that need an order dim but have none → degrade.
  const needsOrder = ['RUNNING_SUM','RUNNING_AVG','RUNNING_COUNT','RUNNING_MAX','RUNNING_MIN',
    'LAG','LEAD','FIRST_VALUE','LAST_VALUE','DIFFERENCE','PERCENT_DIFFERENCE'].includes(win.op);
  if (needsOrder && orderSpec.length === 0) {
    warnings.push(`⚠ Window calc "${calcName}" (${win.op}) — no order field could be determined; degraded to Null.`);
    return false;
  }
  // PERCENT_OF_TOTAL / WINDOW_* / OVER_* / RANK with empty partition → global; allowed.

  // 2b. Base aggregation grain = visual grouping dims (the grain the calc is
  //     evaluated at) ∪ partition fields ∪ order fields. Aggregating only to the
  //     partition/order axes collapses rows and breaks rank/percentOfTotal
  //     (always 1 / 100%). Resolve the visual grain dims to raw cols too.
  const grainRaw: string[] = [];
  const grainSeen = new Set<string>();
  const pushGrain = (raw: string) => {
    const a = _qsColToSql(raw);
    if (grainSeen.has(a)) return; grainSeen.add(a);
    grainRaw.push(raw);
  };
  for (const g of visualGrainDims) {
    const r = resolveRaw(g);
    if (r) pushGrain(r); // silently skip visual dims not on this source (e.g. dims from a joined element)
  }
  for (const p of partitionRaw) pushGrain(p);
  for (const o of orderSpec) pushGrain(o.col);

  // 3. Get-or-create the shared helper element keyed by (source, grain, partition, order).
  const grainKey = grainRaw.map(_qsColToSql).slice().sort().join(',');
  const partKey = partitionRaw.map(_qsColToSql).slice().sort().join(',');
  const orderKey = orderSpec.map(o => `${_qsColToSql(o.col)} ${o.dir}`).join(',');
  const key = `${baseFromSql}||${grainKey}||${partKey}||${orderKey}`;
  let helper = helpers.get(key);
  if (!helper) {
    const cols: SigmaColumn[] = [];
    const order: string[] = [];
    // Project every grain dim (passthrough) so the workbook can group/join on
    // them. Use [Custom SQL/<SQL_ALIAS>] refs (SQL element rule).
    for (const g of grainRaw) {
      const a = _qsColToSql(g);
      const id = sigmaShortId();
      cols.push({ id, name: sigmaDisplayName(g), formula: `[Custom SQL/${a}]` });
      order.push(id);
    }
    const el: SigmaElement = {
      id: sigmaShortId(),
      kind: 'table',
      // SQL elements normally omit element-level name (DM rule #3), but every
      // element needs a unique name for dedupeElementNames — give a descriptive
      // one (dedupe keeps it unique).
      name: `Window ${partitionRaw.join(', ') || 'All'}${orderSpec.length ? ' by ' + orderSpec.map(o => o.col).join(', ') : ''}`,
      source: { connectionId, kind: 'sql', statement: '__QS_WINDOW_PLACEHOLDER__' },
      columns: cols,
      order,
    };
    helper = { element: el, grainRaw, partitionRaw, orderSpec, innerAggs: {}, windowAliases: new Set(), overParts: [], baseFromSql };
    helpers.set(key, helper);
    extraElements.push(el);
  }

  // 4. Register the inner aggregate (e.g. SUM(NET_REVENUE) AS NET_REVENUE).
  let innerAlias = '';
  if (win.innerAggFunc && win.innerExprSql) {
    innerAlias = qsRegisterInnerAgg(helper, win.innerAggFunc, win.innerExprSql);
  }
  // RANK/DENSE_RANK order by a measure expr (e.g. SUM(NET_REVENUE)). Register it
  // as a base aggregate so the OVER orders by the pre-aggregated alias, not a
  // double-aggregate of the base column.
  if ((win.op === 'RANK' || win.op === 'DENSE_RANK') && win.rankSortExprSql) {
    const am = win.rankSortExprSql.match(/^([A-Z_]+)\s*\(\s*(?:DISTINCT\s+)?([A-Z0-9_]+)\s*\)$/i);
    if (am) {
      const fn = /distinct/i.test(win.rankSortExprSql) ? 'COUNT_DISTINCT' : am[1].toUpperCase();
      qsRegisterInnerAgg(helper, fn, am[2].toUpperCase());
    }
  }

  // 5. Build the OVER clause.
  const winAlias = _qsWindowAlias(calcName, usedAliases);
  const overSql = qsBuildOverClause(win, helper, innerAlias);
  if (!overSql) {
    warnings.push(`⚠ Window calc "${calcName}" (${win.op}) — could not build an OVER clause; degraded to Null.`);
    return false;
  }
  helper.overParts.push(`${overSql} AS ${winAlias}`);
  helper.windowAliases.add(winAlias);
  const calcId = sigmaShortId();
  helper.element.columns.push({ id: calcId, name: stripParens(sigmaDisplayName(calcName)), formula: `[Custom SQL/${winAlias}]` });
  helper.element.order.push(calcId);
  warnings.push(`✅ Window "${calcName}" (${win.op}) → SQL helper "${helper.element.name}" alias ${winAlias}`);
  return true;
}

// Resolve the FROM source for a window helper from the primary element.
//   warehouse-table → "DB.SCHEMA.TABLE"
//   sql (CustomSql)  → "(<statement>)" subquery
function qsResolveBaseFrom(primary: SigmaElement): string | null {
  const src = primary.source || {};
  if (src.kind === 'warehouse-table' && Array.isArray(src.path) && src.path.length) {
    return src.path.join('.');
  }
  if (src.kind === 'sql' && typeof src.statement === 'string' && src.statement.trim()
      && !src.statement.includes('_placeholder') && !/^--/.test(src.statement.trim())) {
    // Wrap the custom SQL as a derived table.
    return `(${src.statement.trim().replace(/;\s*$/, '')})`;
  }
  return null;
}

// Known raw column names available on the primary's source (for validation).
function qsKnownRawColumns(primary: SigmaElement, colMap?: Map<string, ColMapEntry>): string[] {
  const out: string[] = [];
  if (colMap) for (const e of colMap.values()) out.push(e.raw);
  return out;
}

function qsRegisterInnerAgg(helper: QSWindowHelper, aggFunc: string, exprSql: string): string {
  const key = `${aggFunc}::${exprSql}`;
  if (helper.innerAggs[key]) return helper.innerAggs[key].alias;
  const idMatch = exprSql.match(/[A-Z][A-Z0-9_]*/);
  let alias = idMatch ? idMatch[0] : 'VAL';
  let n = 2;
  while (helper.windowAliases.has(alias) || Object.values(helper.innerAggs).some(v => v.alias === alias)) {
    alias = idMatch ? `${idMatch[0]}_${n++}` : `VAL_${n++}`;
  }
  helper.innerAggs[key] = { alias };
  return alias;
}

function qsBuildOverClause(win: QSWindowResult, helper: QSWindowHelper, innerAlias: string): string | null {
  const partBy = helper.partitionRaw.length
    ? `PARTITION BY ${helper.partitionRaw.map(_qsColToSql).join(', ')}`
    : '';
  const orderBy = helper.orderSpec.length
    ? `ORDER BY ${helper.orderSpec.map(o => `${_qsColToSql(o.col)} ${o.dir}`).join(', ')}`
    : '';
  const spec = (parts: string[]) => parts.filter(Boolean).join(' ');

  switch (win.op) {
    case 'RUNNING_SUM': case 'RUNNING_AVG': case 'RUNNING_COUNT':
    case 'RUNNING_MAX': case 'RUNNING_MIN': {
      if (!innerAlias || !orderBy) return null;
      const fn = win.op.replace('RUNNING_', '');
      return `${fn}(${innerAlias}) OVER (${spec([partBy, orderBy])} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`;
    }
    case 'PERCENT_OF_TOTAL': {
      if (!innerAlias) return null;
      // x / SUM(x) OVER (partition) * 100
      return `(${innerAlias} / NULLIF(SUM(${innerAlias}) OVER (${partBy}), 0)) * 100`;
    }
    case 'OVER_SUM': case 'OVER_AVG': case 'OVER_COUNT':
    case 'OVER_DISTINCT_COUNT': case 'OVER_MAX': case 'OVER_MIN': {
      if (!innerAlias) return null;
      const fn = win.op === 'OVER_DISTINCT_COUNT' ? 'COUNT'
        : win.op.replace('OVER_', '');
      return `${fn}(${innerAlias}) OVER (${partBy})`;
    }
    case 'WINDOW_SUM': case 'WINDOW_AVG': case 'WINDOW_COUNT':
    case 'WINDOW_MAX': case 'WINDOW_MIN': {
      if (!innerAlias) return null;
      const fn = win.op.replace('WINDOW_', '');
      return `${fn}(${innerAlias}) OVER (${partBy})`;
    }
    case 'RANK': case 'DENSE_RANK': {
      const sortExpr = win.rankSortExprSql;
      if (!sortExpr) return null;
      // The rank measure is computed over the pre-aggregated base alias; if it is
      // an aggregate of an inner col registered as an alias, use that alias.
      let orderExpr = sortExpr;
      // map a "SUM(NET_REVENUE)"-style sort expr to its registered alias if present
      for (const k of Object.keys(helper.innerAggs)) {
        const [fnK, exprK] = k.split('::');
        const reconstructed = fnK === 'COUNT_DISTINCT' ? `COUNT(DISTINCT ${exprK})` : `${fnK}(${exprK})`;
        if (reconstructed === sortExpr) { orderExpr = helper.innerAggs[k].alias; break; }
      }
      const fn = win.op === 'DENSE_RANK' ? 'DENSE_RANK' : 'RANK';
      return `${fn}() OVER (${spec([partBy, `ORDER BY ${orderExpr} ${win.rankDir || 'DESC'}`])})`;
    }
    case 'LAG': case 'LEAD': {
      if (!innerAlias || !orderBy) return null;
      const fn = win.op;
      return `${fn}(${innerAlias}, ${win.offset ?? 1}) OVER (${spec([partBy, orderBy])})`;
    }
    case 'FIRST_VALUE': case 'LAST_VALUE': {
      if (!innerAlias || !orderBy) return null;
      const frame = win.op === 'LAST_VALUE'
        ? 'ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING' : '';
      return `${win.op}(${innerAlias}) OVER (${spec([partBy, orderBy, frame])})`;
    }
    case 'DIFFERENCE': {
      if (!innerAlias || !orderBy) return null;
      return `(${innerAlias} - LAG(${innerAlias}, ${win.offset ?? 1}) OVER (${spec([partBy, orderBy])}))`;
    }
    case 'PERCENT_DIFFERENCE': {
      if (!innerAlias || !orderBy) return null;
      const prev = `LAG(${innerAlias}, ${win.offset ?? 1}) OVER (${spec([partBy, orderBy])})`;
      return `((${innerAlias} - ${prev}) / NULLIF(${prev}, 0)) * 100`;
    }
  }
  return null;
}

// Finalize a helper: build the WITH base AS (...) SELECT ... OVER ... statement.
function finalizeQSWindowHelper(helper: QSWindowHelper): void {
  const selectParts: string[] = [];
  // Base grain dims (visual grouping ∪ partition ∪ order) — pass through, grouped.
  const groupCols: string[] = [];
  const seen = new Set<string>();
  for (const g of helper.grainRaw) {
    const a = _qsColToSql(g);
    if (seen.has(a)) continue; seen.add(a);
    selectParts.push(a); groupCols.push(a);
  }
  // inner aggregates
  for (const k of Object.keys(helper.innerAggs)) {
    const [aggFunc, exprSql] = k.split('::');
    const a = helper.innerAggs[k];
    const sqlFn = aggFunc === 'COUNT_DISTINCT' ? `COUNT(DISTINCT ${exprSql})` : `${aggFunc}(${exprSql})`;
    selectParts.push(`${sqlFn} AS ${a.alias}`);
  }
  const groupByClause = groupCols.length ? ` GROUP BY ${groupCols.map((_, i) => i + 1).join(', ')}` : '';
  const baseSelect = `SELECT ${selectParts.join(', ')} FROM ${helper.baseFromSql}${groupByClause}`;

  const innerProjection: string[] = [
    ...groupCols,
    ...Object.values(helper.innerAggs).map(v => v.alias),
  ];
  const outerProjection = innerProjection.concat(helper.overParts);
  helper.element.source.statement = `WITH base AS (${baseSelect}) SELECT ${outerProjection.join(', ')} FROM base`;
}

// ── Stub for analyses with no DescribeDataSet supplied ──────────────────────

function synthesizeStubDataset(
  identifier: string,
  ctx: DatasetBuildContext,
  _warnings: string[],
): DatasetEntry {
  const element: SigmaElement = {
    id: sigmaShortId(),
    kind: 'table',
    name: stripParens(sigmaDisplayName(identifier)) || 'Stub',
    source: { connectionId: ctx.connectionId, kind: 'sql', statement: `-- TODO: replace with the warehouse SELECT for QuickSight dataset "${identifier}"\nSELECT 1 AS _placeholder` },
    columns: [],
    metrics: [],
    order: [],
  };
  const byLogicalId = new Map([['__stub__', element]]);
  const logicalToPhysical = new Map([['__stub__', '__stub__']]);
  return { elements: [element], byLogicalId, logicalToPhysical, primary: element, primaryColMap: new Map() };
}

/**
 * Land an analysis-level CalculatedField. Strategy:
 *  - If the primary element has a derived view (relationships exist), place
 *    the calc col on the derived view and rewrite any cross-element refs to
 *    [SRC/REL_NAME/Field] form using a relatedNameMap built from joined
 *    dim columns.
 *  - Otherwise, place on the primary element (refs resolve locally).
 *
 * QuickSight calc fields naturally use bare {col} identifiers — translation
 * yields [Col Name] bare refs. The relatedNameMap rewrites any non-local
 * names to the cross-element triple form.
 */
function addAnalysisCalcCol(
  entry: DatasetEntry,
  name: string,
  expression: string,
  derivedViewBySrcId: Map<string, SigmaElement>,
  allElements: SigmaElement[],
  winCtx: QSWindowContext,
  visualGrainDims: string[],
  warnings: string[],
): void {
  const id = sigmaInodeId(name.toUpperCase());
  const display = stripParens(sigmaDisplayName(name));

  // Window/table-calc functions silently error in Sigma DM calc columns. Try to
  // lower to a kind:'sql' helper element (SQL OVER clause) before degrading.
  const win = quicksightParseWindow(expression || '');
  if (win) {
    const ok = lowerQSWindowCalc(win, name, entry.primary, entry.primaryColMap, winCtx.helpers, winCtx.usedAliases, winCtx.extraElements, winCtx.connectionId, visualGrainDims, warnings);
    if (ok) return; // lowered to helper element
  }

  const ex = quicksightFormulaToSigmaEx(expression || '', warnings);
  let formula = ex.formula;
  const description = ex.description;

  const derivedView = derivedViewBySrcId.get(entry.primary.id);
  if (derivedView) {
    // Build a name → triple-form rewrite map from related dim columns.
    const srcEl = entry.primary;
    const srcPath: string[] = srcEl.source.path || [];
    const srcBaseName: string = srcEl.name || srcPath[srcPath.length - 1] || '';
    const relatedNameMap = buildRelatedNameMap(srcEl, srcBaseName, allElements);
    // Local names on the derived view = own warehouse passthroughs (look at
    // derived view's own columns).
    const localNamesOnView = new Set<string>();
    for (const c of derivedView.columns || []) {
      const m = c.formula?.match(/^\[([^\]\/]+)\/([^\]]+)\]$/);
      if (m) localNamesOnView.add(m[2].toLowerCase());
    }
    // Rewrite bare refs: if the name matches a joined dim, use triple-form;
    // if it matches a local view col, leave bare; if it matches a calc col
    // on the source, surface it as a proxy on the derived view first.
    formula = formula.replace(/\[([^\]\/]+)\]/g, (match, refName) => {
      const lower = refName.toLowerCase();
      if (localNamesOnView.has(lower)) return match;
      const triple = relatedNameMap[refName];
      if (triple) return `[${triple}]`;
      // Check if it's a calc col on the source — if so, surface as proxy.
      const srcCalc = (srcEl.columns || []).find(c => c.name && c.name.toLowerCase() === lower);
      if (srcCalc) {
        // Add a proxy column on the derived view referencing the source calc.
        const proxyId = sigmaShortId();
        derivedView.columns.push({ id: proxyId, formula: `[${srcBaseName}/${srcCalc.name}]` });
        derivedView.order.push(proxyId);
        localNamesOnView.add(lower);
        return match;
      }
      return match;
    });
    const dvCol: SigmaColumn = { id, formula, name: display };
    if (description) dvCol.description = description;
    derivedView.columns.push(dvCol);
    derivedView.order.push(id);
  } else {
    // No derived view (single-element dataset). If the primary is a Custom SQL
    // element, bare base-column refs must be rewritten to [Custom SQL/RAW]
    // (fixes the live "Profit Margin=[Net Profit]/[Net Revenue]" bug —
    // beads-sigma-vy4k). colMap carries the raw aliases.
    if (entry.primary.source?.kind === 'sql' && entry.primaryColMap) {
      formula = rewriteSqlRefs(formula, entry.primaryColMap);
    }
    const pCol: SigmaColumn = { id, formula, name: display };
    if (description) pCol.description = description;
    entry.primary.columns.push(pCol);
    entry.primary.order.push(id);
  }
}

/** Map calc-col display names from joined dim elements → [SRC/REL/Field]. */
function buildRelatedNameMap(
  srcEl: SigmaElement,
  srcBaseName: string,
  allElements: SigmaElement[],
): Record<string, string> {
  const map: Record<string, string> = {};
  for (const rel of srcEl.relationships || []) {
    if (!rel.name) continue;
    const tgtEl = allElements.find(e => e.id === rel.targetElementId);
    if (!tgtEl || tgtEl.source?.kind !== 'warehouse-table') continue;
    for (const c of tgtEl.columns || []) {
      if (!c.formula || c.formula === 'Null') continue;
      const fm = c.formula.match(/^\[([^\]]+)\]$/);
      if (!fm) continue;
      const inner = fm[1];
      const s = inner.lastIndexOf('/');
      const dispName = s >= 0 ? inner.slice(s + 1) : inner;
      if (c.name && !(c.name in map)) map[c.name] = `${srcBaseName}/${rel.name}/${dispName}`;
      if (!(dispName in map)) map[dispName] = `${srcBaseName}/${rel.name}/${dispName}`;
    }
  }
  return map;
}

// ── ParameterDeclarations → Sigma controls ──────────────────────────────────

function parameterDeclarationToControl(decl: any, warnings: string[]): any | null {
  // Each ParameterDeclaration is a one-of with a single key
  const inner = decl.StringParameterDeclaration
    || decl.IntegerParameterDeclaration
    || decl.DecimalParameterDeclaration
    || decl.DateTimeParameterDeclaration;
  if (!inner) return null;
  const kind = decl.StringParameterDeclaration ? 'text'
    : decl.IntegerParameterDeclaration ? 'number'
      : decl.DecimalParameterDeclaration ? 'number'
        : 'date';
  const id = sigmaShortId();
  const isMulti = inner.ParameterValueType === 'MULTI_VALUED';
  const staticDefaults: any[] = inner.DefaultValues?.StaticValues || [];
  const control: any = {
    id,
    name: sigmaDisplayName(inner.Name || 'Param'),
    kind,
    multiSelect: isMulti,
  };
  if (staticDefaults.length) control.defaultValue = isMulti ? staticDefaults : staticDefaults[0];
  if (kind === 'number' && isMulti) {
    warnings.push(`ℹ Parameter "${inner.Name}" is multi-valued numeric — Sigma multi-numeric controls have known limitations; verify in UI (see beads-sigma-z3y).`);
  }
  return control;
}

// ── Derived "view" element builder (own warehouse cols + joined dim cols) ──

function buildDerivedView(srcEl: SigmaElement, allElements: SigmaElement[]): SigmaElement | null {
  const srcPath: string[] = srcEl.source.path || [];
  const srcTableName: string = srcPath[srcPath.length - 1] || '';
  const baseName: string = srcEl.name || srcTableName;
  const viewCols: { id: string; formula: string }[] = [];
  const viewOrder: string[] = [];

  for (const col of srcEl.columns || []) {
    if (!col.formula || col.formula === 'Null') continue;
    if (col.name) continue; // skip calc cols (would need rewrite, handled by base element)
    const m = col.formula.match(/^\[([^\/\]]+)\/([^\]]+)\]$/);
    if (!m) continue;
    const dispName = m[2];
    const cId = sigmaShortId();
    viewCols.push({ id: cId, formula: `[${baseName}/${dispName}]` });
    viewOrder.push(cId);
  }

  for (const rel of srcEl.relationships || []) {
    if (!rel.name) continue;
    const tgtEl = allElements.find(e => e.id === rel.targetElementId);
    if (!tgtEl || tgtEl.source?.kind !== 'warehouse-table') continue;
    for (const col of tgtEl.columns || []) {
      if (!col.formula || col.formula === 'Null') continue;
      const fm = col.formula.match(/^\[([^\]]+)\]$/);
      if (!fm) continue;
      const inner = fm[1];
      const s = inner.lastIndexOf('/');
      const dispName = s >= 0 ? inner.slice(s + 1) : inner;
      const cId = sigmaShortId();
      viewCols.push({ id: cId, formula: `[${baseName}/${rel.name}/${dispName}]` });
      viewOrder.push(cId);
    }
  }

  if (viewCols.length === 0) return null;
  return {
    id: sigmaShortId(),
    kind: 'table',
    name: `${sigmaDisplayName(srcTableName) || 'Source'} View`,
    source: { kind: 'table', elementId: srcEl.id },
    columns: viewCols,
    order: viewOrder,
  };
}

// ── Formula dialect mapper ──────────────────────────────────────────────────

/**
 * Translate a QuickSight calculated-field expression to a Sigma formula.
 *
 * Coverage:
 *  - identifier syntax: {col name} → [Col Name]
 *  - string literals: 'x' → "x"
 *  - ifelse(cond, val, cond, val, ..., else) → nested If(...)
 *  - switch / coalesce / nullIf / in
 *  - Aggregate, math, string, date functions
 *
 * Window/table-calc functions (sumOver, runningSum, lag, lead, rank, denseRank,
 * percentOfTotal, period*, lastValue, etc.) cannot be lowered to Sigma DM
 * formulas safely — Sigma's window functions silently error in DM calc cols.
 * Translate them to a comment placeholder and emit a warning suggesting a
 * Custom SQL element.
 */
export interface QSFormulaResult { formula: string; description?: string; }

/**
 * Extended translation: returns a Sigma formula plus optional description.
 * Window/table-calc functions silently error in Sigma DM calc columns, so they
 * degrade to a valid `Null` formula with the original QuickSight expression
 * preserved in the column description (beads-sigma-woaa). Everything else
 * returns `{ formula: <translated> }`.
 */
export function quicksightFormulaToSigmaEx(expr: string, warnings: string[]): QSFormulaResult {
  if (!expr || typeof expr !== 'string') return { formula: '' };
  let s = expr.trim();

  // 1. Block-out string literals so we don't munge their contents.
  const strings: string[] = [];
  s = s.replace(/'((?:[^'\\]|\\.)*)'/g, (_, body) => {
    const idx = strings.length;
    strings.push(`"${body.replace(/"/g, '\\"')}"`);
    return `__STR${idx}__`;
  });

  // 2. Detect window/table-calc functions (case-insensitive identifier match).
  const windowFns = [
    'sumOver','avgOver','countOver','distinctCountOver','maxOver','minOver',
    'stdevOver','stdevpOver','varOver','varpOver','percentileOver',
    'percentileContOver','percentileDiscOver','percentOfTotal',
    'runningSum','runningAvg','runningCount','runningMax','runningMin',
    'rank','denseRank','percentileRank',
    'lag','lead','firstValue','lastValue',
    'difference','percentDifference','periodOverPeriodDifference',
    'periodOverPeriodLastValue','periodOverPeriodPercentDifference',
    'periodToDateSumOverTime','periodToDateAvgOverTime',
    'periodToDateMaxOverTime','periodToDateMinOverTime','periodToDateCountOverTime',
    'windowSum','windowAvg','windowMax','windowMin','windowCount',
  ];
  const windowRe = new RegExp(`\\b(${windowFns.join('|')})\\s*\\(`, 'i');
  if (windowRe.test(s)) {
    warnings.push(`⚠ Formula uses a QuickSight table-calculation function (${s.match(windowRe)![1]}) — Sigma DM calc columns silently error on window functions. Degraded to a Null calc column with the original expression in its description; re-author as a Custom SQL element or a workbook-layer calculation.`);
    return { formula: 'Null', description: `QuickSight table-calc (re-author in Sigma): ${expr}` };
  }

  // 2b. QuickSight parameter refs ${Param} can NOT be lowered into a DM calc
  //     column: Sigma controls are workbook-scoped, and a data-model formula
  //     that references a control silently errors at query time (same class as
  //     window funcs above). The parameter is still emitted as a Sigma control
  //     from its ParameterDeclaration; degrade the dependent calc to a valid
  //     Null + description so it can be re-authored at the workbook layer,
  //     instead of leaving a dangling `$[...]` that breaks the whole POST
  //     (beads-sigma-n730). Must run BEFORE the {col} substitution below, which
  //     would otherwise eat the inner braces and strand the `$`.
  const paramRe = /\$\{([^{}]+)\}/;
  if (paramRe.test(s)) {
    const pname = String(s.match(paramRe)![1]).replace(/\[[^\]]+\]\s*$/, '').trim();
    warnings.push(`⚠ Formula references QuickSight parameter \${${pname}} — Sigma controls are workbook-scoped and can't be referenced from a data-model calc column. Degraded to a Null calc column with the original expression in its description; the parameter is emitted as a Sigma control, so re-author this calculation at the workbook layer using the "${sigmaDisplayName(pname)}" control.`);
    return { formula: 'Null', description: `QuickSight parameter-dependent calc (re-author at the Sigma workbook layer using the "${sigmaDisplayName(pname)}" control): ${expr}` };
  }

  // 3. Identifier substitution {col name} → [Col Name].
  //    QuickSight braces can NOT be nested. Qualifiers like {col[dsId]} are
  //    permitted but only appear inside join ON clauses (not calc-field
  //    expressions per AWS docs), so we strip the [...] suffix defensively.
  s = s.replace(/\{([^{}]+)\}/g, (_, raw) => {
    const cleaned = String(raw).replace(/\[[^\]]+\]\s*$/, '').trim();
    return `[${sigmaDisplayName(cleaned)}]`;
  });

  // 4. ifelse(cond, val, [cond, val, ...], default) → nested If
  s = transformIfElse(s);

  // 5. switch(expr, case1, val1, case2, val2, ..., default) → nested If with equality checks
  s = transformSwitch(s);

  // 6. Function-name remapping (case-insensitive).
  s = remapFunctions(s);

  // 7. SQL-style operators: nothing to do (Sigma uses =, !=, <, >, AND, OR which match QuickSight).
  // QuickSight uses `<>` for not-equals — Sigma uses `!=`.
  s = s.replace(/<>/g, '!=');

  // 8. Restore string literals.
  s = s.replace(/__STR(\d+)__/g, (_, i) => strings[Number(i)]);

  return { formula: s };
}

/** Back-compat string-only wrapper (used where a description has no home — e.g.
 *  join FK/filter expressions). Window funcs collapse to the literal `Null`. */
export function quicksightFormulaToSigma(expr: string, warnings: string[]): string {
  return quicksightFormulaToSigmaEx(expr, warnings).formula;
}

function transformIfElse(s: string): string {
  // Greedy from the inside out
  let prev = '';
  let safety = 0;
  while (prev !== s && safety++ < 50) {
    prev = s;
    s = s.replace(/\bifelse\s*\(([^()]*(?:\([^()]*\)[^()]*)*)\)/i, (_, args) => {
      const parts = splitTopLevel(args, ',');
      if (parts.length < 3) return `If(${args})`;
      // Pairs: cond, val ... ending with optional default
      const isOdd = parts.length % 2 === 1;
      let result = isOdd ? parts[parts.length - 1] : 'null';
      const limit = isOdd ? parts.length - 1 : parts.length;
      for (let i = limit - 2; i >= 0; i -= 2) {
        result = `If(${parts[i]}, ${parts[i + 1]}, ${result})`;
      }
      return result;
    });
  }
  return s;
}

function transformSwitch(s: string): string {
  let prev = '';
  let safety = 0;
  while (prev !== s && safety++ < 50) {
    prev = s;
    s = s.replace(/\bswitch\s*\(([^()]*(?:\([^()]*\)[^()]*)*)\)/i, (_, args) => {
      const parts = splitTopLevel(args, ',');
      if (parts.length < 3) return `Switch(${args})`;
      const subject = parts[0];
      const rest = parts.slice(1);
      const isOdd = rest.length % 2 === 1;
      let result = isOdd ? rest[rest.length - 1] : 'null';
      const limit = isOdd ? rest.length - 1 : rest.length;
      for (let i = limit - 2; i >= 0; i -= 2) {
        result = `If(${subject} = ${rest[i]}, ${rest[i + 1]}, ${result})`;
      }
      return result;
    });
  }
  return s;
}

function splitTopLevel(s: string, sep: string): string[] {
  const out: string[] = [];
  let depth = 0;
  let bracket = 0;
  let cur = '';
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (ch === '(') depth++;
    else if (ch === ')') depth--;
    else if (ch === '[') bracket++;
    else if (ch === ']') bracket--;
    if (ch === sep && depth === 0 && bracket === 0) {
      out.push(cur.trim());
      cur = '';
      continue;
    }
    cur += ch;
  }
  if (cur.trim()) out.push(cur.trim());
  return out;
}

// QuickSight function → Sigma function (Title-Case, applied case-insensitively).
const QS_FUNC_MAP: Record<string, string> = {
  // aggregate
  sum: 'Sum', avg: 'Avg', min: 'Min', max: 'Max', count: 'Count',
  distinct_count: 'CountDistinct', distinctcount: 'CountDistinct',
  median: 'Median',
  percentile: 'Percentile', percentilecont: 'Percentile', percentiledisc: 'Percentile',
  stdev: 'Stdev', stdevp: 'StdevP', var: 'Var', varp: 'VarP',
  // conditional
  isnull: 'IsNull', notnull: 'IsNotNull',
  coalesce: 'Coalesce', nullif: 'Nullif',
  // string
  concat: 'Concat', substring: 'Mid', strlen: 'Len',
  tolower: 'Lower', toupper: 'Upper', trim: 'Trim',
  replace: 'Replace', split: 'Split', locate: 'Find', contains: 'Contains',
  // math
  abs: 'Abs', ceil: 'Ceiling', floor: 'Floor', round: 'Round',
  log: 'Log', exp: 'Exp', sqrt: 'Sqrt', mod: 'Mod',
  // date
  now: 'Now', today: 'Today',
  truncdate: 'DateTrunc',
  adddatetime: 'DateAdd',
  datediff: 'DateDiff',
  extract: 'DatePart',
  epochdate: 'EpochDate',
  formatdate: 'Text', parsedate: 'Date',
  // boolean / set
  in: 'In',
};

function remapFunctions(s: string): string {
  return s.replace(/\b([A-Za-z_][A-Za-z0-9_]*)\s*(?=\()/g, (match, fn) => {
    const mapped = QS_FUNC_MAP[fn.toLowerCase()];
    return mapped ?? match;
  });
}

// ── Helpers ─────────────────────────────────────────────────────────────────

function emptyModel(name: string): any {
  return { name, schemaVersion: 1, pages: [{ id: sigmaShortId(), name: 'Page 1', elements: [] }] };
}

/** Strip parentheses (and their contents) from a name — parens collide with
 *  Sigma's function-call syntax inside [refs] and break column/element resolution. */
function stripParens(name: string): string {
  return (name || '').replace(/\s*\([^)]*\)/g, '').replace(/[()]/g, '').replace(/\s+/g, ' ').trim();
}

/** Ensure every element has a non-empty, unique `name`. Mutates in place. */
function dedupeElementNames(elements: SigmaElement[]): void {
  const seen = new Set<string>();
  for (const el of elements) {
    let base = stripParens(el.name || '') || 'Element';
    let candidate = base;
    let n = 2;
    while (seen.has(candidate.toLowerCase())) {
      candidate = `${base} ${n++}`;
    }
    seen.add(candidate.toLowerCase());
    el.name = candidate;
  }
}

// ── Type defs (subset of AWS shape used by the converter) ───────────────────

interface QSAnalysisDefinition {
  AnalysisId?: string;
  Name?: string;
  Definition: {
    DataSetIdentifierDeclarations?: Array<{ DataSetArn: string; Identifier: string }>;
    CalculatedFields?: Array<{ DataSetIdentifier: string; Name: string; Expression: string }>;
    ColumnConfigurations?: any[];
    FilterGroups?: any[];
    ParameterDeclarations?: any[];
    Options?: any;
    Sheets?: any[]; // not consumed
  };
}

interface QSDataSet {
  Arn?: string;
  DataSetId?: string;
  Name?: string;
  ImportMode?: 'SPICE' | 'DIRECT_QUERY';
  PhysicalTableMap?: Record<string, QSPhysicalTable>;
  LogicalTableMap?: Record<string, QSLogicalTable>;
  OutputColumns?: Array<{ Name: string; Type: string; SubType?: string }>;
  FieldFolders?: Record<string, { columns?: string[]; description?: string }>;
}

interface QSPhysicalTable {
  RelationalTable?: {
    DataSourceArn: string;
    Catalog?: string;
    Schema?: string;
    Name: string;
    InputColumns?: Array<{ Name: string; Type: string; SubType?: string; Id?: string }>;
  };
  CustomSql?: {
    DataSourceArn: string;
    Name: string;
    SqlQuery?: string;
    Columns?: Array<{ Name: string; Type: string; SubType?: string }>;
  };
  S3Source?: {
    DataSourceArn: string;
    UploadSettings: any;
    InputColumns?: Array<{ Name: string; Type: string }>;
  };
}

interface QSLogicalTable {
  Alias: string;
  Source: {
    PhysicalTableId?: string;
    DataSetArn?: string;
    JoinInstruction?: QSJoinInstruction;
  };
  DataTransforms?: QSDataTransform[];
}

interface QSJoinInstruction {
  LeftOperand: string;
  RightOperand: string;
  Type: 'INNER' | 'OUTER' | 'LEFT' | 'RIGHT';
  OnClause: string;
  LeftJoinKeyProperties?: { UniqueKey?: boolean };
  RightJoinKeyProperties?: { UniqueKey?: boolean };
}

interface QSDataTransform {
  CastColumnTypeOperation?: { ColumnName: string; NewColumnType: string; Format?: string; SubType?: string };
  CreateColumnsOperation?: { Columns: Array<{ ColumnId: string; ColumnName: string; Expression: string }> };
  FilterOperation?: { ConditionExpression: string };
  ProjectOperation?: { ProjectedColumns: string[] };
  RenameColumnOperation?: { ColumnName: string; NewColumnName: string };
  TagColumnOperation?: { ColumnName: string; Tags: any[] };
  UntagColumnOperation?: { ColumnName: string; TagNames?: string[] };
  OverrideDatasetParameterOperation?: any;
}
