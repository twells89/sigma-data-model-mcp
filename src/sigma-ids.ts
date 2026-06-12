/**
 * Shared Sigma Computing ID generation and naming utilities.
 * Extracted from the Sigma Data Model Manager tool.
 */

const SIGMA_CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
const _usedIds = new Set<string>();

/** Small words that Sigma keeps lowercase in display names (unless first word) */
const SIGMA_LOWERCASE_WORDS = new Set([
  'a','an','the','and','but','or','for','nor','so','yet',
  'at','by','in','of','on','to','up','as','into','via','per'
]);

/** Reset the ID registry — call at the start of each conversion run */
export function resetIds(): void {
  _usedIds.clear();
}

/** Generate a unique short random ID (base62) */
export function sigmaShortId(len = 10): string {
  let id: string;
  do {
    id = Array.from({ length: len }, () =>
      SIGMA_CHARS[Math.floor(Math.random() * SIGMA_CHARS.length)]
    ).join('');
  } while (_usedIds.has(id));
  _usedIds.add(id);
  return id;
}

/** Column IDs use Sigma's inode format: inode-{22-char base62}/{IDENTIFIER} */
export function sigmaInodeId(identifier: string): string {
  return `inode-${sigmaShortId(22)}/${identifier.toUpperCase()}`;
}

/**
 * SNAKE_CASE or camelCase → "Title Case" display name.
 *
 * Matches Sigma's OWN derivation rule for warehouse column names (verified
 * empirically against live DM readbacks, 2026-06-10): Sigma splits words at
 * underscores, camelCase boundaries, AND every letter↔digit boundary — in BOTH
 * directions. E.g. CY_Q1_REVENUE → "Cy Q 1 Revenue" (NOT "Cy Q1 Revenue"),
 * FY2024 → "Fy 2024", PY_Q4 → "Py Q 4". Raw-column formula refs
 * ([TABLE/Display Name]) must reproduce this exactly or the POST fails with
 * "dependency not found" (beads-sigma-c31q).
 */
export function sigmaDisplayName(s: string): string {
  // Insert underscores at camelCase + letter↔digit boundaries so OrderDate →
  // Order_Date and CY_Q1 → CY_Q_1 (Sigma splits BOTH letters→digits and
  // digits→letters).
  const normalized = (s || '')
    .replace(/([a-z])([A-Z])/g, '$1_$2')        // camelCase → camel_Case
    .replace(/([A-Z]+)([A-Z][a-z])/g, '$1_$2')  // HTMLParser → HTML_Parser
    .replace(/([A-Za-z])([0-9])/g, '$1_$2')     // Q1 → Q_1, FY2024 → FY_2024
    .replace(/([0-9])([A-Za-z])/g, '$1_$2');    // 2024FY → 2024_FY
  // Split on underscores AND whitespace so the function is IDEMPOTENT:
  // sigmaDisplayName("Cyq Rev") === "Cyq Rev". Formulas pass through the expression
  // translator more than once, and same-element sibling refs are resolved
  // case-SENSITIVELY by Sigma — a non-idempotent derivation ("Cyq Rev" → "Cyq rev")
  // breaks the ref and the column compiles to type "error".
  const words = normalized.toLowerCase().split(/[_\s]+/).filter(Boolean);
  return words.map((w, i) =>
    (i === 0 || !SIGMA_LOWERCASE_WORDS.has(w))
      ? w.charAt(0).toUpperCase() + w.slice(1)
      : w
  ).join(' ');
}

/** Column formula: [TABLE_NAME/Display Name] */
export function sigmaColFormula(tableName: string, identifier: string): string {
  return `[${tableName}/${sigmaDisplayName(identifier)}]`;
}

/** Metric formula: aggregation referencing column by display name (no table prefix) */
export function sigmaAggFormula(agg: string, identifier: string): string {
  const dn = sigmaDisplayName(identifier);
  const map: Record<string, string> = {
    sum:            `Sum([${dn}])`,
    avg:            `Avg([${dn}])`,
    average:        `Avg([${dn}])`,
    min:            `Min([${dn}])`,
    max:            `Max([${dn}])`,
    count:          `CountIf(IsNotNull([${dn}]))`,
    count_distinct: `CountDistinct([${dn}])`,
    count_distict:  `CountDistinct([${dn}])`,
    median:         `Median([${dn}])`,
    percentile:     `Percentile([${dn}], 0.5)`,
    sum_boolean:    `CountIf([${dn}])`,
  };
  return map[agg?.toLowerCase()] || `Sum([${dn}])`;
}

/**
 * Infer a Sigma format object from a formula string and display name.
 * Returns null when no rule matches (omit format from output).
 *
 * Priority:
 *  1. Formula is already ×100 percent scale (e.g. `* 100`) → plain number + % suffix
 *  2. Ratio pattern (Agg() / Agg())                         → ,.2%
 *  3. Name keywords for %                                    → ,.2%
 *  4. Name keywords for currency                             → $,.2f
 *  5. Count/CountDistinct formula                            → ,.0f integer
 */
/**
 * Map a source-tool numeric format mask (Excel/.NET style, e.g. "$#,0.00",
 * "0.0%", "#,##0") to a Sigma format object. The PRIMARY format signal when the
 * source carries one — far more reliable than guessing from the formula/name
 * (beads-sigma-4q7k). Returns null for masks we don't recognize (e.g. dates,
 * "General Date") so the heuristic fallback still runs.
 */
export function formatFromMask(mask?: string): Record<string, any> | null {
  if (!mask || typeof mask !== 'string') return null;
  const s = mask.trim();
  if (!s || /general|date|time|@|yy|dd/i.test(s)) return null;
  // decimals = run of 0/# after a decimal point in the mask
  const decM = s.match(/\.([0#]+)/);
  const decimals = decM ? decM[1].length : 0;
  const isPercent = /%/.test(s);
  const isCurrency = /[$£€¥]/.test(s);
  if (isPercent) return { kind: 'number', formatString: `,.${decimals}%` };
  if (isCurrency) return { kind: 'number', formatString: `$,.${decimals}f`, currencySymbol: '$' };
  if (/[0#]/.test(s)) return { kind: 'number', formatString: `,.${decimals}f` };
  return null;
}

export function inferSigmaFormat(formula: string, displayName?: string, sourceMask?: string): Record<string, any> | null {
  // Honor the source format mask first when present — most reliable signal.
  const fromMask = formatFromMask(sourceMask);
  if (fromMask) return fromMask;
  if (!formula) return null;
  const f = formula.trim();
  const n = (displayName || '').toLowerCase();

  const alreadyPctScale = /\*\s*100\b/.test(f);
  if (alreadyPctScale && /\b(rate|margin|pct|percent|ratio|share|mix)\b|%/.test(n)) {
    return { kind: 'number', formatString: ',.2f', suffix: '%' };
  }
  const currencyWord = /\b(revenue|sales|profit|cost|spend|amount|discounts?|price|value|aov|arpu)\b/;
  // A ratio of two aggregates is a PERCENTAGE only when both operands are the same unit
  // (e.g. count/count, revenue/revenue) — NOT when it's a per-unit measure like
  // Sum(Revenue)/CountDistinct(Order) (a dollar amount) or Sum(Revenue)/Count(*) (AOV).
  const ratio = f.match(/^([A-Za-z]+)\s*\(([^)]*)\)\s*\/\s*([A-Za-z]+)\s*\(([^)]*)\)$/);
  if (ratio) {
    const [, numFn, numArg, denFn, denArg] = ratio;
    const isCount = (fn: string) => /^Count/i.test(fn);
    const numIsCurrency = currencyWord.test(numArg.toLowerCase());
    const nameSaysPct = /\b(rate|margin|pct|percent|ratio|share|mix)\b|%/.test(n);
    if (nameSaysPct || (isCount(numFn) && isCount(denFn))) {
      return { kind: 'number', formatString: ',.2%' };
    }
    // Currency numerator over a non-currency denominator (count / quantity) → $ per-unit.
    if (numIsCurrency) {
      return { kind: 'number', formatString: '$,.2f', currencySymbol: '$' };
    }
    // Otherwise a plain decimal ratio.
    return { kind: 'number', formatString: ',.2f' };
  }
  if (/\b(rate|margin|pct|percent|ratio|share|mix)\b|%/.test(n)) {
    return { kind: 'number', formatString: ',.2%' };
  }
  if (currencyWord.test(n)) {
    return { kind: 'number', formatString: '$,.2f', currencySymbol: '$' };
  }
  if (/^Count(?:Distinct|If|DistinctIf)?\s*\(/.test(f)) {
    return { kind: 'number', formatString: ',.0f' };
  }
  return null;
}

/** Sigma Data Model JSON Schema reference (for prompts/docs) */
export const DATA_MODEL_SCHEMA_SUMMARY = `
Sigma Data Model JSON top-level structure:
{
  "name": "Model Name",
  "pages": [{ "id": "pageId", "name": "Page 1", "elements": [...] }]
}

Element types: warehouse-table, custom-sql (kind:"sql"), join, union, control.
Columns: { "id": "inode-xxx/COL", "formula": "[TABLE/Display Name]" }
Calculated columns: { "id": "shortId", "formula": "[Price] - [Cost]", "name": "Profit" }
Metrics: { "id": "shortId", "formula": "Sum([Revenue])", "name": "Total Revenue" }
Relationships: { "id": "shortId", "targetElementId": "...", "keys": [{ "sourceColumnId": "...", "targetColumnId": "..." }] }

Cross-element Reference (accessing related dimension columns via relationships):
  [SOURCE_TABLE/REL_NAME/Column Display Name]
  REL_NAME is the relationship's "name" field (= target table name uppercase by convention).
  Example: DateDiff("day", [ORDER_FACT/PROMO_DIM/Start Date], [ORDER_FACT/PROMO_DIM/End Date])
  ⚠ The dash-link form [SRC/FK_COL - link/Field] does NOT work via the API — use REL_NAME.

Conditional Aggregate Syntax:
  CountIf(condition) — condition only, NO field argument
  SumIf(field, condition) — FIELD FIRST, condition second
  AvgIf/MaxIf/MinIf/CountDistinctIf — all FIELD FIRST
  For booleans: always use [Column] = True, never bare [Column]

Groupings (for LOD / different aggregation levels):
  "groupings": [{ "id": "gId", "groupBy": ["colId1"], "calculations": ["calcId1"] }]
  Array order = nesting hierarchy. Use child elements for LOD patterns.
`.trim();

/** Common column/element interfaces */
export interface SigmaColumn {
  id: string;
  formula: string;
  name?: string;
  description?: string;
  hidden?: boolean;
  format?: Record<string, any>;
}

export interface SigmaMetric {
  id: string;
  formula: string;
  name: string;
  description?: string;
  format?: Record<string, any>;
}

export interface SigmaRelationshipKey {
  sourceColumnId: string;
  targetColumnId: string;
}

export interface SigmaRelationship {
  id: string;
  targetElementId: string;
  keys: SigmaRelationshipKey[];
  name: string;
  relationshipType?: string;
}

export interface SigmaElement {
  id: string;
  kind: string;
  source: Record<string, any>;
  columns: SigmaColumn[];
  metrics?: SigmaMetric[];
  relationships?: SigmaRelationship[];
  order: string[];
  [key: string]: any;
}

export interface SigmaPage {
  id: string;
  name: string;
  elements: SigmaElement[];
}

export interface SigmaDataModel {
  name: string;
  schemaVersion?: number;
  pages: SigmaPage[];
}

/**
 * Detected source-tool row/column security, REPORTED (not injected into `model`).
 * A stateless converter can't create/assign Sigma user attributes or POST, so it
 * never bakes RLS into the returned spec (that would fail-closed standalone).
 * Instead it hands the migration skill everything needed to provision + apply:
 * the target element, the ready-to-use Sigma boolean formula / CLS columns, and
 * the user attributes / teams to create. See apply_sigma_rls.py.
 */
export interface SecurityRule {
  kind: 'rls' | 'cls';
  source: string;                 // human description, e.g. "Looker access_filter", "PBI role 'X'"
  elementId?: string;             // element id in the returned model to apply to
  elementName?: string;
  rls?: {
    name: string;                 // suggested calc-column name, e.g. "RLS: Region"
    formula: string;              // Sigma boolean (CurrentUserAttributeText/InTeam/Email = [col])
    userAttributes?: string[];    // attribute names to provision/assign
    teams?: string[];             // team names referenced by CurrentUserInTeam
    usesCurrentUserEmail?: boolean;
  };
  /** Raw source-tool security expression when no Sigma formula could be derived
   *  (e.g. a Cognos data-module securityFilter expression) — translate manually. */
  sourceExpression?: string;
  /** Source-tool group/role refs the rule is scoped to (e.g. Cognos CAM groups). */
  groups?: string[];
  cls?: {
    restrictedColumnIds: string[];
    restrictedColumnNames?: string[];
    criteria: { kind: 'no-one-can-view' };
  };
  note: string;                   // provisioning guidance / caveats
}

/**
 * A source-tool expression whose faithful Sigma equivalent is a WORKBOOK
 * construct, not a data-model metric/calc column — mirroring the PBI
 * time-intel handoff (DateLookback/CumulativeSum grouped elements). Window
 * functions (Rank/RankDense/Lag/Lead) silently error in DM calc columns and
 * in workbook master calc columns; they only work in date/dim-GROUPED
 * workbook elements (feedback_sigma_window_functions.md; live-verified
 * 2026-06-11: grouped workbook element Rank/RankDense/Lag/Lead == warehouse
 * RANK/DENSE_RANK/LAG/LEAD exactly). The converter reports these — ready-to-
 * place formula included where translatable — and the migration skill's
 * workbook builder places them in a grouped element on the chart's dimension.
 */
export interface WorkbookPattern {
  kind: 'rank' | 'lag' | 'lead' | 'first-sorted-value' | 'unsupported';
  name: string;            // source measure/dimension title
  source: string;          // original source-tool expression
  formula?: string;        // ready-to-use Sigma formula (grouped-element context)
  requires?: string;       // placement requirement (grouping/sort context)
  elementId?: string;      // suggested source element in the returned model
  elementName?: string;
  verify?: boolean;        // semantics approximated — verify numbers vs source
  note: string;
}

export interface ConversionResult {
  model: SigmaDataModel;
  warnings: string[];
  stats: Record<string, number>;
  security?: SecurityRule[];      // detected RLS/CLS — reported, NOT injected into `model`
  workbookPatterns?: WorkbookPattern[];  // window/inter-record calcs — reported for the workbook builder, NOT injected
}

export interface ElementResult {
  element: SigmaElement;
  elementId: string;
  colIdMap: Record<string, string>;
}

/** Resolvable element name: explicit `name`, else the warehouse path tail
 *  (warehouse-table elements have no `name` — Sigma derives it from the path,
 *  e.g. ORDER_FACT — so the skill matches the live element by that). */
function _securityElementName(el: { name?: string; source?: { path?: string[] } }): string | undefined {
  if (el?.name) return el.name;
  const path = el?.source?.path;
  return path && path.length ? String(path[path.length - 1]) : undefined;
}

/** Build an RLS SecurityRule from a Sigma boolean formula, auto-extracting the
 *  user attributes / teams that must be provisioned, plus tailored guidance. */
export function makeRlsSecurity(opts: {
  source: string; element: { id?: string; name?: string; source?: { path?: string[] } }; name: string; formula: string;
}): SecurityRule {
  const attrs = [...opts.formula.matchAll(/CurrentUserAttributeText\(\s*"([^"]+)"/g)].map(m => m[1]);
  const teams = [...opts.formula.matchAll(/CurrentUserInTeam\(\s*"([^"]+)"/g)].map(m => m[1]);
  const usesEmail = /\bCurrentUserEmail\(/.test(opts.formula);
  const provision = [
    attrs.length ? `provision/assign Sigma user attribute(s): ${[...new Set(attrs)].join(', ')}` : '',
    teams.length ? `create Sigma team(s) + membership: ${[...new Set(teams)].join(', ')}` : '',
    usesEmail && !attrs.length && !teams.length ? 'uses CurrentUserEmail() — no provisioning needed' : '',
  ].filter(Boolean).join('; ');
  return {
    kind: 'rls', source: opts.source, elementId: opts.element.id, elementName: _securityElementName(opts.element),
    rls: {
      name: opts.name, formula: opts.formula,
      userAttributes: attrs.length ? [...new Set(attrs)] : undefined,
      teams: teams.length ? [...new Set(teams)] : undefined,
      usesCurrentUserEmail: usesEmail || undefined,
    },
    note: `Fail-closed RLS (boolean calc + element filter, only True rows). ${provision || 'review'}. The skill provisions then applies — the converter does NOT inject it.`,
  };
}

/** Build a CLS SecurityRule (column restriction, reported not injected). */
export function makeClsSecurity(opts: {
  source: string; element: { id?: string; name?: string; source?: { path?: string[] } }; columnIds: string[]; columnNames?: string[]; note?: string;
}): SecurityRule {
  return {
    kind: 'cls', source: opts.source, elementId: opts.element.id, elementName: _securityElementName(opts.element),
    cls: { restrictedColumnIds: opts.columnIds, restrictedColumnNames: opts.columnNames, criteria: { kind: 'no-one-can-view' } },
    note: opts.note || 'Column-level security: restrict via columnSecurities (no-one-can-view, or re-scope to a team/attribute allowlist). The skill applies it — the converter does NOT inject it.',
  };
}

/**
 * Build a derived "join view" element for each source element that has outgoing
 * relationships. The derived element exposes own columns plus dim columns via
 * [SRC/REL_NAME/Col] cross-element formulas. Used by Qlik, OAC, Alteryx, Atlan.
 */
export function buildDerivedElements(elements: SigmaElement[]): SigmaElement[] {
  const derived: SigmaElement[] = [];
  for (const srcEl of elements) {
    if (!srcEl.relationships?.length) continue;
    if (srcEl.source?.kind !== 'warehouse-table') continue;

    const srcPath: string[] = srcEl.source.path || [];
    const srcTableName: string = srcPath[srcPath.length - 1] || '';
    // The base prefix in formulas must match what Sigma resolves the base element as:
    //   - if the element has an explicit `name` field, that is its identifier
    //   - otherwise Sigma falls back to the warehouse-table path-tail uppercase
    const baseName: string = srcEl.name || srcTableName;
    // Derived element NAME must differ from the base so [<base>/Field] is unambiguous.
    const derivedName = `${srcEl.name || sigmaDisplayName(srcTableName)} View`;
    const viewCols: Array<{ id: string; formula: string }> = [];
    const viewOrder: string[] = [];

    for (const col of (srcEl.columns || [])) {
      if (!col.formula || col.formula.startsWith('/*')) continue;
      const fm = col.formula.match(/^\[([^\/\]]+)\/([^\]]+)\]$/);
      if (!fm) continue; // skip calc cols
      const dispName = fm[2];
      // A "/" in the display name breaks Sigma's slash-delimited bracket ref — skip.
      if (dispName.includes('/')) continue;
      const cId = sigmaShortId();
      viewCols.push({ id: cId, formula: `[${baseName}/${dispName}]` });
      viewOrder.push(cId);
    }

    for (const rel of srcEl.relationships) {
      if (!rel.name) continue;
      const tgtEl = elements.find(e => e.id === rel.targetElementId);
      if (!tgtEl || tgtEl.source?.kind !== 'warehouse-table') continue;
      // Don't denormalize the relationship's OWN key column across the join — the base
      // element already carries that value, and the cross-element passthrough of a join
      // key compiles to type "error" in Sigma (verified via readback). Skip it.
      const tgtKeyIds = new Set((rel.keys || []).map((k: any) => k.targetColumnId));
      for (const col of (tgtEl.columns || [])) {
        if (tgtKeyIds.has(col.id)) continue;
        if (!col.formula || col.formula.startsWith('/*')) continue;
        const fm = col.formula.match(/^\[([^\]]+)\]$/);
        if (!fm) continue;
        const inner = fm[1];
        // The display name is everything after the FIRST slash (the table/element
        // prefix). Use indexOf, not lastIndexOf — a display name may itself contain a
        // slash (e.g. Tableau caption "Product Key/Name").
        const s = inner.indexOf('/');
        const dispName = s >= 0 ? inner.slice(s + 1) : inner;
        // A display name containing a "/" cannot be referenced via Sigma's
        // slash-delimited bracket syntax ([Base/Rel/Field] would over-segment). Such a
        // column stays accessible on its own dimension element; just don't surface it as
        // a denormalized cross-element passthrough (it would compile to type "error").
        if (dispName.includes('/')) continue;
        const cId = sigmaShortId();
        viewCols.push({ id: cId, formula: `[${baseName}/${rel.name}/${dispName}]` });
        viewOrder.push(cId);
      }
    }

    if (viewCols.length > 0) {
      derived.push({
        id: sigmaShortId(),
        kind: 'table',
        name: derivedName,
        source: { kind: 'table', elementId: srcEl.id },
        columns: viewCols,
        order: viewOrder,
      });
    }
  }
  return derived;
}
