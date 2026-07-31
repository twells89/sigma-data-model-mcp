/**
 * Shared Sigma Computing ID generation and naming utilities.
 * Extracted from the Sigma Data Model Manager tool.
 */

const SIGMA_CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
const _usedIds = new Set<string>();
// Per-run counter so IDs are DETERMINISTIC: the same input yields byte-identical
// output every run (reproducible / diff-able / cacheable). IDs only need to be
// unique within a single model, which the monotonic counter guarantees. Reset —
// and, for callers that supply one, SEEDED — by resetIds() at the start of each
// conversion. See resetIds() for the per-invocation id-space scoping (W2.4).
let _idCounter = 0;

/** Deterministic base62 encoding of a positive integer, left-padded to `len`. */
function encodeBase62(n: number, len: number): string {
  let x = n, s = '';
  while (x > 0) { s = SIGMA_CHARS[x % 62] + s; x = Math.floor(x / 62); }
  return s.padStart(len, SIGMA_CHARS[0]);
}

/** Deterministic 32-bit FNV-1a hash of a string → unsigned integer. Pure JS (no
 *  node:crypto) so the bundled/vendored converter stays self-contained. */
function fnv1a32(s: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

// Per-invocation id-space scoping (W2.4). A module-global counter reset to 0 at
// every top-level convert made two INDEPENDENT conversions in one process mint
// byte-identical id sequences ("AAAAAAAAAB" twice); merged (multi-datasource /
// multi-workbook), the DM POST failed on duplicate ids. Seeding the counter from
// a stable hash of the invocation's input partitions the counter into disjoint
// BLOCKS: different input ⇒ different NAMESPACE ⇒ ranges [ns·BLOCK, ns·BLOCK+used)
// never overlap, so the two runs' ids are disjoint by construction. Same input ⇒
// same namespace ⇒ same ids (determinism / stable goldens preserved). NS_MODULUS
// buckets × BLOCK ids/run stays well under Number.MAX_SAFE_INTEGER (2^53) so
// encodeBase62's %/÷ never lose precision (max base ≈ 1.48e13 ≪ 9.007e15).
const NS_MODULUS = 62 ** 4;    // 14,776,336 namespace buckets
const NS_BLOCK = 1_000_000;    // ids per invocation (a model mints ~thousands)

/** Small words that Sigma keeps lowercase in display names (unless first word) */
const SIGMA_LOWERCASE_WORDS = new Set([
  'a','an','the','and','but','or','for','nor','so','yet',
  'at','by','in','of','on','to','up','as','into','via','per'
]);

/**
 * Reset the ID registry + counter — call at the start of each conversion run.
 *
 * With NO argument the counter restarts at 0 (historical behavior — every
 * converter that calls `resetIds()` is byte-identical). With a `seed` (the
 * top-level invocation's input — e.g. the .twb text + datasourceIndex) the
 * counter is SEEDED into a per-invocation block so two independent conversions
 * in one process get disjoint id-spaces (W2.4): different seed ⇒ different
 * namespace ⇒ no collision; same seed ⇒ same ids ⇒ deterministic/stable output.
 * Only top-level entries seed; multi-datasource CHILD conversions skip resetIds
 * entirely and continue the parent's counter (id continuity from #107).
 */
export function resetIds(seed?: string): void {
  _usedIds.clear();
  _idCounter = seed == null ? 0 : (fnv1a32(seed) % NS_MODULUS) * NS_BLOCK;
}

/**
 * Clamp an emitted id to Sigma's 64-char id limit. Ids ≤ max pass through
 * verbatim. Longer ids (only the inode column id `inode-{22}/{PHYS}` and the
 * name-derived control ids can overflow) keep a human-readable prefix and get a
 * DETERMINISTIC hash suffix of the full id, so the same input always yields the
 * same clamped id (goldens stay stable) and two distinct long ids can't collide
 * after truncation. The 22-char uniqueness segment of an inode id lives inside
 * the retained prefix, so uniqueness is preserved.
 */
export function clampId(id: string, max = 64): string {
  if (id.length <= max) return id;
  const suffix = '~' + encodeBase62(fnv1a32(id) % (62 ** 6), 6); // 7 chars, stable
  return id.slice(0, max - suffix.length) + suffix;
}

/** Generate a unique, DETERMINISTIC short ID (base62). Seeded by a per-run counter
 *  (reset in resetIds), so the same input + call order produces the same IDs every
 *  run. Unique within the run via the monotonic counter + registry guard. */
export function sigmaShortId(len = 10): string {
  let id: string;
  do {
    id = encodeBase62(++_idCounter, len);
  } while (_usedIds.has(id));
  _usedIds.add(id);
  return id;
}

/** Column IDs use Sigma's inode format: inode-{22-char base62}/{IDENTIFIER} */
// `casing` controls the physical-name folding: Snowflake/BigQuery fold unquoted
// identifiers to UPPER (the default), but Databricks/Spark store them lower-case,
// so those warehouses bind only against a lower-cased physical name. Default
// 'upper' keeps every existing caller byte-identical.
export function sigmaInodeId(identifier: string, casing: 'upper' | 'lower' = 'upper'): string {
  const phys = casing === 'lower' ? identifier.toLowerCase() : identifier.toUpperCase();
  // Clamp to Sigma's 64-char id limit: a very long physical column name (e.g. a
  // Custom-SQL-derived alias) would push `inode-{22}/{PHYS}` past 64. The 22-char
  // uniqueness segment is inside the retained prefix, so clamping the PHYS tail is
  // safe (binding is via the column's [TABLE/Display Name] formula, not this id).
  return clampId(`inode-${sigmaShortId(22)}/${phys}`);
}

/**
 * Warehouse-physical identifier for a model column name (beads-sigma-f5kp).
 *
 * The emitted column id's physical segment and the [TABLE/Display Name] formula
 * ref must agree: Sigma derives the display name FROM the physical column, so
 * the physical name we emit must be one whose Sigma display equals
 * sigmaDisplayName(name). Plain toUpperCase() breaks camelCase names —
 * "LocationID" → LOCATIONID displays as "Locationid", but the ref says
 * "Location Id" → "dependency not found" at POST.
 *
 * Already-canonical warehouse names (ALL_CAPS_WITH_DIGITS, e.g. CY_Q1_REVENUE)
 * pass through VERBATIM — they are real warehouse columns and Sigma's display
 * derivation already round-trips them. Everything else (camelCase, spaces,
 * mixed case) is normalized with the SAME boundary splits as sigmaDisplayName,
 * then upper-snaked: LocationID → LOCATION_ID ("Location Id" ✓), "City Name" →
 * CITY_NAME ("City Name" ✓), Sum_GrossMarginAmount → SUM_GROSS_MARGIN_AMOUNT
 * ("Sum Gross Margin Amount" ✓).
 */
export function sigmaPhysicalName(s: string, casing: 'upper' | 'lower' = 'upper'): string {
  const r = (s || '').trim();
  const verbatim = casing === 'lower' ? /^[a-z0-9_]+$/ : /^[A-Z0-9_]+$/;
  if (verbatim.test(r)) return r; // real warehouse-style name in the target case — keep verbatim
  const normalized = r
    .replace(/[^A-Za-z0-9_\s]/g, ' ')           // identifier-safe
    .replace(/([a-z])([A-Z])/g, '$1_$2')
    .replace(/([A-Z]+)([A-Z][a-z])/g, '$1_$2')
    .replace(/([A-Za-z])([0-9])/g, '$1_$2')
    .replace(/([0-9])([A-Za-z])/g, '$1_$2');
  const joined = normalized.split(/[_\s]+/).filter(Boolean).join('_');
  return casing === 'lower' ? joined.toLowerCase() : joined.toUpperCase();
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
  // Split on underscores, whitespace AND slashes so the function is IDEMPOTENT:
  // sigmaDisplayName("Cyq Rev") === "Cyq Rev". Formulas pass through the expression
  // translator more than once, and same-element sibling refs are resolved
  // case-SENSITIVELY by Sigma — a non-idempotent derivation ("Cyq Rev" → "Cyq rev")
  // breaks the ref and the column compiles to type "error".
  // `/` is Sigma's ref-path separator ([Element/Column]) — a field literally named
  // "Country/Region" emits [ORDERS/Country/region], which Sigma parses as a nested
  // path and cannot resolve (type "error"). Fold the slash to a word boundary so it
  // normalizes to the physical column ("Country Region" → COUNTRY_REGION).
  // `-` is likewise not a warehouse identifier char: Sigma does NOT fold "Sub-Category"
  // to SUB_CATEGORY, so a Tableau caption like "Sub-Category" fails to resolve against
  // the underscore-named warehouse column. Fold it to a space so it does.
  const words = normalized.toLowerCase().split(/[_\s/-]+/).filter(Boolean);
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
export function sigmaAggFormula(agg: string, identifier: string, warnings?: string[]): string {
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
  const mapped = map[agg?.toLowerCase()];
  if (mapped) return mapped;
  // No documented Sigma mapping for this aggregation. Keep Sum as a visible fallback
  // (so downstream metric refs don't break) but NEVER silently — flag it for review.
  warnings?.push(`⚠ aggregation "${agg}" has no Sigma mapping for "${dn}" — defaulted to Sum([${dn}]); verify the aggregation is correct.`);
  return `Sum([${dn}])`;
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
  kind: 'rank' | 'lag' | 'lead' | 'cumulative' | 'moving' | 'percent-of-total'
      | 'index' | 'window' | 'first-sorted-value' | 'aggregate-dimension' | 'unsupported';
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
    const viewCols: Array<{ id: string; formula: string; hidden?: boolean }> = [];
    const viewOrder: string[] = [];
    // Declutter (Phase 1): one folder per relationship target, collecting that
    // target's related/lookup columns so the UI doesn't show hundreds of flat
    // lookup columns next to the relationships that already provide the same
    // reach. Columns are HIDDEN + GROUPED, never dropped — see the `hidden: true`
    // note below for why removal is unsafe.
    const folderByTarget = new Map<string, { id: string; name: string; items: string[] }>();

    for (const col of (srcEl.columns || [])) {
      if (!col.formula || col.formula.startsWith('/*')) continue;
      // CALC columns carry an explicit `name` and an expression formula — they
      // are first-class columns of the base element and pass through by name
      // (skipping them used to break visuals bound to calc columns through the
      // derived view, e.g. the Retail Analysis Sample's Store Type).
      let dispName: string | undefined;
      const fm = col.formula.match(/^\[([^\/\]]+)\/([^\]]+)\]$/);
      if (fm) dispName = fm[2];
      else if ((col as any).name) dispName = String((col as any).name);
      if (!dispName) continue;
      // A "/" in the display name breaks Sigma's slash-delimited bracket ref — skip.
      if (dispName.includes('/')) continue;
      const cId = sigmaShortId();
      viewCols.push({ id: cId, formula: `[${baseName}/${dispName}]` });
      viewOrder.push(cId);
    }

    for (const rel of srcEl.relationships) {
      if (!rel.name) continue;
      const tgtEl = elements.find(e => e.id === rel.targetElementId);
      // Denormalize both warehouse-table AND Custom SQL (derived_table / PDT)
      // relationship targets — a LookML explore that joins a derived_table view
      // must surface that view's columns on the denorm element, or workbook tiles
      // referencing the join measures fail to resolve.
      if (!tgtEl || (tgtEl.source?.kind !== 'warehouse-table' && tgtEl.source?.kind !== 'sql')) continue;
      // Don't denormalize the relationship's OWN key column across the join — the base
      // element already carries that value, and the cross-element passthrough of a join
      // key compiles to type "error" in Sigma (verified via readback). Skip it.
      const tgtKeyIds = new Set((rel.keys || []).map((k: any) => k.targetColumnId));
      // Human folder name for this relationship's target — mirrors the baseName
      // resolution above (explicit `name` wins, else warehouse path tail).
      let tgtTableName = '';
      if (tgtEl.source?.kind === 'warehouse-table') {
        const tgtPath: string[] = tgtEl.source.path || [];
        tgtTableName = tgtPath[tgtPath.length - 1] || '';
      }
      const tgtFolderName: string = tgtEl.name || (tgtTableName ? sigmaDisplayName(tgtTableName) : '') || rel.name;
      for (const col of (tgtEl.columns || [])) {
        if (tgtKeyIds.has(col.id)) continue;
        if (!col.formula || col.formula.startsWith('/*')) continue;
        let dispName: string | undefined;
        // Prefer an explicit `name` (Custom SQL columns and calc columns carry one)
        // so the cross-element ref uses the friendly display the workbook expects;
        // fall back to the formula's table-qualified tail for plain warehouse cols.
        if ((col as any).name) {
          dispName = String((col as any).name);
        } else {
          const fm = col.formula.match(/^\[([^\]]+)\]$/);
          if (fm) {
            const inner = fm[1];
            // The display name is everything after the FIRST slash (the table/element
            // prefix). Use indexOf, not lastIndexOf — a display name may itself contain a
            // slash (e.g. Tableau caption "Product Key/Name").
            const s = inner.indexOf('/');
            dispName = s >= 0 ? inner.slice(s + 1) : inner;
          }
        }
        if (!dispName) continue;
        // A display name containing a "/" cannot be referenced via Sigma's
        // slash-delimited bracket syntax ([Base/Rel/Field] would over-segment). Such a
        // column stays accessible on its own dimension element; just don't surface it as
        // a denormalized cross-element passthrough (it would compile to type "error").
        if (dispName.includes('/')) continue;
        const cId = sigmaShortId();
        // Related/lookup column: HIDE it (Sigma's UI renders these as "lookup"
        // columns; a data model with one per column of every related target is
        // the reported clutter) but never drop it — every [Base/Rel/Field]
        // formula elsewhere, and every alt the powerbi-to-sigma migration skill
        // registers for a workbook binding (migrate-powerbi.rb ~1199-1260), must
        // still resolve through this exact column. Verified live (2026-07-31):
        // a hidden cross-element column on a derived View element (a) still
        // resolves to its real type — not "error" — via
        // GET /v2/dataModels/<id>/columns, and (b) is still resolvable when
        // referenced from ANOTHER element's formula. `hidden` is display/
        // permission-only; it does not affect resolvability.
        viewCols.push({ id: cId, formula: `[${baseName}/${rel.name}/${dispName}]`, hidden: true });
        viewOrder.push(cId);

        let folder = folderByTarget.get(rel.targetElementId);
        if (!folder) {
          folder = { id: sigmaShortId(), name: tgtFolderName, items: [] };
          folderByTarget.set(rel.targetElementId, folder);
        }
        folder.items.push(cId);
      }
    }

    if (viewCols.length > 0) {
      const derivedEl: SigmaElement = {
        id: sigmaShortId(),
        kind: 'table',
        name: derivedName,
        source: { kind: 'table', elementId: srcEl.id },
        columns: viewCols,
        order: viewOrder,
      };
      if (folderByTarget.size > 0) {
        (derivedEl as any).folders = [...folderByTarget.values()];
      }
      derived.push(derivedEl);
    }
  }
  return derived;
}
