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

/** SNAKE_CASE or camelCase → "Title Case" display name */
export function sigmaDisplayName(s: string): string {
  // Insert underscores at camelCase boundaries so OrderDate → Order_Date
  const normalized = (s || '')
    .replace(/([a-z])([A-Z])/g, '$1_$2')       // camelCase → camel_Case
    .replace(/([A-Z]+)([A-Z][a-z])/g, '$1_$2'); // HTMLParser → HTML_Parser
  const words = normalized.toLowerCase().split('_').filter(Boolean);
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
export function inferSigmaFormat(formula: string, displayName?: string): Record<string, any> | null {
  if (!formula) return null;
  const f = formula.trim();
  const n = (displayName || '').toLowerCase();

  const alreadyPctScale = /\*\s*100\b/.test(f);
  if (alreadyPctScale && /\b(rate|margin|pct|percent|ratio|share|mix)\b|%/.test(n)) {
    return { kind: 'number', formatString: ',.2f', suffix: '%' };
  }
  if (/^[A-Za-z]+\s*\([^)]+\)\s*\/\s*[A-Za-z]+\s*\([^)]+\)$/.test(f)) {
    return { kind: 'number', formatString: ',.2%' };
  }
  if (/\b(rate|margin|pct|percent|ratio|share|mix)\b|%/.test(n)) {
    return { kind: 'number', formatString: ',.2%' };
  }
  if (/\b(revenue|sales|profit|cost|spend|amount|discounts?|price|value)\b/.test(n)) {
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
}

export interface SigmaMetric {
  id: string;
  formula: string;
  name: string;
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

export interface ConversionResult {
  model: SigmaDataModel;
  warnings: string[];
  stats: Record<string, number>;
}

export interface ElementResult {
  element: SigmaElement;
  elementId: string;
  colIdMap: Record<string, string>;
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
    const viewCols: Array<{ id: string; formula: string }> = [];
    const viewOrder: string[] = [];

    for (const col of (srcEl.columns || [])) {
      if (!col.formula || col.formula.startsWith('/*')) continue;
      const cId = sigmaShortId();
      viewCols.push({ id: cId, formula: col.formula });
      viewOrder.push(cId);
    }

    for (const rel of srcEl.relationships) {
      if (!rel.name) continue;
      const tgtEl = elements.find(e => e.id === rel.targetElementId);
      if (!tgtEl || tgtEl.source?.kind !== 'warehouse-table') continue;
      for (const col of (tgtEl.columns || [])) {
        if (!col.formula || col.formula.startsWith('/*')) continue;
        const fm = col.formula.match(/^\[([^\]]+)\]$/);
        if (!fm) continue;
        const inner = fm[1];
        const s = inner.lastIndexOf('/');
        const dispName = s >= 0 ? inner.slice(s + 1) : inner;
        const cId = sigmaShortId();
        viewCols.push({ id: cId, formula: `[${srcTableName}/${rel.name}/${dispName}]` });
        viewOrder.push(cId);
      }
    }

    if (viewCols.length > 0) {
      derived.push({
        id: sigmaShortId(),
        kind: 'table',
        name: srcEl.name || sigmaDisplayName(srcTableName),
        source: { kind: 'table', elementId: srcEl.id },
        columns: viewCols,
        order: viewOrder,
      });
    }
  }
  return derived;
}
