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

/** SNAKE_CASE → "Title Case" display name */
export function sigmaDisplayName(s: string): string {
  const words = (s || '').toLowerCase().split('_');
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

Linked Column Reference (accessing related dimension columns via relationships):
  [SOURCE_TABLE/FK_COLUMN - link/Column Display Name]
  Example: DateDiff("day", [ORDER_FACT/PROMO_KEY - link/Start Date], [ORDER_FACT/PROMO_KEY - link/End Date])
  ⚠ Known API limitation: Sigma API may not round-trip linked columns on PUT. Users may need to re-add in UI.

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
