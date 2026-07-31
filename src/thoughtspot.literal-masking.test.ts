/**
 * ThoughtSpot converter tests — string-literal-blind scanning in the
 * measure/RLS formula translator (tsFormulaToSigma and its helpers).
 *
 * tsFormulaToSigma's keyword-mapping tail (sum/count/average/…, the
 * date-part and math/string renames, safe_divide/cond-agg arg swaps) used to
 * run plain regex .replace() over the formula text right after converting a
 * TML '…' literal to a Sigma "…" string — with no further protection until
 * the (also separately masking) bracket-identifier pass ran at the very end.
 * Two concrete, live-reproduced failures motivated the fix:
 *
 *   1. A formula that was NOTHING BUT the literal 'This report shows
 *      sum(x) trends' came back with RAW, UNRESTORED SENTINEL BYTES in the
 *      final Sigma formula — "This report shows Sum(<sentinel>) trends" —
 *      not just mis-rewritten content: two independent, per-call mask/
 *      restore cycles (one nested inside the aggregate-rename replacer, one
 *      on the whole string at the end) corrupted each other.
 *   2. 'sum(' & sum([T::COL]) — a literal containing an unbalanced "("
 *      followed by a real, SEPARATE sum() call — came back with the LITERAL
 *      capitalized to "Sum(" while the REAL sum() call was left untranslated
 *      (lowercase, unmapped) — both directions wrong at once.
 *
 * A `[bracketed field]` (a `[TABLE::COL]` ref, or the `[Display Name]` it's
 * rewritten to) is atomic — an apostrophe inside one is not a quote opener.
 * See the block comment above tsMaskLiterals() in thoughtspot.ts for the
 * full writeup.
 */

import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { convertThoughtSpotToSigma } from './thoughtspot.js';

function conv(measureName: string, expr: string) {
  const esc = expr.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
  const yamlText = `
worksheet:
  name: Literal Masking Test
  tables:
    - name: ORDER_FACT
      db: CSA
      schema: TJ
  table_paths:
    - id: order_fact_1
      table: ORDER_FACT
  formulas:
    - id: f1
      name: ${measureName}
      expr: "${esc}"
  worksheet_columns:
    - name: Net Revenue
      column_id: order_fact_1::NET_REVENUE
      type: MEASURE
      aggregation: SUM
    - name: ${measureName}
      formula_id: f1
      type: MEASURE
`;
  return convertThoughtSpotToSigma(yamlText, { connectionId: 'conn-1', database: 'CSA', schema: 'TJ' });
}

function namedFormula(r: any, name: string): string | undefined {
  const els: any[] = r.model.pages[0].elements;
  const metrics = els.flatMap(e => e.metrics || []);
  const cols = els.flatMap(e => e.columns || []);
  return (metrics.find((m: any) => m.name === name) || cols.find((c: any) => c.name === name))?.formula;
}

describe('thoughtspot literal masking: a literal is not live syntax', () => {
  test('a literal is never lowered by the keyword-mapping tail, and no sentinel bytes leak into the output', () => {
    const r = conv('LiteralBug', "'This report shows sum(x) trends'");
    const f = namedFormula(r, 'LiteralBug');
    assert.equal(f, '"This report shows sum(x) trends"',
      'the literal\'s CONTENT must survive verbatim, and must not contain leftover mask sentinel bytes');
    assert.doesNotMatch(f!, /[\x00-\x08]/, 'no raw control-character sentinel bytes may leak into the final formula');
  });

  test("a literal with an unbalanced paren does not swallow a real, separate sum() call, and does not itself get renamed", () => {
    const r = conv('DroppedReal', "'sum(' & sum([order_fact_1::NET_REVENUE])");
    const f = namedFormula(r, 'DroppedReal');
    assert.ok(f);
    assert.match(f!, /"sum\("/, 'the literal must survive with its original lowercase content, not be renamed to "Sum("');
    assert.match(f!, /Sum\(\[Net Revenue\]\)/, 'the real, separate sum() call must be translated');
  });

  test('a Set Analysis-shaped literal list value containing a keyword substring is not corrupted', () => {
    const r = conv('InLit', "[order_fact_1::NET_REVENUE] in {'contains sum( in it', 'b'}");
    const f = namedFormula(r, 'InLit');
    assert.ok(f);
    assert.equal(f, 'In([Net Revenue], "contains sum( in it", "b")');
  });
});

describe('thoughtspot literal masking: a bracketed field-reference apostrophe is not a quote opener', () => {
  // thoughtspot.ts's ORIGINAL bracket protection was a plain `/\[[^\]]*\]/g`
  // regex (not a character-by-character quote-aware walker like qlik.ts's
  // matchClose) — a `]`-terminated match doesn't care whether an apostrophe
  // sits inside it, so this specific trap was never independently exploitable
  // here. Kept as a regression guard: masking must not newly break it either.
  test("sum([field]) + count([Manager's Approval]) — an apostrophe inside a bracketed field ref does not break translation", () => {
    const r = conv('BracketApostrophe', "sum([order_fact_1::NET_REVENUE]) + count([Manager's Approval])");
    const f = namedFormula(r, 'BracketApostrophe');
    assert.equal(f, "Sum([Net Revenue]) + Count([Manager's Approval])");
  });
});

describe('thoughtspot literal masking: RLS expressions go through the same masking', () => {
  test('an RLS rule literal is not lowered by the keyword tail either (tsRlsExprToSigma no longer pre-converts quotes itself)', () => {
    const yamlText = `
worksheet:
  name: RLS Literal Test
  tables:
    - name: ORDER_FACT
      db: CSA
      schema: TJ
  table_paths:
    - id: order_fact_1
      table: ORDER_FACT
  rls_rules:
    - expr: "[order_fact_1::REGION] = 'contains sum(x) note'"
  worksheet_columns:
    - name: Region
      column_id: order_fact_1::REGION
      type: ATTRIBUTE
    - name: Net Revenue
      column_id: order_fact_1::NET_REVENUE
      type: MEASURE
      aggregation: SUM
`;
    const r = convertThoughtSpotToSigma(yamlText, { connectionId: 'conn-1', database: 'CSA', schema: 'TJ' });
    const rule: any = r.security?.find((s: any) => s.kind === 'rls');
    assert.ok(rule, 'must report a detected RLS rule');
    assert.match(rule.rls.formula || '', /"contains sum\(x\) note"/, 'the RLS literal must survive intact, not be lowered by the keyword tail');
  });
});
