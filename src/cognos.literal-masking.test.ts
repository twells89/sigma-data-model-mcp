/**
 * Cognos converter tests — string-literal-blind scanning in the calculated-
 * column/measure formula translator (translateCognosExpr and its helpers).
 *
 * translateCognosExpr's keyword-mapping passes (the `[NS].[QS].[Item]`
 * column-ref rewrite, the CASE/if-then-else structural splitters, the
 * total/sum/average/… aggregate map, the date/string/math renames) used to
 * run plain regex .replace() over the raw formula text, with the Cognos
 * `'…'` → Sigma `"…"` quote conversion happening LAST — so every earlier
 * pass ran blind to string literals. Three concrete, live-reproduced
 * failures motivated the fix:
 *
 *   1. A calc whose ENTIRE expression was the literal 'This report shows
 *      total(REVENUE) trends' came back with its CONTENT rewritten — both
 *      the function name and a bare word inside the literal got treated as
 *      live code.
 *   2. A literal containing bracket-shaped text — 'see [REVENUE_TOTAL] note'
 *      — had that bracketed span independently re-cased by the very FIRST
 *      pass (the column-ref rewrite, which has no idea a quote exists).
 *   3. Worst: a THEN-branch literal containing the word "else" —
 *      case when REVENUE > 100 then 'high or else low' else 'low' end —
 *      made the CASE splitter find the literal's OWN embedded "else"
 *      instead of the real one, producing a malformed formula with a stray
 *      literal "else" keyword and an unbalanced paren surviving into the
 *      output — the exact defect class the tableau CASE/IF fix
 *      (src/formulas.ts) exists to close.
 *
 * See the block comment above cognosMask() in cognos.ts for the full
 * writeup, including why masking happens in TWO stages (quotes first, then
 * brackets — the column-ref rewrite needs to see real bracket text).
 */

import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { translateCognosExpr, type CognosQuerySubject } from './cognos.js';

const qs: CognosQuerySubject = {
  identifier: 'SALES_FACT',
  items: [
    { identifier: 'REVENUE', label: 'Revenue' },
    { identifier: 'PRODUCT_LINE', label: 'Product Line' },
  ],
};
const ctx: any = { tableTail: 'SALES_FACT' };
const noop = () => '';

function translate(expr: string) {
  return translateCognosExpr(expr, qs, noop, ctx);
}

describe('cognos literal masking: a literal is not live syntax', () => {
  test('a literal is never lowered by the aggregate-function rename', () => {
    const { formula } = translate("'This report shows total(REVENUE) trends'");
    assert.equal(formula, '"This report shows total(REVENUE) trends"',
      'the literal\'s CONTENT must survive verbatim — not be rewritten to "...Sum([SALES_FACT/Revenue])..."');
  });

  test("a literal shaped like an unclosed call does not swallow a real, separate total() call, and does not itself get renamed", () => {
    const { formula } = translate("'total(' & total(REVENUE)");
    assert.match(formula, /"total\("/, 'the literal must survive with its original lowercase content, not be renamed to "Sum("');
    assert.match(formula, /Sum\(\[SALES_FACT\/Revenue\]\)/, 'the real, separate total() call must be translated');
  });

  test('a bare word inside a literal that happens to match a real column identifier is not treated as a live reference', () => {
    const { formula } = translate("'Product_line note'");
    assert.equal(formula, '"Product_line note"',
      'must not become "[SALES_FACT/Product Line] note" — a STRING VALUE, not a column reference');
  });

  test('bracket-shaped text inside a literal is not re-cased by the column-ref rewrite', () => {
    const { formula } = translate("'see [REVENUE_TOTAL] note'");
    assert.equal(formula, '"see [REVENUE_TOTAL] note"',
      'must not become "see [Revenue Total] note" — the bracket is part of the literal\'s text, not a live [NS].[Item] ref');
  });
});

describe('cognos literal masking: CASE literals containing keyword-shaped text', () => {
  test('a THEN-branch literal containing the word "else" does not confuse the CASE→If() splitter', () => {
    const { formula } = translate("case when REVENUE > 100 then 'high or else low' else 'low' end");
    assert.equal(formula, 'If([SALES_FACT/Revenue] > 100, "high or else low", "low")',
      'must be a well-formed 3-arg If() with the literal intact, not a malformed formula with a stray literal "else" keyword');
  });

  test('a WHEN/THEN literal containing the word "end" does not truncate the CASE expression early', () => {
    const { formula } = translate("case when REVENUE > 100 then 'the end result' else 'other' end");
    assert.equal(formula, 'If([SALES_FACT/Revenue] > 100, "the end result", "other")');
  });
});

describe('cognos literal masking: negative control — a real column ref still resolves', () => {
  test('a genuine bracketed column ref is still rewritten to its display name', () => {
    const { formula } = translate('[REVENUE]');
    assert.equal(formula, '[Revenue]');
  });

  test('a genuine bare column identifier known to the subject is still bracketed', () => {
    const { formula } = translate('Product_line');
    assert.equal(formula, '[SALES_FACT/Product Line]');
  });
});
