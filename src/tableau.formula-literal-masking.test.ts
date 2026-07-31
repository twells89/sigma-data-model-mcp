/**
 * Bug: tableauFormulaToSigma's function-name mapping (COUNT→CountIf,
 * ZN→Coalesce, IFNULL→Coalesce, TABLEAU_FUNC_MAP, …) and its keyword-casing
 * passes (TRUE/FALSE/NULL/AND/OR/NOT) scan the raw formula text with no idea
 * that string literals exist. A literal that happens to contain mapped
 * function-name text — or even just the English words "true"/"false"/"null"
 * — gets rewritten INSIDE the quotes, corrupting a value that reaches the
 * customer's dashboard.
 *
 * Demonstrated (live-reproduced, pre-fix):
 *   tableauFormulaToSigma("'See Count(Open Items) report'")
 *     → "See CountIf(IsNotNull(Open Items)) report"    (content rewritten)
 *
 * Control (must keep working): COUNT([Orders]) → CountIf(IsNotNull([Orders]))
 * — a bare COUNT() OUTSIDE any literal must still translate exactly as today.
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { tableauFormulaToSigma } from './formulas.js';

const conv = (f: string) => tableauFormulaToSigma(f);

describe('Tableau literal masking: demonstrated bug', () => {
  test("'See Count(Open Items) report' — literal content is unchanged", () => {
    assert.equal(conv(`'See Count(Open Items) report'`), `"See Count(Open Items) report"`);
  });
});

describe('Tableau literal masking: other mapped function names inside a literal', () => {
  test('ZN( inside a literal is not rewritten', () => {
    assert.equal(conv(`'Please use ZN(x) here'`), `"Please use ZN(x) here"`);
  });
  test('IFNULL( inside a literal is not rewritten', () => {
    assert.equal(conv(`'Try IFNULL(x, 0) instead'`), `"Try IFNULL(x, 0) instead"`);
  });
  test('a TABLEAU_FUNC_MAP name (ROUND() inside a literal is not rewritten', () => {
    assert.equal(conv(`'Please ROUND(x) manually'`), `"Please ROUND(x) manually"`);
  });
});

describe('Tableau literal masking: double-quoted literal', () => {
  test('a double-quoted literal containing a mapped name is unchanged', () => {
    assert.equal(conv(`"See Count(Open Items) report"`), `"See Count(Open Items) report"`);
  });
});

describe("Tableau literal masking: [Manager's Approval] bracket ref", () => {
  test("an apostrophe inside a [bracketed identifier] does not open a literal", () => {
    assert.equal(conv(`[Manager's Approval] = 'Yes'`), `[Manager's Approval] = "Yes"`);
  });
});

describe('Tableau literal masking: [SALES] inside a literal is not a field ref', () => {
  test('an ALL-CAPS bracket-look-alike inside a literal is not display-cased', () => {
    assert.equal(conv(`"Report shows [SALES] total"`), `"Report shows [SALES] total"`);
  });
});

describe('Tableau literal masking: unbalanced parens inside a literal', () => {
  test('a literal containing an unmatched ) does not truncate the surrounding function call', () => {
    assert.equal(
      conv(`COUNT('Has) closing paren inside')`),
      `CountIf(IsNotNull("Has) closing paren inside"))`,
    );
  });
  test('a literal containing an unmatched ( survives untouched', () => {
    assert.equal(conv(`'Report (draft'`), `"Report (draft"`);
  });
});

describe('Tableau literal masking: keyword-casing must not touch literal content', () => {
  test('TRUE/FALSE as plain English words inside a literal are not re-cased', () => {
    assert.equal(conv(`"true story, not false"`), `"true story, not false"`);
  });
  test('NULL as a plain English word inside a literal is not re-cased', () => {
    assert.equal(conv(`'contains a NULL value'`), `"contains a NULL value"`);
  });
});

describe('Tableau literal masking: controls (unchanged behavior OUTSIDE literals)', () => {
  test('COUNT([Orders]) → CountIf(IsNotNull([Orders]))', () => {
    assert.equal(conv(`COUNT([Orders])`), `CountIf(IsNotNull([Orders]))`);
  });
  test('ZN([X]) → Coalesce([X], 0)', () => {
    assert.equal(conv(`ZN([X])`), `Coalesce([X], 0)`);
  });
  test('IFNULL([X], 0) → Coalesce([X], 0)', () => {
    assert.equal(conv(`IFNULL([X], 0)`), `Coalesce([X], 0)`);
  });
  test('ROUND([X]) → Round([X])', () => {
    assert.equal(conv(`ROUND([X])`), `Round([X])`);
  });
  test('bare TRUE/FALSE/NULL keyword casing outside a literal is unchanged', () => {
    assert.equal(conv(`[X] = true`), `[X] = True`);
    assert.equal(conv(`[X] = false`), `[X] = False`);
    assert.equal(conv(`[X] = null`), `[X] = null`);
  });
  test('AND/OR/NOT keyword casing outside a literal is unchanged', () => {
    assert.equal(conv(`[X] AND [Y]`), `[X] and [Y]`);
    assert.equal(conv(`[X] OR [Y]`), `[X] or [Y]`);
    assert.equal(conv(`NOT [X]`), `Not [X]`);
  });
});
