/**
 * Bug: tableauFormulaToSigma's function-name mapping (COUNT→CountIf,
 * ZN→Coalesce, IFNULL→Coalesce, TABLEAU_FUNC_MAP, …), its keyword-casing
 * passes (TRUE/FALSE/NULL/AND/OR/NOT), its IN-list rewrite, its IF/CASE
 * lowering, and its date/user-context functions (DATEPART/DATEADD/.../
 * USERNAME/ISMEMBEROF) all scan the raw formula text with no idea that
 * string literals exist. A literal that happens to contain matching text —
 * a mapped function name, an "IN (", a "THEN"/"WHEN" keyword, a date-part
 * call, or even just the English words "true"/"false"/"null" — gets
 * rewritten or mis-split INSIDE the quotes, corrupting a value (or, for
 * IF/CASE, silently dropping an entire branch) reaching the customer's
 * dashboard.
 *
 * Demonstrated (live-reproduced, pre-fix):
 *   tableauFormulaToSigma("'See Count(Open Items) report'")
 *     → "See CountIf(IsNotNull(Open Items)) report"    (content rewritten)
 *
 * Also reproduced against an EARLIER version of this fix that masked in two
 * separate windows with a stretch of raw text running IN-list/IF-CASE/
 * DATEPART/USERNAME between them (reviewed and rejected — see formulas.ts):
 *   'Please choose IN (1,2,3) as your range'
 *     → "Please (choose = 1 or choose = 2 or choose = 3) as your range"
 *   IF [x] = 'contains THEN keyword' THEN 'a' ELSE 'b' END
 *     → If([x] = "contains, keyword", "b")   ← the THEN value 'a' is GONE
 *   'See DATEPART(\'year\', [Date]) info' → "See Year([Date]) info"
 *   'USERNAME() returns email'            → "CurrentUserEmail() returns email"
 *   'Call STDEVP(x) for pop stddev'       → "Call Sqrt(VariancePop(x)) ..."
 *
 * The fix masks ONCE across the whole function body; every pass runs
 * against masked text, and the two passes that recurse into a fresh
 * tableauFormulaToSigma call per branch (tableauIfToSigma/tableauCaseToSigma)
 * explicitly restore-then-remask around that recursive call.
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
  // NOTE: an earlier version of this test used a BARE literal with an
  // unmatched '(' ('Report (draft') as its own whole formula. Reviewer
  // caught that it doesn't discriminate: no pass in the OLD (unfixed)
  // pipeline ever touches a standalone literal not wrapped in any function
  // call, so it passed even against the unfixed code and proved nothing.
  // Replaced with an input that DOES exercise a real paren-depth scanner:
  // tableauInToSigma counts matching parens char-by-char to find the IN-list's
  // closing ')' — an unmasked literal value containing its own unmatched '('
  // throws that count off and the list either mis-splits or is silently
  // left unconverted.
  test('an IN-list value containing an unmatched ( does not throw off the list scan', () => {
    assert.equal(
      conv(`[Status] IN ('A(', 'B')`),
      `([Status] = "A(" or [Status] = "B")`,
    );
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

describe('Tableau literal masking: IN-list keyword inside a literal', () => {
  test('"IN (" text inside a literal is not rewritten into an or-chain', () => {
    assert.equal(
      conv(`'Please choose IN (1,2,3) as your range'`),
      `"Please choose IN (1,2,3) as your range"`,
    );
  });
});

describe('Tableau literal masking: THEN/ELSE keyword inside a literal (IF)', () => {
  test('a literal containing THEN does not swallow the real THEN branch', () => {
    assert.equal(
      conv(`IF [x] = 'contains THEN keyword' THEN 'a' ELSE 'b' END`),
      `If([x] = "contains THEN keyword", "a", "b")`,
    );
  });
});

describe('Tableau literal masking: CASE/WHEN keyword inside a literal', () => {
  test('a literal containing CASE/WHEN/THEN/END is not read as CASE structure', () => {
    assert.equal(
      conv(`'Use CASE WHEN x THEN y END syntax'`),
      `"Use CASE WHEN x THEN y END syntax"`,
    );
  });
});

describe('Tableau literal masking: DATEPART inside a literal', () => {
  test("a literal containing DATEPART('year', ...) text is not converted", () => {
    assert.equal(
      conv(`'See DATEPART(\\'year\\', [Date]) info'`),
      `"See DATEPART('year', [Date]) info"`,
    );
  });
});

describe('Tableau literal masking: DATEADD inside a literal', () => {
  test("a literal containing DATEADD('day', ...) text is not converted", () => {
    assert.equal(
      conv(`'Try DATEADD(\\'day\\', 1, [Date]) manually'`),
      `"Try DATEADD('day', 1, [Date]) manually"`,
    );
  });
});

describe('Tableau literal masking: WEEK inside a literal', () => {
  test('a literal containing WEEK([Date]) text is not converted', () => {
    assert.equal(
      conv(`'See WEEK([Date]) for the week number'`),
      `"See WEEK([Date]) for the week number"`,
    );
  });
});

describe('Tableau literal masking: STDEVP inside a literal', () => {
  test('a literal containing STDEVP(x) text is not converted', () => {
    assert.equal(
      conv(`'Call STDEVP(x) for pop stddev'`),
      `"Call STDEVP(x) for pop stddev"`,
    );
  });
});

describe('Tableau literal masking: USERNAME inside a literal', () => {
  test('a literal containing USERNAME() text is not converted', () => {
    assert.equal(
      conv(`'USERNAME() returns email'`),
      `"USERNAME() returns email"`,
    );
  });
});

describe('Tableau literal masking: ISMEMBEROF inside a literal', () => {
  test("a literal containing ISMEMBEROF('g') text is not converted", () => {
    assert.equal(
      conv(`'Checks ISMEMBEROF(\\'Sales\\') internally'`),
      `"Checks ISMEMBEROF('Sales') internally"`,
    );
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
  test('a genuine IN-list still converts to an or-chain', () => {
    assert.equal(conv(`[Status] IN ('A', 'B', 'C')`), `([Status] = "A" or [Status] = "B" or [Status] = "C")`);
  });
  test('a genuine IF/THEN/ELSE/END still converts, all branches present', () => {
    assert.equal(conv(`IF [x] = 1 THEN 'a' ELSE 'b' END`), `If([x] = 1, "a", "b")`);
  });
  test('a genuine CASE/WHEN/THEN/ELSE/END still converts, all branches present', () => {
    assert.equal(
      conv(`CASE [x] WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END`),
      `If([x] = 1, "a", If([x] = 2, "b", "c"))`,
    );
  });
  test("a genuine DATEPART('year', [Date]) still converts", () => {
    assert.equal(conv(`DATEPART('year', [Date])`), `Year([Date])`);
  });
  test("a genuine DATEADD('day', 1, [Date]) still converts", () => {
    assert.equal(conv(`DATEADD('day', 1, [Date])`), `DateAdd("day", 1, [Date])`);
  });
  test("a genuine DATEDIFF('day', [A], [B]) still converts", () => {
    assert.equal(conv(`DATEDIFF('day', [A], [B])`), `DateDiff("day", [A], [B])`);
  });
  test("a genuine DATETRUNC('week', [Date], 'monday') still converts (3rd arg stripped)", () => {
    assert.equal(conv(`DATETRUNC('week', [Date], 'monday')`), `DateTrunc("week", [Date])`);
  });
  test('a genuine WEEK([Date]) still converts', () => {
    assert.equal(conv(`WEEK([Date])`), `DatePart("week", [Date])`);
  });
  test('a genuine STDEVP([X]) still converts', () => {
    assert.equal(conv(`STDEVP([X])`), `Sqrt(VariancePop([X]))`);
  });
  test('a genuine USERNAME() still converts', () => {
    assert.equal(conv(`USERNAME()`), `CurrentUserEmail()`);
  });
  test("a genuine ISMEMBEROF('Sales Team') still converts", () => {
    assert.equal(conv(`ISMEMBEROF('Sales Team')`), `CurrentUserInTeam("Sales Team")`);
  });
});
