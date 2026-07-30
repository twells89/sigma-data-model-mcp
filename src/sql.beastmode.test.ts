// Regression coverage for the Domo Beast Mode defect class (beads jva2/sqp1 + five
// defects found alongside them). Every input here is a real shape from the live
// 48-card Domo corpus, normalised the way convert-beast-modes.rb normalises it
// (backtick identifiers → [brackets]).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { stripOuterParens, lookSqlToSigmaRules, tableauTextConcatToSigma, lookConvertExpression } from './formulas.js';

test('stripOuterParens unwraps a whole-expression wrapper, repeatedly (jva2)', () => {
  assert.equal(stripOuterParens('(x)'), 'x');
  assert.equal(stripOuterParens('((x))'), 'x');
  assert.equal(stripOuterParens('  ( x )  '), 'x');
});

test('stripOuterParens leaves non-wrapping parens alone (jva2)', () => {
  assert.equal(stripOuterParens('(a) + (b)'), '(a) + (b)');
  assert.equal(stripOuterParens('(a) AND (b)'), '(a) AND (b)');
  assert.equal(stripOuterParens('Sum(x)'), 'Sum(x)');
  assert.equal(stripOuterParens('(unbalanced'), '(unbalanced');
});

test('stripOuterParens is not fooled by parens inside string literals (jva2)', () => {
  // The ')' here is data, not structure — stripping on a naive depth count corrupts it.
  assert.equal(stripOuterParens("('a)b')"), "'a)b'");
});

test('a paren-wrapped CASE now reaches the CASE rule instead of falling through (jva2)', () => {
  const sql = '(CASE WHEN SUM([Net Revenue]) = 0 THEN 0 ELSE SUM([Gross Profit]) / SUM([Net Revenue]) END )';
  const out = lookSqlToSigmaRules(sql);
  assert.ok(out !== null, 'must match a rule, not return null');
  assert.equal(out, 'If(Sum([Net Revenue]) = 0, 0, Sum([Gross Profit]) / Sum([Net Revenue]))');
});

// Review finding (round 1): a bare apostrophe inside a [bracketed identifier] was
// treated as a string-literal delimiter, putting the scanner in a permanent in-quote
// state that swallowed the real closing ')' — depth never returned to 0, so the
// outer parens were silently left in place. A `[...]` span must be atomic: a quote
// character inside brackets is part of the identifier, not a literal delimiter.
test('stripOuterParens is not fooled by an apostrophe inside a [bracketed identifier] (jva2 review)', () => {
  assert.equal(
    stripOuterParens("(CASE WHEN [Manager's Approval] = 1 THEN 1 ELSE 0 END)"),
    "CASE WHEN [Manager's Approval] = 1 THEN 1 ELSE 0 END",
  );
});

test('lookSqlToSigmaRules reaches the CASE rule when a bracketed identifier contains an apostrophe (jva2 review)', () => {
  const sql = "(CASE WHEN [Manager's Approval] = 1 THEN 1 ELSE 0 END)";
  const out = lookSqlToSigmaRules(sql);
  assert.ok(out !== null, 'must match a rule, not return null');
  assert.equal(out, "If([Manager's Approval] = 1, 1, 0)");
});

test('tableauTextConcatToSigma resolves a paren-wrapped bracket ref with an apostrophe via isTextRef (jva2 review)', () => {
  // Mirrors the [CW_COUNTRY] control case that already works — the only difference
  // is the apostrophe inside the identifier, which must not defeat the paren-unwrap.
  const isTextRef = (name: string) => name === "Manager's Approval";
  const out = tableauTextConcatToSigma("([Manager's Approval]) + [OtherNum]", isTextRef);
  assert.equal(out, "([Manager's Approval]) & [OtherNum]");
});

test('ALL-CAPS text inside a string literal is NOT rewritten as a column ref (A3)', () => {
  // Before the fix this produced [State] = '[Ak]' — silent data corruption.
  assert.equal(lookConvertExpression("[State] = 'AK'"), '[State] = "AK"');
});

test('string literals are emitted double-quoted, Sigma style (A6)', () => {
  assert.equal(lookConvertExpression("'West'"), '"West"');
  // an embedded double quote must be escaped, not emitted raw
  assert.equal(lookConvertExpression(`'say "hi"'`), '"say \\"hi\\""');
  // SQL's doubled-single-quote escape unescapes to one apostrophe
  assert.equal(lookConvertExpression("'it''s'"), '"it\'s"');
});

test('a CASE over string literals converts without corrupting them (A3+A6)', () => {
  const sql = "(CASE WHEN [Billing State] = 'AK' THEN 'West' ELSE 'Other' END)";
  assert.equal(
    lookSqlToSigmaRules(sql),
    'If([Billing State] = "AK", "West", "Other")'
  );
});

// Review finding (A3/A6 masking): an apostrophe inside a [bracketed identifier]
// (e.g. [Manager's Approval]) is part of the identifier, not a string-literal
// delimiter — the same hazard Task 1's review caught in stripOuterParens. A
// naive _maskLiterals regex run over the whole string treats that apostrophe as
// an opening quote and swallows everything up to the NEXT real quote, which
// corrupts BOTH the identifier and the literal that followed it: before the
// fix, `[Manager's Approval] = 'AK'` masked/unmasked into
// `[Manager"s Approval] = "[Ak]'` instead of leaving the identifier alone and
// converting the literal to Sigma double-quoted form.
test("lookConvertExpression does not mis-mask a literal when an apostrophe sits inside a [bracketed identifier] (A3+A6 review)", () => {
  assert.equal(
    lookConvertExpression("[Manager's Approval] = 'AK'"),
    '[Manager\'s Approval] = "AK"'
  );
});

// Round 1 finding: an unterminated '[' (no matching ']' anywhere in the rest of
// the string) was treated as opening one giant atomic bracket span running to
// end-of-string. Every literal after that point was therefore never masked and
// got bracket-corrupted by passes 1-3 when it hit an ALL-CAPS token inside it —
// the exact defect class this task exists to eliminate, reintroduced by the
// bracket-awareness itself. A '[' with no matching ']' must degrade to an
// ordinary character (never swallow the remainder of the string), same as the
// brief's original plain-regex behaviour for this input.
test('an unterminated [ does not swallow the rest of the string, reintroducing A3 corruption (round 1 review)', () => {
  // reviewer's exact reproduction input
  assert.equal(lookConvertExpression("[Foo = 'AK'"), '[Foo = "AK"');
});

test('a trailing unterminated [ with no literal after it is left alone (round 1 review)', () => {
  assert.equal(
    lookConvertExpression("[Region] = 'West' AND [Foo"),
    '[Region] = "West" AND [Foo'
  );
});

test('an unterminated [ followed by two literals still masks both (round 1 review)', () => {
  assert.equal(lookConvertExpression("[Foo = 'A' OR 'B'"), '[Foo = "A" OR "B"');
});

test('SQL keywords before a paren stay infix, not function calls (A4)', () => {
  // Before: ([A] > 1) And([B] < 2) — and And()/Or() as CALLS silently null rows in Sigma.
  assert.equal(lookConvertExpression('(A > 1) AND (B < 2)'), '([A] > 1) AND ([B] < 2)');
  assert.equal(lookConvertExpression('(A > 1) OR (B < 2)'), '([A] > 1) OR ([B] < 2)');
  assert.equal(lookConvertExpression('NOT (A > 1)'), 'NOT ([A] > 1)');
});

test('zero-arg function maps do not double their parens (A5)', () => {
  assert.equal(lookConvertExpression('CURRENT_DATE()'), 'Today()');   // was Today()()
  assert.equal(lookConvertExpression('GETDATE()'), 'Now()');          // was Now()()
});

// Attention item 1 (task-3 review): DISTINCT is in the keyword list, so it must
// never be treated as a callable when it happens to sit directly before a paren
// (the classic `SELECT DISTINCT(col)` style). This must NOT interfere with
// Task 4's separate `COUNT(DISTINCT x)` handling: in that shape DISTINCT is
// followed by a space then the argument, never directly by '(', so pass 1 (the
// name-before-paren regex) never matches DISTINCT there regardless of whether
// it's in the keyword list.
test('DISTINCT directly before a paren is left as literal text, not mapped to a bogus Distinct() call (A4 keyword list)', () => {
  assert.equal(lookConvertExpression('DISTINCT(ORDER_ID)'), 'DISTINCT([Order Id])');
});

// Attention item 2 (task-3 review): confirm excluding IN from pass 1's callable
// mapping does not break pass 2's separate `EXPR IN (a,b,c)` -> `In(...)` rewrite.
// Pass 2 matches "IN" case-insensitively regardless of what pass 1 did to its
// casing, so this must still produce Sigma's real In() call.
test('IN is excluded from pass 1 callable mapping but pass 2 still rewrites it to In(...) (A4 keyword list)', () => {
  assert.equal(lookConvertExpression('ORDER_ID IN (1, 2, 3)'), 'In([Order Id], 1, 2, 3)');
});
