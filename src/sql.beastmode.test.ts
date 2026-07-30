// Regression coverage for the Domo Beast Mode defect class (beads jva2/sqp1 + five
// defects found alongside them). Every input here is a real shape from the live
// 48-card Domo corpus, normalised the way convert-beast-modes.rb normalises it
// (backtick identifiers → [brackets]).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { stripOuterParens, lookSqlToSigmaRules, tableauTextConcatToSigma } from './formulas.js';

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
