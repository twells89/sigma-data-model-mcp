// Regression coverage for the Domo Beast Mode defect class (beads jva2/sqp1 + five
// defects found alongside them). Every input here is a real shape from the live
// 48-card Domo corpus, normalised the way convert-beast-modes.rb normalises it
// (backtick identifiers → [brackets]).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { stripOuterParens, lookSqlToSigmaRules } from './formulas.js';

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
