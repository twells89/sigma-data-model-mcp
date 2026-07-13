/**
 * B1 — silent-passthrough catch-all (bead beads-sigma-tt3z.1).
 *
 * Any Tableau function with no validated Sigma mapping must NEVER reach the
 * output verbatim without a warning. Before this guard, unmapped functions
 * (FINDNTH, CHAR, MAKEDATETIME, MODEL_QUANTILE, the trig family…) passed
 * through as clean-looking-but-invalid Sigma formulas that only failed at query
 * time — the "agent falls off the rails" failure surfaced by the corpus gap
 * analysis. This locks: unmapped ⇒ loud warning; mapped ⇒ no false positive.
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { tableauFormulaToSigma } from './formulas.js';

function conv(formula: string): { out: string; warnings: string[] } {
  const warnings: string[] = [];
  const out = tableauFormulaToSigma(formula, warnings);
  return { out, warnings };
}
const UNMAPPED_RE = /Unmapped Tableau function/;

describe('B1: unmapped Tableau functions warn (never silent)', () => {
  // [formula, expected-function-name-in-warning]
  const cases: [string, string][] = [
    ['FINDNTH([Path], "/", 3)', 'FINDNTH'],
    ['CHAR(65)', 'CHAR'],
    ['MAKEDATETIME([D], [T])', 'MAKEDATETIME'],
    ['MODEL_QUANTILE(0.5, SUM([Sales]))', 'MODEL_QUANTILE'],
    ['MAKETIME(10, 30, 0)', 'MAKETIME'],
    // scalar wrapping a mapped call — outer unmapped must still be caught
    ['CHAR(SUM([Code]))', 'CHAR'],
  ];
  for (const [src, fn] of cases) {
    test(`${src} → warns about ${fn}`, () => {
      const { warnings } = conv(src);
      assert.ok(
        warnings.some(w => UNMAPPED_RE.test(w) && w.includes(fn)),
        `expected an "Unmapped Tableau function" warning naming ${fn}; got: ${JSON.stringify(warnings)}`);
    });
  }
});

describe('B1: mapped constructs do NOT false-positive', () => {
  // Formulas that fully translate must not trip the unmapped-function warning.
  const clean: string[] = [
    'SUM([Sales])',
    'SPLIT([Full Name], " ", 2)',
    'REGEXP_EXTRACT([Code], "([0-9]+)")',
    "DATETRUNC('month', [Order Date])",
    "DATEDIFF('day', [Start], [End])",
    'ISMEMBEROF("Managers")',
    'IF [Region] = "East" THEN "E" ELSE "W" END',
    'ZN(SUM([Sales]))',
    'COUNTD([Customer])',
    // function-like token INSIDE a string literal must be ignored
    'IF [x] = 1 THEN "TOTAL(x) label" ELSE "n/a" END',
  ];
  for (const src of clean) {
    test(`${src} → no unmapped warning`, () => {
      const { warnings } = conv(src);
      assert.ok(
        !warnings.some(w => UNMAPPED_RE.test(w)),
        `unexpected unmapped-function warning for a fully-mapped formula: ${JSON.stringify(warnings)}`);
    });
  }
});
