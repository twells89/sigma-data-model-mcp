/**
 * B2 — newly-mapped Tableau functions (bead beads-sigma-tt3z.2/tt3z.3).
 *
 * Phase A flagged these as unmapped (silently passed through pre-B1). Each Sigma
 * target below was live-verified to resolve against the warehouse (2026-07-10):
 * trig Sin/Cos/Tan/Cot/Asin/Acos/Atan/Atan2, Degrees, Radians, Proper — direct
 * name-swaps; SQUARE(x)→Power(x,2) and SPACE(n)→Repeat(" ",n) — arg-rewrites
 * (Sigma has no Square/Space, but Power/Repeat resolve). Full convert→POST→query
 * e2e passed 13/13.
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { tableauFormulaToSigma } from './formulas.js';
const conv = (f: string) => tableauFormulaToSigma(f, []);

describe('B2: trig / angle / proper direct mappings', () => {
  const cases: [string, string][] = [
    ['SIN([X])', 'Sin([X])'], ['COS([X])', 'Cos([X])'], ['TAN([X])', 'Tan([X])'],
    ['COT([X])', 'Cot([X])'], ['ASIN([X])', 'Asin([X])'], ['ACOS([X])', 'Acos([X])'],
    ['ATAN([X])', 'Atan([X])'], ['ATAN2([Y],[X])', 'Atan2([Y],[X])'],
    ['DEGREES([X])', 'Degrees([X])'], ['RADIANS([X])', 'Radians([X])'],
    ['PROPER([S])', 'Proper([S])'],
  ];
  for (const [src, want] of cases) test(`${src} → ${want}`, () => assert.equal(conv(src), want));
});

describe('B2: arg-rewrite mappings', () => {
  test('SQUARE(x) → Power(x, 2)', () => assert.equal(conv('SQUARE([X])'), 'Power([X], 2)'));
  test('SQUARE of a nested expr', () => assert.equal(conv('SQUARE([A] + [B])'), 'Power([A] + [B], 2)'));
  test('SPACE(n) → Repeat(" ", n)', () => assert.equal(conv('SPACE(3)'), 'Repeat(" ", 3)'));
});

describe('B2: no cross-contamination (prefix regressions)', () => {
  // ATAN must not eat ATAN2; COS must not eat ACOS; SIN must not eat ASIN.
  test('ATAN2 survives the ATAN entry', () => assert.equal(conv('ATAN2([Y],[X])'), 'Atan2([Y],[X])'));
  test('ACOS not mangled by COS', () => assert.equal(conv('ACOS([X])'), 'Acos([X])'));
  test('ASIN not mangled by SIN', () => assert.equal(conv('ASIN([X])'), 'Asin([X])'));
});
