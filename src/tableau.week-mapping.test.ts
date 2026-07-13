/**
 * WEEK → DatePart("week", …) — bead beads-sigma-tt3z.2.
 *
 * Sigma has no Week() function (live query 2026-07-10 returned "Unknown
 * function: Week"), so the old TABLEAU_FUNC_MAP 'WEEK'→'Week' and the DATEPART
 * partMap week→'Week' produced type:error columns. Week-of-year comes from
 * DatePart("week", date). Locks both the standalone WEEK() and DATEPART('week',…)
 * forms, and guards the other date parts that DID resolve live (no regression).
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { tableauFormulaToSigma } from './formulas.js';

const conv = (f: string) => tableauFormulaToSigma(f, []);

describe('WEEK → DatePart("week", …)', () => {
  test('standalone WEEK on a bare date column', () => {
    assert.equal(conv('WEEK([Order Date])'), 'DatePart("week", [Order Date])');
  });
  test('WEEK wrapping a nested date function', () => {
    assert.equal(conv('WEEK(MAKEDATE(2024, 1, 15))'), 'DatePart("week", MakeDate(2024, 1, 15))');
  });
  test('DATEPART(\'week\', …) also routes to DatePart', () => {
    assert.equal(conv("DATEPART('week', [Order Date])"), 'DatePart("week", [Order Date])');
  });
  test('no residual Week( emitted', () => {
    assert.ok(!/\bWeek\s*\(/.test(conv('WEEK([D])')));
    assert.ok(!/\bWeek\s*\(/.test(conv("DATEPART('week', [D])")));
  });
});

describe('other date parts unchanged (no regression)', () => {
  const cases: [string, string][] = [
    ['YEAR([D])', 'Year([D])'],
    ['MONTH([D])', 'Month([D])'],
    ['QUARTER([D])', 'Quarter([D])'],
    ["DATEPART('year', [D])", 'Year([D])'],
    ["DATEPART('month', [D])", 'Month([D])'],
  ];
  for (const [src, want] of cases) {
    test(`${src} → ${want}`, () => assert.equal(conv(src), want));
  }
});
