/**
 * Regression tests for the 2026-06-08 fix batch:
 *   hs5h — DIVIDE operand parenthesization: DIVIDE(a - b, c) must emit (a-b)/(c),
 *          not "a - b / c" (which parses as a - (b/c)).
 *   qx16 — CALCULATE([measureRef], KEEPFILTERS(<pred>)) conditional aggregate:
 *          resolve the measure ref to its DAX + strip KEEPFILTERS so it becomes a
 *          Sigma conditional aggregate instead of being dropped.
 *   jzd8 — a window-function calc COLUMN must never be emitted on a base
 *          warehouse-table element (Rank/RankDense error there).
 *
 * Run: node --import tsx/esm --test src/powerbi.fix-3.test.ts
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync } from 'node:fs';
import { pbiDaxToSigma, convertPowerBIToSigma, hasBareWindowFn } from './powerbi.js';

const COMP_BIM = '/tmp/pbi-migrate/workforce-comp-distribution-untested-dax/model/model.bim';

test('hs5h: DIVIDE(a - b, c) parenthesizes both operands (with divide-by-zero guard)', () => {
  const out = pbiDaxToSigma('DIVIDE([Dept Med] - [Co Med], [Co Med])', [], 'X');
  assert.ok(out, 'should translate');
  // numerator subtraction must be wrapped, denominator guarded: "(... - ...) / NullIf((...), 0)"
  assert.match(out!, /\)\s*\/\s*NullIf\(/, `expected (num) / NullIf((den), 0), got: ${out}`);
  // and NOT the broken "x - y / z" form
  assert.doesNotMatch(out!, /\]\s*-\s*\[[^\]]+\]\s*\/\s/, `unparenthesized subtraction leaked: ${out}`);
});

test('qx16: CALCULATE([measureRef], KEEPFILTERS(SEARCH..>0)) → CountDistinct(If(...)) (dax-fidelity #1)', () => {
  const dax = 'COALESCE(CALCULATE([Headcount], KEEPFILTERS(SEARCH("Manager", EMPLOYEES[ROLE], 1, 0) > 0)), 0)';
  const out = pbiDaxToSigma(dax, [], 'Mgmt Headcount', { Headcount: 'DISTINCTCOUNT(EMPLOYEES[EMPLOYEE_ID])' });
  assert.ok(out, `should translate, got null`);
  assert.match(out!, /CountDistinct\(If\(/, `expected CountDistinct(If(...)), got: ${out}`);
  assert.doesNotMatch(out!, /CountDistinctIf\(/, `must not emit the two-arg CountDistinctIf form: ${out}`);
  assert.match(out!, /Find\(\[ROLE\]/, `expected SEARCH→Find on [ROLE], got: ${out}`);
  assert.doesNotMatch(out!, /KEEPFILTERS/i, `KEEPFILTERS not stripped: ${out}`);
});

test('qx16 guard: a non-simple measure ref (SUM(a)-SUM(b)) is NOT inlined into a broken formula', () => {
  // CALCULATE([Net], <pred>) where [Net] is a multi-aggregate measure must NOT be
  // inlined (greedy aggM would mis-split it). It drops to a warning instead.
  const out = pbiDaxToSigma('CALCULATE([Net], [Region] = "West")', [], 'X',
    { Net: 'SUM(Sales[Amt]) - SUM(Sales[Cost])' });
  // either dropped (null) or, if it ever translates, must not be a broken SumIf split
  if (out !== null) {
    assert.doesNotMatch(out, /\)\s*-\s*(SUM|Sum)\(/, `broken multi-aggregate split leaked: ${out}`);
  }
});

test('jzd8 false-positive: a string literal containing "Rank (" is not dropped', () => {
  // The window-fn guard must ignore matches inside string literals.
  assert.equal(hasBareWindowFn('If([Score] > 90, "Top Rank (1)", "Other")'), false);
  assert.equal(hasBareWindowFn('RankDense([Salary], "desc", [Dept])'), true);
});

test('jzd8: window calc column never lands as a base-table column formula', { skip: !existsSync(COMP_BIM) && 'fixture not on this machine' }, () => {
  const model = JSON.parse(readFileSync(COMP_BIM, 'utf8'));
  const { model: dm, warnings } = convertPowerBIToSigma(model, {
    connectionId: 'c', database: 'CSA', schema: 'TJ',
  }) as any;
  const els = dm.pages?.[0]?.elements || [];
  for (const el of els) {
    if (el?.source?.kind !== 'warehouse-table') continue;
    for (const c of (el.columns || [])) {
      assert.doesNotMatch(String(c.formula || ''), /\b(Rank|RankDense|Lag|Lead)\s*\(/,
        `base element "${el.name}" col "${c.name}" carries a window formula: ${c.formula}`);
    }
  }
  // the Salary Rank In Dept column should be accounted for: either lowered to a
  // sql helper element OR explicitly dropped with a warning.
  const hasHelper = els.some((e: any) => e?.source?.kind === 'sql');
  const warned = (warnings || []).some((w: string) => /Salary Rank In Dept/.test(w));
  assert.ok(hasHelper || warned, 'expected a sql helper element or a drop warning for the rank column');
});

test('qx16 end-to-end: Comp model emits a Mgmt Headcount metric (not dropped)', { skip: !existsSync(COMP_BIM) && 'fixture not on this machine' }, () => {
  const model = JSON.parse(readFileSync(COMP_BIM, 'utf8'));
  const { model: dm } = convertPowerBIToSigma(model, {
    connectionId: 'c', database: 'CSA', schema: 'TJ',
  }) as any;
  const els = dm.pages?.[0]?.elements || [];
  const allMetrics = els.flatMap((e: any) => e.metrics || []);
  const mgmt = allMetrics.find((m: any) => m.name === 'Mgmt Headcount');
  assert.ok(mgmt, 'Mgmt Headcount metric should be emitted (was dropped before qx16)');
  // Headcount = COUNTROWS(EMPLOYEES) → CountIf(pred); a DISTINCTCOUNT base → CountDistinct(If(...)).
  assert.match(String(mgmt.formula), /CountDistinct\(If\(|CountIf\(/, `got: ${mgmt && mgmt.formula}`);
  assert.match(String(mgmt.formula), /Find\(\[Role\]/, `expected SEARCH→Find on [Role], got: ${mgmt.formula}`);
});

test('hs5h end-to-end: Salary vs Company Median is parenthesized when present', { skip: !existsSync(COMP_BIM) && 'fixture not on this machine' }, () => {
  const model = JSON.parse(readFileSync(COMP_BIM, 'utf8'));
  const { model: dm } = convertPowerBIToSigma(model, {
    connectionId: 'c', database: 'CSA', schema: 'TJ',
  }) as any;
  const els = dm.pages?.[0]?.elements || [];
  const allMetrics = els.flatMap((e: any) => e.metrics || []);
  const svc = allMetrics.find((m: any) => m.name === 'Salary vs Company Median');
  if (svc) assert.match(String(svc.formula), /\)\s*\/\s*\(/, `got: ${svc.formula}`);
});
