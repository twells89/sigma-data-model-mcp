/**
 * Cognos converter tests — mirrors the verified plugin converter test suite
 * (sigma-migration-skills/plugins/cognos-to-sigma/.../converter/test.ts).
 *
 * Sections:
 *   sigmaDisplayName — must match Sigma's OWN display-name derivation, incl.
 *     letter↔digit splits in BOTH directions + idempotency (beads-sigma-c31q)
 *   go-sales report  — macro→Switch wired by controlId, segmented controls,
 *     KPI singletons, element filters, groupings, categorical year bind
 *   data module      — smoke + detect-only security reporting (SecurityRule)
 */

import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { convertCognosToSigma } from './cognos.js';
import { convertCognosReportToSigma } from './cognos-report.js';
import { sigmaDisplayName } from './sigma-ids.js';

const FIX = join(dirname(fileURLToPath(import.meta.url)), '..', 'test-fixtures');

// ── sigmaDisplayName must match Sigma's OWN derivation (incl. letter↔digit splits;
// verified against live DM readbacks 2026-06-10 — beads-sigma-c31q) ──────────────
describe('sigmaDisplayName: Sigma-exact derivation', () => {
  const NAME_CASES: Array<[string, string]> = [
    ['CY_Q1_REVENUE', 'Cy Q 1 Revenue'],   // the 16-dep-not-found case: Q1 splits to "Q 1"
    ['PY_Q4', 'Py Q 4'],
    ['FY2024', 'Fy 2024'],                  // letters→digits boundary, multi-digit group
    ['REVENUE_FY2024', 'Revenue Fy 2024'],
    ['X2024FY', 'X 2024 Fy'],               // digits→letters boundary
    ['Sheet1_1', 'Sheet 1 1'],
    ['GROSS_PROFIT', 'Gross Profit'],
    ['Province_or_State', 'Province or State'],  // small words stay lowercase (not first)
    ['Month_number', 'Month Number'],
    ['_row_id', 'Row Id'],
  ];
  for (const [input, expected] of NAME_CASES) {
    test(`${JSON.stringify(input)} → ${JSON.stringify(expected)}`, () => {
      assert.equal(sigmaDisplayName(input), expected);
    });
  }

  test('idempotent: sigmaDisplayName(sigmaDisplayName(x)) === sigmaDisplayName(x)', () => {
    // Formulas pass through the expression translator more than once, and
    // same-element sibling refs are case-SENSITIVE in Sigma — a non-idempotent
    // derivation ("Cyq Rev" → "Cyq rev") breaks the ref (type "error" column).
    for (const s of ['CY_Q1_REVENUE', 'Cyq Rev', 'Gross Profit', 'Fy 2024', 'OrderDate']) {
      const once = sigmaDisplayName(s);
      assert.equal(sigmaDisplayName(once), once, `not idempotent for ${JSON.stringify(s)}`);
    }
  });
});

// ── go-sales-performance regression: macro→Switch wired by controlId, segmented
// control with values+default, KPI singletons, element filters ───────────────────
describe('go-sales-performance report conversion', () => {
  const r = convertCognosReportToSigma(
    readFileSync(join(FIX, 'go-sales-performance.report.xml'), 'utf8'), { dataModelId: 'dm' });
  const els = r.workbook.pages[0].elements as any[];
  const controls = (r.workbook.controls || []) as any[];
  const ctl = controls.find((c) => c.controlId === 'pColumn');

  test('pColumn control is segmented', () => assert.equal(ctl?.controlType, 'segmented'));
  test('pColumn has explicit values', () =>
    assert.deepEqual(ctl?.source?.values, ['Revenue', 'Gross Profit']));
  test('pColumn defaults to Revenue', () => assert.equal(ctl?.value, 'Revenue'));
  test('pQuarter control registered with Q1-Q4 + default Q4', () =>
    assert.ok(controls.some((c) => c.controlId === 'pQuarter' && c.value === 'Q4' && c.source?.values?.length === 4)));
  test('Switch wired by controlId [pColumn]', () =>
    assert.ok(els.some((e) => e.columns?.some((c: any) =>
      /Switch\(\[pColumn\], "Gross Profit", \[Sheet 1\/Gross Profit\], \[Sheet 1\/Revenue\]\)/.test(c.formula)))));
  test('6 KPI singletons converted', () =>
    assert.equal(els.filter((e) => e.kind === 'kpi-chart').length, 6));
  test('KPI value uses columnId', () =>
    assert.ok(els.filter((e) => e.kind === 'kpi-chart').every((e) => e.value?.columnId)));
  test('KPI macro → Switch over digit-split refs', () =>
    assert.ok(els.some((e) => e.kind === 'kpi-chart' &&
      e.columns?.some((c: any) => c.formula.includes('[Sheet 1 1/Cy Q 1 Revenue]')))));
  test('detail filters became element filters', () => assert.ok(r.stats.filters >= 4));
  test('?pQuarter? filter is a boolean match column', () =>
    assert.ok(els.some((e) => e.columns?.some((c: any) => c.formula === '[Quarter Label] = [pQuarter]'))));
  test('lists grouped', () =>
    assert.ok(els.some((e) => e.kind === 'table' && e.groupings?.length)));
  test('year bound categorically on the line chart', () =>
    assert.ok(els.some((e) => e.kind === 'line-chart' &&
      e.columns?.some((c: any) => /^Text\(/.test(c.formula) && c.name === 'Year'))));
  test('no unresolved Switch placeholders', () =>
    assert.ok(!els.some((e) => e.columns?.some((c: any) => /map prompt tokens/.test(c.formula)))));
});

// ── data-module conversion smoke ─────────────────────────────────────────────────
describe('data-module conversion', () => {
  test('sample-data-module converts with elements/columns/metrics', () => {
    const r = convertCognosToSigma(
      readFileSync(join(FIX, 'sample-data-module.module.json'), 'utf8'),
      { connectionId: 'c', database: 'DB', schema: 'S' });
    assert.ok(r.model.pages[0].elements.length > 0, 'no elements');
    assert.ok(r.stats.columns > 0 && r.stats.metrics > 0);
    assert.equal(r.model.schemaVersion, 1);
  });

  test('securityFilter is detected and REPORTED (never injected)', () => {
    const mod = {
      name: 'Secured Module',
      querySubject: [{
        identifier: 'SALES_FACT',
        item: [
          { queryItem: { identifier: 'REGION', usage: 'attribute' } },
          { queryItem: { identifier: 'REVENUE', usage: 'fact', regularAggregate: 'total' } },
        ],
        securityFilter: [{
          identifier: 'regional_rls',
          expression: "[SALES_FACT].[REGION] = #CSVIdentityNameList(',')#",
          securityObject: [{ searchPath: 'CAMID(":Sales Managers")' }],
        }],
      }],
    };
    const r = convertCognosToSigma(mod, { connectionId: 'c', database: 'DB', schema: 'S' });
    assert.equal(r.security?.length, 1);
    const rule = r.security![0];
    assert.equal(rule.kind, 'rls');
    assert.match(rule.source, /Cognos data-module security filter/);
    assert.equal(rule.elementName, 'Sales Fact');
    assert.match(rule.sourceExpression || '', /REGION/);
    assert.deepEqual(rule.groups, ['CAMID(":Sales Managers")']);
    // detect-only: the model spec itself must carry NO security filter artifacts
    assert.ok(!JSON.stringify(r.model).includes('CSVIdentityNameList'));
    assert.ok(r.warnings.some((w) => /SECURITY: 1 data-module security filter/.test(w)));
  });
});
