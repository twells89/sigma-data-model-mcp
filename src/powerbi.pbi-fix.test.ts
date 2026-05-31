/**
 * Regression tests for beads-sigma-j89 + beads-sigma-tkd.
 *
 * j89 — Snowflake M-expression source path auto-extraction:
 *   Power BI's Snowflake connector emits navigation steps tagged with Kind
 *   ({[Name="CSA", Kind="Database"]} -> {...Schema} -> {...Table}). The converter
 *   must derive source.path = [DB, SCHEMA, TABLE] with NO database/schema args.
 *
 * tkd — output directly postable:
 *   (1) every base warehouse-table element carries a `name`,
 *   (2) the model object carries top-level `schemaVersion: 1`.
 *
 * Plus a sampling of MANIFEST.md DAX -> Sigma metric translations.
 *
 * Run: node --import tsx/esm --test src/powerbi.pbi-fix.test.ts
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { convertPowerBIToSigma } from './powerbi.js';

const MODEL_CLEAN = '/tmp/pbix/model_clean.bim';
const FIXTURE_DIR = '/Users/tjwells/sigma-skills-staging/powerbi-to-sigma/fixtures';
const FIXTURES = [
  'fixture_01_mechanical.bim',
  'fixture_02_time_intelligence.bim',
  'fixture_03_filter_context.bim',
  'fixture_04_iterators_rank_var.bim',
  'fixture_05_relationships_hard.bim',
  'fixture_06_kitchen_sink.bim',
];

function load(p: string) { return JSON.parse(readFileSync(p, 'utf8')); }
function baseElements(model: any) {
  return model.pages[0].elements.filter((e: any) => e?.source?.kind === 'warehouse-table');
}
function allMetrics(model: any): Record<string, string> {
  const out: Record<string, string> = {};
  for (const el of model.pages[0].elements)
    for (const m of (el.metrics || [])) out[m.name] = m.formula;
  return out;
}

// (a) Snowflake paths auto-derived (j89) - NO database/schema args
test('j89 (a): model_clean Snowflake paths auto-derived to [CSA, TJ, TABLE]', () => {
  const { model } = convertPowerBIToSigma(load(MODEL_CLEAN)); // NO options
  const bases = baseElements(model);
  assert.ok(bases.length >= 3, `expected >=3 base elements, got ${bases.length}`);
  const byTail: Record<string, string[]> = {};
  for (const el of bases) byTail[el.source.path[el.source.path.length - 1]] = el.source.path;
  for (const tbl of ['EMPLOYEES', 'ABSENCE_RECORDS', 'SAFETY_INCIDENTS']) {
    assert.deepEqual(byTail[tbl], ['CSA', 'TJ', tbl],
      `path for ${tbl} should be ["CSA","TJ","${tbl}"], got ${JSON.stringify(byTail[tbl])}`);
  }
});

test('j89: no "could not extract source path" warning for model_clean', () => {
  const { warnings } = convertPowerBIToSigma(load(MODEL_CLEAN));
  assert.ok(!warnings.some(w => /could not extract source path/.test(w)),
    `unexpected extraction-failure warning:\n${warnings.join('\n')}`);
});

test('j89: caller-supplied database/schema still override the derived path', () => {
  const { model } = convertPowerBIToSigma(load(MODEL_CLEAN), { database: 'PROD', schema: 'HR' });
  for (const el of baseElements(model)) {
    assert.equal(el.source.path[0], 'PROD');
    assert.equal(el.source.path[1], 'HR');
  }
});

// (b) every base element named + (c) schemaVersion present (tkd)
for (const f of [MODEL_CLEAN, ...FIXTURES.map(x => `${FIXTURE_DIR}/${x}`)]) {
  const label = f.split('/').pop();
  test(`tkd (b)+(c) + no-crash: ${label}`, () => {
    const res = convertPowerBIToSigma(load(f)); // NO options
    const { model } = res;
    assert.equal(model.schemaVersion, 1, 'schemaVersion must be 1');
    const bases = baseElements(model);
    assert.ok(bases.length >= 1, 'expected at least one base element');
    for (const el of bases) {
      assert.ok(typeof el.name === 'string' && el.name.length > 0,
        `base element ${el.id} missing name`);
      assert.equal(el.name, el.source.path[el.source.path.length - 1],
        `base element name should equal table (last path segment)`);
    }
    assert.ok(Array.isArray(res.warnings));
    assert.ok(res.stats && typeof res.stats.elements === 'number');
  });
}

// (d) DAX -> Sigma metric sampling from MANIFEST.md
test('d: fixture_01 mechanical DAX measures map per MANIFEST', () => {
  const { model } = convertPowerBIToSigma(load(`${FIXTURE_DIR}/fixture_01_mechanical.bim`));
  const m = allMetrics(model);
  assert.equal(m['Total Salary'], 'Sum([Annual Salary])');
  assert.equal(m['Avg Salary'], 'Avg([Annual Salary])');
  assert.equal(m['Distinct Departments'], 'CountDistinct([Department])');
  assert.equal(m['Total Absence Hours'], 'Sum([Hours])');
  assert.equal(m['Avg Absence Hours'], 'Avg([Hours])');
  assert.equal(m['Incident Count'], 'CountDistinct([Incident Id])');
  assert.equal(m['Headcount'], 'Count()');
  assert.equal(m['Absence Records'], 'Count()');
  assert.equal(m['Pct Active'], '[Active Headcount] / [Headcount]');
});

test('d: structural DAX (time-intel) warns, not crash', () => {
  const { model, warnings } = convertPowerBIToSigma(load(`${FIXTURE_DIR}/fixture_02_time_intelligence.bim`));
  const m = allMetrics(model);
  assert.equal(m['Total Absence Hours'], 'Sum([Hours])');
  assert.ok(!('YTD Absence Hours' in m), 'YTD scalar metric should be skipped');
  assert.ok(warnings.some(w => /time intelligence/i.test(w)),
    'expected a time-intelligence warning');
});

// ── beads-sigma-f0p / 862 / m1a: DAX-gap regression ──────────────────────────
import { pbiDaxToSigma } from './powerbi.js';

// f0p: DATEDIFF(start, end, UNIT) -> DateDiff("unit", start, end)
test('f0p: DATEDIFF -> DateDiff("unit", start, end) — quoted unit FIRST, arg order', () => {
  assert.equal(
    pbiDaxToSigma('DATEDIFF(A, B, DAY)', null, 'x'),
    'DateDiff("day", A, B)');
  assert.equal(
    pbiDaxToSigma('DATEDIFF(EMPLOYEES[HIRE_DATE], TODAY(), YEAR)', null, 'x'),
    'DateDiff("year", [HIRE_DATE], Today())');
  // nested IF in the `end` arg must survive intact
  assert.equal(
    pbiDaxToSigma(
      'DATEDIFF(EMPLOYEES[HIRE_DATE], IF(ISBLANK(EMPLOYEES[TERMINATION_DATE]), TODAY(), EMPLOYEES[TERMINATION_DATE]), DAY)',
      null, 'x'),
    'DateDiff("day", [HIRE_DATE], If(IsNull([TERMINATION_DATE]), Today(), [TERMINATION_DATE]))');
});

test('f0p: fixture_06 "Tenure Days" calc col uses corrected DateDiff form', () => {
  const { model } = convertPowerBIToSigma(load(`${FIXTURE_DIR}/fixture_06_kitchen_sink.bim`));
  let formula: string | undefined;
  for (const el of model.pages[0].elements)
    for (const c of (el.columns || []))
      if (c.name === 'Tenure Days') formula = c.formula;
  assert.equal(formula,
    'DateDiff("day", [Hire Date], If(IsNull([Termination Date]), Today(), [Termination Date]))');
  // No bracketed unit / DAX arg order anywhere.
  assert.ok(!/\[(day|month|year|DAY|MONTH|YEAR)\]/.test(formula!), 'unit must not be bracketed');
});

// 862: CountIf takes ONE logical arg
test('862: CALCULATE(COUNTROWS, pred) -> single-arg CountIf(pred)', () => {
  assert.equal(
    pbiDaxToSigma('CALCULATE(COUNTROWS(EMPLOYEES), EMPLOYEES[STATUS] = "Active")', null, 'Active Headcount'),
    'CountIf([STATUS] = "Active")');
  assert.equal(
    pbiDaxToSigma('CALCULATE(COUNTROWS(EMPLOYEES), FILTER(EMPLOYEES, EMPLOYEES[ANNUAL_SALARY] > 100000))', null, 'High Earner Count'),
    'CountIf([ANNUAL_SALARY] > 100000)');
  // sibling *If aggregates keep their 2-arg (col, pred) signature
  assert.equal(
    pbiDaxToSigma('CALCULATE(SUM(ABSENCE_RECORDS[HOURS]), ABSENCE_RECORDS[APPROVED] = TRUE())', null, 'x'),
    'SumIf([HOURS], [APPROVED] = TRUE())');
});

test('862: fixture_06 "Active Headcount" emits single-arg CountIf (no 2-arg form)', () => {
  const { model } = convertPowerBIToSigma(load(`${FIXTURE_DIR}/fixture_06_kitchen_sink.bim`));
  const m = allMetrics(model);
  assert.equal(m['Active Headcount'], 'CountIf([Status] = "Active")');
  // assert NO metric anywhere uses the illegal 2-arg CountIf([col], [cond]) form
  for (const formula of Object.values(m))
    assert.ok(!/\bCountIf\(\s*\[[^\]]+\]\s*,/.test(formula),
      `2-arg CountIf is illegal in Sigma: ${formula}`);
});

// m1a: cross-table ratio must NOT ship as a silently-null same-element metric
test('m1a: cross-table ratio "Absence Hours Per Head" is NOT emitted as same-element metric', () => {
  const { model, warnings } = convertPowerBIToSigma(load(`${FIXTURE_DIR}/fixture_06_kitchen_sink.bim`));
  const m = allMetrics(model);
  assert.ok(!('Absence Hours Per Head' in m),
    'cross-table ratio must not be shipped as a same-element (null-resolving) metric');
  assert.ok(warnings.some(w => /Absence Hours Per Head/.test(w) && /cross-table/i.test(w)),
    'expected a structured cross-table-ratio warning');
});

test('m1a: same-element ratio "Pct Active" IS kept (both measures on EMPLOYEES)', () => {
  const { model } = convertPowerBIToSigma(load(`${FIXTURE_DIR}/fixture_01_mechanical.bim`));
  const m = allMetrics(model);
  assert.equal(m['Pct Active'], '[Active Headcount] / [Headcount]',
    'a same-element measure-on-measure ratio must still be emitted');
});
