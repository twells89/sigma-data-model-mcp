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
  'fixture_07_comp_distribution.bim',
  'fixture_08_safety_absence_patterns.bim',
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


// ── beads-sigma-9l2 / 3t9 / n9u / w9s: DAX-translation regression ──────────────
function allCalcCols(model: any): Record<string, string> {
  const out: Record<string, string> = {};
  for (const el of model.pages[0].elements)
    for (const c of (el.columns || []))
      if (c.name) out[c.name] = c.formula;
  return out;
}
function sqlElements(model: any) {
  return model.pages[0].elements.filter((e: any) => e?.source?.kind === 'sql');
}
// Raw-DAX tokens that MUST NOT survive into any emitted column/metric formula
// (they would create type=error columns in Sigma). (beads-sigma-9l2)
const RAW_DAX_BANNED =
  /\b(MEDIANX|PERCENTILEX\.INC|PERCENTILEX\.EXC|STDEVX\.P|STDEVX\.S|VARX\.P|VARX\.S|GEOMEANX|DISTINCTCOUNTNOBLANK|COMBINEVALUES|EARLIER|HASONEVALUE|SELECTEDVALUE)\b|SWITCH\s*\(\s*TRUE/i;

// 9l2 (a): stat funcs use the CORRECT Sigma names (unit-level)
test('9l2 (a): stat-iterator DAX maps to correct Sigma function names', () => {
  assert.equal(pbiDaxToSigma('MEDIANX(T, T[Sal])', null, 'x'), 'Median([Sal])');
  assert.equal(pbiDaxToSigma('PERCENTILEX.INC(T, T[Sal], 0.9)', null, 'x'), 'PercentileCont([Sal], 0.9)');
  assert.equal(pbiDaxToSigma('STDEVX.P(T, T[Sal])', null, 'x'), 'Sqrt(VariancePop([Sal]))');
  assert.equal(pbiDaxToSigma('VARX.P(T, T[Sal])', null, 'x'), 'VariancePop([Sal])');
  assert.equal(pbiDaxToSigma('GEOMEANX(T, T[Sal])', null, 'x'), 'Exp(Avg(Ln([Sal])))');
  assert.equal(pbiDaxToSigma('DISTINCTCOUNTNOBLANK(T[Role])', null, 'x'), 'CountDistinct([Role])');
  assert.equal(pbiDaxToSigma('COMBINEVALUES(" | ", T[Dept], T[Role])', null, 'x'), '[Dept] & " | " & [Role]');
  assert.equal(
    pbiDaxToSigma('IF(HASONEVALUE(T[Dept]), SELECTEDVALUE(T[Dept]), "All")', null, 'x'),
    'If(CountDistinct([Dept]) = 1, Min([Dept]), "All")');
});

// 9l2 (a)+(b): fixture_07 comp-distribution measures translated, no raw DAX
test('9l2: fixture_07 stat measures use correct Sigma names; no raw-DAX leftovers', () => {
  const { model } = convertPowerBIToSigma(load(`${FIXTURE_DIR}/fixture_07_comp_distribution.bim`));
  const m = allMetrics(model);
  assert.equal(m['Median Salary'], 'Median([Annual Salary])');
  assert.equal(m['P90 Salary'], 'PercentileCont([Annual Salary], 0.9)');
  assert.equal(m['P10 Salary'], 'PercentileCont([Annual Salary], 0.1)');
  assert.equal(m['Salary StdDev'], 'Sqrt(VariancePop([Annual Salary]))');
  assert.equal(m['Salary Variance'], 'VariancePop([Annual Salary])');
  assert.equal(m['Salary GeoMean'], 'Exp(Avg(Ln([Annual Salary])))');
  assert.equal(m['Distinct Roles'], 'CountDistinct([Role])');
  assert.equal(m['Selected Dept Label'], 'If(CountDistinct([Department]) = 1, Min([Department]), "All Departments")');
  // (b) none of the banned raw-DAX tokens survive in any column/metric formula
  const cc = allCalcCols(model);
  for (const [name, formula] of [...Object.entries(m), ...Object.entries(cc)])
    assert.ok(!RAW_DAX_BANNED.test(formula), `raw DAX leaked in "${name}": ${formula}`);
});

// 9l2: the WRONG names from the original bead text must NOT appear
test('9l2: must NOT emit PercentileInc / StdDevP / VarianceP (nonexistent in Sigma)', () => {
  const { model } = convertPowerBIToSigma(load(`${FIXTURE_DIR}/fixture_07_comp_distribution.bim`));
  for (const formula of Object.values(allMetrics(model))) {
    assert.ok(!/\bPercentileInc\b/.test(formula), `PercentileInc is wrong; use PercentileCont: ${formula}`);
    assert.ok(!/\bStdDevP\b/.test(formula), `StdDevP does not exist in Sigma: ${formula}`);
    assert.ok(!/\bVarianceP\b/.test(formula), `VarianceP is wrong; use VariancePop: ${formula}`);
  }
});

// 3t9: EARLIER-rank idiom -> RankDense (unit + fixture)
test('3t9: COUNTROWS(FILTER(ALL,..EARLIER..))+1 -> RankDense', () => {
  assert.equal(
    pbiDaxToSigma('COUNTROWS(FILTER(ALL(T), T[Sal] > EARLIER(T[Sal]))) + 1', null, 'Rank'),
    'RankDense([Sal], "desc")');
  // partitioned form
  assert.equal(
    pbiDaxToSigma('COUNTROWS(FILTER(T, T[Dept] = EARLIER(T[Dept]) && T[Sal] > EARLIER(T[Sal]))) + 1', null, 'Rank'),
    'RankDense([Sal], "desc", [Dept])');
});
test('3t9: fixture_07 "Salary Rank In Dept" calc col -> RankDense partitioned', () => {
  const { model } = convertPowerBIToSigma(load(`${FIXTURE_DIR}/fixture_07_comp_distribution.bim`));
  const cc = allCalcCols(model);
  assert.equal(cc['Salary Rank In Dept'], 'RankDense([Annual Salary], "desc", [Department])');
  assert.ok(!/EARLIER|COUNTROWS|FILTER/i.test(cc['Salary Rank In Dept']), 'no raw rank-idiom DAX');
});

// n9u: SWITCH(TRUE(), ...) -> nested If (not flat)
test('n9u: SWITCH(TRUE(), c1,v1,c2,v2,def) -> nested If, not flat', () => {
  assert.equal(
    pbiDaxToSigma('SWITCH(TRUE(), [S] >= 90, "A", [S] >= 80, "B", "F")', null, 'x'),
    'If([S] >= 90, "A", If([S] >= 80, "B", "F"))');
  // no-default form -> innermost else is null
  assert.equal(
    pbiDaxToSigma('SWITCH(TRUE(), [S] > 0, "pos")', null, 'x'),
    'If([S] > 0, "pos", null)');
  // value-form SWITCH (non-TRUE) still maps to Sigma Switch, untouched arity
  assert.equal(
    pbiDaxToSigma('SWITCH(T[Sev], "High", 3, "Low", 1, 0)', null, 'x'),
    'Switch([Sev], "High", 3, "Low", 1, 0)');
});

// w9s: calculated (DAX) tables -> sql element, never a path-guessed warehouse-table
test('w9s: GENERATESERIES calc table -> sql element with VALUES, not warehouse-table', () => {
  const { model } = convertPowerBIToSigma(load(`${FIXTURE_DIR}/fixture_07_comp_distribution.bim`));
  const sqls = sqlElements(model);
  const bands = sqls.find((e: any) => e.name === 'SALARYBANDS');
  assert.ok(bands, 'SalaryBands must be emitted as a sql element');
  assert.match(bands.source.statement, /VALUES\s*\(40000\)/, 'series should start at 40000');
  assert.match(bands.source.statement, /\(200000\)/, 'series should include 200000');
  // It must NOT be a warehouse-table with a guessed path.
  const bases = baseElements(model);
  assert.ok(!bases.some((e: any) => e.name === 'SALARYBANDS'),
    'SalaryBands must NOT be a warehouse-table (would 404)');
});
test('w9s: fixture_08 DimMonth GENERATESERIES -> sql element', () => {
  const { model } = convertPowerBIToSigma(load(`${FIXTURE_DIR}/fixture_08_safety_absence_patterns.bim`));
  const sqls = sqlElements(model);
  const dm = sqls.find((e: any) => e.name === 'DIMMONTH');
  assert.ok(dm, 'DimMonth must be a sql element');
  assert.match(dm.source.statement, /VALUES\s*\(0\)/);
  assert.ok(!baseElements(model).some((e: any) => e.name === 'DIMMONTH'),
    'DimMonth must NOT be a warehouse-table');
});
test('w9s: non-GENERATESERIES calc table (CALENDAR) -> sql {ok:false} placeholder, not warehouse-table', () => {
  const { model, warnings } = convertPowerBIToSigma(load(`${FIXTURE_DIR}/fixture_06_kitchen_sink.bim`));
  const sqls = sqlElements(model);
  const dd = sqls.find((e: any) => e.name === 'DIMDATE');
  assert.ok(dd, 'DimDate must be emitted as a sql element (not warehouse-table)');
  assert.equal((dd as any).ok, false, 'non-translatable calc table should carry ok:false');
  assert.match(dd.source.statement, /TODO/, 'placeholder SQL should flag manual work');
  assert.ok(!baseElements(model).some((e: any) => e.name === 'DIMDATE'),
    'DimDate must NOT be a path-guessed warehouse-table');
  assert.ok(warnings.some(w => /DimDate/.test(w) && /calculated table/i.test(w)),
    'expected a structured calculated-table refusal warning');
});

// 9l2: structured refusal — every dropped measure leaves at least a warning
test('9l2: no measure is silently dropped (each non-converted measure warns)', () => {
  const { model, warnings } = convertPowerBIToSigma(load(`${FIXTURE_DIR}/fixture_07_comp_distribution.bim`));
  const emitted = new Set(Object.keys(allMetrics(model)));
  // These fixture_07 measures legitimately cannot become scalar Sigma metrics;
  // each MUST surface a warning rather than vanish.
  for (const name of ['Roles In Dept', 'Top 5 Role Salary', 'Pct In Selected Bands', 'Mgmt Headcount']) {
    if (emitted.has(name)) continue;
    assert.ok(warnings.some(w => w.includes(name)),
      `dropped measure "${name}" must produce a warning, not vanish`);
  }
});
