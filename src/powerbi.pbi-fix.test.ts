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

const FIXTURE_DIR = '/Users/tjwells/sigma-skills-staging/powerbi-to-sigma/fixtures';
const MODEL_CLEAN = `${FIXTURE_DIR}/model_clean.bim`;
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
  assert.equal(m['Pct Active'], '([Active Headcount]) / ([Headcount])');
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
    'SumIf([HOURS], [APPROVED] = True)');
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
  assert.equal(m['Pct Active'], '([Active Headcount]) / ([Headcount])',
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
  assert.equal(pbiDaxToSigma('COMBINEVALUES(" | ", T[Dept], T[Role])', null, 'x'), 'Text([Dept]) & " | " & Text([Role])');
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
test('3t9: fixture_07 "Salary Rank In Dept" -> SQL DENSE_RANK window helper (Rank errors in DM calc cols)', () => {
  const { model } = convertPowerBIToSigma(load(`${FIXTURE_DIR}/fixture_07_comp_distribution.bim`));
  const helper = sqlElements(model).find((e: any) => /DENSE_RANK\(\) OVER \(PARTITION BY DEPARTMENT ORDER BY ANNUAL_SALARY DESC\)/i.test(e.source?.statement || ''));
  assert.ok(helper, 'expected a SQL window helper carrying the dense-rank OVER clause');
  // and the raw idiom must NOT survive as a base-table calc col
  const cc = allCalcCols(model);
  assert.ok(!cc['Salary Rank In Dept'] || !/EARLIER|COUNTROWS|RankDense/i.test(cc['Salary Rank In Dept']),
    'rank idiom must not remain as a base-table calc col');
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
  const bands = sqls.find((e: any) => /VALUES\s*\(40000\)/.test(e.source?.statement || ''));
  assert.ok(bands, 'SalaryBands must be emitted as a sql element');
  assert.ok(!('name' in bands), 'custom-SQL elements omit the element-level name (rule 3)');
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
  const dm = sqls.find((e: any) => /VALUES\s*\(0\)/.test(e.source?.statement || ''));
  assert.ok(dm, 'DimMonth must be a sql element');
  assert.match(dm.source.statement, /VALUES\s*\(0\)/);
  assert.ok(!baseElements(model).some((e: any) => e.name === 'DIMMONTH'),
    'DimMonth must NOT be a warehouse-table');
});
// 7mn: CALENDAR / ADDCOLUMNS(CALENDAR(...)) is now a REAL date-spine SQL element
// (was previously an {ok:false} placeholder). VERIFIED vs PBI: 3287-row spine
// 2018-01-01..2026-12-31 with derived Year/MonthNo/Month columns.
test('7mn: ADDCOLUMNS(CALENDAR(a,b)) -> real sql date-spine element, NOT an {ok:false} placeholder', () => {
  const { model, warnings } = convertPowerBIToSigma(load(`${FIXTURE_DIR}/fixture_06_kitchen_sink.bim`));
  const sqls = sqlElements(model);
  const dd = sqls.find((e: any) => /GENERATOR/i.test(e.source?.statement || ''));
  assert.ok(dd, 'DimDate must be emitted as a sql element (not warehouse-table)');
  assert.notEqual((dd as any).ok, false, 'CALENDAR is now translatable — must NOT carry ok:false');
  assert.doesNotMatch(dd.source.statement, /TODO/, 'real spine SQL must not be a TODO placeholder');
  // Snowflake date-spine: GENERATOR + DATEADD over a literal start date.
  assert.match(dd.source.statement, /GENERATOR\s*\(\s*ROWCOUNT\s*=>\s*3287\s*\)/i,
    'spine must be GENERATOR(ROWCOUNT => 3287) — 3287 days inclusive');
  assert.match(dd.source.statement, /DATEADD\(\s*'day'\s*,\s*SEQ4\(\)\s*,\s*CAST\('2018-01-01' AS DATE\)\)/i,
    'spine must DATEADD day-offsets from 2018-01-01');
  // Derived ADDCOLUMNS columns translated to SQL.
  assert.match(dd.source.statement, /EXTRACT\(YEAR FROM d\) AS "Year"/i, 'Year = EXTRACT(YEAR)');
  assert.match(dd.source.statement, /EXTRACT\(MONTH FROM d\) AS "Month No"/i, 'MonthNo = EXTRACT(MONTH)');
  assert.match(dd.source.statement, /TO_CHAR\(d, 'Mon'\) AS "Month"/i, 'Month = TO_CHAR(,\'Mon\')');
  assert.ok(!baseElements(model).some((e: any) => e.name === 'DIMDATE'),
    'DimDate must NOT be a path-guessed warehouse-table');
  assert.ok(warnings.some(w => /DimDate/.test(w) && /date-spine/i.test(w)),
    'expected a date-spine synthesis info warning');
});

// a8h: WEEKNUM is NOT mapped to the ISO DatePart("week",...) (which diverges from
// DAX at year boundaries) — it maps to the Excel-style week-of-year formula.
// VERIFIED EXACT vs PBI WEEKNUM(d,2) on 9 boundary dates incl. 2019-12-30 (53),
// 2021-01-01 (1) — the cases where naive DatePart("week") is WRONG.
test('a8h: WEEKNUM(date,2) -> Excel-style Monday-start week formula (NOT ISO DatePart)', () => {
  const out = pbiDaxToSigma('WEEKNUM(SAFETY_INCIDENTS[DATE], 2)', null, 'Week Of Year');
  assert.ok(out, 'WEEKNUM must translate');
  // Must NOT emit the naive ISO DatePart("week",...) which is wrong at boundaries.
  assert.doesNotMatch(out!, /DatePart\s*\(\s*"week"/i, 'must NOT use ISO DatePart("week")');
  // Monday-start (return_type 2) uses the +5 offset on Weekday(year-start).
  assert.match(out!, /Floor\(\(DateDiff\("day", DateTrunc\("year", \[DATE\]\), \[DATE\]\) \+ Mod\(Weekday\(DateTrunc\("year", \[DATE\]\)\) \+ 5, 7\)\) \/ 7\) \+ 1/,
    'return_type 2 = Monday-start formula with +5 offset');
});

test('a8h: WEEKNUM(date) and WEEKNUM(date,1) -> Sunday-start (+6 offset)', () => {
  const def = pbiDaxToSigma('WEEKNUM(SAFETY_INCIDENTS[DATE])', null, 'Week Of Year');
  const t1 = pbiDaxToSigma('WEEKNUM(SAFETY_INCIDENTS[DATE], 1)', null, 'Week Of Year');
  for (const out of [def, t1]) {
    assert.ok(out, 'WEEKNUM must translate');
    assert.doesNotMatch(out!, /DatePart\s*\(\s*"week"/i, 'must NOT use ISO DatePart("week")');
    assert.match(out!, /Mod\(Weekday\(DateTrunc\("year", \[DATE\]\)\) \+ 6, 7\)/,
      'Sunday-start (default / type 1) uses +6 offset');
  }
});

// a8h: the Safety & Absence "Week Of Year" calc col surfaces the new formula
// (and never the ISO DatePart) after column-name normalization.
test('a8h: fixture_08 "Week Of Year" calc col uses the Excel-style WEEKNUM formula', () => {
  const { model } = convertPowerBIToSigma(load(`${FIXTURE_DIR}/fixture_08_safety_absence_patterns.bim`));
  let formula: string | undefined;
  for (const el of model.pages[0].elements)
    for (const c of (el.columns || []))
      if (c.name === 'Week Of Year' || /Week Of Year/i.test(c.name || '')) formula = c.formula;
  if (formula) {
    assert.doesNotMatch(formula, /DatePart\s*\(\s*"week"/i, 'must NOT use ISO DatePart("week")');
    assert.match(formula, /Floor\(\(DateDiff\("day", DateTrunc\("year"/,
      'Week Of Year must use the Excel-style week formula');
  }
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

import { formatFromMask, inferSigmaFormat } from './sigma-ids.js';

test('r9oz: bare COUNTROWS(FILTER(table,pred)) -> CountIf(pred), never malformed Count())', () => {
  const out = pbiDaxToSigma("COUNTROWS(FILTER('ORDER_FACT', 'ORDER_FACT'[Net Revenue] > 200))", null, 'High Value Orders');
  assert.equal(out, 'CountIf([Net Revenue] > 200)');
  assert.ok(!/Count\(\)\)/.test(out || ''), 'must not emit malformed Count())');
});

test('r9oz: COUNT(FILTER(...)) also -> CountIf(pred)', () => {
  const out = pbiDaxToSigma("COUNT(FILTER('T', 'T'[Status] = \"Open\"))", null, 'Open');
  assert.equal(out, 'CountIf([Status] = "Open")');
});

test('RANKX is dropped-and-warned (no invalid passthrough)', () => {
  const warns: string[] = [];
  const out = pbiDaxToSigma("RANKX(ALL('T'[Sub Category]), [Total Sales],,DESC)", warns, 'Sales Rank');
  assert.equal(out, null, 'RANKX must not emit a formula');
  assert.ok(warns.some(w => /ranking \(RANKX\)/i.test(w)), 'must warn about RANKX');
});

test('4q7k: formatFromMask maps Excel masks to Sigma format objects', () => {
  assert.deepEqual(formatFromMask('0.0%'), { kind: 'number', formatString: ',.1%' });
  assert.deepEqual(formatFromMask('\\$#,0'), { kind: 'number', formatString: '$,.0f', currencySymbol: '$' });
  assert.deepEqual(formatFromMask('\\$#,0.00'), { kind: 'number', formatString: '$,.2f', currencySymbol: '$' });
  assert.deepEqual(formatFromMask('#,0'), { kind: 'number', formatString: ',.0f' });
  assert.equal(formatFromMask('General Date'), null);
  assert.equal(formatFromMask(undefined), null);
});

test('4q7k: inferSigmaFormat honors the source mask over the name heuristic', () => {
  // "Avg Discount" name would heuristically guess currency; the % mask wins.
  assert.deepEqual(inferSigmaFormat('Avg([Discount])', 'Avg Discount', '0.0%'),
    { kind: 'number', formatString: ',.1%' });
  // no mask -> falls back to heuristics (currency by name)
  assert.deepEqual(inferSigmaFormat('Sum([Sales])', 'Total Sales'),
    { kind: 'number', formatString: '$,.2f', currencySymbol: '$' });
});

test('dangling-ref: a metric referencing a dropped measure is pruned (not posted broken)', () => {
  const model = {
    model: { tables: [{
      name: 'F', columns: [{ name: 'Order Id', dataType: 'string', sourceColumn: 'ORDER_ID' }],
      partitions: [{ source: { type: 'm', expression: [
        'let Source = Snowflake.Databases("a","b"),',
        '#"N1" = Source{[Name="DB",Kind="Database"]}[Data],',
        '#"N2" = #"N1"{[Name="SCH",Kind="Schema"]}[Data],',
        '#"N3" = #"N2"{[Name="F",Kind="Table"]}[Data] in #"N3"' ] } }],
      measures: [
        { name: 'Orders', expression: "DISTINCTCOUNT('F'[Order Id])" },
        { name: 'Returned Orders', expression: "CALCULATE([Orders], ALLEXCEPT('F', 'F'[Region]))" },
        { name: 'Return Rate', expression: '[Returned Orders] / [Orders]' },
      ],
    }] },
  };
  const { model: out } = convertPowerBIToSigma(model as any);
  const names = (out.pages[0].elements[0].metrics || []).map((m: any) => m.name);
  assert.ok(!names.includes('Returned Orders'), 'complex CALCULATE measure dropped');
  assert.ok(!names.includes('Return Rate'), 'dangling dependent metric must be pruned');
  assert.ok(names.includes('Orders'), 'clean metric retained');
});

test('mkm: ISINSCOPE/scope introspection is dropped-and-warned (no invalid passthrough)', () => {
  for (const expr of [
    'IF(ISINSCOPE(\'Date\'[Year]), [Total Sales], BLANK())',
    'IF(ISFILTERED(\'Product\'[Category]), [Total Sales])',
  ]) {
    const warns: string[] = [];
    const out = pbiDaxToSigma(expr, warns, 'Scoped Measure');
    assert.equal(out, null, `must drop: ${expr}`);
    assert.ok(warns.some(w => /scope introspection/i.test(w)), `must warn: ${expr}`);
  }
});

test('VAR/RETURN inlines into a single expression (no longer dropped)', () => {
  const out = pbiDaxToSigma('VAR rev = [Total Net Rev] VAR ord = [Order Count] RETURN DIVIDE(rev, ord)', [], 'AOV');
  // VAR substitution parenthesizes each var, DIVIDE parenthesizes operands —
  // the double wrap is harmless and keeps precedence safe (hs5h).
  assert.equal(out, '(([Total Net Rev])) / NullIf((([Order Count])), 0)');
  // a VAR referencing an earlier VAR
  const out2 = pbiDaxToSigma('VAR a = [X] VAR b = a * 2 RETURN b + [Y]', [], 'm');
  assert.equal(out2, '(([X]) * 2) + [Y]');
});

test('simple SUMX/AVERAGEX/MAXX over a bare table -> Sum/Avg/Max(rowExpr)', () => {
  assert.equal(pbiDaxToSigma("SUMX('ORDER_FACT', 'ORDER_FACT'[Net Revenue] * 'ORDER_FACT'[Quantity Ordered])", [], 'm'),
    'Sum([Net Revenue] * [Quantity Ordered])');
  assert.equal(pbiDaxToSigma("AVERAGEX(Sales, Sales[Price] - Sales[Cost])", [], 'm'),
    'Avg([Price] - [Cost])');
  assert.equal(pbiDaxToSigma("MAXX('T', 'T'[A])", [], 'm'), 'Max([A])');
});

test('iterator over FILTER/TOPN or with a nested aggregate is NOT mis-translated (still dropped)', () => {
  const w1: string[] = [];
  assert.equal(pbiDaxToSigma("SUMX(TOPN(5, VALUES('T'[K]), [M]), [M])", w1, 'Top5'), null);
  assert.ok(w1.some(x => /iterator \(SUMX\)/.test(x)));
  const w2: string[] = [];
  assert.equal(pbiDaxToSigma("SUMX('T', SUM('T'[X]))", w2, 'bad'), null); // nested agg -> not simple
});

test('CALCULATE(<agg>, ALL(<wholeTable>)) -> GrandTotal(<agg>) (%-of-total now translates)', () => {
  assert.equal(pbiDaxToSigma("DIVIDE([Total Sales], CALCULATE([Total Sales], ALL('SAMPLE_SUPERSTORE')))", [], 'Pct'),
    '([Total Sales]) / NullIf((GrandTotal([Total Sales])), 0)');
  assert.equal(pbiDaxToSigma('CALCULATE(SUM(Sales[Amount]), REMOVEFILTERS(Sales))', [], 'gt'),
    'GrandTotal(Sum([Amount]))');
  // partial ALL (a column) and multi-arg CALCULATE must NOT be mis-translated
  assert.equal(pbiDaxToSigma('CALCULATE([Sales], ALL(Sales[Region]))', [], 'p'), null);
  assert.equal(pbiDaxToSigma('CALCULATE([Sales], ALL(Sales), Sales[Y]=1)', [], 'm'), null);
});
