/**
 * Regression tests for beads-sigma-fah8 — the "hard DAX" gap workarounds:
 *
 *   Family 1 — USERELATIONSHIP(col1, col2) inside CALCULATE:
 *     The TMSL model knows its INACTIVE relationships. A measure that
 *     CALCULATEs over one activates it as a distinctly-named alternate join
 *     path (REL = TOTABLE_VIA_FROMCOL); the aggregate emits unchanged and the
 *     derived "<T> View" surfaces the alternate-keyed columns. Derived views
 *     must SKIP the relationship's own join-key passthrough (type=error
 *     otherwise — feedback_sigma_derived_view_skip_join_key).
 *
 *   Family 2 — bare EARLIER idioms beyond rank:
 *     running total / group share-total / peer count lower onto kind:'sql'
 *     window helper elements (window fns silently error in DM calc columns).
 *     Anything else: flag-not-drop with the DAX preserved in the warning.
 *
 *   Family 3 — complex FILTER inside CALCULATE:
 *     boolean AND/OR row predicates → SumIf/CountIf/…; IN {…} → or-chain
 *     (Sigma has no IsIn); FILTER(ALL(T), pred) → GrandTotal(AggIf(…));
 *     REMOVEFILTERS/ALL(T[col]) → GrandTotal + loud caveat; ALLEXCEPT /
 *     ALLSELECTED → flag-not-drop with DAX preserved.
 *
 * Run: node --import tsx/esm --test src/powerbi.hard-dax.test.ts
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  pbiDaxToSigma, convertPowerBIToSigma,
  pbiParseEarlierWindow, extractUseRelationships,
} from './powerbi.js';

// ── shared inline TMSL fixture ────────────────────────────────────────────────
const M_NAV = (table: string) => [
  'let Source = Snowflake.Databases("x.snowflakecomputing.com","WH"),',
  'N1 = Source{[Name="CSA",Kind="Database"]}[Data],',
  'N2 = N1{[Name="TJ",Kind="Schema"]}[Data],',
  `N3 = N2{[Name="${table}",Kind="Table"]}[Data] in N3`,
].join('\n');

function col(name: string, dataType = 'string') {
  return { name, dataType, sourceColumn: name };
}
function ordersModel(opts: { measures?: any[]; calcCols?: any[]; relationships?: any[] } = {}) {
  return {
    model: {
      tables: [
        {
          name: 'ORDERS',
          columns: [
            col('ORDER_ID'), col('REGION'), col('STATUS'),
            col('AMOUNT', 'double'), col('ORDER_DATE_KEY', 'int64'), col('SHIP_DATE_KEY', 'int64'),
            ...(opts.calcCols || []).map((c: any) => ({ ...c, type: 'calculated' })),
          ],
          partitions: [{ source: { type: 'm', expression: M_NAV('ORDER_FACT') } }],
          measures: opts.measures || [],
        },
        {
          name: 'DATE_DIM',
          columns: [col('DATE_KEY', 'int64'), col('FULL_DATE', 'dateTime'), col('MONTH_NAME')],
          partitions: [{ source: { type: 'm', expression: M_NAV('DATE_DIM') } }],
        },
      ],
      relationships: opts.relationships ?? [
        { name: 'r1', fromTable: 'ORDERS', fromColumn: 'ORDER_DATE_KEY', toTable: 'DATE_DIM', toColumn: 'DATE_KEY' },
        { name: 'r2', fromTable: 'ORDERS', fromColumn: 'SHIP_DATE_KEY', toTable: 'DATE_DIM', toColumn: 'DATE_KEY', isActive: false },
      ],
    },
  };
}
function els(model: any) { return model.pages[0].elements; }
function elByName(model: any, name: string) { return els(model).find((e: any) => e.name === name); }
function metricsOf(model: any): Record<string, string> {
  const out: Record<string, string> = {};
  for (const el of els(model)) for (const m of (el.metrics || [])) out[m.name] = m.formula;
  return out;
}
function sqlStatements(model: any): string[] {
  return els(model).filter((e: any) => e?.source?.kind === 'sql').map((e: any) => e.source.statement);
}

// ════ Family 1: USERELATIONSHIP ═══════════════════════════════════════════════

test('f1: extractUseRelationships strips the filter arg and collects the pair', () => {
  const r = extractUseRelationships(
    'CALCULATE(SUM(ORDERS[AMOUNT]), USERELATIONSHIP(ORDERS[SHIP_DATE_KEY], DATE_DIM[DATE_KEY]), ORDERS[STATUS] = "Done")');
  assert.equal(r.pairs.length, 1);
  assert.deepEqual(r.pairs[0], {
    a: { table: 'ORDERS', column: 'SHIP_DATE_KEY' },
    b: { table: 'DATE_DIM', column: 'DATE_KEY' },
  });
  assert.equal(r.dax, 'CALCULATE(SUM(ORDERS[AMOUNT]), ORDERS[STATUS] = "Done")');
});

test('f1: inactive relationship activated as TOTABLE_VIA_FROMCOL; measure emits the plain aggregate', () => {
  const { model, warnings } = convertPowerBIToSigma(ordersModel({
    measures: [
      { name: 'Total Amount', expression: 'SUM(ORDERS[AMOUNT])' },
      { name: 'Shipped Amount', expression: 'CALCULATE(SUM(ORDERS[AMOUNT]), USERELATIONSHIP(ORDERS[SHIP_DATE_KEY], DATE_DIM[DATE_KEY]))' },
    ],
  }), { connectionId: 'c' });
  const orders = elByName(model, 'ORDER_FACT');
  const relNames = (orders.relationships || []).map((r: any) => r.name).sort();
  assert.deepEqual(relNames, ['DATE_DIM', 'DATE_DIM_VIA_SHIP_DATE_KEY']);
  const m = metricsOf(model);
  assert.equal(m['Shipped Amount'], 'Sum([Amount])', 'the aggregate itself is unchanged');
  assert.ok(warnings.some(w => /Shipped Amount/.test(w) && /DATE_DIM_VIA_SHIP_DATE_KEY/.test(w) && /✅/.test(w)),
    'expected an activation warning naming the alternate path');
});

test('f1: derived View carries alternate-path columns and SKIPS the join-key passthrough', () => {
  const { model } = convertPowerBIToSigma(ordersModel({
    measures: [
      { name: 'Shipped Amount', expression: 'CALCULATE(SUM(ORDERS[AMOUNT]), USERELATIONSHIP(ORDERS[SHIP_DATE_KEY], DATE_DIM[DATE_KEY]))' },
    ],
  }), { connectionId: 'c' });
  const view = elByName(model, 'ORDER_FACT View');
  assert.ok(view, 'derived view must exist');
  const formulas = view.columns.map((c: any) => c.formula);
  // alternate-path columns surfaced
  assert.ok(formulas.includes('[ORDER_FACT/DATE_DIM_VIA_SHIP_DATE_KEY/Full Date]'),
    `expected alternate-keyed Full Date, got: ${formulas.join(' | ')}`);
  // active-path columns still present
  assert.ok(formulas.includes('[ORDER_FACT/DATE_DIM/Full Date]'));
  // join-key passthrough must be skipped on BOTH paths (type=error otherwise)
  assert.ok(!formulas.includes('[ORDER_FACT/DATE_DIM/Date Key]'),
    'active-path join key must be skipped');
  assert.ok(!formulas.includes('[ORDER_FACT/DATE_DIM_VIA_SHIP_DATE_KEY/Date Key]'),
    'alternate-path join key must be skipped');
});

test('f1: inactive relationship with NO USERELATIONSHIP usage is skipped (no name collision)', () => {
  const { model, warnings } = convertPowerBIToSigma(ordersModel({
    measures: [{ name: 'Total Amount', expression: 'SUM(ORDERS[AMOUNT])' }],
  }), { connectionId: 'c' });
  const orders = elByName(model, 'ORDER_FACT');
  assert.deepEqual((orders.relationships || []).map((r: any) => r.name), ['DATE_DIM']);
  assert.ok(warnings.some(w => /Inactive relationship/.test(w) && /skipped/.test(w)));
});

test('f1: a metric COMBINING measures on different join paths is refused with the recipe', () => {
  const { model, warnings } = convertPowerBIToSigma(ordersModel({
    measures: [
      { name: 'Ordered Amount', expression: 'CALCULATE(SUM(ORDERS[AMOUNT]), USERELATIONSHIP(ORDERS[ORDER_DATE_KEY], DATE_DIM[DATE_KEY]))' },
      { name: 'Shipped Amount', expression: 'CALCULATE(SUM(ORDERS[AMOUNT]), USERELATIONSHIP(ORDERS[SHIP_DATE_KEY], DATE_DIM[DATE_KEY]))' },
      { name: 'In Transit Amount', expression: '[Ordered Amount] - [Shipped Amount]' },
    ],
  }), { connectionId: 'c' });
  const m = metricsOf(model);
  assert.ok(!('In Transit Amount' in m), 'cross-path combination must not ship as a same-element scalar');
  assert.ok(warnings.some(w => /In Transit Amount/.test(w) && /DIFFERENT relationship paths/i.test(w)));
});

test('f1: USERELATIONSHIP plus an extra predicate still becomes a conditional aggregate', () => {
  const { model } = convertPowerBIToSigma(ordersModel({
    measures: [
      { name: 'Shipped Done', expression: 'CALCULATE(SUM(ORDERS[AMOUNT]), USERELATIONSHIP(ORDERS[SHIP_DATE_KEY], DATE_DIM[DATE_KEY]), ORDERS[STATUS] = "Done")' },
    ],
  }), { connectionId: 'c' });
  assert.equal(metricsOf(model)['Shipped Done'], 'SumIf([Amount], [Status] = "Done")');
});

// ════ Family 2: bare EARLIER idioms ═══════════════════════════════════════════

test('f2 parse: running total → AGG_RUNNING SUM ordered ASC', () => {
  const w = pbiParseEarlierWindow(
    'CALCULATE(SUM(ORDERS[AMOUNT]), FILTER(ALL(ORDERS), ORDERS[ORDER_DATE_KEY] <= EARLIER(ORDERS[ORDER_DATE_KEY])))');
  assert.ok(w);
  assert.equal(w!.op, 'AGG_RUNNING');
  assert.equal(w!.valueFn, 'SUM');
  assert.equal(w!.valueColSql, 'AMOUNT');
  assert.equal(w!.orderColSql, 'ORDER_DATE_KEY');
  assert.equal(w!.orderDir, 'ASC');
});

test('f2 parse: partitioned running total keeps the equality terms as PARTITION BY', () => {
  const w = pbiParseEarlierWindow(
    "SUMX(FILTER(ALL('ORDERS'), 'ORDERS'[REGION] = EARLIER('ORDERS'[REGION]) && 'ORDERS'[ORDER_DATE_KEY] <= EARLIER('ORDERS'[ORDER_DATE_KEY])), 'ORDERS'[AMOUNT])");
  assert.ok(w);
  assert.equal(w!.op, 'AGG_RUNNING');
  assert.deepEqual(w!.partitionRaw, ['REGION']);
});

test('f2 parse: group total (equality only) → AGG_PARTITION; peer count → COUNT(*)', () => {
  const g = pbiParseEarlierWindow(
    'SUMX(FILTER(ALL(ORDERS), ORDERS[REGION] = EARLIER(ORDERS[REGION])), ORDERS[AMOUNT])');
  assert.equal(g!.op, 'AGG_PARTITION');
  assert.deepEqual(g!.partitionRaw, ['REGION']);
  const c = pbiParseEarlierWindow(
    'COUNTROWS(FILTER(ALL(ORDERS), ORDERS[REGION] = EARLIER(ORDERS[REGION])))');
  assert.equal(c!.op, 'AGG_PARTITION');
  assert.equal(c!.valueColSql, '*');
});

test('f2 parse: at-or-above peer comparison (no +1) → window COUNT ordered DESC (ties exact via RANGE)', () => {
  const w = pbiParseEarlierWindow('COUNTROWS(FILTER(ALL(ORDERS), ORDERS[AMOUNT] >= EARLIER(ORDERS[AMOUNT])))');
  assert.ok(w);
  assert.equal(w!.op, 'AGG_RUNNING');
  assert.equal(w!.valueColSql, '*');
  assert.equal(w!.orderDir, 'DESC');
});

test('f2 parse: unrecognized shapes degrade to null (strict inequality, expression terms)', () => {
  assert.equal(pbiParseEarlierWindow(
    'COUNTROWS(FILTER(ALL(ORDERS), ORDERS[AMOUNT] > EARLIER(ORDERS[AMOUNT])))'), null,
    'strict > without +1 is NOT the running/rank tie semantics');
  assert.equal(pbiParseEarlierWindow(
    'SUMX(FILTER(ALL(ORDERS), ORDERS[X] >= EARLIER(ORDERS[Y]) + 1), ORDERS[AMOUNT])'), null);
});

test('f2: calc-column idioms lower onto SQL window helper elements', () => {
  const { model, warnings } = convertPowerBIToSigma(ordersModel({
    calcCols: [
      { name: 'Running Amount', expression: 'CALCULATE(SUM(ORDERS[AMOUNT]), FILTER(ALL(ORDERS), ORDERS[ORDER_DATE_KEY] <= EARLIER(ORDERS[ORDER_DATE_KEY])))' },
      { name: 'Region Total', expression: 'SUMX(FILTER(ALL(ORDERS), ORDERS[REGION] = EARLIER(ORDERS[REGION])), ORDERS[AMOUNT])' },
      { name: 'Region Peers', expression: 'COUNTROWS(FILTER(ALL(ORDERS), ORDERS[REGION] = EARLIER(ORDERS[REGION])))' },
    ],
  }), { connectionId: 'c' });
  const sqls = sqlStatements(model).join('\n;;\n');
  assert.match(sqls, /SUM\(AMOUNT\) OVER \(ORDER BY ORDER_DATE_KEY ASC\)/, 'running total OVER clause');
  assert.match(sqls, /SUM\(AMOUNT\) OVER \(PARTITION BY REGION\)/, 'group total OVER clause');
  assert.match(sqls, /COUNT\(\*\) OVER \(PARTITION BY REGION\)/, 'peer count OVER clause');
  assert.match(sqls, /FROM CSA\.TJ\.ORDER_FACT/, 'helpers select from the real warehouse table');
  assert.ok(warnings.filter(w => /✅/.test(w) && /SQL window helper/.test(w)).length >= 3);
  // The raw idiom must never survive as a base-table calc column.
  const orders = elByName(model, 'ORDER_FACT');
  for (const c of orders.columns) {
    assert.ok(!/EARLIER|CumulativeSum|RankDense/i.test(c.formula || ''), `raw idiom leaked: ${c.formula}`);
  }
});

test('f2: helpers sharing a partition reuse ONE sql element (cols unioned)', () => {
  const { model } = convertPowerBIToSigma(ordersModel({
    calcCols: [
      { name: 'Region Total', expression: 'SUMX(FILTER(ALL(ORDERS), ORDERS[REGION] = EARLIER(ORDERS[REGION])), ORDERS[AMOUNT])' },
      { name: 'Region Peers', expression: 'COUNTROWS(FILTER(ALL(ORDERS), ORDERS[REGION] = EARLIER(ORDERS[REGION])))' },
    ],
  }), { connectionId: 'c' });
  const sqls = sqlStatements(model).filter(s => /OVER/.test(s));
  assert.equal(sqls.length, 1, 'one shared helper element');
  assert.match(sqls[0], /SUM\(AMOUNT\) OVER \(PARTITION BY REGION\)/);
  assert.match(sqls[0], /COUNT\(\*\) OVER \(PARTITION BY REGION\)/);
});

test('f2: unrecognized EARLIER flags with the original DAX preserved, drops the column', () => {
  const weird = 'SUMX(FILTER(ALL(ORDERS), ORDERS[AMOUNT] >= EARLIER(ORDERS[ORDER_DATE_KEY]) * 2), ORDERS[AMOUNT])';
  const { model, warnings } = convertPowerBIToSigma(ordersModel({
    calcCols: [{ name: 'Weird Earlier', expression: weird }],
  }), { connectionId: 'c' });
  assert.ok(warnings.some(w => /Weird Earlier/.test(w) && /unrecognized EARLIER/i.test(w) && w.includes('EARLIER(ORDERS[ORDER_DATE_KEY])')),
    'flag must preserve the original DAX');
  for (const el of els(model)) {
    for (const c of (el.columns || [])) assert.ok(!/EARLIER/i.test(c.formula || ''), `EARLIER leaked: ${c.formula}`);
  }
});

// ════ Family 3: complex FILTER inside CALCULATE ═══════════════════════════════

test('f3 unit: boolean AND/OR predicate → SumIf with and/or', () => {
  assert.equal(
    pbiDaxToSigma('CALCULATE(SUM(T[AMT]), FILTER(T, T[REGION] = "West" && T[STATUS] = "Active" || T[PRIORITY] = "High"))', [], 'x'),
    'SumIf([AMT], [REGION] = "West" and [STATUS] = "Active" or [PRIORITY] = "High")');
});

test('f3 unit: multi-predicate CALCULATE args AND together', () => {
  assert.equal(
    pbiDaxToSigma('CALCULATE(SUM(E[SAL]), E[STATUS] = "Active", E[TYPE] = "Full-Time")', [], 'x'),
    'SumIf([SAL], [STATUS] = "Active" and [TYPE] = "Full-Time")');
});

test('f3 unit: IN {…} → or-chain (Sigma has no IsIn); NOT IN → and-chain of !=', () => {
  assert.equal(
    pbiDaxToSigma('CALCULATE(COUNTROWS(T), T[TYPE] IN {"A", "B", "C"})', [], 'x'),
    'CountIf(([TYPE] = "A" or [TYPE] = "B" or [TYPE] = "C"))');
  assert.equal(
    pbiDaxToSigma('CALCULATE(COUNTROWS(T), NOT T[TYPE] IN {"A", "B"})', [], 'x'),
    'CountIf(([TYPE] != "A" and [TYPE] != "B"))');
  const out = pbiDaxToSigma('CALCULATE(SUM(T[V]), T[TYPE] IN {"A", "B"})', [], 'x');
  assert.ok(!/\bIsIn\b/.test(out || ''), 'IsIn must never be emitted');
});

test('f3 unit: <> → !=; TRUE() → True', () => {
  assert.equal(
    pbiDaxToSigma('CALCULATE(DISTINCTCOUNT(S[ID]), FILTER(S, S[SEV] <> "Low"))', [], 'x'),
    'CountDistinctIf([ID], [SEV] != "Low")');
  assert.equal(
    pbiDaxToSigma('CALCULATE(COUNTROWS(S), S[FLAG] = TRUE())', [], 'x'),
    'CountIf([FLAG] = True)');
});

test('f3 unit: FILTER(ALL(T), pred) — context strip → GrandTotal(AggIf(…))', () => {
  assert.equal(
    pbiDaxToSigma('CALCULATE(SUM(T[AMT]), FILTER(ALL(T), T[AMT] > 100))', [], 'x'),
    'GrandTotal(SumIf([AMT], [AMT] > 100))');
});

test('f3 unit: REMOVEFILTERS(T[col]) / ALL(T[col]) → GrandTotal + loud caveat warning', () => {
  const w1: string[] = [];
  assert.equal(pbiDaxToSigma('CALCULATE(SUM(T[AMT]), REMOVEFILTERS(T[REGION]))', w1, 'm1'),
    'GrandTotal(Sum([AMT]))');
  assert.ok(w1.some(w => /EXACT when \[REGION\] is the only grouping/.test(w)));
  const w2: string[] = [];
  assert.equal(pbiDaxToSigma('CALCULATE(SUM(T[AMT]), ALL(T[REGION]))', w2, 'm2'),
    'GrandTotal(Sum([AMT]))');
  assert.ok(w2.some(w => /window total over the remaining dimensions/.test(w)));
});

test('f3 unit: ALLEXCEPT / ALLSELECTED → flag-not-drop with the DAX preserved', () => {
  const w: string[] = [];
  assert.equal(pbiDaxToSigma('CALCULATE(SUM(T[AMT]), ALLEXCEPT(T, T[DEPT]))', w, 'pct'), null);
  assert.ok(w.some(x => /pct/.test(x) && /subtotal semantics/.test(x) && x.includes('ALLEXCEPT(T, T[DEPT])')),
    'warning must preserve the original DAX');
});

test('f3 unit: predicate comparing to a measure/aggregate still refuses (windowed compare needed)', () => {
  const w: string[] = [];
  assert.equal(pbiDaxToSigma('CALCULATE(SUM(T[AMT]), FILTER(T, T[X] > [Avg X]))', w, 'm'), null);
  assert.ok(w.some(x => /aggregate\/measure/.test(x)));
});

test('f3 unit: spliced conditional aggregates survive inside DIVIDE / COALESCE wrappers', () => {
  assert.equal(
    pbiDaxToSigma('DIVIDE(CALCULATE(SUM(T[AMT]), T[A] = 1), CALCULATE(SUM(T[AMT]), T[B] = 2))', [], 'x'),
    '(SumIf([AMT], [A] = 1)) / (SumIf([AMT], [B] = 2))');
});

test('f3: integration — metrics emit clean Sigma, no raw DAX tokens', () => {
  const { model } = convertPowerBIToSigma(ordersModel({
    measures: [
      { name: 'West Active Amount', expression: 'CALCULATE(SUM(ORDERS[AMOUNT]), FILTER(ORDERS, ORDERS[REGION] = "West" && ORDERS[STATUS] = "Active"))' },
      { name: 'Priority Orders', expression: 'CALCULATE(COUNTROWS(ORDERS), ORDERS[STATUS] IN {"Open", "Rush"})' },
      { name: 'Big Order Total All', expression: 'CALCULATE(SUM(ORDERS[AMOUNT]), FILTER(ALL(ORDERS), ORDERS[AMOUNT] >= 500))' },
    ],
  }), { connectionId: 'c' });
  const m = metricsOf(model);
  assert.equal(m['West Active Amount'], 'SumIf([Amount], [Region] = "West" and [Status] = "Active")');
  assert.equal(m['Priority Orders'], 'CountIf(([Status] = "Open" or [Status] = "Rush"))');
  assert.equal(m['Big Order Total All'], 'GrandTotal(SumIf([Amount], [Amount] >= 500))');
  for (const [name, formula] of Object.entries(m)) {
    assert.ok(!/&&|\|\||\bIN\s*\{|\bCALCULATE\b|\bFILTER\b|\bIsIn\b|<>/.test(formula),
      `raw DAX token leaked in "${name}": ${formula}`);
  }
});

// ════ Time-intelligence synthesis: multi-word column + inactive-rel fidelity ══
// Regression for the emitTimeIntelElements value-column match: lastSeg() yields
// the Sigma DISPLAY name ("Incident Id") while the DAX token is "INCIDENT_ID",
// so a plain uppercase compare silently dropped every multi-word measure column
// (single-word ones like HOURS coincidentally matched). Plus the inactive-date
// fidelity warning surfaced by comparing live DAX vs the synthesized element.
function tiModel(dateRelActive: boolean) {
  return { model: {
    tables: [
      { name: 'INCIDENTS',
        columns: [col('INCIDENT_ID'), col('DEPT_ID'), col('EVENT_DATE', 'dateTime')],
        partitions: [{ source: { type: 'm', expression: M_NAV('INCIDENTS') } }],
        measures: [{ name: 'PY Incident Count',
          expression: 'CALCULATE(DISTINCTCOUNT(INCIDENTS[INCIDENT_ID]), SAMEPERIODLASTYEAR(CAL[Date]))' }] },
      { name: 'DEPTS', columns: [col('DEPT_ID'), col('DEPT_NAME')],
        partitions: [{ source: { type: 'm', expression: M_NAV('DEPTS') } }] },
      { name: 'CAL', columns: [col('Date', 'dateTime')],
        partitions: [{ source: { type: 'm', expression: M_NAV('CAL') } }] },
    ],
    relationships: [
      { name: 'r_dept', fromTable: 'INCIDENTS', fromColumn: 'DEPT_ID', toTable: 'DEPTS', toColumn: 'DEPT_ID' },
      { name: 'r_date', fromTable: 'INCIDENTS', fromColumn: 'EVENT_DATE', toTable: 'CAL', toColumn: 'Date', isActive: dateRelActive },
    ],
  } };
}

test('ti: SAMEPERIODLASTYEAR over a MULTI-WORD distinct-count column synthesizes (no silent drop)', () => {
  const { model } = convertPowerBIToSigma(tiModel(true), { connectionId: 'c' });
  const el = elByName(model, 'PY Incident Count');
  assert.ok(el, 'expected a synthesized prior-year element (multi-word col must match)');
  const formulas = el.columns.map((c: any) => c.formula);
  assert.ok(formulas.some((f: string) => /^CountDistinct\(\[INCIDENTS View\/Incident Id\]\)$/.test(f)),
    `expected CountDistinct of the multi-word value column, got: ${formulas.join(' | ')}`);
  assert.ok(formulas.some((f: string) => /^DateLookback\(/.test(f)), 'expected a DateLookback prior-year column');
});

test('ti: inactive date relationship raises the fidelity-divergence warning', () => {
  const { warnings } = convertPowerBIToSigma(tiModel(false), { connectionId: 'c' });
  assert.ok(warnings.some(w => /PY Incident Count/.test(w) && /INACTIVE/.test(w)
    && /unfiltered total/.test(w) && /own date/.test(w)),
    'inactive date rel must warn that PBI returns an unfiltered total while the element uses the fact date');
});

test('ti: ACTIVE date relationship synthesizes WITHOUT the divergence warning', () => {
  const { model, warnings } = convertPowerBIToSigma(tiModel(true), { connectionId: 'c' });
  assert.ok(elByName(model, 'PY Incident Count'), 'element still synthesized on the active path');
  assert.ok(!warnings.some(w => /PY Incident Count/.test(w) && /INACTIVE/.test(w)),
    'no divergence warning when the date relationship is active');
});
