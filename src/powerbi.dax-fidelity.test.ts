/**
 * DAX → Sigma fidelity fixes (measure-conversion correctness).
 *
 * Ports a set of proven Power BI → Sigma DAX-conversion fixes, validated by
 * rebuilding 3 real insurance reports to ≥90% measure parity. Every fixture
 * here is SYNTHETIC (generic SALES_FACT / DATE_DIM / REGION_DIM names) — no
 * real report data.
 *
 * Fix classes exercised (one describe-block each):
 *   #1  single-arg CountIf; DISTINCTCOUNT → CountDistinct(If(...)); COUNT(col)
 *       counts non-null; never the illegal 2-arg CountIf / CountDistinctIf form
 *   #2  Not (x) with a space
 *   #3  Avg(...) not Average(...)
 *   #4  each measure keeps its HOME fact table (no cross-fact mis-binding)
 *   #5  CALCULATE(agg, ALL(col)) → filter-scoped metric, NOT GrandTotal
 *       (whole-table ALL(table) %-of-total → GrandTotal is preserved)
 *   #6  time-intelligence → self-contained conditional date windows
 *   #7  USERELATIONSHIP is flagged/handled, never silently mangled
 *   #8  IF(SELECTEDVALUE(control)=…) → control-driven If switch
 *   #9  nested [Measure] references resolve (inline, cycle-guarded)
 *   #10 DIVIDE(a,b) → a / NullIf(b,0); FORMAT(numeric) → Text; CONCATENATEX cosmetic
 *   #11 DAX calculated tables (TODAY(), hardcoded lists) → real SQL, not a stub
 *
 * Run: node --import tsx/esm --test src/powerbi.dax-fidelity.test.ts
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { pbiDaxToSigma, convertPowerBIToSigma, expandMeasureRefs } from './powerbi.js';

const conv = (d: string, warns: string[] | null = [], name = 'm', measures: Record<string, string> = {}) =>
  pbiDaxToSigma(d, warns, name, measures);

// ── #1 count family ──────────────────────────────────────────────────────────
test('#1 COUNT(col) with a filter → single-arg CountIf that also counts only non-null', () => {
  assert.equal(
    conv('CALCULATE(COUNT(SALES_FACT[CUSTOMER_KEY]), SALES_FACT[STATUS] = "Closed")'),
    'CountIf([STATUS] = "Closed" and IsNotNull([CUSTOMER_KEY]))');
});

test('#1 COUNTROWS(table) with a filter → single-arg CountIf(cond)', () => {
  assert.equal(
    conv('CALCULATE(COUNTROWS(SALES_FACT), SALES_FACT[STATUS] = "Closed")'),
    'CountIf([STATUS] = "Closed")');
});

test('#1 DISTINCTCOUNT with a filter → CountDistinct(If(...)), never the 2-arg CountDistinctIf', () => {
  const out = conv('CALCULATE(DISTINCTCOUNT(SALES_FACT[CUSTOMER_KEY]), SALES_FACT[STATUS] = "Closed")')!;
  assert.equal(out, 'CountDistinct(If([STATUS] = "Closed", [CUSTOMER_KEY], null))');
  assert.doesNotMatch(out, /CountDistinctIf\(/);
});

test('#1 never emits the illegal two-arg CountIf(cond, [col]) form', () => {
  const out = conv('CALCULATE(COUNT(SALES_FACT[CUSTOMER_KEY]), SALES_FACT[STATUS] = "Closed")')!;
  assert.ok(!/\bCountIf\(\s*[^()]*,\s*\[/.test(out), `two-arg CountIf leaked: ${out}`);
});

// ── #2 Not ( ─────────────────────────────────────────────────────────────────
test('#2 NOT(x) → Not (x) with a space (the proven Sigma form)', () => {
  const out = conv('NOT(ISBLANK(SALES_FACT[REGION]))')!;
  assert.equal(out, 'Not (IsNull([REGION]))');
  assert.doesNotMatch(out, /Not\(/, 'must not emit the no-space Not( form');
});

// ── #3 Avg ───────────────────────────────────────────────────────────────────
test('#3 AVERAGE(col) → Avg(col) (Sigma has no Average)', () => {
  const out = conv('AVERAGE(SALES_FACT[AMOUNT])')!;
  assert.equal(out, 'Avg([AMOUNT])');
  assert.doesNotMatch(out, /Average/);
});

// ── #4 home fact table ─────────────────────────────────────────────────────────
test('#4 a measure binds to its HOME fact element, not the largest element', () => {
  const wt = (name: string) =>
    `let Source = Sql.Database("s","DB"), t = Source{[Name="S",Kind="Schema"]}{[Name="${name}",Kind="Table"]}[Data] in t`;
  const model = {
    tables: [
      { name: 'CP_FACT', columns: [{ name: 'CP_AMOUNT', dataType: 'double' }, { name: 'CP_KEY', dataType: 'int64' }], partitions: [{ source: { expression: wt('CP_FACT') } }] },
      { name: 'GL_FACT', columns: [{ name: 'GL_PREMIUM', dataType: 'double' }, { name: 'GL_KEY', dataType: 'int64' }], partitions: [{ source: { expression: wt('GL_FACT') } }] },
      { name: '_Measures', columns: [], measures: [
        { name: 'CP Total', expression: 'SUM(CP_FACT[CP_AMOUNT])' },
        { name: 'GL Premium', expression: 'SUM(GL_FACT[GL_PREMIUM])' },
      ] },
    ],
    relationships: [],
  };
  const { model: dm } = convertPowerBIToSigma(model as any, { connectionId: 'c', database: 'DB', schema: 'S' }) as any;
  const els = dm.pages?.[0]?.elements || dm.elements || [];
  const homeOf = (metricName: string) => {
    for (const e of els) for (const mt of (e.metrics || [])) if (mt.name === metricName) return (e.source?.path || []).join('.');
    return null;
  };
  assert.match(String(homeOf('GL Premium')), /GL_FACT$/, 'GL Premium must live on the GL fact, not the CP fact');
  assert.match(String(homeOf('CP Total')), /CP_FACT$/);
});

// ── #5 ALL(col) filter-scoped, NOT GrandTotal ─────────────────────────────────
test('#5 CALCULATE(agg, ALL(col)) → filter-scoped plain aggregate, NOT GrandTotal', () => {
  const w: string[] = [];
  const out = conv('CALCULATE(SUM(SALES_FACT[AMOUNT]), ALL(SALES_FACT[REGION]))', w)!;
  assert.equal(out, 'Sum([AMOUNT])');
  assert.doesNotMatch(out, /GrandTotal/, 'ALL(col) must not collapse to GrandTotal');
  assert.ok(w.some(x => /IGNORES any control/.test(x)), 'should flag the metric to ignore the bound control');
});

test('#5 whole-table ALL(table) %-of-total idiom still uses GrandTotal', () => {
  assert.equal(
    conv("DIVIDE(SUM(SALES_FACT[AMOUNT]), CALCULATE(SUM(SALES_FACT[AMOUNT]), ALL('SALES_FACT')))"),
    '(Sum([AMOUNT])) / NullIf((GrandTotal(Sum([AMOUNT]))), 0)');
});

// ── #6 time-intelligence → self-contained windows ─────────────────────────────
test('#6 DATESBETWEEN(date, TODAY()-364, TODAY()) → self-contained rolling-365-day window', () => {
  assert.equal(
    conv("CALCULATE(COUNT(SALES_FACT[ORDER_KEY]), DATESBETWEEN('DATE_DIM'[ORDER_DATE], TODAY()-364, TODAY()))"),
    'CountIf([ORDER_DATE] >= Today()-364 and [ORDER_DATE] <= Today() and IsNotNull([ORDER_KEY]))');
});

test('#6 current-month + current-year filters (MONTH/YEAR(TODAY()) = MONTH/YEAR(date))', () => {
  assert.equal(
    conv("CALCULATE(SUM(SALES_FACT[AMOUNT]), MONTH(TODAY()) = MONTH('DATE_DIM'[ORDER_DATE]), YEAR(TODAY()) = YEAR('DATE_DIM'[ORDER_DATE]))"),
    'SumIf([AMOUNT], Month(Today()) = Month([ORDER_DATE]) and Year(Today()) = Year([ORDER_DATE]))');
});

// ── #7 USERELATIONSHIP ─────────────────────────────────────────────────────────
test('#7 USERELATIONSHIP reaching the formula layer is flagged, never silently mangled', () => {
  const w: string[] = [];
  const out = conv('CALCULATE(SUM(SALES_FACT[AMOUNT]), USERELATIONSHIP(DATE_DIM[DATE_KEY], SALES_FACT[SHIP_DATE_KEY]))', w, 'Ship Amount');
  assert.equal(out, null, 'not translated to a broken formula');
  assert.ok(w.some(x => /USERELATIONSHIP/i.test(x)), 'the relationship remap must be noted');
});

// ── #8 control-driven switch ───────────────────────────────────────────────────
test('#8 IF(SELECTEDVALUE(control) = …, a, b) → control-driven If switch', () => {
  assert.equal(
    conv('IF(SELECTEDVALUE(REGION_DIM[REGION_NAME]) = "West", SUM(SALES_FACT[AMOUNT]), 0)'),
    'If(If(CountDistinct([REGION_NAME]) = 1, Min([REGION_NAME]), null) = "West", Sum([AMOUNT]), 0)');
});

// ── #9 measure composition ─────────────────────────────────────────────────────
test('#9 nested [Measure] references resolve (inline, cycle-guarded)', () => {
  // inside CALCULATE a simple sibling measure inlines to a conditional aggregate
  assert.equal(
    conv('CALCULATE([Base], SALES_FACT[REGION] = "West")', [], 'x', { Base: 'SUM(SALES_FACT[AMOUNT])' }),
    'SumIf([AMOUNT], [REGION] = "West")');
  // expandMeasureRefs inlines a chain and returns null on a cycle
  assert.equal(expandMeasureRefs('[A] + [B]', { A: 'SUM(T[X])', B: 'SUM(T[Y])' }), '(SUM(T[X])) + (SUM(T[Y]))');
  assert.equal(expandMeasureRefs('[A]', { A: '[B]', B: '[A]' }), null);
});

// ── #10 DIVIDE / FORMAT / CONCATENATEX ─────────────────────────────────────────
test('#10 DIVIDE(a, b) → a / NullIf(b, 0) (DAX blank-on-zero, not a Snowflake error)', () => {
  assert.equal(
    conv('DIVIDE(SUM(SALES_FACT[AMOUNT]), COUNT(SALES_FACT[ORDER_KEY]))'),
    '(Sum([AMOUNT])) / NullIf((Count([ORDER_KEY])), 0)');
});

test('#10 DIVIDE(a, b, alt) → guarded quotient with the alternate result', () => {
  assert.equal(
    conv('DIVIDE([A],[B],0)', [], 'x', { A: 'SUM(SALES_FACT[AMOUNT])', B: 'SUM(SALES_FACT[QTY])' }),
    'If(([B]) = 0, 0, ([A]) / ([B]))');
});

test('#10 FORMAT(numeric date expr, "fmt") → Text(expr)', () => {
  assert.equal(conv('FORMAT(MONTH(TODAY()),"00")'), 'Text(Month(Today()))');
});

test('#10 CONCATENATEX list label → flagged cosmetic (not a numeric metric)', () => {
  const w: string[] = [];
  assert.equal(conv('CONCATENATEX(REGION_DIM, REGION_DIM[REGION_NAME], ", ")', w), null);
  assert.ok(w.some(x => /CONCATENATEX/.test(x)));
});

// ── #11 calculated tables → real SQL ───────────────────────────────────────────
function calcTableSql(expr: string, cols: string[]): { stmt: string | undefined; hasPlaceholder: boolean } {
  const model = {
    tables: [{ name: 'CT', columns: cols.map(c => ({ name: c, dataType: 'string' })), partitions: [{ source: { type: 'calculated', expression: expr } }] }],
    relationships: [],
  };
  const { model: dm } = convertPowerBIToSigma(model as any, { connectionId: 'c' }) as any;
  const els = dm.pages?.[0]?.elements || dm.elements || [];
  const sqlEl = els.find((e: any) => e.source?.kind === 'sql');
  return { stmt: sqlEl?.source?.statement, hasPlaceholder: JSON.stringify(dm).includes('_placeholder') };
}

test('#11 TODAY() calc table → real CURRENT_DATE SQL element, not a placeholder stub', () => {
  const r = calcTableSql('ROW("Report Date", TODAY())', ['Report Date']);
  assert.match(String(r.stmt), /SELECT\s+CURRENT_DATE\s+AS\s+"Report Date"/i);
  assert.equal(r.hasPlaceholder, false);
});

test('#11 hardcoded literal-list calc table → real UNION ALL SQL element, not a placeholder', () => {
  const r = calcTableSql('{ "North", "South", "East", "West" }', ['Region']);
  assert.match(String(r.stmt), /SELECT 'North' AS "Region" UNION ALL/);
  assert.ok(/'West'/.test(String(r.stmt)));
  assert.equal(r.hasPlaceholder, false);
});
