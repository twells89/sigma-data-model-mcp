/**
 * Regression tests for the 2026-06-12 Retail Analysis Sample fix batch:
 *   p146 — CALCULATE([measure-ref], col=literal) over a measure CHAIN
 *          (TotalSalesTY = CALCULATE([TotalSales], ScenarioID=1) where
 *          TotalSales = [m1]+[m2], m1/m2 = SUM(col)): flatten the chain and
 *          distribute the predicate over each leaf aggregate. Previously the
 *          whole TY/LY family (14 measures) cascade-dropped.
 *   f5kp — camelCase column physical-name canonicalization: emitted physical
 *          id and the [TABLE/Display Name] formula ref must agree. Plain
 *          toUpperCase gave LOCATIONID, which Sigma displays as "Locationid",
 *          while the ref said "Location Id" → "dependency not found" at POST.
 *
 * Run: node --import tsx/esm --test src/powerbi.fix-4.test.ts
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { pbiDaxToSigma, convertPowerBIToSigma, expandMeasureRefs, isAggCombination } from './powerbi.js';
import { sigmaPhysicalName, sigmaDisplayName } from './sigma-ids.js';

const RETAIL_MEASURES: Record<string, string> = {
  'Regular_Sales_Dollars': 'SUM([Sum_Regular_Sales_Dollars])',
  'Markdown_Sales_Dollars': 'SUM([Sum_Markdown_Sales_Dollars])',
  'TotalSales': '[Regular_Sales_Dollars]+[Markdown_Sales_Dollars]',
};

test('p146: CALCULATE over a measure CHAIN distributes the predicate over each leaf aggregate', () => {
  const warnings: string[] = [];
  const out = pbiDaxToSigma('CALCULATE([TotalSales], Sales[ScenarioID]=1)', warnings, 'TotalSalesTY', RETAIL_MEASURES);
  assert.ok(out, `should translate, got null (warnings: ${warnings.join(' | ')})`);
  const sumIfs = (out!.match(/SumIf\(/g) || []).length;
  assert.equal(sumIfs, 2, `expected 2 SumIf (one per leaf aggregate), got: ${out}`);
  assert.match(out!, /\[ScenarioID\]\s*=\s*1/, `predicate must survive: ${out}`);
  assert.match(out!, /Sum_Regular_Sales_Dollars/, `first leaf col missing: ${out}`);
  assert.match(out!, /Sum_Markdown_Sales_Dollars/, `second leaf col missing: ${out}`);
});

test('p146: chain expansion is cycle-safe', () => {
  assert.equal(expandMeasureRefs('[A]', { A: '[B]', B: '[A]' }), null);
});

test('p146: isAggCombination accepts agg arithmetic, rejects non-agg leftovers', () => {
  assert.equal(isAggCombination('(SUM(Sales[A])) + (SUM(Sales[B]))'), true);
  assert.equal(isAggCombination('(SUM(Sales[A])) / NULLIF(SUM(Sales[B]), 0)'), false); // NULLIF not distributable
  assert.equal(isAggCombination('[UnexpandedRef] + SUM(Sales[B])'), false);
});

test('p146: CALCULATE(chain, ALL(Table)) grand-totals the whole combination', () => {
  const out = pbiDaxToSigma('CALCULATE([TotalSales], ALL(Sales))', [], 'GrandTotalSales', RETAIL_MEASURES);
  assert.ok(out, 'should translate');
  assert.match(out!, /GrandTotal\(/, `expected GrandTotal wrap: ${out}`);
  // either form is valid: GrandTotal over the surviving metric ref, or over the
  // expanded 2-leaf Sum combination.
  const ok = /GrandTotal\(\[TotalSales\]\)/.test(out!) || (out!.match(/Sum\(/g) || []).length === 2;
  assert.ok(ok, `expected GrandTotal([TotalSales]) or 2 plain Sum leaves: ${out}`);
});

test('f5kp: sigmaPhysicalName round-trips through sigmaDisplayName', () => {
  for (const [src, phys] of [
    ['LocationID', 'LOCATION_ID'],
    ['City Name', 'CITY_NAME'],
    ['Sum_GrossMarginAmount', 'SUM_GROSS_MARGIN_AMOUNT'],
    ['StoreNumberName', 'STORE_NUMBER_NAME'],
    ['FamilyNane', 'FAMILY_NANE'],
  ] as const) {
    assert.equal(sigmaPhysicalName(src), phys, `physical of ${src}`);
    // the CONTRACT: Sigma's display of the emitted physical name == the ref we emit
    assert.equal(sigmaDisplayName(phys), sigmaDisplayName(src),
      `display round-trip broken for ${src}: phys=${phys}`);
  }
});

test('f5kp: already-canonical warehouse names pass through verbatim', () => {
  assert.equal(sigmaPhysicalName('CY_Q1_REVENUE'), 'CY_Q1_REVENUE');
  assert.equal(sigmaPhysicalName('ANNUAL_SALARY'), 'ANNUAL_SALARY');
  assert.equal(sigmaPhysicalName('DM'), 'DM');
});

// Minimal Retail-Analysis-shaped model: camelCase columns + a TY measure chain.
const MINI_RETAIL_BIM = {
  model: {
    tables: [
      {
        name: 'Sales',
        columns: [
          { name: 'ScenarioID', dataType: 'int64', sourceColumn: 'ScenarioID' },
          { name: 'Sum_Regular_Sales_Dollars', dataType: 'double', sourceColumn: 'Sum_Regular_Sales_Dollars' },
          { name: 'Sum_Markdown_Sales_Dollars', dataType: 'double', sourceColumn: 'Sum_Markdown_Sales_Dollars' },
        ],
        measures: [
          { name: 'Regular_Sales_Dollars', expression: 'SUM([Sum_Regular_Sales_Dollars])' },
          { name: 'Markdown_Sales_Dollars', expression: 'SUM([Sum_Markdown_Sales_Dollars])' },
          { name: 'TotalSales', expression: '[Regular_Sales_Dollars]+[Markdown_Sales_Dollars]' },
          { name: 'TotalSalesTY', expression: 'CALCULATE([TotalSales], Sales[ScenarioID]=1)' },
          { name: 'This Year Sales', expression: '[TotalSalesTY]' },
        ],
        partitions: [{ name: 'Sales', source: { type: 'query', query: 'SELECT * FROM [Sales]' } }],
      },
      {
        name: 'Store',
        columns: [
          { name: 'LocationID', dataType: 'int64', sourceColumn: 'LocationID' },
          { name: 'City Name', dataType: 'string', sourceColumn: 'City Name' },
        ],
        partitions: [{ name: 'Store', source: { type: 'query', query: 'SELECT * FROM [Store]' } }],
      },
    ],
  },
};

test('p146 end-to-end: TY measure chain survives conversion (no cascade drop)', () => {
  const { model: dm, warnings } = convertPowerBIToSigma(MINI_RETAIL_BIM as any, {
    connectionId: 'c', database: 'CSA', schema: 'TJ',
  }) as any;
  const sales = (dm.pages?.[0]?.elements || []).find((e: any) => e.name === 'SALES');
  assert.ok(sales, 'SALES element exists');
  const names = (sales.metrics || []).map((m: any) => m.name);
  for (const want of ['TotalSalesTY', 'This Year Sales']) {
    assert.ok(names.includes(want), `metric "${want}" dropped (have: ${names.join(', ')}); warnings: ${(warnings || []).join(' | ')}`);
  }
  const ty = (sales.metrics || []).find((m: any) => m.name === 'TotalSalesTY');
  assert.equal((String(ty.formula).match(/SumIf\(/g) || []).length, 2, `TY formula: ${ty.formula}`);
});

test('f5kp end-to-end: emitted physical id matches its own display ref', () => {
  const { model: dm } = convertPowerBIToSigma(MINI_RETAIL_BIM as any, {
    connectionId: 'c', database: 'CSA', schema: 'TJ',
  }) as any;
  const store = (dm.pages?.[0]?.elements || []).find((e: any) => e.name === 'STORE');
  assert.ok(store, 'STORE element exists');
  for (const col of store.columns) {
    const phys = String(col.id).split('/').pop()!;
    const refName = String(col.formula).replace(/^\[STORE\//, '').replace(/\]$/, '');
    assert.equal(sigmaDisplayName(phys), refName,
      `id physical "${phys}" displays as "${sigmaDisplayName(phys)}" but ref says "${refName}"`);
  }
  const locId = store.columns.find((c: any) => String(c.id).endsWith('/LOCATION_ID'));
  assert.ok(locId, `LocationID should emit physical LOCATION_ID (ids: ${store.columns.map((c: any) => c.id).join(', ')})`);
});

test('concat coercion: DAX & wraps column-ref operands in Text()', () => {
  const out = pbiDaxToSigma('Sales[MonthID]&"01"', [], 'ReportingPeriodID');
  assert.equal(out, 'Text([MonthID]) & "01"'.replace(' & ', '&'), `got: ${out}`);
});

test('concat coercion: chained concat coerces every column operand', () => {
  const out = pbiDaxToSigma('[City Name] & ", "&[Territory]', [], 'City');
  assert.match(out!, /^Text\(\[City Name\]\) & ", "&Text\(\[Territory\]\)$/, `got: ${out}`);
});
