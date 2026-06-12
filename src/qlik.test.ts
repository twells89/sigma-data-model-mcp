/**
 * Qlik converter tests — inter-record / window-function workarounds
 * (beads-sigma-7dgk).
 *
 * Disposition under test (mirrors the PBI DateLookback/CumulativeSum handoff):
 *   Rank/Above/Below/Previous/Peek → Sigma Rank/Lag/Lead formulas reported in
 *     result.workbookPatterns (GROUPED workbook element context — window
 *     functions silently error in DM calc columns/metrics; live-verified
 *     2026-06-11 grouped-element Rank/RankDense/Lag/Lead == warehouse
 *     RANK/DENSE_RANK/LAG/LEAD on CSA.TJ.ORDER_FACT).
 *   FirstSortedValue → kind:'sql' QUALIFY helper element + Min() metric, or
 *     the Rank=n-filter pattern (verify-me) when the simple form doesn't hold.
 *   HRank / pivot column-axis functions / script-level Peek → flag-not-drop
 *     ('unsupported' pattern entry + warning).
 */

import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { convertQlikToSigma } from './qlik.js';

const TABLES = [{
  name: 'SALES',
  noOfRows: 100,
  fields: [
    { name: 'CUSTOMER', distinctValueCount: 10 },
    { name: 'REGION', distinctValueCount: 4 },
    { name: 'SALES_AMOUNT', distinctValueCount: 90 },
  ],
}];

function conv(measures: any[], dims: any[] = []) {
  return convertQlikToSigma({
    appName: 'IR Test',
    tables: TABLES,
    masterMeasures: measures,
    masterDimensions: dims,
  }, { connectionId: 'conn-1', database: 'CSA', schema: 'TJ' });
}

function allMetrics(r: any): any[] {
  return r.model.pages[0].elements.flatMap((e: any) => e.metrics || []);
}

describe('qlik inter-record: Rank()', () => {
  test('Rank(Sum(x)) → Rank(…, "desc") workbook pattern, no DM metric', () => {
    const r = conv([{ title: 'Sales Rank', expr: 'Rank(Sum(SALES_AMOUNT))' }]);
    assert.equal(allMetrics(r).length, 0, 'must NOT emit a DM metric');
    assert.ok(r.workbookPatterns?.length === 1);
    const p = r.workbookPatterns![0];
    assert.equal(p.kind, 'rank');
    assert.equal(p.formula, 'Rank(Sum([Sales Amount]), "desc")');
    assert.match(p.requires || '', /GROUPED workbook element/);
    assert.equal(p.elementName, 'SALES');
    assert.ok(r.warnings.some(w => w.includes('workbookPatterns')));
  });

  test('Rank(total …) strips the total qualifier', () => {
    const r = conv([{ title: 'R', expr: 'Rank(total Sum(SALES_AMOUNT))' }]);
    assert.equal(r.workbookPatterns![0].formula, 'Rank(Sum([Sales Amount]), "desc")');
  });

  test('Rank with set analysis reuses the conditional-aggregation translation', () => {
    const r = conv([{ title: 'R West', expr: "Rank(Sum({<REGION={'West'}>} SALES_AMOUNT))" }]);
    const p = r.workbookPatterns![0];
    assert.equal(p.kind, 'rank');
    assert.equal(p.formula, 'Rank(Sum(If([Region] = "West", [Sales Amount], 0)), "desc")');
  });

  test('Rank mode argument → verify flag + warning', () => {
    const r = conv([{ title: 'R4', expr: 'Rank(Sum(SALES_AMOUNT), 4)' }]);
    assert.equal(r.workbookPatterns![0].verify, true);
    assert.ok(r.warnings.some(w => /mode\/fmt argument/.test(w)));
  });

  test('HRank → flag-not-drop (unsupported pattern, no formula)', () => {
    const r = conv([{ title: 'H', expr: 'HRank(Sum(SALES_AMOUNT))' }]);
    assert.equal(allMetrics(r).length, 0);
    const p = r.workbookPatterns![0];
    assert.equal(p.kind, 'unsupported');
    assert.equal(p.formula, undefined);
    assert.match(p.note, /COLUMN dimension/);
  });
});

describe('qlik inter-record: Above/Below/Previous/Peek', () => {
  test('Above(Sum(x)) → Lag(…, 1)', () => {
    const r = conv([{ title: 'Prev', expr: 'Above(Sum(SALES_AMOUNT))' }]);
    const p = r.workbookPatterns![0];
    assert.equal(p.kind, 'lag');
    assert.equal(p.formula, 'Lag(Sum([Sales Amount]), 1)');
    assert.equal(p.verify, true);
  });

  test('Above(Sum(x), 2) → Lag(…, 2); Below → Lead', () => {
    const r = conv([
      { title: 'A2', expr: 'Above(Sum(SALES_AMOUNT), 2)' },
      { title: 'B1', expr: 'Below(Sum(SALES_AMOUNT))' },
    ]);
    assert.equal(r.workbookPatterns![0].formula, 'Lag(Sum([Sales Amount]), 2)');
    assert.equal(r.workbookPatterns![1].formula, 'Lead(Sum([Sales Amount]), 1)');
    assert.equal(r.workbookPatterns![1].kind, 'lead');
  });

  test('negative offset flips Lag/Lead', () => {
    const r = conv([{ title: 'Neg', expr: 'Above(Sum(SALES_AMOUNT), -1)' }]);
    assert.equal(r.workbookPatterns![0].formula, 'Lead(Sum([Sales Amount]), 1)');
  });

  test('RangeSum(Above(expr, 0, 3)) rolling window → folded Lag chain', () => {
    const r = conv([{ title: 'Rolling 3', expr: 'RangeSum(Above(Sum(SALES_AMOUNT), 0, 3))' }]);
    const p = r.workbookPatterns![0];
    assert.equal(p.kind, 'lag');
    assert.equal(p.formula,
      '(Coalesce(Sum([Sales Amount]), 0) + Coalesce(Lag(Sum([Sales Amount]), 1), 0) + Coalesce(Lag(Sum([Sales Amount]), 2), 0))');
  });

  test('Above range form outside a Range aggregation → flag-not-drop', () => {
    const r = conv([{ title: 'BadRange', expr: 'Above(Sum(SALES_AMOUNT), 0, 3)' }]);
    assert.equal(r.workbookPatterns![0].kind, 'unsupported');
  });

  test('Previous(x) → Lag(…, 1) with load-order warning', () => {
    const r = conv([{ title: 'PrevRec', expr: 'Previous(SALES_AMOUNT)' }]);
    assert.equal(r.workbookPatterns![0].formula, 'Lag([Sales Amount], 1)');
    assert.ok(r.warnings.some(w => /LOAD-ORDER/.test(w)));
  });

  test("Peek('f') and Peek('f', -2) → Lag", () => {
    const r = conv([
      { title: 'P1', expr: "Peek('SALES_AMOUNT')" },
      { title: 'P2', expr: "Peek('SALES_AMOUNT', -2)" },
    ]);
    assert.equal(r.workbookPatterns![0].formula, 'Lag([Sales Amount], 1)');
    assert.equal(r.workbookPatterns![1].formula, 'Lag([Sales Amount], 2)');
  });

  test('Peek with absolute row / table arg → flag-not-drop (script-time)', () => {
    const r = conv([
      { title: 'Abs', expr: "Peek('SALES_AMOUNT', 0)" },
      { title: 'Tbl', expr: "Peek('SALES_AMOUNT', -1, 'OTHER')" },
    ]);
    assert.equal(r.workbookPatterns![0].kind, 'unsupported');
    assert.match(r.workbookPatterns![0].note, /ABSOLUTE load-order/);
    assert.equal(r.workbookPatterns![1].kind, 'unsupported');
    assert.match(r.workbookPatterns![1].note, /load buffer/);
  });
});

describe('qlik inter-record: FirstSortedValue()', () => {
  test('simple agg-weight form → SQL QUALIFY helper element + Min() metric', () => {
    const r = conv([{ title: 'Top Customer', expr: 'FirstSortedValue(CUSTOMER, -Sum(SALES_AMOUNT))' }]);
    const el = r.model.pages[0].elements.find((e: any) => e.name === 'Top Customer (FirstSortedValue)') as any;
    assert.ok(el, 'helper element missing');
    assert.equal(el.source.kind, 'sql');
    assert.equal(el.source.statement,
      'SELECT "CUSTOMER" AS "fsv_value" FROM "CSA"."TJ"."SALES" GROUP BY 1 QUALIFY ROW_NUMBER() OVER (ORDER BY SUM("SALES_AMOUNT") DESC) = 1');
    assert.equal(el.columns[0].formula, '[Custom SQL/fsv_value]');
    assert.equal(el.metrics[0].formula, 'Min([Fsv Value])');
    assert.equal(el.metrics[0].name, 'Top Customer');
  });

  test('ascending weight + explicit n', () => {
    const r = conv([{ title: 'Second Smallest', expr: 'FirstSortedValue(CUSTOMER, Sum(SALES_AMOUNT), 2)' }]);
    const el = r.model.pages[0].elements.find((e: any) => /FirstSortedValue/.test(e.name || '')) as any;
    assert.match(el.source.statement, /ORDER BY SUM\("SALES_AMOUNT"\) ASC\) = 2$/);
  });

  test('row-level weight form (no aggregation)', () => {
    const r = conv([{ title: 'Cheapest Customer', expr: 'FirstSortedValue(CUSTOMER, SALES_AMOUNT)' }]);
    const el = r.model.pages[0].elements.find((e: any) => /FirstSortedValue/.test(e.name || '')) as any;
    assert.equal(el.source.statement,
      'SELECT "CUSTOMER" AS "fsv_value" FROM "CSA"."TJ"."SALES" QUALIFY ROW_NUMBER() OVER (ORDER BY "SALES_AMOUNT" ASC) = 1');
  });

  test('complex value expr → Rank=n-filter pattern with verify flag', () => {
    const r = conv([{ title: 'Top Cust Upper', expr: 'FirstSortedValue(Upper(CUSTOMER), -Sum(SALES_AMOUNT))' }]);
    const p = r.workbookPatterns![0];
    assert.equal(p.kind, 'first-sorted-value');
    assert.equal(p.verify, true);
    assert.equal(p.formula, 'If(Rank(Sum([Sales Amount]), "desc") = 1, Upper([Customer]), Null)');
  });
});

describe('qlik inter-record: master dimensions + non-regression', () => {
  test('calc dimension with Rank → pattern, no DM column', () => {
    const before = conv([]).model.pages[0].elements[0].columns.length;
    const r = conv([], [{ title: 'Region Rank', fieldDef: '=Rank(Sum(SALES_AMOUNT))' }]);
    assert.equal(r.model.pages[0].elements[0].columns.length, before, 'no column added');
    const p = r.workbookPatterns![0];
    assert.equal(p.kind, 'rank');
    assert.match(p.note, /master dimension/);
  });

  test('plain aggregate measures still become DM metrics (no patterns)', () => {
    const r = conv([{ title: 'Total Sales', expr: 'Sum(SALES_AMOUNT)' }]);
    assert.equal(allMetrics(r).length, 1);
    assert.equal(r.workbookPatterns, undefined);
  });
});
