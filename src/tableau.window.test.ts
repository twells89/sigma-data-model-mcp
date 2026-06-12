/**
 * Tableau table-calc → Sigma window-function tests (WINPROBE-validated
 * mappings, live-proven 930/930).
 *
 * Mappings under test:
 *   RUNNING_SUM/AVG/MAX/MIN/COUNT(agg)        → Cumulative*(agg)
 *   WINDOW_AVG/SUM/MAX/MIN(agg, -n, 0)        → Moving*(agg, n)
 *   WINDOW_*(agg, -n, m)                      → Moving*(agg, n, m)
 *   WINDOW_STDEV(agg, -n, m)                  → MovingStdDev(agg, n[, m])
 *   SUM(x) / WINDOW_SUM(SUM(x))               → PercentOfTotal(Sum(x), "grand_total")
 *   RUNNING_SUM(agg) / TOTAL(agg)             → CumulativeSum(PercentOfTotal(agg, "grand_total"))
 *   RANK/RANK_DENSE/RANK_PERCENTILE(agg)      → Rank/RankDense/RankPercentile(agg, "desc")
 *   INDEX()                                   → RowNumber()
 *   LOOKUP(agg, ±n)                           → Lag/Lead(agg, n)
 *
 * Context contract: these Sigma window functions are valid ONLY in CHART /
 * grouped-workbook-element context (they silently error in DM element calc
 * columns and workbook master calc columns), so the DM converter must report
 * them in result.workbookPatterns — never emit them as DM columns/metrics.
 * NEVER emit *Over functions (SumOver/MaxOver/CountOver = 'Unknown function'
 * in spec contexts). Untranslatable (loud warning naming the fragment):
 * WINDOW_MEDIAN/PERCENTILE/CORR/COVAR, PREVIOUS_VALUE, SIZE().
 */

import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import {
  tableauFormulaToSigma, tableauWindowToSigmaChart, tableauWindowUntranslatable,
  SIGMA_CHART_ONLY_WINDOW_RE,
} from './formulas.js';
import { convertTableauToSigma } from './tableau.js';

function conv(formula: string): { out: string; warnings: string[] } {
  const warnings: string[] = [];
  const out = tableauFormulaToSigma(formula, warnings);
  return { out, warnings };
}

const NO_OVER_RE = /\b\w+Over\s*\(/;

describe('tableau window fns: RUNNING_* → Cumulative*', () => {
  const cases: [string, string][] = [
    ['RUNNING_SUM(SUM([SALES]))', 'CumulativeSum(Sum([Sales]))'],
    ['RUNNING_AVG(AVG([SALES]))', 'CumulativeAvg(Avg([Sales]))'],
    ['RUNNING_MAX(MAX([PROFIT]))', 'CumulativeMax(Max([Profit]))'],
    ['RUNNING_MIN(MIN([PROFIT]))', 'CumulativeMin(Min([Profit]))'],
    ['RUNNING_COUNT(COUNT([ORDER_ID]))', 'CumulativeCount(Count([Order Id]))'],
    // mixed inner agg passes through
    ['RUNNING_SUM(AVG([SALES]))', 'CumulativeSum(Avg([Sales]))'],
    // bare-column form — inner aggregate implied by the outer fn
    ['RUNNING_SUM([SALES])', 'CumulativeSum(Sum([Sales]))'],
  ];
  for (const [src, expected] of cases) {
    test(`${src} → ${expected}`, () => {
      const { out, warnings } = conv(src);
      assert.equal(out, expected);
      assert.ok(!NO_OVER_RE.test(out), `must never emit *Over: ${out}`);
      assert.ok(warnings.some(w => /CHART\/grouped-element context ONLY/.test(w)),
        'must warn about chart-only context');
    });
  }
});

describe('tableau window fns: WINDOW_*(agg, -n, m) → Moving*', () => {
  const cases: [string, string][] = [
    ['WINDOW_AVG(SUM([SALES]), -2, 0)', 'MovingAvg(Sum([Sales]), 2)'],
    ['WINDOW_SUM(SUM([SALES]), -3, 0)', 'MovingSum(Sum([Sales]), 3)'],
    ['WINDOW_MAX(MAX([SALES]), -6, 0)', 'MovingMax(Max([Sales]), 6)'],
    ['WINDOW_MIN(MIN([SALES]), -6, 0)', 'MovingMin(Min([Sales]), 6)'],
    ['WINDOW_AVG(SUM([SALES]), -2, 2)', 'MovingAvg(Sum([Sales]), 2, 2)'],
    ['WINDOW_STDEV(SUM([SALES]), -5, 0)', 'MovingStdDev(Sum([Sales]), 5)'],
    ['WINDOW_STDEV(SUM([SALES]), -5, 1)', 'MovingStdDev(Sum([Sales]), 5, 1)'],
  ];
  for (const [src, expected] of cases) {
    test(`${src} → ${expected}`, () => {
      const { out } = conv(src);
      assert.equal(out, expected);
      assert.ok(!NO_OVER_RE.test(out), `must never emit *Over: ${out}`);
    });
  }
});

describe('tableau window fns: percent-of-total ratios', () => {
  test('SUM(x) / WINDOW_SUM(SUM(x)) → PercentOfTotal(Sum(x), "grand_total")', () => {
    const { out } = conv('SUM([SALES]) / WINDOW_SUM(SUM([SALES]))');
    assert.equal(out, 'PercentOfTotal(Sum([Sales]), "grand_total")');
  });
  test('RUNNING_SUM(agg) / TOTAL(agg) → CumulativeSum(PercentOfTotal(agg, "grand_total"))', () => {
    const { out } = conv('RUNNING_SUM(SUM([SALES])) / TOTAL(SUM([SALES]))');
    assert.equal(out, 'CumulativeSum(PercentOfTotal(Sum([Sales]), "grand_total"))');
  });
  test('RUNNING_SUM(agg) / WINDOW_SUM(agg) — same semantic, also accepted', () => {
    const { out } = conv('RUNNING_SUM(SUM([SALES])) / WINDOW_SUM(SUM([SALES]))');
    assert.equal(out, 'CumulativeSum(PercentOfTotal(Sum([Sales]), "grand_total"))');
  });
  test('mismatched column does NOT claim the ratio pattern', () => {
    const m = tableauWindowToSigmaChart('SUM([SALES]) / WINDOW_SUM(SUM([PROFIT]))');
    assert.equal(m, null);
  });
});

describe('tableau window fns: RANK family', () => {
  test('RANK(SUM(x)) → Rank(Sum(x), "desc")', () => {
    assert.equal(conv('RANK(SUM([SALES]))').out, 'Rank(Sum([Sales]), "desc")');
  });
  test("RANK(SUM(x), 'asc') honors direction", () => {
    assert.equal(conv("RANK(SUM([SALES]), 'asc')").out, 'Rank(Sum([Sales]), "asc")');
  });
  test('RANK_DENSE → RankDense (NOT DenseRank)', () => {
    assert.equal(conv('RANK_DENSE(SUM([SALES]))').out, 'RankDense(Sum([Sales]), "desc")');
  });
  test('RANK_PERCENTILE → RankPercentile', () => {
    assert.equal(conv('RANK_PERCENTILE(SUM([SALES]))').out, 'RankPercentile(Sum([Sales]), "desc")');
  });
  test('RANK_UNIQUE → Rank with verify note (tie semantics differ)', () => {
    const m = tableauWindowToSigmaChart('RANK_UNIQUE(SUM([SALES]))');
    assert.equal(m?.formula, 'Rank(Sum([Sales]), "desc")');
    assert.equal(m?.verify, true);
  });
});

describe('tableau window fns: INDEX / LOOKUP', () => {
  test('INDEX() → RowNumber()', () => {
    assert.equal(conv('INDEX()').out, 'RowNumber()');
  });
  test('LOOKUP(agg, -1) → Lag(agg, 1)', () => {
    assert.equal(conv('LOOKUP(SUM([SALES]), -1)').out, 'Lag(Sum([Sales]), 1)');
  });
  test('LOOKUP(agg, 2) → Lead(agg, 2)', () => {
    assert.equal(conv('LOOKUP(SUM([SALES]), 2)').out, 'Lead(Sum([Sales]), 2)');
  });
  test('LOOKUP(agg, 0) is the identity', () => {
    assert.equal(conv('LOOKUP(SUM([SALES]), 0)').out, 'Sum([Sales])');
  });
});

describe('tableau window fns: untranslatable — loud, never silent', () => {
  const cases = [
    ['WINDOW_MEDIAN(MEDIAN([SALES]))', 'WINDOW_MEDIAN'],
    ['WINDOW_PERCENTILE(SUM([SALES]), 0.75)', 'WINDOW_PERCENTILE'],
    ['WINDOW_CORR(SUM([SALES]), SUM([PROFIT]))', 'WINDOW_CORR'],
    ['WINDOW_COVAR(SUM([SALES]), SUM([PROFIT]))', 'WINDOW_COVAR'],
    ['PREVIOUS_VALUE(0)', 'PREVIOUS_VALUE'],
    ['SIZE()', 'SIZE'],
  ] as const;
  for (const [src, fn] of cases) {
    test(`${fn} → loud warning naming the fragment, comment placeholder`, () => {
      assert.equal(tableauWindowUntranslatable(src), fn);
      assert.equal(tableauWindowToSigmaChart(src), null, 'must not claim a formula');
      const { out, warnings } = conv(src);
      assert.ok(out.startsWith('/*'), `must degrade to comment, got: ${out}`);
      const warn = warnings.find(w => w.includes(fn));
      assert.ok(warn, `warning must name ${fn}: ${warnings.join(' | ')}`);
      assert.match(warn!, /fragment/i, 'warning must include the untranslated fragment');
    });
  }
  test('embedded table-calc token in a larger expression is flagged', () => {
    const { warnings } = conv('1 + RUNNING_SUM(SUM([SALES]))');
    assert.ok(warnings.some(w => /embedded in a larger expression/.test(w)),
      `expected embedded-fragment warning: ${warnings.join(' | ')}`);
  });
});

// ── Converter-level: workbookPatterns handoff + DM-column guard ─────────────

const TWB = `<?xml version='1.0' encoding='utf-8' ?>
<workbook source-build='2024.1' version='18.1' xmlns:user='http://www.tableausoftware.com/xml/user'>
  <datasources>
    <datasource caption='Superstore Orders' inline='true' name='federated.superstore' version='18.1'>
      <connection class='federated'>
        <named-connections>
          <named-connection caption='Snowflake' name='snowflake.0' />
        </named-connections>
        <relation connection='snowflake.0' name='SUPERSTORE_ORDERS' table='[TJ].[PUBLIC].[SUPERSTORE_ORDERS]' type='table'>
          <columns>
            <column datatype='string' name='REGION' ordinal='1' />
            <column datatype='date' name='ORDER_DATE' ordinal='2' />
            <column datatype='real' name='SALES' ordinal='3' />
            <column datatype='real' name='PROFIT' ordinal='4' />
          </columns>
        </relation>
      </connection>
      <column caption='Sales 3mo Moving Avg' datatype='real' name='[Calc_MovAvg]' role='measure' type='quantitative'>
        <calculation class='tableau' formula='WINDOW_AVG(SUM([SALES]), -2, 0)' />
      </column>
      <column caption='Pct of Total Sales' datatype='real' name='[Calc_PctTotal]' role='measure' type='quantitative'>
        <calculation class='tableau' formula='SUM([SALES]) / WINDOW_SUM(SUM([SALES]))' />
      </column>
      <column caption='Sales Rank Percentile' datatype='real' name='[Calc_RankPctl]' role='measure' type='quantitative'>
        <calculation class='tableau' formula='RANK_PERCENTILE(SUM([SALES]))' />
      </column>
      <column caption='Median Window' datatype='real' name='[Calc_WinMedian]' role='measure' type='quantitative'>
        <calculation class='tableau' formula='WINDOW_MEDIAN(MEDIAN([SALES]))' />
      </column>
      <column caption='Partition Size' datatype='integer' name='[Calc_Size]' role='measure' type='quantitative'>
        <calculation class='tableau' formula='SIZE()' />
      </column>
      <column caption='Prev Accumulator' datatype='real' name='[Calc_PrevVal]' role='measure' type='quantitative'>
        <calculation class='tableau' formula='PREVIOUS_VALUE(SUM([SALES]))' />
      </column>
    </datasource>
  </datasources>
</workbook>`;

describe('tableau converter: chart-context window calcs → workbookPatterns, never DM columns', () => {
  const r: any = convertTableauToSigma(TWB, { connectionId: 'conn-1', database: 'TJ', schema: 'PUBLIC' });
  const allCols = r.model.pages.flatMap((p: any) => p.elements).flatMap((e: any) => e.columns || []);
  const allMetrics = r.model.pages.flatMap((p: any) => p.elements).flatMap((e: any) => e.metrics || []);

  test('no chart-only window function leaks into any DM column or metric', () => {
    for (const c of [...allCols, ...allMetrics]) {
      assert.ok(!SIGMA_CHART_ONLY_WINDOW_RE.test(c.formula || ''),
        `chart-only window fn leaked into DM: ${c.name}: ${c.formula}`);
      assert.ok(!NO_OVER_RE.test(c.formula || ''),
        `*Over fn leaked into DM: ${c.name}: ${c.formula}`);
      assert.ok(!/WINDOW_|RUNNING_|RANK_|PREVIOUS_VALUE|SIZE\s*\(/.test(c.formula || ''),
        `raw table-calc token leaked into DM: ${c.name}: ${c.formula}`);
    }
  });

  test('translatable patterns are reported with ready formulas', () => {
    const byName = Object.fromEntries((r.workbookPatterns || []).map((p: any) => [p.name, p]));
    assert.equal(byName['Sales 3mo Moving Avg']?.formula, 'MovingAvg(Sum([Sales]), 2)');
    assert.equal(byName['Sales 3mo Moving Avg']?.kind, 'moving');
    assert.equal(byName['Pct of Total Sales']?.formula, 'PercentOfTotal(Sum([Sales]), "grand_total")');
    assert.equal(byName['Pct of Total Sales']?.kind, 'percent-of-total');
    assert.equal(byName['Sales Rank Percentile']?.formula, 'RankPercentile(Sum([Sales]), "desc")');
    assert.equal(byName['Sales Rank Percentile']?.kind, 'rank');
    for (const n of ['Sales 3mo Moving Avg', 'Pct of Total Sales', 'Sales Rank Percentile']) {
      assert.match(byName[n]?.requires || '', /GROUPED workbook element/);
    }
  });

  test('untranslatable table calcs are loud unsupported patterns naming the fragment', () => {
    const byName = Object.fromEntries((r.workbookPatterns || []).map((p: any) => [p.name, p]));
    for (const [name, fn] of [
      ['Median Window', 'WINDOW_MEDIAN'],
      ['Partition Size', 'SIZE'],
      ['Prev Accumulator', 'PREVIOUS_VALUE'],
    ] as const) {
      assert.equal(byName[name]?.kind, 'unsupported', `${name} must be flagged unsupported`);
      assert.ok(!byName[name]?.formula, `${name} must not claim a formula`);
      assert.ok(r.warnings.some((w: string) => w.includes(fn) && w.includes('⚠')),
        `loud warning naming ${fn} required`);
    }
  });

  test('warnings tell the user where the patterns went', () => {
    assert.ok(r.warnings.some((w: string) => w.includes('workbookPatterns')));
  });
});
