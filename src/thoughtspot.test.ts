/**
 * ThoughtSpot converter tests — window-function → grouped-element handoff
 * (beads-sigma-5d9k) plus conditional-aggregate arg swap regression coverage.
 *
 * HARD CONSTRAINT under test: Sigma window functions silently compile to
 * error-type columns in DM element calc columns and DM metrics — they only
 * evaluate inside GROUPED elements. The converter must therefore never emit a
 * window function into a host calc column or metric; instead each becomes a
 * flagged Null placeholder + a grouped child element (the PBI time-intel
 * handoff pattern, exact-parity verified).
 */

import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { convertThoughtSpotToSigma } from './thoughtspot.js';

const FIX = join(dirname(fileURLToPath(import.meta.url)), '..', 'regression-corpus', 'thoughtspot', 'window_functions', 'input.yaml');

const result = convertThoughtSpotToSigma(readFileSync(FIX, 'utf8'), {
  connectionId: 'test-conn', database: 'CSA', schema: 'TJ',
});
const elements: any[] = result.model.pages[0].elements;
const byName = (n: string) => elements.find((e: any) => e.name === n);

describe('window functions → grouped child elements', () => {
  test('no window function ever lands in a host calc column or metric', () => {
    const winRe = /\b(CumulativeSum|CumulativeAvg|CumulativeMax|CumulativeMin|CumulativeCount|MovingAvg|MovingSum|MovingMax|MovingMin|Rank|Lag|Lead|First|Last)\s*\(/;
    for (const el of elements) {
      if (el.groupings) continue; // grouped emission elements are the one allowed home
      for (const c of (el.columns || [])) {
        assert.ok(!winRe.test(c.formula || ''), `window fn leaked into calc col "${c.name}" on "${el.name}": ${c.formula}`);
      }
      for (const m of (el.metrics || [])) {
        assert.ok(!winRe.test(m.formula || ''), `window fn leaked into metric "${m.name}" on "${el.name}": ${m.formula}`);
      }
    }
  });

  test('cumulative_sum → grouped CumulativeSum element on host', () => {
    const el = byName('Revenue Running');
    assert.ok(el?.groupings?.length, 'grouped element missing');
    assert.equal(el.source.kind, 'table');
    const win = el.columns.find((c: any) => c.name === 'Revenue Running');
    assert.equal(win.formula, 'CumulativeSum([Net Revenue])');
    const val = el.columns.find((c: any) => c.name === 'Net Revenue');
    assert.equal(val.formula, 'Sum([Order Fact/Net Revenue])');
    const dim = el.columns.find((c: any) => c.name === 'Order Date Key');
    assert.equal(dim.formula, '[Order Fact/Order Date Key]');
    // grouping carries the dim; calcs carry agg + window
    assert.deepEqual(el.groupings[0].groupBy, [dim.id]);
    assert.deepEqual(el.groupings[0].calculations.sort(), [val.id, win.id].sort());
  });

  test('moving_average(m, 2, 1, dim) → MovingAvg([v], 2, 1)', () => {
    const el = byName('Revenue Moving Avg');
    const win = el.columns.find((c: any) => c.name === 'Revenue Moving Avg');
    assert.equal(win.formula, 'MovingAvg([Net Revenue], 2, 1)');
  });

  test('rank_desc with cross-element dim groups on the derived view', () => {
    const el = byName('Region Revenue Rank');
    assert.ok(el?.groupings?.length);
    const view = byName('Order Fact View');
    assert.ok(view, 'derived view missing');
    assert.equal(el.source.elementId, view.id, 'must source the derived join view');
    const win = el.columns.find((c: any) => c.name === 'Region Revenue Rank');
    assert.equal(win.formula, 'Rank([Region Revenue Rank Base], "desc")');
    const dim = el.columns.find((c: any) => c.name === 'Region');
    assert.equal(dim.formula, '[Order Fact View/Region (CUSTOMER_DIM)]');
    const val = el.columns.find((c: any) => c.name === 'Region Revenue Rank Base');
    assert.equal(val.formula, 'Sum([Order Fact View/Net Revenue])');
  });

  test('lag(m, dim, 1) → Lag([v], 1) grouped by dim', () => {
    const el = byName('Prev Period Revenue');
    const win = el.columns.find((c: any) => c.name === 'Prev Period Revenue');
    assert.equal(win.formula, 'Lag([Net Revenue], 1)');
  });

  test('host carries flagged Null placeholders with re-author descriptions', () => {
    const host = byName('Order Fact');
    for (const name of ['Revenue Running', 'Revenue Moving Avg', 'Region Revenue Rank', 'Prev Period Revenue', 'Running Pct']) {
      const c = host.columns.find((x: any) => x.name === name);
      assert.ok(c, `placeholder "${name}" missing on host`);
      assert.equal(c.formula, 'Null');
      assert.match(c.description || '', /window function/i);
    }
  });

  test('embedded window usage degrades flag-only (no grouped element)', () => {
    assert.equal(byName('Running Pct'), undefined, 'embedded usage must not emit a grouped element');
    assert.ok(result.warnings.some(w => w.includes('Running Pct') && /embedded/.test(w)));
  });

  test('`unique count` (two-word) → CountDistinct metric', () => {
    const host = byName('Order Fact');
    const m = host.metrics.find((x: any) => x.name === 'Distinct Customers');
    assert.equal(m.formula, 'CountDistinct([Customer Key])');
  });
});

describe('conditional aggregates (regression: arg swap stays intact)', () => {
  test('sum_if(cond, measure) swaps to SumIf(measure, cond)', () => {
    const yaml = readFileSync(FIX, 'utf8').replace(
      'expr: "cumulative_sum([order_fact_1::NET_REVENUE], [order_fact_1::ORDER_DATE_KEY])"',
      'expr: "sum_if([customer_dim_1::REGION] = \'West\', [order_fact_1::NET_REVENUE])"');
    const r = convertThoughtSpotToSigma(yaml, { connectionId: 'c', database: 'CSA', schema: 'TJ' });
    const els: any[] = r.model.pages[0].elements;
    const all = els.flatMap((e: any) => [...(e.columns || []), ...(e.metrics || [])]);
    const hit = all.find((c: any) => /SumIf\(/.test(c.formula || ''));
    assert.ok(hit, 'SumIf emission missing');
    assert.match(hit.formula, /SumIf\(\[.*Net Revenue\], \[.*Region.*\] = "West"\)/);
  });
});
