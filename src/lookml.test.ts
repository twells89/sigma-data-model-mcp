/**
 * LookML converter tests — uses the real Converter Files/Looker fixtures.
 *
 * Sections:
 *   Regression  — existing behaviour that must not regress
 *   Gap 1       — ${view.SQL_TABLE_NAME} resolution (beads-sigma-ali, P1)
 *   Gap 2       — include: directive warnings (beads-sigma-z6x, P2)
 *   Gap 3       — PDT property warnings (beads-sigma-6jo, P3)
 *
 * Gap tests are marked with a comment so it is easy to locate them once the
 * corresponding fix lands.
 */

import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { convertLookMLToSigma } from './lookml.js';

const LOOKML_DIR = '/Users/tjwells/Desktop/Converter Files/Looker/';

function lkml(filename: string): { name: string; content: string } {
  return { name: filename, content: readFileSync(LOOKML_DIR + filename, 'utf8') };
}

const retailFiles = [
  'retail_analytics.model.lkml',
  'order_fact.view.lkml',
  'customer_dim.view.lkml',
  'product_dim.view.lkml',
  'store_dim.view.lkml',
  'date_dim.view.lkml',
  'promo_dim.view.lkml',
  'vip_customer.view.lkml',
  'monthly_revenue_summary.view.lkml',
].map(lkml);

const twohopFiles = [
  'twohop.model.lkml',
  'order_fact.view.lkml',
  'order_fact_alias.view.lkml',
  'order_fact_double_alias.view.lkml',
  'channel_twohop_derived.view.lkml',
].map(lkml);

const pdtSelfJoinFiles = [
  'pdt_self_join.model.lkml',
  'order_fact_pdt.view.lkml',
  'pdt_self_join_derived.view.lkml',
].map(lkml);

// ── Regression ────────────────────────────────────────────────────────────────

describe('regression: retail_analytics star schema (order_fact explore)', () => {
  test('converts without parse errors', () => {
    const { warnings } = convertLookMLToSigma(retailFiles, {
      exploreName: 'order_fact',
      connectionId: 'test-conn',
    });
    const fatal = warnings.filter(w => /parse error|exception/i.test(w));
    assert.equal(fatal.length, 0, `Fatal warnings: ${fatal.join('; ')}`);
  });

  test('produces elements for all 8 joined views', () => {
    const { model } = convertLookMLToSigma(retailFiles, {
      exploreName: 'order_fact',
      connectionId: 'test-conn',
    });
    // base + customer + product + 2×store + 3×date + promo = 9 warehouse elements
    // (role-playing views share physical elements, so deduplicated count >= 5)
    const warehouseEls = model.pages[0].elements.filter(e => e.source?.kind === 'warehouse-table');
    assert.ok(warehouseEls.length >= 5, `Expected ≥5 warehouse elements, got ${warehouseEls.length}`);
  });

  test('order_fact element path is [CSA, TJ, ORDER_FACT]', () => {
    const { model } = convertLookMLToSigma(retailFiles, {
      exploreName: 'order_fact',
      connectionId: 'test-conn',
    });
    const el = model.pages[0].elements.find(
      e => e.source?.kind === 'warehouse-table' &&
           JSON.stringify(e.source.path) === JSON.stringify(['CSA', 'TJ', 'ORDER_FACT'])
    );
    assert.ok(el, 'No element with path [CSA, TJ, ORDER_FACT]');
  });

  test('order_fact element has ≥5 relationships (role-playing joins are skipped)', () => {
    const { model } = convertLookMLToSigma(retailFiles, {
      exploreName: 'order_fact',
      connectionId: 'test-conn',
    });
    const factEl = model.pages[0].elements.find(
      e => e.source?.kind === 'warehouse-table' &&
           JSON.stringify(e.source.path) === JSON.stringify(['CSA', 'TJ', 'ORDER_FACT'])
    );
    assert.ok(factEl?.relationships && factEl.relationships.length >= 5,
      `Expected ≥5 relationships, got ${factEl?.relationships?.length}`);
  });
});

describe('regression: monthly_revenue_summary derived table', () => {
  test('produces a Custom SQL element', () => {
    const { model } = convertLookMLToSigma(retailFiles, {
      exploreName: 'monthly_revenue_summary',
      connectionId: 'test-conn',
    });
    const sqlEl = model.pages[0].elements.find(e => e.source?.kind === 'sql');
    assert.ok(sqlEl, 'No Custom SQL element found');
  });

  test('SQL statement contains the literal table reference (no ${TABLE} leftover)', () => {
    const { model } = convertLookMLToSigma(retailFiles, {
      exploreName: 'monthly_revenue_summary',
      connectionId: 'test-conn',
    });
    const sqlEl = model.pages[0].elements.find(e => e.source?.kind === 'sql')!;
    const stmt: string = sqlEl.source.statement;
    assert.ok(!stmt.includes('${TABLE}'), '${TABLE} ref not stripped from SQL statement');
    assert.ok(stmt.includes('CSA.TJ.ORDER_FACT'), 'SQL statement missing literal table reference');
  });

  test('simple warehouse-column dims produce 12 columns in the Custom SQL element', () => {
    // Same-view field refs (${dim_name}) are tracked separately in beads-sigma-u7e.
    // This test confirms the simple ${TABLE}.COLUMN dims work correctly.
    const { model } = convertLookMLToSigma(retailFiles, {
      exploreName: 'monthly_revenue_summary',
      connectionId: 'test-conn',
    });
    const sqlEl = model.pages[0].elements.find(e => e.source?.kind === 'sql')!;
    assert.equal(sqlEl.columns.length, 12,
      `Expected 12 simple columns, got ${sqlEl.columns.length}`);
  });
});

describe('regression: vip_customers explore (always_filter)', () => {
  test('emits a warning for always_filter conversion', () => {
    const { warnings } = convertLookMLToSigma(retailFiles, {
      exploreName: 'vip_customers',
      connectionId: 'test-conn',
    });
    const filterWarning = warnings.find(w => /always_filter/i.test(w));
    assert.ok(filterWarning, `Expected an always_filter warning, got:\n${warnings.join('\n')}`);
  });
});

// ── Gap 1: ${view.SQL_TABLE_NAME} resolution (beads-sigma-ali) ────────────────

describe('gap-1: one-hop sql_table_name alias resolution', () => {
  // order_fact_alias.sql_table_name = ${order_fact.SQL_TABLE_NAME}
  // order_fact.sql_table_name = CSA.TJ.ORDER_FACT
  // Expected: warehouse-table element with path ['CSA', 'TJ', 'ORDER_FACT']

  test('element source kind is warehouse-table (not sql)', () => {
    const { model } = convertLookMLToSigma(twohopFiles, {
      exploreName: 'order_fact_alias',
      connectionId: 'test-conn',
    });
    const el = model.pages[0].elements[0];
    assert.equal(el.source.kind, 'warehouse-table', `Got kind=${el.source.kind}`);
  });

  test('element path resolves to [CSA, TJ, ORDER_FACT]', () => {
    const { model } = convertLookMLToSigma(twohopFiles, {
      exploreName: 'order_fact_alias',
      connectionId: 'test-conn',
    });
    const el = model.pages[0].elements.find(e => e.source?.kind === 'warehouse-table');
    assert.ok(el, 'No warehouse-table element');
    assert.deepEqual(el.source.path, ['CSA', 'TJ', 'ORDER_FACT'],
      `Got path ${JSON.stringify(el.source.path)}`);
  });

  test('no unresolved ${...} refs in element source', () => {
    const { model } = convertLookMLToSigma(twohopFiles, {
      exploreName: 'order_fact_alias',
      connectionId: 'test-conn',
    });
    const src = JSON.stringify(model.pages[0].elements[0].source);
    assert.ok(!src.includes('${'), `Unresolved ref in source: ${src}`);
  });
});

describe('gap-1: two-hop sql_table_name chain resolution', () => {
  // order_fact_double_alias → ${order_fact_alias.SQL_TABLE_NAME}
  //   → ${order_fact.SQL_TABLE_NAME} → CSA.TJ.ORDER_FACT

  test('two-hop chain resolves to [CSA, TJ, ORDER_FACT]', () => {
    const { model } = convertLookMLToSigma(twohopFiles, {
      exploreName: 'order_fact_double_alias',
      connectionId: 'test-conn',
    });
    const el = model.pages[0].elements.find(e => e.source?.kind === 'warehouse-table');
    assert.ok(el, 'No warehouse-table element');
    assert.deepEqual(el.source.path, ['CSA', 'TJ', 'ORDER_FACT'],
      `Two-hop chain did not resolve — got ${JSON.stringify(el.source.path)}`);
  });
});

describe('gap-1: ${view.SQL_TABLE_NAME} inside derived table SQL', () => {
  // channel_twohop_derived SQL references ${order_fact_alias.SQL_TABLE_NAME}
  //   → order_fact_alias → ${order_fact.SQL_TABLE_NAME} → CSA.TJ.ORDER_FACT
  // The literal string 'CSA.TJ.ORDER_FACT' must appear in the resolved SQL statement.

  test('produces a Custom SQL element', () => {
    const { model } = convertLookMLToSigma(twohopFiles, {
      exploreName: 'channel_twohop_derived',
      connectionId: 'test-conn',
    });
    const sqlEl = model.pages[0].elements.find(e => e.source?.kind === 'sql');
    assert.ok(sqlEl, 'No Custom SQL element found');
  });

  test('SQL statement contains resolved literal path CSA.TJ.ORDER_FACT', () => {
    const { model } = convertLookMLToSigma(twohopFiles, {
      exploreName: 'channel_twohop_derived',
      connectionId: 'test-conn',
    });
    const sqlEl = model.pages[0].elements.find(e => e.source?.kind === 'sql')!;
    const stmt: string = sqlEl.source.statement;
    assert.ok(stmt.includes('CSA.TJ.ORDER_FACT'),
      `Statement missing resolved path — got:\n${stmt}`);
  });

  test('SQL statement has no unresolved SQL_TABLE_NAME token', () => {
    const { model } = convertLookMLToSigma(twohopFiles, {
      exploreName: 'channel_twohop_derived',
      connectionId: 'test-conn',
    });
    const stmt: string = model.pages[0].elements
      .find(e => e.source?.kind === 'sql')!.source.statement;
    assert.ok(!stmt.includes('SQL_TABLE_NAME'),
      `Statement still contains unresolved SQL_TABLE_NAME:\n${stmt}`);
  });

  test('SQL statement has no unresolved ${...} refs', () => {
    const { model } = convertLookMLToSigma(twohopFiles, {
      exploreName: 'channel_twohop_derived',
      connectionId: 'test-conn',
    });
    const stmt: string = model.pages[0].elements
      .find(e => e.source?.kind === 'sql')!.source.statement;
    assert.ok(!stmt.includes('${'),
      `Statement still has raw \${} refs:\n${stmt}`);
  });
});

// ── Gap 1 (customer case): PDT referencing PDT twice with different aliases ───

describe('gap-1: PDT-references-PDT self-join (customer pattern)', () => {
  // pdt_self_join_derived SQL references ${order_fact_pdt.SQL_TABLE_NAME} TWICE:
  //   FROM ${order_fact_pdt.SQL_TABLE_NAME} a LEFT JOIN ${order_fact_pdt.SQL_TABLE_NAME} b ON ...
  // order_fact_pdt is itself a PDT (derived_table.sql), not a warehouse table.
  // The inline subquery must NOT add AS viewName — the existing aliases (a, b) must stand.

  test('produces a Custom SQL element', () => {
    const { model } = convertLookMLToSigma(pdtSelfJoinFiles, {
      exploreName: 'pdt_self_join_derived',
      connectionId: 'test-conn',
    });
    const sqlEl = model.pages[0].elements.find((e: any) => e.source?.kind === 'sql');
    assert.ok(sqlEl, 'No Custom SQL element found');
  });

  test('SQL statement contains no ${...} refs', () => {
    const { model } = convertLookMLToSigma(pdtSelfJoinFiles, {
      exploreName: 'pdt_self_join_derived',
      connectionId: 'test-conn',
    });
    const stmt: string = model.pages[0].elements
      .find((e: any) => e.source?.kind === 'sql')!.source.statement;
    assert.ok(!stmt.includes('${'), `Unresolved refs remain:\n${stmt}`);
  });

  test('SQL statement contains no SQL_TABLE_NAME token', () => {
    const { model } = convertLookMLToSigma(pdtSelfJoinFiles, {
      exploreName: 'pdt_self_join_derived',
      connectionId: 'test-conn',
    });
    const stmt: string = model.pages[0].elements
      .find((e: any) => e.source?.kind === 'sql')!.source.statement;
    assert.ok(!stmt.includes('SQL_TABLE_NAME'), `SQL_TABLE_NAME not resolved:\n${stmt}`);
  });

  test('PDT SQL is inlined without a spurious AS alias (aliases a and b are preserved)', () => {
    const { model } = convertLookMLToSigma(pdtSelfJoinFiles, {
      exploreName: 'pdt_self_join_derived',
      connectionId: 'test-conn',
    });
    const stmt: string = model.pages[0].elements
      .find((e: any) => e.source?.kind === 'sql')!.source.statement;
    // The subquery must end with ") a" or ") b", not ") AS order_fact_pdt a"
    const spuriousAlias = new RegExp('\\) AS order_fact_pdt').test(stmt);
    assert.ok(!spuriousAlias, `Spurious AS alias found — invalid SQL:\n${stmt}`);
    assert.ok(stmt.includes(') a'), `Expected ") a" alias after first subquery:\n${stmt}`);
    assert.ok(stmt.includes(') b'), `Expected ") b" alias after second subquery:\n${stmt}`);
  });

  test('PDT SQL contains the physical table CSA.TJ.ORDER_FACT', () => {
    const { model } = convertLookMLToSigma(pdtSelfJoinFiles, {
      exploreName: 'pdt_self_join_derived',
      connectionId: 'test-conn',
    });
    const stmt: string = model.pages[0].elements
      .find((e: any) => e.source?.kind === 'sql')!.source.statement;
    assert.ok(stmt.includes('CSA.TJ.ORDER_FACT'), `Physical table not found in SQL:\n${stmt}`);
  });
});

// ── Gap 2: include: directive warnings (beads-sigma-z6x) ─────────────────────

describe('gap-2: include: directive warnings', () => {
  const modelWithIncludes = {
    name: 'includes_test.model.lkml',
    content: `
      connection: "test"
      include: "/Other/*"
      include: "/views/*.view"
      explore: order_fact { label: "Orders" }
    `,
  };

  test('does not throw when include: directives are present', () => {
    assert.doesNotThrow(() => {
      convertLookMLToSigma([modelWithIncludes, lkml('order_fact.view.lkml')], {
        exploreName: 'order_fact',
        connectionId: 'test-conn',
      });
    });
  });

  test('emits a warning that lists the encountered include path(s)', () => {
    const { warnings } = convertLookMLToSigma(
      [modelWithIncludes, lkml('order_fact.view.lkml')],
      { exploreName: 'order_fact', connectionId: 'test-conn' }
    );
    const w = warnings.find(w => /include/i.test(w) && w.includes('/Other/*'));
    assert.ok(w, `Expected include: warning with path, got:\n${warnings.join('\n')}`);
  });
});

// ── Gap 3: PDT property warnings (beads-sigma-6jo) ───────────────────────────

describe('gap-3: PDT property warnings (distribution, sortkeys, datagroup_trigger)', () => {
  const pdtView = {
    name: 'subscription_payments.view.lkml',
    content: `
      view: subscription_payments {
        derived_table: {
          sql: SELECT id, amount FROM payments WHERE status = 'active' ;;
          distribution: "subscription_id"
          sortkeys: ["payment_date"]
          datagroup_trigger: 1hrs
        }
        dimension: id { type: string sql: \${TABLE}.id ;; }
        measure: total { type: sum sql: \${TABLE}.amount ;; }
      }
    `,
  };
  const pdtModel = {
    name: 'subscription.model.lkml',
    content: `
      connection: "test"
      explore: subscription_payments { label: "Payments" }
    `,
  };

  test('emits a warning for distribution:', () => {
    const { warnings } = convertLookMLToSigma([pdtModel, pdtView], {
      exploreName: 'subscription_payments',
      connectionId: 'test-conn',
    });
    const w = warnings.find(w => /distribution/i.test(w));
    assert.ok(w, `Expected distribution warning, got:\n${warnings.join('\n')}`);
  });

  test('emits a warning for sortkeys:', () => {
    const { warnings } = convertLookMLToSigma([pdtModel, pdtView], {
      exploreName: 'subscription_payments',
      connectionId: 'test-conn',
    });
    const w = warnings.find(w => /sortkeys?/i.test(w));
    assert.ok(w, `Expected sortkeys warning, got:\n${warnings.join('\n')}`);
  });

  test('emits a warning for datagroup_trigger:', () => {
    const { warnings } = convertLookMLToSigma([pdtModel, pdtView], {
      exploreName: 'subscription_payments',
      connectionId: 'test-conn',
    });
    const w = warnings.find(w => /datagroup/i.test(w));
    assert.ok(w, `Expected datagroup_trigger warning, got:\n${warnings.join('\n')}`);
  });
});
