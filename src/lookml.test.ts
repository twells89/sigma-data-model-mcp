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

  test('PDT is inlined as a single WITH CTE; aliases a and b are preserved', () => {
    const { model } = convertLookMLToSigma(pdtSelfJoinFiles, {
      exploreName: 'pdt_self_join_derived',
      connectionId: 'test-conn',
    });
    const stmt: string = model.pages[0].elements
      .find((e: any) => e.source?.kind === 'sql')!.source.statement;
    // The PDT body must land exactly once, as a named CTE — not be duplicated
    // inline per reference — and the customer's own aliases (a, b) must stand.
    const spuriousAlias = new RegExp('\\) AS order_fact_pdt').test(stmt);
    assert.ok(!spuriousAlias, `Spurious AS alias found — invalid SQL:\n${stmt}`);
    assert.match(stmt, /^\s*WITH\s+order_fact_pdt\s+AS\s*\(/i, `Expected a WITH order_fact_pdt AS ( CTE prelude:\n${stmt}`);
    const bodyCount = (stmt.match(/FROM CSA\.TJ\.ORDER_FACT/gi) || []).length;
    assert.equal(bodyCount, 1, `PDT body should be inlined exactly once (CTE), found ${bodyCount}:\n${stmt}`);
    assert.match(stmt, /order_fact_pdt\s+a\b/, `Expected "order_fact_pdt a" reference:\n${stmt}`);
    assert.match(stmt, /order_fact_pdt\s+b\b/, `Expected "order_fact_pdt b" reference:\n${stmt}`);
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

// ── Bug fixes (thelook regression) ──────────────────────────────────────────
// Bug 1: measure ${dimension} refs were not resolved → literal ${...} leaked
//        into formulas + phantom columns. Bug 2: snowflake (multi-hop) joins
//        whose FK lives on a joined view were wired off the base element with
//        bogus base.id = target.id keys. Inline LookML mirrors thelook's shape.
describe('bugfix: measure ${dim} refs + snowflake join wiring', () => {
  const model = {
    name: 'shop.model.lkml',
    content: `
      connection: "c"
      explore: orders {
        join: customers {
          type: left_outer
          sql_on: \${orders.customer_id} = \${customers.id} ;;
          relationship: many_to_one
        }
        join: regions {
          type: left_outer
          sql_on: \${customers.region_id} = \${regions.id} ;;
          relationship: many_to_one
        }
      }`,
  };
  const ordersView = {
    name: 'orders.view.lkml',
    content: `
      view: orders {
        sql_table_name: DB.SCH.ORDERS ;;
        dimension: id { primary_key: yes type: number sql: \${TABLE}.id ;; }
        dimension: customer_id { type: number sql: \${TABLE}.customer_id ;; }
        dimension: amount { type: number sql: \${TABLE}.amount ;; }
        dimension: status { type: string sql: \${TABLE}.status ;; }
        measure: total_amount { type: sum sql: \${amount} ;; }
        measure: distinct_customers { type: count_distinct sql: \${customer_id} ;; }
        measure: completed_amount { type: sum sql: \${amount} ;; filters: [status: "Complete"] }
      }`,
  };
  const customersView = {
    name: 'customers.view.lkml',
    content: `
      view: customers {
        sql_table_name: DB.SCH.CUSTOMERS ;;
        dimension: id { primary_key: yes type: number sql: \${TABLE}.id ;; }
        dimension: region_id { type: number sql: \${TABLE}.region_id ;; }
        dimension: name { type: string sql: \${TABLE}.name ;; }
      }`,
  };
  const regionsView = {
    name: 'regions.view.lkml',
    content: `
      view: regions {
        sql_table_name: DB.SCH.REGIONS ;;
        dimension: id { primary_key: yes type: number sql: \${TABLE}.id ;; }
        dimension: name { type: string sql: \${TABLE}.name ;; }
      }`,
  };
  const files = [model, ordersView, customersView, regionsView];
  const run = () => convertLookMLToSigma(files, { exploreName: 'orders', connectionId: 'c' });
  const tableOf = (e: any) => e.source?.path ? e.source.path[e.source.path.length - 1] : (e.name || e.id);

  test('bug1: no ${...} leaks in any column id or formula', () => {
    const { model } = run();
    const els = model.pages[0].elements;
    const leaks: string[] = [];
    for (const e of els) {
      for (const c of (e.columns || [])) {
        if ((c.formula || '').includes('${') || (c.id || '').includes('${')) leaks.push(`${c.id}=${c.formula}`);
      }
      for (const m of (e.metrics || [])) {
        if ((m.formula || '').includes('${')) leaks.push(`${m.name}=${m.formula}`);
      }
    }
    assert.equal(leaks.length, 0, `unexpected \${} leaks:\n${leaks.join('\n')}`);
  });

  test('bug1: measure ${amount} resolves to a Sum over the amount column', () => {
    const { model } = run();
    const orders = model.pages[0].elements.find((e: any) => tableOf(e) === 'ORDERS')!;
    const m = orders.metrics!.find((x: any) => x.name === 'Total Amount')!;
    assert.match(m.formula, /^Sum\(\[Amount\]\)$/i, `got: ${m.formula}`);
    const cd = orders.metrics!.find((x: any) => x.name === 'Distinct Customers')!;
    assert.match(cd.formula, /^CountDistinct\(\[Customer Id\]\)$/i, `got: ${cd.formula}`);
    const cf = orders.metrics!.find((x: any) => x.name === 'Completed Amount')!;
    assert.match(cf.formula, /^SumIf\(\[Amount\], \[Status\] = "Complete"\)$/i, `got: ${cf.formula}`);
  });

  test('bug2: snowflake join (regions) is wired off the customers element, not orders', () => {
    const { model } = run();
    const els = model.pages[0].elements;
    const byId: Record<string, any> = {};
    for (const e of els) byId[e.id] = e;

    // Find the regions relationship wherever it lives.
    let host: any = null, rel: any = null;
    for (const e of els) {
      const r = (e.relationships || []).find((x: any) => x.name === 'regions');
      if (r) { host = e; rel = r; }
    }
    assert.ok(rel, 'regions relationship not found');
    assert.equal(tableOf(host), 'CUSTOMERS', `regions rel should hang off CUSTOMERS, got ${tableOf(host)}`);
    assert.equal(tableOf(byId[rel.targetElementId]), 'REGIONS');

    // Source column must be customers.region_id, NOT orders.id.
    const srcCol = (host.columns || []).find((c: any) => c.id === rel.keys[0].sourceColumnId);
    assert.match(srcCol.formula, /Region Id/i, `source col should be Region Id, got ${srcCol?.formula}`);
  });

  test('bug2: direct join (customers) still wired off the orders base element', () => {
    const { model } = run();
    const orders = model.pages[0].elements.find((e: any) => tableOf(e) === 'ORDERS')!;
    const rel = (orders.relationships || []).find((x: any) => x.name === 'customers');
    assert.ok(rel, 'customers relationship should be on the orders element');
  });
});

describe('bugfix: derived elements must skip the relationship\'s own join-key passthrough', () => {
  // A cross-element passthrough of the join key ([BASE/REL/<key>]) compiles to
  // type "error" in Sigma (feedback_sigma_derived_view_skip_join_key) — the base
  // element already carries that value. The LOCAL buildDerivedElements in
  // lookml.ts must skip it, same as the shared sigma-ids.ts builder.
  const model = {
    name: 'shop.model.lkml',
    content: `
      connection: "c"
      explore: orders {
        join: customers {
          type: left_outer
          sql_on: \${orders.customer_id} = \${customers.id} ;;
          relationship: many_to_one
        }
      }`,
  };
  const ordersView = {
    name: 'orders.view.lkml',
    content: `
      view: orders {
        sql_table_name: DB.SCH.ORDERS ;;
        dimension: id { primary_key: yes type: number sql: \${TABLE}.id ;; }
        dimension: customer_id { type: number sql: \${TABLE}.customer_id ;; }
        dimension: amount { type: number sql: \${TABLE}.amount ;; }
      }`,
  };
  const customersView = {
    name: 'customers.view.lkml',
    content: `
      view: customers {
        sql_table_name: DB.SCH.CUSTOMERS ;;
        dimension: id { primary_key: yes type: number sql: \${TABLE}.id ;; }
        dimension: name { type: string sql: \${TABLE}.name ;; }
        dimension: tier { type: string sql: \${TABLE}.tier ;; }
      }`,
  };
  const run = () => convertLookMLToSigma([model, ordersView, customersView], { exploreName: 'orders', connectionId: 'c' });

  test('derived element exposes joined dims but NOT the join key', () => {
    const { model: m } = run();
    const derived = m.pages[0].elements.filter((e: any) => e.source?.kind === 'table');
    assert.ok(derived.length >= 1, 'expected at least one derived element');
    const formulas = derived.flatMap((e: any) => (e.columns || []).map((c: any) => c.formula));
    const crossEl = formulas.filter((f: string) => /^\[[^\]]+\/customers\//.test(f));
    assert.ok(crossEl.some((f: string) => /\/Name\]$/.test(f)), `joined dim Name missing: ${crossEl.join(', ')}`);
    assert.ok(crossEl.some((f: string) => /\/Tier\]$/.test(f)), `joined dim Tier missing: ${crossEl.join(', ')}`);
    // The customers-rel join key (customers.id) must NOT be denormalized.
    const keyPassthroughs = crossEl.filter((f: string) => /\/Id\]$/.test(f));
    assert.equal(keyPassthroughs.length, 0,
      `join-key passthrough(s) emitted (compile to type "error" in Sigma): ${keyPassthroughs.join(', ')}`);
  });

  test('derived-element refs resolve against the base element NAME, not the table tail', () => {
    // Since elements carry explicit display names (live-E2E fix), the first
    // segment of every ref inside the derived element must be that name —
    // emitting the warehouse path-tail (ORDERS) compiles to
    // `Unknown name: ["ORDERS",...]` in Sigma (CI regression, PR #46).
    const { model: m } = run();
    const base = m.pages[0].elements.find((e: any) =>
      e.source?.kind === 'warehouse-table' && e.source.path?.includes('ORDERS'));
    assert.ok(base, 'base orders element missing');
    assert.equal(base.name, 'Orders', 'base element should carry its display name');
    const derived = m.pages[0].elements.find((e: any) =>
      e.source?.kind === 'table' && (e.source as any).elementId === base.id);
    assert.ok(derived, 'derived element missing');
    for (const c of derived.columns) {
      assert.match(c.formula, /^\[Orders\//,
        `derived ref must start with the base element name [Orders/…]: ${c.formula}`);
    }
  });
});

// ── Layered LookML (synthesized generic fixtures — trades/rates/counterparties
//    shapes: CTE-continuation fragments, cross-view SQL_TABLE_NAME refs,
//    incremental PDTs, dimension_group edge cases, formatting measures) ──────

const ratesViewLkml = `view: daily_rates {
  derived_table: {
    sql:
      SELECT TO_DATE(ORDER_DATE) AS PRICE_DATE, REGION AS ASSET, AVG(UNIT_PRICE) AS USD_RATE
      FROM CSA.TJ.ORDER_FACT
      GROUP BY 1,2 ;;
    datagroup_trigger: nightly_datagroup
  }
  dimension: asset {
    type: string
    sql: \${TABLE}."ASSET" ;;
  }
}
`;

const tradesViewLkml = `view: trades_enriched {
  derived_table: {
    sql:
      -- continuation fragment: Looker prepends inlined PDT CTEs before this comma
      , base AS (
          SELECT t.ORDER_ID, t.ORDER_DATE, t.NET_REVENUE, t.REGION,
                 REGEXP_REPLACE(t.ORDER_STATUS, '\\\\d+', '') AS STATUS_CLEAN
          FROM CSA.TJ.ORDER_FACT t
          WHERE {% incrementcondition %} t.ORDER_DATE {% endincrementcondition %}
      )
      SELECT b.*, r.USD_RATE
      FROM base b
      LEFT JOIN \${daily_rates.SQL_TABLE_NAME} r
        ON TO_DATE(b.ORDER_DATE) = r.PRICE_DATE AND b.REGION = r.ASSET ;;
    datagroup_trigger: nightly_datagroup
    increment_key: "order_date"
    increment_offset: 2
    cluster_keys: ["ORDER_DATE"]
  }
  dimension: order_id {
    type: string
    sql: \${TABLE}."ORDER_ID" ;;
  }
  dimension: net_revenue {
    type: number
    sql: \${TABLE}."NET_REVENUE" ;;
  }
  dimension_group: order_date {
    type: time
    timeframes: [date, week, month]
    sql: CAST(\${TABLE}."ORDER_DATE" AS TIMESTAMP_NTZ) ;;
  }
  dimension_group: max_seen_dim {
    hidden: yes
    type: time
    timeframes: [raw]
    sql: \${TABLE}."ORDER_DATE" ;;
  }
  measure: total_net_revenue {
    type: sum
    value_format_name: usd_0
    sql: \${net_revenue} ;;
  }
  measure: max_seen {
    type: date_time
    convert_tz: no
    sql: MAX(\${max_seen_dim_raw}) ;;
  }
  measure: formatted_revenue {
    type: string
    sql: CASE WHEN \${order_id} IN ('1','2') THEN TO_CHAR(\${total_net_revenue} / 1000, '$FM999,990.0') || ' K' ELSE TO_CHAR(\${total_net_revenue}, '$FM999,999,990') END ;;
  }
  measure: count {
    type: count
  }
}
`;

const lonelyViewLkml = `view: cpty_rollup {
  derived_table: {
    sql: SELECT SHORT_NAME, MAX(STATUS) AS STATUS
      FROM \${counterparty_deals.SQL_TABLE_NAME}
      GROUP BY 1 ;;
  }
  dimension: short_name {
    type: string
    sql: \${TABLE}."SHORT_NAME" ;;
  }
}
`;

const layeredFiles = [
  { name: 'trades_enriched.view.lkml', content: tradesViewLkml },
  { name: 'daily_rates.view.lkml', content: ratesViewLkml },
];

describe('layered: view-only conversion (no model/explore file)', () => {
  test('does not throw; converts each view to a standalone element', () => {
    const { model, warnings } = convertLookMLToSigma(layeredFiles, { connectionId: 'test-conn' });
    assert.equal(model.pages[0].elements.length, 2);
    assert.ok(warnings.some((w: string) => /No explore\/model file provided/i.test(w)));
  });

  test('single view-only file converts and names the model after the view', () => {
    const { model } = convertLookMLToSigma(
      [{ name: 'daily_rates.view.lkml', content: ratesViewLkml }], { connectionId: 'test-conn' });
    assert.equal(model.pages[0].elements.length, 1);
    assert.equal(model.name, 'Daily Rates');
  });
});

describe('layered: cross-view SQL_TABLE_NAME → CTE inlining + fragment completion', () => {
  const { model, warnings } = convertLookMLToSigma(layeredFiles, { connectionId: 'test-conn' });
  const trades: any = model.pages[0].elements[0];
  const stmt: string = trades.source.statement;

  test('referenced derived view is inlined as a WITH CTE named after the view', () => {
    assert.match(stmt, /WITH\s+daily_rates\s+AS\s*\(/i, stmt);
    assert.ok(stmt.includes('FROM daily_rates r') || /daily_rates\s+r\b/.test(stmt), stmt);
  });

  test('CTE-continuation fragment is completed (", base AS (" continues the WITH chain)', () => {
    assert.ok(stmt.includes(', base AS ('), stmt);
    assert.ok(warnings.some((w: string) => /inlined 1 referenced derived view/i.test(w)));
  });

  test('no ${...} refs or Liquid tags remain in the statement', () => {
    assert.ok(!stmt.includes('${'), stmt);
    assert.ok(!/\{%/.test(stmt), stmt);
  });

  test('incrementcondition Liquid replaced with 1=1 + materialization warning', () => {
    assert.ok(/WHERE\s+1=1/.test(stmt), stmt);
    assert.ok(warnings.some((w: string) => /incrementcondition.*replaced with 1=1.*materialization/is.test(w)));
  });

  test('increment_key emits a loud materialization recommendation', () => {
    assert.ok(warnings.some((w: string) => /increment_key: "order_date".*materialization/is.test(w)));
  });
});

describe('layered: unresolved cross-view ref → LOOKER_SCRATCH placeholder', () => {
  const { model, warnings } = convertLookMLToSigma(
    [{ name: 'cpty_rollup.view.lkml', content: lonelyViewLkml }], { connectionId: 'test-conn' });
  const stmt: string = (model.pages[0].elements[0] as any).source.statement;

  test('placeholder table substituted; no ${...} left', () => {
    assert.ok(stmt.includes('LOOKER_SCRATCH.COUNTERPARTY_DEALS'), stmt);
    assert.ok(!stmt.includes('${'), stmt);
  });

  test('LOUD warning names the unresolved view and both fixes', () => {
    const w = warnings.find((w: string) => /UNRESOLVED VIEW "counterparty_deals"/.test(w));
    assert.ok(w, warnings.join('\n'));
    assert.ok(/counterparty_deals\.view\.lkml/.test(w!) && /LOOKER_SCRATCH\.COUNTERPARTY_DEALS/.test(w!), w);
  });
});

describe('layered: dimension_group edge cases', () => {
  const { model } = convertLookMLToSigma(layeredFiles, { connectionId: 'test-conn' });
  const trades: any = model.pages[0].elements[0];
  const colIds = new Set((trades.columns || []).map((c: any) => c.id));

  test('every order entry references an existing column (no dangling raw id when timeframes lack raw/time)', () => {
    for (const id of trades.order || []) {
      assert.ok(colIds.has(id), `order id ${id} not found in columns`);
    }
  });

  test('CAST-wrapped dimension_group expands to DateTrunc timeframes (not skipped)', () => {
    const month = (trades.columns || []).find((c: any) => c.name === 'Order Date Month');
    assert.ok(month, JSON.stringify(trades.columns));
    assert.match(month.formula, /DateTrunc\("month", \[Custom SQL\/ORDER_DATE\]\)/);
  });

  test('two dimension_groups over the same physical column share one base column (no duplicates)', () => {
    const base = (trades.columns || []).filter((c: any) => c.formula === '[Custom SQL/ORDER_DATE]');
    assert.equal(base.length, 1, JSON.stringify(base));
    const ids = (trades.columns || []).map((c: any) => c.id);
    assert.equal(ids.length, new Set(ids).size, 'duplicate column ids found');
  });
});

describe('layered: measure translation guards', () => {
  const { model, warnings } = convertLookMLToSigma(layeredFiles, { connectionId: 'test-conn' });
  const trades: any = model.pages[0].elements[0];
  const metricByName = (n: string) => (trades.metrics || []).find((m: any) => m.name === n);

  test('type: date_time measure MAX(${dim_group_raw}) → Max([col]) metric, no phantom [MAX] column', () => {
    const m = metricByName('Max Seen');
    assert.ok(m, JSON.stringify(trades.metrics));
    assert.equal(m.formula, 'Max([ORDER_DATE])');
    const phantom = (trades.columns || []).find((c: any) => /\[Custom SQL\/MAX\]/.test(c.formula || ''));
    assert.ok(!phantom, 'phantom MAX column fabricated');
  });

  test('TO_CHAR/|| formatting measure warns with the exact fragment instead of emitting broken Sigma', () => {
    assert.ok(!metricByName('Formatted Revenue'), 'broken TO_CHAR metric was emitted');
    const w = warnings.find((w: string) => /"formatted_revenue".*untranslatable fragment/is.test(w));
    assert.ok(w, warnings.join('\n'));
    assert.ok(/TO_CHAR/.test(w!));
  });
});
