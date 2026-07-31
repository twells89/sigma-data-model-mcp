/**
 * Bug: dbt.ts's "pull cross-element calc cols off source elements" pass is a
 * byte-for-byte copy of the same logic in lookml.ts — it scans a column's
 * already-converted SIGMA FORMULA text for bare `[Name]` refs with
 * `/\[([^\]\/]+)\]/g` to decide whether the column reaches outside its own
 * element. Sigma formulas quote string literals with DOUBLE quotes, so a
 * CASE-derived dimension whose branch value happens to contain
 * bracket-look-alike text (e.g. 'Small [X]') gets read as a reference to a
 * field literally named X — the self-contained column is incorrectly
 * pulled off its base element and relocated onto the derived "<Model> View"
 * element (or dropped entirely if no derived companion exists), even though
 * it has zero real cross-element references.
 *
 * Demonstrated (live-reproduced, pre-fix): the `amount_bucket` dimension
 * below ("CASE WHEN amount < 100 THEN 'Small [X]' ELSE 'Large' END") is
 * removed from the "order_fact" element and moved to "Order Fact View"
 * with a "moved to derived (cross-element refs)" warning.
 *
 * Control (must keep working): a dimension whose expr genuinely references
 * another (foreign-entity-joined) model's column — customer_segment, which
 * lives only on customer_dim — must still be detected as cross-element and
 * relocated with its ref rewritten to the qualified
 * [.../customer_dim/Customer Segment] form.
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { convertDbtToSigma } from './dbt.js';

function findElementByColumnName(model: any, colName: string) {
  return model.pages[0].elements.filter((e: any) =>
    (e.columns || []).some((c: any) => c.name === colName)
  );
}

describe('dbt literal masking: CASE-derived dimension with [bracket]-look-alike text', () => {
  const yamlText = `
semantic_models:
  - name: order_fact
    model: "ref('order_fact')"
    entities:
      - name: order_id
        type: primary
        expr: order_id
      - name: customer
        type: foreign
        expr: customer_id
    dimensions:
      - name: amount_bucket
        type: categorical
        expr: "CASE WHEN amount < 100 THEN 'Small [X]' ELSE 'Large' END"
    measures:
      - name: total_amount
        agg: sum
        expr: amount
  - name: customer_dim
    model: "ref('customer_dim')"
    entities:
      - name: customer
        type: primary
        expr: customer_id
    dimensions:
      - name: name
        expr: customer_name
`;
  const run = () => convertDbtToSigma(yamlText, { connectionId: 'c' });

  test('"Amount Bucket" stays on the base order_fact element', () => {
    const { model } = run();
    const owners = findElementByColumnName(model, 'Amount Bucket');
    assert.equal(owners.length, 1, `expected exactly one element carrying "Amount Bucket", got: ${owners.map((e: any) => e.name || e.id).join(', ')}`);
    assert.equal(owners[0].source?.kind, 'warehouse-table', 'expected it to stay on the base warehouse-table element, not a derived view');
  });

  test('no "moved to derived" warning fires for a purely local literal-only column', () => {
    const { warnings } = run();
    const bogus = warnings.filter(w => /moved to derived/i.test(w));
    assert.equal(bogus.length, 0, `expected no cross-element relocation, got: ${bogus.join('; ')}`);
  });

  test('the literal text is preserved verbatim', () => {
    const { model } = run();
    const el = findElementByColumnName(model, 'Amount Bucket')[0];
    const col = el.columns.find((c: any) => c.name === 'Amount Bucket');
    assert.match(col.formula, /Small \[X\]/);
  });
});

describe('dbt literal masking: control — a genuine cross-element ref still relocates', () => {
  const yamlText = `
semantic_models:
  - name: order_fact
    model: "ref('order_fact')"
    entities:
      - name: order_id
        type: primary
        expr: order_id
      - name: customer
        type: foreign
        expr: customer_id
    dimensions:
      - name: is_pro_segment
        type: categorical
        expr: "CASE WHEN customer_segment = 'Pro' THEN 'Yes' ELSE 'No' END"
    measures:
      - name: total_amount
        agg: sum
        expr: amount
  - name: customer_dim
    model: "ref('customer_dim')"
    entities:
      - name: customer
        type: primary
        expr: customer_id
    dimensions:
      - name: customer_segment
        expr: customer_segment
`;
  const run = () => convertDbtToSigma(yamlText, { connectionId: 'c' });

  test('"Is Pro Segment" is NOT on the base order_fact warehouse-table element', () => {
    const { model } = run();
    const owners = findElementByColumnName(model, 'Is Pro Segment');
    const onBase = owners.filter((e: any) => e.source?.kind === 'warehouse-table');
    assert.equal(onBase.length, 0, `expected "Is Pro Segment" pulled off the base element, still on: ${onBase.map((e: any) => e.name || e.id).join(', ')}`);
  });

  test('"Is Pro Segment" lands on a derived view element with a rewritten cross-element ref', () => {
    const { model } = run();
    const owners = findElementByColumnName(model, 'Is Pro Segment');
    assert.equal(owners.length, 1, 'expected exactly one element carrying "Is Pro Segment"');
    const col = owners[0].columns.find((c: any) => c.name === 'Is Pro Segment');
    assert.match(col.formula, /\/Customer Segment\]/);
  });
});
