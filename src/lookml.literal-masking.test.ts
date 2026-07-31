/**
 * Bug: the "pull cross-element calc cols off source elements" pass (mirrors
 * dbt.ts / tableau.ts's buildDerivedElementsAndMoveCalcs) scans a column's
 * already-converted SIGMA FORMULA text for bare `[Name]` refs with
 * `/\[([^\]\/]+)\]/g` to decide whether the column reaches outside its own
 * element. Sigma formulas quote string literals with DOUBLE quotes ("like
 * this") — a `case:` dimension's `label:` (or `else:`) that happens to
 * contain bracket-look-alike text (e.g. "Small [X]") gets read as a
 * reference to a field literally named X, and the otherwise fully
 * self-contained column is incorrectly pulled off its base element and
 * relocated onto (or, when no derived companion element exists, silently
 * dropped from) the output — even though it has zero real cross-element
 * references.
 *
 * Demonstrated (live-reproduced, pre-fix): the `amount_bucket` case
 * dimension below (label "Small [X]") is removed from the "Orders" element
 * and moved to the "Orders View" derived element with a "moved to derived
 * (cross-element refs)" warning.
 *
 * Control (must keep working): a dimension whose SQL genuinely references
 * another (joined) view's field — CONCAT('Cust: ', ${customers.name}) —
 * must still be detected as cross-element and relocated with its ref
 * rewritten to the qualified [Orders/customers/Name] form.
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { convertLookMLToSigma, maskFormulaStringLiterals } from './lookml.js';

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

const customersView = {
  name: 'customers.view.lkml',
  content: `
    view: customers {
      sql_table_name: DB.SCH.CUSTOMERS ;;
      dimension: id { primary_key: yes type: number sql: \${TABLE}.id ;; }
      dimension: name { type: string sql: \${TABLE}.name ;; }
    }`,
};

function ordersElement(model: any) {
  return model.pages[0].elements.find(
    (e: any) => e.source?.kind === 'warehouse-table' && e.source.path?.includes('ORDERS')
  );
}

describe('lookml literal masking: case-dimension label with [bracket]-look-alike text', () => {
  const ordersView = {
    name: 'orders.view.lkml',
    content: `
      view: orders {
        sql_table_name: DB.SCH.ORDERS ;;
        dimension: id { primary_key: yes type: number sql: \${TABLE}.id ;; }
        dimension: customer_id { type: number sql: \${TABLE}.customer_id ;; }
        dimension: amount { type: number sql: \${TABLE}.amount ;; }
        dimension: amount_bucket {
          case: {
            when: {
              sql: \${TABLE}.amount < 100 ;;
              label: "Small [X]"
            }
            else: "Large"
          }
        }
      }`,
  };
  const run = () => convertLookMLToSigma([model, ordersView, customersView], { exploreName: 'orders', connectionId: 'c' });

  test('"Amount Bucket" stays on the base Orders element', () => {
    const { model } = run();
    const orders = ordersElement(model);
    const names = (orders.columns || []).map((c: any) => c.name);
    assert.ok(names.includes('Amount Bucket'), `expected "Amount Bucket" on Orders, got: ${names.join(', ')}`);
  });

  test('no "moved to derived" warning fires for a purely local literal-only column', () => {
    const { warnings } = run();
    const bogus = warnings.filter(w => /moved to derived/i.test(w));
    assert.equal(bogus.length, 0, `expected no cross-element relocation, got: ${bogus.join('; ')}`);
  });

  test('the literal text is preserved verbatim', () => {
    const { model } = run();
    const orders = ordersElement(model);
    const col = (orders.columns || []).find((c: any) => c.name === 'Amount Bucket');
    assert.match(col!.formula, /Small \[X\]/);
  });
});

describe('lookml literal masking: literal text survives cross-element ref rewriting', () => {
  // A SECOND, independent site of the same defect: once a column is
  // correctly identified as cross-element (a genuine ${customers.name} ref),
  // the pass that rewrites bare [Name] refs to the qualified
  // [Orders/customers/Name] form also scans the raw (unmasked) formula text
  // — so a literal elsewhere in the SAME formula that happens to contain
  // "[Name]" (matching a real related column's display name) gets its
  // literal content silently rewritten too.
  // Demonstrated (live-reproduced, pre-fix):
  //   Concat([Name], " says [Name]")
  //     → Concat([Orders/customers/Name], " says [Orders/customers/Name]")
  //   (the literal's content is corrupted — the real ref should be rewritten,
  //   the literal must not be touched)
  const ordersViewMixed = {
    name: 'orders.view.lkml',
    content: `
      view: orders {
        sql_table_name: DB.SCH.ORDERS ;;
        dimension: id { primary_key: yes type: number sql: \${TABLE}.id ;; }
        dimension: customer_id { type: number sql: \${TABLE}.customer_id ;; }
        dimension: amount { type: number sql: \${TABLE}.amount ;; }
        dimension: complex_ref {
          sql: CONCAT(\${customers.name}, ' says [Name]') ;;
        }
      }`,
  };
  const run = () => convertLookMLToSigma([model, ordersViewMixed, customersView], { exploreName: 'orders', connectionId: 'c' });

  test('the literal " says [Name]" is not rewritten into a qualified ref path', () => {
    const { model } = run();
    const view = model.pages[0].elements.find((e: any) => e.name === 'Orders View')!;
    const col = (view.columns || []).find((c: any) => c.name === 'Complex Ref');
    assert.ok(col, 'expected "Complex Ref" on the derived element');
    assert.match(col!.formula, / says "?\[Name\]/, `literal was corrupted: ${col!.formula}`);
  });

  test('the real (non-literal) [Name] ref is still correctly rewritten', () => {
    const { model } = run();
    const view = model.pages[0].elements.find((e: any) => e.name === 'Orders View')!;
    const col = (view.columns || []).find((c: any) => c.name === 'Complex Ref');
    assert.match(col!.formula, /^Concat\(\[Orders\/customers\/Name\]/, `real ref not rewritten: ${col!.formula}`);
  });
});

describe('lookml literal masking: control — a genuine cross-element ref still relocates', () => {
  const ordersViewCross = {
    name: 'orders.view.lkml',
    content: `
      view: orders {
        sql_table_name: DB.SCH.ORDERS ;;
        dimension: id { primary_key: yes type: number sql: \${TABLE}.id ;; }
        dimension: customer_id { type: number sql: \${TABLE}.customer_id ;; }
        dimension: amount { type: number sql: \${TABLE}.amount ;; }
        dimension: full_label {
          sql: CONCAT('Cust: ', \${customers.name}) ;;
        }
      }`,
  };
  const run = () => convertLookMLToSigma([model, ordersViewCross, customersView], { exploreName: 'orders', connectionId: 'c' });

  test('"Full Label" is NOT on the base Orders element', () => {
    const { model } = run();
    const orders = ordersElement(model);
    const names = (orders.columns || []).map((c: any) => c.name);
    assert.ok(!names.includes('Full Label'), `expected "Full Label" to be pulled off Orders, still there: ${names.join(', ')}`);
  });

  test('"Full Label" lands on the derived "Orders View" element with a rewritten cross-element ref', () => {
    const { model } = run();
    const view = model.pages[0].elements.find((e: any) => e.name === 'Orders View');
    assert.ok(view, 'expected an "Orders View" derived element');
    const col = (view.columns || []).find((c: any) => c.name === 'Full Label');
    assert.ok(col, 'expected "Full Label" on the derived element');
    assert.match(col!.formula, /\[Orders\/customers\/Name\]/);
  });
});

describe('lookml literal masking: sentinel-collision guard', () => {
  // maskFormulaStringLiterals uses an ASCII sentinel ("@@LIT0@@"-shaped)
  // rather than a control byte — greppable, no binary-file problem — but an
  // ASCII sentinel is typeable, so formula text could already contain it
  // (e.g. a bracket-look-alike left over from a *previous* pass, or just an
  // unlucky label). If the input already contains the sentinel the naive
  // (unguarded) version corrupts unrelated text on unmask: it can't tell its
  // own inserted placeholder apart from a pre-existing occurrence, so BOTH
  // get replaced with the same literal. Demonstrated against the naive
  // version (see the sibling assertion below, run manually against a copy
  // of the pre-guard implementation): unmasking
  //   If([@@LIT0@@] = 1, "Small", "Large")
  // (masked as `If([@@LIT0@@] = 1, @@LIT0@@, @@LIT1@@)`) turns BOTH
  // occurrences of "@@LIT0@@" into "Small", corrupting the pre-existing
  // bracket-look-alike text into `If(["Small"] = 1, "Small", "Large")`.
  test('a formula already containing the exact default sentinel round-trips unchanged', () => {
    const input = 'If([@@LIT@@0@@LIT@@] = 1, "Small", "Large")';
    const { masked, unmask } = maskFormulaStringLiterals(input);
    const out = unmask(masked);
    assert.equal(out, input, `sentinel collision corrupted the formula: ${out}`);
  });

  test('a formula containing the bare "@@LIT0@@" token (no widening needed) round-trips unchanged', () => {
    const input = 'Concat([@@LIT0@@], "Small [X]")';
    const { masked, unmask } = maskFormulaStringLiterals(input);
    const out = unmask(masked);
    assert.equal(out, input, `sentinel collision corrupted the formula: ${out}`);
  });

  test('the masked text never contains a bare (unwidened) @@LIT sentinel that was not already there', () => {
    // Escalate: an input that contains every "obvious" width of widened
    // marker up to several '@'s — the guard must keep widening past all of
    // them rather than settling on the first one that merely avoids the
    // ORIGINAL default.
    const input = 'If([@@LIT@@@@@0@@LIT@@@@@] = 1, "A", "B")';
    const { masked, unmask } = maskFormulaStringLiterals(input);
    assert.equal(unmask(masked), input, 'round-trip must restore the original text exactly');
  });

  test('control: a formula with no sentinel-look-alike text is masked/unmasked normally', () => {
    const input = 'If([Amount] < 100, "Small", "Large")';
    const { masked, unmask } = maskFormulaStringLiterals(input);
    assert.doesNotMatch(masked, /Small|Large/);
    assert.equal(unmask(masked), input);
  });
});
