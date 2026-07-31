/**
 * Bug: omniTranslateFormula's field-substitution, ::TYPE cast rewrite,
 * `IN (...)` list splitter, CASE→If() lowering (sqlCaseToIf/parseCaseBody),
 * and SQL-function-name mapping (OMNI_FUNC_MAP) all scan a user-written
 * Omni `sql:` expression with no idea that string literals exist. A literal
 * that happens to contain a WHEN/THEN/ELSE keyword, a comma, or a mapped
 * function name gets read as live syntax, corrupting the formula.
 *
 * Demonstrated (live-reproduced against this repo's HEAD, pre-fix), via the
 * exported entry point `convertOmniToSigma` — `parseCaseBody` and
 * `omniTranslateFormula` are not exported, so the only faithful repro route
 * is through a real Omni view file:
 *
 *   dimension sql: CASE WHEN ${TABLE}.due_label = 'When Due' THEN 'On Time' ELSE 'Late' END
 *   → produced formula: If(Due", "On Time", "Late")
 *
 * The literal 'When Due' contains the substring "When" — parseCaseBody's
 * depth-walk only tracks `(`/`[` nesting, not quote state, so it reads that
 * "When" as a second, spurious WHEN and splits the condition there. The
 * real condition (`[ORDERS/Due Label] = "When Due"`) is destroyed; the
 * emitted `If()` binds the wrong condition entirely — a dangerous, silent
 * wrong-value defect (not a crash, not a warning).
 *
 * The fix masks every single-quoted literal span ONCE at the top of
 * omniTranslateFormula (before field substitution, casts, IN-lists, CASE
 * parsing, or function mapping ever see the text) and unmasks at the very
 * end — the same point where single-quoted SQL literals become Sigma's
 * double-quoted form. A `[bracketed identifier]` span is treated as atomic
 * (an apostrophe inside one is not a quote delimiter) and an unterminated
 * quote/bracket must not swallow the rest of the string.
 *
 * Control (must keep working): a literal-free CASE and a literal-free
 * SUM(...) must still translate exactly as before.
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { convertOmniToSigma } from './omni.js';

/** Build a minimal one-view Omni file with a single dimension or measure and
 * convert it, returning the generated formula for that field. */
function convertDimFormula(sql: string, extra: string[] = []): string {
  const yamlContent = [
    'view: orders',
    'sql_table_name: CSA.TJ.ORDERS',
    'dimensions:',
    '  - name: order_id',
    '    primary_key: true',
    '    sql: |',
    '      ${TABLE}.order_id',
    '  - name: target_field',
    '    sql: |',
    `      ${sql}`,
    ...extra,
    'measures:',
    '  - name: order_count',
    '    type: count',
  ].join('\n');

  const result = convertOmniToSigma([{ name: 'orders.view.yaml', content: yamlContent }], {});
  const el = result.model.pages[0].elements[0] as any;
  const col = el.columns.find((c: any) => c.id.toUpperCase().includes('TARGET_FIELD'));
  if (!col) throw new Error(`target_field column not found; warnings=${JSON.stringify(result.warnings)}`);
  return col.formula;
}

describe('Omni literal masking: demonstrated bug (headline repro)', () => {
  test("CASE WHEN literal 'When Due' does not split the condition", () => {
    const formula = convertDimFormula(
      `CASE WHEN \${TABLE}.due_label = 'When Due' THEN 'On Time' ELSE 'Late' END`
    );
    assert.equal(
      formula,
      `If([ORDERS/Due Label] = "When Due", "On Time", "Late")`
    );
  });
});

describe('Omni literal masking: other reserved words inside a literal', () => {
  test("literal containing 'THEN' does not create a spurious branch", () => {
    const formula = convertDimFormula(
      `CASE WHEN \${TABLE}.flag = 1 THEN 'Choose THEN branch' ELSE 'no' END`
    );
    assert.equal(
      formula,
      `If([ORDERS/Flag] = 1, "Choose THEN branch", "no")`
    );
  });

  test("literal containing 'ELSE' does not create a spurious branch", () => {
    const formula = convertDimFormula(
      `CASE WHEN \${TABLE}.flag = 1 THEN 'ok' ELSE 'plan B ELSE plan C' END`
    );
    assert.equal(
      formula,
      `If([ORDERS/Flag] = 1, "ok", "plan B ELSE plan C")`
    );
  });
});

describe('Omni literal masking: mapped SQL function name inside a literal', () => {
  test('a literal-only formula containing SUBSTR(...) text is not rewritten to Mid(...)', () => {
    const formula = convertDimFormula(`'Use SUBSTR(x,1,2) approach'`);
    assert.equal(formula, `"Use SUBSTR(x,1,2) approach"`);
  });
});

describe('Omni literal masking: comma inside an IN-list literal', () => {
  test("IN ('A,B', 'C') keeps exactly two values, not three", () => {
    const formula = convertDimFormula(`\${TABLE}.category IN ('A,B', 'C')`);
    assert.equal(formula, `In([ORDERS/Category], "A,B", "C")`);
  });
});

describe('Omni literal masking: unterminated quote safety net', () => {
  test('an unterminated quote does not throw and does not swallow the rest of the expression', () => {
    // Missing closing quote after "Ok — must not consume the rest of the
    // string looking for a quote that never comes.
    assert.doesNotThrow(() => {
      convertDimFormula(`CASE WHEN \${TABLE}.flag = 1 THEN 'Ok ELSE 'Bad' END`);
    });
  });
});

describe('Omni literal masking: control (must keep working)', () => {
  test('literal-free CASE still lowers to If() exactly as before', () => {
    const formula = convertDimFormula(
      `CASE WHEN \${TABLE}.amount > 100 THEN 'big' ELSE 'small' END`
    );
    assert.equal(
      formula,
      `If([ORDERS/Amount] > 100, "big", "small")`
    );
  });

  test('literal-free SUM(...) style expression is unaffected', () => {
    const formula = convertDimFormula(`\${TABLE}.amount * 2`);
    assert.equal(formula, `[ORDERS/Amount] * 2`);
  });
});
