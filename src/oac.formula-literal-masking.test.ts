/**
 * Bug: oacExprToSigma's unsupported-function check, SQL_TSI_ mapping, the
 * NVL/SUBSTR/.../CURRENT_DATE function-name rewrites, the dotted
 * "table"."col" / bare table.col field-ref substitution, the IN-list
 * splitter, and (via the shared, imported `sqlCaseToIf`) CASE→If() lowering
 * all scan a user-written OAC logical-column expression with no idea that
 * string literals exist. OAC's dialect follows Oracle convention:
 * `"Table"."Col"` for quoted identifiers, `'text'` for string literals.
 *
 * Demonstrated (live-reproduced against this repo's HEAD, pre-fix) via the
 * exported entry point `convertOacToSigma` — `oacExprToSigma` is not
 * exported:
 *
 *   'This uses SQL_TSI_MONTH label'
 *   → "This uses "month" label"
 *   (a SYNTACTICALLY INVALID Sigma string — unescaped nested quotes; this
 *   would fail to parse as a Sigma formula at all)
 *
 *   'Contact ACME.Corp for details'
 *   → "[Corp] for details"
 *   (the literal text is destroyed and replaced with a bracket ref to a
 *   column that may not even exist — a dangerous wrong-value defect)
 *
 *   "ORDERS"."CATEGORY" IN ('A,B', 'C')  → In([Category], "A, B", "C")
 *   (comma-split on live text merges what should be two IN values)
 *
 *   'Use SUBSTR(x,1,2) approach'  → "Use Mid(x,1,2) approach"
 *   (a mapped SQL function name appearing inside a literal-only formula
 *   gets rewritten, corrupting the label text itself)
 *
 * The CASE-lowering path (via the shared, alteryx.ts-owned `sqlCaseToIf`)
 * is exercised here too as a regression check — it is fixed at its own
 * source (see alteryx.formula-literal-masking.test.ts) and this file
 * confirms oac.ts's caller sees the benefit.
 *
 * The fix masks every single-quoted literal span ONCE at the top of
 * oacExprToSigma; every pass below operates on the masked text; unmask
 * happens at the very end, which is also where a literal becomes Sigma's
 * double-quoted form. Only single-quoted spans are masked — the
 * `"double-quoted"` OAC identifier syntax is structural and still needs to
 * be seen by the field-ref substitution pass.
 *
 * Control (must keep working): a literal-free CASE, a literal-free dotted
 * field ref, and a bare mapped function call must still translate exactly
 * as before.
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { convertOacToSigma } from './oac.js';

function convertLogicalExpr(logExprText: string): string {
  const tables = [
    {
      name: 'ORDERS',
      logicalTableSources: [{ tableMapping: { tables: ['ORDERS'] } }],
      logicalColumns: [
        {
          name: 'due_status',
          logicalColumnSource: { logicalExpression: { text: logExprText } },
        },
      ],
    },
  ];
  const result = convertOacToSigma(tables, {});
  const el = (result.model.pages[0].elements as any[])[0];
  const col = el.columns.find((c: any) => c.name === 'Due Status');
  if (!col) throw new Error(`Due Status column not found; warnings=${JSON.stringify(result.warnings)}`);
  return col.formula;
}

describe('OAC literal masking: SQL_TSI_ token inside a literal (headline — produces invalid Sigma syntax pre-fix)', () => {
  test("'This uses SQL_TSI_MONTH label' is not rewritten to a nested-quote string", () => {
    const formula = convertLogicalExpr(`'This uses SQL_TSI_MONTH label'`);
    assert.equal(formula, `"This uses SQL_TSI_MONTH label"`);
  });
});

describe('OAC literal masking: dotted text inside a literal', () => {
  test("'Contact ACME.Corp for details' is not rewritten to a bracket field ref", () => {
    const formula = convertLogicalExpr(`'Contact ACME.Corp for details'`);
    assert.equal(formula, `"Contact ACME.Corp for details"`);
  });
});

describe('OAC literal masking: comma inside an IN-list literal', () => {
  test("IN ('A,B', 'C') keeps exactly two values, not three", () => {
    const formula = convertLogicalExpr(`"ORDERS"."CATEGORY" IN ('A,B', 'C')`);
    assert.equal(formula, `In([Category], "A,B", "C")`);
  });
});

describe('OAC literal masking: mapped SQL function name inside a literal', () => {
  test("a literal-only formula containing SUBSTR(...) text is not rewritten to Mid(...)", () => {
    const formula = convertLogicalExpr(`'Use SUBSTR(x,1,2) approach'`);
    assert.equal(formula, `"Use SUBSTR(x,1,2) approach"`);
  });
});

describe('OAC literal masking: CASE lowering via the shared sqlCaseToIf', () => {
  test("WHEN-condition literal containing 'THEN' does not corrupt the condition", () => {
    const formula = convertLogicalExpr(
      `CASE WHEN "ORDERS"."DUE_LABEL" = 'contains THEN keyword' THEN 'A' ELSE 'B' END`
    );
    assert.equal(formula, `If([Due Label] = "contains THEN keyword", "A", "B")`);
  });
});

describe('OAC literal masking: control (must keep working)', () => {
  test('literal-free CASE still lowers to If() exactly as before', () => {
    const formula = convertLogicalExpr(
      `CASE WHEN "ORDERS"."AMOUNT" > 100 THEN 'big' ELSE 'small' END`
    );
    assert.equal(formula, `If([Amount] > 100, "big", "small")`);
  });

  test('literal-free dotted "table"."col" field ref still converts to a bracket ref', () => {
    const formula = convertLogicalExpr(`"ORDERS"."AMOUNT" * 2`);
    assert.equal(formula, `[Amount] * 2`);
  });

  test('a bare mapped function call (SUBSTR) still maps to Mid', () => {
    const formula = convertLogicalExpr(`SUBSTR("ORDERS"."CATEGORY", 1, 3)`);
    assert.equal(formula, `Mid([Category], 1, 3)`);
  });
});
