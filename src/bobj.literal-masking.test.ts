/**
 * BusinessObjects (bobj.ts) converter tests — string-literal-blind scanning
 * in the object SELECT translator (translateBobjExpr, parseTableColTokens).
 *
 * Both functions run plain regex scans over a universe object's raw SELECT
 * text with no idea a `'…'` string literal exists. Three concrete,
 * live-reproduced failures motivated the fix (all from ONE object whose
 * SELECT was the literal 'See ORDER_FACT.NET_REVENUE note'):
 *
 *   1. parseTableColTokens — called during INGESTION to decide which
 *      PHYSICAL TABLE an object belongs to — found the literal's OWN
 *      dotted-reference-shaped text and invented a phantom "SEE_ORDER_FACT"
 *      warehouse table that doesn't exist in the universe. Not a formula
 *      bug — a corrupted DATA MODEL.
 *   2. translateBobjExpr's identical Table.Col → [Display] rewrite turned
 *      the same literal's content into "[Net Revenue] note", losing
 *      "See ... note" outright.
 *   3. Worst: CASE WHEN 1 = 1 THEN 'high or else low' ELSE 'low' END came
 *      back as `If(1 = 1, "high or, low" ELSE "low")` — sqlCaseToIf (SHARED
 *      with alteryx.ts/oac.ts/cube.ts — NOT owned/edited here) found the
 *      literal's OWN embedded "else" and both mis-split the branches AND
 *      lost part of the literal's text, producing a malformed formula with
 *      a stray literal "ELSE" keyword surviving into the output.
 *
 * See the block comment above bobjMask() in bobj.ts for the full writeup,
 * including why a SEPARATE, unrelated, PRE-EXISTING bug (the Table.Col
 * regex's `[\w ]*?` tolerating spaces, which can swallow a preceding
 * multi-word keyword phrase like "CASE WHEN" into the "table" capture) is
 * NOT addressed here — confirmed byte-identical before/after this fix, so
 * out of scope for the string-literal defect class.
 */

import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { convertBobjToSigma } from './bobj.js';

function convertOneObject(name: string, select: string) {
  const input = {
    universe: {
      name: 'Literal Masking Test Universe',
      classes: [
        {
          name: 'Orders',
          objects: [
            { name: 'Net Revenue', type: 'Measure', aggregation: 'Sum', select: 'ORDER_FACT.NET_REVENUE' },
            { name: 'Customer Key', type: 'Dimension', select: 'ORDER_FACT.CUSTOMER_KEY' },
            { name, type: 'Detail', select },
          ],
        },
        {
          name: 'Customer',
          objects: [{ name: 'Cust Key', type: 'Dimension', select: 'CUSTOMER_DIM.CUSTOMER_KEY' }],
        },
      ],
      joins: [
        { expression: 'ORDER_FACT.CUSTOMER_KEY = CUSTOMER_DIM.CUSTOMER_KEY', cardinality: 'many-to-one' },
      ],
      filters: [],
    },
  };
  return convertBobjToSigma(input, { connectionId: 'conn-1', database: 'CSA', schema: 'TJ' });
}

function tableNames(r: any): string[] {
  return r.model.pages[0].elements.map((e: any) => e.name);
}
function namedColFormula(r: any, name: string): string | undefined {
  const cols = r.model.pages[0].elements.flatMap((e: any) => e.columns || []);
  return cols.find((c: any) => c.name === name)?.formula;
}

describe('bobj literal masking: a literal is not live syntax', () => {
  test("a literal containing dotted-reference-shaped text is not treated as a real Table.Col ref, and creates no phantom table", () => {
    const r = convertOneObject('TableColInLiteral', "'See ORDER_FACT.NET_REVENUE note'");
    assert.equal(namedColFormula(r, 'Table Col in Literal'), '"See ORDER_FACT.NET_REVENUE note"',
      'the literal\'s CONTENT must survive verbatim — not become "[Net Revenue] note"');
    assert.ok(!tableNames(r).some(n => /See/i.test(n)),
      'must NOT invent a phantom table from the literal\'s own dotted-looking text');
  });

  test("a literal containing a keyword-shaped substring is not lowered by the string-function renames", () => {
    const r = convertOneObject('UpperInLiteral', "'Please upper(this) note'");
    assert.equal(namedColFormula(r, 'Upper in Literal'), '"Please upper(this) note"',
      'must not become "Please Upper(this) note"');
  });
});

describe('bobj literal masking: CASE literals containing keyword-shaped text', () => {
  test('a THEN/ELSE-branch literal containing the word "else" does not confuse the shared CASE→If() splitter', () => {
    const r = convertOneObject('CaseLiteralKeyword', "CASE WHEN 1 = 1 THEN 'high or else low' ELSE 'low' END");
    assert.equal(namedColFormula(r, 'Case Literal Keyword'), 'If(1 = 1, "high or else low", "low")',
      'must be a well-formed 3-arg If() with the literal intact, not "If(1 = 1, \\"high or, low\\" ELSE \\"low\\")"');
  });
});

describe('bobj literal masking: negative control — a real column ref still resolves', () => {
  test('a genuine Table.Col SELECT is still rewritten to [Display] and creates no extra table', () => {
    const r = convertOneObject('RealRef', 'ORDER_FACT.NET_REVENUE');
    // A plain Table.Col select is a straight column mapping, not a calculated
    // expression — confirm it lands on the real ORDER_FACT-derived table with
    // no phantom element, matching the fixed-literal test above.
    assert.ok(!tableNames(r).some(n => /RealRef/i.test(n)));
  });

  test('a genuine CASE expression without any literal keyword collision still converts', () => {
    const r = convertOneObject('RealCase', "CASE WHEN 1 = 1 THEN 'yes' ELSE 'no' END");
    assert.equal(namedColFormula(r, 'Real Case'), 'If(1 = 1, "yes", "no")');
  });
});

// Regression (round 2): widening bobjMask to also mask `"…"` spans (the
// Critical 4 fix, above) left innerIsSimpleColumn/isBareColumn out of sync
// with parseTableColTokens — they kept regex-matching the RAW select text,
// which still recognized `"TABLE"."COL"` as a bare column shape, while
// parseTableColTokens (now scanning MASKED text with the old raw-identifier-
// only regex) found zero tokens for the exact same input. That mismatch was
// live-reproduced as an unhandled crash (`tokens[0]` read as undefined) for
// a bare quoted-identifier SELECT, and as a silent table+metric drop for
// `sum("T"."C")` (isBareColumn said yes but the token list was empty, so it
// fell through to a warn-and-continue skip). All three — parseTableColTokens,
// innerIsSimpleColumn/isBareColumn, and translateBobjExpr's Table.Col →
// [Display] rewrite — now share one mask-aware regex + resolver
// (BOBJ_TABLECOL_SRC / bobjResolveToken), so they can't drift apart again.
describe('bobj literal masking: a genuine double-quoted identifier Table.Col ref', () => {
  test('a bare "TABLE"."COL" SELECT does not crash and creates no phantom table', () => {
    const r = convertOneObject('QuotedIdentRef', '"ORDER_FACT"."CUSTOMER_KEY"');
    assert.ok(!tableNames(r).some(n => /QuotedIdentRef/i.test(n)),
      'must resolve as a straight column mapping onto the real table, not a phantom element');
  });

  test('sum("T"."C") — a double-quoted identifier inside an aggregate still emits a metric on the real table', () => {
    const r = convertOneObject('QuotedIdentAgg', 'sum("ORDER_FACT"."NET_REVENUE")');
    assert.equal(namedColFormula(r, 'Quoted Ident Agg'), undefined,
      'this is a MEASURE, not a column — it must not appear in namedColFormula\'s column search');
    const metrics = r.model.pages[0].elements.flatMap((e: any) => e.metrics || []);
    const m = metrics.find((mm: any) => mm.name === 'Quoted Ident Agg');
    assert.ok(m, 'must still emit a metric — must not silently drop it');
    assert.equal(m!.formula, 'Sum([Net Revenue])');
  });

  test('mixed quoting — "TABLE".COL and TABLE."COL" both still resolve', () => {
    const r1 = convertOneObject('MixedA', '"ORDER_FACT".CUSTOMER_KEY');
    assert.ok(!tableNames(r1).some(n => /MixedA/i.test(n)));
    const r2 = convertOneObject('MixedB', 'ORDER_FACT."CUSTOMER_KEY"');
    assert.ok(!tableNames(r2).some(n => /MixedB/i.test(n)));
  });
});
