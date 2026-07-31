/**
 * Qlik converter tests — string-literal-blind scanning in the measure/
 * dimension formula translator (qlikExprToSigma and its helpers).
 *
 * qlikExprToSigma's keyword/function-name scanning (RangeSum/Class/Log/Only/
 * the Set-Analysis `{…}` detector/Peek()'s field-name argument/…) used to run
 * plain regex .test()/.replace()/.match() and indexOf-based scans over the
 * raw formula text with no idea that string literals exist. Two concrete,
 * live-reproduced failures motivated the fix:
 *
 *   1. A measure whose ENTIRE expression was the single literal
 *      'RangeSum(1) legacy code' came back with the LITERAL'S CONTENT
 *      rewritten — "(Coalesce(1, 0)) legacy code" — a value that reaches a
 *      dashboard, not just a mistranslated formula.
 *   2. 'RangeSum(' & RangeSum(Sum(A), Sum(B)) — a literal containing an
 *      unbalanced "(" followed by a real, SEPARATE RangeSum() call — made the
 *      paren-depth walker (matchClose) run off the end of the string, and the
 *      real call later in the same string was silently left untranslated (a
 *      Sigma formula containing a function Sigma doesn't have).
 *
 * A `[bracketed field]` or `"quoted field"` is atomic — an apostrophe inside
 * one is part of the identifier, not a quote opener — and an unterminated
 * quote/bracket must not swallow the rest of the string. See the block
 * comment above maskQlikLiterals() in qlik.ts for the full writeup.
 */

import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { convertQlikToSigma } from './qlik.js';

function conv(measures: any[], fields?: any[]) {
  return convertQlikToSigma({
    appName: 'Literal Masking Test',
    tables: [{
      name: 'SALES',
      noOfRows: 100,
      fields: fields || [
        { name: 'A', distinctValueCount: 10 },
        { name: 'B', distinctValueCount: 10 },
      ],
    }],
    masterMeasures: measures,
    masterDimensions: [],
  }, { connectionId: 'conn-1', database: 'CSA', schema: 'TJ' });
}

function firstFormula(r: any): string | undefined {
  return r.model.pages[0].elements.flatMap((e: any) => e.metrics || [])[0]?.formula;
}

describe('qlik literal masking: a literal is not live syntax', () => {
  test('RangeSum(1) legacy code — a literal is never lowered by the RangeSum pass', () => {
    const r = conv([{ title: 'LiteralBug', expr: "'RangeSum(1) legacy code'" }]);
    assert.equal(firstFormula(r), '"RangeSum(1) legacy code"',
      'the literal\'s CONTENT must survive verbatim — not be rewritten to "(Coalesce(1, 0)) legacy code"');
  });

  test("a literal with an unbalanced paren does not swallow a real, separate RangeSum() call later in the string", () => {
    const r = conv([{ title: 'DroppedReal', expr: "'RangeSum(' & RangeSum(Sum(A), Sum(B))" }]);
    const f = firstFormula(r);
    assert.ok(f, 'must still emit a metric');
    assert.match(f!, /Coalesce\(Sum\(\[A\]\), 0\)/, 'the real RangeSum() call must be translated, not left as literal RangeSum(...) text');
    assert.match(f!, /Coalesce\(Sum\(\[B\]\), 0\)/);
    assert.doesNotMatch(f!, /RangeSum\(Sum/, 'must not contain an untranslated RangeSum(...) call — Sigma has no such function');
  });

  test('Class(A,B) bucket note — a literal is never lowered by the Class() binning pass', () => {
    const r = conv([{ title: 'ClassLiteral', expr: "'Class(A,B) bucket note'" }]);
    assert.equal(firstFormula(r), '"Class(A,B) bucket note"');
  });

  test('Log(10) explanation text — a literal is never lowered by the Log()/Log10() rename', () => {
    const r = conv([{ title: 'LogLiteral', expr: "'Log(10) explanation text'" }]);
    assert.equal(firstFormula(r), '"Log(10) explanation text"');
  });

  test('Only([A]) filter note — a literal is never stripped by the Only() unwrap', () => {
    const r = conv([{ title: 'OnlyLiteral', expr: "'Only([A]) filter note'" }]);
    assert.equal(firstFormula(r), '"Only([A]) filter note"');
  });

  test('a Set Analysis search-string value shaped like a real call does not swallow a real, separate RangeSum() call later in the string', () => {
    // translateSetAnalysis SYNTHESIZES a new Sigma double-quoted comparison
    // string out of the search value — a raw quote it builds itself, not one
    // lifted from the masked source. If that synthesis isn't ALSO routed
    // through the sentinel system, the newly-introduced raw, unbalanced "("
    // re-exposes every later pass in this qlikExprToSigma call to the exact
    // same hazard (the "hazard relocated" trap): the real RangeSum() call
    // after it gets silently dropped again, even with masking on entry.
    const r = conv(
      [{ title: 'SetLit', expr: "Sum({<Region={'Contains RangeSum( in it'}>} A) & RangeSum(Sum(A), Sum(B))" }],
      [{ name: 'Region', distinctValueCount: 4 }, { name: 'A', distinctValueCount: 10 }, { name: 'B', distinctValueCount: 10 }],
    );
    const f = firstFormula(r);
    assert.ok(f);
    assert.match(f!, /\[Region\]\s*=\s*"Contains RangeSum\( in it"/, 'the search string content must survive intact, not be lowered by the RangeSum pass');
    assert.match(f!, /Coalesce\(Sum\(\[A\]\), 0\)/, 'the real, separate RangeSum() call must still be translated');
    assert.match(f!, /Coalesce\(Sum\(\[B\]\), 0\)/);
    assert.doesNotMatch(f!, /& RangeSum\(Sum/, 'must not contain an untranslated RangeSum(...) call');
  });
});

describe('qlik literal masking: the same defect via double-quoted field refs', () => {
  test('a double-quoted field ref with an unbalanced paren does not swallow a real, separate RangeSum() call later in the string', () => {
    const r = conv([{ title: 'DQuoteDroppedReal', expr: '"RangeSum(" & RangeSum(Sum(A), Sum(B))' }]);
    const f = firstFormula(r);
    assert.ok(f, 'must still emit a metric');
    assert.match(f!, /Coalesce\(Sum\(\[A\]\), 0\)/, 'the real RangeSum() call must be translated');
    assert.match(f!, /Coalesce\(Sum\(\[B\]\), 0\)/);
    assert.doesNotMatch(f!, /RangeSum\(Sum/, 'must not contain an untranslated RangeSum(...) call');
  });
});

describe('qlik literal masking: a quoted field-reference apostrophe is not a quote opener', () => {
  test("RangeSum([Manager's Approval], 5) — an apostrophe inside a bracketed field ref does not break translation", () => {
    const r = conv(
      [{ title: 'BracketApostrophe', expr: "RangeSum([Manager's Approval], 5)" }],
      [{ name: "Manager's Approval", distinctValueCount: 10 }, { name: 'B', distinctValueCount: 10 }],
    );
    const f = firstFormula(r);
    assert.ok(f, 'must still emit a metric');
    assert.match(f!, /Coalesce\(\s*\[Manager's Approval\]\s*,\s*0\)/, 'the RangeSum() call must be translated despite the apostrophe');
    assert.match(f!, /Coalesce\(5, 0\)/);
  });

  test('RangeSum("Manager\'s Approval", 5) — an apostrophe inside a double-quoted field ref does not break translation', () => {
    const r = conv(
      [{ title: 'DQuoteApostrophe', expr: `RangeSum("Manager's Approval", 5)` }],
      [{ name: "Manager's Approval", distinctValueCount: 10 }, { name: 'B', distinctValueCount: 10 }],
    );
    const f = firstFormula(r);
    assert.ok(f, 'must still emit a metric');
    assert.match(f!, /Coalesce\(\s*"Manager's Approval"\s*,\s*0\)/);
    assert.match(f!, /Coalesce\(5, 0\)/);
  });
});

describe('qlik literal masking: an unterminated quote does not swallow the rest of the formula', () => {
  test('an unclosed literal is left as plain text, and a real RangeSum() call further along still translates', () => {
    const r = conv([{ title: 'Unterminated', expr: "'unterminated literal & RangeSum(Sum(A), Sum(B))" }]);
    const f = firstFormula(r);
    assert.ok(f, 'must not crash and must still emit a metric');
    assert.match(f!, /Coalesce\(Sum\(\[A\]\), 0\)/, 'the real call after the unterminated quote must still be translated');
    assert.match(f!, /Coalesce\(Sum\(\[B\]\), 0\)/);
  });
});

// Regression: maskQlikLiterals (the entry mask, applied to the WHOLE formula
// before Set Analysis translation) sentinels a `[Bracketed Field]` or
// `"Quoted Field"` name exactly like it sentinels a `'value'` literal — it
// can't tell "field name" (structural syntax) from "data" by delimiter alone.
// clauseToCondition's FIELD-op-{body} regex expects to see the field name as
// literal bracket/quote text; once masked, that regex simply failed to match
// at all (there is no more `[`/`"` character for it to see), so the ENTIRE
// clause — and the whole Set Analysis measure — silently returned no metric.
// Live-reproduced: Sum({<[Sales Region]={'West'}>} A) emitted NOTHING on the
// broken branch, vs. `Sum(If( [Sales Region] = "West" , [A] , 0))` on main.
// clauseToCondition now resolves a masked-sentinel field name back to raw
// text (stripping delimiters) before matching, so both forms work again —
// and a double-quoted field (which main's own regex never handled either,
// since it only special-cased `[...]`) now works too.
describe('qlik literal masking: Set Analysis over a bracketed/quoted field name', () => {
  const fields = [{ name: 'Sales Region', distinctValueCount: 10 }, { name: 'A', distinctValueCount: 10 }];

  test('Sum({<[Sales Region]={\'West\'}>} A) — a bracketed field name in a Set Analysis clause still emits a metric', () => {
    const r = conv([{ title: 'BracketedFieldSetAnalysis', expr: "Sum({<[Sales Region]={'West'}>} A)" }], fields);
    const f = firstFormula(r);
    assert.ok(f, 'must emit a metric — must not silently drop the whole measure');
    assert.match(f!, /\[Sales Region\]\s*=\s*"West"/, 'the field name and comparison value must both survive, unmangled');
    assert.match(f!, /Sum\(If\(.*\[A\].*0\)\)/, 'the aggregation must still wrap an If() over [A]');
  });

  test('Sum({<[Sales Region]-={\'West\'}>} A) — exclusion (-=) on a bracketed field name still emits a metric', () => {
    const r = conv([{ title: 'BracketedFieldExclusion', expr: "Sum({<[Sales Region]-={'West'}>} A)" }], fields);
    const f = firstFormula(r);
    assert.ok(f, 'must emit a metric');
    assert.match(f!, /\[Sales Region\]\s*<>\s*"West"/, 'exclusion must translate to <>, not =');
  });

  test('Sum({<"Sales Region"={\'West\'}>} A) — a double-quoted field name in a Set Analysis clause emits a metric', () => {
    const r = conv([{ title: 'QuotedFieldSetAnalysis', expr: `Sum({<"Sales Region"={'West'}>} A)` }], fields);
    const f = firstFormula(r);
    assert.ok(f, 'must emit a metric — the double-quoted field form must resolve just like the bracketed form');
    assert.match(f!, /\[Sales Region\]\s*=\s*"West"/);
  });
});
