/**
 * Bug (pre-existing, predates all of today's literal-masking work, no
 * literals involved): tableauIfToSigma (~formulas.ts) and tableauCaseToSigma
 * both find their block's closing keyword with a non-greedy, FIRST-MATCH
 * regex —
 *
 *   /\bIF\b([\s\S]+?)\bEND\b/gi
 *   /\bCASE\b([\s\S]+?)\bEND\b/gi
 *
 * — which stops at the FIRST "END" in the string regardless of nesting
 * depth. A nested IF or CASE inside a THEN/ELSE branch has its own END, so
 * the outer block's true closing END (and everything up to it — the real
 * ELSE branch, the real closing structure) is missed entirely.
 *
 * Demonstrated (live-reproduced, pre-fix, via a /tmp script importing
 * formulas.ts by absolute path):
 *   IF [a]=1 THEN IF [b]=2 THEN 'x' ELSE 'y' END ELSE 'z' END
 *     -> `If([a]=1, IF [b]=2, "y") ELSE "z" END`     (garbage, not a crash)
 *   IF [a]=1 THEN CASE [b] WHEN 2 THEN 'x' ELSE 'y' END ELSE 'z' END
 *     -> `If([a]=1, "z"`                              (whole outer IF lost)
 *
 * Fix: replace the first-match regex with a depth-counting scanner
 * (`_scanTableauBlock`) that treats ANY nested IF or CASE as bumping a
 * shared block-nesting depth counter (so IF-inside-CASE and CASE-inside-IF
 * both nest correctly against one counter, not two independent regexes each
 * blind to the other's keywords) and only recognizes THEN/ELSEIF/ELSE/WHEN
 * as belonging to the block that opened this scan when they occur at depth
 * 1 relative to it. The correctly-bounded THEN/ELSE (or WHEN/THEN/ELSE)
 * branch text is then handed to `_tableauRecurse`, which converts it via a
 * FRESH `tableauFormulaToSigma` call — so an inner control structure is
 * converted by its own translator, not by the outer block's naive split.
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { tableauFormulaToSigma } from './formulas.js';

const conv = (f: string) => tableauFormulaToSigma(f);

describe('nested IF/CASE: single-level IF must keep working', () => {
  test(`IF [x] = 1 THEN 'a' ELSE 'b' END`, () => {
    assert.equal(conv(`IF [x] = 1 THEN 'a' ELSE 'b' END`), `If([x] = 1, "a", "b")`);
  });
  test('single-level IF with ELSEIF still converts', () => {
    assert.equal(
      conv(`IF [x] = 1 THEN 'a' ELSEIF [x] = 2 THEN 'b' ELSE 'c' END`),
      `If([x] = 1, "a", If([x] = 2, "b", "c"))`,
    );
  });
  test('single-level CASE must keep working', () => {
    assert.equal(
      conv(`CASE [x] WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END`),
      `If([x] = 1, "a", If([x] = 2, "b", "c"))`,
    );
  });
});

describe('nested IF/CASE: one level of IF-inside-IF nesting', () => {
  test('IF nested inside another IF THEN branch converts both levels correctly', () => {
    assert.equal(
      conv(`IF [a] = 1 THEN IF [b] = 2 THEN 'x' ELSE 'y' END ELSE 'z' END`),
      `If([a] = 1, If([b] = 2, "x", "y"), "z")`,
    );
  });
  test('IF nested inside another IF ELSE branch converts both levels correctly', () => {
    assert.equal(
      conv(`IF [a] = 1 THEN 'z' ELSE IF [b] = 2 THEN 'x' ELSE 'y' END END`),
      `If([a] = 1, "z", If([b] = 2, "x", "y"))`,
    );
  });
});

describe('nested IF/CASE: CASE nested inside IF', () => {
  test('CASE nested inside an IF THEN branch converts both levels correctly', () => {
    assert.equal(
      conv(`IF [a] = 1 THEN CASE [b] WHEN 2 THEN 'x' ELSE 'y' END ELSE 'z' END`),
      `If([a] = 1, If([b] = 2, "x", "y"), "z")`,
    );
  });
  test('IF nested inside a CASE WHEN branch converts both levels correctly', () => {
    assert.equal(
      conv(`CASE [a] WHEN 1 THEN IF [b] = 2 THEN 'x' ELSE 'y' END ELSE 'z' END`),
      `If([a] = 1, If([b] = 2, "x", "y"), "z")`,
    );
  });
});
