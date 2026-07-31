/**
 * Bug: tableauParamSwitchToSigma parses `when <literal> then <value>` pairs
 * with a regex that runs directly on the RAW (unmasked) case body:
 *
 *   /\bwhen\s+(...)\s+then\s+([\s\S]*?)(?=\s*\bwhen\b|\s*\belse\b|$)/gi
 *
 * The `then` capture is non-greedy, bounded by a lookahead for the next
 * `when`/`else` keyword. Because nothing masks string literals first, a
 * `then`-value literal that itself contains the bare word "when" (or "else")
 * satisfies that lookahead EARLY and truncates the value mid-literal —
 * silently producing a WRONG (and here, syntactically broken) Sigma formula,
 * not a crash.
 *
 * Demonstrated (live-reproduced against pre-fix code, via a /tmp script that
 * imported formulas.ts by absolute path):
 *   case [Parameters].[Param1] when 'Signs' then 'Value when true' else 'Default' end
 *   -> cases[0].then === "'Value"            (should be `"Value when true"`)
 *   -> switchFormula contains `'Value, "Default"` (unterminated literal, dangling comma)
 *
 * Fix: mask the case body's string literals ONCE (both single- and
 * double-quoted — this is Tableau calc syntax, not SQL, so `"..."` is a
 * string, not a quoted identifier; that's why this reuses the Tableau-side
 * `_maskTableauLiterals`/`_restoreRawTableauLiterals` pair rather than SQL's
 * single-quote-only `_maskLiterals`) before running the when/then/else
 * split. This mirrors the mask-before-split contract tableauIfToSigma/
 * tableauCaseToSigma already use for the identical class of bug (see
 * tableau.formula-literal-masking.test.ts).
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { tableauParamSwitchToSigma } from './formulas.js';

describe('tableauParamSwitchToSigma: a then-value literal containing "when" does not truncate', () => {
  test('single-quoted then-value containing the bare word "when"', () => {
    const r = tableauParamSwitchToSigma(
      `case [Parameters].[Param1] when 'Signs' then 'Value when true' else 'Default' end`,
      'ctl-123',
    );
    assert.ok(r, 'parsed as a param-switch');
    assert.equal(r!.cases.length, 1);
    assert.equal(r!.cases[0].when, 'Signs');
    assert.equal(r!.cases[0].then, `"Value when true"`);
    assert.equal(r!.elseExpr, `"Default"`);
    assert.equal(
      r!.switchFormula,
      `Switch([ctl-123], "Signs", "Value when true", "Default")`,
    );
  });

  test('then-value literal containing the bare word "else" does not truncate', () => {
    const r = tableauParamSwitchToSigma(
      `case [Parameters].[Param1] when 'A' then 'pick this or else that' when 'B' then 'ok' end`,
      'ctl-9',
    );
    assert.ok(r);
    assert.equal(r!.cases.length, 2);
    assert.equal(r!.cases[0].then, `"pick this or else that"`);
    assert.equal(r!.cases[1].when, 'B');
    assert.equal(r!.cases[1].then, `"ok"`);
  });

  test('double-quoted then-value containing the bare word "when" (Tableau accepts both quote styles)', () => {
    const r = tableauParamSwitchToSigma(
      `case [Parameters].[Param1] when 'X' then "Shown when active" end`,
      'ctl-7',
    );
    assert.ok(r);
    assert.equal(r!.cases[0].then, `"Shown when active"`);
  });

  test('control: no literal keyword collision still works exactly as before', () => {
    const r = tableauParamSwitchToSigma(
      `case [Parameters].[Parameter 17] when 'Signs' then [Signs - Actuals] when 'TAM' then sum([INCREMENTAL_SIGN_TAM]) end`,
      'ctl-parameter-17',
    );
    assert.ok(r);
    assert.equal(r!.paramName, 'Parameter 17');
    assert.equal(r!.cases.length, 2);
    assert.equal(r!.cases[0].when, 'Signs');
    assert.match(r!.switchFormula, /^Switch\(\[ctl-parameter-17\], "Signs", /);
    assert.match(r!.switchFormula, /"TAM", Sum\(\[Incremental Sign Tam\]\)/);
  });

  test('control: else branch + entity-encoded when-values still work', () => {
    const r = tableauParamSwitchToSigma(
      `case [Parameters].[P] when &quot;A&quot; then [X] else [Y] end`, 'ctl-p');
    assert.ok(r);
    assert.equal(r!.elseExpr, '[Y]');
    assert.match(r!.switchFormula, /Switch\(\[ctl-p\], "A", \[X\], \[Y\]\)/);
  });

  test('control: non-switch formula still returns null', () => {
    assert.equal(tableauParamSwitchToSigma('If([X] = 1, 2, 3)', 'ctl-p'), null);
  });
});
