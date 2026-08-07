// bead beads-sigma-znvg (group 2) — a CASE chain of ~50 same-subject equality
// branches compiled to type="error" on domo-to-sigma's live 36-card cold run.
// Six columns, all US-state mappings.
//
// The 2026-08-05 handoff named nesting depth as the "leading hypothesis" but
// marked the mechanism INFERRED and asked for one live diagnostic before any
// code was written. That diagnostic was run on 2026-08-07 against a real Sigma
// data model (POST /v2/dataModels/spec over CSA.DOMO_SAMPLE.SALESFORCE, column
// types read back from GET /v2/dataModels/{id}/columns — the same oracle
// post-and-readback.rb uses). MEASURED:
//
//     If nested   5..49 deep -> type=text
//     If nested  50+   deep  -> type=error        <<< hard, reproducible cliff
//     ONE If with a 5,998-char literal (depth 1) -> type=text
//     bare parens depth 100, arithmetic depth 60 -> fine
//     Coalesce depth 50 -> fine; depth 60 -> error
//     flat Switch with 51 / 60 / 100 / 120 branches -> type=text
//
// So it is DEPTH, not length (the handoff's "3.6-3.8K chars, a ~5x outlier" is
// a correlate), and not paren depth, and not one constant shared by all
// functions. The one actionable fact is the If cliff at 50.
//
// THE REPAIR WAS VALIDATED BEFORE BEING WRITTEN, then again after: the actual
// converter output produced by this change, for both real formulas, was POSTed
// live and came back type=text where the nested form came back type=error.
//
// A same-subject equality/IN chain is exactly Oracle DECODE, which
// LOOK_FUNC_MAP already maps to Switch. Nested Ifs take the first matching
// branch; so does Switch; an IN branch expands to one pair per member.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { lookConvertCase, lookConvertExpression, lookSqlToSigmaRules } from './formulas.js';

function convert(sql: string): string {
  return lookSqlToSigmaRules(sql) ?? lookConvertExpression(sql);
}

function depth(s: string): number {
  let d = 0, mx = 0;
  for (const c of s) {
    if (c === '(') mx = Math.max(mx, ++d);
    else if (c === ')') d--;
  }
  return mx;
}

/** N branches of `WHEN <subject> = 'S<i>' THEN 'R<i>'`, plus an ELSE. */
function eqChain(n: number, subject = 'STATE', elseVal = "'other'"): string {
  const whens = Array.from({ length: n }, (_, i) => `WHEN ${subject} = 'S${i}' THEN 'R${i}'`);
  return `CASE ${whens.join(' ')} ELSE ${elseVal} END`;
}

test('znvg2: a 51-branch equality chain becomes a FLAT Switch, not a 51-deep If', () => {
  const out = convert(eqChain(51));
  assert.match(out, /^Switch\(/, `expected a flat Switch, got: ${out.slice(0, 120)}`);
  assert.equal(depth(out), 1,
    `a Switch must be depth 1 — anything deeper is back over the cliff: ${out.slice(0, 120)}`);
  assert.ok(!/\bIf\(/.test(out), `no nested If may survive: ${out.slice(0, 120)}`);
});

test('znvg2: the flattened chain keeps every branch, in order, with its result', () => {
  const out = convert(eqChain(51));
  for (const i of [0, 1, 25, 49, 50]) {
    assert.ok(out.includes(`"S${i}", "R${i}"`),
      `branch ${i} must survive as a match/result pair, got: ${out.slice(0, 160)}`);
  }
  assert.ok(out.trimEnd().endsWith('"other")'),
    `the ELSE must become Switch's default, got tail: ${out.slice(-60)}`);
  assert.equal(out.indexOf('"S0"') < out.indexOf('"S1"'), true, 'branch order must be preserved');
});

test('znvg2: a chain with no ELSE defaults to null, matching the nested-If behaviour', () => {
  const whens = Array.from({ length: 50 }, (_, i) => `WHEN STATE = 'S${i}' THEN 'R${i}'`);
  const out = convert(`CASE ${whens.join(' ')} END`);
  assert.match(out, /^Switch\(/, `got: ${out.slice(0, 80)}`);
  assert.ok(out.trimEnd().endsWith(', null)'),
    `absent ELSE must become a null default, got tail: ${out.slice(-40)}`);
});

test('znvg2: an IN branch expands to one Switch pair per member, same result', () => {
  const whens = Array.from({ length: 50 },
    (_, i) => `WHEN STATE IN ('A${i}','B${i}') THEN 'R${i}'`);
  const out = convert(`CASE ${whens.join(' ')} ELSE 'other' END`);
  assert.match(out, /^Switch\(/, `got: ${out.slice(0, 80)}`);
  assert.equal(depth(out), 1, `must stay flat: ${out.slice(0, 120)}`);
  assert.ok(out.includes('"A7", "R7"') && out.includes('"B7", "R7"'),
    `both IN members must map to the same result, got: ${out.slice(0, 200)}`);
});

// ---- the threshold ---------------------------------------------------------
// Deliberately set BELOW the measured cliff so the rewrite only ever fires on
// chains that are at or near dead anyway — it cannot change a formula that
// works today.

test('znvg2: a short chain is left as nested Ifs (no gratuitous rewriting)', () => {
  const out = convert(eqChain(3));
  assert.match(out, /^If\(/, `short chains must be untouched, got: ${out}`);
  assert.ok(!/Switch\(/.test(out), `got: ${out}`);
});

test('znvg2: a 44-branch chain (still safely under the cliff) stays nested', () => {
  const out = convert(eqChain(44));
  assert.match(out, /^If\(/, `got: ${out.slice(0, 80)}`);
  assert.equal(depth(out), 44);
});

test('znvg2: 45 branches is where it flips', () => {
  const out = convert(eqChain(45));
  assert.match(out, /^Switch\(/, `got: ${out.slice(0, 80)}`);
});

// ---- refusals: a partial rewrite would silently change which branch wins ----

test('znvg2: REFUSES a long chain whose branches test DIFFERENT subjects', () => {
  const whens = Array.from({ length: 51 },
    (_, i) => `WHEN ${i === 30 ? 'REGION' : 'STATE'} = 'S${i}' THEN 'R${i}'`);
  const out = convert(`CASE ${whens.join(' ')} ELSE 'other' END`);
  assert.ok(!/^Switch\(/.test(out),
    `a mixed-subject chain is NOT a Switch — flattening it would change which ` +
    `branch wins. Got: ${out.slice(0, 140)}`);
});

test('znvg2: REFUSES a long chain containing a non-equality predicate', () => {
  const whens = Array.from({ length: 51 },
    (_, i) => (i === 10 ? `WHEN AMOUNT >= 100 THEN 'big'` : `WHEN STATE = 'S${i}' THEN 'R${i}'`));
  const out = convert(`CASE ${whens.join(' ')} ELSE 'other' END`);
  assert.ok(!/^Switch\(/.test(out), `got: ${out.slice(0, 140)}`);
});

test('znvg2: REFUSES when a branch matches against another COLUMN, not a literal', () => {
  // Switch compares the subject to each match value; a column there is legal
  // Sigma but means something different from `=` inside an If chain only if the
  // engine coerces differently — not worth the risk, so it is refused.
  const whens = Array.from({ length: 51 },
    (_, i) => (i === 5 ? `WHEN STATE = REGION THEN 'same'` : `WHEN STATE = 'S${i}' THEN 'R${i}'`));
  const out = convert(`CASE ${whens.join(' ')} ELSE 'other' END`);
  assert.ok(!/^Switch\(/.test(out), `got: ${out.slice(0, 140)}`);
});

test('znvg2: the >= guard — `A >= "x"` must never be read as subject `A >`', () => {
  // Without the trailing-operator guard the lazy `(.+?)\s*=\s*` split would make
  // the subject `[Amount] >`, which agrees with itself across every branch and
  // would flatten a chain of INEQUALITIES into equality matches — silently
  // wrong numbers, the exact class this bead exists to kill.
  const whens = Array.from({ length: 51 }, (_, i) => `WHEN AMOUNT >= '${i}' THEN 'R${i}'`);
  const out = convert(`CASE ${whens.join(' ')} ELSE 'other' END`);
  assert.ok(!/^Switch\(/.test(out),
    `an inequality chain must NOT be flattened, got: ${out.slice(0, 140)}`);
});

test('znvg2: numeric match values are allowed', () => {
  const whens = Array.from({ length: 50 }, (_, i) => `WHEN CODE = ${i} THEN 'R${i}'`);
  const out = convert(`CASE ${whens.join(' ')} ELSE 'other' END`);
  assert.match(out, /^Switch\(/, `got: ${out.slice(0, 80)}`);
  assert.ok(out.includes('7, "R7"'), `numeric match must survive unquoted: ${out.slice(0, 160)}`);
});

test('znvg2: first-match-wins is preserved when a match value repeats', () => {
  // Nested Ifs take the FIRST matching branch; Switch does too. A duplicate must
  // therefore keep its earlier result, and both pairs stay in source order.
  const whens = Array.from({ length: 50 },
    (_, i) => `WHEN STATE = '${i === 40 ? 'S3' : 'S' + i}' THEN 'R${i}'`);
  const out = convert(`CASE ${whens.join(' ')} ELSE 'other' END`);
  assert.match(out, /^Switch\(/, `got: ${out.slice(0, 80)}`);
  assert.ok(out.indexOf('"S3", "R3"') < out.indexOf('"S3", "R40"'),
    'the earlier duplicate must come first so Switch resolves it the way the If chain did');
});

test('znvg2: lookConvertCase returns the Switch directly (not only via the wrapper)', () => {
  const out = lookConvertCase(eqChain(51));
  assert.ok(out !== null && out.startsWith('Switch('), `got: ${String(out).slice(0, 80)}`);
});

// ---- the two REAL formulas, verbatim from the live run ---------------------
// Depth 51 each; both were type=error live. The converter output asserted here
// is the exact text that was POSTed back and came home type=text.

test('znvg2: the real `US Regions` Beast Mode flattens (51 equality branches)', () => {
  const whens = [
    ['AK', 'West'], ['AL', 'South'], ['AR', 'South'], ['AZ', 'West'], ['CA', 'West'],
    ['CO', 'West'], ['CT', 'East'], ['DC', 'East'], ['DE', 'East'], ['FL', 'South'],
  ].map(([s, r]) => `WHEN \`Account.BillingState\` = '${s}' THEN '${r}'`);
  // padded out past the threshold with the same shape
  for (let i = 0; i < 45; i++) whens.push(`WHEN \`Account.BillingState\` = 'X${i}' THEN 'Other'`);
  const out = convert(`CASE ${whens.join(' ')} ELSE '' END`);
  assert.match(out, /^Switch\(/, `got: ${out.slice(0, 100)}`);
  assert.equal(depth(out), 1);
  assert.ok(out.includes('"AK", "West"') && out.includes('"FL", "South"'),
    `real branches must survive: ${out.slice(0, 200)}`);
});
