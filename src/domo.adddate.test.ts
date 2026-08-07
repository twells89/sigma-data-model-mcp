// bead beads-sigma-zmnt — MySQL ADDDATE and its family were never rewritten to
// Sigma's DateAdd. The second half of the same defect that beads-sigma-znvg's
// DATEDIFF fix (#122) addressed, surfacing through the identical mechanism:
// none of these names is in LOOK_FUNC_MAP, so lookConvertExpression's pass 1
// fell through to _naiveTitleCase and emitted `Adddate(...)` — a function Sigma
// does not have.
//
// MEASURED, on domo-to-sigma's live 36-card cold run
// (~/domo-coldrun-v4/discovery/formulas.json, 81 Beast Modes, run through the
// CURRENTLY VENDORED converter/sql.mjs pinned at 0641a62):
//   * 27 ADDDATE call sites across 7 Beast Modes, every one emitted as `Adddate(`
//     — Series · % Change - Visits · Last 28 Days ·
//       %Change - Daily Unique Visitors (Wk/Wk) · % Change - Pageviews ·
//       % Change - Visitors · Common Date
//   * 109 DATEDIFF sites, ALL now correctly carrying a quoted unit — #122 took;
//     ADDDATE is the sole remaining converter-level defect in this corpus.
//   * the converter's own oracle already knew:
//       lookUnknownFunctions('Adddate(Today(),-1)') -> ["ADDDATE"]
//     but convert-beast-modes.rb surfaces that as a WARNING only, so nothing
//     failed and the run carried the bad formulas live.
//
// TWO THINGS CHANGE, NOT ONE. MySQL is `(date, amount)`; Sigma is
// `(datepart, amount, date)`. The date operand moves from FIRST to LAST *and* a
// unit is prepended. A fix that only corrected the spelling would leave the date
// sitting where Sigma expects the amount — so these tests assert operand
// POSITION explicitly, never merely that `DateAdd` appeared.
//
// The rule was already specified in domo-to-sigma's own
// refs/beast-mode-to-sigma.md:209-212 — `DATE_ADD(d, interval n unit)` /
// `ADDDATE` -> `DateAdd("unit", n, [d])`, `DATE_SUB` / `SUBDATE` ->
// `DateAdd("unit", -n, [d])`. The converter simply never implemented it.
//
// INPUTS ARE MEASURED, NOT INVENTED: every ADDDATE span below is one of the 3
// distinct call shapes extracted from that run's own normalizedSql, or the exact
// nesting they appear in.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { lookConvertExpression, lookSqlToSigmaRules, lookUnknownFunctions } from './formulas.js';

// The two entry points tools/vendor-converters.sh pins for the domo bundle, in
// the order domo's convert-beast-modes.rb tries them.
function convert(sql: string): string {
  return lookSqlToSigmaRules(sql) ?? lookConvertExpression(sql);
}

function assertOrder(out: string, first: string, second: string, why: string) {
  const i = out.indexOf(first), j = out.indexOf(second);
  assert.ok(i !== -1, `expected ${first} in output, got: ${out}`);
  assert.ok(j !== -1, `expected ${second} in output, got: ${out}`);
  assert.ok(i < j, `${why}\n  got: ${out}`);
}

/** No output may still contain the naive-title-case passthrough spellings. */
function assertNoPassthrough(out: string) {
  assert.ok(!/\b(Adddate|Subdate|Addtime|Subtime|Date_add|Date_sub)\s*\(/i.test(out),
    `output still carries an untranslated MySQL date-add call: ${out}`);
}

test('zmnt: measured shape 1 — AddDate(Current_Date(),-1)', () => {
  const out = convert('AddDate(Current_Date(),-1)');
  assertNoPassthrough(out);
  assert.match(out, /DateAdd\(\s*"day"\s*,\s*-1\s*,\s*Today\(\)\s*\)/i,
    `expected DateAdd("day", -1, Today()), got: ${out}`);
});

test('zmnt: measured shape 2 — AddDate([Date],28) moves the date operand LAST', () => {
  const out = convert('AddDate([Date],28)');
  assertNoPassthrough(out);
  assert.match(out, /DateAdd\(\s*"day"\s*,/i, `expected a "day" datepart first, got: ${out}`);
  assertOrder(out, '28', '[Date]',
    'MySQL ADDDATE is (date, amount); Sigma DateAdd is (unit, amount, date). ' +
    'The amount must precede the date, or Sigma reads the date as the amount.');
});

test('zmnt: measured shape 3 — a parenthesised arithmetic amount stays one argument', () => {
  const out = convert('AddDate([Date],(52 * 7))');
  assertNoPassthrough(out);
  assert.match(out, /DateAdd\(\s*"day"\s*,\s*\(52 \* 7\)\s*,\s*\[Date\]\s*\)/i,
    `the (52 * 7) amount must survive intact and precede [Date], got: ${out}`);
});

test('zmnt: the live nesting — ADDDATE inside DATEDIFF inside CASE (both bugs at once)', () => {
  // Verbatim from `% Change - Pageviews` / `Series` / `Last 28 Days`, the shape
  // that carries beads-sigma-znvg AND beads-sigma-zmnt in one expression. Before
  // this fix it emitted DateDiff(Adddate(Today(),-1),[Date]).
  const out = convert(
    "(CASE WHEN ((DateDiff(AddDate(Current_Date(),-1),[Date]) < 28) " +
    "AND (DateDiff(Current_Date(),[Date]) > 0)) THEN 'Last 28 Days' ELSE 'Older' END)");
  assertNoPassthrough(out);
  // The CASE pass must not have carried the ADDDATE out of reach.
  assert.match(out, /DateAdd\(\s*"day"\s*,\s*-1\s*,\s*Today\(\)\s*\)/i,
    `ADDDATE nested inside a WHEN span must still be rewritten, got: ${out}`);
  // And the enclosing DATEDIFF keeps its own znvg fix: MySQL (end, start) means
  // yesterday is the END, so it must land LAST.
  assertOrder(out, '[Date]', 'DateAdd(',
    'DATEDIFF(yesterday, [Date]) is "days from [Date] to yesterday": [Date] is ' +
    'the START and must precede the DateAdd(...) END operand.');
});

test('zmnt: every ADDDATE in a multi-branch CASE is rewritten, not just the first', () => {
  // `Common Date` has ADDDATE in both a predicate and a THEN value.
  const out = convert(
    "(CASE WHEN (DateDiff(AddDate(Current_Date(),-1),[Date]) < 28) THEN [Date] " +
    "WHEN (DateDiff(AddDate(Current_Date(),-1),[Date]) < (28 + 28)) THEN AddDate([Date],28) " +
    "ELSE [Date] END)");
  assertNoPassthrough(out);
  const hits = out.match(/DateAdd\(\s*"day"\s*,/gi) || [];
  assert.equal(hits.length, 3, `all three ADDDATE sites must be rewritten, got: ${out}`);
});

test('zmnt: T-SQL/Snowflake DATEADD is ALREADY (unit, n, date) and must NOT be reordered', () => {
  // The deliberate asymmetry. LOOK_FUNC_MAP's bare-name rename is CORRECT for
  // this spelling; touching it would break formulas that work today.
  const out = convert("DATEADD('day', 7, [Order Date])");
  assertOrder(out, '7', '[Order Date]', 'DATEADD keeps its source argument order.');
  assert.match(out, /DateAdd\(\s*"day"\s*,\s*7\s*,\s*\[Order Date\]\s*\)/i,
    `DATEADD must pass through in source order, got: ${out}`);
});

test('zmnt: SUBDATE negates the amount (Sigma has no DateSub)', () => {
  const out = convert('SUBDATE([Order Date], 30)');
  assertNoPassthrough(out);
  assert.match(out, /DateAdd\(\s*"day"\s*,\s*-30\s*,\s*\[Order Date\]\s*\)/i,
    `SUBDATE(d, 30) -> DateAdd("day", -30, [d]), got: ${out}`);
});

test('zmnt: SUBDATE of a negative literal flips back to positive', () => {
  const out = convert('SUBDATE([Order Date], -7)');
  assert.match(out, /DateAdd\(\s*"day"\s*,\s*7\s*,\s*\[Order Date\]\s*\)/i,
    `double negative must resolve to +7, got: ${out}`);
});

test('zmnt: SUBDATE of a compound amount is WRAPPED, not sign-prefixed', () => {
  // `-52 * 7` is -364 only by operator precedence luck; `-(a - b)` vs `-a - b`
  // is a genuine wrong-number bug. The whole expression must be negated.
  const out = convert('SUBDATE([Date], 28 - 7)');
  assert.match(out, /DateAdd\(\s*"day"\s*,\s*-\(28 - 7\)\s*,\s*\[Date\]\s*\)/i,
    `a compound amount must be negated as a whole, got: ${out}`);
});

test('zmnt: ADDTIME/SUBTIME are REFUSED, and stay visible in the unknown-function report', () => {
  // An earlier cut of this fix mapped these to DateAdd("second", n, t), copying
  // domo-to-sigma's refs/beast-mode-to-sigma.md:211-212. That reference is wrong
  // about MySQL and the mapping was a silent-wrong-number bug:
  //   * expr2 is a TIME EXPRESSION. The documented common form is a quoted
  //     literal — ADDTIME([t], '01:30:00') — which produced
  //     DateAdd("second", "01:30:00", [t]): a string in a numeric slot.
  //   * a bare integer is not seconds either. MySQL parses unquoted numeric
  //     time elapsed-time-style (manual's own example: 1112 -> '00:11:12',
  //     672s). So ADDTIME([t], 3600) means 2160s, not 3600s.
  // Neither appears in the 81-formula live corpus, so mapping them bought
  // nothing. Left unmapped they still reach the operator via
  // lookUnknownFunctions; mapped, they converted silently AND vanished from
  // that report — strictly worse. Refuse, exactly as for MICROSECOND below.
  for (const sql of ["ADDTIME([t], '01:30:00')", 'ADDTIME([t], 3600)', 'SUBTIME([t], 90)']) {
    const out = convert(sql);
    assert.ok(!/DateAdd\(/i.test(out),
      `${sql} must NOT be converted — MySQL's expr2 is a TIME expr, not seconds. Got: ${out}`);
  }
  assert.deepEqual(lookUnknownFunctions('ADDTIME([t], 3600)'), ['ADDTIME'],
    'and it must remain visible in the unknown-function report');
  assert.deepEqual(lookUnknownFunctions('SUBTIME([t], 90)'), ['SUBTIME']);
});

test('zmnt: an apostrophe inside a bracketed identifier does not defeat the rewrite', () => {
  // _splitTopLevelArgs used to test for a quote BEFORE resolving bracket state,
  // so `[Manager's Approval]` opened a quote that never closed and swallowed the
  // argument-separating comma — the call was then silently left unconverted.
  // Same not-bracket-atomic class as bead beads-sigma-k8hv. The sibling
  // _rewriteMysqlDateDiff (from #122) shares the splitter and was equally
  // affected, so this guards both.
  const out = convert("ADDDATE([Manager's Approval], 7)");
  assert.match(out, /DateAdd\(\s*"day"\s*,\s*7\s*,\s*\[Manager's Approval\]\s*\)/i,
    `the bracketed name must stay atomic and the call must still convert, got: ${out}`);

  const dd = convert("DATEDIFF(current_date(), [Manager's Approval])");
  assert.match(dd, /DateDiff\(\s*"day"\s*,\s*\[Manager's Approval\]\s*,\s*Today\(\)\s*\)/i,
    `#122's DATEDIFF rewrite shares the splitter and must also survive, got: ${dd}`);
});

test('zmnt: the INTERVAL form supplies the unit', () => {
  const out = convert('DATE_ADD([Order Date], INTERVAL 3 MONTH)');
  assertNoPassthrough(out);
  assert.match(out, /DateAdd\(\s*"month"\s*,\s*3\s*,\s*\[Order Date\]\s*\)/i,
    `INTERVAL 3 MONTH must set the datepart to "month", got: ${out}`);
});

test('zmnt: DATE_SUB with INTERVAL negates and keeps the unit', () => {
  const out = convert('DATE_SUB([Order Date], INTERVAL 2 WEEK)');
  assert.match(out, /DateAdd\(\s*"week"\s*,\s*-2\s*,\s*\[Order Date\]\s*\)/i,
    `DATE_SUB(..., INTERVAL 2 WEEK) -> DateAdd("week", -2, ...), got: ${out}`);
});

test('zmnt: an INTERVAL unit Sigma has no datepart for is left alone and stays REPORTABLE', () => {
  // Refusing beats guessing: MICROSECOND has no Sigma datepart, so the call is
  // left verbatim and lookUnknownFunctions still names it. Silently mapping it
  // to "second" would be a 1,000,000x wrong number with no error anywhere.
  const src = 'DATE_ADD([t], INTERVAL 5 MICROSECOND)';
  const out = convert(src);
  assert.ok(!/DateAdd\(\s*"(second|day)"/i.test(out),
    `an unmappable INTERVAL unit must not be silently coerced, got: ${out}`);
  assert.deepEqual(lookUnknownFunctions(src), ['DATE_ADD'],
    'the untranslated call must still be reported by the converter\'s own oracle');
});

test('zmnt: a nested call inside the date operand survives the split', () => {
  const out = convert("ADDDATE(DATE_TRUNC('month',[created_on]), 1)");
  assertNoPassthrough(out);
  assert.match(out, /DateAdd\(\s*"day"\s*,\s*1\s*,\s*DateTrunc\(/i,
    `the nested DATE_TRUNC(...) must land intact as the date operand, got: ${out}`);
});

test('zmnt: an unbalanced call is left exactly as found, never half-rewritten', () => {
  const out = convert('ADDDATE([Date], 1');
  assert.ok(!/DateAdd\(/i.test(out),
    `an unbalanced call must not be partially rewritten, got: ${out}`);
});

test('zmnt: a quoted string containing the word adddate is untouched', () => {
  // The rewrite runs on literal-MASKED text precisely so this can never happen.
  const out = convert("CASE WHEN [note] = 'adddate(x,1)' THEN 'y' ELSE 'n' END");
  assert.match(out, /"adddate\(x,1\)"/,
    `a quoted literal must pass through byte-identical, got: ${out}`);
});

test('zmnt: the unknown-function oracle stops crying wolf over the whole family', () => {
  // These seven all convert correctly but were still being reported as "no Sigma
  // mapping" — TIMEDIFF has been a false positive since #122. The warning is
  // operator-facing (domo convert-beast-modes.rb:562) and this bead's own
  // recommendation is to promote it to a hard failure, which standing false
  // positives would make impossible.
  for (const sql of [
    'ADDDATE([d],1)', 'SUBDATE([d],1)',
    'DATE_ADD([d], INTERVAL 1 DAY)', 'DATE_SUB([d], INTERVAL 1 DAY)',
    'TIMEDIFF([a],[b])',
  ]) {
    assert.deepEqual(lookUnknownFunctions(sql), [],
      `${sql} is translated correctly and must not be reported as unknown`);
  }
  // …without going blind: a genuinely unmapped name is still reported.
  assert.deepEqual(lookUnknownFunctions('LEVENSHTEIN([a],[b])'), ['LEVENSHTEIN']);
});

test('zmnt: the measured corpus shape converts clean end to end', () => {
  assert.deepEqual(lookUnknownFunctions('AddDate(Current_Date(),-1)'), [],
    'the live corpus\'s own ADDDATE shape must report nothing');
  assert.deepEqual(lookUnknownFunctions(convert('AddDate(Current_Date(),-1)')), [],
    'and the converted output must likewise contain no unknown function');
});
