// bead beads-sigma-znvg — 2-argument DATEDIFF, the dominant cause of the 15
// columns that compiled to type="error" on domo-to-sigma's live 36-card cold run.
//
// Domo Beast Modes are MySQL dialect, where DATEDIFF(expr1, expr2) is
// expr1 - expr2 in days — i.e. (END, START). Sigma's is
// DateDiff(datepart, start, end) — (START, END), with an explicit unit.
// LOOK_FUNC_MAP renames DATEDIFF -> DateDiff by BARE NAME (formulas.ts:305,
// applied in lookConvertExpression's pass 1), so nothing ever fixed the arity or
// the operand order. `datediff(current_date(),[Date])` came out as
// `DateDiff(Today(),[Date])`: wrong arity AND wrong operand order.
//
// The existing 3-arg handling (lookSqlToSigmaRules "Pattern 3") is anchored to
// the WHOLE expression, so it never fires for the real corpus, where every
// DATEDIFF is nested inside an aggregate or a comparison.
//
// THE DANGEROUS HALF IS THE OPERAND ORDER. Fixing only the arity yields
// `DateDiff("day", Today(), [Date])`, which COMPILES CLEANLY and returns the
// NEGATION of the intended value, so every `>= 7` / `< 28` / `<= 30` predicate
// built on it silently inverts and the KPI is simply wrong with no error
// anywhere. These tests therefore assert operand ORDER explicitly, never just
// that a unit appeared.
//
// The skill's own reference already documented the rule
// (domo-to-sigma refs/beast-mode-to-sigma.md: `DATEDIFF(a, b)` ->
// `DateDiff("day", [b], [a])` "(mind arg order: BM is (end, start))");
// the converter simply never implemented it, and the 2-arg form had ZERO test
// coverage.
//
// INPUTS ARE MEASURED, NOT INVENTED: all five call sites below are the distinct
// DATEDIFF spans extracted from the cold run's own
// discovery/formulas.json normalizedSql (run dir ~/domo-coldrun-v4).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { lookConvertExpression, lookSqlToSigmaRules } from './formulas.js';

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

test('znvg: measured site 1 — datediff(current_date(),[Date])', () => {
  const out = convert('datediff(current_date(),[Date])');
  assert.match(out, /DateDiff\(\s*"day"\s*,/i, `expected a "day" datepart first, got: ${out}`);
  assertOrder(out, '[Date]', 'Today()',
    'operands NOT swapped: Domo\'s first arg is the END and must land LAST, ' +
    'or "days since [Date]" comes back negated and every window predicate inverts.');
});

test('znvg: measured site 2 — mixed-case DateDiff(Current_Date(),[Date])', () => {
  const out = convert('DateDiff(Current_Date(),[Date])');
  assert.match(out, /DateDiff\(\s*"day"\s*,/i, `expected the unit, got: ${out}`);
  assertOrder(out, '[Date]', 'Today()', 'operand order must not depend on source casing.');
});

test('znvg: measured site 3 — DATEDIFF(current_date(),[created_on])', () => {
  const out = convert('DATEDIFF(current_date(),[created_on])');
  assert.match(out, /DateDiff\(\s*"day"\s*,/i, `expected the unit, got: ${out}`);
  assertOrder(out, '[created_on]', 'Today()', 'snake_case column operand must still land first.');
});

test('znvg: measured site 4 — DATEDIFF([CloseDate],[CreatedDate]) swaps two columns', () => {
  // No Today() here, so this is the case where a half-fix is invisible: both
  // operands are columns and the only error is the sign.
  const out = convert('DATEDIFF([CloseDate],[CreatedDate])');
  assert.match(out, /DateDiff\(\s*"day"\s*,/i, `expected the unit, got: ${out}`);
  assertOrder(out, '[CreatedDate]', '[CloseDate]',
    'DATEDIFF(CloseDate, CreatedDate) is "close minus created", so CreatedDate ' +
    'is the START and must come first.');
});

test('znvg: measured site 5 — datediff(current_date(),[Created At])', () => {
  const out = convert('datediff(current_date(),[Created At])');
  assert.match(out, /DateDiff\(\s*"day"\s*,/i, `expected the unit, got: ${out}`);
  assertOrder(out, '[Created At]', 'Today()', 'spaced display name must survive as the START operand.');
});

test('znvg: the real nesting — inside Sum(If(...)) with a window predicate', () => {
  // Shape of the live "Unique Fan Adds" KPI, which is one of the 9 error columns.
  const out = convert(
    'Sum(If((datediff(current_date(),[Date]) - 1) >= 7 AND (datediff(current_date(),[Date]) - 1) < 14, [Unique Fan Adds], 0))');
  assert.ok(!/DateDiff\(\s*Today\(\)/i.test(out),
    `no DateDiff may still lead with Today(), got: ${out}`);
  const hits = out.match(/DateDiff\(\s*"day"\s*,/gi) || [];
  assert.equal(hits.length, 2, `both nested DATEDIFFs must be rewritten, got: ${out}`);
});

test('znvg: the 3-arg quoted-unit form is NOT swapped (already Sigma order)', () => {
  // Swapping here would BREAK formulas that are currently correct.
  const out = convert("DATEDIFF('day', [created_on], [closed_on])");
  assertOrder(out, '[created_on]', '[closed_on]',
    '3-arg form is already (unit, start, end) and must pass through in source order.');
  assert.match(out, /DateDiff\(\s*"day"\s*,/i, `unit should be double-quoted, got: ${out}`);
});

test('znvg: a nested-call argument survives the split (paren-aware, not comma-naive)', () => {
  const out = convert("DATEDIFF(current_date(), DATE_TRUNC('month',[created_on]))");
  assert.match(out, /DateDiff\(\s*"day"\s*,\s*DateTrunc\(/i,
    `the nested DATE_TRUNC(...) must land intact as the START operand, got: ${out}`);
  assertOrder(out, 'DateTrunc(', 'Today()', 'nested start operand must still precede the end.');
});

test('znvg: TIMEDIFF gets seconds, and swaps (same MySQL (end,start) convention)', () => {
  const out = convert('TIMEDIFF([ended_at],[started_at])');
  assert.match(out, /DateDiff\(\s*"second"\s*,/i,
    `TIMEDIFF(a,b) -> DateDiff("second", [b], [a]) per the skill reference, got: ${out}`);
  assertOrder(out, '[started_at]', '[ended_at]', 'TIMEDIFF operands swap the same way.');
});

// --- bead beads-sigma-0goi — EXONERATION regression guard -------------------
// 0goi filed `Illinois -> IllINois` as a converter bug that "uppercases 'in'
// INSIDE string literals", and prescribed masking literals before the IN
// normalization. That diagnosis is WRONG, and this guard records why so the
// no-op fix is not attempted again.
//
// Evidence, from the cold run's own artifacts (~/domo-coldrun-v4):
//   discovery/cards.json and discovery/beast-modes.json — the rawest captures of
//   Domo's API response, written verbatim (domo-discover.rb applies no SQL
//   transformation) — ALREADY contain 'IllINois' and 'INdiana' in the Beast
//   Mode's own originalSql. The corruption is in the Domo source definition,
//   upstream of every line of our code.
//
// The converter is provably innocent: the State Beast Mode converts to
// If(In([Account.BillingState], "AL","Alabama"), ...) — valid, lint-clean Sigma
// with every literal byte-identical to its input. These assertions pin that.
test('0goi: string literals pass through byte-identical (converter exonerated)', () => {
  for (const lit of ['Illinois', 'Indiana', 'Marketing', 'Inbound', 'Washington', 'Printing', 'Insurance']) {
    const out = convert(`CASE WHEN [state] IN ('IL','${lit}') THEN '${lit}' ELSE 'Other' END`);
    assert.match(out, new RegExp(`"${lit}"`),
      `literal '${lit}' must survive verbatim, got: ${out}`);
    const upper = lit.replace(/in/g, 'IN');
    if (upper !== lit) {
      assert.ok(!out.includes(upper),
        `literal '${lit}' was corrupted to '${upper}' — 0goi would be REAL after all: ${out}`);
    }
  }
});

test('0goi: the genuine infix IN still becomes Sigma In(...)', () => {
  const out = convert("[state] IN ('Illinois','Indiana')");
  assert.match(out, /In\(/, `infix IN should convert to In(...), got: ${out}`);
  assert.ok(!/IllINois/.test(out), `and literals inside it stay clean, got: ${out}`);
});
