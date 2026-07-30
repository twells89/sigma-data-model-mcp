// Regression coverage for the Domo Beast Mode defect class (beads jva2/sqp1 + five
// defects found alongside them). Every input here is a real shape from the live
// 48-card Domo corpus, normalised the way convert-beast-modes.rb normalises it
// (backtick identifiers → [brackets]).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { stripOuterParens, lookSqlToSigmaRules, tableauTextConcatToSigma, lookConvertExpression, lookConvertCase, hasResidualCaseKeyword, hasResidualInfixOperator, lookUnknownFunctions } from './formulas.js';

test('stripOuterParens unwraps a whole-expression wrapper, repeatedly (jva2)', () => {
  assert.equal(stripOuterParens('(x)'), 'x');
  assert.equal(stripOuterParens('((x))'), 'x');
  assert.equal(stripOuterParens('  ( x )  '), 'x');
});

test('stripOuterParens leaves non-wrapping parens alone (jva2)', () => {
  assert.equal(stripOuterParens('(a) + (b)'), '(a) + (b)');
  assert.equal(stripOuterParens('(a) AND (b)'), '(a) AND (b)');
  assert.equal(stripOuterParens('Sum(x)'), 'Sum(x)');
  assert.equal(stripOuterParens('(unbalanced'), '(unbalanced');
});

test('stripOuterParens is not fooled by parens inside string literals (jva2)', () => {
  // The ')' here is data, not structure — stripping on a naive depth count corrupts it.
  assert.equal(stripOuterParens("('a)b')"), "'a)b'");
});

test('a paren-wrapped CASE now reaches the CASE rule instead of falling through (jva2)', () => {
  const sql = '(CASE WHEN SUM([Net Revenue]) = 0 THEN 0 ELSE SUM([Gross Profit]) / SUM([Net Revenue]) END )';
  const out = lookSqlToSigmaRules(sql);
  assert.ok(out !== null, 'must match a rule, not return null');
  assert.equal(out, 'If(Sum([Net Revenue]) = 0, 0, Sum([Gross Profit]) / Sum([Net Revenue]))');
});

// Review finding (round 1): a bare apostrophe inside a [bracketed identifier] was
// treated as a string-literal delimiter, putting the scanner in a permanent in-quote
// state that swallowed the real closing ')' — depth never returned to 0, so the
// outer parens were silently left in place. A `[...]` span must be atomic: a quote
// character inside brackets is part of the identifier, not a literal delimiter.
test('stripOuterParens is not fooled by an apostrophe inside a [bracketed identifier] (jva2 review)', () => {
  assert.equal(
    stripOuterParens("(CASE WHEN [Manager's Approval] = 1 THEN 1 ELSE 0 END)"),
    "CASE WHEN [Manager's Approval] = 1 THEN 1 ELSE 0 END",
  );
});

test('lookSqlToSigmaRules reaches the CASE rule when a bracketed identifier contains an apostrophe (jva2 review)', () => {
  const sql = "(CASE WHEN [Manager's Approval] = 1 THEN 1 ELSE 0 END)";
  const out = lookSqlToSigmaRules(sql);
  assert.ok(out !== null, 'must match a rule, not return null');
  assert.equal(out, "If([Manager's Approval] = 1, 1, 0)");
});

test('tableauTextConcatToSigma resolves a paren-wrapped bracket ref with an apostrophe via isTextRef (jva2 review)', () => {
  // Mirrors the [CW_COUNTRY] control case that already works — the only difference
  // is the apostrophe inside the identifier, which must not defeat the paren-unwrap.
  const isTextRef = (name: string) => name === "Manager's Approval";
  const out = tableauTextConcatToSigma("([Manager's Approval]) + [OtherNum]", isTextRef);
  assert.equal(out, "([Manager's Approval]) & [OtherNum]");
});

test('ALL-CAPS text inside a string literal is NOT rewritten as a column ref (A3)', () => {
  // Before the fix this produced [State] = '[Ak]' — silent data corruption.
  assert.equal(lookConvertExpression("[State] = 'AK'"), '[State] = "AK"');
});

test('string literals are emitted double-quoted, Sigma style (A6)', () => {
  assert.equal(lookConvertExpression("'West'"), '"West"');
  // an embedded double quote must be escaped, not emitted raw
  assert.equal(lookConvertExpression(`'say "hi"'`), '"say \\"hi\\""');
  // SQL's doubled-single-quote escape unescapes to one apostrophe
  assert.equal(lookConvertExpression("'it''s'"), '"it\'s"');
});

test('a CASE over string literals converts without corrupting them (A3+A6)', () => {
  const sql = "(CASE WHEN [Billing State] = 'AK' THEN 'West' ELSE 'Other' END)";
  assert.equal(
    lookSqlToSigmaRules(sql),
    'If([Billing State] = "AK", "West", "Other")'
  );
});

// Review finding (A3/A6 masking): an apostrophe inside a [bracketed identifier]
// (e.g. [Manager's Approval]) is part of the identifier, not a string-literal
// delimiter — the same hazard Task 1's review caught in stripOuterParens. A
// naive _maskLiterals regex run over the whole string treats that apostrophe as
// an opening quote and swallows everything up to the NEXT real quote, which
// corrupts BOTH the identifier and the literal that followed it: before the
// fix, `[Manager's Approval] = 'AK'` masked/unmasked into
// `[Manager"s Approval] = "[Ak]'` instead of leaving the identifier alone and
// converting the literal to Sigma double-quoted form.
test("lookConvertExpression does not mis-mask a literal when an apostrophe sits inside a [bracketed identifier] (A3+A6 review)", () => {
  assert.equal(
    lookConvertExpression("[Manager's Approval] = 'AK'"),
    '[Manager\'s Approval] = "AK"'
  );
});

// Round 1 finding: an unterminated '[' (no matching ']' anywhere in the rest of
// the string) was treated as opening one giant atomic bracket span running to
// end-of-string. Every literal after that point was therefore never masked and
// got bracket-corrupted by passes 1-3 when it hit an ALL-CAPS token inside it —
// the exact defect class this task exists to eliminate, reintroduced by the
// bracket-awareness itself. A '[' with no matching ']' must degrade to an
// ordinary character (never swallow the remainder of the string), same as the
// brief's original plain-regex behaviour for this input.
test('an unterminated [ does not swallow the rest of the string, reintroducing A3 corruption (round 1 review)', () => {
  // reviewer's exact reproduction input
  assert.equal(lookConvertExpression("[Foo = 'AK'"), '[Foo = "AK"');
});

test('a trailing unterminated [ with no literal after it is left alone (round 1 review)', () => {
  assert.equal(
    lookConvertExpression("[Region] = 'West' AND [Foo"),
    '[Region] = "West" AND [Foo'
  );
});

test('an unterminated [ followed by two literals still masks both (round 1 review)', () => {
  assert.equal(lookConvertExpression("[Foo = 'A' OR 'B'"), '[Foo = "A" OR "B"');
});

test('SQL keywords before a paren stay infix, not function calls (A4)', () => {
  // Before: ([A] > 1) And([B] < 2) — and And()/Or() as CALLS silently null rows in Sigma.
  assert.equal(lookConvertExpression('(A > 1) AND (B < 2)'), '([A] > 1) AND ([B] < 2)');
  assert.equal(lookConvertExpression('(A > 1) OR (B < 2)'), '([A] > 1) OR ([B] < 2)');
  assert.equal(lookConvertExpression('NOT (A > 1)'), 'NOT ([A] > 1)');
});

test('zero-arg function maps do not double their parens (A5)', () => {
  assert.equal(lookConvertExpression('CURRENT_DATE()'), 'Today()');   // was Today()()
  assert.equal(lookConvertExpression('GETDATE()'), 'Now()');          // was Now()()
});

// Attention item 1 (task-3 review): DISTINCT is in the keyword list, so it must
// never be treated as a callable when it happens to sit directly before a paren
// (the classic `SELECT DISTINCT(col)` style). This must NOT interfere with
// Task 4's separate `COUNT(DISTINCT x)` handling: in that shape DISTINCT is
// followed by a space then the argument, never directly by '(', so pass 1 (the
// name-before-paren regex) never matches DISTINCT there regardless of whether
// it's in the keyword list.
test('DISTINCT directly before a paren is left as literal text, not mapped to a bogus Distinct() call (A4 keyword list)', () => {
  assert.equal(lookConvertExpression('DISTINCT(ORDER_ID)'), 'DISTINCT([Order Id])');
});

// Attention item 2 (task-3 review), REVISED after round-1 review finding 1: a
// test asserting `ORDER_ID IN (1, 2, 3)` -> `In([Order Id], 1, 2, 3)` was a
// tautology — verified (by re-running against pre-round-1 commit 2c94be4) that
// it produces the IDENTICAL output with or without the IN-keyword-exclusion fix,
// because whenever pass 2's `EXPR IN (a,b,c)` regex actually fires, it REPLACES
// the whole matched span (LHS + IN + list) outright, discarding whatever pass 1
// already did to "IN"'s casing/spacing. So the normal-usage case can never prove
// this exclusion does anything; it was passing on both sides.
//
// The exclusion is only externally observable when pass 2's regex does NOT fire
// — i.e. there is no left-hand-side token for it to capture, e.g. a bare
// `IN (1, 2, 3)` with nothing before it. Verified by executing pre-round-1 commit
// 2c94be4 directly:
//   pre-fix (2c94be4):  lookConvertExpression('IN (1, 2, 3)') -> 'In(1, 2, 3)'
//     (pass 1's fallback default-capitalizes "IN" to "In" AND consumes the
//      trailing whitespace as part of its match, collapsing "In (" to "In(" —
//      a fabricated, argument-short call that looks deceptively like valid
//      Sigma output for a keyword pass 2 never touched.)
//   post-fix (this branch onward): lookConvertExpression('IN (1, 2, 3)') -> 'IN (1, 2, 3)'
//     (keyword branch returns the match — name + its original trailing
//      whitespace — unchanged, so nothing invents a call out of it.)
// This is the actual, provable guarantee the IN keyword exclusion gives: pass 1
// will not itself fabricate an `In(...)`-shaped call out of a bare IN that pass 2
// doesn't have an operand for.
test('a bare IN with no left-hand side is left as literal keyword text, not fabricated into a bogus In() call (A4 keyword list, round-1 review finding 1)', () => {
  assert.equal(lookConvertExpression('IN (1, 2, 3)'), 'IN (1, 2, 3)');
});

// Round-1 review finding 2: pass 3 (bare ALL_CAPS identifier bracketing) had its
// OWN separate inline keyword list that had already drifted from pass 1's —
// missing AS, ON, BY, DISTINCT. `A AS B` bracketed AS into a bogus `[As]` column;
// `GROUP_COL BY OTHER` did the same to BY. Confirmed red at pre-round-2 HEAD
// (commit 4b4b5a0, the round-1 fix):
//   lookConvertExpression('A AS B')             -> '[A] [As] [B]'
//   lookConvertExpression('GROUP_COL BY OTHER') -> '[Group Col] [By] [Other]'
// Fixed by unifying pass 1 and pass 3 onto the single shared _SQL_KEYWORD_RE
// constant, so the two lists can no longer independently drift.
test('pass 3 uses the same keyword list as pass 1 — AS/ON/BY/DISTINCT stay literal, not bracketed into bogus columns (round-1 review finding 2)', () => {
  assert.equal(lookConvertExpression('A AS B'), '[A] AS [B]');
  assert.equal(lookConvertExpression('GROUP_COL BY OTHER'), '[Group Col] BY [Other]');
  assert.equal(lookConvertExpression('A ON B'), '[A] ON [B]');
  assert.equal(lookConvertExpression('DISTINCT X'), 'DISTINCT [X]');
});

// Round-1 review finding 3: OVER, GROUP (as in WITHIN GROUP (...)), and EXISTS
// are the same "keyword before a paren looks like a call" defect class as A4,
// and are reachable in production — dbt/snowflake/lookml window-function column
// definitions flow through this converter, and `detectUnsupportedSigmaFunction`
// does not gate on OVER either. Confirmed red at pre-round-2 HEAD (4b4b5a0):
//   lookConvertExpression('SUM(X) OVER (Y)')                  -> 'Sum([X]) Over([Y])'
//   lookConvertExpression('EXISTS (Y)')                        -> 'Exists([Y])'
//   lookConvertExpression('LISTAGG(X) WITHIN GROUP (Y)')       -> 'Listagg([X]) [Within] Group([Y])'
// (WITHIN itself is a separate, pre-existing, out-of-scope gap — not in
// _SQL_KEYWORD_RE, so it still gets bracketed as a bare identifier; only the
// OVER/GROUP/EXISTS callable-mapping defect is in scope for this fix.)
test('OVER, GROUP, and EXISTS before a paren stay infix, not fabricated into bogus calls (round-1 review finding 3)', () => {
  assert.equal(lookConvertExpression('SUM(X) OVER (Y)'), 'Sum([X]) OVER ([Y])');
  assert.equal(lookConvertExpression('EXISTS (Y)'), 'EXISTS ([Y])');
  assert.equal(lookConvertExpression('LISTAGG(X) WITHIN GROUP (Y)'), 'Listagg([X]) [Within] GROUP ([Y])');
});

test('COUNT(DISTINCT x) becomes CountDistinct, bare and bracketed (sqp1)', () => {
  // Before: Count([Distinct] [Order Id]) — DISTINCT bracketed as if it were a column.
  assert.equal(lookConvertExpression('COUNT(DISTINCT ORDER_ID)'), 'CountDistinct([Order Id])');
  assert.equal(lookConvertExpression('COUNT(DISTINCT [Id])'), 'CountDistinct([Id])');
});

// _maskCountDistinct's inner scanner tracked '/" quote state but not [...]
// spans (unlike _maskLiterals and stripOuterParens, both fixed for this same
// defect class earlier on this branch). An apostrophe inside a bracketed
// identifier — the branch's canonical realistic case, [Manager's Approval] —
// was mistaken for a quote delimiter, trapping the scanner in-quote so depth
// never returns to 0 and the whole call falls through unconverted (final
// review, Critical finding).
test("COUNT(DISTINCT [Bracketed Id With An Apostrophe]) does not trap the scanner in-quote (final review)", () => {
  assert.equal(
    lookConvertExpression("COUNT(DISTINCT [Manager's Approval])"),
    "CountDistinct([Manager's Approval])"
  );
});

test('COUNT(DISTINCT ...) with a nested CASE argument converts recursively (sqp1)', () => {
  const sql = 'COUNT(DISTINCT (CASE WHEN [Age] <= 30 THEN [Id] END))';
  assert.equal(lookConvertExpression(sql), 'CountDistinct(If([Age] <= 30, [Id], null))');
});

test('a whole Domo ratio Beast Mode over COUNT(DISTINCT) converts end to end (jva2+sqp1)', () => {
  const sql = '(CASE WHEN (COUNT(DISTINCT [Id]) = 0) THEN 0 ELSE (SUM([Retweet Count]) / COUNT(DISTINCT [Id])) END )';
  assert.equal(
    lookSqlToSigmaRules(sql),
    'If(CountDistinct([Id]) = 0, 0, Sum([Retweet Count]) / CountDistinct([Id]))'
  );
});

// The brief's Step 1 also specified a fourth test here —
// `lookConvertExpression('COUNT(ORDER_ID)') === 'Count([Order Id])'` (plain
// COUNT must not over-reach). Verified it is a TAUTOLOGY as a proof the FIX
// works: identical on both sides of this task (pre-fix HEAD 0c6e8e3 already
// produces 'Count([Order Id])' because _maskCountDistinct's regex requires the
// literal keyword DISTINCT, so a plain COUNT never engages the new masking
// path at all). It is NOT dropped, though — round-2 review (rightly)
// distinguished "tautology pretending to validate the fix" from "pin against
// regression of an adjacent path": if _maskCountDistinct's regex ever became
// over-eager and started engaging on a plain COUNT(x), NOTHING would catch it
// without this assertion existing somewhere. Kept as an explicit
// non-regression guard, not claimed as red-pre-fix evidence for this task —
// see the multi-arg/mixed-unbalanced tests below for the actual red→green
// proof of the "don't over-reach" behavior this task added.
test('plain COUNT(x) is unaffected by the DISTINCT masking — non-regression pin for the adjacent path, expected to pass on both sides of this fix (sqp1)', () => {
  assert.equal(lookConvertExpression('COUNT(ORDER_ID)'), 'Count([Order Id])');
});

// Attention item 2 (ordering): the count-distinct mask MUST run before the
// literal mask, or a string literal inside a COUNT(DISTINCT ...) argument gets
// captured already-raw into `args[]` and is masked/unmasked correctly by the
// nested recursive call. Uses [State] rather than the brief's own [S] example —
// [S] trips an unrelated, pre-existing pass-3 defect (a single ALL-CAPS-letter
// bracketed ref like [S] gets double-bracketed to [[S]] because pass 3's bare
// bare-identifier regex does not skip content already inside [...]; confirmed
// this reproduces identically with NO COUNT(DISTINCT) involved at all —
// `lookConvertExpression("[S] = 'AK'")` → `'[[S]] = "AK"'` on pre-fix HEAD
// 0c6e8e3 too. Out of scope for this task (a pass-3 bug, not a COUNT(DISTINCT)
// bug) — flagged in the task-4 report, not fixed here. [State] (multi-letter,
// mixed-case display form) sidesteps that unrelated defect and isolates the
// ordering behavior this test exists to prove.
// Verified red at pre-fix HEAD 0c6e8e3:
//   lookConvertExpression("COUNT(DISTINCT (CASE WHEN [State] = 'AK' THEN [Id] END))")
//     -> 'Count(DISTINCT (CASE WHEN [State] = "AK" THEN [Id] END))'
test('a string literal inside a COUNT(DISTINCT ...) argument survives correctly — mask ordering (sqp1 attention item 2)', () => {
  const sql = "COUNT(DISTINCT (CASE WHEN [State] = 'AK' THEN [Id] END))";
  assert.equal(lookConvertExpression(sql), 'CountDistinct(If([State] = "AK", [Id], null))');
});

// Attention item 3 (must not over-reach / must not produce garbage): the
// balanced-scan mask does not special-case a multi-argument COUNT(DISTINCT a, b)
// — the argument scan simply captures everything up to the matching ')',
// commas included, and hands the whole "a, b" span to the SAME recursive
// converter. That happens to produce a syntactically well-formed
// CountDistinct([A], [B]) rather than corrupting the input — a deliberate
// choice not to special-case multi-arg (see task-4 report for the semantic
// caveat: SQL's multi-column COUNT(DISTINCT a,b) counts distinct (a,b) PAIRS,
// which is not the same operation as Sigma's CountDistinct given multiple
// arguments — this has NOT been verified against a live Sigma formula
// evaluation). This test pins the current, non-corrupting behavior.
// Verified red at pre-fix HEAD 0c6e8e3:
//   lookConvertExpression('COUNT(DISTINCT ORDER_ID, CUSTOMER_ID)')
//     -> 'Count(DISTINCT [Order Id], [Customer Id])'
test('COUNT(DISTINCT a, b) multi-arg converts without corrupting the input (sqp1 attention item 3)', () => {
  assert.equal(
    lookConvertExpression('COUNT(DISTINCT ORDER_ID, CUSTOMER_ID)'),
    'CountDistinct([Order Id], [Customer Id])'
  );
});

// Attention item 4 (unbalanced input must not corrupt or hang): a malformed,
// unterminated COUNT(DISTINCT ... with no closing ')' is detected by the
// depth-tracking scan (depth never returns to 0) and the balanced-scan mask
// bails out, leaving that entire call untouched rather than guessing at where
// it ends. A solo `COUNT(DISTINCT [Id]` (nothing before it) is therefore a
// TAUTOLOGY — identical pre/post-fix, because bailing on the very first match
// leaves the whole string unmasked, so passes 1-3 do exactly what they always
// did to it. This test instead pairs one VALID, balanced COUNT(DISTINCT ...)
// with a second, unterminated one later in the same expression — proving (a)
// the first one still converts correctly, (b) the scanner does not hang or
// throw on the malformed second one, and (c) it leaves the malformed tail as
// literal text rather than corrupting it or swallowing the valid part before it.
// Verified red at pre-fix HEAD 0c6e8e3:
//   lookConvertExpression('COUNT(DISTINCT ORDER_ID) + COUNT(DISTINCT CUSTOMER_ID')
//     -> 'Count(DISTINCT [Order Id]) + Count(DISTINCT [Customer Id]'
test('a valid COUNT(DISTINCT ...) followed by an unterminated one converts the first and leaves the second untouched (sqp1 attention item 4)', () => {
  assert.equal(
    lookConvertExpression('COUNT(DISTINCT ORDER_ID) + COUNT(DISTINCT CUSTOMER_ID'),
    'CountDistinct([Order Id]) + Count(DISTINCT [Customer Id]'
  );
});

// Attention item 1 (recursion terminates): a COUNT(DISTINCT ...) nested inside
// a CASE that is itself nested inside another COUNT(DISTINCT ...)'s argument —
// two levels of the _unmaskCountDistinct -> lookSqlToSigmaRules/lookConvertCase
// -> lookConvertExpression -> _maskCountDistinct recursion. Each recursive call
// runs on a strictly shorter substring (the captured argument always excludes
// at minimum the "COUNT(DISTINCT " prefix and the closing ")" of its own call),
// so the recursion is well-founded on string length and must terminate; this
// runs in ~1ms with no stack overflow, confirming it in practice as well as in
// principle (see task-4 report for the full termination argument).
// Verified red at pre-fix HEAD 0c6e8e3:
//   lookConvertExpression('COUNT(DISTINCT (CASE WHEN [Age] <= 30 THEN (COUNT(DISTINCT [Id])) ELSE 0 END))')
//     -> 'Count(DISTINCT (CASE WHEN [Age] <= 30 THEN (Count(DISTINCT [Id])) ELSE 0 END))'
test('a COUNT(DISTINCT ...) nested two levels deep inside another one converts recursively without hanging (sqp1 attention item 1)', () => {
  const sql = 'COUNT(DISTINCT (CASE WHEN [Age] <= 30 THEN (COUNT(DISTINCT [Id])) ELSE 0 END))';
  assert.equal(lookConvertExpression(sql), 'CountDistinct(If([Age] <= 30, CountDistinct([Id]), 0))');
});

// ── Task 4b: nested CASE must not be shredded by the WHEN split ─────────────
// lookConvertCase used to split the CASE body on every bare `\bWHEN\b`, blind to
// nesting. A nested CASE (e.g. inside a COUNT(...) argument) has its own WHEN/
// THEN/END, and the naive split cut straight across them, straddling structural
// boundaries. This bug was pre-existing but only became reachable once Task 1
// made outer-paren-wrapped CASE reach the CASE rule at all — see task-4b brief.

// The real corpus example (bm-corpus.json item 11, live 74-formula Domo run).
// Verified red at pre-fix HEAD 57bdd4e (direct probe against src/formulas.ts —
// this exact string was not yet in the suite):
//   lookSqlToSigmaRules(sql) ->
//     'If((DateDiff(Today(),[created_on]) - 1) <= 30, [id] END )) = 0) THEN 0, ' +
//     'If(([status] = "Closed") AND ((DateDiff(Today(),[created_on]) - 1) <= 30), ' +
//     '[id] END )) / Count((CASE, If((DateDiff(Today(),[created_on]) - 1) <= 30, ' +
//     '[id] END ))), Count((CASE  WHEN (([status] = "Closed") AND ' +
//     '((DateDiff(Today(),[created_on]) - 1) <= 30)) THEN [id] END )) / ' +
//     'Count((CASE  WHEN ((DateDiff(Today(),[created_on]) - 1) <= 30) THEN [id] END )))))'
// paren delta -6 (matches the brief's measured -6 exactly); contains the
// literal shredded substring "END )".
test('the real corpus example (bm-corpus item 11) is never shredded — converts cleanly or returns null, never with unbalanced parens (task-4b)', () => {
  const sql = "(CASE  WHEN (COUNT((CASE  WHEN ((DATEDIFF(current_date(),[created_on]) - 1) <= 30) THEN [id] END )) = 0) THEN 0 ELSE (COUNT((CASE  WHEN (([status] = 'Closed') AND ((DATEDIFF(current_date(),[created_on]) - 1) <= 30)) THEN [id] END )) / COUNT((CASE  WHEN ((DATEDIFF(current_date(),[created_on]) - 1) <= 30) THEN [id] END ))) END )";
  const out = lookSqlToSigmaRules(sql);
  // A regression back to `null` would previously slip through green here,
  // since every check below was wrapped in `if (out !== null)` — this corpus
  // formula is fully parseable and must convert, not merely "convert or
  // null" (round-1 review, bundled minor). It DOES convert cleanly (verified
  // below), so this is a hard, unconditional assertion, not a loosened one.
  assert.notEqual(out, null, 'this formula is parseable and must not regress to null');
  assert.ok(!out!.includes('END )'), `must not contain shredded "END )": ${out}`);
  // no bare CASE-structure keyword may survive outside a Sigma string literal
  assert.ok(!/\b(CASE|WHEN|THEN)\b/i.test(out!.replace(/"(?:[^"\\]|\\.)*"/g, '')), `must not leave bare CASE-structure keywords in the output: ${out}`);
  let paren = 0, bracket = 0;
  for (const c of out!) {
    if (c === '(') paren++; else if (c === ')') paren--;
    else if (c === '[') bracket++; else if (c === ']') bracket--;
  }
  assert.equal(paren, 0, `paren delta must be 0, got ${paren}: ${out}`);
  assert.equal(bracket, 0, `bracket delta must be 0, got ${bracket}: ${out}`);
});

// A nested CASE inside an aggregate argument (COUNT((CASE ... END)) = 0), the
// exact shape that shreds under the naive split — converted correctly end to
// end, requirement 2's "recurse rather than split".
// Verified red at pre-fix HEAD 57bdd4e:
//   lookSqlToSigmaRules(sql) -> 'If([Age] <= 30, [Id] END)) = 0) THEN 0, 1)'
//   (paren delta -3; the nested WHEN/THEN/END is cut straight across)
test('a nested CASE inside an aggregate argument converts correctly end to end (task-4b)', () => {
  const sql = '(CASE WHEN (COUNT((CASE WHEN ([Age] <= 30) THEN [Id] END)) = 0) THEN 0 ELSE 1 END)';
  assert.equal(
    lookSqlToSigmaRules(sql),
    'If(Count((If([Age] <= 30, [Id], null))) = 0, 0, 1)'
  );
});

// A nested CASE in the ELSE branch — requirement 2 again, this time with no
// enclosing parens around the inner CASE at all (so only the CASE-nesting-depth
// tracking, not paren depth, keeps the inner WHEN/THEN/ELSE from being mistaken
// for the outer CASE's own structure).
// Verified red at pre-fix HEAD 57bdd4e:
//   lookSqlToSigmaRules(sql) ->
//     'If([FieldA] = 1, [FieldB], If([FieldC] = 2, [FieldD], ' +
//     'CASE WHEN [FieldC] = 2 THEN [FieldD] ELSE [FieldE] END))'
//   (parens happen to balance here, but the inner ELSE's raw, untranslated CASE
//   text is duplicated into the outer If()'s final branch — silently wrong,
//   not merely unbalanced.)
test('a nested CASE in the ELSE branch converts correctly, not duplicated as raw CASE text (task-4b)', () => {
  const sql = "(CASE WHEN [FieldA] = 1 THEN [FieldB] ELSE (CASE WHEN [FieldC] = 2 THEN [FieldD] ELSE [FieldE] END) END)";
  assert.equal(
    lookSqlToSigmaRules(sql),
    'If([FieldA] = 1, [FieldB], If([FieldC] = 2, [FieldD], [FieldE]))'
  );
});

// Malformed: a WHEN with no THEN before the next WHEN. The naive split
// silently DROPPED the first (malformed) branch instead of failing — a wrong
// answer that looks plausible, worse than an honest null.
// Verified red at pre-fix HEAD 57bdd4e:
//   lookSqlToSigmaRules('CASE WHEN [X] = 1 WHEN [Y] = 2 THEN 1 END')
//     -> 'If([[Y]] = 2, 1, null)'
//   (the malformed "WHEN [X] = 1" branch — no THEN before the next WHEN — is
//   silently discarded rather than failing the whole parse)
test('a WHEN with no THEN returns null instead of silently dropping the branch (task-4b)', () => {
  assert.equal(lookConvertCase('CASE WHEN [X] = 1 WHEN [Y] = 2 THEN 1 END'), null);
  assert.equal(lookSqlToSigmaRules('CASE WHEN [X] = 1 WHEN [Y] = 2 THEN 1 END'), null);
});

// Malformed: CASE with a THEN but no closing END at all. Baseline fabricated a
// plausible-looking (WRONG) result instead of failing.
// Verified red at pre-fix HEAD 57bdd4e:
//   lookSqlToSigmaRules('CASE WHEN [X] = 1 THEN [Y]') -> 'If([[X]] = 1, [[Y]], null)'
test('a CASE with a THEN but no END returns null instead of fabricating a result (task-4b)', () => {
  assert.equal(lookConvertCase('CASE WHEN [X] = 1 THEN [Y]'), null);
  assert.equal(lookSqlToSigmaRules('CASE WHEN [X] = 1 THEN [Y]'), null);
});

// Malformed: the brief's own literal example — `CASE WHEN x THEN` with no
// closing END, and not even a value for the one THEN. Baseline ALREADY
// returns null here (the naive split's regex requires a THEN-value it doesn't
// find), so this is a NON-REGRESSION PIN, not red/green evidence: it exists to
// guarantee the rewrite doesn't throw or hang on this input, and stays null.
test('a malformed CASE with no THEN-value and no END returns null, does not throw, does not hang (task-4b, non-regression pin)', () => {
  assert.equal(lookConvertCase('CASE WHEN [X] = 1 THEN'), null);
  assert.equal(lookSqlToSigmaRules('CASE WHEN [X] = 1 THEN'), null);
});

// ── task-4b round-1 review findings ─────────────────────────────────────────

// FINDING 1 (Important, real regression): convertLeaf never rejected an empty
// cond/val chunk — `WHEN THEN`, `THEN ELSE`, `ELSE END` with nothing between
// them, or a value that strips down to nothing (`()`) — so `_isBalanced` (which
// only counts parens/brackets) happily passed a hole spliced straight into the
// output. This is precisely the shredded-but-plausible failure mode task-4b
// exists to prevent, and a real regression: at 57bdd4e this specific input
// returned null (honest); at ccbafe6 it returned a balanced, wrong string.
// Verified red at ccbafe6:
//   lookConvertCase('CASE WHEN THEN 1 ELSE 2 END') -> 'If(, 1, 2)'
test('an empty WHEN-condition returns null instead of splicing a hole into the output (task-4b round-1 finding 1)', () => {
  assert.equal(lookConvertCase('CASE WHEN THEN 1 ELSE 2 END'), null);
});

// Three siblings in the same class. NOT regressions — 57bdd4e already produced
// different (and equally wrong) garbage for each of these — but they share the
// exact defect this finding's fix closes, so they're pinned here too.
// Verified red at ccbafe6:
//   lookConvertCase("CASE WHEN [A]=1 THEN ELSE 2 END") -> 'If([[A]]=1, , 2)'
//   lookConvertCase("CASE WHEN [A]=1 THEN 1 ELSE END") -> 'If([[A]]=1, 1, )'
//   lookConvertCase("CASE WHEN () THEN 1 ELSE 2 END")  -> 'If(, 1, 2)'
// (57bdd4e produced 'If([[A]]=1, ELSE 2, 2)', 'If([[A]]=1, 1 ELSE, null)', and
// 'If(, 1, 2)' respectively for these three — garbage on both sides, just
// different garbage, confirming these are not new regressions.)
test('an empty THEN-value, empty ELSE-value, and a paren-only cond that strips to nothing all return null (task-4b round-1 finding 1)', () => {
  assert.equal(lookConvertCase('CASE WHEN [A]=1 THEN ELSE 2 END'), null);
  assert.equal(lookConvertCase('CASE WHEN [A]=1 THEN 1 ELSE END'), null);
  assert.equal(lookConvertCase('CASE WHEN () THEN 1 ELSE 2 END'), null);
});

// FINDING 2 (Important): _unmaskCountDistinct's `lookSqlToSigmaRules(raw) ??
// lookConvertExpression(raw)` fallback — the very recursion pattern task-4b's
// own nested-CASE handling cited as precedent — did not check whether the
// fallback left raw CASE/WHEN/THEN/END text behind. Task-4b routes MORE CASE
// shapes to null (e.g. the "simple CASE" form `CASE [Region] WHEN 1 THEN ...`,
// which this parser deliberately does not support), so a COUNT(DISTINCT ...)
// wrapping one of those now got dressed up as a plausible-looking
// `CountDistinct(CASE ...)` — a converted-looking call around raw,
// untranslated SQL.
// Verified red at ccbafe6:
//   lookConvertExpression('COUNT(DISTINCT (CASE [Region] WHEN 1 THEN 2 ELSE 3 END))')
//     -> 'CountDistinct(CASE [Region] WHEN 1 THEN 2 ELSE 3 END)'
test('a CASE argument to COUNT(DISTINCT ...) that fails to parse is left as raw SQL, not dressed up as a converted CountDistinct(...) call (task-4b round-1 finding 2)', () => {
  const out = lookConvertExpression('COUNT(DISTINCT (CASE [Region] WHEN 1 THEN 2 ELSE 3 END))');
  assert.ok(!/CountDistinct\s*\(/.test(out), `must not wrap unparsed CASE text as if converted: ${out}`);
  assert.ok(/\bCASE\b/i.test(out), `raw CASE text should still be visibly present, not silently dropped: ${out}`);
});

// hasResidualCaseKeyword itself: must mask brackets/literals first so a
// legitimately-named [End] column or an 'the end' literal never false-positives
// — the same masking idiom formulaHasUntranslatableFragment already uses for
// the Tableau path. Non-regression pin (this is a brand new helper — nothing
// to be red against).
test('hasResidualCaseKeyword does not false-positive on a legitimate [End] column ref or a literal containing "end" (task-4b round-1 finding 2)', () => {
  assert.equal(hasResidualCaseKeyword('If([End] = 1, 1, 0)'), false);
  assert.equal(hasResidualCaseKeyword('[X] = "the end"'), false);
  assert.equal(hasResidualCaseKeyword('CASE [Region] WHEN 1 THEN 2 ELSE 3 END'), true);
});

// task-4c round-1 review finding 1: brand-new helper (no prior state to be
// red against — same non-regression-pin framing task-4b used for
// hasResidualCaseKeyword itself). Masks brackets/literals first so a
// legitimately-named [Between] column or a literal containing "like" never
// false-positives, mirroring hasResidualCaseKeyword's own masking idiom.
test('hasResidualInfixOperator does not false-positive on a legitimate [Between] column ref or a literal containing "like", and DOES flag a genuine LIKE/BETWEEN (task-4c round-1 finding 1)', () => {
  assert.equal(hasResidualInfixOperator('If([Between] = 1, 1, 0)'), false);
  assert.equal(hasResidualInfixOperator('[X] = "we like this"'), false);
  assert.equal(hasResidualInfixOperator('Lower([Region]) LIKE "usa"'), true);
  assert.equal(hasResidualInfixOperator('[Age] BETWEEN 18 AND 65'), true);
});

// task-4c round-1 review finding 1: the actual bug, reproduced against the
// exact tools.ts convert_sql_to_sigma_formula handler logic (no test file
// imports src/tools.ts directly — confirmed via grep, same gap task-4b's
// report noted; tsc is its only prior gate — so this replicates the handler's
// branch logic exactly, the same approach task-4b used to verify its own
// tools.ts fix).
// Verified red at a0e2ca5 (round-1 fix, before this round's finding-1 fix):
//   fallbackConverted = !hasResidualCaseKeyword(fallback)  // LIKE not checked
//   -> true, for a formula whose only defect is an untranslated LIKE —
//   tools.ts would have reported `converted: true` on output Sigma cannot
//   evaluate as written.
test('the tools.ts fallback-converted logic honestly reports false when only a residual infix operator survives, not just a residual CASE keyword (task-4c round-1 finding 1)', () => {
  const sql = "SUM((CASE WHEN (LOWER([Account.BillingCountry]) LIKE 'united states') THEN 0 WHEN (LOWER([Account.BillingCountry]) LIKE 'usa') THEN 0 WHEN (LOWER([Account.BillingCountry]) LIKE 'us') THEN 0 ELSE 1 END ))";
  const ruled = lookSqlToSigmaRules(sql);
  assert.equal(ruled, null, 'must fall through to lookConvertExpression — this pins WHY tools.ts needs the fallback check at all');
  const fallback = lookConvertExpression(sql);
  // Round-1-only check (a0e2ca5): silences the finding — reproduces the RED
  // behavior directly rather than merely asserting the fixed one.
  assert.equal(!hasResidualCaseKeyword(fallback), true, 'the CASE itself DOES fully convert — hasResidualCaseKeyword alone is silenced');
  // Fixed logic (this round): both checks together correctly report false.
  const fallbackConverted = !hasResidualCaseKeyword(fallback) && !hasResidualInfixOperator(fallback);
  assert.equal(fallbackConverted, false, 'a residual LIKE must still be reported as not-fully-converted');
});

// BUNDLED MINOR: the `_isBalanced` backstop (requirement 4) is load-bearing
// but was untested on its own — every other test that reaches null does so via
// an earlier structural check. `[Revenue (USD]` is a well-formed bracket span
// (atomic, per _scanCase/_maskLiterals), so the CASE structure parses cleanly;
// the stray `(` is DATA inside the identifier, but `_isBalanced` counts every
// paren in the final string globally (it does not skip bracket interiors), so
// this is the one input where every structural check passes and only the
// balance backstop saves it. Pinning so it cannot silently regress.
test('_isBalanced backstop alone catches a bracket ref containing a literal unmatched paren (task-4b round-1 bundled minor)', () => {
  assert.equal(lookConvertCase('CASE WHEN [Revenue (USD] > 1 THEN 1 ELSE 2 END'), null);
});

// ── Task 4c: convert CASE spans EMBEDDED in a larger expression ────────────
// lookSqlToSigmaRules anchors its CASE pattern at start-of-string
// (`/^CASE\b/i`). A formula whose OUTER construct is arithmetic or an
// aggregate — with the CASE *inside* it — never matches that pattern, falls
// through to lookConvertExpression, which (pre-task-4c) had no CASE
// awareness at all: raw CASE/WHEN/THEN/END text survived embedded in
// otherwise-converted output. Measured at 16/74 (22%) of the live corpus —
// see task-4c-brief.md. lookConvertExpression now runs task-4b's
// `_convertNestedCases`/`_scanCase` machinery itself, in a 'leave-raw' mode:
// a span that parses converts to `If(...)` and the rest of the (already
// mechanically-converted) expression proceeds through passes 1-3 normally; a
// span that does NOT parse is left exactly as found, and
// `hasResidualCaseKeyword` on the final output is the honest signal.
//
// All five inputs below verified RED at 1a47959 (direct probe against the
// unmodified src/formulas.ts, via a one-off tsx script — this exact test
// block did not exist yet):
//   lookConvertExpression('SUM((CASE WHEN [Age] = 1 THEN 1 ELSE 0 END))')
//     -> 'Sum((CASE WHEN [Age] = 1 THEN 1 ELSE 0 END))'
//   lookConvertExpression('100 * (CASE WHEN [Age] = 1 THEN 1 ELSE 0 END)')
//     -> '100 * (CASE WHEN [Age] = 1 THEN 1 ELSE 0 END)'
//   lookConvertExpression('(CASE WHEN [Age] = 1 THEN 1 ELSE 0 END) / COUNT([Name])')
//     -> '(CASE WHEN [Age] = 1 THEN 1 ELSE 0 END) / Count([Name])'
//   lookConvertExpression('(CASE WHEN [Age] = 1 THEN 1 ELSE 0 END) + (CASE [Region] WHEN 1 THEN 2 ELSE 3 END)')
//     -> '(CASE WHEN [Age] = 1 THEN 1 ELSE 0 END) + (CASE [Region] WHEN 1 THEN 2 ELSE 3 END)'
//   lookConvertExpression("100 * (CASE WHEN (COUNT(DISTINCT [Id]) = 0) THEN 'a' ELSE 'b' END)")
//     -> '100 * (CASE WHEN (CountDistinct([Id]) = 0) THEN "a" ELSE "b" END)'
// (all five: hasResidualCaseKeyword(out) === true at 1a47959)

test('an aggregate wrapping a CASE converts the embedded span (task-4c)', () => {
  const out = lookConvertExpression('SUM((CASE WHEN [Age] = 1 THEN 1 ELSE 0 END))');
  assert.equal(out, 'Sum((If([Age] = 1, 1, 0)))');
  assert.equal(hasResidualCaseKeyword(out), false);
});

test('arithmetic wrapping a CASE on the LEFT of an operator converts the embedded span (task-4c)', () => {
  const out = lookConvertExpression('100 * (CASE WHEN [Age] = 1 THEN 1 ELSE 0 END)');
  assert.equal(out, '100 * (If([Age] = 1, 1, 0))');
  assert.equal(hasResidualCaseKeyword(out), false);
});

test('arithmetic wrapping a CASE on the RIGHT of an operator converts the embedded span (task-4c)', () => {
  const out = lookConvertExpression('(CASE WHEN [Age] = 1 THEN 1 ELSE 0 END) / COUNT([Name])');
  assert.equal(out, '(If([Age] = 1, 1, 0)) / Count([Name])');
  assert.equal(hasResidualCaseKeyword(out), false);
});

// Requirement 3 ("fail honestly, span by span"), made discriminating: TWO
// embedded CASE spans in one expression, one well-formed and one the
// unsupported "simple CASE" form (`CASE expr WHEN val THEN ...` —
// lookConvertCase deliberately rejects this shape, task-4b). A test with only
// the unparseable span would be a tautology (baseline ALSO leaves an
// unparseable span untouched, since baseline does nothing to ANY CASE) — this
// version proves the discriminating behavior requirement 3 actually asks for:
// the GOOD span converts, the BAD span stays raw, in the SAME output.
// Verified red at 1a47959 (baseline converts NEITHER span):
//   lookConvertExpression('(CASE WHEN [Age] = 1 THEN 1 ELSE 0 END) + (CASE [Region] WHEN 1 THEN 2 ELSE 3 END)')
//     -> '(CASE WHEN [Age] = 1 THEN 1 ELSE 0 END) + (CASE [Region] WHEN 1 THEN 2 ELSE 3 END)'
test('a CASE span that cannot parse is left raw while a sibling CASE in the same expression still converts (task-4c requirement 3)', () => {
  const sql = '(CASE WHEN [Age] = 1 THEN 1 ELSE 0 END) + (CASE [Region] WHEN 1 THEN 2 ELSE 3 END)';
  const out = lookConvertExpression(sql);
  assert.equal(out, '(If([Age] = 1, 1, 0)) + (CASE [Region] WHEN 1 THEN 2 ELSE 3 END)');
  assert.ok(hasResidualCaseKeyword(out), 'the unparseable sibling span must still be visible to hasResidualCaseKeyword');
});

// Attention item 1 (ordering / mutual-recursion proof): a CASE span containing
// BOTH a string literal AND a COUNT(DISTINCT ...) call, embedded under
// arithmetic so it must go through lookConvertExpression's NEW seam (not
// lookSqlToSigmaRules' anchored pattern 4). This is the combination that
// exposed a real bug while implementing this task: lookConvertExpression
// masks COUNT(DISTINCT ...) BEFORE the embedded-CASE scan runs (per the
// brief's specified ordering), so the extracted CASE span can carry an
// OUTER-scope CD sentinel (STX/ETX) embedded inside it. The first
// implementation restored only the literal mask before handing the span to
// lookConvertCase (mirroring task-4b's existing _restoreRawLiterals use) and
// left the CD sentinel in place — which leaked into a freshly-scoped
// recursive lookConvertExpression call whose OWN _unmaskCountDistinct indexed
// into its OWN (unrelated, empty) args array with the outer call's index,
// throwing `Cannot read properties of undefined (reading 'trim')` in
// stripOuterParens. Caught by hand-tracing this exact input before it ever
// reached a committed test (see task-4c-report.md) — fixed by adding
// `_restoreRawCountDistinct`, which restores a CD sentinel to genuine raw
// `COUNT(DISTINCT <arg>)` SQL text (mirroring `_restoreRawLiterals`) before
// the span is handed to lookConvertCase, so the inner recursive call
// re-discovers it as ordinary SQL and masks/unmasks it entirely within its
// own call frame. This test pins the fix and is the requirement-1 "verify
// with a CASE span that contains BOTH a string literal AND a COUNT(DISTINCT
// ...)" evidence.
test('a CASE span containing both a string literal and COUNT(DISTINCT ...) converts without cross-scope sentinel collision (task-4c attention item 1)', () => {
  const out = lookConvertExpression("100 * (CASE WHEN (COUNT(DISTINCT [Id]) = 0) THEN 'a' ELSE 'b' END)");
  assert.equal(out, '100 * (If(CountDistinct([Id]) = 0, "a", "b"))');
  assert.equal(hasResidualCaseKeyword(out), false);
});

// The real two-level corpus example quoted in task-4c-brief.md (bm-corpus.json
// item 3 — normalizeDomo's backtick->bracket transform applied, matching how
// the corpus test itself feeds formulas through). A CASE (dividing the whole
// expression) whose ELSE branch itself aggregates a SECOND, nested CASE via
// SUM(...) — the exact "aggregate wrapping a CASE, nested inside an outer
// CASE that itself never reaches lookSqlToSigmaRules's anchor" shape the brief
// opens with. Verified red at 1a47959:
//   lookSqlToSigmaRules(n) -> null (starts with "(", not "CASE" or a bare
//     arithmetic identifier — pattern 4 and pattern 5 both miss it, exactly
//     as task-4c-brief.md describes)
//   lookConvertExpression(n) ->
//     '((CASE  WHEN (Count([Name]) = 0) THEN 0 ELSE Sum((CASE  WHEN ' +
//     '([IsClosed] = "true") THEN 1 ELSE 0 END )) END ) / Count([Name]))'
//   (raw CASE/WHEN/THEN/END survives twice; function names title-cased around it)
test('the real two-level corpus example (bm-corpus item 3, task-4c-brief.md) converts end to end, pinned to its exact expected output (task-4c)', () => {
  const n = "((CASE  WHEN (COUNT([Name]) = 0) THEN 0 ELSE SUM((CASE  WHEN ([IsClosed] = 'true') THEN 1 ELSE 0 END )) END ) / COUNT([Name]))";
  assert.equal(lookSqlToSigmaRules(n), null, 'must still fall through to lookConvertExpression — this pins WHY the fix belongs there');
  const out = lookConvertExpression(n);
  assert.equal(
    out,
    '((If(Count([Name]) = 0, 0, Sum((If([IsClosed] = "true", 1, 0) ))) ) / Count([Name]))'
  );
  assert.equal(hasResidualCaseKeyword(out), false);
});

// Requirement 2 (recursion must terminate): 300 levels of arithmetic each
// wrapping a CASE around the previous level (`100 * (CASE WHEN (<inner>) = 1
// THEN 1 ELSE 2 END)`, 300 deep). Every level requires ONE hop of the mutual
// recursion lookConvertExpression -> _convertNestedCases -> lookConvertCase
// -> convertLeaf -> lookConvertExpression on the (strictly shorter) inner
// condition. If the termination argument in task-4c-report.md were wrong —
// e.g. if a span's `rawSpan` were not actually shorter than its container, or
// if the two functions' recursive calls formed a cycle rather than a
// well-founded descent — this either hangs or throws
// "Maximum call stack size exceeded" (confirmed experimentally: an otherwise
// -identical 2000-level version DOES throw that exact error against Node's
// default stack, well beyond any real Domo Beast Mode's nesting depth — the
// live corpus never exceeds 2-3 levels). 300 levels completes in well under a
// second, is fully converted (no residual CASE keyword), and is
// paren/bracket-balanced.
test('300 levels of CASE-inside-arithmetic-inside-CASE terminates without stack exhaustion (task-4c requirement 2)', () => {
  const DEPTH = 300;
  let expr = '[Amount] = 1';
  for (let i = 0; i < DEPTH; i++) {
    expr = `100 * (CASE WHEN (${expr}) THEN 1 ELSE 2 END)`;
  }
  const start = Date.now();
  const out = lookConvertExpression(expr);
  assert.ok(Date.now() - start < 5000, 'must not hang');
  assert.ok(out.startsWith('100 * (If(100 * (If('), `unexpected shape at depth ${DEPTH}: ${out.slice(0, 60)}`);
  assert.equal(hasResidualCaseKeyword(out), false, 'every level must have converted, not just the outermost');
  let paren = 0, bracket = 0;
  for (const c of out) {
    if (c === '(') paren++; else if (c === ')') paren--;
    else if (c === '[') bracket++; else if (c === ']') bracket--;
  }
  assert.equal(paren, 0, 'parens must balance at full depth');
  assert.equal(bracket, 0, 'brackets must balance at full depth');
});

// ── task-5: A7 — warn instead of inventing a Sigma function name ────────────
// The step-1 fallback in lookConvertExpression title-cases any unrecognised name
// (`fn.charAt(0).toUpperCase() + fn.slice(1).toLowerCase()`), so `AddDate(` silently
// becomes `Adddate(` — a function Sigma does not have. lookUnknownFunctions reports
// exactly the names that fallback would touch, without changing what gets emitted
// (converter-silent-fallback.test.ts / lanq.1/.3 defect class).

test('unmapped functions are reported, not silently invented (A7)', () => {
  // 'Adddate' is not a Sigma function; emitting it silently ships a broken column.
  assert.deepEqual(lookUnknownFunctions('AddDate(CURRENT_DATE(), -1)'), ['ADDDATE']);
});

test('mapped functions and keywords are not reported as unknown (A7)', () => {
  assert.deepEqual(lookUnknownFunctions('SUM([x]) / COUNT([y])'), []);
  assert.deepEqual(lookUnknownFunctions('CURRENT_DATE()'), []);
  assert.deepEqual(lookUnknownFunctions('(A > 1) AND (B < 2)'), []);
  assert.deepEqual(lookUnknownFunctions('COUNT(DISTINCT [Id])'), []);
});

// The allowlist itself is the real risk here (task brief): a name wrongly present
// silently suppresses a warning for genuinely broken output — the exact AddDate
// defect, just for a different function. DATEPART/DATETRUNC (bare, no underscore)
// are a real example: Sigma's actual functions are `DatePart`/`DateTrunc` (a SECOND
// embedded capital), which the naive title-case fallback cannot reproduce — it can
// only ever produce a single leading capital (`Datepart`/`Datetrunc`). Confirmed via
// grep across src/*.ts (DateTrunc/DatePart appear dozens of times, always with the
// second capital — resources.ts's own formula-syntax reference documents
// `DateTrunc("month", [Date])`). These two names are therefore deliberately EXCLUDED
// from _SIGMA_PASSTHROUGH so a bare (underscore-less) use still warns.
test('bare DATEPART/DATETRUNC (no underscore, not in LOOK_FUNC_MAP) are reported as unknown — naive title-case cannot produce the real DatePart/DateTrunc spelling (A7 allowlist correctness)', () => {
  assert.deepEqual(lookUnknownFunctions('DATEPART(CreatedDate)'), ['DATEPART']);
  assert.deepEqual(lookUnknownFunctions('DATETRUNC(CreatedDate)'), ['DATETRUNC']);
});

test('DATE_TRUNC (with underscore — LOOK_FUNC_MAP maps it to the real DateTrunc) is not reported (A7 allowlist correctness)', () => {
  assert.deepEqual(lookUnknownFunctions('DATE_TRUNC(CreatedDate)'), []);
});

// Passthrough names whose naive title-case DOES happen to equal Sigma's real
// (single-leading-capital) spelling — must not false-positive, or the warning
// becomes noise that gets ignored (task brief: "false positives = failed
// implementation").
test('passthrough names whose title-case matches Sigma exactly (Number/Date/Text/If/Median) do not false-positive (A7 allowlist correctness)', () => {
  assert.deepEqual(lookUnknownFunctions('NUMBER([Amount])'), []);
  assert.deepEqual(lookUnknownFunctions('DATE([Ts])'), []);
  assert.deepEqual(lookUnknownFunctions('TEXT([Year])'), []);
  assert.deepEqual(lookUnknownFunctions('IF([X] > 1, 1, 0)'), []);
  assert.deepEqual(lookUnknownFunctions('MEDIAN([Amount])'), []);
});

// Real corpus example (74-formula live Domo bm-corpus.json — the only distinct
// warned name across the whole corpus is ADDDATE, found in 6 formulas): a
// "days ago" bucketing CASE using AddDate(Current_Date(), -1). Confirms the
// warning fires on real production input, not just a synthetic example.
test('a real corpus formula using AddDate(Current_Date(), -1) is flagged (A7 corpus pin)', () => {
  const sql = "DateDiff(AddDate(Current_Date(),-1),[Date])";
  assert.deepEqual(lookUnknownFunctions(sql), ['ADDDATE']);
});

// ── task-5 round-1 review finding 1: hand-written allowlist false-positived on
// real Sigma functions (NOW/TODAY/SWITCH/POWER/trig/RANK/LAG/LEAD) that simply
// didn't appear in the 74-formula corpus. The allowlist is now DERIVED from
// LOOK_FUNC_MAP's and TABLEAU_FUNC_MAP's own VALUES (both proven-real Sigma
// function name strings) via one explicit predicate — does _naiveTitleCase of
// the bare name reproduce Sigma's real spelling exactly? — plus a tiny
// supplemental list (COUNT/RANK/LAG/LEAD) for names neither map emits, run
// through that SAME predicate rather than hand-trusted.

test('real Sigma functions not in the 74-formula corpus (NOW/TODAY/SWITCH/POWER/RANK/LAG/LEAD/a trig fn) do not false-positive (A7 round-1 finding 1)', () => {
  assert.deepEqual(lookUnknownFunctions('NOW()'), []);
  assert.deepEqual(lookUnknownFunctions('TODAY()'), []);
  assert.deepEqual(lookUnknownFunctions('SWITCH([X], 1, "a", 2, "b")'), []);
  assert.deepEqual(lookUnknownFunctions('POWER([X], 2)'), []);
  assert.deepEqual(lookUnknownFunctions('SIN([X])'), []);
  assert.deepEqual(lookUnknownFunctions('RANK([X])'), []);
  assert.deepEqual(lookUnknownFunctions('LAG([X], 1)'), []);
  assert.deepEqual(lookUnknownFunctions('LEAD([X], 1)'), []);
});

// The predicate must still catch genuine multi-word mismatches — not just the
// two DATEPART/DATETRUNC cases found in round 1, but ANY name where Sigma's real
// spelling carries a second embedded capital the naive fallback cannot reproduce.
// STARTSWITH/ENDSWITH/MAKEDATE/REGEXP_EXTRACT/STDEV/PERCENTILE/SPLIT are real
// Sigma-adjacent names (StartsWith/EndsWith/MakeDate/RegexpExtract/StdDev/
// PercentileCont/SplitPart) that the OLD hand-written list also left un-allowlisted
// (so this is not a behavior change for them) — pinned here as evidence the
// derivation catches this whole class uniformly, not just the two cases found by
// hand.
test('the title-case predicate still warns for every multi-word-mismatch case, not just DATEPART/DATETRUNC/ADDDATE (A7 round-1 finding 1)', () => {
  assert.deepEqual(lookUnknownFunctions('DATEPART(CreatedDate)'), ['DATEPART']);
  assert.deepEqual(lookUnknownFunctions('DATETRUNC(CreatedDate)'), ['DATETRUNC']);
  assert.deepEqual(lookUnknownFunctions('AddDate(CURRENT_DATE(), -1)'), ['ADDDATE']);
  assert.deepEqual(lookUnknownFunctions('STARTSWITH([X], "a")'), ['STARTSWITH']);
  assert.deepEqual(lookUnknownFunctions('MAKEDATE(2024, 1)'), ['MAKEDATE']);
  assert.deepEqual(lookUnknownFunctions('STDEV([X])'), ['STDEV']);
});

// ── Task 6: corpus regression — lock the Track-A gain in ────────────────────
// 74 distinct Beast Modes from a live 48-card Domo page (anonymised — see
// src/sql.beastmode.corpus.json). Before Track A, 0 of these reached a rule at
// all. This is a MEASUREMENT, not a tautology dressed up as one: it is written
// to pass at HEAD-of-this-branch by construction (that's the point — it pins
// the number Track A actually achieved), but it goes RED against pre-Track-A
// commit 0be8116 (see task-6-report.md for the pasted failure) and RED again
// the moment a future refactor re-breaks the paren gate, the And()/Or() call
// form, the Today()() double-paren, COUNT(DISTINCT) leaking [Distinct] as a
// column, or a residual raw CASE/WHEN/THEN/END keyword surviving into the
// output. The >= 37 floor may only rise as the converter improves further;
// the five defect-class assertions below are exact zeros and must never move.
//
// SCOPE LIMIT (round-1 review finding 1) — read before trusting a green run:
// every check in `live Domo Beast Mode corpus: rules are reached and output
// is not corrupt` is STRUCTURAL — it screens for specific bad string
// patterns (a leaked keyword, an unbalanced paren, a doubled call-paren) and
// a rule-reached floor. None of it is semantic: a change that inverts an
// `If(cond, then, else)` branch order, swaps an operand, or maps a column to
// the wrong name produces output that is still perfectly balanced, has no
// `[Distinct]`, no `And(`/`Or(`/`When(`, no `)()`, and no residual CASE
// keyword — and this test will report `ok` regardless. Semantic correctness
// is covered separately, by the golden-value tests immediately below (a
// handful of exact-string pins over real corpus entries) and by every
// task-1–5 unit test earlier in this file. Treat this corpus test as a
// tripwire for "did the converter stop reaching / stop cleaning up after
// itself," not as proof any individual formula converts to the right value.
//
// Replicates BOTH pre-steps of convert-beast-modes.rb's normalize_bm function
// (plugins/domo-to-sigma/skills/domo-to-sigma/scripts/convert-beast-modes.rb —
// cited by FUNCTION NAME, not a line range: task-4c round-1 review caught a
// stale line-number citation here (this file is a shared, synced artifact
// across many worktrees, and line numbers drift as it's edited elsewhere —
// exactly the kind of citation that silently goes wrong and is cheap to avoid):
// 1. MySQL backtick identifiers → Sigma [bracket] form.
// 2. WEEKDAY(...) → DAYOFWEEK(...) (Beast Mode does this itself; Domo's own
//    script replicates it for parity, so this fixture must too — see the
//    round-1 finding-3 measurement in task-6-report.md for what this reveals).
const normalizeDomo = (s: string) => {
  let out = s.replace(/`([^`]+)`/g, (_m, c) => `[${c}]`).trim();
  out = out.replace(/\bWEEKDAY\s*\(/gi, 'DAYOFWEEK(');
  return out;
};

test('live Domo Beast Mode corpus: rules are reached and output is not corrupt', () => {
  const corpus: string[] = JSON.parse(
    readFileSync(new URL('./sql.beastmode.corpus.json', import.meta.url), 'utf8')
  );
  assert.equal(corpus.length, 74, 'corpus size is pinned — update deliberately');

  let matched = 0;
  const distinctLeak: string[] = [];
  const callForm: string[] = [];
  const doubleParen: string[] = [];
  const unbalanced: string[] = [];
  const residualCase: string[] = [];
  const residualInfixIndices: number[] = [];

  corpus.forEach((sql, idx) => {
    const n = normalizeDomo(sql);
    const ruled = lookSqlToSigmaRules(n);
    const out = ruled ?? lookConvertExpression(n);
    if (ruled !== null) matched++;
    if (/\[Distinct\]/.test(out)) distinctLeak.push(out);
    if (/\b(?:And|Or|When)\s*\(/.test(out)) callForm.push(out);
    if (/\)\s*\(\)/.test(out)) doubleParen.push(out);
    if ((out.match(/\(/g) || []).length !== (out.match(/\)/g) || []).length) unbalanced.push(out);
    if (hasResidualCaseKeyword(out)) residualCase.push(out);
    if (hasResidualInfixOperator(out)) residualInfixIndices.push(idx);
  });

  // Baseline before Track A was 0. Every paren-wrapped CASE (37) plus the other
  // rule-matching shapes must now be reached.
  assert.ok(matched >= 37, `only ${matched}/74 matched a rule (baseline 0, expected >= 37)`);
  assert.deepEqual(distinctLeak, [], 'no formula may leak [Distinct] as a column (sqp1)');
  assert.deepEqual(callForm, [], 'And()/Or()/When() call form silently nulls rows (A4)');
  assert.deepEqual(doubleParen, [], 'no Today()() style doubled parens (A5)');
  assert.deepEqual(unbalanced, [], 'no unbalanced parentheses');
  // NOTE (round-1 review finding 2): this assertion is added exactly as
  // specified and is NOT expected to be empty right now — see
  // task-6-report.md "Round 1 fix" for the honest, measured result and why
  // it is not being silently narrowed to make this pass.
  assert.deepEqual(residualCase, [], 'no residual raw CASE/WHEN/THEN/END keyword may survive in the output');

  // Task-4c round-1 review finding 1: converting an embedded CASE span can
  // silence residualCase above while a DIFFERENT untranslated SQL construct —
  // an infix LIKE or BETWEEN, neither of which any function in this file
  // translates — still sits inside the newly-produced condition. corpus[63]
  // (`LOWER(...) LIKE 'usa'`, nested inside a CASE that itself only reaches
  // lookConvertExpression via task-4c's new embedded-CASE seam) is the
  // measured, genuinely-unavoidable example: Sigma has no infix LIKE operator
  // at all, and mapping it to Contains/RegexpMatch would silently change
  // semantics (wildcards, anchoring, case-sensitivity all differ) rather than
  // just translate syntax — that is separate work, explicitly out of scope
  // here, not something to paper over with an approximate translation.
  //
  // This assertion is pinned to the EXACT known, hand-verified set — not
  // merely "must be empty" (an assert.deepEqual(residualInfixIndices, [])
  // here would be knowingly, permanently red the same way Task 6's own
  // residualCase assertion was left red pending this task, which would
  // reopen the 26-known-failures full-suite gate rather than close it) and
  // not merely "must be non-empty" either. It goes red — loudly, on purpose —
  // in BOTH directions: if corpus[63]'s LIKE unexpectedly stops leaking (a
  // behavior change worth investigating, not silently accepting), and if any
  // OTHER formula starts leaking an infix operator it didn't before (a
  // genuine new regression). See task-4c-report.md round-2 for the full
  // count and reasoning.
  assert.deepEqual(
    residualInfixIndices,
    [63],
    `residual infix operator (LIKE/BETWEEN) set must be EXACTLY the known, justified corpus[63] LIKE case, got indices: ${JSON.stringify(residualInfixIndices)}`
  );
});

// ── Task 6 round-1 review finding 1: golden-value spot checks ───────────────
// The structural screen above cannot see a semantically-wrong-but-clean
// conversion (proven by the reviewer: flipping If(cond, then, else) branch
// order leaves every check above green). These three pins are a small,
// deliberately-not-exhaustive semantic tripwire over real corpus entries,
// spanning the shapes the brief calls out: a simple (non-nested) CASE ratio,
// a CASE nested inside a COUNT(...) argument that is itself inside an outer
// CASE, and a COUNT(DISTINCT) ratio. Indices are corpus[]'s own (0-based),
// so a future corpus edit that reorders/removes these entries fails loudly
// here rather than silently losing semantic coverage.
//
// Task-4c round-1 review finding 4: corpus[26]/[13]/[11] above are all
// byte-identical before and after task-4c (diffed directly — task-4c's fix
// only changes output for 16 of the 74 corpus formulas, and none of these
// three are among them). That means NONE of the golden pins above would have
// caught a semantic inversion in the specific code path task-4c added —
// only the 16 newly-converted formulas exercise it. The three pins below
// extend coverage into that path: corpus[3] is the exact two-level example
// task-4c-brief.md opens with (a CASE dividing the whole expression, whose
// ELSE branch aggregates a second, nested CASE), corpus[61] is a CASE with a
// compound AND condition using date functions, and corpus[63] is the
// LIKE-carrying formula from finding 1 above — pinned here specifically to
// show the CASE structure itself converts correctly (nested If/If, correct
// branch order, correct operands) even though the LIKE inside it is
// deliberately left untranslated.
test('live Domo Beast Mode corpus: golden-value spot checks over representative shapes (round-1 finding 1)', () => {
  const corpus: string[] = JSON.parse(
    readFileSync(new URL('./sql.beastmode.corpus.json', import.meta.url), 'utf8')
  );
  const convert = (idx: number) => {
    const n = normalizeDomo(corpus[idx]);
    return lookSqlToSigmaRules(n) ?? lookConvertExpression(n);
  };

  // corpus[26]: simple, single-level CASE ratio — "(CASE WHEN SUM(x)=0 THEN 0 ELSE SUM(y)/SUM(x) END)"
  assert.equal(
    convert(26),
    'If(Sum([NumberSent]) = 0, 0, Sum([NumberDelivered]) / Sum([NumberSent]))'
  );

  // corpus[13]: COUNT(DISTINCT ...) ratio — "(CASE WHEN COUNT(DISTINCT x)=0 THEN 0 ELSE SUM(y)/COUNT(DISTINCT x) END)"
  assert.equal(
    convert(13),
    'If(CountDistinct([Id]) = 0, 0, Sum([Reaction Count]) / CountDistinct([Id]))'
  );

  // corpus[11]: a CASE nested inside two COUNT(...) arguments, each of which is
  // itself a condition/branch of the outer CASE (the task-4b "corpus item 11"
  // shape, pinned here to an exact string rather than only structural checks).
  assert.equal(
    convert(11),
    'If(Count((If((DateDiff(Today(),[created_on]) - 1) <= 30, [id], null) )) = 0, 0, ' +
    'Count((If(([status] = "Closed") AND ((DateDiff(Today(),[created_on]) - 1) <= 30), [id], null) )) / ' +
    'Count((If((DateDiff(Today(),[created_on]) - 1) <= 30, [id], null) )))'
  );

  // corpus[3] (task-4c-brief.md's own headline example): a CASE dividing the
  // whole expression — `(CASE ... END) / COUNT(Name)` — whose ELSE branch
  // aggregates a SECOND, nested CASE via SUM(...). Never matches
  // lookSqlToSigmaRules (confirmed elsewhere in this file, task-4c's own
  // unit test) so this exercises the embedded-CASE seam end to end.
  assert.equal(
    convert(3),
    '((If(Count([Name]) = 0, 0, Sum((If([IsClosed] = "true", 1, 0) ))) ) / Count([Name]))'
  );

  // corpus[61]: SUM(...) aggregate wrapping a CASE whose single condition is a
  // compound AND of two date-function equalities (current year/quarter) —
  // `SUM((CASE WHEN ((YEAR(x)=YEAR(CURRENT_DATE())) AND (QUARTER(x)=QUARTER(CURRENT_DATE()))) THEN Amount ELSE 0 END))`.
  assert.equal(
    convert(61),
    'Sum((If((Year([CloseDate])=Year(Today())) AND (Quarter([CloseDate])=Quarter(Today())), [Amount], 0) ))'
  );

  // corpus[63]: the finding-1 LIKE example. Pinned here to show the CASE
  // structure itself (three WHEN branches plus ELSE, correctly nested into
  // If/If/If) converts correctly even though the LIKE operator inside each
  // condition is deliberately left untranslated (Sigma has no equivalent) —
  // a semantic inversion in the newly-converted branch order/operands would
  // fail this pin even though hasResidualInfixOperator still (correctly)
  // flags the formula above.
  assert.equal(
    convert(63),
    'Sum((If(Lower([Account.BillingCountry]) LIKE "united states", 0, If(Lower([Account.BillingCountry]) LIKE "usa", 0, If(Lower([Account.BillingCountry]) LIKE "us", 0, 1))) ))'
  );
});
