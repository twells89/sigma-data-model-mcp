// Regression coverage for the Domo Beast Mode defect class (beads jva2/sqp1 + five
// defects found alongside them). Every input here is a real shape from the live
// 48-card Domo corpus, normalised the way convert-beast-modes.rb normalises it
// (backtick identifiers → [brackets]).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { stripOuterParens, lookSqlToSigmaRules, tableauTextConcatToSigma, lookConvertExpression, lookConvertCase } from './formulas.js';

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
  assert.notEqual(out, undefined);
  if (out !== null) {
    assert.ok(!out.includes('END )'), `must not contain shredded "END )": ${out}`);
    // no bare CASE-structure keyword may survive outside a Sigma string literal
    assert.ok(!/\b(CASE|WHEN|THEN)\b/i.test(out.replace(/"(?:[^"\\]|\\.)*"/g, '')), `must not leave bare CASE-structure keywords in the output: ${out}`);
    let paren = 0, bracket = 0;
    for (const c of out) {
      if (c === '(') paren++; else if (c === ')') paren--;
      else if (c === '[') bracket++; else if (c === ']') bracket--;
    }
    assert.equal(paren, 0, `paren delta must be 0, got ${paren}: ${out}`);
    assert.equal(bracket, 0, `bracket delta must be 0, got ${bracket}: ${out}`);
  }
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
