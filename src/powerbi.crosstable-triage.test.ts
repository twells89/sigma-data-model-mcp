/**
 * Power BI → Sigma: cross-table measure TRIAGE classifier.
 *
 * Every fixture is SYNTHETIC (generic SALES_FACT / AGENT_DIM / REGION_DIM names).
 * The classifier answers, for a measure the converter is about to drop:
 * which "<T> View" could host it, how many join hops away, and whether
 * aggregating across those hops would double-count.
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { enclosingAggregate, enclosingAggregateCall, enclosingAggregateCalls, reachableTables, isNeverHostable, splitTopLevelArgs, aggregateSummand, enumerateAggregateCalls } from './powerbi-crosstable-triage.js';

test('T1a bare reference is not aggregated', () => {
  assert.equal(enclosingAggregate('[AMOUNT]', 'AMOUNT'), null);
});

test('T1b direct aggregate wrapping', () => {
  assert.equal(enclosingAggregate('Sum([AMOUNT])', 'AMOUNT'), 'Sum');
});

test('T1c picks the AGGREGATE, not the nearest function', () => {
  // If() is closer, but AMOUNT is still summed — this is the case that
  // inverts the fan-out verdict if implemented as "nearest function".
  assert.equal(enclosingAggregate('Sum(If([FLAG] = 1, [AMOUNT], 0))', 'AMOUNT'), 'Sum');
});

test('T1d each reference gets its own enclosing aggregate', () => {
  const f = 'Sum([AMOUNT]) - Avg([DISCOUNT])';
  assert.equal(enclosingAggregate(f, 'AMOUNT'), 'Sum');
  assert.equal(enclosingAggregate(f, 'DISCOUNT'), 'Avg');
});

test('T1e a non-aggregate wrapper leaves the ref unaggregated', () => {
  assert.equal(enclosingAggregate('If([STATUS] = "Open", 1, 0)', 'STATUS'), null);
});

test('T1f a ref absent from the formula returns null', () => {
  assert.equal(enclosingAggregate('Sum([AMOUNT])', 'MISSING'), null);
});

test('T1g nested parens between ref and aggregate are skipped correctly', () => {
  assert.equal(enclosingAggregate('Sum(([A] + [B]) * 2)', 'B'), 'Sum');
});

test('T1h enclosingAggregateCall returns the aggregate operand text', () => {
  assert.deepEqual(enclosingAggregateCall('Sum([QTY] * [PRICE])', 'QTY'),
    { name: 'Sum', operand: '[QTY] * [PRICE]' });
  assert.equal(enclosingAggregateCall('[QTY]', 'QTY'), null);
  // the operand is the ENCLOSING aggregate's, not the whole formula
  assert.equal(enclosingAggregateCall('Sum([A]) - Avg([B])', 'B')!.operand, '[B]');
});

test('T1i enclosingAggregateCalls returns one entry per occurrence', () => {
  const calls = enclosingAggregateCalls('Max([X]) + Sum([X])', 'X');
  assert.deepEqual(calls.map((c) => c.name), ['Max', 'Sum']);
  assert.deepEqual(enclosingAggregateCalls('Sum([A])', 'MISSING'), []);
});

test('T1j a real paren INSIDE a string literal does not desync the backward walk', () => {
  // A comparison value "A)B" contains a genuine ')' character. Un-masked, the
  // backward walk misreads it as closing some inner call, inflating depth by
  // one with nothing left between it and Sum's own opening paren to absorb
  // the extra count — so Sum's paren gets treated as already matched and the
  // walk continues straight past it, returning null as if AMOUNT were
  // unaggregated even though it plainly sits inside Sum(...).
  const formula = 'Sum([STATUS] = "A)B" & [AMOUNT])';
  assert.deepEqual(enclosingAggregateCall(formula, 'AMOUNT'),
    { name: 'Sum', operand: '[STATUS] = "A)B" & [AMOUNT]' });
});

test('T1k a "[ref]"-shaped bracket INSIDE a string literal is not a real occurrence', () => {
  // "Weird[AMOUNT]label" merely contains bracket text that looks like a
  // reference to column AMOUNT — it is not one. AMOUNT is not referenced
  // anywhere in this formula at all (only X and the literal 1/0 branches of
  // If are), so it must come back unaggregated, not phantom-enclosed by the
  // real Sum(...) that happens to wrap the string it sits inside.
  const formula = 'Sum(If([X] = "Weird[AMOUNT]label", 1, 0))';
  assert.equal(enclosingAggregateCall(formula, 'AMOUNT'), null);
  assert.deepEqual(enclosingAggregateCalls(formula, 'AMOUNT'), []);
});

// SALES_FACT ──▶ AGENT_DIM ──▶ REGION_DIM      (two hops)
// SALES_FACT ──▶ DATE_DIM                       (one hop)
const RELS = [
  { from: 'SALES_FACT', to: 'AGENT_DIM' },
  { from: 'SALES_FACT', to: 'DATE_DIM' },
  { from: 'AGENT_DIM', to: 'REGION_DIM' },
];

test('T2a origin is reachable at hop 0', () => {
  assert.equal(reachableTables('SALES_FACT', RELS, 1).get('SALES_FACT'), 0);
});

test('T2b depth 1 reaches direct dimensions only', () => {
  const r = reachableTables('SALES_FACT', RELS, 1);
  assert.deepEqual([...r.entries()].sort(), [
    ['AGENT_DIM', 1], ['DATE_DIM', 1], ['SALES_FACT', 0],
  ]);
});

test('T2c depth 2 reaches the snowflaked dimension', () => {
  assert.equal(reachableTables('SALES_FACT', RELS, 2).get('REGION_DIM'), 2);
});

test('T2d traversal is OUTGOING only — a dim does not reach its fact', () => {
  const r = reachableTables('DATE_DIM', RELS, 2);
  assert.deepEqual([...r.keys()], ['DATE_DIM']);
});

test('T2e a relationship cycle terminates and keeps the shortest hop', () => {
  const cyc = [{ from: 'A', to: 'B' }, { from: 'B', to: 'A' }];
  const r = reachableTables('A', cyc, 2);
  assert.equal(r.get('A'), 0);
  assert.equal(r.get('B'), 1);
});

test('T3a SELECTEDVALUE is report-context-dependent', () => {
  assert.equal(isNeverHostable('SELECTEDVALUE(DATE_DIM[YEAR])'), true);
});

test('T3b ISFILTERED is report-context-dependent', () => {
  assert.equal(isNeverHostable('IF(ISFILTERED(AGENT_DIM[NAME]), 1, 0)'), true);
});

test('T3c detection is case-insensitive', () => {
  assert.equal(isNeverHostable('SelectedValue(DATE_DIM[YEAR])'), true);
});

test('T3d a plain cross-table aggregate IS hostable', () => {
  assert.equal(isNeverHostable('SUM(SALES_FACT[AMOUNT])'), false);
});

test('T3e a column merely NAMED like the token does not trip it', () => {
  // must match a CALL, not a substring — no false positive on a column name
  assert.equal(isNeverHostable('SUM(SALES_FACT[SELECTEDVALUE_FLAG])'), false);
});

test('T3f a literal VALUE that merely reads like the token is not a real call', () => {
  // "SELECTEDVALUE(x)" here is a comparison string, not DAX syntax — a raw
  // .test() over rawDax with no string-literal awareness would false-positive
  // this into never-hostable, losing coverage for a measure that never
  // actually reads the report filter context.
  assert.equal(
    isNeverHostable('IF([STATUS] = "SELECTEDVALUE(x)", SUM(SALES_FACT[AMOUNT]), 0)'),
    false,
  );
});

test('T8a splitTopLevelArgs ignores commas nested in parens', () => {
  assert.deepEqual(splitTopLevelArgs('[A], [B]'), ['[A]', ' [B]']);
  assert.deepEqual(splitTopLevelArgs('If([A] = 1, 2, 3), [B]'), ['If([A] = 1, 2, 3)', ' [B]']);
  assert.deepEqual(splitTopLevelArgs('[A]'), ['[A]']);
});

test('T8b aggregateSummand: predicates do not set grain', () => {
  assert.equal(aggregateSummand('Sum', '[A]'), '[A]');
  assert.equal(aggregateSummand('SumIf', '[A], [B] = 1'), '[A]');       // first arg only
  assert.equal(aggregateSummand('Percentile', '[A], 0.9'), '[A]');
  assert.equal(aggregateSummand('CountIf', '[B] = 1'), null);           // implicit row
  assert.equal(aggregateSummand('Count', '[A]'), '[A]');
});

test('T8c enumerateAggregateCalls finds every call with a balanced operand', () => {
  assert.deepEqual(enumerateAggregateCalls('Sum([A]) / Count([B])'),
    [{ name: 'Sum', operand: '[A]' }, { name: 'Count', operand: '[B]' }]);
  assert.deepEqual(enumerateAggregateCalls('Sum(If([A] = 1, [B], 0))'),
    [{ name: 'Sum', operand: 'If([A] = 1, [B], 0)' }]);
  assert.deepEqual(enumerateAggregateCalls('[A] + 1'), []);
  // a non-aggregate function that merely CONTAINS an aggregate name is not a call
  assert.deepEqual(enumerateAggregateCalls('MySum([A])'), []);
});

import { triageCrossTable } from './powerbi-crosstable-triage.js';

const OWNERS = {
  AMOUNT:     ['SALES_FACT'],
  QTY:        ['SALES_FACT'],
  AGENT_KEY:  ['SALES_FACT'],
  AGENT_NAME: ['AGENT_DIM'],
  LIST_PRICE: ['AGENT_DIM'],
  REGION:     ['REGION_DIM'],
};
const base = (over: any) => ({
  metricName: 'M', rawDax: 'SUM(SALES_FACT[AMOUNT])', homeTable: 'AGENT_DIM',
  columnOwners: OWNERS, relationships: RELS, ...over,
});

test('T4a the dominant idiom is SAFE: a dim-homed measure summing a FACT column', () => {
  // SUM(SALES_FACT[AMOUNT]) homed on AGENT_DIM. Hosted on SALES_FACT View the
  // summed column is a BASE column — no duplication — even though home != base.
  const t = triageCrossTable(base({ sigmaFormula: 'Sum([AMOUNT])', refs: ['AMOUNT'] }));
  assert.equal(t.reachability, 'one');
  assert.equal(t.candidates[0].baseTable, 'SALES_FACT');
  assert.equal(t.candidates[0].verdict, 'safe');
  assert.equal(t.candidates[0].maxHop, 0);
});

test('T4b summing a DIM column ALONE is FAN-OUT RISK on the fact View', () => {
  // On SALES_FACT View, LIST_PRICE repeats once per fact row, and nothing else
  // in the Sum's operand is fact-grain — so this is a dim-grain question asked
  // at fact grain. It double-counts.
  const t = triageCrossTable(base({
    sigmaFormula: 'Sum([LIST_PRICE])', refs: ['LIST_PRICE'], homeTable: 'SALES_FACT',
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.verdict, 'fanout-risk');
  assert.deepEqual(c.unsafeRefs, ['LIST_PRICE']);
});

test('T4b2 ...but AGENT_DIM View hosts that same measure safely, and is reported', () => {
  // LIST_PRICE is AGENT_DIM's OWN column (hop 0 there), so summing it on
  // AGENT_DIM View is exactly right. The classifier must find that host rather
  // than reporting the measure unrecoverable just because the fact View fails.
  const t = triageCrossTable(base({
    sigmaFormula: 'Sum([LIST_PRICE])', refs: ['LIST_PRICE'], homeTable: 'SALES_FACT',
  }));
  const safe = t.candidates.find((x) => x.verdict === 'safe')!;
  assert.equal(safe.baseTable, 'AGENT_DIM');
  assert.equal(safe.maxHop, 0);
  assert.equal(t.reachability, 'one');
  assert.equal(t.candidates[0].baseTable, 'AGENT_DIM', 'safe candidates sort first');
});

test('T4c Max across a hop is FAN-OUT RISK — omission, not just duplication', () => {
  // Duplication-idempotence is not enough: a join also OMITS a dimension row
  // matching zero base rows, so Max over a dim column can read too low.
  const t = triageCrossTable(base({
    sigmaFormula: 'Max([LIST_PRICE])', refs: ['LIST_PRICE'], homeTable: 'SALES_FACT',
  }));
  assert.equal(t.candidates.find((x) => x.baseTable === 'SALES_FACT')!.verdict, 'fanout-risk');
});

test('T4d a cross-hop ref anywhere in the Sum operand is FAN-OUT RISK', () => {
  // Conservative: the ref sits in an If()'s predicate arm, not under its own
  // aggregate, but the enclosing Sum's summand is its WHOLE operand — and that
  // operand contains a cross-hop column (AGENT_NAME), so it is not base-grain.
  const t = triageCrossTable(base({
    sigmaFormula: 'Sum(If([AGENT_NAME] = "X", [AMOUNT], 0))',
    refs: ['AGENT_NAME', 'AMOUNT'], homeTable: 'SALES_FACT',
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.verdict, 'fanout-risk');
  assert.equal(c.maxHop, 1);
});

test('T4e a two-hop reference is found at hop 2 and is FAN-OUT RISK', () => {
  const t = triageCrossTable(base({
    sigmaFormula: 'Sum(If([REGION] = "W", [AMOUNT], 0))',
    refs: ['REGION', 'AMOUNT'], homeTable: 'SALES_FACT',
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.maxHop, 2);
  assert.equal(c.verdict, 'fanout-risk');
});

test('T4f depth 1 cannot reach a two-hop reference', () => {
  const t = triageCrossTable(base({
    sigmaFormula: 'Sum(If([REGION] = "W", [AMOUNT], 0))',
    refs: ['REGION', 'AMOUNT'], homeTable: 'SALES_FACT', maxDepth: 1,
  }));
  assert.equal(t.reachability, 'none');
  assert.deepEqual(t.candidates, []);
});

// SALES_FACT ──▶ AGENT_DIM ──▶ REGION_DIM ──▶ COUNTRY_DIM  (three hops)
// A real-world `CALCULATE(SUM(FACT[AMOUNT]), DIM[attr] = value)` shape compiles
// to Sigma `SumIf([AMOUNT], [attr] = value)` — the filtered dimension sits in the
// PREDICATE (SumIf's second argument), which `aggregateSummand` already excludes
// from grain analysis (see T8b/T8d). So a predicate ref that is 3 hops away is a
// pure COVERAGE question, never a grain one: once `maxDepth` is high enough for
// every ref to resolve, the aggregate's own summand (AMOUNT alone, hop 0) is
// exactly as safe as it always was.
const RELS3 = [...RELS, { from: 'REGION_DIM', to: 'COUNTRY_DIM' }];
const OWNERS3 = { ...OWNERS, COUNTRY_NAME: ['COUNTRY_DIM'], COUNTRY_POP: ['COUNTRY_DIM'] };

test('T4p CHANGE 1: default maxDepth is 3 — a 3-hop PREDICATE ref is now covered and the measure is SAFE with no maxDepth override', () => {
  const t = triageCrossTable(base({
    sigmaFormula: 'SumIf([AMOUNT], [COUNTRY_NAME] = "USA")',
    refs: ['AMOUNT', 'COUNTRY_NAME'], homeTable: 'SALES_FACT',
    relationships: RELS3, columnOwners: OWNERS3,
    // maxDepth intentionally omitted — this test is about the DEFAULT.
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT');
  assert.ok(c, `SALES_FACT is a covered candidate (got candidates: ${JSON.stringify(t.candidates)})`);
  assert.equal(c!.verdict, 'safe');
  assert.equal(c!.maxHop, 3, 'coverage reaches the 3-hop predicate ref even though it never enters the grain check');
  assert.equal(t.reachability, 'one');
});

test('T4q CHANGE 1 guard: a genuine 3-hop reference INSIDE the aggregate SUMMAND (not a predicate) still FAN-OUT RISK — depth alone must not manufacture a false safe', () => {
  const t = triageCrossTable(base({
    sigmaFormula: 'Max([COUNTRY_POP])',
    refs: ['COUNTRY_POP'], homeTable: 'SALES_FACT',
    relationships: RELS3, columnOwners: OWNERS3,
    // maxDepth intentionally omitted — this test is about the DEFAULT.
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT');
  assert.ok(c, `SALES_FACT is a covered candidate (got candidates: ${JSON.stringify(t.candidates)})`);
  assert.equal(c!.verdict, 'fanout-risk', 'reaching a column at hop 3 does not make summing it across that join safe');
  assert.deepEqual(c!.unsafeRefs, ['COUNTRY_POP']);
});

// Review round 1, finding 3: nothing exercised the mechanism behind the +5
// `ambiguous` measured on R1-R4 — a measure already `safe` at depth 2 on ONE
// base gaining a SECOND, independently-safe base only at depth 3. AMOUNT is
// multi-owned (a real column on BOTH SALES_FACT and AGENT_DIM, so it sits at
// hop 0 relative to EITHER base); COUNTRY_NAME sits in SumIf's PREDICATE
// argument (excluded from grain, same as T4p) at hop 2 from AGENT_DIM but hop
// 3 from SALES_FACT.
const RELS4 = [...RELS3];
const OWNERS4 = { AMOUNT: ['SALES_FACT', 'AGENT_DIM'], COUNTRY_NAME: ['COUNTRY_DIM'] };

test('T4r CHANGE 1: a measure already SAFE at depth 2 on one base gains a SECOND independently-safe base at depth 3 — correctly AMBIGUOUS, not silently kept single', () => {
  const args = {
    metricName: 'M', rawDax: 'irrelevant, no report-context tokens', homeTable: 'SALES_FACT',
    sigmaFormula: 'SumIf([AMOUNT], [COUNTRY_NAME] = "USA")',
    refs: ['AMOUNT', 'COUNTRY_NAME'],
    columnOwners: OWNERS4, relationships: RELS4,
  };

  // At depth 2 (the OLD default): AGENT_DIM reaches COUNTRY_NAME at hop 2, but
  // SALES_FACT cannot yet (hop 3) — exactly ONE safe host.
  const at2 = triageCrossTable({ ...args, maxDepth: 2 });
  assert.equal(at2.reachability, 'one');
  assert.deepEqual(at2.candidates.filter((c) => c.verdict === 'safe').map((c) => c.baseTable), ['AGENT_DIM']);

  // At depth 3 (the NEW default): SALES_FACT now ALSO reaches COUNTRY_NAME (hop
  // 3) — a pure predicate ref, excluded from grain — and its own summand
  // (AMOUNT, multi-owned, hop 0 either way) is exactly as safe as it always
  // was. TWO independently-safe hosts now exist: this MUST be ambiguous, not a
  // silently-kept single "safe" verdict.
  const at3 = triageCrossTable(args);   // default maxDepth
  assert.equal(at3.reachability, 'many');
  assert.deepEqual(
    at3.candidates.filter((c) => c.verdict === 'safe').map((c) => c.baseTable).sort(),
    ['AGENT_DIM', 'SALES_FACT'],
    'both independently-safe bases are present and named, not one silently dropped',
  );
});

test('T4g a never-hostable measure yields no candidates', () => {
  const t = triageCrossTable(base({
    sigmaFormula: 'Sum([AMOUNT])', refs: ['AMOUNT'],
    rawDax: 'SUM(SALES_FACT[AMOUNT]) * SELECTEDVALUE(DATE_DIM[YEAR])',
  }));
  assert.equal(t.neverHostable, true);
  assert.deepEqual(t.candidates, []);
  assert.equal(t.reachability, 'none');
});

test('T4h an unowned reference makes every candidate fail', () => {
  const t = triageCrossTable(base({
    sigmaFormula: 'Sum([GHOST])', refs: ['GHOST'],
  }));
  assert.equal(t.reachability, 'none');
  assert.deepEqual(t.candidates, []);
});

test('T4i two covering Views are reported as ambiguous, not silently picked', () => {
  // Max of a hop-1 column is no longer safe under the sound rule, so this
  // fixture uses a column both F1 and F2 own at hop 0 — genuinely safe from
  // either base — to keep the ambiguity the test is actually about.
  const rels = [{ from: 'F1', to: 'D' }, { from: 'F2', to: 'D' }];
  const t = triageCrossTable(base({
    sigmaFormula: 'Sum([SHARED])', refs: ['SHARED'], homeTable: 'D',
    columnOwners: { SHARED: ['F1', 'F2'] }, relationships: rels,
  }));
  assert.equal(t.reachability, 'many');
  assert.deepEqual(t.candidates.map((c) => c.baseTable), ['F1', 'F2']);
});

test('T4j a MIXED operand with any cross-hop column is FAN-OUT RISK — conservative', () => {
  // Sum([QTY] * [LIST_PRICE]): the sound rule flags any cross-hop column in the
  // summand, so this mixed row-expression shape is now rejected even though
  // QTY (hop 0) is also present. Measured across R1-R4 this shape has ZERO
  // occurrences in the reference corpus — the conservatism costs nothing.
  const t = triageCrossTable(base({
    sigmaFormula: 'Sum([QTY] * [LIST_PRICE])', refs: ['QTY', 'LIST_PRICE'],
    homeTable: 'SALES_FACT',
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.verdict, 'fanout-risk');
  assert.deepEqual(c.unsafeRefs, ['LIST_PRICE']);
});

test('T4k an operand of ONLY cross-hop columns is FAN-OUT RISK', () => {
  // Neither operand column is fact-grain, so nothing pins the aggregate to the
  // fact's row count — this is the T4j case with its base column removed.
  const t = triageCrossTable(base({
    sigmaFormula: 'Sum([LIST_PRICE] + [AGENT_NAME])', refs: ['LIST_PRICE', 'AGENT_NAME'],
    homeTable: 'SALES_FACT',
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.verdict, 'fanout-risk');
  assert.deepEqual(c.unsafeRefs.sort(), ['AGENT_NAME', 'LIST_PRICE']);
});

test('T4l a multi-owned column: the SHORTEST hop among its owners wins', () => {
  // SHARED_DIM_COL exists on both REGION_DIM (hop 2 from SALES_FACT) and
  // AGENT_DIM (hop 1) — columnOwners is a LIST because the same column name
  // can live on more than one table. maxHop must reflect the nearer owner,
  // not whichever happens to be listed first.
  const t = triageCrossTable(base({
    sigmaFormula: 'Max([SHARED_DIM_COL])', refs: ['SHARED_DIM_COL'], homeTable: 'SALES_FACT',
    columnOwners: { ...OWNERS, SHARED_DIM_COL: ['REGION_DIM', 'AGENT_DIM'] },
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.maxHop, 1);
});

test('T4m a multi-owned operand column does not rescue a genuine cross-hop column', () => {
  // MIXED_OWNER_COL is owned by both AGENT_DIM (hop 1 from SALES_FACT) and
  // SALES_FACT itself (hop 0), so it resolves at hop 0 and is not itself
  // unsafe. But LIST_PRICE (hop 1, AGENT_DIM-only) is still in the same
  // summand — same reasoning as T4j, a cross-hop column anywhere is unsafe
  // regardless of what else shares the operand.
  const t = triageCrossTable(base({
    sigmaFormula: 'Sum([LIST_PRICE] * [MIXED_OWNER_COL])',
    refs: ['LIST_PRICE', 'MIXED_OWNER_COL'], homeTable: 'SALES_FACT',
    columnOwners: { ...OWNERS, MIXED_OWNER_COL: ['AGENT_DIM', 'SALES_FACT'] },
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.verdict, 'fanout-risk');
  assert.deepEqual(c.unsafeRefs, ['LIST_PRICE']);
});

test('T4n a ref appearing MORE THAN ONCE: any unsafe occurrence makes it unsafe', () => {
  // Under the sound rule BOTH Max([LIST_PRICE]) and Sum([LIST_PRICE]) are
  // independently fanout-risk here (LIST_PRICE is hop-1 in each call's summand
  // with nothing to pin it to base grain — see T4c for Max alone). NOTE: because
  // both calls are independently unsafe, this fixture does NOT by itself prove
  // that the guard unions unsafe refs across ALL calls rather than stopping at
  // the first one — code that only inspected the first aggregate call would
  // still pass this exact assertion. T8s is the test that genuinely proves
  // that property (it pairs one SAFE call with one UNSAFE call — Sum([AMOUNT])
  // safe, Max([REGION]) unsafe — and the union still surfaces the unsafe one).
  // This test's own job is narrower: same ref, two DIFFERENT enclosing
  // aggregates, both unsafe, unioned without duplication.
  const t = triageCrossTable(base({
    sigmaFormula: 'Max([LIST_PRICE]) + Sum([LIST_PRICE])', refs: ['LIST_PRICE'],
    homeTable: 'SALES_FACT',
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.verdict, 'fanout-risk');
  assert.deepEqual(c.unsafeRefs, ['LIST_PRICE']);
});

test('T8d SumIf predicate does not set grain — a hop-0 summand is SAFE', () => {
  const t = triageCrossTable(base({
    sigmaFormula: 'SumIf([AMOUNT], [AGENT_NAME] = "X")',
    refs: ['AMOUNT', 'AGENT_NAME'], homeTable: 'SALES_FACT',
  }));
  assert.equal(t.candidates.find((x) => x.baseTable === 'SALES_FACT')!.verdict, 'safe');
});

test('T8e CountIf over a dim predicate is FAN-OUT RISK — it counts base rows', () => {
  // One VIP customer with two sales rows: Power BI 1, the fact View 2.
  const t = triageCrossTable(base({
    sigmaFormula: 'CountIf([AGENT_NAME] = "X")', refs: ['AGENT_NAME'], homeTable: 'AGENT_DIM',
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.verdict, 'fanout-risk');
  assert.deepEqual(c.unsafeRefs, ['CountIf()']);
});

test('T8f CountDistinct of a dim column is FAN-OUT RISK — omission, not duplication', () => {
  // A dim row matching zero base rows vanishes from the View, so the distinct
  // count is too LOW. Duplication-idempotence does not rescue this.
  const t = triageCrossTable(base({
    sigmaFormula: 'CountDistinct([LIST_PRICE])', refs: ['LIST_PRICE'], homeTable: 'SALES_FACT',
  }));
  assert.equal(t.candidates.find((x) => x.baseTable === 'SALES_FACT')!.verdict, 'fanout-risk');
});

test('T8g Count of a base column is SAFE', () => {
  const t = triageCrossTable(base({
    sigmaFormula: 'Count([AMOUNT])', refs: ['AMOUNT'], homeTable: 'AGENT_DIM',
  }));
  assert.equal(t.candidates.find((x) => x.baseTable === 'SALES_FACT')!.verdict, 'safe');
});

test('T8h a string literal that LOOKS LIKE a call is not one — enumerateAggregateCalls', () => {
  // A comparison value reading "Count(5)" must not be read as a real Count call.
  // Naive regex scanning finds "Count(" inside the literal and, worse, closes it
  // on the ")" that is also just literal text — producing a phantom call whose
  // absent bracket refs would otherwise sink the WHOLE measure to fanout-risk.
  const formula = 'If([STATUS] = "Count(5)", Sum([AMOUNT]), 0)';
  assert.deepEqual(enumerateAggregateCalls(formula), [{ name: 'Sum', operand: '[AMOUNT]' }]);

  const t = triageCrossTable(base({
    sigmaFormula: formula, refs: ['STATUS', 'AMOUNT'], homeTable: 'SALES_FACT',
    columnOwners: { ...OWNERS, STATUS: ['SALES_FACT'] },
  }));
  assert.equal(t.candidates.find((x) => x.baseTable === 'SALES_FACT')!.verdict, 'safe');
});

test('T8i Percentile is a real aggregate call end-to-end — AGGREGATES membership and aggregateSummand agree', () => {
  // The predicate-like second slot (REGION, hop 2) must not set grain — only the
  // first argument (AMOUNT, hop 0) is the summand. Exercises the full pipeline,
  // not just the aggregateSummand unit covered by T8b.
  const t = triageCrossTable(base({
    sigmaFormula: 'Percentile([AMOUNT], [REGION])',
    refs: ['AMOUNT', 'REGION'], homeTable: 'SALES_FACT',
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.verdict, 'safe');
});

test('T8j Sum(1) has ZERO bracket refs — distinct code path from CountIf\'s null summand', () => {
  // CountIf's summand is null (aggregateSummand returns null — no summand at
  // all). Sum(1)'s summand is the literal text '1' — a real, non-null summand
  // that simply contains no [ref] brackets. Both end in sRefs = [], but via
  // different branches of `summand === null ? [] : (...)`; this exercises the
  // regex-finds-nothing side specifically.
  const t = triageCrossTable(base({ sigmaFormula: 'Sum(1)', refs: [], homeTable: 'SALES_FACT' }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.verdict, 'fanout-risk');
  assert.deepEqual(c.unsafeRefs, ['Sum()']);
});

test('T8k a string literal containing "[" is not read as a column reference', () => {
  // Sibling of T8h: the summand itself (not just its enclosing formula) can
  // contain a comparison value like "X[9]". Both real refs (QTY, AMOUNT) are
  // hop-0 and safe; a phantom ref '9' extracted from inside the literal would
  // resolve to no owner (hop Infinity), which the cross-hop filter (hopOf >= 1)
  // would wrongly catch, sinking a genuinely safe measure to fanout-risk.
  const t = triageCrossTable(base({
    sigmaFormula: 'Sum(If([QTY] = "X[9]", [AMOUNT], 0))',
    refs: ['QTY', 'AMOUNT'], homeTable: 'SALES_FACT',
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.verdict, 'safe');
  assert.deepEqual(c.unsafeRefs, []);
});

test('T8l splitTopLevelArgs: a literal comma in the SECOND (predicate) argument is harmless', () => {
  // aggregateSummand only ever reads args[0], so a comma inside a value that
  // lives in a LATER argument can't corrupt the summand — the first argument
  // is fully sliced out before the corrupted split point is ever reached.
  assert.equal(aggregateSummand('SumIf', '[AMOUNT], [STATUS] = "A, B"'), '[AMOUNT]');
});

test('T8m splitTopLevelArgs: a literal comma in the FIRST argument used to truncate the summand', () => {
  // Damage case: "A,B" contains a comma directly at the TOP LEVEL of the first
  // argument (not nested inside a further call, where a real paren/bracket
  // would already absorb it), with [AMT] positioned AFTER it in the same
  // argument. An unmasked split reads the embedded comma as the argument
  // separator, truncating args[0] to `[STATUS] = "A` — [AMT] disappears from
  // the grain analysis entirely, silently, with no error.
  const operand = '[STATUS] = "A,B" & [AMT], [Y] = 1';
  assert.deepEqual(splitTopLevelArgs(operand), ['[STATUS] = "A,B" & [AMT]', ' [Y] = 1']);
  assert.equal(aggregateSummand('SumIf', operand), '[STATUS] = "A,B" & [AMT]');
});

test('T8n an UNTERMINATED quote does not swallow the rest of the formula', () => {
  // A quote that never closes must not blank everything after it to
  // end-of-string — that would erase a real, later Sum(...) call entirely.
  // Malformed/truncated input degrades to "scanned as plain text", not to
  // "silently loses whatever came after the dangling quote".
  const formula = 'Sum([AMOUNT]) & "unterminated & Sum([REGION])';
  const calls = enumerateAggregateCalls(formula);
  assert.deepEqual(calls.map((c) => c.name), ['Sum', 'Sum']);
});

test('T8o an unterminated quote is FAIL-CLOSED — never "safe", no grain analysis attempted', () => {
  // CHANGED under the fail-closed rule (was ['REGION'] — see the report for
  // why "grain analysis still finds REGION correctly" turned out not to be a
  // safe claim in general; a differently-shaped unterminated quote could make
  // the SAME formula shape drop Max([REGION]) instead of seeing it, per the
  // adversarial counter-example in T8p). This formula's quote never closes,
  // so triageCrossTable now refuses to run grain analysis on it AT ALL and
  // marks every covered candidate fanout-risk with the malformed marker,
  // regardless of what enumerateAggregateCalls would or wouldn't have found.
  const t = triageCrossTable(base({
    sigmaFormula: 'Sum([AMOUNT]) & "broken & Max([REGION])',
    refs: ['AMOUNT', 'REGION'], homeTable: 'SALES_FACT',
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.verdict, 'fanout-risk');
  assert.deepEqual(c.unsafeRefs, ['malformed-formula']);
});

test('T8p the reviewer\'s counter-example: an unterminated quote whose stray ")" closes Sum() early', () => {
  // This is the input that broke round 4's fix: with the dangling quote left
  // unmasked, enumerateAggregateCalls' depth-walk meets the stray ")" INSIDE
  // the intended (but never-closed) literal first, closes Sum(...) there, and
  // captures operand = '[AMOUNT] & "note' — [REGION] never lands in any
  // summand, and the pre-fail-closed guard reported this "safe". Patching that
  // one shape would only relocate the next one; the fix is to never reach
  // grain analysis on unparseable input at all.
  const t = triageCrossTable(base({
    sigmaFormula: 'Sum([AMOUNT] & "note) & [REGION])',
    refs: ['AMOUNT', 'REGION'], homeTable: 'SALES_FACT',
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.verdict, 'fanout-risk');
  assert.deepEqual(c.unsafeRefs, ['malformed-formula']);
});

test('T8q an unbalanced PAREN with no quotes at all is also fail-closed', () => {
  // A different malformation shape with nothing to do with quotes: a missing
  // closing paren makes enumerateAggregateCalls' forward balance-count never
  // find a close, so the WHOLE Sum([REGION] call — cross-hop ref included —
  // is silently dropped from enumeration, not just truncated. Verified this
  // independently before the fail-closed fix: it reported "safe". The
  // well-formedness gate checks paren balance precisely so this shape doesn't
  // need its own patch at the enumeration site.
  const t = triageCrossTable(base({
    sigmaFormula: 'Max([AMOUNT]) & Sum([REGION]',
    refs: ['AMOUNT', 'REGION'], homeTable: 'SALES_FACT',
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.verdict, 'fanout-risk');
  assert.deepEqual(c.unsafeRefs, ['malformed-formula']);
});

test('T8r an unbalanced BRACKET with no quotes at all is also fail-closed', () => {
  // A third malformation shape: a missing closing bracket makes the [ref]
  // extraction regex fail to match from that point, so REGION silently never
  // appears in sRefs even though the call itself (Sum(...)) is captured fine.
  // Verified independently before the fail-closed fix: it reported "safe"
  // with unsafeRefs: []. The well-formedness gate's bracket-balance check
  // catches this without needing a fix inside the bracket-ref regex itself.
  const t = triageCrossTable(base({
    sigmaFormula: 'Sum([AMOUNT] & [REGION)',
    refs: ['AMOUNT', 'REGION'], homeTable: 'SALES_FACT',
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.verdict, 'fanout-risk');
  assert.deepEqual(c.unsafeRefs, ['malformed-formula']);
});

test('T8s a WELL-FORMED formula is unaffected by the gate — real grain analysis still runs', () => {
  // Control: properly balanced quotes/parens/brackets must take the exact same
  // path as before — a genuinely cross-hop call is still reported by NAME
  // (REGION), never masked behind the malformed marker.
  const t = triageCrossTable(base({
    sigmaFormula: 'Sum([AMOUNT]) & Max([REGION])',
    refs: ['AMOUNT', 'REGION'], homeTable: 'SALES_FACT',
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.verdict, 'fanout-risk');
  assert.deepEqual(c.unsafeRefs, ['REGION']);
});

test('T8t a possessive column display name is ordinary text, not a string delimiter — REGRESSION', () => {
  // Round 4 treated "'" as a second quote character, matching '...' as well as
  // "...". That was wrong: this repo's own converters (src/formulas.ts)
  // REWRITE Tableau's and LookML's single-quoted source literals to
  // double-quotes when emitting a Sigma formula — no converter ever emits a
  // '...' literal in Sigma formula text. Meanwhile possessive display names
  // ("Manager's Approval Amount", "O'Brien") are ordinary warehouse content.
  // A lone apostrophe used to be read as an unterminated quote and condemn the
  // whole, perfectly valid formula to fanout-risk with zero coverage.
  const t = triageCrossTable(base({
    sigmaFormula: "Sum([Manager's Approval Amount])",
    refs: ["Manager's Approval Amount"], homeTable: 'SALES_FACT',
    columnOwners: { ...OWNERS, "Manager's Approval Amount": ['SALES_FACT'] },
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.verdict, 'safe');
  assert.deepEqual(c.unsafeRefs, []);
});

test('T8u CROSS-NESTED parens/brackets are fail-closed, not just independently unbalanced ones', () => {
  // Sum([AMOUNT] & [REGION)] has an EQUAL, independently-balanced count of
  // parens (one pair) and brackets (one pair) — two separate counters accept
  // it. But enumerateAggregateCalls tracks only parens for ITS OWN balance:
  // the ')' closes Sum(...) right after '[REGION', one bracket short, so the
  // captured operand is '[AMOUNT] & [REGION' with no closing ']' anywhere
  // inside it — REGION never reaches sRefs and this used to report "safe".
  // The single-stack check (push on any opener, require the popped opener to
  // match the closer's OWN kind) rejects this the instant the mismatched ')'
  // arrives, because '[' was still on top of the stack, not '('.
  const t = triageCrossTable(base({
    sigmaFormula: 'Sum([AMOUNT] & [REGION)]',
    refs: ['AMOUNT', 'REGION'], homeTable: 'SALES_FACT',
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.verdict, 'fanout-risk');
  assert.deepEqual(c.unsafeRefs, ['malformed-formula']);
});

test('T8v the reviewer\'s literal cross-nesting example is ALSO fail-closed', () => {
  // Note (see the report): this exact formula has an unrelated, independent
  // paren-count imbalance (one '(' but two ')' characters) that the OLD
  // independent-counter check already caught on its own — it does not, by
  // itself, isolate the cross-nesting fix the way T8u does. Included anyway
  // because it was the example given and must still come out fail-closed.
  const t = triageCrossTable(base({
    sigmaFormula: 'Sum([AMOUNT] & [REGION) & X])',
    refs: ['AMOUNT', 'REGION'], homeTable: 'SALES_FACT',
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.verdict, 'fanout-risk');
  assert.deepEqual(c.unsafeRefs, ['malformed-formula']);
});

// ── Sibling-metric refs must not contaminate coverage/grain analysis ───────
// `refs` legitimately contains sibling METRIC names — the call site's drop
// test exempts a ref that matches another metric already on the element, so
// the measure isn't dropped just for referencing one. But `columnOwners` only
// ever holds columns (built from `model.tables[].columns`, never `.measures`),
// so a metric ref resolves to hop Infinity on every candidate — before this
// fix, that sank the WHOLE measure to "no View covers it" even when every
// genuine column reference was fully coverable, silently inflating the
// no-covering-View bucket with measures that have nothing structurally wrong
// with their column references at all.
test('T10a a sibling-metric ref no longer poisons coverage — the column refs decide the verdict', () => {
  const t = triageCrossTable(base({
    sigmaFormula: '[Base Metric] + Sum([AMOUNT])',
    refs: ['Base Metric', 'AMOUNT'], homeTable: 'AGENT_DIM',
    metricRefs: ['Base Metric'],
  }));
  // AMOUNT alone is hop-0 on SALES_FACT and safe — the metric ref must not
  // make this "none"/uncovered the way it did before the fix.
  assert.equal(t.reachability, 'one');
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.verdict, 'safe');
  assert.deepEqual(t.dependsOnMetrics, ['Base Metric']);
});

test('T10b a metric ref INSIDE an aggregate summand is excluded from cross-hop judgment, not falsely declared safe', () => {
  // Sum([Base Metric]) alone has no REAL column left in its summand once the
  // metric ref is filtered out — the same shape as Sum(1) (T8j): nothing pins
  // it to base grain, so it is correctly STILL fanout-risk (the generic
  // 'Sum()' implicit-row-like marker), not falsely safe. What the fix changes
  // is only that the unsafeRef is 'Sum()', not 'Base Metric' — without the
  // fix, 'Base Metric' would resolve to hop Infinity and be reported in
  // unsafeRefs as if it were a genuine (nonexistent) cross-hop COLUMN named
  // "Base Metric", which is a misleading operator message for a metric
  // dependency. The fix must not make an inherently unverifiable aggregate
  // look safe just because the ref inside it happens to be a metric name.
  const t = triageCrossTable(base({
    sigmaFormula: 'Sum([Base Metric]) + Sum([AMOUNT])',
    refs: ['Base Metric', 'AMOUNT'], homeTable: 'AGENT_DIM',
    metricRefs: ['Base Metric'],
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.verdict, 'fanout-risk');
  assert.deepEqual(c.unsafeRefs, ['Sum()']);
});

test('T10c describeTriage notes a metric dependency distinctly, without changing the column verdict', () => {
  const t = triageCrossTable(base({
    sigmaFormula: '[Base Metric] + Sum([AMOUNT])',
    refs: ['Base Metric', 'AMOUNT'], homeTable: 'AGENT_DIM',
    metricRefs: ['Base Metric'],
  }));
  const msg = describeTriage(t);
  assert.match(msg, /fan-out SAFE/, 'the underlying column verdict is unchanged');
  assert.match(msg, /Also depends on metric "Base Metric"/);
  assert.match(msg, /re-homing this measure requires re-homing that one/);
});

test('T10d describeTriage pluralizes when more than one sibling metric is depended on', () => {
  const t = triageCrossTable(base({
    sigmaFormula: '[Base Metric] + [Other Metric] + Sum([AMOUNT])',
    refs: ['Base Metric', 'Other Metric', 'AMOUNT'], homeTable: 'AGENT_DIM',
    metricRefs: ['Base Metric', 'Other Metric'],
  }));
  const msg = describeTriage(t);
  assert.match(msg, /Also depends on metrics "Base Metric", "Other Metric"/);
  assert.match(msg, /re-homing this measure requires re-homing those too/);
});

test('T10e with NO metricRefs passed, a metric-shaped ref is (correctly, conservatively) still just uncovered', () => {
  // Regression guard for the OLD behaviour on callers that don't pass
  // metricRefs at all (the parameter is optional) — unchanged from before this
  // fix, and proves the fix is additive, not a silent behaviour change for
  // callers that never had this problem.
  const t = triageCrossTable(base({
    sigmaFormula: '[Untracked Metric] + Sum([AMOUNT])',
    refs: ['Untracked Metric', 'AMOUNT'], homeTable: 'AGENT_DIM',
  }));
  assert.equal(t.reachability, 'none');
  assert.deepEqual(t.candidates, []);
  assert.deepEqual(t.dependsOnMetrics, []);
});

import { convertPowerBIToSigma } from './powerbi.js';
import { describeTriage } from './powerbi-crosstable-triage.js';

const OPTS = { connectionId: '11111111-2222-3333-4444-555555555555', database: 'DB', schema: 'SCH' };
const tbl = (name: string, cols: string[], measures: any[] = []) => ({
  name,
  columns: cols.map((c) => ({ name: c, dataType: 'string', sourceColumn: c, summarizeBy: 'none' })),
  measures,
  partitions: [{ name, mode: 'import', source: { type: 'm',
    expression: `let S = Sql.Database("h","DB"), N = S{[Name="${name}",Kind="Table"]}[Data] in N` } }],
});

const STAR = {
  name: 'M', compatibilityLevel: 1600,
  model: {
    culture: 'en-US',
    tables: [
      tbl('SALES_FACT', ['AMOUNT', 'AGENT_KEY']),
      tbl('AGENT_DIM', ['AGENT_ID', 'AGENT_NAME'], [
        { name: 'Total Amount', expression: 'SUM(SALES_FACT[AMOUNT])' },
      ]),
    ],
    relationships: [{ name: 'r1', fromTable: 'SALES_FACT', fromColumn: 'AGENT_KEY',
                      toTable: 'AGENT_DIM', toColumn: 'AGENT_ID' }],
  },
};

test('T5a the warning now names the hosting View, its hop, and the verdict', () => {
  const out = convertPowerBIToSigma(STAR, OPTS);
  const w = out.warnings.find((x: string) => x.includes('Total Amount') && x.includes('cross-table measure'));
  assert.ok(w, 'the cross-table warning is still emitted');
  assert.match(w!, /TRIAGE:/);
  assert.match(w!, /SALES_FACT View/);
  assert.match(w!, /fan-out SAFE/);
});

const STAR_SIBLING_METRIC = {
  name: 'M', compatibilityLevel: 1600,
  model: {
    culture: 'en-US',
    tables: [
      tbl('SALES_FACT', ['AMOUNT', 'AGENT_KEY']),
      tbl('AGENT_DIM', ['AGENT_ID', 'AGENT_NAME'], [
        { name: 'Base Metric', expression: 'SUM(AGENT_DIM[AGENT_ID])' },
        // "Ratio" references a sibling metric (bare [Base Metric], legal DAX —
        // no table prefix — and legal Sigma, since it's just another metric on
        // the same element) AND a genuine cross-table column (SALES_FACT[AMOUNT]).
        { name: 'Ratio', expression: '[Base Metric] + SUM(SALES_FACT[AMOUNT])' },
      ]),
    ],
    relationships: [{ name: 'r1', fromTable: 'SALES_FACT', fromColumn: 'AGENT_KEY',
                      toTable: 'AGENT_DIM', toColumn: 'AGENT_ID' }],
  },
};

test('T10f end-to-end: a measure depending on a sibling metric PLUS a coverable column is not falsely "no View covers it"', () => {
  // Before the Important-4 fix, this warning read "TRIAGE: no View covers it
  // within the configured depth (references: Base Metric, AMOUNT)" — the
  // AMOUNT column alone is hop-0-safe on SALES_FACT View, but "Base Metric"
  // (a sibling metric, not a column) resolved to hop Infinity on every
  // candidate and sank the whole verdict to uncovered.
  const out = convertPowerBIToSigma(STAR_SIBLING_METRIC, OPTS);
  const w = out.warnings.find((x: string) => x.includes('"Ratio"') && x.includes('cross-table measure'));
  assert.ok(w, 'the cross-table warning is still emitted');
  assert.match(w!, /TRIAGE: hostable on "SALES_FACT View"/);
  assert.match(w!, /fan-out SAFE/);
  assert.doesNotMatch(w!, /no View covers it/);
  assert.match(w!, /Also depends on metric "Base Metric"/);
});

// ── The rawDax wiring itself has an end-to-end test, not just isNeverHostable
// unit tests ─────────────────────────────────────────────────────────────
// `_rawDax` at the drop site is looked up by `mm.name === metrics[i].name` —
// solely to feed `isNeverHostable`. Every isNeverHostable unit test (T3a-T3f)
// calls it directly with a hand-written string, which proves the FUNCTION is
// correct but nothing about the LOOKUP: if metric names ever diverged from
// PBI measure names (a rename, a display-name remap), `_rawDax` would
// silently become `''`, isNeverHostable would always return false, and the
// 22-of-108 never-hostable bucket would zero out — with every existing test
// in this file still green, because none of them exercise the real
// convertPowerBIToSigma → drop-site → triageCrossTable wiring for this
// specific bucket.
const STAR_SELECTEDVALUE = {
  name: 'M', compatibilityLevel: 1600,
  model: {
    culture: 'en-US',
    tables: [
      tbl('SALES_FACT', ['AMOUNT', 'AGENT_KEY']),
      tbl('AGENT_DIM', ['AGENT_ID', 'AGENT_NAME'], [
        // Cross-table (AMOUNT lives on SALES_FACT, not AGENT_DIM) AND
        // report-context-dependent (SELECTEDVALUE) — must be classified
        // never-hostable, not merely dropped as an ordinary cross-table ref.
        { name: 'Selected Amount', expression: 'SELECTEDVALUE(SALES_FACT[AMOUNT], 0)' },
      ]),
    ],
    relationships: [{ name: 'r1', fromTable: 'SALES_FACT', fromColumn: 'AGENT_KEY',
                      toTable: 'AGENT_DIM', toColumn: 'AGENT_ID' }],
  },
};

test('T11a end-to-end: a SELECTEDVALUE measure is triaged never-hostable through the real drop-site wiring', () => {
  const out = convertPowerBIToSigma(STAR_SELECTEDVALUE, OPTS);
  const w = out.warnings.find((x: string) => x.includes('"Selected Amount"') && x.includes('cross-table measure'));
  assert.ok(w, 'the cross-table warning is still emitted');
  assert.match(w!, /TRIAGE: report-context-dependent \(SELECTEDVALUE\/ISFILTERED\)/);
  assert.match(w!, /no static View can host it/);
});

test('T5b two calls are deterministic, and the dropped metric stays dropped (not the full no-output-change proof — see T5d)', () => {
  // This test proves two IN-PROCESS calls agree with EACH OTHER (determinism)
  // and that PR 1 doesn't attach anything it drops. It does NOT prove "changes
  // no emitted output" against `main` — a bug that made every conversion built
  // *some other*, differently-wrong-but-still-deterministic output would stay
  // green here. That stronger property (this PR's actual safety guarantee) is
  // T5d's job: a golden snapshot pinned to a value committed alongside the
  // code, so a regression has to change the COMMITTED expectation, not just
  // agree with itself.
  const out = convertPowerBIToSigma(STAR, OPTS);
  const strip = (o: any) => JSON.stringify({ model: o.model, stats: o.stats });
  assert.equal(strip(out), strip(convertPowerBIToSigma(STAR, OPTS)), 'deterministic');
  const metrics = (out.model.pages || []).flatMap((p: any) => p.elements || [])
    .flatMap((e: any) => e.metrics || []);
  assert.equal(metrics.find((m: any) => m.name === 'Total Amount'), undefined,
    'still dropped — PR 1 does not attach anything');
});

test('T5c describeTriage renders each bucket distinctly', () => {
  const mk = (over: any): any => ({ metric: 'M', homeTable: 'D', refs: ['X'],
    neverHostable: false, candidates: [], reachability: 'none', dependsOnMetrics: [], ...over });
  assert.match(describeTriage(mk({ neverHostable: true })), /report-context-dependent/);
  assert.match(describeTriage(mk({})), /no View covers it/);
  assert.match(
    describeTriage(mk({ reachability: 'one',
      candidates: [{ baseTable: 'F', maxHop: 1, verdict: 'safe', unsafeRefs: [] }] })),
    /"F View" \(1 hop, fan-out SAFE\)/);
  assert.match(
    describeTriage(mk({ reachability: 'none',
      candidates: [{ baseTable: 'F', maxHop: 1, verdict: 'fanout-risk', unsafeRefs: ['P'] }] })),
    /FAN-OUT RISK/);
  assert.match(
    describeTriage(mk({ reachability: 'many',
      candidates: [{ baseTable: 'F1', maxHop: 1, verdict: 'safe', unsafeRefs: [] },
                    { baseTable: 'F2', maxHop: 1, verdict: 'safe', unsafeRefs: [] }] })),
    /ambiguous/);
  // The malformed-formula sentinel is a distinct bucket from ordinary fan-out
  // risk: same underlying candidate shape (fanout-risk, non-empty unsafeRefs),
  // but a completely different operator message and action. If the branch
  // order in describeTriage ever regresses (e.g. the malformed check moves
  // after the generic fanout-risk check, or the sentinel string is typo'd),
  // this must fail — the two messages are mutually exclusive, never both.
  const malformedMsg = describeTriage(mk({ reachability: 'none',
    candidates: [{ baseTable: 'F', maxHop: 1, verdict: 'fanout-risk', unsafeRefs: ['malformed-formula'] }] }));
  assert.match(malformedMsg, /could not be parsed confidently/);
  assert.doesNotMatch(malformedMsg, /FAN-OUT RISK/);
});

// ── Prototype-named columns must never crash the pre-pass or the guard ─────
// A real Power BI model can and does contain a column literally named
// `toString`, `constructor`, `valueOf`, `hasOwnProperty`, etc. Against a `{}`
// (Object.prototype-backed) map, looking that key up resolves to the
// INHERITED prototype member — truthy, but not an array — so `.includes` (in
// `powerbi.ts`'s pre-pass) or a `for...of` walk (in `hopOf`) throws. The
// pre-pass runs unconditionally, before the table loop even starts, so this
// crashes conversions that succeed cleanly on `main` — breaking the "changes
// no emitted output" guarantee the whole PR is built on (a crash is the
// largest possible output change). Negative proof: reverting either fix
// (`Object.create(null)` in powerbi.ts, `Array.isArray` in hopOf) reproduces
// `triageColumnOwners[key].includes is not a function` / `function is not
// iterable` on these exact tests — verified by hand, not asserted here (there
// is no clean way to un-fix production code from within a test).
const STAR_PROTO_COL = {
  name: 'M', compatibilityLevel: 1600,
  model: {
    culture: 'en-US',
    tables: [
      tbl('SALES_FACT', ['AMOUNT', 'AGENT_KEY', 'toString', 'constructor']),
      tbl('AGENT_DIM', ['AGENT_ID', 'AGENT_NAME'], [
        { name: 'Total Amount', expression: 'SUM(SALES_FACT[AMOUNT])' },
      ]),
    ],
    relationships: [{ name: 'r1', fromTable: 'SALES_FACT', fromColumn: 'AGENT_KEY',
                      toTable: 'AGENT_DIM', toColumn: 'AGENT_ID' }],
  },
};

test('T9a a column literally named "toString"/"constructor" does not crash the triage pre-pass', () => {
  // The crash site (triageColumnOwners pre-pass in powerbi.ts) runs for EVERY
  // column on EVERY table, regardless of whether that column is ever referenced
  // by a cross-table measure — so merely having such a column anywhere in the
  // model is enough to reproduce it. The conversion must complete and still
  // emit the ordinary triaged warning for the unrelated cross-table drop.
  const out = convertPowerBIToSigma(STAR_PROTO_COL, OPTS);
  const w = out.warnings.find((x: string) => x.includes('Total Amount') && x.includes('cross-table measure'));
  assert.ok(w, 'conversion completed and the cross-table warning is still emitted');
  assert.match(w!, /TRIAGE:/);
  assert.match(w!, /SALES_FACT View/);
  assert.match(w!, /fan-out SAFE/);
});

test('T9b triageCrossTable stays safe against a plain-object-literal columnOwners with a prototype-shaped ref', () => {
  // Unit-level proof independent of the converter: callers of this module pass
  // ordinary object literals (every test in this file does), so the module
  // itself — not just powerbi.ts's pre-pass — must not trust truthiness when
  // indexing a caller-supplied map. `columnOwners: {}` has no OWN 'toString' or
  // 'constructor' key, so a naive lookup finds the inherited prototype member.
  const t1 = triageCrossTable(base({
    sigmaFormula: 'Sum([toString])', refs: ['toString'], columnOwners: {},
  }));
  assert.equal(t1.reachability, 'none', 'toString has no real owner — genuinely uncovered, not a crash');
  assert.deepEqual(t1.candidates, []);

  const t2 = triageCrossTable(base({
    sigmaFormula: 'Sum([constructor])', refs: ['constructor'], columnOwners: {},
  }));
  assert.equal(t2.reachability, 'none');
  assert.deepEqual(t2.candidates, []);
});

// ── Output-invariance golden snapshot ───────────────────────────────────────
// THIS is the automated guard for the safety property the whole PR series
// depends on: wiring triage into the drop site must change ONLY the warning
// text, NEVER the emitted `model` or `stats`. T5b (above) only proves two
// in-process calls agree with EACH OTHER — that's determinism, not
// output-invariance; it would stay green even if this task had subtly changed
// what gets built. This test pins `{ model, stats }` for the STAR fixture
// (never `warnings` — warning text is EXPECTED to change; that is the entire
// point of this task) to a value committed alongside the code.
//
// A FAILURE HERE MEANS ONE OF TWO THINGS: a real regression that started
// mutating emitted output, or a deliberate, intentional change to emitted
// output that needs this snapshot updated AND re-justified in the PR
// description. Never "fix" a failure here by loosening or deleting the
// assertion.
//
// STRIPPING, PRECISELY: this pins STRUCTURE, not the id-counter's current
// offsets. Two kinds of generated identity get erased on purpose:
//   1. Any key that IS an id or ENDS in the camelCase `Id` suffix (`id`,
//      `elementId`, `targetElementId`, `sourceColumnId`, `targetColumnId`, …)
//      — these are `sigmaShortId()`/`sigmaInodeId()` output from a single
//      monotonic per-run counter. They're deterministic FOR THIS INPUT today
//      (`resetIds()` runs with no seed at the top of `convertPowerBIToSigma`,
//      so the counter always restarts at 0), but the exact strings shift the
//      moment any UNRELATED call anywhere in the id-generating path is added,
//      removed, or reordered — a false alarm this snapshot must not raise.
//   2. Each `order` array (a ordered list of column ids) is replaced by its
//      LENGTH — still pins "how many columns are ordered", not which
//      generated ids compose the list. The same treatment applies to a
//      folder's `items` array (also a list of generated column ids, added by
//      the lookup-declutter grouping in buildDerivedElements) — pins "how
//      many columns are in this folder", not their generated identity.
// EXCEPTION: `connectionId` also ends in `Id` but is NOT counter-generated —
// it is the caller's `OPTS.connectionId` passed through verbatim — so it is
// deliberately kept. A future bug that drops or corrupts connection wiring
// on some element should still fail this test.
//
// TRADE-OFF, accepted rather than hidden: stripping `sourceColumnId` /
// `targetColumnId` means this snapshot can tell you a relationship's `keys`
// array still has an entry, but no longer WHICH two columns compose it (e.g.
// a regression that silently swapped the join columns would not be caught
// here — only elsewhere, e.g. by tests that assert on relationship column
// NAMES rather than a structural snapshot). Accepted because the alternative
// — pinning generated id strings — makes the far more common failure mode
// (spurious breakage from unrelated id-sequence churn) the norm, which
// trains people to regenerate goldens blindly. That is worse than the
// narrower guarantee this version gives.
const stripIds = (v: any): any =>
  JSON.parse(JSON.stringify(v), (k, val) => {
    if ((k === 'order' || k === 'items') && Array.isArray(val)) return val.length;
    if (k === 'connectionId') return val;
    if (k === 'id' || /Id$/.test(k)) return undefined;
    return val;
  });

const STAR_GOLDEN = {
  model: {
    name: 'M', schemaVersion: 1,
    pages: [{
      name: 'Page 1',
      elements: [
        {
          kind: 'table', name: 'SALES_FACT',
          source: { connectionId: '11111111-2222-3333-4444-555555555555', kind: 'warehouse-table', path: ['SCH', 'SALES_FACT'] },
          columns: [{ formula: '[SALES_FACT/Amount]' }, { formula: '[SALES_FACT/Agent Key]' }],
          order: 2,
          relationships: [{
            keys: [{}],
            name: 'AGENT_DIM',
          }],
        },
        {
          kind: 'table', name: 'AGENT_DIM',
          source: { connectionId: '11111111-2222-3333-4444-555555555555', kind: 'warehouse-table', path: ['SCH', 'AGENT_DIM'] },
          columns: [{ formula: '[AGENT_DIM/Agent Id]' }, { formula: '[AGENT_DIM/Agent Name]' }],
          order: 2,
        },
        {
          kind: 'table', name: 'SALES_FACT View',
          source: { kind: 'table' },
          columns: [
            { formula: '[SALES_FACT/Amount]' },
            { formula: '[SALES_FACT/Agent Key]' },
            { formula: '[SALES_FACT/AGENT_DIM/Agent Name]', hidden: true },
          ],
          // Lookup-declutter (Phase 1): the related column above is hidden and
          // grouped into a per-target folder — see buildDerivedElements in
          // sigma-ids.ts. `items` is stripped to its length (see stripIds).
          folders: [{ name: 'AGENT_DIM', items: 1 }],
          order: 3,
        },
      ],
    }],
  },
  stats: { tables: 2, elements: 3, columns: 7, metrics: 0, relationships: 1 },
};

test('T5d golden snapshot: triage changes NO emitted output — model+stats pinned', () => {
  const out = convertPowerBIToSigma(STAR, OPTS);
  const actual = stripIds({ model: out.model, stats: out.stats });
  assert.deepEqual(actual, STAR_GOLDEN,
    'emitted model/stats drifted from the committed golden — either a real ' +
    'regression, or a deliberate change that needs this snapshot updated and ' +
    're-justified, never silently loosened');
});

test('T5e the flip side of T5d: stripIds erases generated identity, never structure', () => {
  // T5d proves a REAL structural change fails the snapshot. This proves the
  // converse: two shapes that differ ONLY in their concrete generated-id
  // values (the exact kind of drift an unrelated, future id-generating call
  // anywhere in the path would cause) strip down to the SAME result — so T5d
  // does not, and will not, false-alarm on that drift alone. Standing
  // regression coverage for stripIds itself, independent of whatever the
  // STAR fixture's actual counter output happens to be today.
  const shapeWith = (tag: string) => ({
    id: `AAAAAAAAA${tag}`,
    name: 'SALES_FACT',
    source: { connectionId: 'same-conn-id', kind: 'warehouse-table' },   // NOT id-suffix-stripped — held constant
    relationships: [{
      targetElementId: `TARGET-${tag}`,
      keys: [{ sourceColumnId: `SRC-${tag}`, targetColumnId: `TGT-${tag}` }],
      name: 'AGENT_DIM',
    }],
    order: [`inode-${tag}-1`, `inode-${tag}-2`],   // same LENGTH, different literal ids
  });
  const a = shapeWith('1');
  const b = shapeWith('2');
  assert.notDeepEqual(a, b, 'sanity check: the two raw shapes must actually differ before stripping');
  assert.deepEqual(stripIds(a), stripIds(b),
    'two shapes differing ONLY in generated-id-shaped values must strip to an identical result');
});

// ── CHANGE 2: a dropped measure's "bad" ref can be a NAME, not a coverage gap ──
//
// 15 of the 32 `no-covering-View` measures (R1-R4 spike) are not reachability
// problems at all — they inherit an unrelated failure from a sibling METRIC ref
// that `columnOwners` (built only from `model.tables[].columns`) has no entry
// for, so it resolves to hop Infinity on every candidate exactly like a
// genuinely disconnected column would:
//   - 5 reference a metric declared on a DIFFERENT element — a hard Sigma
//     constraint (metrics cannot cross-reference another element's metric); no
//     hop limit ever fixes this.
//   - 6 have a same-table sibling metric dropped for FAN-OUT reasons (grain).
//   - 4 have a same-table sibling dropped as NEVER-HOSTABLE (SELECTEDVALUE).
// `Triage.dependsOnMetrics` already excludes a STILL-LIVE sibling metric ref
// from coverage — but once that sibling is itself dropped (cascade) or lives on
// another element, it no longer matches any "still-live metric name" the caller
// passes in as `metricRefs`, so the ref falls through to being judged as an
// ordinary, uncoverable column. `MetricBlocker`/`describeMetricBlocker` name
// the TRUE blocker directly, bypassing `triageCrossTable` entirely for these
// refs — they are not a reachability question, so a reachability classifier
// should never be asked to describe them.
import { describeMetricBlocker, type MetricBlocker } from './powerbi-crosstable-triage.js';

test('T12a describeMetricBlocker: cross-element metric names the OTHER element and says no hop limit fixes it', () => {
  const b: MetricBlocker = { kind: 'cross-element-metric', metric: 'Base Metric', ownerTable: 'AGENT_DIM' };
  const msg = describeMetricBlocker(b);
  assert.match(msg, /"Base Metric"/);
  assert.match(msg, /AGENT_DIM/);
  assert.match(msg, /DIFFERENT element/);
  assert.match(msg, /no hop limit/i);
  assert.doesNotMatch(msg, /no View covers it/, 'must not read like a reachability verdict');
});

test('T12b describeMetricBlocker: dropped-sibling quotes the sibling\'s own drop reason verbatim', () => {
  const b: MetricBlocker = {
    kind: 'dropped-sibling', metric: 'Sibling',
    siblingReason: 'TRIAGE: "SALES_FACT View" (1 hop) covers it but FAN-OUT RISK — [AGENT_NAME] would double-count across the join; rebuild at the visual\'s grain.',
  };
  const msg = describeMetricBlocker(b);
  assert.match(msg, /"Sibling"/);
  assert.match(msg, /FAN-OUT RISK/, 'the sibling\'s real drop reason is surfaced, not paraphrased away');
  assert.doesNotMatch(msg, /no View covers it/, 'must not read like a reachability verdict');
});

const tbl2 = (name: string, cols: string[], measures: any[] = []) => ({
  name,
  columns: cols.map((c) => ({ name: c, dataType: 'string', sourceColumn: c, summarizeBy: 'none' })),
  measures,
  partitions: [{ name, mode: 'import', source: { type: 'm',
    expression: `let S = Sql.Database("h","DB"), N = S{[Name="${name}",Kind="Table"]}[Data] in N` } }],
});

test('T13a end-to-end: a measure referencing ANOTHER element\'s metric reports the cross-element blocker, not "no View covers it"', () => {
  const m = {
    name: 'M', compatibilityLevel: 1600,
    model: {
      culture: 'en-US',
      tables: [
        tbl2('SALES_FACT', ['AMOUNT', 'AGENT_KEY']),
        tbl2('AGENT_DIM', ['AGENT_ID', 'AGENT_NAME'], [
          { name: 'Agent Metric', expression: 'SUM(AGENT_DIM[AGENT_ID])' },
        ]),
        // REGION_DIM has no relationship to anything — its own measure references
        // a metric declared on AGENT_DIM, a wholly different element.
        tbl2('REGION_DIM', ['REGION_ID'], [
          { name: 'Region Ratio', expression: '[Agent Metric] + 1' },
        ]),
      ],
      relationships: [{ name: 'r1', fromTable: 'SALES_FACT', fromColumn: 'AGENT_KEY',
                        toTable: 'AGENT_DIM', toColumn: 'AGENT_ID' }],
    },
  };
  const out = convertPowerBIToSigma(m, OPTS);
  const w = out.warnings.find((x: string) => x.includes('"Region Ratio"') && x.includes('cross-table measure'));
  assert.ok(w, `the cross-table warning is still emitted (got ${JSON.stringify(out.warnings)})`);
  assert.match(w!, /"Agent Metric"/);
  assert.match(w!, /AGENT_DIM/);
  assert.match(w!, /DIFFERENT element/);
  assert.doesNotMatch(w!, /no View covers it/, 'the real blocker is the cross-element metric ref, not reachability');
  // Review round 1, finding 2: the generic "recreate in a workbook element..."
  // clause implies a View-based fix exists, contradicting "no hop limit fixes
  // this" in the very same warning. Must be suppressed on the blocker path.
  assert.doesNotMatch(w!, /recreate in a workbook element/,
    'the generic View-based-fix clause must not contradict "no hop limit fixes this" in the same warning');
});

test('T13b end-to-end: a measure depending on a sibling dropped for FAN-OUT reports that, not "no View covers it"', () => {
  const m = {
    name: 'M', compatibilityLevel: 1600,
    model: {
      culture: 'en-US',
      tables: [
        // "Sibling" sums AGENT_DIM's own column ALONE while living on SALES_FACT —
        // the T4b shape, and AGENT_DIM has NO outgoing relationship of its own, so
        // there is no alternate safe host: this measure's OWN verdict is genuinely
        // fanout-risk, not merely uncovered.
        tbl2('SALES_FACT', ['AMOUNT', 'AGENT_KEY'], [
          { name: 'Sibling', expression: 'SUM(AGENT_DIM[AGENT_NAME])' },
          { name: 'Main', expression: '[Sibling] + SUM(SALES_FACT[AMOUNT])' },
        ]),
        tbl2('AGENT_DIM', ['AGENT_ID', 'AGENT_NAME']),
      ],
      relationships: [{ name: 'r1', fromTable: 'SALES_FACT', fromColumn: 'AGENT_KEY',
                        toTable: 'AGENT_DIM', toColumn: 'AGENT_ID' }],
    },
  };
  const out = convertPowerBIToSigma(m, OPTS);
  const wSibling = out.warnings.find((x: string) => x.includes('"Sibling"') && x.includes('cross-table measure'));
  assert.ok(wSibling, `Sibling itself is dropped as an ordinary cross-table fan-out risk (got ${JSON.stringify(out.warnings)})`);
  assert.match(wSibling!, /FAN-OUT RISK/);

  const wMain = out.warnings.find((x: string) => x.includes('"Main"') && x.includes('cross-table measure'));
  assert.ok(wMain, `Main is still dropped and warned about (got ${JSON.stringify(out.warnings)})`);
  assert.match(wMain!, /"Sibling"/);
  assert.match(wMain!, /FAN-OUT RISK/, 'Main\'s warning surfaces the REAL blocker — Sibling\'s own fan-out drop');
  assert.doesNotMatch(wMain!, /no View covers it/, 'Main must not be attributed to reachability');
  assert.doesNotMatch(wMain!, /recreate in a workbook element/,
    'the generic View-based-fix clause is not the real fix for a sibling-metric dependency');
});

test('T13c end-to-end: a measure depending on a sibling dropped as NEVER-HOSTABLE reports that, not "no View covers it"', () => {
  const m = {
    name: 'M', compatibilityLevel: 1600,
    model: {
      culture: 'en-US',
      tables: [
        tbl2('SALES_FACT', ['AMOUNT', 'AGENT_KEY'], [
          // Cross-table (AGENT_NAME lives on AGENT_DIM, not SALES_FACT) AND
          // report-context-dependent (SELECTEDVALUE) — same shape as T11a.
          { name: 'Sibling', expression: 'SELECTEDVALUE(AGENT_DIM[AGENT_NAME], "")' },
          { name: 'Main', expression: '[Sibling] + SUM(SALES_FACT[AMOUNT])' },
        ]),
        tbl2('AGENT_DIM', ['AGENT_ID', 'AGENT_NAME']),
      ],
      relationships: [{ name: 'r1', fromTable: 'SALES_FACT', fromColumn: 'AGENT_KEY',
                        toTable: 'AGENT_DIM', toColumn: 'AGENT_ID' }],
    },
  };
  const out = convertPowerBIToSigma(m, OPTS);
  const wSibling = out.warnings.find((x: string) => x.includes('"Sibling"') && x.includes('cross-table measure'));
  assert.ok(wSibling, `Sibling itself is dropped as never-hostable (got ${JSON.stringify(out.warnings)})`);
  assert.match(wSibling!, /report-context-dependent/);

  const wMain = out.warnings.find((x: string) => x.includes('"Main"') && x.includes('cross-table measure'));
  assert.ok(wMain, `Main is still dropped and warned about (got ${JSON.stringify(out.warnings)})`);
  assert.match(wMain!, /"Sibling"/);
  assert.match(wMain!, /report-context-dependent/, 'Main\'s warning surfaces the REAL blocker — Sibling\'s own never-hostable drop');
  assert.doesNotMatch(wMain!, /no View covers it/, 'Main must not be attributed to reachability');
  assert.doesNotMatch(wMain!, /recreate in a workbook element/,
    'the generic View-based-fix clause is not the real fix for a sibling-metric dependency');
});

// Review round 1, finding 1: the OLD code checked `allMetricOwner`/
// `siblingDropReason` BEFORE running triageCrossTable at all — a pure NAME
// match, with no check for whether the ref is ALSO a real, reachable column.
// A name collision (two unrelated things sharing a string) then reported a
// perfectly re-homable measure as permanently unfixable.
test('T13d Fix (round 1, finding 1): a genuine reachable COLUMN is not blocked just because an unrelated element ALSO declares a same-named metric', () => {
  const m = {
    name: 'M', compatibilityLevel: 1600,
    model: {
      culture: 'en-US',
      tables: [
        tbl2('SALES_FACT', ['AMOUNT', 'AGENT_KEY'], [
          { name: 'Tiered Amount', expression: 'MAX(AGENT_DIM[AGENT_NAME])' },
        ]),
        // AGENT_DIM needs its OWN outgoing relationship to qualify as a candidate
        // base at all (triageCrossTable's `bases` are tables that appear as
        // `from` in some relationship) — same shape as T4b2.
        tbl2('AGENT_DIM', ['AGENT_ID', 'AGENT_NAME']),
        tbl2('REGION_DIM', ['REGION_ID']),
        // ORPHAN_TABLE has NO relationship to anything at all. Its measure name
        // "AGENT_NAME" is a pure coincidence — the SAME string as AGENT_DIM's
        // genuine, reachable column, but the two are otherwise unrelated.
        tbl2('ORPHAN_TABLE', ['SOME_COL'], [
          { name: 'AGENT_NAME', expression: 'SUM(ORPHAN_TABLE[SOME_COL])' },
        ]),
      ],
      relationships: [
        { name: 'r1', fromTable: 'SALES_FACT', fromColumn: 'AGENT_KEY', toTable: 'AGENT_DIM', toColumn: 'AGENT_ID' },
        { name: 'r2', fromTable: 'AGENT_DIM', fromColumn: 'AGENT_ID', toTable: 'REGION_DIM', toColumn: 'REGION_ID' },
      ],
    },
  };
  const out = convertPowerBIToSigma(m, OPTS);
  const w = out.warnings.find((x: string) => x.includes('"Tiered Amount"') && x.includes('cross-table measure'));
  assert.ok(w, `the cross-table warning is still emitted (got ${JSON.stringify(out.warnings)})`);
  assert.match(w!, /hostable on "AGENT_DIM View"/, 'the REAL, reachable column verdict wins over the name collision');
  assert.match(w!, /fan-out SAFE/);
  assert.doesNotMatch(w!, /DIFFERENT element/, 'must not be misreported as a cross-element metric block');
  assert.doesNotMatch(w!, /no hop limit fixes this/, 'must not claim this is unfixable — it is a one-hop re-home');
});
