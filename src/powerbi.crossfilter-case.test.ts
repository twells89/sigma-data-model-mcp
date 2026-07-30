/**
 * Power BI → Sigma: CALCULATE relationship MODIFIERS, and case-insensitive column refs.
 *
 * Both defects were found by auditing 4 real insurance reports; every fixture here is
 * SYNTHETIC (generic SALES_FACT / AGENT_DIM / SUBMISSION_DIM names) — no real report data.
 *
 * DEFECT A — CROSSFILTER(...) is not stripped.
 *   CROSSFILTER(t1[c1], t2[c2], None|Both|OneWay) is a CALCULATE *modifier*: it changes a
 *   relationship's cross-filter DIRECTION. Its two column arguments are relationship
 *   ENDPOINTS, not values the aggregate reads. USERELATIONSHIP — the sibling modifier — is
 *   already stripped by extractUseRelationships, but CROSSFILTER was not, so its endpoint
 *   columns survived into the emitted metric formula. The cross-table guard then saw a
 *   reference to a column that is not on this element and DROPPED the whole measure.
 *   Measured: 2 real measures ("Tiered CP Prem", "Tiered GL Prem") were lost this way,
 *   each on a `CROSSFILTER(FACT[AGENt_KEY], DIM[CHILD_ID], None)` argument that has
 *   nothing to do with the SUM being computed.
 *   Sigma has no cross-filter-direction concept, so stripping is the correct translation —
 *   but it IS a fidelity change, so it must WARN rather than vanish.
 *
 * DEFECT B — column-reference matching is case-SENSITIVE.
 *   DAX column references are case-INSENSITIVE: `[AGENt_KEY]` is a valid reference to
 *   `AGENT_KEY`, and real models contain exactly such typos. The cross-table guard compared
 *   refs against exact-match Sets, so a mis-cased ref looked like a foreign column and the
 *   measure was dropped. Measured in real models: `AGENt_KEY` vs `AGENT_KEY` (2 measures)
 *   and `SUBMISSION_key` vs `SUBMISSION_KEY` (1 measure, which additionally caused
 *   USERELATIONSHIP not to match any model relationship, silently ignoring the filter).
 *   Resolving case-insensitively must also NORMALIZE the ref to the canonical column name,
 *   or the formula still carries a name Sigma cannot resolve.
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { convertPowerBIToSigma, extractCrossFilters } from './powerbi.js';

const OPTS = { connectionId: '11111111-2222-3333-4444-555555555555', database: 'DB', schema: 'SCH' };

const model = (tables: any[]) => ({
  name: 'M', compatibilityLevel: 1600,
  model: { culture: 'en-US', tables, relationships: [] },
});

const tbl = (name: string, cols: string[], measures: any[] = []) => ({
  name,
  columns: cols.map((c) => ({ name: c, dataType: 'string', sourceColumn: c, summarizeBy: 'none' })),
  measures,
  partitions: [{ name, mode: 'import', source: { type: 'm',
    expression: `let S = Sql.Database("h","DB"), N = S{[Name="${name}",Kind="Table"]}[Data] in N` } }],
});

const metricsOf = (out: any) =>
  (out.model.pages || []).flatMap((p: any) => p.elements || []).flatMap((e: any) => e.metrics || []);

// ── DEFECT A: CROSSFILTER ────────────────────────────────────────────────────────

test('A1 extractCrossFilters strips the call and returns its endpoint pairs', () => {
  const dax = 'CALCULATE(SUM(SALES_FACT[AMOUNT]), CROSSFILTER(SALES_FACT[AGENT_KEY], AGENT_DIM[AGENT_ID], None))';
  const { dax: cleaned, pairs } = extractCrossFilters(dax);
  assert.doesNotMatch(cleaned, /CROSSFILTER/i, 'the modifier is gone');
  assert.match(cleaned, /SUM\(SALES_FACT\[AMOUNT\]\)/, 'the aggregate is untouched');
  assert.equal(pairs.length, 1);
  assert.deepEqual(pairs[0].a, { table: 'SALES_FACT', column: 'AGENT_KEY' });
  assert.deepEqual(pairs[0].b, { table: 'AGENT_DIM', column: 'AGENT_ID' });
  assert.equal(pairs[0].direction, 'None');
});

test('A2 the endpoint columns do NOT leak into the emitted metric, so it is not dropped', () => {
  const m = model([
    tbl('SALES_FACT', ['AMOUNT', 'AGENT_KEY'], [{
      name: 'Tiered Amount',
      expression: 'CALCULATE(SUM(SALES_FACT[AMOUNT]), CROSSFILTER(SALES_FACT[AGENT_KEY], AGENT_DIM[AGENT_ID], None))',
    }]),
    tbl('AGENT_DIM', ['AGENT_ID']),
  ]);
  const out = convertPowerBIToSigma(m, OPTS);
  const names = metricsOf(out).map((x: any) => x.name);
  assert.ok(names.includes('Tiered Amount'), `metric survived (got ${JSON.stringify(names)})`);
  const f = String(metricsOf(out).find((x: any) => x.name === 'Tiered Amount').formula);
  assert.doesNotMatch(f, /AGENT_ID/, 'the far-side endpoint is not in the formula');
  assert.doesNotMatch(f, /CROSSFILTER/i, 'the modifier is not in the formula');
  assert.match(f, /Sum\(/i, 'the aggregate survives');
});

test('A3 stripping CROSSFILTER WARNS — it is a fidelity change, not a silent no-op', () => {
  const m = model([
    tbl('SALES_FACT', ['AMOUNT', 'AGENT_KEY'], [{
      name: 'Tiered Amount',
      expression: 'CALCULATE(SUM(SALES_FACT[AMOUNT]), CROSSFILTER(SALES_FACT[AGENT_KEY], AGENT_DIM[AGENT_ID], None))',
    }]),
    tbl('AGENT_DIM', ['AGENT_ID']),
  ]);
  const out = convertPowerBIToSigma(m, OPTS);
  const w = (out.warnings || []).filter((x: string) => /CROSSFILTER/i.test(x));
  assert.ok(w.length >= 1, `a CROSSFILTER warning is emitted (got ${JSON.stringify(out.warnings)})`);
  assert.match(w[0], /Tiered Amount/, 'the warning names the measure');
  assert.match(w[0], /direction|cross-filter/i, 'and says what was lost');
});

test('A4 CROSSFILTER alongside USERELATIONSHIP: both modifiers strip, aggregate intact', () => {
  const dax = 'CALCULATE(SUM(SALES_FACT[AMOUNT]), ' +
              'USERELATIONSHIP(SUBMISSION_DIM[SUB_KEY], SALES_FACT[SUB_KEY]), ' +
              'CROSSFILTER(SALES_FACT[AGENT_KEY], AGENT_DIM[AGENT_ID], None))';
  const { dax: cleaned } = extractCrossFilters(dax);
  assert.doesNotMatch(cleaned, /CROSSFILTER/i);
  assert.match(cleaned, /USERELATIONSHIP/i, 'CROSSFILTER stripping leaves USERELATIONSHIP for its own pass');
  assert.match(cleaned, /SUM\(SALES_FACT\[AMOUNT\]\)/);
});

test('A5 a CALCULATE left with ONLY a CROSSFILTER modifier unwraps to the bare aggregate', () => {
  const m = model([
    tbl('SALES_FACT', ['AMOUNT', 'AGENT_KEY'], [{
      name: 'Plain Amount',
      expression: 'CALCULATE(SUM(SALES_FACT[AMOUNT]), CROSSFILTER(SALES_FACT[AGENT_KEY], AGENT_DIM[AGENT_ID], Both))',
    }]),
    tbl('AGENT_DIM', ['AGENT_ID']),
  ]);
  const out = convertPowerBIToSigma(m, OPTS);
  const f = String(metricsOf(out).find((x: any) => x.name === 'Plain Amount').formula);
  assert.match(f, /^Sum\(\[AMOUNT\]\)$/i, `unwrapped to the bare aggregate (got ${f})`);
});

// ── DEFECT B: case-insensitive column refs ───────────────────────────────────────

test('B1 a mis-cased column ref resolves and the measure is NOT dropped', () => {
  const m = model([
    tbl('SALES_FACT', ['AMOUNT', 'AGENT_KEY'], [{
      name: 'Miscased Sum',
      // DAX is case-insensitive: [AGENt_KEY] IS [AGENT_KEY]. Real models contain this typo.
      expression: 'CALCULATE(SUM(SALES_FACT[AMOUNT]), SALES_FACT[AGENt_KEY] = "X")',
    }]),
  ]);
  const out = convertPowerBIToSigma(m, OPTS);
  const names = metricsOf(out).map((x: any) => x.name);
  assert.ok(names.includes('Miscased Sum'),
    `a mis-cased ref must not drop the measure (got ${JSON.stringify(names)}; warnings ${JSON.stringify(out.warnings)})`);
});

test('B2 the emitted formula uses the CANONICAL column name, not the mis-cased one', () => {
  const m = model([
    tbl('SALES_FACT', ['AMOUNT', 'AGENT_KEY'], [{
      name: 'Miscased Sum',
      expression: 'CALCULATE(SUM(SALES_FACT[AMOUNT]), SALES_FACT[AGENt_KEY] = "X")',
    }]),
  ]);
  const out = convertPowerBIToSigma(m, OPTS);
  const f = String(metricsOf(out).find((x: any) => x.name === 'Miscased Sum').formula);
  assert.doesNotMatch(f, /AGENt_KEY/, 'the mis-cased spelling is normalized away');
  assert.match(f, /AGENT_KEY/, `and the canonical name is used (got ${f})`);
});

test('B3 a genuinely FOREIGN column is still dropped — the fix must not blunt the guard', () => {
  const m = model([
    tbl('SALES_FACT', ['AMOUNT'], [{
      name: 'Cross Table Sum',
      expression: 'SUM(OTHER_FACT[SOMETHING_ELSE])',
    }]),
    tbl('OTHER_FACT', ['SOMETHING_ELSE']),
  ]);
  const out = convertPowerBIToSigma(m, OPTS);
  const onFact = (out.model.pages || []).flatMap((p: any) => p.elements || [])
    .find((e: any) => e.name === 'SALES_FACT');
  const names = (onFact?.metrics || []).map((x: any) => x.name);
  assert.ok(!names.includes('Cross Table Sum'),
    'a real cross-table reference is still dropped');
  assert.ok((out.warnings || []).some((w: string) => /cross-table measure/.test(w)),
    'and still warns');
});

// ── DEFECT C: DAX comments are never stripped ───────────────────────────────────
//
// Found while chasing why the two CROSSFILTER measures STILL dropped after Defect A was
// fixed. The DAX carried a block comment:
//     /*use relationship for submission dim & agent submission fact allows proper
//       filtering with crossfilter removing the model relationship*/
// Nothing strips DAX comments, so the CALCULATE filter-predicate detector matched the
// words INSIDE the comment ("relationship", "filtering", "crossfilter") and rejected the
// measure as having a filter-context predicate. A measure was being dropped because of a
// COMMENT. 19 of the measures across 4 real models carry comments, so this is general,
// not a one-off. Stripping must respect string literals — a "//" inside a quoted string
// is data, not a comment.

test('C1 a block comment does not make a measure look like a filter-context predicate', () => {
  const m = model([
    tbl('SALES_FACT', ['AMOUNT', 'STATUS'], [{
      name: 'Commented Sum',
      expression: 'CALCULATE(SUM(SALES_FACT[AMOUNT]), /*use relationship for filtering with crossfilter*/ SALES_FACT[STATUS] = "Closed")',
    }]),
  ]);
  const out = convertPowerBIToSigma(m, OPTS);
  const names = metricsOf(out).map((x: any) => x.name);
  assert.ok(names.includes('Commented Sum'),
    `a comment must not drop the measure (got ${JSON.stringify(names)}; warnings ${JSON.stringify(out.warnings)})`);
  const f = String(metricsOf(out).find((x: any) => x.name === 'Commented Sum').formula);
  assert.doesNotMatch(f, /use relationship/, 'the comment text is not in the emitted formula');
  assert.doesNotMatch(f, /\/\*/, 'no comment delimiters survive');
});

test('C2 a line comment is stripped too', () => {
  const m = model([
    tbl('SALES_FACT', ['AMOUNT', 'STATUS'], [{
      name: 'Line Commented',
      expression: 'CALCULATE(\n  SUM(SALES_FACT[AMOUNT]),  // filter to closed only\n  SALES_FACT[STATUS] = "Closed"\n)',
    }]),
  ]);
  const out = convertPowerBIToSigma(m, OPTS);
  const names = metricsOf(out).map((x: any) => x.name);
  assert.ok(names.includes('Line Commented'), `survived (got ${JSON.stringify(names)})`);
  const f = String(metricsOf(out).find((x: any) => x.name === 'Line Commented').formula);
  assert.doesNotMatch(f, /filter to closed/, 'the line comment is gone');
});

test('C3 a "//" or "/*" INSIDE a string literal is data, never treated as a comment', () => {
  const m = model([
    tbl('SALES_FACT', ['AMOUNT', 'URL_PATH'], [{
      name: 'Url Sum',
      expression: 'CALCULATE(SUM(SALES_FACT[AMOUNT]), SALES_FACT[URL_PATH] = "https://x.test/a/*b*/c")',
    }]),
  ]);
  const out = convertPowerBIToSigma(m, OPTS);
  const mm = metricsOf(out).find((x: any) => x.name === 'Url Sum');
  assert.ok(mm, `metric survived (warnings ${JSON.stringify(out.warnings)})`);
  assert.match(String(mm.formula), /https:\/\/x\.test\/a\/\*b\*\/c/,
    `the string literal is preserved intact (got ${mm.formula})`);
});
