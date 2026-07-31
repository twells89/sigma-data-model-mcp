# Power BI cross-table measure TRIAGE — Implementation Plan (PR 1 + spike)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn 110 opaque `cross-table measure — dropped` warnings into a triaged list naming the View that could host each measure, its hop distance, and its fan-out verdict — without changing anything the converter emits.

**Architecture:** A new pure module `src/powerbi-crosstable-triage.ts` classifies a dropped measure from the raw model alone (columns + relationships), with no dependency on Views or the relationship graph the converter builds later. `src/powerbi.ts` calls it at the existing drop site and appends the verdict to the warning it already pushes. Nothing else changes.

**Tech Stack:** TypeScript, Node built-in test runner (`node --import tsx/esm --test`), `node:assert/strict`.

## Global Constraints

- **Repo:** `sigma-data-model-mcp`, branch `feat/pbi-cross-table-triage` (already exists, already pushed, already carries the design spec).
- **This plan covers PR 1 and the spike ONLY.** PR 2 (deferred drop + attach) is planned separately, after the spike answers §5 of the spec and after Task 6 produces measured buckets.
- **No behaviour change.** After Task 5, the set of emitted metrics, columns, and elements must be byte-identical to `main`. Only warning *text* changes. This is asserted in Task 5.
- **No customer-identifying strings** in any commit message, branch name, test fixture, or committed file. Reports are `R1`–`R4`. All test fixtures synthetic, using generic `SALES_FACT` / `AGENT_DIM` style names — follow the existing convention in `src/powerbi.crossfilter-case.test.ts`.
- **Register every new test file in `package.json`'s `test` script.** The runner takes an explicit file list, not a glob. An unregistered suite runs nowhere and reports green. This is a hard requirement of every task that adds a test file.
- **Baseline, measured on `main` @ `12919f2` for this branch:** `# tests 438 / # pass 409 / # fail 26 / # skipped 3`. **26 pre-existing failures.** After every task, `# fail` must still read **26**. If it rises, you broke something — fix it before committing. Do not infer "pre-existing" from "`main` fails identically" without checking whether this branch made `main` start failing. (The handoff's `427/398/26` was the pre-#114 figure; the failure count is the same.)
- **`npm install` has already been run** in the working clone. Do not re-run it.
- **Sigma aggregate spelling:** `Avg` not `Average`; `CountDistinct` not `DistinctCount`.
- **Commit trailer:** `Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>`

---

## File Structure

| file | responsibility |
|---|---|
| `src/powerbi-crosstable-triage.ts` | **Create.** Pure classifier. Four exported functions, no imports from `powerbi.ts`. Kept out of `powerbi.ts` because that file is already 3172 lines and this logic is independently testable. |
| `src/powerbi.crosstable-triage.test.ts` | **Create.** Unit tests for all four functions. |
| `src/powerbi.ts` | **Modify.** Two edits only: a `columnOwners` pre-pass before the table loop, and the enriched warning at the drop site. |
| `package.json` | **Modify.** Add the new test file to the `test` script. |

### Why the classifier gets its own `columnOwners` pre-pass

`tableColMap` is initialised *inside* the same loop that contains the drop (`tableColMap[tableName] = {}` at `powerbi.ts:2311`; drop at `powerbi.ts:2609`). When table *k* drops a measure, tables *k+1..N* have no entries yet, so `tableColMap` cannot answer "which table owns column X". The pre-pass reads `model.tables` — fully available from the start — instead.

Display names derived in the pre-pass may not match the converter's exactly in edge cases. That is acceptable **for PR 1 only**, because a wrong derivation degrades a *message*. PR 2 must not rely on this pre-pass; by PR 2's attach point the loop has finished and the real maps are complete.

---

## Task 1: `enclosingAggregate`

> ⚠️ **This task's `DUP_UNSAFE` exemption list (Min/Max/CountDistinct-are-safe) is UNSOUND and was replaced.** Task 8 below is authoritative — read it before implementing anything from this task's guard-rule content. Both adversarial passes that found the defect, and the corrected base-grain rule, live there. `enclosingAggregate` itself (the function this task actually produces) is still correct and still used; only the guard rule this task motivates it for was wrong.

Finds the nearest **aggregate** function wrapping a reference. Not the nearest function — `Sum(If([FLAG] = 1, [AMT], 0))` aggregates `AMT` even though `If` is closer. Getting this wrong inverts the fan-out verdict, so it is tested first and alone.

**Files:**
- Create: `src/powerbi-crosstable-triage.ts`
- Create: `src/powerbi.crosstable-triage.test.ts`
- Modify: `package.json` (`test` script)

**Interfaces:**
- Consumes: nothing.
- Produces: `enclosingAggregate(formula: string, ref: string): string | null`, and `DUP_UNSAFE: Set<string>` used by Task 4.

- [ ] **Step 1: Record the test baseline before touching anything**

```bash
cd <repo> && git checkout main && npm test 2>&1 | tail -20 > /tmp/baseline-main.txt; cat /tmp/baseline-main.txt
git checkout feat/pbi-cross-table-triage
```

Write the failure count down. You will compare against it in every later task.

- [ ] **Step 2: Write the failing test**

Create `src/powerbi.crosstable-triage.test.ts`:

```ts
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
import { enclosingAggregate } from './powerbi-crosstable-triage.js';

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
```

- [ ] **Step 3: Run it and confirm it fails for the right reason**

```bash
npm test -- 2>&1 | head -5   # will not yet include the new file
node --import tsx/esm --test src/powerbi.crosstable-triage.test.ts
```

Expected: FAIL — `Cannot find module './powerbi-crosstable-triage.js'`. If it fails any other way, stop and read the error.

- [ ] **Step 4: Write the implementation**

Create `src/powerbi-crosstable-triage.ts`:

```ts
/**
 * Cross-table measure TRIAGE — pure classification, no side effects.
 *
 * When the converter drops a measure because it references a column that is not
 * on its home element, this module answers three questions from the RAW model
 * alone (no Views, no relationship elements — neither exists at drop time):
 *   1. Which "<T> View" element could carry every reference?
 *   2. How many join hops away is the furthest reference?
 *   3. Would aggregating across those hops double-count?
 */

/** Sigma aggregate functions. Spelling matters: Avg (not Average), CountDistinct. */
const AGGREGATES = new Set([
  'Sum', 'Count', 'CountDistinct', 'Avg', 'Min', 'Max',
  'StdDev', 'Var', 'Median', 'Percentile', 'CountIf', 'SumIf',
]);

/**
 * Aggregates that are NOT idempotent under row duplication. A dimension column
 * denormalized onto a fact's View repeats once per fact row, so these over-count.
 * Min/Max/CountDistinct survive duplication unchanged and are therefore safe.
 */
export const DUP_UNSAFE = new Set([
  'Sum', 'Count', 'Avg', 'StdDev', 'Var', 'Median', 'Percentile', 'CountIf', 'SumIf',
]);

/**
 * The nearest AGGREGATE function enclosing `[ref]`, or null if the reference is
 * not aggregated. Walks outward past non-aggregate wrappers: in
 * `Sum(If([FLAG] = 1, [AMOUNT], 0))` the answer for AMOUNT is Sum, not If.
 */
export function enclosingAggregate(formula: string, ref: string): string | null {
  const idx = String(formula).indexOf(`[${ref}]`);
  if (idx < 0) return null;
  let depth = 0;
  for (let i = idx - 1; i >= 0; i--) {
    const ch = formula[i];
    if (ch === ')') { depth++; continue; }
    if (ch !== '(') continue;
    if (depth > 0) { depth--; continue; }
    const m = formula.slice(0, i).match(/([A-Za-z_]\w*)\s*$/);
    if (m && AGGREGATES.has(m[1])) return m[1];
    // a non-aggregate wrapper (If, Coalesce, arithmetic grouping) — keep going outward
  }
  return null;
}
```

- [ ] **Step 5: Register the test file, then run**

In `package.json`, append ` src/powerbi.crosstable-triage.test.ts` to the end of the `test` script's file list. Then:

```bash
npm test 2>&1 | tail -20
```

Expected: the 7 new `T1*` tests pass, and the failure count equals the Step 1 baseline. If the failure count went UP, you broke something — fix it before continuing.

- [ ] **Step 6: Commit**

```bash
git add src/powerbi-crosstable-triage.ts src/powerbi.crosstable-triage.test.ts package.json
git commit -m "$(cat <<'EOF'
powerbi: enclosingAggregate — find the aggregate wrapping a column reference

Walks outward past non-aggregate wrappers, so Sum(If(...,[X],0)) correctly
reports X as summed. First piece of the cross-table triage classifier.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: `reachableTables`

Breadth-first walk over **outgoing** relationships only. Outgoing is the many→one direction in Power BI, so traversal never multiplies the base element's rows at any depth.

**Files:**
- Modify: `src/powerbi-crosstable-triage.ts`
- Modify: `src/powerbi.crosstable-triage.test.ts`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `type Rel = { from: string; to: string }` and `reachableTables(from: string, rels: Rel[], maxDepth: number): Map<string, number>` — table name → hop distance, including the origin at hop 0. Used by Task 4.

- [ ] **Step 1: Write the failing test**

Append to `src/powerbi.crosstable-triage.test.ts`:

```ts
import { reachableTables } from './powerbi-crosstable-triage.js';

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
```

- [ ] **Step 2: Run and confirm it fails**

```bash
node --import tsx/esm --test src/powerbi.crosstable-triage.test.ts 2>&1 | tail -10
```

Expected: FAIL — `reachableTables is not a function` (or an import error). The `T1*` tests must still pass.

- [ ] **Step 3: Write the implementation**

Append to `src/powerbi-crosstable-triage.ts`:

```ts
/** A Power BI relationship, in its declared (many → one) direction. */
export type Rel = { from: string; to: string };

/**
 * Tables reachable from `from` by following OUTGOING relationships, mapped to
 * hop distance (origin = 0). Outgoing is many:one, and many:one composed with
 * many:one is still many:one, so this walk never multiplies the origin's rows —
 * at any depth. First visit wins, so distances are shortest-path and cycles
 * terminate.
 */
export function reachableTables(from: string, rels: Rel[], maxDepth: number): Map<string, number> {
  const out = new Map<string, number>([[from, 0]]);
  let frontier = [from];
  for (let d = 1; d <= maxDepth && frontier.length; d++) {
    const next: string[] = [];
    for (const t of frontier) {
      for (const r of rels) {
        if (r.from !== t || out.has(r.to)) continue;
        out.set(r.to, d);
        next.push(r.to);
      }
    }
    frontier = next;
  }
  return out;
}
```

- [ ] **Step 4: Run and confirm it passes**

```bash
npm test 2>&1 | tail -20
```

Expected: all `T1*` and `T2*` pass; failure count still equals the Task 1 Step 1 baseline.

- [ ] **Step 5: Commit**

```bash
git add src/powerbi-crosstable-triage.ts src/powerbi.crosstable-triage.test.ts
git commit -m "$(cat <<'EOF'
powerbi: reachableTables — shortest-hop walk over outgoing relationships

Outgoing is many:one, so composing hops never multiplies the base element's
rows. Cycles terminate via first-visit-wins.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: `isNeverHostable`

Measures using `SELECTEDVALUE` or `ISFILTERED` read the *report* filter context. No static View can host them, at any depth. Measured: 24 of the 110. These must be reported as never-hostable rather than as "no covering View", or the triage overstates how much is recoverable.

**Files:**
- Modify: `src/powerbi-crosstable-triage.ts`
- Modify: `src/powerbi.crosstable-triage.test.ts`

**Interfaces:**
- Produces: `isNeverHostable(rawDax: string): boolean`. Used by Task 4.

- [ ] **Step 1: Write the failing test**

Append to the test file:

```ts
import { isNeverHostable } from './powerbi-crosstable-triage.js';

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
```

- [ ] **Step 2: Run and confirm it fails**

```bash
node --import tsx/esm --test src/powerbi.crosstable-triage.test.ts 2>&1 | tail -10
```

Expected: FAIL — `isNeverHostable is not a function`.

- [ ] **Step 3: Write the implementation**

Append to `src/powerbi-crosstable-triage.ts`:

```ts
/**
 * True when the measure reads the REPORT filter context and therefore cannot be
 * hosted on any static View, at any depth. Matches a CALL (token followed by an
 * open paren) so a column merely named SELECTEDVALUE_FLAG is not a false positive.
 *
 * Scoped deliberately to the two tokens measured across R1-R4 (24 of 110 drops).
 * Adding tokens changes the measured buckets — re-run the Task 6 measurement if
 * you extend this set.
 */
export function isNeverHostable(rawDax: string): boolean {
  return /\b(?:SELECTEDVALUE|ISFILTERED)\s*\(/i.test(String(rawDax || ''));
}
```

- [ ] **Step 4: Run and confirm it passes**

```bash
npm test 2>&1 | tail -20
```

Expected: all `T1*`–`T3*` pass; failure count unchanged from baseline.

- [ ] **Step 5: Commit**

```bash
git add src/powerbi-crosstable-triage.ts src/powerbi.crosstable-triage.test.ts
git commit -m "$(cat <<'EOF'
powerbi: isNeverHostable — flag report-context-dependent measures

SELECTEDVALUE/ISFILTERED read report filter context; no static View can host
them. Matches a call, not a substring, so SELECTEDVALUE_FLAG is not a hit.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: `triageCrossTable` — the classifier

> ⚠️ **The guard rule this task implements below (`DUP_UNSAFE`, and Min/Max/CountDistinct exempted as duplication-safe) is UNSOUND and was REPLACED by Task 8.** Two independent adversarial passes broke it after this task shipped — a join also OMITS zero-match dimension rows, not just duplicates matched ones, so the idempotence exemption is wrong. **Implement Task 8's rule, not this section's.** This task's `Candidate`/`Triage` types, `reachableTables` composition, and overall shape are still correct and still used; only "the guard rule, stated precisely" below is superseded.

Composes Tasks 1–3 into the verdict. This is the piece whose correctness PR 2 will depend on.

**Files:**
- Modify: `src/powerbi-crosstable-triage.ts`
- Modify: `src/powerbi.crosstable-triage.test.ts`

**Interfaces:**
- Consumes: `DUP_UNSAFE`, `reachableTables`, `Rel`, `isNeverHostable`.
- Also produces (a Task 1 refactor, Step 0 below): `enclosingAggregateCall(formula, ref): { name: string; operand: string } | null`. `enclosingAggregate` becomes a thin wrapper over it and keeps its exact current behaviour — Task 1's committed `T1a`–`T1g` are the regression guard for that.
- Produces:

```ts
export type Candidate = {
  baseTable: string;                      // View is named "<baseTable> View"
  maxHop: number;                         // furthest reference, 0..maxDepth
  verdict: 'safe' | 'fanout-risk';
  unsafeRefs: string[];                   // refs that would double-count here
};
export type Triage = {
  metric: string;
  homeTable: string;
  refs: string[];
  neverHostable: boolean;
  candidates: Candidate[];                // safe ones first, then by maxHop, then name
  reachability: 'one' | 'many' | 'none';  // counts SAFE candidates only
};
export function triageCrossTable(args: {
  metricName: string; sigmaFormula: string; rawDax: string; homeTable: string;
  refs: string[]; columnOwners: Record<string, string[]>; relationships: Rel[];
  maxDepth?: number;                      // default 2
}): Triage;
```

### The guard rule, stated precisely

A reference `R` is **duplication-unsafe** on candidate View `V` when all three hold:

1. `R` resolves across ≥ 1 join hop from `V`'s base, **and**
2. the aggregate enclosing `R` is in `DUP_UNSAFE`, **and**
3. **that aggregate's operand references no hop-0 (base-table) column.**

Condition 3 is what distinguishes intent. `Sum([LIST_PRICE])` aggregates a dimension column *alone* — it means "total the price list", a dim-grain question, and on the fact's View it double-counts. But `Sum(If([AGENT_NAME] = "X", [AMOUNT], 0))` and `Sum([QTY] * [LIST_PRICE])` both aggregate an operand containing a base-table column: they are fact-grain by construction, each fact row contributes once, and the result is correct. A predicate reference is just the degenerate case of this — it never appears alone in the operand.

This is why Step 0 exists: deciding condition 3 needs the aggregate's **operand text**, which `enclosingAggregate` discards.

- [ ] **Step 0: Refactor `enclosingAggregate` to expose the operand — without changing its behaviour**

Task 1's `enclosingAggregate` already walks outward to find the enclosing aggregate but returns only its name. Extract that walk into `enclosingAggregateCall`, which additionally scans forward to the matching close paren and returns the operand text. Re-implement `enclosingAggregate` as a one-line wrapper.

Do NOT duplicate the walk — one implementation, one wrapper. Task 1's `T1a`–`T1g` must stay green with no edits; they are the proof the refactor preserved behaviour.

In `src/powerbi-crosstable-triage.ts`, replace the existing `enclosingAggregate` with:

```ts
/**
 * The aggregate call enclosing `[ref]`: its name and the raw text of its operand.
 * Walks outward past non-aggregate wrappers (If, Coalesce, arithmetic grouping),
 * then scans forward to the matching close paren to capture the operand.
 */
export function enclosingAggregateCall(
  formula: string, ref: string,
): { name: string; operand: string } | null {
  const f = String(formula);
  const idx = f.indexOf(`[${ref}]`);
  if (idx < 0) return null;
  let depth = 0;
  for (let i = idx - 1; i >= 0; i--) {
    const ch = f[i];
    if (ch === ')') { depth++; continue; }
    if (ch !== '(') continue;
    if (depth > 0) { depth--; continue; }
    const m = f.slice(0, i).match(/([A-Za-z_]\w*)\s*$/);
    if (m && AGGREGATES.has(m[1])) {
      let d = 0;
      for (let j = i; j < f.length; j++) {
        if (f[j] === '(') d++;
        else if (f[j] === ')' && --d === 0) return { name: m[1], operand: f.slice(i + 1, j) };
      }
      return { name: m[1], operand: f.slice(i + 1) };   // unbalanced input — degrade, don't throw
    }
  }
  return null;
}

/** The nearest AGGREGATE function enclosing `[ref]`, or null if not aggregated. */
export function enclosingAggregate(formula: string, ref: string): string | null {
  return enclosingAggregateCall(formula, ref)?.name ?? null;
}
```

Add one test for the new export, alongside the `T1*` block:

```ts
test('T1h enclosingAggregateCall returns the aggregate operand text', () => {
  assert.deepEqual(enclosingAggregateCall('Sum([QTY] * [PRICE])', 'QTY'),
    { name: 'Sum', operand: '[QTY] * [PRICE]' });
  assert.equal(enclosingAggregateCall('[QTY]', 'QTY'), null);
  // the operand is the ENCLOSING aggregate's, not the whole formula
  assert.equal(enclosingAggregateCall('Sum([A]) - Avg([B])', 'B')!.operand, '[B]');
});
```

Import `enclosingAggregateCall` in the test file and run:

```bash
node --import tsx/esm --test src/powerbi.crosstable-triage.test.ts 2>&1 | tail -8
```

Expected: `T1a`–`T1g` still pass **unchanged** (the behaviour-preservation proof) and `T1h` passes.

- [ ] **Step 1: Write the failing test**

Append to the test file:

```ts
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

test('T4c Max across a hop is SAFE — idempotent under duplication', () => {
  const t = triageCrossTable(base({
    sigmaFormula: 'Max([LIST_PRICE])', refs: ['LIST_PRICE'], homeTable: 'SALES_FACT',
  }));
  assert.equal(t.candidates.find((x) => x.baseTable === 'SALES_FACT')!.verdict, 'safe');
});

test('T4d an UNAGGREGATED cross-hop ref is SAFE — duplication cannot change a predicate', () => {
  const t = triageCrossTable(base({
    sigmaFormula: 'Sum(If([AGENT_NAME] = "X", [AMOUNT], 0))',
    refs: ['AGENT_NAME', 'AMOUNT'], homeTable: 'SALES_FACT',
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.verdict, 'safe');
  assert.equal(c.maxHop, 1);
});

test('T4e a two-hop reference is found at hop 2', () => {
  const t = triageCrossTable(base({
    sigmaFormula: 'Sum(If([REGION] = "W", [AMOUNT], 0))',
    refs: ['REGION', 'AMOUNT'], homeTable: 'SALES_FACT',
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.maxHop, 2);
  assert.equal(c.verdict, 'safe');
});

test('T4f depth 1 cannot reach a two-hop reference', () => {
  const t = triageCrossTable(base({
    sigmaFormula: 'Sum(If([REGION] = "W", [AMOUNT], 0))',
    refs: ['REGION', 'AMOUNT'], homeTable: 'SALES_FACT', maxDepth: 1,
  }));
  assert.equal(t.reachability, 'none');
  assert.deepEqual(t.candidates, []);
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
  const rels = [{ from: 'F1', to: 'D' }, { from: 'F2', to: 'D' }];
  const t = triageCrossTable(base({
    sigmaFormula: 'Max([DCOL])', refs: ['DCOL'], homeTable: 'D',
    columnOwners: { DCOL: ['D'] }, relationships: rels,
  }));
  assert.equal(t.reachability, 'many');
  assert.deepEqual(t.candidates.map((c) => c.baseTable), ['F1', 'F2']);
});

test('T4j a MIXED operand is SAFE — a base-table column makes it fact-grain', () => {
  // Sum([QTY] * [LIST_PRICE]): QTY is one value per fact row, so each fact row
  // contributes exactly once. The dim value repeating is intended here, not a bug.
  const t = triageCrossTable(base({
    sigmaFormula: 'Sum([QTY] * [LIST_PRICE])', refs: ['QTY', 'LIST_PRICE'],
    homeTable: 'SALES_FACT',
  }));
  const c = t.candidates.find((x) => x.baseTable === 'SALES_FACT')!;
  assert.equal(c.verdict, 'safe');
  assert.deepEqual(c.unsafeRefs, []);
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
```

- [ ] **Step 2: Run and confirm it fails**

```bash
node --import tsx/esm --test src/powerbi.crosstable-triage.test.ts 2>&1 | tail -10
```

Expected: FAIL — `triageCrossTable is not a function`. `T1*`–`T3*` still pass.

- [ ] **Step 3: Write the implementation**

Append to `src/powerbi-crosstable-triage.ts`:

```ts
export type Candidate = {
  baseTable: string;
  maxHop: number;
  verdict: 'safe' | 'fanout-risk';
  unsafeRefs: string[];
};

export type Triage = {
  metric: string;
  homeTable: string;
  refs: string[];
  neverHostable: boolean;
  candidates: Candidate[];
  reachability: 'one' | 'many' | 'none';
};

/**
 * Classify a measure the converter is about to drop as cross-table.
 *
 * A candidate is a table B that (a) has at least one outgoing relationship —
 * only such tables get a "<B> View" element — and (b) can reach the owning
 * table of EVERY reference within maxDepth hops.
 *
 * The fan-out verdict is per-REFERENCE, not per-measure: the hazard is a
 * dimension VALUE repeating once per fact row, so it applies only to references
 * that are both (i) reached across >= 1 hop and (ii) wrapped in a
 * duplication-sensitive aggregate. A base-table reference is safe under any
 * aggregate even when the measure was homed elsewhere — which is exactly the
 * dominant star-schema idiom.
 */
export function triageCrossTable(args: {
  metricName: string; sigmaFormula: string; rawDax: string; homeTable: string;
  refs: string[]; columnOwners: Record<string, string[]>; relationships: Rel[];
  maxDepth?: number;
}): Triage {
  const { metricName, sigmaFormula, rawDax, homeTable, refs, columnOwners, relationships } = args;
  const maxDepth = args.maxDepth ?? 2;
  const shell: Triage = {
    metric: metricName, homeTable, refs, neverHostable: false,
    candidates: [], reachability: 'none',
  };

  if (isNeverHostable(rawDax)) return { ...shell, neverHostable: true };

  // Only tables with an outgoing relationship get a View element built for them.
  const bases = [...new Set(relationships.map((r) => r.from))].sort();
  const candidates: Candidate[] = [];

  for (const b of bases) {
    const reach = reachableTables(b, relationships, maxDepth);
    let maxHop = 0;
    const unsafeRefs: string[] = [];
    let covered = true;

    for (const ref of refs) {
      const owners = columnOwners[ref] || [];
      // shortest hop to ANY table owning this column name
      let hop = Infinity;
      for (const o of owners) {
        const h = reach.get(o);
        if (h !== undefined && h < hop) hop = h;
      }
      if (hop === Infinity) { covered = false; break; }
      if (hop > maxHop) maxHop = hop;

      // Duplication-unsafe only when the ref is across a hop, its aggregate is
      // duplication-sensitive, AND that aggregate's operand contains no hop-0
      // column. A base-table column in the same operand pins the aggregate to
      // the base's row count, which is what the measure meant all along.
      if (hop < 1) continue;
      const call = enclosingAggregateCall(sigmaFormula, ref);
      if (!call || !DUP_UNSAFE.has(call.name)) continue;
      const operandRefs = (call.operand.match(/\[([^\]]+)\]/g) || []).map((s) => s.slice(1, -1));
      const pinnedToBase = operandRefs.some((o) =>
        (columnOwners[o] || []).some((t) => reach.get(t) === 0));
      if (!pinnedToBase) unsafeRefs.push(ref);
    }
    if (!covered) continue;
    candidates.push({
      baseTable: b, maxHop,
      verdict: unsafeRefs.length ? 'fanout-risk' : 'safe',
      unsafeRefs,
    });
  }

  // safe first, then nearest, then alphabetical — deterministic ordering
  candidates.sort((x, y) =>
    (x.verdict === y.verdict ? 0 : x.verdict === 'safe' ? -1 : 1)
    || x.maxHop - y.maxHop
    || x.baseTable.localeCompare(y.baseTable));

  const safeCount = candidates.filter((c) => c.verdict === 'safe').length;
  return {
    ...shell,
    candidates,
    reachability: safeCount === 0 ? 'none' : safeCount === 1 ? 'one' : 'many',
  };
}
```

- [ ] **Step 4: Run and confirm it passes**

```bash
npm test 2>&1 | tail -20
```

Expected: all `T1*`–`T4*` pass; failure count unchanged from baseline.

- [ ] **Step 5: Prove the guard can actually fail — both halves of it**

A guard that cannot fail is not a guard. Run two independent mutations, and paste the real failing output for each into your report.

**Mutation A — disable the hop check.** Change `if (hop < 1) continue;` to `if (hop < 99) continue;`.
Expected: `T4b` and `T4k` FAIL (both would report `safe` for a genuinely double-counting `Sum`). Revert.

**Mutation B — disable the operand check.** Change `if (!pinnedToBase)` to `if (true)`.
Expected: `T4d`, `T4e` and `T4j` FAIL (a mixed or predicate operand would be wrongly called fan-out risk). Revert.

Both mutations must produce the failures listed. If either mutation leaves the suite green, the corresponding half of the guard is untested — say so in your report rather than proceeding. Re-run after reverting and confirm green.

- [ ] **Step 6: Commit**

```bash
git add src/powerbi-crosstable-triage.ts src/powerbi.crosstable-triage.test.ts
git commit -m "$(cat <<'EOF'
powerbi: triageCrossTable — classify a dropped cross-table measure

Per-REFERENCE fan-out verdict: the hazard is a dim value repeating per fact
row, so it applies only to refs reached across a hop AND wrapped in a
duplication-sensitive aggregate. A base-table ref is safe under any aggregate
even when the measure was homed elsewhere — the dominant star-schema idiom.
Ambiguous coverage is reported, never silently resolved.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Wire into the converter

**Files:**
- Modify: `src/powerbi.ts` — pre-pass near line 2177, warning at line 2620
- Modify: `src/powerbi.crosstable-triage.test.ts`

**Interfaces:**
- Consumes: `triageCrossTable`, `Triage` from Task 4.
- Produces: `describeTriage(t: Triage): string` — the human-readable suffix appended to the existing warning.

- [ ] **Step 1: Write the failing test — behaviour must NOT change**

Append to the test file:

```ts
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

test('T5b PR 1 changes NO emitted output — only warning text', () => {
  // The guarantee that makes this PR safe to merge ahead of the attach logic.
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
    neverHostable: false, candidates: [], reachability: 'none', ...over });
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
});
```

- [ ] **Step 2: Run and confirm it fails**

```bash
node --import tsx/esm --test src/powerbi.crosstable-triage.test.ts 2>&1 | tail -10
```

Expected: `T5a` and `T5c` FAIL (`describeTriage is not a function`, no `TRIAGE:` in the warning). `T5b` should already PASS — it asserts the invariant you must not break.

- [ ] **Step 3: Add `describeTriage`**

Append to `src/powerbi-crosstable-triage.ts`:

```ts
/** Render a Triage as the operator-facing suffix appended to the drop warning. */
export function describeTriage(t: Triage): string {
  // Task 8 added a fail-closed gate: a formula we cannot parse confidently never
  // gets a `safe` verdict, and every covered candidate is marked with this
  // sentinel instead. Say so distinctly — "fan-out risk" would mislead an
  // operator into looking for a grain problem that isn't there.
  if (t.candidates.length && t.candidates.every((c) => c.unsafeRefs.includes('malformed-formula'))) {
    return 'TRIAGE: the translated formula could not be parsed confidently (unclosed quote or mis-nested brackets) — not classified; inspect the formula by hand.';
  }
  if (t.neverHostable) {
    return 'TRIAGE: report-context-dependent (SELECTEDVALUE/ISFILTERED) — no static View can host it; rebuild at the visual\'s grain.';
  }
  if (!t.candidates.length) {
    return `TRIAGE: no View covers it within 2 join hops (references: ${t.refs.join(', ')}).`;
  }
  const hop = (c: Candidate) => `${c.maxHop} hop${c.maxHop === 1 ? '' : 's'}`;
  if (t.reachability === 'many') {
    const names = t.candidates.filter((c) => c.verdict === 'safe').map((c) => `"${c.baseTable} View"`);
    return `TRIAGE: ambiguous — ${names.length} Views cover it (${names.join(', ')}); needs a human choice.`;
  }
  const safe = t.candidates.find((c) => c.verdict === 'safe');
  if (safe) return `TRIAGE: hostable on "${safe.baseTable} View" (${hop(safe)}, fan-out SAFE).`;
  const risky = t.candidates[0];
  return `TRIAGE: "${risky.baseTable} View" (${hop(risky)}) covers it but FAN-OUT RISK — ` +
    `[${risky.unsafeRefs.join(', ')}] would double-count across the join; rebuild at the visual's grain.`;
}
```

- [ ] **Step 4: Add the `columnOwners` pre-pass to `powerbi.ts`**

Immediately after the `allPbiToSigmaNames` declaration at `src/powerbi.ts:2177`, insert:

```ts
  // ── Cross-table TRIAGE support ────────────────────────────────────────────
  // `tableColMap` is filled INSIDE the table loop below, so at the moment table k
  // drops a cross-table measure, tables k+1..N are not in it yet. Triage needs a
  // whole-model view, so build one up front from the RAW model.
  // Indexed by BOTH raw and display name: at the drop site refs have already been
  // remapped to display names, but a column whose display name we derive slightly
  // differently still resolves via its raw name. PR 1 only produces a MESSAGE, so an
  // imperfect derivation degrades wording, never output. PR 2 must not rely on this.
  const triageColumnOwners: Record<string, string[]> = {};
  const _own = (key: string, table: string) => {
    if (!key) return;
    if (!triageColumnOwners[key]) triageColumnOwners[key] = [];
    if (!triageColumnOwners[key].includes(table)) triageColumnOwners[key].push(table);
  };
  for (const _t of (model.tables || [])) {
    if (_t.name?.startsWith('LocalDateTable_') || _t.name?.startsWith('DateTableTemplate_')) continue;
    for (const _c of (_t.columns || [])) {
      _own(_c.name, _t.name);
      _own(sigmaDisplayName(String(_c.sourceColumn || _c.name || '').replace(/^\[|\]$/g, '')), _t.name);
    }
  }
  const triageRels: Rel[] = (model.relationships || [])
    .filter((r: any) => r.fromTable && r.toTable)
    .map((r: any) => ({ from: r.fromTable, to: r.toTable }));
```

Add to the import block at the top of `src/powerbi.ts`:

```ts
import { triageCrossTable, describeTriage, type Rel } from './powerbi-crosstable-triage.js';
```

- [ ] **Step 5: Enrich the warning at the drop site**

In `src/powerbi.ts`, replace the single `warnings.push(...)` at line 2620 with:

```ts
          if (bad) {
            const _rawDax = String(
              ((t.measures || []).find((mm: any) => mm.name === metrics[i].name) || {}).expression || ''
            );
            const _triage = triageCrossTable({
              metricName: metrics[i].name,
              sigmaFormula: String(metrics[i].formula),
              rawDax: Array.isArray(_rawDax) ? _rawDax.join('\n') : _rawDax,
              homeTable: tableName,
              refs: [...new Set(refs)],
              columnOwners: triageColumnOwners,
              relationships: triageRels,
            });
            warnings.push(`⚠ "${metrics[i].name}": references "[${bad}]" which is not a column or metric on this element (cross-table measure) — dropped; recreate in a workbook element at the visual's grain (the joined "View" element has the dim columns). ${describeTriage(_triage)}`);
            metrics.splice(i, 1);
          }
```

- [ ] **Step 6: Run everything and confirm behaviour is unchanged**

```bash
npm test 2>&1 | tail -25
```

Expected: `T1*`–`T5*` all pass. **The failure count must equal the Task 1 Step 1 baseline exactly.** In particular `src/powerbi.dax-fidelity.test.ts` and `src/powerbi.crossfilter-case.test.ts` must be unaffected — if either regressed, the warning-text change broke a test that asserts on warning strings. Fix by making the assertion match the new suffix, not by reverting the suffix.

- [ ] **Step 7: Verify no output changed, against `main`**

Write the dump script ONCE, to `/tmp/dump-models.mjs`, and run the same file against both
revisions. `sigmaShortId()` may be call-order dependent, so compare a version with all `id`
fields stripped — the property under test is structure, not identity.

```js
// /tmp/dump-models.mjs   — usage: node --import tsx/esm /tmp/dump-models.mjs <outPrefix>
import { convertPowerBIToSigma } from './src/powerbi.ts';
import { readFileSync, writeFileSync } from 'fs';
const O = { connectionId: '11111111-2222-3333-4444-555555555555', database: 'DB', schema: 'SCH' };
const stripIds = (v) => JSON.parse(JSON.stringify(v), (k, val) => (k === 'id' ? undefined : val));
// R1-R4: map each report's own model file to its R-label locally — the mapping
// itself is not committed (see §10's naming constraint). `<artifacts>` is
// wherever the four reference `.bim` files live on the machine running this.
for (const report of ['R1', 'R2', 'R3', 'R4']) {
  const m = JSON.parse(readFileSync(`<artifacts>/gt/model_${report}.bim`, 'utf8'));
  const o = convertPowerBIToSigma(m, O);
  writeFileSync(`${process.argv[2]}-${report}.json`,
    JSON.stringify(stripIds({ model: o.model, stats: o.stats }), null, 1));
}
```

```bash
node --import tsx/esm /tmp/dump-models.mjs /tmp/triage      # this branch
git stash
node --import tsx/esm /tmp/dump-models.mjs /tmp/mainref     # main's behaviour
git stash pop
for report in R1 R2 R3 R4; do
  diff -q /tmp/mainref-$report.json /tmp/triage-$report.json >/dev/null \
    && echo "$report identical" || echo "$report DIFFERS — investigate"
done
```

Expected: all four report `identical`. If any differs, PR 1 has changed emitted output and must be fixed before commit — that is the whole safety property of shipping triage ahead of attach. Report the diff in your report rather than working around it.

- [ ] **Step 8: Commit**

```bash
git add src/powerbi.ts src/powerbi-crosstable-triage.ts src/powerbi.crosstable-triage.test.ts
git commit -m "$(cat <<'EOF'
powerbi: triage every cross-table measure drop with host, hop, and fan-out verdict

The drop warning now names the View that could host the measure, how many join
hops away it sits, and whether aggregating across those hops would double-count.
Emitted output is byte-identical to main on all four reference models — this PR
changes only what operators are told, not what is built.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Measure the buckets on R1–R4

PR 1's exit condition. Produces the numbers PR 2 gets designed against, replacing the handoff's estimates.

**Files:**
- Create: `<scratchpad>/measure-triage.mjs` — **NOT committed.** It reads `<artifacts>/`, which is derived from customer files. Keep it out of the repo.

- [ ] **Step 1: Write the harness**

Create `<scratchpad>/measure-triage.mjs`:

```js
import { convertPowerBIToSigma } from '<repo>/src/powerbi.ts';
import { readFileSync } from 'fs';
const O = { connectionId: '11111111-2222-3333-4444-555555555555', database: 'DB', schema: 'SCH' };
// Map each report's own model file to its R-label here — the mapping is LOCAL
// to the machine running this harness and is never committed (see §10): e.g.
// `const LABEL = { <report-A-code>: 'R1', <report-B-code>: 'R2', ... };`
const LABEL = {};   // fill in locally before running
const tally = {};
for (const [f, label] of Object.entries(LABEL)) {
  const m = JSON.parse(readFileSync(`<artifacts>/gt/model_${f}.bim`, 'utf8'));
  const w = convertPowerBIToSigma(m, O).warnings.filter((x) => x.includes('cross-table measure'));
  const b = { total: w.length, safe: 0, risky: 0, ambiguous: 0, never: 0, uncovered: 0, hop1: 0, hop2: 0 };
  for (const x of w) {
    if (/report-context-dependent/.test(x)) b.never++;
    else if (/ambiguous/.test(x)) b.ambiguous++;
    else if (/FAN-OUT RISK/.test(x)) b.risky++;
    else if (/fan-out SAFE/.test(x)) b.safe++;
    else b.uncovered++;
    if (/\(1 hop/.test(x)) b.hop1++;
    if (/\(2 hops/.test(x)) b.hop2++;
  }
  tally[label] = b;
}
console.table(tally);
const sum = (k) => Object.values(tally).reduce((n, b) => n + b[k], 0);
console.log('TOTAL', Object.fromEntries(['total','safe','risky','ambiguous','never','uncovered','hop1','hop2'].map((k) => [k, sum(k)])));
```

- [ ] **Step 2: Run it**

```bash
node --import tsx/esm <scratchpad>/measure-triage.mjs
```

- [ ] **Step 3: Sanity-check against the handoff's independently-measured figures**

The handoff measured, by a different method: **110** total cross-table drops, **24** never-hostable (`SELECTEDVALUE`/`ISFILTERED`), **36** needing 2+ hops, **12** ambiguous.

- `total` should be **110**. A different number means the classifier is firing on the wrong warnings — investigate before trusting anything else.
- `never` should be **24**, or close. A large gap means `isNeverHostable`'s token set is wrong.
- `safe` is **expected to exceed 23** — that is this design's central hypothesis (per-reference guard beats the home==base rule). If it comes out at or below 23, the hypothesis is wrong and PR 2's guard should revert to the handoff's rule. **Record the number either way; do not rationalise it.**

- [ ] **Step 4: Write the numbers into the spec and commit**

Add a `## 11. Measured buckets (PR 1 result)` section to
`docs/superpowers/specs/2026-07-30-pbi-cross-table-measure-rehoming-design.md`
containing the per-model table, the totals, and one sentence stating whether the
per-reference hypothesis held.

```bash
git add docs/superpowers/specs/2026-07-30-pbi-cross-table-measure-rehoming-design.md
git commit -m "$(cat <<'EOF'
docs: record measured cross-table triage buckets on R1-R4

Replaces the handoff's estimated split with the classifier's measured one, and
states whether the per-reference fan-out hypothesis held.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 5: Open PR 1**

```bash
git push
gh pr create --title "powerbi: triage cross-table measure drops (host, hop, fan-out verdict)" \
  --body "$(cat <<'EOF'
Turns opaque `cross-table measure — dropped` warnings into a triaged list: which
`<T> View` could host the measure, how many join hops away, and whether
aggregating across those hops would double-count.

**Changes no emitted output** — verified byte-identical model+stats against `main`
on all four reference models. Only warning text differs.

Phase 1 of the design in `docs/superpowers/specs/2026-07-30-pbi-cross-table-measure-rehoming-design.md`.
Phase 2 (deferred drop + attach onto Views) rides on this classifier once a spike
resolves whether Sigma resolves 4-segment cross-element refs.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Task 7: SPIKE — does Sigma resolve a 4-segment cross-element ref?

**Independent of Tasks 1–6; run it in parallel.** Its answer determines whether PR 2's depth-2 support is a longer bracket path or a View-sourcing-a-View. Nothing in PR 1 depends on it.

**Files:** none committed. Scratchpad only.

- [ ] **Step 1: Get a Sigma token**

```bash
eval "$(python3 scripts/get_token.py --print-export)"
```

Never source `settings.json`.

- [ ] **Step 2: Build a minimal 3-table snowflake data model by hand**

`SALES_FACT → AGENT_DIM → REGION_DIM` over real `CSA.TJ` tables on connection
`cb2f5180-641f-47bd-8efa-da9d590d855a`. Add to the `SALES_FACT View` element one
column whose formula is a **4-segment** ref:

```
[SALES_FACT/AGENT_DIM/REGION_DIM/REGION_NAME]
```

- [ ] **Step 3: POST it, then GET it back and read the column's type**

```bash
curl -sS -X POST "$SIGMA_BASE_URL/v2/workbooks/spec" -H "Authorization: Bearer $SIGMA_API_TOKEN" ...
curl -sS "$SIGMA_BASE_URL/v2/datamodels/<id>/spec" -H "Authorization: Bearer $SIGMA_API_TOKEN" | jq '..|.columns?'
```

**The GET-back is the whole point.** A POST that returns 200 proves nothing — a
cross-element ref Sigma cannot resolve compiles to column type `error` and still
saves. Read the type.

- [ ] **Step 4: If 4-segment fails, test the fallback**

Build `SALES_FACT View 2` with `source: { kind: 'table', elementId: <SALES_FACT View id> }` and a
3-segment ref hopping from there. GET back, read the type again.

- [ ] **Step 5: Record the answer**

Append `## 12. Spike result — 4-segment cross-element refs` to the spec: the exact
formula tested, the column type from the readback, and which construction PR 2
must use. One paragraph. Commit to the branch.

---

## Self-Review

**Spec coverage.** Spec §4.1 (ledger), §4.4 (demand-driven depth) and §7's live E2E are deliberately absent — they belong to PR 2/PR 3, which spec §6 explicitly defers. §4.2 (ref resolver) → Task 2. §4.3 (guard) → Tasks 1 + 4. §2.3 (the unguarded measures-only path) is **not** covered here: it is an attach-path concern with nothing to fix while triage changes no output. It must appear in PR 2's plan — noted so it is not lost.

**Placeholders.** None. Every code step carries runnable code. Task 5 Step 7's second `npx tsx -e` is abbreviated as `<same script, ...>` — the implementer repeats the preceding block with the output path changed.

**Type consistency.** `Rel` is defined in Task 2 and imported by name in Task 5. `Candidate`/`Triage` are defined in Task 4 and consumed by `describeTriage` in Task 5. `DUP_UNSAFE` is exported in Task 1 and used in Task 4. `enclosingAggregate`, `reachableTables`, `isNeverHostable`, `triageCrossTable`, `describeTriage` are spelled identically at every site.

---

## Task 8: Replace the guard with the SOUND base-grain rule

Two adversarial passes (DAX filter-context lens and relational-grain lens, run independently) both returned **UNSOUND** on the guard Task 4 shipped, converging on the same defect class. A third pass parsed all 193 measures in R1–R4 and measured what the correction costs. This task replaces the rule.

### Why the shipped guard is wrong

**1. `Min` / `Max` / `CountDistinct` are not unconditionally safe.** They are idempotent under row *duplication*, which is why they were exempted. But a join also **omits**: a dimension row matching zero base rows vanishes from the View. Departments with budgets 100 / 500 / 200 and nobody employed in the 500 department → `Max(budget)` is **500** at dimension grain and **200** on the fact's View.

**2. A comparison-position exemption cannot save `Count`-like aggregates.** `CountIf([Tier] = "VIP")`: the dimension reference is *only* a comparison operand, but `CountIf`'s summand is an implicit "1 per row", and rows are what duplication corrupts. One VIP customer with two sales rows → Power BI **1**, View **2**.

Confirmed *not* broken: `SumIf([Amount], [Tier] = "VIP")` with a hop-0 `Amount` is genuinely safe (150 = 150). The predicate argument does not set grain.

### The sound rule

Grain is set by **what an aggregate ranges over**. On a View every aggregate ranges over base rows, so re-homing is faithful only when the original did too.

> Define an aggregate call's **summand** — the quantity whose grain matters:
> - `Sum` / `Avg` / `Min` / `Max` / `StdDev` / `Var` / `Median` / `Count` / `CountDistinct`: the whole operand.
> - `Percentile(e, p)` and `SumIf(e, pred)`: the **first argument only**. A predicate does not set grain.
> - `CountIf(pred)`: **no summand** — the aggregated quantity is an implicit row.
>
> An aggregate call is **base-grain** on View `V` iff its summand references **at least one hop-0 column** and **no hop-≥1 column**. A call with no summand is never base-grain.
>
> A measure is re-homable onto `V` iff every aggregate call in its formula is base-grain (and every reference resolves on `V`). A formula containing no aggregate call at all changes no grain and is base-grain by default.

`DUP_UNSAFE` and comparison detection are both **deleted** — the rule needs neither.

### What the correction costs — measured, not estimated

> ⚠️ **STALE, corrected below.** This table's per-shape counts (69 safe, 111 strict
> cross-table) predate the full end-to-end classifier run. The authoritative,
> final measured numbers — **13 safe, 108 strict cross-table** — are in the
> spec's `## 11. Measured buckets (PR 1 result)`, produced by actually running
> `triageCrossTable` over R1–R4 through `convertPowerBIToSigma`, not by a
> static per-shape DAX scan. The gap's root cause, per that section: a "bare
> foreign aggregate" is frequently wrapped in `CALCULATE` with a filter
> predicate that references a SECOND foreign table — often one with no
> relationship path to the measure's home table at all — so every reference in
> the formula (not just the aggregate's own summand) must still resolve within
> `maxDepth` hops for the measure to attach anywhere. A per-shape count of the
> aggregate call alone misses that and overcounts "safe."

Across R1–R4 (193 measures, ~111 strict cross-table by an earlier per-shape count — **108** by the final end-to-end measurement, zero parse errors):

| shape | count | under the sound rule |
|---|---|---|
| bare foreign aggregate, `SUM(OTHER[col])` | ~69 by the earlier per-shape count — **13** by the final end-to-end measurement (see spec §11) | **safe** — the recoverable set, preserved |
| foreign value gated by a predicate, `CALCULATE(MIN(DIM[X]), DIM[Y]=1)` | 47–57 | correctly **unsafe** |
| `Min`/`Max`/`CountDistinct` of a foreign column | 15 | now **unsafe** (was falsely safe) |
| mixed row expression, `Sum([QTY] * [PRICE (DIM)])` | **0** | conservatism costs nothing |
| same column under two aggregates | **0** | hypothetical in this corpus |

So the correction removes false "safe" verdicts and loses no genuinely recoverable measure — conservatism is free here — but the SIZE of the recoverable set is 13, not 69; see spec §11 for the final number and why the earlier per-shape count overstated it.

**Files:**
- Modify: `src/powerbi-crosstable-triage.ts`
- Modify: `src/powerbi.crosstable-triage.test.ts`

**Interfaces:**
- Produces: `splitTopLevelArgs(s: string): string[]`, `aggregateSummand(name: string, operand: string): string | null` (null = implicit row), `enumerateAggregateCalls(formula: string): Array<{ name: string; operand: string }>`.
- Removes: `DUP_UNSAFE` (and its uses). Keep `enclosingAggregateCall` / `enclosingAggregateCalls` / `enclosingAggregate` — `T1a`–`T1i` cover them and they stay useful, but the guard no longer consumes them.

- [ ] **Step 1: Write the failing tests for the three helpers**

```ts
import { splitTopLevelArgs, aggregateSummand, enumerateAggregateCalls } from './powerbi-crosstable-triage.js';

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
```

- [ ] **Step 2: Run and confirm they fail**

```bash
node --import tsx/esm --test src/powerbi.crosstable-triage.test.ts 2>&1 | tail -8
```

Expected: FAIL — the three functions do not exist.

- [ ] **Step 3: Implement the three helpers**

```ts
/** Split an argument list on TOP-LEVEL commas only (parens and brackets nest). */
export function splitTopLevelArgs(s: string): string[] {
  const out: string[] = [];
  let depth = 0, start = 0;
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (c === '(' || c === '[') depth++;
    else if (c === ')' || c === ']') depth--;
    else if (c === ',' && depth === 0) { out.push(s.slice(start, i)); start = i + 1; }
  }
  out.push(s.slice(start));
  return out;
}

/**
 * The aggregated QUANTITY of an aggregate call — the part whose grain matters.
 * A predicate argument does NOT set grain, so SumIf/Percentile contribute only
 * their first argument. CountIf has no summand at all: what it aggregates is an
 * implicit row, and which rows exist is exactly what the join changes.
 */
export function aggregateSummand(name: string, operand: string): string | null {
  if (name === 'CountIf') return null;
  if (name === 'SumIf' || name === 'Percentile') return splitTopLevelArgs(operand)[0] ?? '';
  return operand;
}

/** Every aggregate call in a formula, with its balanced operand text. */
export function enumerateAggregateCalls(formula: string): Array<{ name: string; operand: string }> {
  const f = String(formula || '');
  const out: Array<{ name: string; operand: string }> = [];
  const re = /([A-Za-z_]\w*)\s*\(/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(f))) {
    if (!AGGREGATES.has(m[1])) continue;
    // a preceding word char means this is a longer identifier (MySum), not our call
    const before = m.index > 0 ? f[m.index - 1] : '';
    if (/[A-Za-z0-9_]/.test(before)) continue;
    const open = m.index + m[0].length - 1;
    let d = 0;
    for (let j = open; j < f.length; j++) {
      if (f[j] === '(') d++;
      else if (f[j] === ')' && --d === 0) { out.push({ name: m[1], operand: f.slice(open + 1, j) }); break; }
    }
  }
  return out;
}
```

- [ ] **Step 4: Replace the guard inside `triageCrossTable`**

Delete the `DUP_UNSAFE` export and the whole per-reference guard block. The per-candidate loop becomes: resolve every reference's hop for coverage, then judge grain by aggregate call.

```ts
    // coverage: every reference must resolve somewhere on this View
    const hopOf = (ref: string): number => {
      let hop = Infinity;
      for (const o of (columnOwners[ref] || [])) {
        const h = reach.get(o);
        if (h !== undefined && h < hop) hop = h;
      }
      return hop;
    };
    let covered = true, maxHop = 0;
    for (const ref of refs) {
      const hop = hopOf(ref);
      if (hop === Infinity) { covered = false; break; }
      if (hop > maxHop) maxHop = hop;
    }
    if (!covered) continue;

    // grain: every aggregate call must range over the BASE's rows. Its summand
    // must contain a hop-0 column (pinning it to base grain) and no hop->=1
    // column (which would be duplicated by the join, or omitted by it).
    const unsafeRefs: string[] = [];
    for (const call of enumerateAggregateCalls(sigmaFormula)) {
      const summand = aggregateSummand(call.name, call.operand);
      const sRefs = summand === null
        ? []
        : (summand.match(/\[([^\]]+)\]/g) || []).map((s) => s.slice(1, -1));
      const cross = sRefs.filter((r) => hopOf(r) >= 1);
      const hasBase = sRefs.some((r) => hopOf(r) === 0);
      if (cross.length) unsafeRefs.push(...cross);
      else if (!hasBase) unsafeRefs.push(`${call.name}()`);   // implicit-row summand
    }
    candidates.push({
      baseTable: b, maxHop,
      verdict: unsafeRefs.length ? 'fanout-risk' : 'safe',
      unsafeRefs: [...new Set(unsafeRefs)],
    });
```

- [ ] **Step 5: Update the tests the rule change invalidates, and add the new cases**

These existing assertions change because the RULE changed, not because the code broke. Change each and say so in your report:

- **T4c** `Max([LIST_PRICE])` on `SALES_FACT` — was `safe`, now **`fanout-risk`**. This is the omission fix; retitle it to say so.
- **T4d**, **T4e** `Sum(If([AGENT_NAME]="X",[AMOUNT],0))` / `[REGION]` — were `safe`, now **`fanout-risk`** (conservative: a cross-hop ref anywhere in the summand). Retitle.
- **T4j** `Sum([QTY] * [LIST_PRICE])` — was `safe`, now **`fanout-risk`**. Retitle; note the shape has 0 occurrences in the reference corpus.
- **T4m** — was `safe`, now `fanout-risk` for the same reason. Retitle.
- **T4i** ambiguity fixture no longer produces two *safe* candidates (`Max` of a hop-1 column is now unsafe). Rewrite it so both candidates are genuinely safe: relationships `F1→D`, `F2→D`, formula `Sum([SHARED])`, `columnOwners: { SHARED: ['F1','F2'] }` — hop 0 from each base. Keep the assertions (`reachability === 'many'`, candidates `['F1','F2']`).
- **T4a, T4b, T4b2, T4f, T4g, T4h, T4k, T4l, T4n** should be unchanged. If any of them fails, stop and report — that is a real regression, not a rule change.

Add these:

```ts
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
```

- [ ] **Step 6: Run the suite**

```bash
npm test 2>&1 | grep -E "^# (tests|pass|fail|skipped)"
```

Expected: `# fail` still exactly **26**. `T1a`–`T1i` unchanged and green.

- [ ] **Step 7: Prove the new guard can fail — three mutations**

Each must produce the listed failures. If a mutation leaves the suite green, that part of the guard is untested — report it rather than proceeding.

- **A. Remove the cross-hop check:** change `if (cross.length)` to `if (false)`. Expected: T4b, T4c, T4d, T4j, T4k, T4f-unaffected — at minimum T4b and T4c must fail. Revert.
- **B. Remove the implicit-row check:** change `else if (!hasBase)` to `else if (false)`. Expected: **T8e must fail** (`CountIf` would be called safe). Revert.
- **C. Make `SumIf` use its whole operand:** change `aggregateSummand`'s `SumIf` branch to return `operand`. Expected: **T8d must fail**. Revert.

- [ ] **Step 8: Commit**

```bash
git add src/powerbi-crosstable-triage.ts src/powerbi.crosstable-triage.test.ts
git commit -m "$(cat <<'EOF'
powerbi: replace the fan-out guard with a sound base-grain rule

Two independent adversarial passes broke the previous guard. Min/Max/
CountDistinct were exempted as duplication-idempotent, but a join also OMITS
dimension rows matching no base row, so Max over a dim column reads too low.
And no comparison-position exemption can save CountIf, whose summand is an
implicit row.

Grain is set by what an aggregate ranges over: a call is base-grain iff its
summand holds a hop-0 column and no cross-hop column, with a predicate
argument contributing nothing and CountIf having no summand at all. Drops
DUP_UNSAFE and comparison detection entirely.

Measured on the four reference models: removes ~15 false "safe" verdicts,
loses no recoverable measure — the two shapes this rejects conservatively have
zero occurrences.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: Numeric ground-truth proof (REQUIRED before PR 1 merges)

Every hole found so far was found by a *fresh* reader, not by the previous reviewer. Analysis cannot show absence of counter-examples. This task settles the guard with arithmetic.

**Files:** none committed to the repo — harness lives in the scratchpad. Results are recorded in the spec.

- [ ] **Step 1: Build a snowflake fixture over real warehouse tables**

Using connection `cb2f5180-641f-47bd-8efa-da9d590d855a`, database `CSA`, schema `TJ`: create a data model with `FACT → DIM_A → DIM_B` (both joins many→one). The fixture must contain, in the data:
- at least one `DIM_A` row matching **zero** `FACT` rows (this is what exposes the omission bug), and
- at least one `DIM_A` row matching **several** `FACT` rows (the duplication bug).

If no `CSA.TJ` tables have that shape naturally, build the fixture with SQL elements over them rather than assuming — and record what you used.

- [ ] **Step 2: For each of these formulas, compute the value BOTH ways**

Native grain (aggregate over the dimension element directly) versus re-homed (the same aggregate on `FACT View`). Query both through the Sigma API and record the two numbers.

| formula | guard verdict | expectation |
|---|---|---|
| `Sum([FACT_AMOUNT])` | safe | numbers **match** |
| `Count([FACT_AMOUNT])` | safe | match |
| `SumIf([FACT_AMOUNT], [DIM_A_TIER] = "X")` | safe | match |
| `Sum([DIM_A_BUDGET])` | fanout-risk | numbers **differ** |
| `Max([DIM_A_BUDGET])` | fanout-risk | **differ** (omission) |
| `CountDistinct([DIM_A_KEY])` | fanout-risk | **differ** (omission) |
| `CountIf([DIM_A_TIER] = "X")` | fanout-risk | **differ** (duplication) |

- [ ] **Step 3: Assert the guard's verdicts against the arithmetic**

The guard passes only if **every `safe` verdict matches** and **at least the four `fanout-risk` rows genuinely differ**. A `fanout-risk` that happens to match is acceptable (conservative). A `safe` that differs is a **hard stop** — report it and do not merge PR 1.

Read column types via `GET /v2/dataModels/<id>/columns`, not `.../spec` — the spec response does not carry resolved types (established by the Task 7 spike).

- [ ] **Step 4: Record and clean up**

Append `## 13. Numeric verification of the fan-out guard` to the spec with the fixture description, the seven formula pairs and their two numbers each, and a one-line verdict. Delete the Sigma artifacts created (`DELETE /v2/files/<id>`). Commit the spec update.
