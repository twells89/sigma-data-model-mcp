# Power BI → Sigma: cross-table measure re-homing onto joined View elements

**Date:** 2026-07-30
**Repos:** `sigma-data-model-mcp` (PRs 1–2), `sigma-migration-skills` (PR 3)
**Baseline:** mcp `main` = `12919f2`; skills `main` = `b9bb2c79`, plugin `1.6.0`
**Supersedes the numbers in:** `task-7-report.md` (the original spike)

---

## 1. Problem

Idiomatic star-schema DAX is cross-table: `SUM(FACT[premium])` written as a measure homed on
a dimension. The converter scopes every DM metric to a single base element and a metric may
only reference columns on its own element, so every such measure is dropped with:

```
⚠ "<name>": references "[X]" which is not a column or metric on this element
   (cross-table measure) — dropped; recreate in a workbook element at the visual's grain.
```

Measured across four real reports (labelled R1–R4; never use the customer name):
**110 measures dropped for this reason**, and **74–100% of each model's DAX measures never
reach the data model.** Downstream, the workbook binds a visual to a measure with no DM
metric, the queryRef misses the master-map, and `drop_unresolved_columns!` prunes the
column — this is the "missing columns in tables" the customer reported.

The converter already builds joined `<Table> View` elements carrying the related dim columns
(the largest fact's View element carries 327 columns, 308 of them related). **Every one has `metrics: 0`.**

### The skill side is already plumbed

`migrate-powerbi.rb` registers each `<Fact> View` element as a master and wires every field
the View subsumes as an **alt**; `page-base` mode majority-picks the View for cross-table
visuals (`scripts/migrate-powerbi.rb:1199-1258`). The View is already reachable as a page
base. It just arrives carrying no metrics. **The converter is the only missing half**, which
means each recovered metric can unblock more than one dropped binding, not only itself.

### Why it isn't already done

In `src/powerbi.ts` the cross-table drop runs at lines 2609–2625, **inside the per-table
loop**. `buildDerivedElements` does not run until line 3028, and the relationship graph is
not built until line 2724. At drop time neither the Views nor the relationships exist.

---

## 2. Measured findings that shape the design

Both measured during design, not assumed.

### 2.1 No model in the corpus declares cardinality

Across all 8 extracted models (R1–R4 plus the E2E and gold fixtures), **zero** relationships
carry `fromCardinality` / `toCardinality` — TMSL omits them at the many-to-one default. The
converter is entirely cardinality-unaware (no occurrences of `cardinality`,
`crossFiltering`, `manyToMany` in `powerbi.ts`). `crossFilteringBehavior: bothDirections` is
common (15/24 in R1, 22/31 in R2) but that is filter propagation, not cardinality.

`buildDerivedElements` follows only `srcEl.relationships` — the outgoing, many-side
direction. Many:one composed with many:one is still many:one, so **outgoing traversal never
multiplies the base element's rows, at any depth.** Depth 2 therefore needs no new fan-out
analysis; it inherits the same guard as depth 1.

This is a claim about *row count*, and it is not the same as the hazard in §2.2. The View has
one row per base row either way; what §2.2 addresses is that a **dim value** appears repeated
across those rows, so aggregating it over-counts. Both statements hold at once, and the
second is what the guard tests.

### 2.2 The fan-out hazard is per-reference, not per-measure

The handoff proposed guarding on "covering View's base == the measure's home table". The
actual hazard is narrower and differently shaped. On `FACT View` a dim column is denormalized
across the join, so `SUM(DIM[price])` sums one duplicated row per fact row instead of the
dim's true row count. But `SUM(FACT[premium])` homed on a *dimension* — the exact idiom that
dominates the corpus — aggregates a **base** column and is safe on `FACT View` despite
home ≠ base.

So the guard keys on *what is aggregated and how far across the join it sits*. This is
expected to classify more measures as safe than the home==base rule. **That expectation is a
hypothesis; PR 1 measures it before PR 2 acts on it.**

### 2.3 A second, unguarded metric-emission path exists

Measures lifted off "measures-only" tables (`powerbi.ts:2688-2712`) are pushed onto `homeEl`
*after* the per-table cross-table guard has already run, bypassing it entirely. A two-table
measure that falls back to `factEl` can ship error-typed today. Routing both paths through
one ledger closes this.

---

## 3. Approach

**Deferred drop + demand-driven depth-2.** Replace the in-loop `splice` with a pending
ledger; after Views and relationships exist, resolve, guard, and attach-or-drop. Pull only
the second-hop columns a pending metric actually needs, so column growth is bounded by
demand rather than by schema size.

Rejected: a two-pass converter (doubles conversion time on 787-column models and requires
making every side effect in a ~600-line loop idempotent), and post-pass re-homing by parsing
the warning strings (reconstructs structured data from prose and cannot drive demand-driven
hop-2 at all, forcing unbounded column explosion).

---

## 4. Components

Four pieces, each independently testable.

### 4.1 `pendingCrossTable[]` — the ledger

Replaces `metrics.splice(i, 1)` at `powerbi.ts:2621`. Each entry records:

- the metric object (id, name, formula, format, description)
- home table name and home element id
- the unresolved refs
- **the aggregate wrapping each ref** — required by the guard

The existing 5-pass cascade loop stays. A metric dropped because it depends on a pending
metric is recorded as `cascade` and revived if its parent attaches.

The measures-only path (§2.3) routes through the same ledger.

### 4.2 Ref resolver

Given a ref name and a home table, walk the relationship graph and return every element whose
View could carry it, with hop distance. A pure function over `(tableColMap,
model.relationships)` — unit-testable without running a conversion.

### 4.3 Fan-out guard

Per reference in the metric formula:

> ⚠️ **This section was twice wrong and has been replaced. The authoritative rule is in
> §4.3a below.** Two earlier formulations are recorded there as superseded, because both
> looked correct and both would have shipped wrong numbers — that history is the reason the
> final rule is stated the way it is.

#### 4.3a The rule (authoritative — implemented in Task 8)

Grain is set by **what an aggregate ranges over**. On a View every aggregate ranges over base
rows, so re-homing is faithful only when the original did too.

Define an aggregate call's **summand** — the quantity whose grain matters:

| aggregate | summand |
|---|---|
| `Sum` / `Avg` / `Min` / `Max` / `StdDev` / `Var` / `Median` / `Count` / `CountDistinct` | the whole operand |
| `SumIf(e, pred)` / `Percentile(e, p)` | the **first argument only** — a predicate does not set grain |
| `CountIf(pred)` | **none** — the aggregated quantity is an implicit row |

An aggregate call is **base-grain** on View `V` iff its summand references **at least one hop-0
column** and **no hop-≥1 column**. A call with no summand is never base-grain. A measure is
re-homable iff every aggregate call is base-grain; a formula with no aggregate call changes no
grain and is base-grain by default.

Additionally, **fail closed**: a formula that is not well-formed (unclosed `"`, or unbalanced /
mis-nested `(` `[`) is never classified `safe` — every candidate is marked with a
`malformed-formula` sentinel and grain analysis is skipped. Measured on the four reference
models, this costs nothing: **0 of 108** real drops were malformed.

#### Superseded formulations, and why each failed

**(1) "covered by exactly one View AND that View's base == the measure's home table"** (the
handoff's rule). Too strict: rejects `SUM(FACT[premium])` homed on a dimension, which is the
single largest recoverable shape in the corpus.

**(2) "unsafe iff cross-hop AND duplication-sensitive aggregate AND the operand contains no
hop-0 column"** (this section's previous content). Broken two ways, both found by adversarial
review with worked numeric counter-examples:

- `Min` / `Max` / `CountDistinct` were exempted as idempotent under row *duplication*. They
  are — but a join also **omits** dimension rows matching zero base rows. Budgets 100 / 500 /
  200 with nobody in the 500 department: `Max` is **500** at dimension grain and **200** on
  the View.
- A hop-0 column *anywhere* in the operand was taken as pinning it to base grain. But in
  `Sum(If([AMOUNT] > 0, [LIST_PRICE], 0))` the hop-0 column is only in the predicate while the
  summed value is the dimension column — a silent over-count.

A third candidate, exempting references used only as comparison operands, was also rejected:
it cannot save `CountIf([Tier] = "VIP")`, whose summand is an implicit row. One VIP customer
with two fact rows gives Power BI **1** and the View **2**.

A metric attaches only if **every** ref resolves on that View and **every** ref is safe.

**Candidate selection.** Among Views passing the guard: prefer minimum max-hop, then the View
whose base == the measure's home table. If still ambiguous, **do not guess** — drop and
triage naming every candidate. A silently wrong number is worse than a drop.

**Formula rewrite.** Refs are rewritten to the View's display-name spelling before attach,
per the existing `viewColDisplay` helper (`powerbi.ts:2073-2077`): base columns render `Col`,
hop-1 columns render `Col (DIM)`. A collision — two dims exposing the same column name — is a
drop-and-triage case, never first-wins.

### 4.4 Demand-driven depth

`buildDerivedElements(elements, opts?)` where `opts = { depth, demand }`. `demand` is the set
of `(table, column)` pairs some pending metric needs. At depth 2 the builder follows a second
outgoing hop but materializes **only** columns present in `demand`.

Default `{ depth: 1, demand: null }` reproduces today's behaviour exactly. Only `powerbi.ts`
passes anything else.

**Blast-radius containment.** `buildDerivedElements` has 9 callers: `atlan`, `alteryx`,
`qlik`, `cognos`, `lookml`, `oac`, `thoughtspot`, `dbt`, `powerbi`. The other 8 keep
byte-identical output, asserted by snapshot test (§7).

---

## 5. Load-bearing open question — spike before PR 2

`viewColDisplay` handles 3-segment paths. Depth-2 requires `[Base/REL1/REL2/Col]`, a
4-segment ref, and **there is no evidence Sigma resolves those at all.** If it does not,
depth-2 must be built differently — a View sourcing a View rather than a longer path.

**Resolved by a live spike against Sigma with a GET-back readback, not against docs.** PR 1 is
unaffected: triage only needs to *report* hop-2 reachability, not materialize it. PR 2 is not
planned in detail until the spike answers this.

---

## 6. Phasing

Three PRs. Never two repos in one PR; shared and plugin never mix in one PR
(`shared-file-governance`).

| PR | repo | content | exit condition |
|---|---|---|---|
| 1 | mcp | **Triage only.** No change to what is emitted. Classifier computes covering element, hop distance, per-ref aggregate and distance, and fan-out verdict; emits it in the warning. | Per-bucket numbers measured on R1–R4, replacing the handoff's estimates. 110 opaque drops become a triaged list. |
| — | — | **Spike** (§5): does Sigma resolve 4-segment refs? | Live GET-back answer. PR 2 planned only after this. |
| 2 | mcp | **Deferred drop + demand-driven depth-2 + attach**, behind the guard. Includes the `opts` argument and the 8-caller byte-identity assertion. | R1–R4 cross-table drops fall; no metric references a column absent from its element. |
| 3 | skills | **Re-vendor.** `tools/vendor-converters.sh`, commit `converter/` + `PROVENANCE.json`, version bump (or `Skip-Version-Bump` trailer). | Live E2E green, PNG read. |

Splitting 1 from 2 is deliberate: the classifier is the risky math, and it ships first in a
form where a wrong answer is a wrong *message* rather than a wrong *number in a customer's
dashboard*.

**Scope of the implementation plan written from this spec: PR 1 and the spike only.** PR 2
depends on both the spike's answer and PR 1's measured buckets, so planning it now would be
planning against assumptions. PR 2 and PR 3 get their own plan once those land.

Nothing in the skill changes until PR 3 lands — merging in mcp changes nothing in the
vendored bundle. Verify the effect by importing **the branch's** bundle
(the vendored-vs-vendored comparison harness in the local artifacts dir), not the source repo.

---

## 7. Verification

TDD throughout: failing test first, and **prove it can fail** by re-injecting the bug.

- **Unit** — ref resolver and guard classifier are pure functions; table-driven over synthetic
  models, no conversion run needed.
- **Shared-code regression** — snapshot all 8 non-PBI `buildDerivedElements` callers; assert
  byte-identical output across the `opts` change. This is the containment proof.
- **Corpus, offline** — R1–R4 through the converter. Assert the drop count falls, and assert
  the invariant that would have caught this class originally: **no metric on any element
  references a column absent from that element.**
- **Baseline first** — mcp reports `427/398/26` at `12919f2`; the third number is the
  pre-existing failure count. Record it before judging any change. Do not infer
  "pre-existing" from "`main` fails identically" without checking whether this branch made
  `main` start failing.
- **Both test dirs** in the skills repo: `scripts/test-*.rb` **and** `tests/*.rb`. A green
  count from one glob means nothing.
- **Live E2E on PR 3** — a synthetic star-schema `.pbix` over real `CSA.TJ` tables carrying
  cross-table measures at hop 1 and hop 2 (extends the `e2e-viz-roles.pbix` precedent).
  POST the DM, **GET it back, assert zero `error`-typed columns and metrics.** Then the full
  migration, then **read the visual-QA PNG.**

**Why the readback is non-negotiable:** an error-typed metric still resolves in the
master-map, so binding coverage reads 100% and the field-loss gate cannot see it. Numbers-green
≠ render-correct. This is the exact failure mode that cost rework in the prior session.

---

## 8. Out of scope

- `USERELATIONSHIP` (34 drops) and `ISINSCOPE` (1) — genuine Sigma expressiveness limits;
  they belong in the disclosed-degradation ledger.
- `SELECTEDVALUE` / `ISFILTERED` measures (24 of the 110) — inherently report-context-
  dependent; no static View can ever host them. Triage reports them as never-hostable.
- Depth ≥ 3.
- The §5 items from the handoff (cosmetic PNG defects, composite-detector false positive,
  `field_binding_status` over-reporting).

---

## 9. Delegation

Per `~/.claude/CLAUDE.md`, `model` is set explicitly on every dispatch:

| work | model |
|---|---|
| planning, design, orchestration | Opus (this session) |
| implementation of plan tasks | Sonnet |
| mechanical transcription, grep/read/extract | Haiku |
| final whole-branch review | Opus |

A stuck implementer escalates one tier; never a silent retry at the same tier.

---

## 10. Naming and disclosure constraints

R1–R4 everywhere. **No customer name, and no customer-identifying path, in any commit
message, PR, issue, branch name, test fixture, or file committed to either repo.** All
fixtures synthetic.

---

## 11. Measured buckets (PR 1 result)

Measured by running `convertPowerBIToSigma` over the four reference models R1–R4
(`<artifacts>/gt/model_<report>.bim`, one file per report) at commit `78c3528` and
classifying every `(cross-table measure) — dropped` warning by the `TRIAGE:` text
`describeTriage()` (`src/powerbi-crosstable-triage.ts`) appends to it. The harness
lived outside the repo (`/tmp`, not committed — the fixtures are customer-derived)
and its regexes were derived by reading `describeTriage()`'s current output, not
copied from the original task brief (which predates the fail-closed malformed-formula
gate added in Task 8).

| Model | total | safe (hop-1 / hop-2 / hop-0) | fan-out risk | ambiguous | never-hostable | no-covering-View | malformed |
|---|---|---|---|---|---|---|---|
| R1 | 26 | 0 (0/0/0) | 4 | 1 | 13 | 8 | 0 |
| R2 | 57 | 10 (3/6/1) | 24 | 2 | 1 | 20 | 0 |
| R3 | 14 | 3 (3/0/0) | 9 | 0 | 0 | 2 | 0 |
| R4 | 11 | 0 (0/0/0) | 0 | 1 | 8 | 2 | 0 |
| **Total** | **108** | **13 (6/6/1)** | **37** | **4** | **22** | **32** | **0** |

(The one hop-0 safe candidate is a base other than the measure's home table that
owns the ref directly — a legitimate, if uncommon, shape; not a bug.)

**Re-measured after a fix, not the original PR 1 run.** The table above
supersedes the original PR 1 numbers (`34`/`3`/`36` for fan-out risk/ambiguous/
no-covering-View; everything else unchanged). Root cause: `refs` can
legitimately contain sibling METRIC names, not just columns — the drop-site
guard exempts a ref matching another metric already on the element — but
`columnOwners` only ever holds columns, so a metric ref resolved to hop
`Infinity` on every candidate base and sank the WHOLE measure to
"no-covering-View" even when its genuine column references were fully
coverable. This silently conflated two different mechanisms into one bucket:
structurally disconnected `CALCULATE` predicates (a real coverage failure) and
an unrelated metric dependency (not a coverage failure at all, just a
different kind of re-homing dependency). Fixed by threading the caller's
sibling-metric names through as `metricRefs`, excluding them from coverage/
grain analysis, and reporting the dependency distinctly (`Triage
.dependsOnMetrics`, surfaced by `describeTriage` as "Also depends on metric
...") instead of folding it into the column-coverage verdict. 9 of the 108
warnings carry that note. Of those, **4 moved out of no-covering-View** once
their column refs were judged on their own (3 into fan-out-risk, 1 into
ambiguous — matching the aggregate deltas above exactly). **The other 5 remain
no-covering-View** — but now for a REAL reason: excluding the metric ref from
coverage is exactly what the fix does, so a measure still uncovered after it
can only be because one of its genuine COLUMN references — independent of the
metric dependency — still fails to resolve within the configured depth, not an
artifact of the metric-ref confound. All 9 carry the "Also depends on metric"
note regardless of which bucket they land in.

`npm test` after this fix: **511/482/26/3** — pass count rose (new regression
tests for the fix itself), **fail count unchanged at 26** from before it.

### Comparison against independently-measured figures

- **Total cross-table drops:** measured **108** vs. an independent estimate of
  **110–111**. Close (~2%) but not exact, and not forced to match. The bad-ref/drop
  decision that determines *whether* a metric is dropped is unrelated to and
  unaffected by any triage-classifier commit on this branch, so this branch's work is
  not the source of the small gap; the likeliest explanation is that the independent
  figure came from a different method (e.g. a static DAX scan) that classifies a
  handful of edge cases (mis-cased refs already resolved by a pre-existing fix,
  cascade-dropped metrics) differently than running the converter end to end.
- **Report-context (`SELECTEDVALUE`) measures:** an independent DAX parser found
  **29** occurrences across the four models; this run's **never-hostable** bucket is
  **22**. Not a contradiction — 29 counts raw `SELECTEDVALUE` occurrences regardless
  of whether that measure is ever a cross-table drop; 22 counts only measures that
  both reach the drop path and trip `isNeverHostable` (`SELECTEDVALUE` or
  `ISFILTERED`). Consistent with the independent measurement's own caveat that "not
  all of those are necessarily cross-table drops."
- **"69 bare foreign aggregate" measures (expected shape: safe) vs. 13 measured
  safe:** the discrepancy worth real scrutiny, since it bears on the central
  hypothesis. Sampling the actual raw DAX — anonymised, a representative R2 measure
  of the form `CALCULATE(SUM(FACT_A[AMOUNT]), DIM_B[CATEGORY] = "<literal>")` — shows
  the aggregate's own operand IS a bare single-table reference —
  but `CALCULATE`'s filter predicate references a second table, and every ref in the
  formula (not just the aggregate's summand) must still resolve within 2 join hops
  for the measure to attach anywhere. Checking R2's relationships
  directly: `FACT_A` has **no relationship path to `DIM_B` at
  all** (a different fact relates to it) — so that measure is correctly
  `uncovered`, not `safe`. The 69 figure most likely measured only the aggregate
  call's own shape without accounting for `CALCULATE` filter predicates pulling in a
  second table — common throughout this corpus, and something the coverage rule
  (correctly, by design) still requires to be reachable. Confirmed this is a
  structural/schema fact, not a harness or regex artifact: 0 of 108 real warnings
  were unmatched by the classifier regexes across all four models.

### The central hypothesis

The design's hypothesis (§2.2): the per-reference/per-aggregate-call fan-out guard
classifies **more than 23** measures as safe (the handoff's home==base rule estimated
~23 safely automatable).

**Measured safe count: 13. This does not exceed 23 — the hypothesis, as stated, did
not hold on this corpus.**

### 11.1 Two follow-up fixes to the `no-covering-View` bucket, re-measured

A spike categorised all 32 of the PR 1 `no-covering-View` measures and proposed two
fixes. Both were re-measured directly against R1-R4 on this branch (not assumed from
the spike) before and after implementation, using the same method as §11 (running
`convertPowerBIToSigma` over the four models and classifying every `(cross-table
measure) — dropped` warning by its `TRIAGE:` text).

**Fix 1 — raise `triageCrossTable`'s default `maxDepth` from 2 to 3.** The spike found
9 of the 32 are `CALCULATE(agg, DIM[attr] = value)` shapes where the filtered
dimension is reachable from the aggregate's own fact at 3 hops, not 2 — and predicted
all 9 would flip to a clean `safe` verdict, taking safe from 13 to 22.

**Measured result differs from the spike's prediction:**

| bucket | depth 2 (§11 baseline) | depth 3 (this branch) | delta |
|---|---|---|---|
| safe | 13 | 16 | **+3**, not +9 |
| fan-out risk | 37 | 37 | **0** |
| ambiguous | 4 | 9 | **+5** |
| never-hostable | 22 | 22 | **0** |
| no-covering-View | 32 | 24 | **-8** |
| malformed | 0 | 0 | 0 |

8 measures leave `no-covering-View` at depth 3, not 9 — but only **3** land cleanly on
`safe`; the other **5** land on `ambiguous` instead. Depth 3 does not only add new
`safe` hosts for the 8 previously-uncovered measures — for **5 measures that were
already `safe` at depth 2 with exactly one covering base**, the wider reach uncovers a
SECOND independently-safe base that depth 2 couldn't see, and `triageCrossTable`
correctly reports that as `ambiguous` (needs a human choice) rather than silently
picking one, per the existing multi-candidate rule (T4i). Net safe count is 13 + 8 - 5
= 16, not 22. **Reported here because it differs from the spike's premise, not to
reconcile the two** — the spike's per-measure categorisation of which 9 would move
was directionally right (8 of 9 did move), but its claim that they all land on a
*clean* `safe` verdict did not hold: a majority of what moves lands on `ambiguous`
just as often as `safe`.

**The guard's core safety property holds exactly as designed: fan-out risk (37) and
never-hostable (22) are IDENTICAL before and after.** No measure moved from either
bucket into `safe`. This is not incidental — `reachableTables` is a monotonic BFS
(first-visit-wins), so a reference that already resolved at <= 2 hops keeps that exact
hop distance at any higher `maxDepth`; increasing depth can only ADD new candidates
for previously-unreachable refs, never change an already-covered candidate's grain
verdict. Confirmed both by the unchanged aggregate counts and by a per-measure diff
across all 108 warnings: zero measures show a `fanout-risk → safe` or
`never-hostable → safe` transition. New unit tests `T4p`/`T4q`
(`src/powerbi.crosstable-triage.test.ts`) lock this down synthetically: a 3-hop
PREDICATE-only reference is now `safe` with no `maxDepth` override (T4p), while a
genuine 3-hop reference INSIDE an aggregate's summand is still `fanout-risk` (T4q) —
depth alone cannot manufacture a false `safe`.

`describeTriage`'s "no View covers it within the configured depth" wording already
names no specific number (Task 8 made this generic for exactly this reason — a caller
passing a non-default `maxDepth` would otherwise get a message naming the wrong
figure) — confirmed unchanged, no edit needed.

**Fix 2 — attribute the other 15 `no-covering-View` measures to their real blocker.**
The spike found these are not reachability problems: they inherit an unrelated
failure from a sibling METRIC reference that `columnOwners` has no entry for (it is
built only from columns), so the ref resolves to hop `Infinity` on every candidate
exactly like a genuinely disconnected column would — 5 reference a metric declared on
a DIFFERENT element (a hard Sigma constraint; no hop limit ever fixes this), 6 have a
same-table sibling dropped for FAN-OUT reasons, and 4 have a same-table sibling
dropped as NEVER-HOSTABLE.

Implemented as two new caller-side checks in `src/powerbi.ts`, run BEFORE
`triageCrossTable` for a "bad" ref that turns out to be one of these two name shapes,
bypassing the reachability classifier entirely rather than asking it to describe a
non-reachability problem:
- a whole-model, up-front `name → declaring table` index catches a ref naming a
  metric declared on a different element;
- a per-table `Map` recording each dropped metric's own reason (populated across the
  existing multi-pass drop loop) catches a ref naming a same-table sibling that was
  itself already dropped, and quotes that sibling's own drop reason verbatim so a
  chain of dependent drops composes without losing information.

Both surfaced via a new `MetricBlocker` type / `describeMetricBlocker` function in
`src/powerbi-crosstable-triage.ts`, deliberately NOT folded into `Triage`/
`describeTriage` — both conditions are known before any coverage/grain analysis would
run, so running that analysis at all (only to discard the result) risks exactly the
misattribution being fixed if a future edit ever reordered the checks.

**Measured result — combined with Fix 1 (this branch's final state):**

| bucket | depth 2, pre-fix (§11 baseline) | this branch (depth 3 + Fix 2) | delta |
|---|---|---|---|
| safe | 13 | 16 | +3 (Fix 1 only; Fix 2 does not change this) |
| fan-out risk | 37 | 37 | 0 |
| ambiguous | 4 | 9 | +5 (Fix 1 only) |
| never-hostable | 22 | 22 | 0 |
| **blocked — cross-element metric ref** | — | **5** | new bucket |
| **blocked — dropped-sibling cascade** | — | **11** | new bucket |
| no-covering-View | 32 | **8** | -24 total (-8 from Fix 1, -16 from Fix 2) |
| malformed | 0 | 0 | 0 |

**5 cross-element-metric measures exactly matches the spike's count.** The
dropped-sibling cascade measures **11, not 10 (6 fan-out + 4 never-hostable)**: sub-
classifying by the sibling's own ultimate reason confirms 6 fan-out + 4 never-hostable
(exactly matching the spike) **plus 1 more** whose sibling's own verdict is `safe` —
that sibling is only reachable (and therefore only exists as a candidate at all)
because of Fix 1's depth-3 default; at depth 2 it would have been `no-covering-View`
itself. A `safe` verdict does not exempt a measure from being dropped — PR 1 is
triage-only and drops every cross-table measure regardless of verdict, pending PR 2's
deferred-drop-and-attach — so a dependent measure still inherits the gap. This 16th
case is a genuine interaction between the two fixes, not a discrepancy to explain
away: Fix 2's cascade tracking is verdict-agnostic by design (it quotes whatever
reason the sibling was dropped for, not just fan-out/never-hostable specifically), so
it correctly catches this shape too even though the spike's manual read — done before
Fix 1 existed — had no way to find it.

All new coverage (`T12a`/`T12b` unit tests for `describeMetricBlocker`, `T13a`-`T13c`
end-to-end tests through the real drop-site wiring in `src/powerbi.ts`) is in
`src/powerbi.crosstable-triage.test.ts`. Every negative-control proof (revert the
fix, show the specific new test fail for the expected reason, restore) was run and
is recorded in the branch's task report, not reproduced here.

`npm test` on this branch: **590 tests, 561 pass, 26 fail, 3 skipped** — pass count
rose by the 7 new regression tests (`T4p`, `T4q`, `T12a`, `T12b`, `T13a`, `T13b`,
`T13c`); the 26 failing tests are IDENTICAL, by name, to the 26 already failing on
`main` before this branch — confirmed by diff, not just by count. None are caused by
or fixed by this branch's changes.

---

## 13. Numeric verification of the fan-out guard

Task 9 — the merge gate for PR 1. Every prior review of the guard (`triageCrossTable`,
`src/powerbi-crosstable-triage.ts`) was analysis: a fresh reader looking for a
counter-example. This is arithmetic instead: a real 3-table snowflake in the CSA.TJ
warehouse schema, every formula's value computed two independent ways, and the
classifier's actual verdict (not an assumed one) checked against whether the two
numbers agree.

### Fixture

Connection `cb2f5180-641f-47bd-8efa-da9d590d855a`, database `CSA`, schema `TJ`.
`CSA.TJ` was listed first (no table names were assumed); the natural retail star
`RETAIL_SALES → RETAIL_STORE → RETAIL_DISTRICT` was chosen because both joins are
already many→one on real foreign keys (`LOCATION_ID`, `DISTRICT_ID`) — no synthetic
VALUES table was needed for the base shape:

- **FACT** = `RETAIL_SALES` (923,371 rows), columns `FACT_LOCATION_ID` = `Location Id`,
  `FACT_AMOUNT` = `Sum Regular Sales Dollars`.
- **DIM_A** = a `sql`-source element over `RETAIL_STORE` (104 rows):
  `SELECT LOCATION_ID AS DIM_A_KEY, STORE_TYPE AS DIM_A_TIER, DISTRICT_ID,
  CASE WHEN LOCATION_ID = 505 THEN 999999 ELSE SELLING_AREA_SIZE END AS DIM_A_BUDGET
  FROM CSA.TJ.RETAIL_STORE`. The `CASE WHEN` is the one constructed part of the
  fixture, recorded exactly here: natural data already has 4 zero-match stores and
  heavy multi-match duplication (below), but the store holding the naturally-highest
  `SELLING_AREA_SIZE` (65000) happened to have FACT matches, so `Max` would have
  coincidentally agreed either way. Overriding one of the zero-match stores
  (`LOCATION_ID 505`) to hold the highest value in the fixture (999999) makes the
  omission hazard bite `Max` unambiguously instead of leaving it to chance. Every
  other column is the real warehouse value, unmodified.
- **DIM_B** = `RETAIL_DISTRICT`, joined many→one from DIM_A on `DISTRICT_ID` (0
  orphans — every store's district exists). Not referenced by any of the 7 formulas;
  included only to make the fixture a genuine 2-hop snowflake, matching the shape the
  guard is designed to reason about.
- **FACT View** = a `join`-source element, `FACT LEFT JOIN DIM_A ON
  FACT_LOCATION_ID = DIM_A_KEY`, primary source FACT — the denormalized element a
  re-homed measure would actually land on.

**Both required properties confirmed in the data before running anything:**

- **Zero-match DIM_A rows (omission):** 104 `RETAIL_STORE` rows vs. 100 distinct
  `LOCATION_ID` values in `RETAIL_SALES` → exactly 4 stores (`LOCATION_ID` 505, 508,
  509, 513) match **zero** FACT rows.
- **Multi-match DIM_A rows (duplication):** 923,371 FACT rows spread across only 100
  matched stores — e.g. `LOCATION_ID` 13 alone matches 22,208 FACT rows.

### Computation

Native grain = the same Sigma formula text evaluated directly on the element that
actually owns the columns (FACT for `FACT_AMOUNT`-only formulas, DIM_A for
`DIM_A_*`-only formulas), no join. Re-homed = the identical formula text evaluated on
"FACT View", queried through Sigma's own data-model query engine
(`metric('<id>', t)` against a real data model built via `POST /v2/dataModels/spec`,
column types confirmed resolved via `GET /v2/dataModels/<id>/columns` per the Task 7
spike). For `SumIf` — the one formula whose columns span both tables, so it has no
single-table "native" element to evaluate on at all — the native number is instead an
independently hand-written SQL join run directly against the connection (not through
the data model), so the two numbers do not share an implementation path.

| formula | classifier verdict (base=FACT) | native | re-homed (FACT View) | match? |
|---|---|---|---|---|
| `Sum([FACT_AMOUNT])` | safe | 41,013,686.95 | 41,013,686.95 | **match** |
| `Count([FACT_AMOUNT])` | safe | 923,371 | 923,371 | **match** |
| `SumIf([FACT_AMOUNT], [DIM_A_TIER] = "Same Store")` | safe | 36,353,188.24 (independent hand-written join SQL) | 36,353,188.24 | **match** |
| `Sum([DIM_A_BUDGET])` | fanout-risk | 3,519,999 | 35,239,310,000 | **differ** (duplication) |
| `Max([DIM_A_BUDGET])` | fanout-risk | 999,999 | 65,000 | **differ** (omission) |
| `CountDistinct([DIM_A_KEY])` | fanout-risk | 104 | 100 | **differ** (omission) |
| `CountIf([DIM_A_TIER] = "Same Store")` | fanout-risk | 94 | 825,676 | **differ** (duplication) |

Classifier verdicts above are the actual output of `triageCrossTable` (imported by
absolute path from a throwaway `/tmp` script, not assumed) for base table `FACT`,
using `relationships: [{from:"FACT",to:"DIM_A"}, {from:"DIM_A",to:"DIM_B"}]` and
`columnOwners` mapping each ref to its true owning table — not hand-transcribed from
this table's own "guard verdict" column.

### Gate result

**Every `safe` verdict's two numbers match. No hard stop.** All three `safe` rows
(`Sum`/`Count`/`SumIf` of `FACT_AMOUNT`) agree exactly between native and re-homed.

All four `fanout-risk` rows **genuinely differ** — none merely "coincidentally
matched" (which would still have passed the gate conservatively, per the design, but
didn't happen here): `Sum`/`CountIf` differ by ~4 orders of magnitude (duplication —
a store's value or matching predicate repeated once per matched FACT row); `Max`/
`CountDistinct` differ because the omitted stores (zero FACT matches) never appear in
FACT View at all (omission).

**PR 1's fan-out guard is arithmetically verified on this fixture. Cleared to merge.**

### Cleanup

The data model (`dataModelId 7c08b0e4-699a-4478-a9cb-52eede793ed3`, "Task9
CrossTable FanOut Probe (delete me)") was the only Sigma artifact created. Deleted via
`DELETE /v2/files/7c08b0e4-699a-4478-a9cb-52eede793ed3` (200, confirmed
`isArchived: true` on a follow-up GET). No workbook, no export, no data load — every
query against it was a scalar aggregate through Sigma's query engine.
