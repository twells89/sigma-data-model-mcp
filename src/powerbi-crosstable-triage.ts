/**
 * Cross-table measure TRIAGE — pure classification, no side effects.
 *
 * When the converter drops a measure because it references a column that is not
 * on its home element, this module answers three questions from the RAW model
 * alone (no Views, no relationship elements — neither exists at drop time):
 *   1. Which "<T> View" element could carry every reference?
 *   2. How many join hops away is the furthest reference?
 *   3. Would aggregating across those hops double-count?
 *
 * WHAT A "safe" VERDICT DOES AND DOES NOT GUARANTEE: `triageCrossTable` refuses
 * to run its grain analysis at all on a `sigmaFormula` that `isWellFormedFormula`
 * rejects — every unclosed `"`, and every `(`/`[` that doesn't close in proper
 * (non-cross-nested) order, marks every COVERED candidate `fanout-risk` instead
 * (an uncovered candidate is omitted entirely, never marked — see `describeTriage`).
 * That check is a SOUND structural check, not a heuristic, for those two failure
 * modes. It is NOT a full grammar/tokenizer, and it has one known, accepted
 * blind spot: two independent stray `"` characters can coincidentally "pair up"
 * under simple next-matching-quote scanning and mask a real call between them,
 * which would still pass as "well-formed". A `safe` verdict here is trustworthy
 * for machine-generated, well-formed `sigmaFormula` input (the only kind this
 * module is designed to consume) — it is NOT a security boundary against
 * adversarially-constructed text, and callers re-homing a measure on the
 * strength of a `safe` verdict should be aware of that distinction.
 *
 * NOT EVERY EXPORT BELOW IS ON `triageCrossTable`'S PATH. `enclosingAggregate`,
 * `enclosingAggregateCall`, `enclosingAggregateCalls` (and the internal
 * `aggregateCallAt` they share) were the Task 4 guard's original per-reference
 * walk; Task 8 replaced that guard with the per-aggregate-call `enumerateAggregateCalls`
 * + `aggregateSummand` walk used below, and none of the three "enclosing*"
 * exports are called by `triageCrossTable` anymore. They are kept — not dead
 * weight to delete — because the follow-up PR (deferred drop + attach) is
 * expected to need "which aggregate wraps THIS specific occurrence" when
 * rewriting a formula's refs onto a View; see each function's own docstring
 * for the same note, and `T1a`-`T1k` for their still-live regression coverage.
 */

/** Sigma aggregate functions. Spelling matters: Avg (not Average), CountDistinct. */
const AGGREGATES = new Set([
  'Sum', 'Count', 'CountDistinct', 'Avg', 'Min', 'Max',
  'StdDev', 'Var', 'Median', 'Percentile', 'CountIf', 'SumIf',
]);

/**
 * Core walk shared by `maskStringLiterals` and `isWellFormedFormula`: replace
 * every "..." string literal in `s` with same-length blanks (so a scan for
 * names, parens, brackets, or commas can't be fooled by literal TEXT that
 * merely looks like one — a label reading `"Count(5)"`, a comparison value
 * containing a stray `)`, `[`, or `,`), and report whether every quote it
 * opened was actually closed. Positions are preserved — same length, same
 * non-literal characters — so offsets/split-points computed against the mask
 * apply unchanged to the ORIGINAL text.
 *
 * ONLY `"` is a string delimiter. `'` is ordinary character content — verified
 * against this repo's own converters before relying on it: `src/formulas.ts`
 * explicitly REWRITES Tableau's and LookML's single-quoted source literals to
 * double quotes when emitting a Sigma formula (`f.replace(/'([^']*)'/g,
 * '"$1"')`, and the equivalent for LookML `IN (...)`/concat patterns); no
 * converter in this codebase ever emits or expects a `'...'` literal in Sigma
 * formula text. Treating `'` as a delimiter was a real regression: possessive
 * column display names (`Manager's Approval Amount`, `O'Brien`) are ordinary
 * warehouse content, and a single stray apostrophe used to condemn an
 * otherwise-valid formula to `fanout-risk` with zero coverage.
 *
 * An UNTERMINATED quote (malformed or truncated input) is treated as an ordinary
 * character, NOT as the start of a literal that runs to end-of-string — masking
 * to end-of-input would erase every real ref/paren/name past that point. But
 * NOT swallowing it is only half the story: text that follows an unterminated
 * quote is unmasked and re-enters normal scanning, so a stray `)`, `]`, or a
 * name that happens to look like a call INSIDE what the author meant to be a
 * literal can still confuse a downstream scan (see `isWellFormedFormula` — this
 * is exactly why the fix for this class of hazard moved from "patch the mask"
 * to "gate on well-formedness before trusting any scan at all"). `wellFormed`
 * here reports the quote-closure half of that judgment; callers that need the
 * FULL judgment (quotes closed AND parens/brackets properly NESTED, not just
 * independently balanced) use `isWellFormedFormula`, not this flag alone.
 *
 * ASSUMPTION, stated rather than chased: a `"..."` literal escapes an embedded
 * quote with a backslash (`\"`), matching this codebase's other fixtures. If
 * some upstream DAX-to-Sigma stage instead emits the SQL/DAX convention of a
 * DOUBLED quote (`""` for an embedded `"`) with NO backslash, escape pairs are
 * always adjacent, so the masked span comes out identical either way — this
 * assumption turned out to matter less than originally flagged.
 */
function maskAndCheckQuotes(s: string): { masked: string; wellFormed: boolean } {
  let out = '';
  let i = 0;
  let wellFormed = true;
  while (i < s.length) {
    const c = s[i];
    if (c === '"') {
      const quote = c;
      let j = i + 1;
      let closed = false;
      while (j < s.length) {
        if (s[j] === '\\' && j + 1 < s.length) { j += 2; continue; }
        if (s[j] === quote) { j++; closed = true; break; }
        j++;
      }
      if (closed) {
        out += ' '.repeat(j - i);
        i = j;
      } else {
        out += c;   // unterminated — not a literal; leave it and keep scanning
        i++;
        wellFormed = false;
      }
    } else {
      out += c;
      i++;
    }
  }
  return { masked: out, wellFormed };
}

/** Thin wrapper over `maskAndCheckQuotes` for callers that only need the mask. */
function maskStringLiterals(s: string): string {
  return maskAndCheckQuotes(s).masked;
}

/**
 * True iff `s` is well-formed enough for the grain analysis in `triageCrossTable`
 * to trust its own output: every quote it opens closes, AND every paren and
 * bracket it opens (outside string literals) closes IN THE RIGHT ORDER — checked
 * on the MASKED text, so a paren/bracket genuinely inside a well-formed literal
 * doesn't count.
 *
 * "In the right order" is load-bearing: parens and brackets are tracked on a
 * SINGLE stack, not two independent counts, because independent counts accept
 * CROSS-nesting — `(...[...)...]` — where the totals balance but the closers
 * arrive in the wrong order relative to their openers. `Sum([AMOUNT] &
 * [REGION)]` has exactly one `(`/`)` pair and one net-balanced pair of `[`/`]`
 * (two of each), so two independent counters accept it — but
 * `enumerateAggregateCalls` only tracks parens for its own balance, so the `)`
 * closes `Sum(...)` right after `[REGION`, one bracket short: the captured
 * operand is `[AMOUNT] & [REGION`, with no closing `]` anywhere inside it, so
 * the `[ref]` extraction regex never matches REGION at all. A single stack
 * (push on any opener, and require the popped opener to match the closer's
 * OWN kind) rejects this the instant the mismatched `)` arrives — a sound
 * structural check, not a heuristic, and it closes cross-nesting at any depth.
 *
 * This exists because patching each PLACE a malformed formula could confuse a
 * downstream scan turned out to keep relocating the same hazard rather than
 * eliminating it: an unbalanced paren makes `enumerateAggregateCalls`'s
 * forward balance-count never find a close, silently DROPPING the whole
 * aggregate call (and whatever cross-hop ref it held) from enumeration; an
 * unbalanced OR cross-nested bracket makes the `[ref]` extraction regex fail
 * to match from that point, silently dropping just that ref. Either way a
 * genuinely cross-hop reference disappears from consideration and the
 * candidate comes out "safe" with nothing left to contradict it — the one
 * outcome this guard exists to prevent. Gating on well-formedness up front,
 * and refusing to attempt grain analysis at all when it fails, makes the
 * whole class unreachable instead of patched one shape at a time.
 *
 * KNOWN, ACCEPTED LIMIT — not a security boundary: two INDEPENDENT stray `"`
 * characters (e.g. two separately-truncated values) can still coincidentally
 * "pair up" under next-matching-quote scanning and mask a real call between
 * them, passing this check while producing a false "safe". Closing that fully
 * needs grammar/token awareness this lightweight, pattern-based classifier
 * does not have — verified and accepted as a documented gap, not fixed here
 * (see the Task 8 report). A `safe` verdict from this function is trustworthy
 * for machine-generated, well-formed `sigmaFormula` input; it is NOT a
 * guarantee against adversarially-constructed text.
 */
function isWellFormedFormula(s: string): boolean {
  const { masked, wellFormed } = maskAndCheckQuotes(s);
  if (!wellFormed) return false;
  const stack: string[] = [];
  for (const ch of masked) {
    if (ch === '(' || ch === '[') stack.push(ch);
    else if (ch === ')') { if (stack.pop() !== '(') return false; }
    else if (ch === ']') { if (stack.pop() !== '[') return false; }
  }
  return stack.length === 0;
}

/**
 * Shared walk: given the formula text and the index of one `[ref]` occurrence,
 * find the aggregate call enclosing THAT occurrence (name + operand text).
 * Walks outward past non-aggregate wrappers (If, Coalesce, arithmetic grouping),
 * then scans forward to the matching close paren to capture the operand.
 *
 * Exists so the walk is written once — both `enclosingAggregateCall` (first
 * occurrence) and `enclosingAggregateCalls` (every occurrence) call this.
 *
 * Both the backward depth-count and the forward one run against a string-
 * literal-MASKED copy of `f` — a literal ")" character (a comparison value
 * like `"A)B"`) would otherwise desync the backward count and cause it to
 * walk straight past the REAL enclosing aggregate's own paren, treating it as
 * already matched and returning null as if the ref were unaggregated. Operand
 * text is still sliced from the ORIGINAL `f`, so it keeps its real content.
 *
 * NOT used by `triageCrossTable` (see the module header) — retained for the
 * follow-up PR.
 */
function aggregateCallAt(f: string, idx: number): { name: string; operand: string } | null {
  const masked = maskStringLiterals(f);
  let depth = 0;
  for (let i = idx - 1; i >= 0; i--) {
    const ch = masked[i];
    if (ch === ')') { depth++; continue; }
    if (ch !== '(') continue;
    if (depth > 0) { depth--; continue; }
    const m = masked.slice(0, i).match(/([A-Za-z_]\w*)\s*$/);
    if (m && AGGREGATES.has(m[1])) {
      let d = 0;
      for (let j = i; j < masked.length; j++) {
        if (masked[j] === '(') d++;
        else if (masked[j] === ')' && --d === 0) return { name: m[1], operand: f.slice(i + 1, j) };
      }
      return { name: m[1], operand: f.slice(i + 1) };   // unbalanced input — degrade, don't throw
    }
    // a non-aggregate wrapper (If, Coalesce, arithmetic grouping) — keep going outward
  }
  return null;
}

/**
 * The aggregate call enclosing the FIRST occurrence of `[ref]`: its name and the
 * raw text of its operand. A ref can appear more than once in a formula with
 * different verdicts per occurrence — use `enclosingAggregateCalls` when that
 * matters (it does, for the fan-out guard). This first-occurrence accessor
 * exists for `enclosingAggregate` and is otherwise a convenience.
 *
 * The `[ref]` OCCURRENCE is located on a string-literal-MASKED copy — text like
 * `"Weird[AMOUNT]label"` must not be read as a real reference to column AMOUNT
 * just because the bracket shape happens to appear inside a literal.
 *
 * NOT used by `triageCrossTable` (see the module header) — retained for the
 * follow-up PR.
 */
export function enclosingAggregateCall(
  formula: string, ref: string,
): { name: string; operand: string } | null {
  const f = String(formula);
  const idx = maskStringLiterals(f).indexOf(`[${ref}]`);
  if (idx < 0) return null;
  return aggregateCallAt(f, idx);
}

/**
 * The nearest AGGREGATE function enclosing the first `[ref]` occurrence, or null
 * if not aggregated. NOT used by `triageCrossTable` (see the module header) —
 * retained for the follow-up PR.
 */
export function enclosingAggregate(formula: string, ref: string): string | null {
  return enclosingAggregateCall(formula, ref)?.name ?? null;
}

/**
 * The aggregate call enclosing EVERY occurrence of `[ref]` in the formula — a
 * ref can appear more than once (e.g. `Max([X]) + Sum([X])`), and each
 * occurrence can have its own enclosing aggregate. Occurrences with no
 * enclosing aggregate are omitted (they can't themselves cause duplication).
 *
 * Occurrences are located on a string-literal-MASKED copy — same reasoning as
 * `enclosingAggregateCall`.
 *
 * NOT used by `triageCrossTable` (see the module header) — retained for the
 * follow-up PR.
 */
export function enclosingAggregateCalls(
  formula: string, ref: string,
): Array<{ name: string; operand: string }> {
  const f = String(formula);
  const masked = maskStringLiterals(f);
  const target = `[${ref}]`;
  const calls: Array<{ name: string; operand: string }> = [];
  let from = 0;
  for (;;) {
    const idx = masked.indexOf(target, from);
    if (idx < 0) break;
    const call = aggregateCallAt(f, idx);
    if (call) calls.push(call);
    from = idx + target.length;
  }
  return calls;
}

/**
 * Split an argument list on TOP-LEVEL commas only (parens and brackets nest).
 * Split points are found on a string-literal-MASKED copy so a comma inside a
 * value like `"East, West"` is not read as an argument separator; the returned
 * pieces are sliced from the ORIGINAL text (same length as the mask), so they
 * keep their real, unmasked content.
 */
export function splitTopLevelArgs(s: string): string[] {
  const masked = maskStringLiterals(s);
  const out: string[] = [];
  let depth = 0, start = 0;
  for (let i = 0; i < masked.length; i++) {
    const c = masked[i];
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
  // Scan and paren-balance against a string-literal-blanked MASK so text inside a
  // literal — `"Count(5)"`, a value containing `)` — can't be read as real syntax.
  // The mask is the same length as `f`, so indices found in it slice `f` directly.
  const masked = maskStringLiterals(f);
  const out: Array<{ name: string; operand: string }> = [];
  const re = /([A-Za-z_]\w*)\s*\(/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(masked))) {
    if (!AGGREGATES.has(m[1])) continue;
    // a preceding word char means this is a longer identifier (MySum), not our call
    const before = m.index > 0 ? masked[m.index - 1] : '';
    if (/[A-Za-z0-9_]/.test(before)) continue;
    const open = m.index + m[0].length - 1;
    let d = 0;
    for (let j = open; j < masked.length; j++) {
      if (masked[j] === '(') d++;
      else if (masked[j] === ')' && --d === 0) { out.push({ name: m[1], operand: f.slice(open + 1, j) }); break; }
    }
  }
  return out;
}

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

/**
 * True when the measure reads the REPORT filter context and therefore cannot be
 * hosted on any static View, at any depth. Matches a CALL (token followed by an
 * open paren) so a column merely named SELECTEDVALUE_FLAG is not a false positive.
 * Tested against a string-literal-MASKED copy of `rawDax` — a comparison value
 * reading `"SELECTEDVALUE(x)"` is text, not a real DAX call, and must not
 * false-positive the whole measure into never-hostable.
 *
 * Scoped deliberately to the two tokens measured across R1-R4 (24 of 110 drops).
 * Adding tokens changes the measured buckets — re-run the Task 6 measurement if
 * you extend this set.
 */
export function isNeverHostable(rawDax: string): boolean {
  return /\b(?:SELECTEDVALUE|ISFILTERED)\s*\(/i.test(maskStringLiterals(String(rawDax || '')));
}

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
  /**
   * Sibling METRIC names found among `refs` (e.g. a Sigma formula that reads
   * `[Other Metric]` directly, legal because the drop test at the call site
   * exempts metric names from "bad"/foreign). These are NOT columns —
   * `columnOwners` never has an entry for one — so they carry no hop distance
   * and must be excluded from coverage/grain analysis, not treated as an
   * uncovered column. See `triageCrossTable`'s doc comment for why this
   * matters and `describeTriage` for how it's surfaced.
   */
  dependsOnMetrics: string[];
};

/**
 * Classify a measure the converter is about to drop as cross-table.
 *
 * A candidate is a table B that (a) has at least one outgoing relationship —
 * only such tables get a "<B> View" element — and (b) can reach the owning
 * table of EVERY reference within maxDepth hops.
 *
 * The fan-out verdict is per-AGGREGATE-CALL, not per-reference: grain is set by
 * what an aggregate ranges over. On a View every aggregate ranges over base
 * rows, so re-homing is faithful only when the call's summand — the operand,
 * minus any predicate argument, minus the implicit row a CountIf ranges over —
 * references at least one hop-0 column and no hop->=1 column. A cross-hop
 * column in the summand would be duplicated by the join (or, for Min/Max/
 * CountDistinct, silently omitted by it); a summand with no hop-0 column at
 * all is a dimension-grain question asked at base grain either way.
 *
 * A "safe" verdict is a claim this function is willing to stand behind — so a
 * `sigmaFormula` this module cannot confidently tokenize (an unterminated
 * quote, an unbalanced paren, an unbalanced bracket) never reaches the grain
 * analysis above at all: every covered candidate is marked `fanout-risk` with
 * `'malformed-formula'` in `unsafeRefs` instead. This is strictly conservative
 * — it costs coverage on input we can't parse and nothing on input we can —
 * and it makes the entire class of stray-delimiter-inside-a-literal hazards
 * unreachable, rather than requiring a fix for each new shape one at a time.
 *
 * `refs` CAN legitimately contain sibling METRIC names, not just columns — the
 * call site's drop test exempts a ref that matches another metric already on
 * the same element. `columnOwners` never has an entry for a metric name (it is
 * built from `model.tables[].columns`, never `.measures`), so treating a
 * metric ref like a column ref makes it resolve to hop `Infinity` on EVERY
 * candidate, which previously sank the whole measure to "no View covers it"
 * even when every genuine column reference was fully coverable. Pass the
 * caller's sibling-metric names via `metricRefs` so this module can tell the
 * two apart: they're excluded from both coverage and grain analysis (a metric
 * dependency isn't a hop-graph node — this module has no basis to reason about
 * it) and reported distinctly via `Triage.dependsOnMetrics` instead.
 */
export function triageCrossTable(args: {
  metricName: string; sigmaFormula: string; rawDax: string; homeTable: string;
  refs: string[]; columnOwners: Record<string, string[]>; relationships: Rel[];
  maxDepth?: number; metricRefs?: string[];
}): Triage {
  const { metricName, sigmaFormula, rawDax, homeTable, refs, columnOwners, relationships } = args;
  // Default 3, not 2 (raised after re-measuring the `no-covering-View` bucket on
  // R1-R4): 9 of its 32 measures are `CALCULATE(agg, DIM[attr] = value)` shapes
  // whose filtered dimension is reachable from the aggregate's own fact at 3 hops,
  // not 2. Raising the default costs nothing on the grain side — coverage and
  // grain are independent concerns here. `reachableTables` is a monotonic BFS: a
  // ref that already resolved at <= 2 hops keeps that exact hop distance no
  // matter how far `maxDepth` reaches past it, so an existing covered candidate's
  // grain verdict (T4a-T4n, T8d-T8g) cannot change. The only thing depth 3 adds is
  // NEW candidates for refs that were previously unreachable at any hop — each
  // judged by the SAME grain rule as any other candidate, so a genuinely
  // cross-hop column inside an aggregate's summand is still `fanout-risk` at hop
  // 3 exactly as it would be at hop 1 or 2 (T4q). See T4p/T4q.
  const maxDepth = args.maxDepth ?? 3;
  const metricRefSet = new Set(args.metricRefs ?? []);
  const dependsOnMetrics = [...new Set(refs.filter((r) => metricRefSet.has(r)))];
  const columnRefs = refs.filter((r) => !metricRefSet.has(r));
  const shell: Triage = {
    metric: metricName, homeTable, refs, neverHostable: false,
    candidates: [], reachability: 'none', dependsOnMetrics,
  };

  if (isNeverHostable(rawDax)) return { ...shell, neverHostable: true };

  // Fail CLOSED on a formula we cannot confidently tokenize. Computed once —
  // it depends only on sigmaFormula, not on which base is being considered.
  const malformed = !isWellFormedFormula(sigmaFormula);

  // Only tables with an outgoing relationship get a View element built for them.
  const bases = [...new Set(relationships.map((r) => r.from))].sort();
  const candidates: Candidate[] = [];

  for (const b of bases) {
    const reach = reachableTables(b, relationships, maxDepth);

    // coverage: every reference must resolve somewhere on this View
    const hopOf = (ref: string): number => {
      let hop = Infinity;
      // `columnOwners` is a caller-supplied plain object — callers in this codebase
      // and in tests pass object literals (`{}`), not `Object.create(null)` maps. A
      // ref named `toString`/`constructor`/etc. then resolves to an INHERITED
      // Object.prototype/Function.prototype member, which is truthy but not an
      // array — `for...of` over it throws "not iterable". Guard with
      // `Array.isArray` rather than trusting truthiness, so this module stays safe
      // against any plain-object `columnOwners`, not just carefully-built ones.
      const owners = columnOwners[ref];
      for (const o of (Array.isArray(owners) ? owners : [])) {
        const h = reach.get(o);
        if (h !== undefined && h < hop) hop = h;
      }
      return hop;
    };
    let covered = true, maxHop = 0;
    for (const ref of columnRefs) {   // metric refs excluded — see doc comment above
      const hop = hopOf(ref);
      if (hop === Infinity) { covered = false; break; }
      if (hop > maxHop) maxHop = hop;
    }
    if (!covered) continue;

    if (malformed) {
      // Never "safe" on unparseable input — no grain analysis is attempted.
      candidates.push({ baseTable: b, maxHop, verdict: 'fanout-risk', unsafeRefs: ['malformed-formula'] });
      continue;
    }

    // grain: every aggregate call must range over the BASE's rows. Its summand
    // must contain a hop-0 column (pinning it to base grain) and no hop->=1
    // column (which would be duplicated by the join, or omitted by it).
    const unsafeRefs: string[] = [];
    for (const call of enumerateAggregateCalls(sigmaFormula)) {
      const summand = aggregateSummand(call.name, call.operand);
      // Mask string literals before hunting for `[ref]` brackets — a comparison
      // value like `"Weird[X]"` must not be read as a reference to column X.
      // A sibling metric ref inside an aggregate's summand (unusual, but not
      // impossible — e.g. `Sum([Other Metric])`) is excluded here too: it isn't
      // a hop-graph node, so it must not be judged cross-hop just because
      // hopOf() can't resolve it.
      const sRefs = summand === null
        ? []
        : (maskStringLiterals(summand).match(/\[([^\]]+)\]/g) || [])
            .map((s) => s.slice(1, -1))
            .filter((r) => !metricRefSet.has(r));
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

/** Render a Triage as the operator-facing suffix appended to the drop warning. */
export function describeTriage(t: Triage): string {
  const msg = describeVerdict(t);
  // A sibling-metric dependency is orthogonal to the column-coverage verdict
  // above (safe/fanout-risk/ambiguous/no-covering-View all still describe the
  // measure's COLUMN references only — see triageCrossTable's doc comment) —
  // note it distinctly rather than silently folding it into whichever bucket
  // the column refs landed in, so an operator re-homing this measure knows the
  // OTHER metric would need to be re-homed too.
  const deps = t.dependsOnMetrics || [];
  if (deps.length) {
    return `${msg} Also depends on metric${deps.length === 1 ? '' : 's'} ${deps.map((d) => `"${d}"`).join(', ')} — re-homing this measure requires re-homing ${deps.length === 1 ? 'that one' : 'those too'}.`;
  }
  return msg;
}

function describeVerdict(t: Triage): string {
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
    // "2" was a hardcoded echo of triageCrossTable's default maxDepth, which is a
    // parameter (`triageCrossTable({ maxDepth, ... })`), not always 2 — a caller
    // passing a different depth (e.g. T4f's maxDepth: 1) would get a message
    // naming the wrong number. Triage itself doesn't carry the depth it was
    // computed with, so say "the configured depth" rather than assume the default.
    return `TRIAGE: no View covers it within the configured depth (references: ${t.refs.join(', ')}).`;
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

/**
 * True iff `describeVerdict` would render the "no View covers it within the
 * configured depth" message for `t` — i.e. `t` is not `neverHostable` and has
 * NO covering candidates at all (the malformed-formula branch is unreachable
 * here: it requires `t.candidates.length` to be truthy, so it can never fire
 * when the candidate list is empty).
 *
 * A single source of truth for that one branch, exported so a caller can ask
 * "did triageCrossTable actually find nothing to say about this ref?" without
 * re-deriving `describeVerdict`'s branch order (and risking drift from it) or
 * — worse — string-matching its rendered text. Exists for exactly one caller:
 * `powerbi.ts`'s cross-table drop site, which needs to know whether a
 * `MetricBlocker` message (see below) is BETTER information than what
 * `triageCrossTable` itself produced, or WORSE. `triageCrossTable` must
 * always run first and its verdict must always win when it has one — a
 * `MetricBlocker` may only replace "nothing to say" (see that type's own
 * doc comment for why a bypass, checked BEFORE `triageCrossTable` ran at
 * all, was wrong: a ref that happens to share a name with some other
 * element's metric can ALSO be a real, reachable column, and `safe` or
 * `fanout-risk` is always more informative than a name-collision guess).
 */
export function isNoCoveringView(t: Triage): boolean {
  return !t.neverHostable && t.candidates.length === 0;
}

/**
 * A dropped measure's "bad" ref is not always a column-reachability question.
 * `columnOwners` (built only from `model.tables[].columns`) has no entry for a
 * METRIC name, so a ref that names one resolves to hop `Infinity` on every
 * candidate — exactly like a genuinely disconnected column — and
 * `triageCrossTable` has no way to tell the two apart on its own. When
 * `triageCrossTable` comes back with NOTHING to say (`isNoCoveringView`
 * above), and the caller (which HAS the whole-model and cross-pass state this
 * per-call, side-effect-free module deliberately does not keep) can already
 * tell the ref is one of these two NAME shapes, the caller should report the
 * REAL blocker instead of the generic "no View covers it" — surfacing that
 * generic message as the reason for 15 of 32 `no-covering-View` measures
 * (R1-R4 spike) that were never a reachability problem in the first place:
 *
 *   - `cross-element-metric`: the ref names a metric declared on a DIFFERENT
 *     element. Sigma metrics cannot reference another element's metric, at
 *     any join distance — a hard constraint, not a reachability gap. No
 *     `maxDepth` will ever change this verdict, unlike an ordinary
 *     column-coverage failure.
 *   - `dropped-sibling`: the ref names a SAME-element metric that was itself
 *     already dropped, in an earlier pass of the caller's own multi-pass drop
 *     loop, for a reason that has nothing to do with THIS measure's own
 *     columns (fan-out risk, never-hostable, or anything else). This
 *     measure's real blocker is that dependency, so its message quotes the
 *     sibling's own `describeTriage`/`describeMetricBlocker` text VERBATIM —
 *     reusing the already-verified wording is simpler and more trustworthy
 *     than re-deriving a paraphrase, and it lets a cascade of blockers chain
 *     without losing information at each hop.
 *
 * CHECKED ONLY AFTER `triageCrossTable` RUNS, AND ONLY WHEN IT FOUND NOTHING —
 * this used to be a bypass checked BEFORE `triageCrossTable` ran at all, which
 * was wrong: a ref can happen to share a literal NAME with some other
 * element's metric while ALSO being a real, reachable COLUMN (a plain name
 * collision — two unrelated things sharing a string). `triageCrossTable`
 * already resolves that correctly via `columnOwners` — a bypass on the name
 * alone reported a perfectly re-homable `safe` (or an accurately-flagged
 * `fanout-risk`) measure as a permanently-unfixable metric conflict. Running
 * `triageCrossTable` FIRST and asking `isNoCoveringView` afterward makes its
 * verdict authoritative whenever it has one: `safe`/`fanout-risk`/`ambiguous`/
 * `never-hostable`/malformed are all strictly more informative than a
 * name-based guess, and only WIN over silence (`isNoCoveringView`) should a
 * `MetricBlocker` message ever replace it.
 *
 * Deliberately NOT a variant folded into `Triage`/`describeTriage` even so:
 * both `MetricBlocker` cases are about a NAME the caller alone can resolve
 * (whole-model metric ownership, cross-pass drop history) that `Triage` has
 * no way to compute for itself from a single call's arguments.
 */
export type MetricBlocker =
  | { kind: 'cross-element-metric'; metric: string; ownerTable: string }
  | { kind: 'dropped-sibling'; metric: string; siblingReason: string };

/** Render a `MetricBlocker` as the operator-facing suffix appended to the drop warning. */
export function describeMetricBlocker(b: MetricBlocker): string {
  if (b.kind === 'cross-element-metric') {
    return `TRIAGE: references metric "${b.metric}", which is declared on a DIFFERENT element ` +
      `("${b.ownerTable}") — Sigma metrics cannot reference another element's metric, at any join ` +
      `distance; no hop limit fixes this. Recreate the dependency as a workbook-level calculation, ` +
      `or duplicate "${b.metric}" onto this element.`;
  }
  return `TRIAGE: depends on sibling metric "${b.metric}", which was itself dropped — its own drop ` +
    `reason: ${b.siblingReason} That dependency, not column reachability, is this measure's real ` +
    `blocker; resolve "${b.metric}" first, or rewrite this measure without it.`;
}
