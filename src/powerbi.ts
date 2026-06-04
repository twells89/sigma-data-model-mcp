/**
 * Power BI Model (.bim / TOM JSON) → Sigma Data Model JSON converter.
 *
 * Handles:
 * - Tables → Sigma elements with warehouse paths (extracted from M expressions)
 * - DAX measures → Sigma metrics with formula conversion
 * - DAX calculated columns → Sigma calculated columns
 * - Relationships (fromTable=many → toTable=one) → Sigma relationships
 * - Measures-only tables → measures moved to fact element
 * - Display folders → Sigma folders
 * - Cross-element column references → auto-rewrite with [SRC/REL_NAME/Field] form
 * - Calculation groups → derived metric stubs per base measure × calc item
 */

import {
  resetIds, sigmaShortId, sigmaInodeId, sigmaDisplayName, inferSigmaFormat,
  buildDerivedElements,
  type SigmaElement, type SigmaColumn, type ConversionResult,
} from './sigma-ids.js';

// ── Community article links for warnings ──────────────────────────────────────

const PBI_COMMUNITY_LINKS = {
  lod: 'community.sigmacomputing.com/t/tableau-level-of-detail-or-lod-calculations-in-sigma/6427',
  groupings: 'community.sigmacomputing.com/t/how-to-use-groupings-aggregate-calculations/2003',
  rollup: 'community.sigmacomputing.com/t/rollup-perform-aggregate-calculations-across-a-group-of-values-without-using-a-group-by/4367',
  biDiffs: 'community.sigmacomputing.com/t/sigma-differences-from-other-bi-tools-overview-for-new-sigma-creators/3285',
  leveled: 'community.sigmacomputing.com/t/how-to-implement-complex-leveled-aggregations-in-sigma-lods-dax/5203',
  pop: 'community.sigmacomputing.com/t/which-logic-to-use-for-period-over-period-comparisons/3206',
};

// ── DAX → Sigma Formula Converter ─────────────────────────────────────────────

// Split the top-level (depth-1) comma-separated arguments of a DAX/Sigma
// function call. `startIdx` must point at the first char AFTER the opening
// paren. Returns { args, endPos } where endPos is the index just past the
// matching closing paren (so the caller can splice it out). Paren-, bracket-
// and quote-aware so nested calls / [refs] / "strings" don't fool the split.
function splitCallArgs(s: string, startIdx: number): { args: string[]; endPos: number } {
  const args: string[] = [];
  let depth = 1, argStart = startIdx, i = startIdx;
  let inStr: string | null = null;
  for (; i < s.length; i++) {
    const ch = s[i];
    if (inStr) { if (ch === inStr) inStr = null; continue; }
    if (ch === '"' || ch === "'") { inStr = ch; continue; }
    if (ch === '(' || ch === '[') depth++;
    else if (ch === ')' || ch === ']') {
      depth--;
      if (depth === 0) { args.push(s.slice(argStart, i).trim()); i++; break; }
    } else if (ch === ',' && depth === 1) {
      args.push(s.slice(argStart, i).trim());
      argStart = i + 1;
    }
  }
  return { args, endPos: i };
}

// DAX DATEDIFF(start, end, UNIT) -> Sigma DateDiff("unit", start, end):
// quoted lowercased unit FIRST, then start, then end. Nested-paren aware so
// DATEDIFF(a, IF(...), DAY) reorders correctly. MUST run before the generic
// `DATEDIFF(` -> `DateDiff(` rename so it claims the DAX-ordered form.
// (beads-sigma-f0p)
function rewriteDateDiff(f: string): string {
  // Scan forward from a moving cursor so we never re-parse our own emitted
  // `DateDiff(...)` output (the regex is case-insensitive, so re-matching the
  // mixed-case result would scramble args). Match only the DAX-ordered form.
  const re = /\bDATEDIFF\s*\(/gi;
  let cursor = 0;
  for (let guard = 0; guard < 200; guard++) {
    re.lastIndex = cursor;
    const m = re.exec(f);
    if (!m) break;
    const openIdx = m.index + m[0].length;
    const { args, endPos } = splitCallArgs(f, openIdx);
    if (args.length < 3) { cursor = openIdx; continue; } // malformed -> skip
    const start = args[0];
    const end = args[1];
    const unit = args[2].replace(/^\[|\]$/g, '').trim().toLowerCase();
    const replacement = `DateDiff("${unit}", ${start}, ${end})`;
    f = f.slice(0, m.index) + replacement + f.slice(endPos);
    cursor = m.index + replacement.length; // resume AFTER the emitted form
  }
  return f;
}

// DAX WEEKNUM(date[, return_type]) -> Sigma week-of-year formula.
// IMPORTANT: Sigma's native DatePart("week",...)/DATE_PART('week',...) is ISO
// (week containing the first Thursday = week 1) and DIVERGES from DAX WEEKNUM at
// year boundaries (e.g. WEEKNUM('2021-01-01',2)=1 but ISO=53;
// WEEKNUM('2019-12-30',2)=53 but ISO=1). DAX WEEKNUM uses the Excel/US convention:
// the week containing Jan 1 is week 1, and the count increments at each week-start
// boundary. So we synthesize the Excel-style formula explicitly:
//   floor( (dayOfYear-1 + offsetOfJan1) / 7 ) + 1
// where dayOfYear-1 = DateDiff("day", DateTrunc("year",d), d) and offsetOfJan1 is
// the position of Jan 1 within its week (0 = the week-start day).
//   return_type 2 (Monday-start):  offset = Mod(Weekday(jan1)+5, 7)  [Mon=0..Sun=6]
//   return_type 1/default (Sunday): offset = Mod(Weekday(jan1)+6, 7)  [Sun=0..Sat=6]
// Sigma Weekday() returns 1=Sunday..7=Saturday. Validated EXACT vs PBI WEEKNUM(d,2)
// on 9 boundary dates incl. 2019-12-30, 2020-12-31, 2021-01-01 (the year-boundary
// cases where the naive DatePart("week") mapping is WRONG). (beads-sigma-a8h)
function rewriteWeeknum(f: string): string {
  const re = /\bWEEKNUM\s*\(/gi;
  let cursor = 0;
  for (let guard = 0; guard < 200; guard++) {
    re.lastIndex = cursor;
    const m = re.exec(f);
    if (!m) break;
    const openIdx = m.index + m[0].length;
    const { args, endPos } = splitCallArgs(f, openIdx);
    if (args.length < 1) { cursor = openIdx; continue; } // malformed -> skip
    const dateArg = args[0].trim();
    // return_type: DAX defaults to 1 (Sunday-start). Type 2 = Monday-start.
    const rt = args.length >= 2 ? args[1].replace(/^\[|\]$/g, '').trim() : '1';
    // Sunday-start offset = +6, Monday-start offset = +5 (mod 7).
    const off = rt === '2' ? 5 : 6;
    const yearStart = `DateTrunc("year", ${dateArg})`;
    const replacement =
      `Floor((DateDiff("day", ${yearStart}, ${dateArg}) + Mod(Weekday(${yearStart}) + ${off}, 7)) / 7) + 1`;
    f = f.slice(0, m.index) + replacement + f.slice(endPos);
    cursor = m.index + replacement.length; // resume AFTER the emitted form
  }
  return f;
}

// SWITCH(TRUE(), c1, v1, c2, v2, ..., [default]) -> nested ternary Ifs:
//   If(c1, v1, If(c2, v2, ... [, default])). Sigma's If is strictly ternary,
//   so a flat If(c1, v1, c2, v2, default) is malformed. (beads-sigma-n9u)
// Scans for the DAX form on the RAW expression (before generic renames) so the
// pairs split cleanly, then recurses pair-by-pair. Paren/quote-aware.
function rewriteSwitchTrue(f: string): string {
  const re = /\bSWITCH\s*\(\s*TRUE\s*\(\s*\)\s*,/gi;
  for (let guard = 0; guard < 200; guard++) {
    re.lastIndex = 0;
    const m = re.exec(f);
    if (!m) break;
    const openIdx = m.index + m[0].length;
    const { args, endPos } = splitCallArgs(f, openIdx);
    if (args.length < 2) break; // malformed -> leave for generic Switch rename
    // args = [c1, v1, c2, v2, ..., (optional default)]
    const hasDefault = args.length % 2 === 1;
    const def = hasDefault ? args[args.length - 1] : null;
    const pairCount = Math.floor(args.length / 2);
    let nested = def !== null ? def : 'null';
    for (let p = pairCount - 1; p >= 0; p--) {
      const cond = args[p * 2];
      const val = args[p * 2 + 1];
      nested = `If(${cond}, ${val}, ${nested})`;
    }
    f = f.slice(0, m.index) + nested + f.slice(endPos);
  }
  return f;
}

// COUNTROWS(FILTER(ALL(T) | T, <part-eq>* && T[x] > EARLIER(T[x]))) + 1
//   -> RankDense([x], "desc"[, partition]). This is the canonical DAX rank
//   idiom for a calculated column. The EARLIER(T[x]) is the current row's x;
//   counting rows whose x is greater, +1, == dense descending rank. A leading
//   T[p] = EARLIER(T[p]) predicate scopes the rank to a partition.
//   (beads-sigma-3t9)
function rewriteEarlierRank(f: string): string {
  const re = /\bCOUNTROWS\s*\(\s*FILTER\s*\(/gi;
  for (let guard = 0; guard < 50; guard++) {
    re.lastIndex = 0;
    const m = re.exec(f);
    if (!m) break;
    // openIdx is just past FILTER's "(" — splitCallArgs gives FILTER's args.
    const filterOpen = m.index + m[0].length;
    const { args: filterArgs, endPos: filterEnd } = splitCallArgs(f, filterOpen);
    if (filterArgs.length < 2) break;
    // The COUNTROWS wrapper's own close-paren is right after FILTER's close.
    // Find it, then look for a trailing "+ 1".
    let j = filterEnd;
    while (j < f.length && /\s/.test(f[j])) j++;
    if (f[j] !== ')') break; // not the shape we expect
    let after = j + 1;
    const tail = f.slice(after).match(/^\s*\+\s*1\b/);
    if (!tail) break;
    const fullEnd = after + tail[0].length;
    // Parse the predicate (everything after the table arg, joined).
    const pred = filterArgs.slice(1).join(', ');
    // Find the EARLIER-comparison term: <ref> (>|<) EARLIER(<ref>)
    const cmp = pred.match(/(['"]?[\w ]*'?\[[^\]]+\]|\[[^\]]+\])\s*(>|<)\s*EARLIER\s*\(\s*([^)]+?)\s*\)/i);
    if (!cmp) break;
    const rankRefRaw = cmp[1];
    const dir = cmp[2] === '>' ? 'desc' : 'asc';
    // Partition predicates: any <ref> = EARLIER(<ref>) terms (split on &&).
    const partRefs: string[] = [];
    for (const term of pred.split(/&&/)) {
      const pm = term.match(/(['"]?[\w ]*'?\[[^\]]+\]|\[[^\]]+\])\s*=\s*EARLIER\s*\(\s*[^)]+?\s*\)/i);
      if (pm) partRefs.push(pm[1].trim());
    }
    const bare = (x: string) => x
      .replace(/'[^']+'\[([^\]]+)\]/g, '[$1]')
      .replace(/\b[A-Za-z_]\w*\[([^\]]+)\]/g, '[$1]')
      .trim();
    const rankRef = bare(rankRefRaw);
    let replacement = `RankDense(${rankRef}, "${dir}")`;
    if (partRefs.length) {
      const parts = partRefs.map(bare).join(', ');
      replacement = `RankDense(${rankRef}, "${dir}", ${parts})`;
    }
    f = f.slice(0, m.index) + replacement + f.slice(fullEnd);
  }
  return f;
}

// DAX statistical iterators that have clean Sigma equivalents (beads-sigma-9l2).
//   MEDIANX(t, e)            -> Median(e)
//   PERCENTILEX.INC(t, e, k) -> PercentileCont(e, k)      (NOT PercentileInc)
//   STDEVX.P(t, e)           -> Sqrt(VariancePop(e))      (no StdDevP in Sigma)
//   VARX.P(t, e)             -> VariancePop(e)
//   GEOMEANX(t, e)           -> Exp(Avg(Ln(e)))
// The table arg is dropped (Sigma aggregates over element rows / grouping).
function rewriteStatIterators(f: string): string {
  const specs: { re: RegExp; build: (a: string[]) => string | null }[] = [
    { re: /\bMEDIANX\s*\(/i,            build: a => a.length >= 2 ? `Median(${a[1]})` : null },
    { re: /\bPERCENTILEX\.INC\s*\(/i,  build: a => a.length >= 3 ? `PercentileCont(${a[1]}, ${a[2]})` : null },
    { re: /\bPERCENTILEX\.EXC\s*\(/i,  build: a => a.length >= 3 ? `PercentileCont(${a[1]}, ${a[2]})` : null },
    { re: /\bSTDEVX\.P\s*\(/i,         build: a => a.length >= 2 ? `Sqrt(VariancePop(${a[1]}))` : null },
    { re: /\bSTDEVX\.S\s*\(/i,         build: a => a.length >= 2 ? `Sqrt(Variance(${a[1]}))` : null },
    { re: /\bVARX\.P\s*\(/i,           build: a => a.length >= 2 ? `VariancePop(${a[1]})` : null },
    { re: /\bVARX\.S\s*\(/i,           build: a => a.length >= 2 ? `Variance(${a[1]})` : null },
    { re: /\bGEOMEANX\s*\(/i,           build: a => a.length >= 2 ? `Exp(Avg(Ln(${a[1]})))` : null },
  ];
  for (const spec of specs) {
    for (let guard = 0; guard < 50; guard++) {
      const reG = new RegExp(spec.re.source, 'gi');
      reG.lastIndex = 0;
      const m = reG.exec(f);
      if (!m) break;
      const { args, endPos } = splitCallArgs(f, m.index + m[0].length);
      const rep = spec.build(args);
      if (rep === null) break;
      f = f.slice(0, m.index) + rep + f.slice(endPos);
    }
  }
  return f;
}

// COMBINEVALUES(sep, a, b, ...) -> [a] & sep & [b] & sep & ... (beads-sigma-9l2)
function rewriteCombineValues(f: string): string {
  const re = /\bCOMBINEVALUES\s*\(/gi;
  for (let guard = 0; guard < 50; guard++) {
    re.lastIndex = 0;
    const m = re.exec(f);
    if (!m) break;
    const { args, endPos } = splitCallArgs(f, m.index + m[0].length);
    if (args.length < 2) break;
    const sep = args[0];
    const vals = args.slice(1);
    const joined = vals.join(` & ${sep} & `);
    f = f.slice(0, m.index) + joined + f.slice(endPos);
  }
  return f;
}

// IF(HASONEVALUE(col), SELECTEDVALUE(col), default) and standalone
//   HASONEVALUE / SELECTEDVALUE. (beads-sigma-9l2)
//   HASONEVALUE(col)      -> CountDistinct(col) = 1
//   SELECTEDVALUE(col[,d]) -> If(CountDistinct(col) = 1, Min(col), d|null)
// Applied on RAW DAX before generic renames so the col refs are intact.
function rewriteSingleValue(f: string): string {
  // Collapse the common idiom IF(HASONEVALUE(c), SELECTEDVALUE(c[,d]), def)
  //   -> If(CountDistinct(c) = 1, Min(c), def) — matches the spec's canonical
  //   single-value form without a redundant nested CountDistinct check.
  {
    const re = /\bIF\s*\(\s*HASONEVALUE\s*\(/gi;
    for (let guard = 0; guard < 50; guard++) {
      re.lastIndex = 0;
      const m = re.exec(f);
      if (!m) break;
      const ifOpen = m.index + 'IF('.length; // index just past the outer IF(
      const { args, endPos } = splitCallArgs(f, ifOpen);
      if (args.length < 3) break;
      const hovM = args[0].match(/^\s*HASONEVALUE\s*\(/i);
      const svM = args[1].match(/^\s*SELECTEDVALUE\s*\(/i);
      if (!hovM || !svM) break;
      const hovArgs = splitCallArgs(args[0], hovM.index! + hovM[0].length).args;
      const svArgs = splitCallArgs(args[1], svM.index! + svM[0].length).args;
      if (hovArgs.length < 1 || svArgs.length < 1) break;
      const col = svArgs[0]; // value column from SELECTEDVALUE
      const def = args[2];
      const rep = `If(CountDistinct(${col}) = 1, Min(${col}), ${def})`;
      f = f.slice(0, m.index) + rep + f.slice(endPos);
    }
  }
  // SELECTEDVALUE(col, default?) -> If(CountDistinct(col)=1, Min(col), default)
  {
    const re = /\bSELECTEDVALUE\s*\(/gi;
    for (let guard = 0; guard < 50; guard++) {
      re.lastIndex = 0;
      const m = re.exec(f);
      if (!m) break;
      const { args, endPos } = splitCallArgs(f, m.index + m[0].length);
      if (args.length < 1) break;
      const col = args[0];
      const def = args.length >= 2 ? args[1] : 'null';
      const rep = `If(CountDistinct(${col}) = 1, Min(${col}), ${def})`;
      f = f.slice(0, m.index) + rep + f.slice(endPos);
    }
  }
  // HASONEVALUE(col) -> CountDistinct(col) = 1
  {
    const re = /\bHASONEVALUE\s*\(/gi;
    for (let guard = 0; guard < 50; guard++) {
      re.lastIndex = 0;
      const m = re.exec(f);
      if (!m) break;
      const { args, endPos } = splitCallArgs(f, m.index + m[0].length);
      if (args.length < 1) break;
      const rep = `CountDistinct(${args[0]}) = 1`;
      f = f.slice(0, m.index) + rep + f.slice(endPos);
    }
  }
  return f;
}

// COUNTROWS(FILTER(table, pred)) | COUNT(FILTER(table, pred)) -> CountIf(pred).
// The BARE form (no CALCULATE wrapper) otherwise reaches the COUNTROWS catch-all
// (/\bCOUNTROWS\s*\(\s*'?[^)]*'?\s*\)/), whose [^)]* stops at FILTER's inner ')'
// and leaves the outer paren dangling -> malformed 'Count())' that fails the DM
// POST (beads-sigma-r9oz). Run in Tier 0, before the catch-all. Predicate column
// refs are normalized to bare [Col] so downstream name-mapping resolves them.
function rewriteCountRowsFilter(f: string): string {
  const re = /\b(?:COUNTROWS|COUNT)\s*\(/gi;
  for (let guard = 0; guard < 50; guard++) {
    re.lastIndex = 0;
    let replaced = false;
    let m: RegExpExecArray | null;
    while ((m = re.exec(f)) !== null) {
      const { args, endPos } = splitCallArgs(f, m.index + m[0].length);
      if (args.length !== 1) continue;
      const inner = args[0].trim();
      const fm = inner.match(/^FILTER\s*\(/i);
      if (!fm) continue;
      const fr = splitCallArgs(inner, fm[0].length);
      if (fr.args.length < 2) continue;
      let pred = fr.args.slice(1).join(', ').trim();
      pred = pred
        .replace(/'[^']+'\[([^\]]+)\]/g, '[$1]')
        .replace(/\b[A-Za-z_]\w*\[([^\]]+)\]/g, '[$1]');
      f = f.slice(0, m.index) + `CountIf(${pred})` + f.slice(endPos);
      replaced = true;
      break;
    }
    if (!replaced) break;
  }
  return f;
}

// Drop any metric whose formula references a MEASURE that was itself dropped
// (a CALCULATE/iterator/ranking measure that didn't translate) — e.g. a ratio
// built on it. Without this the dependent metric posts but silently resolves to
// "Missing Metric". `droppedNames` is seeded with the source measures that did
// NOT make it into `metrics`; pruned metrics are added back so transitive chains
// (A→B→droppedC) collapse too. Scoped to dropped MEASURE names ONLY — column
// refs and surviving measures are never touched. (dangling-ref cascade)
function pruneDanglingMetrics(metrics: any[], droppedNames: Set<string>, warnings: string[] | null): void {
  for (let pass = 0; pass < 10; pass++) {
    const before = metrics.length;
    for (let i = metrics.length - 1; i >= 0; i--) {
      const refs = (String(metrics[i].formula).match(/\[([^\]\/]+)\]/g) || []).map((r) => r.slice(1, -1));
      const bad = refs.find((r) => droppedNames.has(r));
      if (bad) {
        if (warnings) warnings.push(`⚠ "${metrics[i].name}": references "[${bad}]" which did not translate — dropped to avoid a dangling reference.`);
        droppedNames.add(metrics[i].name);
        metrics.splice(i, 1);
      }
    }
    if (metrics.length === before) break;
  }
}

export function pbiDaxToSigma(
  dax: string | string[],
  warnings: string[] | null,
  measureName: string
): string | null {
  // BIM/TMSL serializes multi-line DAX expressions as a string[] (one entry per line)
  if (Array.isArray(dax)) dax = dax.join('\n');
  if (typeof dax !== 'string' || !dax.trim()) return null;
  let f = dax.trim();

  // ── Tier 0: high-value DAX idioms with clean Sigma equivalents ──
  // Run on the RAW expression BEFORE the structural-warning guards and the
  // generic renames, so these forms translate instead of being dropped to a
  // warning (or shipped as a raw error column). (beads-sigma-9l2 / 3t9 / n9u)
  f = rewriteEarlierRank(f);    // COUNTROWS(FILTER(ALL,..EARLIER..))+1 -> RankDense
  f = rewriteStatIterators(f);  // MEDIANX/PERCENTILEX.INC/STDEVX.P/VARX.P/GEOMEANX
  f = rewriteCombineValues(f);  // COMBINEVALUES(sep,a,b) -> [a] & sep & [b]
  f = rewriteSingleValue(f);    // HASONEVALUE / SELECTEDVALUE
  f = rewriteSwitchTrue(f);     // SWITCH(TRUE(), c,v,...) -> nested If
  f = rewriteCountRowsFilter(f);// COUNTROWS/COUNT(FILTER(t,pred)) -> CountIf(pred) (r9oz)
  // DISTINCTCOUNTNOBLANK(col) -> CountDistinct(col) (Sigma CountDistinct already
  // ignores nulls). Done here so the generic DISTINCTCOUNT rename can't first
  // claim the prefix and leave a dangling NOBLANK token.
  f = f.replace(/\bDISTINCTCOUNTNOBLANK\s*\(/gi, 'CountDistinct(');

  // ── Tier 4: Structural patterns → warnings only ──
  // CALCULATE with ALL/ALLEXCEPT/REMOVEFILTERS
  if (/\bCALCULATE\s*\(/i.test(f) && /\b(ALL|ALLEXCEPT|REMOVEFILTERS|ALLSELECTED)\s*\(/i.test(f)) {
    if (warnings) warnings.push(`⚠ "${measureName}": uses CALCULATE with filter context manipulation. In Sigma, use groupings. See: ${PBI_COMMUNITY_LINKS.leveled}`);
    return null;
  }
  // Iterator functions
  if (/\b(SUMX|AVERAGEX|MINX|MAXX|COUNTAX|CONCATENATEX)\s*\(/i.test(f)) {
    const fn = f.match(/\b(SUMX|AVERAGEX|MINX|MAXX|COUNTAX|CONCATENATEX)/i)![1];
    if (warnings) warnings.push(`⚠ "${measureName}": uses DAX iterator (${fn}). Use groupings or calculated columns. See: ${PBI_COMMUNITY_LINKS.groupings}`);
    return null;
  }
  // Ranking functions — window/scope; no DM-metric equivalent. Emitting RANKX
  // verbatim is an invalid Sigma formula that fails the whole DM POST
  // (beads-sigma-r9oz/mkm). Drop-and-warn instead. (RANKX before RANK so the
  // alternation captures the full token.)
  if (/\b(RANKX|RANK\.EQ|RANK\.AVG|RANK)\s*\(/i.test(f)) {
    const fn = f.match(/\b(RANKX|RANK\.EQ|RANK\.AVG|RANK)/i)![1];
    if (warnings) warnings.push(`⚠ "${measureName}": uses DAX ranking (${fn}). No data-model-metric equivalent — add a workbook Rank() in an ordered table, or a grouped element. See: ${PBI_COMMUNITY_LINKS.groupings}`);
    return null;
  }
  // Time intelligence
  if (/\b(TOTALYTD|TOTALQTD|TOTALMTD|SAMEPERIODLASTYEAR|DATEADD|DATESYTD|PARALLELPERIOD|PREVIOUSMONTH|PREVIOUSQUARTER|PREVIOUSYEAR)\s*\(/i.test(f)) {
    const fn = f.match(/\b(TOTALYTD|TOTALQTD|TOTALMTD|SAMEPERIODLASTYEAR|DATEADD|DATESYTD|PARALLELPERIOD|PREVIOUSMONTH|PREVIOUSQUARTER|PREVIOUSYEAR)/i)![1];
    if (warnings) warnings.push(`⚠ "${measureName}": uses DAX time intelligence (${fn}). Use Period over Period feature. See: ${PBI_COMMUNITY_LINKS.pop}`);
    return null;
  }
  // CALCULATE without ALL (single-predicate filter)
  // DAX: CALCULATE(<agg>(<col-or-table>), <predicate>)
  //   <agg>  = SUM/AVERAGE/MIN/MAX/COUNT/COUNTROWS/DISTINCTCOUNT
  //   <predicate> = a row-level boolean: TABLE[col] = "v" | [col] > 100000 |
  //                 [col] = TRUE() | FILTER(table, <predicate>)
  // SumIf/AvgIf/MinIf/MaxIf/CountDistinctIf take (col, predicate). But Sigma's
  // CountIf takes ONE logical arg: CountIf(<predicate>) — the 2-arg form errors
  // at query time. COUNTROWS / COUNT(table) -> CountIf(<predicate>). (beads-sigma-862)
  if (/\bCALCULATE\s*\(/i.test(f)) {
    const cm = f.match(/\bCALCULATE\s*\(/i);
    if (cm) {
      const { args } = splitCallArgs(f, cm.index! + cm[0].length);
      // exactly: [ aggExpr, predicate ]
      if (args.length === 2) {
        const aggExpr = args[0];
        let pred = args[1];
        const aggM = aggExpr.match(/^\s*(SUM|AVERAGE|MIN|MAX|COUNT|COUNTROWS|DISTINCTCOUNT)\s*\(([\s\S]*)\)\s*$/i);
        if (aggM) {
          const aggFn = aggM[1].toUpperCase();
          // inner ref: 'Table'[Col] | Table[Col] | [Col] | <table-name> (for COUNTROWS)
          const innerRaw = aggM[2].trim();
          // Unwrap a FILTER(table, predicate) wrapper to its predicate.
          const filterM = pred.match(/^\s*FILTER\s*\(/i);
          if (filterM) {
            const fr = splitCallArgs(pred, filterM.index! + filterM[0].length);
            if (fr.args.length >= 2) pred = fr.args.slice(1).join(', ').trim();
          }
          // Normalize qualified col refs in BOTH the inner agg col and the
          // predicate to bare [Col] (downstream name-mapping keys on bare names).
          const bareRef = (x: string) =>
            x.replace(/'[^']+'\[([^\]]+)\]/g, '[$1]').replace(/\b[A-Za-z_]\w*\[([^\]]+)\]/g, '[$1]');
          pred = bareRef(pred);
          // Refuse predicates that compare a column to ANOTHER bracketed ref on
          // the RHS — e.g. FILTER(T, T[Salary] > [Company Avg Salary]). That RHS
          // is a measure/aggregate, not a row literal, so a row-level CountIf
          // would be wrong (needs a windowed compare). Bail to the warning.
          // (MANIFEST row 68 "Above Avg Earner Count" = category b.)
          const cmpRhs = pred.replace(/^[\s\S]*?(=|<>|!=|>=|<=|>|<)/, '').trim();
          if (/\[[^\]]+\]/.test(cmpRhs)) {
            if (warnings) warnings.push(`⚠ "${measureName}": CALCULATE filter compares against an aggregate/measure (${cmpRhs}). Needs a windowed comparison or grouping — add manually. See: ${PBI_COMMUNITY_LINKS.leveled}`);
            return null;
          }
          const isCountish = aggFn === 'COUNTROWS' || aggFn === 'COUNT';
          if (isCountish) {
            return `CountIf(${pred})`;
          }
          if (aggFn === 'DISTINCTCOUNT') {
            const col = bareRef(innerRaw);
            return `CountDistinctIf(${col}, ${pred})`;
          }
          const aggMap: Record<string, string> = { 'SUM': 'SumIf', 'AVERAGE': 'AvgIf', 'MIN': 'MinIf', 'MAX': 'MaxIf' };
          const sigmaFn = aggMap[aggFn] || 'SumIf';
          const col = bareRef(innerRaw);
          return `${sigmaFn}(${col}, ${pred})`;
        }
      }
    }
    if (warnings) warnings.push(`⚠ "${measureName}": complex CALCULATE expression. Use groupings. See: ${PBI_COMMUNITY_LINKS.leveled}`);
    return null;
  }
  // VAR/RETURN blocks
  if (/\bVAR\b/i.test(f) && /\bRETURN\b/i.test(f)) {
    if (warnings) warnings.push(`⚠ "${measureName}": uses DAX VAR/RETURN. Break into multiple calculated columns. See: ${PBI_COMMUNITY_LINKS.biDiffs}`);
    return null;
  }

  // ── Tier 1: Direct mappings ──

  // DATEDIFF(start, end, UNIT) -> DateDiff("unit", start, end). Run first on
  // the raw DAX so arg reordering + unit-quoting happen before bracket/table
  // normalization. (beads-sigma-f0p)
  f = rewriteDateDiff(f);
  // WEEKNUM -> Excel-style week-of-year formula (NOT ISO DatePart). (beads-sigma-a8h)
  f = rewriteWeeknum(f);

  // DIVIDE(a, b, alt) — nested-paren-aware parser
  const divideMatch = f.match(/\bDIVIDE\s*\(/i);
  if (divideMatch) {
    const startIdx = divideMatch.index! + divideMatch[0].length;
    const divArgs: string[] = [];
    let depth = 1, argStart = startIdx;
    for (let i = startIdx; i < f.length && depth > 0; i++) {
      if (f[i] === '(') depth++;
      else if (f[i] === ')') { depth--; if (depth === 0) { divArgs.push(f.slice(argStart, i).trim()); break; } }
      else if (f[i] === ',' && depth === 1) { divArgs.push(f.slice(argStart, i).trim()); argStart = i + 1; }
    }
    if (divArgs.length >= 2) {
      const num = divArgs[0], den = divArgs[1], alt = divArgs[2];
      let d2 = 1, endPos = startIdx;
      for (; endPos < f.length && d2 > 0; endPos++) {
        if (f[endPos] === '(') d2++;
        else if (f[endPos] === ')') d2--;
      }
      let replacement: string;
      if (alt && alt.trim()) {
        replacement = `If(${den} = 0, ${alt.trim()}, ${num} / ${den})`;
      } else {
        replacement = `${num} / ${den}`;
      }
      f = f.slice(0, divideMatch.index!) + replacement + f.slice(endPos);
    }
  }

  // Simple aggregates
  f = f.replace(/\bDISTINCTCOUNT\s*\(/gi, 'CountDistinct(');
  f = f.replace(/\bCOUNTROWS\s*\(\s*'?[^)]*'?\s*\)/gi, 'Count()');
  f = f.replace(/\bCOUNTA\s*\(/gi, 'CountIf(IsNotNull(');
  f = f.replace(/\bSUM\s*\(/gi, 'Sum(');
  f = f.replace(/\bAVERAGE\s*\(/gi, 'Avg(');
  f = f.replace(/\bMIN\s*\(/gi, 'Min(');
  f = f.replace(/\bMAX\s*\(/gi, 'Max(');
  f = f.replace(/\bCOUNT\s*\(/gi, 'Count(');
  // RELATED('table'[Col]) — the inner 'table'[Col] is normalized below to
  // [Col]; we strip the RELATED wrapper after that. The bare ref is
  // intentional: the post-conversion cross-element move pass detects calc
  // cols whose refs aren't local, pulls them off the source warehouse-table,
  // and places them on the derived "<Table> View" element with refs
  // rewritten to the triple form [SRC/REL/Col] — the only form Sigma
  // resolves for cross-element refs.
  const hadRelated = /\bRELATED\s*\(/i.test(f);
  if (hadRelated && warnings) {
    warnings.push(`ℹ Calculated column "${measureName}": uses RELATED() — column will be moved to a derived "<Table> View" element with cross-element refs rewritten to [SRC/REL/Col] form.`);
  }
  f = f.replace(/\bRELATEDTABLE\s*\([^)]*\)/gi, '/* RELATEDTABLE - use relationship */');
  // Logical
  f = f.replace(/\bIF\s*\(/gi, 'If(');
  // SWITCH(TRUE(), ...) is handled earlier by rewriteSwitchTrue (nested If).
  // The remaining SWITCH(value, k1, v1, ..., default) form maps to Sigma Switch.
  f = f.replace(/\bSWITCH\s*\(/gi, 'Switch(');
  f = f.replace(/\bISBLANK\s*\(/gi, 'IsNull(');
  f = f.replace(/\bCOALESCE\s*\(/gi, 'Coalesce(');
  f = f.replace(/\bBLANK\s*\(\s*\)/gi, 'null');
  f = f.replace(/\bNOT\s*\(/gi, 'Not(');
  f = f.replace(/\bTRUE\s*\(\s*\)/gi, 'True');
  f = f.replace(/\bFALSE\s*\(\s*\)/gi, 'False');
  f = f.replace(/&&/g, ' and ');
  f = f.replace(/\|\|/g, ' or ');
  // Text
  f = f.replace(/\bCONCATENATE\s*\(/gi, 'Concat(');
  f = f.replace(/\bLEN\s*\(/gi, 'Len(');
  f = f.replace(/\bUPPER\s*\(/gi, 'Upper(');
  f = f.replace(/\bLOWER\s*\(/gi, 'Lower(');
  f = f.replace(/\bTRIM\s*\(/gi, 'Trim(');
  f = f.replace(/\bLEFT\s*\(/gi, 'Left(');
  f = f.replace(/\bRIGHT\s*\(/gi, 'Right(');
  f = f.replace(/\bMID\s*\(/gi, 'Mid(');
  f = f.replace(/\bSUBSTITUTE\s*\(/gi, 'Replace(');
  f = f.replace(/\bFORMAT\s*\(/gi, 'DateFormat(');
  // Math
  f = f.replace(/\bABS\s*\(/gi, 'Abs(');
  f = f.replace(/\bROUND\s*\(/gi, 'Round(');
  f = f.replace(/\bINT\s*\(/gi, 'Int(');
  f = f.replace(/\bSQRT\s*\(/gi, 'Sqrt(');
  f = f.replace(/\bPOWER\s*\(/gi, 'Power(');
  // Date
  f = f.replace(/\bYEAR\s*\(/gi, 'Year(');
  f = f.replace(/\bMONTH\s*\(/gi, 'Month(');
  f = f.replace(/\bDAY\s*\(/gi, 'Day(');
  f = f.replace(/\bHOUR\s*\(/gi, 'Hour(');
  f = f.replace(/\bMINUTE\s*\(/gi, 'Minute(');
  f = f.replace(/\bSECOND\s*\(/gi, 'Second(');
  f = f.replace(/\bTODAY\s*\(\s*\)/gi, 'Today()');
  f = f.replace(/\bNOW\s*\(\s*\)/gi, 'Now()');
  f = f.replace(/\bDATE\s*\(/gi, 'MakeDate(');
  f = f.replace(/\bDATEDIFF\s*\(/gi, 'DateDiff(');

  // Clean up 'table'[column] → [column] (quoted table qualifier)
  // Collect unique table prefixes before [ to detect multi-table references
  const quotedTablePrefixes = (f.match(/'([^']+)'\[/g) || []).map(m => m.replace(/'\[$/g, '').replace(/^'/g, ''));
  const unquotedTablePrefixes = (f.match(/\b([A-Za-z_]\w*)\[/g) || []).map(m => m.replace(/\[$/, ''));
  const allTablePrefixes = [...new Set([...quotedTablePrefixes, ...unquotedTablePrefixes])].filter(p =>
    !/^(If|Switch|Not|And|Or|Sum|Avg|Min|Max|Count|CountIf|CountDistinct|CumulativeSum|Coalesce|Nullif|Round|Floor|Ceiling|Abs|Upper|Lower|Trim|Left|Right|Mid|Replace|Find|Len|Year|Month|Day|Hour|Minute|Second|Today|Now|MakeDate|DateDiff|DateAdd|DateTrunc|DateFormat|IsNull|IsNotNull|Int|Number|Text|Sqrt|Power|Concat|In|GrandTotal|CumulativeAvg|Weekday|Mod|DateTrunc)$/.test(p)
  );
  if (allTablePrefixes.length > 1 && warnings) {
    const tableNames = allTablePrefixes.join(', ');
    warnings.push(`⚠ Calculated column "${measureName}": references columns from multiple tables (${tableNames}). Column context has been simplified — verify formula references the correct columns.`);
  }
  f = f.replace(/'[^']+'\[([^\]]+)\]/g, '[$1]');
  // Also handle unquoted: Table[Column] → [Column]
  f = f.replace(/\b[A-Za-z_]\w*\[([^\]]+)\]/g, '[$1]');

  // Strip RELATED([col]) → [col] AFTER table-prefix normalization, so that
  // RELATED('dim'[X]) (which the line 121 regex couldn't match because of
  // the quoted prefix) gets unwrapped here.
  f = f.replace(/\bRELATED\s*\(\s*(\[[^\]]+\])\s*\)/gi, '$1');

  return f.trim();
}

// ── Extract table path from Power Query M expression ──────────────────────────

function pbiExtractPathFromM(mExpr: string): string[] | null {
  if (!mExpr) return null;

  // Pattern 1: explicit SQL Server / Azure connector with db arg
  // Sql.Database("server", "DATABASE")
  const sqlDbMatch = mExpr.match(/Sql\.Database\s*\(\s*"[^"]*"\s*,\s*"([^"]+)"/i);
  const schemaMatch = mExpr.match(/\{[^}]*\[Schema\s*=\s*"([^"]+)"\]/i)
    || mExpr.match(/\{[^}]*\[Name\s*=\s*"([^"]+)"\s*,\s*Kind\s*=\s*"Schema"\]/i);
  const tableKindMatch = mExpr.match(/\{[^}]*\[Name\s*=\s*"([^"]+)"\s*,\s*Kind\s*=\s*"Table"\]/i);

  if (sqlDbMatch && tableKindMatch) {
    const db = sqlDbMatch[1];
    const table = tableKindMatch[1];
    const schema = schemaMatch ? schemaMatch[1] : null;
    if (schema) return [db.toUpperCase(), schema.toUpperCase(), table.toUpperCase()];
    return [db.toUpperCase(), table.toUpperCase()];
  }

  // Pattern 2a: Kind-tagged navigation — Snowflake / Databricks / BigQuery / others.
  // Power BI's Snowflake connector emits navigation steps that carry an explicit Kind:
  //   Source{[Name = "CSA", Kind = "Database"]}[Data]
  //   #"Navigation 1"{[Name = "TJ", Kind = "Schema"]}[Data]
  //   #"Navigation 2"{[Name = "EMPLOYEES", Kind = "Table"]}[Data]
  // Key on the Kind so each segment maps to the right path slot regardless of order
  // (and tolerate arbitrary whitespace inside the record). Caller overrides still apply
  // later in convertPowerBIToSigma.
  const kindNavMatches = [...mExpr.matchAll(
    /\[\s*Name\s*=\s*"([^"]+)"\s*,\s*Kind\s*=\s*"(Database|Schema|Table|View)"\s*\]/gi
  )];
  if (kindNavMatches.length) {
    let db: string | null = null, sch: string | null = null, tbl: string | null = null;
    for (const m of kindNavMatches) {
      const kind = m[2].toLowerCase();
      if (kind === 'database') db = m[1];
      else if (kind === 'schema') sch = m[1];
      else if (kind === 'table' || kind === 'view') tbl = m[1];
    }
    if (tbl) {
      const parts = [db, sch, tbl].filter((s): s is string => !!s);
      if (parts.length >= 2) return parts.map(s => s.toUpperCase());
    }
  }

  // Pattern 2b: plain navigation by {[Name="X"]}[Data] (no Kind tag) —
  // older Snowflake/Databricks/BigQuery M, positional DB/Schema/Table order.
  // let DB     = Source{[Name="ANALYTICS"]}[Data]
  // let Schema = DB{[Name="PROD"]}[Data]
  // let Table  = Schema{[Name="SALES"]}[Data]
  const nameNavMatches = [...mExpr.matchAll(/\{\s*\[Name\s*=\s*"([^"]+)"\s*\]\s*\}\s*\[\s*Data\s*\]/gi)];
  if (nameNavMatches.length >= 3) {
    return [
      nameNavMatches[0][1].toUpperCase(),
      nameNavMatches[1][1].toUpperCase(),
      nameNavMatches[2][1].toUpperCase(),
    ];
  }
  if (nameNavMatches.length === 2) {
    return [nameNavMatches[0][1].toUpperCase(), nameNavMatches[1][1].toUpperCase()];
  }

  // Pattern 3: SQL query fallback
  const tblMatch = mExpr.match(/FROM\s+(?:\[?(\w+)\]?\.)?\[?(\w+)\]?\.\[?(\w+)\]?/i);
  if (tblMatch) {
    return [tblMatch[1] || '', tblMatch[2], tblMatch[3]].filter(Boolean).map((s: string) => s.toUpperCase());
  }

  return null;
}

// Translate a simple DAX ADDCOLUMNS derived expression (over the CALENDAR [Date]
// row) into a Snowflake SQL scalar expression over the spine column "d".
// Handles YEAR/MONTH/DAY/QUARTER/WEEKDAY/FORMAT(,"MMM"/"MMMM")/the date itself.
// Returns null when the expression isn't a recognized date-part shape so the
// caller can fall back to a passthrough/comment. (beads-sigma-7mn)
function daxCalendarDerivedToSql(expr: string): string | null {
  const e = expr.trim();
  // The bare CALENDAR date column itself, e.g. [Date] -> the spine date.
  if (/^\[[^\]]+\]$/.test(e)) return 'd';
  let m: RegExpMatchArray | null;
  if ((m = e.match(/^YEAR\s*\(\s*\[[^\]]+\]\s*\)$/i))) return 'EXTRACT(YEAR FROM d)';
  if ((m = e.match(/^MONTH\s*\(\s*\[[^\]]+\]\s*\)$/i))) return 'EXTRACT(MONTH FROM d)';
  if ((m = e.match(/^DAY\s*\(\s*\[[^\]]+\]\s*\)$/i))) return 'EXTRACT(DAY FROM d)';
  if ((m = e.match(/^QUARTER\s*\(\s*\[[^\]]+\]\s*\)$/i))) return 'EXTRACT(QUARTER FROM d)';
  if ((m = e.match(/^WEEKDAY\s*\(\s*\[[^\]]+\]/i))) return 'DAYOFWEEK(d)';
  // FORMAT([Date], "MMM") -> short month name; "MMMM" -> full month name.
  if ((m = e.match(/^FORMAT\s*\(\s*\[[^\]]+\]\s*,\s*"([^"]+)"\s*\)$/i))) {
    const fmt = m[1];
    if (/^MMMM$/.test(fmt)) return "TO_CHAR(d, 'MMMM')";
    if (/^MMM$/.test(fmt)) return "TO_CHAR(d, 'Mon')";
    if (/^YYYY$/.test(fmt)) return "TO_CHAR(d, 'YYYY')";
    return "TO_CHAR(d, '" + fmt.replace(/MMMM/g, 'MMMM').replace(/MMM/g, 'Mon') + "')";
  }
  return null;
}

// CALENDAR(DATE(y,m,d), DATE(y,m,d)) [optionally wrapped in ADDCOLUMNS(..., name, expr, ...)]
// -> a Snowflake date-spine SQL element: GENERATOR(ROWCOUNT=>N) + DATEADD daily
// series from start..end inclusive, plus each ADDCOLUMNS-derived column translated
// via daxCalendarDerivedToSql. VERIFIED vs PBI: AdventureWorks-style spine = 3287
// rows, 2018-01-01..2026-12-31, derived Year/MonthNo/Month exact. (beads-sigma-7mn)
function buildCalendarSpineSql(
  dax: string,
  colDisplayNames: string[]
): { ok: true; sql: string } | { ok: false; reason: string } {
  const cm = dax.match(/\bCALENDAR\s*\(/i);
  if (!cm) return { ok: false, reason: 'not a CALENDAR expression' };
  const { args } = splitCallArgs(dax, cm.index! + cm[0].length);
  if (args.length < 2) return { ok: false, reason: 'CALENDAR with non-literal bounds — recreate the date spine manually.' };
  const parseDate = (a: string): string | null => {
    const dm = a.match(/DATE\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)/i);
    if (!dm) return null;
    const [, y, mo, d] = dm;
    return `${y.padStart(4, '0')}-${mo.padStart(2, '0')}-${d.padStart(2, '0')}`;
  };
  const startStr = parseDate(args[0]);
  const endStr = parseDate(args[1]);
  if (!startStr || !endStr) return { ok: false, reason: 'CALENDAR bounds are not literal DATE(y,m,d) — recreate the date spine manually.' };
  const startMs = Date.parse(startStr + 'T00:00:00Z');
  const endMs = Date.parse(endStr + 'T00:00:00Z');
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs < startMs) {
    return { ok: false, reason: 'CALENDAR bounds invalid — recreate the date spine manually.' };
  }
  const rowCount = Math.round((endMs - startMs) / 86400000) + 1; // inclusive

  // Collect the ADDCOLUMNS derived (name, expr) pairs, if any.
  // ADDCOLUMNS(<table>, "Name1", <expr1>, "Name2", <expr2>, ...)
  const derived: { name: string; expr: string }[] = [];
  const am = dax.match(/\bADDCOLUMNS\s*\(/i);
  if (am) {
    const { args: addArgs } = splitCallArgs(dax, am.index! + am[0].length);
    // addArgs[0] is the table (the CALENDAR(...)); the rest are name/expr pairs.
    for (let i = 1; i + 1 < addArgs.length; i += 2) {
      const name = addArgs[i].trim().replace(/^"|"$/g, '');
      derived.push({ name, expr: addArgs[i + 1].trim() });
    }
  }

  // First declared column = the CALENDAR date series.
  const dateColName = colDisplayNames[0] || 'Date';
  const selects: string[] = [`d AS "${dateColName}"`];
  const unconverted: string[] = [];
  derived.forEach((dv, idx) => {
    // Map derived name -> the declared display name in column order (skip col 0,
    // the date). Fall back to the DAX-derived name when not enough declared cols.
    const display = colDisplayNames[idx + 1] || dv.name;
    const sqlExpr = daxCalendarDerivedToSql(dv.expr);
    if (sqlExpr) {
      selects.push(`${sqlExpr} AS "${display}"`);
    } else {
      selects.push(`NULL AS "${display}"`);
      unconverted.push(display);
    }
  });

  const sql =
    `SELECT ${selects.join(', ')}\n` +
    `FROM (\n` +
    `  SELECT DATEADD('day', SEQ4(), CAST('${startStr}' AS DATE)) AS d\n` +
    `  FROM TABLE(GENERATOR(ROWCOUNT => ${rowCount}))\n` +
    `)`;
  if (unconverted.length) {
    return { ok: true, sql: sql + `\n-- NOTE: derived column(s) ${unconverted.join(', ')} had a DAX expression that could not be auto-translated — emitted as NULL; fill in manually.` };
  }
  return { ok: true, sql };
}

// ── Calculated (DAX) tables → Sigma sql element, never a warehouse-table ──────
// A partition with source.type === "calculated" is a DAX-computed table
// (GENERATESERIES / CALENDAR / ADDCOLUMNS / SELECTCOLUMNS / ROW / DATATABLE …),
// NOT a warehouse object. Path-guessing one yields a fabricated three-part path
// that 404s at query time. Instead synthesize a Sigma `sql` element from a
// VALUES list when the DAX is a GENERATESERIES(start, stop, step) series; for
// anything else, signal { ok: false } so the caller emits a structured refusal
// rather than a broken element. (beads-sigma-w9s)
function buildCalcTableSql(
  dax: string,
  seriesColName: string,
  colDisplayNames: string[] = []
): { ok: true; sql: string } | { ok: false; reason: string } {
  // CALENDAR(a,b) [/ ADDCOLUMNS(CALENDAR(a,b), ...)] -> a real date-spine SQL
  // element with the ADDCOLUMNS-derived columns translated to SQL. Checked
  // before GENERATESERIES so the date spine wins over the numeric-series path.
  // (beads-sigma-7mn)
  if (/\bCALENDAR\s*\(/i.test(dax)) {
    return buildCalendarSpineSql(dax, colDisplayNames);
  }
  // Find GENERATESERIES(start, stop[, step]) anywhere in the expression.
  const gm = dax.match(/\bGENERATESERIES\s*\(/i);
  if (!gm) {
    return { ok: false, reason: 'DAX calculated table is not a GENERATESERIES or CALENDAR — no warehouse source exists; recreate manually as a Sigma SQL element or input table.' };
  }
  const { args } = splitCallArgs(dax, gm.index! + gm[0].length);
  if (args.length < 2) {
    return { ok: false, reason: 'GENERATESERIES with non-literal bounds — recreate the series manually.' };
  }
  const start = Number(args[0]);
  const stop = Number(args[1]);
  const step = args.length >= 3 ? Number(args[2]) : 1;
  if (!Number.isFinite(start) || !Number.isFinite(stop) || !Number.isFinite(step) || step === 0) {
    return { ok: false, reason: 'GENERATESERIES with non-literal/zero bounds — recreate the series manually.' };
  }
  const vals: number[] = [];
  if (step > 0) { for (let v = start; v <= stop && vals.length < 10000; v += step) vals.push(v); }
  else { for (let v = start; v >= stop && vals.length < 10000; v += step) vals.push(v); }
  if (!vals.length) return { ok: false, reason: 'GENERATESERIES yields an empty series — recreate manually.' };
  const rows = vals.map(v => `(${v})`).join(', ');
  const col = seriesColName || 'Value';
  const sql = `SELECT v AS "${col}" FROM (VALUES ${rows}) AS t(v)`;
  return { ok: true, sql };
}

// ── Main conversion ───────────────────────────────────────────────────────────

export interface PowerBIConvertOptions {
  connectionId?: string;
  database?: string;
  schema?: string;
}

// ── Time-intelligence → grouped DM elements (DateLookback / CumulativeSum) ──
// Standalone time-intel measures (SAMEPERIODLASTYEAR / DATEADD / TOTALYTD /
// running-total / hand-rolled prior-year) can't be scalar metrics (they need a
// date grouping) — emit them as grouped/leveled elements on the fact's "<T> View"
// (denormalized join), which is DM-native and verified exact vs Power BI.
function classifyTimeIntel(dax: string): 'prior' | 'ytd' | null {
  const d = dax || '';
  if (/\bTOTALYTD\s*\(|\bDATESYTD\s*\(/i.test(d)) return 'ytd';
  if (/FILTER\s*\(\s*ALL\s*\([^)]*\)\s*,[^<]*<=\s*MAX\s*\(/i.test(d)) return 'ytd'; // running total
  if (/\bSAMEPERIODLASTYEAR\s*\(/i.test(d)) return 'prior';
  if (/\bDATEADD\s*\([^,]+,\s*-?\d+\s*,\s*(YEAR|QUARTER|MONTH|WEEK|DAY)/i.test(d)) return 'prior';
  // hand-rolled prior-year: SELECTEDVALUE(Date[Year]) … ALL(Date[Year]) … [Year]=cy-1
  if (/SELECTEDVALUE\s*\([^)]*\[Year\]/i.test(d) && /ALL\s*\([^)]*\[Year\]/i.test(d) && /-\s*1\b/.test(d)) return 'prior';
  return null;
}
// Sigma's display name for a View column: [A/Col]->"Col"; [A/DIM/Col]->"Col (DIM)".
function viewColDisplay(formula: string): string {
  const p = (formula || '').replace(/^\[|\]$/g, '').split('/');
  return p.length <= 2 ? p[p.length - 1] : `${p[p.length - 1]} (${p[p.length - 2]})`;
}
function emitTimeIntelElements(model: any, elements: any[], warnings: string[]): void {
  const AGG: Record<string, string> = { SUM: 'Sum', AVERAGE: 'Avg', AVG: 'Avg', MIN: 'Min',
    MAX: 'Max', COUNT: 'Count', COUNTA: 'Count', DISTINCTCOUNT: 'CountDistinct' };
  const views = elements.filter((e: any) => e.name && /View$/.test(e.name) && e.source?.kind === 'table');
  if (!views.length) return;
  const lastSeg = (f: string) => (f || '').replace(/^\[|\]$/g, '').split('/').pop() || '';
  for (const t of (model.tables || [])) {
    for (const m of (t.measures || [])) {
      const dax = Array.isArray(m.expression) ? m.expression.join(' ') : String(m.expression || '');
      const shape = classifyTimeIntel(dax);
      if (!shape) continue;
      const am = dax.match(/\b(SUM|AVERAGE|AVG|MIN|MAX|COUNT|DISTINCTCOUNT)\s*\(\s*'?[^'\[]*'?\[([^\]]+)\]/i);
      if (!am) continue;
      const agg = AGG[am[1].toUpperCase()]; const col = am[2];
      // find a View carrying both the value column and a date column
      let parent: any = null, valDisp = '', dateDisp = '';
      for (const v of views) {
        const vc = (v.columns || []).find((c: any) => lastSeg(c.formula).toUpperCase() === col.toUpperCase());
        const dc = (v.columns || []).find((c: any) => /full date/i.test(viewColDisplay(c.formula)))
                || (v.columns || []).find((c: any) => /date/i.test(lastSeg(c.formula)) && !/key/i.test(lastSeg(c.formula)));
        if (vc && dc) { parent = v; valDisp = viewColDisplay(vc.formula); dateDisp = viewColDisplay(dc.formula); break; }
      }
      if (!parent) continue;
      const pn = parent.name; const b = (m.name || 'TI').replace(/[^a-zA-Z0-9]/g, '').slice(0, 14);
      if (shape === 'prior') {
        const prior = `${valDisp} (Prior Year)`;
        const cols = [
          { id: `${b}_d`, formula: `DateTrunc("year", [${pn}/${dateDisp}])`, name: 'Year' },
          { id: `${b}_v`, formula: `${agg}([${pn}/${valDisp}])`, name: valDisp },
          { id: `${b}_p`, formula: `DateLookback([${valDisp}], [Year], 1, "year")`, name: prior },
          { id: `${b}_y`, formula: `([${valDisp}] - [${prior}]) / [${prior}]`, name: `${valDisp} YoY %`, format: { kind: 'number', formatString: ',.1%' } },
        ];
        elements.push({ id: `${b}PP`, kind: 'table', name: m.name, source: { kind: 'table', elementId: parent.id },
          columns: cols, order: cols.map(c => c.id), groupings: [{ id: `${b}_g`, groupBy: [`${b}_d`], calculations: [`${b}_v`, `${b}_p`, `${b}_y`] }] });
        warnings.push(`ℹ Time-intel measure "${m.name}" → grouped DateLookback element on "${pn}" (prior-year + YoY %).`);
      } else {
        const cols = [
          { id: `${b}_o`, formula: `DateTrunc("year", [${pn}/${dateDisp}])`, name: 'Year' },
          { id: `${b}_i`, formula: `DateTrunc("month", [${pn}/${dateDisp}])`, name: 'Month' },
          { id: `${b}_v`, formula: `${agg}([${pn}/${valDisp}])`, name: valDisp },
          { id: `${b}_c`, formula: `CumulativeSum([${valDisp}])`, name: `${valDisp} YTD` },
        ];
        // TWO grouping LEVELS so CumulativeSum resets per outer (year) period.
        elements.push({ id: `${b}YT`, kind: 'table', name: m.name, source: { kind: 'table', elementId: parent.id },
          columns: cols, order: cols.map(c => c.id), groupings: [
            { id: `${b}_go`, groupBy: [`${b}_o`] },
            { id: `${b}_gi`, groupBy: [`${b}_i`], calculations: [`${b}_v`, `${b}_c`] }] });
        warnings.push(`ℹ Time-intel measure "${m.name}" → grouped CumulativeSum (YTD, year-reset) element on "${pn}".`);
      }
    }
  }
}

export function convertPowerBIToSigma(
  modelJson: any,
  options: PowerBIConvertOptions = {}
): ConversionResult {
  resetIds();

  const { connectionId = '', database = '', schema = '' } = options;
  const model = modelJson.model || modelJson;

  if (!model.tables || !Array.isArray(model.tables)) {
    throw new Error('Invalid model — no "tables" array found');
  }

  const dbOverride = (database || '').toUpperCase();
  const schOverride = (schema || '').toUpperCase();
  const warnings: string[] = [];
  const elements: SigmaElement[] = [];
  const tableIdMap: Record<string, string> = {};
  const tableColMap: Record<string, Record<string, string>> = {};
  const allPbiToSigmaNames: Record<string, string> = {};
  // measure (PBI) name -> owning element id, for cross-table ratio detection
  // (beads-sigma-m1a). Includes measures later moved to the fact element.
  const measureToElementId: Record<string, string> = {};

  // Detect "measures only" tables and calculation group tables
  const measureOnlyTables = new Set<string>();
  const calcGroupTables = new Set<string>();
  for (const t of model.tables) {
    if (t.calculationGroup) {
      calcGroupTables.add(t.name);
      continue;
    }
    const dataCols = (t.columns || []).filter((c: any) => c.type !== 'rowNumber' && !c.isGenerated);
    if (dataCols.length === 0 && (t.measures || []).length > 0) {
      measureOnlyTables.add(t.name);
    }
  }

  // Pre-pass: map every table's column PBI-name → Sigma display name across
  // the entire model so calc col formulas referencing related-table columns
  // (e.g. RELATED('dim'[COL_X]) → bare [COL_X]) can be normalized to the
  // display-name form before the cross-element move pass runs.
  for (const t of model.tables) {
    if (calcGroupTables.has(t.name)) continue;
    if (t.name.startsWith('LocalDateTable_') || t.name.startsWith('DateTableTemplate_')) continue;
    for (const c of (t.columns || [])) {
      if (c.type === 'rowNumber' || c.isGenerated) continue;
      const sourceCol = c.sourceColumn || c.name;
      if (!sourceCol) continue;
      if (!(c.name in allPbiToSigmaNames)) {
        allPbiToSigmaNames[c.name] = sigmaDisplayName(sourceCol);
      }
    }
  }

  // ── Convert tables to Sigma elements ────────────────────────────────────────
  for (const t of model.tables) {
    if (measureOnlyTables.has(t.name)) continue;
    if (calcGroupTables.has(t.name)) continue;
    if (t.name.startsWith('LocalDateTable_') || t.name.startsWith('DateTableTemplate_')) continue;

    const elementId = sigmaShortId();
    const tableName: string = t.name;
    tableIdMap[tableName] = elementId;
    tableColMap[tableName] = {};

    const partition = (t.partitions || [])[0];

    // ── DAX calculated tables (source.type === "calculated") ────────────────
    // Branch BEFORE any M-path extraction: these are computed in the model,
    // not warehouse objects. Path-guessing produces a fabricated path that
    // 404s. Emit a Sigma `sql` element (synthesized VALUES for GENERATESERIES)
    // or a structured refusal — never a warehouse-table. (beads-sigma-w9s)
    if (partition?.source?.type === 'calculated') {
      const ctExpr = Array.isArray(partition.source.expression)
        ? partition.source.expression.join('\n')
        : (partition.source.expression || '');
      // Declared columns (calculatedTableColumn / untyped) become surfaced cols.
      const ctCols = (t.columns || []).filter((c: any) => c.type !== 'rowNumber' && !c.isGenerated);
      const ctColDisplayNames: string[] = ctCols.map((c: any) =>
        sigmaDisplayName((c.sourceColumn || c.name || '').replace(/^\[|\]$/g, '')));
      const firstColName = ctColDisplayNames.length ? ctColDisplayNames[0] : 'Value';
      const built = buildCalcTableSql(ctExpr, firstColName, ctColDisplayNames);

      const ctColumns: SigmaColumn[] = [];
      const ctOrder: string[] = [];
      for (const c of ctCols) {
        const sourceCol = (c.sourceColumn || c.name || '').replace(/^\[|\]$/g, '');
        const displayName = sigmaDisplayName(sourceCol);
        const colId = sigmaInodeId((sourceCol || c.name).toUpperCase().replace(/\s+/g, '_'));
        tableColMap[tableName][c.name] = colId;
        allPbiToSigmaNames[c.name] = displayName;
        const col: SigmaColumn = { id: colId, formula: `[${displayName}]` };
        if (c.isHidden) (col as any).hidden = true;
        if (c.description) col.description = c.description;
        ctColumns.push(col);
        ctOrder.push(colId);
      }

      let statement: string;
      if (built.ok) {
        statement = built.sql;
        if (/\bCALENDAR\s*\(/i.test(ctExpr)) {
          warnings.push(`ℹ Calculated table "${tableName}": DAX CALENDAR/ADDCOLUMNS → synthesized a Sigma SQL date-spine element (GENERATOR + DATEADD) with the derived columns translated to SQL.`);
        } else if (ctCols.length > 1) {
          warnings.push(`ℹ Calculated table "${tableName}": synthesized a SQL VALUES series for column "${firstColName}". The remaining derived column(s) (${ctCols.slice(1).map((c: any) => sigmaDisplayName(c.sourceColumn || c.name)).join(', ')}) come from DAX ADDCOLUMNS/SELECTCOLUMNS — add their expressions to the SQL or as Sigma calc columns.`);
        } else {
          warnings.push(`ℹ Calculated table "${tableName}": DAX GENERATESERIES → synthesized Sigma SQL element (VALUES list).`);
        }
      } else {
        statement = `-- TODO (beads-sigma-w9s): ${built.reason}\n-- Original DAX: ${ctExpr.replace(/\n/g, ' ').slice(0, 300)}\nSELECT 1 AS _placeholder`;
        warnings.push(`⛔ Calculated table "${tableName}": ${built.reason} Emitted a placeholder SQL element (NOT a warehouse-table). Original DAX preserved as a comment.`);
      }

      const ctElement: SigmaElement = {
        id: elementId, kind: 'table', name: tableName.toUpperCase(),
        source: { connectionId: connectionId || '<CONNECTION_ID>', kind: 'sql', statement },
        columns: ctColumns, order: ctOrder,
      };
      if (!built.ok) (ctElement as any).ok = false;
      if (t.isHidden) (ctElement as any).visibleAsSource = false;
      elements.push(ctElement);
      continue;
    }

    // Determine source path
    let path: string[] | null = null;
    if (partition?.source) {
      if (partition.source.expression) {
        path = pbiExtractPathFromM(
          Array.isArray(partition.source.expression)
            ? partition.source.expression.join('\n')
            : partition.source.expression
        );
      }
      if (!path && partition.source.query) {
        const tblMatch = partition.source.query.match(/FROM\s+(?:\[?(\w+)\]?\.)?\[?(\w+)\]?\.\[?(\w+)\]?/i);
        if (tblMatch) {
          path = [tblMatch[1] || '', tblMatch[2], tblMatch[3]].filter(Boolean).map((s: string) => s.toUpperCase());
        }
      }
    }
    // Apply overrides
    if (path) {
      if (dbOverride && path.length >= 3) path[0] = dbOverride;
      if (schOverride && path.length >= 3) path[1] = schOverride;
      else if (schOverride && path.length === 2) path[0] = schOverride;
    } else {
      path = [dbOverride || 'DATABASE', schOverride || 'SCHEMA', tableName.toUpperCase()];
      warnings.push(`⚠ Table "${tableName}": could not extract source path from M expression — using default.`);
    }

    // Columns
    const columns: SigmaColumn[] = [];
    const order: string[] = [];
    const pbiToSigmaName: Record<string, string> = {};

    for (const c of (t.columns || [])) {
      if (c.type === 'rowNumber' || c.isGenerated) continue;
      if (c.type === 'calculated') continue;
      const sourceCol = c.sourceColumn || c.name;
      const displayName = sigmaDisplayName(sourceCol);
      const colId = sigmaInodeId(sourceCol.toUpperCase().replace(/\s+/g, '_'));
      tableColMap[tableName][c.name] = colId;
      pbiToSigmaName[c.name] = displayName;
      allPbiToSigmaNames[c.name] = displayName;

      const col: SigmaColumn = { id: colId, formula: `[${tableName.toUpperCase()}/${displayName}]` };
      if (c.isHidden) (col as any).hidden = true;
      if (c.description) col.description = c.description;
      columns.push(col);
      order.push(colId);
    }

    // Calculated columns
    for (const c of (t.columns || [])) {
      if (c.type !== 'calculated') continue;
      let sigmaFormula = pbiDaxToSigma(c.expression, warnings, c.name);
      if (sigmaFormula) {
        // Rewrite PBI column names → Sigma display names. Try local table
        // first, fall back to the global map so cross-table refs (e.g. from
        // RELATED('dim'[COL])) get a usable display name that the post-pass
        // cross-element move can map back to a triple-form ref.
        sigmaFormula = sigmaFormula.replace(/\[([^\]\/]+)\]/g, (_m: string, colName: string) => {
          if (pbiToSigmaName[colName]) return `[${pbiToSigmaName[colName]}]`;
          if (allPbiToSigmaNames[colName]) return `[${allPbiToSigmaNames[colName]}]`;
          return `[${colName}]`;
        });
        const colId = sigmaShortId();
        tableColMap[tableName][c.name] = colId;
        pbiToSigmaName[c.name] = c.name;
        const _calcFmt = inferSigmaFormat(sigmaFormula, c.name, (c as any).formatString);
        const _calcCol: any = { id: colId, formula: sigmaFormula, name: c.name };
        if (_calcFmt) _calcCol.format = _calcFmt;
        columns.push(_calcCol);
        order.push(colId);
        warnings.push(`ℹ "${c.name}" → calculated column. Review: ${sigmaFormula.slice(0, 60)}`);
      } else if (!warnings.some(w => w.includes(c.name))) {
        warnings.push(`⛔ "${c.name}": DAX expression could not be converted. Add manually.`);
      }
    }

    // Measures → metrics
    const metrics: any[] = [];
    for (const m of (t.measures || [])) {
      if (m.name) measureToElementId[m.name] = elementId; // m1a cross-table detection
      let sigmaFormula = pbiDaxToSigma(m.expression, warnings, m.name);
      if (sigmaFormula) {
        sigmaFormula = sigmaFormula.replace(/\[([^\]\/]+)\]/g, (_m2: string, colName: string) => {
          return pbiToSigmaName[colName] ? `[${pbiToSigmaName[colName]}]` : `[${colName}]`;
        });
        const _mFmt = inferSigmaFormat(sigmaFormula, m.name, (m as any).formatString);
        const metric: any = { id: sigmaShortId(), formula: sigmaFormula, name: m.name };
        if (_mFmt) metric.format = _mFmt;
        if (m.description) metric.description = m.description;
        metrics.push(metric);
      } else if (!warnings.some(w => w.includes(`"${m.name}"`))) {
        warnings.push(`⛔ "${m.name}": DAX measure could not be auto-converted. Add manually.`);
      }
    }
    {
      const emitted = new Set(metrics.map((mm: any) => mm.name));
      const dropped = new Set<string>(
        (t.measures || []).map((mm: any) => mm.name).filter((nm: string) => nm && !emitted.has(nm))
      );
      pruneDanglingMetrics(metrics, dropped, warnings);
    }

    // Display folders
    const folders: any[] = [];
    const folderMap: Record<string, { id: string; name: string; items: string[] }> = {};
    for (const c of [...(t.columns || []), ...(t.measures || [])]) {
      if (c.displayFolder) {
        if (!folderMap[c.displayFolder]) {
          folderMap[c.displayFolder] = { id: sigmaShortId(), name: c.displayFolder, items: [] };
        }
        const colId = tableColMap[tableName][c.name];
        if (colId) folderMap[c.displayFolder].items.push(colId);
      }
    }
    for (const folder of Object.values(folderMap)) {
      if (folder.items.length > 0) folders.push(folder);
    }

    // Name the base element after its warehouse table (last path segment) so
    // workbook masters can reference it as [TABLE/Col]. Without this, only the
    // derived "<Table> View" elements were named and unnamed base elements
    // were unaddressable. (Bug beads-sigma-tkd #1)
    const baseElementName = (path && path.length ? path[path.length - 1] : tableName.toUpperCase());
    const element: SigmaElement = {
      id: elementId, kind: 'table', name: baseElementName,
      source: { connectionId: connectionId || '<CONNECTION_ID>', kind: 'warehouse-table', path },
      columns, order
    };
    if (metrics.length > 0) (element as any).metrics = metrics;
    if (folders.length > 0) (element as any).folders = folders;
    if (t.isHidden) (element as any).visibleAsSource = false;
    elements.push(element);
  }

  // ── Move measures from "measures only" tables to fact element ──────────────
  if (measureOnlyTables.size > 0) {
    const factEl = elements.reduce((best, e) =>
      (e.columns || []).length > (best.columns || []).length ? e : best, elements[0]);
    if (factEl) {
      for (const tName of measureOnlyTables) {
        const t = model.tables.find((tb: any) => tb.name === tName);
        if (!t) continue;
        for (const m of (t.measures || [])) {
          if (m.name) measureToElementId[m.name] = factEl.id; // m1a cross-table detection
          let sigmaFormula = pbiDaxToSigma(m.expression, warnings, m.name);
          if (sigmaFormula) {
            sigmaFormula = sigmaFormula.replace(/\[([^\]\/]+)\]/g, (_m2: string, colName: string) => {
              return allPbiToSigmaNames[colName] ? `[${allPbiToSigmaNames[colName]}]` : `[${colName}]`;
            });
            if (!(factEl as any).metrics) (factEl as any).metrics = [];
            const _moFmt = inferSigmaFormat(sigmaFormula, m.name, (m as any).formatString);
            const metric: any = { id: sigmaShortId(), formula: sigmaFormula, name: m.name };
            if (_moFmt) metric.format = _moFmt;
            if (m.description) metric.description = m.description;
            (factEl as any).metrics.push(metric);
          }
        }
        warnings.push(`ℹ Measures table "${tName}" → measures moved to "${factEl.source?.path?.[factEl.source.path.length - 1]}"`);
      }
    }
  }

  // ── Relationships ──────────────────────────────────────────────────────────
  for (const rel of (model.relationships || [])) {
    const fromTable = rel.fromTable;
    const toTable = rel.toTable;
    const fromCol = rel.fromColumn;
    const toCol = rel.toColumn;

    const fromElId = tableIdMap[fromTable];
    const toElId = tableIdMap[toTable];
    if (!fromElId || !toElId) continue;

    const fromColId = tableColMap[fromTable]?.[fromCol];
    const toColId = tableColMap[toTable]?.[toCol];
    if (!fromColId || !toColId) {
      warnings.push(`⚠ Relationship ${fromTable}[${fromCol}] → ${toTable}[${toCol}]: columns not found`);
      continue;
    }

    const fromElement = elements.find(e => e.id === fromElId);
    if (fromElement) {
      if (!fromElement.relationships) fromElement.relationships = [];
      fromElement.relationships.push({
        id: sigmaShortId(),
        targetElementId: toElId,
        keys: [{ sourceColumnId: fromColId, targetColumnId: toColId }],
        name: toTable
      });
    }
  }

  // ── Cross-table ratio / combination measures (beads-sigma-m1a) ──────────────
  // A measure like DIVIDE([Total Absence Hours], [Headcount]) where the numerator
  // and denominator aggregates live on DIFFERENT elements is emitted by the
  // formula converter as a same-element metric ([A] / [B]). The foreign aggregate
  // ([Headcount] on EMPLOYEES, not ABSENCE_RECORDS) then resolves NULL on the
  // host element. Detect these and, rather than ship a silently-null metric,
  // strip the metric and emit a structured warning describing the correct Sigma
  // reproduction: a constant-key (All Key = 1) Lookup join to the foreign
  // element so the foreign aggregate is taken across the FULL related set
  // (e.g. global headcount = total employees, not employees-with-absences).
  const measureRefRe = /\[([^\]\/]+)\]/g;
  for (const el of elements) {
    const mets: any[] = (el as any).metrics || [];
    if (!mets.length) continue;
    const kept: any[] = [];
    for (const metric of mets) {
      const formula: string = metric.formula || '';
      // Only care about formulas that COMBINE values (ratio / arithmetic across
      // measure refs). A lone aggregate or single-ref metric is fine.
      const refs = [...formula.matchAll(measureRefRe)].map(m => m[1]);
      const foreignMeasures = [...new Set(refs)].filter(name => {
        const owner = measureToElementId[name];
        return owner && owner !== el.id; // references a measure owned by ANOTHER element
      });
      // Must also actually combine (contain an operator), else a bare passthrough
      // ref to a foreign measure is rare — still treat as cross-element.
      const combines = /[\/*+\-]/.test(formula.replace(/\[[^\]]*\]/g, ''));
      if (foreignMeasures.length && combines) {
        const owners = foreignMeasures
          .map(n => {
            const oid = measureToElementId[n];
            const oel = elements.find(e => e.id === oid);
            return `[${n}] (on ${oel?.name || oid})`;
          })
          .join(', ');
        warnings.push(
          `⛔ "${metric.name}": cross-table ratio — references ${owners} from a different element than "${el.name}". ` +
          `Emitting a same-element metric would resolve those aggregates as NULL. ` +
          `In Sigma, reproduce via a constant-key (All Key = 1) relationship Lookup to the foreign element so the foreign aggregate is taken across the FULL related set ` +
          `(e.g. denominator = global headcount, not just rows with a match), then divide. ` +
          `Add this metric manually. See: ${PBI_COMMUNITY_LINKS.leveled}`
        );
        // Drop the silently-null metric (do NOT ship it).
        continue;
      }
      kept.push(metric);
    }
    if (kept.length) (el as any).metrics = kept;
    else delete (el as any).metrics;
  }

  // ── Pull cross-element calc cols off source warehouse-table elements ─────
  // A calc col on a warehouse-table whose formula references columns that
  // aren't on that element (e.g. RELATED('dim'[Field]) DAX → bare [Field])
  // cannot resolve there — Sigma doesn't see the related-table columns in
  // scope. We pull these calcs off the source, build derived "<Table> View"
  // elements via buildDerivedElements (which surfaces related cols via
  // [SRC/REL/Field]), then place the calcs on the derived element with
  // their bare [X] refs rewritten to the same triple form.
  // Mirrors tableau.ts buildDerivedElementsAndMoveCalcs Steps 1+3.
  const pbiCrossElCalcsByElId: Record<string, any[]> = {};
  for (const el of elements) {
    if (el.source?.kind !== 'warehouse-table') continue;
    if (!(el as any).relationships?.length) continue;

    const localNames = new Set<string>();
    for (const c of (el.columns || [])) {
      if (c.name) localNames.add(c.name.toUpperCase());
      if (!c.formula) continue;
      const fm = c.formula.match(/^\[[^\]\/]+\/([^\]]+)\]$/);
      if (fm) localNames.add(fm[1].toUpperCase());
    }

    const cross: any[] = [];
    const keep: any[] = [];
    for (const c of (el.columns || []) as any[]) {
      if (!c.name || !c.formula) { keep.push(c); continue; }
      // already-rewritten triple-segment formula (single-ref view col)
      if (/^\[[^\]\/]+\/[^\]\/]+\/[^\]]+\]$/.test(c.formula)) { keep.push(c); continue; }
      // simple 2-seg [Table/Field] passthrough column — keep
      if (/^\[[^\]\/]+\/[^\]\/]+\]$/.test(c.formula)) { keep.push(c); continue; }
      const refs = c.formula.match(/\[([^\]\/]+)\]/g) || [];
      const hasCross = refs.some((ref: string) => {
        const rn = ref.replace(/^\[|\]$/g, '');
        return !/^(true|false|null)$/i.test(rn) && !localNames.has(rn.toUpperCase());
      });
      if (hasCross) {
        const oi = ((el as any).order || []).indexOf(c.id);
        if (oi >= 0) ((el as any).order as string[]).splice(oi, 1);
        cross.push(c);
      } else {
        keep.push(c);
      }
    }
    (el as any).columns = keep;
    if (cross.length) pbiCrossElCalcsByElId[el.id] = cross;
  }

  // ── Calculation groups → derived metric stubs ────────────────────────────
  // Build a flat index: metric name → element so we can attach derived metrics
  // to the same element as their base measure.
  interface MetricRef { elementIndex: number; sigmaFormula: string }
  const metricIndex: Record<string, MetricRef> = {};
  for (let ei = 0; ei < elements.length; ei++) {
    for (const m of ((elements[ei] as any).metrics || [])) {
      if (m.name && m.formula) metricIndex[m.name] = { elementIndex: ei, sigmaFormula: m.formula };
    }
  }

  for (const t of model.tables) {
    if (!calcGroupTables.has(t.name)) continue;
    const cg = t.calculationGroup;
    const items: any[] = cg?.calculationItems || [];
    if (items.length === 0) continue;

    const groupName = t.name;
    warnings.push(
      `ℹ Calculation group "${groupName}" (${items.length} item${items.length !== 1 ? 's' : ''}): ` +
      `${items.map((i: any) => i.name).join(', ')} — ` +
      `derived metric stubs generated. Implement time intelligence using Sigma's Period-over-Period: ${PBI_COMMUNITY_LINKS.pop}`
    );

    // Track which elements get new metrics so we can add/update their folder
    const newMetricsByElement: Record<number, any[]> = {};

    for (const item of items) {
      const itemName: string = item.name || 'Unknown';
      const itemExpr: string = (item.expression || '').trim();

      // Skip "Current" / pass-through items — they're identical to the base measure
      const isPassthrough =
        /^SELECTEDMEASURE\s*\(\s*\)\s*$/i.test(itemExpr) ||
        itemName.toLowerCase() === 'current' ||
        itemName.toLowerCase() === 'actual';
      if (isPassthrough) continue;

      // Classify the item's time intelligence pattern for the description
      let description = `Calculation group "${groupName}" — ${itemName}. `;
      if (/TOTALYTD|DATESYTD/i.test(itemExpr)) {
        description += `Year-to-date. Implement using DateTrunc + CumulativeSum or Sigma's Period-over-Period: ${PBI_COMMUNITY_LINKS.pop}`;
      } else if (/TOTALQTD/i.test(itemExpr)) {
        description += `Quarter-to-date. Use DateTrunc("quarter", …) + CumulativeSum.`;
      } else if (/TOTALMTD/i.test(itemExpr)) {
        description += `Month-to-date. Use DateTrunc("month", …) + CumulativeSum.`;
      } else if (/SAMEPERIODLASTYEAR|PREVIOUSYEAR/i.test(itemExpr)) {
        description += `Same period last year. Implement using Sigma's Period-over-Period: ${PBI_COMMUNITY_LINKS.pop}`;
      } else if (/PREVIOUSQUARTER|PREVIOUSMONTH/i.test(itemExpr)) {
        description += `Previous period. Implement using DateAdd / Sigma's Period-over-Period: ${PBI_COMMUNITY_LINKS.pop}`;
      } else if (/PARALLELPERIOD|DATEADD/i.test(itemExpr)) {
        description += `Date-shifted period. Implement using DateAdd + Sigma's Period-over-Period: ${PBI_COMMUNITY_LINKS.pop}`;
      } else if (/DIVIDE\s*\(/i.test(itemExpr)) {
        description += `Ratio/variance calculation. Implement as a derived metric using base period formulas.`;
      } else {
        description += `DAX expression: ${itemExpr.slice(0, 120)}`;
      }

      // Generate one derived metric per base measure
      for (const [baseName, ref] of Object.entries(metricIndex)) {
        const derivedName = `${baseName} (${itemName})`;
        const derivedMetric: any = {
          id: sigmaShortId(),
          name: derivedName,
          // Use base formula as placeholder so the metric is syntactically valid
          formula: ref.sigmaFormula,
          description,
        };

        if (!newMetricsByElement[ref.elementIndex]) newMetricsByElement[ref.elementIndex] = [];
        newMetricsByElement[ref.elementIndex].push(derivedMetric);
      }
    }

    // Attach derived metrics to their elements and group them in a display folder
    for (const [eiStr, newMetrics] of Object.entries(newMetricsByElement)) {
      const ei = Number(eiStr);
      const el = elements[ei] as any;
      if (!el.metrics) el.metrics = [];
      el.metrics.push(...newMetrics);

      // Add / update a display folder for this calc group
      if (!el.folders) el.folders = [];
      const existingFolder = el.folders.find((f: any) => f.name === groupName);
      const folderItems = newMetrics.map((m: any) => m.id);
      if (existingFolder) {
        existingFolder.items.push(...folderItems);
      } else {
        el.folders.push({ id: sigmaShortId(), name: groupName, items: folderItems });
      }
    }
  }

  // ── Derived "<Table> View" elements + place pulled-off calc cols ────────
  // buildDerivedElements creates a derived element per warehouse-table that
  // has outgoing relationships, exposing own + related cols via [SRC/REL/X]
  // formulas. We then rewrite any pulled-off calc col's bare [X] refs to
  // the same triple form (using the relationship.name as REL segment) and
  // append onto the derived element. Mirrors tableau.ts Step 3.
  const pbiDerivedEls = buildDerivedElements(elements);
  for (const de of pbiDerivedEls) elements.push(de);

  // Auto-emit grouped time-intel elements (DateLookback / CumulativeSum) for
  // standalone time-intel measures, now that the "<T> View" join elements exist.
  emitTimeIntelElements(model, elements, warnings);

  const pbiPlacedSrcElIds: Record<string, boolean> = {};
  for (const de of pbiDerivedEls) {
    if (de.source?.kind !== 'table' || !(de.source as any).elementId) continue;
    const srcElId = (de.source as any).elementId;
    const calcs = pbiCrossElCalcsByElId[srcElId];
    if (!calcs?.length) continue;
    const srcEl = elements.find(e => e.id === srcElId);
    if (!srcEl) continue;
    const srcBaseName = (srcEl as any).name
      || srcEl.source?.path?.[srcEl.source.path.length - 1]
      || '';

    // Build map: bare related-col display name → triple-form path.
    const relatedNameMap: Record<string, string> = {};
    for (const rel of ((srcEl as any).relationships || [])) {
      if (!rel.name) continue;
      const tgtEl = elements.find(e => e.id === rel.targetElementId);
      if (!tgtEl || tgtEl.source?.kind !== 'warehouse-table') continue;
      for (const tc of (tgtEl.columns || [])) {
        if (!tc.formula || tc.formula.startsWith('/*')) continue;
        const fm = tc.formula.match(/^\[([^\]]+)\]$/);
        if (!fm) continue;
        const inner = fm[1];
        const s = inner.lastIndexOf('/');
        const dispName = s >= 0 ? inner.slice(s + 1) : inner;
        if (!(dispName in relatedNameMap)) {
          relatedNameMap[dispName] = `${srcBaseName}/${rel.name}/${dispName}`;
        }
      }
    }

    for (const c of calcs) {
      if (c.formula && Object.keys(relatedNameMap).length) {
        c.formula = c.formula.replace(/\[([^\]\/]+)\]/g, (match: string, refName: string) => {
          const rewritten = relatedNameMap[refName];
          return rewritten ? `[${rewritten}]` : match;
        });
      }
      ((de as any).columns as any[]).push(c);
      ((de as any).order as string[]).push(c.id);
    }
    warnings.push(`ℹ ${calcs.length} calc col(s) moved to derived "${(de as any).name}" (cross-element refs)`);
    pbiPlacedSrcElIds[srcElId] = true;
  }
  for (const elId of Object.keys(pbiCrossElCalcsByElId)) {
    if (pbiPlacedSrcElIds[elId]) continue;
    for (const c of pbiCrossElCalcsByElId[elId]) {
      warnings.push(`⚠ "${c.name}" cross-element refs but no derived element — column dropped`);
    }
  }

  // ── Build output ──────────────────────────────────────────────────────────
  if (!connectionId) warnings.unshift('⚠ Connection ID not set — update in JSON before saving to Sigma');

  const modelName = modelJson.name || model.name || 'Power BI Import';
  const sigmaModel = {
    name: modelName,
    schemaVersion: 1,
    pages: [{ id: sigmaShortId(), name: 'Page 1', elements }]
  };

  const ec = elements.length;
  const mc = elements.reduce((n, e) => n + ((e as any).metrics?.length || 0), 0);
  const rc = elements.reduce((n, e) => n + (e.relationships?.length || 0), 0);
  const cgCount = calcGroupTables.size;

  return {
    model: sigmaModel,
    warnings,
    stats: {
      tables: model.tables.filter((t: any) => !calcGroupTables.has(t.name)).length,
      elements: ec,
      columns: elements.reduce((n, e) => n + (e.columns?.length || 0), 0),
      metrics: mc,
      relationships: rc,
      ...(cgCount > 0 ? { calculationGroups: cgCount } : {}),
    }
  };
}
