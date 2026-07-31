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
  resetIds, sigmaShortId, sigmaInodeId, sigmaDisplayName, sigmaPhysicalName, inferSigmaFormat,
  buildDerivedElements,
  makeRlsSecurity, makeClsSecurity,
  type SigmaElement, type SigmaColumn, type ConversionResult, type SecurityRule,
} from './sigma-ids.js';
import { triageCrossTable, describeTriage, describeMetricBlocker, isNoCoveringView, type Rel, type MetricBlocker } from './powerbi-crosstable-triage.js';

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
    if (ch === '(' || ch === '[' || ch === '{') depth++;
    else if (ch === ')' || ch === ']' || ch === '}') {
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

// FORMAT(<numeric expr of date funcs>, "fmt") -> Text(<expr>) (dax-fidelity #10).
// Only fires when arg0 is a pure numeric / date-function expression (no column
// ref, no string literal) — e.g. FORMAT(MONTH(TODAY()),"00") -> Text(Month(Today())).
// A DateFormat of a real date column still routes to the generic FORMAT rename
// below; the zero-padding format string itself is not modeled.
function rewriteFormatNumeric(f: string): string {
  const re = /\bFORMAT\s*\(/gi;
  for (let guard = 0; guard < 20; guard++) {
    re.lastIndex = 0;
    const m = re.exec(f);
    if (!m) break;
    const { args, endPos } = splitCallArgs(f, m.index + m[0].length);
    if (args.length !== 2) break;
    const expr = args[0].trim();
    if (/^[A-Za-z0-9_()\-+*/. ]+$/.test(expr) && /[A-Za-z]\s*\(/.test(expr) && !/["'\[]/.test(expr)) {
      const rep = `Text(${recaseDateFns(expr)})`;
      f = f.slice(0, m.index) + rep + f.slice(endPos);
      continue;
    }
    break; // a FORMAT we don't claim — leave for the generic FORMAT→DateFormat rename
  }
  return f;
}

// DAX SEARCH/FIND(find_text, within_text[, start[, not_found]]) -> Sigma
//   Find(text, search_for[, start]). Sigma's Find takes the WITHIN text first,
//   then the substring to look for — the OPPOSITE arg order from DAX — so swap
//   args 0/1. DAX is 1-based and so is Sigma Find, so `start` passes through.
//   DAX's optional 4th not_found arg has no Sigma equivalent and is dropped.
//   (DAX SEARCH is case-insensitive, FIND case-sensitive; Sigma Find is
//   case-sensitive — acceptable approximation for the common SEARCH("@",[Email])
//   substring-presence idiom.)
function rewriteSearch(f: string): string {
  // Uppercase-only (no `i` flag): runs on RAW DAX (functions are uppercase) so
  // the generated mixed-case `Find(...)` is NOT re-matched and re-swapped.
  const re = /\b(SEARCH|FIND)\s*\(/g;
  for (let guard = 0; guard < 50; guard++) {
    re.lastIndex = 0;
    const m = re.exec(f);
    if (!m) break;
    const { args, endPos } = splitCallArgs(f, m.index + m[0].length);
    if (args.length < 2) break;
    const findText = args[0];
    const withinText = args[1];
    const passthrough = args.slice(2, 3); // keep start; drop not_found (arg 3)
    const newArgs = [withinText, findText, ...passthrough].map(a => a.trim());
    const rep = `Find(${newArgs.join(', ')})`;
    f = f.slice(0, m.index) + rep + f.slice(endPos);
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

// Inline a DAX VAR ... RETURN block into a single expression (beads-sigma — VAR
// leg). `VAR a = e1 VAR b = e2(a) RETURN f(a,b)` -> f with each var substituted
// by its (parenthesized) expression. Later VARs may reference earlier ones.
// Returns the inlined RETURN expression so the rest of the pipeline (DIVIDE,
// renames, …) processes it; leaves f unchanged if it can't parse cleanly.
function rewriteVarReturn(f: string): string {
  if (!/^\s*VAR\b/i.test(f) || !/\bRETURN\b/i.test(f)) return f;
  // collect top-level VAR / RETURN keyword positions (depth 0, outside strings)
  const marks: { kw: string; pos: number; end: number }[] = [];
  let depth = 0, inStr: string | null = null;
  for (let i = 0; i < f.length; i++) {
    const ch = f[i];
    if (inStr) { if (ch === inStr) inStr = null; continue; }
    if (ch === '"' || ch === "'") { inStr = ch; continue; }
    if (ch === '(' || ch === '[') depth++;
    else if (ch === ')' || ch === ']') depth--;
    else if (depth === 0) {
      const m = f.slice(i).match(/^(VAR|RETURN)\b/i);
      if (m) { marks.push({ kw: m[1].toUpperCase(), pos: i, end: i + m[1].length }); i += m[1].length - 1; }
    }
  }
  const ret = marks.find(m => m.kw === 'RETURN');
  if (!ret || marks[0].kw !== 'VAR') return f;
  const vars: { name: string; expr: string }[] = [];
  for (let k = 0; k < marks.length; k++) {
    if (marks[k].kw !== 'VAR') continue;
    const segEnd = (k + 1 < marks.length) ? marks[k + 1].pos : f.length;
    const body = f.slice(marks[k].end, segEnd);
    const eq = body.indexOf('=');
    if (eq < 0) return f;
    const name = body.slice(0, eq).trim();
    let expr = body.slice(eq + 1).trim();
    if (!/^[A-Za-z_]\w*$/.test(name)) return f;        // not a clean scalar VAR
    // substitute earlier vars into this expr
    for (const v of vars) {
      expr = expr.replace(new RegExp(`\\b${v.name}\\b`, 'g'), `(${v.expr})`);
    }
    vars.push({ name, expr });
  }
  let out = f.slice(ret.end).trim();
  // substitute longest names first to avoid prefix collisions
  for (const v of [...vars].sort((a, b) => b.name.length - a.name.length)) {
    out = out.replace(new RegExp(`\\b${v.name}\\b`, 'g'), `(${v.expr})`);
  }
  // bail if the result still contains a bare VAR/RETURN token (nested block we
  // didn't handle) — let the downstream guard warn instead of emitting garbage.
  if (/\bVAR\b|\bRETURN\b/i.test(out)) return f;
  return out;
}

// Simple row-iterator over a BARE table -> aggregate-of-expression. Sigma accepts
// aggregates of expressions (verified: Sum([a]*[b]) posts clean). Only fires when
// arg1 is a plain table name (NOT FILTER/VALUES/TOPN/SUMMARIZE — those iterate a
// derived row set) and the body has no nested aggregate/CALCULATE (which would
// double-aggregate). Otherwise leaves it for the iterator drop-warn guard.
function rewriteSimpleIterator(f: string): string {
  const ITER: Record<string, string> = { SUMX: 'Sum', AVERAGEX: 'Avg', MINX: 'Min', MAXX: 'Max' };
  const re = /\b(SUMX|AVERAGEX|MINX|MAXX)\s*\(/gi;
  for (let guard = 0; guard < 50; guard++) {
    re.lastIndex = 0;
    const m = re.exec(f);
    if (!m) break;
    const fn = m[1].toUpperCase();
    const { args, endPos } = splitCallArgs(f, m.index + m[0].length);
    if (args.length !== 2) break;
    const tbl = args[0].trim();
    const body = args[1].trim();
    // arg1 must be a bare (optionally quoted) table identifier
    const bareTable = /^'[^']+'$/.test(tbl) || /^[A-Za-z_]\w*$/.test(tbl);
    // body must be a row expression — no nested aggregate / CALCULATE / iterator
    const bodyHasAgg = /\b(SUM|AVERAGE|MIN|MAX|COUNT|COUNTROWS|DISTINCTCOUNT|CALCULATE|SUMX|AVERAGEX|MINX|MAXX|COUNTAX|RANKX)\s*\(/i.test(body);
    if (!bareTable || bodyHasAgg) break;   // not a simple iterator — leave for the guard
    const bareBody = body
      .replace(/'[^']+'\[([^\]]+)\]/g, '[$1]')
      .replace(/\b[A-Za-z_]\w*\[([^\]]+)\]/g, '[$1]');
    f = f.slice(0, m.index) + `${ITER[fn]}(${bareBody})` + f.slice(endPos);
  }
  return f;
}

// CALCULATE(<agg>, ALL(<wholeTable>)) | CALCULATE(<agg>, REMOVEFILTERS(<wholeTable>))
// -> GrandTotal(<agg>) — Sigma's grand-total-over-all-rows aggregate (verified at
// query time: Sum([x])/GrandTotal(Sum([x])) over a grouped table sums to 100%).
// This makes the common %-of-total idiom DIVIDE([m], CALCULATE([m], ALL(T)))
// translate to [m] / GrandTotal([m]) instead of being dropped. Only fires when
// the filter is a WHOLE-table ALL/REMOVEFILTERS (no [Column] inside — ALL(T[c])
// is a partial subtotal, left for the warning) and CALCULATE has exactly 2 args.
function rewriteCalcGrandTotal(f: string): string {
  const re = /\bCALCULATE\s*\(/gi;
  for (let guard = 0; guard < 50; guard++) {
    re.lastIndex = 0;
    let replaced = false;
    let m: RegExpExecArray | null;
    while ((m = re.exec(f)) !== null) {
      const { args, endPos } = splitCallArgs(f, m.index + m[0].length);
      if (args.length !== 2) continue;
      const filt = args[1].trim();
      // whole-table ALL/REMOVEFILTERS only: ALL('Table') or ALL(Table), NO [col]
      if (!/^(ALL|REMOVEFILTERS)\s*\(\s*'?[A-Za-z_][\w ]*'?\s*\)$/i.test(filt)) continue;
      f = f.slice(0, m.index) + `GrandTotal(${args[0].trim()})` + f.slice(endPos);
      replaced = true;
      break;
    }
    if (!replaced) break;
  }
  return f;
}

// ── Family 3 (beads-sigma-fah8): complex boolean FILTER predicates in CALCULATE ──

// Split a DAX IN-list body `a, "b,c", d` on top-level commas (quote-aware).
function splitInList(body: string): string[] {
  const out: string[] = [];
  let start = 0, inStr: string | null = null, depth = 0;
  for (let i = 0; i < body.length; i++) {
    const ch = body[i];
    if (inStr) { if (ch === inStr) inStr = null; continue; }
    if (ch === '"' || ch === "'") { inStr = ch; continue; }
    if (ch === '(' || ch === '{') depth++;
    else if (ch === ')' || ch === '}') depth--;
    else if (ch === ',' && depth === 0) { out.push(body.slice(start, i).trim()); start = i + 1; }
  }
  const last = body.slice(start).trim();
  if (last) out.push(last);
  return out.filter(Boolean);
}

// Translate a row-level DAX boolean predicate into a Sigma boolean expression:
//   - && / || → and / or; NOT(x) → Not(x); TRUE()/FALSE() → True/False; <> → !=
//   - <ref> IN {v1, v2, …} → ([ref] = v1 or [ref] = v2 …)  (Sigma has NO IsIn —
//     an IsIn call silently errors the column; or-chains are the only safe form)
//   - qualified 'Table'[Col] / Table[Col] refs normalized to bare [Col]
// Returns { ok:false, reason } when the predicate is NOT a reproducible row-level
// boolean: it compares against an aggregate/measure ref (needs a windowed compare)
// or contains filter-context functions (nested FILTER/ALL/EARLIER/…).
function translateDaxPredicate(predRaw: string): { ok: true; sigma: string } | { ok: false; reason: string } {
  let p = (predRaw || '').trim();
  if (!p) return { ok: false, reason: 'empty predicate' };
  // Current-period date-part filter (dax-fidelity #6): MONTH/YEAR/DAY(TODAY())
  // compared to MONTH/YEAR/DAY(<date column>), in either order. Row-level (not an
  // aggregate) — translate directly; the generic path below would refuse it
  // because the RHS carries a bracketed column ref.
  {
    const capDp = (s: string) => (({ MONTH: 'Month', YEAR: 'Year', DAY: 'Day' } as Record<string, string>)[s.toUpperCase()] || s);
    const bareDp = (x: string) => x.replace(/'[^']+'\[([^\]]+)\]/g, '[$1]').replace(/\b[A-Za-z_]\w*\[([^\]]+)\]/g, '[$1]');
    const DP_TODAY = String.raw`(MONTH|YEAR|DAY)\s*\(\s*TODAY\s*\(\s*\)\s*\)`;
    const DP_COL = String.raw`(MONTH|YEAR|DAY)\s*\(\s*('?[A-Za-z_][\w ]*'?\[[^\]]+\])\s*\)`;
    const OP = String.raw`(<>|>=|<=|=|>|<)`;
    let dpm = p.match(new RegExp(`^\\s*${DP_TODAY}\\s*${OP}\\s*${DP_COL}\\s*$`, 'i'));
    if (dpm) return { ok: true, sigma: `${capDp(dpm[1])}(Today()) ${dpm[2] === '<>' ? '!=' : dpm[2]} ${capDp(dpm[3])}(${bareDp(dpm[4])})` };
    dpm = p.match(new RegExp(`^\\s*${DP_COL}\\s*${OP}\\s*${DP_TODAY}\\s*$`, 'i'));
    if (dpm) return { ok: true, sigma: `${capDp(dpm[1])}(${bareDp(dpm[2])}) ${dpm[3] === '<>' ? '!=' : dpm[3]} ${capDp(dpm[4])}(Today())` };
  }
  if (/\b(CALCULATE|FILTER|ALL|ALLEXCEPT|ALLSELECTED|REMOVEFILTERS|KEEPFILTERS|VALUES|RELATEDTABLE|EARLIER|TREATAS|USERELATIONSHIP|SELECTEDVALUE)\s*\(/i.test(p)) {
    return { ok: false, reason: `predicate contains filter-context functions (${p.slice(0, 60)})` };
  }
  // [NOT] <ref> IN {…} → or-chain / and-chain (must run BEFORE && / || translation).
  // The leading NOT must be captured separately or the table-qualifier pattern
  // swallows it ("NOT T"[TYPE]) and the negation silently distributes wrong.
  const inRe = /(\bNOT\s+)?((?:'[^']+'|\b[A-Za-z_][\w ]*)?\[[^\]]+\])\s+(NOT\s+)?IN\s*\{/i;
  for (let guard = 0; guard < 20; guard++) {
    const m = p.match(inRe);
    if (!m) break;
    const ref = m[2];
    const negate = !!(m[1] || m[3]);
    const open = m.index! + m[0].length;
    let depth = 1, i = open, inStr: string | null = null;
    for (; i < p.length; i++) {
      const ch = p[i];
      if (inStr) { if (ch === inStr) inStr = null; continue; }
      if (ch === '"') { inStr = ch; continue; }
      if (ch === '{') depth++;
      else if (ch === '}') { depth--; if (!depth) break; }
    }
    if (depth) return { ok: false, reason: 'unbalanced IN { … } list' };
    const items = splitInList(p.slice(open, i));
    if (!items.length) return { ok: false, reason: 'empty IN { } list' };
    const chain = negate
      ? items.map(v => `${ref} != ${v}`).join(' and ')
      : items.map(v => `${ref} = ${v}`).join(' or ');
    p = p.slice(0, m.index!) + `(${chain})` + p.slice(i + 1);
  }
  p = p.replace(/\bNOT\s*\(/gi, 'Not ('); // space: Not (x), the proven Sigma form (dax-fidelity #2)
  p = p.replace(/\bISBLANK\s*\(/gi, 'IsNull(');
  p = p.replace(/\bTRUE\s*\(\s*\)/gi, 'True').replace(/\bFALSE\s*\(\s*\)/gi, 'False');
  p = p.replace(/<>/g, '!=');
  p = p.replace(/&&/g, ' and ').replace(/\|\|/g, ' or ');
  // Qualified refs → bare [Col] (downstream name-mapping keys on bare names).
  p = p.replace(/'[^']+'\[([^\]]+)\]/g, '[$1]').replace(/\b[A-Za-z_]\w*\[([^\]]+)\]/g, '[$1]');
  // Refuse comparisons whose RHS carries a bracketed ref — that's a measure/
  // aggregate (or col-to-col) compare, not a row literal; a row-level *If would
  // be silently wrong (needs a windowed comparison).
  for (const term of p.split(/\b(?:and|or)\b/i)) {
    const c = term.match(/(=|!=|>=|<=|>|<)([\s\S]*)$/);
    if (c && /\[[^\]]+\]/.test(c[2])) {
      return { ok: false, reason: `compares against an aggregate/measure (${c[2].trim().slice(0, 50)})` };
    }
  }
  return { ok: true, sigma: p.replace(/\s+/g, ' ').trim() };
}

// Time-intelligence functions: a CALCULATE carrying one of these as a filter must
// NOT be claimed by the conditional-aggregate rewrite — the dedicated time-intel
// guard/emitTimeIntelElements path handles them.
const CALC_TIME_INTEL_RE = /\b(TOTALYTD|TOTALQTD|TOTALMTD|SAMEPERIODLASTYEAR|DATEADD|DATESYTD|DATESBETWEEN|DATESINPERIOD|PARALLELPERIOD|PREVIOUSMONTH|PREVIOUSQUARTER|PREVIOUSYEAR|PREVIOUSDAY|NEXTMONTH|NEXTQUARTER|NEXTYEAR)\s*\(/i;

// Same set MINUS DATESBETWEEN: a CALCULATE carrying one of THESE is deferred to
// the grouped time-intel element path, but DATESBETWEEN is a self-contained date
// window we translate inline into a conditional aggregation (dax-fidelity #6).
const CALC_TIME_INTEL_DEFER_RE = /\b(TOTALYTD|TOTALQTD|TOTALMTD|SAMEPERIODLASTYEAR|DATEADD|DATESYTD|DATESINPERIOD|PARALLELPERIOD|PREVIOUSMONTH|PREVIOUSQUARTER|PREVIOUSYEAR|PREVIOUSDAY|NEXTMONTH|NEXTQUARTER|NEXTYEAR)\s*\(/i;

// Recase DAX date functions to Sigma casing inside a scalar bound expression
// (TODAY()→Today(), NOW()→Now(), DATE/YEAR/MONTH/DAY→Date/Year/Month/Day). Used
// for DATESBETWEEN window bounds like TODAY()-364 or DATE(YEAR(TODAY()),1,1).
function recaseDateFns(expr: string): string {
  const map: Record<string, string> = { TODAY: 'Today', NOW: 'Now', DATE: 'Date', YEAR: 'Year', MONTH: 'Month', DAY: 'Day' };
  return expr.replace(/\b(TODAY|NOW|DATE|YEAR|MONTH|DAY)\b/gi, (w) => map[w.toUpperCase()] || w);
}

// Rewrite every translatable CALCULATE(...) occurrence into a Sigma conditional
// aggregate, splicing in place so surrounding expressions (DIVIDE, COALESCE,
// arithmetic) keep working. Handles (beads-sigma-fah8):
//   CALCULATE(agg, p1, p2, …)                 → AggIf(col, p1 and p2 …)
//   CALCULATE(agg, FILTER(T, c1 && c2 || c3)) → AggIf(col, translated boolean)
//   CALCULATE(agg, FILTER(ALL(T), pred))      → GrandTotal(AggIf(col, pred))
//     (ALL strips filter context → total semantics over ALL rows matching pred)
//   CALCULATE(agg, ALL(T[col]) | REMOVEFILTERS(T[col])) → GrandTotal(agg) with a
//     loud ⚠ (exact when col is the visual's only grouping; else re-express as a
//     window over the remaining dims in a grouped workbook element)
//   CALCULATE(x)  [1 arg, e.g. after USERELATIONSHIP strip] → x
// Subtotal re-scopes with no faithful scalar equivalent (ALLEXCEPT, ALLSELECTED,
// multi-col ALL) and non-row-level predicates → { dropped:true } with the original
// beads-sigma-p146: recursively inline bare [Measure] refs through a measure
// CHAIN until only base expressions remain. Returns null on a cycle / depth
// blowout so the caller can leave the formula for the structural guards.
// TotalSalesTY = CALCULATE([TotalSales], ...) where TotalSales = [m1]+[m2] and
// m1/m2 are SUM(col) needs the full chain flattened before the conditional
// rewrite can distribute the predicate over each leaf aggregate.
export function expandMeasureRefs(dax: string, measureDax: Record<string, string>): string | null {
  let out = String(dax).trim();
  for (let depth = 0; depth < 8; depth++) {
    let changed = false;
    out = out.replace(/(^|[^\w\]')])\[([^\[\]]+)\]/g, (full, pre, name) => {
      const body = (measureDax as any)[name];
      if (body === undefined) return full;
      changed = true;
      const b = Array.isArray(body) ? (body as string[]).join('\n') : String(body);
      return `${pre}(${b.trim()})`;
    });
    if (!changed) return out;
  }
  return null; // still expanding at max depth - treat as a cycle
}

// beads-sigma-p146: true when an expanded expression is a pure combination of
// simple aggregates, numeric literals, and +-*/() - i.e. a shape we can
// distribute a CALCULATE row-predicate over, aggregate by aggregate.
const SIMPLE_AGG_RE = /\b(SUM|AVERAGE|MIN|MAX|COUNT|COUNTA|COUNTROWS|DISTINCTCOUNT)\s*\(([^()]*)\)/gi;
export function isAggCombination(expr: string): boolean {
  const stripped = String(expr).replace(SIMPLE_AGG_RE, '1');
  return /\d/.test(stripped) && /^[\d\s+\-*/().]+$/.test(stripped);
}

// DAX preserved in the warning (flag-not-drop). Time-intel filters are declined
// silently so the time-intelligence guard classifies them.
function rewriteCalculateConditionals(
  fIn: string,
  warnings: string[] | null,
  measureName: string,
  measureDax: Record<string, string>,
  rawDax: string,
): { f: string; dropped: boolean } {
  let f = fIn;
  const daxNote = String(rawDax).replace(/\s+/g, ' ').trim().slice(0, 220);
  const bareRef = (x: string) =>
    x.replace(/'[^']+'\[([^\]]+)\]/g, '[$1]').replace(/\b[A-Za-z_]\w*\[([^\]]+)\]/g, '[$1]');
  const wholeTableStripRe = /^(ALL|REMOVEFILTERS)\s*\(\s*'?[A-Za-z_][\w ]*'?\s*\)$/i;
  const re = /\bCALCULATE\s*\(/gi;
  let cursor = 0;
  for (let guard = 0; guard < 30; guard++) {
    re.lastIndex = cursor;
    const m = re.exec(f);
    if (!m) break;
    const { args, endPos } = splitCallArgs(f, m.index + m[0].length);
    if (!args.length) { cursor = m.index + m[0].length; continue; }
    // CALCULATE(x) — no filters left (e.g. USERELATIONSHIP was stripped) → x.
    if (args.length === 1) {
      f = f.slice(0, m.index) + args[0].trim() + f.slice(endPos);
      continue; // re-scan from the same cursor
    }
    // Time-intel filter → leave for the time-intelligence guard (DATESBETWEEN is
    // handled inline below as a self-contained date window; dax-fidelity #6).
    if (CALC_TIME_INTEL_DEFER_RE.test(args.join(','))) { cursor = m.index + m[0].length; continue; }

    // Resolve the aggregate arg (bead qx16: may be a bare measure ref).
    let aggExpr = args[0].trim();
    const aggRefM = aggExpr.match(/^\[([^\]]+)\]$/);
    if (aggRefM && measureDax[aggRefM[1]]) {
      const refDax = measureDax[aggRefM[1]].trim();
      // qx16 guard: only inline a SINGLE simple aggregate (one call, no nested
      // parens / top-level operators) — a multi-aggregate body would mis-split.
      if (/^(SUM|AVERAGE|MIN|MAX|COUNT|COUNTA|COUNTROWS|DISTINCTCOUNT)\s*\([^()]*\)$/i.test(refDax)) {
        aggExpr = refDax;
      }
    }
    const aggM = aggExpr.match(/^\s*(SUM|AVERAGE|MIN|MAX|COUNT|COUNTA|COUNTROWS|DISTINCTCOUNT)\s*\(([\s\S]*)\)\s*$/i);
    // beads-sigma-p146: not a single simple aggregate — try flattening a measure
    // CHAIN ([TotalSales] = [m1]+[m2], m1/m2 = SUM(col)) into a pure aggregate
    // combination; the row predicate then distributes over each leaf aggregate.
    let composite: string | null = null;
    if (!aggM) {
      const expanded = expandMeasureRefs(aggExpr, measureDax);
      if (expanded && isAggCombination(expanded)) composite = expanded;
      else { cursor = m.index + m[0].length; continue; } // leave for the guards
    }
    const sigmaAggPlain = (fn: string, arg: string): string => {
      const F = fn.toUpperCase();
      if (F === 'COUNTROWS' || (F === 'COUNT' && !arg.trim())) return 'Count()';
      const map: Record<string, string> = { SUM: 'Sum', AVERAGE: 'Avg', MIN: 'Min', MAX: 'Max', COUNT: 'Count', COUNTA: 'Count', DISTINCTCOUNT: 'CountDistinct' };
      return `${map[F]}(${bareRef(arg.trim())})`;
    };
    const sigmaAggCond = (fn: string, arg: string, combined: string): string => {
      const F = fn.toUpperCase();
      const col = bareRef(arg.trim());
      const hasCol = /\[[^\]]+\]/.test(col);
      // Sigma CountIf takes ONE boolean arg (beads-sigma-862). COUNTROWS (no
      // column) and a column-less COUNT() → CountIf(cond). COUNT(col)/COUNTA(col)
      // count only NON-NULL values, so re-express as CountIf(cond and IsNotNull(col))
      // — matching DAX COUNT semantics (dax-fidelity #1).
      if (F === 'COUNTROWS' || ((F === 'COUNT' || F === 'COUNTA') && !hasCol)) return `CountIf(${combined})`;
      if (F === 'COUNT' || F === 'COUNTA') return `CountIf(${combined} and IsNotNull(${col}))`;
      // DISTINCTCOUNT: emit CountDistinct(If(cond, col, null)). The two-arg
      // CountDistinctIf(cond, col) shape misreads its args in this DM-metric path;
      // CountDistinct(If(...)) is the proven, unambiguous form (dax-fidelity #1).
      if (F === 'DISTINCTCOUNT') return `CountDistinct(If(${combined}, ${col}, null))`;
      const map: Record<string, string> = { SUM: 'SumIf', AVERAGE: 'AvgIf', MIN: 'MinIf', MAX: 'MaxIf' };
      return `${map[F] || 'SumIf'}(${col}, ${combined})`;
    };

    // Classify each filter arg.
    let grandTotal = false;
    const preds: string[] = [];
    // Columns whose filter context a single-column ALL(T[col]) / REMOVEFILTERS(T[col])
    // removes. These become a FILTER-SCOPED metric (the plain aggregate + a note to
    // ignore any control bound to that column) — NOT a GrandTotal (dax-fidelity #5).
    const filterRemovalCols: string[] = [];
    let flagged: string | null = null;
    for (let a of args.slice(1).map(x => x.trim())) {
      // bead qx16: unwrap KEEPFILTERS — it modifies filter-MERGE semantics, not
      // the row predicate Sigma needs.
      const km = a.match(/^KEEPFILTERS\s*\(/i);
      if (km) {
        const kr = splitCallArgs(a, km[0].length);
        if (kr.args.length >= 1) a = kr.args.join(', ').trim();
      }
      if (wholeTableStripRe.test(a)) { grandTotal = true; continue; }
      // Single-column ALL/REMOVEFILTERS → FILTER-SCOPED metric (dax-fidelity #5).
      // DAX ALL(T[col]) removes the filter context on ONE column: the measure
      // should IGNORE any control bound to that column, NOT collapse to a
      // GrandTotal over every row (which would be wrong whenever another dimension
      // is still grouping the visual). Emit the plain aggregate and flag the metric
      // so the workbook author can configure it to ignore that one control.
      const colStrip = a.match(/^(ALL|REMOVEFILTERS)\s*\(\s*('?[A-Za-z_][\w ]*'?\[[^\]]+\])\s*\)$/i);
      if (colStrip) {
        filterRemovalCols.push(colStrip[2]);
        if (warnings) warnings.push(`⚠ "${measureName}": ${colStrip[1].toUpperCase()}(${colStrip[2]}) removes filter context on ONE column — translated as a filter-scoped metric that IGNORES any control bound to ${colStrip[2].replace(/^.*\[/, '[')}. Configure this metric in the workbook to ignore that control; it must NOT collapse to a GrandTotal. Original DAX: ${daxNote}`);
        continue;
      }
      // ALLEXCEPT / ALLSELECTED / multi-col ALL → subtotal re-scope, no faithful
      // scalar metric. Flag-not-drop with the DAX preserved.
      if (/^(ALLEXCEPT|ALLSELECTED|ALL|REMOVEFILTERS)\s*\(/i.test(a)) {
        flagged = `⚠ "${measureName}": CALCULATE filter ${a.slice(0, 70)} re-scopes filter context (subtotal semantics) — no faithful Sigma scalar-metric equivalent. Recreate as a grouped workbook element (group by the kept dimensions, aggregate, then window-total). Original DAX: ${daxNote}`;
        break;
      }
      // DATESBETWEEN('Date'[col], <start>, <end>) → a self-contained date-window
      // condition (dax-fidelity #6). Bounds are recased date expressions so windows
      // like "last 365 days" (TODAY()-364 … TODAY()) or YTD-to-today survive as a
      // real conditional aggregation instead of being dropped as time-intelligence.
      const dbm = a.match(/^DATESBETWEEN\s*\(/i);
      if (dbm) {
        const dbr = splitCallArgs(a, dbm[0].length);
        const colM = dbr.args.length === 3 ? dbr.args[0].match(/('?[A-Za-z_][\w ]*'?\[[^\]]+\])\s*$/) : null;
        if (colM) {
          const col = bareRef(colM[1]);
          const start = recaseDateFns(dbr.args[1].trim());
          const end = recaseDateFns(dbr.args[2].trim());
          preds.push(`${col} >= ${start} and ${col} <= ${end}`);
          continue;
        }
        flagged = `⚠ "${measureName}": DATESBETWEEN filter isn't the <date column>, <start>, <end> shape — recreate the window manually. Original DAX: ${daxNote}`;
        break;
      }
      // FILTER(<table> | ALL(<table>), predicate)
      const fm = a.match(/^FILTER\s*\(/i);
      if (fm) {
        const fr = splitCallArgs(a, fm[0].length);
        if (fr.args.length < 2) { flagged = `⚠ "${measureName}": malformed FILTER in CALCULATE. Original DAX: ${daxNote}`; break; }
        const scope = fr.args[0].trim();
        if (wholeTableStripRe.test(scope)) grandTotal = true;
        else if (!/^'?[A-Za-z_][\w ]*'?$/.test(scope)) {
          flagged = `⚠ "${measureName}": FILTER iterates a derived row set (${scope.slice(0, 50)}) — not a plain table; no row-level conditional-aggregate equivalent. Recreate with a grouped workbook element. Original DAX: ${daxNote}`;
          break;
        }
        const t = translateDaxPredicate(fr.args.slice(1).join(', '));
        if (!t.ok) { flagged = `⚠ "${measureName}": CALCULATE filter ${t.reason}. Needs a windowed comparison or grouping — add manually. Original DAX: ${daxNote} See: ${PBI_COMMUNITY_LINKS.leveled}`; break; }
        preds.push(t.sigma);
        continue;
      }
      // Bare row-level predicate.
      const t = translateDaxPredicate(a);
      if (!t.ok) { flagged = `⚠ "${measureName}": CALCULATE filter ${t.reason}. Needs a windowed comparison or grouping — add manually. Original DAX: ${daxNote} See: ${PBI_COMMUNITY_LINKS.leveled}`; break; }
      preds.push(t.sigma);
    }
    if (flagged) {
      if (warnings) warnings.push(flagged);
      return { f, dropped: true };
    }
    const aggFnEarly = aggM ? aggM[1].toUpperCase() : '';
    // Plain (unconditioned) aggregate for this measure's agg expression.
    const plainAgg = (): string => {
      if (composite) return `(${composite.replace(SIMPLE_AGG_RE, (_mm, fn, arg) => sigmaAggPlain(fn, arg))})`;
      if (aggFnEarly === 'COUNTROWS') return 'Count()';
      const map: Record<string, string> = { SUM: 'Sum', AVERAGE: 'Avg', MIN: 'Min', MAX: 'Max', COUNT: 'Count', COUNTA: 'Count', DISTINCTCOUNT: 'CountDistinct' };
      return `${map[aggFnEarly]}(${bareRef(aggM![2].trim())})`;
    };
    if (!preds.length) {
      if (grandTotal) {
        // Whole-table ALL/REMOVEFILTERS or FILTER(ALL(T)) → GrandTotal(agg): the
        // %-of-total idiom (verified: Sum([x])/GrandTotal(Sum([x])) sums to 100%).
        const gOut = `GrandTotal(${plainAgg()})`;
        f = f.slice(0, m.index) + gOut + f.slice(endPos);
        cursor = m.index + gOut.length;
        continue;
      }
      if (filterRemovalCols.length) {
        // Single-column ALL(T[col]) with NO other predicate → filter-scoped metric:
        // the plain aggregate, flagged (above) to ignore that one control. NOT a
        // GrandTotal (dax-fidelity #5).
        const frOut = plainAgg();
        f = f.slice(0, m.index) + frOut + f.slice(endPos);
        cursor = m.index + frOut.length;
        continue;
      }
      cursor = m.index + m[0].length; continue; // nothing usable — leave for the guards
    }
    const combined = preds.length === 1
      ? preds[0]
      : preds.map(p => /\b(or)\b/i.test(p) ? `(${p})` : p).join(' and ');
    const aggFn = aggFnEarly;
    let out: string;
    if (composite) {
      // beads-sigma-p146: distribute the predicate over every leaf aggregate.
      out = `(${composite.replace(SIMPLE_AGG_RE, (_mm, fn, arg) => sigmaAggCond(fn, arg, combined))})`;
    } else {
      out = sigmaAggCond(aggFn, aggM![2], combined);
    }
    if (grandTotal) out = `GrandTotal(${out})`; // FILTER(ALL(T), pred): context-strip = total over matching rows
    f = f.slice(0, m.index) + out + f.slice(endPos);
    cursor = m.index + out.length;
  }
  return { f, dropped: false };
}

/**
 * Mask `"..."` DAX string-literal spans with same-length blanks, so a
 * downstream regex/depth-walk over raw DAX (or DAX-derived Sigma-formula)
 * text can't mistake literal TEXT that merely LOOKS like syntax — a mapped
 * function name, a comma, a paren, or a `[Column]`-shaped bracket — for the
 * real thing. DAX escapes an embedded quote by DOUBLING it (`""`), matching
 * `stripDaxComments`'s own established convention for this codebase — not a
 * backslash.
 *
 * A `[bracketed identifier]` (DAX `Table[Column]` / a translated Sigma
 * `[Element/Column]`) is treated as an ATOMIC span FIRST — copied through
 * unchanged before any quote-scan ever sees its contents — so an embedded
 * `"` inside an unusual column name can't be misread as opening a literal
 * that then swallows real text hunting for its close. Brackets never nest
 * here; hitting a second `[` before the first one's `]` means the first
 * never closed (leave it as an ordinary character rather than reaching PAST
 * an unrelated later bracket to "close" on ITS `]`, which would swallow
 * everything in between). An unterminated `"` is likewise left as an
 * ordinary character — neither delimiter is ever allowed to swallow the
 * rest of the string.
 *
 * Length-preserving (same length, same non-literal characters) so offsets
 * computed against the masked text apply unchanged to the original — the
 * DIVIDE arg/paren walk in `pbiDaxToSigma` relies on exactly this to slice
 * real argument text out of the ORIGINAL (unmasked) formula.
 */
export function maskDaxStringLiterals(s: string): string {
  let out = '';
  let i = 0;
  while (i < s.length) {
    const c = s[i];
    if (c === '[') {
      let j = i + 1;
      while (j < s.length && s[j] !== ']' && s[j] !== '[') j++;
      if (j < s.length && s[j] === ']') { out += s.slice(i, j + 1); i = j + 1; } else { out += c; i++; }
      continue;
    }
    if (c === '"') {
      let j = i + 1;
      let closed = false;
      while (j < s.length) {
        if (s[j] === '"') {
          if (s[j + 1] === '"') { j += 2; continue; }   // doubled escaped quote
          j++; closed = true; break;
        }
        j++;
      }
      if (closed) { out += ' '.repeat(j - i); i = j; } else { out += c; i++; }   // unterminated — not a literal
      continue;
    }
    out += c;
    i++;
  }
  return out;
}

/** Apply a global-regex replace to `s`, but only for matches that survive
 *  literal-masking (`maskDaxStringLiterals`) — i.e. matches OUTSIDE any
 *  `"..."` DAX string literal. A match that only exists because bracket- or
 *  keyword-shaped TEXT happens to sit inside a literal is left completely
 *  alone (the mask and `s` are identical outside literal spans, so a match
 *  found on the masked copy names the exact same real span in `s`). */
function replaceOutsideDaxLiterals(
  s: string, re: RegExp, replacer: (...args: any[]) => string,
): string {
  const masked = maskDaxStringLiterals(s);
  const scanner = new RegExp(re.source, re.flags.includes('g') ? re.flags : re.flags + 'g');
  let out = '';
  let last = 0;
  let m: RegExpExecArray | null;
  while ((m = scanner.exec(masked))) {
    const args: any[] = [...m, m.index, s];
    out += s.slice(last, m.index) + replacer(...args);
    last = m.index + m[0].length;
    if (m[0].length === 0) scanner.lastIndex++;
  }
  return out + s.slice(last);
}

// Drop any metric whose formula references a MEASURE that was itself dropped
// (a CALCULATE/iterator/ranking measure that didn't translate) — e.g. a ratio
// built on it. Without this the dependent metric posts but silently resolves to
// "Missing Metric". `droppedNames` is seeded with the source measures that did
// NOT make it into `metrics`; pruned metrics are added back so transitive chains
// (A→B→droppedC) collapse too. Scoped to dropped MEASURE names ONLY — column
// refs and surviving measures are never touched. (dangling-ref cascade)
//
// Scan a literal-MASKED copy — a comparison/label value like "see [Some
// Dropped Measure] for detail" must not be mistaken for a real reference to a
// measure that was actually dropped; that false positive cascade-drops a
// legitimate, fully-translated metric (dangerous: looks silent past the
// per-drop warning, which itself misnames the "reference" as real).
function pruneDanglingMetrics(metrics: any[], droppedNames: Set<string>, warnings: string[] | null): void {
  for (let pass = 0; pass < 10; pass++) {
    const before = metrics.length;
    for (let i = metrics.length - 1; i >= 0; i--) {
      const refs = (maskDaxStringLiterals(String(metrics[i].formula)).match(/\[([^\]\/]+)\]/g) || []).map((r) => r.slice(1, -1));
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

// bead jzd8: a base-table calc column / DM metric may not carry a window function
// (Rank/RankDense/Lag/Lead/RowNumber/NTile/FirstValue/LastValue) — Sigma silently
// errors there. Test with string/character literals stripped so a label like
// "Top Rank (1)" doesn't false-trigger the drop.
export function hasBareWindowFn(formula: string): boolean {
  const noStr = String(formula).replace(/"[^"]*"/g, '').replace(/'[^']*'/g, '');
  return /\b(Rank|RankDense|Lag|Lead|RowNumber|NTile|FirstValue|LastValue)\s*\(/.test(noStr);
}

export function pbiDaxToSigma(
  dax: string | string[],
  warnings: string[] | null,
  measureName: string,
  measureDax: Record<string, string> = {}
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
  // Bare EARLIER outside the recognized idioms: flag-not-drop with the DAX
  // preserved (beads-sigma-fah8). The convert layer first tries
  // pbiParseEarlierRank/pbiParseEarlierWindow, which lower the rank / running-
  // total / group-share / peer-count idioms onto SQL window helper elements;
  // reaching here means no idiom matched. Letting EARLIER flow on would mangle
  // into CountIf(...EARLIER...) — an error-typed column.
  if (/\bEARLIER\s*\(/i.test(f)) {
    if (warnings) warnings.push(`⚠ "${measureName}": unrecognized EARLIER row-context pattern — not auto-translated (recognized idioms: rank, running total, group share/total, peer count). Recreate as a window calc in a grouped workbook element. Original DAX: ${String(dax).replace(/\s+/g, ' ').trim().slice(0, 220)}`);
    return null;
  }
  f = rewriteStatIterators(f);  // MEDIANX/PERCENTILEX.INC/STDEVX.P/VARX.P/GEOMEANX
  f = rewriteCombineValues(f);  // COMBINEVALUES(sep,a,b) -> [a] & sep & [b]
  f = rewriteFormatNumeric(f);  // FORMAT(<numeric date expr>,"fmt") -> Text(<expr>) (dax-fidelity #10)
  f = rewriteSearch(f);         // SEARCH/FIND(find,within[,start]) -> Find(within,find[,start]) (arg-order swap)
  f = rewriteSingleValue(f);    // HASONEVALUE / SELECTEDVALUE
  f = rewriteSwitchTrue(f);     // SWITCH(TRUE(), c,v,...) -> nested If
  f = rewriteCountRowsFilter(f);// COUNTROWS/COUNT(FILTER(t,pred)) -> CountIf(pred) (r9oz)
  f = rewriteVarReturn(f);      // VAR x=.. RETURN f(x) -> inlined expression
  f = rewriteSimpleIterator(f); // SUMX/AVERAGEX/MINX/MAXX(bareTable, rowExpr) -> Sum/Avg/Min/Max(rowExpr)
  f = rewriteCalcGrandTotal(f); // CALCULATE(<agg>, ALL(<wholeTable>)) -> GrandTotal(<agg>) (%-of-total)
  // DISTINCTCOUNTNOBLANK(col) -> CountDistinct(col) (Sigma CountDistinct already
  // ignores nulls). Done here so the generic DISTINCTCOUNT rename can't first
  // claim the prefix and leave a dangling NOBLANK token.
  f = f.replace(/\bDISTINCTCOUNTNOBLANK\s*\(/gi, 'CountDistinct(');

  // ── Tier 4: Structural patterns → warnings only ──
  // USERELATIONSHIP reaching the formula layer directly (calc column / direct
  // call): the alternate-join activation happens at the convert layer (which
  // strips the filter arg before calling here). Flag-not-drop. (beads-sigma-fah8)
  if (/\bUSERELATIONSHIP\s*\(/i.test(f)) {
    if (warnings) warnings.push(`⚠ "${measureName}": USERELATIONSHIP outside a model measure — no alternate join path can be activated here. Original DAX: ${String(dax).replace(/\s+/g, ' ').trim().slice(0, 220)}`);
    return null;
  }
  // CALCULATE → Sigma conditional aggregates (incl. complex AND/OR predicates,
  // multi-predicate args, FILTER(ALL(T), pred) context-strips). Spliced in place
  // so DIVIDE/COALESCE wrappers keep working; unclaimed occurrences fall through
  // to the structural guards below. (beads-sigma-fah8)
  if (/\bCALCULATE\s*\(/i.test(f)) {
    const r = rewriteCalculateConditionals(f, warnings, measureName, measureDax, dax);
    if (r.dropped) return null;
    f = r.f;
  }
  // CALCULATE with ALL/ALLEXCEPT/REMOVEFILTERS the rewrite didn't claim
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
  // Scope / filter-context introspection — ISINSCOPE/ISFILTERED/ISCROSSFILTERED/
  // SELECTEDMEASURE inspect the live query's grouping/filter scope, which has no
  // static data-model equivalent. Emitting them verbatim is an invalid Sigma
  // formula that fails the DM POST (beads-sigma-mkm). Drop-and-warn instead.
  if (/\b(ISINSCOPE|ISFILTERED|ISCROSSFILTERED|SELECTEDMEASURE)\s*\(/i.test(f)) {
    const fn = f.match(/\b(ISINSCOPE|ISFILTERED|ISCROSSFILTERED|SELECTEDMEASURE)/i)![1];
    if (warnings) warnings.push(`⚠ "${measureName}": uses DAX scope introspection (${fn}). No static data-model equivalent — express the level explicitly with groupings, or drop. See: ${PBI_COMMUNITY_LINKS.leveled}`);
    return null;
  }
  // Time intelligence
  if (/\b(TOTALYTD|TOTALQTD|TOTALMTD|SAMEPERIODLASTYEAR|DATEADD|DATESYTD|PARALLELPERIOD|PREVIOUSMONTH|PREVIOUSQUARTER|PREVIOUSYEAR)\s*\(/i.test(f)) {
    const fn = f.match(/\b(TOTALYTD|TOTALQTD|TOTALMTD|SAMEPERIODLASTYEAR|DATEADD|DATESYTD|PARALLELPERIOD|PREVIOUSMONTH|PREVIOUSQUARTER|PREVIOUSYEAR)/i)![1];
    if (warnings) warnings.push(`⚠ "${measureName}": uses DAX time intelligence (${fn}). Use Period over Period feature. See: ${PBI_COMMUNITY_LINKS.pop}`);
    return null;
  }
  // CALCULATE the conditional-aggregate rewrite didn't claim → warn + drop.
  if (/\bCALCULATE\s*\(/i.test(f)) {
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

  // DIVIDE(a, b, alt) — nested-paren-aware parser. Walk a literal-MASKED copy
  // so a comma/paren INSIDE a string arg (a fallback label like "N/A, review"
  // or "(no data)") can't be mistaken for an argument separator or change the
  // paren depth — either one corrupts the split args or truncates/extends the
  // replaced span onto the wrong text (dangerous: wrong value). Content is
  // still sliced from the ORIGINAL `f` (masking is length-preserving, so the
  // walk's indices apply unchanged to the unmasked text).
  const divideMatch = f.match(/\bDIVIDE\s*\(/i);
  if (divideMatch) {
    const maskedF = maskDaxStringLiterals(f);
    const startIdx = divideMatch.index! + divideMatch[0].length;
    const divArgs: string[] = [];
    let depth = 1, argStart = startIdx;
    for (let i = startIdx; i < maskedF.length && depth > 0; i++) {
      if (maskedF[i] === '(') depth++;
      else if (maskedF[i] === ')') { depth--; if (depth === 0) { divArgs.push(f.slice(argStart, i).trim()); break; } }
      else if (maskedF[i] === ',' && depth === 1) { divArgs.push(f.slice(argStart, i).trim()); argStart = i + 1; }
    }
    if (divArgs.length >= 2) {
      const num = divArgs[0], den = divArgs[1], alt = divArgs[2];
      let d2 = 1, endPos = startIdx;
      for (; endPos < maskedF.length && d2 > 0; endPos++) {
        if (maskedF[endPos] === '(') d2++;
        else if (maskedF[endPos] === ')') d2--;
      }
      // bead hs5h: parenthesize BOTH operands. A numerator like "DeptMed - CoMed"
      // (from an inlined VAR/RETURN DIVIDE(a-b, c)) otherwise emits "a - b / c",
      // which Sigma parses as "a - (b/c)" — a wrong number. (x)/(y) is always safe.
      let replacement: string;
      if (alt && alt.trim()) {
        replacement = `If((${den}) = 0, ${alt.trim()}, (${num}) / (${den}))`;
      } else {
        // DAX DIVIDE(a, b) returns BLANK on a zero/blank denominator; a bare `a / b`
        // instead ERRORS the Snowflake query on b = 0. NullIf((b), 0) restores DAX
        // semantics: b = 0 → null denominator → a / null → null (dax-fidelity #10).
        replacement = `(${num}) / NullIf((${den}), 0)`;
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
  f = f.replace(/\bNOT\s*\(/gi, 'Not ('); // space: Not (x), the proven Sigma form (dax-fidelity #2)
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
  f = f.replace(/\bMOD\s*\(/gi, 'Mod(');   // DAX MOD(n,d) == Sigma Mod(n,d) (1:1)
  f = f.replace(/\bEXP\s*\(/gi, 'Exp(');   // DAX EXP(x) == Sigma Exp(x) (1:1)
  f = f.replace(/\bLN\s*\(/gi, 'Ln(');     // DAX LN(x) == Sigma Ln(x) (natural log)
  // DAX LOG10(x) and LOG(x,[base]) → Sigma Log(value,[base]) (base-10 default matches).
  f = f.replace(/\bLOG10\s*\(/gi, 'Log(');
  f = f.replace(/\bLOG\s*\(/gi, 'Log(');
  // DAX CEILING/FLOOR(number, significance) — Sigma Ceiling/Floor have no significance
  // arg; align to the multiple manually. (Single-arg fallbacks handled after.)
  f = f.replace(/\bCEILING\s*\(([^(),]+),\s*([^()]+)\)/gi, 'Ceiling($1 / $2) * $2');
  f = f.replace(/\bFLOOR\s*\(([^(),]+),\s*([^()]+)\)/gi, 'Floor($1 / $2) * $2');
  f = f.replace(/\bCEILING\s*\(/gi, 'Ceiling(');
  f = f.replace(/\bFLOOR\s*\(/gi, 'Floor(');
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
  // Collect unique table prefixes before [ to detect multi-table references.
  // Detect AND rewrite against a literal-MASKED copy: a display-text string
  // like "Store[Count]: high" contains a bare Table[Column]-shaped substring
  // that is DATA, not a real qualifier — unmasked, it both (a) pollutes
  // allTablePrefixes into a bogus "multiple tables" warning, and (b) gets
  // silently REWRITTEN by the .replace() calls below, corrupting the literal
  // text baked into the emitted formula (dangerous: wrong value).
  const maskedForPrefixes = maskDaxStringLiterals(f);
  const quotedTablePrefixes = (maskedForPrefixes.match(/'([^']+)'\[/g) || []).map(m => m.replace(/'\[$/g, '').replace(/^'/g, ''));
  const unquotedTablePrefixes = (maskedForPrefixes.match(/\b([A-Za-z_]\w*)\[/g) || []).map(m => m.replace(/\[$/, ''));
  const allTablePrefixes = [...new Set([...quotedTablePrefixes, ...unquotedTablePrefixes])].filter(p =>
    !/^(If|Switch|Not|And|Or|Sum|Avg|Min|Max|Count|CountIf|CountDistinct|CumulativeSum|Coalesce|Nullif|Round|Floor|Ceiling|Abs|Upper|Lower|Trim|Left|Right|Mid|Replace|Find|Len|Year|Month|Day|Hour|Minute|Second|Today|Now|MakeDate|DateDiff|DateAdd|DateTrunc|DateFormat|IsNull|IsNotNull|Int|Number|Text|Sqrt|Power|Concat|In|GrandTotal|CumulativeAvg|Weekday|Mod|DateTrunc)$/.test(p)
  );
  if (allTablePrefixes.length > 1 && warnings) {
    const tableNames = allTablePrefixes.join(', ');
    warnings.push(`⚠ Calculated column "${measureName}": references columns from multiple tables (${tableNames}). Column context has been simplified — verify formula references the correct columns.`);
  }
  f = replaceOutsideDaxLiterals(f, /'[^']+'\[([^\]]+)\]/g, (_m, ref) => `[${ref}]`);
  // Also handle unquoted: Table[Column] → [Column]
  f = replaceOutsideDaxLiterals(f, /\b[A-Za-z_]\w*\[([^\]]+)\]/g, (_m, ref) => `[${ref}]`);

  // Strip RELATED([col]) → [col] AFTER table-prefix normalization, so that
  // RELATED('dim'[X]) (which the line 121 regex couldn't match because of
  // the quoted prefix) gets unwrapped here.
  f = f.replace(/\bRELATED\s*\(\s*(\[[^\]]+\])\s*\)/gi, '$1');

  // DAX `&` auto-coerces operands to text; Sigma's `&` does NOT ("Argument 1
  // invalid for function '&'. Expected text; received number." at QUERY time —
  // the spec posts clean and the column errors silently later). Wrap bare
  // column-ref operands of a concat in Text(): identity for text columns,
  // the missing cast for numeric ones (e.g. [MonthID]&"01" — the MS Retail
  // Analysis Sample's Time join key).
  f = f.replace(/(\[[^\]]+\])(\s*&)/g, 'Text($1)$2');
  f = f.replace(/(&\s*)(\[[^\]]+\])/g, '$1Text($2)');

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
  // A one-row "today" calc table (SELECTCOLUMNS(ROW("Date", TODAY()), …) or the
  // { TODAY() } constructor) → a REAL CURRENT_DATE/CURRENT_TIMESTAMP SQL element,
  // never a `SELECT 1 AS _placeholder` stub (dax-fidelity #11).
  if (/\b(TODAY|NOW)\s*\(\s*\)/i.test(dax) && !/\bGENERATESERIES|\bADDCOLUMNS/i.test(dax) && !/\[[^\]]+\]/.test(dax)) {
    const isNow = /\bNOW\s*\(\s*\)/i.test(dax);
    const col = seriesColName || (isNow ? 'Now' : 'Date');
    return { ok: true, sql: `SELECT ${isNow ? 'CURRENT_TIMESTAMP' : 'CURRENT_DATE'} AS "${col}"` };
  }
  // A hardcoded literal list — the DAX table constructor { v1, v2, … } (or a
  // DATATABLE of literals) → a REAL VALUES/UNION ALL element, not a placeholder (#11).
  const braceList = dax.match(/\{\s*([^{}]*?)\s*\}/);
  if (braceList && braceList[1].trim() && !/\[[^\]]+\]/.test(braceList[1])) {
    const items = splitInList(braceList[1]).filter(Boolean);
    const isLiteral = (v: string) => /^(".*"|'.*'|-?\d+(\.\d+)?)$/.test(v.trim());
    if (items.length && items.every(isLiteral)) {
      const col = seriesColName || 'Value';
      const rows = items.map(v => {
        const tkn = v.trim();
        const sqlv = /^["']/.test(tkn) ? `'${tkn.slice(1, -1).replace(/'/g, "''")}'` : tkn;
        return `SELECT ${sqlv} AS "${col}"`;
      }).join(' UNION ALL ');
      return { ok: true, sql: rows };
    }
  }
  // Find GENERATESERIES(start, stop[, step]) anywhere in the expression.
  const gm = dax.match(/\bGENERATESERIES\s*\(/i);
  if (!gm) {
    return { ok: false, reason: 'DAX calculated table is not a GENERATESERIES / CALENDAR / TODAY / literal-list constructor — no warehouse source exists; recreate manually as a Sigma SQL element or input table.' };
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

// ── DAX ranking / window → SQL window-function lowering ─────────────────────
// DAX RANKX / RANK / the COUNTROWS(FILTER(..EARLIER..)) dense-rank idiom land in
// Sigma DM calc columns / metrics where Sigma's window functions (Rank/RankDense)
// SILENTLY ERROR (feedback_sigma_window_functions.md) — the whole chart blanks.
// Mirroring src/quicksight.ts + src/tableau.ts, we lower each translatable rank/
// window to a kind:'sql' helper element carrying an explicit
//   <RANK|DENSE_RANK>() OVER (PARTITION BY <p> ORDER BY <measure> DESC)
// (or SUM(..) OVER (ORDER BY .. ROWS UNBOUNDED PRECEDING) for running totals)
// projected over the model's underlying warehouse table.
//
// DAX carries no explicit partition/order args like QuickSight — they're inferred
// from the FILTER CONTEXT the rank removes:
//   RANKX(ALL(T[Dim]), <m>[, , DESC|ASC[, DENSE]])
//     ALL(T[Dim]) removes filter context on Dim → the rank is computed across all
//     values of Dim. So Dim is the BASE GRAIN (GROUP BY), ORDER BY = <m>, and the
//     PARTITION is the report visual's grouping dims MINUS the ranked Dim (empty
//     here = a single global ranking, which is the overwhelmingly common case).
//   COUNTROWS(FILTER(T, T[p]=EARLIER(T[p]) && T[x] (>|<) EARLIER(T[x]))) + 1
//     is the canonical row-level dense-rank idiom (a calc COLUMN): PARTITION BY p,
//     ORDER BY x DESC|ASC, evaluated at ROW grain (no GROUP BY). DENSE_RANK().
// Where the rank scope / order measure can't be resolved we DEGRADE (return false
// and let the existing Null+warning path run) — genuinely-no-equivalent DAX
// (ALLSELECTED with a sliced visual, multi-filter CALCULATE) stays flagged.

// Uppercase, SQL-identifier-safe form of a raw PBI column ref. Handles the
// qualified Table[Col] / 'Table'[Col] / [Col] forms — extracts just the bracketed
// column name (the warehouse column), discarding the table qualifier.
function _pbiColToSql(raw: string): string {
  const r = (raw || '').trim();
  const m = r.match(/\[([^\]]+)\]\s*$/); // last [..] = the column
  const col = m ? m[1] : r;
  return col.replace(/[^A-Za-z0-9_]/g, '_').toUpperCase();
}

// A resolver: PBI measure display-name → its inner SQL aggregate, derived from
// the model's own simple single-aggregate measures (e.g. "Total Salary" =
// SUM(EMPLOYEES[ANNUAL_SALARY]) → "SUM(ANNUAL_SALARY)"). Returns null for a
// measure that is itself a window / CALCULATE / ratio (can't be a base agg).
export interface PBIMeasureAggMap { [displayName: string]: { fn: string; colSql: string } | null; }

// Parse a single simple aggregate DAX measure into { fn, colSql }. Handles the
// SUM/AVERAGE/MIN/MAX/COUNT/DISTINCTCOUNT(Table[Col]) and COUNTROWS(Table) forms.
function pbiParseSimpleAgg(dax: string): { fn: string; colSql: string } | null {
  const d = (dax || '').trim();
  let m = d.match(/^(SUM|AVERAGE|MIN|MAX|COUNT|DISTINCTCOUNT)\s*\(\s*'?[^'\[]*'?\[([^\]]+)\]\s*\)$/i);
  if (m) {
    const fnMap: Record<string, string> = { SUM: 'SUM', AVERAGE: 'AVG', MIN: 'MIN', MAX: 'MAX', COUNT: 'COUNT', DISTINCTCOUNT: 'COUNT_DISTINCT' };
    return { fn: fnMap[m[1].toUpperCase()], colSql: _pbiColToSql(m[2]) };
  }
  // COUNTROWS('Table') / COUNTROWS(Table) → COUNT(*)
  if (/^COUNTROWS\s*\(\s*'?[A-Za-z_][\w ]*'?\s*\)$/i.test(d)) {
    return { fn: 'COUNT', colSql: '*' };
  }
  return null;
}

// Build the measure→agg resolver across the whole model (used to resolve a
// RANKX order-measure ref like [Total Salary] to its SQL aggregate).
function pbiBuildMeasureAggMap(model: any): PBIMeasureAggMap {
  const out: PBIMeasureAggMap = {};
  for (const t of (model.tables || [])) {
    for (const meas of (t.measures || [])) {
      const dax = Array.isArray(meas.expression) ? meas.expression.join('\n') : String(meas.expression || '');
      out[meas.name] = pbiParseSimpleAgg(dax);
    }
  }
  return out;
}

interface PBIWindowResult {
  _isWindow: true;
  // RANK/DENSE_RANK = ranking; AGG_RUNNING = <fn>(col) OVER (… ORDER BY …)
  // (running total / window count incl. ties via RANGE default frame);
  // AGG_PARTITION = <fn>(col) OVER (PARTITION BY …) (group total / peer count).
  op: 'RANK' | 'DENSE_RANK' | 'AGG_RUNNING' | 'AGG_PARTITION';
  grainRaw: string[];          // base GROUP BY dim raw names (the ranked dim). [] = row-level (no GROUP BY).
  partitionRaw: string[];      // PARTITION BY raw names
  orderFn: string;             // SQL agg fn for the ORDER BY measure (SUM/AVG/COUNT/COUNT_DISTINCT) — '' at row grain
  orderColSql: string;         // SQL identifier of the order measure's column, or a raw row column at row grain
  orderDir: 'ASC' | 'DESC';
  rowLevel: boolean;           // true = COUNTROWS-EARLIER row-rank (no GROUP BY)
  valueFn?: string;            // AGG_* ops: SQL agg fn over the value column (SUM/AVG/MIN/MAX/COUNT)
  valueColSql?: string;        // AGG_* ops: the aggregated column ('*' for COUNT(*))
}

// Parse a RANKX measure DAX into a structured rank. Resolves the order-measure
// ref via the model measure-agg map. Returns null to DEGRADE.
//   RANKX(ALL(T[Dim]), <orderMeasureRef-or-agg>[, , DESC|ASC[, DENSE]])
function pbiParseRankx(dax: string, measureAggMap: PBIMeasureAggMap): PBIWindowResult | null {
  const d = (dax || '').trim();
  const rm = d.match(/^RANKX\s*\(/i);
  if (!rm) return null;
  const { args, endPos } = splitCallArgs(d, rm.index! + rm[0].length);
  // RANKX must be the whole expression (a bare ranking measure), not wrapped.
  if (endPos < d.length) return null;
  if (args.length < 2) return null;
  // arg0 = the table-scope: ALL(T[Dim]) → rank across Dim. Whole-table ALL(T)
  // (no [col]) has no determinable grain → degrade.
  const scope = args[0].trim();
  const sm = scope.match(/^ALL\s*\(\s*'?[A-Za-z_][\w ]*'?\s*\[([^\]]+)\]\s*\)$/i);
  if (!sm) return null; // ALLSELECTED, whole-table ALL, VALUES(), etc. → degrade
  const rankedDim = _pbiColToSql(sm[1]);
  // arg1 = the order expression: a measure ref [m] or an inline aggregate.
  const orderExpr = args[1].trim();
  let orderFn = '', orderColSql = '';
  const refM = orderExpr.match(/^\[([^\]]+)\]$/);
  if (refM) {
    const agg = measureAggMap[refM[1]];
    if (!agg) return null; // order measure isn't a simple aggregate → degrade
    orderFn = agg.fn; orderColSql = agg.colSql;
  } else {
    const inline = pbiParseSimpleAgg(orderExpr);
    if (!inline) return null;
    orderFn = inline.fn; orderColSql = inline.colSql;
  }
  // arg2 = value/scalar (ignored). arg3 = order (ASC/DESC). arg4 = ties (DENSE/SKIP).
  let dir: 'ASC' | 'DESC' = 'DESC';
  let dense = false;
  for (let i = 2; i < args.length; i++) {
    const a = args[i].trim().toUpperCase();
    if (a === 'ASC' || a === 'DESC') dir = a as 'ASC' | 'DESC';
    else if (a === 'DENSE') dense = true;
    else if (a === 'SKIP') dense = false;
  }
  return {
    _isWindow: true,
    op: dense ? 'DENSE_RANK' : 'RANK',
    grainRaw: [rankedDim],
    partitionRaw: [],
    orderFn, orderColSql, orderDir: dir,
    rowLevel: false,
  };
}

// Parse the COUNTROWS(FILTER(T, p=EARLIER(p) && x (>|<) EARLIER(x))) + 1 idiom
// (a calc COLUMN row-level dense rank). Returns null to DEGRADE.
function pbiParseEarlierRank(dax: string): PBIWindowResult | null {
  const d = (dax || '').trim();
  const cm = d.match(/^COUNTROWS\s*\(\s*FILTER\s*\(/i);
  if (!cm) return null;
  const filterOpen = cm.index! + cm[0].length;
  const { args: filterArgs, endPos: filterEnd } = splitCallArgs(d, filterOpen);
  if (filterArgs.length < 2) return null;
  // COUNTROWS close + trailing "+ 1"
  let j = filterEnd;
  while (j < d.length && /\s/.test(d[j])) j++;
  if (d[j] !== ')') return null;
  const after = d.slice(j + 1).trim();
  if (!/^\+\s*1$/.test(after)) return null;
  const pred = filterArgs.slice(1).join(', ');
  // ranked term: <ref> (>|<) EARLIER(<ref>)
  const cmp = pred.match(/(['"]?[\w ]*'?\[[^\]]+\]|\[[^\]]+\])\s*(>|<)\s*EARLIER\s*\(\s*([^)]+?)\s*\)/i);
  if (!cmp) return null;
  const orderColSql = _pbiColToSql(cmp[1]);
  const dir: 'ASC' | 'DESC' = cmp[2] === '>' ? 'DESC' : 'ASC';
  // partition terms: any <ref> = EARLIER(<ref>) (split on &&)
  const partitionRaw: string[] = [];
  for (const term of pred.split(/&&/)) {
    const pm = term.match(/(['"]?[\w ]*'?\[[^\]]+\]|\[[^\]]+\])\s*=\s*EARLIER\s*\(\s*[^)]+?\s*\)/i);
    if (pm) partitionRaw.push(_pbiColToSql(pm[1]));
  }
  return {
    _isWindow: true,
    op: 'DENSE_RANK', // the idiom counts strictly-greater rows + 1 = dense rank
    grainRaw: [],     // row level — no GROUP BY
    partitionRaw,
    orderFn: '', orderColSql, orderDir: dir,
    rowLevel: true,
  };
}

// ── Family 2 (beads-sigma-fah8): bare-EARLIER calc-column idioms → windows ──
// Parse the FILTER predicate of an EARLIER idiom into partition (equality) terms
// + at most one ordering (inequality) term. Returns null when any term falls
// outside the recognized shapes (degrade → flag-not-drop).
function _parseEarlierTerms(pred: string): { partition: string[]; orderCol: string | null; orderDir: 'ASC' | 'DESC' } | null {
  const partition: string[] = [];
  let orderCol: string | null = null;
  let orderDir: 'ASC' | 'DESC' = 'ASC';
  for (const termRaw of pred.split(/&&/)) {
    const t = termRaw.trim();
    if (!t) continue;
    const eq = t.match(/^(['"]?[\w ]*'?\[[^\]]+\]|\[[^\]]+\])\s*=\s*EARLIER\s*\(\s*[^)]+?\s*\)$/i);
    if (eq) { partition.push(_pbiColToSql(eq[1])); continue; }
    const cmp = t.match(/^(['"]?[\w ]*'?\[[^\]]+\]|\[[^\]]+\])\s*(<=|>=)\s*EARLIER\s*\(\s*[^)]+?\s*\)$/i);
    if (cmp && !orderCol) {
      // <= EARLIER(d): accumulate rows at-or-below the current value → ORDER ASC;
      // >= → ORDER DESC. The SQL default frame with ORDER BY is RANGE UNBOUNDED
      // PRECEDING..CURRENT ROW, which includes TIES — exactly the <=/>= semantics.
      // (Strict </> excludes the current tie group — not this idiom; degrade.)
      orderCol = _pbiColToSql(cmp[1]);
      orderDir = cmp[2] === '<=' ? 'ASC' : 'DESC';
      continue;
    }
    return null;
  }
  return { partition, orderCol, orderDir };
}

// FILTER scope must be a plain table or whole-table ALL (calc columns carry no
// filter context, so FILTER(T,…) and FILTER(ALL(T),…) coincide at row grain).
function _earlierScopeOk(scope: string): boolean {
  const s = scope.trim();
  return /^'?[A-Za-z_][\w ]*'?$/.test(s) || /^ALL\s*\(\s*'?[A-Za-z_][\w ]*'?\s*\)$/i.test(s);
}

// Parse the next-most-common bare-EARLIER calc-column idioms (beyond the
// rank idiom handled by pbiParseEarlierRank). All row-level. Returns null to
// DEGRADE (the formula layer then flags with the DAX preserved).
//   (a) running total:
//       CALCULATE(<AGG>(T[v]), FILTER(ALL(T)|T, [p]=EARLIER([p])* && [d] <= EARLIER([d])))
//       SUMX(FILTER(ALL(T)|T, …same…), T[v])
//         → <AGG>(v) OVER (PARTITION BY p ORDER BY d ASC|DESC)
//   (b) group share / group total: same shapes with ONLY equality terms
//         → <AGG>(v) OVER (PARTITION BY p)
//   (c) peer count: COUNTROWS(FILTER(ALL(T)|T, only-equality terms))
//         → COUNT(*) OVER (PARTITION BY p)
//       COUNTROWS(FILTER(…, [x] >=|<= EARLIER([x]))) (NO trailing +1)
//         → COUNT(*) OVER (PARTITION BY p ORDER BY x DESC|ASC) — the RANGE
//           default frame counts the at-or-ahead peer set including ties EXACTLY.
export function pbiParseEarlierWindow(dax: string): PBIWindowResult | null {
  const d = (dax || '').trim();
  if (!/\bEARLIER\s*\(/i.test(d)) return null;
  const AGG_MAP: Record<string, string> = { SUM: 'SUM', AVERAGE: 'AVG', MIN: 'MIN', MAX: 'MAX' };

  const build = (fn: string, valueColSql: string, filterExpr: string): PBIWindowResult | null => {
    const fm = filterExpr.trim().match(/^FILTER\s*\(/i);
    if (!fm) return null;
    const fr = splitCallArgs(filterExpr.trim(), fm[0].length);
    if (fr.args.length < 2 || fr.endPos !== filterExpr.trim().length) return null;
    if (!_earlierScopeOk(fr.args[0])) return null;
    const terms = _parseEarlierTerms(fr.args.slice(1).join(', '));
    if (!terms) return null;
    if (!terms.orderCol && !terms.partition.length) return null; // whole-table total → GrandTotal path, not a window
    return {
      _isWindow: true,
      op: terms.orderCol ? 'AGG_RUNNING' : 'AGG_PARTITION',
      grainRaw: [],
      partitionRaw: terms.partition,
      orderFn: '',
      orderColSql: terms.orderCol || '',
      orderDir: terms.orderDir,
      rowLevel: true,
      valueFn: fn,
      valueColSql,
    };
  };

  // (a)/(b) CALCULATE(<AGG>(T[v]), FILTER(…)) — must be the WHOLE expression
  let m = d.match(/^CALCULATE\s*\(/i);
  if (m) {
    const { args, endPos } = splitCallArgs(d, m[0].length);
    if (endPos !== d.length || args.length !== 2) return null;
    const am = args[0].trim().match(/^(SUM|AVERAGE|MIN|MAX)\s*\(\s*('?[^'\[]*'?\[[^\]]+\])\s*\)$/i);
    if (!am) return null;
    return build(AGG_MAP[am[1].toUpperCase()], _pbiColToSql(am[2]), args[1]);
  }
  // (a)/(b) SUMX/AVERAGEX/MINX/MAXX(FILTER(…), T[v])
  m = d.match(/^(SUMX|AVERAGEX|MINX|MAXX)\s*\(/i);
  if (m) {
    const { args, endPos } = splitCallArgs(d, m[0].length);
    if (endPos !== d.length || args.length !== 2) return null;
    const body = args[1].trim();
    const bm = body.match(/^('?[^'\[]*'?\[[^\]]+\])$/);
    if (!bm) return null; // body must be a bare column ref
    const fnMap: Record<string, string> = { SUMX: 'SUM', AVERAGEX: 'AVG', MINX: 'MIN', MAXX: 'MAX' };
    return build(fnMap[m[1].toUpperCase()], _pbiColToSql(bm[1]), args[0]);
  }
  // (c) COUNTROWS(FILTER(…)) with NO trailing +1 (that's the rank idiom)
  m = d.match(/^COUNTROWS\s*\(/i);
  if (m) {
    const { args, endPos } = splitCallArgs(d, m[0].length);
    if (endPos !== d.length || args.length !== 1) return null;
    return build('COUNT', '*', args[0]);
  }
  return null;
}

// ── PBI window helper-element registry (shared kind:'sql' elements) ─────────
export interface PBIWindowContext {
  helpers: Map<string, PBIWindowHelper>;
  usedAliases: Set<string>;
  extraElements: SigmaElement[];
  connectionId: string;
}
interface PBIWindowHelper {
  element: SigmaElement;
  grainRaw: string[];          // base GROUP BY dims (empty = row level)
  partitionRaw: string[];
  rowLevel: boolean;
  innerAggs: Record<string, { alias: string }>; // base aggregates keyed fn::colSql
  windowAliases: Set<string>;
  overParts: string[];         // "<over sql> AS <ALIAS>"
  baseFromSql: string;         // fully-qualified table or "(<custom sql>)"
  rowCols: Set<string>;        // row-level passthrough cols (partition + order) for row grain
}

function _pbiWindowAlias(name: string, used: Set<string>): string {
  let b = (name || 'WIN_VAL').toUpperCase().replace(/[^A-Z0-9_]+/g, '_').replace(/^_+|_+$/g, '').replace(/_+/g, '_');
  if (!b) b = 'WIN_VAL';
  let a = b, n = 2;
  while (used.has(a)) a = `${b}_${n++}`;
  used.add(a);
  return a;
}

function pbiRegisterInnerAgg(helper: PBIWindowHelper, fn: string, colSql: string): string {
  const key = `${fn}::${colSql}`;
  if (helper.innerAggs[key]) return helper.innerAggs[key].alias;
  const base = colSql === '*' ? 'CNT' : colSql.replace(/[^A-Z0-9_]/gi, '_').toUpperCase();
  let alias = base || 'VAL', n = 2;
  while (helper.windowAliases.has(alias) || Object.values(helper.innerAggs).some(v => v.alias === alias)) {
    alias = `${base}_${n++}`;
  }
  helper.innerAggs[key] = { alias };
  return alias;
}

// Resolve the FROM source for the helper SQL from the source element.
function pbiResolveBaseFrom(srcEl: SigmaElement): string | null {
  const src = srcEl.source || {};
  if (src.kind === 'warehouse-table' && Array.isArray(src.path) && src.path.length) {
    return src.path.join('.');
  }
  if (src.kind === 'sql' && typeof src.statement === 'string' && src.statement.trim()
      && !src.statement.includes('_placeholder') && !/^--/.test(src.statement.trim())) {
    return `(${src.statement.trim().replace(/;\s*$/, '')})`;
  }
  return null;
}

// Known raw (uppercased SQL) column names available on a source element.
function pbiKnownRawColumns(srcEl: SigmaElement): Set<string> {
  const out = new Set<string>();
  for (const c of (srcEl.columns || [])) {
    const fm = (c.formula || '').match(/^\[[^\]\/]+\/([^\]]+)\]$/);
    if (fm) out.add(_pbiColToSql(fm[1]));
  }
  return out;
}

/**
 * Lower a parsed PBI rank/window onto a shared kind:'sql' helper element.
 * Returns true if translated, false to DEGRADE (caller emits Null + warning).
 *   - srcEl  = the warehouse-table element the rank measure/column belongs to.
 *   - calcName = the metric/column display name (becomes the surfaced col).
 */
function lowerPBIWindowCalc(
  win: PBIWindowResult,
  calcName: string,
  srcEl: SigmaElement,
  winCtx: PBIWindowContext,
  warnings: string[],
): boolean {
  const baseFromSql = pbiResolveBaseFrom(srcEl);
  if (!baseFromSql) {
    warnings.push(`⚠ "${calcName}" (${win.op}): could not resolve a warehouse FROM source for "${srcEl.name}"; degraded to Null.`);
    return false;
  }
  // Validate partition / order / grain cols against the source's known columns.
  const known = pbiKnownRawColumns(srcEl);
  const checkCol = (c: string): boolean => c === '*' || known.size === 0 || known.has(c);
  for (const p of win.partitionRaw) {
    if (!checkCol(p)) { warnings.push(`⚠ "${calcName}" (${win.op}): partition column ${p} not found on "${srcEl.name}"; degraded to Null.`); return false; }
  }
  for (const g of win.grainRaw) {
    if (!checkCol(g)) { warnings.push(`⚠ "${calcName}" (${win.op}): rank dimension ${g} not found on "${srcEl.name}"; degraded to Null.`); return false; }
  }
  if (win.orderColSql && !checkCol(win.orderColSql)) {
    warnings.push(`⚠ "${calcName}" (${win.op}): order column ${win.orderColSql} not found on "${srcEl.name}"; degraded to Null.`);
    return false;
  }
  if (win.valueColSql && win.valueColSql !== '*' && !checkCol(win.valueColSql)) {
    warnings.push(`⚠ "${calcName}" (${win.op}): value column ${win.valueColSql} not found on "${srcEl.name}"; degraded to Null.`);
    return false;
  }

  // Helper grain/partition key: a row-level rank and an aggregated rank never
  // share a helper (different base shape).
  const grainKey = win.grainRaw.slice().sort().join(',');
  const partKey = win.partitionRaw.slice().sort().join(',');
  const key = `${baseFromSql}||${win.rowLevel ? 'ROW' : 'AGG'}||${grainKey}||${partKey}`;
  // Project grain dims (aggregated rank) or partition+order+value cols (row
  // grain) so the workbook can group/join on them. SQL element →
  // [Custom SQL/ALIAS] refs.
  const passthrough = win.rowLevel
    ? [...new Set([
        ...win.partitionRaw,
        ...(win.orderColSql ? [win.orderColSql] : []),
        ...(win.valueColSql && win.valueColSql !== '*' ? [win.valueColSql] : []),
      ])]
    : win.grainRaw;
  const OP_LABEL: Record<string, string> = {
    DENSE_RANK: 'Dense Rank', RANK: 'Rank',
    AGG_RUNNING: 'Running', AGG_PARTITION: 'Group',
  };
  let helper = winCtx.helpers.get(key);
  if (!helper) {
    const cols: SigmaColumn[] = [];
    const order: string[] = [];
    for (const g of passthrough) {
      if (g === '*') continue;
      const id = sigmaShortId();
      cols.push({ id, name: sigmaDisplayName(g), formula: `[Custom SQL/${g}]` });
      order.push(id);
    }
    const el: SigmaElement = {
      id: sigmaShortId(),
      kind: 'table',
      name: `${OP_LABEL[win.op] || 'Window'} ${(win.partitionRaw[0] || win.grainRaw[0] || win.orderColSql || 'Window')}`,
      source: { connectionId: winCtx.connectionId, kind: 'sql', statement: '__PBI_WINDOW_PLACEHOLDER__' },
      columns: cols,
      order,
    };
    helper = {
      element: el, grainRaw: win.grainRaw, partitionRaw: win.partitionRaw, rowLevel: win.rowLevel,
      innerAggs: {}, windowAliases: new Set(), overParts: [], baseFromSql,
      rowCols: new Set(passthrough.filter(c => c !== '*')),
    };
    winCtx.helpers.set(key, helper);
    winCtx.extraElements.push(el);
  } else if (win.rowLevel) {
    // Reusing a shared helper: surface any passthrough cols this calc needs that
    // earlier calcs on the same partition didn't project (e.g. a different value
    // or order column).
    for (const g of passthrough) {
      if (g === '*' || helper.rowCols.has(g)) continue;
      helper.rowCols.add(g);
      const id = sigmaShortId();
      helper.element.columns.push({ id, name: sigmaDisplayName(g), formula: `[Custom SQL/${g}]` });
      helper.element.order.push(id);
    }
  }

  // Build the OVER clause.
  const partBy = win.partitionRaw.length ? `PARTITION BY ${win.partitionRaw.join(', ')}` : '';
  let overSql: string;
  if (win.op === 'AGG_RUNNING' || win.op === 'AGG_PARTITION') {
    // Family 2 (beads-sigma-fah8): running total / group total / peer count.
    // The default frame with ORDER BY is RANGE UNBOUNDED PRECEDING..CURRENT ROW
    // (ties included) — exactly the <=/>= EARLIER accumulation semantics.
    const valExpr = win.valueColSql === '*' ? 'COUNT(*)' : `${win.valueFn}(${win.valueColSql})`;
    const overBody = win.op === 'AGG_RUNNING'
      ? [partBy, `ORDER BY ${win.orderColSql} ${win.orderDir}`].filter(Boolean).join(' ')
      : partBy;
    overSql = `${valExpr} OVER (${overBody})`;
  } else {
    const fn = win.op === 'DENSE_RANK' ? 'DENSE_RANK' : 'RANK';
    let orderExprSql: string;
    if (win.rowLevel) {
      // row grain: ORDER BY the raw column directly.
      orderExprSql = win.orderColSql;
    } else {
      // aggregated grain: ORDER BY a pre-aggregated alias registered on the helper.
      orderExprSql = pbiRegisterInnerAgg(helper, win.orderFn, win.orderColSql);
    }
    overSql = `${fn}() OVER (${[partBy, `ORDER BY ${orderExprSql} ${win.orderDir}`].filter(Boolean).join(' ')})`;
  }
  const winAlias = _pbiWindowAlias(calcName, winCtx.usedAliases);
  helper.overParts.push(`${overSql} AS ${winAlias}`);
  helper.windowAliases.add(winAlias);
  const calcId = sigmaShortId();
  helper.element.columns.push({ id: calcId, name: stripParens(sigmaDisplayName(calcName)), formula: `[Custom SQL/${winAlias}]` });
  helper.element.order.push(calcId);
  warnings.push(`✅ "${calcName}" (${win.op}) → SQL window helper "${helper.element.name}" alias ${winAlias} (${win.rowLevel ? 'row-level' : 'grouped by ' + win.grainRaw.join(', ')}${win.partitionRaw.length ? ', partition ' + win.partitionRaw.join(', ') : ''}${win.op === 'AGG_RUNNING' ? ', ordered by ' + win.orderColSql + ' ' + win.orderDir : ''}).`);
  return true;
}

// Finalize a helper into its real WITH base AS (...) SELECT ... OVER statement.
function finalizePBIWindowHelper(helper: PBIWindowHelper): void {
  if (helper.rowLevel) {
    // Row grain: no GROUP BY. SELECT the passthrough cols + the OVER expressions
    // straight off the base table.
    const proj = [...helper.rowCols, ...helper.overParts];
    helper.element.source.statement = `SELECT ${proj.join(', ')} FROM ${helper.baseFromSql}`;
    return;
  }
  // Aggregated grain: GROUP BY the grain dims, compute base aggregates, then OVER.
  const groupCols: string[] = [];
  const seen = new Set<string>();
  for (const g of helper.grainRaw) {
    if (g === '*' || seen.has(g)) continue; seen.add(g);
    groupCols.push(g);
  }
  const selectParts: string[] = [...groupCols];
  for (const k of Object.keys(helper.innerAggs)) {
    const [fn, colSql] = k.split('::');
    const a = helper.innerAggs[k];
    const sqlFn = fn === 'COUNT_DISTINCT' ? `COUNT(DISTINCT ${colSql})`
      : colSql === '*' ? 'COUNT(*)' : `${fn}(${colSql})`;
    selectParts.push(`${sqlFn} AS ${a.alias}`);
  }
  const groupBy = groupCols.length ? ` GROUP BY ${groupCols.map((_, i) => i + 1).join(', ')}` : '';
  const baseSelect = `SELECT ${selectParts.join(', ')} FROM ${helper.baseFromSql}${groupBy}`;
  const innerProj = [...groupCols, ...Object.values(helper.innerAggs).map(v => v.alias)];
  const outerProj = innerProj.concat(helper.overParts);
  helper.element.source.statement = `WITH base AS (${baseSelect}) SELECT ${outerProj.join(', ')} FROM base`;
}

// ── Family 1 (beads-sigma-fah8): USERELATIONSHIP inside CALCULATE ───────────

// Parse a qualified PBI column ref 'Table'[Col] / Table[Col].
function _pbiParseQualifiedRef(s: string): { table: string; column: string } | null {
  const m = (s || '').trim().match(/^'([^']+)'\s*\[([^\]]+)\]$|^([A-Za-z_][\w ]*?)\s*\[([^\]]+)\]$/);
  if (!m) return null;
  return { table: (m[1] || m[3]).trim(), column: (m[2] || m[4]).trim() };
}

export interface PBIUseRelPair { a: { table: string; column: string }; b: { table: string; column: string } }

// Strip every USERELATIONSHIP(col1, col2) filter arg out of a DAX expression,
// returning the cleaned DAX plus the referenced column pairs. A CALCULATE left
// with no filter args, CALCULATE(x), is unwrapped to x by the conditional-
// aggregate rewrite. The TMSL model knows its inactive relationships — the
// convert layer matches each pair to one and activates it as an alternate join
// path (distinctly named relationship + alternate-keyed derived-view columns).
export function extractUseRelationships(dax: string): { dax: string; pairs: PBIUseRelPair[] } {
  let f = dax;
  const pairs: PBIUseRelPair[] = [];
  const re = /\bUSERELATIONSHIP\s*\(/gi;
  for (let guard = 0; guard < 20; guard++) {
    re.lastIndex = 0;
    const m = re.exec(f);
    if (!m) break;
    const { args, endPos } = splitCallArgs(f, m.index + m[0].length);
    if (args.length >= 2) {
      const a = _pbiParseQualifiedRef(args[0]);
      const b = _pbiParseQualifiedRef(args[1]);
      if (a && b) pairs.push({ a, b });
    }
    // Splice out the call plus ONE adjacent comma (leading if present, else trailing).
    let start = m.index, end = endPos;
    let i = start - 1;
    while (i >= 0 && /\s/.test(f[i])) i--;
    if (f[i] === ',') start = i;
    else {
      let j = end;
      while (j < f.length && /\s/.test(f[j])) j++;
      if (f[j] === ',') end = j + 1;
    }
    f = f.slice(0, start) + f.slice(end);
  }
  return { dax: f, pairs };
}

// Strip DAX comments — /* block */ and // line — while preserving STRING LITERALS.
//
// Nothing stripped comments before, and it dropped real measures. Found while chasing why
// two measures still failed after the CROSSFILTER fix: their DAX carried
//   /*use relationship for submission dim ... filtering with crossfilter ...*/
// and the CALCULATE filter-predicate detector matched the words INSIDE the comment
// ("relationship", "filtering", "crossfilter"), rejecting the measure as having a
// filter-context predicate. A measure was being dropped because of a COMMENT. 19 measures
// across 4 real models carry comments, so this is a general hazard for every downstream
// regex that inspects DAX text.
//
// String-literal awareness is required: a "//" inside "https://host/path" is DATA. DAX
// strings are double-quoted with "" as the escape, which this honors.
export function stripDaxComments(dax: string): string {
  const src = String(dax ?? '');
  let out = '';
  let i = 0;
  while (i < src.length) {
    const c = src[i];
    if (c === '"') {                       // string literal — copy verbatim
      out += c; i++;
      while (i < src.length) {
        if (src[i] === '"') {
          if (src[i + 1] === '"') { out += '""'; i += 2; continue; }  // escaped quote
          out += '"'; i++; break;
        }
        out += src[i]; i++;
      }
      continue;
    }
    if (c === '/' && src[i + 1] === '*') {  // block comment -> a single space
      const end = src.indexOf('*/', i + 2);
      i = end === -1 ? src.length : end + 2;
      out += ' ';
      continue;
    }
    if (c === '/' && src[i + 1] === '/') {  // line comment -> newline preserved
      while (i < src.length && src[i] !== '\n') i++;
      continue;
    }
    out += c; i++;
  }
  return out;
}

export interface PBICrossFilter { a: { table: string; column: string }; b: { table: string; column: string }; direction: string }

// Strip every CROSSFILTER(col1, col2, direction) MODIFIER out of a DAX expression,
// returning the cleaned DAX plus the endpoint pairs and the requested direction.
//
// CROSSFILTER is the sibling of USERELATIONSHIP: both are CALCULATE *modifiers* that
// change how a RELATIONSHIP behaves, not filters the aggregate reads. Its two column
// arguments are relationship ENDPOINTS. USERELATIONSHIP was already stripped here;
// CROSSFILTER was not, so its endpoint columns survived into the emitted metric formula,
// the cross-table guard saw a column that is not on this element, and the whole measure
// was DROPPED. Measured on real models: 2 measures lost to a
// `CROSSFILTER(FACT[AGENt_KEY], DIM[CHILD_ID], None)` argument unrelated to the SUM.
//
// Sigma has no cross-filter-direction concept, so stripping is the correct translation —
// but it changes filter semantics, so the caller WARNS rather than dropping it silently.
export function extractCrossFilters(dax: string): { dax: string; pairs: PBICrossFilter[] } {
  let f = dax;
  const pairs: PBICrossFilter[] = [];
  const re = /\bCROSSFILTER\s*\(/gi;
  for (let guard = 0; guard < 20; guard++) {
    re.lastIndex = 0;
    const m = re.exec(f);
    if (!m) break;
    const { args, endPos } = splitCallArgs(f, m.index + m[0].length);
    if (args.length >= 2) {
      const a = _pbiParseQualifiedRef(args[0]);
      const b = _pbiParseQualifiedRef(args[1]);
      const dir = (args[2] || '').trim().replace(/^["']|["']$/g, '') || 'Both';
      if (a && b) pairs.push({ a, b, direction: dir });
    }
    // Splice out the call plus ONE adjacent comma (leading if present, else trailing) —
    // same shape as extractUseRelationships so a CALCULATE left with no filter args is
    // unwrapped to its bare aggregate downstream.
    let start = m.index, end = endPos;
    let i = start - 1;
    while (i >= 0 && /\s/.test(f[i])) i--;
    if (f[i] === ',') start = i;
    else {
      let j = end;
      while (j < f.length && /\s/.test(f[j])) j++;
      if (f[j] === ',') end = j + 1;
    }
    f = f.slice(0, start) + f.slice(end);
  }
  return { dax: f, pairs };
}

// Find the model relationship matching a USERELATIONSHIP column pair (either
// argument order).
function findModelRelationship(model: any, p: PBIUseRelPair): any | null {
  for (const r of (model.relationships || [])) {
    const fwd = r.fromTable === p.a.table && r.fromColumn === p.a.column && r.toTable === p.b.table && r.toColumn === p.b.column;
    const rev = r.fromTable === p.b.table && r.fromColumn === p.b.column && r.toTable === p.a.table && r.toColumn === p.a.column;
    if (fwd || rev) return r;
  }
  return null;
}

/** Strip parentheses from a name (parens collide with Sigma ref/function syntax). */
function stripParens(name: string): string {
  return (name || '').replace(/\s*\([^)]*\)/g, '').replace(/[()]/g, '').replace(/\s+/g, ' ').trim();
}

// ── Main conversion ───────────────────────────────────────────────────────────

export interface PowerBIConvertOptions {
  connectionId?: string;
  database?: string;
  schema?: string;
  // Target warehouse dialect. Snowflake/BigQuery (the default) fold unquoted
  // identifiers to UPPER; Databricks/Spark store them lower-case and bind only
  // against a lower-cased physical name/path. Pass 'databricks' (or the Sigma
  // connection `type`) so physical identifiers fold to the right case
  // (beads-sigma-lanq.7). Unset → UPPER (unchanged for existing callers).
  warehouseType?: string;
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
      // find a View carrying both the value column and a date column.
      // Compare on a normalized key: lastSeg() yields the Sigma DISPLAY name
      // ("Incident Id") while `col` is the raw DAX token ("INCIDENT_ID"), so a
      // plain uppercase compare misses every multi-word column (single-word
      // ones like HOURS coincidentally matched). Strip spaces/underscores.
      const normCol = (s: string) => (s || '').toUpperCase().replace(/[\s_]/g, '');
      let parent: any = null, valDisp = '', dateDisp = '';
      for (const v of views) {
        const vc = (v.columns || []).find((c: any) => normCol(lastSeg(c.formula)) === normCol(col));
        const dc = (v.columns || []).find((c: any) => /full date/i.test(viewColDisplay(c.formula)))
                || (v.columns || []).find((c: any) => /date/i.test(lastSeg(c.formula)) && !/key/i.test(lastSeg(c.formula)));
        if (vc && dc) { parent = v; valDisp = viewColDisplay(vc.formula); dateDisp = viewColDisplay(dc.formula); break; }
      }
      if (!parent) continue;
      const pn = parent.name; const b = (m.name || 'TI').replace(/[^a-zA-Z0-9]/g, '').slice(0, 14);
      // Fidelity check: the synthesized element groups by the fact's OWN date
      // column. If the source measure's date dimension is reached only through
      // an INACTIVE relationship (and no measure activates it via
      // USERELATIONSHIP), the original DAX never filters by that dimension —
      // it returns an unfiltered grand total per period. Our element is the
      // "intended" per-period result, but that is a real divergence; flag it.
      const refTables = [...dax.matchAll(/([A-Za-z_]\w*)\s*\[/g)].map(x => x[1]);
      const dimTable = refTables.length ? refTables[refTables.length - 1] : null;
      if (dimTable && dimTable !== t.name) {
        const rel = (model.relationships || []).find(
          (r: any) => r.fromTable === t.name && r.toTable === dimTable);
        if (rel && rel.isActive === false) {
          warnings.push(`⚠ "${m.name}": the ${t.name}→${dimTable} relationship is INACTIVE and no measure activates it (USERELATIONSHIP), so the original Power BI measure does not filter by ${dimTable} — it returns an unfiltered total per ${dimTable} period. The synthesized element groups by ${t.name}'s own date instead, yielding a true per-period prior-year. Confirm this matches intent (the source measure may be silently mis-modeled).`);
        }
      }
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

  // Physical-identifier casing. Databricks/Spark store identifiers lower-case and
  // bind only against a lower-cased physical name/path; Snowflake/BigQuery fold to
  // UPPER (the historical default). beads-sigma-lanq.7.
  const whLower = /\b(databricks|spark|hive|delta)\b/i.test(options.warehouseType || '');
  const physCasing: 'upper' | 'lower' = whLower ? 'lower' : 'upper';
  const physCase = (s: string) => whLower ? String(s).toLowerCase() : String(s).toUpperCase();
  const dbOverride = physCase(database || '');
  const schOverride = physCase(schema || '');
  const warnings: string[] = [];
  const security: SecurityRule[] = [];   // detected RLS/OLS — reported, not injected (architecture B)
  const elements: SigmaElement[] = [];
  const tableIdMap: Record<string, string> = {};
  const tableColMap: Record<string, Record<string, string>> = {};
  const allPbiToSigmaNames: Record<string, string> = {};
  // ── Cross-table TRIAGE support ────────────────────────────────────────────
  // `tableColMap` is filled INSIDE the table loop below, so at the moment table k
  // drops a cross-table measure, tables k+1..N are not in it yet. Triage needs a
  // whole-model view, so build one up front from the RAW model.
  // Indexed by BOTH raw and display name: at the drop site refs have already been
  // remapped to display names, but a column whose display name we derive slightly
  // differently still resolves via its raw name. PR 1 only produces a MESSAGE, so an
  // imperfect derivation degrades wording, never output. PR 2 must not rely on this.
  // `Object.create(null)` — NOT `{}` — because a real model can and does contain a
  // column literally named `toString`/`constructor`/`valueOf`/`hasOwnProperty`/etc.
  // Against a `{}`-backed map, `triageColumnOwners['toString']` resolves to the
  // inherited Function.prototype.toString, which is truthy, so the init guard below
  // never fires and `.includes` is called on a function — throwing, and crashing a
  // conversion that succeeds on `main`. A null-prototype object has no inherited
  // members at all, so every key behaves like a plain data slot.
  const triageColumnOwners: Record<string, string[]> = Object.create(null);
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
  // Whole-model metric name -> declaring table, built up front for the same reason
  // as triageColumnOwners above: the cross-table drop loop below runs ONE TABLE AT
  // A TIME, so by the time table k's own metrics are checked, this is the only way
  // to know a "bad" ref names a metric declared on a DIFFERENT table at all — a
  // hard Sigma constraint (metrics cannot reference another element's metric, at
  // any join distance) that `triageCrossTable` has no basis to reason about, since
  // it isn't a column and isn't a same-element ref either. `Object.create(null)` —
  // NOT `{}` — for the same reason as triageColumnOwners: a real model can name a
  // measure `toString`/`constructor`/etc. First-wins on a name collision across
  // tables (rare; DAX measure names are usually unique per model).
  const allMetricOwner: Record<string, string> = Object.create(null);
  for (const _t of (model.tables || [])) {
    for (const _m of (_t.measures || [])) {
      if (_m?.name && !allMetricOwner[_m.name]) allMetricOwner[_m.name] = _t.name;
    }
  }
  // measure (PBI) name -> owning element id, for cross-table ratio detection
  // (beads-sigma-m1a). Includes measures later moved to the fact element.
  const measureToElementId: Record<string, string> = {};

  // ── Rank / window DAX → SQL window-function lowering context ──────────────
  // RANKX / the COUNTROWS(FILTER(..EARLIER..)) dense-rank idiom silently error in
  // Sigma DM metrics/calc cols; lower them to shared kind:'sql' helper elements
  // (SQL OVER clauses) instead of dropping to Null. measureAggMap resolves a
  // RANKX order-measure ref ([Total Salary]) to its inner SQL aggregate.
  const measureAggMap = pbiBuildMeasureAggMap(model);
  // bead qx16: model-wide measure name → raw DAX, so pbiDaxToSigma can inline a
  // bare measure ref used as CALCULATE's first arg (e.g. CALCULATE([Headcount], …)).
  const measureDaxMap: Record<string, string> = {};
  for (const t of (model.tables || [])) {
    for (const meas of (t.measures || [])) {
      measureDaxMap[meas.name] = Array.isArray(meas.expression)
        ? meas.expression.join('\n') : String(meas.expression || '');
    }
  }
  const winCtx: PBIWindowContext = {
    helpers: new Map(),
    usedAliases: new Set(),
    extraElements: [],
    connectionId: connectionId || '<CONNECTION_ID>',
  };

  // ── Family 1 (beads-sigma-fah8): USERELATIONSHIP → alternate join paths ────
  // Inactive model relationship → the distinct Sigma relationship name it gets
  // when some measure activates it via USERELATIONSHIP. Inactive relationships
  // NOT activated by any measure are skipped entirely (adding them would collide
  // with the active relationship's name in [SRC/REL/Field] refs).
  const relActivationNames = new Map<any, string>();
  // measure name → the alternate relationship name its aggregate resolves
  // through ('' = the active path). Used to refuse metrics that COMBINE measures
  // resolving through different join paths (they'd conflate paths in one element).
  const measureAltPath: Record<string, string> = {};
  // CROSSFILTER is the sibling modifier of USERELATIONSHIP — it changes a relationship's
  // cross-filter DIRECTION, so its two column args are relationship ENDPOINTS, not values
  // the aggregate reads. Left in place they leaked into the metric formula and the
  // cross-table guard dropped the whole measure (measured: 2 real measures lost).
  // Sigma has no cross-filter-direction concept, so strip it — and WARN, because the
  // filter semantics DO change; silence here is what made the drop hard to explain.
  const processCrossFilters = (measureName: string, expr: string): string => {
    if (!/\bCROSSFILTER\s*\(/i.test(expr)) return expr;
    const cf = extractCrossFilters(expr);
    for (const pair of cf.pairs) {
      warnings.push(`⚠ "${measureName}": CROSSFILTER(${pair.a.table}[${pair.a.column}], ${pair.b.table}[${pair.b.column}], ${pair.direction}) — Sigma has no cross-filter-direction control, so the modifier is dropped and the relationship keeps its model direction. The aggregate itself is unchanged; if the source measure relied on that direction change to widen or narrow its filter context, verify the number.`);
    }
    return cf.dax;
  };

  const processUseRelationships = (measureName: string, expr: string): string => {
    if (!/\bUSERELATIONSHIP\s*\(/i.test(expr)) return expr;
    const ur = extractUseRelationships(expr);
    for (const pair of ur.pairs) {
      const rel = findModelRelationship(model, pair);
      if (!rel) {
        warnings.push(`⚠ "${measureName}": USERELATIONSHIP(${pair.a.table}[${pair.a.column}], ${pair.b.table}[${pair.b.column}]) has no matching model relationship — filter ignored; verify the grouping path manually.`);
        continue;
      }
      if (rel.isActive === false) {
        let altName = relActivationNames.get(rel);
        if (!altName) {
          altName = `${String(rel.toTable).toUpperCase()}_VIA_${String(rel.fromColumn).toUpperCase()}`.replace(/[^A-Z0-9_]+/g, '_');
          relActivationNames.set(rel, altName);
        }
        measureAltPath[measureName] = altName;
        warnings.push(`✅ "${measureName}": CALCULATE over INACTIVE relationship ${rel.fromTable}[${rel.fromColumn}] → ${rel.toTable}[${rel.toColumn}] — activated as alternate join path "${altName}". The aggregate itself is unchanged; to reproduce the USERELATIONSHIP grouping, group by the "(${altName})" columns on the derived "${rel.fromTable} View" element (the active-path columns remain "(${rel.toTable})").`);
      } else {
        warnings.push(`ℹ "${measureName}": USERELATIONSHIP over an already-ACTIVE relationship (${rel.fromTable}[${rel.fromColumn}] → ${rel.toTable}[${rel.toColumn}]) — no-op, stripped.`);
      }
    }
    return ur.dax;
  };

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

  // Does the MODEL span multiple warehouse schemas / catalogs? (beads-sigma-lanq.6)
  // A single `database`/`schema` override is a repoint that is safe on a single-
  // schema model but, on a multi-schema model (one catalog, N schemas), would
  // collapse every table onto one schema and mass-break the binding — the exact
  // reported bug. Detect the spread here so the override below applies only where
  // it isn't destructive.
  const _schemasSeen = new Set<string>();
  const _catalogsSeen = new Set<string>();
  for (const t of model.tables) {
    if (measureOnlyTables.has(t.name) || calcGroupTables.has(t.name)) continue;
    if (t.name.startsWith('LocalDateTable_') || t.name.startsWith('DateTableTemplate_')) continue;
    const _expr = ((t.partitions || [])[0])?.source?.expression;
    const _mp = _expr ? pbiExtractPathFromM(Array.isArray(_expr) ? _expr.join('\n') : String(_expr)) : null;
    if (_mp && _mp.length >= 3) { _catalogsSeen.add(_mp[0]); _schemasSeen.add(_mp[1]); }
  }
  const modelMultiSchema = _schemasSeen.size > 1;
  const modelMultiCatalog = _catalogsSeen.size > 1;

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

      // Compute the SQL statement FIRST so the column loop can verify which
      // aliases the SQL actually emits (Bug E: a multi-column VALUES calc-table
      // like SalaryBands emits only the numeric series; the DAX-ADDCOLUMNS string
      // column "Band" is NOT in the SQL, so a `[Custom SQL/Band]` ref would be an
      // unresolvable dependency that fails the whole DM POST).
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

      const ctColumns: SigmaColumn[] = [];
      const ctOrder: string[] = [];
      const droppedCols: string[] = [];
      for (const c of ctCols) {
        const sourceCol = (c.sourceColumn || c.name || '').replace(/^\[|\]$/g, '');
        const displayName = sigmaDisplayName(sourceCol);
        // Only surface a column the SQL statement actually emits (alias `AS
        // "Display Name"`). Skip any DAX-derived column the synthesizer couldn't
        // translate — otherwise its `[Custom SQL/X]` ref breaks the DM POST.
        const aliasEmitted = built.ok &&
          new RegExp(`AS\\s+"${displayName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}"`, 'i').test(statement);
        if (built.ok && !aliasEmitted) { droppedCols.push(displayName); continue; }
        const colId = sigmaInodeId(sigmaPhysicalName(sourceCol || c.name));
        tableColMap[tableName][c.name] = colId;
        allPbiToSigmaNames[c.name] = displayName;
        // Bug E: reference the SQL alias with the QUALIFIED `[Custom SQL/Display
        // Name]` form (verified to resolve). The element-level `name` is OMITTED
        // below (rule 3) so the element resolves as "Custom SQL".
        const col: SigmaColumn = { id: colId, formula: `[Custom SQL/${displayName}]` };
        if (c.isHidden) (col as any).hidden = true;
        if (c.description) col.description = c.description;
        ctColumns.push(col);
        ctOrder.push(colId);
      }
      if (droppedCols.length) {
        warnings.push(`⚠ Calculated table "${tableName}": dropped column(s) ${droppedCols.join(', ')} — their DAX (ADDCOLUMNS/SELECTCOLUMNS) expression wasn't translated into the synthesized SQL, so they have no warehouse source. Add them to the SQL statement manually or as Sigma calc columns.`);
      }

      // Rule 3 (CLAUDE.md): a Custom SQL element MUST OMIT the element-level
      // `name` — Sigma resolves it as "Custom SQL", which the `[Custom SQL/...]`
      // column formulas (and relationship targetElementId refs) depend on.
      // Relationships/Views reference this element by id + rel-name, never by the
      // element name, so omitting it is safe. (Bug E)
      const ctElement: SigmaElement = {
        id: elementId, kind: 'table',
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
    // Apply the caller's database/schema override. It is a REPOINT — honored on a
    // single-schema / single-catalog model (the j89 behavior) — but NOT applied to
    // a model that spans multiple schemas/catalogs, where a single override would
    // collapse every table onto one schema (beads-sigma-lanq.6: the converter
    // "assumed all schemas in the same catalog"). There each table's own M-resolved
    // segment wins; explicit per-table repoints stay the job of --table-map.
    if (path) {
      if (path.length >= 3) {
        if (dbOverride && !modelMultiCatalog) path[0] = dbOverride;
        if (schOverride && !modelMultiSchema) path[1] = schOverride;
      } else if (schOverride && path.length === 2) {
        path[0] = schOverride; // legacy 2-part handling (unchanged)
      }
    } else {
      path = [dbOverride || physCase('DATABASE'), schOverride || physCase('SCHEMA'), physCase(tableName)];
      warnings.push(`⚠ Table "${tableName}": could not extract source path from M expression — using default.`);
    }
    // Physical binding path uses the warehouse's identifier case (Databricks →
    // lower); the element name + [TABLE/Col] formula refs stay in the logical
    // (UPPER) case and remain internally consistent (beads-sigma-lanq.7).
    const physPath = whLower && path ? path.map((s: string) => String(s).toLowerCase()) : path;

    // Columns
    const columns: SigmaColumn[] = [];
    const order: string[] = [];
    const pbiToSigmaName: Record<string, string> = {};

    for (const c of (t.columns || [])) {
      if (c.type === 'rowNumber' || c.isGenerated) continue;
      if (c.type === 'calculated') continue;
      // Binary columns (embedded images etc.) have no warehouse representation —
      // a [TABLE/Display] ref to one fails the POST with "dependency not found".
      if (c.dataType === 'binary') {
        warnings.push(`⚠ Table "${tableName}": binary column "${c.name}" skipped — no warehouse/Sigma representation (embedded asset).`);
        continue;
      }
      const sourceCol = c.sourceColumn || c.name;
      const displayName = sigmaDisplayName(sourceCol);
      const colId = sigmaInodeId(sigmaPhysicalName(sourceCol, physCasing), physCasing);
      tableColMap[tableName][c.name] = colId;
      pbiToSigmaName[c.name] = displayName;
      allPbiToSigmaNames[c.name] = displayName;

      const col: SigmaColumn = { id: colId, formula: `[${tableName.toUpperCase()}/${displayName}]` };
      if (c.isHidden) (col as any).hidden = true;
      if (c.description) col.description = c.description;
      columns.push(col);
      order.push(colId);
    }

    // Proxy of the source warehouse-table element (path + base passthrough cols
    // built so far) — passed to the rank/window lowering so it can resolve a
    // FROM source and validate partition/order/grain columns against real cols.
    const srcElProxy: SigmaElement = {
      id: elementId, kind: 'table',
      name: (path && path.length ? path[path.length - 1] : tableName.toUpperCase()),
      source: { connectionId: connectionId || '<CONNECTION_ID>', kind: 'warehouse-table', path: physPath },
      columns, order,
    };

    // Calculated columns
    for (const c of (t.columns || [])) {
      if (c.type !== 'calculated') continue;
      // Rank / window calc COLUMN → SQL window helper element (Sigma's Rank /
      // RankDense silently error in DM calc cols). The COUNTROWS(FILTER(..EARLIER
      // ..))+1 row-level dense-rank idiom is the canonical PBI calc-col rank.
      // Parse the ORIGINAL DAX (before pbiDaxToSigma's rewriteEarlierRank would
      // emit a RankDense() that error-types). DEGRADE → fall through to the
      // normal path which then drops to a warning.
      const cExpr = Array.isArray(c.expression) ? c.expression.join('\n') : String(c.expression || '');
      // Family 2 (beads-sigma-fah8): also try the running-total / group-share /
      // peer-count EARLIER idioms (pbiParseEarlierWindow) when the rank idiom
      // doesn't match.
      const cWin = pbiParseEarlierRank(cExpr) || pbiParseEarlierWindow(cExpr);
      if (cWin && lowerPBIWindowCalc(cWin, c.name, srcElProxy, winCtx, warnings)) {
        tableColMap[tableName][c.name] = sigmaShortId(); // placeholder id; ref lives on helper
        pbiToSigmaName[c.name] = c.name;
        continue;
      }
      let sigmaFormula = pbiDaxToSigma(c.expression, warnings, c.name, measureDaxMap);
      // bead jzd8: a base-table calc COLUMN may not carry a window function —
      // Sigma's Rank/RankDense/Lag/Lead silently ERROR there (the column posts as
      // type "error"). If lowering to a helper element degraded above and the
      // generic translator still produced a window-function formula (e.g.
      // rewriteEarlierRank → RankDense), DROP-and-warn instead of emitting an
      // error column. Window calcs must live on a sql/grouped helper element.
      if (sigmaFormula && hasBareWindowFn(sigmaFormula)) {
        warnings.push(`⛔ "${c.name}": window-function calc column (${sigmaFormula.slice(0, 48)}…) cannot live in a base-table calc column (errors in Sigma) — express it as a workbook Rank() in an ordered table or a grouped element. Dropped.`);
        sigmaFormula = null;
      }
      if (sigmaFormula) {
        // Rewrite PBI column names → Sigma display names. Try local table
        // first, fall back to the global map so cross-table refs (e.g. from
        // RELATED('dim'[COL])) get a usable display name that the post-pass
        // cross-element move can map back to a triple-form ref.
        // Scan/rewrite a literal-MASKED copy — a text value like "see [Amount]
        // for detail" must not be REWRITTEN in place just because "Amount"
        // happens to be a tracked column name (dangerous: corrupts the literal
        // content baked into the emitted formula).
        sigmaFormula = replaceOutsideDaxLiterals(sigmaFormula, /\[([^\]\/]+)\]/g, (_m: string, colName: string) => {
          if (pbiToSigmaName[colName]) return `[${pbiToSigmaName[colName]}]`;
          if (allPbiToSigmaNames[colName]) return `[${allPbiToSigmaNames[colName]}]`;
          return `[${colName}]`;
        });
        const colId = sigmaShortId();
        tableColMap[tableName][c.name] = colId;
        pbiToSigmaName[c.name] = c.name;
        // Honor the calc column's DECLARED type: DAX coerces a concat result
        // back to the declared numeric type ([MonthID]&"01" declared int64);
        // Sigma keeps it text, which then breaks any RELATIONSHIP keyed on the
        // column ("(lookup) Argument 3 expected String, found Integer"). Wrap
        // text-producing formulas in Number() when the model says numeric.
        if (['int64', 'double', 'decimal'].includes(String(c.dataType)) && /&|^\s*(Text|Concat|Format)\s*\(/i.test(sigmaFormula)) {
          sigmaFormula = `Number(${sigmaFormula})`;
        }
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
      // RANKX measure → SQL window helper element (Sigma DM metrics silently
      // error on Rank()). Parse the ORIGINAL DAX before pbiDaxToSigma drops it to
      // a warning; resolve the order-measure ref via the model measure-agg map.
      // DEGRADE → fall through to the existing drop-and-warn path.
      const mExprRaw = stripDaxComments(Array.isArray(m.expression) ? m.expression.join('\n') : String(m.expression || ''));
      // Family 1 (beads-sigma-fah8): strip USERELATIONSHIP filter args and
      // activate the inactive relationship as a distinctly-named alternate path.
      const mExpr = processUseRelationships(m.name, processCrossFilters(m.name, mExprRaw));
      const mWin = pbiParseRankx(mExpr, measureAggMap);
      if (mWin && lowerPBIWindowCalc(mWin, m.name, srcElProxy, winCtx, warnings)) {
        continue; // lowered to helper element; no metric on this element
      }
      let sigmaFormula = pbiDaxToSigma(mExpr, warnings, m.name, measureDaxMap);
      // bead jzd8 (measure path): a measure that translated to a window fn (e.g. an
      // EARLIER idiom → RankDense that lowering didn't claim) would post as an
      // error-typed DM metric. Drop-and-warn instead.
      if (sigmaFormula && hasBareWindowFn(sigmaFormula)) {
        warnings.push(`⛔ "${m.name}": window-function measure has no Sigma DM-metric equivalent — use a workbook Rank()/ordered table or a grouped element. Dropped.`);
        sigmaFormula = null;
      }
      if (sigmaFormula) {
        // Same literal-masked rewrite as the calc-column path above — a text
        // fallback/label value must not have its "[Name]"-shaped content
        // rewritten just because it matches a tracked column name.
        sigmaFormula = replaceOutsideDaxLiterals(sigmaFormula, /\[([^\]\/]+)\]/g, (_m2: string, colName: string) => {
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

    // A metric whose formula references a column on ANOTHER table (e.g. a Sales
    // measure summing Store[SellingAreaSize]) compiles to a silent error-typed
    // metric in Sigma — the element has no such column. Drop-and-warn with grain
    // guidance instead of posting a broken metric. Multi-pass so dependents of a
    // dropped cross-table metric collapse too. (beads-sigma-p146 follow-on)
    {
      const colDisplays = new Set<string>([
        ...Object.values(pbiToSigmaName),
        ...Object.keys(pbiToSigmaName),
      ]);
      // DAX column references are case-INSENSITIVE: [AGENt_KEY] IS [AGENT_KEY], and real
      // models contain exactly that typo. Comparing against exact-match Sets made a
      // mis-cased ref look like a FOREIGN column, so the cross-table guard dropped an
      // otherwise-valid measure (measured: AGENt_KEY vs AGENT_KEY in 2 measures,
      // SUBMISSION_key vs SUBMISSION_KEY in a third). Resolve case-insensitively AND
      // rewrite the ref to the canonical spelling — leaving the mis-cased name in the
      // formula would just move the failure into Sigma, which cannot resolve it either.
      const canonicalCol = new Map<string, string>();
      for (const d of colDisplays) {
        const k = d.toLowerCase();
        if (!canonicalCol.has(k)) canonicalCol.set(k, d);
      }
      // Metric name -> the exact reason text this metric was itself dropped for,
      // across ALL passes of the loop below (persists outside the `pass` loop —
      // a metric dropped in pass 0 must still be found when a LATER metric's
      // ref to it is judged in pass 1+). Populated whichever branch below fires,
      // so a chain of dependent drops composes: if B depends on A and C depends
      // on B, C's message quotes B's message, which already quotes A's.
      const siblingDropReason = new Map<string, string>();
      for (let pass = 0; pass < 5; pass++) {
        const metricNames = new Set(metrics.map((mm: any) => mm.name));
        const canonicalMetric = new Map<string, string>();
        for (const n of metricNames) {
          const k = String(n).toLowerCase();
          if (!canonicalMetric.has(k)) canonicalMetric.set(k, String(n));
        }
        const before = metrics.length;
        for (let i = metrics.length - 1; i >= 0; i--) {
          // normalize any mis-cased ref to its canonical column/metric name first, so the
          // guard below judges the ref on identity rather than on spelling
          // Scan/rewrite a literal-MASKED copy — a label/help string mentioning
          // another measure or column BY NAME (e.g. "See [Broken Measure] note")
          // must not be treated as a real reference: unmasked, it both gets
          // needlessly rewritten in place (corrupting the literal) AND can feed
          // a phantom "bad" ref into the cross-table guard below, which then
          // drops a fully independent, otherwise-valid metric as "cross-table"
          // (dangerous: looks like a deliberate migration-quality warning, but
          // the named blocker was never actually referenced).
          metrics[i].formula = replaceOutsideDaxLiterals(String(metrics[i].formula), /\[([^\]\/]+)\]/g, (whole: string, ref: string) => {
            if (colDisplays.has(ref) || metricNames.has(ref)) return whole;   // already exact
            const c = canonicalCol.get(ref.toLowerCase()) || canonicalMetric.get(ref.toLowerCase());
            return c ? `[${c}]` : whole;
          });
          const refs = (maskDaxStringLiterals(String(metrics[i].formula)).match(/\[([^\]\/]+)\]/g) || []).map((r: string) => r.slice(1, -1));
          const bad = refs.find((r) => !colDisplays.has(r) && !metricNames.has(r));
          if (bad) {
            // ALWAYS run triageCrossTable — never bypass it on a name match alone.
            // `bad` sharing a literal NAME with another element's metric (or with
            // an already-dropped same-table sibling) does not mean it ISN'T also
            // a real, reachable column: `columnOwners` is built from every table's
            // columns, so a genuine hop-1 column named e.g. "AGENT_NAME" resolves
            // correctly here regardless of whether some unrelated table ALSO
            // happens to declare a measure with that same name. triageCrossTable's
            // own verdict — safe/fanout-risk/ambiguous/never-hostable/malformed —
            // is always better information than a name-based guess, so it must run
            // first and win whenever it has anything to say at all.
            //
            // TMSL/BIM serializes a multi-line DAX expression as a string[] (one
            // entry per line) — `String(anArray)` joins with a bare comma, not a
            // newline, silently mangling multi-line DAX before isNeverHostable
            // ever sees it. Coerce INSIDE the array branch so an array joins with
            // '\n' (matching how this codebase joins the same shape elsewhere,
            // e.g. powerbi.ts's own measureDaxMap construction), and only
            // String()-coerce the non-array case.
            const _rawDaxExpr = ((t.measures || []).find((mm: any) => mm.name === metrics[i].name) || {}).expression;
            const _rawDax = Array.isArray(_rawDaxExpr) ? _rawDaxExpr.join('\n') : String(_rawDaxExpr || '');
            const _triage = triageCrossTable({
              metricName: metrics[i].name,
              sigmaFormula: String(metrics[i].formula),
              rawDax: _rawDax,
              homeTable: tableName,
              refs: [...new Set(refs)],
              columnOwners: triageColumnOwners,
              relationships: triageRels,
              metricRefs: [...metricNames],
              // Explicit, not just inherited from triageCrossTable's own default —
              // measured on R1-R4: 9 of 32 `no-covering-View` drops are a filtered
              // dimension reachable at 3 hops, not 2 (see powerbi-crosstable-triage.ts).
              maxDepth: 3,
            });

            // `bad` is not always a reachability question — it can be a NAME
            // rather than a column at all. `columnOwners` has no entry for a
            // metric name (it is built only from `model.tables[].columns`), so
            // either of the two shapes below resolves to hop `Infinity` on every
            // candidate — but ONLY when triageCrossTable found NOTHING BETTER to
            // say (`isNoCoveringView`) do we replace its generic message with the
            // real blocker; a `safe`/`fanout-risk`/`ambiguous`/`never-hostable`
            // verdict always wins outright (measured: 15 of 32 R1-R4
            // `no-covering-View` drops were this — see MetricBlocker's doc
            // comment for the full reasoning and why this must run AFTER, not
            // instead of, triageCrossTable).
            let _blocker: MetricBlocker | null = null;
            if (isNoCoveringView(_triage)) {
              if (allMetricOwner[bad] && allMetricOwner[bad] !== tableName) {
                _blocker = { kind: 'cross-element-metric', metric: bad, ownerTable: allMetricOwner[bad] };
              } else if (siblingDropReason.has(bad)) {
                _blocker = { kind: 'dropped-sibling', metric: bad, siblingReason: siblingDropReason.get(bad)! };
              }
            }
            const _reasonText = _blocker ? describeMetricBlocker(_blocker) : describeTriage(_triage);
            // The generic "recreate in a workbook element..." clause below implies
            // a View-based fix exists — true for every triageCrossTable verdict,
            // even fan-out-risk (rebuild at the visual's grain IS that fix), but
            // FALSE for a MetricBlocker: a cross-element-metric block says "no hop
            // limit fixes this," and stapling the generic clause in front of that
            // would have the same warning contradict itself. `describeMetricBlocker`
            // text is already self-contained (it says exactly what to do), so omit
            // the generic clause whenever a blocker fired.
            const _warning = _blocker
              ? `⚠ "${metrics[i].name}": references "[${bad}]" which is not a column or metric on this element (cross-table measure) — dropped. ${_reasonText}`
              : `⚠ "${metrics[i].name}": references "[${bad}]" which is not a column or metric on this element (cross-table measure) — dropped; recreate in a workbook element at the visual's grain (the joined "View" element has the dim columns). ${_reasonText}`;
            warnings.push(_warning);
            siblingDropReason.set(metrics[i].name, _reasonText);
            metrics.splice(i, 1);
          }
        }
        if (metrics.length === before) break;
      }
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
      source: { connectionId: connectionId || '<CONNECTION_ID>', kind: 'warehouse-table', path: physPath },
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
    // dax-fidelity #4: preserve each measure's HOME fact table. The default
    // (biggest-element) target mis-binds a measure whose aggregate lives on a
    // DIFFERENT fact — e.g. a GL-premium measure collapsed onto a CP fact, whose
    // column then doesn't resolve. Re-home the measure to the element that actually
    // OWNS its aggregated column (via tableColMap); fall back to the biggest fact
    // only when no referenced table owns the column (composite / cross-table).
    const homeElFor = (rawDax: string): any => {
      const refs = [...String(rawDax).matchAll(/(?:'([^']+)'|\b([A-Za-z_]\w*))\s*\[([^\]]+)\]/g)];
      for (const r of refs) {
        const tbl = (r[1] || r[2] || '').trim();
        const colName = r[3];
        const elId = tableIdMap[tbl];
        if (!elId || !(tableColMap[tbl] && (colName in tableColMap[tbl]))) continue;
        // At this point `elements` holds only base source elements (warehouse-table
        // + calc-table sql); the derived join "<T> View" elements are built later.
        const el = elements.find((e: any) => e.id === elId);
        if (el && (el.columns || []).length) return el;
      }
      return factEl;
    };
    if (factEl) {
      for (const tName of measureOnlyTables) {
        const t = model.tables.find((tb: any) => tb.name === tName);
        if (!t) continue;
        for (const m of (t.measures || [])) {
          const moExpr = processUseRelationships(m.name, processCrossFilters(m.name,
            stripDaxComments(Array.isArray(m.expression) ? m.expression.join('\n') : String(m.expression || ''))));
          const homeEl = homeElFor(moExpr);
          if (m.name) measureToElementId[m.name] = homeEl.id; // m1a cross-table detection
          let sigmaFormula = pbiDaxToSigma(moExpr, warnings, m.name, measureDaxMap);
          if (sigmaFormula && hasBareWindowFn(sigmaFormula)) {  // bead jzd8 (measure path)
            warnings.push(`⛔ "${m.name}": window-function measure has no Sigma DM-metric equivalent — use a workbook Rank()/ordered table or a grouped element. Dropped.`);
            sigmaFormula = null;
          }
          if (sigmaFormula) {
            sigmaFormula = sigmaFormula.replace(/\[([^\]\/]+)\]/g, (_m2: string, colName: string) => {
              return allPbiToSigmaNames[colName] ? `[${allPbiToSigmaNames[colName]}]` : `[${colName}]`;
            });
            if (!(homeEl as any).metrics) (homeEl as any).metrics = [];
            const _moFmt = inferSigmaFormat(sigmaFormula, m.name, (m as any).formatString);
            const metric: any = { id: sigmaShortId(), formula: sigmaFormula, name: m.name };
            if (_moFmt) metric.format = _moFmt;
            if (m.description) metric.description = m.description;
            (homeEl as any).metrics.push(metric);
            if (homeEl !== factEl) {
              warnings.push(`ℹ "${m.name}": bound to its home fact element "${homeEl.source?.path?.[homeEl.source.path.length - 1] || homeEl.id}" (the table that owns its aggregated column), not the largest element — preserves the measure's source fact (dax-fidelity #4).`);
            }
          }
        }
        warnings.push(`ℹ Measures table "${tName}" → measures moved to their home fact element(s).`);
      }
    }
  }

  // ── Relationships ──────────────────────────────────────────────────────────
  // Active relationships keep the target-table name (rule 2: [SRC/REL/Field]).
  // INACTIVE relationships (isActive:false) are added ONLY when a measure
  // activates them via USERELATIONSHIP, under a distinct alternate-path name
  // (e.g. DIMDATE_VIA_SHIP_DATE) — adding them under the target-table name would
  // collide with the active relationship's refs. (beads-sigma-fah8 family 1)
  for (const rel of (model.relationships || [])) {
    const fromTable = rel.fromTable;
    const toTable = rel.toTable;
    const fromCol = rel.fromColumn;
    const toCol = rel.toColumn;

    const isActive = rel.isActive !== false;
    const altName = relActivationNames.get(rel);
    if (!isActive && !altName) {
      warnings.push(`ℹ Inactive relationship ${fromTable}[${fromCol}] → ${toTable}[${toCol}] skipped — no measure activates it via USERELATIONSHIP.`);
      continue;
    }

    const fromElId = tableIdMap[fromTable];
    const toElId = tableIdMap[toTable];
    if (!fromElId || !toElId) continue;

    const fromColId = tableColMap[fromTable]?.[fromCol];
    const toColId = tableColMap[toTable]?.[toCol];
    if (!fromColId || !toColId) {
      warnings.push(`⚠ Relationship ${fromTable}[${fromCol}] → ${toTable}[${toCol}]: columns not found`);
      continue;
    }

    // Mixed-type relationship keys: PBI's engine coerces (the MS Retail
    // Analysis Sample joins a STRING calc column to an int64 key); Sigma's
    // lookup errors at QUERY time ("Argument 3 expected String, found
    // Integer"). When one side is a CALCULATED column whose formula we emit,
    // coerce that formula to the other side's type family.
    {
      const typeOf = (tbl: string, col: string): string => {
        const t = (model.tables || []).find((x: any) => x.name === tbl);
        const c = t && (t.columns || []).find((x: any) => x.name === col);
        return c ? String(c.dataType || '') : '';
      };
      const isNum = (d: string) => ['int64', 'double', 'decimal'].includes(d);
      const fromType = typeOf(fromTable, fromCol), toType = typeOf(toTable, toCol);
      if (fromType && toType && isNum(fromType) !== isNum(toType)) {
        const fixSide = (tbl: string, col: string, colId: string, wrap: 'Number' | 'Text'): boolean => {
          const t = (model.tables || []).find((x: any) => x.name === tbl);
          const c = t && (t.columns || []).find((x: any) => x.name === col);
          if (!c || c.type !== 'calculated') return false;
          const el = elements.find(e => e.id === tableIdMap[tbl]);
          const ec: any = el && (el.columns || []).find((x: any) => x.id === colId);
          if (!ec || typeof ec.formula !== 'string' || ec.formula.startsWith(`${wrap}(`)) return false;
          ec.formula = `${wrap}(${ec.formula})`;
          warnings.push(`ℹ Relationship ${fromTable}[${fromCol}] → ${toTable}[${toCol}]: mixed-type keys (${fromType} vs ${toType}) — coerced calc column "${col}" with ${wrap}() to match.`);
          return true;
        };
        const fixed = isNum(toType)
          ? (fixSide(fromTable, fromCol, fromColId, 'Number') || fixSide(toTable, toCol, toColId, 'Text'))
          : (fixSide(toTable, toCol, toColId, 'Number') || fixSide(fromTable, fromCol, fromColId, 'Text'));
        if (!fixed) {
          warnings.push(`⚠ Relationship ${fromTable}[${fromCol}] → ${toTable}[${toCol}]: mixed-type keys (${fromType} vs ${toType}) on physical columns — the Sigma join will error at query time; align the warehouse column types.`);
        }
      }
    }

    const fromElement = elements.find(e => e.id === fromElId);
    if (fromElement) {
      if (!fromElement.relationships) fromElement.relationships = [];
      fromElement.relationships.push({
        id: sigmaShortId(),
        targetElementId: toElId,
        keys: [{ sourceColumnId: fromColId, targetColumnId: toColId }],
        name: isActive ? toTable : altName!
      });
    }
  }

  // ── Family 1 follow-on: refuse metrics that COMBINE measures resolving
  // through DIFFERENT join paths (e.g. Net Change = [Hires via HIRE_DATE path]
  // - [Terms via TERMINATION_DATE path]). On one element both operands compile
  // to the SAME aggregate — the difference only materializes through WHICH
  // path's columns you group by, so a single-element scalar would silently
  // conflate the paths (e.g. always 0). Flag with the recipe instead.
  for (const el of elements) {
    const mets: any[] = (el as any).metrics || [];
    if (!mets.length) continue;
    const kept: any[] = [];
    for (const metric of mets) {
      const refs = [...String(metric.formula).matchAll(/\[([^\]\/]+)\]/g)]
        .map(x => x[1])
        .filter(n => n in measureToElementId); // only measure refs
      const paths = new Set(refs.map(r => measureAltPath[r] || ''));
      if (metric.name in measureAltPath) paths.add(measureAltPath[metric.name]);
      if (paths.size > 1) {
        const detail = refs.map(r => `[${r}] via ${measureAltPath[r] ? `"${measureAltPath[r]}"` : 'the active path'}`).join(', ');
        warnings.push(`⚠ "${metric.name}": combines measures that resolve through DIFFERENT relationship paths (${detail}). A single-element scalar conflates the paths — build each operand as its own grouped element on its path's columns, join on the shared period/dimension, then combine. Dropped.`);
        continue;
      }
      kept.push(metric);
    }
    if (kept.length) (el as any).metrics = kept;
    else delete (el as any).metrics;
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
      // Scan a literal-MASKED copy — a label/help string containing
      // bracket-shaped text must not be mistaken for a real cross-element
      // ref; that false positive needlessly pulls a fully local calc column
      // off its source element (misplaced at best, dropped entirely if no
      // derived "<Table> View" ends up covering it).
      const refs = maskDaxStringLiterals(c.formula).match(/\[([^\]\/]+)\]/g) || [];
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
  //
  // dax-fidelity #12 (model-structure note): a cross-element calculated column —
  // one whose formula reaches a column on a DIFFERENT table via [SRC/REL/Field] —
  // only resolves when it lives on a derived element whose `source.kind === 'table'`
  // (a real join element, the "<T> View" produced here). The model root's bare
  // `relationships: [...]` array declares the joins but is NOT itself an
  // element context: a [SRC/REL/Field] ref placed on a plain warehouse-table
  // element (no join source) compiles to column type `error`. So any measure/
  // column that spans tables MUST be emitted on one of these derived join
  // elements, never on the bare fact element.
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
        // Scan/rewrite a literal-MASKED copy — a text value whose content
        // happens to name a related column must not be rewritten in place
        // (corrupts the literal) just because its bracket-shaped text matches
        // a real relatedNameMap entry.
        c.formula = replaceOutsideDaxLiterals(c.formula, /\[([^\]\/]+)\]/g, (match: string, refName: string) => {
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

  // ── Finalize rank/window SQL helper elements ──────────────────────────────
  // Each accumulated helper now gets its real WITH base AS (...) SELECT ... OVER
  // statement built, then is appended to the model as a kind:'sql' element.
  // Names must be model-unique (two "Dense Rank DEPARTMENT" helpers from different
  // fact tables would otherwise collide); dedupe against existing element names.
  if (winCtx.extraElements.length) {
    const usedNames = new Set<string>();
    for (const e of elements) if (e.name) usedNames.add(e.name.toLowerCase());
    for (const helper of winCtx.helpers.values()) {
      finalizePBIWindowHelper(helper);
      const el = helper.element;
      let base = el.name || 'Window';
      let cand = base, n = 2;
      while (usedNames.has(cand.toLowerCase())) cand = `${base} ${n++}`;
      usedNames.add(cand.toLowerCase());
      el.name = cand;
    }
    elements.push(...winCtx.extraElements);
  }

  // ── Row-level (RLS) + object-level (OLS) security from model.roles[] ───────
  // PBI roles carry a DAX filterExpression per table (RLS) and columnPermissions
  // metadataPermission:"none" (OLS). Emit a fail-closed RLS boolean calc + element
  // filter, and CLS for hidden columns. Role MEMBERSHIP isn't in the file (bound in
  // the Service) → flagged for provisioning. Mirrors the lookml/Tableau RLS pattern.
  const rlsTablesSeen = new Set<string>();
  for (const role of (model.roles || [])) {
    for (const tp of (role.tablePermissions || [])) {
      const el: any = tableIdMap[tp.name] ? elements.find(e => e.id === tableIdMap[tp.name]) : null;
      const feRaw = tp.filterExpression;
      if (feRaw && el) {
        let formula = pbiDaxToSigma(feRaw, warnings, `RLS ${role.name}/${tp.name}`, measureDaxMap);
        if (formula && !formula.startsWith('/*')) {
          // Dynamic RLS: USERNAME()/USERPRINCIPALNAME() → CurrentUserEmail().
          formula = formula.replace(/\b(?:USERNAME|USERPRINCIPALNAME)\s*\(\s*\)/gi, 'CurrentUserEmail()');
          // Normalize PBI column refs [RAW_NAME] → [Display Name] (the direct
          // pbiDaxToSigma call here skips the calc-path's display-name remap).
          formula = formula.replace(/\[([^\]\/]+)\]/g, (m, n) =>
            allPbiToSigmaNames[n] ? `[${allPbiToSigmaNames[n]}]` : m);
          security.push(makeRlsSecurity({ source: `Power BI role "${role.name}" (table "${tp.name}")`, element: el, name: `RLS: ${role.name}`, formula }));
          warnings.push(`🔐 PBI role "${role.name}" RLS on table "${tp.name}" → row-level security DETECTED (reported in result.security, not injected): ${formula.slice(0, 70)}. Role membership is bound in the Power BI Service (not the model file); the migration skill provisions the attribute/team + assigns members, then applies the RLS calc + filter.`);
          if (rlsTablesSeen.has(tp.name)) warnings.push(`⚠ Multiple PBI roles apply RLS to "${tp.name}". Power BI unions role filters (OR); stacked Sigma element filters intersect (AND). Review — you likely want the role conditions OR-combined into one RLS column.`);
          rlsTablesSeen.add(tp.name);
        } else {
          warnings.push(`⚠ PBI role "${role.name}" RLS filter on "${tp.name}" ("${String(feRaw).slice(0, 60)}") could not be translated — re-apply manually as a boolean calc column + element filter.`);
        }
      }
      const hidden = (tp.columnPermissions || []).filter((cp: any) => (cp.metadataPermission || cp.memberPermission) === 'none');
      if (hidden.length && el) {
        const ids = hidden.map((cp: any) => tableColMap[tp.name]?.[cp.name]).filter(Boolean) as string[];
        if (ids.length) {
          security.push(makeClsSecurity({ source: `Power BI OLS role "${role.name}" (table "${tp.name}")`, element: el, columnIds: ids, columnNames: hidden.map((c: any) => c.name), note: 'PBI OLS hides from the role\'s members; Sigma CLS is per-restriction (no-one-can-view, or re-scope to a team/attribute allowlist). The skill applies it — not injected.' }));
          warnings.push(`🔐 PBI role "${role.name}" object-level security hides [${hidden.map((c: any) => c.name).join(', ')}] on "${tp.name}" → CLS DETECTED (reported in result.security, not injected).`);
        }
      }
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
    ...(security.length ? { security } : {}),
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
