/**
 * SQL → Sigma formula conversion utilities.
 * Used by LookML, Snowflake, dbt, and Tableau converters.
 */

import { sigmaDisplayName } from './sigma-ids.js';

/**
 * Decode numeric XML character references (&#10;, &#9;, &#xNN;) plus the five
 * predefined named entities. fast-xml-parser (v4) decodes the named entities in
 * attribute values but leaves NUMERIC references literal, so Tableau formula
 * attributes arrive carrying raw `&#10;` newline tokens that break a calc column
 * (the literal token is parsed as broken syntax). Idempotent on already-decoded
 * text. Trusted first-party input — no expansion-bomb concern.
 */
export function decodeXmlEntities(s: string): string {
  if (!s || s.indexOf('&') === -1) return s;
  return s
    .replace(/&#x([0-9a-fA-F]+);/g, (_m, h) => String.fromCodePoint(parseInt(h, 16)))
    .replace(/&#(\d+);/g, (_m, d) => String.fromCodePoint(parseInt(d, 10)))
    .replace(/&quot;/g, '"').replace(/&apos;/g, "'")
    .replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&');
}

/**
 * Strip Tableau `//` line comments (to end-of-line) WITHOUT touching `//` that
 * appears inside a string literal — Tableau formulas embed URLs ("https://…").
 * String-aware single pass tracking single/double quote state. Tableau has no
 * block comments in calc fields, so only `//` is handled.
 */
export function stripLineComments(s: string): string {
  if (!s || s.indexOf('//') === -1) return s;
  let out = '', inS = false, inD = false;
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (inS) { out += c; if (c === "'") inS = false; continue; }
    if (inD) { out += c; if (c === '"') inD = false; continue; }
    if (c === "'") { inS = true; out += c; continue; }
    if (c === '"') { inD = true; out += c; continue; }
    if (c === '/' && s[i + 1] === '/') { while (i < s.length && s[i] !== '\n') i++; if (i < s.length) out += '\n'; continue; }
    out += c;
  }
  return out;
}

/**
 * Convert Tableau `[Field] IN (a, b, c)` / `NOT IN (...)` to a Sigma boolean
 * chain (Sigma has no IN operator — it silently errors the column / blanks the
 * chart). `IN` → `([F] = a or [F] = b …)`; `NOT IN` → `([F] <> a and [F] <> b …)`.
 * The left operand is the bracket-ref (or simple token) immediately preceding
 * the keyword; values are split on top-level commas so quoted strings with
 * commas survive. Leaves the expression untouched when the operand can't be
 * isolated (defensive — never emit a half-rewrite).
 */
export function tableauInToSigma(formula: string): string {
  const re = /(\[[^\]]+\]|[A-Za-z_][\w]*)\s+(not\s+)?in\s*\(/gi;
  let f = formula, guard = 0;
  for (let m = re.exec(f); m && guard < 200; m = re.exec(f), guard++) {
    const operand = m[1];
    const isNot = !!m[2];
    const open = m.index + m[0].length - 1; // index of '('
    // find matching close paren
    let depth = 0, close = -1;
    for (let i = open; i < f.length; i++) {
      if (f[i] === '(') depth++;
      else if (f[i] === ')') { depth--; if (depth === 0) { close = i; break; } }
    }
    if (close === -1) break;
    const inner = f.slice(open + 1, close);
    // split on top-level commas (respect quotes + nested parens)
    const parts: string[] = []; let buf = '', d = 0, sq = false, dq = false;
    for (const ch of inner) {
      if (sq) { buf += ch; if (ch === "'") sq = false; continue; }
      if (dq) { buf += ch; if (ch === '"') dq = false; continue; }
      if (ch === "'") { sq = true; buf += ch; continue; }
      if (ch === '"') { dq = true; buf += ch; continue; }
      if (ch === '(') { d++; buf += ch; continue; }
      if (ch === ')') { d--; buf += ch; continue; }
      if (ch === ',' && d === 0) { parts.push(buf.trim()); buf = ''; continue; }
      buf += ch;
    }
    if (buf.trim()) parts.push(buf.trim());
    if (!parts.length) continue;
    const op = isNot ? '<>' : '=';
    const join = isNot ? ' and ' : ' or ';
    const chain = '(' + parts.map(p => `${operand} ${op} ${p}`).join(join) + ')';
    f = f.slice(0, m.index) + chain + f.slice(close + 1);
    re.lastIndex = m.index + chain.length;
  }
  return f;
}

/**
 * Convert Tableau text concatenation (`a + b`) to Sigma's `&` operator. Tableau
 * overloads `+` for both numeric addition and string concatenation; Sigma uses
 * `&` (or Concat) for text and `+` for numbers ONLY — a text `+` errors the
 * column. We rewrite a `+` to `&` when an adjacent operand is text-producing: a
 * string literal, a text-returning function (Coalesce/Text/Left/…), or — when an
 * `isTextRef` resolver is supplied by the converter (which knows column types) —
 * a reference to a text column. Numeric `+` is left untouched. Iterates so a
 * whole chain (`"a" + [x] + "b"`) converts in one call.
 */
const _TEXT_FN_RE = /(?:Coalesce|Concat|Text|Left|Right|Mid|Substring|Substr|Upper|Lower|Trim|Replace|MonthName|WeekdayName|DateName|Proper)$/i;

/**
 * Strip parentheses that wrap an ENTIRE expression: `((x))` → `x`. Repeats while a
 * wrapper remains. `(a) + (b)` is left untouched — its first group closes before the
 * end, so the outer parens are two groups, not one wrapper. Quoted spans are skipped
 * so a `)` inside a string literal is treated as data, not structure. A `[bracketed
 * identifier]` span is likewise treated as atomic: a `'`/`"` inside brackets (e.g.
 * `[Manager's Approval]`) is part of the identifier, not a string-literal delimiter —
 * otherwise the scanner would enter a permanent in-quote state, swallow the real
 * closing `)`, and silently leave the outer parens in place.
 *
 * Domo wraps every Beast Mode in outer parens, which made lookSqlToSigmaRules'
 * anchored patterns (`/^CASE\b/i`, `/^ROUND\s*\(/i`, …) unreachable — measured: 0 of
 * 74 live Beast Modes matched any rule before this.
 */
export function stripOuterParens(s: string): string {
  s = s.trim();
  while (s.length > 1 && s.startsWith('(') && s.endsWith(')')) {
    let depth = 0, quote = '', inBracket = false, wraps = true;
    for (let i = 0; i < s.length; i++) {
      const c = s[i];
      if (inBracket) { if (c === ']') inBracket = false; continue; }
      if (quote) { if (c === quote) quote = ''; continue; }
      if (c === '[') { inBracket = true; continue; }
      if (c === "'" || c === '"') { quote = c; continue; }
      if (c === '(') depth++;
      else if (c === ')') {
        depth--;
        if (depth === 0 && i < s.length - 1) { wraps = false; break; }
      }
    }
    if (!wraps || depth !== 0) break;
    s = s.slice(1, -1).trim();
  }
  return s;
}

function _isTextOperand(op: string, isTextRef?: (name: string) => boolean): boolean {
  let s = stripOuterParens(op.trim());
  if (!s) return false;
  if (/^"(?:[^"\\]|\\.)*"$/.test(s) || /^'(?:[^'\\]|\\.)*'$/.test(s)) return true;   // string literal
  const ref = s.match(/^\[([^\]\/]+)\]$/);
  if (ref) return isTextRef ? isTextRef(ref[1]) : false;                              // column ref (type-gated)
  const fn = s.match(/^([A-Za-z_]+)\s*\(.*\)$/s);                                     // Func(...)
  if (fn && _TEXT_FN_RE.test(fn[1])) return true;
  return false;
}
export function tableauTextConcatToSigma(formula: string, isTextRef?: (name: string) => boolean): string {
  if (!formula || formula.indexOf('+') === -1) return formula;
  // Extract the operand immediately to one side of `+` at position i: a balanced
  // (...)/Func(...) group, a [ref], a "string", or a bare token. dir -1 = left.
  const grab = (s: string, i: number, dir: number): string => {
    let j = i;
    while (j >= 0 && j < s.length && /\s/.test(s[j])) j += dir;
    if (j < 0 || j >= s.length) return '';
    // balanced paren/bracket/string ending (left) or starting (right)
    const close = dir < 0 ? s[j] : '';
    if (dir < 0 && (close === ')' || close === ']')) {
      const open = close === ')' ? '(' : '[', cl = close;
      let depth = 0, k = j;
      for (; k >= 0; k--) { if (s[k] === cl) depth++; else if (s[k] === open) { depth--; if (depth === 0) break; } }
      // include a leading function name
      let f = k; while (f - 1 >= 0 && /[A-Za-z0-9_]/.test(s[f - 1])) f--;
      return s.slice(f, j + 1);
    }
    if (dir > 0 && (s[j] === '(' || s[j] === '[')) {
      const open = s[j], cl = open === '(' ? ')' : ']';
      let depth = 0, k = j;
      for (; k < s.length; k++) { if (s[k] === open) depth++; else if (s[k] === cl) { depth--; if (depth === 0) break; } }
      return s.slice(j, k + 1);
    }
    if (s[j] === '"' || s[j] === "'") {
      const q = s[j]; let k = j + dir;
      while (k >= 0 && k < s.length && s[k] !== q) k += dir;
      return dir < 0 ? s.slice(k, j + 1) : s.slice(j, k + 1);
    }
    // bare token
    let k = j;
    while (k >= 0 && k < s.length && /[A-Za-z0-9_.]/.test(s[k])) k += dir;
    return dir < 0 ? s.slice(k + 1, j + 1) : s.slice(j, k);
  };
  let f = formula, changed = true, guard = 0;
  while (changed && guard++ < 500) {
    changed = false;
    for (let i = 0; i < f.length; i++) {
      if (f[i] !== '+') continue;
      const left = grab(f, i - 1, -1), right = grab(f, i + 1, +1);
      if (_isTextOperand(left, isTextRef) || _isTextOperand(right, isTextRef)) {
        f = f.slice(0, i) + '&' + f.slice(i + 1);
        changed = true;
        break;
      }
    }
  }
  return f;
}

/**
 * Parse a Tableau parameter-driven measure/dimension picker —
 *   `case [Parameters].[Param N] when 'Signs' then [A] when 'TAM' then sum([B]) [else [C]] end`
 * — into the Sigma-native equivalent: a control-driven Switch. Returns the param
 * name, the case map, and a ready Switch formula `Switch([<controlId>], "Signs",
 * <transA>, "TAM", <transB>, [<transElse>])`. Each result expression is run through
 * tableauFormulaToSigma; metric references ([Signs - Actuals]) are LEFT intact for
 * the build layer to inline against the posted model. Returns null if the formula
 * isn't a parameter case-switch. This is the n4pi.8 measure-picker re-architecture.
 */
export function tableauParamSwitchToSigma(
  formula: string,
  controlId: string,
  warnings?: string[],
): { paramName: string; controlId: string; cases: { when: string; then: string }[]; elseExpr: string | null; switchFormula: string } | null {
  const f = decodeXmlEntities(stripLineComments(formula)).trim();
  const head = f.match(/^case\s+\[Parameters?\]\s*\.\s*\[([^\]]+)\]\s+([\s\S]*?)\s*end\s*$/i);
  if (!head) return null;
  const paramName = head[1];
  // Mask the body's string literals ONCE before any when/then/else split runs
  // — an unmasked split's `then` capture is bounded by a lookahead for the
  // next `when`/`else` keyword, so a then-value literal that itself contains
  // the bare word "when" or "else" (e.g. `then 'Value when true'`) satisfies
  // that lookahead early and truncates the value mid-literal (live-reproduced:
  // see tableau.param-switch-literal-masking.test.ts). This mirrors the
  // mask-before-split contract tableauControlToSigma already uses
  // for the identical bug class (see _maskTableauLiterals above), reusing
  // that BOTH-quote-style variant rather than SQL's single-quote-only
  // `_maskLiterals` — this is Tableau calc syntax, where `"..."` is a string
  // literal like `'...'`, not (as in SQL) a quoted identifier.
  const { masked: body, lits } = _maskTableauLiterals(head[2]);
  const cases: { when: string; then: string }[] = [];
  // `when <masked-literal-sentinel> then <result up to next when/else/end-of-body>`
  const pairRe = new RegExp(
    `\\bwhen\\s+${_TABLEAU_SENTINEL_SRC}\\s+then\\s+([\\s\\S]*?)(?=\\s*\\bwhen\\b|\\s*\\belse\\b|$)`,
    'gi',
  );
  let m: RegExpExecArray | null;
  while ((m = pairRe.exec(body))) {
    const whenVal = _tabLitInner(lits, m[1]);                 // strip quotes + unescape
    const thenRaw = _restoreRawTableauLiterals(m[2], lits).trim();
    const thenSig = tableauFormulaToSigma(thenRaw, warnings);
    cases.push({ when: whenVal, then: thenSig });
  }
  if (!cases.length) return null;
  const elseM = body.match(/\belse\s+([\s\S]*?)$/i);
  const elseExpr = elseM
    ? tableauFormulaToSigma(_restoreRawTableauLiterals(elseM[1], lits).trim(), warnings)
    : null;
  const parts = cases.map(c => `"${c.when}", ${c.then}`).join(', ');
  const switchFormula = `Switch([${controlId}], ${parts}${elseExpr ? `, ${elseExpr}` : ''})`;
  return { paramName, controlId, cases, elseExpr, switchFormula };
}

/** Convert bare ALL_CAPS SQL identifier to Sigma display-name column ref [Title Case] */
export function lookColRef(identifier: string): string {
  return `[${sigmaDisplayName(identifier)}]`;
}

/** Snowflake-specific SQL constructs that have no Sigma equivalent */
const UNSUPPORTED_SIGMA_SQL: { pattern: RegExp; name: string }[] = [
  { pattern: /\bFLATTEN\s*\(/i,         name: 'FLATTEN' },
  { pattern: /\bLATERAL\b/i,            name: 'LATERAL' },
  { pattern: /\bQUALIFY\b/i,            name: 'QUALIFY' },
  { pattern: /\bPIVOT\s*\(/i,           name: 'PIVOT' },
  { pattern: /\bUNPIVOT\s*\(/i,         name: 'UNPIVOT' },
  { pattern: /\bGENERATOR\s*\(/i,       name: 'GENERATOR' },
  { pattern: /\bTABLESAMPLE\b/i,        name: 'TABLESAMPLE' },
  { pattern: /\bOBJECT_CONSTRUCT\s*\(/i, name: 'OBJECT_CONSTRUCT' },
  { pattern: /\bARRAY_CONSTRUCT\s*\(/i,  name: 'ARRAY_CONSTRUCT' },
];

/**
 * Returns the name of the first unsupported Sigma SQL function found in the
 * expression, or null if none found. Used to skip-with-warning instead of
 * emitting broken formulas.
 */
export function detectUnsupportedSigmaFunction(formula: string): string | null {
  for (const { pattern, name } of UNSUPPORTED_SIGMA_SQL) {
    if (pattern.test(formula)) return name;
  }
  return null;
}

/** Returns true if a sql: value is a complex expression that needs formula conversion */
export function lookIsComplexSql(sql: string): boolean {
  if (!sql) return false;
  const cleaned = sql.replace(/\$\{TABLE\}\./gi, '').replace(/\$\{[^}]+\}/g, 'X').trim();
  // CAST/SAFE_CAST/TRY_CAST(col AS type) wrapping a simple column ref is not complex — just a type hint
  if (/^(?:CAST|SAFE_CAST|TRY_CAST)\s*\(\s*"?[A-Za-z_][A-Za-z0-9_]*"?\s+AS\s+\w[\w_]*\s*\)$/i.test(cleaned)) return false;
  if (/^[A-Za-z_][A-Za-z0-9_]*\s*\(/.test(cleaned)) return true;
  if (/^CASE\b/i.test(cleaned)) return true;
  if (/\bIN\s*\(/i.test(cleaned)) return true;  // SQL IN operator — needs In() formula
  if (cleaned.includes('||')) return true;      // SQL string concat — needs Concat()
  if (/[=<>!+\-*\/%]/.test(cleaned.replace(/'[^']*'/g, ''))) return true;
  return false;
}

/**
 * Split a call's argument list on TOP-LEVEL commas only — parens, quotes and
 * bracket-form identifiers are opaque. `DATE_TRUNC('month',[c])` is one
 * argument, not two.
 */
function _splitTopLevelArgs(s: string): string[] {
  const args: string[] = [];
  let depth = 0, quote = '', bracket = false, cur = '';
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (quote) { cur += c; if (c === quote) quote = ''; continue; }
    if (c === "'" || c === '"') { quote = c; cur += c; continue; }
    if (c === '[') bracket = true;
    else if (c === ']') bracket = false;
    if (!bracket) {
      if (c === '(') depth++;
      else if (c === ')') depth--;
      else if (c === ',' && depth === 0) { args.push(cur); cur = ''; continue; }
    }
    cur += c;
  }
  args.push(cur);
  return args;
}

/** MySQL 2-arg date-difference functions -> the Sigma datepart they imply. */
const _MYSQL_DIFF_UNIT: Record<string, string> = { DATEDIFF: 'day', TIMEDIFF: 'second' };

/**
 * MySQL/Domo `DATEDIFF(end, start)` -> Sigma `DateDiff("day", start, end)`
 * (and `TIMEDIFF` -> `"second"`). Bead beads-sigma-znvg.
 *
 * MySQL's DATEDIFF(expr1, expr2) is expr1 - expr2, i.e. (END, START). Sigma's
 * is DateDiff(datepart, start, end) — (START, END) plus an explicit unit. Before
 * this, LOOK_FUNC_MAP renamed DATEDIFF -> DateDiff by BARE NAME (pass 1 below),
 * so the arity was never corrected and the operands were never swapped:
 * `datediff(current_date(),[Date])` became `DateDiff(Today(),[Date])`. That is
 * not valid Sigma and produced 9 of the 15 type="error" columns on
 * domo-to-sigma's live 36-card cold run.
 *
 * SWAPPING IS THE POINT, not just the arity. `DateDiff("day", Today(), [Date])`
 * compiles cleanly and returns the NEGATION of "days since [Date]", so every
 * `>= 7` / `< 28` / `<= 30` window predicate built on it silently inverts and
 * the KPI is wrong with no error anywhere — strictly worse than the loud
 * type=error this replaces. domo-to-sigma's refs/beast-mode-to-sigma.md already
 * specified the rule ("mind arg order: BM is (end, start)").
 *
 * A 3-argument call is ALREADY in Sigma's (unit, start, end) order — left in
 * source order, never swapped, or currently-correct formulas would break. Any
 * other arity, and an unbalanced call, are left exactly as found; arguments are
 * still descended into either way, so nested calls convert.
 *
 * Runs on literal-MASKED text (see lookConvertExpression), so a quoted string
 * can never be mistaken for a call, and before the CASE pass so a DATEDIFF
 * inside a CASE span is rewritten too.
 */
function _rewriteMysqlDateDiff(expr: string): string {
  const NAME = /\b(DATEDIFF|TIMEDIFF)\s*\(/i;
  let out = '', rest = expr;
  for (;;) {
    const m = NAME.exec(rest);
    if (!m) { out += rest; break; }
    const open = m.index + m[0].length - 1;          // index of '('
    let depth = 0, close = -1;
    for (let i = open; i < rest.length; i++) {
      if (rest[i] === '(') depth++;
      else if (rest[i] === ')') { depth--; if (depth === 0) { close = i; break; } }
    }
    if (close === -1) { out += rest; break; }        // unbalanced — leave verbatim
    const inner = rest.slice(open + 1, close);
    const args = _splitTopLevelArgs(inner);
    out += rest.slice(0, m.index);
    if (args.length === 2) {
      const unit = _MYSQL_DIFF_UNIT[m[1].toUpperCase()];
      const end = _rewriteMysqlDateDiff(args[0]).trim();
      const start = _rewriteMysqlDateDiff(args[1]).trim();
      out += `DateDiff("${unit}", ${start}, ${end})`;
    } else {
      out += `${rest.slice(m.index, open + 1)}${_rewriteMysqlDateDiff(inner)})`;
    }
    rest = rest.slice(close + 1);
  }
  return out;
}

/**
 * MySQL date/time-ADDITION functions -> the Sigma datepart they imply, and
 * whether the amount is added or subtracted. The `unit` here is the default for
 * the 2-arg numeric form; an explicit `INTERVAL n <unit>` second argument
 * overrides it (see _MYSQL_INTERVAL_UNIT).
 */
const _MYSQL_ADD_SPEC: Record<string, { unit: string; negate: boolean }> = {
  ADDDATE:  { unit: 'day',    negate: false },
  SUBDATE:  { unit: 'day',    negate: true  },
  ADDTIME:  { unit: 'second', negate: false },
  SUBTIME:  { unit: 'second', negate: true  },
  DATE_ADD: { unit: 'day',    negate: false },
  DATE_SUB: { unit: 'day',    negate: true  },
};

/** MySQL INTERVAL unit keyword -> Sigma datepart. Unlisted units are refused. */
const _MYSQL_INTERVAL_UNIT: Record<string, string> = {
  SECOND: 'second', MINUTE: 'minute', HOUR: 'hour', DAY: 'day',
  WEEK: 'week', MONTH: 'month', QUARTER: 'quarter', YEAR: 'year',
};

/**
 * Negate an already-converted amount expression, textually but safely.
 * A bare numeric literal flips sign in place (`1` -> `-1`, `-1` -> `1`) so the
 * common case stays readable; anything else is wrapped, because `-a - b` and
 * `-(a - b)` are different numbers.
 */
function _negateAmount(amount: string): string {
  const t = amount.trim();
  const num = t.match(/^([+-]?)(\d+(?:\.\d+)?)$/);
  if (num) return num[1] === '-' ? num[2] : `-${num[2]}`;
  return `-(${t})`;
}

/**
 * MySQL/Domo `ADDDATE(date, n)` -> Sigma `DateAdd("day", n, date)`, plus the
 * rest of that family. Bead beads-sigma-zmnt.
 *
 * Exactly the same defect CLASS as _rewriteMysqlDateDiff above, surfacing
 * through the same mechanism: none of these names is in LOOK_FUNC_MAP, so pass
 * 1 fell through to _naiveTitleCase and emitted `Adddate(Today(), -1)` — a
 * function Sigma does not have. MEASURED on domo-to-sigma's live 36-card cold
 * run (~/domo-coldrun-v4/discovery/formulas.json): 27 ADDDATE call sites across
 * 7 Beast Modes, all emitted as `Adddate(`, and the converter's OWN
 * lookUnknownFunctions already returned ["ADDDATE"] for them — but only as a
 * warning, so nothing failed and the run carried them live.
 *
 * TWO THINGS CHANGE, not one. MySQL's is `(date, amount)`; Sigma's is
 * `(datepart, amount, date)` — so the date operand moves from FIRST to LAST and
 * a unit is prepended. Getting only the spelling right would put the date where
 * Sigma expects the amount.
 *
 * NOTE the deliberate asymmetry with T-SQL/Snowflake `DATEADD`, which is
 * ALREADY `(unit, n, date)` and is correctly handled by LOOK_FUNC_MAP's plain
 * bare-name rename. It is not touched here, and must not be: rewriting it would
 * break formulas that are currently correct.
 *
 * SUBDATE/SUBTIME/DATE_SUB negate the amount rather than mapping to a separate
 * Sigma function — Sigma has no DateSub.
 *
 * Two argument shapes are accepted, both MySQL-legal:
 *   f(date, n)                  -> DateAdd("<default unit>", n, date)
 *   f(date, INTERVAL n <unit>)  -> DateAdd("<unit>", n, date)
 * Only the 2-arg form of these functions exists in MySQL. Any other arity, an
 * unbalanced call, or an INTERVAL naming a unit Sigma has no datepart for is
 * left EXACTLY as found — it then still reaches lookUnknownFunctions and is
 * reported, which is the honest outcome. Arguments are descended into either
 * way, so nested calls convert.
 *
 * Runs on literal-MASKED text and before pass 1's bare-name rename, for the
 * same reasons documented on _rewriteMysqlDateDiff.
 */
function _rewriteMysqlDateAdd(expr: string): string {
  const NAME = /\b(ADDDATE|SUBDATE|ADDTIME|SUBTIME|DATE_ADD|DATE_SUB)\s*\(/i;
  let out = '', rest = expr;
  for (;;) {
    const m = NAME.exec(rest);
    if (!m) { out += rest; break; }
    const open = m.index + m[0].length - 1;          // index of '('
    let depth = 0, close = -1;
    for (let i = open; i < rest.length; i++) {
      if (rest[i] === '(') depth++;
      else if (rest[i] === ')') { depth--; if (depth === 0) { close = i; break; } }
    }
    if (close === -1) { out += rest; break; }        // unbalanced — leave verbatim
    const inner = rest.slice(open + 1, close);
    const args = _splitTopLevelArgs(inner);
    out += rest.slice(0, m.index);

    const spec = _MYSQL_ADD_SPEC[m[1].toUpperCase()];
    let unit = spec.unit;
    let amount: string | null = null;
    if (args.length === 2) {
      const iv = args[1].trim().match(/^INTERVAL\s+(.+?)\s+([A-Za-z_]+)\s*$/i);
      if (iv) {
        const mapped = _MYSQL_INTERVAL_UNIT[iv[2].toUpperCase()];
        // An unmappable INTERVAL unit (MICROSECOND, DAY_HOUR, …) leaves `amount`
        // null so the call falls through untouched and stays reportable.
        if (mapped) { unit = mapped; amount = _rewriteMysqlDateAdd(iv[1]).trim(); }
      } else {
        amount = _rewriteMysqlDateAdd(args[1]).trim();
      }
    }

    if (amount !== null) {
      const date = _rewriteMysqlDateAdd(args[0]).trim();
      out += `DateAdd("${unit}", ${spec.negate ? _negateAmount(amount) : amount}, ${date})`;
    } else {
      out += `${rest.slice(m.index, open + 1)}${_rewriteMysqlDateAdd(inner)})`;
    }
    rest = rest.slice(close + 1);
  }
  return out;
}

/** Map common SQL function names to Sigma equivalents */
const LOOK_FUNC_MAP: Record<string, string> = {
  'MONTH': 'Month', 'YEAR': 'Year', 'DAY': 'Day', 'HOUR': 'Hour',
  'MINUTE': 'Minute', 'SECOND': 'Second', 'QUARTER': 'Quarter',
  'WEEK': 'WeekOfYear', 'WEEKDAY': 'Weekday',
  'DATE_TRUNC': 'DateTrunc', 'DATEADD': 'DateAdd', 'DATEDIFF': 'DateDiff',
  'COALESCE': 'Coalesce', 'NVL': 'Coalesce', 'NULLIF': 'Nullif',
  'ROUND': 'Round', 'FLOOR': 'Floor', 'CEILING': 'Ceiling', 'ABS': 'Abs',
  'UPPER': 'Upper', 'LOWER': 'Lower', 'TRIM': 'Trim', 'LENGTH': 'Length',
  'SUBSTR': 'Substring', 'SUBSTRING': 'Substring', 'CONCAT': 'Concat',
  'CURRENT_DATE': 'Today()', 'GETDATE': 'Now()',
  'IFF': 'If', 'IIF': 'If', 'DECODE': 'Switch',
  'ISNULL': 'IsNull', 'IFNULL': 'Coalesce',
  'TO_DATE': 'ToDate', 'TO_NUMBER': 'ToNumber', 'TO_VARCHAR': 'Text',
};

// ── Structural (depth-aware) CASE parsing ───────────────────────────────────
// A naive split on every bare `\bWHEN\b` is blind to nesting. A nested CASE
// (routine in the live Domo corpus — e.g. inside a COUNT(...) aggregate
// argument) has its own WHEN/THEN/ELSE/END, and such a split would cut
// straight across them: the resulting fragments would straddle structural
// boundaries, producing output that is not merely wrong but shredded
// (unbalanced parens, or — worse — a plausible-looking string with a whole raw
// "CASE ... END" span duplicated into a branch VALUE).

interface _CaseMarker { type: 'WHEN' | 'THEN' | 'ELSE'; start: number; end: number }
interface _CaseScan { endStart: number; endIndex: number; markers: _CaseMarker[] }

const _CASE_KW_RE = /^(CASE|WHEN|THEN|ELSE|END)\b/i;

/**
 * Scan `s` — already literal-masked via `_maskLiterals`, so no live quotes
 * remain outside `[...]` and only bracket-awareness is needed here, not
 * quote-awareness — starting at `pos`, the index right after a "CASE" keyword
 * that put us at case-nesting depth 1. Tracks paren depth and further CASE
 * nesting: a nested CASE bumps depth to 2 (and its own END brings it back to
 * 1), so its WHEN/THEN/ELSE are never mistaken for markers belonging to the
 * CASE that opened this scan — requirement 1's "CASE-nesting depth 0",
 * expressed relative to that opening CASE. A `[bracketed identifier]` span is
 * skipped whole (same idiom as `stripOuterParens`/`_maskLiterals`) so a
 * keyword-shaped substring inside an identifier is never mistaken for
 * structure. Stops the instant depth returns to 0 — the END matching the
 * opening CASE. `endIndex === -1` means unterminated (no matching END).
 */
function _scanCase(s: string, pos: number): _CaseScan {
  const markers: _CaseMarker[] = [];
  let caseDepth = 1, parenDepth = 0, i = pos;
  while (i < s.length) {
    const c = s[i];
    if (c === '[') {
      const close = s.indexOf(']', i + 1);
      i = close === -1 ? s.length : close + 1;
      continue;
    }
    if (c === '(') { parenDepth++; i++; continue; }
    if (c === ')') { parenDepth--; i++; continue; }
    if (/[A-Za-z]/.test(c) && (i === 0 || !/[A-Za-z0-9_]/.test(s[i - 1]))) {
      const m = _CASE_KW_RE.exec(s.slice(i));
      if (m) {
        const kw = m[1].toUpperCase(), start = i, end = i + m[1].length;
        if (kw === 'CASE') {
          caseDepth++;
        } else if (kw === 'END') {
          caseDepth--;
          if (caseDepth === 0) return { endStart: start, endIndex: end, markers };
        } else if (caseDepth === 1 && parenDepth === 0) {
          markers.push({ type: kw as 'WHEN' | 'THEN' | 'ELSE', start, end });
        }
        i = end;
        continue;
      }
    }
    i++;
  }
  return { endStart: -1, endIndex: -1, markers };
}

/**
 * Parens/brackets balanced in FINAL Sigma-formed output — the backstop
 * requirement 4 asks for. Skips content inside a double-quoted Sigma string
 * literal (with `\"` escapes, matching `_unmaskLiterals`'s escaping) so a
 * paren/bracket that is DATA inside a literal (e.g. `"Sales (Q1)"`) is never
 * miscounted as structure.
 */
function _isBalanced(s: string): boolean {
  let paren = 0, bracket = 0, inStr = false;
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (inStr) {
      if (c === '\\') { i++; continue; }
      if (c === '"') inStr = false;
      continue;
    }
    if (c === '"') { inStr = true; continue; }
    if (c === '(') paren++;
    else if (c === ')') { paren--; if (paren < 0) return false; }
    else if (c === '[') bracket++;
    else if (c === ']') { bracket--; if (bracket < 0) return false; }
  }
  return paren === 0 && bracket === 0 && !inStr;
}

// Sentinel for masking a nested CASE...END out of a chunk before it goes
// through lookConvertExpression's mechanical regex passes, mirroring
// _maskCountDistinct's mask-before/unmask-after shape (requirement 2's "same
// machinery" — the same recursive-conversion pattern _unmaskCountDistinct
// already uses). Uses EOT/ENQ so it cannot collide with the literal mask
// (NUL/SOH) or the COUNT(DISTINCT) mask (STX/ETX) and — like both of those —
// carries no letters, so it is inert to pass 1/3's identifier regexes.
const _NESTED_CASE_UNMASK_RE = /(\d+)/g;

/**
 * Find every top-level "CASE ... END" span occurring anywhere inside `s` (a
 * literal-masked chunk — bracket-aware only, same reasoning as `_scanCase`),
 * recursively convert each via `lookConvertCase` (a full CASE always routes
 * there via `lookSqlToSigmaRules`'s pattern 4 — this IS the
 * `lookSqlToSigmaRules(inner) ?? lookConvertExpression(inner)` shape, just
 * inlined since `inner` is already known to start with CASE), and replace it
 * with the inert EOT/ENQ sentinel above so the surrounding mechanical passes
 * never see raw CASE/WHEN/END text.
 *
 * Used from two seams with two different failure contracts, selected by
 * `onUnparseable` (same scanner, not a fourth one; only the failure-handling
 * branch differs between the two call sites):
 *
 *  - `'abort'` (default) — `lookConvertCase`'s own `convertLeaf`, for a CASE
 *    nested inside a cond/val chunk. `lookConvertCase` returns `string | null`
 *    and CAN refuse, so the instant any nested CASE cannot be parsed
 *    confidently (or is unterminated), this returns `null` and propagates
 *    failure to the WHOLE containing formula: falling back to running raw
 *    "CASE WHEN ... END" text through `lookConvertExpression`'s regex passes
 *    would leave keywords sitting in the output as literal text — the exact
 *    shredding this whole scanner exists to prevent.
 *
 *  - `'leave-raw'` — `lookConvertExpression` itself, for a CASE embedded in
 *    an arithmetic/aggregate expression that never reaches `lookConvertCase`
 *    via `lookSqlToSigmaRules`'s anchored pattern 4 (e.g. `100 * (CASE ...
 *    END)`, `SUM((CASE ... END))`). `lookConvertExpression` returns plain
 *    `string` — it CANNOT refuse. Here a span that fails to parse (or an
 *    unterminated CASE with no matching END at all) is left EXACTLY as found
 *    — still masked, still carrying its raw CASE/WHEN/END text — and, for a
 *    parse failure, the scan resumes right after that span so the REST of
 *    the expression still converts. Never partially converts a failed span:
 *    on failure nothing is written for that span at all, so its ORIGINAL
 *    masked text survives untouched into the final output, where
 *    `hasResidualCaseKeyword` is the honest signal to the caller that this
 *    particular span didn't come through. An unterminated CASE has no
 *    reliable span boundary at all, so the scan simply stops — the entire
 *    remainder of the string from that point on is left untouched rather
 *    than guessed at.
 */
function _convertNestedCases(
  s: string,
  lits: string[],
  onUnparseable: 'abort' | 'leave-raw' = 'abort',
  // Only ever non-empty from lookConvertExpression's embedded-CASE seam,
  // which runs AFTER _maskCountDistinct — see _restoreRawCountDistinct.
  // The 'abort' call site (convertLeaf) never needs this: its input always
  // traces back to genuinely raw SQL (lookSqlToSigmaRules' pattern 4, or a
  // span already restored here), so a CD sentinel never reaches it in the
  // first place and this stays a harmless no-op default there.
  cdArgs: string[] = [],
): { text: string; blocks: string[] } | null {
  const blocks: string[] = [];
  let out = '', last = 0, i = 0;
  while (i < s.length) {
    const c = s[i];
    if (c === '[') {
      const close = s.indexOf(']', i + 1);
      i = close === -1 ? s.length : close + 1;
      continue;
    }
    if (/[A-Za-z]/.test(c) && (i === 0 || !/[A-Za-z0-9_]/.test(s[i - 1])) && /^CASE\b/i.test(s.slice(i))) {
      const caseStart = i;
      const scan = _scanCase(s, i + 4);
      if (scan.endIndex === -1) {
        if (onUnparseable === 'abort') return null;                // unterminated nested CASE
        break;                                                     // leave-raw: no reliable end — stop, rest stays as-is
      }
      const rawSpan = _restoreRawCountDistinct(_restoreRawLiterals(s.slice(caseStart, scan.endIndex), lits), cdArgs);
      const converted = lookConvertCase(rawSpan);
      if (converted === null) {
        if (onUnparseable === 'abort') return null;                // nested CASE failed to parse — fail the whole formula
        i = scan.endIndex;                                          // leave-raw: skip past it, leave this span raw
        continue;
      }
      out += s.slice(last, caseStart) + `\x04${blocks.push(converted) - 1}\x05`;
      last = scan.endIndex;
      i = scan.endIndex;
      continue;
    }
    i++;
  }
  return { text: out + s.slice(last), blocks };
}

function _spliceNestedCases(s: string, blocks: string[]): string {
  // `?? _m` is defensive, not reachable from real SQL: every sentinel this
  // masks is one we just minted from `blocks.push(...)`, so the index is
  // always in range. Guards only a crafted/adversarial input containing the
  // literal sentinel bytes.
  return s.replace(_NESTED_CASE_UNMASK_RE, (_m, i) => blocks[Number(i)] ?? _m);
}

/**
 * Convert CASE WHEN ... THEN ... ELSE ... END to nested If().
 *
 * Parses structurally rather than by naive WHEN-splitting: `_scanCase` finds
 * WHEN/THEN/ELSE markers only at case-nesting depth 1 and paren depth 0
 * (requirement 1), so a nested CASE anywhere in a condition or branch value —
 * even one with no enclosing parens at all — never has its own keywords
 * mistaken for the outer CASE's structure. Each cond/val chunk is then
 * independently scanned for an embedded nested CASE and, if found, that inner
 * CASE is recursively converted via this same function (requirement 2) before
 * the chunk's surrounding text goes through `lookConvertExpression`. Returns
 * null — never a best-effort partial parse (requirement 3) — the moment
 * anything is structurally ambiguous: an unmatched CASE/END, a WHEN with no
 * THEN, a nested CASE that itself fails to parse, or a final balance check
 * (requirement 4) that doesn't come out even.
 */
export function lookConvertCase(expr: string): string | null {
  const trimmed = expr.trim();
  const head = /^CASE\b/i.exec(trimmed);
  if (!head) return null;

  const { masked, lits } = _maskLiterals(trimmed);
  const scan = _scanCase(masked, head[0].length);
  if (scan.endIndex === -1) return null;                           // unterminated CASE — no matching END
  if (masked.slice(scan.endIndex).trim() !== '') return null;      // trailing content after the matching END

  const m = scan.markers;
  const firstMarkerStart = m.length ? m[0].start : scan.endStart;
  // Content between "CASE" and the first WHEN means this is (or claims to be)
  // a "simple CASE" (`CASE expr WHEN val THEN ...`), a form this parser does
  // not support — fail honestly rather than silently discard `expr` and
  // misinterpret the first WHEN's value as a standalone boolean condition.
  if (masked.slice(head[0].length, firstMarkerStart).trim() !== '') return null;

  const branches: { cond: string; val: string }[] = [];
  let elseVal: string | null = null;
  let idx = 0;
  while (true) {
    if (idx >= m.length || m[idx].type !== 'WHEN') return null;     // missing WHEN
    const whenTok = m[idx++];
    if (idx >= m.length || m[idx].type !== 'THEN') return null;     // WHEN with no THEN
    const thenTok = m[idx++];
    const condText = masked.slice(whenTok.end, thenTok.start);

    let valEnd: number, sawElse = false;
    if (idx < m.length && m[idx].type === 'WHEN') {
      valEnd = m[idx].start;
    } else if (idx < m.length && m[idx].type === 'ELSE') {
      valEnd = m[idx].start;
      sawElse = true;
    } else if (idx === m.length) {
      valEnd = scan.endStart;
    } else {
      return null;                                                  // unexpected marker order (e.g. THEN, THEN)
    }
    branches.push({ cond: condText, val: masked.slice(thenTok.end, valEnd) });

    if (sawElse) {
      const elseTok = m[idx++];
      if (idx !== m.length) return null;                            // structural marker(s) survive after ELSE
      elseVal = masked.slice(elseTok.end, scan.endStart);
      break;
    }
    if (idx === m.length) break;                                     // no ELSE — done
    // otherwise idx now points at the next WHEN — loop continues
  }

  if (branches.length === 0) return null;

  const convertLeaf = (maskedChunk: string, allowNumber: boolean): string | null => {
    const v = maskedChunk.trim();
    // An empty chunk — `WHEN THEN`, `THEN ELSE`, `ELSE END` with nothing
    // between them — is not a value or condition, it's a hole. Splicing it in
    // anyway would produce exactly the shredded-but-balanced output this whole
    // task exists to prevent: `If(, 1, 2)`. Caught below by the
    // `!spliced.trim()` check (an empty `v` strips/converts/splices down to
    // '' too, so one check covers both an empty chunk and a non-empty chunk
    // that collapses to nothing, e.g. `()`).
    // NOTE: string literals are deliberately NOT special-cased here —
    // lookConvertExpression masks/unmasks literals itself, emitting Sigma's
    // required double-quoted form ("West", not 'West'). A literal
    // short-circuit here would silently re-introduce single-quoted SQL-style
    // output for every CASE-THEN/ELSE string value (A6).
    if (allowNumber && /^-?\d+(\.\d+)?$/.test(v)) return v;          // number literal
    // Strip a whole-chunk paren wrapper HERE, at the point the chunk is handed
    // onward — not inside lookConvertExpression itself:
    // lookConvertExpression is a SHARED contract across lookml.ts, tools.ts,
    // etc.; confining the strip to this CASE-specific call site avoids any
    // risk of it re-associating a caller that splices its result elsewhere).
    const stripped = stripOuterParens(v);
    const nc = _convertNestedCases(stripped, lits);
    if (nc === null) return null;
    const raw = _restoreRawLiterals(nc.text, lits);
    const converted = lookConvertExpression(raw);
    const spliced = _spliceNestedCases(converted, nc.blocks);
    // Catches BOTH an originally-empty chunk (`v` was '') and a non-empty
    // chunk that strips/converts down to nothing — `()` is the live case —
    // since either way `spliced` ends up empty.
    if (!spliced.trim()) return null;
    return spliced;
  };

  let result: string | null = elseVal !== null ? convertLeaf(elseVal, true) : 'null';
  if (result === null) return null;
  for (let i = branches.length - 1; i >= 0; i--) {
    const sigmaCond = convertLeaf(branches[i].cond, false);
    const sigmaVal = convertLeaf(branches[i].val, true);
    if (sigmaCond === null || sigmaVal === null) return null;
    result = `If(${sigmaCond}, ${sigmaVal}, ${result})`;
  }

  if (!_isBalanced(result)) return null;                             // requirement 4 backstop
  return result;
}

/** Convert arithmetic/comparison SQL expression to Sigma formula */
export function lookConvertMathExpr(expr: string): string {
  // NULLIF(x, val) → If([x] = val, null, [x])
  expr = expr.replace(/NULLIF\s*\(([A-Z_][A-Z0-9_]*)\s*,\s*([^)]+)\)/gi, (_, col, val) =>
    `If(${lookColRef(col)} = ${val.trim()}, null, ${lookColRef(col)})`
  );
  return lookConvertExpression(expr);
}

// Rewriting passes below (function mapping, IN-lists, bare-identifier bracketing) are
// regex-driven and cannot tell code from data. Masking string literals out before
// they run — and restoring them after — is what stops `'AK'` becoming `'[Ak]'`.
//
// The sentinel is NUL + digits + SOH, and deliberately contains NO letters: a
// letter-bearing placeholder is itself a bare ALL-CAPS identifier, so pass 3 brackets
// it — verified, a ` L0 ` sentinel comes back as `[L 0]`. Bare digits are skipped by
// pass 3's own `/^\d+$/` guard, and control characters cannot occur in SQL.
const _LIT_RE = /'(?:[^']|'')*'/g;

// A `[bracketed identifier]` span is atomic: an apostrophe inside it
// (`[Manager's Approval]`) is part of the identifier, not a string-literal
// delimiter. Running `_LIT_RE` naively over the whole string would treat that
// apostrophe as an opening quote and swallow everything up to the NEXT real
// quote — corrupting both the identifier and the literal that followed it
// (e.g. `[Manager's Approval] = 'AK'` masked the *wrong* span, then unmasked
// to `[Manager"s Approval] = "[Ak]'` — same class of bug Task 1's review
// caught in stripOuterParens). Bracketed spans are skipped whole below;
// `_LIT_RE` only ever runs against text that is outside of `[...]`.
function _maskLiterals(s: string): { masked: string; lits: string[] } {
  const lits: string[] = [];
  let out = '';
  let i = 0;
  while (i < s.length) {
    if (s[i] === '[') {
      const close = s.indexOf(']', i + 1);
      // An unterminated '[' (no matching ']' anywhere in the rest of the
      // string) is not a real bracketed span — treat it as an ordinary
      // character and keep scanning. Swallowing to end-of-string here would
      // skip masking every literal after it, reintroducing exactly the A3
      // corruption this function exists to prevent.
      if (close !== -1) {
        out += s.slice(i, close + 1);
        i = close + 1;
        continue;
      }
    }
    if (s[i] === "'") {
      _LIT_RE.lastIndex = i;
      const m = _LIT_RE.exec(s);
      if (m && m.index === i) {
        out += `\u0000${lits.push(m[0]) - 1}\u0001`;
        i += m[0].length;
        continue;
      }
    }
    out += s[i];
    i++;
  }
  return { masked: out, lits };
}

// Restores literals in Sigma form: double-quoted, SQL's '' escape collapsed to a
// single apostrophe, and any embedded double quote backslash-escaped. Matches the
// live-verified Tableau path (see the `'x'` → `"x"` rewrite in tableauFormulaToSigma).
function _unmaskLiterals(s: string, lits: string[]): string {
  return s.replace(/\u0000(\d+)\u0001/g, (_m, i) => {
    const inner = lits[Number(i)].slice(1, -1).replace(/''/g, "'").replace(/"/g, '\\"');
    return `"${inner}"`;
  });
}

// Restores literals to their ORIGINAL raw single-quoted SQL text -- unlike
// _unmaskLiterals above, this does NOT finalize to Sigma's double-quoted form.
// Needed by lookConvertCase: a cond/val chunk is sliced out of text
// already masked via _maskLiterals, and before that chunk can be handed back
// to lookConvertExpression (or recursively to lookConvertCase, for a nested
// CASE) it must look like raw SQL again. lookConvertExpression does its OWN
// masking, keyed on finding a real '...' literal, and its OWN unmask at the
// end produces the double-quoted Sigma form -- restoring via _unmaskLiterals
// instead hands lookConvertExpression an ALREADY-double-quoted string; its
// _maskLiterals only matches single quotes, so the literal sails through
// unmasked and pass 3 (bare ALL-CAPS bracketing) corrupts it, e.g. 'AK'
// finalized early to "AK" comes back "[Ak]" -- a real regression this helper
// fixes, caught red by the existing A3/A6 and mask-ordering tests.
function _restoreRawLiterals(s: string, lits: string[]): string {
  // ?? _m is defensive for the same reason as _spliceNestedCases's guard:
  // every sentinel here is one _maskLiterals just minted, so the index is
  // always in range in practice -- this only guards a crafted input carrying
  // the literal sentinel bytes.
  return s.replace(/\u0000(\d+)\u0001/g, (_m, i) => lits[Number(i)] ?? _m);
}

// Restores a COUNT(DISTINCT ...) mask sentinel (STX/ETX) to its
// ORIGINAL raw "COUNT(DISTINCT <arg>)" SQL text -- unlike _unmaskCountDistinct,
// this does NOT convert the argument, it just reconstitutes literal SQL. Needed
// because lookConvertExpression's embedded-CASE scan (below) runs AFTER
// _maskCountDistinct, per the seam the brief specifies: a CASE span it extracts
// can carry an OUTER-scope CD sentinel embedded inside it (e.g. `CASE WHEN
// (COUNT(DISTINCT [Id]) = 0) ...` masks the DISTINCT call before the CASE scan
// ever runs). Handing that sentinel straight into lookConvertCase would leak
// it into an entirely separate, freshly-scoped recursive lookConvertExpression
// call, whose OWN _unmaskCountDistinct indexes into ITS OWN unrelated (likely
// shorter, or empty) args array using the OUTER call's index -- silently
// reading undefined and crashing in stripOuterParens (caught red-handed while
// testing this scanner: `COUNT(DISTINCT [Id])` inside a CASE embedded under
// arithmetic threw `Cannot read properties of undefined (reading 'trim')`).
// Restoring to raw text FIRST means the inner call
// re-discovers "COUNT(DISTINCT ..." as ordinary SQL and masks/processes/
// unmasks it entirely within its own call frame, with no cross-scope index
// collision -- the same reasoning `_restoreRawLiterals` already established
// for the literal mask, applied to the other mask that can now also appear
// upstream of a lookConvertCase call.
function _restoreRawCountDistinct(s: string, args: string[]): string {
  return s.replace(/\x02(\d+)\x03/g, (_m, i) => `COUNT(DISTINCT ${args[Number(i)] ?? ''})`);
}

// COUNT(DISTINCT x) has no single-token equivalent: Sigma spells it CountDistinct(x).
// A regex on the argument is not enough — the live Domo corpus nests a whole CASE
// inside one. So: scan to the matching ')', mask the call out, convert the argument
// RECURSIVELY (it is strictly shorter, so this terminates), and splice the result
// back after the outer passes have run. Masking also keeps step 1 from title-casing
// 'CountDistinct' into 'Countdistinct'. Uses STX/ETX so it cannot collide with the
// literal mask, and carries no letters — see the _maskLiterals note above.
function _maskCountDistinct(s: string): { masked: string; args: string[] } {
  const args: string[] = [];
  const re = /\bCOUNT\s*\(\s*DISTINCT\s+/gi;
  let out = '', last = 0, m: RegExpExecArray | null;
  while ((m = re.exec(s)) !== null) {
    const argStart = m.index + m[0].length;
    let depth = 1, quote = '', i = argStart;
    for (; i < s.length; i++) {
      const c = s[i];
      if (quote) { if (c === quote) quote = ''; continue; }
      if (c === "'" || c === '"') { quote = c; continue; }
      // Treat `[bracketed identifier]` as atomic, same as _maskLiterals: an
      // apostrophe inside one (`[Manager's Approval]`) must not be mistaken
      // for a quote and trap the scanner in-quote for the rest of the string
      // (depth never returns to 0, the whole call falls through unmasked).
      // An unterminated '[' is not a real bracketed span — fall through and
      // scan it as an ordinary char rather than swallowing to end-of-string.
      if (c === '[') { const cl = s.indexOf(']', i + 1); if (cl !== -1) { i = cl; continue; } }
      if (c === '(') depth++;
      else if (c === ')') { depth--; if (depth === 0) break; }
    }
    if (depth !== 0) break;                       // unbalanced — leave the rest as-is
    out += s.slice(last, m.index) + `${args.push(s.slice(argStart, i).trim()) - 1}`;
    last = i + 1;
    re.lastIndex = last;
  }
  return { masked: out + s.slice(last), args };
}

/**
 * True if `s` still contains a bare CASE/WHEN/THEN/END keyword outside a
 * `[bracketed identifier]` or a "..."/'...' literal -- the signal that a CASE
 * argument failed to parse and fell through to `lookConvertExpression`'s
 * mechanical passes, which leave SQL keywords sitting in the output as
 * literal text rather than translating them. `_unmaskCountDistinct`'s
 * `?? lookConvertExpression(raw)` fallback, and `tools.ts`'s
 * `convert_sql_to_sigma_formula` fallback, both need this same check --
 * `lookml.ts` has an equivalent, less precise one. Masks
 * brackets/literals first -- same idiom `formulaHasUntranslatableFragment`
 * uses for the Tableau path -- so a column legitimately named `[End]` or a
 * literal 'the end' never false-positives.
 */
export function hasResidualCaseKeyword(s: string): boolean {
  const masked = s.replace(/"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|\[[^\]]*\]/g, ' ');
  return /\b(?:CASE|WHEN|THEN|END)\b/i.test(masked);
}

/**
 * True if `s` still contains a bare, untranslated SQL infix operator that has
 * NO Sigma equivalent -- LIKE, BETWEEN -- outside a `[bracketed identifier]`
 * or a "..."/'...' literal. Same masking idiom as `hasResidualCaseKeyword`,
 * for the same reason a column legitimately named `[Between]` or a literal
 * containing the word "like" must never false-positive.
 *
 * Converting an embedded CASE span can silence `hasResidualCaseKeyword` (no
 * more CASE/WHEN/THEN/END survives) while a DIFFERENT untranslated SQL
 * construct still sits, unconverted, inside the newly-produced `If(...)`
 * condition -- corpus[63]'s `LOWER(...) LIKE 'usa'` is the measured example:
 * converting the embedded CASE alone makes it convert cleanly on the outside
 * while the LIKE survives untranslated inside it, and without this check the
 * formula would be reported as fully converted. No function anywhere in this
 * file translates
 * LIKE or BETWEEN to a Sigma equivalent -- LIKE in particular has no direct
 * Sigma operator at all (Contains/RegexpMatch differ in wildcards, anchoring,
 * and case-sensitivity, so mapping to either would silently change behavior,
 * not just syntax) -- so this is "report honestly that no translation
 * exists," the same posture `hasResidualCaseKeyword` already takes for an
 * unsupported CASE shape, not "invent a translation."
 */
export function hasResidualInfixOperator(s: string): boolean {
  const masked = s.replace(/"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|\[[^\]]*\]/g, ' ');
  return /\b(?:LIKE|BETWEEN)\b/i.test(masked);
}

function _unmaskCountDistinct(s: string, args: string[]): string {
  return s.replace(/(\d+)/g, (_m, i) => {
    const raw = stripOuterParens(args[Number(i)]);
    const viaRules = lookSqlToSigmaRules(raw);
    const converted = viaRules ?? lookConvertExpression(raw);
    // The argument failed to parse via the rule engine (almost certainly a
    // CASE, the only construct that can reach here and still carry a bare
    // keyword) and the mechanical fallback left WHEN/THEN/END sitting in the
    // text -- the exact shredding this task exists to prevent, one level
    // removed. Leave the whole call as recognizable, untranslated raw SQL
    // rather than dressing broken text up as a converted CountDistinct(...).
    if (viaRules === null && hasResidualCaseKeyword(converted)) {
      return `COUNT(DISTINCT ${raw})`;
    }
    return `CountDistinct(${converted})`;
  });
}

// Reserved words are syntax, not callables. `AND (`, `WHEN (`, `NOT (` all look like
// a function call to a name-before-paren regex; rewriting them to And()/When()/Not()
// produces Sigma that silently returns null rows. `OVER`, `GROUP` (as in
// `WITHIN GROUP (...)`), and `EXISTS` are the same defect class: `SUM(x) OVER (...)`
// and `LISTAGG(x) WITHIN GROUP (...)` were becoming bogus `Over(...)` / `Group(...)`
// calls. DISTINCT is included for the `SELECT DISTINCT(col)` style (DISTINCT
// directly before a paren) — it does NOT interfere with `COUNT(DISTINCT x)`, since
// there DISTINCT is followed by a space then its argument, never directly by '(',
// so this regex never matches it in that shape regardless of list membership.
//
// SHARED between pass 1 (name-before-paren callable check) and pass 3 (bare
// ALL_CAPS identifier bracketing) below, AND with dbt.ts's
// preBracketKnownNames — a second, independently-maintained keyword list is
// exactly how AS/ON/BY/DISTINCT drifted out of pass 3 once already: pass 3
// had its own inline list missing them, so `A AS B` bracketed AS into a bogus
// `[As]` column and `GROUP_COL BY OTHER` did the same to BY. One constant,
// used by all three call sites, closes that off structurally.
export const _SQL_KEYWORD_RE = /^(?:AND|OR|NOT|IN|IS|NULL|CASE|WHEN|THEN|ELSE|END|BETWEEN|LIKE|AS|ON|BY|DISTINCT|TRUE|FALSE|OVER|GROUP|EXISTS)$/i;

// The exact naive title-case pass 1 falls back to for an unrecognised SQL function
// name: first character upper, everything else lower — regardless of the source's own
// casing (`AddDate`/`ADDDATE`/`adddate` all fall back to `Adddate`). Shared between
// pass 1 itself and the passthrough-derivation below so the two can never drift
// apart the way two independently-maintained keyword lists did above.
function _naiveTitleCase(fn: string): string {
  return fn.charAt(0).toUpperCase() + fn.slice(1).toLowerCase();
}

// ── Pass 3 double-bracketing (bead qorq) ────────────────────────────────────
// Pass 3 below rewrites a bare ALL-CAPS identifier to `[Display Name]`, but its
// regex has no way to tell a token that is ALREADY inside `[...]` from a
// genuinely bare one: `\b[A-Z_][A-Z0-9_]*\b` matches `NET_REVENUE` identically
// whether or not it is sitting inside brackets. So an already-bracketed
// ALL-CAPS ref got wrapped a SECOND time: `[NET_REVENUE]` -> `[[Net
// Revenue]]`. Measured on merged main (2ba3ea8): every Beast Mode from a
// Snowflake-backed Domo instance hits this, because Snowflake folds unquoted
// column names to UPPERCASE and Domo's own `convert-beast-modes.rb`
// preprocessing already emits `[Bracketed]` refs — the live 74-formula corpus
// measured 0/74 for this defect purely because that corpus's sample data uses
// mixed-case names, never exercising the ALL-CAPS path at all.
//
// Fix: mask every `[...]` span out of pass 3's view. Unlike the other three
// masks in this file, this one does not restore the ORIGINAL bracket text —
// it restores the FINAL, already-decided text computed up front by
// `_bracketSpanFinalText` (the same "mask now, compute the finished text now,
// splice the finished text back after the mechanical pass" shape
// `_convertNestedCases` already uses for a CASE span, not the
// mask/restore-raw-then-convert-later shape `_maskLiterals` uses for a string
// literal). An ALL-CAPS bracket body is converted to its display name NOW
// (`[NET_REVENUE]` -> `[Net Revenue]`, exactly what pass 3 would do to the
// same text if it were bare); any other bracket body (mixed case, spaces, an
// apostrophe — `[Net Revenue]`, `[Order Id]`, `[Manager's Approval]`) is
// already valid Sigma bracket syntax and is masked/restored VERBATIM.
//
// ORDERING: this mask/unmask pair is scoped tightly around pass 3 alone
// (masked immediately before it, unmasked immediately after), not wrapped
// around passes 1/2 too, and it sits AFTER the CD mask, literal mask, and
// nested-CASE mask that already ran at the top of `lookConvertExpression` —
// load-bearing, not incidental:
//   - AFTER the literal mask: a string literal (already reduced to an inert
//     NUL/digit/SOH sentinel by the time this runs) can never be mistaken for
//     a live `[...]` span by this scanner, even when its ORIGINAL text
//     happened to contain literal `[`/`]` characters — verified below with a
//     literal that contains ALL-CAPS text. Scanning for brackets before the
//     literal mask ran would risk exactly that confusion.
//   - AFTER the CD mask: a bracket already pulled out of `expr` entirely into
//     `cd.args` (e.g. `[ORDER_ID]` inside `COUNT(DISTINCT [ORDER_ID])`) is
//     simply not present here to mis-mask — it gets this SAME fix
//     independently, inside `_unmaskCountDistinct`'s own recursive
//     `lookConvertExpression` call on that shorter argument string.
//   - AFTER the nested-CASE mask: a CASE embedded in arithmetic was already
//     fully converted via `lookConvertCase` (which applies this same fix to
//     its own leaf chunks) and spliced in as an inert EOT/ENQ sentinel before
//     pass 1 ever ran, so no live bracket from that span reaches pass 3 either.
//   - BEFORE only pass 3, not passes 1/2: nothing else in passes 1 (function
//     mapping) or 2 (IN-list) can wrap a bracket's inner content a second
//     time — pattern 2's IN-list rewrite captures a `[...]` LHS as one opaque
//     unit and never touches what's inside it, so those passes need no
//     bracket-blindness fix; only pass 3's identifier-level regex does.
//
// Uses SO/SI (\x0E/\x0F — Shift Out / Shift In) so it cannot collide with the
// literal mask (NUL/SOH), the COUNT(DISTINCT) mask (STX/ETX), or the
// nested-CASE mask (EOT/ENQ), and — like all three — carries NO letters: a
// letter-bearing placeholder would itself look like a bare ALL-CAPS
// identifier and get bracketed by the very pass it exists to hide from
// (verified against the same hazard the literal mask's own comment already
// documents for a ` L0 ` sentinel coming back as `[L 0]`).
const _BRACKET_UNMASK_RE = /\x0E(\d+)\x0F/g;

function _bracketSpanFinalText(rawSpan: string): string {
  const inner = rawSpan.slice(1, -1);
  // Same shape pass 3 itself tests a bare token against: a real ALL-CAPS SQL
  // identifier, not a reserved word. Anything else — mixed case, spaces, an
  // apostrophe, digits-first — is already valid Sigma bracket syntax; leave
  // it exactly as found.
  if (/^[A-Z_][A-Z0-9_]*$/.test(inner) && !_SQL_KEYWORD_RE.test(inner)) {
    return lookColRef(inner);
  }
  return rawSpan;
}

function _maskBrackets(s: string): { masked: string; spans: string[] } {
  const spans: string[] = [];
  let out = '', i = 0;
  while (i < s.length) {
    if (s[i] === '[') {
      const close = s.indexOf(']', i + 1);
      // An unterminated '[' (no matching ']' anywhere in the rest of the
      // string) is not a real bracketed span — same policy _maskLiterals and
      // _maskCountDistinct already settled: treat it as an ordinary character
      // and keep scanning. Swallowing to end-of-string here would hide every
      // ALL-CAPS identifier after it from pass 3 entirely, leaving them
      // un-bracketed — the same class of corruption the unterminated-'['
      // policy elsewhere in this file exists to prevent.
      if (close !== -1) {
        const finalText = _bracketSpanFinalText(s.slice(i, close + 1));
        out += `\x0E${spans.push(finalText) - 1}\x0F`;
        i = close + 1;
        continue;
      }
    }
    out += s[i];
    i++;
  }
  return { masked: out, spans };
}

function _unmaskBrackets(s: string, spans: string[]): string {
  // `?? _m` is defensive, not reachable from real SQL: every sentinel here is
  // one `_maskBrackets` just minted, so the index is always in range — same
  // reasoning as `_spliceNestedCases`'s and `_restoreRawLiterals`'s guards.
  return s.replace(_BRACKET_UNMASK_RE, (_m, i) => spans[Number(i)] ?? _m);
}

/** Convert an entire expression: map functions, convert column refs, fix IN lists */
export function lookConvertExpression(expr: string): string {
  const cd = _maskCountDistinct(expr);
  const { masked, lits } = _maskLiterals(cd.masked);
  expr = masked;

  // MySQL 2-arg DATEDIFF/TIMEDIFF -> Sigma's (unit, start, end). Bead
  // beads-sigma-znvg. Placed here deliberately: AFTER the literal mask (a quoted
  // string can never be mistaken for a call) and BEFORE both the CASE pass and
  // pass 1's bare-name rename — the CASE pass would otherwise carry a DATEDIFF
  // inside a WHEN span out of reach, and pass 1 is what used to rename DATEDIFF
  // to DateDiff while leaving the arity and operand order wrong.
  expr = _rewriteMysqlDateDiff(expr);

  // MySQL ADDDATE/SUBDATE/ADDTIME/SUBTIME/DATE_ADD/DATE_SUB -> Sigma's
  // DateAdd(unit, amount, date). Bead beads-sigma-zmnt — same placement and the
  // same reasons as the DATEDIFF rewrite above. Order between the two does not
  // matter: both split arguments paren-aware and recurse, so a DATEDIFF wrapping
  // an ADDDATE (the live `% Change - Pageviews` shape, which carries BOTH bugs)
  // converts fully either way.
  expr = _rewriteMysqlDateAdd(expr);

  // Convert any embedded "CASE ... END" span, not only a CASE that is the
  // WHOLE expression (lookSqlToSigmaRules' anchored pattern 4 already owns
  // that case). A CASE wrapped in arithmetic or an aggregate — `100 * (CASE
  // ... END)`, `SUM((CASE ... END))`, a ratio with a CASE on either side of
  // `/` — never matches pattern 4's `/^CASE\b/i` anchor, so without this it
  // would fall through to this function with no CASE awareness at all,
  // leaving raw CASE/WHEN/THEN/END text sitting in otherwise-converted
  // output. Reuses `_convertNestedCases`/`_scanCase` verbatim in 'leave-raw'
  // mode — wiring the existing scanner into a new position, not a new parser.
  //
  // SEAM, and why here specifically: after the COUNT(DISTINCT …) mask and the
  // literal mask, before the three rewrite passes below.
  //   - AFTER the COUNT(DISTINCT) mask: a CASE nested inside a
  //     `COUNT(DISTINCT (CASE ...))` argument is already gone from `expr` at
  //     this point — pulled out into `cd.args` as an opaque digit sentinel —
  //     so this scan never sees it. It gets its own fully independent
  //     recursive `lookConvertExpression` call later, inside
  //     `_unmaskCountDistinct`, which runs this same seam again on that
  //     shorter argument string. Scanning for CASE before this mask ran would
  //     mean trying to bound a CASE that is not actually free-standing here.
  //   - AFTER the literal mask: a string literal inside the CASE span (`THEN
  //     'a' ELSE 'b'`) must not be mistaken for CASE-shaped keyword text, and
  //     the extracted span needs to be restored to real (single-quoted) SQL
  //     via `_restoreRawLiterals` before it is handed to `lookConvertCase`,
  //     which does its own literal masking/finalizing independently.
  //   - BEFORE the three passes: a converted `If(...)` is masked out here
  //     too (EOT/ENQ sentinel), so passes 1-3 see only inert sentinel text in
  //     its place, never raw CASE/WHEN/THEN/END for pass 1/3's identifier
  //     regexes to mishandle.
  //
  // MODE 'leave-raw', not convertLeaf's 'abort' default: this function
  // returns `string`, not `string | null` — it cannot refuse (requirement 4).
  // A span that fails to parse is left EXACTLY as found and the rest of the
  // expression still converts around it (requirement 3); `hasResidualCaseKeyword`
  // on the final output is the honest signal that a span did not come through.
  const ec = _convertNestedCases(expr, lits, 'leave-raw', cd.args);
  // 'leave-raw' mode never returns null (see _convertNestedCases) — the `!`
  // reflects that contract, not an unchecked assumption.
  expr = ec!.text;

  // 1. Map SQL function names to Sigma equivalents
  expr = expr.replace(/\b([A-Z_][A-Z0-9_]*)\s*(?=\()/gi, (match, fn) => {
    const upper = fn.toUpperCase();
    if (_SQL_KEYWORD_RE.test(upper)) return match;              // keyword, not a call
    const mapped = LOOK_FUNC_MAP[upper];
    // A map value may already carry its own parens (CURRENT_DATE -> 'Today()'). Only
    // the NAME is being substituted here; the source's own '()' follows, so keeping
    // the mapped parens yields 'Today()()'.
    if (mapped) return mapped.endsWith('()') ? mapped.slice(0, -2) : mapped;
    return _naiveTitleCase(fn);
  });

  // 2. Convert EXPR IN (a, b, c) → In(EXPR, a, b, c)
  // LHS can be a bracket-form [Display Name] or a word/call expression
  expr = expr.replace(/(\[[^\]]+\]|[\w\]\)]+(?:\([^)]*\))?)\s+IN\s*\(([^)]+)\)/gi, (_, lhs, list) => {
    return `In(${lhs}, ${list})`;
  });

  // 3. Convert bare ALL_CAPS identifiers (not followed by '(') to [Display Name]
  // Mask [bracketed] spans first — see the bead-qorq block above: this
  // regex cannot tell a token INSIDE brackets from a bare one, so without the
  // mask an already-bracketed ALL-CAPS ref gets wrapped a second time.
  {
    const { masked: bracketMasked, spans } = _maskBrackets(expr);
    expr = bracketMasked.replace(/\b([A-Z_][A-Z0-9_]*)\b(?!\s*\()/g, (match) => {
      if (_SQL_KEYWORD_RE.test(match)) return match;              // shared with pass 1 — see comment above
      if (/^\d+$/.test(match)) return match;
      return lookColRef(match);
    });
    expr = _unmaskBrackets(expr, spans);
  }

  // Unmask in mirror order relative to how the three masks were applied
  // above (CD mask -> literal mask -> CASE mask): CASE splice first, since it
  // was masked LAST / is the innermost layer, then literals, then
  // COUNT(DISTINCT) last (masked first / outermost layer).
  //
  // A converted CASE block never actually carries an as-yet-unmasked
  // COUNT(DISTINCT) sentinel into this splice: `_restoreRawCountDistinct`
  // (see `_convertNestedCases`) restores any such sentinel to genuine raw
  // `COUNT(DISTINCT ...)` SQL text BEFORE the span is ever handed to
  // `lookConvertCase`, so the recursive call fully masks/converts/unmasks it
  // within its own frame and returns a block with no control byte left in it
  // at all — verified directly: the block produced for `100 * (CASE WHEN
  // (COUNT(DISTINCT [Id]) = 0) THEN 'a' ELSE 'b' END)` contains zero STX/ETX
  // bytes. This mirror ordering is kept anyway as cheap, defensive
  // belt-and-suspenders (it costs nothing and guards against a FUTURE change
  // to `_convertNestedCases`/`_restoreRawCountDistinct` reintroducing a
  // leak), not because a live constraint currently depends on it.
  expr = _spliceNestedCases(expr, ec!.blocks);
  return _unmaskCountDistinct(_unmaskLiterals(expr, lits), cd.args).trim();
}

// Sigma function names not derivable from LOOK_FUNC_MAP's or TABLEAU_FUNC_MAP's own
// VALUES because neither map ever emits them under their own bare name — COUNT/RANK/
// LAG/LEAD are handled by dedicated code (COUNT via _maskCountDistinct/
// _unmaskCountDistinct's CountDistinct(...) wrapping, RANK/LAG/LEAD via the inline
// fnMap objects inside tableauWindowToSigmaChart), never via a name -> name Record.
// Run through the SAME title-case derivation below as the two maps (not hand-asserted)
// so a typo or a future multi-word addition here fails SAFE — excluded, still warns —
// rather than being silently trusted.
const _SUPPLEMENTAL_SIGMA_NAMES = [
  'Count',  // resources.ts formula-syntax reference: "Count([Col])"
  'Rank',   // qlik.test.ts: Rank(Sum([Sales Amount]), "desc")
  'Lag',    // qlik.test.ts: Lag(Sum([Sales Amount]), 1)
  'Lead',   // qlik.test.ts: Lead(Sum([Sales Amount]), 1)
];

/**
 * Sigma functions the SQL path emits directly (same spelling in source and target),
 * on top of everything LOOK_FUNC_MAP already knows how to translate.
 *
 * DERIVED, not hand-maintained — a hand-written list drifts, the same failure
 * mode two independently-maintained keyword lists hit elsewhere in this file
 * (see `_SQL_KEYWORD_RE` above). Built from LOOK_FUNC_MAP's and
 * TABLEAU_FUNC_MAP's own VALUES — both are
 * independently-verified real Sigma function name strings (TABLEAU_FUNC_MAP's values
 * are exercised by a different converter, tableauFormulaToSigma, but the NAMES it emits
 * are real Sigma functions regardless of which converter proves them) — plus the small
 * supplemental list above for names neither map happens to emit. Adding an entry to
 * either map later automatically keeps this set correct with no second edit required.
 *
 * The predicate, applied uniformly to every candidate: does `_naiveTitleCase` of the
 * bare name reproduce Sigma's real spelling EXACTLY? `Now` -> naive title-case of `NOW`
 * is `Now` -> match -> safe, don't warn on a bare `NOW(`. `DateTrunc` -> naive
 * title-case of `DATETRUNC` is `Datetrunc` -> mismatch -> a bare, underscore-less
 * `DATETRUNC(` must still warn, because pass 1's fallback cannot reproduce the second
 * embedded capital no matter how the source SQL cased it. This is the exact reasoning
 * that excluded DATEPART/DATETRUNC from the original hand-written list, made explicit
 * and applied mechanically to every candidate instead of by manual inspection — which
 * is what independently recovers NOW/TODAY/POWER/SWITCH/the trig family (RANK/LAG/LEAD
 * needed the supplemental list above; nothing else did) without hand-listing any of
 * them, and would have caught ABS/COALESCE/DATEDIFF/DATEADD as redundant with
 * LOOK_FUNC_MAP's own keys had they been hand-listed — moot here, since nothing
 * in this derived set is hand-listed for its own sake.
 *
 * Lazily computed and memoized on first call rather than at module-load time, so this
 * has no dependency on LOOK_FUNC_MAP/TABLEAU_FUNC_MAP's declaration order in the file.
 */
let _sigmaPassthroughCache: Set<string> | null = null;
function _sigmaPassthrough(): Set<string> {
  if (_sigmaPassthroughCache) return _sigmaPassthroughCache;
  const names = new Set<string>();
  for (const raw of [...Object.values(LOOK_FUNC_MAP), ...Object.values(TABLEAU_FUNC_MAP), ..._SUPPLEMENTAL_SIGMA_NAMES]) {
    // A map value may carry its own parens (CURRENT_DATE -> 'Today()') — strip them
    // before comparing, same as pass 1 does when splicing a mapped value back in.
    const stripped = raw.endsWith('()') ? raw.slice(0, -2) : raw;
    if (_naiveTitleCase(stripped) === stripped) names.add(stripped.toUpperCase());
  }
  _sigmaPassthroughCache = names;
  return names;
}

/**
 * Names that step 1 of lookConvertExpression would title-case WITHOUT a real mapping
 * — i.e. names Sigma almost certainly does not have (`Levenshtein` → `Levenshtein`).
 * The conversion still returns a formula; this is what lets the caller say so out loud
 * instead of shipping a silently-broken column (see converter-silent-fallback.test.ts).
 * Scans the RAW input (masked for literals only, same idiom as everywhere else in this
 * file), not the converted output — every candidate is a name immediately followed by
 * '(', mirroring pass 1's own name-before-paren regex exactly so this reports precisely
 * the set pass 1 would (mis)handle.
 *
 * "Precisely the set pass 1 would mishandle" is why the MySQL date rewrites run here
 * too (beads-sigma-znvg / beads-sigma-zmnt). They fire BEFORE pass 1 in
 * lookConvertExpression, so a call they consume never reaches pass 1 and must not be
 * reported. Scanning the raw text instead over-reported all seven of their names —
 * ADDDATE, SUBDATE, ADDTIME, SUBTIME, DATE_ADD, DATE_SUB and (since #122) TIMEDIFF —
 * as "no Sigma mapping" while the converter was in fact translating them correctly.
 * That is not a cosmetic wart: domo-to-sigma's convert-beast-modes.rb:562 surfaces
 * this list to operators, and bead zmnt's own recommendation is to PROMOTE that
 * warning to a hard failure. Seven standing false positives would make the warning
 * un-promotable and train operators to ignore it.
 *
 * Rewriting rather than name-listing keeps the report CALL-accurate, not merely
 * name-accurate: `DATE_ADD([d], INTERVAL 5 MICROSECOND)` has no Sigma datepart, so the
 * rewrite deliberately declines it, it really does reach pass 1, and it is still
 * reported here — which a blanket skip-list would have silently swallowed.
 */
export function lookUnknownFunctions(sql: string): string[] {
  const { masked: rawMasked } = _maskLiterals(sql);
  const masked = _rewriteMysqlDateAdd(_rewriteMysqlDateDiff(rawMasked));
  const passthrough = _sigmaPassthrough();
  const seen = new Set<string>();
  for (const m of masked.matchAll(/\b([A-Za-z_][A-Za-z0-9_]*)\s*(?=\()/g)) {
    const upper = m[1].toUpperCase();
    if (_SQL_KEYWORD_RE.test(upper)) continue;
    if (LOOK_FUNC_MAP[upper] || passthrough.has(upper)) continue;
    seen.add(upper);
  }
  return [...seen];
}

/**
 * Rule-based SQL → Sigma formula converter for common patterns.
 * Returns a Sigma formula string, or null if the pattern isn't recognised.
 */
export function lookSqlToSigmaRules(sql: string): string | null {
  let expr = sql
    .replace(/\$\{TABLE\}\./gi, '')
    .replace(/\$\{[^.}]+\.([^}]+)\}/g, (_, f) => f.toUpperCase())
    .replace(/\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g, (_, n) => n.toUpperCase())
    .replace(/[\r\n]+\s*/g, ' ')
    .trim();

  expr = stripOuterParens(expr);

  // Pattern 1: COLUMN = 1 (yesno boolean flag)
  {
    const m = expr.match(/^([A-Z_][A-Z0-9_]*)\s*=\s*(\d+)$/i);
    if (m) return `${lookColRef(m[1])} = ${m[2]}`;
  }

  // Pattern 1b: COLUMN IN ('val1', 'val2', ...) → In([Column], "val1", "val2")
  {
    const m = expr.match(/^([A-Z_][A-Z0-9_]*)\s+IN\s*\(([^)]+)\)$/i);
    if (m) {
      const col = lookColRef(m[1]);
      const vals = m[2].split(',').map(v => {
        v = v.trim();
        if (/^'[^']*'$/.test(v)) return `"${v.slice(1, -1)}"`;
        return v;
      });
      return `In(${col}, ${vals.join(', ')})`;
    }
  }

  // Pattern 1b-bracket: [Display Name] OP number
  // Handles cases where expandFieldRefs already converted to bracket form
  {
    const m = expr.match(/^(\[[^\]]+\])\s*(>=|<=|!=|<>|>|<|=)\s*(-?\d+(?:\.\d+)?)$/i);
    if (m) return `${m[1]} ${m[2] === '<>' ? '!=' : m[2]} ${m[3]}`;
  }

  // Pattern 1c-bracket: [Display Name] IN ('val1', 'val2', ...) → In([Display Name], "val1", ...)
  // Handles bracket-form LHS after expandFieldRefs expansion
  {
    const m = expr.match(/^(\[[^\]]+\])\s+IN\s*\(([^)]+)\)$/i);
    if (m) {
      const vals = m[2].split(',').map(v => {
        v = v.trim();
        if (/^'[^']*'$/.test(v)) return `"${v.slice(1, -1)}"`;
        return v;
      });
      return `In(${m[1]}, ${vals.join(', ')})`;
    }
  }

  // Pattern 2: ROUND(expr, n)
  if (/^ROUND\s*\(/i.test(expr)) {
    const inner = expr.replace(/^ROUND\s*\(/i, '').replace(/\)\s*$/, '');
    const lastComma = inner.lastIndexOf(',');
    if (lastComma >= 0) {
      const mathExpr = inner.slice(0, lastComma).trim();
      const decimals = inner.slice(lastComma + 1).trim();
      const converted = lookConvertMathExpr(mathExpr);
      return `Round(${converted}, ${decimals})`;
    }
  }

  // Pattern 3: DATEDIFF('unit', col_a, col_b)
  {
    const m = expr.match(/^DATEDIFF\s*\(\s*'([^']+)'\s*,\s*([A-Z_][A-Z0-9_]*)\s*,\s*([A-Z_][A-Z0-9_]*)\s*\)$/i);
    if (m) return `DateDiff("${m[1]}", ${lookColRef(m[2])}, ${lookColRef(m[3])})`;
  }

  // Pattern 4: CASE WHEN ... END
  if (/^CASE\b/i.test(expr)) {
    return lookConvertCase(expr);
  }

  // Pattern 5: simple arithmetic on column refs
  if (/^[A-Z_][A-Z0-9_]*\s*[+\-*\/]/.test(expr) || /NULLIF/i.test(expr)) {
    return lookConvertMathExpr(expr);
  }

  // Pattern 6: SQL string concatenation (a || ' ' || b) → Concat(Text([A]), " ", Text([B]))
  // Column refs are wrapped in Text(): SQL || coerces operands to text implicitly,
  // but Sigma Concat requires string args (a numeric column → "Invalid arg type").
  // Text() is a no-op on text columns, so wrapping every ref is always safe.
  if (expr.includes('||')) {
    const parts = expr.split('||').map(p => {
      p = p.trim();
      if (/^'[^']*'$/.test(p)) return `"${p.slice(1, -1)}"`;            // 'x'  -> "x" (literal)
      if (/^\[[^\]]+\]$/.test(p)) return `Text(${p})`;                  // [Display] ref -> Text([Display])
      if (/^[A-Z_][A-Z0-9_]*$/i.test(p)) return `Text(${lookColRef(p)})`; // bare column -> Text([Display])
      return null;                                                     // a part we can't safely map
    });
    if (parts.length > 1 && parts.every(p => p !== null)) return `Concat(${parts.join(', ')})`;
  }

  return null;
}

// ── Tableau formula conversion ────────────────────────────────────────────────

const TABLEAU_FUNC_MAP: Record<string, string> = {
  'AVG': 'Avg', 'MAX': 'Max', 'MIN': 'Min', 'MEDIAN': 'Median',
  'SUM': 'Sum', 'ABS': 'Abs', 'CEILING': 'Ceiling', 'FLOOR': 'Floor',
  'ROUND': 'Round', 'SQRT': 'Sqrt', 'POWER': 'Power',
  // scalar math (verified resolve in Sigma 2026-06-15; Tableau LOG default base 10 == Sigma Log default base 10)
  'LN': 'Ln', 'LOG': 'Log', 'EXP': 'Exp', 'MOD': 'Mod', 'SIGN': 'Sign', 'PI': 'Pi',
  // trig + angle conversion — same names/arg-order in Sigma (live-verified 2026-07-10, bead tt3z.3)
  'SIN': 'Sin', 'COS': 'Cos', 'TAN': 'Tan', 'COT': 'Cot',
  'ASIN': 'Asin', 'ACOS': 'Acos', 'ATAN': 'Atan', 'ATAN2': 'Atan2',
  'DEGREES': 'Degrees', 'RADIANS': 'Radians',
  // PROPER (title-case) — live-verified Sigma Proper() (bead tt3z.3)
  'PROPER': 'Proper',
  'STR': 'Text', 'INT': 'Int', 'FLOAT': 'Number',
  'LEN': 'Len', 'UPPER': 'Upper', 'LOWER': 'Lower',
  'TRIM': 'Trim', 'LTRIM': 'Ltrim', 'RTRIM': 'Rtrim',
  'LEFT': 'Left', 'RIGHT': 'Right', 'MID': 'Mid',
  'REPLACE': 'Replace', 'CONTAINS': 'Contains',
  'STARTSWITH': 'StartsWith', 'ENDSWITH': 'EndsWith', 'FIND': 'Find',
  'TODAY': 'Today', 'NOW': 'Now',
  'YEAR': 'Year', 'MONTH': 'Month', 'DAY': 'Day',
  'HOUR': 'Hour', 'MINUTE': 'Minute', 'SECOND': 'Second',
  // NOTE: no 'WEEK' entry — Sigma has no Week() function; WEEK(date) is rewritten
  // to DatePart("week", date) below (verified via docs + live query 2026-07-10).
  'QUARTER': 'Quarter',
  'DATE': 'Date', 'DATETIME': 'Datetime', 'MAKEDATE': 'MakeDate',
  // regex (same arg order as Tableau)
  'REGEXP_EXTRACT': 'RegexpExtract', 'REGEXP_REPLACE': 'RegexpReplace', 'REGEXP_MATCH': 'RegexpMatch',
  // statistical aggregates (sample variants direct; STDEVP handled above)
  'STDEV': 'StdDev', 'VAR': 'Variance', 'VARP': 'VariancePop',
  'PERCENTILE': 'PercentileCont', 'CORR': 'Corr',
  // string split — both 1-indexed, negatives count from the right
  'SPLIT': 'SplitPart',
};

// ── Tableau table calcs → Sigma window functions (CHART context only) ───────
// WINPROBE-validated mappings (live-proven 930/930 against warehouse window
// functions). CONTEXT CAVEAT: every Sigma function emitted here
// (Cumulative*/Moving*/Rank/RankDense/RankPercentile/PercentOfTotal/
// RowNumber/Lag/Lead) is valid ONLY in chart / grouped-workbook-element
// context. They silently error in data-model element calc columns and in
// workbook master calc columns (feedback_sigma_window_functions.md), so the
// DM converter must route them to result.workbookPatterns, never into a DM
// column/metric. NEVER rewrite to *Over functions — SumOver/MaxOver/CountOver
// resolve as 'Unknown function' in spec contexts.

/** Sigma window functions that only resolve in chart/grouped-element context.
 *  Any converted formula matching this must NOT be emitted as a DM calc
 *  column or metric (GrandTotal is deliberately absent — it is DM-safe). */
export const SIGMA_CHART_ONLY_WINDOW_RE =
  /\b(?:Cumulative(?:Sum|Avg|Min|Max|Count)|Moving(?:Sum|Avg|Min|Max|Count|StdDev)|RankDense|RankPercentile|Rank|PercentOfTotal|RowNumber|Lag|Lead)\s*\(/;

/** Raw Tableau table-calc tokens — used to detect table calcs embedded inside
 *  larger expressions (which the anchored mapper can't claim) so they are
 *  flagged loudly instead of leaking as silently-broken DM formulas.
 *  Deliberately case-sensitive (Tableau table calcs are conventionally
 *  uppercase) so converted Sigma output like Rank(...) never false-positives. */
export const TABLEAU_TABLE_CALC_TOKEN_RE =
  /\b(?:WINDOW_[A-Z]+|RUNNING_[A-Z]+|LOOKUP|PREVIOUS_VALUE|RANK(?:_[A-Z]+)?|INDEX|SIZE|TOTAL|FIRST|LAST)\s*\(/;

/** Case-INSENSITIVE subset of the table-calc tokens whose names no Sigma function
 *  shares — so a lowercase `running_sum(` / `window_sum(` (some .twb files store
 *  table calcs lower-case) is still caught. Kept separate from the case-sensitive
 *  regex above so ambiguous names (Rank/Lookup/First/Last/Index/Total) don't
 *  false-positive on legitimately-converted Sigma output. */
export const TABLEAU_TABLE_CALC_TOKEN_CI_RE =
  /\b(?:WINDOW_[A-Za-z]+|RUNNING_[A-Za-z]+|PREVIOUS_VALUE)\s*\(/i;

/** A leftover Level-of-Detail expression embedded inside a larger formula —
 *  `{ FIXED [d] : agg }`, `{ INCLUDE … }`, `{ EXCLUDE … }`. The top-level LOD
 *  path converts a whole-formula LOD, but a NESTED one survives translation and
 *  would silently break the column in a DM. */
export const TABLEAU_LOD_LEFTOVER_RE = /\{\s*(?:FIXED|INCLUDE|EXCLUDE)\b/i;

/** True when a (already-translated) formula still carries an untranslatable
 *  table-calc / LOD / no-equivalent fragment that must NOT be emitted as a DM
 *  calc column or metric (it silently errors there). Routes to workbookPatterns /
 *  loud-skip instead. Covers leftover comment markers the translator emits for
 *  un-mappable constructs. */
export function formulaHasUntranslatableFragment(f: string): boolean {
  if (!f) return false;
  if (TABLEAU_LOD_LEFTOVER_RE.test(f)
    || TABLEAU_TABLE_CALC_TOKEN_RE.test(f)
    || TABLEAU_TABLE_CALC_TOKEN_CI_RE.test(f)
    || /\/\*\s*(?:LOD|table calc|no Sigma equivalent)/.test(f)) return true;
  // Leftover SQL CASE syntax (Sigma has no CASE/WHEN/THEN/END — only If/Switch).
  // A `then`/`end`/`when` keyword surviving translation means tableauControlToSigma
  // could not claim a (often malformed/nested) CASE; emitting it errors the column.
  // Mask string literals + [bracket refs] first so a column/value containing those
  // letters (e.g. [Month End], "trend") never false-positives.
  const masked = f.replace(/"[^"]*"|'[^']*'|\[[^\]]*\]/g, ' ');
  return /\b(?:then|end|when)\b/i.test(masked);
}

const _TC_AGG_MAP: Record<string, string> = {
  SUM: 'Sum', AVG: 'Avg', MIN: 'Min', MAX: 'Max', COUNT: 'Count',
  COUNTD: 'CountDistinct', MEDIAN: 'Median', STDEV: 'StdDev', VAR: 'Variance',
};

const _TC_COL = '(\\[[^\\]]+\\]|[A-Za-z_][A-Za-z0-9_]*)';
const _TC_AGG = '(SUM|AVG|MIN|MAX|COUNT|COUNTD|MEDIAN|STDEV|VAR)';
// groups: 1 = agg func, 2 = column ref
const _TC_AGG_EXPR = `${_TC_AGG}\\s*\\(\\s*${_TC_COL}\\s*\\)`;

/** Bracketed display-name column ref (same ALL_CAPS → Title Case rule as the
 *  main formula converter's final pass). */
function _tcCol(raw: string): string {
  const name = raw.replace(/^\[|\]$/g, '');
  if (/^[A-Z][A-Z0-9_]{2,}$/.test(name)) return '[' + sigmaDisplayName(name) + ']';
  return '[' + name + ']';
}

function _tcAgg(aggFunc: string, colRaw: string): string {
  const fn = _TC_AGG_MAP[aggFunc.toUpperCase()] || 'Sum';
  return `${fn}(${_tcCol(colRaw)})`;
}

function _tcSameRef(a: string, b: string): boolean {
  const norm = (s: string) => s.replace(/^\[|\]$/g, '').replace(/[^A-Za-z0-9_]/g, '_').toUpperCase();
  return norm(a) === norm(b);
}

export interface TableauWindowChartResult {
  formula: string;   // ready-to-place Sigma formula — CHART/grouped-element context ONLY
  kind: 'cumulative' | 'moving' | 'percent-of-total' | 'rank' | 'lag' | 'lead' | 'index';
  verify?: boolean;  // semantics approximated — verify numbers vs Tableau
  note?: string;
}

/** Returns the name of a Tableau table-calc function that has NO validated
 *  Sigma equivalent (must be flagged loudly, never emitted), or null. */
export function tableauWindowUntranslatable(formula: string): string | null {
  const m = (formula || '').match(/\b(WINDOW_MEDIAN|WINDOW_PERCENTILE|WINDOW_CORR|WINDOW_COVARP?|WINDOW_VARP?|WINDOW_STDEVP|PREVIOUS_VALUE|SIZE)\s*\(/i);
  return m ? m[1].toUpperCase() : null;
}

/**
 * Map a complete Tableau table-calc formula to its validated Sigma
 * chart-context window-function equivalent. Returns null when the formula is
 * not a recognised (validated) table-calc pattern. Patterns are anchored —
 * table calcs embedded inside larger expressions are NOT claimed (use
 * TABLEAU_TABLE_CALC_TOKEN_RE to detect and flag those).
 */
export function tableauWindowToSigmaChart(formula: string): TableauWindowChartResult | null {
  const f = (formula || '').trim();
  if (!f || tableauWindowUntranslatable(f)) return null;
  let m: RegExpMatchArray | null;

  // AGG([x]) / TOTAL(AGG([x]))  (or / WINDOW_SUM(AGG([x])))
  //   →  PercentOfTotal(Agg([x]), "grand_total")
  // Tableau's TOTAL(SUM(x)) and WINDOW_SUM(SUM(x)) are the same grand-total
  // denominator under default (Table) addressing — both map to grand_total.
  m = f.match(new RegExp(`^${_TC_AGG_EXPR}\\s*\\/\\s*(?:TOTAL|WINDOW_SUM)\\s*\\(\\s*${_TC_AGG_EXPR}\\s*\\)$`, 'i'));
  if (m && m[1].toUpperCase() === m[3].toUpperCase() && _tcSameRef(m[2], m[4])) {
    return { formula: `PercentOfTotal(${_tcAgg(m[1], m[2])}, "grand_total")`, kind: 'percent-of-total' };
  }

  // RUNNING_SUM(AGG([x])) / TOTAL(AGG([x]))  (or / WINDOW_SUM(AGG([x])))
  //   →  CumulativeSum(PercentOfTotal(Agg([x]), "grand_total"))
  m = f.match(new RegExp(`^RUNNING_SUM\\s*\\(\\s*${_TC_AGG_EXPR}\\s*\\)\\s*\\/\\s*(?:TOTAL|WINDOW_SUM)\\s*\\(\\s*${_TC_AGG_EXPR}\\s*\\)$`, 'i'));
  if (m && m[1].toUpperCase() === m[3].toUpperCase() && _tcSameRef(m[2], m[4])) {
    return { formula: `CumulativeSum(PercentOfTotal(${_tcAgg(m[1], m[2])}, "grand_total"))`, kind: 'cumulative' };
  }

  // RUNNING_SUM/AVG/MAX/MIN/COUNT(AGG([x]))  →  Cumulative*(Agg([x]))
  m = f.match(new RegExp(`^RUNNING_(SUM|AVG|MIN|MAX|COUNT)\\s*\\(\\s*${_TC_AGG_EXPR}\\s*\\)$`, 'i'));
  if (m) {
    const fn = 'Cumulative' + m[1].charAt(0).toUpperCase() + m[1].slice(1).toLowerCase();
    return { formula: `${fn}(${_tcAgg(m[2], m[3])})`, kind: 'cumulative' };
  }
  // Bare-column form RUNNING_SUM([x]) — inner aggregate implied by the outer fn
  m = f.match(new RegExp(`^RUNNING_(SUM|AVG|MIN|MAX|COUNT)\\s*\\(\\s*${_TC_COL}\\s*\\)$`, 'i'));
  if (m) {
    const fn = 'Cumulative' + m[1].charAt(0).toUpperCase() + m[1].slice(1).toLowerCase();
    return { formula: `${fn}(${_tcAgg(m[1], m[2])})`, kind: 'cumulative' };
  }

  // WINDOW_SUM/AVG/MIN/MAX/STDEV(AGG([x]), -n, 0)  →  Moving*(Agg([x]), n)
  // WINDOW_*(AGG([x]), -n, m)                      →  Moving*(Agg([x]), n, m)
  m = f.match(new RegExp(`^WINDOW_(SUM|AVG|MIN|MAX|STDEV)\\s*\\(\\s*${_TC_AGG_EXPR}\\s*,\\s*(-?\\d+)\\s*,\\s*(-?\\d+)\\s*\\)$`, 'i'));
  if (m) {
    const back = parseInt(m[4], 10);
    const fwd = parseInt(m[5], 10);
    if (back <= 0 && fwd >= 0) {
      const movMap: Record<string, string> = {
        SUM: 'MovingSum', AVG: 'MovingAvg', MIN: 'MovingMin', MAX: 'MovingMax',
        STDEV: 'MovingStdDev',
      };
      const fn = movMap[m[1].toUpperCase()];
      const args = fwd === 0 ? `${-back}` : `${-back}, ${fwd}`;
      return { formula: `${fn}(${_tcAgg(m[2], m[3])}, ${args})`, kind: 'moving' };
    }
    return null; // offsets that don't span the current row — not validated
  }

  // RANK/RANK_DENSE/RANK_PERCENTILE(AGG([x])[, 'asc'|'desc'])
  //   →  Rank/RankDense/RankPercentile(Agg([x]), "desc")
  m = f.match(new RegExp(`^(RANK|RANK_DENSE|RANK_PERCENTILE|RANK_UNIQUE)\\s*\\(\\s*${_TC_AGG_EXPR}\\s*(?:,\\s*['"]?(asc|desc)['"]?\\s*)?\\)$`, 'i'));
  if (m) {
    const fnMap: Record<string, string> = {
      RANK: 'Rank', RANK_DENSE: 'RankDense', RANK_PERCENTILE: 'RankPercentile', RANK_UNIQUE: 'Rank',
    };
    const fn = fnMap[m[1].toUpperCase()];
    const dir = (m[4] || 'desc').toLowerCase();
    const unique = m[1].toUpperCase() === 'RANK_UNIQUE';
    return {
      formula: `${fn}(${_tcAgg(m[2], m[3])}, "${dir}")`, kind: 'rank',
      ...(unique ? { verify: true, note: 'RANK_UNIQUE breaks ties arbitrarily; Sigma Rank assigns equal ranks to ties — verify against Tableau.' } : {}),
    };
  }

  // INDEX() → RowNumber()
  if (/^INDEX\s*\(\s*\)$/i.test(f)) return { formula: 'RowNumber()', kind: 'index' };

  // LOOKUP(AGG([x]), -n) → Lag(Agg([x]), n);  LOOKUP(AGG([x]), n) → Lead(Agg([x]), n)
  m = f.match(new RegExp(`^LOOKUP\\s*\\(\\s*${_TC_AGG_EXPR}\\s*,\\s*(-?\\d+)\\s*\\)$`, 'i'));
  if (m) {
    const off = parseInt(m[3], 10);
    const agg = _tcAgg(m[1], m[2]);
    if (off === 0) return { formula: agg, kind: 'lag', note: 'LOOKUP(expr, 0) is the identity — no window function needed.' };
    return off < 0
      ? { formula: `Lag(${agg}, ${-off})`, kind: 'lag' }
      : { formula: `Lead(${agg}, ${off})`, kind: 'lead' };
  }

  return null;
}

// `f` here is ALREADY masked (see the single mask-on-entry in
// tableauFormulaToSigma below) — real THEN/ELSE/END/WHEN keywords sitting
// inside a live string literal used to confuse this split (a literal
// containing the word "THEN" swallowed a branch value entirely). With
// masking in effect before this runs, a literal's own keyword-looking text is
// hidden inside its opaque sentinel, so only genuine code keywords split.
// `lits` is threaded through so `_tableauRecurse` (below) can hand each
// extracted branch fragment to a FRESH tableauFormulaToSigma call as genuine
// raw text, then fold that call's result back into THIS call's shared
// sentinel space.
//
// ── Depth-aware IF/CASE block scanning ──────────────────────────────────────
// A first-match `/\bIF\b([\s\S]+?)\bEND\b/gi` (or CASE's equivalent) is blind
// to nesting: it stops at the FIRST "END" in the string regardless of depth,
// so a nested IF or CASE inside a THEN/ELSE branch — which has its own END —
// gets mistaken for the OUTER block's closing keyword, leaving everything
// past that point (the real ELSE branch, the real END) as raw leftover text.
// Live-reproduced, no literals involved (see tableau.nested-if-case.test.ts):
//   IF [a]=1 THEN IF [b]=2 THEN 'x' ELSE 'y' END ELSE 'z' END
//     -> `If([a]=1, IF [b]=2, "y") ELSE "z" END`   (garbage, not a crash)
//
// `_scanTableauBlock` fixes this the same way `_scanCase` (SQL/LookML half,
// above) fixes the analogous SQL CASE-nesting bug: starting at `pos` (right
// after the opening IF/CASE keyword, at block-nesting depth 1 relative to
// it), it tracks a SHARED depth counter that ANY nested IF or CASE bumps —
// not two independent counters — so "CASE inside IF" and "IF inside CASE"
// both nest correctly against the same scan, and only records a
// THEN/ELSEIF/ELSE/WHEN marker as belonging to THIS block when it occurs at
// depth 1. A `[bracketed identifier]` span is skipped whole (same idiom as
// `_scanCase`/`_maskLiterals`); string literals need no such care here since
// they are already hidden behind the mask-on-entry sentinel by the time this
// runs. The correctly-bounded branch text is handed to `_tableauRecurse`
// (unchanged), which converts it via a FRESH `tableauFormulaToSigma` call —
// so an inner control structure is converted by its OWN translator, never by
// this scan's naive text-splitting.
interface _TabBlockMarker { type: 'THEN' | 'ELSEIF' | 'ELSE' | 'WHEN'; start: number; end: number }
interface _TabBlockScan { endStart: number; endIndex: number; markers: _TabBlockMarker[] }

const _TAB_BLOCK_KW_RE = /^(IF|CASE|ELSEIF|THEN|ELSE|WHEN|END)\b/i;

function _scanTableauBlock(s: string, pos: number): _TabBlockScan {
  const markers: _TabBlockMarker[] = [];
  let blockDepth = 1, i = pos;
  while (i < s.length) {
    const c = s[i];
    if (c === '[') {
      const close = s.indexOf(']', i + 1);
      i = close === -1 ? s.length : close + 1;
      continue;
    }
    if (/[A-Za-z]/.test(c) && (i === 0 || !/[A-Za-z0-9_]/.test(s[i - 1]))) {
      const m = _TAB_BLOCK_KW_RE.exec(s.slice(i));
      if (m) {
        const kw = m[1].toUpperCase(), start = i, end = i + m[1].length;
        if (kw === 'IF' || kw === 'CASE') {
          blockDepth++;
        } else if (kw === 'END') {
          blockDepth--;
          if (blockDepth === 0) return { endStart: start, endIndex: end, markers };
        } else if (blockDepth === 1) {
          markers.push({ type: kw as 'THEN' | 'ELSEIF' | 'ELSE' | 'WHEN', start, end });
        }
        i = end;
        continue;
      }
    }
    i++;
  }
  return { endStart: -1, endIndex: -1, markers };
}

// Reconstructs an IF block's nested If(...) from its structurally-scanned
// markers (THEN/ELSEIF/ELSE at depth 1 — see `_scanTableauBlock`) rather than
// a naive text `.split()`, so a nested control structure's OWN THEN/ELSEIF/
// ELSE (already excluded from `markers` by the depth check) can never be
// mistaken for this block's structure. `innerOffset` converts the markers'
// absolute positions (in the outer masked formula) to positions relative to
// `inner`.
function _convertIfBody(inner: string, markers: _TabBlockMarker[], innerOffset: number, lits: string[]): string {
  const rel = markers.map(mk => ({ type: mk.type, start: mk.start - innerOffset, end: mk.end - innerOffset }));
  const elseMarker = rel.find(mk => mk.type === 'ELSE');
  const chainEnd = elseMarker ? elseMarker.start : inner.length;
  const elseVal = elseMarker ? _tableauRecurse(inner.slice(elseMarker.end).trim(), lits) : 'null';
  // Chain alternates THEN, ELSEIF, THEN, ELSEIF, ..., THEN — one THEN per
  // clause, one ELSEIF between consecutive clauses.
  const chain = rel.filter(mk => mk.type === 'THEN' || mk.type === 'ELSEIF');
  const clauses: { cond: string; val: string }[] = [];
  let condStart = 0;
  for (let k = 0; k < chain.length; k += 2) {
    const thenMk = chain[k];
    if (!thenMk || thenMk.type !== 'THEN') break;      // malformed — stop, same best-effort spirit as the original split
    const cond = inner.slice(condStart, thenMk.start).trim();
    const nextMk = chain[k + 1];                        // the following ELSEIF marker, if any
    const valEnd = nextMk ? nextMk.start : chainEnd;
    const val = inner.slice(thenMk.end, valEnd).trim();
    clauses.push({ cond, val });
    condStart = nextMk ? nextMk.end : valEnd;
  }
  let result = elseVal;
  for (let k = clauses.length - 1; k >= 0; k--) {
    result = 'If(' + _tableauRecurse(clauses[k].cond, lits) + ', ' + _tableauRecurse(clauses[k].val, lits) + ', ' + result + ')';
  }
  return result;
}

// Same masked-input contract as _convertIfBody above, mirrored for CASE's
// WHEN/THEN/ELSE structure (a CASE clause compares its `field` against each
// WHEN-value, unlike IF's free-form boolean condition).
function _convertCaseBody(inner: string, markers: _TabBlockMarker[], innerOffset: number, lits: string[]): string {
  const rel = markers.map(mk => ({ type: mk.type, start: mk.start - innerOffset, end: mk.end - innerOffset }));
  const elseMarker = rel.find(mk => mk.type === 'ELSE');
  const chainEnd = elseMarker ? elseMarker.start : inner.length;
  const elseVal = elseMarker ? _tableauRecurse(inner.slice(elseMarker.end).trim(), lits) : 'null';
  const chain = rel.filter(mk => mk.type === 'WHEN' || mk.type === 'THEN');
  const firstWhen = chain.find(mk => mk.type === 'WHEN');
  const field = firstWhen ? _tableauRecurse(inner.slice(0, firstWhen.start).trim(), lits) : '[?]';
  const clauses: { cond: string; val: string }[] = [];
  for (let k = 0; k < chain.length; k += 2) {
    const whenMk = chain[k];
    const thenMk = chain[k + 1];
    if (!whenMk || whenMk.type !== 'WHEN' || !thenMk || thenMk.type !== 'THEN') break;  // malformed — stop
    const nextWhenMk = chain[k + 2];
    const cond = inner.slice(whenMk.end, thenMk.start).trim();
    const valEnd = nextWhenMk ? nextWhenMk.start : chainEnd;
    const val = inner.slice(thenMk.end, valEnd).trim();
    clauses.push({ cond, val });
  }
  let result = elseVal;
  for (let k = clauses.length - 1; k >= 0; k--) {
    result = 'If(' + field + ' = ' + _tableauRecurse(clauses[k].cond, lits) + ', ' + _tableauRecurse(clauses[k].val, lits) + ', ' + result + ')';
  }
  return result;
}

// IF-block and CASE-block conversion MUST run as a single unified scan, not
// two independent sequential whole-string passes (which is what an earlier
// version of this fix did, mirroring the pre-fix tableauIfToSigma/
// tableauCaseToSigma split, and which reviewer testing caught as newly
// broken): once a first pass finishes converting every IF block in `f`, the
// string it hands to a second, separate CASE pass already contains the
// literal text "If(" from that conversion — and `_scanTableauBlock`'s
// keyword match is case-insensitive (it has to be, Tableau keywords are
// case-insensitive), so "If(" satisfies `\bIF\b` and is misread as the
// opener of a FRESH nested IF block that (having no THEN/END of its own)
// never finds a matching END, aborting the scan and leaving the outer CASE
// entirely unconverted. Scanning for IF and CASE openers TOGETHER, in one
// left-to-right pass that never revisits text it already substituted,
// avoids this: a nested block (of either kind) is only ever converted once,
// via `_tableauRecurse`'s fresh sub-call, before the outer scan's pointer
// jumps straight past the whole consumed span.
function tableauControlToSigma(f: string, lits: string[]): string {
  let out = '', last = 0, i = 0;
  while (i < f.length) {
    const c = f[i];
    if (c === '[') {
      const close = f.indexOf(']', i + 1);
      i = close === -1 ? f.length : close + 1;
      continue;
    }
    if (/[A-Za-z]/.test(c) && (i === 0 || !/[A-Za-z0-9_]/.test(f[i - 1]))) {
      const isIf = /^IF\b/i.test(f.slice(i));
      const isCase = !isIf && /^CASE\b/i.test(f.slice(i));
      if (isIf || isCase) {
        const blockStart = i;
        const bodyStart = i + (isIf ? 2 : 4);
        const scan = _scanTableauBlock(f, bodyStart);
        if (scan.endIndex === -1) { i++; continue; }      // unterminated — leave as raw text, matches prior behavior
        const inner = f.slice(bodyStart, scan.endStart);
        const converted = isIf
          ? _convertIfBody(inner, scan.markers, bodyStart, lits)
          : _convertCaseBody(inner, scan.markers, bodyStart, lits);
        out += f.slice(last, blockStart) + converted;
        last = scan.endIndex;
        i = scan.endIndex;
        continue;
      }
    }
    i++;
  }
  return out + f.slice(last);
}

// ── Tableau-specific literal masking ────────────────────────────────────────
// tableauFormulaToSigma's function-name mapping (COUNT→CountIf, ZN→Coalesce,
// TABLEAU_FUNC_MAP, …) and its keyword-casing passes (TRUE/FALSE/NULL/AND/
// OR/NOT) scan the raw formula text with no idea that string literals exist —
// live-reproduced: tableauFormulaToSigma("'See Count(Open Items) report'")
// returned `"See CountIf(IsNotNull(Open Items)) report"`, rewriting the
// LITERAL'S CONTENT — a value that reaches the customer's dashboard.
//
// This mirrors _maskLiterals/_unmaskLiterals (SQL/LookML half, above) but is
// a separate, Tableau-specific variant, NOT a widened `_maskLiterals`:
// `_LIT_RE` is single-quote-only ON PURPOSE — in SQL, `"foo"` is a quoted
// IDENTIFIER, not a string, so widening it to double quotes would make the
// SQL/LookML path treat quoted identifiers as data and corrupt THAT path
// instead (it has extensive tests). Tableau accepts BOTH `'...'` and `"..."`
// as string delimiters, so this variant has to handle both.
//
// Escaping: Tableau calculation syntax escapes an embedded quote with a
// BACKSLASH (`'It\'s a test'`, `"She said \"hi\""`) — NOT SQL's doubled-quote
// (`''`) convention. This isn't guesswork: two independent call sites already
// in this file parse Tableau string literals this way — _isTextOperand's
// literal check (`/^'(?:[^'\\]|\\.)*'$/`) just above, and
// tableauParamSwitchToSigma's `when`-value unescape (`.replace(/\\(.)/g,
// '$1')`). The masker below uses the same `(?:[^'\\]|\\.)*` shape.
//
// Same two properties as the SQL masker, preserved for the same reasons (see
// _maskLiterals above): a `[bracketed identifier]` span is atomic — an
// apostrophe inside `[Manager's Approval]` is part of the identifier, not a
// quote — and an unterminated `[` (or unterminated quote) is treated as an
// ordinary character, not swallowed to end-of-string.
//
// tableauFormulaToSigma masks ONCE, on entry, for its ENTIRE body — every
// pass (IN-list, IF/CASE lowering, the date/user-context functions, the
// TABLEAU_FUNC_MAP loop, keyword-casing, bracket-casing) runs against masked
// text, full stop. An earlier version of this fix masked in two separate
// windows with a stretch of raw text running IN-list/IF/CASE/DATEPART/
// USERNAME between them — reviewed and rejected: that stretch reproduced the
// exact bug this fix exists to kill (`'Please choose IN (1,2,3)…'` corrupted;
// `IF [x] = 'contains THEN keyword' THEN 'a' ELSE 'b' END` silently lost the
// 'a' branch — not corruption, a WRONG ANSWER; `'See DATEPART(\'year\',
// [Date]) info'` converted inside the quotes). Mask-once, with the two
// recursing helper (tableauControlToSigma) explicitly
// restoring-then-remasking around their own recursive calls, is the only
// version proven closed against all of those.
const _TABLEAU_LIT_SQ_RE = /'(?:[^'\\]|\\.)*'/g;
const _TABLEAU_LIT_DQ_RE = /"(?:[^"\\]|\\.)*"/g;
const _TABLEAU_SENTINEL_SRC = '\u0000(\\d+)\u0001';
const _TABLEAU_SENTINEL_RE = /\u0000(\d+)\u0001/g;

// `lits` is optional so the top-level call in tableauFormulaToSigma can start
// a fresh array, while _tableauRecurse (below) passes the OUTER call's array
// in to APPEND to it (continuing its index numbering) rather than starting a
// colliding fresh one.
function _maskTableauLiterals(s: string, lits: string[] = []): { masked: string; lits: string[] } {
  let out = '';
  let i = 0;
  while (i < s.length) {
    if (s[i] === '[') {
      const close = s.indexOf(']', i + 1);
      if (close !== -1) {
        out += s.slice(i, close + 1);
        i = close + 1;
        continue;
      }
    }
    if (s[i] === "'" || s[i] === '"') {
      const re = s[i] === "'" ? _TABLEAU_LIT_SQ_RE : _TABLEAU_LIT_DQ_RE;
      re.lastIndex = i;
      const m = re.exec(s);
      if (m && m.index === i) {
        out += `\u0000${lits.push(m[0]) - 1}\u0001`;
        i += m[0].length;
        continue;
      }
    }
    out += s[i];
    i++;
  }
  return { masked: out, lits };
}

// Restores literals to their ORIGINAL raw Tableau text (quotes, backslash
// escapes and all). Used only by _tableauRecurse (below): tableauControlToSigma
// is the only pass that recurses into a FRESH
// tableauFormulaToSigma call per branch, and that fresh call must see genuine
// Tableau text, not a sentinel keyed into THIS call's private `lits` array
// (the same cross-scope-index hazard _restoreRawLiterals documents above for
// the SQL half's lookConvertCase).
function _restoreRawTableauLiterals(s: string, lits: string[]): string {
  return s.replace(_TABLEAU_SENTINEL_RE, (_m, i) => lits[Number(i)] ?? _m);
}

// Resolves a masked literal's TRUE content (quotes stripped, Tableau's
// backslash escapes collapsed) from its sentinel index.
function _tabLitInner(lits: string[], idxStr: string): string {
  const raw = lits[Number(idxStr)];
  return raw === undefined ? '' : raw.slice(1, -1).replace(/\\(.)/g, '$1');
}

// Escapes literal content for Sigma's own double-quoted output — backslash
// first, then embedded double quotes (the same order _unmaskLiterals applies
// for SQL's convention, applied here to Tableau's).
function _tabEscapeForSigma(inner: string): string {
  return inner.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
}

// Finalizes every remaining masked literal to Sigma's double-quoted form —
// the single unmask that closes out tableauFormulaToSigma's mask-on-entry.
function _unmaskTableauLiterals(s: string, lits: string[]): string {
  return s.replace(_TABLEAU_SENTINEL_RE, (_m, i) => `"${_tabEscapeForSigma(_tabLitInner(lits, i))}"`);
}

// Restores a masked slice to raw Tableau text, recursively converts it via a
// FRESH tableauFormulaToSigma call (that call does its own complete
// mask→process→unmask, fully self-contained), then re-masks the result INTO
// the shared `lits` array (continuing its index numbering, so no collision
// with sentinels the OUTER call already minted). Used by tableauControlToSigma,
// the only pass that recurses into a fresh
// top-level call per branch.
//
// Both hazards this closes are real, not hypothetical: handing the recursive
// call a slice that still carries THIS call's sentinels would leak a foreign
// lits index into an unrelated call frame and misresolve; handing the
// recursive call's OUTPUT back to the outer pipeline UNmasked would just
// relocate the original corruption to the branch-splice boundary — a nested
// literal in a branch value would sail through the outer pipeline's
// remaining passes (TABLEAU_FUNC_MAP, keyword-casing, bracket-casing)
// unprotected. Re-masking the recursive result closes both.
function _tableauRecurse(maskedSlice: string, lits: string[]): string {
  const raw = _restoreRawTableauLiterals(maskedSlice, lits);
  const converted = tableauFormulaToSigma(raw);
  const { masked } = _maskTableauLiterals(converted, lits);
  return masked;
}

/** Convert a Tableau calculated field formula to Sigma formula syntax */
export function tableauFormulaToSigma(formula: string, warnings?: string[]): string {
  if (!formula || !formula.trim()) return '';
  // Decode numeric XML entities (fxp leaves &#10; literal) and strip //comments
  // BEFORE any pattern matching, so the rest of the translator sees clean text.
  const raw0 = stripLineComments(decodeXmlEntities(formula)).trim();

  // Mask ONCE, here, for the ENTIRE function body — see _maskTableauLiterals
  // above. From this point on, nothing sees a live quote unless it
  // deliberately restores one (tableauControlToSigma's
  // recursive branches, via _tableauRecurse — see there for why, and how the
  // result gets re-masked before rejoining this pipeline). `raw0` is kept
  // around only for the early bail-out paths just below (LOD/table-calc/
  // COVAR), which show a human the ORIGINAL formula text in a comment —
  // sentinels would be meaningless there.
  const { masked, lits } = _maskTableauLiterals(raw0);
  let f = masked;

  // LOD expressions
  if (/^\s*\{/.test(f)) {
    if (warnings) warnings.push('⚠ LOD expression not converted: ' + raw0.slice(0, 60));
    return '/* LOD: ' + raw0.replace(/\/\*/g, '').replace(/\*\//g, '') + ' */';
  }
  // Table calcs — WINPROBE-validated mappings to Sigma window functions.
  // CONTEXT CAVEAT: the emitted Sigma window functions are valid in CHART /
  // grouped-workbook-element context ONLY — they silently error in DM element
  // calc columns and workbook master calc columns. Never emit *Over functions
  // (SumOver/MaxOver/CountOver = 'Unknown function' in spec contexts).
  //
  // Matched against raw0 (NOT masked `f`): every pattern here is anchored
  // (`^...$` — the ENTIRE trimmed formula must match), and a string literal
  // always starts with a quote character, so an anchored `^RANK\(` -style
  // pattern can never accidentally match into one regardless of whether the
  // text is masked. tableauWindowToSigmaChart's RANK case needs a REAL quote
  // to read its optional 'asc'/'desc' direction argument — masked text broke
  // that (regression caught by tableau.window.test.ts's RANK-family suite:
  // `RANK(SUM(x), 'asc')` fell through to the untranslated-comment path
  // because the masked sentinel no longer looked like `'asc'`).
  {
    const winChart = tableauWindowToSigmaChart(raw0);
    if (winChart) {
      if (warnings) warnings.push(
        `ℹ Table calc → ${winChart.formula} — CHART/grouped-element context ONLY: place in a grouped workbook element (group by the viz dimensions); window functions silently error in data-model calc columns and workbook master calc columns.`
        + (winChart.note ? ' ' + winChart.note : ''));
      return winChart.formula;
    }
    const untrans = tableauWindowUntranslatable(raw0);
    if (untrans) {
      if (warnings) warnings.push(`⚠ Table calculation NOT converted — ${untrans}() has no Sigma equivalent. Untranslated fragment: ${raw0.slice(0, 120)}`);
      return '/* table calc: ' + raw0.replace(/\/\*/g, '').replace(/\*\//g, '') + ' */';
    }
    if (/^(WINDOW_|RUNNING_|FIRST\(|LAST\(|INDEX\(|RANK\b|RANK_|LOOKUP\(|TOTAL\s*\()/i.test(raw0)) {
      // WINDOW_SUM(AGG([x])) with no offsets → GrandTotal(Agg([x])) — DM-safe
      // (exact when the chart groups by a single dimension set).
      const gt = raw0.match(/^WINDOW_SUM\s*\(\s*(SUM|COUNT|AVG|MIN|MAX)\s*\(\s*(\[[^\]]+\])\s*\)\s*\)$/i);
      if (gt) {
        const aggMap: Record<string, string> = { SUM: 'Sum', COUNT: 'Count', AVG: 'Avg', MIN: 'Min', MAX: 'Max' };
        return 'GrandTotal(' + (aggMap[gt[1].toUpperCase()] || gt[1]) + '(' + gt[2] + '))';
      }
      // Anchored table calc we couldn't map — flag loudly, never emit silently.
      if (warnings) warnings.push(`⚠ Table calculation not converted. Untranslated fragment: ${raw0.slice(0, 120)}`);
      return '/* table calc: ' + raw0.replace(/\/\*/g, '').replace(/\*\//g, '') + ' */';
    }
  }

  // COVAR/COVARP have no Sigma equivalent — flag loudly, never emit silently
  // (Sigma has Corr but no covariance function; verified 2026-06-15).
  // Matched against MASKED text: a literal that merely CONTAINS "COVAR("
  // text is already hidden inside its own opaque sentinel by this point, so
  // this can no longer false-bail-out on data (a bonus of mask-once — it
  // used to scan raw text here).
  if (/\bCOVARP?\s*\(/i.test(f)) {
    if (warnings) warnings.push(`⚠ COVAR/COVARP has no Sigma equivalent — not converted. Fragment: ${raw0.slice(0, 120)}`);
    return '/* no Sigma equivalent: ' + raw0.replace(/\/\*/g, '').replace(/\*\//g, '') + ' */';
  }

  // ZN([x]) → Coalesce([x], 0)
  f = f.replace(/\bZN\s*\(([^)]+)\)/gi, 'Coalesce($1, 0)');
  f = f.replace(/\bIFNULL\s*\(/gi, 'Coalesce(').replace(/\bIFERROR\s*\(/gi, 'Coalesce(');
  f = f.replace(/\bISNULL\s*\(/gi, 'IsNull(');
  // COUNT([x]) → CountIf(IsNotNull([x]))
  f = f.replace(/\bCOUNT\s*\(([^)]+)\)/gi, (m, arg) => 'CountIf(IsNotNull(' + arg.trim() + '))');
  f = f.replace(/\bCOUNTD\s*\(/gi, 'CountDistinct(');
  // ATTR([x]) → just [x]
  f = f.replace(/\bATTR\s*\(([^)]+)\)/gi, '$1');

  // [Field] IN (…) → or-chain (Sigma has no IN operator). Run before If/CASE
  // lowering so an `If([X] in (…), …)` condition is already a boolean chain.
  // Safe directly on masked text with NO changes to tableauInToSigma itself:
  // a literal's own "IN (" text is already hidden inside ONE opaque sentinel
  // (masking collapsed the whole literal before this ever runs), and a
  // GENUINE `[Field] IN ('a','b')` list's own value literals are themselves
  // separate sentinels — inert to its internal quote/paren-depth tracking,
  // so they pass through as opaque atoms and resolve correctly at the final
  // unmask below.
  f = tableauInToSigma(f);

  // IF and CASE blocks convert in ONE unified pass — see
  // tableauControlToSigma's comment for why running them as two separate
  // sequential passes (an earlier version of this fix) is wrong.
  f = tableauControlToSigma(f, lits);
  f = f.replace(/\bIIF\s*\(/gi, 'If(');

  // DATEPART('year', [Date]) → Year([Date]) — sentinel-aware: matches the
  // MASKED form of the unit-name literal (never a live quote) and resolves
  // its true content via `lits`. This is what makes both directions correct
  // at once: a literal that merely CONTAINS the text "DATEPART('year', ...)"
  // is, by this point, ALREADY one single opaque sentinel with no "DATEPART("
  // substring exposed for this regex to find; a GENUINE DATEPART(...) call's
  // own first argument is ALSO just a masked literal like any other, and
  // resolves correctly through `lits`.
  f = f.replace(new RegExp(`\\bDATEPART\\s*\\(\\s*${_TABLEAU_SENTINEL_SRC}\\s*,\\s*([^)]+)\\)`, 'gi'), (m, litIdx, dateArg) => {
    const part = _tabLitInner(lits, litIdx);
    if (!/^\w+$/.test(part)) return m; // not a clean unit token — leave unconverted (matches the old '(\w+)'-anchored regex)
    // 'week' has no dedicated Sigma fn — use DatePart("week", …) (see WEEK below).
    if (part.toLowerCase() === 'week') return 'DatePart("week", ' + dateArg.trim() + ')';
    const partMap: Record<string, string> = {
      year: 'Year', month: 'Month', day: 'Day', hour: 'Hour', minute: 'Minute',
      second: 'Second', quarter: 'Quarter', dayofweek: 'DayOfWeek', weekday: 'DayOfWeek'
    };
    const fn = partMap[part.toLowerCase()];
    return fn ? fn + '(' + dateArg.trim() + ')' : m;
  });
  // DATENAME('month', [Date]) → MonthName([Date]); 'weekday' → WeekdayName([Date]);
  // numeric units (year/quarter/day) have no name fn in Sigma → Text(<numeric part>).
  f = f.replace(new RegExp(`\\bDATENAME\\s*\\(\\s*${_TABLEAU_SENTINEL_SRC}\\s*,\\s*([^,)]+)(?:,[^)]*)?\\)`, 'gi'), (m, litIdx, dateArg) => {
    const part = _tabLitInner(lits, litIdx);
    if (!/^\w+$/.test(part)) return m;
    const arg = dateArg.trim();
    switch (part.toLowerCase()) {
      case 'month':                  return 'MonthName(' + arg + ')';
      case 'weekday': case 'dayofweek': return 'WeekdayName(' + arg + ')';
      case 'year':    return 'Text(Year(' + arg + '))';
      case 'quarter': return 'Text(Quarter(' + arg + '))';
      case 'day':     return 'Text(Day(' + arg + '))';
      case 'week':    return 'Text(Week(' + arg + '))';
      case 'hour':    return 'Text(Hour(' + arg + '))';
      case 'minute':  return 'Text(Minute(' + arg + '))';
      case 'second':  return 'Text(Second(' + arg + '))';
      default:        return m;
    }
  });
  f = f.replace(new RegExp(`\\bDATETRUNC\\s*\\(\\s*${_TABLEAU_SENTINEL_SRC}\\s*,`, 'gi'),
    (_m, litIdx) => `DateTrunc("${_tabEscapeForSigma(_tabLitInner(lits, litIdx))}",`);
  // Tableau DATETRUNC('week', date, 'monday') carries a start-of-week 3rd arg that
  // Sigma's DateTrunc (unit, date) has no slot for — strip the weekday literal.
  f = f.replace(new RegExp(`,\\s*${_TABLEAU_SENTINEL_SRC}\\s*\\)`, 'gi'), (m, litIdx) => {
    const val = _tabLitInner(lits, litIdx);
    return /^(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)$/i.test(val) ? ')' : m;
  });
  f = f.replace(new RegExp(`\\bDATEADD\\s*\\(\\s*${_TABLEAU_SENTINEL_SRC}\\s*,`, 'gi'),
    (_m, litIdx) => `DateAdd("${_tabEscapeForSigma(_tabLitInner(lits, litIdx))}",`);
  f = f.replace(new RegExp(`\\bDATEDIFF\\s*\\(\\s*${_TABLEAU_SENTINEL_SRC}\\s*,`, 'gi'),
    (_m, litIdx) => `DateDiff("${_tabEscapeForSigma(_tabLitInner(lits, litIdx))}",`);
  // Tableau WEEK(date) = week-of-year number. Sigma has NO Week() function
  // (live query returned "Unknown function: Week", 2026-07-10) — the week number
  // comes from DatePart("week", date). Handle one level of nested parens so
  // WEEK(MakeDate(...)) / WEEK([Date]) both rewrite cleanly. No quotes involved
  // — safe on masked text unchanged.
  f = f.replace(/\bWEEK\s*\(\s*([^()]*(?:\([^()]*\)[^()]*)*)\)/gi, 'DatePart("week", $1)');

  // STDEVP (population std dev) — Sigma has no population-stddev function;
  // population σ = Sqrt(population variance). Run before the STDEV map entry.
  // No quotes involved — safe on masked text unchanged.
  f = f.replace(/\bSTDEVP\s*\(([^()]+(?:\([^()]*\)[^()]*)*)\)/gi, 'Sqrt(VariancePop($1))');
  // DATEPARSE('format', string) — Tableau orders args (format, string) and uses
  // Java date tokens; Sigma DateParse(text, format) reverses them and uses strftime.
  f = f.replace(new RegExp(`\\bDATEPARSE\\s*\\(\\s*${_TABLEAU_SENTINEL_SRC}\\s*,\\s*([^()]+(?:\\([^()]*\\)[^()]*)*)\\)`, 'gi'),
    (_m, litIdx, str) => {
      const fmtRaw = _tabLitInner(lits, litIdx);
      const sf = fmtRaw
        .replace(/yyyy/g, '%Y').replace(/yy/g, '%y')
        .replace(/MMMM/g, '%B').replace(/MMM/g, '%b').replace(/MM/g, '%m')
        .replace(/dd/g, '%d').replace(/HH/g, '%H').replace(/hh/g, '%I')
        .replace(/mm/g, '%M').replace(/ss/g, '%S');
      if (warnings) warnings.push('⚠ DATEPARSE format translated to strftime tokens — verify the pattern resolves on your warehouse.');
      return `DateParse(${str.trim()}, "${sf}")`;
    });

  // User-context (row-level security) functions → Sigma equivalents.
  // USERNAME()→CurrentUserEmail(); ISMEMBEROF('g')→CurrentUserInTeam("g");
  // USERATTRIBUTE('a')→CurrentUserAttributeText("a"); ISUSERNAME('u')→email match.
  // Sentinel-aware for the same reason as the DATEPART family above.
  f = f.replace(/\bUSERNAME\s*\(\s*\)/gi, 'CurrentUserEmail()');
  f = f.replace(new RegExp(`\\bISMEMBEROF\\s*\\(\\s*${_TABLEAU_SENTINEL_SRC}\\s*\\)`, 'gi'),
    (_m, litIdx) => `CurrentUserInTeam("${_tabEscapeForSigma(_tabLitInner(lits, litIdx))}")`);
  f = f.replace(new RegExp(`\\bUSERATTRIBUTE\\s*\\(\\s*${_TABLEAU_SENTINEL_SRC}\\s*\\)`, 'gi'),
    (_m, litIdx) => `CurrentUserAttributeText("${_tabEscapeForSigma(_tabLitInner(lits, litIdx))}")`);
  f = f.replace(new RegExp(`\\bISUSERNAME\\s*\\(\\s*${_TABLEAU_SENTINEL_SRC}\\s*\\)`, 'gi'),
    (_m, litIdx) => `(CurrentUserEmail() = "${_tabEscapeForSigma(_tabLitInner(lits, litIdx))}")`);

  // Arg-rewrite mappings — Sigma has no direct equivalent, but a trivial rewrite
  // resolves live (bead tt3z.3, verified 2026-07-10):
  //   SQUARE(x) → Power(x, 2)      (no Sigma Square)
  //   SPACE(n)  → Repeat(" ", n)   (no Sigma Space; Repeat resolves)
  // One level of nested parens in the arg is handled. No quotes involved.
  f = f.replace(/\bSQUARE\s*\(\s*([^()]*(?:\([^()]*\)[^()]*)*)\)/gi, 'Power($1, 2)');
  f = f.replace(/\bSPACE\s*\(\s*([^()]*(?:\([^()]*\)[^()]*)*)\)/gi, 'Repeat(" ", $1)');

  // Map remaining functions — safe on masked text: a literal containing any
  // of these names (ZN/ROUND/AVG/…) is already one opaque sentinel by now.
  for (const [tab, sig] of Object.entries(TABLEAU_FUNC_MAP)) {
    f = f.replace(new RegExp('\\b' + tab + '\\s*\\(', 'gi'), sig + '(');
  }

  // Keyword-casing — safe on masked text: a literal containing the plain
  // English words "true"/"false"/"null"/"and"/"or" is already one opaque
  // sentinel and is never re-cased.
  f = f.replace(/\bNOT\b/g, 'Not').replace(/\bAND\b/g, 'and').replace(/\bOR\b/g, 'or');
  f = f.replace(/\bTRUE\b/gi, 'True').replace(/\bFALSE\b/gi, 'False').replace(/\bNULL\b/gi, 'null');

  // Convert physical column name references to display names — safe on
  // masked text: an ALL-CAPS bracket-look-alike (e.g. [SALES]) inside a
  // literal is hidden inside its sentinel, so only genuine field refs match.
  f = f.replace(/\[([A-Z][A-Z0-9_]{2,})\]/g, (match, colName) => {
    if (colName === colName.toLowerCase() || colName.includes(' ')) return match;
    return '[' + sigmaDisplayName(colName) + ']';
  });

  // Finalize: resolve every remaining sentinel to Sigma's double-quoted form.
  // This REPLACES the old `f.replace(/'([^']*)'/g, '"$1"')` single-quote
  // collapse, which never touched double-quoted Tableau literals at all.
  f = _unmaskTableauLiterals(f, lits);

  // Tableau text concat `+` → Sigma `&` (literal / text-function operands only;
  // the converter re-runs this with column-type info for ref-only chains).
  f = tableauTextConcatToSigma(f);

  // Loud (never silent) flag for table-calc tokens embedded inside larger
  // expressions that the anchored mapper could not claim — the leftover token
  // would otherwise pass through as a silently-broken formula.
  if (warnings && TABLEAU_TABLE_CALC_TOKEN_RE.test(f)) {
    warnings.push(`⚠ Table-calc function embedded in a larger expression — NOT translated in place. Untranslated fragment: ${f.slice(0, 120)}`);
  }

  // B1 catch-all — never let an unmapped Tableau function pass through silently.
  // Every function we DO emit is PascalCase (Sum, DateTrunc, CurrentUserEmail…),
  // so any residual ALL-CAPS `FUNC(` token is an unmapped Tableau function that
  // would otherwise reach Sigma verbatim and error only at query time (the
  // "agent falls off the rails" failure — e.g. FINDNTH, CHAR, MAKEDATETIME,
  // MODEL_QUANTILE, the trig family). Mask string literals and column refs first
  // so identifiers inside them aren't flagged; table-calc tokens are already
  // reported just above, so skip them here to avoid duplicate warnings.
  if (warnings) {
    const masked2 = f.replace(/"[^"]*"/g, '""').replace(/\[[^\]]*\]/g, '[]');
    const unmapped = new Set<string>();
    const scan = /\b([A-Z][A-Z0-9_]+)\s*\(/g;
    let mm: RegExpExecArray | null;
    while ((mm = scan.exec(masked2)) !== null) {
      const fn = mm[1];
      if (TABLEAU_TABLE_CALC_TOKEN_RE.test(fn + '(')) continue; // already flagged above
      unmapped.add(fn);
    }
    if (unmapped.size) {
      warnings.push(
        `⚠ Unmapped Tableau function(s) passed through unconverted: ${[...unmapped].join(', ')} — `
        + `no validated Sigma equivalent yet. Rewrite manually; left as-is they error at query time.`);
    }
  }

  return f.trim();
}


/** Check if a Tableau formula contains aggregate functions */
export function tableauIsAggregate(formula: string): boolean {
  return /\b(SUM|AVG|COUNT|COUNTD|MAX|MIN|MEDIAN|STDEV|STDEVP|VAR|VARP|PERCENTILE|CORR|ATTR)\s*\(/i.test(formula);
}

/** A Tableau calc is row-level security if it tests the viewer's identity/membership. */
export function tableauFormulaIsRls(formula: string): boolean {
  return /\b(USERNAME|FULLNAME|USERDOMAIN|ISMEMBEROF|ISUSERNAME|USERATTRIBUTE)\s*\(/i.test(formula || '');
}

/** Strip ${TABLE}. and extract leading identifier from sql: */
export function lookStripSql(sql: string): string {
  if (!sql) return '';
  sql = sql.replace(/\$\{TABLE\}\./gi, '').trim();
  sql = sql.replace(/\$\{[^.}]+\.([^}]+)\}/g, '$1');
  // Strip backtick identifiers (BigQuery/MySQL style)
  sql = sql.replace(/`/g, '');
  // Strip bracket identifiers: [Column Name] → Column Name
  sql = sql.replace(/\[([A-Za-z_][A-Za-z0-9_\s]*)\]/g, '$1');
  // Strip PostgreSQL :: casts: col::INTEGER → col
  sql = sql.replace(/::\w[\w_]*/g, '');
  // Unwrap SAFE_CAST/TRY_CAST/CAST(col AS type) → col
  const castMatch = sql.match(/^(?:SAFE_CAST|TRY_CAST|CAST)\s*\(\s*("?[A-Za-z_][A-Za-z0-9_]*"?)\s+AS\s+\w[\w_]*\s*\)$/i);
  if (castMatch) sql = castMatch[1];
  sql = sql.replace(/"/g, '').trim(); // strip Snowflake double-quote identifiers e.g. "COLUMN_NAME"
  const m = sql.match(/^([A-Za-z_][A-Za-z0-9_]*)/);
  return m ? m[1] : sql;
}

/** Map LookML type to Sigma type */
export function lookSigmaType(lkType: string): string {
  const map: Record<string, string> = {
    string: 'text', number: 'number', yesno: 'boolean',
    date: 'datetime', time: 'datetime', datetime: 'datetime',
    zipcode: 'text', tier: 'text', location: 'text',
    distance: 'number', duration: 'number', count: 'number'
  };
  return map[(lkType || '').toLowerCase()] || 'text';
}

/** Map LookML measure type to Sigma formula */
export function lookSigmaMetric(measureType: string, colName: string): string {
  const dn = sigmaDisplayName(colName);
  const map: Record<string, string> = {
    sum: `Sum([${dn}])`,
    count: `CountIf(IsNotNull([${dn}]))`,
    count_distinct: `CountDistinct([${dn}])`,
    average: `Avg([${dn}])`,
    max: `Max([${dn}])`,
    min: `Min([${dn}])`,
    list: `ListAgg([${dn}])`,
    sum_distinct: `Sum(Distinct [${dn}])`,
    average_distinct: `Avg(Distinct [${dn}])`,
    median: `Median([${dn}])`,
    number: `[${dn}]`,
    yesno: `CountIf([${dn}])`,
  };
  return map[(measureType || '').toLowerCase()] || `CountIf(IsNotNull([${dn}]))`;
}
