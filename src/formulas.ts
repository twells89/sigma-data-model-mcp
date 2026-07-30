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
 * closing `)`, and silently leave the outer parens in place (review finding on jva2).
 *
 * Domo wraps every Beast Mode in outer parens, which made lookSqlToSigmaRules'
 * anchored patterns (`/^CASE\b/i`, `/^ROUND\s*\(/i`, …) unreachable — measured: 0 of
 * 74 live Beast Modes matched any rule before this (bead jva2).
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
  const body = head[2];
  const cases: { when: string; then: string }[] = [];
  // `when <quoted-literal> then <result up to next when/else/end-of-body>`
  const pairRe = /\bwhen\s+("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')\s+then\s+([\s\S]*?)(?=\s*\bwhen\b|\s*\belse\b|$)/gi;
  let m: RegExpExecArray | null;
  while ((m = pairRe.exec(body))) {
    const whenVal = m[1].slice(1, -1).replace(/\\(.)/g, '$1');   // strip quotes + unescape
    const thenSig = tableauFormulaToSigma(m[2].trim(), warnings);
    cases.push({ when: whenVal, then: thenSig });
  }
  if (!cases.length) return null;
  const elseM = body.match(/\belse\s+([\s\S]*?)$/i);
  const elseExpr = elseM ? tableauFormulaToSigma(elseM[1].trim(), warnings) : null;
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

/** Convert CASE WHEN ... THEN ... ELSE ... END to nested If() */
export function lookConvertCase(expr: string): string | null {
  const body = expr.replace(/^CASE\s*/i, '').replace(/\s*END\s*$/i, '').trim();
  const branches: { cond: string; val: string }[] = [];

  // Split on WHEN keyword boundaries — each part is "cond THEN val [ELSE elseVal]"
  const parts = body.split(/\bWHEN\b/i).filter(Boolean);
  let elseVal: string | null = null;

  for (const part of parts) {
    // Match: everything up to THEN (non-greedy), then THEN val, then optionally ELSE elseVal
    const elseMatch = part.match(/^([\s\S]+?)\s+THEN\s+([\s\S]+?)(?:\s+ELSE\s+([\s\S]+))?$/i);
    if (!elseMatch) {
      // Try to extract a bare ELSE from this part
      const e = part.match(/\bELSE\s+([\s\S]+)$/i);
      if (e && !elseVal) elseVal = e[1].trim();
      continue;
    }
    const cond = elseMatch[1].trim();
    let val = elseMatch[2].trim();
    // Check if val itself contains an ELSE clause
    const elseInVal = val.match(/^([\s\S]+?)\s+ELSE\s+([\s\S]+)$/i);
    if (elseInVal) {
      val = elseInVal[1].trim();
      if (!elseVal) elseVal = elseInVal[2].trim();
    }
    branches.push({ cond, val });
  }

  // Also check for top-level ELSE (e.g. "ELSE 'other'" at end of body)
  const topElse = body.match(/\bELSE\s+([\s\S]+)$/i);
  if (topElse && !elseVal) elseVal = topElse[1].trim();

  if (branches.length === 0) return null;

  const convertVal = (v: string): string => {
    v = v.trim();
    // NOTE: string literals are deliberately NOT special-cased here (no longer
    // `return v` raw) — lookConvertExpression now masks/unmasks literals itself,
    // emitting Sigma's required double-quoted form ("West", not 'West'). A
    // literal short-circuit here would silently re-introduce single-quoted
    // SQL-style output for every CASE-THEN/ELSE string value (A6).
    if (/^-?\d+(\.\d+)?$/.test(v)) return v;  // number literal
    // Strip a whole-value paren wrapper HERE, at the point val/elseVal is
    // handed to lookConvertExpression — not inside lookConvertExpression
    // itself. A THEN/ELSE branch value can carry its own local wrap (e.g.
    // `ELSE (SUM(x) / COUNT(DISTINCT y))`), and without stripping it that wrap
    // leaks into the converted output (`(Sum([X]) / CountDistinct([Y]))`
    // instead of `Sum([X]) / CountDistinct([Y])`) — sqp1 round-2 review:
    // lookConvertExpression is a SHARED contract (lookml.ts, tools.ts, plus
    // lookConvertMathExpr/_unmaskCountDistinct in this file); an operator-level
    // caller that splices its result into a larger expression could
    // re-associate wrongly if lookConvertExpression silently stripped a wrap
    // it didn't put there. Confining the strip to this CASE-specific call site
    // gets the identical behavior for the bug being fixed without touching
    // that shared contract at all.
    return lookConvertExpression(stripOuterParens(v));
  };

  let result = elseVal ? convertVal(elseVal) : 'null';
  for (let i = branches.length - 1; i >= 0; i--) {
    // Same reasoning as convertVal above — cond can carry its own local wrap
    // (e.g. `WHEN (COUNT(DISTINCT [Id]) = 0)`) that must not leak through.
    const sigmaCond = lookConvertExpression(stripOuterParens(branches[i].cond));
    const sigmaVal  = convertVal(branches[i].val);
    result = `If(${sigmaCond}, ${sigmaVal}, ${result})`;
  }
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
      // corruption this function exists to prevent (round 1 review finding).
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

function _unmaskCountDistinct(s: string, args: string[]): string {
  return s.replace(/(\d+)/g, (_m, i) => {
    const raw = stripOuterParens(args[Number(i)]);
    return `CountDistinct(${lookSqlToSigmaRules(raw) ?? lookConvertExpression(raw)})`;
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
// so this regex never matches it in that shape regardless of list membership (see
// task-3 report for the full trace).
//
// SHARED between pass 1 (name-before-paren callable check) and pass 3 (bare
// ALL_CAPS identifier bracketing) below — a second, independently-maintained
// keyword list is exactly how AS/ON/BY/DISTINCT drifted out of pass 3 the first
// time (round 1 review finding): pass 3 had its own inline list missing them,
// so `A AS B` bracketed AS into a bogus `[As]` column and `GROUP_COL BY OTHER`
// did the same to BY. One constant, used by both, closes that off structurally.
const _SQL_KEYWORD_RE = /^(?:AND|OR|NOT|IN|IS|NULL|CASE|WHEN|THEN|ELSE|END|BETWEEN|LIKE|AS|ON|BY|DISTINCT|TRUE|FALSE|OVER|GROUP|EXISTS)$/i;

/** Convert an entire expression: map functions, convert column refs, fix IN lists */
export function lookConvertExpression(expr: string): string {
  const cd = _maskCountDistinct(expr);
  const { masked, lits } = _maskLiterals(cd.masked);
  expr = masked;

  // 1. Map SQL function names to Sigma equivalents
  expr = expr.replace(/\b([A-Z_][A-Z0-9_]*)\s*(?=\()/gi, (match, fn) => {
    const upper = fn.toUpperCase();
    if (_SQL_KEYWORD_RE.test(upper)) return match;              // keyword, not a call
    const mapped = LOOK_FUNC_MAP[upper];
    // A map value may already carry its own parens (CURRENT_DATE -> 'Today()'). Only
    // the NAME is being substituted here; the source's own '()' follows, so keeping
    // the mapped parens yields 'Today()()'.
    if (mapped) return mapped.endsWith('()') ? mapped.slice(0, -2) : mapped;
    return fn.charAt(0).toUpperCase() + fn.slice(1).toLowerCase();
  });

  // 2. Convert EXPR IN (a, b, c) → In(EXPR, a, b, c)
  // LHS can be a bracket-form [Display Name] or a word/call expression
  expr = expr.replace(/(\[[^\]]+\]|[\w\]\)]+(?:\([^)]*\))?)\s+IN\s*\(([^)]+)\)/gi, (_, lhs, list) => {
    return `In(${lhs}, ${list})`;
  });

  // 3. Convert bare ALL_CAPS identifiers (not followed by '(') to [Display Name]
  expr = expr.replace(/\b([A-Z_][A-Z0-9_]*)\b(?!\s*\()/g, (match) => {
    if (_SQL_KEYWORD_RE.test(match)) return match;              // shared with pass 1 — see comment above
    if (/^\d+$/.test(match)) return match;
    return lookColRef(match);
  });

  return _unmaskCountDistinct(_unmaskLiterals(expr, lits), cd.args).trim();
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
  // A `then`/`end`/`when` keyword surviving translation means tableauCaseToSigma
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

function tableauIfToSigma(f: string): string {
  return f.replace(/\bIF\b([\s\S]+?)\bEND\b/gi, (match) => {
    let inner = match.replace(/^\s*IF\s*/i, '').replace(/\s*END\s*$/i, '');
    const elseIdx = inner.search(/\bELSE\b(?!\s*IF\b)/i);
    let elseVal = 'null';
    if (elseIdx >= 0) {
      elseVal = tableauFormulaToSigma(inner.slice(elseIdx).replace(/^\s*ELSE\s*/i, '').trim());
      inner = inner.slice(0, elseIdx);
    }
    const parts = inner.split(/\bELSEIF\b/i);
    let result = elseVal;
    for (let i = parts.length - 1; i >= 0; i--) {
      const thenParts = parts[i].split(/\bTHEN\b/i);
      if (thenParts.length < 2) continue;
      const cond = tableauFormulaToSigma(thenParts[0].trim());
      const val = tableauFormulaToSigma(thenParts[1].trim());
      result = 'If(' + cond + ', ' + val + ', ' + result + ')';
    }
    return result;
  });
}

function tableauCaseToSigma(f: string): string {
  return f.replace(/\bCASE\b([\s\S]+?)\bEND\b/gi, (match, body) => {
    const elseIdx = body.search(/\bELSE\b/i);
    let elseVal = 'null';
    let whenBody = body;
    if (elseIdx >= 0) {
      elseVal = tableauFormulaToSigma(body.slice(elseIdx).replace(/^\s*ELSE\s*/i, '').trim());
      whenBody = body.slice(0, elseIdx);
    }
    const fieldMatch = whenBody.match(/^([\s\S]*?)\bWHEN\b/i);
    const field = fieldMatch ? tableauFormulaToSigma(fieldMatch[1].trim()) : '[?]';
    const pairs = whenBody.replace(/^[\s\S]*?\bWHEN\b/i, '').split(/\bWHEN\b/i).filter(Boolean);
    let result = elseVal;
    for (let i = pairs.length - 1; i >= 0; i--) {
      const thenParts = pairs[i].split(/\bTHEN\b/i);
      if (thenParts.length < 2) continue;
      result = 'If(' + field + ' = ' + tableauFormulaToSigma(thenParts[0].trim()) + ', ' + tableauFormulaToSigma(thenParts[1].trim()) + ', ' + result + ')';
    }
    return result;
  });
}

/** Convert a Tableau calculated field formula to Sigma formula syntax */
export function tableauFormulaToSigma(formula: string, warnings?: string[]): string {
  if (!formula || !formula.trim()) return '';
  // Decode numeric XML entities (fxp leaves &#10; literal) and strip //comments
  // BEFORE any pattern matching, so the rest of the translator sees clean text.
  let f = stripLineComments(decodeXmlEntities(formula)).trim();

  // LOD expressions
  if (/^\s*\{/.test(f)) {
    if (warnings) warnings.push('⚠ LOD expression not converted: ' + f.slice(0, 60));
    return '/* LOD: ' + f.replace(/\/\*/g, '').replace(/\*\//g, '') + ' */';
  }
  // Table calcs — WINPROBE-validated mappings to Sigma window functions.
  // CONTEXT CAVEAT: the emitted Sigma window functions are valid in CHART /
  // grouped-workbook-element context ONLY — they silently error in DM element
  // calc columns and workbook master calc columns. Never emit *Over functions
  // (SumOver/MaxOver/CountOver = 'Unknown function' in spec contexts).
  {
    const winChart = tableauWindowToSigmaChart(f);
    if (winChart) {
      if (warnings) warnings.push(
        `ℹ Table calc → ${winChart.formula} — CHART/grouped-element context ONLY: place in a grouped workbook element (group by the viz dimensions); window functions silently error in data-model calc columns and workbook master calc columns.`
        + (winChart.note ? ' ' + winChart.note : ''));
      return winChart.formula;
    }
    const untrans = tableauWindowUntranslatable(f);
    if (untrans) {
      if (warnings) warnings.push(`⚠ Table calculation NOT converted — ${untrans}() has no Sigma equivalent. Untranslated fragment: ${f.slice(0, 120)}`);
      return '/* table calc: ' + f.replace(/\/\*/g, '').replace(/\*\//g, '') + ' */';
    }
    if (/^(WINDOW_|RUNNING_|FIRST\(|LAST\(|INDEX\(|RANK\b|RANK_|LOOKUP\(|TOTAL\s*\()/i.test(f)) {
      // WINDOW_SUM(AGG([x])) with no offsets → GrandTotal(Agg([x])) — DM-safe
      // (exact when the chart groups by a single dimension set).
      const gt = f.match(/^WINDOW_SUM\s*\(\s*(SUM|COUNT|AVG|MIN|MAX)\s*\(\s*(\[[^\]]+\])\s*\)\s*\)$/i);
      if (gt) {
        const aggMap: Record<string, string> = { SUM: 'Sum', COUNT: 'Count', AVG: 'Avg', MIN: 'Min', MAX: 'Max' };
        return 'GrandTotal(' + (aggMap[gt[1].toUpperCase()] || gt[1]) + '(' + gt[2] + '))';
      }
      // Anchored table calc we couldn't map — flag loudly, never emit silently.
      if (warnings) warnings.push(`⚠ Table calculation not converted. Untranslated fragment: ${f.slice(0, 120)}`);
      return '/* table calc: ' + f.replace(/\/\*/g, '').replace(/\*\//g, '') + ' */';
    }
  }

  // COVAR/COVARP have no Sigma equivalent — flag loudly, never emit silently
  // (Sigma has Corr but no covariance function; verified 2026-06-15).
  if (/\bCOVARP?\s*\(/i.test(f)) {
    if (warnings) warnings.push(`⚠ COVAR/COVARP has no Sigma equivalent — not converted. Fragment: ${f.slice(0, 120)}`);
    return '/* no Sigma equivalent: ' + f.replace(/\/\*/g, '').replace(/\*\//g, '') + ' */';
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
  f = tableauInToSigma(f);

  f = tableauIfToSigma(f);
  f = f.replace(/\bIIF\s*\(/gi, 'If(');
  f = tableauCaseToSigma(f);

  // DATEPART('year', [Date]) → Year([Date])
  f = f.replace(/\bDATEPART\s*\(\s*'(\w+)'\s*,\s*([^)]+)\)/gi, (m, part, dateArg) => {
    // 'week' has no dedicated Sigma fn — use DatePart("week", …) (see WEEK above).
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
  f = f.replace(/\bDATENAME\s*\(\s*'(\w+)'\s*,\s*([^,)]+)(?:,[^)]*)?\)/gi, (m, part, dateArg) => {
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
  f = f.replace(/\bDATETRUNC\s*\(\s*'([^']+)'\s*,/gi, 'DateTrunc("$1",');
  // Tableau DATETRUNC('week', date, 'monday') carries a start-of-week 3rd arg that
  // Sigma's DateTrunc (unit, date) has no slot for — strip the weekday literal.
  f = f.replace(/,\s*["'](?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)["']\s*\)/gi, ')');
  f = f.replace(/\bDATEADD\s*\(\s*'([^']+)'\s*,/gi, 'DateAdd("$1",');
  f = f.replace(/\bDATEDIFF\s*\(\s*'([^']+)'\s*,/gi, 'DateDiff("$1",');
  // Tableau WEEK(date) = week-of-year number. Sigma has NO Week() function
  // (live query returned "Unknown function: Week", 2026-07-10) — the week number
  // comes from DatePart("week", date). Handle one level of nested parens so
  // WEEK(MakeDate(...)) / WEEK([Date]) both rewrite cleanly.
  f = f.replace(/\bWEEK\s*\(\s*([^()]*(?:\([^()]*\)[^()]*)*)\)/gi, 'DatePart("week", $1)');

  // STDEVP (population std dev) — Sigma has no population-stddev function;
  // population σ = Sqrt(population variance). Run before the STDEV map entry.
  f = f.replace(/\bSTDEVP\s*\(([^()]+(?:\([^()]*\)[^()]*)*)\)/gi, 'Sqrt(VariancePop($1))');
  // DATEPARSE('format', string) — Tableau orders args (format, string) and uses
  // Java date tokens; Sigma DateParse(text, format) reverses them and uses strftime.
  f = f.replace(/\bDATEPARSE\s*\(\s*('[^']*'|"[^"]*")\s*,\s*([^()]+(?:\([^()]*\)[^()]*)*)\)/gi,
    (_m, fmt, str) => {
      const sf = fmt.slice(1, -1)
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
  // (Run before the single→double quote pass so the arg quoting is normalized here.)
  f = f.replace(/\bUSERNAME\s*\(\s*\)/gi, 'CurrentUserEmail()');
  f = f.replace(/\bISMEMBEROF\s*\(\s*['"]([^'"]+)['"]\s*\)/gi, 'CurrentUserInTeam("$1")');
  f = f.replace(/\bUSERATTRIBUTE\s*\(\s*['"]([^'"]+)['"]\s*\)/gi, 'CurrentUserAttributeText("$1")');
  f = f.replace(/\bISUSERNAME\s*\(\s*['"]([^'"]+)['"]\s*\)/gi, '(CurrentUserEmail() = "$1")');

  // Arg-rewrite mappings — Sigma has no direct equivalent, but a trivial rewrite
  // resolves live (bead tt3z.3, verified 2026-07-10):
  //   SQUARE(x) → Power(x, 2)      (no Sigma Square)
  //   SPACE(n)  → Repeat(" ", n)   (no Sigma Space; Repeat resolves)
  // One level of nested parens in the arg is handled.
  f = f.replace(/\bSQUARE\s*\(\s*([^()]*(?:\([^()]*\)[^()]*)*)\)/gi, 'Power($1, 2)');
  f = f.replace(/\bSPACE\s*\(\s*([^()]*(?:\([^()]*\)[^()]*)*)\)/gi, 'Repeat(" ", $1)');

  // Map remaining functions
  for (const [tab, sig] of Object.entries(TABLEAU_FUNC_MAP)) {
    f = f.replace(new RegExp('\\b' + tab + '\\s*\\(', 'gi'), sig + '(');
  }

  // Single-quote strings → double-quote
  f = f.replace(/'([^']*)'/g, '"$1"');
  f = f.replace(/\bNOT\b/g, 'Not').replace(/\bAND\b/g, 'and').replace(/\bOR\b/g, 'or');
  f = f.replace(/\bTRUE\b/gi, 'True').replace(/\bFALSE\b/gi, 'False').replace(/\bNULL\b/gi, 'null');

  // Convert physical column name references to display names
  f = f.replace(/\[([A-Z][A-Z0-9_]{2,})\]/g, (match, colName) => {
    if (colName === colName.toLowerCase() || colName.includes(' ')) return match;
    return '[' + sigmaDisplayName(colName) + ']';
  });

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
    const masked = f.replace(/"[^"]*"/g, '""').replace(/\[[^\]]*\]/g, '[]');
    const unmapped = new Set<string>();
    const scan = /\b([A-Z][A-Z0-9_]+)\s*\(/g;
    let mm: RegExpExecArray | null;
    while ((mm = scan.exec(masked)) !== null) {
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
