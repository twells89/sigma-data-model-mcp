/**
 * SQL → Sigma formula conversion utilities.
 * Used by LookML, Snowflake, dbt, and Tableau converters.
 */

import { sigmaDisplayName } from './sigma-ids.js';

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
  // CAST(col AS type) wrapping a simple column ref is not complex — just a type hint
  if (/^CAST\s*\(\s*"?[A-Za-z_][A-Za-z0-9_]*"?\s+AS\s+\w[\w_]*\s*\)$/i.test(cleaned)) return false;
  if (/^[A-Za-z_][A-Za-z0-9_]*\s*\(/.test(cleaned)) return true;
  if (/^CASE\b/i.test(cleaned)) return true;
  if (/[=<>!+\-*\/]/.test(cleaned.replace(/'[^']*'/g, ''))) return true;
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
  const branchRe = /WHEN\s+(.+?)\s+THEN\s+('(?:[^'\\]|\\.)*'|\S+)/gi;
  let bm;
  while ((bm = branchRe.exec(body)) !== null) {
    const cond = lookConvertExpression(bm[1].trim());
    const val = bm[2].trim();
    branches.push({ cond, val });
  }
  const elseMatch = body.match(/ELSE\s+('(?:[^'\\]|\\.)*'|\S+)\s*$/i);
  const elseVal = elseMatch ? elseMatch[1] : 'null';

  if (branches.length === 0) return null;
  let result = elseVal;
  for (let i = branches.length - 1; i >= 0; i--) {
    result = `If(${branches[i].cond}, ${branches[i].val}, ${result})`;
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

/** Convert an entire expression: map functions, convert column refs, fix IN lists */
export function lookConvertExpression(expr: string): string {
  // 1. Map SQL function names to Sigma equivalents
  expr = expr.replace(/\b([A-Z_][A-Z0-9_]*)\s*(?=\()/gi, (match, fn) => {
    const upper = fn.toUpperCase();
    return LOOK_FUNC_MAP[upper] || (fn.charAt(0).toUpperCase() + fn.slice(1).toLowerCase());
  });

  // 2. Convert EXPR IN (a, b, c) → In(EXPR, a, b, c)
  expr = expr.replace(/([\w\]\)]+(?:\([^)]*\))?)\s+IN\s*\(([^)]+)\)/gi, (_, lhs, list) => {
    return `In(${lhs}, ${list})`;
  });

  // 3. Convert bare ALL_CAPS identifiers (not followed by '(') to [Display Name]
  expr = expr.replace(/\b([A-Z_][A-Z0-9_]*)\b(?!\s*\()/g, (match) => {
    if (/^(AND|OR|NOT|NULL|IS|IN|BETWEEN|LIKE|THEN|ELSE|END|WHEN|CASE|TRUE|FALSE)$/i.test(match)) return match;
    if (/^\d+$/.test(match)) return match;
    return lookColRef(match);
  });

  return expr.trim();
}

/**
 * Rule-based SQL → Sigma formula converter for common patterns.
 * Returns a Sigma formula string, or null if the pattern isn't recognised.
 */
export function lookSqlToSigmaRules(sql: string): string | null {
  let expr = sql
    .replace(/\$\{TABLE\}\./gi, '')
    .replace(/\$\{[^.}]+\.([^}]+)\}/g, '$1')
    .trim();

  // Pattern 1: COLUMN = 1 (yesno boolean flag)
  {
    const m = expr.match(/^([A-Z_][A-Z0-9_]*)\s*=\s*(\d+)$/i);
    if (m) return `${lookColRef(m[1])} = ${m[2]}`;
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

  return null;
}

// ── Tableau formula conversion ────────────────────────────────────────────────

const TABLEAU_FUNC_MAP: Record<string, string> = {
  'AVG': 'Avg', 'MAX': 'Max', 'MIN': 'Min', 'MEDIAN': 'Median',
  'SUM': 'Sum', 'ABS': 'Abs', 'CEILING': 'Ceiling', 'FLOOR': 'Floor',
  'ROUND': 'Round', 'SQRT': 'Sqrt', 'POWER': 'Power',
  'STR': 'Text', 'INT': 'Int', 'FLOAT': 'Number',
  'LEN': 'Len', 'UPPER': 'Upper', 'LOWER': 'Lower',
  'TRIM': 'Trim', 'LTRIM': 'Ltrim', 'RTRIM': 'Rtrim',
  'LEFT': 'Left', 'RIGHT': 'Right', 'MID': 'Mid',
  'REPLACE': 'Replace', 'CONTAINS': 'Contains',
  'STARTSWITH': 'StartsWith', 'ENDSWITH': 'EndsWith', 'FIND': 'Find',
  'TODAY': 'Today', 'NOW': 'Now',
  'YEAR': 'Year', 'MONTH': 'Month', 'DAY': 'Day',
  'HOUR': 'Hour', 'MINUTE': 'Minute', 'SECOND': 'Second',
  'WEEK': 'Week', 'QUARTER': 'Quarter',
  'DATE': 'Date', 'DATETIME': 'Datetime',
};

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
  let f = formula.trim();

  // LOD expressions
  if (/^\s*\{/.test(f)) {
    if (warnings) warnings.push('⚠ LOD expression not converted: ' + f.slice(0, 60));
    return '/* LOD: ' + f.replace(/\/\*/g, '').replace(/\*\//g, '') + ' */';
  }
  // Table calcs — convert common patterns to Sigma window functions
  if (/^(WINDOW_|RUNNING_|FIRST\(|LAST\(|INDEX\(|RANK\b|RANK_|LOOKUP\(|PREVIOUS_VALUE\()/i.test(f)) {
    // RUNNING_SUM(SUM([x])) or RUNNING_SUM([x]) → CumulativeSum([x])
    let tcMatch = f.match(/^RUNNING_SUM\s*\(\s*(?:SUM\s*\(\s*)?(\[[^\]]+\])\s*\)?\s*\)/i);
    if (tcMatch) return 'CumulativeSum(' + tcMatch[1] + ')';

    // RUNNING_AVG(SUM([x])) → CumulativeAvg([x])
    tcMatch = f.match(/^RUNNING_AVG\s*\(\s*(?:SUM\s*\(\s*)?(\[[^\]]+\])\s*\)?\s*\)/i);
    if (tcMatch) return 'CumulativeAvg(' + tcMatch[1] + ')';

    // RUNNING_MIN/MAX
    tcMatch = f.match(/^RUNNING_(MIN|MAX)\s*\(\s*(?:(?:SUM|MIN|MAX)\s*\(\s*)?(\[[^\]]+\])\s*\)?\s*\)/i);
    if (tcMatch) return 'Cumulative' + tcMatch[1].charAt(0) + tcMatch[1].slice(1).toLowerCase() + '(' + tcMatch[2] + ')';

    // RANK(SUM([x])) or RANK([x]) → Rank([x])
    tcMatch = f.match(/^RANK\s*\(\s*(?:SUM\s*\(\s*)?(\[[^\]]+\])\s*\)?\s*\)/i);
    if (tcMatch) return 'Rank(' + tcMatch[1] + ')';

    // RANK_DENSE → DenseRank
    tcMatch = f.match(/^RANK_DENSE\s*\(\s*(?:SUM\s*\(\s*)?(\[[^\]]+\])\s*\)?\s*\)/i);
    if (tcMatch) return 'DenseRank(' + tcMatch[1] + ')';

    // RANK_UNIQUE → Rank
    tcMatch = f.match(/^RANK_UNIQUE\s*\(\s*(?:SUM\s*\(\s*)?(\[[^\]]+\])\s*\)?\s*\)/i);
    if (tcMatch) return 'Rank(' + tcMatch[1] + ')';

    // INDEX() → RowNumber()
    if (/^INDEX\s*\(\s*\)/i.test(f)) return 'RowNumber()';

    // WINDOW_SUM(SUM([x])) → GrandTotal(Sum([x]))
    tcMatch = f.match(/^WINDOW_SUM\s*\(\s*(SUM|COUNT|AVG|MIN|MAX)\s*\(\s*(\[[^\]]+\])\s*\)\s*\)/i);
    if (tcMatch) {
      const aggMap: Record<string, string> = { SUM: 'Sum', COUNT: 'Count', AVG: 'Avg', MIN: 'Min', MAX: 'Max' };
      return 'GrandTotal(' + (aggMap[tcMatch[1].toUpperCase()] || tcMatch[1]) + '(' + tcMatch[2] + '))';
    }

    // Couldn't parse — fall back to comment
    if (warnings) warnings.push('⚠ Table calculation not converted: ' + f.slice(0, 60));
    return '/* table calc: ' + f.replace(/\/\*/g, '').replace(/\*\//g, '') + ' */';
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

  f = tableauIfToSigma(f);
  f = f.replace(/\bIIF\s*\(/gi, 'If(');
  f = tableauCaseToSigma(f);

  // DATEPART('year', [Date]) → Year([Date])
  f = f.replace(/\bDATEPART\s*\(\s*'(\w+)'\s*,\s*([^)]+)\)/gi, (m, part, dateArg) => {
    const partMap: Record<string, string> = {
      year: 'Year', month: 'Month', day: 'Day', hour: 'Hour', minute: 'Minute',
      second: 'Second', week: 'Week', quarter: 'Quarter', dayofweek: 'DayOfWeek', weekday: 'DayOfWeek'
    };
    const fn = partMap[part.toLowerCase()];
    return fn ? fn + '(' + dateArg.trim() + ')' : m;
  });
  f = f.replace(/\bDATETRUNC\s*\(\s*'([^']+)'\s*,/gi, 'DateTrunc("$1",');
  f = f.replace(/\bDATEADD\s*\(\s*'([^']+)'\s*,/gi, 'DateAdd("$1",');
  f = f.replace(/\bDATEDIFF\s*\(\s*'([^']+)'\s*,/gi, 'DateDiff("$1",');

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

  return f.trim();
}

/** Check if a Tableau formula contains aggregate functions */
export function tableauIsAggregate(formula: string): boolean {
  return /\b(SUM|AVG|COUNT|COUNTD|MAX|MIN|MEDIAN|STDEV|VAR|ATTR)\s*\(/i.test(formula);
}

/** Strip ${TABLE}. and extract leading identifier from sql: */
export function lookStripSql(sql: string): string {
  if (!sql) return '';
  sql = sql.replace(/\$\{TABLE\}\./gi, '').trim();
  sql = sql.replace(/\$\{[^.}]+\.([^}]+)\}/g, '$1');
  // Unwrap CAST(col AS type) → col
  const castMatch = sql.match(/^CAST\s*\(\s*("?[A-Za-z_][A-Za-z0-9_]*"?)\s+AS\s+\w[\w_]*\s*\)$/i);
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
