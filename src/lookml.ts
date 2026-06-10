/**
 * LookML → Sigma Data Model JSON converter.
 * Includes a full LookML parser and explore-based conversion.
 */

import {
  resetIds, sigmaShortId, sigmaInodeId, sigmaDisplayName, makeRlsSecurity,
  type SigmaElement, type ConversionResult, type ElementResult, type SecurityRule
} from './sigma-ids.js';
import { lookIsComplexSql, lookSqlToSigmaRules, lookConvertExpression, lookStripSql, lookSigmaMetric, detectUnsupportedSigmaFunction } from './formulas.js';

// ── LookML number-format → Sigma format ──────────────────────────────────────
// Sigma DM columns/metrics carry a `format` object of the shape
//   { kind: 'number', formatString: '<d3-format>', currencySymbol?, suffix? }
// (the same shape emitted by the powerbi/qlik/alteryx converters). LookML fields
// carry either a named `value_format_name` (usd, percent_1, decimal_2, gbp, …) or
// a custom Excel-style `value_format` mask ("$#,##0.00", "0.0%", "$#,##0.0\"K\"").
// Map both to that shape. Returns null when nothing maps cleanly so callers omit
// the format and (optionally) emit a warning.

/** value_format_name → Sigma format. The decimals/currency are encoded in the name. */
function lookmlNamedFormat(name: string): Record<string, any> | null {
  const n = name.trim().toLowerCase();
  // currency: usd / usd_0, gbp / gbp_0, eur / eur_0, plus a couple of common synonyms
  const CUR: Record<string, string> = { usd: '$', gbp: '£', eur: '€', cad: '$', aud: '$' };
  let m = n.match(/^(usd|gbp|eur|cad|aud)(?:_(\d+))?$/);
  if (m) {
    const sym = CUR[m[1]];
    const dec = m[2] != null ? Number(m[2]) : 2;
    return { kind: 'number', formatString: `${sym},.${dec}f`, currencySymbol: sym };
  }
  m = n.match(/^percent(?:_(\d+))?$/);
  if (m) {
    const dec = m[1] != null ? Number(m[1]) : 0;
    return { kind: 'number', formatString: `,.${dec}%` };
  }
  m = n.match(/^decimal(?:_(\d+))?$/);
  if (m) {
    const dec = m[1] != null ? Number(m[1]) : 0;
    return { kind: 'number', formatString: `,.${dec}f` };
  }
  // id / number-ish integer formats Looker ships
  if (n === 'id') return { kind: 'number', formatString: ',.0f' };
  return null;
}

/**
 * Custom value_format mask (Excel/.NET-ish) → Sigma format. Handles the common
 * patterns: currency symbol, percent, thousands separators, fixed decimals, and
 * a trailing "K"/"M"/"B" scale suffix. Returns null for masks we can't read.
 */
function lookmlCustomFormat(mask: string): Record<string, any> | null {
  if (typeof mask !== 'string') return null;
  const raw = mask.trim();
  if (!raw) return null;
  // dates / text masks — let the heuristic fallback (if any) handle / skip
  if (/general|date|time|@|yyyy|mmm|\bdd\b/i.test(raw)) return null;
  // decimals = run of 0/# after the first decimal point
  const decM = raw.match(/\.([0#]+)/);
  const decimals = decM ? decM[1].length : 0;
  const isPercent = /%/.test(raw);
  const curM = raw.match(/[$£€¥]/);
  // trailing scale suffix in a quoted literal, e.g.  $#,##0.0\"K\"  or  0.0,,"M".
  // The mask may arrive with the inner quotes still escaped (\"K\") after the
  // parser strips the OUTER quotes, so match an optional backslash before each.
  const sufM = raw.match(/\\?["']\s*([KMB])\s*\\?["']/i);
  const suffix = sufM ? sufM[1].toUpperCase() : undefined;
  const SYM: Record<string, string> = { '$': '$', '£': '£', '€': '€', '¥': '¥' };

  if (isPercent) {
    const fmt: Record<string, any> = { kind: 'number', formatString: `,.${decimals}%` };
    if (suffix) fmt.suffix = suffix;
    return fmt;
  }
  if (curM) {
    const sym = SYM[curM[0]] || '$';
    const fmt: Record<string, any> = { kind: 'number', formatString: `${sym},.${decimals}f`, currencySymbol: sym };
    if (suffix) fmt.suffix = suffix;
    return fmt;
  }
  if (/[0#]/.test(raw)) {
    const fmt: Record<string, any> = { kind: 'number', formatString: `,.${decimals}f` };
    if (suffix) fmt.suffix = suffix;
    return fmt;
  }
  return null;
}

/**
 * Resolve a LookML field's number format into a Sigma `format` object.
 * `value_format_name` (named) takes priority over a custom `value_format` mask.
 * Pushes an actionable warning when a format is present but can't be mapped.
 */
function lookmlFieldFormat(field: any, warnings: string[]): Record<string, any> | undefined {
  if (!field || typeof field !== 'object') return undefined;
  const named = field.value_format_name;
  if (typeof named === 'string' && named.trim()) {
    const f = lookmlNamedFormat(named);
    if (f) return f;
    warnings.push(`⚠ "${field._name}": value_format_name "${named}" has no Sigma mapping — set the column format manually.`);
    return undefined;
  }
  const custom = field.value_format;
  if (typeof custom === 'string' && custom.trim()) {
    const f = lookmlCustomFormat(custom);
    if (f) return f;
    warnings.push(`⚠ "${field._name}": value_format "${custom}" could not be translated — set the column format manually.`);
    return undefined;
  }
  return undefined;
}

// ── LookML Parser ────────────────────────────────────────────────────────────

interface LookMLParseResult {
  views: any[];
  explores: any[];
  connection: string | null;
  label: string | null;
  includes: string[];
}

function restoreSqlPlaceholders(obj: any, map: Record<string, string>): void {
  if (!obj || typeof obj !== 'object') return;
  for (const key of Object.keys(obj)) {
    const v = obj[key];
    if (typeof v === 'string' && map[v] !== undefined) {
      obj[key] = map[v];
    } else if (typeof v === 'object') {
      restoreSqlPlaceholders(v, map);
    }
  }
}

export function parseLookML(text: string): LookMLParseResult {
  // Strip line comments — but a `#` inside a double-quoted string is NOT a
  // comment (e.g. value_format: "$#,##0.0\"K\""). Walk char-by-char tracking
  // quote state so we only drop `#`→EOL runs that are outside a string literal.
  text = (() => {
    let out = '';
    let inStr = false;
    for (let i = 0; i < text.length; i++) {
      const ch = text[i];
      if (inStr) {
        out += ch;
        if (ch === '\\' && i + 1 < text.length) { out += text[++i]; continue; }
        if (ch === '"') inStr = false;
        continue;
      }
      if (ch === '"') { inStr = true; out += ch; continue; }
      if (ch === '#') { while (i < text.length && text[i] !== '\n') i++; if (i < text.length) out += '\n'; continue; }
      out += ch;
    }
    return out;
  })();

  // Pre-extract raw sql: ... ;; blocks
  const sqlPlaceholders: Record<string, string> = {};
  let phIdx = 0;
  text = text.replace(/\b(sql_trigger_value|sql_table_name|sql_where|sql_start|sql_end|sql_on|html|sql)\s*:([\s\S]*?);;/g, (match, keyName, sqlContent) => {
    const key = `__SQLPH${phIdx++}__`;
    sqlPlaceholders[key] = sqlContent.trim();
    return `${keyName}: "${key}" ;;`;
  });

  const tokens: string[] = [];
  const re = /;;;?|\$\{[^}]*\}|[\[\]{}]|"(?:[^"\\]|\\.)*"|[^\s\[\]{}:;,"]+|:/g;
  let m;
  while ((m = re.exec(text)) !== null) tokens.push(m[0]);

  let pos = 0;
  const peek = (n?: number) => tokens[pos + (n || 0)];
  const consume = () => tokens[pos++];

  const NAMED_BLOCK_KEYS = new Set([
    'dimension', 'measure', 'dimension_group', 'filter', 'parameter',
    'join', 'set', 'link', 'action', 'form_param', 'option',
    // Native Derived Table (NDT): `explore_source: <explore> { column ... }`
    'explore_source', 'column', 'derived_column'
  ]);

  const SQL_KEYS = new Set(['sql', 'sql_on', 'sql_where', 'sql_table_name',
    'sql_trigger_value', 'html', 'label_from_parameter', 'sql_start', 'sql_end']);

  function parseBlock(): any {
    const obj: any = {};
    while (pos < tokens.length) {
      const t = peek();
      if (t === '}') { consume(); break; }
      if (t === undefined) break;

      const key = consume();
      if (peek() !== ':') continue;
      consume(); // eat ':'

      const a0 = peek();
      const a1 = peek(1);

      if (SQL_KEYS.has(key)) {
        const parts: string[] = [];
        while (pos < tokens.length &&
          peek() !== ';;' && peek() !== ';;;' &&
          peek() !== '}') {
          parts.push(consume());
        }
        if (peek() === ';;' || peek() === ';;;') consume();
        let val = parts.join(' ').trim();
        if (val.startsWith('"') && val.endsWith('"')) val = val.slice(1, -1);
        obj[key] = val;

      } else if (NAMED_BLOCK_KEYS.has(key) && a0 && a0 !== '{' && a1 === '{') {
        const name = consume().replace(/"/g, '');
        consume(); // eat '{'
        const child = parseBlock();
        child._name = name;
        if (obj[key] !== undefined) {
          if (!Array.isArray(obj[key])) obj[key] = [obj[key]];
          obj[key].push(child);
        } else {
          obj[key] = [child];
        }

      } else if (a0 === '{') {
        consume();
        const child = parseBlock();
        if (obj[key] !== undefined) {
          if (!Array.isArray(obj[key])) obj[key] = [obj[key]];
          obj[key].push(child);
        } else {
          obj[key] = child;
        }

      } else if (a0 === ';;' || a0 === ';;;') {
        consume(); obj[key] = '';

      } else if (a0 === '[') {
        // Bracket array: [key: "val", ...] or ["val1", "val2"]
        consume(); // eat '['
        const items: any[] = [];
        while (pos < tokens.length && peek() !== ']') {
          const t1 = consume();
          if (t1 === undefined) break;
          if (peek() === ':') {
            consume(); // eat ':'
            let val = consume() || '';
            if (val.startsWith('"') && val.endsWith('"')) val = val.slice(1, -1);
            items.push({ field: t1.replace(/"/g, ''), value: val });
          } else {
            let val = t1;
            if (val.startsWith('"') && val.endsWith('"')) val = val.slice(1, -1);
            items.push(val);
          }
        }
        if (peek() === ']') consume();
        if (peek() === ';;' || peek() === ';;;') consume();
        obj[key] = items;

      } else {
        let val = consume() || '';
        if (val.startsWith('"') && val.endsWith('"')) val = val.slice(1, -1);
        if (peek() === ';;' || peek() === ';;;') consume();
        obj[key] = val;
      }
    }
    return obj;
  }

  const result: LookMLParseResult = { views: [], explores: [], connection: null, label: null, includes: [] };
  while (pos < tokens.length) {
    const keyword = consume();
    if (!keyword) continue;
    if ((keyword === 'view' || keyword === 'explore') && peek() === ':') {
      consume();
      const name = (consume() || '').replace(/"/g, '');
      if (peek() === '{') {
        consume();
        const block = parseBlock();
        block._name = name;
        result[keyword + 's' as 'views' | 'explores'].push(block);
      }
    } else if (keyword === 'connection' && peek() === ':') {
      consume(); result.connection = (consume() || '').replace(/"/g, '');
    } else if (keyword === 'label' && peek() === ':') {
      consume(); result.label = (consume() || '').replace(/"/g, '');
    } else if (keyword === 'include' && peek() === ':') {
      consume(); result.includes.push((consume() || '').replace(/"/g, ''));
    } else if (peek() === ':') {
      consume();
      if (peek() === '{') { consume(); parseBlock(); }
      else { consume(); if (peek() === ';;' || peek() === ';;;') consume(); }
    }
  }
  restoreSqlPlaceholders(result, sqlPlaceholders);
  return result;
}

// ── SQL_TABLE_NAME resolution ─────────────────────────────────────────────────

const PDT_SQL_PREFIX = '__PDT_SQL__:';

/**
 * Pre-compute a map of viewName → resolved table reference.
 *
 * For regular views:  "SCHEMA.DB.TABLE"   (literal path, ready to split on '.')
 * For derived tables: PDT_SQL_PREFIX + sql (the raw SQL, for inline subquery use)
 *
 * Resolution is iterative to handle N-hop ${ref.SQL_TABLE_NAME} chains.
 */
function buildSqlTableNameMap(views: Record<string, any>): Record<string, string> {
  const map: Record<string, string> = {};

  for (const [name, view] of Object.entries(views)) {
    if (!view) continue;
    if (view.derived_table) {
      const sql = (view.derived_table.sql || '').replace(/;;\s*$/, '').trim();
      map[name] = PDT_SQL_PREFIX + sql;
    } else if (view.sql_table_name) {
      map[name] = view.sql_table_name.trim();
    }
  }

  // Iteratively follow ${ref.SQL_TABLE_NAME} hops until stable (max 20 iterations)
  let changed = true;
  for (let i = 0; i < 20 && changed; i++) {
    changed = false;
    for (const name of Object.keys(map)) {
      const val = map[name];
      if (!val.includes('${')) continue;
      const next = val.replace(/\$\{(\w+)\.SQL_TABLE_NAME\}/gi, (_m, ref) => {
        const refVal = map[ref];
        if (refVal !== undefined && !refVal.includes('${')) return refVal;
        return _m; // not yet resolved
      });
      if (next !== val) { map[name] = next; changed = true; }
    }
  }

  return map;
}

/**
 * Resolve all ${viewName.SQL_TABLE_NAME} references inside a derived-table SQL
 * string using the pre-built map.
 *
 * Regular views   → substituted with the literal path (e.g. CSA.TJ.ORDER_FACT)
 * Derived tables  → substituted with an inline subquery: (SQL) AS viewName
 * Unknown refs    → left as-is; caller emits a warning
 */
function resolveSqlTableNameRefs(
  sql: string,
  map: Record<string, string>,
  warnings: string[],
  contextViewName: string
): string {
  return sql.replace(/\$\{(\w+)\.SQL_TABLE_NAME\}/gi, (_m, ref) => {
    const val = map[ref];
    if (val === undefined) {
      warnings.push(`⚠ View "${contextViewName}": could not resolve \${${ref}.SQL_TABLE_NAME} — view "${ref}" not found in provided files`);
      return _m;
    }
    if (val.includes('${')) {
      warnings.push(`⚠ View "${contextViewName}": \${${ref}.SQL_TABLE_NAME} could not be fully resolved (circular or missing chain)`);
      return _m;
    }
    if (val.startsWith(PDT_SQL_PREFIX)) {
      const pdtSql = val.slice(PDT_SQL_PREFIX.length);
      return `(\n${pdtSql}\n)`;
    }
    return val;
  });
}

// ── LookML View → Sigma Element Conversion ───────────────────────────────────

function lookExtractPath(view: any, sqlTableNameMap?: Record<string, string>): string[] {
  let raw = (view.sql_table_name || view.from || '').trim().replace(/`/g, '');
  if (!raw) return [];

  // Resolve ${ref.SQL_TABLE_NAME} in sql_table_name if a map is provided
  if (sqlTableNameMap && raw.includes('${')) {
    raw = raw.replace(/\$\{(\w+)\.SQL_TABLE_NAME\}/gi, (_m: string, ref: string) => {
      const val = sqlTableNameMap[ref];
      if (val && !val.startsWith(PDT_SQL_PREFIX) && !val.includes('${')) return val;
      return _m;
    });
  }

  // If still unresolved (e.g. cross-file alias not provided), fall back to view name
  if (raw.includes('${')) return [];

  return raw.split('.').map((p: string) => p.trim().toUpperCase()).filter(Boolean);
}

function lookFindColId(elementResult: ElementResult, colName: string): string | null {
  if (!elementResult) return null;
  const upper = (colName || '').toUpperCase();
  return elementResult.colIdMap[upper] || null;
}

// Parse a LookML filter expression into a Sigma list filter object.
// Returns null for date/range expressions (unknown JSON schema).
function lookParseFilterExpr(expr: string, columnId: string): Record<string, any> | null {
  expr = (expr || '').trim();

  if (/^NULL$/i.test(expr))
    return { id: sigmaShortId(), columnId, kind: 'list', mode: 'include', values: [null] };
  if (/^NOT\s+NULL$/i.test(expr))
    return { id: sigmaShortId(), columnId, kind: 'list', mode: 'exclude', values: [null] };

  // Date relative expressions — unsupported
  if (/^\d+\s+(second|minute|hour|day|week|month|quarter|year)s?$/i.test(expr)) return null;
  if (/^(this|last|next|current)\s+/i.test(expr)) return null;
  if (/^\d{4}[\/\-]\d{2}/.test(expr)) return null;

  // Comparison / range — unsupported
  if (/^[><!]=?/.test(expr)) return null;
  if (/^[\[(]/.test(expr)) return null;

  // Negation: -value or -value1,-value2
  if (expr.startsWith('-')) {
    const vals = expr.slice(1).split(/\s*,\s*-?\s*/).map(v => v.replace(/^"|"$/g, '').trim()).filter(Boolean);
    return { id: sigmaShortId(), columnId, kind: 'list', mode: 'exclude', values: vals };
  }

  // Simple string value(s)
  const vals = expr.split(',').map(v => v.replace(/^"|"$/g, '').trim()).filter(Boolean);
  if (vals.length > 0)
    return { id: sigmaShortId(), columnId, kind: 'list', mode: 'include', values: vals };

  return null;
}

interface NdtContext {
  views: Record<string, any>;
  explores: Record<string, any>;
}

/**
 * Best-effort resolution of a Native Derived Table (NDT) — a `derived_table`
 * whose `explore_source` aggregates an existing explore rather than supplying
 * raw `sql:`. We resolve the referenced explore's base view to its warehouse
 * table and translate the listed `column`/`derived_column`/`filters` into a
 * single-table SELECT. This is intentionally conservative: it only emits SQL
 * when every selected column resolves to a physical field on the base view
 * (no cross-view joins), otherwise it returns null so the caller falls back to
 * a clear "rebuild as a Sigma data element" warning. Never produces dangling
 * refs.
 *
 * Returns { sql, resolved } where resolved=false means "could not build SQL".
 */
function resolveNdtToSql(
  ndtViewName: string,
  explSource: any,
  ctx: NdtContext,
  warnings: string[]
): { sql: string; resolved: boolean } {
  const exploreName: string = explSource._name || '';
  const explore = ctx.explores[exploreName];
  if (!explore) {
    warnings.push(`⚠ View "${ndtViewName}" is a Native Derived Table on explore "${exploreName}", which was not found in the provided files. Rebuild it as a Sigma data element (aggregate the source element) after import.`);
    return { sql: '', resolved: false };
  }

  // Resolve the explore's base view → warehouse table path.
  const baseViewName: string = explore.from || exploreName;
  const baseView = ctx.views[baseViewName];
  if (!baseView || baseView.derived_table) {
    warnings.push(`⚠ View "${ndtViewName}" (NDT on explore "${exploreName}"): base view "${baseViewName}" is not a simple warehouse table — cannot auto-generate SQL. Rebuild as a Sigma data element after import.`);
    return { sql: '', resolved: false };
  }
  const basePath = lookExtractPath(baseView);
  if (!basePath.length) {
    warnings.push(`⚠ View "${ndtViewName}" (NDT on explore "${exploreName}"): could not resolve base table for view "${baseViewName}". Rebuild as a Sigma data element after import.`);
    return { sql: '', resolved: false };
  }
  const fromTable = basePath.join('.');

  // Build a map of base-view field name → { sql, isMeasure, aggType }.
  const dimMap = new Map<string, string>(); // field name → physical SQL expr
  const dims = baseView.dimension ? (Array.isArray(baseView.dimension) ? baseView.dimension : [baseView.dimension]) : [];
  for (const d of dims) {
    if (!d._name || !d.sql) continue;
    const expr = d.sql.replace(/\$\{TABLE\}\s*\.\s*/gi, '').replace(/;;\s*$/, '').trim();
    if (/\$\{/.test(expr)) continue; // cross-field/cross-view ref — skip (not tractable)
    dimMap.set(d._name.toLowerCase(), expr);
  }
  const measureMap = new Map<string, { agg: string; col: string }>();
  const measures = baseView.measure ? (Array.isArray(baseView.measure) ? baseView.measure : [baseView.measure]) : [];
  for (const ms of measures) {
    if (!ms._name) continue;
    const t = (ms.type || '').toLowerCase();
    const aggFn: Record<string, string> = { sum: 'SUM', average: 'AVG', avg: 'AVG', min: 'MIN', max: 'MAX', count: 'COUNT', count_distinct: 'COUNT', median: 'MEDIAN' };
    if (!aggFn[t]) continue;
    let col = '*';
    if (ms.sql) {
      const e = ms.sql.replace(/\$\{TABLE\}\s*\.\s*/gi, '').replace(/;;\s*$/, '').trim();
      if (/\$\{(\w+)\}/.test(e)) {
        const refName = e.match(/\$\{(\w+)\}/)![1].toLowerCase();
        col = dimMap.get(refName) || '*';
        if (col === '*') continue;
      } else col = e;
    } else if (t !== 'count') continue;
    const distinct = t === 'count_distinct' ? 'DISTINCT ' : '';
    measureMap.set(ms._name.toLowerCase(), { agg: aggFn[t], col: distinct + col });
  }

  // Translate the listed columns.
  const cols = explSource.column ? (Array.isArray(explSource.column) ? explSource.column : [explSource.column]) : [];
  const selectParts: string[] = [];
  const groupByExprs: string[] = [];
  let hasMeasure = false;
  let unresolved = false;

  for (const c of cols) {
    const outName = c._name || '';
    const fieldRef = (c.field || c._name || '').trim();
    // field ref like "order_fact.order_status" or bare "order_status"
    const fieldName = (fieldRef.includes('.') ? fieldRef.split('.').pop()! : fieldRef).toLowerCase();
    // Uppercase the alias so it matches the warehouse-folded identifier that
    // the NDT view's own dimensions reference (e.g. ${TABLE}.order_status →
    // ORDER_STATUS). Sigma fuzzy-matches case for self-refs inside the SQL
    // element, so this resolves cleanly from both bare-lowercase and
    // bracket-uppercase column formulas.
    const alias = outName ? ` AS "${outName.toUpperCase()}"` : '';
    if (measureMap.has(fieldName)) {
      const m = measureMap.get(fieldName)!;
      selectParts.push(`${m.agg}(${m.col})${alias}`);
      hasMeasure = true;
    } else if (dimMap.has(fieldName)) {
      const expr = dimMap.get(fieldName)!;
      selectParts.push(`${expr}${alias}`);
      groupByExprs.push(expr);
    } else {
      unresolved = true;
      break;
    }
  }

  if (unresolved || selectParts.length === 0) {
    warnings.push(`⚠ View "${ndtViewName}" (NDT on explore "${exploreName}"): one or more selected columns reference fields that could not be resolved on the base view (joined/derived fields are not supported here). Rebuild this NDT as a Sigma data element that aggregates the "${sigmaDisplayName(exploreName)}" element after import.`);
    return { sql: '', resolved: false };
  }

  let sql = `SELECT\n  ${selectParts.join(',\n  ')}\nFROM ${fromTable}`;
  if (hasMeasure && groupByExprs.length) {
    sql += `\nGROUP BY ${groupByExprs.join(', ')}`;
  }
  warnings.push(`ℹ View "${ndtViewName}" → Native Derived Table on explore "${exploreName}" was translated to a Custom SQL element (aggregation pushed to ${fromTable}). Review the generated SQL and consider rebuilding it as a native Sigma data element for full editability.`);
  return { sql, resolved: true };
}

function lookConvertView(
  viewName: string,
  view: any,
  connectionId: string,
  warnings: string[],
  sqlTableNameMap?: Record<string, string>,
  ndtContext?: NdtContext
): ElementResult {
  if (!view) {
    warnings.push(`⚠ View "${viewName}" not found — element will have no columns`);
    const id = sigmaShortId();
    return {
      element: { id, kind: 'table', source: { connectionId: connectionId || '<CONNECTION_ID>', kind: 'warehouse-table', path: [viewName.toUpperCase()] }, columns: [], order: [] },
      elementId: id,
      colIdMap: {}
    };
  }

  const elementId = sigmaShortId();
  let tableName: string, element: SigmaElement;

  if (view.derived_table !== undefined) {
    let rawSql = (view.derived_table.sql || '').replace(/;;\s*$/, '').trim();

    // Native Derived Table (NDT): no raw sql:, aggregates an explore_source.
    // The parser wraps named blocks in an array — unwrap to the first.
    const explSource = Array.isArray(view.derived_table.explore_source)
      ? view.derived_table.explore_source[0]
      : view.derived_table.explore_source;
    if (!rawSql && explSource !== undefined && ndtContext) {
      const ndt = resolveNdtToSql(viewName, explSource, ndtContext, warnings);
      if (ndt.resolved) rawSql = ndt.sql;
      // If not resolved, rawSql stays empty → handled below as a clear warning
      // with an empty (but valid) SQL element. No dangling refs.
    } else if (!rawSql && explSource !== undefined) {
      warnings.push(`⚠ View "${viewName}" is a Native Derived Table (explore_source) but explore context was unavailable — rebuild it as a Sigma data element after import.`);
    }

    // Gap 1: resolve ${ref.SQL_TABLE_NAME} references inside derived table SQL
    if (sqlTableNameMap && rawSql.includes('${')) {
      rawSql = resolveSqlTableNameRefs(rawSql, sqlTableNameMap, warnings, viewName);
    }

    // PDT persistence hints → Sigma scheduled materialization (informational).
    // datagroup_trigger / persist_for / sql_trigger_value control when Looker
    // rebuilds a Persistent Derived Table; the Sigma equivalent is scheduled
    // materialization on the data model (Materialization tab in the DM UI / API).
    const PERSIST_HINTS = ['datagroup_trigger', 'persist_for', 'sql_trigger_value'];
    const dt = view.derived_table as any;
    for (const prop of PERSIST_HINTS) {
      if (dt[prop] !== undefined) {
        const val = typeof dt[prop] === 'string' ? dt[prop] : '';
        warnings.push(`ℹ View "${viewName}": PDT persistence hint "${prop}"${val ? ` (${val})` : ''} maps to Sigma scheduled materialization. Configure a materialization schedule on this data model (Materialization tab in the Sigma UI, or via the API) to get the equivalent refresh cadence.`);
      }
    }

    // Other warehouse-specific PDT properties that have no Sigma equivalent.
    const PDT_SKIP_PROPS = ['distribution', 'sortkeys', 'persist_with', 'cluster_keys', 'partition_keys'];
    for (const prop of PDT_SKIP_PROPS) {
      if (dt[prop] !== undefined) {
        warnings.push(`ℹ View "${viewName}": PDT property "${prop}" is a warehouse-specific materialization hint and is not converted — configure this in your warehouse or Sigma dataset settings.`);
      }
    }

    tableName = 'Custom SQL';
    element = {
      id: elementId,
      kind: 'table',
      source: {
        connectionId: connectionId || '<CONNECTION_ID>',
        statement: rawSql || '',
        kind: 'sql'
      },
      columns: [],
      metrics: [],
      order: []
    };
    if (rawSql) warnings.push(`ℹ View "${viewName}" → Custom SQL element. Review the SQL before saving.`);
    else warnings.push(`⚠ View "${viewName}" derived_table has no sql — SQL statement left blank. Add SQL manually in the JSON before saving.`);
  } else {
    const path = lookExtractPath(view, sqlTableNameMap);
    tableName = (path[path.length - 1] || viewName).toUpperCase();
    element = {
      id: elementId,
      kind: 'table',
      source: {
        connectionId: connectionId || '<CONNECTION_ID>',
        kind: 'warehouse-table',
        path: path.length > 0 ? path : [viewName.toUpperCase()]
      },
      columns: [],
      metrics: [],
      order: []
    };
  }

  const colIdMap: Record<string, string> = {};
  const isCustomSql = (tableName === 'Custom SQL');
  const colLabel = (physCol: string) => isCustomSql ? physCol : sigmaDisplayName(physCol);
  // Warehouse columns get deterministic inode- IDs; calculated/SQL columns get short random IDs
  const makeColId = (physCol: string) => isCustomSql ? sigmaShortId() : sigmaInodeId(physCol);

  // Detect Liquid templating — can't be statically converted
  const viewSqls = JSON.stringify(view);
  if (/\{%-?\s*(if|unless|for|assign|capture)\b/i.test(viewSqls)) {
    warnings.push(`⚠ View "${viewName}": contains Liquid templating ({% if %} blocks). Dimensions using Liquid conditionals will be skipped — review and add manually in Sigma.`);
  }

  // Build per-view maps for same-view field ref expansion in computed dimensions.
  // yesnoExprMap: fieldName → cleaned boolean SQL (e.g. IS_ACTIVE = 1)
  // fieldDisplayMap: fieldName → Sigma display name (uses label if present)
  const yesnoExprMap = new Map<string, string>();
  const fieldDisplayMap = new Map<string, string>();
  // dimension name (lowercase) → physical column (UPPER). Resolves intra-view
  // ${dimension} references inside measure sql (e.g. measure total_revenue {
  // sql: ${sale_price} } → SALE_PRICE).
  const dimPhysColMap = new Map<string, string>();
  {
    const allDims = view.dimension ? (Array.isArray(view.dimension) ? view.dimension : [view.dimension]) : [];
    allDims.forEach((yd: any) => {
      if (!yd._name) return;
      const lname = yd._name.toLowerCase();
      if ((yd.type || '').toLowerCase() === 'yesno' && yd.sql) {
        const expr = yd.sql
          .replace(/\$\{TABLE\}\s*\.\s*/gi, '')
          .replace(/\$\{[^.}]+\.([^}]+)\}/g, (_: string, f: string) => f.toUpperCase())
          .replace(/\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g, (_: string, n: string) => n.toUpperCase())
          .trim();
        yesnoExprMap.set(lname, expr);
      } else {
        // Simple (non-complex) SQL dims have no explicit "name" in the model spec,
        // so Sigma auto-assigns a name via its friendly naming (sigmaDisplayName of physCol).
        // Use that same name for formula refs so [Discount Pct] matches the column.
        // Complex/calculated dims DO get name: label in the spec, so use the label there.
        let displayName: string;
        if (yd.sql && !lookIsComplexSql(yd.sql)) {
          const stripped = lookStripSql(yd.sql) || yd._name;
          const physCol = stripped.split('.').pop()!.replace(/"/g, '').toUpperCase();
          displayName = colLabel(physCol);
          dimPhysColMap.set(lname, physCol);
        } else {
          displayName = yd.label || sigmaDisplayName(yd._name);
        }
        fieldDisplayMap.set(lname, displayName);
      }
    });
  }

  // Pre-expand ${field_ref} in dimension SQL before passing to the converter.
  // Yesno refs → (BOOLEAN_EXPR); other refs → [Display Name] using label if present.
  function expandFieldRefs(sql: string): string {
    if (!yesnoExprMap.size && !fieldDisplayMap.size) return sql;
    return sql.replace(/\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g, (match: string, n: string) => {
      const lname = n.toLowerCase();
      const yesnoExpr = yesnoExprMap.get(lname);
      if (yesnoExpr !== undefined) return `(${yesnoExpr})`;
      const displayName = fieldDisplayMap.get(lname);
      if (displayName !== undefined) return `[${displayName}]`;
      return match;
    });
  }

  // Dimensions
  const dims = view.dimension ? (Array.isArray(view.dimension) ? view.dimension : [view.dimension]) : [];
  for (const d of dims) {
    if (!d._name) continue;
    const colName = d._name.toUpperCase();
    // LookML number format on the dimension (value_format_name / value_format).
    const dFormat = lookmlFieldFormat(d, warnings);

    // ── legacy `case: { when: {sql,label}... else }` → nested If() ──
    if (d.case && typeof d.case === 'object') {
      const whens = Array.isArray(d.case.when) ? d.case.when : (d.case.when ? [d.case.when] : []);
      const toCond = (sql: string) => lookConvertExpression(
        (sql || '').replace(/\$\{TABLE\}\./gi, '')
          .replace(/\$\{[^.}]+\.([^}]+)\}/g, '$1')
          .replace(/[\r\n]+\s*/g, ' ').trim());
      let formula = d.case.else != null ? `"${String(d.case.else)}"` : 'Null';
      for (let i = whens.length - 1; i >= 0; i--) {
        const w = whens[i];
        if (typeof w !== 'object' || !w.sql) continue;
        formula = `If(${toCond(w.sql)}, "${String(w.label ?? w._name ?? '')}", ${formula})`;
      }
      const caseColId = sigmaShortId();
      colIdMap[colName] = caseColId;
      element.columns.push({ id: caseColId, formula, name: d.label || sigmaDisplayName(d._name) });
      element.order.push(caseColId);
      warnings.push(`✅ "${d._name}" (case) → ${formula.slice(0, 70)}`);
      continue;
    }

    // Detect LookML parameter substitution — can't be resolved statically
    if (/\$\{[^.}]+\}/.test(d.sql || '') && !/\$\{TABLE\}/i.test(d.sql || '')) {
      warnings.push(`⚠ "${d._name}": uses LookML parameter substitution — skipped. Add this dimension manually after configuring parameters in Sigma.`);
      continue;
    }

    if (lookIsComplexSql(d.sql)) {
      const cleanedSql = (d.sql || '').replace(/\$\{TABLE\}\./gi, '').replace(/\$\{[^.}]+\.([^}]+)\}/g, '$1').trim();
      const boolMatch = cleanedSql.match(/^([A-Z_][A-Z0-9_]*)\s*=\s*(\d+)$/i);

      if (boolMatch) {
        const physicalCol = boolMatch[1].toUpperCase();
        const val = boolMatch[2];
        let physColId = colIdMap[physicalCol];
        if (!physColId) {
          physColId = makeColId(physicalCol);
          colIdMap[physicalCol] = physColId;
          element.columns.push({ id: physColId, formula: `[${tableName}/${colLabel(physicalCol)}]` });
          element.order.push(physColId);
        }
        const calcId = sigmaShortId();
        colIdMap[colName] = calcId;
        const baseName = d.label || sigmaDisplayName(d._name);
        const displayName = baseName + ' (T/F)';
        element.columns.push({ id: calcId, formula: `[${colLabel(physicalCol)}] = ${val}`, name: displayName });
        element.order.push(calcId);
        continue;
      }

      const unsupported = detectUnsupportedSigmaFunction(d.sql || '');
      if (unsupported) {
        warnings.push(`⚠ "${d._name}": skipped — contains ${unsupported}() which has no Sigma equivalent. Add this column manually in the Sigma UI.`);
        continue;
      }
      const colId = sigmaShortId();
      colIdMap[colName] = colId;
      const expandedSql = expandFieldRefs(d.sql || '');
      let sigmaFormula = lookSqlToSigmaRules(expandedSql);
      if (!sigmaFormula) {
        // Fallback: pre-strip ${TABLE}. and ${view.field} refs to bare uppercase
        // identifiers, then run the generic expression converter. Required so
        // function-call formulas like CONCAT('x', ${other_view.field}) emit a
        // working Sigma formula (Concat("x", [Other Field])) — these calc cols
        // are then moved to a derived element by the cross-element pass below.
        const stripped = expandedSql
          .replace(/\$\{TABLE\}\./gi, '')
          .replace(/\$\{[^.}]+\.([^}]+)\}/g, (_: string, f: string) => f.toUpperCase())
          .replace(/\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g, (_: string, n: string) => n.toUpperCase())
          .replace(/[\r\n]+\s*/g, ' ')
          .trim();
        if (/^[A-Za-z_][A-Za-z0-9_]*\s*\(/.test(stripped)) {
          sigmaFormula = lookConvertExpression(stripped);
        }
      }
      if (sigmaFormula) {
        element.columns.push({ id: colId, formula: sigmaFormula, name: d.label || sigmaDisplayName(d._name), ...(dFormat ? { format: dFormat } : {}) });
        element.order.push(colId);
        warnings.push(`ℹ "${d._name}" → calculated column: ${sigmaFormula}`);
      } else {
        element.columns.push({ id: colId, formula: `[${tableName}/${sigmaDisplayName(colName)}]`, name: d.label || sigmaDisplayName(d._name), ...(dFormat ? { format: dFormat } : {}) });
        element.order.push(colId);
        warnings.push(`⚠ "${d._name}": could not auto-convert. Edit formula manually.`);
      }
      continue;
    }

    const sqlCol = lookStripSql(d.sql) || colName;
    const physicalCol = sqlCol.split('.').pop()!.replace(/"/g, '').toUpperCase();

    // Dedup: if physical column already exists, just map the dimension name
    if (colIdMap[physicalCol]) {
      colIdMap[colName] = colIdMap[physicalCol];
      continue;
    }

    const colId = makeColId(physicalCol);
    colIdMap[colName] = colId;
    colIdMap[physicalCol] = colId;
    element.columns.push({ id: colId, formula: `[${tableName}/${colLabel(physicalCol)}]`, ...(dFormat ? { format: dFormat } : {}) });
    element.order.push(colId);
  }

  // Dimension groups (time) — expand into raw + DateTrunc timeframe columns
  const TIMEFRAME_MAP: Record<string, { suffix: string; formula: (ref: string) => string }> = {
    raw:     { suffix: 'Raw',     formula: ref => ref },
    time:    { suffix: 'Time',    formula: ref => ref },
    date:    { suffix: 'Date',    formula: ref => `DateTrunc("day", ${ref})` },
    week:    { suffix: 'Week',    formula: ref => `DateTrunc("week", ${ref})` },
    month:   { suffix: 'Month',   formula: ref => `DateTrunc("month", ${ref})` },
    quarter: { suffix: 'Quarter', formula: ref => `DateTrunc("quarter", ${ref})` },
    year:    { suffix: 'Year',    formula: ref => `DateTrunc("year", ${ref})` },
  };
  const DEFAULT_TIMEFRAMES = ['raw', 'time', 'date', 'week', 'month', 'quarter', 'year'];

  const dimGroups = view.dimension_group ? (Array.isArray(view.dimension_group) ? view.dimension_group : [view.dimension_group]) : [];
  dimGroups.forEach((dg: any) => {
    if (!dg._name) return;
    const colName = dg._name.toUpperCase();
    const dgType = (dg.type || 'time').toLowerCase();

    // ── type: duration ──────────────────────────────────────────────────────
    if (dgType === 'duration') {
      if (!dg.sql_start || !dg.sql_end) {
        warnings.push(`⚠ Duration group "${dg._name}": missing sql_start/sql_end — skipped.`);
        return;
      }
      const normStart = (dg.sql_start || '').replace(/\$\{TABLE\}\s*\.\s*/gi, '').trim();
      const normEnd   = (dg.sql_end   || '').replace(/\$\{TABLE\}\s*\.\s*/gi, '').trim();
      const startCol  = ((normStart.match(/^([A-Za-z_][A-Za-z0-9_]*)/) || ['', ''])[1]).toUpperCase()
                        || lookStripSql(dg.sql_start).split('.').pop()!.replace(/"/g, '').toUpperCase();
      const endCol    = ((normEnd.match(/^([A-Za-z_][A-Za-z0-9_]*)/) || ['', ''])[1]).toUpperCase()
                        || lookStripSql(dg.sql_end).split('.').pop()!.replace(/"/g, '').toUpperCase();
      // ensure the start/end physical columns exist on the element (the DateDiff
      // references them) — otherwise the duration columns dangle.
      for (const pc of [startCol, endCol]) {
        if (pc && !colIdMap[pc]) {
          const cid = makeColId(pc);
          colIdMap[pc] = cid;
          element.columns.push({ id: cid, formula: `[${tableName}/${colLabel(pc)}]` });
          element.order.push(cid);
        }
      }
      const startRef  = `[${tableName}/${colLabel(startCol)}]`;
      const endRef    = `[${tableName}/${colLabel(endCol)}]`;
      const DG_DURATION: Record<string, string> = {
        second: 'second', minute: 'minute', hour: 'hour',
        day: 'day', week: 'week', month: 'month', quarter: 'quarter', year: 'year'
      };
      const intervals: string[] = Array.isArray(dg.intervals)
        ? dg.intervals.map((i: any) => String(i).toLowerCase())
        : ['day'];
      const folderItems: string[] = [];
      intervals.forEach((interval: string) => {
        const prec = DG_DURATION[interval];
        if (!prec) return;
        const durColId = sigmaShortId();
        const durColName = `${colName}_${interval.toUpperCase()}S`;
        colIdMap[durColName] = durColId;
        element.columns.push({
          id: durColId,
          formula: `DateDiff("${prec}", ${startRef}, ${endRef})`,
          name: sigmaDisplayName(durColName)
        });
        element.order.push(durColId);
        folderItems.push(durColId);
      });
      if (folderItems.length > 0) {
        if (!(element as any).folders) (element as any).folders = [];
        (element as any).folders.push({
          id: sigmaShortId(),
          name: sigmaDisplayName(dg._name),
          items: folderItems
        });
      }
      return;
    }

    // ── type: time (default) ────────────────────────────────────────────────
    // Detect LookML parameter substitution — can't be resolved statically
    if (/\$\{[^.}]+\}/.test(dg.sql || '') && !/\$\{TABLE\}/i.test(dg.sql || '')) {
      warnings.push(`⚠ "${dg._name}": uses LookML parameter substitution — skipped. Add this dimension manually after configuring parameters in Sigma.`);
      return;
    }
    if (lookIsComplexSql(dg.sql)) {
      warnings.push(`⚠ Dimension group "${dg._name}": complex expression — skipped.`);
      return;
    }
    const sqlCol = lookStripSql(dg.sql) || colName;
    const physicalCol = sqlCol.split('.').pop()!.replace(/"/g, '').toUpperCase();

    // Determine which timeframes to expand
    const rawTimeframes: string[] = dg.timeframes
      ? (Array.isArray(dg.timeframes) ? dg.timeframes : [dg.timeframes]).map((t: any) => (t.field || t).toLowerCase())
      : DEFAULT_TIMEFRAMES;
    const timeframes = rawTimeframes.filter(t => TIMEFRAME_MAP[t]);

    const displayBase = sigmaDisplayName(dg._name);
    const colRef = `[${tableName}/${colLabel(physicalCol)}]`;

    // Raw column — primary ID for this physical column
    const rawColId = makeColId(physicalCol);
    colIdMap[colName] = rawColId;
    colIdMap[physicalCol] = rawColId;

    if (timeframes.length <= 1) {
      // No expansion needed — just emit raw column
      element.columns.push({ id: rawColId, formula: colRef });
      element.order.push(rawColId);
      return;
    }

    // Folder to group the timeframes
    const folderItems: string[] = [];

    timeframes.forEach(tf => {
      const { suffix, formula } = TIMEFRAME_MAP[tf];
      const tfFormula = formula(colRef);
      const tfName = `${displayBase} ${suffix}`;
      if (tf === 'raw' || tf === 'time') {
        // Raw/time: emit the physical column itself
        colIdMap[`${colName}_${tf.toUpperCase()}`] = rawColId;
        element.columns.push({ id: rawColId, formula: colRef, name: tfName });
        folderItems.push(rawColId);
      } else {
        const tfId = sigmaShortId();
        element.columns.push({ id: tfId, formula: tfFormula, name: tfName });
        folderItems.push(tfId);
        element.order.push(tfId);
      }
    });

    // Add folder to group timeframes
    if (!(element as any).folders) (element as any).folders = [];
    (element as any).folders.push({ id: sigmaShortId(), name: displayBase, items: folderItems });
    element.order.push(rawColId);
  });

  // Measures → metrics
  const measures = view.measure ? (Array.isArray(view.measure) ? view.measure : [view.measure]) : [];
  const CALC_COL_MEASURE_TYPES = new Set(['running_total', 'percent_of_total']);

  // Pre-pass: resolve each non-computed measure's Sigma aggregate formula so that
  // ratio/number measures referencing other measures (e.g. ${total_revenue} /
  // ${order_count}) can substitute them. Avoids leaking ${...} tokens / fabricating
  // phantom columns from arithmetic expressions.
  const measurePhysCol = (ms: any): string => {
    const resolved = (ms.sql || '').replace(
      /\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g,
      (m: string, r: string) => dimPhysColMap.get(r.toLowerCase()) ?? m);
    const sc = lookStripSql(resolved) || (ms._name || '').toUpperCase();
    return sc.split('.').pop()!.replace(/"/g, '').toUpperCase();
  };
  const filterCondition = (ms: any): string | null => {
    const fl = Array.isArray(ms.filters) ? ms.filters : (ms.filters ? [ms.filters] : []);
    const conds: string[] = [];
    for (const f of fl) {
      if (typeof f !== 'object' || !f) continue;
      const ff = f.field || f._name, fv = f.value;
      if (!ff || fv == null) continue;
      const dn = colLabel(ff.replace(/^.*\./, '').toUpperCase());
      if (fv === 'yes' || fv === 'true') conds.push(`[${dn}] = True`);
      else if (fv === 'no' || fv === 'false') conds.push(`[${dn}] = False`);
      else conds.push(`[${dn}] = "${fv}"`);
    }
    if (!conds.length) return null;
    return conds.length === 1 ? conds[0] : conds.map(c => `(${c})`).join(' And ');
  };
  const simpleMeasureFormula = (ms: any): string | null => {
    const t = (ms.type || 'count').toLowerCase();
    const dn = colLabel(measurePhysCol(ms));
    const cond = filterCondition(ms);
    if (cond) {
      const m: Record<string, string> = {
        sum: `SumIf([${dn}], ${cond})`, count: `CountIf(${cond})`,
        count_distinct: `CountDistinctIf([${dn}], ${cond})`, average: `AvgIf([${dn}], ${cond})`,
        max: `MaxIf([${dn}], ${cond})`, min: `MinIf([${dn}], ${cond})`,
      };
      return m[t] || `SumIf([${dn}], ${cond})`;
    }
    if (t === 'count') return 'Count()';
    if (t === 'count_distinct') return `CountDistinct([${dn}])`;
    if (t === 'percentile') return `Percentile([${dn}], ${(Number(ms.percentile) || 50) / 100})`;
    if (['sum', 'average', 'median', 'min', 'max', 'average_distinct', 'sum_distinct', 'list'].includes(t))
      return lookSigmaMetric(t, measurePhysCol(ms));
    return null; // number/computed/ratio — resolved in the main pass
  };
  const measureSigmaFormula = new Map<string, string>();
  measures.forEach((ms: any) => {
    if (!ms._name) return;
    const f = simpleMeasureFormula(ms);
    if (f) measureSigmaFormula.set(ms._name.toLowerCase(), f);
  });

  measures.forEach((ms: any) => {
    if (!ms._name) return;
    const msName = ms._name.toUpperCase();
    // Resolve intra-view ${dimension} references to the underlying physical
    // column before deriving it (e.g. ${sale_price} → SALE_PRICE). Without this,
    // bare ${field} tokens leak into formulas and fabricate phantom ${...}
    // columns. (${TABLE}.col and ${view.field} forms are handled by lookStripSql.)
    const resolvedMsSql = (ms.sql || '').replace(
      /\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g,
      (match: string, refName: string) => dimPhysColMap.get(refName.toLowerCase()) ?? match
    );
    const sqlCol = lookStripSql(resolvedMsSql) || msName;
    const physicalCol = sqlCol.split('.').pop()!.replace(/"/g, '').toUpperCase() || msName.replace(/"/g, '');
    const msType = (ms.type || 'count').toLowerCase();
    const msLabel = ms.label || sigmaDisplayName(msName);
    // LookML number format (value_format_name / value_format) → Sigma format obj.
    // percent_of_total has no LookML format but is inherently a percentage.
    const msFormat = lookmlFieldFormat(ms, warnings)
      ?? (msType === 'percent_of_total' ? { kind: 'number', formatString: ',.1%' } : undefined);

    // running_total / percent_of_total → calculated columns
    if (CALC_COL_MEASURE_TYPES.has(msType)) {
      if (!colIdMap[physicalCol]) {
        const colId = makeColId(physicalCol);
        colIdMap[physicalCol] = colId;
        element.columns.push({ id: colId, formula: `[${tableName}/${colLabel(physicalCol)}]` });
        element.order.push(colId);
      }
      const dn = colLabel(physicalCol);
      const calcId = sigmaShortId();
      if (msType === 'running_total') {
        element.columns.push({ id: calcId, formula: `CumulativeSum([${dn}])`, name: msLabel, ...(msFormat ? { format: msFormat } : {}) });
        warnings.push(`✅ "${ms._name}" (running_total) → CumulativeSum([${dn}])`);
      } else {
        element.columns.push({ id: calcId, formula: `Sum([${dn}]) / GrandTotal(Sum([${dn}]))`, name: msLabel, ...(msFormat ? { format: msFormat } : {}) });
        warnings.push(`✅ "${ms._name}" (percent_of_total) → Sum/GrandTotal`);
      }
      element.order.push(calcId);
      return;
    }

    // Computed / ratio measures: sql references OTHER measures (${measure}) or is a
    // complex arithmetic expression. Substitute each ${measure} with its Sigma
    // aggregate formula and map SQL funcs → Sigma; emit as a metric (NOT a column).
    const measureRefs = [...((ms.sql || '') as string).matchAll(/\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g)]
      .map(mm => mm[1].toLowerCase());
    const refsOtherMeasure = measureRefs.some(r => measureSigmaFormula.has(r));
    const isComputed = !ms.filters && (refsOtherMeasure ||
      (msType === 'number' && ms.sql && lookIsComplexSql(ms.sql)));
    if (isComputed) {
      let expr = (ms.sql || '') as string;
      expr = expr.replace(/\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g, (m: string, r: string) => {
        const mf = measureSigmaFormula.get(r.toLowerCase());
        if (mf) return `(${mf})`;
        const phys = dimPhysColMap.get(r.toLowerCase());
        return phys ? `[${colLabel(phys.toUpperCase())}]` : m;
      });
      // map common SQL funcs to Sigma (decimals/operators pass through untouched)
      expr = expr.replace(/\bNULLIF\s*\(/gi, 'NullIf(')
                 .replace(/\b(COALESCE|NVL|IFNULL)\s*\(/gi, 'Coalesce(')
                 .replace(/\b(IFF|IIF)\s*\(/gi, 'If(');
      element.metrics!.push({ id: sigmaShortId(), formula: expr.trim(), name: msLabel, ...(msFormat ? { format: msFormat } : {}) });
      warnings.push(`✅ "${ms._name}" (computed) → ${expr.trim().slice(0, 70)}`);
      return;
    }

    // Filtered measures → conditional aggregates
    if (ms.filters && (Array.isArray(ms.filters) ? ms.filters.length : false)) {
      const filters = Array.isArray(ms.filters) ? ms.filters : [];
      const conditions: string[] = [];
      for (const f of filters) {
        if (typeof f !== 'object' || !f) continue;
        const fField = f.field || f._name;
        const fVal = f.value;
        if (fField && fVal) {
          const cleanField = fField.replace(/^.*\./, '').toUpperCase();
          const dn = colLabel(cleanField);
          if (!colIdMap[cleanField]) {
            const colId = makeColId(cleanField);
            colIdMap[cleanField] = colId;
            element.columns.push({ id: colId, formula: `[${tableName}/${dn}]` });
            element.order.push(colId);
          }
          if (fVal === 'yes' || fVal === 'true') conditions.push(`[${dn}] = True`);
          else if (fVal === 'no' || fVal === 'false') conditions.push(`[${dn}] = False`);
          else conditions.push(`[${dn}] = "${fVal}"`);
        }
      }
      if (conditions.length > 0) {
        const condition = conditions.length === 1 ? conditions[0] : conditions.map(c => `(${c})`).join(' And ');
        // A filtered `count` (CountIf) references only the condition columns, not a
        // value column — don't fabricate a physical column from the measure name.
        if (msType !== 'count' && !colIdMap[physicalCol]) {
          const colId = makeColId(physicalCol);
          colIdMap[physicalCol] = colId;
          element.columns.push({ id: colId, formula: `[${tableName}/${colLabel(physicalCol)}]` });
          element.order.push(colId);
        }
        const dn = colLabel(physicalCol);
        const condAggMap: Record<string, string> = {
          sum: `SumIf([${dn}], ${condition})`, count: `CountIf(${condition})`,
          count_distinct: `CountDistinctIf([${dn}], ${condition})`, average: `AvgIf([${dn}], ${condition})`,
          max: `MaxIf([${dn}], ${condition})`, min: `MinIf([${dn}], ${condition})`,
        };
        const formula = condAggMap[msType] || `SumIf([${dn}], ${condition})`;
        element.metrics!.push({ id: sigmaShortId(), formula, name: msLabel, ...(msFormat ? { format: msFormat } : {}) });
        warnings.push(`✅ Filtered "${ms._name}" → ${formula.slice(0, 60)}`);
        return;
      }
      warnings.push(`⚠ "${ms._name}": filters not parsed — metric created without filter`);
    }

    if (msType === 'count') {
      element.metrics!.push({ id: sigmaShortId(), formula: 'Count()', name: msLabel, ...(msFormat ? { format: msFormat } : {}) });
    } else if (msType === 'count_distinct') {
      const cdCol = physicalCol && physicalCol !== msName ? physicalCol : msName;
      if (!colIdMap[cdCol]) {
        const colId = makeColId(cdCol);
        colIdMap[cdCol] = colId;
        element.columns.push({ id: colId, formula: `[${tableName}/${colLabel(cdCol)}]` });
        element.order.push(colId);
      }
      element.metrics!.push({ id: sigmaShortId(), formula: `CountDistinct([${colLabel(cdCol)}])`, name: msLabel, ...(msFormat ? { format: msFormat } : {}) });
    } else {
      if (!colIdMap[physicalCol]) {
        const colId = makeColId(physicalCol);
        colIdMap[physicalCol] = colId;
        element.columns.push({ id: colId, formula: `[${tableName}/${colLabel(physicalCol)}]` });
        element.order.push(colId);
      }
      const dn = colLabel(physicalCol);
      const formula = msType === 'percentile'
        ? `Percentile([${dn}], ${(Number(ms.percentile) || 50) / 100})`
        : lookSigmaMetric(msType, physicalCol);
      element.metrics!.push({ id: sigmaShortId(), formula, name: msLabel, ...(msFormat ? { format: msFormat } : {}) });
    }
  });

  if (element.metrics!.length === 0) delete element.metrics;
  return { element, elementId, colIdMap };
}

// ── Main LookML Conversion ───────────────────────────────────────────────────

export interface LookMLConvertOptions {
  connectionId?: string;
  exploreName?: string;
  joinStrategy?: 'relationships' | 'joins' | 'auto';
}

export function convertLookMLToSigma(
  files: { name: string; content: string }[],
  options: LookMLConvertOptions = {}
): ConversionResult {
  resetIds();

  const { connectionId = '', joinStrategy = 'auto' } = options;

  // Parse all files
  const views: Record<string, any> = {};
  const explores: Record<string, any> = {};
  const warnings: string[] = [];
  const security: SecurityRule[] = [];   // detected RLS/CLS — reported, not injected (architecture B)

  for (const file of files) {
    const isModel = file.name.endsWith('.model.lkml') || file.name.includes('.model.');
    try {
      const parsed = parseLookML(file.content);
      if (isModel) {
        parsed.explores.forEach((ex: any) => { explores[ex._name] = ex; });
      }
      parsed.views.forEach((v: any) => { views[v._name] = v; });
      // Gap 2: warn on include: directives — cross-file resolution is not supported
      if (parsed.includes.length > 0) {
        warnings.push(`ℹ "${file.name}": contains include: directive(s) — ${parsed.includes.join(', ')} — cross-file resolution is not supported. Pass all referenced view files explicitly.`);
      }
    } catch (e: any) {
      throw new Error(`Parse error in ${file.name}: ${e.message}`);
    }
  }

  // Determine which explore to convert
  let exploreName = options.exploreName;
  const exploreNames = Object.keys(explores);
  if (!exploreName) {
    if (exploreNames.length === 1) exploreName = exploreNames[0];
    else if (exploreNames.length === 0) throw new Error('No explores found in the LookML files. Upload a .model.lkml file.');
    else throw new Error(`Multiple explores found: ${exploreNames.join(', ')}. Specify exploreName.`);
  }

  const explore = explores[exploreName];
  if (!explore) throw new Error(`Explore "${exploreName}" not found. Available: ${exploreNames.join(', ')}`);

  // Gap 1: pre-compute resolved table paths for ${view.SQL_TABLE_NAME} substitution
  const sqlTableNameMap = buildSqlTableNameMap(views);

  const strategy = joinStrategy;

  // Build join list
  const baseViewName = explore.from || exploreName;
  const baseAlias = exploreName;
  const isBaseView = (name: string) => name === baseAlias || name === baseViewName;

  const joinDefs: any[] = [];
  const joinsRaw = explore.join ? (Array.isArray(explore.join) ? explore.join : [explore.join]) : [];

  joinsRaw.forEach((j: any) => {
    const alias = j._name || j.join;
    const viewName = j.from || alias;
    const rel = (j.relationship || 'many_to_one').toLowerCase();
    const jType = (j.type || 'left_outer').toLowerCase().replace('_join', '').replace(' ', '_');

    const sqlOn = j.sql_on || '';
    const keyMatch = sqlOn.match(/\$\{(\w+)\.(\w+)\}\s*=\s*\$\{(\w+)\.(\w+)\}/);
    const keys = keyMatch ? [{
      leftView: keyMatch[1], leftCol: keyMatch[2].toUpperCase(),
      rightView: keyMatch[3], rightCol: keyMatch[4].toUpperCase()
    }] : [];

    if (!keyMatch && sqlOn) {
      const isRangeJoin = /\$\{[^}]+\}\s*[><!]|[><!]=?\s*\$\{/.test(sqlOn);
      if (isRangeJoin) {
        warnings.push(`⚠ Join "${alias}": uses range-based sql_on (>=, <=, >, <) which cannot be expressed as a Sigma relationship. Recreate this as a filtered join or custom SQL after import.`);
      } else {
        warnings.push(`⚠ Join "${alias}": complex sql_on could not be parsed automatically — add join keys manually in Sigma's ERD view`);
      }
    }

    joinDefs.push({ alias, viewName, rel, joinType: jType, keys });
  });

  const needsPhysical = (j: any): boolean => {
    if (strategy === 'joins') return true;
    if (strategy === 'relationships') return false;
    // many_to_many is intentionally NOT forced to a physical join here. Sigma
    // has no native M:N relationship, but dropping the join entirely would lose
    // the dimension columns and risk dangling cross-element refs. Instead we
    // keep it on the relationship path and map it to the closest Sigma type
    // (N:1) with a warning recommending a bridge table (see wiring below).
    return j.rel === 'one_to_many' || j.joinType === 'full_outer';
  };

  const relJoins = joinDefs.filter(j => !needsPhysical(j));

  // Build elements
  const elementMap: Record<string, ElementResult> = {};
  const physViewMap: Record<string, ElementResult> = {};

  const ndtContext: NdtContext = { views, explores };
  const baseResult = lookConvertView(baseViewName, views[baseViewName], connectionId, warnings, sqlTableNameMap, ndtContext);
  elementMap[baseAlias] = baseResult;
  physViewMap[baseViewName] = baseResult;
  if (baseAlias !== baseViewName) elementMap[baseViewName] = baseResult;

  for (const j of joinDefs) {
    if (!physViewMap[j.viewName]) {
      const res = lookConvertView(j.viewName, views[j.viewName], connectionId, warnings, sqlTableNameMap, ndtContext);
      physViewMap[j.viewName] = res;
    }
    elementMap[j.alias] = physViewMap[j.viewName];
    if (!elementMap[j.viewName]) elementMap[j.viewName] = physViewMap[j.viewName];
  }

  // Wire relationships.
  // A join's sql_on names the joined ("target") view on one side and the
  // FK-owning ("source") view on the other. The source is NOT always the base
  // explore view: in a snowflake schema the FK can live on another joined view
  // (e.g. ${inventory_items.product_id} = ${products.id} hangs the products
  // relationship off the inventory_items element, not the base fact). Attach
  // each relationship to the element that actually owns the FK column.
  const usedTargetCols = new Set<string>();

  relJoins.forEach(j => {
    const targetRes = elementMap[j.alias] || elementMap[j.viewName];
    if (!targetRes) {
      warnings.push(`⚠ Relationship "${j.alias}": target not found`);
      return;
    }
    const isTargetView = (name: string) => name === j.alias || name === j.viewName;

    j.keys.forEach((k: any) => {
      // Identify which side of the equality is this join's target view; the
      // other side owns the FK and becomes the relationship source.
      let srcView: string, srcCol: string, tgtCol: string;
      if (isTargetView(k.rightView)) {
        srcView = k.leftView; srcCol = k.leftCol; tgtCol = k.rightCol;
      } else if (isTargetView(k.leftView)) {
        srcView = k.rightView; srcCol = k.rightCol; tgtCol = k.leftCol;
      } else {
        // Neither side names the joined view — fall back to the base element.
        warnings.push(`⚠ Relationship "${j.alias}": sql_on does not reference the joined view directly — wired from the base element; verify keys in Sigma.`);
        const baseIsLeft = isBaseView(k.leftView);
        srcView = baseAlias;
        srcCol = baseIsLeft ? k.leftCol : k.rightCol;
        tgtCol = baseIsLeft ? k.rightCol : k.leftCol;
      }

      const srcRes = elementMap[srcView];
      if (!srcRes) {
        warnings.push(`⚠ Relationship "${j.alias}": source view "${srcView}" not found in the explore — skipped`);
        return;
      }

      const srcColId = lookFindColId(srcRes, srcCol);
      const tgtColId = lookFindColId(targetRes, tgtCol);
      if (!srcColId || !tgtColId) {
        warnings.push(`⚠ Relationship "${j.alias}": could not resolve column IDs for keys (${k.leftCol} / ${k.rightCol})`);
        return;
      }

      const pairKey = `${targetRes.elementId}|${tgtColId}`;
      if (usedTargetCols.has(pairKey)) {
        warnings.push(`ℹ Role-playing join "${j.alias}" shares a physical table — add manually in Sigma.`);
        return;
      }
      usedTargetCols.add(pairKey);

      // Map the LookML relationship cardinality to the closest Sigma type.
      // Sigma supports N:1 / 1:1 / 1:N — there is no native many_to_many. For
      // M:N we emit the closest functional approximation (N:1) and warn that a
      // bridge/junction table is needed for correct fan-out behaviour.
      let relType: 'N:1' | '1:1' | '1:N' = 'N:1';
      if (j.rel === 'one_to_one') relType = '1:1';
      else if (j.rel === 'many_to_many') {
        relType = 'N:1';
        warnings.push(`⚠ Relationship "${j.alias}": LookML relationship is many_to_many, which Sigma does not support natively. Mapped to the closest type (N:1) — verify cardinality and introduce a bridge/junction table if the join can fan out on both sides (otherwise aggregates may double-count).`);
      }

      const srcEl = srcRes.element;
      if (!srcEl.relationships) srcEl.relationships = [];
      srcEl.relationships.push({
        id: sigmaShortId(),
        targetElementId: targetRes.elementId,
        keys: [{ sourceColumnId: srcColId, targetColumnId: tgtColId }],
        name: j.alias,
        relationshipType: relType
      });
    });
  });

  // Collect unique elements — dims first, fact (with relationships) last
  const seenIds = new Set<string>();
  let allElements = Object.values(physViewMap).filter(r => {
    if (seenIds.has(r.elementId)) return false;
    seenIds.add(r.elementId);
    return true;
  }).map(r => r.element);

  allElements.sort((a, b) => {
    const aHasRel = !!(a.relationships && a.relationships.length > 0);
    const bHasRel = !!(b.relationships && b.relationships.length > 0);
    if (aHasRel === bHasRel) return 0;
    return aHasRel ? 1 : -1;
  });

  // ── LookML access_filter → Sigma row-level security (spec-expressible) ────
  // access_filter implements row-level security: each row is restricted by
  // matching a column against the value of a Looker user_attribute. This IS
  // expressible in the Sigma data-model spec (verified live end-to-end): on the
  // element that owns the field we add a boolean calc column
  //   RLS: <Field> = CurrentUserAttributeText("<attr>") = [<Field display name>]
  // and an element-level `filters` entry on that calc column that keeps only
  // rows where it is True (kind:list, mode:include, values:[true]) — fail-closed
  // RLS. We never silently drop it, and we emit an actionable informational
  // warning so the user provisions/reuses the matching Sigma user attribute.
  const accessFilters: any[] = explore.access_filter
    ? (Array.isArray(explore.access_filter) ? explore.access_filter : [explore.access_filter])
    : [];
  for (const af of accessFilters) {
    const field = af.field || '(unspecified field)';
    const ua = af.user_attribute || '(unspecified user_attribute)';

    // Resolve the element + column the access_filter restricts.
    const dotIdx = field.lastIndexOf('.');
    const viewPart = dotIdx >= 0 ? field.slice(0, dotIdx) : baseViewName;
    const fieldPart = (dotIdx >= 0 ? field.slice(dotIdx + 1) : field).toUpperCase();
    const targetRes = elementMap[viewPart] || elementMap[baseAlias];

    const colId = targetRes ? lookFindColId(targetRes, fieldPart) : null;
    if (!targetRes || !colId) {
      warnings.push(`⚠ Explore "${exploreName}": access_filter restricts "${field}" by user_attribute "${ua}" (row-level security), but column "${field}" was not found in the converted model — re-apply this RLS rule manually in Sigma (boolean calc column CurrentUserAttributeText("${ua}") = [<field>] + an element filter keeping only True).`);
      continue;
    }

    // Display name to reference inside the calc column. A physical dimension has
    // no explicit `name` — its display name is the trailing segment of its
    // [TABLE/Display] formula; a calc column carries its `name` directly.
    const targetEl: any = targetRes.element;
    const ownerCol = (targetEl.columns || []).find((c: any) => c.id === colId);
    let dispName = ownerCol?.name;
    if (!dispName && ownerCol?.formula) {
      const fm = String(ownerCol.formula).match(/^\[(?:[^\]\/]+\/)?([^\]]+)\]$/);
      if (fm) dispName = fm[1];
    }
    if (!dispName) dispName = sigmaDisplayName(fieldPart);

    // REPORT the RLS (architecture B) — do NOT inject into the model. A stateless
    // converter can't create/assign the Sigma user attribute or POST; injecting a
    // CurrentUserAttributeText filter standalone fail-closes to 0 rows. The skill
    // (apply_sigma_rls.py) provisions the attribute then applies the calc+filter.
    security.push(makeRlsSecurity({
      source: `Looker access_filter (explore "${exploreName}")`,
      element: targetEl,
      name: `RLS: ${dispName}`,
      formula: `CurrentUserAttributeText("${ua}") = [${dispName}]`,
    }));
    warnings.push(`🔐 Explore "${exploreName}": access_filter "${field}" by user_attribute "${ua}" → row-level security DETECTED (reported in result.security, not injected). The migration skill provisions/reuses the Sigma user attribute "${ua}" and applies the RLS calc + filter.`);
  }

  // ── LookML always_filter → Sigma element filters ─────────────────────────
  const alwaysFilterItems: any[] = explore.always_filter?.filters
    ? (Array.isArray(explore.always_filter.filters) ? explore.always_filter.filters : [explore.always_filter.filters])
    : [];

  for (const af of alwaysFilterItems) {
    const fieldRef: string = af.field || '';
    const expr: string = (af.value || '').trim();
    if (!fieldRef || !expr) continue;

    const dotIdx = fieldRef.lastIndexOf('.');
    const viewPart = dotIdx >= 0 ? fieldRef.slice(0, dotIdx) : baseViewName;
    const fieldPart = (dotIdx >= 0 ? fieldRef.slice(dotIdx + 1) : fieldRef).toUpperCase();

    const targetRes = elementMap[viewPart] || elementMap[baseAlias];
    if (!targetRes) {
      warnings.push(`⚠ always_filter "${fieldRef}": view "${viewPart}" not found — filter skipped`);
      continue;
    }

    // Try exact match then strip timeframe suffix
    const colId = lookFindColId(targetRes, fieldPart)
      || lookFindColId(targetRes, fieldPart.replace(/_(?:RAW|TIME|DATE|WEEK|MONTH|QUARTER|YEAR)$/, ''));
    if (!colId) {
      warnings.push(`⚠ always_filter "${fieldRef}": column "${fieldPart}" not found in element — filter skipped`);
      continue;
    }

    const sigmaFilter = lookParseFilterExpr(expr, colId);
    if (!sigmaFilter) {
      warnings.push(`⚠ always_filter "${fieldRef}" = "${expr}": date/range expression cannot be auto-converted — add filter manually in Sigma`);
      continue;
    }

    const targetEl: any = targetRes.element;
    if (!targetEl.filters) targetEl.filters = [];
    targetEl.filters.push(sigmaFilter);
    warnings.push(`✅ always_filter "${fieldRef}" = "${expr}" → element list filter added`);
  }

  if (!connectionId) warnings.unshift('⚠ Connection ID not set — update in JSON before saving to Sigma');

  // ── Pull cross-element calc cols off source elements (moved to derived) ─
  // Calc cols whose formula references a related-table column by display name
  // cannot resolve on the source warehouse-table element — Sigma doesn't see
  // those names in scope. Mirror the smm browser tool's
  // buildDerivedElementsAndMoveCalcs pass: pull them off the fact element
  // here, then re-place them on the derived "<Table> View" element with
  // [BaseElement/REL_NAME/Field] cross-element refs after buildDerivedElements.
  // (Reference: src/tableau.ts:1993-2129 — beads-sigma-047 port.)
  const crossElCalcsByElId: Record<string, any[]> = {};
  for (const el of allElements) {
    if (el.source?.kind !== 'warehouse-table') continue;
    if (!(el as any).relationships?.length) continue;

    const localNames = new Set<string>();
    for (const c of (el.columns || [])) {
      if (!c.formula) continue;
      const m = c.formula.match(/^\[[^\]\/]+\/([^\]]+)\]$/);
      if (m) localNames.add(m[1].toUpperCase());
      if ((c as any).name) localNames.add((c as any).name.toUpperCase());
    }

    const crossEl: any[] = [];
    const keep: any[] = [];
    for (const c of (el.columns || [])) {
      if (!(c as any).name || !c.formula) { keep.push(c); continue; }
      if (/^\[[^\]\/]+\/[^\]]+\]$/.test(c.formula)) { keep.push(c); continue; }
      const refs = c.formula.match(/\[([^\]\/]+)\]/g) || [];
      const hasCross = refs.some(ref => {
        const n = ref.replace(/^\[|\]$/g, '');
        return !/^(true|false|null)$/i.test(n) && !localNames.has(n.toUpperCase());
      });
      if (hasCross) {
        const oi = (el.order || []).indexOf((c as any).id);
        if (oi >= 0) (el.order as string[]).splice(oi, 1);
        crossEl.push(c);
      } else {
        keep.push(c);
      }
    }
    el.columns = keep;
    if (crossEl.length) crossElCalcsByElId[el.id] = crossEl;
  }

  // Build derived (browsable) elements for each fact element that has relationships.
  // Each derived element sources from the fact warehouse element and surfaces all
  // its own columns plus cross-element [TABLE/REL_NAME/Col] refs for joined dims.
  const derivedElements = buildDerivedElements(allElements);
  allElements = [...allElements, ...derivedElements];

  // Place cross-element calc cols (pulled from source above) onto their
  // matching derived element, rewriting bare [X] refs to [SRC/REL/X] form.
  const placedSrcElIds: Record<string, boolean> = {};
  for (const de of derivedElements) {
    if (de.source?.kind !== 'table' || !(de.source as any).elementId) continue;
    const srcElId = (de.source as any).elementId;
    const calcs = crossElCalcsByElId[srcElId];
    if (!calcs?.length) continue;
    const srcEl = allElements.find(e => e.id === srcElId);
    if (!srcEl) continue;
    const srcPath = (srcEl.source?.kind === 'warehouse-table' ? (srcEl.source.path as string[]) : []) || [];
    const srcBaseName = (srcEl as any).name || (srcPath.length ? srcPath[srcPath.length - 1] : '');
    const relatedNameMap: Record<string, string> = {};
    if (srcEl && (srcEl as any).relationships && srcBaseName) {
      for (const rel of ((srcEl as any).relationships || [])) {
        if (!rel.name) continue;
        const tgtEl = allElements.find(e => e.id === rel.targetElementId);
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
    }
    for (const c of calcs) {
      if (c.formula && Object.keys(relatedNameMap).length) {
        c.formula = c.formula.replace(/\[([^\]\/]+)\]/g, (match: string, refName: string) => {
          const rewritten = relatedNameMap[refName];
          return rewritten ? `[${rewritten}]` : match;
        });
      }
      (de.columns as any[]).push(c);
      (de.order as string[]).push(c.id);
    }
    warnings.push(`ℹ ${calcs.length} calc col(s) moved to derived "${(de as any).name}" (cross-element refs)`);
    placedSrcElIds[srcElId] = true;
  }
  // Drop calcs that referenced cross-element cols but had no derived element
  for (const elId of Object.keys(crossElCalcsByElId)) {
    if (placedSrcElIds[elId]) continue;
    for (const c of crossElCalcsByElId[elId]) {
      warnings.push(`⚠ "${c.name}" cross-element refs but no derived element — column dropped`);
    }
  }

  const sigmaModel = {
    name: sigmaDisplayName(exploreName),
    schemaVersion: 1,
    pages: [{ id: sigmaShortId(), name: 'Page 1', elements: allElements }]
  };

  const totalCols = allElements.reduce((s, e) => s + (e.columns?.length || 0), 0);
  const totalMetrics = allElements.reduce((s, e) => s + (e.metrics?.length || 0), 0);
  const totalRels = allElements.reduce((s, e) => s + (e.relationships?.length || 0), 0);

  return {
    model: sigmaModel,
    warnings,
    ...(security.length ? { security } : {}),
    stats: {
      views: Object.keys(views).length,
      explores: Object.keys(explores).length,
      elements: allElements.length,
      columns: totalCols,
      metrics: totalMetrics,
      relationships: totalRels
    }
  };
}

/**
 * Build "derived" (browsable) elements for each fact element that has relationships.
 *
 * The data model manager surfaces joined dimension columns via cross-element
 * [TABLE/REL_NAME/Col] formulas in a derived element that sources from the fact.
 * This mirrors the UI's native element, making the model immediately usable in Sigma.
 */
function buildDerivedElements(elements: SigmaElement[]): SigmaElement[] {
  const derived: SigmaElement[] = [];

  for (const srcEl of elements) {
    if (!srcEl.relationships?.length) continue;
    if (srcEl.source?.kind !== 'warehouse-table') continue;

    const srcPath = srcEl.source.path as string[];
    const srcTableName = srcPath[srcPath.length - 1];

    const viewCols: Array<{ id: string; formula: string }> = [];
    const viewOrder: string[] = [];

    // Own columns from the fact element — physical warehouse refs only.
    // Computed/named columns use bare [Col] refs in their formulas that won't
    // resolve as cross-element refs in the derived element context.
    for (const col of srcEl.columns ?? []) {
      if (!col.formula || col.formula.startsWith('/*')) continue;
      if (col.name) continue;
      const cId = sigmaShortId();
      viewCols.push({ id: cId, formula: col.formula });
      viewOrder.push(cId);
    }

    // Joined dimension columns via [TABLE/REL_NAME/Col] cross-element refs.
    // Only physical warehouse-column refs (no name, simple [TABLE/Col] formula)
    // are included — computed/named columns can't be resolved via cross-element paths.
    for (const rel of srcEl.relationships ?? []) {
      if (!rel.name) continue;
      const tgtEl = elements.find(e => e.id === rel.targetElementId);
      if (!tgtEl || tgtEl.source?.kind !== 'warehouse-table') continue;

      // Don't denormalize the relationship's OWN key column across the join — the base
      // element already carries that value, and the cross-element passthrough of a join
      // key compiles to type "error" in Sigma (verified via readback). Skip it.
      // (Same fix as the shared buildDerivedElements in sigma-ids.ts —
      // feedback_sigma_derived_view_skip_join_key.)
      const tgtKeyIds = new Set((rel.keys ?? []).map((k: any) => k.targetColumnId));
      for (const col of tgtEl.columns ?? []) {
        if (tgtKeyIds.has(col.id)) continue;
        if (!col.formula || col.formula.startsWith('/*')) continue;
        // Named/computed columns (e.g. dimension_group DateTrunc timeframes, CASE
        // dims) are referenced cross-element by their display name; physical refs
        // use the column inside the [TABLE/Col] formula.
        let dispName: string;
        if (col.name) {
          dispName = col.name;
        } else {
          const fm = col.formula.match(/^\[([^\]]+)\]$/);
          if (!fm) continue;
          const inner = fm[1];
          const slashIdx = inner.lastIndexOf('/');
          dispName = slashIdx >= 0 ? inner.slice(slashIdx + 1) : inner;
        }
        const cId = sigmaShortId();
        viewCols.push({ id: cId, formula: `[${srcTableName}/${rel.name}/${dispName}]` });
        viewOrder.push(cId);
      }
    }

    if (viewCols.length > 0) {
      derived.push({
        id: sigmaShortId(),
        kind: 'table',
        name: srcEl.name ?? sigmaDisplayName(srcTableName),
        source: { kind: 'table', elementId: srcEl.id },
        columns: viewCols,
        order: viewOrder,
      } as SigmaElement);
    }
  }

  return derived;
}
