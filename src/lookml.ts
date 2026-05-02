/**
 * LookML → Sigma Data Model JSON converter.
 * Includes a full LookML parser and explore-based conversion.
 */

import {
  resetIds, sigmaShortId, sigmaInodeId, sigmaDisplayName,
  type SigmaElement, type ConversionResult, type ElementResult
} from './sigma-ids.js';
import { lookIsComplexSql, lookSqlToSigmaRules, lookStripSql, lookSigmaMetric, detectUnsupportedSigmaFunction } from './formulas.js';

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
  // Strip line comments
  text = text.replace(/#[^\n]*/g, '');

  // Pre-extract raw sql: ... ;; blocks
  const sqlPlaceholders: Record<string, string> = {};
  let phIdx = 0;
  text = text.replace(/\b(sql(?:_start|_end)?)\s*:([\s\S]*?);;/g, (match, keyName, sqlContent) => {
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
    'join', 'set', 'link', 'action', 'form_param', 'option'
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

function lookConvertView(
  viewName: string,
  view: any,
  connectionId: string,
  warnings: string[],
  sqlTableNameMap?: Record<string, string>
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

    // Gap 1: resolve ${ref.SQL_TABLE_NAME} references inside derived table SQL
    if (sqlTableNameMap && rawSql.includes('${')) {
      rawSql = resolveSqlTableNameRefs(rawSql, sqlTableNameMap, warnings, viewName);
    }

    // Gap 3: warn on PDT-specific properties that are not converted
    const PDT_SKIP_PROPS = ['distribution', 'sortkeys', 'datagroup_trigger', 'persist_with', 'cluster_keys', 'partition_keys'];
    for (const prop of PDT_SKIP_PROPS) {
      if (view.derived_table[prop] !== undefined) {
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
      if (sigmaFormula) {
        element.columns.push({ id: colId, formula: sigmaFormula, name: d.label || sigmaDisplayName(d._name) });
        element.order.push(colId);
        warnings.push(`ℹ "${d._name}" → calculated column: ${sigmaFormula}`);
      } else {
        element.columns.push({ id: colId, formula: `[${tableName}/${sigmaDisplayName(colName)}]`, name: d.label || sigmaDisplayName(d._name) });
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
    element.columns.push({ id: colId, formula: `[${tableName}/${colLabel(physicalCol)}]` });
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

  measures.forEach((ms: any) => {
    if (!ms._name) return;
    const msName = ms._name.toUpperCase();
    const sqlCol = lookStripSql(ms.sql) || msName;
    const physicalCol = sqlCol.split('.').pop()!.replace(/"/g, '').toUpperCase() || msName.replace(/"/g, '');
    const msType = (ms.type || 'count').toLowerCase();
    const msLabel = ms.label || sigmaDisplayName(msName);

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
        element.columns.push({ id: calcId, formula: `CumulativeSum([${dn}])`, name: msLabel });
        warnings.push(`✅ "${ms._name}" (running_total) → CumulativeSum([${dn}])`);
      } else {
        element.columns.push({ id: calcId, formula: `Sum([${dn}]) / GrandTotal(Sum([${dn}]))`, name: msLabel });
        warnings.push(`✅ "${ms._name}" (percent_of_total) → Sum/GrandTotal`);
      }
      element.order.push(calcId);
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
        if (!colIdMap[physicalCol]) {
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
        element.metrics!.push({ id: sigmaShortId(), formula, name: msLabel });
        warnings.push(`✅ Filtered "${ms._name}" → ${formula.slice(0, 60)}`);
        return;
      }
      warnings.push(`⚠ "${ms._name}": filters not parsed — metric created without filter`);
    }

    if (msType === 'count') {
      element.metrics!.push({ id: sigmaShortId(), formula: 'Count()', name: msLabel });
    } else if (msType === 'count_distinct') {
      const cdCol = physicalCol && physicalCol !== msName ? physicalCol : msName;
      if (!colIdMap[cdCol]) {
        const colId = makeColId(cdCol);
        colIdMap[cdCol] = colId;
        element.columns.push({ id: colId, formula: `[${tableName}/${colLabel(cdCol)}]` });
        element.order.push(colId);
      }
      element.metrics!.push({ id: sigmaShortId(), formula: `CountDistinct([${colLabel(cdCol)}])`, name: msLabel });
    } else {
      if (!colIdMap[physicalCol]) {
        const colId = makeColId(physicalCol);
        colIdMap[physicalCol] = colId;
        element.columns.push({ id: colId, formula: `[${tableName}/${colLabel(physicalCol)}]` });
        element.order.push(colId);
      }
      element.metrics!.push({ id: sigmaShortId(), formula: lookSigmaMetric(msType, colLabel(physicalCol)), name: msLabel });
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
    return j.rel === 'one_to_many' || j.rel === 'many_to_many' || j.joinType === 'full_outer';
  };

  const relJoins = joinDefs.filter(j => !needsPhysical(j));

  // Build elements
  const elementMap: Record<string, ElementResult> = {};
  const physViewMap: Record<string, ElementResult> = {};

  const baseResult = lookConvertView(baseViewName, views[baseViewName], connectionId, warnings, sqlTableNameMap);
  elementMap[baseAlias] = baseResult;
  physViewMap[baseViewName] = baseResult;
  if (baseAlias !== baseViewName) elementMap[baseViewName] = baseResult;

  for (const j of joinDefs) {
    if (!physViewMap[j.viewName]) {
      const res = lookConvertView(j.viewName, views[j.viewName], connectionId, warnings, sqlTableNameMap);
      physViewMap[j.viewName] = res;
    }
    elementMap[j.alias] = physViewMap[j.viewName];
    if (!elementMap[j.viewName]) elementMap[j.viewName] = physViewMap[j.viewName];
  }

  // Wire relationships
  const baseEl = elementMap[baseAlias].element;
  const usedTargetCols = new Set<string>();

  relJoins.forEach(j => {
    const targetRes = elementMap[j.alias] || elementMap[j.viewName];
    if (!targetRes) {
      warnings.push(`⚠ Relationship "${j.alias}": target not found`);
      return;
    }

    j.keys.forEach((k: any) => {
      const isTarget = (name: string) => name === j.alias || name === j.viewName;
      const srcColId = lookFindColId(elementMap[baseAlias], isBaseView(k.leftView) ? k.leftCol : k.rightCol);
      const tgtColId = lookFindColId(targetRes, isTarget(k.leftView) ? k.leftCol : k.rightCol);

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

      if (!baseEl.relationships) baseEl.relationships = [];
      baseEl.relationships.push({
        id: sigmaShortId(),
        targetElementId: targetRes.elementId,
        keys: [{ sourceColumnId: srcColId, targetColumnId: tgtColId }],
        name: j.alias,
        relationshipType: 'N:1'
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

  // Build derived (browsable) elements for each fact element that has relationships.
  // Each derived element sources from the fact warehouse element and surfaces all
  // its own columns plus cross-element [TABLE/REL_NAME/Col] refs for joined dims.
  const derivedElements = buildDerivedElements(allElements);
  allElements = [...allElements, ...derivedElements];

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

      for (const col of tgtEl.columns ?? []) {
        if (!col.formula || col.formula.startsWith('/*')) continue;
        if (col.name) continue;
        // Extract the display name from a [TABLE/ColName] or [ColName] formula
        const fm = col.formula.match(/^\[([^\]]+)\]$/);
        if (!fm) continue;
        const inner = fm[1];
        const slashIdx = inner.lastIndexOf('/');
        const dispName = slashIdx >= 0 ? inner.slice(slashIdx + 1) : inner;
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
