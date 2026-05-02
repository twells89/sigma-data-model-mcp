/**
 * Custom SQL → Sigma Data Model JSON converter.
 *
 * Parses SQL SELECT statements (including explicit JOINs and aggregate functions),
 * infers warehouse table sources, builds relationships, and generates Sigma data
 * model elements with derived view elements for JOIN output.
 *
 * Complex queries (subqueries in FROM, implicit cross-joins, unresolvable CTEs)
 * fall back to a Custom SQL element with inferred column names.
 */

import {
  resetIds, sigmaShortId, sigmaInodeId, sigmaDisplayName, sigmaColFormula,
  type SigmaElement, type SigmaColumn, type SigmaMetric, type SigmaRelationship,
  type ConversionResult,
} from './sigma-ids.js';

// ── Public interface ─────────────────────────────────────────────────────────

export interface SqlStatement {
  /** Human-readable name for this query (used as the derived element name). */
  name: string;
  /** The SQL SELECT statement to convert. */
  sql: string;
}

export interface SqlConvertOptions {
  connectionId?: string;
  database?: string;
  schema?: string;
}

export function convertSqlToSigma(
  statements: SqlStatement[],
  options: SqlConvertOptions = {},
): ConversionResult {
  resetIds();

  const { connectionId = '', database = '', schema = '' } = options;
  const db = database.trim().toUpperCase();
  const sc = schema.trim().toUpperCase();

  const warnings: string[] = [];
  const allElements: SigmaElement[] = [];
  const joinElemCache: Record<string, { id: string; elem: SigmaElement }> = {};
  let nativeCount = 0;
  let sqlFallback = 0;

  function makeSource(path: string[]): Record<string, any> {
    const s: Record<string, any> = { kind: 'warehouse-table', path };
    if (connectionId) s.connectionId = connectionId;
    return s;
  }

  for (const stmt of statements) {
    const parsed = parseSqlFull(stmt.sql, db, sc);

    if (!parsed) {
      // Complex query (subquery, implicit cross-join) → Custom SQL element.
      // Wrap the user's SQL in an outer SELECT that aliases each projection to
      // a double-quoted display name. This is the only column shape Sigma
      // resolves at query time for sql-source elements: the SQL output column
      // names must match the formula's [Custom SQL/<Display>] reference.
      const fallbackCols = extractSqlColumns(stmt.sql);
      const aliases = fallbackCols.map(c => ({ phys: c, display: sigmaDisplayName(c) }));
      const wrappedSql = aliases.length
        ? `SELECT ${aliases.map(a => `"${a.phys.toUpperCase()}" AS "${a.display}"`).join(', ')}\nFROM (\n${stmt.sql}\n) AS _src`
        : stmt.sql;
      const fallbackColObjs: SigmaColumn[] = aliases.map(a => ({
        id: sigmaInodeId(a.phys.toUpperCase()),
        formula: `[Custom SQL/${a.display}]`,
        name: a.display,
      }));
      const fallbackSrc: Record<string, any> = { kind: 'sql', statement: wrappedSql };
      if (connectionId) fallbackSrc.connectionId = connectionId;
      allElements.push({
        id: sigmaShortId(), kind: 'table',
        source: fallbackSrc,
        columns: fallbackColObjs,
        order: fallbackColObjs.map(c => c.id),
      } as SigmaElement);
      sqlFallback++;
      warnings.push(`"${stmt.name}": subquery or implicit cross-join — kept as custom SQL element. SQL was wrapped to add display-name aliases.`);
      continue;
    }

    nativeCount++;
    const primaryTableName = parsed.primaryPath[parsed.primaryPath.length - 1];

    // ── Dimension elements FIRST (Sigma convention: dims before facts) ───────
    for (const join of parsed.joins) {
      const jKey = join.path.join('.');
      if (!joinElemCache[jKey]) {
        const jId = sigmaShortId();
        const jElem: SigmaElement = {
          id: jId, kind: 'table',
          source: makeSource(join.path),
          columns: [], order: [],
        };
        allElements.push(jElem);
        joinElemCache[jKey] = { id: jId, elem: jElem };
      }
    }

    // ── Primary warehouse element ────────────────────────────────────────────
    const primaryId = sigmaShortId();
    const primaryElem: SigmaElement = {
      id: primaryId, kind: 'table',
      source: makeSource(parsed.primaryPath),
      columns: [], order: [], metrics: [], relationships: [],
    };
    allElements.push(primaryElem);

    // Own SELECT columns (dim-attributed and computed columns are deferred)
    for (const col of parsed.columns) {
      if (typeof col === 'object' && ((col as JoinCol).joinTable || (col as JoinCol).isComputed)) continue;
      ensureSqlColumn(primaryElem, typeof col === 'string' ? col : (col as JoinCol).name);
    }

    // Aggregate metrics (SUM/COUNT/AVG/MIN/MAX)
    for (const m of parsed.metrics) {
      const mId = sigmaInodeId(m.name.replace(/\s+/g, '_').toUpperCase());
      (primaryElem.metrics ??= []).push({ id: mId, name: m.name, formula: m.formula });
      if (m.sourceCol && m.sourceCol !== 'rows') {
        ensureSqlColumn(primaryElem, m.sourceCol);
      }
    }

    // FK columns + relationships
    for (const join of parsed.joins) {
      const joinEntry = joinElemCache[join.path.join('.')];
      const srcColId = ensureSqlColumn(primaryElem, join.leftKey);
      const tgtColId = ensureSqlColumn(joinEntry.elem, join.rightKey);
      (primaryElem.relationships ??= []).push({
        id: sigmaShortId(),
        targetElementId: joinEntry.id,
        keys: [{ sourceColumnId: srcColId, targetColumnId: tgtColId }],
        name: join.path[join.path.length - 1],
      } as SigmaRelationship);
    }

    // Dim-attributed SELECT columns → add to each dimension element
    for (const col of parsed.columns) {
      if (typeof col !== 'object' || !(col as JoinCol).joinTable) continue;
      const jc = col as JoinCol;
      const jKey = Object.keys(joinElemCache).find(k => k.split('.').pop() === jc.joinTable);
      if (jKey) ensureSqlColumn(joinElemCache[jKey].elem, jc.name);
    }

    // Computed expression columns → ensure every alias.col they reference exists on its element.
    // Without this pass, translateSqlExprToSigma produces valid formulas but the referenced
    // columns are absent from the warehouse-table element, causing "dependency not found" errors.
    for (const col of parsed.columns) {
      if (typeof col !== 'object' || !(col as JoinCol).isComputed) continue;
      for (const [, tblAlias, physCol] of (col as JoinCol).expr!.matchAll(/\b([\w]+)\.([\w]+)\b/g)) {
        const lowerAlias = tblAlias.toLowerCase();
        if (!(lowerAlias in parsed.aliasMap)) continue;
        const joinTable = parsed.aliasMap[lowerAlias];
        if (joinTable === null) {
          ensureSqlColumn(primaryElem, physCol);
        } else {
          const jKey = Object.keys(joinElemCache).find(k => k.split('.').pop() === joinTable);
          if (jKey) ensureSqlColumn(joinElemCache[jKey].elem, physCol);
        }
      }
    }

    // ── Derived view element (sources from primary, surfaces all SELECT columns) ──
    // Uses three-part [TABLE/REL_NAME/Col] formulas for joined dimension columns.
    const viewCols: SigmaColumn[] = [];
    const viewOrder: string[] = [];
    for (const col of parsed.columns) {
      const colName = typeof col === 'string' ? col : (col as JoinCol).name;
      let formula: string;
      if (typeof col === 'object' && (col as JoinCol).isComputed) {
        formula = translateSqlExprToSigma((col as JoinCol).expr!, parsed.aliasMap, primaryTableName);
      } else if (typeof col === 'object' && (col as JoinCol).joinTable) {
        formula = `[${primaryTableName}/${(col as JoinCol).joinTable}/${sigmaDisplayName(colName)}]`;
      } else {
        formula = `[${primaryTableName}/${sigmaDisplayName(colName)}]`;
      }
      const cId = sigmaShortId();
      viewCols.push({ id: cId, formula, name: sigmaDisplayName(colName) });
      viewOrder.push(cId);
    }
    if (parsed.joins.length > 0 && viewCols.length > 0) {
      allElements.push({
        id: sigmaShortId(), kind: 'table',
        name: sigmaDisplayName(stmt.name),
        source: { kind: 'table', elementId: primaryId },
        columns: viewCols, order: viewOrder,
      } as SigmaElement);
    }

    // Clean up empty arrays on primary element
    if (!primaryElem.columns.length) {
      delete (primaryElem as any).columns;
      delete (primaryElem as any).order;
    }
    if (!(primaryElem.metrics?.length)) delete primaryElem.metrics;
    if (!(primaryElem.relationships?.length)) delete primaryElem.relationships;
  }

  const sigmaModel = {
    name: 'Converted SQL Model',
    schemaVersion: 1,
    pages: [{ id: sigmaShortId(), name: 'Page 1', elements: allElements }],
  };

  const totalCols = allElements.reduce((s, e) => s + (e.columns?.length || 0), 0);
  const totalMetrics = allElements.reduce((s, e) => s + (e.metrics?.length || 0), 0);

  return {
    model: sigmaModel,
    warnings,
    stats: {
      elements: allElements.length,
      columns: totalCols,
      metrics: totalMetrics,
      nativeStatements: nativeCount,
      sqlFallbacks: sqlFallback,
    },
  };
}

// ── Internal types ────────────────────────────────────────────────────────────

interface JoinCol {
  name: string;
  joinTable: string | null;
  isComputed?: boolean;
  expr?: string;
}

interface ParsedJoin {
  path: string[];
  alias: string;
  joinType: string;
  leftKey: string;
  rightKey: string;
}

interface ParsedMetric {
  name: string;
  formula: string;
  sourceCol: string | null;
}

interface ParsedSql {
  primaryPath: string[];
  joins: ParsedJoin[];
  columns: Array<string | JoinCol>;
  metrics: ParsedMetric[];
  aliasMap: Record<string, string | null>;
}

// ── SQL Parser helpers ────────────────────────────────────────────────────────

/** Build a warehouse path array from a table reference and db/schema overrides. */
function buildSqlPath(tableRef: string, db: string, schema: string): string[] | null {
  const raw = tableRef.replace(/["`[\]]/g, '');
  if (!raw) return null;
  const parts = raw.split('.').map(p => p.toUpperCase());
  if (parts.length >= 3) return parts.slice(-3);
  if (parts.length === 2) return db ? [db, ...parts] : parts;
  if (db && schema) return [db, schema, parts[0]];
  if (schema)       return [schema, parts[0]];
  return parts;
}

/** Find the matching close-paren index starting from openIdx. */
function sqlFindClose(str: string, openIdx: number): number {
  let d = 0;
  for (let i = openIdx; i < str.length; i++) {
    if (str[i] === '(') d++;
    else if (str[i] === ')') { d--; if (d === 0) return i; }
  }
  return str.length;
}

/** Split a comma-separated string at depth 0. */
function splitDepthZero(str: string): string[] {
  const parts: string[] = [];
  let cur = '';
  let d = 0;
  for (let i = 0; i < str.length; i++) {
    const ch = str[i];
    if (ch === '(')      { d++; cur += ch; }
    else if (ch === ')') { d--; cur += ch; }
    else if (ch === ',' && d === 0) { parts.push(cur.trim()); cur = ''; }
    else cur += ch;
  }
  if (cur.trim()) parts.push(cur.trim());
  return parts;
}

/** Extract alias or final column name from one SELECT item. */
function sqlExtractAlias(part: string): string | null {
  const asM = part.match(/\bAS\s+([\w"`[\]]+)\s*$/i);
  if (asM) return asM[1].replace(/["`[\]]/g, '');
  if (!/[()]/.test(part)) {
    const m = part.trim().match(/(?:[^.]+\.)*([^.\s]+)\s*$/);
    return m ? m[1].replace(/["`[\]]/g, '') : null;
  }
  return null; // function expression without alias → skip
}

/**
 * Extract column names from a SQL SELECT statement.
 * Handles CTEs, DISTINCT, depth-0 comma splitting, and AS aliases.
 */
function extractSqlColumns(sql: string): string[] {
  let workSql = sql;
  if (/^\s*WITH\s/i.test(sql)) {
    let depth = 0, lastTopSel = -1;
    for (let i = 0; i < sql.length; i++) {
      if (sql[i] === '(') depth++;
      else if (sql[i] === ')') depth--;
      else if (depth === 0 && /^SELECT\b/i.test(sql.slice(i))) lastTopSel = i;
    }
    if (lastTopSel >= 0) workSql = sql.slice(lastTopSel);
  }

  const afterSelect = workSql.replace(/^\s*SELECT\s+(?:DISTINCT\s+|ALL\s+)?/i, '');
  let depth2 = 0, fromIdx = afterSelect.length;
  for (let j = 0; j < afterSelect.length; j++) {
    const c = afterSelect[j];
    if (c === '(') depth2++;
    else if (c === ')') depth2--;
    else if (depth2 === 0 && /^FROM\b/i.test(afterSelect.slice(j))) { fromIdx = j; break; }
  }

  const selectList = afterSelect.slice(0, fromIdx).trim();
  if (!selectList || selectList === '*') return [];

  const parts: string[] = [];
  let cur = '', d = 0;
  for (let k = 0; k < selectList.length; k++) {
    const ck = selectList[k];
    if (ck === '(')      { d++; cur += ck; }
    else if (ck === ')') { d--; cur += ck; }
    else if (ck === ',' && d === 0) { parts.push(cur.trim()); cur = ''; }
    else cur += ck;
  }
  if (cur.trim()) parts.push(cur.trim());

  return parts.map((part, idx) => {
    const asM = part.match(/\bAS\s+([\w"`[\]]+)\s*$/i);
    if (asM) return asM[1].replace(/["`[\]]/g, '');
    if (!/[\s(]/.test(part.trim())) {
      const colM = part.match(/(?:[^.]+\.)*([^.]+)$/);
      if (colM) return colM[1].replace(/["`[\]]/g, '');
    }
    return `col_${idx + 1}`;
  }).filter(Boolean);
}

/**
 * Comprehensive SQL SELECT parser.
 * Returns primary table path, explicit JOINs (ON/USING), SELECT columns, and metrics.
 * Returns null if the query is too complex to auto-resolve (subquery in FROM, etc.).
 */
function parseSqlFull(sql: string, db: string, sc: string): ParsedSql | null {
  let workSql = sql;

  // Skip CTEs: find the last top-level SELECT after all CTE definitions
  if (/^\s*WITH\s/i.test(sql)) {
    let d0 = 0, lastSel = -1;
    for (let ci = 0; ci < sql.length; ci++) {
      if (sql[ci] === '(') d0++;
      else if (sql[ci] === ')') d0--;
      else if (d0 === 0 && /^SELECT\b/i.test(sql.slice(ci))) lastSel = ci;
    }
    if (lastSel < 0) return null;
    workSql = sql.slice(lastSel);
  }

  // Locate FROM at depth 0
  const afterSel = workSql.replace(/^\s*SELECT\s+(?:DISTINCT\s+|ALL\s+)?/i, '');
  let d1 = 0, fromAt = -1;
  for (let fi = 0; fi < afterSel.length; fi++) {
    if (afterSel[fi] === '(') d1++;
    else if (afterSel[fi] === ')') d1--;
    else if (d1 === 0 && /^FROM\b/i.test(afterSel.slice(fi))) { fromAt = fi; break; }
  }
  if (fromAt < 0) return null;

  const selectListStr = afterSel.slice(0, fromAt).trim();
  const restStr = afterSel.slice(fromAt + 4).replace(/^\s+/, '');

  // Parse primary table (before first JOIN/WHERE/GROUP BY etc.)
  const stopRe = /\b(WHERE|(?:INNER|LEFT(?:\s+OUTER)?|RIGHT(?:\s+OUTER)?|FULL(?:\s+OUTER)?|CROSS)?\s*JOIN|GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|UNION)\b/i;
  const firstStop = stopRe.exec(restStr);
  const primaryClause = (firstStop ? restStr.slice(0, firstStop.index) : restStr).trim();

  if (/^\s*\(/.test(primaryClause)) return null; // subquery
  if (/,/.test(primaryClause))       return null; // implicit cross-join

  const ptokens = primaryClause.split(/\s+/);
  const primaryTableRef = ptokens[0];
  const aliasIdx = (ptokens[1] || '').toUpperCase() === 'AS' ? 2 : 1;
  const primaryAlias = (ptokens[aliasIdx] || '').replace(/["`[\]]/g, '').toLowerCase();

  const primaryPath = buildSqlPath(primaryTableRef, db, sc);
  if (!primaryPath) return null;

  // Parse explicit JOINs — ON expr = expr
  const joins: ParsedJoin[] = [];
  const joinRe = /\b(INNER\s+JOIN|LEFT\s+(?:OUTER\s+)?JOIN|RIGHT\s+(?:OUTER\s+)?JOIN|FULL\s+(?:OUTER\s+)?JOIN|CROSS\s+JOIN|JOIN)\s+([\w"`[\].]+)(?:\s+(?:AS\s+)?([\w"`[\]]+))?\s+ON\s+([\w"`.[\]]+)\s*=\s*([\w"`.[\]]+)/gi;
  let jm: RegExpExecArray | null;
  while ((jm = joinRe.exec(workSql)) !== null) {
    const jType  = jm[1].replace(/\s+OUTER\s*/i, ' ').replace(/\s+/g, '_').trim().toUpperCase();
    const jTable = jm[2];
    const jAlias = jm[3] ? jm[3].replace(/["`[\]]/g, '') : jm[2].split('.').pop()!.replace(/["`[\]]/g, '');
    const onLeft  = jm[4].split('.').pop()!.replace(/["`[\]]/g, '').toUpperCase();
    const onRight = jm[5].split('.').pop()!.replace(/["`[\]]/g, '').toUpperCase();
    const jPath = buildSqlPath(jTable, db, sc);
    if (jPath) joins.push({ path: jPath, alias: jAlias, joinType: jType, leftKey: onLeft, rightKey: onRight });
  }

  // Parse explicit JOINs — USING (col)
  const usingRe = /\b(INNER\s+JOIN|LEFT\s+(?:OUTER\s+)?JOIN|RIGHT\s+(?:OUTER\s+)?JOIN|FULL\s+(?:OUTER\s+)?JOIN|CROSS\s+JOIN|JOIN)\s+([\w"`[\].]+)(?:\s+(?:AS\s+)?([\w"`[\]]+))?\s+USING\s*\(\s*([\w"`[\]]+)\s*\)/gi;
  while ((jm = usingRe.exec(workSql)) !== null) {
    const ujType  = jm[1].replace(/\s+OUTER\s*/i, ' ').replace(/\s+/g, '_').trim().toUpperCase();
    const ujTable = jm[2];
    const ujAlias = jm[3] ? jm[3].replace(/["`[\]]/g, '') : jm[2].split('.').pop()!.replace(/["`[\]]/g, '');
    const uCol    = jm[4].replace(/["`[\]]/g, '').toUpperCase();
    const ujPath  = buildSqlPath(ujTable, db, sc);
    if (ujPath && !joins.some(j => j.alias.toLowerCase() === ujAlias.toLowerCase())) {
      joins.push({ path: ujPath, alias: ujAlias, joinType: ujType, leftKey: uCol, rightKey: uCol });
    }
  }

  // Build alias → table map for SELECT attribution
  const aliasMap: Record<string, string | null> = {};
  if (primaryAlias) aliasMap[primaryAlias] = null; // null = primary table
  for (const j of joins) {
    if (j.alias) aliasMap[j.alias.toLowerCase()] = j.path[j.path.length - 1];
  }

  // Parse SELECT items into regular columns and aggregate metrics
  const columns: Array<string | JoinCol> = [];
  const metrics: ParsedMetric[] = [];

  for (const part of splitDepthZero(selectListStr)) {
    if (!part || /^\s*\*/.test(part) || /\.\*\s*$/.test(part)) continue; // skip *

    let alias = sqlExtractAlias(part);
    const aggM = part.match(/\b(SUM|COUNT\s+DISTINCT|COUNT|AVG|AVERAGE|MIN|MAX)\s*\(/i);

    if (aggM) {
      if (!alias) alias = aggM[1].replace(/\s+/g, '').toLowerCase() + '_result';
      const funcName = aggM[1].replace(/\s+/g, '').toUpperCase();
      const parenOpen  = aggM.index! + aggM[0].length - 1;
      const parenClose = sqlFindClose(part, parenOpen);
      const inner      = part.slice(parenOpen + 1, parenClose).trim();
      const multiAgg   = (part.match(/\b(SUM|COUNT|AVG|MIN|MAX)\s*\(/gi) ?? []).length > 1;

      let formula: string;
      if (multiAgg) {
        formula = `/* TODO: ${alias} — complex expression, wire manually */`;
      } else {
        const innerCol = inner.split('.').pop()!.replace(/["`[\]]/g, '').replace(/^\*$/, 'rows') || alias;
        const sigmaFn: Record<string, string> = {
          SUM: 'Sum', COUNT: 'CountIf', COUNTDISTINCT: 'CountDistinct',
          AVG: 'Avg', AVERAGE: 'Avg', MIN: 'Min', MAX: 'Max',
        };
        const fn = sigmaFn[funcName] ?? 'Sum';
        formula = funcName === 'COUNT'
          ? `CountIf(IsNotNull([${sigmaDisplayName(innerCol)}]))`
          : `${fn}([${sigmaDisplayName(innerCol)}])`;
        metrics.push({ name: sigmaDisplayName(alias), formula, sourceCol: multiAgg ? null : innerCol });
        continue;
      }
      metrics.push({ name: sigmaDisplayName(alias), formula, sourceCol: null });
    } else {
      if (!alias) continue;

      // Detect table.column attribution via dot notation (e.g. c.customer_name)
      const colExpr = (part.trim().search(/\bAS\s+[\w"`[\]]+\s*$/i) >= 0)
        ? part.trim().slice(0, part.trim().search(/\bAS\s+[\w"`[\]]+\s*$/i)).trim()
        : part.trim();

      let joinTable: string | undefined;
      let isComputed = false;

      if (!/[()]/.test(colExpr)) {
        const dotM = colExpr.match(/^([\w"`[\]]+)\.([\w"`[\]]+)$/);
        if (dotM) {
          const tblAlias = dotM[1].replace(/["`[\]]/g, '').toLowerCase();
          const physCol  = dotM[2].replace(/["`[\]]/g, '');
          if (tblAlias in aliasMap) {
            joinTable = aliasMap[tblAlias] ?? undefined;
            alias = physCol;
          }
        } else if (/[\+\-\*\/|&%]/.test(colExpr) || /\w+\.\w+.*\w+\.\w+/.test(colExpr)) {
          // Operator expression or multiple table.col refs — computed, not a physical column
          isComputed = true;
        }
      } else {
        // Has parentheses and an AS alias — function call or complex expression — computed
        isComputed = true;
      }

      if (isComputed) {
        columns.push({ name: alias!, joinTable: null, isComputed: true, expr: colExpr });
      } else {
        columns.push(joinTable !== undefined ? { name: alias!, joinTable } : alias!);
      }
    }
  }

  return { primaryPath, joins, columns, metrics, aliasMap };
}

/**
 * Translate a SQL expression to a Sigma formula string.
 * Resolves alias.column references to [TABLE/REL/Col] paths and
 * converts SQL operators/functions to Sigma equivalents.
 */
function translateSqlExprToSigma(
  expr: string,
  aliasMap: Record<string, string | null>,
  primaryTableName: string,
): string {
  let result = expr;

  // Replace alias.column references (longest match first to avoid partial replacements)
  result = result.replace(/\b([\w]+)\.([\w]+)\b/g, (_match, tblAlias, col) => {
    const lowerAlias = tblAlias.toLowerCase();
    if (lowerAlias in aliasMap) {
      const joinTable = aliasMap[lowerAlias];
      const dn = sigmaDisplayName(col);
      return joinTable
        ? `[${primaryTableName}/${joinTable}/${dn}]`
        : `[${primaryTableName}/${dn}]`;
    }
    return `[${sigmaDisplayName(col)}]`;
  });

  // SQL → Sigma operator/function translations
  result = result
    .replace(/\|\|/g, '&')                                         // string concat
    .replace(/\bNULLIF\s*\(([^,]+),\s*0\s*\)/gi, '$1')           // NULLIF(x,0) → x (Sigma returns null on /0)
    .replace(/\bNULL\b/gi, 'Null')
    .replace(/'/g, '"');                                            // SQL string literals → Sigma

  return result;
}

/**
 * Ensure a physical column exists on an element and return its ID.
 * Column formula format: [PHYSICAL_TABLE_NAME/Display Name]
 */
function ensureSqlColumn(elem: SigmaElement, colName: string): string {
  const dn = sigmaDisplayName(colName);
  const physTable = elem.source?.path
    ? (elem.source.path as string[])[elem.source.path.length - 1]
    : (elem as any).name?.toUpperCase().replace(/\s+/g, '_') ?? colName.toUpperCase();
  const formula = `[${physTable}/${dn}]`;

  const existing = (elem.columns ?? []).find(c => c.formula === formula);
  if (existing) return existing.id;

  const id = sigmaInodeId(colName.toUpperCase());
  if (!elem.columns) { (elem as any).columns = []; (elem as any).order = []; }
  elem.columns.push({ id, formula });
  elem.order.push(id);
  return id;
}
