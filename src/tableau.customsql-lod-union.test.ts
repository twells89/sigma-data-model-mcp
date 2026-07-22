/**
 * LOD over a Custom SQL fact whose final body is a multi-branch UNION ALL.
 *
 * The CTE→body boundary finder must pick the FIRST depth-0 SELECT, not the last.
 * A `WITH … SELECT … UNION ALL SELECT … UNION ALL SELECT …` body has one depth-0
 * SELECT per branch; taking the last splits the body mid-union and splices
 * `__lod_base AS (…)` immediately after a `UNION ALL`, yielding
 * `… UNION ALL,\n__lod_base AS (…` which fails to compile. Guards the fix.
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { firstTopLevelSelectIndex } from './tableau.js';

// The dummy shape from the field report (not real customer SQL): a CTE chain
// feeding a 3-branch UNION ALL final body.
const UNION_STMT = [
  'WITH',
  'base AS (SELECT id, category, amount FROM raw_table),',
  "leg1 AS (SELECT id, 'A' AS bucket, amount FROM base WHERE category = 'A')",
  'SELECT id, bucket, SUM(amount) AS amt FROM leg1 GROUP BY id, bucket',
  'UNION ALL',
  "SELECT id, 'B' AS bucket, SUM(amount) AS amt FROM base WHERE category = 'B' GROUP BY id",
  'UNION ALL',
  "SELECT id, 'C' AS bucket, SUM(amount) AS amt FROM base WHERE category = 'C' GROUP BY id",
].join('\n');

describe('firstTopLevelSelectIndex — CTE→body boundary', () => {
  test('picks the FIRST depth-0 SELECT on a UNION ALL body', () => {
    const idx = firstTopLevelSelectIndex(UNION_STMT);
    const cte = UNION_STMT.slice(0, idx);
    const body = UNION_STMT.slice(idx);

    // Boundary lands at the first final-body SELECT: the CTE part holds both
    // CTE defs and NOTHING of the union body.
    assert.ok(idx > 0, 'boundary found after the WITH');
    assert.match(cte, /base AS \(/, 'CTE part keeps the first CTE');
    assert.match(cte, /leg1 AS \(/, 'CTE part keeps the second CTE');
    assert.ok(!/UNION ALL/i.test(cte), 'CTE part must NOT contain any UNION ALL branch');

    // The body is the WHOLE union (all three branches).
    assert.equal((body.match(/UNION ALL/gi) || []).length, 2, 'body keeps both UNION ALLs');
    assert.match(body, /^SELECT id, bucket/, 'body starts at the first union branch');

    // The actual splice the converter builds must be valid — no "UNION ALL,\n__lod_base".
    const spliced = `${cte.replace(/^\s*WITH\s+/i, '').replace(/,?\s*$/, '')},\n__lod_base AS (\n${body}\n),\n`;
    assert.ok(!/UNION ALL\s*,\s*__lod_base/i.test(spliced), 'no comma spliced after a UNION ALL branch');
    assert.match(spliced, /leg1 AS \(SELECT[^)]*\),\n__lod_base AS \(/, '__lod_base opens right after the last CTE');
  });

  test('single-body WITH: boundary is the sole final SELECT', () => {
    const stmt = 'WITH a AS (SELECT x FROM t) SELECT x, SUM(y) FROM a GROUP BY x';
    const idx = firstTopLevelSelectIndex(stmt);
    assert.ok(idx > 0);
    assert.match(stmt.slice(0, idx), /^WITH a AS \(SELECT x FROM t\)\s*$/);
    assert.match(stmt.slice(idx), /^SELECT x, SUM\(y\)/);
  });

  test('nested parens in a CTE body do not register as depth-0 SELECTs', () => {
    const stmt = 'WITH a AS (SELECT x FROM (SELECT x FROM t) inner) SELECT x FROM a';
    const idx = firstTopLevelSelectIndex(stmt);
    assert.match(stmt.slice(idx), /^SELECT x FROM a$/, 'only the final body SELECT is depth-0');
  });

  test('plain SELECT with no CTEs → index 0 (caller wraps as subquery)', () => {
    assert.equal(firstTopLevelSelectIndex('SELECT a, b FROM t'), 0);
  });
});
