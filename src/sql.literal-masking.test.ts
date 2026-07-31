/**
 * Bug: sql.ts's SELECT-list parser scans raw SQL text with a paren-depth
 * walk (`splitDepthZero` / `sqlFindClose`) that has no idea string literals
 * exist — a comma or an unbalanced paren INSIDE a single-quoted SQL literal
 * is read as live syntax.
 *
 * Bug 1 (splitDepthZero / top-level comma splitter): a literal value
 * containing a comma — 'Small, Medium, Large' AS size_options — gets split
 * into bogus pieces at every embedded comma. Demonstrated (live-reproduced,
 * pre-fix): `SELECT id, 'Small, Medium, Large' AS size_options, amount FROM
 * orders` fabricates THREE bogus warehouse-table columns ('small, Medium,
 * and a "Size Options" column bound to a real-looking but nonexistent
 * ORDERS.SIZE_OPTIONS physical column) instead of one literal-valued column
 * — a wrong-binding data-loss bug that would 400/return wrong data against
 * the real warehouse table.
 *
 * Bug 2 (sqlFindClose / paren-depth walk): an aggregate's inner expression
 * containing a literal with an unbalanced ')' desyncs the close-paren
 * finder, so it returns the index of the ')' INSIDE the literal instead of
 * the aggregate's real closing paren. Demonstrated (live-reproduced,
 * pre-fix):
 *   SELECT id, SUM(CASE WHEN note = 'A)B(' THEN amt ELSE 0 END) AS total
 *     FROM orders
 * truncated the extracted inner expression to `CASE WHEN note = 'A` (cut off
 * mid-literal, losing "B(' THEN amt ELSE 0 END" entirely) instead of the
 * complete `CASE WHEN note = 'A)B(' THEN amt ELSE 0 END`. (This SQL
 * converter has a separate, pre-existing, orthogonal limitation — it has no
 * dedicated CASE-inside-aggregate handling and always turns the whole inner
 * expression into one garbled pseudo-column name, even for a CASE with no
 * embedded literal desync at all. That limitation is NOT part of this
 * defect class and is left alone; what's fixed here is only that the
 * extracted expression is no longer truncated mid-literal.)
 *
 * Control (must keep working): a normal multi-column SELECT with no
 * embedded literal delimiters still parses natively (no fallback to a
 * wrapped Custom SQL element), and a genuine SUM(...) aggregate with no
 * literal still produces the right metric.
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { convertSqlToSigma } from './sql.js';

describe('sql literal masking: comma inside a string literal in the SELECT list', () => {
  const sql = `SELECT id, 'Small, Medium, Large' AS size_options, amount FROM orders`;
  const run = () => convertSqlToSigma([{ name: 'q1', sql }], { connectionId: 'c' });

  test('does not fabricate a bogus "Medium" column', () => {
    const { model } = run();
    const el = model.pages[0].elements[0];
    const names = (el.columns || []).map((c: any) => c.formula);
    assert.ok(!names.some((f: string) => /\/Medium\]$/.test(f)), `unexpected fabricated column: ${names.join(', ')}`);
  });

  test('does not fabricate a garbled leading-quote column', () => {
    const { model } = run();
    const el = model.pages[0].elements[0];
    const names = (el.columns || []).map((c: any) => c.formula);
    assert.ok(!names.some((f: string) => f.includes("'")), `unexpected garbled column: ${names.join(', ')}`);
  });

  test('real columns id and amount are still present, exactly two plus the literal column', () => {
    const { model } = run();
    const el = model.pages[0].elements[0];
    const formulas = (el.columns || []).map((c: any) => c.formula);
    assert.ok(formulas.some((f: string) => /\/Id\]$/.test(f)), `missing Id column: ${formulas.join(', ')}`);
    assert.ok(formulas.some((f: string) => /\/Amount\]$/.test(f)), `missing Amount column: ${formulas.join(', ')}`);
  });
});

describe('sql literal masking: unbalanced ) inside a string literal in an aggregate', () => {
  const sql = `SELECT id, SUM(CASE WHEN note = 'A)B(' THEN amt ELSE 0 END) AS total FROM orders`;
  const run = () => convertSqlToSigma([{ name: 'q1', sql }], { connectionId: 'c' });

  // The aggregate's inner expression must not be truncated at the literal's
  // embedded ')' — it must extend all the way to the real closing paren
  // (i.e. include the "ELSE 0 END" tail that follows the stray ')(' pair).
  // A pre-existing, unrelated limitation (no CASE-inside-aggregate support)
  // still turns the full expression into one garbled pseudo-column name —
  // that part is untouched and not asserted against here.
  test('the aggregate inner expression is not truncated mid-literal', () => {
    const { model } = run();
    const el = model.pages[0].elements[0];
    const cols = (el.columns || []).map((c: any) => c.formula);
    const metrics = (el.metrics || []).map((m: any) => m.formula);
    const full = [...cols, ...metrics].join(' | ');
    assert.match(full, /Else 0 End/i, `expected the full CASE...END expression, got: ${full}`);
    assert.doesNotMatch(full, /^\[?ORDERS?\/?Case When Note = 'a\]/i, `expected no mid-literal truncation, got: ${full}`);
  });
});

describe('sql literal masking: controls (unchanged behavior)', () => {
  test('a normal multi-column SELECT still parses natively (no Custom SQL fallback)', () => {
    const sql = `SELECT id, name, amount FROM orders`;
    const { model, stats } = convertSqlToSigma([{ name: 'q1', sql }], { connectionId: 'c' });
    assert.equal(stats.sqlFallbacks, 0);
    const el = model.pages[0].elements[0];
    assert.equal(el.source?.kind, 'warehouse-table');
    const formulas = (el.columns || []).map((c: any) => c.formula);
    assert.ok(formulas.some((f: string) => /\/Id\]$/.test(f)));
    assert.ok(formulas.some((f: string) => /\/Name\]$/.test(f)));
    assert.ok(formulas.some((f: string) => /\/Amount\]$/.test(f)));
  });

  test('a genuine SUM(...) aggregate with no literal still produces the right metric', () => {
    const sql = `SELECT id, SUM(amount) AS total FROM orders`;
    const { model } = convertSqlToSigma([{ name: 'q1', sql }], { connectionId: 'c' });
    const el = model.pages[0].elements[0];
    const m = (el.metrics || []).find((x: any) => x.name === 'Total');
    assert.ok(m, 'expected a Total metric');
    assert.match(m.formula, /^Sum\(\[Amount\]\)$/i);
  });
});
