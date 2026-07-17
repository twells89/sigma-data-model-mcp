/**
 * Regression tests for the Databricks / multi-schema Power BI fix batch:
 *   beads-sigma-lanq.7 — physical identifiers were hard-forced UPPER (Snowflake
 *          bias). Databricks stores identifiers lower-case and binds only against
 *          a lower-cased physical name/path, so UPPER → "Source not found" at POST
 *          (verified live on a Databricks connection). The new `warehouseType`
 *          option folds physical names/path to the warehouse case; the element
 *          name + [TABLE/Col] formula refs stay UPPER (Sigma-internal, must only
 *          agree with each other).
 *   beads-sigma-lanq.6 — a single `schema` used to OVERWRITE path[1] on EVERY
 *          table, collapsing a multi-schema model (one catalog, N schemas) onto
 *          one schema. It is now applied as a repoint only on a single-schema
 *          model; a multi-schema model keeps each table's own M-resolved schema.
 *
 * Run: node --import tsx/esm --test src/powerbi.databricks.test.ts
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { convertPowerBIToSigma } from './powerbi.js';
import { sigmaInodeId, sigmaPhysicalName } from './sigma-ids.js';

// Databricks Kind-nav M query for catalog.schema.table.
const mNav = (cat: string, schema: string, table: string) =>
  `let\n Source = Databricks.Catalogs("h","p",[]),\n a = Source{[Name="${cat}", Kind="Database"]}[Data],\n b = a{[Name="${schema}", Kind="Schema"]}[Data],\n c = b{[Name="${table}", Kind="Table"]}[Data]\nin c`;

const tbl = (name: string, cat: string, schema: string, table: string, cols: string[]) => ({
  name,
  columns: cols.map(c => ({ name: c, dataType: 'string', sourceColumn: c })),
  partitions: [{ name: 'p', source: { type: 'm', expression: mNav(cat, schema, table) } }],
});

const warehouseEl = (res: any, name?: string) => {
  const spec = res.model || res;
  const all = (spec.elements || []).concat(...(spec.pages || []).map((p: any) => p.elements || []));
  const els = all.filter((e: any) => e.source && e.source.kind === 'warehouse-table');
  return name ? els.find((e: any) => e.name === name) : els[0];
};
const physOf = (colId: string) => colId.replace(/^inode-[^/]*\//, '');

// ── lanq.7 ──────────────────────────────────────────────────────────────────
test('lanq.7: warehouseType=databricks folds source.path + column physical names to lower-case', () => {
  const model = { model: { tables: [tbl('orders', 'main', 'gold', 'orders', ['order_id', 'order_type'])] } };
  const el = warehouseEl(convertPowerBIToSigma(model as any, { connectionId: 'c', warehouseType: 'databricks' }));
  assert.deepEqual(el.source.path, ['main', 'gold', 'orders'], 'path must be lower-case for Databricks');
  assert.deepEqual((el.columns || []).map((c: any) => physOf(c.id)), ['order_id', 'order_type'], 'column physical names must be lower-case');
});

test('lanq.7: default (no warehouseType) stays UPPER — Snowflake unchanged', () => {
  const model = { model: { tables: [tbl('Sales', 'DB', 'PUBLIC', 'ORDERS', ['order_id', 'amount'])] } };
  const el = warehouseEl(convertPowerBIToSigma(model as any, { connectionId: 'c' }));
  assert.deepEqual(el.source.path, ['DB', 'PUBLIC', 'ORDERS'], 'default path must stay UPPER');
  assert.deepEqual((el.columns || []).map((c: any) => physOf(c.id)), ['ORDER_ID', 'AMOUNT'], 'default physical names must stay UPPER');
});

test('lanq.7: element name + [TABLE/Col] formula refs stay UPPER even for Databricks (Sigma-internal, agree with each other)', () => {
  const model = { model: { tables: [tbl('orders', 'main', 'gold', 'orders', ['order_id'])] } };
  const el = warehouseEl(convertPowerBIToSigma(model as any, { connectionId: 'c', warehouseType: 'databricks' }));
  assert.equal(el.name, 'ORDERS', 'element name stays UPPER');
  assert.match(el.columns[0].formula, /^\[ORDERS\//, 'formula element-segment stays UPPER to match the element name');
});

// ── lanq.6 ──────────────────────────────────────────────────────────────────
test('lanq.6: a single `schema` does NOT clobber per-table M-resolved schemas (multi-schema model)', () => {
  const model = { model: { tables: [
    tbl('A', 'cat', 'schema_a', 'tbl_a', ['id']),
    tbl('B', 'cat', 'schema_b', 'tbl_b', ['id']),
  ] } };
  const res = convertPowerBIToSigma(model as any, { connectionId: 'c', database: 'cat', schema: 'ONE_SCHEMA' });
  const a = warehouseEl(res, 'TBL_A');
  const b = warehouseEl(res, 'TBL_B');
  assert.equal(a.source.path[1], 'SCHEMA_A', 'table A keeps its own schema, not the override');
  assert.equal(b.source.path[1], 'SCHEMA_B', 'table B keeps its own schema, not the override');
});

test('lanq.6: single-schema model still HONORS the caller override (repoint, j89 behavior preserved)', () => {
  const model = { model: { tables: [
    tbl('A', 'cat', 'same_schema', 'tbl_a', ['id']),
    tbl('B', 'cat', 'same_schema', 'tbl_b', ['id']),
  ] } };
  const res = convertPowerBIToSigma(model as any, { connectionId: 'c', database: 'PROD', schema: 'HR' });
  const a = warehouseEl(res, 'TBL_A');
  assert.equal(a.source.path[0], 'PROD', 'single-catalog model: catalog override applies');
  assert.equal(a.source.path[1], 'HR', 'single-schema model: schema override applies (repoint)');
});

test('lanq.6: `schema` still FILLS when the model resolved no path (fallback)', () => {
  const model = { model: { tables: [{
    name: 'Mystery',
    columns: [{ name: 'id', dataType: 'string', sourceColumn: 'id' }],
    partitions: [{ name: 'p', source: { type: 'm', expression: 'let x = SomeUnparseableSource() in x' } }],
  }] } };
  const el = warehouseEl(convertPowerBIToSigma(model as any, { connectionId: 'c', database: 'DB', schema: 'FILLED' }));
  assert.equal(el.source.path[1], 'FILLED', 'override fills the schema slot when the model resolved none');
});

// ── shared sigma-ids casing param (backward-compatible) ──────────────────────
test('sigma-ids: casing param defaults to upper; lower folds down', () => {
  assert.equal(sigmaPhysicalName('region_name'), 'REGION_NAME');
  assert.equal(sigmaPhysicalName('region_name', 'lower'), 'region_name');
  assert.equal(sigmaPhysicalName('Region Name', 'lower'), 'region_name');
  assert.ok(sigmaInodeId('COL_A').endsWith('/COL_A'));
  assert.ok(sigmaInodeId('COL_A', 'lower').endsWith('/col_a'));
});
