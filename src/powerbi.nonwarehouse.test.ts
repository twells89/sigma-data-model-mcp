/**
 * Non-warehouse source detection — Fabric Dataflows / Lakehouse / OneLake /
 * Dataverse / file sources.
 *
 * The converter reads only the semantic model (model.bim/TMSL). A table whose
 * data lives in a Fabric Dataflow (etc.) has an M partition whose connector
 * pbiExtractPathFromM cannot resolve to a warehouse path. Instead of the vague
 * "using default" fallback, the converter must (1) recognize the source kind,
 * (2) emit a placeholder warehouse-table using the extracted entity name, (3)
 * push an actionable ⛔ warning pointing at the land-then-repoint path, and
 * (4) count it in stats.nonWarehouseSourcedTables so the skill can offer the
 * powerbi-import-to-snowflake landing tool.
 *
 * NOTE: no real dataflow-sourced .bim exists on disk — the M grammar below is
 * synthesized from documented Power Query connector syntax. Validate against a
 * real Fabric model before relying on the exact navigation keys.
 *
 * Run: node --import tsx/esm --test src/powerbi.nonwarehouse.test.ts
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { convertPowerBIToSigma, pbiDetectNonWarehouseSource } from './powerbi.js';

// ── documented non-warehouse M grammars ───────────────────────────────────────
const M_DATAFLOW = (entity: string) => [
  'let',
  '  Source = PowerPlatform.Dataflows(null),',
  '  Workspaces = Source{[workspaceId="ws-guid"]}[Data],',
  '  Flow = Workspaces{[dataflowId="df-guid"]}[Data],',
  `  Nav = Flow{[entity="${entity}", version=""]}[Data]`,
  '  in Nav',
].join('\n');
const M_LAKEHOUSE = (entity: string) => [
  'let',
  '  Source = Lakehouse.Contents(null),',
  '  WS = Source{[workspaceId="ws-guid"]}[Data],',
  '  LH = WS{[lakehouseId="lh-guid"]}[Data],',
  `  Nav = LH{[Id="${entity}", ItemKind="Table"]}[Data]`,
  '  in Nav',
].join('\n');
const M_DATAVERSE = (entity: string) => [
  'let',
  '  Source = CommonDataService.Database("org.crm.dynamics.com"),',
  `  Nav = Source{[EntitySetName="${entity}"]}[Data]`,
  '  in Nav',
].join('\n');
const M_FILE = (name: string) => [
  'let',
  `  Source = Excel.Workbook(File.Contents("C:\\\\data\\\\${name}.xlsx"), null, true),`,
  `  Sheet = Source{[Item="${name}", Kind="Sheet"]}[Data]`,
  '  in Sheet',
].join('\n');
// A real warehouse M (Snowflake Kind-nav) — must NOT be flagged.
const M_WAREHOUSE = [
  'let Source = Snowflake.Databases("x.snowflakecomputing.com","WH"),',
  'N1 = Source{[Name="CSA",Kind="Database"]}[Data],',
  'N2 = N1{[Name="TJ",Kind="Schema"]}[Data],',
  'N3 = N2{[Name="ORDER_FACT",Kind="Table"]}[Data] in N3',
].join('\n');

function tbl(name: string, m: string) {
  return {
    name,
    columns: [{ name: 'ID', dataType: 'int64', sourceColumn: 'ID' }, { name: 'VAL', dataType: 'double', sourceColumn: 'VAL' }],
    partitions: [{ source: { type: 'm', expression: m } }],
    measures: [],
  };
}
function model(tables: any[]) { return { model: { tables, relationships: [] } }; }
function elByPathTail(m: any, tail: string) {
  return m.pages[0].elements.find((e: any) => {
    const p = e?.source?.path; return Array.isArray(p) && p[p.length - 1] === tail;
  });
}

// ════ detector unit tests (grammar) ═══════════════════════════════════════════

test('detector: PowerPlatform.Dataflows → dataflow + entity', () => {
  assert.deepEqual(pbiDetectNonWarehouseSource(M_DATAFLOW('Sales')), { kind: 'dataflow', entity: 'Sales' });
});
test('detector: legacy PowerBI.Dataflows also matches', () => {
  const m = M_DATAFLOW('Sales').replace('PowerPlatform.Dataflows', 'PowerBI.Dataflows');
  assert.equal(pbiDetectNonWarehouseSource(m)?.kind, 'dataflow');
});
test('detector: Lakehouse.Contents → lakehouse + Id entity', () => {
  assert.deepEqual(pbiDetectNonWarehouseSource(M_LAKEHOUSE('DimStore')), { kind: 'lakehouse', entity: 'DimStore' });
});
test('detector: CommonDataService → dataverse + EntitySetName', () => {
  assert.deepEqual(pbiDetectNonWarehouseSource(M_DATAVERSE('accounts')), { kind: 'dataverse', entity: 'accounts' });
});
test('detector: Excel.Workbook → file + item', () => {
  assert.deepEqual(pbiDetectNonWarehouseSource(M_FILE('Budget')), { kind: 'file', entity: 'Budget' });
});
test('detector: a real Snowflake warehouse M returns null (not flagged)', () => {
  assert.equal(pbiDetectNonWarehouseSource(M_WAREHOUSE), null);
});
test('detector: empty / undefined M returns null', () => {
  assert.equal(pbiDetectNonWarehouseSource(''), null);
});

// ════ end-to-end through convertPowerBIToSigma ════════════════════════════════

test('convert: each non-warehouse kind flagged + placeholder path uses the entity name', () => {
  const { model: m, warnings, stats } = convertPowerBIToSigma(model([
    tbl('Orders', M_WAREHOUSE),          // control — real warehouse
    tbl('SalesFlow', M_DATAFLOW('Sales')),
    tbl('StoreLake', M_LAKEHOUSE('DimStore')),
    tbl('Accounts', M_DATAVERSE('accounts')),
    tbl('BudgetXlsx', M_FILE('Budget')),
  ]), { connectionId: 'c', database: 'ANALYTICS', schema: 'STAGE' });

  // 4 of 5 tables are non-warehouse.
  assert.equal(stats.nonWarehouseSourcedTables, 4);

  // placeholder path = <db>.<schema>.<TABLE_NAME_UPPERCASED> — the TABLE name in
  // the SAME form the base column formulas use ([TABLE/Col]) so --table-map's
  // formula rewrite matches. NOT the source entity (that's in the warning below).
  assert.deepEqual(elByPathTail(m, 'SALESFLOW')?.source.path, ['ANALYTICS', 'STAGE', 'SALESFLOW']);
  assert.deepEqual(elByPathTail(m, 'STORELAKE')?.source.path, ['ANALYTICS', 'STAGE', 'STORELAKE']);
  assert.deepEqual(elByPathTail(m, 'ACCOUNTS')?.source.path, ['ANALYTICS', 'STAGE', 'ACCOUNTS']);
  assert.deepEqual(elByPathTail(m, 'BUDGETXLSX')?.source.path, ['ANALYTICS', 'STAGE', 'BUDGETXLSX']);

  // the control warehouse table resolved normally (NOT counted, real path).
  assert.deepEqual(elByPathTail(m, 'ORDER_FACT')?.source.path, ['ANALYTICS', 'STAGE', 'ORDER_FACT']);

  // actionable ⛔ warnings name the kind + entity + the remediation path.
  assert.ok(warnings.some(w => /⛔/.test(w) && /SalesFlow/.test(w) && /Dataflow/.test(w)
    && /entity "Sales"/.test(w) && /powerbi-import-to-snowflake/.test(w) && /--table-map/.test(w)),
    'dataflow warning missing kind/entity/remediation');
  assert.ok(warnings.some(w => /⛔/.test(w) && /StoreLake/.test(w) && /Lakehouse\/OneLake/.test(w)));
  assert.ok(warnings.some(w => /⛔/.test(w) && /Accounts/.test(w) && /Dataverse/.test(w)));
  assert.ok(warnings.some(w => /⛔/.test(w) && /BudgetXlsx/.test(w) && /file source/.test(w)));

  // the control table must NOT emit the vague "using default" warning.
  assert.ok(!warnings.some(w => /Orders.*could not extract source path/.test(w)));
});

test('convert: no non-warehouse tables → stat is absent (conditional field)', () => {
  const { stats } = convertPowerBIToSigma(model([tbl('Orders', M_WAREHOUSE)]), { connectionId: 'c' });
  assert.equal(stats.nonWarehouseSourcedTables, undefined);
});
