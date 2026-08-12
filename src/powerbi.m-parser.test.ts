/**
 * beads-sigma-lanq.9 — pbiExtractPathFromM warehouse-family coverage.
 *
 * The M-parser used to resolve ONLY Snowflake/Databricks-A Kind-nav, bare-Name
 * positional nav, and a 3-part FROM. Whole connector families fell through to the
 * "could not extract source path" default → wrong table binding. These pin the
 * VERBATIM M shapes (ground-truthed against real public PBIP 2026-07-17; sources
 * in refs/pbi-model-gotchas.md) each warehouse connector emits, driven through the
 * exported convertPowerBIToSigma and asserted on element.source.path.
 *
 * Run: node --import tsx/esm --test src/powerbi.m-parser.test.ts
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { convertPowerBIToSigma } from './powerbi.js';

const build = (m: string, opts: any = {}) => {
  const model = { model: { tables: [{
    name: 'T',
    columns: [{ name: 'c1', dataType: 'string', sourceColumn: 'c1' }],
    partitions: [{ name: 'p', source: { type: 'm', expression: m } }],
  }] } };
  const res = convertPowerBIToSigma(model as any, { connectionId: 'c', ...opts });
  const spec = (res as any).model || res;
  const all = (spec.elements || []).concat(...(spec.pages || []).map((p: any) => p.elements || []));
  return all.find((e: any) => e.source && e.source.kind === 'warehouse-table');
};
const pathOf = (m: string, opts: any = {}) => (build(m, opts) || {}).source?.path;

// ── SQL Server / Synapse: Sql.Database("srv","DB") + {[Schema="dbo",Item="T"]} ──
test('lanq.9: SQL Server flat [Schema=,Item=] -> [DB, schema, table]', () => {
  const m = `let\n Source = Sql.Database("synapse.sql.azuresynapse.net", "tpch"),\n dbo_ORDERS = Source{[Schema="dbo",Item="ORDERS"]}[Data]\nin dbo_ORDERS`;
  assert.deepEqual(pathOf(m), ['TPCH', 'DBO', 'ORDERS']);
});

// ── PostgreSQL: PostgreSQL.Database("srv","db") + {[Schema=,Item=]} -> [schema, table] (2-part) ──
test('lanq.9: PostgreSQL flat [Schema=,Item=] -> [schema, table] (no db tier)', () => {
  const m = `let\n Source = PostgreSQL.Database("localhost:5432", "fastlab"),\n fast_fato = Source{[Schema="fast",Item="fato_exame"]}[Data]\nin fast_fato`;
  assert.deepEqual(pathOf(m), ['FAST', 'FATO_EXAME']);
});

// ── Databricks shape-B: DatabricksMultiCloud.Catalogs + {[Item=,Schema=,Catalog=]} ──
test('lanq.9: Databricks shape-B single-record [Item,Schema,Catalog] -> [catalog, schema, table]', () => {
  const m = `let\n Source = DatabricksMultiCloud.Catalogs("dbc.cloud.databricks.com", "/sql/1.0/warehouses/x", [Catalog = "mlops_dev", Database = "hotel_operations"]),\n nav = Source{[Item="continuous_reservation",Schema="hotel_operations",Catalog="mlops_dev"]}[Data]\nin nav`;
  assert.deepEqual(pathOf(m, { warehouseType: 'databricks' }), ['mlops_dev', 'hotel_operations', 'continuous_reservation']);
});

// ── BigQuery v1: project is a bare {[Name=]}[Data] nav; dataset/table are Kind-tagged ──
test('lanq.9: BigQuery v1 recovers the bare-Name project tier -> [project, dataset, table]', () => {
  const m = `let\n Source = GoogleBigQuery.Database(),\n #"proj" = Source{[Name="crypto-minutia-490711"]}[Data],\n ds = #"proj"{[Name="analytics",Kind="Schema"]}[Data],\n t = ds{[Name="dim_time",Kind="Table"]}[Data]\nin t`;
  assert.deepEqual(pathOf(m), ['CRYPTO-MINUTIA-490711', 'ANALYTICS', 'DIM_TIME']);
});

// ── BigQuery 2.0: project Kind="Database" (regression — must still work) ──
test('lanq.9: BigQuery Impl=2.0 Kind-tagged project -> [project, dataset, table]', () => {
  const m = `let\n Fonte = GoogleBigQuery.Database([Implementation="2.0"]),\n a = Fonte{[Name="cancel-484723",Kind="Database"]}[Data],\n b = a{[Name="Tabela",Kind="Schema"]}[Data],\n c = b{[Name="Clean",Kind="Table"]}[Data]\nin c`;
  assert.deepEqual(pathOf(m), ['CANCEL-484723', 'TABELA', 'CLEAN']);
});

// ── Native query (Databricks): Value.NativeQuery over a navigated catalog, FROM schema.table ──
test('lanq.9: Value.NativeQuery FROM schema.table prepends the navigated catalog', () => {
  const m = `let\n Source = Value.NativeQuery(Databricks.Catalogs("adb.azuredatabricks.net", "/sql/1.0/warehouses/x", [Catalog="hive_metastore", Database=null]){[Name="tpch",Kind="Database"]}[Data], "select * from sf1.region", null, [EnableFolding=true])\nin Source`;
  assert.deepEqual(pathOf(m), ['TPCH', 'SF1', 'REGION']);
});

// ── Native query (SQL Server): Sql.Database(...,[Query="... FROM dbo.T"]) with #(lf) escapes ──
test('lanq.9: Sql.Database [Query=] with #(lf) -> [db, schema, table]', () => {
  const m = `let\n Source = Sql.Database("srv", "RetailBIDW", [Query="select a, b#(lf)from dbo.Sales#(lf)where x > 0"])\nin Source`;
  assert.deepEqual(pathOf(m), ['RETAILBIDW', 'DBO', 'SALES']);
});

// ── Regressions: the already-handled shapes must be unchanged ──
test('lanq.9 regression: Snowflake Kind-nav still resolves [DB, schema, table]', () => {
  const m = `let\n Source = Snowflake.Databases("acct.snowflakecomputing.com","WH"),\n a = Source{[Name="SNOWFLAKE_SAMPLE_DATA",Kind="Database"]}[Data],\n b = a{[Name="TPCH_SF1",Kind="Schema"]}[Data],\n c = b{[Name="ORDERS",Kind="Table"]}[Data]\nin c`;
  assert.deepEqual(pathOf(m), ['SNOWFLAKE_SAMPLE_DATA', 'TPCH_SF1', 'ORDERS']);
});
test('lanq.9 regression: Databricks shape-A Kind="Database" catalog still resolves', () => {
  const m = `let\n Source = Databricks.Catalogs("h","p",[]),\n a = Source{[Name="main",Kind="Database"]}[Data],\n b = a{[Name="gold",Kind="Schema"]}[Data],\n c = b{[Name="orders",Kind="Table"]}[Data]\nin c`;
  assert.deepEqual(pathOf(m, { warehouseType: 'databricks' }), ['main', 'gold', 'orders']);
});
test('lanq.9 regression: Redshift 2 bare-Name navs still resolve [schema, table]', () => {
  const m = `let\n Source = AmazonRedshift.Database("cluster.redshift.amazonaws.com:5439","dev"),\n tpch = Source{[Name="tpch"]}[Data],\n orders = tpch{[Name="orders"]}[Data]\nin orders`;
  assert.deepEqual(pathOf(m), ['TPCH', 'ORDERS']);
});
