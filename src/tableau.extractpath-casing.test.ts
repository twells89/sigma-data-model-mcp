/**
 * Warehouse-class-aware physical table-path casing in extractPath
 * (companion to sigma-migration-skills #454, Databricks).
 *
 * ROOT CAUSE: extractPath UPPERCASED every table-path segment unconditionally
 * (a Snowflake bias — Snowflake up-folds unquoted identifiers). Databricks /
 * Spark / Hive / Delta are case-PRESERVING and bind against the physical name
 * exactly as written, so an uppercased path ("Order_Fact" → "ORDER_FACT") names
 * a table the warehouse does not have and the DM POST 404s. #454 made tableau
 * hydration pass the correct warehouse class; the converter must now honor it.
 *
 * FIX: extractPath takes a `casing` policy threaded from the workbook's own
 * warehouse connection class (or the explicit `warehouseType` option). Snowflake
 * / unknown → UPPER (unchanged for every historical caller); case-preserving
 * classes → the table segment is kept verbatim.
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { convertTableauToSigma } from './tableau.js';

// A single-table datasource whose live warehouse connection has class `cls` and
// whose physical table name is `table` (deliberately MIXED-CASE so the casing
// policy is observable in the emitted source.path).
const dsBlock = (cls: string, table: string) => `
    <datasource caption='DS' name='federated.ds'>
      <connection class='federated'>
        <named-connections>
          <named-connection name='c' caption='c'><connection class='${cls}' dbname='CSA' schema='TJ'/></named-connection>
        </named-connections>
        <_.fcp.ObjectModelEncapsulateLegacy.true...relation type='collection'>
          <relation connection='c' name='${table}' table='[${table}]' type='table'/>
        </_.fcp.ObjectModelEncapsulateLegacy.true...relation>
        <metadata-records>
          <metadata-record class='column'><remote-name>ORDER_ID</remote-name><caption>Order Id</caption><object-id>[${table}]</object-id><local-type>integer</local-type></metadata-record>
          <metadata-record class='column'><remote-name>AMOUNT</remote-name><caption>Amount</caption><object-id>[${table}]</object-id><local-type>real</local-type></metadata-record>
        </metadata-records>
      </connection>
    </datasource>`;

const ws = `
  <worksheet name='W'>
    <table><view><datasources><datasource name='federated.ds' caption='W'/></datasources></view></table>
  </worksheet>`;

const wb = (cls: string, table: string) =>
  `<?xml version='1.0' encoding='utf-8'?>\n<workbook>\n<datasources>\n${dsBlock(cls, table)}\n</datasources>\n${ws}\n</workbook>`;

// Every warehouse-table source.path array in the emitted model.
function allPaths(model: any): string[][] {
  const paths: string[][] = [];
  const walk = (n: any): void => {
    if (Array.isArray(n)) { n.forEach(walk); return; }
    if (n && typeof n === 'object') {
      if (n.source && n.source.kind === 'warehouse-table' && Array.isArray(n.source.path)) {
        paths.push(n.source.path as string[]);
      }
      for (const v of Object.values(n)) walk(v);
    }
  };
  walk(model);
  return paths;
}

// The physical table segment (last element) of the sole emitted path.
function tableSegment(cls: string, table: string, opts: any = {}): string {
  const model = convertTableauToSigma(wb(cls, table), { connectionId: 'c1', ...opts }).model;
  const paths = allPaths(model);
  assert.equal(paths.length, 1, `expected exactly one warehouse-table path; got ${JSON.stringify(paths)}`);
  return paths[0][paths[0].length - 1];
}

const MIXED = 'Order_Fact';

describe('extractPath casing — Snowflake up-folds (unchanged default)', () => {
  test('snowflake connection class → table path UPPERCASED', () => {
    assert.equal(tableSegment('snowflake', MIXED), 'ORDER_FACT');
  });

  test('bigquery (another up-folding warehouse) → UPPERCASED', () => {
    assert.equal(tableSegment('bigquery', MIXED), 'ORDER_FACT');
  });
});

describe('extractPath casing — case-preserving warehouses keep verbatim (#454)', () => {
  for (const cls of ['databricks', 'spark', 'hive', 'delta']) {
    test(`${cls} connection class → table path PRESERVED (not uppercased)`, () => {
      assert.equal(tableSegment(cls, MIXED), MIXED);
    });
  }

  test('a lower-case Databricks identifier is kept lower-case (would 404 if uppercased)', () => {
    assert.equal(tableSegment('databricks', 'orders_daily'), 'orders_daily');
  });
});

describe('extractPath casing — unknown class defaults to UPPER (documented Snowflake-parity default)', () => {
  test('an unrecognized warehouse class → UPPERCASED (safe historical default)', () => {
    assert.equal(tableSegment('some_new_warehouse', MIXED), 'ORDER_FACT');
  });
});

describe('extractPath casing — explicit warehouseType option overrides the connection class', () => {
  test('warehouseType:"databricks" over a snowflake connection → PRESERVED', () => {
    assert.equal(tableSegment('snowflake', MIXED, { warehouseType: 'databricks' }), MIXED);
  });

  test('warehouseType:"snowflake" over a databricks connection → UPPERCASED', () => {
    assert.equal(tableSegment('databricks', MIXED, { warehouseType: 'snowflake' }), 'ORDER_FACT');
  });
});
