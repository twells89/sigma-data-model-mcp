/**
 * Multi-datasource workbook → multi-element data model.
 *
 * A workbook whose worksheets draw on several INDEPENDENT datasources (NOT a
 * Tableau blend) used to collapse onto datasources[0] — the other sources'
 * columns were dropped, so the workbook spec referenced columns the DM never
 * had and the POST failed "Dependency not found" (a real multi-datasource workbook).
 * The converter now builds one element per datasource. These guard that.
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { convertTableauToSigma } from './tableau.js';

const dsBlock = (caption: string, name: string, table: string, cols: Array<[string, string, string]>) => `
    <datasource caption='${caption}' name='${name}'>
      <connection class='federated'>
        <named-connections>
          <named-connection name='c' caption='c'><connection class='snowflake' dbname='CSA' schema='TJ'/></named-connection>
        </named-connections>
        <_.fcp.ObjectModelEncapsulateLegacy.true...relation type='collection'>
          <relation connection='c' name='${table}' table='[${table}]' type='table'/>
        </_.fcp.ObjectModelEncapsulateLegacy.true...relation>
        <metadata-records>
          ${cols.map(([rn, cap, lt]) => `<metadata-record class='column'><remote-name>${rn}</remote-name><caption>${cap}</caption><object-id>[${table}]</object-id><local-type>${lt}</local-type></metadata-record>`).join('\n          ')}
        </metadata-records>
      </connection>
    </datasource>`;

const ws = (name: string, ds: string) => `
  <worksheet name='${name}'>
    <table><view><datasources><datasource name='${ds}' caption='${name}'/></datasources></view></table>
  </worksheet>`;

const twb = `<?xml version='1.0' encoding='utf-8'?>
<workbook>
  <datasources>
    ${dsBlock('Sales Funnel', 'federated.sales', 'ORDER_FACT', [['ORDER_ID', 'Order Id', 'integer'], ['AMOUNT', 'Amount', 'real']])}
    ${dsBlock('SKU Master GA', 'federated.sku', 'CUSTOMER_DIM', [['CUSTOMER_ID', 'Customer Id', 'integer'], ['CUSTOMER_NAME', 'Customer Name', 'string']])}
  </datasources>
  ${ws('Funnel', 'federated.sales')}
  ${ws('SKUs', 'federated.sku')}
</workbook>`;

describe('multi-datasource workbook (independent sources, not a blend)', () => {
  const out: any = convertTableauToSigma(twb, { connectionId: 'c1', database: 'CSA', schema: 'TJ' });
  const dataEls = (out.model?.pages?.[0]?.elements || []).filter(
    (e: any) => e.source?.kind === 'warehouse-table' || e.source?.kind === 'sql');

  test('builds one element PER datasource (not collapsed to the first)', () => {
    assert.ok(dataEls.length >= 2, `expected >=2 data elements, got ${dataEls.length}`);
  });

  test('columns from BOTH datasources survive (no silent drop)', () => {
    const allCols = dataEls.flatMap((e: any) => (e.columns || []).map((c: any) => `${c.formula || ''} ${c.name || ''}`)).join(' | ');
    assert.match(allCols, /Amount/, 'ORDER_FACT (Sales Funnel) columns present');
    assert.match(allCols, /Customer Name/, 'CUSTOMER_DIM (SKU Master GA) columns present');
  });

  test('every data element carries its own columns (none empty)', () => {
    for (const e of dataEls) {
      assert.ok((e.columns || []).length > 0, `element ${e.name || e.id} has columns`);
    }
  });

  test('emits the multi-datasource explanation warning', () => {
    assert.ok((out.warnings || []).some((w: string) => /multi-datasource/i.test(w) && /multi-element/i.test(w)),
      'a multi-datasource → multi-element warning is present');
  });

  test('single-datasource conversion is unchanged (no regression)', () => {
    const single: any = convertTableauToSigma(
      `<?xml version='1.0'?><workbook><datasources>${dsBlock('Only', 'federated.only', 'ORDER_FACT', [['ORDER_ID', 'Order Id', 'integer']])}</datasources>${ws('W', 'federated.only')}</workbook>`,
      { connectionId: 'c1', database: 'CSA', schema: 'TJ' });
    assert.ok(!(single.warnings || []).some((w: string) => /multi-datasource/i.test(w)),
      'single-datasource workbook does NOT trigger the multi-element path');
  });
});
