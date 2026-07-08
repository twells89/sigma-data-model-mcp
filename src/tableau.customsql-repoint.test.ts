/**
 * --db/--schema override must also repoint a Custom SQL element's FROM clause
 * (field report #7). Previously the override rewrote warehouse-table element
 * paths but left the raw Custom SQL `FROM <db>.<schema>.<table>` on the
 * Tableau-side db/schema, so a repointed model queried the wrong catalog.
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { convertTableauToSigma } from './tableau.js';

const twb = `<?xml version='1.0'?><workbook><datasources>
 <datasource caption='CS' name='federated.cs'>
  <connection class='snowflake' dbname='CSA' schema='TJ'>
    <relation name='Custom SQL Query' type='text'>SELECT ORDER_ID, SEGMENT FROM CSA.TJ.SUPERSTORE_ORDERS</relation>
    <metadata-records>
      <metadata-record class='column'><remote-name>ORDER_ID</remote-name><local-type>integer</local-type></metadata-record>
      <metadata-record class='column'><remote-name>SEGMENT</remote-name><local-type>string</local-type></metadata-record>
    </metadata-records>
  </connection>
 </datasource></datasources>
 <worksheet name='W'><table><view><datasources><datasource name='federated.cs'/></datasources></view></table></worksheet>
</workbook>`;

const sqlOf = (o: any) => o.model.pages[0].elements
  .filter((e: any) => e.source?.kind === 'sql').map((e: any) => e.source.statement).join('\n');

describe('Custom SQL FROM repoint under --db/--schema override', () => {
  test('override rewrites the FROM db.schema prefix', () => {
    const o = convertTableauToSigma(twb, { connectionId: 'c1', database: 'EDNA', schema: 'CONS_ESTUARY' });
    const s = sqlOf(o);
    assert.match(s, /FROM\s+EDNA\.CONS_ESTUARY\.SUPERSTORE_ORDERS/i, 'FROM repointed to the override');
    assert.doesNotMatch(s, /CSA\.TJ\.SUPERSTORE_ORDERS/i, 'no Tableau-side db/schema remains');
  });

  test('no override leaves the original FROM untouched', () => {
    const o = convertTableauToSigma(twb, { connectionId: 'c1' });
    assert.match(sqlOf(o), /FROM\s+CSA\.TJ\.SUPERSTORE_ORDERS/i);
  });

  test('override matching the source db/schema is a no-op (no double-rewrite)', () => {
    const o = convertTableauToSigma(twb, { connectionId: 'c1', database: 'CSA', schema: 'TJ' });
    assert.match(sqlOf(o), /FROM\s+CSA\.TJ\.SUPERSTORE_ORDERS/i);
  });

  test('a FROM in a DIFFERENT db/schema is left alone (targeted rewrite)', () => {
    const other = twb.replace('CSA.TJ.SUPERSTORE_ORDERS', 'OTHERDB.OTHERSC.LOOKUP');
    const o = convertTableauToSigma(other, { connectionId: 'c1', database: 'EDNA', schema: 'CONS_ESTUARY' });
    assert.match(sqlOf(o), /OTHERDB\.OTHERSC\.LOOKUP/i, 'unrelated db/schema untouched');
  });
});
