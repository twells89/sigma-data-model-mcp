/**
 * Tableau internal/virtual fields ([:Measure Names], [:Measure Values]) must
 * NOT be emitted as DM columns — they are pivot pseudo-fields, not warehouse
 * columns, and produce an unresolvable [TABLE/:Measure Names] ref (field
 * report #8).
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { convertTableauToSigma } from './tableau.js';

const twb = `<?xml version='1.0'?><workbook><datasources>
 <datasource caption='Sales' name='federated.s'>
  <connection class='snowflake' dbname='WH' schema='PUBLIC'>
    <relation name='SALES' table='[SALES]' type='table'>
      <columns>
        <column name='[Region]' datatype='string'/>
        <column name='[Amount]' datatype='real'/>
        <column name='[:Measure Names]' datatype='string'/>
        <column name='[:Measure Values]' datatype='real'/>
      </columns>
    </relation>
  </connection>
 </datasource></datasources>
 <worksheet name='W'><table><view><datasources><datasource name='federated.s'/></datasources></view></table></worksheet>
</workbook>`;

describe('Tableau virtual fields are not emitted as columns', () => {
  const out: any = convertTableauToSigma(twb, { connectionId: 'c1', database: 'WH', schema: 'PUBLIC' });
  const cols = out.model.pages[0].elements.flatMap((e: any) => (e.columns || []));

  test('real columns are kept', () => {
    const f = cols.map((c: any) => c.formula).join(' ');
    assert.match(f, /region/i);
    assert.match(f, /amount/i);
  });

  test('[:Measure Names] / [:Measure Values] are dropped (no unresolvable ref)', () => {
    assert.doesNotMatch(JSON.stringify(out.model), /:Measure Names|:Measure Values/,
      'no :Measure Names/Values pseudo-field reaches the spec');
  });
});
