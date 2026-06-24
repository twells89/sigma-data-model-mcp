/**
 * Modern Tableau object-model datasource tests.
 *
 * Tableau 2021.1+ ("Object Model / EncapsulateLegacy") no longer emits a plain
 * <relation> under <connection>; it emits a feature-flag-namespaced element
 * (literal tag `_.fcp.ObjectModelEncapsulateLegacy.true...relation`). The
 * converter previously read only `connection.relation`, got undefined, and
 * produced an EMPTY data model (0 elements / 0 columns) on every modern .twb —
 * the "complex dashboards don't migrate" failure. These tests guard:
 *   1. FCP-namespaced relation is resolved (collection tree is reached).
 *   2. Columns are extracted from <metadata-records> (was 0).
 *   3. A Custom SQL (type='text') child becomes a kind:'sql' element carrying
 *      the actual SQL, NOT a fake warehouse-table path.
 */

import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { convertTableauToSigma } from './tableau.js';

const META = (objId: string) => `
  <metadata-records>
    <metadata-record class='column'><remote-name>A</remote-name><caption>A</caption>
      <object-id>[${objId}]</object-id><local-type>string</local-type></metadata-record>
    <metadata-record class='column'><remote-name>B</remote-name><caption>B</caption>
      <object-id>[${objId}]</object-id><local-type>integer</local-type></metadata-record>
  </metadata-records>`;

const modernCustomSqlTwb = `<?xml version='1.0' encoding='utf-8'?>
<workbook>
  <datasources>
    <datasource caption='DS' name='federated.x'>
      <connection class='federated'>
        <named-connections>
          <named-connection name='snow' caption='snow'>
            <connection class='snowflake' dbname='DB' schema='SC'/>
          </named-connection>
        </named-connections>
        <_.fcp.ObjectModelEncapsulateLegacy.true...relation type='collection'>
          <relation connection='snow' name='Custom SQL Query1' type='text'>select A, B from MY_TABLE</relation>
        </_.fcp.ObjectModelEncapsulateLegacy.true...relation>
        ${META('Custom SQL Query1')}
      </connection>
    </datasource>
  </datasources>
</workbook>`;

describe('modern object-model (FCP-namespaced) datasource', () => {
  test('FCP-namespaced relation resolves → non-empty model with extracted columns', () => {
    const out: any = convertTableauToSigma(modernCustomSqlTwb, {
      connectionId: 'c1', database: 'CSA', schema: 'TJ', datasourceIndex: 0,
    });
    const els = (out.model?.pages?.[0]?.elements || []).filter(
      (e: any) => e.source?.kind === 'sql' || e.source?.kind === 'warehouse-table');
    assert.ok(els.length >= 1, 'at least one data element built (was 0 before the FCP fix)');
    const totalCols = els.reduce((s: number, e: any) => s + (e.columns?.length || 0), 0);
    assert.ok(totalCols >= 2, `columns extracted from metadata-records (got ${totalCols})`);
  });

  test('Custom SQL (type=text) child → kind:sql element carrying the SQL, not a fake table path', () => {
    const out: any = convertTableauToSigma(modernCustomSqlTwb, {
      connectionId: 'c1', database: 'CSA', schema: 'TJ', datasourceIndex: 0,
    });
    const els = out.model?.pages?.[0]?.elements || [];
    const sqlEl = els.find((e: any) => e.source?.kind === 'sql');
    assert.ok(sqlEl, 'Custom SQL relation emitted as kind:sql');
    assert.match(sqlEl.source.statement, /select\s+A,\s*B\s+from\s+MY_TABLE/i,
      'kind:sql element carries the original Custom SQL statement');
    // It must NOT have invented a warehouse path named after the query.
    const fakePath = els.find((e: any) =>
      e.source?.kind === 'warehouse-table' &&
      (e.source.path || []).some((p: string) => /CUSTOM SQL QUERY/i.test(p)));
    assert.equal(fakePath, undefined, 'no fake warehouse-table path for a Custom SQL relation');
  });
});
