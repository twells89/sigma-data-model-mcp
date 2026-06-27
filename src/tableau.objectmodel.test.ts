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

// ── Encapsulated-legacy variant (DDMX-class: multi-custom-SQL, parent-name
//    grouping, composite relationships, GUID-only columns) ───────────────────
// This is the shape that produced "1 populated element + N empty stubs + 0
// relationships" until the per-object column distribution + composite-relationship
// wiring fixes. metadata-records here carry NO <object-id> (only the namespaced
// `…object-id`) and group by <parent-name> = the relation name.
const MR = (remote: string, parent: string, objHash: string, type = 'string', localName?: string) => `
    <metadata-record class='column'>
      <remote-name>${remote}</remote-name>
      <local-name>[${localName ?? remote}]</local-name>
      <parent-name>[${parent}]</parent-name>
      <local-type>${type}</local-type>
      <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[${objHash}]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
    </metadata-record>`;

const FACT_OBJ = 'FACT (DB.FACT)_AAAA1111';
const DIM_OBJ  = 'DIM (DB.DIM)_BBBB2222';
const GUID_COL = 'c2ec6b07-897e-39ab-9422-aa895d35a627';

const encapsulatedTwb = `<?xml version='1.0' encoding='utf-8'?>
<workbook>
  <datasources>
    <datasource caption='DS' name='federated.x'>
      <connection class='federated'>
        <named-connections>
          <named-connection name='snow'><connection class='snowflake' dbname='DB' schema='SC'/></named-connection>
        </named-connections>
        <_.fcp.ObjectModelEncapsulateLegacy.true...relation type='collection'>
          <relation connection='snow' name='Custom SQL Query1' type='text'>select QTR_YR from FACT_TBL</relation>
          <relation connection='snow' name='Custom SQL Query2' type='text'>select QTR_YR_DIM, LABEL from DIM_TBL</relation>
        </_.fcp.ObjectModelEncapsulateLegacy.true...relation>
        <metadata-records>
          ${MR('QTR_YR', 'Custom SQL Query1', FACT_OBJ)}
          ${MR(GUID_COL, 'Custom SQL Query1', FACT_OBJ, 'date')}
          ${MR('QTR_YR_DIM', 'Custom SQL Query2', DIM_OBJ)}
          ${MR('LABEL', 'Custom SQL Query2', DIM_OBJ)}
        </metadata-records>
      </connection>
      <_.fcp.ObjectModelEncapsulateLegacy.true...object-graph>
        <objects>
          <object caption='FACT' id='${FACT_OBJ}'><properties/></object>
          <object caption='DIM' id='${DIM_OBJ}'><properties/></object>
        </objects>
        <relationships>
          <relationship>
            <first-end-point object-id='${FACT_OBJ}'/>
            <second-end-point object-id='${DIM_OBJ}'/>
            <expression op='AND'>
              <expression op='='>
                <expression op='[QTR_YR]'/>
                <expression op='[QTR_YR_DIM]'/>
              </expression>
              <expression op='='>
                <expression _.fcp.RelationshipCalculations.true...op='DATE([QTR_YR])'/>
                <expression op='[LABEL]'/>
              </expression>
            </expression>
          </relationship>
        </relationships>
      </_.fcp.ObjectModelEncapsulateLegacy.true...object-graph>
    </datasource>
  </datasources>
</workbook>`;

describe('encapsulated-legacy object model (DDMX-class)', () => {
  const out: any = convertTableauToSigma(encapsulatedTwb, { connectionId: 'c1', database: 'DB', schema: 'SC', datasourceIndex: 0 });
  const els = (out.model?.pages?.[0]?.elements || []).filter((e: any) => e.source?.kind === 'sql');

  test('columns distribute per object via parent-name (not all on element[0])', () => {
    assert.equal(els.length, 2, 'two custom-SQL elements');
    const populated = els.filter((e: any) => (e.columns || []).length > 0);
    assert.equal(populated.length, 2, 'BOTH elements populated (was 1 + empty stub)');
    const allFormulas = els.flatMap((e: any) => (e.columns || []).map((c: any) => c.formula)).join(' ');
    assert.match(allFormulas, /QTR_YR_DIM/, 'dim object got its own column');
  });

  test('GUID-only metadata column is skipped (no unresolvable [TABLE/<guid>] ref)', () => {
    const allCols = els.flatMap((e: any) => e.columns || []);
    const guidCol = allCols.find((c: any) =>
      (c.name && new RegExp(GUID_COL, 'i').test(c.name)) ||
      (c.formula && new RegExp(GUID_COL, 'i').test(c.formula)));
    assert.equal(guidCol, undefined, 'GUID-named column must not be emitted');
  });

  test('composite relationship wires the physical key, drops the computed condition', () => {
    const withRels = els.find((e: any) => (e.relationships || []).length > 0);
    assert.ok(withRels, 'a relationship was wired (object-graph found under namespaced key)');
    const rel = withRels.relationships[0];
    assert.equal(rel.keys.length, 1, 'one physical key wired (the computed DATE(...) condition dropped)');
    assert.ok(
      (out.warnings || []).some((w: string) => /computed condition\(s\) dropped/i.test(w)),
      'a warning surfaces the dropped computed join condition');
  });
});
