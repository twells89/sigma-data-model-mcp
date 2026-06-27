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
    // Bug #1 (epic n4pi): a kind:sql element's OWN columns must use the literal
    // `Custom SQL` source-alias prefix (data-model-spec rule 3) — NOT the Tableau
    // relation name ("Custom SQL Query1"). The relation-name prefix compiles every
    // column to type=error at POST and was the root cause of blank DDMX dashboards.
    for (const c of (sqlEl.columns || [])) {
      assert.match(c.formula, /^\[Custom SQL\//,
        `kind:sql column formula must be [Custom SQL/...], got ${c.formula}`);
      assert.doesNotMatch(c.formula, /CUSTOM SQL QUERY/i,
        `column formula must not use the Tableau relation name as prefix: ${c.formula}`);
    }
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

  test('a single secondary island is NOT collapsed (stays a native relationship)', () => {
    // Only multi-source blends (≥2 secondaries) collapse; a fact+dim stays 2 elements.
    assert.equal(els.length, 2, 'fact + single dim remain two elements with a relationship');
  });
});

// ── Multi-source blend collapse (epic n4pi.2, fork b) ──────────────────────────
// A fact joined to ≥2 custom-SQL goal islands of MIXED grain. A Sigma master can
// source only one element, so cross-island chart refs ("master/goal") fail and
// the dashboard is blank. The converter must collapse the blend into ONE wide
// kind:'sql' element: each secondary pre-aggregated to its link grain (SUM for
// additive measures, MAX for dims) then LEFT JOINed onto the fact.
const blendTwb = `<?xml version='1.0' encoding='utf-8'?>
<workbook>
  <datasources>
    <datasource caption='DS' name='federated.x'>
      <connection class='federated'>
        <named-connections>
          <named-connection name='snow'><connection class='snowflake' dbname='DB' schema='SC'/></named-connection>
        </named-connections>
        <_.fcp.ObjectModelEncapsulateLegacy.true...relation type='collection'>
          <relation connection='snow' name='Custom SQL Query1' type='text'>select ROLE, WK, SALES from FACT_TBL</relation>
          <relation connection='snow' name='Custom SQL Query2' type='text'>select ROLE_G, WK_G, GOAL from WEEKLY_GOALS</relation>
          <relation connection='snow' name='Custom SQL Query3' type='text'>select ROLE_P, PRODUCT, PGOAL from SC.PRODUCT_GOALS</relation>
        </_.fcp.ObjectModelEncapsulateLegacy.true...relation>
        <metadata-records>
          ${MR('ROLE',    'Custom SQL Query1', 'F (DB.F)_AAAA', 'string')}
          ${MR('WK',      'Custom SQL Query1', 'F (DB.F)_AAAA', 'integer')}
          ${MR('SALES',   'Custom SQL Query1', 'F (DB.F)_AAAA', 'real')}
          ${MR('ROLE_G',  'Custom SQL Query2', 'G (DB.G)_BBBB', 'string')}
          ${MR('WK_G',    'Custom SQL Query2', 'G (DB.G)_BBBB', 'integer')}
          ${MR('GOAL',    'Custom SQL Query2', 'G (DB.G)_BBBB', 'real')}
          ${MR('ROLE_P',  'Custom SQL Query3', 'P (DB.P)_CCCC', 'string')}
          ${MR('PRODUCT', 'Custom SQL Query3', 'P (DB.P)_CCCC', 'string')}
          ${MR('PGOAL',   'Custom SQL Query3', 'P (DB.P)_CCCC', 'real')}
        </metadata-records>
      </connection>
      <_.fcp.ObjectModelEncapsulateLegacy.true...object-graph>
        <objects>
          <object caption='F' id='F (DB.F)_AAAA'><properties/></object>
          <object caption='G' id='G (DB.G)_BBBB'><properties/></object>
          <object caption='P' id='P (DB.P)_CCCC'><properties/></object>
        </objects>
        <relationships>
          <relationship>
            <first-end-point object-id='F (DB.F)_AAAA'/>
            <second-end-point object-id='G (DB.G)_BBBB'/>
            <expression op='AND'>
              <expression op='='><expression op='[ROLE]'/><expression op='[ROLE_G]'/></expression>
              <expression op='='><expression op='[WK]'/><expression op='[WK_G]'/></expression>
            </expression>
          </relationship>
          <relationship>
            <first-end-point object-id='F (DB.F)_AAAA'/>
            <second-end-point object-id='P (DB.P)_CCCC'/>
            <expression op='='><expression op='[ROLE]'/><expression op='[ROLE_P]'/></expression>
          </relationship>
        </relationships>
      </_.fcp.ObjectModelEncapsulateLegacy.true...object-graph>
    </datasource>
  </datasources>
</workbook>`;

describe('multi-source blend → one wide JOIN element', () => {
  const out: any = convertTableauToSigma(blendTwb, { connectionId: 'c1', database: 'DB', schema: 'SC', datasourceIndex: 0 });
  const sqlEls = (out.model?.pages?.[0]?.elements || []).filter((e: any) => e.source?.kind === 'sql');

  test('collapses to a SINGLE kind:sql element (no isolated islands, no relationships)', () => {
    assert.equal(sqlEls.length, 1, 'one merged element');
    assert.ok(!(sqlEls[0].relationships?.length), 'relationships folded into the JOIN');
  });

  test('statement is a WITH + LEFT JOIN over pre-aggregated secondaries', () => {
    const s = sqlEls[0].source.statement;
    assert.match(s, /WITH __f AS/, 'fact CTE');
    assert.equal((s.match(/LEFT JOIN/g) || []).length, 2, 'one LEFT JOIN per secondary');
    assert.match(s, /GROUP BY/, 'secondaries pre-aggregated to link grain');
  });

  test('additive measures use SUM, the product dimension uses MAX (m:m guard)', () => {
    const s = sqlEls[0].source.statement;
    assert.match(s, /SUM\("GOAL"\)/, 'weekly goal summed to (role,wk) grain');
    assert.match(s, /SUM\("PGOAL"\)/, 'product goal summed to (role) grain');
    assert.match(s, /MAX\("PRODUCT"\)/, 'product dimension (m:m) collapsed with MAX, not fanned out');
  });

  test('every emitted column resolves to a SELECT output alias (no error columns)', () => {
    const s = sqlEls[0].source.statement;
    const aliases = new Set([...s.matchAll(/AS "([^"]+)"/g)].map((m: any) => m[1]));
    for (const c of sqlEls[0].columns) {
      const m = c.formula.match(/^\[Custom SQL\/(.+)\]$/);
      assert.ok(m, `blend column uses [Custom SQL/...]: ${c.formula}`);
      assert.ok(aliases.has(m[1]), `column alias "${m[1]}" exists in the SELECT output`);
    }
  });

  test('fact columns are all present in the merged element', () => {
    const names = sqlEls[0].columns.map((c: any) => c.name);
    for (const f of ['ROLE', 'WK', 'SALES']) assert.ok(names.includes(f), `fact col ${f} present`);
    assert.ok(names.includes('GOAL') && names.includes('PGOAL'), 'secondary goal measures surfaced');
  });

  test('2-part FROM is qualified to 3-part (bug #2); 1-part is left alone', () => {
    const s = sqlEls[0].source.statement;
    // database override = 'DB'; the product-goals island used `from SC.PRODUCT_GOALS`.
    assert.match(s, /FROM\s+DB\.SC\.PRODUCT_GOALS/i, '2-part schema.table qualified to db.schema.table');
    assert.doesNotMatch(s, /FROM\s+DB\.FACT_TBL/i, '1-part bare table NOT spuriously qualified');
    assert.doesNotMatch(s, /DB\.DB\./i, 'already-qualified refs not double-qualified');
  });
});
