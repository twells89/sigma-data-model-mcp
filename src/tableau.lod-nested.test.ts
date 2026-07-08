/**
 * Nested LOD → SQL helper.
 *
 * Tableau lets an LOD be wrapped by a viz-level aggregate — `MAX({ FIXED a, b :
 * COUNT(IF cond THEN 1 END) })`. tableauParseLOD only matched a STANDALONE LOD,
 * so a nested LOD fell to the untranslatable path: dropped (single-table) or —
 * on the cross-table Custom SQL path — the raw `{ FIXED }` leaked into the
 * emitted SQL and Sigma rejected the spec (field report 2026-07-08). The
 * converter now strips the outer aggregate and translates the inner LOD through
 * the same GROUP-BY helper path. These guard that.
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { convertTableauToSigma } from './tableau.js';

const mr = (rn: string, cap: string, lt: string) =>
  `<metadata-record class='column'><remote-name>${rn}</remote-name><caption>${cap}</caption><object-id>[SALES]</object-id><local-type>${lt}</local-type></metadata-record>`;

const twb = (formula: string) => `<?xml version='1.0'?><workbook><datasources>
 <datasource caption='Sales' name='federated.s'>
  <connection class='federated'><named-connections><named-connection name='c'><connection class='snowflake' dbname='WH' schema='PUBLIC'/></named-connection></named-connections>
   <_.fcp.ObjectModelEncapsulateLegacy.true...relation type='collection'><relation connection='c' name='SALES' table='[SALES]' type='table'/></_.fcp.ObjectModelEncapsulateLegacy.true...relation>
   <metadata-records>${mr('CUST_ID', 'Cust Id', 'string')}${mr('SEGMENT', 'Segment', 'string')}${mr('AMT', 'Amt', 'real')}</metadata-records>
  </connection>
  <column caption='Keep Row' name='[Calculation_1]' datatype='integer' role='measure' type='quantitative'><calculation class='tableau' formula='${formula}'/></column>
 </datasource></datasources>
 <worksheet name='W'><table><view><datasources><datasource name='federated.s'><column-instance column='[Calculation_1]' derivation='None' name='[sum:Calculation_1:qk]' pivot='key' type='quantitative'/></datasources></view></table></worksheet>
</workbook>`;

function convert(formula: string) {
  return convertTableauToSigma(twb(formula), { connectionId: 'c1', database: 'WH', schema: 'PUBLIC' }) as any;
}

describe('nested LOD inside an outer aggregate', () => {
  const out = convert(`MAX({ FIXED [Cust Id], [Segment] : COUNT(IF [Segment]="VIP" AND [Amt]&lt;&gt;0 THEN 1 END) })`);
  const els = out.model.pages[0].elements;
  const sqlEl = els.find((e: any) => e.source?.kind === 'sql');

  test('builds an LOD helper (kind:sql) instead of dropping/leaking', () => {
    assert.ok(sqlEl, 'a kind:sql LOD helper element was created');
    assert.ok(els.length >= 2, `expected >=2 elements, got ${els.length}`);
  });

  test('the inner IF-condition COUNT is translated to SQL CASE (no raw Tableau IF)', () => {
    assert.match(sqlEl.source.statement, /COUNT\(CASE WHEN .*THEN 1 END\)/i);
    assert.match(sqlEl.source.statement, /GROUP BY/i);
  });

  test('NO raw {FIXED} leaks anywhere in the emitted spec', () => {
    assert.doesNotMatch(JSON.stringify(out.model), /\{\s*FIXED/i, 'no raw LOD syntax in the spec');
  });

  test('reports the outer aggregate so the workbook applies it', () => {
    assert.ok((out.warnings || []).some((w: string) => /wrapped by MAX/i.test(w)),
      'a warning names the stripped outer aggregate');
  });

  test('a plain non-LOD calc is unaffected (no false trigger)', () => {
    const o2 = convert('[Amt] * 2');
    assert.ok(!(o2.warnings || []).some((w: string) => /wrapped by/i.test(w)));
  });

  test('a standalone LOD still works (no regression)', () => {
    const o3 = convert('{ FIXED [Cust Id] : SUM([Amt]) }');
    assert.ok(o3.model.pages[0].elements.some((e: any) => e.source?.kind === 'sql'),
      'standalone LOD still builds a helper');
    assert.ok(!(o3.warnings || []).some((w: string) => /wrapped by/i.test(w)));
  });
});
