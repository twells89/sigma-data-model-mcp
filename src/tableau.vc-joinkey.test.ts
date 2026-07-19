/**
 * Published / virtual-connection (VC) workbook join-key tests.
 *
 * A workbook backed by a Tableau published/virtual connection serializes its
 * federated join tree (<relation type='join'>) with join keys that are internal
 * Tableau field GUIDs — `[TABLE (SCHEMA.TABLE)].[<guid>]` — and carries NO
 * <metadata-records>. The only handle on the physical column is the
 * datasource-level `<column caption='…' name='[<guid>]'>` definition.
 *
 * Before the fix the converter emitted the join-key columns as garbled
 * `[TABLE/<Guid Title Cased>]` phantoms; the downstream phantom-column filter
 * (sigma-migration-skills mechanical-specs.rb) culled them and every VC
 * relationship went with them — `relationships: []` while metrics still
 * referenced joined-side fields, tripping the relationship-reachability guard
 * and forcing the slow manual dm-spec path on every VC migration.
 *
 * These tests guard:
 *  1. GUID join keys with captions → relationship KEPT, keys wired to columns
 *     named by their resolved captions (caption → upcase+underscore physical
 *     fold, mirroring sigma-migration-skills scripts/lib/join_plan.rb
 *     physical_name — do not diverge).
 *  2. No garbled GUID-derived column names/formulas are emitted at all.
 *  3. A join-key GUID with NO caption anywhere → the relationship is dropped
 *     LOUDLY (named in a warning), no phantom key column is invented, and
 *     metrics/columns referencing the unreachable joined side are culled so
 *     the emitted spec is never inconsistent.
 */

import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { convertTableauToSigma } from './tableau.js';

const G_KEY_FACT = 'a1b2c3d4-1111-3abc-9422-aa895d35a001';
const G_KEY_DIM  = 'b2c3d4e5-2222-3abc-9422-aa895d35a002';
const G_SALES    = 'c3d4e5f6-3333-3abc-9422-aa895d35a003';
const G_SCORE    = 'd4e5f6a7-4444-3abc-9422-aa895d35a004';

// keyCaptions: include the <column> defs that give the join-key GUIDs captions.
const vcTwb = (keyCaptions: boolean) => `<?xml version='1.0' encoding='utf-8'?>
<workbook>
  <datasources>
    <datasource caption='Orders VC' name='federated.vc1'>
      <connection class='federated'>
        <named-connections>
          <named-connection name='vc' caption='Orders VC'>
            <connection class='sqlproxy' dbname='0f3a5c7e-9b1d-4f2a-8c6e-123456789abc' server='tableau'/>
          </named-connection>
        </named-connections>
        <relation type='join' join='left'>
          <clause type='join'>
            <expression op='='>
              <expression op='[ORDER_FACT (CSA.ORDER_FACT)].[${G_KEY_FACT}]' />
              <expression op='[CUSTOMER_DIM (CSA.CUSTOMER_DIM)].[${G_KEY_DIM}]' />
            </expression>
          </clause>
          <relation connection='vc' name='ORDER_FACT (CSA.ORDER_FACT)' type='table' table='[d2633ac0-4c26-4cc1-b9e7-486b42493ea6].[ORDER_FACT (CSA.ORDER_FACT)]'>
            <columns>
              <column datatype='string' name='[${G_KEY_FACT}]' />
              <column datatype='real' name='[${G_SALES}]' />
            </columns>
          </relation>
          <relation connection='vc' name='CUSTOMER_DIM (CSA.CUSTOMER_DIM)' type='table' table='[c91c16a6-66d6-4232-b140-bdf71935f323].[CUSTOMER_DIM (CSA.CUSTOMER_DIM)]' />
        </relation>
        <cols>
          <map key='[${G_KEY_FACT}]' value='[ORDER_FACT (CSA.ORDER_FACT)].[${G_KEY_FACT}]' />
          <map key='[${G_KEY_DIM}]' value='[CUSTOMER_DIM (CSA.CUSTOMER_DIM)].[${G_KEY_DIM}]' />
          <map key='[${G_SALES}]' value='[ORDER_FACT (CSA.ORDER_FACT)].[${G_SALES}]' />
          <map key='[${G_SCORE}]' value='[CUSTOMER_DIM (CSA.CUSTOMER_DIM)].[${G_SCORE}]' />
        </cols>
      </connection>
      ${keyCaptions ? `
      <column caption='Customer Key' datatype='string' name='[${G_KEY_FACT}]' role='dimension' type='nominal' />
      <column caption='Customer Key (Customer Dim)' datatype='string' name='[${G_KEY_DIM}]' role='dimension' type='nominal' />` : ''}
      <column caption='Sales' datatype='real' name='[${G_SALES}]' role='measure' type='quantitative' />
      <column caption='Loyalty Score' datatype='real' name='[${G_SCORE}]' role='measure' type='quantitative' />
    </datasource>
  </datasources>
  <worksheets>
    <worksheet name='Sheet 1'>
      <table>
        <view>
          <datasource-dependencies datasource='federated.vc1'>
            <column caption='Sales' datatype='real' name='[${G_SALES}]' role='measure' type='quantitative' />
            <column caption='Loyalty Score' datatype='real' name='[${G_SCORE}]' role='measure' type='quantitative' />
          </datasource-dependencies>
        </view>
      </table>
    </worksheet>
  </worksheets>
</workbook>`;

const convert = (twb: string) => convertTableauToSigma(twb, {
  connectionId: 'c1', database: 'CSA', schema: 'TJ', datasourceIndex: 0,
}) as any;

const dataEls = (out: any) => (out.model?.pages?.[0]?.elements || []).filter(
  (e: any) => e.source?.kind === 'warehouse-table' || e.source?.kind === 'sql');

const GARBLED = /\/[0-9A-Fa-f] \d|\[\[|\/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-/; // "[T/A 1 B 2…]" / "[T/[guid]]" / bare-GUID name

describe('VC GUID join keys WITH captions (published/virtual-connection workbook)', () => {
  const out = convert(vcTwb(true));
  const els = dataEls(out);
  const fact = els.find((e: any) => (e.source.path || []).includes('ORDER_FACT'));
  const dim  = els.find((e: any) => (e.source.path || []).includes('CUSTOMER_DIM'));

  test('relationship is PRESERVED (was dropped to [] pre-fix)', () => {
    assert.ok(fact, 'fact element exists');
    assert.ok(dim, 'dim element exists');
    assert.equal((fact.relationships || []).length, 1, 'fact carries the fact→dim relationship');
    assert.equal(fact.relationships[0].targetElementId, dim.id);
    assert.equal(fact.relationships[0].keys.length, 1);
  });

  test('join-key columns carry caption-resolved names, not GUIDs', () => {
    const key = fact.relationships[0].keys[0];
    const srcCol = fact.columns.find((c: any) => c.id === key.sourceColumnId);
    const tgtCol = dim.columns.find((c: any) => c.id === key.targetColumnId);
    assert.ok(srcCol, 'source key column exists on the fact element');
    assert.ok(tgtCol, 'target key column exists on the dim element');
    assert.equal(srcCol.formula, '[ORDER_FACT/Customer Key]');
    // caption 'Customer Key (Customer Dim)' — single-paren disambiguation kept
    // by guidCaption; only the double-paren VC flatten suffix is stripped.
    assert.match(tgtCol.formula, /^\[CUSTOMER_DIM\/Customer Key/);
  });

  test('no garbled GUID-derived column formulas anywhere', () => {
    for (const e of els) {
      for (const c of (e.columns || [])) {
        assert.doesNotMatch(String(c.formula), GARBLED,
          `garbled GUID column emitted: ${c.formula}`);
      }
    }
  });

  test('fact-side measure still gets its auto Sum() metric', () => {
    const mets = (els.flatMap((e: any) => e.metrics || []));
    assert.ok(mets.some((m: any) => m.name === 'Sales' && /Sum\(\[Sales\]\)/.test(m.formula)),
      'Sum([Sales]) metric emitted on the fact');
  });
});

describe('VC GUID join keys WITHOUT captions (unresolvable)', () => {
  const out = convert(vcTwb(false));
  const els = dataEls(out);
  const fact = els.find((e: any) => (e.source.path || []).includes('ORDER_FACT'));
  const warnings: string[] = out.warnings || [];

  test('relationship is dropped LOUDLY — named in a warning, never silently', () => {
    const totalRels = els.reduce((s: number, e: any) => s + ((e.relationships || []).length), 0);
    assert.equal(totalRels, 0, 'unresolvable GUID join key → relationship not wired');
    assert.ok(warnings.some(w => /DROPPED relationship ORDER_FACT → CUSTOMER_DIM/.test(w)),
      `loud warning names the dropped relationship; got: ${warnings.join(' | ')}`);
  });

  test('no phantom GUID key column is invented', () => {
    for (const e of els) {
      for (const c of (e.columns || [])) {
        assert.doesNotMatch(String(c.formula), GARBLED,
          `phantom GUID key column emitted: ${c.formula}`);
      }
    }
  });

  test('spec stays consistent: no metric references the unreachable joined side', () => {
    const mets = els.flatMap((e: any) => e.metrics || []);
    for (const m of mets) {
      assert.doesNotMatch(String(m.formula), /Loyalty Score/,
        `metric "${m.name}" references the dropped CUSTOMER_DIM side: ${m.formula}`);
    }
    // The fact's own measure is unaffected.
    assert.ok(mets.some((m: any) => m.name === 'Sales'), 'fact-side Sum([Sales]) metric kept');
    // And the loss is surfaced, not silent.
    assert.ok(warnings.some(w => /Loyalty Score/.test(w)),
      'joined-side measure loss is surfaced in a warning');
  });

  test('fact element itself remains intact', () => {
    assert.ok(fact, 'fact element exists');
    assert.ok(fact.columns.some((c: any) => c.formula === '[ORDER_FACT/Sales]'),
      'fact keeps its own physical columns');
  });
});
