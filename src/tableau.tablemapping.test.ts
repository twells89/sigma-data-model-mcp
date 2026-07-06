/**
 * Extract-backed (.twbx) datasource tests: table-name mapping, extract-suffix
 * stripping, mixed-case/spaced schema, and slash-in-column-name handling.
 *
 * Packaged-extract workbooks name their logical table after the extract sheet
 * ("Orders$") — which almost never matches the physical warehouse table
 * ("ORDERS"). Historically the converter (a) force-uppercased the schema override
 * ("Tableau Test" → "TABLEAU TEST", which the warehouse lookup can't resolve),
 * (b) kept the "$" extract suffix in source.path AND every base-column formula
 * prefix, so the DM POST failed with "Source not found", and (c) emitted a raw
 * "/" from a field named "Country/Region" ([ORDERS/Country/region]) which Sigma
 * parses as a nested ref path → column type "error".
 *
 * These guard the fixes: $-strip by default, explicit tableMapping override,
 * case-preserving schema, and slash-folding in display names.
 */

import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { convertTableauToSigma } from './tableau.js';
import { sigmaDisplayName } from './sigma-ids.js';

// Minimal single-table extract-backed .twb: relation named "Orders" with
// table='[Orders$]' (the extract-sheet suffix), one field with a slash in its
// name ("Country/Region").
const extractTwb = `<?xml version='1.0' encoding='utf-8'?>
<workbook>
  <datasources>
    <datasource caption='Superstore' name='federated.orders'>
      <connection class='federated'>
        <named-connections>
          <named-connection name='hyper' caption='hyper'>
            <connection class='hyper' dbname='x.hyper' schema='Extract'/>
          </named-connection>
        </named-connections>
        <relation connection='hyper' name='Orders' table='[Orders$]' type='table'>
          <columns>
            <column name='Row Id' datatype='integer'/>
            <column name='Region' datatype='string'/>
            <column name='Country/Region' datatype='string'/>
            <column name='Sub-Category' datatype='string'/>
            <column name='Sales' datatype='real'/>
          </columns>
        </relation>
      </connection>
    </datasource>
  </datasources>
</workbook>`;

const firstEl = (out: any) =>
  (out.model?.pages || []).flatMap((p: any) => p.elements || [])
    .find((e: any) => e.source?.kind === 'warehouse-table');

describe('extract-backed (.twbx) table mapping + schema case', () => {
  test('default: strips the "$" extract suffix and preserves spaced schema case', () => {
    const out: any = convertTableauToSigma(extractTwb, {
      connectionId: 'c1', database: 'CSA', schema: 'Tableau Test',
    });
    const el = firstEl(out);
    assert.deepEqual(el.source.path, ['CSA', 'Tableau Test', 'ORDERS'],
      'path resolves to CSA."Tableau Test".ORDERS (suffix stripped, schema case kept)');
    // Base-column formula prefix must match the resolved table name (a path-only
    // rewrite leaves [ORDERS$/...] which Sigma cannot resolve).
    for (const c of el.columns) {
      assert.match(c.formula, /^\[ORDERS\//, `formula prefix follows the resolved table: ${c.formula}`);
    }
  });

  test('explicit tableMapping redirects a mismatched logical name', () => {
    const out: any = convertTableauToSigma(extractTwb, {
      connectionId: 'c1', database: 'CSA', schema: 'Tableau Test',
      tableMapping: { 'Orders$': 'SALES_ORDERS' },
    });
    const el = firstEl(out);
    assert.equal(el.source.path[2], 'SALES_ORDERS', 'mapping overrides the table segment');
    for (const c of el.columns) {
      assert.match(c.formula, /^\[SALES_ORDERS\//, `formula prefix follows the mapping: ${c.formula}`);
    }
  });

  test('tableMapping matches case-insensitively and with/without the "$" suffix', () => {
    for (const key of ['orders', 'Orders', 'ORDERS$', 'orders$']) {
      const out: any = convertTableauToSigma(extractTwb, {
        connectionId: 'c1', database: 'CSA', schema: 'SC',
        tableMapping: { [key]: 'MAPPED_TBL' },
      });
      assert.equal(firstEl(out).source.path[2], 'MAPPED_TBL', `mapping key '${key}' matched`);
    }
  });

  test('schema override is NOT force-uppercased (was the "TABLEAU TEST" bug)', () => {
    const out: any = convertTableauToSigma(extractTwb, {
      connectionId: 'c1', database: 'CSA', schema: 'Tableau Test',
    });
    assert.equal(firstEl(out).source.path[1], 'Tableau Test');
  });

  test('a slash in a column name folds to a space (no broken nested-ref formula)', () => {
    const out: any = convertTableauToSigma(extractTwb, {
      connectionId: 'c1', database: 'CSA', schema: 'SC',
    });
    const slashCol = firstEl(out).columns.find((c: any) => /country/i.test(c.formula));
    assert.ok(slashCol, 'Country/Region column present');
    assert.equal(slashCol.formula, '[ORDERS/Country Region]',
      'the "/" in "Country/Region" is folded so the ref resolves to COUNTRY_REGION');
    // sigmaDisplayName stays idempotent after the slash fold.
    assert.equal(sigmaDisplayName('Country/Region'), 'Country Region');
    assert.equal(sigmaDisplayName('Country Region'), 'Country Region');
  });

  test('a dash in a column name folds to a space (resolves against underscore warehouse col)', () => {
    const out: any = convertTableauToSigma(extractTwb, {
      connectionId: 'c1', database: 'CSA', schema: 'SC',
    });
    const subCat = firstEl(out).columns.find((c: any) => /sub/i.test(c.formula));
    assert.equal(subCat.formula, '[ORDERS/Sub Category]',
      'the "-" in "Sub-Category" is folded so the ref resolves to SUB_CATEGORY');
    assert.equal(sigmaDisplayName('Sub-Category'), 'Sub Category');
    assert.equal(sigmaDisplayName('Sub Category'), 'Sub Category'); // idempotent
  });
});
