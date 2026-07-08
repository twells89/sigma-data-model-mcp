/**
 * Tableau LOD / Top-N / window helper SQL — mixed-case warehouse identifier
 * quoting (regression for the "unquoted snake_case vs mixed-case quoted column"
 * bug).
 *
 * Bug: for a plain physical `kind:'table'` fact, the LOD helper SELECT emitted
 * bare UPPER_SNAKE identifiers (`SFDC_OPPTY_ID`), but the real Snowflake column
 * is quoted mixed-case (`"SFDC Oppty ID"`). Snowflake folds the unquoted token
 * to upper and it fails to match. The fix recovers the true `remote-name` from
 * metadata-records and quotes it — but ONLY when the name isn't already a safe
 * bare uppercase identifier, so all-uppercase columns are unaffected.
 */

import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { convertTableauToSigma } from './tableau.js';

// Plain-table fact whose metadata-records carry mixed-case / spaced remote-names.
const mixedCaseTwb = `<?xml version='1.0' encoding='utf-8' ?>
<workbook source-build='2024.1' version='18.1'>
  <datasources>
    <datasource caption='Dormant Accounts' inline='true' name='federated.dormant' version='18.1'>
      <connection class='federated'>
        <named-connections>
          <named-connection caption='Snowflake' name='snowflake.0'>
            <connection class='snowflake' dbname='enterprise' schema='CONS_DATA_MART' server='x.snowflakecomputing.com' warehouse='WH'/>
          </named-connection>
        </named-connections>
        <relation connection='snowflake.0' name='SALES_FUNNEL_CURR' table='[enterprise].[CONS_DATA_MART].[SALES_FUNNEL_CURR]' type='table'>
          <columns>
            <column datatype='string' name='SFDC Oppty ID' ordinal='1' />
            <column datatype='string' name='Sales Region' ordinal='2' />
            <column datatype='string' name='STAGE_NAME' ordinal='3' />
            <column datatype='real'   name='Amount USD' ordinal='4' />
            <column datatype='real'   name='PROFIT' ordinal='5' />
          </columns>
        </relation>
        <metadata-records>
          <metadata-record class='column'><remote-name>SFDC Oppty ID</remote-name><local-name>[SFDC Oppty ID]</local-name><parent-name>[SALES_FUNNEL_CURR]</parent-name><local-type>string</local-type><aggregation>Count</aggregation><caption>SFDC Oppty ID</caption></metadata-record>
          <metadata-record class='column'><remote-name>Sales Region</remote-name><local-name>[Sales Region]</local-name><parent-name>[SALES_FUNNEL_CURR]</parent-name><local-type>string</local-type><aggregation>Count</aggregation><caption>Sales Region</caption></metadata-record>
          <metadata-record class='column'><remote-name>STAGE_NAME</remote-name><local-name>[STAGE_NAME]</local-name><parent-name>[SALES_FUNNEL_CURR]</parent-name><local-type>string</local-type><aggregation>Count</aggregation><caption>Stage Name</caption></metadata-record>
          <metadata-record class='measure'><remote-name>Amount USD</remote-name><local-name>[Amount USD]</local-name><parent-name>[SALES_FUNNEL_CURR]</parent-name><local-type>real</local-type><aggregation>Sum</aggregation><caption>Amount USD</caption></metadata-record>
          <metadata-record class='measure'><remote-name>PROFIT</remote-name><local-name>[PROFIT]</local-name><parent-name>[SALES_FUNNEL_CURR]</parent-name><local-type>real</local-type><aggregation>Sum</aggregation><caption>Profit</caption></metadata-record>
        </metadata-records>
      </connection>
      <column caption='Amount per Oppty' datatype='real' name='[Calculation_AmtPerOppty]' role='measure' type='quantitative'>
        <calculation class='tableau' formula='{INCLUDE [SFDC Oppty ID] : SUM([Amount USD])}' />
      </column>
      <column caption='Region Total' datatype='real' name='[Calculation_RegionTotal]' role='measure' type='quantitative'>
        <calculation class='tableau' formula='{FIXED [Sales Region] : SUM([Amount USD])}' />
      </column>
      <column caption='Region Profit Ratio' datatype='real' name='[Calculation_RegionProfitRatio]' role='measure' type='quantitative'>
        <calculation class='tableau' formula='{FIXED [Sales Region] : AVG([PROFIT]/[Amount USD])}' />
      </column>
    </datasource>
  </datasources>
  <worksheets>
    <worksheet name='Sheet 1'><table><view><datasources><datasource caption='Dormant Accounts' name='federated.dormant' /></datasources>
      <datasource-dependencies datasource='federated.dormant'>
        <column datatype='string' name='[Sales Region]' role='dimension' type='nominal' />
        <column caption='Amount per Oppty' datatype='real' name='[Calculation_AmtPerOppty]' role='measure' type='quantitative' />
      </datasource-dependencies></view>
      <rows>[federated.dormant].[Sales Region]</rows>
      <cols>[federated.dormant].[avg:Calculation_AmtPerOppty:qk]</cols>
    </table></worksheet>
    <worksheet name='Sheet 2'><table><view><datasources><datasource caption='Dormant Accounts' name='federated.dormant' /></datasources>
      <datasource-dependencies datasource='federated.dormant'>
        <column datatype='string' name='[STAGE_NAME]' role='dimension' type='nominal' />
        <column caption='Region Total' datatype='real' name='[Calculation_RegionTotal]' role='measure' type='quantitative' />
      </datasource-dependencies></view>
      <rows>[federated.dormant].[STAGE_NAME]</rows>
      <cols>[federated.dormant].[avg:Calculation_RegionTotal:qk]</cols>
    </table></worksheet>
  </worksheets>
</workbook>`;

function lodSqlStatements(twb: string): string[] {
  const out: any = convertTableauToSigma(twb, {
    connectionId: 'c1', database: 'enterprise', schema: 'CONS_DATA_MART', datasourceIndex: 0,
  });
  const els = (out.model?.pages || []).flatMap((p: any) => p.elements || []);
  return els.filter((e: any) => e.source?.kind === 'sql').map((e: any) => e.source.statement as string);
}

describe('LOD helper SQL: mixed-case warehouse identifiers are quoted', () => {
  const stmts = lodSqlStatements(mixedCaseTwb);

  test('at least one LOD helper element is emitted', () => {
    assert.ok(stmts.length >= 1, `expected LOD helper SQL, got ${stmts.length} sql elements`);
  });

  test('spaced/mixed-case columns are emitted with double quotes', () => {
    const all = stmts.join('\n');
    assert.match(all, /"SFDC Oppty ID"/, 'SFDC Oppty ID must be quoted');
    assert.match(all, /"Sales Region"/, 'Sales Region must be quoted');
    assert.match(all, /"Amount USD"/, 'Amount USD must be quoted');
  });

  test('no mixed-case column name leaks OUTSIDE double quotes', () => {
    for (const s of stmts) {
      // These names contain spaces, so they are ONLY valid quoted. After
      // removing every quoted span, none of them may remain (that would be a
      // bare, invalid reference — the original bug).
      const stripped = s.replace(/"[^"]*"/g, '""');
      for (const name of ['SFDC Oppty ID', 'Sales Region', 'Amount USD']) {
        assert.ok(!stripped.includes(name),
          `bare unquoted "${name}" leaked into: ${s}`);
      }
    }
  });

  test('mixed-case cols are never READ bare from the table (upper-snake ALIAS is fine)', () => {
    // The correct emission quotes the source column and aliases it to the bare
    // upper-snake name (`"Sales Region" AS SALES_REGION`). Dropping every
    // `AS <UPPER>` alias clause should leave NO bare mixed-case column being
    // read — any remainder is the pre-fix bug (a bare source read).
    for (const s of stmts) {
      const noAlias = s.replace(/\bAS\s+[A-Za-z_][A-Za-z0-9_]*/g, '');
      for (const bare of ['SFDC_OPPTY_ID', 'AMOUNT_USD', 'SALES_REGION']) {
        assert.ok(!new RegExp('\\b' + bare + '\\b').test(noAlias),
          `bare source read of ${bare} in: ${s}`);
      }
    }
  });

  test('genuinely-uppercase columns (PROFIT) stay bare — no needless quoting', () => {
    const all = stmts.join('\n');
    assert.match(all, /\bPROFIT\b/, 'PROFIT should appear');
    assert.doesNotMatch(all, /"PROFIT"/, 'PROFIT must NOT be quoted (already a safe bare identifier)');
  });
});
