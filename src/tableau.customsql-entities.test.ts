/**
 * Custom-SQL relation text is stored XML-escaped in the .twb. fast-xml-parser
 * decodes exactly ONE level of named entities and leaves numeric refs untouched,
 * so comparison operators arrived broken in two real field cases:
 *   • double-escaped text (&amp;lt;=) → a residual &lt;= survived into the emitted
 *     statement (invalid SQL; a human reads it as "<<" / ">>=").
 *   • numeric line-break refs (&#13;&#10;) → landed literally in the statement.
 * unescapeCustomSqlEntities now decodes both to a fixed point. These assert the
 * emitted SQL carries real operators, not entity artifacts.
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { convertTableauToSigma } from './tableau.js';

const twb = (whereSql: string) => `<?xml version='1.0'?><workbook><datasources>
 <datasource caption='CS' name='federated.cs'>
  <connection class='snowflake' dbname='CSA' schema='TJ'>
    <relation name='Custom SQL Query' type='text'>SELECT ORDER_ID, PLACEDDATETIME FROM CSA.TJ.BETS WHERE ${whereSql}</relation>
    <metadata-records>
      <metadata-record class='column'><remote-name>ORDER_ID</remote-name><local-type>integer</local-type></metadata-record>
      <metadata-record class='column'><remote-name>PLACEDDATETIME</remote-name><local-type>datetime</local-type></metadata-record>
    </metadata-records>
  </connection>
 </datasource></datasources>
 <worksheet name='W'><table><view><datasources><datasource name='federated.cs'/></datasources></view></table></worksheet>
</workbook>`;

const sqlOf = (o: any) => o.model.pages[0].elements
  .filter((e: any) => e.source?.kind === 'sql').map((e: any) => e.source.statement).join('\n');

describe('Custom SQL XML-entity decoding into source.statement', () => {
  test('single-escaped comparison operators decode to < <= > >=', () => {
    const s = sqlOf(convertTableauToSigma(
      twb('dateadd(hour,-4,PLACEDDATETIME) &lt;= TO_DATE(GETDATE()) and ORDER_ID &gt;= 100'), { connectionId: 'c1' }));
    assert.match(s, /PLACEDDATETIME\) <= TO_DATE/i);
    assert.match(s, /ORDER_ID >= 100/i);
    assert.doesNotMatch(s, /&lt;|&gt;/, 'no residual named entities');
  });

  test('DOUBLE-escaped operators (&amp;lt;=) collapse to <= / >= (the field bug)', () => {
    const s = sqlOf(convertTableauToSigma(
      twb('dateadd(hour,-4,PLACEDDATETIME) &amp;lt;= TO_DATE(GETDATE()) and ORDER_ID &amp;gt;= 100'), { connectionId: 'c1' }));
    assert.match(s, /PLACEDDATETIME\) <= TO_DATE/i);
    assert.match(s, /ORDER_ID >= 100/i);
    assert.doesNotMatch(s, /&lt;|&gt;|&amp;/, 'no residual single- or double-escaped entities');
  });

  test('numeric line-break refs (&#13;&#10;) decode, not land literally', () => {
    const s = sqlOf(convertTableauToSigma(
      twb('ORDER_ID &gt; 0&#13;&#10;  AND ORDER_ID &lt; 999'), { connectionId: 'c1' }));
    assert.doesNotMatch(s, /&#13;|&#10;/, 'numeric refs decoded');
    assert.match(s, /ORDER_ID > 0/i);
    assert.match(s, /ORDER_ID < 999/i);
  });

  test('a legitimate ampersand (A &amp; B, single-escaped) is preserved, not over-decoded', () => {
    const s = sqlOf(convertTableauToSigma(
      twb("CONCAT(SEGMENT, ' &amp; ', REGION) = 'East &amp; West'"), { connectionId: 'c1' }));
    assert.match(s, /' & '/, 'single-escaped & stays a literal ampersand');
    assert.match(s, /'East & West'/);
  });
});
