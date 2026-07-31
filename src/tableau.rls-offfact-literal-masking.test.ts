/**
 * Bug: the post-conversion bracket-reference scans tableau.ts runs to detect
 * (a) row-level-security calcs that reach a related-table (off-fact) column,
 * and (b) aggregate metrics that reach a related-table (off-fact) column,
 * both do `sigmaFormula.match(/\[([^\]\/]+)\]/g)` with no idea that string
 * literals exist. A bracketed phantom "[ref]" living INSIDE a quoted string
 * literal — a help/label string like `"See [Approved Region] for details"` —
 * whose text happens to normalize to the display name of a REAL off-fact
 * (dimension-table) column, is indistinguishable from a genuine reference.
 *
 * Direction/severity: the phantom ref can only ever cause a FALSE POSITIVE
 * "this is cross-element" verdict (it never invents a false NEGATIVE — the
 * regex only ADDS candidate refs, never removes real ones). A false positive
 * here means:
 *   - RLS: a legitimate same-fact RLS rule is DROPPED from `result.security`
 *     (security-relevant false negative on RLS ENFORCEMENT — the migration
 *     skill provisions RLS from `result.security`, so a rule that never lands
 *     there never gets applied at all) and the emitted warning misdirects the
 *     user to "re-apply this rule on the element that owns" a column the
 *     formula never actually referenced.
 *   - Metric: a legitimate same-fact metric is skipped (lost coverage, but
 *     with an accurate-looking-but-wrong warning).
 *
 * The fix (`maskFormulaStringLiterals`) masks `"..."` string-literal spans
 * with same-length blanks before the `[ref]` scan runs, treating a
 * `[bracketed identifier]` as atomic first (so an embedded `"` inside a
 * caption, or an apostrophe inside `[Manager's Approval]`, can't confuse the
 * quote-scan), and never swallows the rest of the string past an
 * unterminated `"` or unterminated `[`.
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { convertTableauToSigma, maskFormulaStringLiterals } from './tableau.js';

// ── Direct unit tests of the masking primitive ──────────────────────────────

describe('maskFormulaStringLiterals: unit', () => {
  test('a bracket-looking ref inside a string literal is masked out', () => {
    const masked = maskFormulaStringLiterals('[Region] = "See [Approved Region] for details"');
    assert.doesNotMatch(masked, /\[Approved Region\]/);
    assert.match(masked, /\[Region\]/, 'the real ref outside the literal survives');
    assert.equal(masked.length, '[Region] = "See [Approved Region] for details"'.length, 'length preserved');
  });

  test('a real ref outside any literal is preserved verbatim', () => {
    assert.match(maskFormulaStringLiterals('[Region] = "West"'), /\[Region\]/);
  });

  test('bracket atomicity: a caption containing an embedded quote is copied through, not misread as opening a literal', () => {
    // [12" Pipe] must not be read as: literal opens at the embedded '"',
    // consuming real text after it looking for a close quote.
    const f = '[12" Pipe] = [Diameter] and [Notes] = "ok"';
    const masked = maskFormulaStringLiterals(f);
    assert.match(masked, /\[12" Pipe\]/, 'bracket with embedded quote is untouched');
    assert.match(masked, /\[Diameter\]/, 'ref after the tricky bracket is still visible');
    assert.doesNotMatch(masked, /ok/, 'the real trailing literal is still masked');
  });

  test("apostrophe inside a bracketed identifier is not a quote opener (moot via atomic bracket-skip, but must not regress)", () => {
    const f = `[Manager's Approval] = "Yes"`;
    const masked = maskFormulaStringLiterals(f);
    assert.match(masked, /\[Manager's Approval\]/);
    assert.doesNotMatch(masked, /Yes/);
  });

  test('unterminated quote does not swallow the rest of the string — a real ref after it is still found', () => {
    // No second '"' anywhere — genuinely unterminated (not the documented,
    // accepted "two independent stray quotes coincidentally pair up" case).
    const f = "[Note] = \"truly unterminated and [Region] is here";
    const masked = maskFormulaStringLiterals(f);
    assert.match(masked, /\[Region\]/, 'ref after an unterminated quote is not swallowed');
  });

  test('unterminated bracket does not swallow the rest of the string — a real literal after it is still masked', () => {
    const f = '[Note = "West" and [Region] = "East"';
    const masked = maskFormulaStringLiterals(f);
    assert.doesNotMatch(masked, /West/, 'literal reachable after the stray "[" is still masked');
    assert.doesNotMatch(masked, /East/, 'literal reachable after the stray "[" is still masked');
  });
});

// ── Integration: RLS / off-fact detection through the real pipeline ────────

const XML_HEADER = `<?xml version='1.0' encoding='utf-8' ?>
<workbook source-build='2024.1' version='18.1'>
  <datasources>
    <datasource caption='D' inline='true' name='federated.d' version='18.1'>
      <connection class='federated'>
        <named-connections>
          <named-connection caption='Snowflake' name='snowflake.0'>
            <connection class='snowflake' dbname='enterprise' schema='SALES' server='x.snowflakecomputing.com' warehouse='WH'/>
          </named-connection>
        </named-connections>
        <relation type='join' join='left'>
          <clause type='join'>
            <expression op='='>
              <expression op='[ORDER_FACT].[CUSTOMER_ID]' />
              <expression op='[CUSTOMER_DIM].[CUSTOMER_ID]' />
            </expression>
          </clause>
          <relation connection='snowflake.0' name='ORDER_FACT' table='[enterprise].[SALES].[ORDER_FACT]' type='table'>
            <columns>
              <column datatype='string' name='CUSTOMER_ID' ordinal='1' />
              <column datatype='string' name='Region' ordinal='2' />
              <column datatype='real' name='Amount' ordinal='3' />
            </columns>
          </relation>
          <relation connection='snowflake.0' name='CUSTOMER_DIM' table='[enterprise].[SALES].[CUSTOMER_DIM]' type='table'>
            <columns>
              <column datatype='string' name='CUSTOMER_ID' ordinal='1' />
              <column datatype='string' name='Approved Region' ordinal='2' />
            </columns>
          </relation>
        </relation>
        <metadata-records>
          <metadata-record class='column'><remote-name>CUSTOMER_ID</remote-name><local-name>[CUSTOMER_ID]</local-name><parent-name>[ORDER_FACT]</parent-name><local-type>string</local-type><aggregation>Count</aggregation><caption>Customer Id</caption></metadata-record>
          <metadata-record class='column'><remote-name>Region</remote-name><local-name>[Region]</local-name><parent-name>[ORDER_FACT]</parent-name><local-type>string</local-type><aggregation>Count</aggregation><caption>Region</caption></metadata-record>
          <metadata-record class='measure'><remote-name>Amount</remote-name><local-name>[Amount]</local-name><parent-name>[ORDER_FACT]</parent-name><local-type>real</local-type><aggregation>Sum</aggregation><caption>Amount</caption></metadata-record>
          <metadata-record class='column'><remote-name>Approved Region</remote-name><local-name>[Approved Region]</local-name><parent-name>[CUSTOMER_DIM]</parent-name><local-type>string</local-type><aggregation>Count</aggregation><caption>Approved Region</caption></metadata-record>
        </metadata-records>
      </connection>`;

function rlsTwb(formula: string): string {
  return `${XML_HEADER}
      <column caption='RLS Region Guard' datatype='boolean' name='[Calculation_Rls1]' role='dimension' type='nominal'>
        <calculation class='tableau' formula='${formula}' />
      </column>
    </datasource>
  </datasources>
  <worksheets>
    <worksheet name='Sheet 1'><table><view><datasources><datasource caption='D' name='federated.d' /></datasources>
      <datasource-dependencies datasource='federated.d'>
        <column caption='RLS Region Guard' datatype='boolean' name='[Calculation_Rls1]' role='dimension' type='nominal' />
      </datasource-dependencies></view>
      <rows></rows>
      <cols>[federated.d].[Calculation_Rls1]</cols>
    </table></worksheet>
  </worksheets>
</workbook>`;
}

function metricTwb(formula: string): string {
  return `${XML_HEADER}
      <column caption='Flagged Amount' datatype='real' name='[Calculation_FlaggedAmt]' role='measure' type='quantitative'>
        <calculation class='tableau' formula='${formula}' />
      </column>
    </datasource>
  </datasources>
  <worksheets>
    <worksheet name='Sheet 1'><table><view><datasources><datasource caption='D' name='federated.d' /></datasources>
      <datasource-dependencies datasource='federated.d'>
        <column caption='Flagged Amount' datatype='real' name='[Calculation_FlaggedAmt]' role='measure' type='quantitative' />
      </datasource-dependencies></view>
      <rows></rows>
      <cols>[federated.d].[avg:Calculation_FlaggedAmt:qk]</cols>
    </table></worksheet>
  </worksheets>
</workbook>`;
}

const convert = (twb: string) => convertTableauToSigma(twb, {
  connectionId: 'c1', database: 'enterprise', schema: 'SALES', datasourceIndex: 0,
}) as any;

describe('RLS off-fact scan is literal-aware (bead lit-wt-pbitab.1)', () => {
  test('adversarial: literal text matching an off-fact display name does NOT suppress a legitimate same-fact RLS rule', () => {
    const formula = 'IF USERNAME() = &quot;admin@co.com&quot; THEN TRUE ELSE [Region] = &quot;See [Approved Region] for details&quot; END';
    const out = convert(rlsTwb(formula));
    assert.ok((out.security || []).some((s: any) => /RLS Region Guard/i.test(s.rls?.name || '')),
      `expected RLS entry in result.security; warnings: ${JSON.stringify(out.warnings)}`);
    assert.ok(!(out.warnings || []).some((w: string) => /cross-element/.test(w)),
      'must not emit a misleading cross-element warning for a same-fact rule');
  });

  test('control: a REAL off-fact reference (not in a literal) is still correctly flagged and NOT auto-emitted', () => {
    const formula = 'IF USERNAME() = &quot;admin@co.com&quot; THEN TRUE ELSE [Approved Region] = &quot;West&quot; END';
    const out = convert(rlsTwb(formula));
    assert.ok(!(out.security || []).some((s: any) => /RLS Region Guard/i.test(s.rls?.name || '')),
      'a genuinely cross-element RLS rule must still be withheld from result.security');
    assert.ok((out.warnings || []).some((w: string) => /cross-element/.test(w) && /Approved Region/.test(w)),
      'must still warn about the real cross-element reference');
  });

  test('control: a plain on-fact RLS rule with no literal trickery is still detected', () => {
    const formula = 'IF USERNAME() = &quot;admin@co.com&quot; THEN TRUE ELSE [Region] = &quot;West&quot; END';
    const out = convert(rlsTwb(formula));
    assert.ok((out.security || []).some((s: any) => /RLS Region Guard/i.test(s.rls?.name || '')));
  });
});

describe('Off-fact metric scan is literal-aware (bead lit-wt-pbitab.1)', () => {
  test('adversarial: literal text matching an off-fact display name does NOT suppress a legitimate same-fact metric', () => {
    const formula = 'SUM(IIF([Region] = &quot;See [Approved Region] for details&quot;, [Amount], 0))';
    const out = convert(metricTwb(formula));
    const mets = (out.model?.pages || []).flatMap((p: any) => p.elements || []).flatMap((e: any) => e.metrics || []);
    assert.ok(mets.some((m: any) => m.name === 'Flagged Amount'),
      `expected "Flagged Amount" metric to be emitted; warnings: ${JSON.stringify(out.warnings)}`);
  });

  test('control: a REAL off-fact reference (not in a literal) is still correctly skipped', () => {
    const formula = 'SUM(IIF([Approved Region] = &quot;West&quot;, [Amount], 0))';
    const out = convert(metricTwb(formula));
    const mets = (out.model?.pages || []).flatMap((p: any) => p.elements || []).flatMap((e: any) => e.metrics || []);
    assert.ok(!mets.some((m: any) => m.name === 'Flagged Amount'),
      'a genuinely cross-element metric must still be skipped');
    assert.ok((out.warnings || []).some((w: string) => /cross-element/.test(w) && /Approved Region/.test(w)));
  });
});
