/**
 * Bug: several DAX-handling sites in powerbi.ts (OUTSIDE
 * powerbi-crosstable-triage.ts, which already masks literals for its own
 * classification) scan or rewrite DAX / DAX-derived Sigma-formula text with a
 * regex, a manual paren-depth walk, or `.replace()` — with no idea that
 * string literals exist. A DAX function name, comma, paren, or
 * `[Bracket]`-shaped reference living INSIDE a quoted string literal gets
 * treated as live syntax:
 *
 *  1. `DIVIDE(a, b, alt)`'s nested-paren-aware arg parser (Tier-1 direct
 *     mapping) walks raw `f` counting `(`/`)`/`,` — a comma or unbalanced
 *     paren inside the fallback-text literal corrupts the arg split and/or
 *     truncates the replaced span onto the wrong text. DANGEROUS: wrong value.
 *  2. The `'table'[col]` / `Table[col]` qualifier-stripping regexes both
 *     DETECT (multi-table warning) and REWRITE `f` — a label string like
 *     `"Store[Count]: high"` has its CONTENT rewritten in place.
 *     DANGEROUS: wrong value (literal corruption).
 *  3. Three post-conversion `\[([^\]\/]+)\]` scans over the translated Sigma
 *     formula — the calc-column/measure canonical-rename rewrite, the
 *     cross-table-guard's rename+detect pass (which decides whether to route
 *     a metric through triageCrossTable's dangling/cross-table drop), and
 *     `pruneDanglingMetrics`'s dangling-measure-reference scan — all treat a
 *     bracket-shaped NAME inside a string literal as a real reference. A
 *     label/help string mentioning another column or measure BY NAME both (a)
 *     gets rewritten in place (corrupting the literal) and (b) can supply a
 *     phantom "bad" ref that gets a fully independent, valid metric WRONGLY
 *     dropped as "cross-table"/"dangling". DANGEROUS: looks like a considered
 *     migration-quality warning, but the named blocker was never referenced.
 *  4. The cross-element calc-column PULL (`hasCross`) and its
 *     related-column-rename rewrite share the identical unmasked scan — a
 *     fully local calc column gets needlessly pulled off its source element
 *     (misplaced, or dropped outright if no derived view covers it).
 *
 * The fix: `maskDaxStringLiterals` masks `"..."` DAX string-literal spans
 * (doubled `""` escape, matching `stripDaxComments`'s convention) with
 * same-length blanks before any of the above scans run; `.replace()` sites
 * use `replaceOutsideDaxLiterals`, which finds matches on the MASKED text (so
 * a literal's content can never match) and applies the same rewrite to the
 * ORIGINAL text at the identical (length-preserved) offsets.
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { pbiDaxToSigma, convertPowerBIToSigma, maskDaxStringLiterals } from './powerbi.js';

const OPTS = { connectionId: '11111111-2222-3333-4444-555555555555', database: 'DB', schema: 'SCH' };
const model = (tables: any[], relationships: any[] = []) => ({
  name: 'M', compatibilityLevel: 1600,
  model: { culture: 'en-US', tables, relationships },
});
const tbl = (name: string, cols: string[], measures: any[] = [], calcCols: any[] = []) => ({
  name,
  columns: [
    ...cols.map((c) => ({ name: c, dataType: 'string', sourceColumn: c, summarizeBy: 'none' })),
    ...calcCols,
  ],
  measures,
  partitions: [{ name, mode: 'import', source: { type: 'm',
    expression: `let S = Sql.Database("h","DB"), N = S{[Name="${name}",Kind="Table"]}[Data] in N` } }],
});
const metricsOf = (out: any) =>
  (out.model.pages || []).flatMap((p: any) => p.elements || []).flatMap((e: any) => e.metrics || []);
const elementsOf = (out: any) => (out.model.pages || []).flatMap((p: any) => p.elements || []);

// ── Direct unit tests of the masking primitive ──────────────────────────────

describe('maskDaxStringLiterals: unit', () => {
  test('a bracket-looking ref inside a string literal is masked out', () => {
    const masked = maskDaxStringLiterals('[Amount] = "See [Approved Region] for details"');
    assert.doesNotMatch(masked, /\[Approved Region\]/);
    assert.match(masked, /\[Amount\]/, 'real ref outside the literal survives');
    assert.equal(masked.length, '[Amount] = "See [Approved Region] for details"'.length);
  });

  test('DAX doubled-quote escape ("") is honored, matching stripDaxComments', () => {
    const masked = maskDaxStringLiterals('"She said ""hello [X]"" to me" & [Y]');
    assert.doesNotMatch(masked, /\[X\]/, 'bracket text inside the escaped-quote literal is masked');
    assert.match(masked, /\[Y\]/, 'real ref after the literal survives');
  });

  test('bracket atomicity: embedded quote in a bracket is not misread as opening a literal', () => {
    const f = '[12" Pipe] = [Diameter] and [Notes] = "ok"';
    const masked = maskDaxStringLiterals(f);
    assert.match(masked, /\[12" Pipe\]/);
    assert.match(masked, /\[Diameter\]/);
    assert.doesNotMatch(masked, /ok/);
  });

  test('unterminated quote does not swallow the rest of the string', () => {
    const f = '[Note] = "truly unterminated and [Region] is here';
    assert.match(maskDaxStringLiterals(f), /\[Region\]/);
  });

  test('unterminated bracket does not reach past an unrelated later bracket', () => {
    const f = '[Note = "West" and [Region] = "East"';
    const masked = maskDaxStringLiterals(f);
    assert.doesNotMatch(masked, /West/);
    assert.doesNotMatch(masked, /East/);
  });
});

// ── 1. DIVIDE nested-paren-aware parser ─────────────────────────────────────

describe('DIVIDE(a, b, alt): literal-aware arg/paren walk', () => {
  test('a comma inside the fallback-text literal does not corrupt the arg split', () => {
    const out = pbiDaxToSigma('DIVIDE([Sales], [Count], "N/A, review")', [], 'M');
    assert.equal(out, 'If(([Count]) = 0, "N/A, review", ([Sales]) / ([Count]))');
  });

  test('unbalanced parens inside the fallback-text literal do not corrupt the replaced span', () => {
    const out = pbiDaxToSigma('DIVIDE([Sales], [Count], "note (unbalanced") + [Other]', [], 'M');
    assert.match(String(out), /"note \(unbalanced"/, 'literal text is untouched');
    assert.match(String(out), /\+ \[Other\]/, 'text after the DIVIDE call is not swallowed/duplicated');
  });
});

// ── 2. 'table'[col] / Table[col] qualifier stripping ────────────────────────

describe("Table[Column] qualifier stripping: literal-aware", () => {
  test('a label string containing Word[Bracket]-shaped text is not rewritten or corrupted', () => {
    const warnings: string[] = [];
    const out = pbiDaxToSigma('CONCATENATE([Amount], " see Store[Count] for detail")', warnings, 'M');
    assert.match(String(out), /Store\[Count\]/, 'literal content preserved verbatim');
    assert.ok(!warnings.some(w => /multiple tables/.test(w)), 'no bogus multi-table warning from literal content');
  });

  test("control: a real 'table'[col] / Table[col] qualifier is still stripped", () => {
    const out = pbiDaxToSigma("SALES[Amount] + 'Cost Table'[Amount]", [], 'M');
    assert.doesNotMatch(String(out), /SALES\[|'Cost Table'\[/, 'real qualifiers are still stripped');
  });
});

// ── 3a. Calc-column / measure canonical-rename rewrite ──────────────────────

describe('Canonical-rename rewrite: literal-aware', () => {
  test('calc column: a label string naming a tracked column is not rewritten', () => {
    const m = model([
      tbl('SALES', ['AMOUNT_TOTAL'], [], [
        { name: 'Labeled', dataType: 'string', type: 'calculated',
          expression: 'CONCATENATE("See [AMOUNT_TOTAL] before adjustments: ", SALES[AMOUNT_TOTAL])' },
      ]),
    ]);
    const out = convertPowerBIToSigma(m, OPTS);
    const sales = elementsOf(out).find((e: any) => e.name === 'SALES');
    const col = sales.columns.find((c: any) => c.name === 'Labeled');
    assert.match(String(col.formula), /See \[AMOUNT_TOTAL\] before adjustments/, 'literal text untouched');
    assert.match(String(col.formula), /\[Amount Total\]/, 'the REAL ref is still renamed to its Sigma display name');
  });

  test('measure: a label string naming a tracked column is not rewritten', () => {
    const m = model([
      tbl('SALES', ['AMOUNT_TOTAL'], [{
        name: 'Labeled Amount',
        expression: 'CONCATENATE("See [AMOUNT_TOTAL] before adjustments: ", SALES[AMOUNT_TOTAL])',
      }]),
    ]);
    const out = convertPowerBIToSigma(m, OPTS);
    const met = metricsOf(out).find((x: any) => x.name === 'Labeled Amount');
    assert.match(String(met.formula), /See \[AMOUNT_TOTAL\] before adjustments/, 'literal text untouched');
    assert.match(String(met.formula), /\[Amount Total\]/, 'the REAL ref is still renamed');
  });
});

// ── 3b. Cross-table-guard rename+detect / pruneDanglingMetrics ──────────────

describe('Cross-table guard + dangling-metric prune: literal-aware', () => {
  test('a metric label mentioning a DROPPED sibling measure by name is NOT cascade-dropped', () => {
    const m = model([
      tbl('SALES', ['AMOUNT_TOTAL'], [
        { name: 'Broken Measure', expression: "TOTALYTD(SUM(SALES[AMOUNT_TOTAL]), 'Date'[Date])" },
        { name: 'Labeled Ratio', expression: 'CONCATENATE("See [Broken Measure] note: ", SALES[AMOUNT_TOTAL])' },
      ]),
    ]);
    const out = convertPowerBIToSigma(m, OPTS);
    const names = metricsOf(out).map((x: any) => x.name);
    assert.ok(names.includes('Labeled Ratio'),
      `independent metric was wrongly cascade-dropped; warnings: ${JSON.stringify(out.warnings)}`);
    assert.ok(!names.includes('Broken Measure'), 'sanity: the genuinely-unconvertible measure IS dropped');
  });

  test('control: a metric that REALLY depends on a dropped measure is still pruned', () => {
    const m = model([
      tbl('SALES', ['AMOUNT_TOTAL'], [
        { name: 'Broken Measure', expression: "TOTALYTD(SUM(SALES[AMOUNT_TOTAL]), 'Date'[Date])" },
        { name: 'Real Dependent', expression: '[Broken Measure] + 1' },
      ]),
    ]);
    const out = convertPowerBIToSigma(m, OPTS);
    const names = metricsOf(out).map((x: any) => x.name);
    assert.ok(!names.includes('Real Dependent'), 'a metric that truly references the dropped measure is still pruned');
  });
});

// ── 4. Cross-element calc-column pull (hasCross) ────────────────────────────

describe('Cross-element calc-column pull: literal-aware', () => {
  test('a fully local calc column with a label naming a RELATED table\'s column is NOT pulled off its source element', () => {
    const m = model([
      tbl('SALES', ['AMOUNT_TOTAL', 'CUSTOMER_ID'], [], [
        { name: 'Labeled Amount', dataType: 'string', type: 'calculated',
          expression: 'CONCATENATE("See [Customer Name] on file: ", SALES[AMOUNT_TOTAL])' },
      ]),
      tbl('CUSTOMER', ['CUSTOMER_ID', 'CUSTOMER_NAME']),
    ], [{ name: 'r1', fromTable: 'SALES', fromColumn: 'CUSTOMER_ID', toTable: 'CUSTOMER', toColumn: 'CUSTOMER_ID' }]);
    const out = convertPowerBIToSigma(m, OPTS);
    const sales = elementsOf(out).find((e: any) => e.name === 'SALES');
    const col = sales.columns.find((c: any) => c.name === 'Labeled Amount');
    assert.ok(col, 'calc column stays on its own source element (not pulled to a derived view)');
    assert.match(String(col.formula), /See \[Customer Name\] on file/, 'literal text untouched');
  });
});
