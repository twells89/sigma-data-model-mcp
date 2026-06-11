/**
 * QuickSight formula-mapper tests — conditional aggregates, regex family,
 * parse/convert family (beads-sigma-lvdw).
 *
 * Verified Sigma signatures these tests encode:
 *   SumIf/AvgIf/MinIf/MaxIf/CountDistinctIf(field, condition…) — field FIRST
 *     (Sigma docs + live CSA.TJ verification via the ThoughtSpot converter),
 *     which matches QuickSight's (measure, condition) order → 1:1 remap.
 *   CountIf(condition…) — conditions only, so QS countIf(operand, cond) drops
 *     the operand.
 *   RegexpExtract / RegexpReplace / RegexpMatch / RegexpCount(text, pattern…)
 *     — same emissions as the Tableau converter (live-verified).
 */

import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { quicksightFormulaToSigmaEx } from './quicksight.js';

function conv(expr: string): { formula: string; description?: string; warnings: string[] } {
  const warnings: string[] = [];
  const r = quicksightFormulaToSigmaEx(expr, warnings);
  return { ...r, warnings };
}

describe('QuickSight conditional aggregates → Sigma *If (field-first, no swap)', () => {
  const CASES: Array<[string, string]> = [
    ["sumIf({Sales}, {Region} = 'West')", 'SumIf([Sales], [Region] = "West")'],
    ["avgIf({Sales}, {Region} = 'West' AND {Year} = 2025)", 'AvgIf([Sales], [Region] = "West" AND [Year] = 2025)'],
    ["minIf({Sales}, {Region} = 'West')", 'MinIf([Sales], [Region] = "West")'],
    ["maxIf({Sales}, {Region} = 'West')", 'MaxIf([Sales], [Region] = "West")'],
    ["distinct_countIf({Customer Id}, {Region} = 'West')", 'CountDistinctIf([Customer Id], [Region] = "West")'],
    ["countIf({Order Id}, {Status} = 'Shipped')", 'CountIf([Status] = "Shipped")'],
    ['countIf({Is Active})', 'CountIf([Is Active])'],
    ["ifelse(sumIf({Sales}, ({Region} = 'West' OR {Region} = 'East')) > 0, 1, 0)",
      'If(SumIf([Sales], ([Region] = "West" OR [Region] = "East")) > 0, 1, 0)'],
  ];
  for (const [input, expected] of CASES) {
    test(input, () => {
      const r = conv(input);
      assert.equal(r.formula, expected);
      assert.equal(r.warnings.length, 0, `unexpected warnings: ${r.warnings.join(' | ')}`);
    });
  }

  test('medianIf / stdevIf / varIf family flag-not-drop (no Sigma equivalent)', () => {
    for (const fn of ['medianIf', 'stdevIf', 'stdevpIf', 'varIf', 'varpIf']) {
      const r = conv(`${fn}({Sales}, {Region} = 'West')`);
      assert.equal(r.formula, 'Null', `${fn} must degrade to Null`);
      assert.ok(r.description?.includes(fn), `${fn} description must carry the original`);
      assert.equal(r.warnings.length, 1);
      assert.match(r.warnings[0], /Sigma has no/i);
    }
  });
});

describe('REGEXP-style functions → Sigma Regexp* (verified names)', () => {
  const CASES: Array<[string, string]> = [
    ["regexp_extract({Email}, '@(.*)$')", 'RegexpExtract([Email], "@(.*)$")'],
    ["REGEXP_EXTRACT({Email}, '@(.*)$')", 'RegexpExtract([Email], "@(.*)$")'],
    ["regexp_substr({Email}, '@.*')", 'RegexpExtract([Email], "@.*")'],
    ["regexp_replace({Phone}, '[^0-9]', '')", 'RegexpReplace([Phone], "[^0-9]", "")'],
    ["regexp_like({Sku}, '^AB-')", 'RegexpMatch([Sku], "^AB-")'],
    ["regexp_matches({Sku}, '^AB-')", 'RegexpMatch([Sku], "^AB-")'],
    ["rlike({Sku}, '^AB-')", 'RegexpMatch([Sku], "^AB-")'],
    ["regexp_count({Notes}, 'error')", 'RegexpCount([Notes], "error")'],
  ];
  for (const [input, expected] of CASES) {
    test(input, () => {
      const r = conv(input);
      assert.equal(r.formula, expected);
      assert.equal(r.warnings.length, 0, `unexpected warnings: ${r.warnings.join(' | ')}`);
    });
  }

  test('unmapped regex functions flag-not-drop', () => {
    for (const expr of ["regexp_instr({Sku}, 'AB')", "regexp_split_to_array({Csv}, ',')"]) {
      const r = conv(expr);
      assert.equal(r.formula, 'Null');
      assert.ok(r.description, 'description must carry the original expression');
      assert.equal(r.warnings.length, 1);
      assert.match(r.warnings[0], /no 1:1 Sigma equivalent/);
    }
  });

  test('regex pattern string literals are not munged', () => {
    const r = conv("regexp_extract({Code}, '([A-Z]{2})-(\\\\d+)')");
    assert.match(r.formula, /^RegexpExtract\(\[Code\], /);
    assert.equal(r.warnings.length, 0);
  });
});

describe('parse/convert family', () => {
  test('parseInt → Int(Number(…))', () => {
    assert.equal(conv('parseInt({Code})').formula, 'Int(Number([Code]))');
  });
  test('parseInt nested', () => {
    assert.equal(
      conv('ifelse(parseInt({Code}) > 5, 1, 0)').formula,
      'If(Int(Number([Code])) > 5, 1, 0)');
  });
  test('parseDecimal → Number', () => {
    assert.equal(conv('parseDecimal({Code})').formula, 'Number([Code])');
  });
  test('toString → Text, decimalToInt → Int', () => {
    assert.equal(conv('toString({Qty})').formula, 'Text([Qty])');
    assert.equal(conv('decimalToInt({Qty})').formula, 'Int([Qty])');
  });
});

describe('existing degrades stay intact', () => {
  test('window/table-calc functions still degrade to Null', () => {
    const r = conv('runningSum(sum({Sales}), [{Order Date} ASC])');
    assert.equal(r.formula, 'Null');
    assert.equal(r.warnings.length, 1);
  });
  test('parameter refs still degrade to Null', () => {
    const r = conv('${BasePeriod} + 1');
    assert.equal(r.formula, 'Null');
    assert.equal(r.warnings.length, 1);
  });
});
