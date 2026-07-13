// Regression coverage for the "silent fallback" defect class (beads-sigma-lanq.1/.3):
// unmapped aggregations and untranslatable metrics must NEVER be emitted silently —
// they warn (and never ship a /* placeholder */ formula). Repro inputs are the
// E2E-confirmed cases from the converter-defect audit.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { convertSqlToSigma } from './sql.js';
import { convertCubeToSigma } from './cube.js';
import { convertDbtToSigma } from './dbt.js';
import { convertCognosToSigma } from './cognos.js';
import { convertOmniToSigma } from './omni.js';
import { convertBobjToSigma } from './bobj.js';

const metricsOf = (model: any): { name: string; formula: string }[] => {
  const out: any[] = [];
  for (const p of model?.pages || []) for (const e of p.elements || []) for (const m of e.metrics || []) out.push({ name: m.name, formula: m.formula });
  return out;
};
const warned = (ws: string[], re: RegExp) => ws.some(w => re.test(w));
const noPlaceholder = (ms: { formula: string }[]) => ms.every(m => !/\/\*/.test(m.formula));

test('sql: compound aggregate is skipped + warned, never a /* TODO */ metric (lanq.3)', () => {
  const r = convertSqlToSigma([{ name: 'Profit', sql: 'SELECT region, SUM(revenue)-SUM(cost) AS profit FROM sales GROUP BY region' }], {});
  assert.ok(noPlaceholder(metricsOf(r.model)), 'no placeholder formula shipped');
  assert.ok(warned(r.warnings, /compound aggregate/i), 'warns about the compound aggregate');
});

test('dbt: unsupported metric type is skipped+warned; unmapped agg (stddev) warns, never silent (lanq.1/.3)', () => {
  const yaml = `semantic_models:
  - name: orders
    model: ref('fct_orders')
    entities: [{name: order_id, type: primary}]
    dimensions: [{name: order_date, type: time}]
    measures:
      - {name: order_total, agg: sum, expr: amount}
      - {name: revenue_stddev, agg: stddev, expr: amount}
metrics:
  - name: cumulative_order_total
    type: cumulative
    type_params: {measure: order_total}`;
  const r = convertDbtToSigma(yaml, { database: 'ANALYTICS', schema: 'DBT' });
  assert.ok(noPlaceholder(metricsOf(r.model)), 'no /* placeholder */ metric shipped');
  assert.ok(warned(r.warnings, /cumulative|could not be translated/i), 'warns about the cumulative metric');
  assert.ok(warned(r.warnings, /stddev|no Sigma mapping/i), 'warns about the unmapped stddev aggregation');
});

test('cube: runningTotal + unknown type (p90) warn instead of silently defaulting to Sum (lanq.1)', () => {
  const content = `cubes:
  - name: orders
    sql_table: ANALYTICS.PUBLIC.ORDERS
    dimensions:
      - {name: id, sql: id, type: number, primary_key: true}
      - {name: status, sql: status, type: string}
      - {name: amount, sql: amount, type: number}
    measures:
      - {name: running_amount, sql: amount, type: runningTotal}
      - {name: p90_amount, sql: amount, type: p90}
      - {name: completed_amount, sql: amount, type: runningTotal, filters: [{sql: "{CUBE}.status = 'completed'"}]}`;
  const r = convertCubeToSigma([{ name: 'orders.yml', content }], { database: 'ANALYTICS', schema: 'PUBLIC' });
  assert.ok(warned(r.warnings, /runningTotal/i), 'warns runningTotal was approximated as Sum');
  assert.ok(warned(r.warnings, /p90|no Sigma mapping/i), 'warns p90 has no mapping');
});

test('omni: percentile measure with sql warns instead of silently defaulting to Sum (lanq.1)', () => {
  const content = `view: orders
sql_table_name: ANALYTICS.PUBLIC.ORDERS
dimensions:
  - name: id
    type: number
    primary_key: true
    sql: \${TABLE}.ID
measures:
  - name: pctl_amount
    type: percentile
    sql: \${TABLE}.AMOUNT`;
  const r = convertOmniToSigma([{ name: 'orders.view.yaml', content } as any], { database: 'ANALYTICS', schema: 'PUBLIC' });
  assert.ok(warned(r.warnings, /percentile|no Sigma mapping/i), 'warns about the unmapped percentile aggregation');
});

test('cognos: countDistinct maps to CountDistinct; standardDeviation warns instead of silent Sum (lanq.1)', () => {
  const mod = {
    identifier: 'Sales Module',
    querySubject: [{
      identifier: 'Sales', ref: ['M1.SALES_FACT'],
      item: [
        { queryItem: { identifier: 'CustomerId', usage: 'identifier' } },
        { queryItem: { identifier: 'DistinctCustomers', usage: 'fact', regularAggregate: 'countDistinct' } },
        { queryItem: { identifier: 'RevenueStdDev', usage: 'fact', regularAggregate: 'standardDeviation' } },
      ],
    }],
  };
  const r = convertCognosToSigma(mod as any, { database: 'CSA', schema: 'TJ' });
  const cd = metricsOf(r.model).find(m => /Distinct Customers/i.test(m.name));
  assert.ok(cd && /CountDistinct/i.test(cd.formula), 'countDistinct maps to CountDistinct, not Sum');
  assert.ok(warned(r.warnings, /standarddeviation|no Sigma mapping/i), 'warns about the unmapped standardDeviation');
});

test('bobj: unknown aggregations (Median/Delegated) warn instead of silently defaulting to Sum (lanq.1)', () => {
  const uni = {
    name: 'Sales Universe',
    tables: [{ name: 'SALES_FACT', schema: 'TJ', database: 'CSA' }],
    objects: [
      { name: 'Total Amount', qualification: 'Measure', select: 'SALES_FACT.AMOUNT', aggregation: 'Sum', dataType: 'Numeric' },
      { name: 'Median Order Value', qualification: 'Measure', select: 'SALES_FACT.AMOUNT', aggregation: 'Median', dataType: 'Numeric' },
      { name: 'Delegated Margin', qualification: 'Measure', select: 'SALES_FACT.MARGIN', aggregation: 'Delegated', dataType: 'Numeric' },
    ],
  };
  const r = convertBobjToSigma(uni, {});
  assert.ok(warned(r.warnings, /aggregation "median"|aggregation "delegated"|no Sigma mapping/i), 'warns about the unmapped Median/Delegated aggregations');
});

// ── lanq.2: cube relationship cardinality — no silent wrong N:1 ──────────────
const relTypeOf = (model: any): string | undefined => {
  for (const p of model?.pages || []) for (const e of p.elements || []) if (e.relationships?.[0]) return e.relationships[0].relationshipType;
  return undefined;
};
const cubeRel = (rel?: string) => {
  const relLine = rel === undefined ? '' : `\n        relationship: ${rel}`;
  const content = `cubes:
  - name: customers
    sql_table: ANALYTICS.PUBLIC.CUSTOMERS
    dimensions:
      - {name: id, sql: id, type: number, primary_key: true}
    joins:
      - name: orders
        sql: "{CUBE}.id = {orders}.customer_id"${relLine}
  - name: orders
    sql_table: ANALYTICS.PUBLIC.ORDERS
    dimensions:
      - {name: id, sql: id, type: number, primary_key: true}
      - {name: customer_id, sql: customer_id, type: number}`;
  return convertCubeToSigma([{ name: 'm.yml', content }], { database: 'ANALYTICS', schema: 'PUBLIC' });
};

test('cube: omitted relationship defaults to N:1 but warns it was missing (lanq.2)', () => {
  const r = cubeRel(undefined);
  assert.equal(relTypeOf(r.model), 'N:1');
  assert.ok(warned(r.warnings, /relationship is missing/i), 'warns the relationship was missing, not silent');
});
test('cube: legacy alias hasMany maps to 1:N with a normalization warning (lanq.2)', () => {
  const r = cubeRel('hasMany');
  assert.equal(relTypeOf(r.model), '1:N');
  assert.ok(warned(r.warnings, /legacy Cube relationship/i), 'warns the legacy alias was normalized');
});
test('cube: canonical one_to_many maps to 1:N with no relationship warning (lanq.2)', () => {
  const r = cubeRel('one_to_many');
  assert.equal(relTypeOf(r.model), '1:N');
  assert.ok(!warned(r.warnings, /relationship is missing|legacy Cube|not a recognized/i), 'no spurious relationship warning on a canonical value');
});
