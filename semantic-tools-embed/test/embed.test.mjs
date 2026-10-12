// Embed-layer behaviour tests. Run against the BUILT ESM bundle:
//   npm run build:semantic-tools && node --test semantic-tools-embed/test/embed.test.mjs
import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { parse as parseYaml, stringify as stringifyYaml } from 'yaml';

import * as S from '../../vendor/semantic-tools/semantic-tools-embed.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const FIX = join(ROOT, '.cache/semantic-tools/packages/cli/src/__tests__');
const ddl = n => readFileSync(join(FIX, 'fixtures', n + '.sql'), 'utf8');
const golden = n => readFileSync(join(FIX, 'golden', n + '.json'), 'utf8');
const PLUGS = readFileSync(join(dirname(fileURLToPath(import.meta.url)), 'fixtures', 'plugs.yaml'), 'utf8');
const BASE = { connectionId: '9ad28df9-5f5f-4bae-8985-bb12d25829de', database: 'ANALYTICS', schema: 'MODELS', columnTypes: {} };
const STAR_ENGINE = { ...BASE, crossTableReferences: 'engine' };
const count = (result, code) => result.warnings.filter(w => w.code === code).length;

test('ENGINE exposes the pinned commit and embed version', () => {
  assert.equal(S.ENGINE.semanticToolsCommit, readFileSync(join(ROOT, 'semantic-tools-embed/PIN'), 'utf8').trim());
  assert.match(S.ENGINE.embedVersion, /^\d+\.\d+\.\d+$/);
});

test('missing connectionId throws; nothing else does', () => {
  assert.throws(() => S.convertSemanticView(ddl('01-single-table'), {}), /connectionId/);
  assert.throws(() => S.convertSemanticView(ddl('01-single-table'), { connectionId: '  ' }), /connectionId/);
});

test('detectFormat', () => {
  assert.equal(S.detectFormat(ddl('01-single-table')), 'snowflake-semantic-view');
  assert.equal(S.detectFormat('create or replace semantic view x tables (...)'), 'snowflake-semantic-view');
  assert.equal(S.detectFormat(PLUGS), 'snowflake-semantic-view-yaml');
  assert.equal(S.detectFormat('SELECT 1'), undefined);
});

test('DDL fixture 12 without database/schema reports what is needed and converts nothing', () => {
  const run = S.convertSemanticView(ddl('12-unqualified-names'), { connectionId: BASE.connectionId, columnTypes: {} });
  const r = run.results[0];
  assert.equal(r.ok, false);
  assert.equal(r.spec, undefined);
  assert.deepEqual(r.needs, { database: ['REGION', 'SF1.NATION'], schema: ['REGION'] });
});

test('needs result carries embed-sourced NEEDS_DATABASE / NEEDS_SCHEMA error findings listing the refs', () => {
  const r = S.convertSemanticView(ddl('12-unqualified-names'), { connectionId: BASE.connectionId, columnTypes: {} }).results[0];
  const db = r.findings.find(f => f.code === 'NEEDS_DATABASE');
  const sc = r.findings.find(f => f.code === 'NEEDS_SCHEMA');
  for (const f of [db, sc]) {
    assert.ok(f, 'finding present');
    assert.equal(f.severity, 'error');
    assert.equal(f.source, 'embed');
  }
  assert.match(db.message, /REGION, SF1\.NATION/);
  assert.match(db.remediation, /database/);
  assert.match(sc.message, /REGION/);
  // database supplied -> only NEEDS_SCHEMA remains
  const r2 = S.convertSemanticView(ddl('12-unqualified-names'), { connectionId: BASE.connectionId, database: 'X', columnTypes: {} }).results[0];
  assert.deepEqual(r2.findings.map(f => f.code), ['NEEDS_SCHEMA']);
  // upstream findings are never tagged as embed
  const ok = S.convertSemanticView(ddl('03-non-additive-metric'), BASE).results[0];
  assert.ok(ok.findings.length > 0 && ok.findings.every(f => f.source === undefined));
});

test('DDL fixture 12 with database/schema matches the golden byte-for-byte', () => {
  const r = S.convertSemanticView(ddl('12-unqualified-names'), BASE).results[0];
  assert.equal(r.ok, true);
  assert.equal(r.needs, undefined);
  assert.equal(r.spec + '\n', golden('12-unqualified-names'));
  assert.ok(Array.isArray(r.tablesForColumnTypes) && r.tablesForColumnTypes.length > 0);
});

test('blank database counts as not supplied (CLI semantics)', () => {
  const r = S.convertSemanticView(ddl('12-unqualified-names'), { ...BASE, database: '   ' }).results[0];
  assert.equal(r.ok, false);
  assert.ok(r.needs.database.length > 0);
});

test('viewName comes from the DDL', () => {
  assert.equal(S.convertSemanticView(ddl('02-star-schema'), BASE).results[0].viewName, 'SALES_VIEW');
});

test('YAML (PLUGS): detected, converts, 3 metrics, schemaVersion 1, SYNONYMS_DROPPED warned', () => {
  const run = S.convertSemanticView(PLUGS, { connectionId: BASE.connectionId });
  assert.equal(run.format, 'snowflake-semantic-view-yaml');
  assert.equal(run.results.length, 1);
  const r = run.results[0];
  assert.equal(r.ok, true);
  const spec = JSON.parse(r.spec);
  assert.equal(spec.schemaVersion, 1);
  const metrics = spec.pages[0].elements.flatMap(e => e.metrics ?? []).map(m => m.name).sort();
  assert.deepEqual(metrics, ['Profit', 'Profit Margin', 'Revenue']);
  assert.ok(count(r, 'SYNONYMS_DROPPED') > 0);
  // columnTypes omitted -> the CLI's COLUMN_TYPES_SKIPPED warning
  assert.equal(count(r, 'COLUMN_TYPES_SKIPPED'), 1);
});

test('columnTypes supplied -> no COLUMN_TYPES_SKIPPED', () => {
  const r = S.convertSemanticView(PLUGS, { connectionId: BASE.connectionId, columnTypes: {} }).results[0];
  assert.equal(count(r, 'COLUMN_TYPES_SKIPPED'), 0);
});

test('synonymsInDescription adds "Also known as:" and removes matching SYNONYMS_DROPPED', () => {
  const off = S.convertSemanticView(PLUGS, { connectionId: BASE.connectionId }).results[0];
  const on = S.convertSemanticView(PLUGS, { connectionId: BASE.connectionId, synonymsInDescription: true }).results[0];
  assert.equal(on.ok, true);
  const descs = [];
  for (const el of JSON.parse(on.spec).pages[0].elements) {
    if (el.description) descs.push(el.description);
    for (const x of [...el.columns, ...(el.metrics ?? [])]) if (x.description) descs.push(x.description);
  }
  assert.ok(descs.some(d => d.includes('Also known as:')));
  assert.ok(count(on, 'SYNONYMS_DROPPED') < count(off, 'SYNONYMS_DROPPED'));
  // Revenue metric carries its synonyms
  const revenue = JSON.parse(on.spec).pages[0].elements.flatMap(e => e.metrics ?? []).find(m => m.name === 'Revenue');
  assert.match(revenue.description, /Also known as: .*revenue_generated/);
});

test('synonymsInDescription for DDL: joins base comment, matches renamed dimension columns', () => {
  const r = S.convertSemanticView(ddl('02-star-schema'), { ...BASE, synonymsInDescription: true }).results[0];
  const els = JSON.parse(r.spec).pages[0].elements;
  const orders = els.find(e => (e.description ?? '').startsWith('One row per order'));
  assert.equal(orders.description, 'One row per order. Also known as: sales orders, purchase orders');
  const lower = S.convertSemanticView(ddl('10-lowercase'), { ...BASE, synonymsInDescription: true }).results[0];
  const status = JSON.parse(lower.spec).pages[0].elements[0].columns.find(c => c.name === 'Status');
  assert.equal(status.description, 'Also known as: state'); // dimension STATUS over column O_ORDERSTATUS
  assert.equal(count(lower, 'SYNONYMS_DROPPED'), 0);
});

test("all opt-ins off + crossTableReferences:'engine': spec is byte-identical to core convert()", () => {
  for (const [source, format, options] of [
    [PLUGS, 'snowflake-semantic-view-yaml', { connectionId: BASE.connectionId, columnTypes: {} }],
    [ddl('02-star-schema'), 'snowflake-semantic-view', BASE],
    [ddl('16-sample-values'), 'snowflake-semantic-view', BASE],
  ]) {
    const raw = S.rawConvert(source, format, options);
    const r = S.convertSemanticView(source, { ...options, synonymsInDescription: false, inferNumberFormats: false, crossTableReferences: 'engine' }).results[0];
    assert.equal(raw.ok, true);
    assert.equal(r.spec, raw.spec);
  }
});

test('inferNumberFormats adds format to metrics/calculated columns and is off by default', () => {
  const off = S.convertSemanticView(PLUGS, { connectionId: BASE.connectionId }).results[0];
  const on = S.convertSemanticView(PLUGS, { connectionId: BASE.connectionId, inferNumberFormats: true }).results[0];
  const fmts = spec => JSON.parse(spec).pages[0].elements.flatMap(e => [...e.columns, ...(e.metrics ?? [])]).filter(x => x.format);
  assert.equal(fmts(off.spec).length, 0);
  const metrics = JSON.parse(on.spec).pages[0].elements.flatMap(e => e.metrics ?? []);
  assert.ok(fmts(on.spec).length > 0);
  // Evidence-based: Profit = Sum(qty * (price - cost)) is currency; Profit Margin = 100 * (a / b) is percent POINTS (',.2f' + '%' suffix).
  assert.equal(metrics.find(m => m.name === 'Profit').format.currencySymbol, '$');
  assert.deepEqual(metrics.find(m => m.name === 'Profit Margin').format, PP);
});

test('multi-statement DDL yields one result per statement', () => {
  const run = S.convertSemanticView(ddl('01-single-table') + '\n;\n' + ddl('02-star-schema'), STAR_ENGINE);
  assert.equal(run.results.length, 2);
  assert.deepEqual(run.results.map(r => r.ok), [true, true]);
  assert.deepEqual(run.results.map(r => r.viewName).length, 2);
  assert.equal(run.results[0].spec + '\n', golden('01-single-table'));
  assert.equal(run.results[1].spec + '\n', golden('02-star-schema'));
});

test('bestEffort on 03-non-additive-metric: refused without, converts with overriddenErrors', () => {
  const strict = S.convertSemanticView(ddl('03-non-additive-metric'), BASE).results[0];
  assert.equal(strict.ok, false);
  assert.equal(strict.bestEffortWouldConvert, true); // the engine says the flag would help...
  const loose = S.convertSemanticView(ddl('03-non-additive-metric'), { ...BASE, bestEffort: true }).results[0];
  assert.equal(loose.ok, true); // ...and it does
  assert.deepEqual(loose.overriddenErrors.map(e => e.code), ['SEMI_ADDITIVE_METRIC']);
  assert.ok(loose.findings.some(f => f.code === 'SEMI_ADDITIVE_METRIC' && f.severity === 'error'));
});

test('bestEffort cannot override an always-blocks refusal (11-tables-only)', () => {
  const r = S.convertSemanticView(ddl('11-tables-only'), { ...BASE, bestEffort: true }).results[0];
  assert.equal(r.ok, false);
  assert.equal(r.bestEffortWouldConvert, false);
});

test('garbage input never throws and yields ok:false with a finding', () => {
  for (const junk of ['', '   ', 'garbage {{{', 'CREATE SEMANTIC VIEW x AS nonsense', 'name: [unterminated', '\u0000\u0001', 'name: x\ntables: 5']) {
    const run = S.convertSemanticView(junk, { connectionId: 'c' });
    assert.ok(run.results.length >= 1, JSON.stringify(junk));
    assert.ok(run.results.every(r => r.ok === false && r.findings.length > 0), JSON.stringify(junk));
  }
  assert.equal(S.convertSemanticView('x', { connectionId: 'c', format: 'nope' }).results[0].ok, false);
  assert.equal(S.convertSemanticView(undefined, { connectionId: 'c' }).results[0].ok, false);
});

test('findings carry code/severity/message and remediation for errors', () => {
  const r = S.convertSemanticView(ddl('03-non-additive-metric'), BASE).results[0];
  assert.ok(r.findings.length > 0);
  for (const f of r.findings) {
    assert.equal(typeof f.code, 'string');
    assert.ok(f.severity === 'error' || f.severity === 'warning');
    assert.equal(typeof f.message, 'string');
    assert.ok(f.message.length > 0);
    if (f.severity === 'error') assert.equal(typeof f.remediation, 'string');
  }
});

test('DDL starting with SQL comments is still detected', () => {
  const r = S.convertSemanticView(ddl('16-sample-values'), BASE);
  assert.equal(r.format, 'snowflake-semantic-view');
  assert.equal(r.results[0].ok, true);
});

// ── UNRECOGNIZED_YAML_KEY (keys the upstream YAML reader silently ignores) ──
const RETAIL = readFileSync(join(dirname(fileURLToPath(import.meta.url)), 'fixtures', 'retail_inline_relationships.yaml'), 'utf8');
const unrec = r => r.findings.filter(f => f.code === 'UNRECOGNIZED_YAML_KEY');
const yamlRun = text => S.convertSemanticView(text, BASE).results[0];
const MINI = `name: V
tables:
  - name: T
    base_table: { database: D, schema: S, table: T }
    dimensons:
      - { name: A, expr: A }
    dimensions:
      - { name: B, expr: B, bogus: 1 }
`;

test('UNRECOGNIZED_YAML_KEY: relationships nested inside a table are reported (retail fixture), result still ok', () => {
  const r = yamlRun(RETAIL);
  assert.equal(r.ok, true);
  const f = unrec(r);
  assert.equal(f.length, 1);
  assert.equal(f[0].severity, 'warning');
  assert.equal(f[0].source, 'embed');
  assert.equal(f[0].object.name, 'RETAIL_ANALYTICS_SEMANTIC_VIEW');
  assert.match(f[0].message, /tables\[ORDER_FACT\]\.relationships/);
  assert.match(f[0].message, /NOT converted/);
  assert.match(f[0].message, /top-level `relationships`.*left_table.*right_table.*relationship_columns/);
  // upstream really does lose them silently: no relationship in the spec
  assert.equal((JSON.parse(r.spec).pages[0].elements.flatMap(e => e.relationships ?? [])).length, 0);
});

test('UNRECOGNIZED_YAML_KEY: PLUGS yaml has none (data_type is benign; access_modifier/synonyms/description/primary_key are read by upstream)', () => {
  const r = yamlRun(PLUGS);
  assert.equal(r.ok, true);
  assert.deepEqual(unrec(r).map(f => f.message), []);
});

test('UNRECOGNIZED_YAML_KEY: exact path set for typos and unknown keys at several levels', () => {
  const doc = `name: V
bogus_root: 1
tables:
  - name: T
    base_table: { database: D, schema: S, table: T, extra: 1 }
    primary_key: { columns: [A], nope: 1 }
    filters: [{ name: f, expr: "A > 1" }]
    dimensons:
      - { name: A, expr: A }
    dimensions:
      - { name: B, expr: B, bogus: 1, data_type: TEXT }
    metrics:
      - { name: M, expr: "SUM(B)", foo: 2 }
metrics:
  - { name: DM, expr: "M", zip: 1 }
`;
  const f = unrec(yamlRun(doc));
  assert.equal(f.length, 1);
  const listed = f[0].message.split('NOT converted: ')[1].replace(/\.$/, '').split(', ');
  const expected = ['bogus_root', 'tables[T].base_table.extra', 'tables[T].primary_key.nope', 'tables[T].filters', 'tables[T].dimensons',
    'tables[T].dimensions[B].bogus', 'tables[T].metrics[M].foo', 'metrics[DM].zip'];
  assert.deepEqual([...listed].sort(), [...expected].sort());
  assert.match(f[0].message, /^8 YAML key paths are not part/);
  assert.ok(!f[0].message.includes('data_type'));
  assert.ok(!/NOT converted;/.test(f[0].message), 'no nested-relationships sentence without tables[].relationships');
});

test('UNRECOGNIZED_YAML_KEY: a table-level typo (`dimensons`) is reported even when the view is refused', () => {
  const r = yamlRun(MINI.replace(/    dimensions:\n.*\n/, ''));
  assert.equal(r.ok, false);
  assert.ok(r.errors.some(e => e.code === 'MISSING_DIMENSIONS_OR_METRICS'));
  const f = unrec(r);
  assert.equal(f.length, 1);
  assert.match(f[0].message, /tables\[T\]\.dimensons/);
});

test('UNRECOGNIZED_YAML_KEY: never raised for DDL input', () => {
  for (const n of ['01-single-table', '02-star-schema', '17-mapped-constructs']) {
    assert.equal(unrec(S.convertSemanticView(ddl(n), BASE).results[0]).length, 0, n);
  }
  // DDL text that merely contains YAML-looking words stays a DDL result
  const run = S.convertSemanticView(ddl('01-single-table'), { ...BASE, format: 'snowflake-semantic-view' });
  assert.equal(run.results.flatMap(r => r.findings).filter(f => f.code === 'UNRECOGNIZED_YAML_KEY').length, 0);
});

test('UNRECOGNIZED_YAML_KEY: path list is capped with a count', () => {
  const dims = Array.from({ length: 20 }, (_, i) => `      - { name: D${i}, expr: D${i}, junk: 1 }`).join('\n');
  const f = unrec(yamlRun(`name: V\ntables:\n  - name: T\n    base_table: { database: D, schema: S, table: T }\n    dimensions:\n${dims}\n`));
  assert.equal(f.length, 1);
  assert.match(f[0].message, /^20 YAML key paths are not part/);
  assert.match(f[0].message, /\(and 8 more\)/);
});

// ── crossTableReferences ('derived' default | 'engine') ──

const THREE_SEG = /\[[^\]/]+\/[^\]/]+\/[^\]/]+\]/;
const els = r => JSON.parse(r.spec).pages[0].elements;
const el = (r, id) => els(r).find(e => e.id === id);
const xref = (r, code) => r.findings.filter(f => f.code === code);

test('crossTableReferences: 02 default moves the cross-table column to orders__joined with the Sigma-resolvable spelling', () => {
  const r = S.convertSemanticView(ddl('02-star-schema'), BASE).results[0];
  assert.equal(r.ok, true);
  const orders = el(r, 'orders');
  const joined = el(r, 'orders__joined');
  assert.ok(!THREE_SEG.test(JSON.stringify(orders)), 'orders keeps no 3-segment reference');
  assert.ok(!orders.columns.some(c => c.id === 'orders__customer_name'));
  assert.ok(!orders.order.includes('orders__customer_name'));
  assert.deepEqual(joined.source, { kind: 'table', elementId: 'orders' });
  assert.equal(joined.kind, 'table');
  assert.equal(joined.name, 'ORDERS (joined)');
  const moved = joined.columns.find(c => c.id === 'orders__customer_name');
  assert.equal(moved.formula, '[ORDERS/Orders To Customer/C Name]');
  assert.equal(moved.name, 'Customer Name');
  // every other orders column has a passthrough (2-part form) on the joined element
  for (const c of orders.columns) {
    const j = joined.columns.find(x => x.id === c.id);
    assert.ok(j, c.id);
    assert.match(j.formula, /^\[ORDERS\/[^\]/]+\]$/);
  }
  assert.equal(joined.columns.find(c => c.id === 'orders__o_orderstatus').formula, '[ORDERS/O Orderstatus]');
  assert.equal(joined.columns.find(c => c.id === 'orders__order_month').formula, '[ORDERS/Order Month]');
  assert.equal(joined.relationships, undefined);
  const f = xref(r, 'CROSS_TABLE_REFERENCES_MOVED');
  assert.equal(f.length, 1);
  assert.equal(f[0].source, 'embed');
  assert.equal(f[0].severity, 'info');
  assert.match(f[0].message, /Customer Name/);
  assert.match(f[0].message, /orders__joined/);
  // elements are not otherwise disturbed
  assert.equal(els(r).length, els(S.convertSemanticView(ddl('02-star-schema'), STAR_ENGINE).results[0]).length + 1);
});

test("crossTableReferences: 'engine' equals the golden byte-for-byte and adds no findings", () => {
  const r = S.convertSemanticView(ddl('02-star-schema'), STAR_ENGINE).results[0];
  assert.equal(r.spec + '\n', golden('02-star-schema'));
  assert.equal(xref(r, 'CROSS_TABLE_REFERENCES_MOVED').length, 0);
});

test("crossTableReferences: goldens without cross-table refs are identical in default and 'engine' mode", () => {
  for (const n of ['01-single-table', '05-role-playing-alias', '08-derived-metric', '09-prose-comments', '10-lowercase', '12-unqualified-names', '13-cortex-extension', '14-conditional-metrics', '15-unique-keys', '16-sample-values', '17-mapped-constructs']) {
    const a = S.convertSemanticView(ddl(n), BASE).results[0];
    const b = S.convertSemanticView(ddl(n), STAR_ENGINE).results[0];
    assert.equal(a.spec, b.spec, n);
    assert.equal(a.spec + '\n', golden(n), n);
    assert.equal(xref(a, 'CROSS_TABLE_REFERENCES_MOVED').length, 0, n);
  }
});

test('crossTableReferences: a dimension and a metric built on a cross-table dimension both move; metric lands in derived.metrics', () => {
  const d = ddl('02-star-schema')
    .replace('orders.order_month AS', 'orders.shout AS UPPER(orders.customer_name),\n    orders.order_month AS')
    .replace('lineitem.average_quantity AS AVG(l_quantity),', 'lineitem.average_quantity AS AVG(l_quantity),\n    orders.name_count AS COUNT(DISTINCT orders.customer_name),');
  const r = S.convertSemanticView(d, BASE).results[0];
  assert.equal(r.ok, true, JSON.stringify(r.errors));
  const orders = el(r, 'orders');
  const joined = el(r, 'orders__joined');
  assert.ok(!THREE_SEG.test(JSON.stringify(orders)));
  assert.equal(orders.metrics, undefined, 'moved metric removed from orders');
  assert.equal(joined.columns.find(c => c.id === 'orders__shout').formula, 'Upper([ORDERS/Orders To Customer/C Name])');
  assert.deepEqual(joined.metrics, [{ id: 'orders__m__name_count', formula: 'CountDistinct([Customer Name])', name: 'Name Count' }]);
  assert.equal(els(r).filter(e => e.id.startsWith('orders__joined')).length, 1, 'one derived element per base element');
  assert.match(xref(r, 'CROSS_TABLE_REFERENCES_MOVED')[0].message, /Shout.*Name Count|Customer Name.*Shout/s);
});

// Synthetic specs: the engine itself never emits a same-element ref to a moved column, so the transitive rule is driven directly.
const synth = () => ({
  schemaVersion: 1, kind: 'data-model',
  pages: [{ id: 'p', name: 'Page 1', elements: [
    { id: 'o', kind: 'table', source: { kind: 'warehouse-table', connectionId: 'c', path: ['D', 'S', 'O'] },
      columns: [
        { id: 'o__k', formula: '[O/K]' },
        { id: 'o__cn', formula: '[O/o__to__c/c__name]', name: 'Cust Name' },
        { id: 'o__dep', formula: 'Upper([Cust Name])', name: 'Dep' },
        { id: 'o__dep2', formula: 'Lower([Dep])', name: 'Dep Two' },
        { id: 'o__plain', formula: 'Upper([K])', name: 'Plain' },
      ],
      metrics: [{ id: 'o__m__x', formula: 'Count([Dep Two])', name: 'X' }, { id: 'o__m__y', formula: 'Count([K])', name: 'Y' }],
      relationships: [{ id: 'o__to__c', targetElementId: 'c', name: 'O To C', keys: [], relationshipType: 'N:1' }],
      order: ['o__k', 'o__cn', 'o__dep', 'o__dep2', 'o__plain'] },
    { id: 'c', kind: 'table', source: { kind: 'warehouse-table', connectionId: 'c', path: ['D', 'S', 'C'] },
      columns: [{ id: 'c__name', formula: '[C/C Name]' }], order: ['c__name'] },
  ] }],
});
const run = spec => S.applyCrossTableReferences(JSON.stringify(spec), 'V');

test('crossTableReferences: dependents of a moved column move transitively (columns and metrics); independents stay', () => {
  const out = run(synth());
  const es = JSON.parse(out.spec).pages[0].elements;
  const o = es.find(e => e.id === 'o');
  const j = es.find(e => e.id === 'o__joined');
  assert.deepEqual(o.columns.map(c => c.id), ['o__k', 'o__plain']);
  assert.deepEqual(o.order, ['o__k', 'o__plain']);
  assert.deepEqual(o.metrics.map(m => m.id), ['o__m__y']);
  assert.deepEqual(j.metrics.map(m => m.id), ['o__m__x']);
  assert.deepEqual(j.columns.map(c => c.id), ['o__k', 'o__cn', 'o__dep', 'o__dep2', 'o__plain']);
  assert.equal(j.columns[1].formula, '[O/O To C/C Name]');
  assert.equal(j.columns[2].formula, 'Upper([Cust Name])', 'moved dependent formula unchanged');
  assert.equal(j.columns[4].formula, '[O/Plain]', 'non-moved calc column becomes a 2-part passthrough');
  assert.equal(out.findings.filter(f => f.code === 'CROSS_TABLE_REFERENCES_MOVED').length, 1);
});

test('crossTableReferences: an unresolvable reference stays in place with a CROSS_TABLE_REFERENCE_UNRESOLVED warning and no derived element', () => {
  const s = synth();
  s.pages[0].elements[1].columns = []; // target column missing
  const out = run(s);
  const es = JSON.parse(out.spec).pages[0].elements;
  assert.equal(es.length, 2);
  assert.ok(es[0].columns.some(c => c.id === 'o__cn' && c.formula === '[O/o__to__c/c__name]'));
  const f = out.findings.filter(x => x.code === 'CROSS_TABLE_REFERENCE_UNRESOLVED');
  assert.equal(f.length, 1);
  assert.equal(f[0].severity, 'warning');
  assert.equal(f[0].source, 'embed');
  assert.equal(out.findings.filter(x => x.code === 'CROSS_TABLE_REFERENCES_MOVED').length, 0);
  // unnamed relationship is also unresolvable
  const s2 = synth();
  delete s2.pages[0].elements[0].relationships[0].name;
  assert.equal(run(s2).findings[0].code, 'CROSS_TABLE_REFERENCE_UNRESOLVED');
});

test('crossTableReferences: derived id is made unique when taken', () => {
  const s = synth();
  s.pages[0].elements.push({ id: 'o__joined', kind: 'table', source: { kind: 'table', elementId: 'c' }, columns: [] });
  const es = JSON.parse(run(s).spec).pages[0].elements;
  assert.ok(es.some(e => e.id === 'o__joined_2' && e.source.elementId === 'o'));
});

test('crossTableReferences: target column reuses its explicit name when set', () => {
  const s = synth();
  s.pages[0].elements[1].columns[0] = { id: 'c__name', formula: '[C/C Name]', name: 'Client' };
  const j = JSON.parse(run(s).spec).pages[0].elements.find(e => e.id === 'o__joined');
  assert.equal(j.columns.find(c => c.id === 'o__cn').formula, '[O/O To C/Client]');
});


const metricSynth = (colName) => {
  const s = synth();
  const o = s.pages[0].elements[0];
  o.columns = [{ id: 'o__k', formula: '[O/K]' }];
  if (colName !== undefined) o.columns.push({ id: 'o__cn', formula: '[O/o__to__c/c__name]', name: colName });
  o.order = o.columns.map(c => c.id);
  o.metrics = [{ id: 'o__m__n', formula: 'CountDistinct([O/o__to__c/c__name])', name: 'N' }];
  return s;
};
const joinedOf = out => JSON.parse(out.spec).pages[0].elements.find(e => e.id === 'o__joined');

test('crossTableReferences: metric over a related column with no matching moved column gets a helper column and references it by name', () => {
  const out = run(metricSynth());
  const j = joinedOf(out);
  const h = j.columns.find(c => c.id === 'o__joined__xref__o_to_c__c_name');
  assert.deepEqual(h, { id: 'o__joined__xref__o_to_c__c_name', formula: '[O/O To C/C Name]', name: 'C Name' });
  assert.equal(j.metrics[0].formula, 'CountDistinct([C Name])');
  assert.ok(j.order.includes(h.id));
  assert.match(out.findings.find(f => f.code === 'CROSS_TABLE_REFERENCES_MOVED').message, /Helper column.*C Name/);
});

test('crossTableReferences: helper column name is suffixed with the relationship name when it collides', () => {
  const s = metricSynth();
  s.pages[0].elements[0].columns.push({ id: 'o__other', formula: 'Upper([K])', name: 'C Name' });
  const j = joinedOf(run(s));
  assert.equal(j.columns.find(c => c.id.includes('__xref__')).name, 'C Name (O To C)');
  assert.equal(j.metrics[0].formula, 'CountDistinct([C Name (O To C)])');
});

test('crossTableReferences: an existing moved column with that exact cross-ref formula is reused (no helper)', () => {
  const j = joinedOf(run(metricSynth('Cust Name')));
  assert.equal(j.metrics[0].formula, 'CountDistinct([Cust Name])');
  assert.ok(!j.columns.some(c => c.id.includes('__xref__')));
});

test('crossTableReferences: a metric with an unrewritable 3-segment ref stays on the base element with UNRESOLVED', () => {
  const s = metricSynth('Cust Name');
  s.pages[0].elements[0].metrics[0].formula = 'CountDistinct([O/o__to__c/c__name]) + Count([X/not_a_rel/Y])';
  const out = run(s);
  const es = JSON.parse(out.spec).pages[0].elements;
  assert.deepEqual(es.find(e => e.id === 'o').metrics.map(m => m.id), ['o__m__n']);
  assert.equal(es.find(e => e.id === 'o__joined').metrics, undefined);
  assert.equal(out.findings.filter(f => f.code === 'CROSS_TABLE_REFERENCE_UNRESOLVED').length, 1);
});

test('crossTableReferences: no metric anywhere contains a 3-segment ref in default output (all upstream fixtures)', () => {
  let n = 0, sawJoinedMetric = false;
  for (const f of readdirSync(join(FIX, 'fixtures')).filter(x => x.endsWith('.sql'))) {
    let text = readFileSync(join(FIX, 'fixtures', f), 'utf8');
    if (f === '02-star-schema.sql') text = text.replace('lineitem.average_quantity AS AVG(l_quantity),', 'lineitem.average_quantity AS AVG(l_quantity),\n    orders.name_count AS COUNT(DISTINCT orders.customer_name),'); // force a cross-table metric
    for (const r of S.convertSemanticView(text, BASE).results) {
      if (!r.ok) continue;
      for (const e of JSON.parse(r.spec).pages[0].elements) for (const m of e.metrics ?? []) { n++; if (e.id.endsWith('__joined')) sawJoinedMetric = true; assert.ok(!THREE_SEG.test(m.formula), `${f}: ${m.formula}`); }
    }
  }
  assert.ok(n > 0);
  assert.ok(sawJoinedMetric, 'a metric was actually moved to a derived element');
});

test('YAML findings carry no placeholder line (upstream YAML has no line tracking)', () => {
  const r = S.convertSemanticView(PLUGS, { connectionId: 'c1' });
  const fs = r.results.flatMap(x => x.findings);
  assert.ok(fs.length > 0);
  for (const f of fs) {
    assert.equal(f.line, undefined, `${f.code} kept a line`);
    assert.ok(!/ at line \d/.test(f.message), `${f.code} message still says "at line": ${f.message}`);
  }
});

test('DDL findings keep their real lines; embed-layer findings never carry one', () => {
  const r = S.convertSemanticView(ddl('02-star-schema'), { connectionId: 'c1', database: 'ANALYTICS', schema: 'MODELS' });
  const fs = r.results.flatMap(x => x.findings);
  const upstream = fs.filter(f => f.source !== 'embed');
  assert.ok(upstream.some(f => typeof f.line === 'number' && f.line > 1), 'some DDL finding has a real line > 1');
  for (const f of fs.filter(f => f.source === 'embed')) assert.equal(f.line, undefined, f.code);
});

test('YAML_PARSE_FAILURE keeps its real line', () => {
  const r = S.convertSemanticView('name: X\ntables:\n  - name: A\n    base_table: [unclosed\n', { connectionId: 'c1' });
  const f = r.results[0].findings.find(x => x.code === 'YAML_PARSE_FAILURE');
  assert.ok(f, r.results[0].findings.map(x => x.code).join(','));
  assert.equal(typeof f.line, 'number');
});

// ── Evidence-based number formats (semantic-tools-embed/src/formats.ts) ──

const TPCH = readFileSync(join(dirname(fileURLToPath(import.meta.url)), 'fixtures', 'tpch.yaml'), 'utf8');
const TPCH_OPTS = { connectionId: BASE.connectionId, crossTableReferences: 'engine' };
const fmtRun = (src, extra = {}) => S.convertSemanticView(src, { ...TPCH_OPTS, inferNumberFormats: true, ...extra }).results[0];
const fmtOf = (r, name) => els(r).flatMap(e => [...e.columns, ...(e.metrics ?? [])]).find(x => x.name === name)?.format;
const CUR = { kind: 'number', formatString: '$,.2f', currencySymbol: '$' };
const INT = { kind: 'number', formatString: ',.0f' };
const PCT = { kind: 'number', formatString: ',.2%' };
const PP = { kind: 'number', formatString: ',.2f', suffix: '%' };
const withSamples = (src, col, vals) => src.replace(new RegExp(`(- name: ${col}\\n        expr: \\w+\\n        data_type: [^\\n]+\\n)`), `$1        sample_values: [${vals.join(', ')}]\n`);

test('formats: TPCH - Average Discount gets NO format without samples (was currency), Total Revenue currency, Order Count integer', () => {
  const r = fmtRun(TPCH);
  assert.equal(fmtOf(r, 'Average Discount'), undefined);
  assert.deepEqual(fmtOf(r, 'Total Revenue'), CUR);
  assert.deepEqual(fmtOf(r, 'Net Price'), CUR);
  assert.deepEqual(fmtOf(r, 'Order Count'), INT);
  const f = r.findings.find(x => x.code === 'FORMATS_NOT_INFERRED');
  assert.match(f.message, /Average Discount/);
  assert.match(f.remediation, /sample_values/);
  assert.equal(f.source, 'embed');
  assert.equal(f.severity, 'info');
  assert.equal(f.line, undefined);
  const g = r.findings.find(x => x.code === 'FORMATS_INFERRED');
  assert.match(g.message, /Total Revenue: currency — Sum of Extended Price × \(1 − Discount\)/);
});

test('formats: TPCH with sample_values [0.04, 0.06] on Discount - Average Discount is percent and Total Revenue stays currency', () => {
  const src = withSamples(TPCH, 'DISCOUNT', ['0.04', '0.06']);
  assert.notEqual(src, TPCH);
  const r = fmtRun(src);
  assert.deepEqual(fmtOf(r, 'Average Discount'), PCT);
  assert.deepEqual(fmtOf(r, 'Total Revenue'), CUR);
  assert.ok(!r.findings.some(x => x.code === 'FORMATS_NOT_INFERRED'));
});

test('formats: samples in (1,100] on an ambiguous-word column (discount) stay plain -> no format', () => {
  const src = withSamples(TPCH, 'DISCOUNT', ['4', '6']);
  assert.equal(fmtOf(fmtRun(src), 'Average Discount'), undefined);
});

test('formats: opt-in off (absent or false) is byte-identical to before; no FORMATS_* findings', () => {
  const a = S.convertSemanticView(TPCH, TPCH_OPTS).results[0];
  const b = S.convertSemanticView(TPCH, { ...TPCH_OPTS, inferNumberFormats: false }).results[0];
  assert.equal(a.spec, b.spec);
  assert.equal(a.spec, S.rawConvert(TPCH, 'snowflake-semantic-view-yaml', { ...BASE, columnTypes: {}, database: undefined, schema: undefined, connectionId: BASE.connectionId }).spec);
  assert.ok(![...a.findings, ...b.findings].some(x => /^FORMATS_/.test(x.code)));
  assert.ok(!els(a).some(e => [...e.columns, ...(e.metrics ?? [])].some(c => c.format)));
});

test('formats: only metrics and calculated columns are formatted (passthroughs never)', () => {
  const r = fmtRun(TPCH);
  for (const e of els(r)) for (const c of e.columns) if (/^\[[^\]/]+\/[^\]/]+\]$/.test(c.formula)) assert.equal(c.format, undefined, c.id);
});

test('formats: text and id-named integers never get a format', () => {
  const y = `name: V
tables:
  - name: T
    base_table: { database: D, schema: S, table: T }
    dimensions:
      - { name: SEG, expr: SEG, data_type: VARCHAR(10) }
      - { name: ORDER_NUMBER, expr: ORDER_NUMBER, data_type: "NUMBER(38,0)" }
      - { name: CUST_KEY, expr: CUST_KEY, data_type: "NUMBER(38,0)" }
    facts:
      - { name: LINE_COUNT, expr: LINE_COUNT, data_type: "NUMBER(38,0)" }
    metrics:
      - { name: SEG_MAX, expr: MAX(SEG) }
      - { name: ORDER_NO_TOTAL, expr: SUM(ORDER_NUMBER) }
      - { name: KEY_TOTAL, expr: SUM(CUST_KEY) }
      - { name: LINES, expr: SUM(LINE_COUNT) }
`;
  const r = fmtRun(y);
  assert.equal(r.ok, true);
  assert.equal(fmtOf(r, 'Seg Max'), undefined);
  assert.equal(fmtOf(r, 'Order No Total'), undefined);
  assert.equal(fmtOf(r, 'Key Total'), undefined);
  assert.deepEqual(fmtOf(r, 'Lines'), INT);
});

test('formats: DDL sample_values are read (fixture 16 style) and drive the unit', () => {
  const d = `create or replace semantic view V
  tables ( DB.S.ORDERS )
  facts ( ORDERS.DISCOUNT as DISCOUNT sample_values ('0.04', '0.06'), ORDERS.AMT2 as AMT2 sample_values ('5', '12') )
  metrics ( ORDERS.AVG_DISC as AVG(DISCOUNT), ORDERS.AVG_AMT2 as AVG(AMT2) );`;
  const r = fmtRun(d, {});
  assert.equal(r.ok, true, JSON.stringify(r.findings));
  assert.deepEqual(fmtOf(r, 'Avg Disc'), PCT);
  const none = fmtRun(d.replace("sample_values ('0.04', '0.06')", ''));
  assert.equal(fmtOf(none, 'Avg Disc'), undefined);
});

test('formats: columnTypes supplies types when the source has none (DDL)', () => {
  const d = `create or replace semantic view V
  tables ( DB.S.ORDERS )
  facts ( ORDERS.QTY as QTY, ORDERS.TOT_COST as TOT_COST )
  metrics ( ORDERS.TOT as SUM(TOT_COST), ORDERS.Q as SUM(QTY) );`;
  assert.deepEqual(fmtOf(fmtRun(d), 'Tot'), CUR); // name word "cost"
  const typed = fmtRun(d, { columnTypes: { 'DB.S.ORDERS': { QTY: 'NUMBER(12,2)', TOT_COST: 'VARCHAR' } } });
  assert.equal(fmtOf(typed, 'Tot'), undefined, 'a VARCHAR column is never a currency');
  assert.equal(fmtOf(typed, 'Q'), undefined, 'decimal quantity is plain');
});

test('formats: classifyBase rules', () => {
  const c = (names, dataType, samples) => S.classifyBase({ names, ...(dataType && { dataType }), ...(samples && { samples }) }).u;
  assert.equal(c(['Extended Price'], 'NUMBER(12,2)'), 'currency');
  assert.equal(c(['L_EXTENDEDPRICE'], 'FLOAT'), 'currency');
  assert.equal(c(['O_TOTALPRICE']), 'currency');
  assert.equal(c(['Total Price'], 'NUMBER(38,0)'), 'integer', 'integer type wins over a currency word');
  assert.equal(c(['Discount'], 'NUMBER(12,2)'), 'plain');
  assert.equal(c(['Discount']), 'unknown');
  assert.equal(c(['Discount'], 'NUMBER(12,2)', ['0.04', '0.06']), 'fraction');
  assert.equal(c(['Discount'], 'NUMBER(12,2)', ['-0.5', '1']), 'fraction');
  assert.equal(c(['Discount'], 'NUMBER(12,2)', ['4', '6']), 'plain');
  assert.equal(c(['Win Rate'], 'FLOAT', ['0.2']), 'fraction');
  assert.equal(c(['Win Rate'], 'FLOAT', ['20', '30']), 'plain', 'bare "rate" with samples > 1 is not percent points');
  assert.equal(c(['Win Pct'], 'FLOAT', ['20', '30']), 'percentpoints');
  assert.equal(c(['Win Percent'], 'FLOAT', ['20', '30']), 'percentpoints');
  assert.equal(c(['Win Percentage'], 'FLOAT', ['20', '30']), 'percentpoints');
  assert.equal(c(['Win Share'], 'FLOAT', ['20', '30']), 'plain');
  assert.equal(c(['hourly_rate'], 'NUMBER(10,2)', ['15.00', '25.50', '45.00']), 'plain', 'hourly rate is a price, not a percent');
  assert.equal(c(['shipping_rate'], 'NUMBER(10,2)', ['4.99', '9.99']), 'plain');
  assert.equal(c(['shipping_fee_rate'], 'NUMBER(10,2)', ['4.99', '9.99']), 'currency', 'rate beside a currency word');
  assert.equal(c(['price_rate'], 'NUMBER(10,2)', ['4.99', '9.99']), 'currency');
  assert.equal(c(['hourly_rate'], 'NUMBER(10,2)'), 'plain');
  assert.equal(c(['Win Rate'], 'FLOAT', ['0.2']), 'fraction', 'rate with 0-1 samples stays a fraction');
  assert.equal(c(['Gross Margin Pct'], 'FLOAT', ['12.5', '-3']), 'percentpoints');
  assert.equal(c(['Discount Pct'], 'FLOAT', ['4', '6']), 'percentpoints', 'a percent word beside an ambiguous word');
  assert.equal(c(['Margin'], 'FLOAT', ['12.5', '30']), 'plain', 'ambiguous word alone stays plain');
  assert.equal(c(['Win Rate'], 'FLOAT', ['20', '300']), 'plain', 'samples above 100 are not percent points');
  assert.equal(c(['Conversion Ratio'], 'FLOAT', ['20', '30']), 'plain', 'ratio is not a percent-points word');
  assert.equal(c(['Discount Amount'], 'NUMBER(12,2)', ['5', '40']), 'currency');
  assert.equal(c(['Discount Amount'], 'NUMBER(12,2)'), 'currency');
  assert.equal(c(['Discount Amount'], 'NUMBER(12,2)', ['0.1', '0.5']), 'plain');
  assert.equal(c(['Order Count']), 'integer');
  assert.equal(c(['Quantity'], 'NUMBER(12,2)'), 'plain');
  assert.equal(c(['Quantity'], 'NUMBER(12,2)', ['1', '2']), 'integer');
  assert.equal(c(['Customer Count', 'x'], 'FLOAT'), 'plain');
  assert.equal(c(['Order Number'], 'NUMBER(38,0)'), 'id');
  assert.equal(c(['Order Number']), 'id');
  assert.equal(c(['Order Number'], 'NUMBER(10,2)'), 'plain');
  assert.equal(c(['Number of Orders']), 'integer');
  assert.equal(c(['L_ORDERKEY'], 'NUMBER(38,0)'), 'id');
  assert.equal(c(['Zip Code'], 'NUMBER(5,0)'), 'id');
  assert.equal(c(['Ship Year'], 'NUMBER(4,0)'), 'id');
  assert.equal(c(['Customer ID']), 'id');
  assert.equal(c(['Paid'], 'FLOAT'), 'plain', '"paid" is not the word id');
  assert.equal(c(['Seg'], 'VARCHAR(10)'), 'nonnumeric');
  assert.equal(c(['Price'], 'DATE'), 'nonnumeric');
  assert.equal(c(['Price'], undefined, ['a', 'b']), 'nonnumeric');
  assert.equal(c(['Mystery']), 'unknown');
  assert.equal(c(['Mystery'], 'FLOAT'), 'plain');
  assert.equal(c(['Revenue', 'Units']), 'unknown');
});

test('formats: typeKind', () => {
  const k = S.typeKind;
  assert.deepEqual(['NUMBER(38,0)', 'INT', 'BIGINT', 'DECIMAL(10)', 'number(5, 0)'].map(k), ['int', 'int', 'int', 'int', 'int']);
  assert.deepEqual(['NUMBER(12,2)', 'FLOAT', 'DOUBLE', 'REAL', 'NUMERIC(9,3)'].map(k), ['dec', 'dec', 'dec', 'dec', 'dec']);
  assert.deepEqual(['NUMBER', 'FIXED'].map(k), ['num', 'num']);
  assert.deepEqual(['VARCHAR(10)', 'DATE', 'TIMESTAMP_NTZ', 'BOOLEAN'].map(k), ['other', 'other', 'other', 'other']);
  assert.equal(k(undefined), undefined);
});

const U = (f, refs = {}) => S.unitOfFormula(f, refs).u;
const R = { Pp: 'percentpoints', Pp2: 'percentpoints', Cur: 'currency', Cur2: 'currency', Int: 'integer', Int2: 'integer', Pl: 'plain', Pl2: 'plain', Fr: 'fraction', Id: 'id', Txt: 'nonnumeric', Unk: 'unknown' };

test('formats: propagation - counts and same-unit aggregates', () => {
  for (const f of ['Count([Cur])', 'CountDistinct([Txt])', 'CountIf([Cur] > 1)', 'CountDistinctIf([Id], [Cur] > 1)']) assert.equal(U(f, R), 'integer', f);
  for (const f of ['Sum', 'Min', 'Max', 'First', 'Last']) { assert.equal(U(`${f}([Cur])`, R), 'currency', f); assert.equal(U(`${f}([Int])`, R), 'integer', f); }
  assert.equal(U('Avg([Cur])', R), 'currency');
  assert.equal(U('Avg([Fr])', R), 'fraction');
  assert.equal(U('Avg([Int])', R), 'plain');
  assert.equal(U('Median([Int])', R), 'plain');
  assert.equal(U('Median([Cur])', R), 'currency');
  for (const f of ['SumIf', 'MinIf', 'MaxIf']) assert.equal(U(`${f}([Cur], [Int] > 1)`, R), 'currency', f);
  assert.equal(U('AvgIf([Int], [Int] > 1)', R), 'plain');
  assert.equal(U('Sum([Id])', R), 'unknown');
  assert.equal(U('Sum([Txt])', R), 'unknown');
  assert.equal(U('Sum(1)', R), 'integer');
});

test('formats: propagation - percentpoints', () => {
  assert.equal(U('[Pp] + [Pp2]', R), 'percentpoints');
  assert.equal(U('[Pp] - [Pp2]', R), 'percentpoints');
  assert.equal(U('[Pp] + 5', R), 'unknown');
  for (const f of ['Avg', 'Min', 'Max', 'Median']) assert.equal(U(`${f}([Pp])`, R), 'percentpoints', f);
  assert.equal(U('Round([Pp], 1)', R), 'percentpoints');
  assert.equal(U('Abs([Pp])', R), 'percentpoints');
  assert.equal(U('Coalesce([Pp], [Pp2])', R), 'percentpoints');
  assert.equal(U('Sum([Pp])', R), 'unknown', 'summing percentages is meaningless');
  assert.equal(U('SumIf([Pp], [Int] > 1)', R), 'unknown');
  assert.equal(U('Round(100 * (Sum([Cur]) / Sum([Cur2])), 1)', R), 'percentpoints');
});

test('formats: Sum of a percent-points column gets no format; percent points emit the alteryx/oac shape', () => {
  const y = `name: V
tables:
  - name: T
    base_table: { database: D, schema: S, table: T }
    facts:
      - { name: WIN_PCT, expr: WIN_PCT, data_type: "FLOAT", sample_values: [20, 35.5] }
      - { name: AMOUNT, expr: AMOUNT, data_type: "NUMBER(12,2)" }
      - { name: COST, expr: COST, data_type: "NUMBER(12,2)" }
    metrics:
      - { name: AVG_WIN, expr: AVG(WIN_PCT) }
      - { name: SUM_WIN, expr: SUM(WIN_PCT) }
      - { name: MARGIN_PTS, expr: 100 * (SUM(AMOUNT) - SUM(COST)) / SUM(AMOUNT) }
`;
  const r = fmtRun(y);
  assert.deepEqual(fmtOf(r, 'Avg Win'), PP);
  assert.equal(fmtOf(r, 'Sum Win'), undefined);
  const f = r.findings.find(x => x.code === 'FORMATS_INFERRED');
  assert.match(f.message, /percent points —/);
});

test('formats: propagation - Round/Abs/Coalesce/Zn/If/Switch', () => {
  assert.equal(U('Round([Cur], 2)', R), 'currency');
  assert.equal(U('Abs([Fr])', R), 'fraction');
  assert.equal(U('Zn([Int])', R), 'integer');
  assert.equal(U('Coalesce([Cur], [Cur2])', R), 'currency');
  assert.equal(U('Coalesce([Cur], [Int])', R), 'unknown');
  assert.equal(U('Coalesce([Cur], 0)', R), 'currency');
  assert.equal(U('If([Int] > 1, [Cur], [Cur2])', R), 'currency');
  assert.equal(U('If([Int] > 1, [Cur], 0)', R), 'currency');
  assert.equal(U('If([Int] > 1, [Cur], [Int])', R), 'unknown');
  assert.equal(U('If([Int] > 1, [Cur], [Int] > 2, [Cur2], null)', R), 'currency');
  assert.equal(U('Switch([Txt], "a", [Cur], "b", [Cur2], [Cur])', R), 'currency');
  assert.equal(U('Switch([Txt], "a", [Cur], "b", [Int])', R), 'unknown');
  assert.equal(U('NullIf([Cur], 0)', R), 'currency');
});

test('formats: propagation - +/- and literals', () => {
  assert.equal(U('2', R), 'scalar');
  assert.equal(U('[Cur] + [Cur2]', R), 'currency');
  assert.equal(U('[Cur] - [Cur2]', R), 'currency');
  assert.equal(U('[Int] + [Int2]', R), 'integer');
  assert.equal(U('1 - [Fr]', R), 'fraction');
  assert.equal(U('1 - [Pl]', R), 'plain');
  assert.equal(U('[Cur] + [Int]', R), 'unknown');
  assert.equal(U('[Cur] + 5', R), 'unknown');
  assert.equal(U('[Cur] + 0', R), 'currency');
  assert.equal(U('-[Cur]', R), 'currency');
  assert.equal(U('1 - [Unk]', R), 'unknown');
  assert.equal(U('[Cur] + [Id]', R), 'unknown');
});

test('formats: propagation - * and /', () => {
  assert.equal(U('[Cur] * [Int]', R), 'currency');
  assert.equal(U('[Cur] * [Pl]', R), 'currency');
  assert.equal(U('[Cur] * [Fr]', R), 'currency');
  assert.equal(U('[Cur] * 2', R), 'currency');
  assert.equal(U('[Cur] * [Cur2]', R), 'unknown');
  assert.equal(U('[Int] * [Int2]', R), 'integer');
  assert.equal(U('2 * [Int]', R), 'integer');
  assert.equal(U('[Fr] * 0.5', R), 'fraction');
  assert.equal(U('100 * [Fr]', R), 'percentpoints');
  assert.equal(U('[Fr] * 100', R), 'percentpoints');
  assert.equal(U('100 * [Pp]', R), 'unknown');
  assert.equal(U('[Pp] * 2', R), 'unknown');
  assert.equal(U('[Pp] / 2', R), 'unknown');
  assert.equal(U('[Pp] * [Pp2]', R), 'unknown');
  assert.equal(U('[Pl] * [Pl2]', R), 'unknown');
  assert.equal(U('[Cur] / [Cur2]', R), 'fraction');
  assert.equal(U('[Int] / [Int2]', R), 'plain', 'integer / integer is only a ratio when the item is named like one');
  assert.equal(U('[Pl] / [Pl2]', R), 'plain');
  for (const nm of ['Win Rate', 'Order Pct', 'Pct Of Orders', 'Mix Percent', 'Fill Percentage', 'Unit Ratio', 'Customer Share']) {
    assert.equal(S.unitOfFormula('[Int] / [Int2]', R, nm).u, 'fraction', nm);
    assert.equal(S.unitOfFormula('[Pl] / [Pl2]', R, nm).u, 'fraction', nm);
  }
  assert.equal(S.unitOfFormula('[Int] / [Int2]', R, 'Units per Order').u, 'plain');
  assert.equal(S.unitOfFormula('[Cur] / [Cur2]', R, 'Units per Order').u, 'fraction', 'currency / currency is always a ratio');
  assert.equal(U('[Cur] / [Int]', R), 'currency');
  assert.equal(U('[Cur] / [Pl]', R), 'currency');
  assert.equal(U('[Cur] / 12', R), 'currency');
  assert.equal(U('[Int] / 2', R), 'integer');
  assert.equal(U('[Int] / [Cur]', R), 'unknown');
  assert.equal(U('100 * (Sum([Cur]) / Sum([Cur2]))', R), 'percentpoints');
  assert.equal(U('Sum([Cur]) / Sum([Cur2]) * 100', R), 'percentpoints');
  assert.equal(U('CallNumber("DIV0NULL", Count([Cur]), Count([Cur2]))', R), 'plain');
  assert.equal(S.unitOfFormula('CallNumber("DIV0NULL", Count([Cur]), Count([Cur2]))', R, 'Hit Rate').u, 'fraction');
  assert.equal(U('CallNumber("DIV0NULL", Sum([Cur]), Sum([Cur2]))', R), 'fraction');
});

test('formats: propagation - parsing, precedence, comparisons, unparseable => unknown', () => {
  assert.equal(U('[Cur] + [Cur2] * 2', R), 'currency');
  assert.equal(U('([Cur] + [Cur2]) * 2', R), 'currency');
  assert.equal(U('[Cur] > 1', R), 'unknown');
  assert.equal(U('[Cur] and [Int]', R), 'unknown');
  assert.equal(U('Sum([Cur]', R), 'unknown');
  assert.equal(U('Sum([Cur]) @ 2', R), 'unknown');
  assert.equal(U('Foo([Cur])', R), 'unknown');
  assert.equal(U('"a" & "b"', R), 'unknown');
  assert.equal(U('[Missing]', R), 'unknown');
  assert.equal(U('[A/Cur]', R), 'currency', 'a path falls back to its last segment');
});

const synthSpec = els => JSON.stringify({ pages: [{ id: 'p', elements: els }] });
const synthEl = (id, cols, metrics, rels) => ({ id, kind: 'table', source: { kind: 'warehouse-table', path: ['D', 'S', id.toUpperCase()] }, columns: cols, ...(metrics && { metrics }), ...(rels && { relationships: rels }) });

test('formats: refs - calculated columns chain, cycles => unknown, cross-table refs resolve through the target', () => {
  const facts = new Map([['o', [{ name: 'PRICE', phys: 'PRICE', dataType: 'NUMBER(12,2)' }, { name: 'N', phys: 'N', dataType: 'NUMBER(38,0)' }]], ['c', [{ name: 'BALANCE', phys: 'BALANCE', dataType: 'NUMBER(12,2)' }]]]);
  const spec = synthSpec([
    synthEl('o', [
      { id: 'o__price', formula: '[O/Price]' }, { id: 'o__n', formula: '[O/N]' },
      { id: 'o__a', formula: '[Price] * [N]', name: 'A' }, { id: 'o__b', formula: '[A] + [Price]', name: 'B' },
      { id: 'o__x', formula: '[Y] + 1', name: 'X' }, { id: 'o__y', formula: '[X] + 1', name: 'Y' },
      { id: 'o__cb', formula: '[O/o__to__c/c__balance]', name: 'Cust Bal' },
    ], [{ id: 'o__m', formula: 'Sum([B])', name: 'M' }, { id: 'o__m2', formula: 'Sum([Cust Bal])', name: 'M2' }, { id: 'o__m3', formula: 'Sum([O/o__to__c/c__balance])', name: 'M3' }],
    [{ id: 'o__to__c', targetElementId: 'c', keys: [] }]),
    synthEl('c', [{ id: 'c__balance', formula: '[C/Balance]' }]),
  ]);
  const out = S.inferFormats(spec, { facts, elementIdOf: a => a }, 'V');
  const by = n => out.items.find(i => i.name === n);
  assert.equal(by('A').unit, 'currency');
  assert.equal(by('B').unit, 'currency');
  assert.equal(by('M').unit, 'currency');
  assert.equal(by('X').unit, 'unknown');
  assert.equal(by('Y').unit, 'unknown');
  assert.equal(by('M3').unit, 'currency');
  assert.equal(by('M2').unit, 'currency'); // a column that is itself a cross-table ref resolves through the target
});

test('formats: FORMATS_INFERRED is capped at 12 + "and N more"', () => {
  const metrics = Array.from({ length: 15 }, (_, i) => `      - { name: M${i}, expr: "SUM(AMOUNT)" }`).join('\n');
  const y = `name: V
tables:
  - name: T
    base_table: { database: D, schema: S, table: T }
    facts:
      - { name: AMOUNT, expr: AMOUNT, data_type: "NUMBER(12,2)" }
    metrics:
${metrics}
`;
  const f = fmtRun(y).findings.find(x => x.code === 'FORMATS_INFERRED');
  assert.match(f.message, /and 3 more/);
  assert.equal((f.message.match(/currency —/g) ?? []).length, 12);
});

test('formats: percent format reuses the exact shape the legacy guesser emits for ratios', () => {
  const y = `name: V
tables:
  - name: T
    base_table: { database: D, schema: S, table: T }
    facts:
      - { name: AMOUNT, expr: AMOUNT, data_type: "NUMBER(12,2)" }
      - { name: TAX, expr: TAX, data_type: "NUMBER(12,2)" }
    metrics:
      - { name: TAX_SHARE, expr: SUM(TAX) / SUM(AMOUNT) }
      - { name: PER_ROW, expr: SUM(AMOUNT) / COUNT(*) }
`;
  const r = fmtRun(y);
  assert.deepEqual(fmtOf(r, 'Tax Share'), PCT);
});

// ── Derived element naming + description + relationship retargeting ──

test('derived element: name = table name as written + " (joined)"; description appended / synthesized', () => {
  const r = S.convertSemanticView(ddl('02-star-schema'), BASE).results[0];
  const joined = el(r, 'orders__joined');
  assert.equal(joined.name, 'ORDERS (joined)');
  assert.equal(joined.description, 'One row per order. Includes columns from related tables: Customer Name.');
  const m = xref(r, 'CROSS_TABLE_REFERENCES_MOVED')[0].message;
  assert.match(m, /"ORDERS \(joined\)"/);
  const y = S.convertSemanticView(TPCH, { connectionId: BASE.connectionId }).results[0];
  const j = el(y, 'orders__joined');
  assert.equal(j.name, 'ORDERS (joined)');
  assert.equal(j.description, 'One row per order. Includes columns from related tables: Customer Segment.');
});

test('derived element: description keeps terminal punctuation and an explicit element name wins', () => {
  const s = synth();
  s.pages[0].elements[0].description = 'Orders!';
  s.pages[0].elements[0].name = 'Order Facts';
  const j = JSON.parse(run(s).spec).pages[0].elements.find(e => e.id === 'o__joined');
  assert.equal(j.name, 'Order Facts (joined)');
  assert.match(j.description, /^Orders! Includes columns from related tables: /);
  const s2 = synth();
  s2.pages[0].elements[0].description = 'Orders';
  const j2 = JSON.parse(run(s2).spec).pages[0].elements.find(e => e.id === 'o__joined');
  assert.match(j2.description, /^Orders\. Includes columns from related tables: /);
  const j3 = JSON.parse(run(synth()).spec).pages[0].elements.find(e => e.id === 'o__joined');
  assert.equal(j3.name, 'O (joined)');
  assert.match(j3.description, /^O plus columns from related tables: Cust Name/);
});

test('relationships into the moved-from element are retargeted at the derived element (keys unchanged)', () => {
  const eng = S.convertSemanticView(ddl('02-star-schema'), STAR_ENGINE).results[0];
  const r = S.convertSemanticView(ddl('02-star-schema'), BASE).results[0];
  const before = el(eng, 'lineitem').relationships[0];
  const after = el(r, 'lineitem').relationships[0];
  assert.equal(before.targetElementId, 'orders');
  assert.equal(after.targetElementId, 'orders__joined');
  assert.deepEqual(after.keys, before.keys);
  // orders' OWN outgoing relationship is untouched
  assert.equal(el(r, 'orders').relationships[0].targetElementId, 'customer');
  assert.match(xref(r, 'CROSS_TABLE_REFERENCES_MOVED')[0].message, /relationship into ORDERS now points at ORDERS \(joined\) so related tables can reach these columns/);
});

test("'engine' mode never retargets", () => {
  const r = S.convertSemanticView(ddl('02-star-schema'), STAR_ENGINE).results[0];
  assert.equal(el(r, 'lineitem').relationships[0].targetElementId, 'orders');
});

test('a relationship whose key column is missing on the derived element is not retargeted and warns', () => {
  const s = synth();
  s.pages[0].elements.push({ id: 'l', kind: 'table', source: { kind: 'warehouse-table', connectionId: 'c', path: ['D', 'S', 'L'] },
    columns: [{ id: 'l__k', formula: '[L/K]' }],
    relationships: [
      { id: 'l__to__o', name: 'L To O', targetElementId: 'o', keys: [{ sourceColumnId: 'l__k', targetColumnId: 'o__k' }] },
      { id: 'l__to__o2', name: 'L To O2', targetElementId: 'o', keys: [{ sourceColumnId: 'l__k', targetColumnId: 'o__missing' }] },
    ] });
  const out = run(s);
  const l = JSON.parse(out.spec).pages[0].elements.find(e => e.id === 'l');
  assert.equal(l.relationships[0].targetElementId, 'o__joined');
  assert.equal(l.relationships[1].targetElementId, 'o');
  const w = out.findings.filter(f => f.code === 'CROSS_TABLE_REFERENCE_UNRESOLVED');
  assert.equal(w.length, 1);
  assert.equal(w[0].severity, 'warning');
  assert.equal(w[0].object.name, 'L To O2');
  assert.match(w[0].message, /o__missing/);
  assert.match(out.findings.find(f => f.code === 'CROSS_TABLE_REFERENCES_MOVED').message, /relationship into/);
});

test('formats: end to end - per-order averages and hourly/shipping rates get no wrong format (review #2/#3)', () => {
  const y = `name: V
tables:
  - name: LINES
    base_table: { database: D, schema: S, table: LINES }
    primary_key: { columns: [LINE_ID] }
    dimensions:
      - { name: line_id, expr: LINE_ID, data_type: "VARCHAR" }
      - { name: order_id, expr: ORDER_ID, data_type: "VARCHAR" }
    facts:
      - { name: quantity, expr: QUANTITY, data_type: "NUMBER(38,0)" }
      - { name: hourly_rate, expr: HOURLY_RATE, data_type: "NUMBER(10,2)", sample_values: ["15.00","25.50","45.00"] }
      - { name: shipping_rate, expr: SHIPPING_RATE, data_type: "NUMBER(10,2)", sample_values: ["4.99","9.99"] }
    metrics:
      - { name: units_per_order, expr: SUM(QUANTITY) / COUNT(DISTINCT ORDER_ID) }
      - { name: lines_per_order, expr: COUNT(LINE_ID) / COUNT(DISTINCT ORDER_ID) }
      - { name: line_share, expr: COUNT(LINE_ID) / COUNT(DISTINCT ORDER_ID) }
      - { name: avg_hourly_rate, expr: AVG(HOURLY_RATE) }
      - { name: avg_ship, expr: AVG(SHIPPING_RATE) }
`;
  const r = S.convertSemanticView(y, { connectionId: 'c', inferNumberFormats: true, columnTypes: {} }).results[0];
  assert.equal(r.ok, true);
  const fm = {};
  for (const e of JSON.parse(r.spec).pages[0].elements) for (const m of e.metrics ?? []) fm[m.name] = m.format;
  assert.equal(fm['Units Per Order'], undefined, JSON.stringify(fm));
  assert.equal(fm['Lines Per Order'], undefined);
  assert.deepEqual(fm['Line Share'], { kind: 'number', formatString: ',.2%' }, 'a percent-word name opts in');
  assert.equal(fm['Avg Hourly Rate'], undefined);
  assert.equal(fm['Avg Ship'], undefined);
});

// ── review #4: comment-aware masking for DDL synonyms / sample values ──

const colDesc = (r, id) => JSON.parse(r.spec).pages[0].elements.flatMap(e => e.columns ?? []).find(c => c.id === id)?.description;
const ddlSyn = body => `CREATE SEMANTIC VIEW D.S.V
  TABLES ( t AS D.S.T PRIMARY KEY (id) )
  FACTS ( ${body} )
  DIMENSIONS ( t.id AS id )
  METRICS ( t.total AS SUM(amt) );`;
const synOpts = { connectionId: 'c', synonymsInDescription: true, columnTypes: {} };

test('DDL comments: a commented-out WITH SYNONYMS / SAMPLE_VALUES is not read as real', () => {
  const d = ddlSyn(`t.amt AS amt -- old: WITH SYNONYMS ('bogus') SAMPLE_VALUES ('abc')
      COMMENT = 'amount'`);
  const r = S.convertSemanticView(d, synOpts).results[0];
  assert.equal(r.ok, true);
  assert.equal(colDesc(r, 't__amt'), 'amount', 'no "Also known as: bogus"');
  assert.ok(!r.spec.includes('bogus'));
  const facts = S.formatFacts(d, 'snowflake-semantic-view');
  assert.equal(facts.get('T').find(f => f.name === 'AMT').samples, undefined, 'commented SAMPLE_VALUES ignored');
  const block = ddlSyn(`t.amt AS amt /* WITH SYNONYMS ('bogus2') SAMPLE_VALUES ('9') */ COMMENT = 'amount'`);
  assert.ok(!S.convertSemanticView(block, synOpts).results[0].spec.includes('bogus2'));
  assert.equal(S.formatFacts(block, 'snowflake-semantic-view').get('T').find(f => f.name === 'AMT').samples, undefined);
});

test('DDL comments: an apostrophe in a -- or /* */ comment does not desynchronise string masking', () => {
  const d = ddlSyn(`t.amt AS amt -- don't mask me wrongly
      COMMENT = 'amount' WITH SYNONYMS = ('gross', 'it''s, a) test') /* it's fine */ SAMPLE_VALUES ('1', -- it's 2
        '2')`);
  const r = S.convertSemanticView(d, synOpts).results[0];
  assert.equal(r.ok, true);
  assert.equal(colDesc(r, 't__amt'), "amount. Also known as: gross, it's, a) test");
  assert.deepEqual(S.formatFacts(d, 'snowflake-semantic-view').get('T').find(f => f.name === 'AMT').samples, ['1', '2']);
});

test('DDL comments: comment markers inside strings and quoted identifiers are not comments; later views are unaffected', () => {
  const d = ddlSyn(`t.amt AS amt WITH SYNONYMS = ('a -- b', 'c /* d') SAMPLE_VALUES ('x--y')`);
  const r = S.convertSemanticView(d, synOpts).results[0];
  assert.equal(colDesc(r, 't__amt'), 'Also known as: a -- b, c /* d');
  assert.deepEqual(S.formatFacts(d, 'snowflake-semantic-view').get('T').find(f => f.name === 'AMT').samples, ['x--y']);
});

// ── review #5: placeholder lines are stripped from EVERY finding array exposed in results ──

const YAML_SEMI = `name: INV
tables:
  - name: inventory
    base_table: { database: D, schema: S, table: PARTSUPP }
    dimensions:
      - { name: ps_partkey, expr: ps_partkey }
    facts:
      - { name: ps_availqty, expr: ps_availqty }
    metrics:
      - name: closing_stock
        expr: SUM(ps_availqty)
        non_additive_dimensions: [ { dimension: ps_partkey } ]
`;

test('YAML: overriddenErrors / errors / warnings carry no placeholder line (consistent with findings)', () => {
  const strict = S.convertSemanticView(YAML_SEMI, BASE).results[0];
  assert.equal(strict.ok, false);
  const loose = S.convertSemanticView(YAML_SEMI, { ...BASE, bestEffort: true }).results[0];
  assert.equal(loose.ok, true);
  assert.deepEqual(loose.overriddenErrors.map(e => e.code), ['SEMI_ADDITIVE_METRIC']);
  for (const r of [strict, loose]) {
    for (const f of [...r.errors, ...r.warnings, ...r.overriddenErrors]) {
      assert.equal(f.line, undefined, `${f.code} kept line ${f.line}`);
      assert.ok(!('line' in f), `${f.code} still has a line key`);
    }
    for (const f of r.findings) assert.equal(f.line, undefined, f.code);
  }
});

test('DDL: overriddenErrors keep their real line (only unknown lines are stripped)', () => {
  const loose = S.convertSemanticView(ddl('03-non-additive-metric'), { ...BASE, bestEffort: true }).results[0];
  assert.ok(loose.overriddenErrors.every(e => typeof e.line === 'number' && e.line > 1), JSON.stringify(loose.overriddenErrors.map(e => e.line)));
});

// ── usingPaths: USING-pinned metrics bound to path-specific derived elements (best effort only) ──

const FIXD = join(dirname(fileURLToPath(import.meta.url)), 'fixtures');
const SV_DDL = readFileSync(join(FIXD, 'RETAIL_ANALYTICS_SV.sql'), 'utf8');
const SV_YAML = readFileSync(join(FIXD, 'RETAIL_ANALYTICS_SV.yaml'), 'utf8');
const SV = { connectionId: BASE.connectionId, database: 'CSA', schema: 'TJ', columnTypes: {} };
const svRun = (src, extra = {}) => S.convertSemanticView(src, { ...SV, bestEffort: true, ...extra }).results[0];
const VIA_NAMES = [
  'ORDER_FACT (via Order To Order Date)',
  'ORDER_FACT (via Order To Order Store)',
  'ORDER_FACT (via Order To Return Date)',
  'ORDER_FACT (via Order To Ship Date)',
  'ORDER_FACT (via Order To Ship Store)',
];
const PINNED_IDS = [
  'total_gross_revenue_by_order_store', 'total_gross_revenue_by_ship_store', 'order_line_count_by_order_store',
  'order_line_count_by_ship_store', 'total_gross_revenue_by_order_date', 'total_gross_revenue_by_ship_date',
  'total_gross_revenue_by_return_date', 'order_line_count_by_order_date', 'order_line_count_by_ship_date',
  'order_line_count_by_return_date',
].map(s => `order_fact__m__${s}`);
const viaOf = r => els(r).filter(e => /__via__/.test(e.id));

for (const [label, src] of [['DDL', SV_DDL], ['YAML', SV_YAML]]) {
  test(`usingPaths ${label}: best effort puts the 10 USING metrics on 5 path-bound via-elements`, () => {
    const r = svRun(src);
    assert.equal(r.ok, true);
    const via = viaOf(r);
    assert.deepEqual(via.map(e => e.name).sort(), VIA_NAMES);
    const moved = via.flatMap(e => (e.metrics ?? []).map(m => m.id)).sort();
    assert.deepEqual(moved, [...PINNED_IDS].sort());
    const base = els(r).find(e => e.id === 'order_fact');
    assert.equal(base.metrics.length, 11, 'unpinned metrics stay');
    assert.ok(!base.metrics.some(m => PINNED_IDS.includes(m.id)));
    for (const e of via) assert.deepEqual(e.source, { kind: 'table', elementId: 'order_fact' });
  });

  test(`usingPaths ${label}: metric refs resolve to local columns, no 3-segment metric refs, ids unique, description names the Snowflake path`, () => {
    const r = svRun(src);
    const all = els(r);
    for (const e of viaOf(r)) {
      const names = new Set(e.columns.map(c => c.name));
      for (const m of e.metrics) {
        assert.ok(!/\[[^\]/]+\/[^\]/]+\/[^\]]+\]/.test(m.formula), `${m.id} has a 3-segment ref`);
        for (const ref of m.formula.matchAll(/\[([^\]]+)\]/g)) assert.ok(names.has(ref[1]), `${m.id}: [${ref[1]}] is not a local column`);
      }
      assert.match(e.description, /^ORDER_FACT joined only through /);
      assert.match(e.description, /follow Snowflake's USING \(ORDER_TO_[A-Z_]+\) path/);
    }
    const ids = all.map(e => e.id);
    assert.equal(new Set(ids).size, ids.length, 'element ids unique');
    const viaIds = viaOf(r).flatMap(e => [...e.columns, ...e.metrics].map(x => x.id));
    assert.equal(new Set(viaIds).size, viaIds.length, 'via column/metric ids unique across via-elements');
    const baseIds = new Set(all.filter(e => !/__via__/.test(e.id)).flatMap(e => [...(e.columns ?? []), ...(e.metrics ?? [])].map(x => x.id)));
    assert.ok(viaIds.every(i => !baseIds.has(i)), 'via ids never collide with base ids');
  });

  test(`usingPaths ${label}: ship-date element joins only through Order To Ship Date (3-part refs by relationship name + target display name)`, () => {
    const r = svRun(src);
    const ship = els(r).find(e => e.name === 'ORDER_FACT (via Order To Ship Date)');
    assert.match(ship.id, /^order_fact__via__order_fact__to__date_dim(__\d)?$/);
    if (label === 'DDL') assert.equal(ship.id, 'order_fact__via__order_fact__to__date_dim__2');
    const month = ship.columns.find(c => c.formula === '[ORDER_FACT/Order To Ship Date/Month Name]');
    assert.ok(month, 'ship path Month Name');
    assert.equal(month.name, 'Month Name');
    assert.ok(!ship.columns.some(c => /Order To (Order|Return) Date/.test(c.formula)));
    assert.ok(ship.columns.some(c => c.formula === '[ORDER_FACT/Gross Revenue]' && c.name === 'Gross Revenue'));
    assert.deepEqual(ship.metrics.map(m => m.formula).sort(), ['Count()', 'Sum([Gross Revenue])']);
    assert.deepEqual(ship.metrics.map(m => m.id).sort(), ['order_fact__m__order_line_count_by_ship_date', 'order_fact__m__total_gross_revenue_by_ship_date']);
    assert.deepEqual(ship.order, ship.columns.map(c => c.id));
  });

  test(`usingPaths ${label}: USING_METRICS_PATH_BOUND info lists the move; METRIC_JOIN_PATH_SELECTOR stays reported`, () => {
    const r = svRun(src);
    const f = r.findings.filter(x => x.code === 'USING_METRICS_PATH_BOUND');
    assert.equal(f.length, 1);
    assert.equal(f[0].severity, 'info');
    assert.equal(f[0].source, 'embed');
    assert.match(f[0].message, /ORDER_LINE_COUNT_BY_SHIP_DATE/);
    assert.match(f[0].message, /ORDER_FACT \(via Order To Ship Date\)/);
    assert.equal(r.findings.filter(x => x.code === 'METRIC_JOIN_PATH_SELECTOR').length, 10);
    assert.equal(r.overriddenErrors.length, 10);
    assert.equal(r.findings.filter(x => x.code === 'USING_PATH_UNRESOLVED').length, 0);
  });

  test(`usingPaths ${label}: usingPaths:'engine' is the previous output byte-for-byte; strict mode is unchanged`, () => {
    const eng = svRun(src, { usingPaths: 'engine' });
    const fmt = src === SV_DDL ? 'snowflake-semantic-view' : 'snowflake-semantic-view-yaml';
    const raw = S.rawConvert(src, fmt, { connectionId: SV.connectionId, database: 'CSA', schema: 'TJ', columnTypes: {}, bestEffort: true });
    assert.equal(eng.spec, raw.spec);
    assert.equal(viaOf(eng).length, 0);
    assert.ok(!eng.findings.some(f => f.code === 'USING_METRICS_PATH_BOUND'));
    const strict = S.convertSemanticView(src, SV).results[0];
    assert.equal(strict.ok, false);
    assert.equal(strict.spec, undefined);
    assert.ok(!strict.findings.some(f => f.code === 'USING_METRICS_PATH_BOUND'));
  });
}

test('usingPaths: DDL and YAML of the same view bind the same metrics to the same paths (ids follow each source\'s relationship order)', () => {
  const shape = r => viaOf(r).map(e => ({ name: e.name, description: e.description, cols: e.columns.map(c => `${c.name}=${c.formula}`).sort(), metrics: e.metrics.map(m => `${m.id}=${m.formula}=${m.name}`).sort() })).sort((a, b) => a.name.localeCompare(b.name));
  assert.deepEqual(shape(svRun(SV_DDL)), shape(svRun(SV_YAML)));
});

test('usingPaths: a view-level YAML metric without an owner is still path-bound', () => {
  const doc = parseYaml(SV_YAML);
  const fact = doc.tables.find(t => t.name === 'ORDER_FACT');
  const index = fact.metrics.findIndex(m => m.name === 'ORDER_LINE_COUNT_BY_SHIP_DATE');
  doc.metrics = fact.metrics.splice(index, 1);
  doc.metrics[0].expr = 'COUNT(order_fact.order_key)';
  const r = svRun(stringifyYaml(doc));
  const metric = viaOf(r).flatMap(e => e.metrics ?? []).find(m => m.name === 'Order Line Count By Ship Date');
  assert.ok(metric);
  assert.equal(r.overriddenErrors.find(f => f.object.name === 'ORDER_LINE_COUNT_BY_SHIP_DATE').object.owner, undefined);
  assert.ok(!r.findings.some(f => f.code === 'USING_PATH_UNRESOLVED'));
});

test('usingPaths: a bestEffort run with no USING metrics is untouched (default option on)', () => {
  const r = S.convertSemanticView(ddl('03-non-additive-metric'), { ...BASE, bestEffort: true }).results[0];
  const off = S.convertSemanticView(ddl('03-non-additive-metric'), { ...BASE, bestEffort: true, usingPaths: 'engine' }).results[0];
  assert.equal(r.spec, off.spec);
  assert.ok(!r.findings.some(f => f.code === 'USING_METRICS_PATH_BOUND' || f.code === 'USING_PATH_UNRESOLVED'));
});

const MULTI_DDL = SV_DDL
  .replace('ORDER_FACT.SHIP_METHOD as SHIP_METHOD,', 'ORDER_FACT.SHIP_METHOD as SHIP_METHOD,\n\t\tORDER_FACT.CUST_REGION_X as CUSTOMER_DIM.CUST_REGION,')
  .replace('\t\tORDER_FACT.ORDER_LINE_COUNT_BY_RETURN_DATE using',
    '\t\tORDER_FACT.LINES_SHIP_AND_ORDER_DATE using (ORDER_TO_SHIP_DATE, ORDER_TO_ORDER_DATE) as COUNT(*),\n\t\tORDER_FACT.ORDER_LINE_COUNT_BY_RETURN_DATE using');

test('usingPaths: a multi-relationship USING set gets one element with sorted ids, joined names, and suffixed colliding target columns', () => {
  const r = svRun(MULTI_DDL);
  const e = els(r).find(x => x.name === 'ORDER_FACT (via Order To Order Date + Order To Ship Date)');
  assert.ok(e, 'multi element present');
  assert.equal(e.id, 'order_fact__via__order_fact__to__date_dim__order_fact__to__date_dim__2');
  assert.deepEqual(e.metrics.map(m => m.id), ['order_fact__m__lines_ship_and_order_date']);
  assert.match(e.description, /USING \(ORDER_TO_ORDER_DATE, ORDER_TO_SHIP_DATE\)/);
  assert.ok(e.columns.some(c => c.formula === '[ORDER_FACT/Order To Order Date/Month Name]' && c.name === 'Month Name'));
  assert.ok(e.columns.some(c => c.formula === '[ORDER_FACT/Order To Ship Date/Month Name]' && c.name === 'Month Name (Order To Ship Date)'));
  const names = e.columns.map(c => c.name);
  assert.equal(new Set(names.map(n => n.toLowerCase())).size, names.length, 'column names unique');
  assert.equal(viaOf(r).length, 6);
});

test('usingPaths composes with crossTableReferences: via-elements source the base, skip moved columns, base keeps joined', () => {
  const r = svRun(MULTI_DDL);
  const all = els(r);
  const ids = all.map(e => e.id);
  assert.ok(ids.includes('order_fact__joined'));
  assert.equal(new Set(ids).size, ids.length);
  const base = all.find(e => e.id === 'order_fact');
  assert.ok(!base.columns.some(c => c.id === 'order_fact__cust_region_x'), 'moved column left the base');
  for (const e of viaOf(r)) {
    assert.equal(e.source.elementId, 'order_fact');
    assert.ok(!e.columns.some(c => /Cust Region X/.test(c.formula)), 'no passthrough of a column the base no longer has');
    const baseNames = new Set(base.columns.map(c => c.name ?? /\/([^/\]]+)\]$/.exec(c.formula)[1]));
    for (const c of e.columns) {
      const m = /^\[ORDER_FACT\/([^/\]]+)\]$/.exec(c.formula);
      if (m) assert.ok(baseNames.has(m[1]), `${m[1]} exists on base`);
    }
  }
  assert.ok(r.findings.some(f => f.code === 'CROSS_TABLE_REFERENCES_MOVED'));
  assert.ok(r.findings.some(f => f.code === 'USING_METRICS_PATH_BOUND'));
  // elements stay adjacent: base, its joined, then its via-elements
  const i = ids.indexOf('order_fact');
  assert.equal(ids[i + 1], 'order_fact__joined');
  assert.match(ids[i + 2], /__via__/);
});

const USING_SPEC = () => JSON.stringify({
  schemaVersion: 1,
  pages: [{ id: 'p', name: 'P', elements: [
    { id: 'f', kind: 'table', source: { kind: 'warehouse-table', path: ['D', 'S', 'F'] },
      columns: [{ id: 'f__k', formula: '[F/K]' }, { id: 'f__amt', formula: '[F/Amt]' }],
      metrics: [{ id: 'f__m__a', formula: 'Sum([Amt])', name: 'A' }, { id: 'f__m__b', formula: 'Count()', name: 'B' }],
      relationships: [{ id: 'f__to__d', targetElementId: 'd', keys: [{ sourceColumnId: 'f__k', targetColumnId: 'd__k' }], name: 'F To D' }] },
    { id: 'd', kind: 'table', source: { kind: 'warehouse-table', path: ['D', 'S', 'D'] },
      columns: [{ id: 'd__k', formula: '[D/K]' }, { id: 'd__nm', formula: '[D/Nm]' }] },
  ] }],
});
const UIN = (using, metricName = 'A') => ({
  viewName: 'V',
  pinned: [{ owner: 'F', name: metricName, using }],
  relationships: [{ name: 'F_TO_D', sourceAlias: 'F', targetAlias: 'D', sourceColumns: ['K'] }],
});

test('usingPaths unit: a mapped relationship builds the via-element', () => {
  const out = S.applyUsingPaths(USING_SPEC(), UIN(['F_TO_D']));
  const e = JSON.parse(out.spec).pages[0].elements;
  assert.deepEqual(e.map(x => x.id), ['f', 'f__via__f__to__d', 'd']);
  assert.deepEqual(out.findings.map(f => f.code), ['USING_METRICS_PATH_BOUND']);
  assert.deepEqual(e[0].metrics.map(m => m.id), ['f__m__b']);
  assert.deepEqual(e[1].metrics, [{ id: 'f__m__a', formula: 'Sum([Amt])', name: 'A' }]);
  assert.ok(e[1].columns.some(c => c.formula === '[F/F To D/Nm]' && c.name === 'Nm'));
});

test('usingPaths unit: a view-level metric without an owner is located on its converted element', () => {
  const input = UIN(['F_TO_D']);
  delete input.pinned[0].owner;
  const out = S.applyUsingPaths(USING_SPEC(), input);
  const e = JSON.parse(out.spec).pages[0].elements;
  assert.deepEqual(out.findings.map(f => f.code), ['USING_METRICS_PATH_BOUND']);
  assert.deepEqual(e[1].metrics.map(m => m.id), ['f__m__a']);
});

test('usingPaths unit: an unmapped USING relationship leaves the metric in place and warns USING_PATH_UNRESOLVED', () => {
  const spec = USING_SPEC();
  const out = S.applyUsingPaths(spec, UIN(['NO_SUCH_REL']));
  assert.equal(out.spec, spec);
  assert.equal(out.findings.length, 1);
  assert.equal(out.findings[0].code, 'USING_PATH_UNRESOLVED');
  assert.equal(out.findings[0].severity, 'warning');
  assert.equal(out.findings[0].source, 'embed');
  assert.match(out.findings[0].message, /NO_SUCH_REL/);
  assert.deepEqual(out.findings[0].object, { kind: 'metric', name: 'A', owner: 'F' });
});

test('usingPaths unit: relationship not emitted into the spec (keys differ) is unmapped, never guessed', () => {
  const spec = USING_SPEC();
  const input = UIN(['F_TO_D']);
  input.relationships[0].sourceColumns = ['OTHER'];
  const out = S.applyUsingPaths(spec, input);
  assert.equal(out.spec, spec);
  assert.equal(out.findings[0].code, 'USING_PATH_UNRESOLVED');
});

test('usingPaths unit: one unmapped metric does not stop a mapped one; unresolvable local refs leave the metric', () => {
  const spec = JSON.parse(USING_SPEC());
  spec.pages[0].elements[0].metrics.push({ id: 'f__m__c', formula: 'Sum([Missing Col])', name: 'C' });
  const input = { ...UIN(['F_TO_D']), pinned: [{ owner: 'F', name: 'A', using: ['F_TO_D'] }, { owner: 'F', name: 'C', using: ['F_TO_D'] }, { owner: 'F', name: 'B', using: ['GONE'] }] };
  const out = S.applyUsingPaths(JSON.stringify(spec), input);
  const e = JSON.parse(out.spec).pages[0].elements;
  assert.deepEqual(e[1].metrics.map(m => m.id), ['f__m__a']);
  assert.deepEqual(e[0].metrics.map(m => m.id).sort(), ['f__m__b', 'f__m__c']);
  assert.deepEqual(out.findings.filter(f => f.code === 'USING_PATH_UNRESOLVED').map(f => f.object.name).sort(), ['B', 'C']);
});

test('usingPaths unit: relationship names match case-insensitively (Snowflake identifiers)', () => {
  const out = S.applyUsingPaths(USING_SPEC(), UIN(['f_to_d']));
  assert.deepEqual(out.findings.map(f => f.code), ['USING_METRICS_PATH_BOUND']);
});

test('ENGINE.embedVersion is 1.3.1 (rel-ref minor-word casing)', () => assert.equal(S.ENGINE.embedVersion, '1.3.1'));

// ── YAML keys from real SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW exports ──

const EXPORT_YAML = `name: V
tables:
  - name: ORDERS
    base_table: { database: D, schema: S, table: ORDERS }
    dimensions:
      - { name: ID, expr: ID, unique: true }
    time_dimensions:
      - { name: DT, expr: DT, unique: false }
    facts:
      - { name: AMT, expr: AMT, unique: false }
  - name: CUSTOMERS
    base_table: { database: D, schema: S, table: CUSTOMERS }
    dimensions:
      - { name: CID, expr: CID, unique: true }
relationships:
  - name: ORDERS_TO_CUSTOMERS
    left_table: ORDERS
    right_table: CUSTOMERS
    relationship_columns: [ { left_column: CID, right_column: CID } ]
    relationship_type: many_to_one
    join_type: left_outer
`;

test('UNRECOGNIZED_YAML_KEY: export-only keys (relationship_type, unique, join_type left_outer) are benign and change nothing', () => {
  const r = yamlRun(EXPORT_YAML);
  assert.equal(r.ok, true);
  assert.deepEqual(unrec(r).map(f => f.message), []);
  assert.ok(!r.findings.some(f => f.code === 'JOIN_TYPE_INNER'));
  const stripped = EXPORT_YAML.replace(/, unique: (true|false)/g, '').replace(/    relationship_type.*\n/, '').replace(/    join_type.*\n/, '');
  assert.equal(yamlRun(stripped).spec, r.spec);
});

test('UNRECOGNIZED_YAML_KEY: a typo next to benign export keys is still reported', () => {
  const r = yamlRun(EXPORT_YAML.replace('unique: true }', 'uniqe: true }'));
  assert.match(unrec(r)[0].message, /uniqe/);
  assert.doesNotMatch(unrec(r)[0].message, /relationship_type/);
});

test('JOIN_TYPE_INNER: an inner join warns per relationship with the Sigma left-join difference; left_outer / absent do not', () => {
  const r = yamlRun(EXPORT_YAML.replace('join_type: left_outer', 'join_type: inner'));
  assert.equal(r.ok, true);
  assert.deepEqual(unrec(r).map(f => f.message), []);
  const f = r.findings.filter(x => x.code === 'JOIN_TYPE_INNER');
  assert.equal(f.length, 1);
  assert.equal(f[0].severity, 'warning');
  assert.equal(f[0].source, 'embed');
  assert.match(f[0].message, /Relationship ORDERS_TO_CUSTOMERS is an INNER join in Snowflake; Sigma relationships keep fact rows without a match, so counts can be higher in Sigma for rows whose key has no match\./);
  assert.equal(f[0].line, undefined);
  assert.deepEqual(f[0].object, { kind: 'relationship', name: 'ORDERS_TO_CUSTOMERS' });
  assert.ok(!yamlRun(EXPORT_YAML.replace(/    join_type.*\n/, '')).findings.some(x => x.code === 'JOIN_TYPE_INNER'));
  assert.ok(!S.convertSemanticView(SV_DDL, { ...SV, bestEffort: true }).results[0].findings.some(x => x.code === 'JOIN_TYPE_INNER'));
});

test('JOIN_TYPE_INNER: the value is case-insensitive', () => {
  const r = yamlRun(EXPORT_YAML.replace('join_type: left_outer', 'join_type: INNER'));
  assert.equal(r.findings.filter(x => x.code === 'JOIN_TYPE_INNER').length, 1);
});

test("usingPaths with crossTableReferences:'engine': a fact column that still holds an engine cross-table ref is not passed through", () => {
  const r = svRun(MULTI_DDL, { crossTableReferences: 'engine' });
  const base = els(r).find(e => e.id === 'order_fact');
  assert.ok(base.columns.some(c => /order_fact__to__customer_dim/.test(c.formula)), 'engine leaves the cross-table ref on the base');
  assert.equal(viaOf(r).length, 6);
  for (const e of viaOf(r)) {
    assert.ok(!e.columns.some(c => /order_fact__to__/.test(c.formula)), 'no engine relationship id in a via formula');
    assert.ok(!e.columns.some(c => c.name === 'Cust Region X'));
  }
});

// ── relDisplaySegment: canonical (minor-word-lowercase) display for refs THROUGH a relationship ──

test('relDisplaySegment lowercases minor words after the first, keeps explicit names and the first word', () => {
  assert.equal(S.relDisplaySegment('Day Of Month', false), 'Day of Month');
  assert.equal(S.relDisplaySegment('Week Of Year', false), 'Week of Year');
  assert.equal(S.relDisplaySegment('Days To Ship', false), 'Days to Ship');
  assert.equal(S.relDisplaySegment('Of Counsel', false), 'Of Counsel');
  assert.equal(S.relDisplaySegment('Day Of Month', true), 'Day Of Month');
  assert.equal(S.relDisplaySegment('Market Segment', false), 'Market Segment');
});

for (const [label, src] of [['DDL', SV_DDL], ['YAML', SV_YAML]]) {
  test(`RETAIL_ANALYTICS_SV ${label}: every 3-segment ref to an unnamed target column uses minor-word-lowercase display`, () => {
    const r = svRun(src);
    const all = els(r);
    const refs = [];
    for (const e of all) for (const c of [...(e.columns ?? []), ...(e.metrics ?? [])]) {
      for (const m of String(c.formula ?? '').matchAll(/\[([^\[\]/]+)\/([^\[\]/]+)\/([^\[\]/]+)\]/g)) refs.push(m[3]);
    }
    assert.ok(refs.length > 0);
    for (const d of refs) assert.equal(d, S.relDisplaySegment(d, false), `${d} not canonical`);
    assert.ok(refs.includes('Day of Month') && refs.includes('Week of Year'), JSON.stringify([...new Set(refs)]));
    assert.ok(!refs.some(d => /\b(Of|To)\b/.test(d.replace(/^\S+ /, ''))), 'no capitalized minor word remains');
    // local column names (metric refs) are unchanged
    for (const e of viaOf(r)) {
      const names = new Set(e.columns.map(c => c.name));
      for (const m of e.metrics) for (const ref of m.formula.matchAll(/\[([^\]]+)\]/g)) assert.ok(names.has(ref[1]));
    }
  });
}
