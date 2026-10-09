// Snowflake semantic view adapter (src/snowflake.ts) over the bundled semantic-tools engine,
// plus the convert_snowflake_to_sigma tool end to end (in-memory MCP client).
// Upstream golden fixtures are read from .cache/semantic-tools (created by `npm run build:semantic-tools`).
import { test } from 'node:test';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { convertSnowflakeSemanticView, CONNECTION_PLACEHOLDER, CONNECTION_WARNING, AUTO_METRICS_DEPRECATION } from './snowflake.js';
import { createSigmaServer } from './tools.js';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const FIX = join(ROOT, '.cache/semantic-tools/packages/cli/src/__tests__');
const HAVE_FIXTURES = existsSync(join(FIX, 'golden'));
const SKIP = HAVE_FIXTURES ? false : 'upstream fixtures absent: run `npm run build:semantic-tools` to clone .cache/semantic-tools';
const ddl = (n: string) => readFileSync(join(FIX, 'fixtures', n + '.sql'), 'utf8');
const withoutName = (m: any) => { const { name: _n, ...rest } = m ?? {}; return rest; };
const golden = (n: string) => readFileSync(join(FIX, 'golden', n + '.json'), 'utf8');
const RETAIL_INLINE = readFileSync(join(ROOT, 'semantic-tools-embed/test/fixtures/retail_inline_relationships.yaml'), 'utf8');
const PLUGS = readFileSync(join(ROOT, 'semantic-tools-embed/test/fixtures/plugs.yaml'), 'utf8');
// Same options upstream's FIXTURE_OPTIONS holds constant for every fixture.
// synonymsInDescription:false — the upstream goldens carry no synonym text; the adapter defaults it on.
const BASE_OPTS = { connectionId: '9ad28df9-5f5f-4bae-8985-bb12d25829de', database: 'ANALYTICS', schema: 'MODELS', columnTypes: {}, synonymsInDescription: false };
// Golden comparisons are against RAW engine output, so they opt out of the derived-element correction.
const OPTS = { ...BASE_OPTS, crossTableReferences: 'engine' as const };

const GOLDEN = ['01-single-table', '02-star-schema', '05-role-playing-alias', '08-derived-metric', '09-prose-comments', '10-lowercase',
  '12-unqualified-names', '13-cortex-extension', '14-conditional-metrics', '15-unique-keys', '16-sample-values', '17-mapped-constructs'];
const REFUSED: [string, string][] = [
  ['03-non-additive-metric', 'SEMI_ADDITIVE_METRIC'],
  ['04-asof-relationship', 'ASOF_RELATIONSHIP'],
  ['06-range-join', 'RANGE_JOIN_CONSTRAINT'],
  ['07-window-metric', 'WINDOW_METRIC_DYNAMIC_PARTITION'],
  ['11-tables-only', 'MISSING_DIMENSIONS_OR_METRICS'],
];
const metricNames = (model: any): string[] => (model?.pages ?? []).flatMap((p: any) => p.elements.flatMap((e: any) => (e.metrics ?? []).map((m: any) => m.name))).sort();

for (const name of GOLDEN) {
  test(`golden ${name}: model equals the upstream golden spec`, { skip: SKIP }, () => {
    const r = convertSnowflakeSemanticView(ddl(name), OPTS);
    assert.equal(r.refused, false);
    assert.equal(JSON.stringify(withoutName(r.model)), JSON.stringify(JSON.parse(golden(name))));
    assert.equal(r.results.length, 1);
    assert.equal(r.results[0].ok, true);
    assert.ok(r.stats.elements >= 1);
  });
}

for (const [name, code] of REFUSED) {
  test(`refusal ${name}: refused, no model, ${code} reported`, { skip: SKIP }, () => {
    const r = convertSnowflakeSemanticView(ddl(name), OPTS);
    assert.equal(r.refused, true);
    assert.equal(r.model, undefined);
    assert.equal(r.results[0].model, undefined);
    assert.deepEqual(r.stats, { tables: 0, elements: 0, columns: 0, metrics: 0, relationships: 0 });
    assert.ok(r.findings.some(f => f.code === code && f.severity === 'error'), JSON.stringify(r.findings.map(f => f.code)));
    assert.ok(r.warnings.some(w => w.startsWith(`${code}: `)), r.warnings.join('\n'));
  });
}

test("default (cross_table_references 'derived'): 02 moves the cross-table column to orders__joined; 'engine' keeps the golden", { skip: SKIP }, () => {
  const r = convertSnowflakeSemanticView(ddl('02-star-schema'), BASE_OPTS);
  assert.equal(r.refused, false);
  const els: any[] = (r.model as any).pages[0].elements;
  const orders = els.find(e => e.id === 'orders');
  const joined = els.find(e => e.id === 'orders__joined');
  assert.ok(!/\[[^\]\/]+\/[^\]\/]+\/[^\]\/]+\]/.test(JSON.stringify(orders)));
  assert.equal(joined.source.elementId, 'orders');
  assert.equal(joined.name, 'ORDERS (joined)');
  assert.equal(els.find(e => e.id === 'lineitem').relationships[0].targetElementId, 'orders__joined');
  assert.equal(joined.columns.find((c: any) => c.id === 'orders__customer_name').formula, '[ORDERS/Orders To Customer/C Name]');
  assert.ok(r.findings.some(f => f.code === 'CROSS_TABLE_REFERENCES_MOVED' && f.source === 'embed' && f.severity === 'info'));
  assert.ok(r.warnings.some(w => w.startsWith('CROSS_TABLE_REFERENCES_MOVED: ')));
  assert.equal(r.stats.elements, 4);
  const eng = convertSnowflakeSemanticView(ddl('02-star-schema'), { ...BASE_OPTS, crossTableReferences: 'engine' });
  assert.equal(JSON.stringify(withoutName(eng.model)), JSON.stringify(JSON.parse(golden('02-star-schema'))));
  assert.equal(eng.findings.some(f => f.code === 'CROSS_TABLE_REFERENCES_MOVED'), false);
});

test('inferNumberFormats (evidence-based): Average Discount is not currency; sample_values make it a percent', () => {
  const TPCH = readFileSync(join(ROOT, 'semantic-tools-embed/test/fixtures/tpch.yaml'), 'utf8');
  const fmt = (src: string, name: string) => {
    const r = convertSnowflakeSemanticView(src, { connectionId: BASE_OPTS.connectionId, inferNumberFormats: true });
    return (r.model as any).pages[0].elements.flatMap((e: any) => e.metrics ?? []).find((m: any) => m.name === name)?.format;
  };
  assert.equal(fmt(TPCH, 'Average Discount'), undefined);
  assert.equal(fmt(TPCH, 'Total Revenue').currencySymbol, '$');
  const withSamples = TPCH.replace('data_type: NUMBER(12,2)\n      - name: NET_PRICE', 'data_type: NUMBER(12,2)\n        sample_values: [0.04, 0.06]\n      - name: NET_PRICE');
  assert.notEqual(withSamples, TPCH);
  assert.equal(fmt(withSamples, 'Average Discount').formatString, ',.2%');
  const off = convertSnowflakeSemanticView(TPCH, { connectionId: BASE_OPTS.connectionId });
  assert.ok(!off.findings.some(f => f.code.startsWith('FORMATS_')));
});

test('YAML with relationships nested inside a table: UNRECOGNIZED_YAML_KEY surfaces in findings and warnings (not silently dropped)', () => {
  const r = convertSnowflakeSemanticView(RETAIL_INLINE, { connectionId: 'c1' });
  assert.equal(r.refused, false);
  const f = r.findings.filter(x => x.code === 'UNRECOGNIZED_YAML_KEY');
  assert.equal(f.length, 1);
  assert.equal(f[0].severity, 'warning');
  assert.equal(f[0].source, 'embed');
  assert.match(f[0].message, /tables\[ORDER_FACT\]\.relationships/);
  assert.deepEqual(r.results[0].findings.filter(x => x.code === 'UNRECOGNIZED_YAML_KEY'), f);
  const w = r.warnings.filter(x => x.startsWith('UNRECOGNIZED_YAML_KEY: '));
  assert.equal(w.length, 1);
  assert.match(w[0], /top-level `relationships`/);
  assert.equal(r.stats.relationships, 0);
  // a clean YAML raises none
  assert.equal(convertSnowflakeSemanticView(PLUGS, { connectionId: 'c1' }).findings.some(x => x.code === 'UNRECOGNIZED_YAML_KEY'), false);
});

test('PLUGS yaml: exactly the view\'s own metrics, no invented Sum() metrics, schemaVersion 1, stats from spec', () => {
  const r = convertSnowflakeSemanticView(PLUGS, { connectionId: 'c1' });
  assert.equal(r.refused, false);
  assert.equal((r.model as any).schemaVersion, 1);
  assert.equal((r.model as any).name, 'PLUGS_SEMANTIC_VIEW');
  assert.deepEqual(metricNames(r.model), ['Profit', 'Profit Margin', 'Revenue']);
  assert.equal(r.stats.metrics, 3);
  assert.ok(r.stats.elements >= 5 && r.stats.relationships >= 4 && r.stats.columns > 0, JSON.stringify(r.stats));
  assert.deepEqual(Object.keys(r.stats).sort(), ['columns', 'elements', 'metrics', 'relationships', 'tables']);
  assert.equal(r.engine.semanticToolsCommit, readFileSync(join(ROOT, 'semantic-tools-embed/PIN'), 'utf8').trim());
});

test('auto_metrics:true is ignored (still no invented metrics) and adds the deprecation warning', () => {
  const on = convertSnowflakeSemanticView(PLUGS, { connectionId: 'c1', autoMetrics: true });
  const off = convertSnowflakeSemanticView(PLUGS, { connectionId: 'c1' });
  assert.deepEqual(metricNames(on.model), ['Profit', 'Profit Margin', 'Revenue']);
  assert.ok(on.warnings.includes(AUTO_METRICS_DEPRECATION));
  assert.ok(!off.warnings.includes(AUTO_METRICS_DEPRECATION));
  assert.equal(JSON.stringify(on.model), JSON.stringify(off.model));
});

test('empty connectionId: placeholder in the spec and the "Connection ID not set" warning', () => {
  const r = convertSnowflakeSemanticView(PLUGS, { connectionId: '' });
  assert.ok(JSON.stringify(r.model).includes(CONNECTION_PLACEHOLDER));
  assert.equal(r.warnings[0], CONNECTION_WARNING);
  const set = convertSnowflakeSemanticView(PLUGS, { connectionId: 'abc' });
  assert.ok(!JSON.stringify(set.model).includes(CONNECTION_PLACEHOLDER));
  assert.ok(!set.warnings.includes(CONNECTION_WARNING));
});

test('needs path: unqualified tables without database/schema -> NEEDS_DATABASE/NEEDS_SCHEMA, no model', { skip: SKIP }, () => {
  const r = convertSnowflakeSemanticView(ddl('12-unqualified-names'), { connectionId: 'c1' });
  assert.equal(r.refused, true);
  assert.equal(r.model, undefined);
  assert.deepEqual(r.needs, { database: ['REGION', 'SF1.NATION'], schema: ['REGION'] });
  const codes = r.findings.map(f => f.code);
  assert.ok(codes.includes('NEEDS_DATABASE') && codes.includes('NEEDS_SCHEMA'), codes.join());
  assert.ok(r.findings.filter(f => f.code.startsWith('NEEDS_')).every(f => f.source === 'embed' && f.severity === 'error'));
  assert.ok(r.warnings.some(w => w.startsWith('NEEDS_DATABASE: ')));
});

test('best_effort on 03: refused without (bestEffortWouldConvert), converts with and reports overriddenErrors', { skip: SKIP }, () => {
  const strict = convertSnowflakeSemanticView(ddl('03-non-additive-metric'), OPTS);
  assert.equal(strict.refused, true);
  assert.equal(strict.bestEffortWouldConvert, true);
  const loose = convertSnowflakeSemanticView(ddl('03-non-additive-metric'), { ...OPTS, bestEffort: true });
  assert.equal(loose.refused, false);
  assert.ok(loose.model);
  assert.deepEqual(loose.results[0].overriddenErrors, ['SEMI_ADDITIVE_METRIC']);
  assert.ok(loose.warnings.some(w => w.startsWith('SEMI_ADDITIVE_METRIC: ')), 'overridden error still surfaced to the caller');
});

test('multi-statement DDL: one result per view, first ok view is the model, warnings are view-prefixed (refused = ANY view refused)', { skip: SKIP }, () => {
  const r = convertSnowflakeSemanticView(ddl('03-non-additive-metric') + '\n;\n' + ddl('02-star-schema'), OPTS);
  assert.equal(r.results.length, 2);
  assert.deepEqual(r.results.map(x => x.ok), [false, true]);
  assert.equal(r.refused, true, 'one refused view => refused');
  assert.equal(r.allRefused, false);
  assert.equal(r.refusedViews.length, 1);
  assert.equal(JSON.stringify(withoutName(r.model)), JSON.stringify(JSON.parse(golden('02-star-schema'))));
  assert.ok(r.warnings.some(w => /^\[\w+\] SEMI_ADDITIVE_METRIC: /.test(w)));
});

test('garbage input never throws: refused with a finding', () => {
  const r = convertSnowflakeSemanticView('SELECT 1', { connectionId: 'c1' });
  assert.equal(r.refused, true);
  assert.equal(r.model, undefined);
  assert.ok(r.findings.length > 0);
});

// ── tool handler, end to end through an in-memory MCP client ──

async function callTool(args: Record<string, unknown>) {
  const server = createSigmaServer();
  const client = new Client({ name: 'test', version: '0.0.0' });
  const [a, b] = InMemoryTransport.createLinkedPair();
  await Promise.all([server.connect(a), client.connect(b)]);
  try {
    const res: any = await client.callTool({ name: 'convert_snowflake_to_sigma', arguments: args });
    const text = res.content[0].text as string;
    return { isError: !!res.isError, text, json: res.isError ? undefined : JSON.parse(text) };
  } finally {
    await client.close();
    await server.close();
  }
}

test('tool: yaml_content alias works', async () => {
  const r = await callTool({ yaml_content: PLUGS, connection_id: '' });
  assert.equal(r.isError, false, r.text);
  assert.equal(r.json.refused, false);
  assert.equal(r.json.sigmaDataModel.schemaVersion, 1);
  assert.deepEqual(metricNames(r.json.sigmaDataModel), ['Profit', 'Profit Margin', 'Revenue']);
  assert.deepEqual(Object.keys(r.json).sort(), ['allRefused', 'bestEffortWouldConvert', 'engine', 'findings', 'needs', 'refused', 'refusedViews', 'results', 'sigmaDataModel', 'stats', 'warnings']);
  assert.equal(r.json.results.length, 1);
  assert.equal(r.json.results[0].sigmaDataModel, undefined, 'single view: model not duplicated in results');
});

test('tool: source works (and auto_metrics is accepted but ignored)', async () => {
  const r = await callTool({ source: PLUGS, connection_id: 'abc', auto_metrics: true });
  assert.equal(r.isError, false, r.text);
  assert.deepEqual(metricNames(r.json.sigmaDataModel), ['Profit', 'Profit Margin', 'Revenue']);
  assert.ok(r.json.warnings.includes(AUTO_METRICS_DEPRECATION));
});

test('tool: cross_table_references enum is plumbed (default derived, engine = raw) and rejects other values', { skip: SKIP }, async () => {
  const d = await callTool({ source: ddl('02-star-schema'), database: 'A', schema: 'B' });
  assert.ok(d.json.sigmaDataModel.pages[0].elements.some((e: any) => e.id === 'orders__joined'));
  const e = await callTool({ source: ddl('02-star-schema'), database: 'A', schema: 'B', cross_table_references: 'engine' });
  assert.ok(!e.json.sigmaDataModel.pages[0].elements.some((x: any) => x.id === 'orders__joined'));
  const bad = await callTool({ source: ddl('02-star-schema'), database: 'A', schema: 'B', cross_table_references: 'nope' });
  assert.equal(bad.isError, true);
});

test('tool: neither source nor yaml_content (or both, or blank) is an error', async () => {
  for (const args of [{}, { source: '   ' }, { source: PLUGS, yaml_content: PLUGS }]) {
    const r = await callTool(args);
    assert.equal(r.isError, true, JSON.stringify(args).slice(0, 60));
    assert.match(r.text, /exactly one/);
  }
});

test('tool: a refusal returns refused:true and NO sigmaDataModel', { skip: SKIP }, async () => {
  const r = await callTool({ source: ddl('03-non-additive-metric'), database: 'A', schema: 'B' });
  assert.equal(r.isError, false);
  assert.equal(r.json.refused, true);
  assert.equal('sigmaDataModel' in r.json, false);
  assert.equal(r.json.bestEffortWouldConvert, true);
  assert.ok(r.json.findings.some((f: any) => f.code === 'SEMI_ADDITIVE_METRIC'));
  const be = await callTool({ source: ddl('03-non-additive-metric'), database: 'A', schema: 'B', best_effort: true });
  assert.equal(be.json.refused, false);
  assert.ok(be.json.sigmaDataModel);
});

test('tool: needs + database/schema/column_types are plumbed through', { skip: SKIP }, async () => {
  const need = await callTool({ source: ddl('12-unqualified-names') });
  assert.equal(need.json.refused, true);
  assert.deepEqual(need.json.needs, { database: ['REGION', 'SF1.NATION'], schema: ['REGION'] });
  const ok = await callTool({ source: ddl('12-unqualified-names'), database: 'ANALYTICS', schema: 'MODELS', column_types: {} });
  assert.equal(ok.json.refused, false);
  assert.equal(ok.json.needs, null);
  assert.ok(!ok.json.warnings.some((w: string) => w.startsWith('COLUMN_TYPES_SKIPPED')), 'column_types supplied');
});

test('tool: multi-view input returns per-view results and no top-level sigmaDataModel', { skip: SKIP }, async () => {
  const r = await callTool({ source: ddl('01-single-table') + '\n;\n' + ddl('02-star-schema'), ...{ database: 'ANALYTICS', schema: 'MODELS' } });
  assert.equal(r.json.results.length, 2);
  assert.equal('sigmaDataModel' in r.json, false);
  assert.ok(r.json.results.every((x: any) => x.ok && x.sigmaDataModel && x.viewName));
});

test('synonyms go into descriptions by default (like the original browser tool); false drops them with SYNONYMS_DROPPED', () => {
  const on = convertSnowflakeSemanticView(PLUGS, { connectionId: 'c1' });
  assert.ok(JSON.stringify(on.model).includes('Also known as: '), 'default output carries synonyms');
  assert.equal(on.findings.some(f => f.code === 'SYNONYMS_DROPPED'), false);
  const off = convertSnowflakeSemanticView(PLUGS, { connectionId: 'c1', synonymsInDescription: false });
  assert.equal(JSON.stringify(off.model).includes('Also known as: '), false);
  assert.ok(off.findings.some(f => f.code === 'SYNONYMS_DROPPED'));
});

// ── multi-view output: every view's model and stats are surfaced, refusal is ANY-view ──

const MULTI_OPTS = { database: 'ANALYTICS', schema: 'MODELS' };

test('adapter: per-view stats, aggregate stats, refusedViews/allRefused', { skip: SKIP }, () => {
  const r = convertSnowflakeSemanticView(ddl('01-single-table') + '\n;\n' + ddl('02-star-schema') + '\n;\n' + ddl('03-non-additive-metric'), { ...OPTS });
  assert.deepEqual(r.results.map(x => x.ok), [true, true, false]);
  assert.equal(r.refused, true);
  assert.equal(r.allRefused, false);
  assert.deepEqual(r.refusedViews, [r.results[2].viewName]);
  assert.ok(r.results[0].stats && r.results[1].stats, 'per-view stats on converted views');
  assert.equal(r.results[2].stats, undefined);
  assert.equal(r.stats.metrics, r.results[0].stats!.metrics + r.results[1].stats!.metrics, 'top-level stats aggregate every converted view');
  assert.equal(r.stats.elements, r.results[0].stats!.elements + r.results[1].stats!.elements);
});

test('tool: top-level refused is true when ANY view refused; stats aggregate; per-view stats in results', { skip: SKIP }, async () => {
  const r = await callTool({ source: ddl('01-single-table') + '\n;\n' + ddl('03-non-additive-metric'), ...MULTI_OPTS });
  assert.equal(r.json.refused, true);
  assert.equal(r.json.allRefused, false);
  assert.equal(r.json.refusedViews.length, 1);
  assert.equal(r.json.results.length, 2);
  assert.ok(r.json.results[0].sigmaDataModel && r.json.results[0].stats);
  assert.equal(r.json.stats.metrics, r.json.results[0].stats.metrics);
  const both = await callTool({ source: ddl('01-single-table') + '\n;\n' + ddl('02-star-schema'), ...MULTI_OPTS });
  assert.equal(both.json.refused, false);
  assert.equal(both.json.stats.metrics, both.json.results[0].stats.metrics + both.json.results[1].stats.metrics);
});

function runCli(sql: string, extra: string[] = []) {
  const dir = mkdtempSync(join(tmpdir(), 'sf-cli-'));
  const f = join(dir, 'in.sql');
  writeFileSync(f, sql);
  const p = spawnSync(process.execPath, ['--import', 'tsx/esm', join(ROOT, 'src/cli.ts'), 'snowflake', '--file', f, '--db', 'ANALYTICS', '--schema', 'MODELS', ...extra], { encoding: 'utf8', cwd: ROOT });
  return { status: p.status, out: p.stdout ? JSON.parse(p.stdout) : undefined, err: p.stderr };
}

test('cli: multi-view input emits EVERY view model in results[], exit 0 when all ok', { skip: SKIP }, () => {
  const r = runCli(ddl('01-single-table') + '\n;\n' + ddl('02-star-schema'));
  assert.equal(r.status, 0, r.err);
  assert.equal(r.out.results.length, 2);
  assert.ok(r.out.results.every((x: any) => x.ok && x.viewName && x.sigmaDataModel && Array.isArray(x.findings)));
  assert.equal('sigmaDataModel' in r.out, false, 'multi: models live in results[]');
});

test('cli: exit non-zero when ANY view refused, and the converted views are still emitted', { skip: SKIP }, () => {
  const r = runCli(ddl('01-single-table') + '\n;\n' + ddl('03-non-additive-metric'));
  assert.equal(r.status, 1);
  assert.deepEqual(r.out.results.map((x: any) => x.ok), [true, false]);
  assert.ok(r.out.results[0].sigmaDataModel);
  assert.equal(r.out.refused, true);
});

test('cli: single view keeps the sigmaDataModel top-level shape', { skip: SKIP }, () => {
  const r = runCli(ddl('01-single-table'));
  assert.equal(r.status, 0, r.err);
  assert.ok(r.out.sigmaDataModel);
});
