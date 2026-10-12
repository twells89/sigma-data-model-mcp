#!/usr/bin/env node
// Gates for the semantic-tools embed bundles. Exits non-zero on any failure.
//
//   node scripts/check-semantic-tools.mjs              # all gates (+ mutation self-test)
//   node scripts/check-semantic-tools.mjs --self-test  # only the mutation self-test
//
// a. Golden parity of upstream's CLI fixtures through BOTH bundles (ESM, and the
//    IIFE evaluated in a bare node:vm context with no require/process/Buffer),
//    plus ESM == IIFE.
// b. Mutation self-test: a bundle with "Page 1" -> "Page One" MUST fail (a).
// c. No Node-only imports in either bundle.
// e. YAML known-key drift guard (+ self-test: patched COPIES of readYaml.ts must fail it).
// d. PROVENANCE hashes match the bundles on disk and the pin, and sourceSha256 matches the current sources (stale-bundle gate).
// (Embed-layer behaviour tests live in semantic-tools-embed/test/embed.test.mjs.)

import { createHash } from 'node:crypto';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import vm from 'node:vm';
import { computeSourceSha256 } from './semantic-tools-source-hash.mjs';
import { checkYamlKeyDrift, READ_YAML, yamlKeyDrift } from './yaml-key-drift.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const VENDOR = join(ROOT, 'vendor', 'semantic-tools');
const ESM = join(VENDOR, 'semantic-tools-embed.mjs');
const IIFE = join(VENDOR, 'semantic-tools-embed.iife.min.js');
const CLONE = join(ROOT, '.cache', 'semantic-tools');
const T = join(CLONE, 'packages/cli/src/__tests__');
// The golden gate compares RAW engine output, so it runs with crossTableReferences:'engine'.
const OPTS = { connectionId: '9ad28df9-5f5f-4bae-8985-bb12d25829de', database: 'ANALYTICS', schema: 'MODELS', columnTypes: {} };

// Fixtures upstream has no golden for: the converter must refuse these, with exactly these codes.
const REFUSALS = {
  '03-non-additive-metric': ['SEMI_ADDITIVE_METRIC'],
  '04-asof-relationship': ['ASOF_RELATIONSHIP', 'DOWNSTREAM_OF_BLOCKED_RELATIONSHIP'],
  '06-range-join': ['RANGE_JOIN_CONSTRAINT', 'RANGE_JOIN_RELATIONSHIP', 'DOWNSTREAM_OF_BLOCKED_RELATIONSHIP'],
  '07-window-metric': ['WINDOW_METRIC_DYNAMIC_PARTITION'],
  '11-tables-only': ['MISSING_DIMENSIONS_OR_METRICS'],
};

const failures = [];
const fail = msg => { failures.push(msg); console.log(`  FAIL ${msg}`); };
const sha256 = buf => createHash('sha256').update(buf).digest('hex');

if (!existsSync(join(T, 'fixtures.ts')) || !existsSync(ESM) || !existsSync(IIFE)) {
  console.error('Missing bundles or upstream clone: run `npm run build:semantic-tools` first.');
  process.exit(2);
}

const fixtureNames = readFileSync(join(T, 'fixtures.ts'), 'utf8').match(/'\d\d-[a-z-]+'/g).map(s => s.slice(1, -1));
const ddl = n => readFileSync(join(T, 'fixtures', n + '.sql'), 'utf8');

async function loadEsm(path) {
  return import(pathToFileURL(path).href + `?t=${Date.now()}${Math.random()}`);
}

function loadIife(path) {
  // Bare context: JS builtins only. No require, process, Buffer, console, globalThis.node internals.
  const ctx = vm.createContext({});
  vm.runInContext(readFileSync(path, 'utf8'), ctx, { filename: 'semantic-tools-embed.iife.min.js' });
  if (typeof ctx.SemanticTools?.convertSemanticView !== 'function') throw new Error('IIFE did not define global SemanticTools');
  return ctx.SemanticTools;
}

/** Runs every upstream fixture through `api`; returns { pass, fail, refused, outputs } and records failures under `label`. */
function goldenGate(label, api, { quiet = false, record = true } = {}) {
  const log = quiet ? () => {} : console.log;
  const bad = [];
  const outputs = {};
  let pass = 0, refused = 0;
  for (const n of fixtureNames) {
    const run = api.convertSemanticView(ddl(n), { ...OPTS, format: 'snowflake-semantic-view', crossTableReferences: 'engine' });
    const r = run.results[0];
    outputs[n] = JSON.stringify(run);
    const goldenPath = join(T, 'golden', n + '.json');
    if (existsSync(goldenPath)) {
      if (r.ok && r.spec + '\n' === readFileSync(goldenPath, 'utf8')) { pass++; log(`  ok   ${label} golden ${n}`); }
      else bad.push(`${label}: ${n} differs from golden (ok=${r.ok})`);
    } else {
      const want = REFUSALS[n];
      const got = r.errors.map(e => e.code).sort();
      if (!want) bad.push(`${label}: ${n} has no golden and no expected-refusal entry`);
      else if (r.ok || JSON.stringify(got) !== JSON.stringify([...want].sort())) bad.push(`${label}: ${n} expected refusal [${want}] got ok=${r.ok} [${got}]`);
      else { refused++; log(`  ok   ${label} refusal ${n}: ${got.join(',')}`); }
    }
    // Auto-detection (no explicit format) must agree with the extension-style explicit format.
    const auto = api.convertSemanticView(ddl(n), { ...OPTS, crossTableReferences: 'engine' }).results[0];
    if (auto.ok !== r.ok || auto.spec !== r.spec) bad.push(`${label}: ${n} auto-detected format result differs from explicit`);
  }
  if (record) for (const b of bad) fail(b);
  return { pass, refused, bad, outputs };
}

// ── Mutation self-test (b) ──
async function selfTest() {
  console.log('Mutation self-test: "Page 1" -> "Page One" in ESM bundle must FAIL the golden gate');
  const src = readFileSync(ESM, 'utf8');
  if (!src.includes('"Page 1"')) { fail('self-test: ESM bundle does not contain "Page 1" to mutate'); return; }
  const dir = mkdtempSync(join(tmpdir(), 'st-mutant-'));
  try {
    const p = join(dir, 'mutant.mjs');
    writeFileSync(p, src.replaceAll('"Page 1"', '"Page One"'));
    const g = goldenGate('mutant', await loadEsm(p), { quiet: true, record: false });
    if (g.bad.length === 0) fail('self-test: mutant bundle PASSED the golden gate (the gate is not sensitive)');
    else console.log(`  ok   mutant rejected (${g.bad.length} golden mismatches caught)`);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

// ── (e) YAML known-key drift guard ──
function driftSelfTest() {
  console.log('Drift self-test: patched COPIES of readYaml.ts must fail the known-key guard');
  const src = readFileSync(READ_YAML, 'utf8');
  const mutants = {
    'new key read on object': src.replace('object.is_enum === true', "object.is_enum === true && object.brand_new_key === undefined"),
    'new key read on table': src.replace('asString(table.description)', 'asString(table.shiny_new_key) ?? asString(table.description)'),
    'known key no longer read': src.replace('table.base_table', 'table.base_tbl'),
    'unknown variable reads a key': src.replace('const name = asString(relationship.name);', "const name = asString(relationship.name); void other_rel.some_new_key;"),
  };
  for (const [label, text] of Object.entries(mutants)) {
    if (text === src) { fail(`drift self-test: mutation "${label}" did not change the copy`); continue; }
    if (yamlKeyDrift(text).length === 0) fail(`drift self-test: mutant "${label}" PASSED the guard (not sensitive)`);
    else console.log(`  ok   mutant rejected: ${label}`);
  }
}

if (process.argv.includes('--self-test')) {
  driftSelfTest();
  await selfTest();
  console.log(failures.length ? `\nSELF-TEST FAILED (${failures.length})` : '\nself-test passed');
  process.exit(failures.length ? 1 : 0);
}

// ── (a) golden parity, ESM + IIFE ──
console.log('Golden parity: ESM');
const esm = await loadEsm(ESM);
const g1 = goldenGate('esm', esm);
console.log('Golden parity: IIFE (bare vm context)');
const iife = loadIife(IIFE);
const g2 = goldenGate('iife', iife);
for (const n of fixtureNames) if (g1.outputs[n] !== g2.outputs[n]) fail(`ESM and IIFE results differ for ${n}`);
console.log(`golden byte-equal: ${g1.pass} pass (ESM), ${g2.pass} pass (IIFE); expected refusals: ${g1.refused} (ESM), ${g2.refused} (IIFE); ESM==IIFE: ${fixtureNames.every(n => g1.outputs[n] === g2.outputs[n])}`);
if (esm.ENGINE.semanticToolsCommit !== readFileSync(join(ROOT, 'semantic-tools-embed/PIN'), 'utf8').trim()) fail('ENGINE.semanticToolsCommit != PIN');
if (JSON.stringify(esm.ENGINE) !== JSON.stringify(iife.ENGINE)) fail('ENGINE differs between ESM and IIFE');

// ── (b) mutation self-test ──
await selfTest();

// ── (e) YAML known-key drift ──
console.log('YAML known-key drift guard');
const drift = checkYamlKeyDrift();
for (const p of drift) fail(p);
if (drift.length === 0) console.log('  ok   yamlKnownKeys.json matches upstream readYaml.ts');
driftSelfTest();

// ── (c) no Node-only imports ──
console.log('No Node-only imports');
for (const [name, p] of [['ESM', ESM], ['IIFE', IIFE]]) {
  const text = readFileSync(p, 'utf8');
  for (const [what, re] of [
    ['require(', /\brequire\s*\(/],
    ['from "node:', /from\s*["']node:/],
    ['import("node:', /import\s*\(\s*["']node:/],
  ]) {
    if (re.test(text)) fail(`${name} bundle contains ${what}`);
  }
}
if (!failures.some(f => f.includes('bundle contains'))) console.log('  ok   no require( / node: imports in either bundle');

// ── (d) provenance matches what is on disk ──
console.log('Provenance');
const prov = JSON.parse(readFileSync(join(VENDOR, 'PROVENANCE.json'), 'utf8'));
if (prov.semanticToolsCommit !== readFileSync(join(ROOT, 'semantic-tools-embed/PIN'), 'utf8').trim()) fail('PROVENANCE commit != PIN (rebuild)');
if (prov.sha256['semantic-tools-embed.mjs'] !== sha256(readFileSync(ESM))) fail('PROVENANCE sha256 for ESM is stale (rebuild)');
if (prov.sha256['semantic-tools-embed.iife.min.js'] !== sha256(readFileSync(IIFE))) fail('PROVENANCE sha256 for IIFE is stale (rebuild)');
if (prov.sourceSha256 === undefined) fail('bundle is stale — run npm run build:semantic-tools (PROVENANCE has no sourceSha256)');
else if (prov.sourceSha256 !== computeSourceSha256(ROOT)) fail('bundle is stale — run npm run build:semantic-tools (sources changed since the bundle was built)');
if (!failures.some(f => f.includes('PROVENANCE') || f.includes('stale'))) console.log('  ok   PROVENANCE matches bundles, pin and current sources');

console.log(failures.length ? `\nFAILED: ${failures.length} problem(s)` : '\nAll semantic-tools gates passed');
process.exit(failures.length ? 1 : 0);
