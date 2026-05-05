#!/usr/bin/env node
// Regression-corpus runner. Walks regression-corpus/<format>/<name>/, runs each
// fixture through the matching MCP converter, POSTs the result to the Sigma
// test folder, asserts shape + zero-error-columns, and cleans up.
//
// Exits 0 if every fixture passes, 1 otherwise.
//
// Hard gate: any fixture that POSTs successfully but has even one column with
// type.type === "error" is a FAIL. A 200-OK save with a runtime-broken column
// is silently broken from the user's perspective and must not be allowed to
// land.
//
// Usage:
//   npm run regression                 # run everything
//   npm run regression -- tableau      # run a single format dir
//   npm run regression -- tableau/lod_basic   # run a single fixture
//
// Required env vars: SIGMA_BASE_URL, SIGMA_CLIENT_ID, SIGMA_CLIENT_SECRET.
// Optional: SIGMA_TEST_FOLDER_ID (default: 9ca9bf60-6a33-43dd-967d-1ba6352c54bb).

import { readFile, readdir, stat } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { join, dirname, basename, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT      = resolve(__dirname, '..');
const CORPUS    = join(ROOT, 'regression-corpus');
const BUILD_DIR = join(ROOT, 'build');

const SIGMA_BASE_URL  = process.env.SIGMA_BASE_URL;
const SIGMA_CLIENT_ID = process.env.SIGMA_CLIENT_ID;
const SIGMA_SECRET    = process.env.SIGMA_CLIENT_SECRET;
const TEST_FOLDER_ID  = process.env.SIGMA_TEST_FOLDER_ID || '9ca9bf60-6a33-43dd-967d-1ba6352c54bb';

if (!SIGMA_BASE_URL || !SIGMA_CLIENT_ID || !SIGMA_SECRET) {
  console.error('FAIL: SIGMA_BASE_URL, SIGMA_CLIENT_ID, SIGMA_CLIENT_SECRET must be set in env.');
  process.exit(2);
}

// ── Sigma API helpers ──────────────────────────────────────────────────────

let _token = null;
async function sigmaToken() {
  if (_token) return _token;
  const resp = await fetch(`${SIGMA_BASE_URL}/v2/auth/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `grant_type=client_credentials&client_id=${SIGMA_CLIENT_ID}&client_secret=${SIGMA_SECRET}`,
  });
  if (!resp.ok) throw new Error(`Sigma token ${resp.status}: ${await resp.text()}`);
  const j = await resp.json();
  _token = j.access_token;
  return _token;
}

async function sigmaPost(spec, name) {
  const token = await sigmaToken();
  const body = { ...spec, name, folderId: TEST_FOLDER_ID };
  const resp = await fetch(`${SIGMA_BASE_URL}/v2/dataModels/spec`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const txt = await resp.text();
  if (!resp.ok) return { success: false, status: resp.status, error: txt };
  let j;
  try { j = JSON.parse(txt); } catch { return { success: false, error: 'non-JSON response: ' + txt.slice(0, 200) }; }
  return { success: true, dataModelId: j.dataModelId };
}

async function sigmaGetColumns(dataModelId) {
  const token = await sigmaToken();
  const resp = await fetch(`${SIGMA_BASE_URL}/v2/dataModels/${dataModelId}/columns`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!resp.ok) throw new Error(`columns ${resp.status}: ${await resp.text()}`);
  return resp.json();
}

async function sigmaDelete(dataModelId) {
  try {
    const token = await sigmaToken();
    await fetch(`${SIGMA_BASE_URL}/v2/files/${dataModelId}`, {
      method: 'DELETE',
      headers: { Authorization: `Bearer ${token}` },
    });
  } catch (e) {
    console.warn(`  cleanup warn: ${e.message}`);
  }
}

// ── Converter dispatch ─────────────────────────────────────────────────────

async function loadConverters() {
  if (!existsSync(BUILD_DIR)) {
    throw new Error(`build/ not found — run 'npm run build' first`);
  }
  const tableau = await import(join(BUILD_DIR, 'tableau.js'));
  const lookml = await import(join(BUILD_DIR, 'lookml.js'));
  const dbt = await import(join(BUILD_DIR, 'dbt.js'));
  const cube = await import(join(BUILD_DIR, 'cube.js'));
  const omni = await import(join(BUILD_DIR, 'omni.js'));
  const alteryx = await import(join(BUILD_DIR, 'alteryx.js'));
  const powerbi = await import(join(BUILD_DIR, 'powerbi.js'));
  return {
    tableau: tableau.convertTableauToSigma || tableau.default?.convertTableauToSigma,
    lookml: lookml.convertLookMLToSigma || lookml.default?.convertLookMLToSigma,
    dbt: dbt.convertDbtToSigma || dbt.default?.convertDbtToSigma,
    cube: cube.convertCubeToSigma || cube.default?.convertCubeToSigma,
    omni: omni.convertOmniToSigma || omni.default?.convertOmniToSigma,
    alteryx: alteryx.convertAlteryxToSigma || alteryx.default?.convertAlteryxToSigma,
    powerbi: powerbi.convertPowerBIToSigma || powerbi.default?.convertPowerBIToSigma,
  };
}

function shapeSummary(model) {
  const elements = (model.pages || []).flatMap(p => p.elements || []);
  return {
    elements: elements.length,
    columns: elements.reduce((n, e) => n + (e.columns?.length || 0), 0),
    metrics: elements.reduce((n, e) => n + (e.metrics?.length || 0), 0),
    relationships: elements.reduce((n, e) => n + (e.relationships?.length || 0), 0),
    folders: elements.reduce((n, e) => n + (e.folders?.length || 0), 0),
    helperElements: elements.filter(e => e.source?.kind === 'sql').length,
  };
}

function checkAsserts(asserts, summary) {
  const issues = [];
  if (asserts?.minElements != null && summary.elements < asserts.minElements) {
    issues.push(`elements ${summary.elements} < expected min ${asserts.minElements}`);
  }
  if (asserts?.minRelationships != null && summary.relationships < asserts.minRelationships) {
    issues.push(`relationships ${summary.relationships} < expected min ${asserts.minRelationships}`);
  }
  if (asserts?.minHelperElements != null && summary.helperElements < asserts.minHelperElements) {
    issues.push(`helper elements ${summary.helperElements} < expected min ${asserts.minHelperElements}`);
  }
  return issues;
}

// ── Fixture discovery ──────────────────────────────────────────────────────

async function discoverFixtures(filter) {
  const fixtures = [];
  const formats = await readdir(CORPUS);
  for (const fmt of formats) {
    const fmtDir = join(CORPUS, fmt);
    if (!(await stat(fmtDir)).isDirectory()) continue;
    const names = await readdir(fmtDir);
    for (const name of names) {
      const fxDir = join(fmtDir, name);
      if (!(await stat(fxDir)).isDirectory()) continue;
      const id = `${fmt}/${name}`;
      if (filter && id !== filter && fmt !== filter) continue;
      // find input.* file
      const files = await readdir(fxDir);
      const input = files.find(f => f.startsWith('input.'));
      if (!input) continue;
      let summary = {};
      const summaryPath = join(fxDir, 'expected.summary.json');
      if (existsSync(summaryPath)) {
        summary = JSON.parse(await readFile(summaryPath, 'utf-8'));
      }
      fixtures.push({ id, fmt, name, dir: fxDir, inputFile: join(fxDir, input), summary });
    }
  }
  return fixtures;
}

// ── Per-fixture runner ─────────────────────────────────────────────────────

async function runFixture(fx, converters) {
  const log = (s) => console.log(`  ${s}`);
  console.log(`\n── ${fx.id} ──`);
  if (fx.summary.description) log(`description: ${fx.summary.description.slice(0, 200)}`);

  const fn = converters[fx.fmt];
  if (!fn) return { id: fx.id, ok: false, reason: `no converter for format ${fx.fmt}` };

  const xml = await readFile(fx.inputFile, 'utf-8');
  const opts = fx.summary.convertOptions || {};
  // multi-file converters take {name,content}[]
  const fileBased = ['lookml', 'cube', 'omni'];
  // powerbi takes a parsed JSON object (not a raw string)
  const jsonBased = ['powerbi'];
  const arg = fileBased.includes(fx.fmt)
    ? [{ name: basename(fx.inputFile), content: xml }]
    : jsonBased.includes(fx.fmt)
      ? JSON.parse(xml)
      : xml;
  let result;
  try {
    result = fn(arg, opts);
  } catch (e) {
    return { id: fx.id, ok: false, reason: `converter threw: ${e.message}` };
  }
  if (!result?.model) return { id: fx.id, ok: false, reason: 'converter returned no model' };

  const summary = shapeSummary(result.model);
  log(`shape: elements=${summary.elements} cols=${summary.columns} rels=${summary.relationships} helpers=${summary.helperElements}`);

  const assertIssues = checkAsserts(fx.summary.asserts, summary);
  if (assertIssues.length) {
    return { id: fx.id, ok: false, reason: `shape: ${assertIssues.join('; ')}`, summary };
  }

  // POST to Sigma
  const dmName = `REGRESS_${fx.id.replace(/\//g, '_')}_${Date.now()}`;
  const post = await sigmaPost(result.model, dmName);
  if (!post.success) {
    return { id: fx.id, ok: false, reason: `POST failed: ${(post.error || '').slice(0, 250)}`, summary };
  }
  const dmId = post.dataModelId;
  log(`POST ok: ${dmId}`);

  // Error-column scan (HARD GATE)
  let errorCols = [];
  let warning = null;
  try {
    const cols = await sigmaGetColumns(dmId);
    errorCols = (cols.entries || []).filter(c => c?.type && typeof c.type === 'object' && c.type.type === 'error');
  } catch (e) {
    warning = `columns endpoint: ${e.message}`;
  }

  // Cleanup
  await sigmaDelete(dmId);

  if (warning) {
    return { id: fx.id, ok: false, reason: warning, summary };
  }

  if (fx.summary.asserts?.noErrorColumns !== false && errorCols.length > 0) {
    const sample = errorCols.slice(0, 3).map(c => `${c.name}: ${JSON.stringify(c.type)}`).join('; ');
    return { id: fx.id, ok: false, reason: `${errorCols.length} error column(s): ${sample}`, summary, errorCols: errorCols.length };
  }

  log(`error cols: 0 ✓`);
  return { id: fx.id, ok: true, summary, errorCols: 0 };
}

// ── Main ───────────────────────────────────────────────────────────────────

(async () => {
  const filter = process.argv[2] || null;
  const converters = await loadConverters();
  const fixtures = await discoverFixtures(filter);
  if (fixtures.length === 0) {
    console.error(`No fixtures found${filter ? ` matching '${filter}'` : ''} under ${CORPUS}`);
    process.exit(2);
  }

  console.log(`Running ${fixtures.length} fixture(s) against ${SIGMA_BASE_URL}`);
  console.log(`Test folder: ${TEST_FOLDER_ID}`);

  const results = [];
  for (const fx of fixtures) {
    results.push(await runFixture(fx, converters));
  }

  console.log('\n══════════════ RESULTS ══════════════');
  const failures = results.filter(r => !r.ok);
  for (const r of results) {
    const tag = r.ok ? '✅ PASS' : '❌ FAIL';
    console.log(`${tag}  ${r.id}${r.reason ? '  — ' + r.reason : ''}`);
  }
  console.log(`\n${results.length - failures.length}/${results.length} passed`);

  process.exit(failures.length === 0 ? 0 : 1);
})().catch(e => {
  console.error('runner error:', e);
  process.exit(2);
});
