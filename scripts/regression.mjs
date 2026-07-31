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

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Retry only genuinely transient, spec-independent failures: 5xx, 429, and
// network blips. Deliberately do NOT retry 400 "schema error / dependency not
// found" — those are deterministic spec problems (a real converter regression
// produced exactly that), and retrying them would MASK regressions while just
// burning time. A transient infra blip recovers; a bad spec fails every attempt
// and must surface.
function isTransientPostError(status /* , body */) {
  return status >= 500 || status === 429;
}

async function sigmaPost(spec, name, attempts = 3) {
  const body = { ...spec, name, folderId: TEST_FOLDER_ID };
  let last = { success: false, error: 'no attempt made' };
  for (let i = 1; i <= attempts; i++) {
    try {
      const token = await sigmaToken();
      const resp = await fetch(`${SIGMA_BASE_URL}/v2/dataModels/spec`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const txt = await resp.text();
      if (resp.ok) {
        try { return { success: true, dataModelId: JSON.parse(txt).dataModelId }; }
        catch { return { success: false, error: 'non-JSON response: ' + txt.slice(0, 200) }; }
      }
      last = { success: false, status: resp.status, error: txt };
      if (i < attempts && isTransientPostError(resp.status, txt)) {
        console.warn(`  POST attempt ${i}/${attempts} transient (${resp.status}); retrying…`);
        await sleep(2000 * i);
        continue;
      }
      return last;
    } catch (e) {
      last = { success: false, error: `network: ${e.message}` };
      if (i < attempts) { await sleep(2000 * i); continue; }
      return last;
    }
  }
  return last;
}

async function sigmaGetColumns(dataModelId, attempts = 3) {
  // Idempotent read — retry on transient infra (5xx/429, network/connection
  // refused). Observed: a 503 "upstream connect error … Connection refused" on
  // this endpoint failed an otherwise-clean run; the next run passed.
  let lastErr;
  for (let i = 1; i <= attempts; i++) {
    try {
      const token = await sigmaToken();
      const resp = await fetch(`${SIGMA_BASE_URL}/v2/dataModels/${dataModelId}/columns`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (resp.ok) return resp.json();
      const body = await resp.text();
      lastErr = new Error(`columns ${resp.status}: ${body}`);
      if (i < attempts && isTransientPostError(resp.status)) { await sleep(2000 * i); continue; }
      throw lastErr;
    } catch (e) {
      lastErr = e;
      if (i < attempts) { await sleep(2000 * i); continue; }
      throw lastErr;
    }
  }
  throw lastErr;
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
  const qlik = await import(join(BUILD_DIR, 'qlik.js'));
  const thoughtspot = await import(join(BUILD_DIR, 'thoughtspot.js'));
  const oac = await import(join(BUILD_DIR, 'oac.js'));
  const quicksight = await import(join(BUILD_DIR, 'quicksight.js'));
  const bobj = await import(join(BUILD_DIR, 'bobj.js'));
  return {
    bobj: bobj.convertBobjToSigma || bobj.default?.convertBobjToSigma,
    tableau: tableau.convertTableauToSigma || tableau.default?.convertTableauToSigma,
    lookml: lookml.convertLookMLToSigma || lookml.default?.convertLookMLToSigma,
    dbt: dbt.convertDbtToSigma || dbt.default?.convertDbtToSigma,
    cube: cube.convertCubeToSigma || cube.default?.convertCubeToSigma,
    omni: omni.convertOmniToSigma || omni.default?.convertOmniToSigma,
    alteryx: alteryx.convertAlteryxToSigma || alteryx.default?.convertAlteryxToSigma,
    powerbi: powerbi.convertPowerBIToSigma || powerbi.default?.convertPowerBIToSigma,
    qlik: qlik.convertQlikToSigma || qlik.default?.convertQlikToSigma,
    qvw: qlik.convertQvwPrjToSigma || qlik.default?.convertQvwPrjToSigma,
    thoughtspot: thoughtspot.convertThoughtSpotToSigma || thoughtspot.default?.convertThoughtSpotToSigma,
    oac: oac.convertOacToSigma || oac.default?.convertOacToSigma,
    quicksight: quicksight.convertQuickSightToSigma || quicksight.default?.convertQuickSightToSigma,
  };
}

function shapeSummary(model, result = {}) {
  const elements = (model.pages || []).flatMap(p => p.elements || []);
  const sec = result.security || [];
  // Doubled-bracket column-ref scan: `[[Foo]]` / `[[Foo] [Bar]]` is never
  // valid Sigma formula syntax — it's the signature of a bracket-identifier
  // rewrite pass running TWICE over the same span (each aggregate-arg call
  // plus a later whole-string pass, or two independent mask/restore cycles
  // colliding). Live-reproduced (thoughtspot.ts, beads-sigma cross-element
  // fixture): `sum(net_revenue)` came back as `Sum([[Net] [Revenue]])` — a
  // 200-OK POST with no error column (the two split refs each happened to
  // resolve), so `noErrorColumns` alone did not catch it. Scan ALL formulas
  // across every element's columns/metrics, not just this one fixture's —
  // the double-bracket defect class isn't converter-specific.
  const allFormulas = elements.flatMap(e => [
    ...(e.columns || []).map(c => c.formula),
    ...(e.metrics || []).map(m => m.formula),
  ]).filter(f => typeof f === 'string');
  const doubledBracketFormulas = allFormulas.filter(f => /\[\s*\[/.test(f));
  return {
    // Architecture B: RLS/CLS is REPORTED in result.security, not injected into
    // the model. So assert on these counts (minSecurity/minRlsRules/minClsRules),
    // not minFilters/minRlsColumns (which stay 0 for reported-but-not-injected RLS).
    security: sec.length,
    rlsRules: sec.filter(s => s.kind === 'rls').length,
    clsRules: sec.filter(s => s.kind === 'cls').length,
    // Chart-context window calcs reported for the workbook builder (NOT in the
    // model) — MCP-runner-only assert; the browser runner only sees the model
    // JSON, so minWorkbookPatterns is ignored there.
    workbookPatterns: (result.workbookPatterns || []).length,
    elements: elements.length,
    columns: elements.reduce((n, e) => n + (e.columns?.length || 0), 0),
    metrics: elements.reduce((n, e) => n + (e.metrics?.length || 0), 0),
    relationships: elements.reduce((n, e) => n + (e.relationships?.length || 0), 0),
    folders: elements.reduce((n, e) => n + (e.folders?.length || 0), 0),
    helperElements: elements.filter(e => e.source?.kind === 'sql').length,
    filters: elements.reduce((n, e) => n + (e.filters?.length || 0), 0),
    rlsColumns: elements.reduce(
      (n, e) => n + (e.columns?.filter(c => /CurrentUserAttribute(Text|Number)?\s*\(/.test(c.formula || '')).length || 0),
      0
    ),
    doubledBracketFormulas: doubledBracketFormulas.length,
    doubledBracketSample: doubledBracketFormulas.slice(0, 3),
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
  if (asserts?.minFilters != null && summary.filters < asserts.minFilters) {
    issues.push(`filters ${summary.filters} < expected min ${asserts.minFilters}`);
  }
  if (asserts?.minRlsColumns != null && summary.rlsColumns < asserts.minRlsColumns) {
    issues.push(`RLS columns ${summary.rlsColumns} < expected min ${asserts.minRlsColumns}`);
  }
  if (asserts?.minSecurity != null && summary.security < asserts.minSecurity) {
    issues.push(`reported security rules ${summary.security} < expected min ${asserts.minSecurity}`);
  }
  if (asserts?.minRlsRules != null && summary.rlsRules < asserts.minRlsRules) {
    issues.push(`reported RLS rules ${summary.rlsRules} < expected min ${asserts.minRlsRules}`);
  }
  if (asserts?.minClsRules != null && summary.clsRules < asserts.minClsRules) {
    issues.push(`reported CLS rules ${summary.clsRules} < expected min ${asserts.minClsRules}`);
  }
  if (asserts?.minWorkbookPatterns != null && summary.workbookPatterns != null
      && summary.workbookPatterns < asserts.minWorkbookPatterns) {
    issues.push(`reported workbook patterns ${summary.workbookPatterns} < expected min ${asserts.minWorkbookPatterns}`);
  }
  // Default-on (mirrors noErrorColumns) — a formula with a doubled bracket
  // ref (`[[Foo]]`) is never valid Sigma syntax; see shapeSummary's comment.
  if (asserts?.noDoubledBracketRefs !== false && summary.doubledBracketFormulas > 0) {
    issues.push(`${summary.doubledBracketFormulas} formula(s) with a doubled bracket ref (e.g. "[[...]]"): ${JSON.stringify(summary.doubledBracketSample)}`);
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
      // find input file(s). Single-file: input.*  Multi-file: any non-summary file.
      const files = await readdir(fxDir);
      const single = files.find(f => f.startsWith('input.'));
      const multi = ['lookml', 'cube', 'omni', 'quicksight', 'qvw'].includes(fmt) && !single
        ? files.filter(f => f !== 'expected.summary.json' && !f.startsWith('expected.') && !f.startsWith('.'))
        : null;
      if (!single && (!multi || multi.length === 0)) continue;
      let summary = {};
      const summaryPath = join(fxDir, 'expected.summary.json');
      if (existsSync(summaryPath)) {
        summary = JSON.parse(await readFile(summaryPath, 'utf-8'));
      }
      fixtures.push({
        id, fmt, name, dir: fxDir,
        inputFile: single ? join(fxDir, single) : null,
        inputFiles: multi ? multi.map(f => join(fxDir, f)) : null,
        summary,
      });
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
  if (!fn) return { id: fx.id, ok: true, skipped: true, reason: `no MCP converter for format ${fx.fmt} (browser-only fixture — covered by regression-browser)` };

  const opts = fx.summary.convertOptions || {};
  // multi-file converters take {name,content}[]
  const fileBased = ['lookml', 'cube', 'omni', 'quicksight', 'qvw'];
  // powerbi/qlik take a parsed JSON object; oac takes a parsed JSON array.
  // JSON.parse handles both shapes, so a single list is sufficient.
  const jsonBased = ['powerbi', 'qlik', 'oac', 'bobj'];
  let arg;
  if (fx.inputFiles && fx.inputFiles.length) {
    const parts = await Promise.all(fx.inputFiles.map(async fp => ({
      name: basename(fp), content: await readFile(fp, 'utf-8'),
    })));
    arg = fileBased.includes(fx.fmt) ? parts : parts.map(p => p.content).join('\n');
  } else {
    const raw = await readFile(fx.inputFile, 'utf-8');
    // jsonBased formats take a parsed object (incl. JSON-content files with other
    // extensions, e.g. powerbi input.bim) — but an .xml input (the bobj SL-SDK
    // fixture) is passed as a raw string for the converter to auto-detect.
    const isXmlFile = fx.inputFile.endsWith('.xml');
    arg = fileBased.includes(fx.fmt)
      ? [{ name: basename(fx.inputFile), content: raw }]
      : jsonBased.includes(fx.fmt)
        ? (isXmlFile ? raw : JSON.parse(raw))
        : raw;
  }
  let result;
  try {
    result = fn(arg, opts);
  } catch (e) {
    return { id: fx.id, ok: false, reason: `converter threw: ${e.message}` };
  }
  if (!result?.model) return { id: fx.id, ok: false, reason: 'converter returned no model' };

  const summary = shapeSummary(result.model, result);
  log(`shape: elements=${summary.elements} cols=${summary.columns} rels=${summary.relationships} helpers=${summary.helperElements} security=${summary.security}`);

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
  const skipped = results.filter(r => r.skipped);
  for (const r of results) {
    const tag = r.skipped ? '⏭  SKIP' : r.ok ? '✅ PASS' : '❌ FAIL';
    console.log(`${tag}  ${r.id}${r.reason ? '  — ' + r.reason : ''}`);
  }
  const passed = results.length - failures.length - skipped.length;
  console.log(`\n${passed} passed, ${skipped.length} skipped, ${failures.length} failed`);

  process.exit(failures.length === 0 ? 0 : 1);
})().catch(e => {
  console.error('runner error:', e);
  process.exit(2);
});
