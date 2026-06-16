#!/usr/bin/env node
// Puppeteer-driven regression runner against the smm browser tool
// (sigma-data-model-manager/index.html). Walks regression-corpus/<format>/<name>/,
// drives the appropriate converter UI in a headless browser, captures the
// converted JSON output, POSTs it to Sigma's test folder, asserts shape +
// zero-error-columns, and cleans up.
//
// This is the v2 surface for the regression corpus: every fixture should pass
// against BOTH the MCP runner (`npm run regression`) and this browser runner
// (`npm run regression:browser`). Either surface may have its own bugs and
// each must be gated independently.
//
// Required env vars: SIGMA_BASE_URL, SIGMA_CLIENT_ID, SIGMA_CLIENT_SECRET.
// Optional: SIGMA_TEST_FOLDER_ID (default: 9ca9bf60-6a33-43dd-967d-1ba6352c54bb),
//           SIGMA_TEST_CONNECTION_ID (default: cb2f5180-641f-47bd-8efa-da9d590d855a).
//
// Usage:
//   npm run regression:browser                  # run everything
//   npm run regression:browser -- tableau       # one format
//   npm run regression:browser -- tableau/lod_basic  # one fixture

import { readFile, readdir, stat } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { createRequire } from 'node:module';
import { join, dirname, basename, resolve } from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';

// Path resolution for the smm browser tool (index.html) and puppeteer.
// Order of precedence:
//   1. SMM_INDEX_PATH env var (filesystem path) — used in CI
//   2. Default: ~/sigma-data-model-manager/index.html — local dev convenience
//
//   1. SMM_PUPPETEER_PATH env var (filesystem path to puppeteer.js entry)
//   2. require.resolve('puppeteer') — works when puppeteer is a normal dep
//   3. Default: tableau-local node_modules path — local dev convenience
const DEFAULT_SMM_INDEX     = '/Users/tjwells/sigma-data-model-manager/index.html';
const DEFAULT_PUPPETEER     = '/Users/tjwells/sigma-data-model-manager/tableau-local/node_modules/puppeteer/lib/esm/puppeteer/puppeteer.js';

function resolveSmmHtml() {
  const p = process.env.SMM_INDEX_PATH || DEFAULT_SMM_INDEX;
  return pathToFileURL(p).href;
}

function resolvePuppeteerPath() {
  if (process.env.SMM_PUPPETEER_PATH) return process.env.SMM_PUPPETEER_PATH;
  try {
    const require = createRequire(import.meta.url);
    return require.resolve('puppeteer');
  } catch {
    return DEFAULT_PUPPETEER;
  }
}

const PUPPETEER_PATH = resolvePuppeteerPath();
const SMM_HTML = resolveSmmHtml();

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT      = resolve(__dirname, '..');
const CORPUS    = join(ROOT, 'regression-corpus');

const SIGMA_BASE_URL  = process.env.SIGMA_BASE_URL;
const SIGMA_CLIENT_ID = process.env.SIGMA_CLIENT_ID;
const SIGMA_SECRET    = process.env.SIGMA_CLIENT_SECRET;
const TEST_FOLDER_ID  = process.env.SIGMA_TEST_FOLDER_ID || '9ca9bf60-6a33-43dd-967d-1ba6352c54bb';
const TEST_CONN_ID    = process.env.SIGMA_TEST_CONNECTION_ID || 'cb2f5180-641f-47bd-8efa-da9d590d855a';

if (!SIGMA_BASE_URL || !SIGMA_CLIENT_ID || !SIGMA_SECRET) {
  console.error('FAIL: SIGMA_BASE_URL, SIGMA_CLIENT_ID, SIGMA_CLIENT_SECRET must be set in env.');
  process.exit(2);
}

// Fixture format dir -> smm converter key (the converterFormat <select> values).
const FMT_TO_KEY = {
  alteryx:     'alteryx',
  cube:        'cube',
  dbt:         'dbt',
  lookml:      'look',
  oac:         'oac',
  omni:        'omni',
  powerbi:     'pbi',
  qlik:        'qlik',
  prep:        'prep',
  tableau:     'tableau',
  thoughtspot: 'thoughtspot',
  quicksight:  'quicksight',
  bobj:        'bobj',
  // Not yet covered by fixtures: contract (atlan), snow, sql, ssas
};

// ── Sigma API helpers (mirrors regression.mjs) ────────────────────────────

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

// ── Fixture discovery (mirror regression.mjs) ─────────────────────────────

async function discoverFixtures(filter) {
  const fixtures = [];
  const formats = await readdir(CORPUS);
  for (const fmt of formats) {
    const fmtDir = join(CORPUS, fmt);
    let st;
    try { st = await stat(fmtDir); } catch { continue; }
    if (!st.isDirectory()) continue;
    const names = await readdir(fmtDir);
    for (const name of names) {
      const fxDir = join(fmtDir, name);
      let st2;
      try { st2 = await stat(fxDir); } catch { continue; }
      if (!st2.isDirectory()) continue;
      const id = `${fmt}/${name}`;
      if (filter && id !== filter && fmt !== filter) continue;
      const files = await readdir(fxDir);
      const single = files.find(f => f.startsWith('input.'));
      const multi = ['lookml', 'cube', 'omni', 'quicksight'].includes(fmt) && !single
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

function shapeSummary(model) {
  const elements = (model.pages || []).flatMap(p => p.elements || []);
  return {
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
  return issues;
}

// ── Browser-side helpers (run in page context) ────────────────────────────

// Set the value of a connection <select>. Defensively appendChild a real
// <option> first because populateConverterConnections may not yet have run,
// in which case `.value = id` silently no-ops and the converter sends the
// literal placeholder string '<CONNECTION_ID>'. See feedback_e2e_ui_tests_required.
function setSelectValueScript() {
  return `(selId, value) => {
    const sel = document.getElementById(selId);
    if (!sel) return false;
    if (![...sel.options].some(o => o.value === value)) {
      const opt = document.createElement('option');
      opt.value = value;
      opt.textContent = value;
      sel.appendChild(opt);
    }
    sel.value = value;
    sel.dispatchEvent(new Event('change'));
    return sel.value === value;
  }`;
}

// Switch converter tab.
async function switchTab(page, key) {
  await page.evaluate((k) => {
    const sel = document.getElementById('converterFormat');
    sel.value = k;
    sel.dispatchEvent(new Event('change'));
    if (typeof switchConverter === 'function') switchConverter(k);
  }, key);
  // brief settle
  await new Promise(r => setTimeout(r, 200));
}

// Wait for converter connections to populate.
async function waitForConnections(page, timeoutMs = 15000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const ready = await page.evaluate(() => {
      const sel = document.getElementById('tableauConnectionId');
      if (!sel) return false;
      // either has real connection options OR access token is set
      return sel.options.length > 1 || !!(window.state && window.state.accessToken);
    });
    if (ready) return true;
    await new Promise(r => setTimeout(r, 500));
  }
  return false;
}

// ── Per-converter driving logic ───────────────────────────────────────────
//
// Each driver:
//  - prepares input in the page (paste textarea OR ingest function call OR DataTransfer file input)
//  - sets connection / database / schema overrides
//  - calls the run<X>Conversion() handler
//  - reads the JSON output element
//  - returns { ok, model, error }

async function driveTableau(page, fx, opts) {
  const xml = await readFile(fx.inputFile, 'utf-8');
  const result = await page.evaluate(async (xmlStr, fname, connId, db, schema) => {
    try {
      // Reset prior state if any.
      if (typeof clearTableauFiles === 'function') clearTableauFiles();
      window.ingestTableauXml(xmlStr, fname);
    } catch (e) { return { ok: false, error: 'ingest: ' + e.message }; }
    // Set connection
    const sel = document.getElementById('tableauConnectionId');
    if (sel) {
      if (![...sel.options].some(o => o.value === connId)) {
        const opt = document.createElement('option');
        opt.value = connId; opt.textContent = connId;
        sel.appendChild(opt);
      }
      sel.value = connId;
    }
    if (db) document.getElementById('tableauDatabase').value = db;
    if (schema) document.getElementById('tableauSchema').value = schema;
    try {
      runTableauConversion();
    } catch (e) { return { ok: false, error: 'run: ' + e.message }; }
    return { ok: true };
  }, xml, basename(fx.inputFile), opts.connectionId || '<CONNECTION_ID>', opts.database || '', opts.schema || '');
  if (!result.ok) return result;
  return readOutput(page, 'tableauJsonOutput');
}

async function driveAlteryx(page, fx, opts) {
  const xml = await readFile(fx.inputFile, 'utf-8');
  const result = await page.evaluate(async (xmlStr, fname, connId, db, schema) => {
    try {
      if (window._alteryxParsed) window._alteryxParsed = null;
      window.ingestAlteryxXml(xmlStr, fname);
    } catch (e) { return { ok: false, error: 'ingest: ' + e.message }; }
    const sel = document.getElementById('alteryxConnectionId');
    if (sel) {
      if (![...sel.options].some(o => o.value === connId)) {
        const opt = document.createElement('option');
        opt.value = connId; opt.textContent = connId;
        sel.appendChild(opt);
      }
      sel.value = connId;
    }
    if (db) document.getElementById('alteryxDatabase').value = db;
    if (schema) document.getElementById('alteryxSchema').value = schema;
    try {
      runAlteryxConversion();
    } catch (e) { return { ok: false, error: 'run: ' + e.message }; }
    return { ok: true };
  }, xml, basename(fx.inputFile), opts.connectionId || '<CONNECTION_ID>', opts.database || '', opts.schema || '');
  if (!result.ok) return result;
  return readOutput(page, 'alteryxJsonOutput');
}

// Generic paste-textarea + run-handler driver
async function drivePaste(page, fx, opts, cfg) {
  const text = await readFile(fx.inputFile, 'utf-8');
  const result = await page.evaluate(async (txt, connId, db, schema, c) => {
    const ta = document.getElementById(c.inputId);
    if (!ta) return { ok: false, error: 'no input element ' + c.inputId };
    ta.value = txt;
    ta.dispatchEvent(new Event('input'));
    if (c.connId) {
      const sel = document.getElementById(c.connId);
      if (sel) {
        if (![...sel.options].some(o => o.value === connId)) {
          const opt = document.createElement('option');
          opt.value = connId; opt.textContent = connId;
          sel.appendChild(opt);
        }
        sel.value = connId;
      }
    }
    if (c.dbId && db) document.getElementById(c.dbId).value = db;
    if (c.schemaId && schema) document.getElementById(c.schemaId).value = schema;
    // optional target-layer remap (e.g. bobj tableMap/columnMap) — must be staged
    // in its textarea BEFORE the run so the converter reads it.
    if (c.remapId && c.remap) {
      const r = document.getElementById(c.remapId);
      if (r) { r.value = c.remap; r.dispatchEvent(new Event('input')); }
    }
    try {
      // reset any cached "_qlikModel" / "_pbiModel" etc. so paste-driven path is taken
      if (c.resetGlobal) window[c.resetGlobal] = null;
      window[c.runFn]();
    } catch (e) { return { ok: false, error: 'run: ' + e.message }; }
    return { ok: true };
  }, text, opts.connectionId || '<CONNECTION_ID>', opts.database || '', opts.schema || '', cfg);
  if (!result.ok) return result;
  return readOutput(page, cfg.outputId);
}

// Drive lookml: load file via DataTransfer to populate explores, then pick first explore
async function driveLookML(page, fx, opts) {
  const inputs = fx.inputFiles
    ? await Promise.all(fx.inputFiles.map(async fp => ({ name: basename(fp), content: await readFile(fp, 'utf-8') })))
    : [{ name: basename(fx.inputFile), content: await readFile(fx.inputFile, 'utf-8') }];

  // Ingest files
  await page.evaluate(async (files) => {
    // Reset
    window.lookProject = { modelName: '', explores: {}, views: {}, includes: [] };
    if (typeof renderLookExplores === 'function') renderLookExplores();
    const fileObjs = files.map(f => new File([f.content], f.name, { type: 'text/plain' }));
    await window.ingestLookFiles(fileObjs);
  }, inputs);

  await new Promise(r => setTimeout(r, 300));

  // Set explore + connection + db/schema, then run
  const result = await page.evaluate(async (connId, db, schema, wantExplore) => {
    const expSel = document.getElementById('lookExploreSelect');
    // Honor convertOptions.exploreName when supplied; otherwise pick the first
    // real option (matches the MCP runner's single-explore default).
    const opts = expSel ? [...expSel.options].filter(o => o.value) : [];
    const realOpt = (wantExplore && opts.find(o => o.value === wantExplore)) || opts[0];
    if (!realOpt) {
      return { ok: false, error: 'no explores parsed from LookML' };
    }
    expSel.value = realOpt.value;
    expSel.dispatchEvent(new Event('change'));
    const sel = document.getElementById('lookConnectionId');
    if (sel) {
      if (![...sel.options].some(o => o.value === connId)) {
        const opt = document.createElement('option');
        opt.value = connId; opt.textContent = connId;
        sel.appendChild(opt);
      }
      sel.value = connId;
    }
    // lookml has no db/schema input fields (uses sql_table_name)
    try {
      await runLookConversion();
    } catch (e) { return { ok: false, error: 'run: ' + e.message }; }
    return { ok: true };
  }, opts.connectionId || '<CONNECTION_ID>', opts.database || '', opts.schema || '', opts.exploreName || '');
  if (!result.ok) return result;
  // Look conversion is async — wait briefly for output
  for (let i = 0; i < 50; i++) {
    const v = await page.evaluate(() => document.getElementById('lookJsonOutput')?.value || '');
    if (v && v.length > 50 && !v.startsWith('//')) break;
    await new Promise(r => setTimeout(r, 200));
  }
  return readOutput(page, 'lookJsonOutput');
}

async function driveOmni(page, fx, opts) {
  const inputs = fx.inputFiles
    ? await Promise.all(fx.inputFiles.map(async fp => ({ name: basename(fp), content: await readFile(fp, 'utf-8') })))
    : [{ name: basename(fx.inputFile), content: await readFile(fx.inputFile, 'utf-8') }];

  await page.evaluate(async (files) => {
    // Reset prior state
    window._omniViews = []; window._omniExplores = [];
    const fileObjs = files.map(f => new File([f.content], f.name, { type: 'text/yaml' }));
    await window.processOmniFiles(fileObjs);
  }, inputs);

  // processOmniFiles auto-runs; now set connection and re-run with overrides
  await new Promise(r => setTimeout(r, 300));
  const result = await page.evaluate(async (connId, db, schema) => {
    const sel = document.getElementById('omniConnectionId');
    if (sel) {
      if (![...sel.options].some(o => o.value === connId)) {
        const opt = document.createElement('option');
        opt.value = connId; opt.textContent = connId;
        sel.appendChild(opt);
      }
      sel.value = connId;
    }
    if (db) document.getElementById('omniDatabase').value = db;
    if (schema) document.getElementById('omniSchema').value = schema;
    try {
      runOmniConversion();
    } catch (e) { return { ok: false, error: 'run: ' + e.message }; }
    return { ok: true };
  }, opts.connectionId || '<CONNECTION_ID>', opts.database || '', opts.schema || '');
  if (!result.ok) return result;
  return readOutput(page, 'omniJsonOutput');
}

async function driveCube(page, fx, opts) {
  // Cube fixture is single yml file, paste into cubeInput textarea
  return drivePaste(page, fx, opts, {
    inputId: 'cubeInput',
    connId: 'cubeConnectionId',
    dbId: 'cubeDatabase',
    schemaId: 'cubeSchema',
    runFn: 'runCubeConversion',
    outputId: 'cubeJsonOutput',
  });
}

async function driveDbt(page, fx, opts) {
  return drivePaste(page, fx, opts, {
    inputId: 'dbtYamlInput',
    connId: 'dbtConnectionId',
    dbId: 'dbtDatabase',
    schemaId: 'dbtSchema',
    runFn: 'runDbtConversion',
    outputId: 'dbtJsonOutput',
  });
}

async function driveThoughtSpot(page, fx, opts) {
  return drivePaste(page, fx, opts, {
    inputId: 'tsYamlInput',
    connId: 'tsConnectionId',
    dbId: 'tsDatabase',
    schemaId: 'tsSchema',
    runFn: 'runThoughtSpotConversion',
    outputId: 'tsJsonOutput',
  });
}

async function driveBobj(page, fx, opts) {
  // bobj fixture is a single RWS universe JSON, pasted into the bobjJsonInput textarea.
  // Stage the fixture's target-layer remap (tableMap/columnMap) into #bobjRemapJson
  // so a restructured-universe fixture binds to real warehouse tables (mirrors the
  // MCP convertOptions). Without it the unmapped physical names (e.g. CUST_DIM_DE)
  // POST as "Source not found".
  const remap = {};
  if (opts.tableMap) remap.tableMap = opts.tableMap;
  if (opts.columnMap) remap.columnMap = opts.columnMap;
  return drivePaste(page, fx, opts, {
    inputId: 'bobjJsonInput',
    connId: 'bobjConnectionId',
    dbId: 'bobjDatabase',
    schemaId: 'bobjSchema',
    runFn: 'runBobjConversion',
    outputId: 'bobjJsonOutput',
    remapId: 'bobjRemapJson',
    remap: Object.keys(remap).length ? JSON.stringify(remap) : '',
  });
}

async function driveQuickSight(page, fx, opts) {
  // Multi-file: paste each JSON separated by `---` markers (matches the
  // textarea split-and-parse path in runQuickSightConversion).
  const inputs = fx.inputFiles
    ? await Promise.all(fx.inputFiles.map(async fp => ({ name: basename(fp), content: await readFile(fp, 'utf-8') })))
    : [{ name: basename(fx.inputFile), content: await readFile(fx.inputFile, 'utf-8') }];

  const result = await page.evaluate(async (files, connId, db, schema) => {
    // Reset any previously loaded files
    if (window._qsRawFiles) window._qsRawFiles.length = 0;
    document.getElementById('qsFileList').innerHTML = '';
    // Stage the textarea
    document.getElementById('qsJsonInput').value = files
      .map(f => `// ${f.name}\n${f.content}`)
      .join('\n---\n');
    const sel = document.getElementById('qsConnectionId');
    if (sel) {
      if (![...sel.options].some(o => o.value === connId)) {
        const opt = document.createElement('option');
        opt.value = connId; opt.textContent = connId;
        sel.appendChild(opt);
      }
      sel.value = connId;
    }
    if (db) document.getElementById('qsDatabase').value = db;
    if (schema) document.getElementById('qsSchema').value = schema;
    try {
      runQuickSightConversion();
    } catch (e) { return { ok: false, error: 'run: ' + e.message }; }
    return { ok: true };
  }, inputs, opts.connectionId || '<CONNECTION_ID>', opts.database || '', opts.schema || '');
  if (!result.ok) return result;
  return readOutput(page, 'qsJsonOutput');
}

async function driveQlik(page, fx, opts) {
  return drivePaste(page, fx, opts, {
    inputId: 'qlikJsonInput',
    connId: 'qlikConnectionId',
    dbId: 'qlikDatabase',
    schemaId: 'qlikSchema',
    runFn: 'runQlikConversion',
    outputId: 'qlikJsonOutput',
    resetGlobal: '_qlikModel',
  });
}

async function drivePbi(page, fx, opts) {
  // .bim file — load via DataTransfer to #pbiFileInput
  const text = await readFile(fx.inputFile, 'utf-8');
  await page.evaluate(async (txt, fname) => {
    const file = new File([txt], fname, { type: 'application/json' });
    await window.processPbiFile(file);
  }, text, basename(fx.inputFile));

  await new Promise(r => setTimeout(r, 300));
  const result = await page.evaluate(async (connId, db, schema) => {
    const sel = document.getElementById('pbiConnectionId');
    if (sel) {
      if (![...sel.options].some(o => o.value === connId)) {
        const opt = document.createElement('option');
        opt.value = connId; opt.textContent = connId;
        sel.appendChild(opt);
      }
      sel.value = connId;
    }
    if (db) document.getElementById('pbiDatabase').value = db;
    if (schema) document.getElementById('pbiSchema').value = schema;
    try {
      runPbiConversion();
    } catch (e) { return { ok: false, error: 'run: ' + e.message }; }
    return { ok: true };
  }, opts.connectionId || '<CONNECTION_ID>', opts.database || '', opts.schema || '');
  if (!result.ok) return result;
  return readOutput(page, 'pbiJsonOutput');
}

async function driveOac(page, fx, opts) {
  // OAC fixture: single JSON file with array of logical tables. Browser
  // tool's processOacFiles expects each file to be ONE logical table
  // (with a top-level logicalColumns array) OR a zip. Split the array into
  // one File per element to match.
  const txt = await readFile(fx.inputFile, 'utf-8');
  let arr;
  try { arr = JSON.parse(txt); } catch (e) { return { ok: false, error: 'JSON parse: ' + e.message }; }
  if (!Array.isArray(arr)) {
    // Single object — pass through as one file
    arr = [arr];
  }

  await page.evaluate(async (logicalTables) => {
    window._oacParsed = null;
    const files = logicalTables.map((lt, i) => {
      const content = JSON.stringify(lt);
      const name = (lt.name || ('logical_' + i)).replace(/\s+/g, '_') + '.json';
      return new File([content], name, { type: 'application/json' });
    });
    await window.processOacFiles(files);
  }, arr);

  await new Promise(r => setTimeout(r, 300));
  const result = await page.evaluate(async (connId, db, schema) => {
    const modelSel = document.getElementById('oacModelSelect');
    if (modelSel && modelSel.options.length) {
      modelSel.value = modelSel.options[0].value;
      modelSel.dispatchEvent(new Event('change'));
    }
    const sel = document.getElementById('oacConnectionId');
    if (sel) {
      if (![...sel.options].some(o => o.value === connId)) {
        const opt = document.createElement('option');
        opt.value = connId; opt.textContent = connId;
        sel.appendChild(opt);
      }
      sel.value = connId;
    }
    if (db) document.getElementById('oacDatabase').value = db;
    if (schema) document.getElementById('oacSchema').value = schema;
    try {
      runOacConversion();
    } catch (e) { return { ok: false, error: 'run: ' + e.message }; }
    return { ok: true };
  }, opts.connectionId || '<CONNECTION_ID>', opts.database || '', opts.schema || '');
  if (!result.ok) return result;
  return readOutput(page, 'oacJsonOutput');
}

// Drive Tableau Prep: load raw flow JSON via loadPrepFile (handles .json and .tfl
// inputs the same way), set connection/db/schema, then call runPrepConversion.
async function drivePrep(page, fx, opts) {
  const text = await readFile(fx.inputFile, 'utf-8');
  const fname = basename(fx.inputFile);
  const result = await page.evaluate(async (txt, fileName, connId, db, schema) => {
    try {
      // Reset prior state across runs.
      if (typeof _prepFlowJson !== 'undefined') { try { _prepFlowJson = null; } catch {} }
      if (typeof _prepTdsFiles !== 'undefined') { try { _prepTdsFiles.length = 0; } catch {} }
      // loadPrepFile inspects file.name extension. .flow.json matches the .json branch
      // and reads via file.text() — exactly what we want for raw flow JSON.
      const file = new File([txt], fileName, { type: 'application/json' });
      await window.loadPrepFile(file, /*deferConvert*/ true);
    } catch (e) { return { ok: false, error: 'ingest: ' + e.message }; }
    const sel = document.getElementById('prepConnectionId');
    if (sel) {
      if (![...sel.options].some(o => o.value === connId)) {
        const opt = document.createElement('option');
        opt.value = connId; opt.textContent = connId;
        sel.appendChild(opt);
      }
      sel.value = connId;
    }
    if (db) document.getElementById('prepDatabase').value = db;
    if (schema) document.getElementById('prepSchema').value = schema;
    try {
      window.runPrepConversion();
    } catch (e) { return { ok: false, error: 'run: ' + e.message }; }
    return { ok: true };
  }, text, fname, opts.connectionId || '<CONNECTION_ID>', opts.database || '', opts.schema || '');
  if (!result.ok) return result;
  return readOutput(page, 'prepJsonOutput');
}

// Read the output textarea, parse JSON.
async function readOutput(page, outputId) {
  const v = await page.evaluate((id) => document.getElementById(id)?.value || '', outputId);
  if (!v) return { ok: false, error: 'output empty' };
  if (v.startsWith('//')) return { ok: false, error: v.split('\n').slice(0, 3).join(' | ') };
  try {
    return { ok: true, model: JSON.parse(v) };
  } catch (e) {
    return { ok: false, error: 'output JSON parse: ' + e.message + ' / first 200: ' + v.slice(0, 200) };
  }
}

const DRIVERS = {
  tableau:     driveTableau,
  alteryx:     driveAlteryx,
  cube:        driveCube,
  dbt:         driveDbt,
  lookml:      driveLookML,
  oac:         driveOac,
  omni:        driveOmni,
  powerbi:     drivePbi,
  prep:        drivePrep,
  qlik:        driveQlik,
  thoughtspot: driveThoughtSpot,
  quicksight:  driveQuickSight,
  bobj:        driveBobj,
};

// ── Per-fixture runner ────────────────────────────────────────────────────

async function runFixture(page, fx) {
  const log = (s) => console.log(`  ${s}`);
  console.log(`\n── ${fx.id} ──`);
  if (fx.summary.description) log(`description: ${fx.summary.description.slice(0, 200)}`);

  const driver = DRIVERS[fx.fmt];
  if (!driver) {
    // MCP-only converters (e.g. bobj) have no browser tab — skip, don't fail.
    return { id: fx.id, ok: true, skipped: true, reason: `no browser driver for format ${fx.fmt} (skipped — not yet supported)` };
  }
  const key = FMT_TO_KEY[fx.fmt];
  await switchTab(page, key);

  // Capture page errors during this fixture
  const pageErrors = [];
  const onErr = (e) => pageErrors.push(e.message || String(e));
  page.on('pageerror', onErr);

  let result;
  try {
    result = await driver(page, fx, fx.summary.convertOptions || {});
  } catch (e) {
    page.off('pageerror', onErr);
    return { id: fx.id, ok: false, reason: `driver threw: ${e.message}` };
  }
  page.off('pageerror', onErr);

  if (!result.ok) {
    const extra = pageErrors.length ? ` | pageErrors: ${pageErrors.slice(0, 2).join('; ')}` : '';
    return { id: fx.id, ok: false, reason: `convert: ${result.error}${extra}` };
  }
  if (!result.model) {
    return { id: fx.id, ok: false, reason: 'no model produced' };
  }

  const summary = shapeSummary(result.model);
  log(`shape: elements=${summary.elements} cols=${summary.columns} rels=${summary.relationships} helpers=${summary.helperElements}`);

  const assertIssues = checkAsserts(fx.summary.asserts, summary);
  if (assertIssues.length) {
    return { id: fx.id, ok: false, reason: `shape: ${assertIssues.join('; ')}`, summary };
  }

  // POST to Sigma
  const dmName = `REGRESS_BROWSER_${fx.id.replace(/\//g, '_')}_${Date.now()}`;
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

// ── Main ──────────────────────────────────────────────────────────────────

(async () => {
  const filter = process.argv[2] || null;

  const fixtures = await discoverFixtures(filter);
  if (fixtures.length === 0) {
    console.error(`No fixtures found${filter ? ` matching '${filter}'` : ''} under ${CORPUS}`);
    process.exit(2);
  }

  console.log(`Running ${fixtures.length} fixture(s) against ${SIGMA_BASE_URL} (smm browser tool)`);
  console.log(`Test folder: ${TEST_FOLDER_ID}`);
  console.log(`Test connection: ${TEST_CONN_ID}`);

  // Launch puppeteer. Accept either a bare module specifier ('puppeteer')
  // or an absolute filesystem path (legacy local-dev convenience).
  const puppeteerSpec = PUPPETEER_PATH.startsWith('/')
    ? pathToFileURL(PUPPETEER_PATH).href
    : PUPPETEER_PATH;
  const puppeteer = (await import(puppeteerSpec)).default;
  const browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox'] });
  const page = await browser.newPage();
  await page.setViewport({ width: 1400, height: 900 });

  // forward important console messages
  page.on('console', msg => {
    const t = msg.text();
    // only surface obvious failures
    if (/converter|parse error|Sigma|❌/i.test(t) && !t.includes('phTrack')) {
      // process.stderr.write(`[page] ${t}\n`);
    }
  });
  page.on('pageerror', err => process.stderr.write(`[page-error] ${err.message}\n`));

  try {
    // index.html is a single ~30k-line file; cold Chrome starts on slower hosts
    // can exceed the 30s default, so allow more headroom (load is one-time).
    await page.goto(SMM_HTML, { waitUntil: 'domcontentloaded', timeout: 120000 });
    await new Promise(r => setTimeout(r, 1000));

    // Connect to Sigma so the connection lists populate (even though we
    // also defensively appendChild the test connection ID in the drivers).
    console.log('Connecting to Sigma...');
    await page.evaluate((url, id, secret) => {
      document.getElementById('apiRegion').value = url;
      document.getElementById('clientId').value = id;
      document.getElementById('clientSecret').value = secret;
    }, SIGMA_BASE_URL, SIGMA_CLIENT_ID, SIGMA_SECRET);
    await page.click('#connectBtn');
    const ready = await waitForConnections(page, 20000);
    if (!ready) {
      console.warn('  (connection populate not confirmed — proceeding with defensive option-injection)');
    } else {
      console.log('  connected.');
    }

    const results = [];
    for (const fx of fixtures) {
      results.push(await runFixture(page, fx));
    }

    console.log('\n══════════════ RESULTS (smm browser tool) ══════════════');
    const failures = results.filter(r => !r.ok);
    const skipped = results.filter(r => r.skipped);
    for (const r of results) {
      const tag = r.skipped ? '⏭️  SKIP' : (r.ok ? '✅ PASS' : '❌ FAIL');
      console.log(`${tag}  ${r.id}${r.reason ? '  — ' + r.reason : ''}`);
    }
    console.log(`\n${results.length - failures.length - skipped.length}/${results.length - skipped.length} passed, ${skipped.length} skipped`);

    await browser.close();
    process.exit(failures.length === 0 ? 0 : 1);
  } catch (e) {
    console.error('runner error:', e);
    try { await browser.close(); } catch {}
    process.exit(2);
  }
})().catch(e => {
  console.error('fatal:', e);
  process.exit(2);
});
