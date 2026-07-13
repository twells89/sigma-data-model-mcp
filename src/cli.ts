#!/usr/bin/env node
/**
 * Local converter CLI — run any converter offline on a file, no MCP server / no Sigma API.
 *
 *   sigma-convert <source> --file <path> [--file <path>...] \
 *       [--connection <uuid>] [--db <name>] [--schema <name>] \
 *       [--opts '<json>'] [--out <path>]
 *
 *   # dev (no build): tsx src/cli.ts <source> --file ...
 *   node build/cli.js <source> --file ...
 *
 * Prints { sigmaDataModel, stats, warnings } to stdout (or --out) and a warning
 * summary to stderr. Exit code is non-zero if the converter threw. This mirrors
 * the convert_<source>_to_sigma MCP tools by reusing the same converter functions.
 */
import { readFileSync, writeFileSync } from 'node:fs';
import { basename, extname } from 'node:path';
import { convertDbtToSigma } from './dbt.js';
import { convertSnowflakeSemanticView } from './snowflake.js';
import { convertLookMLToSigma } from './lookml.js';
import { convertPowerBIToSigma } from './powerbi.js';
import { convertTableauToSigma } from './tableau.js';
import { convertCognosToSigma } from './cognos.js';
import { convertCognosReportToSigma } from './cognos-report.js';
import { convertOmniToSigma } from './omni.js';
import { convertSqlToSigma } from './sql.js';
import { convertThoughtSpotToSigma } from './thoughtspot.js';
import { convertQlikToSigma } from './qlik.js';
import { convertAtlanToSigma } from './atlan.js';
import { convertAlteryxToSigma } from './alteryx.js';
import { convertCubeToSigma } from './cube.js';
import { convertTableauPrepToSigma } from './tableau-prep.js';
import { convertQuickSightToSigma } from './quicksight.js';
import { convertOacToSigma } from './oac.js';
import { convertBobjToSigma } from './bobj.js';

type Kind = 'text' | 'files' | 'json' | 'sql';
// Most converters return a data `model`; cognos-report returns a `workbook`.
type Result = { model?: any; workbook?: any; warnings: string[]; stats?: any };
interface Entry { kind: Kind; run: (input: any, opts: any) => Result; note?: string; }

// One entry per converter. `kind` says how --file(s) become the first arg:
//   text  → raw file contents (string); single file
//   files → [{ name, content }] for each --file (multi-file formats)
//   json  → JSON.parse(file) (object or array); single file
//   sql   → .sql file → [{ name, sql }]; .json file → parsed statements array
const REG: Record<string, Entry> = {
  dbt:             { kind: 'text',  run: (i, o) => convertDbtToSigma(i, o) },
  snowflake:       { kind: 'text',  run: (i, o) => convertSnowflakeSemanticView(i, o) },
  lookml:          { kind: 'files', run: (i, o) => convertLookMLToSigma(i, o), note: 'pass the .model.lkml AND its .view.lkml files; set --opts \'{"exploreName":"..."}\' to pick an explore' },
  powerbi:         { kind: 'json',  run: (i, o) => convertPowerBIToSigma(i, o), note: 'input is a .bim / TMSL JSON' },
  tableau:         { kind: 'text',  run: (i, o) => convertTableauToSigma(i, o), note: 'owned by the Tableau workstream (beads-sigma-93ps); runner only' },
  cognos:          { kind: 'text',  run: (i, o) => convertCognosToSigma(i, o) },
  'cognos-report': { kind: 'text',  run: (i, o) => convertCognosReportToSigma(i, o) },
  omni:            { kind: 'files', run: (i, o) => convertOmniToSigma(i, o) },
  sql:             { kind: 'sql',   run: (i, o) => convertSqlToSigma(i, o) },
  thoughtspot:     { kind: 'text',  run: (i, o) => convertThoughtSpotToSigma(i, o) },
  qlik:            { kind: 'json',  run: (i, o) => convertQlikToSigma(i, o) },
  atlan:           { kind: 'text',  run: (i, o) => convertAtlanToSigma(i, o) },
  alteryx:         { kind: 'text',  run: (i, o) => convertAlteryxToSigma(i, o) },
  cube:            { kind: 'files', run: (i, o) => convertCubeToSigma(i, o) },
  'tableau-prep':  { kind: 'text',  run: (i, o) => convertTableauPrepToSigma(i, o) },
  quicksight:      { kind: 'files', run: (i, o) => convertQuickSightToSigma(i, o) },
  oac:             { kind: 'json',  run: (i, o) => convertOacToSigma(i, o), note: 'input is the OAC tables JSON array; --opts \'{"physicalMap":{...}}\' optional' },
  bobj:            { kind: 'text',  run: (i, o) => convertBobjToSigma(i, o) },
};

function usage(): never {
  const sources = Object.keys(REG).sort().join(', ');
  process.stderr.write(
    `sigma-convert — run a Sigma converter locally (offline, no API)\n\n` +
    `Usage:\n  sigma-convert <source> --file <path> [--file <path>...] \\\n` +
    `      [--connection <uuid>] [--db <name>] [--schema <name>] [--opts '<json>'] [--out <path>]\n\n` +
    `Sources:\n  ${sources}\n\n` +
    `Notes:\n  --file repeats for multi-file formats (lookml, cube, omni, quicksight).\n` +
    `  --opts merges extra converter options as JSON (e.g. auto_metrics, exploreName, tableMap, physicalMap, dataModelId).\n` +
    `  Output is { sigmaDataModel, stats, warnings } JSON; warnings also print to stderr.\n`,
  );
  process.exit(2);
}

function parseArgs(argv: string[]) {
  const files: string[] = [];
  const opts: Record<string, string> = {};
  let source = '';
  let out = '';
  let jsonOpts: any = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '-h' || a === '--help') usage();
    else if (a === '--file' || a === '-f') files.push(argv[++i]);
    else if (a === '--connection') opts.connectionId = argv[++i];
    else if (a === '--db' || a === '--database') opts.database = argv[++i];
    else if (a === '--schema') opts.schema = argv[++i];
    else if (a === '--out' || a === '-o') out = argv[++i];
    else if (a === '--opts') { try { jsonOpts = JSON.parse(argv[++i]); } catch { throw new Error('--opts must be valid JSON'); } }
    else if (!a.startsWith('-') && !source) source = a;
    else throw new Error(`unrecognized argument: ${a}`);
  }
  return { source, files, options: { ...opts, ...jsonOpts }, out };
}

function buildInput(kind: Kind, files: string[]): any {
  if (files.length === 0) throw new Error('at least one --file is required');
  if (kind === 'files') return files.map(p => ({ name: basename(p), content: readFileSync(p, 'utf8') }));
  if (files.length > 1) throw new Error(`this source accepts a single --file (got ${files.length})`);
  const raw = readFileSync(files[0], 'utf8');
  if (kind === 'text') return raw;
  if (kind === 'json') return JSON.parse(raw);
  if (kind === 'sql') return extname(files[0]).toLowerCase() === '.json'
    ? JSON.parse(raw)
    : [{ name: basename(files[0], extname(files[0])), sql: raw }];
  throw new Error(`unknown kind ${kind}`);
}

function main() {
  const { source, files, options, out } = parseArgs(process.argv.slice(2));
  if (!source) usage();
  const entry = REG[source];
  if (!entry) { process.stderr.write(`Unknown source "${source}". Known: ${Object.keys(REG).sort().join(', ')}\n`); process.exit(2); }

  const input = buildInput(entry.kind, files);
  const result = entry.run(input, options);
  const artifactKey = result.model ? 'sigmaDataModel' : 'sigmaWorkbook';
  const payload = JSON.stringify({ [artifactKey]: result.model ?? result.workbook, stats: result.stats, warnings: result.warnings }, null, 2);
  if (out) { writeFileSync(out, payload); process.stderr.write(`Wrote ${out}\n`); }
  else process.stdout.write(payload + '\n');

  const ws = result.warnings || [];
  if (ws.length) { process.stderr.write(`\n${ws.length} warning(s):\n`); for (const w of ws) process.stderr.write(`  - ${w}\n`); }
}

try { main(); }
catch (e: any) { process.stderr.write(`Error: ${e?.message || e}\n`); process.exit(1); }
