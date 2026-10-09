#!/usr/bin/env node
// Compares the legacy name-only guesser (src/sigma-ids.ts inferSigmaFormat) with the evidence-based
// inference (semantic-tools-embed/src/formats.ts) over every Snowflake fixture we have.
//   npm run build:semantic-tools && node --import tsx/esm scripts/compare-format-inference.mjs
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { inferSigmaFormat } from '../src/sigma-ids.ts';
import * as S from '../vendor/semantic-tools/semantic-tools-embed.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const T = join(ROOT, '.cache/semantic-tools/packages/cli/src/__tests__/fixtures');
const inputs = [];
if (existsSync(T)) for (const f of readdirSync(T).filter(f => f.endsWith('.sql')).sort()) inputs.push([`upstream/${f}`, join(T, f)]);
const FIX = join(ROOT, 'semantic-tools-embed/test/fixtures');
for (const f of readdirSync(FIX).filter(f => /\.ya?ml$/.test(f)).sort()) inputs.push([`embed-fixtures/${f}`, join(FIX, f)]);
const RC = join(ROOT, 'regression-corpus/snowflake');
for (const d of readdirSync(RC).sort()) for (const f of readdirSync(join(RC, d)).filter(f => f.startsWith('input.'))) inputs.push([`regression-corpus/${d}/${f}`, join(RC, d, f)]);
const extra = process.argv[2] ?? '/Users/tjwells/sst-merge/example-tpch-semantic-view.yaml';
if (existsSync(extra)) inputs.push([`example/${extra.split('/').pop()}`, extra]);

const label = f => !f ? '-' : f.formatString === ',.2%' ? '% (,.2%)' : f.currencySymbol ? '$ (,.2f)' : f.formatString === ',.0f' ? 'int (,.0f)' : `${f.formatString}${f.suffix ?? ''}`;
const sameFmt = (a, b) => JSON.stringify(a ?? null) === JSON.stringify(b ?? null);
const OPTS = { connectionId: 'c', database: 'DB', schema: 'SC', crossTableReferences: 'engine', synonymsInDescription: false };
let total = 0, agree = 0; const dis = [];
const PASS = /^\[[^\[\]/]+\/[^\[\]]+\]$/;

for (const [label0, path] of inputs) {
  const text = readFileSync(path, 'utf8');
  const raw = S.convertSemanticView(text, OPTS);
  const rows = [];
  for (const v of raw.results) {
    if (!v.ok) { console.log(`\n## ${label0}: not converted (${v.errors.map(e => e.code).join(', ') || 'refused'})`); continue; }
    const facts = S.formatFacts(text, raw.format);
    const res = S.inferFormats(v.spec, { facts, elementIdOf: S.mkElementId }, v.viewName);
    for (const it of res.items) rows.push(it);
  }
  if (rows.length === 0) { if (raw.results.some(v => v.ok)) console.log(`\n## ${label0}: no metrics/calculated columns`); continue; }
  console.log(`\n## ${label0}`);
  console.log('| item | legacy | new | reason |\n|---|---|---|---|');
  for (const it of rows) {
    total++;
    const ok = sameFmt(it.legacy, it.format);
    if (ok) agree++; else dis.push(`${label0} :: ${it.name}: legacy ${label(it.legacy)} -> new ${label(it.format)} (${it.why})`);
    console.log(`| ${it.name} | ${label(it.legacy)} | ${label(it.format)} | ${it.format ? it.unit : it.why} |`);
  }
}
console.log(`\n# Summary: ${total} items, ${agree} agree, ${total - agree} disagree`);
for (const d of dis) console.log(`- ${d}`);
