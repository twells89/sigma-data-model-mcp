// node --import tsx/esm src/cognos.batchtest.ts
import { readFileSync, readdirSync } from 'node:fs';
import { convertCognosToSigma } from './cognos.js';
import { convertCognosReportToSigma } from './cognos-report.js';

const DIR = '/Users/tjwells/cognos-samples';
const files = readdirSync(DIR);

console.log('======== DATA MODULES → Sigma DM ========');
for (const f of files.filter((x) => x.endsWith('.module.json'))) {
  try {
    const r = convertCognosToSigma(readFileSync(`${DIR}/${f}`, 'utf8'), { connectionId: 'c', database: 'DB', schema: 'S' });
    console.log(`${f.padEnd(34)} OK  elems=${r.stats.elements} cols=${r.stats.columns} metrics=${r.stats.metrics} rels=${r.stats.relationships} warns=${r.warnings.length}`);
  } catch (e: any) { console.log(`${f.padEnd(34)} CRASH ${e.message}`); }
}

console.log('\n======== REPORTS → Sigma workbook ========');
for (const f of files.filter((x) => x.endsWith('.report.xml'))) {
  try {
    const r = convertCognosReportToSigma(readFileSync(`${DIR}/${f}`, 'utf8'), { dataModelId: 'dm' });
    console.log(`${f.padEnd(34)} OK  tables=${r.stats.tables} cols=${r.stats.columns} controls=${r.stats.controls} warns=${r.warnings.length}`);
  } catch (e: any) { console.log(`${f.padEnd(34)} CRASH ${e.message}`); }
}

// Aggregate unique warning *kinds* to find gaps worth fixing
console.log('\n======== warning kinds (gap signal) ========');
const kinds = new Map<string, number>();
for (const f of files.filter((x) => x.endsWith('.module.json'))) {
  try { convertCognosToSigma(readFileSync(`${DIR}/${f}`, 'utf8'), { connectionId: 'c', database: 'DB', schema: 'S' }).warnings.forEach(tally); } catch {}
}
for (const f of files.filter((x) => x.endsWith('.report.xml'))) {
  try { convertCognosReportToSigma(readFileSync(`${DIR}/${f}`, 'utf8'), {}).warnings.forEach(tally); } catch {}
}
function tally(w: string) {
  const key = w.replace(/"[^"]*":\s*/, '').replace(/"[^"]+"/g, '"…"').replace(/\b[A-Za-z_]\w{3,}\(\)/g, 'fn()').slice(0, 90);
  kinds.set(key, (kinds.get(key) || 0) + 1);
}
[...kinds.entries()].sort((a, b) => b[1] - a[1]).forEach(([k, n]) => console.log(`  ${String(n).padStart(3)}×  ${k}`));
