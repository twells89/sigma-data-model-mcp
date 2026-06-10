// node --import tsx/esm src/cognos-report.localtest.ts [path-to-report.xml]
import { readFileSync } from 'node:fs';
import { convertCognosReportToSigma } from './cognos-report.js';

const path = process.argv[2] || '/Users/tjwells/cognos-samples/go-sales-performance.report.xml';
const res = convertCognosReportToSigma(readFileSync(path, 'utf8'), { dataModelId: 'demo-dm' });

console.log('=== stats ===', JSON.stringify(res.stats));
console.log('workbook:', res.workbook.name);
for (const p of res.workbook.pages) {
  console.log(`\npage "${p.name}"  elements=${p.elements.length}`);
  for (const el of p.elements) {
    if (el.kind === 'control') { console.log(`  control [${(el as any).controlId}]`); continue; }
    console.log(`  table "${el.name}"  source=${el.source.kind}:${el.source.elementId}  cols=${(el.columns || []).length}`);
    (el.columns || []).forEach((c: any) => console.log(`     ${c.name.padEnd(20)} = ${c.formula.slice(0, 80)}`));
  }
}
console.log(`\n=== warnings (${res.warnings.length}) ===`);
res.warnings.forEach((w) => console.log('  !', w.slice(0, 150)));
