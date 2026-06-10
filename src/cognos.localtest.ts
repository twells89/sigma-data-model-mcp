// Local smoke test (WIP, not committed):
//   node --import tsx/esm src/cognos.localtest.ts [path-to-module.json]
import { readFileSync } from 'node:fs';
import { convertCognosToSigma } from './cognos.js';

const path = process.argv[2] || new URL('../regression-corpus/cognos/sample-data-module.json', import.meta.url).pathname;
const res = convertCognosToSigma(readFileSync(path, 'utf8'), { connectionId: 'demo', database: 'RETAIL', schema: 'PUBLIC' });

console.log('=== stats ===', JSON.stringify(res.stats));
for (const el of res.model.pages[0].elements.slice(0, 8)) {
  console.log(`\n• ${el.kind} "${el.name}"  path=${(el.source.path || []).join('.')}  cols=${(el.columns || []).length} metrics=${((el as any).metrics || []).length} rels=${(el.relationships || []).length}`);
  ((el as any).metrics || []).slice(0, 3).forEach((m: any) => console.log(`    metric ${m.name} = ${m.formula}`));
  (el.columns || []).filter((c: any) => c.name && /If\(|Over\(|DateAdd|&|Coalesce/.test(c.formula)).slice(0, 3).forEach((c: any) => console.log(`    calc   ${c.name} = ${String(c.formula).slice(0, 95)}`));
  (el.relationships || []).slice(0, 4).forEach((r: any) => console.log(`    rel    →${r.name}`));
}
console.log(`\n=== warnings (${res.warnings.length}) ===`);
res.warnings.slice(0, 14).forEach(w => console.log('  !', w.slice(0, 150)));
