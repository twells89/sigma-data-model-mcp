// Extract param-switch measure-pickers from the converter, inline DM metric refs
// so the Switch resolves against master columns, and emit a materialization spec.
import { convertTableauToSigma } from '/tmp/wt-blend/build/tableau.js';
import fs from 'fs';
const [,, twb, conn, db, schema, out] = process.argv;
const r = convertTableauToSigma(fs.readFileSync(twb,'utf8'), { connectionId:conn, database:db, schema:schema });
const el = (r.model?.pages||[]).flatMap(p=>p.elements||[]).find(e=>e.source?.kind==='sql');
const nk = s => (s||'').replace(/[^a-z0-9]/gi,'').toLowerCase();
const metricByName = {}; for (const m of (el.metrics||[])) if (m.name) metricByName[m.name.toLowerCase()] = m.formula;
const metricByNorm = {}; for (const m of (el.metrics||[])) if (m.name) metricByNorm[nk(m.name)] = m.formula;
const colNames = new Set((el.columns||[]).map(c=>(c.name||'').toLowerCase()));
const colByNorm = {}; for (const c of (el.columns||[])) if (c.name) colByNorm[nk(c.name)] = c.name;
// recursively inline [MetricRef] -> (its formula); resolve column refs to their exact
// alias name (caption ↔ SQL-alias, normalized); leave base columns + control refs.
function inline(f, depth=0) {
  if (depth>5) return f;
  // Match FULL bracket contents (incl. metric names with '/', e.g. [TAM / CW]).
  return f.replace(/\[([^\]]+)\]/g, (m, ref) => {
    const k = ref.toLowerCase();
    if (k.startsWith('ctl-')) return m;                       // control ref
    if (metricByName[k]) return '(' + inline(metricByName[k], depth+1) + ')';
    if (metricByNorm[nk(ref)]) return '(' + inline(metricByNorm[nk(ref)], depth+1) + ')';
    if (colNames.has(k)) return m;                            // exact base column
    if (colByNorm[nk(ref)]) return `[${colByNorm[nk(ref)]}]`;  // caption → SQL-alias
    return m;                                                  // unknown — flagged
  });
}
const allRefs = f => (f.match(/\[([^\]]+)\]/g)||[]).map(s=>s.slice(1,-1));
const unresolved = f => allRefs(f).filter(n => !colNames.has(n.toLowerCase()) && !n.toLowerCase().startsWith('ctl-'));
const params = {}; for (const p of (r.parameters||[])) params[p.rawName] = p;
const specs = [];
for (const p of (r.workbookPatterns||[]).filter(w=>w.kind==='param-switch')) {
  const def = params[p.paramName] || {};
  const values = def.members || [];                 // already decoded+unquoted by the converter
  const dflt   = def.currentValue || (def.members||[])[0] || '';
  const inlined = inline(p.formula);
  // a case still referencing an unknown (non-column, non-control) name or a window fn is "impure"
  const leftover = unresolved(inlined);
  const hasWindow = /window_|running_/i.test(inlined);
  specs.push({ caption:p.name, controlId:p.controlId, paramName:p.paramName, values, default:dflt,
               switchFormula:inlined, pure: leftover.length===0 && !hasWindow, leftover, hasWindow });
}
fs.writeFileSync(out, JSON.stringify(specs,null,2));
console.log('param-switch specs:', specs.length);
specs.forEach(s=>console.log(`  ${s.caption} [${s.controlId}] pure=${s.pure} vals=${JSON.stringify(s.values)}\n    => ${s.switchFormula.slice(0,160)}${s.leftover.length?'\n    LEFTOVER: '+s.leftover.join(','):''}`));
