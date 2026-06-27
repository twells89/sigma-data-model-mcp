// Build a "Not Migrated (and why)" punch-list for the DDMX run: categorize every
// source visual/tile that didn't make it into the final workbook, with a reason +
// action — so no empty tab is mysterious. (n4pi.8 surfacing; aligns with bead ncwe.)
import { convertTableauToSigma } from '/tmp/wt-blend/build/tableau.js';
import fs from 'fs';
const [,, run, twb, conn, db, schema] = process.argv;
const xml = fs.readFileSync(twb,'utf8');
const r = convertTableauToSigma(xml, { connectionId:conn, database:db, schema:schema });
const wp = r.workbookPatterns || [];
const paramSwitch = new Map(wp.filter(p=>p.kind==='param-switch').map(p=>[p.name, p]));
const paramFilter = new Map(wp.filter(p=>p.kind==='param-filter').map(p=>[p.name, p]));
// .twb: which calcs are fully-commented (inert in source) + caption↔internal-name
const cols = [...xml.matchAll(/<column\b[^>]*\bcaption='([^']*)'[^>]*\bname='\[([^']+)\]'[^>]*>([\s\S]*?)<\/column>/g)];
const inert = new Set(), nameToCap = {}, capToName = {};
for (const [,cap,name,body] of cols) {
  nameToCap[name]=cap; capToName[cap]=name;
  const fm = body.match(/formula='([^']*)'/);
  if (fm) { const f = fm[1].replace(/&#10;/g,'\n').replace(/&quot;/g,'"');
    const live = f.split('\n').map(l=>l.replace(/\/\/.*$/,'').trim()).filter(Boolean).join(' ');
    if (f.trim() && !live) inert.add(cap), inert.add(name);
  }
}
// fields known absent from the collapsed SQL (phantom-skipped)
const el = (r.model?.pages||[]).flatMap(p=>p.elements||[]).find(e=>e.source?.kind==='sql');
const sql = el?.source?.statement || '';
const colset = new Set((el?.columns||[]).map(c=>(c.name||'').toLowerCase()));
const nk = s => (s||'').replace(/[^a-z0-9]/gi,'').toLowerCase();
const colnorm = new Set([...colset].map(nk));
const metricNorm = new Set((el?.metrics||[]).map(m=>nk(m.name)));
const capByNorm = {}; for (const [name,cap] of Object.entries(nameToCap)) capByNorm[nk(name)] = cap;
const isCalcInternal = s => /^Calculation_\d+/i.test(s) || /_\d{6,}(?::|$)/.test(s) || /\(copy\)/i.test(s);
// dropped elements: in chart-specs, not in final wb-spec
const cs = JSON.parse(fs.readFileSync(`${run}/chart-specs.json`,'utf8'));
const wb = JSON.parse(fs.readFileSync(`${run}/wb-spec.json`,'utf8'));
const kept = new Set(); for (const pg of wb.pages||[]) for (const e of pg.elements||[]) kept.add(e.id);
const refRe = /\[master\/([^\]]+)\]/ig;
const collect=(o,a)=>{ if(o&&typeof o==='object'){ for(const v of Object.values(o)) collect(v,a);} else if(typeof o==='string'){ let m; while((m=refRe.exec(o))) a.push(m[1]); } return a; };
function classify(ref){
  const base = ref.replace(/\s*\([^)]*\)\s*$/,'').replace(/\s*\(copy\)_\d+$/,'').replace(/:nk(:\d+)?$/,'');
  // resolve a Calculation_NNN / internal name to its friendly caption when we can
  const cap = nameToCap[ref] || capByNorm[nk(ref)] || ref;
  if (paramSwitch.has(cap) || paramSwitch.has(ref)) {
    const ps = paramSwitch.get(cap)||paramSwitch.get(ref);
    return ['param-measure-picker', `Tableau parameter measure-picker "${cap}" → Sigma control-driven Switch (pattern verified, KPI proof). Control [${ps.controlId}] emitted; set the tile's measure column to the Switch.`];
  }
  if (paramFilter.has(cap) || /\bParam(eter)?\b/i.test(cap) || /Parameter/i.test(ref))
    return ['param-driven', `references Tableau parameter "${cap}" → bind a Sigma control (segmented/list) to the SOURCE element (control→viz 400s); control-driven dynamic field/filter.`];
  if (inert.has(ref) || inert.has(cap))
    return ['inert-in-source', `the Tableau calc "${cap}" is fully commented-out (//) in the source .twb — dead in Tableau too; nothing to migrate.`];
  if (metricNorm.has(nk(base)) || metricNorm.has(nk(cap)))
    return ['aggregate-metric', `"${cap}" is an aggregate metric — plot it as the tile's MEASURE (chart-context aggregate), not a row column.`];
  if (isCalcInternal(ref))
    return ['unresolved-calc', `calc "${cap}" did not resolve to a model column (window/LOD/percent-of-total or copy-calc) — rebuild in a grouped chart element.`];
  if (!colnorm.has(nk(base)))
    return ['absent-from-sql', `physical field "${ref}" is not in the custom-SQL SELECT of the collapsed model — add it to the source query to migrate.`];
  return ['unresolved-calc', `field "${ref}" did not resolve to a model column.`];
}
// Binding-constraint priority: a tile can have several blockers; lead with the one
// the user must fix first (a field missing from the SQL blocks the tile regardless
// of anything else; a param-picker is the most fixable).
const PRIORITY = ['absent-from-sql','unresolved-calc','aggregate-metric','inert-in-source','param-measure-picker','param-driven'];
const rows = [];
for (const pg of cs.pages||[]) for (const e of pg.elements||[]) {
  if (kept.has(e.id)) continue;
  const refs = [...new Set(collect(e,[]).filter(x=>!colset.has(x.toLowerCase())))];
  const cats = refs.map(classify);
  if (!cats.length) cats.push(['unresolved','no resolvable reason captured']);
  const distinct = [...new Set(cats.map(c=>c[0]))];
  distinct.sort((a,b)=>PRIORITY.indexOf(a)-PRIORITY.indexOf(b));
  const primary = distinct[0];
  const reason = (cats.find(c=>c[0]===primary)||cats[0])[1];
  rows.push({ page: pg.name, tile: e.id, kind: e.kind||e.type, category: primary,
              also: distinct.slice(1), reason, refs: refs.slice(0,5) });
}
// report
const byCat = {}; rows.forEach(r=>byCat[r.category]=(byCat[r.category]||0)+1);
let md = `# DDMX migration — Not Migrated (and why)\n\n`;
md += `${rows.length} tile(s) not migrated, by reason:\n\n`;
for (const [c,n] of Object.entries(byCat).sort((a,b)=>b[1]-a[1])) md += `- **${c}**: ${n}\n`;
md += `\n---\n\n`;
const byPage={}; rows.forEach(r=>(byPage[r.page]=byPage[r.page]||[]).push(r));
for (const [pg,list] of Object.entries(byPage)) {
  md += `## ${pg} (${list.length})\n\n`;
  for (const r of list) md += `- \`${r.tile}\` (${r.kind}) — **${r.category}**${r.also.length?` _(also: ${r.also.join(', ')})_`:''}: ${r.reason}${r.refs.length?` _[refs: ${r.refs.join(', ')}]_`:''}\n`;
  md += `\n`;
}
fs.writeFileSync(`${run}/migration-notes.md`, md);
console.log(`migration-notes.md: ${rows.length} tiles categorized`);
for (const [c,n] of Object.entries(byCat).sort((a,b)=>b[1]-a[1])) console.log(`  ${String(n).padStart(3)}  ${c}`);
