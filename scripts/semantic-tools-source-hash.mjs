// Hash of everything the semantic-tools embed bundles are built from, besides the (pinned) upstream clone:
// semantic-tools-embed/src/** (sorted relative paths + contents), src/sigma-ids.ts, PIN, and the esbuild / yaml versions.
// build-semantic-tools.mjs records it in vendor/semantic-tools/PROVENANCE.json (sourceSha256);
// check-semantic-tools.mjs recomputes it and fails when the bundle is stale.
import { createHash } from 'node:crypto';
import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

function walk(dir, base = '') {
  const out = [];
  for (const e of readdirSync(join(dir, base), { withFileTypes: true })) {
    const rel = base ? `${base}/${e.name}` : e.name;
    if (e.isDirectory()) out.push(...walk(dir, rel));
    else out.push(rel);
  }
  return out;
}

export function computeSourceSha256(root) {
  const h = createHash('sha256');
  const part = (label, data) => { h.update(`\0${label}\0${data.length}\0`); h.update(data); };
  const srcDir = join(root, 'semantic-tools-embed', 'src');
  for (const rel of walk(srcDir).sort()) part(`embed-src/${rel}`, readFileSync(join(srcDir, rel)));
  part('src/sigma-ids.ts', readFileSync(join(root, 'src', 'sigma-ids.ts')));
  part('PIN', Buffer.from(readFileSync(join(root, 'semantic-tools-embed', 'PIN'), 'utf8').trim()));
  for (const pkg of ['esbuild', 'yaml']) {
    part(`version/${pkg}`, Buffer.from(JSON.parse(readFileSync(join(root, 'node_modules', pkg, 'package.json'), 'utf8')).version));
  }
  return h.digest('hex');
}
