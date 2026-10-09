#!/usr/bin/env node
// Builds the semantic-tools embed bundles (vendor/semantic-tools/*) from the
// pinned upstream commit in semantic-tools-embed/PIN.
//
//   node scripts/build-semantic-tools.mjs                # build
//   node scripts/build-semantic-tools.mjs --record-ports # (re)record the upstream-helper hashes
//
// The upstream repo is cloned into .cache/semantic-tools (gitignored) and
// checked out at the pin; it is never modified. Bundles are built with esbuild
// straight from upstream's TypeScript sources (no upstream build step needed).

import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { copyFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';
import { computeSourceSha256 } from './semantic-tools-source-hash.mjs';
import { checkYamlKeyDrift } from './yaml-key-drift.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const EMBED = join(ROOT, 'semantic-tools-embed');
const CLONE = join(ROOT, '.cache', 'semantic-tools');
const OUT = join(ROOT, 'vendor', 'semantic-tools');
const REPO_URL = 'https://github.com/sigmacomputing/semantic-tools.git';
const PACKAGES = ['core', 'sql-expression', 'snowflake-semantic-view', 'snowflake-sigma', 'sigma-data-model'];
const PORTS_FILE = join(EMBED, 'UPSTREAM_PORTS.json');

const sha256 = data => createHash('sha256').update(data).digest('hex');
const git = (...args) => execFileSync('git', args, { cwd: CLONE, encoding: 'utf8' }).trim();
const readJson = p => JSON.parse(readFileSync(p, 'utf8'));

const pin = readFileSync(join(EMBED, 'PIN'), 'utf8').trim();
if (!/^[0-9a-f]{40}$/.test(pin)) throw new Error(`semantic-tools-embed/PIN must hold a full 40-char SHA, got "${pin}"`);

// 1. Clone (if absent) and check out the pin; refuse a dirty or mismatched tree.
if (!existsSync(join(CLONE, '.git'))) {
  mkdirSync(dirname(CLONE), { recursive: true });
  console.log(`Cloning ${REPO_URL} -> .cache/semantic-tools`);
  execFileSync('git', ['clone', '--quiet', REPO_URL, CLONE], { stdio: 'inherit' });
}
if (git('status', '--porcelain') !== '') throw new Error('.cache/semantic-tools has local modifications; delete it and rebuild');
if (git('rev-parse', 'HEAD') !== pin) {
  try {
    git('checkout', '--quiet', '--detach', pin);
  } catch {
    git('fetch', '--quiet', 'origin');
    git('checkout', '--quiet', '--detach', pin);
  }
}
if (git('rev-parse', 'HEAD') !== pin || git('status', '--porcelain') !== '') {
  throw new Error(`.cache/semantic-tools is not cleanly at pinned commit ${pin}`);
}
console.log(`semantic-tools @ ${pin}`);

// 2. Upstream helpers we ported verbatim: detect drift against the pinned clone.
const PORTS = [
  { file: 'packages/cli/src/registry.ts', symbol: 'detectFromByContent' },
  { file: 'packages/cli/src/main.ts', symbol: 'unqualifiedReferences' },
];
function extractFunction(file, symbol) {
  const lines = readFileSync(join(CLONE, file), 'utf8').split('\n');
  const start = lines.findIndex(l => new RegExp(`^(export )?function ${symbol}\\(`).test(l));
  if (start < 0) return undefined;
  let end = start;
  while (end < lines.length && lines[end] !== '}') end++;
  return lines.slice(start, end + 1).join('\n');
}
const currentPorts = PORTS.map(p => {
  const text = extractFunction(p.file, p.symbol);
  return { ...p, sha256: text === undefined ? null : sha256(text) };
});
if (process.argv.includes('--record-ports')) {
  writeFileSync(PORTS_FILE, JSON.stringify({ commit: pin, ports: currentPorts }, null, 2) + '\n');
  console.log('Recorded upstream port hashes ->', PORTS_FILE);
} else if (!existsSync(PORTS_FILE)) {
  console.warn('WARNING: semantic-tools-embed/UPSTREAM_PORTS.json missing; run with --record-ports');
} else {
  const recorded = readJson(PORTS_FILE).ports;
  for (const cur of currentPorts) {
    const rec = recorded.find(r => r.file === cur.file && r.symbol === cur.symbol);
    if (!rec || rec.sha256 !== cur.sha256) {
      console.warn(
        `WARNING: upstream ${cur.file} ${cur.symbol}() changed vs the copy ported into semantic-tools-embed/src/index.ts.\n` +
          `         Re-diff it, update the port, then run: node scripts/build-semantic-tools.mjs --record-ports`,
      );
    }
  }
}

// 2b. Drift guard: the embed layer's known-key table (UNRECOGNIZED_YAML_KEY) must equal what readYaml.ts reads.
const keyDrift = checkYamlKeyDrift();
if (keyDrift.length > 0) {
  throw new Error(
    'semantic-tools-embed/src/yamlKnownKeys.json is out of sync with the pinned upstream readYaml.ts:\n  ' +
      keyDrift.join('\n  ') +
      '\nUpdate yamlKnownKeys.json (and scripts/yaml-key-drift.mjs VAR_LEVEL if a new record variable appeared), then rebuild.',
  );
}
console.log('YAML known-key table matches upstream readYaml.ts');

// 3. Bundle.
const embedVersion = /EMBED_VERSION\s*=\s*'([^']+)'/.exec(readFileSync(join(EMBED, 'src', 'version.ts'), 'utf8'))?.[1];
if (!embedVersion) throw new Error('could not read EMBED_VERSION from semantic-tools-embed/src/version.ts');

const alias = { yaml: join(ROOT, 'node_modules', 'yaml', 'browser', 'index.js') };
for (const p of PACKAGES) alias[`@semantic-tools/${p}`] = join(CLONE, 'packages', p, 'src', 'index.ts');

const common = {
  entryPoints: [join(EMBED, 'src', 'index.ts')],
  bundle: true,
  platform: 'neutral',
  target: 'es2022',
  alias,
  define: {
    __SEMANTIC_TOOLS_COMMIT__: JSON.stringify(pin),
    __EMBED_VERSION__: JSON.stringify(embedVersion),
  },
  legalComments: 'none',
  logLevel: 'warning',
};

mkdirSync(OUT, { recursive: true });
const esmPath = join(OUT, 'semantic-tools-embed.mjs');
const iifePath = join(OUT, 'semantic-tools-embed.iife.min.js');
await build({ ...common, format: 'esm', outfile: esmPath });
await build({ ...common, format: 'iife', globalName: 'SemanticTools', minify: true, outfile: iifePath });

// 4. License, notice, provenance.
copyFileSync(join(CLONE, 'LICENSE'), join(OUT, 'LICENSE'));
writeFileSync(
  join(OUT, 'NOTICE'),
  `The semantic-tools-embed bundles in this directory contain code from https://github.com/sigmacomputing/semantic-tools at commit ${pin}, licensed under Apache-2.0 (see LICENSE).\n`,
);

const esbuildVersion = readJson(join(ROOT, 'node_modules', 'esbuild', 'package.json')).version;
const yamlVersion = readJson(join(ROOT, 'node_modules', 'yaml', 'package.json')).version;
const provenance = {
  semanticToolsRepo: REPO_URL,
  semanticToolsCommit: pin,
  embedVersion,
  builtWith: { esbuild: esbuildVersion, yaml: yamlVersion },
  // Hash of the embed sources + src/sigma-ids.ts + PIN + tool versions this bundle was built from; `npm run test:semantic-tools` fails when it drifts.
  sourceSha256: computeSourceSha256(ROOT),
  sha256: {
    'semantic-tools-embed.mjs': sha256(readFileSync(esmPath)),
    'semantic-tools-embed.iife.min.js': sha256(readFileSync(iifePath)),
  },
};
writeFileSync(join(OUT, 'PROVENANCE.json'), JSON.stringify(provenance, null, 2) + '\n');

const kb = p => (readFileSync(p).length / 1024).toFixed(1) + ' KB';
console.log(`Built vendor/semantic-tools: ESM ${kb(esmPath)}, IIFE(min) ${kb(iifePath)}`);
console.log(JSON.stringify(provenance, null, 2));
