// Drift guard for semantic-tools-embed/src/yamlKnownKeys.json.
// Derives, from the pinned upstream readYaml.ts, the keys it reads at each object level
// (property accesses on its well-known record variables) and compares them with the
// embed layer's known-key table. Used by build-semantic-tools.mjs (throws) and
// check-semantic-tools.mjs (fails); also runnable directly:
//   node scripts/yaml-key-drift.mjs [path/to/readYaml.ts]   # exit 1 on drift
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
export const READ_YAML = join(ROOT, '.cache/semantic-tools/packages/snowflake-semantic-view/src/readYaml.ts');
export const KNOWN_KEYS = join(ROOT, 'semantic-tools-embed/src/yamlKnownKeys.json');

// Upstream variable name -> level. Each is a `Record<string, unknown>` for exactly that level.
const VAR_LEVEL = {
  document: 'root',
  table: 'table',
  baseTable: 'base_table',
  key: 'unique_key',
  object: 'object',
  entry: 'non_additive_dimension',
  relationship: 'relationship',
  column: 'relationship_column',
  rangeEntry: 'relationship_column',
  rightRange: 'right_range',
};

/** { level: Set<key> } read by the given readYaml.ts source text. */
export function extractReadKeys(src) {
  const code = src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');
  const levels = {};
  const add = (level, k) => (levels[level] ??= new Set()).add(k);
  for (const m of code.matchAll(/\b(\w+)\.([a-z][a-z_]*)\b/g)) {
    const level = Object.hasOwn(VAR_LEVEL, m[1]) ? VAR_LEVEL[m[1]] : undefined;
    if (level) add(level, m[2]);
  }
  for (const m of code.matchAll(/\btable\.primary_key\.([a-z_]+)/g)) add('primary_key', m[1]);
  return levels;
}

/** Problems (strings) when `src` and the known-key table disagree; [] when in sync. */
export function yamlKeyDrift(src, known = JSON.parse(readFileSync(KNOWN_KEYS, 'utf8')).read) {
  const problems = [];
  const upstream = extractReadKeys(src);
  const levels = new Set([...Object.keys(upstream), ...Object.keys(known)]);
  for (const level of levels) {
    const up = upstream[level] ?? new Set();
    const ours = new Set(known[level] ?? []);
    // `primary_key`/`table.primary_key` etc. are themselves keys of the parent level: `table.primary_key` already counted there.
    for (const k of up) if (!ours.has(k)) problems.push(`upstream readYaml.ts reads "${level}.${k}" but yamlKnownKeys.json does not list it`);
    for (const k of ours) if (!up.has(k)) problems.push(`yamlKnownKeys.json lists "${level}.${k}" but upstream readYaml.ts no longer reads it`);
  }
  // Catch reads through a variable name this script does not know: any snake_case property access not in our table.
  const all = new Set(Object.values(known).flat());
  const code = src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');
  for (const m of code.matchAll(/\b(\w+)\.([a-z]+_[a-z_]+)\b/g)) {
    if (!all.has(m[2])) problems.push(`readYaml.ts accesses "${m[1]}.${m[2]}" (unknown variable or key): update scripts/yaml-key-drift.mjs VAR_LEVEL and/or yamlKnownKeys.json`);
  }
  return [...new Set(problems)];
}

export function checkYamlKeyDrift(path = READ_YAML) {
  return yamlKeyDrift(readFileSync(path, 'utf8'));
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const problems = checkYamlKeyDrift(process.argv[2] ? resolve(process.argv[2]) : READ_YAML);
  if (problems.length) { console.error('YAML known-key drift:\n  ' + problems.join('\n  ')); process.exit(1); }
  console.log('YAML known-key table matches upstream readYaml.ts');
}
