/**
 * Surfaces YAML keys the upstream reader (readYaml.ts) silently ignores.
 * Upstream reads a fixed set of keys and drops the rest without any finding, so a
 * legacy/typo'd construct (e.g. relationships nested inside a table) converts to
 * a model that is quietly missing it. This module walks the parsed document with
 * the same shape upstream reads and reports every key it never looks at.
 *
 * Known-key table: yamlKnownKeys.json, drift-checked against readYaml.ts by
 * scripts/yaml-key-drift.mjs. Browser-safe (no node: imports).
 */
import { parse as parseYaml } from 'yaml';

import KEYS from './yamlKnownKeys.json';

export const UNRECOGNIZED_YAML_KEY = 'UNRECOGNIZED_YAML_KEY';
const MAX_PATHS = 12;

type Rec = Record<string, unknown>;
type Level = keyof typeof KEYS.read;

const benign = KEYS.benign as Partial<Record<Level, string[]>>;
const isRec = (v: unknown): v is Rec => typeof v === 'object' && v !== null && !Array.isArray(v);
const recs = (v: unknown): Rec[] => (Array.isArray(v) ? v.filter(isRec) : []);
const label = (o: Rec, i: number): string => (typeof o.name === 'string' || typeof o.name === 'number' ? String(o.name) : String(i));

function walkKeys(o: Rec, level: Level, path: string, out: string[]): void {
  const known = new Set<string>([...KEYS.read[level], ...(benign[level] ?? [])]);
  for (const k of Object.keys(o)) if (!known.has(k)) out.push(path === '' ? k : `${path}.${k}`);
}

function walkObjects(list: unknown, path: string, out: string[]): void {
  recs(list).forEach((o, i) => walkKeys(o, 'object', `${path}[${label(o, i)}]`, out));
}

/** Every key path in `document` that upstream's YAML reader never reads (and does not already flag). */
export function unrecognizedYamlPaths(document: unknown): string[] {
  const out: string[] = [];
  if (!isRec(document)) return out;
  walkKeys(document, 'root', '', out);
  recs(document.tables).forEach((t, i) => {
    const tp = `tables[${label(t, i)}]`;
    walkKeys(t, 'table', tp, out);
    if (isRec(t.base_table)) walkKeys(t.base_table, 'base_table', `${tp}.base_table`, out);
    if (isRec(t.primary_key)) walkKeys(t.primary_key, 'primary_key', `${tp}.primary_key`, out);
    recs(t.unique_keys).forEach((k, j) => walkKeys(k, 'unique_key', `${tp}.unique_keys[${j}]`, out));
    for (const kind of ['dimensions', 'time_dimensions', 'facts', 'metrics'] as const) walkObjects(t[kind], `${tp}.${kind}`, out);
  });
  walkObjects(document.metrics, 'metrics', out);
  recs(document.relationships).forEach((r, i) => {
    const rp = `relationships[${label(r, i)}]`;
    walkKeys(r, 'relationship', rp, out);
    recs(r.relationship_columns).forEach((c, j) => {
      const cp = `${rp}.relationship_columns[${j}]`;
      walkKeys(c, 'relationship_column', cp, out);
      if (isRec(c.right_range)) walkKeys(c.right_range, 'right_range', `${cp}.right_range`, out);
    });
  });
  return out;
}

export interface UnrecognizedKeysFinding {
  code: string;
  severity: 'warning';
  message: string;
  remediation: string;
  line: number;
  object: { kind: 'view'; name: string };
  source: 'embed';
}

/** One finding for a YAML view, or undefined when every key is read (or the text is not a parseable mapping). */
export function unrecognizedYamlKeyFinding(yamlText: string, viewName: string): UnrecognizedKeysFinding | undefined {
  let doc: unknown;
  try {
    doc = parseYaml(yamlText);
  } catch {
    return undefined; // the upstream reader reports the parse failure
  }
  const paths = unrecognizedYamlPaths(doc);
  if (paths.length === 0) return undefined;
  const shown = paths.slice(0, MAX_PATHS).join(', ');
  const more = paths.length > MAX_PATHS ? ` (and ${paths.length - MAX_PATHS} more)` : '';
  const nested = paths.filter(p => /^tables\[.*\]\.relationships$/.test(p));
  let message = `${paths.length} YAML key path${paths.length === 1 ? ' is' : 's are'} not part of the Snowflake semantic view spec the converter reads, so ${paths.length === 1 ? 'it was' : 'they were'} ignored and NOT converted: ${shown}${more}.`;
  if (nested.length > 0) {
    message +=
      ` Relationships declared inside a table (${nested.join(', ')}) are not part of the Snowflake semantic view spec and were NOT converted;` +
      ' move them to the top-level `relationships` list with `left_table` / `right_table` / `relationship_columns`.';
  }
  return {
    code: UNRECOGNIZED_YAML_KEY,
    severity: 'warning',
    message,
    remediation: 'Fix or remove the listed keys (check for typos such as `dimensons`) and convert again; the converted model is missing whatever they described.',
    line: 1,
    object: { kind: 'view', name: viewName },
    source: 'embed',
  };
}
