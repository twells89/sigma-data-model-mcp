/**
 * Embed layer over the unmodified semantic-tools Snowflake-semantic-view ->
 * Sigma data model engine (https://github.com/sigmacomputing/semantic-tools).
 *
 * Bundled by scripts/build-semantic-tools.mjs into two single-file builds
 * (ESM for Node, IIFE `SemanticTools` for the browser). This file composes the
 * engine's EXPORTED package APIs the same way packages/cli/src/{registry,main}.ts
 * do, and adds two opt-in post-processors that are OFF by default plus the
 * `crossTableReferences` correction (default 'derived'; with it 'engine' and the opt-ins off, `spec` is
 * byte-identical to core `convert()` output).
 *
 * Must stay browser-safe: no node: imports, no process/Buffer/require.
 */
import { convert, formatFinding, mkFinding, sortFindings } from '@semantic-tools/core';
import type {
  ColumnTypes_t,
  ConversionResult_t,
  ConvertOptions_t,
  Finding_t,
  Ir_t,
} from '@semantic-tools/core';
import { SIGMA_DATA_MODEL_WRITER, mkColumnId, mkElementId, mkMetricId, physicalTablesOf, toDisplayName } from '@semantic-tools/sigma-data-model';
import {
  mkSource,
  parseSemanticView,
  repairCsvQuoteDoubling,
  SNOWFLAKE_SEMANTIC_VIEW_READER,
  SNOWFLAKE_SEMANTIC_VIEW_YAML_READER,
  splitStatements,
  splitYamlStatements,
} from '@semantic-tools/snowflake-semantic-view';
import { SNOWFLAKE_TO_SIGMA_TRANSLATOR } from '@semantic-tools/snowflake-sigma';
import { parse as parseYaml } from 'yaml';

import { EMBED_VERSION } from './version.js';
import { applyCrossTableReferences } from './crossTable.js';
import { applyUsingPaths } from './usingPaths.js';
import { joinTypeInnerFindings, unrecognizedYamlKeyFinding } from './yamlKeys.js';
import { inferFormats, type ColFact, type SourceFacts } from './formats.js';

declare const __SEMANTIC_TOOLS_COMMIT__: string;
declare const __EMBED_VERSION__: string;

export const ENGINE = {
  semanticToolsCommit: typeof __SEMANTIC_TOOLS_COMMIT__ === 'undefined' ? 'dev' : __SEMANTIC_TOOLS_COMMIT__,
  embedVersion: typeof __EMBED_VERSION__ === 'undefined' ? EMBED_VERSION : __EMBED_VERSION__,
} as const;

/** Exposed for tests: the 'derived' post-processor over a spec JSON text. */
export { applyCrossTableReferences, applyUsingPaths, mkElementId };
export { relDisplaySegment } from './relDisplay.js';
/** Exposed for tests: the evidence-based number-format inference and its building blocks. */
export { classifyBase, inferFormats, parseFormula, unitOfFormula, typeKind, nameTokens, sampleStats } from './formats.js';

export type SourceFormat = 'snowflake-semantic-view' | 'snowflake-semantic-view-yaml';

// ── Ported verbatim from upstream (hashes tracked in semantic-tools-embed/UPSTREAM_PORTS.json) ──

/**
 * Ported verbatim from sigmacomputing/semantic-tools @ 360eb6a,
 * packages/cli/src/registry.ts `detectFromByContent` (return values are the
 * registry's `--from` keys, which are the same strings as `SourceFormat`).
 * Content sniff: DDL opens with CREATE [OR REPLACE] SEMANTIC VIEW; native YAML
 * opens with a top-level `name:` key.
 */
export function detectFormat(source: string): SourceFormat | undefined {
  const head = source.slice(0, 2000);
  if (/^\s*CREATE\s+(OR\s+REPLACE\s+)?SEMANTIC\s+VIEW\b/i.test(head)) {
    return 'snowflake-semantic-view';
  }
  if (/^\s*name\s*:/m.test(head)) {
    return 'snowflake-semantic-view-yaml';
  }
  return undefined;
}

/**
 * Embed-layer extension (NOT upstream): the CLI mostly picks the format from the file extension and only
 * sniffs content as a fallback, so it never has to cope with SQL comments before CREATE SEMANTIC VIEW.
 * Pasted DDL commonly starts with `-- ...` / block comments; strip those and retry the verbatim sniff.
 */
function detectFormatLenient(source: string): SourceFormat | undefined {
  const direct = detectFormat(source);
  if (direct !== undefined) return direct;
  const stripped = source.replace(/^(?:\s+|--[^\n]*(?:\n|$)|\/\*[\s\S]*?\*\/)+/, '');
  return stripped === source ? undefined : detectFormat(stripped);
}

/**
 * Ported verbatim from sigmacomputing/semantic-tools @ 360eb6a,
 * packages/cli/src/main.ts `unqualifiedReferences`.
 * A physical table reference needs `database` below three parts and `schema`
 * below two. `raw`, so messages name the reference as written.
 */
function unqualifiedReferences(ir: Ir_t): { database: string[]; schema: string[] } {
  const database: string[] = [];
  const schema: string[] = [];
  for (const table of ir.tables) {
    const partCount = table.table.parts.length;
    if (partCount < 3) database.push(table.table.raw);
    if (partCount < 2) schema.push(table.table.raw);
  }
  return { database, schema };
}

// ── Triples (mirrors packages/cli/src/registry.ts REGISTRY) ──

interface Triple {
  reader: typeof SNOWFLAKE_SEMANTIC_VIEW_READER;
  splitStatements: (source: string) => string[];
  prepareSource: (source: string) => { source: string; repairedCount: number };
}

const TRIPLES: Record<SourceFormat, Triple> = {
  'snowflake-semantic-view': {
    reader: SNOWFLAKE_SEMANTIC_VIEW_READER,
    splitStatements,
    prepareSource: source => ({ source, repairedCount: 0 }),
  },
  'snowflake-semantic-view-yaml': {
    reader: SNOWFLAKE_SEMANTIC_VIEW_YAML_READER,
    splitStatements: splitYamlStatements,
    prepareSource: source => {
      const repaired = repairCsvQuoteDoubling(source);
      return { source: repaired.text, repairedCount: repaired.count };
    },
  },
};

// ── Public types ──

export interface ConvertSemanticViewOptions {
  /** Required. Callers working offline pass a placeholder. */
  connectionId: string;
  database?: string;
  schema?: string;
  /** `{ 'DB.SCHEMA.TABLE': { COLUMN: 'TIMESTAMP_TZ', ... } }`. Omit to skip zone checks (adds COLUMN_TYPES_SKIPPED). */
  columnTypes?: Record<string, Record<string, string>>;
  bestEffort?: boolean;
  /** Skip detection and force a format. */
  format?: SourceFormat;
  /** Append "Also known as: a, b" to element/column/metric descriptions. Default false. */
  synonymsInDescription?: boolean;
  /**
   * Add `format` (currency / integer / percent) to metrics and calculated columns, only where the evidence supports it:
   * column type (YAML `data_type` or `columnTypes`), `sample_values`, word-level name tokens, propagated through the
   * formula. Ambiguous items get no format (FORMATS_NOT_INFERRED says why). Default false.
   */
  inferNumberFormats?: boolean;
  /**
   * 'derived' (default): columns/metrics that use a cross-table reference `[T/relationship_id/column_id]` (which Sigma
   * rejects on the owning element) move to a derived `<element>__joined` element using the spelling Sigma resolves.
   * 'engine': raw engine output, byte-identical to core convert().
   */
  crossTableReferences?: 'derived' | 'engine';
  /**
   * Only acts with `bestEffort`. 'bound' (default): each metric pinned with `USING (relationship, ...)` (which the engine
   * refuses, METRIC_JOIN_PATH_SELECTOR, and under bestEffort emits as a plain metric identical to the unpinned one) moves to a
   * derived `<FACT> (via <relationship>)` element that joins ONLY through the pinned relationship(s), so its numbers follow
   * Snowflake's USING path. 'engine': raw engine output (the plain metric stays on the fact).
   */
  usingPaths?: 'bound' | 'engine';
}

export interface EmbedFinding {
  code: string;
  severity: string;
  message: string;
  remediation?: string;
  /** Source line. Absent for YAML input (upstream reads YAML without line tracking and reports every finding at line 1) and for embed-layer findings. */
  line?: number;
  object: { kind: string; name: string; owner?: string };
  /** 'embed' for findings raised by this embed layer (not an upstream engine code), e.g. NEEDS_DATABASE. */
  source?: 'embed';
}

export interface ViewResult {
  viewName: string;
  ok: boolean;
  /** POST-ready JSON text when ok. */
  spec?: string;
  warnings: Finding_t[];
  errors: Finding_t[];
  overriddenErrors: Finding_t[];
  bestEffortWouldConvert: boolean;
  /** Present (ok:false) when database/schema were needed but not supplied; nothing was converted. */
  needs?: { database: string[]; schema: string[] };
  /** Physical tables the spec reads, for a column-type lookup (only when ok). */
  tablesForColumnTypes?: string[][];
  findings: EmbedFinding[];
}

export interface ConvertSemanticViewResult {
  engine: typeof ENGINE;
  format: SourceFormat | undefined;
  repairedCount: number;
  results: ViewResult[];
}

// ── Helpers ──

const OBJ_NONE = { kind: 'view' as const, name: '' };

function toEmbedFindings(list: readonly Finding_t[]): EmbedFinding[] {
  return list.map(f => {
    const text = formatFinding(f);
    return {
      code: f.code,
      severity: f.severity,
      ...text,
      line: f.line,
      object: f.object,
    };
  });
}

function mkResult(
  base: Omit<ViewResult, 'findings'>,
): ViewResult {
  return {
    ...base,
    findings: toEmbedFindings([...base.errors, ...base.overriddenErrors, ...base.warnings]),
  };
}

/** Embed-layer findings (source:'embed') for a `needs` result: database/schema required but not supplied. */
function needsFindings(viewName: string, needs: { database: string[]; schema: string[] }): EmbedFinding[] {
  const out: EmbedFinding[] = [];
  const mk = (code: string, what: 'database' | 'schema', refs: string[]) => {
    if (refs.length === 0) return;
    out.push({
      code,
      severity: 'error',
      message: `These table references are not fully qualified, so a ${what} is required but was not supplied: ${refs.join(', ')}.`,
      remediation: `Supply \`${what}\` (the ${what} that holds these tables) and convert again.`,
      line: 1,
      object: { kind: 'view', name: viewName },
      source: 'embed',
    });
  };
  mk('NEEDS_DATABASE', 'database', needs.database);
  mk('NEEDS_SCHEMA', 'schema', needs.schema);
  return out;
}

function failureResult(viewName: string, diagnostic: string): ViewResult {
  return mkResult({
    viewName,
    ok: false,
    warnings: [],
    errors: [mkFinding('MALFORMED_DDL', OBJ_NONE, 1, { diagnostic })],
    overriddenErrors: [],
    bestEffortWouldConvert: false,
  });
}

function errMsg(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

/** Raw core `convert()` for one statement — the reference the embed layer must equal when opt-ins are off. */
export function rawConvert(
  source: string,
  format: SourceFormat,
  options: ConvertOptions_t,
): ConversionResult_t {
  const t = TRIPLES[format];
  return convert({
    reader: t.reader,
    translator: SNOWFLAKE_TO_SIGMA_TRANSLATOR,
    writer: SIGMA_DATA_MODEL_WRITER,
    source,
    options,
  });
}

// ── Synonym extraction (the IR keeps only a COUNT, so the text is re-read from the source) ──

interface Synonyms {
  /** table alias (resolved, as in the IR) -> synonyms */
  tables: Map<string, string[]>;
  /** `${kind}:${owner ?? ''}.${name}` -> synonyms */
  objects: Map<string, string[]>;
}

const objKey = (kind: string, owner: string | undefined, name: string) => `${kind}:${owner ?? ''}.${name}`;

function strArray(v: unknown): string[] {
  return Array.isArray(v)
    ? v.filter((x): x is string | number => typeof x === 'string' || typeof x === 'number').map(String)
    : [];
}

function rec(v: unknown): Record<string, unknown>[] {
  return Array.isArray(v)
    ? v.filter((x): x is Record<string, unknown> => typeof x === 'object' && x !== null && !Array.isArray(x))
    : [];
}

function yamlSynonyms(source: string): Synonyms {
  const out: Synonyms = { tables: new Map(), objects: new Map() };
  const doc = parseYaml(source) as Record<string, unknown> | null;
  if (typeof doc !== 'object' || doc === null) return out;
  const add = (m: Map<string, string[]>, k: string, syns: string[]) => {
    if (syns.length > 0) m.set(k, syns);
  };
  for (const table of rec(doc.tables)) {
    const alias = String(table.name);
    add(out.tables, alias, strArray(table.synonyms));
    const kinds: [string, unknown][] = [
      ['fact', table.facts],
      ['dimension', table.dimensions],
      ['dimension', table.time_dimensions],
      ['metric', table.metrics],
    ];
    for (const [kind, list] of kinds) {
      for (const o of rec(list)) add(out.objects, objKey(kind, alias, String(o.name)), strArray(o.synonyms));
    }
  }
  for (const o of rec(doc.metrics)) add(out.objects, objKey('metric', undefined, String(o.name)), strArray(o.synonyms));
  return out;
}

/**
 * Same-length copy of `text` with everything that is not code blanked to spaces: string-literal and quoted-identifier
 * contents (so keyword search can't match inside COMMENT/SYNONYM text) AND `-- ...EOL` / `/* ... *\/` comments (so a
 * commented-out `WITH SYNONYMS (...)` is not read as real and an apostrophe in a comment cannot open a string).
 * Mirrors the upstream lexer: `--` and block comments only, `\'` escapes and doubled delimiters inside strings.
 */
function maskNonCode(text: string): string {
  const out: string[] = [];
  const blank = (from: number, to: number): void => {
    for (let k = from; k < to; k++) out.push(text[k] === '\n' ? '\n' : ' ');
  };
  let i = 0;
  while (i < text.length) {
    const c = text[i]!;
    if (c === '-' && text[i + 1] === '-') {
      const eol = text.indexOf('\n', i);
      const end = eol === -1 ? text.length : eol;
      blank(i, end);
      i = end;
    } else if (c === '/' && text[i + 1] === '*') {
      const close = text.indexOf('*/', i + 2);
      const end = close === -1 ? text.length : close + 2;
      blank(i, end);
      i = end;
    } else if (c === "'" || c === '"') {
      out.push(c);
      i++;
      while (i < text.length) {
        if (c === "'" && text[i] === '\\') { blank(i, Math.min(i + 2, text.length)); i += 2; continue; }
        if (text[i] === c && text[i + 1] === c) { out.push(' ', ' '); i += 2; continue; }
        if (text[i] === c) break;
        blank(i, i + 1);
        i++;
      }
      if (i < text.length) { out.push(c); i++; }
    } else {
      out.push(c);
      i++;
    }
  }
  return out.join('');
}

/**
 * Quoted values of the first `<keyword> ( 'a', 'b' )` clause that sits in CODE (not in a comment or string) in `slice`;
 * undefined when absent. The clause is located and delimited on the masked text; the values come from the original text.
 */
function ddlQuotedList(slice: string, keyword: RegExp): string[] | undefined {
  const masked = maskNonCode(slice);
  const m = keyword.exec(masked);
  if (!m) return undefined;
  const result: string[] = [];
  let i = m.index + m[0].length;
  while (i < masked.length && masked[i] !== ')') {
    if (masked[i] === "'") {
      const close = masked.indexOf("'", i + 1);
      if (close < 0) break;
      result.push(slice.slice(i + 1, close).replace(/''/g, "'"));
      i = close;
    }
    i++;
  }
  return result;
}

/** Synonym strings from the first `WITH SYNONYMS [=] ( 'a', 'b' )` clause in `slice`. */
function ddlSynonymsIn(slice: string): string[] {
  return ddlQuotedList(slice, /\bWITH\s+SYNONYMS\s*=?\s*\(/i) ?? [];
}

function ddlSynonyms(statement: string): Synonyms {
  const out: Synonyms = { tables: new Map(), objects: new Map() };
  const parsed = parseSemanticView(mkSource(statement));
  if (!parsed.ok) return out;
  const view = parsed.view;
  for (const t of view.tables) {
    const s = ddlSynonymsIn(statement.slice(t.span.start, t.span.end));
    if (s.length > 0) out.tables.set(t.alias.name, s);
  }
  for (const o of [...view.facts, ...view.dimensions, ...view.metrics]) {
    const s = ddlSynonymsIn(statement.slice(o.span.start, o.span.end));
    if (s.length > 0) out.objects.set(objKey(o.kind, o.owner?.name, o.name.name), s);
  }
  return out;
}

/** DMM's snowBuildDesc (sigma-data-model-manager/index.html): base + '. ' + suffix, or suffix alone (see note below on the join). */
function buildDesc(description: string | undefined, synonyms: string[]): string | undefined {
  const base = (description ?? '').trim();
  const syns = synonyms.map(s => s.trim()).filter(Boolean);
  if (syns.length === 0) return base || undefined;
  const suffix = 'Also known as: ' + syns.join(', ');
  // DMM joins with '. '; avoid a doubled period when the base already ends a sentence.
  return base ? base + (/[.!?]$/.test(base) ? ' ' : '. ') + suffix : suffix;
}

type Json = Record<string, any>;

/** Return a copy of `o` with `description` set — in place if present, else at `after` position (or end). */
function withDescription(o: Json, description: string, afterKey?: string): Json {
  if ('description' in o) return { ...o, description };
  const out: Json = {};
  let placed = false;
  for (const [k, v] of Object.entries(o)) {
    out[k] = v;
    if (afterKey !== undefined && k === afterKey) { out.description = description; placed = true; }
  }
  if (!placed) out.description = description;
  return out;
}

/**
 * Apply synonyms to a spec. Returns the new spec JSON text plus the set of
 * finding object keys (`kind:owner.name` / `table:alias`) whose synonyms were
 * successfully carried (their SYNONYMS_DROPPED warnings should be removed).
 */
function applySynonyms(specText: string, syn: Synonyms): { spec: string; applied: Set<string> } {
  const applied = new Set<string>();
  const spec = JSON.parse(specText) as Json;
  const elements: Json[] = spec.pages?.[0]?.elements ?? [];
  const byId = new Map<string, number>();
  elements.forEach((e, i) => byId.set(e.id, i));

  for (const [alias, syns] of syn.tables) {
    const idx = byId.get(mkElementId(alias));
    if (idx === undefined) continue;
    const el = elements[idx]!;
    const d = buildDesc(el.description, syns);
    if (d === undefined) continue;
    elements[idx] = withDescription(el, d, 'kind');
    applied.add(`table:${alias}`);
  }

  for (const [key, syns] of syn.objects) {
    const m = /^(fact|dimension|metric):([^.]*)\.(.*)$/s.exec(key);
    if (!m) continue;
    const [, kind, owner, name] = m as unknown as [string, string, string, string];
    if (kind === 'metric') {
      const display = toDisplayName(name);
      // Owned metric: id is deterministic. Derived metric (no owner): find it on whichever element carries it.
      const candidates = owner !== '' ? [byId.get(mkElementId(owner))] : elements.map((_, i) => i);
      for (const idx of candidates) {
        if (idx === undefined) continue;
        const el = elements[idx]!;
        const metrics: Json[] = el.metrics ?? [];
        const mi = metrics.findIndex(x => (owner !== '' ? x.id === mkMetricId(el.id, name) : x.name === display && String(x.id).includes('__m__')));
        if (mi < 0) continue;
        const d = buildDesc(metrics[mi]!.description, syns);
        if (d === undefined) continue;
        metrics[mi] = withDescription(metrics[mi]!, d);
        applied.add(key);
        break;
      }
    } else {
      const idx = byId.get(mkElementId(owner));
      if (idx === undefined) continue;
      const el = elements[idx]!;
      const cols: Json[] = el.columns ?? [];
      // Column ids come from the underlying warehouse column; a dimension whose name differs is emitted
      // as a renamed column (`name` = display name of the dimension), so fall back to matching on that.
      const display = toDisplayName(name);
      let ci = cols.findIndex(c => c.id === mkColumnId(el.id, name));
      if (ci < 0) ci = cols.findIndex(c => c.name === display);
      if (ci < 0) continue;
      const d = buildDesc(cols[ci]!.description, syns);
      if (d === undefined) continue;
      cols[ci] = withDescription(cols[ci]!, d);
      applied.add(key);
    }
  }
  return { spec: JSON.stringify(spec, null, 2), applied };
}

// ── Number-format evidence (type / sample values re-read from the source; the IR carries neither) ──

const SIMPLE_IDENT = /^(?:(\w+|"[^"]+")\s*\.\s*)?(\w+|"[^"]+")$/;

/** Warehouse column an expression names when it is a bare (optionally own-alias-qualified) column, else undefined. */
function bareColumn(expr: unknown, alias: string): string | undefined {
  if (typeof expr !== 'string') return undefined;
  const m = SIMPLE_IDENT.exec(expr.trim());
  if (!m) return undefined;
  const unq = (x: string) => (x.startsWith('"') ? x.slice(1, -1) : x.toUpperCase());
  if (m[1] !== undefined && unq(m[1]).toUpperCase() !== alias.toUpperCase()) return undefined;
  return unq(m[2]!);
}

function yamlFacts(source: string): SourceFacts {
  const out: SourceFacts = new Map();
  const doc = parseYaml(source) as Record<string, unknown> | null;
  if (typeof doc !== 'object' || doc === null) return out;
  for (const table of rec(doc.tables)) {
    const alias = String(table.name);
    const list: ColFact[] = [];
    for (const o of [...rec(table.dimensions), ...rec(table.time_dimensions), ...rec(table.facts)]) {
      const phys = bareColumn(o.expr, alias);
      list.push({
        name: String(o.name),
        ...(phys !== undefined && { phys }),
        ...(typeof o.data_type === 'string' && { dataType: o.data_type }),
        ...(Array.isArray(o.sample_values) && { samples: strArray(o.sample_values) }),
      });
    }
    out.set(alias, list);
  }
  return out;
}

/** Quoted strings of the first `SAMPLE_VALUES ( 'a', 'b' )` clause in `slice` (undefined when absent; comments ignored). */
function ddlSampleValuesIn(slice: string): string[] | undefined {
  return ddlQuotedList(slice, /\bSAMPLE_VALUES\s*\(/i);
}

function ddlFacts(statement: string): SourceFacts {
  const out: SourceFacts = new Map();
  const parsed = parseSemanticView(mkSource(statement));
  if (!parsed.ok) return out;
  for (const o of [...parsed.view.facts, ...parsed.view.dimensions]) {
    if (o.owner === null) continue;
    const alias = o.owner.name;
    const phys = bareColumn(o.expression, alias);
    const samples = ddlSampleValuesIn(statement.slice(o.span.start, o.span.end));
    const list = out.get(alias) ?? [];
    list.push({ name: o.name.name, ...(phys !== undefined && { phys }), ...(samples !== undefined && { samples }) });
    out.set(alias, list);
  }
  return out;
}

/** Exposed for the comparison script / tests: the type + sample-value evidence re-read from one statement's source text. */
export function formatFacts(statement: string, format: SourceFormat): SourceFacts {
  return format === 'snowflake-semantic-view-yaml' ? yamlFacts(statement) : ddlFacts(statement);
}

// ── Main entry ──

/**
 * Upstream's YAML reader has no line tracking (readYaml.ts ZERO_SPAN), so every YAML
 * finding except YAML_PARSE_FAILURE says "line 1", in `line` and inside the message.
 * Embed-layer findings carry a placeholder line too. Drop both rather than mislead.
 */
function withoutUnknownLine(f: EmbedFinding, format: SourceFormat | undefined): EmbedFinding {
  const unknown = f.source === 'embed' || (format === 'snowflake-semantic-view-yaml' && f.code !== 'YAML_PARSE_FAILURE');
  if (!unknown || f.line === undefined) return f;
  const at = ` at line ${f.line}`;
  const { line: _line, ...rest } = f;
  return {
    ...rest,
    message: rest.message.split(at).join(''),
    ...(rest.remediation !== undefined && { remediation: rest.remediation.split(at).join('') }),
  };
}

export function convertSemanticView(source: string, opts: ConvertSemanticViewOptions): ConvertSemanticViewResult {
  if (opts === undefined || opts === null || typeof opts.connectionId !== 'string' || opts.connectionId.trim() === '') {
    throw new Error('convertSemanticView requires a non-empty connectionId (pass a placeholder when offline)');
  }
  const text = typeof source === 'string' ? source : '';
  const format: SourceFormat | undefined =
    opts.format !== undefined && opts.format in TRIPLES ? opts.format : opts.format !== undefined ? undefined : detectFormatLenient(text);

  if (format === undefined) {
    return {
      engine: ENGINE,
      format: undefined,
      repairedCount: 0,
      results: [
        failureResult(
          '',
          opts.format !== undefined
            ? `Unsupported format "${String(opts.format)}"`
            : 'Could not detect the input format: expected a CREATE SEMANTIC VIEW statement or a semantic view YAML (top-level "name:").',
        ),
      ],
    };
  }

  const triple = TRIPLES[format];
  let repairedCount = 0;
  let statements: string[];
  try {
    const prepared = triple.prepareSource(text);
    repairedCount = prepared.repairedCount;
    statements = triple.splitStatements(prepared.source);
    // Nothing split out (e.g. whitespace only / no statement found): let the reader report the real diagnostic.
    if (statements.length === 0) statements = [prepared.source];
  } catch (e) {
    return { engine: ENGINE, format, repairedCount, results: [failureResult('', `Could not read input: ${errMsg(e)}`)] };
  }

  const columnTypesSkipped = opts.columnTypes === undefined;
  const columnTypes: ColumnTypes_t = (opts.columnTypes ?? {}) as ColumnTypes_t;
  const dbGiven = opts.database !== undefined && opts.database.trim() !== '';
  const schemaGiven = opts.schema !== undefined && opts.schema.trim() !== '';
  const options: ConvertOptions_t = {
    connectionId: opts.connectionId,
    ...(opts.database !== undefined && { database: opts.database }),
    ...(opts.schema !== undefined && { schema: opts.schema }),
    columnTypes,
    ...(opts.bestEffort && { bestEffort: true }),
  };

  const results: ViewResult[] = statements.map((statement, index) => {
    const fallbackName = `statement-${index + 1}`;
    try {
      const read = triple.reader.read(statement);
      if (!read.ok) {
        return mkResult({
          viewName: fallbackName,
          ok: false,
          warnings: [],
          errors: sortFindings(read.errors),
          overriddenErrors: [],
          bestEffortWouldConvert: false,
        });
      }
      const ir = read.ir;
      const viewName = ir.name.parts.at(-1)?.name ?? fallbackName;

      const refs = unqualifiedReferences(ir);
      const needDb = dbGiven ? [] : refs.database;
      const needSchema = schemaGiven ? [] : refs.schema;
      if (needDb.length > 0 || needSchema.length > 0) {
        const needs = { database: needDb, schema: needSchema };
        const needsResult = mkResult({
          viewName,
          ok: false,
          warnings: [],
          errors: [],
          overriddenErrors: [],
          bestEffortWouldConvert: false,
          needs,
        });
        needsResult.findings.push(...needsFindings(viewName, needs));
        return needsResult;
      }

      // Same call the CLI makes once it already holds the parsed IR (identical output to core convert()).
      const result = SIGMA_DATA_MODEL_WRITER.write(ir, SNOWFLAKE_TO_SIGMA_TRANSLATOR, options);

      // Run-level warnings the CLI appends after conversion, in the same order.
      if (columnTypesSkipped) result.warnings.push(mkFinding('COLUMN_TYPES_SKIPPED', OBJ_NONE, 1));
      if (repairedCount > 0) {
        result.warnings.push(mkFinding('YAML_QUOTE_REPAIR_APPLIED', OBJ_NONE, 1, { count: repairedCount }));
      }

      if (!result.ok) {
        return mkResult({
          viewName,
          ok: false,
          warnings: result.warnings,
          errors: result.errors,
          overriddenErrors: [],
          bestEffortWouldConvert: result.bestEffortWouldConvert,
        });
      }

      let spec = result.spec;
      let warnings = result.warnings;
      if (opts.synonymsInDescription === true) {
        const syn = format === 'snowflake-semantic-view-yaml' ? yamlSynonyms(statement) : ddlSynonyms(statement);
        const { spec: next, applied } = applySynonyms(spec, syn);
        spec = next;
        warnings = warnings.filter(
          w => !(w.code === 'SYNONYMS_DROPPED' && applied.has(w.object.kind === 'table' ? `table:${w.object.name}` : objKey(w.object.kind, w.object.owner, w.object.name))),
        );
      }
      let extra: EmbedFinding[] = [];
      if (opts.inferNumberFormats === true) {
        const facts = formatFacts(statement, format);
        const inferred = inferFormats(spec, { facts, columnTypes: opts.columnTypes, elementIdOf: mkElementId }, result.viewName);
        spec = inferred.spec;
        extra = inferred.findings;
      }

      if (opts.crossTableReferences !== 'engine') {
        const moved = applyCrossTableReferences(spec, result.viewName);
        spec = moved.spec;
        extra = [...extra, ...moved.findings];
      }

      // Best effort overrode USING-pinned metrics: bind each to a path-specific derived element (after crossTable: it reads the final elements).
      if (opts.bestEffort === true && opts.usingPaths !== 'engine') {
        const pinned = result.overriddenErrors
          .filter(f => f.code === 'METRIC_JOIN_PATH_SELECTOR' && f.object.kind === 'metric' && f.object.owner !== undefined)
          .map(f => ({
            owner: f.object.owner as string,
            name: f.object.name,
            using: ((f as { details?: { relationships?: string[] } }).details?.relationships ?? []).slice(),
          }));
        if (pinned.length > 0) {
          const bound = applyUsingPaths(spec, {
            viewName: result.viewName,
            pinned,
            relationships: ir.relationships.map(r => ({
              name: r.name?.name ?? `${r.sourceAlias.name}_TO_${r.targetAlias.name}`,
              sourceAlias: r.sourceAlias.name,
              targetAlias: r.targetAlias.name,
              sourceColumns: r.sourceColumns.map(c => c.name),
            })),
          });
          spec = bound.spec;
          extra = [...extra, ...bound.findings];
        }
      }

      const okResult = mkResult({
        viewName: result.viewName,
        ok: true,
        spec,
        warnings,
        errors: [],
        overriddenErrors: result.overriddenErrors,
        bestEffortWouldConvert: false,
        tablesForColumnTypes: physicalTablesOf(result.spec),
      });
      okResult.findings.push(...extra);
      return okResult;
    } catch (e) {
      return failureResult(fallbackName, `Internal error converting statement ${index + 1}: ${errMsg(e)}`);
    }
  });

  // YAML only: upstream silently ignores keys it does not read; surface them (one finding per view).
  if (format === 'snowflake-semantic-view-yaml') {
    results.forEach((r, i) => {
      const f = unrecognizedYamlKeyFinding(statements[i]!, r.viewName);
      if (f !== undefined) r.findings.push(f);
      r.findings.push(...joinTypeInnerFindings(statements[i]!));
    });
  }

  for (const r of results) {
    r.findings = r.findings.map(f => withoutUnknownLine(f, format));
    // The raw Finding_t arrays are exposed too (DMM matches overridden errors against findings): same stripping, so they agree.
    if (format === 'snowflake-semantic-view-yaml') {
      const strip = (list: Finding_t[]): Finding_t[] =>
        list.map(f => {
          if (f.code === 'YAML_PARSE_FAILURE' || (f as { line?: number }).line === undefined) return f;
          const { line: _line, ...rest } = f as Finding_t & { line?: number };
          return rest as Finding_t;
        });
      r.errors = strip(r.errors);
      r.warnings = strip(r.warnings);
      r.overriddenErrors = strip(r.overriddenErrors);
    }
  }
  return { engine: ENGINE, format, repairedCount, results };
}
