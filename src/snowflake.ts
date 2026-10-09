/**
 * Snowflake Semantic View → Sigma Data Model converter.
 *
 * Thin adapter over the bundled semantic-tools engine (vendor/semantic-tools,
 * built from sigmacomputing/semantic-tools by `npm run build:semantic-tools`).
 * Accepts Snowflake semantic view YAML or CREATE SEMANTIC VIEW DDL. Metrics come
 * ONLY from the semantic view's own definitions (nothing is invented), and
 * constructs that would return different numbers than Snowflake are refused
 * (unless bestEffort). Behaviour matches the semantic-tools CLI.
 */

import {
  convertSemanticView,
  ENGINE,
  type EmbedFinding,
  type ViewResult,
} from '../vendor/semantic-tools/semantic-tools-embed.mjs';
import type { SigmaDataModel, ConversionResult } from './sigma-ids.js';

export interface SnowflakeConvertOptions {
  /** Sigma connection UUID. Empty/omitted -> placeholder + warning. */
  connectionId?: string;
  /** Needed only when the view has table refs with fewer than 3 parts. */
  database?: string;
  /** Needed only when the view has table refs with fewer than 2 parts. */
  schema?: string;
  /** `{ 'DB.SCHEMA.TABLE': { COL: 'TIMESTAMP_TZ' } }` — enables time-zone checks. */
  columnTypes?: Record<string, Record<string, string>>;
  /** Emit constructs that would otherwise be refused (differences are reported). */
  bestEffort?: boolean;
  /** Append "Also known as: a, b" to descriptions (default true, as the original browser tool did; Sigma has no synonyms field). Pass false to drop them. */
  synonymsInDescription?: boolean;
  /** Add number formats to metrics / calculated columns. */
  inferNumberFormats?: boolean;
  /**
   * 'derived' (default): columns/metrics using a cross-table reference move to a derived `<element>__joined`
   * element (the only place Sigma resolves them). 'engine': raw semantic-tools output.
   */
  crossTableReferences?: 'derived' | 'engine';
  /** @deprecated Accepted and ignored: metrics now come only from the view's own definitions. */
  autoMetrics?: boolean;
}

export interface SnowflakeViewResult {
  viewName: string;
  ok: boolean;
  /** Parsed spec (present only when ok). */
  model?: SigmaDataModel;
  /** Spec counts for this view (present only when ok). */
  stats?: Record<string, number>;
  findings: EmbedFinding[];
  bestEffortWouldConvert: boolean;
  needs?: { database: string[]; schema: string[] };
  overriddenErrors: string[];
}

export interface SnowflakeConversionResult extends Omit<ConversionResult, 'model'> {
  /** Spec of the first successfully converted view (back-compat; with several views read `results[].model`); absent when every view was refused (never a partial/failed model). */
  model?: SigmaDataModel;
  /** True when ANY view was refused (a partial result is still a refusal: check `refusedViews`). */
  refused: boolean;
  /** True when NO view converted. */
  allRefused: boolean;
  /** Names of the refused views. */
  refusedViews: string[];
  /** `stats` is the SUM over every converted view; per-view counts are in `results[].stats`. */
  /** True when at least one refused view would convert with bestEffort. */
  bestEffortWouldConvert: boolean;
  /** database/schema the caller must supply (merged across views); absent when nothing is needed. */
  needs?: { database: string[]; schema: string[] };
  findings: EmbedFinding[];
  results: SnowflakeViewResult[];
  engine: { semanticToolsCommit: string; embedVersion: string };
}

export const CONNECTION_PLACEHOLDER = '<CONNECTION_ID>';
export const CONNECTION_WARNING = '⚠ Connection ID not set — update in JSON before saving to Sigma';
export const AUTO_METRICS_DEPRECATION =
  'auto_metrics is no longer supported: metrics now come only from the semantic view\'s own definitions';

function specStats(model: SigmaDataModel | undefined): Record<string, number> {
  const elements: any[] = (model?.pages ?? []).flatMap((p: any) => p.elements ?? []);
  const sum = (key: string) => elements.reduce((n, e) => n + (e[key]?.length ?? 0), 0);
  return {
    tables: elements.filter(e => e.source?.kind === 'warehouse-table').length,
    elements: elements.length,
    columns: sum('columns'),
    metrics: sum('metrics'),
    relationships: sum('relationships'),
  };
}

function sumStats(all: (Record<string, number> | undefined)[]): Record<string, number> {
  const out = specStats(undefined);
  for (const st of all) for (const [k, v] of Object.entries(st ?? {})) out[k] = (out[k] ?? 0) + v;
  return out;
}

function mergeNeeds(results: ViewResult[]): { database: string[]; schema: string[] } | undefined {
  const database = new Set<string>();
  const schema = new Set<string>();
  for (const r of results) {
    r.needs?.database.forEach(x => database.add(x));
    r.needs?.schema.forEach(x => schema.add(x));
  }
  return database.size || schema.size ? { database: [...database], schema: [...schema] } : undefined;
}

export function convertSnowflakeSemanticView(
  source: string,
  options: SnowflakeConvertOptions = {},
): SnowflakeConversionResult {
  const connectionId = (options.connectionId ?? '').trim();
  const run = convertSemanticView(source, {
    connectionId: connectionId || CONNECTION_PLACEHOLDER,
    ...(options.database?.trim() && { database: options.database.trim() }),
    ...(options.schema?.trim() && { schema: options.schema.trim() }),
    ...(options.columnTypes && { columnTypes: options.columnTypes }),
    ...(options.bestEffort && { bestEffort: true }),
    ...(options.synonymsInDescription !== false && { synonymsInDescription: true }),
    ...(options.inferNumberFormats && { inferNumberFormats: true }),
    crossTableReferences: options.crossTableReferences === 'engine' ? 'engine' : 'derived',
  });

  const multi = run.results.length > 1;
  const results: SnowflakeViewResult[] = run.results.map(r => ({
    viewName: r.viewName,
    ok: r.ok,
    // The engine's spec carries no top-level name; the semantic-tools CLI names a
    // deployed model after the raw view name (main.ts, deployTarget.name ?? viewName).
    ...(r.ok && r.spec !== undefined && {
      model: { ...(r.viewName && { name: r.viewName }), ...JSON.parse(r.spec) } as SigmaDataModel,
    }),
    ...(r.ok && r.spec !== undefined && { stats: specStats(JSON.parse(r.spec)) }),
    findings: r.findings,
    bestEffortWouldConvert: r.bestEffortWouldConvert,
    ...(r.needs && { needs: r.needs }),
    overriddenErrors: r.overriddenErrors.map(f => f.code),
  }));

  const firstOk = results.find(r => r.ok && r.model !== undefined);
  const findings = run.results.flatMap(r => r.findings);
  const warnings: string[] = [];
  if (!connectionId) warnings.push(CONNECTION_WARNING);
  if (options.autoMetrics !== undefined) warnings.push(AUTO_METRICS_DEPRECATION);
  for (const r of run.results) {
    for (const f of r.findings) {
      warnings.push(`${multi ? `[${r.viewName || 'view'}] ` : ''}${f.code}: ${f.message}`);
    }
  }

  const needs = mergeNeeds(run.results);
  return {
    ...(firstOk && { model: firstOk.model }),
    warnings,
    stats: sumStats(results.map(r => r.stats)),
    refused: results.some(r => !r.ok) || results.length === 0,
    allRefused: firstOk === undefined,
    refusedViews: results.filter(r => !r.ok).map(r => r.viewName || '(unnamed)'),
    bestEffortWouldConvert: run.results.some(r => r.bestEffortWouldConvert),
    ...(needs && { needs }),
    findings,
    results,
    engine: { ...ENGINE },
  };
}
