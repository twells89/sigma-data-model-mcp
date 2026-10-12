// Hand-written declarations for semantic-tools-embed.mjs (source: semantic-tools-embed/src/index.ts).
// Keep in sync with the exported surface of that file.

export type SourceFormat = 'snowflake-semantic-view' | 'snowflake-semantic-view-yaml';

export interface Finding {
  code: string;
  severity: 'error' | 'warning';
  object: { kind: 'view' | 'table' | 'relationship' | 'fact' | 'dimension' | 'metric'; name: string; owner?: string };
  line: number;
  details?: Record<string, unknown>;
}

export interface EmbedFinding {
  code: string;
  severity: string;
  message: string;
  remediation?: string;
  /** Absent for YAML input (no line tracking upstream; YAML_PARSE_FAILURE keeps its real line) and for embed-layer findings. */
  line?: number;
  object: { kind: string; name: string; owner?: string };
  /** 'embed' for findings raised by the embed layer itself (e.g. NEEDS_DATABASE / NEEDS_SCHEMA), not an upstream engine code. */
  source?: 'embed';
}

export interface ConvertSemanticViewOptions {
  /** Required. Callers working offline pass a placeholder. */
  connectionId: string;
  database?: string;
  schema?: string;
  /** `{ 'DB.SCHEMA.TABLE': { COLUMN: 'TIMESTAMP_TZ' } }`. Omit to skip zone checks (adds COLUMN_TYPES_SKIPPED). */
  columnTypes?: Record<string, Record<string, string>>;
  bestEffort?: boolean;
  /** Skip content detection and force a format. */
  format?: SourceFormat;
  /** Append "Also known as: a, b" to element/column/metric descriptions. Default false. */
  synonymsInDescription?: boolean;
  /**
   * Add `format` (currency / integer / percent) to metrics and calculated columns, only where the evidence supports it:
   * column type (YAML `data_type` or `columnTypes`), `sample_values`, word-level name tokens, propagated through the
   * formula. Ambiguous items get no format (FORMATS_NOT_INFERRED says why). Default false.
   */
  inferNumberFormats?: boolean;
  /** 'derived' (default): cross-table columns/metrics move to a derived `<element>__joined` element (Sigma only resolves them there). 'engine': raw engine output. */
  crossTableReferences?: 'derived' | 'engine';
  /** Only with bestEffort. 'bound' (default): USING-pinned metrics move to `<FACT> (via <relationship>)` derived elements joined only through the pinned relationship(s). 'engine': raw engine output. */
  usingPaths?: 'bound' | 'engine';
}

export interface ViewResult {
  viewName: string;
  ok: boolean;
  /** POST-ready JSON text when ok. */
  spec?: string;
  warnings: Finding[];
  errors: Finding[];
  overriddenErrors: Finding[];
  bestEffortWouldConvert: boolean;
  /** Set (ok:false, nothing converted) when database/schema were required but not supplied. */
  needs?: { database: string[]; schema: string[] };
  /** Physical tables the spec reads, for a column-type lookup (only when ok). */
  tablesForColumnTypes?: string[][];
  findings: EmbedFinding[];
}

export interface ConvertSemanticViewResult {
  engine: { readonly semanticToolsCommit: string; readonly embedVersion: string };
  format: SourceFormat | undefined;
  repairedCount: number;
  results: ViewResult[];
}

export declare const ENGINE: { readonly semanticToolsCommit: string; readonly embedVersion: string };
export declare function detectFormat(source: string): SourceFormat | undefined;
/** Throws only when connectionId is missing/blank; malformed input yields ok:false results. */
export declare function convertSemanticView(source: string, opts: ConvertSemanticViewOptions): ConvertSemanticViewResult;
/** Low-level: core convert() for one statement. Used by tests as the byte-for-byte reference. */
export declare function rawConvert(
  source: string,
  format: SourceFormat,
  options: { connectionId: string; database?: string; schema?: string; columnTypes: Record<string, Record<string, string>>; bestEffort?: boolean },
): { ok: boolean; spec?: string; viewName?: string; errors?: Finding[]; warnings: Finding[]; overriddenErrors?: Finding[]; bestEffortWouldConvert?: boolean };

/** Number-format inference building blocks (exposed for tests and the comparison script). */
export type Unit = 'currency' | 'fraction' | 'integer' | 'plain' | 'scalar' | 'id' | 'nonnumeric' | 'unknown';
export interface UnitResult { u: Unit; v?: number; why: string }
export interface ColFact { name: string; phys?: string; dataType?: string; samples?: string[] }
export type SourceFacts = Map<string, ColFact[]>;
export declare function formatFacts(statement: string, format: SourceFormat): SourceFacts;
export declare function mkElementId(alias: string): string;
export declare function classifyBase(ev: { names: string[]; dataType?: string; samples?: string[] }): UnitResult;
/** `itemName`: the name of the metric/column being typed; integer/integer and plain/plain quotients are a ratio only when it has a percent word (rate, pct, percent, percentage, ratio, share). */
export declare function unitOfFormula(formula: string, refs: Record<string, Unit | UnitResult>, itemName?: string): UnitResult;
export declare function parseFormula(src: string): unknown;
export declare function typeKind(t: string | undefined): 'int' | 'dec' | 'num' | 'other' | undefined;
export declare function nameTokens(names: readonly string[]): Set<string>;
export declare function sampleStats(samples: readonly string[] | undefined): { numeric: boolean; min: number; max: number; allInt: boolean } | undefined;
export declare function inferFormats(
  specText: string,
  opts: { facts: SourceFacts; columnTypes?: Record<string, Record<string, string>>; elementIdOf: (alias: string) => string },
  viewName: string,
): { spec: string; findings: EmbedFinding[]; items: { owner: string; name: string; unit: Unit; format?: Record<string, unknown>; legacy?: Record<string, unknown> | null; why: string }[] };
export interface UsingPathsInput {
  viewName: string;
  pinned: { owner: string; name: string; using: string[] }[];
  relationships: { name: string; sourceAlias: string; targetAlias: string; sourceColumns: string[] }[];
}
export declare function applyUsingPaths(specText: string, input: UsingPathsInput): { spec: string; findings: EmbedFinding[] };
export declare function applyCrossTableReferences(specText: string, viewName: string): { spec: string; findings: EmbedFinding[] };
