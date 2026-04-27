/**
 * Shared tool and prompt registration for the Sigma Data Model MCP Server.
 * Used by both the stdio (local) and HTTP (remote) entry points.
 */

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';

import { convertDbtToSigma } from './dbt.js';
import { convertSnowflakeSemanticView } from './snowflake.js';
import { convertLookMLToSigma, parseLookML } from './lookml.js';
import { convertPowerBIToSigma } from './powerbi.js';
import { convertTableauToSigma } from './tableau.js';
import { convertOmniToSigma } from './omni.js';
import { convertSqlToSigma } from './sql.js';
import { convertThoughtSpotToSigma } from './thoughtspot.js';
import { convertQlikToSigma } from './qlik.js';
import { convertAtlanToSigma } from './atlan.js';
import { convertAlteryxToSigma } from './alteryx.js';
import { convertOacToSigma } from './oac.js';
import { lookSqlToSigmaRules, tableauFormulaToSigma, lookConvertExpression } from './formulas.js';
import { DATA_MODEL_SCHEMA_SUMMARY, sigmaDisplayName } from './sigma-ids.js';
import { registerResources } from './resources.js';

/** Create and configure a new MCP server with all tools and prompts registered. */
export function createSigmaServer(): McpServer {
  const server = new McpServer({
    name: 'sigma-data-model',
    version: '1.0.0',
  });

  registerTools(server);
  registerPrompts(server);
  registerResources(server);

  return server;
}

function registerTools(server: McpServer): void {

  // ── convert_dbt_to_sigma ─────────────────────────────────────────────────

  server.tool(
    'convert_dbt_to_sigma',
    `Convert dbt semantic model YAML to Sigma Computing data model JSON.

Accepts dbt semantic_models YAML (from semantic_manifest.json, YAML files, or
dbt Cloud). Handles entities, dimensions, measures, metrics, and foreign entity
cross-references → Sigma relationships.

The output JSON can be POSTed to the Sigma API (POST /v2/dataModels/spec) to
create a data model, or PUT to /v2/dataModels/{id}/spec to update one.`,
    {
      yaml_content: z.string().describe('The dbt semantic model YAML content'),
      connection_id: z.string().describe('Sigma connection UUID (from GET /v2/connections); pass empty string to omit'),
      database: z.string().describe('Override database name (e.g. ANALYTICS); pass empty string to omit'),
      schema: z.string().describe('Override schema name (e.g. DBT_PROD); pass empty string to omit'),
    },
    async ({ yaml_content, connection_id, database, schema }) => {
      try {
        const result = convertDbtToSigma(yaml_content, {
          connectionId: connection_id || undefined,
          database: database || undefined,
          schema: schema || undefined,
        });
        return {
          content: [{
            type: 'text' as const,
            text: JSON.stringify({ sigmaDataModel: result.model, stats: result.stats, warnings: result.warnings }, null, 2),
          }],
        };
      } catch (e: any) {
        return { content: [{ type: 'text' as const, text: `Error: ${e.message}` }], isError: true };
      }
    }
  );

  // ── convert_snowflake_to_sigma ───────────────────────────────────────────

  server.tool(
    'convert_snowflake_to_sigma',
    `Convert Snowflake Semantic View YAML to Sigma Computing data model JSON.

Accepts Snowflake Cortex Analyst semantic view YAML definitions. Handles tables
with dimensions, time_dimensions, facts, primary keys, inline relationships,
and top-level relationships. Facts auto-generate Sum() metrics.`,
    {
      yaml_content: z.string().describe('The Snowflake semantic view YAML content'),
      connection_id: z.string().describe('Sigma connection UUID; pass empty string to omit'),
      auto_metrics: z.boolean().describe('Auto-generate Sum() metrics for fact columns (pass true unless you want to skip metrics)'),
    },
    async ({ yaml_content, connection_id, auto_metrics }) => {
      try {
        const result = convertSnowflakeSemanticView(yaml_content, {
          connectionId: connection_id || undefined,
          autoMetrics: auto_metrics,
        });
        return {
          content: [{
            type: 'text' as const,
            text: JSON.stringify({ sigmaDataModel: result.model, stats: result.stats, warnings: result.warnings }, null, 2),
          }],
        };
      } catch (e: any) {
        return { content: [{ type: 'text' as const, text: `Error: ${e.message}` }], isError: true };
      }
    }
  );

  // ── convert_lookml_to_sigma ──────────────────────────────────────────────

  server.tool(
    'convert_lookml_to_sigma',
    `Convert LookML project files to Sigma Computing data model JSON.

Provide one or more LookML files (views + model). Parses LookML, resolves
explores/joins, converts dimensions → columns, measures → metrics,
sql_on → relationships, and derived_table → Custom SQL elements.

${'{'}view.SQL_TABLE_NAME{'}'} substitution is fully resolved including N-hop alias
chains and PDT-referencing-PDT patterns (same PDT referenced multiple times
with different SQL aliases is handled correctly).

include: directives are parsed and listed in warnings — resolution is limited
to the files provided; referenced views not in the input are silently skipped.

PDT materialization hints (distribution, sortkeys, datagroup_trigger,
persist_with, cluster_keys, partition_keys) are not converted and emit
informational warnings.

Pass files as an array of {name, content} objects.`,
    {
      files: z.array(z.object({
        name: z.string().describe('Filename (e.g. "sales.model.lkml" or "orders.view.lkml")'),
        content: z.string().describe('Full file content'),
      })).describe('Array of LookML files to parse'),
      connection_id: z.string().describe('Sigma connection UUID; pass empty string to omit'),
      explore_name: z.string().describe('Name of the explore to convert; pass empty string to auto-detect (uses first explore found)'),
      join_strategy: z.string().describe('How to handle joins: "relationships" (lazy), "joins" (eager physical), "auto", or "" for default'),
    },
    async ({ files, connection_id, explore_name, join_strategy }) => {
      try {
        const result = convertLookMLToSigma(files, {
          connectionId: connection_id || undefined,
          exploreName: explore_name || undefined,
          joinStrategy: (join_strategy || undefined) as any,
        });
        return {
          content: [{
            type: 'text' as const,
            text: JSON.stringify({ sigmaDataModel: result.model, stats: result.stats, warnings: result.warnings }, null, 2),
          }],
        };
      } catch (e: any) {
        return { content: [{ type: 'text' as const, text: `Error: ${e.message}` }], isError: true };
      }
    }
  );

  // ── convert_powerbi_to_sigma ───────────────────────────────────────────

  server.tool(
    'convert_powerbi_to_sigma',
    `Convert a Power BI model (TOM JSON / .bim) to Sigma Computing data model JSON.

Accepts the JSON content of a Power BI model — either a .bim file, a
DataModelSchema from a .pbit, or TOM JSON from SSAS/Power BI Service.

Handles tables, columns, DAX measures → Sigma metrics, DAX calculated
columns, relationships, display folders, measures-only tables, and
M expression path extraction for warehouse table sources.

Complex DAX patterns (CALCULATE+ALL, iterators, time intelligence, VAR/RETURN)
generate warnings with links to equivalent Sigma patterns.`,
    {
      model_json: z.string().describe('Power BI model JSON content (.bim file, DataModelSchema, or TOM JSON)'),
      connection_id: z.string().describe('Sigma connection UUID (from GET /v2/connections); pass empty string to omit'),
      database: z.string().describe('Override database name; pass empty string to omit'),
      schema: z.string().describe('Override schema name; pass empty string to omit'),
    },
    async ({ model_json, connection_id, database, schema }) => {
      try {
        const parsed = JSON.parse(model_json);
        const result = convertPowerBIToSigma(parsed, {
          connectionId: connection_id || undefined,
          database: database || undefined,
          schema: schema || undefined,
        });
        return {
          content: [{
            type: 'text' as const,
            text: JSON.stringify({ sigmaDataModel: result.model, stats: result.stats, warnings: result.warnings }, null, 2),
          }],
        };
      } catch (e: any) {
        return { content: [{ type: 'text' as const, text: `Error: ${e.message}` }], isError: true };
      }
    }
  );

  // ── convert_tableau_to_sigma ──────────────────────────────────────────

  server.tool(
    'convert_tableau_to_sigma',
    `Convert a Tableau workbook or data source to Sigma Computing data model JSON.

Accepts raw XML content from .twb (workbook), .tds (data source), or the
extracted XML from .twbx/.tdsx packaged files.

Parses data sources, joins/relationships, calculated fields with formula
conversion, LOD FIXED expressions → child elements with groupings,
parameters → controls, and cross-element column reference auto-fixing.

Supports federated/Excel-backed sources: when the Tableau source is not a
direct warehouse connection, pass database and schema to set the warehouse
path. Table names are uppercased automatically (Orders → ORDERS). Use
table_mapping to override specific names when they differ in the warehouse.

Complex patterns (LOD INCLUDE/EXCLUDE, table calculations, RUNNING_SUM, RANK)
generate warnings with community article links.`,
    {
      xml_content: z.string().describe('Tableau XML content (.twb or .tds file content)'),
      connection_id: z.string().describe('Sigma connection UUID; pass empty string to omit'),
      database: z.string().describe('Override database name; pass empty string to omit'),
      schema: z.string().describe('Override schema name; pass empty string to omit'),
      datasource_index: z.number().describe('Which data source to convert, 0-indexed (pass 0 for default)'),
      table_mapping: z.string().optional().describe('Optional JSON map of Tableau table names to warehouse table names, e.g. {"Orders":"ORDERS","People":"PEOPLE"}. Required when the Tableau source is Excel/flat-file and warehouse table names differ from the Tableau sheet names.'),
    },
    async ({ xml_content, connection_id, database, schema, datasource_index, table_mapping }) => {
      try {
        let tableMapping: Record<string, string> | undefined;
        if (table_mapping) {
          try { tableMapping = JSON.parse(table_mapping); }
          catch { throw new Error('table_mapping must be valid JSON, e.g. {"Orders":"ORDERS"}'); }
        }
        const result = convertTableauToSigma(xml_content, {
          connectionId: connection_id || undefined,
          database: database || undefined,
          schema: schema || undefined,
          datasourceIndex: datasource_index,
          tableMapping,
        });
        return {
          content: [{
            type: 'text' as const,
            text: JSON.stringify({ sigmaDataModel: result.model, stats: result.stats, warnings: result.warnings }, null, 2),
          }],
        };
      } catch (e: any) {
        return { content: [{ type: 'text' as const, text: `Error: ${e.message}` }], isError: true };
      }
    }
  );

  // ── convert_omni_to_sigma ────────────────────────────────────────────────

  server.tool(
    'convert_omni_to_sigma',
    `Convert Omni Analytics model files to Sigma Computing data model JSON.

Accepts .view.yaml files (dimensions + measures) and .model.yaml files
(explores + joins). Pass multiple files together — views and explores are
merged and resolved automatically.

Converts:
  - Views → Sigma elements with warehouse table paths
  - Dimensions → Sigma columns; \${TABLE}.col and \${field} refs translated
  - type: time dimensions → expanded per timeframe using DateTrunc()
  - Measures → Sigma metrics (sum, avg, min, max, count_distinct, count)
  - Explores/joins (sql_on + foreign_key) → Sigma relationships with FK/PK keys
  - CASE WHEN, IN (...), SQL functions → Sigma formula syntax

The output JSON can be POSTed to the Sigma API (POST /v2/dataModels/spec) to
create a data model, or PUT to /v2/dataModels/{id}/spec to update one.`,
    {
      files: z.array(z.object({
        name:    z.string().describe('Filename (e.g. "orders.view.yaml" or "retail_analytics.model.yaml")'),
        content: z.string().describe('Full file content'),
      })).describe('Array of Omni YAML files (.view.yaml and/or .model.yaml)'),
      connection_id: z.string().describe('Sigma connection UUID (from GET /v2/connections); pass empty string to omit'),
      database: z.string().describe('Override database name (e.g. "ANALYTICS"); pass empty string to omit'),
      schema:   z.string().describe('Override schema name (e.g. "PUBLIC"); pass empty string to omit'),
    },
    async ({ files, connection_id, database, schema }) => {
      try {
        const result = convertOmniToSigma(files, {
          connectionId: connection_id || undefined,
          database: database || undefined,
          schema: schema || undefined,
        });
        return {
          content: [{
            type: 'text' as const,
            text: JSON.stringify({ sigmaDataModel: result.model, stats: result.stats, warnings: result.warnings }, null, 2),
          }],
        };
      } catch (e: any) {
        return { content: [{ type: 'text' as const, text: `Error: ${e.message}` }], isError: true };
      }
    }
  );

  // ── convert_sql_to_sigma ─────────────────────────────────────────────────

  server.tool(
    'convert_sql_to_sigma',
    `Convert SQL SELECT statements to Sigma Computing data model JSON.

Parses SQL SELECT statements (including explicit JOINs and aggregate functions)
and generates a Sigma data model with warehouse elements, relationships, and
a derived view element that surfaces all SELECT columns.

Supports:
  - Explicit JOIN ON / JOIN USING → Sigma relationships with FK/PK column IDs
  - SUM/COUNT/AVG/MIN/MAX → Sigma metrics
  - CTEs (uses the last top-level SELECT, ignores CTE bodies)
  - Dot-qualified columns (t.col) → attributed to the correct warehouse element
  - DISTINCT / ALL SELECT modifiers
  - Multi-table models (pass multiple statements)

Complex queries (subqueries in FROM, implicit cross-joins) fall back to a
Custom SQL element with inferred column names.

The output JSON can be POSTed to the Sigma API (POST /v2/dataModels/spec) to
create a data model, or PUT to /v2/dataModels/{id}/spec to update one.`,
    {
      statements: z.array(z.object({
        name:    z.string().describe('Human-readable name for this query (used as derived element name)'),
        sql:     z.string().describe('The SQL SELECT statement to convert'),
      })).describe('Array of named SQL SELECT statements to convert'),
      connection_id: z.string().describe('Sigma connection UUID (from GET /v2/connections); pass empty string to omit'),
      database: z.string().describe('Override database name (e.g. "ANALYTICS"); pass empty string to omit'),
      schema:   z.string().describe('Override schema name (e.g. "PUBLIC"); pass empty string to omit'),
    },
    async ({ statements, connection_id, database, schema }) => {
      try {
        const result = convertSqlToSigma(statements, {
          connectionId: connection_id || undefined,
          database: database || undefined,
          schema: schema || undefined,
        });
        return {
          content: [{
            type: 'text' as const,
            text: JSON.stringify({ sigmaDataModel: result.model, stats: result.stats, warnings: result.warnings }, null, 2),
          }],
        };
      } catch (e: any) {
        return { content: [{ type: 'text' as const, text: `Error: ${e.message}` }], isError: true };
      }
    }
  );

  // ── convert_sql_to_sigma_formula ─────────────────────────────────────────

  server.tool(
    'convert_sql_to_sigma_formula',
    `Convert a SQL expression to a Sigma Computing formula.

Handles: CASE WHEN, DATEDIFF, DATEADD, ROUND, NULLIF, COALESCE, arithmetic,
column refs (SNAKE_CASE → [Title Case]), IN lists, and more.`,
    {
      sql: z.string().describe('The SQL expression to convert'),
    },
    async ({ sql }) => {
      try {
        const result = lookSqlToSigmaRules(sql);
        if (result) {
          return { content: [{ type: 'text' as const, text: JSON.stringify({ sigmaFormula: result, converted: true }, null, 2) }] };
        }
        const fallback = lookConvertExpression(sql);
        return {
          content: [{
            type: 'text' as const,
            text: JSON.stringify({ sigmaFormula: fallback, converted: true, note: 'Used general expression converter — review for accuracy' }, null, 2),
          }],
        };
      } catch (e: any) {
        return { content: [{ type: 'text' as const, text: `Error: ${e.message}` }], isError: true };
      }
    }
  );

  // ── convert_tableau_formula_to_sigma ──────────────────────────────────────

  server.tool(
    'convert_tableau_formula_to_sigma',
    `Convert a Tableau calculated field formula to a Sigma Computing formula.

Handles: IF/ELSEIF/ELSE/END, CASE/WHEN, IIF, ZN, COUNTD, DATEPART,
DATETRUNC, DATEADD, DATEDIFF. LOD expressions → comment placeholder.`,
    {
      formula: z.string().describe('The Tableau formula to convert'),
    },
    async ({ formula }) => {
      try {
        const warnings: string[] = [];
        const result = tableauFormulaToSigma(formula, warnings);
        return {
          content: [{
            type: 'text' as const,
            text: JSON.stringify({ sigmaFormula: result, warnings: warnings.length > 0 ? warnings : undefined }, null, 2),
          }],
        };
      } catch (e: any) {
        return { content: [{ type: 'text' as const, text: `Error: ${e.message}` }], isError: true };
      }
    }
  );

  // ── parse_lookml ─────────────────────────────────────────────────────────

  server.tool(
    'parse_lookml',
    `Parse a LookML file and return its structured representation as JSON.
Useful for inspecting views, explores, dimensions, measures, joins before conversion.`,
    {
      content: z.string().describe('LookML file content to parse'),
    },
    async ({ content }) => {
      try {
        const result = parseLookML(content);
        return {
          content: [{
            type: 'text' as const,
            text: JSON.stringify({
              views: result.views.map((v: any) => v._name),
              explores: result.explores.map((e: any) => e._name),
              connection: result.connection,
              parsed: result,
            }, null, 2),
          }],
        };
      } catch (e: any) {
        return { content: [{ type: 'text' as const, text: `Parse error: ${e.message}` }], isError: true };
      }
    }
  );

  // ── get_sigma_data_model_schema ──────────────────────────────────────────

  server.tool(
    'get_sigma_data_model_schema',
    `Return the Sigma Computing data model JSON schema reference.
Shows element types, columns, metrics, relationships, controls, and more.

For detailed specs on specific components, read the MCP resources:
  sigma://data-model-spec/data-model  — top-level structure, ID rules
  sigma://data-model-spec/column      — column formulas, prefix rules per source type
  sigma://data-model-spec/metric      — metric formulas (use source column names, not spec IDs)
  sigma://data-model-spec/source      — all 7 source kinds overview
  sigma://data-model-spec/join-source — join source with name field requirements
  sigma://data-model-spec/table       — full table element field reference
  sigma://data-model-spec/filter, /folder, /grouping, /control, /relationship — supporting types`,
    {},
    async () => ({
      content: [{
        type: 'text' as const,
        text: DATA_MODEL_SCHEMA_SUMMARY + `

## Key Rules

1. Warehouse columns use inode-style IDs: "inode-{22char}/{COLUMN}"
2. Calculated columns use short random IDs and MUST NOT include table prefix in formula
3. Metrics reference SOURCE column names (warehouse names) by display name — NO table prefix, NO spec IDs
4. Relationships go on the SOURCE (fact/many-side) element pointing to TARGET (dim/one-side)
5. Element IDs must be unique across ALL pages in the model (not just within one page)
6. Element order matters: dimension elements BEFORE fact elements (that reference them)
7. Duplicate source.path values cause "cycle in dependency order" errors — deduplicate
8. Join source REQUIRES "name" fields on both the join source and each relationship to resolve column formulas
9. Sigma API endpoints:
   - POST /v2/dataModels/spec — create new
   - PUT /v2/dataModels/{id}/spec — update existing
   - GET /v2/dataModels/{id}/spec — get current spec
   - GET /v2/connections — list connections (for connectionId)

## Formula prefix by source type
  warehouse-table: [LastPathSegment/ColName]     — last segment of path[], e.g. ORDERS
  sql:             [ElementNameField/ColName]    — the table element's "name" field
  table/data-model:[ReferencedElem.name/ColName]
  join:            [JoinSource.name/ColName]     — source.name for head, joins[N].name for each right
  union/transpose: [ColName]                     — no prefix`,
      }],
    })
  );

  // ── format_sigma_display_name ────────────────────────────────────────────

  server.tool(
    'format_sigma_display_name',
    'Convert a SNAKE_CASE identifier to Sigma display name format (Title Case).',
    {
      identifier: z.string().describe('SNAKE_CASE identifier (e.g. "ORDER_NUMBER")'),
    },
    async ({ identifier }) => ({
      content: [{ type: 'text' as const, text: sigmaDisplayName(identifier) }],
    })
  );

  // ── diagnose_sigma_save_error ─────────────────────────────────────────────

  server.tool(
    'diagnose_sigma_save_error',
    `Identify which SQL element in a Sigma data model caused a save error.

When Sigma returns a syntax error like "syntax error line 49 at position 24
unexpected '('" it only provides a connection ID, not which element failed.
This tool parses the error, scans all Custom SQL elements in the model JSON,
and pinpoints the likely culprit by matching line number and error token position.

Returns:
- A list of candidate elements (those whose SQL has >= errorLine lines)
- The exact line content at the error line for each candidate
- A "likely culprit" flag on the element where the error token appears at errorPosition`,
    {
      error_message: z.string().describe('The full Sigma error message, e.g. "syntax error line 49 at position 24 unexpected \'(\'"'),
      model_json: z.string().describe('The full Sigma data model JSON string (the output from a converter or GET /v2/dataModels/{id}/spec)'),
    },
    async ({ error_message, model_json }) => {
      try {
        // Parse error message
        const lineMatch = error_message.match(/syntax error line (\d+) at position (\d+)/i);
        if (!lineMatch) {
          return {
            content: [{
              type: 'text' as const,
              text: JSON.stringify({
                error: 'Could not parse error message. Expected format: "syntax error line N at position M unexpected \'X\'"',
                raw: error_message,
              }, null, 2),
            }],
          };
        }

        const targetLine  = parseInt(lineMatch[1], 10);
        const targetPos   = parseInt(lineMatch[2], 10);
        const tokenMatch  = error_message.match(/unexpected '([^']+)'/i);
        const errorToken  = tokenMatch ? tokenMatch[1] : '';

        // Parse model JSON
        let model: any;
        try {
          model = JSON.parse(model_json);
        } catch (e: any) {
          return { content: [{ type: 'text' as const, text: `Error: Could not parse model_json — ${e.message}` }], isError: true };
        }

        // Collect all SQL elements
        const sqlElements: any[] = [];
        for (const page of (model.pages || [])) {
          for (const el of (page.elements || [])) {
            if (el.source && el.source.kind === 'sql' && el.source.statement) {
              sqlElements.push({ name: el.name || el.id || '(unnamed)', id: el.id, sql: el.source.statement });
            }
          }
        }

        if (sqlElements.length === 0) {
          return {
            content: [{
              type: 'text' as const,
              text: JSON.stringify({ message: 'No Custom SQL elements found in the model.', targetLine, targetPos, errorToken }, null, 2),
            }],
          };
        }

        // Scan candidates
        const candidates: any[] = [];
        for (const el of sqlElements) {
          const lines = el.sql.split('\n');
          if (lines.length < targetLine) continue;

          const lineContent = lines[targetLine - 1];   // 1-indexed → 0-indexed
          const tokenAtPos  = errorToken && lineContent.slice(targetPos, targetPos + errorToken.length) === errorToken;
          const likelyCulprit = tokenAtPos;

          candidates.push({
            name:         el.name,
            id:           el.id,
            totalLines:   lines.length,
            errorLineContent: lineContent,
            tokenAtPosition:  tokenAtPos,
            likelyCulprit,
          });
        }

        if (candidates.length === 0) {
          return {
            content: [{
              type: 'text' as const,
              text: JSON.stringify({
                message: `No SQL element has ${targetLine} or more lines. The error may be in a data source not included in this model.`,
                targetLine,
                targetPos,
                errorToken,
                totalSqlElements: sqlElements.length,
              }, null, 2),
            }],
          };
        }

        const culprits = candidates.filter(c => c.likelyCulprit);
        return {
          content: [{
            type: 'text' as const,
            text: JSON.stringify({
              targetLine,
              targetPos,
              errorToken: errorToken || '(not parsed)',
              candidates,
              summary: culprits.length > 0
                ? `Likely culprit: "${culprits[0].name}" — error token "${errorToken}" found at position ${targetPos} on line ${targetLine}.`
                : `${candidates.length} element(s) have enough lines, but token "${errorToken}" was not found at position ${targetPos} in any of them. Check the candidates list and inspect lines manually.`,
            }, null, 2),
          }],
        };

      } catch (e: any) {
        return { content: [{ type: 'text' as const, text: `Error: ${e.message}` }], isError: true };
      }
    }
  );

  // ── convert_thoughtspot_to_sigma ─────────────────────────────────────────

  server.tool(
    'convert_thoughtspot_to_sigma',
    `Convert a ThoughtSpot TML (YAML) worksheet or model to Sigma Computing data model JSON.

Accepts ThoughtSpot Table/Worksheet TML YAML — the format exported from
ThoughtSpot's Develop > TML interface. Handles table_paths (physical table
aliases), worksheet_columns with "ALIAS::column" separator, formula columns,
and joins with SQL ON clause parsing.

The output JSON can be POSTed to the Sigma API (POST /v2/dataModels/spec) to
create a data model, or PUT to /v2/dataModels/{id}/spec to update one.`,
    {
      tml_yaml: z.string().describe('ThoughtSpot TML YAML content (worksheet or table format)'),
      connection_id: z.string().describe('Sigma connection UUID (from GET /v2/connections); pass empty string to omit'),
      database: z.string().describe('Override database name; pass empty string to omit'),
      schema: z.string().describe('Override schema name; pass empty string to omit'),
    },
    async ({ tml_yaml, connection_id, database, schema }) => {
      try {
        const result = convertThoughtSpotToSigma(tml_yaml, {
          connectionId: connection_id || undefined,
          database: database || undefined,
          schema: schema || undefined,
        });
        return {
          content: [{
            type: 'text' as const,
            text: JSON.stringify({ sigmaDataModel: result.model, stats: result.stats, warnings: result.warnings }, null, 2),
          }],
        };
      } catch (e: any) {
        return { content: [{ type: 'text' as const, text: `Error: ${e.message}` }], isError: true };
      }
    }
  );

  // ── convert_qlik_to_sigma ─────────────────────────────────────────────────

  server.tool(
    'convert_qlik_to_sigma',
    `Convert a Qlik Sense app metadata JSON to Sigma Computing data model JSON.

Accepts Qlik Engine API "qtr" format or REST API "tables" format. Handles
tables with fields → warehouse elements, shared field names → relationships,
master measures → metrics, master dimensions → calculated columns.

Qlik Set Analysis expressions are flagged as warnings and omitted since
they have no direct Sigma equivalent.

The output JSON can be POSTed to the Sigma API (POST /v2/dataModels/spec) to
create a data model, or PUT to /v2/dataModels/{id}/spec to update one.`,
    {
      model_json: z.string().describe('Qlik app metadata JSON (qtr format or REST tables array)'),
      connection_id: z.string().describe('Sigma connection UUID; pass empty string to omit'),
      database: z.string().describe('Override database name; pass empty string to omit'),
      schema: z.string().describe('Override schema name; pass empty string to omit'),
    },
    async ({ model_json, connection_id, database, schema }) => {
      try {
        const parsed = JSON.parse(model_json);
        const result = convertQlikToSigma(parsed, {
          connectionId: connection_id || undefined,
          database: database || undefined,
          schema: schema || undefined,
        });
        return {
          content: [{
            type: 'text' as const,
            text: JSON.stringify({ sigmaDataModel: result.model, stats: result.stats, warnings: result.warnings }, null, 2),
          }],
        };
      } catch (e: any) {
        return { content: [{ type: 'text' as const, text: `Error: ${e.message}` }], isError: true };
      }
    }
  );

  // ── convert_atlan_to_sigma ────────────────────────────────────────────────

  server.tool(
    'convert_atlan_to_sigma',
    `Convert an Atlan Data Contract (YAML or JSON) to Sigma Computing data model JSON.

Accepts an Atlan data contract document. Reads the "models" object to create
one Sigma element per model, auto-generates Sum() metrics for numeric columns,
and builds relationships from "field.references" in "model.column" format.

The output JSON can be POSTed to the Sigma API (POST /v2/dataModels/spec) to
create a data model, or PUT to /v2/dataModels/{id}/spec to update one.`,
    {
      contract_text: z.string().describe('Atlan data contract content (YAML or JSON string)'),
      connection_id: z.string().describe('Sigma connection UUID; pass empty string to omit'),
      database: z.string().describe('Override database name; pass empty string to omit'),
      schema: z.string().describe('Override schema name; pass empty string to omit'),
    },
    async ({ contract_text, connection_id, database, schema }) => {
      try {
        const result = convertAtlanToSigma(contract_text, {
          connectionId: connection_id || undefined,
          database: database || undefined,
          schema: schema || undefined,
        });
        return {
          content: [{
            type: 'text' as const,
            text: JSON.stringify({ sigmaDataModel: result.model, stats: result.stats, warnings: result.warnings }, null, 2),
          }],
        };
      } catch (e: any) {
        return { content: [{ type: 'text' as const, text: `Error: ${e.message}` }], isError: true };
      }
    }
  );

  // ── convert_alteryx_to_sigma ──────────────────────────────────────────────

  server.tool(
    'convert_alteryx_to_sigma',
    `Convert an Alteryx Designer workflow (.yxmd XML) to Sigma Computing data model JSON.

Parses the Alteryx workflow XML. DbFileInput tools → warehouse elements,
Join tools → relationships (traced back to source inputs via connection graph),
Formula tools → calculated columns, Summarize tools → metrics.

Cross-element column references are dropped with warnings since Sigma metrics
must reference columns within the same element.

The output JSON can be POSTed to the Sigma API (POST /v2/dataModels/spec) to
create a data model, or PUT to /v2/dataModels/{id}/spec to update one.`,
    {
      xml_content: z.string().describe('Alteryx workflow XML content (.yxmd file content)'),
      connection_id: z.string().describe('Sigma connection UUID; pass empty string to omit'),
      database: z.string().describe('Override database name; pass empty string to omit'),
      schema: z.string().describe('Override schema name; pass empty string to omit'),
    },
    async ({ xml_content, connection_id, database, schema }) => {
      try {
        const result = convertAlteryxToSigma(xml_content, {
          connectionId: connection_id || undefined,
          database: database || undefined,
          schema: schema || undefined,
        });
        return {
          content: [{
            type: 'text' as const,
            text: JSON.stringify({ sigmaDataModel: result.model, stats: result.stats, warnings: result.warnings }, null, 2),
          }],
        };
      } catch (e: any) {
        return { content: [{ type: 'text' as const, text: `Error: ${e.message}` }], isError: true };
      }
    }
  );

  // ── convert_oac_to_sigma ──────────────────────────────────────────────────

  server.tool(
    'convert_oac_to_sigma',
    `Convert Oracle Analytics Cloud (OAC) logical tables JSON to Sigma Computing data model JSON.

Accepts an array of OAC logical table objects (SMML export format). Each table
has logicalColumns and logicalTableSources. Handles physical vs derived column
mappings, aggregation rules → metrics, and logical joins → relationships.

Pass an optional physicalMap for DB/schema metadata when the OAC model does
not include full path information.

The output JSON can be POSTed to the Sigma API (POST /v2/dataModels/spec) to
create a data model, or PUT to /v2/dataModels/{id}/spec to update one.`,
    {
      tables_json: z.string().describe('JSON array of OAC logical table objects (SMML export)'),
      connection_id: z.string().describe('Sigma connection UUID; pass empty string to omit'),
      database: z.string().describe('Override database name; pass empty string to omit'),
      schema: z.string().describe('Override schema name; pass empty string to omit'),
      physical_map_json: z.string().describe('Optional JSON object mapping table name → {database, schema}; pass empty string to omit'),
    },
    async ({ tables_json, connection_id, database, schema, physical_map_json }) => {
      try {
        const tables = JSON.parse(tables_json);
        const physicalMap = physical_map_json ? JSON.parse(physical_map_json) : {};
        const result = convertOacToSigma(tables, {
          connectionId: connection_id || undefined,
          database: database || undefined,
          schema: schema || undefined,
          physicalMap,
        });
        return {
          content: [{
            type: 'text' as const,
            text: JSON.stringify({ sigmaDataModel: result.model, stats: result.stats, warnings: result.warnings }, null, 2),
          }],
        };
      } catch (e: any) {
        return { content: [{ type: 'text' as const, text: `Error: ${e.message}` }], isError: true };
      }
    }
  );
}

function registerPrompts(server: McpServer): void {
  server.prompt(
    'convert_dbt_model',
    'Guide for converting a dbt semantic model to Sigma',
    () => ({
      messages: [{
        role: 'user' as const,
        content: {
          type: 'text' as const,
          text: `I need to convert a dbt semantic model to a Sigma data model.

Steps:
1. Read the dbt YAML file (semantic_manifest.json or _semantic_models.yml)
2. Use the convert_dbt_to_sigma tool with the YAML content
3. If I have a Sigma connection ID, include it. Otherwise, get one from GET /v2/connections
4. Review warnings and fix any issues
5. Save to Sigma via POST /v2/dataModels/spec with folderId`,
        },
      }],
    })
  );

  server.prompt(
    'convert_snowflake_semantic_view',
    'Guide for converting a Snowflake Semantic View to Sigma',
    () => ({
      messages: [{
        role: 'user' as const,
        content: {
          type: 'text' as const,
          text: `I need to convert a Snowflake Cortex Analyst semantic view to a Sigma data model.

Steps:
1. Read the Snowflake semantic view YAML
2. Use the convert_snowflake_to_sigma tool
3. Facts auto-generate Sum() metrics (disable with auto_metrics: false)
4. Review warnings, then save to Sigma via the API`,
        },
      }],
    })
  );

  server.prompt(
    'convert_lookml_project',
    'Guide for converting a LookML project to Sigma',
    () => ({
      messages: [{
        role: 'user' as const,
        content: {
          type: 'text' as const,
          text: `I need to convert a LookML project (Looker) to a Sigma data model.

Steps:
1. Read all .model.lkml and .view.lkml files
2. Pass them to convert_lookml_to_sigma as [{name, content}, ...]
3. Specify which explore to convert (or auto-detect if only one)
4. Choose join strategy: "relationships" | "joins" | "auto"
5. Review warnings:
   - include: directives → listed with paths; upload all referenced view files to resolve joins
   - PDT property warnings (distribution, sortkeys, datagroup_trigger, etc.) → informational, no action needed
   - \${view.SQL_TABLE_NAME} refs → fully resolved including N-hop chains and PDT self-joins
6. Save to Sigma via the API`,
        },
      }],
    })
  );

  server.prompt(
    'convert_powerbi_model',
    'Guide for converting a Power BI model to Sigma',
    () => ({
      messages: [{
        role: 'user' as const,
        content: {
          type: 'text' as const,
          text: `I need to convert a Power BI model to a Sigma data model.

Steps:
1. Read the .bim file or DataModelSchema JSON from a .pbit
2. Pass the JSON content to convert_powerbi_to_sigma
3. Include a Sigma connection ID if available
4. Optionally override database/schema names
5. Review warnings — complex DAX patterns may need manual conversion
6. Linked columns referencing related dimensions may need to be re-added in the Sigma UI
7. Save to Sigma via POST /v2/dataModels/spec with folderId`,
        },
      }],
    })
  );

  server.prompt(
    'convert_tableau_workbook',
    'Guide for converting a Tableau workbook or data source to Sigma',
    () => ({
      messages: [{
        role: 'user' as const,
        content: {
          type: 'text' as const,
          text: `I need to convert a Tableau workbook or data source to a Sigma data model.

Steps:
1. Read the .twb or .tds XML file (or extract from .twbx/.tdsx ZIP)
2. Pass the XML content to convert_tableau_to_sigma
3. Provide a Sigma connection_id if available
4. Optionally override database/schema names
5. If the file has multiple data sources, specify datasource_index (0-indexed)
6. Review warnings:
   - LOD FIXED → auto-converted to child elements with groupings
   - LOD INCLUDE/EXCLUDE → manual conversion needed
   - Table calculations (RUNNING_SUM, RANK, WINDOW_*) → manual conversion
   - Linked columns may need re-adding in Sigma UI
7. Save to Sigma via POST /v2/dataModels/spec with folderId`,
        },
      }],
    })
  );

  server.prompt(
    'convert_omni_model',
    'Guide for converting an Omni Analytics model to Sigma',
    () => ({
      messages: [{
        role: 'user' as const,
        content: {
          type: 'text' as const,
          text: `I need to convert an Omni Analytics model to a Sigma data model.

Steps:
1. Read all .view.yaml files (one per view — dimensions & measures)
   and the .model.yaml file (explores & joins)
2. Pass them all to convert_omni_to_sigma as [{name, content}, ...]
   — views and explores are merged automatically
3. Provide a Sigma connection_id if available
4. If sql_table_name values are just table names (no db/schema), use the
   database and schema override parameters to complete the paths
5. Review warnings:
   - derived_table views need their source paths filled in manually
   - Complex SQL expressions that couldn't be auto-translated
   - Joins where FK/PK columns couldn't be resolved
6. Save to Sigma via POST /v2/dataModels/spec with folderId

How to get Omni files:
- Git sync: connect your Omni model to GitHub (Omni Settings → Git Sync)
- Omni IDE: copy YAML directly from the model editor`,
        },
      }],
    })
  );
}
