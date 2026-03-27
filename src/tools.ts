/**
 * Shared tool and prompt registration for the Sigma Data Model MCP Server.
 * Used by both the stdio (local) and HTTP (remote) entry points.
 */

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';

import { convertDbtToSigma } from './dbt.js';
import { convertSnowflakeSemanticView } from './snowflake.js';
import { convertLookMLToSigma, parseLookML } from './lookml.js';
import { lookSqlToSigmaRules, tableauFormulaToSigma, lookConvertExpression } from './formulas.js';
import { DATA_MODEL_SCHEMA_SUMMARY, sigmaDisplayName } from './sigma-ids.js';

/** Create and configure a new MCP server with all tools and prompts registered. */
export function createSigmaServer(): McpServer {
  const server = new McpServer({
    name: 'sigma-data-model',
    version: '1.0.0',
  });

  registerTools(server);
  registerPrompts(server);

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
      connection_id: z.string().optional().describe('Sigma connection UUID (from GET /v2/connections)'),
      database: z.string().optional().describe('Override database name (e.g. ANALYTICS)'),
      schema: z.string().optional().describe('Override schema name (e.g. DBT_PROD)'),
    },
    async ({ yaml_content, connection_id, database, schema }) => {
      try {
        const result = convertDbtToSigma(yaml_content, {
          connectionId: connection_id,
          database,
          schema,
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
      connection_id: z.string().optional().describe('Sigma connection UUID'),
      auto_metrics: z.boolean().optional().describe('Auto-generate Sum() metrics for fact columns (default: true)'),
    },
    async ({ yaml_content, connection_id, auto_metrics }) => {
      try {
        const result = convertSnowflakeSemanticView(yaml_content, {
          connectionId: connection_id,
          autoMetrics: auto_metrics ?? true,
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

Pass files as an array of {name, content} objects.`,
    {
      files: z.array(z.object({
        name: z.string().describe('Filename (e.g. "sales.model.lkml" or "orders.view.lkml")'),
        content: z.string().describe('Full file content'),
      })).describe('Array of LookML files to parse'),
      connection_id: z.string().optional().describe('Sigma connection UUID'),
      explore_name: z.string().optional().describe('Name of the explore to convert (auto-detected if only one)'),
      join_strategy: z.enum(['relationships', 'joins', 'auto']).optional()
        .describe('How to handle joins: "relationships" (lazy), "joins" (eager physical), "auto"'),
    },
    async ({ files, connection_id, explore_name, join_strategy }) => {
      try {
        const result = convertLookMLToSigma(files, {
          connectionId: connection_id,
          exploreName: explore_name,
          joinStrategy: join_strategy as any,
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
Shows element types, columns, metrics, relationships, controls, and more.`,
    {},
    async () => ({
      content: [{
        type: 'text' as const,
        text: DATA_MODEL_SCHEMA_SUMMARY + `

## Key Rules

1. Warehouse columns use inode-style IDs: "inode-{22char}/{COLUMN}"
2. Calculated columns use short random IDs and MUST NOT include table prefix in formula
3. Metrics reference columns by display name only — NO table prefix
4. Relationships go on the SOURCE (fact/many-side) element pointing to TARGET (dim/one-side)
5. Element order matters: dimension elements BEFORE fact elements (that reference them)
6. Duplicate source.path values cause "cycle in dependency order" errors — deduplicate
7. Sigma API endpoints:
   - POST /v2/dataModels/spec — create new
   - PUT /v2/dataModels/{id}/spec — update existing
   - GET /v2/dataModels/{id}/spec — get current spec
   - GET /v2/connections — list connections (for connectionId)`,
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
5. Review warnings, then save to Sigma via the API`,
        },
      }],
    })
  );
}
