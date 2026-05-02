# Sigma Data Model MCP Server

An MCP server that converts data models from **dbt**, **Snowflake Semantic Views**, **LookML** (Looker), **Tableau**, **Power BI**, **Omni**, **SQL**, **ThoughtSpot**, **Qlik Sense**, **Atlan**, **Alteryx**, and **Oracle Analytics Cloud** into [Sigma Computing](https://sigmacomputing.com) data model JSON format.

Works with **Claude Code**, **Claude Desktop**, **Claude.ai**, **Cursor**, and any MCP-compatible client.

## Quick Start — Connect via URL

If the server is already hosted, just point your client at it:

### Claude Code
```bash
claude mcp add sigma-data-model --transport http https://sigma-data-model-mcp.onrender.com/mcp
```

### Claude Desktop
Add to `claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "sigma-data-model": {
      "command": "npx",
      "args": ["mcp-remote", "https://sigma-data-model-mcp.onrender.com/mcp"]
    }
  }
}
```

### Claude.ai
Add as a connector in Settings → Connected MCP Servers using the URL:
```
https://sigma-data-model-mcp.onrender.com/mcp
```

That's it — no cloning, no building. Just connect and start converting.

---

## Tools

| Tool | Description |
|------|-------------|
| `convert_dbt_to_sigma` | dbt semantic model YAML → Sigma data model JSON |
| `convert_snowflake_to_sigma` | Snowflake Cortex Analyst semantic view YAML → Sigma JSON |
| `convert_lookml_to_sigma` | LookML project files (views + explores) → Sigma JSON |
| `convert_powerbi_to_sigma` | Power BI model (.bim / TOM JSON) → Sigma JSON |
| `convert_tableau_to_sigma` | Tableau workbook/data source (.twb/.tds XML) → Sigma JSON |
| `convert_omni_to_sigma` | Omni Analytics view + model YAML → Sigma JSON |
| `convert_sql_to_sigma` | SQL SELECT statements → Sigma JSON |
| `convert_thoughtspot_to_sigma` | ThoughtSpot TML (YAML) worksheet → Sigma JSON |
| `convert_qlik_to_sigma` | Qlik Sense app metadata JSON → Sigma JSON |
| `convert_atlan_to_sigma` | Atlan data contract (YAML or JSON) → Sigma JSON |
| `convert_alteryx_to_sigma` | Alteryx Designer workflow (.yxmd XML) → Sigma JSON |
| `convert_oac_to_sigma` | Oracle Analytics Cloud logical tables JSON → Sigma JSON |
| `convert_cube_to_sigma` | Cube.dev schemas (YAML or JS) → Sigma JSON |
| `convert_tableau_prep_to_sigma` | Tableau Prep flow JSON (.tfl/.tflx) → Sigma JSON |
| `convert_sql_to_sigma_formula` | SQL expression → Sigma calculated column formula |
| `convert_tableau_formula_to_sigma` | Tableau calculated field → Sigma formula |
| `parse_lookml` | Parse LookML and return structured AST |
| `get_sigma_data_model_schema` | Return the Sigma data model JSON schema reference |
| `diagnose_sigma_save_error` | Pinpoint which Custom SQL element caused a Sigma save error |
| `format_sigma_display_name` | Convert SNAKE_CASE → Sigma Title Case |

## How It Works

1. **You provide** source files — YAML, JSON, XML, or SQL depending on the converter
2. **The server converts** tables → elements, columns → Sigma columns, aggregates → metrics, joins → relationships
3. **You get back** Sigma data model JSON ready to POST to the Sigma API

The output JSON is compatible with:
- `POST /v2/dataModels/spec` — create a new data model
- `PUT /v2/dataModels/{dataModelId}/spec` — update an existing one

If you also have the [Sigma API MCP server](https://help.sigmacomputing.com/mcp) connected, your AI client can convert AND save in one flow.

## Tool Details

### convert_dbt_to_sigma
Converts dbt semantic model YAML into Sigma JSON. Handles `entities` (primary/unique/foreign), `dimensions`, `measures`, `metrics` (simple, ratio, derived), cross-model relationships, and `node_relation` / `ref()` resolution.

**Parameters:**
- `yaml_content` (required) — dbt YAML content
- `connection_id` — Sigma connection UUID
- `database` — Override database name
- `schema` — Override schema name

### convert_snowflake_to_sigma
Converts Snowflake Cortex Analyst semantic view YAML. Handles tables with dimensions, time_dimensions, facts (auto-generates Sum metrics), inline and top-level relationships, indicator column detection, and duplicate path deduplication.

**Parameters:**
- `yaml_content` (required) — Snowflake semantic view YAML
- `connection_id` — Sigma connection UUID
- `auto_metrics` — Auto-generate Sum() metrics for facts (default: true)

### convert_lookml_to_sigma
Converts a full LookML project into a Sigma data model. Includes a complete LookML parser. Handles explores, sql_on join keys, derived_table → Custom SQL, dimension/measure conversion, and role-playing dimension deduplication.

`${view.SQL_TABLE_NAME}` substitution is fully resolved including N-hop alias chains and PDT-referencing-PDT patterns (e.g. the same PDT referenced twice with different table aliases in a self-join).

`include:` directives are parsed and reported in warnings — resolution is limited to the files provided.

PDT materialization hints (`distribution`, `sortkeys`, `datagroup_trigger`, `persist_with`, `cluster_keys`, `partition_keys`) are not converted and emit informational warnings.

**Parameters:**
- `files` (required) — Array of `{name, content}` objects (view + model files)
- `connection_id` — Sigma connection UUID
- `explore_name` — Which explore to convert (auto-detected if only one)
- `join_strategy` — `"relationships"` | `"joins"` | `"auto"`

### convert_powerbi_to_sigma
Converts Power BI models (.bim / TOM JSON / DataModelSchema from .pbit) to Sigma JSON. Handles tables, DAX measures, calculated columns, relationships, display folders, measures-only tables, and M expression path extraction.

DAX conversion tiers:
- **Tier 1** — Direct mappings: SUM, AVERAGE, DIVIDE (nested-paren-aware), IF, SWITCH, date functions, text functions
- **Tier 2** — Simple CALCULATE with filter → SumIf/CountIf with correct argument order
- **Tier 3** — Complex patterns (CALCULATE+ALL, iterators, time intelligence, VAR/RETURN) → warnings with community links

**Parameters:**
- `model_json` (required) — Power BI model JSON content
- `connection_id` — Sigma connection UUID
- `database` — Override database name
- `schema` — Override schema name

### convert_tableau_to_sigma
Converts Tableau workbooks (.twb) and data sources (.tds) to Sigma JSON. Parses data sources, joins/relationships, calculated fields with formula conversion, LOD FIXED expressions → child elements with groupings, parameters → controls.

Handles: IF/ELSEIF/ELSE/END → nested If(), CASE/WHEN, ZN → Coalesce, COUNTD → CountDistinct, DATEPART/DATETRUNC/DATEADD/DATEDIFF, LOD FIXED → child elements with groupings. LOD INCLUDE/EXCLUDE and table calculations generate warnings.

**Parameters:**
- `xml_content` (required) — Tableau XML content (.twb or .tds)
- `connection_id` — Sigma connection UUID
- `database` — Override database name
- `schema` — Override schema name
- `datasource_index` — Which data source to convert (0-indexed, default: 0)

### convert_omni_to_sigma
Converts Omni Analytics `.view.yaml` and `.model.yaml` files. Handles views → elements, dimensions → columns, type:time dimensions → DateTrunc() expansions per timeframe, measures → metrics, explores/joins → relationships.

**Parameters:**
- `files` (required) — Array of `{name, content}` objects (.view.yaml and/or .model.yaml)
- `connection_id` — Sigma connection UUID
- `database` — Override database name
- `schema` — Override schema name

### convert_sql_to_sigma
Converts SQL SELECT statements into a Sigma data model. Parses explicit JOINs → relationships, aggregate functions → metrics, CTEs, and dot-qualified column references. Falls back to a Custom SQL element for complex queries (subqueries in FROM, implicit cross-joins).

**Parameters:**
- `statements` (required) — Array of `{name, sql}` objects
- `connection_id` — Sigma connection UUID
- `database` — Override database name
- `schema` — Override schema name

### convert_thoughtspot_to_sigma
Converts ThoughtSpot TML YAML (worksheet or table format, exported from Develop → TML). Handles `table_paths` physical table aliases, `worksheet_columns` with `ALIAS::column` separator, formula columns, and joins with SQL ON clause parsing.

**Parameters:**
- `tml_yaml` (required) — ThoughtSpot TML YAML content
- `connection_id` — Sigma connection UUID
- `database` — Override database name
- `schema` — Override schema name

### convert_qlik_to_sigma
Converts Qlik Sense app metadata JSON (Engine API `qtr` format or REST API `tables` format). Tables → warehouse elements, shared field names → relationships, master measures → metrics, master dimensions → calculated columns. Qlik Set Analysis expressions are flagged as warnings and omitted.

**Parameters:**
- `model_json` (required) — Qlik app metadata JSON
- `connection_id` — Sigma connection UUID
- `database` — Override database name
- `schema` — Override schema name

### convert_atlan_to_sigma
Converts an Atlan data contract (YAML or JSON). Reads the `models` object, creates one Sigma element per model, auto-generates Sum() metrics for numeric columns, and builds relationships from `field.references` in `"model.column"` format.

**Parameters:**
- `contract_text` (required) — Atlan data contract content (YAML or JSON string)
- `connection_id` — Sigma connection UUID
- `database` — Override database name
- `schema` — Override schema name

### convert_alteryx_to_sigma
Converts an Alteryx Designer workflow (.yxmd XML). DbFileInput tools → warehouse elements, Join tools → relationships (traced back through the connection graph), Formula tools → calculated columns, Summarize tools → metrics. Cross-element column references are dropped with warnings.

**Parameters:**
- `xml_content` (required) — Alteryx workflow XML (.yxmd file content)
- `connection_id` — Sigma connection UUID
- `database` — Override database name
- `schema` — Override schema name

### convert_oac_to_sigma
Converts Oracle Analytics Cloud logical tables JSON (SMML export format). Handles physical vs. derived (expression-based) column mappings, aggregation rules → metrics, and logical joins → relationships. Pass an optional `physical_map_json` when the OAC model doesn't include full database/schema path info.

**Parameters:**
- `tables_json` (required) — JSON array of OAC logical table objects
- `connection_id` — Sigma connection UUID
- `database` — Override database name
- `schema` — Override schema name
- `physical_map_json` — Optional JSON object mapping table name → `{database, schema}`

### convert_cube_to_sigma
Converts Cube.dev schemas to Sigma data model JSON. Accepts YAML (`.yml`, `.yaml`) and JavaScript (`.js`) schema files in the same call — the parser dispatches by extension. Supports `cube(`name`, { ... })` and `view(`name`, { ... })` calls, template literals (`${CUBE}.col`, `${OtherCube.dim}`, `${measure_name}`), and multi-line backtick SQL.

Mapping:
- Cube with `sql_table` → warehouse-table element
- Cube with `sql` → Custom SQL element (template refs normalized to plain SQL aliases)
- Dimensions (string/number/time/boolean) → columns
- Measures (count/sum/avg/min/max/count_distinct/number/percent) → metrics
- Filtered measures (`filters: [{ sql: ... }]`) → `SumIf` / `CountIf` / `AvgIf` wrappers
- Joins (one_to_one / one_to_many / many_to_one) → relationships with FK/PK keys parsed from join SQL
- Views (`cubes: [{ join_path, includes, prefix }]`) → derived elements with linked-column refs

Pre-aggregations and segments are skipped with informational warnings — Sigma uses warehouse-side caching and has no direct equivalent.

**Parameters:**
- `files` (required) — array of `{name, content}` objects (`.yml`, `.yaml`, `.js`, `.cjs`, `.mjs`)
- `connection_id` — Sigma connection UUID
- `database` — Override database name
- `schema` — Override schema name

### convert_tableau_prep_to_sigma
Converts Tableau Prep flows (.tfl/.tflx) to Sigma data model JSON. Pass the unzipped `flow` JSON content (a .tfl is a ZIP archive containing a file named `flow`).

Mapping:
- Inputs (LoadSql, LoadCsv, LoadExcel, LoadJson, LoadHyper, LoadGoogle) → warehouse-table elements
- LoadSqlProxy (Tableau Server published datasource) → Custom SQL placeholder, OR auto-resolved when a companion `.tds`/`.tdsx` is included in `tds_files` (matched by datasource caption — `type='table'` relations become warehouse-table elements, `type='text'` relations become Custom SQL with the real SELECT body)
- Containers (`.v1.Container`) → recursively flattened into the parent graph
- Linear transform chains (AddColumn / RemoveColumns / RenameColumn / Remap / FilterOperation / ChangeColumnType) → one Sigma element with calc cols + display name renames + filter passthroughs
- SuperJoin (`.v2018_2_3.SuperJoin` with `.v1.SimpleJoin` actionNode) → relationship with FK/PK keys + a derived "join view" element with cross-element refs of the form `[FACT_TABLE/REL_NAME/Field]`
- SuperUnion (`.v2018_2_3.SuperUnion`) → element with `source.kind: 'union'`
- SuperAggregate (`.v2018_2_3.SuperAggregate` with `.v1.Aggregate` actionNode) → child element with `groupings: [{ groupBy, calculations }]`
- Output nodes (WriteToHyper, PublishExtract, WriteToCsv) → ignored

Pivot, Script, RunCommand, and Prediction nodes are skipped with warnings. Inline `actions[]` on input nodes (e.g. Salesforce extract column drops) are processed before walking nextNodes.

**Parameters:**
- `flow_json` (required) — the unzipped `flow` JSON content from a .tfl/.tflx archive
- `connection_id` — Sigma connection UUID
- `database` — Override database name
- `schema` — Override schema name
- `table_mapping` — Optional JSON map of input names → warehouse table names, e.g. `{"Orders_Central":"ORDERS"}`
- `tds_files` — Optional array of `{name, content}` objects for companion `.tds`/`.tdsx` XML files. When a `.tds` caption matches a `LoadSqlProxy.datasourceName`, the placeholder is replaced with the real relation

### convert_sql_to_sigma_formula
Converts SQL expressions to Sigma formula syntax:
- `CASE WHEN x THEN y END` → `If(x, y, null)`
- `DATEDIFF('day', a, b)` → `DateDiff("day", [A], [B])`
- `ROUND(x / NULLIF(y, 0), 2)` → `Round([X] / If([Y] = 0, null, [Y]), 2)`
- Column refs: `SNAKE_CASE` → `[Title Case]`

### convert_tableau_formula_to_sigma
Converts Tableau calculated field formulas:
- `IF/ELSEIF/ELSE/END` → nested `If()`
- `ZN([x])` → `Coalesce([x], 0)`, `COUNTD` → `CountDistinct`
- LOD expressions → comment placeholder (cannot be auto-converted)

### diagnose_sigma_save_error
When Sigma returns a syntax error like `"syntax error line 49 at position 24 unexpected '('"` without naming which element failed, this tool scans all Custom SQL elements in the model JSON and pinpoints the likely culprit by matching line number and token position.

**Parameters:**
- `error_message` (required) — The full Sigma error message
- `model_json` (required) — The full Sigma data model JSON string

---

## Self-Hosting

### Deploy to Render (recommended)

1. Push this repo to GitHub
2. Go to [Render Dashboard](https://dashboard.render.com) → New → Web Service
3. Connect your repo
4. Render auto-detects `render.yaml` — just click Deploy

### Deploy anywhere else

Any platform that runs Node.js works (Railway, Koyeb, Fly.io, AWS, etc.):

```bash
npm install
npm run build
PORT=3000 npm start
```

The server exposes:
- `POST /mcp` — MCP Streamable HTTP endpoint
- `GET /` — Health check / info JSON

### Run locally (stdio mode for Claude Code)

```bash
npm install && npm run build
claude mcp add sigma-data-model -- node $(pwd)/build/index.js
```

---

## Sigma Data Model JSON Structure

```json
{
  "name": "My Data Model",
  "folderId": "workspace-or-folder-id",
  "pages": [{
    "id": "page1",
    "name": "Page 1",
    "elements": [{
      "id": "elem1",
      "kind": "table",
      "source": {
        "connectionId": "uuid",
        "kind": "warehouse-table",
        "path": ["DATABASE", "SCHEMA", "TABLE"]
      },
      "columns": [
        { "id": "inode-xxx/ORDER_ID", "formula": "[ORDER_FACT/Order Id]" }
      ],
      "metrics": [
        { "id": "metricId", "formula": "Sum([Revenue])", "name": "Total Revenue" }
      ],
      "relationships": [{
        "id": "relId",
        "targetElementId": "dimElem",
        "keys": [{ "sourceColumnId": "...", "targetColumnId": "..." }],
        "name": "Customer"
      }],
      "order": ["inode-xxx/ORDER_ID"]
    }]
  }]
}
```

## Credits

Converter logic from the [Sigma Data Model Manager](https://github.com/twells89/sigma-data-model-manager) by TJ Wells.

## License

MIT
