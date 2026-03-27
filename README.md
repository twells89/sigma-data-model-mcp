# Sigma Data Model MCP Server

An MCP server that converts data models from **dbt**, **Snowflake Semantic Views**, **LookML** (Looker), and **Tableau** into [Sigma Computing](https://sigmacomputing.com) data model JSON format.

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
| `convert_sql_to_sigma_formula` | SQL expression → Sigma calculated column formula |
| `convert_tableau_formula_to_sigma` | Tableau calculated field → Sigma formula |
| `parse_lookml` | Parse LookML and return structured AST |
| `get_sigma_data_model_schema` | Return the Sigma data model JSON schema reference |
| `format_sigma_display_name` | Convert SNAKE_CASE → Sigma Title Case |

## How It Works

1. **You provide** a dbt YAML, Snowflake semantic view YAML, or LookML files
2. **The server converts** entities → columns, measures → metrics, joins → relationships
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

**Parameters:**
- `files` (required) — Array of `{name, content}` objects (view + model files)
- `connection_id` — Sigma connection UUID
- `explore_name` — Which explore to convert (auto-detected if only one)
- `join_strategy` — `"relationships"` | `"joins"` | `"auto"`

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

---

## Self-Hosting

### Deploy to Render (recommended)

1. Push this repo to GitHub
2. Go to [Render Dashboard](https://dashboard.render.com) → New → Web Service
3. Connect your repo
4. Render auto-detects `render.yaml` — just click Deploy

Or use the one-click deploy button if configured.

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

Converter logic from the [Sigma Data Model Manager](https://github.com/twells89/sigma-data-models) by TJ Wells.

## License

MIT
