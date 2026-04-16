/**
 * MCP resource registrations for the Sigma Data Model spec documentation.
 * Each resource exposes a plain-text reference for a specific spec object type.
 */

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';

// ── Resource text ────────────────────────────────────────────────────────────

const DATA_MODEL_RESOURCE_TEXT = `Data Model Spec - Top Level

The "pages" parameter passed to createDataModelSpec / updateDataModelSpec is an ARRAY of Page objects directly. Do NOT wrap it in { "pages": [...] } — the tool schema already names this field.

Quick reference — minimal valid call:
{
  "name": "My Data Model",
  "folderId": "<folder-uuid>",
  "pages": [
    {
      "id": "page-1",
      "name": "Main",
      "elements": [
        {
          "id": "table-1",
          "kind": "table",
          "source": { "kind": "warehouse-table", "connectionId": "<conn-uuid>", "path": ["DB", "SCHEMA", "TABLE"] },
          "columns": [
            { "id": "col-1", "formula": "[TABLE/ColumnName]", "name": "Column Name" }
          ]
        }
      ]
    }
  ]
}

ID rules (global):
- All IDs are strings you choose. Not UUIDs, not auto-generated.
- Page IDs: unique within pages array.
- Element IDs (tables + controls): unique across ALL pages.
- Column/metric/relationship/filter/folder/grouping IDs: unique within their parent table.
- Folder IDs must not collide with column IDs in the same table.

Cross-references — every one of these MUST match an existing ID:
- source.elementId (kind:"table") -> a table element id
- source.dataModelId (kind:"data-model") -> an external data model id
- relationship.targetElementId -> a table element id
- relationship.keys[].sourceColumnId -> column id in owning table
- relationship.keys[].targetColumnId -> column id in target table
- filter.columnId -> column id in same table
- folder.items[] -> column or folder ids in same table
- grouping.groupBy[] / calculations[] -> column ids in same table
- table.order[] -> column or folder ids in same table
- table.summary[] -> column ids in same table
- control.filters[].source.elementId -> a table element id

Formula rules (critical):
- Column formulas depend on source type. See sigma://data-model-spec/column for full details.
- For warehouse-table sources: [TableName/ColumnName] where TableName = last segment of the path. Example: path ["DB","S","ORDERS"] → [ORDERS/OrderId].
- For sql sources: [TableName/ColumnName] where TableName = the "name" field of the table element.
- For table/data-model sources: [TableName/ColumnName] where TableName = the name field of the referenced table element.
- For join sources: [JoinInputName/ColumnName] — you MUST set "name" fields on the join source and relationships. See sigma://data-model-spec/join-source.
- Join column mapping formulas use simple [ColumnName] with no prefix (case-sensitive).
- Function names are case-insensitive (Sum, SUM, sum all work). String literals use "double" or 'single' quotes.

Sub-resources (read as needed for nested objects):
- sigma://data-model-spec/page — Page structure
- sigma://data-model-spec/element — Element union (table vs control)
- sigma://data-model-spec/table — Table element
- sigma://data-model-spec/source — Source types overview (warehouse-table, sql, table, data-model)
- sigma://data-model-spec/join-source — Join source (includes formula naming rules)
- sigma://data-model-spec/union-source — Union source
- sigma://data-model-spec/transpose-source — Transpose source
- sigma://data-model-spec/column — Column + formula syntax (includes prefix rules per source type)
- sigma://data-model-spec/control — Control element (14 types)
- sigma://data-model-spec/metric — Metrics
- sigma://data-model-spec/relationship — Relationships
- sigma://data-model-spec/filter — Column filters
- sigma://data-model-spec/folder — Folders
- sigma://data-model-spec/grouping — Groupings

Join-sourced table example (the most error-prone pattern):
{
  "name": "Orders Report",
  "folderId": "<folder-uuid>",
  "pages": [
    {
      "id": "page-1",
      "name": "Joined Data",
      "elements": [
        {
          "id": "joined-table",
          "kind": "table",
          "name": "Orders with Customers",
          "source": {
            "kind": "join",
            "name": "Orders",
            "joins": [
              {
                "left": { "kind": "warehouse-table", "connectionId": "<conn-uuid>", "path": ["DB", "S", "ORDERS"] },
                "right": { "kind": "warehouse-table", "connectionId": "<conn-uuid>", "path": ["DB", "S", "CUSTOMERS"] },
                "columns": [{ "left": "[CustomerId]", "right": "[Id]" }],
                "joinType": "left-outer",
                "name": "Customers"
              }
            ]
          },
          "columns": [
            { "id": "order-id", "formula": "[Orders/OrderId]", "name": "Order ID" },
            { "id": "amount", "formula": "[Orders/Amount]", "name": "Amount" },
            { "id": "cust-name", "formula": "[Customers/Name]", "name": "Customer Name" }
          ]
        }
      ]
    }
  ]
}

Note: source.name="Orders" → [Orders/...], joins[0].name="Customers" → [Customers/...].
Join mapping formulas use bare [CustomerId] and [Id] (no prefix, case-sensitive).

Tips:
- Use getDataModelSpec on an existing data model to see real output.
- Use listConnections to find valid connectionId values.
- Start with the simplest possible data model and iterate.
`;

const PAGE_RESOURCE_TEXT = `Data Model Spec - Page

{
  "id": string,         // required — unique within pages array
  "name": string,       // required — display name
  "elements": Element[] // required — array of table and/or control elements
}

Element IDs must be unique across ALL pages, not just within one page.

See sigma://data-model-spec/element for element construction.

Example:
{
  "id": "main-page",
  "name": "Sales Overview",
  "elements": [
    {
      "kind": "table",
      "id": "sales-table",
      "source": { "kind": "warehouse-table", "connectionId": "conn-uuid", "path": ["DB", "PUBLIC", "SALES"] },
      "columns": [
        { "id": "amount", "formula": "[SALES/Amount]", "name": "Amount" }
      ]
    }
  ]
}
`;

const ELEMENT_RESOURCE_TEXT = `Data Model Spec - Element

Union type discriminated by "kind". Two variants:

kind: "table" — dataset with source, columns, optional metrics/relationships/filters.
  See sigma://data-model-spec/table

kind: "control" — interactive input (dropdown, slider, date picker, etc.).
  See sigma://data-model-spec/control

Element IDs must be unique across the ENTIRE data model (all pages).
`;

const TABLE_RESOURCE_TEXT = `Data Model Spec - Table Element

Required fields:
{
  "id": string,             // unique across ALL elements in the data model
  "kind": "table",          // literal
  "columns": Column[],      // see sigma://data-model-spec/column
  "source": Source           // see sigma://data-model-spec/source
}

Optional fields:
  "name": string             — display title
  "description": string      — description text
  "metrics": Metric[]        — see sigma://data-model-spec/metric
  "relationships": Relationship[] — see sigma://data-model-spec/relationship
  "filters": ColumnFilter[]  — see sigma://data-model-spec/filter
  "folders": Folder[]        — see sigma://data-model-spec/folder
  "groupings": Grouping[]    — see sigma://data-model-spec/grouping
  "summary": string[]        — column IDs that appear in summary row
  "order": string[]          — column and folder IDs controlling display order
  "visibleAsSource": boolean — whether other docs can reference this table (default true)
  "sort": LevelSortKey[]     — sort config (see below)

sort entry format:
{ "columnId": string, "direction": "ascending" | "descending", "nulls"?: "first" | "last" | "connection-default" }

ID cross-references from this table:
- source.elementId -> another table element id (for kind:"table" sources)
- relationship.targetElementId -> another table element id
- relationship.keys[].sourceColumnId -> column id in THIS table
- relationship.keys[].targetColumnId -> column id in TARGET table
- filter.columnId -> column id in this table
- folder.items[] -> column or folder ids in this table
- grouping.groupBy[] / calculations[] -> column ids in this table
- order[] -> column or folder ids in this table
- summary[] -> column ids in this table
- sort[].columnId -> column id in this table

Example:
{
  "id": "orders-table",
  "kind": "table",
  "name": "Orders",
  "source": {
    "kind": "warehouse-table",
    "connectionId": "conn-uuid",
    "path": ["ANALYTICS", "PUBLIC", "ORDERS"]
  },
  "columns": [
    { "id": "order-id", "formula": "[ORDERS/OrderId]", "name": "Order ID" },
    { "id": "customer-id", "formula": "[ORDERS/CustomerId]", "name": "Customer ID" },
    { "id": "amount", "formula": "[ORDERS/Amount]", "name": "Amount" },
    { "id": "order-date", "formula": "[ORDERS/OrderDate]", "name": "Order Date" }
  ],
  "metrics": [
    { "id": "total-rev", "formula": "Sum([ORDERS/Amount])", "name": "Total Revenue" }
  ],
  "relationships": [
    {
      "id": "to-customers",
      "targetElementId": "customers-table",
      "keys": [{ "sourceColumnId": "customer-id", "targetColumnId": "cust-id" }]
    }
  ],
  "order": ["order-id", "customer-id", "amount", "order-date"],
  "sort": [{ "columnId": "order-date", "direction": "descending" }]
}
`;

const SOURCE_RESOURCE_TEXT = `Data Model Spec - Source

Union type discriminated by "kind". Pick the kind that matches your use case:

warehouse-table — direct connection to a database table (most common)
sql             — custom SQL query
table           — reference another table in this data model
data-model      — reference a table from another published data model
join            — combine multiple sources via JOINs → see sigma://data-model-spec/join-source
union           — UNION ALL of multiple sources → see sigma://data-model-spec/union-source
transpose       — PIVOT / UNPIVOT → see sigma://data-model-spec/transpose-source

---

kind: "warehouse-table"
{
  "kind": "warehouse-table",
  "connectionId": string,  // UUID of connection — use listConnections tool to find this
  "path": string[]         // table path, e.g. ["DATABASE", "SCHEMA", "TABLE_NAME"]
}

Column formulas use [TableName/ColumnName] where TableName is the last segment of the path and ColumnName matches the database schema column names.

Example:
{ "kind": "warehouse-table", "connectionId": "a1b2c3d4-...", "path": ["ANALYTICS_DB", "PUBLIC", "ORDERS"] }
→ column formulas: [ORDERS/OrderId], [ORDERS/Amount], [ORDERS/OrderDate]

---

kind: "sql"
{
  "kind": "sql",
  "connectionId": string,  // UUID of connection
  "statement": string      // SQL query
}

Column formulas use [TableName/ColumnName] where TableName is the "name" field of the table element and ColumnName matches the output column names of the SQL query.

Example:
{ "kind": "sql", "connectionId": "a1b2c3d4-...", "statement": "select * from orders where status = 'active'" }

---

kind: "table"
{
  "kind": "table",
  "elementId": string,    // MUST match the id of another table element in this data model
  "groupingId"?: string   // optional — reference a specific grouping level
}

Column formulas use [TableName/ColumnName] where TableName is the "name" field of the referenced table element (not the element id) and ColumnName matches the display name or id of columns in that table.

Example:
{ "kind": "table", "elementId": "orders-table" }

With grouping:
{ "kind": "table", "elementId": "sales-table", "groupingId": "by-region" }

---

kind: "data-model"
{
  "kind": "data-model",
  "dataModelId": string,  // UUID of the external data model
  "elementId": string,    // element ID within that data model
  "groupingId"?: string   // optional — specific grouping level
}

Use getDataModelSpec to inspect an existing data model for its element IDs.

Example:
{ "kind": "data-model", "dataModelId": "f8a9b0c1-...", "elementId": "customers-table" }

---

For join, union, and transpose sources, read the dedicated sub-resources:
- sigma://data-model-spec/join-source — IMPORTANT: name fields are required for column formulas
- sigma://data-model-spec/union-source
- sigma://data-model-spec/transpose-source
`;

const JOIN_SOURCE_RESOURCE_TEXT = `Data Model Spec - Join Source

Combines multiple sources via SQL JOINs.

{
  "kind": "join",
  "joins": JoinRelationship[],        // required — at least one
  "primarySource"?: ComposableSource,  // optional — inferred if omitted
  "name"?: string                      // optional but REQUIRED if you add columns to this table — see Formula Naming below
}

ComposableSource (allowed inside joins):
  Only these 3 kinds: warehouse-table, table, data-model.
  Join/union/sql/transpose cannot be used directly — create a separate table element and reference it with kind:"table".

JoinRelationship:
{
  "left": ComposableSource,
  "right": ComposableSource,
  "columns": JoinColumnMapping[],  // required — join conditions
  "joinType"?: "inner" | "left-outer" | "right-outer" | "full-outer" | "lookup",
  "name"?: string                   // optional but REQUIRED if you add columns from this right source — see Formula Naming below
}

JoinColumnMapping:
{
  "left": string,   // column reference from left source, e.g. "[CustomerId]"
  "right": string,  // column reference from right source, e.g. "[Id]"
  "op"?: "=" | "!=" | "<" | "<=" | ">" | ">=" | "within" | "intersects"  // default "="
}

Join column mapping formulas (the "left" and "right" fields above):
- Use simple [ColumnName] — no prefix needed because each side is scoped to its source.
- ColumnName must match database column names from the source's warehouse schema.
- These are CASE-SENSITIVE (unlike regular column formulas).
- Can be expressions: DateTrunc("year", [Date]) or [Price] * [Quantity].

Rules:
- All sources in a join must share the same database connection.
- The join graph must form a tree (no cycles).
- primarySource is inferred as the unique source that appears on the left but never on the right.

---

FORMULA NAMING (critical for writing columns on join-sourced tables)

When a table's source is a join, column formulas MUST use [InputName/ColumnName] format.
The InputName is determined by the "name" fields on the join:

  join source "name" field → formula prefix for HEAD/PRIMARY source columns
  join relationship "name" field → formula prefix for each RIGHT source's columns

Without these names set, the system cannot resolve column references and formulas will fail.

Anti-pattern — do NOT guess suffixes like [ORDERS (1)/...] or [TABLE_NAME/...].
Always explicitly set the "name" fields and use those exact names as formula prefixes.

---

COMPLETE EXAMPLE (join with named inputs + columns)

{
  "id": "joined-table",
  "kind": "table",
  "name": "Orders with Customers",
  "source": {
    "kind": "join",
    "name": "Orders",
    "joins": [
      {
        "left": { "kind": "warehouse-table", "connectionId": "c", "path": ["DB", "S", "ORDERS"] },
        "right": { "kind": "warehouse-table", "connectionId": "c", "path": ["DB", "S", "CUSTOMERS"] },
        "columns": [{ "left": "[CustomerId]", "right": "[Id]" }],
        "joinType": "left-outer",
        "name": "Customers"
      }
    ]
  },
  "columns": [
    { "id": "order-id", "formula": "[Orders/OrderId]", "name": "Order ID" },
    { "id": "amount", "formula": "[Orders/Amount]", "name": "Amount" },
    { "id": "cust-name", "formula": "[Customers/Name]", "name": "Customer Name" },
    { "id": "cust-email", "formula": "[Customers/Email]", "name": "Customer Email" }
  ]
}

Note how:
- source.name is "Orders" → columns use [Orders/...] for head source columns
- joins[0].name is "Customers" → columns use [Customers/...] for right source columns
- join column mappings use simple [CustomerId] and [Id] (no prefix, case-sensitive)

---

3-way join example:

{
  "kind": "join",
  "name": "Orders",
  "joins": [
    {
      "left": { "kind": "warehouse-table", "connectionId": "c", "path": ["DB", "S", "ORDERS"] },
      "right": { "kind": "warehouse-table", "connectionId": "c", "path": ["DB", "S", "CUSTOMERS"] },
      "columns": [{ "left": "[CustomerId]", "right": "[Id]" }],
      "joinType": "left-outer",
      "name": "Customers"
    },
    {
      "left": { "kind": "warehouse-table", "connectionId": "c", "path": ["DB", "S", "ORDERS"] },
      "right": { "kind": "warehouse-table", "connectionId": "c", "path": ["DB", "S", "PRODUCTS"] },
      "columns": [{ "left": "[ProductId]", "right": "[Id]" }],
      "joinType": "inner",
      "name": "Products"
    }
  ]
}

Column formulas: [Orders/OrderId], [Customers/Name], [Products/ProductName]
`;

const UNION_SOURCE_RESOURCE_TEXT = `Data Model Spec - Union Source

Vertically concatenates rows from multiple sources (UNION ALL).

{
  "kind": "union",
  "sources": ComposableSource[],  // required — sources to union
  "matches": UnionMatch[]         // required — output column mappings
}

ComposableSource: only warehouse-table, table, or data-model kinds.

UnionMatch:
{
  "outputColumnName": string,        // name of the output column
  "sourceColumns": (string | null)[] // one entry per source — column name or null if missing
}

Rules:
- sourceColumns array length MUST equal sources array length.
- All sources must share the same database connection.
- Use null for a source that lacks a matching column (produces NULL values).

Example (union of US and EU sales):
{
  "kind": "union",
  "sources": [
    { "kind": "warehouse-table", "connectionId": "c", "path": ["DB", "PUBLIC", "US_SALES"] },
    { "kind": "warehouse-table", "connectionId": "c", "path": ["DB", "PUBLIC", "EU_SALES"] }
  ],
  "matches": [
    { "outputColumnName": "OrderId", "sourceColumns": ["OrderId", "OrderId"] },
    { "outputColumnName": "Amount", "sourceColumns": ["Amount", "SaleAmount"] },
    { "outputColumnName": "Region", "sourceColumns": ["Region", null] }
  ]
}
`;

const TRANSPOSE_SOURCE_RESOURCE_TEXT = `Data Model Spec - Transpose Source

Reshapes data via PIVOT (row-to-column) or UNPIVOT (column-to-row).

ComposableSource: only warehouse-table, table, or data-model kinds.

--- Row-to-Column (PIVOT) ---

{
  "kind": "transpose",
  "source": ComposableSource,
  "direction": "row-to-column",
  "outputColumns": string[],       // list of new column names created by the pivot
  "columnToTranspose": string,     // column whose values become column headers
  "valueColumn": string,           // column with cell values
  "aggregate": "min" | "max" | "count" | "count-if" | "count-distinct" | "sum" | "avg" | "median"
}

Example:
{
  "kind": "transpose",
  "source": { "kind": "warehouse-table", "connectionId": "c", "path": ["DB", "S", "SALES"] },
  "direction": "row-to-column",
  "outputColumns": ["Q1", "Q2", "Q3", "Q4"],
  "columnToTranspose": "Quarter",
  "valueColumn": "Revenue",
  "aggregate": "sum"
}

--- Column-to-Row (UNPIVOT) ---

{
  "kind": "transpose",
  "source": ComposableSource,
  "direction": "column-to-row",
  "columnsToMerge": string[],                // columns to stack (must be same data type)
  "columnLabelForMergedColumns": string,     // header for the "which column" column
  "columnLabelForValues": string             // header for the values column
}

Example:
{
  "kind": "transpose",
  "source": { "kind": "warehouse-table", "connectionId": "c", "path": ["DB", "S", "FINANCIALS"] },
  "direction": "column-to-row",
  "columnsToMerge": ["Revenue", "Cost", "Profit"],
  "columnLabelForMergedColumns": "Metric",
  "columnLabelForValues": "Value"
}
`;

const COLUMN_RESOURCE_TEXT = `Data Model Spec - Column

{
  "id": string,          // required — unique within this table
  "formula": string,     // required — column reference or calculation
  "name"?: string,       // optional — display name
  "description"?: string,// optional
  "hidden"?: boolean     // optional — hides column from UI
}

This column's id is referenced by: order[], summary[], filter.columnId, folder.items[], grouping.groupBy[], grouping.calculations[], relationship.keys[].sourceColumnId, relationship.keys[].targetColumnId.

---

FORMULA PREFIX BY SOURCE TYPE

The formula prefix depends on the table's source kind. This is critical — using the wrong prefix will cause errors.

source kind: warehouse-table
  Format: [TableName/ColumnName]
  TableName = the last segment of the source path (i.e., the warehouse table name).
  ColumnName = the database column name from the warehouse schema.
  Case-insensitive.
  Example: If path is ["DATABASE", "SCHEMA", "ORDERS"], use [ORDERS/OrderId], [ORDERS/Amount]
  Example: If path is ["EXAMPLES_NEW", "BIKES", "TRIP"], use [TRIP/Id], [TRIP/Duration]

  IMPORTANT: Do NOT omit the TableName prefix. [Id] is WRONG. [TRIP/Id] is CORRECT.

source kind: sql
  Format: [TableName/ColumnName]
  TableName = the "name" field of the table element (the element's display name, not the element id).
  ColumnName = the output column name from the SQL query.
  Case-insensitive.

source kind: table or data-model
  Format: [TableName/ColumnName]
  TableName = the "name" field of the referenced table element (not the element id).
  ColumnName = the display name (or id) of columns in the referenced table.
  Case-insensitive.
  Example: If the referenced table element has name "Orders", use [Orders/Revenue], [Orders/Order Date]

source kind: join
  Format: [InputName/ColumnName]
  InputName comes from the join's "name" fields:
    - join source "name" field → prefix for head/primary source columns
    - join relationship "name" field → prefix for each right source's columns
  You MUST set these name fields when the table has columns.
  Case-insensitive.
  Example: If join source name is "Orders" and relationship name is "Customers":
    [Orders/OrderId], [Orders/Amount], [Customers/Name], [Customers/Email]
  See sigma://data-model-spec/join-source for full details.

source kind: union
  Format: [ColumnName]
  ColumnName = the outputColumnName from the union's matches array.
  Case-insensitive.

source kind: transpose
  Format: [ColumnName]
  ColumnName = output column names from the transpose configuration.

---

FORMULA SYNTAX

Column references:
  [ColumnName]              — simple reference
  [Prefix/ColumnName]       — qualified reference (join sources, cross-table via relationship)

Escaping special characters inside brackets:
  \\/ for literal /    \\[ for literal [    \\] for literal ]    \\\\ for literal \\

Operators:
  + - * / %               — arithmetic
  &                        — string concatenation
  = != < <= > >=          — comparison
  And Or Not              — logical

Aggregation functions (PascalCase, case-insensitive):
  Sum([Col])  Avg([Col])  Count([Col])  CountDistinct([Col])
  Min([Col])  Max([Col])  Median([Col])

Date functions:
  DateTrunc("month", [Date])  DateDiff("day", [Start], [End])
  Year([Date])  Month([Date])  Day([Date])

String functions:
  Contains([Text], "search")  Left([Text], 5)  Upper([Text])  Trim([Text])

Other:
  If([Amount] > 100, "High", "Low")   — conditional
  Coalesce([A], [B])                   — null handling

Rules:
- Function names are PascalCase but case-insensitive (Sum, SUM, sum all work).
- String literals use double quotes "text" or single quotes 'text'.
- Cross-table references via relationships: [RelatedTableName/ColumnName].

---

Example (warehouse-table source with path ["ANALYTICS_DB", "PUBLIC", "ORDERS"]):
[
  { "id": "order-id", "formula": "[ORDERS/OrderId]", "name": "Order ID" },
  { "id": "revenue", "formula": "[ORDERS/Price] * [ORDERS/Quantity]", "name": "Revenue" },
  { "id": "total", "formula": "Sum([ORDERS/Amount])", "name": "Total", "hidden": true }
]
`;

const METRIC_RESOURCE_TEXT = `Data Model Spec - Metric

{
  "id": string,          // required — unique within this table's metrics
  "formula": string,     // required — typically an aggregation
  "name"?: string,       // optional — display name
  "description"?: string // optional
}

FORMULA RULES FOR METRICS

Metric formulas use the SAME resolution as column formulas — see sigma://data-model-spec/column for full syntax.

Critical: metric formulas reference SOURCE column names (from the database/warehouse schema), NOT the column "id" values you defined in the spec's columns array. The column "id" is an internal identifier for cross-referencing within the spec — it cannot be used in formulas.

For warehouse-table/sql sources:
  Formula references use database column names: Sum([Amount]), Avg([Price])

For join sources:
  Formula references MUST use the join input name prefix: Sum([Orders/Amount]), Avg([Products/Price])
  The prefix comes from the join's "name" fields — see sigma://data-model-spec/join-source.

Metrics can also reference:
  - Calculated columns by their display name (the "name" field on a column, if set)
  - Other metrics on the same table by their display name

Example (warehouse-table source where database has columns Amount, Price, CustomerId):

Given columns:
  { "id": "amt", "formula": "[Amount]", "name": "Order Amount" }
  { "id": "price", "formula": "[Price]", "name": "Unit Price" }

Correct metric formulas:
  { "id": "total-rev", "formula": "Sum([Amount])", "name": "Total Revenue" }
  { "id": "avg-price", "formula": "Avg([Price])", "name": "Avg Price" }
  { "id": "unique-cust", "formula": "CountDistinct([CustomerId])", "name": "Unique Customers" }

WRONG — do not use column spec IDs in formulas:
  { "formula": "Sum([amt])" }         ← WRONG: "amt" is the spec column id, not the source column name
  { "formula": "Sum([Order Amount])" } ← this works only if a sheet column named "Order Amount" exists

Example (join source with name="Orders"):
  { "id": "total-rev", "formula": "Sum([Orders/Amount])", "name": "Total Revenue" }
`;

const RELATIONSHIP_RESOURCE_TEXT = `Data Model Spec - Relationship

Defines a join between two tables in the data model. Enables cross-table formula references: [TargetTableName/ColumnName].

{
  "id": string,               // required — unique within this table's relationships
  "targetElementId": string,   // required — MUST match another table element's id
  "keys": [                    // required — at least one key pair
    {
      "sourceColumnId": string,  // MUST match a column id in THIS table
      "targetColumnId": string   // MUST match a column id in the TARGET table
    }
  ],
  "name"?: string,
  "description"?: string
}

All three IDs (targetElementId, sourceColumnId, targetColumnId) MUST exactly match existing IDs.

Example — orders to customers on customer_id:
{
  "id": "orders-to-customers",
  "targetElementId": "customers-table",
  "keys": [{ "sourceColumnId": "customer-id", "targetColumnId": "cust-id" }]
}

Composite key example:
{
  "id": "multi-key",
  "targetElementId": "products-table",
  "keys": [
    { "sourceColumnId": "product-id", "targetColumnId": "prod-id" },
    { "sourceColumnId": "region", "targetColumnId": "region" }
  ]
}
`;

const FILTER_RESOURCE_TEXT = `Data Model Spec - Column Filter

Base fields (all filter kinds):
{
  "id": string,        // required — unique within this table's filters
  "columnId": string,   // required — MUST match a column id in this table
  "state"?: "enabled" | "disabled",  // optional, default "enabled"
  "kind": string,       // required — discriminant (see below)
  ...kindSpecificFields
}

--- kind: "number-range" ---
{ "kind": "number-range", "min"?: number, "max"?: number, "includeNulls"?: IncludeNulls }

--- kind: "list" ---
{ "kind": "list", "mode"?: "include" | "exclude", "values"?: (string | number | boolean | null)[] }
Values must be the same type. Dates use ISO 8601 strings.

--- kind: "text-match" ---
{
  "kind": "text-match",
  "mode": "equals" | "does-not-equal" | "contains" | "does-not-contain" | "starts-with" | "does-not-start-with" | "ends-with" | "does-not-end-with" | "like" | "not-like" | "matches-regexp" | "does-not-match-regexp",
  "value"?: string,
  "case"?: "sensitive" | "insensitive",
  "includeNulls"?: IncludeNulls
}

--- kind: "date-range" ---
Discriminated by "mode":
  mode "custom":  { "startDate": DateValue, "endDate": DateValue }
  mode "current": { "unit": DateUnit }
  mode "last":    { "value": number, "unit": DateUnit, "includeToday": boolean }
  mode "next":    { "value": number, "unit": DateUnit, "includeToday": boolean }
  mode "after":   { "date": string }  (ISO 8601)
  mode "before":  { "date": string }  (ISO 8601)
  mode "on":      { "date": string }  (ISO 8601)
Optional: "includeNulls"?: IncludeNulls

DateUnit: "year" | "quarter" | "month" | "week-starting-sunday" | "week-starting-monday" | "day" | "hour" | "minute"
DateValue: ISO 8601 string OR relative { "op": "now-minus" | "now-plus", "unit": DateUnit, "value": number }

--- kind: "top-n" ---
By row count:
{ "kind": "top-n", "mode": "top-n" | "bottom-n", "rankingFunction": "rank" | "rank-dense" | "row-number", "rowCount"?: number, "includeNulls"?: IncludeNulls }

By percentile:
{ "kind": "top-n", "mode": "top-percentile" | "bottom-percentile", "rankingFunction": "rank-percentile" | "cume-dist", "percentile"?: number, "includeNulls"?: IncludeNulls }

IncludeNulls: "always" | "never" | "when-no-value-is-selected"

Example:
[
  { "id": "active-only", "columnId": "status-col", "kind": "list", "mode": "include", "values": ["Active"] },
  { "id": "recent", "columnId": "date-col", "kind": "date-range", "mode": "last", "value": 90, "unit": "day", "includeToday": true },
  { "id": "high-val", "columnId": "amount-col", "kind": "number-range", "min": 100 }
]
`;

const FOLDER_RESOURCE_TEXT = `Data Model Spec - Folder

Organizes columns into groups in the UI.

{
  "id": string,       // required — unique within this table, must not collide with column IDs
  "name": string,     // required — display name
  "items"?: string[]  // optional — column IDs and/or folder IDs in this folder
}

Folders can nest (a folder ID in another folder's items).
Folder IDs can appear in the table's "order" array to control positioning.

Example:
[
  { "id": "customer-info", "name": "Customer Info", "items": ["cust-name", "cust-email"] },
  { "id": "order-details", "name": "Order Details", "items": ["order-date", "amount"] }
]
`;

const GROUPING_RESOURCE_TEXT = `Data Model Spec - Grouping

Defines how rows are grouped and what aggregates are computed per group.

{
  "id": string,              // required — unique within this table's groupings
  "groupBy"?: string[],      // optional — column IDs that define the group keys
  "calculations"?: string[], // optional — column IDs for aggregate calcs in each group
  "sort"?: LevelSortKey[]    // optional — sort within this level
}

LevelSortKey: { "columnId": string, "direction": "ascending" | "descending", "nulls"?: "first" | "last" | "connection-default" }

All IDs in groupBy, calculations, and sort.columnId MUST match column IDs in this table.

This grouping's id is referenced by source.groupingId when another table uses this table as a source at this grouping level.

Example:
{
  "id": "by-region",
  "groupBy": ["region"],
  "calculations": ["total-revenue"]
}

Referenced as:
{ "kind": "table", "elementId": "sales-table", "groupingId": "by-region" }
`;

const CONTROL_RESOURCE_TEXT = `Data Model Spec - Control Element

Base fields (all control types):
{
  "kind": "control",         // required — literal
  "id": string,              // required — unique across all elements
  "controlId": string,       // required — the variable name used to reference this control in formulas (NOT the same as id)
  "controlType": string,     // required — discriminant for which control variant
  "filters"?: ControlFilter[],
  "parameters"?: ControlParameter[]
}

ControlFilter: { "source": { "kind": "table"|"warehouse-table"|"data-model", ... }, "columnId": string }
ControlParameter: { "kind": "data-model", "dataModelId": string, "controlId": string }

--- controlType: "checkbox" / "switch" ---
{ "mode": "True/False" | "True/All", "value"?: boolean }

--- controlType: "text" ---
{ "mode": "equals"|"does-not-equal"|"contains"|"does-not-contain"|"starts-with"|"does-not-start-with"|"ends-with"|"does-not-end-with"|"like"|"not-like"|"matches-regexp"|"does-not-match-regexp", "value"?: string, "case"?: "sensitive"|"insensitive", "includeNulls"?: IncludeNulls, "showOperators"?: boolean }

--- controlType: "text-area" ---
{ "value"?: string }

--- controlType: "number" ---
{ "mode": "<="|"="|">=", "value"?: number, "includeNulls"?: IncludeNulls }

--- controlType: "number-range" ---
{ "min"?: number, "max"?: number, "includeNulls"?: IncludeNulls }

--- controlType: "date" ---
{ "mode": "<="|"="|">=", "value"?: DateValue, "includeNulls"?: IncludeNulls }
DateValue: ISO 8601 string OR { "op": "now-minus"|"now-plus", "unit": DateUnit, "value": number }

--- controlType: "date-range" ---
Same date-range modes as filter (see sigma://data-model-spec/filter):
  custom/current/last/next/after/before/on
"includeNulls"?: IncludeNulls

--- controlType: "top-n" ---
Same structure as top-n filter (see sigma://data-model-spec/filter).

--- controlType: "list" ---
{
  "source"?: ListSource,
  "selectionMode"?: "single" | "multiple",
  "mode"?: "include" | "exclude",
  "value"?: scalar,          // for single selection
  "values"?: scalar[]        // for multiple selection (default)
}
ListSource manual: { "kind": "manual", "valueType": "text"|"number"|"date"|"boolean", "values"?: array, "labels"?: (string|null)[] }
ListSource source: { "kind": "source", "source": ComposableSource, "columnId": string, "displayColumnId"?: string }

--- controlType: "segmented" ---
{ "source"?: ListSource, "value"?: scalar, "clearLabel"?: string, "showClearLabel"?: boolean }

--- controlType: "hierarchy" ---
{ "source"?: { "source": TableSource, "columnId": string }, "mode"?: "include"|"exclude", "values"?: string[][] }
Values are hierarchical paths: [["East"], ["West", "California"]]

--- controlType: "slider" ---
{ "mode": "<="|"="|">=", "low"?: number, "high"?: number, "step"?: number, "value"?: number, "includeNulls"?: IncludeNulls }

--- controlType: "range-slider" ---
{ "low"?: number, "high"?: number, "step"?: number, "min"?: number, "max"?: number, "includeNulls"?: IncludeNulls }

IncludeNulls: "always" | "never" | "when-no-value-is-selected"
DateUnit: "year"|"quarter"|"month"|"week-starting-sunday"|"week-starting-monday"|"day"|"hour"|"minute"

Example — list control with filter target:
{
  "kind": "control",
  "id": "region-ctrl",
  "controlId": "region_filter",
  "controlType": "list",
  "source": { "kind": "manual", "valueType": "text", "values": ["East", "West", "North", "South"] },
  "selectionMode": "multiple",
  "mode": "include",
  "values": ["East"],
  "filters": [{ "source": { "kind": "table", "elementId": "orders-table" }, "columnId": "region-col" }]
}
`;

// ── Registration ─────────────────────────────────────────────────────────────

/** Register all data model spec resources on the given MCP server. */
export function registerResources(server: McpServer): void {
  server.resource(
    'dataModelSpec',
    'sigma://data-model-spec/data-model',
    async (uri) => ({
      contents: [{ uri: uri.href, mimeType: 'text/plain', text: DATA_MODEL_RESOURCE_TEXT }]
    })
  );

  server.resource(
    'dataModelSpecPage',
    'sigma://data-model-spec/page',
    async (uri) => ({
      contents: [{ uri: uri.href, mimeType: 'text/plain', text: PAGE_RESOURCE_TEXT }]
    })
  );

  server.resource(
    'dataModelSpecElement',
    'sigma://data-model-spec/element',
    async (uri) => ({
      contents: [{ uri: uri.href, mimeType: 'text/plain', text: ELEMENT_RESOURCE_TEXT }]
    })
  );

  server.resource(
    'dataModelSpecTable',
    'sigma://data-model-spec/table',
    async (uri) => ({
      contents: [{ uri: uri.href, mimeType: 'text/plain', text: TABLE_RESOURCE_TEXT }]
    })
  );

  server.resource(
    'dataModelSpecSource',
    'sigma://data-model-spec/source',
    async (uri) => ({
      contents: [{ uri: uri.href, mimeType: 'text/plain', text: SOURCE_RESOURCE_TEXT }]
    })
  );

  server.resource(
    'dataModelSpecJoinSource',
    'sigma://data-model-spec/join-source',
    async (uri) => ({
      contents: [{ uri: uri.href, mimeType: 'text/plain', text: JOIN_SOURCE_RESOURCE_TEXT }]
    })
  );

  server.resource(
    'dataModelSpecUnionSource',
    'sigma://data-model-spec/union-source',
    async (uri) => ({
      contents: [{ uri: uri.href, mimeType: 'text/plain', text: UNION_SOURCE_RESOURCE_TEXT }]
    })
  );

  server.resource(
    'dataModelSpecTransposeSource',
    'sigma://data-model-spec/transpose-source',
    async (uri) => ({
      contents: [{ uri: uri.href, mimeType: 'text/plain', text: TRANSPOSE_SOURCE_RESOURCE_TEXT }]
    })
  );

  server.resource(
    'dataModelSpecColumn',
    'sigma://data-model-spec/column',
    async (uri) => ({
      contents: [{ uri: uri.href, mimeType: 'text/plain', text: COLUMN_RESOURCE_TEXT }]
    })
  );

  server.resource(
    'dataModelSpecMetric',
    'sigma://data-model-spec/metric',
    async (uri) => ({
      contents: [{ uri: uri.href, mimeType: 'text/plain', text: METRIC_RESOURCE_TEXT }]
    })
  );

  server.resource(
    'dataModelSpecRelationship',
    'sigma://data-model-spec/relationship',
    async (uri) => ({
      contents: [{ uri: uri.href, mimeType: 'text/plain', text: RELATIONSHIP_RESOURCE_TEXT }]
    })
  );

  server.resource(
    'dataModelSpecFilter',
    'sigma://data-model-spec/filter',
    async (uri) => ({
      contents: [{ uri: uri.href, mimeType: 'text/plain', text: FILTER_RESOURCE_TEXT }]
    })
  );

  server.resource(
    'dataModelSpecFolder',
    'sigma://data-model-spec/folder',
    async (uri) => ({
      contents: [{ uri: uri.href, mimeType: 'text/plain', text: FOLDER_RESOURCE_TEXT }]
    })
  );

  server.resource(
    'dataModelSpecGrouping',
    'sigma://data-model-spec/grouping',
    async (uri) => ({
      contents: [{ uri: uri.href, mimeType: 'text/plain', text: GROUPING_RESOURCE_TEXT }]
    })
  );

  server.resource(
    'dataModelSpecControl',
    'sigma://data-model-spec/control',
    async (uri) => ({
      contents: [{ uri: uri.href, mimeType: 'text/plain', text: CONTROL_RESOURCE_TEXT }]
    })
  );
}
