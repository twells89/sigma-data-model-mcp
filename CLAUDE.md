# Sigma Data Model MCP — Project Rules

## Pre-commit requirement
**ALWAYS run `/review-commit` before any `git commit` in this repo.**
Step 9 of that command audits the converter spec correctness rules below.

## Architecture
TypeScript MCP server providing a `convert_<source>_to_sigma` tool per source format. Each converter lives in `src/<source>.ts` and exports a `convert*ToSigma` function returning `{ model, warnings, stats }`. Build target is `build/`. The companion browser tool at `/Users/tjwells/sigma-data-model-manager/index.html` mirrors these converters in inline JS — keep them in sync (see `feedback_mcp_sync.md`).

## Sigma data model JSON spec correctness rules
These are bug classes that have shipped to production and broken Sigma POSTs. Every converter must satisfy all of them.

1. **`schemaVersion: 1`** at the model root — required for `/v2/dataModels/spec` to accept POSTs. Missing → "schema error" rejection. See `SigmaDataModel` interface in `src/sigma-ids.ts`.

2. **dbt-style relationship `name`** = uppercase target warehouse-table name (e.g., `CUSTOMER_DIM`), NOT a sigmaDisplayName phrase like `"Order Fact to Customer Dim"`. The relationship name is also the middle segment of three-part cross-element formulas `[SRC_TABLE/REL_NAME/Col]` in derived elements built by `buildDerivedElements`.

3. **Custom SQL elements** (`source.kind === 'sql'`):
   - OMIT the element-level `name` field entirely (not `null`, not present).
   - Column formulas use bare `[Display Name]` form for snake_case SQL columns (Sigma fuzzy-matches case/underscore variants for self-references inside the same SQL element).
   - The qualified `[Custom SQL/Display Name]` form ONLY works when the SQL emits literally-matching double-quoted aliases (`AS "Display Name"`) — most converters don't, so default to bare.

4. **Cross-element column references** use the relationship-name form `[ELEMENT_NAME/REL_NAME/Field]`. The dash-link form `[ELEMENT_NAME/FK_COL - link/Field]` does NOT work via the API — POSTs are silently rejected or refs render broken.

5. **Union elements** must use:
   - `source.kind: 'union'`
   - `sources: [{ kind: 'table', elementId: '<id>' }, ...]`
   - `matches: [{ outputColumnName: '...', sourceColumns: ['[Display]', ...] }, ...]`
   - Column formulas `[Union of N Sources/ColName]`
   The older `inputs: [...]` shape is wrong and silently fails.

## Sigma window functions in DM elements
`CountOver`/`SumOver` etc. silently error when used in workbook master calc cols (grouping-table) or data-model element calc cols. Workarounds and the ID-reassignment side effect are documented in memory file `feedback_sigma_window_functions.md`.

## Test fixtures (verified to POST cleanly against connection `cb2f5180-641f-47bd-8efa-da9d590d855a` — CSA.TJ schema)
- dbt: `/Users/tjwells/Downloads/retail_analytics_dbt.yml`
- snowflake: `/Users/tjwells/Downloads/retail_analytics_snowflake (1).yaml` (the one with `tables:` keys, NOT the `semantic_models:` variant)
- lookml: synthesize a minimal model pointing at `CSA.TJ.ORDER_FACT` / `CSA.TJ.CUSTOMER_DIM` (the LookML chain in Downloads references DPBPROD which isn't on this connection)
- powerbi: `/Users/tjwells/Downloads/retail_analytics.bim`
- alteryx: `/Users/tjwells/Downloads/retail_analytics_csa_tj.yxmd` (NOT `retail_analytics_pipeline.yxmd` — the latter has fake `ANALYTICS.PUBLIC.*` paths)
- cube: `/tmp/cube_tests/retail_analytics.yml`
- tableau-prep: `/tmp/prep_tests/test_retail.tfl`

## API testing pattern
```bash
bash -c 'eval "$(~/sigma-skills/tableau-to-sigma/scripts/get-token.sh)"; node /tmp/converter_tests/<script>.mjs'
```
NEVER `TOKEN=$(eval "$(...)")` — eval inside `$()` sets the var in a subshell that dies before curl runs.

POST endpoint: `${SIGMA_BASE_URL}/v2/dataModels/spec`, body `{ folderId, ...spec }`.

## Pre-commit check (runs in addition to spec correctness rules above)

When the diff modifies any `src/<converter>.ts`, run `npm run build` and a real-API end-to-end test before committing. The bar for PASS is: a query against the saved data model returns real warehouse data, NOT error-typed columns or all-null rows. Just verifying the POST returns 2xx is insufficient — Sigma will accept structurally-valid specs whose formulas don't resolve at query time.

For each converter touched:
1. Convert a real fixture (CSA.TJ-pointed; see Test fixtures section above) to a spec via the build module
2. POST to `${SIGMA_BASE_URL}/v2/dataModels/spec` with body `{ folderId: '9ca9bf60-...', ...spec }` and capture `dataModelId`
3. `mcp__sigma-mcp-v2__describe(type="datamodel-element", dataModelId, elementId)` on the largest fact-style element. **Every column must have a concrete type** (`text` / `integer` / `number` / `datetime` / `boolean`). Any column showing as `error` is a FAIL — the formula didn't resolve.
4. `mcp__sigma-mcp-v2__query(type="datamodel", dataModelId, sql="SELECT * FROM \"datamodel\".\"<elementId>\" LIMIT 5")`. Must return rows with real warehouse values. All-null rows or "Unknown column" errors are a FAIL.
5. **Cleanup (mandatory):** `DELETE ${SIGMA_BASE_URL}/v2/files/{dataModelId}` for each test data model created.

Test folder lists via `GET ${SIGMA_BASE_URL}/v2/files?parentId=9ca9bf60-...&limit=200` so you can sweep accumulated `TEST audit *` / `BROWSER TEST *` models.

**Don't substitute JSDOM-based tests for the real flow** — they bypass async setup and have produced false negatives on this codebase.

## Browser-tool sync

When a converter changes here, the matching inline-JS converter in `/Users/tjwells/sigma-data-model-manager/index.html` must be updated to keep the two implementations in lock-step. The browser-tool repo's `/review-commit` Step 10 runs the same data-verification check via Puppeteer driving the live UI.
