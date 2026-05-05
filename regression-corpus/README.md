# Regression Corpus

End-to-end test fixtures that gate merges. Run via `npm run regression`.

## What's here

```
regression-corpus/
├── tableau/
│   ├── lod_basic/
│   │   ├── input.twb
│   │   └── expected.summary.json
│   ├── setsbug_cross_element/
│   ├── customer_retail_real_tds/
│   └── ...
└── (lookml/, dbt/, pbi/, ... — add as fixtures grow)
```

Each fixture is a directory containing:

- **`input.<ext>`** — the source artifact (`.twb`, `.tds`, `.lkml`, `.bim`, etc.)
- **`expected.summary.json`** — assertions about the converted output:
  ```json
  {
    "description": "what this fixture is for and why it matters",
    "convertOptions": {
      "connectionId": "cb2f5180-...",
      "database": "TJ",
      "schema": "PUBLIC"
    },
    "asserts": {
      "minElements": 4,
      "minRelationships": 2,
      "minHelperElements": 1,
      "noErrorColumns": true
    }
  }
  ```
- (optional) **`README.md`** — long-form notes about the fixture, the bug it reproduces, etc.

## What the runner does (per fixture)

1. Read `input.<ext>`.
2. Run the matching MCP converter (`convertTableauToSigma`, etc.) with `convertOptions`.
3. Compute shape summary (element/column/metric/relationship/helper counts).
4. Check `asserts.minXxx` are met.
5. POST the model to Sigma's test folder (`SIGMA_TEST_FOLDER_ID`, default `9ca9bf60-...`).
6. **HARD GATE**: `GET /v2/dataModels/{id}/columns`. Any column with `type.type === "error"` fails the fixture.
7. `DELETE /v2/files/{dataModelId}` to clean up.

If POST fails or any column errors, the fixture FAILS. The runner exits 1 if any fixture fails.

## Adding a fixture

When you fix a bug, add a fixture that reproduces it. This is mandatory per the
"bug-driven corpus growth" rule (see `/review-commit` Step 10):

```bash
mkdir regression-corpus/tableau/<my_fixture>
cp /path/to/repro.twb regression-corpus/tableau/<my_fixture>/input.twb
cat > regression-corpus/tableau/<my_fixture>/expected.summary.json <<EOF
{
  "description": "Repro for beads-sigma-XXX. Triggered when ...",
  "convertOptions": { "connectionId": "...", "database": "...", "schema": "..." },
  "asserts": { "minElements": N, "noErrorColumns": true }
}
EOF
```

Then verify:

```bash
npm run build
npm run regression -- tableau/<my_fixture>
```

## Running

```bash
# One-time per env:
export SIGMA_BASE_URL=https://aws-api.sigmacomputing.com
export SIGMA_CLIENT_ID=...
export SIGMA_CLIENT_SECRET=...

npm run build
npm run regression                          # all fixtures
npm run regression -- tableau               # one format
npm run regression -- tableau/lod_basic     # one fixture
```

Exit code:
- `0` — every fixture passed.
- `1` — at least one fixture failed.
- `2` — runner error (missing env, no build, etc.).

## What this catches

- Any spec that POSTs successfully but has a runtime-broken column (`type.type === "error"`). On 2026-05-05 this caught the `[Customer Segment]` bare-formula bug that POSTed 200 but left the column unresolved.
- Shape regressions (helper element dedup breaking, relationships disappearing, etc.) via the `minXxx` asserts.
- Cross-element move pass coherence (folder-scrub + formula rewrite) via `customer_retail_real_tds` and `setsbug_cross_element`.

## Roadmap

v1 (this commit):
- Tableau format only
- MCP converter only (smm + tableau-local browser tools not yet exercised)
- 3 seed fixtures

v2 (future):
- Browser-tool variant via Puppeteer (runs same fixtures against `index.html` and `tableau-local.html`, asserts identical specs across surfaces)
- LookML, dbt, Power BI, etc. fixtures
- Optional `expected.query.json` (SQL + expected rows) for numeric drift detection
- Pre-push git hook + GitHub Actions wiring
