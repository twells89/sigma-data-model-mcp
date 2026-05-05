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

## Surface matrix

Each fixture is run against TWO surfaces. Both must pass for a green merge.

| Surface | Command | What it tests |
| --- | --- | --- |
| MCP | `npm run regression` | The TypeScript converters in `build/` (`convertTableauToSigma`, `convertLookMLToSigma`, …). Direct function call. |
| smm browser | `npm run regression:browser` | Same fixtures driven through the `index.html` browser tool via Puppeteer — the same code path real users hit. |

`npm run regression:all` runs both sequentially and exits 0 only if both surfaces pass every fixture.

## smm browser runner (`npm run regression:browser`)

Uses Puppeteer to:

1. Launch headless Chromium and load `file:///Users/tjwells/sigma-data-model-manager/index.html`.
2. Connect to Sigma via the UI (fills `#apiRegion`/`#clientId`/`#clientSecret`, clicks `#connectBtn`).
3. For each fixture: switch the converter tab via `#converterFormat`, feed the fixture's input through the converter's own UI handler (`ingestTableauXml`, `processOmniFiles`, `runDbtConversion`, etc.), set the connection / database / schema, run the conversion, and read the generated JSON from the converter's output textarea.
4. POST the JSON to Sigma's `/v2/dataModels/spec`, scan for error columns (same hard gate as the MCP runner), then DELETE the model.
5. Print a per-fixture pass/fail matrix and exit 0/1.

Why we run both: the smm browser tool and the MCP converters share family resemblance but were forked at points and have diverged. A converter can pass MCP and fail smm (and vice-versa) — for example, several smm converters omit the required `schemaVersion: 1` at the model root, which is invisible inside the smm Save modal (which adds it on save) but breaks any direct POST. The browser runner catches that whole class of bug.

## Roadmap

v1 (initial):
- Tableau format only.
- MCP converter only.
- 3 seed fixtures.

v2 (this commit):
- Puppeteer browser variant added for the smm browser tool (`npm run regression:browser`).
- 10 new cross-element fixtures (alteryx, cube, dbt, lookml, oac, omni, powerbi, qlik, thoughtspot) covering the bug class identified in the 2026-05-04 sweep.
- Surface matrix: every fixture exercised against both MCP and smm browser surfaces.

v3 (future):
- `tableau-local.html` browser variant.
- Optional `expected.query.json` (SQL + expected rows) for numeric drift detection.
- Pre-push git hook + GitHub Actions wiring.
