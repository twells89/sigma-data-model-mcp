import { readFileSync, writeFileSync } from 'node:fs';
import { convertTableauToSigma } from "/tmp/wt-blend/build/tableau.js";
const xml = readFileSync("/tmp/ddmx-stage1/workbook-content.twb", 'utf8');
const out = convertTableauToSigma(xml, {
  connectionId: "cb2f5180-641f-47bd-8efa-da9d590d855a",
  database: "REDACTED_DB",
  schema: "PUBLIC",
});
const bare = out.model || out.sigmaDataModel || out;
writeFileSync("/tmp/wt-blend/_e2e/run1/dm-raw.json", JSON.stringify(bare, null, 2));
// Capture out.security too — detected RLS/CLS rules (architecture B:
// reported, not injected). Dropping it here is how RLS silently
// vanished from the orchestrated path; the orchestrator now gates on it.
writeFileSync("/tmp/wt-blend/_e2e/run1/conv-meta.json", JSON.stringify({ model: bare, warnings: out.warnings || [], stats: out.stats || {}, security: out.security || [] }, null, 2));
