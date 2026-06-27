import { readFileSync, writeFileSync } from 'node:fs';
import { convertTableauToSigma } from "/tmp/wt-blend/build/tableau.js";
const xml = readFileSync("/tmp/ddmx-stage1/workbook-content.twb", 'utf8');
const out = convertTableauToSigma(xml, {
  connectionId: "bc0319f8-9fe0-4315-aea3-6a2d1eef0623",
  database: "REDACTED_DB", schema: "PUBLIC",
});
const bare = out.model || out.sigmaDataModel || out;
writeFileSync("/tmp/wt-blend/_e2e/dm-raw.json", JSON.stringify(bare, null, 2));
writeFileSync("/tmp/wt-blend/_e2e/conv-meta.json", JSON.stringify({ warnings: out.warnings||[], stats: out.stats||{}, security: out.security||[] }, null, 2));
console.log("warnings:", (out.warnings||[]).length, "stats:", JSON.stringify(out.stats||{}));
