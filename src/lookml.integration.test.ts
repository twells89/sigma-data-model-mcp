/**
 * LookML integration tests — convert and POST to Sigma.
 *
 * Each test converts a real LookML explore, posts to the Sigma API, asserts
 * success, then deletes the created data model. Failures leave no orphans if
 * cleanup is reached; the afterEach guard handles partial failures.
 *
 * Requires environment variables (already set in this shell):
 *   SIGMA_BASE_URL     https://aws-api.sigmacomputing.com
 *   SIGMA_CLIENT_ID
 *   SIGMA_CLIENT_SECRET
 *
 * Connection:  3A7GBHg1iYo4w7ORDZgbj0  (Snowflake — CSA.TJ.*)
 * Test folder: 9ca9bf60-6a33-43dd-967d-1ba6352c54bb  (My Documents/Test)
 */

import { describe, test, before, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { convertLookMLToSigma } from './lookml.js';

const LOOKML_DIR   = '/Users/tjwells/Desktop/Converter Files/Looker/';
const BASE_URL     = process.env['SIGMA_BASE_URL']!;
const CLIENT_ID    = process.env['SIGMA_CLIENT_ID']!;
const CLIENT_SECRET = process.env['SIGMA_CLIENT_SECRET']!;
const CONNECTION_ID = 'cb2f5180-641f-47bd-8efa-da9d590d855a';
const FOLDER_ID     = '9ca9bf60-6a33-43dd-967d-1ba6352c54bb';

function lkml(filename: string): { name: string; content: string } {
  return { name: filename, content: readFileSync(LOOKML_DIR + filename, 'utf8') };
}

const retailFiles = [
  'retail_analytics.model.lkml',
  'order_fact.view.lkml',
  'customer_dim.view.lkml',
  'product_dim.view.lkml',
  'store_dim.view.lkml',
  'date_dim.view.lkml',
  'promo_dim.view.lkml',
  'vip_customer.view.lkml',
  'monthly_revenue_summary.view.lkml',
].map(lkml);

const twohopFiles = [
  'twohop.model.lkml',
  'order_fact.view.lkml',
  'order_fact_alias.view.lkml',
  'order_fact_double_alias.view.lkml',
  'channel_twohop_derived.view.lkml',
].map(lkml);

const pdtSelfJoinFiles = [
  'pdt_self_join.model.lkml',
  'order_fact_pdt.view.lkml',
  'pdt_self_join_derived.view.lkml',
].map(lkml);

// ── Auth ──────────────────────────────────────────────────────────────────────

let accessToken = '';

async function getToken(): Promise<string> {
  if (accessToken) return accessToken;
  const res = await fetch(`${BASE_URL}/v2/auth/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'client_credentials',
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
    }),
  });
  assert.ok(res.ok, `Auth failed: ${res.status}`);
  const data = await res.json() as { access_token: string };
  accessToken = data.access_token;
  return accessToken;
}

async function authHeaders(): Promise<Record<string, string>> {
  return { Authorization: `Bearer ${await getToken()}`, 'Content-Type': 'application/json' };
}

// ── API helpers ───────────────────────────────────────────────────────────────

async function postDataModel(name: string, pages: any[]): Promise<string> {
  const res = await fetch(`${BASE_URL}/v2/dataModels/spec`, {
    method: 'POST',
    headers: await authHeaders(),
    body: JSON.stringify({ name, schemaVersion: 1, folderId: FOLDER_ID, pages }),
  });
  const body = await res.json() as { success?: boolean; dataModelId?: string; message?: string };
  assert.ok(res.ok, `POST /v2/dataModels/spec failed (${res.status}): ${body.message ?? JSON.stringify(body)}`);
  assert.ok(body.dataModelId, `Response missing dataModelId: ${JSON.stringify(body)}`);
  return body.dataModelId!;
}

async function deleteDataModel(id: string): Promise<void> {
  await fetch(`${BASE_URL}/v2/files/${id}`, {
    method: 'DELETE',
    headers: await authHeaders(),
  });
}

// ── Test state ────────────────────────────────────────────────────────────────

const createdIds: string[] = [];

// Always clean up, even on test failure
afterEach(async () => {
  while (createdIds.length > 0) {
    const id = createdIds.pop()!;
    await deleteDataModel(id);
  }
});

// ── Integration tests ─────────────────────────────────────────────────────────

describe('integration: LookML → Sigma API', () => {
  before(async () => {
    assert.ok(BASE_URL, 'SIGMA_BASE_URL not set');
    assert.ok(CLIENT_ID, 'SIGMA_CLIENT_ID not set');
    assert.ok(CLIENT_SECRET, 'SIGMA_CLIENT_SECRET not set');
    await getToken();
  });

  test('product_with_promo (1:1 join): converts and posts successfully', async () => {
    // product_dim + promo_dim one_to_one join. Avoids order_fact/customer_dim which have
    // pre-existing computed-column issues tracked in beads-sigma-u7e.
    const { model, warnings } = convertLookMLToSigma(retailFiles, {
      exploreName: 'product_with_promo',
      connectionId: CONNECTION_ID,
    });
    const fatal = warnings.filter(w => /parse error|exception/i.test(w));
    assert.equal(fatal.length, 0, `Conversion errors: ${fatal.join('; ')}`);

    const id = await postDataModel('[TEST] Product With Promo', model.pages);
    createdIds.push(id);
    assert.ok(id, 'No dataModelId returned');
  });

  test('one-hop sql_table_name alias (gap-1): resolves path and posts successfully', async () => {
    const { model } = convertLookMLToSigma(twohopFiles, {
      exploreName: 'order_fact_alias',
      connectionId: CONNECTION_ID,
    });
    const el = model.pages[0].elements.find((e: any) => e.source?.kind === 'warehouse-table');
    assert.deepEqual(el?.source.path, ['CSA', 'TJ', 'ORDER_FACT'], 'Path not resolved before posting');

    const id = await postDataModel('[TEST] Order Fact Alias (one-hop)', model.pages);
    createdIds.push(id);
    assert.ok(id, 'No dataModelId returned');
  });

  test('two-hop sql_table_name chain (gap-1): resolves path and posts successfully', async () => {
    const { model } = convertLookMLToSigma(twohopFiles, {
      exploreName: 'order_fact_double_alias',
      connectionId: CONNECTION_ID,
    });
    const el = model.pages[0].elements.find((e: any) => e.source?.kind === 'warehouse-table');
    assert.deepEqual(el?.source.path, ['CSA', 'TJ', 'ORDER_FACT'], 'Two-hop chain not resolved before posting');

    const id = await postDataModel('[TEST] Order Fact Double Alias (two-hop)', model.pages);
    createdIds.push(id);
    assert.ok(id, 'No dataModelId returned');
  });

  test('derived table with SQL_TABLE_NAME ref (gap-1): resolves SQL and posts successfully', async () => {
    const { model } = convertLookMLToSigma(twohopFiles, {
      exploreName: 'channel_twohop_derived',
      connectionId: CONNECTION_ID,
    });
    const sqlEl = model.pages[0].elements.find((e: any) => e.source?.kind === 'sql');
    assert.ok(sqlEl?.source.statement.includes('CSA.TJ.ORDER_FACT'), 'SQL not resolved before posting');

    const id = await postDataModel('[TEST] Channel Twohop Derived (SQL_TABLE_NAME)', model.pages);
    createdIds.push(id);
    assert.ok(id, 'No dataModelId returned');
  });

  test('PDT-references-PDT self-join (customer pattern, gap-1): resolves SQL and posts successfully', async () => {
    const { model } = convertLookMLToSigma(pdtSelfJoinFiles, {
      exploreName: 'pdt_self_join_derived',
      connectionId: CONNECTION_ID,
    });
    const sqlEl = model.pages[0].elements.find((e: any) => e.source?.kind === 'sql');
    const stmt: string = sqlEl?.source.statement ?? '';
    assert.ok(!stmt.includes('${'), `Unresolved refs in SQL before posting:\n${stmt}`);
    assert.ok(stmt.includes('CSA.TJ.ORDER_FACT'), `Physical table not found in SQL:\n${stmt}`);
    assert.ok(!new RegExp('\\) AS order_fact_pdt').test(stmt), `Spurious AS alias found — invalid SQL:\n${stmt}`);

    const id = await postDataModel('[TEST] PDT Self-Join (customer pattern)', model.pages);
    createdIds.push(id);
    assert.ok(id, 'No dataModelId returned');
  });
});
