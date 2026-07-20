/**
 * Per-invocation id-space scoping + 64-char id clamp (W2.4 / PR-16).
 *
 * Two defects in the module-global id counter:
 *  1. resetIds() zeroed a MODULE-GLOBAL counter at every top-level convert, so
 *     two INDEPENDENT conversions in one process minted byte-identical id
 *     sequences ("AAAAAAAAAB" twice). Merged — multi-datasource routing or a
 *     batch of workbooks in one node process — the DM POST failed on duplicate
 *     ids. Fix: seed the counter per-invocation from the input so different
 *     inputs land in DISJOINT id blocks, same input reproduces the same ids.
 *  2. A very long physical column name pushed the inode column id
 *     `inode-{22}/{PHYS}` past Sigma's 64-char id limit. Fix: clamp with a
 *     deterministic hash suffix, preserving the 22-char uniqueness segment.
 *
 * #107's multi-datasource CHILD id CONTINUITY must survive (children skip the
 * reset and continue the parent's counter) — guarded here and in
 * tableau.multidatasource.test.ts.
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { convertTableauToSigma } from './tableau.js';

const dsBlock = (caption: string, name: string, table: string, cols: Array<[string, string, string]>) => `
    <datasource caption='${caption}' name='${name}'>
      <connection class='federated'>
        <named-connections>
          <named-connection name='c' caption='c'><connection class='snowflake' dbname='CSA' schema='TJ'/></named-connection>
        </named-connections>
        <_.fcp.ObjectModelEncapsulateLegacy.true...relation type='collection'>
          <relation connection='c' name='${table}' table='[${table}]' type='table'/>
        </_.fcp.ObjectModelEncapsulateLegacy.true...relation>
        <metadata-records>
          ${cols.map(([rn, cap, lt]) => `<metadata-record class='column'><remote-name>${rn}</remote-name><caption>${cap}</caption><object-id>[${table}]</object-id><local-type>${lt}</local-type></metadata-record>`).join('\n          ')}
        </metadata-records>
      </connection>
    </datasource>`;

const ws = (name: string, ds: string) => `
  <worksheet name='${name}'>
    <table><view><datasources><datasource name='${ds}' caption='${name}'/></datasources></view></table>
  </worksheet>`;

const wb = (...parts: string[]) => `<?xml version='1.0' encoding='utf-8'?>\n<workbook>\n<datasources>\n${parts.join('\n')}\n</datasources>\n</workbook>`;

const OPTS = { connectionId: 'c1', database: 'CSA', schema: 'TJ' };

// Every string value under an id-bearing key (mirrors the corpus normalizer's
// ID_KEYS) — the set of ids the DM POST must find unique.
const ID_KEYS = new Set(['id', 'elementId', 'targetElementId', 'sourceColumnId', 'targetColumnId', 'columnId']);
function collectIds(model: any): string[] {
  const ids: string[] = [];
  const walk = (n: any): void => {
    if (Array.isArray(n)) { n.forEach(walk); return; }
    if (n && typeof n === 'object') {
      for (const [k, v] of Object.entries(n)) {
        if (ID_KEYS.has(k) && typeof v === 'string') ids.push(v);
        walk(v);
      }
    }
  };
  walk(model);
  return ids;
}

const wbA = wb(
  dsBlock('Sales Funnel', 'federated.sales', 'ORDER_FACT',
    [['ORDER_ID', 'Order Id', 'integer'], ['AMOUNT', 'Amount', 'real']]),
) + ws('Funnel', 'federated.sales');

const wbB = wb(
  dsBlock('SKU Master', 'federated.sku', 'CUSTOMER_DIM',
    [['CUSTOMER_ID', 'Customer Id', 'integer'], ['CUSTOMER_NAME', 'Customer Name', 'string']]),
) + ws('SKUs', 'federated.sku');

describe('per-invocation id-space scoping (W2.4)', () => {
  test('two INDEPENDENT top-level conversions get DISJOINT id-spaces (no cross-run collision)', () => {
    const a = new Set(collectIds(convertTableauToSigma(wbA, OPTS).model));
    const b = new Set(collectIds(convertTableauToSigma(wbB, OPTS).model));
    assert.ok(a.size > 0 && b.size > 0, 'both conversions minted ids');
    const shared = [...a].filter((id) => b.has(id));
    assert.deepEqual(shared, [], `id-spaces must be disjoint; shared: ${shared.slice(0, 5).join(', ')}`);
  });

  test('SAME workbook converted at different datasourceIndex → disjoint (independent multi-DS calls)', () => {
    const multi = wb(
      dsBlock('DS A', 'federated.a', 'ORDER_FACT', [['ORDER_ID', 'Order Id', 'integer']]),
      dsBlock('DS B', 'federated.b', 'CUSTOMER_DIM', [['CUSTOMER_ID', 'Customer Id', 'integer']]),
    ) + ws('A', 'federated.a') + ws('B', 'federated.b');
    const i0 = new Set(collectIds(convertTableauToSigma(multi, { ...OPTS, datasourceIndex: 0 }).model));
    const i1 = new Set(collectIds(convertTableauToSigma(multi, { ...OPTS, datasourceIndex: 1 }).model));
    const shared = [...i0].filter((id) => i1.has(id));
    // controls are workbook-global and legitimately identical across indices;
    // element/column/inode ids (the DM-POST uniqueness surface) must be disjoint.
    const sharedStructural = shared.filter((id) => id.startsWith('inode-') || /^[A-Za-z0-9]{10}$/.test(id));
    assert.deepEqual(sharedStructural, [],
      `element/column id-spaces must be disjoint across indices; shared: ${sharedStructural.slice(0, 5).join(', ')}`);
  });

  test('same input reproduces byte-identical ids (deterministic — goldens stay stable)', () => {
    const one = collectIds(convertTableauToSigma(wbA, OPTS).model);
    const two = collectIds(convertTableauToSigma(wbA, OPTS).model);
    assert.deepEqual(one, two, 'identical input → identical id sequence');
  });

  test('#107 continuity: a multi-datasource conversion still mints all-unique ids', () => {
    const multi = wb(
      dsBlock('DS A', 'federated.a', 'ORDER_FACT', [['ORDER_ID', 'Order Id', 'integer'], ['AMOUNT', 'Amount', 'real']]),
      dsBlock('DS B', 'federated.b', 'CUSTOMER_DIM', [['CUSTOMER_ID', 'Customer Id', 'integer'], ['CUSTOMER_NAME', 'Customer Name', 'string']]),
    ) + ws('A', 'federated.a') + ws('B', 'federated.b');
    const ids = collectIds(convertTableauToSigma(multi, OPTS).model);
    assert.equal(ids.length, new Set(ids).size, 'no duplicate id across the merged multi-DS model');
  });
});

describe('64-char id clamp (W2.4)', () => {
  const LONG = 'MONTHLY_AGGREGATED_REVENUE_BY_CUSTOMER_SEGMENT_AND_PRODUCT_CATEGORY_WITH_YOY_DELTA_AND_ROLLING_AVERAGE';
  const longWb = wb(
    dsBlock('DS', 'federated.long', 'ORDER_FACT',
      [['ORDER_ID', 'Order Id', 'integer'], [LONG, 'Long Metric', 'real']]),
  ) + ws('W', 'federated.long');

  test('every emitted id is <= 64 chars even with a very long column name', () => {
    const ids = collectIds(convertTableauToSigma(longWb, OPTS).model);
    const over = ids.filter((id) => id.length > 64);
    assert.deepEqual(over, [], `ids over 64 chars: ${over.map((i) => `${i.length}:${i}`).join(', ')}`);
  });

  test('the long name actually triggers a clamp (hash suffix present, prefix readable)', () => {
    const ids = collectIds(convertTableauToSigma(longWb, OPTS).model);
    const clamped = ids.find((id) => id.startsWith('inode-') && id.includes('~') && id.length === 64);
    assert.ok(clamped, `expected a clamped 64-char inode id; got ${ids.filter((i) => i.startsWith('inode-')).join(', ')}`);
    assert.match(clamped!, /^inode-[A-Za-z0-9]{22}\/MONTHLY_AGGREGATED/, 'human-readable inode + physical prefix retained');
  });

  test('clamped ids are unique AND stable across runs', () => {
    const a = collectIds(convertTableauToSigma(longWb, OPTS).model);
    const b = collectIds(convertTableauToSigma(longWb, OPTS).model);
    assert.deepEqual(a, b, 'clamp is deterministic (same input → same clamped ids)');
    assert.equal(a.length, new Set(a).size, 'clamped ids remain unique within the model');
  });
});
