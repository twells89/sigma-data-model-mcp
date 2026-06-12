/**
 * Cube converter regression tests.
 *
 * Guards the YAML brace-ref forms ({CUBE}.col, {CUBE.col}, {other.dim},
 * {measure}) alongside the JS template-literal forms (${CUBE}.col, …).
 * Real-world repro: cubedevinc/ecommerce_demo — every join uses the bare
 * YAML form and previously fell through every \$\{...\}-anchored regex,
 * yielding relationships without key mappings and {CUBE} leaks in formulas.
 */
import { test } from 'node:test';
import assert from 'node:assert';
import { convertCubeToSigma } from './cube.js';

const USERS_YML = `
cubes:
  - name: users
    sql_table: ECOMMERCE.USERS
    dimensions:
      - name: id
        sql: ID
        type: number
        primary_key: true
      - name: first_name
        sql: FIRST_NAME
        type: string
      - name: last_name
        sql: LAST_NAME
        type: string
      - name: full_name
        sql: "CONCAT({first_name}, ' ', {last_name})"
        type: string
    measures:
      - name: count
        type: count
`;

const EVENTS_YML = `
cubes:
  - name: events
    sql_table: ECOMMERCE.EVENTS
    joins:
      - name: users
        sql: "{CUBE}.USER_ID = {users.id}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: ID
        type: number
        primary_key: true
      - name: user_id
        sql: USER_ID
        type: number
      - name: location
        sql: |
          CONCAT({CUBE}.CITY, ', ', {CUBE}.STATE)
        type: string
    measures:
      - name: count
        type: count
`;

const ORDER_ITEMS_YML = `
cubes:
  - name: order_items
    sql_table: ECOMMERCE.ORDER_ITEMS
    joins:
      - name: users
        sql: "{CUBE.user_id} = {users.id}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: "{CUBE}.ID"
        type: number
        primary_key: true
      - name: user_id
        sql: "{CUBE}.USER_ID"
        type: number
`;

test('YAML brace join refs resolve key mappings ({CUBE}.col = {other.dim})', () => {
  const out = convertCubeToSigma(
    [
      { name: 'users.yml', content: USERS_YML },
      { name: 'events.yml', content: EVENTS_YML },
    ],
    {},
  );
  const events = out.model.pages[0].elements.find((e: any) => e.name === 'Events') as any;
  assert.ok(events, 'events element exists');
  assert.equal(events.relationships?.length, 1, 'one relationship');
  const rel = events.relationships[0];
  assert.ok(rel.keys?.length, `relationship has key mappings: ${JSON.stringify(rel)}`);
  assert.match(rel.keys[0].sourceColumnId, /USER_ID/i);
  assert.match(rel.keys[0].targetColumnId, /\/ID$/i);
  const warn = out.warnings.filter((w: string) => w.includes('could not resolve column keys'));
  assert.equal(warn.length, 0, `no unresolved-key warnings: ${warn}`);
});

test('YAML inline-dot join form resolves ({CUBE.dim} = {other.dim})', () => {
  const out = convertCubeToSigma(
    [
      { name: 'users.yml', content: USERS_YML },
      { name: 'order_items.yml', content: ORDER_ITEMS_YML },
    ],
    {},
  );
  const oi = out.model.pages[0].elements.find((e: any) => e.name === 'Order Items') as any;
  assert.ok(oi.relationships?.[0]?.keys?.length, 'inline-dot join resolved keys');
});

test('YAML brace refs in dimension SQL translate (no {CUBE} / {field} leaks)', () => {
  const out = convertCubeToSigma(
    [
      { name: 'users.yml', content: USERS_YML },
      { name: 'events.yml', content: EVENTS_YML },
    ],
    {},
  );
  for (const el of out.model.pages[0].elements as any[]) {
    for (const c of [...(el.columns ?? []), ...(el.metrics ?? [])]) {
      assert.doesNotMatch(
        c.formula ?? '',
        /\{\w+/,
        `no brace-ref leak in ${el.name}: ${c.formula}`,
      );
    }
  }
});

test('JS template-literal forms still work (${CUBE}.col = ${other.id})', () => {
  const js = `
cube('Sessions', {
  sql_table: 'WEB.SESSIONS',
  joins: {
    users: {
      sql: \`\${CUBE}.USER_ID = \${users.id}\`,
      relationship: 'many_to_one',
    },
  },
  dimensions: {
    id: { sql: 'ID', type: 'number', primaryKey: true },
    user_id: { sql: 'USER_ID', type: 'number' },
  },
});
`;
  const out = convertCubeToSigma(
    [
      { name: 'users.yml', content: USERS_YML },
      { name: 'sessions.js', content: js },
    ],
    {},
  );
  const sess = out.model.pages[0].elements.find((e: any) => /sessions/i.test(e.name)) as any;
  assert.ok(sess, 'sessions element exists');
  assert.ok(sess.relationships?.[0]?.keys?.length, 'JS-form join resolved keys');
});
