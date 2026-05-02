/**
 * Tableau Prep flow JSON → Sigma Data Model JSON converter.
 *
 * Accepts the unzipped `flow` JSON document from a .tfl/.tflx file. The flow is
 * a DAG of typed nodes (input, transform, container, superNode, output) connected
 * via nextNodes[] edges.
 *
 * v1 strategy:
 *   1. Recursively flatten .v1.Container.loomContainer.nodes into the parent graph
 *   2. Walk DAG from initialNodes (inputs):
 *      - Inputs → warehouse-table or Custom SQL element (Custom SQL for LoadSqlProxy
 *        / file inputs that can't be auto-mapped)
 *      - Process input.actions[] inline transforms before walking nextNodes
 *   3. Linear chain of action nodes → accumulated calc cols / column drops /
 *      renames / casts on the same element
 *   4. Branch nodes force a new element:
 *      - SuperJoin (.v1.SimpleJoin actionNode) → relationship FK/PK from
 *        conditions[].leftExpression/rightExpression
 *      - SuperUnion (.v1.SimpleUnion actionNode) → element with source.kind:'union'
 *      - SuperAggregate (.v1.Aggregate actionNode) → child element with
 *        groupings + metrics
 *   5. Output nodes (WriteToHyper, etc.) → ignored
 *
 * Skipped with warnings: Pivot, Script/RunCommand/Prediction.
 */

import { XMLParser } from 'fast-xml-parser';
import {
  resetIds, sigmaShortId, sigmaInodeId, sigmaDisplayName,
  sigmaColFormula,
  type SigmaElement, type ConversionResult,
} from './sigma-ids.js';
import { tableauFormulaToSigma } from './formulas.js';

const tdsXmlParser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '@_',
  isArray: (name) => ['datasource', 'connection', 'named-connection', 'relation', 'column', 'metadata-record'].includes(name),
  trimValues: true,
});

function tdsAttr(node: any, key: string): string {
  return (node && node[`@_${key}`]) || '';
}

function tdsAsArray(val: any): any[] {
  if (!val) return [];
  return Array.isArray(val) ? val : [val];
}

/**
 * Parse a Tableau .tds/.tdsx XML file and return an index of
 * datasourceCaption/datasourceName → resolved connection info.
 */
interface TdsResolved {
  caption: string;
  name: string;
  /** When present, use as a warehouse-table source (path = [db, schema, table]) */
  table?: { db: string; schema: string; table: string };
  /** When present, use as a Custom SQL element with this SELECT body */
  customSql?: string;
  /** Connection class for warehouse path qualification (e.g. 'snowflake', 'bigquery') */
  connectionClass?: string;
  /** Field schema for column synthesis when fields[] on the .tfl is incomplete */
  fields?: { name: string; type?: string; caption?: string }[];
}

function parseTdsFile(xmlText: string): TdsResolved[] {
  let parsed: any;
  try { parsed = tdsXmlParser.parse(xmlText); } catch { return []; }
  const datasources = tdsAsArray(parsed?.datasource || parsed?.workbook?.datasources?.datasource || []);
  const out: TdsResolved[] = [];
  for (const ds of datasources) {
    const caption = tdsAttr(ds, 'caption') || tdsAttr(ds, 'name') || '';
    const name = tdsAttr(ds, 'name') || caption;
    const r: TdsResolved = { caption, name };

    // Pull connection — could be top-level <connection> or nested under <connection class='federated'>
    let conn: any = ds.connection;
    if (Array.isArray(conn)) conn = conn[0];
    if (conn) {
      const cclass = tdsAttr(conn, 'class');
      r.connectionClass = cclass;
      // Federated wraps named-connections
      if (cclass === 'federated') {
        const named = tdsAsArray(conn['named-connections']?.['named-connection'] || [])[0];
        const inner = named?.connection;
        if (inner) {
          r.connectionClass = tdsAttr(inner, 'class');
        }
      }
    }

    // Find a <relation> — could be a child of connection
    const relations = tdsAsArray(conn?.relation || []);
    if (relations.length > 0) {
      const rel = relations[0];
      const relType = tdsAttr(rel, 'type');
      const relTable = tdsAttr(rel, 'table');
      const relText = (typeof rel === 'string' ? rel : (rel['#text'] || rel._text || ''));

      if (relType === 'text' && relText) {
        r.customSql = String(relText).trim();
      } else if (relTable) {
        // table='[CSA].[TJ].[PRODUCT_DIM]' or similar
        const parts = relTable.replace(/[\[\]]/g, '').split('.').map((s: string) => s.trim()).filter(Boolean);
        if (parts.length >= 1) {
          const table = parts[parts.length - 1];
          const schema = parts.length >= 2 ? parts[parts.length - 2] : '';
          const db = parts.length >= 3 ? parts[parts.length - 3] : '';
          r.table = { db, schema, table };
        }
      }
    }

    // Pull field schema from metadata-records (helps if .tfl fields[] is incomplete)
    const metaRecords = tdsAsArray(conn?.['metadata-records']?.['metadata-record'] || []);
    if (metaRecords.length > 0) {
      r.fields = [];
      for (const mr of metaRecords) {
        if (tdsAttr(mr, 'class') !== 'column') continue;
        const remoteName = (mr['remote-name'] || '').toString().trim();
        const localType = (mr['local-type'] || 'string').toString().trim();
        const localName = (mr['local-name'] || '').toString().trim().replace(/^\[|\]$/g, '');
        if (!remoteName) continue;
        r.fields.push({ name: remoteName, type: localType, caption: localName });
      }
    }

    if (r.caption || r.name) out.push(r);
  }
  return out;
}

// ── Public interface ─────────────────────────────────────────────────────────

export interface TableauPrepConvertOptions {
  connectionId?: string;
  database?: string;
  schema?: string;
  /** Map Prep input names (CSV basename or LoadSqlProxy datasource name) → warehouse table name */
  tableMapping?: Record<string, string>;
  /** Optional companion .tds/.tdsx XML files. When a .tds caption matches a
   * LoadSqlProxy.datasourceName, the placeholder Custom SQL stub is replaced with
   * the actual relation (warehouse-table for type='table', or Custom SQL with the
   * real SELECT body for type='text'). */
  tdsFiles?: { name: string; content: string }[];
}

export function convertTableauPrepToSigma(
  flowJsonText: string,
  options: TableauPrepConvertOptions = {},
): ConversionResult {
  resetIds();

  const { connectionId = '<CONNECTION_ID>', database = '', schema = '', tableMapping = {}, tdsFiles = [] } = options;
  const dbOverride = (database || '').trim().toUpperCase();
  const schemaOverride = (schema || '').trim().toUpperCase();
  const conn = connectionId || '<CONNECTION_ID>';

  // ── Parse companion .tds files into a caption/name → resolved-connection index ──
  const tdsIndex = new Map<string, TdsResolved>();
  for (const f of tdsFiles) {
    for (const r of parseTdsFile(f.content)) {
      if (r.caption) tdsIndex.set(r.caption.toLowerCase(), r);
      if (r.name && !tdsIndex.has(r.name.toLowerCase())) tdsIndex.set(r.name.toLowerCase(), r);
    }
  }

  // ── Parse flow JSON ──────────────────────────────────────────────────────

  let flow: PrepFlow;
  try {
    flow = JSON.parse(flowJsonText);
  } catch (e: any) {
    throw new Error('Tableau Prep flow JSON parse error: ' + e.message);
  }
  if (!flow.nodes || typeof flow.nodes !== 'object') {
    throw new Error('Invalid Tableau Prep flow: missing nodes object');
  }

  const warnings: string[] = [];
  const stats = {
    nodes: 0, inputs: 0, transforms: 0, joins: 0, unions: 0, aggregates: 0, outputs: 0,
    skipped: 0, calculatedColumns: 0, filters: 0, renames: 0, removedColumns: 0,
    elements: 0, relationships: 0,
  };

  // ── Step 1: flatten containers ───────────────────────────────────────────

  const flatNodes: Record<string, PrepNode> = {};
  flattenContainers(flow.nodes, flatNodes, warnings);

  // ── Step 2: build incoming-edge map ──────────────────────────────────────

  // For each node id, the set of (predecessor id, edge metadata)
  const incoming = new Map<string, Array<{ from: string; namespace: string; nextNamespace: string }>>();
  for (const [nid, n] of Object.entries(flatNodes)) {
    for (const ne of (n.nextNodes || [])) {
      if (!incoming.has(ne.nextNodeId)) incoming.set(ne.nextNodeId, []);
      incoming.get(ne.nextNodeId)!.push({
        from: nid,
        namespace: ne.namespace || 'Default',
        nextNamespace: ne.nextNamespace || 'Default',
      });
    }
  }

  // ── Step 3: walk DAG, process each node, emit Sigma elements ─────────────

  const elements: SigmaElement[] = [];
  // Map node id → ChainState (the in-progress element built up from this node)
  const chainState = new Map<string, ChainState>();
  const visited = new Set<string>();
  const queue: string[] = [];

  // Seed with initial (input) nodes
  for (const id of (flow.initialNodes || [])) {
    if (flatNodes[id]) queue.push(id);
  }
  // Also include any input nodes not in initialNodes (defensive)
  for (const [nid, n] of Object.entries(flatNodes)) {
    if (n.baseType === 'input' && !queue.includes(nid)) queue.push(nid);
  }

  while (queue.length > 0) {
    const nid = queue.shift()!;
    if (visited.has(nid)) continue;
    const n = flatNodes[nid];
    if (!n) continue;
    stats.nodes++;

    // Don't process a non-input node until all its predecessors have been visited
    const preds = incoming.get(nid) || [];
    if (n.baseType !== 'input' && !preds.every(p => visited.has(p.from))) {
      // requeue at the back
      queue.push(nid);
      continue;
    }
    visited.add(nid);

    if (n.baseType === 'input') {
      const state = processInputNode(n, conn, dbOverride, schemaOverride, tableMapping, tdsIndex, warnings);
      stats.inputs++;
      // Process inline input.actions[] before publishing the chain
      for (const action of (n.actions || [])) {
        applyTransformAction(action, state, warnings, stats);
      }
      chainState.set(nid, state);
      // Continue downstream
      for (const ne of (n.nextNodes || [])) queue.push(ne.nextNodeId);

    } else if (n.baseType === 'transform' || n.baseType === 'output') {
      // Linear transform — inherit upstream chain (if exactly one predecessor)
      const upstream = preds[0];
      if (!upstream) {
        warnings.push(`⚠ Transform node "${n.name || nid}" has no predecessor — skipped.`);
        continue;
      }
      const state = chainState.get(upstream.from);
      if (!state) {
        warnings.push(`⚠ Transform node "${n.name || nid}" upstream chain missing — skipped.`);
        continue;
      }

      if (n.baseType === 'output') {
        stats.outputs++;
        // Finalize the chain into an element if not already finalized
        if (!state.finalized) {
          finalizeChain(state, elements);
          stats.elements++;
        }
        continue;
      }

      // Transform: try inline action handling
      applyTransformAction(n, state, warnings, stats);
      chainState.set(nid, state);
      for (const ne of (n.nextNodes || [])) queue.push(ne.nextNodeId);

    } else if (n.baseType === 'superNode') {
      const nodeType = n.nodeType || '';
      if (nodeType.includes('SuperJoin')) {
        const state = processSuperJoin(n, preds, chainState, elements, warnings);
        stats.joins++;
        if (state) {
          chainState.set(nid, state);
          stats.relationships++;
        }
      } else if (nodeType.includes('SuperUnion')) {
        const state = processSuperUnion(n, preds, chainState, elements, conn, warnings);
        stats.unions++;
        if (state) chainState.set(nid, state);
      } else if (nodeType.includes('SuperAggregate')) {
        const state = processSuperAggregate(n, preds, chainState, elements, warnings);
        stats.aggregates++;
        if (state) chainState.set(nid, state);
      } else {
        warnings.push(`ℹ SuperNode type "${nodeType}" not recognized — skipped.`);
        stats.skipped++;
      }
      for (const ne of (n.nextNodes || [])) queue.push(ne.nextNodeId);

    } else {
      // container nodes shouldn't appear post-flatten, but defensive
      stats.skipped++;
      warnings.push(`ℹ Node "${n.name || nid}" (${n.nodeType}, baseType=${n.baseType}) — skipped.`);
      for (const ne of (n.nextNodes || [])) queue.push(ne.nextNodeId);
    }
  }

  // Finalize any unfinalized chains. SQL-element stubs may legitimately have zero
  // declared columns (Sigma auto-discovers from the SQL); keep them anyway.
  for (const state of chainState.values()) {
    if (state.finalized) continue;
    if (state.element.columns.length === 0 && (state.element.source as any)?.kind !== 'sql') continue;
    finalizeChain(state, elements);
    stats.elements++;
  }

  // Compute final stats
  stats.elements = elements.length;
  stats.relationships = elements.reduce((s, e) => s + (e.relationships?.length || 0), 0);

  if (!options.connectionId) warnings.unshift('⚠ Connection ID not set — update in JSON before saving to Sigma');

  const modelName = elements.length === 1
    ? elements[0].name || 'Tableau Prep Flow'
    : 'Tableau Prep Flow';

  return {
    model: {
      name: modelName,
      pages: [{ id: sigmaShortId(), name: 'Page 1', elements }],
    },
    warnings,
    stats: {
      nodes: stats.nodes,
      inputs: stats.inputs,
      joins: stats.joins,
      unions: stats.unions,
      aggregates: stats.aggregates,
      outputs: stats.outputs,
      skipped: stats.skipped,
      elements: stats.elements,
      columns: elements.reduce((s, e) => s + (e.columns?.length ?? 0), 0),
      metrics: elements.reduce((s, e) => s + (e.metrics?.length ?? 0), 0),
      relationships: stats.relationships,
      calculatedColumns: stats.calculatedColumns,
      filters: stats.filters,
      renames: stats.renames,
      removedColumns: stats.removedColumns,
    },
  };
}

// ── Container flatten ────────────────────────────────────────────────────────

/**
 * Inline .v1.Container.loomContainer.nodes into the parent graph. For a container
 * with one outgoing edge and a chain of inner action nodes, this means:
 *   - inner nodes become first-class siblings of the container
 *   - the container's incoming edge is rewired to the first inner node
 *   - the container's outgoing nextNodes are rewired to the last inner node
 *
 * For our v1 we do a simpler pass: copy all inner nodes to the flat map and
 * rewire edges so that any edge into the container instead points to the first
 * inner node (the container's loomContainer.initialNodes[0] if present, else the
 * inner node with no incoming edge), and any edge out of the container is
 * sourced from the inner node whose nextNodes[] is empty.
 */
function flattenContainers(
  rootNodes: Record<string, PrepNode>,
  outFlat: Record<string, PrepNode>,
  warnings: string[],
): void {
  // First pass — copy non-container nodes; recursively process containers
  const containerEdgeRewrites: Array<{
    containerId: string;
    firstInnerId: string | null;
    lastInnerIds: string[]; // every inner node with no nextNodes[] (terminal in container)
    outgoing: NextNode[];
  }> = [];

  function pass(nodes: Record<string, PrepNode>) {
    for (const [nid, n] of Object.entries(nodes)) {
      if (n.nodeType === '.v1.Container' && n.loomContainer) {
        const inner = n.loomContainer.nodes || {};
        // Recurse into inner first
        pass(inner);
        // Determine first inner node — initialNodes if provided, else any node not present in nextNodes targets
        const innerIds = Object.keys(inner);
        const targeted = new Set<string>();
        for (const inN of Object.values(inner)) {
          for (const ne of inN.nextNodes || []) targeted.add(ne.nextNodeId);
        }
        let firstInnerId: string | null = (n.loomContainer.initialNodes || [])[0] || null;
        if (!firstInnerId) {
          firstInnerId = innerIds.find(id => !targeted.has(id)) || innerIds[0] || null;
        }
        const lastInnerIds = innerIds.filter(id => !(inner[id].nextNodes || []).length);
        containerEdgeRewrites.push({
          containerId: nid,
          firstInnerId,
          lastInnerIds,
          outgoing: n.nextNodes || [],
        });
      } else {
        if (!outFlat[nid]) outFlat[nid] = n;
      }
    }
  }
  pass(rootNodes);

  // Apply edge rewrites:
  //   - any node pointing to a container should now point to firstInnerId
  //   - container's outgoing edges should originate from each lastInnerId
  for (const rw of containerEdgeRewrites) {
    if (!rw.firstInnerId && !rw.lastInnerIds.length) {
      warnings.push(`ℹ Container "${rw.containerId}" was empty — dropped.`);
      continue;
    }
    // Rewrite incoming edges
    for (const n of Object.values(outFlat)) {
      if (!n.nextNodes) continue;
      for (const ne of n.nextNodes) {
        if (ne.nextNodeId === rw.containerId && rw.firstInnerId) {
          ne.nextNodeId = rw.firstInnerId;
        }
      }
    }
    // Append the container's outgoing nextNodes onto each lastInnerId
    for (const lastId of rw.lastInnerIds) {
      const lastN = outFlat[lastId];
      if (!lastN) continue;
      lastN.nextNodes = [...(lastN.nextNodes || []), ...rw.outgoing];
    }
  }
}

// ── Input node processing ────────────────────────────────────────────────────

interface ChainState {
  element: SigmaElement;
  /** Display-name → column id, for column lookup during renames/removals */
  colByName: Map<string, string>;
  /** Track upstream node id chains — used by SuperJoin to identify left/right sides */
  upstreamChainIds: string[];
  /** sourceTable name used in [TABLE/Display] formulas; 'Custom SQL' for SQL elements */
  sourceTable: string;
  isCustomSql: boolean;
  finalized: boolean;
  /** Human-friendly name for use in relationship names etc. — survives even when
   * element.name is omitted (Custom SQL elements). Falls back to sourceTable. */
  displayableName: string;
}

function processInputNode(
  n: PrepNode,
  connectionId: string,
  dbOverride: string,
  schemaOverride: string,
  tableMapping: Record<string, string>,
  tdsIndex: Map<string, TdsResolved>,
  warnings: string[],
): ChainState {
  const elementId = sigmaShortId();
  const nodeType = n.nodeType || '';
  const inputName = (n.name || 'Input').trim();

  let element: SigmaElement;
  let sourceTable: string;
  let isCustomSql = false;

  if (nodeType === '.v2019_3_1.LoadSqlProxy' || nodeType === 'LoadPublishedDataSource') {
    // Look up a companion .tds by datasourceName / inputName.
    const dsLookupKey = (n.connectionAttributes?.datasourceName || inputName || '').toLowerCase();
    const tds = tdsIndex.get(dsLookupKey) || tdsIndex.get(inputName.toLowerCase());

    if (tds && tds.table) {
      // Resolved to a warehouse table — emit a real warehouse-table element instead of a stub.
      const tableName = tds.table.table.toUpperCase();
      let path: string[] = [];
      const tdsDb = (tds.table.db || dbOverride).toUpperCase();
      const tdsSchema = (tds.table.schema || schemaOverride).toUpperCase();
      if (tdsDb && tdsSchema) path = [tdsDb, tdsSchema, tableName];
      else if (tdsSchema) path = [tdsSchema, tableName];
      else if (tdsDb) path = [tdsDb, tableName];
      else path = [tableName];
      sourceTable = tableName;
      element = {
        id: elementId,
        kind: 'table',
        name: tableName,
        source: { connectionId, kind: 'warehouse-table', path },
        columns: [],
        metrics: [],
        order: [],
      };
      warnings.push(`✓ "${inputName}" resolved via companion .tds → warehouse-table ${path.join('.')}`);
      // Fall through to add columns from n.fields[]
    } else if (tds && tds.customSql) {
      // Resolved to Custom SQL with a real SELECT body
      isCustomSql = true;
      sourceTable = 'Custom SQL';
      element = {
        id: elementId,
        kind: 'table',
        source: { connectionId, kind: 'sql', statement: tds.customSql },
        columns: [],
        metrics: [],
        order: [],
      } as SigmaElement;
      warnings.push(`✓ "${inputName}" resolved via companion .tds → Custom SQL with real SELECT body`);
      // Fall through to add columns from n.fields[]
    } else {
      // No .tds match — fall back to v1 placeholder stub behavior
    // Tableau Server published datasource — emit Custom SQL placeholder. The SQL
    // declares each declared field as a typed NULL column so the element's column
    // refs ([Field]) resolve until the user replaces the SELECT body.
    isCustomSql = true;
    sourceTable = 'Custom SQL';
    const fields = (n.fields || []).filter(f => f.name);
    // Use the column's display name in the SQL alias too — keeps `AS "Key"` aligned
    // with the formula's `[Key]` ref so Sigma's resolver can match them.
    const sqlCols = fields.length > 0
      ? fields.map(f => {
          const dn = (f.caption || sigmaDisplayName(f.name)).replace(/"/g, '""');
          return `CAST(NULL AS ${prepFieldTypeToSql(f.type)}) AS "${dn}"`;
        }).join(',\n  ')
      : 'NULL AS "Placeholder"';
    // Custom SQL elements: omit `name` field entirely (matches production convention).
    // The element shows in Sigma UI as "Custom SQL".
    element = {
      id: elementId,
      kind: 'table',
      source: {
        connectionId,
        kind: 'sql',
        statement: `-- ${inputName} (Tableau Server datasource: ${n.connectionAttributes?.datasourceName || inputName})\n-- TODO: replace with the equivalent warehouse query\nSELECT\n  ${sqlCols}`,
      },
      columns: [],
      metrics: [],
      order: [],
    } as SigmaElement;
      warnings.push(`ℹ "${inputName}" is a LoadSqlProxy (Tableau Server datasource) — emitted as Custom SQL placeholder. Replace the SELECT with the equivalent warehouse query, or pass a companion .tds file via tdsFiles to auto-resolve.`);
    }
  } else if (nodeType.startsWith('.v1.Load')) {
    // File or warehouse table input. Determine table name + path.
    let tableName = '';
    if (nodeType === '.v1.LoadSql') {
      // SQL connection — connectionAttributes may carry table info
      tableName = String(n.connectionAttributes?.table || n.connectionAttributes?.['table-name'] || inputName).toUpperCase();
    } else {
      // File input — pull from filename basename if available
      const fname = String(n.connectionAttributes?.filename || inputName);
      const base = fname.split(/[\\/]/).pop()!.replace(/\.[^.]+$/, '');
      tableName = base.toUpperCase().replace(/[^A-Z0-9_]/g, '_');
    }

    // Apply tableMapping override
    const mapped = tableMapping[tableName] || tableMapping[inputName] || tableName;
    tableName = mapped.toUpperCase();

    let path: string[] = [];
    if (dbOverride && schemaOverride) path = [dbOverride, schemaOverride, tableName];
    else if (schemaOverride) path = [schemaOverride, tableName];
    else if (dbOverride) path = [dbOverride, tableName];
    else path = [tableName];

    sourceTable = tableName;
    element = {
      id: elementId,
      kind: 'table',
      // Use the warehouse table name (uppercase) as the element name. This matches the
      // convention in production data models — necessary for cross-element refs of the
      // form [ELEMENT_NAME/RelName/Col] to resolve, since the resolver matches the first
      // segment against the element's name field.
      name: tableName,
      source: { connectionId, kind: 'warehouse-table', path },
      columns: [],
      metrics: [],
      order: [],
    };
    if (nodeType !== '.v1.LoadSql') {
      warnings.push(`ℹ "${inputName}" is a ${nodeType} file input — mapped to warehouse table "${tableName}". Use tableMapping to override.`);
    }
  } else {
    // Unknown input type — fall back to Custom SQL
    isCustomSql = true;
    sourceTable = 'Custom SQL';
    element = {
      id: elementId,
      kind: 'table',
      source: {
        connectionId,
        kind: 'sql',
        statement: `-- ${inputName} (unrecognized Tableau Prep input type ${nodeType})\n-- TODO: replace with the equivalent warehouse query\nSELECT NULL AS "Placeholder"`,
      },
      columns: [],
      metrics: [],
      order: [],
    } as SigmaElement;
    warnings.push(`⚠ "${inputName}" has unrecognized input type ${nodeType} — emitted as Custom SQL placeholder.`);
  }

  // Add columns from input.fields[].
  //
  // For Custom SQL elements use `[Custom SQL/<col>]` formula form (matches production
  // data model convention). The `<col>` matches the SQL output column alias. For
  // warehouse-table elements use the standard `[TABLE/Display]` form.
  const colByName = new Map<string, string>();
  for (const f of (n.fields || [])) {
    if (!f.name) continue;
    const dispName = (f.caption || sigmaDisplayName(f.name));
    const upper = f.name.toUpperCase();
    const id = isCustomSql ? sigmaShortId() : sigmaInodeId(upper);
    const formula = isCustomSql
      ? `[Custom SQL/${dispName}]`
      : sigmaColFormula(sourceTable, f.name);
    const col: any = { id, formula };
    // Custom SQL elements: leave name unset (per convention). Warehouse-table elements:
    // set name only if the field has an explicit caption.
    if (!isCustomSql && f.caption) col.name = f.caption;
    element.columns.push(col);
    element.order.push(id);
    colByName.set(dispName, id);
    colByName.set(f.name, id);
  }

  return {
    element, colByName,
    upstreamChainIds: [n.id],
    sourceTable, isCustomSql,
    finalized: false,
    displayableName: sigmaDisplayName(inputName),
  };
}

// ── Transform action handlers ────────────────────────────────────────────────

function applyTransformAction(
  n: PrepNode,
  state: ChainState,
  warnings: string[],
  stats: { calculatedColumns: number; filters: number; renames: number; removedColumns: number; transforms: number; skipped: number },
): void {
  const t = n.nodeType || '';
  stats.transforms++;
  if (t === '.v1.AddColumn') {
    handleAddColumn(n, state, warnings);
    stats.calculatedColumns++;
  } else if (t === '.v1.RemoveColumns') {
    handleRemoveColumns(n, state);
    stats.removedColumns += (n.columnNames?.length || 0);
  } else if (t === '.v1.RenameColumn') {
    handleRenameColumn(n, state);
    stats.renames++;
  } else if (t.endsWith('.Remap')) {
    handleRemap(n, state, warnings);
    stats.calculatedColumns++;
  } else if (t === '.v1.FilterOperation') {
    handleFilter(n, state, warnings);
    stats.filters++;
  } else if (t === '.v1.ChangeColumnType') {
    handleChangeType(n, state, warnings);
  } else if (t === '.v1.KeepOnlyColumns' || t === '.v1.RestrictColumns') {
    handleKeepOnly(n, state);
  } else if (t === '.v1.Pivot' || t.toLowerCase().includes('pivot')) {
    warnings.push(`ℹ Pivot step "${n.name}" — not converted, no Sigma equivalent. Add manually.`);
    stats.skipped++;
  } else if (t === '.v1.Script' || t === '.v1.RunScript' || t === '.v1.RunCommand' || t === '.v1.Prediction') {
    warnings.push(`⚠ Script/Run step "${n.name}" (${t}) skipped — no Sigma equivalent.`);
    stats.skipped++;
  } else {
    warnings.push(`ℹ Transform "${n.name}" (${t}) not recognized — skipped.`);
    stats.skipped++;
  }
}

function handleAddColumn(n: PrepNode, state: ChainState, warnings: string[]): void {
  const newColName = n.columnName || 'New Column';
  const expr = n.expression || '';
  const localWarnings: string[] = [];
  const formula = tableauFormulaToSigma(expr, localWarnings) || `/* ${expr} */`;
  // Tableau Prep formulas use [Field Name] refs that are already display-style.
  // For warehouse-table elements, leave them as-is (Sigma resolves [Display] within the element).
  const id = sigmaShortId();
  state.element.columns.push({ id, formula, name: newColName });
  state.element.order.push(id);
  state.colByName.set(newColName, id);
  for (const w of localWarnings) warnings.push(`AddColumn "${newColName}": ${w}`);
}

function handleRemoveColumns(n: PrepNode, state: ChainState): void {
  // Resolve each name via colByName (handles both SNAKE_CASE and Display Name keys)
  const dropIds = new Set<string>();
  const dropNames = new Set<string>();
  for (const cn of n.columnNames || []) {
    const id = state.colByName.get(cn) || state.colByName.get(sigmaDisplayName(cn));
    if (id) {
      dropIds.add(id);
      dropNames.add(cn);
      dropNames.add(sigmaDisplayName(cn));
    }
  }
  // If any other calc col still references a column we're about to drop, keep that
  // column as a hidden passthrough — Sigma's data model evaluates formulas lazily so
  // dropping a referenced column would break the dependent formula.
  const stillReferenced = new Set<string>();
  for (const c of state.element.columns) {
    if (dropIds.has(c.id)) continue;
    for (const m of (c.formula || '').matchAll(/\[([^/\]]+?)\]/g)) {
      const ref = m[1];
      if (dropNames.has(ref)) stillReferenced.add(ref);
    }
  }
  if (stillReferenced.size > 0) {
    // Mark referenced columns as hidden but keep them
    const keepIds = new Set<string>();
    for (const c of state.element.columns) {
      if (!dropIds.has(c.id)) continue;
      const dn = c.name || extractDisplayFromFormula(c.formula);
      if (stillReferenced.has(dn) || stillReferenced.has(sigmaDisplayName(dn))) {
        (c as any).hidden = true;
        keepIds.add(c.id);
      }
    }
    for (const id of keepIds) dropIds.delete(id);
  }
  state.element.columns = state.element.columns.filter(c => !dropIds.has(c.id));
  state.element.order = state.element.order.filter(id => !dropIds.has(id));
  for (const cn of (n.columnNames || [])) {
    const id = state.colByName.get(cn);
    if (id && dropIds.has(id)) state.colByName.delete(cn);
    const id2 = state.colByName.get(sigmaDisplayName(cn));
    if (id2 && dropIds.has(id2)) state.colByName.delete(sigmaDisplayName(cn));
  }
}

function handleRenameColumn(n: PrepNode, state: ChainState): void {
  const oldName = n.columnName || '';
  const newName = n.rename || oldName;
  if (!oldName || !newName) return;
  // Find the column id via colByName (which has both SNAKE_CASE and Display Name keys)
  const targetId = state.colByName.get(oldName)
    || state.colByName.get(sigmaDisplayName(oldName));
  if (!targetId) return;
  for (const c of state.element.columns) {
    if (c.id !== targetId) continue;
    c.name = newName;
    state.colByName.set(newName, c.id);
    state.colByName.delete(oldName);
    state.colByName.delete(sigmaDisplayName(oldName));
    return;
  }
}

function handleChangeType(n: PrepNode, state: ChainState, warnings: string[]): void {
  const colName = n.columnName || '';
  const newType = (n.newType || '').toLowerCase();
  if (!colName) return;
  const wrap: Record<string, string> = {
    string: 'Text', text: 'Text',
    integer: 'Int', int: 'Int',
    double: 'Number', float: 'Number', decimal: 'Number', number: 'Number',
    date: 'Date', datetime: 'Datetime', boolean: 'Boolean',
  };
  const fn = wrap[newType];
  if (!fn) {
    warnings.push(`ChangeColumnType "${colName}" → ${newType}: unknown type, skipped.`);
    return;
  }
  const targetId = state.colByName.get(colName) || state.colByName.get(sigmaDisplayName(colName));
  for (const c of state.element.columns) {
    if (c.id !== targetId) continue;
    c.formula = `${fn}(${c.formula})`;
    return;
  }
}

function handleRemap(n: PrepNode, state: ChainState, _warnings: string[]): void {
  const colName = n.columnName || '';
  const values = n.values || {};
  if (!colName || !Object.keys(values).length) return;
  const targetId = state.colByName.get(colName) || state.colByName.get(sigmaDisplayName(colName));
  const targetCol = state.element.columns.find(c => c.id === targetId);
  if (!targetCol) return;
  // Use the column's CURRENT formula as the value base (e.g. [ORDER_FACT/Order Status]).
  // Using [displayName] would create a self-reference Sigma can't resolve.
  const baseRef = targetCol.formula;
  const stripQ = (s: string) => s.replace(/^"(.*)"$/, '$1');
  let formula = baseRef;
  for (const [newValRaw, oldVals] of Object.entries(values)) {
    const newVal = stripQ(newValRaw);
    const oldArr = (oldVals as string[]).map(stripQ);
    if (oldArr.length === 1) {
      formula = `If(${baseRef} = "${oldArr[0]}", "${newVal}", ${formula})`;
    } else {
      const inList = oldArr.map(v => `"${v}"`).join(', ');
      formula = `If(In(${baseRef}, ${inList}), "${newVal}", ${formula})`;
    }
  }
  targetCol.formula = formula;
  // Preserve display name so downstream refs to [Order Status] continue to resolve.
  if (!targetCol.name) {
    targetCol.name = sigmaDisplayName(colName);
  }
}

function handleFilter(n: PrepNode, state: ChainState, warnings: string[]): void {
  const expr = n.filterExpression || '';
  if (!expr) return;
  const sigma = tableauFormulaToSigma(expr, warnings) || expr;
  const id = sigmaShortId();
  state.element.columns.push({
    id,
    formula: sigma,
    name: `Filter: ${(n.name || 'Filter').replace(/^Filter\s*/i, '').trim() || expr.slice(0, 40)}`,
  });
  state.element.order.push(id);
  warnings.push(`ℹ Filter "${n.name}" emitted as a calculated boolean column "${state.element.columns[state.element.columns.length - 1].name}" — wire as a page filter in Sigma.`);
}

function handleKeepOnly(n: PrepNode, state: ChainState): void {
  const keepIds = new Set<string>();
  for (const cn of n.columnNames || []) {
    const id = state.colByName.get(cn) || state.colByName.get(sigmaDisplayName(cn));
    if (id) keepIds.add(id);
  }
  state.element.columns = state.element.columns.filter(c => keepIds.has(c.id));
  state.element.order = state.element.order.filter(id => keepIds.has(id));
}

/** Map Tableau Prep field types to ANSI SQL types for placeholder CAST() expressions. */
function prepFieldTypeToSql(t: string | undefined): string {
  switch ((t || '').toLowerCase()) {
    case 'string': return 'VARCHAR';
    case 'integer': case 'int': return 'INTEGER';
    case 'number': case 'double': case 'float': case 'decimal': return 'DOUBLE';
    case 'date': return 'DATE';
    case 'datetime': case 'timestamp': return 'TIMESTAMP';
    case 'boolean': case 'bool': return 'BOOLEAN';
    default: return 'VARCHAR';
  }
}

function extractDisplayFromFormula(formula: string): string {
  // Match "[TABLE/Display]" → Display, or "[Display]" → Display, or "[X/Y/Display]" → Display
  const m = formula.match(/^\[(?:.+\/)?([^\]]+)\]$/);
  return m ? m[1] : '';
}

/**
 * Inline a calc-column formula's element-internal `[Display]` refs by recursively
 * substituting them with their upstream column's formula. This converts
 * `[Gross Profit] - [Discount Amount]` (calc col on Order Fact) → `[ORDER_FACT/Gross Profit] - [ORDER_FACT/Discount Amount]`
 * (warehouse-table refs that resolve from any descendant element).
 *
 * Refs that already have a `/` (warehouse-table refs or cross-element refs) are
 * left alone. Refs that match a column on `sourceElement` get inlined.
 */
function inlineCalcColFormula(formula: string, sourceElement: SigmaElement, depth = 0): string {
  if (depth > 5) return formula;
  let result = formula;
  const refPattern = /\[([^/\]]+?)\]/g;
  let changed = false;
  result = result.replace(refPattern, (match, name) => {
    const col = sourceElement.columns.find(c => (c.name || extractDisplayFromFormula(c.formula)) === name);
    if (!col) return match;
    // If the matched column's formula is itself a simple [TABLE/Col] warehouse passthrough, use it
    if (/^\[[^/\]]+\/[^\]]+\]$/.test(col.formula.trim())) {
      changed = true;
      return col.formula;
    }
    // Otherwise inline its formula recursively
    changed = true;
    return '(' + inlineCalcColFormula(col.formula, sourceElement, depth + 1) + ')';
  });
  return changed ? result : formula;
}

/**
 * Find the canonical display name for a logical ref. logicalRefs may have multiple
 * keys mapping to the same formula (display name + SNAKE_CASE alias). Return the
 * one that looks like a display name (has spaces or mixed case).
 */
function logicalRefsName(logicalRefs: Map<string, string>, candidate: string): string | null {
  const ref = logicalRefs.get(candidate) || logicalRefs.get(sigmaDisplayName(candidate));
  if (!ref) return null;
  // Find the prettiest key that maps to this ref
  let pretty: string | null = null;
  for (const [k, v] of logicalRefs) {
    if (v !== ref) continue;
    if (!pretty || (k.includes(' ') || (/[a-z]/.test(k) && /[A-Z]/.test(k)))) pretty = k;
  }
  return pretty;
}

function finalizeChain(state: ChainState, elements: SigmaElement[]): void {
  if (state.finalized) return;
  state.finalized = true;
  if (state.element.metrics?.length === 0) delete state.element.metrics;
  elements.push(state.element);
}

// ── Branch nodes (Join / Union / Aggregate) ──────────────────────────────────

function processSuperJoin(
  n: PrepNode,
  preds: Array<{ from: string; namespace: string; nextNamespace: string }>,
  chainState: Map<string, ChainState>,
  elements: SigmaElement[],
  warnings: string[],
): ChainState | null {
  const action = n.actionNode;
  if (!action || !action.conditions || action.conditions.length === 0) {
    warnings.push(`⚠ SuperJoin "${n.name}" missing actionNode.conditions — skipped.`);
    return null;
  }
  // Identify left/right predecessor by namespace
  const leftPred = preds.find(p => (p.nextNamespace || '').toLowerCase() === 'left');
  const rightPred = preds.find(p => (p.nextNamespace || '').toLowerCase() === 'right');
  if (!leftPred || !rightPred) {
    warnings.push(`⚠ SuperJoin "${n.name}": couldn't identify left/right inputs (preds: ${preds.map(p => p.nextNamespace).join(',')}) — skipped.`);
    return null;
  }
  const leftState = chainState.get(leftPred.from);
  const rightState = chainState.get(rightPred.from);
  if (!leftState || !rightState) {
    warnings.push(`⚠ SuperJoin "${n.name}": upstream chains missing — skipped.`);
    return null;
  }

  // Finalize both upstream elements before adding the relationship
  if (!leftState.finalized) finalizeChain(leftState, elements);
  if (!rightState.finalized) finalizeChain(rightState, elements);

  // Build relationship from action.conditions[]
  const keys: { sourceColumnId: string; targetColumnId: string }[] = [];
  for (const c of action.conditions) {
    const lExpr = (c.leftExpression || '').replace(/^\[(.+)\]$/, '$1');
    const rExpr = (c.rightExpression || '').replace(/^\[(.+)\]$/, '$1');
    if (!lExpr || !rExpr) continue;
    const sId = leftState.colByName.get(lExpr);
    const tId = rightState.colByName.get(rExpr);
    if (!sId || !tId) {
      warnings.push(`⚠ SuperJoin "${n.name}" condition ${lExpr}=${rExpr}: column not found on left or right — relationship key skipped.`);
      continue;
    }
    keys.push({ sourceColumnId: sId, targetColumnId: tId });
  }
  // Relationship name: use the TARGET warehouse table name (matches the convention in
  // production data models — see Retail Analytics models which name relationships
  // "CUSTOMER_DIM", "STORE_DIM" etc. Sigma's cross-element ref resolver expects this).
  // For Custom SQL targets (no warehouse path, no element name), fall back to the input's
  // friendly name so the relationship is human-readable rather than the join namespace.
  const rightSrcTable = (rightState.element.source as any)?.path?.slice(-1)[0]
    || rightState.element.name
    || rightState.displayableName
    || 'Right';
  const relName = String(rightSrcTable).toUpperCase().replace(/\s+/g, '_');
  if (keys.length === 0) {
    warnings.push(`⚠ SuperJoin "${n.name}": no resolvable join keys — relationship not created.`);
  } else {
    const joinType = (action.joinType || 'left').toLowerCase();
    const relType = joinType === 'inner' ? 'N:1' : joinType === 'right' ? '1:N' : 'N:1';
    (leftState.element.relationships ??= []).push({
      id: sigmaShortId(),
      targetElementId: rightState.element.id,
      keys,
      name: relName,
      relationshipType: relType,
    });
  }

  // Build a derived "join view" element exposing both sides' columns. Left columns
  // copy the upstream formula verbatim (warehouse-table refs resolve through lineage).
  // Right columns use the relationship-name cross-element form: [LeftTable/RelName/Field].
  // (Linked-column form `[Table/FK - link/Field]` is NOT supported via the API.)
  const leftSrcTable = (leftState.element.source as any)?.path?.slice(-1)[0] || (leftState.element.name || 'Left');
  const joinViewId = sigmaShortId();
  const joinView: SigmaElement = {
    id: joinViewId,
    kind: 'table',
    name: sigmaDisplayName(n.name || `${leftState.element.name} joined`),
    source: { kind: 'table', elementId: leftState.element.id },
    columns: [],
    metrics: [],
    order: [],
  };
  const joinColByName = new Map<string, string>();
  // Track logical refs for downstream child elements: column name → formula string suitable
  // for a child of the LEFT warehouse-table element. Left cols use [LeftTable/Col]; right
  // cols use [LeftTable/RelName/Col].
  const joinLogicalRefs = new Map<string, string>();

  // Left passthroughs — use [LeftElement.name/CurrentDisplayName] for both the join view
  // itself and the logical-ref map. Sigma resolves [TABLE/ColName] via the parent's CURRENT
  // column display name; copying the upstream formula verbatim breaks for renamed columns
  // because the formula references the now-stale physical name.
  const leftElementName = leftState.element.name || leftSrcTable;
  for (const c of leftState.element.columns) {
    const dn = c.name || extractDisplayFromFormula(c.formula);
    if (!dn) continue;
    const id = sigmaShortId();
    const childRef = `[${leftElementName}/${dn}]`;
    joinView.columns.push({ id, formula: childRef, name: dn });
    joinView.order.push(id);
    joinColByName.set(dn, id);
    joinLogicalRefs.set(dn, childRef);
    for (const [origKey, origId] of leftState.colByName) {
      if (origId === c.id && origKey !== dn) {
        joinColByName.set(origKey, id);
        joinLogicalRefs.set(origKey, childRef);
      }
    }
  }
  // Right cross-element refs — only if relationship was created
  if (keys.length > 0) {
    for (const c of rightState.element.columns) {
      const dn = c.name || extractDisplayFromFormula(c.formula);
      if (!dn) continue;
      const id = sigmaShortId();
      const xref = `[${leftSrcTable}/${relName}/${dn}]`;
      joinView.columns.push({ id, formula: xref, name: dn });
      joinView.order.push(id);
      if (!joinColByName.has(dn)) {
        joinColByName.set(dn, id);
        joinLogicalRefs.set(dn, xref);
      }
      for (const [origKey, origId] of rightState.colByName) {
        if (origId === c.id && origKey !== dn && !joinColByName.has(origKey)) {
          joinColByName.set(origKey, id);
          joinLogicalRefs.set(origKey, xref);
        }
      }
    }
  }

  elements.push(joinView);
  const newState: ChainState = {
    element: joinView,
    colByName: joinColByName,
    upstreamChainIds: [...leftState.upstreamChainIds, ...rightState.upstreamChainIds, n.id],
    sourceTable: '',
    isCustomSql: false,
    finalized: true,
    displayableName: leftState.displayableName,
  };
  // Stash the logical-ref map and the underlying left warehouse element id so downstream
  // SuperAggregate / SuperUnion can build child elements off the LEFT warehouse-table directly
  // (where cross-element refs resolve cleanly) rather than off the derived join view.
  (newState as any)._logicalRefs = joinLogicalRefs;
  (newState as any)._aggregateParent = leftState.element;
  return newState;
}

function processSuperUnion(
  n: PrepNode,
  preds: Array<{ from: string; namespace: string; nextNamespace: string }>,
  chainState: Map<string, ChainState>,
  elements: SigmaElement[],
  connectionId: string,
  warnings: string[],
): ChainState | null {
  const upstreamStates = preds.map(p => chainState.get(p.from)).filter(Boolean) as ChainState[];
  if (upstreamStates.length === 0) {
    warnings.push(`⚠ SuperUnion "${n.name}" has no upstream chains — skipped.`);
    return null;
  }
  // Finalize each upstream
  for (const s of upstreamStates) if (!s.finalized) finalizeChain(s, elements);

  // Build a Sigma union element. Uses inputs[] referencing elementIds.
  const elementId = sigmaShortId();
  const action = n.actionNode || {};
  const fieldMappings = (action.namespaceFieldMappings || []) as { namespaceName: string; fieldMappings: Record<string, string> }[];
  // Sigma's union source: kind:'union', inputs:[{elementId:..}], with column refs to the canonical name.
  const unionEl: SigmaElement = {
    id: elementId,
    kind: 'table',
    name: sigmaDisplayName(n.name || 'Union'),
    source: {
      connectionId,
      kind: 'union',
      inputs: upstreamStates.map(s => ({ elementId: s.element.id })),
    },
    columns: [],
    metrics: [],
    order: [],
  };

  // Use the first upstream's columns as the canonical schema. Sigma will line up by column name.
  const canon = upstreamStates[0];
  const colByName = new Map<string, string>();
  for (const c of canon.element.columns) {
    const dn = c.name || extractDisplayFromFormula(c.formula) || 'col';
    const id = sigmaShortId();
    unionEl.columns.push({
      id,
      formula: `[${dn}]`,
      name: c.name,
    });
    unionEl.order.push(id);
    colByName.set(dn, id);
  }

  if (fieldMappings.length > 0) {
    warnings.push(`ℹ SuperUnion "${n.name}" has namespaceFieldMappings — sources with renamed columns may need manual reconciliation.`);
  }

  elements.push(unionEl);
  return {
    element: unionEl,
    colByName,
    upstreamChainIds: upstreamStates.flatMap(s => s.upstreamChainIds),
    sourceTable: '',
    isCustomSql: false,
    finalized: true,
    displayableName: sigmaDisplayName(n.name || 'Union'),
  };
}

function processSuperAggregate(
  n: PrepNode,
  preds: Array<{ from: string; namespace: string; nextNamespace: string }>,
  chainState: Map<string, ChainState>,
  elements: SigmaElement[],
  warnings: string[],
): ChainState | null {
  const upstream = preds[0] && chainState.get(preds[0].from);
  if (!upstream) {
    warnings.push(`⚠ SuperAggregate "${n.name}" has no upstream chain — skipped.`);
    return null;
  }
  if (!upstream.finalized) finalizeChain(upstream, elements);

  const action = n.actionNode || {};
  const groupBys = (action.groupByFields || []) as { columnName: string; function: string; newColumnName: string | null }[];
  const aggs = (action.aggregateFields || []) as { columnName: string; function: string; newColumnName: string | null }[];

  // Determine the parent for the aggregate child element. When upstream came from a
  // SuperJoin we have an _aggregateParent (= the LEFT warehouse-table element) and
  // _logicalRefs (column name → formula string usable from a child of LEFT). Otherwise
  // we use upstream's element directly and its columns' formulas.
  const parentEl: SigmaElement = (upstream as any)._aggregateParent || upstream.element;
  const logicalRefs: Map<string, string> = (upstream as any)._logicalRefs || new Map();
  // Fallback: if no _logicalRefs, build them from upstream.element.columns using the
  // [ParentElementName/Display] form so refs resolve via the parent's current display name.
  if (logicalRefs.size === 0) {
    const parentName = upstream.element.name
      || (upstream.element.source as any)?.path?.slice(-1)[0]
      || 'Parent';
    for (const c of upstream.element.columns) {
      const dn = c.name || extractDisplayFromFormula(c.formula);
      if (dn) logicalRefs.set(dn, `[${parentName}/${dn}]`);
    }
  }

  const elementId = sigmaShortId();
  const childEl: SigmaElement = {
    id: elementId,
    kind: 'table',
    name: sigmaDisplayName(n.name || 'Aggregate'),
    source: { kind: 'table', elementId: parentEl.id },
    columns: [],
    metrics: [],
    order: [],
  };

  const colByName = new Map<string, string>();

  // Helper: resolve a column reference (by Tableau Prep display or SNAKE_CASE name)
  // to an upstream formula. Tries upstream.colByName first, then falls back to direct
  // logicalRefs lookup or building a warehouse-table ref.
  function resolveRef(name: string): string | null {
    // logicalRefs is keyed by display name and SNAKE_CASE
    if (logicalRefs.has(name)) return logicalRefs.get(name)!;
    if (logicalRefs.has(sigmaDisplayName(name))) return logicalRefs.get(sigmaDisplayName(name))!;
    return null;
  }

  // Group-by → passthrough columns referencing the parent warehouse-table directly
  const groupByIds: string[] = [];
  for (const gb of groupBys) {
    const ref = resolveRef(gb.columnName);
    if (!ref) {
      warnings.push(`⚠ SuperAggregate "${n.name}": groupBy column "${gb.columnName}" not on upstream — skipped.`);
      continue;
    }
    const cId = sigmaShortId();
    const dn = gb.newColumnName || (logicalRefsName(logicalRefs, gb.columnName) || gb.columnName);
    childEl.columns.push({ id: cId, formula: ref, name: dn });
    childEl.order.push(cId);
    colByName.set(dn, cId);
    groupByIds.push(cId);
  }

  // Aggregates → metrics
  const aggMap: Record<string, (col: string) => string> = {
    SUM: c => `Sum([${c}])`,
    AVG: c => `Avg([${c}])`,
    AVERAGE: c => `Avg([${c}])`,
    MIN: c => `Min([${c}])`,
    MAX: c => `Max([${c}])`,
    COUNT: c => `CountIf(IsNotNull([${c}]))`,
    COUNTD: c => `CountDistinct([${c}])`,
    MEDIAN: c => `Median([${c}])`,
    STDEV: c => `StdDev([${c}])`,
  };
  // Aggregations follow the LOD pattern: each aggregate's referenced columns are
  // added as passthroughs on the child element first, then the aggregate calc col
  // references them by display name. This keeps the aggregate self-contained.
  const calcIds: string[] = [];
  // Track passthrough columns added for aggregate refs to avoid duplicates
  const passthroughAdded = new Map<string, string>(); // display name → col id
  for (const a of aggs) {
    const fn = aggMap[(a.function || '').toUpperCase()];
    if (!fn) {
      warnings.push(`⚠ SuperAggregate "${n.name}": agg function "${a.function}" not supported — skipped.`);
      continue;
    }
    const refFormula = resolveRef(a.columnName);
    if (!refFormula) {
      warnings.push(`⚠ SuperAggregate "${n.name}": agg column "${a.columnName}" not on upstream — skipped.`);
      continue;
    }
    const refDispName = logicalRefsName(logicalRefs, a.columnName) || sigmaDisplayName(a.columnName);

    // Add referenced column as a HIDDEN passthrough if not already present.
    // Hiding it keeps the aggregate functional while ensuring Sigma's grouping
    // produces one row per (groupBy) — un-hidden non-grouped columns force per-row output.
    let passId = passthroughAdded.get(refDispName) || colByName.get(refDispName);
    if (!passId) {
      passId = sigmaShortId();
      childEl.columns.push({ id: passId, formula: refFormula, name: refDispName, hidden: true } as any);
      childEl.order.push(passId);
      passthroughAdded.set(refDispName, passId);
      colByName.set(refDispName, passId);
    }

    // Now the aggregate calc col references the passthrough by display name
    const dn = a.newColumnName || a.columnName;
    const id = sigmaShortId();
    childEl.columns.push({ id, formula: fn(refDispName), name: dn });
    childEl.order.push(id);
    colByName.set(dn, id);
    calcIds.push(id);
  }
  if (groupByIds.length > 0 && calcIds.length > 0) {
    (childEl as any).groupings = [{
      id: sigmaShortId(),
      groupBy: groupByIds,
      calculations: calcIds,
    }];
  }
  // Drop empty metrics array — aggs are calc cols, not metrics
  if (childEl.metrics?.length === 0) delete childEl.metrics;

  elements.push(childEl);
  return {
    element: childEl,
    colByName,
    upstreamChainIds: [...upstream.upstreamChainIds, n.id],
    sourceTable: '',
    isCustomSql: false,
    finalized: true,
    displayableName: sigmaDisplayName(n.name || 'Aggregate'),
  };
}

// ── Internal types ───────────────────────────────────────────────────────────

interface NextNode {
  namespace: string;
  nextNodeId: string;
  nextNamespace: string;
}

interface PrepField {
  name: string;
  type?: string;
  collation?: string;
  caption?: string;
}

interface PrepNode {
  id: string;
  nodeType: string;
  baseType: string;
  name: string;
  nextNodes?: NextNode[];
  // Input
  connectionId?: string;
  connectionAttributes?: any;
  fields?: PrepField[];
  actions?: PrepNode[];
  // Transform-specific
  columnName?: string;
  expression?: string;
  rename?: string;
  newType?: string;
  values?: Record<string, string[]>;
  columnNames?: string[];
  filterExpression?: string;
  // Container
  loomContainer?: { nodes: Record<string, PrepNode>; initialNodes?: string[] };
  // Super
  actionNode?: any;
  conditions?: { leftExpression: string; rightExpression: string; comparator: string }[];
  joinType?: string;
  groupByFields?: { columnName: string; function: string; newColumnName: string | null }[];
  aggregateFields?: { columnName: string; function: string; newColumnName: string | null }[];
  namespaceFieldMappings?: { namespaceName: string; fieldMappings: Record<string, string> }[];
}

interface PrepFlow {
  nodes: Record<string, PrepNode>;
  initialNodes?: string[];
  connections?: Record<string, any>;
  parameters?: any;
  documentId?: string;
  majorVersion?: number;
  minorVersion?: number;
}
