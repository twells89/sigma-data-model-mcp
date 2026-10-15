// .cache/semantic-tools/packages/core/src/util.ts
var UnreachableCaseError = class extends Error {
  constructor(message, value) {
    super(
      value === void 0 ? message : `${message}: ${JSON.stringify(value)}`
    );
  }
};
function escapeRegExp(value) {
  const str = typeof value === "string" ? value : (value ?? "") + "";
  if (RegExp.escape) {
    return RegExp.escape(str);
  }
  return str.replace(/[\\^$.*+?()[\]{}|]/g, "\\$&");
}

// .cache/semantic-tools/packages/core/src/sqlExpr.ts
function subExpressions(node) {
  switch (node.kind) {
    case "column":
    case "number":
    case "string":
    case "boolean":
    case "null":
      return [];
    case "unary":
    case "not":
      return [node.operand];
    case "isNull":
      return [node.operand];
    case "in":
      return [node.value, ...node.candidates];
    case "like":
      return [node.value, node.pattern];
    case "case":
      return [
        ...node.branches.flatMap((branch) => [branch.when, branch.then]),
        ...node.otherwise === null ? [] : [node.otherwise]
      ];
    case "binary":
      return [node.left, node.right];
    case "call":
      return node.args;
    case "variantPath":
      return [node.base];
    case "cast":
      return [node.operand];
  }
}
function referencedQualifiers(expression) {
  const qualifiers = /* @__PURE__ */ new Set();
  const visit3 = (node) => {
    switch (node.kind) {
      case "column":
        if (node.parts.length > 1) {
          const qualifier = node.parts[node.parts.length - 2];
          if (qualifier !== void 0) qualifiers.add(qualifier);
        }
        return;
      case "unary":
        visit3(node.operand);
        return;
      case "binary":
        visit3(node.left);
        visit3(node.right);
        return;
      case "call":
        for (const arg of node.args) visit3(arg);
        return;
      case "variantPath":
        visit3(node.base);
        return;
      case "cast":
        visit3(node.operand);
        return;
      case "not":
      case "isNull":
        visit3(node.operand);
        return;
      case "in":
        visit3(node.value);
        for (const candidate of node.candidates) visit3(candidate);
        return;
      case "like":
        visit3(node.value);
        visit3(node.pattern);
        return;
      case "case":
        for (const branch of node.branches) {
          visit3(branch.when);
          visit3(branch.then);
        }
        if (node.otherwise !== null) visit3(node.otherwise);
        return;
      case "number":
      case "string":
      case "boolean":
      case "null":
        return;
      default:
        throw new UnreachableCaseError("Unhandled expression node", node);
    }
  };
  visit3(expression);
  return [...qualifiers].sort();
}

// .cache/semantic-tools/packages/core/src/findings.ts
var SEVERITY_BY_CODE = {
  SEMI_ADDITIVE_METRIC: "error",
  ASOF_RELATIONSHIP: "error",
  RANGE_JOIN_RELATIONSHIP: "error",
  RANGE_JOIN_CONSTRAINT: "error",
  METRIC_JOIN_PATH_SELECTOR: "error",
  WINDOW_METRIC_DYNAMIC_PARTITION: "error",
  WINDOW_METRIC: "error",
  DOWNSTREAM_OF_BLOCKED_RELATIONSHIP: "error",
  MULTIPLE_STATEMENTS: "error",
  MALFORMED_DDL: "error",
  PARSE_FAILURE: "error",
  MISSING_DIMENSIONS_OR_METRICS: "error",
  YAML_PARSE_FAILURE: "error",
  DERIVED_METRIC: "warning",
  CROSS_TABLE_FACT_AGGREGATE: "warning",
  PRIVATE_OBJECT: "warning",
  FILTER_LABEL: "warning",
  EXPRESSION_PASSTHROUGH: "warning",
  EXPRESSION_NOT_REPRESENTABLE: "warning",
  ZONE_DEPENDENT_EXPRESSION: "warning",
  GRAIN_METADATA_NOT_EMITTED: "warning",
  SYNONYMS_DROPPED: "warning",
  TAGS_DROPPED: "warning",
  CORTEX_SEARCH_DROPPED: "warning",
  SAMPLE_VALUES_DROPPED: "warning",
  AI_INSTRUCTIONS_DROPPED: "warning",
  VERIFIED_QUERIES_DROPPED: "warning",
  MAX_STALENESS_DROPPED: "warning",
  COPY_GRANTS_DROPPED: "warning",
  COLUMN_TYPES_SKIPPED: "warning",
  YAML_QUOTE_REPAIR_APPLIED: "warning"
};
var ALL_FINDING_CODES = Object.keys(
  SEVERITY_BY_CODE
);
var BEST_EFFORT_POLICY = {
  // The model answers, and answers differently.
  SEMI_ADDITIVE_METRIC: "overridable",
  METRIC_JOIN_PATH_SELECTOR: "overridable",
  WINDOW_METRIC: "overridable",
  WINDOW_METRIC_DYNAMIC_PARTITION: "overridable",
  ASOF_RELATIONSHIP: "overridable",
  RANGE_JOIN_RELATIONSHIP: "overridable",
  RANGE_JOIN_CONSTRAINT: "overridable",
  DOWNSTREAM_OF_BLOCKED_RELATIONSHIP: "overridable",
  // Nothing parsed, or nothing coherent to emit from what did.
  MULTIPLE_STATEMENTS: "always-blocks",
  MALFORMED_DDL: "always-blocks",
  PARSE_FAILURE: "always-blocks",
  MISSING_DIMENSIONS_OR_METRICS: "always-blocks",
  YAML_PARSE_FAILURE: "always-blocks"
};
function mkFinding(code, object, line, ...details) {
  const finding = {
    code,
    severity: SEVERITY_BY_CODE[code],
    object,
    line,
    ...details.length > 0 && { details: details[0] }
  };
  return finding;
}
function sortFindings(findings) {
  return [...findings].sort((a, b) => {
    if (a.severity !== b.severity) return a.severity === "error" ? -1 : 1;
    if (a.line !== b.line) return a.line - b.line;
    return a.code < b.code ? -1 : a.code > b.code ? 1 : 0;
  });
}

// .cache/semantic-tools/packages/core/src/driver.ts
function requireOption(value, name) {
  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`convert requires a non-empty ${name}`);
  }
}
function convert(args) {
  const { reader, translator, writer, source, options } = args;
  requireOption(options.connectionId, "connectionId");
  if (reader.dialect !== translator.from || translator.to !== writer.dialect) {
    throw new Error(
      `dialect mismatch: reader produces '${reader.dialect}', translator converts '${translator.from}' to '${translator.to}', writer expects '${writer.dialect}'`
    );
  }
  const read2 = reader.read(source);
  if (!read2.ok) {
    return {
      ok: false,
      errors: sortFindings(read2.errors),
      warnings: [],
      bestEffortWouldConvert: false
    };
  }
  return writer.write(read2.ir, translator, options);
}

// .cache/semantic-tools/packages/core/src/formatFinding.ts
function describe(object) {
  const qualified = object.owner === void 0 ? object.name : `${object.owner}.${object.name}`;
  return `${object.kind} ${qualified}`;
}
function list(names) {
  return names.join(", ");
}
function describeRefusal(cause) {
  switch (cause.kind) {
    case "function":
      return `${cause.name} has no exact Sigma equivalent`;
    case "arity":
      return `${cause.name} was called with ${cause.args} argument(s), which Sigma's equivalent does not accept`;
    case "argument-type":
      return `${cause.name} was given an argument that is not provably numeric, and its Sigma equivalent requires every argument to share one type`;
    case "date-part":
      return cause.part === void 0 ? `${cause.name} was given a date part Sigma cannot enumerate` : `${cause.name} was given the date part ${cause.part}, which has no exact Sigma equivalent`;
    case "distinct":
      return `DISTINCT cannot be conveyed through ${cause.name}`;
    case "star":
      return `* cannot be conveyed through ${cause.name}`;
    case "column":
      return `${cause.column} is not reachable from the emitted element`;
    case "literal":
      return "a string literal holds a character Sigma cannot escape";
    case "unparsed":
      return "it is not an expression the parser can read";
    case "too-large":
      return "it is longer than the converter will parse";
    case "construct":
      return `${cause.construct} is outside the expression subset the converter models`;
    case "operator":
      return `the ${cause.operator} operator has no exact Sigma equivalent`;
    case "simple-case":
      return "a CASE with a subject expression has no Sigma equivalent that avoids duplicating it";
    case "aggregate-clause":
      return `a ${cause.clause} clause changes what the aggregate means and cannot be carried`;
    case "unresolved-reference":
      return "a name it references could not be resolved";
    case "no-aggregate":
      return "a metric must aggregate, and this expression does not";
    default:
      throw new UnreachableCaseError("Unhandled refusal", cause);
  }
}
function formatFinding(finding) {
  const location = `line ${finding.line}`;
  switch (finding.code) {
    case "SEMI_ADDITIVE_METRIC":
      return {
        message: `${describe(finding.object)} at ${location} is semi-additive: it declares NON ADDITIVE BY (${list(
          finding.details.dimensions
        )}). Sigma has no equivalent, and the aggregate would double-count across that dimension.`,
        remediation: "Remove the NON ADDITIVE BY clause and redefine the metric at a fixed grain, or exclude this metric from the imported view."
      };
    case "ASOF_RELATIONSHIP":
      return {
        message: `${describe(
          finding.object
        )} at ${location} joins with ASOF, an inequality join Sigma relationships cannot express.`,
        remediation: "Materialize the ASOF join as a Snowflake view and import that view as a table, or exclude this relationship and the objects that traverse it."
      };
    case "RANGE_JOIN_RELATIONSHIP":
      return {
        message: `${describe(
          finding.object
        )} at ${location} joins on a BETWEEN ... EXCLUSIVE range, which Sigma relationships cannot express.`,
        remediation: "Materialize the range join as a Snowflake view and import that view as a table, or exclude this relationship and the objects that traverse it."
      };
    case "RANGE_JOIN_CONSTRAINT":
      return {
        message: `${describe(
          finding.object
        )} at ${location} declares a CONSTRAINT ... DISTINCT RANGE key, which has no Sigma equivalent.`,
        remediation: "Remove the range constraint and the relationships that depend on it, or import a Snowflake view that resolves the range join."
      };
    case "METRIC_JOIN_PATH_SELECTOR":
      return {
        message: `${describe(finding.object)} at ${location} pins its join path with USING (${list(
          finding.details.relationships
        )}). Sigma would resolve a path of its own choosing.`,
        remediation: "Remove the ambiguity in the relationship graph so the metric no longer needs USING, or exclude this metric from the imported view."
      };
    case "WINDOW_METRIC_DYNAMIC_PARTITION":
      return {
        message: `${describe(
          finding.object
        )} at ${location} partitions with PARTITION BY EXCLUDING, whose partition is defined against the requesting query's dimensions.`,
        remediation: "Redefine the metric without a window function, or exclude it from the imported view and build the calculation in a Sigma workbook where the query grain is known."
      };
    case "WINDOW_METRIC":
      return {
        message: `${describe(
          finding.object
        )} at ${location} uses an OVER (...) window clause, which presumes the query grain.`,
        remediation: "Redefine the metric without a window function, or exclude it from the imported view and build the calculation in a Sigma workbook where the query grain is known."
      };
    case "DOWNSTREAM_OF_BLOCKED_RELATIONSHIP":
      return {
        message: `${describe(finding.object)} at ${location} may resolve through blocked relationship ${finding.details.blockedRelationship}, which touches ${list(finding.details.taintedTables)}.`,
        remediation: "Resolve the blocked relationship first; this object needs no change of its own."
      };
    case "MULTIPLE_STATEMENTS":
      return {
        message: `The input contains ${finding.details.count} CREATE SEMANTIC VIEW statements. This converter handles exactly one.`,
        remediation: "Split the input so each call converts a single CREATE SEMANTIC VIEW statement."
      };
    case "MALFORMED_DDL":
      return {
        message: `The statement parsed but does not hang together at ${location}: ${finding.details.diagnostic}.`,
        remediation: "Correct the statement so every alias it names is declared in TABLES and every relationship joins the same number of columns on each side."
      };
    case "PARSE_FAILURE":
      return {
        message: `The DDL could not be parsed at ${location}.`,
        remediation: "Check the statement against the CREATE SEMANTIC VIEW grammar; if it is valid Snowflake DDL, this is a converter bug worth reporting with the statement attached."
      };
    case "YAML_PARSE_FAILURE":
      return {
        message: `The YAML could not be parsed at ${location}: ${finding.details.diagnostic}.`,
        remediation: "Check the file against the semantic-view YAML specification. If it was obtained via a query result exported to CSV or a spreadsheet, re-export it directly instead (e.g. via SnowSQL or a script writing the raw result to a file) \u2014 that path is known to corrupt quoted values."
      };
    case "MISSING_DIMENSIONS_OR_METRICS":
      return {
        message: `The view declares ${finding.details.tableCount} table(s) but no DIMENSIONS or METRICS clause. Snowflake requires at least one of the two \u2014 a FACTS clause alone does not satisfy it \u2014 so this DDL could not have come from a real semantic view.`,
        remediation: "Add a DIMENSIONS or METRICS clause, or confirm this DDL is what Snowflake actually emitted for this view."
      };
    case "DERIVED_METRIC":
      return {
        message: `${describe(
          finding.object
        )} at ${location} is a derived metric with no owning logical table. Confirm its grain individually against the emitted element.`
      };
    case "CROSS_TABLE_FACT_AGGREGATE":
      return {
        message: `${describe(finding.object)} at ${location} aggregates ${list(
          finding.details.foreignTables
        )}, which the emitted element cannot reach. It was dropped.`
      };
    case "PRIVATE_OBJECT":
      return {
        message: `${describe(
          finding.object
        )} at ${location} is PRIVATE. It was inlined into its consumers and is not queryable on its own, matching Snowflake.`
      };
    case "FILTER_LABEL":
      return {
        message: `${describe(
          finding.object
        )} at ${location} carries LABELS = (FILTER). The named filter was dropped; the predicate was not pre-applied.`
      };
    case "EXPRESSION_PASSTHROUGH":
      return {
        message: `${describe(finding.object)} at ${location} was emitted as a warehouse passthrough call, preserving \`${finding.details.snowflakeExpression}\` exactly.`
      };
    case "EXPRESSION_NOT_REPRESENTABLE":
      return {
        message: `${describe(finding.object)} at ${location} was dropped: \`${finding.details.snowflakeExpression}\` could not be translated because ${describeRefusal(
          finding.details.cause
        )}${finding.details.via === void 0 ? "" : `, reached through ${describe(finding.details.via)}`}, and an approximation would change results.`
      };
    case "ZONE_DEPENDENT_EXPRESSION":
      return {
        message: `${describe(finding.object)} at ${location} was dropped: \`${finding.details.snowflakeExpression}\` reads \`${finding.details.column}\`, a TIMESTAMP_TZ column. Snowflake evaluates it in the offset each value carries and Sigma evaluates it in the session timezone, so the two disagree row by row.`
      };
    case "GRAIN_METADATA_NOT_EMITTED": {
      const sentences = [];
      if (finding.details.tables.length > 0) {
        sentences.push(
          `${list(
            finding.details.tables
          )} declared more keys than a data model can hold, so one key each was kept and the rest dropped.`
        );
      }
      if (finding.details.tablesWithoutKey.length > 0) {
        const tables = finding.details.tablesWithoutKey;
        sentences.push(
          `No key declared on ${list(tables)} can be expressed, so ${tables.length === 1 ? "it carries" : "they carry"} none.`
        );
      }
      return { message: sentences.join(" ") };
    }
    case "SYNONYMS_DROPPED":
      return {
        message: `${finding.details.count} synonym(s) on ${describe(finding.object)} at ${location} were dropped.`
      };
    case "TAGS_DROPPED":
      return {
        message: `${finding.details.count} tag(s) on ${describe(finding.object)} at ${location} were dropped.`
      };
    case "CORTEX_SEARCH_DROPPED":
      return {
        message: `${finding.details.count} Cortex Search service binding(s) on ${describe(
          finding.object
        )} at ${location} were dropped.`
      };
    case "SAMPLE_VALUES_DROPPED":
      return {
        message: `${finding.details.count} sample value(s) on ${describe(finding.object)} at ${location} were dropped.`
      };
    case "AI_INSTRUCTIONS_DROPPED":
      return {
        message: `AI instructions on ${describe(finding.object)} at ${location} were dropped.`
      };
    case "VERIFIED_QUERIES_DROPPED":
      return {
        message: `${finding.details.count} verified query/queries on ${describe(
          finding.object
        )} at ${location} were dropped.`
      };
    case "MAX_STALENESS_DROPPED":
      return {
        message: `MAX_STALENESS on ${describe(finding.object)} at ${location} was dropped; Sigma manages materialization separately.`
      };
    case "COPY_GRANTS_DROPPED":
      return {
        message: `COPY GRANTS on ${describe(finding.object)} at ${location} was dropped; Sigma manages permissions separately.`
      };
    case "COLUMN_TYPES_SKIPPED":
      return {
        message: `No column types were supplied for this conversion, so TIMESTAMP_TZ zone-dependence checks did not run. Any expression reading a TIMESTAMP_TZ column may convert clean while disagreeing with Snowflake row by row.`
      };
    case "YAML_QUOTE_REPAIR_APPLIED":
      return {
        message: `${finding.details.count} value(s) in this YAML had doubled quote characters mechanically repaired before parsing \u2014 a known artifact of downloading a query result (e.g. SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW's output) as CSV. Spot-check the converted output, and fetch the YAML directly next time to avoid this.`
      };
    default:
      throw new UnreachableCaseError("Unhandled finding code", finding);
  }
}

// .cache/semantic-tools/packages/sigma-data-model/src/naming.ts
var WORD_PATTERN = /\p{L}+|[0-9]+/gu;
function toDisplayName(rawColumnName) {
  return (rawColumnName.match(WORD_PATTERN) ?? []).map((word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()).join(" ");
}
function slug(value) {
  return value.toLowerCase().replace(/[^a-z0-9]/g, "_");
}
function mkElementId(alias) {
  return slug(alias);
}
function mkColumnId(elementId, columnName) {
  return `${elementId}__${slug(columnName)}`;
}
function mkMetricId(elementId, metricName) {
  return `${elementId}__m__${slug(metricName)}`;
}
function mkRelationshipId(sourceElementId, targetElementId) {
  return `${sourceElementId}__to__${targetElementId}`;
}
function claim(candidate, taken, separator) {
  if (!taken.has(candidate)) {
    taken.add(candidate);
    return candidate;
  }
  let suffix = 2;
  while (taken.has(`${candidate}${separator}${suffix}`)) suffix++;
  const claimed = `${candidate}${separator}${suffix}`;
  taken.add(claimed);
  return claimed;
}
function claimId(candidate, taken) {
  return claim(candidate, taken, "__");
}
function claimName(candidate, taken) {
  return claim(candidate, taken, " ");
}

// .cache/semantic-tools/packages/sigma-data-model/src/physicalTables.ts
function physicalTablesOf(spec) {
  let parsed;
  try {
    parsed = JSON.parse(spec);
  } catch {
    return [];
  }
  const pages = parsed.pages;
  if (!Array.isArray(pages)) return [];
  const seen = /* @__PURE__ */ new Set();
  const tables = [];
  for (const page of pages) {
    const elements = page.elements;
    if (!Array.isArray(elements)) continue;
    for (const element of elements) {
      const path = element.source?.path;
      if (!Array.isArray(path)) continue;
      if (!path.every((segment) => typeof segment === "string")) {
        continue;
      }
      const key = path.join(".");
      if (seen.has(key)) continue;
      seen.add(key);
      tables.push(path);
    }
  }
  return tables;
}

// .cache/semantic-tools/packages/sigma-data-model/src/rules.ts
function resolveContextName(name) {
  if (name.length >= 2 && name.startsWith('"') && name.endsWith('"')) {
    return name.slice(1, -1).replace(/""/g, '"');
  }
  return name.toUpperCase();
}
function requireContextName(value, name) {
  if (value === void 0) {
    throw new Error(
      `resolvePath needed a session-context ${name}, but ConvertOptions_t.${name} was not supplied`
    );
  }
  return resolveContextName(value);
}
function resolvePath(parts, options) {
  if (parts.length >= 3) return parts.slice(parts.length - 3);
  const database = requireContextName(options.database, "database");
  if (parts.length === 2) return [database, ...parts];
  return [database, requireContextName(options.schema, "schema"), ...parts];
}
function relationshipName(relationship) {
  return relationship.name?.name ?? `${relationship.sourceAlias.name}_TO_${relationship.targetAlias.name}`;
}
function objectRef(object) {
  return {
    kind: object.kind,
    name: object.name.name,
    ...object.owner !== null && { owner: object.owner.name }
  };
}
function relationshipRef(relationship) {
  return { kind: "relationship", name: relationshipName(relationship) };
}
function objectKey(object) {
  return `${object.kind}:${object.owner?.name ?? ""}.${object.name.name}`;
}
function blockedRelationships(view) {
  return view.relationships.filter((relationship) => {
    if (relationship.target.kind === "asof") return true;
    if (relationship.target.kind === "range") return true;
    return view.tables.some(
      (table) => table.rangeConstraint !== null && (table.alias.name === relationship.sourceAlias.name || table.alias.name === relationship.targetAlias.name)
    );
  });
}
function applyBlockingRules(view) {
  const errors = [];
  const blockedObjects = /* @__PURE__ */ new Set();
  for (const table of view.tables) {
    if (table.rangeConstraint !== null) {
      errors.push(
        mkFinding(
          "RANGE_JOIN_CONSTRAINT",
          { kind: "table", name: table.alias.name },
          table.span.line
        )
      );
    }
  }
  for (const relationship of view.relationships) {
    if (relationship.target.kind === "asof") {
      errors.push(
        mkFinding(
          "ASOF_RELATIONSHIP",
          relationshipRef(relationship),
          relationship.span.line
        )
      );
    } else if (relationship.target.kind === "range") {
      errors.push(
        mkFinding(
          "RANGE_JOIN_RELATIONSHIP",
          relationshipRef(relationship),
          relationship.span.line
        )
      );
    }
  }
  for (const metric of view.metrics) {
    const ref = objectRef(metric);
    if (metric.nonAdditiveBy !== null) {
      errors.push(
        mkFinding("SEMI_ADDITIVE_METRIC", ref, metric.span.line, {
          dimensions: metric.nonAdditiveBy.map((dimension) => dimension.name)
        })
      );
      blockedObjects.add(objectKey(metric));
    }
    if (metric.using !== null) {
      errors.push(
        mkFinding("METRIC_JOIN_PATH_SELECTOR", ref, metric.span.line, {
          relationships: metric.using.map((relationship) => relationship.name)
        })
      );
      blockedObjects.add(objectKey(metric));
    }
    if (metric.windowConstruct === "dynamicPartition") {
      errors.push(
        mkFinding(
          "WINDOW_METRIC_DYNAMIC_PARTITION",
          ref,
          metric.expression.span.line
        )
      );
      blockedObjects.add(objectKey(metric));
    } else if (metric.windowConstruct === "plain") {
      errors.push(
        mkFinding("WINDOW_METRIC", ref, metric.expression.span.line)
      );
      blockedObjects.add(objectKey(metric));
    }
  }
  return { errors, blockedObjects };
}
function propagateBlockedRelationships(view, blockedObjects) {
  const blocked = blockedRelationships(view);
  if (blocked.length === 0) return [];
  const causeByAlias = /* @__PURE__ */ new Map();
  for (const relationship of blocked) {
    for (const alias of [relationship.sourceAlias, relationship.targetAlias]) {
      if (!causeByAlias.has(alias.name)) {
        causeByAlias.set(alias.name, { alias, relationship });
      }
    }
  }
  const findings = [];
  for (const object of [...view.facts, ...view.dimensions, ...view.metrics]) {
    if (blockedObjects.has(objectKey(object))) continue;
    let cause = object.kind === "dimension" || object.owner === null ? void 0 : causeByAlias.get(object.owner.name)?.relationship;
    if (cause === void 0) {
      for (const { alias, relationship } of causeByAlias.values()) {
        if (object.referencedAliases.includes(alias.name)) {
          cause = relationship;
          break;
        }
      }
    }
    if (cause === void 0) continue;
    findings.push(
      mkFinding(
        "DOWNSTREAM_OF_BLOCKED_RELATIONSHIP",
        objectRef(object),
        object.span.line,
        {
          taintedTables: [cause.sourceAlias.name, cause.targetAlias.name],
          blockedRelationship: relationshipName(cause)
        }
      )
    );
  }
  return findings;
}

// .cache/semantic-tools/packages/sigma-data-model/src/droppedMetadata.ts
function collectDroppedMetadata(view, grain) {
  const findings = [];
  const viewRef = {
    kind: "view",
    name: view.name.parts.at(-1)?.name ?? ""
  };
  const countable = (code, count, ref, line) => {
    if (count > 0) findings.push(mkFinding(code, ref, line, { count }));
  };
  countable("TAGS_DROPPED", view.tagCount, viewRef, view.span.line);
  countable(
    "VERIFIED_QUERIES_DROPPED",
    view.verifiedQueryCount,
    viewRef,
    view.span.line
  );
  if (view.hasMaxStaleness) {
    findings.push(mkFinding("MAX_STALENESS_DROPPED", viewRef, view.span.line));
  }
  if (view.hasAiMetadata) {
    findings.push(
      mkFinding("AI_INSTRUCTIONS_DROPPED", viewRef, view.span.line)
    );
  }
  if (view.hasCopyGrants) {
    findings.push(mkFinding("COPY_GRANTS_DROPPED", viewRef, view.span.line));
  }
  for (const table of view.tables) {
    const ref = { kind: "table", name: table.alias.name };
    countable("SYNONYMS_DROPPED", table.synonymCount, ref, table.span.line);
    countable("TAGS_DROPPED", table.tagCount, ref, table.span.line);
  }
  for (const object of [...view.facts, ...view.dimensions, ...view.metrics]) {
    const ref = objectRef(object);
    countable("SYNONYMS_DROPPED", object.synonymCount, ref, object.span.line);
    countable("TAGS_DROPPED", object.tagCount, ref, object.span.line);
    countable(
      "CORTEX_SEARCH_DROPPED",
      object.cortexSearchCount,
      ref,
      object.span.line
    );
    countable(
      "SAMPLE_VALUES_DROPPED",
      object.sampleValueCount,
      ref,
      object.span.line
    );
    if (object.filterLabel) {
      findings.push(mkFinding("FILTER_LABEL", ref, object.span.line));
    }
    if (object.visibility === "private") {
      findings.push(mkFinding("PRIVATE_OBJECT", ref, object.span.line));
    }
  }
  const { droppedTables, tablesWithoutKey } = grain;
  if (droppedTables.length > 0 || tablesWithoutKey.length > 0) {
    findings.push(
      mkFinding("GRAIN_METADATA_NOT_EMITTED", viewRef, view.span.line, {
        tables: droppedTables,
        tablesWithoutKey
      })
    );
  }
  return findings;
}

// .cache/semantic-tools/packages/sigma-data-model/src/grain.ts
function uniqueKeyNames(table) {
  return table.uniqueKeys.map((key) => key.map((column) => column.name));
}
function resolveImplicitKey(table) {
  const primaryKey = table.primaryKey?.map((column) => column.name);
  if (primaryKey !== void 0)
    return { kind: "resolved", columns: primaryKey };
  const uniques = uniqueKeyNames(table);
  if (uniques.length === 0) return { kind: "none" };
  if (uniques.length > 1) {
    return { kind: "ambiguous", uniqueCount: uniques.length };
  }
  return { kind: "resolved", columns: uniques[0] ?? [] };
}
function sameColumns(left, right) {
  if (left.length !== right.length) return false;
  const remaining = new Set(right);
  return left.every((column) => remaining.delete(column));
}
function referencedColumnSets(view, table) {
  const sets = [];
  for (const relationship of view.relationships) {
    if (relationship.targetAlias.name !== table.alias.name) continue;
    if (relationship.target.kind === "columns") {
      sets.push(relationship.target.columns.map((column) => column.name));
      continue;
    }
    if (relationship.target.kind !== "implicit") continue;
    const implicit = resolveImplicitKey(table);
    if (implicit.kind === "resolved") sets.push(implicit.columns);
  }
  return sets;
}
function deriveGrainMetadata(view) {
  const byAlias = /* @__PURE__ */ new Map();
  const droppedTables = [];
  const tablesWithoutKey = [];
  for (const table of view.tables) {
    const alias = table.alias.name;
    const primaryKey = table.primaryKey?.map((column) => column.name) ?? null;
    const uniques = uniqueKeyNames(table);
    let keyColumns = [];
    if (primaryKey !== null) {
      keyColumns = primaryKey;
    } else {
      const referenced = referencedColumnSets(view, table);
      keyColumns = uniques.find(
        (unique) => referenced.some((target) => sameColumns(unique, target))
      ) ?? [];
    }
    const declaredCount = (primaryKey === null ? 0 : 1) + uniques.length;
    const droppedKeys = declaredCount > (keyColumns.length > 0 ? 1 : 0);
    byAlias.set(alias, { keyColumns, droppedKeys });
    if (!droppedKeys) continue;
    if (keyColumns.length > 0) droppedTables.push(alias);
    else tablesWithoutKey.push(alias);
  }
  return { byAlias, droppedTables, tablesWithoutKey };
}

// .cache/semantic-tools/packages/sigma-data-model/src/emit.ts
var PAGE_ID = "page1";
var PAGE_NAME = "Page 1";
var SCHEMA_VERSION = 1;
var RELATIONSHIP_CARDINALITY = "N:1";
var MAX_INLINE_CHAIN = 16;
function failureOutcome(result) {
  if (result.zoneDependentColumn !== void 0) {
    return { kind: "zoned", column: result.zoneDependentColumn };
  }
  return { kind: "refused", cause: result.refusal ?? { kind: "unparsed" } };
}
function mkObjectRegistry(view) {
  const byQualifiedName = /* @__PURE__ */ new Map();
  const byBareName = /* @__PURE__ */ new Map();
  for (const object of [...view.facts, ...view.dimensions, ...view.metrics]) {
    byQualifiedName.set(
      `${object.owner?.name ?? ""}.${object.name.name}`,
      object
    );
    const matches = byBareName.get(object.name.name);
    if (matches === void 0) byBareName.set(object.name.name, [object]);
    else matches.push(object);
  }
  return { byQualifiedName, byBareName };
}
function containsAggregate(expression, translator) {
  switch (expression.kind) {
    case "call":
      if (translator.isAggregateFunction(expression.name)) return true;
      return expression.args.some((arg) => containsAggregate(arg, translator));
    case "unary":
    case "not":
    case "isNull":
      return containsAggregate(expression.operand, translator);
    case "binary":
      return containsAggregate(expression.left, translator) || containsAggregate(expression.right, translator);
    case "in":
      return containsAggregate(expression.value, translator) || expression.candidates.some(
        (candidate) => containsAggregate(candidate, translator)
      );
    case "like":
      return containsAggregate(expression.value, translator) || containsAggregate(expression.pattern, translator);
    case "case":
      return expression.branches.some(
        (branch) => containsAggregate(branch.when, translator) || containsAggregate(branch.then, translator)
      ) || expression.otherwise !== null && containsAggregate(expression.otherwise, translator);
    case "variantPath":
      return containsAggregate(expression.base, translator);
    case "cast":
      return containsAggregate(expression.operand, translator);
    case "column":
    case "number":
    case "string":
    case "boolean":
    case "null":
      return false;
    default:
      throw new UnreachableCaseError("Unhandled expression node", expression);
  }
}
function carriesCause(tree, cause) {
  const matches = (node) => {
    switch (cause.kind) {
      case "function":
      case "arity":
      case "argument-type":
      case "date-part":
      case "distinct":
      case "star":
        return node.kind === "call" && node.name === cause.name;
      case "column":
        return node.kind === "column" && node.raw === cause.column;
      case "literal":
      case "unparsed":
      case "too-large":
      case "construct":
      case "operator":
      case "simple-case":
      case "aggregate-clause":
      case "unresolved-reference":
      case "no-aggregate":
        return false;
      default:
        throw new UnreachableCaseError("Unhandled refusal", cause);
    }
  };
  if (matches(tree)) return true;
  return subExpressions(tree).some((child) => carriesCause(child, cause));
}
function inlineReferences(expression, ownerAlias, registry, trace, translator) {
  const inlineObject = (object) => {
    if (trace.visited.size >= MAX_INLINE_CHAIN) return null;
    const key = objectKey(object);
    const parse2 = object.expression.parse;
    if (!parse2.ok) return null;
    const parsed = parse2.expression;
    trace.contributors.push({ object, tree: parsed });
    trace.visited.add(key);
    const inlined = inlineReferences(
      parsed,
      object.owner?.name ?? null,
      registry,
      trace,
      translator
    );
    trace.visited.delete(key);
    return inlined;
  };
  switch (expression.kind) {
    case "column": {
      const available = (object) => object === void 0 || trace.visited.has(objectKey(object)) ? void 0 : object;
      if (expression.parts.length === 1) {
        const [name] = expression.parts;
        if (name === void 0) return null;
        if (ownerAlias === null) {
          const matches = registry.byBareName.get(name) ?? [];
          const object2 = matches.length === 1 ? available(matches[0]) : void 0;
          return object2 === void 0 ? null : inlineObject(object2);
        }
        const object = available(
          registry.byQualifiedName.get(`${ownerAlias}.${name}`)
        );
        if (object !== void 0) return inlineObject(object);
        return {
          kind: "column",
          parts: [ownerAlias, name],
          raw: expression.raw
        };
      }
      if (expression.parts.length === 2) {
        const [alias, name] = expression.parts;
        if (alias === void 0 || name === void 0) return null;
        const object = available(
          registry.byQualifiedName.get(`${alias}.${name}`)
        );
        return object === void 0 ? expression : inlineObject(object);
      }
      return null;
    }
    case "unary": {
      const operand = inlineReferences(
        expression.operand,
        ownerAlias,
        registry,
        trace,
        translator
      );
      return operand === null ? null : { ...expression, operand };
    }
    case "binary": {
      const left = inlineReferences(
        expression.left,
        ownerAlias,
        registry,
        trace,
        translator
      );
      if (left === null) return null;
      const right = inlineReferences(
        expression.right,
        ownerAlias,
        registry,
        trace,
        translator
      );
      return right === null ? null : { ...expression, left, right };
    }
    case "call": {
      const datePartArg = translator.datePartArgIndex(expression.name);
      const args = [];
      for (const [index, arg] of expression.args.entries()) {
        if (index === datePartArg) {
          args.push(arg);
          continue;
        }
        const inlined = inlineReferences(
          arg,
          ownerAlias,
          registry,
          trace,
          translator
        );
        if (inlined === null) return null;
        args.push(inlined);
      }
      return { ...expression, args };
    }
    case "not":
    case "isNull": {
      const operand = inlineReferences(
        expression.operand,
        ownerAlias,
        registry,
        trace,
        translator
      );
      return operand === null ? null : { ...expression, operand };
    }
    case "in": {
      const value = inlineReferences(
        expression.value,
        ownerAlias,
        registry,
        trace,
        translator
      );
      if (value === null) return null;
      const candidates = [];
      for (const candidate of expression.candidates) {
        const inlined = inlineReferences(
          candidate,
          ownerAlias,
          registry,
          trace,
          translator
        );
        if (inlined === null) return null;
        candidates.push(inlined);
      }
      return { ...expression, value, candidates };
    }
    case "like": {
      const value = inlineReferences(
        expression.value,
        ownerAlias,
        registry,
        trace,
        translator
      );
      if (value === null) return null;
      const pattern = inlineReferences(
        expression.pattern,
        ownerAlias,
        registry,
        trace,
        translator
      );
      return pattern === null ? null : { ...expression, value, pattern };
    }
    case "case": {
      const branches = [];
      for (const branch of expression.branches) {
        const when = inlineReferences(
          branch.when,
          ownerAlias,
          registry,
          trace,
          translator
        );
        if (when === null) return null;
        const then = inlineReferences(
          branch.then,
          ownerAlias,
          registry,
          trace,
          translator
        );
        if (then === null) return null;
        branches.push({ when, then });
      }
      let otherwise = null;
      if (expression.otherwise !== null) {
        otherwise = inlineReferences(
          expression.otherwise,
          ownerAlias,
          registry,
          trace,
          translator
        );
        if (otherwise === null) return null;
      }
      return { ...expression, branches, otherwise };
    }
    case "variantPath": {
      const base = inlineReferences(
        expression.base,
        ownerAlias,
        registry,
        trace,
        translator
      );
      return base === null ? null : { ...expression, base };
    }
    case "cast": {
      const operand = inlineReferences(
        expression.operand,
        ownerAlias,
        registry,
        trace,
        translator
      );
      return operand === null ? null : { ...expression, operand };
    }
    case "number":
    case "string":
    case "boolean":
    case "null":
      return expression;
    default:
      throw new UnreachableCaseError("Unhandled expression node", expression);
  }
}
function implicitKeyColumns(table) {
  const key = resolveImplicitKey(table);
  return key.kind === "resolved" ? key.columns : [];
}
function bareColumnOf(expression, ownerAlias) {
  if (expression.kind !== "column" || expression.parts.length !== 2)
    return null;
  const [alias, column] = expression.parts;
  return alias === ownerAlias && column !== void 0 ? column : null;
}
function emitSpec(view, options, translator, grain) {
  const warnings = [];
  const elementIds = /* @__PURE__ */ new Set();
  const columnIds = /* @__PURE__ */ new Set();
  const metricIds = /* @__PURE__ */ new Set();
  const relationshipIds = /* @__PURE__ */ new Set();
  const elements = /* @__PURE__ */ new Map();
  for (const table of view.tables) {
    const parts = table.table.parts.map((part) => part.name);
    const path = resolvePath(parts, options);
    const tableName = parts[parts.length - 1] ?? table.alias.name;
    elements.set(table.alias.name, {
      id: claimId(mkElementId(table.alias.name), elementIds),
      alias: table.alias.name,
      tableName,
      path,
      columns: [],
      columnsByPhysical: /* @__PURE__ */ new Map(),
      metrics: [],
      relationships: [],
      uniqueKeyColumnIds: [],
      displayNames: /* @__PURE__ */ new Set(),
      comment: table.comment
    });
  }
  const ensureColumn = (element, physicalName) => {
    const existing = element.columnsByPhysical.get(physicalName);
    if (existing !== void 0) return existing;
    const physicalDisplayName = toDisplayName(physicalName);
    const displayName = claimName(physicalDisplayName, element.displayNames);
    const draft = {
      id: claimId(mkColumnId(element.id, physicalName), columnIds),
      physicalName,
      formula: `[${element.tableName}/${physicalDisplayName}]`,
      displayName,
      renamed: displayName !== physicalDisplayName,
      // No owning fact/dimension: a relationship key or unique-key column the
      // DDL never declared as its own object, so there is no COMMENT to carry.
      comment: null
    };
    element.columns.push(draft);
    element.columnsByPhysical.set(physicalName, draft);
    return draft;
  };
  const pendingRelationships = [];
  const relationshipNames = /* @__PURE__ */ new Set();
  for (const relationship of view.relationships) {
    const sourceElement = elements.get(relationship.sourceAlias.name);
    const targetElement = elements.get(relationship.targetAlias.name);
    if (sourceElement === void 0 || targetElement === void 0) continue;
    const targetTable = view.tables.find(
      (table) => table.alias.name === relationship.targetAlias.name
    );
    const targetColumns = relationship.target.kind === "columns" ? relationship.target.columns.map((column) => column.name) : relationship.target.kind === "implicit" && targetTable !== void 0 ? implicitKeyColumns(targetTable) : [];
    if (targetColumns.length === 0 || targetColumns.length !== relationship.sourceColumns.length) {
      continue;
    }
    const draft = {
      id: claimId(
        mkRelationshipId(sourceElement.id, targetElement.id),
        relationshipIds
      ),
      name: claimName(
        toDisplayName(relationshipName(relationship)),
        relationshipNames
      ),
      targetAlias: targetElement.alias,
      targetElementId: targetElement.id,
      keys: []
    };
    sourceElement.relationships.push(draft);
    pendingRelationships.push({
      relationship,
      sourceElement,
      targetElement,
      targetColumns,
      draft
    });
  }
  const mkResolver = (element) => (parts) => {
    if (parts.length !== 2) return null;
    const [alias, column] = parts;
    if (alias === void 0 || column === void 0) return null;
    if (alias === element.alias) {
      return `[${ensureColumn(element, column).displayName}]`;
    }
    const hops = element.relationships.filter(
      (relationship) => relationship.targetAlias === alias
    );
    const hop = hops.length === 1 ? hops[0] : void 0;
    if (hop === void 0) return null;
    const targetElement = elements.get(alias);
    if (targetElement === void 0) return null;
    const target = ensureColumn(targetElement, column);
    return `[${element.tableName}/${hop.id}/${target.id}]`;
  };
  const resolverCache = /* @__PURE__ */ new Map();
  const resolverFor = (element) => {
    const cached = resolverCache.get(element);
    if (cached !== void 0) return cached;
    const resolver = mkResolver(element);
    resolverCache.set(element, resolver);
    return resolver;
  };
  const mkTypeResolver = () => (parts) => {
    const [alias, column] = parts;
    if (alias === void 0 || column === void 0) return null;
    const element = elements.get(alias);
    if (element === void 0) return null;
    const table = options.columnTypes[element.path.join(".").toUpperCase()];
    return table?.[column.toUpperCase()] ?? null;
  };
  const typeResolver = mkTypeResolver();
  const registry = mkObjectRegistry(view);
  const pendingColumns = [];
  const prepared = /* @__PURE__ */ new Map();
  const traces = /* @__PURE__ */ new Map();
  const skip = (object) => object.visibility === "private";
  const attribution = (object, cause) => {
    const trace = traces.get(object);
    if (trace === void 0 || carriesCause(trace.own, cause)) return void 0;
    const source = trace.contributors.find(
      (contributor) => carriesCause(contributor.tree, cause)
    );
    return source === void 0 ? void 0 : objectRef(source.object);
  };
  const mkExpressionFinding = (object, outcome) => {
    if (outcome.kind === "zoned") {
      return mkFinding(
        "ZONE_DEPENDENT_EXPRESSION",
        objectRef(object),
        object.expression.span.line,
        { snowflakeExpression: object.expression.text, column: outcome.column }
      );
    }
    const via = attribution(object, outcome.cause);
    return mkFinding(
      "EXPRESSION_NOT_REPRESENTABLE",
      objectRef(object),
      object.expression.span.line,
      {
        snowflakeExpression: object.expression.text,
        cause: outcome.cause,
        ...via !== void 0 && { via }
      }
    );
  };
  const prepare = (object) => {
    const attempt = object.expression.parse;
    const parsed = attempt.ok ? attempt.expression : null;
    const trace = {
      visited: /* @__PURE__ */ new Set([objectKey(object)]),
      contributors: []
    };
    const inlined = parsed === null ? null : inlineReferences(
      parsed,
      object.owner?.name ?? null,
      registry,
      trace,
      translator
    );
    if (parsed !== null) {
      traces.set(object, { own: parsed, contributors: trace.contributors });
    }
    if (inlined === null) {
      warnings.push(
        mkExpressionFinding(object, {
          kind: "refused",
          // A parse that failed already said why — an unmodelled construct, an
          // operator with no counterpart, a size bound. Only an inlining failure
          // is this pass's own news.
          cause: attempt.ok ? { kind: "unresolved-reference" } : attempt.refusal
        })
      );
      return null;
    }
    prepared.set(object, inlined);
    return inlined;
  };
  for (const object of [...view.facts, ...view.dimensions]) {
    if (skip(object)) continue;
    const element = object.owner === null ? void 0 : elements.get(object.owner.name);
    if (element === void 0) continue;
    const inlined = prepare(object);
    if (inlined === null) continue;
    if (containsAggregate(inlined, translator)) {
      warnings.push(
        mkFinding(
          "CROSS_TABLE_FACT_AGGREGATE",
          objectRef(object),
          object.expression.span.line,
          {
            foreignTables: referencedQualifiers(inlined).filter(
              (alias) => alias !== element.alias
            )
          }
        )
      );
      prepared.delete(object);
      continue;
    }
    const physicalName = bareColumnOf(inlined, element.alias);
    const objectDisplayName = toDisplayName(object.name.name);
    if (physicalName !== null) {
      const physicalDisplayName = toDisplayName(physicalName);
      const existing = element.columnsByPhysical.get(physicalName);
      if (existing !== void 0) {
        if (existing.displayName === objectDisplayName) continue;
        element.columns.push({
          id: claimId(mkColumnId(element.id, object.name.name), columnIds),
          physicalName: null,
          formula: `[${existing.displayName}]`,
          displayName: claimName(objectDisplayName, element.displayNames),
          renamed: true,
          comment: object.comment
        });
        continue;
      }
      const displayName = claimName(objectDisplayName, element.displayNames);
      const draft2 = {
        id: claimId(mkColumnId(element.id, physicalName), columnIds),
        physicalName,
        formula: `[${element.tableName}/${physicalDisplayName}]`,
        displayName,
        renamed: displayName !== physicalDisplayName,
        comment: object.comment
      };
      element.columns.push(draft2);
      element.columnsByPhysical.set(physicalName, draft2);
      continue;
    }
    const draft = {
      id: claimId(mkColumnId(element.id, object.name.name), columnIds),
      physicalName: null,
      formula: null,
      displayName: claimName(objectDisplayName, element.displayNames),
      renamed: true,
      comment: object.comment
    };
    element.columns.push(draft);
    pendingColumns.push({ object, element, draft });
  }
  for (const pending of pendingRelationships) {
    for (const [
      index,
      sourceColumn
    ] of pending.relationship.sourceColumns.entries()) {
      const targetColumn = pending.targetColumns[index];
      if (targetColumn === void 0) continue;
      pending.draft.keys.push({
        sourceColumnId: ensureColumn(pending.sourceElement, sourceColumn.name).id,
        targetColumnId: ensureColumn(pending.targetElement, targetColumn).id
      });
    }
  }
  for (const table of view.tables) {
    const element = elements.get(table.alias.name);
    const keyColumns = grain.byAlias.get(table.alias.name)?.keyColumns ?? [];
    if (element === void 0 || keyColumns.length === 0) continue;
    element.uniqueKeyColumnIds = keyColumns.map(
      (column) => ensureColumn(element, column).id
    );
  }
  for (const pending of pendingColumns) {
    const inlined = prepared.get(pending.object);
    if (inlined === void 0) continue;
    const result = translator.transpile(
      inlined,
      resolverFor(pending.element),
      typeResolver
    );
    if (!result.ok) {
      warnings.push(
        mkExpressionFinding(pending.object, failureOutcome(result))
      );
      continue;
    }
    pending.draft.formula = result.formula;
    if (result.usedPassthrough) {
      warnings.push(
        mkFinding(
          "EXPRESSION_PASSTHROUGH",
          objectRef(pending.object),
          pending.object.expression.span.line,
          { snowflakeExpression: pending.object.expression.text }
        )
      );
    }
  }
  for (const metric of view.metrics) {
    if (skip(metric)) continue;
    const inlined = prepare(metric);
    if (inlined === null) continue;
    const element = pickMetricElement(metric, inlined, view, elements);
    if (metric.owner === null) {
      warnings.push(
        mkFinding("DERIVED_METRIC", objectRef(metric), metric.span.line)
      );
    }
    if (element === void 0) {
      warnings.push(
        mkExpressionFinding(metric, {
          kind: "refused",
          cause: { kind: "unresolved-reference" }
        })
      );
      continue;
    }
    const result = translator.transpile(
      inlined,
      resolverFor(element),
      typeResolver
    );
    if (!result.ok || !result.hasAggregate) {
      warnings.push(
        mkExpressionFinding(
          metric,
          result.ok ? { kind: "refused", cause: { kind: "no-aggregate" } } : failureOutcome(result)
        )
      );
      continue;
    }
    element.metrics.push({
      id: claimId(mkMetricId(element.id, metric.name.name), metricIds),
      formula: result.formula,
      name: claimName(toDisplayName(metric.name.name), element.displayNames),
      comment: metric.comment
    });
    if (result.usedPassthrough) {
      warnings.push(
        mkFinding(
          "EXPRESSION_PASSTHROUGH",
          objectRef(metric),
          metric.expression.span.line,
          { snowflakeExpression: metric.expression.text }
        )
      );
    }
  }
  const spec = {
    schemaVersion: SCHEMA_VERSION,
    kind: "data-model",
    ...view.comment !== null && { description: view.comment },
    pages: [
      {
        id: PAGE_ID,
        name: PAGE_NAME,
        elements: view.tables.flatMap((table) => {
          const element = elements.get(table.alias.name);
          return element === void 0 ? [] : [serializeElement(element, options)];
        })
      }
    ]
  };
  const seenPaths = /* @__PURE__ */ new Set();
  const physicalTables = [];
  for (const table of view.tables) {
    const element = elements.get(table.alias.name);
    if (element === void 0) continue;
    const key = element.path.join(".");
    if (seenPaths.has(key)) continue;
    seenPaths.add(key);
    physicalTables.push(element.path);
  }
  return { spec: JSON.stringify(spec, null, 2), warnings, physicalTables };
}
function pickMetricElement(metric, inlined, view, elements) {
  if (metric.owner !== null) return elements.get(metric.owner.name);
  const qualifiers = referencedQualifiers(inlined);
  if (qualifiers.length === 0) return void 0;
  for (const table of view.tables) {
    const candidate = elements.get(table.alias.name);
    if (candidate === void 0) continue;
    const reachable = qualifiers.every(
      (qualifier) => qualifier === candidate.alias || candidate.relationships.filter(
        (relationship) => relationship.targetAlias === qualifier
      ).length === 1
    );
    if (reachable) return candidate;
  }
  return void 0;
}
function serializeElement(element, options) {
  const columns = element.columns.filter((column) => column.formula !== null);
  const serialized = {
    id: element.id,
    kind: "table",
    ...element.comment !== null && { description: element.comment },
    source: {
      connectionId: options.connectionId,
      kind: "warehouse-table",
      path: element.path
    },
    columns: columns.map((column) => ({
      id: column.id,
      formula: column.formula,
      ...column.renamed && { name: column.displayName },
      ...column.comment !== null && { description: column.comment }
    })),
    ...element.metrics.length > 0 && {
      metrics: element.metrics.map((metric) => ({
        id: metric.id,
        formula: metric.formula,
        name: metric.name,
        ...metric.comment !== null && { description: metric.comment }
      }))
    },
    ...element.relationships.length > 0 && {
      relationships: element.relationships.map((relationship) => ({
        id: relationship.id,
        targetElementId: relationship.targetElementId,
        keys: relationship.keys,
        name: relationship.name,
        relationshipType: RELATIONSHIP_CARDINALITY
      }))
    },
    ...element.uniqueKeyColumnIds.length > 0 && {
      uniqueKeys: element.uniqueKeyColumnIds
    },
    order: columns.map((column) => column.id)
  };
  return serialized;
}

// .cache/semantic-tools/packages/sigma-data-model/src/write.ts
function write(ir, translator, options) {
  const viewName = ir.name.parts.at(-1)?.name ?? "";
  const grain = deriveGrainMetadata(ir);
  const warnings = collectDroppedMetadata(ir, grain);
  const errors = [];
  const { errors: ruleErrors, blockedObjects } = applyBlockingRules(ir);
  errors.push(...ruleErrors);
  errors.push(...propagateBlockedRelationships(ir, blockedObjects));
  const rejecting = options.bestEffort === true ? errors.filter(
    (error) => BEST_EFFORT_POLICY[error.code] === "always-blocks"
  ) : errors;
  if (rejecting.length > 0) {
    return {
      ok: false,
      errors: sortFindings(errors),
      warnings: sortFindings(warnings),
      // True whether or not `bestEffort` was already set: if it was, and this
      // still refused, an 'always-blocks' code is necessarily among `errors`
      // (that's the only thing `rejecting` can be non-empty from once the
      // flag is on), so the check below is already correctly `false` for
      // "already tried, still can't" — no separate `options.bestEffort`
      // branch needed.
      bestEffortWouldConvert: errors.every(
        (error) => BEST_EFFORT_POLICY[error.code] !== "always-blocks"
      )
    };
  }
  const emitted = emitSpec(ir, options, translator, grain);
  return {
    ok: true,
    overriddenErrors: sortFindings(errors),
    spec: emitted.spec,
    viewName,
    warnings: sortFindings([...warnings, ...emitted.warnings])
  };
}
var SIGMA_DATA_MODEL_WRITER = {
  dialect: "sigma",
  write
};

// .cache/semantic-tools/packages/snowflake-semantic-view/src/lexer.ts
var WORD_CHARS = /[A-Za-z0-9_$]/;
var WHITESPACE = /\s/;
function collapseDoubledDelimiter(body, delimiter) {
  return body.split(delimiter + delimiter).join(delimiter);
}
function scanQuoted(text, open, delimiter) {
  let cursor = open + 1;
  while (cursor < text.length) {
    if (text[cursor] === "\\" && delimiter === "'") {
      cursor += 2;
      continue;
    }
    if (text[cursor] === delimiter) {
      if (text[cursor + 1] === delimiter) {
        cursor += 2;
        continue;
      }
      return { end: cursor + 1, terminated: true };
    }
    cursor++;
  }
  return { end: text.length, terminated: false };
}
function scan(text) {
  const tokens = [];
  const comments = [];
  let index = 0;
  while (index < text.length) {
    const char = text[index] ?? "";
    if (WHITESPACE.test(char)) {
      index++;
      continue;
    }
    if (char === "-" && text[index + 1] === "-") {
      const lineEnd = text.indexOf("\n", index);
      const end = lineEnd === -1 ? text.length : lineEnd;
      comments.push({ start: index, end });
      index = end;
      continue;
    }
    if (char === "/" && text[index + 1] === "*") {
      const close = text.indexOf("*/", index + 2);
      const end = close === -1 ? text.length : close + 2;
      comments.push({ start: index, end });
      index = end;
      continue;
    }
    if (char === "'" || char === '"') {
      const run = scanQuoted(text, index, char);
      const raw = text.slice(index, run.end);
      const body = run.terminated ? raw.slice(1, -1) : raw.slice(1);
      tokens.push({
        kind: char === '"' ? "quoted" : "string",
        value: collapseDoubledDelimiter(body, char),
        text: raw,
        start: index,
        end: run.end,
        terminated: run.terminated
      });
      index = run.end;
      continue;
    }
    if (WORD_CHARS.test(char)) {
      let end = index;
      while (end < text.length && WORD_CHARS.test(text[end] ?? "")) end++;
      const raw = text.slice(index, end);
      tokens.push({
        kind: "word",
        value: raw.toUpperCase(),
        text: raw,
        start: index,
        end,
        terminated: true
      });
      index = end;
      continue;
    }
    tokens.push({
      kind: "punctuation",
      value: char,
      text: char,
      start: index,
      end: index + 1,
      terminated: true
    });
    index++;
  }
  return { tokens, comments };
}

// .cache/semantic-tools/packages/snowflake-semantic-view/src/sqlText.ts
function maskDdl(text, scanned) {
  const { tokens, comments } = scanned;
  const out = text.split("");
  const blank = (from, until) => {
    for (let i = from; i < until; i++) {
      if (out[i] !== "\n") out[i] = " ";
    }
  };
  for (const comment of comments) blank(comment.start, comment.end);
  for (const token of tokens) {
    if (token.kind !== "string" && token.kind !== "quoted") continue;
    blank(token.start + 1, token.terminated ? token.end - 1 : token.end);
  }
  return out.join("");
}
function mkSource(text) {
  const scanned = scan(text);
  const mask = maskDdl(text, scanned);
  const lineStarts = [0];
  for (let i = 0; i < text.length; i++) {
    if (text[i] === "\n") lineStarts.push(i + 1);
  }
  return {
    text,
    mask,
    tokens: scanned.tokens,
    lineAt(offset) {
      let low = 0;
      let high = lineStarts.length - 1;
      while (low < high) {
        const mid = low + high + 1 >> 1;
        if ((lineStarts[mid] ?? 0) <= offset) low = mid;
        else high = mid - 1;
      }
      return low + 1;
    }
  };
}

// .cache/semantic-tools/packages/snowflake-semantic-view/src/parse.ts
var ParseError_t = class extends Error {
  offset;
  constructor(message, offset) {
    super(message);
    this.offset = offset;
  }
};
function offsetOf(ctx, index) {
  return ctx.tokens[index]?.start ?? ctx.source.text.length;
}
function mkSpan(ctx, range) {
  const start = offsetOf(ctx, range.start);
  const last = range.end > range.start ? ctx.tokens[range.end - 1] : void 0;
  return {
    start,
    end: last === void 0 ? start : last.end,
    line: ctx.source.lineAt(start)
  };
}
function isWord(token, value) {
  return token !== void 0 && token.kind === "word" && token.value === value;
}
function isPunctuation(token, value) {
  return token !== void 0 && token.kind === "punctuation" && token.value === value;
}
function matchWords(ctx, from, limit, words) {
  if (words.length === 0 || from + words.length > limit) return -1;
  for (const [offset, word] of words.entries()) {
    if (!isWord(ctx.tokens[from + offset], word)) return -1;
  }
  return from + words.length;
}
function matchAny(ctx, from, limit, spellings) {
  for (const words of spellings) {
    const after = matchWords(ctx, from, limit, words);
    if (after !== -1) return after;
  }
  return -1;
}
function matchParen(ctx, open, limit) {
  let depth = 0;
  for (let i = open; i < limit; i++) {
    if (isPunctuation(ctx.tokens[i], "(")) depth++;
    else if (isPunctuation(ctx.tokens[i], ")")) {
      depth--;
      if (depth === 0) return i;
    }
  }
  return -1;
}
function findAtDepth0(ctx, from, limit, predicate) {
  let depth = 0;
  for (let i = from; i < limit; i++) {
    if (depth === 0 && predicate(i)) return i;
    if (isPunctuation(ctx.tokens[i], "(")) depth++;
    else if (isPunctuation(ctx.tokens[i], ")")) depth--;
  }
  return -1;
}
function splitTopLevel(ctx, from, limit) {
  const items = [];
  let depth = 0;
  let start = from;
  for (let i = from; i < limit; i++) {
    if (isPunctuation(ctx.tokens[i], "(")) depth++;
    else if (isPunctuation(ctx.tokens[i], ")")) depth--;
    else if (depth === 0 && isPunctuation(ctx.tokens[i], ",")) {
      if (start < i) items.push({ start, end: i });
      start = i + 1;
    }
  }
  if (start < limit) items.push({ start, end: limit });
  return items;
}
function readIdentifier(ctx, from, limit) {
  const token = from < limit ? ctx.tokens[from] : void 0;
  if (token === void 0) {
    throw new ParseError_t("expected an identifier", offsetOf(ctx, from));
  }
  if (token.kind === "quoted") {
    if (!token.terminated) {
      throw new ParseError_t("unterminated quoted identifier", token.start);
    }
    return { value: { name: token.value, raw: token.text }, end: from + 1 };
  }
  if (token.kind !== "word") {
    throw new ParseError_t("expected an identifier", token.start);
  }
  return { value: { name: token.value, raw: token.text }, end: from + 1 };
}
function readQualifiedName(ctx, from, limit) {
  const parts = [];
  let pos = from;
  for (; ; ) {
    const part = readIdentifier(ctx, pos, limit);
    parts.push(part.value);
    pos = part.end;
    if (pos >= limit || !isPunctuation(ctx.tokens[pos], ".")) break;
    pos++;
  }
  const first = ctx.tokens[from];
  const last = ctx.tokens[pos - 1];
  const raw = first === void 0 || last === void 0 ? "" : ctx.source.text.slice(first.start, last.end);
  return { value: { parts, raw }, end: pos };
}
function flatten(name) {
  return {
    name: name.parts.map((part) => part.name).join("."),
    raw: name.raw
  };
}
function readParenRange(ctx, from, limit) {
  if (from >= limit || !isPunctuation(ctx.tokens[from], "(")) {
    throw new ParseError_t("expected '('", offsetOf(ctx, from));
  }
  const close = matchParen(ctx, from, limit);
  if (close === -1) {
    throw new ParseError_t("unbalanced '('", offsetOf(ctx, from));
  }
  return { value: { start: from + 1, end: close }, end: close + 1 };
}
function readColumnList(ctx, from, limit) {
  const paren = readParenRange(ctx, from, limit);
  const columns = splitTopLevel(ctx, paren.value.start, paren.value.end).map(
    (item) => readIdentifier(ctx, item.start, item.end).value
  );
  return { value: columns, end: paren.end };
}
function readStringLiteral(ctx, from, limit) {
  const token = from < limit ? ctx.tokens[from] : void 0;
  if (token === void 0 || token.kind !== "string") {
    throw new ParseError_t("expected a string literal", offsetOf(ctx, from));
  }
  if (!token.terminated) {
    throw new ParseError_t("unterminated string literal", token.start);
  }
  return from + 1;
}
function readParenItemCount(ctx, from, limit) {
  let pos = from;
  if (pos < limit && isPunctuation(ctx.tokens[pos], "=")) pos++;
  const paren = readParenRange(ctx, pos, limit);
  return {
    value: splitTopLevel(ctx, paren.value.start, paren.value.end).length,
    end: paren.end
  };
}
function readRangeConstraint(ctx, from, limit) {
  let pos = from;
  let name = null;
  if (matchWords(ctx, pos, limit, ["DISTINCT"]) === -1) {
    const read2 = readIdentifier(ctx, pos, limit);
    name = read2.value;
    pos = read2.end;
  }
  const afterRange = matchWords(ctx, pos, limit, [
    "DISTINCT",
    "RANGE",
    "BETWEEN"
  ]);
  if (afterRange === -1) {
    throw new ParseError_t(
      "expected DISTINCT RANGE BETWEEN",
      offsetOf(ctx, pos)
    );
  }
  const startColumn = readIdentifier(ctx, afterRange, limit);
  const afterAnd = matchWords(ctx, startColumn.end, limit, ["AND"]);
  if (afterAnd === -1) {
    throw new ParseError_t(
      "expected AND in range constraint",
      offsetOf(ctx, startColumn.end)
    );
  }
  const endColumn = readIdentifier(ctx, afterAnd, limit);
  const afterExclusive = matchWords(ctx, endColumn.end, limit, ["EXCLUSIVE"]);
  if (afterExclusive === -1) {
    throw new ParseError_t(
      "expected EXCLUSIVE in range constraint",
      offsetOf(ctx, endColumn.end)
    );
  }
  return {
    value: { name, startColumn: startColumn.value, endColumn: endColumn.value },
    end: afterExclusive
  };
}
function keyword(...spellings) {
  return (ctx, from, limit) => matchAny(ctx, from, limit, spellings);
}
function keywordList(...spellings) {
  return (ctx, from, limit) => {
    const after = matchAny(ctx, from, limit, spellings);
    if (after === -1) return -1;
    const paren = isPunctuation(ctx.tokens[after], "=") ? after + 1 : after;
    return isPunctuation(ctx.tokens[paren], "(") ? paren : -1;
  };
}
function endsExpression(token) {
  if (token === void 0) return false;
  if (token.kind === "punctuation") return token.value === ")";
  return true;
}
function bareKeyword(...spellings) {
  return (ctx, from, limit) => endsExpression(ctx.tokens[from - 1]) ? matchAny(ctx, from, limit, spellings) : -1;
}
function assigned(...spellings) {
  return (ctx, from, limit) => {
    const after = matchAny(ctx, from, limit, spellings);
    if (after === -1 || !isPunctuation(ctx.tokens[after], "=")) return -1;
    return after + 1;
  };
}
function clauseAt(ctx, from, limit, clauses) {
  for (const clause of clauses) {
    const after = clause.match(ctx, from, limit);
    if (after !== -1) return { clause, after };
  }
  return null;
}
function clauseStart(ctx, from, limit, clauses) {
  const found = findAtDepth0(
    ctx,
    from,
    limit,
    (index) => clauseAt(ctx, index, limit, clauses) !== null
  );
  return found === -1 ? limit : found;
}
function readClauses(ctx, from, limit, clauses, target, diagnostic) {
  let pos = from;
  while (pos < limit) {
    const found = clauseAt(ctx, pos, limit, clauses);
    if (found === null) {
      throw new ParseError_t(diagnostic, offsetOf(ctx, pos));
    }
    pos = found.clause.read(ctx, found.after, limit, target);
  }
}
var TABLE_CLAUSES = [
  {
    match: keyword(["PRIMARY", "KEY"]),
    read: (ctx, after, limit, target) => {
      const read2 = readColumnList(ctx, after, limit);
      target.primaryKey = read2.value;
      return read2.end;
    }
  },
  {
    match: keywordList(["UNIQUE"]),
    read: (ctx, after, limit, target) => {
      const read2 = readColumnList(ctx, after, limit);
      target.uniqueKeys.push(read2.value);
      return read2.end;
    }
  },
  {
    match: keyword(["CONSTRAINT"]),
    read: (ctx, after, limit, target) => {
      const read2 = readRangeConstraint(ctx, after, limit);
      target.rangeConstraint = read2.value;
      return read2.end;
    }
  },
  {
    match: keyword(["WITH", "SYNONYMS"]),
    read: (ctx, after, limit, target) => {
      const read2 = readParenItemCount(ctx, after, limit);
      target.synonymCount += read2.value;
      return read2.end;
    }
  },
  {
    match: assigned(["COMMENT"]),
    read: (ctx, after, limit, target) => {
      const token = after < limit ? ctx.tokens[after] : void 0;
      target.comment = token?.kind === "string" ? token.value : null;
      return readStringLiteral(ctx, after, limit);
    }
  },
  {
    match: keywordList(["WITH", "TAG"], ["TAG"]),
    read: (ctx, after, limit, target) => {
      const read2 = readParenItemCount(ctx, after, limit);
      target.tagCount += read2.value;
      return read2.end;
    }
  }
];
function parseLogicalTable(ctx, range) {
  const headerEnd = clauseStart(ctx, range.start, range.end, TABLE_CLAUSES);
  const asKeyword = findAtDepth0(
    ctx,
    range.start,
    headerEnd,
    (index) => isWord(ctx.tokens[index], "AS")
  );
  let alias;
  let table;
  if (asKeyword === -1) {
    table = readQualifiedName(ctx, range.start, headerEnd).value;
    const last = table.parts[table.parts.length - 1];
    if (last === void 0) {
      throw new ParseError_t(
        "expected a table name",
        offsetOf(ctx, range.start)
      );
    }
    alias = last;
  } else {
    alias = readIdentifier(ctx, range.start, asKeyword).value;
    table = readQualifiedName(ctx, asKeyword + 1, headerEnd).value;
  }
  const clauses = {
    primaryKey: null,
    uniqueKeys: [],
    rangeConstraint: null,
    synonymCount: 0,
    comment: null,
    tagCount: 0
  };
  readClauses(
    ctx,
    headerEnd,
    range.end,
    TABLE_CLAUSES,
    clauses,
    "unrecognized clause in logical table"
  );
  return { alias, table, ...clauses, span: mkSpan(ctx, range) };
}
function parseRelationshipTarget(ctx, range) {
  const asof = findAtDepth0(
    ctx,
    range.start,
    range.end,
    (index) => isWord(ctx.tokens[index], "ASOF")
  );
  const between = findAtDepth0(
    ctx,
    range.start,
    range.end,
    (index) => isWord(ctx.tokens[index], "BETWEEN")
  );
  if (between !== -1) {
    const startColumn = readIdentifier(ctx, between + 1, range.end);
    const afterAnd = matchWords(ctx, startColumn.end, range.end, ["AND"]);
    if (afterAnd === -1) {
      throw new ParseError_t(
        "expected AND in range relationship",
        offsetOf(ctx, startColumn.end)
      );
    }
    const endColumn = readIdentifier(ctx, afterAnd, range.end);
    return {
      kind: "range",
      startColumn: startColumn.value,
      endColumn: endColumn.value
    };
  }
  const columns = [];
  for (const item of splitTopLevel(ctx, range.start, range.end)) {
    let pos = item.start;
    const afterAsof = matchWords(ctx, pos, item.end, ["ASOF"]);
    if (afterAsof !== -1) pos = afterAsof;
    columns.push(readIdentifier(ctx, pos, item.end).value);
  }
  return asof === -1 ? { kind: "columns", columns } : { kind: "asof", columns };
}
function parseRelationship(ctx, range) {
  const open = findAtDepth0(
    ctx,
    range.start,
    range.end,
    (index) => isPunctuation(ctx.tokens[index], "(")
  );
  if (open === -1) {
    throw new ParseError_t(
      "expected a source column list",
      offsetOf(ctx, range.start)
    );
  }
  const asKeyword = findAtDepth0(
    ctx,
    range.start,
    open,
    (index) => isWord(ctx.tokens[index], "AS")
  );
  let name = null;
  let aliasStart = range.start;
  if (asKeyword !== -1) {
    name = readIdentifier(ctx, range.start, asKeyword).value;
    aliasStart = asKeyword + 1;
  }
  const sourceAlias = readIdentifier(ctx, aliasStart, open);
  const sourceColumns = readColumnList(ctx, sourceAlias.end, range.end);
  const afterReferences = matchWords(ctx, sourceColumns.end, range.end, [
    "REFERENCES"
  ]);
  if (afterReferences === -1) {
    throw new ParseError_t(
      "expected REFERENCES",
      offsetOf(ctx, sourceColumns.end)
    );
  }
  const targetAlias = readIdentifier(ctx, afterReferences, range.end);
  let target = { kind: "implicit" };
  if (targetAlias.end < range.end && isPunctuation(ctx.tokens[targetAlias.end], "(")) {
    const paren = readParenRange(ctx, targetAlias.end, range.end);
    target = parseRelationshipTarget(ctx, paren.value);
  }
  return {
    name,
    sourceAlias: sourceAlias.value,
    sourceColumns: sourceColumns.value,
    targetAlias: targetAlias.value,
    target,
    span: mkSpan(ctx, range)
  };
}
var LABELS_CLAUSE = assigned(["LABELS"]);
var OBJECT_CLAUSES = [
  {
    match: keyword(["WITH", "SYNONYMS"]),
    read: (ctx, after, limit, target) => {
      const read2 = readParenItemCount(ctx, after, limit);
      target.synonymCount += read2.value;
      return read2.end;
    }
  },
  {
    match: keyword(["WITH", "CORTEX", "SEARCH", "SERVICE"]),
    read: (ctx, after, limit, target) => {
      target.cortexSearchCount++;
      const service = readQualifiedName(ctx, after, limit);
      const afterUsing = matchWords(ctx, service.end, limit, ["USING"]);
      return afterUsing === -1 ? service.end : readIdentifier(ctx, afterUsing, limit).end;
    }
  },
  {
    match: assigned(["COMMENT"]),
    read: (ctx, after, limit, target) => {
      const token = after < limit ? ctx.tokens[after] : void 0;
      target.comment = token?.kind === "string" ? token.value : null;
      return readStringLiteral(ctx, after, limit);
    }
  },
  {
    match: keywordList(["WITH", "TAG"], ["TAG"]),
    read: (ctx, after, limit, target) => {
      const read2 = readParenItemCount(ctx, after, limit);
      target.tagCount += read2.value;
      return read2.end;
    }
  },
  {
    match: keywordList(["WITH", "SAMPLE_VALUES"], ["SAMPLE_VALUES"]),
    read: (ctx, after, limit, target) => {
      const read2 = readParenItemCount(ctx, after, limit);
      target.sampleValueCount += read2.value;
      return read2.end;
    }
  },
  {
    // The only clause with no argument list, so what precedes it is the whole
    // of what tells it from a column: after a dot or an operator the expression
    // is still running and the word belongs to it.
    match: bareKeyword(["IS_ENUM"]),
    read: (_ctx, after, _limit, target) => {
      target.isEnum = true;
      return after;
    }
  }
];
function parseExpressionObject(ctx, range, kind) {
  let pos = range.start;
  let visibility = "public";
  if (matchWords(ctx, pos, range.end, ["PRIVATE"]) !== -1) {
    visibility = "private";
    pos++;
  } else if (matchWords(ctx, pos, range.end, ["PUBLIC"]) !== -1) {
    pos++;
  }
  const qualified = readQualifiedName(ctx, pos, range.end);
  const parts = qualified.value.parts;
  const last = parts[parts.length - 1];
  if (last === void 0) {
    throw new ParseError_t("expected an object name", offsetOf(ctx, pos));
  }
  const owner = parts.length > 1 ? parts[parts.length - 2] ?? null : null;
  pos = qualified.end;
  const asKeyword = findAtDepth0(
    ctx,
    pos,
    range.end,
    (index) => isWord(ctx.tokens[index], "AS")
  );
  if (asKeyword === -1) {
    throw new ParseError_t(
      "expected AS before the expression",
      offsetOf(ctx, pos)
    );
  }
  let filterLabel = false;
  let using = null;
  let nonAdditiveBy = null;
  while (pos < asKeyword) {
    const afterLabels = LABELS_CLAUSE(ctx, pos, asKeyword);
    if (afterLabels !== -1) {
      const paren = readParenRange(ctx, afterLabels, asKeyword);
      filterLabel = findAtDepth0(
        ctx,
        paren.value.start,
        paren.value.end,
        (index) => isWord(ctx.tokens[index], "FILTER")
      ) !== -1;
      pos = paren.end;
      continue;
    }
    const afterUsing = matchWords(ctx, pos, asKeyword, ["USING"]);
    if (afterUsing !== -1) {
      const paren = readParenRange(ctx, afterUsing, asKeyword);
      using = splitTopLevel(ctx, paren.value.start, paren.value.end).map(
        (item) => readIdentifier(ctx, item.start, item.end).value
      );
      pos = paren.end;
      continue;
    }
    const afterNonAdditive = matchWords(ctx, pos, asKeyword, [
      "NON",
      "ADDITIVE",
      "BY"
    ]);
    if (afterNonAdditive !== -1) {
      const paren = readParenRange(ctx, afterNonAdditive, asKeyword);
      nonAdditiveBy = splitTopLevel(
        ctx,
        paren.value.start,
        paren.value.end
      ).map(
        (item) => flatten(readQualifiedName(ctx, item.start, item.end).value)
      );
      pos = paren.end;
      continue;
    }
    throw new ParseError_t("unrecognized clause before AS", offsetOf(ctx, pos));
  }
  const expressionStart = asKeyword + 1;
  const expressionEnd = clauseStart(
    ctx,
    expressionStart + 1,
    range.end,
    OBJECT_CLAUSES
  );
  if (expressionStart >= expressionEnd) {
    throw new ParseError_t(
      "empty expression after AS",
      offsetOf(ctx, expressionStart)
    );
  }
  const expressionSpan = mkSpan(ctx, {
    start: expressionStart,
    end: expressionEnd
  });
  const clauses = {
    synonymCount: 0,
    comment: null,
    tagCount: 0,
    cortexSearchCount: 0,
    sampleValueCount: 0,
    isEnum: false
  };
  readClauses(
    ctx,
    expressionEnd,
    range.end,
    OBJECT_CLAUSES,
    clauses,
    "unrecognized clause after the expression"
  );
  return {
    kind,
    visibility,
    owner,
    name: last,
    filterLabel,
    using,
    nonAdditiveBy,
    // Sliced from the original text rather than rebuilt from the tokens, so the
    // expression the converter reads is exactly what the author wrote.
    expression: ctx.source.text.slice(expressionSpan.start, expressionSpan.end),
    expressionSpan,
    ...clauses,
    span: mkSpan(ctx, range)
  };
}
var EXPRESSION_SECTIONS = [
  ["FACTS", "fact", "facts"],
  ["DIMENSIONS", "dimension", "dimensions"],
  ["METRICS", "metric", "metrics"]
];
function consumeExpressionClause(ctx, from, limit, view) {
  for (const [word, kind, target] of EXPRESSION_SECTIONS) {
    const after = matchWords(ctx, from, limit, [word]);
    if (after === -1) continue;
    const paren = readParenRange(ctx, after, limit);
    for (const item of splitTopLevel(ctx, paren.value.start, paren.value.end)) {
      view[target].push(parseExpressionObject(ctx, item, kind));
    }
    return paren.end;
  }
  return -1;
}
var VIEW_CLAUSES = [
  {
    match: assigned(["COMMENT"]),
    read: (ctx, after, limit, view) => {
      const token = after < limit ? ctx.tokens[after] : void 0;
      view.comment = token?.kind === "string" ? token.value : null;
      return readStringLiteral(ctx, after, limit);
    }
  },
  {
    match: assigned(["MAX_STALENESS"]),
    read: (ctx, after, limit, view) => {
      view.hasMaxStaleness = true;
      const token = after < limit ? ctx.tokens[after] : void 0;
      if (token === void 0 || token.kind !== "word" || !/^[0-9]+$/.test(token.value)) {
        throw new ParseError_t(
          "expected a MAX_STALENESS value",
          offsetOf(ctx, after)
        );
      }
      return after + 1;
    }
  },
  {
    match: keyword(["AI_SQL_GENERATION"], ["AI_QUESTION_CATEGORIZATION"]),
    read: (ctx, after, limit, view) => {
      view.hasAiMetadata = true;
      return readStringLiteral(ctx, after, limit);
    }
  },
  {
    // Undocumented in the CREATE SEMANTIC VIEW grammar but emitted by GET_DDL: a
    // blob of Cortex Analyst metadata, chiefly per-column sample values.
    match: keyword(["WITH", "EXTENSION"], ["EXTENSION"]),
    read: (ctx, after, limit, view) => {
      view.hasAiMetadata = true;
      return readParenRange(ctx, after, limit).end;
    }
  },
  {
    match: keyword(["AI_VERIFIED_QUERIES"]),
    read: (ctx, after, limit, view) => {
      const paren = readParenRange(ctx, after, limit);
      view.verifiedQueryCount += splitTopLevel(
        ctx,
        paren.value.start,
        paren.value.end
      ).length;
      return paren.end;
    }
  },
  {
    match: keyword(["COPY", "GRANTS"]),
    read: (_ctx, after, _limit, view) => {
      view.hasCopyGrants = true;
      return after;
    }
  },
  {
    match: keywordList(["WITH", "TAG"], ["TAG"]),
    read: (ctx, after, limit, view) => {
      const paren = readParenRange(ctx, after, limit);
      view.tagCount += splitTopLevel(
        ctx,
        paren.value.start,
        paren.value.end
      ).length;
      return paren.end;
    }
  }
];
var STATEMENT_HEADERS = [
  ["CREATE", "OR", "REPLACE", "SEMANTIC", "VIEW", "IF", "NOT", "EXISTS"],
  ["CREATE", "OR", "REPLACE", "SEMANTIC", "VIEW"],
  ["CREATE", "SEMANTIC", "VIEW", "IF", "NOT", "EXISTS"],
  ["CREATE", "SEMANTIC", "VIEW"]
];
function statementOffsets(source) {
  const ctx = { source, tokens: source.tokens };
  const offsets = [];
  for (let i = 0; i < ctx.tokens.length; i++) {
    if (matchAny(ctx, i, ctx.tokens.length, STATEMENT_HEADERS) !== -1) {
      offsets.push(offsetOf(ctx, i));
    }
  }
  return offsets;
}
function splitStatements(ddl) {
  const source = mkSource(ddl);
  const offsets = statementOffsets(source);
  if (offsets.length <= 1) return offsets.length === 0 ? [] : [ddl];
  return offsets.map((start, index) => {
    const searchLimit = offsets[index + 1] ?? ddl.length;
    let end = searchLimit;
    for (const token of source.tokens) {
      if (token.start < start) continue;
      if (token.start >= searchLimit) break;
      if (token.kind === "punctuation" && token.value === ";") {
        end = token.end;
        break;
      }
    }
    return ddl.slice(start, end).trim();
  });
}
function parseView(ctx) {
  const limit = ctx.tokens.length;
  let statementStart = -1;
  let afterHeader = -1;
  for (let i = 0; i < limit; i++) {
    const after = matchAny(ctx, i, limit, STATEMENT_HEADERS);
    if (after !== -1) {
      statementStart = i;
      afterHeader = after;
      break;
    }
  }
  if (statementStart === -1) {
    throw new ParseError_t("no CREATE SEMANTIC VIEW statement found", 0);
  }
  const name = readQualifiedName(ctx, afterHeader, limit);
  const view = {
    name: name.value,
    tables: [],
    relationships: [],
    facts: [],
    dimensions: [],
    metrics: [],
    comment: null,
    tagCount: 0,
    hasMaxStaleness: false,
    hasAiMetadata: false,
    verifiedQueryCount: 0,
    hasCopyGrants: false,
    span: {
      start: offsetOf(ctx, statementStart),
      end: ctx.source.text.length,
      line: ctx.source.lineAt(offsetOf(ctx, statementStart))
    }
  };
  let sawTables = false;
  let pos = name.end;
  while (pos < limit) {
    if (isPunctuation(ctx.tokens[pos], ";")) break;
    const afterTables = matchWords(ctx, pos, limit, ["TABLES"]);
    if (afterTables !== -1) {
      const paren = readParenRange(ctx, afterTables, limit);
      for (const item of splitTopLevel(
        ctx,
        paren.value.start,
        paren.value.end
      )) {
        view.tables.push(parseLogicalTable(ctx, item));
      }
      sawTables = true;
      pos = paren.end;
      continue;
    }
    const afterRelationships = matchWords(ctx, pos, limit, ["RELATIONSHIPS"]);
    if (afterRelationships !== -1) {
      const paren = readParenRange(ctx, afterRelationships, limit);
      for (const item of splitTopLevel(
        ctx,
        paren.value.start,
        paren.value.end
      )) {
        view.relationships.push(parseRelationship(ctx, item));
      }
      pos = paren.end;
      continue;
    }
    const afterExpressions = consumeExpressionClause(ctx, pos, limit, view);
    if (afterExpressions !== -1) {
      pos = afterExpressions;
      continue;
    }
    const found = clauseAt(ctx, pos, limit, VIEW_CLAUSES);
    if (found !== null) {
      pos = found.clause.read(ctx, found.after, limit, view);
      continue;
    }
    throw new ParseError_t(
      "unrecognized clause in semantic view",
      offsetOf(ctx, pos)
    );
  }
  if (!sawTables) {
    throw new ParseError_t("expected a TABLES clause", offsetOf(ctx, pos));
  }
  return view;
}
function parseSemanticView(source) {
  try {
    return {
      ok: true,
      view: parseView({ source, tokens: source.tokens })
    };
  } catch (error) {
    if (error instanceof ParseError_t) {
      return { ok: false, diagnostic: error.message, offset: error.offset };
    }
    throw error;
  }
}

// .cache/semantic-tools/packages/snowflake-semantic-view/src/grain.ts
function uniqueKeyNames2(table) {
  return table.uniqueKeys.map((key) => key.map((column) => column.name));
}
function resolveImplicitKey2(table) {
  const primaryKey = table.primaryKey?.map((column) => column.name);
  if (primaryKey !== void 0)
    return { kind: "resolved", columns: primaryKey };
  const uniques = uniqueKeyNames2(table);
  if (uniques.length === 0) return { kind: "none" };
  if (uniques.length > 1) {
    return { kind: "ambiguous", uniqueCount: uniques.length };
  }
  return { kind: "resolved", columns: uniques[0] ?? [] };
}

// .cache/semantic-tools/packages/snowflake-semantic-view/src/validate.ts
function relationshipName2(relationship) {
  return relationship.name?.name ?? `${relationship.sourceAlias.name}_TO_${relationship.targetAlias.name}`;
}
function validateStructure(view) {
  const findings = [];
  const aliases = new Map(view.tables.map((table) => [table.alias.name, table]));
  const fail = (name, line, diagnostic, kind) => {
    findings.push(
      mkFinding("MALFORMED_DDL", { kind, name }, line, {
        diagnostic
      })
    );
  };
  for (const relationship of view.relationships) {
    const name = relationshipName2(relationship);
    const sourceTable = aliases.get(relationship.sourceAlias.name);
    const targetTable = aliases.get(relationship.targetAlias.name);
    if (sourceTable === void 0) {
      fail(
        name,
        relationship.span.line,
        `relationship references undeclared table alias ${relationship.sourceAlias.name}`,
        "relationship"
      );
      continue;
    }
    if (targetTable === void 0) {
      fail(
        name,
        relationship.span.line,
        `relationship references undeclared table alias ${relationship.targetAlias.name}`,
        "relationship"
      );
      continue;
    }
    if (relationship.target.kind === "asof" || relationship.target.kind === "range") {
      continue;
    }
    let targetColumns;
    if (relationship.target.kind === "columns") {
      targetColumns = relationship.target.columns.map((column) => column.name);
    } else {
      const implicit = resolveImplicitKey2(targetTable);
      if (implicit.kind !== "resolved") {
        fail(
          name,
          relationship.span.line,
          `REFERENCES ${relationship.targetAlias.name} omits a column list and that table declares ${implicit.kind === "none" ? "no key" : `${implicit.uniqueCount} UNIQUE constraints and no PRIMARY KEY, so the referenced key is ambiguous`}`,
          "relationship"
        );
        continue;
      }
      targetColumns = implicit.columns;
    }
    if (targetColumns.length !== relationship.sourceColumns.length) {
      fail(
        name,
        relationship.span.line,
        `relationship joins ${relationship.sourceColumns.length} source column(s) to ${targetColumns.length} target column(s)`,
        "relationship"
      );
    }
  }
  for (const object of [...view.facts, ...view.dimensions, ...view.metrics]) {
    if (object.owner === null) continue;
    if (aliases.has(object.owner.name)) continue;
    fail(
      object.name.name,
      object.span.line,
      `${object.kind} is qualified with undeclared table alias ${object.owner.name}`,
      object.kind
    );
  }
  return findings;
}

// .cache/semantic-tools/packages/sql-expression/src/keywords.ts
var KEYWORDS = /* @__PURE__ */ new Set([
  // Boolean connectives.
  "AND",
  "OR",
  "NOT",
  // IS [NOT] NULL / IS [NOT] DISTINCT FROM.
  "IS",
  "DISTINCT",
  "FROM",
  // [NOT] BETWEEN, [NOT] IN, [NOT] LIKE / ILIKE. Not RLIKE or ESCAPE — see
  // the module comment above.
  "BETWEEN",
  "IN",
  "LIKE",
  "ILIKE",
  // CASE, both simple and searched.
  "CASE",
  "WHEN",
  "THEN",
  "ELSE",
  "END",
  // CAST(x AS t). `x::t` is an operator (`::`), not a keyword.
  "CAST",
  "AS",
  // Each always refused regardless of internal shape — see
  // `REFUSED_KEYWORD_CONSTRUCTS` in `parser.ts`. Not `SUBSTRING`/`SUBSTR`:
  // those are name-dispatched like `TRY_CAST`, since the oracle accepts
  // `SUBSTRING(s, 1, 2)` as an ordinary call and only refuses the
  // `FROM`/`FOR` form.
  "EXTRACT",
  "TRIM",
  "POSITION",
  "OVERLAY",
  "VALUES",
  // `SUBSTRING(s FROM 1 FOR 2)`'s keywords. `FROM` is already listed above.
  "FOR",
  // Recognized so a window clause can be refused by name rather than
  // misparsed as a second top-level expression; contents are not modelled.
  "OVER",
  "PARTITION",
  "BY",
  "ORDER",
  "ASC",
  "DESC",
  "NULLS",
  "FIRST",
  "LAST",
  "ROWS",
  "RANGE",
  "GROUPS",
  "PRECEDING",
  "FOLLOWING",
  "UNBOUNDED",
  "CURRENT",
  "ROW",
  // Aggregate-modifying clauses that must be recognized to be refused
  // (`{ kind: 'aggregate-clause' }`) rather than misparsed.
  "FILTER",
  "WITHIN",
  "GROUP"
]);

// .cache/semantic-tools/packages/sql-expression/src/lexer.ts
var IDENTIFIER_QUOTE = '"';
var MULTI_CHAR_OPERATORS = ["||", "::", "!=", ">=", "<=", "=>", "<>"];
var ESCAPES = {
  n: "\n",
  t: "	",
  r: "\r",
  b: "\b",
  f: "\f",
  "\\": "\\"
};
var LexError = class extends Error {
};
var Lexer = class {
  source;
  cursor = 0;
  tokens = [];
  constructor(source) {
    this.source = source;
  }
  tokenize() {
    while (!this.isAtEnd()) {
      this.scanToken();
    }
    this.push("EOF", "", "", this.cursor);
    return this.tokens;
  }
  scanToken() {
    this.skipTrivia();
    if (this.isAtEnd()) return;
    const start = this.cursor;
    const char = this.advance();
    if (char === IDENTIFIER_QUOTE) {
      this.scanQuotedIdentifier(start);
      return;
    }
    if (char === "'") {
      this.scanStringLiteral(start);
      return;
    }
    if (char === "$" && this.isDollarQuoteStart()) {
      this.scanDollarQuotedString(start);
      return;
    }
    if (this.isDigit(char)) {
      this.scanNumber(start);
      return;
    }
    if (char === "." && this.isDigit(this.peek())) {
      const prevChar = start > 0 ? this.source[start - 1] : void 0;
      if (prevChar !== ".") {
        this.scanNumber(start);
        return;
      }
    }
    if ((char === "$" || char === ":") && (this.isAlpha(this.peek()) || this.isDigit(this.peek()))) {
      const prevChar = start > 0 ? this.source[start - 1] : void 0;
      const precededBySpace = prevChar === void 0 || /\s/.test(prevChar);
      if (!(char === ":" && !precededBySpace)) {
        this.scanParameter(start);
        return;
      }
    }
    if (this.isAlpha(char)) {
      this.scanIdentifier(start);
      return;
    }
    this.scanOperatorOrPunctuation(char, start);
  }
  // --- Scanners ---
  scanQuotedIdentifier(start) {
    let value = "";
    let closed = false;
    while (!this.isAtEnd()) {
      if (this.peek() === IDENTIFIER_QUOTE) {
        if (this.peekNext() === IDENTIFIER_QUOTE) {
          this.advance();
          this.advance();
          value += IDENTIFIER_QUOTE;
          continue;
        }
        this.advance();
        closed = true;
        break;
      }
      value += this.advance();
    }
    if (!closed) throw new LexError("Unclosed quoted identifier");
    this.push("IDENTIFIER", this.source.slice(start, this.cursor), value, start);
  }
  scanStringLiteral(start) {
    let value = "";
    let closed = false;
    while (!this.isAtEnd()) {
      const char = this.peek();
      if (char === "\\") {
        this.advance();
        if (this.isAtEnd()) break;
        const escaped = this.advance();
        value += ESCAPES[escaped] ?? escaped;
        continue;
      }
      if (char === "'") {
        if (this.peekNext() === "'") {
          this.advance();
          this.advance();
          value += "'";
          continue;
        }
        this.advance();
        closed = true;
        break;
      }
      value += this.advance();
    }
    if (!closed) throw new LexError("Unclosed string literal");
    this.push("STRING", this.source.slice(start, this.cursor), value, start);
  }
  scanDollarQuotedString(start) {
    let tag = "$";
    while (!this.isAtEnd() && this.peek() !== "$") tag += this.advance();
    if (this.isAtEnd()) throw new LexError("Unclosed dollar-quote tag");
    tag += this.advance();
    let value = "";
    let closed = false;
    while (!this.isAtEnd()) {
      if (this.source.startsWith(tag, this.cursor)) {
        this.cursor += tag.length;
        closed = true;
        break;
      }
      value += this.advance();
    }
    if (!closed) throw new LexError("Unclosed dollar-quoted string");
    this.push("STRING", this.source.slice(start, this.cursor), value, start);
  }
  scanParameter(start) {
    while (this.isAlphaNumeric(this.peek())) this.advance();
    const text = this.source.slice(start, this.cursor);
    this.push("PARAMETER", text, text, start);
  }
  /**
   * `SqlExpr_t`'s `number` node carries only exact source `text` — see
   * `sqlExpr.ts` — so there is no int/float distinction to track here, only
   * the exact span to consume.
   */
  scanNumber(start) {
    if (this.source[start] === "0" && this.peek().toLowerCase() === "x") {
      this.advance();
      while (this.isHexDigit(this.peek())) this.advance();
      const text2 = this.source.slice(start, this.cursor);
      this.push("NUMBER", text2, text2, start);
      return;
    }
    this.consumeDigitRun();
    if (this.peek() === "." && this.peekNext() !== ".") {
      this.advance();
      this.consumeDigitRun();
    }
    if (this.peek().toLowerCase() === "e") {
      const next = this.peekNext();
      const isScientific = this.isDigit(next) || (next === "+" || next === "-") && this.isDigit(this.source[this.cursor + 2] ?? "");
      if (isScientific) {
        this.advance();
        if (this.peek() === "+" || this.peek() === "-") this.advance();
        this.consumeDigitRun();
      }
    }
    const text = this.source.slice(start, this.cursor);
    this.push("NUMBER", text, text, start);
  }
  /** A run of decimal digits, allowing a single `_` between two digits. */
  consumeDigitRun() {
    while (this.isDigit(this.peek()) || this.peek() === "_" && this.isDigit(this.peekNext())) {
      this.advance();
    }
  }
  scanIdentifier(start) {
    while (this.isAlphaNumeric(this.peek())) this.advance();
    const text = this.source.slice(start, this.cursor);
    const upper = text.toUpperCase();
    let type = "IDENTIFIER";
    if (upper === "TRUE" || upper === "FALSE") type = "BOOLEAN";
    else if (upper === "NULL") type = "NULL";
    else if (KEYWORDS.has(upper)) type = "KEYWORD";
    this.push(type, text, upper, start);
  }
  scanOperatorOrPunctuation(char, start) {
    const twoChar = char + (this.peek() === "\0" ? "" : this.peek());
    if (MULTI_CHAR_OPERATORS.includes(twoChar)) {
      this.advance();
      this.push("OPERATOR", twoChar, twoChar, start);
      return;
    }
    switch (char) {
      case "(":
      case ")":
      case ",":
      case ";":
      case "[":
      case "]":
      case "{":
      case "}":
      case ".":
        this.push("PUNCTUATION", char, char, start);
        return;
      case "=":
      case ">":
      case "<":
      case "+":
      case "-":
      case "*":
      case "/":
      case "%":
      case "!":
      case ":":
      case "?":
      case "@":
      case "|":
      case "&":
        this.push("OPERATOR", char, char, start);
        return;
      default:
        throw new LexError(`Unexpected character: ${char}`);
    }
  }
  // --- Trivia ---
  skipTrivia() {
    for (; ; ) {
      const char = this.peek();
      if (char === " " || char === "	" || char === "\r" || char === "\n") {
        this.advance();
        continue;
      }
      if (char === "-" && this.peekNext() === "-") {
        while (this.peek() !== "\n" && !this.isAtEnd()) this.advance();
        continue;
      }
      if (char === "/" && this.peekNext() === "*") {
        this.advance();
        this.advance();
        let closed = false;
        while (!this.isAtEnd()) {
          if (this.peek() === "*" && this.peekNext() === "/") {
            this.advance();
            this.advance();
            closed = true;
            break;
          }
          this.advance();
        }
        if (!closed) throw new LexError("Unclosed block comment");
        continue;
      }
      return;
    }
  }
  // --- Character helpers ---
  push(type, text, value, start) {
    this.tokens.push({ type, text, value, start, end: this.cursor });
  }
  advance() {
    return this.source[this.cursor++];
  }
  peek() {
    return this.isAtEnd() ? "\0" : this.source[this.cursor];
  }
  peekNext() {
    return this.cursor + 1 >= this.source.length ? "\0" : this.source[this.cursor + 1];
  }
  isAtEnd() {
    return this.cursor >= this.source.length;
  }
  isDigit(c) {
    return c >= "0" && c <= "9";
  }
  isHexDigit(c) {
    return this.isDigit(c) || c >= "a" && c <= "f" || c >= "A" && c <= "F";
  }
  isAlpha(c) {
    return /\p{L}|_/u.test(c);
  }
  isAlphaNumeric(c) {
    return /[\p{L}\p{N}_$]/u.test(c);
  }
  isDollarQuoteStart() {
    let i = this.cursor;
    while (i < this.source.length) {
      const c = this.source[i];
      if (c === "$") return true;
      if (!this.isAlphaNumeric(c)) return false;
      i++;
    }
    return false;
  }
};

// .cache/semantic-tools/packages/sql-expression/src/parser.ts
var BINARY_OPERATOR_BY_TOKEN = {
  "+": "+",
  "-": "-",
  "*": "*",
  "/": "/",
  "=": "=",
  "!=": "!=",
  "<>": "!=",
  "<": "<",
  "<=": "<=",
  ">": ">",
  ">=": ">=",
  AND: "and",
  OR: "or",
  "||": "||"
};
var INFIX_PRECEDENCE = {
  OR: 20 /* OR */,
  AND: 30 /* AND */,
  NOT: 40 /* NOT */,
  "=": 50 /* COMPARISON */,
  "!=": 50 /* COMPARISON */,
  "<>": 50 /* COMPARISON */,
  "<": 50 /* COMPARISON */,
  "<=": 50 /* COMPARISON */,
  ">": 50 /* COMPARISON */,
  ">=": 50 /* COMPARISON */,
  LIKE: 50 /* COMPARISON */,
  ILIKE: 50 /* COMPARISON */,
  IS: 50 /* COMPARISON */,
  IN: 50 /* COMPARISON */,
  BETWEEN: 50 /* COMPARISON */,
  "+": 60 /* SUM */,
  "-": 60 /* SUM */,
  "||": 60 /* SUM */,
  "*": 70 /* PRODUCT */,
  "/": 70 /* PRODUCT */,
  "%": 70 /* PRODUCT */,
  // Recognized so they refuse by name (the generic unmapped-operator
  // fallback) instead of as a leftover-token `unparsed`. Verified against
  // the oracle. `<<`/`>>`/`**`/`^` are deliberately absent: Snowflake's own
  // dialect doesn't tokenize them as these two-character operators at all
  // (`<<` lexes as two `<` tokens, `**` as two `*` tokens), and `^` isn't a
  // valid character in Snowflake's lexer's own grammar — the oracle's lexer
  // throws *uncaught* for it, a shared limitation of both parsers' contracts
  // for genuinely unrecognized input, not a divergence to fix.
  "&": 70 /* PRODUCT */,
  "|": 70 /* PRODUCT */,
  OVER: 110 /* OVER */,
  "::": 120 /* DOT */,
  ".": 120 /* DOT */,
  ":": 120 /* DOT */,
  "[": 120 /* DOT */
};
var REFUSED_KEYWORD_CONSTRUCTS = {
  EXTRACT: "extract",
  TRIM: "trim",
  POSITION: "position",
  OVERLAY: "overlay",
  VALUES: "values"
};
var CASTABLE_FIXED_NUMBER_TYPES = /* @__PURE__ */ new Set([
  "NUMBER",
  "DECIMAL",
  "NUMERIC",
  "INT",
  "INTEGER",
  "BIGINT",
  "SMALLINT",
  "TINYINT",
  "BYTEINT"
]);
var CASTABLE_FLOAT_TYPES = /* @__PURE__ */ new Set([
  "FLOAT",
  "FLOAT4",
  "FLOAT8",
  "DOUBLE",
  "DOUBLE PRECISION",
  "REAL"
]);
var ParseError = class extends Error {
};
var NULL_PLACEHOLDER = { kind: "null" };
var IDENTIFIER_QUOTE2 = '"';
var SAFE_PATH_KEY = /^[A-Za-z_][A-Za-z0-9_]*$/;
var Parser = class {
  source;
  tokens;
  cursor = 0;
  pendingRefusal;
  /**
   * A per-instance copy of `INFIX_PRECEDENCE`, mutable so `parseBracketAccess`
   * can temporarily un-register `:` while parsing a slice's first bound —
   * mirroring `exprParser.ts`, which does the same to its own
   * `infixPrecedence` map for exactly this reason, per instance rather than
   * globally, so nothing else observes the temporary change.
   */
  infixPrecedence;
  constructor(source, tokens) {
    this.source = source;
    this.tokens = tokens;
    this.infixPrecedence = { ...INFIX_PRECEDENCE };
  }
  get refusal() {
    return this.pendingRefusal;
  }
  isAtEnd() {
    return this.peek().type === "EOF";
  }
  /**
   * Records a refusal, unconditionally replacing whatever was already
   * pending. Parsing continues (rather than throwing) so a later genuine
   * syntax error elsewhere in the same text is still found — matching the
   * source's two-phase design, where the general parser validates the
   * *whole* input before the separate lowering pass ever refuses anything.
   *
   * Unconditional overwrite matches the source's own priority for a
   * construct that refuses regardless of what it wraps (`cast`, `between`,
   * `window`, an unmapped operator, …): in `lowerSqlExpression.ts`, these are
   * checked against the node's own type *before* ever recursing into its
   * children, so whichever such construct is syntactically outermost always
   * wins over anything nested inside it — found empirically (`SUM(x) OVER
   * (...)::INT` refuses as `cast`, not `window`; `CAST(x AS INT).y` refuses
   * as `qualified reference`, not `cast`), not assumed.
   *
   * This alone would wrongly let a *later, unrelated* sibling (a function's
   * second argument, say) override an *earlier* sibling's already-decided
   * refusal — the source's `for (arg of args) { if (!ok) return }` keeps the
   * first failing argument, not the last. `parseSlot` is what protects
   * siblings from each other; every call site that calls `refuse` on one of
   * several independently-evaluated elements must parse that element with
   * `parseSlot`, not `parseExpression`, or this method's overwrite is wrong.
   */
  refuse(refusal) {
    this.pendingRefusal = refusal;
    return NULL_PLACEHOLDER;
  }
  /**
   * Parses one of several independently-evaluated siblings — a function
   * argument, an `IN` candidate, a `CASE` branch, a binary operator's
   * right-hand side. If an earlier sibling already recorded a refusal, this
   * one is still parsed in full, so a later genuine syntax error is still
   * caught, but nothing found while parsing it — including a wrapping
   * construct's own `refuse` — may replace that earlier refusal. Verified
   * against the oracle (`FUNC(y % 2, CAST(w[0] AS INT))` refuses as `%`, the
   * first argument's failure, never reaching the second).
   */
  parseSlot(minPrecedence = 0 /* LOWEST */) {
    if (this.pendingRefusal === void 0) {
      return this.parseExpression(minPrecedence);
    }
    const before = this.pendingRefusal;
    const node = this.parseExpression(minPrecedence);
    this.pendingRefusal = before;
    return node;
  }
  // --- Token stream helpers ---
  peek() {
    return this.tokens[this.cursor];
  }
  peekValue() {
    return this.peek().value.toUpperCase();
  }
  advance() {
    const token = this.tokens[this.cursor];
    if (token.type !== "EOF") this.cursor++;
    return token;
  }
  check(type, value) {
    const token = this.peek();
    if (token.type !== type) return false;
    return value === void 0 || token.value.toUpperCase() === value;
  }
  /** For an identifier whose *resolved* text names a construct, e.g. `TRY_CAST`. */
  checkIdentifierNamed(name) {
    return this.check("IDENTIFIER") && this.peekValue() === name;
  }
  match(type, value) {
    if (!this.check(type, value)) return false;
    this.advance();
    return true;
  }
  expect(type, value, what) {
    if (!this.check(type, value)) {
      throw new ParseError(`Expected ${what}`);
    }
    return this.advance();
  }
  getPrecedence() {
    const token = this.peek();
    if (token.type !== "OPERATOR" && token.type !== "KEYWORD" && token.type !== "PUNCTUATION") {
      return 0 /* LOWEST */;
    }
    return this.infixPrecedence[token.value.toUpperCase()] ?? 0 /* LOWEST */;
  }
  /** Consumes a balanced `( ... )`, the cursor already having consumed neither paren. */
  skipBalancedParens() {
    this.skipBalanced("(", ")");
  }
  /** Consumes a balanced pair of punctuation, neither having been consumed yet. */
  skipBalanced(open, close) {
    this.expect("PUNCTUATION", open, `'${open}'`);
    let depth = 1;
    while (depth > 0) {
      if (this.isAtEnd()) throw new ParseError(`Unmatched '${open}'`);
      const token = this.advance();
      if (token.type === "PUNCTUATION" && token.value === open) depth++;
      else if (token.type === "PUNCTUATION" && token.value === close) depth--;
    }
  }
  // --- Entry point ---
  parseExpression(minPrecedence = 0 /* LOWEST */) {
    let left = this.parsePrefix();
    while (minPrecedence < this.getPrecedence()) {
      const token = this.advance();
      left = this.parseInfix(left, token);
    }
    return left;
  }
  // --- Prefix (primary) parselets ---
  parsePrefix() {
    const token = this.peek();
    if (token.type === "NUMBER") {
      this.advance();
      return { kind: "number", text: token.text };
    }
    if (token.type === "STRING") {
      this.advance();
      return { kind: "string", value: token.value };
    }
    if (token.type === "BOOLEAN") {
      this.advance();
      return { kind: "boolean", value: token.value === "TRUE" };
    }
    if (token.type === "NULL") {
      this.advance();
      return { kind: "null" };
    }
    if (token.type === "PUNCTUATION" && token.value === "(") {
      this.advance();
      return this.parseParenthesized();
    }
    if (token.type === "OPERATOR" && (token.value === "-" || token.value === "+")) {
      this.advance();
      const operand = this.parseExpression(90 /* PREFIX */);
      return { kind: "unary", operator: token.value, operand };
    }
    if (token.type === "OPERATOR" && token.value === "*") {
      this.advance();
      return this.refuse({ kind: "construct", construct: "star" });
    }
    if (token.type === "PUNCTUATION" && token.value === "[") {
      return this.parseArrayLiteral();
    }
    if (token.type === "PUNCTUATION" && token.value === "{") {
      return this.parseStructLiteral();
    }
    if (token.type === "PARAMETER") {
      this.advance();
      if (this.check("PUNCTUATION", "(")) {
        return this.parseInvalidFunctionName();
      }
      return this.refuse({ kind: "construct", construct: "non-column identifier" });
    }
    if (token.type === "KEYWORD" && token.value.toUpperCase() === "NOT") {
      this.advance();
      const operand = this.parseExpression(40 /* NOT */);
      return { kind: "not", operand };
    }
    if (token.type === "KEYWORD" && token.value.toUpperCase() === "CASE") {
      this.advance();
      return this.parseCase();
    }
    if (token.type === "KEYWORD" && token.value.toUpperCase() === "CAST") {
      this.advance();
      return this.parseCastLike();
    }
    if (token.type === "KEYWORD" && REFUSED_KEYWORD_CONSTRUCTS[token.value.toUpperCase()] !== void 0) {
      this.advance();
      return this.parseRefusedKeywordConstruct(token.value.toUpperCase());
    }
    if (token.type === "IDENTIFIER") {
      this.advance();
      return this.parseIdentifierPrimary(token);
    }
    throw new ParseError(`Unexpected token '${token.text}'`);
  }
  /**
   * `(expr)` is transparent; `(expr, expr, ...)` is a `tuple`, verified
   * against the oracle (`(1, 2)` refuses as `tuple`, distinct from a plain
   * grouping paren).
   */
  parseParenthesized() {
    const inner = this.parseExpression(0 /* LOWEST */);
    if (this.match("PUNCTUATION", ",")) {
      do {
        this.parseExpression(0 /* LOWEST */);
      } while (this.match("PUNCTUATION", ","));
      this.expect("PUNCTUATION", ")", "')'");
      return this.refuse({ kind: "construct", construct: "tuple" });
    }
    this.expect("PUNCTUATION", ")", "')'");
    return inner;
  }
  /** `[1, 2, 3]` — an array literal, verified against the oracle as `array`. */
  parseArrayLiteral() {
    this.expect("PUNCTUATION", "[", "'['");
    if (!this.check("PUNCTUATION", "]")) {
      do {
        this.parseExpression(0 /* LOWEST */);
      } while (this.match("PUNCTUATION", ","));
    }
    this.expect("PUNCTUATION", "]", "']'");
    return this.refuse({ kind: "construct", construct: "array" });
  }
  /** `{'a': 1}` — a struct/object literal, verified against the oracle as `struct`. */
  parseStructLiteral() {
    this.skipBalanced("{", "}");
    return this.refuse({ kind: "construct", construct: "struct" });
  }
  /** `EXTRACT`, `TRIM`, `POSITION`, `OVERLAY`, `VALUES` — see `REFUSED_KEYWORD_CONSTRUCTS`. */
  parseRefusedKeywordConstruct(keyword2) {
    this.skipBalancedParens();
    if (keyword2 === "VALUES") {
      while (this.match("PUNCTUATION", ",")) {
        this.skipBalancedParens();
      }
    }
    const construct = REFUSED_KEYWORD_CONSTRUCTS[keyword2];
    if (construct === void 0) throw new ParseError(`Unknown construct ${keyword2}`);
    return this.refuse({ kind: "construct", construct });
  }
  /**
   * An identifier already consumed: a column reference (possibly qualified,
   * `a.b.c`), or a call if `(` follows.
   *
   * A qualified chain is consumed entirely here, in one loop over `.`, rather
   * than through the generic infix dispatch for `.` — `column.raw` needs the
   * exact source span of the *whole* reference, and this is the one place
   * that still holds the first token's offset without having to reconstruct
   * it later. `parseDot` below handles the residual case: a `.` following
   * something that is not a plain identifier chain, which `columnParts` in
   * the source refuses the same way.
   */
  parseIdentifierPrimary(token) {
    const upper = token.value.toUpperCase();
    if (this.check("PUNCTUATION", "(")) {
      if (upper === "TRY_CAST") return this.parseTryCastLike();
      if (upper === "SUBSTRING" || upper === "SUBSTR") {
        return this.parseSubstringLike(token);
      }
      return this.parseFunctionCall(token);
    }
    if (this.check("STRING") || this.check("NUMBER")) {
      this.advance();
      return this.refuse({ kind: "construct", construct: "cast" });
    }
    const parts = [this.resolveIdentifier(token)];
    let end = token.end;
    for (; ; ) {
      const savedCursor = this.cursor;
      if (!this.match("PUNCTUATION", ".")) break;
      if (this.check("OPERATOR", "*")) {
        this.advance();
        return this.refuse({ kind: "construct", construct: "star" });
      }
      if (!this.check("IDENTIFIER")) {
        this.cursor = savedCursor;
        break;
      }
      const nameToken = this.advance();
      parts.push(this.resolveIdentifier(nameToken));
      end = nameToken.end;
    }
    return { kind: "column", parts, raw: this.source.slice(token.start, end) };
  }
  /** `SUBSTRING(s, 1, 2)` (ordinary call) vs `SUBSTRING(s FROM 1 FOR 2)` (refused). */
  parseSubstringLike(nameToken) {
    this.expect("PUNCTUATION", "(", "'('");
    const first = this.parseSlot();
    if (this.match("KEYWORD", "FROM")) {
      this.parseExpression(0 /* LOWEST */);
      if (this.match("KEYWORD", "FOR")) {
        this.parseExpression(0 /* LOWEST */);
      }
      this.expect("PUNCTUATION", ")", "')'");
      return this.refuse({ kind: "construct", construct: "substring" });
    }
    const args = [first];
    while (this.match("PUNCTUATION", ",")) {
      args.push(this.parseSlot());
    }
    this.expect("PUNCTUATION", ")", "')'");
    const name = this.resolveIdentifier(nameToken);
    return this.parseCallSuffixes({ kind: "call", name, distinct: false, star: false, args });
  }
  resolveIdentifier(token) {
    return token.value;
  }
  // --- Function calls ---
  /** A call-shaped expression whose "name" isn't a valid function identifier. */
  parseInvalidFunctionName() {
    this.skipBalancedParens();
    return this.refuse({ kind: "construct", construct: "function name" });
  }
  parseFunctionCall(nameToken) {
    this.expect("PUNCTUATION", "(", "'('");
    const name = this.resolveIdentifier(nameToken);
    const distinct = this.match("KEYWORD", "DISTINCT");
    if (this.check("OPERATOR", "*")) {
      this.advance();
      this.expect("PUNCTUATION", ")", "')'");
      return { kind: "call", name, distinct, star: true, args: [] };
    }
    const qualifiedStarStart = this.cursor;
    if (this.match("IDENTIFIER") && this.match("PUNCTUATION", ".") && this.match("OPERATOR", "*")) {
      if (this.check("PUNCTUATION", ")")) {
        this.advance();
        return this.refuse({ kind: "construct", construct: "qualified star" });
      }
    }
    this.cursor = qualifiedStarStart;
    const args = [];
    if (!this.check("PUNCTUATION", ")")) {
      do {
        args.push(this.parseSlot());
      } while (this.match("PUNCTUATION", ","));
    }
    let orderByPresent = false;
    if (this.match("KEYWORD", "ORDER")) {
      orderByPresent = true;
      this.expect("KEYWORD", "BY", "'BY'");
      do {
        this.parseExpression(0 /* LOWEST */);
        if (!this.match("KEYWORD", "ASC")) this.match("KEYWORD", "DESC");
        if (this.match("KEYWORD", "NULLS")) {
          if (!this.match("KEYWORD", "FIRST")) this.match("KEYWORD", "LAST");
        }
      } while (this.match("PUNCTUATION", ","));
    }
    this.expect("PUNCTUATION", ")", "')'");
    let result = { kind: "call", name, distinct, star: false, args };
    if (orderByPresent) {
      result = this.refuse({ kind: "aggregate-clause", clause: "ORDER BY" });
    }
    result = this.parseCallSuffixes(result);
    return result;
  }
  /**
   * `WITHIN GROUP (...)`, `FILTER (...)`, `RESPECT/IGNORE NULLS`, and
   * `OVER (...)` — every clause `lowerSqlExpression.ts` refuses under
   * `{ kind: 'aggregate-clause' }` plus the window clause, all of which
   * follow a completed call. Consumed with a balanced-paren skip rather than
   * modelled internally, since their contents are refused unread either way.
   */
  parseCallSuffixes(result) {
    if (this.match("KEYWORD", "WITHIN")) {
      this.expect("KEYWORD", "GROUP", "'GROUP'");
      this.skipBalancedParens();
      result = this.refuse({ kind: "aggregate-clause", clause: "WITHIN GROUP" });
    }
    if (this.match("KEYWORD", "FILTER")) {
      this.skipBalancedParens();
      result = this.refuse({ kind: "aggregate-clause", clause: "FILTER" });
    }
    let hasNullHandling = false;
    if (this.checkIdentifierNamed("RESPECT") || this.checkIdentifierNamed("IGNORE")) {
      this.advance();
      this.expect("KEYWORD", "NULLS", "'NULLS'");
      hasNullHandling = true;
    }
    if (this.match("KEYWORD", "OVER")) {
      this.skipBalancedParens();
      return this.refuse({ kind: "construct", construct: "window" });
    }
    if (hasNullHandling) {
      return this.refuse({ kind: "aggregate-clause", clause: "null handling" });
    }
    return result;
  }
  /** `TRY_CAST(expr AS type)` — recognized by name, not by keyword-token type. */
  parseTryCastLike() {
    this.advance();
    this.parseExpression(0 /* LOWEST */);
    this.expect("KEYWORD", "AS", "'AS'");
    this.skipDataType();
    this.expect("PUNCTUATION", ")", "')'");
    return this.refuse({ kind: "construct", construct: "cast" });
  }
  /** `CAST(expr AS type)` — the `CAST` keyword already consumed. */
  parseCastLike() {
    this.expect("PUNCTUATION", "(", "'('");
    this.parseExpression(0 /* LOWEST */);
    this.expect("KEYWORD", "AS", "'AS'");
    this.skipDataType();
    this.expect("PUNCTUATION", ")", "')'");
    return this.refuse({ kind: "construct", construct: "cast" });
  }
  /** A data type name, optionally with `(precision[, scale])` — contents unread. */
  skipDataType() {
    if (!this.check("IDENTIFIER") && !this.check("KEYWORD")) {
      throw new ParseError("Expected a data type");
    }
    this.advance();
    while (this.check("IDENTIFIER") || this.check("KEYWORD")) {
      this.advance();
    }
    if (this.check("PUNCTUATION", "(")) {
      this.skipBalancedParens();
    }
  }
  // --- CASE ---
  parseCase() {
    const hasSubject = !this.check("KEYWORD", "WHEN");
    if (hasSubject) {
      this.parseExpression(0 /* LOWEST */);
    }
    const branches = [];
    while (this.match("KEYWORD", "WHEN")) {
      const when = this.parseSlot();
      this.expect("KEYWORD", "THEN", "'THEN'");
      const then = this.parseSlot();
      branches.push({ when, then });
    }
    let otherwise = null;
    if (this.match("KEYWORD", "ELSE")) {
      otherwise = this.parseSlot();
    }
    this.expect("KEYWORD", "END", "'END'");
    if (hasSubject) return this.refuse({ kind: "simple-case" });
    return { kind: "case", branches, otherwise };
  }
  // --- Infix parselets ---
  parseInfix(left, token) {
    const value = token.value.toUpperCase();
    if (value === ".") return this.parseDot();
    if (value === "::") return this.parseCastOperator(left);
    if (value === ":") return this.parseColonAccess(left);
    if (value === "[") return this.parseBracketAccess();
    if (value === "IN") return this.parseIn(left, false);
    if (value === "BETWEEN") return this.parseBetween();
    if (value === "LIKE" || value === "ILIKE") return this.parseLike(left, value === "ILIKE", false);
    if (value === "IS") return this.parseIs(left);
    if (value === "NOT") return this.parseNotInfix(left);
    if (value === "OVER") {
      if (left.kind !== "call") {
        throw new ParseError("'OVER' may only follow a function call");
      }
      this.cursor--;
      return this.parseCallSuffixes(left);
    }
    const precedence = this.infixPrecedence[value] ?? 0 /* LOWEST */;
    const operator = BINARY_OPERATOR_BY_TOKEN[value];
    if (operator === void 0) {
      this.parseExpression(precedence);
      return this.refuse({ kind: "operator", operator: token.value });
    }
    const right = this.parseSlot(precedence);
    return { kind: "binary", operator, left, right };
  }
  /**
   * A `.` reaching the generic infix dispatch means the left side was not a
   * plain identifier chain — that case is fully handled inside
   * `parseIdentifierPrimary`. Matches `columnParts` in the source: only a
   * chain of plain names starting from an identifier is a qualified
   * reference at all; anything else is refused.
   */
  parseDot() {
    if (this.check("OPERATOR", "*")) {
      this.advance();
      return this.refuse({ kind: "construct", construct: "star" });
    }
    if (!this.isAtEnd()) this.advance();
    return this.refuse({ kind: "construct", construct: "qualified reference" });
  }
  /**
   * `expr::type` — the one cast shape this parser models, and only for a
   * target `readCastTarget` recognizes (see there). `CAST(...)`,
   * `TRY_CAST(...)`, and the typed-literal shape (`DATE '2024-01-01'`) are
   * out of scope and keep refusing unconditionally, via `parseCastLike`/
   * `parseTryCastLike`/`parseIdentifierPrimary`'s own `skipDataType` calls —
   * this method's target-reading logic is deliberately not shared with
   * those, so they cannot accidentally start modelling something too.
   */
  parseCastOperator(left) {
    const target = this.readCastTarget();
    if (target === null) {
      return this.refuse({ kind: "construct", construct: "cast" });
    }
    return { kind: "cast", operand: left, target };
  }
  /**
   * Reads `::`'s target type, returning the `CastTarget_t` it names when
   * Sigma has a verified, non-approximating equivalent — otherwise null,
   * having still consumed the same grammar so the caller can refuse exactly
   * as `skipDataType` would have.
   *
   * `NUMBER`/`DECIMAL`/`NUMERIC` with an explicit `(precision[, scale])` are
   * deliberately refused (return null) rather than guessed at: Snowflake's
   * fixed-point family defaults to scale 0 (and so *rounds*, measured live)
   * only when no precision is given, and this method does not read the
   * scale, so it cannot tell an explicit `(38, 0)` — identical to the bare
   * default — from an explicit `(10, 2)`, which would round differently.
   * `FLOAT`/`DOUBLE`/`REAL` never take a precision in Snowflake to begin
   * with, so no such ambiguity exists for them.
   */
  readCastTarget() {
    if (!this.check("IDENTIFIER") && !this.check("KEYWORD")) {
      throw new ParseError("Expected a data type");
    }
    const words = [this.advance().value.toUpperCase()];
    while (this.check("IDENTIFIER") || this.check("KEYWORD")) {
      words.push(this.advance().value.toUpperCase());
    }
    const hasPrecision = this.check("PUNCTUATION", "(");
    if (hasPrecision) this.skipBalancedParens();
    const name = words.join(" ");
    if (name === "BOOLEAN") return "boolean";
    if (CASTABLE_FLOAT_TYPES.has(name)) return "floatNumber";
    if (CASTABLE_FIXED_NUMBER_TYPES.has(name) && !hasPrecision) return "fixedNumber";
    return null;
  }
  /**
   * `col:field` — a bind-parameter-shaped colon that the lexer resolved to
   * the path-access operator (see `lexer.ts`'s space-preceded-parameter
   * rule). Unlike every other construct this parser recognizes, this one
   * *is* modelled: Sigma has its own VARIANT/JSON path-access formula syntax
   * (`[Column].field`), verified live against a real Sigma org, so this
   * diverges deliberately from the oracle parser this package is otherwise
   * characterized against (`lowerSqlExpression.ts`'s `BINARY_OPERATORS` has
   * no entry for `:` either — see `DIVERGENCES.md`).
   *
   * Only when the key is shaped like a bare identifier: Sigma's dot-path
   * syntax has no documented escaping for a key that isn't one, so a quoted
   * identifier or string literal containing anything else (a space, a
   * symbol, a leading digit) stays refused as the plain unmapped `:`
   * operator, exactly as before this method modelled anything.
   *
   * The key's *case* is read from the token's raw text/value, not through
   * `resolveIdentifier` — Snowflake's path-access keys are matched against
   * the VARIANT's literal JSON keys case-sensitively, unlike an ordinary
   * identifier, which Snowflake upper-cases before resolving.
   */
  parseColonAccess(left) {
    const token = this.peek();
    if (!this.match("IDENTIFIER") && !this.match("STRING")) {
      throw new ParseError("Expected a path key after ':'");
    }
    const key = this.resolvePathKey(token);
    if (!SAFE_PATH_KEY.test(key)) {
      return this.refuse({ kind: "operator", operator: ":" });
    }
    return { kind: "variantPath", base: left, key };
  }
  /** The path key's literal, case-preserved text — see `parseColonAccess`. */
  resolvePathKey(token) {
    if (token.type === "STRING") return token.value;
    return token.text.startsWith(IDENTIFIER_QUOTE2) ? token.value : token.text;
  }
  /**
   * `col[0]` vs `col[0:2]` — verified against the oracle as two different
   * refusals: a single index is the unmapped operator `[]`, but a slice
   * (colon present) is the named construct `slice`.
   */
  parseBracketAccess() {
    const savedColonPrecedence = this.infixPrecedence[":"];
    delete this.infixPrecedence[":"];
    try {
      this.parseExpression(0 /* LOWEST */);
    } finally {
      this.infixPrecedence[":"] = savedColonPrecedence;
    }
    if (this.match("OPERATOR", ":")) {
      if (!this.check("PUNCTUATION", "]")) {
        this.parseExpression(0 /* LOWEST */);
      }
      this.expect("PUNCTUATION", "]", "']'");
      return this.refuse({ kind: "construct", construct: "slice" });
    }
    this.expect("PUNCTUATION", "]", "']'");
    return this.refuse({ kind: "operator", operator: "[]" });
  }
  parseIn(left, negated) {
    this.expect("PUNCTUATION", "(", "'('");
    const candidates = [];
    if (!this.check("PUNCTUATION", ")")) {
      do {
        candidates.push(this.parseSlot());
      } while (this.match("PUNCTUATION", ","));
    }
    this.expect("PUNCTUATION", ")", "')'");
    return { kind: "in", value: left, candidates, negated };
  }
  // `negated` (`NOT BETWEEN` vs `BETWEEN`) is intentionally not threaded
  // through: `lowerSqlExpression.ts` refuses `between` as one generic
  // `{ kind: 'construct' }`, the same way regardless of negation.
  parseBetween() {
    this.parseExpression(30 /* AND */ + 1);
    this.expect("KEYWORD", "AND", "'AND'");
    this.parseExpression(50 /* COMPARISON */);
    return this.refuse({ kind: "construct", construct: "between" });
  }
  parseLike(left, caseInsensitive, negated) {
    const pattern = this.parseSlot(50 /* COMPARISON */);
    return { kind: "like", value: left, pattern, caseInsensitive, negated };
  }
  parseIs(left) {
    let operator = "IS";
    if (this.match("KEYWORD", "NOT")) {
      operator = "IS NOT";
      if (this.match("KEYWORD", "DISTINCT")) {
        this.expect("KEYWORD", "FROM", "'FROM'");
        operator = "IS NOT DISTINCT FROM";
      }
    } else if (this.match("KEYWORD", "DISTINCT")) {
      this.expect("KEYWORD", "FROM", "'FROM'");
      operator = "IS DISTINCT FROM";
    }
    const right = this.parseExpression(50 /* COMPARISON */);
    const isBareIs = operator === "IS" || operator === "IS NOT";
    if (isBareIs && right.kind === "null") {
      return { kind: "isNull", operand: left, negated: operator === "IS NOT" };
    }
    return isBareIs ? this.refuse({ kind: "operator", operator: `${operator} \u2026` }) : this.refuse({ kind: "operator", operator });
  }
  /** `x NOT IN (...)`, `x NOT BETWEEN ... AND ...`, `x NOT LIKE/ILIKE ...`. */
  parseNotInfix(left) {
    if (this.match("KEYWORD", "IN")) return this.parseIn(left, true);
    if (this.match("KEYWORD", "BETWEEN")) return this.parseBetween();
    if (this.match("KEYWORD", "LIKE")) return this.parseLike(left, false, true);
    if (this.match("KEYWORD", "ILIKE")) return this.parseLike(left, true, true);
    throw new ParseError("Expected 'IN', 'BETWEEN' or 'LIKE' after 'NOT'");
  }
};
function createParser(source) {
  const tokens = new Lexer(source).tokenize();
  return new Parser(source, tokens);
}

// .cache/semantic-tools/packages/sql-expression/src/sqlExpr.ts
var MAX_EXPRESSION_DEPTH = 64;
var MAX_EXPRESSION_CHARS = 8e3;
function parseExpressionOrRefuse(text) {
  if (text.trim() === "") return { ok: false, refusal: { kind: "unparsed" } };
  if (text.length > MAX_EXPRESSION_CHARS) {
    return { ok: false, refusal: { kind: "too-large" } };
  }
  let expression;
  let parser;
  try {
    parser = createParser(text);
    expression = parser.parseExpression();
    if (!parser.isAtEnd()) {
      return { ok: false, refusal: { kind: "unparsed" } };
    }
  } catch (error) {
    if (error instanceof ParseError || error instanceof LexError) {
      return { ok: false, refusal: { kind: "unparsed" } };
    }
    throw error;
  }
  if (parser.refusal !== void 0) return { ok: false, refusal: parser.refusal };
  return exceedsDepth(expression, MAX_EXPRESSION_DEPTH) ? { ok: false, refusal: { kind: "too-large" } } : { ok: true, expression };
}
function exceedsDepth(expression, maxDepth) {
  const pending = [
    { node: expression, depth: 1 }
  ];
  while (pending.length > 0) {
    const current = pending.pop();
    if (current === void 0) break;
    if (current.depth > maxDepth) return true;
    const childDepth = current.depth + 1;
    switch (current.node.kind) {
      case "unary":
        pending.push({ node: current.node.operand, depth: childDepth });
        break;
      case "binary":
        pending.push({ node: current.node.left, depth: childDepth });
        pending.push({ node: current.node.right, depth: childDepth });
        break;
      case "call":
        for (const arg of current.node.args) {
          pending.push({ node: arg, depth: childDepth });
        }
        break;
      case "variantPath":
        pending.push({ node: current.node.base, depth: childDepth });
        break;
      case "cast":
        pending.push({ node: current.node.operand, depth: childDepth });
        break;
      case "not":
        pending.push({ node: current.node.operand, depth: childDepth });
        break;
      case "isNull":
        pending.push({ node: current.node.operand, depth: childDepth });
        break;
      case "in":
        pending.push({ node: current.node.value, depth: childDepth });
        for (const candidate of current.node.candidates) {
          pending.push({ node: candidate, depth: childDepth });
        }
        break;
      case "like":
        pending.push({ node: current.node.value, depth: childDepth });
        pending.push({ node: current.node.pattern, depth: childDepth });
        break;
      case "case":
        for (const branch of current.node.branches) {
          pending.push({ node: branch.when, depth: childDepth });
          pending.push({ node: branch.then, depth: childDepth });
        }
        if (current.node.otherwise !== null) {
          pending.push({ node: current.node.otherwise, depth: childDepth });
        }
        break;
      default:
        break;
    }
  }
  return false;
}

// .cache/semantic-tools/packages/snowflake-semantic-view/src/toIr.ts
var WINDOW_PATTERN = /\bOVER\s*\(/i;
var DYNAMIC_PARTITION_PATTERN = /\bPARTITION\s+BY\s+EXCLUDING\b/i;
function windowConstruct(source) {
  if (DYNAMIC_PARTITION_PATTERN.test(source.mask)) return "dynamicPartition";
  if (WINDOW_PATTERN.test(source.mask)) return "plain";
  return null;
}
function referencesAlias(expression, aliasName) {
  if (new RegExp(`\\b${escapeRegExp(aliasName)}\\s*\\.`, "i").test(expression.mask)) {
    return true;
  }
  const quoted = `"${aliasName.replace(/"/g, '""')}"`;
  return new RegExp(`${escapeRegExp(quoted)}\\s*\\.`).test(expression.text);
}
function toIrExpressionObject(object, aliasNames) {
  const source = mkSource(object.expression);
  const referencedAliases = aliasNames.filter(
    (aliasName) => referencesAlias(source, aliasName)
  );
  return {
    kind: object.kind,
    visibility: object.visibility,
    owner: object.owner,
    name: object.name,
    filterLabel: object.filterLabel,
    using: object.using,
    nonAdditiveBy: object.nonAdditiveBy,
    expression: {
      dialect: "snowflake",
      text: object.expression,
      span: object.expressionSpan,
      parse: parseExpressionOrRefuse(object.expression)
    },
    synonymCount: object.synonymCount,
    comment: object.comment,
    tagCount: object.tagCount,
    cortexSearchCount: object.cortexSearchCount,
    sampleValueCount: object.sampleValueCount,
    isEnum: object.isEnum,
    windowConstruct: windowConstruct(source),
    referencedAliases,
    span: object.span
  };
}
function toIr(view) {
  const aliasNames = view.tables.map((table) => table.alias.name);
  return {
    name: view.name,
    tables: view.tables,
    relationships: view.relationships,
    facts: view.facts.map((fact) => toIrExpressionObject(fact, aliasNames)),
    dimensions: view.dimensions.map(
      (dimension) => toIrExpressionObject(dimension, aliasNames)
    ),
    metrics: view.metrics.map((metric) => toIrExpressionObject(metric, aliasNames)),
    comment: view.comment,
    tagCount: view.tagCount,
    hasMaxStaleness: view.hasMaxStaleness,
    hasAiMetadata: view.hasAiMetadata,
    verifiedQueryCount: view.verifiedQueryCount,
    hasCopyGrants: view.hasCopyGrants,
    span: view.span
  };
}

// .cache/semantic-tools/packages/snowflake-semantic-view/src/fromSemanticView.ts
function fromSemanticView(view) {
  const viewName = view.name.parts.at(-1)?.name ?? "";
  if (view.dimensions.length + view.metrics.length === 0) {
    return {
      ok: false,
      errors: [
        mkFinding(
          "MISSING_DIMENSIONS_OR_METRICS",
          { kind: "view", name: viewName },
          view.span.line,
          { tableCount: view.tables.length }
        )
      ]
    };
  }
  const structuralErrors = validateStructure(view);
  if (structuralErrors.length > 0) {
    return { ok: false, errors: structuralErrors };
  }
  return { ok: true, ir: toIr(view) };
}

// .cache/semantic-tools/packages/snowflake-semantic-view/src/read.ts
function read(ddl) {
  const source = mkSource(ddl);
  const offsets = statementOffsets(source);
  if (offsets.length > 1) {
    const secondOffset = offsets[1] ?? 0;
    return {
      ok: false,
      errors: [
        mkFinding(
          "MULTIPLE_STATEMENTS",
          { kind: "view", name: "" },
          source.lineAt(secondOffset),
          { count: offsets.length }
        )
      ]
    };
  }
  const parsed = parseSemanticView(source);
  if (!parsed.ok) {
    return {
      ok: false,
      errors: [
        mkFinding(
          "PARSE_FAILURE",
          { kind: "view", name: "" },
          source.lineAt(parsed.offset),
          { diagnostic: parsed.diagnostic }
        )
      ]
    };
  }
  return fromSemanticView(parsed.view);
}
var SNOWFLAKE_SEMANTIC_VIEW_READER = {
  dialect: "snowflake",
  read
};

// node_modules/yaml/browser/dist/nodes/identity.js
var ALIAS = /* @__PURE__ */ Symbol.for("yaml.alias");
var DOC = /* @__PURE__ */ Symbol.for("yaml.document");
var MAP = /* @__PURE__ */ Symbol.for("yaml.map");
var PAIR = /* @__PURE__ */ Symbol.for("yaml.pair");
var SCALAR = /* @__PURE__ */ Symbol.for("yaml.scalar");
var SEQ = /* @__PURE__ */ Symbol.for("yaml.seq");
var NODE_TYPE = /* @__PURE__ */ Symbol.for("yaml.node.type");
var isAlias = (node) => !!node && typeof node === "object" && node[NODE_TYPE] === ALIAS;
var isDocument = (node) => !!node && typeof node === "object" && node[NODE_TYPE] === DOC;
var isMap = (node) => !!node && typeof node === "object" && node[NODE_TYPE] === MAP;
var isPair = (node) => !!node && typeof node === "object" && node[NODE_TYPE] === PAIR;
var isScalar = (node) => !!node && typeof node === "object" && node[NODE_TYPE] === SCALAR;
var isSeq = (node) => !!node && typeof node === "object" && node[NODE_TYPE] === SEQ;
function isCollection(node) {
  if (node && typeof node === "object")
    switch (node[NODE_TYPE]) {
      case MAP:
      case SEQ:
        return true;
    }
  return false;
}
function isNode(node) {
  if (node && typeof node === "object")
    switch (node[NODE_TYPE]) {
      case ALIAS:
      case MAP:
      case SCALAR:
      case SEQ:
        return true;
    }
  return false;
}
var hasAnchor = (node) => (isScalar(node) || isCollection(node)) && !!node.anchor;

// node_modules/yaml/browser/dist/visit.js
var BREAK = /* @__PURE__ */ Symbol("break visit");
var SKIP = /* @__PURE__ */ Symbol("skip children");
var REMOVE = /* @__PURE__ */ Symbol("remove node");
function visit(node, visitor) {
  const visitor_ = initVisitor(visitor);
  if (isDocument(node)) {
    const cd = visit_(null, node.contents, visitor_, Object.freeze([node]));
    if (cd === REMOVE)
      node.contents = null;
  } else
    visit_(null, node, visitor_, Object.freeze([]));
}
visit.BREAK = BREAK;
visit.SKIP = SKIP;
visit.REMOVE = REMOVE;
function visit_(key, node, visitor, path) {
  const ctrl = callVisitor(key, node, visitor, path);
  if (isNode(ctrl) || isPair(ctrl)) {
    replaceNode(key, path, ctrl);
    return visit_(key, ctrl, visitor, path);
  }
  if (typeof ctrl !== "symbol") {
    if (isCollection(node)) {
      path = Object.freeze(path.concat(node));
      for (let i = 0; i < node.items.length; ++i) {
        const ci2 = visit_(i, node.items[i], visitor, path);
        if (typeof ci2 === "number")
          i = ci2 - 1;
        else if (ci2 === BREAK)
          return BREAK;
        else if (ci2 === REMOVE) {
          node.items.splice(i, 1);
          i -= 1;
        }
      }
    } else if (isPair(node)) {
      path = Object.freeze(path.concat(node));
      const ck = visit_("key", node.key, visitor, path);
      if (ck === BREAK)
        return BREAK;
      else if (ck === REMOVE)
        node.key = null;
      const cv = visit_("value", node.value, visitor, path);
      if (cv === BREAK)
        return BREAK;
      else if (cv === REMOVE)
        node.value = null;
    }
  }
  return ctrl;
}
async function visitAsync(node, visitor) {
  const visitor_ = initVisitor(visitor);
  if (isDocument(node)) {
    const cd = await visitAsync_(null, node.contents, visitor_, Object.freeze([node]));
    if (cd === REMOVE)
      node.contents = null;
  } else
    await visitAsync_(null, node, visitor_, Object.freeze([]));
}
visitAsync.BREAK = BREAK;
visitAsync.SKIP = SKIP;
visitAsync.REMOVE = REMOVE;
async function visitAsync_(key, node, visitor, path) {
  const ctrl = await callVisitor(key, node, visitor, path);
  if (isNode(ctrl) || isPair(ctrl)) {
    replaceNode(key, path, ctrl);
    return visitAsync_(key, ctrl, visitor, path);
  }
  if (typeof ctrl !== "symbol") {
    if (isCollection(node)) {
      path = Object.freeze(path.concat(node));
      for (let i = 0; i < node.items.length; ++i) {
        const ci2 = await visitAsync_(i, node.items[i], visitor, path);
        if (typeof ci2 === "number")
          i = ci2 - 1;
        else if (ci2 === BREAK)
          return BREAK;
        else if (ci2 === REMOVE) {
          node.items.splice(i, 1);
          i -= 1;
        }
      }
    } else if (isPair(node)) {
      path = Object.freeze(path.concat(node));
      const ck = await visitAsync_("key", node.key, visitor, path);
      if (ck === BREAK)
        return BREAK;
      else if (ck === REMOVE)
        node.key = null;
      const cv = await visitAsync_("value", node.value, visitor, path);
      if (cv === BREAK)
        return BREAK;
      else if (cv === REMOVE)
        node.value = null;
    }
  }
  return ctrl;
}
function initVisitor(visitor) {
  if (typeof visitor === "object" && (visitor.Collection || visitor.Node || visitor.Value)) {
    return Object.assign({
      Alias: visitor.Node,
      Map: visitor.Node,
      Scalar: visitor.Node,
      Seq: visitor.Node
    }, visitor.Value && {
      Map: visitor.Value,
      Scalar: visitor.Value,
      Seq: visitor.Value
    }, visitor.Collection && {
      Map: visitor.Collection,
      Seq: visitor.Collection
    }, visitor);
  }
  return visitor;
}
function callVisitor(key, node, visitor, path) {
  if (typeof visitor === "function")
    return visitor(key, node, path);
  if (isMap(node))
    return visitor.Map?.(key, node, path);
  if (isSeq(node))
    return visitor.Seq?.(key, node, path);
  if (isPair(node))
    return visitor.Pair?.(key, node, path);
  if (isScalar(node))
    return visitor.Scalar?.(key, node, path);
  if (isAlias(node))
    return visitor.Alias?.(key, node, path);
  return void 0;
}
function replaceNode(key, path, node) {
  const parent = path[path.length - 1];
  if (isCollection(parent)) {
    parent.items[key] = node;
  } else if (isPair(parent)) {
    if (key === "key")
      parent.key = node;
    else
      parent.value = node;
  } else if (isDocument(parent)) {
    parent.contents = node;
  } else {
    const pt = isAlias(parent) ? "alias" : "scalar";
    throw new Error(`Cannot replace node with ${pt} parent`);
  }
}

// node_modules/yaml/browser/dist/doc/directives.js
var escapeChars = {
  "!": "%21",
  ",": "%2C",
  "[": "%5B",
  "]": "%5D",
  "{": "%7B",
  "}": "%7D"
};
var escapeTagName = (tn) => tn.replace(/[!,[\]{}]/g, (ch) => escapeChars[ch]);
var Directives = class _Directives {
  constructor(yaml, tags) {
    this.docStart = null;
    this.docEnd = false;
    this.yaml = Object.assign({}, _Directives.defaultYaml, yaml);
    this.tags = Object.assign({}, _Directives.defaultTags, tags);
  }
  clone() {
    const copy = new _Directives(this.yaml, this.tags);
    copy.docStart = this.docStart;
    return copy;
  }
  /**
   * During parsing, get a Directives instance for the current document and
   * update the stream state according to the current version's spec.
   */
  atDocument() {
    const res = new _Directives(this.yaml, this.tags);
    switch (this.yaml.version) {
      case "1.1":
        this.atNextDocument = true;
        break;
      case "1.2":
        this.atNextDocument = false;
        this.yaml = {
          explicit: _Directives.defaultYaml.explicit,
          version: "1.2"
        };
        this.tags = Object.assign({}, _Directives.defaultTags);
        break;
    }
    return res;
  }
  /**
   * @param onError - May be called even if the action was successful
   * @returns `true` on success
   */
  add(line, onError) {
    if (this.atNextDocument) {
      this.yaml = { explicit: _Directives.defaultYaml.explicit, version: "1.1" };
      this.tags = Object.assign({}, _Directives.defaultTags);
      this.atNextDocument = false;
    }
    const parts = line.trim().split(/[ \t]+/);
    const name = parts.shift();
    switch (name) {
      case "%TAG": {
        if (parts.length !== 2) {
          onError(0, "%TAG directive should contain exactly two parts");
          if (parts.length < 2)
            return false;
        }
        const [handle, prefix] = parts;
        this.tags[handle] = prefix;
        return true;
      }
      case "%YAML": {
        this.yaml.explicit = true;
        if (parts.length !== 1) {
          onError(0, "%YAML directive should contain exactly one part");
          return false;
        }
        const [version] = parts;
        if (version === "1.1" || version === "1.2") {
          this.yaml.version = version;
          return true;
        } else {
          const isValid = /^\d+\.\d+$/.test(version);
          onError(6, `Unsupported YAML version ${version}`, isValid);
          return false;
        }
      }
      default:
        onError(0, `Unknown directive ${name}`, true);
        return false;
    }
  }
  /**
   * Resolves a tag, matching handles to those defined in %TAG directives.
   *
   * @returns Resolved tag, which may also be the non-specific tag `'!'` or a
   *   `'!local'` tag, or `null` if unresolvable.
   */
  tagName(source, onError) {
    if (source === "!")
      return "!";
    if (source[0] !== "!") {
      onError(`Not a valid tag: ${source}`);
      return null;
    }
    if (source[1] === "<") {
      const verbatim = source.slice(2, -1);
      if (verbatim === "!" || verbatim === "!!") {
        onError(`Verbatim tags aren't resolved, so ${source} is invalid.`);
        return null;
      }
      if (source[source.length - 1] !== ">")
        onError("Verbatim tags must end with a >");
      return verbatim;
    }
    const [, handle, suffix] = source.match(/^(.*!)([^!]*)$/s);
    if (!suffix)
      onError(`The ${source} tag has no suffix`);
    const prefix = this.tags[handle];
    if (prefix) {
      try {
        return prefix + decodeURIComponent(suffix);
      } catch (error) {
        onError(String(error));
        return null;
      }
    }
    if (handle === "!")
      return source;
    onError(`Could not resolve tag: ${source}`);
    return null;
  }
  /**
   * Given a fully resolved tag, returns its printable string form,
   * taking into account current tag prefixes and defaults.
   */
  tagString(tag) {
    for (const [handle, prefix] of Object.entries(this.tags)) {
      if (tag.startsWith(prefix))
        return handle + escapeTagName(tag.substring(prefix.length));
    }
    return tag[0] === "!" ? tag : `!<${tag}>`;
  }
  toString(doc) {
    const lines = this.yaml.explicit ? [`%YAML ${this.yaml.version || "1.2"}`] : [];
    const tagEntries = Object.entries(this.tags);
    let tagNames;
    if (doc && tagEntries.length > 0 && isNode(doc.contents)) {
      const tags = {};
      visit(doc.contents, (_key, node) => {
        if (isNode(node) && node.tag)
          tags[node.tag] = true;
      });
      tagNames = Object.keys(tags);
    } else
      tagNames = [];
    for (const [handle, prefix] of tagEntries) {
      if (handle === "!!" && prefix === "tag:yaml.org,2002:")
        continue;
      if (!doc || tagNames.some((tn) => tn.startsWith(prefix)))
        lines.push(`%TAG ${handle} ${prefix}`);
    }
    return lines.join("\n");
  }
};
Directives.defaultYaml = { explicit: false, version: "1.2" };
Directives.defaultTags = { "!!": "tag:yaml.org,2002:" };

// node_modules/yaml/browser/dist/doc/anchors.js
function anchorIsValid(anchor) {
  if (/[\x00-\x19\s,[\]{}]/.test(anchor)) {
    const sa = JSON.stringify(anchor);
    const msg = `Anchor must not contain whitespace or control characters: ${sa}`;
    throw new Error(msg);
  }
  return true;
}
function anchorNames(root) {
  const anchors = /* @__PURE__ */ new Set();
  visit(root, {
    Value(_key, node) {
      if (node.anchor)
        anchors.add(node.anchor);
    }
  });
  return anchors;
}
function findNewAnchor(prefix, exclude) {
  for (let i = 1; true; ++i) {
    const name = `${prefix}${i}`;
    if (!exclude.has(name))
      return name;
  }
}
function createNodeAnchors(doc, prefix) {
  const aliasObjects = [];
  const sourceObjects = /* @__PURE__ */ new Map();
  let prevAnchors = null;
  return {
    onAnchor: (source) => {
      aliasObjects.push(source);
      prevAnchors ?? (prevAnchors = anchorNames(doc));
      const anchor = findNewAnchor(prefix, prevAnchors);
      prevAnchors.add(anchor);
      return anchor;
    },
    /**
     * With circular references, the source node is only resolved after all
     * of its child nodes are. This is why anchors are set only after all of
     * the nodes have been created.
     */
    setAnchors: () => {
      for (const source of aliasObjects) {
        const ref = sourceObjects.get(source);
        if (typeof ref === "object" && ref.anchor && (isScalar(ref.node) || isCollection(ref.node))) {
          ref.node.anchor = ref.anchor;
        } else {
          const error = new Error("Failed to resolve repeated object (this should not happen)");
          error.source = source;
          throw error;
        }
      }
    },
    sourceObjects
  };
}

// node_modules/yaml/browser/dist/doc/applyReviver.js
function applyReviver(reviver, obj, key, val) {
  if (val && typeof val === "object") {
    if (Array.isArray(val)) {
      for (let i = 0, len = val.length; i < len; ++i) {
        const v0 = val[i];
        const v1 = applyReviver(reviver, val, String(i), v0);
        if (v1 === void 0)
          delete val[i];
        else if (v1 !== v0)
          val[i] = v1;
      }
    } else if (val instanceof Map) {
      for (const k of Array.from(val.keys())) {
        const v0 = val.get(k);
        const v1 = applyReviver(reviver, val, k, v0);
        if (v1 === void 0)
          val.delete(k);
        else if (v1 !== v0)
          val.set(k, v1);
      }
    } else if (val instanceof Set) {
      for (const v0 of Array.from(val)) {
        const v1 = applyReviver(reviver, val, v0, v0);
        if (v1 === void 0)
          val.delete(v0);
        else if (v1 !== v0) {
          val.delete(v0);
          val.add(v1);
        }
      }
    } else {
      for (const [k, v0] of Object.entries(val)) {
        const v1 = applyReviver(reviver, val, k, v0);
        if (v1 === void 0)
          delete val[k];
        else if (v1 !== v0)
          val[k] = v1;
      }
    }
  }
  return reviver.call(obj, key, val);
}

// node_modules/yaml/browser/dist/nodes/toJS.js
function toJS(value, arg, ctx) {
  if (Array.isArray(value))
    return value.map((v, i) => toJS(v, String(i), ctx));
  if (value && typeof value.toJSON === "function") {
    if (!ctx || !hasAnchor(value))
      return value.toJSON(arg, ctx);
    const data = { aliasCount: 0, count: 1, res: void 0 };
    ctx.anchors.set(value, data);
    ctx.onCreate = (res2) => {
      data.res = res2;
      delete ctx.onCreate;
    };
    const res = value.toJSON(arg, ctx);
    if (ctx.onCreate)
      ctx.onCreate(res);
    return res;
  }
  if (typeof value === "bigint" && !ctx?.keep)
    return Number(value);
  return value;
}

// node_modules/yaml/browser/dist/nodes/Node.js
var NodeBase = class {
  constructor(type) {
    Object.defineProperty(this, NODE_TYPE, { value: type });
  }
  /** Create a copy of this node.  */
  clone() {
    const copy = Object.create(Object.getPrototypeOf(this), Object.getOwnPropertyDescriptors(this));
    if (this.range)
      copy.range = this.range.slice();
    return copy;
  }
  /** A plain JavaScript representation of this node. */
  toJS(doc, { mapAsMap, maxAliasCount, onAnchor, reviver } = {}) {
    if (!isDocument(doc))
      throw new TypeError("A document argument is required");
    const ctx = {
      anchors: /* @__PURE__ */ new Map(),
      doc,
      keep: true,
      mapAsMap: mapAsMap === true,
      mapKeyWarned: false,
      maxAliasCount: typeof maxAliasCount === "number" ? maxAliasCount : 100
    };
    const res = toJS(this, "", ctx);
    if (typeof onAnchor === "function")
      for (const { count, res: res2 } of ctx.anchors.values())
        onAnchor(res2, count);
    return typeof reviver === "function" ? applyReviver(reviver, { "": res }, "", res) : res;
  }
};

// node_modules/yaml/browser/dist/nodes/Alias.js
var Alias = class extends NodeBase {
  constructor(source) {
    super(ALIAS);
    this.source = source;
    Object.defineProperty(this, "tag", {
      set() {
        throw new Error("Alias nodes cannot have tags");
      }
    });
  }
  /**
   * Resolve the value of this alias within `doc`, finding the last
   * instance of the `source` anchor before this node.
   */
  resolve(doc, ctx) {
    if (ctx?.maxAliasCount === 0)
      throw new ReferenceError("Alias resolution is disabled");
    let nodes;
    if (ctx?.aliasResolveCache) {
      nodes = ctx.aliasResolveCache;
    } else {
      nodes = [];
      visit(doc, {
        Node: (_key, node) => {
          if (isAlias(node) || hasAnchor(node))
            nodes.push(node);
        }
      });
      if (ctx)
        ctx.aliasResolveCache = nodes;
    }
    let found = void 0;
    for (const node of nodes) {
      if (node === this)
        break;
      if (node.anchor === this.source)
        found = node;
    }
    return found;
  }
  toJSON(_arg, ctx) {
    if (!ctx)
      return { source: this.source };
    const { anchors, doc, maxAliasCount } = ctx;
    const source = this.resolve(doc, ctx);
    if (!source) {
      const msg = `Unresolved alias (the anchor must be set before the alias): ${this.source}`;
      throw new ReferenceError(msg);
    }
    let data = anchors.get(source);
    if (!data) {
      toJS(source, null, ctx);
      data = anchors.get(source);
    }
    if (data?.res === void 0) {
      const msg = "This should not happen: Alias anchor was not resolved?";
      throw new ReferenceError(msg);
    }
    if (maxAliasCount >= 0) {
      data.count += 1;
      if (data.aliasCount === 0)
        data.aliasCount = getAliasCount(doc, source, anchors);
      if (data.count * data.aliasCount > maxAliasCount) {
        const msg = "Excessive alias count indicates a resource exhaustion attack";
        throw new ReferenceError(msg);
      }
    }
    return data.res;
  }
  toString(ctx, _onComment, _onChompKeep) {
    const src = `*${this.source}`;
    if (ctx) {
      anchorIsValid(this.source);
      if (ctx.options.verifyAliasOrder && !ctx.anchors.has(this.source)) {
        const msg = `Unresolved alias (the anchor must be set before the alias): ${this.source}`;
        throw new Error(msg);
      }
      if (ctx.implicitKey)
        return `${src} `;
    }
    return src;
  }
};
function getAliasCount(doc, node, anchors) {
  if (isAlias(node)) {
    const source = node.resolve(doc);
    const anchor = anchors && source && anchors.get(source);
    return anchor ? anchor.count * anchor.aliasCount : 0;
  } else if (isCollection(node)) {
    let count = 0;
    for (const item of node.items) {
      const c = getAliasCount(doc, item, anchors);
      if (c > count)
        count = c;
    }
    return count;
  } else if (isPair(node)) {
    const kc = getAliasCount(doc, node.key, anchors);
    const vc = getAliasCount(doc, node.value, anchors);
    return Math.max(kc, vc);
  }
  return 1;
}

// node_modules/yaml/browser/dist/nodes/Scalar.js
var isScalarValue = (value) => !value || typeof value !== "function" && typeof value !== "object";
var Scalar = class extends NodeBase {
  constructor(value) {
    super(SCALAR);
    this.value = value;
  }
  toJSON(arg, ctx) {
    return ctx?.keep ? this.value : toJS(this.value, arg, ctx);
  }
  toString() {
    return String(this.value);
  }
};
Scalar.BLOCK_FOLDED = "BLOCK_FOLDED";
Scalar.BLOCK_LITERAL = "BLOCK_LITERAL";
Scalar.PLAIN = "PLAIN";
Scalar.QUOTE_DOUBLE = "QUOTE_DOUBLE";
Scalar.QUOTE_SINGLE = "QUOTE_SINGLE";

// node_modules/yaml/browser/dist/doc/createNode.js
var defaultTagPrefix = "tag:yaml.org,2002:";
function findTagObject(value, tagName, tags) {
  if (tagName) {
    const match = tags.filter((t) => t.tag === tagName);
    const tagObj = match.find((t) => !t.format) ?? match[0];
    if (!tagObj)
      throw new Error(`Tag ${tagName} not found`);
    return tagObj;
  }
  return tags.find((t) => t.identify?.(value) && !t.format);
}
function createNode(value, tagName, ctx) {
  if (isDocument(value))
    value = value.contents;
  if (isNode(value))
    return value;
  if (isPair(value)) {
    const map2 = ctx.schema[MAP].createNode?.(ctx.schema, null, ctx);
    map2.items.push(value);
    return map2;
  }
  if (value instanceof String || value instanceof Number || value instanceof Boolean || typeof BigInt !== "undefined" && value instanceof BigInt) {
    value = value.valueOf();
  }
  const { aliasDuplicateObjects, onAnchor, onTagObj, schema: schema4, sourceObjects } = ctx;
  let ref = void 0;
  if (aliasDuplicateObjects && value && typeof value === "object") {
    ref = sourceObjects.get(value);
    if (ref) {
      ref.anchor ?? (ref.anchor = onAnchor(value));
      return new Alias(ref.anchor);
    } else {
      ref = { anchor: null, node: null };
      sourceObjects.set(value, ref);
    }
  }
  if (tagName?.startsWith("!!"))
    tagName = defaultTagPrefix + tagName.slice(2);
  let tagObj = findTagObject(value, tagName, schema4.tags);
  if (!tagObj) {
    if (value && typeof value.toJSON === "function") {
      value = value.toJSON();
    }
    if (!value || typeof value !== "object") {
      const node2 = new Scalar(value);
      if (ref)
        ref.node = node2;
      return node2;
    }
    tagObj = value instanceof Map ? schema4[MAP] : Symbol.iterator in Object(value) ? schema4[SEQ] : schema4[MAP];
  }
  if (onTagObj) {
    onTagObj(tagObj);
    delete ctx.onTagObj;
  }
  const node = tagObj?.createNode ? tagObj.createNode(ctx.schema, value, ctx) : typeof tagObj?.nodeClass?.from === "function" ? tagObj.nodeClass.from(ctx.schema, value, ctx) : new Scalar(value);
  if (tagName)
    node.tag = tagName;
  else if (!tagObj.default)
    node.tag = tagObj.tag;
  if (ref)
    ref.node = node;
  return node;
}

// node_modules/yaml/browser/dist/nodes/Collection.js
function collectionFromPath(schema4, path, value) {
  let v = value;
  for (let i = path.length - 1; i >= 0; --i) {
    const k = path[i];
    if (typeof k === "number" && Number.isInteger(k) && k >= 0) {
      const a = [];
      a[k] = v;
      v = a;
    } else {
      v = /* @__PURE__ */ new Map([[k, v]]);
    }
  }
  return createNode(v, void 0, {
    aliasDuplicateObjects: false,
    keepUndefined: false,
    onAnchor: () => {
      throw new Error("This should not happen, please report a bug.");
    },
    schema: schema4,
    sourceObjects: /* @__PURE__ */ new Map()
  });
}
var isEmptyPath = (path) => path == null || typeof path === "object" && !!path[Symbol.iterator]().next().done;
var Collection = class extends NodeBase {
  constructor(type, schema4) {
    super(type);
    Object.defineProperty(this, "schema", {
      value: schema4,
      configurable: true,
      enumerable: false,
      writable: true
    });
  }
  /**
   * Create a copy of this collection.
   *
   * @param schema - If defined, overwrites the original's schema
   */
  clone(schema4) {
    const copy = Object.create(Object.getPrototypeOf(this), Object.getOwnPropertyDescriptors(this));
    if (schema4)
      copy.schema = schema4;
    copy.items = copy.items.map((it) => isNode(it) || isPair(it) ? it.clone(schema4) : it);
    if (this.range)
      copy.range = this.range.slice();
    return copy;
  }
  /**
   * Adds a value to the collection. For `!!map` and `!!omap` the value must
   * be a Pair instance or a `{ key, value }` object, which may not have a key
   * that already exists in the map.
   */
  addIn(path, value) {
    if (isEmptyPath(path))
      this.add(value);
    else {
      const [key, ...rest] = path;
      const node = this.get(key, true);
      if (isCollection(node))
        node.addIn(rest, value);
      else if (node === void 0 && this.schema)
        this.set(key, collectionFromPath(this.schema, rest, value));
      else
        throw new Error(`Expected YAML collection at ${key}. Remaining path: ${rest}`);
    }
  }
  /**
   * Removes a value from the collection.
   * @returns `true` if the item was found and removed.
   */
  deleteIn(path) {
    const [key, ...rest] = path;
    if (rest.length === 0)
      return this.delete(key);
    const node = this.get(key, true);
    if (isCollection(node))
      return node.deleteIn(rest);
    else
      throw new Error(`Expected YAML collection at ${key}. Remaining path: ${rest}`);
  }
  /**
   * Returns item at `key`, or `undefined` if not found. By default unwraps
   * scalar values from their surrounding node; to disable set `keepScalar` to
   * `true` (collections are always returned intact).
   */
  getIn(path, keepScalar) {
    const [key, ...rest] = path;
    const node = this.get(key, true);
    if (rest.length === 0)
      return !keepScalar && isScalar(node) ? node.value : node;
    else
      return isCollection(node) ? node.getIn(rest, keepScalar) : void 0;
  }
  hasAllNullValues(allowScalar) {
    return this.items.every((node) => {
      if (!isPair(node))
        return false;
      const n = node.value;
      return n == null || allowScalar && isScalar(n) && n.value == null && !n.commentBefore && !n.comment && !n.tag;
    });
  }
  /**
   * Checks if the collection includes a value with the key `key`.
   */
  hasIn(path) {
    const [key, ...rest] = path;
    if (rest.length === 0)
      return this.has(key);
    const node = this.get(key, true);
    return isCollection(node) ? node.hasIn(rest) : false;
  }
  /**
   * Sets a value in this collection. For `!!set`, `value` needs to be a
   * boolean to add/remove the item from the set.
   */
  setIn(path, value) {
    const [key, ...rest] = path;
    if (rest.length === 0) {
      this.set(key, value);
    } else {
      const node = this.get(key, true);
      if (isCollection(node))
        node.setIn(rest, value);
      else if (node === void 0 && this.schema)
        this.set(key, collectionFromPath(this.schema, rest, value));
      else
        throw new Error(`Expected YAML collection at ${key}. Remaining path: ${rest}`);
    }
  }
};

// node_modules/yaml/browser/dist/stringify/stringifyComment.js
var stringifyComment = (str) => str.replace(/^(?!$)(?: $)?/gm, "#");
function indentComment(comment, indent) {
  if (/^\n+$/.test(comment))
    return comment.substring(1);
  return indent ? comment.replace(/^(?! *$)/gm, indent) : comment;
}
var lineComment = (str, indent, comment) => str.endsWith("\n") ? indentComment(comment, indent) : comment.includes("\n") ? "\n" + indentComment(comment, indent) : (str.endsWith(" ") ? "" : " ") + comment;

// node_modules/yaml/browser/dist/stringify/foldFlowLines.js
var FOLD_FLOW = "flow";
var FOLD_BLOCK = "block";
var FOLD_QUOTED = "quoted";
function foldFlowLines(text, indent, mode = "flow", { indentAtStart, lineWidth = 80, minContentWidth = 20, onFold, onOverflow } = {}) {
  if (!lineWidth || lineWidth < 0)
    return text;
  if (lineWidth < minContentWidth)
    minContentWidth = 0;
  const endStep = Math.max(1 + minContentWidth, 1 + lineWidth - indent.length);
  if (text.length <= endStep)
    return text;
  const folds = [];
  const escapedFolds = {};
  let end = lineWidth - indent.length;
  if (typeof indentAtStart === "number") {
    if (indentAtStart > lineWidth - Math.max(2, minContentWidth))
      folds.push(0);
    else
      end = lineWidth - indentAtStart;
  }
  let split = void 0;
  let prev = void 0;
  let overflow = false;
  let i = -1;
  let escStart = -1;
  let escEnd = -1;
  if (mode === FOLD_BLOCK) {
    i = consumeMoreIndentedLines(text, i, indent.length);
    if (i !== -1)
      end = i + endStep;
  }
  for (let ch; ch = text[i += 1]; ) {
    if (mode === FOLD_QUOTED && ch === "\\") {
      escStart = i;
      switch (text[i + 1]) {
        case "x":
          i += 3;
          break;
        case "u":
          i += 5;
          break;
        case "U":
          i += 9;
          break;
        default:
          i += 1;
      }
      escEnd = i;
    }
    if (ch === "\n") {
      if (mode === FOLD_BLOCK)
        i = consumeMoreIndentedLines(text, i, indent.length);
      end = i + indent.length + endStep;
      split = void 0;
    } else {
      if (ch === " " && prev && prev !== " " && prev !== "\n" && prev !== "	") {
        const next = text[i + 1];
        if (next && next !== " " && next !== "\n" && next !== "	")
          split = i;
      }
      if (i >= end) {
        if (split) {
          folds.push(split);
          end = split + endStep;
          split = void 0;
        } else if (mode === FOLD_QUOTED) {
          while (prev === " " || prev === "	") {
            prev = ch;
            ch = text[i += 1];
            overflow = true;
          }
          const j = i > escEnd + 1 ? i - 2 : escStart - 1;
          if (escapedFolds[j])
            return text;
          folds.push(j);
          escapedFolds[j] = true;
          end = j + endStep;
          split = void 0;
        } else {
          overflow = true;
        }
      }
    }
    prev = ch;
  }
  if (overflow && onOverflow)
    onOverflow();
  if (folds.length === 0)
    return text;
  if (onFold)
    onFold();
  let res = text.slice(0, folds[0]);
  for (let i2 = 0; i2 < folds.length; ++i2) {
    const fold = folds[i2];
    const end2 = folds[i2 + 1] || text.length;
    if (fold === 0)
      res = `
${indent}${text.slice(0, end2)}`;
    else {
      if (mode === FOLD_QUOTED && escapedFolds[fold])
        res += `${text[fold]}\\`;
      res += `
${indent}${text.slice(fold + 1, end2)}`;
    }
  }
  return res;
}
function consumeMoreIndentedLines(text, i, indent) {
  let end = i;
  let start = i + 1;
  let ch = text[start];
  while (ch === " " || ch === "	") {
    if (i < start + indent) {
      ch = text[++i];
    } else {
      do {
        ch = text[++i];
      } while (ch && ch !== "\n");
      end = i;
      start = i + 1;
      ch = text[start];
    }
  }
  return end;
}

// node_modules/yaml/browser/dist/stringify/stringifyString.js
var getFoldOptions = (ctx, isBlock2) => ({
  indentAtStart: isBlock2 ? ctx.indent.length : ctx.indentAtStart,
  lineWidth: ctx.options.lineWidth,
  minContentWidth: ctx.options.minContentWidth
});
var containsDocumentMarker = (str) => /^(%|---|\.\.\.)/m.test(str);
function lineLengthOverLimit(str, lineWidth, indentLength) {
  if (!lineWidth || lineWidth < 0)
    return false;
  const limit = lineWidth - indentLength;
  const strLen = str.length;
  if (strLen <= limit)
    return false;
  for (let i = 0, start = 0; i < strLen; ++i) {
    if (str[i] === "\n") {
      if (i - start > limit)
        return true;
      start = i + 1;
      if (strLen - start <= limit)
        return false;
    }
  }
  return true;
}
function doubleQuotedString(value, ctx) {
  const json = JSON.stringify(value);
  if (ctx.options.doubleQuotedAsJSON)
    return json;
  const { implicitKey } = ctx;
  const minMultiLineLength = ctx.options.doubleQuotedMinMultiLineLength;
  const indent = ctx.indent || (containsDocumentMarker(value) ? "  " : "");
  let str = "";
  let start = 0;
  for (let i = 0, ch = json[i]; ch; ch = json[++i]) {
    if (ch === " " && json[i + 1] === "\\" && json[i + 2] === "n") {
      str += json.slice(start, i) + "\\ ";
      i += 1;
      start = i;
      ch = "\\";
    }
    if (ch === "\\")
      switch (json[i + 1]) {
        case "u":
          {
            str += json.slice(start, i);
            const code = json.substr(i + 2, 4);
            switch (code) {
              case "0000":
                str += "\\0";
                break;
              case "0007":
                str += "\\a";
                break;
              case "000b":
                str += "\\v";
                break;
              case "001b":
                str += "\\e";
                break;
              case "0085":
                str += "\\N";
                break;
              case "00a0":
                str += "\\_";
                break;
              case "2028":
                str += "\\L";
                break;
              case "2029":
                str += "\\P";
                break;
              default:
                if (code.substr(0, 2) === "00")
                  str += "\\x" + code.substr(2);
                else
                  str += json.substr(i, 6);
            }
            i += 5;
            start = i + 1;
          }
          break;
        case "n":
          if (implicitKey || json[i + 2] === '"' || json.length < minMultiLineLength) {
            i += 1;
          } else {
            str += json.slice(start, i) + "\n\n";
            while (json[i + 2] === "\\" && json[i + 3] === "n" && json[i + 4] !== '"') {
              str += "\n";
              i += 2;
            }
            str += indent;
            if (json[i + 2] === " ")
              str += "\\";
            i += 1;
            start = i + 1;
          }
          break;
        default:
          i += 1;
      }
  }
  str = start ? str + json.slice(start) : json;
  return implicitKey ? str : foldFlowLines(str, indent, FOLD_QUOTED, getFoldOptions(ctx, false));
}
function singleQuotedString(value, ctx) {
  if (ctx.options.singleQuote === false || ctx.implicitKey && value.includes("\n") || /[ \t]\n|\n[ \t]/.test(value))
    return doubleQuotedString(value, ctx);
  const indent = ctx.indent || (containsDocumentMarker(value) ? "  " : "");
  const res = "'" + value.replace(/'/g, "''").replace(/\n+/g, `$&
${indent}`) + "'";
  return ctx.implicitKey ? res : foldFlowLines(res, indent, FOLD_FLOW, getFoldOptions(ctx, false));
}
function quotedString(value, ctx) {
  const { singleQuote } = ctx.options;
  let qs;
  if (singleQuote === false)
    qs = doubleQuotedString;
  else {
    const hasDouble = value.includes('"');
    const hasSingle = value.includes("'");
    if (hasDouble && !hasSingle)
      qs = singleQuotedString;
    else if (hasSingle && !hasDouble)
      qs = doubleQuotedString;
    else
      qs = singleQuote ? singleQuotedString : doubleQuotedString;
  }
  return qs(value, ctx);
}
var blockEndNewlines;
try {
  blockEndNewlines = new RegExp("(^|(?<!\n))\n+(?!\n|$)", "g");
} catch {
  blockEndNewlines = /\n+(?!\n|$)/g;
}
function blockString({ comment, type, value }, ctx, onComment, onChompKeep) {
  const { blockQuote, commentString, lineWidth } = ctx.options;
  if (!blockQuote || /\n[\t ]+$/.test(value)) {
    return quotedString(value, ctx);
  }
  const indent = ctx.indent || (ctx.forceBlockIndent || containsDocumentMarker(value) ? "  " : "");
  const literal = blockQuote === "literal" ? true : blockQuote === "folded" || type === Scalar.BLOCK_FOLDED ? false : type === Scalar.BLOCK_LITERAL ? true : !lineLengthOverLimit(value, lineWidth, indent.length);
  if (!value)
    return literal ? "|\n" : ">\n";
  let chomp;
  let endStart;
  for (endStart = value.length; endStart > 0; --endStart) {
    const ch = value[endStart - 1];
    if (ch !== "\n" && ch !== "	" && ch !== " ")
      break;
  }
  let end = value.substring(endStart);
  const endNlPos = end.indexOf("\n");
  if (endNlPos === -1) {
    chomp = "-";
  } else if (value === end || endNlPos !== end.length - 1) {
    chomp = "+";
    if (onChompKeep)
      onChompKeep();
  } else {
    chomp = "";
  }
  if (end) {
    value = value.slice(0, -end.length);
    if (end[end.length - 1] === "\n")
      end = end.slice(0, -1);
    end = end.replace(blockEndNewlines, `$&${indent}`);
  }
  let startWithSpace = false;
  let startEnd;
  let startNlPos = -1;
  for (startEnd = 0; startEnd < value.length; ++startEnd) {
    const ch = value[startEnd];
    if (ch === " ")
      startWithSpace = true;
    else if (ch === "\n")
      startNlPos = startEnd;
    else
      break;
  }
  let start = value.substring(0, startNlPos < startEnd ? startNlPos + 1 : startEnd);
  if (start) {
    value = value.substring(start.length);
    start = start.replace(/\n+/g, `$&${indent}`);
  }
  const indentSize = indent ? "2" : "1";
  let header = (startWithSpace ? indentSize : "") + chomp;
  if (comment) {
    header += " " + commentString(comment.replace(/ ?[\r\n]+/g, " "));
    if (onComment)
      onComment();
  }
  if (!literal) {
    const foldedValue = value.replace(/\n+/g, "\n$&").replace(/(?:^|\n)([\t ].*)(?:([\n\t ]*)\n(?![\n\t ]))?/g, "$1$2").replace(/\n+/g, `$&${indent}`);
    let literalFallback = false;
    const foldOptions = getFoldOptions(ctx, true);
    if (blockQuote !== "folded" && type !== Scalar.BLOCK_FOLDED) {
      foldOptions.onOverflow = () => {
        literalFallback = true;
      };
    }
    const body = foldFlowLines(`${start}${foldedValue}${end}`, indent, FOLD_BLOCK, foldOptions);
    if (!literalFallback)
      return `>${header}
${indent}${body}`;
  }
  value = value.replace(/\n+/g, `$&${indent}`);
  return `|${header}
${indent}${start}${value}${end}`;
}
function plainString(item, ctx, onComment, onChompKeep) {
  const { type, value } = item;
  const { actualString, implicitKey, indent, indentStep, inFlow } = ctx;
  if (implicitKey && value.includes("\n") || inFlow && /[[\]{},]/.test(value)) {
    return quotedString(value, ctx);
  }
  if (/^[\n\t ,[\]{}#&*!|>'"%@`]|^[?-]$|^[?-][ \t]|[\n:][ \t]|[ \t]\n|[\n\t ]#|[\n\t :]$/.test(value)) {
    return implicitKey || inFlow || !value.includes("\n") ? quotedString(value, ctx) : blockString(item, ctx, onComment, onChompKeep);
  }
  if (!implicitKey && !inFlow && type !== Scalar.PLAIN && value.includes("\n")) {
    return blockString(item, ctx, onComment, onChompKeep);
  }
  if (containsDocumentMarker(value)) {
    if (indent === "") {
      ctx.forceBlockIndent = true;
      return blockString(item, ctx, onComment, onChompKeep);
    } else if (implicitKey && indent === indentStep) {
      return quotedString(value, ctx);
    }
  }
  const str = value.replace(/\n+/g, `$&
${indent}`);
  if (actualString) {
    const test = (tag) => tag.default && tag.tag !== "tag:yaml.org,2002:str" && tag.test?.test(str);
    const { compat, tags } = ctx.doc.schema;
    if (tags.some(test) || compat?.some(test))
      return quotedString(value, ctx);
  }
  return implicitKey ? str : foldFlowLines(str, indent, FOLD_FLOW, getFoldOptions(ctx, false));
}
function stringifyString(item, ctx, onComment, onChompKeep) {
  const { implicitKey, inFlow } = ctx;
  const ss = typeof item.value === "string" ? item : Object.assign({}, item, { value: String(item.value) });
  let { type } = item;
  if (type !== Scalar.QUOTE_DOUBLE) {
    if (/[\x00-\x08\x0b-\x1f\x7f-\x9f\u{D800}-\u{DFFF}]/u.test(ss.value))
      type = Scalar.QUOTE_DOUBLE;
  }
  const _stringify = (_type) => {
    switch (_type) {
      case Scalar.BLOCK_FOLDED:
      case Scalar.BLOCK_LITERAL:
        return implicitKey || inFlow ? quotedString(ss.value, ctx) : blockString(ss, ctx, onComment, onChompKeep);
      case Scalar.QUOTE_DOUBLE:
        return doubleQuotedString(ss.value, ctx);
      case Scalar.QUOTE_SINGLE:
        return singleQuotedString(ss.value, ctx);
      case Scalar.PLAIN:
        return plainString(ss, ctx, onComment, onChompKeep);
      default:
        return null;
    }
  };
  let res = _stringify(type);
  if (res === null) {
    const { defaultKeyType, defaultStringType } = ctx.options;
    const t = implicitKey && defaultKeyType || defaultStringType;
    res = _stringify(t);
    if (res === null)
      throw new Error(`Unsupported default string type ${t}`);
  }
  return res;
}

// node_modules/yaml/browser/dist/stringify/stringify.js
function createStringifyContext(doc, options) {
  const opt = Object.assign({
    blockQuote: true,
    commentString: stringifyComment,
    defaultKeyType: null,
    defaultStringType: "PLAIN",
    directives: null,
    doubleQuotedAsJSON: false,
    doubleQuotedMinMultiLineLength: 40,
    falseStr: "false",
    flowCollectionPadding: true,
    indentSeq: true,
    lineWidth: 80,
    minContentWidth: 20,
    nullStr: "null",
    simpleKeys: false,
    singleQuote: null,
    trailingComma: false,
    trueStr: "true",
    verifyAliasOrder: true
  }, doc.schema.toStringOptions, options);
  let inFlow;
  switch (opt.collectionStyle) {
    case "block":
      inFlow = false;
      break;
    case "flow":
      inFlow = true;
      break;
    default:
      inFlow = null;
  }
  return {
    anchors: /* @__PURE__ */ new Set(),
    doc,
    flowCollectionPadding: opt.flowCollectionPadding ? " " : "",
    indent: "",
    indentStep: typeof opt.indent === "number" ? " ".repeat(opt.indent) : "  ",
    inFlow,
    options: opt
  };
}
function getTagObject(tags, item) {
  if (item.tag) {
    const match = tags.filter((t) => t.tag === item.tag);
    if (match.length > 0)
      return match.find((t) => t.format === item.format) ?? match[0];
  }
  let tagObj = void 0;
  let obj;
  if (isScalar(item)) {
    obj = item.value;
    let match = tags.filter((t) => t.identify?.(obj));
    if (match.length > 1) {
      const testMatch = match.filter((t) => t.test);
      if (testMatch.length > 0)
        match = testMatch;
    }
    tagObj = match.find((t) => t.format === item.format) ?? match.find((t) => !t.format);
  } else {
    obj = item;
    tagObj = tags.find((t) => t.nodeClass && obj instanceof t.nodeClass);
  }
  if (!tagObj) {
    const name = obj?.constructor?.name ?? (obj === null ? "null" : typeof obj);
    throw new Error(`Tag not resolved for ${name} value`);
  }
  return tagObj;
}
function stringifyProps(node, tagObj, { anchors, doc }) {
  if (!doc.directives)
    return "";
  const props = [];
  const anchor = (isScalar(node) || isCollection(node)) && node.anchor;
  if (anchor && anchorIsValid(anchor)) {
    anchors.add(anchor);
    props.push(`&${anchor}`);
  }
  const tag = node.tag ?? (tagObj.default ? null : tagObj.tag);
  if (tag)
    props.push(doc.directives.tagString(tag));
  return props.join(" ");
}
function stringify(item, ctx, onComment, onChompKeep) {
  if (isPair(item))
    return item.toString(ctx, onComment, onChompKeep);
  if (isAlias(item)) {
    if (ctx.doc.directives)
      return item.toString(ctx);
    if (ctx.resolvedAliases?.has(item)) {
      throw new TypeError(`Cannot stringify circular structure without alias nodes`);
    } else {
      if (ctx.resolvedAliases)
        ctx.resolvedAliases.add(item);
      else
        ctx.resolvedAliases = /* @__PURE__ */ new Set([item]);
      item = item.resolve(ctx.doc);
    }
  }
  let tagObj = void 0;
  const node = isNode(item) ? item : ctx.doc.createNode(item, { onTagObj: (o) => tagObj = o });
  tagObj ?? (tagObj = getTagObject(ctx.doc.schema.tags, node));
  const props = stringifyProps(node, tagObj, ctx);
  if (props.length > 0)
    ctx.indentAtStart = (ctx.indentAtStart ?? 0) + props.length + 1;
  const str = typeof tagObj.stringify === "function" ? tagObj.stringify(node, ctx, onComment, onChompKeep) : isScalar(node) ? stringifyString(node, ctx, onComment, onChompKeep) : node.toString(ctx, onComment, onChompKeep);
  if (!props)
    return str;
  return isScalar(node) || str[0] === "{" || str[0] === "[" ? `${props} ${str}` : `${props}
${ctx.indent}${str}`;
}

// node_modules/yaml/browser/dist/stringify/stringifyPair.js
function stringifyPair({ key, value }, ctx, onComment, onChompKeep) {
  const { allNullValues, doc, indent, indentStep, options: { commentString, indentSeq, simpleKeys } } = ctx;
  let keyComment = isNode(key) && key.comment || null;
  if (simpleKeys) {
    if (keyComment) {
      throw new Error("With simple keys, key nodes cannot have comments");
    }
    if (isCollection(key) || !isNode(key) && typeof key === "object") {
      const msg = "With simple keys, collection cannot be used as a key value";
      throw new Error(msg);
    }
  }
  let explicitKey = !simpleKeys && (!key || keyComment && value == null && !ctx.inFlow || isCollection(key) || (isScalar(key) ? key.type === Scalar.BLOCK_FOLDED || key.type === Scalar.BLOCK_LITERAL : typeof key === "object"));
  ctx = Object.assign({}, ctx, {
    allNullValues: false,
    implicitKey: !explicitKey && (simpleKeys || !allNullValues),
    indent: indent + indentStep
  });
  let keyCommentDone = false;
  let chompKeep = false;
  let str = stringify(key, ctx, () => keyCommentDone = true, () => chompKeep = true);
  if (!explicitKey && !ctx.inFlow && str.length > 1024) {
    if (simpleKeys)
      throw new Error("With simple keys, single line scalar must not span more than 1024 characters");
    explicitKey = true;
  }
  if (ctx.inFlow) {
    if (allNullValues || value == null) {
      if (keyCommentDone && onComment)
        onComment();
      return str === "" ? "?" : explicitKey ? `? ${str}` : str;
    }
  } else if (allNullValues && !simpleKeys || value == null && explicitKey) {
    str = `? ${str}`;
    if (keyComment && !keyCommentDone) {
      str += lineComment(str, ctx.indent, commentString(keyComment));
    } else if (chompKeep && onChompKeep)
      onChompKeep();
    return str;
  }
  if (keyCommentDone)
    keyComment = null;
  if (explicitKey) {
    if (keyComment)
      str += lineComment(str, ctx.indent, commentString(keyComment));
    str = `? ${str}
${indent}:`;
  } else {
    str = `${str}:`;
    if (keyComment)
      str += lineComment(str, ctx.indent, commentString(keyComment));
  }
  let vsb, vcb, valueComment;
  if (isNode(value)) {
    vsb = !!value.spaceBefore;
    vcb = value.commentBefore;
    valueComment = value.comment;
  } else {
    vsb = false;
    vcb = null;
    valueComment = null;
    if (value && typeof value === "object")
      value = doc.createNode(value);
  }
  ctx.implicitKey = false;
  if (!explicitKey && !keyComment && isScalar(value))
    ctx.indentAtStart = str.length + 1;
  chompKeep = false;
  if (!indentSeq && indentStep.length >= 2 && !ctx.inFlow && !explicitKey && isSeq(value) && !value.flow && !value.tag && !value.anchor) {
    ctx.indent = ctx.indent.substring(2);
  }
  let valueCommentDone = false;
  const valueStr = stringify(value, ctx, () => valueCommentDone = true, () => chompKeep = true);
  let ws = " ";
  if (keyComment || vsb || vcb) {
    ws = vsb ? "\n" : "";
    if (vcb) {
      const cs = commentString(vcb);
      ws += `
${indentComment(cs, ctx.indent)}`;
    }
    if (valueStr === "" && !ctx.inFlow) {
      if (ws === "\n" && valueComment)
        ws = "\n\n";
    } else {
      ws += `
${ctx.indent}`;
    }
  } else if (!explicitKey && isCollection(value)) {
    const vs0 = valueStr[0];
    const nl0 = valueStr.indexOf("\n");
    const hasNewline = nl0 !== -1;
    const flow = ctx.inFlow ?? value.flow ?? value.items.length === 0;
    if (hasNewline || !flow) {
      let hasPropsLine = false;
      if (hasNewline && (vs0 === "&" || vs0 === "!")) {
        let sp0 = valueStr.indexOf(" ");
        if (vs0 === "&" && sp0 !== -1 && sp0 < nl0 && valueStr[sp0 + 1] === "!") {
          sp0 = valueStr.indexOf(" ", sp0 + 1);
        }
        if (sp0 === -1 || nl0 < sp0)
          hasPropsLine = true;
      }
      if (!hasPropsLine)
        ws = `
${ctx.indent}`;
    }
  } else if (valueStr === "" || valueStr[0] === "\n") {
    ws = "";
  }
  str += ws + valueStr;
  if (ctx.inFlow) {
    if (valueCommentDone && onComment)
      onComment();
  } else if (valueComment && !valueCommentDone) {
    str += lineComment(str, ctx.indent, commentString(valueComment));
  } else if (chompKeep && onChompKeep) {
    onChompKeep();
  }
  return str;
}

// node_modules/yaml/browser/dist/log.js
function warn(logLevel, warning) {
  if (logLevel === "debug" || logLevel === "warn") {
    console.warn(warning);
  }
}

// node_modules/yaml/browser/dist/schema/yaml-1.1/merge.js
var MERGE_KEY = "<<";
var merge = {
  identify: (value) => value === MERGE_KEY || typeof value === "symbol" && value.description === MERGE_KEY,
  default: "key",
  tag: "tag:yaml.org,2002:merge",
  test: /^<<$/,
  resolve: () => Object.assign(new Scalar(Symbol(MERGE_KEY)), {
    addToJSMap: addMergeToJSMap
  }),
  stringify: () => MERGE_KEY
};
var isMergeKey = (ctx, key) => (merge.identify(key) || isScalar(key) && (!key.type || key.type === Scalar.PLAIN) && merge.identify(key.value)) && ctx?.doc.schema.tags.some((tag) => tag.tag === merge.tag && tag.default);
function addMergeToJSMap(ctx, map2, value) {
  const source = resolveAliasValue(ctx, value);
  if (isSeq(source))
    for (const it of source.items)
      mergeValue(ctx, map2, it);
  else if (Array.isArray(source))
    for (const it of source)
      mergeValue(ctx, map2, it);
  else
    mergeValue(ctx, map2, source);
}
function mergeValue(ctx, map2, value) {
  const source = resolveAliasValue(ctx, value);
  if (!isMap(source))
    throw new Error("Merge sources must be maps or map aliases");
  const srcMap = source.toJSON(null, ctx, Map);
  for (const [key, value2] of srcMap) {
    if (map2 instanceof Map) {
      if (!map2.has(key))
        map2.set(key, value2);
    } else if (map2 instanceof Set) {
      map2.add(key);
    } else if (!Object.prototype.hasOwnProperty.call(map2, key)) {
      Object.defineProperty(map2, key, {
        value: value2,
        writable: true,
        enumerable: true,
        configurable: true
      });
    }
  }
  return map2;
}
function resolveAliasValue(ctx, value) {
  return ctx && isAlias(value) ? value.resolve(ctx.doc, ctx) : value;
}

// node_modules/yaml/browser/dist/nodes/addPairToJSMap.js
function addPairToJSMap(ctx, map2, { key, value }) {
  if (isNode(key) && key.addToJSMap)
    key.addToJSMap(ctx, map2, value);
  else if (isMergeKey(ctx, key))
    addMergeToJSMap(ctx, map2, value);
  else {
    const jsKey = toJS(key, "", ctx);
    if (map2 instanceof Map) {
      map2.set(jsKey, toJS(value, jsKey, ctx));
    } else if (map2 instanceof Set) {
      map2.add(jsKey);
    } else {
      const stringKey = stringifyKey(key, jsKey, ctx);
      const jsValue = toJS(value, stringKey, ctx);
      if (stringKey in map2)
        Object.defineProperty(map2, stringKey, {
          value: jsValue,
          writable: true,
          enumerable: true,
          configurable: true
        });
      else
        map2[stringKey] = jsValue;
    }
  }
  return map2;
}
function stringifyKey(key, jsKey, ctx) {
  if (jsKey === null)
    return "";
  if (typeof jsKey !== "object")
    return String(jsKey);
  if (isNode(key) && ctx?.doc) {
    const strCtx = createStringifyContext(ctx.doc, {});
    strCtx.anchors = /* @__PURE__ */ new Set();
    for (const node of ctx.anchors.keys())
      strCtx.anchors.add(node.anchor);
    strCtx.inFlow = true;
    strCtx.inStringifyKey = true;
    const strKey = key.toString(strCtx);
    if (!ctx.mapKeyWarned) {
      let jsonStr = JSON.stringify(strKey);
      if (jsonStr.length > 40)
        jsonStr = jsonStr.substring(0, 36) + '..."';
      warn(ctx.doc.options.logLevel, `Keys with collection values will be stringified due to JS Object restrictions: ${jsonStr}. Set mapAsMap: true to use object keys.`);
      ctx.mapKeyWarned = true;
    }
    return strKey;
  }
  return JSON.stringify(jsKey);
}

// node_modules/yaml/browser/dist/nodes/Pair.js
function createPair(key, value, ctx) {
  const k = createNode(key, void 0, ctx);
  const v = createNode(value, void 0, ctx);
  return new Pair(k, v);
}
var Pair = class _Pair {
  constructor(key, value = null) {
    Object.defineProperty(this, NODE_TYPE, { value: PAIR });
    this.key = key;
    this.value = value;
  }
  clone(schema4) {
    let { key, value } = this;
    if (isNode(key))
      key = key.clone(schema4);
    if (isNode(value))
      value = value.clone(schema4);
    return new _Pair(key, value);
  }
  toJSON(_, ctx) {
    const pair = ctx?.mapAsMap ? /* @__PURE__ */ new Map() : {};
    return addPairToJSMap(ctx, pair, this);
  }
  toString(ctx, onComment, onChompKeep) {
    return ctx?.doc ? stringifyPair(this, ctx, onComment, onChompKeep) : JSON.stringify(this);
  }
};

// node_modules/yaml/browser/dist/stringify/stringifyCollection.js
function stringifyCollection(collection, ctx, options) {
  const flow = ctx.inFlow ?? collection.flow;
  const stringify4 = flow ? stringifyFlowCollection : stringifyBlockCollection;
  return stringify4(collection, ctx, options);
}
function stringifyBlockCollection({ comment, items }, ctx, { blockItemPrefix, flowChars, itemIndent, onChompKeep, onComment }) {
  const { indent, options: { commentString } } = ctx;
  const itemCtx = Object.assign({}, ctx, { indent: itemIndent, type: null });
  let chompKeep = false;
  const lines = [];
  for (let i = 0; i < items.length; ++i) {
    const item = items[i];
    let comment2 = null;
    if (isNode(item)) {
      if (!chompKeep && item.spaceBefore)
        lines.push("");
      addCommentBefore(ctx, lines, item.commentBefore, chompKeep);
      if (item.comment)
        comment2 = item.comment;
    } else if (isPair(item)) {
      const ik = isNode(item.key) ? item.key : null;
      if (ik) {
        if (!chompKeep && ik.spaceBefore)
          lines.push("");
        addCommentBefore(ctx, lines, ik.commentBefore, chompKeep);
      }
    }
    chompKeep = false;
    let str2 = stringify(item, itemCtx, () => comment2 = null, () => chompKeep = true);
    if (comment2)
      str2 += lineComment(str2, itemIndent, commentString(comment2));
    if (chompKeep && comment2)
      chompKeep = false;
    lines.push(blockItemPrefix + str2);
  }
  let str;
  if (lines.length === 0) {
    str = flowChars.start + flowChars.end;
  } else {
    str = lines[0];
    for (let i = 1; i < lines.length; ++i) {
      const line = lines[i];
      str += line ? `
${indent}${line}` : "\n";
    }
  }
  if (comment) {
    str += "\n" + indentComment(commentString(comment), indent);
    if (onComment)
      onComment();
  } else if (chompKeep && onChompKeep)
    onChompKeep();
  return str;
}
function stringifyFlowCollection({ items }, ctx, { flowChars, itemIndent }) {
  const { indent, indentStep, flowCollectionPadding: fcPadding, options: { commentString } } = ctx;
  itemIndent += indentStep;
  const itemCtx = Object.assign({}, ctx, {
    indent: itemIndent,
    inFlow: true,
    type: null
  });
  let reqNewline = false;
  let linesAtValue = 0;
  const lines = [];
  for (let i = 0; i < items.length; ++i) {
    const item = items[i];
    let comment = null;
    if (isNode(item)) {
      if (item.spaceBefore)
        lines.push("");
      addCommentBefore(ctx, lines, item.commentBefore, false);
      if (item.comment)
        comment = item.comment;
    } else if (isPair(item)) {
      const ik = isNode(item.key) ? item.key : null;
      if (ik) {
        if (ik.spaceBefore)
          lines.push("");
        addCommentBefore(ctx, lines, ik.commentBefore, false);
        if (ik.comment)
          reqNewline = true;
      }
      const iv = isNode(item.value) ? item.value : null;
      if (iv) {
        if (iv.comment)
          comment = iv.comment;
        if (iv.commentBefore)
          reqNewline = true;
      } else if (item.value == null && ik?.comment) {
        comment = ik.comment;
      }
    }
    if (comment)
      reqNewline = true;
    let str = stringify(item, itemCtx, () => comment = null);
    reqNewline || (reqNewline = lines.length > linesAtValue || str.includes("\n"));
    if (i < items.length - 1) {
      str += ",";
    } else if (ctx.options.trailingComma) {
      if (ctx.options.lineWidth > 0) {
        reqNewline || (reqNewline = lines.reduce((sum, line) => sum + line.length + 2, 2) + (str.length + 2) > ctx.options.lineWidth);
      }
      if (reqNewline) {
        str += ",";
      }
    }
    if (comment)
      str += lineComment(str, itemIndent, commentString(comment));
    lines.push(str);
    linesAtValue = lines.length;
  }
  const { start, end } = flowChars;
  if (lines.length === 0) {
    return start + end;
  } else {
    if (!reqNewline) {
      const len = lines.reduce((sum, line) => sum + line.length + 2, 2);
      reqNewline = ctx.options.lineWidth > 0 && len > ctx.options.lineWidth;
    }
    if (reqNewline) {
      let str = start;
      for (const line of lines)
        str += line ? `
${indentStep}${indent}${line}` : "\n";
      return `${str}
${indent}${end}`;
    } else {
      return `${start}${fcPadding}${lines.join(" ")}${fcPadding}${end}`;
    }
  }
}
function addCommentBefore({ indent, options: { commentString } }, lines, comment, chompKeep) {
  if (comment && chompKeep)
    comment = comment.replace(/^\n+/, "");
  if (comment) {
    const ic = indentComment(commentString(comment), indent);
    lines.push(ic.trimStart());
  }
}

// node_modules/yaml/browser/dist/nodes/YAMLMap.js
function findPair(items, key) {
  const k = isScalar(key) ? key.value : key;
  for (const it of items) {
    if (isPair(it)) {
      if (it.key === key || it.key === k)
        return it;
      if (isScalar(it.key) && it.key.value === k)
        return it;
    }
  }
  return void 0;
}
var YAMLMap = class extends Collection {
  static get tagName() {
    return "tag:yaml.org,2002:map";
  }
  constructor(schema4) {
    super(MAP, schema4);
    this.items = [];
  }
  /**
   * A generic collection parsing method that can be extended
   * to other node classes that inherit from YAMLMap
   */
  static from(schema4, obj, ctx) {
    const { keepUndefined, replacer } = ctx;
    const map2 = new this(schema4);
    const add = (key, value) => {
      if (typeof replacer === "function")
        value = replacer.call(obj, key, value);
      else if (Array.isArray(replacer) && !replacer.includes(key))
        return;
      if (value !== void 0 || keepUndefined)
        map2.items.push(createPair(key, value, ctx));
    };
    if (obj instanceof Map) {
      for (const [key, value] of obj)
        add(key, value);
    } else if (obj && typeof obj === "object") {
      for (const key of Object.keys(obj))
        add(key, obj[key]);
    }
    if (typeof schema4.sortMapEntries === "function") {
      map2.items.sort(schema4.sortMapEntries);
    }
    return map2;
  }
  /**
   * Adds a value to the collection.
   *
   * @param overwrite - If not set `true`, using a key that is already in the
   *   collection will throw. Otherwise, overwrites the previous value.
   */
  add(pair, overwrite) {
    let _pair;
    if (isPair(pair))
      _pair = pair;
    else if (!pair || typeof pair !== "object" || !("key" in pair)) {
      _pair = new Pair(pair, pair?.value);
    } else
      _pair = new Pair(pair.key, pair.value);
    const prev = findPair(this.items, _pair.key);
    const sortEntries = this.schema?.sortMapEntries;
    if (prev) {
      if (!overwrite)
        throw new Error(`Key ${_pair.key} already set`);
      if (isScalar(prev.value) && isScalarValue(_pair.value))
        prev.value.value = _pair.value;
      else
        prev.value = _pair.value;
    } else if (sortEntries) {
      const i = this.items.findIndex((item) => sortEntries(_pair, item) < 0);
      if (i === -1)
        this.items.push(_pair);
      else
        this.items.splice(i, 0, _pair);
    } else {
      this.items.push(_pair);
    }
  }
  delete(key) {
    const it = findPair(this.items, key);
    if (!it)
      return false;
    const del = this.items.splice(this.items.indexOf(it), 1);
    return del.length > 0;
  }
  get(key, keepScalar) {
    const it = findPair(this.items, key);
    const node = it?.value;
    return (!keepScalar && isScalar(node) ? node.value : node) ?? void 0;
  }
  has(key) {
    return !!findPair(this.items, key);
  }
  set(key, value) {
    this.add(new Pair(key, value), true);
  }
  /**
   * @param ctx - Conversion context, originally set in Document#toJS()
   * @param {Class} Type - If set, forces the returned collection type
   * @returns Instance of Type, Map, or Object
   */
  toJSON(_, ctx, Type) {
    const map2 = Type ? new Type() : ctx?.mapAsMap ? /* @__PURE__ */ new Map() : {};
    if (ctx?.onCreate)
      ctx.onCreate(map2);
    for (const item of this.items)
      addPairToJSMap(ctx, map2, item);
    return map2;
  }
  toString(ctx, onComment, onChompKeep) {
    if (!ctx)
      return JSON.stringify(this);
    for (const item of this.items) {
      if (!isPair(item))
        throw new Error(`Map items must all be pairs; found ${JSON.stringify(item)} instead`);
    }
    if (!ctx.allNullValues && this.hasAllNullValues(false))
      ctx = Object.assign({}, ctx, { allNullValues: true });
    return stringifyCollection(this, ctx, {
      blockItemPrefix: "",
      flowChars: { start: "{", end: "}" },
      itemIndent: ctx.indent || "",
      onChompKeep,
      onComment
    });
  }
};

// node_modules/yaml/browser/dist/schema/common/map.js
var map = {
  collection: "map",
  default: true,
  nodeClass: YAMLMap,
  tag: "tag:yaml.org,2002:map",
  resolve(map2, onError) {
    if (!isMap(map2))
      onError("Expected a mapping for this tag");
    return map2;
  },
  createNode: (schema4, obj, ctx) => YAMLMap.from(schema4, obj, ctx)
};

// node_modules/yaml/browser/dist/nodes/YAMLSeq.js
var YAMLSeq = class extends Collection {
  static get tagName() {
    return "tag:yaml.org,2002:seq";
  }
  constructor(schema4) {
    super(SEQ, schema4);
    this.items = [];
  }
  add(value) {
    this.items.push(value);
  }
  /**
   * Removes a value from the collection.
   *
   * `key` must contain a representation of an integer for this to succeed.
   * It may be wrapped in a `Scalar`.
   *
   * @returns `true` if the item was found and removed.
   */
  delete(key) {
    const idx = asItemIndex(key);
    if (typeof idx !== "number")
      return false;
    const del = this.items.splice(idx, 1);
    return del.length > 0;
  }
  get(key, keepScalar) {
    const idx = asItemIndex(key);
    if (typeof idx !== "number")
      return void 0;
    const it = this.items[idx];
    return !keepScalar && isScalar(it) ? it.value : it;
  }
  /**
   * Checks if the collection includes a value with the key `key`.
   *
   * `key` must contain a representation of an integer for this to succeed.
   * It may be wrapped in a `Scalar`.
   */
  has(key) {
    const idx = asItemIndex(key);
    return typeof idx === "number" && idx < this.items.length;
  }
  /**
   * Sets a value in this collection. For `!!set`, `value` needs to be a
   * boolean to add/remove the item from the set.
   *
   * If `key` does not contain a representation of an integer, this will throw.
   * It may be wrapped in a `Scalar`.
   */
  set(key, value) {
    const idx = asItemIndex(key);
    if (typeof idx !== "number")
      throw new Error(`Expected a valid index, not ${key}.`);
    const prev = this.items[idx];
    if (isScalar(prev) && isScalarValue(value))
      prev.value = value;
    else
      this.items[idx] = value;
  }
  toJSON(_, ctx) {
    const seq2 = [];
    if (ctx?.onCreate)
      ctx.onCreate(seq2);
    let i = 0;
    for (const item of this.items)
      seq2.push(toJS(item, String(i++), ctx));
    return seq2;
  }
  toString(ctx, onComment, onChompKeep) {
    if (!ctx)
      return JSON.stringify(this);
    return stringifyCollection(this, ctx, {
      blockItemPrefix: "- ",
      flowChars: { start: "[", end: "]" },
      itemIndent: (ctx.indent || "") + "  ",
      onChompKeep,
      onComment
    });
  }
  static from(schema4, obj, ctx) {
    const { replacer } = ctx;
    const seq2 = new this(schema4);
    if (obj && Symbol.iterator in Object(obj)) {
      let i = 0;
      for (let it of obj) {
        if (typeof replacer === "function") {
          const key = obj instanceof Set ? it : String(i++);
          it = replacer.call(obj, key, it);
        }
        seq2.items.push(createNode(it, void 0, ctx));
      }
    }
    return seq2;
  }
};
function asItemIndex(key) {
  let idx = isScalar(key) ? key.value : key;
  if (idx && typeof idx === "string")
    idx = Number(idx);
  return typeof idx === "number" && Number.isInteger(idx) && idx >= 0 ? idx : null;
}

// node_modules/yaml/browser/dist/schema/common/seq.js
var seq = {
  collection: "seq",
  default: true,
  nodeClass: YAMLSeq,
  tag: "tag:yaml.org,2002:seq",
  resolve(seq2, onError) {
    if (!isSeq(seq2))
      onError("Expected a sequence for this tag");
    return seq2;
  },
  createNode: (schema4, obj, ctx) => YAMLSeq.from(schema4, obj, ctx)
};

// node_modules/yaml/browser/dist/schema/common/string.js
var string = {
  identify: (value) => typeof value === "string",
  default: true,
  tag: "tag:yaml.org,2002:str",
  resolve: (str) => str,
  stringify(item, ctx, onComment, onChompKeep) {
    ctx = Object.assign({ actualString: true }, ctx);
    return stringifyString(item, ctx, onComment, onChompKeep);
  }
};

// node_modules/yaml/browser/dist/schema/common/null.js
var nullTag = {
  identify: (value) => value == null,
  createNode: () => new Scalar(null),
  default: true,
  tag: "tag:yaml.org,2002:null",
  test: /^(?:~|[Nn]ull|NULL)?$/,
  resolve: () => new Scalar(null),
  stringify: ({ source }, ctx) => typeof source === "string" && nullTag.test.test(source) ? source : ctx.options.nullStr
};

// node_modules/yaml/browser/dist/schema/core/bool.js
var boolTag = {
  identify: (value) => typeof value === "boolean",
  default: true,
  tag: "tag:yaml.org,2002:bool",
  test: /^(?:[Tt]rue|TRUE|[Ff]alse|FALSE)$/,
  resolve: (str) => new Scalar(str[0] === "t" || str[0] === "T"),
  stringify({ source, value }, ctx) {
    if (source && boolTag.test.test(source)) {
      const sv = source[0] === "t" || source[0] === "T";
      if (value === sv)
        return source;
    }
    return value ? ctx.options.trueStr : ctx.options.falseStr;
  }
};

// node_modules/yaml/browser/dist/stringify/stringifyNumber.js
function stringifyNumber({ format, minFractionDigits, tag, value }) {
  if (typeof value === "bigint")
    return String(value);
  const num = typeof value === "number" ? value : Number(value);
  if (!isFinite(num))
    return isNaN(num) ? ".nan" : num < 0 ? "-.inf" : ".inf";
  let n = Object.is(value, -0) ? "-0" : JSON.stringify(value);
  if (!format && minFractionDigits && (!tag || tag === "tag:yaml.org,2002:float") && /^-?\d/.test(n) && !n.includes("e")) {
    let i = n.indexOf(".");
    if (i < 0) {
      i = n.length;
      n += ".";
    }
    let d = minFractionDigits - (n.length - i - 1);
    while (d-- > 0)
      n += "0";
  }
  return n;
}

// node_modules/yaml/browser/dist/schema/core/float.js
var floatNaN = {
  identify: (value) => typeof value === "number",
  default: true,
  tag: "tag:yaml.org,2002:float",
  test: /^(?:[-+]?\.(?:inf|Inf|INF)|\.nan|\.NaN|\.NAN)$/,
  resolve: (str) => str.slice(-3).toLowerCase() === "nan" ? NaN : str[0] === "-" ? Number.NEGATIVE_INFINITY : Number.POSITIVE_INFINITY,
  stringify: stringifyNumber
};
var floatExp = {
  identify: (value) => typeof value === "number",
  default: true,
  tag: "tag:yaml.org,2002:float",
  format: "EXP",
  test: /^[-+]?(?:\.[0-9]+|[0-9]+(?:\.[0-9]*)?)[eE][-+]?[0-9]+$/,
  resolve: (str) => parseFloat(str),
  stringify(node) {
    const num = Number(node.value);
    return isFinite(num) ? num.toExponential() : stringifyNumber(node);
  }
};
var float = {
  identify: (value) => typeof value === "number",
  default: true,
  tag: "tag:yaml.org,2002:float",
  test: /^[-+]?(?:\.[0-9]+|[0-9]+\.[0-9]*)$/,
  resolve(str) {
    const node = new Scalar(parseFloat(str));
    const dot = str.indexOf(".");
    if (dot !== -1 && str[str.length - 1] === "0")
      node.minFractionDigits = str.length - dot - 1;
    return node;
  },
  stringify: stringifyNumber
};

// node_modules/yaml/browser/dist/schema/core/int.js
var intIdentify = (value) => typeof value === "bigint" || Number.isInteger(value);
var intResolve = (str, offset, radix, { intAsBigInt }) => intAsBigInt ? BigInt(str) : parseInt(str.substring(offset), radix);
function intStringify(node, radix, prefix) {
  const { value } = node;
  if (intIdentify(value) && value >= 0)
    return prefix + value.toString(radix);
  return stringifyNumber(node);
}
var intOct = {
  identify: (value) => intIdentify(value) && value >= 0,
  default: true,
  tag: "tag:yaml.org,2002:int",
  format: "OCT",
  test: /^0o[0-7]+$/,
  resolve: (str, _onError, opt) => intResolve(str, 2, 8, opt),
  stringify: (node) => intStringify(node, 8, "0o")
};
var int = {
  identify: intIdentify,
  default: true,
  tag: "tag:yaml.org,2002:int",
  test: /^[-+]?[0-9]+$/,
  resolve: (str, _onError, opt) => intResolve(str, 0, 10, opt),
  stringify: stringifyNumber
};
var intHex = {
  identify: (value) => intIdentify(value) && value >= 0,
  default: true,
  tag: "tag:yaml.org,2002:int",
  format: "HEX",
  test: /^0x[0-9a-fA-F]+$/,
  resolve: (str, _onError, opt) => intResolve(str, 2, 16, opt),
  stringify: (node) => intStringify(node, 16, "0x")
};

// node_modules/yaml/browser/dist/schema/core/schema.js
var schema = [
  map,
  seq,
  string,
  nullTag,
  boolTag,
  intOct,
  int,
  intHex,
  floatNaN,
  floatExp,
  float
];

// node_modules/yaml/browser/dist/schema/json/schema.js
function intIdentify2(value) {
  return typeof value === "bigint" || Number.isInteger(value);
}
var stringifyJSON = ({ value }) => JSON.stringify(value);
var jsonScalars = [
  {
    identify: (value) => typeof value === "string",
    default: true,
    tag: "tag:yaml.org,2002:str",
    resolve: (str) => str,
    stringify: stringifyJSON
  },
  {
    identify: (value) => value == null,
    createNode: () => new Scalar(null),
    default: true,
    tag: "tag:yaml.org,2002:null",
    test: /^null$/,
    resolve: () => null,
    stringify: stringifyJSON
  },
  {
    identify: (value) => typeof value === "boolean",
    default: true,
    tag: "tag:yaml.org,2002:bool",
    test: /^true$|^false$/,
    resolve: (str) => str === "true",
    stringify: stringifyJSON
  },
  {
    identify: intIdentify2,
    default: true,
    tag: "tag:yaml.org,2002:int",
    test: /^-?(?:0|[1-9][0-9]*)$/,
    resolve: (str, _onError, { intAsBigInt }) => intAsBigInt ? BigInt(str) : parseInt(str, 10),
    stringify: ({ value }) => intIdentify2(value) ? value.toString() : JSON.stringify(value)
  },
  {
    identify: (value) => typeof value === "number",
    default: true,
    tag: "tag:yaml.org,2002:float",
    test: /^-?(?:0|[1-9][0-9]*)(?:\.[0-9]*)?(?:[eE][-+]?[0-9]+)?$/,
    resolve: (str) => parseFloat(str),
    stringify: stringifyJSON
  }
];
var jsonError = {
  default: true,
  tag: "",
  test: /^/,
  resolve(str, onError) {
    onError(`Unresolved plain scalar ${JSON.stringify(str)}`);
    return str;
  }
};
var schema2 = [map, seq].concat(jsonScalars, jsonError);

// node_modules/yaml/browser/dist/schema/yaml-1.1/binary.js
var binary = {
  identify: (value) => value instanceof Uint8Array,
  // Buffer inherits from Uint8Array
  default: false,
  tag: "tag:yaml.org,2002:binary",
  /**
   * Returns a Buffer in node and an Uint8Array in browsers
   *
   * To use the resulting buffer as an image, you'll want to do something like:
   *
   *   const blob = new Blob([buffer], { type: 'image/jpeg' })
   *   document.querySelector('#photo').src = URL.createObjectURL(blob)
   */
  resolve(src, onError) {
    if (typeof atob === "function") {
      const str = atob(src.replace(/[\n\r]/g, ""));
      const buffer = new Uint8Array(str.length);
      for (let i = 0; i < str.length; ++i)
        buffer[i] = str.charCodeAt(i);
      return buffer;
    } else {
      onError("This environment does not support reading binary tags; either Buffer or atob is required");
      return src;
    }
  },
  stringify({ comment, type, value }, ctx, onComment, onChompKeep) {
    if (!value)
      return "";
    const buf = value;
    let str;
    if (typeof btoa === "function") {
      let s = "";
      for (let i = 0; i < buf.length; ++i)
        s += String.fromCharCode(buf[i]);
      str = btoa(s);
    } else {
      throw new Error("This environment does not support writing binary tags; either Buffer or btoa is required");
    }
    type ?? (type = Scalar.BLOCK_LITERAL);
    if (type !== Scalar.QUOTE_DOUBLE) {
      const lineWidth = Math.max(ctx.options.lineWidth - ctx.indent.length, ctx.options.minContentWidth);
      const n = Math.ceil(str.length / lineWidth);
      const lines = new Array(n);
      for (let i = 0, o = 0; i < n; ++i, o += lineWidth) {
        lines[i] = str.substr(o, lineWidth);
      }
      str = lines.join(type === Scalar.BLOCK_LITERAL ? "\n" : " ");
    }
    return stringifyString({ comment, type, value: str }, ctx, onComment, onChompKeep);
  }
};

// node_modules/yaml/browser/dist/schema/yaml-1.1/pairs.js
function resolvePairs(seq2, onError) {
  if (isSeq(seq2)) {
    for (let i = 0; i < seq2.items.length; ++i) {
      let item = seq2.items[i];
      if (isPair(item))
        continue;
      else if (isMap(item)) {
        if (item.items.length > 1)
          onError("Each pair must have its own sequence indicator");
        const pair = item.items[0] || new Pair(new Scalar(null));
        if (item.commentBefore)
          pair.key.commentBefore = pair.key.commentBefore ? `${item.commentBefore}
${pair.key.commentBefore}` : item.commentBefore;
        if (item.comment) {
          const cn = pair.value ?? pair.key;
          cn.comment = cn.comment ? `${item.comment}
${cn.comment}` : item.comment;
        }
        item = pair;
      }
      seq2.items[i] = isPair(item) ? item : new Pair(item);
    }
  } else
    onError("Expected a sequence for this tag");
  return seq2;
}
function createPairs(schema4, iterable, ctx) {
  const { replacer } = ctx;
  const pairs2 = new YAMLSeq(schema4);
  pairs2.tag = "tag:yaml.org,2002:pairs";
  let i = 0;
  if (iterable && Symbol.iterator in Object(iterable))
    for (let it of iterable) {
      if (typeof replacer === "function")
        it = replacer.call(iterable, String(i++), it);
      let key, value;
      if (Array.isArray(it)) {
        if (it.length === 2) {
          key = it[0];
          value = it[1];
        } else
          throw new TypeError(`Expected [key, value] tuple: ${it}`);
      } else if (it && it instanceof Object) {
        const keys = Object.keys(it);
        if (keys.length === 1) {
          key = keys[0];
          value = it[key];
        } else {
          throw new TypeError(`Expected tuple with one key, not ${keys.length} keys`);
        }
      } else {
        key = it;
      }
      pairs2.items.push(createPair(key, value, ctx));
    }
  return pairs2;
}
var pairs = {
  collection: "seq",
  default: false,
  tag: "tag:yaml.org,2002:pairs",
  resolve: resolvePairs,
  createNode: createPairs
};

// node_modules/yaml/browser/dist/schema/yaml-1.1/omap.js
var YAMLOMap = class _YAMLOMap extends YAMLSeq {
  constructor() {
    super();
    this.add = YAMLMap.prototype.add.bind(this);
    this.delete = YAMLMap.prototype.delete.bind(this);
    this.get = YAMLMap.prototype.get.bind(this);
    this.has = YAMLMap.prototype.has.bind(this);
    this.set = YAMLMap.prototype.set.bind(this);
    this.tag = _YAMLOMap.tag;
  }
  /**
   * If `ctx` is given, the return type is actually `Map<unknown, unknown>`,
   * but TypeScript won't allow widening the signature of a child method.
   */
  toJSON(_, ctx) {
    if (!ctx)
      return super.toJSON(_);
    const map2 = /* @__PURE__ */ new Map();
    if (ctx?.onCreate)
      ctx.onCreate(map2);
    for (const pair of this.items) {
      let key, value;
      if (isPair(pair)) {
        key = toJS(pair.key, "", ctx);
        value = toJS(pair.value, key, ctx);
      } else {
        key = toJS(pair, "", ctx);
      }
      if (map2.has(key))
        throw new Error("Ordered maps must not include duplicate keys");
      map2.set(key, value);
    }
    return map2;
  }
  static from(schema4, iterable, ctx) {
    const pairs2 = createPairs(schema4, iterable, ctx);
    const omap2 = new this();
    omap2.items = pairs2.items;
    return omap2;
  }
};
YAMLOMap.tag = "tag:yaml.org,2002:omap";
var omap = {
  collection: "seq",
  identify: (value) => value instanceof Map,
  nodeClass: YAMLOMap,
  default: false,
  tag: "tag:yaml.org,2002:omap",
  resolve(seq2, onError) {
    const pairs2 = resolvePairs(seq2, onError);
    const seenKeys = [];
    for (const { key } of pairs2.items) {
      if (isScalar(key)) {
        if (seenKeys.includes(key.value)) {
          onError(`Ordered maps must not include duplicate keys: ${key.value}`);
        } else {
          seenKeys.push(key.value);
        }
      }
    }
    return Object.assign(new YAMLOMap(), pairs2);
  },
  createNode: (schema4, iterable, ctx) => YAMLOMap.from(schema4, iterable, ctx)
};

// node_modules/yaml/browser/dist/schema/yaml-1.1/bool.js
function boolStringify({ value, source }, ctx) {
  const boolObj = value ? trueTag : falseTag;
  if (source && boolObj.test.test(source))
    return source;
  return value ? ctx.options.trueStr : ctx.options.falseStr;
}
var trueTag = {
  identify: (value) => value === true,
  default: true,
  tag: "tag:yaml.org,2002:bool",
  test: /^(?:Y|y|[Yy]es|YES|[Tt]rue|TRUE|[Oo]n|ON)$/,
  resolve: () => new Scalar(true),
  stringify: boolStringify
};
var falseTag = {
  identify: (value) => value === false,
  default: true,
  tag: "tag:yaml.org,2002:bool",
  test: /^(?:N|n|[Nn]o|NO|[Ff]alse|FALSE|[Oo]ff|OFF)$/,
  resolve: () => new Scalar(false),
  stringify: boolStringify
};

// node_modules/yaml/browser/dist/schema/yaml-1.1/float.js
var floatNaN2 = {
  identify: (value) => typeof value === "number",
  default: true,
  tag: "tag:yaml.org,2002:float",
  test: /^(?:[-+]?\.(?:inf|Inf|INF)|\.nan|\.NaN|\.NAN)$/,
  resolve: (str) => str.slice(-3).toLowerCase() === "nan" ? NaN : str[0] === "-" ? Number.NEGATIVE_INFINITY : Number.POSITIVE_INFINITY,
  stringify: stringifyNumber
};
var floatExp2 = {
  identify: (value) => typeof value === "number",
  default: true,
  tag: "tag:yaml.org,2002:float",
  format: "EXP",
  test: /^[-+]?(?:[0-9][0-9_]*)?(?:\.[0-9_]*)?[eE][-+]?[0-9]+$/,
  resolve: (str) => parseFloat(str.replace(/_/g, "")),
  stringify(node) {
    const num = Number(node.value);
    return isFinite(num) ? num.toExponential() : stringifyNumber(node);
  }
};
var float2 = {
  identify: (value) => typeof value === "number",
  default: true,
  tag: "tag:yaml.org,2002:float",
  test: /^[-+]?(?:[0-9][0-9_]*)?\.[0-9_]*$/,
  resolve(str) {
    const node = new Scalar(parseFloat(str.replace(/_/g, "")));
    const dot = str.indexOf(".");
    if (dot !== -1) {
      const f = str.substring(dot + 1).replace(/_/g, "");
      if (f[f.length - 1] === "0")
        node.minFractionDigits = f.length;
    }
    return node;
  },
  stringify: stringifyNumber
};

// node_modules/yaml/browser/dist/schema/yaml-1.1/int.js
var intIdentify3 = (value) => typeof value === "bigint" || Number.isInteger(value);
function intResolve2(str, offset, radix, { intAsBigInt }) {
  const sign = str[0];
  if (sign === "-" || sign === "+")
    offset += 1;
  str = str.substring(offset).replace(/_/g, "");
  if (intAsBigInt) {
    switch (radix) {
      case 2:
        str = `0b${str}`;
        break;
      case 8:
        str = `0o${str}`;
        break;
      case 16:
        str = `0x${str}`;
        break;
    }
    const n2 = BigInt(str);
    return sign === "-" ? BigInt(-1) * n2 : n2;
  }
  const n = parseInt(str, radix);
  return sign === "-" ? -1 * n : n;
}
function intStringify2(node, radix, prefix) {
  const { value } = node;
  if (intIdentify3(value)) {
    const str = value.toString(radix);
    return value < 0 ? "-" + prefix + str.substr(1) : prefix + str;
  }
  return stringifyNumber(node);
}
var intBin = {
  identify: intIdentify3,
  default: true,
  tag: "tag:yaml.org,2002:int",
  format: "BIN",
  test: /^[-+]?0b[0-1_]+$/,
  resolve: (str, _onError, opt) => intResolve2(str, 2, 2, opt),
  stringify: (node) => intStringify2(node, 2, "0b")
};
var intOct2 = {
  identify: intIdentify3,
  default: true,
  tag: "tag:yaml.org,2002:int",
  format: "OCT",
  test: /^[-+]?0[0-7_]+$/,
  resolve: (str, _onError, opt) => intResolve2(str, 1, 8, opt),
  stringify: (node) => intStringify2(node, 8, "0")
};
var int2 = {
  identify: intIdentify3,
  default: true,
  tag: "tag:yaml.org,2002:int",
  test: /^[-+]?[0-9][0-9_]*$/,
  resolve: (str, _onError, opt) => intResolve2(str, 0, 10, opt),
  stringify: stringifyNumber
};
var intHex2 = {
  identify: intIdentify3,
  default: true,
  tag: "tag:yaml.org,2002:int",
  format: "HEX",
  test: /^[-+]?0x[0-9a-fA-F_]+$/,
  resolve: (str, _onError, opt) => intResolve2(str, 2, 16, opt),
  stringify: (node) => intStringify2(node, 16, "0x")
};

// node_modules/yaml/browser/dist/schema/yaml-1.1/set.js
var YAMLSet = class _YAMLSet extends YAMLMap {
  constructor(schema4) {
    super(schema4);
    this.tag = _YAMLSet.tag;
  }
  add(key) {
    let pair;
    if (isPair(key))
      pair = key;
    else if (key && typeof key === "object" && "key" in key && "value" in key && key.value === null)
      pair = new Pair(key.key, null);
    else
      pair = new Pair(key, null);
    const prev = findPair(this.items, pair.key);
    if (!prev)
      this.items.push(pair);
  }
  /**
   * If `keepPair` is `true`, returns the Pair matching `key`.
   * Otherwise, returns the value of that Pair's key.
   */
  get(key, keepPair) {
    const pair = findPair(this.items, key);
    return !keepPair && isPair(pair) ? isScalar(pair.key) ? pair.key.value : pair.key : pair;
  }
  set(key, value) {
    if (typeof value !== "boolean")
      throw new Error(`Expected boolean value for set(key, value) in a YAML set, not ${typeof value}`);
    const prev = findPair(this.items, key);
    if (prev && !value) {
      this.items.splice(this.items.indexOf(prev), 1);
    } else if (!prev && value) {
      this.items.push(new Pair(key));
    }
  }
  toJSON(_, ctx) {
    return super.toJSON(_, ctx, Set);
  }
  toString(ctx, onComment, onChompKeep) {
    if (!ctx)
      return JSON.stringify(this);
    if (this.hasAllNullValues(true))
      return super.toString(Object.assign({}, ctx, { allNullValues: true }), onComment, onChompKeep);
    else
      throw new Error("Set items must all have null values");
  }
  static from(schema4, iterable, ctx) {
    const { replacer } = ctx;
    const set2 = new this(schema4);
    if (iterable && Symbol.iterator in Object(iterable))
      for (let value of iterable) {
        if (typeof replacer === "function")
          value = replacer.call(iterable, value, value);
        set2.items.push(createPair(value, null, ctx));
      }
    return set2;
  }
};
YAMLSet.tag = "tag:yaml.org,2002:set";
var set = {
  collection: "map",
  identify: (value) => value instanceof Set,
  nodeClass: YAMLSet,
  default: false,
  tag: "tag:yaml.org,2002:set",
  createNode: (schema4, iterable, ctx) => YAMLSet.from(schema4, iterable, ctx),
  resolve(map2, onError) {
    if (isMap(map2)) {
      if (map2.hasAllNullValues(true))
        return Object.assign(new YAMLSet(), map2);
      else
        onError("Set items must all have null values");
    } else
      onError("Expected a mapping for this tag");
    return map2;
  }
};

// node_modules/yaml/browser/dist/schema/yaml-1.1/timestamp.js
function parseSexagesimal(str, asBigInt) {
  const sign = str[0];
  const parts = sign === "-" || sign === "+" ? str.substring(1) : str;
  const num = (n) => asBigInt ? BigInt(n) : Number(n);
  const res = parts.replace(/_/g, "").split(":").reduce((res2, p) => res2 * num(60) + num(p), num(0));
  return sign === "-" ? num(-1) * res : res;
}
function stringifySexagesimal(node) {
  let { value } = node;
  let num = (n) => n;
  if (typeof value === "bigint")
    num = (n) => BigInt(n);
  else if (isNaN(value) || !isFinite(value))
    return stringifyNumber(node);
  let sign = "";
  if (value < 0) {
    sign = "-";
    value *= num(-1);
  }
  const _60 = num(60);
  const parts = [value % _60];
  if (value < 60) {
    parts.unshift(0);
  } else {
    value = (value - parts[0]) / _60;
    parts.unshift(value % _60);
    if (value >= 60) {
      value = (value - parts[0]) / _60;
      parts.unshift(value);
    }
  }
  return sign + parts.map((n) => String(n).padStart(2, "0")).join(":").replace(/000000\d*$/, "");
}
var intTime = {
  identify: (value) => typeof value === "bigint" || Number.isInteger(value),
  default: true,
  tag: "tag:yaml.org,2002:int",
  format: "TIME",
  test: /^[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+$/,
  resolve: (str, _onError, { intAsBigInt }) => parseSexagesimal(str, intAsBigInt),
  stringify: stringifySexagesimal
};
var floatTime = {
  identify: (value) => typeof value === "number",
  default: true,
  tag: "tag:yaml.org,2002:float",
  format: "TIME",
  test: /^[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+\.[0-9_]*$/,
  resolve: (str) => parseSexagesimal(str, false),
  stringify: stringifySexagesimal
};
var timestamp = {
  identify: (value) => value instanceof Date,
  default: true,
  tag: "tag:yaml.org,2002:timestamp",
  // If the time zone is omitted, the timestamp is assumed to be specified in UTC. The time part
  // may be omitted altogether, resulting in a date format. In such a case, the time part is
  // assumed to be 00:00:00Z (start of day, UTC).
  test: RegExp("^([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})(?:(?:t|T|[ \\t]+)([0-9]{1,2}):([0-9]{1,2}):([0-9]{1,2}(\\.[0-9]+)?)(?:[ \\t]*(Z|[-+][012]?[0-9](?::[0-9]{2})?))?)?$"),
  resolve(str) {
    const match = str.match(timestamp.test);
    if (!match)
      throw new Error("!!timestamp expects a date, starting with yyyy-mm-dd");
    const [, year, month, day, hour, minute, second] = match.map(Number);
    const millisec = match[7] ? Number((match[7] + "00").substr(1, 3)) : 0;
    let date = Date.UTC(year, month - 1, day, hour || 0, minute || 0, second || 0, millisec);
    const tz = match[8];
    if (tz && tz !== "Z") {
      let d = parseSexagesimal(tz, false);
      if (Math.abs(d) < 30)
        d *= 60;
      date -= 6e4 * d;
    }
    return new Date(date);
  },
  stringify: ({ value }) => value?.toISOString().replace(/(T00:00:00)?\.000Z$/, "") ?? ""
};

// node_modules/yaml/browser/dist/schema/yaml-1.1/schema.js
var schema3 = [
  map,
  seq,
  string,
  nullTag,
  trueTag,
  falseTag,
  intBin,
  intOct2,
  int2,
  intHex2,
  floatNaN2,
  floatExp2,
  float2,
  binary,
  merge,
  omap,
  pairs,
  set,
  intTime,
  floatTime,
  timestamp
];

// node_modules/yaml/browser/dist/schema/tags.js
var schemas = /* @__PURE__ */ new Map([
  ["core", schema],
  ["failsafe", [map, seq, string]],
  ["json", schema2],
  ["yaml11", schema3],
  ["yaml-1.1", schema3]
]);
var tagsByName = {
  binary,
  bool: boolTag,
  float,
  floatExp,
  floatNaN,
  floatTime,
  int,
  intHex,
  intOct,
  intTime,
  map,
  merge,
  null: nullTag,
  omap,
  pairs,
  seq,
  set,
  timestamp
};
var coreKnownTags = {
  "tag:yaml.org,2002:binary": binary,
  "tag:yaml.org,2002:merge": merge,
  "tag:yaml.org,2002:omap": omap,
  "tag:yaml.org,2002:pairs": pairs,
  "tag:yaml.org,2002:set": set,
  "tag:yaml.org,2002:timestamp": timestamp
};
function getTags(customTags, schemaName, addMergeTag) {
  const schemaTags = schemas.get(schemaName);
  if (schemaTags && !customTags) {
    return addMergeTag && !schemaTags.includes(merge) ? schemaTags.concat(merge) : schemaTags.slice();
  }
  let tags = schemaTags;
  if (!tags) {
    if (Array.isArray(customTags))
      tags = [];
    else {
      const keys = Array.from(schemas.keys()).filter((key) => key !== "yaml11").map((key) => JSON.stringify(key)).join(", ");
      throw new Error(`Unknown schema "${schemaName}"; use one of ${keys} or define customTags array`);
    }
  }
  if (Array.isArray(customTags)) {
    for (const tag of customTags)
      tags = tags.concat(tag);
  } else if (typeof customTags === "function") {
    tags = customTags(tags.slice());
  }
  if (addMergeTag)
    tags = tags.concat(merge);
  return tags.reduce((tags2, tag) => {
    const tagObj = typeof tag === "string" ? tagsByName[tag] : tag;
    if (!tagObj) {
      const tagName = JSON.stringify(tag);
      const keys = Object.keys(tagsByName).map((key) => JSON.stringify(key)).join(", ");
      throw new Error(`Unknown custom tag ${tagName}; use one of ${keys}`);
    }
    if (!tags2.includes(tagObj))
      tags2.push(tagObj);
    return tags2;
  }, []);
}

// node_modules/yaml/browser/dist/schema/Schema.js
var sortMapEntriesByKey = (a, b) => a.key < b.key ? -1 : a.key > b.key ? 1 : 0;
var Schema = class _Schema {
  constructor({ compat, customTags, merge: merge2, resolveKnownTags, schema: schema4, sortMapEntries, toStringDefaults }) {
    this.compat = Array.isArray(compat) ? getTags(compat, "compat") : compat ? getTags(null, compat) : null;
    this.name = typeof schema4 === "string" && schema4 || "core";
    this.knownTags = resolveKnownTags ? coreKnownTags : {};
    this.tags = getTags(customTags, this.name, merge2);
    this.toStringOptions = toStringDefaults ?? null;
    Object.defineProperty(this, MAP, { value: map });
    Object.defineProperty(this, SCALAR, { value: string });
    Object.defineProperty(this, SEQ, { value: seq });
    this.sortMapEntries = typeof sortMapEntries === "function" ? sortMapEntries : sortMapEntries === true ? sortMapEntriesByKey : null;
  }
  clone() {
    const copy = Object.create(_Schema.prototype, Object.getOwnPropertyDescriptors(this));
    copy.tags = this.tags.slice();
    return copy;
  }
};

// node_modules/yaml/browser/dist/stringify/stringifyDocument.js
function stringifyDocument(doc, options) {
  const lines = [];
  let hasDirectives = options.directives === true;
  if (options.directives !== false && doc.directives) {
    const dir = doc.directives.toString(doc);
    if (dir) {
      lines.push(dir);
      hasDirectives = true;
    } else if (doc.directives.docStart)
      hasDirectives = true;
  }
  if (hasDirectives)
    lines.push("---");
  const ctx = createStringifyContext(doc, options);
  const { commentString } = ctx.options;
  if (doc.commentBefore) {
    if (lines.length !== 1)
      lines.unshift("");
    const cs = commentString(doc.commentBefore);
    lines.unshift(indentComment(cs, ""));
  }
  let chompKeep = false;
  let contentComment = null;
  if (doc.contents) {
    if (isNode(doc.contents)) {
      if (doc.contents.spaceBefore && hasDirectives)
        lines.push("");
      if (doc.contents.commentBefore) {
        const cs = commentString(doc.contents.commentBefore);
        lines.push(indentComment(cs, ""));
      }
      ctx.forceBlockIndent = !!doc.comment;
      contentComment = doc.contents.comment;
    }
    const onChompKeep = contentComment ? void 0 : () => chompKeep = true;
    let body = stringify(doc.contents, ctx, () => contentComment = null, onChompKeep);
    if (contentComment)
      body += lineComment(body, "", commentString(contentComment));
    if ((body[0] === "|" || body[0] === ">") && lines[lines.length - 1] === "---") {
      lines[lines.length - 1] = `--- ${body}`;
    } else
      lines.push(body);
  } else {
    lines.push(stringify(doc.contents, ctx));
  }
  if (doc.directives?.docEnd) {
    if (doc.comment) {
      const cs = commentString(doc.comment);
      if (cs.includes("\n")) {
        lines.push("...");
        lines.push(indentComment(cs, ""));
      } else {
        lines.push(`... ${cs}`);
      }
    } else {
      lines.push("...");
    }
  } else {
    let dc = doc.comment;
    if (dc && chompKeep)
      dc = dc.replace(/^\n+/, "");
    if (dc) {
      if ((!chompKeep || contentComment) && lines[lines.length - 1] !== "")
        lines.push("");
      lines.push(indentComment(commentString(dc), ""));
    }
  }
  return lines.join("\n") + "\n";
}

// node_modules/yaml/browser/dist/doc/Document.js
var Document = class _Document {
  constructor(value, replacer, options) {
    this.commentBefore = null;
    this.comment = null;
    this.errors = [];
    this.warnings = [];
    Object.defineProperty(this, NODE_TYPE, { value: DOC });
    let _replacer = null;
    if (typeof replacer === "function" || Array.isArray(replacer)) {
      _replacer = replacer;
    } else if (options === void 0 && replacer) {
      options = replacer;
      replacer = void 0;
    }
    const opt = Object.assign({
      intAsBigInt: false,
      keepSourceTokens: false,
      logLevel: "warn",
      prettyErrors: true,
      strict: true,
      stringKeys: false,
      uniqueKeys: true,
      version: "1.2"
    }, options);
    this.options = opt;
    let { version } = opt;
    if (options?._directives) {
      this.directives = options._directives.atDocument();
      if (this.directives.yaml.explicit)
        version = this.directives.yaml.version;
    } else
      this.directives = new Directives({ version });
    this.setSchema(version, options);
    this.contents = value === void 0 ? null : this.createNode(value, _replacer, options);
  }
  /**
   * Create a deep copy of this Document and its contents.
   *
   * Custom Node values that inherit from `Object` still refer to their original instances.
   */
  clone() {
    const copy = Object.create(_Document.prototype, {
      [NODE_TYPE]: { value: DOC }
    });
    copy.commentBefore = this.commentBefore;
    copy.comment = this.comment;
    copy.errors = this.errors.slice();
    copy.warnings = this.warnings.slice();
    copy.options = Object.assign({}, this.options);
    if (this.directives)
      copy.directives = this.directives.clone();
    copy.schema = this.schema.clone();
    copy.contents = isNode(this.contents) ? this.contents.clone(copy.schema) : this.contents;
    if (this.range)
      copy.range = this.range.slice();
    return copy;
  }
  /** Adds a value to the document. */
  add(value) {
    if (assertCollection(this.contents))
      this.contents.add(value);
  }
  /** Adds a value to the document. */
  addIn(path, value) {
    if (assertCollection(this.contents))
      this.contents.addIn(path, value);
  }
  /**
   * Create a new `Alias` node, ensuring that the target `node` has the required anchor.
   *
   * If `node` already has an anchor, `name` is ignored.
   * Otherwise, the `node.anchor` value will be set to `name`,
   * or if an anchor with that name is already present in the document,
   * `name` will be used as a prefix for a new unique anchor.
   * If `name` is undefined, the generated anchor will use 'a' as a prefix.
   */
  createAlias(node, name) {
    if (!node.anchor) {
      const prev = anchorNames(this);
      node.anchor = // eslint-disable-next-line @typescript-eslint/prefer-nullish-coalescing
      !name || prev.has(name) ? findNewAnchor(name || "a", prev) : name;
    }
    return new Alias(node.anchor);
  }
  createNode(value, replacer, options) {
    let _replacer = void 0;
    if (typeof replacer === "function") {
      value = replacer.call({ "": value }, "", value);
      _replacer = replacer;
    } else if (Array.isArray(replacer)) {
      const keyToStr = (v) => typeof v === "number" || v instanceof String || v instanceof Number;
      const asStr = replacer.filter(keyToStr).map(String);
      if (asStr.length > 0)
        replacer = replacer.concat(asStr);
      _replacer = replacer;
    } else if (options === void 0 && replacer) {
      options = replacer;
      replacer = void 0;
    }
    const { aliasDuplicateObjects, anchorPrefix, flow, keepUndefined, onTagObj, tag } = options ?? {};
    const { onAnchor, setAnchors, sourceObjects } = createNodeAnchors(
      this,
      // eslint-disable-next-line @typescript-eslint/prefer-nullish-coalescing
      anchorPrefix || "a"
    );
    const ctx = {
      aliasDuplicateObjects: aliasDuplicateObjects ?? true,
      keepUndefined: keepUndefined ?? false,
      onAnchor,
      onTagObj,
      replacer: _replacer,
      schema: this.schema,
      sourceObjects
    };
    const node = createNode(value, tag, ctx);
    if (flow && isCollection(node))
      node.flow = true;
    setAnchors();
    return node;
  }
  /**
   * Convert a key and a value into a `Pair` using the current schema,
   * recursively wrapping all values as `Scalar` or `Collection` nodes.
   */
  createPair(key, value, options = {}) {
    const k = this.createNode(key, null, options);
    const v = this.createNode(value, null, options);
    return new Pair(k, v);
  }
  /**
   * Removes a value from the document.
   * @returns `true` if the item was found and removed.
   */
  delete(key) {
    return assertCollection(this.contents) ? this.contents.delete(key) : false;
  }
  /**
   * Removes a value from the document.
   * @returns `true` if the item was found and removed.
   */
  deleteIn(path) {
    if (isEmptyPath(path)) {
      if (this.contents == null)
        return false;
      this.contents = null;
      return true;
    }
    return assertCollection(this.contents) ? this.contents.deleteIn(path) : false;
  }
  /**
   * Returns item at `key`, or `undefined` if not found. By default unwraps
   * scalar values from their surrounding node; to disable set `keepScalar` to
   * `true` (collections are always returned intact).
   */
  get(key, keepScalar) {
    return isCollection(this.contents) ? this.contents.get(key, keepScalar) : void 0;
  }
  /**
   * Returns item at `path`, or `undefined` if not found. By default unwraps
   * scalar values from their surrounding node; to disable set `keepScalar` to
   * `true` (collections are always returned intact).
   */
  getIn(path, keepScalar) {
    if (isEmptyPath(path))
      return !keepScalar && isScalar(this.contents) ? this.contents.value : this.contents;
    return isCollection(this.contents) ? this.contents.getIn(path, keepScalar) : void 0;
  }
  /**
   * Checks if the document includes a value with the key `key`.
   */
  has(key) {
    return isCollection(this.contents) ? this.contents.has(key) : false;
  }
  /**
   * Checks if the document includes a value at `path`.
   */
  hasIn(path) {
    if (isEmptyPath(path))
      return this.contents !== void 0;
    return isCollection(this.contents) ? this.contents.hasIn(path) : false;
  }
  /**
   * Sets a value in this document. For `!!set`, `value` needs to be a
   * boolean to add/remove the item from the set.
   */
  set(key, value) {
    if (this.contents == null) {
      this.contents = collectionFromPath(this.schema, [key], value);
    } else if (assertCollection(this.contents)) {
      this.contents.set(key, value);
    }
  }
  /**
   * Sets a value in this document. For `!!set`, `value` needs to be a
   * boolean to add/remove the item from the set.
   */
  setIn(path, value) {
    if (isEmptyPath(path)) {
      this.contents = value;
    } else if (this.contents == null) {
      this.contents = collectionFromPath(this.schema, Array.from(path), value);
    } else if (assertCollection(this.contents)) {
      this.contents.setIn(path, value);
    }
  }
  /**
   * Change the YAML version and schema used by the document.
   * A `null` version disables support for directives, explicit tags, anchors, and aliases.
   * It also requires the `schema` option to be given as a `Schema` instance value.
   *
   * Overrides all previously set schema options.
   */
  setSchema(version, options = {}) {
    if (typeof version === "number")
      version = String(version);
    let opt;
    switch (version) {
      case "1.1":
        if (this.directives)
          this.directives.yaml.version = "1.1";
        else
          this.directives = new Directives({ version: "1.1" });
        opt = { resolveKnownTags: false, schema: "yaml-1.1" };
        break;
      case "1.2":
      case "next":
        if (this.directives)
          this.directives.yaml.version = version;
        else
          this.directives = new Directives({ version });
        opt = { resolveKnownTags: true, schema: "core" };
        break;
      case null:
        if (this.directives)
          delete this.directives;
        opt = null;
        break;
      default: {
        const sv = JSON.stringify(version);
        throw new Error(`Expected '1.1', '1.2' or null as first argument, but found: ${sv}`);
      }
    }
    if (options.schema instanceof Object)
      this.schema = options.schema;
    else if (opt)
      this.schema = new Schema(Object.assign(opt, options));
    else
      throw new Error(`With a null YAML version, the { schema: Schema } option is required`);
  }
  // json & jsonArg are only used from toJSON()
  toJS({ json, jsonArg, mapAsMap, maxAliasCount, onAnchor, reviver } = {}) {
    const ctx = {
      anchors: /* @__PURE__ */ new Map(),
      doc: this,
      keep: !json,
      mapAsMap: mapAsMap === true,
      mapKeyWarned: false,
      maxAliasCount: typeof maxAliasCount === "number" ? maxAliasCount : 100
    };
    const res = toJS(this.contents, jsonArg ?? "", ctx);
    if (typeof onAnchor === "function")
      for (const { count, res: res2 } of ctx.anchors.values())
        onAnchor(res2, count);
    return typeof reviver === "function" ? applyReviver(reviver, { "": res }, "", res) : res;
  }
  /**
   * A JSON representation of the document `contents`.
   *
   * @param jsonArg Used by `JSON.stringify` to indicate the array index or
   *   property name.
   */
  toJSON(jsonArg, onAnchor) {
    return this.toJS({ json: true, jsonArg, mapAsMap: false, onAnchor });
  }
  /** A YAML representation of the document. */
  toString(options = {}) {
    if (this.errors.length > 0)
      throw new Error("Document with errors cannot be stringified");
    if ("indent" in options && (!Number.isInteger(options.indent) || Number(options.indent) <= 0)) {
      const s = JSON.stringify(options.indent);
      throw new Error(`"indent" option must be a positive integer, not ${s}`);
    }
    return stringifyDocument(this, options);
  }
};
function assertCollection(contents) {
  if (isCollection(contents))
    return true;
  throw new Error("Expected a YAML collection as document contents");
}

// node_modules/yaml/browser/dist/errors.js
var YAMLError = class extends Error {
  constructor(name, pos, code, message) {
    super();
    this.name = name;
    this.code = code;
    this.message = message;
    this.pos = pos;
  }
};
var YAMLParseError = class extends YAMLError {
  constructor(pos, code, message) {
    super("YAMLParseError", pos, code, message);
  }
};
var YAMLWarning = class extends YAMLError {
  constructor(pos, code, message) {
    super("YAMLWarning", pos, code, message);
  }
};
var prettifyError = (src, lc) => (error) => {
  if (error.pos[0] === -1)
    return;
  error.linePos = error.pos.map((pos) => lc.linePos(pos));
  const { line, col } = error.linePos[0];
  error.message += ` at line ${line}, column ${col}`;
  let ci2 = col - 1;
  let lineStr = src.substring(lc.lineStarts[line - 1], lc.lineStarts[line]).replace(/[\n\r]+$/, "");
  if (ci2 >= 60 && lineStr.length > 80) {
    const trimStart = Math.min(ci2 - 39, lineStr.length - 79);
    lineStr = "\u2026" + lineStr.substring(trimStart);
    ci2 -= trimStart - 1;
  }
  if (lineStr.length > 80)
    lineStr = lineStr.substring(0, 79) + "\u2026";
  if (line > 1 && /^ *$/.test(lineStr.substring(0, ci2))) {
    let prev = src.substring(lc.lineStarts[line - 2], lc.lineStarts[line - 1]);
    if (prev.length > 80)
      prev = prev.substring(0, 79) + "\u2026\n";
    lineStr = prev + lineStr;
  }
  if (/[^ ]/.test(lineStr)) {
    let count = 1;
    const end = error.linePos[1];
    if (end?.line === line && end.col > col) {
      count = Math.max(1, Math.min(end.col - col, 80 - ci2));
    }
    const pointer = " ".repeat(ci2) + "^".repeat(count);
    error.message += `:

${lineStr}
${pointer}
`;
  }
};

// node_modules/yaml/browser/dist/compose/resolve-props.js
function resolveProps(tokens, { flow, indicator, next, offset, onError, parentIndent, startOnNewline }) {
  let spaceBefore = false;
  let atNewline = startOnNewline;
  let hasSpace = startOnNewline;
  let comment = "";
  let commentSep = "";
  let hasNewline = false;
  let reqSpace = false;
  let tab = null;
  let anchor = null;
  let tag = null;
  let newlineAfterProp = null;
  let comma = null;
  let found = null;
  let start = null;
  for (const token of tokens) {
    if (reqSpace) {
      if (token.type !== "space" && token.type !== "newline" && token.type !== "comma")
        onError(token.offset, "MISSING_CHAR", "Tags and anchors must be separated from the next token by white space");
      reqSpace = false;
    }
    if (tab) {
      if (atNewline && token.type !== "comment" && token.type !== "newline") {
        onError(tab, "TAB_AS_INDENT", "Tabs are not allowed as indentation");
      }
      tab = null;
    }
    switch (token.type) {
      case "space":
        if (!flow && (indicator !== "doc-start" || next?.type !== "flow-collection") && token.source.includes("	")) {
          tab = token;
        }
        hasSpace = true;
        break;
      case "comment": {
        if (!hasSpace)
          onError(token, "MISSING_CHAR", "Comments must be separated from other tokens by white space characters");
        const cb = token.source.substring(1) || " ";
        if (!comment)
          comment = cb;
        else
          comment += commentSep + cb;
        commentSep = "";
        atNewline = false;
        break;
      }
      case "newline":
        if (atNewline) {
          if (comment)
            comment += token.source;
          else if (!found || indicator !== "seq-item-ind")
            spaceBefore = true;
        } else
          commentSep += token.source;
        atNewline = true;
        hasNewline = true;
        if (anchor || tag)
          newlineAfterProp = token;
        hasSpace = true;
        break;
      case "anchor":
        if (anchor)
          onError(token, "MULTIPLE_ANCHORS", "A node can have at most one anchor");
        if (token.source.endsWith(":"))
          onError(token.offset + token.source.length - 1, "BAD_ALIAS", "Anchor ending in : is ambiguous", true);
        anchor = token;
        start ?? (start = token.offset);
        atNewline = false;
        hasSpace = false;
        reqSpace = true;
        break;
      case "tag": {
        if (tag)
          onError(token, "MULTIPLE_TAGS", "A node can have at most one tag");
        tag = token;
        start ?? (start = token.offset);
        atNewline = false;
        hasSpace = false;
        reqSpace = true;
        break;
      }
      case indicator:
        if (anchor || tag)
          onError(token, "BAD_PROP_ORDER", `Anchors and tags must be after the ${token.source} indicator`);
        if (found)
          onError(token, "UNEXPECTED_TOKEN", `Unexpected ${token.source} in ${flow ?? "collection"}`);
        found = token;
        atNewline = indicator === "seq-item-ind" || indicator === "explicit-key-ind";
        hasSpace = false;
        break;
      case "comma":
        if (flow) {
          if (comma)
            onError(token, "UNEXPECTED_TOKEN", `Unexpected , in ${flow}`);
          comma = token;
          atNewline = false;
          hasSpace = false;
          break;
        }
      // else fallthrough
      default:
        onError(token, "UNEXPECTED_TOKEN", `Unexpected ${token.type} token`);
        atNewline = false;
        hasSpace = false;
    }
  }
  const last = tokens[tokens.length - 1];
  const end = last ? last.offset + last.source.length : offset;
  if (reqSpace && next && next.type !== "space" && next.type !== "newline" && next.type !== "comma" && (next.type !== "scalar" || next.source !== "")) {
    onError(next.offset, "MISSING_CHAR", "Tags and anchors must be separated from the next token by white space");
  }
  if (tab && (atNewline && tab.indent <= parentIndent || next?.type === "block-map" || next?.type === "block-seq"))
    onError(tab, "TAB_AS_INDENT", "Tabs are not allowed as indentation");
  return {
    comma,
    found,
    spaceBefore,
    comment,
    hasNewline,
    anchor,
    tag,
    newlineAfterProp,
    end,
    start: start ?? end
  };
}

// node_modules/yaml/browser/dist/compose/util-contains-newline.js
function containsNewline(key) {
  if (!key)
    return null;
  switch (key.type) {
    case "alias":
    case "scalar":
    case "double-quoted-scalar":
    case "single-quoted-scalar":
      if (key.source.includes("\n"))
        return true;
      if (key.end) {
        for (const st of key.end)
          if (st.type === "newline")
            return true;
      }
      return false;
    case "flow-collection":
      for (const it of key.items) {
        for (const st of it.start)
          if (st.type === "newline")
            return true;
        if (it.sep) {
          for (const st of it.sep)
            if (st.type === "newline")
              return true;
        }
        if (containsNewline(it.key) || containsNewline(it.value))
          return true;
      }
      return false;
    default:
      return true;
  }
}

// node_modules/yaml/browser/dist/compose/util-flow-indent-check.js
function flowIndentCheck(indent, fc, onError) {
  if (fc?.type === "flow-collection") {
    const end = fc.end[0];
    if (end.indent === indent && (end.source === "]" || end.source === "}") && containsNewline(fc)) {
      const msg = "Flow end indicator should be more indented than parent";
      onError(end, "BAD_INDENT", msg, true);
    }
  }
}

// node_modules/yaml/browser/dist/compose/util-map-includes.js
function mapIncludes(ctx, items, search) {
  const { uniqueKeys } = ctx.options;
  if (uniqueKeys === false)
    return false;
  const isEqual = typeof uniqueKeys === "function" ? uniqueKeys : (a, b) => a === b || isScalar(a) && isScalar(b) && a.value === b.value;
  return items.some((pair) => isEqual(pair.key, search));
}

// node_modules/yaml/browser/dist/compose/resolve-block-map.js
var startColMsg = "All mapping items must start at the same column";
function resolveBlockMap({ composeNode: composeNode2, composeEmptyNode: composeEmptyNode2 }, ctx, bm, onError, tag) {
  const NodeClass = tag?.nodeClass ?? YAMLMap;
  const map2 = new NodeClass(ctx.schema);
  if (ctx.atRoot)
    ctx.atRoot = false;
  let offset = bm.offset;
  let commentEnd = null;
  for (const collItem of bm.items) {
    const { start, key, sep, value } = collItem;
    const keyProps = resolveProps(start, {
      indicator: "explicit-key-ind",
      next: key ?? sep?.[0],
      offset,
      onError,
      parentIndent: bm.indent,
      startOnNewline: true
    });
    const implicitKey = !keyProps.found;
    if (implicitKey) {
      if (key) {
        if (key.type === "block-seq")
          onError(offset, "BLOCK_AS_IMPLICIT_KEY", "A block sequence may not be used as an implicit map key");
        else if ("indent" in key && key.indent !== bm.indent)
          onError(offset, "BAD_INDENT", startColMsg);
      }
      if (!keyProps.anchor && !keyProps.tag && !sep) {
        commentEnd = keyProps.end;
        if (keyProps.comment) {
          if (map2.comment)
            map2.comment += "\n" + keyProps.comment;
          else
            map2.comment = keyProps.comment;
        }
        continue;
      }
      if (keyProps.newlineAfterProp || containsNewline(key)) {
        onError(key ?? start[start.length - 1], "MULTILINE_IMPLICIT_KEY", "Implicit keys need to be on a single line");
      }
    } else if (keyProps.found?.indent !== bm.indent) {
      onError(offset, "BAD_INDENT", startColMsg);
    }
    ctx.atKey = true;
    const keyStart = keyProps.end;
    const keyNode = key ? composeNode2(ctx, key, keyProps, onError) : composeEmptyNode2(ctx, keyStart, start, null, keyProps, onError);
    if (ctx.schema.compat)
      flowIndentCheck(bm.indent, key, onError);
    ctx.atKey = false;
    if (mapIncludes(ctx, map2.items, keyNode))
      onError(keyStart, "DUPLICATE_KEY", "Map keys must be unique");
    const valueProps = resolveProps(sep ?? [], {
      indicator: "map-value-ind",
      next: value,
      offset: keyNode.range[2],
      onError,
      parentIndent: bm.indent,
      startOnNewline: !key || key.type === "block-scalar"
    });
    offset = valueProps.end;
    if (valueProps.found) {
      if (implicitKey) {
        if (value?.type === "block-map" && !valueProps.hasNewline)
          onError(offset, "BLOCK_AS_IMPLICIT_KEY", "Nested mappings are not allowed in compact mappings");
        if (ctx.options.strict && keyProps.start < valueProps.found.offset - 1024)
          onError(keyNode.range, "KEY_OVER_1024_CHARS", "The : indicator must be at most 1024 chars after the start of an implicit block mapping key");
      }
      const valueNode = value ? composeNode2(ctx, value, valueProps, onError) : composeEmptyNode2(ctx, offset, sep, null, valueProps, onError);
      if (ctx.schema.compat)
        flowIndentCheck(bm.indent, value, onError);
      offset = valueNode.range[2];
      const pair = new Pair(keyNode, valueNode);
      if (ctx.options.keepSourceTokens)
        pair.srcToken = collItem;
      map2.items.push(pair);
    } else {
      if (implicitKey)
        onError(keyNode.range, "MISSING_CHAR", "Implicit map keys need to be followed by map values");
      if (valueProps.comment) {
        if (keyNode.comment)
          keyNode.comment += "\n" + valueProps.comment;
        else
          keyNode.comment = valueProps.comment;
      }
      const pair = new Pair(keyNode);
      if (ctx.options.keepSourceTokens)
        pair.srcToken = collItem;
      map2.items.push(pair);
    }
  }
  if (commentEnd && commentEnd < offset)
    onError(commentEnd, "IMPOSSIBLE", "Map comment with trailing content");
  map2.range = [bm.offset, offset, commentEnd ?? offset];
  return map2;
}

// node_modules/yaml/browser/dist/compose/resolve-block-seq.js
function resolveBlockSeq({ composeNode: composeNode2, composeEmptyNode: composeEmptyNode2 }, ctx, bs, onError, tag) {
  const NodeClass = tag?.nodeClass ?? YAMLSeq;
  const seq2 = new NodeClass(ctx.schema);
  if (ctx.atRoot)
    ctx.atRoot = false;
  if (ctx.atKey)
    ctx.atKey = false;
  let offset = bs.offset;
  let commentEnd = null;
  for (const { start, value } of bs.items) {
    const props = resolveProps(start, {
      indicator: "seq-item-ind",
      next: value,
      offset,
      onError,
      parentIndent: bs.indent,
      startOnNewline: true
    });
    if (!props.found) {
      if (props.anchor || props.tag || value) {
        if (value?.type === "block-seq")
          onError(props.end, "BAD_INDENT", "All sequence items must start at the same column");
        else
          onError(offset, "MISSING_CHAR", "Sequence item without - indicator");
      } else {
        commentEnd = props.end;
        if (props.comment)
          seq2.comment = props.comment;
        continue;
      }
    }
    const node = value ? composeNode2(ctx, value, props, onError) : composeEmptyNode2(ctx, props.end, start, null, props, onError);
    if (ctx.schema.compat)
      flowIndentCheck(bs.indent, value, onError);
    offset = node.range[2];
    seq2.items.push(node);
  }
  seq2.range = [bs.offset, offset, commentEnd ?? offset];
  return seq2;
}

// node_modules/yaml/browser/dist/compose/resolve-end.js
function resolveEnd(end, offset, reqSpace, onError) {
  let comment = "";
  if (end) {
    let hasSpace = false;
    let sep = "";
    for (const token of end) {
      const { source, type } = token;
      switch (type) {
        case "space":
          hasSpace = true;
          break;
        case "comment": {
          if (reqSpace && !hasSpace)
            onError(token, "MISSING_CHAR", "Comments must be separated from other tokens by white space characters");
          const cb = source.substring(1) || " ";
          if (!comment)
            comment = cb;
          else
            comment += sep + cb;
          sep = "";
          break;
        }
        case "newline":
          if (comment)
            sep += source;
          hasSpace = true;
          break;
        default:
          onError(token, "UNEXPECTED_TOKEN", `Unexpected ${type} at node end`);
      }
      offset += source.length;
    }
  }
  return { comment, offset };
}

// node_modules/yaml/browser/dist/compose/resolve-flow-collection.js
var blockMsg = "Block collections are not allowed within flow collections";
var isBlock = (token) => token && (token.type === "block-map" || token.type === "block-seq");
function resolveFlowCollection({ composeNode: composeNode2, composeEmptyNode: composeEmptyNode2 }, ctx, fc, onError, tag) {
  const isMap2 = fc.start.source === "{";
  const fcName = isMap2 ? "flow map" : "flow sequence";
  const NodeClass = tag?.nodeClass ?? (isMap2 ? YAMLMap : YAMLSeq);
  const coll = new NodeClass(ctx.schema);
  coll.flow = true;
  const atRoot = ctx.atRoot;
  if (atRoot)
    ctx.atRoot = false;
  if (ctx.atKey)
    ctx.atKey = false;
  let offset = fc.offset + fc.start.source.length;
  for (let i = 0; i < fc.items.length; ++i) {
    const collItem = fc.items[i];
    const { start, key, sep, value } = collItem;
    const props = resolveProps(start, {
      flow: fcName,
      indicator: "explicit-key-ind",
      next: key ?? sep?.[0],
      offset,
      onError,
      parentIndent: fc.indent,
      startOnNewline: false
    });
    if (!props.found) {
      if (!props.anchor && !props.tag && !sep && !value) {
        if (i === 0 && props.comma)
          onError(props.comma, "UNEXPECTED_TOKEN", `Unexpected , in ${fcName}`);
        else if (i < fc.items.length - 1)
          onError(props.start, "UNEXPECTED_TOKEN", `Unexpected empty item in ${fcName}`);
        if (props.comment) {
          if (coll.comment)
            coll.comment += "\n" + props.comment;
          else
            coll.comment = props.comment;
        }
        offset = props.end;
        continue;
      }
      if (!isMap2 && ctx.options.strict && containsNewline(key))
        onError(
          key,
          // checked by containsNewline()
          "MULTILINE_IMPLICIT_KEY",
          "Implicit keys of flow sequence pairs need to be on a single line"
        );
    }
    if (i === 0) {
      if (props.comma)
        onError(props.comma, "UNEXPECTED_TOKEN", `Unexpected , in ${fcName}`);
    } else {
      if (!props.comma)
        onError(props.start, "MISSING_CHAR", `Missing , between ${fcName} items`);
      if (props.comment) {
        let prevItemComment = "";
        loop: for (const st of start) {
          switch (st.type) {
            case "comma":
            case "space":
              break;
            case "comment":
              prevItemComment = st.source.substring(1);
              break loop;
            default:
              break loop;
          }
        }
        if (prevItemComment) {
          let prev = coll.items[coll.items.length - 1];
          if (isPair(prev))
            prev = prev.value ?? prev.key;
          if (prev.comment)
            prev.comment += "\n" + prevItemComment;
          else
            prev.comment = prevItemComment;
          props.comment = props.comment.substring(prevItemComment.length + 1);
        }
      }
    }
    if (!isMap2 && !sep && !props.found) {
      const valueNode = value ? composeNode2(ctx, value, props, onError) : composeEmptyNode2(ctx, props.end, sep, null, props, onError);
      coll.items.push(valueNode);
      offset = valueNode.range[2];
      if (isBlock(value))
        onError(valueNode.range, "BLOCK_IN_FLOW", blockMsg);
    } else {
      ctx.atKey = true;
      const keyStart = props.end;
      const keyNode = key ? composeNode2(ctx, key, props, onError) : composeEmptyNode2(ctx, keyStart, start, null, props, onError);
      if (isBlock(key))
        onError(keyNode.range, "BLOCK_IN_FLOW", blockMsg);
      ctx.atKey = false;
      const valueProps = resolveProps(sep ?? [], {
        flow: fcName,
        indicator: "map-value-ind",
        next: value,
        offset: keyNode.range[2],
        onError,
        parentIndent: fc.indent,
        startOnNewline: false
      });
      if (valueProps.found) {
        if (!isMap2 && !props.found && ctx.options.strict) {
          if (sep)
            for (const st of sep) {
              if (st === valueProps.found)
                break;
              if (st.type === "newline") {
                onError(st, "MULTILINE_IMPLICIT_KEY", "Implicit keys of flow sequence pairs need to be on a single line");
                break;
              }
            }
          if (props.start < valueProps.found.offset - 1024)
            onError(valueProps.found, "KEY_OVER_1024_CHARS", "The : indicator must be at most 1024 chars after the start of an implicit flow sequence key");
        }
      } else if (value) {
        if ("source" in value && value.source?.[0] === ":")
          onError(value, "MISSING_CHAR", `Missing space after : in ${fcName}`);
        else
          onError(valueProps.start, "MISSING_CHAR", `Missing , or : between ${fcName} items`);
      }
      const valueNode = value ? composeNode2(ctx, value, valueProps, onError) : valueProps.found ? composeEmptyNode2(ctx, valueProps.end, sep, null, valueProps, onError) : null;
      if (valueNode) {
        if (isBlock(value))
          onError(valueNode.range, "BLOCK_IN_FLOW", blockMsg);
      } else if (valueProps.comment) {
        if (keyNode.comment)
          keyNode.comment += "\n" + valueProps.comment;
        else
          keyNode.comment = valueProps.comment;
      }
      const pair = new Pair(keyNode, valueNode);
      if (ctx.options.keepSourceTokens)
        pair.srcToken = collItem;
      if (isMap2) {
        const map2 = coll;
        if (mapIncludes(ctx, map2.items, keyNode))
          onError(keyStart, "DUPLICATE_KEY", "Map keys must be unique");
        map2.items.push(pair);
      } else {
        const map2 = new YAMLMap(ctx.schema);
        map2.flow = true;
        map2.items.push(pair);
        const endRange = (valueNode ?? keyNode).range;
        map2.range = [keyNode.range[0], endRange[1], endRange[2]];
        coll.items.push(map2);
      }
      offset = valueNode ? valueNode.range[2] : valueProps.end;
    }
  }
  const expectedEnd = isMap2 ? "}" : "]";
  const [ce, ...ee] = fc.end;
  let cePos = offset;
  if (ce?.source === expectedEnd)
    cePos = ce.offset + ce.source.length;
  else {
    const name = fcName[0].toUpperCase() + fcName.substring(1);
    const msg = atRoot ? `${name} must end with a ${expectedEnd}` : `${name} in block collection must be sufficiently indented and end with a ${expectedEnd}`;
    onError(offset, atRoot ? "MISSING_CHAR" : "BAD_INDENT", msg);
    if (ce && ce.source.length !== 1)
      ee.unshift(ce);
  }
  if (ee.length > 0) {
    const end = resolveEnd(ee, cePos, ctx.options.strict, onError);
    if (end.comment) {
      if (coll.comment)
        coll.comment += "\n" + end.comment;
      else
        coll.comment = end.comment;
    }
    coll.range = [fc.offset, cePos, end.offset];
  } else {
    coll.range = [fc.offset, cePos, cePos];
  }
  return coll;
}

// node_modules/yaml/browser/dist/compose/compose-collection.js
function resolveCollection(CN2, ctx, token, onError, tagName, tag) {
  const coll = token.type === "block-map" ? resolveBlockMap(CN2, ctx, token, onError, tag) : token.type === "block-seq" ? resolveBlockSeq(CN2, ctx, token, onError, tag) : resolveFlowCollection(CN2, ctx, token, onError, tag);
  const Coll = coll.constructor;
  if (tagName === "!" || tagName === Coll.tagName) {
    coll.tag = Coll.tagName;
    return coll;
  }
  if (tagName)
    coll.tag = tagName;
  return coll;
}
function composeCollection(CN2, ctx, token, props, onError) {
  const tagToken = props.tag;
  const tagName = !tagToken ? null : ctx.directives.tagName(tagToken.source, (msg) => onError(tagToken, "TAG_RESOLVE_FAILED", msg));
  if (token.type === "block-seq") {
    const { anchor, newlineAfterProp: nl } = props;
    const lastProp = anchor && tagToken ? anchor.offset > tagToken.offset ? anchor : tagToken : anchor ?? tagToken;
    if (lastProp && (!nl || nl.offset < lastProp.offset)) {
      const message = "Missing newline after block sequence props";
      onError(lastProp, "MISSING_CHAR", message);
    }
  }
  const expType = token.type === "block-map" ? "map" : token.type === "block-seq" ? "seq" : token.start.source === "{" ? "map" : "seq";
  if (!tagToken || !tagName || tagName === "!" || tagName === YAMLMap.tagName && expType === "map" || tagName === YAMLSeq.tagName && expType === "seq") {
    return resolveCollection(CN2, ctx, token, onError, tagName);
  }
  let tag = ctx.schema.tags.find((t) => t.tag === tagName && t.collection === expType);
  if (!tag) {
    const kt = ctx.schema.knownTags[tagName];
    if (kt?.collection === expType) {
      ctx.schema.tags.push(Object.assign({}, kt, { default: false }));
      tag = kt;
    } else {
      if (kt) {
        onError(tagToken, "BAD_COLLECTION_TYPE", `${kt.tag} used for ${expType} collection, but expects ${kt.collection ?? "scalar"}`, true);
      } else {
        onError(tagToken, "TAG_RESOLVE_FAILED", `Unresolved tag: ${tagName}`, true);
      }
      return resolveCollection(CN2, ctx, token, onError, tagName);
    }
  }
  const coll = resolveCollection(CN2, ctx, token, onError, tagName, tag);
  const res = tag.resolve?.(coll, (msg) => onError(tagToken, "TAG_RESOLVE_FAILED", msg), ctx.options) ?? coll;
  const node = isNode(res) ? res : new Scalar(res);
  node.range = coll.range;
  node.tag = tagName;
  if (tag?.format)
    node.format = tag.format;
  return node;
}

// node_modules/yaml/browser/dist/compose/resolve-block-scalar.js
function resolveBlockScalar(ctx, scalar, onError) {
  const start = scalar.offset;
  const header = parseBlockScalarHeader(scalar, ctx.options.strict, onError);
  if (!header)
    return { value: "", type: null, comment: "", range: [start, start, start] };
  const type = header.mode === ">" ? Scalar.BLOCK_FOLDED : Scalar.BLOCK_LITERAL;
  const lines = scalar.source ? splitLines(scalar.source) : [];
  let chompStart = lines.length;
  for (let i = lines.length - 1; i >= 0; --i) {
    const content = lines[i][1];
    if (content === "" || content === "\r")
      chompStart = i;
    else
      break;
  }
  if (chompStart === 0) {
    const value2 = header.chomp === "+" && lines.length > 0 ? "\n".repeat(Math.max(1, lines.length - 1)) : "";
    let end2 = start + header.length;
    if (scalar.source)
      end2 += scalar.source.length;
    return { value: value2, type, comment: header.comment, range: [start, end2, end2] };
  }
  let trimIndent = scalar.indent + header.indent;
  let offset = scalar.offset + header.length;
  let contentStart = 0;
  for (let i = 0; i < chompStart; ++i) {
    const [indent, content] = lines[i];
    if (content === "" || content === "\r") {
      if (header.indent === 0 && indent.length > trimIndent)
        trimIndent = indent.length;
    } else {
      if (indent.length < trimIndent) {
        const message = "Block scalars with more-indented leading empty lines must use an explicit indentation indicator";
        onError(offset + indent.length, "MISSING_CHAR", message);
      }
      if (header.indent === 0)
        trimIndent = indent.length;
      contentStart = i;
      if (trimIndent === 0 && !ctx.atRoot) {
        const message = "Block scalar values in collections must be indented";
        onError(offset, "BAD_INDENT", message);
      }
      break;
    }
    offset += indent.length + content.length + 1;
  }
  for (let i = lines.length - 1; i >= chompStart; --i) {
    if (lines[i][0].length > trimIndent)
      chompStart = i + 1;
  }
  let value = "";
  let sep = "";
  let prevMoreIndented = false;
  for (let i = 0; i < contentStart; ++i)
    value += lines[i][0].slice(trimIndent) + "\n";
  for (let i = contentStart; i < chompStart; ++i) {
    let [indent, content] = lines[i];
    offset += indent.length + content.length + 1;
    const crlf = content[content.length - 1] === "\r";
    if (crlf)
      content = content.slice(0, -1);
    if (content && indent.length < trimIndent) {
      const src = header.indent ? "explicit indentation indicator" : "first line";
      const message = `Block scalar lines must not be less indented than their ${src}`;
      onError(offset - content.length - (crlf ? 2 : 1), "BAD_INDENT", message);
      indent = "";
    }
    if (type === Scalar.BLOCK_LITERAL) {
      value += sep + indent.slice(trimIndent) + content;
      sep = "\n";
    } else if (indent.length > trimIndent || content[0] === "	") {
      if (sep === " ")
        sep = "\n";
      else if (!prevMoreIndented && sep === "\n")
        sep = "\n\n";
      value += sep + indent.slice(trimIndent) + content;
      sep = "\n";
      prevMoreIndented = true;
    } else if (content === "") {
      if (sep === "\n")
        value += "\n";
      else
        sep = "\n";
    } else {
      value += sep + content;
      sep = " ";
      prevMoreIndented = false;
    }
  }
  switch (header.chomp) {
    case "-":
      break;
    case "+":
      for (let i = chompStart; i < lines.length; ++i)
        value += "\n" + lines[i][0].slice(trimIndent);
      if (value[value.length - 1] !== "\n")
        value += "\n";
      break;
    default:
      value += "\n";
  }
  const end = start + header.length + scalar.source.length;
  return { value, type, comment: header.comment, range: [start, end, end] };
}
function parseBlockScalarHeader({ offset, props }, strict, onError) {
  if (props[0].type !== "block-scalar-header") {
    onError(props[0], "IMPOSSIBLE", "Block scalar header not found");
    return null;
  }
  const { source } = props[0];
  const mode = source[0];
  let indent = 0;
  let chomp = "";
  let error = -1;
  for (let i = 1; i < source.length; ++i) {
    const ch = source[i];
    if (!chomp && (ch === "-" || ch === "+"))
      chomp = ch;
    else {
      const n = Number(ch);
      if (!indent && n)
        indent = n;
      else if (error === -1)
        error = offset + i;
    }
  }
  if (error !== -1)
    onError(error, "UNEXPECTED_TOKEN", `Block scalar header includes extra characters: ${source}`);
  let hasSpace = false;
  let comment = "";
  let length = source.length;
  for (let i = 1; i < props.length; ++i) {
    const token = props[i];
    switch (token.type) {
      case "space":
        hasSpace = true;
      // fallthrough
      case "newline":
        length += token.source.length;
        break;
      case "comment":
        if (strict && !hasSpace) {
          const message = "Comments must be separated from other tokens by white space characters";
          onError(token, "MISSING_CHAR", message);
        }
        length += token.source.length;
        comment = token.source.substring(1);
        break;
      case "error":
        onError(token, "UNEXPECTED_TOKEN", token.message);
        length += token.source.length;
        break;
      /* istanbul ignore next should not happen */
      default: {
        const message = `Unexpected token in block scalar header: ${token.type}`;
        onError(token, "UNEXPECTED_TOKEN", message);
        const ts = token.source;
        if (ts && typeof ts === "string")
          length += ts.length;
      }
    }
  }
  return { mode, indent, chomp, comment, length };
}
function splitLines(source) {
  const split = source.split(/\n( *)/);
  const first = split[0];
  const m = first.match(/^( *)/);
  const line0 = m?.[1] ? [m[1], first.slice(m[1].length)] : ["", first];
  const lines = [line0];
  for (let i = 1; i < split.length; i += 2)
    lines.push([split[i], split[i + 1]]);
  return lines;
}

// node_modules/yaml/browser/dist/compose/resolve-flow-scalar.js
function resolveFlowScalar(scalar, strict, onError) {
  const { offset, type, source, end } = scalar;
  let _type;
  let value;
  const _onError = (rel, code, msg) => onError(offset + rel, code, msg);
  switch (type) {
    case "scalar":
      _type = Scalar.PLAIN;
      value = plainValue(source, _onError);
      break;
    case "single-quoted-scalar":
      _type = Scalar.QUOTE_SINGLE;
      value = singleQuotedValue(source, _onError);
      break;
    case "double-quoted-scalar":
      _type = Scalar.QUOTE_DOUBLE;
      value = doubleQuotedValue(source, _onError);
      break;
    /* istanbul ignore next should not happen */
    default:
      onError(scalar, "UNEXPECTED_TOKEN", `Expected a flow scalar value, but found: ${type}`);
      return {
        value: "",
        type: null,
        comment: "",
        range: [offset, offset + source.length, offset + source.length]
      };
  }
  const valueEnd = offset + source.length;
  const re = resolveEnd(end, valueEnd, strict, onError);
  return {
    value,
    type: _type,
    comment: re.comment,
    range: [offset, valueEnd, re.offset]
  };
}
function plainValue(source, onError) {
  let badChar = "";
  switch (source[0]) {
    /* istanbul ignore next should not happen */
    case "	":
      badChar = "a tab character";
      break;
    case ",":
      badChar = "flow indicator character ,";
      break;
    case "%":
      badChar = "directive indicator character %";
      break;
    case "|":
    case ">": {
      badChar = `block scalar indicator ${source[0]}`;
      break;
    }
    case "@":
    case "`": {
      badChar = `reserved character ${source[0]}`;
      break;
    }
  }
  if (badChar)
    onError(0, "BAD_SCALAR_START", `Plain value cannot start with ${badChar}`);
  return foldLines(source);
}
function singleQuotedValue(source, onError) {
  if (source[source.length - 1] !== "'" || source.length === 1)
    onError(source.length, "MISSING_CHAR", "Missing closing 'quote");
  return foldLines(source.slice(1, -1)).replace(/''/g, "'");
}
function foldLines(source) {
  let first, line;
  try {
    first = new RegExp("(.*?)(?<![ 	])[ 	]*\r?\n", "sy");
    line = new RegExp("[ 	]*(.*?)(?:(?<![ 	])[ 	]*)?\r?\n", "sy");
  } catch {
    first = /(.*?)[ \t]*\r?\n/sy;
    line = /[ \t]*(.*?)[ \t]*\r?\n/sy;
  }
  let match = first.exec(source);
  if (!match)
    return source;
  let res = match[1];
  let sep = " ";
  let pos = first.lastIndex;
  line.lastIndex = pos;
  while (match = line.exec(source)) {
    if (match[1] === "") {
      if (sep === "\n")
        res += sep;
      else
        sep = "\n";
    } else {
      res += sep + match[1];
      sep = " ";
    }
    pos = line.lastIndex;
  }
  const last = /[ \t]*(.*)/sy;
  last.lastIndex = pos;
  match = last.exec(source);
  return res + sep + (match?.[1] ?? "");
}
function doubleQuotedValue(source, onError) {
  let res = "";
  for (let i = 1; i < source.length - 1; ++i) {
    const ch = source[i];
    if (ch === "\r" && source[i + 1] === "\n")
      continue;
    if (ch === "\n") {
      const { fold, offset } = foldNewline(source, i);
      res += fold;
      i = offset;
    } else if (ch === "\\") {
      let next = source[++i];
      const cc = escapeCodes[next];
      if (cc)
        res += cc;
      else if (next === "\n") {
        next = source[i + 1];
        while (next === " " || next === "	")
          next = source[++i + 1];
      } else if (next === "\r" && source[i + 1] === "\n") {
        next = source[++i + 1];
        while (next === " " || next === "	")
          next = source[++i + 1];
      } else if (next === "x" || next === "u" || next === "U") {
        const length = next === "x" ? 2 : next === "u" ? 4 : 8;
        res += parseCharCode(source, i + 1, length, onError);
        i += length;
      } else {
        const raw = source.substr(i - 1, 2);
        onError(i - 1, "BAD_DQ_ESCAPE", `Invalid escape sequence ${raw}`);
        res += raw;
      }
    } else if (ch === " " || ch === "	") {
      const wsStart = i;
      let next = source[i + 1];
      while (next === " " || next === "	")
        next = source[++i + 1];
      if (next !== "\n" && !(next === "\r" && source[i + 2] === "\n"))
        res += i > wsStart ? source.slice(wsStart, i + 1) : ch;
    } else {
      res += ch;
    }
  }
  if (source[source.length - 1] !== '"' || source.length === 1)
    onError(source.length, "MISSING_CHAR", 'Missing closing "quote');
  return res;
}
function foldNewline(source, offset) {
  let fold = "";
  let ch = source[offset + 1];
  while (ch === " " || ch === "	" || ch === "\n" || ch === "\r") {
    if (ch === "\r" && source[offset + 2] !== "\n")
      break;
    if (ch === "\n")
      fold += "\n";
    offset += 1;
    ch = source[offset + 1];
  }
  if (!fold)
    fold = " ";
  return { fold, offset };
}
var escapeCodes = {
  "0": "\0",
  // null character
  a: "\x07",
  // bell character
  b: "\b",
  // backspace
  e: "\x1B",
  // escape character
  f: "\f",
  // form feed
  n: "\n",
  // line feed
  r: "\r",
  // carriage return
  t: "	",
  // horizontal tab
  v: "\v",
  // vertical tab
  N: "\x85",
  // Unicode next line
  _: "\xA0",
  // Unicode non-breaking space
  L: "\u2028",
  // Unicode line separator
  P: "\u2029",
  // Unicode paragraph separator
  " ": " ",
  '"': '"',
  "/": "/",
  "\\": "\\",
  "	": "	"
};
function parseCharCode(source, offset, length, onError) {
  const cc = source.substr(offset, length);
  const ok = cc.length === length && /^[0-9a-fA-F]+$/.test(cc);
  const code = ok ? parseInt(cc, 16) : NaN;
  try {
    return String.fromCodePoint(code);
  } catch {
    const raw = source.substr(offset - 2, length + 2);
    onError(offset - 2, "BAD_DQ_ESCAPE", `Invalid escape sequence ${raw}`);
    return raw;
  }
}

// node_modules/yaml/browser/dist/compose/compose-scalar.js
function composeScalar(ctx, token, tagToken, onError) {
  const { value, type, comment, range } = token.type === "block-scalar" ? resolveBlockScalar(ctx, token, onError) : resolveFlowScalar(token, ctx.options.strict, onError);
  const tagName = tagToken ? ctx.directives.tagName(tagToken.source, (msg) => onError(tagToken, "TAG_RESOLVE_FAILED", msg)) : null;
  let tag;
  if (ctx.options.stringKeys && ctx.atKey) {
    tag = ctx.schema[SCALAR];
  } else if (tagName)
    tag = findScalarTagByName(ctx.schema, value, tagName, tagToken, onError);
  else if (token.type === "scalar")
    tag = findScalarTagByTest(ctx, value, token, onError);
  else
    tag = ctx.schema[SCALAR];
  let scalar;
  try {
    const res = tag.resolve(value, (msg) => onError(tagToken ?? token, "TAG_RESOLVE_FAILED", msg), ctx.options);
    scalar = isScalar(res) ? res : new Scalar(res);
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    onError(tagToken ?? token, "TAG_RESOLVE_FAILED", msg);
    scalar = new Scalar(value);
  }
  scalar.range = range;
  scalar.source = value;
  if (type)
    scalar.type = type;
  if (tagName)
    scalar.tag = tagName;
  if (tag.format)
    scalar.format = tag.format;
  if (comment)
    scalar.comment = comment;
  return scalar;
}
function findScalarTagByName(schema4, value, tagName, tagToken, onError) {
  if (tagName === "!")
    return schema4[SCALAR];
  const matchWithTest = [];
  for (const tag of schema4.tags) {
    if (!tag.collection && tag.tag === tagName) {
      if (tag.default && tag.test)
        matchWithTest.push(tag);
      else
        return tag;
    }
  }
  for (const tag of matchWithTest)
    if (tag.test?.test(value))
      return tag;
  const kt = schema4.knownTags[tagName];
  if (kt && !kt.collection) {
    schema4.tags.push(Object.assign({}, kt, { default: false, test: void 0 }));
    return kt;
  }
  onError(tagToken, "TAG_RESOLVE_FAILED", `Unresolved tag: ${tagName}`, tagName !== "tag:yaml.org,2002:str");
  return schema4[SCALAR];
}
function findScalarTagByTest({ atKey, directives, schema: schema4 }, value, token, onError) {
  const tag = schema4.tags.find((tag2) => (tag2.default === true || atKey && tag2.default === "key") && tag2.test?.test(value)) || schema4[SCALAR];
  if (schema4.compat) {
    const compat = schema4.compat.find((tag2) => tag2.default && tag2.test?.test(value)) ?? schema4[SCALAR];
    if (tag.tag !== compat.tag) {
      const ts = directives.tagString(tag.tag);
      const cs = directives.tagString(compat.tag);
      const msg = `Value may be parsed as either ${ts} or ${cs}`;
      onError(token, "TAG_RESOLVE_FAILED", msg, true);
    }
  }
  return tag;
}

// node_modules/yaml/browser/dist/compose/util-empty-scalar-position.js
function emptyScalarPosition(offset, before, pos) {
  if (before) {
    pos ?? (pos = before.length);
    for (let i = pos - 1; i >= 0; --i) {
      let st = before[i];
      switch (st.type) {
        case "space":
        case "comment":
        case "newline":
          offset -= st.source.length;
          continue;
      }
      st = before[++i];
      while (st?.type === "space") {
        offset += st.source.length;
        st = before[++i];
      }
      break;
    }
  }
  return offset;
}

// node_modules/yaml/browser/dist/compose/compose-node.js
var CN = { composeNode, composeEmptyNode };
function composeNode(ctx, token, props, onError) {
  const atKey = ctx.atKey;
  const { spaceBefore, comment, anchor, tag } = props;
  let node;
  let isSrcToken = true;
  switch (token.type) {
    case "alias":
      node = composeAlias(ctx, token, onError);
      if (anchor || tag)
        onError(token, "ALIAS_PROPS", "An alias node must not specify any properties");
      break;
    case "scalar":
    case "single-quoted-scalar":
    case "double-quoted-scalar":
    case "block-scalar":
      node = composeScalar(ctx, token, tag, onError);
      if (anchor)
        node.anchor = anchor.source.substring(1);
      break;
    case "block-map":
    case "block-seq":
    case "flow-collection":
      try {
        node = composeCollection(CN, ctx, token, props, onError);
        if (anchor)
          node.anchor = anchor.source.substring(1);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        onError(token, "RESOURCE_EXHAUSTION", message);
      }
      break;
    default: {
      const message = token.type === "error" ? token.message : `Unsupported token (type: ${token.type})`;
      onError(token, "UNEXPECTED_TOKEN", message);
      isSrcToken = false;
    }
  }
  node ?? (node = composeEmptyNode(ctx, token.offset, void 0, null, props, onError));
  if (anchor && node.anchor === "")
    onError(anchor, "BAD_ALIAS", "Anchor cannot be an empty string");
  if (atKey && ctx.options.stringKeys && (!isScalar(node) || typeof node.value !== "string" || node.tag && node.tag !== "tag:yaml.org,2002:str")) {
    const msg = "With stringKeys, all keys must be strings";
    onError(tag ?? token, "NON_STRING_KEY", msg);
  }
  if (spaceBefore)
    node.spaceBefore = true;
  if (comment) {
    if (token.type === "scalar" && token.source === "")
      node.comment = comment;
    else
      node.commentBefore = comment;
  }
  if (ctx.options.keepSourceTokens && isSrcToken)
    node.srcToken = token;
  return node;
}
function composeEmptyNode(ctx, offset, before, pos, { spaceBefore, comment, anchor, tag, end }, onError) {
  const token = {
    type: "scalar",
    offset: emptyScalarPosition(offset, before, pos),
    indent: -1,
    source: ""
  };
  const node = composeScalar(ctx, token, tag, onError);
  if (anchor) {
    node.anchor = anchor.source.substring(1);
    if (node.anchor === "")
      onError(anchor, "BAD_ALIAS", "Anchor cannot be an empty string");
  }
  if (spaceBefore)
    node.spaceBefore = true;
  if (comment) {
    node.comment = comment;
    node.range[2] = end;
  }
  return node;
}
function composeAlias({ options }, { offset, source, end }, onError) {
  const alias = new Alias(source.substring(1));
  if (alias.source === "")
    onError(offset, "BAD_ALIAS", "Alias cannot be an empty string");
  if (alias.source.endsWith(":"))
    onError(offset + source.length - 1, "BAD_ALIAS", "Alias ending in : is ambiguous", true);
  const valueEnd = offset + source.length;
  const re = resolveEnd(end, valueEnd, options.strict, onError);
  alias.range = [offset, valueEnd, re.offset];
  if (re.comment)
    alias.comment = re.comment;
  return alias;
}

// node_modules/yaml/browser/dist/compose/compose-doc.js
function composeDoc(options, directives, { offset, start, value, end }, onError) {
  const opts = Object.assign({ _directives: directives }, options);
  const doc = new Document(void 0, opts);
  const ctx = {
    atKey: false,
    atRoot: true,
    directives: doc.directives,
    options: doc.options,
    schema: doc.schema
  };
  const props = resolveProps(start, {
    indicator: "doc-start",
    next: value ?? end?.[0],
    offset,
    onError,
    parentIndent: 0,
    startOnNewline: true
  });
  if (props.found) {
    doc.directives.docStart = true;
    if (value && (value.type === "block-map" || value.type === "block-seq") && !props.hasNewline)
      onError(props.end, "MISSING_CHAR", "Block collection cannot start on same line with directives-end marker");
  }
  doc.contents = value ? composeNode(ctx, value, props, onError) : composeEmptyNode(ctx, props.end, start, null, props, onError);
  const contentEnd = doc.contents.range[2];
  const re = resolveEnd(end, contentEnd, false, onError);
  if (re.comment)
    doc.comment = re.comment;
  doc.range = [offset, contentEnd, re.offset];
  return doc;
}

// node_modules/yaml/browser/dist/compose/composer.js
function getErrorPos(src) {
  if (typeof src === "number")
    return [src, src + 1];
  if (Array.isArray(src))
    return src.length === 2 ? src : [src[0], src[1]];
  const { offset, source } = src;
  return [offset, offset + (typeof source === "string" ? source.length : 1)];
}
function parsePrelude(prelude) {
  let comment = "";
  let atComment = false;
  let afterEmptyLine = false;
  for (let i = 0; i < prelude.length; ++i) {
    const source = prelude[i];
    switch (source[0]) {
      case "#":
        comment += (comment === "" ? "" : afterEmptyLine ? "\n\n" : "\n") + (source.substring(1) || " ");
        atComment = true;
        afterEmptyLine = false;
        break;
      case "%":
        if (prelude[i + 1]?.[0] !== "#")
          i += 1;
        atComment = false;
        break;
      default:
        if (!atComment)
          afterEmptyLine = true;
        atComment = false;
    }
  }
  return { comment, afterEmptyLine };
}
var Composer = class {
  constructor(options = {}) {
    this.doc = null;
    this.atDirectives = false;
    this.prelude = [];
    this.errors = [];
    this.warnings = [];
    this.onError = (source, code, message, warning) => {
      const pos = getErrorPos(source);
      if (warning)
        this.warnings.push(new YAMLWarning(pos, code, message));
      else
        this.errors.push(new YAMLParseError(pos, code, message));
    };
    this.directives = new Directives({ version: options.version || "1.2" });
    this.options = options;
  }
  decorate(doc, afterDoc) {
    const { comment, afterEmptyLine } = parsePrelude(this.prelude);
    if (comment) {
      const dc = doc.contents;
      if (afterDoc) {
        doc.comment = doc.comment ? `${doc.comment}
${comment}` : comment;
      } else if (afterEmptyLine || doc.directives.docStart || !dc) {
        doc.commentBefore = comment;
      } else if (isCollection(dc) && !dc.flow && dc.items.length > 0) {
        let it = dc.items[0];
        if (isPair(it))
          it = it.key;
        const cb = it.commentBefore;
        it.commentBefore = cb ? `${comment}
${cb}` : comment;
      } else {
        const cb = dc.commentBefore;
        dc.commentBefore = cb ? `${comment}
${cb}` : comment;
      }
    }
    if (afterDoc) {
      for (let i = 0; i < this.errors.length; ++i)
        doc.errors.push(this.errors[i]);
      for (let i = 0; i < this.warnings.length; ++i)
        doc.warnings.push(this.warnings[i]);
    } else {
      doc.errors = this.errors;
      doc.warnings = this.warnings;
    }
    this.prelude = [];
    this.errors = [];
    this.warnings = [];
  }
  /**
   * Current stream status information.
   *
   * Mostly useful at the end of input for an empty stream.
   */
  streamInfo() {
    return {
      comment: parsePrelude(this.prelude).comment,
      directives: this.directives,
      errors: this.errors,
      warnings: this.warnings
    };
  }
  /**
   * Compose tokens into documents.
   *
   * @param forceDoc - If the stream contains no document, still emit a final document including any comments and directives that would be applied to a subsequent document.
   * @param endOffset - Should be set if `forceDoc` is also set, to set the document range end and to indicate errors correctly.
   */
  *compose(tokens, forceDoc = false, endOffset = -1) {
    for (const token of tokens)
      yield* this.next(token);
    yield* this.end(forceDoc, endOffset);
  }
  /** Advance the composer by one CST token. */
  *next(token) {
    switch (token.type) {
      case "directive":
        this.directives.add(token.source, (offset, message, warning) => {
          const pos = getErrorPos(token);
          pos[0] += offset;
          this.onError(pos, "BAD_DIRECTIVE", message, warning);
        });
        this.prelude.push(token.source);
        this.atDirectives = true;
        break;
      case "document": {
        const doc = composeDoc(this.options, this.directives, token, this.onError);
        if (this.atDirectives && !doc.directives.docStart)
          this.onError(token, "MISSING_CHAR", "Missing directives-end/doc-start indicator line");
        this.decorate(doc, false);
        if (this.doc)
          yield this.doc;
        this.doc = doc;
        this.atDirectives = false;
        break;
      }
      case "byte-order-mark":
      case "space":
        break;
      case "comment":
      case "newline":
        this.prelude.push(token.source);
        break;
      case "error": {
        const msg = token.source ? `${token.message}: ${JSON.stringify(token.source)}` : token.message;
        const error = new YAMLParseError(getErrorPos(token), "UNEXPECTED_TOKEN", msg);
        if (this.atDirectives || !this.doc)
          this.errors.push(error);
        else
          this.doc.errors.push(error);
        break;
      }
      case "doc-end": {
        if (!this.doc) {
          const msg = "Unexpected doc-end without preceding document";
          this.errors.push(new YAMLParseError(getErrorPos(token), "UNEXPECTED_TOKEN", msg));
          break;
        }
        this.doc.directives.docEnd = true;
        const end = resolveEnd(token.end, token.offset + token.source.length, this.doc.options.strict, this.onError);
        this.decorate(this.doc, true);
        if (end.comment) {
          const dc = this.doc.comment;
          this.doc.comment = dc ? `${dc}
${end.comment}` : end.comment;
        }
        this.doc.range[2] = end.offset;
        break;
      }
      default:
        this.errors.push(new YAMLParseError(getErrorPos(token), "UNEXPECTED_TOKEN", `Unsupported token ${token.type}`));
    }
  }
  /**
   * Call at end of input to yield any remaining document.
   *
   * @param forceDoc - If the stream contains no document, still emit a final document including any comments and directives that would be applied to a subsequent document.
   * @param endOffset - Should be set if `forceDoc` is also set, to set the document range end and to indicate errors correctly.
   */
  *end(forceDoc = false, endOffset = -1) {
    if (this.doc) {
      this.decorate(this.doc, true);
      yield this.doc;
      this.doc = null;
    } else if (forceDoc) {
      const opts = Object.assign({ _directives: this.directives }, this.options);
      const doc = new Document(void 0, opts);
      if (this.atDirectives)
        this.onError(endOffset, "MISSING_CHAR", "Missing directives-end indicator line");
      doc.range = [0, endOffset, endOffset];
      this.decorate(doc, false);
      yield doc;
    }
  }
};

// node_modules/yaml/browser/dist/parse/cst-visit.js
var BREAK2 = /* @__PURE__ */ Symbol("break visit");
var SKIP2 = /* @__PURE__ */ Symbol("skip children");
var REMOVE2 = /* @__PURE__ */ Symbol("remove item");
function visit2(cst, visitor) {
  if ("type" in cst && cst.type === "document")
    cst = { start: cst.start, value: cst.value };
  _visit(Object.freeze([]), cst, visitor);
}
visit2.BREAK = BREAK2;
visit2.SKIP = SKIP2;
visit2.REMOVE = REMOVE2;
visit2.itemAtPath = (cst, path) => {
  let item = cst;
  for (const [field, index] of path) {
    const tok = item?.[field];
    if (tok && "items" in tok) {
      item = tok.items[index];
    } else
      return void 0;
  }
  return item;
};
visit2.parentCollection = (cst, path) => {
  const parent = visit2.itemAtPath(cst, path.slice(0, -1));
  const field = path[path.length - 1][0];
  const coll = parent?.[field];
  if (coll && "items" in coll)
    return coll;
  throw new Error("Parent collection not found");
};
function _visit(path, item, visitor) {
  let ctrl = visitor(item, path);
  if (typeof ctrl === "symbol")
    return ctrl;
  for (const field of ["key", "value"]) {
    const token = item[field];
    if (token && "items" in token) {
      for (let i = 0; i < token.items.length; ++i) {
        const ci2 = _visit(Object.freeze(path.concat([[field, i]])), token.items[i], visitor);
        if (typeof ci2 === "number")
          i = ci2 - 1;
        else if (ci2 === BREAK2)
          return BREAK2;
        else if (ci2 === REMOVE2) {
          token.items.splice(i, 1);
          i -= 1;
        }
      }
      if (typeof ctrl === "function" && field === "key")
        ctrl = ctrl(item, path);
    }
  }
  return typeof ctrl === "function" ? ctrl(item, path) : ctrl;
}

// node_modules/yaml/browser/dist/parse/cst.js
var BOM = "\uFEFF";
var DOCUMENT = "";
var FLOW_END = "";
var SCALAR2 = "";
function tokenType(source) {
  switch (source) {
    case BOM:
      return "byte-order-mark";
    case DOCUMENT:
      return "doc-mode";
    case FLOW_END:
      return "flow-error-end";
    case SCALAR2:
      return "scalar";
    case "---":
      return "doc-start";
    case "...":
      return "doc-end";
    case "":
    case "\n":
    case "\r\n":
      return "newline";
    case "-":
      return "seq-item-ind";
    case "?":
      return "explicit-key-ind";
    case ":":
      return "map-value-ind";
    case "{":
      return "flow-map-start";
    case "}":
      return "flow-map-end";
    case "[":
      return "flow-seq-start";
    case "]":
      return "flow-seq-end";
    case ",":
      return "comma";
  }
  switch (source[0]) {
    case " ":
    case "	":
      return "space";
    case "#":
      return "comment";
    case "%":
      return "directive-line";
    case "*":
      return "alias";
    case "&":
      return "anchor";
    case "!":
      return "tag";
    case "'":
      return "single-quoted-scalar";
    case '"':
      return "double-quoted-scalar";
    case "|":
    case ">":
      return "block-scalar-header";
  }
  return null;
}

// node_modules/yaml/browser/dist/parse/lexer.js
function isEmpty(ch) {
  switch (ch) {
    case void 0:
    case " ":
    case "\n":
    case "\r":
    case "	":
      return true;
    default:
      return false;
  }
}
var hexDigits = new Set("0123456789ABCDEFabcdef");
var tagChars = new Set("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-#;/?:@&=+$_.!~*'()");
var flowIndicatorChars = new Set(",[]{}");
var invalidAnchorChars = new Set(" ,[]{}\n\r	");
var isNotAnchorChar = (ch) => !ch || invalidAnchorChars.has(ch);
var Lexer2 = class {
  constructor() {
    this.atEnd = false;
    this.blockScalarIndent = -1;
    this.blockScalarKeep = false;
    this.buffer = "";
    this.flowKey = false;
    this.flowLevel = 0;
    this.indentNext = 0;
    this.indentValue = 0;
    this.lineEndPos = null;
    this.next = null;
    this.pos = 0;
  }
  /**
   * Generate YAML tokens from the `source` string. If `incomplete`,
   * a part of the last line may be left as a buffer for the next call.
   *
   * @returns A generator of lexical tokens
   */
  *lex(source, incomplete = false) {
    if (source) {
      if (typeof source !== "string")
        throw TypeError("source is not a string");
      this.buffer = this.buffer ? this.buffer + source : source;
      this.lineEndPos = null;
    }
    this.atEnd = !incomplete;
    let next = this.next ?? "stream";
    while (next && (incomplete || this.hasChars(1)))
      next = yield* this.parseNext(next);
  }
  atLineEnd() {
    let i = this.pos;
    let ch = this.buffer[i];
    while (ch === " " || ch === "	")
      ch = this.buffer[++i];
    if (!ch || ch === "#" || ch === "\n")
      return true;
    if (ch === "\r")
      return this.buffer[i + 1] === "\n";
    return false;
  }
  charAt(n) {
    return this.buffer[this.pos + n];
  }
  continueScalar(offset) {
    let ch = this.buffer[offset];
    if (this.indentNext > 0) {
      let indent = 0;
      while (ch === " ")
        ch = this.buffer[++indent + offset];
      if (ch === "\r") {
        const next = this.buffer[indent + offset + 1];
        if (next === "\n" || !next && !this.atEnd)
          return offset + indent + 1;
      }
      return ch === "\n" || indent >= this.indentNext || !ch && !this.atEnd ? offset + indent : -1;
    }
    if (ch === "-" || ch === ".") {
      const dt = this.buffer.substr(offset, 3);
      if ((dt === "---" || dt === "...") && isEmpty(this.buffer[offset + 3]))
        return -1;
    }
    return offset;
  }
  getLine() {
    let end = this.lineEndPos;
    if (typeof end !== "number" || end !== -1 && end < this.pos) {
      end = this.buffer.indexOf("\n", this.pos);
      this.lineEndPos = end;
    }
    if (end === -1)
      return this.atEnd ? this.buffer.substring(this.pos) : null;
    if (this.buffer[end - 1] === "\r")
      end -= 1;
    return this.buffer.substring(this.pos, end);
  }
  hasChars(n) {
    return this.pos + n <= this.buffer.length;
  }
  setNext(state) {
    this.buffer = this.buffer.substring(this.pos);
    this.pos = 0;
    this.lineEndPos = null;
    this.next = state;
    return null;
  }
  peek(n) {
    return this.buffer.substr(this.pos, n);
  }
  *parseNext(next) {
    switch (next) {
      case "stream":
        return yield* this.parseStream();
      case "line-start":
        return yield* this.parseLineStart();
      case "block-start":
        return yield* this.parseBlockStart();
      case "doc":
        return yield* this.parseDocument();
      case "flow":
        return yield* this.parseFlowCollection();
      case "quoted-scalar":
        return yield* this.parseQuotedScalar();
      case "block-scalar":
        return yield* this.parseBlockScalar();
      case "plain-scalar":
        return yield* this.parsePlainScalar();
    }
  }
  *parseStream() {
    let line = this.getLine();
    if (line === null)
      return this.setNext("stream");
    if (line[0] === BOM) {
      yield* this.pushCount(1);
      line = line.substring(1);
    }
    if (line[0] === "%") {
      let dirEnd = line.length;
      let cs = line.indexOf("#");
      while (cs !== -1) {
        const ch = line[cs - 1];
        if (ch === " " || ch === "	") {
          dirEnd = cs - 1;
          break;
        } else {
          cs = line.indexOf("#", cs + 1);
        }
      }
      while (true) {
        const ch = line[dirEnd - 1];
        if (ch === " " || ch === "	")
          dirEnd -= 1;
        else
          break;
      }
      const n = (yield* this.pushCount(dirEnd)) + (yield* this.pushSpaces(true));
      yield* this.pushCount(line.length - n);
      this.pushNewline();
      return "stream";
    }
    if (this.atLineEnd()) {
      const sp = yield* this.pushSpaces(true);
      yield* this.pushCount(line.length - sp);
      yield* this.pushNewline();
      return "stream";
    }
    yield DOCUMENT;
    return yield* this.parseLineStart();
  }
  *parseLineStart() {
    const ch = this.charAt(0);
    if (!ch && !this.atEnd)
      return this.setNext("line-start");
    if (ch === "-" || ch === ".") {
      if (!this.atEnd && !this.hasChars(4))
        return this.setNext("line-start");
      const s = this.peek(3);
      if ((s === "---" || s === "...") && isEmpty(this.charAt(3))) {
        yield* this.pushCount(3);
        this.indentValue = 0;
        this.indentNext = 0;
        return s === "---" ? "doc" : "stream";
      }
    }
    this.indentValue = yield* this.pushSpaces(false);
    if (this.indentNext > this.indentValue && !isEmpty(this.charAt(1)))
      this.indentNext = this.indentValue;
    return yield* this.parseBlockStart();
  }
  *parseBlockStart() {
    const [ch0, ch1] = this.peek(2);
    if (!ch1 && !this.atEnd)
      return this.setNext("block-start");
    if ((ch0 === "-" || ch0 === "?" || ch0 === ":") && isEmpty(ch1)) {
      const n = (yield* this.pushCount(1)) + (yield* this.pushSpaces(true));
      this.indentNext = this.indentValue + 1;
      this.indentValue += n;
      return "block-start";
    }
    return "doc";
  }
  *parseDocument() {
    yield* this.pushSpaces(true);
    const line = this.getLine();
    if (line === null)
      return this.setNext("doc");
    let n = yield* this.pushIndicators();
    switch (line[n]) {
      case "#":
        yield* this.pushCount(line.length - n);
      // fallthrough
      case void 0:
        yield* this.pushNewline();
        return yield* this.parseLineStart();
      case "{":
      case "[":
        yield* this.pushCount(1);
        this.flowKey = false;
        this.flowLevel = 1;
        return "flow";
      case "}":
      case "]":
        yield* this.pushCount(1);
        return "doc";
      case "*":
        yield* this.pushUntil(isNotAnchorChar);
        return "doc";
      case '"':
      case "'":
        return yield* this.parseQuotedScalar();
      case "|":
      case ">":
        n += yield* this.parseBlockScalarHeader();
        n += yield* this.pushSpaces(true);
        yield* this.pushCount(line.length - n);
        yield* this.pushNewline();
        return yield* this.parseBlockScalar();
      default:
        return yield* this.parsePlainScalar();
    }
  }
  *parseFlowCollection() {
    let nl, sp;
    let indent = -1;
    do {
      nl = yield* this.pushNewline();
      if (nl > 0) {
        sp = yield* this.pushSpaces(false);
        this.indentValue = indent = sp;
      } else {
        sp = 0;
      }
      sp += yield* this.pushSpaces(true);
    } while (nl + sp > 0);
    const line = this.getLine();
    if (line === null)
      return this.setNext("flow");
    if (indent !== -1 && indent < this.indentNext && line[0] !== "#" || indent === 0 && (line.startsWith("---") || line.startsWith("...")) && isEmpty(line[3])) {
      const atFlowEndMarker = indent === this.indentNext - 1 && this.flowLevel === 1 && (line[0] === "]" || line[0] === "}");
      if (!atFlowEndMarker) {
        this.flowLevel = 0;
        yield FLOW_END;
        return yield* this.parseLineStart();
      }
    }
    let n = 0;
    while (line[n] === ",") {
      n += yield* this.pushCount(1);
      n += yield* this.pushSpaces(true);
      this.flowKey = false;
    }
    n += yield* this.pushIndicators();
    switch (line[n]) {
      case void 0:
        return "flow";
      case "#":
        yield* this.pushCount(line.length - n);
        return "flow";
      case "{":
      case "[":
        yield* this.pushCount(1);
        this.flowKey = false;
        this.flowLevel += 1;
        return "flow";
      case "}":
      case "]":
        yield* this.pushCount(1);
        this.flowKey = true;
        this.flowLevel -= 1;
        return this.flowLevel ? "flow" : "doc";
      case "*":
        yield* this.pushUntil(isNotAnchorChar);
        return "flow";
      case '"':
      case "'":
        this.flowKey = true;
        return yield* this.parseQuotedScalar();
      case ":": {
        const next = this.charAt(1);
        if (this.flowKey || isEmpty(next) || next === ",") {
          this.flowKey = false;
          yield* this.pushCount(1);
          yield* this.pushSpaces(true);
          return "flow";
        }
      }
      // fallthrough
      default:
        this.flowKey = false;
        return yield* this.parsePlainScalar();
    }
  }
  *parseQuotedScalar() {
    const quote = this.charAt(0);
    let end = this.buffer.indexOf(quote, this.pos + 1);
    if (quote === "'") {
      while (end !== -1 && this.buffer[end + 1] === "'")
        end = this.buffer.indexOf("'", end + 2);
    } else {
      while (end !== -1) {
        let n = 0;
        while (this.buffer[end - 1 - n] === "\\")
          n += 1;
        if (n % 2 === 0)
          break;
        end = this.buffer.indexOf('"', end + 1);
      }
    }
    const qb = this.buffer.substring(0, end);
    let nl = qb.indexOf("\n", this.pos);
    if (nl !== -1) {
      while (nl !== -1) {
        const cs = this.continueScalar(nl + 1);
        if (cs === -1)
          break;
        nl = qb.indexOf("\n", cs);
      }
      if (nl !== -1) {
        end = nl - (qb[nl - 1] === "\r" ? 2 : 1);
      }
    }
    if (end === -1) {
      if (!this.atEnd)
        return this.setNext("quoted-scalar");
      end = this.buffer.length;
    }
    yield* this.pushToIndex(end + 1, false);
    return this.flowLevel ? "flow" : "doc";
  }
  *parseBlockScalarHeader() {
    this.blockScalarIndent = -1;
    this.blockScalarKeep = false;
    let i = this.pos;
    while (true) {
      const ch = this.buffer[++i];
      if (ch === "+")
        this.blockScalarKeep = true;
      else if (ch > "0" && ch <= "9")
        this.blockScalarIndent = Number(ch) - 1;
      else if (ch !== "-")
        break;
    }
    return yield* this.pushUntil((ch) => isEmpty(ch) || ch === "#");
  }
  *parseBlockScalar() {
    let nl = this.pos - 1;
    let indent = 0;
    let ch;
    loop: for (let i2 = this.pos; ch = this.buffer[i2]; ++i2) {
      switch (ch) {
        case " ":
          indent += 1;
          break;
        case "\n":
          nl = i2;
          indent = 0;
          break;
        case "\r": {
          const next = this.buffer[i2 + 1];
          if (!next && !this.atEnd)
            return this.setNext("block-scalar");
          if (next === "\n")
            break;
        }
        // fallthrough
        default:
          break loop;
      }
    }
    if (!ch && !this.atEnd)
      return this.setNext("block-scalar");
    if (indent >= this.indentNext) {
      if (this.blockScalarIndent === -1)
        this.indentNext = indent;
      else {
        this.indentNext = this.blockScalarIndent + (this.indentNext === 0 ? 1 : this.indentNext);
      }
      do {
        const cs = this.continueScalar(nl + 1);
        if (cs === -1)
          break;
        nl = this.buffer.indexOf("\n", cs);
      } while (nl !== -1);
      if (nl === -1) {
        if (!this.atEnd)
          return this.setNext("block-scalar");
        nl = this.buffer.length;
      }
    }
    let i = nl + 1;
    ch = this.buffer[i];
    while (ch === " ")
      ch = this.buffer[++i];
    if (ch === "	") {
      while (ch === "	" || ch === " " || ch === "\r" || ch === "\n")
        ch = this.buffer[++i];
      nl = i - 1;
    } else if (!this.blockScalarKeep) {
      do {
        let i2 = nl - 1;
        let ch2 = this.buffer[i2];
        if (ch2 === "\r")
          ch2 = this.buffer[--i2];
        const lastChar = i2;
        while (ch2 === " ")
          ch2 = this.buffer[--i2];
        if (ch2 === "\n" && i2 >= this.pos && i2 + 1 + indent > lastChar)
          nl = i2;
        else
          break;
      } while (true);
    }
    yield SCALAR2;
    yield* this.pushToIndex(nl + 1, true);
    return yield* this.parseLineStart();
  }
  *parsePlainScalar() {
    const inFlow = this.flowLevel > 0;
    let end = this.pos - 1;
    let i = this.pos - 1;
    let ch;
    while (ch = this.buffer[++i]) {
      if (ch === ":") {
        const next = this.buffer[i + 1];
        if (isEmpty(next) || inFlow && flowIndicatorChars.has(next))
          break;
        end = i;
      } else if (isEmpty(ch)) {
        let next = this.buffer[i + 1];
        if (ch === "\r") {
          if (next === "\n") {
            i += 1;
            ch = "\n";
            next = this.buffer[i + 1];
          } else
            end = i;
        }
        if (next === "#" || inFlow && flowIndicatorChars.has(next))
          break;
        if (ch === "\n") {
          const cs = this.continueScalar(i + 1);
          if (cs === -1)
            break;
          i = Math.max(i, cs - 2);
        }
      } else {
        if (inFlow && flowIndicatorChars.has(ch))
          break;
        end = i;
      }
    }
    if (!ch && !this.atEnd)
      return this.setNext("plain-scalar");
    yield SCALAR2;
    yield* this.pushToIndex(end + 1, true);
    return inFlow ? "flow" : "doc";
  }
  *pushCount(n) {
    if (n > 0) {
      yield this.buffer.substr(this.pos, n);
      this.pos += n;
      return n;
    }
    return 0;
  }
  *pushToIndex(i, allowEmpty) {
    const s = this.buffer.slice(this.pos, i);
    if (s) {
      yield s;
      this.pos += s.length;
      return s.length;
    } else if (allowEmpty)
      yield "";
    return 0;
  }
  *pushIndicators() {
    let n = 0;
    loop: while (true) {
      switch (this.charAt(0)) {
        case "!":
          n += yield* this.pushTag();
          n += yield* this.pushSpaces(true);
          continue loop;
        case "&":
          n += yield* this.pushUntil(isNotAnchorChar);
          n += yield* this.pushSpaces(true);
          continue loop;
        case "-":
        // this is an error
        case "?":
        // this is an error outside flow collections
        case ":": {
          const inFlow = this.flowLevel > 0;
          const ch1 = this.charAt(1);
          if (isEmpty(ch1) || inFlow && flowIndicatorChars.has(ch1)) {
            if (!inFlow)
              this.indentNext = this.indentValue + 1;
            else if (this.flowKey)
              this.flowKey = false;
            n += yield* this.pushCount(1);
            n += yield* this.pushSpaces(true);
            continue loop;
          }
        }
      }
      break loop;
    }
    return n;
  }
  *pushTag() {
    if (this.charAt(1) === "<") {
      let i = this.pos + 2;
      let ch = this.buffer[i];
      while (!isEmpty(ch) && ch !== ">")
        ch = this.buffer[++i];
      return yield* this.pushToIndex(ch === ">" ? i + 1 : i, false);
    } else {
      let i = this.pos + 1;
      let ch = this.buffer[i];
      while (ch) {
        if (tagChars.has(ch))
          ch = this.buffer[++i];
        else if (ch === "%" && hexDigits.has(this.buffer[i + 1]) && hexDigits.has(this.buffer[i + 2])) {
          ch = this.buffer[i += 3];
        } else
          break;
      }
      return yield* this.pushToIndex(i, false);
    }
  }
  *pushNewline() {
    const ch = this.buffer[this.pos];
    if (ch === "\n")
      return yield* this.pushCount(1);
    else if (ch === "\r" && this.charAt(1) === "\n")
      return yield* this.pushCount(2);
    else
      return 0;
  }
  *pushSpaces(allowTabs) {
    let i = this.pos - 1;
    let ch;
    do {
      ch = this.buffer[++i];
    } while (ch === " " || allowTabs && ch === "	");
    const n = i - this.pos;
    if (n > 0) {
      yield this.buffer.substr(this.pos, n);
      this.pos = i;
    }
    return n;
  }
  *pushUntil(test) {
    let i = this.pos;
    let ch = this.buffer[i];
    while (!test(ch))
      ch = this.buffer[++i];
    return yield* this.pushToIndex(i, false);
  }
};

// node_modules/yaml/browser/dist/parse/line-counter.js
var LineCounter = class {
  constructor() {
    this.lineStarts = [];
    this.addNewLine = (offset) => this.lineStarts.push(offset);
    this.linePos = (offset) => {
      let low = 0;
      let high = this.lineStarts.length;
      while (low < high) {
        const mid = low + high >> 1;
        if (this.lineStarts[mid] < offset)
          low = mid + 1;
        else
          high = mid;
      }
      if (this.lineStarts[low] === offset)
        return { line: low + 1, col: 1 };
      if (low === 0)
        return { line: 0, col: offset };
      const start = this.lineStarts[low - 1];
      return { line: low, col: offset - start + 1 };
    };
  }
};

// node_modules/yaml/browser/dist/parse/parser.js
function includesToken(list2, type) {
  for (let i = 0; i < list2.length; ++i)
    if (list2[i].type === type)
      return true;
  return false;
}
function findNonEmptyIndex(list2) {
  for (let i = 0; i < list2.length; ++i) {
    switch (list2[i].type) {
      case "space":
      case "comment":
      case "newline":
        break;
      default:
        return i;
    }
  }
  return -1;
}
function isFlowToken(token) {
  switch (token?.type) {
    case "alias":
    case "scalar":
    case "single-quoted-scalar":
    case "double-quoted-scalar":
    case "flow-collection":
      return true;
    default:
      return false;
  }
}
function getPrevProps(parent) {
  switch (parent.type) {
    case "document":
      return parent.start;
    case "block-map": {
      const it = parent.items[parent.items.length - 1];
      return it.sep ?? it.start;
    }
    case "block-seq":
      return parent.items[parent.items.length - 1].start;
    /* istanbul ignore next should not happen */
    default:
      return [];
  }
}
function getFirstKeyStartProps(prev) {
  if (prev.length === 0)
    return [];
  let i = prev.length;
  loop: while (--i >= 0) {
    switch (prev[i].type) {
      case "doc-start":
      case "explicit-key-ind":
      case "map-value-ind":
      case "seq-item-ind":
      case "newline":
        break loop;
    }
  }
  while (prev[++i]?.type === "space") {
  }
  return prev.splice(i, prev.length);
}
function arrayPushArray(target, source) {
  if (source.length < 1e5)
    Array.prototype.push.apply(target, source);
  else
    for (let i = 0; i < source.length; ++i)
      target.push(source[i]);
}
function fixFlowSeqItems(fc) {
  if (fc.start.type === "flow-seq-start") {
    for (const it of fc.items) {
      if (it.sep && !it.value && !includesToken(it.start, "explicit-key-ind") && !includesToken(it.sep, "map-value-ind")) {
        if (it.key)
          it.value = it.key;
        delete it.key;
        if (isFlowToken(it.value)) {
          if (it.value.end)
            arrayPushArray(it.value.end, it.sep);
          else
            it.value.end = it.sep;
        } else
          arrayPushArray(it.start, it.sep);
        delete it.sep;
      }
    }
  }
}
var Parser2 = class {
  /**
   * @param onNewLine - If defined, called separately with the start position of
   *   each new line (in `parse()`, including the start of input).
   */
  constructor(onNewLine) {
    this.atNewLine = true;
    this.atScalar = false;
    this.indent = 0;
    this.offset = 0;
    this.onKeyLine = false;
    this.stack = [];
    this.source = "";
    this.type = "";
    this.lexer = new Lexer2();
    this.onNewLine = onNewLine;
  }
  /**
   * Parse `source` as a YAML stream.
   * If `incomplete`, a part of the last line may be left as a buffer for the next call.
   *
   * Errors are not thrown, but yielded as `{ type: 'error', message }` tokens.
   *
   * @returns A generator of tokens representing each directive, document, and other structure.
   */
  *parse(source, incomplete = false) {
    if (this.onNewLine && this.offset === 0)
      this.onNewLine(0);
    for (const lexeme of this.lexer.lex(source, incomplete))
      yield* this.next(lexeme);
    if (!incomplete)
      yield* this.end();
  }
  /**
   * Advance the parser by the `source` of one lexical token.
   */
  *next(source) {
    this.source = source;
    if (this.atScalar) {
      this.atScalar = false;
      yield* this.step();
      this.offset += source.length;
      return;
    }
    const type = tokenType(source);
    if (!type) {
      const message = `Not a YAML token: ${source}`;
      yield* this.pop({ type: "error", offset: this.offset, message, source });
      this.offset += source.length;
    } else if (type === "scalar") {
      this.atNewLine = false;
      this.atScalar = true;
      this.type = "scalar";
    } else {
      this.type = type;
      yield* this.step();
      switch (type) {
        case "newline":
          this.atNewLine = true;
          this.indent = 0;
          if (this.onNewLine)
            this.onNewLine(this.offset + source.length);
          break;
        case "space":
          if (this.atNewLine && source[0] === " ")
            this.indent += source.length;
          break;
        case "explicit-key-ind":
        case "map-value-ind":
        case "seq-item-ind":
          if (this.atNewLine)
            this.indent += source.length;
          break;
        case "doc-mode":
        case "flow-error-end":
          return;
        default:
          this.atNewLine = false;
      }
      this.offset += source.length;
    }
  }
  /** Call at end of input to push out any remaining constructions */
  *end() {
    while (this.stack.length > 0)
      yield* this.pop();
  }
  get sourceToken() {
    const st = {
      type: this.type,
      offset: this.offset,
      indent: this.indent,
      source: this.source
    };
    return st;
  }
  *step() {
    const top = this.peek(1);
    if (this.type === "doc-end" && top?.type !== "doc-end") {
      while (this.stack.length > 0)
        yield* this.pop();
      this.stack.push({
        type: "doc-end",
        offset: this.offset,
        source: this.source
      });
      return;
    }
    if (!top)
      return yield* this.stream();
    switch (top.type) {
      case "document":
        return yield* this.document(top);
      case "alias":
      case "scalar":
      case "single-quoted-scalar":
      case "double-quoted-scalar":
        return yield* this.scalar(top);
      case "block-scalar":
        return yield* this.blockScalar(top);
      case "block-map":
        return yield* this.blockMap(top);
      case "block-seq":
        return yield* this.blockSequence(top);
      case "flow-collection":
        return yield* this.flowCollection(top);
      case "doc-end":
        return yield* this.documentEnd(top);
    }
    yield* this.pop();
  }
  peek(n) {
    return this.stack[this.stack.length - n];
  }
  *pop(error) {
    const token = error ?? this.stack.pop();
    if (!token) {
      const message = "Tried to pop an empty stack";
      yield { type: "error", offset: this.offset, source: "", message };
    } else if (this.stack.length === 0) {
      yield token;
    } else {
      const top = this.peek(1);
      if (token.type === "block-scalar") {
        token.indent = "indent" in top ? top.indent : 0;
      } else if (token.type === "flow-collection" && top.type === "document") {
        token.indent = 0;
      }
      if (token.type === "flow-collection")
        fixFlowSeqItems(token);
      switch (top.type) {
        case "document":
          top.value = token;
          break;
        case "block-scalar":
          top.props.push(token);
          break;
        case "block-map": {
          const it = top.items[top.items.length - 1];
          if (it.value) {
            top.items.push({ start: [], key: token, sep: [] });
            this.onKeyLine = true;
            return;
          } else if (it.sep) {
            it.value = token;
          } else {
            Object.assign(it, { key: token, sep: [] });
            this.onKeyLine = !it.explicitKey;
            return;
          }
          break;
        }
        case "block-seq": {
          const it = top.items[top.items.length - 1];
          if (it.value)
            top.items.push({ start: [], value: token });
          else
            it.value = token;
          break;
        }
        case "flow-collection": {
          const it = top.items[top.items.length - 1];
          if (!it || it.value)
            top.items.push({ start: [], key: token, sep: [] });
          else if (it.sep)
            it.value = token;
          else
            Object.assign(it, { key: token, sep: [] });
          return;
        }
        /* istanbul ignore next should not happen */
        default:
          yield* this.pop();
          yield* this.pop(token);
      }
      if ((top.type === "document" || top.type === "block-map" || top.type === "block-seq") && (token.type === "block-map" || token.type === "block-seq")) {
        const last = token.items[token.items.length - 1];
        if (last && !last.sep && !last.value && last.start.length > 0 && findNonEmptyIndex(last.start) === -1 && (token.indent === 0 || last.start.every((st) => st.type !== "comment" || st.indent < token.indent))) {
          if (top.type === "document")
            top.end = last.start;
          else
            top.items.push({ start: last.start });
          token.items.splice(-1, 1);
        }
      }
    }
  }
  *stream() {
    switch (this.type) {
      case "directive-line":
        yield { type: "directive", offset: this.offset, source: this.source };
        return;
      case "byte-order-mark":
      case "space":
      case "comment":
      case "newline":
        yield this.sourceToken;
        return;
      case "doc-mode":
      case "doc-start": {
        const doc = {
          type: "document",
          offset: this.offset,
          start: []
        };
        if (this.type === "doc-start")
          doc.start.push(this.sourceToken);
        this.stack.push(doc);
        return;
      }
    }
    yield {
      type: "error",
      offset: this.offset,
      message: `Unexpected ${this.type} token in YAML stream`,
      source: this.source
    };
  }
  *document(doc) {
    if (doc.value)
      return yield* this.lineEnd(doc);
    switch (this.type) {
      case "doc-start": {
        if (findNonEmptyIndex(doc.start) !== -1) {
          yield* this.pop();
          yield* this.step();
        } else
          doc.start.push(this.sourceToken);
        return;
      }
      case "anchor":
      case "tag":
      case "space":
      case "comment":
      case "newline":
        doc.start.push(this.sourceToken);
        return;
    }
    const bv = this.startBlockValue(doc);
    if (bv)
      this.stack.push(bv);
    else {
      yield {
        type: "error",
        offset: this.offset,
        message: `Unexpected ${this.type} token in YAML document`,
        source: this.source
      };
    }
  }
  *scalar(scalar) {
    if (this.type === "map-value-ind") {
      const prev = getPrevProps(this.peek(2));
      const start = getFirstKeyStartProps(prev);
      let sep;
      if (scalar.end) {
        sep = scalar.end;
        sep.push(this.sourceToken);
        delete scalar.end;
      } else
        sep = [this.sourceToken];
      const map2 = {
        type: "block-map",
        offset: scalar.offset,
        indent: scalar.indent,
        items: [{ start, key: scalar, sep }]
      };
      this.onKeyLine = true;
      this.stack[this.stack.length - 1] = map2;
    } else
      yield* this.lineEnd(scalar);
  }
  *blockScalar(scalar) {
    switch (this.type) {
      case "space":
      case "comment":
      case "newline":
        scalar.props.push(this.sourceToken);
        return;
      case "scalar":
        scalar.source = this.source;
        this.atNewLine = true;
        this.indent = 0;
        if (this.onNewLine) {
          let nl = this.source.indexOf("\n") + 1;
          while (nl !== 0) {
            this.onNewLine(this.offset + nl);
            nl = this.source.indexOf("\n", nl) + 1;
          }
        }
        yield* this.pop();
        break;
      /* istanbul ignore next should not happen */
      default:
        yield* this.pop();
        yield* this.step();
    }
  }
  *blockMap(map2) {
    const it = map2.items[map2.items.length - 1];
    switch (this.type) {
      case "newline":
        this.onKeyLine = false;
        if (it.value) {
          const end = "end" in it.value ? it.value.end : void 0;
          const last = Array.isArray(end) ? end[end.length - 1] : void 0;
          if (last?.type === "comment")
            end?.push(this.sourceToken);
          else
            map2.items.push({ start: [this.sourceToken] });
        } else if (it.sep) {
          it.sep.push(this.sourceToken);
        } else {
          it.start.push(this.sourceToken);
        }
        return;
      case "space":
      case "comment":
        if (it.value) {
          map2.items.push({ start: [this.sourceToken] });
        } else if (it.sep) {
          it.sep.push(this.sourceToken);
        } else {
          if (this.atIndentedComment(it.start, map2.indent)) {
            const prev = map2.items[map2.items.length - 2];
            const end = prev?.value?.end;
            if (Array.isArray(end)) {
              arrayPushArray(end, it.start);
              end.push(this.sourceToken);
              map2.items.pop();
              return;
            }
          }
          it.start.push(this.sourceToken);
        }
        return;
    }
    if (this.indent >= map2.indent) {
      const atMapIndent = !this.onKeyLine && this.indent === map2.indent;
      const atNextItem = atMapIndent && (it.sep || it.explicitKey) && this.type !== "seq-item-ind";
      let start = [];
      if (atNextItem && it.sep && !it.value) {
        const nl = [];
        for (let i = 0; i < it.sep.length; ++i) {
          const st = it.sep[i];
          switch (st.type) {
            case "newline":
              nl.push(i);
              break;
            case "space":
              break;
            case "comment":
              if (st.indent > map2.indent)
                nl.length = 0;
              break;
            default:
              nl.length = 0;
          }
        }
        if (nl.length >= 2)
          start = it.sep.splice(nl[1]);
      }
      switch (this.type) {
        case "anchor":
        case "tag":
          if (atNextItem || it.value) {
            start.push(this.sourceToken);
            map2.items.push({ start });
            this.onKeyLine = true;
          } else if (it.sep) {
            it.sep.push(this.sourceToken);
          } else {
            it.start.push(this.sourceToken);
          }
          return;
        case "explicit-key-ind":
          if (!it.sep && !it.explicitKey) {
            it.start.push(this.sourceToken);
            it.explicitKey = true;
          } else if (atNextItem || it.value) {
            start.push(this.sourceToken);
            map2.items.push({ start, explicitKey: true });
          } else {
            this.stack.push({
              type: "block-map",
              offset: this.offset,
              indent: this.indent,
              items: [{ start: [this.sourceToken], explicitKey: true }]
            });
          }
          this.onKeyLine = true;
          return;
        case "map-value-ind":
          if (it.explicitKey) {
            if (!it.sep) {
              if (includesToken(it.start, "newline")) {
                Object.assign(it, { key: null, sep: [this.sourceToken] });
              } else {
                const start2 = getFirstKeyStartProps(it.start);
                this.stack.push({
                  type: "block-map",
                  offset: this.offset,
                  indent: this.indent,
                  items: [{ start: start2, key: null, sep: [this.sourceToken] }]
                });
              }
            } else if (it.value) {
              map2.items.push({ start: [], key: null, sep: [this.sourceToken] });
            } else if (includesToken(it.sep, "map-value-ind")) {
              this.stack.push({
                type: "block-map",
                offset: this.offset,
                indent: this.indent,
                items: [{ start, key: null, sep: [this.sourceToken] }]
              });
            } else if (isFlowToken(it.key) && !includesToken(it.sep, "newline")) {
              const start2 = getFirstKeyStartProps(it.start);
              const key = it.key;
              const sep = it.sep;
              sep.push(this.sourceToken);
              delete it.key;
              delete it.sep;
              this.stack.push({
                type: "block-map",
                offset: this.offset,
                indent: this.indent,
                items: [{ start: start2, key, sep }]
              });
            } else if (start.length > 0) {
              it.sep = it.sep.concat(start, this.sourceToken);
            } else {
              it.sep.push(this.sourceToken);
            }
          } else {
            if (!it.sep) {
              Object.assign(it, { key: null, sep: [this.sourceToken] });
            } else if (it.value || atNextItem) {
              map2.items.push({ start, key: null, sep: [this.sourceToken] });
            } else if (includesToken(it.sep, "map-value-ind")) {
              this.stack.push({
                type: "block-map",
                offset: this.offset,
                indent: this.indent,
                items: [{ start: [], key: null, sep: [this.sourceToken] }]
              });
            } else {
              it.sep.push(this.sourceToken);
            }
          }
          this.onKeyLine = true;
          return;
        case "alias":
        case "scalar":
        case "single-quoted-scalar":
        case "double-quoted-scalar": {
          const fs = this.flowScalar(this.type);
          if (atNextItem || it.value) {
            map2.items.push({ start, key: fs, sep: [] });
            this.onKeyLine = true;
          } else if (it.sep) {
            this.stack.push(fs);
          } else {
            Object.assign(it, { key: fs, sep: [] });
            this.onKeyLine = true;
          }
          return;
        }
        default: {
          const bv = this.startBlockValue(map2);
          if (bv) {
            if (bv.type === "block-seq") {
              if (!it.explicitKey && it.sep && !includesToken(it.sep, "newline")) {
                yield* this.pop({
                  type: "error",
                  offset: this.offset,
                  message: "Unexpected block-seq-ind on same line with key",
                  source: this.source
                });
                return;
              }
            } else if (atMapIndent) {
              map2.items.push({ start });
            }
            this.stack.push(bv);
            return;
          }
        }
      }
    }
    yield* this.pop();
    yield* this.step();
  }
  *blockSequence(seq2) {
    const it = seq2.items[seq2.items.length - 1];
    switch (this.type) {
      case "newline":
        if (it.value) {
          const end = "end" in it.value ? it.value.end : void 0;
          const last = Array.isArray(end) ? end[end.length - 1] : void 0;
          if (last?.type === "comment")
            end?.push(this.sourceToken);
          else
            seq2.items.push({ start: [this.sourceToken] });
        } else
          it.start.push(this.sourceToken);
        return;
      case "space":
      case "comment":
        if (it.value)
          seq2.items.push({ start: [this.sourceToken] });
        else {
          if (this.atIndentedComment(it.start, seq2.indent)) {
            const prev = seq2.items[seq2.items.length - 2];
            const end = prev?.value?.end;
            if (Array.isArray(end)) {
              arrayPushArray(end, it.start);
              end.push(this.sourceToken);
              seq2.items.pop();
              return;
            }
          }
          it.start.push(this.sourceToken);
        }
        return;
      case "anchor":
      case "tag":
        if (it.value || this.indent <= seq2.indent)
          break;
        it.start.push(this.sourceToken);
        return;
      case "seq-item-ind":
        if (this.indent !== seq2.indent)
          break;
        if (it.value || includesToken(it.start, "seq-item-ind"))
          seq2.items.push({ start: [this.sourceToken] });
        else
          it.start.push(this.sourceToken);
        return;
    }
    if (this.indent > seq2.indent) {
      const bv = this.startBlockValue(seq2);
      if (bv) {
        this.stack.push(bv);
        return;
      }
    }
    yield* this.pop();
    yield* this.step();
  }
  *flowCollection(fc) {
    const it = fc.items[fc.items.length - 1];
    if (this.type === "flow-error-end") {
      let top;
      do {
        yield* this.pop();
        top = this.peek(1);
      } while (top?.type === "flow-collection");
    } else if (fc.end.length === 0) {
      switch (this.type) {
        case "comma":
        case "explicit-key-ind":
          if (!it || it.sep)
            fc.items.push({ start: [this.sourceToken] });
          else
            it.start.push(this.sourceToken);
          return;
        case "map-value-ind":
          if (!it || it.value)
            fc.items.push({ start: [], key: null, sep: [this.sourceToken] });
          else if (it.sep)
            it.sep.push(this.sourceToken);
          else
            Object.assign(it, { key: null, sep: [this.sourceToken] });
          return;
        case "space":
        case "comment":
        case "newline":
        case "anchor":
        case "tag":
          if (!it || it.value)
            fc.items.push({ start: [this.sourceToken] });
          else if (it.sep)
            it.sep.push(this.sourceToken);
          else
            it.start.push(this.sourceToken);
          return;
        case "alias":
        case "scalar":
        case "single-quoted-scalar":
        case "double-quoted-scalar": {
          const fs = this.flowScalar(this.type);
          if (!it || it.value)
            fc.items.push({ start: [], key: fs, sep: [] });
          else if (it.sep)
            this.stack.push(fs);
          else
            Object.assign(it, { key: fs, sep: [] });
          return;
        }
        case "flow-map-end":
        case "flow-seq-end":
          fc.end.push(this.sourceToken);
          return;
      }
      const bv = this.startBlockValue(fc);
      if (bv)
        this.stack.push(bv);
      else {
        yield* this.pop();
        yield* this.step();
      }
    } else {
      const parent = this.peek(2);
      if (parent.type === "block-map" && (this.type === "map-value-ind" && parent.indent === fc.indent || this.type === "newline" && !parent.items[parent.items.length - 1].sep)) {
        yield* this.pop();
        yield* this.step();
      } else if (this.type === "map-value-ind" && parent.type !== "flow-collection") {
        const prev = getPrevProps(parent);
        const start = getFirstKeyStartProps(prev);
        fixFlowSeqItems(fc);
        const sep = fc.end.splice(1, fc.end.length);
        sep.push(this.sourceToken);
        const map2 = {
          type: "block-map",
          offset: fc.offset,
          indent: fc.indent,
          items: [{ start, key: fc, sep }]
        };
        this.onKeyLine = true;
        this.stack[this.stack.length - 1] = map2;
      } else {
        yield* this.lineEnd(fc);
      }
    }
  }
  flowScalar(type) {
    if (this.onNewLine) {
      let nl = this.source.indexOf("\n") + 1;
      while (nl !== 0) {
        this.onNewLine(this.offset + nl);
        nl = this.source.indexOf("\n", nl) + 1;
      }
    }
    return {
      type,
      offset: this.offset,
      indent: this.indent,
      source: this.source
    };
  }
  startBlockValue(parent) {
    switch (this.type) {
      case "alias":
      case "scalar":
      case "single-quoted-scalar":
      case "double-quoted-scalar":
        return this.flowScalar(this.type);
      case "block-scalar-header":
        return {
          type: "block-scalar",
          offset: this.offset,
          indent: this.indent,
          props: [this.sourceToken],
          source: ""
        };
      case "flow-map-start":
      case "flow-seq-start":
        return {
          type: "flow-collection",
          offset: this.offset,
          indent: this.indent,
          start: this.sourceToken,
          items: [],
          end: []
        };
      case "seq-item-ind":
        return {
          type: "block-seq",
          offset: this.offset,
          indent: this.indent,
          items: [{ start: [this.sourceToken] }]
        };
      case "explicit-key-ind": {
        this.onKeyLine = true;
        const prev = getPrevProps(parent);
        const start = getFirstKeyStartProps(prev);
        start.push(this.sourceToken);
        return {
          type: "block-map",
          offset: this.offset,
          indent: this.indent,
          items: [{ start, explicitKey: true }]
        };
      }
      case "map-value-ind": {
        this.onKeyLine = true;
        const prev = getPrevProps(parent);
        const start = getFirstKeyStartProps(prev);
        return {
          type: "block-map",
          offset: this.offset,
          indent: this.indent,
          items: [{ start, key: null, sep: [this.sourceToken] }]
        };
      }
    }
    return null;
  }
  atIndentedComment(start, indent) {
    if (this.type !== "comment")
      return false;
    if (this.indent <= indent)
      return false;
    return start.every((st) => st.type === "newline" || st.type === "space");
  }
  *documentEnd(docEnd) {
    if (this.type !== "doc-mode") {
      if (docEnd.end)
        docEnd.end.push(this.sourceToken);
      else
        docEnd.end = [this.sourceToken];
      if (this.type === "newline")
        yield* this.pop();
    }
  }
  *lineEnd(token) {
    switch (this.type) {
      case "comma":
      case "doc-start":
      case "doc-end":
      case "flow-seq-end":
      case "flow-map-end":
      case "map-value-ind":
        yield* this.pop();
        yield* this.step();
        break;
      case "newline":
        this.onKeyLine = false;
      // fallthrough
      case "space":
      case "comment":
      default:
        if (token.end)
          token.end.push(this.sourceToken);
        else
          token.end = [this.sourceToken];
        if (this.type === "newline")
          yield* this.pop();
    }
  }
};

// node_modules/yaml/browser/dist/public-api.js
function parseOptions(options) {
  const prettyErrors = options.prettyErrors !== false;
  const lineCounter = options.lineCounter || prettyErrors && new LineCounter() || null;
  return { lineCounter, prettyErrors };
}
function parseDocument(source, options = {}) {
  const { lineCounter, prettyErrors } = parseOptions(options);
  const parser = new Parser2(lineCounter?.addNewLine);
  const composer = new Composer(options);
  let doc = null;
  for (const _doc of composer.compose(parser.parse(source), true, source.length)) {
    if (!doc)
      doc = _doc;
    else if (doc.options.logLevel !== "silent") {
      doc.errors.push(new YAMLParseError(_doc.range.slice(0, 2), "MULTIPLE_DOCS", "Source contains multiple documents; please use YAML.parseAllDocuments()"));
      break;
    }
  }
  if (prettyErrors && lineCounter) {
    doc.errors.forEach(prettifyError(source, lineCounter));
    doc.warnings.forEach(prettifyError(source, lineCounter));
  }
  return doc;
}
function parse(src, reviver, options) {
  let _reviver = void 0;
  if (typeof reviver === "function") {
    _reviver = reviver;
  } else if (options === void 0 && reviver && typeof reviver === "object") {
    options = reviver;
  }
  const doc = parseDocument(src, options);
  if (!doc)
    return null;
  doc.warnings.forEach((warning) => warn(doc.options.logLevel, warning));
  if (doc.errors.length > 0) {
    if (doc.options.logLevel !== "silent")
      throw doc.errors[0];
    else
      doc.errors = [];
  }
  return doc.toJS(Object.assign({ reviver: _reviver }, options));
}

// .cache/semantic-tools/packages/snowflake-semantic-view/src/readYaml.ts
var ZERO_SPAN = { start: 0, end: 0, line: 1 };
var MappingError = class extends Error {
};
function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function asArray(value) {
  return Array.isArray(value) ? value : [];
}
function asRecordArray(value) {
  return asArray(value).filter(isRecord);
}
function asString(value) {
  return typeof value === "string" ? value : typeof value === "number" ? String(value) : void 0;
}
function asStringArray(value) {
  return asArray(value).map(asString).filter((item) => item !== void 0);
}
function mkIdentifier(name) {
  return { name, raw: name };
}
function requireString(value, field) {
  const text = asString(value);
  if (text === void 0) {
    throw new MappingError(`expected a string for "${field}"`);
  }
  return text;
}
function requireIdentifier(value, field) {
  return mkIdentifier(requireString(value, field));
}
function readVisibility(value) {
  return asString(value) === "private_access" ? "private" : "public";
}
function readFilterLabel(value) {
  return asStringArray(value).includes("filter");
}
function readIdentifierList(value) {
  const items = asStringArray(value);
  return items.length === 0 ? null : items.map(mkIdentifier);
}
function readNonAdditiveBy(value) {
  const entries = asRecordArray(value);
  if (entries.length === 0) return null;
  const names = entries.map((entry) => asString(entry.dimension)).filter((name) => name !== void 0);
  return names.length === 0 ? null : names.map(mkIdentifier);
}
function readExpressionObject(object, kind, owner) {
  const name = requireIdentifier(object.name, `${kind}.name`);
  const expression = requireString(object.expr, `${kind}.expr`);
  return {
    kind,
    visibility: readVisibility(object.access_modifier),
    owner,
    name,
    filterLabel: readFilterLabel(object.labels),
    using: readIdentifierList(object.using_relationships),
    nonAdditiveBy: readNonAdditiveBy(object.non_additive_dimensions),
    expression,
    expressionSpan: ZERO_SPAN,
    synonymCount: asStringArray(object.synonyms).length,
    comment: asString(object.description) ?? null,
    tagCount: asArray(object.tags).length,
    cortexSearchCount: object.cortex_search_service !== void 0 ? 1 : 0,
    sampleValueCount: asArray(object.sample_values).length,
    isEnum: object.is_enum === true,
    span: ZERO_SPAN
  };
}
function readTable(table) {
  const alias = requireIdentifier(table.name, "tables[].name");
  const baseTable = table.base_table;
  if (!isRecord(baseTable) || asString(baseTable.table) === void 0) {
    throw new MappingError(
      `table "${alias.name}" has no base_table.database/schema/table (a base_table.definition query source is not supported)`
    );
  }
  const parts = [baseTable.database, baseTable.schema, baseTable.table].map(asString).filter((part) => part !== void 0);
  const table_ = {
    parts: parts.map(mkIdentifier),
    raw: parts.join(".")
  };
  const primaryKeyColumns = isRecord(table.primary_key) ? asStringArray(table.primary_key.columns) : [];
  const uniqueKeys = asRecordArray(table.unique_keys).map((key) => asStringArray(key.columns)).filter((columns) => columns.length > 0);
  return {
    alias,
    table: table_,
    primaryKey: primaryKeyColumns.length === 0 ? null : primaryKeyColumns.map(mkIdentifier),
    uniqueKeys: uniqueKeys.map((columns) => columns.map(mkIdentifier)),
    rangeConstraint: null,
    synonymCount: asStringArray(table.synonyms).length,
    comment: asString(table.description) ?? null,
    tagCount: asArray(table.tags).length,
    span: ZERO_SPAN
  };
}
function readRelationshipTarget(columns) {
  const rangeEntry = columns.find((column) => asString(column.type) === "range");
  if (rangeEntry !== void 0) {
    const rightRange = rangeEntry.right_range;
    if (!isRecord(rightRange)) {
      throw new MappingError('a relationship_columns entry has type "range" but no right_range');
    }
    return {
      target: {
        kind: "range",
        startColumn: requireIdentifier(rightRange.start_column, "right_range.start_column"),
        endColumn: requireIdentifier(rightRange.end_column, "right_range.end_column")
      },
      sourceColumns: columns.map((column) => asString(column.left_column)).filter((name) => name !== void 0).map(mkIdentifier)
    };
  }
  const asofColumns = columns.filter((column) => asString(column.type) === "asof");
  if (asofColumns.length > 0) {
    return {
      target: {
        kind: "asof",
        columns: asofColumns.map((column) => asString(column.right_column)).filter((name) => name !== void 0).map(mkIdentifier)
      },
      sourceColumns: columns.map((column) => asString(column.left_column)).filter((name) => name !== void 0).map(mkIdentifier)
    };
  }
  return {
    target: {
      kind: "columns",
      columns: columns.map((column) => asString(column.right_column)).filter((name) => name !== void 0).map(mkIdentifier)
    },
    sourceColumns: columns.map((column) => asString(column.left_column)).filter((name) => name !== void 0).map(mkIdentifier)
  };
}
function readRelationship(relationship) {
  const sourceAlias = requireIdentifier(relationship.left_table, "relationships[].left_table");
  const targetAlias = requireIdentifier(relationship.right_table, "relationships[].right_table");
  const columns = asRecordArray(relationship.relationship_columns);
  if (columns.length === 0) {
    throw new MappingError(
      `relationship ${sourceAlias.name}_TO_${targetAlias.name} has no relationship_columns`
    );
  }
  const { target, sourceColumns } = readRelationshipTarget(columns);
  const name = asString(relationship.name);
  return {
    name: name === void 0 ? null : mkIdentifier(name),
    sourceAlias,
    sourceColumns,
    targetAlias,
    target,
    span: ZERO_SPAN
  };
}
function readTableDimensions(table, owner) {
  return [...asRecordArray(table.dimensions), ...asRecordArray(table.time_dimensions)].map(
    (object) => readExpressionObject(object, "dimension", owner)
  );
}
function mapToSemanticView(document) {
  if (!isRecord(document)) {
    throw new MappingError("the document is not a YAML mapping");
  }
  const name = requireString(document.name, "name");
  const tables = asRecordArray(document.tables);
  if (tables.length === 0) {
    throw new MappingError("no tables declared");
  }
  if (asArray(document.variables).length > 0) {
    throw new MappingError("declares a variables section, which this converter does not support");
  }
  const logicalTables = tables.map(readTable);
  const facts = tables.flatMap(
    (table) => asRecordArray(table.facts).map(
      (object) => readExpressionObject(object, "fact", requireIdentifier(table.name, "tables[].name"))
    )
  );
  const dimensions = tables.flatMap(
    (table) => readTableDimensions(table, requireIdentifier(table.name, "tables[].name"))
  );
  const tableMetrics = tables.flatMap(
    (table) => asRecordArray(table.metrics).map(
      (object) => readExpressionObject(object, "metric", requireIdentifier(table.name, "tables[].name"))
    )
  );
  const derivedMetrics = asRecordArray(document.metrics).map(
    (object) => readExpressionObject(object, "metric", null)
  );
  const relationships = asRecordArray(document.relationships).map(readRelationship);
  return {
    name: { parts: [mkIdentifier(name)], raw: name },
    tables: logicalTables,
    relationships,
    facts,
    dimensions,
    metrics: [...tableMetrics, ...derivedMetrics],
    comment: asString(document.description) ?? null,
    tagCount: asArray(document.tags).length,
    hasMaxStaleness: document.max_staleness !== void 0,
    hasAiMetadata: document.custom_instructions !== void 0 || document.module_custom_instructions !== void 0,
    verifiedQueryCount: asArray(document.verified_queries).length,
    // COPY GRANTS is a DDL ownership/permissions clause with nothing to
    // export into the semantic model itself — never present in this format.
    hasCopyGrants: false,
    span: ZERO_SPAN
  };
}
function yamlErrorLine(error) {
  return error.linePos?.[0]?.line ?? 1;
}
function readYaml(source) {
  let document;
  try {
    document = parse(source);
  } catch (error) {
    const diagnostic = error instanceof Error ? error.message : String(error);
    const line = error instanceof YAMLParseError ? yamlErrorLine(error) : 1;
    return {
      ok: false,
      errors: [mkFinding("YAML_PARSE_FAILURE", { kind: "view", name: "" }, line, { diagnostic })]
    };
  }
  let view;
  try {
    view = mapToSemanticView(document);
  } catch (error) {
    if (!(error instanceof MappingError)) throw error;
    return {
      ok: false,
      errors: [mkFinding("MALFORMED_DDL", { kind: "view", name: "" }, 1, { diagnostic: error.message })]
    };
  }
  return fromSemanticView(view);
}
function splitYamlStatements(source) {
  return source.trim() === "" ? [] : [source];
}
var SNOWFLAKE_SEMANTIC_VIEW_YAML_READER = {
  dialect: "snowflake",
  read: readYaml
};

// .cache/semantic-tools/packages/snowflake-semantic-view/src/csvQuoteRepair.ts
function repairCsvQuoteDoubling(text) {
  let count = 0;
  const repaired = text.replace(/""(.+?)""/g, (_match, content) => {
    count += 1;
    return `"${content}"`;
  });
  return { text: repaired, count };
}

// .cache/semantic-tools/packages/snowflake-sigma/src/snowflakeFunctions.ts
var PART_ALIASES = {
  year: ["Y", "YY", "YYY", "YYYY", "YR", "YEARS", "YRS"],
  quarter: ["Q", "QTR", "QTRS", "QUARTERS"],
  month: ["MM", "MON", "MONS", "MONTHS"],
  day: ["D", "DD", "DAYS", "DAYOFMONTH"],
  hour: ["H", "HH", "HR", "HOURS", "HRS"],
  minute: ["M", "MI", "MIN", "MINUTES", "MINS"],
  second: ["S", "SEC", "SECONDS", "SECS"],
  millisecond: ["MS", "MSEC", "MILLISECONDS"],
  day_of_year: ["DOY", "DY", "YEARDAY"]
};
function withAliases(parts) {
  const expanded = { ...parts };
  for (const unit of Object.values(parts)) {
    for (const alias of PART_ALIASES[unit] ?? []) expanded[alias] = unit;
  }
  return expanded;
}
var BASE_DATE_PARTS = {
  YEAR: "year",
  QUARTER: "quarter",
  MONTH: "month",
  DAY: "day",
  HOUR: "hour",
  MINUTE: "minute",
  SECOND: "second"
};
var TRUNC_DATE_PARTS = {
  ...BASE_DATE_PARTS,
  MILLISECOND: "millisecond"
};
var EXTRACT_DATE_PARTS = {
  ...BASE_DATE_PARTS,
  DAYOFYEAR: "day_of_year"
};
var ADD_DATE_PARTS = BASE_DATE_PARTS;
var MAPPINGS = {
  // Aggregates with an exact Sigma equivalent.
  SUM: {
    sigma: "Sum",
    // No `sigmaDistinct` here: unlike `Count`/`CountDistinct`, Sigma has no
    // publicly documented `SumDistinct` — https://help.sigmacomputing.com/docs/sumdistinct
    // 404s, and `Sum`'s own docs describe no distinct mode. The source claimed
    // one; checked against the public function index and no public
    // counterpart was found, so it's dropped rather than shipped unverified.
    // See `PROVENANCE.md`. `SUM(DISTINCT x)` now refuses via the plain
    // `{ kind: 'distinct' }` path, the same as any aggregate with no distinct
    // variant.
    arities: [1],
    isAggregate: true,
    returnType: "number"
  },
  AVG: {
    sigma: "Avg",
    arities: [1],
    isAggregate: true,
    returnType: "number"
  },
  COUNT: {
    sigma: "Count",
    sigmaDistinct: "CountDistinct",
    arities: [0, 1],
    isAggregate: true,
    returnType: "number"
  },
  MIN: { sigma: "Min", arities: [1], isAggregate: true, returnType: null },
  MAX: { sigma: "Max", arities: [1], isAggregate: true, returnType: null },
  /**
   * Sigma's `CountIf` counts rows where every condition holds, so a null
   * predicate is not true and is not counted — which is Snowflake's rule.
   * Pinned to one argument for the same reason as `IFF`: the Sigma parameter
   * is a splat, so a second argument would silently become another condition.
   */
  COUNT_IF: {
    sigma: "CountIf",
    arities: [1],
    isAggregate: true,
    returnType: "number"
  },
  MEDIAN: {
    sigma: "Median",
    arities: [1],
    isAggregate: true,
    returnType: "number"
  },
  STDDEV: {
    sigma: "Stddev",
    arities: [1],
    isAggregate: true,
    returnType: "number"
  },
  STDDEV_SAMP: {
    sigma: "Stddev",
    arities: [1],
    isAggregate: true,
    returnType: "number"
  },
  VARIANCE: {
    sigma: "Variance",
    arities: [1],
    isAggregate: true,
    returnType: "number"
  },
  VAR_SAMP: {
    sigma: "Variance",
    arities: [1],
    isAggregate: true,
    returnType: "number"
  },
  VARIANCE_SAMP: {
    sigma: "Variance",
    arities: [1],
    isAggregate: true,
    returnType: "number"
  },
  VAR_POP: {
    sigma: "VariancePop",
    arities: [1],
    isAggregate: true,
    returnType: "number"
  },
  VARIANCE_POP: {
    sigma: "VariancePop",
    arities: [1],
    isAggregate: true,
    returnType: "number"
  },
  CORR: {
    sigma: "Corr",
    arities: [2],
    isAggregate: true,
    returnType: "number"
  },
  // Aggregates reachable only as a warehouse passthrough.
  APPROX_COUNT_DISTINCT: {
    sigma: null,
    isAggregate: true,
    returnType: "number"
  },
  APPROX_PERCENTILE: { sigma: null, isAggregate: true, returnType: "number" },
  BITAND_AGG: { sigma: null, isAggregate: true, returnType: "number" },
  BITOR_AGG: { sigma: null, isAggregate: true, returnType: "number" },
  BITXOR_AGG: { sigma: null, isAggregate: true, returnType: "number" },
  BOOLAND_AGG: { sigma: null, isAggregate: true, returnType: "logical" },
  BOOLOR_AGG: { sigma: null, isAggregate: true, returnType: "logical" },
  BOOLXOR_AGG: { sigma: null, isAggregate: true, returnType: "logical" },
  COVAR_POP: { sigma: null, isAggregate: true, returnType: "number" },
  COVAR_SAMP: { sigma: null, isAggregate: true, returnType: "number" },
  HLL: { sigma: null, isAggregate: true, returnType: "number" },
  KURTOSIS: { sigma: null, isAggregate: true, returnType: "number" },
  LISTAGG: { sigma: null, isAggregate: true, returnType: "text" },
  // `MODE` is absent rather than mapped: it returns the most frequent value of
  // whatever type it was given — `MODE` over text answers text — and
  // `returnType` selects one passthrough variant, so no single value is right.
  SKEW: { sigma: null, isAggregate: true, returnType: "number" },
  STDDEV_POP: { sigma: null, isAggregate: true, returnType: "number" },
  // Scalars with an exact Sigma equivalent.
  ABS: {
    sigma: "Abs",
    arities: [1],
    isAggregate: false,
    returnType: "number"
  },
  CEIL: {
    sigma: "Ceiling",
    arities: [1],
    isAggregate: false,
    returnType: "number"
  },
  CEILING: {
    sigma: "Ceiling",
    arities: [1],
    isAggregate: false,
    returnType: "number"
  },
  FLOOR: {
    sigma: "Floor",
    arities: [1],
    isAggregate: false,
    returnType: "number"
  },
  EXP: {
    sigma: "Exp",
    arities: [1],
    isAggregate: false,
    returnType: "number"
  },
  LN: { sigma: "Ln", arities: [1], isAggregate: false, returnType: "number" },
  SQRT: {
    sigma: "Sqrt",
    arities: [1],
    isAggregate: false,
    returnType: "number"
  },
  SIGN: {
    sigma: "Sign",
    arities: [1],
    isAggregate: false,
    returnType: "number"
  },
  ROUND: {
    sigma: "Round",
    arities: [1, 2],
    isAggregate: false,
    returnType: "number"
  },
  POWER: {
    sigma: "Power",
    arities: [2],
    isAggregate: false,
    returnType: "number"
  },
  POW: {
    sigma: "Power",
    arities: [2],
    isAggregate: false,
    returnType: "number"
  },
  MOD: {
    sigma: "Mod",
    arities: [2],
    isAggregate: false,
    returnType: "number"
  },
  GREATEST: { sigma: "Greatest", isAggregate: false, returnType: null },
  LEAST: { sigma: "Least", isAggregate: false, returnType: null },
  // Both pairs were run side by side against a null argument: the plain pair
  // returns null in each system and the null-ignoring pair skips it in each, so
  // the correspondence holds in both directions rather than only for the
  // variants whose names promise it.
  GREATEST_IGNORE_NULLS: {
    sigma: "GreatestNonNull",
    isAggregate: false,
    returnType: null
  },
  LEAST_IGNORE_NULLS: {
    sigma: "LeastNonNull",
    isAggregate: false,
    returnType: null
  },
  COALESCE: { sigma: "Coalesce", isAggregate: false, returnType: null },
  IFNULL: {
    sigma: "Coalesce",
    arities: [2],
    isAggregate: false,
    returnType: null
  },
  NVL: {
    sigma: "Coalesce",
    arities: [2],
    isAggregate: false,
    returnType: null
  },
  /**
   * Sigma has no one-argument form, and `Coalesce`/`NullIf` with a zero
   * literal are these operations exactly. Both require a numeric argument —
   * `Coalesce` unifies its arguments to one type, so a text column would emit
   * a formula Sigma rejects, so a caller has to check the argument's type
   * before using this mapping.
   */
  ZEROIFNULL: {
    sigma: "Coalesce",
    appendArgs: [0],
    arities: [1],
    isAggregate: false,
    returnType: "number"
  },
  NULLIFZERO: {
    sigma: "NullIf",
    appendArgs: [0],
    arities: [1],
    isAggregate: false,
    returnType: "number"
  },
  /**
   * `IFF(c, a, b)` is `CASE WHEN c THEN a ELSE b END` by definition, and that
   * is already lowered to Sigma's `If`, so this claims nothing the conditional
   * path does not claim already. The arity is pinned because Sigma's `If` is
   * variadic: a malformed four-argument call would otherwise be reinterpreted
   * as the start of a second branch rather than refused.
   */
  IFF: {
    sigma: "If",
    arities: [3],
    isAggregate: false,
    returnType: null
  },
  NULLIF: {
    sigma: "NullIf",
    arities: [2],
    isAggregate: false,
    returnType: null
  },
  UPPER: {
    sigma: "Upper",
    arities: [1],
    isAggregate: false,
    returnType: "text"
  },
  LOWER: {
    sigma: "Lower",
    arities: [1],
    isAggregate: false,
    returnType: "text"
  },
  INITCAP: {
    sigma: "Proper",
    arities: [1],
    isAggregate: false,
    returnType: "text"
  },
  REVERSE: {
    sigma: "Reverse",
    arities: [1],
    isAggregate: false,
    returnType: "text"
  },
  TRIM: {
    sigma: "Trim",
    arities: [1],
    isAggregate: false,
    returnType: "text"
  },
  LTRIM: {
    sigma: "LTrim",
    arities: [1],
    isAggregate: false,
    returnType: "text"
  },
  RTRIM: {
    sigma: "RTrim",
    arities: [1],
    isAggregate: false,
    returnType: "text"
  },
  LENGTH: {
    sigma: "Len",
    arities: [1],
    isAggregate: false,
    returnType: "number"
  },
  LEN: {
    sigma: "Len",
    arities: [1],
    isAggregate: false,
    returnType: "number"
  },
  LEFT: {
    sigma: "Left",
    arities: [2],
    isAggregate: false,
    returnType: "text"
  },
  RIGHT: {
    sigma: "Right",
    arities: [2],
    isAggregate: false,
    returnType: "text"
  },
  MD5: { sigma: "MD5", arities: [1], isAggregate: false, returnType: "text" },
  YEAR: {
    sigma: "Year",
    arities: [1],
    isAggregate: false,
    returnType: "number",
    zoneSensitive: true
  },
  QUARTER: {
    sigma: "Quarter",
    arities: [1],
    isAggregate: false,
    returnType: "number",
    zoneSensitive: true
  },
  MONTH: {
    sigma: "Month",
    arities: [1],
    isAggregate: false,
    returnType: "number",
    zoneSensitive: true
  },
  DAY: {
    sigma: "Day",
    arities: [1],
    isAggregate: false,
    returnType: "number",
    zoneSensitive: true
  },
  DAYOFYEAR: {
    sigma: "DayOfYear",
    arities: [1],
    isAggregate: false,
    returnType: "number",
    zoneSensitive: true
  },
  HOUR: {
    sigma: "Hour",
    arities: [1],
    isAggregate: false,
    returnType: "number",
    zoneSensitive: true
  },
  MINUTE: {
    sigma: "Minute",
    arities: [1],
    isAggregate: false,
    returnType: "number",
    zoneSensitive: true
  },
  SECOND: {
    sigma: "Second",
    arities: [1],
    isAggregate: false,
    returnType: "number",
    zoneSensitive: true
  },
  DATE_TRUNC: {
    sigma: "DateTrunc",
    arities: [2],
    isAggregate: false,
    returnType: "datetime",
    datePartArg: 0,
    datePartNames: withAliases(TRUNC_DATE_PARTS),
    zoneSensitive: true
  },
  /**
   * Both count the date-part boundaries crossed rather than measuring elapsed
   * time: across `2019-12-31` to `2020-01-01` each answers one year. Sigma's
   * description says the result is "rounded to a whole number", which reads like
   * an elapsed measure and is not one — the two were run side by side.
   */
  DATEDIFF: {
    sigma: "DateDiff",
    arities: [3],
    isAggregate: false,
    returnType: "number",
    datePartArg: 0,
    datePartNames: withAliases(TRUNC_DATE_PARTS),
    zoneSensitive: true
  },
  DATE_PART: {
    sigma: "DatePart",
    arities: [2],
    isAggregate: false,
    returnType: "number",
    datePartArg: 0,
    datePartNames: withAliases(EXTRACT_DATE_PARTS),
    zoneSensitive: true
  },
  // Scalars reachable only as a warehouse passthrough.
  CONCAT: { sigma: null, isAggregate: false, returnType: "text" },
  // Sigma's `Date` converts to a date *and time* rather than truncating to a
  // day, and its `Today` takes no timezone while Snowflake's `CURRENT_DATE`
  // follows the session's. Neither is exact, so both go through the warehouse,
  // which is what makes them equivalent rather than approximate.
  CURRENT_DATE: { sigma: null, isAggregate: false, returnType: "datetime" },
  DATE: { sigma: null, isAggregate: false, returnType: "datetime" },
  /**
   * The conversion family. Every one of these is format- or
   * session-dependent — `TO_NUMBER` defaults to scale 0 and so rounds where
   * Sigma's `Number` does not, and would raise on unparseable text where
   * Sigma returns null; the date renderings follow the session's output
   * format; and a two-argument form carries Snowflake's own format language,
   * which is a sub-language rather than a mapping. Handing the arguments to
   * the warehouse verbatim keeps all of that exactly as written.
   */
  TO_VARCHAR: { sigma: null, isAggregate: false, returnType: "text" },
  TO_CHAR: { sigma: null, isAggregate: false, returnType: "text" },
  TO_NUMBER: { sigma: null, isAggregate: false, returnType: "number" },
  TO_DATE: { sigma: null, isAggregate: false, returnType: "datetime" },
  /**
   * Sigma has a `MonthName`, and its documented example matches Snowflake's
   * three-letter abbreviation — but whether it is produced by the warehouse or
   * localized for the viewer is not answerable from this repo, and a localized
   * label is the right number under the wrong bucket. Passthrough needs no
   * answer.
   */
  MONTHNAME: { sigma: null, isAggregate: false, returnType: "text" },
  DAYNAME: { sigma: null, isAggregate: false, returnType: "text" },
  /**
   * Sigma's `DateAdd` takes the same arguments in the same order, so the
   * objection that excludes `LOG` does not apply — but it documents rounding
   * the amount to the nearest whole number without saying which way, and
   * whether it clamps a month-end the way Snowflake does is unverified.
   * Neither question needs answering while the warehouse evaluates it.
   */
  DATEADD: {
    sigma: null,
    isAggregate: false,
    returnType: "datetime",
    datePartArg: 0,
    datePartNames: ADD_DATE_PARTS
  },
  DIV0: { sigma: null, isAggregate: false, returnType: "number" },
  DIV0NULL: { sigma: null, isAggregate: false, returnType: "number" },
  LOG: { sigma: null, isAggregate: false, returnType: "number" },
  REPLACE: { sigma: null, isAggregate: false, returnType: "text" },
  SPLIT_PART: { sigma: null, isAggregate: false, returnType: "text" },
  SUBSTR: { sigma: null, isAggregate: false, returnType: "text" },
  SUBSTRING: { sigma: null, isAggregate: false, returnType: "text" },
  WIDTH_BUCKET: { sigma: null, isAggregate: false, returnType: "number" }
};
var PASSTHROUGH_SUFFIX = {
  number: "Number",
  text: "Text",
  datetime: "Datetime",
  logical: "Logical"
};
function passthroughFunction(returnType, isAggregate) {
  return `${isAggregate ? "Agg" : "Call"}${PASSTHROUGH_SUFFIX[returnType]}`;
}
var ZONE_SENSITIVE_FUNCTIONS = new Set(
  Object.entries(MAPPINGS).filter(([, mapping]) => mapping.zoneSensitive === true).map(([name]) => name)
);
var SNOWFLAKE_FUNCTIONS = new Map(Object.entries(MAPPINGS));

// .cache/semantic-tools/packages/snowflake-sigma/src/transpile.ts
var ZONED_TIMESTAMP = "TIMESTAMP_TZ";
function findZoneDependentColumn(node, resolveColumnType) {
  if (node.kind === "call" && ZONE_SENSITIVE_FUNCTIONS.has(node.name)) {
    const zoned = findZonedColumn(node, resolveColumnType);
    if (zoned !== null) return zoned;
  }
  const pair = comparedLiteral(node);
  if (pair !== null && resolveColumnType(pair.column.parts)?.toUpperCase() === ZONED_TIMESTAMP) {
    return pair.column.raw;
  }
  const inList = columnInListLiterals(node);
  if (inList !== null && resolveColumnType(inList.column.parts)?.toUpperCase() === ZONED_TIMESTAMP) {
    return inList.column.raw;
  }
  for (const child of subExpressions(node)) {
    const found = findZoneDependentColumn(child, resolveColumnType);
    if (found !== null) return found;
  }
  return null;
}
function findZonedColumn(node, resolveColumnType) {
  if (node.kind === "column" && resolveColumnType(node.parts)?.toUpperCase() === ZONED_TIMESTAMP) {
    return node.raw;
  }
  for (const child of subExpressions(node)) {
    const found = findZonedColumn(child, resolveColumnType);
    if (found !== null) return found;
  }
  return null;
}
var CASTABLE_DATE_TYPES = /* @__PURE__ */ new Set([
  "DATE",
  "TIMESTAMP",
  "TIMESTAMP_NTZ",
  "DATETIME"
]);
var COMPARISONS = /* @__PURE__ */ new Set([
  "=",
  "!=",
  "<",
  "<=",
  ">",
  ">="
]);
var NUMERIC_TYPES = /* @__PURE__ */ new Set([
  "NUMBER",
  "DECIMAL",
  "NUMERIC",
  "INT",
  "INTEGER",
  "BIGINT",
  "SMALLINT",
  "TINYINT",
  "BYTEINT",
  "FLOAT",
  "FLOAT4",
  "FLOAT8",
  "DOUBLE",
  "DOUBLE PRECISION",
  "REAL"
]);
function isNumeric(node, resolveColumnType) {
  switch (node.kind) {
    case "number":
      return true;
    case "column": {
      const type = resolveColumnType(node.parts);
      return type !== null && NUMERIC_TYPES.has(type.toUpperCase());
    }
    case "unary":
      return isNumeric(node.operand, resolveColumnType);
    case "binary":
      return ["+", "-", "*", "/"].includes(node.operator) && isNumeric(node.left, resolveColumnType) && isNumeric(node.right, resolveColumnType);
    case "call": {
      const mapping = SNOWFLAKE_FUNCTIONS.get(node.name);
      if (mapping === void 0) return false;
      if (mapping.returnType === "number") return true;
      const [first] = node.args;
      return mapping.returnType === null && first !== void 0 ? isNumeric(first, resolveColumnType) : false;
    }
    case "case": {
      const results = [
        ...node.branches.map((branch) => branch.then),
        ...node.otherwise === null ? [] : [node.otherwise]
      ];
      return results.length > 0 && results.every((result) => isNumeric(result, resolveColumnType));
    }
    case "cast":
      return node.target !== "boolean";
    // Exhaustive rather than a `default: false` fallthrough: a new
    // SqlExpr_t node kind that should be numeric (or not) is a compile
    // error here, the same guard `translate`'s own switch already has —
    // a silent default would let this drift from `translate` unnoticed.
    case "string":
    case "boolean":
    case "null":
    case "not":
    case "isNull":
    case "in":
    case "like":
    case "variantPath":
      return false;
    default:
      throw new UnreachableCaseError("Unhandled expression node", node);
  }
}
function comparedLiteral(node) {
  if (node.kind !== "binary" || !COMPARISONS.has(node.operator)) return null;
  for (const [left, right] of [
    [node.left, node.right],
    [node.right, node.left]
  ]) {
    if (left.kind === "column" && right.kind === "string") {
      return { column: left, literal: right.value };
    }
  }
  return null;
}
function columnInListLiterals(node) {
  if (node.kind !== "in" || node.value.kind !== "column") return null;
  const literals = node.candidates.filter((candidate) => candidate.kind === "string").map((candidate) => candidate.value);
  return literals.length > 0 ? { column: node.value, literals } : null;
}
function retypeComparisons(node, resolveColumnType) {
  const pair = comparedLiteral(node);
  if (pair !== null && node.kind === "binary") {
    const type = resolveColumnType(pair.column.parts)?.toUpperCase();
    if (type !== void 0 && type !== null && CASTABLE_DATE_TYPES.has(type)) {
      const cast = (side) => side.kind === "string" ? {
        kind: "call",
        name: "TO_DATE",
        distinct: false,
        star: false,
        args: [side]
      } : side;
      return { ...node, left: cast(node.left), right: cast(node.right) };
    }
  }
  const inList = columnInListLiterals(node);
  if (inList !== null && node.kind === "in") {
    const type = resolveColumnType(inList.column.parts)?.toUpperCase();
    if (type !== void 0 && type !== null && CASTABLE_DATE_TYPES.has(type)) {
      const cast = (candidate) => candidate.kind === "string" ? {
        kind: "call",
        name: "TO_DATE",
        distinct: false,
        star: false,
        args: [candidate]
      } : candidate;
      return { ...node, candidates: node.candidates.map(cast) };
    }
  }
  switch (node.kind) {
    case "unary":
    case "not":
      return {
        ...node,
        operand: retypeComparisons(node.operand, resolveColumnType)
      };
    case "isNull":
      return {
        ...node,
        operand: retypeComparisons(node.operand, resolveColumnType)
      };
    case "binary":
      return {
        ...node,
        left: retypeComparisons(node.left, resolveColumnType),
        right: retypeComparisons(node.right, resolveColumnType)
      };
    case "in":
      return {
        ...node,
        value: retypeComparisons(node.value, resolveColumnType),
        candidates: node.candidates.map(
          (candidate) => retypeComparisons(candidate, resolveColumnType)
        )
      };
    case "like":
      return {
        ...node,
        value: retypeComparisons(node.value, resolveColumnType),
        pattern: retypeComparisons(node.pattern, resolveColumnType)
      };
    case "case":
      return {
        ...node,
        branches: node.branches.map((branch) => ({
          when: retypeComparisons(branch.when, resolveColumnType),
          // `then` is `SqlExpr_t`'s own field name (CASE...WHEN...THEN), not
          // an accidental thenable.
          // oxlint-disable-next-line no-thenable
          then: retypeComparisons(branch.then, resolveColumnType)
        })),
        otherwise: node.otherwise === null ? null : retypeComparisons(node.otherwise, resolveColumnType)
      };
    case "call":
      return {
        ...node,
        args: node.args.map((arg) => retypeComparisons(arg, resolveColumnType))
      };
    case "cast":
      return {
        ...node,
        operand: retypeComparisons(node.operand, resolveColumnType)
      };
    case "variantPath":
      return {
        ...node,
        base: retypeComparisons(node.base, resolveColumnType)
      };
    default:
      return node;
  }
}
function findNonNumericAppend(node, resolveColumnType) {
  if (node.kind === "call") {
    const mapping = SNOWFLAKE_FUNCTIONS.get(node.name);
    const [first] = node.args;
    if (mapping?.appendArgs !== void 0 && (first === void 0 || !isNumeric(first, resolveColumnType))) {
      return node.name;
    }
  }
  for (const child of subExpressions(node)) {
    const found = findNonNumericAppend(child, resolveColumnType);
    if (found !== null) return found;
  }
  return null;
}
var PRECEDENCE_OR = 1;
var PRECEDENCE_AND = 2;
var PRECEDENCE_EQUALITY = 3;
var PRECEDENCE_ORDERING = 4;
var PRECEDENCE_ADDITIVE = 5;
var PRECEDENCE_MULTIPLICATIVE = 6;
var PRECEDENCE_UNARY = 7;
var PRECEDENCE_PRIMARY = 8;
var PRECEDENCE_BY_OPERATOR = {
  or: PRECEDENCE_OR,
  and: PRECEDENCE_AND,
  "=": PRECEDENCE_EQUALITY,
  "!=": PRECEDENCE_EQUALITY,
  "<": PRECEDENCE_ORDERING,
  "<=": PRECEDENCE_ORDERING,
  ">": PRECEDENCE_ORDERING,
  ">=": PRECEDENCE_ORDERING,
  "+": PRECEDENCE_ADDITIVE,
  "-": PRECEDENCE_ADDITIVE,
  "*": PRECEDENCE_MULTIPLICATIVE,
  "/": PRECEDENCE_MULTIPLICATIVE
};
function refuse(sink, refusal) {
  if (sink.refusal === null) sink.refusal = refusal;
  return null;
}
var SIGMA_STRING_ESCAPES = {
  "\\": "\\\\",
  '"': '\\"',
  "\n": "\\n",
  "	": "\\t",
  "\r": "\\r",
  "\0": "\\0"
};
function sigmaStringLiteral(value) {
  let out = '"';
  for (const char of value) {
    const escape = SIGMA_STRING_ESCAPES[char];
    if (escape !== void 0) {
      out += escape;
      continue;
    }
    if (char.charCodeAt(0) < 32) return null;
    out += char;
  }
  return `${out}"`;
}
function wrap(name, args) {
  return {
    text: `${name}(${args.map((arg) => arg.text).join(", ")})`,
    precedence: PRECEDENCE_PRIMARY,
    usedPassthrough: args.some((arg) => arg.usedPassthrough),
    hasAggregate: args.some((arg) => arg.hasAggregate)
  };
}
function atom(text) {
  return {
    text,
    precedence: PRECEDENCE_PRIMARY,
    usedPassthrough: false,
    hasAggregate: false
  };
}
function parenthesize(fragment, minimum) {
  return fragment.precedence < minimum ? `(${fragment.text})` : fragment.text;
}
function translateCall(node, resolveColumn, sink) {
  const mapping = SNOWFLAKE_FUNCTIONS.get(node.name);
  if (mapping === void 0) {
    return refuse(sink, { kind: "function", name: node.name });
  }
  const argFragments = [];
  const argTexts = [];
  for (const [index, arg] of node.args.entries()) {
    if (mapping.datePartArg === index) {
      const partText = datePartLiteral(arg, mapping.datePartNames);
      if (partText === null) {
        const spelling = datePartSpelling(arg);
        return refuse(sink, {
          kind: "date-part",
          name: node.name,
          ...spelling !== void 0 && { part: spelling }
        });
      }
      argTexts.push(partText);
      continue;
    }
    const fragment = translate(arg, resolveColumn, sink);
    if (fragment === null) return null;
    argFragments.push(fragment);
    argTexts.push(fragment.text);
  }
  const argsUsePassthrough = argFragments.some(
    (fragment) => fragment.usedPassthrough
  );
  const argsAggregate = argFragments.some((fragment) => fragment.hasAggregate);
  if (mapping.sigma !== null) {
    const sigmaName = node.distinct ? mapping.sigmaDistinct : mapping.sigma;
    if (sigmaName === void 0) {
      return refuse(sink, { kind: "distinct", name: node.name });
    }
    const argumentCount = node.star ? 0 : node.args.length;
    if (mapping.arities !== void 0 && !mapping.arities.includes(argumentCount)) {
      return refuse(sink, {
        kind: "arity",
        name: node.name,
        args: argumentCount
      });
    }
    const appended = (mapping.appendArgs ?? []).map(String);
    return {
      text: `${sigmaName}(${[...argTexts, ...appended].join(", ")})`,
      precedence: PRECEDENCE_PRIMARY,
      usedPassthrough: argsUsePassthrough,
      hasAggregate: mapping.isAggregate || argsAggregate
    };
  }
  if (node.distinct) return refuse(sink, { kind: "distinct", name: node.name });
  if (node.star) return refuse(sink, { kind: "star", name: node.name });
  if (mapping.returnType === null) {
    return refuse(sink, { kind: "function", name: node.name });
  }
  const callee = passthroughFunction(mapping.returnType, mapping.isAggregate);
  const nameLiteral = sigmaStringLiteral(node.name);
  if (nameLiteral === null) return refuse(sink, { kind: "literal" });
  return {
    text: `${callee}(${[nameLiteral, ...argTexts].join(", ")})`,
    precedence: PRECEDENCE_PRIMARY,
    usedPassthrough: true,
    hasAggregate: mapping.isAggregate || argsAggregate
  };
}
function datePartLiteral(arg, names) {
  if (names === void 0) return null;
  const spelling = datePartSpelling(arg);
  if (spelling === void 0) return null;
  const sigmaPart = names[spelling];
  return sigmaPart === void 0 ? null : `"${sigmaPart}"`;
}
function datePartSpelling(arg) {
  if (arg.kind === "column" && arg.parts.length === 1) return arg.parts[0];
  if (arg.kind === "string") return arg.value.toUpperCase();
  return void 0;
}
function concatOperands(node) {
  const operands = [];
  const visit3 = (current) => {
    if (current.kind === "binary" && current.operator === "||") {
      visit3(current.left);
      visit3(current.right);
    } else {
      operands.push(current);
    }
  };
  visit3(node);
  return operands;
}
function translate(node, resolveColumn, sink) {
  switch (node.kind) {
    case "column": {
      const reference = resolveColumn(node.parts);
      if (reference === null) {
        return refuse(sink, { kind: "column", column: node.raw });
      }
      return atom(reference);
    }
    case "number":
      return atom(node.text);
    case "string": {
      const literal = sigmaStringLiteral(node.value);
      return literal === null ? refuse(sink, { kind: "literal" }) : atom(literal);
    }
    case "boolean":
      return atom(node.value ? "true" : "false");
    case "null":
      return atom("null");
    case "unary": {
      const operand = translate(node.operand, resolveColumn, sink);
      if (operand === null) return null;
      if (node.operator === "+") return operand;
      return {
        text: `-${parenthesize(operand, PRECEDENCE_UNARY + 1)}`,
        precedence: PRECEDENCE_UNARY,
        usedPassthrough: operand.usedPassthrough,
        hasAggregate: operand.hasAggregate
      };
    }
    case "binary": {
      if (node.operator === "||") {
        return translateCall(
          {
            kind: "call",
            name: "CONCAT",
            args: concatOperands(node),
            distinct: false,
            star: false
          },
          resolveColumn,
          sink
        );
      }
      const left = translate(node.left, resolveColumn, sink);
      if (left === null) return null;
      const right = translate(node.right, resolveColumn, sink);
      if (right === null) return null;
      const precedence = PRECEDENCE_BY_OPERATOR[node.operator];
      const rightText = parenthesize(right, precedence + 1);
      return {
        text: `${parenthesize(left, precedence)} ${node.operator} ${rightText}`,
        precedence,
        usedPassthrough: left.usedPassthrough || right.usedPassthrough,
        hasAggregate: left.hasAggregate || right.hasAggregate
      };
    }
    case "not": {
      const operand = translate(node.operand, resolveColumn, sink);
      return operand === null ? null : wrap("Not", [operand]);
    }
    case "isNull": {
      const operand = translate(node.operand, resolveColumn, sink);
      if (operand === null) return null;
      return wrap(node.negated ? "IsNotNull" : "IsNull", [operand]);
    }
    case "in": {
      const value = translate(node.value, resolveColumn, sink);
      if (value === null) return null;
      const candidates = [];
      for (const candidate of node.candidates) {
        const translated = translate(candidate, resolveColumn, sink);
        if (translated === null) return null;
        candidates.push(translated);
      }
      const membership = wrap("In", [value, ...candidates]);
      return node.negated ? wrap("Not", [membership]) : membership;
    }
    case "like": {
      const value = translate(node.value, resolveColumn, sink);
      if (value === null) return null;
      const pattern = translate(node.pattern, resolveColumn, sink);
      if (pattern === null) return null;
      const match = wrap(node.caseInsensitive ? "ILike" : "Like", [
        value,
        pattern
      ]);
      return node.negated ? wrap("Not", [match]) : match;
    }
    case "case": {
      const parts = [];
      for (const branch of node.branches) {
        const when = translate(branch.when, resolveColumn, sink);
        if (when === null) return null;
        const then = translate(branch.then, resolveColumn, sink);
        if (then === null) return null;
        parts.push(when, then);
      }
      const otherwise = node.otherwise === null ? atom("null") : translate(node.otherwise, resolveColumn, sink);
      if (otherwise === null) return null;
      return wrap("If", [...parts, otherwise]);
    }
    case "call":
      return translateCall(node, resolveColumn, sink);
    case "cast": {
      const operand = translate(node.operand, resolveColumn, sink);
      if (operand === null) return null;
      if (node.target === "boolean") return wrap("Logical", [operand]);
      const asNumber = wrap("Number", [operand]);
      return node.target === "fixedNumber" ? wrap("Round", [asNumber, atom("0")]) : asNumber;
    }
    case "variantPath": {
      const base = translate(node.base, resolveColumn, sink);
      if (base === null) return null;
      return {
        text: `${base.text}.${node.key}`,
        precedence: PRECEDENCE_PRIMARY,
        usedPassthrough: base.usedPassthrough,
        hasAggregate: base.hasAggregate
      };
    }
    default:
      throw new UnreachableCaseError("Unhandled expression node", node);
  }
}
function transpile(expression, resolveColumn, resolveColumnType) {
  const zoneDependentColumn = findZoneDependentColumn(
    expression,
    resolveColumnType
  );
  if (zoneDependentColumn !== null) return { ok: false, zoneDependentColumn };
  const nonNumeric = findNonNumericAppend(expression, resolveColumnType);
  if (nonNumeric !== null) {
    return { ok: false, refusal: { kind: "argument-type", name: nonNumeric } };
  }
  const sink = { refusal: null };
  const fragment = translate(
    retypeComparisons(expression, resolveColumnType),
    resolveColumn,
    sink
  );
  if (fragment === null) {
    return {
      ok: false,
      ...sink.refusal !== null && { refusal: sink.refusal }
    };
  }
  return {
    ok: true,
    formula: fragment.text,
    usedPassthrough: fragment.usedPassthrough,
    hasAggregate: fragment.hasAggregate
  };
}

// .cache/semantic-tools/packages/snowflake-sigma/src/index.ts
var SNOWFLAKE_TO_SIGMA_TRANSLATOR = {
  from: "snowflake",
  to: "sigma",
  transpile,
  isAggregateFunction: (name) => SNOWFLAKE_FUNCTIONS.get(name)?.isAggregate === true,
  datePartArgIndex: (name) => SNOWFLAKE_FUNCTIONS.get(name)?.datePartArg
};

// src/sigma-ids.ts
var NS_MODULUS = 62 ** 4;
var SIGMA_LOWERCASE_WORDS = /* @__PURE__ */ new Set([
  "a",
  "an",
  "the",
  "and",
  "but",
  "or",
  "for",
  "nor",
  "so",
  "yet",
  "at",
  "by",
  "in",
  "of",
  "on",
  "to",
  "up",
  "as",
  "into",
  "via",
  "per"
]);
function formatFromMask(mask) {
  if (!mask || typeof mask !== "string") return null;
  const s = mask.trim();
  if (!s || /general|date|time|@|yy|dd/i.test(s)) return null;
  const decM = s.match(/\.([0#]+)/);
  const decimals = decM ? decM[1].length : 0;
  const isPercent = /%/.test(s);
  const isCurrency = /[$£€¥]/.test(s);
  if (isPercent) return { kind: "number", formatString: `,.${decimals}%` };
  if (isCurrency) return { kind: "number", formatString: `$,.${decimals}f`, currencySymbol: "$" };
  if (/[0#]/.test(s)) return { kind: "number", formatString: `,.${decimals}f` };
  return null;
}
function inferSigmaFormat(formula, displayName, sourceMask) {
  const fromMask = formatFromMask(sourceMask);
  if (fromMask) return fromMask;
  if (!formula) return null;
  const f = formula.trim();
  const n = (displayName || "").toLowerCase();
  const alreadyPctScale = /\*\s*100\b/.test(f);
  if (alreadyPctScale && /\b(rate|margin|pct|percent|ratio|share|mix)\b|%/.test(n)) {
    return { kind: "number", formatString: ",.2f", suffix: "%" };
  }
  const currencyWord = /\b(revenue|sales|profit|cost|spend|amount|discounts?|price|value|aov|arpu)\b/;
  const ratio = f.match(/^([A-Za-z]+)\s*\(([^)]*)\)\s*\/\s*([A-Za-z]+)\s*\(([^)]*)\)$/);
  if (ratio) {
    const [, numFn, numArg, denFn, denArg] = ratio;
    const isCount = (fn) => /^Count/i.test(fn);
    const numIsCurrency = currencyWord.test(numArg.toLowerCase());
    const nameSaysPct = /\b(rate|margin|pct|percent|ratio|share|mix)\b|%/.test(n);
    if (nameSaysPct || isCount(numFn) && isCount(denFn)) {
      return { kind: "number", formatString: ",.2%" };
    }
    if (numIsCurrency) {
      return { kind: "number", formatString: "$,.2f", currencySymbol: "$" };
    }
    return { kind: "number", formatString: ",.2f" };
  }
  if (/\b(rate|margin|pct|percent|ratio|share|mix)\b|%/.test(n)) {
    return { kind: "number", formatString: ",.2%" };
  }
  if (currencyWord.test(n)) {
    return { kind: "number", formatString: "$,.2f", currencySymbol: "$" };
  }
  if (/^Count(?:Distinct|If|DistinctIf)?\s*\(/.test(f)) {
    return { kind: "number", formatString: ",.0f" };
  }
  return null;
}
var DATA_MODEL_SCHEMA_SUMMARY = `
Sigma Data Model JSON top-level structure:
{
  "name": "Model Name",
  "pages": [{ "id": "pageId", "name": "Page 1", "elements": [...] }]
}

Element types: warehouse-table, custom-sql (kind:"sql"), join, union, control.
Columns: { "id": "inode-xxx/COL", "formula": "[TABLE/Display Name]" }
Calculated columns: { "id": "shortId", "formula": "[Price] - [Cost]", "name": "Profit" }
Metrics: { "id": "shortId", "formula": "Sum([Revenue])", "name": "Total Revenue" }
Relationships: { "id": "shortId", "targetElementId": "...", "keys": [{ "sourceColumnId": "...", "targetColumnId": "..." }] }

Cross-element Reference (accessing related dimension columns via relationships):
  [SOURCE_TABLE/REL_NAME/Column Display Name]
  REL_NAME is the relationship's "name" field (= target table name uppercase by convention).
  Example: DateDiff("day", [ORDER_FACT/PROMO_DIM/Start Date], [ORDER_FACT/PROMO_DIM/End Date])
  \u26A0 The dash-link form [SRC/FK_COL - link/Field] does NOT work via the API \u2014 use REL_NAME.

Conditional Aggregate Syntax:
  CountIf(condition) \u2014 condition only, NO field argument
  SumIf(field, condition) \u2014 FIELD FIRST, condition second
  AvgIf/MaxIf/MinIf/CountDistinctIf \u2014 all FIELD FIRST
  For booleans: always use [Column] = True, never bare [Column]

Groupings (for LOD / different aggregation levels):
  "groupings": [{ "id": "gId", "groupBy": ["colId1"], "calculations": ["calcId1"] }]
  Array order = nesting hierarchy. Use child elements for LOD patterns.
`.trim();

// semantic-tools-embed/src/relDisplay.ts
function relDisplaySegment(display, explicitName) {
  if (explicitName) return display;
  return display.split(" ").map((w, i) => i > 0 && SIGMA_LOWERCASE_WORDS.has(w.toLowerCase()) ? w.toLowerCase() : w).join(" ");
}

// semantic-tools-embed/src/crossTable.ts
var REF3 = /\[([^\[\]/]+)\/([^\[\]/]+)\/([^\[\]/]+)\]/g;
var PASSTHROUGH = /^\[([^\[\]/]+)\/([^\[\]]+)\]$/;
var refsOf = (formula) => typeof formula === "string" ? [...formula.matchAll(REF3)] : [];
function displayOf(col) {
  if (typeof col.name === "string") return col.name;
  const m = typeof col.formula === "string" ? PASSTHROUGH.exec(col.formula) : null;
  return m ? m[2] : void 0;
}
function mentions(formula, tableName, display, id) {
  if (formula.includes(`[${id}]`)) return true;
  if (display === void 0) return false;
  return formula.includes(`[${display}]`) || tableName !== void 0 && formula.includes(`[${tableName}/${display}]`);
}
function applyCrossTableReferences(specText, viewName) {
  const spec = JSON.parse(specText);
  const elements = spec.pages?.[0]?.elements ?? [];
  const byId = new Map(elements.map((e) => [e.id, e]));
  const findings = [];
  const hasCross = (e) => {
    const rels = new Set((e.relationships ?? []).map((r) => r.id));
    return [...e.columns ?? [], ...e.metrics ?? []].some((o) => refsOf(o.formula).some((m) => rels.has(m[2])));
  };
  if (!elements.some(hasCross)) return { spec: specText, findings };
  const blocked = /* @__PURE__ */ new Map();
  let moved = /* @__PURE__ */ new Map();
  const resolveRef = (e, m, targetMoved) => {
    const rel = (e.relationships ?? []).find((r) => r.id === m[2]);
    if (!rel || typeof rel.name !== "string" || rel.name === "") return { why: `relationship \`${m[2]}\` has no name` };
    const target = byId.get(rel.targetElementId);
    const col = target?.columns?.find((c) => c.id === m[3]);
    if (!target || !col) return { why: `target column \`${m[3]}\` not found on element \`${rel.targetElementId}\`` };
    if (refsOf(col.formula).length > 0 || targetMoved.get(target.id)?.has(col)) {
      return { why: `target column \`${m[3]}\` is itself a cross-table column` };
    }
    const display = displayOf(col);
    if (display === void 0) return { why: `target column \`${m[3]}\` has no resolvable display name` };
    return { text: `[${m[1]}/${rel.name}/${relDisplaySegment(display, typeof col.name === "string")}]` };
  };
  for (let pass = 0; pass < 20; pass++) {
    const next = /* @__PURE__ */ new Map();
    const newBlocked = /* @__PURE__ */ new Map();
    for (const e of elements) {
      const rels = new Set((e.relationships ?? []).map((r) => r.id));
      const items = [
        ...(e.columns ?? []).map((obj) => ({ kind: "column", obj })),
        ...(e.metrics ?? []).map((obj) => ({ kind: "metric", obj }))
      ];
      const set2 = /* @__PURE__ */ new Set();
      for (const it of items) {
        const mine = refsOf(it.obj.formula).filter((m) => rels.has(m[2]));
        if (mine.length === 0 || blocked.has(it.obj)) continue;
        let bad2;
        for (const m of mine) {
          const r = resolveRef(e, m, moved);
          if ("why" in r) {
            bad2 = r.why;
            break;
          }
        }
        if (bad2 !== void 0) newBlocked.set(it.obj, bad2);
        else set2.add(it.obj);
      }
      if (set2.size > 0) {
        const tableName = [...set2].map((o) => refsOf(o.formula)[0]?.[1]).find(Boolean);
        let grew = true;
        while (grew) {
          grew = false;
          for (const it of items) {
            if (set2.has(it.obj) || typeof it.obj.formula !== "string" || blocked.has(it.obj)) continue;
            for (const mo of set2) {
              const disp = mo.name ?? displayOf(mo);
              if (mentions(it.obj.formula, tableName, disp, mo.id)) {
                set2.add(it.obj);
                grew = true;
                break;
              }
            }
          }
        }
        next.set(e.id, set2);
      }
    }
    const changed = newBlocked.size > 0 && [...newBlocked.keys()].some((k) => !blocked.has(k));
    for (const [k, v] of newBlocked) blocked.set(k, v);
    moved = next;
    if (!changed) break;
  }
  for (const e of elements) {
    for (const o of [...e.columns ?? [], ...e.metrics ?? []]) {
      const why = blocked.get(o);
      if (why === void 0) continue;
      findings.push({
        code: "CROSS_TABLE_REFERENCE_UNRESOLVED",
        severity: "warning",
        message: `\`${o.name ?? o.id}\` on \`${e.id}\` uses a cross-table reference that could not be rewritten for Sigma (${why}); it was left as the engine emitted it and will not validate in Sigma.`,
        remediation: "Rebuild this column in Sigma on a derived element over the table, using the relationship picker.",
        line: 1,
        object: { kind: (e.metrics ?? []).includes(o) ? "metric" : "column", name: String(o.name ?? o.id), owner: e.id },
        source: "embed"
      });
    }
  }
  const used = new Set(elements.map((e) => e.id));
  const out = [];
  const derivedInfo = [];
  for (const e of elements) {
    const set2 = moved.get(e.id);
    if (!set2 || set2.size === 0) {
      out.push(e);
      continue;
    }
    let id = `${e.id}__joined`;
    for (let n = 2; used.has(id); n++) id = `${e.id}__joined_${n}`;
    used.add(id);
    const anyFormula = [...set2].map((o) => refsOf(o.formula)[0]?.[1]).find(Boolean);
    const tableName = anyFormula ?? (e.columns ?? []).map((c) => PASSTHROUGH.exec(c.formula ?? "")?.[1]).find(Boolean);
    const rewrite = (formula) => formula.replace(REF3, (all, a, b, c) => {
      const m = [all, a, b, c];
      if (!(e.relationships ?? []).some((r2) => r2.id === b)) return all;
      const r = resolveRef(e, m, /* @__PURE__ */ new Map());
      return "text" in r ? r.text : all;
    });
    const dCols = [];
    for (const c of e.columns ?? []) {
      if (set2.has(c)) {
        dCols.push({ ...c, formula: rewrite(c.formula) });
        continue;
      }
      const display = displayOf(c);
      if (display === void 0 || tableName === void 0) continue;
      dCols.push({
        id: c.id,
        formula: `[${tableName}/${display}]`,
        ...typeof c.name === "string" && { name: c.name },
        ...c.description !== void 0 && { description: c.description },
        ...c.format !== void 0 && { format: c.format }
      });
    }
    const slug3 = (x) => x.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "") || "x";
    const helpers = [];
    const nameTaken = (n) => [...dCols, ...helpers].some((c) => (displayOf(c) ?? "").toLowerCase() === n.toLowerCase());
    const refName = (all, m) => {
      const r = resolveRef(e, m, /* @__PURE__ */ new Map());
      if (!("text" in r)) return void 0;
      const hit = [...dCols, ...helpers].find((c) => c.formula === r.text && typeof c.name === "string");
      if (hit) return hit.name;
      const rel = (e.relationships ?? []).find((x) => x.id === m[2]);
      const display = r.text.slice(r.text.lastIndexOf("/") + 1, -1);
      let name = display;
      if (nameTaken(name)) {
        name = `${display} (${rel.name})`;
        for (let n = 2; nameTaken(name); n++) name = `${display} (${rel.name}) ${n}`;
      }
      let hid = `${id}__xref__${slug3(rel.name)}__${slug3(display)}`;
      const ids = new Set([...dCols, ...helpers].map((c) => c.id));
      for (let n = 2; ids.has(hid); n++) hid = `${id}__xref__${slug3(rel.name)}__${slug3(display)}_${n}`;
      helpers.push({ id: hid, formula: r.text, name });
      return name;
    };
    const failedMetrics = /* @__PURE__ */ new Set();
    const dMetrics = [];
    for (const m of (e.metrics ?? []).filter((x) => set2.has(x))) {
      let ok = true;
      const formula = String(m.formula).replace(REF3, (all, a, b, c) => {
        if (!(e.relationships ?? []).some((r) => r.id === b)) {
          ok = false;
          return all;
        }
        const nm = refName(all, [all, a, b, c]);
        if (nm === void 0) {
          ok = false;
          return all;
        }
        return `[${nm}]`;
      });
      if (ok && refsOf(formula).length === 0) dMetrics.push({ ...m, formula });
      else failedMetrics.add(m);
    }
    for (const m of failedMetrics) {
      findings.push({
        code: "CROSS_TABLE_REFERENCE_UNRESOLVED",
        severity: "warning",
        message: `\`${m.name ?? m.id}\` on \`${e.id}\` uses a cross-table reference that could not be rewritten to a same-element reference; it was left on \`${e.id}\` as the engine emitted it and will not validate in Sigma.`,
        remediation: "Rebuild this metric in Sigma on a derived element over the table, using the relationship picker.",
        line: 1,
        object: { kind: "metric", name: String(m.name ?? m.id), owner: e.id },
        source: "embed"
      });
    }
    dCols.push(...helpers);
    const base = { ...e };
    for (const m of failedMetrics) set2.delete(m);
    base.columns = (e.columns ?? []).filter((c) => !set2.has(c));
    if (e.metrics !== void 0) {
      const rest = e.metrics.filter((m) => !set2.has(m));
      if (rest.length > 0) base.metrics = rest;
      else delete base.metrics;
    }
    const movedIds = new Set([...set2].map((o) => o.id));
    if (Array.isArray(e.order)) base.order = e.order.filter((x) => !movedIds.has(x));
    const path = e.source?.path;
    const lastSeg = Array.isArray(path) && path.length > 0 ? String(path[path.length - 1]) : void 0;
    const baseName = typeof e.name === "string" && e.name !== "" ? e.name : lastSeg ?? toDisplayName(e.id);
    const names = [...set2].map((o) => String(o.name ?? displayOf(o) ?? o.id));
    const includes = `Includes columns from related tables: ${names.join(", ")}.`;
    const baseDesc = typeof e.description === "string" ? e.description.trim() : "";
    const description = baseDesc !== "" ? `${baseDesc}${/[.!?]$/.test(baseDesc) ? " " : ". "}${includes}` : `${baseName} plus columns from related tables: ${names.join(", ")}.`;
    const derived = {
      id,
      kind: "table",
      name: `${baseName} (joined)`,
      description,
      source: { kind: "table", elementId: e.id },
      columns: dCols,
      ...dMetrics.length > 0 && { metrics: dMetrics },
      ...Array.isArray(e.order) && { order: dCols.map((c) => c.id) }
    };
    out.push(base, derived);
    const helperNote = helpers.length > 0 ? ` Helper column(s) added to \`${id}\` so metrics can reference joined columns by name: ${helpers.map((h) => h.name).join(", ")}.` : "";
    const movedFinding = {
      code: "CROSS_TABLE_REFERENCES_MOVED",
      severity: "info",
      message: `Sigma resolves cross-table references only on a derived element, so ${names.length} column(s)/metric(s) of \`${e.id}\` (${names.join(", ")}) now live on the derived element \`${id}\` ("${derived.name}"), which reads from \`${e.id}\` and joins through its relationships. \`${e.id}\` itself no longer carries them.${helperNote}`,
      remediation: `Use "${derived.name}" wherever you need these columns.`,
      line: 1,
      object: { kind: "view", name: viewName },
      source: "embed"
    };
    findings.push(movedFinding);
    derivedInfo.push({ baseId: e.id, baseName, derivedId: id, derivedName: derived.name, colIds: new Set(dCols.map((c) => c.id)), finding: movedFinding });
  }
  for (const info of derivedInfo) {
    let retargeted = 0;
    for (let i = 0; i < out.length; i++) {
      const x = out[i];
      if (x.id === info.baseId || x.id === info.derivedId || !Array.isArray(x.relationships)) continue;
      let changed = false;
      const rels = x.relationships.map((r) => {
        if (r.targetElementId !== info.baseId) return r;
        const keys = Array.isArray(r.keys) ? r.keys : [];
        const missing = keys.find((k) => !info.colIds.has(k.targetColumnId));
        if (missing !== void 0) {
          findings.push({
            code: "CROSS_TABLE_REFERENCE_UNRESOLVED",
            severity: "warning",
            message: `Relationship \`${r.name ?? r.id}\` on \`${x.id}\` was not retargeted at \`${info.derivedId}\` ("${info.derivedName}") because its key column \`${missing.targetColumnId}\` does not exist on that derived element; it still points at \`${info.baseId}\`, so related tables cannot reach the moved columns through it.`,
            remediation: `Point the relationship at "${info.derivedName}" in Sigma, or add the key column to that element.`,
            line: 1,
            object: { kind: "relationship", name: String(r.name ?? r.id), owner: x.id },
            source: "embed"
          });
          return r;
        }
        changed = true;
        retargeted++;
        return { ...r, targetElementId: info.derivedId };
      });
      if (changed) out[i] = { ...x, relationships: rels };
    }
    if (retargeted > 0) {
      info.finding.message += ` ${retargeted === 1 ? "The relationship" : `${retargeted} relationships`} into ${info.baseName} now ${retargeted === 1 ? "points" : "point"} at ${info.derivedName} so related tables can reach these columns.`;
    }
  }
  spec.pages[0].elements = out;
  return { spec: JSON.stringify(spec, null, 2), findings };
}

// semantic-tools-embed/src/usingPaths.ts
var REF32 = /\[([^\[\]/]+)\/([^\[\]/]+)\/([^\[\]/]+)\]/;
var PASSTHROUGH2 = /^\[([^\[\]/]+)\/([^\[\]]+)\]$/;
var MAX_LISTED = 12;
var slug2 = (x) => x.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "") || "x";
var ci = (x) => x.toLowerCase();
function displayOf2(col) {
  if (typeof col.name === "string" && col.name !== "") return col.name;
  const m = typeof col.formula === "string" ? PASSTHROUGH2.exec(col.formula) : null;
  return m ? m[2] : void 0;
}
function applyUsingPaths(specText, input) {
  const spec = JSON.parse(specText);
  const elements = spec.pages?.[0]?.elements ?? [];
  const byId = new Map(elements.map((e) => [e.id, e]));
  const findings = [];
  const unresolved = (pin, why) => {
    const owner = pin.owner === void 0 ? "" : ` on \`${pin.owner}\``;
    findings.push({
      code: "USING_PATH_UNRESOLVED",
      severity: "warning",
      message: `Metric \`${pin.name}\`${owner} is pinned with USING (${pin.using.join(", ")}) but ${why}; it was left on its table, where it behaves exactly like an unpinned metric (Snowflake would follow only the pinned relationship path).`,
      remediation: "Rebuild this metric in Sigma on a derived element over the table that joins only through the pinned relationship.",
      line: 1,
      object: { kind: "metric", name: pin.name, ...pin.owner !== void 0 && { owner: pin.owner } },
      source: "embed"
    });
  };
  const mapRelationship = (ir) => {
    const srcId = mkElementId(ir.sourceAlias);
    const tgtId = mkElementId(ir.targetAlias);
    const src = byId.get(srcId);
    if (!src) return { why: `table \`${ir.sourceAlias}\` is not in the converted model` };
    const wantKeys = ir.sourceColumns.map((c) => mkColumnId(srcId, c));
    const hits = (src.relationships ?? []).filter((r) => {
      const t = byId.get(r.targetElementId);
      const targetOk = t !== void 0 && (t.id === tgtId || t.source?.kind === "table" && t.source.elementId === tgtId);
      const keys = Array.isArray(r.keys) ? r.keys : [];
      return targetOk && keys.length === wantKeys.length && keys.every((k, i) => k.sourceColumnId === wantKeys[i]);
    });
    const base = toDisplayName(ir.name);
    const named = hits.filter((r) => r.name === base || typeof r.name === "string" && r.name.startsWith(`${base} `) && /^\d+$/.test(r.name.slice(base.length + 1)));
    const pool = named.length > 0 ? named : [];
    if (pool.length === 0) return { why: `relationship \`${ir.name}\` was not emitted into the model` };
    const exact = pool.filter((r) => r.name === base);
    const one = pool.length === 1 ? pool[0] : exact.length === 1 ? exact[0] : void 0;
    if (one === void 0) return { why: `relationship \`${ir.name}\` matches several emitted relationships` };
    if (typeof one.name !== "string" || one.name === "") return { why: `relationship \`${ir.name}\` has no name in the model` };
    return { rel: one, src };
  };
  const groups = /* @__PURE__ */ new Map();
  for (const pin of input.pinned) {
    const candidates = pin.owner === void 0 ? elements.filter((e) => e.metrics?.some((m) => m.id === mkMetricId(e.id, pin.name))) : [byId.get(mkElementId(pin.owner))].filter((e) => e !== void 0);
    if (candidates.length !== 1) {
      unresolved(pin, candidates.length === 0 ? "its converted metric was not found" : "its converted metric is ambiguous");
      continue;
    }
    const fact = candidates[0];
    const metric = fact?.metrics?.find((m) => m.id === mkMetricId(fact.id, pin.name));
    if (!metric) {
      unresolved(pin, `its metric is no longer on \`${fact.id}\``);
      continue;
    }
    if (pin.using.length === 0) {
      unresolved(pin, "no USING relationship was reported");
      continue;
    }
    const rels = [];
    let bad2;
    for (const name of pin.using) {
      const matching = input.relationships.filter(
        (r) => ci(r.name) === ci(name) && (pin.owner === void 0 ? mkElementId(r.sourceAlias) === fact.id : ci(r.sourceAlias) === ci(pin.owner))
      );
      if (matching.length !== 1) {
        bad2 = `relationship \`${name}\` ${matching.length === 0 ? "was not found on that table" : "is ambiguous"}`;
        break;
      }
      const m = mapRelationship(matching[0]);
      if ("why" in m) {
        bad2 = m.why;
        break;
      }
      if (!rels.some((x) => x.rel.id === m.rel.id)) rels.push({ rel: m.rel, snowflake: matching[0].name });
    }
    if (bad2 !== void 0) {
      unresolved(pin, bad2);
      continue;
    }
    rels.sort((a, b) => a.rel.id < b.rel.id ? -1 : a.rel.id > b.rel.id ? 1 : 0);
    const key = `${fact.id}\0${rels.map((r) => r.rel.id).join("\0")}`;
    const g = groups.get(key) ?? { fact, rels, pins: [] };
    g.pins.push({ pin, metric });
    groups.set(key, g);
  }
  if (groups.size === 0) return { spec: specText, findings };
  const used = new Set(elements.map((e) => e.id));
  const built = [];
  for (const key of [...groups.keys()].sort()) {
    const g = groups.get(key);
    const fact = g.fact;
    const tableName = (fact.columns ?? []).map((c) => PASSTHROUGH2.exec(c.formula ?? "")?.[1]).find(Boolean);
    if (tableName === void 0) {
      for (const p of g.pins) unresolved(p.pin, `\`${fact.id}\` has no passthrough column to build the path-bound element from`);
      continue;
    }
    const path = fact.source?.path;
    const lastSeg = Array.isArray(path) && path.length > 0 ? String(path[path.length - 1]) : void 0;
    const baseName = typeof fact.name === "string" && fact.name !== "" ? fact.name : lastSeg ?? toDisplayName(fact.id);
    const relNames = g.rels.map((r) => String(r.rel.name));
    let id = `${fact.id}__via__${g.rels.map((r) => r.rel.id).join("__")}`;
    for (let n = 2; used.has(id); n++) id = `${fact.id}__via__${g.rels.map((r) => r.rel.id).join("__")}_${n}`;
    used.add(id);
    const cols = [];
    const names = /* @__PURE__ */ new Set();
    const colIds = /* @__PURE__ */ new Set();
    const add = (display, formula, from, suffix) => {
      let name = display;
      if (names.has(ci(name))) {
        name = `${display} (${suffix})`;
        for (let n = 2; names.has(ci(name)); n++) name = `${display} (${suffix}) ${n}`;
      }
      names.add(ci(name));
      let cid = `${id}__${slug2(name)}`;
      for (let n = 2; colIds.has(cid); n++) cid = `${id}__${slug2(name)}_${n}`;
      colIds.add(cid);
      cols.push({
        id: cid,
        formula,
        name,
        ...from.description !== void 0 && { description: from.description },
        ...from.format !== void 0 && { format: from.format }
      });
    };
    for (const c of fact.columns ?? []) {
      const display = displayOf2(c);
      if (display === void 0 || REF32.test(String(c.formula ?? ""))) continue;
      add(display, `[${tableName}/${display}]`, c, "");
    }
    for (const { rel } of g.rels) {
      const target = byId.get(rel.targetElementId);
      for (const c of target?.columns ?? []) {
        const display = displayOf2(c);
        if (display === void 0 || REF32.test(String(c.formula ?? ""))) continue;
        add(display, `[${tableName}/${rel.name}/${relDisplaySegment(display, typeof c.name === "string" && c.name !== "")}]`, c, String(rel.name));
      }
    }
    let candidates = g.pins.slice();
    for (; ; ) {
      const metricNames = new Set(candidates.map((p) => String(p.metric.name ?? "")));
      const keep = candidates.filter((p) => {
        const f = String(p.metric.formula ?? "");
        const refs = [...f.matchAll(/\[([^\]]*)\]/g)].map((m) => m[1]);
        const bad2 = refs.find((r) => r.includes("/") || !cols.some((c) => c.name === r) && !metricNames.has(r));
        if (bad2 === void 0) return true;
        unresolved(p.pin, `its formula references \`[${bad2}]\`, which is not a column of the path-bound element`);
        return false;
      });
      if (keep.length === candidates.length) break;
      candidates = keep;
    }
    if (candidates.length === 0) continue;
    const moved = new Set(candidates.map((p) => p.metric));
    const derived = {
      id,
      kind: "table",
      name: `${baseName} (via ${relNames.join(" + ")})`,
      description: `${baseName} joined only through ${relNames.join(" + ")}. Metrics here follow Snowflake's USING (${g.rels.map((r) => r.snowflake).join(", ")}) path \u2014 slice them by this element's columns.`,
      source: { kind: "table", elementId: fact.id },
      columns: cols,
      metrics: candidates.map((p) => p.metric),
      order: cols.map((c) => c.id)
    };
    built.push({ fact, derived, moved, group: g });
  }
  if (built.length === 0) return { spec: specText, findings };
  const removed = new Set(built.flatMap((b) => [...b.moved]));
  const insertAfter = /* @__PURE__ */ new Map();
  for (const b of built) {
    const idx = elements.indexOf(b.fact);
    let at = idx;
    while (at + 1 < elements.length && String(elements[at + 1].id).startsWith(`${b.fact.id}__joined`)) at++;
    const anchor = elements[at].id;
    insertAfter.set(anchor, [...insertAfter.get(anchor) ?? [], b.derived]);
  }
  const out = [];
  for (const e of elements) {
    if (Array.isArray(e.metrics) && e.metrics.some((m) => removed.has(m))) {
      const rest = e.metrics.filter((m) => !removed.has(m));
      const copy = { ...e };
      if (rest.length > 0) copy.metrics = rest;
      else delete copy.metrics;
      out.push(copy);
    } else out.push(e);
    out.push(...insertAfter.get(e.id) ?? []);
  }
  spec.pages[0].elements = out;
  const lines = built.flatMap((b) => b.group.pins.filter((p) => b.moved.has(p.metric)).map((p) => `${p.pin.name} -> "${b.derived.name}"`));
  const listed = lines.slice(0, MAX_LISTED).join("; ") + (lines.length > MAX_LISTED ? `; and ${lines.length - MAX_LISTED} more` : "");
  findings.push({
    code: "USING_METRICS_PATH_BOUND",
    severity: "info",
    message: `${lines.length} metric(s) pinned with USING were moved to path-bound derived elements that join only through the pinned relationship(s), so their numbers follow Snowflake's USING path instead of being plain metrics identical to the unpinned ones: ${listed}.`,
    remediation: "Slice each of these metrics by the columns of the element it lives on; columns from a different element do not follow the pinned path.",
    line: 1,
    object: { kind: "view", name: input.viewName },
    source: "embed"
  });
  return { spec: JSON.stringify(spec, null, 2), findings };
}

// semantic-tools-embed/src/multiFact.ts
var MAX_LISTED2 = 5;
var NOT_REACHABLE = /\b([A-Za-z0-9_$]+)\.[A-Za-z0-9_$"]+ is not reachable from the emitted element/;
var lastSegment = (e) => {
  const p = e.source?.path;
  return typeof e.name === "string" && e.name !== "" ? e.name : Array.isArray(p) && p.length > 0 ? String(p[p.length - 1]) : String(e.id);
};
function applyMultiFact(specText, viewName, engineFindings = []) {
  const none = { findings: [] };
  let spec;
  try {
    spec = JSON.parse(specText);
  } catch {
    return none;
  }
  const elements = spec.pages?.[0]?.elements ?? [];
  const byId = new Map(elements.map((e) => [e.id, e]));
  const baseOf = (id) => {
    let cur = byId.get(id);
    for (let i = 0; cur !== void 0 && i < 50; i++) {
      const s = cur.source;
      if (s?.kind === "table" && typeof s.elementId === "string" && byId.has(s.elementId)) cur = byId.get(s.elementId);
      else break;
    }
    return cur?.id ?? id;
  };
  const adj = /* @__PURE__ */ new Map();
  const hasIncoming = /* @__PURE__ */ new Set();
  for (const e of elements) {
    const from = baseOf(e.id);
    for (const r of e.relationships ?? []) {
      if (typeof r.targetElementId !== "string") continue;
      const to = baseOf(r.targetElementId);
      if (to === from) continue;
      if (!adj.has(from)) adj.set(from, /* @__PURE__ */ new Set());
      adj.get(from).add(to);
      hasIncoming.add(to);
    }
  }
  const roots = [...adj.keys()].filter((id) => !hasIncoming.has(id));
  if (roots.length < 2) return none;
  const reach = (start) => {
    const seen = /* @__PURE__ */ new Set();
    const stack = [...adj.get(start) ?? []];
    while (stack.length > 0) {
      const id = stack.pop();
      if (seen.has(id)) continue;
      seen.add(id);
      for (const n of adj.get(id) ?? []) stack.push(n);
    }
    return seen;
  };
  const reachOf = new Map(roots.map((r) => [r, reach(r)]));
  const nameOf = (id) => lastSegment(byId.get(id) ?? { id });
  const pairs2 = [];
  for (let i = 0; i < roots.length; i++) {
    for (let j = i + 1; j < roots.length; j++) {
      const a = roots[i];
      const b = roots[j];
      const ra = reachOf.get(a);
      const rb = reachOf.get(b);
      const shared = [...ra].filter((x) => rb.has(x)).map(nameOf);
      if (shared.length > 0) pairs2.push({ a, b, shared });
    }
  }
  if (pairs2.length === 0) return none;
  const listPair = (p) => {
    const shown = p.shared.slice(0, MAX_LISTED2).join(", ");
    const more = p.shared.length > MAX_LISTED2 ? `, and ${p.shared.length - MAX_LISTED2} more` : "";
    return `${nameOf(p.a)} and ${nameOf(p.b)} share ${p.shared.length} dimension${p.shared.length === 1 ? "" : "s"} (${shown}${more}) but are not related to each other`;
  };
  const rootKey = /* @__PURE__ */ new Map();
  for (const r of roots) {
    const el = byId.get(r);
    rootKey.set(String(r).toLowerCase(), r);
    rootKey.set(lastSegment(el).toLowerCase(), r);
  }
  const dropped = [];
  for (const f of engineFindings) {
    if (f.code !== "EXPRESSION_NOT_REPRESENTABLE" || f.object.kind !== "metric") continue;
    const m = NOT_REACHABLE.exec(f.message);
    const ownerRoot = f.object.owner !== void 0 ? rootKey.get(f.object.owner.toLowerCase()) : void 0;
    let cross = false;
    if (m) {
      const other = rootKey.get(m[1].toLowerCase());
      cross = other !== void 0 && other !== ownerRoot;
    } else if (ownerRoot === void 0) {
      const expr = /`([^`]*)`/.exec(f.message)?.[1] ?? "";
      const hit = /* @__PURE__ */ new Set();
      for (const t of expr.matchAll(/([A-Za-z0-9_$]+)\./g)) {
        const root = rootKey.get(t[1].toLowerCase());
        if (root !== void 0) hit.add(root);
      }
      cross = hit.size >= 2;
    }
    if (cross && !dropped.includes(f.object.name)) dropped.push(f.object.name);
  }
  const message = `${pairs2.map(listPair).join("; ")}. Snowflake can combine their metrics per shared dimension; a Sigma element can only reach one of them. Build cross-fact metrics (a measure on one fact divided by a measure on another) by aggregating each fact to the shared dimension first, then joining those results; never join the raw facts on a dimension key (that double counts).` + (dropped.length > 0 ? ` Dropped cross-fact metrics: ${dropped.join(", ")}.` : "");
  return {
    findings: [
      {
        code: "MULTI_FACT_SHARED_DIMENSIONS",
        severity: "warning",
        message,
        remediation: "Aggregate each fact to the shared dimension (grouped element or custom SQL), then join the aggregates on that dimension; do not join the raw fact tables.",
        object: { kind: "view", name: viewName },
        source: "embed"
      }
    ]
  };
}

// semantic-tools-embed/src/yamlKnownKeys.json
var yamlKnownKeys_default = {
  _comment: "Keys the pinned upstream YAML reader (packages/snowflake-semantic-view/src/readYaml.ts) READS, per object level. scripts/yaml-key-drift.mjs derives the same sets from readYaml.ts and fails the build/check when they differ, so this table cannot rot when the PIN moves. `benign` = keys upstream does NOT read but which carry no conversion-relevant information (Sigma infers column types/granularity from the warehouse; `unique` and `relationship_type` are what SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW adds and the engine re-derives cardinality from the keys; `join_type: inner` is reported separately as JOIN_TYPE_INNER); NOT drift-checked, maintained by hand. Anything in neither list is reported as UNRECOGNIZED_YAML_KEY.",
  read: {
    root: ["name", "tables", "variables", "metrics", "relationships", "description", "tags", "max_staleness", "custom_instructions", "module_custom_instructions", "verified_queries"],
    table: ["name", "base_table", "primary_key", "unique_keys", "synonyms", "description", "tags", "facts", "dimensions", "time_dimensions", "metrics"],
    base_table: ["table", "database", "schema"],
    primary_key: ["columns"],
    unique_key: ["columns"],
    object: ["name", "expr", "access_modifier", "labels", "using_relationships", "non_additive_dimensions", "synonyms", "description", "tags", "cortex_search_service", "sample_values", "is_enum"],
    non_additive_dimension: ["dimension"],
    relationship: ["left_table", "right_table", "relationship_columns", "name"],
    relationship_column: ["type", "right_range", "left_column", "right_column"],
    right_range: ["start_column", "end_column"]
  },
  benign: {
    object: ["data_type", "time_granularity", "unique"],
    relationship: ["relationship_type", "join_type"]
  }
};

// semantic-tools-embed/src/yamlKeys.ts
var UNRECOGNIZED_YAML_KEY = "UNRECOGNIZED_YAML_KEY";
var MAX_PATHS = 12;
var benign = yamlKnownKeys_default.benign;
var isRec = (v) => typeof v === "object" && v !== null && !Array.isArray(v);
var recs = (v) => Array.isArray(v) ? v.filter(isRec) : [];
var label = (o, i) => typeof o.name === "string" || typeof o.name === "number" ? String(o.name) : String(i);
function walkKeys(o, level, path, out) {
  const known = /* @__PURE__ */ new Set([...yamlKnownKeys_default.read[level], ...benign[level] ?? []]);
  for (const k of Object.keys(o)) if (!known.has(k)) out.push(path === "" ? k : `${path}.${k}`);
}
function walkObjects(list2, path, out) {
  recs(list2).forEach((o, i) => walkKeys(o, "object", `${path}[${label(o, i)}]`, out));
}
function unrecognizedYamlPaths(document) {
  const out = [];
  if (!isRec(document)) return out;
  walkKeys(document, "root", "", out);
  recs(document.tables).forEach((t, i) => {
    const tp = `tables[${label(t, i)}]`;
    walkKeys(t, "table", tp, out);
    if (isRec(t.base_table)) walkKeys(t.base_table, "base_table", `${tp}.base_table`, out);
    if (isRec(t.primary_key)) walkKeys(t.primary_key, "primary_key", `${tp}.primary_key`, out);
    recs(t.unique_keys).forEach((k, j) => walkKeys(k, "unique_key", `${tp}.unique_keys[${j}]`, out));
    for (const kind of ["dimensions", "time_dimensions", "facts", "metrics"]) walkObjects(t[kind], `${tp}.${kind}`, out);
  });
  walkObjects(document.metrics, "metrics", out);
  recs(document.relationships).forEach((r, i) => {
    const rp = `relationships[${label(r, i)}]`;
    walkKeys(r, "relationship", rp, out);
    recs(r.relationship_columns).forEach((c, j) => {
      const cp = `${rp}.relationship_columns[${j}]`;
      walkKeys(c, "relationship_column", cp, out);
      if (isRec(c.right_range)) walkKeys(c.right_range, "right_range", `${cp}.right_range`, out);
    });
  });
  return out;
}
function unrecognizedYamlKeyFinding(yamlText, viewName) {
  let doc;
  try {
    doc = parse(yamlText);
  } catch {
    return void 0;
  }
  const paths = unrecognizedYamlPaths(doc);
  if (paths.length === 0) return void 0;
  const shown = paths.slice(0, MAX_PATHS).join(", ");
  const more = paths.length > MAX_PATHS ? ` (and ${paths.length - MAX_PATHS} more)` : "";
  const nested = paths.filter((p) => /^tables\[.*\]\.relationships$/.test(p));
  let message = `${paths.length} YAML key path${paths.length === 1 ? " is" : "s are"} not part of the Snowflake semantic view spec the converter reads, so ${paths.length === 1 ? "it was" : "they were"} ignored and NOT converted: ${shown}${more}.`;
  if (nested.length > 0) {
    message += ` Relationships declared inside a table (${nested.join(", ")}) are not part of the Snowflake semantic view spec and were NOT converted; move them to the top-level \`relationships\` list with \`left_table\` / \`right_table\` / \`relationship_columns\`.`;
  }
  return {
    code: UNRECOGNIZED_YAML_KEY,
    severity: "warning",
    message,
    remediation: "Fix or remove the listed keys (check for typos such as `dimensons`) and convert again; the converted model is missing whatever they described.",
    line: 1,
    object: { kind: "view", name: viewName },
    source: "embed"
  };
}
function joinTypeInnerFindings(yamlText) {
  let doc;
  try {
    doc = parse(yamlText);
  } catch {
    return [];
  }
  if (!isRec(doc)) return [];
  const out = [];
  recs(doc.relationships).forEach((r, i) => {
    if (typeof r.join_type !== "string" || r.join_type.trim().toLowerCase() !== "inner") return;
    const name = label(r, i);
    out.push({
      code: "JOIN_TYPE_INNER",
      severity: "warning",
      message: `Relationship ${name} is an INNER join in Snowflake; Sigma relationships keep fact rows without a match, so counts can be higher in Sigma for rows whose key has no match.`,
      remediation: "If the difference matters, filter out rows with no match in Sigma (for example, require the joined key to be non-null) or check the key coverage in the warehouse.",
      line: 1,
      object: { kind: "relationship", name },
      source: "embed"
    });
  });
  return out;
}

// semantic-tools-embed/src/formats.ts
var mk = (u, why, v) => ({ u, why, ...v !== void 0 && { v } });
var unknown = (why) => mk("unknown", why);
function typeKind(t) {
  if (t === void 0) return void 0;
  const s = t.trim().toUpperCase();
  if (s === "") return void 0;
  const base = s.replace(/\(.*$/s, "").trim();
  if (/^(INT|INTEGER|BIGINT|SMALLINT|TINYINT|BYTEINT)$/.test(base)) return "int";
  if (/^(NUMBER|DECIMAL|NUMERIC|DEC)$/.test(base)) {
    const m = /\(\s*\d+\s*(?:,\s*(\d+)\s*)?\)/.exec(s);
    if (!m) return "num";
    return (m[1] ?? "0") === "0" ? "int" : "dec";
  }
  if (/^(FLOAT|FLOAT4|FLOAT8|DOUBLE|DOUBLE PRECISION|REAL|DECFLOAT)$/.test(base)) return "dec";
  if (/^(FIXED)$/.test(base)) return "num";
  return "other";
}
var NUM_RE = /^[-+]?(?:\d+\.?\d*|\.\d+)(?:e[-+]?\d+)?$/i;
function sampleStats(samples) {
  if (samples === void 0) return void 0;
  const vals = samples.map((s) => String(s).trim()).filter((s) => s !== "");
  if (vals.length === 0) return void 0;
  if (!vals.every((v) => NUM_RE.test(v))) return { numeric: false, min: NaN, max: NaN, allInt: false };
  const n = vals.map(Number);
  return { numeric: true, min: Math.min(...n), max: Math.max(...n), allInt: n.every(Number.isInteger) };
}
var CURRENCY = new Set("amount amt price cost revenue sales balance acctbal totalprice extendedprice supplycost spend fee fees tax profit income payment salary charge".split(" "));
var PERCENT = new Set("rate pct percent percentage ratio share".split(" "));
var PERCENT_POINTS = new Set("pct percent percentage".split(" "));
var CURRENCYISH = new Set("hourly shipping fee fees price cost wage pay salary".split(" "));
var AMBIGUOUS = /* @__PURE__ */ new Set(["discount", "margin"]);
var COUNT = /* @__PURE__ */ new Set(["count", "qty", "quantity", "units"]);
var IDENT = new Set("key id ids code zip phone year month day identifier uuid guid sk fk pk".split(" "));
function nameTokens(names) {
  const out = /* @__PURE__ */ new Set();
  for (const n of names) {
    const words = n.replace(/([a-z0-9])([A-Z])/g, "$1 $2").split(/[^A-Za-z0-9]+/).filter(Boolean).map((w) => w.toLowerCase());
    for (const w of words) {
      out.add(w);
      if (w.length > 3 && w.endsWith("s") && !w.endsWith("ss")) out.add(w.slice(0, -1));
    }
  }
  return out;
}
var hasAny = (tk, words) => [...tk].find((w) => words.has(w));
function classifyBase(ev) {
  const kind = typeKind(ev.dataType);
  const stats = sampleStats(ev.samples);
  const shown = ev.names.find(Boolean) ?? "column";
  if (kind === "other") return mk("nonnumeric", `${shown} is ${ev.dataType}, not numeric`);
  if (stats !== void 0 && !stats.numeric) return mk("nonnumeric", `${shown} has non-numeric sample values`);
  const tk = nameTokens(ev.names);
  const idWord = hasAny(tk, IDENT) ?? [...tk].find((w) => w.length > 3 && w.endsWith("key")) ?? (tk.has("number") && !tk.has("of") && kind !== "dec" ? "number" : void 0);
  if (idWord !== void 0) return mk("id", `${shown} looks like an identifier (word "${idWord}")`);
  if (kind === "int") return mk("integer", `${shown} is ${ev.dataType} (integer)`);
  const typed = kind !== void 0;
  const pct = hasAny(tk, PERCENT);
  const amb = hasAny(tk, AMBIGUOUS);
  const cur = hasAny(tk, CURRENCY);
  const cnt = hasAny(tk, COUNT) ?? (tk.has("number") && tk.has("of") ? "number of" : void 0);
  const bareRate = pct !== void 0 && hasAny(tk, PERCENT_POINTS) === void 0;
  const curish = bareRate ? hasAny(tk, CURRENCYISH) : void 0;
  const noEvidence = (what) => typed || stats !== void 0 ? mk("plain", `${shown}: ${what}`) : unknown(`${shown}: ${what}; no data_type or sample_values`);
  if (curish !== void 0 && !(stats !== void 0 && stats.min >= -1 && stats.max <= 1)) {
    return cur !== void 0 ? mk("currency", `${shown} is a "${pct}" beside currency word "${cur}"`) : noEvidence(`"${curish} ${pct}" is a price-like rate, not a percentage, and no currency word confirms an amount`);
  }
  if (pct !== void 0 || amb !== void 0 && cur === void 0) {
    const w = pct ?? amb;
    if (stats !== void 0) {
      if (stats.min >= -1 && stats.max <= 1) return mk("fraction", `${shown} ("${w}") has sample values within [${stats.min}, ${stats.max}]`);
      const ppWord = hasAny(tk, PERCENT_POINTS);
      if (ppWord !== void 0 && stats.max > 1 && stats.max <= 100 && stats.min >= -100) {
        return mk("percentpoints", `${shown} ("${ppWord}") has sample values in (1, 100], i.e. percent points`);
      }
      return mk("plain", `${shown} ("${w}") has sample values outside 0-1 (percent points or amounts)`);
    }
    return noEvidence(`"${w}" could be a rate or an amount and there are no sample_values to tell`);
  }
  if (amb !== void 0 && cur !== void 0) {
    if (stats !== void 0 && stats.min >= -1 && stats.max <= 1) return mk("plain", `${shown} ("${amb}" + "${cur}") has samples within 0-1; rate or amount is unclear`);
    return mk("currency", `${shown} has currency word "${cur}"`);
  }
  if (cur !== void 0 && cnt !== void 0) {
    return typed ? mk("plain", `${shown} mixes "${cur}" and "${cnt}"`) : unknown(`${shown} mixes "${cur}" and "${cnt}"; no data_type`);
  }
  if (cur !== void 0) return mk("currency", `${shown} has currency word "${cur}"`);
  if (cnt !== void 0) {
    if (typed) return stats?.allInt ? mk("integer", `${shown} has count word "${cnt}" and integer samples`) : mk("plain", `${shown} is a decimal with count word "${cnt}"`);
    if (stats !== void 0 && !stats.allInt) return mk("plain", `${shown} has count word "${cnt}" but fractional samples`);
    return mk("integer", `${shown} has count word "${cnt}"`);
  }
  return noEvidence("no unit word in the name");
}
function tokenize(src) {
  const out = [];
  let i = 0;
  while (i < src.length) {
    const c = src[i];
    if (/\s/.test(c)) {
      i++;
      continue;
    }
    if (c === "[") {
      const j = src.indexOf("]", i + 1);
      if (j < 0) throw new Error("unterminated ref");
      out.push({ t: "ref", v: src.slice(i + 1, j) });
      i = j + 1;
      continue;
    }
    if (c === '"' || c === "'") {
      let j = i + 1;
      for (; ; ) {
        if (j >= src.length) throw new Error("unterminated string");
        if (src[j] === "\\") {
          j += 2;
          continue;
        }
        if (src[j] === c) {
          if (src[j + 1] === c) {
            j += 2;
            continue;
          }
          break;
        }
        j++;
      }
      out.push({ t: "str", v: src.slice(i + 1, j) });
      i = j + 1;
      continue;
    }
    const num = /^(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?/.exec(src.slice(i));
    if (num) {
      out.push({ t: "num", v: num[0] });
      i += num[0].length;
      continue;
    }
    const id = /^[A-Za-z_][A-Za-z0-9_]*/.exec(src.slice(i));
    if (id) {
      out.push({ t: "id", v: id[0] });
      i += id[0].length;
      continue;
    }
    const two = src.slice(i, i + 2);
    if (["<=", ">=", "<>", "!=", "==", "&&", "||"].includes(two)) {
      out.push({ t: "op", v: two });
      i += 2;
      continue;
    }
    if ("+-*/^%&(),<>=!".includes(c)) {
      out.push({ t: "op", v: c });
      i++;
      continue;
    }
    throw new Error(`unexpected character ${c}`);
  }
  return out;
}
function parseFormula(src) {
  try {
    const toks = tokenize(src);
    let p = 0;
    const peek = () => toks[p];
    const isOp = (v) => peek()?.t === "op" && peek().v === v;
    const isKw = (v) => peek()?.t === "id" && peek().v.toLowerCase() === v;
    const expect = (v) => {
      if (!isOp(v)) throw new Error(`expected ${v}`);
      p++;
    };
    const parseOr = () => {
      let l = parseAnd();
      while (isKw("or") || isOp("||")) {
        p++;
        l = { k: "bin", op: "or", l, r: parseAnd() };
      }
      return l;
    };
    const parseAnd = () => {
      let l = parseNot();
      while (isKw("and") || isOp("&&")) {
        p++;
        l = { k: "bin", op: "and", l, r: parseNot() };
      }
      return l;
    };
    const parseNot = () => {
      if (isKw("not") || isOp("!")) {
        p++;
        return { k: "not", x: parseNot() };
      }
      return parseCmp();
    };
    const CMP = ["=", "==", "!=", "<>", "<", ">", "<=", ">="];
    const parseCmp = () => {
      let l = parseAdd();
      while (peek()?.t === "op" && CMP.includes(peek().v)) {
        const op = peek().v;
        p++;
        l = { k: "bin", op, l, r: parseAdd() };
      }
      return l;
    };
    const parseAdd = () => {
      let l = parseMul();
      while (isOp("+") || isOp("-") || isOp("&")) {
        const op = peek().v;
        p++;
        l = { k: "bin", op, l, r: parseMul() };
      }
      return l;
    };
    const parseMul = () => {
      let l = parseUnary();
      while (isOp("*") || isOp("/") || isOp("%")) {
        const op = peek().v;
        p++;
        l = { k: "bin", op, l, r: parseUnary() };
      }
      return l;
    };
    const parseUnary = () => {
      if (isOp("-")) {
        p++;
        return { k: "neg", x: parseUnary() };
      }
      if (isOp("+")) {
        p++;
        return parseUnary();
      }
      const base = parsePrimary();
      if (isOp("^")) {
        p++;
        return { k: "bin", op: "^", l: base, r: parseUnary() };
      }
      return base;
    };
    const parsePrimary = () => {
      const t = peek();
      if (t === void 0) throw new Error("unexpected end");
      if (t.t === "num") {
        p++;
        return { k: "num", v: Number(t.v) };
      }
      if (t.t === "str") {
        p++;
        return { k: "str", v: t.v };
      }
      if (t.t === "ref") {
        p++;
        return { k: "ref", path: t.v.split("/") };
      }
      if (t.t === "id") {
        p++;
        if (isOp("(")) {
          p++;
          const args = [];
          if (!isOp(")")) {
            for (; ; ) {
              args.push(parseOr());
              if (isOp(",")) {
                p++;
                continue;
              }
              break;
            }
          }
          expect(")");
          return { k: "call", name: t.v, args };
        }
        return { k: "id", name: t.v };
      }
      if (isOp("(")) {
        p++;
        const e = parseOr();
        expect(")");
        return e;
      }
      throw new Error(`unexpected ${t.v}`);
    };
    const root = parseOr();
    if (p !== toks.length) throw new Error("trailing tokens");
    return root;
  } catch {
    return void 0;
  }
}
var numeric = (a) => a.u !== "unknown" && a.u !== "id" && a.u !== "nonnumeric";
var isZero = (a) => a.u === "scalar" && a.v === 0;
var bad = (a, b, op) => unknown(!numeric(a) ? a.why : !numeric(b) ? b.why : `${a.u} ${op} ${b.u} has no confident unit`);
function addUnits(a, b, op) {
  if (!numeric(a) || !numeric(b)) return bad(a, b, op);
  if (isZero(a)) return b;
  if (isZero(b)) return a;
  if (a.u === b.u) return a.u === "scalar" ? mk("scalar", "literal", a.v !== void 0 && b.v !== void 0 ? op === "+" ? a.v + b.v : a.v - b.v : void 0) : a;
  const [s, o] = a.u === "scalar" ? [a, b] : b.u === "scalar" ? [b, a] : [void 0, void 0];
  if (s !== void 0 && o !== void 0) {
    if (o.u === "fraction") return o;
    if (o.u === "plain") return o;
    if (o.u === "integer" && Number.isInteger(s.v)) return o;
  }
  return bad(a, b, op);
}
function mulUnits(a, b) {
  if (!numeric(a) || !numeric(b)) return bad(a, b, "*");
  if (a.u === "scalar" && b.u === "scalar") return mk("scalar", "literal", a.v !== void 0 && b.v !== void 0 ? a.v * b.v : void 0);
  for (const [s, o] of [[a, b], [b, a]]) {
    if (s.u === "scalar") {
      if (s.v === 100 && o.u === "fraction") return mk("percentpoints", "a fraction times 100 is percent points");
      if (o.u === "percentpoints") return unknown("scaling percent points has no confident unit");
      return o;
    }
  }
  if (a.u === "currency" && ["integer", "plain", "fraction"].includes(b.u) || b.u === "currency" && ["integer", "plain", "fraction"].includes(a.u)) {
    return mk("currency", "currency scaled by a number");
  }
  if (a.u === "integer" && b.u === "integer") return mk("integer", "integer times integer");
  return bad(a, b, "*");
}
var RATIO_NAME = new Set("rate pct percent percentage ratio share".split(" "));
var hasRatioName = (name) => name !== void 0 && hasAny(nameTokens([name]), RATIO_NAME) !== void 0;
function divUnits(a, b, ratioNamed = false) {
  if (!numeric(a) || !numeric(b)) return bad(a, b, "/");
  if (a.u === "scalar" && b.u === "scalar") return mk("scalar", "literal", a.v !== void 0 && b.v !== void 0 && b.v !== 0 ? a.v / b.v : void 0);
  if (a.u === b.u && a.u === "currency") return mk("fraction", "currency divided by currency is a ratio");
  if (a.u === b.u && (a.u === "integer" || a.u === "plain")) {
    return ratioNamed ? mk("fraction", `${a.u} divided by ${b.u} and the name says ratio`) : mk("plain", `${a.u} divided by ${b.u} is an average or per-unit figure unless its name says rate/percent/ratio/share`);
  }
  if (b.u === "scalar") return a.u === "percentpoints" ? unknown("dividing percent points by a number has no confident unit") : a;
  if (a.u === "currency" && (b.u === "integer" || b.u === "plain")) return mk("currency", "currency per unit");
  return bad(a, b, "/");
}
function unifyUnits(list2) {
  const real = list2.filter((x) => !isZero(x));
  if (real.length === 0) return list2[0] ?? unknown("no result branches");
  const first = real[0];
  if (!numeric(first)) return first.u === "nonnumeric" && real.every((x) => x.u === "nonnumeric") ? first : unknown(first.why);
  for (const x of real) {
    if (x.u !== first.u) return unknown(`branches differ (${first.u} vs ${x.u})`);
  }
  return first;
}
var COUNT_FNS = /* @__PURE__ */ new Set(["count", "countdistinct", "countif", "countdistinctif"]);
var SAME_FNS = /* @__PURE__ */ new Set(["sum", "avg", "min", "max", "median", "first", "last", "sumif", "avgif", "minif", "maxif"]);
var PASS_FNS = /* @__PURE__ */ new Set(["round", "abs", "zn", "floor", "ceiling"]);
function evalNode(n, ref, ratioNamed = false) {
  switch (n.k) {
    case "num":
      return mk("scalar", "literal", n.v);
    case "str":
      return mk("nonnumeric", "text literal");
    case "ref":
      return ref(n.path);
    case "id": {
      const l = n.name.toLowerCase();
      if (l === "null") return mk("scalar", "null", 0);
      return unknown(`${n.name} is not numeric`);
    }
    case "neg": {
      const x = evalNode(n.x, ref, ratioNamed);
      return numeric(x) ? x : unknown(x.why);
    }
    case "not":
      return unknown("logical");
    case "bin": {
      const l = evalNode(n.l, ref, ratioNamed);
      const r = evalNode(n.r, ref, ratioNamed);
      switch (n.op) {
        case "+":
        case "-":
          return addUnits(l, r, n.op);
        case "*":
          return mulUnits(l, r);
        case "/":
          return divUnits(l, r, ratioNamed);
        default:
          return unknown(`operator ${n.op}`);
      }
    }
    case "call": {
      const f = n.name.toLowerCase();
      const a = n.args;
      if (COUNT_FNS.has(f)) return mk("integer", `${n.name} counts rows`);
      if (SAME_FNS.has(f)) {
        if (a.length === 0) return unknown(`${n.name} has no argument`);
        const x = evalNode(a[0], ref, ratioNamed);
        if ((f === "sum" || f === "sumif") && x.u === "percentpoints") return unknown("summing percent points is meaningless");
        if (f === "sum" && x.u === "scalar") return Number.isInteger(x.v) ? mk("integer", "sum of an integer literal") : unknown("sum of a decimal literal");
        if (["min", "max", "first", "last"].includes(f)) return x.u === "scalar" ? unknown("literal aggregate") : x;
        if (!numeric(x) || x.u === "scalar") return unknown(x.why);
        if ((f === "avg" || f === "median" || f === "avgif") && x.u === "integer") return mk("plain", `${n.name} of an integer is not an integer`);
        return x;
      }
      if (PASS_FNS.has(f)) return a.length > 0 ? evalNode(a[0], ref, ratioNamed) : unknown(`${n.name} has no argument`);
      if (f === "nullif") return a.length > 0 ? evalNode(a[0], ref, ratioNamed) : unknown("NullIf has no argument");
      if (f === "callnumber" && a.length === 3 && a[0].k === "str" && /^div0(null)?$/i.test(a[0].v)) return divUnits(evalNode(a[1], ref, ratioNamed), evalNode(a[2], ref, ratioNamed), ratioNamed);
      if (f === "coalesce") return unifyUnits(a.map((x) => evalNode(x, ref, ratioNamed)));
      if (f === "if") {
        const res = [];
        for (let i = 1; i < a.length; i += 2) res.push(a[i]);
        if (a.length % 2 === 1 && a.length > 1) res.push(a[a.length - 1]);
        return res.length === 0 ? unknown("If has no result branch") : unifyUnits(res.map((x) => evalNode(x, ref, ratioNamed)));
      }
      if (f === "switch") {
        const res = [];
        for (let i = 2; i < a.length; i += 2) res.push(a[i]);
        if (a.length % 2 === 0 && a.length > 2) res.push(a[a.length - 1]);
        return res.length === 0 ? unknown("Switch has no result branch") : unifyUnits(res.map((x) => evalNode(x, ref, ratioNamed)));
      }
      return unknown(`function ${n.name} is not modelled`);
    }
  }
}
function unitOfFormula(formula, refs, itemName) {
  const ast = parseFormula(formula);
  if (ast === void 0) return unknown("formula could not be parsed");
  return evalNode(ast, (path) => {
    const hit = refs[path.join("/")] ?? refs[path[path.length - 1]];
    if (hit === void 0) return unknown(`unresolved reference [${path.join("/")}]`);
    return typeof hit === "string" ? mk(hit, "given") : hit;
  }, hasRatioName(itemName));
}
function formatFor(u) {
  switch (u) {
    case "currency":
      return { kind: "number", formatString: "$,.2f", currencySymbol: "$" };
    case "integer":
      return { kind: "number", formatString: ",.0f" };
    case "fraction":
      return { kind: "number", formatString: ",.2%" };
    // Same shape src/alteryx.ts and src/oac.ts emit for values already multiplied by 100.
    case "percentpoints":
      return { kind: "number", formatString: ",.2f", suffix: "%" };
    default:
      return void 0;
  }
}
var LABEL = { currency: "currency", integer: "integer", fraction: "percent", percentpoints: "percent points" };
var PASSTHROUGH3 = /^\[[^\[\]/]+\/[^\[\]]+\]$/;
var PASSTHROUGH22 = /^\[[^\[\]/]+\/[^\[\]/]+\]$/;
var PASSTHROUGH_PARTS = /^\[([^\[\]/]+)\/([^\[\]]+)\]$/;
function pretty(formula) {
  let s = formula.replace(/\[([^\]]*)\]/g, (_m, inner) => inner.split("/").pop()).replace(/\s*\*\s*/g, " \xD7 ").replace(/\s+\/\s+/g, " \xF7 ").replace(/\s+-\s+/g, " \u2212 ");
  const m = /^([A-Za-z]+)\((.*)\)$/s.exec(s);
  if (m) {
    let depth = 0;
    let ok = true;
    for (const ch of m[2]) {
      if (ch === "(") depth++;
      else if (ch === ")" && --depth < 0) {
        ok = false;
        break;
      }
    }
    if (ok && depth === 0) s = `${m[1]} of ${m[2]}`;
  }
  return s;
}
function inferFormats(specText, opts, viewName) {
  const spec = JSON.parse(specText);
  const elements = spec.pages?.[0]?.elements ?? [];
  const infos = /* @__PURE__ */ new Map();
  const factsByEl = /* @__PURE__ */ new Map();
  for (const [alias, list2] of opts.facts) factsByEl.set(opts.elementIdOf(alias), list2);
  for (const el of elements) {
    const byName = /* @__PURE__ */ new Map();
    const byId = /* @__PURE__ */ new Map();
    for (const o of [...el.columns ?? [], ...el.metrics ?? []]) {
      byId.set(o.id, o);
      const disp = typeof o.name === "string" ? o.name : PASSTHROUGH_PARTS.exec(String(o.formula ?? ""))?.[2];
      if (disp !== void 0) {
        byName.set(disp, o);
        if (!byName.has(disp.toLowerCase())) byName.set(disp.toLowerCase(), o);
      }
    }
    const path = el.source?.path;
    const tableNames = /* @__PURE__ */ new Set([String(el.id).toLowerCase()]);
    if (Array.isArray(path) && path.length > 0) tableNames.add(String(path[path.length - 1]).toLowerCase());
    if (typeof el.name === "string") tableNames.add(el.name.toLowerCase());
    infos.set(el.id, { el, byName, byId, tableNames, facts: factsByEl.get(el.id) ?? [] });
  }
  const colTypes = /* @__PURE__ */ new Map();
  for (const [k, cols] of Object.entries(opts.columnTypes ?? {})) {
    colTypes.set(k.toUpperCase(), new Map(Object.entries(cols ?? {}).map(([c, t]) => [c.toUpperCase(), String(t)])));
  }
  const memo = /* @__PURE__ */ new Map();
  const baseOf = (info, col) => {
    const tail = String(col.id).startsWith(`${info.el.id}__`) ? String(col.id).slice(String(info.el.id).length + 2) : String(col.id);
    const display = PASSTHROUGH_PARTS.exec(String(col.formula))?.[2];
    const matched = info.facts.filter((f) => f.phys !== void 0 && mkColumnId(info.el.id, f.phys) === col.id);
    const names = /* @__PURE__ */ new Set([tail.toUpperCase()]);
    if (typeof col.name === "string") names.add(col.name);
    if (display !== void 0) names.add(display);
    let dataType;
    let samples;
    for (const f of matched) {
      names.add(f.name);
      if (f.phys !== void 0) names.add(f.phys);
      dataType ??= f.dataType;
      if (f.samples !== void 0) samples = [...samples ?? [], ...f.samples];
    }
    if (dataType === void 0) {
      const path = info.el.source?.path;
      const tbl = Array.isArray(path) ? colTypes.get(path.join(".").toUpperCase()) : void 0;
      const physGuess = matched.find((f) => f.phys)?.phys ?? tail.toUpperCase();
      dataType = tbl?.get(physGuess) ?? (display !== void 0 ? tbl?.get(display.toUpperCase().replace(/\s+/g, "_")) : void 0);
    }
    return classifyBase({ names: [...names], ...dataType !== void 0 && { dataType }, ...samples !== void 0 && { samples } });
  };
  const unitOf = (info, o) => {
    const seen = memo.get(o);
    if (seen === "visiting") return unknown("circular reference");
    if (seen !== void 0) return seen;
    memo.set(o, "visiting");
    let r;
    const formula = typeof o.formula === "string" ? o.formula : "";
    if (PASSTHROUGH22.test(formula) && (info.el.columns ?? []).includes(o)) r = baseOf(info, o);
    else {
      const ast = parseFormula(formula);
      r = ast === void 0 ? unknown("formula could not be parsed") : evalNode(ast, (path) => resolve(info, path), hasRatioName(typeof o.name === "string" ? o.name : void 0));
    }
    memo.set(o, r);
    return r;
  };
  const findEl = (info, name) => {
    const l = name.toLowerCase();
    if (info.tableNames.has(l)) return info;
    for (const other of infos.values()) if (other.tableNames.has(l)) return other;
    return info;
  };
  const lookup = (info, name) => info.byName.get(name) ?? info.byName.get(name.toLowerCase()) ?? info.byId.get(name);
  const resolve = (info, path) => {
    if (path.length === 1) {
      const o = lookup(info, path[0]);
      return o ? unitOf(info, o) : unknown(`unresolved reference [${path[0]}]`);
    }
    if (path.length === 2) {
      const owner = findEl(info, path[0]);
      const o = lookup(owner, path[1]);
      return o ? unitOf(owner, o) : unknown(`unresolved reference [${path.join("/")}]`);
    }
    if (path.length === 3) {
      const owner = findEl(info, path[0]);
      const rel = (owner.el.relationships ?? []).find((r) => r.id === path[1] || r.name === path[1]);
      const target = rel ? infos.get(rel.targetElementId) : void 0;
      const o = target ? lookup(target, path[2]) : void 0;
      return target && o ? unitOf(target, o) : unknown(`unresolved reference [${path.join("/")}]`);
    }
    return unknown(`unresolved reference [${path.join("/")}]`);
  };
  const items = [];
  const inferred = [];
  const notInferred = [];
  const visit3 = (info, o, isMetric) => {
    if (o.format !== void 0 || typeof o.formula !== "string") return;
    if (!isMetric && PASSTHROUGH3.test(o.formula)) return;
    const r = unitOf(info, o);
    const fmt = formatFor(r.u);
    const display = String(o.name ?? o.id);
    const legacy = inferSigmaFormat(o.formula, o.name);
    items.push({ owner: String(info.el.id), name: display, unit: r.u, ...fmt && { format: fmt }, legacy, why: r.why });
    if (fmt) {
      o.format = fmt;
      inferred.push(`${display}: ${LABEL[r.u]} \u2014 ${pretty(o.formula)}`);
    } else if (legacy) {
      notInferred.push(`${display} (${r.why})`);
    }
  };
  for (const info of infos.values()) {
    for (const c of info.el.columns ?? []) visit3(info, c, false);
    for (const m of info.el.metrics ?? []) visit3(info, m, true);
  }
  const cap = (list2) => list2.length > 12 ? `${list2.slice(0, 12).join("; ")}; and ${list2.length - 12} more` : list2.join("; ");
  const findings = [];
  if (inferred.length > 0) {
    findings.push({
      code: "FORMATS_INFERRED",
      severity: "info",
      message: `Inferred number formats for ${inferred.length} metric(s)/calculated column(s) from column types, sample values and formula structure: ${cap(inferred)}.`,
      remediation: "Review these formats. Adding `data_type` and `sample_values` to the source columns (or passing column types) improves the result.",
      object: { kind: "view", name: viewName },
      source: "embed"
    });
  }
  if (notInferred.length > 0) {
    findings.push({
      code: "FORMATS_NOT_INFERRED",
      severity: "info",
      message: `${notInferred.length} metric(s)/calculated column(s) were left without a number format because the evidence did not confirm a unit (a name-only guess could be wrong, e.g. a rate shown as currency): ${cap(notInferred)}.`,
      remediation: "Add `sample_values` (e.g. values between 0 and 1 mark a rate as a percentage) and/or `data_type` to the underlying columns, or pass column types, then convert again; or set the format in Sigma.",
      object: { kind: "view", name: viewName },
      source: "embed"
    });
  }
  return { spec: JSON.stringify(spec, null, 2), findings, items };
}

// semantic-tools-embed/src/index.ts
var ENGINE = {
  semanticToolsCommit: false ? "dev" : "360eb6af316deed841d78d879583c966239bf8be",
  embedVersion: false ? EMBED_VERSION : "1.4.0"
};
function detectFormat(source) {
  const head = source.slice(0, 2e3);
  if (/^\s*CREATE\s+(OR\s+REPLACE\s+)?SEMANTIC\s+VIEW\b/i.test(head)) {
    return "snowflake-semantic-view";
  }
  if (/^\s*name\s*:/m.test(head)) {
    return "snowflake-semantic-view-yaml";
  }
  return void 0;
}
function detectFormatLenient(source) {
  const direct = detectFormat(source);
  if (direct !== void 0) return direct;
  const stripped = source.replace(/^(?:\s+|--[^\n]*(?:\n|$)|\/\*[\s\S]*?\*\/)+/, "");
  return stripped === source ? void 0 : detectFormat(stripped);
}
function unqualifiedReferences(ir) {
  const database = [];
  const schema4 = [];
  for (const table of ir.tables) {
    const partCount = table.table.parts.length;
    if (partCount < 3) database.push(table.table.raw);
    if (partCount < 2) schema4.push(table.table.raw);
  }
  return { database, schema: schema4 };
}
var TRIPLES = {
  "snowflake-semantic-view": {
    reader: SNOWFLAKE_SEMANTIC_VIEW_READER,
    splitStatements,
    prepareSource: (source) => ({ source, repairedCount: 0 })
  },
  "snowflake-semantic-view-yaml": {
    reader: SNOWFLAKE_SEMANTIC_VIEW_YAML_READER,
    splitStatements: splitYamlStatements,
    prepareSource: (source) => {
      const repaired = repairCsvQuoteDoubling(source);
      return { source: repaired.text, repairedCount: repaired.count };
    }
  }
};
var OBJ_NONE = { kind: "view", name: "" };
function toEmbedFindings(list2) {
  return list2.map((f) => {
    const text = formatFinding(f);
    return {
      code: f.code,
      severity: f.severity,
      ...text,
      line: f.line,
      object: f.object
    };
  });
}
function mkResult(base) {
  return {
    ...base,
    findings: toEmbedFindings([...base.errors, ...base.overriddenErrors, ...base.warnings])
  };
}
function needsFindings(viewName, needs) {
  const out = [];
  const mk2 = (code, what, refs) => {
    if (refs.length === 0) return;
    out.push({
      code,
      severity: "error",
      message: `These table references are not fully qualified, so a ${what} is required but was not supplied: ${refs.join(", ")}.`,
      remediation: `Supply \`${what}\` (the ${what} that holds these tables) and convert again.`,
      line: 1,
      object: { kind: "view", name: viewName },
      source: "embed"
    });
  };
  mk2("NEEDS_DATABASE", "database", needs.database);
  mk2("NEEDS_SCHEMA", "schema", needs.schema);
  return out;
}
function failureResult(viewName, diagnostic) {
  return mkResult({
    viewName,
    ok: false,
    warnings: [],
    errors: [mkFinding("MALFORMED_DDL", OBJ_NONE, 1, { diagnostic })],
    overriddenErrors: [],
    bestEffortWouldConvert: false
  });
}
function errMsg(e) {
  return e instanceof Error ? e.message : String(e);
}
function rawConvert(source, format, options) {
  const t = TRIPLES[format];
  return convert({
    reader: t.reader,
    translator: SNOWFLAKE_TO_SIGMA_TRANSLATOR,
    writer: SIGMA_DATA_MODEL_WRITER,
    source,
    options
  });
}
var objKey = (kind, owner, name) => `${kind}:${owner ?? ""}.${name}`;
function strArray(v) {
  return Array.isArray(v) ? v.filter((x) => typeof x === "string" || typeof x === "number").map(String) : [];
}
function rec(v) {
  return Array.isArray(v) ? v.filter((x) => typeof x === "object" && x !== null && !Array.isArray(x)) : [];
}
function yamlSynonyms(source) {
  const out = { tables: /* @__PURE__ */ new Map(), objects: /* @__PURE__ */ new Map() };
  const doc = parse(source);
  if (typeof doc !== "object" || doc === null) return out;
  const add = (m, k, syns) => {
    if (syns.length > 0) m.set(k, syns);
  };
  for (const table of rec(doc.tables)) {
    const alias = String(table.name);
    add(out.tables, alias, strArray(table.synonyms));
    const kinds = [
      ["fact", table.facts],
      ["dimension", table.dimensions],
      ["dimension", table.time_dimensions],
      ["metric", table.metrics]
    ];
    for (const [kind, list2] of kinds) {
      for (const o of rec(list2)) add(out.objects, objKey(kind, alias, String(o.name)), strArray(o.synonyms));
    }
  }
  for (const o of rec(doc.metrics)) add(out.objects, objKey("metric", void 0, String(o.name)), strArray(o.synonyms));
  return out;
}
function maskNonCode(text) {
  const out = [];
  const blank = (from, to) => {
    for (let k = from; k < to; k++) out.push(text[k] === "\n" ? "\n" : " ");
  };
  let i = 0;
  while (i < text.length) {
    const c = text[i];
    if (c === "-" && text[i + 1] === "-") {
      const eol = text.indexOf("\n", i);
      const end = eol === -1 ? text.length : eol;
      blank(i, end);
      i = end;
    } else if (c === "/" && text[i + 1] === "*") {
      const close = text.indexOf("*/", i + 2);
      const end = close === -1 ? text.length : close + 2;
      blank(i, end);
      i = end;
    } else if (c === "'" || c === '"') {
      out.push(c);
      i++;
      while (i < text.length) {
        if (c === "'" && text[i] === "\\") {
          blank(i, Math.min(i + 2, text.length));
          i += 2;
          continue;
        }
        if (text[i] === c && text[i + 1] === c) {
          out.push(" ", " ");
          i += 2;
          continue;
        }
        if (text[i] === c) break;
        blank(i, i + 1);
        i++;
      }
      if (i < text.length) {
        out.push(c);
        i++;
      }
    } else {
      out.push(c);
      i++;
    }
  }
  return out.join("");
}
function ddlQuotedList(slice, keyword2) {
  const masked = maskNonCode(slice);
  const m = keyword2.exec(masked);
  if (!m) return void 0;
  const result = [];
  let i = m.index + m[0].length;
  while (i < masked.length && masked[i] !== ")") {
    if (masked[i] === "'") {
      const close = masked.indexOf("'", i + 1);
      if (close < 0) break;
      result.push(slice.slice(i + 1, close).replace(/''/g, "'"));
      i = close;
    }
    i++;
  }
  return result;
}
function ddlSynonymsIn(slice) {
  return ddlQuotedList(slice, /\bWITH\s+SYNONYMS\s*=?\s*\(/i) ?? [];
}
function ddlSynonyms(statement) {
  const out = { tables: /* @__PURE__ */ new Map(), objects: /* @__PURE__ */ new Map() };
  const parsed = parseSemanticView(mkSource(statement));
  if (!parsed.ok) return out;
  const view = parsed.view;
  for (const t of view.tables) {
    const s = ddlSynonymsIn(statement.slice(t.span.start, t.span.end));
    if (s.length > 0) out.tables.set(t.alias.name, s);
  }
  for (const o of [...view.facts, ...view.dimensions, ...view.metrics]) {
    const s = ddlSynonymsIn(statement.slice(o.span.start, o.span.end));
    if (s.length > 0) out.objects.set(objKey(o.kind, o.owner?.name, o.name.name), s);
  }
  return out;
}
function buildDesc(description, synonyms) {
  const base = (description ?? "").trim();
  const syns = synonyms.map((s) => s.trim()).filter(Boolean);
  if (syns.length === 0) return base || void 0;
  const suffix = "Also known as: " + syns.join(", ");
  return base ? base + (/[.!?]$/.test(base) ? " " : ". ") + suffix : suffix;
}
function withDescription(o, description, afterKey) {
  if ("description" in o) return { ...o, description };
  const out = {};
  let placed = false;
  for (const [k, v] of Object.entries(o)) {
    out[k] = v;
    if (afterKey !== void 0 && k === afterKey) {
      out.description = description;
      placed = true;
    }
  }
  if (!placed) out.description = description;
  return out;
}
function applySynonyms(specText, syn) {
  const applied = /* @__PURE__ */ new Set();
  const spec = JSON.parse(specText);
  const elements = spec.pages?.[0]?.elements ?? [];
  const byId = /* @__PURE__ */ new Map();
  elements.forEach((e, i) => byId.set(e.id, i));
  for (const [alias, syns] of syn.tables) {
    const idx = byId.get(mkElementId(alias));
    if (idx === void 0) continue;
    const el = elements[idx];
    const d = buildDesc(el.description, syns);
    if (d === void 0) continue;
    elements[idx] = withDescription(el, d, "kind");
    applied.add(`table:${alias}`);
  }
  for (const [key, syns] of syn.objects) {
    const m = /^(fact|dimension|metric):([^.]*)\.(.*)$/s.exec(key);
    if (!m) continue;
    const [, kind, owner, name] = m;
    if (kind === "metric") {
      const display = toDisplayName(name);
      const candidates = owner !== "" ? [byId.get(mkElementId(owner))] : elements.map((_, i) => i);
      for (const idx of candidates) {
        if (idx === void 0) continue;
        const el = elements[idx];
        const metrics = el.metrics ?? [];
        const mi = metrics.findIndex((x) => owner !== "" ? x.id === mkMetricId(el.id, name) : x.name === display && String(x.id).includes("__m__"));
        if (mi < 0) continue;
        const d = buildDesc(metrics[mi].description, syns);
        if (d === void 0) continue;
        metrics[mi] = withDescription(metrics[mi], d);
        applied.add(key);
        break;
      }
    } else {
      const idx = byId.get(mkElementId(owner));
      if (idx === void 0) continue;
      const el = elements[idx];
      const cols = el.columns ?? [];
      const display = toDisplayName(name);
      let ci2 = cols.findIndex((c) => c.id === mkColumnId(el.id, name));
      if (ci2 < 0) ci2 = cols.findIndex((c) => c.name === display);
      if (ci2 < 0) continue;
      const d = buildDesc(cols[ci2].description, syns);
      if (d === void 0) continue;
      cols[ci2] = withDescription(cols[ci2], d);
      applied.add(key);
    }
  }
  return { spec: JSON.stringify(spec, null, 2), applied };
}
var SIMPLE_IDENT = /^(?:(\w+|"[^"]+")\s*\.\s*)?(\w+|"[^"]+")$/;
function bareColumn(expr, alias) {
  if (typeof expr !== "string") return void 0;
  const m = SIMPLE_IDENT.exec(expr.trim());
  if (!m) return void 0;
  const unq = (x) => x.startsWith('"') ? x.slice(1, -1) : x.toUpperCase();
  if (m[1] !== void 0 && unq(m[1]).toUpperCase() !== alias.toUpperCase()) return void 0;
  return unq(m[2]);
}
function yamlFacts(source) {
  const out = /* @__PURE__ */ new Map();
  const doc = parse(source);
  if (typeof doc !== "object" || doc === null) return out;
  for (const table of rec(doc.tables)) {
    const alias = String(table.name);
    const list2 = [];
    for (const o of [...rec(table.dimensions), ...rec(table.time_dimensions), ...rec(table.facts)]) {
      const phys = bareColumn(o.expr, alias);
      list2.push({
        name: String(o.name),
        ...phys !== void 0 && { phys },
        ...typeof o.data_type === "string" && { dataType: o.data_type },
        ...Array.isArray(o.sample_values) && { samples: strArray(o.sample_values) }
      });
    }
    out.set(alias, list2);
  }
  return out;
}
function ddlSampleValuesIn(slice) {
  return ddlQuotedList(slice, /\bSAMPLE_VALUES\s*\(/i);
}
function ddlFacts(statement) {
  const out = /* @__PURE__ */ new Map();
  const parsed = parseSemanticView(mkSource(statement));
  if (!parsed.ok) return out;
  for (const o of [...parsed.view.facts, ...parsed.view.dimensions]) {
    if (o.owner === null) continue;
    const alias = o.owner.name;
    const phys = bareColumn(o.expression, alias);
    const samples = ddlSampleValuesIn(statement.slice(o.span.start, o.span.end));
    const list2 = out.get(alias) ?? [];
    list2.push({ name: o.name.name, ...phys !== void 0 && { phys }, ...samples !== void 0 && { samples } });
    out.set(alias, list2);
  }
  return out;
}
function formatFacts(statement, format) {
  return format === "snowflake-semantic-view-yaml" ? yamlFacts(statement) : ddlFacts(statement);
}
function withoutUnknownLine(f, format) {
  const unknown2 = f.source === "embed" || format === "snowflake-semantic-view-yaml" && f.code !== "YAML_PARSE_FAILURE";
  if (!unknown2 || f.line === void 0) return f;
  const at = ` at line ${f.line}`;
  const { line: _line, ...rest } = f;
  return {
    ...rest,
    message: rest.message.split(at).join(""),
    ...rest.remediation !== void 0 && { remediation: rest.remediation.split(at).join("") }
  };
}
function convertSemanticView(source, opts) {
  if (opts === void 0 || opts === null || typeof opts.connectionId !== "string" || opts.connectionId.trim() === "") {
    throw new Error("convertSemanticView requires a non-empty connectionId (pass a placeholder when offline)");
  }
  const text = typeof source === "string" ? source : "";
  const format = opts.format !== void 0 && opts.format in TRIPLES ? opts.format : opts.format !== void 0 ? void 0 : detectFormatLenient(text);
  if (format === void 0) {
    return {
      engine: ENGINE,
      format: void 0,
      repairedCount: 0,
      results: [
        failureResult(
          "",
          opts.format !== void 0 ? `Unsupported format "${String(opts.format)}"` : 'Could not detect the input format: expected a CREATE SEMANTIC VIEW statement or a semantic view YAML (top-level "name:").'
        )
      ]
    };
  }
  const triple = TRIPLES[format];
  let repairedCount = 0;
  let statements;
  try {
    const prepared = triple.prepareSource(text);
    repairedCount = prepared.repairedCount;
    statements = triple.splitStatements(prepared.source);
    if (statements.length === 0) statements = [prepared.source];
  } catch (e) {
    return { engine: ENGINE, format, repairedCount, results: [failureResult("", `Could not read input: ${errMsg(e)}`)] };
  }
  const columnTypesSkipped = opts.columnTypes === void 0;
  const columnTypes = opts.columnTypes ?? {};
  const dbGiven = opts.database !== void 0 && opts.database.trim() !== "";
  const schemaGiven = opts.schema !== void 0 && opts.schema.trim() !== "";
  const options = {
    connectionId: opts.connectionId,
    ...opts.database !== void 0 && { database: opts.database },
    ...opts.schema !== void 0 && { schema: opts.schema },
    columnTypes,
    ...opts.bestEffort && { bestEffort: true }
  };
  const results = statements.map((statement, index) => {
    const fallbackName = `statement-${index + 1}`;
    try {
      const read2 = triple.reader.read(statement);
      if (!read2.ok) {
        return mkResult({
          viewName: fallbackName,
          ok: false,
          warnings: [],
          errors: sortFindings(read2.errors),
          overriddenErrors: [],
          bestEffortWouldConvert: false
        });
      }
      const ir = read2.ir;
      const viewName = ir.name.parts.at(-1)?.name ?? fallbackName;
      const refs = unqualifiedReferences(ir);
      const needDb = dbGiven ? [] : refs.database;
      const needSchema = schemaGiven ? [] : refs.schema;
      if (needDb.length > 0 || needSchema.length > 0) {
        const needs = { database: needDb, schema: needSchema };
        const needsResult = mkResult({
          viewName,
          ok: false,
          warnings: [],
          errors: [],
          overriddenErrors: [],
          bestEffortWouldConvert: false,
          needs
        });
        needsResult.findings.push(...needsFindings(viewName, needs));
        return needsResult;
      }
      const result = SIGMA_DATA_MODEL_WRITER.write(ir, SNOWFLAKE_TO_SIGMA_TRANSLATOR, options);
      if (columnTypesSkipped) result.warnings.push(mkFinding("COLUMN_TYPES_SKIPPED", OBJ_NONE, 1));
      if (repairedCount > 0) {
        result.warnings.push(mkFinding("YAML_QUOTE_REPAIR_APPLIED", OBJ_NONE, 1, { count: repairedCount }));
      }
      if (!result.ok) {
        return mkResult({
          viewName,
          ok: false,
          warnings: result.warnings,
          errors: result.errors,
          overriddenErrors: [],
          bestEffortWouldConvert: result.bestEffortWouldConvert
        });
      }
      let spec = result.spec;
      let warnings = result.warnings;
      if (opts.synonymsInDescription === true) {
        const syn = format === "snowflake-semantic-view-yaml" ? yamlSynonyms(statement) : ddlSynonyms(statement);
        const { spec: next, applied } = applySynonyms(spec, syn);
        spec = next;
        warnings = warnings.filter(
          (w) => !(w.code === "SYNONYMS_DROPPED" && applied.has(w.object.kind === "table" ? `table:${w.object.name}` : objKey(w.object.kind, w.object.owner, w.object.name)))
        );
      }
      let extra = [];
      if (opts.inferNumberFormats === true) {
        const facts = formatFacts(statement, format);
        const inferred = inferFormats(spec, { facts, columnTypes: opts.columnTypes, elementIdOf: mkElementId }, result.viewName);
        spec = inferred.spec;
        extra = inferred.findings;
      }
      if (opts.crossTableReferences !== "engine") {
        const moved = applyCrossTableReferences(spec, result.viewName);
        spec = moved.spec;
        extra = [...extra, ...moved.findings];
      }
      if (opts.bestEffort === true && opts.usingPaths !== "engine") {
        const pinned = result.overriddenErrors.filter((f) => f.code === "METRIC_JOIN_PATH_SELECTOR" && f.object.kind === "metric").map((f) => ({
          owner: f.object.owner,
          name: f.object.name,
          using: (f.details?.relationships ?? []).slice()
        }));
        if (pinned.length > 0) {
          const bound = applyUsingPaths(spec, {
            viewName: result.viewName,
            pinned,
            relationships: ir.relationships.map((r) => ({
              name: r.name?.name ?? `${r.sourceAlias.name}_TO_${r.targetAlias.name}`,
              sourceAlias: r.sourceAlias.name,
              targetAlias: r.targetAlias.name,
              sourceColumns: r.sourceColumns.map((c) => c.name)
            }))
          });
          spec = bound.spec;
          extra = [...extra, ...bound.findings];
        }
      }
      if (opts.multiFactWarning !== false) {
        extra = [...extra, ...applyMultiFact(spec, result.viewName, toEmbedFindings(warnings)).findings];
      }
      const okResult = mkResult({
        viewName: result.viewName,
        ok: true,
        spec,
        warnings,
        errors: [],
        overriddenErrors: result.overriddenErrors,
        bestEffortWouldConvert: false,
        tablesForColumnTypes: physicalTablesOf(result.spec)
      });
      okResult.findings.push(...extra);
      return okResult;
    } catch (e) {
      return failureResult(fallbackName, `Internal error converting statement ${index + 1}: ${errMsg(e)}`);
    }
  });
  if (format === "snowflake-semantic-view-yaml") {
    results.forEach((r, i) => {
      const f = unrecognizedYamlKeyFinding(statements[i], r.viewName);
      if (f !== void 0) r.findings.push(f);
      r.findings.push(...joinTypeInnerFindings(statements[i]));
    });
  }
  for (const r of results) {
    r.findings = r.findings.map((f) => withoutUnknownLine(f, format));
    if (format === "snowflake-semantic-view-yaml") {
      const strip = (list2) => list2.map((f) => {
        if (f.code === "YAML_PARSE_FAILURE" || f.line === void 0) return f;
        const { line: _line, ...rest } = f;
        return rest;
      });
      r.errors = strip(r.errors);
      r.warnings = strip(r.warnings);
      r.overriddenErrors = strip(r.overriddenErrors);
    }
  }
  return { engine: ENGINE, format, repairedCount, results };
}
export {
  ENGINE,
  applyCrossTableReferences,
  applyMultiFact,
  applyUsingPaths,
  classifyBase,
  convertSemanticView,
  detectFormat,
  formatFacts,
  inferFormats,
  mkElementId,
  nameTokens,
  parseFormula,
  rawConvert,
  relDisplaySegment,
  sampleStats,
  typeKind,
  unitOfFormula
};
