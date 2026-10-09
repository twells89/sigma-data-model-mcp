/**
 * Evidence-based number-format inference for the opt-in `inferNumberFormats` (Snowflake path only).
 *
 * The legacy guesser (src/sigma-ids.ts inferSigmaFormat) looks only at a name and a formula's text, so it formats
 * `Average Discount = Avg([Discount])` as currency. This module instead classifies each BASE column from
 * (1) its declared type, (2) its sample values and (3) word-level name tokens, then propagates a UNIT through the
 * Sigma formula a metric / calculated column uses. Ambiguity yields NO format, never a wrong one.
 *
 * Units: currency | fraction (a 0-1 rate) | percentpoints (a rate already x100, e.g. 12.5 = 12.5%) | integer | plain (numeric, no confident unit) | scalar (a literal)
 *        | id | nonnumeric | unknown.
 * Must stay browser-safe.
 */
import { mkColumnId } from '@semantic-tools/sigma-data-model';

import { inferSigmaFormat } from '../../src/sigma-ids.js';

type Json = Record<string, any>;

export type Unit = 'currency' | 'fraction' | 'percentpoints' | 'integer' | 'plain' | 'scalar' | 'id' | 'nonnumeric' | 'unknown';

export interface U {
  u: Unit;
  /** Literal value (scalar only). */
  v?: number;
  /** Why this unit (short, human-readable). */
  why: string;
}

const mk = (u: Unit, why: string, v?: number): U => ({ u, why, ...(v !== undefined && { v }) });
const unknown = (why: string): U => mk('unknown', why);

// ── Source facts (re-read from the YAML / DDL text by index.ts; the IR carries neither type nor samples) ──

export interface ColFact {
  /** The dimension/fact name as written. */
  name: string;
  /** Underlying warehouse column when the expression is a bare column (UPPER), else undefined. */
  phys?: string;
  dataType?: string;
  samples?: string[];
}
/** table alias (as in the IR) -> its dimensions/facts */
export type SourceFacts = Map<string, ColFact[]>;

// ── Type / sample / name evidence ──

type TypeKind = 'int' | 'dec' | 'num' | 'other';

/** int = scale-0 numeric; dec = fractional numeric; num = numeric of unknown scale; other = non-numeric. */
export function typeKind(t: string | undefined): TypeKind | undefined {
  if (t === undefined) return undefined;
  const s = t.trim().toUpperCase();
  if (s === '') return undefined;
  const base = s.replace(/\(.*$/s, '').trim();
  if (/^(INT|INTEGER|BIGINT|SMALLINT|TINYINT|BYTEINT)$/.test(base)) return 'int';
  if (/^(NUMBER|DECIMAL|NUMERIC|DEC)$/.test(base)) {
    const m = /\(\s*\d+\s*(?:,\s*(\d+)\s*)?\)/.exec(s);
    if (!m) return 'num';
    return (m[1] ?? '0') === '0' ? 'int' : 'dec';
  }
  if (/^(FLOAT|FLOAT4|FLOAT8|DOUBLE|DOUBLE PRECISION|REAL|DECFLOAT)$/.test(base)) return 'dec';
  if (/^(FIXED)$/.test(base)) return 'num';
  return 'other';
}

const NUM_RE = /^[-+]?(?:\d+\.?\d*|\.\d+)(?:e[-+]?\d+)?$/i;

interface SampleStats { numeric: boolean; min: number; max: number; allInt: boolean }

export function sampleStats(samples: readonly string[] | undefined): SampleStats | undefined {
  if (samples === undefined) return undefined;
  const vals = samples.map(s => String(s).trim()).filter(s => s !== '');
  if (vals.length === 0) return undefined;
  if (!vals.every(v => NUM_RE.test(v))) return { numeric: false, min: NaN, max: NaN, allInt: false };
  const n = vals.map(Number);
  return { numeric: true, min: Math.min(...n), max: Math.max(...n), allInt: n.every(Number.isInteger) };
}

const CURRENCY = new Set('amount amt price cost revenue sales balance acctbal totalprice extendedprice supplycost spend fee fees tax profit income payment salary charge'.split(' '));
const PERCENT = new Set('rate pct percent percentage ratio share'.split(' '));
/** Words that mean "already x100" when the samples sit in (1, 100]; a bare "rate"/"ratio"/"share" and the ambiguous words stay plain
 *  (hourly_rate 15/25.5/45 and shipping_rate 4.99/9.99 are prices, not percent points). */
const PERCENT_POINTS = new Set('pct percent percentage'.split(' '));
/** Tokens that make a bare "rate" a price/charge rather than a percentage. */
const CURRENCYISH = new Set('hourly shipping fee fees price cost wage pay salary'.split(' '));
const AMBIGUOUS = new Set(['discount', 'margin']);
const COUNT = new Set(['count', 'qty', 'quantity', 'units']);
const IDENT = new Set('key id ids code zip phone year month day identifier uuid guid sk fk pk'.split(' '));

/** Lower-case word tokens (split on non-alphanumerics and camelCase), plus a de-pluralised twin of each. */
export function nameTokens(names: readonly string[]): Set<string> {
  const out = new Set<string>();
  for (const n of names) {
    const words = n.replace(/([a-z0-9])([A-Z])/g, '$1 $2').split(/[^A-Za-z0-9]+/).filter(Boolean).map(w => w.toLowerCase());
    for (const w of words) {
      out.add(w);
      if (w.length > 3 && w.endsWith('s') && !w.endsWith('ss')) out.add(w.slice(0, -1));
    }
  }
  return out;
}

const hasAny = (tk: Set<string>, words: Set<string>): string | undefined => [...tk].find(w => words.has(w));

export interface BaseEvidence {
  names: string[];
  dataType?: string;
  samples?: string[];
}

/** Classify one base (warehouse) column. */
export function classifyBase(ev: BaseEvidence): U {
  const kind = typeKind(ev.dataType);
  const stats = sampleStats(ev.samples);
  const shown = ev.names.find(Boolean) ?? 'column';
  if (kind === 'other') return mk('nonnumeric', `${shown} is ${ev.dataType}, not numeric`);
  if (stats !== undefined && !stats.numeric) return mk('nonnumeric', `${shown} has non-numeric sample values`);

  const tk = nameTokens(ev.names);
  const idWord = hasAny(tk, IDENT) ?? [...tk].find(w => w.length > 3 && w.endsWith('key'))
    ?? (tk.has('number') && !tk.has('of') && kind !== 'dec' ? 'number' : undefined);
  if (idWord !== undefined) return mk('id', `${shown} looks like an identifier (word "${idWord}")`);

  if (kind === 'int') return mk('integer', `${shown} is ${ev.dataType} (integer)`);

  const typed = kind !== undefined; // dec | num
  const pct = hasAny(tk, PERCENT);
  const amb = hasAny(tk, AMBIGUOUS);
  const cur = hasAny(tk, CURRENCY);
  const cnt = hasAny(tk, COUNT) ?? (tk.has('number') && tk.has('of') ? 'number of' : undefined);
  const bareRate = pct !== undefined && hasAny(tk, PERCENT_POINTS) === undefined;
  const curish = bareRate ? hasAny(tk, CURRENCYISH) : undefined;
  const noEvidence = (what: string): U =>
    typed || stats !== undefined ? mk('plain', `${shown}: ${what}`) : unknown(`${shown}: ${what}; no data_type or sample_values`);

  if (curish !== undefined && !(stats !== undefined && stats.min >= -1 && stats.max <= 1)) {
    // "<hourly|shipping|fee|...> rate": an amount per unit, not a percentage. Currency only when a currency word says so.
    return cur !== undefined
      ? mk('currency', `${shown} is a "${pct}" beside currency word "${cur}"`)
      : noEvidence(`"${curish} ${pct}" is a price-like rate, not a percentage, and no currency word confirms an amount`);
  }
  if (pct !== undefined || (amb !== undefined && cur === undefined)) {
    const w = pct ?? amb!;
    if (stats !== undefined) {
      if (stats.min >= -1 && stats.max <= 1) return mk('fraction', `${shown} ("${w}") has sample values within [${stats.min}, ${stats.max}]`);
      const ppWord = hasAny(tk, PERCENT_POINTS);
      if (ppWord !== undefined && stats.max > 1 && stats.max <= 100 && stats.min >= -100) {
        return mk('percentpoints', `${shown} ("${ppWord}") has sample values in (1, 100], i.e. percent points`);
      }
      return mk('plain', `${shown} ("${w}") has sample values outside 0-1 (percent points or amounts)`);
    }
    return noEvidence(`"${w}" could be a rate or an amount and there are no sample_values to tell`);
  }
  if (amb !== undefined && cur !== undefined) {
    // e.g. discount_amount: the currency word disambiguates, unless the samples are all tiny fractions.
    if (stats !== undefined && stats.min >= -1 && stats.max <= 1) return mk('plain', `${shown} ("${amb}" + "${cur}") has samples within 0-1; rate or amount is unclear`);
    return mk('currency', `${shown} has currency word "${cur}"`);
  }
  if (cur !== undefined && cnt !== undefined) {
    return typed ? mk('plain', `${shown} mixes "${cur}" and "${cnt}"`) : unknown(`${shown} mixes "${cur}" and "${cnt}"; no data_type`);
  }
  if (cur !== undefined) return mk('currency', `${shown} has currency word "${cur}"`);
  if (cnt !== undefined) {
    if (typed) return stats?.allInt ? mk('integer', `${shown} has count word "${cnt}" and integer samples`) : mk('plain', `${shown} is a decimal with count word "${cnt}"`);
    if (stats !== undefined && !stats.allInt) return mk('plain', `${shown} has count word "${cnt}" but fractional samples`);
    return mk('integer', `${shown} has count word "${cnt}"`);
  }
  return noEvidence('no unit word in the name');
}

// ── Formula tokenizer / parser ──

type Node =
  | { k: 'num'; v: number }
  | { k: 'str'; v: string }
  | { k: 'ref'; path: string[] }
  | { k: 'id'; name: string }
  | { k: 'call'; name: string; args: Node[] }
  | { k: 'bin'; op: string; l: Node; r: Node }
  | { k: 'neg'; x: Node }
  | { k: 'not'; x: Node };

interface Tok { t: 'num' | 'str' | 'ref' | 'id' | 'op'; v: string }

function tokenize(src: string): Tok[] {
  const out: Tok[] = [];
  let i = 0;
  while (i < src.length) {
    const c = src[i]!;
    if (/\s/.test(c)) { i++; continue; }
    if (c === '[') {
      const j = src.indexOf(']', i + 1);
      if (j < 0) throw new Error('unterminated ref');
      out.push({ t: 'ref', v: src.slice(i + 1, j) });
      i = j + 1;
      continue;
    }
    if (c === '"' || c === "'") {
      let j = i + 1;
      for (;;) {
        if (j >= src.length) throw new Error('unterminated string');
        if (src[j] === '\\') { j += 2; continue; }
        if (src[j] === c) { if (src[j + 1] === c) { j += 2; continue; } break; }
        j++;
      }
      out.push({ t: 'str', v: src.slice(i + 1, j) });
      i = j + 1;
      continue;
    }
    const num = /^(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?/.exec(src.slice(i));
    if (num) { out.push({ t: 'num', v: num[0] }); i += num[0].length; continue; }
    const id = /^[A-Za-z_][A-Za-z0-9_]*/.exec(src.slice(i));
    if (id) { out.push({ t: 'id', v: id[0] }); i += id[0].length; continue; }
    const two = src.slice(i, i + 2);
    if (['<=', '>=', '<>', '!=', '==', '&&', '||'].includes(two)) { out.push({ t: 'op', v: two }); i += 2; continue; }
    if ('+-*/^%&(),<>=!'.includes(c)) { out.push({ t: 'op', v: c }); i++; continue; }
    throw new Error(`unexpected character ${c}`);
  }
  return out;
}

export function parseFormula(src: string): Node | undefined {
  try {
    const toks = tokenize(src);
    let p = 0;
    const peek = (): Tok | undefined => toks[p];
    const isOp = (v: string): boolean => peek()?.t === 'op' && peek()!.v === v;
    const isKw = (v: string): boolean => peek()?.t === 'id' && peek()!.v.toLowerCase() === v;
    const expect = (v: string): void => { if (!isOp(v)) throw new Error(`expected ${v}`); p++; };

    const parseOr = (): Node => {
      let l = parseAnd();
      while (isKw('or') || isOp('||')) { p++; l = { k: 'bin', op: 'or', l, r: parseAnd() }; }
      return l;
    };
    const parseAnd = (): Node => {
      let l = parseNot();
      while (isKw('and') || isOp('&&')) { p++; l = { k: 'bin', op: 'and', l, r: parseNot() }; }
      return l;
    };
    const parseNot = (): Node => {
      if (isKw('not') || isOp('!')) { p++; return { k: 'not', x: parseNot() }; }
      return parseCmp();
    };
    const CMP = ['=', '==', '!=', '<>', '<', '>', '<=', '>='];
    const parseCmp = (): Node => {
      let l = parseAdd();
      while (peek()?.t === 'op' && CMP.includes(peek()!.v)) { const op = peek()!.v; p++; l = { k: 'bin', op, l, r: parseAdd() }; }
      return l;
    };
    const parseAdd = (): Node => {
      let l = parseMul();
      while (isOp('+') || isOp('-') || isOp('&')) { const op = peek()!.v; p++; l = { k: 'bin', op, l, r: parseMul() }; }
      return l;
    };
    const parseMul = (): Node => {
      let l = parseUnary();
      while (isOp('*') || isOp('/') || isOp('%')) { const op = peek()!.v; p++; l = { k: 'bin', op, l, r: parseUnary() }; }
      return l;
    };
    const parseUnary = (): Node => {
      if (isOp('-')) { p++; return { k: 'neg', x: parseUnary() }; }
      if (isOp('+')) { p++; return parseUnary(); }
      const base = parsePrimary();
      if (isOp('^')) { p++; return { k: 'bin', op: '^', l: base, r: parseUnary() }; }
      return base;
    };
    const parsePrimary = (): Node => {
      const t = peek();
      if (t === undefined) throw new Error('unexpected end');
      if (t.t === 'num') { p++; return { k: 'num', v: Number(t.v) }; }
      if (t.t === 'str') { p++; return { k: 'str', v: t.v }; }
      if (t.t === 'ref') { p++; return { k: 'ref', path: t.v.split('/') }; }
      if (t.t === 'id') {
        p++;
        if (isOp('(')) {
          p++;
          const args: Node[] = [];
          if (!isOp(')')) {
            for (;;) {
              args.push(parseOr());
              if (isOp(',')) { p++; continue; }
              break;
            }
          }
          expect(')');
          return { k: 'call', name: t.v, args };
        }
        return { k: 'id', name: t.v };
      }
      if (isOp('(')) { p++; const e = parseOr(); expect(')'); return e; }
      throw new Error(`unexpected ${t.v}`);
    };

    const root = parseOr();
    if (p !== toks.length) throw new Error('trailing tokens');
    return root;
  } catch {
    return undefined;
  }
}

// ── Unit algebra ──

const numeric = (a: U): boolean => a.u !== 'unknown' && a.u !== 'id' && a.u !== 'nonnumeric';
const isZero = (a: U): boolean => a.u === 'scalar' && a.v === 0;
const bad = (a: U, b: U, op: string): U => unknown(!numeric(a) ? a.why : !numeric(b) ? b.why : `${a.u} ${op} ${b.u} has no confident unit`);

export function addUnits(a: U, b: U, op: '+' | '-'): U {
  if (!numeric(a) || !numeric(b)) return bad(a, b, op);
  if (isZero(a)) return b;
  if (isZero(b)) return a;
  if (a.u === b.u) return a.u === 'scalar' ? mk('scalar', 'literal', a.v !== undefined && b.v !== undefined ? (op === '+' ? a.v + b.v : a.v - b.v) : undefined) : a;
  const [s, o] = a.u === 'scalar' ? [a, b] : b.u === 'scalar' ? [b, a] : [undefined, undefined];
  if (s !== undefined && o !== undefined) {
    if (o.u === 'fraction') return o;
    if (o.u === 'plain') return o;
    if (o.u === 'integer' && Number.isInteger(s.v)) return o;
  }
  return bad(a, b, op);
}

export function mulUnits(a: U, b: U): U {
  if (!numeric(a) || !numeric(b)) return bad(a, b, '*');
  if (a.u === 'scalar' && b.u === 'scalar') return mk('scalar', 'literal', a.v !== undefined && b.v !== undefined ? a.v * b.v : undefined);
  for (const [s, o] of [[a, b], [b, a]] as const) {
    if (s.u === 'scalar') {
      if (s.v === 100 && o.u === 'fraction') return mk('percentpoints', 'a fraction times 100 is percent points');
      if (o.u === 'percentpoints') return unknown('scaling percent points has no confident unit');
      return o;
    }
  }
  if ((a.u === 'currency' && ['integer', 'plain', 'fraction'].includes(b.u)) || (b.u === 'currency' && ['integer', 'plain', 'fraction'].includes(a.u))) {
    return mk('currency', 'currency scaled by a number');
  }
  if (a.u === 'integer' && b.u === 'integer') return mk('integer', 'integer times integer');
  return bad(a, b, '*');
}

/** Words that make an integer/integer or plain/plain quotient a ratio ("Win Rate", "Order Share"). */
const RATIO_NAME = new Set('rate pct percent percentage ratio share'.split(' '));
export const hasRatioName = (name: string | undefined): boolean => name !== undefined && hasAny(nameTokens([name]), RATIO_NAME) !== undefined;

/**
 * `ratioNamed`: the item being typed has a percent-like name. currency / currency is always a ratio, but
 * integer / integer (Sum(Quantity) / CountDistinct(OrderId) = units per order) and plain / plain are a ratio ONLY then.
 */
export function divUnits(a: U, b: U, ratioNamed = false): U {
  if (!numeric(a) || !numeric(b)) return bad(a, b, '/');
  if (a.u === 'scalar' && b.u === 'scalar') return mk('scalar', 'literal', a.v !== undefined && b.v !== undefined && b.v !== 0 ? a.v / b.v : undefined);
  if (a.u === b.u && a.u === 'currency') return mk('fraction', 'currency divided by currency is a ratio');
  if (a.u === b.u && (a.u === 'integer' || a.u === 'plain')) {
    return ratioNamed ? mk('fraction', `${a.u} divided by ${b.u} and the name says ratio`) : mk('plain', `${a.u} divided by ${b.u} is an average or per-unit figure unless its name says rate/percent/ratio/share`);
  }
  if (b.u === 'scalar') return a.u === 'percentpoints' ? unknown('dividing percent points by a number has no confident unit') : a;
  if (a.u === 'currency' && (b.u === 'integer' || b.u === 'plain')) return mk('currency', 'currency per unit');
  return bad(a, b, '/');
}

/** Unify branch results: neutral zero/Null literals adapt; otherwise all units must agree. */
export function unifyUnits(list: U[]): U {
  const real = list.filter(x => !isZero(x));
  if (real.length === 0) return list[0] ?? unknown('no result branches');
  const first = real[0]!;
  if (!numeric(first)) return first.u === 'nonnumeric' && real.every(x => x.u === 'nonnumeric') ? first : unknown(first.why);
  for (const x of real) {
    if (x.u !== first.u) return unknown(`branches differ (${first.u} vs ${x.u})`);
  }
  return first;
}

const COUNT_FNS = new Set(['count', 'countdistinct', 'countif', 'countdistinctif']);
const SAME_FNS = new Set(['sum', 'avg', 'min', 'max', 'median', 'first', 'last', 'sumif', 'avgif', 'minif', 'maxif']);
const PASS_FNS = new Set(['round', 'abs', 'zn', 'floor', 'ceiling']);

export type RefResolver = (path: string[]) => U;

export function evalNode(n: Node, ref: RefResolver, ratioNamed = false): U {
  switch (n.k) {
    case 'num': return mk('scalar', 'literal', n.v);
    case 'str': return mk('nonnumeric', 'text literal');
    case 'ref': return ref(n.path);
    case 'id': {
      const l = n.name.toLowerCase();
      if (l === 'null') return mk('scalar', 'null', 0);
      return unknown(`${n.name} is not numeric`);
    }
    case 'neg': { const x = evalNode(n.x, ref, ratioNamed); return numeric(x) ? x : unknown(x.why); }
    case 'not': return unknown('logical');
    case 'bin': {
      const l = evalNode(n.l, ref, ratioNamed);
      const r = evalNode(n.r, ref, ratioNamed);
      switch (n.op) {
        case '+': case '-': return addUnits(l, r, n.op);
        case '*': return mulUnits(l, r);
        case '/': return divUnits(l, r, ratioNamed);
        default: return unknown(`operator ${n.op}`);
      }
    }
    case 'call': {
      const f = n.name.toLowerCase();
      const a = n.args;
      if (COUNT_FNS.has(f)) return mk('integer', `${n.name} counts rows`);
      if (SAME_FNS.has(f)) {
        if (a.length === 0) return unknown(`${n.name} has no argument`);
        const x = evalNode(a[0]!, ref, ratioNamed);
        if ((f === 'sum' || f === 'sumif') && x.u === 'percentpoints') return unknown('summing percent points is meaningless');
        if (f === 'sum' && x.u === 'scalar') return Number.isInteger(x.v) ? mk('integer', 'sum of an integer literal') : unknown('sum of a decimal literal');
        if (['min', 'max', 'first', 'last'].includes(f)) return x.u === 'scalar' ? unknown('literal aggregate') : x;
        if (!numeric(x) || x.u === 'scalar') return unknown(x.why);
        if ((f === 'avg' || f === 'median' || f === 'avgif') && x.u === 'integer') return mk('plain', `${n.name} of an integer is not an integer`);
        return x;
      }
      if (PASS_FNS.has(f)) return a.length > 0 ? evalNode(a[0]!, ref, ratioNamed) : unknown(`${n.name} has no argument`);
      if (f === 'nullif') return a.length > 0 ? evalNode(a[0]!, ref, ratioNamed) : unknown('NullIf has no argument');
      // The engine wraps Snowflake's DIV0 / DIV0NULL as CallNumber("DIV0NULL", n, d): a division.
      if (f === 'callnumber' && a.length === 3 && a[0]!.k === 'str' && /^div0(null)?$/i.test(a[0]!.v)) return divUnits(evalNode(a[1]!, ref, ratioNamed), evalNode(a[2]!, ref, ratioNamed), ratioNamed);
      if (f === 'coalesce') return unifyUnits(a.map(x => evalNode(x, ref, ratioNamed)));
      if (f === 'if') {
        const res: Node[] = [];
        for (let i = 1; i < a.length; i += 2) res.push(a[i]!);
        if (a.length % 2 === 1 && a.length > 1) res.push(a[a.length - 1]!);
        return res.length === 0 ? unknown('If has no result branch') : unifyUnits(res.map(x => evalNode(x, ref, ratioNamed)));
      }
      if (f === 'switch') {
        const res: Node[] = [];
        for (let i = 2; i < a.length; i += 2) res.push(a[i]!);
        if (a.length % 2 === 0 && a.length > 2) res.push(a[a.length - 1]!);
        return res.length === 0 ? unknown('Switch has no result branch') : unifyUnits(res.map(x => evalNode(x, ref, ratioNamed)));
      }
      return unknown(`function ${n.name} is not modelled`);
    }
  }
}

/** Unit of a formula given a name->unit lookup for its [refs] (keyed by the full ref text, e.g. "Discount"). Exposed for tests. */
export function unitOfFormula(formula: string, refs: Record<string, Unit | U>, itemName?: string): U {
  const ast = parseFormula(formula);
  if (ast === undefined) return unknown('formula could not be parsed');
  return evalNode(ast, path => {
    const hit = refs[path.join('/')] ?? refs[path[path.length - 1]!];
    if (hit === undefined) return unknown(`unresolved reference [${path.join('/')}]`);
    return typeof hit === 'string' ? mk(hit, 'given') : hit;
  }, hasRatioName(itemName));
}

// ── Formats ──

/** Exact shapes inferSigmaFormat (src/sigma-ids.ts) emits. */
export function formatFor(u: Unit): Json | undefined {
  switch (u) {
    case 'currency': return { kind: 'number', formatString: '$,.2f', currencySymbol: '$' };
    case 'integer': return { kind: 'number', formatString: ',.0f' };
    case 'fraction': return { kind: 'number', formatString: ',.2%' };
    // Same shape src/alteryx.ts and src/oac.ts emit for values already multiplied by 100.
    case 'percentpoints': return { kind: 'number', formatString: ',.2f', suffix: '%' };
    default: return undefined;
  }
}
const LABEL: Record<string, string> = { currency: 'currency', integer: 'integer', fraction: 'percent', percentpoints: 'percent points' };

// ── Spec walk ──

const PASSTHROUGH = /^\[[^\[\]/]+\/[^\[\]]+\]$/;
const PASSTHROUGH2 = /^\[[^\[\]/]+\/[^\[\]/]+\]$/;
const PASSTHROUGH_PARTS = /^\[([^\[\]/]+)\/([^\[\]]+)\]$/;

export interface InferOptions {
  facts: SourceFacts;
  /** `{ 'DB.SCHEMA.TABLE': { COLUMN: 'TYPE' } }` as supplied to the converter. */
  columnTypes?: Record<string, Record<string, string>>;
  /** element id of each table alias in `facts` (index.ts passes mkElementId). */
  elementIdOf: (alias: string) => string;
}

interface ElInfo {
  el: Json;
  byName: Map<string, Json>;
  byId: Map<string, Json>;
  tableNames: Set<string>;
  facts: ColFact[];
}

function pretty(formula: string): string {
  let s = formula.replace(/\[([^\]]*)\]/g, (_m, inner: string) => inner.split('/').pop()!).replace(/\s*\*\s*/g, ' × ').replace(/\s+\/\s+/g, ' ÷ ').replace(/\s+-\s+/g, ' − ');
  const m = /^([A-Za-z]+)\((.*)\)$/s.exec(s);
  if (m) {
    let depth = 0;
    let ok = true;
    for (const ch of m[2]!) { if (ch === '(') depth++; else if (ch === ')' && --depth < 0) { ok = false; break; } }
    if (ok && depth === 0) s = `${m[1]} of ${m[2]}`;
  }
  return s;
}

export interface InferResult {
  spec: string;
  findings: { code: string; severity: 'info'; message: string; remediation?: string; object: { kind: string; name: string }; source: 'embed' }[];
  /** Per formatted/unformatted item, for tooling and tests. */
  items: { owner: string; name: string; unit: Unit; format?: Json; legacy?: Json | null; why: string }[];
}

export function inferFormats(specText: string, opts: InferOptions, viewName: string): InferResult {
  const spec = JSON.parse(specText) as Json;
  const elements: Json[] = spec.pages?.[0]?.elements ?? [];
  const infos = new Map<string, ElInfo>();
  const factsByEl = new Map<string, ColFact[]>();
  for (const [alias, list] of opts.facts) factsByEl.set(opts.elementIdOf(alias), list);

  for (const el of elements) {
    const byName = new Map<string, Json>();
    const byId = new Map<string, Json>();
    for (const o of [...(el.columns ?? []), ...(el.metrics ?? [])]) {
      byId.set(o.id, o);
      const disp = typeof o.name === 'string' ? o.name : PASSTHROUGH_PARTS.exec(String(o.formula ?? ''))?.[2];
      if (disp !== undefined) { byName.set(disp, o); if (!byName.has(disp.toLowerCase())) byName.set(disp.toLowerCase(), o); }
    }
    const path: unknown = el.source?.path;
    const tableNames = new Set<string>([String(el.id).toLowerCase()]);
    if (Array.isArray(path) && path.length > 0) tableNames.add(String(path[path.length - 1]).toLowerCase());
    if (typeof el.name === 'string') tableNames.add(el.name.toLowerCase());
    infos.set(el.id, { el, byName, byId, tableNames, facts: factsByEl.get(el.id) ?? [] });
  }

  const colTypes = new Map<string, Map<string, string>>();
  for (const [k, cols] of Object.entries(opts.columnTypes ?? {})) {
    colTypes.set(k.toUpperCase(), new Map(Object.entries(cols ?? {}).map(([c, t]) => [c.toUpperCase(), String(t)])));
  }

  const memo = new Map<Json, U | 'visiting'>();

  const baseOf = (info: ElInfo, col: Json): U => {
    const tail = String(col.id).startsWith(`${info.el.id}__`) ? String(col.id).slice(String(info.el.id).length + 2) : String(col.id);
    const display = PASSTHROUGH_PARTS.exec(String(col.formula))?.[2];
    const matched = info.facts.filter(f => (f.phys !== undefined && mkColumnId(info.el.id, f.phys) === col.id));
    const names = new Set<string>([tail.toUpperCase()]);
    if (typeof col.name === 'string') names.add(col.name);
    if (display !== undefined) names.add(display);
    let dataType: string | undefined;
    let samples: string[] | undefined;
    for (const f of matched) {
      names.add(f.name);
      if (f.phys !== undefined) names.add(f.phys);
      dataType ??= f.dataType;
      if (f.samples !== undefined) samples = [...(samples ?? []), ...f.samples];
    }
    if (dataType === undefined) {
      const path: unknown = info.el.source?.path;
      const tbl = Array.isArray(path) ? colTypes.get(path.join('.').toUpperCase()) : undefined;
      const physGuess = matched.find(f => f.phys)?.phys ?? tail.toUpperCase();
      dataType = tbl?.get(physGuess) ?? (display !== undefined ? tbl?.get(display.toUpperCase().replace(/\s+/g, '_')) : undefined);
    }
    return classifyBase({ names: [...names], ...(dataType !== undefined && { dataType }), ...(samples !== undefined && { samples }) });
  };

  const unitOf = (info: ElInfo, o: Json): U => {
    const seen = memo.get(o);
    if (seen === 'visiting') return unknown('circular reference');
    if (seen !== undefined) return seen;
    memo.set(o, 'visiting');
    let r: U;
    const formula = typeof o.formula === 'string' ? o.formula : '';
    if (PASSTHROUGH2.test(formula) && (info.el.columns ?? []).includes(o)) r = baseOf(info, o);
    else {
      const ast = parseFormula(formula);
      r = ast === undefined ? unknown('formula could not be parsed') : evalNode(ast, path => resolve(info, path), hasRatioName(typeof o.name === 'string' ? o.name : undefined));
    }
    memo.set(o, r);
    return r;
  };

  const findEl = (info: ElInfo, name: string): ElInfo => {
    const l = name.toLowerCase();
    if (info.tableNames.has(l)) return info;
    for (const other of infos.values()) if (other.tableNames.has(l)) return other;
    return info;
  };
  const lookup = (info: ElInfo, name: string): Json | undefined => info.byName.get(name) ?? info.byName.get(name.toLowerCase()) ?? info.byId.get(name);

  const resolve = (info: ElInfo, path: string[]): U => {
    if (path.length === 1) {
      const o = lookup(info, path[0]!);
      return o ? unitOf(info, o) : unknown(`unresolved reference [${path[0]}]`);
    }
    if (path.length === 2) {
      const owner = findEl(info, path[0]!);
      const o = lookup(owner, path[1]!);
      return o ? unitOf(owner, o) : unknown(`unresolved reference [${path.join('/')}]`);
    }
    if (path.length === 3) {
      const owner = findEl(info, path[0]!);
      const rel = (owner.el.relationships ?? []).find((r: Json) => r.id === path[1] || r.name === path[1]);
      const target = rel ? infos.get(rel.targetElementId) : undefined;
      const o = target ? lookup(target, path[2]!) : undefined;
      return target && o ? unitOf(target, o) : unknown(`unresolved reference [${path.join('/')}]`);
    }
    return unknown(`unresolved reference [${path.join('/')}]`);
  };

  const items: InferResult['items'] = [];
  const inferred: string[] = [];
  const notInferred: string[] = [];

  const visit = (info: ElInfo, o: Json, isMetric: boolean): void => {
    if (o.format !== undefined || typeof o.formula !== 'string') return;
    if (!isMetric && PASSTHROUGH.test(o.formula)) return;
    const r = unitOf(info, o);
    const fmt = formatFor(r.u);
    const display = String(o.name ?? o.id);
    const legacy = inferSigmaFormat(o.formula, o.name);
    items.push({ owner: String(info.el.id), name: display, unit: r.u, ...(fmt && { format: fmt }), legacy, why: r.why });
    if (fmt) {
      o.format = fmt;
      inferred.push(`${display}: ${LABEL[r.u]} — ${pretty(o.formula)}`);
    } else if (legacy) {
      notInferred.push(`${display} (${r.why})`);
    }
  };
  for (const info of infos.values()) {
    for (const c of info.el.columns ?? []) visit(info, c, false);
    for (const m of info.el.metrics ?? []) visit(info, m, true);
  }

  const cap = (list: string[]): string => (list.length > 12 ? `${list.slice(0, 12).join('; ')}; and ${list.length - 12} more` : list.join('; '));
  const findings: InferResult['findings'] = [];
  if (inferred.length > 0) {
    findings.push({
      code: 'FORMATS_INFERRED',
      severity: 'info',
      message: `Inferred number formats for ${inferred.length} metric(s)/calculated column(s) from column types, sample values and formula structure: ${cap(inferred)}.`,
      remediation: 'Review these formats. Adding `data_type` and `sample_values` to the source columns (or passing column types) improves the result.',
      object: { kind: 'view', name: viewName },
      source: 'embed',
    });
  }
  if (notInferred.length > 0) {
    findings.push({
      code: 'FORMATS_NOT_INFERRED',
      severity: 'info',
      message: `${notInferred.length} metric(s)/calculated column(s) were left without a number format because the evidence did not confirm a unit (a name-only guess could be wrong, e.g. a rate shown as currency): ${cap(notInferred)}.`,
      remediation: 'Add `sample_values` (e.g. values between 0 and 1 mark a rate as a percentage) and/or `data_type` to the underlying columns, or pass column types, then convert again; or set the format in Sigma.',
      object: { kind: 'view', name: viewName },
      source: 'embed',
    });
  }
  return { spec: JSON.stringify(spec, null, 2), findings, items };
}
