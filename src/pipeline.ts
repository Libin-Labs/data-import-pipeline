import { isRecord, positive } from './parsers.js';
import type { SourceRecord } from './parsers.js';
export interface RowIssue {
  code: string;
  message: string;
  field?: string;
}
export interface ColumnRule {
  source?: string;
  required?: boolean;
  default?: unknown;
}
export interface RowContext {
  row: number;
  signal: AbortSignal | undefined;
}
export interface Checkpoint {
  version: 1;
  jobKey: string;
  afterRow: number;
  processed: number;
  accepted: number;
  rejected: number;
  seenKeys: string[];
}
export type ImportOutcome =
  | {
      status: 'accepted';
      row: number;
      original: Record<string, unknown>;
      value: Record<string, unknown>;
      issues: [];
    }
  | {
      status: 'rejected';
      row: number;
      original: Record<string, unknown>;
      value: Record<string, unknown>;
      issues: RowIssue[];
    };
export interface ImportOptions {
  jobKey: string;
  columns?: Readonly<Record<string, ColumnRule>>;
  transform?: (
    record: Record<string, unknown>,
    context: RowContext,
  ) => Record<string, unknown> | Promise<Record<string, unknown>>;
  validate?: (
    record: Readonly<Record<string, unknown>>,
    context: RowContext,
  ) => readonly RowIssue[] | Promise<readonly RowIssue[]>;
  key?: (record: Readonly<Record<string, unknown>>) => string;
  checkpoint?: Checkpoint;
  maxRows?: number;
  maxKeys?: number;
  signal?: AbortSignal;
}
export interface ImportSession {
  run(source: Iterable<SourceRecord> | AsyncIterable<SourceRecord>): AsyncGenerator<ImportOutcome>;
  checkpoint(): Checkpoint;
}
export class ImportRowError extends Error {
  constructor(
    message: string,
    readonly code = 'TRANSFORM',
    readonly field?: string,
  ) {
    super(message);
    this.name = 'ImportRowError';
  }
}
function checkedIssues(value: readonly RowIssue[]): RowIssue[] {
  if (
    !Array.isArray(value) ||
    value.some(
      (issue) =>
        !isRecord(issue) ||
        typeof issue.code !== 'string' ||
        typeof issue.message !== 'string' ||
        (issue.field !== undefined && typeof issue.field !== 'string'),
    )
  )
    throw new TypeError('Validation must return an array of row issues.');
  return value.map((issue) => ({ ...issue }));
}
export function createImport(options: ImportOptions): ImportSession {
  const { jobKey, signal, transform, validate, key } = options;
  if (typeof jobKey !== 'string' || !jobKey || jobKey.length > 1000)
    throw new TypeError('jobKey must contain 1 to 1000 characters.');
  const maxRows = positive(options.maxRows ?? 1000000, 'maxRows');
  const maxKeys = positive(options.maxKeys ?? 100000, 'maxKeys');
  for (const fn of [transform, validate, key])
    if (fn !== undefined && typeof fn !== 'function')
      throw new TypeError('Import callbacks must be functions.');
  if (options.columns !== undefined && !isRecord(options.columns))
    throw new TypeError('columns must be a plain map.');
  const columns =
    options.columns === undefined
      ? undefined
      : Object.entries(options.columns).map(([field, rule]) => {
          if (
            !isRecord(rule) ||
            (rule.source !== undefined && typeof rule.source !== 'string') ||
            (rule.required !== undefined && typeof rule.required !== 'boolean')
          )
            throw new TypeError(`Invalid column rule for ${field}.`);
          return {
            field,
            source: rule.source ?? field,
            required: rule.required ?? false,
            hasDefault: Object.hasOwn(rule, 'default'),
            fallback: rule.default,
          };
        });
  let afterRow = 0,
    processed = 0,
    accepted = 0,
    rejected = 0,
    running = false;
  const seen = new Set<string>();
  const saved = options.checkpoint;
  if (saved !== undefined) {
    if (
      !isRecord(saved) ||
      saved.version !== 1 ||
      saved.jobKey !== jobKey ||
      [saved.afterRow, saved.processed, saved.accepted, saved.rejected].some(
        (n) => !Number.isSafeInteger(n) || n < 0,
      ) ||
      saved.accepted + saved.rejected !== saved.processed ||
      saved.processed > saved.afterRow ||
      saved.processed > maxRows ||
      !Array.isArray(saved.seenKeys) ||
      saved.seenKeys.length > maxKeys ||
      saved.seenKeys.some((item) => typeof item !== 'string' || item.length > 1000) ||
      new Set(saved.seenKeys).size !== saved.seenKeys.length ||
      (key === undefined && saved.seenKeys.length !== 0) ||
      (key !== undefined && saved.seenKeys.length !== saved.accepted)
    )
      throw new TypeError('Invalid or incompatible import checkpoint.');
    ({ afterRow, processed, accepted, rejected } = saved);
    for (const item of saved.seenKeys) seen.add(item);
  }
  return {
    checkpoint() {
      return { version: 1, jobKey, afterRow, processed, accepted, rejected, seenKeys: [...seen] };
    },
    async *run(source) {
      if (running) throw new Error('An import session cannot run concurrently.');
      running = true;
      let previous = 0;
      try {
        signal?.throwIfAborted();
        for await (const entry of source) {
          signal?.throwIfAborted();
          if (
            !entry ||
            !Number.isSafeInteger(entry.row) ||
            entry.row <= previous ||
            !isRecord(entry.record)
          ) {
            throw new TypeError(
              'Source records require strictly increasing positive safe row numbers and plain records.',
            );
          }
          previous = entry.row;
          if (entry.row <= afterRow) continue;
          if (processed >= maxRows) throw new RangeError('Import row limit exceeded.');
          const original = { ...entry.record },
            context: RowContext = { row: entry.row, signal };
          let value: Record<string, unknown> = columns === undefined ? { ...original } : {};
          const issues: RowIssue[] = [];
          for (const column of columns ?? []) {
            const missing = !Object.hasOwn(original, column.source);
            const next =
              missing && column.hasDefault
                ? column.fallback
                : missing
                  ? undefined
                  : original[column.source];
            Object.defineProperty(value, column.field, {
              value: next,
              enumerable: true,
              writable: true,
              configurable: true,
            });
            if (column.required && (next === undefined || next === null || next === ''))
              issues.push({
                field: column.field,
                code: 'REQUIRED',
                message: 'A nonempty mapped value is required.',
              });
          }
          if (!issues.length) {
            try {
              if (transform) value = await transform(value, context);
              if (!isRecord(value)) throw new TypeError('transform must return a plain record.');
              if (validate) issues.push(...checkedIssues(await validate(value, context)));
            } catch (error) {
              if (!(error instanceof ImportRowError)) throw error;
              issues.push({
                code: error.code,
                message: error.message,
                ...(error.field === undefined ? {} : { field: error.field }),
              });
            }
          }
          signal?.throwIfAborted();
          let identity: string | undefined;
          if (!issues.length && key) {
            identity = key(value);
            if (typeof identity !== 'string' || identity.length > 1000)
              throw new TypeError('Duplicate keys must be strings of at most 1000 characters.');
            if (seen.has(identity))
              issues.push({
                code: 'DUPLICATE',
                message: 'A previously accepted row has the same key.',
              });
            else if (seen.size >= maxKeys) throw new RangeError('Duplicate-key capacity exceeded.');
          }
          if (issues.length) rejected++;
          else {
            accepted++;
            if (identity !== undefined) seen.add(identity);
          }
          afterRow = entry.row;
          processed++;
          if (issues.length) yield { status: 'rejected', row: entry.row, original, value, issues };
          else yield { status: 'accepted', row: entry.row, original, value, issues: [] };
        }
      } finally {
        running = false;
      }
    },
  };
}
function cell(value: string, safe: boolean): string {
  const protectedValue = safe && /^[\s]*[=+\-@\t\r\n]/u.test(value) ? `'${value}` : value;
  return `"${protectedValue.replace(/"/g, '""')}"`;
}
export async function* issueReportCsv(
  outcomes: Iterable<ImportOutcome> | AsyncIterable<ImportOutcome>,
  options: { protectFormulas?: boolean } = {},
): AsyncGenerator<string> {
  const safe = options.protectFormulas ?? true;
  if (typeof safe !== 'boolean') throw new TypeError('protectFormulas must be boolean.');
  yield '"row","field","code","message"\r\n';
  for await (const outcome of outcomes)
    for (const issue of outcome.issues) {
      yield [String(outcome.row), issue.field ?? '', issue.code, issue.message]
        .map((value) => cell(value, safe))
        .join(',') + '\r\n';
    }
}
