export type TextSource = string | Iterable<string> | AsyncIterable<string>;
export interface SourceRecord {
  row: number;
  record: Record<string, unknown>;
}
export interface CsvOptions {
  delimiter?: string;
  maxFieldChars?: number;
  maxRowChars?: number;
  maxColumns?: number;
  maxRows?: number;
  signal?: AbortSignal;
}
export interface CsvRecordOptions extends CsvOptions {
  headers?: readonly string[];
  skipEmpty?: boolean;
}
export interface JsonLineOptions {
  maxLineChars?: number;
  maxRows?: number;
  signal?: AbortSignal;
}
export class ImportParseError extends Error {
  constructor(
    message: string,
    readonly row: number,
    readonly column: number,
  ) {
    super(`Row ${row}, column ${column}: ${message}`);
    this.name = 'ImportParseError';
  }
}
export function positive(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 1)
    throw new RangeError(`${name} must be a positive safe integer.`);
  return value;
}
export function isRecord(value: unknown): value is Record<string, unknown> {
  return (
    value !== null &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null)
  );
}
async function* chunks(source: TextSource, signal?: AbortSignal): AsyncGenerator<string> {
  for await (const chunk of typeof source === 'string' ? [source] : source) {
    signal?.throwIfAborted();
    if (typeof chunk !== 'string')
      throw new TypeError('Text sources must yield decoded string chunks.');
    yield chunk;
  }
  signal?.throwIfAborted();
}
export async function* csvRows(
  source: TextSource,
  options: CsvOptions = {},
): AsyncGenerator<string[]> {
  const delimiter = options.delimiter ?? ',';
  if (typeof delimiter !== 'string' || delimiter.length !== 1 || /["\r\n\uFEFF]/.test(delimiter))
    throw new TypeError('delimiter must be one non-quote, non-newline UTF-16 character.');
  const maxField = positive(options.maxFieldChars ?? 1000000, 'maxFieldChars');
  const maxRow = positive(options.maxRowChars ?? 2000000, 'maxRowChars');
  const maxColumns = positive(options.maxColumns ?? 10000, 'maxColumns');
  const maxRows = positive(options.maxRows ?? 1000000, 'maxRows');
  let state: 'start' | 'plain' | 'quoted' | 'closed' = 'start';
  let row: string[] = [],
    field = '',
    touched = false,
    skipLf = false,
    first = true,
    size = 0,
    number = 1;
  const fail = (message: string): never => {
    throw new ImportParseError(message, number, row.length + 1);
  };
  const append = (character: string): void => {
    field += character;
    if (field.length > maxField) fail('Field character limit exceeded.');
  };
  const finishField = (): void => {
    if (row.length >= maxColumns) fail('Column limit exceeded.');
    row.push(field);
    field = '';
    state = 'start';
  };
  const finishRow = (): string[] => {
    if (number > maxRows) fail('Row limit exceeded.');
    finishField();
    const result = row;
    row = [];
    touched = false;
    size = 0;
    number++;
    return result;
  };
  for await (const chunk of chunks(source, options.signal)) {
    for (let index = 0; index < chunk.length; index++) {
      const character = chunk[index]!;
      if (first) {
        first = false;
        if (character === '\uFEFF') continue;
      }
      if (skipLf) {
        skipLf = false;
        if (character === '\n') continue;
      }
      if (++size > maxRow) fail('Row character limit exceeded.');
      if (state === 'quoted') {
        touched = true;
        if (character === '"') state = 'closed';
        else append(character);
      } else if (state === 'closed' && character === '"') {
        append('"');
        state = 'quoted';
      } else if (character === delimiter) {
        touched = true;
        finishField();
      } else if (character === '\r' || character === '\n') {
        skipLf = character === '\r';
        yield finishRow();
        options.signal?.throwIfAborted();
      } else if (state === 'closed') fail('Unexpected character after a closing quote.');
      else if (character === '"') {
        if (state !== 'start') fail('Quote inside an unquoted field.');
        touched = true;
        state = 'quoted';
      } else {
        touched = true;
        append(character);
        state = 'plain';
      }
    }
  }
  if (state === 'quoted') fail('Unterminated quoted field.');
  if (touched || field || row.length) yield finishRow();
}
function headerNames(headers: readonly string[]): string[] {
  if (
    !Array.isArray(headers) ||
    !headers.length ||
    headers.some((header) => typeof header !== 'string' || !header) ||
    new Set(headers).size !== headers.length
  ) {
    throw new TypeError('CSV headers must be nonempty unique strings.');
  }
  return [...headers];
}
export async function* csvRecords(
  source: TextSource,
  options: CsvRecordOptions = {},
): AsyncGenerator<SourceRecord> {
  let headers = options.headers === undefined ? undefined : headerNames(options.headers);
  const skipEmpty = options.skipEmpty ?? true;
  if (typeof skipEmpty !== 'boolean') throw new TypeError('skipEmpty must be boolean.');
  let number = 0;
  for await (const values of csvRows(source, options)) {
    number++;
    if (skipEmpty && values.length === 1 && values[0] === '') continue;
    if (!headers) {
      headers = headerNames(values);
      continue;
    }
    if (values.length !== headers.length)
      throw new ImportParseError(
        `Expected ${headers.length} fields, received ${values.length}.`,
        number,
        values.length,
      );
    yield {
      row: number,
      record: Object.fromEntries(headers.map((header, index) => [header, values[index]!])),
    };
  }
}
export async function* jsonLines(
  source: TextSource,
  options: JsonLineOptions = {},
): AsyncGenerator<SourceRecord> {
  const maxLine = positive(options.maxLineChars ?? 2000000, 'maxLineChars');
  const maxRows = positive(options.maxRows ?? 1000000, 'maxRows');
  let buffer = '',
    row = 1,
    skipLf = false,
    first = true;
  const parse = (): SourceRecord | undefined => {
    if (row > maxRows) throw new ImportParseError('Line limit exceeded.', row, 1);
    if (!buffer.trim()) return;
    let value: unknown;
    try {
      value = JSON.parse(buffer);
    } catch (error) {
      if (!(error instanceof SyntaxError)) throw error;
      throw new ImportParseError('Invalid JSON object.', row, 1);
    }
    if (!isRecord(value)) throw new ImportParseError('Each JSON line must be an object.', row, 1);
    return { row, record: value };
  };
  for await (const chunk of chunks(source, options.signal)) {
    for (let index = 0; index < chunk.length; index++) {
      const character = chunk[index]!;
      if (first) {
        first = false;
        if (character === '\uFEFF') continue;
      }
      if (skipLf) {
        skipLf = false;
        if (character === '\n') continue;
      }
      if (character === '\r' || character === '\n') {
        const value = parse();
        buffer = '';
        row++;
        skipLf = character === '\r';
        if (value) yield value;
        options.signal?.throwIfAborted();
      } else {
        buffer += character;
        if (buffer.length > maxLine)
          throw new ImportParseError('Line character limit exceeded.', row, 1);
      }
    }
  }
  if (buffer) {
    const value = parse();
    if (value) yield value;
  }
}
