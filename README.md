# @lp-labs/data-import-pipeline

Streaming CSV/JSONL ingestion, column mapping, async transformation/validation, duplicate checks, row outcomes, resumable checkpoints and CSV issue exports. Zero runtime or peer dependencies. No sibling-package imports.

## Install

```sh
npm install @lp-labs/data-import-pipeline
```

Node 18+ or ES2022 browsers with async iterators and AbortController. ESM/CommonJS and declarations included.

## Complete example

```js
import { csvRecords, createImport } from '@lp-labs/data-import-pipeline';

const session = createImport({
  jobKey: 'customer-file-sha256-and-rule-version-1',
  columns: {
    email: { source: 'Email', required: true },
    name: { source: 'Name', required: true },
  },
  transform: (record) => ({ ...record, email: record.email.trim().toLowerCase() }),
  validate: (record) => record.email.includes('@') ? [] : [
    { field: 'email', code: 'EMAIL', message: 'Email must contain @' },
  ],
  key: (record) => record.email,
});
const csv = 'Email,Name\nADA@EXAMPLE.COM,Ada\nada@example.com,Duplicate\ninvalid,Other';
for await (const outcome of session.run(csvRecords(csv))) {
  if (outcome.status === 'accepted') {
    console.log('Import:', outcome.value);
    // Await your idempotent storage write here before saving the checkpoint.
  } else console.log('Review:', outcome.row, outcome.issues);
}
console.log(session.checkpoint().accepted); // 1
```

CommonJS: `const { csvRecords, createImport } = require('@lp-labs/data-import-pipeline');`.

## Streaming sources

`TextSource` accepts a string, `Iterable<string>` or `AsyncIterable<string>`. Use decoded string chunks, not Uint8Array/Buffer:

```js
import { createReadStream } from 'node:fs';
import { csvRecords } from '@lp-labs/data-import-pipeline';

const source = createReadStream('customers.csv', { encoding: 'utf8' });
for await (const entry of csvRecords(source)) {
  console.log(entry.row, entry.record);
}
```

In browsers, use `file.stream().pipeThrough(new TextDecoderStream())` when your platform supports async iteration on ReadableStream; otherwise provide a reader-backed async generator with `finally` cleanup. Decode bytes with a streaming decoder so multibyte characters crossing chunks remain intact.

## Parser APIs

### `csvRows(source, options?): AsyncGenerator<string[]>`

Incremental strict CSV parsing with quoted fields, doubled quote escaping, embedded newlines, CRLF/LF/CR records and arbitrary chunk boundaries. A leading BOM is removed once. Whitespace is preserved. Quotes inside unquoted fields and non-delimiter text after a closing quote throw.

| Option | Default | Meaning |
|---|---|---|
| `delimiter` | `","` | One UTF-16 character, excluding quote/newline/BOM |
| `maxFieldChars` | 1000000 | Maximum decoded field size |
| `maxRowChars` | 2000000 | Maximum raw row size, including separators/terminator |
| `maxColumns` | 10000 | Maximum fields per row |
| `maxRows` | 1000000 | Maximum logical rows, including header/empty rows |
| `signal` | none | AbortSignal checked during source/row iteration |

Limits are positive safe integers, measured in UTF-16 code units, not bytes. CRLF counts its CR terminator but not the skipped LF. Embedded newlines count as field content. Empty source emits nothing; a final newline does not create an extra row; empty lines produce `[""]`. No dialect guessing, comments or whitespace-after-quote mode.

### `csvRecords(source, options?): AsyncGenerator<SourceRecord>`

Options extend CSV options:

- `headers?: readonly string[]`: explicit names; otherwise first non-skipped row is the header.
- `skipEmpty?: boolean`: default true, skips rows containing one empty field (including quoted empty).

Headers are exact nonempty unique strings, not trimmed automatically. Every data row must match header width. `SourceRecord` is `{ row: number, record: Record<string, unknown> }`. CSV row numbers are one-based **logical source rows**, including header and skipped blank rows, not physical line numbers inside quoted fields. Explicit headers make the first source row data.

Header names such as `__proto__` are treated as own fields, not prototype setters. Header-only/empty files yield no data.

### `jsonLines(source, options?): AsyncGenerator<SourceRecord>`

Parse one plain JSON object per line. Supports CRLF/LF/CR, leading BOM and blank lines. Row labels retain one-based source line numbers. Arrays/primitives and malformed JSON throw.

Options: `maxLineChars` defaults 2000000; `maxRows` defaults 1000000 (physical lines, including blank lines); optional `signal`. Pretty-printed multiline JSON and whole-document JSON arrays are not supported. Use an application-provided SourceRecord generator for other formats.

`ImportParseError` contains `row`, `column` and a diagnostic message. CSV column means field position; JSONL uses column 1 rather than pretending to provide an exact parser offset. Parser failure terminates the stream; it is not a recoverable row-validation result.

## Import session API

### `createImport(options): ImportSession`

| Option | Default | Meaning |
|---|---|---|
| `jobKey` | required | Nonempty 1-1000-character identity for source AND processing configuration |
| `columns` | none | Optional output-field map: `{ source?, required?, default? }` |
| `transform(record, context)` | none | Sync/async conversion returning a plain record |
| `validate(record, context)` | none | Sync/async function returning `RowIssue[]` |
| `key(record)` | none | Synchronous duplicate identity string, at most 1000 characters |
| `checkpoint` | none | Resume a compatible saved Checkpoint |
| `maxRows` | 1000000 | Maximum cumulative processed outcomes |
| `maxKeys` | 100000 | Maximum distinct accepted keys retained |
| `signal` | none | AbortSignal passed to callbacks |

Column mapping projects only declared output fields. Source names are literal top-level fields, defaulting to the output name. Defaults apply to **absent own source fields**, not null/empty values. Required rules reject null/undefined/empty strings before transformation. Without columns, the record is shallow-copied.

Callbacks receive `RowContext` `{ row, signal }`. Validation issues are `{ code: string, message: string, field?: string }`. Invalid callback returns throw TypeError. A `key` is evaluated only after successful validation; duplicate accepted keys yield a rejected DUPLICATE issue. Rejected rows do not reserve identities. Capacity exhaustion throws rather than silently dropping duplicate protection.

Only expected `ImportRowError(message, code = 'TRANSFORM', field?)` failures become row issues. Other callback exceptions propagate without advancing the failing row's checkpoint. Never return success-shaped defaults for failed processing.

### `session.run(source): AsyncGenerator<ImportOutcome>`

Accepts iterable/async-iterable SourceRecord values with strictly increasing positive safe-integer row labels. Runs sequentially with pull-based backpressure; no hidden prefetch/concurrency. A second simultaneous run throws.

Each outcome contains:

- `status`: `"accepted"` or `"rejected"`.
- `row`, shallow `original` snapshot and mapped/transformed `value`.
- `issues`: empty array for accepted, nonempty for rejected.

Records may contain arbitrary domain values from custom sources; this is not a deep JSON validator. Nested objects, defaults and callback values are not cloned. Custom transformations must avoid mutating input references.

Breaking a `for await` loop closes the source iterator and releases the session for another run. This enables bounded previews, but previews advance this session's checkpoint. Use a fresh session to execute the full import after a preview.

## Checkpoints and recovery

`session.checkpoint()` returns a fresh `Checkpoint`:

```ts
interface Checkpoint {
  version: 1;
  jobKey: string;
  afterRow: number;
  processed: number;
  accepted: number;
  rejected: number;
  seenKeys: string[];
}
```

Resume with `createImport({ ...sameOptions, checkpoint })`, then reread the same source from its beginning or supply later row labels. Rows at/before afterRow are skipped. Duplicate keys are restored and counts remain cumulative.

Important guarantees and obligations:

1. `jobKey` must bind an immutable source identity and rule/validator/key version. No automatic hashing or config-equivalence proof is provided.
2. The checkpoint advances immediately before an outcome is yielded. **Persist it only after committing that outcome downstream.**
3. Use idempotent writes and preferably store checkpoint plus data in one transaction. This package alone does not provide exactly-once delivery or distributed commits.
4. Persist rejected-row decisions too if you do not want them replayed.
5. Capture checkpoints between outcomes, not during an in-flight callback. It represents the last yielded row.
6. This is row-based replay, not byte-offset seeking. Reopening CSV still parses skipped rows. Invalid syntax before the checkpoint still fails.
7. Checkpoints include exact accepted keys and may contain sensitive identifiers. Secure/redact storage appropriately.

Malformed versions, identity mismatch, inconsistent counts, duplicate keys and incompatible capacity/key-mode state throw explicitly. `maxRows` is cumulative across resumes. Checkpoint key copies cost O(keys); avoid copying them after every row for huge imports unless required by your durability design.

## Downloadable error reports

```js
import { issueReportCsv } from '@lp-labs/data-import-pipeline';
// rejectedOutcomes is an iterable/async iterable of ImportOutcome.
// Stream each returned string to your file writer or HTTP response:
// for await (const chunk of issueReportCsv(rejectedOutcomes)) await writer.write(chunk);
```

`issueReportCsv(outcomes, { protectFormulas = true }?)` emits a CSV header and one line per issue: row, field, code, message. All cells are quoted and embedded quotes escaped. Potential spreadsheet formula prefixes receive an apostrophe by default. This is a conservative mitigation, not a guarantee across every spreadsheet application. Setting false deliberately disables that protection.

The helper does not include raw input by default. It consumes outcomes: if you also need storage writes, perform those in a generator that yields the processed outcomes onward.

## Exports

Runtime: `csvRows`, `csvRecords`, `jsonLines`, `ImportParseError`, `createImport`, `issueReportCsv`, `ImportRowError`.

Types: `TextSource`, `SourceRecord`, `CsvOptions`, `CsvRecordOptions`, `JsonLineOptions`, `RowIssue`, `ColumnRule`, `RowContext`, `Checkpoint`, `ImportOutcome`, `ImportOptions`, `ImportSession`.

## Performance, cancellation and limits

- Parsers retain the current row/line plus source chunk; the session retains counters and optional accepted-key set, not prior payloads/outcomes.
- Memory is bounded by your chunk/row limits plus up to maxKeys identity strings. User callbacks, collecting results in arrays, or an already-materialized source can use additional memory.
- The regression suite processes 20000 generated rows and checks lazy one-row preview consumption. This is not a certified memory benchmark.
- Cancellation is checked between source chunks/rows and after async processing. It cannot interrupt a blocked source iterator or callback that ignores its signal; supply cancellable I/O.
- Supply parser signals as well as a session signal when parsing separately. Mid-row work on an already-received large chunk is synchronous.
- Errors from source iteration, parsing, cancellation, capacity checks or unexpected callbacks propagate. No broad catch converts them into successful imports.
- There is no XLSX/XML parser, encoding detector, database driver, upload transport, web worker pool, UI or implicit transaction manager.

This independently authored initial release intentionally provides explicit primitives rather than claiming every import format or storage guarantee.

## Support

If this library is useful to you, consider supporting its maintenance with a [donation via PayPal](https://www.paypal.me/LibinPrasanth).

## Author

Created by [Libin Prasanth](https://www.linkedin.com/in/libinprasanth/). Connect on LinkedIn for updates and discussion.
