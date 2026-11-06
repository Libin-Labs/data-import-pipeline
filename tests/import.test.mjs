import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import {
  csvRows,
  csvRecords,
  jsonLines,
  createImport,
  issueReportCsv,
  ImportRowError,
  ImportParseError,
} from '../dist/esm/index.js';
async function collect(source) {
  const rows = [];
  for await (const row of source) rows.push(row);
  return rows;
}

test('CSV supports quoted commas, escaped quotes, CRLF and embedded newlines', async () => {
  const text = '\uFEFFid,name,notes\r\n1,"Ada, A.","said ""hi""\r\nnext"\r\n2,Lin,\r\n';
  const expected = [
    ['id', 'name', 'notes'],
    ['1', 'Ada, A.', 'said "hi"\r\nnext'],
    ['2', 'Lin', ''],
  ];
  assert.deepEqual(await collect(csvRows(text)), expected);
  assert.deepEqual(await collect(csvRows([...text])), expected);
});
test('every CSV split boundary has identical parsing semantics', async () => {
  const text = 'a,b\r\n"x""y","line\nnext"\r\n"z",end';
  const expected = await collect(csvRows(text));
  for (let split = 0; split <= text.length; split++) {
    assert.deepEqual(await collect(csvRows([text.slice(0, split), text.slice(split)])), expected);
  }
});
test('generated quoted CSV round trips across small streamed chunks', async () => {
  const fields = ['', 'plain', ',', '"', 'line\nnext', '\r\n', '😀', ' spaced ', 'a""b'];
  const rows = Array.from({ length: 60 }, (_, row) =>
    Array.from({ length: 5 }, (_, column) => fields[(row * 3 + column) % fields.length]),
  );
  const encoded = rows
    .map((row) => row.map((field) => `"${field.replace(/"/g, '""')}"`).join(','))
    .join('\r\n');
  for (const size of [1, 2, 3, 7, 64]) {
    async function* chunks() {
      for (let offset = 0; offset < encoded.length; offset += size)
        yield encoded.slice(offset, offset + size);
    }
    assert.deepEqual(await collect(csvRows(chunks())), rows);
  }
});
test('CSV handles empty input, trailing fields, quoted empty and configurable delimiter', async () => {
  assert.deepEqual(await collect(csvRows('')), []);
  assert.deepEqual(await collect(csvRows('a,')), [['a', '']]);
  assert.deepEqual(await collect(csvRows('""')), [['']]);
  assert.deepEqual(await collect(csvRows('\n\n')), [[''], ['']]);
  assert.deepEqual(await collect(csvRows('a;b\n1;2', { delimiter: ';' })), [
    ['a', 'b'],
    ['1', '2'],
  ]);
});
test('CSV rejects malformed quoting and enforces exact limits', async () => {
  for (const source of ['"unterminated', 'un"quoted', '"closed" trailing'])
    await assert.rejects(collect(csvRows(source)), ImportParseError);
  assert.deepEqual(await collect(csvRows('abc', { maxFieldChars: 3, maxRowChars: 3 })), [['abc']]);
  await assert.rejects(collect(csvRows('abcd', { maxFieldChars: 3 })), /Field/);
  await assert.rejects(collect(csvRows('a,b', { maxRowChars: 2 })), /Row character/);
  await assert.rejects(collect(csvRows('a,b', { maxColumns: 1 })), /Column/);
  await assert.rejects(collect(csvRows('a\nb', { maxRows: 1 })), /Row limit/);
  await assert.rejects(collect(csvRows('a', { delimiter: '"' })), TypeError);
  await assert.rejects(collect(csvRows([new Uint8Array([1])])), /decoded/);
});
test('CSV records validate headers, width and preserve special names safely', async () => {
  const rows = await collect(csvRecords('id,__proto__\n1,x\n\n2,y'));
  assert.deepEqual(
    rows.map((entry) => entry.row),
    [2, 4],
  );
  assert.equal(Object.hasOwn(rows[0].record, '__proto__'), true);
  assert.equal(Object.getPrototypeOf(rows[0].record), Object.prototype);
  assert.deepEqual(await collect(csvRecords('1,Ada', { headers: ['id', 'name'] })), [
    { row: 1, record: { id: '1', name: 'Ada' } },
  ]);
  await assert.rejects(collect(csvRecords('a,a\n1,2')), /unique/);
  await assert.rejects(collect(csvRecords('a,b\n1')), /Expected 2/);
});
test('JSONL preserves source line numbers and handles arbitrary CRLF splits', async () => {
  const text = '\uFEFF{"id":1}\r\n\n{"id":2}\r{"id":3}';
  const expected = [
    { row: 1, record: { id: 1 } },
    { row: 3, record: { id: 2 } },
    { row: 4, record: { id: 3 } },
  ];
  assert.deepEqual(await collect(jsonLines([...text])), expected);
  await assert.rejects(collect(jsonLines('{"id":}')), /Invalid JSON/);
  await assert.rejects(collect(jsonLines('[]')), /must be an object/);
  await assert.rejects(collect(jsonLines('{"id":1}', { maxLineChars: 3 })), /Line character/);
  await assert.rejects(collect(jsonLines('{}\n{}', { maxRows: 1 })), /Line limit/);
});
test('pipeline mapping transforms validation and duplicates report per-row outcomes', async () => {
  const session = createImport({
    jobKey: 'customers-v1',
    columns: { id: { source: 'ID', required: true }, name: { source: 'Name', default: 'Unknown' } },
    transform: async (record) => ({ ...record, name: record.name.trim() }),
    validate: async (record) =>
      record.name ? [] : [{ field: 'name', code: 'EMPTY', message: 'Name required' }],
    key: (record) => record.id,
  });
  const results = await collect(session.run(csvRecords('ID,Name\n1,Ada\n1,Other\n2,\n,Missing')));
  assert.deepEqual(
    results.map((result) => result.status),
    ['accepted', 'rejected', 'rejected', 'rejected'],
  );
  assert.deepEqual(
    results.slice(1).map((result) => result.issues[0].code),
    ['DUPLICATE', 'EMPTY', 'REQUIRED'],
  );
  assert.equal(results[0].original.ID, '1');
  assert.equal(results[0].value.id, '1');
  assert.deepEqual(session.checkpoint(), {
    version: 1,
    jobKey: 'customers-v1',
    afterRow: 5,
    processed: 4,
    accepted: 1,
    rejected: 3,
    seenKeys: ['1'],
  });
});
test('checkpoint resume skips acknowledged source rows and restores duplicate tracking', async () => {
  const source = 'id\n1\n2\n1\n3';
  const options = { jobKey: 'stable-source-and-config-v1', key: (record) => record.id };
  const first = createImport(options);
  for await (const outcome of first.run(csvRecords(source))) {
    assert.equal(outcome.row, 2);
    break;
  }
  const checkpoint = first.checkpoint();
  const resumed = createImport({ ...options, checkpoint });
  const rest = await collect(resumed.run(csvRecords(source)));
  assert.deepEqual(
    rest.map((result) => result.status),
    ['accepted', 'rejected', 'accepted'],
  );
  assert.equal(resumed.checkpoint().processed, 4);
  checkpoint.seenKeys.push('external');
  assert.equal(resumed.checkpoint().seenKeys.length, 3);
  assert.throws(() => createImport({ ...options, jobKey: 'different', checkpoint }), /checkpoint/);
});
test('invalid checkpoint counts and modes are rejected before processing', () => {
  const saved = {
    version: 1,
    jobKey: 'job',
    afterRow: 1,
    processed: 1,
    accepted: 1,
    rejected: 0,
    seenKeys: ['a'],
  };
  for (const checkpoint of [
    { ...saved, version: 2 },
    { ...saved, accepted: 2 },
    { ...saved, afterRow: 0 },
    { ...saved, seenKeys: ['a', 'a'] },
    { ...saved, seenKeys: [] },
    { ...saved, afterRow: NaN },
  ])
    assert.throws(() => createImport({ jobKey: 'job', key: () => 'a', checkpoint }), /checkpoint/);
  assert.throws(() => createImport({ jobKey: 'job', checkpoint: saved }), /checkpoint/);
});
test('row failures are explicit while programming failures stop without advancing checkpoint', async () => {
  const session = createImport({
    jobKey: 'row-errors',
    transform(record) {
      if (record.id === 1) throw new ImportRowError('Invalid ID', 'BAD_ID', 'id');
      if (record.id === 2) throw new Error('programming failure');
      return record;
    },
  });
  const generator = session.run([
    { row: 1, record: { id: 1 } },
    { row: 2, record: { id: 2 } },
  ]);
  const result = await generator.next();
  assert.equal(result.value.issues[0].code, 'BAD_ID');
  await assert.rejects(generator.next(), /programming failure/);
  assert.equal(session.checkpoint().afterRow, 1);
});
test('abort closes source and does not commit interrupted async transformations', async () => {
  const controller = new AbortController();
  let closed = false;
  async function* source() {
    try {
      yield { row: 1, record: { id: 1 } };
      yield { row: 2, record: { id: 2 } };
    } finally {
      closed = true;
    }
  }
  const session = createImport({
    jobKey: 'abort',
    signal: controller.signal,
    transform: async (record) => {
      controller.abort(new Error('cancelled'));
      return record;
    },
  });
  await assert.rejects(collect(session.run(source())), /cancelled/);
  assert.equal(closed, true);
  assert.equal(session.checkpoint().processed, 0);
  await assert.rejects(collect(csvRows('a', { signal: controller.signal })), /cancelled/);
});
test('backpressure and consumer early return do not consume the full source', async () => {
  let pulled = 0,
    closed = false;
  async function* source() {
    try {
      for (let row = 1; row <= 100000; row++) {
        pulled++;
        yield { row, record: { id: row } };
      }
    } finally {
      closed = true;
    }
  }
  const session = createImport({ jobKey: 'preview' });
  for await (const result of session.run(source())) {
    assert.equal(result.row, 1);
    break;
  }
  assert.equal(pulled, 1);
  assert.equal(closed, true);
});
test('concurrency, malformed callbacks, invalid source order and capacities fail explicitly', async () => {
  const session = createImport({ jobKey: 'single' });
  const iterator = session.run([
    { row: 1, record: {} },
    { row: 2, record: {} },
  ]);
  await iterator.next();
  await assert.rejects(collect(session.run([])), /concurrently/);
  await iterator.return();
  await assert.rejects(
    collect(
      session.run([
        { row: 3, record: {} },
        { row: 3, record: {} },
      ]),
    ),
    /increasing/,
  );
  await assert.rejects(
    collect(
      createImport({ jobKey: 'bad-validation', validate: () => 'bad' }).run([
        { row: 1, record: {} },
      ]),
    ),
    /array/,
  );
  await assert.rejects(
    collect(
      createImport({ jobKey: 'bad-transform', transform: () => [] }).run([{ row: 1, record: {} }]),
    ),
    /plain record/,
  );
  await assert.rejects(
    collect(
      createImport({ jobKey: 'cap', maxRows: 1 }).run([
        { row: 1, record: {} },
        { row: 2, record: {} },
      ]),
    ),
    /row limit/,
  );
  await assert.rejects(
    collect(
      createImport({ jobKey: 'keys', maxKeys: 1, key: (record) => record.id }).run([
        { row: 1, record: { id: 'a' } },
        { row: 2, record: { id: 'b' } },
      ]),
    ),
    /capacity/,
  );
});
test('error CSV escapes quotes/newlines and protects spreadsheet formula prefixes', async () => {
  const outcomes = [
    {
      status: 'rejected',
      row: 2,
      original: {},
      value: {},
      issues: [
        { code: 'BAD', field: '=CMD()', message: 'bad "quote"\nnext' },
        { code: '-1', message: ' =SUM(A1)' },
      ],
    },
  ];
  const report = (await collect(issueReportCsv(outcomes))).join('');
  assert.ok(report.includes('"\'=CMD()"'));
  assert.ok(report.includes('"bad ""quote""\nnext"'));
  assert.ok(report.includes('"\' =SUM(A1)"'));
  assert.ok(
    (await collect(issueReportCsv(outcomes, { protectFormulas: false })))
      .join('')
      .includes('"=CMD()"'),
  );
});
test('large streaming import retains counts not accepted payloads', async () => {
  async function* source() {
    for (let row = 1; row <= 20000; row++) yield { row, record: { id: row } };
  }
  const session = createImport({ jobKey: 'large' });
  let count = 0;
  for await (const result of session.run(source())) {
    assert.equal(result.row, ++count);
  }
  assert.equal(count, 20000);
  assert.equal(session.checkpoint().seenKeys.length, 0);
});
test('CommonJS supports streaming ingestion without sibling packages', async () => {
  const cjs = createRequire(import.meta.url)('../dist/cjs/index.js');
  assert.deepEqual(await collect(cjs.csvRows('a,b')), [['a', 'b']]);
});
