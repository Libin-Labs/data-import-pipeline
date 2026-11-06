import {
  csvRows,
  csvRecords,
  jsonLines,
  createImport,
  issueReportCsv,
  ImportRowError,
} from '@lp-labs/data-import-pipeline';
import type {
  Checkpoint,
  ImportOutcome,
  SourceRecord,
  RowIssue,
} from '@lp-labs/data-import-pipeline';
const session = createImport({
  jobKey: 'example-v1',
  columns: { id: { source: 'ID', required: true } },
  transform: async (record) => ({ ...record, id: String(record.id) }),
  validate: (record): RowIssue[] =>
    record.id ? [] : [{ code: 'REQUIRED', message: 'ID required' }],
  key: (record) => String(record.id),
});
const source: AsyncIterable<SourceRecord> = csvRecords('ID\n1\n');
for await (const result of session.run(source)) {
  const outcome: ImportOutcome = result;
  if (outcome.status === 'accepted') {
    const value: unknown = outcome.value.id;
    void value;
  }
}
const checkpoint: Checkpoint = session.checkpoint();
createImport({ jobKey: 'example-v1', checkpoint });
for await (const row of csvRows('a,b')) {
  const field: string | undefined = row[0];
  void field;
}
for await (const row of jsonLines('{"id":1}')) {
  void row;
}
for await (const chunk of issueReportCsv([])) {
  const text: string = chunk;
  void text;
}
void new ImportRowError('bad input', 'INPUT', 'id');
// @ts-expect-error key must be a string
createImport({ jobKey: 'bad', key: () => 1 });
// @ts-expect-error source chunks are decoded text, not bytes
csvRows([new Uint8Array()]);
