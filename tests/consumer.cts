import { createImport, csvRecords } from '@lp-labs/data-import-pipeline';
const session = createImport({ jobKey: 'test' });
const rows: AsyncIterable<unknown> = session.run(csvRecords('id\n1'));
void rows;
