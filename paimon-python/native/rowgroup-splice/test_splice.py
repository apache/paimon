# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import subprocess
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq


class SpliceTest(unittest.TestCase):
    def test_preserves_values_schema_and_page_indexes(self):
        binary = os.environ['PAIMON_ROWGROUP_SPLICE_TEST_BIN']
        with tempfile.TemporaryDirectory() as tmp:
            source = os.path.join(tmp, 'source.parquet')
            replacement = os.path.join(tmp, 'replacement.parquet')
            output = os.path.join(tmp, 'output.parquet')
            schema = pa.schema([
                pa.field('payload_text', pa.string(), metadata={b'PARQUET:field_id': b'10'}),
                pa.field('payload_map', pa.map_(pa.string(), pa.string()),
                         metadata={b'PARQUET:field_id': b'11'}),
                pa.field('payload_time', pa.timestamp('ms'),
                         metadata={b'PARQUET:field_id': b'12'}),
            ])
            original = pa.Table.from_arrays([
                pa.array(['old-{}'.format(i) if i % 5 else None for i in range(12)]),
                pa.array([[('a', str(i))] if i % 3 else [] for i in range(12)],
                         type=schema.field('payload_map').type),
                pa.array([i * 1000 for i in range(12)], type=pa.timestamp('ms')),
            ], schema=schema)
            pq.write_table(original, source, row_group_size=4, write_page_index=True)
            changed = original.slice(4, 4).set_column(
                0, schema.field('payload_text'), pa.array(['new-{}'.format(i) for i in range(4)]))
            pq.write_table(changed, replacement, row_group_size=4, write_page_index=True)
            subprocess.run([binary, source, output, '1', replacement], check=True)
            actual = pq.read_table(output)
            expected = pa.concat_tables([original.slice(0, 4), changed, original.slice(8, 4)])
            self.assertEqual(actual.to_pylist(), expected.to_pylist())
            self.assertEqual(pq.ParquetFile(output).schema, pq.ParquetFile(source).schema)
            self.assertEqual(pq.read_metadata(output).metadata, pq.read_metadata(source).metadata)
            metadata = pq.ParquetFile(output).metadata
            self.assertEqual(metadata.num_row_groups, 3)
            for rg in range(3):
                for col in range(3):
                    self.assertTrue(metadata.row_group(rg).column(col).has_column_index)
                    self.assertTrue(metadata.row_group(rg).column(col).has_offset_index)

    def test_one_source_group_can_be_replaced_by_multiple_groups(self):
        binary = os.environ['PAIMON_ROWGROUP_SPLICE_TEST_BIN']
        with tempfile.TemporaryDirectory() as tmp:
            source = os.path.join(tmp, 'source.parquet')
            replacement = os.path.join(tmp, 'replacement.parquet')
            output = os.path.join(tmp, 'output.parquet')
            original = pa.table({'value': ['old-{}'.format(i) for i in range(12)]})
            changed = pa.table({'value': ['new-{}'.format(i) for i in range(4)]})
            pq.write_table(original, source, row_group_size=4, write_page_index=True)
            pq.write_table(changed, replacement, row_group_size=2, write_page_index=True)
            subprocess.run([binary, source, output, '1', replacement], check=True)
            expected = pa.concat_tables([original.slice(0, 4), changed, original.slice(8, 4)])
            self.assertEqual(expected.to_pylist(), pq.read_table(output).to_pylist())
            metadata = pq.read_metadata(output)
            self.assertEqual(4, metadata.num_row_groups)
            self.assertEqual([4, 2, 2, 4], [metadata.row_group(i).num_rows for i in range(4)])


if __name__ == '__main__':
    unittest.main()
