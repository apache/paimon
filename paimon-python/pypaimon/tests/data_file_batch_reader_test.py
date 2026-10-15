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

import unittest
from unittest import mock

import pyarrow as pa

from pypaimon.read.reader.data_file_batch_reader import DataFileBatchReader
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import PyarrowFieldParser


class DataFileBatchReaderTest(unittest.TestCase):
    def reader(self, batches, schema, **kwargs):
        format_reader = mock.Mock(spec=RecordBatchReader)
        format_reader.read_arrow_batch.side_effect = list(batches) + [None]
        return DataFileBatchReader(
            format_reader, None, None, None,
            PyarrowFieldParser.to_paimon_schema(schema),
            max_sequence_number=7, first_row_id=20,
            row_tracking_enabled=bool(kwargs), system_fields=kwargs)

    def test_matching_batches_are_reused(self):
        batch = pa.record_batch([pa.array([1, 2])], names=['id'])
        reader = self.reader([batch, batch], batch.schema)
        self.assertEqual(reader.read_arrow_batch(), batch)
        self.assertIs(reader.read_arrow_batch(), batch)
        self.assertIsNone(reader.read_arrow_batch())

    def test_schema_changes_and_metadata_are_still_aligned(self):
        target = pa.schema([pa.field('id', pa.int64(), nullable=False)])
        matching = pa.record_batch([pa.array([1, 2])], schema=target)
        old = pa.record_batch([pa.array([1, 2], type=pa.int32())], names=['id'])
        metadata = pa.schema([pa.field('id', pa.int64(), nullable=False,
                                       metadata={'source': 'file'})],
                             metadata={'writer': 'test'})
        annotated = pa.record_batch([pa.array([1, 2])], schema=metadata)
        batches = [old, matching, old, annotated, matching]
        reader = self.reader(batches, target)
        for _ in batches:
            actual = reader.read_arrow_batch()
            self.assertTrue(actual.schema.equals(target, check_metadata=True))
            self.assertEqual(actual, matching)

    def test_row_tracking_advances_on_reused_batches(self):
        batch = pa.record_batch([pa.array([1, 2]), pa.array([0, 0])],
                                names=['id', '_ROW_ID'])
        reader = self.reader([batch, batch], batch.schema, _ROW_ID=1)
        self.assertEqual(reader.read_arrow_batch()['_ROW_ID'].to_pylist(), [20, 21])
        self.assertEqual(reader.read_arrow_batch()['_ROW_ID'].to_pylist(), [22, 23])

    def test_reordered_columns_do_not_reuse_previous_schema(self):
        first = pa.record_batch([pa.array([1]), pa.array(['a'])], names=['id', 'text'])
        second = first.select(['text', 'id'])
        reader = self.reader([first, second, first], first.schema)
        for expected in [first, second, first]:
            self.assertEqual(reader.read_arrow_batch(), expected)
