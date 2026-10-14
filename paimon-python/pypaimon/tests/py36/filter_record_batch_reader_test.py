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
from unittest.mock import patch

import pyarrow as pa

from pypaimon.common.predicate import Predicate
from pypaimon.read.reader.filter_record_batch_reader import FilterRecordBatchReader
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import AtomicType, DataField


class _Batches(RecordBatchReader):
    def __init__(self, batches):
        self.batches = iter(batches)

    def read_arrow_batch(self, start_idx=None, end_idx=None):
        return next(self.batches, None)

    def close(self):
        pass


class FilterRecordBatchReaderTest(unittest.TestCase):
    def test_row_filter_uses_logical_field_order_and_preserves_metadata(self):
        schema = pa.schema([
            ('_row_kind', pa.string()), ('id', pa.int64()), ('behavior', pa.string()),
        ], metadata={b'source': b'row-filter'})
        batch = pa.RecordBatch.from_arrays([
            pa.array(['+I', '-U', '+U', '-D']), pa.array([1, 2, 3, 4]),
            pa.array(['apple', None, 'pear', 'apricot']),
        ], schema=schema)
        # The empty first result must not terminate the reader.
        source = _Batches([batch.slice(2, 1), batch])
        predicate = Predicate('or', None, None, [
            Predicate('startsWith', 0, 'behavior', ['a']),
            Predicate('isNull', 0, 'behavior', []),
        ])
        fields = [DataField(0, 'behavior', AtomicType('STRING')), DataField(1, 'id', AtomicType('BIGINT'))]
        reader = FilterRecordBatchReader(source, predicate, schema_fields=fields)
        try:
            result = reader.read_arrow_batch()
            self.assertTrue(result.schema.equals(schema, check_metadata=True))
            self.assertEqual(result.column(0).to_pylist(), ['+I', '-U', '-D'])
            self.assertEqual(result.column(1).to_pylist(), [1, 2, 4])
            self.assertEqual(result.column(2).to_pylist(), ['apple', None, 'apricot'])
            self.assertIsNone(reader.read_arrow_batch())
        finally:
            reader.close()

    def test_large_filter_without_concat_batches(self):
        count = 140000
        batch = pa.RecordBatch.from_arrays([pa.array(range(count))], names=['id'])
        reader = FilterRecordBatchReader(
            _Batches([batch]), Predicate('greaterOrEqual', 0, 'id', [0]))
        with patch.object(pa, 'concat_batches', None, create=True):
            result = reader.read_arrow_batch()
        self.assertEqual(result.num_rows, count)
        self.assertEqual(result.column(0).to_pylist(), list(range(count)))
        self.assertIsNone(reader.read_arrow_batch())
