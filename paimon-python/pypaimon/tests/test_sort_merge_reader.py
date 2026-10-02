################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import unittest

from pypaimon.read.reader.iface.record_iterator import RecordIterator
from pypaimon.read.reader.iface.record_reader import RecordReader
from pypaimon.read.reader.sort_merge_reader import SortMergeReaderWithMinHeap
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.schema.table_schema import TableSchema
from pypaimon.table.row.key_value import KeyValue
from pypaimon.table.row.row_kind import RowKind


class _ListIterator(RecordIterator):
    def __init__(self, records):
        self.records = iter(records)

    def next(self):
        return next(self.records, None)


class _BatchReader(RecordReader):
    def __init__(self, batches):
        self.batches = iter(batches)
        self.closed = False

    def read_batch(self):
        batch = next(self.batches, None)
        return None if batch is None else _ListIterator(batch)

    def close(self):
        self.closed = True


def _kv(key, sequence, value):
    return KeyValue(1, 1).replace((key, sequence, RowKind.INSERT.value, value))


class SortMergeReaderTest(unittest.TestCase):
    def test_empty_batches_between_records_and_before_eof(self):
        first = _BatchReader([
            [_kv(1, 1, 10)], [], [],
            [_kv(3, 1, 30), _kv(5, 1, 50)], [], [],
        ])
        second = _BatchReader([
            [_kv(2, 2, 20), _kv(3, 2, 300), _kv(4, 2, 40)],
        ])
        schema = TableSchema(
            fields=[DataField(0, 'id', AtomicType('INT'))],
            primary_keys=['id'],
        )
        reader = SortMergeReaderWithMinHeap([first, second], schema)
        try:
            actual = []
            while True:
                batch = reader.read_batch()
                if batch is None:
                    break
                while True:
                    kv = batch.next()
                    if kv is None:
                        break
                    actual.append((kv.key.get_field(0), kv.value.get_field(0)))

            self.assertEqual(actual, [(1, 10), (2, 20), (3, 300), (4, 40), (5, 50)])
            self.assertTrue(first.closed)
            self.assertTrue(second.closed)
        finally:
            reader.close()
