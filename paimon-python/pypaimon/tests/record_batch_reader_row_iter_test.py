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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
#  implied. See the License for the specific language governing
#  permissions and limitations under the License.
################################################################################

"""Unit tests for Arrow row materialization used by RecordBatchReader."""

import unittest

import pyarrow as pa

from pypaimon.read.reader.iface.record_batch_reader import (
    _iter_arrow_table_rows,
    _polars_arrow_table,
)


class _TableWithoutToPylist(object):
    """Stand-in for older PyArrow tables that lack ``Table.to_pylist``."""

    def __init__(self, table):
        self._table = table

    @property
    def num_columns(self):
        return self._table.num_columns

    @property
    def num_rows(self):
        return self._table.num_rows

    @property
    def column_names(self):
        return self._table.column_names

    def to_batches(self):
        return self._table.to_batches()


class IterArrowTableRowsTest(unittest.TestCase):

    def test_to_pylist_path_preserves_nested_and_top_level_ns(self):
        for tz in (None, 'UTC'):
            with self.subTest(tz=tz):
                ts_type = pa.timestamp('ns', tz=tz) if tz else pa.timestamp('ns')
                value_type = pa.struct([
                    ('value', pa.string()),
                    ('event_time', ts_type),
                ])
                batch = pa.RecordBatch.from_pylist([
                    {
                        'id': '1',
                        'ts': 1_000_000_001,
                        'updates': [('k', {
                            'value': 'old', 'event_time': 1_000_000_001})],
                    },
                    {
                        'id': '1',
                        'ts': 1_000_000_002,
                        'updates': [('k', {
                            'value': 'new', 'event_time': 1_000_000_002})],
                    },
                ], schema=pa.schema([
                    ('id', pa.string()),
                    ('ts', ts_type),
                    ('updates', pa.map_(pa.string(), value_type)),
                ]))
                rows = list(_iter_arrow_table_rows(_polars_arrow_table(batch)))
                self.assertEqual(len(rows), 2)
                self.assertLess(rows[0][1], rows[1][1])
                self.assertEqual(getattr(rows[0][1], 'nanosecond', None), 1)
                self.assertEqual(getattr(rows[1][1], 'nanosecond', None), 2)
                older = rows[0][2][0]['value']['event_time']
                newer = rows[1][2][0]['value']['event_time']
                self.assertLess(older, newer)
                self.assertEqual(getattr(older, 'nanosecond', None), 1)
                self.assertEqual(getattr(newer, 'nanosecond', None), 2)

    def test_batch_column_fallback_matches_to_pylist_path(self):
        value_type = pa.struct([
            ('value', pa.string()),
            ('event_time', pa.timestamp('ns')),
        ])
        batch = pa.RecordBatch.from_pylist([
            {'updates': [('k', {
                'value': 'old', 'event_time': 1_000_000_001})]},
            {'updates': [('k', {
                'value': 'new', 'event_time': 1_000_000_002})]},
            {'updates': None},
        ], schema=pa.schema([
            ('updates', pa.map_(pa.string(), value_type)),
        ]))
        table = _polars_arrow_table(batch)
        via_table = list(_iter_arrow_table_rows(table))
        via_fallback = list(_iter_arrow_table_rows(_TableWithoutToPylist(table)))
        self.assertEqual(via_fallback, via_table)
        self.assertIsNone(via_fallback[2][0])
        older = via_fallback[0][0][0]['value']['event_time']
        newer = via_fallback[1][0][0]['value']['event_time']
        self.assertLess(older, newer)
        self.assertEqual(getattr(older, 'nanosecond', None), 1)
        self.assertEqual(getattr(newer, 'nanosecond', None), 2)

    def test_fallback_handles_empty_table_and_nulls(self):
        empty = pa.table({'a': pa.array([], type=pa.int64())})
        self.assertEqual(list(_iter_arrow_table_rows(
            _TableWithoutToPylist(empty))), [])

        with_nulls = pa.table({
            'a': pa.array([1, None, 3], type=pa.int64()),
            'b': pa.array([None, 'x', 'y'], type=pa.string()),
        })
        rows = list(_iter_arrow_table_rows(_TableWithoutToPylist(with_nulls)))
        self.assertEqual(rows, [(1, None), (None, 'x'), (3, 'y')])

    def test_fallback_zero_column_table_yields_empty_tuples(self):
        class _ZeroColumnTable(object):
            num_columns = 0
            num_rows = 2

            def to_batches(self):
                raise AssertionError('zero-column path must not scan batches')

        self.assertEqual(
            list(_iter_arrow_table_rows(_ZeroColumnTable())), [(), ()])

    def test_microsecond_timestamps_remain_distinguishable(self):
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1_000_001, 1_000_002], type=pa.timestamp('us'))],
            names=['ts'])
        table = _polars_arrow_table(batch)
        for source in (table, _TableWithoutToPylist(table)):
            rows = list(_iter_arrow_table_rows(source))
            self.assertNotEqual(rows[0][0], rows[1][0])
            self.assertLess(rows[0][0], rows[1][0])


if __name__ == '__main__':
    unittest.main()
