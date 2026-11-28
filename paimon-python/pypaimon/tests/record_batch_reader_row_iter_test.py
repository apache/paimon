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

    def test_min_ns_timestamp_not_nat_top_level_and_nested(self):
        from pypaimon.read.reader.iface.record_batch_reader import (
            _PhysicalNsTimestamp,
        )

        min_ns = -(1 << 63)
        for tz in (None, 'UTC'):
            with self.subTest(tz=tz):
                ts_type = pa.timestamp('ns', tz=tz) if tz else pa.timestamp('ns')
                value_type = pa.struct([
                    ('value', pa.string()),
                    ('event_time', ts_type),
                ])
                batch = pa.RecordBatch.from_arrays([
                    pa.array([min_ns, 0], type=ts_type),
                    pa.array([
                        [('k', {'value': 'old', 'event_time': min_ns})],
                        [('k', {'value': 'new', 'event_time': 0})],
                    ], type=pa.map_(pa.string(), value_type)),
                ], names=['ts', 'updates'])
                for source in (_polars_arrow_table(batch),
                               _TableWithoutToPylist(_polars_arrow_table(batch))):
                    rows = list(_iter_arrow_table_rows(source))
                    self.assertIsInstance(rows[0][0], _PhysicalNsTimestamp)
                    self.assertEqual(rows[0][0].value, min_ns)
                    self.assertLess(rows[0][0], rows[1][0])
                    older = rows[0][1][0]['value']['event_time']
                    newer = rows[1][1][0]['value']['event_time']
                    self.assertIsInstance(older, _PhysicalNsTimestamp)
                    self.assertEqual(older.value, min_ns)
                    self.assertLess(older, newer)

    def test_min_ns_keytime_aggregate_keeps_newer(self):
        from pypaimon.read.reader.iface.record_batch_reader import (
            _PhysicalNsTimestamp,
        )
        from pypaimon.execution.python.read.reader.aggregate.aggregators import (
            FieldMergeMapWithKeyTimeAgg,
        )
        from pypaimon.common.options.core_options import CoreOptions
        from pypaimon.common.options.options import Options
        from pypaimon.schema.data_types import AtomicType, DataField, MapType, RowType

        min_ns = _PhysicalNsTimestamp(-(1 << 63))
        epoch = pa.scalar(0, type=pa.timestamp('ns')).as_py()
        fields = [
            DataField(0, 'value', AtomicType('STRING')),
            DataField(1, 'event_time', AtomicType('TIMESTAMP(9)')),
        ]
        map_type = MapType(True, AtomicType('STRING'), RowType(True, fields))
        options = CoreOptions(Options({
            'fields.updates.ts-field': 'event_time',
        }))
        agg = FieldMergeMapWithKeyTimeAgg('updates', map_type, 'updates', options)
        acc = agg.agg(None, {'k': {'value': 'old', 'event_time': min_ns}})
        acc = agg.agg(acc, {'k': {'value': 'new', 'event_time': epoch}})
        self.assertEqual(acc['k']['value'], 'new')
        self.assertEqual(acc['k']['event_time'], epoch)

    def test_min_ns_final_result_arrow_roundtrip_keeps_physical_value(self):
        """Min-ns final keytime winner survives the real Arrow emit path.

        Public ``to_arrow`` rebuilds batches via
        ``PythonTableRead._emit_overflow_safe_batches`` →
        ``pyarrow.array(values, type=...)``. Pass the wrapper itself (not
        ``.value``) through that path. pandas ``to_pylist`` may still show
        NaT; assert Arrow scalar ``.value`` / ``is_valid``.
        """
        from pypaimon.read.reader.iface.record_batch_reader import (
            _PhysicalNsTimestamp,
        )
        from pypaimon.execution.python.read.reader.aggregate.aggregators import (
            FieldMergeMapWithKeyTimeAgg,
        )
        from pypaimon.execution.python.read.table_read import PythonTableRead
        from pypaimon.common.options.core_options import CoreOptions
        from pypaimon.common.options.options import Options
        from pypaimon.schema.data_types import AtomicType, DataField, MapType, RowType

        min_ns = -(1 << 63)
        for tz in (None, 'UTC'):
            with self.subTest(tz=tz):
                ts_type = pa.timestamp('ns', tz=tz) if tz else pa.timestamp('ns')
                batch = pa.RecordBatch.from_arrays(
                    [pa.array([min_ns], type=ts_type)], names=['event_time'])
                materialized = list(_iter_arrow_table_rows(
                    _polars_arrow_table(batch)))[0][0]
                self.assertIsInstance(materialized, _PhysicalNsTimestamp)
                self.assertEqual(materialized.value, min_ns)

                fields = [
                    DataField(0, 'value', AtomicType('STRING')),
                    DataField(1, 'event_time', AtomicType(
                        'TIMESTAMP_LTZ(9)' if tz else 'TIMESTAMP(9)')),
                ]
                map_type = MapType(
                    True, AtomicType('STRING'), RowType(True, fields))
                agg = FieldMergeMapWithKeyTimeAgg(
                    'updates', map_type, 'updates',
                    CoreOptions(Options({
                        'fields.updates.ts-field': 'event_time',
                    })))
                # Sole input: min-ns is the final keytime winner.
                acc = agg.agg(None, {
                    'k': {'value': 'only', 'event_time': materialized}})
                winner_ts = acc['k']['event_time']
                self.assertIsInstance(winner_ts, _PhysicalNsTimestamp)
                self.assertEqual(winner_ts.value, min_ns)

                # Top-level column emit (same helper as PythonTableRead.to_arrow).
                top_schema = pa.schema([('event_time', ts_type)])
                top_batches = list(PythonTableRead._emit_overflow_safe_batches(
                    {'event_time': [winner_ts]}, 1, top_schema))
                self.assertEqual(len(top_batches), 1)
                top_scalar = top_batches[0].column(0)[0]
                self.assertTrue(top_scalar.is_valid)
                self.assertEqual(top_scalar.value, min_ns)

                # Nested MAP emit: dict-shaped map value carrying the wrapper.
                map_type_arrow = pa.map_(
                    pa.string(),
                    pa.struct([
                        ('value', pa.string()),
                        ('event_time', ts_type),
                    ]))
                map_schema = pa.schema([('updates', map_type_arrow)])
                map_batches = list(PythonTableRead._emit_overflow_safe_batches(
                    {'updates': [{'k': {
                        'value': 'only', 'event_time': winner_ts}}]},
                    1, map_schema))
                self.assertEqual(len(map_batches), 1)
                nested_ts = map_batches[0].column(0)[0].values[0]['value'][
                    'event_time']
                self.assertTrue(nested_ts.is_valid)
                self.assertEqual(nested_ts.value, min_ns)


if __name__ == '__main__':
    unittest.main()
