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
from typing import List, Optional

from pypaimon.read.reader.iface.record_iterator import RecordIterator
from pypaimon.read.reader.iface.record_reader import RecordReader
from pypaimon.read.reader.outer_projection_record_reader import (
    OuterProjectionRecordReader, _extract, _PathSpec, _step_into)
from pypaimon.table.row.internal_row import InternalRow
from pypaimon.table.row.offset_row import OffsetRow


class _StaticIterator(RecordIterator[InternalRow]):
    """Iterator over a pre-built list of OffsetRows; yields None when drained."""

    def __init__(self, rows: List[OffsetRow]):
        self._rows = list(rows)
        self._idx = 0

    def next(self) -> Optional[InternalRow]:
        if self._idx >= len(self._rows):
            return None
        row = self._rows[self._idx]
        self._idx += 1
        return row


class _StaticReader(RecordReader[InternalRow]):
    """Reader that emits a single batch then EOF, with close-tracking."""

    def __init__(self, rows: List[OffsetRow]):
        self._rows = rows
        self._delivered = False
        self.closed = False

    def read_batch(self) -> Optional[RecordIterator[InternalRow]]:
        if self._delivered:
            return None
        self._delivered = True
        return _StaticIterator(self._rows)

    def close(self) -> None:
        self.closed = True


def _row(*values) -> OffsetRow:
    return OffsetRow(tuple(values), 0, len(values))


class StepIntoTest(unittest.TestCase):

    def test_dict_lookup(self):
        self.assertEqual(_step_into({'a': 1, 'b': 2}, 'a'), 1)
        self.assertIsNone(_step_into({'a': 1}, 'missing'))

    def test_internal_row_rejected(self):
        # Defensive: we never expect a nested InternalRow in the polars path.
        row = OffsetRow((10, 20), 0, 2)
        with self.assertRaises(TypeError):
            _step_into(row, 'whatever')

    def test_unsupported_type_raises(self):
        with self.assertRaises(TypeError):
            _step_into(42, 'name')


class ExtractTest(unittest.TestCase):

    def test_empty_sub_names_returns_top_level_field(self):
        # Empty sub_names → return the top-level slot itself (used when
        # mixing top-level and nested paths in the same projection).
        row = _row(1, {'v': 100, 's': 'a'})
        spec = _PathSpec(top_idx=0, sub_names=[])
        self.assertEqual(_extract(row, spec), 1)

    def test_nested_walk_returns_leaf(self):
        row = _row(1, {'v': 100, 's': 'a'})
        spec = _PathSpec(top_idx=1, sub_names=['v'])
        self.assertEqual(_extract(row, spec), 100)

    def test_none_at_top_short_circuits(self):
        row = _row(1, None)
        spec = _PathSpec(top_idx=1, sub_names=['v'])
        self.assertIsNone(_extract(row, spec))

    def test_missing_sub_name_returns_none(self):
        row = _row(1, {'v': 100})
        spec = _PathSpec(top_idx=1, sub_names=['s'])
        self.assertIsNone(_extract(row, spec))


class OuterProjectionRecordReaderTest(unittest.TestCase):

    def _build_reader(self, rows, top_names, name_paths):
        return OuterProjectionRecordReader(
            _StaticReader(rows), top_names, name_paths)

    def test_extracts_nested_leaf(self):
        rows = [
            _row(1, {'v': 100, 's': 'a'}, 'x'),
            _row(2, {'v': 200, 's': 'b'}, 'y'),
        ]
        reader = self._build_reader(
            rows,
            top_names=['id', 'mv', 'val'],
            name_paths=[['mv', 'v']])
        batch = reader.read_batch()
        out = []
        while True:
            r = batch.next()
            if r is None:
                break
            out.append(tuple(r.get_field(i) for i in range(len(r))))
        self.assertEqual(out, [(100,), (200,)])

    def test_mixed_top_level_and_nested_preserves_order(self):
        rows = [_row(1, {'v': 100, 's': 'a'}, 'x')]
        reader = self._build_reader(
            rows,
            top_names=['id', 'mv', 'val'],
            name_paths=[['val'], ['mv', 'v'], ['id']])
        batch = reader.read_batch()
        r = batch.next()
        self.assertEqual(
            tuple(r.get_field(i) for i in range(len(r))),
            ('x', 100, 1))

    def test_nullable_struct_returns_none(self):
        # The wrapper reuses a single OffsetRow per batch, so consumers must
        # materialise each row before advancing — same contract as
        # InternalRowWrapperIterator.
        rows = [_row(1, None, 'x'), _row(2, {'v': 200, 's': 'b'}, 'y')]
        reader = self._build_reader(
            rows,
            top_names=['id', 'mv', 'val'],
            name_paths=[['mv', 'v']])
        batch = reader.read_batch()
        first_value = batch.next().get_field(0)
        second_value = batch.next().get_field(0)
        self.assertIsNone(first_value)
        self.assertEqual(second_value, 200)

    def test_inherits_row_kind(self):
        rows = [_row(1, {'v': 100, 's': 'a'})]
        rows[0].set_row_kind_byte(2)  # -U
        reader = self._build_reader(
            rows,
            top_names=['id', 'mv'],
            name_paths=[['mv', 'v']])
        batch = reader.read_batch()
        r = batch.next()
        self.assertEqual(r.row_kind_byte, 2)

    def test_eof_returns_none(self):
        reader = self._build_reader(
            [], top_names=['id'], name_paths=[['id']])
        # First batch is empty (delivered) but the iterator immediately yields None.
        first_batch = reader.read_batch()
        self.assertIsNone(first_batch.next())
        # Subsequent read_batch returns None once the inner reader is drained.
        self.assertIsNone(reader.read_batch())

    def test_close_propagates(self):
        inner = _StaticReader([_row(1, {'v': 100})])
        reader = OuterProjectionRecordReader(
            inner, ['id', 'mv'], [['mv', 'v']])
        reader.close()
        self.assertTrue(inner.closed)

    def test_unknown_top_name_raises_at_construction(self):
        with self.assertRaises(ValueError):
            OuterProjectionRecordReader(
                _StaticReader([]), ['id', 'mv'], [['nope', 'v']])

    def test_empty_name_paths_rejected(self):
        with self.assertRaises(ValueError):
            OuterProjectionRecordReader(_StaticReader([]), ['id'], [])

    def test_empty_individual_path_rejected(self):
        with self.assertRaises(ValueError):
            OuterProjectionRecordReader(_StaticReader([]), ['id'], [[]])


if __name__ == '__main__':
    unittest.main()
