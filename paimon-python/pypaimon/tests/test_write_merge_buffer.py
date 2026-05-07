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

"""Unit tests for ``KeyValueDataWriter._merge_pending_by_pk``.

Drives the in-memory merge buffer with synthetic ``pa.Table`` inputs
to pin down the merge-function dispatch independently of the rest of
the catalog/write stack. Mirrors the per-key fold loop in Java
``SortBufferWriteBuffer.MergeIterator.advanceIfNeeded``.
"""

import unittest
from unittest.mock import Mock

import pyarrow as pa

from pypaimon.read.reader.deduplicate_merge_function import \
    DeduplicateMergeFunction
from pypaimon.read.reader.partial_update_merge_function import \
    PartialUpdateMergeFunction
from pypaimon.write.writer.key_value_data_writer import KeyValueDataWriter


# Layout matches what ``KeyValueDataWriter._add_system_fields`` emits:
# ``[_KEY_id, _SEQUENCE_NUMBER, _VALUE_KIND, id, a, b]``. ``id`` is
# duplicated on the value side because the value layout in Paimon's
# row tuple includes every original column.
_SCHEMA = pa.schema([
    pa.field('_KEY_id', pa.int64(), nullable=False),
    pa.field('_SEQUENCE_NUMBER', pa.int64(), nullable=False),
    pa.field('_VALUE_KIND', pa.int8(), nullable=False),
    pa.field('id', pa.int64(), nullable=False),
    pa.field('a', pa.string()),
    pa.field('b', pa.string()),
])


def _row(pk, seq, a, b):
    return {
        '_KEY_id': pk,
        '_SEQUENCE_NUMBER': seq,
        '_VALUE_KIND': 0,
        'id': pk,
        'a': a,
        'b': b,
    }


class _Harness(KeyValueDataWriter):
    """Bypass ``DataWriter.__init__`` to keep the tests focused on the
    merge step. ``_merge_pending_by_pk`` only needs ``trimmed_primary_keys``
    and ``_merge_function``.
    """

    def __init__(self, merge_function):
        self.trimmed_primary_keys = ['id']
        self._merge_function = merge_function


class WriteMergeBufferTest(unittest.TestCase):

    # -- deduplicate ------------------------------------------------------

    def test_dedupe_collapses_same_pk_run_to_latest(self):
        writer = _Harness(DeduplicateMergeFunction())
        data = pa.Table.from_pylist(
            [_row(1, 1, 'old', None), _row(1, 2, 'new', None)],
            schema=_SCHEMA,
        )
        out = writer._merge_pending_by_pk(data)
        self.assertEqual(out.num_rows, 1)
        self.assertEqual(
            out.to_pylist(),
            [_row(1, 2, 'new', None)],
        )

    def test_dedupe_keeps_disjoint_keys(self):
        writer = _Harness(DeduplicateMergeFunction())
        data = pa.Table.from_pylist(
            [_row(1, 1, 'A', None),
             _row(2, 2, 'B', None),
             _row(3, 3, 'C', None)],
            schema=_SCHEMA,
        )
        out = writer._merge_pending_by_pk(data)
        self.assertEqual(out.num_rows, 3)
        self.assertEqual(
            sorted(out.to_pylist(), key=lambda r: r['id']),
            [_row(1, 1, 'A', None),
             _row(2, 2, 'B', None),
             _row(3, 3, 'C', None)],
        )

    # -- partial-update ---------------------------------------------------

    def _partial_update(self):
        # Value-side carries 3 columns (id, a, b). The PK column ``id``
        # is duplicated into the value side so partial-update can apply
        # last-non-null semantics uniformly across every original
        # user column.
        return PartialUpdateMergeFunction(
            key_arity=1, value_arity=3, nullables=[True, True, True])

    def test_partial_update_merges_non_null_per_field(self):
        writer = _Harness(self._partial_update())
        data = pa.Table.from_pylist(
            [_row(1, 1, 'A', None), _row(1, 2, None, 'B')],
            schema=_SCHEMA,
        )
        out = writer._merge_pending_by_pk(data)
        self.assertEqual(out.num_rows, 1)
        self.assertEqual(out.to_pylist(), [_row(1, 2, 'A', 'B')])

    def test_partial_update_three_writes_compose(self):
        writer = _Harness(self._partial_update())
        data = pa.Table.from_pylist(
            [_row(1, 1, 'A', None),
             _row(1, 2, None, 'B'),
             _row(1, 3, None, None)],
            schema=_SCHEMA,
        )
        out = writer._merge_pending_by_pk(data)
        self.assertEqual(out.to_pylist(), [_row(1, 3, 'A', 'B')])

    def test_partial_update_later_null_does_not_clobber_earlier_value(self):
        writer = _Harness(self._partial_update())
        data = pa.Table.from_pylist(
            [_row(1, 1, 'KEEP', 'B'), _row(1, 2, None, None)],
            schema=_SCHEMA,
        )
        out = writer._merge_pending_by_pk(data)
        self.assertEqual(out.to_pylist(), [_row(1, 2, 'KEEP', 'B')])

    # -- edge cases -------------------------------------------------------

    def test_empty_buffer_returns_empty(self):
        writer = _Harness(DeduplicateMergeFunction())
        empty = pa.Table.from_pylist([], schema=_SCHEMA)
        out = writer._merge_pending_by_pk(empty)
        self.assertEqual(out.num_rows, 0)

    def test_single_row_buffer_skips_merge(self):
        # Mock to confirm the merge function isn't invoked: a single
        # row cannot have duplicates, so we sidestep the to_pylist
        # round-trip.
        mock_mf = Mock()
        writer = _Harness(mock_mf)
        data = pa.Table.from_pylist(
            [_row(1, 1, 'X', None)], schema=_SCHEMA)
        out = writer._merge_pending_by_pk(data)
        self.assertEqual(out.num_rows, 1)
        mock_mf.reset.assert_not_called()
        mock_mf.add.assert_not_called()
        mock_mf.get_result.assert_not_called()

    def test_get_result_none_drops_pk_run(self):
        # Future-proof: contract says ``get_result`` returning ``None``
        # means the entire PK group should be dropped.
        class DropAll:
            def reset(self):
                pass

            def add(self, _):
                pass

            def get_result(self):
                return None

        writer = _Harness(DropAll())
        data = pa.Table.from_pylist(
            [_row(1, 1, 'A', None), _row(1, 2, 'B', None)],
            schema=_SCHEMA,
        )
        out = writer._merge_pending_by_pk(data)
        self.assertEqual(out.num_rows, 0)


if __name__ == '__main__':
    unittest.main()
