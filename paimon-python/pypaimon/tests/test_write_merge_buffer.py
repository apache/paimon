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

"""Unit tests for ``KeyValueDataWriter`` buffer behaviour.

Covers the fold algorithm (`_merge_pending_by_pk`), the flush lifecycle
(`_flush_all` empties the buffer + clears pending_data), and the
roll-write helper (`_roll_write` splits oversized buffers across
multiple files). Drives a thin harness that bypasses
``DataWriter.__init__`` so tests can exercise these paths without
spinning up the real catalog/write stack.
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
    """Bypass ``DataWriter.__init__`` to keep tests focused.

    Provides just the attributes ``_merge_pending_by_pk`` / ``_flush_all``
    / ``_roll_write`` read, plus a recording stub for
    ``_write_data_to_file`` so the roll-write path can be exercised
    without touching the filesystem.
    """

    def __init__(self, merge_function, target_file_size: int = 10 ** 12):
        self.trimmed_primary_keys = ['id']
        self._merge_function = merge_function
        # Large enough that ``_check_and_roll_if_needed`` does not
        # trigger on its own in tests that don't care about rolling.
        self.target_file_size = target_file_size
        self.pending_data = None
        self.committed_files = []
        self.written_chunks = []

    def _write_data_to_file(self, data):
        # Record each chunk instead of writing to disk; mirrors the
        # base writer's contract of appending to ``committed_files``.
        self.written_chunks.append(data)


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

    # -- KeyValue pooling -------------------------------------------------

    def test_keyvalue_pool_does_not_alias_results_across_runs(self):
        # Pooling reuses one KeyValue across the whole fold. If the
        # PartialUpdateMergeFunction's get_result snapshotting were
        # broken, run 1's result would mutate when run 2's data is
        # written into the pooled instance. This test would catch that
        # regression: build a buffer with two distinct PK runs and
        # verify both results stand on their own.
        writer = _Harness(self._partial_update())
        data = pa.Table.from_pylist(
            [_row(1, 1, 'A1', None),
             _row(1, 2, None, 'B1'),
             _row(2, 3, 'A2', None),
             _row(2, 4, None, 'B2')],
            schema=_SCHEMA,
        )
        out = writer._merge_pending_by_pk(data)
        self.assertEqual(
            sorted(out.to_pylist(), key=lambda r: r['id']),
            [_row(1, 2, 'A1', 'B1'), _row(2, 4, 'A2', 'B2')],
        )

    # -- _process_data (no longer sorts) ----------------------------------

    def test_process_data_adds_system_fields_without_sorting(self):
        # Deferred-sort design: ``_process_data`` must not pre-sort the
        # incoming batch. The global sort happens once inside
        # ``_flush_all`` over the concatenated buffer.
        writer = _Harness(DeduplicateMergeFunction())
        writer.sequence_generator = _StubSeqGen()
        # Intentionally pass rows in descending PK order; if _process_data
        # were still sorting, the output would come back ascending.
        batch = pa.RecordBatch.from_pylist(
            [{'id': 3, 'a': 'C', 'b': None},
             {'id': 1, 'a': 'A', 'b': None},
             {'id': 2, 'a': 'B', 'b': None}],
            schema=pa.schema([
                pa.field('id', pa.int64(), nullable=False),
                pa.field('a', pa.string()),
                pa.field('b', pa.string()),
            ]),
        )
        out = writer._process_data(batch)
        self.assertEqual(
            [r['id'] for r in out.to_pylist()],
            [3, 1, 2],
        )

    # -- _flush_all -------------------------------------------------------

    def test_flush_all_sorts_folds_and_writes_one_file(self):
        # Buffer with duplicate PKs in arbitrary order. _flush_all is
        # responsible for sorting before folding, so unsorted input is
        # the right stress case.
        writer = _Harness(DeduplicateMergeFunction())
        writer.pending_data = pa.Table.from_pylist(
            [_row(2, 5, 'B2-new', None),
             _row(1, 2, 'A1-mid', None),
             _row(1, 1, 'A1-old', None),
             _row(2, 4, 'B2-old', None),
             _row(1, 3, 'A1-new', None)],
            schema=_SCHEMA,
        )
        writer._flush_all()

        # Buffer cleared.
        self.assertIsNone(writer.pending_data)
        # Exactly one file written (size well under target).
        self.assertEqual(len(writer.written_chunks), 1)
        flushed = writer.written_chunks[0]
        result = sorted(flushed.to_pylist(), key=lambda r: r['id'])
        # Dedup -> 1 row per PK, with the highest seq value retained.
        self.assertEqual(result, [
            _row(1, 3, 'A1-new', None),
            _row(2, 5, 'B2-new', None),
        ])

    def test_flush_all_on_empty_buffer_is_noop(self):
        writer = _Harness(DeduplicateMergeFunction())
        writer.pending_data = None
        writer._flush_all()
        self.assertIsNone(writer.pending_data)
        self.assertEqual(writer.written_chunks, [])

    def test_flush_all_clears_buffer_even_when_fold_drops_everything(self):
        # MergeFunction that returns None for every group; verifies
        # ``_flush_all`` still resets ``pending_data`` so a subsequent
        # write starts from a clean slate.
        class DropAll:
            def reset(self):
                pass

            def add(self, _):
                pass

            def get_result(self):
                return None

        writer = _Harness(DropAll())
        writer.pending_data = pa.Table.from_pylist(
            [_row(1, 1, 'A', None), _row(1, 2, 'B', None)],
            schema=_SCHEMA,
        )
        writer._flush_all()
        self.assertIsNone(writer.pending_data)
        self.assertEqual(writer.written_chunks, [])

    # -- _roll_write ------------------------------------------------------

    def test_roll_write_single_chunk_when_under_target(self):
        writer = _Harness(DeduplicateMergeFunction(),
                          target_file_size=10 ** 9)
        data = pa.Table.from_pylist(
            [_row(1, 1, 'A', None), _row(2, 2, 'B', None)],
            schema=_SCHEMA,
        )
        writer._roll_write(data)
        self.assertEqual(len(writer.written_chunks), 1)
        self.assertEqual(writer.written_chunks[0].num_rows, 2)

    def test_roll_write_splits_oversized_buffer_into_multiple_files(self):
        # Build a buffer whose nbytes comfortably exceeds the chosen
        # target. With a small target_file_size the writer should hand
        # back at least two files. Use long strings so nbytes scales
        # predictably with row count.
        rows = [
            _row(i, i, 'x' * 64, 'y' * 64) for i in range(1, 401)
        ]
        data = pa.Table.from_pylist(rows, schema=_SCHEMA)
        # Target small enough that 400 rows will not fit in one file.
        target = data.nbytes // 4
        writer = _Harness(DeduplicateMergeFunction(),
                          target_file_size=target)
        writer._roll_write(data)

        self.assertGreaterEqual(len(writer.written_chunks), 2)
        total_rows = sum(c.num_rows for c in writer.written_chunks)
        self.assertEqual(total_rows, data.num_rows)
        # Each chunk except possibly the last should respect the target.
        for chunk in writer.written_chunks[:-1]:
            self.assertLessEqual(chunk.nbytes, target)


class _StubSeqGen:
    """Stand-in for ``SequenceGenerator`` so the harness can call
    ``_process_data`` without going through the real ``DataWriter.__init__``.
    """

    def __init__(self):
        self._n = 0

    def next(self) -> int:
        self._n += 1
        return self._n


if __name__ == '__main__':
    unittest.main()
