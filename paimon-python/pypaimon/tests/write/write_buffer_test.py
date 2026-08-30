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

"""Tests for ``WriteBuffer`` and the deferred-fold write path.

The regression these guard: ``DataWriter.write`` used to fold every incoming
batch into its buffer and then measure the result, which made writing N batches
O(N^2) -- ``pa.concat_tables`` leaves N chunks per column and the
``pa.Table.nbytes`` walk behind each rolling decision re-visits all of them.
Folding is now deferred, so the tests below assert both halves of that: the
fold count no longer scales with the number of writes, and every rolling
trigger the eager path had still fires.
"""

import contextlib
import unittest

import pyarrow as pa

from pypaimon.write.writer.append_only_data_writer import AppendOnlyDataWriter
from pypaimon.write.writer.data_vector_writer import DataVectorWriter
from pypaimon.write.writer.dedicated_format_writer import DedicatedFormatWriter
from pypaimon.write.writer.write_buffer import WriteBuffer

_SCHEMA = pa.schema([
    pa.field('id', pa.int64(), nullable=False),
    pa.field('name', pa.string()),
])

# Anything past this is effectively "no rolling"; matches how
# ``target_file_row_num`` defaults to the max long when the option is unset.
_NO_LIMIT = 2 ** 63 - 1


def _batch(start: int, num_rows: int) -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict(
        {
            'id': list(range(start, start + num_rows)),
            'name': ['n%d' % i for i in range(start, start + num_rows)],
        },
        schema=_SCHEMA,
    )


def _table(start: int, num_rows: int) -> pa.Table:
    return pa.Table.from_batches([_batch(start, num_rows)])


# Same columns and types as ``_SCHEMA``, but ``id`` is nullable. This is one of
# the differences ``TableWrite._validate_pyarrow_schema`` lets through (it only
# compares field types) while ``pa.concat_tables`` rejects it.
_NULLABLE_SCHEMA = pa.schema([
    pa.field('id', pa.int64()),
    pa.field('name', pa.string()),
])

# ``_SCHEMA`` carrying metadata, which neither ``Schema.equals`` nor
# ``concat_tables`` looks at.
_ANNOTATED_SCHEMA = pa.schema(
    [
        pa.field('id', pa.int64(), nullable=False, metadata={b'k': b'v'}),
        pa.field('name', pa.string()),
    ],
    metadata={b'origin': b'test'},
)


def _batch_with(schema: pa.Schema, start: int, num_rows: int) -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict(
        {
            'id': list(range(start, start + num_rows)),
            'name': ['n%d' % i for i in range(start, start + num_rows)],
        },
        schema=schema,
    )


def _table_with(schema: pa.Schema, start: int, num_rows: int) -> pa.Table:
    return pa.Table.from_batches([_batch_with(schema, start, num_rows)])


@contextlib.contextmanager
def _count_folds():
    """Count the concat calls a fold performs, as a list of one entry each.

    ``pa.concat_tables`` is the only way the buffer or a writer's
    ``_merge_data`` collapses tables, so its call count is the fold count. The
    eager path called it once per write; the deferred path calls it once per
    ``materialize`` that has something to fold.
    """
    calls = []
    original = pa.concat_tables

    def counting(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)

    pa.concat_tables = counting
    try:
        yield calls
    finally:
        pa.concat_tables = original


class _Harness(AppendOnlyDataWriter):
    """Append-only writer with the file layer stubbed out.

    Bypasses ``DataWriter.__init__`` -- which needs a real table, catalog and
    file IO -- and sets up only what the write/roll path reads.
    """

    def __init__(self, target_file_size: int = _NO_LIMIT,
                 target_file_row_num: int = _NO_LIMIT):
        self.target_file_size = target_file_size
        self.target_file_row_num = target_file_row_num
        self._buffer = WriteBuffer(self._merge_data)
        self.committed_files = []
        self.written_chunks = []
        self.aborted = False

    def _write_data_to_file(self, data: pa.Table):
        self.written_chunks.append(data)

    def abort(self):
        # The real ``abort`` deletes the committed files through file IO this
        # harness has none of; record that it ran and do the rest.
        self.aborted = True
        self._buffer.reset()
        self.committed_files.clear()


class WriteBufferTest(unittest.TestCase):

    def _buffer(self):
        return WriteBuffer(lambda a, b: pa.concat_tables([a, b]))

    def test_append_tracks_counts_without_folding(self):
        buffer = self._buffer()
        with _count_folds() as folds:
            for i in range(50):
                buffer.append(_table(i * 10, 10))
        self.assertEqual(folds, [])
        self.assertEqual(buffer.num_rows, 500)
        self.assertFalse(buffer.is_empty)

    def test_running_nbytes_matches_the_folded_table(self):
        # The gate on the write path trusts the running sum, so it has to agree
        # with what the folded table reports -- separate tables share no Arrow
        # buffers, so the per-table sizes add up exactly.
        buffer = self._buffer()
        for i in range(10):
            buffer.append(_table(i * 10, 10))
        running = buffer.nbytes
        self.assertEqual(running, buffer.materialize().nbytes)
        self.assertEqual(running, buffer.nbytes)

    def test_materialize_folds_once_and_is_idempotent(self):
        buffer = self._buffer()
        for i in range(20):
            buffer.append(_table(i, 1))
        with _count_folds() as folds:
            first = buffer.materialize()
            second = buffer.materialize()
        # One concat for the 20 appended tables, and nothing on re-read.
        self.assertEqual(len(folds), 1)
        self.assertIs(first, second)
        self.assertEqual(first.num_rows, 20)
        self.assertEqual(first.column('id').to_pylist(), list(range(20)))

    def test_single_append_skips_the_concat_entirely(self):
        buffer = self._buffer()
        buffer.append(_table(0, 5))
        with _count_folds() as folds:
            self.assertEqual(buffer.materialize().num_rows, 5)
        self.assertEqual(folds, [])

    def test_append_after_materialize_goes_through_merge(self):
        merged = []

        def merge(existing, new):
            merged.append((existing.num_rows, new.num_rows))
            return pa.concat_tables([existing, new])

        buffer = WriteBuffer(merge)
        buffer.append(_table(0, 3))
        buffer.materialize()
        buffer.append(_table(3, 4))
        self.assertEqual(buffer.materialize().num_rows, 7)
        self.assertEqual(merged, [(3, 4)])

    def test_empty_buffer_materializes_to_none(self):
        buffer = self._buffer()
        self.assertTrue(buffer.is_empty)
        self.assertIsNone(buffer.materialize())
        self.assertEqual(buffer.nbytes, 0)
        self.assertEqual(buffer.num_rows, 0)

    def test_reset_replaces_contents_and_remeasures(self):
        buffer = self._buffer()
        buffer.append(_table(0, 100))
        replacement = _table(0, 7)
        buffer.reset(replacement)
        self.assertIs(buffer.materialize(), replacement)
        self.assertEqual(buffer.num_rows, 7)
        self.assertEqual(buffer.nbytes, replacement.nbytes)

    def test_reset_to_none_empties_the_buffer(self):
        buffer = self._buffer()
        buffer.append(_table(0, 100))
        buffer.reset()
        self.assertTrue(buffer.is_empty)
        self.assertIsNone(buffer.materialize())
        self.assertEqual(buffer.num_rows, 0)
        self.assertEqual(buffer.nbytes, 0)

    def test_zero_row_table_is_not_reported_empty(self):
        # ``is_empty`` has to distinguish "nothing set" from "an empty table was
        # set", because the writers use it to decide whether there is anything
        # to roll at all.
        buffer = self._buffer()
        buffer.reset(_table(0, 0))
        self.assertFalse(buffer.is_empty)
        self.assertEqual(buffer.num_rows, 0)
        self.assertIsNotNone(buffer.materialize())


class SchemaGuardTest(unittest.TestCase):
    """The guard keeps a mismatch failing where the eager fold failed.

    Folding on append meant a batch ``concat_tables`` could not accept raised
    inside ``DataWriter.write``, which aborts and cleans up. Deferring the fold
    would otherwise push that failure out to ``prepare_commit``, which has no
    such handler, so ``append`` rejects up front exactly what concat rejects.
    """

    def _buffer(self):
        return WriteBuffer(lambda a, b: pa.concat_tables([a, b]))

    def test_append_rejects_a_schema_concat_would_reject(self):
        buffer = self._buffer()
        buffer.append(_table(0, 3))
        with self.assertRaises(ValueError) as caught:
            buffer.append(_table_with(_NULLABLE_SCHEMA, 3, 3))
        self.assertIn('schema differs', str(caught.exception))
        # And a rejected batch leaves the running counts describing the rows
        # that are actually buffered.
        self.assertEqual(buffer.num_rows, 3)
        self.assertEqual(buffer.materialize().num_rows, 3)

    def test_append_rejects_a_mismatch_after_a_materialize(self):
        # ``materialize`` rebases the schema onto the folded table, so the
        # second half of a buffer's life is guarded too.
        buffer = self._buffer()
        buffer.append(_table(0, 3))
        buffer.materialize()
        with self.assertRaises(ValueError):
            buffer.append(_table_with(_NULLABLE_SCHEMA, 3, 3))

    def test_append_accepts_a_metadata_only_difference(self):
        # ``concat_tables`` ignores schema and field metadata, so the guard has
        # to as well or it would reject batches the old path folded fine.
        buffer = self._buffer()
        buffer.append(_table(0, 3))
        buffer.append(_table_with(_ANNOTATED_SCHEMA, 3, 4))
        self.assertEqual(buffer.materialize().num_rows, 7)

    def test_reset_rebases_the_schema(self):
        # A writer that resets to a new table starts a new file; batches
        # matching that table's schema have to be accepted afterwards.
        buffer = self._buffer()
        buffer.append(_table(0, 3))
        buffer.reset(_table_with(_NULLABLE_SCHEMA, 0, 2))
        buffer.append(_table_with(_NULLABLE_SCHEMA, 2, 2))
        self.assertEqual(buffer.materialize().num_rows, 4)
        with self.assertRaises(ValueError):
            buffer.append(_table(4, 1))

    def test_first_append_after_an_empty_reset_sets_the_schema(self):
        buffer = self._buffer()
        buffer.append(_table(0, 3))
        buffer.reset()
        buffer.append(_table_with(_NULLABLE_SCHEMA, 0, 3))
        self.assertEqual(buffer.materialize().num_rows, 3)


class DeferredFoldWritePathTest(unittest.TestCase):

    def test_many_small_writes_fold_a_constant_number_of_times(self):
        # The regression: with an eager fold this was 199 concats for 200
        # writes, each walking a buffer one chunk longer than the last.
        writer = _Harness()
        with _count_folds() as folds:
            for i in range(200):
                writer.write(_batch(i * 5, 5))
        self.assertEqual(folds, [])
        # Reading the buffer folds it -- once, not once per write.
        with _count_folds() as folds:
            pending = writer._buffer.materialize()
        self.assertEqual(len(folds), 1)
        self.assertEqual(pending.num_rows, 1000)
        self.assertEqual(pending.column('id').to_pylist(), list(range(1000)))

    def test_row_num_rolling_still_fires(self):
        # ``target_file_row_num`` postdates the eager fold, so a byte-only gate
        # would leave row-based rolling silently dead for tables that set it
        # while staying well under target_file_size.
        writer = _Harness(target_file_row_num=10)
        for i in range(10):
            writer.write(_batch(i * 3, 3))
        self.assertEqual([c.num_rows for c in writer.written_chunks],
                         [10, 10])
        self.assertEqual(writer.pending_row_count, 10)
        self.assertEqual(
            [row for c in writer.written_chunks
             for row in c.column('id').to_pylist()],
            list(range(20)),
        )

    def test_row_num_rolling_splits_a_single_oversized_write(self):
        writer = _Harness(target_file_row_num=4)
        writer.write(_batch(0, 14))
        self.assertEqual([c.num_rows for c in writer.written_chunks],
                         [4, 4, 4])
        self.assertEqual(writer.pending_row_count, 2)

    def test_size_rolling_still_bounds_written_files(self):
        target = _table(0, 100).nbytes // 4
        writer = _Harness(target_file_size=target)
        for i in range(20):
            writer.write(_batch(i * 5, 5))
        self.assertGreaterEqual(len(writer.written_chunks), 3)
        for chunk in writer.written_chunks:
            self.assertLessEqual(chunk.nbytes, target)
        written = sum(c.num_rows for c in writer.written_chunks)
        self.assertEqual(written + writer.pending_row_count, 100)

    def test_row_that_alone_exceeds_target_size_is_rolled_by_itself(self):
        writer = _Harness(target_file_size=1)
        writer.write(_batch(0, 3))
        # Rolls the first two rows one at a time; the last stays buffered
        # because the loop stops once the buffer is down to a single row.
        self.assertEqual([c.num_rows for c in writer.written_chunks], [1, 1])
        self.assertEqual(writer.pending_row_count, 1)

    def test_rolling_does_not_refold_on_every_subsequent_write(self):
        # After a roll the remainder goes back into the buffer, which
        # re-measures it. If that left the roll condition stuck open, every
        # later write would fold again and the quadratic would be back.
        writer = _Harness(target_file_row_num=8)
        writer.write(_batch(0, 12))
        # Rolled 8 rows, 4 left buffered.
        self.assertEqual(len(writer.written_chunks), 1)
        with _count_folds() as folds:
            for i in range(20):
                writer.write(_batch(100 + i, 1))
        # 4 buffered + 20 single-row writes crosses 8 rows exactly twice, so
        # only two of those 20 writes fold anything. Asserting a bound rather
        # than an exact count: how many concats one fold takes is up to
        # ``WriteBuffer``, but it must not be one per write.
        self.assertLess(len(folds), 20)
        self.assertEqual(len(writer.written_chunks), 3)

    def test_pending_row_count_does_not_fold(self):
        # The composite writers read this on every write to size the next
        # slice; folding here would bring the quadratic back through the side
        # door.
        writer = _Harness()
        with _count_folds() as folds:
            for i in range(30):
                writer.write(_batch(i, 1))
                self.assertEqual(writer.pending_row_count, i + 1)
        self.assertEqual(folds, [])

    def test_prepare_commit_flushes_the_deferred_buffer(self):
        writer = _Harness()
        for i in range(10):
            writer.write(_batch(i * 2, 2))
        writer.prepare_commit()
        self.assertEqual([c.num_rows for c in writer.written_chunks], [20])
        self.assertTrue(writer._buffer.is_empty)

    def test_close_flushes_the_deferred_buffer(self):
        writer = _Harness()
        for i in range(10):
            writer.write(_batch(i * 2, 2))
        writer.close()
        self.assertEqual([c.num_rows for c in writer.written_chunks], [20])
        self.assertTrue(writer._buffer.is_empty)

    def test_mismatched_schema_fails_the_write_that_carries_it(self):
        # Not ``prepare_commit``: only ``write`` aborts, so a failure deferred
        # to commit time would leave the files rolled so far orphaned.
        writer = _Harness()
        writer.write(_batch(0, 3))
        with self.assertRaises(ValueError):
            writer.write(_batch_with(_NULLABLE_SCHEMA, 3, 3))
        self.assertTrue(writer.aborted)


class FlushFailureTest(unittest.TestCase):
    """A failed flush leaves the rows buffered for the retry.

    ``StreamTableWrite`` is reusable, so a transient storage error followed by
    another ``prepare_commit`` on the same writer has to write the same rows,
    not silently skip them. Draining the buffer before the write would lose
    them.
    """

    class _FailOnceHarness(_Harness):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.fail_next = True

        def _write_data_to_file(self, data: pa.Table):
            if self.fail_next:
                self.fail_next = False
                raise IOError('transient storage failure')
            super()._write_data_to_file(data)

    def test_failed_prepare_commit_keeps_the_rows_for_the_retry(self):
        writer = self._FailOnceHarness()
        for i in range(3):
            writer.write(_batch(i, 1))
        with self.assertRaises(IOError):
            writer.prepare_commit()
        self.assertEqual(writer.pending_row_count, 3)
        writer.prepare_commit()
        self.assertEqual([c.num_rows for c in writer.written_chunks], [3])
        self.assertEqual(writer.pending_row_count, 0)

    class _FailOnceVectorHarness(DataVectorWriter):
        """``_close_current_writers`` with the file layer stubbed out.

        No ``vector_writer``, so this covers the normal half on its own; the
        point is only where the buffer is cleared relative to the write.
        """

        def __init__(self):
            self.target_file_size = _NO_LIMIT
            self.target_file_row_num = _NO_LIMIT
            self.record_count = 0
            self.vector_writer = None
            self._normal_buffer = WriteBuffer(self._merge_data)
            self.committed_files = []
            self.written = []
            self.fail_next = True

        def _write_normal_data_to_file(self, data: pa.Table):
            if self.fail_next:
                self.fail_next = False
                raise IOError('transient storage failure')
            self.written.append(data)
            return object()

    def test_failed_normal_flush_keeps_the_rows_for_the_retry(self):
        # Otherwise the retry finds no normal_meta, flushes the sidecars alone
        # and skips the row-count check, committing sidecar-only metadata.
        writer = self._FailOnceVectorHarness()
        writer._normal_buffer.append(_table(0, 3))
        with self.assertRaises(IOError):
            writer._close_current_writers()
        self.assertEqual(writer._normal_buffer.num_rows, 3)
        writer._close_current_writers()
        self.assertEqual([t.num_rows for t in writer.written], [3])
        self.assertEqual(writer._normal_buffer.num_rows, 0)


class _StubMeta:
    """The handful of ``DataFileMeta`` fields the flush and abort paths read."""

    def __init__(self, row_count: int, file_name: str):
        self.row_count = row_count
        self.file_name = file_name
        self.file_path = '/warehouse/%s' % file_name
        self.external_path = None
        self.extra_files = []


class _RecordingFileIO:
    def __init__(self):
        self.deleted = []

    def delete_quietly(self, path):
        self.deleted.append(path)


class _StubSidecarWriter:
    """A blob/vector writer that fails its first ``prepare_commit``.

    Models the real ones in the way that matters here: a failure produces no
    metadata, and because the sub-writer drains its own buffer as it writes, a
    later call returns whatever has landed so far -- so the parent must be able
    to harvest the same metas twice without double-counting them.
    """

    def __init__(self, row_count: int, file_name: str, fail_times: int = 0,
                 delete_on_abort: bool = True):
        self.committed_files = []
        self.pending_row_count = 0
        self.prepare_commit_calls = 0
        self.aborted = False
        self._row_count = row_count
        self._file_name = file_name
        self._fail_times = fail_times
        self._delete_on_abort = delete_on_abort

    def prepare_commit(self):
        self.prepare_commit_calls += 1
        if self._fail_times > 0:
            self._fail_times -= 1
            raise IOError('transient sidecar failure')
        if not self.committed_files:
            self.committed_files.append(
                _StubMeta(self._row_count, self._file_name))
        return self.committed_files.copy()

    def delete_file_upon_abort(self):
        return self._delete_on_abort

    def abort(self):
        self.aborted = True
        self.committed_files.clear()


class CompositeFlushResumeTest(unittest.TestCase):
    """A composite flush publishes all of its files or none of them.

    One flush writes the normal data file and then the blob/vector sidecars. The
    sidecar writers drain their own buffers as they go, so a failure part way
    through cannot be rolled back -- deleting the sidecars that already landed
    would lose rows nothing can replay. So the flush resumes instead: the normal
    rows stay buffered until their file lands, the landed file is remembered so
    the retry skips it, and no metadata is published until every phase is done.

    Publishing the normal meta before the sidecars ran, as the code used to,
    left the retry writing a second copy of rows the first meta already covered
    -- and ``_validate_consistency`` then checked the sidecars against that
    second copy only.
    """

    class _VectorHarness(DataVectorWriter):
        def __init__(self, vector_writer, fail_normal_times: int = 0):
            self.target_file_size = _NO_LIMIT
            self.target_file_row_num = _NO_LIMIT
            self.record_count = 0
            self.vector_writer = vector_writer
            self.normal_column_names = ['id', 'name']
            self.vector_write_columns = []
            self._normal_buffer = WriteBuffer(self._merge_data)
            self._buffer = WriteBuffer(self._merge_data)
            self.committed_files = []
            self.committed_changelog_files = []
            self.file_io = _RecordingFileIO()
            self.written = []
            self._fail_normal_times = fail_normal_times

        def _write_normal_data_to_file(self, data: pa.Table):
            if self._fail_normal_times > 0:
                self._fail_normal_times -= 1
                raise IOError('transient storage failure')
            self.written.append(data)
            return _StubMeta(data.num_rows, 'data-%d' % len(self.written))

    class _DedicatedHarness(DedicatedFormatWriter):
        def __init__(self, blob_writers, vector_writer=None):
            self.target_file_size = _NO_LIMIT
            self.target_file_row_num = _NO_LIMIT
            self.record_count = 0
            self.blob_writers = blob_writers
            self.blob_file_column_names = list(blob_writers)
            self.vector_writer = vector_writer
            self._normal_buffer = WriteBuffer(self._merge_normal_data)
            self._buffer = WriteBuffer(self._merge_normal_data)
            self.committed_files = []
            self._committed_files_to_delete_on_abort = []
            self.file_io = _RecordingFileIO()
            self.written = []
            first_blob = next(iter(blob_writers.values()), None)
            self.total_record_count = first_blob._row_count if first_blob else 0
            self._blob_prepared_record_counts = {
                column: 0 for column in self.blob_file_column_names
            }

        def _write_normal_data_to_file(self, data: pa.Table):
            self.written.append(data)
            return _StubMeta(data.num_rows, 'data-%d' % len(self.written))

    def test_failed_sidecar_publishes_nothing_and_the_retry_resumes(self):
        vector = _StubSidecarWriter(3, 'vector-0', fail_times=1)
        writer = self._VectorHarness(vector)
        writer._normal_buffer.append(_table(0, 3))

        with self.assertRaises(IOError):
            writer._close_current_writers()
        # The normal file landed, so the rows are no longer buffered -- but
        # nothing is committed and the file is remembered for the retry.
        self.assertEqual([t.num_rows for t in writer.written], [3])
        self.assertEqual(writer.committed_files, [])
        self.assertEqual(writer._normal_buffer.num_rows, 0)
        self.assertIsNotNone(writer._pending_normal_meta)

        writer._close_current_writers()
        # Still one normal file: the retry resumed at the sidecar phase rather
        # than writing the same 3 rows again.
        self.assertEqual([t.num_rows for t in writer.written], [3])
        self.assertEqual([m.file_name for m in writer.committed_files],
                         ['data-1', 'vector-0'])
        self.assertIsNone(writer._pending_normal_meta)
        # Harvested once the flush completed, and cleared only then.
        self.assertEqual(vector.committed_files, [])

    def test_successful_flush_publishes_normal_then_sidecars(self):
        vector = _StubSidecarWriter(3, 'vector-0')
        writer = self._VectorHarness(vector)
        writer._normal_buffer.append(_table(0, 3))
        writer._close_current_writers()
        self.assertEqual([m.file_name for m in writer.committed_files],
                         ['data-1', 'vector-0'])
        self.assertEqual(vector.committed_files, [])
        self.assertIsNone(writer._pending_normal_meta)
        self.assertEqual(writer.record_count, 0)

    def test_failed_normal_write_keeps_the_rows_and_publishes_nothing(self):
        # The other half of the same rule: the sidecars are never reached, so
        # the rows have to stay where a retry can find them.
        vector = _StubSidecarWriter(3, 'vector-0')
        writer = self._VectorHarness(vector, fail_normal_times=1)
        writer._normal_buffer.append(_table(0, 3))
        with self.assertRaises(IOError):
            writer._close_current_writers()
        self.assertEqual(writer._normal_buffer.num_rows, 3)
        self.assertEqual(writer.committed_files, [])
        self.assertIsNone(writer._pending_normal_meta)
        self.assertEqual(vector.prepare_commit_calls, 0)

        writer._close_current_writers()
        self.assertEqual([t.num_rows for t in writer.written], [3])
        self.assertEqual([m.file_name for m in writer.committed_files],
                         ['data-1', 'vector-0'])

    def test_write_is_rejected_while_a_flush_is_unfinished(self):
        # Rows appended between a failed flush and its retry would belong to no
        # file: the resumed flush skips the normal write, while the sidecar
        # writer would drain them -- breaking the row-count check.
        vector = _StubSidecarWriter(3, 'vector-0', fail_times=1)
        writer = self._VectorHarness(vector)
        writer._normal_buffer.append(_table(0, 3))
        with self.assertRaises(IOError):
            writer._close_current_writers()

        with self.assertRaises(RuntimeError) as caught:
            writer.write(_batch(3, 1))
        self.assertIn('Cannot write', str(caught.exception))
        # Rejecting does not abort, so the flush is still resumable.
        self.assertIsNotNone(writer._pending_normal_meta)
        writer._close_current_writers()
        self.assertEqual([t.num_rows for t in writer.written], [3])

    def test_abort_deletes_the_unpublished_normal_file(self):
        # It is in no committed list, so abort has to know about it separately
        # or it leaks a data file no snapshot references.
        vector = _StubSidecarWriter(3, 'vector-0', fail_times=1)
        writer = self._VectorHarness(vector)
        writer._normal_buffer.append(_table(0, 3))
        with self.assertRaises(IOError):
            writer._close_current_writers()

        writer.abort()
        self.assertEqual(writer.file_io.deleted, ['/warehouse/data-1'])
        self.assertIsNone(writer._pending_normal_meta)
        self.assertTrue(vector.aborted)

    def test_dedicated_writer_failed_blob_phase_resumes_without_rewrite(self):
        blob = _StubSidecarWriter(3, 'blob-0', fail_times=1)
        writer = self._DedicatedHarness({'payload': blob})
        writer._normal_buffer.append(_table(0, 3))

        with self.assertRaises(IOError):
            writer._close_current_writers()
        self.assertEqual([t.num_rows for t in writer.written], [3])
        self.assertEqual([m.file_name for m in writer.committed_files], ['data-1'])
        self.assertEqual(
            [m.file_name for m in writer._committed_files_to_delete_on_abort],
            ['data-1'])

        writer._close_current_writers()
        self.assertEqual([t.num_rows for t in writer.written], [3])
        self.assertEqual([m.file_name for m in writer.committed_files],
                         ['data-1', 'blob-0'])
        self.assertEqual(blob.committed_files, [])
        self.assertIsNone(writer._pending_normal_meta)

    def test_dedicated_writer_keeps_normal_vector_ranges_before_blobs(self):
        blob = _StubSidecarWriter(3, 'blob-0')
        vector = _StubSidecarWriter(3, 'vector-0')
        writer = self._DedicatedHarness({'payload': blob}, vector)
        writer._normal_buffer.append(_table(0, 3))
        writer._close_current_writers()
        self.assertEqual([m.file_name for m in writer.committed_files],
                         ['data-1', 'vector-0', 'blob-0'])
        self.assertEqual(
            [m.file_name for m in writer._committed_files_to_delete_on_abort],
            ['data-1', 'vector-0', 'blob-0'])

    def test_dedicated_writer_respects_the_blob_delete_policy(self):
        # Externally managed blob files are not the writer's to delete, so they
        # must stay out of the abort list even now that it is filled at publish
        # time rather than as each sidecar lands.
        blob = _StubSidecarWriter(3, 'blob-0', delete_on_abort=False)
        writer = self._DedicatedHarness({'payload': blob})
        writer._normal_buffer.append(_table(0, 3))
        writer._close_current_writers()
        self.assertEqual([m.file_name for m in writer.committed_files],
                         ['data-1', 'blob-0'])
        self.assertEqual(
            [m.file_name for m in writer._committed_files_to_delete_on_abort],
            ['data-1'])


class VectorNormalBufferTest(unittest.TestCase):
    """``DataVectorWriter`` keeps its own buffer for the normal columns."""

    class _VectorHarness(DataVectorWriter):
        """Only the fields ``_should_roll_normal`` reads.

        ``CHECK_ROLLING_RECORD_CNT`` is 1 rather than the real 1000 so the size
        branch is reached on every write instead of being short-circuited by the
        periodic check.
        """

        CHECK_ROLLING_RECORD_CNT = 1

        def __init__(self, target_file_size: int = _NO_LIMIT):
            self.target_file_size = target_file_size
            self.target_file_row_num = _NO_LIMIT
            self.record_count = 0
            self.vector_writer = None
            self._normal_buffer = WriteBuffer(self._merge_data)
            self._buffer = WriteBuffer(self._merge_data)

    def test_should_roll_normal_does_not_fold(self):
        writer = self._VectorHarness()
        with _count_folds() as folds:
            for i in range(50):
                writer._normal_buffer.append(_table(i * 5, 5))
                writer.record_count += 5
                self.assertFalse(writer._should_roll_normal())
        self.assertEqual(folds, [])
        self.assertEqual(writer._normal_buffer.num_rows, 250)

    def test_should_roll_normal_fires_off_the_running_size(self):
        writer = self._VectorHarness(target_file_size=_table(0, 20).nbytes)
        writer.record_count = 1
        for i in range(10):
            writer._normal_buffer.append(_table(i * 5, 5))
            if writer._should_roll_normal():
                break
        self.assertTrue(writer._should_roll_normal())
        # Fires before the buffer grows far past the target, i.e. off the
        # accumulated size rather than at some arbitrary later point.
        self.assertLessEqual(writer._normal_buffer.num_rows, 30)

    def test_pending_row_count_reports_the_normal_buffer(self):
        # The inherited property reads the base ``_buffer``, which this writer
        # never fills, so it has to be overridden or it always answers zero.
        writer = self._VectorHarness()
        self.assertEqual(writer.pending_row_count, 0)
        writer._normal_buffer.append(_table(0, 7))
        self.assertEqual(writer.pending_row_count, 7)
        self.assertTrue(writer._buffer.is_empty)

    def test_pending_row_count_falls_back_to_the_vector_writer(self):
        # A table whose columns are all vectors buffers nothing normal, so the
        # count has to come from the sidecar instead of reading as zero.
        writer = self._VectorHarness()
        writer.vector_writer = _StubSidecarWriter(0, 'vector-0')
        writer.vector_writer.pending_row_count = 4
        self.assertEqual(writer.pending_row_count, 4)
        writer._normal_buffer.append(_table(0, 7))
        self.assertEqual(writer.pending_row_count, 7)

    def test_dedicated_writer_pending_row_count_reports_the_normal_buffer(self):
        writer = CompositeFlushResumeTest._DedicatedHarness({})
        self.assertEqual(writer.pending_row_count, 0)
        writer._normal_buffer.append(_table(0, 3))
        self.assertEqual(writer.pending_row_count, 3)
        self.assertTrue(writer._buffer.is_empty)


if __name__ == '__main__':
    unittest.main()
