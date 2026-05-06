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

import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.compact.coordinator.append_compact_coordinator import \
    AppendCompactCoordinator
from pypaimon.compact.options import CompactOptions
from pypaimon.compact.rewriter.append_compact_rewriter import \
    AppendCompactRewriter


class AppendCompactRewriterTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.temp_dir, "warehouse")
        cls.catalog = CatalogFactory.create({"warehouse": cls.warehouse})
        cls.catalog.create_database("rw_db", False)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.temp_dir, ignore_errors=True)

    def _make_unaware_table(self, name: str):
        full = f"rw_db.{name}"
        opts = {
            CoreOptions.BUCKET.key(): "-1",
            CoreOptions.TARGET_FILE_SIZE.key(): "10mb",
            CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST.key(): "0",
        }
        pa_schema = pa.schema([("id", pa.int32()), ("name", pa.string())])
        schema = Schema.from_pyarrow_schema(pa_schema, options=opts)
        self.catalog.create_table(full, schema, True)
        return self.catalog.get_table(full)

    def _write_n(self, table, n: int):
        builder = table.new_batch_write_builder()
        for i in range(n):
            w = builder.new_write()
            c = builder.new_commit()
            data = pa.Table.from_pydict({
                "id": pa.array([i], type=pa.int32()),
                "name": [f"row-{i}"],
            })
            w.write_arrow(data)
            c.commit(w.prepare_commit())
            w.close()
            c.close()

    def test_does_not_mutate_input_metadata(self):
        table = self._make_unaware_table("no_mutate")
        self._write_n(table, n=5)
        table = self.catalog.get_table("rw_db.no_mutate")
        coord = AppendCompactCoordinator(table, CompactOptions(min_file_num=5))
        tasks = coord.plan()
        self.assertEqual(1, len(tasks))
        files = tasks[0].files

        original_paths = [f.file_path for f in files]
        self.assertTrue(all(p is None for p in original_paths),
                        "Coordinator should hand off manifest entries with file_path=None")

        rewriter = AppendCompactRewriter(table)
        rewriter.rewrite(tasks[0].partition, tasks[0].bucket, files)

        # Rewriter must not write file_path back onto manifest-owned objects.
        self.assertEqual(original_paths, [f.file_path for f in files])

    def test_output_seq_range_starts_at_input0_min_seq_and_spans_total_rows(self):
        # Mirrors Java BaseAppendFileStoreWrite.compactRewrite seeding the
        # rolling writer's counter with toCompact.get(0).minSequenceNumber()
        # and bumping it once per row written. After compact, the union of
        # output [min_seq, max_seq] ranges must be:
        #   [files[0].min_seq, files[0].min_seq + total_input_rows - 1]
        # contiguous, no gaps, no overlap.
        table = self._make_unaware_table("seq_range")
        self._write_n(table, n=5)  # 5 files, 1 row each → 5 rows total
        table = self.catalog.get_table("rw_db.seq_range")
        coord = AppendCompactCoordinator(table, CompactOptions(min_file_num=5))
        tasks = coord.plan()
        self.assertEqual(1, len(tasks))

        # files arrive size-asc-sorted; in unaware tables every write seeded
        # at seq=0, so files[0].min_sequence_number is also 0 here.
        seed = tasks[0].files[0].min_sequence_number
        total_rows = sum(f.row_count for f in tasks[0].files)

        rewriter = AppendCompactRewriter(table)
        new_files = rewriter.rewrite(tasks[0].partition, tasks[0].bucket, list(tasks[0].files))

        self.assertGreater(len(new_files), 0)
        self.assertEqual(total_rows, sum(f.row_count for f in new_files),
                         "Compact must preserve total row count")
        # Per-file invariant: max - min + 1 == row_count (the rolling writer
        # advanced exactly once per row in this file's slice).
        for f in new_files:
            self.assertEqual(f.row_count, f.max_sequence_number - f.min_sequence_number + 1,
                             f"file {f.file_name} seq range must match its row_count")
        # Cross-file invariant: starts at seed, no gaps/overlaps when sorted.
        sorted_out = sorted(new_files, key=lambda f: f.min_sequence_number)
        self.assertEqual(seed, sorted_out[0].min_sequence_number)
        for prev, curr in zip(sorted_out, sorted_out[1:]):
            self.assertEqual(prev.max_sequence_number + 1, curr.min_sequence_number,
                             "Adjacent output files must form a contiguous seq range")
        # Total upper bound matches Java: seed + total_rows - 1.
        self.assertEqual(seed + total_rows - 1, sorted_out[-1].max_sequence_number)

    def test_output_files_tagged_compact_source(self):
        from pypaimon.compact.rewriter.merge_tree_rolling_writer import \
            FILE_SOURCE_COMPACT
        table = self._make_unaware_table("source_tag")
        self._write_n(table, n=5)
        table = self.catalog.get_table("rw_db.source_tag")
        coord = AppendCompactCoordinator(table, CompactOptions(min_file_num=5))
        tasks = coord.plan()

        new_files = AppendCompactRewriter(table).rewrite(
            tasks[0].partition, tasks[0].bucket, list(tasks[0].files),
        )
        self.assertGreater(len(new_files), 0)
        for f in new_files:
            self.assertEqual(FILE_SOURCE_COMPACT, f.file_source)

    def test_aborts_partial_output_on_failure(self):
        table = self._make_unaware_table("abort_on_failure")
        self._write_n(table, n=5)
        table = self.catalog.get_table("rw_db.abort_on_failure")
        coord = AppendCompactCoordinator(table, CompactOptions(min_file_num=5))
        tasks = coord.plan()
        self.assertEqual(1, len(tasks))

        rewriter = AppendCompactRewriter(table)
        # Force AppendOnlyDataWriter.prepare_commit to blow up after some
        # batches have already been buffered/flushed; rewriter must abort
        # those outputs rather than leave them on disk.
        with patch(
            "pypaimon.write.writer.append_only_data_writer.AppendOnlyDataWriter.prepare_commit",
            side_effect=RuntimeError("boom"),
        ):
            with self.assertRaises(RuntimeError):
                rewriter.rewrite(tasks[0].partition, tasks[0].bucket, list(tasks[0].files))

        # Snapshot id should not have advanced (no successful commit happened),
        # and no new compaction snapshot should exist.
        latest = table.snapshot_manager().get_latest_snapshot()
        self.assertNotEqual("COMPACT", latest.commit_kind,
                            "Failed compaction must not produce a COMPACT snapshot")


if __name__ == "__main__":
    unittest.main()
