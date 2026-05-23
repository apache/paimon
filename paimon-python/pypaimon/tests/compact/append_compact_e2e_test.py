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

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.compact.options import CompactOptions


class AppendCompactE2ETest(unittest.TestCase):
    """End-to-end test: write many small files, run a CompactJob, verify
    the table reads back identical data with fewer underlying files and a
    new snapshot tagged commit_kind=COMPACT.
    """

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.temp_dir, "warehouse")
        cls.catalog = CatalogFactory.create({"warehouse": cls.warehouse})
        cls.catalog.create_database("e2e_db", False)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.temp_dir, ignore_errors=True)

    def _make_table(self, name: str, partitioned: bool = False):
        full = f"e2e_db.{name}"
        opts = {
            CoreOptions.BUCKET.key(): "-1",  # unaware bucket
            CoreOptions.TARGET_FILE_SIZE.key(): "10mb",  # plenty of headroom for small writes
            # Zero open-file-cost so the size-based packer doesn't drain
            # mid-loop on these tiny test files (each ~1 KB; with the 4 MB
            # default cost a couple of files would already weigh more than
            # 2x target and trigger a premature drain).
            CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST.key(): "0",
        }
        if partitioned:
            pa_schema = pa.schema([
                ("id", pa.int32()),
                ("name", pa.string()),
                ("dt", pa.string()),
            ])
            schema = Schema.from_pyarrow_schema(
                pa_schema, partition_keys=["dt"], options=opts,
            )
        else:
            pa_schema = pa.schema([
                ("id", pa.int32()),
                ("name", pa.string()),
            ])
            schema = Schema.from_pyarrow_schema(pa_schema, options=opts)
        self.catalog.create_table(full, schema, True)
        return self.catalog.get_table(full)

    def _write_one(self, table, batch: pa.Table):
        builder = table.new_batch_write_builder()
        write = builder.new_write()
        commit = builder.new_commit()
        write.write_arrow(batch)
        commit.commit(write.prepare_commit())
        write.close()
        commit.close()

    def _read_sorted(self, table, sort_col: str = "id") -> pa.Table:
        rb = table.new_read_builder()
        scan = rb.new_scan()
        splits = scan.plan().splits()
        return rb.new_read().to_arrow(splits).sort_by(sort_col)

    def _count_live_files(self, table) -> int:
        from pypaimon.read.scanner.file_scanner import FileScanner
        from pypaimon.manifest.manifest_list_manager import ManifestListManager
        snapshot = table.snapshot_manager().get_latest_snapshot()
        if snapshot is None:
            return 0
        mlm = ManifestListManager(table)

        def manifest_scanner():
            return mlm.read_all(snapshot), snapshot
        return len(FileScanner(table, manifest_scanner).plan_files())

    def test_unpartitioned_compact_reduces_file_count_and_preserves_data(self):
        table = self._make_table("flat")

        rows_per_write = 4
        n_writes = 6
        for i in range(n_writes):
            self._write_one(table, pa.Table.from_pydict({
                "id": pa.array(
                    list(range(i * rows_per_write, (i + 1) * rows_per_write)),
                    type=pa.int32(),
                ),
                "name": [f"r-{j}" for j in range(rows_per_write)],
            }))

        table = self.catalog.get_table("e2e_db.flat")
        before_files = self._count_live_files(table)
        self.assertGreaterEqual(before_files, n_writes,
                                "Each write should leave at least one file")
        before_data = self._read_sorted(table)

        job = table.new_compact_job(compact_options=CompactOptions(min_file_num=5))
        messages = job.execute()

        self.assertEqual(1, len(messages),
                         "Single (partition, bucket) → single CommitMessage")
        msg = messages[0]
        self.assertEqual(n_writes, len(msg.compact_before),
                         "All n_writes small files should have been picked up")
        self.assertGreaterEqual(len(msg.compact_after), 1)

        table = self.catalog.get_table("e2e_db.flat")
        after_files = self._count_live_files(table)
        self.assertLess(after_files, before_files,
                        f"Compact must reduce live file count ({before_files} → {after_files})")

        after_data = self._read_sorted(table)
        self.assertEqual(before_data, after_data,
                         "Compact must preserve data identity")

        latest = table.snapshot_manager().get_latest_snapshot()
        self.assertEqual("COMPACT", latest.commit_kind)

    def test_partitioned_compact_emits_per_partition_messages(self):
        table = self._make_table("partitioned", partitioned=True)
        for partition in ["p1", "p2"]:
            for i in range(5):
                self._write_one(table, pa.Table.from_pydict({
                    "id": pa.array([i * 10 + k for k in range(3)], type=pa.int32()),
                    "name": [f"x-{k}" for k in range(3)],
                    "dt": [partition] * 3,
                }))

        table = self.catalog.get_table("e2e_db.partitioned")
        messages = table.new_compact_job(
            compact_options=CompactOptions(min_file_num=5),
        ).execute()

        partitions = sorted(m.partition for m in messages)
        self.assertEqual([("p1",), ("p2",)], partitions)
        for m in messages:
            self.assertEqual(5, len(m.compact_before))
            self.assertGreaterEqual(len(m.compact_after), 1)

    def test_no_op_when_nothing_to_compact(self):
        table = self._make_table("noop")
        # Only 2 writes — below default min_file_num.
        for i in range(2):
            self._write_one(table, pa.Table.from_pydict({
                "id": pa.array([i], type=pa.int32()),
                "name": [f"x-{i}"],
            }))
        table = self.catalog.get_table("e2e_db.noop")
        snapshot_before = table.snapshot_manager().get_latest_snapshot().id

        messages = table.new_compact_job().execute()

        self.assertEqual([], messages)
        table = self.catalog.get_table("e2e_db.noop")
        self.assertEqual(snapshot_before,
                         table.snapshot_manager().get_latest_snapshot().id,
                         "No-op compact must not produce a new snapshot")


if __name__ == "__main__":
    unittest.main()
