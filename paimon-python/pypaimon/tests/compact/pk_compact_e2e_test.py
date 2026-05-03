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


class PrimaryKeyCompactE2ETest(unittest.TestCase):
    """End-to-end test for primary-key compaction.

    Writes multiple snapshots that each leave a new L0 file, runs the compact
    job, and verifies: (1) the compacted files are tagged COMPACT in the
    snapshot, (2) read-after-compact returns the deduplicated latest values,
    and (3) the file count drops.
    """

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.temp_dir, "warehouse")
        cls.catalog = CatalogFactory.create({"warehouse": cls.warehouse})
        cls.catalog.create_database("pk_db", False)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.temp_dir, ignore_errors=True)

    def _make_pk_table(self, name: str):
        full = f"pk_db.{name}"
        opts = {
            CoreOptions.BUCKET.key(): "1",  # single bucket → single (partition,bucket) key
            CoreOptions.TARGET_FILE_SIZE.key(): "10mb",
            CoreOptions.METADATA_STATS_MODE.key()
            if hasattr(CoreOptions, "METADATA_STATS_MODE")
            else "metadata.stats-mode": "truncate(16)",
        }
        pa_schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
        schema = Schema.from_pyarrow_schema(pa_schema, primary_keys=["id"], options=opts)
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

    def _read_sorted(self, table) -> pa.Table:
        rb = table.new_read_builder()
        scan = rb.new_scan()
        splits = scan.plan().splits()
        return rb.new_read().to_arrow(splits).sort_by("id")

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

    def test_full_compaction_dedup_keeps_latest(self):
        table = self._make_pk_table("dedup_keep_latest")

        # Write 3 generations of (id, name) for the same set of ids — the
        # latest seen value should win after compaction.
        for gen in range(3):
            self._write_one(table, pa.Table.from_pydict({
                "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
                "name": [f"gen{gen}-{i}" for i in range(1, 6)],
            }))

        table = self.catalog.get_table("pk_db.dedup_keep_latest")
        before_files = self._count_live_files(table)
        before_data = self._read_sorted(table)
        # Read path already dedups — sanity check that the table sees latest.
        self.assertEqual(["gen2-1", "gen2-2", "gen2-3", "gen2-4", "gen2-5"],
                         before_data.column("name").to_pylist())

        messages = table.new_compact_job(
            compact_options=CompactOptions(full_compaction=True),
        ).execute()

        # Single bucket → single message expected.
        self.assertEqual(1, len(messages))
        msg = messages[0]
        self.assertGreaterEqual(len(msg.compact_before), 3,
                                "All 3 small writes should have been picked up")
        self.assertGreaterEqual(len(msg.compact_after), 1,
                                "Compaction must produce at least one output file")

        table = self.catalog.get_table("pk_db.dedup_keep_latest")
        after_files = self._count_live_files(table)
        self.assertLess(after_files, before_files,
                        f"File count must decrease ({before_files} → {after_files})")

        after_data = self._read_sorted(table)
        self.assertEqual(before_data, after_data,
                         "Compact must preserve the dedup result")

        latest = table.snapshot_manager().get_latest_snapshot()
        self.assertEqual("COMPACT", latest.commit_kind)

        # Output files should land at a level > 0 (the strategy promotes them).
        max_level = max(f.level for f in msg.compact_after)
        self.assertGreater(max_level, 0,
                           "Compacted output should land at a level > 0")

    def test_no_op_when_below_compaction_trigger(self):
        table = self._make_pk_table("noop_below_trigger")
        # Only 2 writes — far below default num-sorted-run.compaction-trigger=5.
        for i in range(2):
            self._write_one(table, pa.Table.from_pydict({
                "id": pa.array([i], type=pa.int64()),
                "name": [f"row-{i}"],
            }))
        table = self.catalog.get_table("pk_db.noop_below_trigger")
        snapshot_before = table.snapshot_manager().get_latest_snapshot().id

        messages = table.new_compact_job().execute()

        self.assertEqual([], messages)
        table = self.catalog.get_table("pk_db.noop_below_trigger")
        self.assertEqual(snapshot_before,
                         table.snapshot_manager().get_latest_snapshot().id,
                         "Strategy decided no-op → no new snapshot")


if __name__ == "__main__":
    unittest.main()
