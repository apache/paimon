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
from pypaimon.compact.coordinator.append_compact_coordinator import \
    AppendCompactCoordinator
from pypaimon.compact.options import CompactOptions
from pypaimon.common.options.core_options import CoreOptions


class AppendCompactCoordinatorTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.temp_dir, "warehouse")
        cls.catalog = CatalogFactory.create({"warehouse": cls.warehouse})
        cls.catalog.create_database("compact_db", False)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.temp_dir, ignore_errors=True)

    def _create_unaware_table(self, table_name: str, options=None) -> "FileStoreTable":  # noqa: F821
        full_name = f"compact_db.{table_name}"
        try:
            self.catalog.file_io.delete(self.catalog.get_table_path(
                self.catalog.identifier_from_string(full_name) if hasattr(
                    self.catalog, "identifier_from_string") else None), recursive=True)
        except Exception:
            pass
        # Force a small target_file_size so a few rows are already "small enough"
        # to be candidates without writing thousands of rows per test. We also
        # zero out source.split.open-file-cost so the size-based packer's bin
        # accounting degenerates to raw file_size — keeps test assertions
        # crisp instead of having to reason about a 4 MB per-file overhead
        # dwarfing the 1 KB test files.
        opts = {
            CoreOptions.BUCKET.key(): "-1",
            CoreOptions.TARGET_FILE_SIZE.key(): "1mb",
            CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST.key(): "0",
        }
        if options:
            opts.update(options)
        pa_schema = pa.schema([("id", pa.int32()), ("name", pa.string())])
        schema = Schema.from_pyarrow_schema(pa_schema, options=opts)
        self.catalog.create_table(full_name, schema, True)
        return self.catalog.get_table(full_name)

    def _write_n_files(self, table, n: int, rows_per_file: int = 5):
        builder = table.new_batch_write_builder()
        for i in range(n):
            write = builder.new_write()
            commit = builder.new_commit()
            data = pa.Table.from_pydict(
                {
                    "id": pa.array(
                        list(range(i * rows_per_file, (i + 1) * rows_per_file)),
                        type=pa.int32(),
                    ),
                    "name": [f"row-{j}" for j in range(rows_per_file)],
                }
            )
            write.write_arrow(data)
            commit.commit(write.prepare_commit())
            write.close()
            commit.close()

    def test_no_tasks_when_below_min_file_num(self):
        table = self._create_unaware_table("below_min")
        self._write_n_files(table, n=3)  # default min_file_num=5
        # Re-fetch table so it sees the new snapshots.
        table = self.catalog.get_table("compact_db.below_min")

        coordinator = AppendCompactCoordinator(table, CompactOptions(min_file_num=5))
        tasks = coordinator.plan()

        self.assertEqual(0, len(tasks),
                         "Coordinator should not plan when fewer than min_file_num small files exist")

    def test_one_task_when_threshold_met(self):
        table = self._create_unaware_table("at_threshold")
        self._write_n_files(table, n=6)
        table = self.catalog.get_table("compact_db.at_threshold")

        coordinator = AppendCompactCoordinator(table, CompactOptions(min_file_num=5))
        tasks = coordinator.plan()

        self.assertEqual(1, len(tasks))
        self.assertEqual((), tasks[0].partition)
        self.assertEqual(0, tasks[0].bucket)
        self.assertGreaterEqual(len(tasks[0].files), 5)

    def test_full_compaction_overrides_threshold(self):
        table = self._create_unaware_table("full_compact")
        self._write_n_files(table, n=2)  # well below min_file_num=5
        table = self.catalog.get_table("compact_db.full_compact")

        coordinator = AppendCompactCoordinator(
            table,
            CompactOptions(min_file_num=5, full_compaction=True),
        )
        tasks = coordinator.plan()

        self.assertEqual(1, len(tasks),
                         "full_compaction should produce a task even below min_file_num")
        self.assertEqual(2, len(tasks[0].files))

    def test_many_small_files_pack_into_single_task(self):
        # Real parquet files written here are ~1KB (well under the 1MB target
        # set in setUp), so the size-based packer never reaches its drain
        # threshold and emits a single trailing chunk containing every file.
        table = self._create_unaware_table("packed_single")
        self._write_n_files(table, n=12)
        table = self.catalog.get_table("compact_db.packed_single")

        coordinator = AppendCompactCoordinator(table, CompactOptions(min_file_num=5))
        tasks = coordinator.plan()

        self.assertEqual(1, len(tasks))
        self.assertEqual(12, len(tasks[0].files))

    def test_pk_table_rejected(self):
        full_name = "compact_db.pk_rejected"
        pa_schema = pa.schema([("id", pa.int32()), ("name", pa.string())])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=["id"],
            options={CoreOptions.BUCKET.key(): "1"},
        )
        try:
            self.catalog.create_table(full_name, schema, True)
        except Exception:
            pass
        table = self.catalog.get_table(full_name)
        with self.assertRaises(ValueError):
            AppendCompactCoordinator(table)


if __name__ == "__main__":
    unittest.main()
