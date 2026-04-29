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


class MaintenanceTest(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.warehouse = os.path.join(self.tempdir, "warehouse")
        self.catalog = CatalogFactory.create({"warehouse": self.warehouse})
        self.catalog.create_database("default", True)
        self.pa_schema = pa.schema([
            ("id", pa.int32()),
            ("name", pa.string()),
            ("dt", pa.string()),
        ])

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_compact_rewrites_files_with_compact_snapshot(self):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            partition_keys=["dt"],
            options={"bucket": "-1"},
        )
        self.catalog.create_table("default.compact_t", schema, False)
        table = self.catalog.get_table("default.compact_t")

        self._write(table, [1, 2], ["a", "b"], ["p1", "p1"])
        self._write(table, [3, 4], ["c", "d"], ["p1", "p1"])
        self._write(table, [5, 6], ["e", "f"], ["p1", "p1"])

        self.assertGreater(self._file_count(table), 1)

        result = table.new_maintenance().compact({"dt": "p1"})

        self.assertEqual(6, result.rewritten_record_count)
        self.assertEqual(1, self._file_count(table))
        self.assertEqual("COMPACT", table.snapshot_manager().get_latest_snapshot().commit_kind)
        self.assertEqual([1, 2, 3, 4, 5, 6], self._read_ids(table))

    def test_rescale_bucket_rewrites_partition(self):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            partition_keys=["dt"],
            options={"bucket": "1", "bucket-key": "id"},
        )
        self.catalog.create_table("default.rescale_t", schema, False)
        table = self.catalog.get_table("default.rescale_t")

        self._write(table, [1, 2, 3, 4, 5, 6], ["a", "b", "c", "d", "e", "f"], ["p1"] * 6)

        result = table.new_maintenance().rescale_bucket(2, {"dt": "p1"})

        self.assertEqual(6, result.rewritten_record_count)
        self.assertEqual("OVERWRITE", table.snapshot_manager().get_latest_snapshot().commit_kind)
        self.assertLessEqual(max(self._buckets(table)), 1)
        self.assertEqual([1, 2, 3, 4, 5, 6], self._read_ids(table))

    def _write(self, table, ids, names, dts):
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data = pa.Table.from_pydict(
            {
                "id": ids,
                "name": names,
                "dt": dts,
            },
            schema=self.pa_schema
        )
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

    def _file_count(self, table):
        return sum(len(split.files) for split in table.new_read_builder().new_scan().plan().splits())

    def _buckets(self, table):
        return [split.bucket for split in table.new_read_builder().new_scan().plan().splits()]

    def _read_ids(self, table):
        read_builder = table.new_read_builder()
        result = read_builder.new_read().to_arrow(read_builder.new_scan().plan().splits())
        return result.sort_by("id").column("id").to_pylist()


if __name__ == '__main__':
    unittest.main()
