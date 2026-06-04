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

"""End-to-end tests for the ``$manifests`` system table."""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.schema.data_types import DataField
from pypaimon.table.system.manifests_table import ManifestsTable


def _read(table):
    rb = table.new_read_builder()
    return rb.new_read().to_arrow(rb.new_scan().plan().splits())


class ManifestsTableTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="manifests_sys_")
        warehouse = os.path.join(self.tmp, "warehouse")
        self.catalog = CatalogFactory.create({"warehouse": warehouse})
        self.catalog.create_database("db", False)
        fields = [
            DataField.from_dict({"id": 0, "name": "id", "type": "INT"}),
            DataField.from_dict({"id": 1, "name": "v", "type": "STRING"}),
        ]
        self.catalog.create_table("db.t", Schema(fields=fields), False)
        self.table = self.catalog.get_table("db.t")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _write_one_commit(self):
        write_builder = self.table.new_batch_write_builder()
        writer = write_builder.new_write()
        commit = write_builder.new_commit()
        writer.write_arrow(pa.table({
            "id": pa.array([1, 2], type=pa.int32()),
            "v": ["a", "b"],
        }))
        commit.commit(writer.prepare_commit())
        writer.close()
        commit.close()

    def test_manifests_table_loaded_via_catalog(self):
        table = self.catalog.get_table("db.t$manifests")
        self.assertIsInstance(table, ManifestsTable)

    def test_schema_column_layout(self):
        table = self.catalog.get_table("db.t$manifests")
        row_type = table.row_type()
        expected = [
            ("file_name", False), ("file_size", False),
            ("num_added_files", False), ("num_deleted_files", False),
            ("schema_id", False), ("min_partition_stats", True),
            ("max_partition_stats", True), ("min_row_id", True),
            ("max_row_id", True),
        ]
        self.assertEqual([n for n, _ in expected],
                         [f.name for f in row_type.fields])
        for field, (_, expected_nullable) in zip(row_type.fields, expected):
            self.assertEqual(expected_nullable, field.type.nullable,
                             "field {} nullability".format(field.name))
        self.assertEqual(["file_name"], table.primary_keys())

    def test_empty_when_no_snapshot_exists(self):
        arrow_table = _read(self.catalog.get_table("db.t$manifests"))
        self.assertEqual(0, arrow_table.num_rows)

    def test_lists_manifests_of_latest_snapshot(self):
        self._write_one_commit()
        arrow_table = _read(self.catalog.get_table("db.t$manifests"))
        self.assertGreater(arrow_table.num_rows, 0)

        for name in arrow_table.column("file_name").to_pylist():
            self.assertTrue(name)
        for size in arrow_table.column("file_size").to_pylist():
            self.assertGreater(size, 0)
        for n_added in arrow_table.column("num_added_files").to_pylist():
            self.assertGreaterEqual(n_added, 0)

        # min/max_partition_stats are placeholders until the partition
        # cast-to-string helper lands; pin the placeholder contract.
        for value in arrow_table.column("min_partition_stats").to_pylist():
            self.assertIsNone(value)
        for value in arrow_table.column("max_partition_stats").to_pylist():
            self.assertIsNone(value)


if __name__ == "__main__":
    unittest.main()
