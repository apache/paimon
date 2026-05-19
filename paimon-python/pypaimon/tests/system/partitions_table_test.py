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

"""End-to-end tests for the ``$partitions`` system table."""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.schema.data_types import DataField
from pypaimon.table.system.partitions_table import PartitionsTable


def _read(table):
    rb = table.new_read_builder()
    return rb.new_read().to_arrow(rb.new_scan().plan().splits())


class PartitionsTableTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="partitions_sys_")
        warehouse = os.path.join(self.tmp, "warehouse")
        self.catalog = CatalogFactory.create({"warehouse": warehouse})
        self.catalog.create_database("db", False)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _create_partitioned_table(self):
        fields = [
            DataField.from_dict({"id": 0, "name": "id", "type": "INT"}),
            DataField.from_dict({"id": 1, "name": "v", "type": "STRING"}),
            DataField.from_dict({"id": 2, "name": "dt", "type": "STRING"}),
        ]
        self.catalog.create_table(
            "db.t",
            Schema(fields=fields, partition_keys=["dt"]),
            False,
        )

    def _write_two_partitions(self):
        table = self.catalog.get_table("db.t")
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        commit = write_builder.new_commit()
        writer.write_arrow(pa.table({
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "v": ["a", "b", "c"],
            "dt": ["2024-01-01", "2024-01-01", "2024-01-02"],
        }))
        commit.commit(writer.prepare_commit())
        writer.close()
        commit.close()

    def test_partitions_table_loaded_via_catalog(self):
        self._create_partitioned_table()
        table = self.catalog.get_table("db.t$partitions")
        self.assertIsInstance(table, PartitionsTable)

    def test_schema_column_layout(self):
        self._create_partitioned_table()
        table = self.catalog.get_table("db.t$partitions")
        row_type = table.row_type()
        expected = [
            ("partition", True), ("record_count", False),
            ("file_size_in_bytes", False), ("file_count", False),
            ("last_update_time", True), ("created_at", True),
            ("created_by", True), ("updated_by", True),
            ("options", True), ("total_buckets", False),
            ("done", False),
        ]
        self.assertEqual([n for n, _ in expected],
                         [f.name for f in row_type.fields])
        for field, (_, expected_nullable) in zip(row_type.fields, expected):
            self.assertEqual(expected_nullable, field.type.nullable,
                             "field {} nullability".format(field.name))
        self.assertEqual(["partition"], table.primary_keys())

    def test_empty_when_no_snapshot_exists(self):
        self._create_partitioned_table()
        arrow_table = _read(self.catalog.get_table("db.t$partitions"))
        self.assertEqual(0, arrow_table.num_rows)

    def test_aggregates_partitions_after_commit(self):
        self._create_partitioned_table()
        self._write_two_partitions()

        arrow_table = _read(self.catalog.get_table("db.t$partitions"))
        partitions = arrow_table.column("partition").to_pylist()
        self.assertEqual({"dt=2024-01-01", "dt=2024-01-02"}, set(partitions))

        record_counts = dict(zip(
            partitions, arrow_table.column("record_count").to_pylist()))
        self.assertEqual(2, record_counts["dt=2024-01-01"])
        self.assertEqual(1, record_counts["dt=2024-01-02"])

        for size in arrow_table.column("file_size_in_bytes").to_pylist():
            self.assertGreater(size, 0)
        for count in arrow_table.column("file_count").to_pylist():
            self.assertGreaterEqual(count, 1)
        for total_buckets in arrow_table.column("total_buckets").to_pylist():
            self.assertGreaterEqual(total_buckets, 1)
        # FileSystem catalog does not maintain a "done" flag.
        for done in arrow_table.column("done").to_pylist():
            self.assertFalse(done)
        # Catalog-managed fields are not surfaced by the filesystem path.
        for field in ("created_by", "updated_by", "options", "created_at"):
            for value in arrow_table.column(field).to_pylist():
                self.assertIsNone(value)


if __name__ == "__main__":
    unittest.main()
