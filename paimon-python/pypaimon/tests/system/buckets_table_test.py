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

"""End-to-end tests for the ``$buckets`` system table."""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.schema.data_types import DataField
from pypaimon.table.system.buckets_table import BucketsTable


def _read(table):
    rb = table.new_read_builder()
    return rb.new_read().to_arrow(rb.new_scan().plan().splits())


class BucketsTableTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="buckets_sys_")
        warehouse = os.path.join(self.tmp, "warehouse")
        self.catalog = CatalogFactory.create({"warehouse": warehouse})
        self.catalog.create_database("db", False)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _create_partitioned_table(self, num_buckets=2):
        fields = [
            DataField.from_dict({"id": 0, "name": "id", "type": "INT"}),
            DataField.from_dict({"id": 1, "name": "v", "type": "STRING"}),
            DataField.from_dict({"id": 2, "name": "dt", "type": "STRING"}),
        ]
        self.catalog.create_table(
            "db.t",
            Schema(
                fields=fields,
                partition_keys=["dt"],
                options={"bucket": str(num_buckets)},
            ),
            False,
        )

    def _write_data(self):
        table = self.catalog.get_table("db.t")
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        commit = write_builder.new_commit()
        writer.write_arrow(pa.table({
            "id": pa.array([1, 2, 3, 4], type=pa.int32()),
            "v": ["a", "b", "c", "d"],
            "dt": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"],
        }))
        commit.commit(writer.prepare_commit())
        writer.close()
        commit.close()

    def test_buckets_table_loaded_via_catalog(self):
        self._create_partitioned_table()
        table = self.catalog.get_table("db.t$buckets")
        self.assertIsInstance(table, BucketsTable)

    def test_schema_column_layout(self):
        self._create_partitioned_table()
        table = self.catalog.get_table("db.t$buckets")
        row_type = table.row_type()
        expected = [
            ("partition", True), ("bucket", False),
            ("record_count", False), ("file_size_in_bytes", False),
            ("file_count", False), ("last_update_time", True),
        ]
        self.assertEqual([n for n, _ in expected],
                         [f.name for f in row_type.fields])
        for field, (_, expected_nullable) in zip(row_type.fields, expected):
            self.assertEqual(expected_nullable, field.type.nullable,
                             "field {} nullability".format(field.name))
        self.assertEqual(["partition", "bucket"], table.primary_keys())

    def test_empty_when_no_snapshot_exists(self):
        self._create_partitioned_table()
        arrow_table = _read(self.catalog.get_table("db.t$buckets"))
        self.assertEqual(0, arrow_table.num_rows)

    def test_aggregates_by_partition_and_bucket(self):
        self._create_partitioned_table(num_buckets=2)
        self._write_data()

        arrow_table = _read(self.catalog.get_table("db.t$buckets"))
        self.assertGreater(arrow_table.num_rows, 0)

        partitions = arrow_table.column("partition").to_pylist()
        self.assertTrue(all(p in ("dt=2024-01-01", "dt=2024-01-02")
                            for p in partitions))

        for size in arrow_table.column("file_size_in_bytes").to_pylist():
            self.assertGreater(size, 0)
        for count in arrow_table.column("file_count").to_pylist():
            self.assertGreaterEqual(count, 1)

        total_records = sum(arrow_table.column("record_count").to_pylist())
        self.assertEqual(4, total_records)

    def test_rows_sorted_by_partition_then_bucket(self):
        self._create_partitioned_table(num_buckets=2)
        self._write_data()

        arrow_table = _read(self.catalog.get_table("db.t$buckets"))
        partitions = arrow_table.column("partition").to_pylist()
        buckets = arrow_table.column("bucket").to_pylist()

        rows = list(zip(partitions, buckets))
        self.assertEqual(rows, sorted(rows))


if __name__ == "__main__":
    unittest.main()
