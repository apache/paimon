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

"""End-to-end tests for the ``$files`` system table."""

import json
import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.table.system.files_table import FilesTable, _row_values


def _read(table):
    rb = table.new_read_builder()
    return rb.new_read().to_arrow(rb.new_scan().plan().splits())


class FilesTableTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="files_sys_")
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

    def test_files_table_loaded_via_catalog(self):
        self._create_partitioned_table()
        table = self.catalog.get_table("db.t$files")
        self.assertIsInstance(table, FilesTable)

    def test_schema_column_layout(self):
        self._create_partitioned_table()
        table = self.catalog.get_table("db.t$files")
        row_type = table.row_type()
        expected = [
            ("partition", True), ("bucket", False),
            ("file_path", False), ("file_format", False),
            ("schema_id", False), ("level", False),
            ("record_count", False), ("file_size_in_bytes", False),
            ("min_key", True), ("max_key", True),
            ("null_value_counts", False),
            ("min_value_stats", False), ("max_value_stats", False),
            ("min_sequence_number", True), ("max_sequence_number", True),
            ("creation_time", True), ("deleteRowCount", True),
            ("file_source", True), ("first_row_id", True),
            ("write_cols", True),
        ]
        self.assertEqual([n for n, _ in expected],
                         [f.name for f in row_type.fields])
        for field, (_, expected_nullable) in zip(row_type.fields, expected):
            self.assertEqual(expected_nullable, field.type.nullable,
                             "field {} nullability".format(field.name))
        self.assertEqual(["file_path"], table.primary_keys())

    def test_empty_when_no_snapshot_exists(self):
        self._create_partitioned_table()
        arrow_table = _read(self.catalog.get_table("db.t$files"))
        self.assertEqual(0, arrow_table.num_rows)

    def test_lists_files_with_partition_aggregation(self):
        self._create_partitioned_table()
        self._write_two_partitions()

        arrow_table = _read(self.catalog.get_table("db.t$files"))
        self.assertGreater(arrow_table.num_rows, 0)

        # One row per file; partitions captured as `pt=value` strings.
        partitions = set(arrow_table.column("partition").to_pylist())
        self.assertIn("dt=2024-01-01", partitions)
        self.assertIn("dt=2024-01-02", partitions)

        for path in arrow_table.column("file_path").to_pylist():
            self.assertTrue(path)
        for record_count in arrow_table.column("record_count").to_pylist():
            self.assertGreater(record_count, 0)
        for size in arrow_table.column("file_size_in_bytes").to_pylist():
            self.assertGreater(size, 0)

        # Stats columns are JSON dicts keyed by column name.
        for value in arrow_table.column("null_value_counts").to_pylist():
            self.assertIsInstance(value, str)
            decoded = json.loads(value)
            self.assertIsInstance(decoded, dict)
        for value in arrow_table.column("min_value_stats").to_pylist():
            self.assertIsInstance(value, str)
            self.assertIsInstance(json.loads(value), dict)
        for value in arrow_table.column("max_value_stats").to_pylist():
            self.assertIsInstance(value, str)
            self.assertIsInstance(json.loads(value), dict)

        # Append-only tables expose empty min_key / max_key.
        for value in arrow_table.column("min_key").to_pylist():
            self.assertIsNone(value)
        for value in arrow_table.column("max_key").to_pylist():
            self.assertIsNone(value)

        for fmt in arrow_table.column("file_format").to_pylist():
            self.assertTrue(fmt)
        for level in arrow_table.column("level").to_pylist():
            self.assertGreaterEqual(level, 0)

    def test_value_stats_render_actual_values(self):
        # A partitioned table with full stats: the value-stats maps must carry
        # the real per-column values (including the partition column), not {}.
        fields = [
            DataField.from_dict({"id": 0, "name": "id", "type": "INT"}),
            DataField.from_dict({"id": 1, "name": "v", "type": "STRING"}),
            DataField.from_dict({"id": 2, "name": "dt", "type": "STRING"}),
        ]
        self.catalog.create_table(
            "db.stats_t",
            Schema(fields=fields, partition_keys=["dt"],
                   options={"metadata.stats-mode": "full"}),
            False,
        )
        table = self.catalog.get_table("db.stats_t")
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

        arrow_table = _read(self.catalog.get_table("db.stats_t$files"))
        self.assertGreater(arrow_table.num_rows, 0)
        mins = [json.loads(s) for s in
                arrow_table.column("min_value_stats").to_pylist()]
        maxs = [json.loads(s) for s in
                arrow_table.column("max_value_stats").to_pylist()]
        # Real per-column values, not an empty {} (the BinaryRow-lacks-values bug).
        self.assertTrue(any(m for m in mins),
                        "min_value_stats all empty: {}".format(mins))
        self.assertEqual(1, min(m["id"] for m in mins if "id" in m))
        self.assertEqual(3, max(m["id"] for m in maxs if "id" in m))
        # The partition column also lands in the stats map.
        self.assertTrue(any("dt" in m for m in mins))

    def _write(self, identifier, data):
        write_builder = self.catalog.get_table(identifier).new_batch_write_builder()
        writer = write_builder.new_write()
        commit = write_builder.new_commit()
        writer.write_arrow(data)
        commit.commit(writer.prepare_commit())
        writer.close()
        commit.close()

    def test_value_stats_follow_field_ids_after_drop_and_re_add(self):
        fields = [
            DataField.from_dict({"id": 0, "name": "id", "type": "INT"}),
            DataField.from_dict({"id": 1, "name": "v", "type": "STRING"}),
        ]
        self.catalog.create_table(
            "db.evolved",
            Schema(fields=fields, options={"metadata.stats-mode": "full"}),
            False,
        )
        self._write("db.evolved", pa.table({
            "id": pa.array([1], type=pa.int32()), "v": ["old"]}))
        self.catalog.alter_table(
            "db.evolved", [SchemaChange.drop_column("v")], False)
        self.catalog.alter_table(
            "db.evolved", [SchemaChange.add_column("v", AtomicType("INT"))],
            False)
        self._write("db.evolved", pa.table({
            "id": pa.array([2], type=pa.int32()),
            "v": pa.array([20], type=pa.int32())}))

        rows = sorted(_read(self.catalog.get_table("db.evolved$files"))
                      .to_pylist(), key=lambda row: row["schema_id"])
        self.assertEqual(2, len(rows))
        old_file, new_file = rows
        # The re-added ``v`` is a different column: the old file has no stats
        # for it and all of its rows are null there.
        self.assertEqual({"id": 1, "v": None},
                         json.loads(old_file["min_value_stats"]))
        self.assertEqual({"id": 1, "v": None},
                         json.loads(old_file["max_value_stats"]))
        self.assertEqual({"id": 0, "v": 1},
                         json.loads(old_file["null_value_counts"]))
        self.assertEqual({"id": 2, "v": 20},
                         json.loads(new_file["min_value_stats"]))
        self.assertEqual({"id": 2, "v": 20},
                         json.loads(new_file["max_value_stats"]))
        self.assertEqual({"id": 0, "v": 0},
                         json.loads(new_file["null_value_counts"]))

    def test_stats_decode_errors_are_not_swallowed(self):
        class CorruptRow:
            def __len__(self):
                return 1

            def get_field(self, pos):
                raise ValueError("corrupt stats row")

        with self.assertRaises(ValueError):
            _row_values(CorruptRow())


if __name__ == "__main__":
    unittest.main()
