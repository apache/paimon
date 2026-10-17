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

import os
import shutil
import tempfile
import time
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.table.row.generic_row import GenericRow, GenericRowDeserializer, GenericRowSerializer
from pypaimon.table.system.files_table import FilesTable, _metadata_file_path, _render_partition, _row_values
from pypaimon.utils.file_store_path_factory import FileStorePathFactory


def _read(table):
    rb = table.new_read_builder()
    return rb.new_read().to_arrow(rb.new_scan().plan().splits())


def _stats_map(text):
    # FilesTable exposes Java Map.toString, with sorted names and unquoted cells.
    assert text.startswith('{') and text.endswith('}')
    pairs = {}
    for item in text[1:-1].split(', ') if text != '{}' else []:
        name, value = item.split('=', 1)
        try:
            value = int(value)
        except ValueError:
            value = None if value == 'null' else value
        pairs[name] = value
    return pairs


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

    def test_file_path_suffix_filter(self):
        self._create_partitioned_table()
        self._write_two_partitions()
        table = self.catalog.get_table("db.t$files")
        expected = _read(table).column("file_path").to_pylist()
        self.assertTrue(expected)
        rb = table.new_read_builder()
        pb = rb.new_predicate_builder()
        for suffix, paths in ((".parquet", expected), (".missing", [])):
            with self.subTest(suffix=suffix):
                rb.with_filter(pb.endswith("file_path", suffix)).with_projection(["file_path"])
                result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
                self.assertEqual(result.column("file_path").to_pylist(), paths)

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

        # One row per file; partitions rendered with Java Row to STRING.
        partitions = set(arrow_table.column("partition").to_pylist())
        self.assertIn("{2024-01-01}", partitions)
        self.assertIn("{2024-01-02}", partitions)

        for path in arrow_table.column("file_path").to_pylist():
            self.assertTrue(path)
        for record_count in arrow_table.column("record_count").to_pylist():
            self.assertGreater(record_count, 0)
        for size in arrow_table.column("file_size_in_bytes").to_pylist():
            self.assertGreater(size, 0)

        # Stats columns use Java Map.toString keyed by column name.
        for value in arrow_table.column("null_value_counts").to_pylist():
            self.assertIsInstance(value, str)
            decoded = _stats_map(value)
            self.assertIsInstance(decoded, dict)
        for value in arrow_table.column("min_value_stats").to_pylist():
            self.assertIsInstance(value, str)
            self.assertIsInstance(_stats_map(value), dict)
        for value in arrow_table.column("max_value_stats").to_pylist():
            self.assertIsInstance(value, str)
            self.assertIsInstance(_stats_map(value), dict)

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
        mins = [_stats_map(s) for s in
                arrow_table.column("min_value_stats").to_pylist()]
        maxs = [_stats_map(s) for s in
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
                         _stats_map(old_file["min_value_stats"]))
        self.assertEqual({"id": 1, "v": None},
                         _stats_map(old_file["max_value_stats"]))
        self.assertEqual({"id": 0, "v": 1},
                         _stats_map(old_file["null_value_counts"]))
        self.assertEqual({"id": 2, "v": 20},
                         _stats_map(new_file["min_value_stats"]))
        self.assertEqual({"id": 2, "v": 20},
                         _stats_map(new_file["max_value_stats"]))
        self.assertEqual({"id": 0, "v": 0},
                         _stats_map(new_file["null_value_counts"]))

    def test_stats_decode_errors_are_not_swallowed(self):
        class CorruptRow:
            def __len__(self):
                return 1

            def get_field(self, pos):
                raise ValueError("corrupt stats row")

        with self.assertRaises(ValueError):
            _row_values(CorruptRow())


@pytest.mark.parametrize('type_name', ['TIMESTAMP_LTZ(9)', 'TIMESTAMP(9) WITH LOCAL TIME ZONE'])
@pytest.mark.parametrize('value,expected', [
    (datetime(2500, 1, 1, tzinfo=timezone.utc), {
        'UTC': '2500-01-01 00:00:00.000000000',
        'America/New_York': '2499-12-31 19:00:00.000000000',
        'Asia/Shanghai': '2500-01-01 08:00:00.000000000',
    }),
    (datetime(2500, 7, 1, microsecond=123456, tzinfo=timezone.utc), {
        'UTC': '2500-07-01 00:00:00.123456000',
        'America/New_York': '2500-06-30 20:00:00.123456000',
        'Asia/Shanghai': '2500-07-01 08:00:00.123456000',
    }),
    (datetime(1600, 1, 1, microsecond=123456, tzinfo=timezone.utc), {
        'UTC': '1600-01-01 00:00:00.123456000',
        'America/New_York': '1599-12-31 19:03:58.123456000',
        'Asia/Shanghai': '1600-01-01 08:05:43.123456000',
    }),
])
def test_ltz_partition_strings_outside_arrow_nanosecond_range(type_name, value, expected):
    zone = os.environ.get('TZ', time.tzname[0])
    if zone not in expected:
        pytest.skip('Run Java timezone golden cases with TZ set to UTC, New York or Shanghai')
    fields = [DataField(0, 'pt', AtomicType(type_name))]
    row = GenericRow([value], fields)
    decoded = GenericRowDeserializer.from_bytes(GenericRowSerializer.to_bytes(row), fields)
    assert _render_partition(decoded) == '{' + expected[zone] + '}'


def test_ltz_partition_strings_preserve_submicrosecond_fraction():
    import pandas as pd
    expected = {
        'UTC': '2024-01-01 00:00:00.123456789',
        'America/New_York': '2023-12-31 19:00:00.123456789',
        'Asia/Shanghai': '2024-01-01 08:00:00.123456789',
    }
    zone = os.environ.get('TZ', time.tzname[0])
    if zone not in expected:
        pytest.skip('Run Java timezone golden cases with TZ set to UTC, New York or Shanghai')
    row = GenericRow([pd.Timestamp('2024-01-01 00:00:00.123456789', tz='UTC')],
                     [DataField(0, 'pt', AtomicType('TIMESTAMP_LTZ(9)'))])
    assert _render_partition(row) == '{' + expected[zone] + '}'


@pytest.mark.parametrize('root,expected', [
    ('file:/warehouse/t', 'file:/warehouse/t/'),
    ('file:///warehouse/t', 'file:/warehouse/t/'),
    ('s3://bucket/warehouse/t', 's3://bucket/warehouse/t/'),
])
@pytest.mark.parametrize('data_directory', [None, 'data'])
def test_metadata_file_paths_keep_logical_scheme_and_escaped_partition(root, expected, data_directory):
    factory = FileStorePathFactory(
        root=root, partition_keys=['pt'], default_part_value='__DEFAULT_PARTITION__',
        format_identifier='parquet', data_file_prefix='data-', changelog_file_prefix='changelog-',
        legacy_partition_name=False, file_suffix_include_compression=False, file_compression='zstd',
        data_file_path_directory=data_directory, partition_types=[AtomicType('STRING')])
    table = SimpleNamespace(table_path=root, path_factory=lambda: factory)
    entry = SimpleNamespace(file=SimpleNamespace(external_path=None, file_name='data.parquet'),
                            partition=GenericRow(['a/b'], [DataField(0, 'pt', AtomicType('STRING'))]), bucket=0)
    assert _metadata_file_path(table, entry) == (
        expected + ('data/' if data_directory else '') + 'pt=a%2Fb/bucket-0/data.parquet')

if __name__ == "__main__":
    unittest.main()
