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
from unittest import mock

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from pypaimon import CatalogFactory, Schema
from pypaimon.read.reader.format_pyarrow_reader import FormatPyArrowReader
from pypaimon.schema.data_types import AtomicType, DataField


N = 300
ROW_GROUP_SIZE = 64
TABLE_OPTIONS = {
    "row-tracking.enabled": "true",
    "data-evolution.enabled": "true",
    "read.batch-size": "50",
}
VARIANT_TYPE = pa.struct(
    [
        pa.field("value", pa.binary(), nullable=False),
        pa.field("metadata", pa.binary(), nullable=False),
    ]
)


class _LocalFileIO:
    filesystem = pafs.LocalFileSystem()

    def to_filesystem_path(self, path):
        return path


def _commit_with_row_groups(table, data):
    original = table.file_io.write_parquet

    def patched(path, arrow_table, **kwargs):
        kwargs.setdefault("row_group_size", ROW_GROUP_SIZE)
        return original(path, arrow_table, **kwargs)

    table.file_io.write_parquet = patched
    try:
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        commit = write_builder.new_commit()
        writer.write_arrow(data)
        commit.commit(writer.prepare_commit())
        writer.close()
        commit.close()
    finally:
        table.file_io.write_parquet = original


def _data_file_paths(table):
    warehouse_root = table.table_path.replace("file://", "")
    paths = []
    for root, _, files in os.walk(warehouse_root):
        parts = set(os.path.relpath(root, warehouse_root).split(os.sep))
        if parts & {"manifest", "schema", "snapshot", "index"}:
            continue
        for file_name in files:
            if file_name.endswith(".parquet"):
                paths.append(os.path.join(root, file_name))
    return sorted(paths)


def _rows(schema, count):
    return pa.table(
        {
            "group_id": ["g{}".format(index // 100)
                         for index in range(count)],
            "payload": [bytes([index % 256]) * 64
                        for index in range(count)],
            "value": list(range(count)),
        },
        schema=schema,
    )


class ToRunsTest(unittest.TestCase):

    def test_runs_are_sorted_deduplicated_and_empty_safe(self):
        from pypaimon.read.reader.format_pyarrow_reader import (
            _normalize_runs,
            _to_runs,
        )

        self.assertEqual(_to_runs([]), [])
        self.assertEqual(_to_runs([1, 2, 3]), [(1, 3)])
        self.assertEqual(
            _to_runs([9, 4, 1, 3, 4]),
            [(1, 1), (3, 4), (9, 9)],
        )
        self.assertEqual(_normalize_runs([]), [])
        self.assertEqual(
            _normalize_runs([
                (900_000_000, 999_999_999),
                (0, 900_000_001),
            ]),
            [(0, 999_999_999)],
        )


class RowIdRangeCursorTest(unittest.TestCase):

    def test_generates_offsets_across_batches_and_ranges(self):
        from pypaimon.read.reader.data_file_batch_reader import (
            _RowIdRangeCursor,
        )

        cursor = _RowIdRangeCursor([(10, 12), (100, 102)])
        self.assertEqual(cursor.take(2), [10, 11])
        self.assertEqual(cursor.take(3), [12, 100, 101])
        self.assertEqual(cursor.take(1), [102])


class ParquetRowRangeTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog = CatalogFactory.create(
            {"warehouse": os.path.join(cls.tempdir, "warehouse")})
        cls.catalog.create_database("default", False)
        cls.arrow_schema = pa.schema(
            [
                ("group_id", pa.string()),
                ("payload", pa.large_binary()),
                ("value", pa.int32()),
            ]
        )
        identifier = "default.parquet_row_range"
        cls.catalog.create_table(
            identifier,
            Schema.from_pyarrow_schema(
                cls.arrow_schema,
                options=TABLE_OPTIONS,
            ),
            False,
        )
        table = cls.catalog.get_table(identifier)
        _commit_with_row_groups(table, _rows(cls.arrow_schema, N))
        cls.table = cls.catalog.get_table(identifier)

        data_files = _data_file_paths(cls.table)
        assert len(data_files) == 1, data_files
        cls.data_file = data_files[0]
        assert pq.ParquetFile(cls.data_file).num_row_groups == 5

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _read(
            self,
            predicate_factory=None,
            columns=("group_id", "value", "_ROW_ID")):
        read_builder = self.table.new_read_builder().with_projection(
            list(columns))
        if predicate_factory is not None:
            predicate_builder = read_builder.new_predicate_builder()
            read_builder = read_builder.with_filter(
                predicate_factory(predicate_builder))
        splits = read_builder.new_scan().plan().splits()
        return read_builder.new_read().to_arrow(splits)

    def test_reads_only_intersecting_row_groups(self):
        result = self._read(
            lambda builder: builder.between("_ROW_ID", 100, 163))
        self.assertEqual(
            result.column("_ROW_ID").to_pylist(),
            list(range(100, 164)),
        )

        reader = FormatPyArrowReader(
            _LocalFileIO(),
            "parquet",
            self.data_file,
            [DataField(0, "value", AtomicType("INT"))],
            None,
            batch_size=50,
            row_ranges=[(100, 163)],
        )
        self.assertIsNotNone(reader._parquet_file)
        self.assertEqual(
            reader._selected_parquet_row_groups,
            [1, 2],
        )

    def test_direct_row_indices_are_sorted_and_deduplicated(self):
        reader = FormatPyArrowReader(
            _LocalFileIO(),
            "parquet",
            self.data_file,
            [DataField(0, "value", AtomicType("INT"))],
            None,
            batch_size=2,
            row_indices=[130, 11, 10, 10],
        )
        values = []
        while True:
            batch = reader.read_arrow_batch()
            if batch is None:
                break
            values.extend(batch.column(0).to_pylist())
        self.assertEqual(values, [10, 11, 130])

    def test_system_only_projection_preserves_rows(self):
        result = self._read(
            lambda builder: builder.between("_ROW_ID", 2, 7),
            columns=("_ROW_ID",),
        )
        self.assertEqual(
            result.column("_ROW_ID").to_pylist(),
            list(range(2, 8)),
        )

    def test_missing_only_projection_preserves_rows(self):
        reader = FormatPyArrowReader(
            _LocalFileIO(),
            "parquet",
            self.data_file,
            [DataField(99, "missing", AtomicType("INT"))],
            None,
            batch_size=2,
            row_ranges=[(2, 7)],
        )
        values = []
        while True:
            batch = reader.read_arrow_batch()
            if batch is None:
                break
            values.extend(batch.column(0).to_pylist())
        self.assertEqual(values, [None] * 6)

    def test_skips_per_row_python_range_filter(self):
        from pypaimon.read.reader.row_range_filter_record_reader import (
            RowIdFilterRecordBatchReader,
        )

        with mock.patch.object(
                RowIdFilterRecordBatchReader,
                "_is_row_in_range",
                wraps=RowIdFilterRecordBatchReader._is_row_in_range,
        ) as range_spy:
            result = self._read(
                lambda builder: builder.between("_ROW_ID", 100, 163))
        self.assertEqual(result.num_rows, 64)
        range_spy.assert_not_called()

    def test_disjoint_ranges_preserve_order_and_payload(self):
        result = self._read(
            lambda builder: builder.or_predicates(
                [
                    builder.between("_ROW_ID", 10, 20),
                    builder.between("_ROW_ID", 130, 140),
                    builder.between("_ROW_ID", 290, 299),
                ]
            ),
            columns=("payload", "value", "_ROW_ID"),
        )
        expected = (
            list(range(10, 21))
            + list(range(130, 141))
            + list(range(290, 300))
        )
        self.assertEqual(
            result.column("_ROW_ID").to_pylist(), expected)
        self.assertEqual(result.column("value").to_pylist(), expected)
        self.assertEqual(
            result.column(
                result.schema.get_field_index("payload")).to_pylist(),
            [bytes([index % 256]) * 64 for index in expected],
        )

    def test_column_predicate_falls_back_without_position_shift(self):
        result = self._read(
            lambda builder: builder.and_predicates(
                [
                    builder.between("_ROW_ID", 64, 250),
                    builder.greater_or_equal("value", 200),
                ]
            )
        )
        self.assertEqual(
            result.column("_ROW_ID").to_pylist(),
            list(range(200, 251)),
        )
        self.assertEqual(
            result.column("value").to_pylist(),
            list(range(200, 251)),
        )

        result = self._read(
            lambda builder: builder.and_predicates(
                [
                    builder.between("_ROW_ID", 90, 210),
                    builder.equal("group_id", "g1"),
                ]
            )
        )
        self.assertEqual(
            result.column("_ROW_ID").to_pylist(),
            list(range(100, 200)),
        )

    def test_full_scan_is_unchanged(self):
        real_parquet_file = pq.ParquetFile
        with mock.patch.object(
                pq,
                "ParquetFile",
                wraps=real_parquet_file,
        ) as parquet_file_spy:
            result = self._read(columns=("value",))
        self.assertEqual(
            sorted(result.column("value").to_pylist()),
            list(range(N)),
        )
        parquet_file_spy.assert_not_called()

    def test_projected_variant_uses_same_exact_row_slicing(self):
        path = os.path.join(self.tempdir, "variant-row-ranges.parquet")
        payload = pa.array(
            [
                {"value": "v{}".format(index).encode(), "metadata": b"m"}
                for index in range(N)
            ],
            type=VARIANT_TYPE,
        )
        pq.write_table(
            pa.table({"payload": payload}),
            path,
            row_group_size=ROW_GROUP_SIZE,
        )
        requested = list(range(60, 71)) + list(range(130, 134))
        reader = FormatPyArrowReader(
            _LocalFileIO(),
            "parquet",
            path,
            [DataField(0, "payload", AtomicType("VARIANT"))],
            None,
            batch_size=50,
            row_ranges=[(60, 70), (130, 133)],
        )
        self.assertIsNotNone(reader._parquet_file)
        self.assertEqual(
            reader._selected_parquet_row_groups,
            [0, 1, 2],
        )
        values = []
        while True:
            batch = reader.read_arrow_batch()
            if batch is None:
                break
            values.extend(
                batch.column(
                    batch.schema.get_field_index("payload")).to_pylist())
        self.assertEqual(
            values,
            [
                {
                    "value": "v{}".format(index).encode(),
                    "metadata": b"m",
                }
                for index in requested
            ],
        )


if __name__ == "__main__":
    unittest.main()
