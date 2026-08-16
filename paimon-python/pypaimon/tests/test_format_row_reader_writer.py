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

import os
import tempfile
from decimal import Decimal

import pyarrow as pa
import pytest

from pypaimon.read.reader.format_row_reader import FormatRowReader
from pypaimon.schema.data_types import (
    ArrayType, AtomicType, DataField, MapType, RowType
)
from pypaimon.write.writer.format_row_writer import FormatRowWriter


class SimpleFileIO:
    """Minimal FileIO for testing."""

    def get_file_size(self, path):
        return os.path.getsize(path)

    def new_input_stream(self, path):
        return open(path, 'rb')


def _write_row_file(path, fields, data_table):
    with open(path, 'wb') as f:
        writer = FormatRowWriter(f, fields)
        writer.write_table(data_table)
        writer.close()


def _read_row_file(path, fields, read_field_names=None, row_indices=None):
    file_io = SimpleFileIO()
    if read_field_names is None:
        read_field_names = [f.name for f in fields]
    reader = FormatRowReader(file_io, path, read_field_names, fields, None,
                             row_indices=row_indices)
    batches = []
    while True:
        batch = reader.read_arrow_batch()
        if batch is None:
            break
        batches.append(batch)
    reader.close()
    if not batches:
        return pa.table({f.name: [] for f in fields})
    return pa.Table.from_batches(batches)


class TestFormatRowReaderWriter:

    def test_basic_int_string(self):
        fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "name", AtomicType("STRING")),
        ]
        data = pa.table({
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "name": pa.array(["alice", "bob", "charlie"], type=pa.string()),
        })

        with tempfile.NamedTemporaryFile(suffix=".row", delete=False) as tmp:
            path = tmp.name

        try:
            _write_row_file(path, fields, data)
            result = _read_row_file(path, fields)
            assert result.column("id").to_pylist() == [1, 2, 3]
            assert result.column("name").to_pylist() == ["alice", "bob", "charlie"]
        finally:
            os.unlink(path)

    def test_all_primitive_types(self):
        fields = [
            DataField(0, "bool_col", AtomicType("BOOLEAN")),
            DataField(1, "tinyint_col", AtomicType("TINYINT")),
            DataField(2, "smallint_col", AtomicType("SMALLINT")),
            DataField(3, "int_col", AtomicType("INT")),
            DataField(4, "bigint_col", AtomicType("BIGINT")),
            DataField(5, "float_col", AtomicType("FLOAT")),
            DataField(6, "double_col", AtomicType("DOUBLE")),
            DataField(7, "string_col", AtomicType("STRING")),
            DataField(8, "binary_col", AtomicType("BYTES")),
        ]
        data = pa.table({
            "bool_col": pa.array([True, False], type=pa.bool_()),
            "tinyint_col": pa.array([1, -1], type=pa.int8()),
            "smallint_col": pa.array([100, -100], type=pa.int16()),
            "int_col": pa.array([1000, -1000], type=pa.int32()),
            "bigint_col": pa.array([100000, -100000], type=pa.int64()),
            "float_col": pa.array([1.5, -2.5], type=pa.float32()),
            "double_col": pa.array([3.14, -2.71], type=pa.float64()),
            "string_col": pa.array(["hello", "world"], type=pa.string()),
            "binary_col": pa.array([b"\x01\x02", b"\x03\x04"], type=pa.binary()),
        })

        with tempfile.NamedTemporaryFile(suffix=".row", delete=False) as tmp:
            path = tmp.name

        try:
            _write_row_file(path, fields, data)
            result = _read_row_file(path, fields)
            assert result.column("bool_col").to_pylist() == [True, False]
            assert result.column("tinyint_col").to_pylist() == [1, -1]
            assert result.column("smallint_col").to_pylist() == [100, -100]
            assert result.column("int_col").to_pylist() == [1000, -1000]
            assert result.column("bigint_col").to_pylist() == [100000, -100000]
            assert result.column("float_col").to_pylist()[0] == pytest.approx(1.5)
            assert result.column("float_col").to_pylist()[1] == pytest.approx(-2.5)
            assert result.column("double_col").to_pylist() == [pytest.approx(3.14), pytest.approx(-2.71)]
            assert result.column("string_col").to_pylist() == ["hello", "world"]
            assert result.column("binary_col").to_pylist() == [b"\x01\x02", b"\x03\x04"]
        finally:
            os.unlink(path)

    def test_nulls(self):
        fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "name", AtomicType("STRING")),
        ]
        data = pa.table({
            "id": pa.array([1, None, 3], type=pa.int32()),
            "name": pa.array([None, "bob", None], type=pa.string()),
        })

        with tempfile.NamedTemporaryFile(suffix=".row", delete=False) as tmp:
            path = tmp.name

        try:
            _write_row_file(path, fields, data)
            result = _read_row_file(path, fields)
            assert result.column("id").to_pylist() == [1, None, 3]
            assert result.column("name").to_pylist() == [None, "bob", None]
        finally:
            os.unlink(path)

    def test_decimal(self):
        fields = [
            DataField(0, "d1", AtomicType("DECIMAL(10, 2)")),
            DataField(1, "d2", AtomicType("DECIMAL(20, 5)")),
        ]
        data = pa.table({
            "d1": pa.array([Decimal("123.45"), Decimal("-67.89")], type=pa.decimal128(10, 2)),
            "d2": pa.array([Decimal("12345.67890"), Decimal("-99999.12345")], type=pa.decimal128(20, 5)),
        })

        with tempfile.NamedTemporaryFile(suffix=".row", delete=False) as tmp:
            path = tmp.name

        try:
            _write_row_file(path, fields, data)
            result = _read_row_file(path, fields)
            assert result.column("d1").to_pylist() == [Decimal("123.45"), Decimal("-67.89")]
            assert result.column("d2").to_pylist() == [Decimal("12345.67890"), Decimal("-99999.12345")]
        finally:
            os.unlink(path)

    def test_timestamp(self):
        fields = [
            DataField(0, "ts_millis", AtomicType("TIMESTAMP(3)")),
            DataField(1, "ts_micros", AtomicType("TIMESTAMP(6)")),
        ]
        data = pa.table({
            "ts_millis": pa.array([1000, 2000], type=pa.timestamp('ms')),
            "ts_micros": pa.array([1000000, 2000000], type=pa.timestamp('us')),
        })

        with tempfile.NamedTemporaryFile(suffix=".row", delete=False) as tmp:
            path = tmp.name

        try:
            _write_row_file(path, fields, data)
            result = _read_row_file(path, fields)
            assert result.num_rows == 2
        finally:
            os.unlink(path)

    def test_array_type(self):
        element_type = AtomicType("INT")
        fields = [
            DataField(0, "arr", ArrayType(True, element_type)),
        ]
        data = pa.table({
            "arr": pa.array([[1, 2, 3], [4, 5]], type=pa.list_(pa.int32())),
        })

        with tempfile.NamedTemporaryFile(suffix=".row", delete=False) as tmp:
            path = tmp.name

        try:
            _write_row_file(path, fields, data)
            result = _read_row_file(path, fields)
            assert result.column("arr").to_pylist() == [[1, 2, 3], [4, 5]]
        finally:
            os.unlink(path)

    def test_map_type(self):
        fields = [
            DataField(0, "m", MapType(True, AtomicType("STRING"), AtomicType("INT"))),
        ]
        data = pa.table({
            "m": pa.array(
                [[("a", 1), ("b", 2)], [("c", 3)]],
                type=pa.map_(pa.string(), pa.int32())
            ),
        })

        with tempfile.NamedTemporaryFile(suffix=".row", delete=False) as tmp:
            path = tmp.name

        try:
            _write_row_file(path, fields, data)
            result = _read_row_file(path, fields)
            result_maps = result.column("m").to_pylist()
            assert len(result_maps) == 2
            assert len(result_maps[0]) == 2
            assert len(result_maps[1]) == 1
        finally:
            os.unlink(path)

    def test_nested_row(self):
        inner_type = RowType(True, [
            DataField(0, "x", AtomicType("INT")),
            DataField(1, "y", AtomicType("STRING")),
        ])
        fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "nested", inner_type),
        ]
        data = pa.table({
            "id": pa.array([1, 2], type=pa.int32()),
            "nested": pa.array(
                [{"x": 10, "y": "a"}, {"x": 20, "y": "b"}],
                type=pa.struct([
                    pa.field("x", pa.int32()),
                    pa.field("y", pa.string()),
                ])
            ),
        })

        with tempfile.NamedTemporaryFile(suffix=".row", delete=False) as tmp:
            path = tmp.name

        try:
            _write_row_file(path, fields, data)
            result = _read_row_file(path, fields)
            assert result.column("id").to_pylist() == [1, 2]
            nested = result.column("nested").to_pylist()
            assert nested[0] == {"x": 10, "y": "a"}
            assert nested[1] == {"x": 20, "y": "b"}
        finally:
            os.unlink(path)

    def test_multi_block(self):
        fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "data", AtomicType("STRING")),
        ]
        num_rows = 5000
        ids = list(range(num_rows))
        strings = [f"value_{i}" for i in range(num_rows)]
        data = pa.table({
            "id": pa.array(ids, type=pa.int32()),
            "data": pa.array(strings, type=pa.string()),
        })

        with tempfile.NamedTemporaryFile(suffix=".row", delete=False) as tmp:
            path = tmp.name

        try:
            with open(path, 'wb') as f:
                writer = FormatRowWriter(f, fields, block_size=4096)
                writer.write_table(data)
                writer.close()

            result = _read_row_file(path, fields)
            assert result.num_rows == num_rows
            assert result.column("id").to_pylist() == ids
            assert result.column("data").to_pylist() == strings
        finally:
            os.unlink(path)

    def test_empty_file(self):
        fields = [
            DataField(0, "id", AtomicType("INT")),
        ]
        data = pa.table({
            "id": pa.array([], type=pa.int32()),
        })

        with tempfile.NamedTemporaryFile(suffix=".row", delete=False) as tmp:
            path = tmp.name

        try:
            _write_row_file(path, fields, data)
            result = _read_row_file(path, fields)
            assert result.num_rows == 0
        finally:
            os.unlink(path)

    def test_column_projection(self):
        fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "name", AtomicType("STRING")),
            DataField(2, "value", AtomicType("DOUBLE")),
        ]
        data = pa.table({
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "name": pa.array(["a", "b", "c"], type=pa.string()),
            "value": pa.array([1.1, 2.2, 3.3], type=pa.float64()),
        })

        with tempfile.NamedTemporaryFile(suffix=".row", delete=False) as tmp:
            path = tmp.name

        try:
            _write_row_file(path, fields, data)
            result = _read_row_file(path, fields, read_field_names=["id", "value"])
            assert result.num_columns == 2
            assert result.column("id").to_pylist() == [1, 2, 3]
            assert result.column("value").to_pylist() == [pytest.approx(1.1), pytest.approx(2.2), pytest.approx(3.3)]
        finally:
            os.unlink(path)

    def test_date_and_time(self):
        fields = [
            DataField(0, "d", AtomicType("DATE")),
            DataField(1, "t", AtomicType("TIME")),
        ]
        data = pa.table({
            "d": pa.array([18000, 19000], type=pa.date32()),
            "t": pa.array([3600000, 7200000], type=pa.time32('ms')),
        })

        with tempfile.NamedTemporaryFile(suffix=".row", delete=False) as tmp:
            path = tmp.name

        try:
            _write_row_file(path, fields, data)
            result = _read_row_file(path, fields)
            assert result.num_rows == 2
        finally:
            os.unlink(path)

    def test_variant_type(self):
        fields = [
            DataField(0, "v", AtomicType("VARIANT")),
        ]
        data = pa.table({
            "v": pa.array(
                [{"value": b"\x01\x02", "metadata": b"\x03\x04"},
                 {"value": b"\x05", "metadata": b"\x06\x07\x08"}],
                type=pa.struct([
                    pa.field("value", pa.binary(), nullable=False),
                    pa.field("metadata", pa.binary(), nullable=False),
                ])
            ),
        })

        with tempfile.NamedTemporaryFile(suffix=".row", delete=False) as tmp:
            path = tmp.name

        try:
            _write_row_file(path, fields, data)
            result = _read_row_file(path, fields)
            variants = result.column("v").to_pylist()
            assert variants[0]["value"] == b"\x01\x02"
            assert variants[0]["metadata"] == b"\x03\x04"
            assert variants[1]["value"] == b"\x05"
            assert variants[1]["metadata"] == b"\x06\x07\x08"
        finally:
            os.unlink(path)

    def test_row_indices_random_access(self):
        """Test reading specific rows by index (O(1) row-number lookup)."""
        fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "name", AtomicType("VARCHAR")),
        ]
        data = pa.table({
            "id": pa.array(list(range(100)), type=pa.int32()),
            "name": pa.array([f"row_{i}" for i in range(100)]),
        })

        with tempfile.NamedTemporaryFile(suffix=".row", delete=False) as tmp:
            path = tmp.name

        try:
            _write_row_file(path, fields, data)

            # Read specific rows: 0, 5, 50, 99
            result = _read_row_file(path, fields, row_indices=[0, 5, 50, 99])
            assert result.num_rows == 4
            assert result.column("id").to_pylist() == [0, 5, 50, 99]
            assert result.column("name").to_pylist() == [
                "row_0", "row_5", "row_50", "row_99"
            ]

            # Read single row
            result = _read_row_file(path, fields, row_indices=[42])
            assert result.num_rows == 1
            assert result.column("id").to_pylist() == [42]

            # Read empty indices
            result = _read_row_file(path, fields, row_indices=[])
            assert result.num_rows == 0
        finally:
            os.unlink(path)

    def test_row_indices_multi_block(self):
        """Test row_indices across multiple blocks."""
        fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "value", AtomicType("VARCHAR")),
        ]
        # Write enough data to create multiple blocks (small block size)
        n_rows = 500
        data = pa.table({
            "id": pa.array(list(range(n_rows)), type=pa.int32()),
            "value": pa.array([f"val_{i}" * 10 for i in range(n_rows)]),
        })

        with tempfile.NamedTemporaryFile(suffix=".row", delete=False) as tmp:
            path = tmp.name

        try:
            with open(path, 'wb') as f:
                writer = FormatRowWriter(f, fields, block_size=1024)
                writer.write_table(data)
                writer.close()

            # Read rows from different blocks
            indices = [0, 100, 200, 300, 499]
            result = _read_row_file(path, fields, row_indices=indices)
            assert result.num_rows == 5
            assert result.column("id").to_pylist() == indices
        finally:
            os.unlink(path)

    def test_data_evolution_row_id_read(self):
        """Test Data Evolution scenario: partial-column write then row-id based read.

        Simulates the Data Evolution pattern where:
        1. First commit writes columns (f0, f1)
        2. Second commit writes column (f2) with first_row_id=0
        3. Read merges by row ID to reconstruct full rows
        """
        import shutil
        from pypaimon import CatalogFactory, Schema

        tempdir = tempfile.mkdtemp()
        try:
            warehouse = os.path.join(tempdir, 'warehouse')
            catalog = CatalogFactory.create({'warehouse': warehouse})
            catalog.create_database('default', True)

            pa_schema = pa.schema([
                ('f0', pa.int32()),
                ('f1', pa.string()),
                ('f2', pa.string()),
            ])

            schema = Schema.from_pyarrow_schema(
                pa_schema,
                options={
                    'file.format': 'row',
                    'row-tracking.enabled': 'true',
                    'data-evolution.enabled': 'true',
                })
            catalog.create_table('default.de_row_id_test', schema, False)
            table = catalog.get_table('default.de_row_id_test')

            # First commit: write (f0, f1)
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write().with_write_type(['f0', 'f1'])
            table_commit = write_builder.new_commit()

            data1 = pa.table({
                'f0': pa.array([1, 2, 3, 4, 5], type=pa.int32()),
                'f1': pa.array(['a1', 'a2', 'a3', 'a4', 'a5']),
            })
            table_write.write_arrow(data1)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

            # Second commit: write (f2) with first_row_id = 0
            table_write = write_builder.new_write().with_write_type(['f2'])
            table_commit = write_builder.new_commit()

            data2 = pa.table({
                'f2': pa.array(['b1', 'b2', 'b3', 'b4', 'b5']),
            })
            table_write.write_arrow(data2)
            cmts = table_write.prepare_commit()
            cmts[0].new_files[0].first_row_id = 0
            table_commit.commit(cmts)
            table_write.close()
            table_commit.close()

            # Read full table - should merge partial columns by row ID
            table = catalog.get_table('default.de_row_id_test')
            read_builder = table.new_read_builder()
            splits = read_builder.new_scan().plan().splits()
            result = read_builder.new_read().to_arrow(splits)

            assert result.num_rows == 5
            result_sorted = result.sort_by('f0')
            assert result_sorted.column('f0').to_pylist() == [1, 2, 3, 4, 5]
            assert result_sorted.column('f1').to_pylist() == [
                'a1', 'a2', 'a3', 'a4', 'a5'
            ]
            assert result_sorted.column('f2').to_pylist() == [
                'b1', 'b2', 'b3', 'b4', 'b5'
            ]
        finally:
            shutil.rmtree(tempdir, ignore_errors=True)
