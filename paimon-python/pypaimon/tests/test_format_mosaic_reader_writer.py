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

import pyarrow as pa
import pytest

import mosaic
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.reader.format_mosaic_reader import FormatMosaicReader
from pypaimon.schema.data_types import AtomicType, DataField


class SimpleFileIO:
    """Minimal FileIO for testing."""

    def get_file_size(self, path):
        return os.path.getsize(path)

    def new_input_stream(self, path):
        return open(path, 'rb')


def _write_mosaic_file(path, data: pa.Table, options=None):
    with open(path, 'wb') as f:
        mosaic.write_table(data, f, options=options)


def _read_mosaic_file(path, read_fields, push_down_predicate=None,
                      row_group_predicate=None):
    file_io = SimpleFileIO()
    reader = FormatMosaicReader(file_io, path, read_fields,
                                push_down_predicate, batch_size=1024,
                                row_group_predicate=row_group_predicate)
    batches = []
    while True:
        batch = reader.read_arrow_batch()
        if batch is None:
            break
        batches.append(batch)
    reader.close()
    if not batches:
        return pa.table({f.name: pa.array([], type=pa.int32()) for f in read_fields})
    return pa.Table.from_batches(batches)


class TestFormatMosaicReaderWriter:

    def test_basic_int_string(self):
        fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "name", AtomicType("STRING")),
        ]
        data = pa.table({
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "name": pa.array(["alice", "bob", "charlie"], type=pa.string()),
        })

        with tempfile.NamedTemporaryFile(suffix=".mosaic", delete=False) as tmp:
            path = tmp.name

        try:
            _write_mosaic_file(path, data)
            result = _read_mosaic_file(path, fields)
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

        with tempfile.NamedTemporaryFile(suffix=".mosaic", delete=False) as tmp:
            path = tmp.name

        try:
            _write_mosaic_file(path, data)
            result = _read_mosaic_file(path, fields)
            assert result.column("bool_col").to_pylist() == [True, False]
            assert result.column("tinyint_col").to_pylist() == [1, -1]
            assert result.column("smallint_col").to_pylist() == [100, -100]
            assert result.column("int_col").to_pylist() == [1000, -1000]
            assert result.column("bigint_col").to_pylist() == [100000, -100000]
            assert result.column("float_col").to_pylist()[0] == pytest.approx(1.5)
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

        with tempfile.NamedTemporaryFile(suffix=".mosaic", delete=False) as tmp:
            path = tmp.name

        try:
            _write_mosaic_file(path, data)
            result = _read_mosaic_file(path, fields)
            assert result.column("id").to_pylist() == [1, None, 3]
            assert result.column("name").to_pylist() == [None, "bob", None]
        finally:
            os.unlink(path)

    def test_decimal(self):
        from decimal import Decimal

        fields = [
            DataField(0, "d1", AtomicType("DECIMAL(10, 2)")),
        ]
        data = pa.table({
            "d1": pa.array([Decimal("123.45"), Decimal("-67.89")], type=pa.decimal128(10, 2)),
        })

        with tempfile.NamedTemporaryFile(suffix=".mosaic", delete=False) as tmp:
            path = tmp.name

        try:
            _write_mosaic_file(path, data)
            result = _read_mosaic_file(path, fields)
            assert result.column("d1").to_pylist() == [Decimal("123.45"), Decimal("-67.89")]
        finally:
            os.unlink(path)

    def test_timestamp(self):
        fields = [
            DataField(0, "ts_millis", AtomicType("TIMESTAMP(3)")),
        ]
        data = pa.table({
            "ts_millis": pa.array([1000, 2000], type=pa.timestamp('ms')),
        })

        with tempfile.NamedTemporaryFile(suffix=".mosaic", delete=False) as tmp:
            path = tmp.name

        try:
            _write_mosaic_file(path, data)
            result = _read_mosaic_file(path, fields)
            assert result.num_rows == 2
        finally:
            os.unlink(path)

    def test_column_projection(self):
        data = pa.table({
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "name": pa.array(["a", "b", "c"], type=pa.string()),
            "value": pa.array([1.1, 2.2, 3.3], type=pa.float64()),
        })

        with tempfile.NamedTemporaryFile(suffix=".mosaic", delete=False) as tmp:
            path = tmp.name

        try:
            _write_mosaic_file(path, data)
            projected_fields = [
                DataField(0, "id", AtomicType("INT")),
                DataField(2, "value", AtomicType("DOUBLE")),
            ]
            result = _read_mosaic_file(path, projected_fields)
            assert result.num_columns == 2
            assert result.column("id").to_pylist() == [1, 2, 3]
            assert result.column("value").to_pylist() == [
                pytest.approx(1.1), pytest.approx(2.2), pytest.approx(3.3)]
        finally:
            os.unlink(path)

    def test_schema_evolution_missing_field(self):
        """Reading a file that doesn't have a column added later (schema evolution)."""
        data = pa.table({
            "id": pa.array([1, 2], type=pa.int32()),
            "name": pa.array(["a", "b"], type=pa.string()),
        })

        with tempfile.NamedTemporaryFile(suffix=".mosaic", delete=False) as tmp:
            path = tmp.name

        try:
            _write_mosaic_file(path, data)
            fields_read = [
                DataField(0, "id", AtomicType("INT")),
                DataField(1, "name", AtomicType("STRING")),
                DataField(2, "score", AtomicType("DOUBLE")),
            ]
            result = _read_mosaic_file(path, fields_read)
            assert result.column("id").to_pylist() == [1, 2]
            assert result.column("name").to_pylist() == ["a", "b"]
            assert result.column("score").to_pylist() == [None, None]
        finally:
            os.unlink(path)

    def test_predicate_pushdown(self):
        import pyarrow.compute as pc

        fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "name", AtomicType("STRING")),
        ]
        data = pa.table({
            "id": pa.array(list(range(100)), type=pa.int32()),
            "name": pa.array([f"user_{i}" for i in range(100)], type=pa.string()),
        })

        with tempfile.NamedTemporaryFile(suffix=".mosaic", delete=False) as tmp:
            path = tmp.name

        try:
            _write_mosaic_file(path, data)
            predicate = pc.field("id") > 95
            result = _read_mosaic_file(path, fields, push_down_predicate=predicate)
            assert result.num_rows == 4
            assert all(v > 95 for v in result.column("id").to_pylist())
        finally:
            os.unlink(path)

    def test_predicate_skips_row_groups_by_stats(self, monkeypatch):
        import pyarrow.compute as pc

        fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "name", AtomicType("STRING")),
        ]
        num_rows = 5000
        data = pa.table({
            "id": pa.array(list(range(num_rows)), type=pa.int32()),
            "name": pa.array([f"user_{i}" for i in range(num_rows)], type=pa.string()),
        })

        with tempfile.NamedTemporaryFile(suffix=".mosaic", delete=False) as tmp:
            path = tmp.name

        original_from_input_file = mosaic.MosaicReader.from_input_file
        readers = []

        class CountingReader:
            def __init__(self, reader):
                self.reader = reader
                self.read_row_groups = []

            def __getattr__(self, name):
                return getattr(self.reader, name)

            def read_row_group(self, row_group_index):
                self.read_row_groups.append(row_group_index)
                return self.reader.read_row_group(row_group_index)

        def from_input_file(read_at, file_length):
            reader = CountingReader(original_from_input_file(read_at, file_length))
            readers.append(reader)
            return reader

        monkeypatch.setattr(
            mosaic.MosaicReader, "from_input_file", staticmethod(from_input_file))

        try:
            options = mosaic.WriterOptions(
                compression=mosaic.WriterOptions.COMPRESSION_NONE,
                num_buckets=1,
                row_group_max_size=1024,
                stats_columns=["id"])
            with open(path, 'wb') as f:
                with mosaic.MosaicWriter(f, data.schema, options) as writer:
                    for batch in data.to_batches(max_chunksize=250):
                        writer.write(batch)

            predicate = PredicateBuilder(fields).greater_than("id", num_rows + 1)
            result = _read_mosaic_file(
                path,
                fields,
                push_down_predicate=pc.field("id") > num_rows + 1,
                row_group_predicate=predicate)

            assert result.num_rows == 0
            assert len(readers) == 1
            assert readers[0].num_row_groups > 1
            assert readers[0].read_row_groups == []
        finally:
            os.unlink(path)

    def test_predicate_skips_non_matching_row_groups_by_stats(self, monkeypatch):
        import pyarrow.compute as pc

        fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "name", AtomicType("STRING")),
        ]
        num_rows = 5000
        data = pa.table({
            "id": pa.array(list(range(num_rows)), type=pa.int32()),
            "name": pa.array([f"user_{i}" for i in range(num_rows)], type=pa.string()),
        })

        with tempfile.NamedTemporaryFile(suffix=".mosaic", delete=False) as tmp:
            path = tmp.name

        original_from_input_file = mosaic.MosaicReader.from_input_file
        readers = []

        class CountingReader:
            def __init__(self, reader):
                self.reader = reader
                self.read_row_groups = []

            def __getattr__(self, name):
                return getattr(self.reader, name)

            def read_row_group(self, row_group_index):
                self.read_row_groups.append(row_group_index)
                return self.reader.read_row_group(row_group_index)

        def from_input_file(read_at, file_length):
            reader = CountingReader(original_from_input_file(read_at, file_length))
            readers.append(reader)
            return reader

        monkeypatch.setattr(
            mosaic.MosaicReader, "from_input_file", staticmethod(from_input_file))

        try:
            options = mosaic.WriterOptions(
                compression=mosaic.WriterOptions.COMPRESSION_NONE,
                num_buckets=1,
                row_group_max_size=1024,
                stats_columns=["id"])
            with open(path, 'wb') as f:
                with mosaic.MosaicWriter(f, data.schema, options) as writer:
                    for batch in data.to_batches(max_chunksize=250):
                        writer.write(batch)

            predicate = PredicateBuilder(fields).greater_than("id", 4900)
            result = _read_mosaic_file(
                path,
                fields,
                push_down_predicate=pc.field("id") > 4900,
                row_group_predicate=predicate)

            assert result.column("id").to_pylist() == list(range(4901, num_rows))
            assert len(readers) == 1
            assert readers[0].num_row_groups > 1
            assert 0 not in readers[0].read_row_groups
            assert len(readers[0].read_row_groups) < readers[0].num_row_groups
        finally:
            os.unlink(path)

    def test_large_dataset(self):
        fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "data", AtomicType("STRING")),
        ]
        num_rows = 10000
        data = pa.table({
            "id": pa.array(list(range(num_rows)), type=pa.int32()),
            "data": pa.array([f"value_{i}" for i in range(num_rows)], type=pa.string()),
        })

        with tempfile.NamedTemporaryFile(suffix=".mosaic", delete=False) as tmp:
            path = tmp.name

        try:
            _write_mosaic_file(path, data)
            result = _read_mosaic_file(path, fields)
            assert result.num_rows == num_rows
            assert result.column("id").to_pylist() == list(range(num_rows))
        finally:
            os.unlink(path)

    def test_write_mosaic_local_file_io(self):
        """Test write_mosaic via LocalFileIO."""
        from pypaimon.filesystem.local_file_io import LocalFileIO

        data = pa.table({
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "name": pa.array(["a", "b", "c"], type=pa.string()),
        })

        with tempfile.NamedTemporaryFile(suffix=".mosaic", delete=False) as tmp:
            path = tmp.name

        try:
            file_io = LocalFileIO({})
            file_io.write_mosaic(path, data)

            assert os.path.getsize(path) > 0

            fields = [
                DataField(0, "id", AtomicType("INT")),
                DataField(1, "name", AtomicType("STRING")),
            ]
            result = _read_mosaic_file(path, fields)
            assert result.column("id").to_pylist() == [1, 2, 3]
            assert result.column("name").to_pylist() == ["a", "b", "c"]
        finally:
            os.unlink(path)
