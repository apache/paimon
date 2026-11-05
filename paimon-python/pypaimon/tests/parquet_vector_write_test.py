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

import inspect
from decimal import Decimal

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from pypaimon.execution.python.write.writer.single_file_writer import SingleFileWriter
from pypaimon.execution.python.read.reader.format_pyarrow_reader import FormatPyArrowReader
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.schema.data_types import PyarrowFieldParser


@pytest.mark.parametrize("wrapper", ["vector", "row", "array", "map"])
def test_streamed_nullable_vectors_preserve_values_and_metadata(tmp_path, wrapper):
    vector = pa.list_(pa.field("element", pa.float32(), metadata={b"child": b"retained"}), 2)
    if wrapper == "vector":
        data_type, values = vector, [[1., 2.], None, [3., 4.]]
    elif wrapper == "row":
        data_type = pa.struct([pa.field("vector", vector, metadata={b"nested": b"retained"})])
        values = [{"vector": [1., 2.]}, {"vector": None}, None]
    elif wrapper == "array":
        data_type = pa.list_(vector)
        values = [[[1., 2.]], [None, [3., 4.]], None]
    else:
        data_type = pa.map_(pa.string(), vector, keys_sorted=True)
        values = [[("a", [1., 2.])], [("b", None)], None]
    schema = pa.schema([pa.field("feature", data_type, metadata={b"field": b"retained"})],
                       metadata={b"dataset": b"retained"})
    data = pa.Table.from_arrays([pa.array(values, type=data_type)], schema=schema)
    path = str(tmp_path / "vectors.parquet")
    writer = SingleFileWriter(LocalFileIO.create(), path, schema, "parquet", "zstd", 1, [], None)
    try:
        # The schema is fixed while the first batch has no NULL vector. A later
        # batch must remain readable without losing any schema/field metadata.
        writer.write(data.slice(0, 1))
        writer.write(data.slice(1))
    finally:
        writer.close()

    physical = pq.read_table(path)
    assert physical.schema.metadata == schema.metadata
    assert physical.schema.field("feature").metadata == schema.field("feature").metadata
    assert physical.cast(schema).to_pylist() == data.to_pylist()
    if wrapper == "vector":
        assert physical.schema.field("feature").type.value_field.metadata == {b"child": b"retained"}
    elif wrapper == "row":
        assert physical.schema.field("feature").type.field("vector").metadata == {b"nested": b"retained"}


@pytest.mark.parametrize("wrapper", ["vector", "row", "array", "map"])
@pytest.mark.parametrize("selection", ["all", "filter", "rows"])
@pytest.mark.parametrize("compliant_lists", [False, True])
def test_read_nullable_vectors_from_fixed_size_list_parquet_metadata(tmp_path, wrapper, selection, compliant_lists):
    vector = pa.list_(pa.field("component", pa.float32(), metadata={b"child": b"retained"}), 2)
    if wrapper == "vector":
        data_type, values = vector, [[1., 2.], None, [3., 4.]]
    elif wrapper == "row":
        data_type = pa.struct([pa.field("vector", vector)])
        values = [{"vector": [1., 2.]}, {"vector": None}, None]
    elif wrapper == "array":
        data_type = pa.list_(vector)
        values = [[[1., 2.]], [[3., 4.]], None]
    else:
        data_type = pa.map_(pa.string(), vector, keys_sorted=True)
        values = [[("a", [1., 2.])], [("b", None)], None]
    schema = pa.schema([pa.field("id", pa.int64()),
                        pa.field("feature", data_type, metadata={b"PARQUET:field_id": b"7"})])
    data = pa.Table.from_arrays([pa.array([0, 1, 2]), pa.array(values, type=data_type)], schema=schema)
    path = str(tmp_path / "fixed-size.parquet")
    # Model native/older writers that preserve fixed-size Arrow metadata.
    pq.write_table(data, path, row_group_size=2, use_compliant_nested_type=compliant_lists)
    reader = FormatPyArrowReader(
        LocalFileIO.create(), "parquet", path, PyarrowFieldParser.to_paimon_schema(schema),
        ds.field("id") >= 1 if selection == "filter" else None,
        batch_size=1, row_indices=[1, 2] if selection == "rows" else None)
    try:
        batches = []
        while True:
            batch = reader.read_arrow_batch()
            if batch is None:
                break
            assert batch.num_rows <= 1
            batches.append(batch)
    finally:
        reader.close()
    expected = data if selection == "all" else data.slice(1)
    assert pa.Table.from_batches(batches).to_pylist() == expected.to_pylist()


@pytest.mark.parametrize("physical_options", [
    {"use_deprecated_int96_timestamps": True},
    {"store_decimal_as_integer": True},
    {"use_deprecated_int96_timestamps": True, "store_decimal_as_integer": True},
])
def test_vector_footer_override_preserves_timestamp_and_decimal_encodings(tmp_path, physical_options):
    if any(name not in inspect.signature(pq.ParquetWriter).parameters for name in physical_options):
        pytest.skip("Parquet physical encoding option is unavailable in this PyArrow version")
    schema = pa.schema([("feature", pa.list_(pa.float32(), 2)),
                        ("time", pa.timestamp("us")), ("amount", pa.decimal128(12, 2))])
    data = pa.Table.from_arrays([
        pa.array([[1., 2.], None, [3., 4.]], type=schema.field("feature").type),
        pa.array([0, 1000, 2000], type=schema.field("time").type),
        pa.array([Decimal("1.25"), None, Decimal("3.50")], type=schema.field("amount").type),
    ], schema=schema)
    path = str(tmp_path / "physical-encodings.parquet")
    pq.write_table(data, path, row_group_size=2, **physical_options)
    reader = FormatPyArrowReader(
        LocalFileIO.create(), "parquet", path, PyarrowFieldParser.to_paimon_schema(schema), None, batch_size=1)
    try:
        batches = []
        while True:
            batch = reader.read_arrow_batch()
            if batch is None:
                break
            batches.append(batch)
    finally:
        reader.close()
    assert pa.Table.from_batches(batches).to_pylist() == data.to_pylist()
