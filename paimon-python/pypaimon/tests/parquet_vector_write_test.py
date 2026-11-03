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

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon.execution.python.write.writer.single_file_writer import SingleFileWriter
from pypaimon.filesystem.local_file_io import LocalFileIO


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
