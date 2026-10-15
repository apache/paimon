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

from unittest import mock

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.filesystem.hdfs_native_file_io import HdfsNativeFileIO
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO


vortex = pytest.importorskip("vortex")
OPTION = "vortex.compact.enabled"


@pytest.mark.parametrize("setting", [None, "false", "true"])
@pytest.mark.parametrize("mode", ["data", "primary_key", "data_evolution", "vector"])
@pytest.mark.python_write
def test_compact_table_option_and_round_trip(tmp_path, setting, mode):
    data = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "text": ["a long repeated string " * 20, None, "日本語"],
        "embed": pa.array([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]],
                          type=pa.list_(pa.float32(), 2)),
    })
    options = {"file.format": "vortex"}
    if setting is not None:
        options[OPTION] = setting
    if mode == "primary_key":
        options["bucket"] = "1"
    if mode in ("data_evolution", "vector"):
        options.update({"row-tracking.enabled": "true", "data-evolution.enabled": "true"})
    if mode == "vector":
        options["vector.file.format"] = "vortex"
    catalog = CatalogFactory.create({"warehouse": str(tmp_path)})
    catalog.create_database("default", True)
    catalog.create_table("default.data", Schema.from_pyarrow_schema(
        data.schema, options=options, primary_keys=["id"] if mode == "primary_key" else []), False)
    table = catalog.get_table("default.data")
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    with mock.patch("vortex.io.VortexWriteOptions", wraps=vortex.io.VortexWriteOptions) as presets:
        try:
            writer.write_arrow(data)
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
    paths = list(tmp_path.rglob("*.vortex"))
    assert len(paths) == (2 if mode == "vector" else 1)
    assert presets.compact.call_count == (len(paths) if setting == "true" else 0)
    reader = table.new_read_builder().with_projection(data.column_names)
    actual = reader.new_read().to_arrow(reader.new_scan().plan().splits())
    assert actual.to_pydict() == data.to_pydict()


@pytest.mark.parametrize("file_io_class", [LocalFileIO, PyArrowFileIO, HdfsNativeFileIO])
@pytest.mark.parametrize("store_kwargs", [None, {"endpoint": "https://storage.example"}])
def test_compact_preserves_path_and_store_options(tmp_path, file_io_class, store_kwargs):
    file_io = file_io_class.__new__(file_io_class)
    path = str(tmp_path / "data.vortex")
    data = pa.table({"id": [1, 2]})
    with mock.patch("pypaimon.read.reader.vortex_utils.to_vortex_specified",
                    return_value=(path, store_kwargs)), \
            mock.patch("vortex.store.from_url") as from_url, \
            mock.patch("vortex.io.VortexWriteOptions") as presets:
        file_io.write_vortex(path, data, compact=True)
    args, kwargs = presets.compact.return_value.write.call_args
    assert args[0].to_arrow_table().to_pydict() == data.to_pydict()
    assert args[1] == path
    if store_kwargs:
        from_url.assert_called_once_with(path, **store_kwargs)
        assert kwargs["store"] is from_url.return_value
    else:
        from_url.assert_not_called()
        assert kwargs["store"] is None


@pytest.mark.parametrize("file_io_class", [LocalFileIO, PyArrowFileIO, HdfsNativeFileIO])
def test_compact_write_failure_cleans_up(tmp_path, file_io_class):
    file_io = file_io_class.__new__(file_io_class)
    file_io.delete_quietly = mock.Mock()
    path = str(tmp_path / "data.vortex")
    with mock.patch("pypaimon.read.reader.vortex_utils.to_vortex_specified",
                    return_value=(path, None)), \
            mock.patch("vortex.io.VortexWriteOptions") as presets:
        presets.compact.return_value.write.side_effect = RuntimeError("write failed")
        with pytest.raises(RuntimeError, match="Failed to write Vortex file"):
            file_io.write_vortex(path, pa.table({"id": [1]}), compact=True)
    file_io.delete_quietly.assert_called_once_with(path)
