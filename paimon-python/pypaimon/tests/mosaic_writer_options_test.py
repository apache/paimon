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

import sys
import types

import pyarrow as pa
import pytest

from pypaimon.common.identifier import Identifier
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.schema.table_schema import TableSchema
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.execution.python.write.writer.append_only_data_writer import AppendOnlyDataWriter
from pypaimon.execution.python.write.writer.mosaic_writer_options import create_mosaic_writer_options


class _FakeWriterOptions:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


class _FileIO:
    def __init__(self):
        self.mosaic_options = None

    def write_mosaic(self, path, data, **kwargs):
        self.mosaic_options = kwargs.get("options")

    def get_file_size(self, path):
        return 1

    def delete_quietly(self, path):
        pass


def test_create_mosaic_writer_options_maps_table_options(monkeypatch):
    fake_mosaic = types.SimpleNamespace(WriterOptions=_FakeWriterOptions)
    monkeypatch.setitem(sys.modules, "mosaic", fake_mosaic)
    options = CoreOptions.from_dict({
        "file.compression.zstd-level": "5",
        "file.block-size": "64kb",
        "mosaic.num-buckets": "8",
        "mosaic.stats-columns": " id, name ,,",
    })

    writer_options = create_mosaic_writer_options(options)

    assert writer_options.kwargs == {
        "zstd_level": 5,
        "num_buckets": 8,
        "row_group_max_size": 64 * 1024,
        "stats_columns": ["id", "name"],
    }


def test_create_mosaic_writer_options_rejects_non_zstd_compression(monkeypatch):
    fake_mosaic = types.SimpleNamespace(WriterOptions=_FakeWriterOptions)
    monkeypatch.setitem(sys.modules, "mosaic", fake_mosaic)
    options = CoreOptions.from_dict({"file.compression": "snappy"})

    with pytest.raises(ValueError, match="Mosaic format only supports zstd compression"):
        create_mosaic_writer_options(options)


def test_write_mosaic_local_file_io_passes_writer_options(monkeypatch, tmp_path):
    captured = {}
    fake_mosaic = types.SimpleNamespace(
        write_table=lambda data, output_stream, options=None: captured.update(
            data=data, options=options))
    monkeypatch.setitem(sys.modules, "mosaic", fake_mosaic)

    data = pa.table({"id": pa.array([1], type=pa.int32())})
    writer_options = object()
    file_io = LocalFileIO({})

    file_io.write_mosaic(str(tmp_path / "data.mosaic"), data, options=writer_options)

    assert captured["data"] is data
    assert captured["options"] is writer_options


def test_data_writer_passes_mosaic_writer_options(monkeypatch, tmp_path):
    fake_mosaic = types.SimpleNamespace(WriterOptions=_FakeWriterOptions)
    monkeypatch.setitem(sys.modules, "mosaic", fake_mosaic)
    schema = TableSchema(fields=[DataField(0, "id", AtomicType("INT"))], options={
        "file.format": "mosaic",
        "mosaic.num-buckets": "4",
        "mosaic.stats-columns": "id",
    })
    file_io = _FileIO()
    table = FileStoreTable(file_io, Identifier("db", "mosaic_table"), str(tmp_path), schema)
    writer = AppendOnlyDataWriter(table, (), 0, 0, table.options)

    writer.write(pa.record_batch({"id": pa.array([1], type=pa.int32())}))
    writer.prepare_commit()

    assert isinstance(file_io.mosaic_options, _FakeWriterOptions)
    assert file_io.mosaic_options.kwargs["num_buckets"] == 4
    assert file_io.mosaic_options.kwargs["stats_columns"] == ["id"]
