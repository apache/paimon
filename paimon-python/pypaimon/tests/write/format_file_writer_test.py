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
from unittest.mock import patch
from urllib.parse import urlparse

import pyarrow as pa
import pytest

from pypaimon.execution.python.read.reader.format_blob_reader import FormatBlobReader
from pypaimon.execution.python.read.reader.format_row_reader import FormatRowReader
from pypaimon.execution.python.write.format_file_writer import write_blob_file, write_row_file
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.filesystem.resolving_file_io import ResolvingFileIO
from pypaimon.common.options import Options
from pypaimon.schema.data_types import AtomicType, DataField


class TrackingFileIO(LocalFileIO):
    def __init__(self):
        super().__init__()
        self.outputs = []
        self.deleted = []

    def new_output_stream(self, path):
        stream = super().new_output_stream(path)
        self.outputs.append(stream)
        return stream

    def delete_quietly(self, path):
        self.deleted.append(path)
        return super().delete_quietly(path)


def _format(format_name):
    if format_name == 'row':
        return (write_row_file, pa.table({'value': pa.array([1, None, 3], pa.int32())}),
                DataField(0, 'value', AtomicType('INT')), FormatRowReader)
    return (write_blob_file, pa.table({'value': [b'first', None, b'last']},
                                      schema=pa.schema([('value', pa.large_binary())])),
            DataField(0, 'value', AtomicType('BLOB')), FormatBlobReader)


@pytest.mark.parametrize('format_name', ['row', 'blob'])
@pytest.mark.parametrize('resolving', [False, True])
def test_python_format_uses_file_io_streams_and_preserves_contents(tmp_path, format_name, resolving):
    file_io = ResolvingFileIO(Options({})) if resolving else TrackingFileIO()
    write, expected, field, reader_type = _format(format_name)
    path = (tmp_path / 'nested' / ('data.' + format_name)).as_uri()
    write(file_io, path, expected)
    if not resolving:
        assert len(file_io.outputs) == 1
        assert file_io.outputs[0].closed
        assert not file_io.deleted
    reader_options = {'blob_as_descriptor': False} if format_name == 'blob' else {}
    reader = reader_type(file_io, path, ['value'], [field], None, **reader_options)
    try:
        batches = []
        while True:
            batch = reader.read_arrow_batch()
            if batch is None:
                break
            batches.append(batch)
        assert pa.Table.from_batches(batches).to_pydict() == expected.to_pydict()
    finally:
        reader.close()
        file_io.close()


@pytest.mark.parametrize('format_name,method', [
    ('row', 'FormatRowWriter.write_table'),
    ('blob', 'BlobFormatWriter.write_value'),
])
def test_python_format_closes_output_and_removes_failed_unpublished_file(tmp_path, format_name, method):
    file_io = TrackingFileIO()
    write, data, _, _ = _format(format_name)
    path = os.fspath(tmp_path / ('failed.' + format_name))
    failure = OSError('encoding failed')
    with patch('pypaimon.execution.python.write.format_file_writer.' + method, side_effect=failure):
        with pytest.raises(RuntimeError, match='encoding failed') as raised:
            write(file_io, path, data)
    assert raised.value.__cause__ is failure
    assert len(file_io.outputs) == 1
    assert file_io.outputs[0].closed
    assert file_io.deleted == [path]
    assert not file_io.exists(path)


@pytest.mark.parametrize('wrapper', [
    'plain', 'cached', 'resolving', 'cached-resolving', 'rest', 'cached-rest',
])
@pytest.mark.parametrize('destination_scheme', ['file', 'oss'])
def test_blob_descriptor_input_routes_by_source_uri(tmp_path, wrapper, destination_scheme):
    from pypaimon.catalog.rest.rest_token_file_io import RESTTokenFileIO
    from pypaimon.common.file_io import FileIO
    from pypaimon.common.identifier import Identifier
    from pypaimon.filesystem.caching_file_io import CachingFileIO, LocalMemoryCacheManager
    from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO
    from pypaimon.table.row.blob import BlobDescriptor

    source = tmp_path / 'source-bucket' / 'shared' / 'source.bin'
    source.parent.mkdir(parents=True)
    source.write_bytes(b'prefix-payload-suffix')
    destination = tmp_path / 'table-bucket'
    (destination / 'table').mkdir(parents=True)
    (destination / 'shared').mkdir()
    # An authority-routing error must not silently copy this colliding key.
    (destination / 'shared' / 'source.bin').write_bytes(b'prefix-incorrect-suffix')
    descriptor = BlobDescriptor('oss://source-bucket/shared/source.bin', 7, 7)
    data = pa.table({'value': [descriptor.serialize(), None]},
                    schema=pa.schema([('value', pa.large_binary())]))
    target = ('oss://table-bucket/table/result.blob' if destination_scheme == 'oss'
              else (destination / 'table' / 'result.blob').as_uri())

    def local_bucket_filesystem(file_io, path):
        file_io._use_jindo = True
        bucket_dir = tmp_path / urlparse(path).netloc
        return pa.fs.SubTreeFileSystem(os.fspath(bucket_dir), pa.fs.LocalFileSystem())

    # Use ordinary FileIO URI resolution and path conversion, with each OSS
    # bucket backed by a real, separate directory instead of a network service.
    with patch('pypaimon.filesystem.pyarrow_file_io.JINDO_AVAILABLE', True), \
            patch.object(PyArrowFileIO, '_initialize_jindo_fs', local_bucket_filesystem), \
            patch.object(RESTTokenFileIO, 'try_to_refresh_token'):
        options = Options({})
        if 'resolving' in wrapper:
            file_io = ResolvingFileIO(options)
        elif 'rest' in wrapper:
            file_io = RESTTokenFileIO(Identifier('db', 'table'), target, options)
        else:
            file_io = FileIO.get(target, options)
        if wrapper.startswith('cached'):
            file_io = CachingFileIO(file_io, LocalMemoryCacheManager(1024))
        write_blob_file(file_io, target, data)
        field = DataField(0, 'value', AtomicType('BLOB'))
        reader = FormatBlobReader(file_io, target, ['value'], [field], None, False)
        try:
            assert reader.read_arrow_batch().column(0).to_pylist() == [b'payload', None]
        finally:
            reader.close()
            file_io.close()


@pytest.mark.parametrize('denied', [False, True])
def test_explicit_source_file_io_preserves_token_refresh_and_denial(tmp_path, denied):
    from unittest.mock import Mock

    from pypaimon.catalog.rest.rest_token_file_io import RESTTokenFileIO
    from pypaimon.common.file_io import FileIO
    from pypaimon.common.identifier import Identifier
    from pypaimon.common.uri_reader import UriReaderFactory
    from pypaimon.execution.python.write.writer.blob_file_writer import BlobFileWriter
    from pypaimon.table.row.blob import BlobDescriptor

    source = tmp_path / 'source.bin'
    source.write_bytes(b'payload')
    descriptor = BlobDescriptor(source.as_uri(), 0, 7)
    data = pa.table({'value': [descriptor.serialize()]},
                    schema=pa.schema([('value', pa.large_binary())]))
    target = (tmp_path / 'result.blob').as_uri()
    initial, refreshed = LocalFileIO(), LocalFileIO()
    initial.new_input_stream = Mock(side_effect=AssertionError('stale credentials were used'))
    if denied:
        refreshed.new_input_stream = Mock(side_effect=PermissionError('source access denied'))
    else:
        refreshed.new_input_stream = Mock(wraps=refreshed.new_input_stream)
    source_io = RESTTokenFileIO(Identifier('db', 'source'), tmp_path.as_uri(), Options({}))
    factory = UriReaderFactory.from_file_io(source_io)
    with patch.object(source_io, 'file_io', side_effect=[initial, refreshed]) as resolve, \
            patch.object(FileIO, 'get', side_effect=AssertionError('token wrapper was bypassed')):
        # Prime source credentials before opening the lazy descriptor, as a
        # configured source table does before handing its FileIO to a writer.
        source_io.file_io()
        writer = BlobFileWriter(LocalFileIO(), target, uri_reader_factory=factory)
        try:
            if denied:
                with pytest.raises(IOError, match='source access denied'):
                    writer.write_row(data)
            else:
                writer.write_row(data)
        finally:
            writer.close()
        assert resolve.call_count == 2
    initial.new_input_stream.assert_not_called()
    refreshed.new_input_stream.assert_called_once_with(source.as_uri())
    if not denied:
        field = DataField(0, 'value', AtomicType('BLOB'))
        # Inspect physical contents through a separate reader after proving
        # descriptor materialization used the refreshed token wrapper.
        reader = FormatBlobReader(LocalFileIO(), target, ['value'], [field], None, False)
        try:
            assert reader.read_arrow_batch().column(0).to_pylist() == [b'payload']
        finally:
            reader.close()
