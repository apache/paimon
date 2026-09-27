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

import io
from unittest.mock import patch

import pytest

from pypaimon.common.options.options import Options
from pypaimon.filesystem.caching_file_io import CachingFileIO, CachingInputStream
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.read.reader.format_blob_reader import (
    FormatBlobReader, _BLOB_INDEX_CACHE, _BLOB_INDEX_CACHE_LOCK,
)
from pypaimon.schema.data_types import ArrayType, AtomicType, DataField, MapType
from pypaimon.table.row.blob import BlobData, BlobDescriptor
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.utils.file_type import FileType
from pypaimon.write.blob_format_writer import BlobFormatWriter


@pytest.mark.parametrize('disk', [False, True], ids=['memory', 'disk'])
@pytest.mark.parametrize('kind', ['scalar', 'array', 'map'])
def test_blob_metadata_ranges_exclude_values(tmp_path, disk, kind):
    payload = b'body' * 4096
    blob = AtomicType('BLOB')
    types = {'scalar': blob, 'array': ArrayType(True, blob),
             'map': MapType(True, AtomicType('STRING'), blob)}
    values = {'scalar': BlobData(payload),
              'array': [BlobData(payload), None, BlobData(b'')],
              'map': [('key', BlobData(payload)), ('missing', None), ('empty', BlobData(b''))]}
    field = DataField(0, 'value', types[kind])
    path = str(tmp_path / 'data.blob')
    writer = BlobFormatWriter(open(path, 'wb'))
    writer.add_element(GenericRow([values[kind]], [field]))
    writer.close()
    contents = (tmp_path / 'data.blob').read_bytes()
    options = Options({'local-cache.enabled': 'true', 'local-cache.max-size': '1 mb',
                       **({'local-cache.dir': str(tmp_path / 'cache')} if disk else {})})
    delegate = LocalFileIO(str(tmp_path), Options({}))
    cache = CachingFileIO.create_cache_manager(options)
    file_io = CachingFileIO.wrap_with_caching_if_needed(delegate, options, cache)
    reads = []

    class CountingStream(io.BytesIO):
        def read(self, size=-1):
            start = self.tell()
            result = super().read(size)
            reads.append((start, start + len(result)))
            return result

    def read(descriptors=True):
        # Isolate the byte cache from the existing parsed-index cache.
        with _BLOB_INDEX_CACHE_LOCK:
            _BLOB_INDEX_CACHE.clear()
        reader = FormatBlobReader(file_io, path, ['value'], [field], None, descriptors,
                                  file_size=len(contents))
        try:
            return reader.read_arrow_batch().column(0)[0].as_py()
        finally:
            reader.close()

    with patch.object(delegate, 'new_input_stream', side_effect=lambda _: CountingStream(contents)):
        first = read()
        serialized = ([first] if kind == 'scalar' else first if kind == 'array'
                      else [value for _, value in first])
        descriptors = [BlobDescriptor.deserialize(value) for value in serialized if value is not None]
        assert reads
        for descriptor in descriptors:
            if descriptor.length:
                assert all(end <= descriptor.offset or start >= descriptor.offset + descriptor.length
                           for start, end in reads)
        retained = cache._current_size
        assert retained > 0
        reads.clear()
        if disk:
            # Reopen the disk manager to verify persistence, not just memory hits.
            cache = CachingFileIO.create_cache_manager(options)
            file_io = CachingFileIO.wrap_with_caching_if_needed(delegate, options, cache)
        assert read() == first
        assert reads == []
        materialized = read(False)
        expected = {'scalar': payload, 'array': [payload, None, b''],
                    'map': [('key', payload), ('missing', None), ('empty', b'')]}[kind]
        assert materialized == expected
        assert reads  # Values still come from the delegate.
        assert cache._current_size == retained


def test_blob_meta_is_a_read_category_not_a_file_type(tmp_path):
    delegate = LocalFileIO(str(tmp_path), Options({}))
    options = Options({'local-cache.enabled': 'true', 'local-cache.whitelist': 'blob-meta'})
    cache = CachingFileIO.create_cache_manager(options)
    file_io = CachingFileIO.wrap_with_caching_if_needed(delegate, options, cache)
    assert FileType.classify('data.blob') == FileType.DATA
    assert FileType.parse_whitelist('blob-meta') == {FileType.BLOB_META}
    for name in ('data.parquet', '.data.blob.uuid.tmp'):
        (tmp_path / name).write_bytes(b'data')
        with file_io.new_input_stream(str(tmp_path / name)) as stream:
            assert stream.read() == b'data'
        assert cache._current_size == 0


@pytest.mark.parametrize('disk', [False, True])
def test_metadata_cache_budget_and_data_cache_coexist(tmp_path, disk):
    path = str(tmp_path / 'data.blob')
    (tmp_path / 'data.blob').write_bytes(b'abcdefgh')
    delegate = LocalFileIO(str(tmp_path), Options({}))
    options = Options({'local-cache.enabled': 'true', 'local-cache.whitelist': 'blob-meta',
                       'local-cache.max-size': '4096 b', 'local-cache.block-size': '4 b',
                       **({'local-cache.dir': str(tmp_path / 'cache')} if disk else {})})
    cache = CachingFileIO.create_cache_manager(options)
    file_io = CachingFileIO.wrap_with_caching_if_needed(delegate, options, cache)
    with file_io.new_input_stream(path) as stream:
        assert stream.read_blob_metadata(4) == b'abcd'
        assert stream.read_blob_metadata(4) == b'efgh'
        assert 0 < cache._current_size <= 4096
        retained = cache._current_size
        assert stream.read_blob_metadata(4) == b''  # Never cache a short read.
        assert cache._current_size == retained
        stream.seek(0)
        assert stream.read_blob_metadata(4) == b'abcd'
        assert 0 < cache._current_size <= 4096
    # Reusing a manager for normal data blocks cannot hit metadata range entries.
    data_io = CachingFileIO(delegate, cache, {FileType.DATA, FileType.BLOB_META})
    with data_io.new_input_stream(path) as stream:
        assert type(stream) is CachingInputStream
        assert stream.read() == b'abcdefgh'


@pytest.mark.parametrize('disk', [False, True], ids=['memory', 'disk'])
@pytest.mark.parametrize('kind', ['array', 'map'])
def test_many_blob_rows_respect_cache_budget(tmp_path, disk, kind):
    blob = AtomicType('BLOB')
    field = DataField(0, 'value', ArrayType(True, blob) if kind == 'array'
                      else MapType(True, AtomicType('STRING'), blob))
    value = [BlobData(b'x')] if kind == 'array' else [('key', BlobData(b'x'))]
    path = str(tmp_path / 'data.blob')
    writer = BlobFormatWriter(open(path, 'wb'))
    for _ in range(1000):
        writer.add_element(GenericRow([value], [field]))
    writer.close()
    budget = 64 * 1024
    options = Options({'local-cache.enabled': 'true', 'local-cache.max-size': '64 kb',
                       **({'local-cache.dir': str(tmp_path / 'cache')} if disk else {})})
    cache = CachingFileIO.create_cache_manager(options)
    delegate = LocalFileIO(str(tmp_path), Options({}))
    file_io = CachingFileIO.wrap_with_caching_if_needed(delegate, options, cache)
    reader = FormatBlobReader(file_io, path, ['value'], [field], None, True)
    try:
        batch = reader.read_arrow_batch()
        assert batch.num_rows == 1000
        for row in batch.column(0).to_pylist():
            raw = row[0] if kind == 'array' else row[0][1]
            descriptor = BlobDescriptor.deserialize(raw)
            with open(path, 'rb') as stream:
                stream.seek(descriptor.offset)
                assert stream.read(descriptor.length) == b'x'
    finally:
        reader.close()
    assert 0 < cache._current_size <= budget
    entries = cache._entry_index if disk else cache._cache
    assert 0 < len(entries) <= budget // (4096 if disk else 512)
    if disk:
        files = [p for p in (tmp_path / 'cache').rglob('*') if p.is_file()]
        assert len(files) == len(entries)
        assert sum(p.stat().st_blocks * 512 for p in files) <= budget
        reopened = CachingFileIO.create_cache_manager(options)
        assert reopened._current_size == cache._current_size
        assert len(reopened._entry_index) == len(entries)


@pytest.mark.parametrize('disk', [False, True], ids=['memory', 'disk'])
def test_tiny_ranges_respect_entry_limit_and_lru(tmp_path, disk):
    options = Options({'local-cache.enabled': 'true', 'local-cache.max-size': '1 gb',
                       **({'local-cache.dir': str(tmp_path / 'cache')} if disk else {})})
    cache = CachingFileIO.create_cache_manager(options)
    with patch('pypaimon.filesystem.caching_file_io._MAX_CACHE_ENTRIES', 32):
        for offset in range(32):
            cache.put_block('data.blob', ('blob-meta', offset, 1), b'x')
        assert cache.get_block('data.blob', ('blob-meta', 0, 1)) == b'x'
        cache.put_block('data.blob', ('blob-meta', 32, 1), b'x')
        assert cache.get_block('data.blob', ('blob-meta', 0, 1)) == b'x'
        assert cache.get_block('data.blob', ('blob-meta', 1, 1)) is None
        for offset in range(33, 1000):
            cache.put_block('data.blob', ('blob-meta', offset, 1), b'x')
        entries = cache._entry_index if disk else cache._cache
        assert len(entries) == 32
        assert cache._current_size >= 32 * (4096 if disk else 512)
    if disk:
        # Opening an existing directory must enforce both limits immediately.
        with patch('pypaimon.filesystem.caching_file_io._MAX_CACHE_ENTRIES', 8):
            reopened = CachingFileIO.create_cache_manager(options)
            assert len(reopened._entry_index) == 8
        options = Options({'local-cache.enabled': 'true', 'local-cache.max-size': '4 kb',
                           'local-cache.dir': str(tmp_path / 'cache')})
        reopened = CachingFileIO.create_cache_manager(options)
        assert reopened._current_size <= 4096
        assert len(reopened._entry_index) <= 1
        assert len([p for p in (tmp_path / 'cache').rglob('*') if p.is_file()]) <= 1


@pytest.mark.parametrize('whitelist', ['blob-meta', 'data'])
def test_blob_readinto(tmp_path, whitelist):
    from pypaimon.table.row.blob import BlobRef

    path = str(tmp_path / 'data.blob')
    (tmp_path / 'data.blob').write_bytes(b'abcdefgh')
    delegate = LocalFileIO(str(tmp_path), Options({}))
    options = Options({'local-cache.enabled': 'true', 'local-cache.whitelist': whitelist})
    cache = CachingFileIO.create_cache_manager(options)
    file_io = CachingFileIO.wrap_with_caching_if_needed(delegate, options, cache)
    blob = BlobRef(file_io, BlobDescriptor(path, 1, 4))
    with blob.new_input_stream() as stream:
        assert stream.readinto(bytearray()) == 0
        buf = bytearray(b'------')
        assert stream.readinto(buf) == 4
        assert buf == b'bcde--'
        assert stream.readinto(buf) == 0
        stream.seek(0)
        assert stream.readinto(memoryview(buf)[1:3]) == 2
        assert buf == b'bbce--'
        assert stream.tell() == 2
    if whitelist == 'blob-meta':
        assert cache._current_size == 0  # readinto must not cache value bodies.
    else:
        assert cache._current_size > 0


@pytest.mark.parametrize('support', ['direct', 'missing', 'unsupported'])
def test_blob_readinto_delegates_without_caching(tmp_path, support):
    from pypaimon.table.row.blob import BlobRef

    calls = {'read': 0, 'readinto': 0}

    class CountingStream(io.BytesIO):
        def read(self, size=-1):
            calls['read'] += 1
            return super().read(size)

        def readinto(self, buffer):
            calls['readinto'] += 1
            if support == 'unsupported':
                raise io.UnsupportedOperation('readinto')
            # Exercise a short read and cursor updates.
            return super().readinto(memoryview(buffer)[:2])

    remote = CountingStream(b'abcdefgh')
    if support == 'missing':
        remote.readinto = None
    delegate = LocalFileIO(str(tmp_path), Options({}))
    options = Options({'local-cache.enabled': 'true'})
    cache = CachingFileIO.create_cache_manager(options)
    file_io = CachingFileIO.wrap_with_caching_if_needed(delegate, options, cache)
    blob = BlobRef(file_io, BlobDescriptor('data.blob', 1, 4))
    with patch.object(delegate, 'new_input_stream', return_value=remote):
        with blob.new_input_stream() as stream:
            buf = bytearray(b'----')
            if support == 'direct':
                assert stream.readinto(buf) == 2
                assert buf == b'bc--'
                assert stream.tell() == 2
                assert stream.readinto(memoryview(buf)[2:]) == 2
                assert calls == {'read': 0, 'readinto': 2}
            else:
                assert stream.readinto(buf) == 4
                assert calls['read'] > 0
            assert buf == b'bcde'
            assert stream.tell() == 4
            assert stream.readinto(buf) == 0
            stream.seek(0)
            assert stream.readinto(memoryview(buf)[:2]) == 2
            assert stream.tell() == 2
    assert cache._current_size == 0
