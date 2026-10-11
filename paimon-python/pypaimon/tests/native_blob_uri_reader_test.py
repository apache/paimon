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

"""Custom URI readers must copy Blob references in Rust core."""

import io

import pyarrow as pa
import pytest

from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.table.row.blob import BlobDescriptor
from pypaimon.tests.native_blob_write_test import _table, _read
from pypaimon.write.native_write import NativeTableWrite

pytestmark = pytest.mark.native_plan


class Factory:
    def __init__(self):
        self.sources = {'custom://source': b'prefixPAYLOADsuffix'}
        self.opened = []
        self.closed = []
        self.requests = []
        self.errors = {}
        self.short_reads = False
        self.no_seek = False

    def create(self, uri):
        if 'create' in self.errors:
            raise self.errors['create']
        assert uri in self.sources
        return self

    def new_input_stream(self, uri):
        if 'open' in self.errors:
            raise self.errors['open']
        self.opened.append(uri)
        owner = self

        class Stream(io.BytesIO):
            def read(self, length=-1):
                owner.requests.append(length)
                if 'read' in owner.errors:
                    raise owner.errors['read']
                if owner.short_reads:
                    length = min(length, 2)
                return super().read(length)

            def seek(self, offset, whence=0):
                if owner.no_seek:
                    raise io.UnsupportedOperation('source cannot seek')
                if 'seek' in owner.errors:
                    raise owner.errors['seek']
                return super().seek(offset, whence)

            def close(self):
                if not self.closed:
                    owner.closed.append(uri)
                super().close()
                if 'close' in owner.errors:
                    raise owner.errors['close']

        return Stream(self.sources[uri])


def test_custom_uri_reader_keeps_native_writer(tmp_path):
    table = _table(tmp_path)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    factory = Factory()
    try:
        assert writer.with_blob_uri_reader_factory(factory) is writer
        assert isinstance(writer, NativeTableWrite)
        descriptor = BlobDescriptor('custom://source', 6, 7).serialize()
        writer.write_arrow(pa.Table.from_pylist([
            {'id': 1, 'large': descriptor, 'small': b'inline'},
            {'id': 2, 'large': descriptor, 'small': None},
        ], schema=PyarrowFieldParser.from_paimon_schema(table.fields)))
        commit.commit(writer.prepare_commit())
        assert writer._python_writer is None
        assert _read(table, True, True) == [
            {'id': 1, 'large': b'PAYLOAD', 'small': b'inline'},
            {'id': 2, 'large': b'PAYLOAD', 'small': None},
        ]
        assert factory.opened == factory.closed
    finally:
        writer.close()
        commit.close()


@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('row_input', [False, True])
def test_scoped_application_factory_preserves_native_and_live_row_sources(tmp_path, stream, row_input):
    from pypaimon.table.row.generic_row import GenericRow
    from pypaimon.tests.native_blob_row_write_test import StreamBlob
    table = _table(tmp_path, {'blob.copy-buffer-size': '3 B'})
    source = tmp_path / 'source'
    source.write_bytes(b'prefixFILEIO!suffix')
    native_reference = BlobDescriptor(source.as_uri(), 6, 7).serialize()

    class ScopedFactory(Factory):
        def __init__(self):
            super().__init__()
            self.selected = []
            self.created = []

        def _supports_uri(self, uri):
            self.selected.append(uri)
            return uri.startswith('custom://')

        def create(self, uri):
            self.created.append(uri)
            return super().create(uri)

    factory = ScopedFactory()
    blob = StreamBlob(b'OBJECT')
    values = [[0, blob if row_input else b'BYTES', native_reference],
              [1, BlobDescriptor('custom://source', 6, 7).serialize(), None]]
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.with_blob_uri_reader_factory(factory)
        if row_input:
            for value in values:
                writer.write_row(GenericRow(value, table.fields))
            assert blob.opened == 1 and all(source.closed for source in blob.streams)
        else:
            writer.write_arrow(pa.Table.from_pylist([
                dict(zip(['id', 'large', 'small'], value)) for value in values],
                schema=PyarrowFieldParser.from_paimon_schema(table.fields)))
        messages = writer.prepare_commit(7) if stream else writer.prepare_commit()
        commit.commit(messages, 7) if stream else commit.commit(messages)
        assert writer._python_writer is None
        assert sorted(factory.selected) == sorted([source.as_uri(), 'custom://source'])
        assert factory.created == ['custom://source']
        assert _read(table, True, True) == [
            dict(id=0, large=b'OBJECT' if row_input else b'BYTES', small=b'FILEIO!'),
            dict(id=1, large=b'PAYLOAD', small=None),
        ]
    finally:
        writer.close()
        commit.close()


@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('row_input', [False, True])
def test_application_factory_selection_errors_do_not_fall_back_to_native_io(tmp_path, stream, row_input):
    from pypaimon.table.row.generic_row import GenericRow
    table = _table(tmp_path)
    source = tmp_path / 'source'
    source.write_bytes(b'native readable')
    reference = BlobDescriptor(source.as_uri(), 0, -1).serialize()
    error = OSError('application selector failed')
    selected = []

    class FailedSelection:
        def _supports_uri(self, uri):
            selected.append(uri)
            raise error

        def create(self, uri):
            pytest.fail('A selection error must not select any reader')

    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer = builder.new_write()
    try:
        writer.with_blob_uri_reader_factory(FailedSelection())
        with pytest.raises(OSError) as caught:
            if row_input:
                writer.write_row(GenericRow([0, reference, None], table.fields))
            else:
                writer.write_arrow(pa.Table.from_pylist([dict(id=0, large=reference, small=None)],
                                   schema=PyarrowFieldParser.from_paimon_schema(table.fields)))
        assert caught.value is error
        assert selected == [source.as_uri()]
        assert writer._python_writer is None
        assert writer._blob_rows.readers == {}
    finally:
        writer.close()
    assert not list(tmp_path.rglob('*.blob'))
    assert not list(tmp_path.rglob('*.parquet'))


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('stream', [False, True])
def test_custom_readers_copy_scalar_and_collection_windows(tmp_path, native, stream):
    from pypaimon.tests.native_blob_consumer_test import _collection_table, _payload_at
    table = _collection_table(tmp_path, native, options={
        'target-file-row-num': '1000', 'blob.target-file-size': '1 MB'})
    factory = Factory()
    factory.short_reads = True
    descriptor = BlobDescriptor('custom://source', 6, 7).serialize()
    expected = [{'id': 1, 'payload': b'PAYLOAD', 'items': [b'PAYLOAD', None, b''],
                 'attrs': [('a', b'PAYLOAD'), ('b', None)]},
                {'id': 2, 'payload': None, 'items': None, 'attrs': None},
                {'id': 3, 'payload': b'', 'items': [], 'attrs': []}]
    source = [dict(expected[0], payload=descriptor, items=[descriptor, None, b''],
                   attrs=[('a', descriptor), ('b', None)]), expected[1], expected[2]]
    received = []
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.with_blob_uri_reader_factory(factory).with_blob_consumer(
            lambda name, descriptor: (received.append((name, descriptor)) or True))
        data = pa.Table.from_pylist(source, schema=PyarrowFieldParser.from_paimon_schema(table.fields))
        writer.write_arrow(data)
        messages = writer.prepare_commit(1) if stream else writer.prepare_commit()
        commit.commit(messages, 1) if stream else commit.commit(messages)
        assert _read(table, True, True) == expected
        assert factory.opened == ['custom://source'] * 3
        assert factory.closed == factory.opened
        if native:
            assert writer._python_writer is None
            assert all(length <= 3 for length in factory.requests)
        assert [_payload_at(descriptor) for name, descriptor in received
                if name == 'payload' and descriptor is not None] == [b'PAYLOAD', b'']
    finally:
        writer.close()
        commit.close()


@pytest.mark.parametrize('unknown', [False, True])
def test_non_seekable_source_recovers_rewind_without_leaking_an_exception(tmp_path, unknown):
    table = _table(tmp_path, {'blob.target-file-size': '1 MB', 'target-file-row-num': '1000'})
    factory = Factory()
    factory.no_seek = True
    length = -1 if unknown else 3
    reference = BlobDescriptor('custom://source', 0, length).serialize()
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.with_blob_uri_reader_factory(factory)
        writer.write_arrow(pa.Table.from_pylist([
            {'id': 1, 'large': reference, 'small': None},
            {'id': 2, 'large': reference, 'small': None},
        ], schema=PyarrowFieldParser.from_paimon_schema(table.fields)))
        table.new_batch_write_builder().new_commit().commit(writer.prepare_commit())
        value = factory.sources['custom://source'] if unknown else b'pre'
        assert [row['large'] for row in _read(table, True, True)] == [value, value]
        assert factory.opened == factory.closed == ['custom://source'] * 2
    finally:
        writer.close()


@pytest.mark.parametrize('operation', ['create', 'open', 'seek', 'read', 'close'])
def test_custom_reader_exceptions_are_not_retried_and_streams_close(tmp_path, operation):
    table = _table(tmp_path, {'blob.target-file-size': '1 MB', 'target-file-row-num': '1000'})
    factory = Factory()
    failure = RuntimeError('custom source failed: ' + operation)
    factory.errors[operation] = failure
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.with_blob_uri_reader_factory(factory)
        data = pa.Table.from_pylist([
            {'id': 1, 'large': BlobDescriptor('custom://source', 6, 7).serialize(), 'small': None},
        ], schema=PyarrowFieldParser.from_paimon_schema(table.fields))
        if operation == 'close':
            writer.write_arrow(data)
            with pytest.raises(RuntimeError) as caught:
                writer.prepare_commit()
        else:
            with pytest.raises(RuntimeError) as caught:
                writer.write_arrow(data)
        assert caught.value is failure
        assert writer._python_writer is None
        assert factory.opened == factory.closed
        expected = (RuntimeError, 'one-time committing') if operation == 'close' else (
            ValueError, 'cannot be reused')
        with pytest.raises(expected[0], match=expected[1]):
            writer.prepare_commit()
    finally:
        writer.close()
    assert not list(tmp_path.rglob('*.blob'))
    assert not list(tmp_path.rglob('*.parquet'))


def test_reader_factory_replacement_clear_and_native_rows(tmp_path):
    from pypaimon.table.row.generic_row import GenericRow
    table = _table(tmp_path)
    factory = Factory()
    factory.errors['create'] = AssertionError('cleared factory invoked')
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.with_blob_uri_reader_factory(factory)
        writer.with_blob_uri_reader_factory(None)
        writer.write_arrow(pa.Table.from_pylist(
            [{'id': 1, 'large': b'inline', 'small': None}],
            schema=PyarrowFieldParser.from_paimon_schema(table.fields)))
        assert factory.opened == []
        with pytest.raises(RuntimeError, match='before any write'):
            writer.with_blob_uri_reader_factory(factory)
    finally:
        writer.close()

    factory = Factory()
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.with_blob_uri_reader_factory(factory)
        writer.write_row(GenericRow([
            2, BlobDescriptor('custom://source', 6, 7).serialize(), None], table.fields))
        assert writer._python_writer is None
        commit.commit(writer.prepare_commit())
        assert factory.opened == factory.closed == ['custom://source']
        assert _read(table, True, True) == [{'id': 2, 'large': b'PAYLOAD', 'small': None}]
    finally:
        writer.close()
        commit.close()


@pytest.mark.parametrize('mode', ['fixed', 'pending', 'postpone-fixed'])
def test_managed_blob_copies_use_the_configured_reader_factory(tmp_path, mode):
    from pypaimon.tests.native_managed_blob_write_test import (
        _table as managed_table, _builder, _input, _read as managed_read)
    table = managed_table(tmp_path, mode)
    factory = Factory()
    reference = BlobDescriptor('custom://source', 6, 7).serialize()
    builder = _builder(table, mode)
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.with_blob_uri_reader_factory(factory)
        values = [{'id': 1, 'payload': reference, 'items': [reference, None],
                   'mapping': [('k', reference)], 'op': '+I'}]
        writer.write_arrow(_input(table, values))
        messages = writer.prepare_commit()
        commit.commit(messages)
        assert writer._python_writer is None
        if mode == 'pending':
            import pyarrow.parquet as pq
            from pypaimon.table.row.blob import Blob
            file = messages[0].new_files[0]
            with table.file_io.new_input_stream(file.file_path) as source:
                row = pq.ParquetFile(source).read().to_pylist()[0]
            assert Blob.from_descriptor_bytes(row['payload'], table.file_io).to_data() == b'PAYLOAD'
            assert Blob.from_descriptor_bytes(row['items'][0], table.file_io).to_data() == b'PAYLOAD'
            assert Blob.from_descriptor_bytes(row['mapping'][0][1], table.file_io).to_data() == b'PAYLOAD'
        else:
            expected = [{'id': 1, 'payload': b'PAYLOAD', 'items': [b'PAYLOAD', None],
                         'mapping': [('k', b'PAYLOAD')], 'op': '+I'}]
            assert managed_read(table) == expected
        assert factory.opened == factory.closed
    finally:
        writer.close()
        commit.close()


def test_same_uri_with_different_reader_identity_reopens_the_source(tmp_path):
    table = _table(tmp_path, {'blob.target-file-size': '1 MB', 'target-file-row-num': '1000'})

    class RotatingFactory(Factory):
        def create(self, uri):
            owner = self
            payload = b'FIRST__' if not self.opened else b'SECOND_'

            class Reader:
                def new_input_stream(self, uri):
                    owner.sources[uri] = payload
                    return owner.new_input_stream(uri)

            return Reader()

    factory = RotatingFactory()
    reference = BlobDescriptor('custom://source', 0, 7).serialize()
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.with_blob_uri_reader_factory(factory)
        writer.write_arrow(pa.Table.from_pylist([
            {'id': 1, 'large': reference, 'small': None},
            {'id': 2, 'large': reference, 'small': None},
        ], schema=PyarrowFieldParser.from_paimon_schema(table.fields)))
        commit.commit(writer.prepare_commit())
        assert [row['large'] for row in _read(table, True, True)] == [b'FIRST__', b'SECOND_']
        assert factory.opened == factory.closed == ['custom://source'] * 2
    finally:
        writer.close()
        commit.close()


def test_premature_eof_closes_source_and_leaves_no_uncommitted_files(tmp_path):
    table = _table(tmp_path)
    factory = Factory()
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.with_blob_uri_reader_factory(factory)
        data = pa.Table.from_pylist([
            {'id': 1, 'large': BlobDescriptor('custom://source', 6, 100).serialize(), 'small': None},
        ], schema=PyarrowFieldParser.from_paimon_schema(table.fields))
        with pytest.raises(ValueError, match='Unexpected EOF'):
            writer.write_arrow(data)
        assert factory.opened == factory.closed == ['custom://source']
    finally:
        writer.close()
    assert not list(tmp_path.rglob('*.blob'))
    assert not list(tmp_path.rglob('*.parquet'))


def test_reader_identity_is_preserved_across_interleaved_blob_columns(tmp_path):
    table = _table(tmp_path, {'blob.target-file-size': '1 MB', 'target-file-row-num': '1000'})

    class InterleavedFactory:
        def __init__(self):
            self.readers = {uri: Factory() for uri in ('custom://a', 'custom://b')}
            for uri, reader in self.readers.items():
                reader.sources = {uri: b'firstsecond'}

        def create(self, uri):
            return self.readers[uri]

    factory = InterleavedFactory()
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.with_blob_uri_reader_factory(factory)
        for identifier, offset, length in ((1, 0, 5), (2, 5, 6)):
            writer.write_arrow(pa.Table.from_pylist([
                {'id': identifier,
                 'large': BlobDescriptor('custom://a', offset, length).serialize(),
                 'small': BlobDescriptor('custom://b', offset, length).serialize()},
            ], schema=PyarrowFieldParser.from_paimon_schema(table.fields)))
        commit.commit(writer.prepare_commit())
        assert _read(table, True, True) == [
            {'id': 1, 'large': b'first', 'small': b'first'},
            {'id': 2, 'large': b'second', 'small': b'second'},
        ]
        for uri, reader in factory.readers.items():
            assert reader.opened == reader.closed == [uri]
    finally:
        writer.close()
        commit.close()
