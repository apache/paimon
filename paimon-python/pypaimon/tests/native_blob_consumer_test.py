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

"""Blob callbacks must retain Native writes and report Java payload ranges."""

import pyarrow as pa
import pytest
from urllib.parse import urlparse
from unittest.mock import patch

from pypaimon import CatalogFactory, Schema
from pypaimon.schema.data_types import AtomicType, ArrayType, DataField, MapType, PyarrowFieldParser
from pypaimon.table.row.blob import Blob, BlobDescriptor
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.tests.native_blob_write_test import _table, _data, _read
from pypaimon.write.native_write import NativeTableWrite

pytestmark = pytest.mark.native_plan


def test_blob_consumer_keeps_native_writer(tmp_path):
    table = _table(tmp_path)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    received = []

    def consumer(field_name, descriptor):
        received.append((field_name, descriptor))
        return True

    try:
        assert writer.with_blob_consumer(consumer) is writer
        assert isinstance(writer, NativeTableWrite)
        writer.write_arrow(_data())
        messages = writer.prepare_commit()
        commit.commit(messages)
        assert writer._python_writer is None
        assert [descriptor.length if descriptor is not None else None
                for name, descriptor in received if name == 'large'] == [40, None]
        assert [descriptor.length for name, descriptor in received if name == 'small'] == [0, 3]
        assert _read(table, True, True) == _data().to_pylist()
    finally:
        writer.close()
        commit.close()


def _collection_table(tmp_path, native, external=False, options=None):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('db', True)
    blob = AtomicType('BLOB')
    fields = [DataField(0, 'id', AtomicType('INT')),
              DataField(1, 'payload', blob),
              DataField(2, 'items', ArrayType(True, blob)),
              DataField(3, 'attrs', MapType(True, AtomicType('STRING'), blob))]
    settings = {'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
                'write.native.enabled': str(native).lower(), 'target-file-row-num': '2',
                'blob.target-file-size': '32 B', 'blob.copy-buffer-size': '3 B',
                'data-file.path-directory': 'data'}
    if external:
        settings['data-file.external-paths'] = (tmp_path / 'external').as_uri()
        settings['data-file.external-paths.strategy'] = 'round-robin'
    settings.update(options or {})
    catalog.create_table('db.t', Schema(fields=fields, options=settings), False)
    return catalog.get_table('db.t')


def _payload_at(descriptor):
    path = urlparse(descriptor.uri).path if descriptor.uri.startswith('file:') else descriptor.uri
    with open(path, 'rb') as source:
        source.seek(descriptor.offset)
        return source.read(descriptor.length)


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('stream', [False, True])
@pytest.mark.parametrize('external', [False, True])
@pytest.mark.parametrize('reference', [False, True])
def test_blob_consumer_ranges_nulls_and_collection_elements(tmp_path, native, stream, external, reference):
    table = _collection_table(tmp_path, native, external)
    received = []

    def consumer(name, descriptor):
        received.append((name, descriptor))
        return len(received) % 2 == 1

    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    expected = []
    try:
        assert writer.with_blob_consumer(consumer) is writer
        for identifier in range(1, 3 if stream else 2):
            received.clear()
            value = b'payload-with-many-copy-chunks'
            if reference:
                path = tmp_path / 'source'
                path.write_bytes(b'prefix' + value + b'suffix')
                value_input = BlobDescriptor(str(path), 6, len(value)).serialize()
            else:
                value_input = value
            rows = [{'id': identifier * 3, 'payload': value_input,
                     'items': [b'a', None, b''], 'attrs': [('key', value_input), ('null', None)]},
                    {'id': identifier * 3 + 1, 'payload': None, 'items': None, 'attrs': None},
                    {'id': identifier * 3 + 2, 'payload': b'', 'items': [], 'attrs': []}]
            expected.extend([dict(rows[0], payload=value, attrs=[('key', value), ('null', None)]),
                             rows[1], rows[2]])
            writer.write_arrow(pa.Table.from_pylist(
                rows, schema=PyarrowFieldParser.from_paimon_schema(table.fields)))
            messages = writer.prepare_commit(identifier) if stream else writer.prepare_commit()
            for name, values in [('payload', [value, None, b'']),
                                 ('items', [b'a', b'', None]), ('attrs', [value, None])]:
                descriptors = [descriptor for field, descriptor in received if field == name]
                assert len(descriptors) == len(values)
                for descriptor, payload in zip(descriptors, values):
                    if payload is None:
                        assert descriptor is None
                    else:
                        assert isinstance(descriptor, BlobDescriptor)
                        assert descriptor.length == len(payload)
                        assert _payload_at(descriptor) == payload
                        assert ('/external/' in descriptor.uri) == external
            commit.commit(messages, identifier) if stream else commit.commit(messages)
            for planner in (False, True):
                for reader in (False, True):
                    assert _read(table, planner, reader) == expected
            if native:
                assert writer._python_writer is None
    finally:
        writer.close()
        commit.close()


def test_blob_consumer_failure_preserves_exception_and_does_not_retry(tmp_path):
    table = _collection_table(tmp_path, True)
    writer = table.new_batch_write_builder().new_write()
    calls = []
    failure = RuntimeError('consumer stopped the write')

    def consumer(name, descriptor):
        calls.append((name, descriptor))
        raise failure

    try:
        writer.with_blob_consumer(consumer)
        schema = PyarrowFieldParser.from_paimon_schema(table.fields)
        data = pa.Table.from_pylist([{'id': 1, 'payload': b'a'}, {'id': 2, 'payload': b'b'}], schema=schema)
        with pytest.raises(RuntimeError) as caught:
            writer.write_arrow(data)
        assert caught.value is failure
        assert len(calls) == 1
        assert writer._python_writer is None
        assert table.snapshot_manager().get_latest_snapshot() is None
        # Java relinquishes deletion rights as soon as a consumer is installed.
        # The descriptor may already have been handed to another application.
        assert list(tmp_path.rglob('*.blob'))
        assert _payload_at(calls[0][1]) == b'a'
        assert not list(tmp_path.rglob('*.parquet'))
        with pytest.raises(Exception, match='write failure'):
            writer.write_arrow(data)
        assert len(calls) == 1
    finally:
        writer.close()


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('action', ['close', 'abort', 'callback-failure'])
def test_blob_consumer_preserves_uncommitted_exposed_files(tmp_path, native, action):
    table = _table(tmp_path, {'write.native.enabled': str(native).lower(),
                              'target-file-row-num': '1'})
    writer = table.new_batch_write_builder().new_write()
    received = []

    def consumer(name, descriptor):
        received.append((name, descriptor))
        if action == 'callback-failure' and descriptor is None:
            raise RuntimeError('after exposing a payload')
        return True

    try:
        writer.with_blob_consumer(consumer)
        if action == 'callback-failure':
            with pytest.raises(RuntimeError, match='after exposing'):
                writer.write_arrow(_data())
        else:
            writer.write_arrow(_data())
            getattr(writer, action)()
        assert table.snapshot_manager().get_latest_snapshot() is None
        for _, descriptor in received:
            if descriptor is not None:
                assert _payload_at(descriptor) in (b'\x01' * 40, b'', b'abc')
        assert list(tmp_path.rglob('*.blob'))
        if native:
            assert not list(tmp_path.rglob('*.parquet'))
            assert writer._python_writer is None
    finally:
        writer.close()


def test_blob_consumer_flush_makes_local_payload_visible_before_prepare(tmp_path):
    table = _table(tmp_path, {'target-file-row-num': '1000', 'blob.target-file-size': '1 MB'})
    writer = table.new_batch_write_builder().new_write()
    received = []
    try:
        writer.with_blob_consumer(lambda name, descriptor: (received.append((name, descriptor)) or True))
        writer.write_arrow(_data().slice(0, 1))
        # No roll or prepare can hide a no-op flush by closing the output.
        assert [_payload_at(descriptor) for _, descriptor in received] == [b'\x01' * 40, b'']
        assert table.snapshot_manager().get_latest_snapshot() is None
        assert writer._python_writer is None
    finally:
        writer.close()


def test_blob_consumer_can_be_replaced_cleared_and_cannot_change_after_write(tmp_path):
    table = _table(tmp_path)
    writer = table.new_batch_write_builder().new_write()
    try:
        writer.with_blob_consumer(lambda name, descriptor: pytest.fail('replaced consumer invoked'))
        with pytest.raises(TypeError, match='callable'):
            writer.with_blob_consumer(123)
        writer.with_blob_consumer(None)
        writer.write_arrow(_data())
        with pytest.raises(RuntimeError, match='before any write'):
            writer.with_blob_consumer(None)
        assert writer._python_writer is None
    finally:
        writer.close()


def test_blob_consumer_survives_switch_to_row_writer(tmp_path):
    table = _table(tmp_path)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    received = []
    try:
        writer.with_blob_consumer(lambda name, descriptor: received.append((name, descriptor)))
        row = GenericRow([1, Blob.from_data(b'row-payload'), None], table.fields)
        writer.write_row(row)
        messages = writer.prepare_commit()
        commit.commit(messages)
        assert [(name, None if descriptor is None else _payload_at(descriptor))
                for name, descriptor in received] == [('large', b'row-payload'), ('small', None)]
    finally:
        writer.close()
        commit.close()


def test_blob_consumer_reentrant_native_read_and_write_type_projection(tmp_path):
    table = _table(tmp_path)
    first = table.new_batch_write_builder()
    initial = first.new_write()
    initial.write_arrow(_data())
    first.new_commit().commit(initial.prepare_commit())
    initial.close()
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    received = []

    def consumer(name, descriptor):
        assert _read(table, True, True) == _data().to_pylist()
        received.append((name, descriptor))
        return False

    try:
        writer.with_blob_consumer(consumer).with_write_type(['id', 'small'])
        with patch.object(writer, '_switch_to_python', side_effect=AssertionError('Python writer fallback')):
            writer.write_arrow(_data(2))
            commit.commit(writer.prepare_commit())
        assert [name for name, _ in received] == ['small', 'small']
        assert [_payload_at(descriptor) for _, descriptor in received] == [b'', b'abc']
        assert writer._python_writer is None
    finally:
        writer.close()
        commit.close()
