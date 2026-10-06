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

"""Blob Arrow writes must retain row identity across physical file rolls."""

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.schema.data_types import AtomicType, ArrayType, DataField, MapType, PyarrowFieldParser
from pypaimon.table.row.blob import BlobDescriptor
from pypaimon.write.native_write import NativeTableWrite
from pypaimon.tests.native_blob_write_test import _read, _physical_files

pytestmark = pytest.mark.native_plan


def _table(tmp_path, native, external=False, optimize=True, catalog=None, partitioned=False, not_null=False):
    if catalog is None:
        catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('db', True)
    blob = AtomicType('BLOB', nullable=not not_null)
    fields = [DataField(0, 'id', AtomicType('INT')),
              DataField(1, 'items', ArrayType(True, blob)),
              DataField(2, 'attrs', MapType(True, AtomicType('STRING'), blob))]
    options = {'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
               'write.native.enabled': str(native).lower(), 'target-file-row-num': '2',
               'blob.target-file-size': '32 B',
               'data-evolution.write-cols-optimization.enabled': str(optimize).lower()}
    if external:
        options['data-file.external-paths'] = (tmp_path / 'external').as_uri()
    schema = Schema(fields=fields, options=options, partition_keys=['id'] if partitioned else [])
    catalog.create_table('db.t', schema, False)
    return catalog.get_table('db.t')


def _data(table, tmp_path, descriptor, start):
    payload = b'payload' * 10
    if descriptor:
        source = tmp_path / 'payload'
        source.write_bytes(b'prefix' + payload)
        value = BlobDescriptor(str(source), 6, -1).serialize()
    else:
        value = payload
    rows = [{'id': -1, 'items': [b'ignored'], 'attrs': [('ignored', b'ignored')]},
            {'id': start, 'items': [value, None, b''], 'attrs': [('first', value), ('', b''), ('null', None)]},
            {'id': start + 1, 'items': None, 'attrs': None},
            {'id': start + 2, 'items': [], 'attrs': []}]
    data = pa.Table.from_pylist(rows, schema=PyarrowFieldParser.from_paimon_schema(table.fields)).slice(1)
    expected = [{'id': start, 'items': [payload, None, b''],
                 'attrs': [('first', payload), ('', b''), ('null', None)]}, rows[2], rows[3]]
    return data, expected


@pytest.mark.parametrize('native_write', [False, True])
@pytest.mark.parametrize('external', [False, True])
@pytest.mark.parametrize('descriptor', [False, True])
@pytest.mark.parametrize('stream', [False, True])
def test_collection_blob_write_cross_reads_and_rolls(tmp_path, native_write, external, descriptor, stream):
    table = _table(tmp_path, native_write, external)
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    expected = []
    try:
        assert isinstance(writer, NativeTableWrite) == native_write
        for identifier in range(1, 3 if stream else 2):
            data, rows = _data(table, tmp_path, descriptor, identifier * 3)
            writer.write_arrow(data)
            expected.extend(rows)
            messages = writer.prepare_commit(identifier) if stream else writer.prepare_commit()
            files = [file for message in messages for file in message.new_files]
            for name in ('items', 'attrs'):
                dedicated = [file for file in files if file.write_cols == [name]]
                assert dedicated and all(file.file_name.endswith('.blob') for file in dedicated)
                assert sum(file.row_count for file in dedicated) == 3
            commit.commit(messages, identifier) if stream else commit.commit(messages)
            for planner in (False, True):
                for reader in (False, True):
                    assert _read(table, planner, reader) == expected
            if native_write:
                assert writer._python_writer is None
    finally:
        writer.close()
        commit.close()


@pytest.mark.parametrize('native_commit', [False, True])
@pytest.mark.parametrize('optimize', [False, True])
def test_collection_blob_descriptors_are_payload_ranges(tmp_path, native_rest_catalog, native_commit, optimize):
    table = _table(tmp_path, True, optimize=optimize, catalog=native_rest_catalog).copy(
        {'commit.native.enabled': str(native_commit).lower()})
    data, expected = _data(table, tmp_path, False, 1)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        assert isinstance(writer, NativeTableWrite)
        writer.write_arrow(data)
        messages = writer.prepare_commit()
        normal = [file for message in messages for file in message.new_files if file.file_name.endswith('.parquet')]
        assert all(file.write_cols == (None if optimize else ['id']) for file in normal)
        commit.commit(messages)
        assert (commit._native_commit is not None) == native_commit
        for reader in (False, True):
            rows = _read(table.copy({'blob-as-descriptor': 'true'}), True, reader)
            assert rows[1]['items'] is None and rows[2]['items'] == []
            assert rows[1]['attrs'] is None and rows[2]['attrs'] == []
            for descriptors, values in ((rows[0]['items'], expected[0]['items']),
                                        ([item for _, item in rows[0]['attrs']],
                                         [item for _, item in expected[0]['attrs']])):
                for encoded, payload in zip(descriptors, values):
                    if payload is None:
                        assert encoded is None
                    else:
                        descriptor = BlobDescriptor.deserialize(encoded)
                        assert descriptor.uri.endswith('.blob') and descriptor.length == len(payload)
                        from urllib.parse import urlparse
                        with open(urlparse(descriptor.uri).path, 'rb') as source:
                            source.seek(descriptor.offset)
                            assert source.read(descriptor.length) == payload
    finally:
        writer.close()
        commit.close()


@pytest.mark.parametrize('action', ['close', 'abort', 'failed-write', 'short-read'])
def test_collection_blob_failed_writes_and_abort_clean_all_groups(tmp_path, action):
    table = _table(tmp_path, True)
    writer = table.new_batch_write_builder().new_write()
    try:
        assert isinstance(writer, NativeTableWrite)
        data, _ = _data(table, tmp_path, False, 1)
        writer.write_arrow(data)
        assert {path.suffix for path in _physical_files(tmp_path)} == {'.parquet', '.blob'}
        if action in ('failed-write', 'short-read'):
            if action == 'short-read':
                source = tmp_path / 'truncated'
                source.write_bytes(b'x' * (8 * 1024 * 1024))
                missing = BlobDescriptor(str(source), 0, 8 * 1024 * 1024 + 1).serialize()
            else:
                missing = BlobDescriptor(str(tmp_path / 'missing'), 0, 10).serialize()
            data = pa.table({'id': [5], 'items': [[b'already-copied', missing]],
                             'attrs': [[('x', b'ok')]]}, schema=data.schema)
            with pytest.raises(Exception):
                writer.write_arrow(data)
        else:
            getattr(writer, action)()
        assert not _physical_files(tmp_path)
        assert table.snapshot_manager().get_latest_snapshot() is None
    finally:
        writer.close()


@pytest.mark.parametrize('native_write', [False, True])
@pytest.mark.parametrize('action', ['abort', 'failed-write'])
def test_stream_collection_failure_keeps_already_committed_groups(tmp_path, native_write, action):
    table = _table(tmp_path, native_write)
    builder = table.new_stream_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        data, expected = _data(table, tmp_path, False, 1)
        writer.write_arrow(data)
        messages = writer.prepare_commit(1)
        committed = {file.file_name for message in messages for file in message.new_files}
        commit.commit(messages, 1)
        assert writer.prepare_commit(2) == []
        if action == 'failed-write':
            missing = BlobDescriptor(str(tmp_path / 'missing'), 0, 1).serialize()
            with pytest.raises(Exception):
                writer.write_arrow(pa.table({'id': [10], 'items': [[b'ok', missing]], 'attrs': [[]]},
                                            schema=data.schema))
        else:
            writer.write_arrow(data)
            writer.abort()
        assert committed <= {path.name for path in _physical_files(tmp_path)}
        for native_read in (False, True):
            assert _read(table, True, native_read) == expected
    finally:
        writer.close()
        commit.close()


@pytest.mark.parametrize('projection', [
    ['id', "attrs['first']", "attrs['null']", "attrs['missing']"],
    {'payload': "attrs['first']", 'array': 'items'},
    ['attrs', "attrs['first']", 'id'],
])
def test_native_collection_projection_keeps_selected_blob_keys(tmp_path, projection):
    from unittest.mock import patch

    table = _table(tmp_path, True)
    data, _ = _data(table, tmp_path, False, 1)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(data)
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    for native in (False, True):
        read_table = table.copy({'read.native.enabled': str(native).lower()})
        read_builder = read_table.new_read_builder().with_projection(projection)
        read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        if native:
            with patch.object(read, '_create_split_read', side_effect=AssertionError('Python fallback')):
                result = read.to_arrow(splits)
        else:
            expected = read.to_arrow(splits)
            continue
        assert result.schema == expected.schema
        assert result.to_pylist() == expected.to_pylist()


def test_collection_partial_partition_prepare_keeps_cleanup_ownership(tmp_path):
    from unittest.mock import patch
    from pypaimon.write.writer.composite_data_writer import CompositeDataWriter

    table = _table(tmp_path, False, partitioned=True)
    writer = table.new_batch_write_builder().new_write()
    try:
        data, _ = _data(table, tmp_path, False, 1)
        writer.write_arrow(data)
        partitions = list(writer.file_store_write.data_writers.values())
        assert len(partitions) == 3 and all(isinstance(partition, CompositeDataWriter) for partition in partitions)
        with patch.object(partitions[1], 'prepare_commit', side_effect=RuntimeError('Second partition failed')):
            with pytest.raises(RuntimeError, match='Second partition failed'):
                writer.prepare_commit()
        assert partitions[0].committed_files
        assert _physical_files(tmp_path)
        writer.abort()
        assert not _physical_files(tmp_path)
        assert table.snapshot_manager().get_latest_snapshot() is None
    finally:
        writer.close()


@pytest.mark.parametrize('native_write', [False, True])
@pytest.mark.parametrize('sliced', [False, True])
@pytest.mark.parametrize('aliases', [False, True])
def test_not_null_collection_children_ignore_masked_and_unreferenced_values(
        tmp_path, native_write, sliced, aliases):
    table = _table(tmp_path, native_write, not_null=True)
    offsets = pa.array([0, 1, 2, 3, 4], type=pa.int32())
    mask = pa.array([True, False, True, False])
    values = pa.array([None, b'first', None, b'last'], type=pa.large_binary())
    items = pa.ListArray.from_arrays(
        offsets, values, mask=mask,
        type=pa.list_(pa.field('item' if aliases else 'element', pa.large_binary(), nullable=False)))
    attrs = pa.MapArray.from_arrays(
        offsets, pa.array(['hidden', 'first', 'hidden', 'last']), values, mask=mask,
        type=pa.map_(pa.field('source_key' if aliases else 'key', pa.string(), nullable=False),
                     pa.field('source_value' if aliases else 'value', pa.large_binary(), nullable=False)))
    data = pa.table({'id': pa.array(range(4), type=pa.int32()), 'items': items, 'attrs': attrs})
    if sliced:
        data = data.slice(1, 1)
    data.validate(full=True)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        assert isinstance(writer, NativeTableWrite) == native_write
        writer.write_arrow(data)
        commit.commit(writer.prepare_commit())
        for native_read in (False, True):
            assert _read(table, True, native_read) == data.to_pylist()
    finally:
        writer.close()
        commit.close()
