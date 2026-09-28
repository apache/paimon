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

"""Java-compatible data directories across Python and Rust execution."""

from contextlib import ExitStack
import os
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.read.native_plan import native_plan
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.write.native_write import NativeTableWrite
from pypaimon.write.table_upsert_by_key import TableUpsertByKey


pytestmark = pytest.mark.native_plan
_SCHEMA = pa.schema([('id', pa.int32()), ('pt', pa.string()), ('value', pa.int32())])


def _table(tmp_path, mode='append', directory='relative', native=True, partitioned=True, extra=None):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('db', True)
    directories = {'relative': 'data/nested', 'normalized': 'data//discard/../nested/.',
                   'absolute': str(tmp_path / 'relocated'), 'uri': (tmp_path / 'relocated').as_uri(),
                   'literal_uri': 'file:' + str(tmp_path / 'data%2Fwith space?#fragment')}
    options = {'data-file.path-directory': directories[directory],
               'write.native.enabled': str(native).lower()}
    if mode == 'pk':
        options.update({'bucket': '1', 'changelog-producer': 'input'})
    if mode == 'evolution':
        options.update({'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
                        'target-file-row-num': '2',
                        'deletion-vectors.enabled': 'true'})
    options.update(extra or {})
    partitions = ['pt'] if partitioned else []
    keys = ['id'] + partitions if mode == 'pk' else []
    catalog.create_table('db.t', Schema.from_pyarrow_schema(
        _SCHEMA, primary_keys=keys, partition_keys=partitions, options=options), False)
    return catalog.get_table('db.t')


def _write(table, data, native, streaming=False, identifier=1):
    builder = table.new_stream_write_builder() if streaming else table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        if native:
            assert isinstance(writer, NativeTableWrite)
        writer.write_arrow(data)
        messages = writer.prepare_commit(identifier) if streaming else writer.prepare_commit()
        factory = table.path_factory()
        for message in messages:
            expected = factory.bucket_path(tuple(message.partition), message.bucket, canonical_partition=True)
            for file in message.new_files + message.changelog_files:
                assert file.file_path.startswith(expected + '/')
                assert table.file_io.exists(file.file_path)
        if streaming:
            commit.commit(messages, identifier)
        else:
            commit.commit(messages)
        if native:
            assert writer._python_writer is None
    finally:
        writer.close()
        commit.close()


def _read(table, native_scan, native_read, streaming=False, snapshot=None):
    options = {'read.native.enabled': str(native_read).lower(),
               'scan.native-plan.enabled': str(native_scan).lower()}
    if snapshot is not None:
        options['scan.snapshot-id'] = str(snapshot)
    table = table.copy(options)
    builder = table.new_read_builder()
    plan = native_plan(table) if native_scan else builder.new_scan().plan()
    read = builder.new_read()
    with ExitStack() as stack:
        if native_read:
            stack.enter_context(patch.object(read, '_create_split_read',
                                             side_effect=AssertionError('Python read fallback')))
        if streaming:
            reader = read.to_arrow_batch_reader(plan.splits())
            try:
                result = reader.read_all()
            finally:
                reader.close()
        else:
            result = read.to_arrow(plan.splits())
    return result.sort_by('id').to_pylist()


@pytest.mark.parametrize('directory', [
    'relative', 'normalized', 'absolute', 'uri',
    pytest.param('literal_uri', marks=pytest.mark.skipif(os.name == 'nt', reason='POSIX file names'))])
@pytest.mark.parametrize('mode', ['append', 'pk', 'evolution'])
@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('partitioned', [False, True])
def test_data_directory_interoperability(tmp_path, directory, mode, native, partitioned):
    table = _table(tmp_path, mode, directory, native, partitioned)
    data = pa.table({'id': [1, 2, 3], 'pt': ['a', 'a', 'b'], 'value': [10, 20, 30]}, schema=_SCHEMA)
    _write(table, data, native)
    _write(table, pa.table({'id': [4], 'pt': ['b'], 'value': [40]}, schema=_SCHEMA), native,
           streaming=True, identifier=2)
    if directory == 'literal_uri':
        assert (tmp_path / 'data%2Fwith space?#fragment').is_dir()
        assert not (tmp_path / 'data').exists()
    expected = data.to_pylist() + [{'id': 4, 'pt': 'b', 'value': 40}]
    for planner in (False, True):
        for reader in (False, True):
            assert _read(table, planner, reader, streaming=reader) == expected
    assert (Path(table.table_path) / 'snapshot' / 'snapshot-2').is_file()
    assert (Path(table.table_path) / 'schema' / 'schema-0').is_file()
    assert not (Path(table.table_path) / 'bucket-0').exists()


@pytest.mark.skipif(os.name == 'nt', reason='POSIX file names')
@pytest.mark.parametrize('strategy', ['round-robin', 'entropy-inject', 'weight-robin'])
def test_literal_data_directory_with_external_paths(tmp_path, strategy):
    table = _table(tmp_path, directory='literal_uri', native=False, extra={
        'data-file.external-paths': ','.join((tmp_path / name).as_uri() for name in ['first', 'second']),
        'data-file.external-paths.strategy': strategy,
        'data-file.external-paths.weights': '1,2',
    })
    data = pa.table({'id': [1], 'pt': ['a'], 'value': [10]}, schema=_SCHEMA)
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(data)
        messages = writer.prepare_commit()
        assert all(file.external_path for message in messages for file in message.new_files)
        commit.commit(messages)
    finally:
        writer.close()
        commit.close()
    assert (tmp_path / 'data%2Fwith space?#fragment').is_dir()
    for planner in (False, True):
        for reader in (False, True):
            assert _read(table, planner, reader) == data.to_pylist()


@pytest.mark.parametrize('directory', [
    'relative', 'normalized', 'absolute', 'uri',
    pytest.param('literal_uri', marks=pytest.mark.skipif(os.name == 'nt', reason='POSIX file names'))])
@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('bucket_indexes', [False, True])
def test_data_directory_updates_deletes_history_and_abort(tmp_path, directory, native, bucket_indexes):
    table = _table(tmp_path, 'evolution', directory, native, extra={
        'index-file-in-data-file-dir': str(bucket_indexes).lower()})
    _write(table, pa.table({'id': [1, 2, 3], 'pt': ['a'] * 3, 'value': [10, 20, 30]}, schema=_SCHEMA), native)
    builder = table.new_batch_write_builder()
    update = builder.new_update().with_update_type(['value'])
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch.object(TableUpsertByKey, '_upsert_partition',
                                             side_effect=AssertionError('Python upsert fallback')))
        messages = update.upsert_by_arrow_with_key(
            pa.table({'id': [1, 4], 'pt': ['a', 'b'], 'value': [11, 40]}, schema=_SCHEMA), ['id'])
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    for row_id, expected in [(0, [20, 30, 40]), (1, [30, 40])]:
        messages = builder.new_update().delete_by_row_id([row_id])
        commit = builder.new_commit()
        try:
            commit.commit(messages)
        finally:
            commit.close()
        for planner in (False, True):
            for reader in (False, True):
                assert sorted(row['value'] for row in _read(table, planner, reader)) == expected
    messages = builder.new_update().delete_by_row_id([2])
    staged = {entry.index_file.file_name for msg in messages for entry in msg.index_adds}
    assert staged
    before = {p for p in tmp_path.rglob('index-*') if p.name in staged}
    assert before
    commit = builder.new_commit()
    try:
        commit.abort(messages)
    finally:
        commit.close()
    assert not any(path.exists() for path in before)
    assert [row['value'] for row in _read(table, True, True, snapshot=1)] == [10, 20, 30]
    assert [row['value'] for row in _read(table, True, True)] == [30, 40]


@pytest.mark.parametrize('directory', ['relative', 'absolute', 'uri'])
def test_data_directory_blob_native_read(tmp_path, directory):
    table = _table(tmp_path, 'evolution', directory, False)
    catalog = table.catalog_environment.catalog_loader.load()
    catalog.drop_table('db.t')
    fields = [DataField(0, 'id', AtomicType('INT')), DataField(1, 'payload', AtomicType('BLOB'))]
    catalog.create_table('db.t', Schema(fields=fields, options=table.table_schema.options), False)
    table = catalog.get_table('db.t')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'id': pa.array([1, 2, 3], pa.int32()),
                                    'payload': pa.array([b'hello', None, b''], pa.large_binary())}))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    for planner in (False, True):
        for reader in (False, True):
            assert _read(table, planner, reader) == [
                {'id': 1, 'payload': b'hello'}, {'id': 2, 'payload': None}, {'id': 3, 'payload': b''}]
