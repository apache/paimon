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

"""Native external data files interoperate with Python readers and committers."""

from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.read.native_plan import native_plan
from pypaimon.write.native_write import NativeTableWrite

pytestmark = pytest.mark.native_plan
_SCHEMA = pa.schema([('id', pa.int32()), ('pt', pa.string()), ('value', pa.int32())])


def _table(tmp_path, mode, strategy, native, partitioned=True, extra=None):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('db', True)
    paths = [(tmp_path / name).as_uri() for name in ['external-a', 'external-b']]
    if strategy == 'specific-fs':
        paths.insert(0, 's3://unused-bucket/data')
    options = {'write.native.enabled': str(native).lower(),
               'data-file.external-paths': ','.join(paths),
               'data-file.external-paths.strategy': strategy,
               'data-file.external-paths.weights': '1,2',
               'data-file.external-paths.specific-fs': 'FILE',
               'data-file.path-directory': 'data/nested'}
    partitions = ['pt'] if partitioned else []
    keys = []
    if mode == 'pk':
        keys = ['id'] + partitions
        options.update({'bucket': '1', 'changelog-producer': 'input'})
    elif mode == 'evolution':
        options.update({'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
                        'deletion-vectors.enabled': 'true', 'target-file-row-num': '1',
                        'index-file-in-data-file-dir': 'true'})
    options.update(extra or {})
    catalog.create_table('db.t', Schema.from_pyarrow_schema(
        _SCHEMA, primary_keys=keys, partition_keys=partitions, options=options), False)
    return catalog.get_table('db.t')


def _write(table, data, native, streaming=False, identifier=1):
    builder = table.new_stream_write_builder() if streaming else table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        assert isinstance(writer, NativeTableWrite) == native
        writer.write_arrow(data)
        messages = writer.prepare_commit(identifier) if streaming else writer.prepare_commit()
        for message in messages:
            for file in message.new_files + message.changelog_files:
                assert bool(file.external_path) == (table.options.data_file_external_paths_strategy() != 'none')
                assert table.file_io.exists(file.file_path), file.file_path
                if file.external_path:
                    assert '/external-' in file.external_path
                    assert not Path(table.table_path, file.file_name).exists()
        if streaming:
            commit.commit(messages, identifier)
        else:
            commit.commit(messages)
        if native:
            assert writer._python_writer is None
        return messages
    finally:
        writer.close()
        commit.close()


def _read(table, planner, reader, snapshot=None):
    options = {'scan.native-plan.enabled': str(planner).lower(),
               'read.native.enabled': str(reader).lower()}
    if snapshot is not None:
        options['scan.snapshot-id'] = str(snapshot)
    table = table.copy(options)
    builder = table.new_read_builder()
    plan = native_plan(table) if planner else builder.new_scan().plan()
    read = builder.new_read()
    with ExitStack() as stack:
        if reader:
            stack.enter_context(patch.object(read, '_create_split_read',
                                             side_effect=AssertionError('Python read fallback')))
        result = read.to_arrow(plan.splits())
    return sorted(result.to_pylist(), key=lambda row: row['id'])


@pytest.mark.parametrize('mode', ['append', 'pk', 'evolution'])
@pytest.mark.parametrize('strategy', ['none', 'round-robin', 'weight-robin', 'entropy-inject', 'specific-fs'])
@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('partitioned', [False, True])
def test_external_batch_and_stream_interoperability(tmp_path, mode, strategy, native, partitioned):
    table = _table(tmp_path, mode, strategy, native, partitioned)
    first = pa.table({'id': [1, 2], 'pt': ['a', 'b'], 'value': [10, 20]}, schema=_SCHEMA)
    second = pa.table({'id': [3], 'pt': ['a'], 'value': [30]}, schema=_SCHEMA)
    _write(table, first, native)
    _write(table, second, native, streaming=True, identifier=2)
    # Existing files must use their recorded paths after changing write destinations.
    table = table.copy({'data-file.external-paths': (tmp_path / 'new-location').as_uri()})
    for planner in (False, True):
        for reader in (False, True):
            assert _read(table, planner, reader) == first.to_pylist() + second.to_pylist()


@pytest.mark.parametrize('strategy', ['round-robin', 'entropy-inject'])
def test_native_external_escaped_partition_and_abort(tmp_path, strategy):
    table = _table(tmp_path, 'append', strategy, True)
    data = pa.table({'id': [1], 'pt': ['a/b%?#'], 'value': [10]}, schema=_SCHEMA)
    _write(table, data, True)
    for planner in (False, True):
        for reader in (False, True):
            assert _read(table, planner, reader) == data.to_pylist()
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(data)
        messages = writer.prepare_commit()
        files = [file for message in messages for file in message.new_files]
        assert files and all(table.file_io.exists(file.file_path) for file in files)
        commit.abort(messages)
        assert not any(table.file_io.exists(file.file_path) for file in files)
    finally:
        writer.close()
        commit.close()
    assert _read(table, True, True) == data.to_pylist()


@pytest.mark.parametrize('strategy', ['round-robin', 'entropy-inject'])
@pytest.mark.parametrize('native', [False, True])
def test_external_updates_upserts_deletes_history_and_abort(tmp_path, strategy, native):
    table = _table(tmp_path, 'evolution', strategy, native)
    original = pa.table({'id': [1, 2, 3], 'pt': ['a'] * 3, 'value': [10, 20, 30]}, schema=_SCHEMA)
    _write(table, original, native)
    builder = table.new_batch_write_builder()
    update = builder.new_update().with_update_type(['value'])
    messages = update.upsert_by_arrow_with_key(pa.table(
        {'id': [1, 4], 'pt': ['a', 'b'], 'value': [11, 40]}, schema=_SCHEMA), ['id'])
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
                assert [row['value'] for row in _read(table, planner, reader)] == expected
    messages = builder.new_update().delete_by_row_id([2])
    staged = [table.path_factory().bucket_index_path(
        tuple(entry.partition.values), entry.bucket, entry.index_file)
        for message in messages for entry in message.index_adds]
    assert staged and all(table.file_io.exists(path) for path in staged)
    commit = builder.new_commit()
    try:
        commit.abort(messages)
    finally:
        commit.close()
    assert not any(table.file_io.exists(path) for path in staged)
    assert _read(table, True, True, snapshot=1) == original.to_pylist()
    assert [row['value'] for row in _read(table, True, True)] == [30, 40]


@pytest.mark.parametrize('strategy', ['none', 'round-robin', 'entropy-inject'])
@pytest.mark.parametrize('native', [False, True])
def test_external_blob_writer_and_readers(tmp_path, strategy, native):
    from pypaimon.schema.data_types import AtomicType, DataField
    table = _table(tmp_path, 'evolution', strategy, native, partitioned=False)
    catalog = table.catalog_environment.catalog_loader.load()
    catalog.drop_table('db.t')
    fields = [DataField(0, 'id', AtomicType('INT')),
              DataField(1, 'first', AtomicType('BLOB')), DataField(2, 'second', AtomicType('BLOB'))]
    catalog.create_table('db.t', Schema(fields=fields, options=table.table_schema.options), False)
    table = catalog.get_table('db.t')
    data = pa.table({'id': pa.array([1, 2, 3], pa.int32()),
                     'first': pa.array([b'hello', None, b''], pa.large_binary()),
                     'second': pa.array([None, b'world', b'!'], pa.large_binary())})
    _write(table, data, native)
    for planner in (False, True):
        for reader in (False, True):
            assert _read(table, planner, reader) == data.to_pylist()


@pytest.mark.parametrize('mode', ['append', 'pk', 'evolution'])
def test_python_abort_removes_native_external_sidecars(tmp_path, mode):
    table = _table(tmp_path, mode, 'entropy-inject', True, extra={
        'commit.native.enabled': 'false', 'file-index.bloom-filter.columns': 'value',
        'file-index.bloom-filter.value.items': '10', 'file-index.in-manifest-threshold': '0 B'})
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        assert isinstance(writer, NativeTableWrite)
        writer.write_arrow(pa.table({'id': [1], 'pt': ['a/b%?#'], 'value': [10]}, schema=_SCHEMA))
        messages = writer.prepare_commit()
        files = [file for message in messages for file in message.new_files]
        assert files and all(file.extra_files for file in files)
        paths = [path for message in messages for file in message.new_files + message.changelog_files
                 for path in file.collect_files()]
        assert all(table.file_io.exists(path) for path in paths)
        commit.abort(messages)
        assert not any(table.file_io.exists(path) for path in paths)
    finally:
        writer.close()
        commit.close()
