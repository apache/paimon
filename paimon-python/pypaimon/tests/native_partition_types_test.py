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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Java partition values across native append, PK and data-evolution writes."""

from unittest.mock import Mock, patch
from urllib.parse import urlparse

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.write.native_write import NativeTableWrite


_PARTITIONS = [
    (pa.float32(), [1.5, 42., 0.0001, None], ['1.5', '42.0', '1.0E-4', '__DEFAULT_PARTITION__']),
    (pa.float64(), [2.25, 42., 1e23, None], ['2.25', '42.0', '1.0E23', '__DEFAULT_PARTITION__']),
    (pa.binary(), [b'a/b', '\u6c49\u5b57'.encode(), b' \t', None],
     ['a%2Fb', '\u6c49\u5b57', '__DEFAULT_PARTITION__', '__DEFAULT_PARTITION__']),
    (pa.binary(3), [b'a/b', b'x=y', b' \t ', None],
     ['a%2Fb', 'x%3Dy', '__DEFAULT_PARTITION__', '__DEFAULT_PARTITION__']),
]


def _table(tmp_path, partition_type, mode='append', native=True, placement='local', extra_options=None):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('db', True)
    options = {'partition.legacy-name': 'false', 'write.native.enabled': str(native).lower(),
               'scan.native-plan.enabled': 'true', 'read.native.enabled': 'true'}
    if mode == 'pk':
        options['bucket'] = '1'
    elif mode == 'evolution':
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'})
    if placement == 'directory':
        options['data-file.path-directory'] = 'data'
    elif placement == 'external':
        options['data-file.external-paths'] = (tmp_path / 'external').as_uri()
        options['data-file.external-paths.strategy'] = 'round-robin'
    options.update(extra_options or {})
    arrow_schema = pa.schema([('id', pa.int32()), ('p', partition_type), ('value', pa.int32())])
    catalog.create_table('db.t', Schema.from_pyarrow_schema(
        arrow_schema, partition_keys=['p'], primary_keys=['p', 'id'] if mode == 'pk' else [],
        options=options), False)
    return catalog.get_table('db.t'), arrow_schema


def _read(table):
    builder = table.new_read_builder()
    return sorted(builder.new_read().to_arrow(builder.new_scan().plan().splits()).to_pylist(),
                  key=lambda row: row['id'])


def _write(table, schema, rows, stream=False):
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer = builder.new_write()
    try:
        if table.options.native_write_enabled():
            assert isinstance(writer, NativeTableWrite)
        writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
        messages = writer.prepare_commit(71) if stream else writer.prepare_commit()
        commit = builder.new_commit()
        try:
            commit.commit(messages, 71) if stream else commit.commit(messages)
        finally:
            commit.close()
        return messages
    finally:
        writer.close()


@pytest.mark.native_plan
@pytest.mark.parametrize('partition_type,values,names', _PARTITIONS)
@pytest.mark.parametrize('mode', ['append', 'pk', 'evolution'])
@pytest.mark.parametrize('placement', ['local', 'directory', 'external'])
def test_native_partition_write_read_and_file_paths(tmp_path, partition_type, values, names, mode, placement):
    table, schema = _table(tmp_path, partition_type, mode, placement=placement)
    rows = [dict(id=i, p=value, value=10 + i) for i, value in enumerate(values)]
    if mode == 'pk':
        rows = [row for row in rows if row['p'] is not None]
        names = [name for value, name in zip(values, names) if value is not None]
    messages = _write(table, schema, rows)
    assert _read(table) == pa.Table.from_pylist(rows, schema=schema).to_pylist()
    paths = []
    for message in messages:
        for file in message.new_files:
            assert table.file_io.exists(file.file_path)
            if placement == 'external':
                assert file.external_path is not None
                assert urlparse(file.external_path).path.startswith(str(tmp_path / 'external') + '/')
            paths.append(file.file_path)
    for name in names:
        assert any('/p=' + name + '/' in path for path in paths)


@pytest.mark.native_plan
@pytest.mark.parametrize('partition_type,values,names', _PARTITIONS)
@pytest.mark.parametrize('stream', [False, True])
def test_native_evolution_updates_use_typed_partition_values(tmp_path, partition_type, values, names, stream):
    table, schema = _table(tmp_path, partition_type, 'evolution')
    rows = [dict(id=i, p=value, value=i) for i, value in enumerate(values)]
    _write(table, schema, rows, stream)
    # Fail if the Python update planner or file writer takes over the operation.
    with patch('pypaimon.write.table_update.BatchTableUpdate._build_predicate_update_table',
               side_effect=AssertionError('Python row-ID update')):
        builder = table.new_batch_write_builder()
        messages = builder.new_update().update_by_predicate(None, {'value': 99})
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    expected = pa.Table.from_pylist(rows, schema=schema).to_pylist()
    assert _read(table) == [dict(row, value=99) for row in expected]


@pytest.mark.python_write
@pytest.mark.native_plan
@pytest.mark.parametrize('partition_type,values,names', _PARTITIONS[2:])
def test_python_binary_partitions_use_java_paths_and_native_reads(tmp_path, partition_type, values, names):
    table, schema = _table(tmp_path, partition_type, native=False)
    rows = [dict(id=i, p=value, value=i) for i, value in enumerate(values)]
    messages = _write(table, schema, rows)
    for message in messages:
        for file in message.new_files:
            assert table.file_io.exists(file.file_path)
    assert _read(table) == rows


@pytest.mark.native_plan
def test_native_deletion_vector_restore_reads_java8_partition_directory(tmp_path):
    table, schema = _table(tmp_path, pa.float64(), 'evolution', extra_options={
        'deletion-vectors.enabled': 'true', 'index-file-in-data-file-dir': 'true'})
    _write(table, schema, [dict(id=i, p=1e23, value=i) for i in range(3)])

    def delete(row_id):
        builder = table.new_batch_write_builder()
        messages = builder.new_update().delete_by_row_id([row_id])
        commit = builder.new_commit()
        try:
            commit.commit(messages)
        finally:
            commit.close()

    delete(0)
    from pathlib import Path
    directory = Path(table.table_path)
    (directory / 'p=1.0E23').rename(directory / 'p=9.999999999999999E22')
    assert [row['id'] for row in _read(table)] == [1, 2]
    delete(1)
    assert [row['id'] for row in _read(table)] == [2]


@pytest.mark.native_plan
@pytest.mark.parametrize('action', ['DELETE', 'UPDATE SET value = 99'])
def test_native_cow_merge_preserves_java8_file_identity(tmp_path, action):
    from pathlib import Path
    from pypaimon_rust.datafusion import SQLContext

    table, schema = _table(tmp_path, pa.float64())
    _write(table, schema, [dict(id=i, p=1e23, value=i) for i in range(3)])
    directory = Path(table.table_path)
    (directory / 'p=1.0E23').rename(directory / 'p=9.999999999999999E22')
    context = SQLContext()
    context.register_catalog('paimon', {'warehouse': str(tmp_path / 'warehouse')})
    context.sql('MERGE INTO paimon.db.t t USING (SELECT CAST(1 AS INT) AS id) s '
                'ON t.id = s.id WHEN MATCHED THEN ' + action)
    expected = [dict(id=i, p=1e23, value=99 if action.startswith('UPDATE') and i == 1 else i)
                for i in range(3) if action != 'DELETE' or i != 1]
    assert _read(table) == expected


@pytest.mark.parametrize('value,expected', [
    (b'a/b=c', 'a%2Fb%3Dc'), (b'\xed\xa0\x80', '\ufffd'), (b'\xed\xa0A', '\ufffdA'),
    (b'\xe2\x82', '\ufffd'), (b'\xe0\x80\x80', '\ufffd\ufffd\ufffd'),
    (b'', '__DEFAULT_PARTITION__'), (b' \t\x1c', '__DEFAULT_PARTITION__'),
    ('\u00a0'.encode(), '\u00a0'), ('\u0085'.encode(), '\u0085'),
])
def test_binary_partition_string_conversion_matches_java(tmp_path, value, expected):
    table, _ = _table(tmp_path, pa.binary(), native=False)
    assert table.path_factory().relative_bucket_path((value,), 0, canonical_partition=True) == (
        'p=' + expected + '/bucket-0')


def test_python_partition_statistics_keep_java_values_and_typed_identity(tmp_path):
    from pypaimon.manifest.schema.manifest_entry import ManifestEntry
    from pypaimon.table.row.generic_row import GenericRow
    from pypaimon.write.file_store_commit import FileStoreCommit

    table, _ = _table(tmp_path, pa.binary(), native=False)
    commit = FileStoreCommit.__new__(FileStoreCommit)
    commit.table = table
    entries = [ManifestEntry(0, GenericRow([value], table.partition_keys_fields), 0, 1,
                             Mock(row_count=2, file_size=10, creation_time=None))
               for value in [None, b' \t', b'a/b', b'\xed\xa0\x80', bytearray(b'a/b'), memoryview(b'a/b')]]
    statistics = commit._generate_partition_statistics(entries)
    # NULL and whitespace name the same directory, but remain different typed
    # partitions, as in Java PartitionEntry.merge and Rust BinaryRow grouping.
    assert [stat.spec for stat in statistics] == [
        {'p': '__DEFAULT_PARTITION__'}, {'p': '__DEFAULT_PARTITION__'}, {'p': 'a/b'}, {'p': '\ufffd'}]
    assert [stat.record_count for stat in statistics] == [2, 2, 6, 2]


@pytest.mark.parametrize('legacy', [False, True])
def test_native_commit_binary_partition_capability_precedes_execution(tmp_path, legacy):
    from pypaimon.write.native_commit import create_native_commit

    table, _ = _table(tmp_path, pa.binary(), native=False,
                      extra_options={'partition.legacy-name': str(legacy).lower()})
    with patch('pypaimon.write.native_commit._rest_catalog_supported', return_value=True), \
            patch('pypaimon.write.native_commit.native_commit_available', return_value=True), \
            patch('pypaimon.write.native_commit.create_native_write_table') as native:
        committer = create_native_commit(table, 'user')
        if legacy:
            assert committer is None
            native.assert_not_called()
        else:
            assert committer is not None
            native.assert_called_once_with(table)
