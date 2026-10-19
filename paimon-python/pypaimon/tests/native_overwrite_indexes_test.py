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

"""Overwrite retires matching index metadata without deleting historical files."""

import json
from contextlib import nullcontext
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.tests import native_plan_rest_test


rest_catalog = native_plan_rest_test.rest_catalog
pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
_ENGINES = [False, pytest.param(True, marks=pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed'))]
_SCHEMA = pa.schema([('id', pa.int64()), ('pt', pa.string()), ('region', pa.string())])
_ROWS = [dict(id=1, pt='a', region='us'), dict(id=2, pt='a', region='us'),
         dict(id=3, pt='b', region='eu'), dict(id=4, pt='b', region='eu')]


def _write(table, rows):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=_SCHEMA))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _indexed_table(catalog, index_type):
    options = {
        'bucket': '-1', 'dynamic-bucket.target-row-num': '100',
        'dynamic-partition-overwrite': 'false',
        'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
        'commit.min-retry-wait': '0 ms', 'commit.max-retry-wait': '0 ms',
    }
    if index_type != 'HASH':
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
                        'deletion-vectors.enabled': 'true' if index_type == 'DELETION_VECTORS' else 'false'})
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        _SCHEMA, partition_keys=['pt', 'region'],
        primary_keys=['id', 'pt', 'region'] if index_type == 'HASH' else [], options=options), False)
    table = catalog.get_table('default.t')
    _write(table, _ROWS)
    if index_type == 'DELETION_VECTORS':
        _delete(table, [0, 2])
    elif index_type == 'btree':
        assert table.create_global_index('id') == 2
    entries = IndexFileHandler(table).scan(table.snapshot_manager().get_latest_snapshot())
    assert {entry.index_file.index_type for entry in entries} == {index_type}
    assert {entry.partition.values[0] for entry in entries} == {'a', 'b'}
    return table


def _delete(table, row_ids):
    builder = table.new_batch_write_builder()
    commit = builder.new_commit()
    try:
        commit.commit(builder.new_update().delete_by_row_id(row_ids))
    finally:
        commit.close()


def _rows(table, snapshot=None):
    if snapshot is not None:
        table = table._copy_with_snapshot(snapshot)
    builder = table.new_read_builder()
    return builder.new_read().to_arrow(builder.new_scan().plan().splits()).sort_by('id').to_pylist()


def _index_paths(table, entries):
    factory = table.path_factory()
    return [entry.index_file.external_path or (
        factory.bucket_index_path(tuple(entry.partition.values), entry.bucket, entry.index_file)
        if entry.index_file.index_type in ('HASH', 'DELETION_VECTORS') else
        factory.global_index_path_factory().to_path(entry.index_file.file_name)) for entry in entries]


def _native_only(commit, use_native):
    return (patch.object(commit.file_store_commit, '_try_commit', side_effect=AssertionError('Python fallback'))
            if use_native else nullcontext())


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('index_type', ['HASH', 'DELETION_VECTORS', 'btree'])
@pytest.mark.parametrize('operation', ['truncate-table', 'truncate-partitions', 'overwrite'])
def test_overwrite_retires_only_matching_index_metadata(rest_catalog, use_native, index_type, operation):
    catalog, _ = rest_catalog
    table = _indexed_table(catalog, index_type)
    before = table.snapshot_manager().get_latest_snapshot()
    original_rows = _rows(table)
    original_indexes = IndexFileHandler(table).scan(before)
    paths = _index_paths(table, original_indexes)
    table = table.copy({'commit.native.enabled': str(use_native).lower()})
    builder = table.new_batch_write_builder()
    if operation == 'overwrite':
        builder.overwrite({'pt': 'a'})
    commit = builder.new_commit()
    writer = None
    try:
        with _native_only(commit, use_native):
            if operation == 'truncate-table':
                commit.truncate_table()
            elif operation == 'truncate-partitions':
                commit.truncate_partitions([{'pt': 'a'}])
            else:
                writer = builder.new_write()
                writer.write_arrow(pa.Table.from_pylist([dict(id=10, pt='a', region='us')], schema=_SCHEMA))
                commit.commit(writer.prepare_commit())
        latest = table.snapshot_manager().get_latest_snapshot()
        remaining = IndexFileHandler(table).scan(latest)
        old_names = {entry.index_file.file_name for entry in original_indexes if entry.partition.values[0] == 'a'}
        assert not old_names.intersection(entry.index_file.file_name for entry in remaining)
        if operation == 'truncate-table':
            assert latest.index_manifest is None
            expected = []
        else:
            retained_names = {entry.index_file.file_name for entry in remaining if entry.partition.values[0] == 'b'}
            assert retained_names == {entry.index_file.file_name for entry in original_indexes
                                      if entry.partition.values[0] == 'b'}
            expected = [row for row in original_rows if row['pt'] == 'b']
            if operation == 'overwrite':
                expected = [dict(id=10, pt='a', region='us')] + expected
        assert _rows(table) == sorted(expected, key=lambda row: row['id'])
        assert _rows(table, before) == original_rows
        assert all(table.file_io.exists(path) for path in paths)
        assert table.file_io.exists(table.table_path.rstrip('/') + '/manifest/' + before.index_manifest)
    finally:
        if writer is not None:
            writer.close()
        commit.close()


@pytest.mark.parametrize('use_native', _ENGINES)
@pytest.mark.parametrize('index_type', ['HASH', 'DELETION_VECTORS'])
def test_truncate_retry_replans_indexes_added_by_concurrent_writer(rest_catalog, use_native, index_type):
    catalog, server = rest_catalog
    table = _indexed_table(catalog, index_type)
    original = table.snapshot_manager().get_latest_snapshot()
    original_rows = _rows(table)
    builder = table.copy({'commit.native.enabled': str(use_native).lower()}).new_batch_write_builder()
    commit = builder.new_commit()
    publish = server._table_commit_handle
    concurrent = []
    attempts = []

    def change_before_first_compare_and_swap(method, data, identifier, branch=None):
        request = json.loads(data)
        if request['snapshot']['commitUser'] == builder.commit_user:
            attempts.append(request['snapshot']['id'])
            if len(attempts) == 1:
                # A direct storage writer runs inside the single-threaded mock
                # REST server, avoiding a recursive HTTP request to that server.
                storage = server._get_file_table(identifier, branch)
                if index_type == 'HASH':
                    _write(storage, [dict(id=5, pt='a', region='us')])
                else:
                    _delete(storage, [1])
                snapshot = storage.snapshot_manager().get_latest_snapshot()
                concurrent.extend(IndexFileHandler(storage).scan(snapshot))
        return publish(method, data, identifier, branch)

    try:
        with patch.object(server, '_table_commit_handle', side_effect=change_before_first_compare_and_swap), \
                _native_only(commit, use_native):
            commit.truncate_partitions([{'pt': 'a'}])
        assert attempts == [original.id + 1, original.id + 2]
        latest = table.snapshot_manager().get_latest_snapshot()
        remaining = IndexFileHandler(table).scan(latest)
        assert {entry.partition.values[0] for entry in remaining} == {'b'}
        assert {entry.index_file.file_name for entry in remaining} == {
            entry.index_file.file_name for entry in concurrent if entry.partition.values[0] == 'b'}
        assert all(table.file_io.exists(path) for path in _index_paths(table, concurrent))
        assert _rows(table) == [row for row in original_rows if row['pt'] == 'b']
        assert _rows(table, original) == original_rows
    finally:
        commit.close()
