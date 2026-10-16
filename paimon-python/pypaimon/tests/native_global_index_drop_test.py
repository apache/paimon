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

"""Rust plans index drops; Python retains commit and dry-run ownership."""

from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from pypaimon.globalindex.drop_global_index import GlobalIndexDropper
from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.read.native_plan import _native_table, _predicate_to_native, native_method_available
from pypaimon.tests import native_sorted_index_build_test as indexes
from pypaimon.write.table_commit import BatchTableCommit

rest_catalog = indexes.rest_catalog
native_drop = pytest.mark.skipif(
    not native_method_available('Table', 'new_global_index_drop_builder'),
    reason='Rust global index drop builder required')


@native_drop
def test_preparation_failure_does_not_retry_python_planning():
    table = SimpleNamespace(
        field_dict={'id': None},
        options=SimpleNamespace(native_write_enabled=lambda: True, query_auth_enabled=False))
    native = Mock()
    native.with_index_columns.return_value = native
    native.with_index_type.return_value = native
    native.build.side_effect = OSError('metadata read failed')
    factory = SimpleNamespace(supports_index_type=lambda kind: True)
    with patch('pypaimon.read.native_plan.native_method_available', return_value=True), \
            patch('pypaimon.write.native_commit._rest_catalog_supported', return_value=True), \
            patch('pypaimon.read.native_plan._native_table') as native_table, \
            patch('pypaimon_rust.datafusion.GlobalIndexDropBuilder', factory, create=True), \
            patch.object(IndexFileHandler, 'scan', side_effect=AssertionError('Python fallback')):
        native_table.return_value.new_global_index_drop_builder.return_value = native
        with pytest.raises(OSError, match='metadata read failed'):
            GlobalIndexDropper(table, 'id').build()
    native.build.assert_called_once_with()
    native.execute.assert_not_called()


@pytest.mark.native_plan
@native_drop
@pytest.mark.parametrize('kind', ['btree', 'bitmap'])
@pytest.mark.parametrize('selection', ['all', 'dict', 'list', 'null', 'predicate'])
def test_prepare_and_dry_run_preserve_files_until_explicit_commit(rest_catalog, kind, selection):
    catalog, _ = rest_catalog
    table = indexes._create(catalog, partitioned=True)
    indexes._append(table, indexes.ROWS)
    table.create_global_index('name', index_type=kind)
    table.create_global_index('id', index_type=kind)
    entries = IndexFileHandler(table).scan(None)
    all_files = [entry.index_file for entry in entries]
    predicates = table.new_read_builder().new_predicate_builder()
    kwargs = {
        'all': {},
        'dict': {'partitions': {'pt': 1}},
        'list': {'partitions': [{'pt': 0}, {'pt': 1}]},
        'null': {'partitions': {'pt': None}},
        'predicate': {'partition_filter': predicates.equal('pt', 0)},
    }[selection]
    selected_partitions = {
        'all': {0, 1, None}, 'dict': {1}, 'list': {0, 1}, 'null': {None}, 'predicate': {0},
    }[selection]
    expected = {entry.index_file.file_name for entry in entries
                if entry.index_file.global_index_meta.index_field_id == table.field_dict['name'].id
                and entry.partition.values[0] in selected_partitions}
    before = table.snapshot_manager().get_latest_snapshot().id
    with patch.object(IndexFileHandler, 'scan', side_effect=AssertionError('Python drop planning')):
        messages = GlobalIndexDropper(table, 'name', index_type=kind, **kwargs).build()
        with patch.object(BatchTableCommit, 'commit', side_effect=AssertionError('dry run committed')):
            assert table.drop_global_index('name', index_type=kind, dry_run=True, **kwargs) == len(expected)
    assert {entry.index_file.file_name for message in messages for entry in message.index_deletes} == expected
    assert all(not message.new_files and not message.index_adds for message in messages)
    assert table.snapshot_manager().get_latest_snapshot().id == before
    assert {file.file_name for file in indexes._files(table)} == {file.file_name for file in all_files}
    indexes._commit(table, messages)
    assert {file.file_name for file in indexes._files(table)} == {
        file.file_name for file in all_files if file.file_name not in expected}
    # Old snapshots still refer to the removed metadata's files.
    assert all(table.file_io.exists(indexes._path(table, file)) for file in all_files)
    assert table.drop_global_index('name', index_type=kind, **kwargs) == 0
    assert table.snapshot_manager().get_latest_snapshot().id == before + 1


@pytest.mark.native_plan
@native_drop
def test_native_drop_binding_conjunction_and_execute_dry_run(rest_catalog):
    catalog, _ = rest_catalog
    table = indexes._create(catalog, partitioned=True)
    indexes._append(table, indexes.ROWS)
    table.create_global_index('name')
    before = table.snapshot_manager().get_latest_snapshot().id
    predicates = table.new_read_builder().new_predicate_builder()
    native = _native_table(table).new_global_index_drop_builder().with_index_column('name')
    native.with_partition_filter(_predicate_to_native(predicates.equal('pt', 0)))
    native.with_partition_filter(_predicate_to_native(predicates.equal('pt', 1)))
    assert native.build() == []
    assert native.execute() == 0
    assert table.snapshot_manager().get_latest_snapshot().id == before
    with pytest.raises(Exception, match='only partition keys'):
        native.with_partition_filter(_predicate_to_native(predicates.equal('id', 1)))
    native = _native_table(table).new_global_index_drop_builder().with_index_column('name')
    files = indexes._files(table)
    assert native.with_dry_run(True).execute() == len(files)
    assert table.snapshot_manager().get_latest_snapshot().id == before
    assert len(native.build()) > 0
    assert native.with_dry_run(False).execute() == len(files)
    assert indexes._files(table) == []
    assert all(table.file_io.exists(indexes._path(table, file)) for file in files)


@pytest.mark.native_plan
@native_drop
def test_lost_drop_commit_response_preserves_index_files(rest_catalog):
    catalog, _ = rest_catalog
    table = indexes._create(catalog)
    indexes._append(table, indexes.ROWS)
    table.create_global_index('name')
    files = indexes._files(table)
    before = table.snapshot_manager().get_latest_snapshot().id
    original = BatchTableCommit.commit

    def publish_then_raise(commit, messages, *args, **kwargs):
        original(commit, messages, *args, **kwargs)
        raise OSError('commit response lost')

    with patch.object(BatchTableCommit, 'commit', publish_then_raise):
        with pytest.raises(OSError, match='commit response lost'):
            table.drop_global_index('name')
    assert table.snapshot_manager().get_latest_snapshot().id == before + 1
    assert indexes._files(table) == []
    assert all(table.file_io.exists(indexes._path(table, file)) for file in files)


@pytest.mark.native_plan
@native_drop
def test_empty_drop_validates_columns_and_publishes_nothing(rest_catalog):
    catalog, _ = rest_catalog
    table = indexes._create(catalog)
    assert GlobalIndexDropper(table, 'name').build() == []
    assert table.drop_global_index('name') == 0
    assert table.drop_global_index('name', dry_run=True) == 0
    assert table.snapshot_manager().get_latest_snapshot() is None
    native = _native_table(table).new_global_index_drop_builder()
    with pytest.raises(Exception, match='column is required'):
        native.build()
