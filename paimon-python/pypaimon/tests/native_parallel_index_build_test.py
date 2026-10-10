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

"""Parallel Rust preparation preserves shard IDs, commit ownership and REST snapshots."""

import json
import struct
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon.api.api_response import GetTableSnapshotResponse
from pypaimon.globalindex.create_global_index import GlobalIndexBuilder
from pypaimon.read.native_plan import _native_table
from pypaimon.snapshot.table_snapshot import TableSnapshot
from pypaimon.tests import native_generic_index_build_test as generic
from pypaimon.tests import native_sorted_index_build_test as indexes
from pypaimon.tests import native_vector_index_build_test as vectors

rest_catalog = indexes.rest_catalog
pytestmark = generic.pytestmark
SCHEMA = pa.schema([('id', pa.int32()), ('name', pa.string()),
                    ('embedding', pa.list_(pa.float32())), ('pt', pa.int32())])
PARALLELISM = 'global-index.build.parallelism'


def _rows(start, count):
    return [{'id': i, 'name': 'paimon index' if i % 2 else None,
             'embedding': [1., 0.] if i >= 4 and i % 2 else None, 'pt': i // 16}
            for i in range(start, start + count)]


def _table(catalog, options=None, partitioned=False):
    settings = {PARALLELISM: '3', 'vector-index.search-mode': 'fast', 'full-text-index.search-mode': 'fast'}
    settings.update(options or {})
    table = indexes._create(catalog, schema=SCHEMA, options=settings, partitioned=partitioned)
    indexes._append(table, _rows(0, 20), schema=SCHEMA)
    return table


def _build(table, kind, options=None, **kwargs):
    settings = generic._options(kind, 4)
    settings.update(options or {})
    with patch.object(GlobalIndexBuilder, '_build_generic_index',
                      side_effect=AssertionError('Python rows were materialized')):
        return GlobalIndexBuilder(table, 'name' if kind == 'full-text' else 'embedding',
                                  index_type=kind, options=settings, **kwargs).build()


def _metadata(file):
    meta = file.global_index_meta
    return (file.index_type, file.row_count, meta.index_field_id, meta.row_range_start, meta.row_range_end,
            meta.source_meta, json.loads(meta.index_meta.decode('utf-8')))


def _assert_ids(table, kind, expected):
    if kind == 'full-text':
        classic = table.copy({'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
        result = (classic.new_full_text_search_builder().with_query('name', '{"match":{"query":"paimon"}}')
                  .with_limit(64).execute_local())
        assert sorted(result.results()) == expected
    else:
        vectors._assert_ids(table, expected, kind)


@pytest.mark.parametrize('kind', generic.KINDS)
@pytest.mark.parametrize('option_source', ['table', 'build'])
@pytest.mark.parametrize('native_commit', [False, True])
def test_parallel_preparation_matches_serial_and_incremental_results(rest_catalog, kind, option_source, native_commit):
    catalog, _ = rest_catalog
    table = _table(catalog, {PARALLELISM: '3' if option_source == 'table' else '1',
                             'commit.native.enabled': str(native_commit).lower()})
    options = {} if option_source == 'table' else {PARALLELISM: '3'}
    before = table.snapshot_manager().get_latest_snapshot().id
    control = indexes._message_files(_build(table, kind, {PARALLELISM: '1'}))
    messages = _build(table, kind, options)
    files = indexes._message_files(messages)
    assert table.snapshot_manager().get_latest_snapshot().id == before
    assert indexes._files(table) == []
    assert [_metadata(file) for file in files] == [_metadata(file) for file in control]
    assert [file.global_index_meta.row_range_start for file in files] == (
        [0, 4, 8, 12, 16] if kind == 'full-text' else [4, 8, 12, 16])
    assert all(struct.unpack('>iiq', file.global_index_meta.source_meta) == (0x44454958, 1, before)
               for file in files)
    if kind != 'full-text':
        assert [vectors._bytes(table, file) for file in files] == [vectors._bytes(table, file) for file in control]
    indexes._commit(table, messages)
    assert table.snapshot_manager().get_latest_snapshot().id == before + 1
    _assert_ids(table, kind, list(range(1 if kind == 'full-text' else 5, 20, 2)))
    assert _build(table, kind, options) == []
    indexes._append(table, _rows(20, 4), schema=SCHEMA)
    added = _build(table, kind, options)
    new_files = indexes._message_files(added)
    assert len(new_files) == 1
    assert new_files[0].global_index_meta.row_range_start == 20
    assert new_files[0].global_index_meta.row_range_end == 23
    indexes._commit(table, added)
    _assert_ids(table, kind, list(range(1 if kind == 'full-text' else 5, 24, 2)))
    # This also exercises actual native planning in the native-plan CI session.
    assert indexes._ids(table) == list(range(24))


@pytest.mark.parametrize('kind', generic.KINDS)
def test_parallel_partition_selection_preserves_global_row_ids(rest_catalog, kind):
    catalog, _ = rest_catalog
    table = _table(catalog, partitioned=True)
    predicates = table.new_read_builder().new_predicate_builder()
    messages = _build(table, kind, partition_filter=predicates.equal('pt', 1))
    assert {message.partition for message in messages} == {(1,)}
    files = indexes._message_files(messages)
    assert len(files) == 1
    assert (files[0].global_index_meta.row_range_start, files[0].global_index_meta.row_range_end) == (16, 19)
    indexes._commit(table, messages)
    _assert_ids(table, kind, [17, 19])
    assert indexes._ids(table, predicates.equal('pt', 1)) == [16, 17, 18, 19]


@pytest.mark.parametrize('kind', generic.KINDS)
@pytest.mark.parametrize('value,expected', [(2.0, '2'), (True, '1'), (None, '1')])
def test_typed_build_parallelism_uses_existing_option_conversion(rest_catalog, kind, value, expected):
    catalog, _ = rest_catalog
    table = _table(catalog)
    # Observe the control passed over FFI while retaining the real Rust builder.
    native = Mock(wraps=_native_table(table).new_global_index_build_builder())
    proxy = SimpleNamespace(new_global_index_build_builder=lambda: native)
    with patch('pypaimon.globalindex.native_index_build._native_table', return_value=proxy):
        messages = _build(table, kind, {PARALLELISM: value})
    assert native.with_options.call_args[0][0][PARALLELISM] == expected
    assert len(indexes._message_files(messages)) == (5 if kind == 'full-text' else 4)


@pytest.mark.parametrize('kind', ['full-text', 'ivf-flat'])
@pytest.mark.parametrize('option_source', ['table', 'build'])
@pytest.mark.parametrize('value', ['0', '-1', 'invalid'])
def test_invalid_parallelism_fails_before_output_creation(rest_catalog, kind, option_source, value):
    catalog, _ = rest_catalog
    table = _table(catalog, {PARALLELISM: value} if option_source == 'table' else None)
    with pytest.raises(Exception, match='parallelism|integer|invalid literal'):
        _build(table, kind, {PARALLELISM: value} if option_source == 'build' else None)
    assert table.snapshot_manager().get_latest_snapshot().id == 1
    root = table.path_factory().global_index_path_factory().global_index_root_path()
    assert not table.file_io.exists(root) or table.file_io.list_status(root) == []


@pytest.mark.parametrize('kind', vectors.VINDEX_IDENTIFIERS)
@pytest.mark.parametrize('external', [False, True])
def test_failed_parallel_preparation_retains_previous_caller_outputs(rest_catalog, tmp_path, kind, external):
    catalog, _ = rest_catalog
    root = (tmp_path / 'indexes').as_uri()
    table = _table(catalog, {'global-index.external-path': root} if external else None)
    handed_off = indexes._message_files(_build(table, kind))
    invalid = [{'id': i, 'name': None, 'embedding': [1., None], 'pt': 1} for i in range(20, 24)]
    indexes._append(table, invalid, schema=SCHEMA)
    before = table.snapshot_manager().get_latest_snapshot().id
    with pytest.raises(Exception, match='null vector element'):
        _build(table, kind)
    assert table.snapshot_manager().get_latest_snapshot().id == before
    assert indexes._files(table) == []
    assert all(table.file_io.exists(indexes._path(table, file)) for file in handed_off)
    directory = root if external else table.path_factory().global_index_path_factory().global_index_root_path()
    assert len(table.file_io.list_status(directory)) == len(handed_off)


@pytest.mark.parametrize('kind', ['full-text', 'ivf-flat'])
@pytest.mark.parametrize('native_commit', [False, True])
def test_parallel_messages_survive_commit_source_conflict(rest_catalog, kind, native_commit):
    catalog, _ = rest_catalog
    table = _table(catalog, {'commit.native.enabled': str(native_commit).lower()})
    messages = _build(table, kind)
    column = 'name' if kind == 'full-text' else 'embedding'
    data = pa.table({'_ROW_ID': pa.array([5], pa.int64()),
                     column: pa.array(['changed' if kind == 'full-text' else [99., 99.]], SCHEMA.field(column).type)})
    indexes._commit(table, table.new_batch_write_builder().new_update().update_by_arrow_with_row_id(data))
    before = table.snapshot_manager().get_latest_snapshot().id
    with pytest.raises(Exception, match='Global index source conflict'):
        indexes._commit(table, messages)
    assert table.snapshot_manager().get_latest_snapshot().id == before
    assert all(table.file_io.exists(indexes._path(table, file)) for file in indexes._message_files(messages))


@pytest.mark.parametrize('kind', generic.KINDS)
@pytest.mark.parametrize('response', ['first', 'empty'])
def test_parallel_build_obeys_rest_snapshot_response(rest_catalog, kind, response):
    catalog, server = rest_catalog
    table = _table(catalog)
    first = table.snapshot_manager().get_latest_snapshot()
    indexes._append(table, _rows(20, 4), schema=SCHEMA)
    reply = (GetTableSnapshotResponse(TableSnapshot(first, 1, 0, 19, first.time_millis)) if response == 'first'
             else GetTableSnapshotResponse())
    with patch.object(server, '_table_snapshot_handle', return_value=server._mock_response(reply, 200)):
        messages = _build(table, kind)
    files = indexes._message_files(messages)
    if response == 'empty':
        assert files == []
    else:
        assert sum(file.row_count for file in files) == (20 if kind == 'full-text' else 16)
        assert max(file.global_index_meta.row_range_end for file in files) == 19
        assert all(struct.unpack('>iiq', file.global_index_meta.source_meta)[2] == first.id for file in files)
    assert table.snapshot_manager().get_latest_snapshot().id == 2
