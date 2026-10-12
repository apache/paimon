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

"""Resolved read views travel through Native planning and every local search API."""

from contextlib import contextmanager
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon.read.scanner.file_scanner import FileScanner
from pypaimon.table.source.full_text_search_builder import FullTextSearchBuilderImpl
from pypaimon.table.source.vector_search_read import DataEvolutionVectorRead, BatchVectorSearchReadImpl
from pypaimon.tests import native_hybrid_search_test as hybrid, native_vector_search_test as vectors
from pypaimon.tests.native_full_text_search_test import _scores
from pypaimon.tests.vector_search_filter_test import match_query

rest_catalog = hybrid.rest_catalog
golden_catalog = vectors.golden_catalog
pk_vector_table = vectors.pk_vector_table
pytestmark = pytest.mark.native_plan


@contextmanager
def _native_only():
    with patch.object(FileScanner, 'scan', side_effect=AssertionError('Python file scan')), \
            patch.object(DataEvolutionVectorRead, 'read_plan', side_effect=AssertionError('Python vector read')), \
            patch.object(BatchVectorSearchReadImpl, 'read_batch_plan',
                         side_effect=AssertionError('Python batch read')), \
            patch.object(FullTextSearchBuilderImpl, 'new_full_text_scan',
                         side_effect=AssertionError('Python text scan')):
        yield


def _search(table, family):
    if family == 'scan':
        return hybrid.indexes._ids(table)
    if family == 'text':
        return _scores(table.new_full_text_search_builder().with_query('content', match_query('paimon'))
                       .with_limit(16).execute_local())
    if family == 'batch':
        return [_scores(result) for result in table.new_batch_vector_search_builder()
                .with_vector_column('embedding').with_query_vectors([[0., 0.], [6., 0.], [0., 0.]])
                .with_limit(16).execute_batch_local()]
    return _scores(table.new_vector_search_builder().with_vector_column('embedding')
                   .with_query_vector([0., 0.]).with_limit(16).execute_local())


@pytest.mark.parametrize('family', ['scan', 'vector', 'batch', 'text'])
def test_fixed_empty_view_does_not_follow_latest(rest_catalog, family):
    catalog, _ = rest_catalog
    table = hybrid._table(catalog)
    fixed = table._copy_with_snapshot(None)
    with _native_only():
        result = _search(fixed, family)
    assert result == ([{}, {}, {}] if family == 'batch' else [] if family == 'scan' else {})
    assert hybrid.indexes._ids(table) == list(range(8))


@pytest.mark.parametrize('family', ['scan', 'vector', 'batch', 'text'])
def test_retained_tag_metadata_is_used_without_original_snapshot_file(rest_catalog, family):
    catalog, _ = rest_catalog
    table = hybrid._table(catalog)
    snapshot = table.snapshot_manager().get_latest_snapshot()
    table.tag_manager().create_tag(snapshot, 'before')
    fixed = table.copy({'scan.tag-name': 'before'})._copy_with_snapshot(snapshot)
    hybrid.indexes._append(table, hybrid._rows(8, 4), schema=hybrid.SCHEMA)
    table.file_io.delete(table.snapshot_manager().get_snapshot_path(snapshot.id))
    classic = fixed.copy({'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    expected = _search(classic, family)
    with _native_only():
        result = _search(fixed, family)
    if family == 'scan':
        assert result == expected == list(range(8))
    elif family == 'batch':
        assert len(result) == 3
        for actual, reference in zip(result, expected):
            assert actual == pytest.approx(reference, abs=1e-6)
    else:
        assert result == pytest.approx(expected, abs=1e-6)
    assert hybrid.indexes._ids(table) == list(range(12))


def _query(catalog, table, family):
    from pypaimon.multimodal.table import MultimodalTable, text_route, vector_route
    multimodal = MultimodalTable(catalog, table.identifier, table)
    if family == 'batch':
        query = multimodal.search_vectors([[0., 0.], [6., 0.]], column='embedding')
    elif family == 'text':
        query = multimodal.search('paimon', column='content')
    elif family == 'hybrid':
        query = multimodal.search_hybrid([
            vector_route('embedding', [0., 0.]), text_route(match_query('paimon'), column='content')])
    else:
        query = multimodal.search([0., 0.], column='embedding')
    return query.pre_filter("label = 'keep'").select(['id', 'label']).limit(16)


@pytest.mark.parametrize('family', ['vector', 'batch', 'text', 'hybrid'])
@pytest.mark.parametrize('change', ['update', 'delete'])
def test_native_search_and_lookup_use_one_view_across_commit(rest_catalog, family, change):
    from pypaimon.multimodal.query import ScanQuery
    catalog, _ = rest_catalog
    schema = pa.schema([pa.field('embedding', pa.list_(pa.float32(), 2))
                        if field.name == 'embedding' else field for field in hybrid.SCHEMA])
    table = hybrid.indexes._create(catalog, schema=schema, partitioned=True, options={
        'read.native.enabled': 'true', 'vector-index.search-mode': 'full',
        'full-text-index.search-mode': 'full'})
    hybrid.indexes._append(table, hybrid._rows(), schema=schema)
    table.create_global_index('content', index_type='full-text')
    table.create_global_index('embedding', index_type='ivf-flat', options={
        'ivf-flat.dimension': '2', 'ivf-flat.nlist': '1', 'ivf-flat.distance.metric': 'l2'})
    table = table.copy({'deletion-vectors.enabled': 'true'})
    query = _query(catalog, table, family)
    expected = query.to_list()
    lookup = ScanQuery._read_global_index_result
    views = []
    write_builder = table.new_batch_write_builder()
    updater = write_builder.new_update()
    predicate = table.new_read_builder().new_predicate_builder().equal('id', 1)
    messages = (updater.update_by_predicate(predicate, {'label': 'drop'}) if change == 'update'
                else updater.delete_by_predicate(predicate))
    commit = write_builder.new_commit()

    def commit_before_lookup(execution, result):
        if not views:
            commit.commit(messages)
        views.append(execution._table)
        return lookup(execution, result)

    try:
        with _native_only(), hybrid._core_only(), \
                patch.object(ScanQuery, '_read_global_index_result', commit_before_lookup):
            assert query.to_list() == expected
    finally:
        commit.close()
    assert all(view._read_snapshot.id == views[0]._read_snapshot.id for view in views)
    with _native_only(), hybrid._core_only():
        latest = query.to_list()
    assert latest != expected
    for rows in (latest if family == 'batch' else [latest]):
        assert 1 not in {row['id'] for row in rows}
    assert not hasattr(query._table, '_read_snapshot')


@pytest.mark.parametrize('family', ['scan', 'vector', 'batch', 'text'])
def test_captured_view_skips_rest_snapshot_lookup_and_keeps_cache_isolated(rest_catalog, family):
    from pypaimon.api.api_response import ErrorResponse
    catalog, server = rest_catalog
    table = hybrid._table(catalog)
    first = table.snapshot_manager().get_latest_snapshot()
    fixed = table._copy_with_snapshot(first).copy({'read.batch-size': '1'})
    expected = _search(fixed.copy({'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'}), family)
    hybrid.indexes._append(table, hybrid._rows(8, 4), schema=hybrid.SCHEMA)
    reply = ErrorResponse('TABLE', 'indexes', 'latest must not be loaded', 503)
    with patch.object(server, '_table_snapshot_handle', return_value=server._mock_response(reply, 503)) as latest:
        with _native_only():
            actual = _search(fixed, family)
            empty = _search(table._copy_with_snapshot(None), family)
        assert latest.call_count == 0
    if family == 'batch':
        for scores, reference in zip(actual, expected):
            assert scores == pytest.approx(reference, abs=1e-6)
        assert empty == [{}, {}, {}]
    else:
        assert actual == (expected if family == 'scan' else pytest.approx(expected, abs=1e-6))
        assert empty == ([] if family == 'scan' else {})
    # The cached REST Table is a base view. Pinning copies cannot change it.
    assert hybrid.indexes._ids(table) == list(range(12))
    changed = fixed.copy({'scan.snapshot-id': None, 'scan.mode': 'default'})
    assert not hasattr(changed, '_read_snapshot')
    assert hybrid.indexes._ids(changed) == list(range(12))


@pytest.mark.parametrize('family', ['vector', 'batch', 'text'])
@pytest.mark.parametrize('response', ['empty', 'error'])
def test_rest_snapshot_response_is_authoritative_without_search_retry(rest_catalog, family, response):
    from pypaimon.api.api_response import ErrorResponse, GetTableSnapshotResponse
    catalog, server = rest_catalog
    table = hybrid._table(catalog)
    reply = GetTableSnapshotResponse() if response == 'empty' else ErrorResponse(
        'TABLE', 'indexes', 'snapshot unavailable', 503)
    code = 200 if response == 'empty' else 503
    with patch.object(server, '_table_snapshot_handle', return_value=server._mock_response(reply, code)) as latest:
        with _native_only():
            if response == 'error':
                with pytest.raises(Exception, match='snapshot unavailable'):
                    _search(table, family)
            else:
                assert _search(table, family) == ([{}, {}, {}] if family == 'batch' else {})
        assert latest.call_count == 1
    assert hybrid.indexes._ids(table) == list(range(8))


@pytest.mark.parametrize('family', ['vector', 'batch', 'text'])
def test_missing_index_preserves_native_error_without_retry(rest_catalog, family):
    catalog, _ = rest_catalog
    table = hybrid._table(catalog)
    index_type = 'full-text' if family == 'text' else 'ivf-flat'
    index = next(file for file in hybrid.indexes._files(table) if file.index_type == index_type)
    table.file_io.delete(hybrid.indexes._path(table, index))
    with _native_only(), pytest.raises(Exception, match='not found|No such file|does not exist|NotFound'):
        _search(table, family)
    assert hybrid.indexes._ids(table) == list(range(8))


def test_snapshot_json_binding_requires_java_snapshot_metadata(rest_catalog):
    from pypaimon.read.native_plan import _native_table
    catalog, _ = rest_catalog
    table = hybrid._table(catalog)
    with pytest.raises(ValueError, match='Invalid snapshot JSON'):
        _native_table(table).copy_with_pinned_snapshot('{}')
    with pytest.raises(ValueError, match='Invalid snapshot JSON'):
        _native_table(table).copy_with_pinned_snapshot('not json')
    assert hybrid.indexes._ids(table) == list(range(8))


@pytest.mark.parametrize('view', ['empty', 'tag'])
def test_primary_key_vector_search_uses_captured_metadata(pk_vector_table, view):
    from pypaimon.table.source.primary_key_vector_read import PrimaryKeyVectorRead
    table = pk_vector_table
    snapshot = table.snapshot_manager().get_latest_snapshot()
    if view == 'tag':
        table.tag_manager().create_tag(snapshot, 'before')
    fixed = table._copy_with_snapshot(snapshot if view == 'tag' else None)
    if view == 'tag':
        table.file_io.delete(table.snapshot_manager().get_snapshot_path(snapshot.id))

    def search(read_table):
        return (read_table.new_vector_search_builder().with_vector_column('embedding')
                .with_query_vector([1., 0., 0., 0.]).with_limit(2).execute_local())

    classic = fixed.copy({'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    expected = search(classic)
    native = fixed.copy({'read.native.enabled': 'true', 'scan.native-plan.enabled': 'true'})
    with _native_only(), patch.object(PrimaryKeyVectorRead, 'read_plan',
                                      side_effect=AssertionError('Python primary-key vector read')):
        result = search(native)
        read = native.new_read_builder()
        assert read.new_scan().plan().snapshot_id == (snapshot.id if view == 'tag' else None)
        rows = read.new_read().to_arrow(result.splits)
    assert result.snapshot_id == expected.snapshot_id
    assert [(position.bucket, position.data_file_name, position.row_position) for position in result.positions] == [
        (position.bucket, position.data_file_name, position.row_position) for position in expected.positions]
    assert [position.score for position in result.positions] == pytest.approx([
        position.score for position in expected.positions])
    assert sorted(rows.column('id').to_pylist()) == ([1, 2] if view == 'tag' else [])
