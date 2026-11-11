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

"""Python readers keep their internal dependency reads on the same backend."""

import builtins
from contextlib import contextmanager
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.python.read.search import global_index_live_row_filter
from pypaimon.execution.python.read.search.full_text_read import DataEvolutionFullTextRead
from pypaimon.execution.python.read.search.global_index_row_filter import matching_rows
from pypaimon.execution.python.read.search.primary_key_full_text_read import PrimaryKeyFullTextRead
from pypaimon.execution.python.read.search.primary_key_vector_read import PrimaryKeyVectorRead
from pypaimon.execution.python.read.search.primary_key_vector_scan import _residual_row_ranges
from pypaimon.execution.python.read.search.vector_search_read import (
    BatchVectorSearchReadImpl, DataEvolutionVectorRead)
from pypaimon.table.row.blob import BlobViewStruct
from pypaimon.table.source.primary_key_scored_result import PrimaryKeySearchPosition, _partition_bytes
from pypaimon.table.source.search_types import PrimaryKeyVectorScanPlan, PrimaryKeyVectorSearchSplit
from pypaimon.utils.range import Range
from pypaimon.utils.roaring_bitmap import RoaringBitmap64

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]


@contextmanager
def _forbid_native():
    original_import = builtins.__import__

    def import_python_only(name, *args, **kwargs):
        if name.startswith(('pypaimon.execution.native', 'pypaimon_rust')):
            raise AssertionError('Selected Python reader entered Native: ' + name)
        return original_import(name, *args, **kwargs)

    with patch('builtins.__import__', side_effect=import_python_only):
        yield


def _write(table, data):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(data)
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def test_python_blob_view_lookup_keeps_native_upstream_in_python(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('picture', pa.large_binary())])
    options = {'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
               'write.native.enabled': 'false', 'commit.native.enabled': 'false',
               'scan.native-plan.enabled': 'true', 'read.native.enabled': 'true'}
    catalog.create_table('default.source', Schema.from_pyarrow_schema(schema, options=options), False)
    source = catalog.get_table('default.source')
    _write(source.copy({'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false'}),
           pa.table({'id': [1], 'picture': [b'upstream payload']}, schema=schema))
    catalog.create_table('default.target', Schema.from_pyarrow_schema(schema, options={
        **options, 'blob-view-field': 'picture', 'scan.native-plan.enabled': 'false',
        'read.native.enabled': 'false'}), False)
    target = catalog.get_table('default.target')
    _write(target, pa.table({'id': [2], 'picture': [
        BlobViewStruct.of(source, 'picture', 0).serialize()]}, schema=schema))

    with _forbid_native():
        builder = target.new_read_builder()
        result = builder.new_read().to_arrow(builder.new_scan().plan().splits())
    assert result.to_pylist() == [{'id': 2, 'picture': b'upstream payload'}]
    assert source.options.native_read_enabled()
    assert source.options.native_plan_enabled()
    assert not target.options.native_read_enabled()
    assert not target.options.native_plan_enabled()


@pytest.fixture
def search_table(tmp_path, request):
    primary_key = getattr(request, 'param', False)
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('text', pa.string()),
                        ('embedding', pa.list_(pa.float32()))])
    catalog.create_table('default.search', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] if primary_key else [], options={
            'bucket': '1' if primary_key else '-1',
            'row-tracking.enabled': 'false' if primary_key else 'true',
            'data-evolution.enabled': 'false' if primary_key else 'true',
            'deletion-vectors.enabled': 'true',
            'write.native.enabled': 'false', 'commit.native.enabled': 'false',
            'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'}), False)
    table = catalog.get_table('default.search')
    _write(table, pa.table({'id': [1, 2], 'text': ['first', 'second'],
                           'embedding': [[0., 0.], [1., 0.]]}, schema=schema))
    return table.copy_without_time_travel({
        'read.native.enabled': 'true', 'scan.native-plan.enabled': 'true'})


@pytest.mark.parametrize('path', ['vector', 'batch-vector', 'full-text', 'filter', 'live-rows'])
@pytest.mark.parametrize('captured', [False, True])
def test_python_search_dependency_reads_keep_real_rows_in_python(search_table, path, captured):
    table = search_table
    if captured:
        table = table._copy_with_snapshot(table.snapshot_manager().get_latest_snapshot())
    predicate = table.new_read_builder().new_predicate_builder().equal('id', 2)
    with _forbid_native():
        if path == 'vector':
            reader = DataEvolutionVectorRead(table, 2, table.field_dict['embedding'], [0., 0.])
            result = reader._read_raw_arrow([Range(0, 1)], include_filter=False)
            assert result.column('_ROW_ID').to_pylist() == [0, 1]
            assert result.column('embedding').to_pylist() == [[0., 0.], [1., 0.]]
        elif path == 'batch-vector':
            reader = BatchVectorSearchReadImpl(table, 2, table.field_dict['embedding'], [[0., 0.]])
            result = reader._read_raw_batch_search([Range(0, 1)], None)[0]
            assert result.results().to_list() == [0, 1]
            assert result.score_getter()(0) > result.score_getter()(1)
        elif path == 'full-text':
            reader = DataEvolutionFullTextRead(table, 2, table.field_dict['text'], 'first')
            result = reader._read_raw_rows([Range(1, 1)])
            assert result.column('text').to_pylist() == ['second']
        elif path == 'filter':
            candidates = RoaringBitmap64()
            candidates.add_range(0, 1)
            assert matching_rows(table, predicate, candidates).to_list() == [1]
        else:
            assert global_index_live_row_filter.live_rows(table).to_list() == [0, 1]
    assert table.options.native_read_enabled()
    assert table.options.native_plan_enabled()


@pytest.mark.parametrize('search_table', [True], indirect=True)
@pytest.mark.parametrize('path', ['raw-vector', 'rerank-vector', 'residual', 'raw-full-text'])
@pytest.mark.parametrize('captured', [False, True])
def test_python_primary_key_search_dependency_reads_keep_python(search_table, path, captured):
    table = search_table
    if captured:
        table = table._copy_with_snapshot(table.snapshot_manager().get_latest_snapshot())
    scan = table.copy_without_time_travel({
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'}).new_read_builder().new_scan()
    splits = scan.plan_all().splits()
    plan = PrimaryKeyVectorScanPlan(1, [PrimaryKeyVectorSearchSplit(
        split, (), tuple(file.file_name for file in split.files), {}) for split in splits])
    column = table.field_dict['embedding']
    reader = PrimaryKeyVectorRead(table, 2, column, [0., 0.], options={'metric': 'l2'})
    with _forbid_native():
        if path == 'raw-vector':
            candidates = list(reader._raw_candidates(plan))
            assert [candidate.row_position for candidate in candidates] == [0, 1]
            assert candidates[0].score > candidates[1].score
        elif path == 'rerank-vector':
            split = splits[0]
            candidates = [PrimaryKeySearchPosition(
                _partition_bytes(split.partition), split.bucket, split.files[0].file_name, pos, 0.)
                for pos in (0, 1)]
            result = reader._rerank_indexed(plan, candidates, 'ivf-flat')
            assert [candidate.row_position for candidate in result] == [0, 1]
            assert result[0].score > result[1].score
        elif path == 'residual':
            predicate = table.new_read_builder().new_predicate_builder().equal('id', 2)
            assert _residual_row_ranges(table, predicate, splits[0], {}) == {
                splits[0].files[0].file_name: (Range(1, 1),)}
        else:
            text_reader = PrimaryKeyFullTextRead.__new__(PrimaryKeyFullTextRead)
            text_reader._table, text_reader._text_columns = table, [table.field_dict['text']]
            with patch.object(text_reader, '_build_raw_index', return_value=None) as build_index:
                assert text_reader._raw_candidates(plan) == []
            build_index.assert_called_once_with([0, 1], ['first', 'second'], 0)
    assert table.options.native_read_enabled()
    assert table.options.native_plan_enabled()


@pytest.mark.parametrize('empty', [False, True])
def test_pinned_view_copies_do_not_load_backend(search_table, empty):
    snapshot = None if empty else search_table.snapshot_manager().get_latest_snapshot()
    pinned = search_table._copy_with_snapshot(snapshot)
    with _forbid_native():
        copied = pinned.copy_without_time_travel({'read.batch-size': '1'})
        copied = copied.copy({'read.batch-size': '2'})
    assert copied._read_snapshot is snapshot
    assert copied.table_schema.fields == pinned.table_schema.fields
    assert copied.current_branch() == pinned.current_branch()
    assert copied.options.native_read_enabled()
    assert copied.options.native_plan_enabled()
