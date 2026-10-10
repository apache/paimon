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

"""REST vector builds delegated to Rust preserve Java writer semantics."""

from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon.globalindex.create_global_index import GlobalIndexBuilder
from pypaimon.globalindex.vindex.vindex_vector_global_index_reader import (
    PaimonVindexInput, VINDEX_IDENTIFIERS,
)
from pypaimon.tests import native_generic_index_build_test as generic
from pypaimon.tests import native_sorted_index_build_test as indexes
from pypaimon.read.native_plan import _native_table

rest_catalog = indexes.rest_catalog
pytestmark = generic.pytestmark
SCHEMA = pa.schema([('id', pa.int32()), ('embedding', pa.list_(pa.float32())), ('pt', pa.int32())])


def _rows(vectors, start=0, partition=0):
    return [{'id': start + i, 'embedding': vector, 'pt': partition} for i, vector in enumerate(vectors)]


def _table(catalog, vectors, options=None, partitioned=False):
    settings = {'vector-index.search-mode': 'fast'}
    settings.update(options or {})
    table = indexes._create(catalog, schema=SCHEMA, options=settings, partitioned=partitioned)
    indexes._append(table, _rows(vectors), schema=SCHEMA)
    return table


def _build(table, kind, options=None, **kwargs):
    with patch.object(GlobalIndexBuilder, '_build_generic_index',
                      side_effect=AssertionError('Python vector rows were materialized')):
        return GlobalIndexBuilder(table, 'embedding', index_type=kind, options=options, **kwargs).build()


def _metadata(table, file):
    from paimon_vindex import VectorIndexReader
    with table.file_io.new_input_stream(indexes._path(table, file)) as stream:
        adapter = PaimonVindexInput(stream)
        try:
            with VectorIndexReader(adapter) as reader:
                return reader.metadata()
        finally:
            adapter.close()


def _bytes(table, file):
    with table.file_io.new_input_stream(indexes._path(table, file)) as stream:
        return stream.read()


def _assert_ids(table, expected, kind):
    # Both readers must observe original IDs rather than a compacted 0..N sequence.
    search_options = {'diskann.l_search': '64'} if kind == 'diskann' else {'ivf.nprobe': '64'}
    classic = table.copy({'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    result = (classic.new_vector_search_builder().with_vector_column('embedding')
              .with_query_vector([1., 1.]).with_limit(64).with_options(search_options).execute_local())
    assert sorted(result.results()) == expected
    result = (_native_table(table).new_vector_search_builder().with_vector_column('embedding')
              .with_query_vector([1., 1.]).with_limit(64).with_options(search_options).execute_local())
    assert sorted(result.row_ids()) == expected


@pytest.mark.parametrize('kind', VINDEX_IDENTIFIERS)
@pytest.mark.parametrize('granule', [False, True])
def test_nullable_vectors_preserve_row_ids_and_logical_cardinality(rest_catalog, kind, granule):
    catalog, _ = rest_catalog
    vectors = [None, [1., 0.], None, [0., 1.], None, None, [1., 1.], None]
    table = _table(catalog, vectors)
    options = generic._options(kind)
    options[kind + '.train.sample-ratio'] = '0.5'
    if kind != 'diskann':
        options['vindex.build.granule.enabled'] = str(granule).lower()
    before = table.snapshot_manager().get_latest_snapshot().id
    messages = _build(table, kind, options)
    files = indexes._message_files(messages)
    assert len(files) == 1
    assert files[0].row_count == 8
    assert files[0].global_index_meta.row_range_start == 0
    assert files[0].global_index_meta.row_range_end == 7
    assert _metadata(table, files[0]).total_vectors == 3
    assert table.snapshot_manager().get_latest_snapshot().id == before
    indexes._commit(table, messages)
    _assert_ids(table, [1, 3, 6], kind)
    assert _build(table, kind, options) == []


@pytest.mark.parametrize('kind', VINDEX_IDENTIFIERS)
def test_all_null_shards_create_no_file_or_snapshot(rest_catalog, kind):
    catalog, _ = rest_catalog
    table = _table(catalog, [None] * 9)
    before = table.snapshot_manager().get_latest_snapshot().id
    assert _build(table, kind, generic._options(kind, 4)) == []
    assert table.create_global_index('embedding', index_type=kind, options=generic._options(kind, 4)) == 0
    assert table.snapshot_manager().get_latest_snapshot().id == before
    assert indexes._files(table) == []
    root = table.path_factory().global_index_path_factory().global_index_root_path()
    assert not table.file_io.exists(root) or table.file_io.list_status(root) == []


@pytest.mark.parametrize('kind', VINDEX_IDENTIFIERS)
@pytest.mark.parametrize('native_commit', [False, True])
def test_empty_shard_does_not_shift_next_shard_or_incremental_ids(rest_catalog, kind, native_commit):
    catalog, _ = rest_catalog
    table = _table(catalog, [None] * 4 + [None, [1., 0.], None, [0., 1.]],
                   {'commit.native.enabled': str(native_commit).lower()})
    options = generic._options(kind, 4)
    messages = _build(table, kind, options)
    files = indexes._message_files(messages)
    assert len(files) == 1
    assert files[0].row_count == 4
    assert files[0].global_index_meta.row_range_start == 4
    assert files[0].global_index_meta.row_range_end == 7
    indexes._commit(table, messages)
    _assert_ids(table, [5, 7], kind)
    indexes._append(table, _rows([None, [1., 1.], None], 8), schema=SCHEMA)
    messages = _build(table, kind, options)
    files = indexes._message_files(messages)
    assert len(files) == 1
    assert files[0].global_index_meta.row_range_start == 8
    assert files[0].row_count == 3
    indexes._commit(table, messages)
    _assert_ids(table, [5, 7, 9], kind)


@pytest.mark.parametrize('kind', VINDEX_IDENTIFIERS)
def test_sparse_full_spill_bytes_match_python_writer(rest_catalog, kind):
    catalog, _ = rest_catalog
    vectors = [None if i % 5 == 0 else [float(i % 7), float(i // 7)] for i in range(97)]
    table = _table(catalog, vectors, {'read.batch-size': '7'})
    options = generic._options(kind)
    options[kind + '.train.sample-ratio'] = ' 0.37 '
    if kind != 'diskann':
        options['vindex.build.granule.enabled'] = 'false'
    native = _build(table, kind, options)
    classic = GlobalIndexBuilder(table.copy({'write.native.enabled': 'false'}), 'embedding',
                                 index_type=kind, options=options).build()
    native_files, classic_files = indexes._message_files(native), indexes._message_files(classic)
    assert len(native_files) == len(classic_files) == 1
    assert native_files[0].row_count == classic_files[0].row_count == len(vectors)
    assert _bytes(table, native_files[0]) == _bytes(table, classic_files[0])


@pytest.mark.parametrize('kind', VINDEX_IDENTIFIERS)
@pytest.mark.parametrize('budget', [None, 4096])
def test_default_training_config_matches_python_without_fixed_nlist_or_pq_m(rest_catalog, kind, budget):
    catalog, _ = rest_catalog
    table = _table(catalog, [None, [1., 0.], None, [0., 1.], [1., 1.]])
    options = {kind + '.dimension': '2'}
    if budget is not None:
        options[kind + '.max-bytes-per-vector'] = str(budget)
    native = indexes._message_files(_build(table, kind, options))
    classic_builder = GlobalIndexBuilder(table.copy({'write.native.enabled': 'false'}),
                                         'embedding', index_type=kind, options=options)
    classic = indexes._message_files(classic_builder.build())
    assert _bytes(table, native[0]) == _bytes(table, classic[0])
    assert _metadata(table, native[0]).total_vectors == 3


@pytest.mark.parametrize('kind', ['ivf-flat', 'ivf-pq'])
def test_field_aliases_and_build_options_override_table_options(rest_catalog, kind):
    catalog, _ = rest_catalog
    table = _table(catalog, [None, [1., 0.], [0., 1.]],
                   {kind + '.dimension': '4', 'fields.embedding.distance.metric': 'inner_product'})
    options = {kind + '.index.dimension': '2', 'fields.embedding.metric': 'l2',
               kind + '.expected-vector-count': '32', 'ivf.coarse-assignment': 'exact',
               'global-index.build.parallelism': ' 2 ', 'metric': 'invalid', 'dimension': '999',
               kind + '.ignored-option': 'ignored',
               'fields.embedding.ivf.train.max-points-per-centroid': '16'}
    if kind == 'ivf-pq':
        options.update({'fields.embedding.pq.code-ratio': '0.25', kind + '.ivf.pq-encoding': 'canonical',
                        kind + '.use-opq': 'true'})
    native = indexes._message_files(_build(table, kind, options))
    classic_builder = GlobalIndexBuilder(table.copy({'write.native.enabled': 'false'}),
                                         'embedding', index_type=kind, options=options)
    classic = indexes._message_files(classic_builder.build())
    assert _bytes(table, native[0]) == _bytes(table, classic[0])
    meta = _metadata(table, native[0])
    assert meta.dimension == 2
    assert meta.metric == 'l2'


@pytest.mark.parametrize('vector', [[1.], [1., None]])
def test_invalid_non_null_vectors_fail_without_python_retry(rest_catalog, vector):
    catalog, _ = rest_catalog
    table = _table(catalog, [[1., 0.], None, vector])
    before = table.snapshot_manager().get_latest_snapshot().id
    with pytest.raises(Exception, match='dimension|null'):
        _build(table, 'ivf-flat', generic._options('ivf-flat', 1))
    assert table.snapshot_manager().get_latest_snapshot().id == before
    assert indexes._files(table) == []
    root = table.path_factory().global_index_path_factory().global_index_root_path()
    assert not table.file_io.exists(root) or table.file_io.list_status(root) == []


@pytest.mark.parametrize('value', ['0', '-1', 'invalid'])
def test_invalid_build_parallelism_is_rejected_before_writing_files(rest_catalog, value):
    catalog, _ = rest_catalog
    table = _table(catalog, [[1., 0.]])
    options = generic._options('ivf-flat')
    options['global-index.build.parallelism'] = value
    before = table.snapshot_manager().get_latest_snapshot().id
    with pytest.raises(Exception, match='parallelism'):
        _build(table, 'ivf-flat', options)
    assert table.snapshot_manager().get_latest_snapshot().id == before
    assert indexes._files(table) == []


@pytest.mark.parametrize('kind', VINDEX_IDENTIFIERS)
def test_all_null_build_does_not_instantiate_native_training(rest_catalog, kind):
    catalog, _ = rest_catalog
    table = _table(catalog, [None] * 4)
    options = {kind + '.dimension': '2', kind + '.distance.metric': 'invalid'}
    if kind != 'diskann':
        options[kind + '.nlist'] = '0'
    assert _build(table, kind, options) == []
    classic = table.copy({'write.native.enabled': 'false'})
    assert GlobalIndexBuilder(classic, 'embedding', index_type=kind, options=options).build() == []
    assert indexes._files(table) == []


def test_automatic_pq_budget_uses_real_non_null_cardinality(rest_catalog):
    catalog, _ = rest_catalog
    vectors = [None if i % 3 == 0 else [float(i % 7), float(i // 7)] for i in range(1024)]
    table = _table(catalog, vectors)
    options = {'ivf-pq.dimension': '2', 'ivf-pq.max-bytes-per-vector': '64',
               'global-index.row-count-per-shard': '2048'}
    native = indexes._message_files(_build(table, 'ivf-pq', options))
    classic_table = table.copy({'write.native.enabled': 'false'})
    classic_builder = GlobalIndexBuilder(classic_table, 'embedding', index_type='ivf-pq', options=options)
    classic = indexes._message_files(classic_builder.build())
    assert _bytes(table, native[0]) == _bytes(table, classic[0])
    assert native[0].row_count == 1024
    assert _metadata(table, native[0]).total_vectors == 682
