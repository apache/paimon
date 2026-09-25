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

import sys
import types
from unittest.mock import patch

import pyarrow as pa
import pytest

import pypaimon.multimodal as pm
from pypaimon.globalindex.create_global_index import GlobalIndexBuilder
from pypaimon.globalindex.vindex.vindex_vector_global_index_reader import VINDEX_IDENTIFIERS
from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.tests.global_index_build_test import _FakeVectorIndexTrainer, _FakeVectorIndexWriter


SCHEMA = pa.schema([('id', pa.int64()), ('embedding', pa.list_(pa.float32(), 2)), ('category', pa.string())])


def append(table, ids, null_vector=False, category='yes'):
    # Older Parquet writers cannot encode NULL fixed-size lists.
    schema = SCHEMA.set(1, pa.field('embedding', pa.list_(pa.float32()))) if null_vector else SCHEMA
    table.add(pa.table({'id': ids, 'embedding': [None if null_vector and i == 3 else [float(i), 1.] for i in ids],
                        'category': [category] * len(ids)}, schema=schema))


def create_table(tmp_path, null_vector=False):
    schema = SCHEMA.set(1, pa.field('embedding', pa.list_(pa.float32()))) if null_vector else SCHEMA
    table = pm.connect(options={'warehouse': str(tmp_path)}).create_table(
        'vectors', schema=schema, options={
            'file.format': 'parquet', 'vector.file.format': 'parquet', 'read.batch-size': '2',
            'global-index.row-count-per-shard': '4', 'global-index.build.parallelism': '1'})
    append(table, list(range(7)), null_vector)
    return table


@pytest.fixture
def table(tmp_path):
    return create_table(tmp_path)


def entries(table):
    return sorted((e.index_file for e in IndexFileHandler(table.raw_table).scan(
        table.raw_table.snapshot_manager().get_latest_snapshot()) if e.index_file.index_type in VINDEX_IDENTIFIERS),
        key=lambda f: f.global_index_meta.row_range_start)


def build(table, metric='l2'):
    pytest.importorskip('paimon_vindex')
    return table.create_index('embedding', 'ivf-flat', options={
        'ivf-flat.dimension': '2', 'ivf-flat.nlist': '1', 'ivf-flat.distance.metric': metric})


def search(table, batch=False, refine=False, **kwargs):
    options = {'ivf.nprobe': '1'}
    if refine:
        options['refine_factor'] = '2'
    query = (table.search_vectors([[1., 1.], [1., 1.]], column='embedding', options=options, **kwargs) if batch
             else table.search([1., 1.], column='embedding', options=options, **kwargs))
    return query.select(['id']).limit(3).to_list()


@pytest.mark.parametrize('index_type', VINDEX_IDENTIFIERS)
@pytest.mark.parametrize('all_deleted', [False, True])
def test_build_preserves_physical_ids_after_deletion(tmp_path, index_type, all_deleted):
    table = create_table(tmp_path, null_vector=True)
    # Overlapping column files must not duplicate IDs or re-add deleted rows.
    table.update('id >= 0', {'category': 'changed'})
    table.delete('id >= 0' if all_deleted else 'id = 0 OR id = 1 OR id = 6')
    fake = types.SimpleNamespace(VectorIndexTrainer=_FakeVectorIndexTrainer, VectorIndexWriter=_FakeVectorIndexWriter)
    with patch.dict(sys.modules, {'paimon_vindex': fake}), patch.object(_FakeVectorIndexWriter, 'instances', []):
        assert table.create_index('embedding', index_type, options={index_type + '.dimension': '2'}) == 2
        writers = _FakeVectorIndexWriter.instances
        # NULL row 3 is counted in coverage but omitted from the native vectors.
        assert [w.added_ids for w in writers] == [[0, 1, 2], [0, 1, 2]]
        assert [w.added_vectors for w in writers] == [
            [[0., 1.], [1., 1.], [2., 1.]], [[4., 1.], [5., 1.], [6., 1.]]]
        assert [(f.global_index_meta.row_range_start, f.global_index_meta.row_range_end, f.row_count)
                for f in entries(table)] == [(0, 3, 4), (4, 6, 3)]
        assert table.create_index('embedding', index_type) == 0


@pytest.mark.parametrize('metric', ['l2', 'cosine', 'inner_product'])
@pytest.mark.parametrize('refine', [False, True])
def test_native_search_excludes_deletions_before_and_after_build(table, metric, refine):
    table.delete('id = 1 OR id = 6')
    assert build(table, metric) == 2
    table.delete('id = 0')
    table.create_index('category', 'bitmap')
    expected = [{'id': i} for i in ((3, 4, 5) if metric == 'inner_product' else (2, 3, 4))]
    for batch in (False, True):
        assert search(table, batch, refine, pre_filter="category = 'yes'") == (
            [expected, expected] if batch else expected)
    table.delete('id >= 0')
    assert search(table) == []
    assert search(table, batch=True) == [[], []]
    assert build(table, metric) == 0


def test_native_build_and_query_keep_snapshot_semantics(table):
    table.delete('id = 1 OR id = 6')
    assert build(table) == 2
    saved = table.raw_table.snapshot_manager().get_latest_snapshot().id
    builder = (table.raw_table.new_vector_search_builder().with_vector_column('embedding')
               .with_query_vector([0., 1.]).with_limit(1).with_option('ivf.nprobe', '1'))
    plan = builder.new_vector_search_scan().scan()
    table.delete('id = 0')
    assert list(builder.new_vector_search_read().read_plan(plan).results()) == [0]
    assert list(builder.execute_local().results()) == [2]
    assert search(table, snapshot_id=saved) == [{'id': i} for i in (0, 2, 3)]


def test_native_incremental_build_with_concurrent_delete(table):
    assert build(table) == 2
    append(table, [7, 8])
    expected = [{'id': i} for i in (6, 7, 8)]
    assert table.search([7., 1.], column='embedding').select(['id']).limit(3).to_list() == expected
    builder = GlobalIndexBuilder(table.raw_table, 'embedding', 'ivf-flat', options={
        'ivf-flat.dimension': '2', 'ivf-flat.nlist': '1', 'ivf-flat.distance.metric': 'l2'})
    messages = builder.build()
    # Deleting a newly indexed row between build and commit must remain visible.
    table.delete('id = 7')
    commit = table.raw_table.new_batch_write_builder().new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    assert [(f.global_index_meta.row_range_start, f.global_index_meta.row_range_end)
            for f in entries(table)] == [(0, 3), (4, 6), (7, 7), (8, 8)]
    expected = [{'id': i} for i in (5, 6, 8)]
    assert table.search([7., 1.], column='embedding').select(['id']).limit(3).to_list() == expected
    assert build(table) == 0


def test_native_partition_scoped_build_after_deletion(tmp_path):
    pytest.importorskip('paimon_vindex')
    table = pm.connect(options={'warehouse': str(tmp_path)}).create_table(
        'vectors', schema=SCHEMA, partitioned=['category'], options={'file.format': 'parquet'})
    append(table, [0, 1, 2], category='other')
    append(table, [3, 4, 5])
    table.delete("category = 'yes' AND id = 4")
    assert table.raw_table.create_global_index('embedding', 'ivf-flat', partitions={'category': 'yes'},
                                               options={'ivf-flat.nlist': '1', 'ivf-flat.distance.metric': 'l2'}) == 1
    assert [(f.global_index_meta.row_range_start, f.global_index_meta.row_range_end)
            for f in entries(table)] == [(3, 5)]
    table.raw_table = table.raw_table.copy({'vector-index.search-mode': 'fast'})
    assert search(table, pre_filter="category = 'yes'") == [{'id': 3}, {'id': 5}]
    assert search(table, pre_filter="category = 'other'") == []


def test_native_index_commit_rejects_replaced_row_ids(table):
    pytest.importorskip('paimon_vindex')
    messages = GlobalIndexBuilder(table.raw_table, 'embedding', 'ivf-flat', options={
        'ivf-flat.nlist': '1'}).build()
    write_builder = table.raw_table.new_batch_write_builder().overwrite({})
    writer, commit = write_builder.new_write(), write_builder.new_commit()
    try:
        writer.write_arrow(pa.table({'id': [99], 'embedding': [[99., 1.]], 'category': ['yes']}, schema=SCHEMA))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    commit = table.raw_table.new_batch_write_builder().new_commit()
    try:
        with pytest.raises(RuntimeError, match='Global index row ID existence conflict'):
            commit.commit(messages)
    finally:
        commit.close()
    assert entries(table) == []
