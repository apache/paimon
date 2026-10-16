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

"""Indexed searches on row-tracked append tables share the native search path."""

from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon.read.native_plan import native_method_available
from pypaimon.table.source.full_text_search_builder import FullTextSearchBuilderImpl
from pypaimon.table.source.hybrid_search_builder import HybridSearchBuilderImpl
from pypaimon.table.source.vector_search_read import DataEvolutionVectorRead
from pypaimon.tests import native_sorted_index_build_test as indexes
from pypaimon.tests.vector_search_filter_test import match_query

rest_catalog = indexes.rest_catalog
pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_method_available('Table', 'new_hybrid_search_builder'), reason='Rust search API required')]

SCHEMA = pa.schema([
    ('id', pa.int32()), ('content', pa.string()), ('embedding', pa.list_(pa.float32())),
    ('label', pa.string()), ('pt', pa.int32()),
])
ROWS = [
    {'id': 0, 'content': 'paimon lake', 'embedding': [0., 0.], 'label': 'keep', 'pt': 0},
    {'id': 1, 'content': 'paimon', 'embedding': [1., 0.], 'label': 'drop', 'pt': 0},
    {'id': 2, 'content': 'lake', 'embedding': [2., 0.], 'label': 'keep', 'pt': 1},
    {'id': 3, 'content': 'paimon', 'embedding': [3., 0.], 'label': 'keep', 'pt': 1},
]


def _table(catalog, global_index):
    table = indexes._create(catalog, schema=SCHEMA, partitioned=True, options={
        'data-evolution.enabled': 'false', 'global-index.enabled': str(global_index).lower(),
        'write.native.enabled': 'false', 'read.native.enabled': 'false',
        'scan.native-plan.enabled': 'false', 'vector-index.search-mode': 'fast',
        'full-text-index.search-mode': 'fast', 'scalar-index.search-mode': 'fast',
    })
    indexes._append(table, ROWS, SCHEMA)
    # The existing Python builders already support row-tracked append tables.
    # Keep these builds independent from the native builder being extended.
    table.create_global_index('content', index_type='full-text')
    table.create_global_index('embedding', index_type='ivf-flat', options={
        'ivf-flat.dimension': '2', 'ivf-flat.nlist': '1', 'ivf-flat.distance.metric': 'l2'})
    table.create_global_index('label', index_type='btree')
    return table


def _builder(table, family, partition=False, row_filter=False):
    if family == 'full-text':
        builder = table.new_full_text_search_builder().with_query('content', match_query('paimon')).with_limit(4)
    elif family == 'vector':
        builder = (table.new_vector_search_builder().with_vector_column('embedding')
                   .with_query_vector([0., 0.]).with_limit(4).with_options({'ivf.nprobe': '1'}))
    else:
        builder = table.new_hybrid_search_builder().with_limit(4)
        if family in ('hybrid-vector', 'hybrid-mixed'):
            builder.add_vector_route('embedding', [0., 0.], 4, options={'ivf.nprobe': '1'})
        if family in ('hybrid-text', 'hybrid-mixed'):
            builder.add_full_text_route('content', match_query('paimon'), 4)
    predicate = table.new_read_builder().new_predicate_builder()
    if partition:
        builder.with_partition_filter(predicate.equal('pt', 1))
    if row_filter:
        builder.with_filter(predicate.equal('label', 'keep'))
    return builder


def _scores(result):
    getter = result.score_getter() if not result.is_empty() else None
    return {row_id: getter(row_id) for row_id in result.results()}


def _native_only():
    stack = ExitStack()
    for target, name in [
        (FullTextSearchBuilderImpl, 'new_full_text_scan'),
        (DataEvolutionVectorRead, 'read_plan'),
        (HybridSearchBuilderImpl, 'route_builders'),
        (HybridSearchBuilderImpl, 'rank'),
    ]:
        stack.enter_context(patch.object(target, name, side_effect=AssertionError('Python search ran')))
    return stack


@pytest.mark.parametrize('global_index', [False, True])
@pytest.mark.parametrize('partition', [False, True])
@pytest.mark.parametrize('family', ['full-text', 'vector', 'hybrid-text', 'hybrid-vector', 'hybrid-mixed'])
def test_indexed_append_searches_use_native_without_data_evolution(rest_catalog, family, partition, global_index):
    table = _table(rest_catalog[0], global_index)
    expected = _scores(_builder(table, family, partition).execute_local())
    with _native_only():
        actual = _scores(_builder(table.copy({'read.native.enabled': 'true'}), family, partition).execute_local())
    assert actual == pytest.approx(expected, abs=1e-6)
    wanted = {3} if partition else {0, 1, 3}
    if family in ('vector', 'hybrid-vector', 'hybrid-mixed'):
        wanted = {2, 3} if partition else {0, 1, 2, 3}
    assert set(actual) == wanted
    # Exercise the ordinary native planner too in the native-plan CI lane.
    reader = table.copy({'scan.native-plan.enabled': 'true'}).new_read_builder()
    rows = reader.new_read().to_arrow(reader.new_scan().plan().splits())
    assert sorted(rows.column('id').to_pylist()) == [0, 1, 2, 3]


@pytest.mark.parametrize('family', ['vector', 'hybrid-vector'])
def test_exact_scalar_index_filters_work_on_append_vector_routes(rest_catalog, family):
    table = _table(rest_catalog[0], True)
    expected = _scores(_builder(table, family, row_filter=True).execute_local())
    with _native_only():
        actual = _scores(_builder(table.copy({'read.native.enabled': 'true'}), family, row_filter=True).execute_local())
    assert actual == pytest.approx(expected, abs=1e-6)
    assert set(actual) == {0, 2, 3}


@pytest.mark.parametrize('family', ['full-text', 'hybrid-text', 'hybrid-mixed'])
@pytest.mark.parametrize('native', [False, True])
def test_append_full_text_row_filters_remain_unsupported(rest_catalog, family, native):
    table = _table(rest_catalog[0], True).copy({'read.native.enabled': str(native).lower()})
    with pytest.raises(Exception, match='[Ff]ull-text row filters require a data-evolution table'):
        _builder(table, family, row_filter=True).execute_local()
