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

import json

import pytest

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.common.options.vector_index_options import VectorIndexOptions


def _assert_metric(options, expected):
    for key in ('ivf-pq.metric', 'ivf-pq.distance.metric',
                'fields.embedding.metric', 'fields.embedding.distance.metric'):
        assert options.to_map()[key] == expected


@pytest.mark.parametrize('declaration', [
    'pk-vector.index.columns', 'global-index.ivf-pq.index-column', 'global-index.ivf-pq.index-columns'])
def test_resolution_returns_matching_type_and_options(declaration):
    values = {declaration: 'embedding', 'fields.embedding.pk-vector.index.type': ' IVF-PQ ',
              'ivf-pq.dimension': '768'}
    suffix = 'pk-vector.index.options' if declaration.startswith('pk-') else 'index-options'
    values['fields.embedding.' + suffix] = '{"nlist":64,"metric":"COSINE"}'
    original = dict(values)
    resolved = VectorIndexOptions.resolve(Options(values), None, 'embedding')
    assert resolved.index_type == 'ivf-pq'
    assert resolved.options.to_map()['ivf-pq.nlist'] == '64'
    assert resolved.options.to_map()['ivf-pq.dimension'] == '768'
    assert resolved.metric == 'cosine'
    assert values == original
    # An algorithm supplied by persisted metadata wins over the table declaration.
    persisted = VectorIndexOptions.resolve(Options(values), 'ivf-flat', 'embedding', default_metric=False)
    assert persisted.index_type == 'ivf-flat'
    assert persisted.options.to_map()['ivf-flat.nlist'] == '64'
    assert persisted.metric == 'cosine'


@pytest.mark.parametrize('suffix', ['index-column', 'index-columns'])
def test_legacy_registration_is_only_a_compatibility_fallback(suffix):
    values = {'pk-vector.index.columns': ' old, other_old ',
              'fields.old.pk-vector.index.type': 'ivf-pq',
              'fields.other_old.pk-vector.index.type': 'diskann'}
    options = Options(values)
    assert VectorIndexOptions.columns(options) == ['old', 'other_old']
    assert VectorIndexOptions.resolve(options, None, 'old', default_metric=False).index_type == 'ivf-pq'
    assert CoreOptions(options).vector_index_columns('ivf-pq') == ['old']
    assert CoreOptions(options).vector_index_columns('diskann') == ['other_old']
    assert CoreOptions(options).vector_index_columns('ivf-flat') == []
    assert VectorIndexOptions.columns_option_key(options) == 'pk-vector.index.columns'
    values.update({'global-index.ivf-pq.' + suffix: ' embedding, other ',
                   'global-index.diskann.' + suffix: 'video'})
    assert VectorIndexOptions.columns(options) == ['video', 'embedding', 'other']
    assert VectorIndexOptions.resolve(options, None, 'embedding', default_metric=False).index_type == 'ivf-pq'
    assert VectorIndexOptions.resolve(options, None, 'video', default_metric=False).index_type == 'diskann'
    assert CoreOptions(options).vector_index_columns('ivf-pq') == ['embedding', 'other']
    assert CoreOptions(options).vector_index_columns('diskann') == ['video']
    assert VectorIndexOptions.columns_option_key(options) == (
        'global-index.diskann.%s, global-index.ivf-pq.%s' % (suffix, suffix))


@pytest.mark.parametrize('key', ['index_type', 'index-type', 'vector.index-type', 'fields.embedding.index-type'])
def test_generic_merge_does_not_infer_historical_vector_types(key):
    values = {key: 'ivf-pq', 'fields.embedding.index-options': '{"nlist":64}'}
    assert CoreOptions(Options(values)).global_index_options(None, 'embedding').to_map() == values


@pytest.mark.parametrize('key', ['index_type', 'index-type', 'vector.index-type', 'fields.embedding.index-type'])
def test_removed_type_aliases_are_ignored(key):
    options = Options({key: 'ivf-pq'})
    assert VectorIndexOptions.resolve(options, None, 'embedding', default_metric=False).index_type is None
    assert VectorIndexOptions.resolve(options, None, 'embedding').options.to_map() == options.to_map()
    options.to_map()['fields.embedding.pk-vector.index.type'] = 'diskann'
    assert VectorIndexOptions.resolve(options, None, 'embedding', default_metric=False).index_type == 'diskann'
    options.to_map()['global-index.ivf-flat.index-columns'] = 'embedding'
    assert VectorIndexOptions.resolve(options, None, 'embedding', default_metric=False).index_type == 'ivf-flat'


def test_legacy_primary_key_type_resolves_field_parameters():
    values = {'fields.embedding.pk-vector.index.type': ' IVF-PQ ', 'ivf-pq.dimension': '768',
              'fields.embedding.index-options': '{"nlist":64,"metric":"COSINE"}'}
    options = Options(values)
    assert VectorIndexOptions.resolve(options, None, 'embedding', default_metric=False).index_type == 'ivf-pq'
    resolved = VectorIndexOptions.resolve(options, None, 'embedding', default_metric=False).options
    _assert_metric(resolved, 'cosine')
    assert resolved.to_map()['ivf-pq.dimension'] == '768'
    assert resolved.to_map()['ivf-pq.nlist'] == '64'
    assert (VectorIndexOptions.resolve(resolved, None, 'embedding', default_metric=False).options.to_map()
            == resolved.to_map())
    assert options.to_map() == values


def test_ambiguous_declarations_do_not_fall_back_to_historical_hint():
    options = Options({'global-index.ivf-pq.index-columns': 'embedding',
                       'global-index.diskann.index-column': 'embedding', 'index_type': 'ivf-flat'})
    assert VectorIndexOptions.resolve(options, None, 'embedding', default_metric=False).index_type is None


@pytest.mark.parametrize('key,value,expected', [
    ('fields.embedding.pk-vector.distance.metric', 'COSINE', 'cosine'),
    ('fields.embedding.distance.metric', 'COSINE', 'cosine'),
    ('fields.embedding.metric', 'INNER-PRODUCT', 'inner_product'),
    ('ivf-pq.distance.metric', 'COSINE', 'cosine'),
    ('ivf-pq.metric', 'L2', 'l2'),
    ('distance.metric', 'INNER-PRODUCT', 'inner_product'),
    ('metric', 'COSINE', 'cosine'),
])
def test_legacy_and_dlf_metric_aliases(key, value, expected):
    values = {'global-index.ivf-pq.index-column': 'embedding', 'ivf-pq.dimension': '768',
              'fields.embedding.nlist': '48', key: value}
    resolved = VectorIndexOptions.resolve(Options(values), None, 'embedding').options
    _assert_metric(resolved, expected)
    assert resolved.to_map()['ivf-pq.dimension'] == '768'
    assert resolved.to_map()['fields.embedding.nlist'] == '48'
    assert VectorIndexOptions.resolve(resolved, 'ivf-pq', 'embedding').options.to_map() == resolved.to_map()
    assert values[key] == value


@pytest.mark.parametrize('suffix,key', [
    ('index-options', 'metric'), ('index-options', 'distance.metric'),
    ('pk-vector.index.options', 'metric'),
])
def test_field_json_aliases_remain_idempotent(suffix, key):
    values = {'pk-vector.index.columns': 'embedding',
              'fields.embedding.pk-vector.index.type': 'ivf-pq',
              'fields.embedding.pk-vector.distance.metric': 'cosine',
              'fields.embedding.' + suffix: json.dumps({key: 'COSINE', 'nlist': 64})}
    resolved = VectorIndexOptions.resolve(Options(values), None, 'embedding').options
    _assert_metric(resolved, 'cosine')
    assert resolved.to_map()['ivf-pq.nlist'] == '64'
    assert VectorIndexOptions.resolve(resolved, 'ivf-pq', 'embedding').options.to_map() == resolved.to_map()
    assert VectorIndexOptions.resolve(Options(values), None, 'embedding').options.to_map() == resolved.to_map()


def test_reader_keeps_unspecified_metric_and_writer_uses_default():
    values = Options({'fields.embedding.index-options': '{"nlist":64}'})
    resolved = VectorIndexOptions.resolve(values, 'ivf-pq', 'embedding', default_metric=False).options.to_map()
    assert resolved['ivf-pq.nlist'] == '64'
    assert 'ivf-pq.metric' not in resolved
    assert 'fields.embedding.distance.metric' not in resolved
    _assert_metric(VectorIndexOptions.resolve(values, 'ivf-pq', 'embedding').options, 'inner_product')
    assert VectorIndexOptions.resolve(values, None, 'embedding').options.to_map() == values.to_map()
