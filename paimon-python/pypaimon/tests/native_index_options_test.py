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
from types import ModuleType
from unittest.mock import Mock, patch

import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.native.index_options import native_scalar_index_read_supported
from pypaimon.execution.native.read.builder import _native_read_builder
from pypaimon.execution.native.read.plan import native_plan
from pypaimon.execution.native.read.native_search_plan import _index_search_options
from pypaimon.execution.native.table import _resolved_schema_json
from pypaimon.execution.native.write.native_commit import create_native_write_table
from pypaimon.index.pk.primary_key_index_definitions import PrimaryKeyIndexDefinitions
from pypaimon.schema.data_types import AtomicType, DataField, VectorType


def _table(tmp_path, primary_key, extra=None):
    options = {'row-tracking.enabled': 'false' if primary_key else 'true',
               'bucket': '2' if primary_key else '-1',
               'deletion-vectors.enabled': 'true' if primary_key else 'false',
               'data-evolution.enabled': 'false' if primary_key else 'true'}
    options.update(extra or {})
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    fields = [DataField(0, 'id', AtomicType('INT', False)),
              DataField(1, 'amount', AtomicType('INT')),
              DataField(2, 'other', AtomicType('INT')),
              DataField(3, 'embedding', VectorType(True, AtomicType('FLOAT'), 2)),
              DataField(4, 'body', AtomicType('STRING'))]
    catalog.create_table('default.search', Schema(
        fields=fields, primary_keys=['id'] if primary_key else [], options=options), False)
    return catalog.get_table('default.search')


@pytest.mark.parametrize('primary_key', [False, True])
@pytest.mark.parametrize('suffix', ['index-column', 'index-columns'])
def test_native_read_and_write_schemas_share_resolved_index_contract(tmp_path, primary_key, suffix):
    table = _table(tmp_path, primary_key, {
        'global-index.btree.' + suffix: 'amount',
        'fields.amount.index-options': '{"fallback-scan-max-size":"1 kb"}',
        'global-index.ivf-pq.' + suffix: 'embedding',
        'ivf-pq.dimension': '2',
        'ivf-pq.refine_factor': '3',
        'fields.embedding.index-options': '{"nlist":64,"metric":"COSINE"}',
        'global-index.full-text.' + suffix: 'body',
        'fields.body.index-options': '{"tokenizer":"raw","lower-case":false}',
    })
    original = dict(table.table_schema.options)
    read_options = json.loads(_resolved_schema_json(table))['options']
    module = ModuleType('pypaimon_rust.datafusion')
    module.Table = Mock()
    with patch.dict('sys.modules', {'pypaimon_rust.datafusion': module}):
        assert create_native_write_table(table) is module.Table.from_resolved_schema.return_value
    write_options = json.loads(module.Table.from_resolved_schema.call_args[0][1])['options']
    for options in (read_options, write_options):
        assert not any(key.startswith('global-index.') and key.endswith(('.index-column', '.index-columns'))
                       for key in options)
        if primary_key:
            assert options['pk-btree.index.columns'] == 'amount'
            assert options['pk-vector.index.columns'] == 'embedding'
            assert options['fields.embedding.pk-vector.index.type'] == 'ivf-pq'
            assert options['fields.embedding.pk-vector.distance.metric'] == 'cosine'
            assert options['ivf-pq.dimension'] == '2'
            assert options['ivf-pq.refine_factor'] == '3'
            assert options['full-text.tokenizer'] == 'raw'
            assert options['full-text.lower-case'] == 'false'
            definitions = PrimaryKeyIndexDefinitions.create(table.table_schema.copy(new_options=options)).definitions
            by_type = {definition.index_type: definition for definition in definitions}
            assert by_type['btree'].options.to_map()['btree-index.fallback-scan-max-size'] == '1 kb'
            assert by_type['ivf-pq'].options.to_map()['ivf-pq.nlist'] == '64'
            assert by_type['ivf-pq'].options.to_map()['ivf-pq.dimension'] == '2'
            assert by_type['ivf-pq'].options.to_map()['ivf-pq.metric'] == 'cosine'
            assert by_type['full-text'].options.to_map()['full-text.lower-case'] == 'false'
        else:
            assert not any(key.startswith('pk-') for key in options)
            assert 'fields.embedding.index-type' not in options
            assert options['fields.embedding.nlist'] == '64'
            assert options['fields.embedding.metric'] == 'cosine'
            assert options['btree-index.fallback-scan-max-size'] == '1 kb'
    assert table.table_schema.options == original


def test_native_primary_key_fields_keep_independent_overrides(tmp_path):
    table = _table(tmp_path, True, {
        'global-index.btree.index-columns': 'amount,other',
        'btree-index.block-size': '4 kb',
        'fields.amount.index-options': '{"block-size":"8 kb"}',
        'fields.other.index-options': '{"block-size":"16 kb"}',
    })
    options = json.loads(_resolved_schema_json(table))['options']
    assert 'btree-index.block-size' not in options
    assert json.loads(options['fields.amount.pk-btree.index.options'])['btree-index.block-size'] == '8 kb'
    assert json.loads(options['fields.other.pk-btree.index.options'])['btree-index.block-size'] == '16 kb'


@pytest.mark.parametrize('field_options', [
    {},
    {'fields.embedding.pk-vector.index.options': '{"nlist":64}'},
    {'fields.embedding.index-options': '{"nlist":64}'},
])
def test_native_legacy_primary_key_keeps_flat_vector_read_parameters(tmp_path, field_options):
    options = {'pk-vector.index.columns': 'embedding',
               'fields.embedding.pk-vector.index.type': 'ivf-pq',
               'fields.embedding.pk-vector.distance.metric': 'COSINE',
               'ivf-pq.dimension': '2', 'ivf-pq.refine_factor': '3'}
    options.update(field_options)
    table = _table(tmp_path, True, options)
    wire = json.loads(_resolved_schema_json(table))['options']
    assert wire['pk-vector.index.columns'] == 'embedding'
    assert wire['fields.embedding.pk-vector.index.type'] == 'ivf-pq'
    assert wire['fields.embedding.pk-vector.distance.metric'] == 'cosine'
    assert wire['ivf-pq.dimension'] == '2'
    assert wire['ivf-pq.refine_factor'] == '3'
    assert wire['ivf-pq.metric'] == 'cosine'
    if field_options:
        assert wire['ivf-pq.nlist'] == '64'


def test_native_data_evolution_does_not_invent_a_vector_metric(tmp_path):
    table = _table(tmp_path, False, {
        'global-index.ivf-pq.index-columns': 'embedding',
        'fields.embedding.index-options': '{"nlist":64}',
    })
    options = json.loads(_resolved_schema_json(table))['options']
    assert options['fields.embedding.nlist'] == '64'
    assert 'fields.embedding.index-type' not in options
    assert 'fields.embedding.metric' not in options
    assert 'ivf-pq.metric' not in options


@pytest.mark.parametrize('primary_key', [False, True])
@pytest.mark.parametrize('key', ['index_type', 'index-type', 'vector.index-type', 'fields.embedding.index-type'])
def test_native_schema_drops_removed_type_aliases(tmp_path, primary_key, key):
    table = _table(tmp_path, primary_key, {
        'global-index.ivf-pq.index-columns': 'embedding', key: 'diskann',
        'fields.embedding.index-options': '{"nlist":64}',
    })
    original = dict(table.table_schema.options)
    options = json.loads(_resolved_schema_json(table))['options']
    assert key not in options
    assert 'fields.embedding.index-type' not in options
    if primary_key:
        assert options['fields.embedding.pk-vector.index.type'] == 'ivf-pq'
        assert key not in json.loads(options['fields.embedding.pk-vector.index.options'])
    assert table.table_schema.options == original


@pytest.mark.parametrize('primary_key', [False, True])
@pytest.mark.parametrize('suffix', ['index-options', 'pk-vector.index.options'])
def test_field_json_cannot_restore_removed_native_type_hint(tmp_path, primary_key, suffix):
    table = _table(tmp_path, primary_key, {
        'global-index.ivf-pq.index-columns': 'embedding',
        'fields.embedding.' + suffix: '{"index-type":"diskann","nlist":64}',
    })
    options = json.loads(_resolved_schema_json(table))['options']
    assert 'fields.embedding.index-type' not in options
    if primary_key:
        assert 'fields.embedding.index-type' not in json.loads(options['fields.embedding.pk-vector.index.options'])
        assert options['fields.embedding.pk-vector.index.type'] == 'ivf-pq'


def test_native_qualifies_distinct_scalar_field_options_before_reading(tmp_path):
    table = _table(tmp_path, False, {
        'global-index.btree.index-columns': 'amount,other',
        'fields.amount.index-options': '{"fallback-scan-max-size":"1 kb"}',
        'fields.other.index-options': '{"fallback-scan-max-size":"2 kb"}',
    })
    assert not native_scalar_index_read_supported(table)
    predicate = table.new_read_builder().new_predicate_builder().equal('amount', 1)
    with patch('pypaimon.execution.native.table._native_table') as bridge, \
            patch('pypaimon.execution.native.read.plan.runtime.native_runtime_available', return_value=True):
        with pytest.raises(NotImplementedError, match='field index options'):
            native_plan(table, predicate=predicate)
        assert _index_search_options(table, 'body', 'full-text', scalar_filter=predicate) is None
    bridge.assert_not_called()


@pytest.mark.parametrize('global_index', [True, False])
def test_index_field_options_do_not_disable_native_physical_reads(tmp_path, global_index):
    table = _table(tmp_path, False, {
        'global-index.enabled': str(global_index).lower(),
        'fields.embedding.index-options': '{"nlist":64}',
    })
    if not global_index:
        assert native_scalar_index_read_supported(table)
    with patch('pypaimon.execution.native.table._native_table') as bridge:
        assert _native_read_builder(table) is bridge.return_value.new_read_builder.return_value
    bridge.assert_called_once_with(table)
