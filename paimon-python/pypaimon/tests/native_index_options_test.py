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
from pypaimon.execution.native.read.builder import _native_read_builder
from pypaimon.execution.native.read.plan import native_plan, native_stream_scan
from pypaimon.execution.native.table import _resolved_schema_json, _options_to_native
from pypaimon.execution.native.write.native_commit import create_native_write_table
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
@pytest.mark.parametrize('declaration', ['legacy', 'index-column', 'index-columns'])
def test_native_schemas_preserve_index_configuration_verbatim(tmp_path, primary_key, declaration):
    values = {'ivf-pq.dimension': '2', 'opaque.native.option': 'preserved',
              'btree-index.block-size': '4 kb',
              'fields.amount.index-options': '{"block-size":"8 kb"}',
              'fields.other.index-options': '{"block-size":"16 kb"}',
              'fields.embedding.index-options': '{"nlist":64,"metric":"COSINE"}',
              'fields.embedding.pk-vector.index.options': '{"nlist":32}',
              'fields.body.index-options': '{"stop-words":["paimon","lake"],"lower-case":false}'}
    if declaration == 'legacy':
        values.update({'pk-vector.index.columns': 'embedding',
                       'fields.embedding.pk-vector.index.type': 'IVF-PQ',
                       'pk-btree.index.columns': 'amount,other', 'pk-full-text.index.columns': 'body'})
    else:
        values.update({'global-index.ivf-pq.' + declaration: 'embedding',
                       'global-index.btree.' + declaration: 'amount,other',
                       'global-index.full-text.' + declaration: 'body'})
    table = _table(tmp_path, primary_key, values)
    original = dict(table.table_schema.options)
    read = json.loads(_resolved_schema_json(table))['options']
    module = ModuleType('pypaimon_rust.datafusion')
    module.Table = Mock()
    with patch.dict('sys.modules', {'pypaimon_rust.datafusion': module}):
        assert create_native_write_table(table) is module.Table.from_resolved_schema.return_value
    write = json.loads(module.Table.from_resolved_schema.call_args[0][1])['options']
    for actual in (read, write):
        for key, value in values.items():
            assert actual[key] == value
    assert table.table_schema.options == original


@pytest.mark.parametrize('primary_key', [False, True])
@pytest.mark.parametrize('key', ['index_type', 'index-type', 'vector.index-type', 'fields.embedding.index-type'])
def test_native_schema_keeps_uninterpreted_options(tmp_path, primary_key, key):
    values = {'global-index.ivf-pq.index-columns': 'embedding', key: 'diskann',
              'fields.embedding.index-options': '{"index-type":"other","nlist":64}'}
    table = _table(tmp_path, primary_key, values)
    actual = json.loads(_resolved_schema_json(table))['options']
    for name, value in values.items():
        assert actual[name] == value
    assert 'ivf-pq.nlist' not in actual
    assert 'fields.embedding.metric' not in actual


@pytest.mark.parametrize('field_options', ['{"nlist":64}', '[1,2]'])
def test_native_defers_field_option_parsing_to_rust(tmp_path, field_options):
    table = _table(tmp_path, False, {'fields.embedding.index-options': field_options})
    actual = json.loads(_resolved_schema_json(table))['options']
    assert actual['fields.embedding.index-options'] == field_options


@pytest.mark.parametrize('streaming', [False, True])
def test_distinct_scalar_field_options_reach_native_scan(tmp_path, streaming):
    table = _table(tmp_path, False, {
        'global-index.btree.index-columns': 'amount,other',
        'fields.amount.index-options': '{"fallback-scan-max-size":"1 kb"}',
        'fields.other.index-options': '{"fallback-scan-max-size":"2 kb"}',
    })
    predicate = table.new_read_builder().new_predicate_builder().equal('amount', 1)
    from pypaimon.execution.native.read import plan
    with patch.object(plan.runtime, 'native_runtime_available', return_value=True), \
            patch.object(plan.native_builder, '_native_read_builder') as bridge, \
            patch.object(plan.native_builder, '_configure_native_read_builder',
                         side_effect=lambda builder, *args, **kwargs: builder), \
            patch.object(plan, '_from_native_plan') as convert:
        if streaming:
            assert native_stream_scan(table, predicate=predicate) is bridge.return_value.new_stream_scan.return_value
        else:
            assert native_plan(table, predicate=predicate) is convert.return_value
        bridge.assert_called_once_with(table)
    options = json.loads(_resolved_schema_json(table))['options']
    assert options['fields.amount.index-options'] != options['fields.other.index-options']


@pytest.mark.parametrize('global_index', [True, False])
def test_index_field_options_do_not_disable_native_physical_reads(tmp_path, global_index):
    table = _table(tmp_path, False, {
        'global-index.enabled': str(global_index).lower(),
        'fields.embedding.index-options': '{"nlist":64}',
    })
    with patch('pypaimon.execution.native.table._native_table') as bridge:
        assert _native_read_builder(table) is bridge.return_value.new_read_builder.return_value
    bridge.assert_called_once_with(table)


def test_native_options_only_encode_values_and_leave_input_untouched():
    original = {'boolean': True, 'number': 4, 'list': ['paimon', 'lake'],
                'object': {'value': False}, 'text': ' COSINE ', 'unset': None}
    actual = _options_to_native(original)
    assert actual == {'boolean': 'true', 'number': '4', 'list': '["paimon", "lake"]',
                      'object': '{"value": false}', 'text': ' COSINE '}
    assert original['unset'] is None
    assert original['list'] == ['paimon', 'lake']
