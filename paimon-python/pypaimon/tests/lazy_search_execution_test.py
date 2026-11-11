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

"""Native search factories own no eagerly constructed Python executor."""

from contextlib import ExitStack
import importlib
import json
import pickle
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.table.source.search_types import FullTextScanPlan, VectorSearchScanPlan


@pytest.fixture
def search_table(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    catalog.create_table('default.search', Schema.from_pyarrow_schema(
        pa.schema([('embedding', pa.list_(pa.float32())), ('body', pa.string())]),
        options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
                 'read.native.enabled': 'true'}), False)
    return catalog.get_table('default.search')


def _builder(table, family):
    if family == 'text':
        return table.new_full_text_search_builder().with_query('body', 'needle').with_limit(2)
    if family == 'batch':
        return table.new_batch_vector_search_builder().with_vector_column('embedding').with_limit(2).with_query_vectors(
            [[1.0, 0.0], [0.0, 1.0]])
    return table.new_vector_search_builder().with_vector_column('embedding').with_limit(2).with_query_vector([1.0, 0.0])


@pytest.mark.parametrize('family,legacy', [
    ('vector', False), ('batch', False), ('text', False), ('hybrid', False), ('vector', True),
])
@pytest.mark.parametrize('configure_metric', [True, False])
def test_native_search_routes_use_shared_index_options(search_table, tmp_path, family, configure_metric, legacy):
    from pypaimon.execution.native.read import native_vector_search, native_full_text_search, native_hybrid_search
    from pypaimon.table.source.hybrid_search_builder import HybridSearchRoute

    declaration = ({'pk-vector.index.columns': 'embedding', 'fields.embedding.pk-vector.index.type': 'ivf-pq'}
                   if legacy else {'global-index.ivf-pq.index-columns': 'embedding'})
    options = dict(declaration, **{
        'global-index.full-text.index-column': 'body',
        'ivf-pq.dimension': '2',
        'fields.embedding.index-options': json.dumps(
            {'distance.metric': 'COSINE', 'refine_factor': 3} if configure_metric else {'refine_factor': 3}),
        'fields.body.index-options': '{"tokenizer":"raw","lower-case":false}',
    })
    if legacy:
        from pypaimon.tests.native_index_options_test import _table
        table = _table(tmp_path / 'pk', True, dict(options, **{'read.native.enabled': 'true'}))
    else:
        table = search_table.copy(options)
    core = Mock()
    core.with_limit.return_value = core
    core.add_full_text_route.side_effect = HybridSearchRoute.full_text_route
    proxy = SimpleNamespace(new_vector_search_builder=lambda: core,
                            new_batch_vector_search_builder=lambda: core,
                            new_full_text_search_builder=lambda: core,
                            new_hybrid_search_builder=lambda: core)
    module = native_hybrid_search if family == 'hybrid' else (
        native_full_text_search if family == 'text' else native_vector_search)
    with ExitStack() as stack:
        stack.enter_context(patch.object(module, 'native_runtime_available', return_value=True))
        bridge = stack.enter_context(patch.object(module, '_native_table', return_value=proxy))
        if family in ('vector', 'batch'):
            stack.enter_context(patch.object(module, '_catalog_metastore', return_value='rest'))
            module._native_vector_builder(_builder(table, family), family == 'batch')
        else:
            stack.enter_context(patch.object(module, '_rest_catalog_supported', return_value=True))
            if family == 'text':
                module._native_full_text_builder(_builder(table, family))
            else:
                builder = (table.new_hybrid_search_builder().with_limit(2)
                           .add_vector_route('embedding', [1.0, 0.0], 2)
                           .add_full_text_route('body', '{}', 2))
                module._native_builder(builder)
        assert bridge.call_args[0][0] is table
        from pypaimon.execution.native.table import _resolved_schema_json
        wire = json.loads(_resolved_schema_json(table))['options']
        for key, value in options.items():
            assert wire[key] == value
        if family == 'hybrid':
            assert core.add_vector_route.call_args[0][-1] == {}
            assert core.add_full_text_route.call_args[0][-1] == {}
        elif family in ('vector', 'batch'):
            core.with_options.assert_called_once_with({})


@pytest.mark.parametrize('family', ['vector', 'batch', 'hybrid'])
@pytest.mark.parametrize('query_options', [
    {},
    {'fields.embedding.index-options': '{"metric":"COSINE","refine_factor":4}'},
    {'fields.embedding.pk-vector.index.options': '{"distance.metric":"COSINE","refine_factor":4}'},
    {'fields.embedding.metric': 'COSINE'},
    {'ivf-pq.distance.metric': 'COSINE'},
    {'metric': 'COSINE'},
    {'fields.embedding.index-options': '{"refine_factor":4}'},
])
def test_native_vector_query_options_preserve_explicit_metric_scope(search_table, family, query_options):
    from pypaimon.execution.native.read import native_vector_search, native_hybrid_search

    table = search_table.copy({'global-index.ivf-pq.index-columns': 'embedding',
                               'ivf-pq.dimension': '2', 'fields.embedding.distance.metric': 'l2',
                               'fields.embedding.refine_factor': '3'})
    core = Mock()
    core.with_limit.return_value = core
    proxy = SimpleNamespace(new_vector_search_builder=lambda: core,
                            new_batch_vector_search_builder=lambda: core,
                            new_hybrid_search_builder=lambda: core)
    module = native_hybrid_search if family == 'hybrid' else native_vector_search
    original = dict(query_options)
    with ExitStack() as stack:
        stack.enter_context(patch.object(module, 'native_runtime_available', return_value=True))
        bridge = stack.enter_context(patch.object(module, '_native_table', return_value=proxy))
        if family == 'hybrid':
            stack.enter_context(patch.object(module, '_rest_catalog_supported', return_value=True))
            builder = (table.new_hybrid_search_builder().with_limit(2)
                       .add_vector_route('embedding', [1., 0.], 2, options=query_options))
            assert module._native_builder(builder) is not None
            actual = core.add_vector_route.call_args[0][-1]
        else:
            stack.enter_context(patch.object(module, '_catalog_metastore', return_value='rest'))
            builder = _builder(table, family).with_options(query_options)
            assert module._native_vector_builder(builder, family == 'batch') is core
            actual = core.with_options.call_args[0][0]
    assert actual == original
    assert bridge.call_args[0][0] is table
    assert table.options.options.to_map()['fields.embedding.distance.metric'] == 'l2'
    assert query_options == original


@pytest.mark.parametrize('family,legacy', [
    ('vector', False), ('batch', False), ('hybrid', False), ('vector', True),
])
@pytest.mark.parametrize('configured', [True, False])
def test_native_configuration_needs_only_standard_builder_api(search_table, tmp_path, family, configured, legacy):
    from pypaimon.execution.native.read import native_vector_search, native_hybrid_search

    declaration = ({'pk-vector.index.columns': 'embedding', 'fields.embedding.pk-vector.index.type': 'ivf-flat'}
                   if legacy else {'global-index.ivf-flat.index-columns': 'embedding'})
    if legacy:
        from pypaimon.tests.native_index_options_test import _table
        table = _table(tmp_path / 'pk', True, dict(
            declaration if configured else {}, **{'read.native.enabled': 'true'}))
    else:
        table = search_table.copy(declaration if configured else {})
    core = Mock(spec=['with_limit', 'with_ranker', 'with_options', 'with_vector_column',
                      'add_vector_route', 'execute', 'route_builders'])
    core.with_limit.return_value = core
    proxy = SimpleNamespace(new_vector_search_builder=lambda: core,
                            new_batch_vector_search_builder=lambda: core,
                            new_hybrid_search_builder=lambda: core)
    module = native_hybrid_search if family == 'hybrid' else native_vector_search
    with ExitStack() as stack:
        stack.enter_context(patch.object(module, 'native_runtime_available', return_value=True))
        stack.enter_context(patch.object(module, '_native_table', return_value=proxy))
        if family == 'hybrid':
            stack.enter_context(patch.object(module, '_rest_catalog_supported', return_value=True))
            builder = table.new_hybrid_search_builder().with_limit(2).add_vector_route('embedding', [1., 0.], 2)
            result = module._native_builder(builder)
        else:
            stack.enter_context(patch.object(module, '_catalog_metastore', return_value='rest'))
            result = module._native_vector_builder(_builder(table, family), family == 'batch')
    assert result is not None
    core.execute.assert_not_called()
    core.route_builders.assert_not_called()
    core.with_limit.assert_called_once_with(2)


@pytest.mark.parametrize('family,entry', [
    ('vector', 'execute'), ('vector', 'scan'), ('vector', 'read'),
    ('batch', 'execute'), ('batch', 'scan'), ('batch', 'read'),
    ('hybrid', 'execute'), ('hybrid', 'routes'),
])
def test_native_option_errors_never_select_python(search_table, family, entry):
    from pypaimon.execution.native.read import native_vector_search, native_hybrid_search

    table = search_table.copy({'global-index.ivf-flat.index-columns': 'embedding'})
    core = Mock()
    core.with_limit.return_value = core
    core.with_options.side_effect = ValueError('invalid native options')
    core.add_vector_route.side_effect = ValueError('invalid native options')
    proxy = SimpleNamespace(new_vector_search_builder=lambda: core,
                            new_batch_vector_search_builder=lambda: core,
                            new_hybrid_search_builder=lambda: core)
    module = native_hybrid_search if family == 'hybrid' else native_vector_search
    with ExitStack() as stack:
        stack.enter_context(_forbid_python_execution())
        stack.enter_context(patch.object(module, 'native_runtime_available', return_value=True))
        stack.enter_context(patch.object(module, '_native_table', return_value=proxy))
        if family == 'hybrid':
            stack.enter_context(patch.object(module, '_rest_catalog_supported', return_value=True))
            builder = table.new_hybrid_search_builder().with_limit(2).add_vector_route('embedding', [1., 0.], 2)
            invoke = builder.execute_local if entry == 'execute' else builder.route_builders
        else:
            stack.enter_context(patch.object(module, '_catalog_metastore', return_value='rest'))
            builder = _builder(table, family)
            if entry == 'execute':
                invoke = builder.execute_batch_local if family == 'batch' else builder.execute_local
            else:
                invoke = _factories(builder, family)[0 if entry == 'scan' else 1]
        with pytest.raises(ValueError, match='invalid native options'):
            invoke()
    core.execute.assert_not_called()
    core.route_builders.assert_not_called()


@pytest.mark.parametrize('family', ['vector', 'batch', 'hybrid'])
@pytest.mark.parametrize('key', ['index_type', 'index-type', 'vector.index-type', 'fields.embedding.index-type'])
def test_native_search_keeps_uninterpreted_options(search_table, family, key):
    from pypaimon.execution.native.read import native_vector_search, native_hybrid_search

    table = search_table.copy({key: 'diskann'})
    core = Mock()
    core.with_limit.return_value = core
    proxy = SimpleNamespace(new_vector_search_builder=lambda: core,
                            new_batch_vector_search_builder=lambda: core,
                            new_hybrid_search_builder=lambda: core)
    module = native_hybrid_search if family == 'hybrid' else native_vector_search
    with ExitStack() as stack:
        stack.enter_context(patch.object(module, 'native_runtime_available', return_value=True))
        stack.enter_context(patch.object(module, '_native_table', return_value=proxy))
        if family == 'hybrid':
            stack.enter_context(patch.object(module, '_rest_catalog_supported', return_value=True))
            builder = (table.new_hybrid_search_builder().with_limit(2)
                       .add_vector_route('embedding', [1., 0.], 2, options={key: 'ivf-pq'}))
            assert module._native_builder(builder) is not None
            assert core.add_vector_route.call_args[0][-1][key] == 'ivf-pq'
        else:
            stack.enter_context(patch.object(module, '_catalog_metastore', return_value='rest'))
            builder = _builder(table, family).with_option(key, 'ivf-pq')
            assert module._native_vector_builder(builder, family == 'batch') is core
            assert core.with_options.call_args[0][0][key] == 'ivf-pq'


@pytest.mark.parametrize('family', ['vector', 'batch', 'hybrid'])
def test_native_table_configuration_errors_propagate(search_table, family):
    from pypaimon.execution.native.read import native_vector_search, native_hybrid_search

    table = search_table.copy({'global-index.ivf-flat.index-columns': 'embedding'})
    core = Mock()
    module = native_hybrid_search if family == 'hybrid' else native_vector_search
    with ExitStack() as stack:
        stack.enter_context(patch.object(module, 'native_runtime_available', return_value=True))
        stack.enter_context(patch.object(
            module, '_native_table', side_effect=ValueError('unsupported native index configuration')))
        stack.enter_context(pytest.raises(ValueError, match='unsupported native index configuration'))
        if family == 'hybrid':
            stack.enter_context(patch.object(module, '_rest_catalog_supported', return_value=True))
            builder = table.new_hybrid_search_builder().with_limit(2).add_vector_route('embedding', [1., 0.], 2)
            module._native_builder(builder)
        else:
            stack.enter_context(patch.object(module, '_catalog_metastore', return_value='rest'))
            module._native_vector_builder(_builder(table, family), family == 'batch')
    core.execute.assert_not_called()
    core.route_builders.assert_not_called()


def test_native_hybrid_vector_columns_keep_independent_types(tmp_path):
    from pypaimon.execution.native.read import native_hybrid_search

    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    catalog.create_table('default.search', Schema.from_pyarrow_schema(
        pa.schema([('embedding', pa.list_(pa.float32())), ('other', pa.list_(pa.float32()))]),
        options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
                 'read.native.enabled': 'true', 'global-index.ivf-pq.index-columns': 'embedding',
                 'global-index.diskann.index-column': 'other'}), False)
    table = catalog.get_table('default.search')
    builder = (table.new_hybrid_search_builder().with_limit(2)
               .add_vector_route('other', [0., 1.], 2).add_vector_route('embedding', [1., 0.], 2))
    core = Mock()
    core.with_limit.return_value = core
    proxy = SimpleNamespace(new_hybrid_search_builder=lambda: core)
    with patch.object(native_hybrid_search, 'native_runtime_available', return_value=True), \
            patch.object(native_hybrid_search, '_rest_catalog_supported', return_value=True), \
            patch.object(native_hybrid_search, '_native_table', return_value=proxy) as bridge:
        assert native_hybrid_search._native_builder(builder) is not None
    assert bridge.call_args[0][0] is table
    assert table.table_schema.options['global-index.ivf-pq.index-columns'] == 'embedding'
    assert table.table_schema.options['global-index.diskann.index-column'] == 'other'
    assert [call[0][0] for call in core.add_vector_route.call_args_list] == ['other', 'embedding']


@pytest.mark.parametrize('same_analyzer', [True, False])
def test_native_hybrid_full_text_routes_preserve_each_field_analyzer(tmp_path, same_analyzer):
    from pypaimon.execution.native.read import native_hybrid_search
    from pypaimon.table.source.hybrid_search_builder import HybridSearchRoute

    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    catalog.create_table('default.search', Schema.from_pyarrow_schema(
        pa.schema([('body', pa.string()), ('title', pa.string())]),
        options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
                 'read.native.enabled': 'true',
                 'fields.body.index-options': '{"tokenizer":"raw"}',
                 'fields.title.index-options': '{"tokenizer":"%s"}' % ('raw' if same_analyzer else 'jieba')}),
        False)
    table = catalog.get_table('default.search')
    builder = (table.new_hybrid_search_builder().with_limit(2)
               .add_full_text_route('body', '{}', 2).add_full_text_route('title', '{}', 2))
    core = Mock()
    core.with_limit.return_value = core
    core.add_full_text_route.side_effect = HybridSearchRoute.full_text_route
    proxy = SimpleNamespace(new_hybrid_search_builder=lambda: core)
    with patch.object(native_hybrid_search, 'native_runtime_available', return_value=True), \
            patch.object(native_hybrid_search, '_rest_catalog_supported', return_value=True), \
            patch.object(native_hybrid_search, '_native_table', return_value=proxy) as bridge:
        configured = native_hybrid_search._native_builder(builder)
    assert configured is not None
    assert bridge.call_args[0][0] is table
    assert table.table_schema.options['fields.body.index-options'] == '{"tokenizer":"raw"}'
    assert table.table_schema.options['fields.title.index-options'] == (
        '{"tokenizer":"%s"}' % ('raw' if same_analyzer else 'jieba'))
    assert core.add_full_text_route.call_count == 2
    assert all(call.args[-1] == {} for call in core.add_full_text_route.call_args_list)


def test_native_vector_passes_field_options_without_knowing_type(search_table):
    from pypaimon.execution.native.read import native_vector_search

    table = search_table.copy({'fields.embedding.index-options': '{"nlist":64}'})
    core = Mock(spec=['with_limit', 'with_options', 'with_vector_column'])
    core.with_limit.return_value = core
    proxy = SimpleNamespace(new_vector_search_builder=lambda: core)
    with patch.object(native_vector_search, 'native_runtime_available', return_value=True), \
            patch.object(native_vector_search, '_catalog_metastore', return_value='rest'), \
            patch.object(native_vector_search, '_native_table', return_value=proxy) as bridge:
        assert native_vector_search._native_vector_builder(_builder(table, 'vector')) is core
    bridge.assert_called_once_with(table)
    assert table.table_schema.options['fields.embedding.index-options'] == '{"nlist":64}'


def _factories(builder, family):
    if family == 'text':
        return builder.new_full_text_scan, builder.new_full_text_read
    if family == 'batch':
        return builder.new_vector_search_scan, builder.new_batch_vector_search_read
    return builder.new_vector_search_scan, builder.new_vector_search_read


def _native_builder(family):
    plan = SimpleNamespace(snapshot=lambda: None, splits=lambda: [])
    result = SimpleNamespace(row_ids=lambda: {7: 0.5})
    reader = SimpleNamespace(read_plan=Mock(return_value=result), read_batch_plan=Mock(return_value=[result, result]))
    scan = SimpleNamespace(scan=Mock(return_value=plan))
    builder = Mock()
    builder.new_full_text_scan.return_value = scan
    builder.new_vector_search_scan.return_value = scan
    builder.new_full_text_read.return_value = reader
    builder.new_vector_search_read.return_value = reader
    builder.new_batch_vector_search_read.return_value = reader
    return builder, scan, reader


def _native_factory(family):
    return ('pypaimon.execution.native.read.native_full_text_search._native_full_text_builder'
            if family == 'text' else 'pypaimon.execution.native.read.native_vector_search._native_vector_builder')


def _forbid_python_execution():
    stack = ExitStack()
    for module, classes in (
            ('vector_search_scan', ['DataEvolutionVectorScan']),
            ('vector_search_read', ['DataEvolutionVectorRead', 'BatchVectorSearchReadImpl']),
            ('full_text_scan', ['DataEvolutionFullTextScan']),
            ('full_text_read', ['DataEvolutionFullTextRead'])):
        for cls in classes:
            stack.enter_context(patch(
                'pypaimon.execution.python.read.search.' + module + '.' + cls + '.__init__',
                side_effect=AssertionError('Native search initialized Python execution')))
    return stack


def _ordinary_plan(family):
    return (FullTextScanPlan if family == 'text' else VectorSearchScanPlan)([], None)


@pytest.mark.parametrize('family', ['vector', 'batch', 'text'])
def test_native_search_prepares_plain_plans_and_transports_readers_without_python(search_table, family):
    builder = _builder(search_table, family)
    native, core_scan, core_read = _native_builder(family)
    scan_factory, read_factory = _factories(builder, family)
    native_plan = object()
    with _forbid_python_execution(), patch(_native_factory(family), return_value=native), patch(
            'pypaimon.execution.native.read.native_search_plan._plan_to_native', return_value=native_plan) as convert:
        scan, reader = scan_factory(), read_factory()
        plan = scan.scan()
        assert type(plan) is (FullTextScanPlan if family == 'text' else VectorSearchScanPlan)
        assert not hasattr(plan, '_native_plan')
        results = reader.read_batch_plan(plan) if family == 'batch' else [reader.read_plan(plan)]
        assert [list(result.results()) for result in results] == [[7]] * (2 if family == 'batch' else 1)
        # The transported reader reconstructs only the selected Native backend.
        transported = pickle.loads(pickle.dumps(reader))
        assert transported._native_reader._native is None
        copied_plan = pickle.loads(pickle.dumps(plan))
        copied_results = (transported.read_batch_plan(copied_plan) if family == 'batch'
                          else [transported.read_plan(copied_plan)])
        assert [list(result.results()) for result in copied_results] == [[7]] * (2 if family == 'batch' else 1)
        assert transported._python_executor is None
        copied_scan = pickle.loads(pickle.dumps(scan))
        assert copied_scan._native_scan.available()
        assert copied_scan.scan().splits() == []
        assert copied_scan._python_executor is None
        assert convert.call_count == 2
        assert convert.call_args[0][0].splits() == []
    assert core_scan.scan.call_args_list == [((),), ((),)]
    method = core_read.read_batch_plan if family == 'batch' else core_read.read_plan
    assert method.call_args_list == [((native_plan,),), ((native_plan,),)]


@pytest.mark.parametrize('family', ['vector', 'batch', 'text'])
def test_native_search_reader_introspection_does_not_select_python_execution(search_table, family):
    builder = _builder(search_table, family)
    native, _, _ = _native_builder(family)
    _, read_factory = _factories(builder, family)
    with _forbid_python_execution(), patch(_native_factory(family), return_value=native):
        reader = read_factory()
        # Cleanup helpers and interactive frontends probe optional attributes;
        # observing a reader must not select another execution backend.
        for name in ('close', '_repr_html_', '_ipython_display_', 'missing_attribute'):
            assert not hasattr(reader, name)
        method = 'read_batch' if family == 'batch' else 'read'
        assert callable(getattr(reader, method))
        assert not hasattr(reader, 'read' if family == 'batch' else 'read_batch')
        assert reader._python_executor is None


@pytest.mark.parametrize('family', ['vector', 'batch', 'text'])
def test_native_search_split_read_delegates_to_native_without_python(search_table, family):
    builder = _builder(search_table, family)
    native, _, core_read = _native_builder(family)
    _, read_factory = _factories(builder, family)
    native_plan = object()
    method_name = 'read_batch' if family == 'batch' else 'read'
    with _forbid_python_execution(), patch(_native_factory(family), return_value=native), patch(
            'pypaimon.execution.native.read.native_search_plan._plan_to_native',
            return_value=native_plan) as convert, patch(
            'pypaimon.snapshot.time_travel_util.TimeTravelUtil.resolve_snapshot',
            return_value=None) as snapshot:
        reader = read_factory()
        split_read = getattr(reader, method_name)
        splits = []
        for _ in range(2):
            results = split_read(splits) if family == 'batch' else [split_read(splits)]
            assert [list(result.results()) for result in results] == [[7]] * (2 if family == 'batch' else 1)
        assert reader._python_executor is None
        assert snapshot.call_count == 2
        assert convert.call_count == 2
        assert all(call[0][0].splits() is splits and call[0][0].snapshot() is None for call in convert.call_args_list)
    method = core_read.read_batch_plan if family == 'batch' else core_read.read_plan
    assert method.call_args_list == [((native_plan,),), ((native_plan,),)]


@pytest.mark.parametrize('phase', ['scan', 'read'])
def test_native_search_failure_never_creates_python_execution(search_table, phase):
    builder = _builder(search_table, 'vector')
    native, core_scan, core_read = _native_builder('vector')
    failure = OSError('search may already have read remote state')
    with _forbid_python_execution(), patch(_native_factory('vector'), return_value=native), patch(
            'pypaimon.execution.native.read.native_search_plan._plan_to_native', return_value=object()):
        scan, reader = builder.new_vector_search_scan(), builder.new_vector_search_read()
        if phase == 'scan':
            core_scan.scan.side_effect = failure
            operation = scan.scan
        else:
            plan = scan.scan()
            core_read.read_plan.side_effect = failure
            operation = lambda: reader.read_plan(plan)
        with pytest.raises(OSError) as caught:
            operation()
        assert caught.value is failure


@pytest.mark.parametrize('family', ['vector', 'batch', 'text'])
def test_ordinary_python_plans_use_native_without_fallback(search_table, family):
    builder = _builder(search_table, family)
    native, _, core_read = _native_builder(family)
    _, read_factory = _factories(builder, family)
    native_plan = object()
    with _forbid_python_execution(), patch(_native_factory(family), return_value=native), patch(
            'pypaimon.execution.native.read.native_search_plan._plan_to_native', return_value=native_plan) as convert:
        reader = read_factory()
        plan = _ordinary_plan(family)
        results = reader.read_batch_plan(plan) if family == 'batch' else [reader.read_plan(plan)]
        assert [list(result.results()) for result in results] == [[7]] * (2 if family == 'batch' else 1)
        assert reader._python_executor is None
        assert convert.call_args[0][0] is plan
    method = core_read.read_batch_plan if family == 'batch' else core_read.read_plan
    method.assert_called_once_with(native_plan)


def test_native_reader_keeps_query_options_captured_before_transport(search_table):
    builder = _builder(search_table, 'vector').with_option('ivf-flat.metric', 'l2')
    native, _, _ = _native_builder('vector')
    with patch(_native_factory('vector'), return_value=native):
        reader = builder.new_vector_search_read()
    builder.with_option('ivf-flat.metric', 'cosine')
    transported = pickle.loads(pickle.dumps(reader))
    with _forbid_python_execution(), patch(_native_factory('vector'), return_value=native) as prepare, patch(
            'pypaimon.execution.native.read.native_search_plan._plan_to_native', return_value=object()):
        transported.read_plan(_ordinary_plan('vector'))
    assert prepare.call_args[0][0]._options == {'ivf-flat.metric': 'l2'}
    assert transported._python_executor is None


def test_batch_reader_freezes_query_sequence_before_transport(search_table):
    queries = [[1.0, 0.0]]
    builder = _builder(search_table, 'batch').with_query_vectors(queries)
    native, _, _ = _native_builder('batch')
    with patch(_native_factory('batch'), return_value=native):
        reader = builder.new_batch_vector_search_read()
    queries.append([0.0, 1.0])
    queries[0][:] = [3.0, 4.0]
    assert native.with_query_vectors.call_args[0][0] == [[1.0, 0.0]]
    transported = pickle.loads(pickle.dumps(reader))
    with _forbid_python_execution(), patch(_native_factory('batch'), return_value=native), patch(
            'pypaimon.execution.native.read.native_search_plan._plan_to_native', return_value=object()):
        transported.read_batch_plan(_ordinary_plan('batch'))
    assert native.with_query_vectors.call_args[0][0] == [[1.0, 0.0]]
    assert transported._python_executor is None


def test_full_text_reader_keeps_query_and_limit_captured_before_transport(search_table):
    builder = _builder(search_table, 'text')
    native, _, _ = _native_builder('text')
    with patch(_native_factory('text'), return_value=native):
        reader = builder.new_full_text_read()
    builder.with_query('body', 'changed').with_limit(99)
    transported = pickle.loads(pickle.dumps(reader))
    with _forbid_python_execution(), patch(_native_factory('text'), return_value=native) as prepare, patch(
            'pypaimon.execution.native.read.native_search_plan._plan_to_native', return_value=object()):
        transported.read_plan(_ordinary_plan('text'))
    captured = prepare.call_args[0][0]
    assert (captured._field_name, captured._query, captured._limit) == ('body', 'needle', 2)
    assert transported._python_executor is None


@pytest.mark.parametrize('name', [
    'VectorSearchScanPlan', 'VectorSearchScan', 'VectorSearchRead', 'BatchVectorSearchRead',
    'FullTextScanPlan', 'FullTextScan', 'FullTextRead', 'PrimaryKeyVectorSearchSplit',
    'PrimaryKeyVectorScanPlan', 'PrimaryKeyFullTextSearchSplit', 'PrimaryKeyFullTextScanPlan',
])
def test_shared_search_contracts_serialize_from_shared_module(name):
    canonical = getattr(importlib.import_module('pypaimon.table.source.search_types'), name)
    assert pickle.loads(pickle.dumps(canonical)) is canonical


def test_vector_split_uses_its_shared_contract_module():
    from pypaimon.table.source.vector_search_split import VectorSearchSplit

    assert pickle.loads(pickle.dumps(VectorSearchSplit)) is VectorSearchSplit


def test_single_reader_freezes_query_values_before_transport(search_table):
    query = [1.0, 0.0]
    builder = _builder(search_table, 'vector').with_query_vector(query)
    native, _, _ = _native_builder('vector')
    with patch(_native_factory('vector'), return_value=native):
        reader = builder.new_vector_search_read()
    query[:] = [3.0, 4.0]
    assert native.with_query_vector.call_args[0][0] == [1.0, 0.0]
    transported = pickle.loads(pickle.dumps(reader))
    with _forbid_python_execution(), patch(_native_factory('vector'), return_value=native), patch(
            'pypaimon.execution.native.read.native_search_plan._plan_to_native', return_value=object()):
        transported.read_plan(_ordinary_plan('vector'))
    assert native.with_query_vector.call_args[0][0] == [1.0, 0.0]
    assert transported._python_executor is None


def _typed_native_sdk():
    """Strict SDK shapes exercise object transport without loading an extension."""
    class NativeSnapshot:
        @classmethod
        def _from_json(cls, value):
            assert isinstance(value, str)
            result = cls()
            result.value = json.loads(value)
            return result

        def _to_json(self):
            return json.dumps(self.value)

        def id(self):
            return self.value['id']

    class NativeIndexFileMeta:
        @classmethod
        def _deserialize(cls, value):
            assert isinstance(value, bytes)
            result = cls()
            result.value = value
            return result

        def _serialize(self):
            return self.value

    class NativeSplit:
        @classmethod
        def deserialize(cls, value):
            assert isinstance(value, bytes)
            result = cls()
            result.value = value
            return result

        def serialize(self):
            return self.value

    class IndexSplit:
        def __init__(self, start, end, files, scalar=()):
            assert all(isinstance(file, NativeIndexFileMeta) for file in list(files) + list(scalar))
            self.values = start, end, list(files), list(scalar)

        def row_range_start(self):
            return self.values[0]

        def row_range_end(self):
            return self.values[1]

        def scalar_index_files(self):
            return self.values[3]

    class IndexVectorSplit(IndexSplit):
        def vector_index_files(self):
            return self.values[2]

    class IndexTextSplit(IndexSplit):
        def __init__(self, column, start, end, files, scalar=()):
            super().__init__(start, end, files, scalar)
            self.column = column

        def column_name(self):
            return self.column

        def full_text_index_files(self):
            return self.values[2]

    class RawVectorSplit:
        def __init__(self, ranges, scalar=(), index_type=None):
            assert all(isinstance(value, tuple) and len(value) == 2 for value in ranges)
            self.values = list(ranges), list(scalar), index_type

        def row_ranges(self):
            return self.values[0]

        def scalar_index_files(self):
            return self.values[1]

        def index_type(self):
            return self.values[2]

    class RawTextSplit:
        def __init__(self, ranges):
            assert all(isinstance(value, tuple) and len(value) == 2 for value in ranges)
            self.ranges = list(ranges)

        def row_ranges(self):
            return self.ranges

    class PrimaryKeySplit:
        def __init__(self, split, payloads, uncovered, ranges):
            assert isinstance(split, NativeSplit)
            assert all(isinstance(file, NativeIndexFileMeta) for file in payloads)
            self.values = split, list(payloads), list(uncovered), dict(ranges)

        def data_split(self):
            return self.values[0]

        def payloads(self):
            return self.values[1]

        def uncovered_data_files(self):
            return self.values[2]

        def row_ranges_by_file(self):
            return self.values[3]

    class PrimaryKeyVectorSplit(PrimaryKeySplit):
        pass

    class PrimaryKeyTextSplit(PrimaryKeySplit):
        pass

    class Plan:
        def __init__(self, splits, snapshot=None):
            assert isinstance(splits, (list, tuple))
            assert snapshot is None or isinstance(snapshot, NativeSnapshot)
            assert all(type(split) in self.split_types for split in splits)
            self.sources, self.captured = list(splits), snapshot

        def splits(self):
            return self.sources

        def snapshot(self):
            return self.captured

    class VectorPlan(Plan):
        split_types = IndexVectorSplit, RawVectorSplit, PrimaryKeyVectorSplit

    class TextPlan(Plan):
        split_types = IndexTextSplit, RawTextSplit, PrimaryKeyTextSplit

    sdk, datafusion = ModuleType('pypaimon_rust'), ModuleType('pypaimon_rust.datafusion')
    for name, cls in [('Snapshot', NativeSnapshot), ('IndexFileMeta', NativeIndexFileMeta), ('Split', NativeSplit),
                      ('VectorScanPlan', VectorPlan), ('FullTextScanPlan', TextPlan),
                      ('IndexVectorSearchSplit', IndexVectorSplit), ('RawVectorSearchSplit', RawVectorSplit),
                      ('PrimaryKeyVectorSearchSplit', PrimaryKeyVectorSplit),
                      ('IndexFullTextSearchSplit', IndexTextSplit), ('RawFullTextSearchSplit', RawTextSplit),
                      ('PrimaryKeyFullTextSearchSplit', PrimaryKeyTextSplit)]:
        setattr(datafusion, name, cls)
    sdk.datafusion = datafusion
    return sdk, datafusion


@pytest.mark.parametrize('family', ['vector', 'batch', 'text'])
def test_python_source_metadata_round_trips_through_public_native_plan_constructor(search_table, family):
    from pypaimon.execution.native.read.native_search_plan import _plan_from_native, _plan_to_native
    from pypaimon.index.index_file_meta import IndexFileMeta
    from pypaimon.snapshot.snapshot import Snapshot
    from pypaimon.table.source.full_text_search_split import IndexFullTextSearchSplit, RawFullTextSearchSplit
    from pypaimon.table.source.vector_search_split import IndexVectorSearchSplit, RawVectorSearchSplit
    from pypaimon.utils.range import Range

    sdk, datafusion = _typed_native_sdk()
    full_text = family == 'text'
    snapshot = Snapshot(3, 7, 2, 'base', 'delta', 10, 10, 'writer', 1, 'APPEND', 0, next_row_id=10)
    index = IndexFileMeta('full-text' if full_text else 'ivf-flat', 'index-1', 1024, 4,
                          external_path='s3://bucket/index-1')
    scalar = IndexFileMeta('btree', 'index-scalar', 128, 4)
    splits = ([IndexFullTextSearchSplit('body', 0, 3, [index], [scalar]), RawFullTextSearchSplit([Range(4, 9)])]
              if full_text else [IndexVectorSearchSplit(0, 3, [index], [scalar]),
                                 RawVectorSearchSplit([Range(4, 9)], [scalar], 'ivf-flat')])
    source = (FullTextScanPlan if full_text else VectorSearchScanPlan)(splits, snapshot)
    with patch.dict('sys.modules', {'pypaimon_rust': sdk, 'pypaimon_rust.datafusion': datafusion}):
        native = _plan_to_native(source, full_text)
        restored = _plan_from_native(search_table, native, full_text, False)
        copied = _plan_to_native(pickle.loads(pickle.dumps(restored)), full_text)
        empty = _plan_to_native(_ordinary_plan(family), full_text)
    assert json.loads(native.snapshot()._to_json())['schemaId'] == 2
    assert isinstance(native.snapshot(), datafusion.Snapshot)
    assert isinstance(native.splits()[0], datafusion.IndexFullTextSearchSplit if full_text else
                      datafusion.IndexVectorSearchSplit)
    assert copied.snapshot()._to_json() == native.snapshot()._to_json()
    assert copied.splits()[0].__class__ is native.splits()[0].__class__
    assert restored.snapshot() == snapshot
    restored_index = restored.splits()[0]
    restored_files = restored_index.full_text_index_files if full_text else restored_index.vector_index_files
    assert restored_files[0].external_path == index.external_path
    assert restored_index.scalar_index_files == [scalar]
    assert restored.splits() == source.splits()
    assert empty.snapshot() is None and empty.splits() == []
    assert not hasattr(native, 'metadata') and not hasattr(native, 'snapshot_id')
    assert not hasattr(restored, '_native_plan')


@pytest.mark.parametrize('full_text', [False, True])
def test_primary_key_plan_with_only_snapshot_id_resolves_that_exact_snapshot(full_text):
    from pypaimon.execution.native.read.native_search_plan import NativeSearchRead
    from pypaimon.table.source.search_types import PrimaryKeyFullTextScanPlan, PrimaryKeyVectorScanPlan

    snapshot = SimpleNamespace(id=7)
    manager = SimpleNamespace(
        get_snapshot_by_id=Mock(return_value=snapshot),
        get_latest_snapshot=Mock(side_effect=AssertionError('Resolved latest instead of the requested snapshot')))
    table = SimpleNamespace(snapshot_manager=Mock(return_value=manager))
    core_result = SimpleNamespace(snapshot_id=lambda: 7, positions=lambda: [], splits=lambda: [])
    core = SimpleNamespace(read_plan=Mock(return_value=core_result))
    reader = NativeSearchRead(table, core, True, full_text=full_text)
    sources = [object()]
    requested = (PrimaryKeyFullTextScanPlan if full_text else PrimaryKeyVectorScanPlan)(7, sources)
    native_plan = object()
    with patch('pypaimon.execution.native.read.native_search_plan._plan_to_native',
               return_value=native_plan) as convert, patch(
            'pypaimon.snapshot.time_travel_util.TimeTravelUtil.resolve_snapshot',
            side_effect=AssertionError('Resolved table time travel instead of the explicit plan snapshot')):
        result = reader.read_plan(requested)
    manager.get_snapshot_by_id.assert_called_once_with(7)
    manager.get_latest_snapshot.assert_not_called()
    converted = convert.call_args[0][0]
    assert converted.snapshot() is snapshot
    assert converted.splits() is sources
    assert requested.snapshot() is None
    assert result.snapshot_id == 7
    core.read_plan.assert_called_once_with(native_plan)


@pytest.mark.parametrize('full_text', [False, True])
def test_missing_primary_key_plan_snapshot_does_not_fall_back_to_latest(full_text):
    from pypaimon.execution.native.read.native_search_plan import NativeSearchRead
    from pypaimon.table.source.search_types import PrimaryKeyFullTextScanPlan, PrimaryKeyVectorScanPlan

    manager = SimpleNamespace(
        get_snapshot_by_id=Mock(return_value=None),
        get_latest_snapshot=Mock(side_effect=AssertionError('Expired plans cannot be rebound to latest')))
    table = SimpleNamespace(snapshot_manager=Mock(return_value=manager))
    core = SimpleNamespace(read_plan=Mock())
    reader = NativeSearchRead(table, core, True, full_text=full_text)
    requested = (PrimaryKeyFullTextScanPlan if full_text else PrimaryKeyVectorScanPlan)(7, [object()])
    with patch('pypaimon.execution.native.read.native_search_plan._plan_to_native') as convert:
        with pytest.raises(ValueError, match='7'):
            reader.read_plan(requested)
    manager.get_snapshot_by_id.assert_called_once_with(7)
    manager.get_latest_snapshot.assert_not_called()
    core.read_plan.assert_not_called()
    convert.assert_not_called()


@pytest.mark.parametrize('full_text', [False, True])
@pytest.mark.parametrize('split_snapshot', [None, 7])
def test_primary_key_sources_round_trip_as_typed_splits(search_table, full_text, split_snapshot):
    from pypaimon.execution.native.read.native_search_plan import _plan_from_native, _plan_to_native
    from pypaimon.index.index_file_meta import IndexFileMeta
    from pypaimon.manifest.schema.data_file_meta import DataFileMeta
    from pypaimon.manifest.schema.simple_stats import SimpleStats
    from pypaimon.read.split import DataSplit
    from pypaimon.read.split_serializer import serialize_split_v1
    from pypaimon.snapshot.snapshot import Snapshot
    from pypaimon.table.row.generic_row import GenericRow
    from pypaimon.table.source.deletion_file import DeletionFile
    from pypaimon.table.source.search_types import (
        PrimaryKeyFullTextScanPlan, PrimaryKeyFullTextSearchSplit,
        PrimaryKeyVectorScanPlan, PrimaryKeyVectorSearchSplit)
    from pypaimon.utils.range import Range

    sdk, datafusion = _typed_native_sdk()
    snapshot = Snapshot(3, 7, 2, 'base', 'delta', 10, 10, 'writer', 1, 'APPEND', 0, next_row_id=10)
    empty = GenericRow([], [])
    file = DataFileMeta.create('tail.parquet', 256, 5, empty, empty,
                               SimpleStats.empty_stats(), SimpleStats.empty_stats(), 0, 4, 2, 1, [])
    data = DataSplit([file], empty, 0, data_deletion_files=[DeletionFile('dv/one', 2, 10, 3)],
                     snapshot_id=split_snapshot, bucket_path='bucket-0', total_buckets=1)
    payload = IndexFileMeta('full-text' if full_text else 'ivf-flat', 'payload', 1024, 4,
                            external_path='s3://bucket/payload')
    split_class = PrimaryKeyFullTextSearchSplit if full_text else PrimaryKeyVectorSearchSplit
    source_class = PrimaryKeyFullTextScanPlan if full_text else PrimaryKeyVectorScanPlan
    source_split = split_class(
        data, (payload,), ('tail.parquet',), {'tail.parquet': (Range(1, 2), Range(4, 4))})
    source = source_class(7, [source_split], snapshot)
    with patch.dict('sys.modules', {'pypaimon_rust': sdk, 'pypaimon_rust.datafusion': datafusion}):
        native = _plan_to_native(pickle.loads(pickle.dumps(source)), full_text)
        restored = _plan_from_native(search_table, native, full_text, True)
    expected_type = datafusion.PrimaryKeyFullTextSearchSplit if full_text else datafusion.PrimaryKeyVectorSearchSplit
    assert type(native.splits()[0]) is expected_type
    assert type(native.splits()[0].data_split()) is datafusion.Split
    from copy import copy
    normalized = copy(data)
    normalized.snapshot_id = 7
    assert serialize_split_v1(restored.splits()[0].data_split) == serialize_split_v1(normalized)
    assert data.snapshot_id == split_snapshot
    assert restored.snapshot() == snapshot and restored.snapshot_id == 7
    assert restored.splits()[0].payloads == (payload,)
    assert restored.splits()[0].uncovered_data_files == ('tail.parquet',)
    assert restored.splits()[0].row_ranges_by_file == source.splits()[0].row_ranges_by_file


@pytest.mark.parametrize('full_text', [False, True])
@pytest.mark.parametrize('indexed', [False, True])
def test_search_plan_conversion_rejects_splits_from_the_other_search_family(search_table, full_text, indexed):
    from pypaimon.execution.native.read.native_search_plan import _plan_from_native, _plan_to_native
    from pypaimon.table.source.full_text_search_split import IndexFullTextSearchSplit, RawFullTextSearchSplit
    from pypaimon.table.source.vector_search_split import IndexVectorSearchSplit, RawVectorSearchSplit
    from pypaimon.utils.range import Range

    sdk, datafusion = _typed_native_sdk()
    if full_text:
        wrong = IndexVectorSearchSplit(0, 1, []) if indexed else RawVectorSearchSplit([Range(0, 1)])
        wrong_native = (datafusion.IndexVectorSearchSplit(0, 1, []) if indexed else
                        datafusion.RawVectorSearchSplit([(0, 1)]))
    else:
        wrong = IndexFullTextSearchSplit('body', 0, 1, []) if indexed else RawFullTextSearchSplit([Range(0, 1)])
        wrong_native = (datafusion.IndexFullTextSearchSplit('body', 0, 1, []) if indexed else
                        datafusion.RawFullTextSearchSplit([(0, 1)]))
    source = (FullTextScanPlan if full_text else VectorSearchScanPlan)([wrong], None)
    native = SimpleNamespace(snapshot=lambda: None, splits=lambda: [wrong_native])
    with patch.dict('sys.modules', {'pypaimon_rust': sdk, 'pypaimon_rust.datafusion': datafusion}):
        with pytest.raises(TypeError, match='search split'):
            _plan_to_native(source, full_text)
        with pytest.raises(TypeError, match='search split'):
            _plan_from_native(search_table, native, full_text, False)


@pytest.mark.parametrize('family', ['vector', 'batch', 'text'])
def test_selected_native_reader_does_not_probe_or_replay_an_older_sdk(search_table, family):
    class OlderSdk(ModuleType):
        def __getattr__(self, name):
            if name in ('VectorScanPlan', 'FullTextScanPlan'):
                raise AttributeError('required typed search plan API is missing')
            raise AssertionError('Unexpected Native SDK capability probe: ' + name)

    sdk, datafusion = ModuleType('pypaimon_rust'), OlderSdk('pypaimon_rust.datafusion')
    sdk.datafusion = datafusion
    native, _, core_read = _native_builder(family)
    builder = _builder(search_table, family)
    _, read_factory = _factories(builder, family)
    with patch.dict('sys.modules', {'pypaimon_rust': sdk, 'pypaimon_rust.datafusion': datafusion}), \
            _forbid_python_execution(), patch(_native_factory(family), return_value=native):
        reader = read_factory()
        with pytest.raises(AttributeError, match='required typed search plan API'):
            (reader.read_batch_plan(_ordinary_plan(family)) if family == 'batch' else
             reader.read_plan(_ordinary_plan(family)))
    assert reader._python_executor is None
    core_read.read_plan.assert_not_called()
    core_read.read_batch_plan.assert_not_called()


@pytest.mark.parametrize('family', ['vector', 'batch', 'text', 'hybrid'])
@pytest.mark.parametrize('failure', [False, True])
def test_native_search_adapters_use_only_sdk_execute(search_table, family, failure):
    from pypaimon.execution.native.read import native_full_text_search, native_hybrid_search, native_vector_search

    expected = SimpleNamespace(row_ids=lambda: {7: 0.75})
    error = OSError('Native execution started')

    class ExecuteOnlyBuilder:
        def __init__(self):
            self.execute = Mock(side_effect=error if failure else None,
                                return_value=[expected, expected] if family == 'batch' else expected)
            for method in ('with_query_vector', 'with_query_vectors', 'with_limit', 'with_ranker',
                           'add_vector_route', 'add_full_text_route'):
                setattr(self, method, Mock(return_value=self))

        def __getattr__(self, name):
            if name in ('execute_local', 'execute_batch_local'):
                raise AssertionError('Native adapter probed a removed SDK method: ' + name)
            raise AttributeError(name)

    core = ExecuteOnlyBuilder()
    with ExitStack() as stack:
        if family in ('vector', 'batch'):
            stack.enter_context(patch.object(native_vector_search, '_native_vector_builder', return_value=core))
            builder = _builder(search_table, family)
            queries = builder._query_vectors if family == 'batch' else builder._query_vector
            operation = lambda: native_vector_search.try_native_vector_search(builder, queries, family == 'batch')
        elif family == 'text':
            stack.enter_context(patch.object(native_full_text_search, '_native_full_text_builder', return_value=core))
            operation = lambda: native_full_text_search.try_native_full_text_search(_builder(search_table, 'text'))
        else:
            stack.enter_context(patch.object(native_hybrid_search, 'native_runtime_available', return_value=True))
            stack.enter_context(patch.object(native_hybrid_search, '_rest_catalog_supported', return_value=True))
            stack.enter_context(patch.object(native_hybrid_search, '_native_table', return_value=SimpleNamespace(
                new_hybrid_search_builder=Mock(return_value=core))))
            builder = search_table.new_hybrid_search_builder().with_limit(2)
            builder.add_vector_route('embedding', [1.0, 0.0], 2)
            builder.add_full_text_route('body', '{"match":{"query":"needle"}}', 2)
            operation = lambda: native_hybrid_search.try_native_hybrid_search(builder)
        if failure:
            with pytest.raises(OSError) as caught:
                operation()
            assert caught.value is error
        else:
            results = operation()
            if family != 'batch':
                results = [results]
            assert [{row: result.score_getter()(row) for row in result.results()} for result in results] == (
                [{7: 0.75}] * (2 if family == 'batch' else 1))
    core.execute.assert_called_once_with()
