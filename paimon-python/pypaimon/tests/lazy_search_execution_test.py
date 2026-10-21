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
import pickle
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.table.source import search_dispatch
from pypaimon.table.source.search_types import VectorSearchScanPlan


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


def _factories(builder, family):
    if family == 'text':
        return builder.new_full_text_scan, builder.new_full_text_read
    if family == 'batch':
        return builder.new_vector_search_scan, builder.new_batch_vector_search_read
    return builder.new_vector_search_scan, builder.new_vector_search_read


def _native_builder(family):
    plan = SimpleNamespace(_metadata=lambda: '{"snapshot":null,"snapshot_id":null,"splits":[]}')
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


@pytest.mark.parametrize('family', ['vector', 'batch', 'text'])
def test_native_search_prepares_scans_reads_and_serializes_without_python(search_table, family):
    builder = _builder(search_table, family)
    native, core_scan, core_read = _native_builder(family)
    scan_factory, read_factory = _factories(builder, family)
    with _forbid_python_execution(), patch(_native_factory(family), return_value=native):
        scan, read = scan_factory(), read_factory()
        plan = scan.scan()
        results = read.read_batch_plan(plan) if family == 'batch' else [read.read_plan(plan)]
        assert [list(result.results()) for result in results] == [[7]] * (2 if family == 'batch' else 1)
        # Dropping live handles for transport must not create fallback readers.
        assert pickle.loads(pickle.dumps(read))._native_reader._native is None
        assert not pickle.loads(pickle.dumps(scan))._native_scan.available()
    core_scan.scan.assert_called_once_with()
    (core_read.read_batch_plan if family == 'batch' else core_read.read_plan).assert_called_once_with(plan._native_plan)


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
def test_native_search_split_read_selects_python_only_when_invoked(search_table, family):
    builder = _builder(search_table, family)
    native, _, _ = _native_builder(family)
    _, read_factory = _factories(builder, family)
    method_name = 'read_batch' if family == 'batch' else 'read'
    python_method = Mock(return_value='split fallback')
    python = SimpleNamespace(**{method_name: python_method})
    factory = ('_new_python_full_text_read' if family == 'text' else
               '_new_python_batch_vector_read' if family == 'batch' else '_new_python_vector_read')
    with patch(_native_factory(family), return_value=native), \
            patch.object(type(builder), factory, return_value=python) as prepare:
        reader = read_factory()
        split_read = getattr(reader, method_name)
        prepare.assert_not_called()
        splits = ['transported split']
        assert split_read(splits) == 'split fallback'
        assert split_read(splits) == 'split fallback'
    prepare.assert_called_once_with()
    assert python_method.call_args_list == [((splits,),), ((splits,),)]


@pytest.mark.parametrize('phase', ['scan', 'read'])
def test_native_search_failure_never_creates_python_execution(search_table, phase):
    builder = _builder(search_table, 'vector')
    native, core_scan, core_read = _native_builder('vector')
    failure = OSError('search may already have read remote state')
    with _forbid_python_execution(), patch(_native_factory('vector'), return_value=native):
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


def test_python_plan_fallback_is_created_once_when_actually_needed(search_table):
    builder = _builder(search_table, 'vector')
    native, _, _ = _native_builder('vector')
    python = SimpleNamespace(_table=search_table, read_plan=Mock(return_value='fallback'))
    with patch(_native_factory('vector'), return_value=native), \
            patch.object(type(builder), '_new_python_vector_read', return_value=python) as prepare:
        reader = builder.new_vector_search_read()
        prepare.assert_not_called()
        plan = VectorSearchScanPlan([], None)
        assert reader.read_plan(plan) == 'fallback'
        assert reader.read_plan(plan) == 'fallback'
    prepare.assert_called_once_with()
    assert python.read_plan.call_count == 2


def test_native_reader_keeps_the_query_options_captured_at_construction(search_table):
    builder = _builder(search_table, 'vector').with_option('ivf-flat.metric', 'l2')
    native, _, _ = _native_builder('vector')
    with patch(_native_factory('vector'), return_value=native):
        reader = builder.new_vector_search_read()
    builder.with_option('ivf-flat.metric', 'cosine')
    fallback = search_dispatch.python_search_reader(reader)
    assert fallback._options == {'ivf-flat.metric': 'l2'}


def test_batch_reader_freezes_query_sequence_before_lazy_fallback(search_table):
    queries = [[1.0, 0.0]]
    builder = _builder(search_table, 'batch').with_query_vectors(queries)
    native, _, _ = _native_builder('batch')
    with patch(_native_factory('batch'), return_value=native):
        reader = builder.new_batch_vector_search_read()
    queries.append([0.0, 1.0])
    queries[0][:] = [3.0, 4.0]
    assert native.with_query_vectors.call_args[0][0] == [[1.0, 0.0]]
    fallback = search_dispatch.python_search_reader(pickle.loads(pickle.dumps(reader)))
    assert fallback._query_vectors == [[1.0, 0.0]]


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


def test_single_reader_freezes_query_values_before_transport_fallback(search_table):
    query = [1.0, 0.0]
    builder = _builder(search_table, 'vector').with_query_vector(query)
    native, _, _ = _native_builder('vector')
    with patch(_native_factory('vector'), return_value=native):
        reader = builder.new_vector_search_read()
    query[:] = [3.0, 4.0]
    assert native.with_query_vector.call_args[0][0] == [1.0, 0.0]
    fallback = search_dispatch.python_search_reader(pickle.loads(pickle.dumps(reader)))
    assert fallback._query_vector == [1.0, 0.0]
