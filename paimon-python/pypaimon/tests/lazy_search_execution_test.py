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
from pypaimon.execution.native.read import native_search_plan
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
        assert pickle.loads(pickle.dumps(read))._native is None
        assert pickle.loads(pickle.dumps(scan))._scan is None
    core_scan.scan.assert_called_once_with()
    (core_read.read_batch_plan if family == 'batch' else core_read.read_plan).assert_called_once_with(plan._native_plan)


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
    fallback = native_search_plan.python_search_reader(reader)
    assert fallback._options == {'ivf-flat.metric': 'l2'}


def test_batch_reader_freezes_query_sequence_before_lazy_fallback(search_table):
    queries = [[1.0, 0.0]]
    builder = _builder(search_table, 'batch').with_query_vectors(queries)
    native, _, _ = _native_builder('batch')
    with patch(_native_factory('batch'), return_value=native):
        reader = builder.new_batch_vector_search_read()
    queries.append([0.0, 1.0])
    fallback = native_search_plan.python_search_reader(pickle.loads(pickle.dumps(reader)))
    assert fallback._query_vectors == [[1.0, 0.0]]


@pytest.mark.parametrize('module,name', [
    ('vector_search_scan', 'VectorSearchScanPlan'),
    ('vector_search_scan', 'VectorSearchScan'),
    ('vector_search_read', 'VectorSearchRead'),
    ('vector_search_read', 'BatchVectorSearchRead'),
    ('full_text_scan', 'FullTextScanPlan'),
    ('full_text_scan', 'FullTextScan'),
    ('full_text_read', 'FullTextRead'),
    ('primary_key_vector_scan', 'PrimaryKeyVectorSearchSplit'),
    ('primary_key_vector_scan', 'PrimaryKeyVectorScanPlan'),
    ('primary_key_full_text_scan', 'PrimaryKeyFullTextSearchSplit'),
    ('primary_key_full_text_scan', 'PrimaryKeyFullTextScanPlan'),
])
def test_shared_search_contracts_keep_original_import_and_pickle_paths(module, name):
    old_module = 'pypaimon.table.source.' + module
    canonical = getattr(importlib.import_module('pypaimon.table.source.search_types'), name)
    assert getattr(importlib.import_module(old_module), name) is canonical
    assert pickle.loads(('c%s\n%s\n.' % (old_module, name)).encode('ascii')) is canonical


def test_vector_scan_keeps_its_shared_split_export():
    from pypaimon.table.source.vector_search_scan import VectorSearchSplit
    from pypaimon.table.source.vector_search_split import VectorSearchSplit as shared_split

    assert VectorSearchSplit is shared_split
    assert pickle.loads(b'cpypaimon.table.source.vector_search_scan\nVectorSearchSplit\n.') is shared_split
