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

"""Explicit search plans keep native work and transportable source metadata."""

from contextlib import ExitStack, contextmanager
from copy import copy
import pickle
from unittest.mock import patch

import pytest
import pyarrow as pa

from pypaimon.read.native_plan import native_method_available
from pypaimon.table.source.full_text_read import DataEvolutionFullTextRead
from pypaimon.table.source.full_text_scan import DataEvolutionFullTextScan
from pypaimon.table.source.primary_key_full_text_read import PrimaryKeyFullTextRead
from pypaimon.table.source.primary_key_full_text_scan import PrimaryKeyFullTextScan
from pypaimon.table.source.primary_key_vector_read import PrimaryKeyVectorRead
from pypaimon.table.source.primary_key_vector_scan import PrimaryKeyVectorScan
from pypaimon.table.source.vector_search_read import BatchVectorSearchReadImpl, DataEvolutionVectorRead
from pypaimon.table.source.vector_search_scan import DataEvolutionVectorScan
from pypaimon.tests import native_hybrid_search_test as hybrid
from pypaimon.tests import native_pk_full_text_search_test as text
from pypaimon.tests import native_sorted_index_build_test as indexes
from pypaimon.tests import native_vector_search_test as vectors

rest_catalog = indexes.rest_catalog
golden_catalog = text.golden_catalog
source = text.source
pk_vector_table = vectors.pk_vector_table
pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_method_available('VectorScanPlan', '_metadata'), reason='Rust search plan metadata required')]


@pytest.fixture
def table(rest_catalog):
    table = hybrid._table(rest_catalog[0], options={
        'write.native.enabled': 'false', 'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
    }, partial=True)
    table.create_global_index('label', index_type='btree')
    # Count actual table planning/reading in the native CI lanes, in addition
    # to the separate search APIs exercised below.
    read = table.copy({'read.native.enabled': 'true', 'scan.native-plan.enabled': 'true'}).new_read_builder()
    assert read.new_read().to_arrow(read.new_scan().plan().splits()).num_rows == 12
    return table


def _builder(table, family, *, native=False, filtered=False, partition=False, limit=3):
    table = table.copy({'read.native.enabled': str(native).lower(),
                        'scan.native-plan.enabled': str(native).lower()})
    if family == 'full-text':
        builder = text._builder(table, text='paimon', limit=limit)
    elif family == 'batch':
        builder = (table.new_batch_vector_search_builder().with_vector_column('embedding')
                   .with_query_vectors([[0., 0.], [7., 0.]]).with_limit(limit))
    else:
        builder = (table.new_vector_search_builder().with_vector_column('embedding')
                   .with_query_vector([0., 0.]).with_limit(limit))
    predicate = table.new_read_builder().new_predicate_builder()
    if filtered:
        builder.with_filter(predicate.equal('label', 'keep'))
    if partition:
        builder.with_partition_filter(predicate.equal('pt', 1))
    return builder


def _scan(builder, family):
    return builder.new_full_text_scan() if family == 'full-text' else builder.new_vector_search_scan()


def _reader(builder, family):
    if family == 'full-text':
        return builder.new_full_text_read()
    return builder.new_batch_vector_search_read() if family == 'batch' else builder.new_vector_search_read()


def _read(reader, family, plan):
    return reader.read_batch_plan(plan) if family == 'batch' else [reader.read_plan(plan)]


def _scores(result):
    if hasattr(result, 'positions'):
        return {(p.partition_bytes, p.bucket, p.data_file_name, p.row_position): p.score for p in result.positions}
    getter = result.score_getter() if not result.is_empty() else None
    return {row_id: getter(row_id) for row_id in result.results()}


def _assert_results(actual, expected):
    assert len(actual) == len(expected)
    for a, b in zip(actual, expected):
        assert _scores(a) == pytest.approx(_scores(b), abs=1e-6)


def _native_only():
    stack = ExitStack()
    for cls in [DataEvolutionVectorScan, DataEvolutionFullTextScan,
                PrimaryKeyVectorScan, PrimaryKeyFullTextScan]:
        stack.enter_context(patch.object(cls, 'scan', side_effect=AssertionError('Python search scan ran')))
    for cls, method in [(DataEvolutionVectorRead, 'read_plan'), (DataEvolutionFullTextRead, 'read_plan'),
                        (BatchVectorSearchReadImpl, 'read_batch_plan'), (PrimaryKeyVectorRead, 'read_plan'),
                        (PrimaryKeyFullTextRead, 'read_plan')]:
        stack.enter_context(patch.object(cls, method, side_effect=AssertionError('Python search read ran')))
    return stack


@pytest.mark.parametrize('family', ['vector', 'batch', 'full-text'])
@pytest.mark.parametrize('mode', ['fast', 'full', 'detail'])
@pytest.mark.parametrize('filtered,partition', [(False, False), (True, False), (False, True)])
def test_explicit_native_plan_and_read_avoid_python_search(table, family, mode, filtered, partition):
    table = table.copy({'vector-index.search-mode': mode, 'full-text-index.search-mode': mode})
    classic = _builder(table, family, filtered=filtered, partition=partition)
    expected_plan = _scan(classic, family).scan()
    expected = _read(_reader(classic, family), family, expected_plan)
    native = _builder(table, family, native=True, filtered=filtered, partition=partition)
    with _native_only():
        plan = _scan(native, family).scan()
        actual = _read(_reader(native, family), family, plan)
    assert plan.snapshot().id == expected_plan.snapshot().id
    _assert_results(actual, expected)
    # Ordinary Python readers can consume the exported metadata directly.
    _assert_results(_read(_reader(classic, family), family, pickle.loads(pickle.dumps(plan))), expected)


@pytest.mark.parametrize('family', ['vector', 'batch', 'full-text'])
@pytest.mark.parametrize('transport', ['pickle-plan', 'pickle-reader', 'split-list'])
def test_transported_plan_uses_captured_snapshot_without_replanning(table, family, transport):
    native = _builder(table, family, native=True, limit=20)
    plan = _scan(native, family).scan()
    reader = _reader(native, family)
    expected = _read(reader, family, plan)
    snapshot = plan.snapshot().id
    captured = pickle.dumps(plan.splits())
    indexes._append(table, hybrid._rows(12, 4), hybrid.SCHEMA)
    if transport == 'pickle-plan':
        plan = pickle.loads(pickle.dumps(plan))
    elif transport == 'pickle-reader':
        reader = pickle.loads(pickle.dumps(reader))
    with patch.object(DataEvolutionVectorScan, 'scan', side_effect=AssertionError('Replanned vector search')), \
            patch.object(DataEvolutionFullTextScan, 'scan', side_effect=AssertionError('Replanned full-text search')):
        if transport == 'split-list':
            classic = _reader(_builder(table._copy_with_snapshot(plan.snapshot()), family, limit=20), family)
            actual = classic.read_batch(plan.splits()) if family == 'batch' else [classic.read(plan.splits())]
        else:
            actual = _read(reader, family, plan)
    assert plan.snapshot().id == snapshot
    assert pickle.dumps(plan.splits()) == captured
    _assert_results(actual, expected)


@pytest.mark.parametrize('family', ['vector', 'batch', 'full-text'])
def test_modified_plan_and_shallow_alias_observe_current_split_selection(table, family):
    builder = _builder(table, family, native=True)
    plan = _scan(builder, family).scan()
    alias = copy(plan)
    alias.splits().clear()
    results = _read(_reader(builder, family), family, plan)
    assert len(results) == (2 if family == 'batch' else 1)
    assert all(result.is_empty() for result in results)


@pytest.mark.parametrize('family', ['vector', 'batch', 'full-text'])
def test_read_plan_context_errors_do_not_retry_python(table, family):
    plan = _scan(_builder(table, family, native=True), family).scan()
    reader = _reader(_builder(table, family, native=True, filtered=True), family)
    with _native_only(), pytest.raises(Exception, match='same table|same.*filter|different table|different.*filter'):
        _read(reader, family, plan)


@pytest.mark.parametrize('family', ['vector', 'batch', 'full-text'])
def test_reader_modes_do_not_expand_the_captured_plan(table, family):
    fast = table.copy({'vector-index.search-mode': 'fast', 'full-text-index.search-mode': 'fast'})
    native = _builder(fast, family, native=True, limit=20)
    plan = _scan(native, family).scan()
    read_builder = _builder(table, family, native=True, limit=20)
    if family != 'full-text':
        read_builder.with_options({'vector-index.search-mode': 'full'})
    reader = _reader(read_builder, family)
    transported = pickle.loads(pickle.dumps(plan))
    expected = _read(reader, family, transported)
    actual = _read(reader, family, plan)
    _assert_results(actual, expected)
    assert all(set(_scores(result)) <= set(range(8)) for result in actual)


@pytest.mark.parametrize('family', ['vector', 'batch'])
def test_reader_table_options_apply_to_captured_sources(rest_catalog, family):
    table = hybrid._table(rest_catalog[0], indexed=False, options={
        'write.native.enabled': 'false', 'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    scan_builder = _builder(table.copy({'fields.embedding.distance.metric': 'l2'}), family, native=True, limit=2)
    plan = _scan(scan_builder, family).scan()
    read_builder = _builder(table.copy({'fields.embedding.distance.metric': 'inner_product'}),
                            family, native=True, limit=2)
    if family == 'vector':
        read_builder.with_query_vector([1., 0.])
    else:
        read_builder.with_query_vectors([[1., 0.], [2., 0.]])
    reader = _reader(read_builder, family)
    expected = _read(reader, family, pickle.loads(pickle.dumps(plan)))
    _assert_results(_read(reader, family, plan), expected)
    assert all(set(_scores(result)) == {6, 7} for result in expected)


@pytest.mark.parametrize('family', ['vector', 'batch'])
@pytest.mark.parametrize('metric', [None, 'cosine'])
@pytest.mark.parametrize('index_type', [' IVF-FLAT ', 'lumina', 'lumina-vector-ann'])
def test_raw_native_plans_use_java_configured_indexer_default(rest_catalog, family, metric, index_type):
    options = {
        'fields.embedding.index-type': index_type, 'write.native.enabled': 'false',
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
        'fields.other.distance.metric': 'l2',
    }
    if metric is not None:
        options[index_type.strip().lower() + '.distance.metric'] = metric
    table = hybrid._table(rest_catalog[0], indexed=False, options=options)
    builder = _builder(table, family, native=True, limit=2)
    builder.with_options({'fields.other.metric': 'l2'})
    if family == 'vector':
        builder.with_query_vector([1., 0.])
    else:
        builder.with_query_vectors([[1., 0.], [2., 0.]])
    with _native_only():
        plan = _scan(builder, family).scan()
        actual = _read(_reader(builder, family), family, plan)
    assert all(split.index_type == index_type.strip().lower() for split in plan.splits())
    expected = _read(_reader(builder, family)._python, family, pickle.loads(pickle.dumps(plan)))
    _assert_results(actual, expected)
    assert all(set(_scores(result)) == ({6, 7} if metric is None else {1, 2}) for result in actual)


def test_native_scan_and_execute_reject_mixed_persisted_vector_formats(table):
    table.create_global_index('embedding', index_type='ivf-sq', options={
        'ivf-sq.dimension': '2', 'ivf-sq.nlist': '1', 'ivf-sq.distance.metric': 'l2'})
    classic = _builder(table, 'vector')
    with pytest.raises(ValueError, match='multiple index types'):
        classic.new_vector_search_scan().scan()
    for family in ('vector', 'batch'):
        builder = _builder(table, family, native=True)
        with _native_only(), pytest.raises(Exception, match='multiple index types'):
            builder.new_vector_search_scan().scan()
        with _native_only(), pytest.raises(Exception, match='multiple index types'):
            (builder.execute_batch_local() if family == 'batch' else builder.execute_local())


@pytest.mark.parametrize('family', ['vector', 'batch'])
def test_persisted_vector_family_ignores_other_columns_metric(table, family):
    assert table.options.options.to_map().get('fields.embedding.index-type') is None
    builder = _builder(table, family, native=True, limit=20).with_options({
        'fields.other.metric': 'inner_product', 'ivf-flat.distance.metric': 'l2'})
    with _native_only():
        plan = _scan(builder, family).scan()
        actual = _read(_reader(builder, family), family, plan)
        implicit = builder.execute_batch_local() if family == 'batch' else [builder.execute_local()]
    expected = _read(_reader(builder, family)._python, family, pickle.loads(pickle.dumps(plan)))
    _assert_results(actual, expected)
    _assert_results(implicit, expected)


@pytest.mark.parametrize('family', ['vector', 'batch', 'full-text'])
def test_empty_native_plans_remain_transportable(rest_catalog, family):
    catalog, _ = rest_catalog
    table = indexes._create(catalog, schema=hybrid.SCHEMA)
    builder = _builder(table, family, native=True)
    with _native_only():
        plan = _scan(builder, family).scan()
        results = _read(_reader(builder, family), family, plan)
    assert plan.snapshot() is None
    assert plan.splits() == []
    assert all(result.is_empty() for result in results)
    _assert_results(_read(_reader(builder, family), family, pickle.loads(pickle.dumps(plan))), results)


def test_vector_scan_does_not_require_query_or_limit(table):
    builder = table.copy({'read.native.enabled': 'true'}).new_vector_search_builder().with_vector_column('embedding')
    with _native_only():
        plan = builder.new_vector_search_scan().scan()
    assert plan.snapshot() is not None and plan.splits()
    builder.with_query_vector([0., 0.]).with_limit(3)
    with _native_only():
        assert len(builder.new_vector_search_read().read_plan(plan).results()) == 3


def test_full_text_scan_does_not_require_limit(table):
    builder = text._builder(table.copy({'read.native.enabled': 'true'}), limit=None)
    with _native_only():
        plan = builder.new_full_text_scan().scan()
    assert plan.snapshot() is not None and plan.splits()
    builder.with_limit(3)
    with _native_only():
        assert len(builder.new_full_text_read().read_plan(plan).results()) == 3


@pytest.mark.parametrize('family', ['vector', 'batch', 'full-text'])
def test_native_plan_is_query_independent_and_pickle_keeps_original_handle(table, family):
    builder = _builder(table, family, native=True, limit=1)
    plan = _scan(builder, family).scan()
    pickle.dumps(plan)
    assert plan._unchanged_native_plan() is not None
    if family == 'full-text':
        builder.with_query('content', text.match_query('lake'))
    elif family == 'batch':
        builder.with_query_vectors([[7., 0.], [0., 0.], [4., 0.]])
    else:
        builder.with_query_vector([7., 0.]).with_options({'ivf.nprobe': '1'})
    builder.with_limit(4)
    classic = _reader(builder, family)._python
    expected = _read(classic, family, pickle.loads(pickle.dumps(plan)))
    with _native_only():
        actual = _read(_reader(builder, family), family, plan)
    _assert_results(actual, expected)


@pytest.mark.parametrize('family', ['vector', 'batch', 'full-text'])
def test_ray_accepts_native_reader_and_transports_captured_sources(table, family):
    # Run the real Ray adapters and workers with in-process serialized task
    # dispatch. This exercises their reader qualification and metadata contract
    # without starting another cluster in the native CI jobs.
    from pypaimon.ray import vector_search, batch_vector_search, full_text_search
    module, reader_type, execute, method = {
        'vector': (vector_search, vector_search._RayVectorSearchRead,
                   vector_search._execute_vector_search, 'read_plan'),
        'batch': (batch_vector_search, batch_vector_search._RayBatchVectorSearchRead,
                  batch_vector_search._execute_batch_vector_search, 'read_batch_plan'),
        'full-text': (full_text_search, full_text_search._RayFullTextRead,
                      full_text_search._execute_full_text_search, 'read_plan'),
    }[family]
    classic = _builder(table, family, limit=20)
    expected = _read(_reader(classic, family), family, _scan(classic, family).scan())
    original_read = getattr(reader_type, method)

    def read_after_append(reader, plan):
        captured = plan.snapshot().id
        transported = pickle.loads(pickle.dumps(plan))
        assert plan._unchanged_native_plan() is not None
        indexes._append(table, hybrid._rows(12, 4), hybrid.SCHEMA)
        assert transported.snapshot().id == captured
        return original_read(reader, transported)

    def dispatch(worker, context, items, *args):
        for ordinal, item in enumerate(items):
            yield ordinal, worker(pickle.loads(pickle.dumps(context)), pickle.loads(pickle.dumps(item)))

    @contextmanager
    def scalar_filters(*args):
        yield None

    with ExitStack() as stack:
        stack.enter_context(patch.object(module, '_require_ray'))
        stack.enter_context(patch.object(module, '_map_tasks', dispatch))
        stack.enter_context(patch.object(reader_type, method, read_after_append))
        if family != 'full-text':
            stack.enter_context(patch.object(module, '_shared_scalar_filters', scalar_filters))
        for cls in (DataEvolutionVectorScan, DataEvolutionFullTextScan):
            stack.enter_context(patch.object(cls, 'scan', side_effect=AssertionError('Python search scan ran')))
        actual = execute(_builder(table, family, native=True, limit=20))
    _assert_results(actual if family == 'batch' else [actual], expected)


@pytest.mark.parametrize('family', ['vector', 'full-text'])
@pytest.mark.parametrize('filtered', [False, True])
def test_pk_native_plans_export_physical_sources_for_python_readers(source, pk_vector_table, family, filtered):
    table = source[0] if family == 'full-text' else pk_vector_table

    def builder(native):
        value = table.copy({'read.native.enabled': str(native).lower(), 'scan.native-plan.enabled': 'false'})
        query = text._builder(value) if family == 'full-text' else vectors._single(value, [1., 0., 0., 0.], 2)
        if filtered and family == 'vector':
            query.with_filter(value.new_read_builder().new_predicate_builder().equal('id', 3))
        return query

    classic = builder(False)
    expected = _read(_reader(classic, family), family, _scan(classic, family).scan())
    native = builder(True)
    with _native_only():
        plan = _scan(native, family).scan()
        actual = _read(_reader(native, family), family, plan)
    assert plan.snapshot_id == expected[0].snapshot_id
    assert plan.snapshot().id == plan.snapshot_id
    _assert_results(actual, expected)
    transported = pickle.loads(pickle.dumps(plan))
    _assert_results(_read(_reader(classic, family), family, transported), expected)


@pytest.mark.parametrize('family', ['vector', 'full-text'])
def test_pk_plan_retains_snapshot_and_sources_after_primary_key_update(source, pk_vector_table, family):
    table = source[0] if family == 'full-text' else pk_vector_table
    native = table.copy({'read.native.enabled': 'true', 'scan.native-plan.enabled': 'false'})
    builder = text._builder(native) if family == 'full-text' else vectors._single(native, [1., 0., 0., 0.], 2)
    plan = _scan(builder, family).scan()
    expected = _read(_reader(builder, family), family, plan)
    expected_rows = vectors._rows(table, expected[0]).to_pylist()
    captured = plan.snapshot_id
    if family == 'full-text':
        schema = pa.schema([pa.field('id', pa.int32(), False), pa.field('content', pa.string())])
        rows = [{'id': 1, 'content': 'changed after the captured search'}, {'id': 100, 'content': 'new document'}]
    else:
        schema = pa.schema([pa.field('id', pa.int32(), False),
                            pa.field('embedding', pa.list_(pa.float32(), 4))])
        rows = [{'id': 1, 'embedding': [0., 0., 0., 1.]}, {'id': 100, 'embedding': [0., 0., 0., 1.]}]
    write = table.copy({'write.native.enabled': 'true', 'pk-vector.index.columns': '',
                        'pk-full-text.index.columns': ''}).new_batch_write_builder()
    writer, commit = write.new_write(), write.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    assert table.snapshot_manager().get_latest_snapshot().id > captured
    with _native_only():
        actual = _read(_reader(builder, family), family, plan)
    transported = _read(_reader(builder, family), family, pickle.loads(pickle.dumps(plan)))
    _assert_results(actual, expected)
    _assert_results(transported, expected)
    assert vectors._rows(table, actual[0]).to_pylist() == expected_rows
    assert vectors._rows(table, transported[0]).to_pylist() == expected_rows
