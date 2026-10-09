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

"""Existing local vector searches delegated to Rust with a REST table context."""

import shutil
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.read.native_plan import _native_table, _predicate_to_native, native_method_available
from pypaimon.table.source.primary_key_scored_result import PrimaryKeyScoredResult
from pypaimon.tests import native_plan_rest_test, primary_key_global_index_golden_test

rest_catalog = native_plan_rest_test.rest_catalog
golden_catalog = primary_key_global_index_golden_test.catalog

pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_method_available('Table', 'new_vector_search_builder'), reason='Rust vector API required')]


def _write(table, rows):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=pa.schema([
            ('id', pa.int32()), ('embedding', pa.list_(pa.float32())), ('pt', pa.int32())])))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


@pytest.fixture
def vector_table(rest_catalog):
    catalog, _ = rest_catalog
    catalog.create_table('default.vectors', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int32()), ('embedding', pa.list_(pa.float32())), ('pt', pa.int32())]),
        partition_keys=['pt'], options={
            'file.format': 'parquet', 'bucket': '-1', 'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true', 'global-index.enabled': 'true',
            'vector-index.search-mode': 'full', 'read.batch-size': '1',
        }), False)
    table = catalog.get_table('default.vectors')
    _write(table, [
        {'id': 1, 'embedding': [1, 0], 'pt': 0},
        {'id': 2, 'embedding': [0, 1], 'pt': 0},
        {'id': 3, 'embedding': None, 'pt': 0},
        {'id': 4, 'embedding': [1, 0], 'pt': 1},
        {'id': 5, 'embedding': [0, 1], 'pt': 1},
        {'id': 6, 'embedding': [1, 1], 'pt': 1},
    ])
    return table


def _single(table, query=(1, 0), limit=3):
    return table.new_vector_search_builder().with_vector_column('embedding').with_query_vector(query).with_limit(limit)


def _scores(result):
    getter = result.score_getter()
    return {row_id: getter(row_id) for row_id in result.results()}


def _assert_scores(actual, expected):
    assert set(actual) == set(expected)
    assert actual == pytest.approx(expected, abs=1e-6)


def _rows(table, result):
    builder = table.new_read_builder()
    splits = builder.new_scan().with_global_index_result(result).plan().splits()
    return builder.new_read().to_arrow(splits)


@pytest.mark.parametrize('metric', ['l2', 'cosine', 'inner_product'])
@pytest.mark.parametrize('filtered', [False, True])
def test_single_and_batch_search_match_python(vector_table, metric, filtered):
    classic = vector_table.copy({'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    native = vector_table.copy({'read.native.enabled': 'true', 'scan.native-plan.enabled': 'true'})
    predicate = classic.new_read_builder().new_predicate_builder()
    queries = [[1, 0], [0, 1], [1, 0]]

    def configure(builder):
        builder.with_options({'ivf-flat.metric': metric})
        if filtered:
            builder.with_filter(predicate.greater_than('id', 1)).with_filter(predicate.less_than('id', 6))
            # Partition is field 2 in the table, and field 0 in the partition row.
            builder.with_partition_filter(predicate.equal('pt', 1))
        return builder

    expected = [_scores(configure(_single(classic, query)).execute_local()) for query in queries]
    from pypaimon.table.source.vector_search_read import DataEvolutionVectorRead, BatchVectorSearchReadImpl
    with patch.object(DataEvolutionVectorRead, 'read_plan', side_effect=AssertionError('Python search ran')), \
            patch.object(BatchVectorSearchReadImpl, 'read_batch_plan',
                         side_effect=AssertionError('Python batch search ran')):
        actual = [configure(_single(native, query)).execute_local() for query in queries]
        batch = configure(native.new_batch_vector_search_builder().with_vector_column('embedding')
                          .with_query_vectors(queries).with_limit(3)).execute_batch_local()
    assert len(batch) == len(queries)
    for single, batched, scores in zip(actual, batch, expected):
        _assert_scores(_scores(single), scores)
        _assert_scores(_scores(batched), scores)
    rows = _rows(native, actual[0]).to_pylist()
    if filtered:
        assert rows and all(row['pt'] == 1 for row in rows)
    else:
        assert len(rows) == 3


@pytest.mark.parametrize('empty', ['table', 'filter', 'partition'])
def test_empty_search_keeps_batch_arity(vector_table, rest_catalog, empty):
    native = vector_table.copy({'read.native.enabled': 'true'})
    if empty == 'table':
        catalog, _ = rest_catalog
        schema = vector_table.table_schema
        catalog.create_table('default.empty_vectors', Schema(
            schema.fields, schema.partition_keys, schema.primary_keys, schema.options), False)
        native = catalog.get_table('default.empty_vectors').copy({'read.native.enabled': 'true'})
    predicate = native.new_read_builder().new_predicate_builder()
    builder = (native.new_batch_vector_search_builder().with_vector_column('embedding')
               .with_query_vectors([[1, 0], [0, 1]]).with_limit(2))
    if empty == 'filter':
        builder.with_filter(predicate.equal('id', 999))
    elif empty == 'partition':
        builder.with_partition_filter(predicate.equal('pt', 999))
    from pypaimon.table.source.vector_search_read import BatchVectorSearchReadImpl
    with patch.object(BatchVectorSearchReadImpl, 'read_batch_plan', side_effect=AssertionError('Python search ran')):
        results = builder.execute_batch_local()
    assert [_scores(result) for result in results] == [{}, {}]


def test_scan_plan_is_reusable_and_snapshot_pinned(vector_table):
    table = _native_table(vector_table.copy({'read.native.enabled': 'true'}))
    builder = table.new_vector_search_builder().with_vector_column('embedding')
    scan = builder.new_vector_search_scan()
    plan = scan.scan()
    snapshot = plan.snapshot_id()
    _write(vector_table, [{'id': 7, 'embedding': [1, 0], 'pt': 0}])
    builder.with_limit(10).with_query_vector([1, 0])
    reader = builder.new_vector_search_read()
    result = reader.read_plan(plan)
    assert result.snapshot_id() == snapshot
    assert len(result) == 5
    assert reader.read_plan(plan).row_ids() == result.row_ids()
    assert len(builder.execute_local()) == 6
    filtered = table.new_vector_search_builder().with_vector_column('embedding').with_query_vector([1, 0]).with_limit(2)
    predicate = vector_table.new_read_builder().new_predicate_builder().equal('id', 1)
    filtered.with_filter(_predicate_to_native(predicate))
    with pytest.raises(Exception, match='plan.*reader|context|different'):
        filtered.new_vector_search_read().read_plan(plan)


@pytest.fixture
def pk_vector_table(rest_catalog, golden_catalog):
    catalog, server = rest_catalog
    golden = golden_catalog.get_table('default.test_pk_vector_golden')
    schema = golden.table_schema
    catalog.create_table('default.pk_vectors', Schema(
        schema.fields, schema.partition_keys, schema.primary_keys, schema.options), False)
    source = Path(golden.table_path)
    destination = Path(server.data_path) / server.warehouse / 'default' / 'pk_vectors'
    for directory in ('bucket-0', 'index', 'manifest', 'snapshot'):
        shutil.copytree(str(source / directory), str(destination / directory))
    return catalog.get_table('default.pk_vectors')


@pytest.mark.parametrize('filtered', [False, True])
@pytest.mark.parametrize('refine', [False, True])
def test_java_pk_index_results_are_physical_selections(pk_vector_table, filtered, refine):
    classic = pk_vector_table.copy({'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    native = pk_vector_table.copy({'read.native.enabled': 'true', 'scan.native-plan.enabled': 'true'})
    predicate = classic.new_read_builder().new_predicate_builder().equal('id', 3)

    def configure(table):
        builder = _single(table, [1, 0, 0, 0], limit=2)
        if filtered:
            builder.with_filter(predicate)
        if refine:
            builder.with_option('ivf.refine_factor', '2')
        return builder

    expected = configure(classic).execute_local()
    from pypaimon.table.source.primary_key_vector_read import PrimaryKeyVectorRead
    with patch.object(PrimaryKeyVectorRead, 'read_plan', side_effect=AssertionError('Python PK search ran')):
        result = configure(native).execute_local()
    assert isinstance(result, PrimaryKeyScoredResult)
    assert result.snapshot_id == expected.snapshot_id
    assert [(p.bucket, p.data_file_name, p.row_position) for p in result.positions] == [
        (p.bucket, p.data_file_name, p.row_position) for p in expected.positions]
    assert [p.score for p in result.positions] == pytest.approx([p.score for p in expected.positions])
    rows = _rows(native, result)
    assert sorted(rows.column('id').to_pylist()) == sorted(_rows(classic, expected).column('id').to_pylist())
    assert len(result.splits) == len(expected.splits)
    for split in result.splits:
        assert len(split.files) == 1
        assert not split.raw_convertible
        assert split.scores is not None


def test_native_search_failure_falls_back(vector_table):
    native = vector_table.copy({'read.native.enabled': 'true'})
    expected = _scores(_single(native.copy({'read.native.enabled': 'false'})).execute_local())
    from pypaimon.table.source import native_vector_search
    with patch.object(native_vector_search, '_native_table', side_effect=RuntimeError('unavailable')):
        _assert_scores(_scores(_single(native).execute_local()), expected)


def test_explicit_disable_uses_python(vector_table):
    table = vector_table.copy({'read.native.enabled': 'false'})
    from pypaimon.table.source import native_vector_search
    with patch.object(native_vector_search, '_native_table', side_effect=AssertionError('Native search ran')):
        assert len(_single(table).execute_local().results()) == 3


@pytest.mark.parametrize('mode', ['fast', 'full', 'detail'])
@pytest.mark.parametrize('filtered', [False, True])
def test_indexed_search_and_raw_tail_match_python(vector_table, mode, filtered):
    from pypaimon.globalindex.create_global_index import create_global_index
    from pypaimon.table.source.vector_search_read import DataEvolutionVectorRead, BatchVectorSearchReadImpl
    create_global_index(vector_table, 'embedding', 'ivf-flat', options={
        'ivf-flat.dimension': '2', 'ivf-flat.nlist': '1', 'ivf-flat.distance.metric': 'cosine'})
    _write(vector_table, [{'id': 7, 'embedding': [1, 0], 'pt': 1}])
    classic = vector_table.copy({'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
                                 'vector-index.search-mode': mode})
    native = vector_table.copy({'read.native.enabled': 'true', 'scan.native-plan.enabled': 'true',
                                'vector-index.search-mode': mode})
    predicate = classic.new_read_builder().new_predicate_builder()

    def configure(builder):
        builder.with_option('refine_factor', '2').with_option('ivf.nprobe', '1')
        if filtered:
            builder.with_filter(predicate.greater_or_equal('id', 4))
            builder.with_partition_filter(predicate.equal('pt', 1))
        return builder

    queries = [[1, 0], [0, 1]]
    expected = [_scores(configure(_single(classic, query, 3)).execute_local()) for query in queries]
    with patch.object(DataEvolutionVectorRead, 'read_plan', side_effect=AssertionError('Python search ran')), \
            patch.object(BatchVectorSearchReadImpl, 'read_batch_plan', side_effect=AssertionError('Python search ran')):
        actual = [configure(_single(native, query, 3)).execute_local() for query in queries]
        batch = configure(native.new_batch_vector_search_builder().with_vector_column('embedding')
                          .with_query_vectors(queries).with_limit(3)).execute_batch_local()
    for single, batched, scores in zip(actual, batch, expected):
        _assert_scores(_scores(single), scores)
        _assert_scores(_scores(batched), scores)


def test_fork_safety_error_does_not_fall_back(vector_table):
    from pypaimon_rust import ForkSafetyError
    from pypaimon.table.source import native_vector_search
    table = vector_table.copy({'read.native.enabled': 'true'})
    with patch.object(native_vector_search, '_native_table', side_effect=ForkSafetyError('unsafe fork')):
        with pytest.raises(ForkSafetyError, match='unsafe fork'):
            _single(table).execute_local()


@pytest.mark.parametrize('filtered', [False, True])
@pytest.mark.parametrize('mode', ['fast', 'full'])
def test_mixed_index_metrics_are_rejected(vector_table, filtered, mode):
    from pypaimon.globalindex.create_global_index import create_global_index
    for metric, identifier in [('l2', 7), ('cosine', 8)]:
        _write(vector_table, [{'id': identifier, 'embedding': [float(identifier), 0], 'pt': 1}])
        create_global_index(vector_table, 'embedding', 'ivf-flat', options={
            'ivf-flat.dimension': '2', 'ivf-flat.nlist': '1', 'ivf-flat.distance.metric': metric})
    predicate = vector_table.new_read_builder().new_predicate_builder().greater_than('id', 0)
    for native in (False, True):
        table = vector_table.copy({'read.native.enabled': str(native).lower(), 'vector-index.search-mode': mode})
        if native:
            direct = (_native_table(table).new_vector_search_builder().with_vector_column('embedding')
                      .with_query_vector([1, 0]).with_limit(3))
            if filtered:
                direct.with_filter(_predicate_to_native(predicate))
            with pytest.raises(Exception, match='Cannot merge vector indexes with different metrics'):
                direct.execute_local()
        builders = [_single(table), table.new_batch_vector_search_builder().with_vector_column('embedding')
                    .with_query_vectors([[1, 0], [0, 1]]).with_limit(3)]
        for builder in builders:
            if filtered:
                builder.with_filter(predicate)
            with pytest.raises(Exception, match='Cannot merge vector indexes with different metrics'):
                builder.execute_batch_local() if hasattr(builder, 'execute_batch_local') else builder.execute_local()


@pytest.mark.parametrize('option', ['hadoop_conf', 'prefer_io_loader', 'fallback_io_loader'])
def test_custom_io_context_keeps_python_search(vector_table, option):
    from copy import copy
    from pypaimon.table.source import native_vector_search
    table = vector_table.copy({'read.native.enabled': 'true'})
    loader = table.catalog_environment.catalog_loader
    context = copy(loader.context())
    setattr(context, option, object())
    expected = _scores(_single(table.copy({'read.native.enabled': 'false'})).execute_local())
    with patch.object(loader, 'context', return_value=context), \
            patch.object(native_vector_search, '_native_table',
                         side_effect=AssertionError('Unsupported Native context')):
        _assert_scores(_scores(_single(table).execute_local()), expected)
