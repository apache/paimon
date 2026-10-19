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

"""Externally computed PK selections keep their snapshot and physical addresses."""

import copy
import pickle
from unittest.mock import patch

import pytest

from pypaimon import Schema
from pypaimon.read.scanner.file_scanner import FileScanner
from pypaimon.read.native_plan import _partition_fields
from pypaimon.read.split_serializer import deserialize_split_v1, serialize_split_v1
from pypaimon.table.source.global_index_split_result import GlobalIndexSplitResult
from pypaimon.tests import native_hybrid_search_test as hybrid
from pypaimon.tests import native_pk_full_text_search_test as text
from pypaimon.tests import native_query_auth_test as auth
from pypaimon.tests import native_vector_search_test as vectors

rest_catalog = text.rest_catalog
golden_catalog = text.golden_catalog
source = text.source
guarded_table = auth.guarded_table
pk_vector_table = vectors.pk_vector_table
vector_table = vectors.vector_table
pytestmark = pytest.mark.native_plan


class _SplitResult(GlobalIndexSplitResult):
    def __init__(self, plan):
        self._plan = plan

    @property
    def snapshot_id(self):
        return self._plan.snapshot_id

    @property
    def splits(self):
        return self._plan.splits()


@pytest.mark.parametrize('native_read', ['true', 'false'])
@pytest.mark.parametrize('snapshot', [1, 5, 8])
@pytest.mark.parametrize('query', ['Paimon', 'no match'])
def test_pk_supplied_results_use_native_scan_and_preserve_metadata(source, native_read, snapshot, query):
    table, _ = source
    captured = table.copy({'scan.snapshot-id': str(snapshot), 'read.native.enabled': 'false'})
    result = text._builder(captured, query).execute_local()
    latest = table.copy({'scan.native-plan.enabled': 'true', 'read.native.enabled': native_read})
    builder = latest.new_read_builder().with_projection(['id'])
    with patch.object(FileScanner, 'scan', side_effect=AssertionError('Python scan ran')):
        plan = builder.new_scan().with_global_index_result(result).plan()
    assert plan.snapshot_id == result.snapshot_id
    assert [serialize_split_v1(s) for s in plan.splits()] == [serialize_split_v1(s) for s in result.splits]
    expected = captured.new_read_builder().with_projection(['id']).new_read().to_arrow(list(result.splits))
    actual = builder.new_read().to_arrow(plan.splits())
    assert actual.to_pylist() == expected.to_pylist()


@pytest.mark.parametrize('hint', ['filter', 'limit', 'shard'])
def test_supplied_pk_splits_precede_ordinary_planning_hints(source, hint):
    table, _ = source
    result = text._builder(table.copy({'read.native.enabled': 'false'})).execute_local()
    builder = table.copy({'scan.native-plan.enabled': 'true'}).new_read_builder()
    if hint == 'filter':
        builder.with_filter(builder.new_predicate_builder().greater_than('id', 100))
    elif hint == 'limit':
        builder.with_limit(0)
    scan = builder.new_scan().with_global_index_result(result)
    if hint == 'shard':
        scan.with_shard(1, 2)
    with patch.object(FileScanner, 'scan', side_effect=AssertionError('Python scan ran')):
        plan = scan.plan()
    assert plan.snapshot_id == result.snapshot_id
    assert [serialize_split_v1(s) for s in plan.splits()] == [serialize_split_v1(s) for s in result.splits]
    if hint in ('filter', 'limit'):
        assert builder.new_read().to_arrow(plan.splits()).num_rows == 0


def test_core_search_result_passes_directly_to_scan_and_scored_splits_round_trip(source):
    from pypaimon_rust.datafusion import GlobalIndexResult, Split
    table, _ = source
    native = text._native_table(table)
    result = text._native(table).execute_local()
    assert isinstance(result, GlobalIndexResult)
    scan = native.new_read_builder().with_projection(['id']).new_scan().with_global_index_result(result)
    for plan in [scan.plan(), scan.plan()]:
        assert plan.snapshot_id() == result.snapshot_id()
        assert [s.serialize() for s in plan.splits()] == result.splits()
    splits = [pickle.loads(pickle.dumps(Split.deserialize(s))) for s in result.splits()]
    carried = GlobalIndexResult.from_splits(result.snapshot_id(), iter(splits))
    plan = native.new_read_builder().new_scan().with_global_index_result(carried).plan()
    assert [s.serialize() for s in plan.splits()] == result.splits()
    reader = native.new_read_builder().with_projection(['id']).new_read()
    assert sum(batch.num_rows for batch in reader.read(plan.splits())) == 2


@pytest.mark.parametrize('family', ['full-text', 'vector'])
def test_native_search_result_hands_core_selection_directly_to_scan(source, pk_vector_table, family):
    table = source[0] if family == 'full-text' else pk_vector_table
    table = table.copy({'read.native.enabled': 'true', 'scan.native-plan.enabled': 'true'})
    search = text._builder(table) if family == 'full-text' else vectors._single(table, [1, 0, 0, 0], 2)
    result = search.execute_local()
    from pypaimon_rust.datafusion import GlobalIndexResult
    assert isinstance(result._native_result, GlobalIndexResult)
    expected = sorted(position.row_position for position in result.positions)
    with patch('pypaimon.read.native_plan.serialize_split_v1',
               side_effect=AssertionError('Native result round-tripped through Python')), \
            patch.object(FileScanner, 'scan', side_effect=AssertionError('Python scan ran')):
        plan = table.new_read_builder().new_scan().with_global_index_result(result).plan()
    assert plan.snapshot_id == result.snapshot_id
    assert sorted(position for split in plan.splits() for r in split.row_ranges()
                  for position in range(r.from_, r.to + 1)) == expected


def test_exposed_python_splits_and_pickled_results_use_current_metadata(source):
    table, _ = source
    table = table.copy({'read.native.enabled': 'true', 'scan.native-plan.enabled': 'true'})
    result = text._builder(table).execute_local()
    transported = pickle.loads(pickle.dumps(result))
    builder = table.new_read_builder().with_projection(['id'])
    plan = builder.new_scan().with_global_index_result(transported).plan()
    assert sorted(builder.new_read().to_arrow(plan.splits()).column('id').to_pylist()) == [1, 3]
    # Accessing splits exposes mutable lists. Subsequent scans must respect
    # the edited selections, including a deliberately empty one.
    for split in result.splits:
        split.row_ranges().clear()
        split.scores().clear()
    with patch.object(FileScanner, 'scan', side_effect=AssertionError('Python scan ran')):
        plan = builder.new_scan().with_global_index_result(result).plan()
    assert plan.snapshot_id == result.snapshot_id
    assert all(split.row_count == 0 and split.scores() == [] for split in plan.splits())
    assert builder.new_read().to_arrow(plan.splits()).num_rows == 0


def test_empty_native_pk_search_preserves_java_snapshot_zero(source, rest_catalog):
    catalog, _ = rest_catalog
    schema = source[0].table_schema
    catalog.create_table('default.empty_text', Schema(
        schema.fields, schema.partition_keys, schema.primary_keys, schema.options), False)
    table = catalog.get_table('default.empty_text').copy({
        'read.native.enabled': 'true', 'scan.native-plan.enabled': 'true'})
    result = text._builder(table).execute_local()
    assert result.snapshot_id == 0
    with patch.object(FileScanner, 'scan', side_effect=AssertionError('Python scan ran')):
        plan = table.new_read_builder().new_scan().with_global_index_result(result).plan()
    assert plan.snapshot_id == 0
    assert plan.splits() == []


def test_shallow_copy_split_edits_are_observed_when_scanning_the_original(source):
    table = source[0].copy({'read.native.enabled': 'true', 'scan.native-plan.enabled': 'true'})
    result = text._builder(table).execute_local()
    alias = copy.copy(result)
    for split in alias.splits:
        split.row_ranges().clear()
        split.scores().clear()
    with patch.object(FileScanner, 'scan', side_effect=AssertionError('Python scan ran')):
        plan = table.new_read_builder().new_scan().with_global_index_result(result).plan()
    assert plan.snapshot_id == result.snapshot_id
    assert all(split.row_count == 0 for split in plan.splits())


@pytest.mark.parametrize('native_read', ['true', 'false'])
def test_pk_vector_results_use_the_same_native_scan(pk_vector_table, native_read):
    source = pk_vector_table.copy({'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
                                   'vector-index.search-mode': 'fast'})
    result = vectors._single(source, [1, 0, 0, 0]).execute_local()
    expected = vectors._rows(source, result).column('id').to_pylist()
    table = pk_vector_table.copy({'read.native.enabled': native_read, 'scan.native-plan.enabled': 'true'})
    builder = table.new_read_builder().with_projection(['id'])
    with patch.object(FileScanner, 'scan', side_effect=AssertionError('Python scan ran')):
        plan = builder.new_scan().with_global_index_result(result).plan()
    assert [serialize_split_v1(s) for s in plan.splits()] == [serialize_split_v1(s) for s in result.splits]
    assert sorted(builder.new_read().to_arrow(plan.splits()).column('id').to_pylist()) == sorted(expected)


@pytest.mark.parametrize('native_read', ['true', 'false'])
def test_nonindexed_pk_splits_are_preserved_too(source, native_read):
    table, _ = source
    original = table.copy({'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false'})
    original_builder = original.new_read_builder()
    result = _SplitResult(original_builder.new_scan().plan())
    expected = original_builder.new_read().to_arrow(result.splits).to_pylist()
    builder = table.copy({'scan.native-plan.enabled': 'true', 'read.native.enabled': native_read}).new_read_builder()
    with patch.object(FileScanner, 'scan', side_effect=AssertionError('Python scan ran')):
        plan = builder.new_scan().with_global_index_result(result).plan()
    assert [serialize_split_v1(s) for s in plan.splits()] == [serialize_split_v1(s) for s in result.splits]
    assert builder.new_read().to_arrow(plan.splits()).to_pylist() == expected


@pytest.mark.parametrize('selection', ['all', 'slice', 'shard', 'empty'])
def test_de_scored_results_align_scores_in_core(vector_table, selection):
    from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
    table = vector_table.copy({'scan.native-plan.enabled': 'true', 'read.native.enabled': 'true'})
    result = DictBasedScoredIndexResult({4: 0.2, 0: 0.9, 3: 0.7} if selection != 'empty' else {})
    scan = table.new_read_builder().new_scan().with_global_index_result(result)
    if selection == 'slice':
        scan.with_slice(3, 5)
    elif selection == 'shard':
        scan.with_shard(1, 2)
    with patch.object(FileScanner, 'scan', side_effect=AssertionError('Python scan ran')):
        plan = scan.plan()
    expected = {0: 0.9, 3: 0.7, 4: 0.2}
    if selection in ('slice', 'shard'):
        expected.pop(0)
    elif selection == 'empty':
        expected = {}
    actual = {}
    for split in plan.splits():
        for range_ in split.row_ranges():
            for row_id in range(range_.from_, range_.to + 1):
                actual[row_id] = split.get_score(row_id)
    assert actual == pytest.approx(expected)


def test_de_supplied_result_preserves_explicit_incremental_window(vector_table):
    from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
    from pypaimon.read.native_plan import native_plan
    table = vector_table.copy({'read.native.enabled': 'true'})
    start = table.snapshot_manager().get_latest_snapshot().id
    vectors._write(table, [{'id': 7, 'embedding': [1, 0], 'pt': 1}])
    end = table.snapshot_manager().get_latest_snapshot().id
    plan = native_plan(table, incremental_range=(start, end),
                       global_index_result=DictBasedScoredIndexResult({0: 0.1, 6: 0.9}))
    assert plan.snapshot_id == end
    assert len(plan.splits()) == 1
    assert plan.splits()[0].scores() == pytest.approx([0.9])
    assert vectors._rows(table, DictBasedScoredIndexResult({6: 0.9})).column('id').to_pylist() == [7]
    builder = table.new_read_builder().with_projection(['id'])
    assert builder.new_read().to_arrow(plan.splits()).to_pylist() == [{'id': 7}]


@pytest.mark.parametrize('guarded_table', [
    ('primary-key', {'bucket': '1'}),
    ('data-evolution', {'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'}),
], indirect=True)
@pytest.mark.parametrize('native', [False, True])
def test_authorized_scans_ignore_external_raw_index_selections(guarded_table, native):
    from pypaimon.api.api_response import AuthTableQueryResponse
    from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
    from pypaimon.read.plan import Plan
    table, server = guarded_table
    server.table_query_auth_store[table.identifier.get_full_name()] = AuthTableQueryResponse(
        filter=[auth._filter('id', 'GREATER_THAN', [1])])
    result = _SplitResult(Plan([], 1)) if table.is_primary_key_table else DictBasedScoredIndexResult({0: 0.9})
    table = table.copy({'scan.native-plan.enabled': str(native).lower(), 'read.native.enabled': str(native).lower()})
    builder = table.new_read_builder().with_projection(['id'])
    scan = builder.new_scan().with_global_index_result(result)
    if native:
        with patch.object(FileScanner, 'scan', side_effect=AssertionError('Python scan ran')):
            plan = scan.plan()
    else:
        plan = scan.plan()
    assert sorted(builder.new_read().to_arrow(plan.splits()).column('id').to_pylist()) == [2, 3, 4]


@pytest.mark.parametrize('family', ['vector', 'batch-vector', 'full-text', 'hybrid'])
def test_all_core_searches_return_one_result_for_read_and_scan(rest_catalog, family):
    import pyarrow as pa
    from pypaimon_rust.datafusion import GlobalIndexResult
    table = hybrid._table(rest_catalog[0])
    native = text._native_table(table)
    if family == 'vector':
        results = [(native.new_vector_search_builder().with_vector_column('embedding')
                    .with_query_vector([0., 0.]).with_limit(3).execute_local())]
    elif family == 'batch-vector':
        results = (native.new_batch_vector_search_builder().with_vector_column('embedding')
                   .with_query_vectors([[0., 0.], [7., 0.]]).with_limit(3).execute_batch_local())
        assert len(results) == 2
    elif family == 'full-text':
        results = [text._native(table, 'paimon', 3).execute_local()]
    else:
        results = [(native.new_hybrid_search_builder().with_limit(3)
                    .add_vector_route('embedding', [0., 0.], 3, 1., {})
                    .add_full_text_route('content', hybrid.match_query('paimon'), 3, 1., {})
                    .execute_local())]
    for result in results:
        assert isinstance(result, GlobalIndexResult)
        assert result.snapshot_id() == table.snapshot_manager().get_latest_snapshot().id
        expected = result.row_ids()
        ranked = pa.Table.from_batches(result.new_read_builder().with_projection(['id']).read())
        assert ranked.column_names == ['id', '__paimon_search_score']
        assert sorted(ranked.column('__paimon_search_score').to_pylist()) == pytest.approx(sorted(expected.values()))
        builder = native.new_read_builder().with_projection(['id'])
        plan = builder.new_scan().with_global_index_result(result).plan()
        rows = pa.Table.from_batches(builder.new_read().read(plan.splits()))
        assert sorted(rows.column('id').to_pylist()) == sorted(ranked.column('id').to_pylist())
        carried_scores = {}
        for selected in plan.splits():
            split = deserialize_split_v1(selected.serialize(), _partition_fields(table), [])
            for range_ in split.row_ranges():
                for row_id in range(range_.from_, range_.to + 1):
                    carried_scores[row_id] = split.get_score(row_id)
        assert carried_scores == pytest.approx(expected)
