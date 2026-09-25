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

from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

import pypaimon.multimodal as pm
from pypaimon.common.predicate import Predicate
from pypaimon.globalindex.data_evolution_global_index_scanner import _PaddingGlobalIndexReader
from pypaimon.globalindex.global_index_evaluator import GlobalIndexEvaluator
from pypaimon.globalindex.global_index_reader import FieldRef, _completed_future
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.offset_global_index_reader import OffsetGlobalIndexReader
from pypaimon.globalindex.union_global_index_reader import UnionGlobalIndexReader
from pypaimon.table.source.vector_search_read import AbstractVectorSearchReadImpl, DataEvolutionVectorRead
from pypaimon.tests.vector_search_filter_test import _StubTable, _field
from pypaimon.tests.global_index_evaluator_test import StubGlobalIndexReader, _make_fields
from pypaimon.utils.range import Range


@pytest.fixture
def table(tmp_path):
    pytest.importorskip("paimon_vindex")
    schema = pa.schema([("id", pa.int64()), ("name", pa.string()), ("embedding", pa.list_(pa.float32(), 2))])
    table = pm.connect(options={"warehouse": str(tmp_path)}).create_table(
        "vectors", schema=schema, options={"file.format": "parquet", "vector.file.format": "parquet",
                                           "read.batch-size": "1"})
    table.add(pa.table({"id": [0, 1, 2], "name": ["alpha", "beta zeta", "gamma"],
                        "embedding": [[0., 1.], [1., 1.], [2., 1.]]}, schema=schema))
    table.raw_table.create_global_index(
        "embedding", "ivf-flat", options={"ivf-flat.nlist": "1", "ivf-flat.distance.metric": "l2"})
    return table


def scalar_index(table, kind="btree"):
    table.raw_table.copy({"deletion-vectors.enabled": "false"}).create_global_index("name", kind)


def query(table, predicate, batch=False):
    search = table.search_vectors([[0., 1.], [0., 1.]], pre_filter=predicate) if batch else table.search(
        [0., 1.], pre_filter=predicate)
    return search.select(["id"]).limit(1)


@pytest.mark.parametrize("pattern", ["%zeta%", "beta%"])
@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("mode", ["full", "fast"])
@pytest.mark.parametrize("refine", [False, True])
def test_btree_candidates_are_verified_before_top_k(table, caplog, pattern, batch, mode, refine):
    scalar_index(table)
    table.raw_table = table.raw_table.copy({
        "vector-index.search-mode": mode, "global-index.filter.refine-from-data": str(refine).lower()})
    result = query(table, "name LIKE '%s'" % pattern, batch).to_list()
    expected = [{"id": 1}] if refine else []
    assert result == ([expected, expected] if batch else expected)
    assert ("global-index.filter.refine-from-data=true" in caplog.text) == (not refine)


@pytest.mark.parametrize("kind, predicate", [
    ("btree", "name = 'beta zeta'"), ("btree", "name >= 'beta' AND name < 'gamma'"),
    ("btree", "name LIKE 'beta zeta'"), ("bitmap", "name LIKE '%zeta%'"),
])
def test_exact_indexes_do_not_read_filter_columns(table, kind, predicate):
    scalar_index(table, kind)
    table.raw_table = table.raw_table.copy({"global-index.filter.refine-from-data": "true"})
    with patch.object(AbstractVectorSearchReadImpl, "_matching_candidate_rows",
                      side_effect=AssertionError("exact index recheck")):
        assert query(table, predicate).to_list() == [{"id": 1}]


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("mode", ["full", "fast"])
@pytest.mark.parametrize("refine", [False, True])
def test_mixed_btree_and_bitmap_preserve_exact_matches(table, batch, mode, refine):
    scalar_index(table, "btree")
    scalar_index(table, "bitmap")
    table.raw_table = table.raw_table.copy({
        "vector-index.search-mode": mode, "global-index.filter.refine-from-data": str(refine).lower()})
    with patch.object(AbstractVectorSearchReadImpl, "_matching_candidate_rows",
                      side_effect=AssertionError("exact index recheck")):
        result = query(table, "name LIKE '%zeta%'", batch).to_list()
    expected = [{"id": 1}]
    assert result == ([expected, expected] if batch else expected)


@pytest.mark.parametrize("method", ["leaf", "and"])
@pytest.mark.parametrize("first_exact, second_exact", [(False, False), (False, True), (True, False), (True, True)])
def test_reader_intersection_and_predicate_conjunction_exactness(method, first_exact, second_exact):
    results = [GlobalIndexResult.create(
        GlobalIndexResult.from_range(Range(1, 2) if exact else Range(0, 3)).results(), is_exact=exact)
        for exact in (first_exact, second_exact)]
    readers = [StubGlobalIndexReader(result) for result in results]
    leaves = [Predicate(method="equal", index=i, field=field, literals=[1]) for i, field in enumerate(("a", "b"))]
    predicate = leaves[0] if method == "leaf" else Predicate(
        method="and", index=None, field=None, literals=leaves)
    with GlobalIndexEvaluator(_make_fields(), lambda field: readers + [StubGlobalIndexReader(None)]
                              if method == "leaf" else [readers[field.id]]) as evaluator:
        result = evaluator.evaluate(predicate)
    assert list(result.results()) == ([1, 2] if first_exact or second_exact else [0, 1, 2, 3])
    assert result.is_exact() == ((first_exact or second_exact) if method == "leaf" else (
        first_exact and second_exact))


@pytest.mark.parametrize("predicate", ["name LIKE '%zeta%'", "name >= 'a' AND name LIKE '%zeta%'"])
def test_unsupported_leaf_and_same_field_conjunction_can_be_refined(table, predicate):
    scalar_index(table)
    table.raw_table = table.raw_table.copy({"global-index.filter.refine-from-data": "true",
                                            "btree-index.fallback-scan-max-size": "0 b"})
    assert query(table, predicate).to_list() == [{"id": 1}]


def test_commit_before_candidate_verification_keeps_snapshot(table):
    scalar_index(table)
    table.raw_table = table.raw_table.copy({"global-index.filter.refine-from-data": "true",
                                            "global-index.column-update-action": "DROP_PARTITION_INDEX"})
    search = query(table, "name LIKE '%zeta%'")
    original = AbstractVectorSearchReadImpl._matching_candidate_rows

    def update_before_read(reader, candidates, snapshot):
        table.update("id = 1", {"name": "changed"})
        return original(reader, candidates, snapshot)

    with patch.object(AbstractVectorSearchReadImpl, "_matching_candidate_rows", update_before_read):
        assert search.to_list() == [{"id": 1}]
    assert search.to_list() == []


@pytest.mark.parametrize("method", ["and", "or"])
def test_exactness_follows_predicate_tree_and_reader_wrappers(method):
    candidate = GlobalIndexResult.create(GlobalIndexResult.from_range(Range(0, 3)).results(), is_exact=False)
    exact = GlobalIndexResult.from_range(Range(1, 2))
    assert not candidate.offset(10).is_exact()
    assert not candidate.and_(exact).is_exact()
    assert not candidate.or_(exact).is_exact()
    assert candidate.and_(GlobalIndexResult.create_empty()).is_exact()
    reader = UnionGlobalIndexReader([OffsetGlobalIndexReader(StubGlobalIndexReader(candidate), 10, 13)])
    assert not reader.visit_equal(FieldRef(0, "a", "INT"), 1).result().is_exact()
    padded = _PaddingGlobalIndexReader(StubGlobalIndexReader(exact), GlobalIndexResult.from_range(Range(5, 6)))
    assert not padded.visit_equal(FieldRef(0, "a", "INT"), 1).result().is_exact()

    class PartialReader(StubGlobalIndexReader):
        def visit_greater_than(self, field_ref, literal):
            return _completed_future(None)

    evaluator = GlobalIndexEvaluator(_make_fields(), lambda field: [PartialReader(exact)])
    predicate = Predicate(method=method, index=None, field=None, literals=[
        Predicate(method="equal", index=0, field="a", literals=[1]),
        Predicate(method="greaterThan", index=0, field="a", literals=[0]),
    ])
    try:
        result = evaluator.evaluate(predicate)
        if method == "or":
            assert result is None
        else:
            assert list(result.results()) == [1, 2]
            assert not result.is_exact()
    finally:
        evaluator.close()


@pytest.mark.parametrize("selector", ["snapshot", "tag"])
def test_candidate_verification_preserves_historical_deletions(table, selector):
    scalar_index(table)
    table.raw_table = table.raw_table.copy({"global-index.filter.refine-from-data": "true"})
    saved = table.raw_table.snapshot_manager().get_latest_snapshot()
    table.raw_table.create_tag("saved", snapshot_id=saved.id)
    table.delete("id = 1")
    options = {"snapshot_id": saved.id} if selector == "snapshot" else {"tag_name": "saved"}
    search = table.search([0., 1.], pre_filter="name LIKE '%zeta%'", **options).select(["id"]).limit(1)
    assert search.to_list() == [{"id": 1}]
    assert query(table, "name LIKE '%zeta%'").to_list() == []


@pytest.mark.parametrize("fail_read", [False, True])
def test_candidate_verification_projects_only_row_ids_and_closes_stream(fail_read):
    field = _field(1, "embedding", "FLOAT")
    table = _StubTable([field], [])
    read = DataEvolutionVectorRead(table, 1, field, [0.], filter_="filter")
    builder = Mock()
    builder.with_filter.return_value = builder
    builder.with_projection.return_value = builder
    table.new_read_builder = Mock(return_value=builder)
    schema = pa.schema([("_ROW_ID", pa.int64())])
    closed = []

    def batches():
        try:
            yield pa.record_batch([[1]], schema=schema)
            if fail_read:
                raise ValueError("filter read failed")
        finally:
            closed.append(True)

    source = batches()
    resource = Mock()
    resource.read_next_batch.side_effect = lambda: next(source)
    builder.new_read.return_value._new_arrow_batch_reader.return_value = resource, source
    candidates = GlobalIndexResult.from_range(Range(0, 2)).results()
    if fail_read:
        with pytest.raises(ValueError, match="filter read failed"):
            read._matching_candidate_rows(candidates, None)
    else:
        assert list(read._matching_candidate_rows(candidates, None)) == [1]
    builder.with_filter.assert_called_once_with("filter")
    builder.with_projection.assert_called_once_with(["_ROW_ID"])
    resource.close.assert_called_once_with()
    assert closed == [True]
