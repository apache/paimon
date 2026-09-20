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

from unittest.mock import patch

import pyarrow as pa
import pytest

import pypaimon.multimodal as pm
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.multimodal.query import ScanQuery
from pypaimon.read.table_read import TableRead


@pytest.fixture(params=[1, 3])
def docs(tmp_path, request):
    schema = pa.schema([
        ("id", pa.int64()), ("category", pa.string()), ("embedding", pa.list_(pa.float32(), 2)),
        ("info", pa.struct([("label", pa.string()), ("value", pa.int32())])),
        ("attrs", pa.map_(pa.string(), pa.string())), ("payload", pa.large_binary()),
    ])
    table = pm.connect(options={"warehouse": str(tmp_path)}).create_table(
        "docs", schema=schema, partitioned=["category"], options={
            "file.format": "parquet", "vector.file.format": "parquet",
            "vector-index.search-mode": "full", "read.parallelism": str(request.param),
            "read.batch-size": "2", "blob-as-descriptor": "false",
        })
    for start in (0, 4, 8):
        table.add(pa.Table.from_pylist([{
            "id": i, "category": "a" if i % 2 else "b", "embedding": [float(i), 0.0],
            "info": {"label": str(i), "value": i}, "attrs": {"x": str(i)},
            "payload": ("payload-%d" % i).encode(),
        } for i in range(start, start + 4)], schema=schema))
    return table


def configure(query, projection, with_row_id=False, post_filter=None):
    if projection is not None:
        query.select(projection)
    if with_row_id:
        query.with_row_id()
    if post_filter is not None:
        query.where(post_filter)
    return query.limit(3)


@pytest.mark.parametrize("projection", [
    None, [], ["id"], ["payload"], ["_ROW_ID", "id"], ["info", "attrs['x']"], ["missing"]])
@pytest.mark.parametrize("with_row_id", [False, True])
def test_batch_lookup_matches_individual_queries(docs, projection, with_row_id):
    vectors = [[1.0, 0.0], [9.0, 0.0], [1.0, 0.0]]
    expected = [configure(docs.search(vector), projection, with_row_id).to_arrow() for vector in vectors]
    query = configure(docs.search_vectors(vectors), projection, with_row_id)
    original = TableRead.to_arrow
    reads = []

    def read(table_read, splits, *args, **kwargs):
        reads.append(len(splits))
        return original(table_read, splits, *args, **kwargs)

    with patch.object(TableRead, "to_arrow", read):
        actual = query.to_arrow()
    assert len(actual) == len(expected)
    for left, right in zip(expected, actual):
        assert left.schema == right.schema
        assert left.to_pylist() == right.to_pylist()
    # Preserve the existing zero-column behavior of invalid projections.
    assert len(reads) == (3 if projection == ["missing"] and not with_row_id else 1)


def test_shared_lookup_keeps_pre_and_post_filters_and_per_query_limits(docs):
    vectors = [[1.0, 0.0], [9.0, 0.0], [1.0, 0.0]]
    options = {"pre_filter": "category = 'b'"}
    expected = [configure(docs.search(vector, **options), ["id"], post_filter="id >= 5").to_arrow()
                for vector in vectors]
    actual = configure(docs.search_vectors(vectors, **options), ["id"], post_filter="id >= 5").to_arrow()
    assert [result.to_pylist() for result in actual] == [result.to_pylist() for result in expected]
    assert actual[0].num_rows == 0
    assert actual[1].num_rows == 3


def test_empty_membership_missing_rows_and_scores_are_independent(docs):
    row_ids = [row["_ROW_ID"] for row in docs.scan().select(["id"]).with_row_id().to_list()]
    results = [DictBasedScoredIndexResult(values) for values in (
        {row_ids[0]: 0.9, row_ids[1]: 0.7}, {}, {row_ids[0]: 0.1}, {10000: 1.0})]
    query = docs.search_vectors([[0.0, 0.0]] * len(results)).select(["id"]).limit(3)._for_execution()
    expected = [query._read_global_index_result(result) for result in results]
    actual = query._read_batch_results(results)
    assert [table.to_pylist() for table in actual] == [table.to_pylist() for table in expected]
    assert all(table.schema == expected[0].schema for table in actual)
    assert results[0].score_getter()(row_ids[0]) == 0.9
    assert results[2].score_getter()(row_ids[0]) == 0.1


def test_all_empty_results_preserve_projection_schema(docs):
    query = docs.search_vectors([[0.0, 0.0], [11.0, 0.0]], pre_filter="category = 'missing'")
    actual = query.select(["id", "payload"]).with_row_id().limit(2).to_arrow()
    expected = docs.search([0.0, 0.0], pre_filter="category = 'missing'").select(
        ["id", "payload"]).with_row_id().limit(2).to_arrow()
    assert len(actual) == 2
    assert all(table.equals(expected) for table in actual)


@pytest.mark.parametrize("change", ["update", "delete"])
def test_shared_lookup_uses_search_snapshot_and_query_can_be_reused(docs, change):
    query = docs.search_vectors([[0.0, 0.0], [11.0, 0.0]]).select(["id", "info"]).limit(1)
    expected = query.to_list()
    lookup = ScanQuery._read_global_index_result
    calls = []

    def commit_before_lookup(execution, result):
        calls.append(execution._table)
        if change == "update":
            docs.update("id = 0", {"info": {"label": "updated", "value": 99}})
        else:
            docs.delete("id = 0")
        return lookup(execution, result)

    with patch.object(ScanQuery, "_read_global_index_result", commit_before_lookup):
        assert query.to_list() == expected
    assert len(calls) == 1
    assert query.to_list() != expected


@pytest.mark.parametrize("selector", ["snapshot", "tag"])
def test_shared_lookup_keeps_historical_views(docs, selector):
    snapshot = docs.raw_table.snapshot_manager().get_latest_snapshot()
    docs.raw_table.create_tag("before", snapshot_id=snapshot.id)
    options = {"snapshot_id": snapshot.id} if selector == "snapshot" else {"tag_name": "before"}
    query = docs.search_vectors([[0.0, 0.0], [11.0, 0.0]], **options).select(["id", "info"]).limit(1)
    expected = query.to_list()
    docs.update("id = 0", {"info": {"label": "new", "value": 99}})
    if selector == "tag":
        docs.raw_table.file_io.delete(docs.raw_table.snapshot_manager().get_snapshot_path(snapshot.id))
    assert query.to_list() == expected


def test_single_query_keeps_single_lookup(docs):
    expected = docs.search([1.0, 0.0]).select(["id"]).limit(3).to_arrow()
    result = docs.search_vectors([[1.0, 0.0]]).select(["id"]).limit(3).to_arrow()
    assert result[0].equals(expected)


def test_unsupported_nested_row_projection_still_raises(docs):
    with pytest.raises(NotImplementedError, match="ROW nested-field projection"):
        docs.search_vectors([[0.0, 0.0], [1.0, 0.0]]).select(["info.label"]).to_arrow()


def test_native_index_batch_uses_shared_lookup(docs):
    pytest.importorskip("paimon_vindex")
    docs.raw_table.copy({"deletion-vectors.enabled": "false"}).create_global_index(
        "embedding", index_type="ivf-flat", options={
            "ivf-flat.dimension": "2", "ivf-flat.nlist": "1", "ivf-flat.distance.metric": "l2"})
    vectors = [[1.0, 0.0], [10.0, 0.0], [1.0, 0.0]]
    expected = [docs.search(vector).select(["id"]).limit(3).to_list() for vector in vectors]
    query = docs.search_vectors(vectors).select(["id"]).limit(3)
    lookup = ScanQuery._read_global_index_result
    calls = []

    def read(execution, result):
        calls.append(result.results().cardinality())
        return lookup(execution, result)

    with patch.object(ScanQuery, "_read_global_index_result", read):
        actual = query.to_list()
    assert actual == expected
    assert calls == [6]
