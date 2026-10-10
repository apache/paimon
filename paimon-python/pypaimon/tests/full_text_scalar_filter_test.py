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
from pypaimon.table.source.full_text_read import DataEvolutionFullTextRead
from pypaimon.table.source.global_index_row_filter import matching_rows


@pytest.fixture
def docs(tmp_path):
    pytest.importorskip("paimon_ftindex")
    schema = pa.schema([("id", pa.int64()), ("text", pa.string()), ("label", pa.string()),
                        ("pt", pa.string()), ("embedding", pa.list_(pa.float32(), 2))])
    table = pm.connect(options={"warehouse": str(tmp_path)}).create_table(
        "docs", schema=schema, partitioned=["pt"], options={
            "file.format": "parquet", "vector.file.format": "parquet", "read.batch-size": "1",
            "full-text-index.search-mode": "full", "vector-index.search-mode": "full"})
    table.add(pa.table({"id": [0, 1, 2], "text": ["paimon", "paimon paimon", "paimon long document"],
                        "label": ["other", "other", "target"], "pt": ["a"] * 3,
                        "embedding": [[0., 1.], [1., 1.], [2., 1.]]}, schema=schema))
    table.raw_table.copy({"deletion-vectors.enabled": "false"}).create_global_index("text", "full-text")
    return table


def append_rows(docs, row_ids=(3, 4)):
    rows = [{"id": 3, "text": "paimon", "label": "other", "pt": "b", "embedding": [3., 1.]},
            {"id": 4, "text": "paimon another long document", "label": "target", "pt": "b", "embedding": [4., 1.]}]
    batch = pa.Table.from_pylist(
        [row for row in rows if row["id"] in row_ids], schema=docs.scan().to_arrow().schema)
    docs.add(batch)


def builder(docs, predicate=None, limit=10):
    result = docs.raw_table.new_full_text_search_builder().with_query(
        "text", '{"match":{"query":"paimon"}}').with_limit(limit)
    if predicate is not None:
        from pypaimon.common.where_parser import parse_where_clause
        result.with_filter(parse_where_clause(predicate, docs.raw_table.fields))
    return result


def scores(result):
    return {row_id: result.score_getter()(row_id) for row_id in result.results()}


@pytest.mark.parametrize("kind", [None, "btree", "bitmap"])
@pytest.mark.parametrize("raw", [False, True])
@pytest.mark.parametrize("refine", [False, True])
def test_full_text_data_filters_precede_top_k_and_preserve_scores(docs, kind, raw, refine):
    docs.raw_table = docs.raw_table.copy({"global-index.filter.refine-from-data": str(refine).lower()})
    if kind:
        docs.raw_table.copy({"deletion-vectors.enabled": "false"}).create_global_index("label", kind)
    if raw:
        append_rows(docs)
    expected = scores(builder(docs).execute_local())
    actual = scores(builder(docs, "label LIKE '%target%'", limit=1).execute_local())
    # BTree LIKE supplies candidates requiring refinement. Exact bitmap
    # matches and raw rows do not depend on the refinement option.
    candidates = [] if kind == "btree" and not refine else [2]
    if raw:
        candidates.append(4)
    best = max(candidates, key=lambda row_id: (expected[row_id], -row_id)) if candidates else None
    assert actual == ({best: expected[best]} if best is not None else {})
    public = docs.search("paimon", column="text", pre_filter="label LIKE '%target%'").select(["id"]).limit(1)
    assert public.to_list() == ([{"id": best}] if best is not None else [])


@pytest.mark.parametrize("mode, expected", [("fast", [2]), ("full", [2, 4]), ("detail", [2, 4])])
def test_partial_scalar_coverage_follows_search_mode(docs, mode, expected):
    docs.raw_table.copy({"deletion-vectors.enabled": "false"}).create_global_index("label", "btree")
    append_rows(docs)
    docs.raw_table.copy({"deletion-vectors.enabled": "false"}).create_global_index("text", "full-text")
    docs.raw_table = docs.raw_table.copy({"scalar-index.search-mode": mode})
    actual = docs.search("paimon", column="text", pre_filter="label = 'target'").select(["id"]).limit(10).to_list()
    assert sorted(row["id"] for row in actual) == expected


@pytest.mark.parametrize("predicate, expected", [
    ("pt = 'b' AND label = 'target'", [4]),
    ("pt = 'a' OR id = 4", [0, 1, 2, 4]),
    ("id = 2", [2]),
])
def test_partition_and_data_predicates_keep_boolean_semantics(docs, predicate, expected):
    append_rows(docs, row_ids=[3])
    docs.raw_table.copy({"deletion-vectors.enabled": "false"}).create_global_index("text", "full-text")
    append_rows(docs, row_ids=[4])
    actual = docs.search("paimon", column="text", pre_filter=predicate).select(["id"]).limit(10).to_list()
    assert sorted(row["id"] for row in actual) == expected


def test_partition_only_filter_keeps_existing_path(docs):
    with patch("pypaimon.table.source.global_index_row_filter.matching_rows",
               side_effect=AssertionError("partition-only data read")):
        assert len(docs.search("paimon", column="text", pre_filter="pt = 'a'").limit(10).to_list()) == 3


@pytest.mark.parametrize("mode", ["fast", "full", "detail"])
def test_partition_without_full_text_definition_is_empty(docs, mode):
    append_rows(docs)
    docs.raw_table = docs.raw_table.copy({"full-text-index.search-mode": mode})
    search = docs.search("paimon", column="text", pre_filter="pt = 'b' AND label = 'target'")
    assert search.select(["id"]).limit(1).to_list() == []


@pytest.mark.parametrize("mode, expected", [("fast", []), ("full", [{"id": 4}]), ("detail", [{"id": 4}])])
def test_partition_with_full_text_definition_reads_unindexed_tail(docs, mode, expected):
    append_rows(docs, row_ids=[3])
    docs.raw_table.copy({"deletion-vectors.enabled": "false"}).create_global_index("text", "full-text")
    append_rows(docs, row_ids=[4])
    docs.raw_table = docs.raw_table.copy({"full-text-index.search-mode": mode})
    search = docs.search("paimon", column="text", pre_filter="pt = 'b' AND label = 'target'")
    assert search.select(["id"]).limit(1).to_list() == expected


def test_non_partitioned_table_filters_before_top_k(tmp_path):
    pytest.importorskip("paimon_ftindex")
    schema = pa.schema([("id", pa.int64()), ("text", pa.string())])
    docs = pm.connect(options={"warehouse": str(tmp_path)}).create_table(
        "docs", schema=schema, options={"file.format": "parquet"})
    docs.add(pa.table({"id": [0, 1], "text": ["paimon", "paimon long document"]}, schema=schema))
    docs.raw_table.copy({"deletion-vectors.enabled": "false"}).create_global_index("text", "full-text")
    assert docs.search("paimon", column="text").where("id = 1").limit(1).to_list() == []
    assert docs.search("paimon", column="text", pre_filter="id = 1").select(["id"]).limit(1).to_list() == [
        {"id": 1}]


def test_empty_matches_skip_native_search(docs):
    with patch.object(DataEvolutionFullTextRead, "_eval", side_effect=AssertionError("empty search")):
        assert docs.search("paimon", column="text", pre_filter="id < 0").limit(1).to_list() == []


@pytest.mark.parametrize("selector", ["snapshot", "tag"])
def test_filters_use_historical_snapshot_and_deletions(docs, selector):
    saved = docs.raw_table.snapshot_manager().get_latest_snapshot()
    docs.raw_table.create_tag("saved", snapshot_id=saved.id)
    options = {"snapshot_id": saved.id} if selector == "snapshot" else {"tag_name": "saved"}
    docs.delete("id = 2")
    search = docs.search("paimon", column="text", pre_filter="label = 'target'", **options)
    assert search.select(["id"]).limit(1).to_list() == [{"id": 2}]
    assert docs.search("paimon", column="text", pre_filter="label = 'target'").limit(1).to_list() == []


def test_commit_during_filter_read_keeps_execution_snapshot(docs):
    search = docs.search("paimon", column="text", pre_filter="label = 'target'").select(["id"]).limit(1)

    def verify(table, predicate, candidates, partition_filter=None, snapshot=None):
        docs.update("id = 2", {"label": "changed"})
        return matching_rows(table, predicate, candidates, partition_filter, snapshot)

    with patch("pypaimon.table.source.global_index_row_filter.matching_rows", verify):
        assert search.to_list() == [{"id": 2}]
    assert search.to_list() == []


def test_hybrid_applies_data_filter_to_both_routes(docs):
    search = docs.search_hybrid([
        pm.vector_route("embedding", [0., 1.], limit=1),
        pm.text_route("paimon", column="text", limit=1),
    ], pre_filter="label = 'target'").select(["id"]).limit(1)
    assert search.to_list() == [{"id": 2}]


def test_data_filter_does_not_project_vectors_or_text(docs):
    from pypaimon.read.table_read import TableRead
    original = TableRead._new_arrow_batch_reader
    projections = []

    def read(reader, *args, **kwargs):
        projections.append([field.name for field in reader.read_type])
        return original(reader, *args, **kwargs)

    with patch.object(TableRead, "_new_arrow_batch_reader", read):
        assert docs.search("paimon", column="text", pre_filter="label = 'target'").select(["id"]).limit(1).to_list()
    assert projections == [["_ROW_ID"]]
    assert all("embedding" not in fields and "text" not in fields for fields in projections)


def test_primary_key_row_filter_is_explicitly_unsupported(docs):
    docs.raw_table = docs.raw_table.copy({"data-evolution.enabled": "false"})
    with pytest.raises(NotImplementedError, match="data-evolution"):
        builder(docs, "id = 2").new_full_text_read()
