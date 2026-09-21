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
from pypaimon.multimodal.query import ScanQuery


@pytest.fixture
def docs(tmp_path):
    schema = pa.schema([("id", pa.int64()), ("text", pa.string()),
                        ("embedding", pa.list_(pa.float32(), 2))])
    table = pm.connect(options={"warehouse": str(tmp_path)}).create_table(
        "docs", schema=schema, options={"file.format": "parquet", "vector.file.format": "parquet",
                                        "vector-index.search-mode": "full", "read.batch-size": "1"})
    table.add(pa.table({"id": [0, 1, 2, 3], "text": ["paimon long document", "paimon", "other", "paimon"],
                        "embedding": [[2., 1.], [0., 1.], [1., 1.], [0., 1.]]}, schema=schema))
    return table


def query(docs, batch=False, **kwargs):
    return (docs.search_vectors([[0., 1.], [2., 1.]], **kwargs) if batch else
            docs.search([0., 1.], **kwargs)).limit(4)


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("indexed", [False, True])
def test_scores_align_with_rows_and_ordering_is_opt_in(docs, batch, indexed):
    if indexed:
        pytest.importorskip("paimon_vindex")
        docs.raw_table.copy({"deletion-vectors.enabled": "false"}).create_global_index(
            "embedding", "ivf-flat", options={"ivf-flat.nlist": "1", "ivf-flat.distance.metric": "l2"})
    plain = query(docs, batch).select(["id"]).to_arrow()
    scored = query(docs, batch).select(["id"]).with_score().to_arrow()
    ordered = query(docs, batch).select(["id"]).with_score("relevance").order_by_score().to_arrow()
    plain, scored, ordered = ([plain], [scored], [ordered]) if not batch else (plain, scored, ordered)
    expected = [[.2, 1., .5, 1.], [1., .2, .5, .2]]
    for i, (before, after, ranked) in enumerate(zip(plain, scored, ordered)):
        assert before.equals(after.select(["id"]))
        assert after.column("_score").type == pa.float64()
        expected_scores = [expected[i][row] for row in after["id"].to_pylist()]
        assert after.column("_score").to_pylist() == pytest.approx(expected_scores)
        ids = ranked["id"].to_pylist()
        assert ids == sorted(range(4), key=lambda row: (-expected[i][row], row))
        assert ranked.column_names == ["id", "relevance"]


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("projection", [[], ["_ROW_ID"]])
@pytest.mark.parametrize("indexed", [False, True])
def test_metadata_only_skips_final_lookup(docs, batch, projection, indexed):
    if indexed:
        pytest.importorskip("paimon_vindex")
        docs.raw_table.copy({"deletion-vectors.enabled": "false"}).create_global_index(
            "embedding", "ivf-flat", options={"ivf-flat.nlist": "1", "ivf-flat.distance.metric": "l2"})
    with patch.object(ScanQuery, "_read_global_index_result", side_effect=AssertionError("final lookup")):
        actual = query(docs, batch).select(projection).with_score().order_by_score().to_arrow()
    for result in actual if batch else [actual]:
        assert result.num_rows == 4
        assert result.column_names == projection + ["_score"]
        assert result["_score"].to_pylist() == sorted(result["_score"].to_pylist(), reverse=True)


def test_ordering_does_not_require_score_projection(docs):
    actual = query(docs).select(["id"]).order_by_score().to_list()
    assert actual == [{"id": 1}, {"id": 3}, {"id": 2}, {"id": 0}]


@pytest.mark.parametrize("batch", [False, True])
def test_score_only_with_post_filter(docs, batch):
    actual = query(docs, batch).select([]).where("id >= 2").with_score().order_by_score().to_arrow()
    for i, table in enumerate(actual if batch else [actual]):
        expected = [1., .5] if i == 0 else [.5, .2]
        assert table.column_names == ["_score"]
        assert table["_score"].to_pylist() == pytest.approx(expected)


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("column", ["id", "_ROW_ID"])
def test_duplicate_projections_keep_scores(docs, batch, column):
    actual = query(docs, batch).select([column, column]).with_score().order_by_score().to_arrow()
    for table in actual if batch else [actual]:
        assert table.column_names == [column, column, "_score"]
        assert table.column(0).equals(table.column(1))
    if column == "_ROW_ID":
        with_lookup = query(docs, batch).select([column, column]).where("id >= 0").with_score().to_arrow()
        assert (with_lookup[0] if batch else with_lookup).schema == (actual[0] if batch else actual).schema


@pytest.mark.parametrize("batch", [False, True])
def test_post_filter_and_deleted_rows_keep_score_alignment(docs, batch):
    docs.delete("id = 1")
    actual = query(docs, batch).select(["id"]).where("id >= 2").with_score().order_by_score().to_arrow()
    tables = actual if batch else [actual]
    assert tables[0].to_pylist() == [{"id": 3, "_score": 1.}, {"id": 2, "_score": .5}]
    with patch.object(ScanQuery, "_read_global_index_result", wraps=None) as lookup:
        lookup.side_effect = RuntimeError("post filter needs lookup")
        with pytest.raises(RuntimeError, match="post filter needs lookup"):
            query(docs, batch).select(["_ROW_ID"]).where("id >= 2").with_score().to_arrow()


def test_query_authorization_keeps_final_lookup(docs):
    from pypaimon.catalog.filesystem_catalog import FileSystemCatalog
    from pypaimon.catalog.table_query_auth import TableQueryAuthResult
    from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
    from pypaimon.tests.table_query_auth_test import _simple_filter_json

    docs.raw_table = docs.raw_table.copy({"query-auth.enabled": "true"})
    result = DictBasedScoredIndexResult({0: .2, 1: 1., 2: .5, 3: 1.})
    auth = TableQueryAuthResult([_simple_filter_json("text", "paimon")], None)
    search = query(docs).select(["_ROW_ID"]).with_score().order_by_score()._for_execution()
    with patch.object(FileSystemCatalog, "auth_table_query", return_value=auth):
        assert search._read_global_index_result(result).to_pylist() == [
            {"_ROW_ID": 1, "_score": 1.}, {"_ROW_ID": 3, "_score": 1.}]


@pytest.mark.parametrize("batch", [False, True])
def test_empty_result_keeps_score_schema(docs, batch):
    actual = query(docs, batch, pre_filter="id < 0").select(["id"]).with_score().order_by_score().to_arrow()
    for result in actual if batch else [actual]:
        assert result.num_rows == 0
        assert result.schema == pa.schema([("id", pa.int64()), ("_score", pa.float64())])


def test_batch_keeps_one_lookup_and_query_specific_scores(docs):
    original = ScanQuery._read_global_index_result
    calls = []

    def lookup(query, result):
        calls.append(True)
        return original(query, result)

    with patch.object(ScanQuery, "_read_global_index_result", lookup):
        actual = query(docs, True).select(["id"]).with_score().order_by_score().to_arrow()
    assert calls == [True]
    assert actual[0].to_pylist()[0] == {"id": 1, "_score": 1.}
    assert actual[1].to_pylist()[0] == {"id": 0, "_score": 1.}


@pytest.mark.parametrize("selector", ["snapshot", "tag"])
def test_metadata_only_keeps_historical_deletions(docs, selector):
    snapshot = docs.raw_table.snapshot_manager().get_latest_snapshot()
    docs.raw_table.create_tag("saved", snapshot_id=snapshot.id)
    options = {"snapshot_id": snapshot.id} if selector == "snapshot" else {"tag_name": "saved"}
    before = query(docs, **options).select(["_ROW_ID"]).with_score().order_by_score()
    expected = before.to_list()
    docs.delete("id = 1")
    assert before.to_list() == expected
    assert query(docs).select(["_ROW_ID"]).with_score().to_arrow().num_rows == 3


def test_commit_between_search_and_lookup_keeps_one_snapshot(docs):
    search = query(docs).select(["id", "text"]).with_score().order_by_score()
    expected = search.to_list()
    original = ScanQuery._read_global_index_result

    def lookup(execution, result):
        docs.update("id = 1", {"text": "changed"})
        return original(execution, result)

    with patch.object(ScanQuery, "_read_global_index_result", lookup):
        assert search.to_list() == expected
    assert search.to_list()[0]["text"] == "changed"


@pytest.mark.parametrize("name", [None, "", 1, "id", "_ROW_ID"])
def test_score_alias_validation(docs, name):
    with pytest.raises(ValueError, match="Score column name"):
        query(docs).with_score(name)


def test_scores_require_data_evolution(docs):
    docs.raw_table = docs.raw_table.copy({"data-evolution.enabled": "false"})
    with pytest.raises(NotImplementedError, match="data-evolution"):
        query(docs).with_score()
    with pytest.raises(NotImplementedError, match="data-evolution"):
        query(docs).order_by_score()


def test_full_text_and_hybrid_scores(docs):
    pytest.importorskip("paimon_ftindex")
    docs.raw_table.copy({"deletion-vectors.enabled": "false"}).create_global_index("text", "full-text")
    for search in [docs.search("paimon", column="text"), docs.search_hybrid([
            pm.vector_route("embedding", [0., 1.]), pm.text_route("paimon", column="text")])]:
        result = search.select(["id"]).with_score().order_by_score().limit(3).to_arrow()
        assert result.num_rows == 3
        assert result["_score"].to_pylist() == sorted(result["_score"].to_pylist(), reverse=True)
        assert all(score > 0 for score in result["_score"].to_pylist())
