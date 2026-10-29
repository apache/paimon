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

import tarfile
from types import SimpleNamespace

import pytest

from pypaimon.catalog.catalog_factory import CatalogFactory
from pypaimon.multimodal.query import BatchVectorQuery, VectorQuery
from pypaimon.table.source.primary_key_scored_result import PrimaryKeyScoredResult
from pypaimon.tests.primary_key_global_index_golden_test import GOLDEN_ARCHIVE


def _query(query_type):
    table = SimpleNamespace(
        fields=[], options=SimpleNamespace(data_evolution_enabled=lambda: False))
    vectors = [[1., 0.]] if query_type is BatchVectorQuery else [1., 0.]
    return query_type(table, vectors, "embedding")


@pytest.mark.parametrize("query_type", [VectorQuery, BatchVectorQuery])
def test_pk_ray_dataset_conversion_requires_row_id_results(query_type):
    pytest.importorskip("ray")
    from pypaimon.ray.search_result import read_search_result

    query = _query(query_type)
    with pytest.raises(NotImplementedError, match="require a data-evolution table"):
        read_search_result(query, PrimaryKeyScoredResult(0, [], []), 1, {}, None)


@pytest.mark.parametrize("query_type", [VectorQuery, BatchVectorQuery])
@pytest.mark.parametrize("method", ["with_score", "order_by_score"])
def test_pk_score_outputs_rejected_when_configuring_query(query_type, method):
    query = _query(query_type)
    with pytest.raises(NotImplementedError, match="Search score output and ordering require a data-evolution"):
        getattr(query, method)()


@pytest.fixture
def pk_table(tmp_path):
    pytest.importorskip("paimon_vindex")
    with tarfile.open(GOLDEN_ARCHIVE, "r:gz") as archive:
        archive.extractall(tmp_path)
    return CatalogFactory.create({"warehouse": str(tmp_path)}).get_table(
        "default.test_pk_vector_golden").copy({
            "read.native.enabled": "false", "scan.native-plan.enabled": "false",
            "vector-index.search-mode": "full"})


@pytest.mark.parametrize("projection", [None, [], ["id"], ["id", "id"], ["missing"]])
@pytest.mark.parametrize("post_filter", [None, "id >= 3"])
def test_pk_batch_physical_lookup_matches_single_queries(pk_table, projection, post_filter):
    vectors = [[1., 0., 0., 0.], [0., 1., 0., 0.], [1., 0., 0., 0.]]

    def configure(query):
        if projection is not None:
            query.select(projection)
        if post_filter is not None:
            query.where(post_filter)
        return query.limit(3)

    expected = [configure(VectorQuery(pk_table, vector, "embedding")).to_arrow() for vector in vectors]
    actual = configure(BatchVectorQuery(pk_table, vectors, "embedding")).to_arrow()
    assert len(actual) == len(expected)
    assert all(batch.equals(single) for batch, single in zip(actual, expected))
