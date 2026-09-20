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

import pytest

pytest.importorskip("ray")

from pypaimon.read.table_read import TableRead
from pypaimon.table.source.vector_search_read import AbstractVectorSearchReadImpl
from pypaimon.tests import ray_vector_search_test as ray_fixtures
from pypaimon.tests import vector_filter_exactness_test as fixtures
from pypaimon.tests.vector_filter_exactness_test import query, scalar_index

ray_cluster = ray_fixtures.ray_cluster
table = fixtures.table


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("mode", ["full", "fast"])
@pytest.mark.parametrize("refine", [False, True])
def test_ray_applies_exact_row_filter_before_worker_top_k(table, ray_cluster, batch, mode, refine):
    scalar_index(table)
    table.raw_table = table.raw_table.copy({
        "vector-index.search-mode": mode, "global-index.filter.refine-from-data": str(refine).lower()})
    original = AbstractVectorSearchReadImpl._matching_candidate_rows
    arrow_read = TableRead._new_arrow_batch_reader
    calls = []

    def no_vectors(read, *args, **kwargs):
        assert "embedding" not in [field.name for field in read.read_type]
        return arrow_read(read, *args, **kwargs)

    def verify(reader, candidates, snapshot):
        calls.append(list(candidates))
        assert snapshot is not None
        with patch.object(TableRead, "_new_arrow_batch_reader", no_vectors):
            return original(reader, candidates, snapshot)

    with patch.object(AbstractVectorSearchReadImpl, "_matching_candidate_rows", verify):
        result = query(table, "name LIKE '%zeta%'", batch).to_arrow(execution="ray", concurrency=2)
    actual = [value.to_pylist() for value in result] if batch else result.to_pylist()
    expected = [{"id": 1}] if refine else []
    assert actual == ([expected, expected] if batch else expected)
    assert calls == ([[0, 1, 2]] if refine else [])
