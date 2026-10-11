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

from pypaimon.ray import batch_vector_search, vector_search
from pypaimon.tests import ray_vector_search_test as fixtures
from pypaimon.tests.ray_vector_search_test import VECTORS, add_rows, build_index

ray_cluster = fixtures.ray_cluster
table = fixtures.table


def make_query(table, batch):
    queries = [[1., 1.], [-1., 2.], [1., 1.]]
    return (table.search_vectors(queries) if batch else table.search(queries[0])).select(["id"]).limit(2)


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("method", ["to_pandas", "to_list"])
@pytest.mark.parametrize("indexed", [False, True])
def test_ray_result_conversion(table, ray_cluster, batch, method, indexed):
    add_rows(table, VECTORS)
    if indexed:
        build_index(table)
        add_rows(table, [[1., 1.]], 6)
    query = make_query(table, batch)
    expected = query.to_list()
    module = batch_vector_search if batch else vector_search
    name = "_execute_batch_vector_search" if batch else "_execute_vector_search"
    with patch.object(module, name, wraps=getattr(module, name)) as execute:
        actual = getattr(query, method)(
            execution="ray", concurrency=2, ray_remote_args={"num_cpus": 1})
    execute.assert_called_once()
    assert execute.call_args[1] == {"concurrency": 2, "ray_remote_args": {"num_cpus": 1}}
    if method == "to_pandas":
        actual = [frame.to_dict("records") for frame in actual] if batch else actual.to_dict("records")
    assert actual == expected


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("method", ["to_pandas", "to_list"])
def test_conversion_keeps_empty_results_and_default_local_execution(table, batch, method):
    query = make_query(table, batch)
    with patch.object(vector_search, "_execute_vector_search", side_effect=AssertionError("Ray dispatch")), \
            patch.object(batch_vector_search, "_execute_batch_vector_search",
                         side_effect=AssertionError("Ray dispatch")):
        actual = getattr(query, method)()
    values = actual if batch else [actual]
    assert len(values) == (3 if batch else 1)
    assert all(len(value) == 0 for value in values)
    if method == "to_pandas":
        assert all(list(frame.columns) == ["id"] for frame in values)


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("method", ["to_pandas", "to_list"])
def test_conversion_validates_options_and_propagates_failure(table, batch, method):
    query = make_query(table, batch)
    with pytest.raises(ValueError, match="execution must"):
        getattr(query, method)(execution="invalid")
    with pytest.raises(ValueError, match="require execution"):
        getattr(query, method)(concurrency=2)
    module = batch_vector_search if batch else vector_search
    name = "_execute_batch_vector_search" if batch else "_execute_vector_search"
    failure = RuntimeError("worker failed")
    with patch.object(module, name, side_effect=failure), pytest.raises(RuntimeError) as exc:
        getattr(query, method)(execution="ray")
    assert exc.value is failure
