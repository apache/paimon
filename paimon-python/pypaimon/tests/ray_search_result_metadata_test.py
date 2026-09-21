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

from pypaimon.multimodal.query import ScanQuery
from pypaimon.tests import ray_vector_search_test as ray_fixtures
from pypaimon.tests import search_result_metadata_test as fixtures

ray_cluster = ray_fixtures.ray_cluster
docs = fixtures.docs


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("indexed", [False, True])
@pytest.mark.parametrize("metadata_only", [False, True])
def test_ray_metadata_and_ordering_match_local(docs, ray_cluster, batch, indexed, metadata_only):
    if indexed:
        pytest.importorskip("paimon_vindex")
        docs.raw_table.copy({"deletion-vectors.enabled": "false"}).create_global_index(
            "embedding", "ivf-flat", options={"ivf-flat.nlist": "1", "ivf-flat.distance.metric": "l2"})
    search = fixtures.query(docs, batch, options={"ivf-flat.refine-factor": "2"})
    search.select(["_ROW_ID"] if metadata_only else ["id"]).with_score().order_by_score()
    expected = search.to_arrow()
    original = ScanQuery._read_global_index_result
    calls = []

    def lookup(query, result):
        calls.append(True)
        return original(query, result)

    with patch.object(ScanQuery, "_read_global_index_result", lookup):
        actual = search.to_arrow(execution="ray", concurrency=2)
    assert len(calls) == (0 if metadata_only else 1)
    expected_rows = [table.to_pylist() for table in expected] if batch else expected.to_pylist()
    actual_rows = [table.to_pylist() for table in actual] if batch else actual.to_pylist()
    assert actual_rows == expected_rows
    as_list = search.to_list(execution="ray", concurrency=2)
    assert as_list == expected_rows
