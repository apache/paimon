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
pytest.importorskip("paimon_ftindex")

import pypaimon.multimodal as pm
from pypaimon.tests import ray_vector_search_test as ray_fixtures
from pypaimon.tests import full_text_scalar_filter_test as text_fixtures
from pypaimon.ray import full_text_search as module

ray_cluster = ray_fixtures.ray_cluster
docs = text_fixtures.docs


@pytest.mark.parametrize("predicate", [None, "label = 'target'", "pt = 'b'", "id < 0"])
@pytest.mark.parametrize("concurrency", [1, 2])
def test_index_raw_filters_and_scores(docs, ray_cluster, predicate, concurrency):
    text_fixtures.append_rows(docs)
    docs.delete("id = 1")
    query = docs.search("paimon", column="text", pre_filter=predicate).select(["id"]).with_score().order_by_score()
    expected = query.to_list()
    dispatched = []
    original = module._map_tasks

    def record(worker, context, items, *args):
        dispatched.append((worker.__name__, len(items)))
        return original(worker, context, items, *args)

    with patch.object(module, "_map_tasks", record):
        assert query.to_list(execution="ray", concurrency=concurrency) == expected
    if predicate is None:
        assert ("_search_index", 1) in dispatched
        assert ("_search_raw", 1) in dispatched


@pytest.mark.parametrize("ranker", ["rrf", "weighted_score", "mrr"])
def test_hybrid_routes_preserve_ranking(docs, ray_cluster, ranker):
    text_fixtures.append_rows(docs)
    query = docs.search_hybrid([
        pm.vector_route("embedding", [0., 1.], limit=3, weight=2),
        pm.text_route("paimon", column="text", limit=4),
    ], ranker=ranker, pre_filter="id > 0").select(["id"]).with_score().order_by_score().limit(3)
    assert query.to_list(execution="ray", concurrency=2) == query.to_list()


def test_snapshot_pinning_and_post_filter(docs, ray_cluster):
    saved = docs.raw_table.snapshot_manager().get_latest_snapshot().id
    docs.delete("id = 2")
    query = docs.search("paimon", column="text", snapshot_id=saved,
                        pre_filter="label = 'target'").select(["id"]).with_score()
    assert query.to_list(execution="ray") == query.to_list()
    query.where("id = -1")
    assert query.to_list(execution="ray") == []


@pytest.mark.parametrize("options", [{"execution": "unknown"}, {"concurrency": 2},
                                     {"execution": "ray", "concurrency": 0},
                                     {"execution": "ray", "ray_remote_args": {"num_returns": 2}}])
def test_execution_validation(docs, options):
    with pytest.raises(ValueError):
        docs.search("paimon", column="text").to_arrow(**options)


def test_multiple_index_shards_and_worker_failure(docs, ray_cluster):
    text_fixtures.append_rows(docs)
    docs.raw_table.copy({"deletion-vectors.enabled": "false"}).create_global_index("text", "full-text")
    query = docs.search("paimon", column="text").select(["id"]).with_score().order_by_score()
    assert query.to_list(execution="ray", concurrency=2) == query.to_list()
    with patch.object(module, "_search_index", _fail_index):
        with pytest.raises(Exception, match="injected full-text failure"):
            query.to_arrow(execution="ray", concurrency=1)
    assert query.to_list(execution="ray") == query.to_list()


def _fail_index(reader, item):
    raise RuntimeError("injected full-text failure")
