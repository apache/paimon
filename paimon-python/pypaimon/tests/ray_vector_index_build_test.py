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

"""Exercise Ray vector-index construction with real worker processes."""

import time
from unittest.mock import patch

import pytest

ray = pytest.importorskip("ray")
pytest.importorskip("paimon_vindex")

from pypaimon.globalindex.create_global_index import GlobalIndexBuilder
from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.ray import vector_index_build
from pypaimon.tests import ray_vector_search_test as fixtures
from pypaimon.tests.ray_vector_search_test import add_rows, VECTORS

ray_cluster = fixtures.ray_cluster
table = fixtures.table


OPTIONS = {"global-index.row-count-per-shard": "3", "ivf-flat.nlist": "1"}


def index_files(table):
    return [entry.index_file for entry in IndexFileHandler(table.raw_table).scan(
        table.raw_table.snapshot_manager().get_latest_snapshot())
        if entry.index_file.index_type == "ivf-flat"]


@pytest.mark.parametrize("concurrency", [1, 2])
def test_build_and_incremental_coverage(table, ray_cluster, concurrency):
    add_rows(table, VECTORS)
    table.delete("id = 0")
    before = table.raw_table.snapshot_manager().get_latest_snapshot().id
    assert table.create_index("embedding", "ivf-flat", options=OPTIONS,
                              execution="ray", concurrency=concurrency) == 2
    assert table.raw_table.snapshot_manager().get_latest_snapshot().id == before + 1
    files = sorted(index_files(table), key=lambda f: f.global_index_meta.row_range_start)
    assert [(f.global_index_meta.row_range_start, f.global_index_meta.row_range_end) for f in files] == [(0, 2), (3, 5)]
    query = table.search([1., 1.], column="embedding", options={"ivf.nprobe": "1"}).select(["id"]).limit(2)
    assert query.to_list() == query.to_list(execution="ray")
    assert all(row["id"] != 0 for row in query.to_list())
    assert table.create_index("embedding", "ivf-flat", options=OPTIONS, execution="ray") == 0
    add_rows(table, [[20., 1.]], 6)
    assert table.create_index("embedding", "ivf-flat", options=OPTIONS, execution="ray") == 1
    assert len(index_files(table)) == 3


def test_build_messages_do_not_publish_or_include_later_appends(table, ray_cluster):
    add_rows(table, VECTORS[:3])
    builder = GlobalIndexBuilder(table.raw_table, "embedding", "ivf-flat", options=OPTIONS)
    snapshot = table.raw_table.snapshot_manager().get_latest_snapshot().id
    messages = builder.build(execution="ray")
    assert table.raw_table.snapshot_manager().get_latest_snapshot().id == snapshot
    assert not index_files(table)
    add_rows(table, VECTORS[3:], 3)
    commit = table.raw_table.new_batch_write_builder().new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()
    assert [(f.global_index_meta.row_range_start, f.global_index_meta.row_range_end)
            for f in index_files(table)] == [(0, 2)]
    assert table.create_index("embedding", "ivf-flat", options=OPTIONS, execution="ray") == 1


def _write_then_fail(context, shard, name):
    if shard[1].from_:
        time.sleep(0.3)
    message = vector_index_build._build_shard(context, shard, name)
    if not shard[1].from_:
        raise RuntimeError("failed after writing an index file")
    return message


def test_worker_failure_drains_and_cleans_all_outputs(table, ray_cluster):
    add_rows(table, VECTORS)
    before = table.raw_table.snapshot_manager().get_latest_snapshot().id
    # Pass a real serialized worker function; a failure after finish() must also
    # clean files whose commit messages were never delivered to the driver.
    original_remote = ray.remote

    def remote(worker):
        return original_remote(_write_then_fail)

    with patch.object(ray, "remote", remote):
        with pytest.raises(Exception, match="failed after writing"):
            table.create_index("embedding", "ivf-flat", options=OPTIONS,
                               execution="ray", concurrency=2)
    assert table.raw_table.snapshot_manager().get_latest_snapshot().id == before
    assert not index_files(table)
    root = table.raw_table.path_factory().global_index_path_factory().global_index_root_path()
    from pathlib import Path
    assert not list(Path(root).glob("*.index"))
    assert table.create_index("embedding", "ivf-flat", options=OPTIONS, execution="ray") == 2


@pytest.mark.parametrize("kwargs,match", [
    ({"execution": "other"}, "execution"),
    ({"concurrency": 1}, "execution='ray'"),
    ({"execution": "ray", "concurrency": 0}, "concurrency"),
    ({"execution": "ray", "ray_remote_args": {"max_retries": 1}}, "max_retries"),
    ({"execution": "ray", "ray_remote_args": {"retry_exceptions": True}}, "max_retries"),
])
def test_validate_before_empty_plan(table, kwargs, match):
    with pytest.raises(ValueError, match=match):
        table.create_index("embedding", "ivf-flat", options=OPTIONS, **kwargs)


def test_reject_non_vector_ray_build(table):
    with pytest.raises(ValueError, match="only native vector"):
        table.create_index("id", "btree", execution="ray")
