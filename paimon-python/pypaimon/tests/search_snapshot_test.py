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

import threading
from unittest.mock import patch

import pyarrow as pa
import pytest

import pypaimon.multimodal as pm
from pypaimon.multimodal.query import ScanQuery
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.table.source.full_text_scan import FullTextScanPlan
from pypaimon.table.source.full_text_search_split import RawFullTextSearchSplit
from pypaimon.table.source.vector_search_scan import DataEvolutionVectorScan
from pypaimon.utils.range import Range

SCHEMA = pa.schema([
    ("id", pa.int64()), ("category", pa.string()), ("embedding", pa.list_(pa.float32(), 2))])
DATA = pa.table({"id": [1, 2], "category": ["allowed", "allowed"],
                 "embedding": [[0, 0], [10, 0]]}, schema=SCHEMA)


@pytest.fixture
def table(tmp_path):
    return pm.connect(options={"warehouse": str(tmp_path)}).create_table(
        "vectors", schema=SCHEMA,
        options={"file.format": "parquet", "vector.file.format": "parquet",
                 "vector-index.search-mode": "full"})


def search(table, kind="vector", **options):
    if kind == "batch":
        query = table.search_vectors([[0, 0], [0, 0]], **options)
    elif kind == "hybrid":
        query = table.search_hybrid([
            pm.vector_route("embedding", [0, 0]), pm.vector_route("embedding", [1, 0])], **options)
    else:
        query = table.search([0, 0], **options)
    return query.pre_filter("category = 'allowed'").select(["id", "category"]).limit(1)


@pytest.mark.parametrize("kind", ["vector", "batch", "hybrid"])
@pytest.mark.parametrize("change", ["update", "delete"])
@pytest.mark.parametrize("profiled", [False, True])
def test_search_and_lookup_share_snapshot_and_query_is_reusable(table, kind, change, profiled):
    table.add(DATA)
    query = search(table, kind)
    original_table = query._table
    expected = query.to_list()
    expected_row = [{"id": 1, "category": "allowed"}]
    assert expected == ([expected_row, expected_row] if kind == "batch" else expected_row)
    lookup = ScanQuery._read_global_index_result
    lookups = []

    def commit_before_lookup(execution, result, *args):
        if not lookups:
            if change == "delete":
                table.delete("id = 1")
            else:
                table.update("id = 1", {"category": "blocked"})
        lookups.append(execution._table)
        return lookup(execution, result, *args)

    with patch.object(ScanQuery, "_read_global_index_result", commit_before_lookup):
        if profiled:
            profile = query.profile()
            actual = ([t.to_pylist() for t in profile.result] if kind == "batch"
                      else profile.result.to_pylist())
            assert actual == expected
            snapshot_id = profile.plan.routes[0].snapshot_id
            assert all(route.snapshot_id == snapshot_id for route in profile.plan.routes)
            assert profile.lookup_snapshot_ids == [snapshot_id] * (2 if kind == "batch" else 1)
        else:
            assert query.to_list() == expected
    assert query._table is original_table
    assert all(t is lookups[0] for t in lookups)
    latest = [{"id": 2, "category": "allowed"}]
    assert query.to_list() == ([latest, latest] if kind == "batch" else latest)


def test_hybrid_builder_pins_snapshot_before_routes_start(table):
    table.add(DATA)
    builder = table.raw_table.new_hybrid_search_builder().with_limit(1)
    builder.add_vector_route("embedding", [0, 0], limit=1)
    builder.add_vector_route("embedding", [1, 0], limit=1)
    original_scan = DataEvolutionVectorScan.scan
    snapshots = []
    lock = threading.Lock()

    def scan_and_commit(scan):
        with lock:
            plan = original_scan(scan)
            if not snapshots:
                table.delete("id = 1")
            snapshots.append(plan.snapshot().id)
            return plan

    with patch.object(DataEvolutionVectorScan, "scan", scan_and_commit):
        result = builder.execute_local()
    assert snapshots == [1, 1]
    assert list(result.results()) == [0]


def test_empty_search_does_not_follow_first_commit(table):
    query = search(table)
    original_scan = DataEvolutionVectorScan.scan

    def append_before_scan(scan):
        table.add(DATA)
        return original_scan(scan)

    with patch.object(DataEvolutionVectorScan, "scan", append_before_scan):
        assert query.to_list() == []
    assert query.to_list() == [{"id": 1, "category": "allowed"}]


@pytest.mark.parametrize("selector", ["snapshot", "tag", "timestamp"])
def test_time_travel_survives_concurrent_writes_and_retained_tag_snapshot(table, selector):
    table.add(DATA)
    source = table.raw_table.snapshot_manager().get_latest_snapshot()
    if selector == "tag":
        table.raw_table.create_tag("training", snapshot_id=source.id)
        query = search(table, tag_name="training")
        # Tags retain manifests even after their original snapshot JSON expires.
        table.add(pa.table({"id": [3], "category": ["allowed"],
                            "embedding": [[20, 0]]}, schema=SCHEMA))
        table.raw_table.file_io.delete(table.raw_table.snapshot_manager().get_snapshot_path(source.id))
    elif selector == "timestamp":
        from pypaimon.multimodal.query import VectorQuery
        read_table = table.raw_table.copy({"scan.timestamp-millis": str(source.time_millis)})
        query = VectorQuery(read_table, [0, 0], "embedding").select(["id", "category"]).limit(1)
    else:
        query = search(table, snapshot_id=source.id)
    lookup = ScanQuery._read_global_index_result

    def update_before_lookup(execution, result):
        table.update("id = 1", {"category": "blocked"})
        if selector == "tag":
            table.raw_table.replace_tag("training")
        # Read-option copies must retain the captured snapshot too.
        execution._table = execution._table.copy({"read.batch-size": "2"})
        return lookup(execution, result)

    with patch.object(ScanQuery, "_read_global_index_result", update_before_lookup):
        assert query.to_list() == [{"id": 1, "category": "allowed"}]


def test_full_text_plan_pins_live_rows_and_raw_fallback_without_leaking_to_read(table):
    table.add(DATA)
    snapshot = table.raw_table.snapshot_manager().get_latest_snapshot()
    splits = [RawFullTextSearchSplit([Range(0, 1)])]
    plan = FullTextScanPlan(splits, snapshot)
    reader = (table.raw_table.new_full_text_search_builder()
              .with_query("category", "allowed").with_limit(2).new_full_text_read())
    table.delete("id = 1")
    live, raw = [], []

    def indexed_query(unused, rows):
        live.append(list(rows))
        return DictBasedScoredIndexResult({})

    def raw_index(row_ids, texts, offset):
        raw.append((row_ids, texts))
        return None

    with patch.object(reader, "_eval_column_query", indexed_query), \
            patch.object(reader, "_build_raw_index", raw_index):
        reader.read_plan(plan)
        reader.read(splits)
    assert live == [[0, 1], [1]]
    assert raw == [([0, 1], ["allowed", "allowed"]), ([1], ["allowed"])]


def test_read_view_copy_retains_snapshot_unless_selector_changes(table):
    from pypaimon.multimodal.query import VectorQuery

    table.add(DATA)
    view = search(table)._for_execution()._table
    table.update("id = 1", {"category": "blocked"})

    def read(read_table):
        return VectorQuery(read_table, [0, 0], "embedding").select(["category"]).limit(1).to_list()

    assert read(view.copy({"read.batch-size": "2"})) == [{"category": "allowed"}]
    latest = table.raw_table.snapshot_manager().get_latest_snapshot()
    assert read(view.copy({"scan.snapshot-id": str(latest.id)})) == [{"category": "blocked"}]
    assert read(view.copy({"scan.snapshot-id": None, "scan.mode": "default"})) == [{"category": "blocked"}]
    table.raw_table.branch_manager().create_branch("empty")
    assert read(view.copy({"branch": "empty", "scan.snapshot-id": None, "scan.mode": "default"})) == []


def test_indexed_search_lookup_uses_planned_snapshot(tmp_path):
    pytest.importorskip("paimon_vindex")
    from pypaimon.read.table_scan import TableScan

    table = pm.connect(options={"warehouse": str(tmp_path)}).create_table(
        "indexed", schema=SCHEMA,
        options={"file.format": "parquet", "vector.file.format": "parquet",
                 "deletion-vectors.enabled": "false"})
    table.add(DATA)
    table.create_index("embedding", index_type="ivf-flat", options={
        "ivf-flat.dimension": "2", "ivf-flat.nlist": "1", "ivf-flat.distance.metric": "l2"})
    query = table.search([0, 0]).select(["id"]).limit(1)
    snapshot = table.raw_table.snapshot_manager().get_latest_snapshot()
    lookup = ScanQuery._read_global_index_result
    plan = TableScan.plan
    lookup_snapshots = []

    def record_plan(scan):
        result = plan(scan)
        lookup_snapshots.append(result.snapshot_id)
        return result

    def append_before_lookup(execution, result):
        table.add(DATA)
        with patch.object(TableScan, "plan", record_plan):
            return lookup(execution, result)

    with patch.object(ScanQuery, "_read_global_index_result", append_before_lookup):
        assert query.to_list() == [{"id": 1}]
    assert lookup_snapshots == [snapshot.id]
    assert table.raw_table.snapshot_manager().get_latest_snapshot().id > snapshot.id
