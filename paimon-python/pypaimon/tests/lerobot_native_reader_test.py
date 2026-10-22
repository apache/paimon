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

import pickle
import multiprocessing
from unittest.mock import patch

import pyarrow as pa
import pytest

import pypaimon.multimodal as pm
from pypaimon.multimodal.lerobot.reader import _PaimonTableFrameReader
from pypaimon.execution.native.runtime import native_runtime_available
from pypaimon.execution.native.read.plan import native_plan

pytest.importorskip("torch")


def _read_in_worker(reader, output):
    try:
        assert reader._table.options.native_read_enabled()
        with patch.object(reader._locator, "locate", side_effect=AssertionError("Python frame lookup")):
            output.put(reader.read_indices([10, 40], ["index"])["index"].to_pylist())
    finally:
        reader.close()


@pytest.fixture
def frames(tmp_path):
    conn = pm.connect(options={"warehouse": str(tmp_path)})
    table = conn.create_table("frames", data=pa.table({
        "index": [10, 20, 30], "value": [1, 2, 3],
    }), options={
        "row-tracking.enabled": "true",
        "data-evolution.enabled": "true",
        "global-index.enabled": "true",
        "bucket": "-1",
    })
    table.raw_table.create_global_index("index", index_type="btree")
    return table


def reader(table, **options):
    return _PaimonTableFrameReader(
        table.raw_table.copy(options), columns=["index", "value"])


@pytest.mark.skipif(not native_runtime_available(), reason="native reader required")
@pytest.mark.parametrize("option", [None, "read.native.enabled", "scan.native-plan.enabled"])
def test_native_frame_lookup_keeps_residual_and_snapshot(frames, option):
    # The index covers the original rows, not this append.
    frames.add(pa.table({"index": [40, 50], "value": [4, 5]}))
    current = reader(frames, **({option: "true"} if option else {}))
    if option is None:
        assert current._table.options.native_read_enabled()
    frames.add(pa.table({"index": [60], "value": [6]}))
    native_plans = []

    def checked_plan(*args, **kwargs):
        plan = native_plan(*args, **kwargs)
        native_plans.append(plan)
        return plan

    try:
        with patch.object(current._locator, "locate",
                          side_effect=AssertionError("Python frame lookup")), \
                patch("pypaimon.execution.native.read.plan.native_plan", side_effect=checked_plan):
            for indices, expected in [([40, 20, 40], [20, 40]),
                                      ([60, 99], []), ([], [])]:
                result = current.read_indices(indices, ["value", "index"])
                assert result.column_names == ["value", "index"]
                assert sorted(result["index"].to_pylist()) == expected
            assert len(native_plans) == 3
            assert all(plan.snapshot_id == current.snapshot_id for plan in native_plans)
    finally:
        current.close()


@pytest.mark.skipif(not native_runtime_available(), reason="native reader required")
def test_native_frame_lookup_preserves_tag_after_pickle(frames):
    frames.raw_table.create_tag("training")
    current = reader(frames, **{
        "scan.tag-name": "training",
    })
    restored = pickle.loads(pickle.dumps(current))
    frames.add(pa.table({"index": [40], "value": [4]}))
    frames.raw_table.replace_tag("training")
    try:
        assert current.snapshot_id == restored.snapshot_id
        assert restored.read_indices([10, 40], ["index"])["index"].to_pylist() == [10]
    finally:
        current.close()
        restored.close()


@pytest.mark.skipif(not native_runtime_available(), reason="native reader required")
def test_native_frame_lookup_in_spawn_worker(frames):
    current = reader(frames)
    frames.add(pa.table({"index": [40], "value": [4]}))
    context = multiprocessing.get_context("spawn")
    output = context.Queue()
    process = context.Process(target=_read_in_worker, args=(current, output))
    try:
        process.start()
        assert output.get(timeout=30) == [10]
        process.join(timeout=30)
        assert process.exitcode == 0
    finally:
        if process.is_alive():
            process.terminate()
            process.join()
        output.close()
        current.close()


def test_missing_native_runtime_keeps_cached_python_lookup(frames):
    current = reader(frames, **{"read.native.enabled": "true"})
    try:
        with patch("pypaimon.execution.native.runtime.native_runtime_available", return_value=False), \
                patch.object(current._locator, "locate", wraps=current._locator.locate) as locate:
            assert current.read_indices([30], ["value"])["value"].to_pylist() == [3]
            locate.assert_called_once_with([30])
    finally:
        current.close()


@pytest.mark.skipif(not native_runtime_available(), reason="native reader required")
def test_native_plan_failure_reuses_scanner_and_keeps_predicate(frames):
    from pypaimon.globalindex import DataEvolutionGlobalIndexScanner
    frames.add(pa.table({"index": [40, 50], "value": [4, 5]}))
    current = reader(frames, **{
        "scan.native-plan.enabled": "true", "read.native.enabled": "false",
    })
    try:
        with patch("pypaimon.execution.native.read.plan.native_plan",
                   side_effect=RuntimeError("unavailable")), \
                patch.object(current._locator, "locate", wraps=current._locator.locate) as locate, \
                patch.object(DataEvolutionGlobalIndexScanner, "create",
                             wraps=DataEvolutionGlobalIndexScanner.create) as create:
            for indices, expected in [([20], [2]), ([40], [4]), ([10, 99], [1])]:
                assert current.read_indices(indices, ["value"])["value"].to_pylist() == expected
            assert locate.call_count == 3
            assert create.call_count == 1
            scanner = current._locator._scanner
            with patch.object(scanner, "close", wraps=scanner.close) as close:
                current.close()
                close.assert_called_once()
    finally:
        current.close()


@pytest.mark.python_plan
@pytest.mark.python_read
@pytest.mark.parametrize("options", [
    {"read.native.enabled": "false"},
    {"scan.native-plan.enabled": "false"},
])
def test_explicit_python_lookup_is_preserved(frames, options):
    current = reader(frames, **options)
    try:
        assert not current._table.options.native_read_enabled()
        with patch.object(current._locator, "locate", wraps=current._locator.locate) as locate:
            assert current.read_indices([20], ["value"])["value"].to_pylist() == [2]
            locate.assert_called_once_with([20])
    finally:
        current.close()


@pytest.mark.python_plan
@pytest.mark.python_read
@pytest.mark.parametrize("available", [False, True])
def test_auto_native_uses_private_table_copy(frames, available):
    original = dict(frames.raw_table.options.options.to_map())
    with patch("pypaimon.execution.native.runtime.native_runtime_available", return_value=available):
        current = reader(frames)
    try:
        assert current._table.options.native_read_enabled() == available
        assert frames.raw_table.options.options.to_map() == original
        if not available:
            with patch.object(current._locator, "locate", wraps=current._locator.locate) as locate:
                assert current.read_indices([30], ["value"])["value"].to_pylist() == [3]
                locate.assert_called_once_with([30])
    finally:
        current.close()


def test_unsupported_native_plan_reuses_python_index_scanner(frames):
    from pypaimon.globalindex import DataEvolutionGlobalIndexScanner
    frames.add(pa.table({"index": [40], "value": [4]}))
    with patch("pypaimon.execution.native.runtime.native_runtime_available", return_value=True):
        current = reader(frames)
    try:
        assert current._table.options.native_read_enabled()
        with patch("pypaimon.execution.native.runtime.native_runtime_available", return_value=True), \
                patch("pypaimon.execution.native.read.plan.NativeTableScan._native_plan_supported",
                      return_value=False), \
                patch("pypaimon.execution.native.read.plan.native_plan",
                      side_effect=AssertionError("unsupported native plan")), \
                patch.object(DataEvolutionGlobalIndexScanner, "create",
                             wraps=DataEvolutionGlobalIndexScanner.create) as create:
            for indices, expected in [([20], [2]), ([40], [4]), ([10, 99], [1])]:
                result = current.read_indices(indices, ["value"])
                assert result["value"].to_pylist() == expected
            assert create.call_count == 1
            scanner = current._locator._scanner
            with patch.object(scanner, "close", wraps=scanner.close) as close:
                current.close()
                close.assert_called_once()
    finally:
        current.close()


@pytest.mark.skipif(not native_runtime_available(), reason="native reader required")
@pytest.mark.parametrize("denied", [False, True])
def test_native_lookup_preserves_query_authorization(frames, denied):
    from pypaimon.read.table_scan import TableScan
    current = reader(frames)
    calls = []

    def authorize(*args, **kwargs):
        calls.append(True)
        if denied:
            raise PermissionError("denied")
        return None

    original_init = TableScan.__init__

    def init_with_auth(scan, *args, **kwargs):
        original_init(scan, *args, **kwargs)
        scan._query_auth_fn = authorize

    try:
        with patch.object(TableScan, "__init__", init_with_auth), \
                patch.object(current._locator, "locate", side_effect=AssertionError("authorization bypass")), \
                patch("pypaimon.execution.native.read.plan.native_plan",
                      side_effect=RuntimeError("unavailable")):
            if denied:
                with pytest.raises(PermissionError, match="denied"):
                    current.read_indices([20], ["value"])
            else:
                assert current.read_indices([20], ["value"])["value"].to_pylist() == [2]
            assert calls
    finally:
        current.close()


@pytest.mark.parametrize("option", ["scan.native-plan.enabled", "read.native.enabled"])
def test_pickled_native_locator_rebuilds_pinned_fallback_once(frames, option):
    from pypaimon.globalindex import DataEvolutionGlobalIndexScanner
    frames.add(pa.table({"index": [40], "value": [4]}))
    current = reader(frames, **{
        "scan.native-plan.enabled": "false", "read.native.enabled": "false",
        option: "true"})
    restored = pickle.loads(pickle.dumps(current))
    assert restored._locator._splits is None
    frames.add(pa.table({"index": [60], "value": [6]}))
    try:
        with patch("pypaimon.execution.native.read.plan.native_plan",
                   side_effect=RuntimeError("native unavailable")), \
                patch.object(DataEvolutionGlobalIndexScanner, "create",
                             wraps=DataEvolutionGlobalIndexScanner.create) as create:
            for unused in range(3):
                result = restored.read_indices([10, 40, 60], ["value", "index"])
                assert result.column_names == ["value", "index"]
                assert sorted(result["index"].to_pylist()) == [10, 40]
                assert sorted(result["value"].to_pylist()) == [1, 4]
            assert create.call_count == 1
            assert restored._locator._splits is not None
    finally:
        current.close()
        restored.close()


def test_pickled_native_locator_rebuilds_original_projection(tmp_path):
    from pypaimon.catalog.table_query_auth import TableQueryAuthResult
    from pypaimon.read.table_scan import TableScan

    conn = pm.connect(options={"warehouse": str(tmp_path)})
    table = conn.create_table("projected_frames", data=pa.table({
        "index": [10, 20, 30],
        "value": [1, 2, 3],
        "secret": [4, 5, 6],
    }), options={
        "row-tracking.enabled": "true",
        "data-evolution.enabled": "true",
        "global-index.enabled": "true",
        "bucket": "-1",
    })
    table.raw_table.create_global_index("index", index_type="btree")
    selected = []

    def authorize(columns):
        selected.append(columns)
        if "secret" in columns:
            raise PermissionError("secret column denied")
        return TableQueryAuthResult(None, None)

    original_init = TableScan.__init__

    def init_with_auth(scan, *args, **kwargs):
        original_init(scan, *args, **kwargs)
        scan._query_auth_fn = authorize

    with patch.object(TableScan, "__init__", init_with_auth):
        current = reader(table, **{
            "scan.native-plan.enabled": "false",
            "read.native.enabled": "true",
        })
    restored = pickle.loads(pickle.dumps(current))
    try:
        assert restored._locator._splits is None
        with patch.object(TableScan, "__init__", init_with_auth), \
                patch("pypaimon.execution.native.runtime.native_runtime_available",
                      return_value=False):
            result = restored.read_indices([20], ["value"])
        assert result["value"].to_pylist() == [2]
        assert selected == [["index", "value"], ["index", "value"]]
    finally:
        current.close()
        restored.close()


def test_pickled_python_locator_keeps_fallback_splits(frames):
    current = reader(frames, **{
        "scan.native-plan.enabled": "false", "read.native.enabled": "false"})
    restored = pickle.loads(pickle.dumps(current))
    try:
        assert restored._locator._splits is not None
        assert restored.read_indices([20], ["value"])["value"].to_pylist() == [2]
    finally:
        current.close()
        restored.close()
