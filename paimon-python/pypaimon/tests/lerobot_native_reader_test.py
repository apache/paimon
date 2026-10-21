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
@pytest.mark.parametrize("option", ["read.native.enabled", "scan.native-plan.enabled"])
def test_native_frame_lookup_keeps_residual_and_snapshot(frames, option):
    # The index covers the original rows, not this append.
    frames.add(pa.table({"index": [40, 50], "value": [4, 5]}))
    current = reader(frames, **{option: "true"})
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
        "read.native.enabled": "true", "scan.tag-name": "training",
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
    current = reader(frames, **{"read.native.enabled": "true"})
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
def test_native_plan_failure_keeps_predicate(frames):
    current = reader(frames, **{"read.native.enabled": "true"})
    try:
        with patch("pypaimon.execution.native.read.plan.native_plan", side_effect=RuntimeError("unavailable")):
            assert current.read_indices([20], ["value"])["value"].to_pylist() == [2]
    finally:
        current.close()
