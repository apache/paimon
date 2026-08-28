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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import importlib.util
import subprocess
import sys

import numpy as np
import pyarrow as pa
import pytest

import pypaimon.multimodal as pmm
from pypaimon.sample import robomind_agilex as agilex


h5py = pytest.importorskip("h5py")

requires_vortex = pytest.mark.skipif(
    importlib.util.find_spec("vortex") is None,
    reason="RoboMIND ingestion uses Vortex, which requires Python >= 3.11",
)


_NUMERIC_PATHS = [path for _, path in agilex.NUMERIC_FIELDS]
_IMAGE_PATHS = [path for _, path in agilex.IMAGE_FIELDS]


def _write_episode(
        root, split, name, offset, frames=3, status="success_episodes"):
    path = (root / "13_packbowl" / status / split / name
            / "data" / "trajectory.hdf5")
    path.parent.mkdir(parents=True)
    with h5py.File(path, "w") as h5:
        h5.attrs["compress"] = True
        h5.attrs["sim"] = False
        h5.create_dataset("language_raw", data=[b"pack the bowl"])
        h5.create_dataset(
            "language_distilbert",
            data=np.full((1, 1, 768), offset, dtype=np.float16),
        )
        for index, hdf5_path in enumerate(_NUMERIC_PATHS):
            values = np.arange(frames * 7, dtype=np.float64).reshape(frames, 7)
            h5.create_dataset(hdf5_path, data=values + offset + index * 100)
        variable = h5py.vlen_dtype(np.dtype("uint8"))
        for index, hdf5_path in enumerate(_IMAGE_PATHS):
            dataset = h5.create_dataset(hdf5_path, (frames,), dtype=variable)
            for frame_index in range(frames):
                payload = "%s:%s:%s" % (name, index, frame_index)
                dataset[frame_index] = np.frombuffer(
                    payload.encode("utf-8"), dtype=np.uint8)
    return path


@pytest.fixture
def agilex_input(tmp_path):
    paths = [
        _write_episode(tmp_path, "train", "train-a", 0),
        _write_episode(tmp_path, "train", "train-b", 10),
        _write_episode(tmp_path, "val", "val-a", 20),
        _write_episode(
            tmp_path, "val", "val-b", 30, status="failed_episodes"),
    ]
    return tmp_path / "13_packbowl", paths


@pytest.fixture
def customer_agilex_input(request):
    value = request.config.getoption("--robomind-agilex-input")
    if not value:
        pytest.skip("use --robomind-agilex-input to test downloaded data")
    return value


@requires_vortex
def test_explicit_customer_input_uses_downloaded_episodes(
        customer_agilex_input, tmp_path):
    episodes = agilex.discover_episodes(customer_agilex_input)
    result = agilex.ingest_local(
        customer_agilex_input, tmp_path / "customer-warehouse")
    assert result.episode_count == len(episodes)
    assert result.frame_count > 0


def _read(warehouse, table_name, columns=None):
    connection = pmm.connect(
        database=agilex.DEFAULT_DATABASE,
        options={"warehouse": str(warehouse)},
    )
    table = connection.get_table(table_name)
    query = table.scan()
    if columns is not None:
        query = query.select(columns)
    return table, query.to_arrow()


def _logical_rows(warehouse, table_name, schema):
    _, rows = _read(warehouse, table_name, schema.names)
    if "frame_index" in schema.names:
        sort_keys = [
            ("episode_id", "ascending"),
            ("frame_index", "ascending"),
        ]
    elif "episode_id" in schema.names:
        sort_keys = [("episode_id", "ascending")]
    else:
        sort_keys = [("statistics_version", "ascending")]
    return rows.sort_by(sort_keys)


def _contains_payload(value):
    if isinstance(value, (bytes, pa.Table, pa.RecordBatch, pa.Array)):
        return True
    if isinstance(value, dict):
        return any(_contains_payload(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_payload(item) for item in value)
    return False


def test_shared_transform_streams_complete_agilex_business_schema(
        agilex_input):
    root, paths = agilex_input
    episodes = agilex.discover_episodes(root)
    transform = agilex.RoboMindAgileXFrameTransform(
        episodes, batch_size=2)
    source = pmm.Hdf5File(path=paths[0].as_uri())

    with h5py.File(paths[0], "r") as h5:
        batches = list(transform(h5, source))

    assert [batch.num_rows for batch in batches] == [2, 1]
    assert all(batch.schema == agilex.frame_schema() for batch in batches)
    frames = pa.Table.from_batches(batches)
    assert frames["episode_id"].to_pylist() == ["train-a"] * 3
    assert frames["frame_index"].to_pylist() == [0, 1, 2]
    assert frames["rgb_front"][0].as_py() == b"train-a:0:0"
    assert frames["depth_right_wrist"][2].as_py() == b"train-a:5:2"
    assert frames["action_joint_position_left"][1].as_py() == [
        float(value) for value in range(1207, 1214)
    ]


@requires_vortex
def test_local_ingest_and_backfill_materialize_only_canonical_action(
        agilex_input, tmp_path):
    root, paths = agilex_input
    warehouse = tmp_path / "local-warehouse"

    completed = subprocess.run(
        [
            sys.executable,
            "-m", "pypaimon.sample.robomind_agilex",
            "--input", str(root),
            "--warehouse", str(warehouse),
            "--batch-size", "2",
            "--statistics-version", "synthetic-actions@1",
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout)
    ingest = result["ingest"]
    backfill = result["backfill"]

    assert ingest["mode"] == "local"
    assert ingest["episode_count"] == len(paths)
    assert ingest["frame_count"] == 12
    assert set(ingest) == {
        "mode",
        "episode_count",
        "frame_count",
        "episodes_snapshot_id",
        "frames_snapshot_id",
    }
    assert not _contains_payload(result)
    assert backfill["row_count"] == 12
    assert backfill["statistics_version"] == "synthetic-actions@1"

    episodes, episode_rows = _read(
        warehouse, agilex.EPISODES_TABLE, agilex.episode_schema().names)
    frames, frame_rows = _read(warehouse, agilex.FRAMES_TABLE)
    stats, stats_rows = _read(warehouse, agilex.FEATURE_STATS_TABLE)
    assert episode_rows.num_rows == 4
    assert set(episode_rows["split"].to_pylist()) == {"train", "val"}
    assert set(episode_rows["success"].to_pylist()) == {True, False}
    for table in (episodes, frames, stats):
        options = table.raw_table.table_schema.options
        assert options["deletion-vectors.enabled"] == "true"
        assert options["vector.file.format"] == "vortex"
        assert options["blob-as-descriptor"] == "false"
        assert "file.format" not in agilex.TABLE_OPTIONS
    assert "action" in frames.raw_table.field_names
    assert "act_action_normalized" not in frames.raw_table.field_names

    ordered = frame_rows.select([
        "episode_id",
        "frame_index",
        "action_joint_position_left",
        "action_joint_position_right",
        "action",
    ]).sort_by([
        ("episode_id", "ascending"),
        ("frame_index", "ascending"),
    ])
    for row in ordered.to_pylist():
        expected = np.asarray(
            row["action_joint_position_left"]
            + row["action_joint_position_right"], dtype=np.float32)
        assert np.array_equal(np.asarray(row["action"], dtype=np.float32), expected)

    assert stats_rows.num_rows == 1
    stats_row = stats_rows.to_pylist()[0]
    assert stats_row["statistics_version"] == "synthetic-actions@1"
    assert stats_row["source_snapshot_id"] == backfill["frames_snapshot_id"]
    assert stats_row["source_split"] == "train"
    assert stats_row["frame_count"] == 6
    assert stats_row["standard_deviation_floor"] == 0.01
    assert len(stats_row["action_mean"]) == 14
    assert len(stats_row["action_std"]) == 14

    refreshed_snapshot = agilex.refresh_action_statistics(
        warehouse, statistics_version="synthetic-actions-refresh@1")
    assert refreshed_snapshot > backfill["statistics_snapshot_id"]


@requires_vortex
def test_ray_ingest_matches_local_schema_rows_and_backfill(
        agilex_input, tmp_path):
    ray = pytest.importorskip("ray")

    root, paths = agilex_input
    local_warehouse = tmp_path / "local-comparison"
    ray_warehouse = tmp_path / "ray-comparison"
    agilex.ingest_local(root, local_warehouse, batch_size=2)
    local_backfill = agilex.backfill_canonical_action(
        local_warehouse, statistics_version="synthetic-actions@1")

    ray.init(num_cpus=2, include_dashboard=False)
    try:
        ray_result = agilex.ingest_ray(
            root, ray_warehouse, batch_size=2, concurrency=2)
    finally:
        ray.shutdown()
    ray_backfill = agilex.backfill_canonical_action(
        ray_warehouse, statistics_version="synthetic-actions@1")

    assert ray_result.mode == "ray"
    assert ray_result.episode_count == len(paths)
    assert ray_result.frame_count == 12
    assert ray_backfill.row_count == local_backfill.row_count == 12

    for table_name, schema in (
            (agilex.EPISODES_TABLE, agilex.episode_schema()),
            (agilex.FRAMES_TABLE, agilex.backfilled_frame_schema()),
            (agilex.FEATURE_STATS_TABLE, agilex.feature_stats_schema())):
        local_table, _ = _read(local_warehouse, table_name)
        ray_table, _ = _read(ray_warehouse, table_name)
        assert local_table.raw_table.table_schema.options == (
            ray_table.raw_table.table_schema.options)
        assert _logical_rows(local_warehouse, table_name, schema).equals(
            _logical_rows(ray_warehouse, table_name, schema))


def test_action_statistics_are_independent_of_ingest_row_order():
    frame_count = 257
    base = np.linspace(-0.7, 1.3, frame_count * 14).reshape(
        frame_count, 14)
    values = base + np.sin(base * 17.0) * 0.003
    source = pa.table({
        "episode_id": ["train-a"] * frame_count,
        "frame_index": np.arange(frame_count, dtype=np.int32),
        "action_joint_position_left": values[:, :7].tolist(),
        "action_joint_position_right": values[:, 7:].tolist(),
        "_ROW_ID": np.arange(frame_count, dtype=np.int64),
    })
    reversed_source = source.take(
        pa.array(np.arange(frame_count - 1, -1, -1), type=pa.int64()))

    expected = agilex.build_action_statistics(source.append_column(
        "action", pa.array(values.astype(np.float32).tolist(),
                           type=pa.list_(pa.float32(), 14))), ["train-a"])
    actual = agilex.build_action_statistics(reversed_source.append_column(
        "action", pa.array(values[::-1].astype(np.float32).tolist(),
                           type=pa.list_(pa.float32(), 14))), ["train-a"])

    assert actual == expected
