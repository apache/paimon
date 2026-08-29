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
import tracemalloc
from io import BytesIO
from unittest.mock import patch

import numpy as np
import pytest
import torch
from PIL import Image

import pypaimon.multimodal as pmm
import pypaimon.benchmark.act_harness as act_harness
import pypaimon.benchmark.paired_act as paired_act
from pypaimon.benchmark.paired_act import (
    IMAGE_COLUMNS,
    BenchmarkConfig,
    _git_head,
    _paimon_datasets,
    _shared_normalization,
    _snapshot_id,
    run,
)
from pypaimon.benchmark.act_harness import (
    _SequenceDataset,
    build_window_plan,
    run_backend,
)
from pypaimon.multimodal.query import ScanQuery
from pypaimon.multimodal.window_dataset import ContiguousWindowDataset
from pypaimon.sample import robomind_agilex as agilex


h5py = pytest.importorskip("h5py")


def test_default_fetch_group_covers_eight_logical_batches():
    assert BenchmarkConfig().fetch_batches == 8


def test_sequence_dataset_forwards_plural_access():
    class BatchDataset:
        def __getitems__(self, indices):
            return ["sample-%d" % index for index in indices]

    dataset = _SequenceDataset(BatchDataset(), (7, 3, 5))

    assert dataset.__getitems__([0, 2]) == ["sample-7", "sample-5"]


def test_logical_batches_coalesce_one_physical_fetch():
    class BatchDataset:
        def __init__(self):
            self.calls = []

        def __getitems__(self, indices):
            self.calls.append(list(indices))
            return [{"value": torch.tensor(index)} for index in indices]

    dataset = BatchDataset()

    batches = list(act_harness._iter_logical_batches(
        dataset,
        tuple(range(8)),
        logical_batch_size=2,
        fetch_batches=4,
    ))

    assert dataset.calls == [list(range(8))]
    assert [batch["value"].tolist() for batch in batches] == [
        [0, 1], [2, 3], [4, 5], [6, 7],
    ]


def test_logical_batches_reject_partial_checkpoint_tail():
    class BatchDataset:
        def __getitems__(self, indices):
            return [{"value": torch.tensor(index)} for index in indices]

    with pytest.raises(ValueError, match="complete logical batches"):
        list(act_harness._iter_logical_batches(
            BatchDataset(),
            tuple(range(9)),
            logical_batch_size=2,
            fetch_batches=4,
        ))


def test_backend_times_without_tracemalloc_and_measures_memory_separately():
    states = []
    config = BenchmarkConfig(
        seed=11,
        action_horizon=1,
        batch_size=1,
        optimizer_steps=1,
        image_height=2,
        image_width=2,
        warmup_batches=1,
        loader_batches=1,
        rounds=3,
    )

    class TracingDataset(torch.utils.data.Dataset):
        def __len__(self):
            return 2

        def __getitem__(self, index):
            states.append(tracemalloc.is_tracing())
            return {
                "sample_id": "episode-a#%d" % index,
                "episode_id": "episode-a",
                "step_idx": index,
                "qpos": torch.zeros(14),
                "action": torch.zeros((1, 14)),
                "images": torch.zeros((3, 3, 2, 2)),
                "is_pad": torch.zeros(1, dtype=torch.bool),
            }

    dataset = TracingDataset()
    plan = build_window_plan(len(dataset), len(dataset), config)
    result = run_backend(
        "test",
        1,
        lambda: (dataset, dataset),
        plan,
        config,
        "sequence-sha256",
        policy_factory=_policy_factory,
    )

    assert states[0] is False
    assert states[-1] is True
    assert result["peak_memory_measurement"] == (
        "python-tracemalloc-separate-dataset-first-batch")


def test_backend_coalesces_timed_loader_fetches():
    config = BenchmarkConfig(
        seed=11,
        action_horizon=1,
        batch_size=2,
        optimizer_steps=1,
        image_height=2,
        image_width=2,
        warmup_batches=1,
        loader_batches=4,
        fetch_batches=4,
        rounds=3,
    )

    class BatchDataset(torch.utils.data.Dataset):
        def __init__(self):
            self.calls = []

        def __len__(self):
            return 16

        def __getitem__(self, index):
            return {
                "sample_id": "episode-a#%d" % index,
                "episode_id": "episode-a",
                "step_idx": index,
                "qpos": torch.zeros(14),
                "action": torch.zeros((1, 14)),
                "images": torch.zeros((3, 3, 2, 2)),
                "is_pad": torch.zeros(1, dtype=torch.bool),
            }

        def __getitems__(self, indices):
            self.calls.append(list(indices))
            return [self[index] for index in indices]

    dataset = BatchDataset()
    plan = build_window_plan(len(dataset), len(dataset), config)

    run_backend(
        "test",
        1,
        lambda: (dataset, dataset),
        plan,
        config,
        "sequence-sha256",
        policy_factory=_policy_factory,
    )

    assert any(len(indices) == 8 for indices in dataset.calls)


def _jpeg(value):
    buffer = BytesIO()
    Image.fromarray(np.full((8, 10, 3), value, dtype=np.uint8)).save(
        buffer, format="JPEG")
    return np.frombuffer(buffer.getvalue(), dtype=np.uint8)


def _write_episode(root, split, name, offset, frames=6):
    path = (root / "13_packbowl" / "success_episodes" / split / name
            / "data" / "trajectory.hdf5")
    path.parent.mkdir(parents=True)
    with h5py.File(path, "w") as h5:
        h5.create_dataset("language_raw", data=[b"pack the bowl"])
        h5.create_dataset(
            "language_distilbert",
            data=np.zeros((1, 1, 768), dtype=np.float16),
        )
        for index, (_, hdf5_path) in enumerate(agilex.NUMERIC_FIELDS):
            values = np.arange(frames * 7, dtype=np.float64).reshape(frames, 7)
            h5.create_dataset(hdf5_path, data=values + offset + index * 100)
        variable = h5py.vlen_dtype(np.dtype("uint8"))
        for image_index, (_, hdf5_path) in enumerate(agilex.IMAGE_FIELDS):
            dataset = h5.create_dataset(hdf5_path, (frames,), dtype=variable)
            for frame_index in range(frames):
                dataset[frame_index] = _jpeg(
                    offset + image_index + frame_index)
    return path


@pytest.fixture
def paired_input(tmp_path):
    root = tmp_path / "input"
    _write_episode(root, "train", "train-a", 1)
    _write_episode(root, "train", "train-b", 11)
    _write_episode(root, "val", "val-a", 21)
    warehouse = tmp_path / "warehouse"
    agilex.ingest_local(root, warehouse, batch_size=2)
    agilex.backfill_canonical_action(
        warehouse, statistics_version="paired-test@1")
    return root, warehouse


class _Policy(torch.nn.Module):

    def __init__(self):
        super().__init__()
        self.scale = torch.nn.Parameter(torch.tensor(0.0))

    def forward(self, batch):
        assert self.training
        target = batch["action"].mean() + batch["observation.state"].mean()
        loss = (self.scale - target).square()
        return loss, {
            "l1_loss": loss.detach(),
            "kld_loss": torch.tensor(0.0),
        }


def _policy_factory(config):
    return _Policy(), {
        "implementation": "test-policy",
        "chunk_size": config.action_horizon,
        "parameter_count": 1,
    }


def test_runs_three_alternating_rounds_with_one_shared_contract(
        paired_input, tmp_path):
    input_root, warehouse = paired_input
    report_path = tmp_path / "paired-report.json"
    config = BenchmarkConfig(
        seed=17,
        action_horizon=3,
        batch_size=2,
        optimizer_steps=2,
        image_height=8,
        image_width=10,
        warmup_batches=1,
        loader_batches=2,
        rounds=3,
    )

    report = run(
        input_root,
        warehouse,
        report_path,
        config=config,
        statistics_version="paired-test@1",
        policy_factory=_policy_factory,
    )

    assert report_path.exists()
    assert json.loads(report_path.read_text()) == report
    assert report["schema_version"] == "robomind-paired-act-benchmark@1"
    assert report["status"] == "SUCCEEDED"
    assert report["parameters"]["config"] == config.to_dict()
    assert report["parameters"]["cache_control"] == "uncontrolled"
    assert report["input"]["paimon_window_dataset"] == (
        "pypaimon.multimodal.ContiguousWindowDataset")
    assert report["input"]["paimon_window_snapshot_id"] == (
        report["input"]["frames_snapshot_id"])
    assert report["execution_order"] == [
        "hdf5", "paimon", "paimon", "hdf5", "hdf5", "paimon",
    ]
    assert len(report["runs"]) == 6
    assert all(report["correctness"]["checks"].values())
    assert report["correctness"]["tensor_parity"]["passed"]
    assert report["correctness"]["tensor_parity"][
        "checked_window_count"] > 0
    assert (
        report["correctness"]["tensor_parity"]["max_absolute_difference"]
        == {
            "qpos": 0.0,
            "action": 0.0,
            "images": 0.0,
        }
    )
    assert report["correctness"]["loss_parity"]["passed"]
    assert all(
        comparison["train_loss_exact"]
        and comparison["validation_loss_exact"]
        for comparison in report["correctness"]["loss_parity"]["rounds"]
    )
    assert report["window_plan"]["seed"] == 17
    assert len(report["window_plan"]["sha256"]) == 64
    assert report["normalization"]["statistics_version"] == "paired-test@1"
    assert len(report["normalization"]["sha256"]) == 64
    assert set(report["summary"]) == {"hdf5", "paimon"}
    for backend in ("hdf5", "paimon"):
        assert report["summary"][backend]["round_count"] == 3
        for metric in (
                "first_batch_s",
                "dataloader_samples_per_s",
                "fixed_steps_s",
                "python_peak_allocated_bytes"):
            assert set(report["summary"][backend][metric]) == {
                "median", "min", "max",
            }
    for round_index in range(3):
        paired = [item for item in report["runs"]
                  if item["round"] == round_index + 1]
        by_backend = {item["backend"]: item for item in paired}
        assert by_backend["hdf5"]["sample_sequence_sha256"] == (
            by_backend["paimon"]["sample_sequence_sha256"])
        assert by_backend["hdf5"]["train_loss"] == (
            by_backend["paimon"]["train_loss"])
        assert by_backend["hdf5"]["validation_loss"] == (
            by_backend["paimon"]["validation_loss"])


def test_paimon_windows_are_lazy_and_snapshot_pinned(paired_input):
    input_root, warehouse = paired_input
    connection = pmm.connect(
        database=agilex.DEFAULT_DATABASE,
        options={"warehouse": str(warehouse)},
    )
    frames = connection.get_table(agilex.FRAMES_TABLE)
    snapshot_id = _snapshot_id(frames)
    normalization, _ = _shared_normalization(
        agilex.discover_episodes(input_root),
        connection,
        snapshot_id,
        "paired-test@1",
    )

    original = ScanQuery._fetch_bodies
    with patch.object(
            ScanQuery, "_fetch_bodies", side_effect=original) as fetch:
        train, validation = _paimon_datasets(
            frames,
            snapshot_id,
            "train-a",
            "val-a",
            normalization,
            BenchmarkConfig(
                action_horizon=3,
                batch_size=1,
                optimizer_steps=1,
                image_height=8,
                image_width=10,
                rounds=3,
            ),
        )
        assert fetch.call_count == 0
        assert isinstance(train, ContiguousWindowDataset)
        assert isinstance(validation, ContiguousWindowDataset)
        sample_before_append = train[0]
        assert fetch.call_count == 1
        assert {
            name: len(fetch.call_args.args[1][name])
            for name in IMAGE_COLUMNS
        } == {name: 1 for name in IMAGE_COLUMNS}

    scalar, blobs = frames.scan().where(
        "episode_id = 'train-a' AND frame_index = 5"
    ).read_blobs(IMAGE_COLUMNS)
    appended = scalar.to_pylist()[0]
    appended["frame_index"] = 6
    for name in IMAGE_COLUMNS:
        appended[name] = blobs[name][0]
    frames.add([appended])

    assert train.snapshot_id == snapshot_id
    assert validation.snapshot_id == snapshot_id
    assert _snapshot_id(frames) != snapshot_id
    assert len(train) == 4
    sample_after_append = train[0]
    for name in ("qpos", "action", "images", "is_pad"):
        assert torch.equal(
            sample_before_append[name], sample_after_append[name])


def test_tensor_parity_rejects_different_hdf5_bytes(
        paired_input, tmp_path):
    input_root, warehouse = paired_input
    changed = (input_root / "13_packbowl" / "success_episodes" / "train"
               / "train-a" / "data" / "trajectory.hdf5")
    with h5py.File(changed, "r+") as h5:
        h5["puppet/joint_position_left"][0, 0] += 1

    with pytest.raises(AssertionError, match="tensor differs"):
        run(
            input_root,
            warehouse,
            tmp_path / "must-not-exist.json",
            config=BenchmarkConfig(
                action_horizon=3,
                batch_size=1,
                optimizer_steps=1,
                image_height=8,
                image_width=10,
                rounds=3,
            ),
            statistics_version="paired-test@1",
            policy_factory=_policy_factory,
        )


def test_requires_at_least_three_alternating_rounds():
    with pytest.raises(ValueError, match="rounds must be at least 3"):
        BenchmarkConfig(rounds=2)


def test_fetch_batches_must_be_positive():
    assert BenchmarkConfig(fetch_batches=4).fetch_batches == 4
    with pytest.raises(ValueError, match="fetch_batches must be a positive int"):
        BenchmarkConfig(fetch_batches=0)


def test_cli_documents_physical_fetch_batches(capsys):
    with pytest.raises(SystemExit):
        paired_act.main(["--help"])

    assert "--fetch-batches" in capsys.readouterr().out


def test_source_commit_falls_back_outside_git_checkout(tmp_path):
    with patch(
            "pypaimon.benchmark.paired_act.subprocess.check_output",
            side_effect=FileNotFoundError):
        assert _git_head(tmp_path) == "UNKNOWN"
