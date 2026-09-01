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

# ruff: noqa: E402

import json
import tracemalloc
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pytest


torch = pytest.importorskip("torch")
Image = pytest.importorskip("PIL.Image")
h5py = pytest.importorskip("h5py")

import pypaimon.multimodal as pmm
import pypaimon.benchmark.act.harness as act_harness
import pypaimon.benchmark.act.__main__ as act_cli
from pypaimon.benchmark.act.runner import (
    BenchmarkConfig,
    _git_head,
    prepare_experiment,
    run_experiment,
)
from pypaimon.benchmark.act.experiment import load_experiment
from pypaimon.benchmark.act.compare import canonical_sha256, compare_results
from pypaimon.benchmark.act.harness import build_window_plan, run_backend
from pypaimon.benchmark.act.hdf5 import Hdf5ACTWindowDataset
from pypaimon.benchmark.act.paimon import (
    ACTION_COLUMNS,
    IMAGE_COLUMNS,
    QPOS_COLUMNS,
    create_datasets as create_paimon_datasets,
    latest_snapshot_id,
)
from pypaimon.multimodal.query import ScanQuery
from pypaimon.multimodal.window_dataset import ContiguousWindowDataset
from pypaimon.sample import robomind_agilex as agilex


def test_default_fetch_group_covers_eight_logical_batches():
    assert BenchmarkConfig().fetch_batches == 8


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


def test_logical_batches_reject_incomplete_batch_tail():
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
        timed_batches=1,
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


def test_backend_coalesces_timed_batch_fetches():
    config = BenchmarkConfig(
        seed=11,
        action_horizon=1,
        batch_size=2,
        optimizer_steps=1,
        image_height=2,
        image_width=2,
        warmup_batches=1,
        timed_batches=4,
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

    with patch.object(act_harness, "_measure_python_peak", return_value=0):
        run_backend(
            "test",
            1,
            lambda: (dataset, dataset),
            plan,
            config,
            "sequence-sha256",
            policy_factory=_policy_factory,
        )

    assert dataset.calls == [
        list(plan.measurement_indices[:2]),
        list(plan.measurement_indices[2:10]),
        list(plan.train_indices),
        list(plan.validation_indices),
    ]


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
def benchmark_input(tmp_path, monkeypatch):
    root = tmp_path / "input"
    _write_episode(root, "train", "train-a", 1)
    _write_episode(root, "train", "train-b", 11)
    _write_episode(root, "val", "val-a", 21)
    warehouse = tmp_path / "warehouse"
    monkeypatch.setattr(agilex, "TABLE_OPTIONS", {
        **agilex.TABLE_OPTIONS,
        "vector.file.format": "parquet",
    })
    agilex.ingest_local(root, warehouse, batch_size=2)
    agilex.backfill_canonical_action(
        warehouse, statistics_version="act-test@1")
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


def test_prepare_writes_resolved_experiment(benchmark_input, tmp_path):
    input_root, warehouse = benchmark_input
    definition = load_experiment()
    definition["statistics_version"] = "act-test@1"
    definition["config"].update({
        "seed": 17,
        "action_horizon": 3,
        "batch_size": 2,
        "optimizer_steps": 2,
        "image_height": 8,
        "image_width": 10,
        "timed_batches": 2,
    })
    output = tmp_path / "experiment.json"

    experiment = prepare_experiment(
        input_root, warehouse, output, definition=definition)

    assert json.loads(output.read_text()) == experiment
    assert experiment["schema_version"] == "act-benchmark-experiment@1"
    assert experiment["train_episode_id"] == "train-a"
    assert experiment["validation_episode_id"] == "val-a"
    assert experiment["source"]["episodes"][0]["frame_count"] == 6
    assert len(experiment["source"]["sha256"]) == 64
    assert len(experiment["normalization"]["sha256"]) == 64
    assert len(experiment["window_plan"]["sha256"]) == 64
    assert experiment["paimon"]["frames_snapshot_id"] > 0


def test_hdf5_run_consumes_resolved_experiment_without_warehouse(
        benchmark_input, tmp_path):
    input_root, warehouse = benchmark_input
    definition = load_experiment()
    definition["statistics_version"] = "act-test@1"
    definition["config"].update({
        "seed": 17,
        "action_horizon": 3,
        "batch_size": 2,
        "optimizer_steps": 2,
        "image_height": 8,
        "image_width": 10,
        "timed_batches": 2,
    })
    experiment_path = tmp_path / "experiment.json"
    experiment = prepare_experiment(
        input_root, warehouse, experiment_path, definition=definition)
    result_path = tmp_path / "hdf5-result.json"

    result = run_experiment(
        "hdf5",
        experiment_path,
        result_path,
        input_root=input_root,
        policy_factory=_policy_factory,
    )

    assert json.loads(result_path.read_text()) == result
    assert result["schema_version"] == "act-benchmark-result@1"
    assert result["status"] == "SUCCEEDED"
    assert result["backend"] == "hdf5"
    assert result["experiment"] == experiment
    assert len(result["experiment_sha256"]) == 64
    assert len(result["tensor_fingerprint"]["sha256"]) == 64
    assert len(result["runs"]) == 3
    assert result["summary"]["round_count"] == 3


def test_independent_backend_results_preserve_tensor_and_loss_parity(
        benchmark_input, tmp_path):
    input_root, warehouse = benchmark_input
    definition = load_experiment()
    definition["statistics_version"] = "act-test@1"
    definition["config"].update({
        "seed": 17,
        "action_horizon": 3,
        "batch_size": 2,
        "optimizer_steps": 2,
        "image_height": 8,
        "image_width": 10,
        "timed_batches": 2,
    })
    experiment_path = tmp_path / "experiment.json"
    prepare_experiment(
        input_root, warehouse, experiment_path, definition=definition)

    hdf5_result = run_experiment(
        "hdf5",
        experiment_path,
        tmp_path / "hdf5-result.json",
        input_root=input_root,
        policy_factory=_policy_factory,
    )
    paimon_result = run_experiment(
        "paimon",
        experiment_path,
        tmp_path / "paimon-result.json",
        warehouse=warehouse,
        policy_factory=_policy_factory,
    )

    assert hdf5_result["tensor_fingerprint"] == (
        paimon_result["tensor_fingerprint"])
    assert [run["train_loss"] for run in hdf5_result["runs"]] == [
        run["train_loss"] for run in paimon_result["runs"]
    ]
    assert [run["validation_loss"] for run in hdf5_result["runs"]] == [
        run["validation_loss"] for run in paimon_result["runs"]
    ]
    comparison = compare_results([hdf5_result, paimon_result])
    assert comparison["status"] == "SUCCEEDED"
    assert comparison["experiments"][0]["backends"] == ["hdf5", "paimon"]


def test_backends_match_the_golden_act_window_contract(benchmark_input):
    input_root, warehouse = benchmark_input
    normalization = {
        "qpos_mean": np.zeros(14, dtype=np.float32),
        "qpos_std": np.ones(14, dtype=np.float32),
        "action_mean": np.zeros(14, dtype=np.float32),
        "action_std": np.ones(14, dtype=np.float32),
    }
    hdf5 = Hdf5ACTWindowDataset(
        SimpleNamespace(
            path=(input_root / "13_packbowl" / "success_episodes" / "train"
                  / "train-a" / "data" / "trajectory.hdf5"),
            episode_id="train-a",
            frame_count=6,
        ),
        normalization,
        action_horizon=3,
    )
    connection = pmm.connect(
        database=agilex.DEFAULT_DATABASE,
        options={"warehouse": str(warehouse)},
    )
    frames = connection.get_table(agilex.FRAMES_TABLE)
    paimon, _ = create_paimon_datasets(
        frames,
        latest_snapshot_id(frames),
        "train-a",
        "val-a",
        normalization,
        BenchmarkConfig(action_horizon=3),
    )

    expected = hdf5[1]
    actual = paimon[1]

    assert set(expected) == {
        "sample_id", "episode_id", "step_idx", "qpos", "action",
        "images", "is_pad",
    }
    assert expected["sample_id"] == "train-a#1"
    assert expected["episode_id"] == "train-a"
    assert expected["step_idx"] == 1
    assert torch.equal(expected["qpos"], torch.tensor(
        list(range(408, 415)) + list(range(508, 515)),
        dtype=torch.float32,
    ))
    assert torch.equal(expected["action"], torch.tensor([
        list(range(1208, 1215)) + list(range(1308, 1315)),
        list(range(1215, 1222)) + list(range(1315, 1322)),
        list(range(1222, 1229)) + list(range(1322, 1329)),
    ], dtype=torch.float32))
    assert torch.allclose(
        expected["images"][:, :, 0, 0],
        torch.tensor([[2 / 255] * 3, [3 / 255] * 3, [4 / 255] * 3]),
    )
    assert not expected["is_pad"].any()
    for name in ("qpos", "action", "images", "is_pad"):
        assert torch.equal(expected[name], actual[name])
    for name in ("sample_id", "episode_id", "step_idx"):
        assert expected[name] == actual[name]


def test_hdf5_window_index_bounds(tmp_path):
    path = _write_episode(tmp_path, "train", "train-a", 1)
    dataset = Hdf5ACTWindowDataset(
        SimpleNamespace(
            path=path,
            episode_id="train-a",
            frame_count=6,
        ),
        {
            "qpos_mean": np.zeros(14, dtype=np.float32),
            "qpos_std": np.ones(14, dtype=np.float32),
            "action_mean": np.zeros(14, dtype=np.float32),
            "action_std": np.ones(14, dtype=np.float32),
        },
        action_horizon=3,
    )

    assert dataset[-1]["sample_id"] == "train-a#3"
    with pytest.raises(IndexError):
        dataset[-len(dataset) - 1]
    with pytest.raises(IndexError):
        dataset[len(dataset)]


def test_paimon_run_rejects_normalization_not_recorded_in_statistics(
        benchmark_input, tmp_path):
    input_root, warehouse = benchmark_input
    definition = load_experiment()
    definition["statistics_version"] = "act-test@1"
    definition["config"].update({
        "action_horizon": 3,
        "batch_size": 1,
        "optimizer_steps": 1,
        "image_height": 8,
        "image_width": 10,
    })
    experiment_path = tmp_path / "experiment.json"
    experiment = prepare_experiment(
        input_root, warehouse, experiment_path, definition=definition)
    experiment["normalization"]["values"]["action_mean"][0] += 1
    experiment["normalization"]["sha256"] = canonical_sha256(
        experiment["normalization"]["values"])
    experiment_path.write_text(json.dumps(experiment))

    with pytest.raises(ValueError, match="normalization differs"):
        run_experiment(
            "paimon",
            experiment_path,
            tmp_path / "must-not-exist.json",
            warehouse=warehouse,
            policy_factory=_policy_factory,
        )


def test_run_rejects_tampered_source_manifest(benchmark_input, tmp_path):
    input_root, warehouse = benchmark_input
    definition = load_experiment()
    definition["statistics_version"] = "act-test@1"
    definition["config"].update({
        "action_horizon": 3,
        "batch_size": 1,
        "optimizer_steps": 1,
        "image_height": 8,
        "image_width": 10,
    })
    experiment_path = tmp_path / "experiment.json"
    experiment = prepare_experiment(
        input_root, warehouse, experiment_path, definition=definition)
    experiment["source"]["episodes"][0]["frame_count"] += 1
    experiment_path.write_text(json.dumps(experiment))

    with pytest.raises(ValueError, match="source-manifest hash differs"):
        run_experiment(
            "hdf5",
            experiment_path,
            tmp_path / "must-not-exist.json",
            input_root=input_root,
            policy_factory=_policy_factory,
        )


def test_run_rejects_config_not_used_to_build_window_plan(
        benchmark_input, tmp_path):
    input_root, warehouse = benchmark_input
    definition = load_experiment()
    definition["statistics_version"] = "act-test@1"
    definition["config"].update({
        "action_horizon": 3,
        "batch_size": 1,
        "optimizer_steps": 1,
        "image_height": 8,
        "image_width": 10,
    })
    experiment_path = tmp_path / "experiment.json"
    experiment = prepare_experiment(
        input_root, warehouse, experiment_path, definition=definition)
    experiment["config"]["seed"] += 1
    experiment_path.write_text(json.dumps(experiment))

    with pytest.raises(ValueError, match="window plan was not built"):
        run_experiment(
            "hdf5",
            experiment_path,
            tmp_path / "must-not-exist.json",
            input_root=input_root,
            policy_factory=_policy_factory,
        )


def test_paimon_windows_are_lazy_snapshot_pinned_and_vortex_independent(
        benchmark_input, tmp_path):
    input_root, warehouse = benchmark_input
    definition = load_experiment()
    definition["statistics_version"] = "act-test@1"
    definition["config"]["action_horizon"] = 3
    experiment = prepare_experiment(
        input_root,
        warehouse,
        tmp_path / "experiment.json",
        definition=definition,
    )
    normalization = {
        name: np.asarray(value, dtype=np.float32)
        for name, value in experiment["normalization"]["values"].items()
    }
    connection = pmm.connect(
        database=agilex.DEFAULT_DATABASE,
        options={"warehouse": str(warehouse)},
    )
    frames = connection.get_table(agilex.FRAMES_TABLE)
    assert frames.raw_table.table_schema.options["vector.file.format"] == (
        "parquet")
    snapshot_id = latest_snapshot_id(frames)

    original = ScanQuery._fetch_bodies
    with patch.object(
            ScanQuery, "_fetch_bodies", side_effect=original) as fetch:
        train, validation = create_paimon_datasets(
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
        with patch.object(
                train, "_read_rows", wraps=train._read_rows) as read_rows:
            sample_before_append = train[0]
        assert [call.args[1] for call in read_rows.call_args_list] == [
            list(ACTION_COLUMNS),
            list(QPOS_COLUMNS + IMAGE_COLUMNS),
        ]
        assert [len(call.args[0]) for call in read_rows.call_args_list] == [3, 1]
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
    assert latest_snapshot_id(frames) != snapshot_id
    assert len(train) == 4
    sample_after_append = train[0]
    for name in ("qpos", "action", "images", "is_pad"):
        assert torch.equal(
            sample_before_append[name], sample_after_append[name])


def test_compare_rejects_different_hdf5_tensor_bytes(
        benchmark_input, tmp_path):
    input_root, warehouse = benchmark_input
    definition = load_experiment()
    definition["statistics_version"] = "act-test@1"
    definition["config"].update({
        "action_horizon": 3,
        "batch_size": 1,
        "optimizer_steps": 1,
        "image_height": 8,
        "image_width": 10,
    })
    experiment_path = tmp_path / "experiment.json"
    prepare_experiment(
        input_root, warehouse, experiment_path, definition=definition)
    paimon_result = run_experiment(
        "paimon",
        experiment_path,
        tmp_path / "paimon-result.json",
        warehouse=warehouse,
        policy_factory=_policy_factory,
    )
    changed = (input_root / "13_packbowl" / "success_episodes" / "train"
               / "train-a" / "data" / "trajectory.hdf5")
    with h5py.File(changed, "r+") as h5:
        h5["puppet/joint_position_left"][0, 0] += 1

    hdf5_result = run_experiment(
        "hdf5",
        experiment_path,
        tmp_path / "hdf5-result.json",
        input_root=input_root,
        policy_factory=_policy_factory,
    )

    comparison = compare_results([hdf5_result, paimon_result])
    assert comparison["status"] == "FAILED"
    assert comparison["experiments"][0]["reason"] == (
        "tensor fingerprints differ")


def test_requires_at_least_three_measurement_rounds():
    with pytest.raises(ValueError, match="rounds must be at least 3"):
        BenchmarkConfig(rounds=2)


def test_fetch_batches_must_be_positive():
    assert BenchmarkConfig(fetch_batches=4).fetch_batches == 4
    with pytest.raises(ValueError, match="fetch_batches must be a positive int"):
        BenchmarkConfig(fetch_batches=0)


def test_cli_exposes_prepare_run_and_compare_contracts(capsys):
    with pytest.raises(SystemExit):
        act_cli.main(["prepare", "--help"])

    prepare_help = capsys.readouterr().out
    assert "--experiment" in prepare_help
    assert "--fetch-batches" in prepare_help

    with pytest.raises(SystemExit):
        act_cli.main(["run", "--help"])

    run_help = capsys.readouterr().out
    assert "--backend" in run_help
    assert "--experiment" in run_help
    assert "--results-dir" in run_help

    with pytest.raises(SystemExit):
        act_cli.main(["compare", "--help"])

    assert "--results-dir" in capsys.readouterr().out


def test_automatic_artifact_paths_do_not_overwrite_same_second(tmp_path):
    with patch.object(act_cli, "datetime") as now:
        now.now.return_value.strftime.return_value = "20260901T120000Z"

        first = act_cli._artifact_path(tmp_path, "robomind-act-hdf5")
        second = act_cli._artifact_path(tmp_path, "robomind-act-hdf5")

    assert first != second
    assert first.parent == tmp_path
    assert second.parent == tmp_path


def test_source_commit_falls_back_outside_git_checkout(tmp_path):
    with patch(
            "pypaimon.benchmark.act.runner.subprocess.check_output",
            side_effect=FileNotFoundError):
        assert _git_head(tmp_path) == "UNKNOWN"
