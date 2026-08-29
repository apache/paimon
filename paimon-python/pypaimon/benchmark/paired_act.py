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

"""Paired RoboMIND ACT benchmark over original HDF5 and Paimon.

Both adapters consume one immutable :class:`BenchmarkConfig`, one train-only
normalization object, and one explicit window plan. The runner resets the same
seed before constructing the same LeRobot ACT policy and AdamW trainer for each
backend. It measures three alternating rounds without attempting OS cache
control and writes tensor and loss parity alongside timing and memory evidence.
Ingestion and canonical-action backfill are deliberately outside the benchmark.
"""

import argparse
import gc
import hashlib
import json
import os
import platform
import subprocess
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import torch
from torch.utils.data import Dataset

import pypaimon.multimodal as pmm
from pypaimon.benchmark.act_harness import (
    BenchmarkConfig,
    build_window_plan,
    decode_rgb_image,
    run_backend,
)
from pypaimon.sample import robomind_agilex as agilex


QPOS_COLUMNS = (
    "state_joint_position_left",
    "state_joint_position_right",
)
ACTION_COLUMNS = ("action",)
IMAGE_COLUMNS = (
    "rgb_front",
    "rgb_left_wrist",
    "rgb_right_wrist",
)
HDF5_QPOS_FIELDS = (
    "puppet/joint_position_left",
    "puppet/joint_position_right",
)
HDF5_ACTION_FIELDS = (
    "master/joint_position_left",
    "master/joint_position_right",
)
HDF5_IMAGE_FIELDS = (
    "observations/rgb_images/camera_front",
    "observations/rgb_images/camera_left_wrist",
    "observations/rgb_images/camera_right_wrist",
)


@dataclass(frozen=True)
class _BenchmarkEpisode:
    path: Path
    source_key: str
    episode_id: str
    split: str
    success: bool
    frame_count: int


class Hdf5ACTWindowDataset(Dataset):
    """Map-style complete ACT windows read on demand from one HDF5 episode."""

    def __init__(self, episode, normalization, action_horizon):
        self.episode = episode
        self.normalization = normalization
        self.action_horizon = action_horizon
        self.window_count = episode.frame_count - action_horizon + 1
        if self.window_count <= 0:
            raise ValueError(
                "Episode %s is shorter than action horizon %d."
                % (episode.episode_id, action_horizon))

    def __len__(self):
        return self.window_count

    def __getitem__(self, anchor):
        if anchor < 0:
            anchor += self.window_count
        if anchor < 0 or anchor >= self.window_count:
            raise IndexError(anchor)
        import h5py

        with h5py.File(str(self.episode.path), "r") as h5:
            qpos = _read_vectors(h5, HDF5_QPOS_FIELDS, anchor)
            action = _read_vectors(
                h5,
                HDF5_ACTION_FIELDS,
                slice(anchor, anchor + self.action_horizon),
            )
            images = np.stack([
                _decode_hdf5_image(h5[field][anchor])
                for field in HDF5_IMAGE_FIELDS
            ])
        qpos = (
            (qpos - self.normalization["qpos_mean"])
            / self.normalization["qpos_std"])
        action = (
            (action - self.normalization["action_mean"])
            / self.normalization["action_std"])
        return {
            "sample_id": "%s#%d" % (self.episode.episode_id, anchor),
            "episode_id": self.episode.episode_id,
            "step_idx": anchor,
            "qpos": torch.from_numpy(np.ascontiguousarray(qpos)),
            "action": torch.from_numpy(np.ascontiguousarray(action)),
            "images": torch.from_numpy(np.ascontiguousarray(images)),
            "is_pad": torch.zeros(self.action_horizon, dtype=torch.bool),
        }


class _PaimonACTAdapter:
    """Adapt one generic Paimon row window to the shared ACT contract."""

    def __init__(self, normalization):
        self.normalization = normalization

    def __call__(self, sample):
        qpos = np.concatenate([
            np.asarray(sample[name][0], dtype=np.float32)
            for name in QPOS_COLUMNS
        ])
        action = np.concatenate([
            np.asarray(sample[name], dtype=np.float32)
            for name in ACTION_COLUMNS
        ], axis=-1)
        images = np.stack([
            _decode_image(sample[name][0])
            for name in IMAGE_COLUMNS
        ])
        qpos = (
            (qpos - self.normalization["qpos_mean"])
            / self.normalization["qpos_std"])
        action = (
            (action - self.normalization["action_mean"])
            / self.normalization["action_std"])
        episode_id = sample["episode_id"]
        step_idx = sample["frame_index"]
        return {
            "sample_id": "%s#%d" % (episode_id, step_idx),
            "episode_id": episode_id,
            "step_idx": step_idx,
            "qpos": torch.from_numpy(np.ascontiguousarray(qpos)),
            "action": torch.from_numpy(np.ascontiguousarray(action)),
            "images": torch.from_numpy(np.ascontiguousarray(images)),
            "is_pad": sample["is_pad"],
        }


def run(
        input_root,
        warehouse,
        report_path,
        *,
        config=None,
        database=agilex.DEFAULT_DATABASE,
        statistics_version=agilex.DEFAULT_STATISTICS_VERSION,
        train_episode_id=None,
        validation_episode_id=None,
        policy_factory=None):
    """Run the paired benchmark without performing ingest or backfill."""
    config = config or BenchmarkConfig()
    if not isinstance(config, BenchmarkConfig):
        raise TypeError("config must be a BenchmarkConfig.")
    started_at = _utc_now()
    started = time.monotonic()
    input_root = Path(input_root).expanduser().resolve()
    warehouse = Path(warehouse).expanduser().resolve()
    report_path = Path(report_path).expanduser().resolve()

    discovered_episodes = agilex.discover_episodes(input_root)
    connection = pmm.connect(
        database=database, options={"warehouse": str(warehouse)})
    episode_rows = _episode_rows(connection)
    source_episodes, source_identity_sha256 = _validate_source_identity(
        discovered_episodes, episode_rows)
    source_by_id = {episode.episode_id: episode for episode in source_episodes}
    frames = connection.get_table(agilex.FRAMES_TABLE)
    frames_snapshot_id = _snapshot_id(frames)

    normalization, normalization_metadata = _shared_normalization(
        source_episodes,
        connection,
        frames_snapshot_id,
        statistics_version,
    )
    train_episode = _select_episode(
        source_by_id,
        split="train",
        requested=train_episode_id,
        action_horizon=config.action_horizon,
    )
    validation_episode = _select_episode(
        source_by_id,
        split="val",
        requested=validation_episode_id,
        action_horizon=config.action_horizon,
    )
    plan = build_window_plan(
        train_episode.frame_count - config.action_horizon + 1,
        validation_episode.frame_count - config.action_horizon + 1,
        config,
    )
    sequence_sha256 = _sample_sequence_sha256(
        train_episode.episode_id,
        validation_episode.episode_id,
        plan,
    )

    factories = {
        "hdf5": lambda: _hdf5_datasets(
            train_episode, validation_episode, normalization, config),
        "paimon": lambda: _paimon_datasets(
            frames,
            frames_snapshot_id,
            train_episode.episode_id,
            validation_episode.episode_id,
            normalization,
            config,
        ),
    }
    tensor_parity = _tensor_parity(
        factories["hdf5"](), factories["paimon"](), plan)
    del source_by_id
    gc.collect()

    runs = []
    execution_order = []
    for round_index in range(config.rounds):
        order = (
            ("hdf5", "paimon")
            if round_index % 2 == 0 else ("paimon", "hdf5"))
        for backend in order:
            execution_order.append(backend)
            runs.append(run_backend(
                backend,
                round_index + 1,
                factories[backend],
                plan,
                config,
                sequence_sha256,
                policy_factory=policy_factory,
            ))
            gc.collect()

    loss_parity = _loss_parity(runs, config.rounds)
    checks = {
        "source_hdf5_matches_paimon": True,
        "versioned_action_normalization_matches_hdf5": True,
        "shared_normalization_object": True,
        "shared_config": True,
        "shared_seed": True,
        "paimon_windows_snapshot_pinned": True,
        "shared_window_sequence": len({
            item["sample_sequence_sha256"] for item in runs
        }) == 1,
        "tensor_parity": tensor_parity["passed"],
        "train_and_validation_loss_parity": loss_parity["passed"],
        "three_or_more_alternating_rounds": (
            config.rounds >= 3
            and execution_order == _expected_order(config.rounds)),
        "all_losses_finite": all(
            np.isfinite(value)
            for item in runs
            for value in item["train_loss"] + [item["validation_loss"]]),
    }
    status = "SUCCEEDED" if all(checks.values()) else "FAILED"
    report = {
        "schema_version": "robomind-paired-act-benchmark@1",
        "benchmark_id": "M0-paired-ACT",
        "run_id": "%s-%s" % (
            started_at.replace(":", "").replace("-", ""),
            uuid.uuid4().hex[:8],
        ),
        "status": status,
        "scope": "paired CPU ACT training path; ingest and backfill excluded",
        "input": {
            "dataset": "RoboMIND AgileX",
            "input_manifest_sha256": source_identity_sha256,
            "episode_count": len(source_episodes),
            "warehouse": str(warehouse),
            "database": database,
            "frames_table": agilex.FRAMES_TABLE,
            "frames_snapshot_id": frames_snapshot_id,
            "paimon_window_dataset": (
                "pypaimon.multimodal.ContiguousWindowDataset"),
            "paimon_window_snapshot_id": frames_snapshot_id,
            "train_episode_id": train_episode.episode_id,
            "validation_episode_id": validation_episode.episode_id,
        },
        "parameters": {
            "config": config.to_dict(),
            "cache_control": "uncontrolled",
            "device": "cpu",
            "data_loader_workers": 0,
        },
        "normalization": normalization_metadata,
        "window_plan": {
            **plan.to_dict(),
            "sha256": plan.sha256,
            "sample_sequence_sha256": sequence_sha256,
            "train_episode_id": train_episode.episode_id,
            "validation_episode_id": validation_episode.episode_id,
        },
        "execution_order": execution_order,
        "runs": runs,
        "summary": {
            backend: _summarize(
                [item for item in runs if item["backend"] == backend])
            for backend in ("hdf5", "paimon")
        },
        "correctness": {
            "passed": all(checks.values()),
            "checks": checks,
            "tensor_parity": tensor_parity,
            "loss_parity": loss_parity,
        },
        "environment": {
            "python": platform.python_version(),
            "os": platform.platform(),
            "machine": platform.machine(),
            "torch": torch.__version__,
            "source_commit": _git_head(Path(__file__).resolve().parents[3]),
        },
        "command": _sanitized_command(),
        "timing": {"wall_time_s": time.monotonic() - started},
        "unverified": [
            "OS page cache is uncontrolled; no cache dropping was attempted.",
            "CPU fixed-step loss parity proves engineering equivalence, "
            "not policy quality.",
            "GPU, multi-worker DataLoader, distributed training, and "
            "recovery are unverified.",
            "Python tracemalloc does not include all native Arrow or "
            "Torch allocations and is measured in a separate dataset-first-"
            "batch replay.",
        ],
        "started_at": started_at,
        "finished_at": _utc_now(),
    }
    if status != "SUCCEEDED":
        raise AssertionError("Paired ACT correctness gate failed: %s" % checks)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return report


def _hdf5_datasets(train_episode, validation_episode, normalization, config):
    return (
        Hdf5ACTWindowDataset(
            train_episode, normalization, config.action_horizon),
        Hdf5ACTWindowDataset(
            validation_episode, normalization, config.action_horizon),
    )


def _paimon_datasets(
        frames,
        frames_snapshot_id,
        train_episode_id,
        validation_episode_id,
        normalization,
        config):
    datasets = tuple(
        frames.scan(snapshot_id=frames_snapshot_id).where(
            "episode_id = '%s'" % episode_id.replace("'", "''")
        ).to_contiguous_window_dataset(
            window_size=config.action_horizon,
            columns=QPOS_COLUMNS + ACTION_COLUMNS + IMAGE_COLUMNS,
            anchor_columns=IMAGE_COLUMNS,
            group_key="episode_id",
            order_key="frame_index",
            stride=1,
            tail="drop",
            adapter=_PaimonACTAdapter(normalization),
        )
        for episode_id in (train_episode_id, validation_episode_id)
    )
    actual_snapshot_ids = {dataset.snapshot_id for dataset in datasets}
    if actual_snapshot_ids != {frames_snapshot_id}:
        raise RuntimeError(
            "Paimon ACT windows must remain pinned to frames snapshot %s; "
            "got %s."
            % (frames_snapshot_id, sorted(actual_snapshot_ids)))
    return datasets


def _shared_normalization(
        episodes,
        connection,
        frames_snapshot_id,
        statistics_version):
    train = [
        episode for episode in episodes
        if episode.split == "train" and episode.success
    ]
    if not train:
        raise ValueError("No successful train episodes are available.")
    qpos = _Moments(14)
    action = _Moments(14)
    import h5py

    for episode in sorted(train, key=lambda item: item.episode_id):
        with h5py.File(str(episode.path), "r") as h5:
            qpos.update(_read_vectors(
                h5, HDF5_QPOS_FIELDS, slice(None), dtype=np.float64))
            action.update(_read_vectors(
                h5, HDF5_ACTION_FIELDS, slice(None), dtype=np.float64))
    qpos_mean, qpos_std = qpos.finish()
    action_mean, action_std = action.finish()
    row = _statistics_row(connection, statistics_version)
    if row["source_snapshot_id"] != frames_snapshot_id:
        raise ValueError(
            "Normalization source snapshot %s differs from frames "
            "snapshot %s."
            % (row["source_snapshot_id"], frames_snapshot_id))
    if row["source_split"] != "train" or row["frame_count"] != action.count:
        raise ValueError(
            "Versioned action normalization has the wrong train scope.")
    if row["feature_name"] != "action":
        raise ValueError("Versioned normalization feature must be action.")
    if row["standard_deviation_floor"] != 1e-2:
        raise ValueError(
            "Versioned normalization must use the 1e-2 std floor.")
    stored_mean = np.asarray(row["action_mean"], dtype=np.float64)
    stored_std = np.asarray(row["action_std"], dtype=np.float64)
    if not (
            np.allclose(stored_mean, action_mean, rtol=1e-10, atol=1e-10)
            and np.allclose(stored_std, action_std, rtol=1e-10, atol=1e-10)):
        raise ValueError(
            "Versioned Paimon action normalization differs from HDF5 source.")
    normalization = {
        "qpos_mean": qpos_mean.astype(np.float32),
        "qpos_std": qpos_std.astype(np.float32),
        "action_mean": stored_mean.astype(np.float32),
        "action_std": stored_std.astype(np.float32),
    }
    serializable = {
        name: value.tolist() for name, value in normalization.items()
    }
    digest = hashlib.sha256(json.dumps(
        serializable, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest()
    return normalization, {
        "statistics_version": statistics_version,
        "source_split": "train",
        "frame_count": action.count,
        "standard_deviation_floor": 1e-2,
        "values": serializable,
        "sha256": digest,
    }


def _statistics_row(connection, statistics_version):
    escaped = statistics_version.replace("'", "''")
    rows = (connection.get_table(agilex.FEATURE_STATS_TABLE).scan()
            .where("statistics_version = '%s'" % escaped).to_list())
    if len(rows) != 1:
        raise ValueError(
            "Expected one normalization row for %r, got %d."
            % (statistics_version, len(rows)))
    return rows[0]


def _episode_rows(connection):
    return connection.get_table(agilex.EPISODES_TABLE).scan().select([
        "episode_id",
        "source_key",
        "split",
        "success",
        "frame_count",
    ]).to_list()


def _validate_source_identity(episodes, rows):
    expected = {
        item.episode_id: {
            "episode_id": item.episode_id,
            "source_key": item.source_key,
            "split": item.split,
            "success": item.success,
        }
        for item in episodes
    }
    actual = {
        item["episode_id"]: {
            "episode_id": item["episode_id"],
            "source_key": item["source_key"],
            "split": item["split"],
            "success": item["success"],
        }
        for item in rows
    }
    if actual != expected or len(actual) != len(rows):
        raise ValueError(
            "HDF5 and Paimon source identity differ; rebuild or select "
            "matching inputs.")
    rows_by_id = {item["episode_id"]: item for item in rows}
    enriched = [
        _BenchmarkEpisode(
            path=item.path,
            source_key=item.source_key,
            episode_id=item.episode_id,
            split=item.split,
            success=item.success,
            frame_count=rows_by_id[item.episode_id]["frame_count"],
        )
        for item in episodes
    ]
    manifest = sorted([
        {
            "episode_id": item.episode_id,
            "source_key": item.source_key,
            "split": item.split,
            "success": item.success,
            "frame_count": rows_by_id[item.episode_id]["frame_count"],
        }
        for item in episodes
    ], key=lambda item: item["episode_id"])
    payload = json.dumps(manifest, sort_keys=True, separators=(",", ":"))
    return enriched, hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _select_episode(source_by_id, split, requested, action_horizon):
    eligible = {
        episode_id: episode
        for episode_id, episode in source_by_id.items()
        if episode.split == split
        and episode.success
        and episode.frame_count >= action_horizon
    }
    if not eligible:
        raise ValueError(
            "No successful %s episode is long enough for horizon %d."
            % (split, action_horizon))
    selected = requested or min(eligible)
    if selected not in eligible:
        raise ValueError(
            "Requested %s episode is missing, unsuccessful, or too short: %s."
            % (split, selected))
    return eligible[selected]


def _tensor_parity(hdf5_datasets, paimon_datasets, plan):
    comparisons = (
        ("train", hdf5_datasets[0], paimon_datasets[0],
         sorted(set(plan.loader_indices + plan.train_indices))),
        ("validation", hdf5_datasets[1], paimon_datasets[1],
         sorted(set(plan.validation_indices))),
    )
    checked = 0
    max_absolute_difference = {
        "qpos": 0.0,
        "action": 0.0,
        "images": 0.0,
    }
    for split, hdf5_dataset, paimon_dataset, indices in comparisons:
        if len(hdf5_dataset) != len(paimon_dataset):
            raise AssertionError(
                "%s window counts differ: HDF5=%d Paimon=%d."
                % (split, len(hdf5_dataset), len(paimon_dataset)))
        for index in indices:
            hdf5_sample = hdf5_dataset[index]
            paimon_sample = paimon_dataset[index]
            for name in ("sample_id", "episode_id", "step_idx"):
                if hdf5_sample[name] != paimon_sample[name]:
                    raise AssertionError(
                        "%s %s differs at window %d." % (split, name, index))
            for name in ("qpos", "action", "images", "is_pad"):
                if not torch.equal(hdf5_sample[name], paimon_sample[name]):
                    raise AssertionError(
                        "%s %s tensor differs at %s."
                        % (split, name, hdf5_sample["sample_id"]))
                if name in max_absolute_difference:
                    difference = torch.max(torch.abs(
                        hdf5_sample[name] - paimon_sample[name])).item()
                    max_absolute_difference[name] = max(
                        max_absolute_difference[name], difference)
            checked += 1
    return {
        "passed": True,
        "checked_window_count": checked,
        "comparison": "torch.equal",
        "max_absolute_difference": max_absolute_difference,
    }


def _loss_parity(runs, round_count):
    comparisons = []
    passed = True
    for round_number in range(1, round_count + 1):
        by_backend = {
            item["backend"]: item
            for item in runs if item["round"] == round_number
        }
        hdf5_train = np.asarray(by_backend["hdf5"]["train_loss"])
        paimon_train = np.asarray(by_backend["paimon"]["train_loss"])
        train_equal = np.array_equal(hdf5_train, paimon_train)
        validation_equal = (
            by_backend["hdf5"]["validation_loss"]
            == by_backend["paimon"]["validation_loss"])
        passed = passed and train_equal and validation_equal
        comparisons.append({
            "round": round_number,
            "train_loss_exact": bool(train_equal),
            "validation_loss_exact": bool(validation_equal),
            "train_max_absolute_difference": float(np.max(np.abs(
                hdf5_train - paimon_train))),
            "validation_absolute_difference": abs(
                by_backend["hdf5"]["validation_loss"]
                - by_backend["paimon"]["validation_loss"]),
        })
    return {
        "passed": bool(passed),
        "comparison": "exact CPU deterministic equality",
        "rounds": comparisons,
    }


def _summarize(runs):
    metrics = (
        "dataset_build_s",
        "first_batch_s",
        "dataloader_samples_per_s",
        "fixed_steps_s",
        "validation_loss",
        "python_peak_allocated_bytes",
        "wall_time_s",
    )
    result = {"round_count": len(runs)}
    for name in metrics:
        values = [item[name] for item in runs]
        result[name] = {
            "median": float(np.median(values)),
            "min": float(np.min(values)),
            "max": float(np.max(values)),
        }
    return result


def _sample_sequence_sha256(train_episode_id, validation_episode_id, plan):
    value = {
        "loader": [
            "%s#%d" % (train_episode_id, index)
            for index in plan.loader_indices
        ],
        "train": [
            "%s#%d" % (train_episode_id, index)
            for index in plan.train_indices
        ],
        "validation": [
            "%s#%d" % (validation_episode_id, index)
            for index in plan.validation_indices
        ],
    }
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest()


def _read_vectors(h5, fields, selection, dtype=np.float32):
    value = np.concatenate([
        np.asarray(h5[field][selection], dtype=dtype)
        for field in fields
    ], axis=-1)
    if not np.isfinite(value).all():
        raise ValueError("ACT vector contains NaN or Inf.")
    return value


def _decode_hdf5_image(value):
    return _decode_image(value)


def _decode_image(value):
    payload = (
        bytes(value)
        if isinstance(value, (bytes, bytearray, memoryview))
        else np.asarray(value, dtype=np.uint8).tobytes()
    )
    image = decode_rgb_image(payload)
    return np.transpose(image, (2, 0, 1)).astype(np.float32) / 255.0


class _Moments(object):
    def __init__(self, width):
        self.count = 0
        self.total = np.zeros(width, dtype=np.float64)
        self.total_square = np.zeros(width, dtype=np.float64)

    def update(self, value):
        value = np.asarray(value, dtype=np.float64)
        if value.ndim != 2 or value.shape[1] != len(self.total):
            raise ValueError(
                "Unexpected normalization shape %s." % (value.shape,))
        if not np.isfinite(value).all():
            raise ValueError("Normalization input contains NaN or Inf.")
        self.count += value.shape[0]
        self.total += value.sum(axis=0)
        self.total_square += np.square(value).sum(axis=0)

    def finish(self):
        if self.count == 0:
            raise ValueError("Cannot compute normalization from no frames.")
        mean = self.total / self.count
        variance = np.maximum(
            self.total_square / self.count - np.square(mean), 0.0)
        return mean, np.maximum(np.sqrt(variance), 1e-2)


def _snapshot_id(table):
    snapshot = table.raw_table.snapshot_manager().get_latest_snapshot()
    if snapshot is None:
        raise ValueError("Paimon frames table has no snapshot.")
    return snapshot.id


def _expected_order(rounds):
    result = []
    for index in range(rounds):
        result.extend(
            ("hdf5", "paimon") if index % 2 == 0 else ("paimon", "hdf5"))
    return result


def _git_head(repository):
    try:
        return subprocess.check_output(
            ["git", "-C", str(repository), "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL,
            universal_newlines=True,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return "UNKNOWN"


def _sanitized_command():
    import sys
    return [os.path.basename(sys.executable)] + list(sys.argv)


def _utc_now():
    return datetime.now(timezone.utc).isoformat(
        timespec="seconds").replace("+00:00", "Z")


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input", required=True, help="RoboMIND AgileX HDF5 root directory.")
    parser.add_argument(
        "--warehouse", required=True, help="Existing Paimon warehouse path.")
    parser.add_argument(
        "--report", required=True, help="Destination JSON report path.")
    parser.add_argument(
        "--database", default=agilex.DEFAULT_DATABASE,
        help="Paimon database containing the ingested dataset.")
    parser.add_argument(
        "--statistics-version", default=agilex.DEFAULT_STATISTICS_VERSION,
        help="Canonical action statistics version to verify and use.")
    parser.add_argument(
        "--train-episode-id", help="Train episode; defaults to the first eligible episode.")
    parser.add_argument(
        "--validation-episode-id",
        help="Validation episode; defaults to the first eligible episode.")
    parser.add_argument(
        "--seed", type=int, default=BenchmarkConfig.seed,
        help="Shared random seed and window-plan seed.")
    parser.add_argument(
        "--action-horizon", type=int, default=BenchmarkConfig.action_horizon,
        help="Number of contiguous action rows in each sample.")
    parser.add_argument(
        "--batch-size", type=int, default=BenchmarkConfig.batch_size,
        help="Shared DataLoader batch size.")
    parser.add_argument(
        "--optimizer-steps", type=int, default=BenchmarkConfig.optimizer_steps,
        help="Fixed optimizer steps per backend run.")
    parser.add_argument(
        "--image-height", type=int, default=BenchmarkConfig.image_height,
        help="ACT input image height after resizing.")
    parser.add_argument(
        "--image-width", type=int, default=BenchmarkConfig.image_width,
        help="ACT input image width after resizing.")
    parser.add_argument(
        "--learning-rate", type=float, default=BenchmarkConfig.learning_rate,
        help="Shared AdamW learning rate.")
    parser.add_argument(
        "--weight-decay", type=float, default=BenchmarkConfig.weight_decay,
        help="Shared AdamW weight decay.")
    parser.add_argument(
        "--warmup-batches", type=int, default=BenchmarkConfig.warmup_batches,
        help="DataLoader batches consumed before timing.")
    parser.add_argument(
        "--loader-batches", type=int, default=BenchmarkConfig.loader_batches,
        help="Batches used for DataLoader throughput measurement.")
    parser.add_argument(
        "--rounds", type=int, default=BenchmarkConfig.rounds,
        help="Alternating backend rounds; must be at least three.")
    args = parser.parse_args(argv)
    config = BenchmarkConfig(
        seed=args.seed,
        action_horizon=args.action_horizon,
        batch_size=args.batch_size,
        optimizer_steps=args.optimizer_steps,
        image_height=args.image_height,
        image_width=args.image_width,
        learning_rate=args.learning_rate,
        weight_decay=args.weight_decay,
        warmup_batches=args.warmup_batches,
        loader_batches=args.loader_batches,
        rounds=args.rounds,
    )
    report = run(
        args.input,
        args.warehouse,
        args.report,
        config=config,
        database=args.database,
        statistics_version=args.statistics_version,
        train_episode_id=args.train_episode_id,
        validation_episode_id=args.validation_episode_id,
    )
    print(json.dumps({
        "status": report["status"],
        "report": str(Path(args.report).expanduser().resolve()),
        "input_manifest_sha256": report["input"]["input_manifest_sha256"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
