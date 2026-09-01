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

"""Prepare and run RoboMIND ACT benchmarks over HDF5 or Paimon.

Both adapters consume one immutable :class:`BenchmarkConfig`, one train-only
normalization object, and one explicit window plan. The runner resets the same
seed before constructing the same LeRobot ACT policy and AdamW trainer for each
backend. Each backend runs independently without attempting OS cache control
and writes its tensor fingerprint, loss trace, timing metrics, and Python
allocation metrics to one result JSON document.
Ingestion and canonical-action backfill are deliberately outside the benchmark.
"""

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
import pypaimon.multimodal as pmm
from pypaimon.benchmark.act.hdf5 import (
    compute_normalization as compute_hdf5_normalization,
    create_datasets as create_hdf5_datasets,
)
from pypaimon.benchmark.act.experiment import load_experiment
from pypaimon.benchmark.act.harness import (
    BenchmarkConfig,
    WindowPlan,
    build_window_plan,
    run_backend,
)
from pypaimon.benchmark.act.compare import canonical_sha256
from pypaimon.benchmark.act.paimon import (
    create_datasets as create_paimon_datasets,
    latest_snapshot_id,
    statistics_row,
)
from pypaimon.sample import robomind_agilex as agilex


@dataclass(frozen=True)
class _BenchmarkEpisode:
    path: Path
    source_key: str
    episode_id: str
    split: str
    success: bool
    frame_count: int


def prepare_experiment(
        input_root,
        warehouse,
        output_path,
        *,
        definition=None,
        database=agilex.DEFAULT_DATABASE):
    """Resolve a benchmark definition against matching HDF5 and Paimon data.

    Preparation is outside timed benchmark execution. It verifies source
    identity and Paimon statistics, selects eligible train/validation episodes,
    computes train-only normalization, and fixes every logical window index.

    Args:
        input_root: RoboMIND AgileX HDF5 root used as the source of episode
            files and raw normalization moments.
        warehouse: Existing Paimon warehouse containing the matching ingested
            and canonical-action-backfilled dataset.
        output_path: Destination for the resolved experiment JSON document.
        definition: Optional decoded experiment definition. The packaged
            defaults are used when omitted.
        database: Paimon database containing the RoboMIND tables.

    Returns:
        The resolved, JSON-compatible experiment dictionary written to
        ``output_path``.
    """
    definition = load_experiment() if definition is None else definition
    if definition.get("schema_version") != "act-benchmark-experiment@1":
        raise ValueError("Unsupported ACT benchmark experiment schema.")
    config = BenchmarkConfig(**definition["config"])
    statistics_version = definition["statistics_version"]
    input_root = Path(input_root).expanduser().resolve()
    warehouse = Path(warehouse).expanduser().resolve()
    output_path = Path(output_path).expanduser().resolve()

    discovered = agilex.discover_episodes(input_root)
    connection = pmm.connect(
        database=database, options={"warehouse": str(warehouse)})
    source_episodes, source_sha256 = _validate_source_identity(
        discovered, _episode_rows(connection))
    source_by_id = {episode.episode_id: episode for episode in source_episodes}
    frames = connection.get_table(agilex.FRAMES_TABLE)
    frames_snapshot_id = latest_snapshot_id(frames)
    normalization, normalization_metadata = _shared_normalization(
        source_episodes,
        connection,
        frames_snapshot_id,
        statistics_version,
    )
    del normalization
    train_episode = _select_episode(
        source_by_id,
        split="train",
        requested=definition.get("train_episode_id"),
        action_horizon=config.action_horizon,
    )
    validation_episode = _select_episode(
        source_by_id,
        split="val",
        requested=definition.get("validation_episode_id"),
        action_horizon=config.action_horizon,
    )
    plan = build_window_plan(
        train_episode.frame_count - config.action_horizon + 1,
        validation_episode.frame_count - config.action_horizon + 1,
        config,
    )
    sequence_sha256 = _sample_sequence_sha256(
        train_episode.episode_id, validation_episode.episode_id, plan)
    episodes = sorted(({
        "episode_id": episode.episode_id,
        "source_key": episode.source_key,
        "split": episode.split,
        "success": episode.success,
        "frame_count": episode.frame_count,
    } for episode in source_episodes), key=lambda item: item["episode_id"])
    experiment = {
        "schema_version": "act-benchmark-experiment@1",
        "benchmark_id": definition.get("benchmark_id", "robomind-act"),
        "dataset": definition.get("dataset", "RoboMIND AgileX"),
        "config": config.to_dict(),
        "statistics_version": statistics_version,
        "train_episode_id": train_episode.episode_id,
        "validation_episode_id": validation_episode.episode_id,
        "source": {
            "sha256": source_sha256,
            "episodes": episodes,
        },
        "normalization": normalization_metadata,
        "window_plan": {
            **plan.to_dict(),
            "sha256": plan.sha256,
            "sample_sequence_sha256": sequence_sha256,
        },
        "paimon": {
            "database": database,
            "frames_table": agilex.FRAMES_TABLE,
            "frames_snapshot_id": frames_snapshot_id,
        },
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(experiment, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return experiment


def run_experiment(
        backend,
        experiment_path,
        output_path,
        *,
        input_root=None,
        warehouse=None,
        policy_factory=None):
    """Run one storage backend against a resolved ACT experiment.

    Args:
        backend: Result label and dataset implementation, either ``hdf5`` or
            ``paimon``.
        experiment_path: Resolved JSON produced by :func:`prepare_experiment`.
        output_path: Destination JSON result path.
        input_root: Required only for the HDF5 backend.
        warehouse: Required only for the Paimon backend.
        policy_factory: Optional test hook returning ``(policy, metadata)``.

    Returns:
        A JSON-compatible single-backend result containing the resolved
        experiment, runtime environment, tensor fingerprint, per-round raw
        metrics, and median/min/max summary.
    """
    if backend not in ("hdf5", "paimon"):
        raise ValueError("backend must be 'hdf5' or 'paimon'.")
    experiment = load_experiment(experiment_path)
    _validate_resolved_experiment(experiment)
    config = BenchmarkConfig(**experiment["config"])
    plan = _window_plan_from_experiment(experiment)
    normalization = {
        name: np.asarray(value, dtype=np.float32)
        for name, value in experiment["normalization"]["values"].items()
    }
    sequence_sha256 = experiment["window_plan"]["sample_sequence_sha256"]
    if backend == "hdf5":
        if input_root is None:
            raise ValueError("input_root is required for the HDF5 backend.")
        episodes = _hdf5_episodes_from_experiment(input_root, experiment)
        by_id = {episode.episode_id: episode for episode in episodes}
        train_episode = by_id[experiment["train_episode_id"]]
        validation_episode = by_id[experiment["validation_episode_id"]]

        def dataset_factory():
            return create_hdf5_datasets(
                train_episode, validation_episode, normalization, config)

        source = {"input_root": str(Path(input_root).expanduser().resolve())}
    else:
        if warehouse is None:
            raise ValueError("warehouse is required for the Paimon backend.")
        dataset_factory, source = _paimon_factory_from_experiment(
            warehouse, experiment, normalization, config)

    started_at = _utc_now()
    started = time.monotonic()
    fingerprint = _tensor_fingerprint(dataset_factory(), plan)
    runs = []
    for round_number in range(1, config.rounds + 1):
        runs.append(run_backend(
            backend,
            round_number,
            dataset_factory,
            plan,
            config,
            sequence_sha256,
            policy_factory=policy_factory,
        ))
        gc.collect()
    result = {
        "schema_version": "act-benchmark-result@1",
        "benchmark_id": experiment["benchmark_id"],
        "run_id": "%s-%s" % (
            started_at.replace(":", "").replace("-", ""),
            uuid.uuid4().hex[:8],
        ),
        "status": "SUCCEEDED",
        "backend": backend,
        "experiment": experiment,
        "experiment_sha256": canonical_sha256(experiment),
        "source": source,
        "tensor_fingerprint": fingerprint,
        "model": runs[0]["model"],
        "runs": runs,
        "summary": _summarize(runs),
        "environment": {
            "python": platform.python_version(),
            "os": platform.platform(),
            "machine": platform.machine(),
            "torch": torch.__version__,
            "source_commit": _git_head(Path(__file__).resolve().parents[4]),
        },
        "command": _command_argv(),
        "timing": {"wall_time_s": time.monotonic() - started},
        "unverified": [
            "OS page cache is uncontrolled; no cache dropping was attempted.",
            "CPU fixed-step loss parity proves engineering equivalence, "
            "not policy quality.",
            "GPU, multi-worker dataset loading, distributed training, and "
            "recovery are unverified.",
            "Python tracemalloc excludes native Arrow and Torch allocations.",
        ],
        "started_at": started_at,
        "finished_at": _utc_now(),
    }
    output_path = Path(output_path).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return result


def _validate_resolved_experiment(experiment):
    """Reject incomplete or internally inconsistent resolved experiments."""
    required = {
        "schema_version", "benchmark_id", "dataset", "config",
        "statistics_version", "train_episode_id", "validation_episode_id",
        "source", "normalization", "window_plan", "paimon",
    }
    if experiment.get("schema_version") != "act-benchmark-experiment@1":
        raise ValueError("Unsupported ACT benchmark experiment schema.")
    missing = required - set(experiment)
    if missing:
        raise ValueError(
            "Resolved ACT experiment is missing: %s."
            % ", ".join(sorted(missing)))
    source = experiment["source"]
    if canonical_sha256(source["episodes"]) != source["sha256"]:
        raise ValueError("ACT experiment source-manifest hash differs.")
    normalization = experiment["normalization"]
    if canonical_sha256(normalization["values"]) != normalization["sha256"]:
        raise ValueError("ACT experiment normalization hash differs.")
    plan = _window_plan_from_experiment(experiment)
    if plan.sha256 != experiment["window_plan"]["sha256"]:
        raise ValueError("ACT experiment window-plan hash differs.")
    episodes = {
        item["episode_id"]: item for item in source["episodes"]
    }
    try:
        train = episodes[experiment["train_episode_id"]]
        validation = episodes[experiment["validation_episode_id"]]
    except KeyError as error:
        raise ValueError(
            "ACT experiment selected episode is absent from the source."
        ) from error
    config = BenchmarkConfig(**experiment["config"])
    expected_plan = build_window_plan(
        train["frame_count"] - config.action_horizon + 1,
        validation["frame_count"] - config.action_horizon + 1,
        config,
    )
    if expected_plan.to_dict() != plan.to_dict():
        raise ValueError(
            "ACT experiment window plan was not built from its config and "
            "selected episodes.")
    expected_sequence = _sample_sequence_sha256(
        train["episode_id"], validation["episode_id"], plan)
    if expected_sequence != experiment["window_plan"][
            "sample_sequence_sha256"]:
        raise ValueError("ACT experiment sample-sequence hash differs.")


def _window_plan_from_experiment(experiment):
    """Reconstruct immutable logical-window indices from JSON values."""
    value = experiment["window_plan"]
    return WindowPlan(
        seed=value["seed"],
        measurement_indices=tuple(value["measurement_indices"]),
        train_indices=tuple(value["train_indices"]),
        validation_indices=tuple(value["validation_indices"]),
    )


def _hdf5_episodes_from_experiment(input_root, experiment):
    """Validate HDF5 episode identity and attach manifest frame counts."""
    discovered = agilex.discover_episodes(Path(input_root).expanduser().resolve())
    by_id = {episode.episode_id: episode for episode in discovered}
    expected = experiment["source"]["episodes"]
    actual_identity = sorted(({
        "episode_id": episode.episode_id,
        "source_key": episode.source_key,
        "split": episode.split,
        "success": episode.success,
    } for episode in discovered), key=lambda item: item["episode_id"])
    expected_identity = [{
        "episode_id": item["episode_id"],
        "source_key": item["source_key"],
        "split": item["split"],
        "success": item["success"],
    } for item in expected]
    if actual_identity != expected_identity:
        raise ValueError("HDF5 source differs from the ACT experiment.")
    return [
        _BenchmarkEpisode(
            path=by_id[item["episode_id"]].path,
            source_key=item["source_key"],
            episode_id=item["episode_id"],
            split=item["split"],
            success=item["success"],
            frame_count=item["frame_count"],
        )
        for item in expected
    ]


def _paimon_factory_from_experiment(
        warehouse, experiment, normalization, config):
    """Validate Paimon source/statistics and return a pinned dataset factory."""
    warehouse = Path(warehouse).expanduser().resolve()
    paimon = experiment["paimon"]
    connection = pmm.connect(
        database=paimon["database"],
        options={"warehouse": str(warehouse)},
    )
    frames = connection.get_table(paimon["frames_table"])
    snapshot_id = paimon["frames_snapshot_id"]
    expected_episodes = experiment["source"]["episodes"]
    actual_episodes = sorted(_episode_rows(connection),
                             key=lambda item: item["episode_id"])
    if actual_episodes != expected_episodes:
        raise ValueError("Paimon source differs from the ACT experiment.")
    row = statistics_row(connection, experiment["statistics_version"])
    expected_normalization = experiment["normalization"]
    action_mean = np.asarray(row["action_mean"], dtype=np.float32)
    action_std = np.asarray(row["action_std"], dtype=np.float32)
    if (
            row["source_snapshot_id"] != snapshot_id
            or row["source_split"] != "train"
            or row["frame_count"] != expected_normalization["frame_count"]
            or row["feature_name"] != "action"
            or row["standard_deviation_floor"] != 1e-2
            or not np.array_equal(
                action_mean, normalization["action_mean"])
            or not np.array_equal(action_std, normalization["action_std"])):
        raise ValueError(
            "Paimon normalization differs from the ACT experiment.")

    def factory():
        return create_paimon_datasets(
            frames,
            snapshot_id,
            experiment["train_episode_id"],
            experiment["validation_episode_id"],
            normalization,
            config,
        )

    return factory, {
        "warehouse": str(warehouse),
        "database": paimon["database"],
        "frames_table": paimon["frames_table"],
        "frames_snapshot_id": snapshot_id,
    }


def _tensor_fingerprint(datasets, plan):
    """Hash the exact planned sample IDs and tensors outside timed execution."""
    comparisons = (
        ("train", datasets[0],
         sorted(set(plan.measurement_indices + plan.train_indices))),
        ("validation", datasets[1],
         sorted(set(plan.validation_indices))),
    )
    digest = hashlib.sha256()
    count = 0
    for split, dataset, indices in comparisons:
        for index in indices:
            sample = dataset[index]
            identity = {
                "split": split,
                "index": index,
                "sample_id": sample["sample_id"],
                "episode_id": sample["episode_id"],
                "step_idx": sample["step_idx"],
            }
            digest.update(json.dumps(
                identity, sort_keys=True, separators=(",", ":")
            ).encode("utf-8"))
            for name in ("qpos", "action", "images", "is_pad"):
                tensor = sample[name].detach().cpu().contiguous()
                digest.update(name.encode("utf-8"))
                digest.update(str(tensor.dtype).encode("ascii"))
                digest.update(str(tuple(tensor.shape)).encode("ascii"))
                digest.update(tensor.numpy().tobytes())
            count += 1
    return {
        "sha256": digest.hexdigest(),
        "checked_window_count": count,
        "fields": [
            "sample_id", "episode_id", "step_idx", "qpos", "action",
            "images", "is_pad",
        ],
    }


def _shared_normalization(
        episodes,
        connection,
        frames_snapshot_id,
        statistics_version):
    """Build one train-only normalization contract for both backends.

    HDF5 supplies state and action moments from successful train episodes.
    Versioned Paimon action statistics must match the float64 HDF5 moments,
    train scope, frame count, source snapshot, feature name, and ``1e-2``
    standard-deviation floor.

    Returns:
        ``(arrays, metadata)`` where arrays are float32 training values and
        metadata is JSON-compatible and includes their canonical SHA-256.
    """
    normalization, hdf5_metadata = compute_hdf5_normalization(episodes)
    action_mean = hdf5_metadata["action_mean"]
    action_std = hdf5_metadata["action_std"]
    action_count = hdf5_metadata["frame_count"]
    row = statistics_row(connection, statistics_version)
    if row["source_snapshot_id"] != frames_snapshot_id:
        raise ValueError(
            "Normalization source snapshot %s differs from frames "
            "snapshot %s."
            % (row["source_snapshot_id"], frames_snapshot_id))
    if row["source_split"] != "train" or row["frame_count"] != action_count:
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
    normalization["action_mean"] = stored_mean.astype(np.float32)
    normalization["action_std"] = stored_std.astype(np.float32)
    serializable = {
        name: value.tolist() for name, value in normalization.items()
    }
    digest = hashlib.sha256(json.dumps(
        serializable, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest()
    return normalization, {
        "statistics_version": statistics_version,
        "source_split": "train",
        "frame_count": action_count,
        "standard_deviation_floor": 1e-2,
        "values": serializable,
        "sha256": digest,
    }


def _episode_rows(connection):
    return connection.get_table(agilex.EPISODES_TABLE).scan().select([
        "episode_id",
        "source_key",
        "split",
        "success",
        "frame_count",
    ]).to_list()


def _validate_source_identity(episodes, rows):
    """Match HDF5 discovery to Paimon episodes and return a manifest hash.

    Episode ID, source key, split, and success must match exactly. Paimon's
    versioned episode rows contribute frame counts used to build complete
    windows. The returned records retain the local HDF5 paths while the hash
    covers only portable source metadata.
    """
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
    """Select a successful, split-matching episode long enough for one window.

    An explicit episode is honored when eligible; otherwise the
    lexicographically first eligible episode ID is selected.
    """
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


def _summarize(runs):
    """Return median, minimum, and maximum metrics across backend repeats."""
    metrics = (
        "dataset_build_s",
        "first_batch_s",
        "batch_fetch_samples_per_s",
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
    """Hash episode-qualified sample IDs in measurement/train/validation order."""
    value = {
        "batch_fetch": [
            "%s#%d" % (train_episode_id, index)
            for index in plan.measurement_indices
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


def _git_head(repository):
    try:
        return subprocess.check_output(
            ["git", "-C", str(repository), "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL,
            universal_newlines=True,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return "UNKNOWN"


def _command_argv():
    """Return the invoked Python basename and command-line arguments."""
    import sys
    return [os.path.basename(sys.executable)] + list(sys.argv)


def _utc_now():
    return datetime.now(timezone.utc).isoformat(
        timespec="seconds").replace("+00:00", "Z")
