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

"""RoboMIND AgileX HDF5 ingestion and canonical action backfill."""

import argparse
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import pyarrow as pa

import pypaimon.multimodal as pmm


DEFAULT_DATABASE = "robomind"
EPISODES_TABLE = "episodes_agilex"
FRAMES_TABLE = "frames_agilex"
FEATURE_STATS_TABLE = "feature_stats_agilex"
DEFAULT_STATISTICS_VERSION = "robomind-agilex-joint-position@1"

TABLE_OPTIONS = {
    "blob-as-descriptor": "false",
}

NUMERIC_FIELDS = (
    ("state_end_effector_left", "puppet/end_effector_left"),
    ("state_end_effector_right", "puppet/end_effector_right"),
    ("state_joint_effort_left", "puppet/joint_effort_left"),
    ("state_joint_effort_right", "puppet/joint_effort_right"),
    ("state_joint_position_left", "puppet/joint_position_left"),
    ("state_joint_position_right", "puppet/joint_position_right"),
    ("state_joint_velocity_left", "puppet/joint_velocity_left"),
    ("state_joint_velocity_right", "puppet/joint_velocity_right"),
    ("action_end_effector_left", "master/end_effector_left"),
    ("action_end_effector_right", "master/end_effector_right"),
    ("action_joint_effort_left", "master/joint_effort_left"),
    ("action_joint_effort_right", "master/joint_effort_right"),
    ("action_joint_position_left", "master/joint_position_left"),
    ("action_joint_position_right", "master/joint_position_right"),
    ("action_joint_velocity_left", "master/joint_velocity_left"),
    ("action_joint_velocity_right", "master/joint_velocity_right"),
)
IMAGE_FIELDS = (
    ("rgb_front", "observations/rgb_images/camera_front"),
    ("rgb_left_wrist", "observations/rgb_images/camera_left_wrist"),
    ("rgb_right_wrist", "observations/rgb_images/camera_right_wrist"),
    ("depth_front", "observations/depth_images/camera_front"),
    ("depth_left_wrist", "observations/depth_images/camera_left_wrist"),
    ("depth_right_wrist", "observations/depth_images/camera_right_wrist"),
)

_ACTION_LEFT = "action_joint_position_left"
_ACTION_RIGHT = "action_joint_position_right"
_ACTION_COLUMN = "action"
_ACTION_VECTOR_TYPE = pa.list_(pa.float32(), 14)
_STANDARD_DEVIATION_FLOOR = 1e-2


@dataclass(frozen=True)
class EpisodeSource:
    """RoboMIND metadata derived without opening the HDF5 source."""

    path: Path
    source_key: str
    episode_id: str
    split: str
    success: bool


@dataclass(frozen=True)
class IngestResult:
    """Small control-plane result returned by an AgileX ingestion."""

    mode: str
    episode_count: int
    frame_count: int
    episodes_snapshot_id: int
    frames_snapshot_id: int


@dataclass(frozen=True)
class BackfillResult:
    """Result of materializing canonical action and its statistics row."""

    row_count: int
    frames_snapshot_id: int
    statistics_snapshot_id: int
    statistics_version: str


@dataclass(frozen=True)
class LocalPipelineResult:
    """Result of the complete local ingestion and backfill pipeline."""

    ingest: IngestResult
    backfill: BackfillResult


def episode_schema():
    """Return the shared AgileX episode business schema."""
    return pa.schema([
        pa.field("episode_id", pa.string(), nullable=False),
        pa.field("source_key", pa.string(), nullable=False),
        pa.field("split", pa.string(), nullable=False),
        pa.field("success", pa.bool_(), nullable=False),
        pa.field("instruction", pa.string()),
        pa.field("instruction_embedding", pa.list_(pa.float32(), 768)),
        pa.field("frame_count", pa.int32(), nullable=False),
        pa.field("hdf5_compress", pa.bool_()),
        pa.field("hdf5_sim", pa.bool_()),
    ])


def frame_schema():
    """Return the shared AgileX frame schema before canonical backfill."""
    fields = [
        pa.field("episode_id", pa.string(), nullable=False),
        pa.field("frame_index", pa.int32(), nullable=False),
    ]
    fields.extend(
        pa.field(name, pa.large_binary(), nullable=False)
        for name, _ in IMAGE_FIELDS
    )
    fields.extend(
        pa.field(name, pa.list_(pa.float64(), 7), nullable=False)
        for name, _ in NUMERIC_FIELDS
    )
    return pa.schema(fields)


def backfilled_frame_schema():
    """Return the frame schema after canonical action is added."""
    return frame_schema().append(pa.field(_ACTION_COLUMN, _ACTION_VECTOR_TYPE))


def feature_stats_schema():
    """Return the versioned normalization-statistics schema."""
    return pa.schema([
        pa.field("statistics_version", pa.string(), nullable=False),
        pa.field("source_table", pa.string(), nullable=False),
        pa.field("source_snapshot_id", pa.int64(), nullable=False),
        pa.field("source_split", pa.string(), nullable=False),
        pa.field("split_manifest_sha256", pa.string(), nullable=False),
        pa.field("feature_name", pa.string(), nullable=False),
        pa.field("frame_count", pa.int64(), nullable=False),
        pa.field("action_mean", pa.list_(pa.float64(), 14), nullable=False),
        pa.field("action_std", pa.list_(pa.float64(), 14), nullable=False),
        pa.field("standard_deviation_floor", pa.float64(), nullable=False),
    ])


def discover_episodes(input_root):
    """Discover RoboMIND episode paths without reading HDF5 contents."""
    root = Path(input_root).expanduser().resolve()
    if not root.is_dir():
        raise ValueError("RoboMIND input root does not exist: %s" % root)
    paths = sorted(root.glob("**/data/trajectory.hdf5"))
    if not paths:
        raise ValueError("No RoboMIND trajectory.hdf5 files found below %s." % root)

    episodes = []
    episode_ids = set()
    for path in paths:
        source_key = path.relative_to(root).as_posix()
        split = _path_component(source_key, ("train", "val"), "split")
        status = _path_component(
            source_key, ("success_episodes", "failed_episodes"), "status")
        episode_id = path.parent.parent.name
        if episode_id in episode_ids:
            raise ValueError("Duplicate RoboMIND episode_id %r." % episode_id)
        episode_ids.add(episode_id)
        episodes.append(EpisodeSource(
            path=path,
            source_key=source_key,
            episode_id=episode_id,
            split=split,
            success=status == "success_episodes",
        ))
    return episodes


class _RoboMindAgileXTransform:

    def __init__(self, episodes):
        self._episodes = {
            episode.path: episode for episode in episodes
        }
        if not self._episodes:
            raise ValueError("episodes must not be empty.")

    def _source(self, source):
        source_path = source.local_path
        if source_path is None:
            raise ValueError(
                "RoboMIND AgileX requires a local HDF5 source: %s"
                % source.path
            )
        source_path = source_path.resolve()
        episode = self._episodes.get(source_path)
        if episode is None:
            raise ValueError("Unknown RoboMIND source path %r." % source_path)
        return episode


class RoboMindAgileXEpisodeTransform(_RoboMindAgileXTransform):
    """Validate one AgileX file and emit its episode metadata row."""

    def __call__(self, h5, source):
        episode = self._source(source)
        frame_count = _validate_source(h5, episode.source_key)
        yield self._episode_batch(h5, episode, frame_count)

    @staticmethod
    def _episode_batch(h5, episode, frame_count):
        instruction = _instruction(h5, episode.source_key)
        embedding = _instruction_embedding(h5, episode.source_key)
        return pa.RecordBatch.from_pydict({
            "episode_id": [episode.episode_id],
            "source_key": [episode.source_key],
            "split": [episode.split],
            "success": [episode.success],
            "instruction": [instruction],
            "instruction_embedding": [embedding],
            "frame_count": [frame_count],
            "hdf5_compress": [_optional_bool(h5.attrs.get("compress"))],
            "hdf5_sim": [_optional_bool(h5.attrs.get("sim"))],
        }, schema=episode_schema())


class RoboMindAgileXFrameTransform(_RoboMindAgileXTransform):
    """Validate one AgileX file and stream its frame rows."""

    def __init__(self, episodes, *, batch_size=64):
        super().__init__(episodes)
        self.batch_size = _positive_int(batch_size, "batch_size")

    def __call__(self, h5, source):
        episode = self._source(source)
        frame_count = _validate_source(h5, episode.source_key)
        for begin in range(0, frame_count, self.batch_size):
            end = min(begin + self.batch_size, frame_count)
            count = end - begin
            columns = {
                "episode_id": [episode.episode_id] * count,
                "frame_index": np.arange(begin, end, dtype=np.int32),
            }
            for name, hdf5_path in IMAGE_FIELDS:
                columns[name] = [
                    np.asarray(value, dtype=np.uint8).tobytes()
                    for value in h5[hdf5_path][begin:end]
                ]
            for name, hdf5_path in NUMERIC_FIELDS:
                values = np.asarray(h5[hdf5_path][begin:end], dtype=np.float64)
                if not np.isfinite(values).all():
                    raise ValueError(
                        "%s: /%s contains NaN or Inf."
                        % (episode.source_key, hdf5_path)
                    )
                columns[name] = values.tolist()
            yield pa.RecordBatch.from_pydict(columns, schema=frame_schema())


def ingest_local(
        input_root,
        warehouse,
        *,
        database=DEFAULT_DATABASE,
        batch_size=64):
    """Ingest AgileX episodes locally through strict ``load_from_hdf5``."""
    episodes = discover_episodes(input_root)
    connection, _, _ = _create_tables(warehouse, database)
    paths = [episode.path for episode in episodes]
    episode_result = connection.load_from_hdf5(
        EPISODES_TABLE,
        paths,
        transform=RoboMindAgileXEpisodeTransform(episodes),
    )
    frame_result = connection.load_from_hdf5(
        FRAMES_TABLE,
        paths,
        transform=RoboMindAgileXFrameTransform(
            episodes, batch_size=batch_size),
    )
    del connection
    return IngestResult(
        mode="local",
        episode_count=episode_result.row_count,
        frame_count=frame_result.row_count,
        episodes_snapshot_id=episode_result.snapshot_id,
        frames_snapshot_id=frame_result.snapshot_id,
    )


def run_local_pipeline(
        input_root,
        warehouse,
        *,
        database=DEFAULT_DATABASE,
        batch_size=64,
        statistics_version=DEFAULT_STATISTICS_VERSION):
    """Run local AgileX ingestion and canonical-action backfill."""
    ingest = ingest_local(
        input_root,
        warehouse,
        database=database,
        batch_size=batch_size,
    )
    backfill = backfill_canonical_action(
        warehouse,
        database=database,
        statistics_version=statistics_version,
    )
    return LocalPipelineResult(ingest=ingest, backfill=backfill)


def ingest_ray(
        input_root,
        warehouse,
        *,
        database=DEFAULT_DATABASE,
        batch_size=64,
        concurrency=None,
        ray_address=None):
    """Ingest AgileX through the public distributed HDF5 loader."""
    if concurrency is not None:
        concurrency = _positive_int(concurrency, "concurrency")
    episodes = discover_episodes(input_root)
    _, episodes_table, frames_table = _create_tables(warehouse, database)

    try:
        import ray
    except ImportError:
        raise ImportError(
            "Ray ingestion requires ray; install pypaimon[ray,hdf5].")

    initialized_here = not ray.is_initialized()
    if initialized_here:
        init_args = {
            "include_dashboard": False,
            "ignore_reinit_error": True,
        }
        if ray_address is None:
            init_args["num_cpus"] = 2
        else:
            init_args["address"] = ray_address
        ray.init(**init_args)
    elif ray_address is not None:
        raise ValueError(
            "ray_address cannot be set after Ray has already been initialized.")

    try:
        from pypaimon.ray import load_from_hdf5
        catalog_options = {
            "warehouse": str(Path(warehouse).expanduser().resolve())}
        paths = [episode.path for episode in episodes]
        load_from_hdf5(
            "%s.%s" % (database, EPISODES_TABLE), paths, catalog_options,
            transform=RoboMindAgileXEpisodeTransform(episodes),
            concurrency=concurrency,
        )
        load_from_hdf5(
            "%s.%s" % (database, FRAMES_TABLE), paths, catalog_options,
            transform=RoboMindAgileXFrameTransform(
                episodes, batch_size=batch_size),
            concurrency=concurrency,
        )
        episode_rows = _read_raw(episodes_table.raw_table, ["episode_id"])
        frame_rows = _read_raw(frames_table.raw_table, ["frame_index"])
        return IngestResult(
            mode="ray",
            episode_count=episode_rows.num_rows,
            frame_count=frame_rows.num_rows,
            episodes_snapshot_id=_snapshot_id(episodes_table),
            frames_snapshot_id=_snapshot_id(frames_table),
        )
    finally:
        if initialized_here:
            ray.shutdown()


def build_canonical_action_backfill(source):
    """Build canonical actions keyed by the physical row ID."""
    required = [_ACTION_LEFT, _ACTION_RIGHT, "_ROW_ID"]
    missing = [name for name in required if name not in source.column_names]
    if missing:
        raise ValueError("Source is missing required columns: %s." % missing)
    left = np.asarray(source[_ACTION_LEFT].to_pylist(), dtype=np.float64)
    right = np.asarray(source[_ACTION_RIGHT].to_pylist(), dtype=np.float64)
    if left.ndim != 2 or left.shape[1:] != (7,):
        raise ValueError(
            "%s must have shape (rows, 7), got %s."
            % (_ACTION_LEFT, left.shape)
        )
    if right.shape != left.shape:
        raise ValueError(
            "%s must have shape %s, got %s."
            % (_ACTION_RIGHT, left.shape, right.shape)
        )
    action64 = np.concatenate([left, right], axis=1)
    if not np.isfinite(action64).all():
        raise ValueError("Canonical action input contains NaN or Inf.")
    action = action64.astype(np.float32)
    return pa.table({
        "_ROW_ID": source["_ROW_ID"],
        _ACTION_COLUMN: pa.array(action.tolist(), type=_ACTION_VECTOR_TYPE),
    })


def build_action_statistics(source, train_episode_ids):
    """Compute train-only action population statistics."""
    train_ids = set(train_episode_ids)
    if not train_ids:
        raise ValueError("Cannot compute action statistics without train episodes.")
    episode_ids = np.asarray(source["episode_id"].to_pylist(), dtype=object)
    frame_indices = np.asarray(source["frame_index"].to_pylist(), dtype=np.int64)
    train = np.asarray([value in train_ids for value in episode_ids], dtype=bool)
    if not train.any():
        raise ValueError("No frame rows belong to the train episodes.")
    action = np.asarray(source[_ACTION_COLUMN].to_pylist(), dtype=np.float64)
    train_rows = sorted(
        np.flatnonzero(train),
        key=lambda index: (episode_ids[index], int(frame_indices[index])))
    train_action = action[train_rows]
    mean = train_action.mean(axis=0)
    std = np.maximum(train_action.std(axis=0), _STANDARD_DEVIATION_FLOOR)
    statistics = {
        "frame_count": int(train.sum()),
        "action_mean": mean.tolist(),
        "action_std": std.tolist(),
        "standard_deviation_floor": _STANDARD_DEVIATION_FLOOR,
    }
    return statistics


def backfill_canonical_action(
        warehouse,
        *,
        database=DEFAULT_DATABASE,
        statistics_version=DEFAULT_STATISTICS_VERSION):
    """Run the independently recoverable action and statistics stages."""
    row_count, frames_snapshot_id = materialize_canonical_action(
        warehouse, database=database)
    statistics_snapshot_id = refresh_action_statistics(
        warehouse,
        database=database,
        statistics_version=statistics_version,
    )
    return BackfillResult(
        row_count=row_count,
        frames_snapshot_id=frames_snapshot_id,
        statistics_snapshot_id=statistics_snapshot_id,
        statistics_version=statistics_version,
    )


def materialize_canonical_action(warehouse, *, database=DEFAULT_DATABASE):
    """Stage one: add and populate canonical action, then commit it."""
    connection = pmm.connect(
        database=database,
        options={"warehouse": str(Path(warehouse).expanduser().resolve())},
    )
    frames_table = connection.get_table(FRAMES_TABLE)
    _validate_backfill_target(frames_table)

    from pypaimon.schema.data_types import AtomicType, VectorType
    from pypaimon.schema.schema_change import SchemaChange

    connection.catalog.alter_table(
        frames_table.identifier,
        [SchemaChange.add_column(
            _ACTION_COLUMN,
            VectorType(True, AtomicType("FLOAT"), 14),
            comment=(
                "Canonical AgileX action: master joint position left "
                "followed by right."),
        )],
        False,
    )
    frames_table = connection.get_table(FRAMES_TABLE)
    row_count = _update_canonical_action_batches(frames_table.raw_table)
    frames_table = connection.get_table(FRAMES_TABLE)
    frames_snapshot_id = _snapshot_id(frames_table)
    return row_count, frames_snapshot_id


def refresh_action_statistics(
        warehouse,
        *,
        database=DEFAULT_DATABASE,
        statistics_version=DEFAULT_STATISTICS_VERSION):
    """Stage two: recompute statistics from committed episode/frame tables."""
    if not isinstance(statistics_version, str) or not statistics_version:
        raise ValueError("statistics_version must be a non-empty string.")
    connection = pmm.connect(
        database=database,
        options={"warehouse": str(Path(warehouse).expanduser().resolve())},
    )
    episodes_table = connection.get_table(EPISODES_TABLE)
    frames_table = connection.get_table(FRAMES_TABLE)
    if _ACTION_COLUMN not in frames_table.raw_table.field_names:
        raise ValueError("Canonical action column does not exist.")

    episode_rows = _read_raw(
        episodes_table.raw_table, ["episode_id", "split", "success"])
    train_episode_ids = sorted(
        row["episode_id"]
        for row in episode_rows.to_pylist()
        if row["split"] == "train" and row["success"]
    )
    frames_snapshot_id = _snapshot_id(frames_table)
    statistics = _stream_action_statistics(
        frames_table.raw_table, train_episode_ids)

    split_manifest_sha256 = hashlib.sha256(
        "".join("%s\n" % value for value in train_episode_ids)
        .encode("utf-8")
    ).hexdigest()
    stats_table = connection.create_table(
        FEATURE_STATS_TABLE,
        schema=feature_stats_schema(),
        options=TABLE_OPTIONS,
        ignore_if_exists=True,
    )
    stats_table.add(pa.Table.from_pylist([{
        "statistics_version": statistics_version,
        "source_table": "%s.%s" % (database, FRAMES_TABLE),
        "source_snapshot_id": frames_snapshot_id,
        "source_split": "train",
        "split_manifest_sha256": split_manifest_sha256,
        "feature_name": _ACTION_COLUMN,
        "frame_count": statistics["frame_count"],
        "action_mean": statistics["action_mean"],
        "action_std": statistics["action_std"],
        "standard_deviation_floor": statistics[
            "standard_deviation_floor"],
    }], schema=feature_stats_schema()))
    return _snapshot_id(stats_table)


def _create_tables(warehouse, database):
    connection = pmm.connect(
        database=database,
        options={"warehouse": str(Path(warehouse).expanduser().resolve())},
    )
    episodes_table = connection.create_table(
        EPISODES_TABLE, schema=episode_schema(), options=TABLE_OPTIONS)
    frames_table = connection.create_table(
        FRAMES_TABLE, schema=frame_schema(), options=TABLE_OPTIONS)
    return connection, episodes_table, frames_table


def _validate_backfill_target(frames_table):
    missing = [
        name for name in (_ACTION_LEFT, _ACTION_RIGHT)
        if name not in frames_table.raw_table.field_names
    ]
    if missing:
        raise ValueError("Frames table is missing raw action columns: %s." % missing)
    if _ACTION_COLUMN in frames_table.raw_table.field_names:
        raise ValueError("Canonical action column already exists.")


def _update_canonical_action_batches(table):
    """Transform one planned Paimon split at a time and commit all updates once."""
    builder = table.new_batch_write_builder()
    commit = builder.new_commit()
    messages = []
    row_count = 0
    try:
        for source in _iter_raw(
                table, [_ACTION_LEFT, _ACTION_RIGHT, "_ROW_ID"]):
            updates = build_canonical_action_backfill(source)
            messages.extend(
                builder.new_update()
                .with_update_type([_ACTION_COLUMN])
                .update_by_arrow_with_row_id(updates)
            )
            row_count += len(updates)
        commit.commit(messages)
    finally:
        commit.close()
    return row_count


def _stream_action_statistics(table, train_episode_ids):
    """Accumulate only fixed-size count/sum/sum-of-squares on the driver."""
    train_ids = set(train_episode_ids)
    if not train_ids:
        raise ValueError("Cannot compute action statistics without train episodes.")
    count = 0
    total = np.zeros(14, dtype=np.float64)
    total_square = np.zeros(14, dtype=np.float64)
    for source in _iter_raw(table, ["episode_id", _ACTION_COLUMN]):
        selected = [
            index for index, value in enumerate(source["episode_id"].to_pylist())
            if value in train_ids
        ]
        if not selected:
            continue
        action = np.asarray(
            source[_ACTION_COLUMN].take(pa.array(selected)).to_pylist(),
            dtype=np.float64,
        )
        count += len(action)
        total += action.sum(axis=0)
        total_square += np.square(action).sum(axis=0)
    if count == 0:
        raise ValueError("No frame rows belong to the train episodes.")
    mean = total / count
    variance = np.maximum(total_square / count - np.square(mean), 0.0)
    return {
        "frame_count": count,
        "action_mean": mean.tolist(),
        "action_std": np.maximum(
            np.sqrt(variance), _STANDARD_DEVIATION_FLOOR).tolist(),
        "standard_deviation_floor": _STANDARD_DEVIATION_FLOOR,
    }


def _iter_raw(table, columns):
    builder = table.new_read_builder().with_projection(columns)
    read = builder.new_read()
    for split in builder.new_scan().plan().splits():
        yield read.to_arrow([split])


def _read_raw(table, columns):
    builder = table.new_read_builder().with_projection(columns)
    plan = builder.new_scan().plan()
    return builder.new_read().to_arrow(plan.splits())


def _snapshot_id(table):
    raw_table = table.raw_table if hasattr(table, "raw_table") else table
    snapshot = raw_table.snapshot_manager().get_latest_snapshot()
    if snapshot is None:
        raise RuntimeError("Expected a committed Paimon snapshot.")
    return snapshot.id


def _validate_source(h5, source_key):
    lengths = set()
    for _, hdf5_path in NUMERIC_FIELDS:
        if hdf5_path not in h5 or h5[hdf5_path].shape[1:] != (7,):
            raise ValueError(
                "%s: invalid /%s shape." % (source_key, hdf5_path))
        if h5[hdf5_path].dtype != np.dtype("float64"):
            raise ValueError(
                "%s: invalid /%s dtype." % (source_key, hdf5_path))
        lengths.add(int(h5[hdf5_path].shape[0]))
    for _, hdf5_path in IMAGE_FIELDS:
        if hdf5_path not in h5 or len(h5[hdf5_path].shape) != 1:
            raise ValueError(
                "%s: invalid /%s shape." % (source_key, hdf5_path))
        lengths.add(int(h5[hdf5_path].shape[0]))
    if len(lengths) != 1:
        raise ValueError("%s: frame lengths differ." % source_key)
    frame_count = lengths.pop()
    if frame_count <= 0:
        raise ValueError("%s: episode has no frames." % source_key)
    _instruction(h5, source_key)
    _instruction_embedding(h5, source_key)
    return frame_count


def _instruction(h5, source_key):
    if "language_raw" not in h5 or h5["language_raw"].shape != (1,):
        raise ValueError("%s: invalid /language_raw shape." % source_key)
    value = h5["language_raw"][0]
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, str):
        return value
    raise ValueError("%s: /language_raw is not UTF-8 text." % source_key)


def _instruction_embedding(h5, source_key):
    if ("language_distilbert" not in h5
            or h5["language_distilbert"].shape != (1, 1, 768)):
        raise ValueError(
            "%s: invalid /language_distilbert shape." % source_key)
    values = np.asarray(h5["language_distilbert"][0, 0], dtype=np.float32)
    if not np.isfinite(values).all():
        raise ValueError("%s: language embedding contains NaN or Inf." % source_key)
    return values.tolist()


def _path_component(source_key, candidates, label):
    matches = [value for value in Path(source_key).parts if value in candidates]
    if len(matches) != 1:
        raise ValueError(
            "Cannot derive RoboMIND %s from %s." % (label, source_key))
    return matches[0]


def _optional_bool(value):
    return None if value is None else bool(value)


def _positive_int(value, name):
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("%s must be a positive int." % name)
    return value


def _pipeline_summary(result):
    """Return the fixed-size control-plane result printed by the CLI."""
    return {
        "ingest": {
            "mode": result.ingest.mode,
            "episode_count": result.ingest.episode_count,
            "frame_count": result.ingest.frame_count,
            "episodes_snapshot_id": result.ingest.episodes_snapshot_id,
            "frames_snapshot_id": result.ingest.frames_snapshot_id,
        },
        "backfill": asdict(result.backfill),
    }


def main(argv=None):
    """Run the complete local RoboMIND AgileX pipeline from the command line."""
    parser = argparse.ArgumentParser(
        description=(
            "Ingest a downloaded RoboMIND AgileX HDF5 directory and "
            "materialize canonical actions and normalization statistics."
        )
    )
    parser.add_argument(
        "--input", required=True, metavar="DIRECTORY",
        help="downloaded RoboMIND AgileX HDF5 root",
    )
    parser.add_argument(
        "--warehouse", required=True, metavar="DIRECTORY",
        help="new local Paimon warehouse directory",
    )
    parser.add_argument(
        "--database", default=DEFAULT_DATABASE,
        help="Paimon database name (default: %(default)s)",
    )
    parser.add_argument(
        "--batch-size", default=64, type=int, metavar="ROWS",
        help="frame rows per transform batch (default: %(default)s)",
    )
    parser.add_argument(
        "--statistics-version", default=DEFAULT_STATISTICS_VERSION,
        help="version stored with train-split action statistics",
    )
    args = parser.parse_args(argv)
    result = run_local_pipeline(
        args.input,
        args.warehouse,
        database=args.database,
        batch_size=args.batch_size,
        statistics_version=args.statistics_version,
    )
    print(json.dumps(_pipeline_summary(result), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
