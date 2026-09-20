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
import io
import uuid
import json
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import pyarrow as pa

import pypaimon.multimodal as pmm


DEFAULT_DATABASE = "robomind"
INFO_TABLE = "agilex__info"
STAT_TABLE = "agilex__stat"
EPISODES_TABLE = "agilex__episode"
FRAMES_TABLE = "agilex__frame"
DEFAULT_STATISTICS_VERSION = "robomind-agilex-joint-position@1"

TABLE_OPTIONS = {
    "deletion-vectors.enabled": "true",
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
    ("observation_images_rgb_front", "observations/rgb_images/camera_front"),
    (
        "observation_images_rgb_wrist_left",
        "observations/rgb_images/camera_left_wrist",
    ),
    (
        "observation_images_rgb_wrist_right",
        "observations/rgb_images/camera_right_wrist",
    ),
    (
        "observation_images_depth_front",
        "observations/depth_images/camera_front",
    ),
    (
        "observation_images_depth_wrist_left",
        "observations/depth_images/camera_left_wrist",
    ),
    (
        "observation_images_depth_wrist_right",
        "observations/depth_images/camera_right_wrist",
    ),
)

_ACTION_LEFT = "action_joint_position_left"
_ACTION_RIGHT = "action_joint_position_right"
_ACTION_COLUMN = "action"
_ACTION_VECTOR_TYPE = pa.list_(pa.float32(), 14)
_STANDARD_DEVIATION_FLOOR = 1e-2


@dataclass(frozen=True)
class EpisodeSource:
    """Source identity and stable frame range allocated during discovery."""

    path: Path
    source_key: str
    episode_id: str
    split: str
    success: bool
    episode_index: int
    dataset_from_index: int
    frame_count: int


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


@dataclass(frozen=True)
class RayPipelineResult:
    """Result of the complete Ray ingestion and backfill pipeline."""

    ingest: IngestResult
    backfill: BackfillResult


def info_schema():
    """One typed group description per published Tag."""
    return pa.schema([
        pa.field("group_id", pa.string(), nullable=False),
        pa.field("robot", pa.struct([
            pa.field("type", pa.string(), nullable=False),
            pa.field("instance_id", pa.string()),
        ]), nullable=False),
        pa.field("storage_mode", pa.string(), nullable=False),
        pa.field("fps", pa.float64()),
        pa.field("total_episodes", pa.int64(), nullable=False),
        pa.field("total_frames", pa.int64(), nullable=False),
        pa.field("total_tasks", pa.int64()),
        pa.field("features", pa.string(), nullable=False),
        pa.field("quality_statuses", pa.map_(pa.int32(), pa.string()), nullable=False),
        pa.field("tag", pa.string(), nullable=False),
        pa.field("tables", pa.map_(pa.string(), pa.string()), nullable=False),
        pa.field("metadata", pa.string()),
    ])


def stat_schema():
    """Feature moments and optional ACT normalization metadata in one table."""
    return pa.schema([
        pa.field("feature", pa.string(), nullable=False),
        pa.field("split", pa.string(), nullable=False),
        pa.field("source_table", pa.string(), nullable=False),
        pa.field("source_tag", pa.string(), nullable=False),
        pa.field("stats", pa.string(), nullable=False),
    ])


def episode_schema():
    """Return the contract episode schema; source-specific values stay in metadata."""
    return pa.schema([
        pa.field("episode_index", pa.int64(), nullable=False),
        pa.field("frame_count", pa.int64(), nullable=False),
        pa.field("dataset_from_index", pa.int64(), nullable=False),
        pa.field("dataset_to_index", pa.int64(), nullable=False),
        pa.field("split", pa.string(), nullable=False),
        pa.field("success", pa.bool_()),
        pa.field("quality_status", pa.int32(), nullable=False),
        pa.field("instruction", pa.string()),
        pa.field("task_index", pa.int64()),
        pa.field("source_episode_key", pa.string()),
        pa.field("stats", pa.string()),
        pa.field("metadata", pa.string()),
    ])


def frame_schema():
    """Return the shared AgileX frame schema before canonical backfill."""
    fields = [
        pa.field("episode_id", pa.string(), nullable=False),
        pa.field("frame_index", pa.int64(), nullable=False),
        pa.field("episode_index", pa.int64(), nullable=False),
        pa.field("index", pa.int64(), nullable=False),
        pa.field("timestamp_ns", pa.int64()),
        pa.field("quality_status", pa.int32(), nullable=False),
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


def discover_episodes(input_root):
    """Allocate identities from sorted source paths and HDF5 row counts.

    This importer creates a new group from complete episodes. It refuses an
    existing destination; these initial IDs must never be recomputed for an
    append or an update of an already published group.
    """
    import h5py
    root = Path(input_root).expanduser().resolve()
    if not root.is_dir():
        raise ValueError("RoboMIND input root does not exist: %s" % root)
    paths = sorted(root.glob("**/data/trajectory.hdf5"))
    if not paths:
        raise ValueError("No RoboMIND trajectory.hdf5 files found below %s." % root)

    episodes = []
    episode_ids = set()
    global_index = 0
    for path in paths:
        resolved_path = path.resolve()
        try:
            resolved_path.relative_to(root)
        except ValueError:
            raise ValueError(
                "RoboMIND trajectory path escapes input root: %s" % path)
        if path.is_symlink():
            raise ValueError(
                "RoboMIND trajectory path must not be a symlink: %s" % path)
        source_key = path.relative_to(root).as_posix()
        split = _path_component(source_key, ("train", "val"), "split")
        status = _path_component(
            source_key, ("success_episodes", "failed_episodes"), "status")
        episode_id = path.parent.parent.name
        if episode_id in episode_ids:
            raise ValueError("Duplicate RoboMIND episode_id %r." % episode_id)
        episode_ids.add(episode_id)
        with h5py.File(resolved_path, "r") as h5:
            frame_count = int(h5["puppet/joint_position_left"].shape[0])
        episodes.append(EpisodeSource(
            path=resolved_path,
            source_key=source_key,
            episode_id=episode_id,
            split="eval" if split == "val" else split,
            success=status == "success_episodes",
            episode_index=len(episodes),
            dataset_from_index=global_index,
            frame_count=frame_count,
        ))
        global_index += frame_count
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
        if frame_count != episode.frame_count:
            raise ValueError("Source frame count changed after discovery.")
        yield self._episode_batch(h5, episode, frame_count)

    @staticmethod
    def _episode_batch(h5, episode, frame_count):
        instruction = _instruction(h5, episode.source_key)
        embedding = _instruction_embedding(h5, episode.source_key)
        return pa.RecordBatch.from_pydict({
            "episode_index": [episode.episode_index],
            "dataset_from_index": [episode.dataset_from_index],
            "dataset_to_index": [episode.dataset_from_index + frame_count],
            "source_episode_key": [episode.episode_id],
            "quality_status": [0],
            "task_index": [None],
            "stats": [None],
            "split": [episode.split],
            "success": [episode.success],
            "instruction": [instruction],
            "frame_count": [frame_count],
            "metadata": [json.dumps({
                "source": {"uri": episode.path.as_uri()},
                "source_key": episode.source_key,
                "instruction_embedding": embedding,
                "hdf5": {
                    "compress": _optional_bool(h5.attrs.get("compress")),
                    "sim": _optional_bool(h5.attrs.get("sim")),
                },
            })],
        }, schema=episode_schema())


class RoboMindAgileXFrameTransform(_RoboMindAgileXTransform):
    """Validate one AgileX file and stream its frame rows."""

    def __init__(self, episodes, *, batch_size=64):
        super().__init__(episodes)
        self.batch_size = _positive_int(batch_size, "batch_size")

    def __call__(self, h5, source):
        episode = self._source(source)
        frame_count = _validate_source(h5, episode.source_key)
        if frame_count != episode.frame_count:
            raise ValueError("Source frame count changed after discovery.")
        for begin in range(0, frame_count, self.batch_size):
            end = min(begin + self.batch_size, frame_count)
            count = end - begin
            columns = {
                "episode_id": [episode.episode_id] * count,
                "frame_index": np.arange(begin, end, dtype=np.int64),
                "episode_index": [episode.episode_index] * count,
                "index": np.arange(episode.dataset_from_index + begin,
                                   episode.dataset_from_index + end,
                                   dtype=np.int64),
                "timestamp_ns": [None] * count,
                "quality_status": [0] * count,
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
                columns[name] = pa.FixedSizeListArray.from_arrays(
                    pa.array(values.reshape(-1), type=pa.float64()), 7)
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


def _require_ray_250(ray):
    from packaging.version import parse

    if parse(ray.__version__) < parse("2.50.0"):
        raise RuntimeError(
            "RoboMIND Ray backfill requires ray>=2.50; installed ray is %s."
            % ray.__version__)


def run_ray_pipeline(
        input_root,
        warehouse,
        *,
        database=DEFAULT_DATABASE,
        batch_size=64,
        concurrency=None,
        statistics_version=DEFAULT_STATISTICS_VERSION,
        num_partitions=None,
        ray_address=None):
    """Run Ray ingestion and backfill on one managed Ray cluster."""
    if num_partitions is not None:
        num_partitions = _positive_int(num_partitions, "num_partitions")
    if not isinstance(statistics_version, str) or not statistics_version:
        raise ValueError("statistics_version must be a non-empty string.")

    try:
        import ray
    except ImportError:
        raise ImportError(
            "Ray pipeline requires ray; install pypaimon[ray,hdf5].")
    _require_ray_250(ray)

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
        ingest = ingest_ray(
            input_root,
            warehouse,
            database=database,
            batch_size=batch_size,
            concurrency=concurrency,
        )
        backfill = backfill_canonical_action_ray(
            warehouse,
            database=database,
            statistics_version=statistics_version,
            num_partitions=num_partitions,
        )
        return RayPipelineResult(ingest=ingest, backfill=backfill)
    finally:
        if initialized_here:
            ray.shutdown()


def ingest_ray(
        input_root,
        warehouse,
        *,
        database=DEFAULT_DATABASE,
        batch_size=64,
        concurrency=None):
    """Ingest AgileX through an already initialized Ray cluster."""
    if concurrency is not None:
        concurrency = _positive_int(concurrency, "concurrency")
    episodes = discover_episodes(input_root)
    _create_tables(warehouse, database)

    from pypaimon.ray import load_from_hdf5
    catalog_options = {
        "warehouse": str(Path(warehouse).expanduser().resolve())}
    paths = [episode.path for episode in episodes]
    episode_result = load_from_hdf5(
        "%s.%s" % (database, EPISODES_TABLE), paths, catalog_options,
        transform=RoboMindAgileXEpisodeTransform(episodes),
        concurrency=concurrency,
    )
    frame_result = load_from_hdf5(
        "%s.%s" % (database, FRAMES_TABLE), paths, catalog_options,
        transform=RoboMindAgileXFrameTransform(
            episodes, batch_size=batch_size),
        concurrency=concurrency,
    )
    return IngestResult(
        mode="ray",
        episode_count=episode_result.row_count,
        frame_count=frame_result.row_count,
        episodes_snapshot_id=episode_result.snapshot_id,
        frames_snapshot_id=frame_result.snapshot_id,
    )


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


def backfill_canonical_action(
        warehouse,
        *,
        database=DEFAULT_DATABASE,
        statistics_version=DEFAULT_STATISTICS_VERSION):
    """Materialize action, compute statistics, and publish a new group Tag."""
    _reject_published_tag(warehouse, database, statistics_version)
    row_count, frames_snapshot_id = materialize_canonical_action(
        warehouse, database=database)
    statistics_snapshot_id = refresh_action_statistics(
        warehouse,
        database=database,
        statistics_version=statistics_version,
    )
    publish_contract(warehouse, database=database, tag=statistics_version)
    return BackfillResult(
        row_count=row_count,
        frames_snapshot_id=frames_snapshot_id,
        statistics_snapshot_id=statistics_snapshot_id,
        statistics_version=statistics_version,
    )


def backfill_canonical_action_ray(
        warehouse,
        *,
        database=DEFAULT_DATABASE,
        statistics_version=DEFAULT_STATISTICS_VERSION,
        num_partitions=None):
    """Run distributed backfill on an already initialized Ray cluster."""
    try:
        import ray
    except ImportError:
        raise ImportError(
            "Ray backfill requires ray; install pypaimon[ray].")
    _require_ray_250(ray)

    _reject_published_tag(warehouse, database, statistics_version)
    row_count, frames_snapshot_id = _materialize_canonical_action_ray(
        warehouse,
        database=database,
        num_partitions=num_partitions,
    )
    statistics_snapshot_id = refresh_action_statistics(
        warehouse,
        database=database,
        statistics_version=statistics_version,
    )
    publish_contract(warehouse, database=database, tag=statistics_version)
    return BackfillResult(
        row_count=row_count,
        frames_snapshot_id=frames_snapshot_id,
        statistics_snapshot_id=statistics_snapshot_id,
        statistics_version=statistics_version,
    )


def materialize_canonical_action(warehouse, *, database=DEFAULT_DATABASE):
    """Stage one: add and populate canonical action, then commit it."""
    connection, frames_table = _prepare_canonical_action_table(
        warehouse, database)
    row_count = _update_canonical_action_batches(frames_table.raw_table)
    frames_table = connection.get_table(FRAMES_TABLE)
    frames_snapshot_id = _snapshot_id(frames_table)
    return row_count, frames_snapshot_id


def _materialize_canonical_action_ray(
        warehouse,
        *,
        database=DEFAULT_DATABASE,
        num_partitions=None):
    """Stage one: materialize canonical action with Ray self-merge."""
    if num_partitions is not None:
        num_partitions = _positive_int(num_partitions, "num_partitions")

    from pypaimon.ray import WhenMatched, merge_into

    connection, _ = _prepare_canonical_action_table(
        warehouse, database)
    catalog_options = {
        "warehouse": str(Path(warehouse).expanduser().resolve())}

    def canonical_action(rows):
        return build_canonical_action_backfill(rows)[_ACTION_COLUMN]

    target = "%s.%s" % (database, FRAMES_TABLE)
    result = merge_into(
        target,
        target,
        catalog_options,
        on=["_ROW_ID"],
        when_matched=[WhenMatched.update({
            _ACTION_COLUMN: canonical_action,
        })],
        read_columns=[_ACTION_LEFT, _ACTION_RIGHT],
        num_partitions=num_partitions,
    )

    frames_table = connection.get_table(FRAMES_TABLE)
    return result["num_matched"], _snapshot_id(frames_table)


def _prepare_canonical_action_table(warehouse, database):
    connection = pmm.connect(
        database=database,
        options={"warehouse": str(Path(warehouse).expanduser().resolve())},
    )
    frames_table = connection.get_table(FRAMES_TABLE)

    from pypaimon.schema.data_types import AtomicType, VectorType
    from pypaimon.schema.schema_change import SchemaChange

    action_type = VectorType(True, AtomicType("FLOAT"), 14)
    if _validate_backfill_target(frames_table, action_type):
        connection.catalog.alter_table(
            frames_table.identifier,
            [SchemaChange.add_column(
                _ACTION_COLUMN,
                action_type,
                comment=(
                    "Canonical AgileX action: master joint position left "
                    "followed by right."),
            )],
            False,
        )
        frames_table = connection.get_table(FRAMES_TABLE)
    return connection, frames_table


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
        episodes_table.raw_table, ["source_episode_key", "split", "success", "quality_status"])
    train_episode_ids = sorted(
        row["source_episode_key"]
        for row in episode_rows.to_pylist()
        if row["split"] == "train" and row["success"] and row["quality_status"] == 0
    )
    frames_snapshot_id = _snapshot_id(frames_table)
    train_ids = [row["source_episode_key"] for row in episode_rows.to_pylist()
                 if row["split"] == "train" and row["quality_status"] == 0]
    statistics = _stream_action_statistics(
        frames_table.raw_table, train_ids, valid_only=True)
    act = _stream_action_statistics(
        frames_table.raw_table, train_episode_ids, valid_only=True)
    statistics["source_snapshot_id"] = frames_snapshot_id
    statistics["act"] = {
        "count": act["count"], "mean": act["mean"], "std": act["std"],
        "std_floor": _STANDARD_DEVIATION_FLOOR,
        "episode_filter": "success = true AND quality_status = 0",
        "split_manifest_sha256": hashlib.sha256(
            "".join("%s\n" % value for value in train_episode_ids)
            .encode("utf-8")).hexdigest(),
    }
    stats_table = connection.create_table(
        STAT_TABLE, schema=stat_schema(), options=TABLE_OPTIONS, ignore_if_exists=True)
    stats_table.overwrite(pa.Table.from_pylist([{
        "feature": _ACTION_COLUMN,
        "split": "train",
        "source_table": frames_table.identifier,
        "source_tag": statistics_version,
        "stats": json.dumps(statistics),
    }], schema=stat_schema()))
    return _snapshot_id(stats_table)


def _reject_published_tag(warehouse, database, tag):
    if not isinstance(tag, str) or not tag.strip() or any(
            char in tag for char in ("/", "\\", "\0")):
        raise ValueError("statistics_version must be a non-empty valid Tag name.")
    connection = pmm.connect(database=database, options={"warehouse": str(warehouse)})
    existing_tables = connection.catalog.list_tables(database)
    for name in (INFO_TABLE, EPISODES_TABLE, FRAMES_TABLE,
                 STAT_TABLE):
        if name in existing_tables:
            member = connection.get_table(name)
            if member.raw_table.tag_manager().tag_exists(tag):
                raise ValueError(
                    "Group Tag %r is already published or partially published; "
                    "use a new Tag name." % tag)


def _frame_features(frames):
    """Describe the actual wide-table schema and encoded image dimensions."""
    from PIL import Image

    image_names = [name for name, _ in IMAGE_FIELDS]
    image_row = frames.scan().select(image_names).limit(1).to_list()[0]
    features = {}
    for field in backfilled_frame_schema():
        spec = {"dtype": "float64", "shape": [7], "names": None}
        if field.name in image_names:
            with Image.open(io.BytesIO(image_row[field.name])) as image:
                channels = len(image.getbands())
                spec = {"dtype": "image",
                        "shape": [image.height, image.width, channels],
                        "names": ["height", "width", "channels"]}
        elif field.name == _ACTION_COLUMN:
            spec = {"dtype": "float32", "shape": [14], "names": None}
        elif not pa.types.is_fixed_size_list(field.type):
            dtype = ("string" if pa.types.is_string(field.type)
                     else "int32" if pa.types.is_int32(field.type) else "int64")
            spec = {"dtype": dtype, "shape": [1], "names": None}
        features[field.name] = spec
    return features


def publish_contract(warehouse, *, database=DEFAULT_DATABASE, tag):
    """Publish member Tags first and expose the single info row last.

    The benchmark is a single-writer workflow. Independent table APIs do not
    provide a cross-table transaction; failed publication has no readable info
    Tag and existing Tags remain untouched. After a partial publication,
    rerun backfill with a new Tag name; never move already created member Tags.
    """
    _reject_published_tag(warehouse, database, tag)
    connection = pmm.connect(database=database, options={"warehouse": str(warehouse)})
    episodes = connection.get_table(EPISODES_TABLE)
    frames = connection.get_table(FRAMES_TABLE)
    rows = episodes.scan().to_list()
    if STAT_TABLE not in connection.catalog.list_tables(database):
        raise ValueError("ACT statistics are missing for the frame snapshot.")
    stat = connection.get_table(STAT_TABLE)
    normalization = stat.scan().where(
        "feature = 'action' AND split = 'train' AND source_tag = '%s'"
        % tag.replace("'", "''")).to_list()
    if (len(normalization) != 1
            or json.loads(normalization[0]["stats"])["source_snapshot_id"] != _snapshot_id(frames)):
        raise ValueError("ACT statistics must uniquely match the current frame snapshot.")
    features = _frame_features(frames)
    tables = {
        "episode": episodes.identifier, "frame": frames.identifier,
        "stat": stat.identifier,
    }
    info = connection.create_table(INFO_TABLE, schema=info_schema(),
                                   options=TABLE_OPTIONS, ignore_if_exists=True)
    previous = (info.scan().to_list()
                if info.raw_table.snapshot_manager().get_latest_snapshot() else [])
    info_row = {
        "group_id": previous[0]["group_id"] if previous else str(uuid.uuid4()),
        "robot": {"type": "robomind_agilex", "instance_id": None},
        "storage_mode": "frame", "fps": None,
        "total_episodes": len(rows),
        "total_frames": sum(row["frame_count"] for row in rows),
        "total_tasks": None, "features": json.dumps(features),
        "quality_statuses": {0: "valid", 1: "review", 2: "invalid"},
        "tag": tag, "tables": tables,
        "metadata": json.dumps({
            "source": {"format": "robomind_hdf5", "dataset": "RoboMIND",
                       "adapter": "benchmark.act.robomind_agilex"},
            "action": {"source": "master joint position left then right",
                       "semantics": "demonstration supervision; receipt by the robot is not verified"},
        }),
    }
    for name in tables.values():
        member = connection.get_table(name)
        snapshot_id = _snapshot_id(member)
        existing = member.raw_table.tag_manager().get(tag)
        if existing is not None:
            if existing.id != snapshot_id:
                raise ValueError("Member Tag %r points to a different snapshot." % tag)
        else:
            member.raw_table.create_tag(tag, snapshot_id=snapshot_id)
    info.overwrite(pa.Table.from_pylist([info_row], schema=info_schema()))
    connection.get_table(INFO_TABLE).raw_table.create_tag(tag)


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


def _validate_backfill_target(frames_table, action_type):
    missing = [
        name for name in (_ACTION_LEFT, _ACTION_RIGHT)
        if name not in frames_table.raw_table.field_names
    ]
    if missing:
        raise ValueError("Frames table is missing raw action columns: %s." % missing)
    action_field = next(
        (field for field in frames_table.raw_table.table_schema.fields
         if field.name == _ACTION_COLUMN),
        None,
    )
    if action_field is None:
        return True
    if action_field.type != action_type:
        raise ValueError(
            "Canonical action column has incompatible type: %s."
            % action_field.type)
    return False


def _update_canonical_action_batches(table):
    """Transform one planned Paimon split at a time and commit all updates once."""
    builder = table.new_batch_write_builder()
    commit = builder.new_commit()
    row_count = 0

    def updates():
        nonlocal row_count
        for source in _iter_raw(
                table, [_ACTION_LEFT, _ACTION_RIGHT, "_ROW_ID"]):
            if source.num_rows == 0:
                continue
            update = build_canonical_action_backfill(source)
            row_count += len(update)
            yield update

    try:
        update_batches = updates()
        first = next(update_batches, None)
        if first is None:
            messages = []
        else:
            def all_updates():
                yield first
                yield from update_batches

            messages = (
                builder.new_update()
                .with_update_type([_ACTION_COLUMN])
                .update_by_arrow_batches_with_row_id(all_updates())
            )
        commit.commit(messages)
    finally:
        commit.close()
    return row_count


def _stream_action_statistics(table, train_episode_ids, *, valid_only=False):
    """Accumulate fixed-size count, running mean, and M2 on the driver."""
    train_ids = set(train_episode_ids)
    if not train_ids:
        raise ValueError("Cannot compute action statistics without train episodes.")
    count = 0
    mean = np.zeros(14, dtype=np.float64)
    m2 = np.zeros(14, dtype=np.float64)
    minimum = np.full(14, np.inf)
    maximum = np.full(14, -np.inf)
    columns = ["episode_id", _ACTION_COLUMN]
    if valid_only:
        columns.append("quality_status")
    for source in _iter_raw(table, columns):
        quality = source["quality_status"].to_pylist() if valid_only else None
        selected = [
            index for index, value in enumerate(source["episode_id"].to_pylist())
            if value in train_ids and (quality is None or quality[index] == 0)
        ]
        if not selected:
            continue
        action = np.asarray(
            source[_ACTION_COLUMN].take(pa.array(selected)).to_pylist(),
            dtype=np.float64,
        )
        if not np.isfinite(action).all():
            raise ValueError("Valid action contains NaN or Inf.")
        minimum = np.minimum(minimum, action.min(axis=0))
        maximum = np.maximum(maximum, action.max(axis=0))
        batch_count = len(action)
        batch_mean = action.mean(axis=0)
        batch_m2 = np.square(action - batch_mean).sum(axis=0)
        delta = batch_mean - mean
        combined_count = count + batch_count
        mean += delta * batch_count / combined_count
        m2 += (
            batch_m2
            + np.square(delta) * count * batch_count / combined_count
        )
        count = combined_count
    if count == 0:
        raise ValueError("No frame rows belong to the train episodes.")
    variance = np.maximum(m2 / count, 0.0)
    return {"count": count, "min": minimum.tolist(),
            "max": maximum.tolist(), "mean": mean.tolist(),
            "std": np.sqrt(variance).tolist()}


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
    if "language_raw" not in h5:
        return None
    if h5["language_raw"].shape != (1,):
        raise ValueError("%s: invalid /language_raw shape." % source_key)
    value = h5["language_raw"][0]
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, str):
        return value
    raise ValueError("%s: /language_raw is not UTF-8 text." % source_key)


def _instruction_embedding(h5, source_key):
    if "language_distilbert" not in h5:
        return None
    if h5["language_distilbert"].shape != (1, 1, 768):
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
