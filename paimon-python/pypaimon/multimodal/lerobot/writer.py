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

"""LeRobot-compatible capture writer for multimodal Paimon tables."""

import copy
import io
import json
from typing import Mapping, Optional, Sequence

import numpy as np
import pyarrow as pa

from pypaimon.catalog.catalog_exception import (
    DatabaseNotExistException,
    TableNotExistException,
)
from pypaimon.multimodal.arrow_utils import strict_arrow_table
from pypaimon.multimodal.hdf5 import _SnapshotRecorder
from pypaimon.multimodal.lerobot.metadata import (
    _COMPANION_OPTION_KEYS,
    _EMPTY_EPISODES_SCHEMA,
    _append_arrow,
    _companion_table_identifiers,
    _managed_table_options,
    _metadata_table,
    _overwrite_arrow,
    _prepare_metadata_tables,
)
from pypaimon.multimodal.lerobot.loader import (
    _encode_media_frame,
    _normalize_value,
    _safe_array,
    _value_shape,
)
from pypaimon.multimodal.lerobot.schema import (
    _feature_shape,
    _schema_from_info,
    _validate_lerobot_schema,
    _validate_v3_required_features,
    _video_feature_names,
)
from pypaimon.multimodal.table import _target_schema


_DEFAULT_FEATURES = {
    "timestamp": {"dtype": "float32", "shape": (1,), "names": None},
    "frame_index": {"dtype": "int64", "shape": (1,), "names": None},
    "episode_index": {"dtype": "int64", "shape": (1,), "names": None},
    "index": {"dtype": "int64", "shape": (1,), "names": None},
    "task_index": {"dtype": "int64", "shape": (1,), "names": None},
}
_STATE_PREFIX = "pypaimon.lerobot."
_STATE_VERSION = _STATE_PREFIX + "state-version"
_STATE_VERSION_VALUE = "1"
_NEXT_INDEX = _STATE_PREFIX + "next-index"
_NEXT_EPISODE_INDEX = _STATE_PREFIX + "next-episode-index"
_STAT_NAMES = (
    "min", "max", "mean", "std", "count",
    "q01", "q10", "q50", "q90", "q99",
)
_INTEGER_DTYPES = {
    "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32",
}
_EPISODE_STATE_COLUMNS = list(_EMPTY_EPISODES_SCHEMA.names) + [
    "stats/index/count",
]


def _read_arrow(table, projection=None):
    builder = table.new_read_builder()
    if projection is not None:
        builder = builder.with_projection(projection)
    return builder.new_read().to_arrow(builder.new_scan().plan().splits())


def _lerobot_stats_functions():
    try:
        from lerobot.datasets.compute_stats import (
            aggregate_stats,
            auto_downsample_height_width,
            compute_episode_stats,
            get_feature_stats,
            sample_indices,
        )
    except ImportError as error:
        raise ImportError(
            "PaimonLeRobotWriter statistics require LeRobot; install "
            "'pypaimon[lerobot]'.") from error
    return (aggregate_stats, auto_downsample_height_width,
            compute_episode_stats, get_feature_stats, sample_indices)


def _nested_list_type(value_type, depth):
    for _ in range(depth):
        value_type = pa.list_(value_type)
    return value_type


def _episode_schema(features):
    fields = list(_EMPTY_EPISODES_SCHEMA)
    for name, feature in features.items():
        dtype = str(feature.get("dtype", ""))
        if dtype == "string":
            continue
        feature_shape = _feature_shape(feature, name)
        for stat in _STAT_NAMES:
            if stat == "count":
                value_type = pa.int64()
                depth = 1
            elif dtype == "image":
                value_type = pa.float64()
                depth = 3
            elif stat in ("min", "max") and dtype in _INTEGER_DTYPES:
                value_type = pa.int64()
            elif stat in ("min", "max") and dtype in ("bool", "boolean"):
                value_type = pa.bool_()
            else:
                value_type = pa.float64()
            if stat != "count" and dtype != "image":
                depth = max(1, len(feature_shape))
            fields.append(pa.field(
                "stats/%s/%s" % (name, stat),
                _nested_list_type(value_type, depth),
                nullable=False,
            ))
    return pa.schema(fields)


def _metadata_values(table, component):
    result = {}
    for row in _read_arrow(table).to_pylist():
        if row["key"] in result:
            raise ValueError(
                "Existing LeRobot %s metadata repeats key %r."
                % (component, row["key"]))
        try:
            result[row["key"]] = json.loads(row["value"])
        except (TypeError, ValueError) as error:
            raise ValueError(
                "Existing LeRobot %s metadata is invalid."
                % component) from error
    return result


def _image_stats(values):
    try:
        from PIL import Image
    except ImportError as error:
        raise ImportError(
            "PaimonLeRobotWriter image statistics require Pillow from "
            "'pypaimon[lerobot]'.") from error
    (_, downsample, _, get_feature_stats,
     sample_indices) = _lerobot_stats_functions()
    images = []
    for index in sample_indices(len(values)):
        with Image.open(io.BytesIO(values[index])) as image:
            array = np.asarray(image.convert("RGB"), dtype=np.uint8)
        images.append(downsample(np.transpose(array, (2, 0, 1))))
    stats = get_feature_stats(
        np.stack(images), axis=(0, 2, 3), keepdims=True)
    return {
        name: value if name == "count" else np.squeeze(
            value / 255.0, axis=0)
        for name, value in stats.items()
    }


def _compute_stats(episode, features):
    _, _, compute_episode_stats, _, _ = \
        _lerobot_stats_functions()
    data = {}
    numeric_features = {}
    reshaped_features = {}
    result = {}
    for name, feature in features.items():
        dtype = str(feature.get("dtype", ""))
        values = episode.column(name).to_pylist()
        if dtype == "image":
            result[name] = _image_stats(values)
        else:
            numeric_features[name] = feature
            array = (
                values if dtype == "string" else np.asarray(
                    values,
                    dtype=np.dtype("bool" if dtype == "boolean" else dtype),
                )
            )
            if dtype != "string" and array.ndim > 2:
                # Keep higher-rank stats stable across one- and multi-frame
                # episodes while delegating the calculation to LeRobot.
                reshaped_features[name] = array.shape[1:]
                array = array.reshape(array.shape[0], -1)
            data[name] = array
    numeric_stats = compute_episode_stats(data, numeric_features)
    for name, shape in reshaped_features.items():
        numeric_stats[name] = {
            stat: value if stat == "count" else value.reshape(shape)
            for stat, value in numeric_stats[name].items()
        }
    result.update(numeric_stats)
    return result


def _aggregate_stats(stats_list, features):
    aggregate_stats = _lerobot_stats_functions()[0]
    result = {}
    for name, feature in features.items():
        if feature.get("dtype") == "string":
            continue
        key = "image" if feature.get("dtype") == "image" else "feature"
        result[name] = aggregate_stats([
            {key: stats[name]} for stats in stats_list
        ])[key]
    return result


def _indexed_metadata_table(component, entries):
    import pandas as pd

    entries = list(entries)
    indices, labels = zip(*entries) if entries else ((), ())
    return pa.Table.from_pandas(pd.DataFrame(
        {component + "_index": np.asarray(indices, dtype=np.int64)},
        index=pd.Index(
            labels, dtype="string", name=component,
        ),
    ))


def _subtasks_table(subtasks):
    return _indexed_metadata_table("subtask", enumerate(subtasks))


def _validate_subtasks(subtasks, has_feature):
    if subtasks is None:
        return None
    if not has_feature:
        raise ValueError(
            "subtasks require a subtask_index feature.")
    if isinstance(subtasks, (str, bytes)) \
            or not isinstance(subtasks, Sequence):
        raise ValueError("subtasks must be a sequence of strings.")
    result = tuple(subtasks)
    if not result or any(not isinstance(value, str) or not value
                         for value in result):
        raise ValueError("subtasks must contain non-empty strings.")
    if len(set(result)) != len(result):
        raise ValueError("subtasks must not contain duplicates.")
    return result


class PaimonLeRobotWriter:
    """Collect LeRobot frames and commit completed episodes to Paimon."""

    def __init__(
            self,
            connection,
            table_name: str,
            *,
            fps: int,
            features: Mapping[str, Mapping[str, object]],
            subtasks: Optional[Sequence[str]] = None,
            episodes_per_commit: int = -1,
            options: Optional[Mapping[str, object]] = None):
        if isinstance(fps, bool) or not isinstance(fps, int) or fps <= 0:
            raise ValueError("fps must be a positive integer.")
        if isinstance(episodes_per_commit, bool) \
                or not isinstance(episodes_per_commit, int) \
                or episodes_per_commit == 0 \
                or episodes_per_commit < -1:
            raise ValueError(
                "episodes_per_commit must be -1 or a positive integer.")
        if not isinstance(features, Mapping) or not features:
            raise ValueError("features must be a non-empty mapping.")
        if "task" in features:
            raise ValueError("task is managed by PaimonLeRobotWriter.")
        video_fields = _video_feature_names({"features": features})
        if video_fields:
            raise ValueError(
                "PaimonLeRobotWriter does not support video features: %s."
                % ", ".join(video_fields))
        requested_subtasks = _validate_subtasks(
            subtasks, "subtask_index" in features)
        _lerobot_stats_functions()

        self.fps = fps
        self.episodes_per_commit = episodes_per_commit
        self.features = copy.deepcopy(dict(features))
        self._user_features = copy.deepcopy(dict(features))
        self.features.update(copy.deepcopy(_DEFAULT_FEATURES))
        _validate_v3_required_features({"features": self.features})
        self._source_schema = _schema_from_info({"features": self.features})
        metadata = self._writer_metadata(
            fps, self.features, requested_subtasks)
        self._episodes_schema = metadata["episodes_schema"]
        create_options = dict(options or {})
        reserved_options = set(_COMPANION_OPTION_KEYS.values()).intersection(
            create_options)
        if reserved_options:
            raise ValueError(
                "%s are managed by PaimonLeRobotWriter."
                % sorted(reserved_options))
        create_options.update(_managed_table_options(
            connection._identifier(table_name), metadata))
        try:
            self._table = connection.get_table(table_name)
            created = False
        except (DatabaseNotExistException, TableNotExistException):
            if "subtask_index" in self.features \
                    and requested_subtasks is None:
                raise ValueError(
                    "subtasks are required when creating a table with "
                    "subtask_index.")
            self._table = connection.create_table(
                table_name,
                schema=self._source_schema,
                options=create_options,
            )
            created = True
        self._target_schema = _target_schema(self._table.raw_table)
        _validate_lerobot_schema(
            self._source_schema, self._target_schema, table_name)
        strict_arrow_table(
            pa.Table.from_batches([], schema=self._source_schema),
            self._target_schema,
            table_name,
            0,
            "LeRobot",
        )
        self._metadata_tables = (
            _prepare_metadata_tables(
                connection, self._table.raw_table, metadata)
            if created else self._open_metadata_tables(
                connection, self._table.raw_table, metadata)
        )

        (self.num_frames, self.num_episodes, self._task_indices,
         self._stats, stored_subtasks) = self._load_existing_state()
        if stored_subtasks is not None:
            if requested_subtasks is not None \
                    and requested_subtasks != stored_subtasks:
                raise ValueError(
                    "subtasks do not match the existing LeRobot table.")
            self.subtasks = stored_subtasks
        else:
            self.subtasks = requested_subtasks
        if "subtask_index" in self.features and self.subtasks is None:
            raise ValueError(
                "subtasks are required when creating a table with "
                "subtask_index.")
        self._next_task_index = (
            max(self._task_indices.values()) + 1
            if self._task_indices else 0
        )
        self.pending_episodes = 0
        self._episode_frames = []
        self._pending_episode_rows = []
        self._committed_task_count = len(self._task_indices)
        self._subtasks_committed = self.num_frames > 0
        self._table_write = None
        self._table_commit = None
        self._snapshot_recorder = None
        self._finalized = False
        self._failed = False

    @staticmethod
    def _writer_metadata(fps, features, subtasks):
        info = {
            "codebase_version": "v3.0",
            "fps": fps,
            "features": features,
            "total_frames": 0,
            "total_episodes": 0,
            "total_tasks": 0,
            "splits": {},
        }
        return {
            "info_table": _metadata_table(info),
            "episodes_schema": _episode_schema(features),
            "tasks_table": _indexed_metadata_table("task", ()),
            "stats_table": _metadata_table({}),
            "subtasks_table": (
                _subtasks_table(subtasks or ())
                if "subtask_index" in features else None
            ),
        }

    @staticmethod
    def _open_metadata_tables(connection, frames_table, metadata):
        identifiers = _companion_table_identifiers(frames_table)
        expected = {
            "info": metadata["info_table"].schema,
            "episodes": metadata["episodes_schema"],
            "tasks": metadata["tasks_table"].schema,
            "stats": metadata["stats_table"].schema,
        }
        if metadata["subtasks_table"] is not None:
            expected["subtasks"] = metadata["subtasks_table"].schema
        if set(identifiers) != set(expected):
            raise ValueError(
                "PaimonLeRobotWriter companion tables do not match "
                "the declared features.")
        tables = {
            name: connection.catalog.get_table(identifier)
            for name, identifier in identifiers.items()
        }
        for name, table in tables.items():
            if not _target_schema(table).equals(
                    expected[name], check_metadata=False):
                raise ValueError(
                    "LeRobot %s companion schema does not match "
                    "PaimonLeRobotWriter." % name)
        return tables

    def _load_existing_state(self):
        snapshot = self._table.raw_table.snapshot_manager() \
            .get_latest_snapshot()
        if snapshot is None:
            if any(table.snapshot_manager().get_latest_snapshot() is not None
                   for table in self._metadata_tables.values()):
                raise ValueError(
                    "Existing LeRobot table group state is inconsistent.")
            return 0, 0, {}, None, None
        properties = snapshot.properties or {}
        if _STATE_VERSION in properties:
            state = self._state_from_snapshot_properties(properties)
            metadata_state = self._state_from_companion_tables()
            if state != metadata_state[:2]:
                raise ValueError(
                    "Existing LeRobot table group state is inconsistent.")
            return metadata_state
        if any(key.startswith(_STATE_PREFIX) for key in properties):
            raise ValueError("Existing LeRobot snapshot state is incomplete.")
        return self._state_from_companion_tables()

    def _state_from_companion_tables(self):
        task_rows = _read_arrow(self._metadata_tables["tasks"]).to_pylist()
        task_rows.sort(key=lambda row: row["task_index"])
        task_indices = {}
        for expected, row in enumerate(task_rows):
            task = row["task"]
            if row["task_index"] != expected or not isinstance(task, str) \
                    or task in task_indices:
                raise ValueError(
                    "Existing LeRobot task metadata is invalid.")
            task_indices[task] = expected

        subtasks = None
        if "subtasks" in self._metadata_tables:
            subtask_rows = _read_arrow(
                self._metadata_tables["subtasks"]).to_pylist()
            subtask_rows.sort(key=lambda row: row["subtask_index"])
            labels = []
            for expected, row in enumerate(subtask_rows):
                label = row["subtask"]
                if row["subtask_index"] != expected \
                        or not isinstance(label, str) or not label \
                        or label in labels:
                    raise ValueError(
                        "Existing LeRobot subtask metadata is invalid.")
                labels.append(label)
            if not labels:
                raise ValueError(
                    "Existing LeRobot subtask metadata is empty.")
            subtasks = tuple(labels)

        episode_rows = _read_arrow(
            self._metadata_tables["episodes"],
            _EPISODE_STATE_COLUMNS,
        ).to_pylist()
        episode_rows.sort(key=lambda row: row["episode_index"])
        next_index = 0
        for expected, row in enumerate(episode_rows):
            if row["episode_index"] != expected \
                    or row["dataset_from_index"] != next_index \
                    or row["dataset_to_index"] <= next_index \
                    or row["length"] != (
                        row["dataset_to_index"] - next_index) \
                    or row["stats/index/count"] != [row["length"]] \
                    or any(task not in task_indices
                           for task in row["tasks"]):
                raise ValueError(
                    "Existing LeRobot episode metadata is invalid.")
            next_index = row["dataset_to_index"]

        info = _metadata_values(self._metadata_tables["info"], "info")
        if int(info.get("fps", -1)) != self.fps \
                or info.get("features") != json.loads(json.dumps(
                    self.features, ensure_ascii=False)) \
                or info.get("total_frames") != next_index \
                or info.get("total_episodes") != len(episode_rows) \
                or info.get("total_tasks") != len(task_indices):
            raise ValueError(
                "Existing LeRobot info metadata is inconsistent.")
        stats = _metadata_values(self._metadata_tables["stats"], "stats")
        expected_stats = {
            name for name, feature in self.features.items()
            if feature.get("dtype") != "string"
        }
        if set(stats) != expected_stats or any(
                set(feature_stats) != set(_STAT_NAMES)
                for feature_stats in stats.values()):
            raise ValueError(
                "Existing LeRobot stats metadata is inconsistent.")
        numpy_stats = {
            name: {
                stat: np.asarray(value)
                for stat, value in feature_stats.items()
            }
            for name, feature_stats in stats.items()
        }
        try:
            _aggregate_stats([numpy_stats], self.features)
        except (TypeError, ValueError) as error:
            raise ValueError(
                "Existing LeRobot stats metadata is invalid.") from error
        if set(numpy_stats["index"]) != set(_STAT_NAMES) \
                or numpy_stats["index"]["count"].tolist() != [next_index]:
            raise ValueError(
                "Existing LeRobot stats metadata is inconsistent.")
        return (next_index, len(episode_rows), task_indices, numpy_stats,
                subtasks)

    @staticmethod
    def _state_from_snapshot_properties(properties):
        if properties[_STATE_VERSION] != _STATE_VERSION_VALUE:
            raise ValueError(
                "Unsupported LeRobot snapshot state version %r."
                % properties[_STATE_VERSION])
        try:
            next_index = int(properties[_NEXT_INDEX])
            next_episode_index = int(properties[_NEXT_EPISODE_INDEX])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(
                "Existing LeRobot snapshot state is invalid.") from error
        if next_index < 0 or next_episode_index < 0:
            raise ValueError("Existing LeRobot snapshot state is invalid.")
        return next_index, next_episode_index

    def _snapshot_properties(self):
        return {
            _STATE_VERSION: _STATE_VERSION_VALUE,
            _NEXT_INDEX: str(self.num_frames),
            _NEXT_EPISODE_INDEX: str(self.num_episodes),
        }

    def add_frame(self, frame):
        self._require_open("add_frame")
        if not isinstance(frame, Mapping):
            raise ValueError("frame must be a mapping.")
        expected = set(self._user_features)
        actual = set(frame) - {"task"}
        if actual != expected:
            missing = sorted(expected - actual)
            extra = sorted(actual - expected)
            raise ValueError(
                "LeRobot frame fields do not match features; missing=%s, "
                "extra=%s." % (missing, extra))
        task = frame.get("task")
        if not isinstance(task, str):
            raise ValueError("LeRobot frame task must be a string.")

        values = {"task": task}
        for name, feature in self._user_features.items():
            if feature.get("dtype") == "image":
                value = self._image_bytes(frame[name], feature, name)
            else:
                value = self._normalize_frame_value(
                    frame[name], feature, name)
            if name == "subtask_index" \
                    and (value < 0 or value >= len(self.subtasks)):
                raise ValueError(
                    "LeRobot frame subtask_index %d outside [0, %d)."
                    % (value, len(self.subtasks)))
            _safe_array(
                [value],
                self._source_schema.field(name),
                name,
                str(feature.get("dtype", "")),
            )
            values[name] = value
        self._episode_frames.append(values)

    @staticmethod
    def _normalize_frame_value(value, feature, name):
        numpy = getattr(value, "numpy", None)
        if callable(numpy):
            value = numpy()
        dtype = str(feature.get("dtype", ""))
        if dtype == "string":
            if not isinstance(value, str):
                raise ValueError(
                    "LeRobot feature %s must be a string." % name)
        else:
            if not isinstance(value, np.ndarray):
                raise ValueError(
                    "LeRobot feature %s must be a NumPy array." % name)
            if value.dtype != np.dtype(dtype):
                raise ValueError(
                    "LeRobot feature %s expected dtype %s, got %s."
                    % (name, dtype, value.dtype))
            expected_shape = _feature_shape(feature, name)
            if value.shape != expected_shape:
                raise ValueError(
                    "LeRobot feature %s expected shape %s, got %s."
                    % (name, expected_shape, value.shape))
        value = _normalize_value(value, feature, name)
        return value.tolist() if isinstance(value, np.ndarray) else value

    @staticmethod
    def _image_bytes(value, feature, name):
        actual_shape = _value_shape(value)
        getbands = getattr(value, "getbands", None)
        image_size = getattr(value, "size", None)
        if not actual_shape and callable(getbands) \
                and isinstance(image_size, tuple) and len(image_size) == 2:
            actual_shape = (image_size[1], image_size[0], len(getbands()))
        expected_shape = _feature_shape(feature, name)
        names = tuple(feature.get("names") or ())
        if names == ("height", "width", "channels"):
            channel_first_shape = (
                expected_shape[2], expected_shape[0], expected_shape[1])
            channel_last_shape = expected_shape
        else:
            channel_first_shape = expected_shape
            channel_last_shape = (
                (expected_shape[1], expected_shape[2], expected_shape[0])
                if len(expected_shape) == 3 else ()
            )
        if actual_shape and actual_shape != channel_first_shape \
                and actual_shape != channel_last_shape:
            raise ValueError(
                "LeRobot feature %s expected shape %s, got %s."
                % (name, expected_shape, actual_shape))
        return _encode_media_frame(
            value,
            channel_first=actual_shape == channel_first_shape,
        )

    def save_episode(self):
        self._require_open("save_episode")
        if not self._episode_frames:
            raise ValueError("Cannot save an empty LeRobot episode.")

        episode = self._episode_table()
        episode_stats = _compute_stats(episode, self.features)
        stats = (
            _aggregate_stats([self._stats, episode_stats], self.features)
            if self._stats is not None else episode_stats
        )
        try:
            self._ensure_batch()
            self._table_write.write_arrow(episode)
        except BaseException:
            self._fail_batch(abort=True)
            raise

        self.num_frames += episode.num_rows
        self.num_episodes += 1
        self.pending_episodes += 1
        self._pending_episode_rows.append({
            "episode_index": self.num_episodes - 1,
            "dataset_from_index": self.num_frames - episode.num_rows,
            "dataset_to_index": self.num_frames,
            "tasks": list(dict.fromkeys(
                frame["task"] for frame in self._episode_frames)),
            "length": episode.num_rows,
            **{
                "stats/%s/%s" % (feature, stat): value.tolist()
                for feature, feature_stats in episode_stats.items()
                for stat, value in feature_stats.items()
            },
        })
        self._stats = stats
        self._episode_frames = []
        if self.episodes_per_commit != -1 \
                and self.pending_episodes >= self.episodes_per_commit:
            self.flush()
        return None

    def clear_episode_buffer(self, delete_images=True):
        self._require_open("clear_episode_buffer")
        self._episode_frames = []

    def has_pending_frames(self):
        return bool(self._episode_frames)

    def flush(self):
        self._require_open("flush")
        if self.pending_episodes == 0:
            return None
        commit_started = False
        try:
            messages = self._table_write.prepare_commit()
            if "subtasks" in self._metadata_tables \
                    and not self._subtasks_committed:
                _append_arrow(
                    self._metadata_tables["subtasks"],
                    _subtasks_table(self.subtasks),
                )
            task_rows = [
                (index, task)
                for task, index in sorted(
                    self._task_indices.items(), key=lambda item: item[1])
                if index >= self._committed_task_count
            ]
            _append_arrow(
                self._metadata_tables["tasks"],
                _indexed_metadata_table("task", task_rows),
            )
            _append_arrow(
                self._metadata_tables["episodes"],
                pa.Table.from_pylist(
                    self._pending_episode_rows,
                    schema=self._episodes_schema,
                ),
            )
            _overwrite_arrow(
                self._metadata_tables["stats"],
                _metadata_table(self._stats),
            )
            _overwrite_arrow(
                self._metadata_tables["info"],
                _metadata_table({
                    "codebase_version": "v3.0",
                    "fps": self.fps,
                    "features": self.features,
                    "total_frames": self.num_frames,
                    "total_episodes": self.num_episodes,
                    "total_tasks": len(self._task_indices),
                    "splits": {
                        "train": "0:%d" % self.num_episodes,
                    },
                }),
            )
            commit_started = True
            self._table_commit.commit(
                messages,
                snapshot_properties=self._snapshot_properties())
            snapshot_id = self._snapshot_recorder.snapshot_id
            if snapshot_id is None:
                raise RuntimeError(
                    "LeRobot batch committed without reporting a snapshot id.")
        except BaseException:
            self._fail_batch(abort=not commit_started)
            raise
        self._close_batch()
        self.pending_episodes = 0
        self._pending_episode_rows = []
        self._committed_task_count = len(self._task_indices)
        self._subtasks_committed = True
        return None

    def finalize(self):
        if self._finalized:
            return None
        self._require_open("finalize")
        if self._episode_frames:
            raise RuntimeError(
                "Cannot finalize with unsaved LeRobot frames; call "
                "save_episode() or clear_episode_buffer() first.")
        self.flush()
        self._finalized = True
        return None

    def _episode_table(self):
        """Build one episode as a target-schema ``pyarrow.Table``."""
        episode_index = self.num_episodes
        first_index = self.num_frames
        size = len(self._episode_frames)
        tasks = [frame["task"] for frame in self._episode_frames]
        task_indices = []
        for task in tasks:
            if task not in self._task_indices:
                self._task_indices[task] = self._next_task_index
                self._next_task_index += 1
            task_indices.append(self._task_indices[task])

        generated = {
            "timestamp": [index / self.fps for index in range(size)],
            "frame_index": list(range(size)),
            "episode_index": [episode_index] * size,
            "index": list(range(first_index, first_index + size)),
            "task_index": task_indices,
        }
        arrays = []
        for name, feature in self.features.items():
            values = generated.get(name)
            if values is None:
                values = [frame[name] for frame in self._episode_frames]
            field = self._source_schema.field(name)
            arrays.append(_safe_array(
                values, field, name, str(feature.get("dtype", ""))))
        source = pa.Table.from_arrays(arrays, schema=self._source_schema)
        return strict_arrow_table(
            source,
            self._target_schema,
            self._table.identifier,
            self.num_episodes,
            "LeRobot",
        )

    def _ensure_batch(self):
        if self._table_write is not None:
            return
        builder = self._table.raw_table.new_batch_write_builder()
        self._table_write = builder.new_write()
        self._table_commit = builder.new_commit()
        self._snapshot_recorder = _SnapshotRecorder()
        self._table_commit.add_commit_callback(self._snapshot_recorder)

    def _fail_batch(self, abort):
        self._failed = True
        if abort and self._table_write is not None:
            self._table_write.abort()
        self._close_batch()

    def _close_batch(self):
        try:
            if self._table_write is not None:
                self._table_write.close()
        finally:
            if self._table_commit is not None:
                self._table_commit.close()
        self._table_write = None
        self._table_commit = None
        self._snapshot_recorder = None

    def _require_open(self, method):
        if self._failed:
            raise RuntimeError(
                "Cannot call %s() after a Paimon write failure." % method)
        if self._finalized:
            raise RuntimeError(
                "Cannot call %s() after finalize()." % method)
