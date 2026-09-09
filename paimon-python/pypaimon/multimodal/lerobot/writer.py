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
import json
from typing import Mapping, Optional

import numpy as np
import pyarrow as pa

from pypaimon.multimodal.arrow_utils import strict_arrow_table
from pypaimon.multimodal.hdf5 import _SnapshotRecorder
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
)
from pypaimon.multimodal.table import _target_schema


_DEFAULT_FEATURES = {
    "timestamp": {"dtype": "float32", "shape": (1,), "names": None},
    "frame_index": {"dtype": "int64", "shape": (1,), "names": None},
    "episode_index": {"dtype": "int64", "shape": (1,), "names": None},
    "index": {"dtype": "int64", "shape": (1,), "names": None},
    "task_index": {"dtype": "int64", "shape": (1,), "names": None},
}
_TASK_FEATURE = {"dtype": "string", "shape": (1,), "names": None}
_STATE_PREFIX = "pypaimon.lerobot."
_STATE_VERSION = _STATE_PREFIX + "state-version"
_NEXT_INDEX = _STATE_PREFIX + "next-index"
_NEXT_EPISODE_INDEX = _STATE_PREFIX + "next-episode-index"
_TASK_INDICES = _STATE_PREFIX + "task-indices"


class PaimonLeRobotWriter:
    """Collect LeRobot frames and commit completed episodes to Paimon."""

    def __init__(
            self,
            connection,
            table_name: str,
            *,
            fps: int,
            features: Mapping[str, Mapping[str, object]],
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

        self.fps = fps
        self.episodes_per_commit = episodes_per_commit
        self.features = copy.deepcopy(dict(features))
        self._user_features = copy.deepcopy(dict(features))
        self.features.update(copy.deepcopy(_DEFAULT_FEATURES))
        schema_features = dict(self.features)
        schema_features["task"] = _TASK_FEATURE
        self._source_schema = _schema_from_info({"features": schema_features})
        self._table = connection.create_table(
            table_name,
            schema=self._source_schema,
            options=options,
            ignore_if_exists=True,
        )
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

        self.num_frames, self.num_episodes, self._task_indices = \
            self._load_existing_state()
        self._next_task_index = (
            max(self._task_indices.values()) + 1
            if self._task_indices else 0
        )
        self.pending_episodes = 0
        self._episode_frames = []
        self._table_write = None
        self._table_commit = None
        self._snapshot_recorder = None
        self._finalized = False
        self._failed = False

    def _load_existing_state(self):
        snapshot = self._table.raw_table.snapshot_manager() \
            .get_latest_snapshot()
        if snapshot is None:
            return 0, 0, {}
        properties = snapshot.properties or {}
        if _STATE_VERSION in properties:
            return self._state_from_snapshot_properties(properties)
        if any(key.startswith(_STATE_PREFIX) for key in properties):
            raise ValueError("Existing LeRobot snapshot state is incomplete.")

        # ponytail: one resume scan; persist counters if startup cost matters.
        rows = self._table.scan().select([
            "index", "episode_index", "task_index", "task"
        ]).to_arrow().to_pylist()
        if not rows:
            return 0, 0, {}

        task_indices = {}
        index_tasks = {}
        for row in rows:
            task = row["task"]
            task_index = row["task_index"]
            if ((task in task_indices
                 and task_indices[task] != task_index)
                    or (task_index in index_tasks
                        and index_tasks[task_index] != task)):
                raise ValueError(
                    "Existing LeRobot task and task_index values conflict.")
            task_indices[task] = task_index
            index_tasks[task_index] = task
        return (
            max(row["index"] for row in rows) + 1,
            max(row["episode_index"] for row in rows) + 1,
            task_indices,
        )

    @staticmethod
    def _state_from_snapshot_properties(properties):
        if properties[_STATE_VERSION] != "1":
            raise ValueError(
                "Unsupported LeRobot snapshot state version %r."
                % properties[_STATE_VERSION])
        try:
            next_index = int(properties[_NEXT_INDEX])
            next_episode_index = int(properties[_NEXT_EPISODE_INDEX])
            task_indices = json.loads(properties[_TASK_INDICES])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(
                "Existing LeRobot snapshot state is invalid.") from error
        if next_index < 0 or next_episode_index < 0 \
                or not isinstance(task_indices, dict):
            raise ValueError("Existing LeRobot snapshot state is invalid.")
        indices = list(task_indices.values())
        if any(not isinstance(task, str)
               or isinstance(index, bool)
               or not isinstance(index, int)
               or index < 0
               for task, index in task_indices.items()) \
                or len(set(indices)) != len(indices):
            raise ValueError("Existing LeRobot snapshot state is invalid.")
        return next_index, next_episode_index, task_indices

    def _snapshot_properties(self):
        return {
            _STATE_VERSION: "1",
            _NEXT_INDEX: str(self.num_frames),
            _NEXT_EPISODE_INDEX: str(self.num_episodes),
            _TASK_INDICES: json.dumps(
                self._task_indices,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ),
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
        try:
            self._ensure_batch()
            self._table_write.write_arrow(episode)
        except BaseException:
            self._fail_batch(abort=True)
            raise

        self.num_frames += episode.num_rows
        self.num_episodes += 1
        self.pending_episodes += 1
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
        arrays.append(pa.array(tasks, type=pa.string()))
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
