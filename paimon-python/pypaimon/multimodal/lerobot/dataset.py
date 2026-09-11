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

"""LeRobot-compatible map-style reads from a multimodal Paimon table."""

import bisect
import io
import json
import math
import operator
import os
import sys

import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.multimodal.lerobot.metadata import (
    _companion_table_identifiers,
    _restore_pandas_metadata,
    _tag_snapshot_id,
    _validate_tag_name,
)
from pypaimon.multimodal.lerobot.loader import _DECLARED_NUMERIC_RANGES
from pypaimon.multimodal.lerobot.schema import (
    _feature_shape,
    _require_v3,
    _schema_from_info,
    _validate_lerobot_schema,
)
from pypaimon.multimodal.table import _target_schema, _time_travel_table
from pypaimon.read.query_auth_split import QueryAuthSplit


_TORCH_DTYPE_NAMES = {
    "bool": "bool",
    "boolean": "bool",
    "int8": "int8",
    "int16": "int16",
    "int32": "int32",
    "int64": "int64",
    "uint8": "uint8",
    "uint16": "uint16",
    "uint32": "uint32",
    "float16": "float16",
    "float32": "float32",
    "float64": "float64",
}

_IMAGE_READ_ATTEMPTS = 3

_CONTROL_FEATURES = frozenset({
    "index",
    "episode_index",
    "frame_index",
    "timestamp",
    "task_index",
    "subtask_index",
})


class PaimonLeRobotDataset:
    """Map-style LeRobot reader backed by indexed Paimon reads.

    LeRobot metadata is resolved from the Paimon table group and remains
    available through :attr:`meta`.

    Set ``return_uint8=True`` to keep 8-bit images in their decoded
    ``torch.uint8`` representation instead of normalizing them to float32.
    Higher-bit-depth images retain the existing float32 behavior.
    """

    def __init__(
            self,
            table,
            *,
            tag_name=None,
            episodes=None,
            image_transforms=None,
            delta_timestamps=None,
            tolerance_s=1e-4,
            blob_parallelism=16,
            return_uint8=False):
        if sys.version_info < (3, 10):
            raise RuntimeError(
                "PaimonLeRobotDataset requires Python 3.10 or newer; "
                "install and run 'pypaimon[lerobot]' on a supported Python "
                "version.")
        raw_table, self.meta = _load_dataset(table, tag_name)
        self.tag_name = tag_name
        self.repo_id = self.meta.repo_id
        self.image_transforms = image_transforms
        self.delta_timestamps = delta_timestamps
        self.tolerance_s = float(tolerance_s)
        if not math.isfinite(self.tolerance_s) or self.tolerance_s < 0:
            raise ValueError("tolerance_s must be finite and non-negative.")
        self.blob_parallelism = _positive_int(
            blob_parallelism, "blob_parallelism")
        if not isinstance(return_uint8, bool):
            raise TypeError("return_uint8 must be a boolean.")
        self.return_uint8 = return_uint8
        if image_transforms is not None and not callable(image_transforms):
            raise TypeError("image_transforms must be callable or None.")

        info = self._init_metadata()
        self._init_episodes(episodes)
        self._init_reader(raw_table, info)

    def _init_metadata(self):
        info = dict(_metadata_member(self.meta, "info", {}))
        _require_v3(info, self.repo_id)
        self._features = dict(
            _metadata_member(self.meta, "features", info.get("features")))
        if not self._features:
            raise ValueError("LeRobot metadata must define features.")
        self._image_keys = [
            name for name, feature in self._features.items()
            if feature.get("dtype") == "image"
        ]
        video_keys = [
            name for name, feature in self._features.items()
            if feature.get("dtype") == "video"
        ]
        if video_keys:
            raise NotImplementedError(
                "PaimonLeRobotDataset currently supports image-backed "
                "features only; video features are not yet supported: %s"
                % video_keys)

        self._total_frames = int(
            _metadata_member(
                self.meta, "total_frames", info.get("total_frames", -1)))
        self._total_episodes = int(
            _metadata_member(
                self.meta, "total_episodes", info.get("total_episodes", -1)))
        self._total_tasks = int(
            _metadata_member(
                self.meta, "total_tasks", info.get("total_tasks", -1)))
        if self._total_frames < 0 or self._total_episodes < 0:
            raise ValueError(
                "LeRobot metadata must define total_frames and "
                "total_episodes.")
        if self._total_tasks < 0:
            raise ValueError("LeRobot metadata must define total_tasks.")

        self._fps = int(
            _metadata_member(self.meta, "fps", info.get("fps", 0)))
        if self._fps <= 0:
            raise ValueError("LeRobot metadata fps must be positive.")
        return info

    def _init_episodes(self, episodes):
        self._episode_ranges = _episode_ranges(
            self.meta, self._total_frames, self._total_episodes)
        self._episode_ends = [end for _, end in self._episode_ranges] \
            if self._episode_ranges is not None else None
        self.episodes = _selected_episodes(episodes, self._total_episodes)
        if self.episodes is not None and self._episode_ranges is None:
            raise ValueError("Episode selection requires episode metadata.")
        self._selected_ranges = None
        if self.episodes is not None:
            # LeRobot exposes the caller's episode order but its Parquet filter
            # returns frames in their stored dataset order.
            range_episodes = sorted(self.episodes)
            self._selected_ranges = [
                self._episode_ranges[index] for index in range_episodes
            ]
            self._selected_ends = []
            size = 0
            for begin, end in self._selected_ranges:
                size += end - begin
                self._selected_ends.append(size)

        self._delta_indices = _delta_indices(
            self.delta_timestamps,
            self._fps,
            self.tolerance_s,
            self._features,
        )
        if self._delta_indices and self._episode_ranges is None:
            raise ValueError("delta_timestamps requires episode metadata.")

    def _init_reader(self, raw_table, info):
        target_schema = _target_schema(raw_table)
        table_fields = set(target_schema.names)
        tasks = _metadata_member(self.meta, "tasks")
        subtasks = _metadata_member(self.meta, "subtasks")
        _validate_component_metadata(
            self._features, self._total_tasks, tasks, subtasks)
        source_schema = _schema_from_info(info)
        _validate_lerobot_schema(source_schema, target_schema, self.repo_id)
        validation_context = _build_frame_validation_context(
            self.meta,
            self._episode_ranges,
            self._fps,
            tasks,
            subtasks,
            source_schema.field("timestamp").type,
        )
        projection = list(self._features)
        missing = set(projection) - table_fields
        if missing:
            raise ValueError(
                "Paimon table is missing LeRobot fields: %s"
                % sorted(missing))

        self._read_table, self._snapshot_id, splits = _indexed_read_table(
            raw_table, projection)
        snapshot = self._read_table.snapshot_manager().get_snapshot_by_id(
            self._snapshot_id)
        if snapshot.next_row_id != self._total_frames:
            raise ValueError(
                "Paimon table has %d rows but metadata declares %d frames."
                % (snapshot.next_row_id, self._total_frames))
        self._projection = projection
        self._frame_locator = _FrameLocator(
            self._read_table, snapshot, splits)
        self._validation_context = validation_context
        self._file_io = self._read_table.file_io
        self._task_names = validation_context["task_names"]
        self._subtask_names = validation_context["subtask_names"]
        self._delta_projection = None
        if self._delta_indices:
            self._delta_projection = list(dict.fromkeys(
                [
                    "index", "episode_index", "frame_index", "timestamp",
                    "task_index",
                ]
                + (["subtask_index"] if subtasks is not None else [])
                + list(self._delta_indices)
            ))

    @property
    def features(self):
        return self._features

    @property
    def fps(self):
        return self._fps

    @property
    def num_frames(self):
        if self.episodes is None:
            return self._total_frames
        return self._selected_ends[-1] if self._selected_ends else 0

    @property
    def num_episodes(self):
        return self._total_episodes if self.episodes is None \
            else len(self.episodes)

    def __len__(self):
        return self.num_frames

    def __getitem__(self, index):
        if isinstance(index, slice):
            return self.__getitems__(range(*index.indices(len(self))))
        return self.__getitems__([index])[0]

    def __getitems__(self, indices):
        dataset_indices = [
            _normalize_index(index, len(self)) for index in indices
        ]
        if not dataset_indices:
            return []
        frame_indices = [
            self._global_index(index) for index in dataset_indices
        ]
        plans = [self._plan(index) for index in frame_indices]

        unique_frame_indices = sorted(set(frame_indices))
        unique_frame_index_set = set(unique_frame_indices)
        delta_indices = sorted({
            position
            for plan in plans
            for positions in plan["windows"].values()
            for position in positions
            if position not in unique_frame_index_set
        })
        lookup_indices = sorted(unique_frame_index_set.union(delta_indices))
        splits, needs_filter = self._frame_locator.locate(lookup_indices)
        rows = self._read_rows(
            lookup_indices, self._projection, splits, needs_filter)
        base_rows = {
            index: rows[index] for index in unique_frame_indices
        }
        delta_rows = {
            index: {
                name: rows[index][name] for name in self._delta_projection
            }
            for index in delta_indices
        } if delta_indices else {}

        _attach_task_labels(
            base_rows, self._task_names, self._subtask_names)
        row_groups = [base_rows, delta_rows]
        image_sources = _image_blob_sources(
            row_groups, self._image_keys)
        for attempt in range(_IMAGE_READ_ATTEMPTS):
            if attempt:
                _restore_image_blob_sources(image_sources)
            try:
                _resolve_image_blobs(
                    self._file_io,
                    row_groups,
                    self._image_keys,
                    self.blob_parallelism,
                )
                converted = {
                    position: _torch_row(
                        row, self._features, self.return_uint8)
                    for position, row in base_rows.items()
                }
                converted.update({
                    position: _torch_row(
                        row, self._features, self.return_uint8)
                    for position, row in delta_rows.items()
                })
                break
            except OSError:
                if attempt + 1 == _IMAGE_READ_ATTEMPTS:
                    raise

        import torch
        duplicates = _duplicate_indices(plans)
        result = []
        for plan in plans:
            item = dict(converted[plan["index"]])
            if plan["index"] in duplicates:
                item = {
                    key: value.clone() if torch.is_tensor(value) else value
                    for key, value in item.items()
                }
            for key, positions in plan["windows"].items():
                item[key] = torch.stack([
                    converted[position][key] for position in positions
                ])
            item.update(plan["padding"])
            if self.image_transforms is not None:
                for key in self._image_keys:
                    item[key] = self.image_transforms(item[key])
            result.append(item)
        return result

    def _read_rows(
            self, indices, projection, splits=None, needs_filter=True):
        if not indices:
            return {}
        return _read_rows_by_index(
            self._read_table,
            projection,
            indices,
            self._validation_context,
            self.tolerance_s,
            self._features,
            splits,
            needs_filter,
        )

    def set_image_transforms(self, image_transforms):
        if image_transforms is not None and not callable(image_transforms):
            raise TypeError("image_transforms must be callable or None.")
        self.image_transforms = image_transforms

    def clear_image_transforms(self):
        self.image_transforms = None

    def _global_index(self, index):
        if self._selected_ranges is None:
            return index
        range_index = bisect.bisect_right(self._selected_ends, index)
        previous_end = self._selected_ends[range_index - 1] \
            if range_index else 0
        return self._selected_ranges[range_index][0] + index - previous_end

    def _plan(self, index):
        windows = {}
        padding = {}
        if self._delta_indices:
            episode = bisect.bisect_right(self._episode_ends, index)
            begin, end = self._episode_ranges[episode]
            import torch
            for key, deltas in self._delta_indices.items():
                windows[key] = [
                    min(max(index + delta, begin), end - 1)
                    for delta in deltas
                ]
                padding["%s_is_pad" % key] = torch.BoolTensor([
                    not begin <= index + delta < end for delta in deltas
                ])
        return {"index": index, "windows": windows, "padding": padding}

    def __repr__(self):
        return (
            "%s(repo_id=%r, episodes=%d, frames=%d, features=%r)"
            % (self.__class__.__name__, self.repo_id, self.num_episodes,
               self.num_frames, list(self.features)))


class _FrameLocator:
    """Locate LeRobot frame rows in one fixed Paimon snapshot."""

    def __init__(self, table, snapshot, splits):
        self._table = table
        self._snapshot = snapshot
        self._scanner = None
        self._scanner_initialized = False
        self._process_id = os.getpid()
        self._set_splits(splits)

    def _set_splits(self, splits):
        from pypaimon.read.datasource.torch_dataset import (
            SplitRangeIndex,
            row_ranges_for_split,
        )

        self._splits = splits
        self._split_ranges = [
            row_ranges_for_split(split) for split in splits
        ]
        self._split_range_index = SplitRangeIndex(self._split_ranges)

    def locate(self, indices):
        """Return narrowed splits and whether rows still need filtering."""
        self._ensure_process()
        predicate = _index_predicate(self._table, indices)
        try:
            scanner = self._index_scanner(predicate)
        except Exception as error:
            raise RuntimeError(
                "Failed to open the Paimon global index for LeRobot frame "
                "lookups.") from error
        if scanner is None:
            raise RuntimeError(
                "PaimonLeRobotDataset requires a readable global index on "
                "the frame 'index' column.")
        try:
            evaluation = scanner.scan_with_coverage(predicate)
            if evaluation is None:
                raise RuntimeError(
                    "The Paimon global index could not evaluate the LeRobot "
                    "frame index predicate.")
            unindexed = scanner.unindexed_ranges(
                predicate,
                search_mode=self._table.options.scalar_index_search_mode(),
                contributing_field_ids=evaluation.contributing_field_ids,
            )
            ranges = evaluation.result.results().to_range_list() + unindexed
            from pypaimon.read.datasource.torch_dataset import (
                select_indexed_splits,
            )
            from pypaimon.utils.range import Range
            return select_indexed_splits(
                self._splits,
                self._split_ranges,
                self._split_range_index,
                Range.sort_and_merge_overlap(ranges, True),
            ), bool(unindexed)
        except RuntimeError:
            raise
        except Exception as error:
            raise RuntimeError(
                "Failed to query the Paimon global index for LeRobot "
                "frames.") from error

    def _ensure_process(self):
        process_id = os.getpid()
        if process_id == self._process_id:
            return
        self._scanner = None
        self._scanner_initialized = False
        self._set_splits(self._splits)
        self._process_id = process_id

    def _index_scanner(self, predicate):
        if not self._scanner_initialized:
            from pypaimon.globalindex import DataEvolutionGlobalIndexScanner
            self._scanner = DataEvolutionGlobalIndexScanner.create(
                self._table,
                predicate=predicate,
                snapshot=self._snapshot,
            )
            self._scanner_initialized = True
        return self._scanner

    def close(self):
        scanner = self._scanner
        self._scanner = None
        self._scanner_initialized = False
        if scanner is not None and self._process_id == os.getpid():
            scanner.close()

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_scanner"] = None
        state["_scanner_initialized"] = False
        state["_process_id"] = None
        state["_split_ranges"] = None
        state["_split_range_index"] = None
        return state

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


class _PaimonLeRobotMetadata:

    def __init__(
            self, repo_id, tag_name, info, stats, episodes, tasks,
            subtasks):
        self.repo_id = repo_id
        self.revision = tag_name
        self.info = info
        self.stats = stats
        self.episodes = episodes
        self.tasks = tasks
        self.subtasks = subtasks

    def __getattr__(self, name):
        info = self.__dict__.get("info", {})
        try:
            return info[name]
        except KeyError as error:
            raise AttributeError(name) from error

    @property
    def image_keys(self):
        return [
            name for name, feature in self.features.items()
            if feature["dtype"] == "image"
        ]

    @property
    def video_keys(self):
        return [
            name for name, feature in self.features.items()
            if feature["dtype"] == "video"
        ]

    @property
    def camera_keys(self):
        return [
            name for name, feature in self.features.items()
            if feature["dtype"] in ("image", "video")
        ]

    @property
    def names(self):
        return {
            name: feature.get("names")
            for name, feature in self.features.items()
        }

    @property
    def shapes(self):
        return {
            name: tuple(feature["shape"])
            for name, feature in self.features.items()
        }

    def get_task_index(self, task):
        if task not in self.tasks.index:
            return None
        return int(self.tasks.loc[task].task_index)


def _load_dataset(table, tag_name):
    raw_table = getattr(table, "raw_table", None)
    if raw_table is None:
        raise TypeError("table must be a MultimodalTable.")
    if tag_name is not None:
        _validate_tag_name(tag_name)
    identifiers = _companion_table_identifiers(raw_table)
    catalog = table.catalog
    frames = _component_table(catalog, raw_table, tag_name)
    episodes_table = _component_table(
        catalog, catalog.get_table(identifiers["episodes"]), tag_name)
    episodes = _episode_dataset(episodes_table)
    tasks_table = _component_table(
        catalog, catalog.get_table(identifiers["tasks"]), tag_name)
    tasks = _component_dataframe(tasks_table, "task_index")
    subtasks = None
    if "subtasks" in identifiers:
        subtasks_table = _component_table(
            catalog, catalog.get_table(identifiers["subtasks"]), tag_name)
        subtasks = _component_dataframe(subtasks_table, "subtask_index")

    info = _metadata_object(_component_table(
        catalog, catalog.get_table(identifiers["info"]), tag_name), "info")
    for feature in info.get("features", {}).values():
        feature["shape"] = tuple(feature["shape"])
    stats = None
    if "stats" in identifiers:
        stats = _numpy_stats(_metadata_object(_component_table(
            catalog, catalog.get_table(identifiers["stats"]), tag_name),
            "stats"))
    metadata = _PaimonLeRobotMetadata(
        str(table.identifier), tag_name, info, stats, episodes, tasks,
        subtasks)
    return frames, metadata


def _component_table(catalog, table, tag_name):
    if tag_name is None:
        return table
    snapshot_id = _tag_snapshot_id(catalog, table.identifier, tag_name)
    if snapshot_id is None:
        raise ValueError(
            "Paimon LeRobot component %s is missing tag %s."
            % (table.identifier, tag_name))
    return _time_travel_table(table, tag_name=tag_name)


def _read_arrow(table, projection=None):
    builder = table.new_read_builder()
    if projection is not None:
        builder = builder.with_projection(projection)
    plan = builder.new_scan().plan()
    return builder.new_read().to_arrow(plan.splits())


def _episode_dataset(table):
    try:
        from datasets import Dataset
    except ImportError as error:
        raise ImportError(
            "PaimonLeRobotDataset requires datasets from "
            "'pypaimon[lerobot]'.") from error

    projection = [
        name for name in _target_schema(table).names
        if not name.startswith("stats/")
    ]
    data = _read_arrow(table, projection).sort_by("episode_index")
    return Dataset(data)


def _component_dataframe(table, index_field):
    data = _read_arrow(table).sort_by(index_field)
    return _restore_pandas_metadata(table, data).to_pandas()


def _metadata_object(table, name):
    result = {}
    for row in _read_arrow(table).to_pylist():
        key = row.get("key")
        if not isinstance(key, str) or key in result:
            raise ValueError(
                "Paimon LeRobot %s metadata contains an invalid key."
                % name)
        try:
            result[key] = json.loads(row.get("value"))
        except (TypeError, ValueError) as error:
            raise ValueError(
                "Paimon LeRobot %s metadata value for %r is invalid JSON."
                % (name, key)) from error
    return result


def _numpy_stats(value):
    if isinstance(value, dict):
        return {name: _numpy_stats(item) for name, item in value.items()}
    import numpy as np
    return np.array(value)


def _metadata_member(metadata, name, default=None):
    value = getattr(metadata, name, None)
    return default if value is None else value


def _episode_row(episodes, ordinal):
    return episodes.iloc[ordinal] if hasattr(episodes, "iloc") \
        else episodes[ordinal]


def _episode_ranges(metadata, total_frames, total_episodes):
    episodes = _metadata_member(metadata, "episodes")
    if episodes is None:
        return None
    if len(episodes) != total_episodes:
        raise ValueError(
            "LeRobot episode metadata contains %d rows, expected %d."
            % (len(episodes), total_episodes))
    ranges = []
    expected = 0
    for ordinal in range(total_episodes):
        row = _episode_row(episodes, ordinal)
        try:
            index = operator.index(row["episode_index"])
            begin = operator.index(row["dataset_from_index"])
            end = operator.index(row["dataset_to_index"])
            length = operator.index(row["length"])
        except (KeyError, TypeError) as error:
            raise ValueError(
                "LeRobot episode %d metadata must contain integer controls."
                % ordinal) from error
        if index != ordinal:
            raise ValueError(
                "LeRobot episode row %d has episode_index=%d."
                % (ordinal, index))
        if begin != expected or end <= begin:
            raise ValueError(
                "LeRobot episode %d has invalid frame range [%d, %d)."
                % (ordinal, begin, end))
        if length != end - begin:
            raise ValueError(
                "LeRobot episode %d has length %d, expected %d."
                % (ordinal, length, end - begin))
        ranges.append((begin, end))
        expected = end
    if expected != total_frames:
        raise ValueError(
            "LeRobot episode ranges cover %d frames, expected %d."
            % (expected, total_frames))
    return ranges


def _validate_component_metadata(features, total_tasks, tasks, subtasks):
    task_count = 0 if tasks is None else len(tasks)
    if task_count != total_tasks:
        raise ValueError(
            "LeRobot task metadata contains %d rows, expected %d."
            % (task_count, total_tasks))
    has_subtasks = subtasks is not None
    has_subtask_feature = "subtask_index" in features
    if has_subtasks != has_subtask_feature:
        raise ValueError(
            "Paimon LeRobot subtask metadata does not match the "
            "subtask_index feature.")


def _build_frame_validation_context(
        metadata, episode_ranges, fps, tasks, subtasks, timestamp_type):
    task_names = _index_names(tasks, "task_index")
    subtask_names = _index_names(subtasks, "subtask_index")
    episode_tasks = _episode_tasks(metadata, len(episode_ranges)) \
        if episode_ranges is not None else None
    return {
        "episode_ranges": episode_ranges,
        "episode_ends": (
            [end for _, end in episode_ranges]
            if episode_ranges is not None else None),
        "fps": fps,
        "task_names": task_names,
        "subtask_names": subtask_names,
        "episode_tasks": episode_tasks,
        "timestamp_type": timestamp_type,
    }


def _index_names(values, index_field):
    if values is None or len(values) == 0:
        return None
    if not hasattr(values, "iterrows"):
        return {
            index: str(value) for index, value in enumerate(values)
        }
    try:
        indices = values[index_field]
    except KeyError as error:
        raise ValueError(
            "LeRobot %s metadata must contain integer indices."
            % index_field) from error
    result = {}
    for name, value in zip(values.index, indices):
        try:
            index = operator.index(value)
        except TypeError as error:
            raise ValueError(
                "LeRobot %s metadata must contain integer indices."
                % index_field) from error
        if index in result:
            raise ValueError(
                "LeRobot %s metadata contains duplicate index %d."
                % (index_field, index))
        result[index] = str(name)
    if sorted(result) != list(range(len(result))):
        raise ValueError(
            "LeRobot %s metadata indices must be contiguous."
            % index_field)
    return result


def _episode_tasks(metadata, total_episodes):
    episodes = _metadata_member(metadata, "episodes")
    if episodes is None:
        return None
    result = []
    for ordinal in range(total_episodes):
        row = _episode_row(episodes, ordinal)
        tasks = row.get("tasks") if hasattr(row, "get") else None
        if tasks is None:
            result.append(None)
        elif isinstance(tasks, str):
            result.append((tasks,))
        else:
            result.append(tuple(sorted(str(task) for task in tasks)))
    return tuple(result)


def _selected_episodes(episodes, total_episodes):
    if episodes is None:
        return None
    selected = []
    seen = set()
    for value in episodes:
        try:
            index = operator.index(value)
        except TypeError as error:
            raise ValueError(
                "episodes must contain integer indices.") from error
        if index < 0 or index >= total_episodes:
            raise ValueError(
                "episodes must contain indices in [0, %d)." % total_episodes)
        if index in seen:
            raise ValueError("episodes must not contain duplicate indices.")
        seen.add(index)
        selected.append(index)
    return selected


def _delta_indices(delta_timestamps, fps, tolerance_s, features):
    if delta_timestamps is None:
        return None
    if fps <= 0:
        raise ValueError("LeRobot metadata fps must be positive.")
    result = {}
    for key, timestamps in delta_timestamps.items():
        if key not in features:
            raise ValueError("Unknown LeRobot delta feature: %s" % key)
        deltas = []
        for timestamp in timestamps:
            index = round(float(timestamp) * fps)
            if abs(float(timestamp) - index / fps) > tolerance_s:
                raise ValueError(
                    "delta_timestamps for %s must be multiples of 1/%d."
                    % (key, fps))
            deltas.append(index)
        result[key] = deltas
    return result


def _indexed_read_table(raw_table, projection):
    read_table = raw_table.copy({
        CoreOptions.BLOB_AS_DESCRIPTOR.key(): "true"
    })
    plan = read_table.new_read_builder().with_projection(
        projection).new_scan().plan()
    splits = plan.splits()
    if any(
            isinstance(split, QueryAuthSplit)
            and (
                getattr(split.auth_result, "filter", None)
                or getattr(split.auth_result, "column_masking", None)
            )
            for split in splits):
        raise ValueError(
            "PaimonLeRobotDataset does not support query authorization "
            "filters or column masking.")
    if plan.snapshot_id is None:
        raise ValueError("Paimon LeRobot frames table has no snapshot.")
    if read_table.options.scan_tag_name() is None:
        read_table = _time_travel_table(
            read_table, snapshot_id=plan.snapshot_id)
    return read_table, plan.snapshot_id, splits


def _index_predicate(table, indices):
    return table.new_read_builder().new_predicate_builder().is_in(
        "index", indices)


def _read_rows_by_index(
        table, projection, indices, validation_context, tolerance_s, features,
        splits=None, needs_filter=True):
    builder = table.new_read_builder().with_projection(projection)
    if needs_filter:
        builder = builder.with_filter(_index_predicate(table, indices))
    if splits is None:
        splits = builder.new_scan().plan().splits()
    rows = _arrow_rows(builder.new_read().to_arrow(splits), features)
    expected = set(indices)
    result = {}
    for row in rows:
        index = _control_index(row, "index", -1)
        if index not in expected or index in result:
            raise ValueError(
                "Paimon BTree returned an unexpected or duplicate LeRobot "
                "index: %d." % index)
        _validate_control_row(index, row, validation_context, tolerance_s)
        result[index] = row
    missing = expected - set(result)
    if missing:
        raise RuntimeError(
            "Paimon index lookup did not return LeRobot indices %s."
            % sorted(missing))
    return result


def _arrow_rows(table, features):
    """Convert indexed Arrow results without expanding tensors to lists."""
    rows = [{} for unused in range(table.num_rows)]
    for name in table.column_names:
        feature = features.get(name)
        if (name not in _CONTROL_FEATURES
                and feature is not None
                and feature.get("dtype") in _TORCH_DTYPE_NAMES):
            values = _numeric_tensor_rows(table.column(name), name, feature)
        else:
            values = table.column(name).to_pylist()
        for row, value in zip(rows, values):
            row[name] = value
    return rows


def _numeric_tensor_rows(column, name, feature):
    import numpy as np
    import torch

    values = column.combine_chunks()
    if values.null_count:
        raise ValueError(
            "LeRobot numeric feature %s contains null values." % name)
    shape = _feature_shape(feature, name)
    if shape not in ((), (1,)):
        for size in shape:
            if pa.types.is_fixed_size_list(values.type):
                if values.type.list_size != size:
                    raise ValueError(
                        "LeRobot feature %s has Arrow type %s, expected "
                        "shape %s." % (name, column.type, shape))
                start = values.offset * size
                values = values.values.slice(start, len(values) * size)
            elif (pa.types.is_list(values.type)
                  or pa.types.is_large_list(values.type)):
                offsets = values.offsets.to_numpy(zero_copy_only=False)
                if not np.all(np.diff(offsets) == size):
                    raise ValueError(
                        "LeRobot feature %s has Arrow type %s, expected "
                        "shape %s." % (name, column.type, shape))
                start = int(offsets[0])
                values = values.values.slice(
                    start, int(offsets[-1]) - start)
            else:
                raise ValueError(
                    "LeRobot feature %s has Arrow type %s, expected shape "
                    "%s." % (name, column.type, shape))
            if values.null_count:
                raise ValueError(
                    "LeRobot numeric feature %s contains null values." % name)
    if not (pa.types.is_integer(values.type)
            or pa.types.is_floating(values.type)
            or pa.types.is_boolean(values.type)):
        raise ValueError(
            "LeRobot numeric feature %s has unsupported Arrow type %s."
            % (name, column.type))
    numpy_values = values.to_numpy(zero_copy_only=False)
    if shape not in ((), (1,)):
        numpy_values = numpy_values.reshape((len(column),) + shape)
    declared_dtype = feature.get("dtype")
    if declared_dtype in ("uint8", "uint16", "uint32", "float16"):
        minimum, maximum = _DECLARED_NUMERIC_RANGES[declared_dtype]
        comparable = numpy_values[np.isfinite(numpy_values)] \
            if declared_dtype == "float16" else numpy_values
        if comparable.size and (comparable.min() < minimum
                                or comparable.max() > maximum):
            raise ValueError(
                "LeRobot numeric feature %s contains a value outside the "
                "%s range [%s, %s]."
                % (name, declared_dtype, minimum, maximum))
    dtype = getattr(torch, _TORCH_DTYPE_NAMES[declared_dtype])
    return torch.tensor(numpy_values, dtype=dtype).unbind(0)


def _validate_control_row(index, row, validation_context, tolerance_s):
    episode = bisect.bisect_right(
        validation_context["episode_ends"], index)
    begin, unused_end = validation_context["episode_ranges"][episode]
    frame = index - begin
    for name, expected in (
            ("index", index),
            ("episode_index", episode),
            ("frame_index", frame)):
        try:
            actual = operator.index(row[name])
        except (KeyError, TypeError) as error:
            raise ValueError(
                "Paimon LeRobot %s at index %d must be an integer."
                % (name, index)) from error
        if actual != expected:
            raise ValueError(
                "Paimon %s at LeRobot index %d is %r; expected %r."
                % (name, index, actual, expected))

    timestamp = row.get("timestamp")
    expected_timestamp = pa.scalar(
        frame / validation_context["fps"],
        type=validation_context["timestamp_type"],
    ).as_py()
    if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)) \
            or not math.isfinite(float(timestamp)) \
            or not math.isclose(
                float(timestamp), float(expected_timestamp),
                rel_tol=0.0, abs_tol=tolerance_s):
        raise ValueError(
            "Paimon timestamp at LeRobot index %d is %r; expected %r."
            % (index, timestamp, expected_timestamp))

    task = _control_index(row, "task_index", index)
    task_name = (validation_context["task_names"] or {}).get(task)
    if task_name is None:
        raise ValueError(
            "Paimon task_index at LeRobot index %d is absent from metadata: "
            "%r." % (index, task))
    episode_tasks = validation_context["episode_tasks"]
    if episode_tasks is not None and episode_tasks[episode] is not None \
            and task_name not in episode_tasks[episode]:
        raise ValueError(
            "Paimon task at LeRobot index %d is not assigned to Episode %d."
            % (index, episode))
    subtasks = validation_context["subtask_names"]
    if subtasks is not None:
        subtask = _control_index(row, "subtask_index", index)
        if subtask not in subtasks:
            raise ValueError(
                "Paimon subtask_index at LeRobot index %d is absent from "
                "metadata: %r." % (index, subtask))


def _control_index(row, name, index):
    try:
        return operator.index(row[name])
    except (KeyError, TypeError) as error:
        raise ValueError(
            "Paimon LeRobot %s at index %d must be an integer."
            % (name, index)) from error


def _duplicate_indices(plans):
    seen = set()
    duplicates = set()
    for plan in plans:
        index = plan["index"]
        if index in seen:
            duplicates.add(index)
        seen.add(index)
    return duplicates


def _resolve_image_blobs(
        file_io, row_groups, image_keys, parallelism):
    from pypaimon.multimodal.blob_read import fetch_blob_bodies

    values = {key: [] for key in image_keys}
    targets = {key: [] for key in image_keys}
    for rows in row_groups:
        for row in rows.values():
            for key in image_keys:
                if key in row:
                    targets[key].append(row)
                    values[key].append(row[key])
    used = [key for key in image_keys if values[key]]
    if not used:
        return
    bodies = fetch_blob_bodies(
        file_io, values, used, parallelism)
    for key in used:
        for row, body in zip(targets[key], bodies[key]):
            row[key] = body


def _image_blob_sources(row_groups, image_keys):
    return [
        (row, key, row[key])
        for rows in row_groups
        for row in rows.values()
        for key in image_keys
        if key in row
    ]


def _restore_image_blob_sources(sources):
    for row, key, descriptor in sources:
        row[key] = descriptor


def _attach_task_labels(rows, task_names, subtask_names):
    for row in rows.values():
        task_index = operator.index(row["task_index"])
        row["task"] = task_names[task_index]
        if subtask_names is not None:
            subtask_index = operator.index(row["subtask_index"])
            row["subtask"] = subtask_names[subtask_index]


def _torch_row(row, features, return_uint8=False):
    import torch

    result = dict(row)
    for key, feature in features.items():
        if key not in result:
            continue
        value = result[key]
        if feature.get("dtype") == "image":
            result[key] = _image_tensor(
                value, feature, return_uint8=return_uint8)
        elif feature.get("dtype") != "string" and not torch.is_tensor(value):
            dtype = getattr(torch, _TORCH_DTYPE_NAMES[feature.get("dtype")])
            result[key] = torch.tensor(value, dtype=dtype)
    return result


def _image_tensor(payload, feature, return_uint8=False):
    if payload is None:
        raise ValueError("LeRobot image feature contains a null frame.")
    import numpy as np
    import torch
    try:
        from PIL import Image, ImageOps
    except ImportError as error:
        raise ImportError(
            "PaimonLeRobotDataset requires Pillow from "
            "'pypaimon[lerobot]'.") from error

    expected_shape = _feature_shape(feature, "image")
    if len(expected_shape) != 3:
        raise ValueError(
            "LeRobot image feature must have three dimensions.")
    names = feature.get("names") or []
    payload_shape = expected_shape[1:] + expected_shape[:1] \
        if names and names[0] in ("channel", "channels") \
        else expected_shape
    with Image.open(io.BytesIO(payload)) as image:
        array = np.array(ImageOps.exif_transpose(image), copy=True)
    if array.ndim == 2:
        array = array[:, :, None]
    if array.shape != payload_shape:
        raise ValueError(
            "LeRobot image payload has shape %s, expected %s."
            % (array.shape, payload_shape))
    normalize = array.dtype == np.uint8
    tensor = torch.from_numpy(array).permute(2, 0, 1)
    if normalize and return_uint8:
        return tensor
    # Preserve high-bit-depth and floating-point images in native units.
    tensor = tensor.float()
    return tensor.div_(255) if normalize else tensor


def _normalize_index(index, size):
    index = operator.index(index)
    if index < 0:
        index += size
    if index < 0 or index >= size:
        raise IndexError("PaimonLeRobotDataset index out of range")
    return index


def _positive_int(value, name):
    try:
        value = operator.index(value)
    except TypeError as error:
        raise ValueError("%s must be a positive integer." % name) from error
    if isinstance(value, bool) or value <= 0:
        raise ValueError("%s must be a positive integer." % name)
    return value
