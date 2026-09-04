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
from array import array

import pyarrow as pa

from pypaimon.catalog.catalog_exception import TagNotExistException
from pypaimon.multimodal.lerobot.metadata import (
    _companion_table_identifiers,
    _restore_pandas_metadata,
    _tag_snapshot_id,
)
from pypaimon.multimodal.lerobot.schema import (
    _feature_shape,
    _require_v3,
    _schema_from_info,
    _validate_lerobot_schema,
)
from pypaimon.multimodal.table import _target_schema, _time_travel_table


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


class _LeRobotIndexMapping:
    """Reusable semantic-index mapping bound to one table snapshot."""

    def __init__(
            self,
            table_identifier,
            snapshot_id,
            metadata_signature,
            tolerance_s,
            positions):
        self._table_identifier = table_identifier
        self._snapshot_id = snapshot_id
        self._metadata_signature = metadata_signature
        self._tolerance_s = tolerance_s
        self._positions = positions


class PaimonLeRobotDataset:
    """Map-style LeRobot reader backed by Paimon's lazy Torch dataset.

    The published version and its LeRobot metadata are resolved from the
    Paimon table group and remain available through :attr:`meta`.
    """

    def __init__(
            self,
            table,
            *,
            version_id=None,
            episodes=None,
            image_transforms=None,
            delta_timestamps=None,
            tolerance_s=1e-4,
            index_mapping=None,
            blob_parallelism=16):
        raw_table, self.meta, self.version_id = _load_published_version(
            table, version_id)
        self.repo_id = self.meta.repo_id
        self.image_transforms = image_transforms
        self.delta_timestamps = delta_timestamps
        self.tolerance_s = float(tolerance_s)
        if not math.isfinite(self.tolerance_s) or self.tolerance_s < 0:
            raise ValueError("tolerance_s must be finite and non-negative.")
        self.blob_parallelism = _positive_int(
            blob_parallelism, "blob_parallelism")
        if image_transforms is not None and not callable(image_transforms):
            raise TypeError("image_transforms must be callable or None.")

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

        self._fps = int(
            _metadata_member(self.meta, "fps", info.get("fps", 0)))
        if self._fps <= 0:
            raise ValueError("LeRobot metadata fps must be positive.")
        self._delta_indices = _delta_indices(
            delta_timestamps,
            self._fps,
            self.tolerance_s,
            self._features,
        )
        if self._delta_indices and self._episode_ranges is None:
            raise ValueError("delta_timestamps requires episode metadata.")

        target_schema = _target_schema(raw_table)
        table_fields = set(target_schema.names)
        tasks = _metadata_member(self.meta, "tasks")
        subtasks = _metadata_member(self.meta, "subtasks")
        _validate_component_metadata(
            self._features, self._total_tasks, tasks, subtasks)
        source_schema = _schema_from_info(info)
        _validate_lerobot_schema(source_schema, target_schema, self.repo_id)
        control_contract = _control_contract(
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

        self._dataset, splits, read_table, snapshot_id = _lazy_torch_dataset(
            raw_table, projection)
        if len(self._dataset) != self._total_frames:
            raise ValueError(
                "Paimon table has %d rows but metadata declares %d frames."
                % (len(self._dataset), self._total_frames))
        table_identifier = str(table.identifier)
        if index_mapping is None:
            self._index_mapping = _semantic_index_mapping(
                read_table,
                splits,
                self._total_frames,
                table_identifier,
                snapshot_id,
                control_contract,
                self.tolerance_s,
            )
        else:
            self._index_mapping = _reuse_index_mapping(
                index_mapping,
                table_identifier,
                snapshot_id,
                control_contract["signature"],
                self._total_frames,
                self.tolerance_s,
            )
        self._index_positions = self._index_mapping._positions
        self._file_io = read_table.file_io
        self._task_names = control_contract["task_names"]
        self._subtask_names = control_contract["subtask_names"]
        self._delta_dataset = None
        if self._delta_indices:
            delta_projection = ["index"] + [
                key for key in self._delta_indices if key != "index"
            ]
            self._delta_dataset = _lazy_torch_dataset_for_splits(
                read_table, delta_projection, splits)

    @property
    def features(self):
        return self._features

    @property
    def fps(self):
        return self._fps

    @property
    def index_mapping(self):
        """Mapping reusable by another reader of the same table snapshot."""
        return self._index_mapping

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
        relative = [_normalize_index(index, len(self)) for index in indices]
        if not relative:
            return []
        absolute = [self._absolute_index(index) for index in relative]
        plans = [self._plan(index) for index in absolute]

        base_indices = sorted(set(absolute))
        base_rows = _read_rows(
            self._dataset, base_indices, self._index_positions)
        delta_indices = sorted({
            position
            for plan in plans
            for positions in plan["windows"].values()
            for position in positions
            if position not in base_rows
        })
        delta_rows = _read_rows(
            self._delta_dataset, delta_indices, self._index_positions) \
            if delta_indices else {}

        _materialize_images(
            self._file_io,
            [base_rows, delta_rows],
            self._image_keys,
            self.blob_parallelism,
        )
        _materialize_labels(
            base_rows, self._task_names, self._subtask_names)
        converted = {
            position: _torch_row(row, self._features)
            for position, row in base_rows.items()
        }
        converted.update({
            position: _torch_row(row, self._features)
            for position, row in delta_rows.items()
        })

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

    def set_image_transforms(self, image_transforms):
        if image_transforms is not None and not callable(image_transforms):
            raise TypeError("image_transforms must be callable or None.")
        self.image_transforms = image_transforms

    def clear_image_transforms(self):
        self.image_transforms = None

    def _absolute_index(self, index):
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


class _PaimonLeRobotMetadata:

    def __init__(
            self, repo_id, version_id, info, stats, episodes, tasks,
            subtasks):
        self.repo_id = repo_id
        self.revision = str(version_id)
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


def _load_published_version(table, version_id):
    raw_table = getattr(table, "raw_table", None)
    if raw_table is None:
        raise TypeError("table must be a MultimodalTable.")
    identifiers = _companion_table_identifiers(raw_table)
    catalog = table.catalog
    manifests = _read_arrow(catalog.get_table(
        identifiers["versions"])).to_pylist()
    version_id, manifest = _select_manifest(manifests, version_id)
    tag = str(version_id)

    frames = _tagged_table(catalog, raw_table, tag)
    episodes_table = _tagged_table(
        catalog, catalog.get_table(identifiers["episodes"]), tag)
    episodes = _episode_dataset(episodes_table)
    tasks_table = _tagged_table(
        catalog, catalog.get_table(identifiers["tasks"]), tag)
    tasks = _component_dataframe(tasks_table, "task_index")
    subtasks = None
    if manifest["has_subtasks"]:
        subtasks_table = _tagged_table(
            catalog, catalog.get_table(identifiers["subtasks"]), tag)
        subtasks = _component_dataframe(subtasks_table, "subtask_index")

    info = _json_object(manifest["info_json"], "info_json")
    stats = None if manifest["stats_json"] is None else _numpy_stats(
        _json_object(manifest["stats_json"], "stats_json"))
    metadata = _PaimonLeRobotMetadata(
        str(table.identifier), version_id, info, stats, episodes, tasks,
        subtasks)
    return frames, metadata, version_id


def _select_manifest(manifests, version_id):
    if not manifests:
        raise ValueError("Paimon LeRobot table has no published versions.")
    if version_id is None:
        version_id = max(row["version_id"] for row in manifests)
    else:
        try:
            version_id = operator.index(version_id)
        except TypeError as error:
            raise ValueError(
                "version_id must be an integer or None.") from error
        if isinstance(version_id, bool):
            raise ValueError("version_id must be an integer or None.")
    matches = [
        row for row in manifests if row["version_id"] == version_id
    ]
    if len(matches) != 1:
        raise ValueError(
            "Paimon LeRobot version %d has %d manifest rows."
            % (version_id, len(matches)))
    return version_id, matches[0]


def _tagged_table(catalog, table, tag):
    try:
        snapshot_id = _tag_snapshot_id(catalog, table.identifier, tag)
    except TagNotExistException:
        snapshot_id = None
    if snapshot_id is None:
        raise ValueError(
            "Paimon LeRobot component %s is missing tag %s."
            % (table.identifier, tag))
    return _time_travel_table(table, tag_name=tag)


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


def _json_object(value, field):
    try:
        result = json.loads(value)
    except (TypeError, ValueError) as error:
        raise ValueError(
            "Paimon LeRobot manifest %s is invalid JSON." % field
        ) from error
    if not isinstance(result, dict):
        raise ValueError(
            "Paimon LeRobot manifest %s must contain an object." % field)
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
            "Paimon LeRobot manifest has_subtasks does not match the "
            "subtask_index feature.")


def _control_contract(
        metadata, episode_ranges, fps, tasks, subtasks, timestamp_type):
    task_names = _index_names(tasks, "task_index")
    subtask_names = _index_names(subtasks, "subtask_index")
    episode_tasks = _episode_tasks(metadata, len(episode_ranges)) \
        if episode_ranges is not None else None
    signature = (
        tuple(episode_ranges) if episode_ranges is not None else None,
        fps,
        tuple(sorted(task_names.items())) if task_names is not None else None,
        tuple(sorted(subtask_names.items())) if subtask_names is not None
        else None,
        episode_tasks,
        str(timestamp_type),
    )
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
        "signature": signature,
    }


def _index_names(values, index_field):
    if values is None or len(values) == 0:
        return None
    if not hasattr(values, "iterrows"):
        return {
            index: str(value) for index, value in enumerate(values)
        }
    result = {}
    for name, row in values.iterrows():
        try:
            index = operator.index(row[index_field])
        except (KeyError, TypeError) as error:
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


def _lazy_torch_dataset(raw_table, projection):
    from pypaimon.common.options.core_options import CoreOptions
    read_table = raw_table.copy({
        CoreOptions.BLOB_AS_DESCRIPTOR.key(): "true"
    })
    builder = read_table.new_read_builder().with_projection(projection)
    plan = builder.new_scan().plan()
    splits = plan.splits()
    return (
        _required_lazy_torch_dataset(builder.new_read(), splits),
        splits,
        read_table,
        plan.snapshot_id,
    )


def _lazy_torch_dataset_for_splits(read_table, projection, splits):
    builder = read_table.new_read_builder().with_projection(projection)
    return _required_lazy_torch_dataset(builder.new_read(), splits)


def _required_lazy_torch_dataset(table_read, splits):
    from pypaimon.read.datasource.torch_dataset import TorchDataset
    return TorchDataset(table_read, splits, require_lazy=True)


def _semantic_index_mapping(
        read_table,
        splits,
        size,
        table_identifier,
        snapshot_id,
        control_contract,
        tolerance_s):
    projection = [
        "index", "episode_index", "frame_index", "timestamp", "task_index"
    ]
    if control_contract["subtask_names"] is not None:
        projection.append("subtask_index")
    index_dataset = _lazy_torch_dataset_for_splits(
        read_table, projection, splits)
    if len(index_dataset) != size:
        raise ValueError(
            "Paimon index contains %d rows, expected %d."
            % (len(index_dataset), size))

    positions = None
    batch_size = 65536
    for begin in range(0, size, batch_size):
        end = min(begin + batch_size, size)
        rows = index_dataset.__getitems__(range(begin, end))
        if len(rows) != end - begin:
            raise ValueError(
                "Paimon index read returned %d rows for range [%d, %d)."
                % (len(rows), begin, end))
        for offset, row in enumerate(rows):
            physical = begin + offset
            try:
                index = operator.index(row["index"])
            except (KeyError, TypeError) as error:
                raise ValueError(
                    "Paimon LeRobot index must contain integers.") from error
            if index < 0 or index >= size:
                raise ValueError(
                    "Paimon LeRobot index %d is outside [0, %d)."
                    % (index, size))
            _validate_control_row(
                row, index, control_contract, tolerance_s)
            if positions is None and index == physical:
                continue
            if positions is None:
                positions = array("q", [-1]) * size
                for previous in range(physical):
                    positions[previous] = previous
            if positions[index] >= 0:
                raise ValueError(
                    "Paimon LeRobot index contains duplicate value %d."
                    % index)
            positions[index] = physical

    if positions is not None and any(position < 0 for position in positions):
        raise ValueError("Paimon LeRobot index is not contiguous.")
    return _LeRobotIndexMapping(
        table_identifier,
        snapshot_id,
        control_contract["signature"],
        tolerance_s,
        range(size) if positions is None else positions,
    )


def _reuse_index_mapping(
        mapping,
        table_identifier,
        snapshot_id,
        metadata_signature,
        size,
        tolerance_s):
    if not isinstance(mapping, _LeRobotIndexMapping):
        raise TypeError(
            "index_mapping must come from PaimonLeRobotDataset.index_mapping.")
    if mapping._table_identifier != table_identifier:
        raise ValueError("index_mapping belongs to a different Paimon table.")
    if mapping._snapshot_id != snapshot_id:
        raise ValueError("index_mapping belongs to a different Paimon snapshot.")
    if mapping._metadata_signature != metadata_signature:
        raise ValueError("index_mapping belongs to different LeRobot metadata.")
    if len(mapping._positions) != size:
        raise ValueError("index_mapping has an incompatible frame count.")
    if mapping._tolerance_s > tolerance_s:
        raise ValueError(
            "index_mapping was validated with a looser tolerance_s.")
    return mapping


def _validate_control_row(row, index, contract, tolerance_s):
    ranges = contract["episode_ranges"]
    if ranges is None:
        return
    episode = bisect.bisect_right(contract["episode_ends"], index)
    begin, _ = ranges[episode]
    frame = index - begin
    _validate_int_control(row, "episode_index", index, episode)
    _validate_int_control(row, "frame_index", index, frame)

    expected_timestamp = pa.scalar(
        frame / contract["fps"], type=contract["timestamp_type"]).as_py()
    try:
        timestamp = float(row["timestamp"])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(
            "Paimon timestamp at LeRobot index %d must be numeric." % index
        ) from error
    if not math.isfinite(timestamp) \
            or abs(timestamp - expected_timestamp) > tolerance_s:
        raise ValueError(
            "Paimon timestamp at LeRobot index %d is %r, metadata expects %r."
            % (index, timestamp, expected_timestamp))

    task_names = contract["task_names"]
    task_index = _control_int(row, "task_index", index)
    task = None if task_names is None else task_names.get(task_index)
    if task is None:
        raise ValueError(
            "Paimon task_index at LeRobot index %d is absent from metadata: "
            "%r." % (index, task_index))
    episode_tasks = contract["episode_tasks"]
    allowed = episode_tasks[episode] if episode_tasks is not None else None
    if allowed is not None and task not in allowed:
        raise ValueError(
            "Paimon task at LeRobot index %d is not assigned to episode %d: "
            "%r." % (index, episode, task))

    subtask_names = contract["subtask_names"]
    if subtask_names is not None:
        subtask_index = _control_int(row, "subtask_index", index)
        if subtask_index not in subtask_names:
            raise ValueError(
                "Paimon subtask_index at LeRobot index %d is absent from "
                "metadata: %r." % (index, subtask_index))


def _validate_int_control(row, field, index, expected):
    actual = _control_int(row, field, index)
    if actual != expected:
        raise ValueError(
            "Paimon %s at LeRobot index %d is %r, metadata expects %r."
            % (field, index, actual, expected))


def _control_int(row, field, index):
    try:
        return operator.index(row[field])
    except (KeyError, TypeError) as error:
        raise ValueError(
            "Paimon %s at LeRobot index %d must be an integer."
            % (field, index)) from error


def _read_rows(dataset, indices, index_positions):
    if not indices:
        return {}
    positions = [
        index_positions[index] for index in indices
    ]
    rows = dataset.__getitems__(positions)
    result = {}
    for index, row in zip(indices, rows):
        if int(row["index"]) != index:
            raise ValueError(
                "Paimon row mapped to LeRobot index %d contains index=%r."
                % (index, row["index"]))
        result[index] = row
    return result


def _duplicate_indices(plans):
    seen = set()
    duplicates = set()
    for plan in plans:
        index = plan["index"]
        if index in seen:
            duplicates.add(index)
        seen.add(index)
    return duplicates


def _materialize_images(
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


def _materialize_labels(rows, task_names, subtask_names):
    for row in rows.values():
        task_index = operator.index(row["task_index"])
        row["task"] = task_names[task_index]
        if subtask_names is not None:
            subtask_index = operator.index(row["subtask_index"])
            row["subtask"] = subtask_names[subtask_index]


def _torch_row(row, features):
    import torch

    result = dict(row)
    for key, feature in features.items():
        if key not in result:
            continue
        value = result[key]
        if feature.get("dtype") == "image":
            result[key] = _image_tensor(value, feature)
        elif feature.get("dtype") != "string":
            dtype = getattr(torch, _TORCH_DTYPE_NAMES[feature.get("dtype")])
            result[key] = torch.tensor(value, dtype=dtype)
    return result


def _image_tensor(payload, feature):
    if payload is None:
        raise ValueError("LeRobot image feature contains a null frame.")
    import numpy as np
    import torch
    try:
        from PIL import Image
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
        array = np.array(image, copy=True)
    if array.ndim == 2:
        array = array[:, :, None]
    if array.shape != payload_shape:
        raise ValueError(
            "LeRobot image payload has shape %s, expected %s."
            % (array.shape, payload_shape))
    normalize = array.dtype == np.uint8
    if not normalize:
        # Preserve high-bit-depth and floating-point images in native units.
        array = array.astype(np.float32, copy=False)
    tensor = torch.from_numpy(array).permute(2, 0, 1).float()
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
