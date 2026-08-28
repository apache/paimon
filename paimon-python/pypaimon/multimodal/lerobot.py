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

"""One-time LeRobot Dataset v3 import into a multimodal Paimon table."""

import io
import json
import shutil
import sys
import tempfile
from bisect import bisect_right
from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, Optional

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from pypaimon.catalog.catalog_exception import (
    DatabaseNotExistException,
    TableNotExistException,
)
from pypaimon.common.options import Options
from pypaimon.filesystem.pyarrow_file_io import LegacyOssDirectoryListingError
from pypaimon.multimodal.arrow_utils import strict_arrow_table
from pypaimon.multimodal.hdf5 import (
    _Hdf5SourceFileIO,
    _SnapshotRecorder,
    _normalize_source_path,
    _qualified_status_path,
    _validated_source_options,
)
from pypaimon.multimodal.table import _target_schema


_DEFAULT_TABLE_OPTIONS = {
    "file.format": "parquet",
    "vector.file.format": "parquet",
}
_SCALAR_DTYPES = {
    "bool": pa.bool_(),
    "boolean": pa.bool_(),
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.int16(),
    "uint16": pa.int32(),
    "uint32": pa.int64(),
    "float16": pa.float32(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "string": pa.string(),
}
_MEDIA_DTYPES = ("image", "video")


@dataclass(frozen=True)
class LeRobotLoadResult:
    """Counts and snapshot for one ``load_from_lerobot`` call."""

    episode_count: int
    batch_count: int
    row_count: int
    snapshot_id: Optional[int]


@dataclass(frozen=True)
class _LeRobotSource:
    path: str
    root: Optional[Path]
    repo_id: str
    file_io: object = None


@dataclass(frozen=True)
class _RemoteLeRobotMeta:
    info: dict
    episodes: list
    tasks: list


def load_from_lerobot(
        connection,
        table_name: str,
        source,
        *,
        transform: Optional[Callable] = None,
        feature_mapping: Optional[Mapping[str, str]] = None,
        batch_size: int = 1024,
        options: Optional[Mapping[str, object]] = None,
        source_options: Optional[Mapping[str, object]] = None):
    """Import one LeRobot Dataset v3 and commit all frames once.

    A missing target table is created from LeRobot metadata. An existing table
    receives the same strict schema validation and append semantics as
    :meth:`MultimodalConnection.load_from_hdf5`. FileIO URI credentials come
    only from ``source_options`` and are not inherited from the target Catalog.
    """
    if sys.version_info < (3, 10):
        raise RuntimeError(
            "load_from_lerobot requires Python 3.10 or newer; install and "
            "run 'pypaimon[lerobot]' on a supported Python version.")
    if transform is not None and not callable(transform):
        raise ValueError("transform must be callable or None.")
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) \
            or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer.")

    validated_source_options = _validated_source_options(source_options)
    with _resolved_source(source, validated_source_options) as (
            resolved_source, local_info):
        if local_info is not None:
            _require_v3(local_info, resolved_source.path)
        LeRobotDataset = _import_lerobot_dataset()
        dataset = _open_resolved_dataset(
            LeRobotDataset, resolved_source, local_info)
        try:
            info = dict(dataset.meta.info)
            _require_v3(info, resolved_source.path)

            source_schema, mapped_names = _schema_from_info(
                info, feature_mapping,
                include_task=_has_tasks(dataset, info))
            table = _get_or_create_table(
                connection, table_name, source_schema, options)
            target_schema = _target_schema(table.raw_table)
            _strict_lerobot_table(
                pa.Table.from_batches([], schema=source_schema),
                target_schema,
                resolved_source,
                0,
            )

            episode_count = int(info.get("total_episodes", 0))
            row_count = int(info.get("total_frames", len(dataset)))
            if row_count == 0:
                return LeRobotLoadResult(
                    episode_count=episode_count,
                    batch_count=0,
                    row_count=0,
                    snapshot_id=None,
                )
            return _write_dataset(
                table,
                dataset,
                info,
                resolved_source,
                source_schema,
                mapped_names,
                transform,
                batch_size,
            )
        finally:
            close = getattr(dataset, "close", None)
            if callable(close):
                close()


@contextmanager
def _resolved_source(source, source_options):
    if isinstance(source, Path):
        root = source.expanduser().resolve()
        if not root.is_dir():
            raise FileNotFoundError(
                "LeRobot source directory does not exist: %s" % root)
        yield _local_source(root)
        return
    if not isinstance(source, str) or not source.strip():
        raise ValueError(
            "source must be a local directory or Hugging Face repo_id.")

    value = source.strip()
    candidate = Path(value).expanduser()
    if candidate.is_dir():
        yield _local_source(candidate.resolve())
        return
    if candidate.is_absolute() or value.startswith((".", "~")):
        raise FileNotFoundError(
            "LeRobot source directory does not exist: %s" % candidate)
    if "://" not in value:
        yield _LeRobotSource(path=value, root=None, repo_id=value), None
        return

    source_uri = _normalize_source_path(value).rstrip("/")
    source_file_io = _Hdf5SourceFileIO(Options(source_options))
    try:
        try:
            status = source_file_io.get_file_status(source_uri)
        except FileNotFoundError as error:
            raise FileNotFoundError(
                "LeRobot source directory does not exist: %s" % source_uri
            ) from error
        if status.type != pafs.FileType.Directory:
            raise ValueError(
                "LeRobot URI source must be a directory: %s" % source_uri)
        info = _read_remote_json(
            source_file_io, _remote_path(source_uri, "meta/info.json"))
        yield (
            _LeRobotSource(
                path=source_uri,
                root=None,
                repo_id="",
                file_io=source_file_io,
            ),
            info,
        )
    finally:
        source_file_io.close()


def _local_source(root, display_path=None):
    info_path = root / "meta" / "info.json"
    if not info_path.is_file():
        raise ValueError(
            "LeRobot source is missing meta/info.json: %s"
            % (display_path or root))
    try:
        with info_path.open("r", encoding="utf-8") as file:
            info = json.load(file)
    except (OSError, ValueError) as error:
        raise ValueError(
            "Cannot read LeRobot metadata %s: %s"
            % (info_path, error)) from error
    return (
        _LeRobotSource(
            path=str(display_path or root),
            root=root,
            repo_id="local/pypaimon-import",
        ),
        info,
    )


def _strict_lerobot_table(data, target_schema, source, batch_index):
    return strict_arrow_table(
        data,
        target_schema,
        source.path,
        batch_index,
        "LeRobot",
    )


def _require_v3(info, source):
    version = str(info.get("codebase_version", ""))
    if not (version == "v3" or version.startswith("v3.")):
        raise ValueError(
            "load_from_lerobot supports LeRobot Dataset v3 only; %s reports "
            "codebase_version=%r. Upgrade the dataset to v3 first."
            % (source, version or None))


def _import_lerobot_dataset():
    try:
        from lerobot.datasets.lerobot_dataset import LeRobotDataset
    except ImportError as error:
        raise ImportError(
            "load_from_lerobot requires LeRobot; install "
            "'pypaimon[lerobot]'.") from error
    return LeRobotDataset


def _open_dataset(LeRobotDataset, source):
    try:
        if source.root is not None:
            return LeRobotDataset(
                repo_id=source.repo_id,
                root=source.root,
                video_backend="pyav",
            )
        return LeRobotDataset(repo_id=source.repo_id, video_backend="pyav")
    except Exception as error:
        raise ValueError(
            "Cannot open LeRobot Dataset v3 source %s: %s"
            % (source.path, error)) from error


def _open_resolved_dataset(LeRobotDataset, source, info):
    if source.file_io is not None:
        return _RemoteLeRobotDataset(source, info)
    return _open_dataset(LeRobotDataset, source)


class _RemoteLeRobotDataset:

    def __init__(self, source, info):
        self.source = source
        self.root = source.path
        self._file_io = source.file_io
        self._episodes = self._load_episodes(info)
        self._tasks = self._load_tasks(info)
        self.meta = _RemoteLeRobotMeta(info, self._episodes, self._tasks)
        self._episode_starts = [
            int(episode["dataset_from_index"])
            for episode in self._episodes
        ]
        self._episodes_by_index = {
            int(episode["episode_index"]): episode
            for episode in self._episodes
        }
        self._data_ranges = self._build_data_ranges(info)
        self._cached_data_path = None
        self._cached_data_table = None
        self._video_cache = {}
        self._video_temp_dir = None

    def __len__(self):
        return int(self.meta.info.get("total_frames", 0))

    def close(self):
        self._cached_data_table = None
        self._video_cache.clear()
        if self._video_temp_dir is not None:
            self._video_temp_dir.cleanup()
            self._video_temp_dir = None

    def read_batch(self, begin, end):
        episode = self._episode_for_range(begin, end)
        relative_path = self._data_path(episode, self.meta.info)
        source_path = _remote_path(self.source.path, relative_path)
        if source_path != self._cached_data_path:
            table = _read_remote_parquet(self._file_io, source_path)
            expected_begin, expected_end = self._data_ranges[relative_path]
            expected_rows = expected_end - expected_begin
            if table.num_rows != expected_rows:
                raise ValueError(
                    "LeRobot data file %s has %d rows; metadata expects %d."
                    % (source_path, table.num_rows, expected_rows))
            self._cached_data_path = source_path
            self._cached_data_table = table
        file_begin = self._data_ranges[relative_path][0]
        return self._cached_data_table.slice(begin - file_begin, end - begin)

    def image_bytes(self, value):
        if value is None:
            raise ValueError("LeRobot image feature contains a null frame.")
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value)
        if isinstance(value, dict):
            body = value.get("bytes")
            if body is not None:
                return bytes(body)
            image_path = value.get("path")
            if image_path:
                source_path = image_path if "://" in image_path else \
                    _remote_path(self.source.path, image_path)
                return _read_remote_bytes(self._file_io, source_path)
        return _encode_media_frame(value)

    def read_video_values(self, name, raw):
        episode_indices = raw.column("episode_index").to_pylist()
        if not episode_indices or len(set(episode_indices)) != 1:
            raise ValueError(
                "LeRobot video batches must contain exactly one Episode.")
        episode_index = int(_python_scalar(episode_indices[0]))
        episode = self._episodes_by_index.get(episode_index)
        if episode is None:
            raise ValueError(
                "LeRobot metadata is missing Episode %d." % episode_index)
        info = self.meta.info
        relative_path = info["video_path"].format(
            video_key=name,
            chunk_index=int(episode[
                "videos/%s/chunk_index" % name]),
            file_index=int(episode[
                "videos/%s/file_index" % name]),
        )
        source_path = _remote_path(self.source.path, relative_path)
        local_path = self._cached_video_path(name, source_path)
        start = float(episode["videos/%s/from_timestamp" % name])
        timestamps = [
            start + float(_python_scalar(value))
            for value in raw.column("timestamp").to_pylist()
        ]
        try:
            from lerobot.datasets.video_utils import decode_video_frames
        except ImportError as error:
            raise ImportError(
                "LeRobot video import requires the video dependencies from "
                "'pypaimon[lerobot]'.") from error
        frames = decode_video_frames(
            local_path,
            timestamps,
            1e-4,
            "pyav",
        )
        if len(frames) != len(timestamps):
            raise ValueError(
                "LeRobot video %s returned %d frames; expected %d."
                % (source_path, len(frames), len(timestamps)))
        return [_encode_media_frame(frame) for frame in frames]

    def _load_episodes(self, info):
        episode_count = int(info.get("total_episodes", 0))
        if episode_count == 0:
            return []
        directory = _remote_path(self.source.path, "meta/episodes")
        paths = _remote_parquet_files(self._file_io, directory)
        rows = []
        for path in paths:
            rows.extend(_read_remote_parquet(
                self._file_io, path).to_pylist())
        rows.sort(key=lambda row: int(row["episode_index"]))
        if len(rows) != episode_count:
            raise ValueError(
                "LeRobot metadata reports %d Episodes but %d were found."
                % (episode_count, len(rows)))
        return rows

    def _load_tasks(self, info):
        task_count = int(info.get("total_tasks", 0))
        if task_count == 0:
            return []
        path = _remote_path(self.source.path, "meta/tasks.parquet")
        rows = _read_remote_parquet(self._file_io, path).to_pylist()
        tasks = [None] * task_count
        for row in rows:
            index = int(row["task_index"])
            name = row.get("__index_level_0__")
            if name is None:
                name = row.get("task", row.get("name"))
            if index < 0 or index >= task_count or name is None:
                raise ValueError("LeRobot task metadata is invalid: %s" % row)
            tasks[index] = str(name)
        if any(task is None for task in tasks):
            raise ValueError(
                "LeRobot metadata reports %d tasks but %d were found."
                % (task_count, len(rows)))
        return tasks

    def _build_data_ranges(self, info):
        ranges = {}
        for episode in self._episodes:
            path = self._data_path(episode, info)
            begin = int(episode["dataset_from_index"])
            end = int(episode["dataset_to_index"])
            if path in ranges:
                previous_begin, previous_end = ranges[path]
                if begin != previous_end:
                    raise ValueError(
                        "LeRobot data file %s has non-contiguous Episode "
                        "ranges." % path)
                ranges[path] = previous_begin, end
            else:
                ranges[path] = begin, end
        return ranges

    def _episode_for_range(self, begin, end):
        index = bisect_right(self._episode_starts, begin) - 1
        if index < 0:
            raise ValueError("LeRobot frame range starts before Episode 0.")
        episode = self._episodes[index]
        episode_end = int(episode["dataset_to_index"])
        if end > episode_end:
            raise ValueError("LeRobot frame batch crosses an Episode boundary.")
        return episode

    @staticmethod
    def _data_path(episode, info):
        return info["data_path"].format(
            chunk_index=int(episode["data/chunk_index"]),
            file_index=int(episode["data/file_index"]),
        )

    def _cached_video_path(self, name, source_path):
        cached = self._video_cache.get(name)
        if cached is not None and cached[0] == source_path:
            return cached[1]
        if cached is not None:
            try:
                cached[1].unlink()
            except FileNotFoundError:
                pass
        if self._video_temp_dir is None:
            self._video_temp_dir = tempfile.TemporaryDirectory(
                prefix="pypaimon_lerobot_video_")
        output = tempfile.NamedTemporaryFile(
            dir=self._video_temp_dir.name,
            suffix=".mp4",
            delete=False,
        )
        try:
            stream = self._file_io.new_input_stream(source_path)
            with closing(stream) as source_stream:
                shutil.copyfileobj(source_stream, output)
        finally:
            output.close()
        local_path = Path(output.name)
        self._video_cache[name] = source_path, local_path
        return local_path


def _remote_path(root, relative_path):
    return "%s/%s" % (root.rstrip("/"), relative_path.lstrip("/"))


def _read_remote_bytes(source_file_io, path):
    stream = source_file_io.new_input_stream(path)
    with closing(stream) as source_stream:
        return source_stream.read()


def _read_remote_json(source_file_io, path):
    try:
        return json.loads(_read_remote_bytes(
            source_file_io, path).decode("utf-8"))
    except (OSError, UnicodeError, ValueError) as error:
        raise ValueError(
            "Cannot read LeRobot metadata %s: %s" % (path, error)) from error


def _read_remote_parquet(source_file_io, path):
    stream = source_file_io.new_input_stream(path)
    with closing(stream) as source_stream:
        try:
            return pq.read_table(source_stream)
        except (OSError, ValueError, pa.ArrowException) as error:
            raise ValueError(
                "Cannot read LeRobot Parquet file %s: %s"
                % (path, error)) from error


def _remote_parquet_files(source_file_io, directory):
    try:
        statuses = source_file_io.list_status(directory)
    except LegacyOssDirectoryListingError as error:
        raise ValueError(
            "LeRobot URI directory listing is unavailable at %s; use "
            "Jindo or upgrade PyArrow." % directory) from error
    paths = []
    for status in statuses:
        path = _qualified_status_path(directory, status)
        if status.type == pafs.FileType.Directory:
            paths.extend(_remote_parquet_files(source_file_io, path))
        elif status.type == pafs.FileType.File and path.endswith(".parquet"):
            paths.append(path)
    return sorted(paths)


def _has_tasks(dataset, info):
    return int(info.get("total_tasks", 0)) > 0 \
        and getattr(dataset.meta, "tasks", None) is not None


def _schema_from_info(info, feature_mapping, include_task):
    features = info.get("features")
    if not isinstance(features, dict) or not features:
        raise ValueError("LeRobot metadata features must be a non-empty object.")
    source_names = list(features)
    if include_task:
        source_names.append("task")
    mapping = _validated_feature_mapping(feature_mapping, source_names)

    fields = []
    mapped_names = {}
    for source_name, feature in features.items():
        target_name = mapping.get(source_name, source_name)
        mapped_names[source_name] = target_name
        fields.append(_feature_field(target_name, source_name, feature))
    if include_task:
        target_name = mapping.get("task", "task")
        mapped_names["task"] = target_name
        fields.append(pa.field(
            target_name,
            pa.string(),
            nullable=False,
            metadata={b"description": b"LeRobot task"},
        ))
    return pa.schema(fields), mapped_names


def _validated_feature_mapping(feature_mapping, source_names):
    if feature_mapping is None:
        return {}
    if not isinstance(feature_mapping, Mapping):
        raise ValueError("feature_mapping must be a mapping or None.")
    mapping = dict(feature_mapping)
    unknown = sorted(set(mapping).difference(source_names))
    if unknown:
        raise ValueError("feature_mapping contains unknown features: %s" % unknown)
    targets = []
    for source_name in source_names:
        target = mapping.get(source_name, source_name)
        if not isinstance(target, str) or not target:
            raise ValueError(
                "feature_mapping target for %s must be a non-empty string."
                % source_name)
        targets.append(target)
    duplicates = sorted({name for name in targets if targets.count(name) > 1})
    if duplicates:
        raise ValueError(
            "feature_mapping produces duplicate columns: %s" % duplicates)
    return mapping


def _feature_field(target_name, source_name, feature):
    if not isinstance(feature, dict):
        raise ValueError(
            "LeRobot feature %s metadata must be an object." % source_name)
    dtype = str(feature.get("dtype", ""))
    shape = _feature_shape(feature, source_name)
    if dtype in _MEDIA_DTYPES:
        if dtype == "video" and _is_depth_video(feature):
            raise ValueError(
                "LeRobot depth-video feature %s is not supported; its "
                "decoded values cannot be losslessly stored as PNG frames."
                % source_name)
        arrow_type = pa.large_binary()
    else:
        scalar_type = _SCALAR_DTYPES.get(dtype)
        if scalar_type is None:
            suffix = " (uint64 has no lossless Paimon integer mapping)" \
                if dtype == "uint64" else ""
            raise ValueError(
                "Unsupported LeRobot dtype %r for feature %s%s."
                % (dtype, source_name, suffix))
        if pa.types.is_string(scalar_type) and shape not in ((), (1,)):
            raise ValueError(
                "LeRobot string feature %s must be scalar." % source_name)
        arrow_type = _tensor_type(scalar_type, shape)
    description = "LeRobot dtype=%s, shape=%s" % (dtype, list(shape))
    return pa.field(
        target_name,
        arrow_type,
        nullable=False,
        metadata={b"description": description.encode("utf-8")},
    )


def _is_depth_video(feature):
    info = feature.get("info") or feature.get("video_info") or {}
    return bool(info.get("is_depth_map")
                or info.get("video.is_depth_map"))


def _feature_shape(feature, name):
    shape = feature.get("shape", ())
    if shape is None:
        shape = ()
    if not isinstance(shape, (list, tuple)):
        raise ValueError("LeRobot feature %s has an invalid shape: %r" % (name, shape))
    try:
        result = tuple(int(size) for size in shape)
    except (TypeError, ValueError) as error:
        raise ValueError(
            "LeRobot feature %s has an invalid shape: %r" % (name, shape)) from error
    if any(size <= 0 for size in result):
        raise ValueError("LeRobot feature %s has an invalid shape: %r" % (name, shape))
    return result


def _tensor_type(scalar_type, shape):
    if shape in ((), (1,)):
        return scalar_type
    if len(shape) == 1:
        return pa.list_(scalar_type, shape[0])
    result = pa.list_(scalar_type, shape[-1])
    for unused_size in reversed(shape[1:-1]):
        result = pa.list_(result)
    return pa.list_(result)


def _get_or_create_table(connection, table_name, schema, options):
    try:
        return connection.get_table(table_name)
    except (DatabaseNotExistException, TableNotExistException):
        table_options = dict(_DEFAULT_TABLE_OPTIONS)
        if options:
            table_options.update({str(key): str(value)
                                  for key, value in options.items()})
        return connection.create_table(
            table_name,
            schema=schema,
            options=table_options,
        )


def _write_dataset(
        table,
        dataset,
        info,
        source,
        source_schema,
        mapped_names,
        transform,
        batch_size):
    target_schema = _target_schema(table.raw_table)
    write_builder = table.raw_table.new_batch_write_builder()
    table_write = None
    table_commit = None
    commit_started = False
    batch_count = 0
    row_count = 0
    snapshot_recorder = _SnapshotRecorder()

    try:
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_commit.add_commit_callback(snapshot_recorder)
        for begin, end in _episode_batches(dataset, info, batch_size):
            batch = _read_batch(
                dataset, info, begin, end, source_schema, mapped_names)
            if transform is not None:
                transformed = transform(batch)
                transformed = _as_arrow_table(transformed)
                _validate_transform_order(batch, transformed, mapped_names)
                batch = transformed
            batch = _strict_lerobot_table(
                batch,
                target_schema,
                source,
                batch_count,
            )
            table_write.write_arrow(batch)
            batch_count += 1
            row_count += batch.num_rows

        expected_rows = int(info.get("total_frames", len(dataset)))
        if row_count != expected_rows:
            raise ValueError(
                "LeRobot metadata reports %d frames but import produced %d."
                % (expected_rows, row_count))
        messages = table_write.prepare_commit()
        commit_started = True
        table_commit.commit(messages)
        if snapshot_recorder.snapshot_id is None:
            raise RuntimeError(
                "LeRobot append committed without reporting a snapshot id.")
        return LeRobotLoadResult(
            episode_count=int(info.get("total_episodes", 0)),
            batch_count=batch_count,
            row_count=row_count,
            snapshot_id=snapshot_recorder.snapshot_id,
        )
    except BaseException:
        if table_write is not None and not commit_started:
            table_write.abort()
        raise
    finally:
        try:
            if table_write is not None:
                table_write.close()
        finally:
            if table_commit is not None:
                table_commit.close()


def _episode_batches(dataset, info, batch_size):
    episodes = getattr(dataset.meta, "episodes", None)
    episode_count = int(info.get("total_episodes", 0))
    total_frames = int(info.get("total_frames", len(dataset)))
    if episodes is None:
        raise ValueError("LeRobot v3 metadata is missing episode boundaries.")
    expected_begin = 0
    for ordinal in range(episode_count):
        episode = episodes.iloc[ordinal] if hasattr(episodes, "iloc") \
            else episodes[ordinal]
        begin = int(_python_scalar(episode["dataset_from_index"]))
        end = int(_python_scalar(episode["dataset_to_index"]))
        if begin != expected_begin or end <= begin:
            raise ValueError(
                "LeRobot episode %d has invalid frame range [%d, %d); "
                "expected it to start at %d."
                % (ordinal, begin, end, expected_begin))
        while begin < end:
            batch_end = min(begin + batch_size, end)
            yield begin, batch_end
            begin = batch_end
        expected_begin = end
    if expected_begin != total_frames:
        raise ValueError(
            "LeRobot episode ranges cover %d frames but metadata reports %d."
            % (expected_begin, total_frames))


def _read_batch(dataset, info, begin, end, schema, mapped_names):
    read_batch = getattr(dataset, "read_batch", None)
    if callable(read_batch):
        raw = read_batch(begin, end)
    else:
        raw = dataset.hf_dataset.with_format("arrow")[begin:end]
    if isinstance(raw, pa.RecordBatch):
        raw = pa.Table.from_batches([raw])
    elif not isinstance(raw, pa.Table):
        raw = pa.Table.from_pydict(raw)
    features = info["features"]
    video_names = [name for name, feature in features.items()
                   if feature["dtype"] == "video"]
    remote_video_reader = getattr(dataset, "read_video_values", None)
    if callable(remote_video_reader):
        video_values = {
            name: remote_video_reader(name, raw)
            for name in video_names
        }
    else:
        video_values = {name: [] for name in video_names}
        if video_names:
            for index in range(begin, end):
                item = dataset[index]
                for name in video_names:
                    video_values[name].append(
                        _encode_media_frame(item[name]))

    arrays = []
    fields = []
    for source_name, feature in features.items():
        target_name = mapped_names[source_name]
        field = schema.field(target_name)
        dtype = feature["dtype"]
        if dtype == "video":
            values = video_values[source_name]
        else:
            if source_name not in raw.column_names:
                raise ValueError(
                    "LeRobot data is missing metadata feature %s." % source_name)
            values = raw.column(source_name).to_pylist()
            if dtype == "image":
                image_reader = getattr(dataset, "image_bytes", None)
                if callable(image_reader):
                    values = [image_reader(value) for value in values]
                else:
                    values = [_image_bytes(value, dataset.root)
                              for value in values]
            else:
                values = [_normalize_value(value, feature, source_name)
                          for value in values]
        arrays.append(pa.array(values, type=field.type))
        fields.append(field)

    if "task" in mapped_names:
        task_indices = raw.column("task_index").to_pylist()
        arrays.append(pa.array(
            [_task_name(dataset.meta.tasks, value) for value in task_indices],
            type=pa.string(),
        ))
        fields.append(schema.field(mapped_names["task"]))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def _normalize_value(value, feature, name):
    shape = _feature_shape(feature, name)
    if shape in ((), (1,)):
        if isinstance(value, (list, tuple)):
            if len(value) != 1:
                raise ValueError(
                    "LeRobot feature %s expected shape %s, got %s."
                    % (name, shape, _value_shape(value)))
            return value[0]
        return _python_scalar(value)
    actual_shape = _value_shape(value)
    if actual_shape != shape:
        raise ValueError(
            "LeRobot feature %s expected shape %s, got %s."
            % (name, shape, actual_shape))
    return value


def _value_shape(value):
    if hasattr(value, "shape"):
        return tuple(int(size) for size in value.shape)
    if isinstance(value, (list, tuple)):
        if not value:
            return (0,)
        child = _value_shape(value[0])
        if any(_value_shape(item) != child for item in value[1:]):
            return (len(value), -1)
        return (len(value),) + child
    return ()


def _python_scalar(value):
    item = getattr(value, "item", None)
    if callable(item):
        return item()
    return value


def _image_bytes(value, root):
    if value is None:
        raise ValueError("LeRobot image feature contains a null frame.")
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, dict):
        body = value.get("bytes")
        if body is not None:
            return bytes(body)
        image_path = value.get("path")
        if image_path:
            path = Path(image_path)
            if not path.is_absolute():
                path = Path(root) / path
            return path.read_bytes()
    return _encode_media_frame(value)


def _encode_media_frame(value):
    try:
        import numpy as np
        from PIL import Image
    except ImportError as error:
        raise ImportError(
            "LeRobot media import requires numpy and Pillow from the "
            "'pypaimon[lerobot]' extra.") from error

    if isinstance(value, Image.Image):
        image = value
    else:
        detach = getattr(value, "detach", None)
        if callable(detach):
            value = detach().cpu().numpy()
        array = np.asarray(value)
        if array.ndim == 3 and array.shape[0] in (1, 3, 4):
            array = np.transpose(array, (1, 2, 0))
        if np.issubdtype(array.dtype, np.floating):
            array = np.rint(np.clip(array, 0.0, 1.0) * 255.0).astype(np.uint8)
        if array.ndim == 3 and array.shape[2] == 1:
            array = array[:, :, 0]
        try:
            image = Image.fromarray(array)
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(
                "Unsupported LeRobot media frame shape or dtype: %s, %s."
                % (array.shape, array.dtype)) from error
    output = io.BytesIO()
    image.save(output, format="PNG")
    return output.getvalue()


def _task_name(tasks, task_index):
    index = int(_python_scalar(task_index))
    if hasattr(tasks, "iloc"):
        return str(tasks.iloc[index].name)
    task = tasks[index]
    if isinstance(task, dict):
        return str(task.get("task", task.get("name")))
    return str(task)


def _as_arrow_table(value):
    if isinstance(value, pa.RecordBatch):
        return pa.Table.from_batches([value])
    if isinstance(value, pa.Table):
        return value
    raise ValueError("LeRobot transform must return one Arrow table or batch.")


def _validate_transform_order(before, after, mapped_names):
    if before.num_rows != after.num_rows:
        raise ValueError(
            "LeRobot transform must preserve the number of frames in each batch.")
    for source_name in ("episode_index", "frame_index", "index"):
        target_name = mapped_names.get(source_name)
        if target_name is None or target_name not in after.column_names:
            continue
        if not before.column(target_name).equals(after.column(target_name)):
            raise ValueError(
                "LeRobot transform must preserve %s order." % source_name)
