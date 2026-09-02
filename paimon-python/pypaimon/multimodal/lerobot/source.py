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

"""LeRobot source resolution for local, Hub, and FileIO datasets."""

import json
import logging
import posixpath
from bisect import bisect_right
from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import quote, unquote, urlparse, urlunparse

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from pypaimon.common.options import Options
from pypaimon.filesystem.pyarrow_file_io import LegacyOssDirectoryListingError
from pypaimon.multimodal.source_utils import (
    _SourceFileIO,
    _normalize_source_path,
    _qualified_status_path,
)
from pypaimon.multimodal.lerobot.loader import _encode_media_frame


_LOGGER = logging.getLogger(__name__)


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
    source_file_io = _SourceFileIO(Options(source_options))
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
        _close_quietly(source_file_io, "source FileIO")


def _close_quietly(resource, name):
    try:
        resource.close()
    except Exception:
        _LOGGER.warning("Failed to close LeRobot %s.", name, exc_info=True)


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


def _import_lerobot_dataset():
    try:
        from lerobot.datasets.lerobot_dataset import LeRobotDataset
    except ImportError as error:
        raise ImportError(
            "load_from_lerobot requires LeRobot; install "
            "'pypaimon[lerobot]'.") from error
    return LeRobotDataset


def _load_hub_info(source):
    try:
        from lerobot.datasets.lerobot_dataset import LeRobotDatasetMetadata
    except ImportError as error:
        raise ImportError(
            "load_from_lerobot requires LeRobot; install "
            "'pypaimon[lerobot]'.") from error
    try:
        return dict(LeRobotDatasetMetadata(repo_id=source.repo_id).info)
    except Exception as error:
        raise ValueError(
            "Cannot open LeRobot Dataset v3 metadata %s: %s"
            % (source.path, error)) from error


def _open_dataset(LeRobotDataset, source):
    try:
        if source.root is not None:
            return LeRobotDataset(
                repo_id=source.repo_id,
                root=source.root,
                download_videos=False,
            )
        return LeRobotDataset(
            repo_id=source.repo_id,
            download_videos=False,
        )
    except Exception as error:
        raise ValueError(
            "Cannot open LeRobot Dataset v3 source %s: %s"
            % (source.path, error)) from error


def _open_resolved_dataset(LeRobotDataset, source, info):
    if source.file_io is not None:
        return _RemoteLeRobotDataset(source, info)
    return _open_dataset(LeRobotDataset, source)


class _RemoteLeRobotDataset:

    _EPISODE_COLUMNS = [
        "episode_index",
        "dataset_from_index",
        "dataset_to_index",
        "data/chunk_index",
        "data/file_index",
    ]

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
        self._data_ranges = self._build_data_ranges(info)
        self._cached_data_path = None
        self._cached_data_table = None

    def __len__(self):
        return int(self.meta.info.get("total_frames", 0))

    def close(self):
        self._cached_data_table = None

    def read_batch(self, begin, end):
        episode = self._episode_for_range(begin, end)
        relative_path = self._data_path(episode, self.meta.info)
        source_path = _remote_source_path(
            self.source.path,
            relative_path,
            "info.data_path",
            self._file_io,
        )
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
                source_path = _remote_source_path(
                    self.source.path,
                    image_path,
                    "image path",
                    self._file_io,
                )
                return _read_remote_bytes(self._file_io, source_path)
        return _encode_media_frame(value)

    def _load_episodes(self, info):
        episode_count = int(info.get("total_episodes", 0))
        if episode_count == 0:
            return []
        directory = _remote_path(self.source.path, "meta/episodes")
        paths = _remote_parquet_files(self._file_io, directory)
        rows = []
        for path in paths:
            rows.extend(_read_remote_parquet(
                self._file_io,
                path,
                columns=self._EPISODE_COLUMNS,
            ).to_pylist())
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
        path = info["data_path"].format(
            chunk_index=int(episode["data/chunk_index"]),
            file_index=int(episode["data/file_index"]),
        )
        return _relative_dataset_path(path, "info.data_path")


def _remote_path(root, relative_path):
    return "%s/%s" % (root.rstrip("/"), relative_path.lstrip("/"))


def _relative_dataset_path(path, name):
    if not isinstance(path, str) or not path:
        raise ValueError("LeRobot %s must be a relative path." % name)
    path = _fully_unquote(path)
    if "\\" in path or urlparse(path).scheme or path.startswith(("/", "~")):
        raise ValueError(
            "LeRobot %s must stay within the source directory: %s"
            % (name, path))
    normalized = posixpath.normpath(path)
    if normalized in (".", "..") or normalized.startswith("../"):
        raise ValueError(
            "LeRobot %s must stay within the source directory: %s"
            % (name, path))
    return normalized


def _remote_source_path(root, path, name, source_file_io=None):
    if not isinstance(path, str) or not path:
        _relative_dataset_path(path, name)
    root_uri = urlparse(root)
    path_uri = urlparse(path)
    root_path = posixpath.normpath(_fully_unquote(root_uri.path) or "/")
    if path_uri.scheme:
        if path_uri.query or path_uri.fragment \
                or path_uri.scheme.lower() != root_uri.scheme.lower() \
                or path_uri.netloc != root_uri.netloc:
            raise ValueError(
                "LeRobot %s must stay within the source directory: %s"
                % (name, path))
        source_path = posixpath.normpath(
            _fully_unquote(path_uri.path) or "/")
    else:
        relative_path = _relative_dataset_path(path, name)
        source_path = posixpath.normpath(posixpath.join(
            root_path,
            relative_path,
        ))
    if posixpath.commonpath([root_path, source_path]) != root_path:
        raise ValueError(
            "LeRobot %s must stay within the source directory: %s"
            % (name, path))
    source_uri = urlunparse((
        root_uri.scheme,
        root_uri.netloc,
        quote(source_path, safe="/:"),
        "",
        "",
        "",
    ))
    _validate_filesystem_containment(
        source_file_io, root, source_uri, name, path)
    return source_uri


def _fully_unquote(path):
    decoded = unquote(path)
    while decoded != path:
        path = decoded
        decoded = unquote(path)
    return decoded


def _validate_filesystem_containment(
        source_file_io, root, source, name, original_path):
    to_filesystem_path = getattr(
        source_file_io, "to_filesystem_path", None)
    if not callable(to_filesystem_path):
        return
    root_path = _fully_unquote(to_filesystem_path(root))
    source_path = _fully_unquote(to_filesystem_path(source))
    if urlparse(root).scheme.lower() == "file":
        root_path = Path(root_path).resolve()
        source_path = Path(source_path).resolve()
        try:
            source_path.relative_to(root_path)
        except ValueError as error:
            raise ValueError(
                "LeRobot %s must stay within the source directory: %s"
                % (name, original_path)) from error
        return
    else:
        root_path = posixpath.normpath(root_path)
        source_path = posixpath.normpath(source_path)
    relative_path = posixpath.relpath(source_path, root_path)
    if relative_path == ".." or relative_path.startswith("../"):
        raise ValueError(
            "LeRobot %s must stay within the source directory: %s"
            % (name, original_path))


def _validate_info_paths(info):
    for name in ("data_path", "video_path"):
        path = info.get(name)
        if path is not None:
            _relative_dataset_path(path, "info.%s" % name)


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


def _read_remote_parquet(source_file_io, path, columns=None):
    stream = source_file_io.new_input_stream(path)
    with closing(stream) as source_stream:
        try:
            return pq.read_table(source_stream, columns=columns)
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
