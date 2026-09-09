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

"""ROSBag source descriptions and discovery."""

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Optional, Tuple
from urllib.parse import quote, unquote, urlparse

import pyarrow.fs as pafs

from pypaimon.multimodal.source_utils import (
    _normalize_source_path,
    _qualified_status_path,
)


@dataclass(frozen=True)
class RosbagSourceMember:
    """One physical file belonging to a logical ROSBag source."""

    uri: str
    relative_path: str
    size: int
    mtime_ns: Optional[int]


@dataclass(frozen=True)
class RosbagSourceManifest:
    """Immutable physical-file manifest for one logical ROSBag source."""

    uri: str
    format: str
    members: Tuple[RosbagSourceMember, ...]
    expected_message_count: Optional[int] = None
    directory_members: Optional[Tuple[str, ...]] = None


@dataclass(frozen=True)
class RosbagSource:
    """Read context supplied to a ROSBag transform.

    ``format`` is one of ``ros1``, ``ros2_mcap``, ``ros2_sqlite3``, or
    ``ros2_sqlite3_fragment``.
    """

    uri: str
    local_path: Path
    format: str

    @property
    def name(self):
        """Base name of the original source URI."""
        return PurePosixPath(unquote(urlparse(self.uri).path)).name

    @property
    def stem(self):
        """Base name without the final suffix."""
        return PurePosixPath(self.name).stem

    @property
    def is_remote(self):
        """Whether the original source requires FileIO materialization."""
        return urlparse(self.uri).scheme.lower() != "file"


def _discover_rosbag_sources(
        paths, source_file_io, allow_storage_fragment=False):
    """Discover normalized logical ROSBag sources."""
    manifests = {}
    visited_directories = set()
    for value in paths:
        path = _normalize_source_path(value)
        try:
            status = source_file_io.get_file_status(path)
        except FileNotFoundError as not_found_error:
            try:
                children = source_file_io.list_status(path)
            except (FileNotFoundError, KeyError):
                raise ValueError(
                    "ROSBag source path does not exist: %s" % path
                ) from not_found_error
            if not children:
                raise ValueError(
                    "ROSBag source path does not exist: %s" % path
                ) from not_found_error
            status = pafs.FileInfo(path, pafs.FileType.Directory)
        _discover_status(
            path,
            status,
            source_file_io,
            manifests,
            visited_directories,
            allow_storage_fragment,
            explicit=True,
        )
    return [manifests[key] for key in sorted(manifests)]


def _discover_status(
        parent_path,
        status,
        source_file_io,
        manifests,
        visited_directories,
        allow_storage_fragment,
        *,
        explicit):
    qualified = _qualified_status_path(parent_path, status)
    if status.type == pafs.FileType.File:
        manifest = _file_manifest(
            qualified,
            status,
            allow_storage_fragment,
            explicit=explicit,
        )
        if manifest is not None:
            manifests[manifest.uri] = manifest
        return
    if status.type != pafs.FileType.Directory:
        if explicit:
            raise ValueError("Unsupported ROSBag source: %s" % qualified)
        return
    if qualified in visited_directories:
        return
    visited_directories.add(qualified)

    metadata_uri = _join_source_path(qualified, "metadata.yaml")
    try:
        source_file_io.get_file_status(metadata_uri)
    except FileNotFoundError:
        children = source_file_io.list_status(qualified)
        if explicit and any(
                _storage_member_status(qualified, child)
                for child in children):
            raise ValueError(
                "ROS2 recording directory is missing metadata.yaml: %s"
                % qualified)
        for child in children:
            _discover_status(
                qualified,
                child,
                source_file_io,
                manifests,
                visited_directories,
                allow_storage_fragment,
                explicit=False,
            )
        return

    manifest = _ros2_directory_manifest(qualified, source_file_io)
    manifests[manifest.uri] = manifest


def _storage_member_status(parent, status):
    if status.type != pafs.FileType.File:
        return False
    path = _qualified_status_path(parent, status)
    suffix = PurePosixPath(unquote(urlparse(path).path)).suffix.lower()
    return suffix in (".db3", ".mcap")


def _file_manifest(path, status, allow_storage_fragment, *, explicit):
    decoded_path = unquote(urlparse(path).path)
    if decoded_path.lower().endswith(".bag.active"):
        raise ValueError(
            "ROS1 recording appears to be still being written: %s" % path)
    suffix = PurePosixPath(decoded_path).suffix.lower()
    formats = {".bag": "ros1", ".mcap": "ros2_mcap"}
    if suffix == ".db3" and not allow_storage_fragment:
        raise ValueError(
            "Standalone ROS2 .db3 files may be incomplete recording "
            "fragments; pass allow_storage_fragment=True to import the "
            "file without recording completeness guarantees: %s" % path)
    if suffix == ".db3" and explicit:
        formats[suffix] = "ros2_sqlite3_fragment"
    if suffix not in formats:
        if explicit:
            raise ValueError("Unsupported ROSBag source file: %s" % path)
        return None
    member = RosbagSourceMember(
        uri=path,
        relative_path=PurePosixPath(decoded_path).name,
        size=status.size,
        mtime_ns=_status_mtime_ns(status),
    )
    return RosbagSourceManifest(
        uri=path,
        format=formats[suffix],
        members=(member,),
    )


def _ros2_directory_manifest(directory, source_file_io):
    metadata_uri = _join_source_path(directory, "metadata.yaml")
    try:
        metadata_status = source_file_io.get_file_status(metadata_uri)
    except FileNotFoundError as error:
        raise ValueError(
            "ROS2 recording is missing metadata.yaml: %s" % directory
        ) from error
    if metadata_status.type != pafs.FileType.File:
        raise ValueError(
            "ROS2 metadata.yaml is not a file: %s" % metadata_uri)

    metadata = _read_ros2_metadata(metadata_uri, source_file_io)
    storage_identifier = metadata.get("storage_identifier")
    formats = {
        "sqlite3": "ros2_sqlite3",
        "mcap": "ros2_mcap",
    }
    if storage_identifier not in formats:
        raise ValueError(
            "Unsupported ROS2 storage identifier %r in %s."
            % (storage_identifier, metadata_uri))

    members = [
        _source_member(
            "metadata.yaml",
            metadata_status,
            directory,
        )
    ]
    normalized_members = set()
    for value in metadata.get("relative_file_paths", []):
        relative_path = _safe_ros2_member_path(value)
        collision_key = relative_path.casefold()
        if collision_key in normalized_members:
            raise ValueError(
                "duplicate ROS2 metadata relative_file_paths entry: %r"
                % value)
        normalized_members.add(collision_key)
        member_uri = _join_source_path(directory, relative_path)
        if storage_identifier == "sqlite3":
            _reject_sqlite_sidecars(member_uri, source_file_io)
        try:
            status = source_file_io.get_file_status(member_uri)
        except FileNotFoundError as error:
            raise ValueError(
                "ROS2 recording member is missing: %s" % member_uri
            ) from error
        if status.type != pafs.FileType.File:
            raise ValueError(
                "ROS2 recording member is not a file: %s" % member_uri)
        members.append(_source_member(
            relative_path,
            status,
            directory,
        ))

    return RosbagSourceManifest(
        uri=directory,
        format=formats[storage_identifier],
        members=tuple(members),
        expected_message_count=int(metadata.get("message_count", 0)),
        directory_members=_listed_directory_members(
            directory, source_file_io),
    )


def _read_ros2_metadata(metadata_uri, source_file_io):
    try:
        from ruamel.yaml import YAML
        from ruamel.yaml.error import YAMLError
    except ImportError as error:
        raise ImportError(
            "ROSBag loading requires rosbags; install 'pypaimon[rosbag]'."
        ) from error
    with closing(source_file_io.new_input_stream(metadata_uri)) as stream:
        content = stream.read()
    try:
        document = YAML(typ="safe").load(content)
        return document["rosbag2_bagfile_information"]
    except (KeyError, TypeError, ValueError, YAMLError) as error:
        raise ValueError(
            "Cannot read ROS2 metadata: %s" % metadata_uri) from error


def _listed_directory_members(directory, source_file_io):
    return tuple(sorted(
        _qualified_status_path(directory, status)
        for status in source_file_io.list_status(directory)
    ))


def _source_member(relative_path, status, parent):
    return RosbagSourceMember(
        uri=_qualified_status_path(parent, status),
        relative_path=relative_path,
        size=status.size,
        mtime_ns=_status_mtime_ns(status),
    )


def _join_source_path(directory, relative_path):
    return "%s/%s" % (
        directory.rstrip("/"), quote(relative_path, safe="/:%"))


def _reject_sqlite_sidecars(database_uri, source_file_io):
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = database_uri + suffix
        try:
            source_file_io.get_file_status(sidecar)
        except FileNotFoundError:
            continue
        raise ValueError(
            "ROS2 SQLite recording is not finalized or requires recovery; "
            "found sidecar file: %s" % sidecar)


def _safe_ros2_member_path(value):
    if not isinstance(value, str) or not value:
        raise ValueError(
            "unsafe ROS2 metadata relative_file_paths entry: %r"
            % value)
    decoded = value
    for _ in range(8):
        next_value = unquote(decoded)
        if next_value == decoded:
            break
        decoded = next_value
    else:
        raise ValueError(
            "unsafe ROS2 metadata relative_file_paths entry: %r"
            % value)
    parsed = urlparse(decoded)
    path = PurePosixPath(decoded)
    if (
            "\\" in decoded
            or parsed.scheme
            or parsed.netloc
            or parsed.query
            or parsed.fragment
            or path.is_absolute()
            or any(part in ("", ".", "..") for part in path.parts)):
        raise ValueError(
            "unsafe ROS2 metadata relative_file_paths entry: %r"
            % value)
    if len(path.parts) != 1:
        raise ValueError(
            "nested ROS2 metadata relative_file_paths entries are not "
            "supported by rosbags: %r" % value)
    return path.as_posix()


def _status_mtime_ns(status):
    value = getattr(status, "mtime_ns", None)
    if value is not None:
        return None if value < 0 else value
    value = getattr(status, "mtime", None)
    if value is None:
        return None
    if hasattr(value, "timestamp"):
        value = value.timestamp()
    return None if value < 0 else int(value * 1_000_000_000)
