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

"""Strict append-only ingestion from seekable HDF5 sources."""

import os
import re
import sys
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Callable, Mapping, Optional
from urllib.parse import quote, unquote, urlparse, urlunparse

import pyarrow as pa
import pyarrow.fs as pafs

from pypaimon.common.options import Options
from pypaimon.filesystem.local_file_io import _file_uri_path
from pypaimon.filesystem.pyarrow_file_io import LegacyOssDirectoryListingError
from pypaimon.filesystem.resolving_file_io import ResolvingFileIO
from pypaimon.multimodal.arrow_utils import strict_arrow_table
from pypaimon.multimodal.source_utils import (
    _source_path_text,
    _validated_source_options,
    _validate_source_kerberos,
)
from pypaimon.multimodal.table import _target_schema
from pypaimon.write.commit_callback import CommitCallback


_HDF5_SUFFIXES = (".h5", ".hdf5")


@dataclass(frozen=True)
class Hdf5File:
    """Read context supplied to an HDF5 transform."""

    path: str

    @property
    def local_path(self) -> Optional[Path]:
        """Decoded local path, or ``None`` for a remote source."""
        parsed = urlparse(self.path)
        if parsed.scheme.lower() != "file":
            return None
        return Path(_file_uri_path(parsed))

    @property
    def name(self) -> str:
        """Base name of the local path or remote URI."""
        local_path = self.local_path
        if local_path is not None:
            return local_path.name
        parsed = urlparse(self.path)
        path = unquote(parsed.path) if parsed.scheme else self.path
        return PurePosixPath(path).name

    @property
    def stem(self) -> str:
        """Base name without the final HDF5 suffix."""
        return PurePosixPath(self.name).stem


@dataclass(frozen=True)
class Hdf5LoadResult:
    """Counts and optional snapshot for one ``load_from_hdf5`` call."""

    file_count: int
    batch_count: int
    row_count: int
    snapshot_id: Optional[int]


class _SnapshotRecorder(CommitCallback):

    def __init__(self):
        self.snapshot_id = None

    def call(self, context):
        self.snapshot_id = context.snapshot.id


class _Hdf5SourceFileIO:
    """Resolve HDF5 URIs while keeping decoding local to external sources."""

    def __init__(self, options):
        self._resolver = ResolvingFileIO(options)

    def _resolve(self, path):
        file_io = self._resolver._get_fileio(path)
        native_path = file_io.to_filesystem_path(path)
        if urlparse(path).scheme.lower() != "file":
            native_path = unquote(native_path)
        return file_io, native_path

    def get_file_status(self, path):
        file_io, native_path = self._resolve(path)
        return file_io.get_file_status(native_path)

    def list_status(self, path):
        file_io, native_path = self._resolve(path)
        return file_io.list_status(native_path)

    def new_input_stream(self, path):
        file_io, native_path = self._resolve(path)
        return file_io.new_input_stream(native_path)

    def to_filesystem_path(self, path):
        return self._resolve(path)[1]

    def close(self):
        self._resolver.close()


def load_from_hdf5(
        table,
        paths,
        *,
        transform: Callable,
        source_options: Optional[Mapping[str, object]] = None):
    """Load HDF5 files into an existing multimodal table.

    ``transform`` receives an open ``h5py.File`` and :class:`Hdf5File`, and
    must return one Arrow table/batch or an iterable of Arrow tables/batches.
    All unique files and batches in one call share one writer and one commit.
    Local paths and FileIO-supported URIs are accepted. ``source_options`` are
    used only for source FileIO resolution and are never inherited from the
    target table's warehouse.

    This API is strictly append-only. It does not track sources or detect
    duplicates between calls, so calling it again writes the rows again. It is
    not retry-safe: a commit exception may have happened after the snapshot
    became visible and is returned without retrying or aborting written files.
    Empty discovery is a no-op and returns zero counts with no snapshot.
    """
    if sys.version_info < (3, 8):
        raise RuntimeError(
            "load_from_hdf5 requires Python 3.8 or newer; the hdf5 extra "
            "is not available on older Python versions.")
    if not callable(transform):
        raise ValueError("transform must be callable.")
    validated_options = _validated_source_options(source_options)
    path_values = _path_values(paths)
    _validate_source_kerberos(path_values, validated_options)
    source_file_io = _Hdf5SourceFileIO(Options(validated_options))
    try:
        files = _discover_hdf5_files(path_values, source_file_io)
        if not files:
            return Hdf5LoadResult(
                file_count=0,
                batch_count=0,
                row_count=0,
                snapshot_id=None,
            )
        # Preserve the dependency-free no-op for empty discovery; only require
        # h5py once at least one HDF5 source will actually be opened.
        try:
            import h5py
        except ImportError as error:
            raise ImportError(
                "load_from_hdf5 requires h5py; install 'pypaimon[hdf5]'."
            ) from error
        return _load_hdf5_files(
            table, files, transform, source_file_io, h5py)
    finally:
        source_file_io.close()


def _load_hdf5_files(table, files, transform, source_file_io, h5py):
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

        for source in files:
            source_row_count = 0
            with closing(source_file_io.new_input_stream(source.path)) as stream:
                _require_seekable(stream, source)
                with h5py.File(stream, "r") as h5:
                    transformed = transform(h5, source)
                    batches = None
                    try:
                        batches = _arrow_batches(transformed)
                        for value in batches:
                            arrow_table = _strict_arrow_table(
                                value,
                                target_schema,
                                source,
                                batch_count,
                            )
                            batch_count += 1
                            row_count += arrow_table.num_rows
                            source_row_count += arrow_table.num_rows
                            if arrow_table.num_rows:
                                table_write.write_arrow(arrow_table)
                    finally:
                        _close_transform_iterator(
                            batches if batches is not None else transformed)

            if source_row_count == 0:
                raise ValueError(
                    "HDF5 source %s produced no rows." % source.path)

        commit_messages = table_write.prepare_commit()
        commit_started = True
        table_commit.commit(commit_messages)
        if snapshot_recorder.snapshot_id is None:
            raise RuntimeError(
                "HDF5 append committed without reporting a snapshot id.")
        return Hdf5LoadResult(
            file_count=len(files),
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


def _discover_hdf5_files(paths, source_file_io):
    values = _path_values(paths)
    normalized = {}
    visited_directories = set()
    for value in values:
        input_path = _source_path_text(value)
        path = _normalize_source_path(input_path)
        try:
            status = source_file_io.get_file_status(path)
        except FileNotFoundError as error:
            _discover_missing_path(
                source_file_io,
                path,
                normalized,
                visited_directories,
                error,
                input_path,
            )
            continue
        _discover_status(
            source_file_io,
            path,
            status,
            normalized,
            visited_directories,
            explicit_path=input_path,
        )
    return [normalized[key] for key in sorted(normalized)]


def _discover_missing_path(
        source_file_io,
        path,
        normalized,
        visited_directories,
        not_found_error,
        input_path):
    if _hdf5_suffix(path):
        raise ValueError(
            "HDF5 path does not exist: %s" % input_path
        ) from not_found_error
    try:
        children = source_file_io.list_status(path)
    except LegacyOssDirectoryListingError as error:
        _raise_legacy_directory_listing_error(path, error)
    if not children:
        raise ValueError(
            "HDF5 path does not exist: %s" % input_path
        ) from not_found_error
    visited_directories.add(path)
    _discover_directory_children(
        source_file_io,
        path,
        children,
        normalized,
        visited_directories,
    )


def _discover_status(
        source_file_io,
        parent_path,
        status,
        normalized,
        visited_directories,
        explicit_path=None):
    path = _qualified_status_path(parent_path, status)
    if status.type == pafs.FileType.File:
        if not _hdf5_suffix(path):
            if explicit_path is not None:
                raise ValueError(
                    "HDF5 file has unsupported suffix: %s; expected .h5 or "
                    ".hdf5." % explicit_path)
            return
        normalized[path] = Hdf5File(path=path)
        return
    if status.type != pafs.FileType.Directory:
        raise ValueError("Unsupported HDF5 source status for path: %s" % path)
    if path in visited_directories:
        return
    visited_directories.add(path)
    try:
        children = source_file_io.list_status(path)
    except LegacyOssDirectoryListingError as error:
        _raise_legacy_directory_listing_error(path, error)
    _discover_directory_children(
        source_file_io,
        path,
        children,
        normalized,
        visited_directories,
    )


def _discover_directory_children(
        source_file_io,
        directory,
        children,
        normalized,
        visited_directories):
    for child in children:
        _discover_status(
            source_file_io,
            directory,
            child,
            normalized,
            visited_directories,
            explicit_path=None,
        )


def _raise_legacy_directory_listing_error(path, error):
    raise ValueError(
        "Recursive HDF5 discovery is unavailable for legacy OSS at %s; "
        "pass explicit HDF5 file paths, use Jindo, or upgrade PyArrow."
        % path) from error


def _normalize_source_path(value):
    path = _source_path_text(value)
    parsed = urlparse(path)
    if _is_windows_drive_path(parsed):
        windows_path = PureWindowsPath(path)
        if not windows_path.is_absolute():
            raise ValueError("Windows source paths must be absolute: %s" % path)
        return "file:///%s" % quote(windows_path.as_posix(), safe="/:")
    if not parsed.scheme:
        return Path(path).expanduser().resolve().as_uri()
    return _quote_uri_path(path)


def _quote_uri_path(uri):
    match = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*://[^/]*)(.*)$", uri)
    if match is None:
        return uri
    return match.group(1) + quote(match.group(2), safe="/:%")


def _qualified_status_path(parent_path, status):
    status_path = str(status.path)
    status_uri = urlparse(status_path)
    if status_uri.scheme and not _is_windows_drive_path(status_uri):
        return _quote_uri_path(status_path)

    parent_uri = urlparse(parent_path)
    scheme = parent_uri.scheme.lower()
    if scheme == "file":
        return _normalize_source_path(status_path)
    if not scheme or _is_windows_drive_path(parent_uri):
        return _normalize_source_path(status_path)

    if scheme in ("hdfs", "viewfs"):
        return urlunparse((
            scheme,
            parent_uri.netloc,
            quote("/" + status_path.lstrip("/"), safe="/:"),
            "",
            "",
            "",
        ))

    key = status_path.lstrip("/")
    if parent_uri.netloc and not (
            key == parent_uri.netloc
            or key.startswith(parent_uri.netloc + "/")):
        key = parent_uri.netloc + "/" + key
    return "%s://%s" % (scheme, quote(key, safe="/:"))


def _is_windows_drive_path(parsed):
    return len(parsed.scheme) == 1 and not parsed.netloc


def _hdf5_suffix(path):
    parsed = urlparse(path)
    return PurePosixPath(unquote(parsed.path)).suffix.lower() in _HDF5_SUFFIXES


def _path_values(paths):
    if isinstance(paths, (str, os.PathLike)):
        return [paths]
    if isinstance(paths, bytes):
        raise ValueError("paths must be a path or an iterable of paths.")
    try:
        return list(paths)
    except TypeError as error:
        raise ValueError(
            "paths must be a path or an iterable of paths.") from error


def _require_seekable(stream, source):
    required = ("read", "seek", "tell")
    if any(not callable(getattr(stream, method, None)) for method in required):
        raise ValueError(
            "HDF5 source stream must be seekable: %s" % source.path)
    seekable = getattr(stream, "seekable", None)
    if callable(seekable) and not seekable():
        raise ValueError(
            "HDF5 source stream must be seekable: %s" % source.path)
    try:
        stream.seek(stream.tell())
    except (OSError, TypeError, ValueError) as error:
        raise ValueError(
            "HDF5 source stream must be seekable: %s" % source.path
        ) from error


def _strict_arrow_table(data, target_schema, source, batch_index):
    return strict_arrow_table(
        data,
        target_schema,
        source.path,
        batch_index,
        "HDF5",
    )


def _arrow_batches(transformed):
    if isinstance(transformed, (pa.Table, pa.RecordBatch)):
        return iter([transformed])
    if transformed is None or isinstance(transformed, (str, bytes, dict)):
        raise ValueError(
            "HDF5 transform must return Arrow data or an iterable of Arrow data.")
    try:
        return iter(transformed)
    except TypeError as error:
        raise ValueError(
            "HDF5 transform must return Arrow data or an iterable of Arrow data."
        ) from error


def _close_transform_iterator(iterator):
    close = getattr(iterator, "close", None)
    if close is not None:
        close()
