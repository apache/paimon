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

"""FileIO that sends each request to an io-cache target or the origin endpoint."""

import logging
import os
import threading
from typing import Callable, Dict, Iterable, List, Optional, Tuple

import pyarrow.fs as pafs
from pyarrow._fs import FileSystemHandler

from pypaimon.common.file_io import FileIO
from pypaimon.filesystem.io_cache_routing import IoCacheRouting, Op, schema_file_prefixes
from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO
from pypaimon.filesystem.resolving_file_io import ResolvingFileIO


class IoCacheRoutingFileIO(FileIO):
    """Sends each call to origin or to a target FileIO, all built for the table path.
    Calls and attributes that are not routed go to origin."""

    def __init__(self, routing: IoCacheRouting, origin: FileIO,
                 create_target: Callable[[str], FileIO]):
        self._routing = routing
        self._origin = origin
        self._create_target = create_target
        self._targets: Dict[str, FileIO] = {}
        self._targets_lock = threading.Lock()
        self._pid = os.getpid()
        self._filesystem = pafs.PyFileSystem(_IoCacheRoutingHandler(self))
        self._base = self
        self._table: Optional[Tuple[str, Tuple[str, ...], Tuple[str, ...]]] = None
        self._views: Dict[tuple, 'IoCacheRoutingFileIO'] = {}

    def origin_only(self) -> FileIO:
        return self._origin

    def for_table(self, table_path: str, file_prefixes: Iterable[str], branch: str) -> FileIO:
        return self._base.table_view(table_view_spec(self._table, table_path, file_prefixes, branch))

    def table_view(self, table, catalog_prefixes=None) -> 'IoCacheRoutingFileIO':
        """The view of this FileIO for table = (path, prefixes, branches), see table_view_spec;
        catalog_prefixes(branch) adds the prefixes a catalog keeps outside the schema directory."""
        view = self._views.get(table)
        if view is not None:
            return view
        self._check_pid()
        with self._targets_lock:
            view = self._views.get(table)
            if view is None:
                table_path, prefixes, branches = table
                history = _SchemaPrefixHistory(self._origin, table_path, branches, catalog_prefixes)
                # The view takes its targets from this FileIO.
                view = IoCacheRoutingFileIO(
                    self._routing.for_table(table_path, prefixes, history), self._origin, self._file_io)
                view._base, view._table = self, table
                self._views[table] = view
            return view

    def _check_pid(self):
        if self._pid != os.getpid():
            # A forked child inherits the lock in whatever state a parent thread left it.
            self._targets = {}
            self._targets_lock = threading.Lock()
            self._pid = os.getpid()

    @property
    def properties(self):
        return self._origin.properties

    @property
    def filesystem(self):
        """Filesystem for readers; it takes the URIs returned by to_filesystem_path."""
        return self._filesystem

    def _route(self, op: Op, path: str) -> Optional[str]:
        return self._routing.route(op, path)

    def _file_io(self, name: Optional[str]) -> FileIO:
        """Origin, or the FileIO of a target, created on first use."""
        if name is None:
            return self._origin
        self._check_pid()
        file_io = self._targets.get(name)
        if file_io is not None:
            return file_io
        with self._targets_lock:
            file_io = self._targets.get(name)
            if file_io is None:
                file_io = self._create_target(name)
                if isinstance(file_io, (PyArrowFileIO, ResolvingFileIO)):
                    file_io.creates_parent_dirs = False
                    file_io.cleans_up_failed_writes = False
                self._targets[name] = file_io
            return file_io

    def _call(self, op: Op, path: str, action):
        return action(self._file_io(self._route(op, path)))

    def _write(self, path: str, action, cleans_up: bool = False):
        name = self._route(Op.WRITE, path)
        if name is None:
            return action(self._origin)
        # A target takes only the object; its parent directories are created on origin, as unrouted.
        self._origin.create_parent_dirs(path)
        try:
            return action(self._file_io(name))
        except Exception:
            # The cleanup the origin FileIO would do after a failed write also runs on origin.
            if cleans_up:
                self._origin.delete_quietly(path)
            raise

    def _call_batch(self, op: Op, paths: List[str], action) -> list:
        """Runs action(file_io, paths) once per routed group; results follow the order of paths."""
        results = [None] * len(paths)
        groups: Dict[Optional[str], List[int]] = {}
        for index, path in enumerate(paths):
            groups.setdefault(self._route(op, path), []).append(index)
        for name, indices in groups.items():
            group = [paths[index] for index in indices]
            values = action(self._file_io(name), group)
            for index, value in zip(indices, values):
                results[index] = value
        return results

    def new_input_stream(self, path: str):
        return self._call(Op.READ, path, lambda io: io.new_input_stream(path))

    def get_file_status(self, path: str):
        return self._call(Op.META, path, lambda io: io.get_file_status(path))

    def get_file_size(self, path: str) -> int:
        return self._call(Op.META, path, lambda io: io.get_file_size(path))

    def new_output_stream(self, path: str):
        return self._write(path, lambda io: io.new_output_stream(path))

    def create_parent_dirs(self, path: str):
        return self._origin.create_parent_dirs(path)

    def exists(self, path: str) -> bool:
        return self._call(Op.EXISTS, path, lambda io: io.exists(path))

    def exists_batch(self, paths: List[str]) -> Dict[str, bool]:
        paths = list(paths)
        found = self._call_batch(
            Op.EXISTS, paths, lambda io, group: [io.exists_batch(group)[p] for p in group])
        return dict(zip(paths, found))

    def is_dir(self, path: str) -> bool:
        return self._origin.is_dir(path)

    def list_status(self, path: str):
        return self._origin.list_status(path)

    def delete(self, path: str, recursive: bool = False) -> bool:
        return self._origin.delete(path, recursive)

    def mkdirs(self, path: str) -> bool:
        return self._origin.mkdirs(path)

    def rename(self, src: str, dst: str) -> bool:
        return self._origin.rename(src, dst)

    def copy_file(self, source_path: str, target_path: str, overwrite: bool = False):
        return self._origin.copy_file(source_path, target_path, overwrite)

    def try_to_write_atomic(self, path: str, content: str) -> bool:
        return self._origin.try_to_write_atomic(path, content)

    def write_file(self, path: str, content: str, overwrite: bool = False):
        # The existence check before a write runs on origin, so no cache sees the file before it exists.
        if not overwrite and self._origin.exists(path):
            raise FileExistsError(f"File {path} already exists and overwrite=False")
        return self._write(path, lambda io: io.write_file(path, content, True))

    def overwrite_file_utf8(self, path: str, content: str):
        return self._write(path, lambda io: io.overwrite_file_utf8(path, content))

    # Helpers that delete, or check right after a delete, run on origin, probes included.

    def delete_quietly(self, path: str):
        return self._origin.delete_quietly(path)

    def delete_files_quietly(self, files: List[str]):
        return self._origin.delete_files_quietly(files)

    def delete_directory_quietly(self, directory: str):
        return self._origin.delete_directory_quietly(directory)

    def check_or_mkdirs(self, path: str):
        return self._origin.check_or_mkdirs(path)

    def copy_files(self, source_directory: str, target_directory: str, overwrite: bool = False):
        return self._origin.copy_files(source_directory, target_directory, overwrite)

    def create_blob_presigned_url(self, table_root, descriptor, validity) -> str:
        return self._origin.create_blob_presigned_url(table_root, descriptor, validity)

    def to_filesystem_path(self, path: str) -> str:
        # The routing filesystem needs the full path, it converts it for the chosen filesystem.
        return path

    def write_parquet(self, path: str, data, compression: str = 'zstd',
                      zstd_level: int = 1, **kwargs):
        return self._write(
            path, lambda io: io.write_parquet(path, data, compression, zstd_level, **kwargs), True)

    def write_orc(self, path: str, data, compression: str = 'zstd',
                  zstd_level: int = 1, **kwargs):
        return self._write(
            path, lambda io: io.write_orc(path, data, compression, zstd_level, **kwargs), True)

    def write_avro(self, path: str, data, avro_schema=None,
                   compression: str = 'zstd', zstd_level: int = 1, **kwargs):
        return self._write(path, lambda io: io.write_avro(
            path, data, avro_schema, compression, zstd_level, **kwargs))

    def close(self):
        if self._base is not self:
            # A table view shares the FileIOs of its base, which closes them.
            return
        with self._targets_lock:
            targets = list(self._targets.values())
            self._targets.clear()
        try:
            self._origin.close()
        finally:
            for target in targets:
                target.close()

    def __getattr__(self, name):
        origin = self.__dict__.get('_origin')
        if origin is None:
            raise AttributeError(name)
        return getattr(origin, name)


class _SchemaPrefixHistory:
    """Data file prefixes of every schema of a table's branches, read from origin at most once."""

    def __init__(self, origin: FileIO, table_path: str, branches: Tuple[str, ...],
                 catalog_prefixes: Optional[Callable[[str], Iterable[str]]] = None):
        self._origin = origin
        self._table_path = table_path
        self._branches = branches
        self._catalog_prefixes = catalog_prefixes
        self._prefixes: Optional[Tuple[str, ...]] = None
        self._lock = threading.Lock()
        self._pid = os.getpid()

    def __call__(self) -> Tuple[str, ...]:
        if self._prefixes is not None:
            return self._prefixes
        if self._pid != os.getpid():
            self._lock = threading.Lock()
            self._pid = os.getpid()
        with self._lock:
            if self._prefixes is None:
                self._prefixes = self._load()
            return self._prefixes

    def _load(self) -> Tuple[str, ...]:
        prefixes = []
        for branch in self._branches:
            prefixes += self._quietly(self._schema_directory_prefixes, branch)
            if self._catalog_prefixes is not None:
                prefixes += self._quietly(self._catalog_prefixes, branch)
        return tuple(dict.fromkeys(prefixes))

    def _schema_directory_prefixes(self, branch: str) -> List[str]:
        from pypaimon.schema.schema_manager import SchemaManager
        schemas = SchemaManager(self._origin, self._table_path, branch).list_all()
        return [prefix for schema in schemas for prefix in schema_file_prefixes(schema.options)]

    def _quietly(self, load: Callable[[str], Iterable[str]], branch: str) -> List[str]:
        try:
            return list(load(branch))
        except Exception:
            # Files with a prefix missed here are read from origin, which only loses io-cache.
            logging.getLogger(__name__).warning(
                "Failed to load schema file prefixes of %s (branch %s) for io-cache routing",
                self._table_path, branch, exc_info=True)
            return []


def table_view_spec(table, table_path: str, file_prefixes: Iterable[str], branch: str):
    """(path, prefixes, branches) of a table view; a copy of the same table keeps those it had."""
    prefixes, branches = tuple(dict.fromkeys(file_prefixes)), (branch,)
    if table is not None and table[0] == table_path:
        prefixes = table[1] + tuple(p for p in prefixes if p not in table[1])
        branches = table[2] + tuple(b for b in branches if b not in table[2])
    return table_path, prefixes, branches


class _IoCacheRoutingHandler(FileSystemHandler):
    """Sends each pyarrow filesystem call to the filesystem of origin or of a target FileIO."""

    def __init__(self, file_io: IoCacheRoutingFileIO):
        self._routing_io = file_io

    def __eq__(self, other):
        if isinstance(other, _IoCacheRoutingHandler):
            return self._routing_io is other._routing_io
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, _IoCacheRoutingHandler):
            return self._routing_io is not other._routing_io
        return NotImplemented

    def get_type_name(self) -> str:
        return "io-cache-routing"

    def normalize_path(self, path: str) -> str:
        return path

    def register_file_size(self, path: str, file_size: int):
        file_io = self._routing_io._file_io(self._routing_io._route(Op.READ, path))
        handler = getattr(file_io.filesystem, "handler", None)
        register = getattr(handler, "register_file_size", None)
        if register is not None:
            register(file_io.to_filesystem_path(path), file_size)

    def get_file_info(self, paths) -> list:
        infos = self._routing_io._call_batch(
            Op.META, list(paths),
            lambda io, group: io.filesystem.get_file_info([io.to_filesystem_path(path) for path in group]))
        return [self._file_info(path, info) for path, info in zip(paths, infos)]

    @staticmethod
    def _file_info(path, info):
        return pafs.FileInfo(path, info.type, mtime_ns=info.mtime_ns, size=info.size)

    def open_input_file(self, path: str):
        return self._routing_io._call(
            Op.READ, path,
            lambda io: io.filesystem.open_input_file(io.to_filesystem_path(path)))

    def open_input_stream(self, path: str):
        # The outer PyFileSystem already applied the requested decompression.
        return self._routing_io._call(
            Op.READ, path,
            lambda io: io.filesystem.open_input_stream(io.to_filesystem_path(path), compression=None))

    def open_output_stream(self, path: str, metadata):
        return self._routing_io._call(
            Op.WRITE, path,
            lambda io: io.filesystem.open_output_stream(
                io.to_filesystem_path(path), compression=None, metadata=metadata))

    def open_append_stream(self, path: str, metadata):
        return self._routing_io._call(
            Op.WRITE, path,
            lambda io: io.filesystem.open_append_stream(
                io.to_filesystem_path(path), compression=None, metadata=metadata))

    def get_file_info_selector(self, selector) -> list:
        origin = self._routing_io._origin
        base = origin.to_filesystem_path(selector.base_dir).rstrip("/")
        infos = origin.filesystem.get_file_info(pafs.FileSelector(
            base,
            allow_not_found=selector.allow_not_found, recursive=selector.recursive))
        # Backend paths must stay in the full-URI namespace accepted by this filesystem.
        return [self._file_info(selector.base_dir.rstrip("/") + info.path[len(base):], info)
                for info in infos]

    def create_dir(self, path: str, recursive: bool):
        origin = self._routing_io._origin
        origin.filesystem.create_dir(origin.to_filesystem_path(path), recursive=recursive)

    def delete_dir(self, path: str):
        origin = self._routing_io._origin
        origin.filesystem.delete_dir(origin.to_filesystem_path(path))

    def delete_dir_contents(self, path: str, missing_dir_ok: bool = False):
        origin = self._routing_io._origin
        origin.filesystem.delete_dir_contents(
            origin.to_filesystem_path(path), missing_dir_ok=missing_dir_ok)

    def delete_root_dir_contents(self):
        raise NotImplementedError("io-cache routing filesystem has no root directory")

    def delete_file(self, path: str):
        origin = self._routing_io._origin
        origin.filesystem.delete_file(origin.to_filesystem_path(path))

    def move(self, src: str, dest: str):
        origin = self._routing_io._origin
        origin.filesystem.move(origin.to_filesystem_path(src), origin.to_filesystem_path(dest))

    def copy_file(self, src: str, dest: str):
        origin = self._routing_io._origin
        origin.filesystem.copy_file(origin.to_filesystem_path(src), origin.to_filesystem_path(dest))
