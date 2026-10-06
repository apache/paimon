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

import threading
from typing import Callable, Dict, List, Optional

import pyarrow.fs as pafs
from pyarrow._fs import FileSystemHandler

from pypaimon.common.file_io import FileIO
from pypaimon.filesystem.io_cache_routing import IoCacheRouting, Op


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
        self._filesystem = pafs.PyFileSystem(_IoCacheRoutingHandler(self))

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
        file_io = self._targets.get(name)
        if file_io is not None:
            return file_io
        with self._targets_lock:
            file_io = self._targets.get(name)
            if file_io is None:
                file_io = self._create_target(name)
                self._targets[name] = file_io
            return file_io

    def _call(self, op: Op, path: str, action):
        return action(self._file_io(self._route(op, path)))

    def read_file_io(self, path: str) -> FileIO:
        """FileIO for native readers that open files using their own storage client."""
        return self._file_io(self._route(Op.READ, path))

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
        return self._origin.new_output_stream(path)

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

    # Helpers that write, delete or check right after a delete run on origin, probes included.
    def write_file(self, path: str, content: str, overwrite: bool = False):
        return self._origin.write_file(path, content, overwrite)

    def overwrite_file_utf8(self, path: str, content: str):
        return self._origin.overwrite_file_utf8(path, content)

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
        return self._origin.write_parquet(path, data, compression, zstd_level, **kwargs)

    def write_orc(self, path: str, data, compression: str = 'zstd',
                  zstd_level: int = 1, **kwargs):
        return self._origin.write_orc(path, data, compression, zstd_level, **kwargs)

    def write_avro(self, path: str, data, avro_schema=None,
                   compression: str = 'zstd', zstd_level: int = 1, **kwargs):
        return self._origin.write_avro(path, data, avro_schema, compression, zstd_level, **kwargs)

    def write_lance(self, path: str, data, **kwargs):
        return self._origin.write_lance(path, data, **kwargs)

    def write_blob(self, path: str, data, **kwargs):
        return self._origin.write_blob(path, data, **kwargs)

    def write_vortex(self, path: str, data, **kwargs):
        return self._origin.write_vortex(path, data, **kwargs)

    def write_row(self, path: str, data, fields=None, zstd_level: int = 1, **kwargs):
        return self._origin.write_row(path, data, fields, zstd_level, **kwargs)

    def close(self):
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
        origin = self._routing_io._origin
        return origin.filesystem.open_output_stream(
            origin.to_filesystem_path(path), compression=None, metadata=metadata)

    def open_append_stream(self, path: str, metadata):
        origin = self._routing_io._origin
        return origin.filesystem.open_append_stream(
            origin.to_filesystem_path(path), compression=None, metadata=metadata)

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
