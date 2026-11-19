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

import logging
import threading
from typing import Dict, List
from urllib.parse import urlparse

import pyarrow.fs as pafs
from pyarrow._fs import FileSystemHandler

from pypaimon.common.file_io import FileIO
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions, OssOptions
from pypaimon.utils.file_type import FileType


_LOG = logging.getLogger(__name__)


class CachePolicyFileIO(FileIO):
    """Routes OSS operations to origin or cache endpoints by REST cache policy.

    The routing layer sits above ``PyArrowFileIO``. Both the pyjindo and native
    Arrow OSS implementations therefore use the same policy behavior.
    Cache uses ``fs.oss.endpoint``; bypassed operations fall back to
    ``io-cache.origin.endpoint`` when provided.
    """

    META_CACHE_ENABLED_TAG = "meta"
    READ_CACHE_ENABLED_TAG = "read"
    WRITE_CACHE_ENABLED_TAG = "write"
    DISABLE_CACHE_TAG = "none"

    @classmethod
    def create(cls, path: str, catalog_options: Options, file_io_factory=None) -> FileIO:
        """Create an origin FileIO, wrapping it only when cache policy is active."""
        if file_io_factory is None:
            file_io_factory = FileIO.get

        origin_options = cls._origin_options(catalog_options)
        origin_file_io = file_io_factory(path, origin_options)
        if not cls._cache_enabled(catalog_options):
            return origin_file_io

        cache_options = catalog_options.copy()
        return cls(
            path,
            catalog_options,
            origin_file_io,
            cache_options,
            file_io_factory,
        )

    @staticmethod
    def _origin_options(options: Options) -> Options:
        result = options.copy()
        endpoint = options.get(CatalogOptions.IO_CACHE_ORIGIN_ENDPOINT)
        if endpoint is not None and endpoint.strip():
            result.set(OssOptions.OSS_ENDPOINT, endpoint)
        return result

    @classmethod
    def _cache_enabled(cls, options: Options) -> bool:
        policy = options.get(CatalogOptions.IO_CACHE_POLICY)
        return (
            policy is not None
            and cls.DISABLE_CACHE_TAG not in policy
        )

    def __init__(
            self,
            path: str,
            catalog_options: Options,
            origin_file_io: FileIO,
            cache_options: Options,
            file_io_factory):
        self._path = path
        self.properties = catalog_options
        self._origin_file_io = origin_file_io
        self._cache_options = cache_options
        self._file_io_factory = file_io_factory
        self._cache_file_io = None
        self._cache_file_io_lock = threading.Lock()
        self._filesystem_path_prefix = self._infer_filesystem_path_prefix()

        policy = catalog_options.get(CatalogOptions.IO_CACHE_POLICY) or ""
        self.meta_cache_enabled = self.META_CACHE_ENABLED_TAG in policy
        self.read_cache_enabled = self.READ_CACHE_ENABLED_TAG in policy
        self.write_cache_enabled = self.WRITE_CACHE_ENABLED_TAG in policy

        whitelist = catalog_options.get(CatalogOptions.IO_CACHE_WHITELIST)
        if whitelist is not None and whitelist.strip() == "*":
            self._cache_whitelist = set(FileType)
        else:
            self._cache_whitelist = FileType.parse_whitelist(whitelist)

        whitelist_path = catalog_options.get(CatalogOptions.IO_CACHE_WHITELIST_PATH)
        self._cache_all_paths = (
            whitelist_path is not None and whitelist_path.strip() == "*")
        self._cache_whitelist_paths = []
        if whitelist_path is not None and not self._cache_all_paths:
            self._cache_whitelist_paths = [
                pattern.strip()
                for pattern in whitelist_path.split(",")
                if pattern.strip()
            ]

        self._initialize_filesystem()
        _LOG.info(
            "Cache enabled with cache policy: meta cache enabled %s, read cache "
            "enabled %s, write cache enabled %s, whitelist file types: %s, "
            "whitelist path: %s",
            self.meta_cache_enabled,
            self.read_cache_enabled,
            self.write_cache_enabled,
            self._cache_whitelist,
            whitelist_path,
        )

    def _initialize_filesystem(self):
        self._filesystem_handler = _CachePolicyFileSystemHandler(self)
        self.filesystem = pafs.PyFileSystem(self._filesystem_handler)

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop("_cache_file_io_lock", None)
        state.pop("_filesystem_handler", None)
        state.pop("filesystem", None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._cache_file_io_lock = threading.Lock()
        self._initialize_filesystem()

    def _get_cache_file_io(self) -> FileIO:
        if self._cache_file_io is None:
            with self._cache_file_io_lock:
                if self._cache_file_io is None:
                    self._cache_file_io = self._file_io_factory(
                        self._path, self._cache_options)
        return self._cache_file_io

    def _infer_filesystem_path_prefix(self) -> str:
        """Find the backend-only prefix, such as the bucket in Arrow S3 paths."""
        parsed = urlparse(self._path)
        if not parsed.scheme:
            return ""

        uri_path = parsed.path.lstrip("/")
        filesystem_path = self._origin_file_io.to_filesystem_path(
            self._path).lstrip("/")
        if uri_path and filesystem_path.endswith(uri_path):
            return filesystem_path[:-len(uri_path)].rstrip("/")
        if not uri_path and parsed.netloc and filesystem_path == parsed.netloc:
            return parsed.netloc
        return ""

    def _policy_path(self, path: str) -> str:
        parsed = urlparse(path)
        if parsed.scheme:
            return parsed.path or path

        normalized = path.lstrip("/")
        prefix = self._filesystem_path_prefix
        if prefix and normalized == prefix:
            normalized = ""
        elif prefix and normalized.startswith(prefix + "/"):
            normalized = normalized[len(prefix) + 1:]
        return "/" + normalized if normalized else "/"

    def should_cache(self, path: str) -> bool:
        policy_path = self._policy_path(path)
        if FileType.is_mutable(policy_path):
            return False
        if FileType.classify(policy_path) not in self._cache_whitelist:
            return False
        if self._cache_all_paths:
            return True

        return any(
            pattern in policy_path for pattern in self._cache_whitelist_paths)

    def _should_use_cache(self, path: str, operation: str) -> bool:
        enabled = (
            (operation == "meta" and self.meta_cache_enabled)
            or (operation == "read" and self.read_cache_enabled)
            or (operation == "write" and self.write_cache_enabled)
        )
        return enabled and self.should_cache(path)

    def _log_route(self, path: str, operation: str, use_cache: bool):
        options = (
            self._cache_options
            if use_cache else self._origin_file_io.properties)
        policy_path = self._policy_path(path)
        _LOG.debug(
            "CACHE_POLICY_ROUTE operation=%s route=%s endpoint=%s file_type=%s "
            "path=%s policy_path=%s",
            operation,
            "cache" if use_cache else "origin",
            options.get(OssOptions.OSS_ENDPOINT),
            FileType.classify(policy_path).value,
            path,
            policy_path,
        )

    def _file_io_for(self, path: str, operation: str) -> FileIO:
        use_cache = self._should_use_cache(path, operation)
        self._log_route(path, operation, use_cache)
        if use_cache:
            return self._get_cache_file_io()
        return self._origin_file_io

    def _origin_file_io_for(self, path: str, operation: str) -> FileIO:
        self._log_route(path, operation, False)
        return self._origin_file_io

    def new_input_stream(self, path: str):
        return self._file_io_for(path, "read").new_input_stream(path)

    def new_output_stream(self, path: str):
        return self._file_io_for(path, "write").new_output_stream(path)

    def get_file_status(self, path: str):
        return self._file_io_for(path, "meta").get_file_status(path)

    def list_status(self, path: str):
        return self._origin_file_io_for(path, "list").list_status(path)

    def exists(self, path: str) -> bool:
        return self._file_io_for(path, "meta").exists(path)

    def exists_batch(self, paths: List[str]) -> Dict[str, bool]:
        grouped = {}
        for path in paths:
            use_cache = self._should_use_cache(path, "meta")
            self._log_route(path, "meta", use_cache)
            grouped.setdefault(use_cache, []).append(path)

        result = {}
        for use_cache, grouped_paths in grouped.items():
            file_io = (
                self._get_cache_file_io()
                if use_cache else self._origin_file_io)
            result.update(file_io.exists_batch(grouped_paths))
        return result

    def delete(self, path: str, recursive: bool = False) -> bool:
        return self._origin_file_io_for(path, "delete").delete(path, recursive)

    def mkdirs(self, path: str) -> bool:
        return self._origin_file_io_for(path, "mkdirs").mkdirs(path)

    def rename(self, src: str, dst: str) -> bool:
        return self._origin_file_io_for(src, "rename").rename(src, dst)

    def get_file_size(self, path: str) -> int:
        return self._file_io_for(path, "meta").get_file_size(path)

    def is_dir(self, path: str) -> bool:
        return self._file_io_for(path, "meta").is_dir(path)

    def to_filesystem_path(self, path: str) -> str:
        return self._origin_file_io.to_filesystem_path(path)

    def try_to_write_atomic(self, path: str, content: str) -> bool:
        return self._origin_file_io_for(
            path, "atomic-write").try_to_write_atomic(path, content)

    def create_blob_presigned_url(self, table_root, descriptor, validity) -> str:
        return self._origin_file_io.create_blob_presigned_url(
            table_root, descriptor, validity)

    @property
    def uri_reader_factory(self):
        from pypaimon.common.uri_reader import UriReaderFactory
        return UriReaderFactory.from_file_io(self)

    def write_parquet(self, path: str, *args, **kwargs):
        return self._file_io_for(path, "write").write_parquet(path, *args, **kwargs)

    def write_orc(self, path: str, *args, **kwargs):
        return self._file_io_for(path, "write").write_orc(path, *args, **kwargs)

    def write_avro(self, path: str, *args, **kwargs):
        return self._file_io_for(path, "write").write_avro(path, *args, **kwargs)

    def write_lance(self, path: str, *args, **kwargs):
        return self._file_io_for(path, "write").write_lance(path, *args, **kwargs)

    def write_blob(self, path: str, *args, **kwargs):
        return self._file_io_for(path, "write").write_blob(path, *args, **kwargs)

    def write_mosaic(self, path: str, *args, **kwargs):
        return self._file_io_for(path, "write").write_mosaic(path, *args, **kwargs)

    def write_vortex(self, path: str, *args, **kwargs):
        return self._file_io_for(path, "write").write_vortex(path, *args, **kwargs)

    def write_row(self, path: str, *args, **kwargs):
        return self._file_io_for(path, "write").write_row(path, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._origin_file_io, name)

    def close(self):
        try:
            self._origin_file_io.close()
        finally:
            if self._cache_file_io is not None:
                self._cache_file_io.close()


class _CachePolicyFileSystemHandler(FileSystemHandler):
    """PyArrow filesystem view that applies the same operation-level policy."""

    def __init__(self, file_io: CachePolicyFileIO):
        self._file_io = file_io

    def _filesystem_for(self, path: str, operation: str):
        return self._file_io._file_io_for(path, operation).filesystem

    @property
    def _origin_filesystem(self):
        return self._file_io._origin_file_io.filesystem

    def _origin_filesystem_for(self, path: str, operation: str):
        self._file_io._log_route(path, operation, False)
        return self._origin_filesystem

    def get_type_name(self) -> str:
        return "cache-policy"

    def normalize_path(self, path: str) -> str:
        return self._origin_filesystem.normalize_path(path)

    def get_file_info(self, paths) -> list:
        paths = list(paths)
        result = [None] * len(paths)
        grouped = {}
        for index, path in enumerate(paths):
            use_cache = self._file_io._should_use_cache(path, "meta")
            self._file_io._log_route(path, "meta", use_cache)
            grouped.setdefault(use_cache, []).append((index, path))

        for use_cache, indexed_paths in grouped.items():
            filesystem = (
                self._file_io._get_cache_file_io().filesystem
                if use_cache else self._origin_filesystem)
            infos = filesystem.get_file_info(
                [path for _, path in indexed_paths])
            for (index, _), info in zip(indexed_paths, infos):
                result[index] = info
        return result

    def get_file_info_selector(self, selector: pafs.FileSelector) -> list:
        return self._origin_filesystem_for(
            selector.base_dir, "list").get_file_info(selector)

    def create_dir(self, path: str, recursive: bool):
        return self._origin_filesystem_for(
            path, "mkdirs").create_dir(path, recursive=recursive)

    def delete_dir(self, path: str):
        return self._origin_filesystem_for(path, "delete").delete_dir(path)

    def delete_dir_contents(self, path: str, missing_dir_ok: bool = False):
        return self._origin_filesystem_for(path, "delete").delete_dir_contents(
            path, missing_dir_ok=missing_dir_ok)

    def delete_root_dir_contents(self):
        return self._origin_filesystem_for(
            "/", "delete").delete_dir_contents(
            "/", accept_root_dir=True)

    def delete_file(self, path: str):
        return self._origin_filesystem_for(path, "delete").delete_file(path)

    def move(self, src: str, dest: str):
        return self._origin_filesystem_for(src, "rename").move(src, dest)

    def copy_file(self, src: str, dest: str):
        return self._origin_filesystem_for(src, "copy").copy_file(src, dest)

    def open_input_stream(self, path: str):
        return self._filesystem_for(path, "read").open_input_stream(path)

    def open_input_file(self, path: str):
        return self._filesystem_for(path, "read").open_input_file(path)

    def open_output_stream(self, path: str, metadata):
        filesystem = self._filesystem_for(path, "write")
        if metadata is None:
            return filesystem.open_output_stream(path)
        return filesystem.open_output_stream(path, metadata=metadata)

    def open_append_stream(self, path: str, metadata):
        filesystem = self._filesystem_for(path, "write")
        if metadata is None:
            return filesystem.open_append_stream(path)
        return filesystem.open_append_stream(path, metadata=metadata)
