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

import errno
import logging
import os
import socket
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


def _is_connection_failure(error):
    """Recognize transport diagnostics, including SDKs which lose socket causes."""
    visited = set()
    pending = [error]
    connected = False
    while pending:
        current = pending.pop()
        if current is None or id(current) in visited:
            continue
        visited.add(id(current))
        if isinstance(current, (FileNotFoundError, FileExistsError, PermissionError,
                                InterruptedError)):
            return False
        if isinstance(current, (ConnectionError, TimeoutError, socket.gaierror)):
            connected = True
        if isinstance(current, OSError):
            connected |= current.errno in (
                errno.ECONNREFUSED, errno.ECONNRESET, errno.ECONNABORTED,
                errno.ENETUNREACH, errno.EHOSTUNREACH, errno.ETIMEDOUT, errno.EPIPE)
            # pyjindo stores the native diagnostic as bytes in args[0].
            message = current.args[0] if current.args else ""
            if isinstance(message, bytes):
                message = message.decode("utf-8", errors="replace")
            if isinstance(message, str):
                connected |= (
                    "Failed to connect Http service:" in message
                    or ("AWS Error NETWORK_CONNECTION" in message
                        and "curlCode:" in message))
        pending.extend((current.__cause__, current.__context__))
    return connected


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
        if (not cls._cache_enabled(catalog_options)
                or catalog_options.get(OssOptions.OSS_ENDPOINT) == origin_options.get(OssOptions.OSS_ENDPOINT)):
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
        self._origin_delegate = origin_file_io
        self._process_id = os.getpid()
        self._cache_options = cache_options
        self._file_io_factory = file_io_factory
        self._cache_file_io = None
        self._cache_unavailable = False
        self._cache_file_io_lock = threading.RLock()
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
        self._process_id = os.getpid()
        self._cache_file_io_lock = threading.RLock()
        self._initialize_filesystem()

    def _ensure_process(self):
        if self._process_id != os.getpid():
            # A fork does not run __setstate__. The thread holding the inherited
            # lock may not exist in the child; rebuild its backends lazily too.
            self._cache_file_io_lock = threading.RLock()
            self._cache_file_io = None
            self._origin_delegate = None
            self._process_id = os.getpid()

    @property
    def _origin_file_io(self):
        self._ensure_process()
        if self._origin_delegate is None:
            with self._cache_file_io_lock:
                if self._origin_delegate is None:
                    self._origin_delegate = self._file_io_factory(
                        self._path, self._origin_options(self.properties))
        return self._origin_delegate

    def _get_cache_file_io(self, path, operation) -> FileIO:
        self._ensure_process()
        if self._cache_file_io is None:
            with self._cache_file_io_lock:
                if self._cache_unavailable:
                    return self._origin_file_io
                if self._cache_file_io is None:
                    candidate = None
                    try:
                        candidate = self._file_io_factory(self._path, self._cache_options)
                        # Native writers may defer the connection until close. Probe the
                        # first allowed path before handing out a stream or filesystem.
                        # False is valid for a file that has not been created yet.
                        candidate.exists(path)
                    except Exception as error:
                        if candidate is not None:
                            try:
                                candidate.close()
                            except Exception:
                                pass
                        if not _is_connection_failure(error):
                            raise
                        self._disable_cache(path, operation, error)
                        return self._origin_file_io
                    self._cache_file_io = candidate
        return self._cache_file_io

    def _disable_cache(self, path, operation, error):
        self._ensure_process()
        with self._cache_file_io_lock:
            if not self._cache_unavailable:
                self._cache_unavailable = True
                _LOG.warning(
                    "CACHE_POLICY_FALLBACK operation=%s cache_endpoint=%s "
                    "origin_endpoint=%s path=%s failure_type=%s; "
                    "using origin for subsequent operations on this FileIO",
                    operation, self._cache_options.get(OssOptions.OSS_ENDPOINT),
                    self._origin_file_io.properties.get(OssOptions.OSS_ENDPOINT),
                    path, type(error).__name__,
                    exc_info=(type(error), error, error.__traceback__))

    def _execute(self, path, operation, action, retry=True):
        delegate = self._file_io_for(path, operation)
        try:
            return action(delegate)
        except Exception as error:
            if delegate is self._origin_file_io or not _is_connection_failure(error):
                raise
            self._disable_cache(path, operation, error)
            if not retry:
                # A format writer may have consumed an iterator or persisted partial data.
                raise
            # Python retains the cache failure as context if origin also fails.
            return action(self._origin_file_io)

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
        return not self._cache_unavailable and enabled and self.should_cache(path)

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
        self._ensure_process()
        use_cache = self._should_use_cache(path, operation)
        self._log_route(path, operation, use_cache)
        if use_cache:
            return self._get_cache_file_io(path, operation)
        return self._origin_file_io

    def _origin_file_io_for(self, path: str, operation: str) -> FileIO:
        self._log_route(path, operation, False)
        return self._origin_file_io

    def new_input_stream(self, path: str):
        return self._execute(path, "read", lambda io: io.new_input_stream(path))

    def new_output_stream(self, path: str):
        return self._execute(path, "write", lambda io: io.new_output_stream(path))

    def get_file_status(self, path: str):
        return self._execute(path, "meta", lambda io: io.get_file_status(path))

    def list_status(self, path: str):
        return self._origin_file_io_for(path, "list").list_status(path)

    def exists(self, path: str) -> bool:
        return self._execute(path, "meta", lambda io: io.exists(path))

    def exists_batch(self, paths: List[str]) -> Dict[str, bool]:
        grouped = {}
        for path in paths:
            use_cache = self._should_use_cache(path, "meta")
            self._log_route(path, "meta", use_cache)
            grouped.setdefault(use_cache, []).append(path)

        result = {}
        for use_cache, grouped_paths in grouped.items():
            if use_cache:
                values = self._execute(
                    grouped_paths[0], "meta", lambda io: io.exists_batch(grouped_paths))
            else:
                values = self._origin_file_io.exists_batch(grouped_paths)
            result.update(values)
        return result

    def delete(self, path: str, recursive: bool = False) -> bool:
        return self._origin_file_io_for(path, "delete").delete(path, recursive)

    def mkdirs(self, path: str) -> bool:
        return self._origin_file_io_for(path, "mkdirs").mkdirs(path)

    def rename(self, src: str, dst: str) -> bool:
        return self._origin_file_io_for(src, "rename").rename(src, dst)

    def get_file_size(self, path: str) -> int:
        return self._execute(path, "meta", lambda io: io.get_file_size(path))

    def is_dir(self, path: str) -> bool:
        return self._execute(path, "meta", lambda io: io.is_dir(path))

    def to_filesystem_path(self, path: str) -> str:
        # The policy filesystem needs the URI scheme/authority to resolve each
        # routed delegate, which may use a different physical path convention.
        return path

    def for_path(self, path: str, operation: str):
        return self._file_io_for(path, operation).for_path(path, operation)

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
        return self._execute(path, "write", lambda io: io.write_parquet(path, *args, **kwargs), retry=False)

    def write_orc(self, path: str, *args, **kwargs):
        return self._execute(path, "write", lambda io: io.write_orc(path, *args, **kwargs), retry=False)

    def write_avro(self, path: str, *args, **kwargs):
        return self._execute(path, "write", lambda io: io.write_avro(path, *args, **kwargs), retry=False)

    def write_lance(self, path: str, *args, **kwargs):
        return self._execute(path, "write", lambda io: io.write_lance(path, *args, **kwargs), retry=False)

    def write_blob(self, path: str, *args, **kwargs):
        return self._execute(path, "write", lambda io: io.write_blob(path, *args, **kwargs), retry=False)

    def write_mosaic(self, path: str, *args, **kwargs):
        return self._execute(path, "write", lambda io: io.write_mosaic(path, *args, **kwargs), retry=False)

    def write_vortex(self, path: str, *args, **kwargs):
        return self._execute(path, "write", lambda io: io.write_vortex(path, *args, **kwargs), retry=False)

    def write_row(self, path: str, *args, **kwargs):
        return self._execute(path, "write", lambda io: io.write_row(path, *args, **kwargs), retry=False)

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

    def _execute(self, path, operation, action):
        def apply(io):
            backend = io.for_path(path, operation)
            return action(backend.filesystem, backend.to_filesystem_path(path))
        return self._file_io._execute(path, operation, apply)

    def _origin_execute(self, path, operation, action):
        origin = self._file_io._origin_file_io_for(path, operation).for_path(path, operation)
        return action(origin.filesystem, origin.to_filesystem_path(path))

    @staticmethod
    def _logical_info(info, path):
        if isinstance(info, pafs.FileInfo):
            return pafs.FileInfo(path, info.type, mtime_ns=info.mtime_ns, size=info.size)
        return info

    def get_type_name(self) -> str:
        return "cache-policy"

    def normalize_path(self, path: str) -> str:
        return path

    def get_file_info(self, paths) -> list:
        paths = list(paths)
        result = [None] * len(paths)
        grouped = {}
        for index, path in enumerate(paths):
            use_cache = self._file_io._should_use_cache(path, "meta")
            self._file_io._log_route(path, "meta", use_cache)
            parsed = urlparse(path)
            grouped.setdefault((use_cache, parsed.scheme, parsed.netloc), []).append((index, path))

        for (use_cache, _, _), indexed_paths in grouped.items():
            grouped_paths = [path for _, path in indexed_paths]

            def get_infos(io):
                backend = io.for_path(grouped_paths[0], "meta")
                return backend.filesystem.get_file_info(
                    [backend.to_filesystem_path(path) for path in grouped_paths])
            if use_cache:
                infos = self._file_io._execute(grouped_paths[0], "meta", get_infos)
            else:
                infos = get_infos(self._file_io._origin_file_io)
            for (index, path), info in zip(indexed_paths, infos):
                result[index] = self._logical_info(info, path)
        return result

    def get_file_info_selector(self, selector: pafs.FileSelector) -> list:
        def get_infos(fs, backend_path):
            backend_selector = pafs.FileSelector(
                backend_path, recursive=selector.recursive, allow_not_found=selector.allow_not_found)
            infos = fs.get_file_info(backend_selector)
            return [self._logical_info(
                info, selector.base_dir.rstrip("/") + "/" + info.path[len(backend_path.rstrip("/")):].lstrip("/"))
                for info in infos]
        return self._origin_execute(selector.base_dir, "list", get_infos)

    def create_dir(self, path: str, recursive: bool):
        return self._origin_execute(path, "mkdirs", lambda fs, p: fs.create_dir(p, recursive=recursive))

    def delete_dir(self, path: str):
        return self._origin_execute(path, "delete", lambda fs, p: fs.delete_dir(p))

    def delete_dir_contents(self, path: str, missing_dir_ok: bool = False):
        return self._origin_execute(
            path, "delete", lambda fs, p: fs.delete_dir_contents(p, missing_dir_ok=missing_dir_ok))

    def delete_root_dir_contents(self):
        return self._origin_execute(
            self._file_io._path, "delete", lambda fs, p: fs.delete_dir_contents("/", accept_root_dir=True))

    def delete_file(self, path: str):
        return self._origin_execute(path, "delete", lambda fs, p: fs.delete_file(p))

    def move(self, src: str, dest: str):
        origin = self._file_io._origin_file_io_for(src, "rename").for_path(src, "rename")
        return origin.filesystem.move(origin.to_filesystem_path(src), origin.to_filesystem_path(dest))

    def copy_file(self, src: str, dest: str):
        origin = self._file_io._origin_file_io_for(src, "copy").for_path(src, "copy")
        return origin.filesystem.copy_file(origin.to_filesystem_path(src), origin.to_filesystem_path(dest))

    def open_input_stream(self, path: str):
        return self._execute(path, "read", lambda fs, p: fs.open_input_stream(p))

    def open_input_file(self, path: str):
        return self._execute(path, "read", lambda fs, p: fs.open_input_file(p))

    def open_output_stream(self, path: str, metadata):
        kwargs = {} if metadata is None else {"metadata": metadata}
        return self._execute(path, "write", lambda fs, p: fs.open_output_stream(p, **kwargs))

    def open_append_stream(self, path: str, metadata):
        kwargs = {} if metadata is None else {"metadata": metadata}
        return self._execute(path, "write", lambda fs, p: fs.open_append_stream(p, **kwargs))
