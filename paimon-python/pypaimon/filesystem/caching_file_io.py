################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
################################################################################

"""Block-level local cache for Paimon files.

Provides a CachingFileIO wrapper that transparently caches remote file reads
at block granularity. If a cache directory is configured, disk cache is used;
otherwise an in-memory LRU cache is used. Files are classified by FileType and
only cacheable types in the whitelist are cached; others are read directly from
the delegate FileIO.
"""

import hashlib
import os
import threading
from collections import OrderedDict
from typing import Optional

from pypaimon.common.file_io import FileIO
from pypaimon.utils.file_type import FileType


class LocalMemoryCacheManager:
    """Block-level in-memory cache with LRU eviction."""

    def __init__(self, max_size_bytes: int, block_size: int = 1 * 1024 * 1024):
        self._max_size_bytes = max_size_bytes
        self._block_size = block_size
        self._lock = threading.Lock()
        self._current_size = 0
        self._cache: OrderedDict = OrderedDict()
        self._file_size_cache: dict = {}

    @property
    def block_size(self) -> int:
        return self._block_size

    def get_block(self, file_path: str, block_index: int) -> Optional[bytes]:
        key = (file_path, block_index)
        with self._lock:
            data = self._cache.get(key)
            if data is not None:
                self._cache.move_to_end(key)
            return data

    def put_block(self, file_path: str, block_index: int, data: bytes) -> None:
        key = (file_path, block_index)
        with self._lock:
            if key in self._cache:
                return
            self._current_size += len(data)
            self._cache[key] = data
            while (self._max_size_bytes < (2 ** 63 - 1)
                   and self._current_size > self._max_size_bytes
                   and self._cache):
                _, evicted = self._cache.popitem(last=False)
                self._current_size -= len(evicted)

    def get_file_size(self, file_path: str) -> int:
        return self._file_size_cache.get(file_path, -1)

    def put_file_size(self, file_path: str, size: int) -> None:
        self._file_size_cache[file_path] = size


class LocalDiskCacheManager:
    """Block-level local disk cache with LRU eviction."""

    def __init__(self, cache_dir: str, max_size_bytes: int,
                 block_size: int = 1 * 1024 * 1024):
        self._cache_dir = cache_dir
        self._max_size_bytes = max_size_bytes
        self._block_size = block_size
        self._lock = threading.Lock()
        self._current_size = 0
        self._file_size_cache: dict = {}
        # LRU-ordered index: cache_path -> size. OrderedDict with move_to_end for access order.
        self._entry_index: OrderedDict = OrderedDict()
        os.makedirs(cache_dir, exist_ok=True)
        self._current_size = self._scan_and_populate_index()

    @property
    def block_size(self) -> int:
        return self._block_size

    def _cache_path(self, file_path: str, block_index: int) -> str:
        key = f"{file_path}:{block_index}"
        h = hashlib.sha256(key.encode('utf-8')).hexdigest()
        prefix = h[:2]
        sub_dir = os.path.join(self._cache_dir, prefix)
        return os.path.join(sub_dir, h)

    def get_block(self, file_path: str, block_index: int) -> Optional[bytes]:
        path = self._cache_path(file_path, block_index)
        with self._lock:
            if path not in self._entry_index:
                return None
            self._entry_index.move_to_end(path)
        try:
            with open(path, 'rb') as f:
                return f.read()
        except (FileNotFoundError, OSError):
            with self._lock:
                size = self._entry_index.pop(path, None)
                if size is not None:
                    self._current_size -= size
            return None

    def put_block(self, file_path: str, block_index: int, data: bytes) -> None:
        path = self._cache_path(file_path, block_index)

        with self._lock:
            if path in self._entry_index:
                return

        sub_dir = os.path.dirname(path)
        os.makedirs(sub_dir, exist_ok=True)

        tmp_path = path + f".tmp.{os.getpid()}.{threading.get_ident()}"
        try:
            with open(tmp_path, 'wb') as f:
                f.write(data)
            os.rename(tmp_path, path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            return

        need_evict = False
        with self._lock:
            self._entry_index[path] = len(data)
            self._current_size += len(data)
            need_evict = (self._max_size_bytes < (2 ** 63 - 1)
                          and self._current_size > self._max_size_bytes)
        if need_evict:
            self._evict()

    def _evict(self) -> None:
        to_delete = []
        with self._lock:
            if self._current_size <= self._max_size_bytes:
                return
            while self._entry_index and self._current_size > self._max_size_bytes:
                path, size = self._entry_index.popitem(last=False)
                self._current_size -= size
                to_delete.append((path, size))

        for path, size in to_delete:
            try:
                os.unlink(path)
            except OSError:
                with self._lock:
                    self._entry_index[path] = size
                    self._current_size += size

    def _scan_and_populate_index(self) -> int:
        total = 0
        for dirpath, _, filenames in os.walk(self._cache_dir):
            for fn in filenames:
                if '.tmp.' in fn:
                    continue
                fp = os.path.join(dirpath, fn)
                try:
                    size = os.path.getsize(fp)
                    self._entry_index[fp] = size
                    total += size
                except OSError:
                    pass
        return total

    def get_file_size(self, file_path: str) -> int:
        return self._file_size_cache.get(file_path, -1)

    def put_file_size(self, file_path: str, size: int) -> None:
        self._file_size_cache[file_path] = size


class CachingInputStream:
    """Wraps a remote stream with block-level caching."""

    def __init__(self, file_io, file_path: str, cache):
        self._file_io = file_io
        self._stream = None
        self._file_path = file_path
        self._file_size = -1
        self._cache = cache
        self._pos = 0

    def _get_file_size(self) -> int:
        if self._file_size == -1:
            cached = self._cache.get_file_size(self._file_path)
            if cached >= 0:
                self._file_size = cached
            else:
                self._file_size = self._file_io.get_file_size(self._file_path)
                self._cache.put_file_size(self._file_path, self._file_size)
        return self._file_size

    def seek(self, offset, whence=0):
        if whence == 0:
            self._pos = max(0, offset)
        elif whence == 1:
            self._pos = max(0, self._pos + offset)
        elif whence == 2:
            self._pos = max(0, self._get_file_size() + offset)
        return self._pos

    def tell(self) -> int:
        return self._pos

    def read(self, size=-1) -> bytes:
        if size == -1 or size is None:
            size = self._get_file_size() - self._pos
        if size <= 0 or self._pos >= self._get_file_size():
            return b''

        end = min(self._pos + size, self._get_file_size())
        block_size = self._cache.block_size

        first_block = self._pos // block_size
        last_block = (end - 1) // block_size

        result = bytearray()
        for bi in range(first_block, last_block + 1):
            block_data = self._read_block(bi)

            block_start = bi * block_size
            start_in_block = max(self._pos - block_start, 0)
            end_in_block = min(end - block_start, len(block_data))
            result.extend(block_data[start_in_block:end_in_block])

        self._pos = end
        return bytes(result)

    def _read_block(self, block_index: int) -> bytes:
        cached = self._cache.get_block(self._file_path, block_index)
        if cached is not None:
            return cached

        block_size = self._cache.block_size
        offset = block_index * block_size
        read_size = min(block_size, self._get_file_size() - offset)

        stream = self._get_remote_stream()
        stream.seek(offset)
        data = self._read_fully(stream, read_size)

        self._cache.put_block(self._file_path, block_index, data)
        return data

    def _read_fully(self, stream, size: int) -> bytes:
        buf = bytearray()
        remaining = size
        while remaining > 0:
            chunk = stream.read(remaining)
            if not chunk:
                break
            buf.extend(chunk)
            remaining -= len(chunk)
        return bytes(buf)

    def _get_remote_stream(self):
        if self._stream is None:
            self._stream = self._file_io.new_input_stream(self._file_path)
        return self._stream

    def close(self):
        if self._stream is not None:
            self._stream.close()
            self._stream = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


class CachingFileIO(FileIO):
    """FileIO wrapper that caches reads at block granularity.

    Only file types in the whitelist are cached. Others are read directly
    from the delegate. After pickling/unpickling, the cache is None and reads
    fall through to the delegate directly.
    """

    def __init__(self, delegate: FileIO, cache, whitelist=None):
        self._delegate = delegate
        self._cache = cache
        if whitelist is None:
            self._whitelist = {FileType.META, FileType.GLOBAL_INDEX}
        else:
            self._whitelist = whitelist

    @staticmethod
    def create_cache_manager(options):
        """Creates a cache manager from options, or returns None if caching is not enabled."""
        from pypaimon.common.options.core_options import CoreOptions
        opts = CoreOptions(options)
        if not opts.local_cache_enabled():
            return None
        cache_dir = opts.local_cache_dir()
        max_size_opt = opts.local_cache_max_size()
        max_size = max_size_opt.get_bytes() if max_size_opt is not None else (2 ** 63 - 1)
        block_size = opts.local_cache_block_size().get_bytes()
        if cache_dir is not None:
            return LocalDiskCacheManager(cache_dir, max_size, block_size)
        else:
            return LocalMemoryCacheManager(max_size, block_size)

    @staticmethod
    def wrap_with_caching_if_needed(file_io, options, cache=None):
        """Wraps the given FileIO with caching if local cache is enabled.

        Args:
            file_io: the FileIO to potentially wrap
            options: an Options object containing cache configuration
            cache: the cache manager instance (managed by the caller)

        Returns:
            a CachingFileIO if caching is enabled and configured, otherwise the original FileIO
        """
        if isinstance(file_io, CachingFileIO):
            return file_io
        if cache is None:
            return file_io
        from pypaimon.common.options.core_options import CoreOptions
        opts = CoreOptions(options)
        whitelist = FileType.parse_whitelist(opts.local_cache_whitelist())
        if not whitelist:
            return file_io
        return CachingFileIO(file_io, cache, whitelist)

    def new_input_stream(self, path: str):
        file_type = FileType.classify(path)
        if self._cache is None or file_type not in self._whitelist or FileType.is_mutable(path):
            return self._delegate.new_input_stream(path)
        return CachingInputStream(self._delegate, path, self._cache)

    def new_output_stream(self, path: str):
        return self._delegate.new_output_stream(path)

    def get_file_status(self, path: str):
        return self._delegate.get_file_status(path)

    def list_status(self, path: str):
        return self._delegate.list_status(path)

    def exists(self, path: str) -> bool:
        return self._delegate.exists(path)

    def delete(self, path: str, recursive: bool = False) -> bool:
        return self._delegate.delete(path, recursive)

    def mkdirs(self, path: str) -> bool:
        return self._delegate.mkdirs(path)

    def rename(self, src: str, dst: str) -> bool:
        return self._delegate.rename(src, dst)

    def get_file_size(self, path: str) -> int:
        return self._delegate.get_file_size(path)

    def is_dir(self, path: str) -> bool:
        return self._delegate.is_dir(path)

    def __getattr__(self, name):
        return getattr(self._delegate, name)

    def close(self):
        self._delegate.close()
