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

import sys
from collections import namedtuple
from threading import Lock

from cachetools import LRUCache

from pypaimon.common.memory_size import MemorySize


_CachedBlobIndex = namedtuple(
    "_CachedBlobIndex", ["blob_lengths", "blob_offsets", "size_bytes"]
)
_CACHED_ENTRY_OVERHEAD = sys.getsizeof(_CachedBlobIndex((), (), 0))
_ESTIMATED_INT_SIZE = sys.getsizeof(1 << 60)
# Allow for cachetools' two dict slots, OrderedDict node, and allocator slack.
_LRU_ENTRY_OVERHEAD = 256


class BlobIndexCache:
    """Catalog-owned parsed indexes bounded by estimated retained bytes."""

    def __init__(self, max_size):
        if not isinstance(max_size, MemorySize):
            raise ValueError("cache.blob-index.max-size must be a memory size")
        self.max_size_bytes = max_size.get_bytes()
        self._cache = LRUCache(
            maxsize=self.max_size_bytes,
            getsizeof=lambda entry: entry.size_bytes,
        )
        self._lock = Lock()

    def get(self, file_path):
        if self.max_size_bytes == 0:
            return None
        with self._lock:
            entry = self._cache.get(file_path)
        if entry is None:
            return None
        return entry.blob_lengths, entry.blob_offsets

    def put(self, file_path, blob_lengths, blob_offsets):
        if self.max_size_bytes == 0:
            return
        size_bytes = (
            sys.getsizeof(file_path)
            + sys.getsizeof(blob_lengths)
            + sys.getsizeof(blob_offsets)
            + _CACHED_ENTRY_OVERHEAD
            + _LRU_ENTRY_OVERHEAD
            + (len(blob_lengths) + len(blob_offsets)) * _ESTIMATED_INT_SIZE
        )
        if size_bytes > self.max_size_bytes:
            return
        entry = _CachedBlobIndex(blob_lengths, blob_offsets, size_bytes)
        with self._lock:
            self._cache[file_path] = entry

    def __len__(self):
        with self._lock:
            return len(self._cache)

    def __contains__(self, file_path):
        with self._lock:
            return file_path in self._cache

    def clear(self):
        with self._lock:
            self._cache.clear()

    @property
    def size_bytes(self):
        with self._lock:
            return self._cache.currsize

    def __reduce__(self):
        return type(self), (MemorySize.of_bytes(self.max_size_bytes),)
