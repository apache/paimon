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

import bisect
import ctypes
import random
import struct
from abc import ABC, abstractmethod
from typing import List


class ExternalPathProvider(ABC):
    """Provider for external data paths."""

    @abstractmethod
    def get_next_external_data_path(self, file_name: str) -> str:
        """Get the next external data path for the given file name."""

    @staticmethod
    def create(strategy, external_table_paths, relative_bucket_path="", weights=None):
        """Factory method to create the appropriate ExternalPathProvider."""
        from pypaimon.common.options.core_options import ExternalPathStrategy

        if strategy is None:
            return None
        if isinstance(strategy, str):
            strategy = ExternalPathStrategy(strategy)

        if strategy == ExternalPathStrategy.NONE:
            return None
        elif strategy in (ExternalPathStrategy.ROUND_ROBIN, ExternalPathStrategy.SPECIFIC_FS):
            return RoundRobinExternalPathProvider(external_table_paths, relative_bucket_path)
        elif strategy == ExternalPathStrategy.ENTROPY_INJECT:
            return EntropyInjectExternalPathProvider(external_table_paths, relative_bucket_path)
        elif strategy == ExternalPathStrategy.WEIGHTED:
            if len(external_table_paths) < 2 or not weights:
                return RoundRobinExternalPathProvider(external_table_paths, relative_bucket_path)
            return WeightedExternalPathProvider(external_table_paths, relative_bucket_path, weights)
        else:
            raise ValueError(f"Unsupported external path strategy: {strategy}")


class RoundRobinExternalPathProvider(ExternalPathProvider):
    """Provider for round-robin external data paths."""

    def __init__(self, external_table_paths: List[str], relative_bucket_path: str = ""):
        if not external_table_paths:
            raise ValueError("external_table_paths must not be empty")
        self._external_table_paths = external_table_paths
        self._relative_bucket_path = relative_bucket_path
        self._position = random.randint(0, len(external_table_paths) - 1)

    def get_next_external_data_path(self, file_name: str) -> str:
        self._position += 1
        if self._position == len(self._external_table_paths):
            self._position = 0

        external_base = self._external_table_paths[self._position]
        if self._relative_bucket_path:
            return f"{external_base.rstrip('/')}/{self._relative_bucket_path.strip('/')}/{file_name}"
        else:
            return f"{external_base.rstrip('/')}/{file_name}"


class EntropyInjectExternalPathProvider(ExternalPathProvider):
    """Provider for entropy-injected external data paths.

    Generates hash-based directory structures from filenames using murmur3_32.
    Constants: 20-bit hash, depth=3 dirs of 4 bits each, 8-bit remainder.
    """

    _HASH_BINARY_STRING_BITS = 20
    _ENTROPY_DIR_LENGTH = 4
    _ENTROPY_DIR_DEPTH = 3

    def __init__(self, external_table_paths: List[str], relative_bucket_path: str = ""):
        if not external_table_paths:
            raise ValueError("external_table_paths must not be empty")
        self._external_table_paths = external_table_paths
        self._relative_bucket_path = relative_bucket_path
        self._position = 0

    def get_next_external_data_path(self, file_name: str) -> str:
        hash_dirs = self._compute_hash(file_name)
        if self._relative_bucket_path:
            file_path_with_hash = f"{self._relative_bucket_path.strip('/')}/{hash_dirs}/{file_name}"
        else:
            file_path_with_hash = f"{hash_dirs}/{file_name}"

        self._position += 1
        if self._position == len(self._external_table_paths):
            self._position = 0

        external_base = self._external_table_paths[self._position]
        return f"{external_base.rstrip('/')}/{file_path_with_hash}"

    def _compute_hash(self, file_name: str) -> str:
        hash_int = _murmur3_32(file_name.encode('utf-8'))
        binary_string = format((hash_int & 0xFFFFFFFF) | 0x80000000, '032b')
        hash_str = binary_string[32 - self._HASH_BINARY_STRING_BITS:]

        parts = []
        total_prefix = self._ENTROPY_DIR_DEPTH * self._ENTROPY_DIR_LENGTH
        for i in range(0, total_prefix, self._ENTROPY_DIR_LENGTH):
            end = min(i + self._ENTROPY_DIR_LENGTH, len(hash_str))
            parts.append(hash_str[i:end])
        if len(hash_str) > total_prefix:
            parts.append(hash_str[total_prefix:])
        return "/".join(parts)


class WeightedExternalPathProvider(ExternalPathProvider):
    """Provider for weighted external data paths.

    Uses cumulative weights with binary search for path selection.
    """

    def __init__(self, external_table_paths: List[str], relative_bucket_path: str, weights: List[int]):
        if len(external_table_paths) != len(weights):
            raise ValueError(
                f"The number of external paths and weights should be the same. "
                f"Paths: {len(external_table_paths)}, Weights: {len(weights)}"
            )
        self._external_table_paths = external_table_paths
        self._relative_bucket_path = relative_bucket_path
        self._total_weight = sum(weights)
        self._cumulative_weights: List[int] = []
        cumulative = 0
        for w in weights:
            cumulative += w
            self._cumulative_weights.append(cumulative)

    def get_next_external_data_path(self, file_name: str) -> str:
        random_value = random.random() * self._total_weight
        index = bisect.bisect_right(self._cumulative_weights, random_value)
        if index >= len(self._external_table_paths):
            index = len(self._external_table_paths) - 1
        selected_base = self._external_table_paths[index]
        if self._relative_bucket_path:
            return f"{selected_base.rstrip('/')}/{self._relative_bucket_path.strip('/')}/{file_name}"
        else:
            return f"{selected_base.rstrip('/')}/{file_name}"


def _murmur3_32(data: bytes, seed: int = 0) -> int:
    """Pure-Python murmur3_32 hash, compatible with Guava Hashing.murmur3_32().

    Returns a signed 32-bit integer identical to Java's int representation.
    """
    c1 = 0xCC9E2D51
    c2 = 0x1B873593
    length = len(data)
    h1 = seed & 0xFFFFFFFF
    rounded_end = (length & 0xFFFFFFFC)

    for i in range(0, rounded_end, 4):
        k1 = struct.unpack_from('<I', data, i)[0]
        k1 = (k1 * c1) & 0xFFFFFFFF
        k1 = ((k1 << 15) | (k1 >> 17)) & 0xFFFFFFFF
        k1 = (k1 * c2) & 0xFFFFFFFF
        h1 ^= k1
        h1 = ((h1 << 13) | (h1 >> 19)) & 0xFFFFFFFF
        h1 = (h1 * 5 + 0xE6546B64) & 0xFFFFFFFF

    k1 = 0
    remaining = length & 3
    if remaining >= 3:
        k1 ^= data[rounded_end + 2] << 16
    if remaining >= 2:
        k1 ^= data[rounded_end + 1] << 8
    if remaining >= 1:
        k1 ^= data[rounded_end]
        k1 = (k1 * c1) & 0xFFFFFFFF
        k1 = ((k1 << 15) | (k1 >> 17)) & 0xFFFFFFFF
        k1 = (k1 * c2) & 0xFFFFFFFF
        h1 ^= k1

    h1 ^= length
    h1 ^= h1 >> 16
    h1 = (h1 * 0x85EBCA6B) & 0xFFFFFFFF
    h1 ^= h1 >> 13
    h1 = (h1 * 0xC2B2AE35) & 0xFFFFFFFF
    h1 ^= h1 >> 16

    return ctypes.c_int32(h1).value
