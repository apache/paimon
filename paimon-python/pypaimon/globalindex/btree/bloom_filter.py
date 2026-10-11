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

"""Bloom filter compatible with Paimon's Java SST implementation."""

import math
from typing import Collection, Optional


_INT_MASK = 0xFFFFFFFF
_INT_SIGN_BIT = 0x80000000
_MURMUR_C1 = 0xCC9E2D51
_MURMUR_C2 = 0x1B873593
_MURMUR_DEFAULT_SEED = 42


def _int32(value: int) -> int:
    value &= _INT_MASK
    return value if value < _INT_SIGN_BIT else value - (_INT_MASK + 1)


def _rotate_left(value: int, distance: int) -> int:
    unsigned = value & _INT_MASK
    return _int32(
        (unsigned << distance) | (unsigned >> (32 - distance)))


def _mix_k1(value: int) -> int:
    value = _int32(value * _MURMUR_C1)
    value = _rotate_left(value, 15)
    return _int32(value * _MURMUR_C2)


def _mix_h1(hash_value: int, mixed_value: int) -> int:
    hash_value = _int32(
        (hash_value & _INT_MASK) ^ (mixed_value & _INT_MASK))
    hash_value = _rotate_left(hash_value, 13)
    return _int32(hash_value * 5 + 0xE6546B64)


def _fmix(hash_value: int, length: int) -> int:
    hash_value = _int32((hash_value & _INT_MASK) ^ length)
    hash_value = _int32(
        (hash_value & _INT_MASK) ^ ((hash_value & _INT_MASK) >> 16))
    hash_value = _int32(hash_value * 0x85EBCA6B)
    hash_value = _int32(
        (hash_value & _INT_MASK) ^ ((hash_value & _INT_MASK) >> 13))
    hash_value = _int32(hash_value * 0xC2B2AE35)
    return _int32(
        (hash_value & _INT_MASK) ^ ((hash_value & _INT_MASK) >> 16))


def murmur_hash_bytes(data: bytes) -> int:
    """Return the signed hash produced by Java ``MurmurHashUtils.hashBytes``."""
    aligned_length = len(data) - len(data) % 4
    hash_value = _MURMUR_DEFAULT_SEED

    for offset in range(0, aligned_length, 4):
        word = int.from_bytes(data[offset:offset + 4], "little", signed=True)
        hash_value = _mix_h1(hash_value, _mix_k1(word))

    # Paimon's Java implementation mixes every remaining signed byte as a
    # separate word instead of combining the Murmur3 tail bytes.
    for byte in data[aligned_length:]:
        signed_byte = byte if byte < 128 else byte - 256
        hash_value = _mix_h1(hash_value, _mix_k1(signed_byte))

    return _fmix(hash_value, len(data))


class BloomFilter:
    """A compact Bloom filter using the Java Paimon bit layout."""

    def __init__(
        self,
        expected_entries: int,
        byte_size: int,
        data: Optional[bytes] = None,
    ):
        if expected_entries <= 0:
            raise ValueError("expected_entries must be positive")
        if byte_size <= 0:
            raise ValueError("byte_size must be positive")
        if data is not None and len(data) != byte_size:
            raise ValueError(
                "Bloom filter size mismatch: expected %s bytes, got %s"
                % (byte_size, len(data)))

        self.expected_entries = expected_entries
        self._bits = bytearray(byte_size if data is None else data)
        hashes = byte_size * 8 / expected_entries * math.log(2)
        # Java Math.round rounds positive halfway values toward positive infinity.
        self._num_hash_functions = max(1, int(math.floor(hashes + 0.5)))

    @classmethod
    def from_hashes(
        cls,
        hashes: Collection[int],
        false_positive_probability: float,
    ) -> Optional['BloomFilter']:
        expected_entries = len(hashes)
        if expected_entries == 0:
            return None
        if not 0 < false_positive_probability < 1:
            raise ValueError(
                "false_positive_probability must be between 0 and 1")

        optimal_bits = int(
            -expected_entries * math.log(false_positive_probability)
            / (math.log(2) * math.log(2)))
        byte_size = int(math.ceil(optimal_bits / 8.0))
        bloom_filter = cls(expected_entries, byte_size)
        for hash_value in hashes:
            bloom_filter.add_hash(hash_value)
        return bloom_filter

    def add_hash(self, hash_value: int) -> None:
        for position in self._positions(hash_value):
            self._bits[position >> 3] |= 1 << (position & 7)

    def test_hash(self, hash_value: int) -> bool:
        return all(
            self._bits[position >> 3] & (1 << (position & 7))
            for position in self._positions(hash_value))

    def to_bytes(self) -> bytes:
        return bytes(self._bits)

    def _positions(self, hash_value: int):
        hash_value = _int32(hash_value)
        hash2 = (hash_value & _INT_MASK) >> 16
        bit_size = len(self._bits) * 8
        for index in range(1, self._num_hash_functions + 1):
            combined_hash = _int32(hash_value + index * hash2)
            if combined_hash < 0:
                combined_hash = _int32(~combined_hash)
            yield combined_hash % bit_size
