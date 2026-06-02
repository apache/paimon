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

"""Bloom filter implementation for file index."""

import math


def _to_signed_32(value: int) -> int:
    """Reinterpret the low 32 bits of ``value`` as a signed 32-bit integer.

    Mirrors Java's silent overflow on ``int`` arithmetic. The combined hash in
    ``BloomFilter64`` is computed with ``int`` in Java, so ``hash1 + i * hash2``
    wraps modulo 2**32 before the negativity check; without this wrap the modulo
    lands on a different bit and the filter probes the wrong positions.
    """
    value &= 0xFFFFFFFF
    if value >= 0x80000000:
        value -= 0x100000000
    return value


class BitSet:
    """Bit set used for bloom filter 64."""

    MASK = 0x07  # Lower 3 bits

    def __init__(self, data: bytearray, offset: int):
        assert len(data) > 0, "data length is zero!"
        assert offset >= 0, "offset is negative!"
        self.data = data
        self.offset = offset

    def set(self, index: int):
        """Set bit at index to 1."""
        byte_idx = (index >> 3) + self.offset
        bit_idx = index & self.MASK
        self.data[byte_idx] |= (1 << bit_idx)

    def get(self, index: int) -> bool:
        """Get bit at index."""
        byte_idx = (index >> 3) + self.offset
        bit_idx = index & self.MASK
        return (self.data[byte_idx] & (1 << bit_idx)) != 0

    def bit_size(self) -> int:
        """Return total number of bits."""
        return (len(self.data) - self.offset) * 8


class BloomFilter64:
    """Bloom filter handling 64-bit hashes."""

    def __init__(self, items_or_num_hash, fpp_or_bitset):
        if isinstance(fpp_or_bitset, float):
            # Constructor: BloomFilter64(items, fpp)
            items = items_or_num_hash
            fpp = fpp_or_bitset

            nb = int(-items * math.log(fpp) / (math.log(2) * math.log(2)))
            self.num_bits = nb + (8 - (nb % 8))
            self.num_hash_functions = max(1, int(round(
                (self.num_bits / items) * math.log(2)
            )))
            self.bitset = BitSet(bytearray(self.num_bits // 8), 0)
        else:
            # Constructor: BloomFilter64(num_hash_functions, bitset)
            self.num_hash_functions = items_or_num_hash
            self.bitset = fpp_or_bitset
            self.num_bits = self.bitset.bit_size()

    def add_hash(self, hash64: int):
        """Add a 64-bit hash to the filter."""
        # Split into two signed 32-bit hashes (Java: (int) hash64, (int) (hash64 >>> 32))
        hash1 = _to_signed_32(hash64)
        hash2 = _to_signed_32(hash64 >> 32)

        for i in range(1, self.num_hash_functions + 1):
            combined_hash = _to_signed_32(hash1 + (i * hash2))
            # Flip bits if negative
            if combined_hash < 0:
                combined_hash = ~combined_hash
            pos = combined_hash % self.num_bits
            self.bitset.set(pos)

    def test_hash(self, hash64: int) -> bool:
        """Test if a 64-bit hash might be in the filter."""
        # Split into two signed 32-bit hashes (Java: (int) hash64, (int) (hash64 >>> 32))
        hash1 = _to_signed_32(hash64)
        hash2 = _to_signed_32(hash64 >> 32)

        for i in range(1, self.num_hash_functions + 1):
            combined_hash = _to_signed_32(hash1 + (i * hash2))
            # Flip bits if negative
            if combined_hash < 0:
                combined_hash = ~combined_hash
            pos = combined_hash % self.num_bits
            if not self.bitset.get(pos):
                return False
        return True

    def get_num_hash_functions(self) -> int:
        return self.num_hash_functions

    def get_bitset(self) -> BitSet:
        return self.bitset
