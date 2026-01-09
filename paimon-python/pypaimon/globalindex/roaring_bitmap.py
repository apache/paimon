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
# limitations under the License.
################################################################################

"""
Roaring Bitmap implementation for 64-bit integers.

This module provides a Python implementation of RoaringBitmap64 that is compatible
with the Java implementation in Paimon. It supports efficient storage and operations
on sets of 64-bit integers.
"""

from typing import Iterator, Set
import struct


class RoaringBitmap64:
    """
    A 64-bit roaring bitmap implementation.
    This class provides efficient storage and operations for sets of 64-bit integers.
    It uses a set-based implementation for simplicity, which can be replaced with
    a more efficient roaring bitmap library if needed.
    """

    def __init__(self):
        self._data: Set[int] = set()

    def add(self, value: int) -> None:
        """Add a single value to the bitmap."""
        self._data.add(value)

    def add_range(self, from_: int, to: int) -> None:
        """Add a range of values [from_, to] to the bitmap."""
        for i in range(from_, to + 1):
            self._data.add(i)

    def contains(self, value: int) -> bool:
        """Check if the bitmap contains the given value."""
        return value in self._data

    def is_empty(self) -> bool:
        """Check if the bitmap is empty."""
        return len(self._data) == 0

    def cardinality(self) -> int:
        """Return the number of elements in the bitmap."""
        return len(self._data)

    def __iter__(self) -> Iterator[int]:
        """Iterate over all values in the bitmap in sorted order."""
        return iter(sorted(self._data))

    def __len__(self) -> int:
        """Return the number of elements in the bitmap."""
        return len(self._data)

    def __contains__(self, value: int) -> bool:
        """Check if the bitmap contains the given value."""
        return self.contains(value)

    def clear(self) -> None:
        """Clear all values from the bitmap."""
        self._data.clear()

    def to_list(self) -> list:
        """Return a sorted list of all values in the bitmap."""
        return sorted(self._data)

    def to_range_list(self) -> list:
        """
        Convert the bitmap to a list of Range objects.
        """
        from pypaimon.globalindex.range import Range

        if self.is_empty():
            return []

        sorted_values = sorted(self._data)
        ranges = []
        start = sorted_values[0]
        end = start

        for i in range(1, len(sorted_values)):
            if sorted_values[i] == end + 1:
                # Consecutive, extend the range
                end = sorted_values[i]
            else:
                # Gap, close current range and start new one
                ranges.append(Range(start, end))
                start = sorted_values[i]
                end = start

        # Add the last range
        ranges.append(Range(start, end))

        return ranges

    @staticmethod
    def and_(a: 'RoaringBitmap64', b: 'RoaringBitmap64') -> 'RoaringBitmap64':
        """Return the intersection of two bitmaps."""
        result = RoaringBitmap64()
        result._data = a._data & b._data
        return result

    @staticmethod
    def or_(a: 'RoaringBitmap64', b: 'RoaringBitmap64') -> 'RoaringBitmap64':
        """Return the union of two bitmaps."""
        result = RoaringBitmap64()
        result._data = a._data | b._data
        return result

    def serialize(self) -> bytes:
        """Serialize the bitmap to bytes."""
        # Simple serialization format: count followed by sorted values
        values = sorted(self._data)
        data = struct.pack('>Q', len(values))  # 8-byte count
        for v in values:
            data += struct.pack('>q', v)  # 8-byte signed value
        return data

    @staticmethod
    def deserialize(data: bytes) -> 'RoaringBitmap64':
        """Deserialize a bitmap from bytes."""
        result = RoaringBitmap64()
        count = struct.unpack('>Q', data[:8])[0]
        offset = 8
        for _ in range(count):
            value = struct.unpack('>q', data[offset:offset + 8])[0]
            result.add(value)
            offset += 8
        return result

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RoaringBitmap64):
            return False
        return self._data == other._data

    def __hash__(self) -> int:
        return hash(frozenset(self._data))

    def __repr__(self) -> str:
        if len(self._data) <= 10:
            return f"RoaringBitmap64({sorted(self._data)})"
        return f"RoaringBitmap64({len(self._data)} elements)"
