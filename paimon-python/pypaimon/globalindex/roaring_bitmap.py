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
Roaring Bitmap.
"""

from typing import Iterator
from pyroaring import BitMap64


class RoaringBitmap64:
    """
    A 64-bit roaring bitmap implementation.

    This class provides efficient storage and operations for sets of 64-bit integers.
    It uses pyroaring.BitMap64 for better performance and memory efficiency.
    """

    def __init__(self):
        self._data = BitMap64()

    def add(self, value: int) -> None:
        """Add a single value to the bitmap."""
        self._data.add(value)

    def add_range(self, from_: int, to: int) -> None:
        """Add a range of values [from_, to] to the bitmap."""
        self._data.add_range(from_, to + 1)

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
        return iter(self._data)

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
        return list(self._data)

    def to_range_list(self) -> list:
        """
        Convert the bitmap to a list of Range objects.
        """
        from pypaimon.globalindex.range import Range

        if self.is_empty():
            return []

        # Use pyroaring's efficient iteration
        ranges = []
        sorted_values = list(self._data)
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

    @staticmethod
    def remove_all(a: 'RoaringBitmap64', b: 'RoaringBitmap64') -> 'RoaringBitmap64':
        result = RoaringBitmap64()
        result._data = a._data - b._data
        return result

    def serialize(self) -> bytes:
        """Serialize the bitmap to bytes."""
        return self._data.serialize()

    @staticmethod
    def deserialize(data: bytes) -> 'RoaringBitmap64':
        """Deserialize a bitmap from bytes."""
        result = RoaringBitmap64()
        result._data = BitMap64.deserialize(data)
        return result

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RoaringBitmap64):
            return False
        return self._data == other._data

    def __hash__(self) -> int:
        return hash(tuple(sorted(self._data)))

    def __repr__(self) -> str:
        values = list(self._data)
        if len(values) <= 10:
            return f"RoaringBitmap64({values})"
        return f"RoaringBitmap64({len(values)} elements)"
