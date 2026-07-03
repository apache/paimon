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

"""
Roaring Bitmap.
"""

import struct
from typing import Iterator


_UINT32_MASK = (1 << 32) - 1
_INT64_MAX = (1 << 63) - 1


def _check_int64(value: int) -> None:
    if value < 0 or value > _INT64_MAX:
        raise ValueError("RoaringBitmap64 only supports values from 0 to 2^63 - 1.")


class _ChunkedBitMap64:
    """64-bit bitmap fallback built from pyroaring.BitMap chunks."""

    def __init__(self):
        from pyroaring import BitMap

        self._bitmap_cls = BitMap
        self._chunks = {}

    def add(self, value: int) -> None:
        _check_int64(value)
        key = value >> 32
        low = value & _UINT32_MASK
        self._chunks.setdefault(key, self._bitmap_cls()).add(low)

    def add_range(self, from_: int, to: int) -> None:
        _check_int64(from_)
        _check_int64(to - 1 if to > from_ else from_)
        current = from_
        while current < to:
            key = current >> 32
            next_key_start = (key + 1) << 32
            chunk_end = min(to, next_key_start)
            self._chunks.setdefault(key, self._bitmap_cls()).add_range(
                current & _UINT32_MASK,
                ((chunk_end - 1) & _UINT32_MASK) + 1
            )
            current = chunk_end

    def __contains__(self, value: int) -> bool:
        if value < 0 or value > _INT64_MAX:
            return False
        key = value >> 32
        chunk = self._chunks.get(key)
        return chunk is not None and (value & _UINT32_MASK) in chunk

    def __iter__(self) -> Iterator[int]:
        for key in sorted(self._chunks):
            base = key << 32
            for low in self._chunks[key]:
                yield base + low

    def __len__(self) -> int:
        return sum(len(chunk) for chunk in self._chunks.values())

    def clear(self) -> None:
        self._chunks.clear()

    def _copy_chunk(self, chunk):
        return self._bitmap_cls.deserialize(chunk.serialize())

    def serialize(self) -> bytes:
        chunks = [
            (key, chunk)
            for key, chunk in sorted(self._chunks.items())
            if len(chunk) > 0
        ]
        result = bytearray(struct.pack("<q", len(chunks)))
        for key, chunk in chunks:
            result.extend(struct.pack("<i", key))
            result.extend(chunk.serialize())
        return bytes(result)

    @classmethod
    def deserialize(cls, data: bytes) -> '_ChunkedBitMap64':
        from pyroaring import BitMap

        result = cls()
        offset = 0
        count = struct.unpack_from("<q", data, offset)[0]
        offset += 8
        if count < 0:
            raise ValueError("Invalid 64-bit roaring bitmap chunk count: %s" % count)

        last_key = -1
        for _ in range(count):
            key = struct.unpack_from("<i", data, offset)[0]
            offset += 4
            if key < 0:
                raise ValueError("Invalid 64-bit roaring bitmap chunk key: %s" % key)
            if key <= last_key:
                raise ValueError("64-bit roaring bitmap chunk keys must be ascending.")

            chunk = BitMap.deserialize(data[offset:])
            consumed = len(chunk.serialize())
            if consumed <= 0:
                raise ValueError("Invalid empty 32-bit roaring bitmap payload.")
            offset += consumed
            if len(chunk) > 0:
                result._chunks[key] = chunk
            last_key = key

        if offset != len(data):
            raise ValueError("Trailing bytes in 64-bit roaring bitmap payload.")

        return result

    def __and__(self, other: '_ChunkedBitMap64') -> '_ChunkedBitMap64':
        result = _ChunkedBitMap64()
        for key in set(self._chunks).intersection(other._chunks):
            chunk = self._chunks[key] & other._chunks[key]
            if len(chunk) > 0:
                result._chunks[key] = chunk
        return result

    def __or__(self, other: '_ChunkedBitMap64') -> '_ChunkedBitMap64':
        result = _ChunkedBitMap64()
        for key in set(self._chunks).union(other._chunks):
            left = self._chunks.get(key)
            right = other._chunks.get(key)
            if left is None:
                chunk = self._copy_chunk(right)
            elif right is None:
                chunk = self._copy_chunk(left)
            else:
                chunk = left | right
            if len(chunk) > 0:
                result._chunks[key] = chunk
        return result

    def __sub__(self, other: '_ChunkedBitMap64') -> '_ChunkedBitMap64':
        result = _ChunkedBitMap64()
        for key, chunk in self._chunks.items():
            other_chunk = other._chunks.get(key)
            if other_chunk is None:
                diff = self._copy_chunk(chunk)
            else:
                diff = chunk - other_chunk
            if len(diff) > 0:
                result._chunks[key] = diff
        return result

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _ChunkedBitMap64):
            return False
        return self._chunks == other._chunks


def _new_bitmap64():
    try:
        from pyroaring import BitMap64

        return BitMap64()
    except ImportError:
        return _ChunkedBitMap64()


class RoaringBitmap64:
    """
    A 64-bit roaring bitmap implementation.

    This class provides efficient storage and operations for sets of 64-bit integers.
    It uses pyroaring.BitMap64 for better performance and memory efficiency.
    """

    def __init__(self):
        self._data = _new_bitmap64()

    def add(self, value: int) -> None:
        """Add a single value to the bitmap."""
        _check_int64(value)
        self._data.add(value)

    def add_range(self, from_: int, to: int) -> None:
        """Add a range of values [from_, to] to the bitmap."""
        _check_int64(from_)
        _check_int64(to)
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
        from pypaimon.utils.range import Range

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
        try:
            from pyroaring import BitMap64

            result._data = BitMap64.deserialize(data)
        except ImportError:
            result._data = _ChunkedBitMap64.deserialize(data)
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


class RoaringBitmap:
    """
    A 32-bit roaring bitmap implementation.

    This class provides efficient storage and operations for sets of 32-bit integers.
    It uses pyroaring.BitMap for better performance and memory efficiency.
    """

    def __init__(self):
        from pyroaring import BitMap
        self._data = BitMap()

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

    @staticmethod
    def and_(a: 'RoaringBitmap', b: 'RoaringBitmap') -> 'RoaringBitmap':
        """Return the intersection of two bitmaps."""
        result = RoaringBitmap()
        result._data = a._data & b._data
        return result

    @staticmethod
    def or_(a: 'RoaringBitmap', b: 'RoaringBitmap') -> 'RoaringBitmap':
        """Return the union of two bitmaps."""
        result = RoaringBitmap()
        result._data = a._data | b._data
        return result

    @staticmethod
    def remove_all(a: 'RoaringBitmap', b: 'RoaringBitmap') -> 'RoaringBitmap':
        result = RoaringBitmap()
        result._data = a._data - b._data
        return result

    def serialize(self) -> bytes:
        """Serialize the bitmap to bytes."""
        return self._data.serialize()

    @staticmethod
    def deserialize(data: bytes) -> 'RoaringBitmap':
        """Deserialize a bitmap from bytes."""
        result = RoaringBitmap()
        from pyroaring import BitMap
        result._data = BitMap.deserialize(data)
        return result

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RoaringBitmap):
            return False
        return self._data == other._data

    def __hash__(self) -> int:
        return hash(tuple(sorted(self._data)))

    def __repr__(self) -> str:
        values = list(self._data)
        if len(values) <= 10:
            return f"RoaringBitmap({values})"
        return f"RoaringBitmap({len(values)} elements)"
