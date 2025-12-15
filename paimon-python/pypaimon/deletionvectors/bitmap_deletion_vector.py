#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from pypaimon.deletionvectors.deletion_vector import DeletionVector
import struct
import zlib
from pyroaring import BitMap


class BitmapDeletionVector(DeletionVector):
    """
    A DeletionVector based on RoaringBitmap, it only supports files with row count
    not exceeding 2147483647 (max value for 32-bit integer).
    """

    MAGIC_NUMBER = 1581511376
    MAGIC_NUMBER_SIZE_BYTES = 4
    MAX_VALUE = 2147483647

    def __init__(self, bitmap: BitMap = None):
        """
        Initialize a BitmapDeletionVector.

        Args:
            bitmap: Optional RoaringBitmap instance. If None, creates an empty bitmap.
        """
        self._bitmap = bitmap if bitmap is not None else BitMap()

    def delete(self, position: int) -> None:
        """
        Marks the row at the specified position as deleted.

        Args:
            position: The position of the row to be marked as deleted.
        """
        self._check_position(position)
        self._bitmap.add(position)

    def is_deleted(self, position: int) -> bool:
        """
        Checks if the row at the specified position is deleted.
        
        Args:
            position: The position of the row to check.

        Returns:
            True if the row is deleted, False otherwise.
        """
        self._check_position(position)
        return position in self._bitmap

    def is_empty(self) -> bool:
        return len(self._bitmap) == 0

    def get_cardinality(self) -> int:
        """
        Returns the number of distinct integers added to the DeletionVector.

        Returns:
            The number of deleted positions.
        """
        return len(self._bitmap)

    def merge(self, deletion_vector: DeletionVector) -> None:
        """
        Merge another DeletionVector to this current one.

        Args:
            deletion_vector: The other DeletionVector to merge.
        """
        if isinstance(deletion_vector, BitmapDeletionVector):
            self._bitmap |= deletion_vector._bitmap
        else:
            raise RuntimeError("Only instance with the same class type can be merged.")

    def serialize(self) -> bytes:
        """
        Serializes the deletion vector to bytes.

        Returns:
            The serialized bytes.
        """
        # Serialize the bitmap
        bitmap_bytes = self._bitmap.serialize()

        # Create the full data with magic number
        magic_bytes = struct.pack('>I', self.MAGIC_NUMBER)
        data = magic_bytes + bitmap_bytes

        # Calculate size and checksum
        size = len(data)
        checksum = self._calculate_checksum(data)

        # Pack: size (4 bytes) + data + checksum (4 bytes)
        result = struct.pack('>I', size) + data + struct.pack('>I', checksum)

        return result

    @staticmethod
    def deserialize_from_bytes(data: bytes) -> 'BitmapDeletionVector':
        """
        Deserializes a BitmapDeletionVector from bytes.

        Args:
            data: The serialized bytes (without magic number).

        Returns:
            A BitmapDeletionVector instance.
        """
        bitmap = BitMap.deserialize(data)
        return BitmapDeletionVector(bitmap)

    def bit_map(self):
        return self._bitmap

    def _check_position(self, position: int) -> None:
        """
        Checks if the position is valid.

        Args:
            position: The position to check.

        Raises:
            ValueError: If the position exceeds the maximum value.
        """
        if position > self.MAX_VALUE:
            raise ValueError(
                f"The file has too many rows, RoaringBitmap32 only supports files "
                f"with row count not exceeding {self.MAX_VALUE}."
            )

    @staticmethod
    def _calculate_checksum(data: bytes) -> int:
        """
        Calculates CRC32 checksum for the given data.

        Args:
            data: The data to calculate checksum for.

        Returns:
            The CRC32 checksum as an unsigned 32-bit integer.
        """
        return zlib.crc32(data) & 0xffffffff

    def __eq__(self, other):
        if not isinstance(other, BitmapDeletionVector):
            return False
        return self._bitmap == other._bitmap

    def __hash__(self):
        return hash(tuple(self._bitmap))
