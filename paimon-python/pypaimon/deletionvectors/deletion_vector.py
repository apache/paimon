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

from abc import ABC, abstractmethod

from pypaimon.common.file_io import FileIO
from pypaimon.table.source.deletion_file import DeletionFile


class DeletionVector(ABC):
    """
    The DeletionVector can efficiently record the positions of rows that are deleted in a file,
    which can then be used to filter out deleted rows when processing the file.
    """

    @abstractmethod
    def bit_map(self):
        """
        Returns the bitmap of the DeletionVector.
        """
        pass

    @abstractmethod
    def delete(self, position: int) -> None:
        """
        Marks the row at the specified position as deleted.

        Args:
            position: The position of the row to be marked as deleted.
        """
        pass

    @abstractmethod
    def is_deleted(self, position: int) -> bool:
        """
        Checks if the row at the specified position is deleted.

        Args:
            position: The position of the row to check.

        Returns:
            True if the row is deleted, False otherwise.
        """
        pass

    @abstractmethod
    def is_empty(self) -> bool:
        """
        Determines if the deletion vector is empty, indicating no deletions.

        Returns:
            True if the deletion vector is empty, False if it contains deletions.
        """
        pass

    @abstractmethod
    def get_cardinality(self) -> int:
        """
        Returns the number of distinct integers added to the DeletionVector.

        Returns:
            The number of deleted positions.
        """
        pass

    @abstractmethod
    def merge(self, deletion_vector: 'DeletionVector') -> None:
        """
        Merge another DeletionVector to this current one.

        Args:
            deletion_vector: The other DeletionVector to merge.
        """
        pass

    def checked_delete(self, position: int) -> bool:
        """
        Marks the row at the specified position as deleted.

        Args:
            position: The position of the row to be marked as deleted.

        Returns:
            True if the added position wasn't already deleted. False otherwise.
        """
        if self.is_deleted(position):
            return False
        else:
            self.delete(position)
            return True

    @staticmethod
    def read(file_io: FileIO, deletion_file: DeletionFile) -> 'DeletionVector':
        """
        Read a DeletionVector from a file.
        """
        from pypaimon.deletionvectors.bitmap_deletion_vector import BitmapDeletionVector

        with file_io.new_input_stream(deletion_file.dv_index_path) as f:
            f.seek(deletion_file.offset)

            # Read bitmap length
            bitmap_length_bytes = f.read(4)
            bitmap_length = int.from_bytes(bitmap_length_bytes, byteorder='big')

            # Read magic number
            magic_number_bytes = f.read(4)
            magic_number = int.from_bytes(magic_number_bytes, byteorder='big')

            if magic_number == BitmapDeletionVector.MAGIC_NUMBER:
                if deletion_file.length is not None and bitmap_length != deletion_file.length:
                    raise RuntimeError(
                        f"Size not match, actual size: {bitmap_length}, expected size: {deletion_file.length}"
                    )

                # Magic number has been read, read remaining bytes
                remaining_bytes = bitmap_length - BitmapDeletionVector.MAGIC_NUMBER_SIZE_BYTES
                data = f.read(remaining_bytes)

                # Skip CRC (4 bytes)
                f.read(4)

                return BitmapDeletionVector.deserialize_from_bytes(data)
            else:
                raise RuntimeError(
                    f"Invalid magic number: {magic_number}, "
                    f"expected: {BitmapDeletionVector.MAGIC_NUMBER}"
                )
