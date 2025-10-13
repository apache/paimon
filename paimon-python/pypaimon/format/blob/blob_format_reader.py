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

import struct
from typing import List, Optional, Iterator

from pypaimon.table.row.blob import BlobRef, BlobDescriptor
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.format.blob.delta_varint_compressor import DeltaVarintCompressor


class BlobFormatReader:
    """
    Python implementation of Paimon's BlobFormatReader.

    Reads blob data from files in the Paimon blob format, which includes:
    - Reading the footer to get blob index information
    - Creating blob references for each blob entry
    - Supporting selection of specific blob entries
    """

    def __init__(self, file_path: str, file_size: int, selection: Optional[List[int]] = None):
        """
        Initialize BlobFormatReader.

        Args:
            file_path: Path to the blob file
            file_size: Size of the blob file in bytes
            selection: Optional list of blob indices to read (0-based)
        """
        self.file_path = file_path
        self.file_size = file_size
        self.blob_lengths: List[int] = []
        self.blob_offsets: List[int] = []
        self.returned = False

        # Read the file footer to get blob index
        self._read_index(selection)

    def _read_index(self, selection: Optional[List[int]] = None) -> None:
        """
        Read the blob index from the file footer.

        Args:
            selection: Optional list of blob indices to select
        """
        with open(self.file_path, 'rb') as f:
            # Read header (5 bytes from end: 4 bytes index length + 1 byte version)
            f.seek(self.file_size - 5)
            header = f.read(5)

            if len(header) != 5:
                raise IOError("Invalid blob file: cannot read header")

            # Parse header
            index_length = struct.unpack('<I', header[:4])[0]  # Little endian
            version = header[4]

            if version != 1:
                raise IOError(f"Unsupported blob file version: {version}")

            # Read index data
            f.seek(self.file_size - 5 - index_length)
            index_bytes = f.read(index_length)

            if len(index_bytes) != index_length:
                raise IOError("Invalid blob file: cannot read index")

            # Decompress blob lengths
            blob_lengths = DeltaVarintCompressor.decompress(index_bytes)

            # Calculate blob offsets
            blob_offsets = []
            offset = 0
            for length in blob_lengths:
                blob_offsets.append(offset)
                offset += length

            # Apply selection if provided
            if selection is not None:
                selected_lengths = []
                selected_offsets = []
                for idx in selection:
                    if 0 <= idx < len(blob_lengths):
                        selected_lengths.append(blob_lengths[idx])
                        selected_offsets.append(blob_offsets[idx])
                blob_lengths = selected_lengths
                blob_offsets = selected_offsets

            self.blob_lengths = blob_lengths
            self.blob_offsets = blob_offsets

    def read_batch(self) -> Optional['BlobRecordIterator']:
        """
        Read a batch of blob records.

        Returns:
            Iterator over blob records, or None if already returned
        """
        if self.returned:
            return None

        self.returned = True
        return BlobRecordIterator(self.file_path, self.blob_lengths, self.blob_offsets)

    def close(self) -> None:
        """Close the reader (no-op for file-based reader)."""
        pass


class BlobRecordIterator:
    """Iterator for blob records in a blob file."""

    def __init__(self, file_path: str, blob_lengths: List[int], blob_offsets: List[int]):
        """
        Initialize blob record iterator.

        Args:
            file_path: Path to the blob file
            blob_lengths: List of blob lengths
            blob_offsets: List of blob offsets
        """
        self.file_path = file_path
        self.blob_lengths = blob_lengths
        self.blob_offsets = blob_offsets
        self.current_position = 0

    def __iter__(self) -> Iterator[GenericRow]:
        """Return iterator."""
        return self

    def __next__(self) -> GenericRow:
        """
        Get next blob record.

        Returns:
            GenericRow containing a single blob field

        Raises:
            StopIteration: When no more records
        """
        if self.current_position >= len(self.blob_lengths):
            raise StopIteration

        # Create blob reference for the current blob
        # Skip magic number (4 bytes) and exclude length (8 bytes) + CRC (4 bytes) = 12 bytes
        blob_offset = self.blob_offsets[self.current_position] + 4  # Skip magic number
        blob_length = self.blob_lengths[self.current_position] - 16  # Exclude magic(4) + length(8) + CRC(4)

        # Create BlobDescriptor for this blob
        descriptor = BlobDescriptor(self.file_path, blob_offset, blob_length)
        blob = BlobRef(descriptor)

        self.current_position += 1

        # Return as GenericRow with single blob field
        from pypaimon.schema.data_types import DataField, AtomicType
        from pypaimon.table.row.row_kind import RowKind

        fields = [DataField(0, "blob_field", AtomicType("BLOB"))]
        return GenericRow([blob], fields, RowKind.INSERT)

    def returned_position(self) -> int:
        """Get current position in the iterator."""
        return self.current_position

    def file_path(self) -> str:
        """Get the file path."""
        return self.file_path

    def release_batch(self) -> None:
        """Release batch resources (no-op)."""
        pass
