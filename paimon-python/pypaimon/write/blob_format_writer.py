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
import zlib
from typing import BinaryIO, List

from pypaimon.table.row.blob import BlobData
from pypaimon.common.delta_varint_compressor import DeltaVarintCompressor


class BlobFormatWriter:
    """
    Python implementation of Paimon's BlobFormatWriter.

    Writes blob data to files in the Paimon blob format, which includes:
    - Magic number for each blob entry
    - Blob data content
    - Length and CRC32 checksum for each blob
    - Index of blob lengths at the end
    - Header with index length and version
    """

    VERSION = 1
    MAGIC_NUMBER = 1481511375  # Same as Java implementation
    BUFFER_SIZE = 4096

    def __init__(self, output_stream: BinaryIO):
        """
        Initialize BlobFormatWriter.

        Args:
            output_stream: Binary output stream to write to
        """
        self.output_stream = output_stream
        self.lengths: List[int] = []
        self.position = 0

    def add_element(self, row) -> None:
        """
        Add a row element containing blob data.

        Args:
            row: Row containing exactly one blob field

        Raises:
            ValueError: If row doesn't have exactly one field or blob is null
        """
        if not hasattr(row, 'values') or len(row.values) != 1:
            raise ValueError("BlobFormatWriter only supports one field")

        blob_value = row.values[0]
        if blob_value is None:
            raise ValueError("BlobFormatWriter only supports non-null blob")

        if not isinstance(blob_value, BlobData):
            raise ValueError("Field must be BlobData instance")

        previous_pos = self.position
        crc32 = 0  # Initialize CRC32

        # Write magic number
        magic_bytes = struct.pack('<I', self.MAGIC_NUMBER)  # Little endian
        crc32 = self._write_with_crc(magic_bytes, crc32)

        # Write blob data
        blob_data = blob_value.to_data()
        crc32 = self._write_with_crc(blob_data, crc32)

        # Calculate total length including magic number, data, length, and CRC
        bin_length = self.position - previous_pos + 12  # +12 for length(8) + crc(4)
        self.lengths.append(bin_length)

        # Write length (8 bytes, little endian)
        length_bytes = struct.pack('<Q', bin_length)
        self.output_stream.write(length_bytes)
        self.position += 8

        # Write CRC32 (4 bytes, little endian)
        crc_bytes = struct.pack('<I', crc32 & 0xffffffff)
        self.output_stream.write(crc_bytes)
        self.position += 4

    def _write_with_crc(self, data: bytes, crc32: int) -> int:
        """
        Write data and update CRC32.

        Args:
            data: Data to write
            crc32: Current CRC32 value

        Returns:
            Updated CRC32 value
        """
        crc32 = zlib.crc32(data, crc32)
        self.output_stream.write(data)
        self.position += len(data)
        return crc32

    def reach_target_size(self, suggested_check: bool, target_size: int) -> bool:
        """
        Check if the current file size has reached the target size.

        Args:
            suggested_check: Whether size check is suggested
            target_size: Target file size in bytes

        Returns:
            True if target size is reached
        """
        # Check target size every record since each blob is typically large
        return self.position >= target_size

    def close(self) -> None:
        """
        Close the writer and write the file footer.

        The footer contains:
        - Compressed index of blob lengths
        - Index length (4 bytes)
        - Version (1 byte)
        """
        # Compress and write index
        index_bytes = DeltaVarintCompressor.compress(self.lengths)
        self.output_stream.write(index_bytes)

        # Write header (index length + version)
        header = struct.pack('<I', len(index_bytes))  # Index length (4 bytes, little endian)
        header += struct.pack('<B', self.VERSION)  # Version (1 byte)
        self.output_stream.write(header)

        # Flush and close
        if hasattr(self.output_stream, 'flush'):
            self.output_stream.flush()
        if hasattr(self.output_stream, 'close'):
            self.output_stream.close()
