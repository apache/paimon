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

from pypaimon.table.row.blob import Blob, BlobData
from pypaimon.common.delta_varint_compressor import DeltaVarintCompressor


class BlobFormatWriter:
    VERSION = 1
    MAGIC_NUMBER = 1481511375
    BUFFER_SIZE = 4096
    METADATA_SIZE = 12  # 8-byte length + 4-byte CRC

    def __init__(self, output_stream: BinaryIO):
        self.output_stream = output_stream
        self.lengths: List[int] = []
        self.position = 0

    def add_element(self, row) -> None:
        if not hasattr(row, 'values') or len(row.values) != 1:
            raise ValueError("BlobFormatWriter only supports one field")

        blob_value = row.values[0]
        if blob_value is None:
            raise ValueError("BlobFormatWriter only supports non-null blob")

        # Support both BlobData and BlobRef via Blob interface
        if not isinstance(blob_value, (BlobData, Blob)):
            raise ValueError("Field must be Blob/BlobData instance")

        previous_pos = self.position
        crc32 = 0  # Initialize CRC32

        # Write magic number
        magic_bytes = struct.pack('<I', self.MAGIC_NUMBER)  # Little endian
        crc32 = self._write_with_crc(magic_bytes, crc32)

        # Write blob data
        if isinstance(blob_value, BlobData):
            data = blob_value.to_data()
            crc32 = self._write_with_crc(data, crc32)
        else:
            # Stream from BlobRef/Blob
            stream = blob_value.new_input_stream()
            try:
                chunk = stream.read(self.BUFFER_SIZE)
                while chunk:
                    crc32 = self._write_with_crc(chunk, crc32)
                    chunk = stream.read(self.BUFFER_SIZE)
            finally:
                stream.close()

        # Calculate total length including magic + data + metadata (length + CRC)
        bin_length = self.position - previous_pos + self.METADATA_SIZE
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
        crc32 = zlib.crc32(data, crc32)
        self.output_stream.write(data)
        self.position += len(data)
        return crc32

    def reach_target_size(self, suggested_check: bool, target_size: int) -> bool:
        return self.position >= target_size

    def close(self) -> None:
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
