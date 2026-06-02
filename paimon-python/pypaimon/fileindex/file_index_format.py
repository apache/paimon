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

"""File index format reader for PyPaimon."""

import struct
from typing import Dict, Tuple, Optional


MAGIC = 1493475289347502
VERSION_1 = 1
EMPTY_INDEX_FLAG = -1


class FileIndexFormatReader:
    """
    Reader for file index format.

    Format:
    - magic (8 bytes long, big-endian)
    - version (4 bytes int, big-endian)
    - head_length (4 bytes int, big-endian)
    - column_count (4 bytes int)
    - For each column:
      - column_name (2 bytes length + utf-8 bytes)
      - index_count (4 bytes int)
      - For each index:
        - index_name (2 bytes length + utf-8 bytes)
        - start_pos (4 bytes int)
        - length (4 bytes int)
    - redundant_length (4 bytes int)
    - redundant_bytes (variable)
    - body (variable)
    """

    def __init__(self, data: bytes):
        self.data = data
        self.offset = 0
        self.header: Dict[str, Dict[str, Tuple[int, int]]] = {}
        self._parse_header()

    def _read_long(self) -> int:
        """Read 8-byte big-endian long."""
        val = struct.unpack('>Q', self.data[self.offset:self.offset + 8])[0]
        self.offset += 8
        return val

    def _read_int(self) -> int:
        """Read 4-byte big-endian signed int.

        Java writes these with ``DataOutputStream.writeInt`` (signed), and uses
        ``-1`` (:data:`EMPTY_INDEX_FLAG`) as a sentinel for empty index columns,
        so the value must be read as signed to round-trip that flag.
        """
        val = struct.unpack('>i', self.data[self.offset:self.offset + 4])[0]
        self.offset += 4
        return val

    def _read_utf(self) -> str:
        """Read a Java ``DataOutput.writeUTF`` string (2-byte length + bytes).

        Java encodes these as *modified* UTF-8 (NUL is two bytes, and characters
        outside the BMP are written as a CESU-8 surrogate pair), which is not
        byte-identical to standard UTF-8. We decode as standard UTF-8, which is
        correct for ASCII column names (the only case in practice) but would
        raise on a column name containing NUL or a non-BMP character. Such a
        failure propagates up and is handled fail-open by the caller (the file
        is kept), so it degrades safely rather than mis-decoding.
        """
        length = struct.unpack('>H', self.data[self.offset:self.offset + 2])[0]
        self.offset += 2
        s = self.data[self.offset:self.offset + length].decode('utf-8')
        self.offset += length
        return s

    def _parse_header(self):
        """Parse the header section."""
        # Read and verify magic
        magic = self._read_long()
        if magic != MAGIC:
            raise ValueError(f"Invalid magic number: {magic}")

        # Read version
        version = self._read_int()
        if version != VERSION_1:
            raise ValueError(f"Unsupported version: {version}")

        # Read head length (consumed to advance the offset; value not needed here)
        self._read_int()

        # Read column count
        column_count = self._read_int()

        # Parse each column
        for _ in range(column_count):
            column_name = self._read_utf()
            index_count = self._read_int()

            column_indexes = {}
            for _ in range(index_count):
                index_name = self._read_utf()
                start_pos = self._read_int()
                length = self._read_int()
                column_indexes[index_name] = (start_pos, length)

            self.header[column_name] = column_indexes

        # Read redundant length and skip redundant bytes
        redundant_length = self._read_int()
        self.offset += redundant_length

    def read_column_index(self, column_name: str, index_type: str = "bloom-filter") -> Optional[bytes]:
        """
        Read the index bytes for a specific column and index type.

        Args:
            column_name: Name of the column
            index_type: Type of index (default: "bloom-filter")

        Returns:
            Index bytes or None if not found
        """
        if column_name not in self.header:
            return None

        column_indexes = self.header[column_name]
        if index_type not in column_indexes:
            return None

        start_pos, length = column_indexes[index_type]
        # Java writes EMPTY_INDEX_FLAG (-1) as the start for columns with no
        # actual index body. Treat that as "no index".
        if start_pos == EMPTY_INDEX_FLAG or length <= 0:
            return None
        # Bounds-check before slicing: Python slicing silently truncates an
        # out-of-range range, which on a corrupt/truncated blob would hand back
        # fewer bytes than were written. A short bitset yields the wrong
        # num_bits and makes the bloom filter probe wrong positions, which can
        # turn into false SKIPs (dropped rows). Refuse such input instead.
        if start_pos < 0 or start_pos + length > len(self.data):
            return None
        return self.data[start_pos:start_pos + length]

    def get_columns(self):
        """Get all column names in the index."""
        return list(self.header.keys())
