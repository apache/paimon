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

from typing import List


class DeltaVarintCompressor:
    """
    Python implementation of delta varint compression.

    This compresses a list of integers by:
    1. Computing deltas between consecutive values
    2. Encoding each delta using variable-length encoding

    This is a simplified implementation that provides the same interface
    as the Java DeltaVarintCompressor for blob file format compatibility.
    """

    @staticmethod
    def compress(values: List[int]) -> bytes:
        """
        Compress a list of integers using delta varint encoding.

        Args:
            values: List of integers to compress

        Returns:
            Compressed bytes
        """
        if not values:
            return b''

        result = bytearray()

        # First value is stored as-is
        result.extend(DeltaVarintCompressor._encode_varint(values[0]))

        # Subsequent values are stored as deltas
        for i in range(1, len(values)):
            delta = values[i] - values[i - 1]
            result.extend(DeltaVarintCompressor._encode_varint(delta))

        return bytes(result)

    @staticmethod
    def decompress(data: bytes) -> List[int]:
        """
        Decompress bytes back to a list of integers.

        Args:
            data: Compressed bytes

        Returns:
            List of decompressed integers
        """
        if not data:
            return []

        result = []
        offset = 0

        # Read first value
        value, offset = DeltaVarintCompressor._decode_varint(data, offset)
        result.append(value)

        # Read deltas and reconstruct values
        while offset < len(data):
            delta, offset = DeltaVarintCompressor._decode_varint(data, offset)
            value = result[-1] + delta
            result.append(value)

        return result

    @staticmethod
    def _encode_varint(value: int) -> bytes:
        """
        Encode an integer using variable-length encoding.

        Args:
            value: Integer to encode

        Returns:
            Encoded bytes
        """
        # Handle negative numbers by zigzag encoding
        if value < 0:
            value = (-value << 1) | 1
        else:
            value = value << 1

        result = bytearray()
        while value >= 0x80:
            result.append((value & 0x7F) | 0x80)
            value >>= 7
        result.append(value & 0x7F)

        return bytes(result)

    @staticmethod
    def _decode_varint(data: bytes, offset: int) -> tuple[int, int]:
        """
        Decode a variable-length encoded integer.

        Args:
            data: Bytes to decode from
            offset: Starting offset

        Returns:
            Tuple of (decoded_value, new_offset)
        """
        value = 0
        shift = 0

        while offset < len(data):
            byte = data[offset]
            offset += 1

            value |= (byte & 0x7F) << shift
            if (byte & 0x80) == 0:
                break
            shift += 7

        # Decode zigzag encoding
        if value & 1:
            value = -(value >> 1)
        else:
            value = value >> 1

        return value, offset
