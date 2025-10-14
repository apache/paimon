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

import io
from typing import List


class DeltaVarintCompressor:

    @staticmethod
    def compress(data: List[int]) -> bytes:
        if not data:
            return b''

        # Estimate output size (conservative: 5 bytes per varint max)
        estimated_size = len(data) * 5
        out = io.BytesIO()
        out.truncate(estimated_size)  # Pre-allocate buffer
        out.seek(0)

        # Encode first value directly
        DeltaVarintCompressor._encode_varint(data[0], out)

        # Encode deltas without intermediate list creation
        prev = data[0]
        for i in range(1, len(data)):
            current = data[i]
            delta = current - prev
            DeltaVarintCompressor._encode_varint(delta, out)
            prev = current

        # Return only the used portion of the buffer
        position = out.tell()
        result = out.getvalue()
        out.close()
        return result[:position]

    @staticmethod
    def decompress(compressed: bytes) -> List[int]:
        if not compressed:
            return []

        # Fast path: decode directly into result without intermediate deltas list
        in_stream = io.BytesIO(compressed)
        result = []

        try:
            # Decode first value
            first_value = DeltaVarintCompressor._decode_varint(in_stream)
            result.append(first_value)

            # Decode and reconstruct remaining values in one pass
            current_value = first_value
            while True:
                try:
                    delta = DeltaVarintCompressor._decode_varint(in_stream)
                    current_value += delta
                    result.append(current_value)
                except RuntimeError:
                    # End of stream reached
                    break

        except RuntimeError:
            # Handle empty stream case
            pass
        finally:
            in_stream.close()

        return result

    @staticmethod
    def _encode_varint(value: int, out: io.BytesIO) -> None:
        # ZigZag encoding - use reliable arithmetic approach
        if value >= 0:
            zigzag = value << 1
        else:
            zigzag = ((-value) << 1) - 1

        # Optimized varint encoding - batch byte operations
        if zigzag < 0x80:
            # Single byte case (most common)
            out.write(bytes([zigzag]))
        elif zigzag < 0x4000:
            # Two byte case
            out.write(bytes([
                (zigzag & 0x7F) | 0x80,
                zigzag >> 7
            ]))
        elif zigzag < 0x200000:
            # Three byte case
            out.write(bytes([
                (zigzag & 0x7F) | 0x80,
                ((zigzag >> 7) & 0x7F) | 0x80,
                zigzag >> 14
            ]))
        else:
            # General case for larger numbers
            while zigzag >= 0x80:
                out.write(bytes([(zigzag & 0x7F) | 0x80]))
                zigzag >>= 7
            out.write(bytes([zigzag]))

    @staticmethod
    def _decode_varint(in_stream: io.BytesIO) -> int:
        # Read first byte
        byte_data = in_stream.read(1)
        if not byte_data:
            raise RuntimeError("End of stream")

        b0 = byte_data[0]

        # Fast path: single byte (most common case)
        if b0 < 0x80:
            return DeltaVarintCompressor._zigzag_decode(b0)

        # Multi-byte case
        result = b0 & 0x7F
        shift = 7

        # Unroll first few iterations for better performance
        for _ in range(4):  # Handle up to 5 bytes total
            byte_data = in_stream.read(1)
            if not byte_data:
                raise RuntimeError("Unexpected end of input")

            b = byte_data[0]
            result |= (b & 0x7F) << shift

            if b < 0x80:
                return DeltaVarintCompressor._zigzag_decode(result)

            shift += 7
            if shift > 63:
                raise RuntimeError("Varint overflow")

        # Handle remaining bytes (rare case for very large numbers)
        while True:
            byte_data = in_stream.read(1)
            if not byte_data:
                raise RuntimeError("Unexpected end of input")

            b = byte_data[0]
            result |= (b & 0x7F) << shift

            if b < 0x80:
                break

            shift += 7
            if shift > 63:
                raise RuntimeError("Varint overflow")

        return DeltaVarintCompressor._zigzag_decode(result)

    @staticmethod
    def _zigzag_decode(value: int) -> int:
        """Fast ZigZag decoding using bit operations."""
        if value & 1:
            return -((value + 1) >> 1)
        else:
            return value >> 1
