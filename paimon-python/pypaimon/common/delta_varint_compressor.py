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
        # ZigZag encoding: maps signed integers to unsigned integers
        if value >= 0:
            zigzag = value << 1
        else:
            zigzag = ((-value) << 1) - 1

        # Varint encoding
        while zigzag >= 0x80:
            out.write(bytes([(zigzag & 0x7F) | 0x80]))
            zigzag >>= 7
        out.write(bytes([zigzag & 0x7F]))

    @staticmethod
    def _decode_varint(in_stream: io.BytesIO) -> int:
        result = 0
        shift = 0
        while True:
            byte_data = in_stream.read(1)
            if not byte_data:
                if shift == 0:
                    # Natural end of stream
                    raise RuntimeError("End of stream")
                else:
                    # Unexpected end in middle of varint
                    raise RuntimeError("Unexpected end of input")

            b = byte_data[0]
            result |= (b & 0x7F) << shift
            if (b & 0x80) == 0:
                break

            shift += 7
            if shift > 63:
                raise RuntimeError("Varint overflow")

        # ZigZag decoding: maps unsigned integers back to signed integers
        if result & 1:
            return -((result + 1) >> 1)
        else:
            return result >> 1
