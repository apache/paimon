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

"""Shared block decompression for Java global-index block trailers."""

import struct
import zlib

from pypaimon.globalindex.memory_slice_input import MemorySliceInput


COMPRESSION_NONE = 0
COMPRESSION_ZSTD = 1
COMPRESSION_LZ4 = 2
COMPRESSION_LZO = 3
BLOCK_TRAILER_LENGTH = 5
_HEADER_LENGTH = 8


def crc32c(data: bytes, compression_type: int) -> int:
    crc = zlib.crc32(data)
    crc = zlib.crc32(bytes([compression_type & 0xFF]), crc)
    return crc & 0xFFFFFFFF


def decompress_block(block_bytes: bytes, compression_type: int) -> bytes:
    if compression_type == COMPRESSION_NONE:
        return block_bytes
    if compression_type == COMPRESSION_ZSTD:
        return _decompress_zstd(block_bytes)
    if compression_type == COMPRESSION_LZ4:
        return _decompress_lz4(block_bytes)
    if compression_type == COMPRESSION_LZO:
        return _decompress_lzo(block_bytes)
    raise ValueError("Compression type %s not supported" % compression_type)


def decompress_block_with_trailer(block_and_trailer: bytes, block_length: int) -> bytes:
    if len(block_and_trailer) < block_length + BLOCK_TRAILER_LENGTH:
        raise ValueError("Block data too short to contain trailer.")

    block_bytes = block_and_trailer[:block_length]
    trailer = block_and_trailer[block_length:block_length + BLOCK_TRAILER_LENGTH]
    compression_type = trailer[0]
    expected_crc = struct.unpack('<I', trailer[1:5])[0]
    actual_crc = crc32c(block_bytes, compression_type)
    if actual_crc != expected_crc:
        raise ValueError(
            "CRC32 mismatch: expected %s, got %s" % (expected_crc, actual_crc))
    return decompress_block(block_bytes, compression_type)


def _decompress_zstd(block_bytes: bytes) -> bytes:
    import zstandard as zstd
    from io import BytesIO

    memory_input = MemorySliceInput(block_bytes)
    expected_len = memory_input.read_var_len_int()
    compressed = block_bytes[memory_input.position():]
    with zstd.ZstdDecompressor().stream_reader(BytesIO(compressed)) as reader:
        uncompressed = reader.read()
    if len(uncompressed) != expected_len:
        raise ValueError("Corrupted block, decompression failed.")
    return uncompressed


def _decompress_lz4(block_bytes: bytes) -> bytes:
    memory_input = MemorySliceInput(block_bytes)
    expected_len = memory_input.read_var_len_int()
    payload_start = memory_input.position()
    if len(block_bytes) - payload_start < _HEADER_LENGTH:
        raise ValueError("Compressed block data too short to contain header.")
    compressed_len, original_len = struct.unpack_from('<ii', block_bytes, payload_start)
    _validate_lengths(compressed_len, original_len)
    if original_len != expected_len:
        raise ValueError("Compressed block header has unexpected original length.")
    payload_start += _HEADER_LENGTH
    payload_end = payload_start + compressed_len
    if len(block_bytes) < payload_end:
        raise ValueError("Compressed block data is incomplete.")

    payload = block_bytes[payload_start:payload_end]
    uncompressed = _decompress_lz4_raw(payload, original_len)
    if len(uncompressed) != original_len:
        raise ValueError("Corrupted block, decompression failed.")
    return uncompressed


def _decompress_lzo(block_bytes: bytes) -> bytes:
    memory_input = MemorySliceInput(block_bytes)
    expected_len = memory_input.read_var_len_int()
    payload_start = memory_input.position()
    if len(block_bytes) - payload_start < _HEADER_LENGTH:
        raise ValueError("Compressed block data too short to contain header.")
    compressed_len, original_len = struct.unpack_from('<ii', block_bytes, payload_start)
    _validate_lengths(compressed_len, original_len)
    if original_len != expected_len:
        raise ValueError("Compressed block header has unexpected original length.")
    payload_start += _HEADER_LENGTH
    payload_end = payload_start + compressed_len
    if len(block_bytes) < payload_end:
        raise ValueError("Compressed block data is incomplete.")

    payload = block_bytes[payload_start:payload_end]
    uncompressed = _decompress_lzo_raw(payload, original_len)
    if len(uncompressed) != original_len:
        raise ValueError("Corrupted block, decompression failed.")
    return uncompressed


def _decompress_lz4_raw(payload: bytes, output_len: int) -> bytes:
    output = bytearray()
    pos = 0
    payload_len = len(payload)

    while pos < payload_len:
        token = payload[pos]
        pos += 1

        literal_len = token >> 4
        if literal_len == 15:
            length_part, pos = _read_lz4_length(payload, pos, payload_len)
            literal_len += length_part
        literal_end = pos + literal_len
        if literal_end > payload_len:
            raise ValueError("Corrupted LZ4 block, literal exceeds input.")
        output.extend(payload[pos:literal_end])
        pos = literal_end

        if pos == payload_len:
            break
        if pos + 2 > payload_len:
            raise ValueError("Corrupted LZ4 block, missing match offset.")
        offset = payload[pos] | (payload[pos + 1] << 8)
        pos += 2
        if offset <= 0 or offset > len(output):
            raise ValueError("Corrupted LZ4 block, invalid match offset.")

        match_len = token & 0x0F
        if match_len == 15:
            length_part, pos = _read_lz4_length(payload, pos, payload_len)
            match_len += length_part
        match_len += 4

        start = len(output) - offset
        for _ in range(match_len):
            output.append(output[start])
            start += 1
        if len(output) > output_len:
            raise ValueError("Corrupted LZ4 block, output exceeds expected length.")

    if len(output) != output_len:
        raise ValueError("Corrupted LZ4 block, unexpected output length.")
    return bytes(output)


def _decompress_lzo_raw(payload: bytes, output_len: int) -> bytes:
    if not payload:
        if output_len == 0:
            return b''
        raise ValueError("Corrupted LZO block, empty payload.")

    output = bytearray()
    pos = 0
    payload_len = len(payload)
    first_command = True
    last_literal_len = 0

    while pos < payload_len:
        first_command = True
        last_literal_len = 0
        while True:
            if pos >= payload_len:
                raise ValueError("Corrupted LZO block, missing command.")
            command = payload[pos]
            pos += 1

            match_len = 0
            match_offset = 0

            if (command & 0xF0) == 0:
                if last_literal_len == 0:
                    literal_len = command & 0x0F
                    if literal_len == 0:
                        literal_len, pos = _read_lzo_variable_length(
                            payload, pos, payload_len, 0x0F)
                    literal_len += 3
                elif last_literal_len <= 3:
                    if pos >= payload_len:
                        raise ValueError("Corrupted LZO block, missing match offset.")
                    match_len = 2
                    match_offset = ((command & 0x0C) >> 2) | (payload[pos] << 2)
                    pos += 1
                    literal_len = command & 0x03
                else:
                    if pos >= payload_len:
                        raise ValueError("Corrupted LZO block, missing match offset.")
                    match_len = 3
                    match_offset = (((command & 0x0C) >> 2)
                                    | (payload[pos] << 2)
                                    | 0x0800)
                    pos += 1
                    literal_len = command & 0x03
            elif first_command:
                literal_len = command - 17
            elif (command & 0xF0) == 0x10:
                match_len = command & 0x07
                if match_len == 0:
                    match_len, pos = _read_lzo_variable_length(
                        payload, pos, payload_len, 0x07)
                match_len += 2
                trailer, pos = _read_lzo_short(payload, pos, payload_len)
                match_offset = ((command & 0x08) << 11) + (trailer >> 2)
                if match_offset == 0:
                    break
                match_offset += 0x3FFF
                literal_len = trailer & 0x03
            elif (command & 0xE0) == 0x20:
                match_len = command & 0x1F
                if match_len == 0:
                    match_len, pos = _read_lzo_variable_length(
                        payload, pos, payload_len, 0x1F)
                match_len += 2
                trailer, pos = _read_lzo_short(payload, pos, payload_len)
                match_offset = trailer >> 2
                literal_len = trailer & 0x03
            elif (command & 0xC0) != 0:
                if pos >= payload_len:
                    raise ValueError("Corrupted LZO block, missing match offset.")
                match_len = ((command & 0xE0) >> 5) + 1
                match_offset = ((command & 0x1C) >> 2) | (payload[pos] << 3)
                pos += 1
                literal_len = command & 0x03
            else:
                raise ValueError("Corrupted LZO block, invalid command.")

            first_command = False
            if match_len < 0 or literal_len < 0:
                raise ValueError("Corrupted LZO block, invalid length.")

            if match_len:
                _copy_lzo_match(output, match_offset + 1, match_len, output_len)

            literal_end = pos + literal_len
            if literal_end > payload_len:
                raise ValueError("Corrupted LZO block, literal exceeds input.")
            output.extend(payload[pos:literal_end])
            pos = literal_end
            if len(output) > output_len:
                raise ValueError("Corrupted LZO block, output exceeds expected length.")
            last_literal_len = literal_len

    if len(output) != output_len:
        raise ValueError("Corrupted LZO block, unexpected output length.")
    return bytes(output)


def _read_lzo_variable_length(payload: bytes, pos: int, payload_len: int, base: int):
    length = base
    while True:
        if pos >= payload_len:
            raise ValueError("Corrupted LZO block, malformed length.")
        value = payload[pos]
        pos += 1
        if value != 0:
            return length + value, pos
        length += 255


def _read_lzo_short(payload: bytes, pos: int, payload_len: int):
    if pos + 2 > payload_len:
        raise ValueError("Corrupted LZO block, missing match trailer.")
    return payload[pos] | (payload[pos + 1] << 8), pos + 2


def _copy_lzo_match(output: bytearray, offset: int, match_len: int, output_len: int) -> None:
    if offset <= 0 or offset > len(output):
        raise ValueError("Corrupted LZO block, invalid match offset.")
    if len(output) + match_len > output_len:
        raise ValueError("Corrupted LZO block, output exceeds expected length.")

    start = len(output) - offset
    for _ in range(match_len):
        output.append(output[start])
        start += 1


def _read_lz4_length(payload: bytes, pos: int, payload_len: int):
    length = 0
    while True:
        if pos >= payload_len:
            raise ValueError("Corrupted LZ4 block, malformed length.")
        value = payload[pos]
        pos += 1
        length += value
        if value != 255:
            return length, pos


def _validate_lengths(compressed_len: int, original_len: int) -> None:
    if (original_len < 0
            or compressed_len < 0
            or (original_len == 0 and compressed_len != 0)
            or (original_len != 0 and compressed_len == 0)):
        raise ValueError("Input is corrupted, invalid length.")
