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

"""Shared helpers for Java-compatible global index files."""

from dataclasses import dataclass
import struct
import uuid

from pypaimon.globalindex.block_compression import COMPRESSION_NONE, crc32c


@dataclass(frozen=True)
class BlockInfo:
    offset: int
    length: int


class PositionOutput:
    def __init__(self, output_stream):
        self._output_stream = output_stream
        self._pos = 0

    @property
    def pos(self) -> int:
        return self._pos

    def write(self, data: bytes) -> None:
        self._output_stream.write(data)
        self._pos += len(data)

    def close(self) -> None:
        self._output_stream.close()


def new_global_index_file_name(prefix: str) -> str:
    return "%s-global-index-%s.index" % (prefix, uuid.uuid4())


def write_uncompressed_block(output: PositionOutput, data: bytes) -> BlockInfo:
    offset = output.pos
    output.write(data)
    write_block_trailer(output, data, COMPRESSION_NONE)
    return BlockInfo(offset, len(data))


def write_block_trailer(
    output: PositionOutput,
    data: bytes,
    compression_type: int,
) -> None:
    output.write(
        struct.pack("<B", compression_type)
        + struct.pack("<I", crc32c(data, compression_type))
    )


def write_var_len_int(value: int) -> bytes:
    if value < 0:
        raise ValueError("negative value: v=%s" % value)
    result = bytearray()
    while value & ~0x7F:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


def write_var_len_long(value: int) -> bytes:
    if value < 0:
        raise ValueError("negative value: v=%s" % value)
    result = bytearray()
    while value & ~0x7F:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


def var_len_int_size(value: int) -> int:
    if value < 0:
        raise ValueError("negative value: v=%s" % value)
    size = 1
    while value & ~0x7F:
        value >>= 7
        size += 1
    return size


def var_len_long_size(value: int) -> int:
    if value < 0:
        raise ValueError("negative value: v=%s" % value)
    size = 1
    while value & ~0x7F:
        value >>= 7
        size += 1
    return size
