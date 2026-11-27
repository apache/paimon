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

import struct


FOOTER_SIZE = 32
MAGIC = 0x524F5753
VERSION_PLAIN = 1
VERSION_WITH_SCHEMA = 2
SCHEMA_HEADER_SIZE = 4
MAX_PHYSICAL_SCHEMA_SIZE = 64 * 1024 * 1024


def compress_row_schema(data, level):
    import zstandard as zstd

    if not 0 < len(data) <= MAX_PHYSICAL_SCHEMA_SIZE:
        raise ValueError(f"Invalid row file physical schema length: {len(data)}")
    compressed = zstd.ZstdCompressor(level=level).compress(data)
    if len(compressed) > MAX_PHYSICAL_SCHEMA_SIZE:
        raise ValueError(f"Compressed row file physical schema is too large: {len(compressed)}")
    return struct.pack('<I', len(data)) + compressed


def decompress_row_schema(block):
    import zstandard as zstd

    if not SCHEMA_HEADER_SIZE < len(block) <= SCHEMA_HEADER_SIZE + MAX_PHYSICAL_SCHEMA_SIZE:
        raise IOError(f"Invalid row file physical schema length: {len(block)}")
    raw_length = struct.unpack_from('<I', block)[0]
    if not 0 < raw_length <= MAX_PHYSICAL_SCHEMA_SIZE:
        raise IOError(f"Invalid row file uncompressed physical schema length: {raw_length}")
    compressed = memoryview(block)[SCHEMA_HEADER_SIZE:]
    try:
        # zstandard ignores max_output_size when the frame declares its own size.
        frame_size = zstd.get_frame_parameters(compressed).content_size
        if frame_size not in (raw_length, zstd.CONTENTSIZE_UNKNOWN):
            raise IOError("Row file physical schema length does not match its header")
        data = zstd.ZstdDecompressor().decompress(
            compressed, max_output_size=raw_length, allow_extra_data=False)
    except zstd.ZstdError as error:
        raise IOError("Invalid row file compressed physical schema") from error
    if len(data) != raw_length:
        raise IOError("Row file physical schema length does not match its header")
    return data


def read_row_footer(stream, file_size):
    if file_size < FOOTER_SIZE:
        raise IOError(f"Invalid row file: {file_size} bytes hold no {FOOTER_SIZE}-byte footer")

    stream.seek(file_size - FOOTER_SIZE)
    footer = stream.read(FOOTER_SIZE)
    if len(footer) != FOOTER_SIZE:
        raise IOError("Invalid row file: cannot read footer")

    magic = struct.unpack_from('<I', footer, 28)[0]
    if magic != MAGIC:
        raise IOError(f"Invalid row file magic: expected 0x{MAGIC:08X}, got 0x{magic:08X}")
    return footer
