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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import struct
from dataclasses import dataclass
from typing import List

from pypaimon.index.pk.primary_key_index_source_file import (
    PrimaryKeyIndexSourceFile,
)


@dataclass(frozen=True)
class PrimaryKeyIndexSourceMeta:
    data_level: int
    source_files: List[PrimaryKeyIndexSourceFile]

    VERSION = 1

    def __post_init__(self):
        if self.data_level <= 0:
            raise ValueError("Primary-key index data level must be positive.")
        if not self.source_files:
            raise ValueError("An index must reference source files.")
        object.__setattr__(self, "source_files", tuple(self.source_files))

    @staticmethod
    def from_index_file(index_file):
        meta = index_file.global_index_meta
        if meta is None or meta.source_meta is None:
            raise ValueError("Index file %s has no source metadata." % index_file.file_name)
        return PrimaryKeyIndexSourceMeta.deserialize(meta.source_meta)

    def serialize(self) -> bytes:
        parts = [struct.pack(">iii", self.VERSION, self.data_level, len(self.source_files))]
        for source in self.source_files:
            encoded = _encode_modified_utf8(source.file_name)
            if len(encoded) > 65535:
                raise ValueError("Primary-key index source file name is too long.")
            parts.append(struct.pack(">H", len(encoded)))
            parts.append(encoded)
            parts.append(struct.pack(">q", source.row_count))
        return b"".join(parts)

    @staticmethod
    def deserialize(data: bytes):
        try:
            if len(data) < 12:
                raise ValueError("Truncated index source metadata.")
            version, data_level, count = struct.unpack_from(">iii", data, 0)
            if version != PrimaryKeyIndexSourceMeta.VERSION:
                raise ValueError("Unsupported index source version: %s." % version)
            if count <= 0 or count > (len(data) - 12) // 10:
                raise ValueError("Invalid index source file count: %s." % count)
            offset = 12
            sources = []
            for _ in range(count):
                length = struct.unpack_from(">H", data, offset)[0]
                offset += 2
                end = offset + length
                if end + 8 > len(data):
                    raise ValueError("Truncated index source metadata.")
                name = _decode_modified_utf8(data[offset:end])
                row_count = struct.unpack_from(">q", data, end)[0]
                offset = end + 8
                sources.append(PrimaryKeyIndexSourceFile(name, row_count))
            if offset != len(data):
                raise ValueError("Unexpected trailing bytes in index source metadata.")
            return PrimaryKeyIndexSourceMeta(data_level, sources)
        except (UnicodeDecodeError, struct.error) as exc:
            raise ValueError("Failed to deserialize index source metadata.") from exc


def _encode_modified_utf8(value: str) -> bytes:
    encoded = bytearray()
    utf16 = value.encode("utf-16-be", "surrogatepass")
    for offset in range(0, len(utf16), 2):
        code = (utf16[offset] << 8) | utf16[offset + 1]
        if 0x0001 <= code <= 0x007F:
            encoded.append(code)
        elif code <= 0x07FF:
            encoded.extend((0xC0 | ((code >> 6) & 0x1F), 0x80 | (code & 0x3F)))
        else:
            encoded.extend((0xE0 | ((code >> 12) & 0x0F),
                            0x80 | ((code >> 6) & 0x3F),
                            0x80 | (code & 0x3F)))
    return bytes(encoded)


def _decode_modified_utf8(data: bytes) -> str:
    units = bytearray()
    offset = 0
    while offset < len(data):
        first = data[offset]
        if first & 0x80 == 0:
            if first == 0:
                raise UnicodeDecodeError("modified-utf8", data, offset, offset + 1,
                                         "NUL must use two-byte encoding")
            code = first
            offset += 1
        elif first & 0xE0 == 0xC0 and offset + 1 < len(data):
            code = ((first & 0x1F) << 6) | (data[offset + 1] & 0x3F)
            offset += 2
        elif first & 0xF0 == 0xE0 and offset + 2 < len(data):
            code = ((first & 0x0F) << 12) | ((data[offset + 1] & 0x3F) << 6) \
                | (data[offset + 2] & 0x3F)
            offset += 3
        else:
            raise UnicodeDecodeError("modified-utf8", data, offset, offset + 1,
                                     "invalid byte sequence")
        units.extend(((code >> 8) & 0xFF, code & 0xFF))
    return bytes(units).decode("utf-16-be", "surrogatepass")
