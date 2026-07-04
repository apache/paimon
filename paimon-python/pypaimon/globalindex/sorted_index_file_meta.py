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

"""Manifest-level min/max metadata for one sorted global index file."""

from dataclasses import dataclass
from typing import Optional
import struct


@dataclass
class SortedIndexFileMeta:
    """Metadata shared by sorted global indexes such as btree and bitmap."""

    FORMAT_VERSION_WITH_NULL_FLAGS = 1
    FIRST_KEY_IS_NULL = 1
    LAST_KEY_IS_NULL = 1 << 1

    first_key: Optional[bytes]
    last_key: Optional[bytes]
    has_nulls: bool

    def only_nulls(self) -> bool:
        return self.first_key is None and self.last_key is None

    def serialize(self) -> bytes:
        result = bytearray()
        null_key_flags = 0

        if self.first_key is None:
            result.extend(struct.pack('<I', 0))
            null_key_flags |= self.FIRST_KEY_IS_NULL
        else:
            result.extend(struct.pack('<I', len(self.first_key)))
            result.extend(self.first_key)

        if self.last_key is None:
            result.extend(struct.pack('<I', 0))
            null_key_flags |= self.LAST_KEY_IS_NULL
        else:
            result.extend(struct.pack('<I', len(self.last_key)))
            result.extend(self.last_key)

        result.extend(struct.pack('<B', 1 if self.has_nulls else 0))
        result.extend(struct.pack('<B', self.FORMAT_VERSION_WITH_NULL_FLAGS))
        result.extend(struct.pack('<B', null_key_flags))
        return bytes(result)

    @classmethod
    def deserialize(cls, data: bytes) -> 'SortedIndexFileMeta':
        offset = 0

        first_key_length = struct.unpack('<I', data[offset:offset + 4])[0]
        offset += 4
        first_key = data[offset:offset + first_key_length]
        offset += first_key_length

        last_key_length = struct.unpack('<I', data[offset:offset + 4])[0]
        offset += 4
        last_key = data[offset:offset + last_key_length]
        offset += last_key_length

        has_nulls = struct.unpack('<B', data[offset:offset + 1])[0] == 1
        offset += 1

        if len(data) - offset >= 2:
            format_version = struct.unpack('<B', data[offset:offset + 1])[0]
            offset += 1
            if format_version == cls.FORMAT_VERSION_WITH_NULL_FLAGS:
                null_key_flags = struct.unpack('<B', data[offset:offset + 1])[0]
                if null_key_flags & cls.FIRST_KEY_IS_NULL:
                    first_key = None
                if null_key_flags & cls.LAST_KEY_IS_NULL:
                    last_key = None
        elif first_key_length == 0 and last_key_length == 0 and has_nulls:
            first_key = None
            last_key = None

        return cls(first_key, last_key, has_nulls)
