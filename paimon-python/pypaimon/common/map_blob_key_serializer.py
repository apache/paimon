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
from typing import Optional

from pypaimon.schema.data_types import AtomicType, DataType


class MapBlobKeySerializer:

    def __init__(self, type_name: str, struct_format: Optional[str] = None):
        self._type_name = type_name
        self._struct_format = struct_format
        self.fixed_length = -1 if struct_format is None else struct.calcsize(struct_format)

    def serialize(self, key) -> bytes:
        if self._struct_format is None:
            if not isinstance(key, str):
                raise ValueError(
                    f"MAP<X, BLOB> {self._type_name} key must be a string."
                )
            return key.encode('utf-8')

        if not isinstance(key, int) or isinstance(key, bool):
            raise ValueError(
                f"MAP<X, BLOB> {self._type_name} key must be an integer."
            )
        try:
            return struct.pack(self._struct_format, key)
        except struct.error as error:
            raise ValueError(
                f"MAP<X, BLOB> {self._type_name} key is out of range: {key}."
            ) from error

    def deserialize(self, data: bytes):
        if self._struct_format is None:
            try:
                return data.decode('utf-8')
            except UnicodeDecodeError as error:
                raise ValueError("Invalid MAP<X, BLOB> string key.") from error

        if len(data) != self.fixed_length:
            raise ValueError(
                f"Expected {self.fixed_length} key bytes, but found {len(data)}."
            )
        return struct.unpack(self._struct_format, data)[0]


def create_map_blob_key_serializer(data_type: DataType) -> MapBlobKeySerializer:
    if not isinstance(data_type, AtomicType):
        raise ValueError(f"Unsupported key type for MAP<X, BLOB>: {data_type}")

    type_name = data_type.type.upper()
    if type_name == 'TINYINT':
        return MapBlobKeySerializer(type_name, '<b')
    if type_name == 'SMALLINT':
        return MapBlobKeySerializer(type_name, '<h')
    if type_name in ('INT', 'INTEGER'):
        return MapBlobKeySerializer(type_name, '<i')
    if type_name == 'BIGINT':
        return MapBlobKeySerializer(type_name, '<q')
    if type_name == 'STRING' or type_name.startswith('CHAR') or type_name.startswith('VARCHAR'):
        return MapBlobKeySerializer(type_name)
    raise ValueError(f"Unsupported key type for MAP<X, BLOB>: {data_type}")
