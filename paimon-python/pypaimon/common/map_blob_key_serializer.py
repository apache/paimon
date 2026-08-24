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

import datetime
import struct
from decimal import Decimal as BigDecimal
from typing import Optional

from pypaimon.data.decimal import Decimal
from pypaimon.schema.data_types import AtomicType, DataType

_EPOCH_DATE = datetime.date(1970, 1, 1)


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


class BooleanMapBlobKeySerializer(MapBlobKeySerializer):

    def __init__(self):
        self.fixed_length = 1

    def serialize(self, key) -> bytes:
        if not isinstance(key, bool):
            raise ValueError("MAP<X, BLOB> BOOLEAN key must be a boolean.")
        return b'\x01' if key else b'\x00'

    def deserialize(self, data: bytes):
        if len(data) != self.fixed_length:
            raise ValueError(
                f"Expected {self.fixed_length} key bytes, but found {len(data)}."
            )
        if data[0] == 0:
            return False
        if data[0] == 1:
            return True
        raise ValueError("Invalid MAP<X, BLOB> boolean key.")


class BinaryMapBlobKeySerializer(MapBlobKeySerializer):

    def __init__(self, type_name: str):
        self._type_name = type_name
        self.fixed_length = -1

    def serialize(self, key) -> bytes:
        if not isinstance(key, bytes):
            raise ValueError(
                f"MAP<X, BLOB> {self._type_name} key must be bytes."
            )
        return key

    def deserialize(self, data: bytes):
        return data


class DecimalMapBlobKeySerializer(MapBlobKeySerializer):

    def __init__(self, type_name: str, precision: int, scale: int):
        self._type_name = type_name
        self._precision = precision
        self._scale = scale
        self.fixed_length = 8 if Decimal.is_compact_precision(precision) else -1

    def serialize(self, key) -> bytes:
        if not isinstance(key, BigDecimal):
            raise ValueError(
                f"MAP<X, BLOB> {self._type_name} key must be a decimal.Decimal."
            )
        decimal = Decimal.from_big_decimal(key, self._precision, self._scale)
        if decimal is None:
            raise ValueError(
                f"MAP<X, BLOB> {self._type_name} key exceeds declared precision."
            )
        if decimal.is_compact():
            return struct.pack('<q', decimal.to_unscaled_long())
        return decimal.to_unscaled_bytes()

    def deserialize(self, data: bytes):
        if self.fixed_length >= 0:
            if len(data) != self.fixed_length:
                raise ValueError(
                    f"Expected {self.fixed_length} key bytes, but found {len(data)}."
                )
            decimal = Decimal.from_unscaled_long(
                struct.unpack('<q', data)[0],
                self._precision,
                self._scale,
            )
        else:
            if not data:
                raise ValueError("Invalid MAP<X, BLOB> decimal key.")
            decimal = Decimal.from_unscaled_bytes(
                data,
                self._precision,
                self._scale,
            )
            if decimal is None:
                raise ValueError(
                    "MAP<X, BLOB> decimal key exceeds declared precision."
                )
        return decimal.to_big_decimal()


class DateMapBlobKeySerializer(MapBlobKeySerializer):

    def __init__(self):
        self.fixed_length = 4

    def serialize(self, key) -> bytes:
        if isinstance(key, datetime.datetime) or not isinstance(key, datetime.date):
            raise ValueError("MAP<X, BLOB> DATE key must be a datetime.date.")
        try:
            return struct.pack('<i', (key - _EPOCH_DATE).days)
        except struct.error as error:
            raise ValueError(f"MAP<X, BLOB> DATE key is out of range: {key}.") from error

    def deserialize(self, data: bytes):
        if len(data) != self.fixed_length:
            raise ValueError(
                f"Expected {self.fixed_length} key bytes, but found {len(data)}."
            )
        return _EPOCH_DATE + datetime.timedelta(days=struct.unpack('<i', data)[0])


class TimeMapBlobKeySerializer(MapBlobKeySerializer):

    def __init__(self, type_name: str):
        self._type_name = type_name
        self.fixed_length = 4

    def serialize(self, key) -> bytes:
        if not isinstance(key, datetime.time):
            raise ValueError(
                f"MAP<X, BLOB> {self._type_name} key must be a datetime.time."
            )
        millis = (
            (key.hour * 3600 + key.minute * 60 + key.second) * 1000
            + key.microsecond // 1000
        )
        return struct.pack('<i', millis)

    def deserialize(self, data: bytes):
        if len(data) != self.fixed_length:
            raise ValueError(
                f"Expected {self.fixed_length} key bytes, but found {len(data)}."
            )
        millis = struct.unpack('<i', data)[0]
        seconds, millis = divmod(millis, 1000)
        minutes, second = divmod(seconds, 60)
        hour, minute = divmod(minutes, 60)
        try:
            return datetime.time(hour, minute, second, millis * 1000)
        except ValueError as error:
            raise ValueError("Invalid MAP<X, BLOB> TIME key.") from error


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
    if type_name == 'BOOLEAN':
        return BooleanMapBlobKeySerializer()
    if type_name.startswith('DECIMAL'):
        precision, scale = Decimal.extract_decimal_precision_scale(type_name)
        return DecimalMapBlobKeySerializer(type_name, precision, scale)
    if type_name == 'DATE':
        return DateMapBlobKeySerializer()
    if type_name == 'TIME' or type_name.startswith('TIME('):
        return TimeMapBlobKeySerializer(type_name)
    if (
        type_name == 'BYTES'
        or type_name.startswith('BINARY')
        or type_name.startswith('VARBINARY')
    ):
        return BinaryMapBlobKeySerializer(type_name)
    if type_name == 'STRING' or type_name.startswith('CHAR') or type_name.startswith('VARCHAR'):
        return MapBlobKeySerializer(type_name)
    raise ValueError(f"Unsupported key type for MAP<X, BLOB>: {data_type}")
