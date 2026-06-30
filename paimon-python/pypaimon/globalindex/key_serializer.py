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

"""Key serializer for global indexes."""

from abc import ABC, abstractmethod
import datetime
from decimal import Decimal, ROUND_HALF_UP
import math
import re
from typing import Callable, Tuple
import struct

from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.data_types import DataType


class KeySerializer(ABC):
    """Interface for serializing, deserializing, and comparing global index keys."""

    @abstractmethod
    def serialize(self, key: object) -> bytes:
        """Serialize a key to bytes."""
        pass

    @abstractmethod
    def deserialize(self, data: bytes) -> object:
        """Deserialize bytes to a key."""
        pass

    @abstractmethod
    def create_comparator(self) -> Callable[[object, object], int]:
        """Create a comparator function for keys."""
        pass


class StringSerializer(KeySerializer):
    """Serializer for STRING type."""

    def serialize(self, key: object) -> bytes:
        if isinstance(key, str):
            return key.encode('utf-8')
        return str(key).encode('utf-8')

    def deserialize(self, data: bytes) -> object:
        return data.decode('utf-8')

    def create_comparator(self) -> Callable[[object, object], int]:
        def compare(a: object, b: object) -> int:
            str_a = a if isinstance(a, str) else str(a)
            str_b = b if isinstance(b, str) else str(b)
            if str_a < str_b:
                return -1
            if str_a > str_b:
                return 1
            return 0
        return compare


class LongSerializer(KeySerializer):
    """Serializer for BIGINT type."""

    def serialize(self, key: object) -> bytes:
        return struct.pack('<q', int(key))

    def deserialize(self, data: bytes) -> object:
        return struct.unpack('<q', data)[0]

    def create_comparator(self) -> Callable[[object, object], int]:
        def compare(a: object, b: object) -> int:
            long_a = int(a)
            long_b = int(b)
            if long_a < long_b:
                return -1
            if long_a > long_b:
                return 1
            return 0
        return compare


class ByteSerializer(KeySerializer):
    """Serializer for TINYINT type."""

    def serialize(self, key: object) -> bytes:
        return struct.pack('<b', int(key))

    def deserialize(self, data: bytes) -> object:
        return struct.unpack('<b', data)[0]

    def create_comparator(self) -> Callable[[object, object], int]:
        return _numeric_compare


class ShortSerializer(KeySerializer):
    """Serializer for SMALLINT type."""

    def serialize(self, key: object) -> bytes:
        return struct.pack('<h', int(key))

    def deserialize(self, data: bytes) -> object:
        return struct.unpack('<h', data)[0]

    def create_comparator(self) -> Callable[[object, object], int]:
        return _numeric_compare


class IntSerializer(KeySerializer):
    """Serializer for INT type."""

    def serialize(self, key: object) -> bytes:
        return struct.pack('<i', int(key))

    def deserialize(self, data: bytes) -> object:
        return struct.unpack('<i', data)[0]

    def create_comparator(self) -> Callable[[object, object], int]:
        def compare(a: object, b: object) -> int:
            int_a = int(a)
            int_b = int(b)
            if int_a < int_b:
                return -1
            if int_a > int_b:
                return 1
            return 0
        return compare


class BooleanSerializer(KeySerializer):
    """Serializer for BOOLEAN type."""

    def serialize(self, key: object) -> bytes:
        return b'\x01' if bool(key) else b'\x00'

    def deserialize(self, data: bytes) -> object:
        return data[0] == 1

    def create_comparator(self) -> Callable[[object, object], int]:
        def compare(a: object, b: object) -> int:
            return _cmp(bool(a), bool(b))
        return compare


class FloatSerializer(KeySerializer):
    """Serializer for FLOAT type."""

    def serialize(self, key: object) -> bytes:
        return struct.pack('<f', float(key))

    def deserialize(self, data: bytes) -> object:
        return struct.unpack('<f', data)[0]

    def create_comparator(self) -> Callable[[object, object], int]:
        return _float_compare


class DoubleSerializer(KeySerializer):
    """Serializer for DOUBLE type."""

    def serialize(self, key: object) -> bytes:
        return struct.pack('<d', float(key))

    def deserialize(self, data: bytes) -> object:
        return struct.unpack('<d', data)[0]

    def create_comparator(self) -> Callable[[object, object], int]:
        return _float_compare


class DecimalSerializer(KeySerializer):
    """Serializer for DECIMAL type."""

    def __init__(self, precision: int, scale: int):
        self._precision = precision
        self._scale = scale

    def serialize(self, key: object) -> bytes:
        unscaled = _decimal_unscaled(key, self._scale)
        if self._precision <= 18:
            return struct.pack('<q', unscaled)
        return _signed_big_endian_bytes(unscaled)

    def deserialize(self, data: bytes) -> object:
        if self._precision <= 18:
            unscaled = struct.unpack('<q', data)[0]
        else:
            unscaled = int.from_bytes(data, byteorder='big', signed=True)
        return Decimal(unscaled).scaleb(-self._scale)

    def create_comparator(self) -> Callable[[object, object], int]:
        def compare(a: object, b: object) -> int:
            return _cmp(Decimal(a), Decimal(b))
        return compare


class DateSerializer(IntSerializer):
    """Serializer for DATE type."""

    def serialize(self, key: object) -> bytes:
        return struct.pack('<i', _date_to_epoch_days(key))

    def deserialize(self, data: bytes) -> object:
        days = struct.unpack('<i', data)[0]
        return datetime.date(1970, 1, 1) + datetime.timedelta(days=days)

    def create_comparator(self) -> Callable[[object, object], int]:
        def compare(a: object, b: object) -> int:
            return _cmp(_date_to_epoch_days(a), _date_to_epoch_days(b))
        return compare


class TimeSerializer(IntSerializer):
    """Serializer for TIME type."""

    def serialize(self, key: object) -> bytes:
        return struct.pack('<i', _time_to_millis(key))

    def deserialize(self, data: bytes) -> object:
        millis = struct.unpack('<i', data)[0]
        seconds, millis = divmod(millis, 1000)
        minutes, second = divmod(seconds, 60)
        hour, minute = divmod(minutes, 60)
        return datetime.time(hour, minute, second, millis * 1000)

    def create_comparator(self) -> Callable[[object, object], int]:
        def compare(a: object, b: object) -> int:
            return _cmp(_time_to_millis(a), _time_to_millis(b))
        return compare


class TimestampSerializer(KeySerializer):
    """Serializer for TIMESTAMP and TIMESTAMP WITH LOCAL TIME ZONE types."""

    def __init__(self, precision: int):
        self._precision = precision

    def serialize(self, key: object) -> bytes:
        millis, nano_of_millisecond = _timestamp_to_millis_nanos(
            key, self._precision)
        if self._precision <= 3:
            return struct.pack('<q', millis)
        return struct.pack('<q', millis) + _write_var_len_int(nano_of_millisecond)

    def deserialize(self, data: bytes) -> object:
        millis = struct.unpack('<q', data[:8])[0]
        nano_of_millisecond = 0
        if self._precision > 3 and len(data) > 8:
            nano_of_millisecond, _ = _read_var_len_int(data, 8)
        total_micros = millis * 1000 + nano_of_millisecond // 1000
        return (
            datetime.datetime(1970, 1, 1)
            + datetime.timedelta(microseconds=total_micros)
        )

    def create_comparator(self) -> Callable[[object, object], int]:
        def compare(a: object, b: object) -> int:
            return _cmp(
                _timestamp_to_millis_nanos(a, self._precision),
                _timestamp_to_millis_nanos(b, self._precision),
            )
        return compare


def _cmp(a, b) -> int:
    if a < b:
        return -1
    if a > b:
        return 1
    return 0


def _numeric_compare(a: object, b: object) -> int:
    return _cmp(int(a), int(b))


def _float_compare(a: object, b: object) -> int:
    left = float(a)
    right = float(b)
    left_nan = math.isnan(left)
    right_nan = math.isnan(right)
    if left_nan or right_nan:
        if left_nan and right_nan:
            return 0
        return 1 if left_nan else -1
    return _cmp(left, right)


def _parse_decimal_params(type_name: str) -> Tuple[int, int]:
    if type_name in ("DECIMAL", "NUMERIC"):
        return 10, 0
    match = re.fullmatch(r'(?:DECIMAL|NUMERIC)\((\d+),\s*(\d+)\)', type_name)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = re.fullmatch(r'(?:DECIMAL|NUMERIC)\((\d+)\)', type_name)
    if match:
        return int(match.group(1)), 0
    raise ValueError(f"Invalid decimal type: {type_name}")


def _parse_precision(type_name: str, default: int) -> int:
    match = re.search(r'\((\d+)\)', type_name)
    return int(match.group(1)) if match else default


def _decimal_unscaled(value: object, scale: int) -> int:
    decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    quant = Decimal(1).scaleb(-scale)
    rounded = decimal_value.quantize(quant, rounding=ROUND_HALF_UP)
    return int(rounded.scaleb(scale))


def _signed_big_endian_bytes(value: int) -> bytes:
    length = max(1, (value.bit_length() + 8) // 8)
    while True:
        try:
            encoded = value.to_bytes(length, byteorder='big', signed=True)
            break
        except OverflowError:
            length += 1
    while len(encoded) > 1:
        if encoded[0] == 0x00 and encoded[1] < 0x80:
            encoded = encoded[1:]
        elif encoded[0] == 0xFF and encoded[1] >= 0x80:
            encoded = encoded[1:]
        else:
            break
    return encoded


def _date_to_epoch_days(value: object) -> int:
    if isinstance(value, datetime.datetime):
        value = value.date()
    if isinstance(value, datetime.date):
        return (value - datetime.date(1970, 1, 1)).days
    return int(value)


def _time_to_millis(value: object) -> int:
    if isinstance(value, datetime.time):
        return (
            ((value.hour * 60 + value.minute) * 60 + value.second) * 1000
            + value.microsecond // 1000
        )
    return int(value)


def _timestamp_to_millis_nanos(
    value: object, precision: int = 6
) -> Tuple[int, int]:
    if hasattr(value, "get_millisecond") and hasattr(value, "get_nano_of_millisecond"):
        return int(value.get_millisecond()), int(value.get_nano_of_millisecond())
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            epoch = datetime.datetime(1970, 1, 1)
        else:
            epoch = datetime.datetime(1970, 1, 1, tzinfo=value.tzinfo)
        delta = value - epoch
        total_micros = (
            delta.days * 86_400_000_000
            + delta.seconds * 1_000_000
            + delta.microseconds
        )
        millis, micros_in_milli = divmod(total_micros, 1000)
        return millis, micros_in_milli * 1000
    if precision <= 3:
        return int(value), 0
    micros = int(value)
    millis, micros_in_milli = divmod(micros, 1000)
    return millis, micros_in_milli * 1000


def _write_var_len_int(value: int) -> bytes:
    if value < 0:
        raise ValueError("negative value: v=%s" % value)
    result = bytearray()
    while value & ~0x7F:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


def _read_var_len_int(data: bytes, offset: int = 0):
    shift = 0
    result = 0
    while True:
        b = data[offset]
        offset += 1
        result |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            return result, offset
        shift += 7


def create_serializer(data_type: DataType) -> KeySerializer:
    if not isinstance(data_type, AtomicType):
        raise ValueError(
            f"Key serializer only support AtomicType yet, meet {data_type.__class__}")
    type_name = data_type.type.upper()
    if type_name == 'BOOLEAN':
        return BooleanSerializer()
    if type_name == 'TINYINT':
        return ByteSerializer()
    if type_name == 'SMALLINT':
        return ShortSerializer()
    if type_name in ('CHAR', 'VARCHAR', 'STRING') or type_name.startswith('CHAR') or type_name.startswith('VARCHAR'):
        return StringSerializer()
    if type_name in ('INT', 'INTEGER'):
        return IntSerializer()
    if type_name == 'BIGINT':
        return LongSerializer()
    if type_name == 'FLOAT':
        return FloatSerializer()
    if type_name == 'DOUBLE':
        return DoubleSerializer()
    if type_name.startswith('DECIMAL') or type_name.startswith('NUMERIC'):
        precision, scale = _parse_decimal_params(type_name)
        return DecimalSerializer(precision, scale)
    if type_name == 'DATE':
        return DateSerializer()
    if type_name.startswith('TIME') and not type_name.startswith('TIMESTAMP'):
        return TimeSerializer()
    if type_name.startswith('TIMESTAMP'):
        return TimestampSerializer(_parse_precision(type_name, 6))
    raise ValueError(f"DataType: {data_type} is not supported by global index now.")
