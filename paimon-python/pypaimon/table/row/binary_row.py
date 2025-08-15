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

import struct
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, List

from pypaimon.schema.data_types import AtomicType, DataField, DataType
from pypaimon.table.row.row_kind import RowKind


@dataclass
class BinaryRow:
    values: List[Any]
    fields: List[DataField]
    row_kind: RowKind = RowKind.INSERT

    def to_dict(self):
        return {self.fields[i].name: self.values[i] for i in range(len(self.fields))}


class BinaryRowDeserializer:
    HEADER_SIZE_IN_BITS = 8
    MAX_FIX_PART_DATA_SIZE = 7
    HIGHEST_FIRST_BIT = 0x80 << 56
    HIGHEST_SECOND_TO_EIGHTH_BIT = 0x7F << 56

    @classmethod
    def from_bytes(
            cls,
            bytes_data: bytes,
            data_fields: List[DataField]
    ) -> BinaryRow:
        if not bytes_data:
            return BinaryRow([], data_fields)

        arity = len(data_fields)
        actual_data = bytes_data
        if len(bytes_data) >= 4:
            arity_from_bytes = struct.unpack('>i', bytes_data[:4])[0]
            if 0 < arity_from_bytes < 1000:
                actual_data = bytes_data[4:]

        fields = []
        null_bits_size_in_bytes = cls._calculate_bit_set_width_in_bytes(arity)
        for i, data_field in enumerate(data_fields):
            value = None
            if not cls._is_null_at(actual_data, 0, i):
                value = cls._parse_field_value(actual_data, 0, null_bits_size_in_bytes, i, data_field.type)
            fields.append(value)

        return BinaryRow(fields, data_fields, RowKind(actual_data[0]))

    @classmethod
    def _calculate_bit_set_width_in_bytes(cls, arity: int) -> int:
        return ((arity + 63 + cls.HEADER_SIZE_IN_BITS) // 64) * 8

    @classmethod
    def _is_null_at(cls, bytes_data: bytes, offset: int, pos: int) -> bool:
        index = pos + cls.HEADER_SIZE_IN_BITS
        byte_index = offset + (index // 8)
        bit_index = index % 8
        return (bytes_data[byte_index] & (1 << bit_index)) != 0

    @classmethod
    def _parse_field_value(
            cls,
            bytes_data: bytes,
            base_offset: int,
            null_bits_size_in_bytes: int,
            pos: int,
            data_type: DataType
    ) -> Any:
        if not isinstance(data_type, AtomicType):
            raise ValueError(f"BinaryRow only support AtomicType yet, meet {data_type.__class__}")
        field_offset = base_offset + null_bits_size_in_bytes + pos * 8
        if field_offset >= len(bytes_data):
            raise ValueError(f"Field offset {field_offset} exceeds data length {len(bytes_data)}")
        type_name = data_type.type.upper()

        if type_name in ['BOOLEAN', 'BOOL']:
            return cls._parse_boolean(bytes_data, field_offset)
        elif type_name in ['TINYINT', 'BYTE']:
            return cls._parse_byte(bytes_data, field_offset)
        elif type_name in ['SMALLINT', 'SHORT']:
            return cls._parse_short(bytes_data, field_offset)
        elif type_name in ['INT', 'INTEGER']:
            return cls._parse_int(bytes_data, field_offset)
        elif type_name in ['BIGINT', 'LONG']:
            return cls._parse_long(bytes_data, field_offset)
        elif type_name in ['FLOAT', 'REAL']:
            return cls._parse_float(bytes_data, field_offset)
        elif type_name in ['DOUBLE']:
            return cls._parse_double(bytes_data, field_offset)
        elif type_name in ['VARCHAR', 'STRING', 'CHAR']:
            return cls._parse_string(bytes_data, base_offset, field_offset)
        elif type_name in ['BINARY', 'VARBINARY', 'BYTES']:
            return cls._parse_binary(bytes_data, base_offset, field_offset)
        elif type_name in ['DECIMAL', 'NUMERIC']:
            return cls._parse_decimal(bytes_data, base_offset, field_offset, data_type)
        elif type_name in ['TIMESTAMP', 'TIMESTAMP_WITHOUT_TIME_ZONE']:
            return cls._parse_timestamp(bytes_data, base_offset, field_offset, data_type)
        elif type_name in ['DATE']:
            return cls._parse_date(bytes_data, field_offset)
        elif type_name in ['TIME', 'TIME_WITHOUT_TIME_ZONE']:
            return cls._parse_time(bytes_data, field_offset)
        else:
            return cls._parse_string(bytes_data, base_offset, field_offset)

    @classmethod
    def _parse_boolean(cls, bytes_data: bytes, field_offset: int) -> bool:
        return bytes_data[field_offset] != 0

    @classmethod
    def _parse_byte(cls, bytes_data: bytes, field_offset: int) -> int:
        return struct.unpack('<b', bytes_data[field_offset:field_offset + 1])[0]

    @classmethod
    def _parse_short(cls, bytes_data: bytes, field_offset: int) -> int:
        return struct.unpack('<h', bytes_data[field_offset:field_offset + 2])[0]

    @classmethod
    def _parse_int(cls, bytes_data: bytes, field_offset: int) -> int:
        if field_offset + 4 > len(bytes_data):
            raise ValueError(f"Not enough bytes for INT: need 4, have {len(bytes_data) - field_offset}")
        return struct.unpack('<i', bytes_data[field_offset:field_offset + 4])[0]

    @classmethod
    def _parse_long(cls, bytes_data: bytes, field_offset: int) -> int:
        if field_offset + 8 > len(bytes_data):
            raise ValueError(f"Not enough bytes for LONG: need 8, have {len(bytes_data) - field_offset}")
        return struct.unpack('<q', bytes_data[field_offset:field_offset + 8])[0]

    @classmethod
    def _parse_float(cls, bytes_data: bytes, field_offset: int) -> float:
        return struct.unpack('<f', bytes_data[field_offset:field_offset + 4])[0]

    @classmethod
    def _parse_double(cls, bytes_data: bytes, field_offset: int) -> float:
        if field_offset + 8 > len(bytes_data):
            raise ValueError(f"Not enough bytes for DOUBLE: need 8, have {len(bytes_data) - field_offset}")
        return struct.unpack('<d', bytes_data[field_offset:field_offset + 8])[0]

    @classmethod
    def _parse_string(cls, bytes_data: bytes, base_offset: int, field_offset: int) -> str:
        if field_offset + 8 > len(bytes_data):
            raise ValueError(f"Not enough bytes for STRING offset: need 8, have {len(bytes_data) - field_offset}")

        offset_and_len = struct.unpack('<q', bytes_data[field_offset:field_offset + 8])[0]
        mark = offset_and_len & cls.HIGHEST_FIRST_BIT
        if mark == 0:
            sub_offset = (offset_and_len >> 32) & 0xFFFFFFFF
            length = offset_and_len & 0xFFFFFFFF
            actual_string_offset = base_offset + sub_offset
            if actual_string_offset + length > len(bytes_data):
                raise ValueError(
                    f"String data out of bounds: actual_offset={actual_string_offset}, length={length}, "
                    f"total_length={len(bytes_data)}")
            string_data = bytes_data[actual_string_offset:actual_string_offset + length]
            return string_data.decode('utf-8')
        else:
            length = (offset_and_len & cls.HIGHEST_SECOND_TO_EIGHTH_BIT) >> 56
            start_offset = field_offset
            if start_offset + length > len(bytes_data):
                raise ValueError(f"Compact string data out of bounds: length={length}")
            string_data = bytes_data[start_offset:start_offset + length]
            return string_data.decode('utf-8')

    @classmethod
    def _parse_binary(cls, bytes_data: bytes, base_offset: int, field_offset: int) -> bytes:
        offset_and_len = struct.unpack('<q', bytes_data[field_offset:field_offset + 8])[0]
        mark = offset_and_len & cls.HIGHEST_FIRST_BIT
        if mark == 0:
            sub_offset = (offset_and_len >> 32) & 0xFFFFFFFF
            length = offset_and_len & 0xFFFFFFFF
            return bytes_data[base_offset + sub_offset:base_offset + sub_offset + length]
        else:
            length = (offset_and_len & cls.HIGHEST_SECOND_TO_EIGHTH_BIT) >> 56
            return bytes_data[field_offset + 1:field_offset + 1 + length]

    @classmethod
    def _parse_decimal(cls, bytes_data: bytes, base_offset: int, field_offset: int, data_type: DataType) -> Decimal:
        unscaled_long = struct.unpack('<q', bytes_data[field_offset:field_offset + 8])[0]
        type_str = str(data_type)
        if '(' in type_str and ')' in type_str:
            try:
                precision_scale = type_str.split('(')[1].split(')')[0]
                if ',' in precision_scale:
                    scale = int(precision_scale.split(',')[1])
                else:
                    scale = 0
            except:
                scale = 0
        else:
            scale = 0
        return Decimal(unscaled_long) / (10 ** scale)

    @classmethod
    def _parse_timestamp(cls, bytes_data: bytes, base_offset: int, field_offset: int, data_type: DataType) -> datetime:
        millis = struct.unpack('<q', bytes_data[field_offset:field_offset + 8])[0]
        return datetime.fromtimestamp(millis / 1000.0, tz=timezone.utc)

    @classmethod
    def _parse_date(cls, bytes_data: bytes, field_offset: int) -> datetime:
        days = struct.unpack('<i', bytes_data[field_offset:field_offset + 4])[0]
        return datetime(1970, 1, 1) + timedelta(days=days)

    @classmethod
    def _parse_time(cls, bytes_data: bytes, field_offset: int) -> datetime:
        millis = struct.unpack('<i', bytes_data[field_offset:field_offset + 4])[0]
        seconds = millis // 1000
        microseconds = (millis % 1000) * 1000
        return datetime(1970, 1, 1).replace(
            hour=seconds // 3600,
            minute=(seconds % 3600) // 60,
            second=seconds % 60,
            microsecond=microseconds
        )


class BinaryRowSerializer:
    HEADER_SIZE_IN_BITS = 8
    MAX_FIX_PART_DATA_SIZE = 7
    HIGHEST_FIRST_BIT = 0x80 << 56
    HIGHEST_SECOND_TO_EIGHTH_BIT = 0x7F << 56

    @classmethod
    def to_bytes(cls, binary_row: BinaryRow) -> bytes:
        if not binary_row.values:
            return b''

        arity = len(binary_row.fields)
        null_bits_size_in_bytes = cls._calculate_bit_set_width_in_bytes(arity)
        fixed_part_size = null_bits_size_in_bytes + arity * 8
        fixed_part = bytearray(fixed_part_size)
        fixed_part[0] = binary_row.row_kind.value

        for i, value in enumerate(binary_row.values):
            if value is None:
                cls._set_null_bit(fixed_part, 0, i)

        variable_data = []
        variable_offsets = []
        current_offset = fixed_part_size

        for i, (value, field) in enumerate(zip(binary_row.values, binary_row.fields)):
            if value is None:
                struct.pack_into('<q', fixed_part, null_bits_size_in_bytes + i * 8, 0)
                variable_data.append(b'')
                variable_offsets.append(0)
                continue

            field_offset = null_bits_size_in_bytes + i * 8
            if not isinstance(field.type, AtomicType):
                raise ValueError(f"BinaryRow only support AtomicType yet, meet {field.type.__class__}")
            if field.type.type.upper() in ['VARCHAR', 'STRING', 'CHAR', 'BINARY', 'VARBINARY', 'BYTES']:
                if field.type.type.upper() in ['VARCHAR', 'STRING', 'CHAR']:
                    if isinstance(value, str):
                        value_bytes = value.encode('utf-8')
                    else:
                        value_bytes = bytes(value)
                else:
                    if isinstance(value, bytes):
                        value_bytes = value
                    else:
                        value_bytes = bytes(value)

                length = len(value_bytes)
                if length <= cls.MAX_FIX_PART_DATA_SIZE:
                    fixed_part[field_offset:field_offset + length] = value_bytes
                    for j in range(length, 8):
                        fixed_part[field_offset + j] = 0
                    packed_long = struct.unpack_from('<q', fixed_part, field_offset)[0]

                    offset_and_len = packed_long | (length << 56) | cls.HIGHEST_FIRST_BIT
                    if offset_and_len > 0x7FFFFFFFFFFFFFFF:
                        offset_and_len = offset_and_len - 0x10000000000000000
                    struct.pack_into('<q', fixed_part, field_offset, offset_and_len)
                    variable_data.append(b'')
                    variable_offsets.append(0)
                else:
                    variable_data.append(value_bytes)
                    variable_offsets.append(current_offset)
                    current_offset += len(value_bytes)
                    offset_and_len = (variable_offsets[i] << 32) | len(variable_data[i])
                    struct.pack_into('<q', fixed_part, null_bits_size_in_bytes + i * 8, offset_and_len)
            else:
                if field.type.type.upper() in ['BOOLEAN', 'BOOL']:
                    struct.pack_into('<b', fixed_part, field_offset, 1 if value else 0)
                elif field.type.type.upper() in ['TINYINT', 'BYTE']:
                    struct.pack_into('<b', fixed_part, field_offset, value)
                elif field.type.type.upper() in ['SMALLINT', 'SHORT']:
                    struct.pack_into('<h', fixed_part, field_offset, value)
                elif field.type.type.upper() in ['INT', 'INTEGER']:
                    struct.pack_into('<i', fixed_part, field_offset, value)
                elif field.type.type.upper() in ['BIGINT', 'LONG']:
                    struct.pack_into('<q', fixed_part, field_offset, value)
                elif field.type.type.upper() in ['FLOAT', 'REAL']:
                    struct.pack_into('<f', fixed_part, field_offset, value)
                elif field.type.type.upper() in ['DOUBLE']:
                    struct.pack_into('<d', fixed_part, field_offset, value)
                else:
                    field_bytes = cls._serialize_field_value(value, field.type)
                    fixed_part[field_offset:field_offset + len(field_bytes)] = field_bytes

                variable_data.append(b'')
                variable_offsets.append(0)

        result = bytes(fixed_part) + b''.join(variable_data)
        return result

    @classmethod
    def _calculate_bit_set_width_in_bytes(cls, arity: int) -> int:
        return ((arity + 63 + cls.HEADER_SIZE_IN_BITS) // 64) * 8

    @classmethod
    def _set_null_bit(cls, bytes_data: bytearray, offset: int, pos: int) -> None:
        index = pos + cls.HEADER_SIZE_IN_BITS
        byte_index = offset + (index // 8)
        bit_index = index % 8
        bytes_data[byte_index] |= (1 << bit_index)

    @classmethod
    def _serialize_field_value(cls, value: Any, data_type: AtomicType) -> bytes:
        type_name = data_type.type.upper()

        if type_name in ['BOOLEAN', 'BOOL']:
            return cls._serialize_boolean(value)
        elif type_name in ['TINYINT', 'BYTE']:
            return cls._serialize_byte(value)
        elif type_name in ['SMALLINT', 'SHORT']:
            return cls._serialize_short(value)
        elif type_name in ['INT', 'INTEGER']:
            return cls._serialize_int(value)
        elif type_name in ['BIGINT', 'LONG']:
            return cls._serialize_long(value)
        elif type_name in ['FLOAT', 'REAL']:
            return cls._serialize_float(value)
        elif type_name in ['DOUBLE']:
            return cls._serialize_double(value)
        elif type_name in ['VARCHAR', 'STRING', 'CHAR']:
            return cls._serialize_string(value)
        elif type_name in ['BINARY', 'VARBINARY', 'BYTES']:
            return cls._serialize_binary(value)
        elif type_name in ['DECIMAL', 'NUMERIC']:
            return cls._serialize_decimal(value, data_type)
        elif type_name in ['TIMESTAMP', 'TIMESTAMP_WITHOUT_TIME_ZONE']:
            return cls._serialize_timestamp(value)
        elif type_name in ['DATE']:
            return cls._serialize_date(value)
        elif type_name in ['TIME', 'TIME_WITHOUT_TIME_ZONE']:
            return cls._serialize_time(value)
        else:
            return cls._serialize_string(str(value))

    @classmethod
    def _serialize_boolean(cls, value: bool) -> bytes:
        return struct.pack('<b', 1 if value else 0)

    @classmethod
    def _serialize_byte(cls, value: int) -> bytes:
        return struct.pack('<b', value)

    @classmethod
    def _serialize_short(cls, value: int) -> bytes:
        return struct.pack('<h', value)

    @classmethod
    def _serialize_int(cls, value: int) -> bytes:
        return struct.pack('<i', value)

    @classmethod
    def _serialize_long(cls, value: int) -> bytes:
        return struct.pack('<q', value)

    @classmethod
    def _serialize_float(cls, value: float) -> bytes:
        return struct.pack('<f', value)

    @classmethod
    def _serialize_double(cls, value: float) -> bytes:
        return struct.pack('<d', value)

    @classmethod
    def _serialize_string(cls, value) -> bytes:
        if isinstance(value, str):
            value_bytes = value.encode('utf-8')
        else:
            value_bytes = bytes(value)

        length = len(value_bytes)

        offset_and_len = (0x80 << 56) | (length << 56)
        if offset_and_len > 0x7FFFFFFFFFFFFFFF:
            offset_and_len = offset_and_len - 0x10000000000000000
        return struct.pack('<q', offset_and_len)

    @classmethod
    def _serialize_binary(cls, value: bytes) -> bytes:
        if isinstance(value, bytes):
            data_bytes = value
        else:
            data_bytes = bytes(value)
        length = len(data_bytes)
        offset_and_len = (0x80 << 56) | (length << 56)
        if offset_and_len > 0x7FFFFFFFFFFFFFFF:
            offset_and_len = offset_and_len - 0x10000000000000000
        return struct.pack('<q', offset_and_len)

    @classmethod
    def _serialize_decimal(cls, value: Decimal, data_type: DataType) -> bytes:
        type_str = str(data_type)
        if '(' in type_str and ')' in type_str:
            try:
                precision_scale = type_str.split('(')[1].split(')')[0]
                if ',' in precision_scale:
                    scale = int(precision_scale.split(',')[1])
                else:
                    scale = 0
            except:
                scale = 0
        else:
            scale = 0

        unscaled_value = int(value * (10 ** scale))
        return struct.pack('<q', unscaled_value)

    @classmethod
    def _serialize_timestamp(cls, value: datetime) -> bytes:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        millis = int(value.timestamp() * 1000)
        return struct.pack('<q', millis)

    @classmethod
    def _serialize_date(cls, value: datetime) -> bytes:
        if isinstance(value, datetime):
            epoch = datetime(1970, 1, 1)
            days = (value - epoch).days
        else:
            epoch = datetime(1970, 1, 1)
            days = (value - epoch).days
        return struct.pack('<i', days)

    @classmethod
    def _serialize_time(cls, value: datetime) -> bytes:
        if isinstance(value, datetime):
            midnight = value.replace(hour=0, minute=0, second=0, microsecond=0)
            millis = int((value - midnight).total_seconds() * 1000)
        else:
            millis = value.hour * 3600000 + value.minute * 60000 + value.second * 1000 + value.microsecond // 1000
        return struct.pack('<i', millis)
