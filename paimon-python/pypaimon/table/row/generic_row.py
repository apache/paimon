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
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, List, Union

from dataclasses import dataclass

from pypaimon.schema.data_types import AtomicType, DataField, DataType
from pypaimon.table.row.binary_row import BinaryRow
from pypaimon.table.row.internal_row import InternalRow, RowKind
from pypaimon.table.row.blob import BlobData


MAX_COMPACT_DECIMAL_PRECISION = 18


def _decimal_precision_scale(data_type: DataType):
    type_str = str(data_type)
    precision = MAX_COMPACT_DECIMAL_PRECISION
    scale = 0
    if '(' in type_str and ')' in type_str:
        try:
            precision_scale = type_str.split('(', 1)[1].split(')', 1)[0]
            parts = [p.strip() for p in precision_scale.split(',')]
            if parts and parts[0]:
                precision = int(parts[0])
            if len(parts) > 1 and parts[1]:
                scale = int(parts[1])
        except:
            pass
    return precision, scale


def _decimal_unscaled_value(value: Decimal, scale: int) -> int:
    sign, digits, exponent = value.as_tuple()
    unscaled_value = 0
    for digit in digits:
        unscaled_value = unscaled_value * 10 + digit

    scale_delta = exponent + scale
    if scale_delta >= 0:
        unscaled_value *= 10 ** scale_delta
    else:
        unscaled_value //= 10 ** (-scale_delta)

    return -unscaled_value if sign else unscaled_value


def _decimal_from_unscaled_value(unscaled_value: int, scale: int) -> Decimal:
    if unscaled_value == 0:
        return Decimal((0, (0,), -scale))
    sign = 1 if unscaled_value < 0 else 0
    digits = tuple(int(d) for d in str(abs(unscaled_value)))
    return Decimal((sign, digits, -scale))


def _int_to_signed_bytes(value: int) -> bytes:
    if value == 0:
        return b'\x00'
    bits = value.bit_length() + 1 if value > 0 else (~value).bit_length() + 1
    length = max(1, (bits + 7) // 8)
    return value.to_bytes(length, byteorder='big', signed=True)


@dataclass
class GenericRow(InternalRow):

    def __init__(self, values: List[Any], fields: List[DataField], row_kind: RowKind = RowKind.INSERT):
        self.values = values
        self.fields = fields
        self.row_kind = row_kind

    def to_dict(self):
        return {self.fields[i].name: self.values[i] for i in range(len(self.fields))}

    def get_field(self, pos: int) -> Any:
        if pos >= len(self.values):
            raise IndexError(f"Position {pos} is out of bounds for row arity {len(self.values)}")
        return self.values[pos]

    def get_row_kind(self) -> RowKind:
        return self.row_kind

    def __len__(self) -> int:
        return len(self.values)

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, GenericRow):
            return False
        return self.values == other.values and self.row_kind == other.row_kind

    def __hash__(self):
        return hash((tuple(self.values), tuple(self.fields), self.row_kind))

    def __str__(self):
        field_strs = [f"{field.name}={repr(value)}" for field, value in zip(self.fields, self.values)]
        return f"GenericRow(row_kind={self.row_kind.name}, {', '.join(field_strs)})"


class GenericRowDeserializer:
    HEADER_SIZE_IN_BITS = 8
    MAX_FIX_PART_DATA_SIZE = 7
    HIGHEST_FIRST_BIT = 0x80 << 56
    HIGHEST_SECOND_TO_EIGHTH_BIT = 0x7F << 56

    @classmethod
    def from_bytes(
            cls,
            bytes_data: bytes,
            data_fields: List[DataField]
    ) -> GenericRow:
        if not bytes_data:
            return GenericRow([], data_fields)

        arity = len(data_fields)
        actual_data = bytes_data
        if len(bytes_data) >= 4:
            actual_data = bytes_data[4:]

        fields = []
        null_bits_size_in_bytes = cls.calculate_bit_set_width_in_bytes(arity)
        for i, data_field in enumerate(data_fields):
            value = None
            if not cls.is_null_at(actual_data, 0, i):
                value = cls.parse_field_value(actual_data, 0, null_bits_size_in_bytes, i, data_field.type)
            fields.append(value)

        return GenericRow(fields, data_fields, RowKind(actual_data[0]))

    @classmethod
    def calculate_bit_set_width_in_bytes(cls, arity: int) -> int:
        return ((arity + 63 + cls.HEADER_SIZE_IN_BITS) // 64) * 8

    @classmethod
    def is_null_at(cls, bytes_data: bytes, offset: int, pos: int) -> bool:
        index = pos + cls.HEADER_SIZE_IN_BITS
        byte_index = offset + (index // 8)
        bit_index = index % 8
        return (bytes_data[byte_index] & (1 << bit_index)) != 0

    @classmethod
    def parse_field_value(
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
        elif type_name.startswith('CHAR') or type_name.startswith('VARCHAR') or type_name == 'STRING':
            return cls._parse_string(bytes_data, base_offset, field_offset)
        elif type_name.startswith('BINARY') or type_name.startswith('VARBINARY') or type_name == 'BYTES':
            return cls._parse_binary(bytes_data, base_offset, field_offset)
        elif type_name == 'BLOB':
            return cls._parse_blob(bytes_data, base_offset, field_offset)
        elif type_name.startswith('DECIMAL') or type_name.startswith('NUMERIC'):
            return cls._parse_decimal(bytes_data, base_offset, field_offset, data_type)
        elif type_name.startswith('TIMESTAMP'):
            return cls._parse_timestamp(bytes_data, base_offset, field_offset, data_type)
        elif type_name in ['DATE']:
            return cls._parse_date(bytes_data, field_offset)
        elif type_name.startswith('TIME'):
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
            return bytes_data[field_offset:field_offset + length]

    @classmethod
    def _parse_blob(cls, bytes_data: bytes, base_offset: int, field_offset: int) -> BlobData:
        """Parse BLOB data from binary format and return a BlobData instance."""
        # BLOB uses the same binary format as regular binary data
        binary_data = cls._parse_binary(bytes_data, base_offset, field_offset)
        return BlobData.from_bytes(binary_data)

    @classmethod
    def _parse_decimal(cls, bytes_data: bytes, base_offset: int, field_offset: int, data_type: DataType) -> Decimal:
        precision, scale = _decimal_precision_scale(data_type)
        if precision <= MAX_COMPACT_DECIMAL_PRECISION:
            unscaled_value = struct.unpack('<q', bytes_data[field_offset:field_offset + 8])[0]
        else:
            offset_and_len = struct.unpack('<q', bytes_data[field_offset:field_offset + 8])[0]
            sub_offset = (offset_and_len >> 32) & 0xFFFFFFFF
            length = offset_and_len & 0xFFFFFFFF
            actual_decimal_offset = base_offset + sub_offset
            if actual_decimal_offset + length > len(bytes_data):
                raise ValueError(
                    f"Decimal data out of bounds: actual_offset={actual_decimal_offset}, "
                    f"length={length}, total_length={len(bytes_data)}")
            decimal_data = bytes_data[actual_decimal_offset:actual_decimal_offset + length]
            unscaled_value = int.from_bytes(decimal_data, byteorder='big', signed=True)
        return _decimal_from_unscaled_value(unscaled_value, scale)

    @classmethod
    def _parse_timestamp(cls, bytes_data: bytes, base_offset: int, field_offset: int, data_type: DataType) -> datetime:
        millis = struct.unpack('<q', bytes_data[field_offset:field_offset + 8])[0]
        return datetime.fromtimestamp(millis / 1000.0, tz=None)

    @classmethod
    def _parse_date(cls, bytes_data: bytes, field_offset: int) -> date:
        days = struct.unpack('<i', bytes_data[field_offset:field_offset + 4])[0]
        return date(1970, 1, 1) + timedelta(days=days)

    @classmethod
    def _parse_time(cls, bytes_data: bytes, field_offset: int) -> time:
        millis = struct.unpack('<i', bytes_data[field_offset:field_offset + 4])[0]
        seconds = millis // 1000
        microseconds = (millis % 1000) * 1000
        return time(
            hour=seconds // 3600,
            minute=(seconds % 3600) // 60,
            second=seconds % 60,
            microsecond=microseconds
        )


class GenericRowSerializer:
    HEADER_SIZE_IN_BITS = 8
    MAX_FIX_PART_DATA_SIZE = 7

    @classmethod
    def to_bytes(cls, row: Union[GenericRow, BinaryRow]) -> bytes:
        if isinstance(row, BinaryRow):
            return row.data
        arity = len(row.fields)
        null_bits_size_in_bytes = cls._calculate_bit_set_width_in_bytes(arity)
        fixed_part_size = null_bits_size_in_bytes + arity * 8
        fixed_part = bytearray(fixed_part_size)
        fixed_part[0] = row.row_kind.value

        variable_part_data = []
        current_variable_offset = 0

        for i, (value, field) in enumerate(zip(row.values, row.fields)):
            field_fixed_offset = null_bits_size_in_bytes + i * 8

            if value is None:
                cls._set_null_bit(fixed_part, 0, i)
                struct.pack_into('<q', fixed_part, field_fixed_offset, 0)
                continue

            if not isinstance(field.type, AtomicType):
                raise ValueError(f"BinaryRow only support AtomicType yet, meet {field.type.__class__}")

            type_name = field.type.type.upper()
            if any(type_name.startswith(p) for p in ['CHAR', 'VARCHAR', 'STRING',
                                                     'BINARY', 'VARBINARY', 'BYTES', 'BLOB']):
                if any(type_name.startswith(p) for p in ['CHAR', 'VARCHAR', 'STRING']):
                    value_bytes = str(value).encode('utf-8')
                elif type_name == 'BLOB':
                    value_bytes = value.to_data()
                else:
                    value_bytes = bytes(value)

                length = len(value_bytes)
                if length <= cls.MAX_FIX_PART_DATA_SIZE:
                    fixed_part[field_fixed_offset: field_fixed_offset + length] = value_bytes
                    for j in range(length, 7):
                        fixed_part[field_fixed_offset + j] = 0
                    header_byte = 0x80 | length
                    fixed_part[field_fixed_offset + 7] = header_byte
                else:
                    var_length = cls._round_number_of_bytes_to_nearest_word(len(value_bytes))
                    var_value_bytes = value_bytes + b'\x00' * (var_length - length)
                    offset_in_variable_part = current_variable_offset
                    variable_part_data.append(var_value_bytes)
                    current_variable_offset += var_length

                    absolute_offset = fixed_part_size + offset_in_variable_part
                    offset_and_len = (absolute_offset << 32) | length
                    struct.pack_into('<q', fixed_part, field_fixed_offset, offset_and_len)
            elif type_name.startswith('DECIMAL') or type_name.startswith('NUMERIC'):
                precision, scale = _decimal_precision_scale(field.type)
                if precision <= MAX_COMPACT_DECIMAL_PRECISION:
                    field_bytes = cls._serialize_decimal(value, field.type)
                    fixed_part[field_fixed_offset: field_fixed_offset + len(field_bytes)] = field_bytes
                else:
                    unscaled_bytes = _int_to_signed_bytes(_decimal_unscaled_value(value, scale))
                    if len(unscaled_bytes) > 16:
                        raise ValueError(f"Decimal value exceeds 16 bytes: {value}")
                    variable_part_data.append(unscaled_bytes + b'\x00' * (16 - len(unscaled_bytes)))
                    absolute_offset = fixed_part_size + current_variable_offset
                    current_variable_offset += 16
                    offset_and_len = (absolute_offset << 32) | len(unscaled_bytes)
                    struct.pack_into('<q', fixed_part, field_fixed_offset, offset_and_len)
            else:
                field_bytes = cls._serialize_field_value(value, field.type)
                fixed_part[field_fixed_offset: field_fixed_offset + len(field_bytes)] = field_bytes

        row_data = bytes(fixed_part) + b''.join(variable_part_data)
        arity_prefix = struct.pack('>i', arity)
        return arity_prefix + row_data

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
            return cls._serialize_boolean(value) + b'\x00' * 7
        elif type_name in ['TINYINT', 'BYTE']:
            return cls._serialize_byte(value) + b'\x00' * 7
        elif type_name in ['SMALLINT', 'SHORT']:
            return cls._serialize_short(value) + b'\x00' * 6
        elif type_name in ['INT', 'INTEGER']:
            return cls._serialize_int(value) + b'\x00' * 4
        elif type_name in ['BIGINT', 'LONG']:
            return cls._serialize_long(value)
        elif type_name in ['FLOAT', 'REAL']:
            return cls._serialize_float(value) + b'\x00' * 4
        elif type_name in ['DOUBLE']:
            return cls._serialize_double(value)
        elif type_name.startswith('DECIMAL') or type_name.startswith('NUMERIC'):
            return cls._serialize_decimal(value, data_type)
        elif type_name.startswith('TIMESTAMP'):
            return cls._serialize_timestamp(value)
        elif type_name in ['DATE']:
            return cls._serialize_date(value) + b'\x00' * 4
        elif type_name.startswith('TIME'):
            return cls._serialize_time(value) + b'\x00' * 4
        else:
            raise TypeError(f"Unsupported type for serialization: {type_name}")

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
    def _serialize_decimal(cls, value: Decimal, data_type: DataType) -> bytes:
        # This helper is only for compact decimals. Wider decimals are written
        # through the variable-length path in to_bytes so their length can be
        # preserved in the fixed part.
        _, scale = _decimal_precision_scale(data_type)
        unscaled_value = _decimal_unscaled_value(value, scale)
        return struct.pack('<q', unscaled_value)

    @classmethod
    def _serialize_timestamp(cls, value: datetime) -> bytes:
        if value.tzinfo is not None:
            raise RuntimeError("datetime tzinfo not supported yet")
        millis = int(value.timestamp() * 1000)
        return struct.pack('<q', millis)

    @classmethod
    def _serialize_date(cls, value: date) -> bytes:
        if isinstance(value, date):
            epoch = datetime(1970, 1, 1).date()
            days = (value - epoch).days
        else:
            raise RuntimeError("value should be datatime.date")
        return struct.pack('<i', days)

    @classmethod
    def _serialize_time(cls, value: time) -> bytes:
        if isinstance(value, time):
            millis = value.hour * 3600000 + value.minute * 60000 + value.second * 1000 + value.microsecond // 1000
        else:
            raise RuntimeError("value should be datatime.time")
        return struct.pack('<i', millis)

    @classmethod
    def _round_number_of_bytes_to_nearest_word(cls, num_bytes: int) -> int:
        remainder = num_bytes & 0x07
        if remainder == 0:
            return num_bytes
        else:
            return num_bytes + (8 - remainder)
