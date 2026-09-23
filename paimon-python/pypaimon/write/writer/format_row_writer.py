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
import re
import struct
from decimal import Decimal
from typing import Any, List

import pyarrow as pa

from pypaimon.common.delta_varint_compressor import DeltaVarintCompressor
from pypaimon.schema.data_types import (
    ArrayType, AtomicType, DataField, MapType, MultisetType, RowType, VectorType
)

FOOTER_SIZE = 32
MAGIC = 0x524F5753  # "ROWS"
VERSION = 1
DEFAULT_BLOCK_SIZE = 65536


class FormatRowWriter:

    def __init__(self, output_stream, fields: List[DataField],
                 block_size: int = DEFAULT_BLOCK_SIZE, zstd_level: int = 1):
        self._out = output_stream
        self._fields = fields
        self._block_size = block_size
        self._zstd_level = zstd_level

        self._block_buf = _BlockBuffer()
        self._row_offsets: List[int] = []

        self._block_compressed_sizes: List[int] = []
        self._block_uncompressed_sizes: List[int] = []
        self._block_row_starts: List[int] = []
        self._total_row_count = 0
        self._bytes_written = 0

    def write_table(self, data: pa.Table):
        columns = {field.name: data.column(field.name).to_pylist()
                   for field in self._fields if field.name in data.column_names}
        num_rows = data.num_rows

        for row_idx in range(num_rows):
            row_values = [columns[field.name][row_idx] for field in self._fields]
            self._write_row(row_values)
            self._total_row_count += 1

            if self._block_buf.position + len(self._row_offsets) * 4 + 4 >= self._block_size:
                self._flush_block()

    def close(self):
        self._flush_block()

        index_offset = self._bytes_written
        self._write_block_index()
        index_length = self._bytes_written - index_offset

        self._write_footer(index_offset, index_length)
        self._out.flush()

    def _write_row(self, values: List[Any]):
        self._row_offsets.append(self._block_buf.position)
        arity = len(self._fields)
        header_size = (arity + 7) // 8

        header_start = self._block_buf.position
        self._block_buf.write_zeros(header_size)

        for i, field in enumerate(self._fields):
            if values[i] is None:
                byte_idx = header_start + i // 8
                self._block_buf.buffer[byte_idx] |= (1 << (i % 8))
            else:
                _write_field(self._block_buf, values[i], field.type)

    def _flush_block(self):
        if not self._row_offsets:
            return

        import zstandard as zstd

        self._block_row_starts.append(self._total_row_count - len(self._row_offsets))

        for offset in self._row_offsets:
            self._block_buf.write_int_le(offset)
        self._block_buf.write_int_le(len(self._row_offsets))

        uncompressed = bytes(self._block_buf.buffer[:self._block_buf.position])
        self._block_uncompressed_sizes.append(len(uncompressed))

        compressor = zstd.ZstdCompressor(level=self._zstd_level)
        compressed = compressor.compress(uncompressed)
        self._block_compressed_sizes.append(len(compressed))

        self._out.write(compressed)
        self._bytes_written += len(compressed)

        self._block_buf.reset()
        self._row_offsets.clear()

    def _write_block_index(self):
        encoded_compressed = DeltaVarintCompressor.compress(self._block_compressed_sizes)
        encoded_uncompressed = DeltaVarintCompressor.compress(self._block_uncompressed_sizes)
        encoded_row_starts = DeltaVarintCompressor.compress(self._block_row_starts)

        self._write_varint_prefixed(encoded_compressed)
        self._write_varint_prefixed(encoded_uncompressed)
        self._write_varint_prefixed(encoded_row_starts)

    def _write_varint_prefixed(self, data: bytes):
        varint_bytes = _encode_var_int(len(data))
        self._out.write(varint_bytes)
        self._out.write(data)
        self._bytes_written += len(varint_bytes) + len(data)

    def _write_footer(self, index_offset: int, index_length: int):
        buf = bytearray(FOOTER_SIZE)
        struct.pack_into('<q', buf, 0, self._total_row_count)
        struct.pack_into('<i', buf, 8, len(self._block_compressed_sizes))
        struct.pack_into('<q', buf, 12, index_offset)
        struct.pack_into('<i', buf, 20, index_length)
        buf[24] = VERSION
        # bytes 25-27 reserved (zeros)
        struct.pack_into('<I', buf, 28, MAGIC)
        self._out.write(bytes(buf))
        self._bytes_written += FOOTER_SIZE


class _BlockBuffer:

    def __init__(self, initial_capacity: int = DEFAULT_BLOCK_SIZE):
        self.buffer = bytearray(initial_capacity)
        self.position = 0

    def reset(self):
        self.position = 0

    def _ensure_capacity(self, additional: int):
        required = self.position + additional
        if required > len(self.buffer):
            new_size = max(len(self.buffer) * 2, required)
            new_buf = bytearray(new_size)
            new_buf[:self.position] = self.buffer[:self.position]
            self.buffer = new_buf

    def write_zeros(self, count: int):
        self._ensure_capacity(count)
        for i in range(count):
            self.buffer[self.position + i] = 0
        self.position += count

    def write_boolean(self, value: bool):
        self._ensure_capacity(1)
        self.buffer[self.position] = 1 if value else 0
        self.position += 1

    def write_byte(self, value: int):
        self._ensure_capacity(1)
        self.buffer[self.position] = value & 0xFF
        self.position += 1

    def write_short_le(self, value: int):
        self._ensure_capacity(2)
        struct.pack_into('<h', self.buffer, self.position, value)
        self.position += 2

    def write_int_le(self, value: int):
        self._ensure_capacity(4)
        struct.pack_into('<i', self.buffer, self.position, value)
        self.position += 4

    def write_long_le(self, value: int):
        self._ensure_capacity(8)
        struct.pack_into('<q', self.buffer, self.position, value)
        self.position += 8

    def write_float_le(self, value: float):
        self._ensure_capacity(4)
        struct.pack_into('<f', self.buffer, self.position, value)
        self.position += 4

    def write_double_le(self, value: float):
        self._ensure_capacity(8)
        struct.pack_into('<d', self.buffer, self.position, value)
        self.position += 8

    def write_var_int(self, value: int):
        self._ensure_capacity(5)
        while (value & ~0x7F) != 0:
            self.buffer[self.position] = (value & 0x7F) | 0x80
            self.position += 1
            value >>= 7
        self.buffer[self.position] = value & 0x7F
        self.position += 1

    def write_bytes_with_length(self, data: bytes):
        self.write_var_int(len(data))
        self._ensure_capacity(len(data))
        self.buffer[self.position:self.position + len(data)] = data
        self.position += len(data)

    def write_string(self, value: str):
        encoded = value.encode('utf-8')
        self.write_bytes_with_length(encoded)


def _encode_var_int(value: int) -> bytes:
    result = bytearray()
    while (value & ~0x7F) != 0:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value & 0x7F)
    return bytes(result)


def _write_field(buf: _BlockBuffer, value: Any, data_type) -> None:
    if isinstance(data_type, AtomicType):
        type_name = data_type.type.upper()
        if type_name == 'BOOLEAN':
            buf.write_boolean(value)
        elif type_name == 'TINYINT':
            buf.write_byte(value)
        elif type_name == 'SMALLINT':
            buf.write_short_le(value)
        elif type_name in ('INT', 'INTEGER'):
            buf.write_int_le(value)
        elif type_name == 'DATE':
            if isinstance(value, datetime.date):
                epoch = datetime.date(1970, 1, 1)
                days = (value - epoch).days
                buf.write_int_le(days)
            else:
                buf.write_int_le(value)
        elif type_name == 'TIME' or (type_name.startswith('TIME') and not type_name.startswith('TIMESTAMP')):
            if isinstance(value, datetime.time):
                millis = (value.hour * 3600 + value.minute * 60 + value.second) * 1000 + value.microsecond // 1000
                buf.write_int_le(millis)
            else:
                buf.write_int_le(value)
        elif type_name == 'BIGINT':
            buf.write_long_le(value)
        elif type_name == 'FLOAT':
            buf.write_float_le(value)
        elif type_name == 'DOUBLE':
            buf.write_double_le(value)
        elif type_name == 'STRING' or type_name.startswith('CHAR') or type_name.startswith('VARCHAR'):
            buf.write_string(value)
        elif type_name == 'BYTES' or type_name.startswith('BINARY') or type_name.startswith('VARBINARY'):
            buf.write_bytes_with_length(value)
        elif type_name == 'BLOB':
            buf.write_bytes_with_length(value)
        elif type_name.startswith('DECIMAL'):
            precision, scale = _parse_decimal_params(type_name)
            if precision <= 18:
                if isinstance(value, Decimal):
                    unscaled = int(value * (10 ** scale))
                else:
                    unscaled = int(Decimal(str(value)) * (10 ** scale))
                buf.write_long_le(unscaled)
            else:
                if isinstance(value, Decimal):
                    unscaled = int(value * (10 ** scale))
                else:
                    unscaled = int(Decimal(str(value)) * (10 ** scale))
                raw = unscaled.to_bytes(
                    (unscaled.bit_length() + 8) // 8, byteorder='big', signed=True)
                buf.write_bytes_with_length(raw)
        elif type_name.startswith('TIMESTAMP'):
            precision = _parse_timestamp_precision(type_name)
            if isinstance(value, datetime.datetime):
                epoch = datetime.datetime(1970, 1, 1, tzinfo=value.tzinfo)
                delta = value - epoch
                total_micros = int(delta.total_seconds() * 1_000_000)
                if precision <= 3:
                    buf.write_long_le(total_micros // 1000)
                else:
                    millis = total_micros // 1000
                    nano_of_milli = (total_micros % 1000) * 1000
                    buf.write_long_le(millis)
                    buf.write_var_int(nano_of_milli)
            elif precision <= 3:
                buf.write_long_le(value)
            else:
                if isinstance(value, int):
                    millis = value // 1000
                    nano_of_milli = (value % 1000) * 1000
                else:
                    millis = int(value) // 1000
                    nano_of_milli = (int(value) % 1000) * 1000
                buf.write_long_le(millis)
                buf.write_var_int(nano_of_milli)
        elif type_name == 'VARIANT':
            if isinstance(value, dict):
                buf.write_bytes_with_length(value['value'])
                buf.write_bytes_with_length(value['metadata'])
            else:
                buf.write_bytes_with_length(value.value if hasattr(value, 'value') else b'')
                buf.write_bytes_with_length(value.metadata if hasattr(value, 'metadata') else b'')
        else:
            raise ValueError(f"Unsupported atomic type for writing: {type_name}")

    elif isinstance(data_type, ArrayType):
        _write_array_elements(buf, value, data_type.element)

    elif isinstance(data_type, VectorType):
        _write_vector(buf, value, data_type.element)

    elif isinstance(data_type, MapType):
        if isinstance(value, dict):
            keys = list(value.keys())
            values = list(value.values())
        else:
            keys = [pair[0] for pair in value]
            values = [pair[1] for pair in value]
        _write_array_elements(buf, keys, data_type.key)
        _write_array_elements(buf, values, data_type.value)

    elif isinstance(data_type, MultisetType):
        if isinstance(value, dict):
            keys = list(value.keys())
            counts = list(value.values())
        else:
            keys = [pair[0] for pair in value]
            counts = [pair[1] for pair in value]
        _write_array_elements(buf, keys, data_type.element)
        _write_array_elements(buf, counts, AtomicType("INT"))

    elif isinstance(data_type, RowType):
        _write_nested_row(buf, value, data_type)

    else:
        raise ValueError(f"Unsupported data type for writing: {data_type}")


def _write_array_elements(buf: _BlockBuffer, elements: list, element_type) -> None:
    size = len(elements)
    buf.write_var_int(size)
    null_bitmap_bytes = (size + 7) // 8

    null_start = buf.position
    buf.write_zeros(null_bitmap_bytes)

    for i, elem in enumerate(elements):
        if elem is None:
            buf.buffer[null_start + i // 8] |= (1 << (i % 8))
        else:
            _write_field(buf, elem, element_type)


def _write_vector(buf: _BlockBuffer, elements: list, element_type) -> None:
    size = len(elements)
    buf.write_var_int(size)
    for elem in elements:
        _write_field(buf, elem, element_type)


def _write_nested_row(buf: _BlockBuffer, value, row_type: RowType) -> None:
    fields = row_type.fields
    arity = len(fields)
    header_size = (arity + 7) // 8

    header_start = buf.position
    buf.write_zeros(header_size)

    for i, field in enumerate(fields):
        if isinstance(value, dict):
            field_value = value.get(field.name)
        else:
            field_value = value[i] if i < len(value) else None

        if field_value is None:
            buf.buffer[header_start + i // 8] |= (1 << (i % 8))
        else:
            _write_field(buf, field_value, field.type)


def _parse_decimal_params(type_name: str) -> tuple:
    match = re.fullmatch(r'DECIMAL\((\d+),\s*(\d+)\)', type_name)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = re.fullmatch(r'DECIMAL\((\d+)\)', type_name)
    if match:
        return int(match.group(1)), 0
    return 10, 0


def _parse_timestamp_precision(type_name: str) -> int:
    match = re.search(r'\((\d+)\)', type_name)
    if match:
        return int(match.group(1))
    return 6
