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
from decimal import Decimal
from typing import Any, List, Optional

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import RecordBatch

from pypaimon.common.delta_varint_compressor import DeltaVarintCompressor
from pypaimon.common.file_io import FileIO
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import (
    ArrayType, DataField, MapType, MultisetType, PyarrowFieldParser, RowType, VectorType, AtomicType
)

FOOTER_SIZE = 32
MAGIC = 0x524F5753  # "ROWS"
VERSION = 1


class FormatRowReader(RecordBatchReader):

    def __init__(self, file_io: FileIO, file_path: str, read_fields: List[str],
                 full_fields: List[DataField], push_down_predicate: Any,
                 batch_size: int = 1024, row_indices: Optional[List[int]] = None):
        self._file_io = file_io
        self._file_path = file_path
        self._push_down_predicate = push_down_predicate
        self._batch_size = batch_size

        self._file_size = file_io.get_file_size(file_path)

        full_fields_map = {field.name: field for field in full_fields}
        self._projected_fields = [full_fields_map[name] for name in read_fields]
        self._all_fields = full_fields
        self._schema = PyarrowFieldParser.from_paimon_schema(self._projected_fields)

        self._block_compressed_sizes: List[int] = []
        self._block_uncompressed_sizes: List[int] = []
        self._block_row_starts: List[int] = []
        self._block_offsets: List[int] = []
        self._total_row_count = 0
        self._block_count = 0
        self._current_block_idx = 0

        self._row_indices = row_indices
        self._row_indices_pos = 0

        self._read_metadata()

        if self._row_indices is not None:
            self._blocks_to_read = self._compute_blocks_for_indices()
            self._blocks_to_read_pos = 0
        else:
            self._blocks_to_read = None

    def _compute_blocks_for_indices(self) -> List[tuple]:
        """Group row_indices by block. Returns list of (block_idx, [local_row_offsets])."""
        import bisect
        result = []
        row_starts = self._block_row_starts
        for idx in self._row_indices:
            block_idx = bisect.bisect_right(row_starts, idx) - 1
            local_row = idx - row_starts[block_idx]
            if result and result[-1][0] == block_idx:
                result[-1][1].append(local_row)
            else:
                result.append((block_idx, [local_row]))
        return result

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        if self._row_indices is not None:
            return self._read_batch_indexed()
        return self._read_batch_sequential()

    def _read_batch_sequential(self) -> Optional[RecordBatch]:
        if self._current_block_idx >= self._block_count:
            return None

        block_data = self._read_and_decompress_block(self._current_block_idx)
        self._current_block_idx += 1

        columns = self._decode_block(block_data)

        if not columns or all(len(col) == 0 for col in columns):
            return None

        pydict = {field.name: columns[i] for i, field in enumerate(self._projected_fields)}
        table = pa.Table.from_pydict(pydict, self._schema)

        if self._push_down_predicate is not None:
            dataset = ds.InMemoryDataset(table)
            scanner = dataset.scanner(filter=self._push_down_predicate)
            table = scanner.to_table().combine_chunks()

        if table.num_rows == 0:
            return self._read_batch_sequential()

        return table.to_batches()[0]

    def _read_batch_indexed(self) -> Optional[RecordBatch]:
        if self._blocks_to_read_pos >= len(self._blocks_to_read):
            return None

        block_idx, local_rows = self._blocks_to_read[self._blocks_to_read_pos]
        self._blocks_to_read_pos += 1

        block_data = self._read_and_decompress_block(block_idx)
        columns = self._decode_block(block_data, row_filter=local_rows)

        if not columns or all(len(col) == 0 for col in columns):
            return self._read_batch_indexed()

        pydict = {field.name: columns[i] for i, field in enumerate(self._projected_fields)}
        table = pa.Table.from_pydict(pydict, self._schema)

        if self._push_down_predicate is not None:
            dataset = ds.InMemoryDataset(table)
            scanner = dataset.scanner(filter=self._push_down_predicate)
            table = scanner.to_table().combine_chunks()

        if table.num_rows == 0:
            return self._read_batch_indexed()

        return table.to_batches()[0]

    def close(self):
        pass

    def _read_metadata(self):
        with self._file_io.new_input_stream(self._file_path) as f:
            f.seek(self._file_size - FOOTER_SIZE)
            footer_bytes = f.read(FOOTER_SIZE)

        if len(footer_bytes) != FOOTER_SIZE:
            raise IOError("Invalid row file: cannot read footer")

        magic = struct.unpack_from('<I', footer_bytes, 28)[0]
        if magic != MAGIC:
            raise IOError(f"Invalid row file magic: expected 0x{MAGIC:08X}, got 0x{magic:08X}")

        version = footer_bytes[24]
        if version != VERSION:
            raise IOError(f"Unsupported row file version: {version}")

        self._total_row_count = struct.unpack_from('<q', footer_bytes, 0)[0]
        self._block_count = struct.unpack_from('<i', footer_bytes, 8)[0]
        index_offset = struct.unpack_from('<q', footer_bytes, 12)[0]
        index_length = struct.unpack_from('<i', footer_bytes, 20)[0]

        with self._file_io.new_input_stream(self._file_path) as f:
            f.seek(index_offset)
            index_bytes = f.read(index_length)

        if len(index_bytes) != index_length:
            raise IOError("Invalid row file: cannot read block index")

        self._parse_block_index(index_bytes)

    def _parse_block_index(self, index_data: bytes):
        pos = 0

        len1, consumed = _decode_var_int(index_data, pos)
        pos += consumed
        self._block_compressed_sizes = DeltaVarintCompressor.decompress(index_data[pos:pos + len1])
        pos += len1

        len2, consumed = _decode_var_int(index_data, pos)
        pos += consumed
        self._block_uncompressed_sizes = DeltaVarintCompressor.decompress(index_data[pos:pos + len2])
        pos += len2

        len3, consumed = _decode_var_int(index_data, pos)
        pos += consumed
        self._block_row_starts = DeltaVarintCompressor.decompress(index_data[pos:pos + len3])

        offset = 0
        self._block_offsets = []
        for size in self._block_compressed_sizes:
            self._block_offsets.append(offset)
            offset += size

    def _read_and_decompress_block(self, block_idx: int) -> bytes:
        import zstandard as zstd

        offset = self._block_offsets[block_idx]
        compressed_size = self._block_compressed_sizes[block_idx]

        with self._file_io.new_input_stream(self._file_path) as f:
            f.seek(offset)
            compressed_data = f.read(compressed_size)

        decompressor = zstd.ZstdDecompressor()
        uncompressed_size = self._block_uncompressed_sizes[block_idx]
        return decompressor.decompress(compressed_data, max_output_size=uncompressed_size)

    def _decode_block(self, block_data: bytes,
                      row_filter: Optional[List[int]] = None) -> List[List]:
        data_len = len(block_data)
        row_count = struct.unpack_from('<i', block_data, data_len - 4)[0]
        offset_array_start = data_len - 4 - row_count * 4

        projected_indices = []
        for pf in self._projected_fields:
            for i, af in enumerate(self._all_fields):
                if af.name == pf.name:
                    projected_indices.append(i)
                    break

        columns = [[] for _ in self._projected_fields]
        arity = len(self._all_fields)
        header_size = (arity + 7) // 8

        rows_to_read = row_filter if row_filter is not None else range(row_count)
        proj_set = set(projected_indices)
        proj_col_map = {field_idx: col_idx for col_idx, field_idx in enumerate(projected_indices)}

        for row_idx in rows_to_read:
            row_offset = struct.unpack_from('<i', block_data, offset_array_start + row_idx * 4)[0]
            decoder = _RowDecoder(block_data, row_offset)

            null_bitmap = block_data[decoder.pos:decoder.pos + header_size]
            decoder.pos += header_size

            for field_idx in range(arity):
                is_null = (null_bitmap[field_idx // 8] & (1 << (field_idx % 8))) != 0
                if is_null:
                    if field_idx in proj_set:
                        columns[proj_col_map[field_idx]].append(None)
                else:
                    value = _read_field(decoder, self._all_fields[field_idx].type)
                    if field_idx in proj_set:
                        columns[proj_col_map[field_idx]].append(value)

        return columns


class _RowDecoder:

    __slots__ = ('data', 'pos')

    def __init__(self, data: bytes, pos: int = 0):
        self.data = data
        self.pos = pos

    def read_boolean(self) -> bool:
        v = self.data[self.pos] != 0
        self.pos += 1
        return v

    def read_byte(self) -> int:
        v = struct.unpack_from('<b', self.data, self.pos)[0]
        self.pos += 1
        return v

    def read_short(self) -> int:
        v = struct.unpack_from('<h', self.data, self.pos)[0]
        self.pos += 2
        return v

    def read_int(self) -> int:
        v = struct.unpack_from('<i', self.data, self.pos)[0]
        self.pos += 4
        return v

    def read_long(self) -> int:
        v = struct.unpack_from('<q', self.data, self.pos)[0]
        self.pos += 8
        return v

    def read_float(self) -> float:
        v = struct.unpack_from('<f', self.data, self.pos)[0]
        self.pos += 4
        return v

    def read_double(self) -> float:
        v = struct.unpack_from('<d', self.data, self.pos)[0]
        self.pos += 8
        return v

    def read_var_int(self) -> int:
        result = 0
        shift = 0
        while True:
            b = self.data[self.pos]
            self.pos += 1
            result |= (b & 0x7F) << shift
            if (b & 0x80) == 0:
                return result
            shift += 7

    def read_string(self) -> str:
        length = self.read_var_int()
        s = self.data[self.pos:self.pos + length].decode('utf-8')
        self.pos += length
        return s

    def read_bytes(self) -> bytes:
        length = self.read_var_int()
        b = self.data[self.pos:self.pos + length]
        self.pos += length
        return bytes(b)


def _decode_var_int(data: bytes, offset: int) -> tuple:
    result = 0
    shift = 0
    pos = offset
    while True:
        b = data[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            return result, pos - offset
        shift += 7


def _read_field(decoder: _RowDecoder, data_type) -> Any:
    if isinstance(data_type, AtomicType):
        type_name = data_type.type.upper()
        if type_name == 'BOOLEAN':
            return decoder.read_boolean()
        elif type_name == 'TINYINT':
            return decoder.read_byte()
        elif type_name == 'SMALLINT':
            return decoder.read_short()
        elif type_name in ('INT', 'INTEGER', 'DATE', 'TIME'):
            return decoder.read_int()
        elif type_name.startswith('TIME') and not type_name.startswith('TIMESTAMP'):
            return decoder.read_int()
        elif type_name == 'BIGINT':
            return decoder.read_long()
        elif type_name == 'FLOAT':
            return decoder.read_float()
        elif type_name == 'DOUBLE':
            return decoder.read_double()
        elif type_name == 'STRING' or type_name.startswith('CHAR') or type_name.startswith('VARCHAR'):
            return decoder.read_string()
        elif type_name == 'BYTES' or type_name.startswith('BINARY') or type_name.startswith('VARBINARY'):
            return decoder.read_bytes()
        elif type_name == 'BLOB':
            return decoder.read_bytes()
        elif type_name.startswith('DECIMAL'):
            precision, scale = _parse_decimal_params(type_name)
            if precision <= 18:
                unscaled = decoder.read_long()
                return Decimal(unscaled) / Decimal(10 ** scale)
            else:
                raw = decoder.read_bytes()
                unscaled = int.from_bytes(raw, byteorder='big', signed=True)
                return Decimal(unscaled) / Decimal(10 ** scale)
        elif type_name.startswith('TIMESTAMP'):
            precision = _parse_timestamp_precision(type_name)
            millis = decoder.read_long()
            if precision <= 3:
                return millis
            else:
                nano_of_milli = decoder.read_var_int()
                micros = millis * 1000 + nano_of_milli // 1000
                return micros
        elif type_name == 'VARIANT':
            value_bytes = decoder.read_bytes()
            metadata_bytes = decoder.read_bytes()
            return {'value': value_bytes, 'metadata': metadata_bytes}
        else:
            raise ValueError(f"Unsupported atomic type: {type_name}")

    elif isinstance(data_type, ArrayType):
        return _read_array(decoder, data_type.element)

    elif isinstance(data_type, VectorType):
        return _read_vector(decoder, data_type.element)

    elif isinstance(data_type, MapType):
        keys = _read_array_elements(decoder, data_type.key)
        values = _read_array_elements(decoder, data_type.value)
        return list(zip(keys, values))

    elif isinstance(data_type, MultisetType):
        keys = _read_array_elements(decoder, data_type.element)
        counts = _read_array_elements(decoder, AtomicType("INT"))
        return list(zip(keys, counts))

    elif isinstance(data_type, RowType):
        return _read_nested_row(decoder, data_type)

    else:
        raise ValueError(f"Unsupported data type: {data_type}")


def _read_array(decoder: _RowDecoder, element_type) -> list:
    return _read_array_elements(decoder, element_type)


def _read_array_elements(decoder: _RowDecoder, element_type) -> list:
    size = decoder.read_var_int()
    null_bitmap_bytes = (size + 7) // 8
    null_bitmap = decoder.data[decoder.pos:decoder.pos + null_bitmap_bytes]
    decoder.pos += null_bitmap_bytes

    elements = []
    for i in range(size):
        is_null = (null_bitmap[i // 8] & (1 << (i % 8))) != 0
        if is_null:
            elements.append(None)
        else:
            elements.append(_read_field(decoder, element_type))
    return elements


def _read_vector(decoder: _RowDecoder, element_type) -> list:
    size = decoder.read_var_int()
    elements = []
    for _ in range(size):
        elements.append(_read_field(decoder, element_type))
    return elements


def _read_nested_row(decoder: _RowDecoder, row_type: RowType) -> dict:
    fields = row_type.fields
    arity = len(fields)
    header_size = (arity + 7) // 8
    null_bitmap = decoder.data[decoder.pos:decoder.pos + header_size]
    decoder.pos += header_size

    result = {}
    for i, field in enumerate(fields):
        is_null = (null_bitmap[i // 8] & (1 << (i % 8))) != 0
        if is_null:
            result[field.name] = None
        else:
            result[field.name] = _read_field(decoder, field.type)
    return result


def _parse_decimal_params(type_name: str) -> tuple:
    import re
    match = re.fullmatch(r'DECIMAL\((\d+),\s*(\d+)\)', type_name)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = re.fullmatch(r'DECIMAL\((\d+)\)', type_name)
    if match:
        return int(match.group(1)), 0
    return 10, 0


def _parse_timestamp_precision(type_name: str) -> int:
    import re
    match = re.search(r'\((\d+)\)', type_name)
    if match:
        return int(match.group(1))
    return 6
