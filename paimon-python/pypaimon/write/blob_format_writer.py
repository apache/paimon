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
from collections.abc import Mapping
from typing import BinaryIO, List, Optional

try:
    from isal import isal_zlib as crc_backend
except ImportError:
    import zlib as crc_backend

from pypaimon.common.map_blob_key_serializer import create_map_blob_key_serializer
from pypaimon.schema.data_types import (
    MapType,
    is_array_blob_type,
    is_blob_file_type,
    is_map_blob_type,
)
from pypaimon.table.row.blob import Blob, BlobData, BlobDescriptor, BlobConsumer
from pypaimon.common.delta_varint_compressor import DeltaVarintCompressor


class BlobFormatWriter:
    VERSION = 1
    MAGIC_NUMBER = 1481511375
    ARRAY_VERSION = 1
    ARRAY_MAGIC_NUMBER = 1094861634
    MAP_VERSION = 1
    MAP_MAGIC_NUMBER = 0x4D424342
    NULL_LENGTH = -1
    PLACE_HOLDER_LENGTH = -2
    ARRAY_NULL_ELEMENT_LENGTH = -1
    MAP_NULL_KEY_LENGTH = -1
    MAP_NULL_VALUE_LENGTH = -1
    BUFFER_SIZE = 4096
    METADATA_SIZE = 12  # 8-byte length + 4-byte CRC

    def __init__(self, output_stream: BinaryIO,
                 blob_consumer: Optional[BlobConsumer] = None,
                 file_path: Optional[str] = None,
                 copy_buffer_size: int = BUFFER_SIZE):
        if copy_buffer_size <= 0:
            raise ValueError(
                f"BLOB copy buffer size must be positive, but was {copy_buffer_size}.")
        self.output_stream = output_stream
        self._blob_consumer = blob_consumer
        self._file_path = file_path
        self.copy_buffer_size = copy_buffer_size
        self.lengths: List[int] = []
        self.position = 0

    def add_element(self, row) -> None:
        if not hasattr(row, 'values') or len(row.values) != 1:
            raise ValueError("BlobFormatWriter only supports one field")

        blob_field_name = row.fields[0].name
        blob_field_type = row.fields[0].type
        blob_value = row.values[0]
        if isinstance(blob_field_type, MapType) and not is_map_blob_type(blob_field_type):
            raise ValueError(
                f"Map-Blob value type must be BLOB, but is {blob_field_type.value}."
            )
        if blob_value is None:
            self.lengths.append(self.NULL_LENGTH)
            if self._blob_consumer is not None:
                self._blob_consumer(blob_field_name, None)
            return

        if is_map_blob_type(blob_field_type):
            if blob_value is Blob.MAP_PLACE_HOLDER:
                self.lengths.append(self.PLACE_HOLDER_LENGTH)
                return
            self.add_blob_map(
                blob_field_name,
                blob_value,
                blob_field_type.key,
            )
            return

        if is_array_blob_type(blob_field_type):
            if blob_value is Blob.ARRAY_PLACE_HOLDER:
                self.lengths.append(self.PLACE_HOLDER_LENGTH)
                return
            self.add_blob_array(
                blob_field_name,
                blob_value,
                element_nullable=blob_field_type.element.nullable,
            )
            return

        if blob_value is Blob.PLACE_HOLDER:
            self.lengths.append(self.PLACE_HOLDER_LENGTH)
            return

        self.add_blob(blob_field_name, blob_value)

    def add_blob(self, blob_field_name: str, blob_value: Blob) -> None:
        if not isinstance(blob_value, Blob):
            raise ValueError("Field must be Blob/BlobData instance")

        previous_pos = self.position
        crc32 = 0  # Initialize CRC32

        # Write magic number
        magic_bytes = struct.pack('<I', self.MAGIC_NUMBER)  # Little endian
        crc32 = self._write_with_crc(magic_bytes, crc32)

        blob_pos, blob_length, crc32 = self._write_blob_data(blob_value, crc32)

        # Calculate total length including magic + data + metadata (length + CRC)
        bin_length = self.position - previous_pos + self.METADATA_SIZE
        self.lengths.append(bin_length)

        # Write length (8 bytes, little endian)
        length_bytes = struct.pack('<Q', bin_length)
        self.output_stream.write(length_bytes)
        self.position += 8

        # Write CRC32 (4 bytes, little endian)
        crc_bytes = struct.pack('<I', crc32 & 0xffffffff)
        self.output_stream.write(crc_bytes)
        self.position += 4

        if self._blob_consumer is not None:
            descriptor = BlobDescriptor(self._file_path, blob_pos, blob_length)
            flush = self._blob_consumer(blob_field_name, descriptor)
            if flush:
                self.output_stream.flush()

    def add_blob_array(
            self,
            blob_field_name: str,
            blob_values,
            element_nullable: bool = True) -> None:
        if isinstance(blob_values, (bytes, bytearray, str)) or not hasattr(blob_values, '__iter__'):
            raise ValueError("ARRAY<BLOB> field value must be a list or tuple of Blob values")

        blob_values = list(blob_values)
        for blob_value in blob_values:
            if blob_value is None:
                if not element_nullable:
                    raise ValueError(
                        f"ARRAY<BLOB> field '{blob_field_name}' does not allow null elements"
                    )
                continue
            if not isinstance(blob_value, Blob):
                raise ValueError("ARRAY<BLOB> elements must be Blob/BlobData instances or None")
            if blob_value is Blob.PLACE_HOLDER or blob_value is Blob.ARRAY_PLACE_HOLDER:
                raise ValueError("ARRAY<BLOB> elements do not support placeholders")

        previous_pos = self.position
        crc32 = 0

        crc32 = self._write_with_crc(struct.pack('<I', self.MAGIC_NUMBER), crc32)
        crc32 = self._write_with_crc(struct.pack('<I', self.ARRAY_MAGIC_NUMBER), crc32)
        crc32 = self._write_with_crc(struct.pack('<B', self.ARRAY_VERSION), crc32)
        crc32 = self._write_with_crc(struct.pack('<I', len(blob_values)), crc32)

        element_lengths = []
        flush = False
        for blob_value in blob_values:
            if blob_value is None:
                element_lengths.append(self.ARRAY_NULL_ELEMENT_LENGTH)
                continue

            blob_pos, blob_length, crc32 = self._write_blob_data(blob_value, crc32)
            element_lengths.append(blob_length)
            if self._blob_consumer is not None:
                descriptor = BlobDescriptor(self._file_path, blob_pos, blob_length)
                flush = self._blob_consumer(blob_field_name, descriptor) or flush

        element_index_bytes = DeltaVarintCompressor.compress(element_lengths)
        crc32 = self._write_with_crc(element_index_bytes, crc32)
        crc32 = self._write_with_crc(struct.pack('<I', len(element_index_bytes)), crc32)

        bin_length = self.position - previous_pos + self.METADATA_SIZE
        self.lengths.append(bin_length)
        self.output_stream.write(struct.pack('<Q', bin_length))
        self.position += 8
        self.output_stream.write(struct.pack('<I', crc32 & 0xffffffff))
        self.position += 4

        if flush:
            self.output_stream.flush()

    def add_blob_map(self, blob_field_name: str, blob_values, key_type) -> None:
        entries = self._map_entries(blob_values)
        if len(entries) > 0x7fffffff:
            raise ValueError(f"Invalid MAP<X, BLOB> entry count: {len(entries)}")

        key_serializer = create_map_blob_key_serializer(key_type)
        key_bytes = []
        for key, _ in entries:
            if key is None:
                key_bytes.append(None)
                continue
            serialized = key_serializer.serialize(key)
            if len(serialized) > 0x7fffffff:
                raise ValueError(f"MAP<X, BLOB> key is too large: {len(serialized)}")
            key_bytes.append(serialized)

        for _, blob_value in entries:
            if blob_value is None:
                continue
            if (
                blob_value is Blob.PLACE_HOLDER
                or blob_value is Blob.ARRAY_PLACE_HOLDER
                or blob_value is Blob.MAP_PLACE_HOLDER
            ):
                raise ValueError("MAP<X, BLOB> values do not support placeholders")
            if not isinstance(blob_value, Blob):
                raise ValueError("MAP<X, BLOB> values must be Blob/BlobData instances or None")

        previous_pos = self.position
        crc32 = 0
        crc32 = self._write_with_crc(struct.pack('<I', self.MAGIC_NUMBER), crc32)
        crc32 = self._write_with_crc(struct.pack('<I', self.MAP_MAGIC_NUMBER), crc32)
        crc32 = self._write_with_crc(struct.pack('<B', self.MAP_VERSION), crc32)
        crc32 = self._write_with_crc(struct.pack('<I', len(entries)), crc32)

        key_lengths = []
        for serialized in key_bytes:
            if serialized is None:
                key_lengths.append(self.MAP_NULL_KEY_LENGTH)
            else:
                key_lengths.append(len(serialized))
                crc32 = self._write_with_crc(serialized, crc32)

        value_lengths = []
        flush = False
        for _, blob_value in entries:
            if blob_value is None:
                value_lengths.append(self.MAP_NULL_VALUE_LENGTH)
                continue
            blob_pos, blob_length, crc32 = self._write_blob_data(blob_value, crc32)
            value_lengths.append(blob_length)
            if self._blob_consumer is not None:
                descriptor = BlobDescriptor(self._file_path, blob_pos, blob_length)
                flush = self._blob_consumer(blob_field_name, descriptor) or flush

        key_index_bytes = DeltaVarintCompressor.compress(key_lengths)
        value_index_bytes = DeltaVarintCompressor.compress(value_lengths)
        crc32 = self._write_with_crc(key_index_bytes, crc32)
        crc32 = self._write_with_crc(value_index_bytes, crc32)
        crc32 = self._write_with_crc(struct.pack('<I', len(key_index_bytes)), crc32)
        crc32 = self._write_with_crc(struct.pack('<I', len(value_index_bytes)), crc32)

        bin_length = self.position - previous_pos + self.METADATA_SIZE
        self.lengths.append(bin_length)
        crc32 = self._write_with_crc(struct.pack('<Q', bin_length), crc32)
        self.output_stream.write(struct.pack('<I', crc32 & 0xffffffff))
        self.position += 4

        if flush:
            self.output_stream.flush()

    def _write_blob_data(self, blob_value: Blob, crc32: int):
        blob_pos = self.position

        if isinstance(blob_value, BlobData):
            data = blob_value.to_data()
            crc32 = self._write_with_crc(data, crc32)
        else:
            stream = blob_value.new_input_stream()
            try:
                chunk = stream.read(self.copy_buffer_size)
                while chunk:
                    crc32 = self._write_with_crc(chunk, crc32)
                    chunk = stream.read(self.copy_buffer_size)
            finally:
                stream.close()

        return blob_pos, self.position - blob_pos, crc32

    def _write_with_crc(self, data: bytes, crc32: int) -> int:
        crc32 = crc_backend.crc32(data, crc32)
        self.output_stream.write(data)
        self.position += len(data)
        return crc32

    def write_value(self, col_data, fields, uri_reader_factory=None) -> None:
        from pypaimon.table.row.generic_row import GenericRow
        from pypaimon.table.row.row_kind import RowKind

        is_map_blob = is_map_blob_type(fields[0].type)
        is_blob = is_blob_file_type(fields[0].type) or is_map_blob

        if col_data is None:
            if not is_blob:
                raise RuntimeError("Null values are only supported for BLOB type fields")
            self.lengths.append(self.NULL_LENGTH)
            return

        if is_map_blob:
            row_values = [self._to_blob_map(col_data, uri_reader_factory)]
        elif is_array_blob_type(fields[0].type):
            row_values = [self._to_blob_array(col_data, uri_reader_factory)]
        elif is_blob:
            row_values = [self._to_blob(col_data, uri_reader_factory)]
        else:
            row_values = [col_data]

        row = GenericRow(row_values, fields, RowKind.INSERT)
        self.add_element(row)

    @staticmethod
    def _to_blob(col_data, uri_reader_factory=None) -> Blob:
        if col_data is Blob.PLACE_HOLDER:
            return Blob.PLACE_HOLDER
        if hasattr(col_data, 'as_py'):
            col_data = col_data.as_py()
        if isinstance(col_data, str):
            col_data = col_data.encode('utf-8')
        if isinstance(col_data, bytearray):
            col_data = bytes(col_data)
        if isinstance(col_data, Blob):
            return col_data
        if isinstance(col_data, bytes):
            if BlobDescriptor.is_blob_descriptor(col_data):
                if uri_reader_factory is None:
                    raise RuntimeError("uri_reader_factory is required for BlobDescriptor bytes.")
                descriptor = BlobDescriptor.deserialize(col_data)
                uri_reader = uri_reader_factory.create(descriptor.uri)
                return Blob.from_descriptor(uri_reader, descriptor)
            return BlobData(col_data)
        raise RuntimeError(
            "Blob field value must be bytes/blob or serialized BlobDescriptor bytes."
        )

    @classmethod
    def _to_blob_array(cls, col_data, uri_reader_factory=None):
        if col_data is Blob.ARRAY_PLACE_HOLDER:
            return Blob.ARRAY_PLACE_HOLDER
        if hasattr(col_data, 'as_py'):
            col_data = col_data.as_py()
        if col_data is None:
            return None
        if isinstance(col_data, (bytes, bytearray, str)) or not hasattr(col_data, '__iter__'):
            raise RuntimeError("ARRAY<BLOB> field value must be a list or tuple.")
        result = []
        for element in col_data:
            if element is None:
                result.append(None)
                continue
            if element is Blob.PLACE_HOLDER or element is Blob.ARRAY_PLACE_HOLDER:
                raise RuntimeError("ARRAY<BLOB> elements do not support placeholders.")
            result.append(cls._to_blob(element, uri_reader_factory))
        return result

    @classmethod
    def _to_blob_map(cls, col_data, uri_reader_factory=None):
        if col_data is Blob.MAP_PLACE_HOLDER:
            return Blob.MAP_PLACE_HOLDER
        if hasattr(col_data, 'as_py'):
            col_data = col_data.as_py()
        if col_data is None:
            return None

        result = []
        for key, value in cls._map_entries(col_data):
            if value is None:
                result.append((key, None))
                continue
            if (
                value is Blob.PLACE_HOLDER
                or value is Blob.ARRAY_PLACE_HOLDER
                or value is Blob.MAP_PLACE_HOLDER
            ):
                raise RuntimeError("MAP<X, BLOB> values do not support placeholders.")
            result.append((key, cls._to_blob(value, uri_reader_factory)))
        return result

    @staticmethod
    def _map_entries(blob_values):
        if isinstance(blob_values, Mapping):
            return list(blob_values.items())
        if isinstance(blob_values, (bytes, bytearray, str)) or not hasattr(
            blob_values, '__iter__'
        ):
            raise ValueError("MAP<X, BLOB> field value must be a mapping or key/value pairs")

        entries = []
        for entry in blob_values:
            if not isinstance(entry, (list, tuple)) or len(entry) != 2:
                raise ValueError("MAP<X, BLOB> entries must be key/value pairs")
            entries.append((entry[0], entry[1]))
        return entries

    def reach_target_size(self, target_size: int) -> bool:
        return self.position >= target_size

    def close(self) -> None:
        index_bytes = DeltaVarintCompressor.compress(self.lengths)
        self.output_stream.write(index_bytes)

        # Write header (index length + version)
        header = struct.pack('<I', len(index_bytes))  # Index length (4 bytes, little endian)
        header += struct.pack('<B', self.VERSION)  # Version (1 byte)
        self.output_stream.write(header)

        # Flush and close
        if hasattr(self.output_stream, 'flush'):
            self.output_stream.flush()
        if hasattr(self.output_stream, 'close'):
            self.output_stream.close()
