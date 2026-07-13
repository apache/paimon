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
from typing import List, Optional, Any, Iterator, BinaryIO

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import RecordBatch

from pypaimon.common.delta_varint_compressor import DeltaVarintCompressor
from pypaimon.common.file_io import FileIO
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import (
    DataField,
    PyarrowFieldParser,
    AtomicType,
    is_array_blob_type,
)
from pypaimon.table.row.blob import Blob
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.row_kind import RowKind


class FormatBlobReader(RecordBatchReader):
    NULL_LENGTH = -1
    PLACE_HOLDER_LENGTH = -2

    def __init__(self, file_io: FileIO, file_path: str, read_fields: List[str],
                 full_fields: List[DataField], push_down_predicate: Any, blob_as_descriptor: bool,
                 batch_size: int = 1024, row_indices: Optional[Any] = None,
                 blob_parallelism: int = 1):
        self._file_io = file_io
        self._file_path = file_path
        self._push_down_predicate = push_down_predicate
        self._blob_as_descriptor = blob_as_descriptor
        self._batch_size = batch_size
        self._blob_parallelism = blob_parallelism

        # Initialize the low-level blob format reader
        self.file_path = file_path
        self.blob_lengths: List[int] = []
        self.blob_offsets: List[int] = []
        self.returned = False
        self._input_stream = None
        self._blob_iterator = None
        self._current_batch = None
        try:
            self._file_size = file_io.get_file_size(file_path)
            self._input_stream = file_io.new_input_stream(file_path)
            self._read_index()
            self._apply_row_indices(row_indices)

            # Set up fields and schema before deciding whether the stream can be dropped.
            if len(read_fields) > 1:
                raise RuntimeError("Blob reader only supports one field.")
            self._fields = read_fields
            full_fields_map = {field.name: field for field in full_fields}
            projected_data_fields = [full_fields_map[name] for name in read_fields]
            self._data_field = projected_data_fields[0]
            self._is_array_blob = is_array_blob_type(self._data_field.type)
            self._schema = PyarrowFieldParser.from_paimon_schema(projected_data_fields)

            # Drop the shared stream: descriptor/concurrent reads yield BlobRefs
            # that each open their own stream (one stream isn't thread-safe).
            # ARRAY<BLOB> needs the stream to read the nested element index.
            if not self._is_array_blob and (self._blob_as_descriptor or self._blob_parallelism > 1):
                self._input_stream.close()
                self._input_stream = None
        except Exception:
            self.close()
            raise

    def read_arrow_batch(self, start_idx=None, end_idx=None) -> Optional[RecordBatch]:
        """
         start_idx: start index record of the blob file
         end_idx: end index record of the blob file
        """
        if self._blob_iterator is None:
            if self.returned:
                return None
            self.returned = True
            batch_iterator = BlobRecordIterator(
                self._file_io, self.file_path, self.blob_lengths,
                self.blob_offsets, self._data_field, self._input_stream,
                blob_as_descriptor=self._blob_as_descriptor
            )
            self._blob_iterator = iter(batch_iterator)
        read_size = self._batch_size
        if start_idx is not None and end_idx is not None:
            if self._blob_iterator.current_position >= end_idx:
                return None
            if self._blob_iterator.current_position < start_idx:
                self._blob_iterator.current_position = start_idx
            read_size = min(end_idx - self._blob_iterator.current_position, self._batch_size)
        # Collect records for this batch
        pydict_data = {name: [] for name in self._fields}
        records_in_batch = 0
        blobs_to_resolve = []

        try:
            while True:
                blob_row = next(self._blob_iterator)
                if blob_row is None:
                    break
                blob = blob_row.values[0]
                for field_name in self._fields:
                    if self._is_array_blob:
                        pydict_data[field_name].append(self._array_value_for_arrow(blob))
                    else:
                        if blob is None:
                            pydict_data[field_name].append(None)
                        elif blob is Blob.PLACE_HOLDER:
                            raise RuntimeError(
                                "Blob placeholder is not supported by FormatBlobReader yet."
                            )
                        elif self._blob_as_descriptor:
                            pydict_data[field_name].append(blob.to_descriptor().serialize())
                        elif self._blob_parallelism > 1:
                            idx = len(pydict_data[field_name])
                            pydict_data[field_name].append(None)
                            blobs_to_resolve.append((field_name, idx, blob))
                        else:
                            pydict_data[field_name].append(blob.to_data())

                records_in_batch += 1
                if records_in_batch >= read_size:
                    break

        except StopIteration:
            pass

        if blobs_to_resolve:
            self._resolve_blobs_concurrent(pydict_data, blobs_to_resolve)

        if records_in_batch == 0:
            return None

        # Create RecordBatch
        if self._push_down_predicate is None:
            # Convert to Table first, then to RecordBatch
            table = pa.Table.from_pydict(pydict_data, self._schema)
            if table.num_rows > 0:
                return table.to_batches()[0]
            else:
                return None
        else:
            # Apply predicate filtering
            pa_batch = pa.Table.from_pydict(pydict_data, self._schema)
            dataset = ds.InMemoryDataset(pa_batch)
            scanner = dataset.scanner(filter=self._push_down_predicate)
            combine_chunks = scanner.to_table().combine_chunks()
            if combine_chunks.num_rows > 0:
                return combine_chunks.to_batches()[0]
            else:
                return None

    def _resolve_blobs_concurrent(self, pydict_data, blobs_to_resolve):
        blobs = [item[2] for item in blobs_to_resolve]
        results = self._file_io.read_blobs_concurrent(blobs, self._blob_parallelism)
        for (field_name, idx, _), data in zip(blobs_to_resolve, results):
            pydict_data[field_name][idx] = data

    def _array_value_for_arrow(self, blob_array):
        if blob_array is None:
            return None
        if blob_array is Blob.ARRAY_PLACE_HOLDER:
            raise RuntimeError(
                "Blob placeholder is not supported by FormatBlobReader yet."
            )
        result = []
        for blob in blob_array:
            if blob is None:
                result.append(None)
            elif self._blob_as_descriptor:
                result.append(blob.to_descriptor().serialize())
            else:
                result.append(blob.to_data())
        return result

    def close(self):
        self._blob_iterator = None
        if self._input_stream is not None:
            self._input_stream.close()
            self._input_stream = None

    def _read_index(self) -> None:
        f = self._input_stream

        # Seek to header: last 5 bytes
        f.seek(self._file_size - 5)
        header = f.read(5)

        if len(header) != 5:
            raise IOError("Invalid blob file: cannot read header")

        # Parse header
        index_length = struct.unpack('<I', header[:4])[0]  # Little endian
        version = header[4]

        if version != 1:
            raise IOError(f"Unsupported blob file version: {version}")

        # Read index data
        f.seek(self._file_size - 5 - index_length)
        index_bytes = f.read(index_length)

        if len(index_bytes) != index_length:
            raise IOError("Invalid blob file: cannot read index")

        # Decompress blob lengths and compute offsets
        blob_lengths = DeltaVarintCompressor.decompress(index_bytes)
        blob_offsets = []
        offset = 0
        for length in blob_lengths:
            if length < 0:
                blob_offsets.append(-1)
            else:
                blob_offsets.append(offset)
                offset += length
        self.blob_lengths = blob_lengths
        self.blob_offsets = blob_offsets

    def _apply_row_indices(self, row_indices: Optional[Any]) -> None:
        if row_indices is None:
            return

        selected_lengths = []
        selected_offsets = []
        record_count = len(self.blob_lengths)
        for row_index in row_indices:
            row_index = int(row_index)
            if row_index < 0 or row_index >= record_count:
                raise IndexError(
                    f"Blob row index {row_index} is out of range for file "
                    f"{self.file_path}, record count: {record_count}."
                )
            selected_lengths.append(self.blob_lengths[row_index])
            selected_offsets.append(self.blob_offsets[row_index])

        self.blob_lengths = selected_lengths
        self.blob_offsets = selected_offsets


class BlobRecordIterator:
    MAGIC_NUMBER_SIZE = 4
    METADATA_OVERHEAD = 16
    ARRAY_HEADER_SIZE = 9
    ARRAY_VERSION = 1
    ARRAY_MAGIC_NUMBER = 1094861634
    ARRAY_NULL_ELEMENT_LENGTH = -1
    ARRAY_INDEX_LENGTH_SIZE = 4
    MIN_ARRAY_PAYLOAD_LENGTH = ARRAY_HEADER_SIZE + ARRAY_INDEX_LENGTH_SIZE
    NULL_LENGTH = -1
    PLACE_HOLDER_LENGTH = -2

    def __init__(self, file_io: FileIO, file_path: str, blob_lengths: List[int],
                 blob_offsets: List[int], field,
                 input_stream: Optional[BinaryIO] = None,
                 blob_as_descriptor: bool = False):
        self.file_io = file_io
        self.file_path = file_path
        self.input_stream = input_stream
        if isinstance(field, DataField):
            self.field = field
        else:
            self.field = DataField(0, field, AtomicType("BLOB"))
        self.field_name = self.field.name
        self.is_array_blob = is_array_blob_type(self.field.type)
        self.blob_as_descriptor = blob_as_descriptor
        self.blob_lengths = blob_lengths
        self.blob_offsets = blob_offsets
        self.current_position = 0

    def __iter__(self) -> Iterator[GenericRow]:
        return self

    def __next__(self) -> GenericRow:
        if self.current_position >= len(self.blob_lengths):
            raise StopIteration
        fields = [self.field]
        length = self.blob_lengths[self.current_position]
        if length == self.NULL_LENGTH:
            self.current_position += 1
            return GenericRow([None], fields, RowKind.INSERT)
        if length == self.PLACE_HOLDER_LENGTH:
            self.current_position += 1
            placeholder = Blob.ARRAY_PLACE_HOLDER if self.is_array_blob else Blob.PLACE_HOLDER
            return GenericRow([placeholder], fields, RowKind.INSERT)
        # Create blob reference for the current blob
        # Skip magic number (4 bytes) and exclude length (8 bytes) + CRC (4 bytes) = 12 bytes
        blob_offset = self.blob_offsets[self.current_position] + self.MAGIC_NUMBER_SIZE  # Skip magic number
        blob_length = length - self.METADATA_OVERHEAD
        if self.is_array_blob:
            blob = self._read_blob_array(blob_offset, blob_length)
        elif self.input_stream is not None and not self.blob_as_descriptor:
            blob = Blob.from_data(self._read_inline_blob(blob_offset, blob_length))
        else:
            blob = Blob.from_file(self.file_io, self.file_path, blob_offset, blob_length)
        self.current_position += 1
        return GenericRow([blob], fields, RowKind.INSERT)

    def returned_position(self) -> int:
        return self.current_position

    def _read_inline_blob(self, position: int, length: int) -> bytes:
        self.input_stream.seek(position)
        data = self._read_fully(length)
        if len(data) != length:
            raise IOError("Invalid blob file: cannot read blob data")
        return data

    def _read_blob_array(self, position: int, length: int):
        if position < 0 or length < self.MIN_ARRAY_PAYLOAD_LENGTH:
            raise ValueError(
                f"Invalid ARRAY<BLOB> payload position or length: {position}, {length}"
            )

        stream = self.input_stream
        close_stream = False
        if stream is None:
            stream = self.file_io.new_input_stream(self.file_path)
            close_stream = True
        try:
            stream.seek(position)
            header = self._read_fully_from(stream, self.ARRAY_HEADER_SIZE)
            if len(header) != self.ARRAY_HEADER_SIZE:
                raise IOError("Invalid ARRAY<BLOB> payload: cannot read header")
            magic, version, element_count = struct.unpack('<IBI', header)
            if magic != self.ARRAY_MAGIC_NUMBER:
                raise ValueError(f"Invalid ARRAY<BLOB> payload magic number: {magic}")
            if version != self.ARRAY_VERSION:
                raise ValueError(f"Unsupported ARRAY<BLOB> payload version: {version}")
            if element_count > 0x7fffffff:
                raise ValueError(f"Invalid ARRAY<BLOB> element count: {element_count}")

            payload_end = position + length
            element_data_start = position + self.ARRAY_HEADER_SIZE
            index_length_position = payload_end - self.ARRAY_INDEX_LENGTH_SIZE
            stream.seek(index_length_position)
            index_length_bytes = self._read_fully_from(stream, self.ARRAY_INDEX_LENGTH_SIZE)
            if len(index_length_bytes) != self.ARRAY_INDEX_LENGTH_SIZE:
                raise IOError("Invalid ARRAY<BLOB> payload: cannot read index length")
            index_length = struct.unpack('<I', index_length_bytes)[0]
            maximum_index_length = length - self.MIN_ARRAY_PAYLOAD_LENGTH
            if index_length > 0x7fffffff or index_length > maximum_index_length:
                raise ValueError(
                    f"Invalid ARRAY<BLOB> element index length: {index_length}"
                )
            if element_count > index_length:
                raise ValueError(
                    "ARRAY<BLOB> element count exceeds element index length."
                )

            element_index_start = index_length_position - index_length
            stream.seek(element_index_start)
            index_bytes = self._read_fully_from(stream, index_length)
            if len(index_bytes) != index_length:
                raise IOError("Invalid ARRAY<BLOB> payload: cannot read element index")
            self._validate_array_element_index(index_bytes)
            element_lengths = DeltaVarintCompressor.decompress(index_bytes)
            if len(element_lengths) != element_count:
                raise ValueError(
                    "ARRAY<BLOB> element count does not match element index length."
                )

            element_data_length = element_index_start - element_data_start
            total_element_length = 0
            for element_length in element_lengths:
                if element_length == self.ARRAY_NULL_ELEMENT_LENGTH:
                    continue
                if element_length < 0:
                    raise ValueError(
                        f"Invalid ARRAY<BLOB> element length: {element_length}"
                    )
                if element_length > element_data_length - total_element_length:
                    raise ValueError(
                        "ARRAY<BLOB> element lengths exceed the payload data length."
                    )
                total_element_length += element_length
            if total_element_length != element_data_length:
                raise ValueError(
                    "ARRAY<BLOB> element lengths do not match the payload data length."
                )

            blobs = []
            element_offset = element_data_start
            for element_length in element_lengths:
                if element_length == self.ARRAY_NULL_ELEMENT_LENGTH:
                    blobs.append(None)
                    continue
                if self.blob_as_descriptor:
                    blobs.append(
                        Blob.from_file(self.file_io, self.file_path, element_offset, element_length)
                    )
                else:
                    stream.seek(element_offset)
                    data = self._read_fully_from(stream, element_length)
                    if len(data) != element_length:
                        raise IOError("Invalid ARRAY<BLOB> payload: cannot read element data")
                    blobs.append(Blob.from_data(data))
                element_offset += element_length
            return blobs
        finally:
            if close_stream:
                stream.close()

    @staticmethod
    def _validate_array_element_index(index_bytes: bytes) -> None:
        varint_length = 0
        for value in index_bytes:
            varint_length += 1
            if varint_length > 10:
                raise ValueError("Invalid ARRAY<BLOB> element index.")
            if value & 0x80 == 0:
                varint_length = 0
        if varint_length != 0:
            raise ValueError("Invalid ARRAY<BLOB> element index.")

    def _read_fully(self, length: int) -> bytes:
        return self._read_fully_from(self.input_stream, length)

    @staticmethod
    def _read_fully_from(stream, length: int) -> bytes:
        data = bytearray()
        while len(data) < length:
            chunk = stream.read(length - len(data))
            if not chunk:
                break
            data.extend(chunk)
        return bytes(data)
