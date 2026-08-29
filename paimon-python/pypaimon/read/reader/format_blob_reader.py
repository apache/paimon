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

import bisect
import struct
from typing import List, Optional, Any, Iterator, BinaryIO

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import RecordBatch

from pypaimon.common.delta_varint_compressor import DeltaVarintCompressor
from pypaimon.common.file_io import FileIO
from pypaimon.common.map_blob_key_serializer import create_map_blob_key_serializer
from pypaimon.common.uri_reader import UriReader
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import (
    DataField,
    PyarrowFieldParser,
    AtomicType,
    MapType,
    is_array_blob_type,
    is_map_blob_type,
)
from pypaimon.table.row.blob import Blob, VideoFrameDescriptor
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.row_kind import RowKind


class FormatBlobReader(RecordBatchReader):
    NULL_LENGTH = -1
    PLACE_HOLDER_LENGTH = -2

    def __init__(self, file_io: FileIO, file_path: str, read_fields: List[str],
                 full_fields: List[DataField], push_down_predicate: Any, blob_as_descriptor: bool,
                 batch_size: int = 1024, row_indices: Optional[Any] = None,
                 blob_parallelism: int = 1, file_size: Optional[int] = None):
        self._file_io = file_io
        self._file_path = file_path
        self._push_down_predicate = push_down_predicate
        self._blob_as_descriptor = blob_as_descriptor
        self._batch_size = batch_size
        self._blob_parallelism = blob_parallelism
        self._is_video = file_path.endswith('.video')
        self._video_meta = None

        # Initialize the low-level blob format reader
        self.file_path = file_path
        self.blob_lengths: List[int] = []
        self.blob_offsets: List[int] = []
        self.returned = False
        self._input_stream = None
        self._blob_iterator = None
        self._current_batch = None
        try:
            self._file_size = (
                file_size
                if file_size is not None and file_size > 0
                else file_io.get_file_size(file_path)
            )
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
            if isinstance(self._data_field.type, MapType) and not is_map_blob_type(
                self._data_field.type
            ):
                raise ValueError(
                    "Map-Blob value type must be BLOB, but is "
                    f"{self._data_field.type.value}."
                )
            self._is_array_blob = is_array_blob_type(self._data_field.type)
            self._is_map_blob = is_map_blob_type(self._data_field.type)
            self._schema = PyarrowFieldParser.from_paimon_schema(projected_data_fields)

            # Drop the shared stream: descriptor/concurrent reads yield BlobRefs
            # that each open their own stream (one stream isn't thread-safe).
            # Nested Blob formats need the stream to read their keys and indexes.
            if (
                not self._is_array_blob
                and not self._is_map_blob
                and (
                    self._is_video
                    or self._blob_as_descriptor
                    or self._blob_parallelism > 1
                )
            ):
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
            if self._is_video:
                batch_iterator = VideoFrameRecordIterator(
                    self._file_io,
                    self.file_path,
                    self._video_meta,
                    self._data_field,
                )
            else:
                batch_iterator = BlobRecordIterator(
                    self._file_io, self.file_path, self.blob_lengths,
                    self.blob_offsets, self._data_field, self._input_stream,
                    blob_as_descriptor=(
                        self._blob_as_descriptor or self._blob_parallelism > 1
                    )
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
                    if self._is_map_blob:
                        row_index = len(pydict_data[field_name])
                        pydict_data[field_name].append(
                            self._map_value_for_arrow(
                                blob,
                                field_name,
                                row_index,
                                blobs_to_resolve,
                            )
                        )
                    elif self._is_array_blob:
                        row_index = len(pydict_data[field_name])
                        pydict_data[field_name].append(
                            self._array_value_for_arrow(
                                blob,
                                field_name,
                                row_index,
                                blobs_to_resolve,
                            )
                        )
                    else:
                        if blob is None:
                            pydict_data[field_name].append(None)
                        elif blob is Blob.PLACE_HOLDER:
                            raise RuntimeError(
                                "Blob placeholder is not supported by FormatBlobReader yet."
                            )
                        elif self._is_video or self._blob_as_descriptor:
                            pydict_data[field_name].append(blob.to_descriptor().serialize())
                        elif self._blob_parallelism > 1:
                            idx = len(pydict_data[field_name])
                            pydict_data[field_name].append(None)
                            blobs_to_resolve.append((field_name, idx, 'raw', None, blob))
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
        blobs = [item[4] for item in blobs_to_resolve]
        results = self._file_io.read_blobs_concurrent(blobs, self._blob_parallelism)
        for target, data in zip(blobs_to_resolve, results):
            field_name, row_index, container_kind, slot, _ = target
            if container_kind == 'raw':
                pydict_data[field_name][row_index] = data
            elif container_kind == 'array':
                pydict_data[field_name][row_index][slot] = data
            elif container_kind == 'map':
                key = pydict_data[field_name][row_index][slot][0]
                pydict_data[field_name][row_index][slot] = (key, data)

    def _array_value_for_arrow(
        self,
        blob_array,
        field_name,
        row_index,
        blobs_to_resolve,
    ):
        if blob_array is None:
            return None
        if blob_array is Blob.ARRAY_PLACE_HOLDER:
            raise RuntimeError(
                "Blob placeholder is not supported by FormatBlobReader yet."
            )
        result = []
        for element_index, blob in enumerate(blob_array):
            if blob is None:
                result.append(None)
            elif self._blob_as_descriptor:
                result.append(blob.to_descriptor().serialize())
            elif self._blob_parallelism > 1:
                result.append(None)
                blobs_to_resolve.append(
                    (field_name, row_index, 'array', element_index, blob)
                )
            else:
                result.append(blob.to_data())
        return result

    def _map_value_for_arrow(
        self,
        blob_map,
        field_name,
        row_index,
        blobs_to_resolve,
    ):
        if blob_map is None:
            return None
        if blob_map is Blob.MAP_PLACE_HOLDER:
            raise RuntimeError(
                "Blob placeholder is not supported by FormatBlobReader yet."
            )
        if None in blob_map:
            raise ValueError(
                "MAP<X, BLOB> with null keys cannot be converted to a PyArrow Map."
            )

        result = []
        for entry_index, (key, blob) in enumerate(blob_map.items()):
            if blob is None:
                value = None
            elif self._blob_as_descriptor:
                value = blob.to_descriptor().serialize()
            elif self._blob_parallelism > 1:
                value = None
                blobs_to_resolve.append(
                    (field_name, row_index, 'map', entry_index, blob)
                )
            else:
                value = blob.to_data()
            result.append((key, value))
        return result

    def close(self):
        self._blob_iterator = None
        if self._input_stream is not None:
            self._input_stream.close()
            self._input_stream = None

    @property
    def record_count(self) -> int:
        if self._is_video:
            return self._video_meta.record_count
        return len(self.blob_lengths)

    def _read_index(self) -> None:
        if self._is_video:
            self._video_meta = VideoFileMeta(
                self._input_stream, self._file_size
            )
            return

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

        if self._is_video:
            self._video_meta.select(row_indices)
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


class VideoFileMeta:
    """Validated embedded index of a ``.video`` file."""

    VERSION = 1
    MAGIC_NUMBER = 0x4F454449
    FOOTER_SIZE = 21
    NULL_REFERENCE = -1
    PLACE_HOLDER_REFERENCE = -2

    def __init__(self, stream, file_size: int):
        if file_size < self.FOOTER_SIZE:
            raise IOError(
                "Corrupt video file: file is smaller than its footer."
            )
        footer_start = file_size - self.FOOTER_SIZE
        stream.seek(footer_start)
        footer = stream.read(self.FOOTER_SIZE)
        if len(footer) != self.FOOTER_SIZE:
            raise IOError("Corrupt video file: cannot read footer.")
        lengths = struct.unpack('<IIIIIB', footer)
        index_lengths = lengths[:4]
        magic, version = lengths[4:]
        if magic != self.MAGIC_NUMBER:
            raise IOError(
                "Corrupt video file: invalid footer magic %s." % magic
            )
        if version != self.VERSION:
            raise IOError("Unsupported video format version: %s" % version)

        total_index_length = sum(index_lengths)
        if total_index_length > footer_start:
            raise IOError("Corrupt video file: indexes exceed the file size.")
        index_start = footer_start - total_index_length
        indexes = []
        offset = index_start
        for name, length in zip(
                ("physical video", "run length", "run reference", "first frame"),
                index_lengths):
            stream.seek(offset)
            raw = stream.read(length)
            if len(raw) != length:
                raise IOError(
                    "Corrupt video file: cannot read %s index." % name
                )
            indexes.append(DeltaVarintCompressor.decompress(raw))
            offset += length

        physical_lengths, run_lengths, references, first_frames = indexes
        physical_offsets = []
        payload_offset = 0
        for ordinal, length in enumerate(physical_lengths):
            if length <= 0 or length > index_start - payload_offset:
                raise IOError(
                    "Corrupt video file: invalid physical video length %s "
                    "at ordinal %s." % (length, ordinal)
                )
            physical_offsets.append(payload_offset)
            payload_offset += length
        if payload_offset != index_start:
            raise IOError(
                "Corrupt video file: indexed videos use %s bytes, but payload "
                "region contains %s bytes." % (payload_offset, index_start)
            )

        if not (len(run_lengths) == len(references) == len(first_frames)):
            raise IOError(
                "Corrupt video file: run indexes have different counts."
            )
        run_ends = []
        row_count = 0
        for run, (length, reference, first_frame) in enumerate(zip(
                run_lengths, references, first_frames)):
            if length <= 0:
                raise IOError(
                    "Corrupt video file: invalid run length %s at run %s."
                    % (length, run)
                )
            if (
                reference not in (
                    self.NULL_REFERENCE, self.PLACE_HOLDER_REFERENCE
                )
                and (reference < 0 or reference >= len(physical_lengths))
            ):
                raise IOError(
                    "Corrupt video file: run %s references physical video %s, "
                    "but physical video count is %s."
                    % (run, reference, len(physical_lengths))
                )
            if reference >= 0 and first_frame < 0:
                raise IOError(
                    "Corrupt video file: run %s has negative first frame %s."
                    % (run, first_frame)
                )
            row_count += length
            run_ends.append(row_count)

        self.physical_lengths = physical_lengths
        self.physical_offsets = physical_offsets
        self.run_ends = run_ends
        self.references = references
        self.first_frames = first_frames
        self.row_count = row_count
        self.selected_positions = None

    @property
    def record_count(self) -> int:
        return (
            self.row_count
            if self.selected_positions is None
            else len(self.selected_positions)
        )

    def select(self, row_indices) -> None:
        selected = []
        for value in row_indices:
            position = int(value)
            if position < 0 or position >= self.row_count:
                raise IndexError(
                    "Video row index %s is out of range, record count: %s."
                    % (position, self.row_count)
                )
            selected.append(position)
        self.selected_positions = selected

    def logical_position(self, returned_row: int) -> int:
        if self.selected_positions is None:
            return returned_row
        return self.selected_positions[returned_row]

    def frame(self, returned_row: int):
        logical = self.logical_position(returned_row)
        run = bisect.bisect_left(self.run_ends, logical + 1)
        reference = self.references[run]
        if reference == self.NULL_REFERENCE:
            return None
        if reference == self.PLACE_HOLDER_REFERENCE:
            return Blob.PLACE_HOLDER
        run_start = 0 if run == 0 else self.run_ends[run - 1]
        return (
            self.physical_offsets[reference],
            self.physical_lengths[reference],
            self.first_frames[run] + logical - run_start,
        )


class VideoFrameRecordIterator:

    def __init__(self, file_io, file_path, meta, field):
        self.file_io = file_io
        self.file_path = file_path
        self.meta = meta
        self.field = field
        self.current_position = 0
        self._uri_reader = UriReader.from_file(file_io)

    def __iter__(self):
        return self

    def __next__(self):
        if self.current_position >= self.meta.record_count:
            raise StopIteration
        value = self.meta.frame(self.current_position)
        if isinstance(value, tuple):
            offset, length, frame_index = value
            descriptor = VideoFrameDescriptor(
                self.file_path, offset, length, frame_index
            )
            value = Blob.from_descriptor(self._uri_reader, descriptor)
        self.current_position += 1
        return GenericRow([value], [self.field], RowKind.INSERT)


class BlobRecordIterator:
    MAGIC_NUMBER_SIZE = 4
    METADATA_OVERHEAD = 16
    ARRAY_HEADER_SIZE = 9
    ARRAY_VERSION = 1
    ARRAY_MAGIC_NUMBER = 1094861634
    ARRAY_NULL_ELEMENT_LENGTH = -1
    ARRAY_INDEX_LENGTH_SIZE = 4
    MIN_ARRAY_PAYLOAD_LENGTH = ARRAY_HEADER_SIZE + ARRAY_INDEX_LENGTH_SIZE
    MAP_HEADER_SIZE = 9
    MAP_VERSION = 1
    MAP_MAGIC_NUMBER = 0x4D424342
    MAP_NULL_KEY_LENGTH = -1
    MAP_NULL_VALUE_LENGTH = -1
    MAP_INDEX_LENGTH_SIZE = 4
    MAP_INDEX_LENGTHS_SIZE = MAP_INDEX_LENGTH_SIZE * 2
    MIN_MAP_PAYLOAD_LENGTH = MAP_HEADER_SIZE + MAP_INDEX_LENGTHS_SIZE
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
        if isinstance(self.field.type, MapType) and not is_map_blob_type(self.field.type):
            raise ValueError(
                f"Map-Blob value type must be BLOB, but is {self.field.type.value}."
            )
        self.is_array_blob = is_array_blob_type(self.field.type)
        self.is_map_blob = is_map_blob_type(self.field.type)
        self.map_key_serializer = (
            create_map_blob_key_serializer(self.field.type.key)
            if self.is_map_blob
            else None
        )
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
            if self.is_map_blob:
                placeholder = Blob.MAP_PLACE_HOLDER
            elif self.is_array_blob:
                placeholder = Blob.ARRAY_PLACE_HOLDER
            else:
                placeholder = Blob.PLACE_HOLDER
            return GenericRow([placeholder], fields, RowKind.INSERT)
        # Create blob reference for the current blob
        # Skip magic number (4 bytes) and exclude length (8 bytes) + CRC (4 bytes) = 12 bytes
        blob_offset = self.blob_offsets[self.current_position] + self.MAGIC_NUMBER_SIZE  # Skip magic number
        blob_length = length - self.METADATA_OVERHEAD
        if self.is_map_blob:
            blob = self._read_blob_map(blob_offset, blob_length)
        elif self.is_array_blob:
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

            element_data = None
            if not self.blob_as_descriptor:
                stream.seek(element_data_start)
                element_data = self._read_fully_from(stream, element_data_length)
                if len(element_data) != element_data_length:
                    raise IOError("Invalid ARRAY<BLOB> payload: cannot read element data")

            blobs = []
            element_offset = element_data_start
            data_offset = 0
            for element_length in element_lengths:
                if element_length == self.ARRAY_NULL_ELEMENT_LENGTH:
                    blobs.append(None)
                    continue
                if self.blob_as_descriptor:
                    blobs.append(
                        Blob.from_file(self.file_io, self.file_path, element_offset, element_length)
                    )
                else:
                    blobs.append(Blob.from_data(
                        element_data[data_offset:data_offset + element_length]
                    ))
                element_offset += element_length
                data_offset += element_length
            return blobs
        finally:
            if close_stream:
                stream.close()

    def _read_blob_map(self, position: int, length: int):
        if position < 0 or length < self.MIN_MAP_PAYLOAD_LENGTH:
            raise ValueError(
                f"Invalid MAP<X, BLOB> payload position or length: {position}, {length}"
            )

        stream = self.input_stream
        close_stream = False
        if stream is None:
            stream = self.file_io.new_input_stream(self.file_path)
            close_stream = True
        try:
            stream.seek(position)
            header = self._read_fully_from(stream, self.MAP_HEADER_SIZE)
            if len(header) != self.MAP_HEADER_SIZE:
                raise IOError("Invalid MAP<X, BLOB> payload: cannot read header")
            magic, version, entry_count = struct.unpack('<IBI', header)
            if magic != self.MAP_MAGIC_NUMBER:
                raise ValueError(f"Invalid MAP<X, BLOB> payload magic number: {magic}")
            if version != self.MAP_VERSION:
                raise ValueError(f"Unsupported MAP<X, BLOB> payload version: {version}")
            if entry_count > 0x7fffffff:
                raise ValueError(f"Invalid MAP<X, BLOB> entry count: {entry_count}")

            payload_end = position + length
            data_start = position + self.MAP_HEADER_SIZE
            index_lengths_position = payload_end - self.MAP_INDEX_LENGTHS_SIZE
            stream.seek(index_lengths_position)
            index_length_bytes = self._read_fully_from(
                stream, self.MAP_INDEX_LENGTHS_SIZE
            )
            if len(index_length_bytes) != self.MAP_INDEX_LENGTHS_SIZE:
                raise IOError("Invalid MAP<X, BLOB> payload: cannot read index lengths")
            key_index_length, value_index_length = struct.unpack('<II', index_length_bytes)
            self._check_map_index_lengths(
                key_index_length,
                value_index_length,
                length,
                entry_count,
            )

            value_index_start = index_lengths_position - value_index_length
            key_index_start = value_index_start - key_index_length
            stream.seek(key_index_start)
            key_index_bytes = self._read_fully_from(stream, key_index_length)
            if len(key_index_bytes) != key_index_length:
                raise IOError("Invalid MAP<X, BLOB> payload: cannot read key index")
            stream.seek(value_index_start)
            value_index_bytes = self._read_fully_from(stream, value_index_length)
            if len(value_index_bytes) != value_index_length:
                raise IOError("Invalid MAP<X, BLOB> payload: cannot read value index")

            self._validate_map_index(key_index_bytes, "key")
            self._validate_map_index(value_index_bytes, "value")
            try:
                key_lengths = DeltaVarintCompressor.decompress(key_index_bytes)
            except RuntimeError as error:
                raise ValueError("Invalid MAP<X, BLOB> key index.") from error
            try:
                value_lengths = DeltaVarintCompressor.decompress(value_index_bytes)
            except RuntimeError as error:
                raise ValueError("Invalid MAP<X, BLOB> value index.") from error

            data_length = key_index_start - data_start
            key_data_length = self._check_map_key_lengths(
                key_lengths, data_length, entry_count
            )
            value_data_length = data_length - key_data_length
            self._check_map_value_lengths(
                value_lengths, value_data_length, entry_count
            )

            stream.seek(data_start)
            key_data = self._read_fully_from(stream, key_data_length)
            if len(key_data) != key_data_length:
                raise IOError("Invalid MAP<X, BLOB> payload: cannot read key data")
            keys = []
            key_data_offset = 0
            for key_length in key_lengths:
                if key_length == self.MAP_NULL_KEY_LENGTH:
                    keys.append(None)
                    continue
                serialized_key = key_data[
                    key_data_offset:key_data_offset + key_length
                ]
                try:
                    keys.append(self.map_key_serializer.deserialize(serialized_key))
                except ValueError as error:
                    raise ValueError("Invalid MAP<X, BLOB> key.") from error
                key_data_offset += key_length

            value_data_start = data_start + key_data_length
            value_data = None
            if not self.blob_as_descriptor:
                stream.seek(value_data_start)
                value_data = self._read_fully_from(stream, value_data_length)
                if len(value_data) != value_data_length:
                    raise IOError("Invalid MAP<X, BLOB> payload: cannot read value data")

            result = {}
            value_offset = value_data_start
            value_data_offset = 0
            for key, value_length in zip(keys, value_lengths):
                if value_length == self.MAP_NULL_VALUE_LENGTH:
                    value = None
                elif self.blob_as_descriptor:
                    value = Blob.from_file(
                        self.file_io, self.file_path, value_offset, value_length
                    )
                else:
                    value = Blob.from_data(
                        value_data[value_data_offset:value_data_offset + value_length]
                    )
                if value_length != self.MAP_NULL_VALUE_LENGTH:
                    value_offset += value_length
                    value_data_offset += value_length
                result[key] = value
            if len(result) != entry_count:
                raise ValueError("Invalid MAP<X, BLOB> payload: duplicate key.")
            return result
        finally:
            if close_stream:
                stream.close()

    @classmethod
    def _check_map_index_lengths(
        cls,
        key_index_length: int,
        value_index_length: int,
        payload_length: int,
        entry_count: int,
    ) -> None:
        maximum_indexes_length = payload_length - cls.MIN_MAP_PAYLOAD_LENGTH
        if key_index_length > 0x7fffffff or key_index_length > maximum_indexes_length:
            raise ValueError(
                f"Invalid MAP<X, BLOB> key index length: {key_index_length}"
            )
        if value_index_length > 0x7fffffff or value_index_length > maximum_indexes_length:
            raise ValueError(
                f"Invalid MAP<X, BLOB> value index length: {value_index_length}"
            )
        if key_index_length + value_index_length > maximum_indexes_length:
            raise ValueError("MAP<X, BLOB> indexes exceed the payload length.")
        if entry_count > key_index_length:
            raise ValueError("MAP<X, BLOB> entry count exceeds key index length.")
        if entry_count > value_index_length:
            raise ValueError("MAP<X, BLOB> entry count exceeds value index length.")

    def _check_map_key_lengths(
        self, key_lengths, max_data_length: int, entry_count: int
    ) -> int:
        if len(key_lengths) != entry_count:
            raise ValueError(
                "MAP<X, BLOB> entry count does not match key index length."
            )

        key_data_length = 0
        fixed_key_length = self.map_key_serializer.fixed_length
        for key_length in key_lengths:
            if key_length == self.MAP_NULL_KEY_LENGTH:
                continue
            if key_length < 0:
                raise ValueError(f"Invalid MAP<X, BLOB> key length: {key_length}")
            if key_length > 0x7fffffff:
                raise ValueError(f"MAP<X, BLOB> key is too large: {key_length}")
            if fixed_key_length >= 0 and key_length != fixed_key_length:
                raise ValueError(
                    f"Invalid MAP<X, BLOB> fixed-width key length: {key_length}"
                )
            if key_length > max_data_length - key_data_length:
                raise ValueError(
                    "MAP<X, BLOB> key lengths exceed the payload data length."
                )
            key_data_length += key_length
        return key_data_length

    def _check_map_value_lengths(
        self, value_lengths, max_data_length: int, entry_count: int
    ) -> None:
        if len(value_lengths) != entry_count:
            raise ValueError(
                "MAP<X, BLOB> entry count does not match value index length."
            )

        value_data_length = 0
        for value_length in value_lengths:
            if value_length == self.MAP_NULL_VALUE_LENGTH:
                continue
            if value_length < 0:
                raise ValueError(f"Invalid MAP<X, BLOB> value length: {value_length}")
            if not self.blob_as_descriptor and value_length > 0x7fffffff:
                raise ValueError(
                    f"MAP<X, BLOB> inline value is too large: {value_length}"
                )
            if value_length > max_data_length - value_data_length:
                raise ValueError(
                    "MAP<X, BLOB> value lengths exceed the payload data length."
                )
            value_data_length += value_length
        if value_data_length != max_data_length:
            raise ValueError(
                "MAP<X, BLOB> key/value lengths do not match the payload data length."
            )

    @staticmethod
    def _validate_map_index(index_bytes: bytes, index_name: str) -> None:
        varint_length = 0
        for value in index_bytes:
            varint_length += 1
            if varint_length > 10:
                raise ValueError(f"Invalid MAP<X, BLOB> {index_name} index.")
            if value & 0x80 == 0:
                varint_length = 0
        if varint_length != 0:
            raise ValueError(f"Invalid MAP<X, BLOB> {index_name} index.")

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
