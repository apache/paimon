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
from typing import List, Optional, Any, Iterator

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import RecordBatch

from pypaimon.common.delta_varint_compressor import DeltaVarintCompressor
from pypaimon.common.file_io import FileIO
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import DataField, PyarrowFieldParser, AtomicType
from pypaimon.table.row.blob import Blob
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.row_kind import RowKind


class FormatBlobReader(RecordBatchReader):

    def __init__(self, file_io: FileIO, file_path: str, read_fields: List[str],
                 full_fields: List[DataField], push_down_predicate: Any, blob_as_descriptor: bool,
                 batch_size: int = 4096):
        self._file_io = file_io
        self._file_path = file_path
        self._push_down_predicate = push_down_predicate
        self._blob_as_descriptor = blob_as_descriptor
        self._batch_size = batch_size
        # Get file size
        self._file_size = file_io.get_file_size(file_path)

        # Initialize the low-level blob format reader
        self.file_path = file_path
        self.blob_lengths: List[int] = []
        self.blob_offsets: List[int] = []
        self.returned = False
        self._read_index()

        # Set up fields and schema
        if len(read_fields) > 1:
            raise RuntimeError("Blob reader only supports one field.")
        self._fields = read_fields
        full_fields_map = {field.name: field for field in full_fields}
        projected_data_fields = [full_fields_map[name] for name in read_fields]
        self._schema = PyarrowFieldParser.from_paimon_schema(projected_data_fields)

        # Initialize iterator
        self._blob_iterator = None
        self._current_batch = None

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
                self.blob_offsets, self._fields[0]
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

        try:
            while True:
                blob_row = next(self._blob_iterator)
                if blob_row is None:
                    break
                blob = blob_row.values[0]
                for field_name in self._fields:
                    blob_descriptor = blob.to_descriptor()
                    if self._blob_as_descriptor:
                        blob_data = blob_descriptor.serialize()
                    else:
                        blob_data = blob.to_data()
                    pydict_data[field_name].append(blob_data)

                records_in_batch += 1
                if records_in_batch >= read_size:
                    break

        except StopIteration:
            # Stop immediately when StopIteration occurs
            pass

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

    def close(self):
        self._blob_iterator = None

    def _read_index(self) -> None:
        with self._file_io.new_input_stream(self.file_path) as f:
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
                blob_offsets.append(offset)
                offset += length
            self.blob_lengths = blob_lengths
            self.blob_offsets = blob_offsets


class BlobRecordIterator:
    MAGIC_NUMBER_SIZE = 4
    METADATA_OVERHEAD = 16

    def __init__(self, file_io: FileIO, file_path: str, blob_lengths: List[int],
                 blob_offsets: List[int], field_name: str):
        self.file_io = file_io
        self.file_path = file_path
        self.field_name = field_name
        self.blob_lengths = blob_lengths
        self.blob_offsets = blob_offsets
        self.current_position = 0

    def __iter__(self) -> Iterator[GenericRow]:
        return self

    def __next__(self) -> GenericRow:
        if self.current_position >= len(self.blob_lengths):
            raise StopIteration
        # Create blob reference for the current blob
        # Skip magic number (4 bytes) and exclude length (8 bytes) + CRC (4 bytes) = 12 bytes
        blob_offset = self.blob_offsets[self.current_position] + self.MAGIC_NUMBER_SIZE  # Skip magic number
        blob_length = self.blob_lengths[self.current_position] - self.METADATA_OVERHEAD
        blob = Blob.from_file(self.file_io, self.file_path, blob_offset, blob_length)
        self.current_position += 1
        fields = [DataField(0, self.field_name, AtomicType("BLOB"))]
        return GenericRow([blob], fields, RowKind.INSERT)

    def returned_position(self) -> int:
        return self.current_position
