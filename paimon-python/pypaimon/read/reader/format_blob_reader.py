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
from pypaimon.schema.data_types import DataField, PyarrowFieldParser
from pypaimon.table.row.blob import Blob, BlobDescriptor, BlobRef
from pypaimon.table.row.generic_row import GenericRow


class FormatBlobReader(RecordBatchReader):

    def __init__(self, file_io: FileIO, file_path: str, read_fields: List[str],
                 full_fields: List[DataField], push_down_predicate: Any):
        self._file_io = file_io
        self._file_path = file_path
        self._push_down_predicate = push_down_predicate

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
            raise RuntimeError("BlobFileFormat only support one field.")
        self._fields = read_fields
        full_fields_map = {field.name: field for field in full_fields}
        projected_data_fields = [full_fields_map[name] for name in read_fields]
        self._schema = PyarrowFieldParser.from_paimon_schema(projected_data_fields)

        # Initialize iterator
        self._blob_iterator = None
        self._current_batch = None

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        if self._blob_iterator is None:
            if self.returned:
                return None
            self.returned = True
            batch_iterator = BlobRecordIterator(self.file_path, self.blob_lengths, self.blob_offsets, self._fields[0])
            self._blob_iterator = iter(batch_iterator)

        # Collect records for this batch
        pydict_data = {name: [] for name in self._fields}
        records_in_batch = 0

        try:
            while True:
                # Get next blob record
                blob_row = next(self._blob_iterator)
                # Check if first read returns None, stop immediately
                if blob_row is None:
                    break

                # Extract blob data from the row
                blob = blob_row.values[0]  # Blob files have single blob field

                # Convert blob to appropriate format for each requested field
                for field_name in self._fields:
                    # For blob files, all fields should contain blob data
                    if isinstance(blob, Blob):
                        blob_data = blob.to_data()
                    else:
                        blob_data = bytes(blob) if blob is not None else None
                    pydict_data[field_name].append(blob_data)

                records_in_batch += 1

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
        with open(self.file_path, 'rb') as f:
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

            # Decompress blob lengths
            blob_lengths = DeltaVarintCompressor.decompress(index_bytes)

            # Calculate blob offsets
            blob_offsets = []
            offset = 0
            for length in blob_lengths:
                blob_offsets.append(offset)
                offset += length
            self.blob_lengths = blob_lengths
            self.blob_offsets = blob_offsets


class BlobRecordIterator:
    """Iterator for blob records in a blob file."""

    def __init__(self, file_path: str, blob_lengths: List[int], blob_offsets: List[int], field_name: str):
        """
        Initialize blob record iterator.

        Args:
            file_path: Path to the blob file
            blob_lengths: List of blob lengths
            blob_offsets: List of blob offsets
        """
        self.file_path = file_path
        self.field_name = field_name
        self.blob_lengths = blob_lengths
        self.blob_offsets = blob_offsets
        self.current_position = 0

    def __iter__(self) -> Iterator[GenericRow]:
        """Return iterator."""
        return self

    def __next__(self) -> GenericRow:
        """
        Get next blob record.

        Returns:
            GenericRow containing a single blob field

        Raises:
            StopIteration: When no more records
        """
        if self.current_position >= len(self.blob_lengths):
            raise StopIteration

        # Create blob reference for the current blob
        # Skip magic number (4 bytes) and exclude length (8 bytes) + CRC (4 bytes) = 12 bytes
        blob_offset = self.blob_offsets[self.current_position] + 4  # Skip magic number
        blob_length = self.blob_lengths[self.current_position] - 16  # Exclude magic(4) + length(8) + CRC(4)

        # Create BlobDescriptor for this blob
        descriptor = BlobDescriptor(self.file_path, blob_offset, blob_length)
        blob = BlobRef(descriptor)

        self.current_position += 1

        # Return as GenericRow with single blob field
        from pypaimon.schema.data_types import DataField, AtomicType
        from pypaimon.table.row.row_kind import RowKind

        fields = [DataField(0, self.field_name, AtomicType("BLOB"))]
        return GenericRow([blob], fields, RowKind.INSERT)

    def returned_position(self) -> int:
        """Get current position in the iterator."""
        return self.current_position

    def file_path(self) -> str:
        """Get the file path."""
        return self.file_path

    def release_batch(self) -> None:
        """Release batch resources (no-op)."""
        pass
