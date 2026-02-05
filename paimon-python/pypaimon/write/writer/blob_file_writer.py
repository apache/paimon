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
#  limitations under the License.
################################################################################

import pyarrow as pa
from pathlib import Path

from pypaimon.write.blob_format_writer import BlobFormatWriter
from pypaimon.table.row.generic_row import GenericRow, RowKind
from pypaimon.table.row.blob import Blob, BlobData, BlobDescriptor
from pypaimon.schema.data_types import DataField, PyarrowFieldParser


class BlobFileWriter:
    """
    Single blob file writer
    Writes rows one by one and tracks file size.
    """

    def __init__(self, file_io, file_path: Path):
        self.file_io = file_io
        self.file_path = file_path
        self.output_stream = file_io.new_output_stream(file_path)
        self.writer = BlobFormatWriter(self.output_stream)
        self.row_count = 0
        self.closed = False

    def write_row(self, row_data: pa.Table):
        """Write a single row to the blob file."""
        if row_data.num_rows != 1:
            raise ValueError(f"Expected 1 row, got {row_data.num_rows}")

        # Convert PyArrow row to GenericRow
        records_dict = row_data.to_pydict()
        field_name = row_data.schema[0].name
        col_data = records_dict[field_name][0]

        blob_data = self._to_blob(col_data)

        # Create GenericRow
        fields = [DataField(0, field_name, PyarrowFieldParser.to_paimon_type(row_data.schema[0].type, False))]
        row = GenericRow([blob_data], fields, RowKind.INSERT)

        # Write to blob format writer
        self.writer.add_element(row)
        self.row_count += 1

    def _to_blob(self, col_data) -> Blob:
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
                descriptor = BlobDescriptor.deserialize(col_data)
                uri_reader = self.file_io.uri_reader_factory.create(descriptor.uri)
                return Blob.from_descriptor(uri_reader, descriptor)
            else:
                return BlobData(col_data)

        raise ValueError(
            "Blob field value must be bytes/blob or serialized BlobDescriptor bytes, "
            f"got {type(col_data)}."
        )

    @staticmethod
    def _deserialize_descriptor_or_none(raw: bytes):
        if not BlobDescriptor.is_blob_descriptor(raw):
            return None
        return BlobDescriptor.deserialize(raw)

    def reach_target_size(self, suggested_check: bool, target_size: int) -> bool:
        return self.writer.reach_target_size(suggested_check, target_size)

    def close(self) -> int:
        if self.closed:
            return self.file_io.get_file_size(self.file_path)

        self.writer.close()
        self.closed = True

        # Get actual file size
        file_size = self.file_io.get_file_size(self.file_path)
        return file_size

    def abort(self):
        """Abort the writer and delete the file."""
        if not self.closed:
            try:
                if hasattr(self.output_stream, 'close'):
                    self.output_stream.close()
            except Exception:
                pass
            self.closed = True

        # Delete the file
        self.file_io.delete_quietly(self.file_path)
