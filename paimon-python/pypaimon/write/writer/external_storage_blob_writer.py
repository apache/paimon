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

import logging
import uuid
from typing import Dict, Set

import pyarrow as pa

from pypaimon.table.row.blob import BlobDescriptor
from pypaimon.write.blob_format_writer import BlobFormatWriter
from pypaimon.write.writer.blob_file_writer import BlobFileWriter

logger = logging.getLogger(__name__)

# Size of the magic number written before each blob record in BlobFormatWriter
_BLOB_MAGIC_SIZE = 4


class ExternalStorageBlobWriter:
    """
    Writes raw BLOB data from external-storage fields to an external storage path
    and replaces the column values with serialized BlobDescriptor bytes.

    This aligns with Java's ExternalStorageBlobWriter behavior:
    - For each external-storage field, maintains an independent BlobFileWriter
    - Writes blob data to external storage .blob files
    - Replaces column values with BlobDescriptor(uri, offset, length)
    - External files are NOT tracked in Paimon snapshot metadata
    - Orphan file cleanup does NOT apply to the external storage path
    - Path is flat: {externalStoragePath}/{prefix}{uuid}-{counter}.blob
    - Rolling check happens after write (next write creates new file)
    """

    def __init__(
        self,
        file_io,
        external_storage_path: str,
        external_storage_fields: Set[str],
        blob_target_file_size: int,
        data_file_prefix: str = "data-",
    ):
        self._file_io = file_io
        self._external_storage_path = external_storage_path.rstrip('/')
        self._external_storage_fields = external_storage_fields
        self._blob_target_file_size = blob_target_file_size
        self._data_file_prefix = data_file_prefix
        # Per-field BlobFileWriter instances
        self._field_writers: Dict[str, BlobFileWriter] = {}
        # Per-field flag: whether current writer has reached target size (roll on next write)
        self._field_needs_roll: Dict[str, bool] = {}
        # Fixed UUID for this writer instance + incrementing path counter (aligned with Java)
        self._writer_uuid = str(uuid.uuid4())
        self._path_count = 0
        self._closed = False

    def transform_batch(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """
        Transform a RecordBatch by writing external-storage field data to
        external blob files and replacing column values with serialized BlobDescriptor bytes.

        Args:
            batch: The input RecordBatch containing raw blob data in external-storage columns.

        Returns:
            A new RecordBatch where external-storage columns are replaced with
            serialized BlobDescriptor bytes.
        """
        if batch.num_rows == 0:
            return batch

        # Find which columns in this batch are external-storage fields
        columns_to_transform = []
        for field_name in self._external_storage_fields:
            if field_name in batch.schema.names:
                columns_to_transform.append(field_name)

        if not columns_to_transform:
            return batch

        # Build new columns array
        new_columns = []
        for i, field in enumerate(batch.schema):
            if field.name in columns_to_transform:
                transformed_column = self._transform_column(field.name, batch.column(i))
                new_columns.append(transformed_column)
            else:
                new_columns.append(batch.column(i))

        return pa.RecordBatch.from_arrays(new_columns, schema=batch.schema)

    def _transform_column(self, field_name: str, column: pa.Array) -> pa.Array:
        """Transform a single column: write blob data to external storage and return descriptor bytes.

        Handles both raw bytes and serialized BlobDescriptor bytes as input.
        For BlobDescriptor input, BlobFileWriter will construct a BlobRef and
        stream the original data from the source file (aligned with Java behavior).
        """
        descriptor_values = []
        values = column.to_pylist()

        for value in values:
            if value is None:
                descriptor_values.append(None)
                continue

            # Write to external storage and get descriptor.
            # BlobFileWriter._to_blob() handles both raw bytes and BlobDescriptor bytes:
            # - raw bytes → BlobData → direct write
            # - BlobDescriptor bytes → BlobRef → stream read from source → write
            descriptor = self._write_to_external_storage(field_name, value)
            descriptor_values.append(descriptor.serialize())

        return pa.array(descriptor_values, type=column.type)

    def _write_to_external_storage(self, field_name: str, value: bytes) -> BlobDescriptor:
        """Write blob data to external storage and return a BlobDescriptor.

        Accepts both raw bytes and serialized BlobDescriptor bytes. BlobFileWriter
        handles the conversion internally (BlobDescriptor → BlobRef → stream read).

        In blob format, each record is:
            [Magic (4B)] [Raw Data (variable)] [Length (8B)] [CRC32 (4B)]

        The BlobDescriptor offset points to the raw data start (after magic),
        and length is the actual raw data size. This aligns with Java's
        ExternalStorageBlobWriter behavior.
        """
        writer = self._get_or_create_writer(field_name)

        # The offset of raw data in the file = current position + magic size
        raw_data_offset = writer.writer.position + _BLOB_MAGIC_SIZE

        # Write using BlobFileWriter (standard blob format)
        # BlobFileWriter._to_blob() handles both raw bytes and BlobDescriptor bytes
        single_row = pa.table({field_name: [value]})
        writer.write_row(single_row)

        # Calculate actual blob data length from position difference
        # (works for both raw bytes and BlobDescriptor/BlobRef input)
        blob_length = writer.writer.position - raw_data_offset - BlobFormatWriter.METADATA_SIZE

        # After write, check if target size reached (aligned with Java's post-write check)
        self._mark_roll_if_needed(field_name)

        # Build the descriptor pointing to external storage
        file_uri = str(writer.file_path)
        return BlobDescriptor(file_uri, raw_data_offset, blob_length)

    def _get_or_create_writer(self, field_name: str) -> BlobFileWriter:
        """Get or create a BlobFileWriter for the given field.

        Aligned with Java's rolling behavior: check happens after write.
        If the previous write caused the writer to reach target size,
        close and create a new one on the next call.
        """
        # Check if previous write triggered a roll
        if self._field_needs_roll.get(field_name, False):
            writer = self._field_writers.get(field_name)
            if writer is not None and not writer.closed:
                writer.close()
            self._field_writers.pop(field_name, None)
            self._field_needs_roll[field_name] = False

        writer = self._field_writers.get(field_name)
        if writer is None:
            new_path = self._generate_external_blob_path()
            writer = BlobFileWriter(self._file_io, new_path)
            self._field_writers[field_name] = writer
            logger.debug("Created new external storage blob file: %s for field: %s", new_path, field_name)

        return writer

    def _mark_roll_if_needed(self, field_name: str):
        """After writing, check if target size reached and mark for rolling on next write."""
        writer = self._field_writers.get(field_name)
        if writer is not None and writer.reach_target_size(self._blob_target_file_size):
            self._field_needs_roll[field_name] = True

    def _generate_external_blob_path(self) -> str:
        """Generate a new external storage blob file path.

        Aligned with Java's DataFilePathFactory.newExternalStorageBlobPath():
        flat structure under externalStoragePath, filename = {prefix}-{uuid}-{counter}.blob
        """
        file_name = f"{self._data_file_prefix}{self._writer_uuid}-{self._path_count}.blob"
        self._path_count += 1
        return f"{self._external_storage_path}/{file_name}"

    def close(self):
        """Close all field writers."""
        if self._closed:
            return
        for writer in self._field_writers.values():
            if not writer.closed:
                writer.close()
        self._field_writers.clear()
        self._closed = True

    def abort(self):
        """Abort all writers.

        Aligned with Java's ExternalStorageBlobFieldWriter.abort():
        - The current in-progress (not yet finalized) file is deleted via writer.abort()
        - Already finalized/closed external files remain on disk (orphan cleanup
          does not cover the external storage path)
        """
        if self._closed:
            return
        for writer in self._field_writers.values():
            if not writer.closed:
                writer.abort()
        self._field_writers.clear()
        self._closed = True
