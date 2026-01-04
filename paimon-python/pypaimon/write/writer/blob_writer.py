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

import logging
import uuid
import pyarrow as pa
from typing import Optional, Tuple, Dict

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.data.timestamp import Timestamp
from pypaimon.write.writer.append_only_data_writer import AppendOnlyDataWriter
from pypaimon.write.writer.blob_file_writer import BlobFileWriter

logger = logging.getLogger(__name__)

CHECK_ROLLING_RECORD_CNT = 1000


class BlobWriter(AppendOnlyDataWriter):

    def __init__(self, table, partition: Tuple, bucket: int, max_seq_number: int, blob_column: str,
                 options: Dict[str, str] = None):
        super().__init__(table, partition, bucket, max_seq_number,
                         options, write_cols=[blob_column])

        # Override file format to "blob"
        self.file_format = CoreOptions.FILE_FORMAT_BLOB

        # Store blob column name for use in metadata creation
        self.blob_column = blob_column

        options = self.table.options
        self.blob_target_file_size = CoreOptions.blob_target_file_size(options)

        self.current_writer: Optional[BlobFileWriter] = None
        self.current_file_path: Optional[str] = None
        self.record_count = 0

        self.file_uuid = str(uuid.uuid4())
        self.file_count = 0

        logger.info(f"Initialized BlobWriter with blob file format, blob_target_file_size={self.blob_target_file_size}")

    def _check_and_roll_if_needed(self):
        if self.pending_data is None:
            return

        if self.blob_as_descriptor:
            # blob-as-descriptor=true: Write row by row and check actual file size
            for i in range(self.pending_data.num_rows):
                row_data = self.pending_data.slice(i, 1)
                self._write_row_to_file(row_data)
                self.record_count += 1

                if self.rolling_file(False):
                    self.close_current_writer()

            # All data has been written
            self.pending_data = None
        else:
            # blob-as-descriptor=false: Use blob_target_file_size instead of target_file_size
            current_size = self.pending_data.nbytes
            if current_size > self.blob_target_file_size:
                split_row = self._find_optimal_split_point(self.pending_data, self.blob_target_file_size)
                if split_row > 0:
                    data_to_write = self.pending_data.slice(0, split_row)
                    remaining_data = self.pending_data.slice(split_row)

                    self._write_data_to_file(data_to_write)
                    self.pending_data = remaining_data
                    self._check_and_roll_if_needed()

    def _write_row_to_file(self, row_data: pa.Table):
        """Write a single row to the current blob file. Opens a new file if needed."""
        if row_data.num_rows == 0:
            return

        if self.current_writer is None:
            self.open_current_writer()

        self.current_writer.write_row(row_data)
        # This ensures each row has a unique sequence number for data versioning and consistency
        self.sequence_generator.next()

    def open_current_writer(self):
        file_name = (f"{CoreOptions.data_file_prefix(self.options)}"
                     f"{self.file_uuid}-{self.file_count}.{self.file_format}")
        self.file_count += 1  # Increment counter for next file
        file_path = self._generate_file_path(file_name)
        self.current_file_path = file_path
        self.current_writer = BlobFileWriter(self.file_io, file_path, self.blob_as_descriptor)

    def rolling_file(self, force_check: bool = False) -> bool:
        if self.current_writer is None:
            return False

        should_check = force_check or (self.record_count % CHECK_ROLLING_RECORD_CNT == 0)
        return self.current_writer.reach_target_size(should_check, self.blob_target_file_size)

    def close_current_writer(self):
        """Close current writer and create metadata."""
        if self.current_writer is None:
            return

        file_size = self.current_writer.close()
        file_name = self.current_file_path.split('/')[-1]
        row_count = self.current_writer.row_count

        # Determine if this is an external path
        is_external_path = self.external_path_provider is not None
        external_path_str = self.current_file_path if is_external_path else None

        self._add_file_metadata(file_name, self.current_file_path, row_count, file_size, external_path_str)

        self.current_writer = None
        self.current_file_path = None

    def _write_data_to_file(self, data):
        """
        Override for blob format in normal mode (blob-as-descriptor=false).
        Only difference from parent: use shared UUID + counter for file naming.
        """
        if data.num_rows == 0:
            return

        # This ensures each row gets a unique sequence number, matching the behavior expected
        for _ in range(data.num_rows):
            self.sequence_generator.next()

        file_name = f"data-{self.file_uuid}-{self.file_count}.{self.file_format}"
        self.file_count += 1
        file_path = self._generate_file_path(file_name)

        # Write blob file (parent class already supports blob format)
        self.file_io.write_blob(file_path, data, self.blob_as_descriptor)

        file_size = self.file_io.get_file_size(file_path)

        is_external_path = self.external_path_provider is not None
        external_path_str = file_path if is_external_path else None

        # Reuse _add_file_metadata for consistency (blob table is append-only, no primary keys)
        self._add_file_metadata(file_name, file_path, data, file_size, external_path_str)

    def _add_file_metadata(self, file_name: str, file_path: str, data_or_row_count, file_size: int,
                           external_path: Optional[str] = None):
        """Add file metadata to committed_files."""
        from pypaimon.manifest.schema.data_file_meta import DataFileMeta
        from pypaimon.manifest.schema.simple_stats import SimpleStats
        from pypaimon.table.row.generic_row import GenericRow
        from pypaimon.schema.data_types import PyarrowFieldParser

        # Handle both Table and row_count
        if isinstance(data_or_row_count, pa.Table):
            data = data_or_row_count
            row_count = data.num_rows
            data_fields = PyarrowFieldParser.to_paimon_schema(data.schema)
            # Compute statistics across all batches, not just the first one
            # This ensures correct min/max/null_counts when data has multiple batches
            column_stats = {
                field.name: self._get_column_stats(data, field.name)
                for field in data_fields
            }
            min_value_stats = [column_stats[field.name]['min_values'] for field in data_fields]
            max_value_stats = [column_stats[field.name]['max_values'] for field in data_fields]
            value_null_counts = [column_stats[field.name]['null_counts'] for field in data_fields]
        else:
            # row_count only (from BlobFileWriter)
            row_count = data_or_row_count
            data_fields = [PyarrowFieldParser.to_paimon_schema(pa.schema([(self.blob_column, pa.large_binary())]))[0]]
            min_value_stats = [None]
            max_value_stats = [None]
            value_null_counts = [0]

        min_seq = self.sequence_generator.current - row_count
        max_seq = self.sequence_generator.current - 1
        self.sequence_generator.start = self.sequence_generator.current

        self.committed_files.append(DataFileMeta.create(
            file_name=file_name,
            file_size=file_size,
            row_count=row_count,
            min_key=GenericRow([], []),
            max_key=GenericRow([], []),
            key_stats=SimpleStats(GenericRow([], []), GenericRow([], []), []),
            value_stats=SimpleStats(
                GenericRow(min_value_stats, data_fields),
                GenericRow(max_value_stats, data_fields),
                value_null_counts),
            min_sequence_number=min_seq,
            max_sequence_number=max_seq,
            schema_id=self.table.table_schema.id,
            level=0,
            extra_files=[],
            creation_time=Timestamp.now(),
            delete_row_count=0,
            file_source=0,  # FileSource.APPEND = 0
            value_stats_cols=None,
            external_path=external_path,
            first_row_id=None,
            write_cols=self.write_cols,
            file_path=file_path,
        ))

    def prepare_commit(self):
        """Prepare commit, ensuring all data is written."""
        # Close current file if open (blob-as-descriptor=true mode)
        if self.current_writer is not None:
            self.close_current_writer()

        # Call parent to handle pending_data (blob-as-descriptor=false mode)
        return super().prepare_commit()

    def close(self):
        """Close current writer if open."""
        # Close current file if open (blob-as-descriptor=true mode)
        if self.current_writer is not None:
            self.close_current_writer()

        # Call parent to handle pending_data (blob-as-descriptor=false mode)
        super().close()

    def abort(self):
        if self.current_writer is not None:
            try:
                self.current_writer.abort()
            except Exception as e:
                logger.warning(f"Error aborting blob writer: {e}", exc_info=e)
            self.current_writer = None
            self.current_file_path = None
        super().abort()

    @staticmethod
    def _get_column_stats(data_or_batch, column_name: str):
        """
        Compute column statistics for a column in a Table or RecordBatch.
        """
        # Handle both Table and RecordBatch
        if isinstance(data_or_batch, pa.Table):
            column_array = data_or_batch.column(column_name)
        else:
            column_array = data_or_batch.column(column_name)

        # For blob data, don't generate min/max values
        return {
            "min_values": None,
            "max_values": None,
            "null_counts": column_array.null_count,
        }
