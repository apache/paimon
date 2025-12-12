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
from typing import List, Optional, Tuple


import pyarrow as pa

from pypaimon.data.timestamp import Timestamp
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.writer.data_writer import DataWriter

logger = logging.getLogger(__name__)


class DataBlobWriter(DataWriter):
    """
    A rolling file writer that handles both normal data and blob data. This writer creates separate
    files for normal columns and blob columns, managing their lifecycle independently.

    For example, given a table schema with normal columns (id INT, name STRING) and a blob column
    (data BLOB), this writer will create separate files for (id, name) and (data).

    Key features:
    - Blob data can roll independently when normal data doesn't need rolling
    - When normal data rolls, blob data MUST also be closed (Java behavior)
    - Blob data uses more aggressive rolling (smaller target size) to prevent memory issues
    - One normal data file may correspond to multiple blob data files
    - Blob data is written immediately to disk to prevent memory corruption
    - Blob file metadata is stored as separate DataFileMeta objects after normal file metadata

    Rolling behavior:
    - Normal data rolls: Both normal and blob writers are closed together, blob metadata added after normal metadata
    - Blob data rolls independently: Only blob writer is closed, blob metadata is cached until normal data rolls

    Metadata organization:
    - Normal file metadata is added first to committed_files
    - Blob file metadata is added after normal file metadata in committed_files
    - When blob rolls independently, metadata is cached until normal data rolls
    - Result: [normal_meta, blob_meta1, blob_meta2, blob_meta3, ...]

    Example file organization:
    committed_files = [
        normal_file1_meta,    # f1.parquet metadata
        blob_file1_meta,      # b1.blob metadata
        blob_file2_meta,      # b2.blob metadata
        blob_file3_meta,      # b3.blob metadata
        normal_file2_meta,    # f1-2.parquet metadata
        blob_file4_meta,      # b4.blob metadata
        blob_file5_meta,      # b5.blob metadata
    ]

    This matches the Java RollingBlobFileWriter behavior exactly.
    """

    # Constant for checking rolling condition periodically
    CHECK_ROLLING_RECORD_CNT = 1000

    def __init__(self, table, partition: Tuple, bucket: int, max_seq_number: int, options: CoreOptions = None):
        super().__init__(table, partition, bucket, max_seq_number, options)

        # Determine blob column from table schema
        self.blob_column_name = self._get_blob_columns_from_schema()

        # Split schema into normal and blob columns
        all_column_names = self.table.field_names
        self.normal_column_names = [col for col in all_column_names if col != self.blob_column_name]
        self.write_cols = self.normal_column_names

        # State management for blob writer
        self.record_count = 0
        self.closed = False

        # Track pending data for normal data only
        self.pending_normal_data: Optional[pa.Table] = None

        # Initialize blob writer with blob column name
        from pypaimon.write.writer.blob_writer import BlobWriter
        self.blob_writer = BlobWriter(
            table=self.table,
            partition=self.partition,
            bucket=self.bucket,
            max_seq_number=max_seq_number,
            blob_column=self.blob_column_name,
            options=options
        )

        logger.info(f"Initialized DataBlobWriter with blob column: {self.blob_column_name}")

    def _get_blob_columns_from_schema(self) -> str:
        blob_columns = []
        for field in self.table.table_schema.fields:
            type_str = str(field.type).lower()
            if 'blob' in type_str:
                blob_columns.append(field.name)

        # Validate blob column count (matching Java constraint)
        if len(blob_columns) == 0:
            raise ValueError("No blob field found in table schema.")
        elif len(blob_columns) > 1:
            raise ValueError("Limit exactly one blob field in one paimon table yet.")

        return blob_columns[0]  # Return single blob column name

    def _process_data(self, data: pa.RecordBatch) -> pa.RecordBatch:
        normal_data, _ = self._split_data(data)
        return normal_data

    def _merge_data(self, existing_data: pa.Table, new_data: pa.Table) -> pa.Table:
        return self._merge_normal_data(existing_data, new_data)

    def write(self, data: pa.RecordBatch):
        try:
            # Split data into normal and blob parts
            normal_data, blob_data = self._split_data(data)

            # Process and accumulate normal data
            processed_normal = self._process_normal_data(normal_data)
            if self.pending_normal_data is None:
                self.pending_normal_data = processed_normal
            else:
                self.pending_normal_data = self._merge_normal_data(self.pending_normal_data, processed_normal)

            # Write blob data directly to blob writer (handles its own rolling)
            if blob_data is not None and blob_data.num_rows > 0:
                # Write blob data directly to blob writer
                self.blob_writer.write(blob_data)

            self.record_count += data.num_rows

            # Check if normal data rolling is needed
            if self._should_roll_normal():
                # When normal data rolls, close both writers and fetch blob metadata
                self._close_current_writers()

        except Exception as e:
            logger.error("Exception occurs when writing data. Cleaning up.", exc_info=e)
            self.abort()
            raise e

    def prepare_commit(self) -> List[DataFileMeta]:
        # Close any remaining data
        self._close_current_writers()

        return self.committed_files.copy()

    def close(self):
        if self.closed:
            return

        try:
            if self.pending_normal_data is not None and self.pending_normal_data.num_rows > 0:
                self._close_current_writers()
        except Exception as e:
            logger.error("Exception occurs when closing writer. Cleaning up.", exc_info=e)
            self.abort()
        finally:
            self.closed = True
            self.pending_normal_data = None

    def abort(self):
        """Abort all writers and clean up resources."""
        self.blob_writer.abort()
        self.pending_normal_data = None
        self.committed_files.clear()

    def _split_data(self, data: pa.RecordBatch) -> Tuple[pa.RecordBatch, pa.RecordBatch]:
        """Split data into normal and blob parts based on column names."""
        # Use the pre-computed column names
        normal_columns = self.normal_column_names
        blob_columns = [self.blob_column_name]  # Single blob column

        # Create projected batches
        normal_data = data.select(normal_columns) if normal_columns else None
        blob_data = data.select(blob_columns) if blob_columns else None

        return normal_data, blob_data

    def _process_normal_data(self, data: pa.RecordBatch) -> pa.Table:
        """Process normal data (similar to base DataWriter)."""
        if data is None or data.num_rows == 0:
            return pa.Table.from_batches([])
        return pa.Table.from_batches([data])

    def _merge_normal_data(self, existing_data: pa.Table, new_data: pa.Table) -> pa.Table:
        return pa.concat_tables([existing_data, new_data])

    def _should_roll_normal(self) -> bool:
        if self.pending_normal_data is None:
            return False

        # Check rolling condition periodically (every CHECK_ROLLING_RECORD_CNT records)
        if self.record_count % self.CHECK_ROLLING_RECORD_CNT != 0:
            return False

        # Check if normal data exceeds target size
        current_size = self.pending_normal_data.nbytes
        return current_size > self.target_file_size

    def _close_current_writers(self):
        """Close both normal and blob writers and add blob metadata after normal metadata (Java behavior)."""
        if self.pending_normal_data is None or self.pending_normal_data.num_rows == 0:
            return

        # Close normal writer and get metadata
        normal_meta = self._write_normal_data_to_file(self.pending_normal_data)

        # Fetch blob metadata from blob writer
        blob_metas = self.blob_writer.prepare_commit()

        # Validate consistency between normal and blob files (Java behavior)
        self._validate_consistency(normal_meta, blob_metas)

        # Add normal file metadata first
        self.committed_files.append(normal_meta)

        # Add blob file metadata after normal metadata
        self.committed_files.extend(blob_metas)

        # Reset pending data
        self.pending_normal_data = None

        logger.info(f"Closed both writers - normal: {normal_meta.file_name}, "
                    f"added {len(blob_metas)} blob file metadata after normal metadata")

    def _write_normal_data_to_file(self, data: pa.Table) -> DataFileMeta:
        if data.num_rows == 0:
            return None

        file_name = f"{CoreOptions.data_file_prefix(self.options)}{uuid.uuid4()}-0.{self.file_format}"
        file_path = self._generate_file_path(file_name)

        # Write file based on format
        if self.file_format == CoreOptions.FILE_FORMAT_PARQUET:
            self.file_io.write_parquet(file_path, data, compression=self.compression)
        elif self.file_format == CoreOptions.FILE_FORMAT_ORC:
            self.file_io.write_orc(file_path, data, compression=self.compression)
        elif self.file_format == CoreOptions.FILE_FORMAT_AVRO:
            self.file_io.write_avro(file_path, data)
        elif self.file_format == CoreOptions.FILE_FORMAT_LANCE:
            self.file_io.write_lance(file_path, data)
        else:
            raise ValueError(f"Unsupported file format: {self.file_format}")

        # Determine if this is an external path
        is_external_path = self.external_path_provider is not None
        external_path_str = file_path if is_external_path else None

        return self._create_data_file_meta(file_name, file_path, data, external_path_str)

    def _create_data_file_meta(self, file_name: str, file_path: str, data: pa.Table,
                               external_path: Optional[str] = None) -> DataFileMeta:
        # Column stats (only for normal columns)
        column_stats = {
            field.name: self._get_column_stats(data, field.name)
            for field in self.table.table_schema.fields
            if field.name != self.blob_column_name
        }

        # Get normal fields only
        normal_fields = [field for field in self.table.table_schema.fields
                         if field.name != self.blob_column_name]

        min_value_stats = [column_stats[field.name]['min_values'] for field in normal_fields]
        max_value_stats = [column_stats[field.name]['max_values'] for field in normal_fields]
        value_null_counts = [column_stats[field.name]['null_counts'] for field in normal_fields]

        self.sequence_generator.start = self.sequence_generator.current

        return DataFileMeta.create(
            file_name=file_name,
            file_size=self.file_io.get_file_size(file_path),
            row_count=data.num_rows,
            min_key=GenericRow([], []),
            max_key=GenericRow([], []),
            key_stats=SimpleStats(
                GenericRow([], []),
                GenericRow([], []),
                []),
            value_stats=SimpleStats(
                GenericRow(min_value_stats, normal_fields),
                GenericRow(max_value_stats, normal_fields),
                value_null_counts),
            min_sequence_number=-1,
            max_sequence_number=-1,
            schema_id=self.table.table_schema.id,
            level=0,
            extra_files=[],
            creation_time=Timestamp.now(),
            delete_row_count=0,
            file_source=0,
            value_stats_cols=self.normal_column_names,
            external_path=external_path,
            file_path=file_path,
            write_cols=self.write_cols)

    def _validate_consistency(self, normal_meta: DataFileMeta, blob_metas: List[DataFileMeta]):
        if normal_meta is None:
            return

        normal_row_count = normal_meta.row_count
        blob_row_count = sum(meta.row_count for meta in blob_metas)

        if normal_row_count != blob_row_count:
            raise RuntimeError(
                f"This is a bug: The row count of main file and blob files does not match. "
                f"Main file: {normal_meta.file_name} (row count: {normal_row_count}), "
                f"blob files: {[meta.file_name for meta in blob_metas]} (total row count: {blob_row_count})"
            )
