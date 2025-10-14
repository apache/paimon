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
import pyarrow as pa
import pyarrow.compute as pc
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pypaimon.common.core_options import CoreOptions
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.table.bucket_mode import BucketMode
from pypaimon.table.row.generic_row import GenericRow


class DataWriter(ABC):
    """Base class for data writers that handle PyArrow tables directly."""

    def __init__(self, table, partition: Tuple, bucket: int, max_seq_number: int,
                 write_cols: Optional[List[str]] = None):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.partition = partition
        self.bucket = bucket

        self.file_io = self.table.file_io
        self.trimmed_primary_key_fields = self.table.table_schema.get_trimmed_primary_key_fields()
        self.trimmed_primary_key = [field.name for field in self.trimmed_primary_key_fields]

        options = self.table.options
        self.target_file_size = 256 * 1024 * 1024
        self.file_format = options.get(CoreOptions.FILE_FORMAT,
                                       CoreOptions.FILE_FORMAT_PARQUET
                                       if self.bucket != BucketMode.POSTPONE_BUCKET.value
                                       else CoreOptions.FILE_FORMAT_AVRO)
        self.compression = options.get(CoreOptions.FILE_COMPRESSION, "zstd")
        self.sequence_generator = SequenceGenerator(max_seq_number)

        self.pending_data: Optional[pa.Table] = None
        self.committed_files: List[DataFileMeta] = []
        self.write_cols = write_cols

    def write(self, data: pa.RecordBatch):
        processed_data = self._process_data(data)

        if self.pending_data is None:
            self.pending_data = processed_data
        else:
            self.pending_data = self._merge_data(self.pending_data, processed_data)

        self._check_and_roll_if_needed()

    def prepare_commit(self) -> List[DataFileMeta]:
        if self.pending_data is not None and self.pending_data.num_rows > 0:
            self._write_data_to_file(self.pending_data)
            self.pending_data = None

        return self.committed_files.copy()

    def close(self):
        self.pending_data = None
        self.committed_files.clear()

    @abstractmethod
    def _process_data(self, data: pa.RecordBatch) -> pa.RecordBatch:
        """Process incoming data (e.g., add system fields, sort). Must be implemented by subclasses."""

    @abstractmethod
    def _merge_data(self, existing_data: pa.RecordBatch, new_data: pa.RecordBatch) -> pa.RecordBatch:
        """Merge existing data with new data. Must be implemented by subclasses."""

    def _check_and_roll_if_needed(self):
        if self.pending_data is None:
            return

        current_size = self.pending_data.nbytes
        if current_size > self.target_file_size:
            split_row = self._find_optimal_split_point(self.pending_data, self.target_file_size)
            if split_row > 0:
                data_to_write = self.pending_data.slice(0, split_row)
                remaining_data = self.pending_data.slice(split_row)

                self._write_data_to_file(data_to_write)
                self.pending_data = remaining_data
                self._check_and_roll_if_needed()

    def _write_data_to_file(self, data: pa.Table):
        if data.num_rows == 0:
            return
        file_name = f"data-{uuid.uuid4()}-0.{self.file_format}"
        file_path = self._generate_file_path(file_name)
        if self.file_format == CoreOptions.FILE_FORMAT_PARQUET:
            self.file_io.write_parquet(file_path, data, compression=self.compression)
        elif self.file_format == CoreOptions.FILE_FORMAT_ORC:
            self.file_io.write_orc(file_path, data, compression=self.compression)
        elif self.file_format == CoreOptions.FILE_FORMAT_AVRO:
            self.file_io.write_avro(file_path, data)
        else:
            raise ValueError(f"Unsupported file format: {self.file_format}")

        # min key & max key

        selected_table = data.select(self.trimmed_primary_key)
        key_columns_batch = selected_table.to_batches()[0]
        min_key_row_batch = key_columns_batch.slice(0, 1)
        max_key_row_batch = key_columns_batch.slice(key_columns_batch.num_rows - 1, 1)
        min_key = [col.to_pylist()[0] for col in min_key_row_batch.columns]
        max_key = [col.to_pylist()[0] for col in max_key_row_batch.columns]

        # key stats & value stats
        data_fields = self.table.fields if self.table.is_primary_key_table \
            else PyarrowFieldParser.to_paimon_schema(data.schema)
        column_stats = {
            field.name: self._get_column_stats(data, field.name)
            for field in data_fields
        }
        all_fields = data_fields
        min_value_stats = [column_stats[field.name]['min_values'] for field in all_fields]
        max_value_stats = [column_stats[field.name]['max_values'] for field in all_fields]
        value_null_counts = [column_stats[field.name]['null_counts'] for field in all_fields]
        key_fields = self.trimmed_primary_key_fields
        min_key_stats = [column_stats[field.name]['min_values'] for field in key_fields]
        max_key_stats = [column_stats[field.name]['max_values'] for field in key_fields]
        key_null_counts = [column_stats[field.name]['null_counts'] for field in key_fields]
        if not all(count == 0 for count in key_null_counts):
            raise RuntimeError("Primary key should not be null")

        min_seq = self.sequence_generator.start
        max_seq = self.sequence_generator.current
        self.sequence_generator.start = self.sequence_generator.current
        self.committed_files.append(DataFileMeta(
            file_name=file_name,
            file_size=self.file_io.get_file_size(file_path),
            row_count=data.num_rows,
            min_key=GenericRow(min_key, self.trimmed_primary_key_fields),
            max_key=GenericRow(max_key, self.trimmed_primary_key_fields),
            key_stats=SimpleStats(
                GenericRow(min_key_stats, self.trimmed_primary_key_fields),
                GenericRow(max_key_stats, self.trimmed_primary_key_fields),
                key_null_counts,
            ),
            value_stats=SimpleStats(
                GenericRow(min_value_stats, data_fields),
                GenericRow(max_value_stats, data_fields),
                value_null_counts,
            ),
            min_sequence_number=min_seq,
            max_sequence_number=max_seq,
            schema_id=self.table.table_schema.id,
            level=0,
            extra_files=[],
            creation_time=datetime.now(),
            delete_row_count=0,
            file_source="APPEND",
            value_stats_cols=None,  # None means all columns in the data have statistics
            external_path=None,
            first_row_id=None,
            write_cols=self.write_cols,
            # None means all columns in the table have been written
            file_path=str(file_path),
        ))

    def _generate_file_path(self, file_name: str) -> Path:
        path_builder = self.table.table_path

        for i, field_name in enumerate(self.table.partition_keys):
            path_builder = path_builder / (field_name + "=" + str(self.partition[i]))
        if self.bucket == BucketMode.POSTPONE_BUCKET.value:
            bucket_name = "postpone"
        else:
            bucket_name = str(self.bucket)
        path_builder = path_builder / ("bucket-" + bucket_name) / file_name

        return path_builder

    @staticmethod
    def _find_optimal_split_point(data: pa.RecordBatch, target_size: int) -> int:
        total_rows = data.num_rows
        if total_rows <= 1:
            return 0

        left, right = 1, total_rows
        best_split = 0

        while left <= right:
            mid = (left + right) // 2
            slice_data = data.slice(0, mid)
            slice_size = slice_data.nbytes

            if slice_size <= target_size:
                best_split = mid
                left = mid + 1
            else:
                right = mid - 1

        return best_split

    @staticmethod
    def _get_column_stats(record_batch: pa.RecordBatch, column_name: str) -> Dict:
        column_array = record_batch.column(column_name)
        if column_array.null_count == len(column_array):
            return {
                "min_values": None,
                "max_values": None,
                "null_counts": column_array.null_count,
            }
        min_values = pc.min(column_array).as_py()
        max_values = pc.max(column_array).as_py()
        null_counts = column_array.null_count
        return {
            "min_values": min_values,
            "max_values": max_values,
            "null_counts": null_counts,
        }


class SequenceGenerator:
    def __init__(self, start: int = 0):
        self.start = start
        self.current = start

    def next(self) -> int:
        self.current += 1
        return self.current
