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

import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional, Tuple

import pyarrow as pa

from pypaimon.common.core_options import CoreOptions
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.table.row.binary_row import BinaryRow


class DataWriter(ABC):
    """Base class for data writers that handle PyArrow tables directly."""

    def __init__(self, table, partition: Tuple, bucket: int):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.partition = partition
        self.bucket = bucket

        self.file_io = self.table.file_io
        self.trimmed_primary_key_fields = self.table.table_schema.get_trimmed_primary_key_fields()
        self.trimmed_primary_key = [field.name for field in self.trimmed_primary_key_fields]

        options = self.table.options
        self.target_file_size = 256 * 1024 * 1024
        self.file_format = options.get(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_PARQUET)
        self.compression = options.get(CoreOptions.FILE_COMPRESSION, "zstd")

        self.pending_data: Optional[pa.RecordBatch] = None
        self.committed_files: List[DataFileMeta] = []

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

        current_size = self.pending_data.get_total_buffer_size()
        if current_size > self.target_file_size:
            split_row = _find_optimal_split_point(self.pending_data, self.target_file_size)
            if split_row > 0:
                data_to_write = self.pending_data.slice(0, split_row)
                remaining_data = self.pending_data.slice(split_row)

                self._write_data_to_file(data_to_write)
                self.pending_data = remaining_data
                self._check_and_roll_if_needed()

    def _write_data_to_file(self, data: pa.RecordBatch):
        if data.num_rows == 0:
            return
        file_name = f"data-{uuid.uuid4()}.{self.file_format}"
        file_path = self._generate_file_path(file_name)
        if self.file_format == CoreOptions.FILE_FORMAT_PARQUET:
            self.file_io.write_parquet(file_path, data, compression=self.compression)
        elif self.file_format == CoreOptions.FILE_FORMAT_ORC:
            self.file_io.write_orc(file_path, data, compression=self.compression)
        elif self.file_format == CoreOptions.FILE_FORMAT_AVRO:
            self.file_io.write_avro(file_path, data)
        else:
            raise ValueError(f"Unsupported file format: {self.file_format}")

        key_columns_batch = data.select(self.trimmed_primary_key)
        min_key_row_batch = key_columns_batch.slice(0, 1)
        min_key_data = [col.to_pylist()[0] for col in min_key_row_batch.columns]
        max_key_row_batch = key_columns_batch.slice(key_columns_batch.num_rows - 1, 1)
        max_key_data = [col.to_pylist()[0] for col in max_key_row_batch.columns]
        self.committed_files.append(DataFileMeta(
            file_name=file_name,
            file_size=self.file_io.get_file_size(file_path),
            row_count=data.num_rows,
            min_key=BinaryRow(min_key_data, self.trimmed_primary_key_fields),
            max_key=BinaryRow(max_key_data, self.trimmed_primary_key_fields),
            key_stats=None,  # TODO
            value_stats=None,
            min_sequence_number=0,
            max_sequence_number=0,
            schema_id=0,
            level=0,
            extra_files=None,
            file_path=str(file_path),
        ))

    def _generate_file_path(self, file_name: str) -> Path:
        path_builder = self.table.table_path

        for i, field_name in enumerate(self.table.partition_keys):
            path_builder = path_builder / (field_name + "=" + str(self.partition[i]))
        path_builder = path_builder / ("bucket-" + str(self.bucket)) / file_name

        return path_builder


def _find_optimal_split_point(data: pa.RecordBatch, target_size: int) -> int:
    total_rows = data.num_rows
    if total_rows <= 1:
        return 0

    left, right = 1, total_rows
    best_split = 0

    while left <= right:
        mid = (left + right) // 2
        slice_data = data.slice(0, mid)
        slice_size = slice_data.get_total_buffer_size()

        if slice_size <= target_size:
            best_split = mid
            left = mid + 1
        else:
            right = mid - 1

    return best_split
