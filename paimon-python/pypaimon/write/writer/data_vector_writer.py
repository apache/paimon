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
from typing import List, Optional, Tuple

import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.data.timestamp import Timestamp
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.schema.data_types import VectorType
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.writer.data_writer import DataWriter

logger = logging.getLogger(__name__)


class DataVectorWriter(DataWriter):
    """A rolling file writer that stores vector columns separately from normal columns.

    All vector columns are written to a single `.vector.<format>` file (matching
    Java behavior), while normal columns go to standard data files.

    Metadata organization:
    committed_files = [
        normal_file_meta,       # data.parquet (id, label columns)
        vector_file_meta,       # data.vector.lance (all vector columns)
        ...
    ]
    """

    CHECK_ROLLING_RECORD_CNT = 1000

    def __init__(self, table, partition: Tuple, bucket: int, max_seq_number: int,
                 options: CoreOptions = None, write_cols: Optional[List[str]] = None):
        super().__init__(table, partition, bucket, max_seq_number, options, write_cols=write_cols)

        self.vector_column_names = self._get_vector_columns_from_schema()
        self.vector_file_format = options.vector_file_format()

        all_column_names = self.table.field_names
        vector_set = set(self.vector_column_names)

        if write_cols is not None:
            write_col_set = set(write_cols)
            self.vector_write_columns = [
                col for col in self.vector_column_names if col in write_col_set
            ]
            self.normal_column_names = [
                col for col in write_cols if col not in vector_set
            ]
        else:
            self.vector_write_columns = list(self.vector_column_names)
            self.normal_column_names = [
                col for col in all_column_names if col not in vector_set
            ]

        normal_name_set = set(self.normal_column_names)
        self.normal_columns = [
            field for field in self.table.table_schema.fields if field.name in normal_name_set
        ]
        self.write_cols = self.normal_column_names

        self.record_count = 0
        self.closed = False
        self.pending_normal_data: Optional[pa.Table] = None

        from pypaimon.write.writer.vector_writer import VectorWriter
        self.vector_writer: Optional[VectorWriter] = None
        if self.vector_write_columns:
            self.vector_writer = VectorWriter(
                table=self.table,
                partition=self.partition,
                bucket=self.bucket,
                max_seq_number=max_seq_number,
                vector_columns=self.vector_write_columns,
                vector_file_format=self.vector_file_format,
                options=options,
            )

    def _get_vector_columns_from_schema(self) -> List[str]:
        return [
            field.name for field in self.table.table_schema.fields
            if isinstance(field.type, VectorType)
        ]

    def _process_data(self, data: pa.RecordBatch) -> pa.RecordBatch:
        normal_data, _ = self._split_data(data)
        return normal_data

    def _merge_data(self, existing_data: pa.Table, new_data: pa.Table) -> pa.Table:
        return pa.concat_tables([existing_data, new_data])

    def write(self, data: pa.RecordBatch):
        try:
            normal_data, vector_data = self._split_data(data)

            processed_normal = pa.Table.from_batches([normal_data]) if normal_data is not None else None
            if self.pending_normal_data is None:
                self.pending_normal_data = processed_normal
            elif processed_normal is not None:
                self.pending_normal_data = pa.concat_tables([self.pending_normal_data, processed_normal])

            if self.vector_writer is not None and vector_data is not None and vector_data.num_rows > 0:
                self.vector_writer.write(vector_data)

            self.record_count += data.num_rows

            if self._should_roll_normal():
                self._close_current_writers()

        except Exception as e:
            logger.error("Exception occurs when writing data. Cleaning up.", exc_info=e)
            self.abort()
            raise e

    def prepare_commit(self) -> List[DataFileMeta]:
        self._close_current_writers()
        return self.committed_files.copy()

    def close(self):
        if self.closed:
            return
        try:
            self._close_current_writers()
        except Exception as e:
            logger.error("Exception occurs when closing writer. Cleaning up.", exc_info=e)
            self.abort()
        finally:
            self.closed = True
            self.pending_normal_data = None

    def abort(self):
        if self.vector_writer is not None:
            self.vector_writer.abort()
        self.pending_normal_data = None
        self.committed_files.clear()

    def _split_data(self, data: pa.RecordBatch) -> Tuple[pa.RecordBatch, pa.RecordBatch]:
        normal_data = (
            pa.RecordBatch.from_arrays(
                [data.column(name) for name in self.normal_column_names],
                names=self.normal_column_names,
            )
            if self.normal_column_names else None
        )
        vector_data = (
            pa.RecordBatch.from_arrays(
                [data.column(name) for name in self.vector_write_columns],
                names=self.vector_write_columns,
            )
            if self.vector_write_columns else None
        )
        return normal_data, vector_data

    def _should_roll_normal(self) -> bool:
        if self.pending_normal_data is None:
            return False
        if self.record_count % self.CHECK_ROLLING_RECORD_CNT != 0:
            return False
        return self.pending_normal_data.nbytes > self.target_file_size

    def _close_current_writers(self):
        has_normal = self.pending_normal_data is not None and self.pending_normal_data.num_rows > 0

        normal_meta = None
        if has_normal:
            normal_meta = self._write_normal_data_to_file(self.pending_normal_data)
            self.committed_files.append(normal_meta)

        if self.vector_writer is not None:
            vector_metas = self.vector_writer.prepare_commit()
            self.vector_writer.committed_files.clear()
            if vector_metas:
                if normal_meta is not None:
                    self._validate_consistency(normal_meta, vector_metas)
                self.committed_files.extend(vector_metas)

        self.pending_normal_data = None

    def _write_normal_data_to_file(self, data: pa.Table) -> Optional[DataFileMeta]:
        if data.num_rows == 0:
            return None

        file_name = f"{CoreOptions.data_file_prefix(self.options)}{uuid.uuid4()}-0.{self.file_format}"
        file_path = self._generate_file_path(file_name)

        if self.file_format == CoreOptions.FILE_FORMAT_PARQUET:
            self.file_io.write_parquet(file_path, data, compression=self.compression, zstd_level=self.zstd_level)
        elif self.file_format == CoreOptions.FILE_FORMAT_ORC:
            self.file_io.write_orc(file_path, data, compression=self.compression, zstd_level=self.zstd_level)
        elif self.file_format == CoreOptions.FILE_FORMAT_AVRO:
            self.file_io.write_avro(file_path, data, compression=self.compression, zstd_level=self.zstd_level)
        elif self.file_format == CoreOptions.FILE_FORMAT_LANCE:
            self.file_io.write_lance(file_path, data)
        elif self.file_format == CoreOptions.FILE_FORMAT_VORTEX:
            self.file_io.write_vortex(file_path, data)
        elif self.file_format == CoreOptions.FILE_FORMAT_MOSAIC:
            self.file_io.write_mosaic(file_path, data)
        elif self.file_format == CoreOptions.FILE_FORMAT_ROW:
            self.file_io.write_row(file_path, data, zstd_level=self.zstd_level)
        else:
            raise ValueError(f"Unsupported file format: {self.file_format}")

        is_external_path = self.external_path_provider is not None
        external_path_str = file_path if is_external_path else None

        metadata_stats_enabled = self.options.metadata_stats_enabled()
        stats_columns = self.normal_columns if metadata_stats_enabled else []
        value_stats = self._collect_value_stats(data, stats_columns)

        return DataFileMeta.create(
            file_name=file_name,
            file_size=self.file_io.get_file_size(file_path),
            row_count=data.num_rows,
            min_key=GenericRow([], []),
            max_key=GenericRow([], []),
            key_stats=SimpleStats.empty_stats(),
            value_stats=value_stats,
            min_sequence_number=-1,
            max_sequence_number=-1,
            schema_id=self.table.table_schema.id,
            level=0,
            extra_files=[],
            creation_time=Timestamp.now(),
            delete_row_count=0,
            file_source=0,
            value_stats_cols=[column.name for column in stats_columns],
            external_path=external_path_str,
            file_path=file_path,
            write_cols=self.write_cols,
        )

    def _validate_consistency(
            self, normal_meta: DataFileMeta, vector_metas: List[DataFileMeta]):
        if normal_meta is None:
            return
        normal_row_count = normal_meta.row_count
        vector_row_count = sum(meta.row_count for meta in vector_metas)
        if normal_row_count != vector_row_count:
            raise RuntimeError(
                f"Row count mismatch between main file and vector files. "
                f"Main file: {normal_meta.file_name} (rows: {normal_row_count}), "
                f"vector files: {[m.file_name for m in vector_metas]} (rows: {vector_row_count})"
            )
