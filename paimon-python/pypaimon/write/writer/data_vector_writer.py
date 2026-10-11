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
from typing import List, Optional, Tuple

import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.schema.data_types import VectorType
from pypaimon.write.writer.composite_data_writer import CompositeDataWriter

logger = logging.getLogger(__name__)


class DataVectorWriter(CompositeDataWriter):
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
        all_normal_column_names = [
            col for col in all_column_names if col not in vector_set
        ]
        self.write_cols = (
            None
            if options.data_evolution_enabled(False)
            and options.data_evolution_write_cols_optimization_enabled(False)
            and self.normal_column_names == all_normal_column_names
            else self.normal_column_names
        )

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
        self._require_finished_flush()
        try:
            offset = 0
            # _write_batch keeps normal and vector pending rows in lockstep
            # and closes both writers when the shared row limit is reached.
            while offset < data.num_rows:
                capacity = self.target_file_row_num - self.pending_row_count
                if capacity <= 0:
                    self._close_current_writers()
                    capacity = self.target_file_row_num
                length = min(capacity, data.num_rows - offset)
                self._write_batch(data.slice(offset, length))
                offset += length

        except Exception as e:
            logger.error("Exception occurs when writing data. Cleaning up.", exc_info=e)
            self.abort()
            raise e

    def _write_batch(self, data: pa.RecordBatch):
        if data.num_rows == 0:
            return

        normal_data, vector_data = self._split_data(data)

        if normal_data is not None:
            self._normal_buffer.append(pa.Table.from_batches([normal_data]))

        if self.vector_writer is not None and vector_data is not None and vector_data.num_rows > 0:
            self.vector_writer.write(vector_data)

        self.record_count += data.num_rows

        if self._should_roll_normal():
            self._close_current_writers()

    def abort(self):
        if self.vector_writer is not None:
            self.vector_writer.abort()
        super().abort()

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

    @property
    def pending_row_count(self) -> int:
        # Overrides the base property, which reads a buffer this writer never
        # fills. Normal and vector rows are kept in lockstep, so either half
        # answers for the pair; the vector writer is asked only when the table
        # has no normal columns at all.
        if not self._normal_buffer.is_empty:
            return self._normal_buffer.num_rows
        if self.vector_writer is not None:
            # Running count, not a folded buffer: this runs on every write.
            return self.vector_writer.pending_row_count
        return 0

    def _prepare_sidecar_commits(self, normal_meta):
        if self.vector_writer is None:
            return []
        vector_metas = self.vector_writer.prepare_commit()
        if vector_metas and normal_meta is not None:
            self._validate_consistency(normal_meta, vector_metas)
        return [(self.vector_writer, vector_metas)]

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
