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
from typing import Dict, List, Optional, Tuple

import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.data.timestamp import Timestamp
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.schema.data_types import VectorType
from pypaimon.table.row.blob import BlobConsumer
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.writer.data_writer import DataWriter

logger = logging.getLogger(__name__)


class DedicatedFormatWriter(DataWriter):
    """A rolling file writer that writes normal, blob, and vector columns to dedicated files.

    Splits incoming data three ways:
    - Normal columns → standard data files (.parquet / .orc / .vortex / …)
    - Blob columns (large_binary) → .blob files
    - Vector columns (when vector.file.format is configured) → .vector.<format> files

    This mirrors Java's DedicatedFormatRollingFileWriter.

    Metadata order in committed_files:
        [normal_meta, blob_meta1, …, vector_meta1, …]
    """

    # Constant for checking rolling condition periodically
    CHECK_ROLLING_RECORD_CNT = 1000

    def __init__(self, table, partition: Tuple, bucket: int, max_seq_number: int, options: CoreOptions = None,
                 write_cols: Optional[List[str]] = None, blob_consumer: Optional[BlobConsumer] = None):
        super().__init__(table, partition, bucket, max_seq_number, options, write_cols=write_cols)

        # Determine blob columns from table schema
        self.blob_column_names = self._get_blob_columns_from_schema()
        self.blob_descriptor_fields = CoreOptions.blob_descriptor_fields(self.options)
        self.blob_view_fields = CoreOptions.blob_view_fields(self.options)
        self.blob_inline_fields = self.blob_descriptor_fields.union(self.blob_view_fields)

        unknown_descriptor_fields = self.blob_descriptor_fields.difference(
            set(self.blob_column_names)
        )
        if unknown_descriptor_fields:
            raise ValueError(
                "Fields in 'blob-descriptor-field' must be blob fields in schema. "
                f"Unknown fields: {sorted(unknown_descriptor_fields)}"
            )

        # Blob fields that should still be written to `.blob` files.
        self.blob_file_column_names = [
            col for col in self.blob_column_names if col not in self.blob_inline_fields
        ]
        full_blob_file_set = set(self.blob_file_column_names)
        all_column_names = self.table.field_names

        # Detect vector columns that should be written to dedicated files.
        full_vector_column_names = self._get_vector_columns_from_schema()
        full_vector_set = set(full_vector_column_names)
        # Only split vector columns when vector.file.format is configured.
        has_dedicated_vector = bool(full_vector_column_names) and options.with_vector_format()
        dedicated_set = full_blob_file_set | (full_vector_set if has_dedicated_vector else set())

        # Narrow columns when TableWrite.with_write_type(...) supplies a partial column list.
        # Incoming RecordBatches only contain those columns; selecting full normal/blob lists
        # would raise KeyError.
        if write_cols is not None:
            write_col_set = set(write_cols)
            self.blob_file_column_names = [
                col for col in self.blob_file_column_names if col in write_col_set
            ]
            self.vector_write_columns = [
                col for col in full_vector_column_names if col in write_col_set
            ] if has_dedicated_vector else []
            self.normal_column_names = [
                col for col in write_cols if col not in dedicated_set
            ]
        else:
            self.vector_write_columns = list(full_vector_column_names) if has_dedicated_vector else []
            self.normal_column_names = [
                col for col in all_column_names if col not in dedicated_set
            ]
        normal_name_set = set(self.normal_column_names)
        self.normal_columns = [
            field for field in self.table.table_schema.fields if field.name in normal_name_set
        ]
        self.write_cols = self.normal_column_names

        # State management for blob writer
        self.record_count = 0
        self.closed = False

        # Track pending data for normal data only
        self.pending_normal_data: Optional[pa.Table] = None

        # Initialize blob writers for each blob-file column.
        from pypaimon.write.writer.blob_writer import BlobWriter
        self.blob_writers: Dict[str, BlobWriter] = {}
        for blob_column in self.blob_file_column_names:
            self.blob_writers[blob_column] = BlobWriter(
                table=self.table,
                partition=self.partition,
                bucket=self.bucket,
                max_seq_number=max_seq_number,
                blob_column=blob_column,
                options=options,
                blob_consumer=blob_consumer,
            )

        # Initialize vector writer when vector.file.format is configured.
        from pypaimon.write.writer.vector_writer import VectorWriter
        self.vector_writer: Optional[VectorWriter] = None
        if self.vector_write_columns:
            self.vector_writer = VectorWriter(
                table=self.table,
                partition=self.partition,
                bucket=self.bucket,
                max_seq_number=max_seq_number,
                vector_columns=self.vector_write_columns,
                vector_file_format=options.vector_file_format(),
                options=options,
            )

        # Initialize ExternalStorageBlobWriter if configured
        self._external_storage_writer = None
        external_storage_fields = self.options.blob_external_storage_fields()
        external_storage_path = self.options.blob_external_storage_path()
        if external_storage_fields and external_storage_path:
            from pypaimon.write.writer.external_storage_blob_writer import \
                ExternalStorageBlobWriter
            self._external_storage_writer = ExternalStorageBlobWriter(
                file_io=self.file_io,
                external_storage_path=external_storage_path,
                external_storage_fields=external_storage_fields,
                blob_target_file_size=self.options.blob_target_file_size(),
                data_file_prefix=CoreOptions.data_file_prefix(self.options),
            )

        logger.info(
            "Initialized DedicatedFormatWriter with blob columns: %s, blob file columns: %s, "
            "vector columns: %s, descriptor stored columns: %s, external storage fields: %s, view stored columns: %s",
            self.blob_column_names,
            self.blob_file_column_names,
            self.vector_write_columns,
            sorted(self.blob_descriptor_fields),
            sorted(external_storage_fields) if external_storage_fields else [],
            sorted(self.blob_view_fields)
        )

    def _get_blob_columns_from_schema(self) -> List[str]:
        blob_columns = []
        for field in self.table.table_schema.fields:
            type_str = str(field.type).lower()
            if 'blob' in type_str:
                blob_columns.append(field.name)

        if len(blob_columns) == 0:
            raise ValueError("No blob field found in table schema.")
        return blob_columns

    def _get_vector_columns_from_schema(self) -> List[str]:
        return [
            field.name for field in self.table.table_schema.fields
            if isinstance(field.type, VectorType)
        ]

    def _process_data(self, data: pa.RecordBatch) -> pa.RecordBatch:
        normal_data, _, _ = self._split_data(data)
        return normal_data

    def _merge_data(self, existing_data: pa.Table, new_data: pa.Table) -> pa.Table:
        return self._merge_normal_data(existing_data, new_data)

    def write(self, data: pa.RecordBatch):
        try:
            # Transform external-storage fields: write raw blob to external storage,
            # replace with serialized BlobDescriptor
            if self._external_storage_writer:
                data = self._external_storage_writer.transform_batch(data)

            # Split data into normal, blob, and vector parts
            normal_data, blob_data_map, vector_data = self._split_data(data)
            self._validate_inline_stored_fields_input(data)

            # Process and accumulate normal data (may be None for partial writes)
            processed_normal = self._process_normal_data(normal_data)
            if processed_normal is not None:
                if self.pending_normal_data is None:
                    self.pending_normal_data = processed_normal
                else:
                    self.pending_normal_data = self._merge_normal_data(self.pending_normal_data, processed_normal)

            # Write blob-file columns to dedicated blob writers.
            for blob_column, blob_data in blob_data_map.items():
                if blob_data is not None and blob_data.num_rows > 0:
                    self.blob_writers[blob_column].write(blob_data)

            # Write vector columns to dedicated vector writer.
            if self.vector_writer is not None and vector_data is not None and vector_data.num_rows > 0:
                self.vector_writer.write(vector_data)

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
            self._close_current_writers()
            if self._external_storage_writer:
                self._external_storage_writer.close()
        except Exception as e:
            logger.error("Exception occurs when closing writer. Cleaning up.", exc_info=e)
            self.abort()
        finally:
            self.closed = True
            self.pending_normal_data = None

    def abort(self):
        """Abort all writers and clean up resources."""
        for blob_writer in self.blob_writers.values():
            blob_writer.abort()
        if self.vector_writer is not None:
            self.vector_writer.abort()
        if self._external_storage_writer:
            self._external_storage_writer.abort()
        self.pending_normal_data = None
        self.committed_files.clear()

    def _split_data(self, data: pa.RecordBatch) -> Tuple[
            pa.RecordBatch, Dict[str, pa.RecordBatch], Optional[pa.RecordBatch]]:
        """Split data into normal, blob, and vector parts based on column names."""
        normal_data = data.select(self.normal_column_names) if self.normal_column_names else None
        blob_data_map = {
            blob_column: data.select([blob_column]) for blob_column in self.blob_file_column_names
        }
        vector_data = (
            pa.RecordBatch.from_arrays(
                [data.column(name) for name in self.vector_write_columns],
                names=self.vector_write_columns,
            ) if self.vector_write_columns else None
        )
        return normal_data, blob_data_map, vector_data

    def _validate_inline_stored_fields_input(self, data: pa.RecordBatch):
        if not self.blob_inline_fields:
            return

        from pypaimon.table.row.blob import BlobDescriptor, BlobViewStruct

        for field_name in self.blob_descriptor_fields:
            if field_name not in data.schema.names:
                continue
            values = data.column(data.schema.get_field_index(field_name)).to_pylist()
            for value in values:
                if value is None:
                    continue
                if hasattr(value, 'as_py'):
                    value = value.as_py()
                if isinstance(value, str):
                    value = value.encode('utf-8')
                if not isinstance(value, (bytes, bytearray)):
                    raise ValueError(
                        "blob-descriptor-field requires blob field value to be a serialized "
                        "BlobDescriptor."
                    )
                try:
                    descriptor_bytes = bytes(value)
                    descriptor = BlobDescriptor.deserialize(descriptor_bytes)
                    if descriptor.serialize() != descriptor_bytes:
                        raise ValueError("Descriptor payload contains trailing bytes.")
                except Exception as e:
                    raise ValueError(
                        "blob-descriptor-field requires blob field value to be a serialized "
                        "BlobDescriptor."
                    ) from e

        for field_name in self.blob_view_fields:
            if field_name not in data.schema.names:
                continue
            values = data.column(data.schema.get_field_index(field_name)).to_pylist()
            for value in values:
                if value is None:
                    continue
                if hasattr(value, 'as_py'):
                    value = value.as_py()
                if isinstance(value, str):
                    value = value.encode('utf-8')
                if not isinstance(value, (bytes, bytearray)):
                    raise ValueError(
                        "blob-view-field requires blob field value to be a serialized "
                        "BlobViewStruct."
                    )
                try:
                    view_bytes = bytes(value)
                    view_struct = BlobViewStruct.deserialize(view_bytes)
                    if view_struct.serialize() != view_bytes:
                        raise ValueError("BlobViewStruct payload contains trailing bytes.")
                except Exception as e:
                    raise ValueError(
                        "blob-view-field requires blob field value to be a serialized "
                        "BlobViewStruct."
                    ) from e

    @staticmethod
    def _process_normal_data(data: pa.RecordBatch) -> Optional[pa.Table]:
        """Process normal data (similar to base DataWriter)."""
        if data is None or data.num_rows == 0:
            return None
        return pa.Table.from_batches([data])

    @staticmethod
    def _merge_normal_data(existing_data: pa.Table, new_data: pa.Table) -> pa.Table:
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
        """Close normal, blob, and vector writers; add metadata in order: normal, blob, vector."""
        normal_meta = None
        if self.pending_normal_data is not None and self.pending_normal_data.num_rows > 0:
            normal_meta = self._write_normal_data_to_file(self.pending_normal_data)

        blob_metas = []
        for blob_column in self.blob_file_column_names:
            writer_metas = self.blob_writers[blob_column].prepare_commit()
            if normal_meta is not None:
                self._validate_consistency(normal_meta, writer_metas, blob_column)
            blob_metas.extend(writer_metas)

        vector_metas = []
        if self.vector_writer is not None:
            vector_metas = self.vector_writer.prepare_commit()
            self.vector_writer.committed_files.clear()
            if vector_metas and normal_meta is not None:
                self._validate_consistency(normal_meta, vector_metas, 'vector')

        if normal_meta is not None:
            self.committed_files.append(normal_meta)
        self.committed_files.extend(blob_metas)
        self.committed_files.extend(vector_metas)

        self.pending_normal_data = None

        if normal_meta is not None or blob_metas or vector_metas:
            normal_name = normal_meta.file_name if normal_meta is not None else '<none>'
            logger.info(f"Closed writers - normal: {normal_name}, "
                        f"{len(blob_metas)} blob metas, {len(vector_metas)} vector metas")

    def _write_normal_data_to_file(self, data: pa.Table) -> Optional[DataFileMeta]:
        if data.num_rows == 0:
            return None

        file_name = f"{CoreOptions.data_file_prefix(self.options)}{uuid.uuid4()}-0.{self.file_format}"
        file_path = self._generate_file_path(file_name)

        # Write file based on format
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

        # Determine if this is an external path
        is_external_path = self.external_path_provider is not None
        external_path_str = file_path if is_external_path else None

        return self._create_data_file_meta(file_name, file_path, data, external_path_str)

    def _create_data_file_meta(self, file_name: str, file_path: str, data: pa.Table,
                               external_path: Optional[str] = None) -> DataFileMeta:
        # Column stats (only for normal columns)
        metadata_stats_enabled = self.options.metadata_stats_enabled()
        stats_columns = self.normal_columns if metadata_stats_enabled else []
        value_stats = self._collect_value_stats(data, stats_columns)

        self.sequence_generator.start = self.sequence_generator.current

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
            external_path=external_path,
            file_path=file_path,
            write_cols=self.write_cols)

    def _validate_consistency(
            self, normal_meta: DataFileMeta, blob_metas: List[DataFileMeta], blob_column: str):
        if normal_meta is None:
            return

        normal_row_count = normal_meta.row_count
        blob_row_count = sum(meta.row_count for meta in blob_metas)

        if normal_row_count != blob_row_count:
            raise RuntimeError(
                f"This is a bug: The row count of main file and blob files does not match. "
                f"Main file: {normal_meta.file_name} (row count: {normal_row_count}), "
                f"blob field: {blob_column}, "
                f"blob files: {[meta.file_name for meta in blob_metas]} (total row count: {blob_row_count})"
            )
