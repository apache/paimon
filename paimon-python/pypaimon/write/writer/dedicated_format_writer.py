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
from typing import Dict, List, Optional, Tuple

import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions, ChangelogProducer
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.schema.data_types import (
    PyarrowFieldParser,
    VectorType,
    is_blob_file_field,
    is_blob_type,
)
from pypaimon.table.row.blob import (
    Blob,
    BlobConsumer,
    video_payload_descriptor,
)
from pypaimon.write.row_utils import (
    require_columns,
    row_to_named_values,
    row_values_to_arrow_table,
)
from pypaimon.write.writer.composite_data_writer import CompositeDataWriter
from pypaimon.write.writer.video_group import VideoGroupRollingPolicy

logger = logging.getLogger(__name__)


class DedicatedFormatWriter(CompositeDataWriter):
    """A rolling file writer that writes normal, blob, and vector columns to dedicated files.

    Splits incoming data three ways:
    - Normal columns → standard data files (.parquet / .orc / .vortex / …)
    - Blob columns (large_binary) → .blob or .video files
    - Vector columns (when vector.file.format is configured) → .vector.<format> files

    This mirrors Java's DedicatedFormatRollingFileWriter.

    Metadata order in committed_files:
        [normal_meta, blob_meta1, …, vector_meta1, …]
    """

    def __init__(self, table, partition: Tuple, bucket: int, max_seq_number: int, options: CoreOptions = None,
                 write_cols: Optional[List[str]] = None, blob_consumer: Optional[BlobConsumer] = None,
                 changelog_producer: ChangelogProducer = ChangelogProducer.NONE,
                 blob_uri_reader_factory=None):
        super().__init__(table, partition, bucket, max_seq_number, options, write_cols=write_cols,
                         changelog_producer=changelog_producer)

        # Determine blob columns from table schema
        self.blob_column_names = self._get_blob_columns_from_schema()
        self.blob_descriptor_fields = CoreOptions.blob_descriptor_fields(self.options)
        self.blob_view_fields = CoreOptions.blob_view_fields(self.options)
        configured_video_fields = CoreOptions.video_frame_fields(self.options)
        self.blob_inline_fields = self.blob_descriptor_fields.union(self.blob_view_fields)

        unknown_descriptor_fields = self.blob_descriptor_fields.difference(
            set(self.blob_column_names)
        )
        if unknown_descriptor_fields:
            raise ValueError(
                "Fields in 'blob-descriptor-field' must be blob fields in schema. "
                f"Unknown fields: {sorted(unknown_descriptor_fields)}"
            )
        inline_nested_blob_fields = [
            field.name
            for field in self.table.table_schema.fields
            if field.name in self.blob_inline_fields and not is_blob_type(field.type)
        ]
        if inline_nested_blob_fields:
            raise ValueError(
                "ARRAY<BLOB> and MAP<X, BLOB> are only supported by 'blob-field'. "
                f"Invalid inline blob fields: {sorted(inline_nested_blob_fields)}"
            )

        # Blob fields that should still be written to dedicated BLOB files.
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
        self.video_frame_columns = [
            column for column in self.blob_file_column_names
            if column in configured_video_fields
        ]
        normal_name_set = set(self.normal_column_names)
        self.normal_columns = [
            field for field in self.table.table_schema.fields if field.name in normal_name_set
        ]
        all_normal_column_names = [
            col for col in all_column_names if col not in dedicated_set
        ]
        self.write_cols = (
            None
            if options.data_evolution_enabled(False)
            and options.data_evolution_write_cols_optimization_enabled(False)
            and self.normal_column_names == all_normal_column_names
            else self.normal_column_names
        )

        self._video_group_policy = (
            VideoGroupRollingPolicy()
            if self.video_frame_columns
            else None
        )

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
                video=blob_column in configured_video_fields,
                uri_reader_factory=blob_uri_reader_factory,
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
                rolling_managed_by_parent=bool(self.video_frame_columns),
            )

        logger.info(
            "Initialized DedicatedFormatWriter with blob columns: %s, blob file columns: %s, "
            "vector columns: %s, descriptor stored columns: %s, view stored columns: %s",
            self.blob_column_names,
            self.blob_file_column_names,
            self.vector_write_columns,
            sorted(self.blob_descriptor_fields),
            sorted(self.blob_view_fields)
        )

    def _get_blob_columns_from_schema(self) -> List[str]:
        blob_columns = [
            field.name
            for field in self.table.table_schema.fields
            if is_blob_file_field(field)
        ]
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
        # Outside the try on purpose: rejecting the write must not abort the
        # writer, or the unfinished flush would lose its chance to be retried.
        self._require_finished_flush()
        try:
            if self.video_frame_columns:
                self._write_video_batches(data)
                return

            self._write_bounded_batches(data)

        except Exception as e:
            logger.error("Exception occurs when writing data. Cleaning up.", exc_info=e)
            self.abort()
            raise e

    def _write_batch(self, data: pa.RecordBatch):
        if data.num_rows == 0:
            return

        # Split data into normal, blob, and vector parts
        normal_data, blob_data_map, vector_data = self._split_data(data)
        self._validate_inline_stored_fields_input(data)

        # Process and accumulate normal data (may be None for partial writes)
        processed_normal = self._process_normal_data(normal_data)
        if processed_normal is not None:
            self._normal_buffer.append(processed_normal)

        # Write blob-file columns to dedicated blob writers.
        for blob_column, blob_data in blob_data_map.items():
            if blob_data is not None and blob_data.num_rows > 0:
                self.blob_writers[blob_column].write(blob_data)

        # Write vector columns to dedicated vector writer.
        if self.vector_writer is not None and vector_data is not None and vector_data.num_rows > 0:
            self.vector_writer.write(vector_data)

        self.record_count += data.num_rows

        # Defer any active video-group roll to its Episode boundary.
        if self._should_roll_active_group():
            self._roll_or_defer_for_video_group()

    def write_row(self, row):
        self._require_finished_flush()
        try:
            values_by_name = row_to_named_values(
                row, self.table.table_schema.fields)
            required_columns = (
                list(self.normal_column_names)
                + list(self.blob_file_column_names)
                + list(self.vector_write_columns)
            )
            require_columns(values_by_name, required_columns, "write_row")

            if self.video_frame_columns:
                next_group = self._video_group(
                    values_by_name[column]
                    for column in self.video_frame_columns
                )
                self._roll_before_video_group(next_group)
                self._video_group_policy.record(next_group)

            if self.normal_column_names:
                normal_values = dict(values_by_name)
                for field_name in self.normal_column_names:
                    normal_values[field_name] = (
                        self._normal_row_value(field_name, normal_values[field_name])
                    )
                normal_data = row_values_to_arrow_table(
                    normal_values,
                    self.table.table_schema.fields,
                    self.normal_column_names,
                ).to_batches()[0]
                self._validate_inline_stored_fields_input(normal_data)
                processed_normal = self._process_normal_data(normal_data)
                if processed_normal is not None:
                    self._normal_buffer.append(processed_normal)

            for blob_column in self.blob_file_column_names:
                arrow_type = PyarrowFieldParser.from_paimon_type(
                    self.table.field_dict[blob_column].type)
                self.blob_writers[blob_column].write_blob(
                    values_by_name[blob_column], arrow_type)

            if self.vector_writer is not None and self.vector_write_columns:
                vector_data = row_values_to_arrow_table(
                    values_by_name,
                    self.table.table_schema.fields,
                    self.vector_write_columns,
                ).to_batches()[0]
                self.vector_writer.write(vector_data)

            self.record_count += 1
            if self._should_roll_active_group():
                self._roll_or_defer_for_video_group()

        except Exception as e:
            logger.error("Exception occurs when writing row. Cleaning up.", exc_info=e)
            self.abort()
            raise e

    def _normal_row_value(self, field_name: str, value):
        if field_name in self.blob_descriptor_fields and value is not None:
            if isinstance(value, Blob):
                try:
                    return value.to_descriptor().serialize()
                except Exception as e:
                    raise ValueError(
                        "blob-descriptor-field row values must be serialized "
                        "BlobDescriptor bytes or a Blob with a descriptor."
                    ) from e
            return value

        if field_name in self.blob_view_fields and value is not None:
            from pypaimon.table.row.blob import BlobView

            if isinstance(value, BlobView):
                return value.view_struct.serialize()
            return value

        return value

    def abort(self):
        """Abort all writers and clean up resources."""
        for blob_writer in self.blob_writers.values():
            blob_writer.abort()
        if self.vector_writer is not None:
            self.vector_writer.abort()
        super().abort()

    def _split_data(self, data: pa.RecordBatch) -> Tuple[
            Optional[pa.RecordBatch], Dict[str, pa.RecordBatch], Optional[pa.RecordBatch]]:
        """Split data into normal, blob, and vector parts based on column names."""
        normal_data = (
            self._project_columns(data, self.normal_column_names)
            if self.normal_column_names else None
        )
        blob_data_map = {
            blob_column: self._project_columns(data, [blob_column])
            for blob_column in self.blob_file_column_names
        }
        vector_data = (
            self._project_columns(data, self.vector_write_columns)
            if self.vector_write_columns else None
        )
        return normal_data, blob_data_map, vector_data

    @staticmethod
    def _project_columns(data: pa.RecordBatch, column_names: List[str]) -> pa.RecordBatch:
        indices = [data.schema.get_field_index(name) for name in column_names]
        missing_columns = [
            name for name, index in zip(column_names, indices) if index < 0
        ]
        if missing_columns:
            raise KeyError(f"Columns not found in record batch: {missing_columns}")

        projected_schema = pa.schema(
            [data.schema.field(index) for index in indices],
            metadata=data.schema.metadata,
        )
        return pa.RecordBatch.from_arrays(
            [data.column(index) for index in indices],
            schema=projected_schema,
        )

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
                descriptor_bytes = bytes(value)
                if descriptor_bytes:
                    version = descriptor_bytes[0]
                    if version < 1 or version > BlobDescriptor.CURRENT_VERSION:
                        raise ValueError(
                            f"blob-descriptor-field requires BlobDescriptor version "
                            f"in [1, {BlobDescriptor.CURRENT_VERSION}], but found "
                            f"{version}."
                        )
                try:
                    BlobDescriptor.deserialize(descriptor_bytes)
                except Exception as e:
                    raise ValueError(
                        "blob-descriptor-field requires blob field value to be a serialized "
                        "BlobDescriptor."
                    ) from e
                # serialize() always emits CURRENT_VERSION, so a round-trip
                # would reject exact v1 bytes. Check exact wire length instead.
                if BlobDescriptor.parse_if_serialized(descriptor_bytes) is None:
                    raise ValueError("Descriptor payload contains trailing bytes.")

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

    def roll_before_group_if_needed(self, row_count: int):
        """Roll current files before the next logical write group if needed."""
        self._require_finished_flush()
        if self._video_group_policy is None:
            return

        pending_rows = self.pending_row_count
        should_roll = pending_rows > 0 and (
            self._video_group_policy.pending_roll
            or pending_rows + row_count > self.target_file_row_num
            or (
                not self._normal_buffer.is_empty
                and self._normal_buffer.nbytes > self.target_file_size
            )
        )
        should_roll = should_roll or any(
            self.blob_writers[column].should_roll_before_group(row_count)
            for column in self.video_frame_columns
        )
        should_roll = should_roll or (
            self.vector_writer is not None
            and self.vector_writer.should_roll_before_group(row_count)
        )
        if should_roll:
            self._close_current_writers()

    def _should_roll_active_group(self) -> bool:
        return self._should_roll_normal() or (
            self._video_group_policy is not None
            and self.vector_writer is not None
            and self.vector_writer.rolling_file()
        )

    def _roll_or_defer_for_video_group(self):
        if self._video_group_policy is not None and self._video_group_policy.defer_roll():
            return
        self._close_current_writers()

    def _roll_before_video_group(self, next_group):
        if (
            self._video_group_policy is not None
            and self._video_group_policy.should_roll_before(next_group)
        ):
            self._close_current_writers()

    def _write_video_batches(self, data: pa.RecordBatch):
        for batch, group in self._video_group_runs(data):
            self._roll_before_video_group(group)
            self._video_group_policy.record(group)
            if group is None:
                self._write_bounded_batches(batch)
            else:
                self._write_batch(batch)

    def _write_bounded_batches(self, data: pa.RecordBatch):
        offset = 0
        # _write_batch keeps normal/blob/vector pending rows in lockstep
        # and closes all writers when the common row limit is reached.
        while offset < data.num_rows:
            capacity = self.target_file_row_num - self.pending_row_count
            if capacity <= 0:
                self._close_current_writers()
                capacity = self.target_file_row_num
            length = min(capacity, data.num_rows - offset)
            self._write_batch(data.slice(offset, length))
            offset += length

    def _video_group_runs(self, data: pa.RecordBatch):
        column_indices = [
            data.schema.get_field_index(column)
            for column in self.video_frame_columns
        ]
        missing = [
            column for column, index
            in zip(self.video_frame_columns, column_indices)
            if index < 0
        ]
        if missing:
            raise KeyError(f"Video columns were not found in the record batch: {missing}")
        if data.num_rows == 0:
            return

        columns = [data.column(index) for index in column_indices]
        start = 0
        current_group = self._video_group(column[0] for column in columns)
        for index in range(1, data.num_rows):
            next_group = self._video_group(column[index] for column in columns)
            if next_group != current_group:
                yield data.slice(start, index - start), current_group
                start = index
                current_group = next_group
        yield data.slice(start, data.num_rows - start), current_group

    @staticmethod
    def _video_group(values):
        group = tuple(video_payload_descriptor(value) for value in values)
        return group if any(value is not None for value in group) else None

    @property
    def pending_row_count(self) -> int:
        # Overrides the base property, which reads a buffer this writer never
        # fills. Normal, blob and vector rows are kept in lockstep, so any half
        # answers for all of them; the sidecars are asked only when the table
        # has no normal columns at all.
        if not self._normal_buffer.is_empty:
            return self._normal_buffer.num_rows
        for blob_writer in self.blob_writers.values():
            if blob_writer.current_writer is not None:
                return blob_writer.current_writer.row_count
        if self.vector_writer is not None:
            # Running count, not a folded buffer: this runs on every write.
            return self.vector_writer.pending_row_count
        return 0

    def _close_current_writers(self):
        super()._close_current_writers()
        if self._video_group_policy is not None:
            self._video_group_policy.reset()

    def _prepare_sidecar_commits(self, normal_meta):
        prepared = []
        for blob_column in self.blob_file_column_names:
            blob_writer = self.blob_writers[blob_column]
            writer_metas = blob_writer.prepare_commit()
            if normal_meta is not None:
                self._validate_consistency(normal_meta, writer_metas, blob_column)
            prepared.append((blob_writer, writer_metas))

        if self.vector_writer is not None:
            vector_metas = self.vector_writer.prepare_commit()
            if vector_metas and normal_meta is not None:
                self._validate_consistency(normal_meta, vector_metas, 'vector')
            prepared.append((self.vector_writer, vector_metas))
        return prepared

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
