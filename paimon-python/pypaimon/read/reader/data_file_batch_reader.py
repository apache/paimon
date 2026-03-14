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

from typing import List, Optional

import pyarrow as pa
from pyarrow import RecordBatch

from pypaimon.common.file_io import FileIO
from pypaimon.read.partition_info import PartitionInfo
from pypaimon.read.reader.format_blob_reader import FormatBlobReader
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import DataField, PyarrowFieldParser
from pypaimon.table.row.blob import Blob, BlobDescriptor
from pypaimon.table.special_fields import SpecialFields


class DataFileBatchReader(RecordBatchReader):
    """
    Reads record batch from files of different formats
    """

    def __init__(self, format_reader: RecordBatchReader, index_mapping: List[int], partition_info: PartitionInfo,
                 system_primary_key: Optional[List[str]], fields: List[DataField],
                 max_sequence_number: int,
                 first_row_id: int,
                 row_tracking_enabled: bool,
                 system_fields: dict,
                 blob_as_descriptor: bool = False,
                 blob_descriptor_fields: Optional[set] = None,
                 file_io: Optional[FileIO] = None):
        self.format_reader = format_reader
        self.index_mapping = index_mapping
        self.partition_info = partition_info
        self.system_primary_key = system_primary_key
        self.schema_map = {field.name: field for field in PyarrowFieldParser.from_paimon_schema(fields)}
        self.row_tracking_enabled = row_tracking_enabled
        self.first_row_id = first_row_id
        self.max_sequence_number = max_sequence_number
        self.system_fields = system_fields
        self.blob_as_descriptor = blob_as_descriptor
        self.blob_descriptor_fields = blob_descriptor_fields or set()
        self.file_io = file_io
        self.blob_field_names = {
            field.name
            for field in fields
            if hasattr(field.type, 'type') and field.type.type == 'BLOB'
        }
        self.descriptor_blob_fields = {
            field_name
            for field_name in self.blob_descriptor_fields
            if field_name in self.blob_field_names
        }

    def read_arrow_batch(self, start_idx=None, end_idx=None) -> Optional[RecordBatch]:
        if isinstance(self.format_reader, FormatBlobReader):
            record_batch = self.format_reader.read_arrow_batch(start_idx, end_idx)
        else:
            record_batch = self.format_reader.read_arrow_batch()
        if record_batch is None:
            return None

        if self.partition_info is None and self.index_mapping is None:
            if self.row_tracking_enabled and self.system_fields:
                record_batch = self._assign_row_tracking(record_batch)
            return record_batch

        inter_arrays = []
        inter_names = []
        num_rows = record_batch.num_rows

        if self.partition_info is not None:
            for i in range(self.partition_info.size()):
                if self.partition_info.is_partition_row(i):
                    partition_value, partition_field = self.partition_info.get_partition_value(i)
                    const_array = pa.repeat(partition_value, num_rows)
                    inter_arrays.append(const_array)
                    inter_names.append(partition_field.name)
                else:
                    real_index = self.partition_info.get_real_index(i)
                    if real_index < record_batch.num_columns:
                        inter_arrays.append(record_batch.column(real_index))
                        inter_names.append(record_batch.schema.field(real_index).name)
        else:
            inter_arrays = record_batch.columns
            inter_names = record_batch.schema.names

        if self.index_mapping is not None:
            mapped_arrays = []
            mapped_names = []
            for i, real_index in enumerate(self.index_mapping):
                if 0 <= real_index < len(inter_arrays):
                    mapped_arrays.append(inter_arrays[real_index])
                    mapped_names.append(inter_names[real_index])
                else:
                    null_array = pa.nulls(num_rows)
                    mapped_arrays.append(null_array)
                    mapped_names.append(f"null_col_{i}")

            if self.system_primary_key:
                for i in range(len(self.system_primary_key)):
                    if not mapped_names[i].startswith("_KEY_"):
                        mapped_names[i] = f"_KEY_{mapped_names[i]}"

            inter_arrays = mapped_arrays
            inter_names = mapped_names

        # to contains 'not null' property
        final_fields = []
        for i, name in enumerate(inter_names):
            array = inter_arrays[i]
            target_field = self.schema_map.get(name)
            if not target_field:
                target_field = pa.field(name, array.type)
            final_fields.append(target_field)
        final_schema = pa.schema(final_fields)
        record_batch = pa.RecordBatch.from_arrays(inter_arrays, schema=final_schema)

        # Handle row tracking fields
        if self.row_tracking_enabled and self.system_fields:
            record_batch = self._assign_row_tracking(record_batch)

        record_batch = self._convert_descriptor_stored_blob_columns(record_batch)

        return record_batch

    def _convert_descriptor_stored_blob_columns(self, record_batch: RecordBatch) -> RecordBatch:
        if isinstance(self.format_reader, FormatBlobReader):
            return record_batch
        if not self.descriptor_blob_fields:
            return record_batch

        schema_names = set(record_batch.schema.names)
        target_fields = [f for f in self.descriptor_blob_fields if f in schema_names]
        if not target_fields:
            return record_batch

        arrays = list(record_batch.columns)
        for field_name in target_fields:
            field_idx = record_batch.schema.get_field_index(field_name)
            values = record_batch.column(field_idx).to_pylist()

            if self.blob_as_descriptor:
                converted = [self._normalize_blob_cell(v) for v in values]
            else:
                converted = [self._blob_cell_to_data(v) for v in values]
            arrays[field_idx] = pa.array(converted, type=pa.large_binary())

        return pa.RecordBatch.from_arrays(arrays, schema=record_batch.schema)

    @staticmethod
    def _normalize_blob_cell(value):
        if value is None:
            return None
        if hasattr(value, 'as_py'):
            value = value.as_py()
        if isinstance(value, str):
            value = value.encode('utf-8')
        if isinstance(value, bytearray):
            value = bytes(value)
        return value

    def _blob_cell_to_data(self, value):
        value = self._normalize_blob_cell(value)
        if value is None:
            return None

        if not isinstance(value, bytes):
            return value

        descriptor = self._deserialize_descriptor_or_none(value)
        if descriptor is None:
            return value

        try:
            uri_reader = self.file_io.uri_reader_factory.create(descriptor.uri)
            blob = Blob.from_descriptor(uri_reader, descriptor)
            return blob.to_data()
        except Exception as e:
            raise RuntimeError(
                "Failed to read blob bytes from descriptor URI while converting blob value."
            ) from e

    @staticmethod
    def _deserialize_descriptor_or_none(raw: bytes):
        if not BlobDescriptor.is_blob_descriptor(raw):
            return None
        return BlobDescriptor.deserialize(raw)

    def _assign_row_tracking(self, record_batch: RecordBatch) -> RecordBatch:
        """Assign row tracking meta fields (_ROW_ID and _SEQUENCE_NUMBER)."""
        arrays = list(record_batch.columns)

        # Handle _ROW_ID field
        if SpecialFields.ROW_ID.name in self.system_fields.keys():
            idx = self.system_fields[SpecialFields.ROW_ID.name]
            # Create a new array that fills with computed row IDs
            arrays[idx] = pa.array(range(self.first_row_id, self.first_row_id + record_batch.num_rows), type=pa.int64())

        # Handle _SEQUENCE_NUMBER field
        if SpecialFields.SEQUENCE_NUMBER.name in self.system_fields.keys():
            idx = self.system_fields[SpecialFields.SEQUENCE_NUMBER.name]
            # Create a new array that fills with max_sequence_number
            arrays[idx] = pa.repeat(self.max_sequence_number, record_batch.num_rows)

        names = record_batch.schema.names
        table = None
        for i, name in enumerate(names):
            field = pa.field(
                name, arrays[i].type,
                nullable=record_batch.schema.field(name).nullable
            )
            if table is None:
                table = pa.table({name: arrays[i]}, schema=pa.schema([field]))
            else:
                table = table.append_column(field, arrays[i])
        return table.to_batches()[0]

    def close(self) -> None:
        self.format_reader.close()
