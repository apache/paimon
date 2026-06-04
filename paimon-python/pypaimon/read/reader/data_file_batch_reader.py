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

from typing import List, Optional

import pyarrow as pa
from pyarrow import RecordBatch

from pypaimon.common.file_io import FileIO
from pypaimon.read.partition_info import PartitionInfo
from pypaimon.read.reader.format_blob_reader import FormatBlobReader
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import DataField, PyarrowFieldParser
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
                 file_io: Optional[FileIO] = None,
                 row_id_offsets: Optional[List[int]] = None):
        self.format_reader = format_reader
        self.index_mapping = index_mapping
        self.partition_info = partition_info
        self.system_primary_key = system_primary_key
        self.schema_map = {field.name: field for field in PyarrowFieldParser.from_paimon_schema(fields)}
        self.row_tracking_enabled = row_tracking_enabled
        self.first_row_id = first_row_id
        self.row_id_offsets = row_id_offsets
        self._row_id_cursor = 0
        self.max_sequence_number = max_sequence_number
        self.system_fields = system_fields
        self.file_io = file_io

    def read_arrow_batch(self, start_idx=None, end_idx=None) -> Optional[RecordBatch]:
        if isinstance(self.format_reader, FormatBlobReader):
            record_batch = self.format_reader.read_arrow_batch(start_idx, end_idx)
        else:
            record_batch = self.format_reader.read_arrow_batch()
        if record_batch is None:
            return None

        if self.partition_info is None and self.index_mapping is None:
            # A file written under an older schema (e.g. before an INT -> BIGINT
            # promotion or a DECIMAL precision change) yields columns in the
            # data file's original types. Without reordering or partition padding
            # to rebuild the batch, those old types would otherwise leak through
            # here -- returning a type that depends on whether this read happens
            # to span newer-schema files, and failing to concatenate when it
            # does. Align them to the current read schema, mirroring the rebuild
            # path below.
            record_batch = self._align_batch_to_read_schema(
                record_batch.schema.names, record_batch.columns)
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

        # Rebuild the batch typed by the read schema (carries 'not null' and
        # aligns old-schema column types).
        record_batch = self._align_batch_to_read_schema(inter_names, inter_arrays)

        # Handle row tracking fields
        if self.row_tracking_enabled and self.system_fields:
            record_batch = self._assign_row_tracking(record_batch)

        return record_batch

    def _align_batch_to_read_schema(self, names: List[str], arrays: list) -> RecordBatch:
        """Build a record batch for ``names``/``arrays`` typed by the read schema.

        Each known field is cast to the current read schema's type, which also
        carries the 'not null' property; unknown columns keep the array's own
        type. Columns whose type already matches are reused as-is, keeping the
        common (non-evolution) path zero-copy.

        Casts use ``safe=False`` to match Java ``CastExecutors`` semantics for
        the read-time conversions a user-approved schema evolution implies
        (e.g. DECIMAL scale-down or DOUBLE -> INT truncate rather than raise).
        Evolution legality is the writer's concern (``DataTypeCasts``); the read
        path only materializes the result.
        """
        out_arrays = []
        out_fields = []
        for name, array in zip(names, arrays):
            target_field = self.schema_map.get(name)
            if target_field is None:
                target_field = pa.field(name, array.type)
            elif array.type != target_field.type:
                array = array.cast(target_field.type, safe=False)
            out_arrays.append(array)
            out_fields.append(target_field)
        return pa.RecordBatch.from_arrays(out_arrays, schema=pa.schema(out_fields))

    def _assign_row_tracking(self, record_batch: RecordBatch) -> RecordBatch:
        """Assign row tracking meta fields (_ROW_ID and _SEQUENCE_NUMBER)."""
        arrays = list(record_batch.columns)

        # Handle _ROW_ID field
        if SpecialFields.ROW_ID.name in self.system_fields.keys():
            idx = self.system_fields[SpecialFields.ROW_ID.name]
            if self.row_id_offsets is not None:
                end = self._row_id_cursor + record_batch.num_rows
                row_ids = [self.first_row_id + o for o in self.row_id_offsets[self._row_id_cursor:end]]
                arrays[idx] = pa.array(row_ids, type=pa.int64())
                self._row_id_cursor = end
            else:
                row_id_range = range(self.first_row_id, self.first_row_id + record_batch.num_rows)
                arrays[idx] = pa.array(row_id_range, type=pa.int64())
                self.first_row_id += record_batch.num_rows

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
