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

from typing import List, Optional, Tuple

import pyarrow as pa
from pyarrow import RecordBatch

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
                 system_fields: dict):
        self.format_reader = format_reader
        self.index_mapping = index_mapping
        self.partition_info = partition_info
        self.system_primary_key = system_primary_key
        self.schema_map = {field.name: field for field in PyarrowFieldParser.from_paimon_schema(fields)}
        self.row_tracking_enabled = row_tracking_enabled
        self.first_row_id = first_row_id
        self.max_sequence_number = max_sequence_number
        self.system_fields = system_fields
        self.requested_field_names = [field.name for field in fields] if fields else None
        self.fields = fields

    def _align_to_requested_names(
        self,
        inter_arrays: List,
        inter_names: List,
        requested_field_names: List[str],
        num_rows: int,
    ) -> Tuple[List, List]:
        name_to_idx = {n: i for i, n in enumerate(inter_names)}
        ordered_arrays = []
        ordered_names = []
        for name in requested_field_names:
            idx = name_to_idx.get(name)
            if idx is None and name.startswith("_KEY_") and name[5:] in name_to_idx:
                idx = name_to_idx[name[5:]]
            if idx is not None:
                ordered_arrays.append(inter_arrays[idx])
                ordered_names.append(name)
            else:
                field = self.schema_map.get(name)
                ordered_arrays.append(
                    pa.nulls(num_rows, type=field.type) if field is not None else pa.nulls(num_rows)
                )
                ordered_names.append(name)
        return ordered_arrays, ordered_names

    def read_arrow_batch(self, start_idx=None, end_idx=None) -> Optional[RecordBatch]:
        if isinstance(self.format_reader, FormatBlobReader):
            record_batch = self.format_reader.read_arrow_batch(start_idx, end_idx)
        else:
            record_batch = self.format_reader.read_arrow_batch()
        if record_batch is None:
            return None

        num_rows = record_batch.num_rows
        if self.partition_info is None and self.index_mapping is None:
            if self.row_tracking_enabled and self.system_fields:
                record_batch = self._assign_row_tracking(record_batch)
            if self.requested_field_names is not None:
                inter_arrays = list(record_batch.columns)
                inter_names = list(record_batch.schema.names)
                ordered_arrays, ordered_names = self._align_to_requested_names(
                    inter_arrays, inter_names, self.requested_field_names, num_rows
                )
                record_batch = pa.RecordBatch.from_arrays(ordered_arrays, ordered_names)
            return record_batch

        if (self.partition_info is None and self.index_mapping is not None
                and not self.requested_field_names):
            ncol = record_batch.num_columns
            if len(self.index_mapping) == ncol and self.index_mapping == list(range(ncol)):
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
                    name = (
                        self.requested_field_names[i]
                        if self.requested_field_names and i < len(self.requested_field_names)
                        else f"_col_{i}"
                    )
                    batch_names = record_batch.schema.names
                    col_idx = None
                    if name in batch_names:
                        col_idx = record_batch.schema.get_field_index(name)
                    elif name.startswith("_KEY_") and name[5:] in batch_names:
                        col_idx = record_batch.schema.get_field_index(name[5:])
                    if col_idx is not None:
                        inter_arrays.append(record_batch.column(col_idx))
                        inter_names.append(name)
                    elif real_index < record_batch.num_columns:
                        inter_arrays.append(record_batch.column(real_index))
                        inter_names.append(name)
                    else:
                        field = self.schema_map.get(name)
                        inter_arrays.append(
                            pa.nulls(num_rows, type=field.type) if field is not None else pa.nulls(num_rows)
                        )
                        inter_names.append(name)
        else:
            inter_arrays = list(record_batch.columns)
            inter_names = list(record_batch.schema.names)

        if self.requested_field_names is not None:
            inter_arrays, inter_names = self._align_to_requested_names(
                inter_arrays, inter_names, self.requested_field_names, num_rows
            )

        if self.index_mapping is not None and not (
                self.requested_field_names is not None and inter_names == self.requested_field_names):
            mapped_arrays = []
            mapped_names = []
            partition_names = (
                set(pf.name for pf in self.partition_info.partition_fields)
                if self.partition_info else set()
            )
            non_partition_indices = [idx for idx, name in enumerate(inter_names) if name not in partition_names]
            for i, real_index in enumerate(self.index_mapping):
                if 0 <= real_index < len(non_partition_indices):
                    actual_index = non_partition_indices[real_index]
                    mapped_arrays.append(inter_arrays[actual_index])
                    mapped_names.append(inter_names[actual_index])
                else:
                    name = (
                        self.requested_field_names[i]
                        if self.requested_field_names and i < len(self.requested_field_names)
                        else f"null_col_{i}"
                    )
                    field = self.schema_map.get(name)
                    null_array = pa.nulls(num_rows, type=field.type) if field is not None else pa.nulls(num_rows)
                    mapped_arrays.append(null_array)
                    mapped_names.append(name)

            if self.partition_info:
                partition_arrays_map = {
                    inter_names[i]: inter_arrays[i]
                    for i in range(len(inter_names))
                    if inter_names[i] in partition_names
                }

                if self.requested_field_names:
                    final_arrays = []
                    final_names = []
                    mapped_name_to_array = {name: arr for name, arr in zip(mapped_names, mapped_arrays)}

                    for name in self.requested_field_names:
                        if name in mapped_name_to_array:
                            final_arrays.append(mapped_name_to_array[name])
                            final_names.append(name)
                        elif name in partition_arrays_map:
                            final_arrays.append(partition_arrays_map[name])
                            final_names.append(name)
                        else:
                            # Field not in file (e.g. index_mapping -1): output null column
                            field = self.schema_map.get(name)
                            null_arr = pa.nulls(num_rows, type=field.type) if field is not None else pa.nulls(num_rows)
                            final_arrays.append(null_arr)
                            final_names.append(name)

                    inter_arrays = final_arrays
                    inter_names = final_names
                else:
                    mapped_name_set = set(mapped_names)
                    for name, arr in partition_arrays_map.items():
                        if name not in mapped_name_set:
                            mapped_arrays.append(arr)
                            mapped_names.append(name)
                    inter_arrays = mapped_arrays
                    inter_names = mapped_names
            else:
                inter_arrays = mapped_arrays
                inter_names = mapped_names

            if self.system_primary_key:
                for i in range(len(self.system_primary_key)):
                    if i < len(inter_names) and not inter_names[i].startswith("_KEY_"):
                        inter_names[i] = f"_KEY_{inter_names[i]}"

        if self.requested_field_names is not None and len(inter_arrays) < len(self.requested_field_names):
            for name in self.requested_field_names[len(inter_arrays):]:
                field = self.schema_map.get(name)
                inter_arrays.append(
                    pa.nulls(num_rows, type=field.type) if field is not None else pa.nulls(num_rows)
                )
                inter_names.append(name)

        for i, name in enumerate(inter_names):
            target_field = self.schema_map.get(name)
            if target_field is not None and inter_arrays[i].type != target_field.type:
                try:
                    inter_arrays[i] = inter_arrays[i].cast(target_field.type)
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                    inter_arrays[i] = pa.nulls(num_rows, type=target_field.type)

        # to contains 'not null' property
        final_fields = []
        for i, name in enumerate(inter_names):
            array = inter_arrays[i]
            target_field = self.schema_map.get(name)
            if target_field is None:
                final_fields.append(pa.field(name, array.type))
            else:
                final_fields.append(target_field)
        final_schema = pa.schema(final_fields)
        record_batch = pa.RecordBatch.from_arrays(inter_arrays, schema=final_schema)

        # Handle row tracking fields
        if self.row_tracking_enabled and self.system_fields:
            record_batch = self._assign_row_tracking(record_batch)

        return record_batch

    def _assign_row_tracking(self, record_batch: RecordBatch) -> RecordBatch:
        """Assign row tracking meta fields (_ROW_ID and _SEQUENCE_NUMBER)."""
        arrays = list(record_batch.columns)
        num_cols = len(arrays)

        # Handle _ROW_ID field
        if SpecialFields.ROW_ID.name in self.system_fields.keys():
            idx = self.system_fields[SpecialFields.ROW_ID.name]
            if idx < num_cols:
                if self.first_row_id is None:
                    raise ValueError(
                        "Row tracking requires first_row_id on the file; "
                        "got None. Ensure file metadata has first_row_id when reading _ROW_ID."
                    )
                arrays[idx] = pa.array(
                    range(self.first_row_id, self.first_row_id + record_batch.num_rows),
                    type=pa.int64())

        # Handle _SEQUENCE_NUMBER field
        if SpecialFields.SEQUENCE_NUMBER.name in self.system_fields.keys():
            idx = self.system_fields[SpecialFields.SEQUENCE_NUMBER.name]
            # Create a new array that fills with max_sequence_number
            if idx < num_cols:
                arrays[idx] = pa.repeat(self.max_sequence_number, record_batch.num_rows)

        names = record_batch.schema.names
        fields = [
            pa.field(name, arrays[i].type, nullable=record_batch.schema.field(name).nullable)
            for i, name in enumerate(names)
        ]
        return pa.RecordBatch.from_arrays(arrays, schema=pa.schema(fields))

    def close(self) -> None:
        self.format_reader.close()
