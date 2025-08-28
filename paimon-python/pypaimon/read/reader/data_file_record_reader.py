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

from pypaimon.common.predicate import Predicate
from pypaimon.read.partition_info import PartitionInfo
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import DataField, PyarrowFieldParser
from pypaimon.table.row.offset_row import OffsetRow


class DataFileBatchReader(RecordBatchReader):
    """
    Reads record batch from data files.
    """

    def __init__(self, format_reader: RecordBatchReader, index_mapping: List[int], partition_info: PartitionInfo,
                 system_primary_key: Optional[List[str]], fields: List[DataField],
                 predicate: Optional[Predicate] = None):
        self.format_reader = format_reader
        self.index_mapping = index_mapping
        self.partition_info = partition_info
        self.system_primary_key = system_primary_key
        self.predicate = predicate
        self.fields = fields
        self.schema_map = {field.name: field for field in PyarrowFieldParser.from_paimon_schema(fields)}

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        record_batch = self.format_reader.read_arrow_batch()
        if record_batch is None:
            return None

        if self.partition_info is None and self.index_mapping is None:
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

        final_batch = pa.RecordBatch.from_arrays(inter_arrays, schema=final_schema)

        if self.predicate is not None:
            final_batch = self._filter_batch_python(final_batch, inter_names)

        return final_batch

    def _filter_batch_python(self, batch: RecordBatch, field_names: List[str]) -> RecordBatch:
        if batch.num_rows == 0:
            return batch

        pydict = batch.to_pydict()
        filtered_rows = []

        actual_field_names = batch.schema.names

        # Create field name to index mapping
        field_name_to_index = {name: i for i, name in enumerate(actual_field_names)}

        for i in range(batch.num_rows):
            # Create row data with correct field mapping
            row_data = []
            for field_name in actual_field_names:
                row_data.append(pydict[field_name][i])

            row = OffsetRow(tuple(row_data), 0, len(actual_field_names))

            # Update predicate field indices if needed
            if self.predicate and self.predicate.field:
                field_index = field_name_to_index.get(self.predicate.field)
                if field_index is not None and self.predicate.index != field_index:
                    # Create a copy of the predicate with correct index
                    updated_predicate = Predicate(
                        method=self.predicate.method,
                        index=field_index,
                        field=self.predicate.field,
                        literals=self.predicate.literals
                    )
                    if updated_predicate.test(row):
                        filtered_rows.append(i)
                else:
                    if self.predicate.test(row):
                        filtered_rows.append(i)
            elif self.predicate and self.predicate.test(row):
                filtered_rows.append(i)

        if not filtered_rows:
            return batch.slice(0, 0)

        filtered_batch = batch.take(pa.array(filtered_rows))
        return filtered_batch

    def close(self) -> None:
        self.format_reader.close()
