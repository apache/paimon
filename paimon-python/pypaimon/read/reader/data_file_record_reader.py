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

from pypaimon.read.partition_info import PartitionInfo
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import DataField, PyarrowFieldParser


class DataFileBatchReader(RecordBatchReader):
    """
    Reads record batch from data files.
    """

    def __init__(self, format_reader: RecordBatchReader, index_mapping: List[int], partition_info: PartitionInfo,
                 system_primary_key: Optional[List[str]], fields: List[DataField]):
        self.format_reader = format_reader
        self.index_mapping = index_mapping
        self.partition_info = partition_info
        self.system_primary_key = system_primary_key
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

        return pa.RecordBatch.from_arrays(inter_arrays, schema=final_schema)

    def close(self) -> None:
        self.format_reader.close()
