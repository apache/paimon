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

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.write.writer.data_writer import DataWriter


class KeyValueDataWriter(DataWriter):
    """Data writer for primary key tables with system fields and sorting."""

    def _process_data(self, data: pa.RecordBatch) -> pa.Table:
        enhanced_data = self._add_system_fields(data)
        return pa.Table.from_batches([self._sort_by_primary_key(enhanced_data)])

    def _merge_data(self, existing_data: pa.Table, new_data: pa.Table) -> pa.Table:
        combined = pa.concat_tables([existing_data, new_data])
        return self._sort_by_primary_key(combined)

    def _add_system_fields(self, data: pa.RecordBatch) -> pa.RecordBatch:
        """
        Add system fields: _KEY_{pk_key}, _SEQUENCE_NUMBER, _VALUE_KIND.
        """
        num_rows = data.num_rows
        
        new_arrays = []
        new_names = []
        
        for pk_key in self.trimmed_primary_keys:
            if pk_key in data.schema.names:
                key_column = data.column(pk_key)
                new_arrays.append(key_column)
                new_names.append(f'_KEY_{pk_key}')
        
        sequence_column = pa.array([self.sequence_generator.next() for _ in range(num_rows)], type=pa.int64())
        new_arrays.append(sequence_column)
        new_names.append('_SEQUENCE_NUMBER')
        
        # TODO: support real row kind here
        value_kind_column = pa.array([0] * num_rows, type=pa.int8())
        new_arrays.append(value_kind_column)
        new_names.append('_VALUE_KIND')
        
        for i in range(data.num_columns):
            new_arrays.append(data.column(i))
            new_names.append(data.schema.names[i])
        
        return pa.RecordBatch.from_arrays(new_arrays, names=new_names)

    def _sort_by_primary_key(self, data: pa.RecordBatch) -> pa.RecordBatch:
        sort_keys = [(key, 'ascending') for key in self.trimmed_primary_keys]
        if '_SEQUENCE_NUMBER' in data.schema.names:
            sort_keys.append(('_SEQUENCE_NUMBER', 'ascending'))

        sorted_indices = pc.sort_indices(data, sort_keys=sort_keys)
        sorted_batch = data.take(sorted_indices)
        return sorted_batch
