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
        """Merge existing data with new data and deduplicate by primary key.
        
        The merge process:
        1. Concatenate existing and new data
        2. Sort by primary key fields and sequence number
        3. Deduplicate by primary key, keeping the record with maximum sequence number
        
        Args:
            existing_data: Previously buffered data
            new_data: Newly written data to be merged
            
        Returns:
            Deduplicated and sorted table
        """
        combined = pa.concat_tables([existing_data, new_data])
        sorted_data = self._sort_by_primary_key(combined)
        deduplicated_data = self._deduplicate_by_primary_key(sorted_data)
        return deduplicated_data

    def _add_system_fields(self, data: pa.RecordBatch) -> pa.RecordBatch:
        """Add system fields: _KEY_{pk_key}, _SEQUENCE_NUMBER, _VALUE_KIND."""
        num_rows = data.num_rows
        enhanced_table = data

        for pk_key in reversed(self.trimmed_primary_key):
            if pk_key in data.column_names:
                key_column = data.column(pk_key)
                enhanced_table = enhanced_table.add_column(0, f'_KEY_{pk_key}', key_column)

        sequence_column = pa.array([self.sequence_generator.next() for _ in range(num_rows)], type=pa.int64())
        enhanced_table = enhanced_table.add_column(len(self.trimmed_primary_key), '_SEQUENCE_NUMBER', sequence_column)

        # TODO: support real row kind here
        value_kind_column = pa.array([0] * num_rows, type=pa.int32())
        enhanced_table = enhanced_table.add_column(len(self.trimmed_primary_key) + 1, '_VALUE_KIND',
                                                   value_kind_column)

        return enhanced_table

    def _deduplicate_by_primary_key(self, data: pa.RecordBatch) -> pa.RecordBatch:
        """Deduplicate data by primary key, keeping the record with maximum sequence number.
        
        Prerequisite: data is sorted by (primary_keys, _SEQUENCE_NUMBER)
        
        Algorithm: Since data is sorted by primary key and then by sequence number in ascending
        order, for each primary key group, the last occurrence has the maximum sequence number.
        We iterate through and track the last index of each primary key, then keep only those rows.
        
        Args:
            data: Sorted record batch with system fields (_KEY_*, _SEQUENCE_NUMBER, _VALUE_KIND)
            
        Returns:
            Deduplicated record batch with only the latest record per primary key
        """
        if data.num_rows <= 1:
            return data
        
        # Build primary key column names (prefixed with _KEY_)
        pk_columns = [f'_KEY_{pk}' for pk in self.trimmed_primary_key]
        
        # First pass: find the last index for each primary key
        last_index_for_key = {}
        for i in range(data.num_rows):
            current_key = tuple(
                data.column(col)[i].as_py() for col in pk_columns
            )
            last_index_for_key[current_key] = i
        
        # Second pass: collect indices to keep (maintaining original order)
        indices_to_keep = []
        for i in range(data.num_rows):
            current_key = tuple(
                data.column(col)[i].as_py() for col in pk_columns
            )
            # Only keep this row if it's the last occurrence of this primary key
            if i == last_index_for_key[current_key]:
                indices_to_keep.append(i)
        
        # Extract kept rows using PyArrow's take operation
        indices_array = pa.array(indices_to_keep, type=pa.int64())
        return data.take(indices_array)

    def _sort_by_primary_key(self, data: pa.RecordBatch) -> pa.RecordBatch:
        """Sort data by primary key fields and sequence number.
        
        Args:
            data: Record batch to sort
            
        Returns:
            Sorted record batch
        """
        sort_keys = [(key, 'ascending') for key in self.trimmed_primary_key]
        if '_SEQUENCE_NUMBER' in data.column_names:
            sort_keys.append(('_SEQUENCE_NUMBER', 'ascending'))

        sorted_indices = pc.sort_indices(data, sort_keys=sort_keys)
        sorted_batch = data.take(sorted_indices)
        return sorted_batch
