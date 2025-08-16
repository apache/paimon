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


class SequenceGenerator:
    def __init__(self, start: int = 0):
        self.current = start

    def next(self) -> int:
        self.current += 1
        return self.current


sequence_generator = SequenceGenerator()


class KeyValueDataWriter(DataWriter):
    """Data writer for primary key tables with system fields and sorting."""

    def _process_data(self, data: pa.RecordBatch) -> pa.RecordBatch:
        enhanced_data = self._add_system_fields(data)
        return self._sort_by_primary_key(enhanced_data)

    def _merge_data(self, existing_data: pa.RecordBatch, new_data: pa.RecordBatch) -> pa.RecordBatch:
        combined = pa.concat_tables([existing_data, new_data])
        return self._sort_by_primary_key(combined)

    def _add_system_fields(self, data: pa.RecordBatch) -> pa.RecordBatch:
        """Add system fields: _KEY_{pk_key}, _SEQUENCE_NUMBER, _VALUE_KIND."""
        num_rows = data.num_rows

        # Collect all columns and their names
        columns = []
        names = []

        # Add key columns first
        for pk_key in reversed(self.trimmed_primary_key):
            if pk_key in data.schema.names:
                key_column = data.column(pk_key)
                columns.append(key_column)
                names.append(f'_KEY_{pk_key}')

        # Add sequence number column
        sequence_column = pa.array([sequence_generator.next() for _ in range(num_rows)], type=pa.int64())
        columns.append(sequence_column)
        names.append('_SEQUENCE_NUMBER')

        # Add value kind column
        # TODO: support real row kind here
        value_kind_column = pa.repeat(0, num_rows)
        columns.append(value_kind_column)
        names.append('_VALUE_KIND')

        # Add original data columns
        for i in range(data.num_columns):
            columns.append(data.column(i))
            names.append(data.schema.names[i])

        # Create new RecordBatch with all columns
        return pa.record_batch(columns, names=names)

    def _sort_by_primary_key(self, data: pa.RecordBatch) -> pa.RecordBatch:
        sort_keys = [(key, 'ascending') for key in self.trimmed_primary_key]
        if '_SEQUENCE_NUMBER' in data.schema.names:
            sort_keys.append(('_SEQUENCE_NUMBER', 'ascending'))

        sorted_indices = pc.sort_indices(data, sort_keys=sort_keys)
        sorted_batch = data.take(sorted_indices)
        return sorted_batch
