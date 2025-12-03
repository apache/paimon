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

import logging

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.write.writer.data_writer import DataWriter

logger = logging.getLogger(__name__)


class KeyValueDataWriter(DataWriter):
    """Data writer for primary key tables with system fields and sorting."""

    def _process_data(self, data: pa.RecordBatch) -> pa.Table:
        enhanced_data = self._add_system_fields(data)
        return pa.Table.from_batches([self._sort_by_primary_key(enhanced_data)])

    def _merge_data(self, existing_data: pa.Table, new_data: pa.Table) -> pa.Table:
        combined = pa.concat_tables([existing_data, new_data])
        return self._sort_by_primary_key(combined)

    def _add_system_fields(self, data: pa.RecordBatch) -> pa.RecordBatch:
        """Add system fields: _KEY_{pk_key}, _SEQUENCE_NUMBER, _VALUE_KIND.

        Supports real RowKind from '__row_kind__' column in input data.
        If '__row_kind__' column is present, its values are used for _VALUE_KIND.
        Otherwise, defaults to INSERT (value 0) for all rows.
        """
        num_rows = data.num_rows
        enhanced_table = data

        for pk_key in reversed(self.trimmed_primary_keys):
            if pk_key in data.column_names:
                key_column = data.column(pk_key)
                enhanced_table = enhanced_table.add_column(0, f'_KEY_{pk_key}', key_column)

        sequence_column = pa.array([self.sequence_generator.next() for _ in range(num_rows)], type=pa.int64())
        enhanced_table = enhanced_table.add_column(len(self.trimmed_primary_keys), '_SEQUENCE_NUMBER', sequence_column)

        # Extract or generate RowKind column
        value_kind_column = self._extract_row_kind_column(data, num_rows)
        enhanced_table = enhanced_table.add_column(len(self.trimmed_primary_keys) + 1, '_VALUE_KIND',
                                                   value_kind_column)

        # Remove temporary '__row_kind__' column if it exists
        if '__row_kind__' in enhanced_table.column_names:
            idx = enhanced_table.column_names.index('__row_kind__')
            enhanced_table = enhanced_table.remove_column(idx)

        return enhanced_table

    def _extract_row_kind_column(self, data: pa.RecordBatch, num_rows: int) -> pa.Array:
        """Extract or generate RowKind column from input data.

        If '__row_kind__' column exists in input, validates and returns it.
        Otherwise, returns default INSERT kind (0) for all rows.

        Args:
            data: Input record batch
            num_rows: Number of rows

        Returns:
            RowKind column as int32 array

        Raises:
            ValueError: If '__row_kind__' column has invalid type or values
        """
        if '__row_kind__' in data.column_names:
            row_kind_col = data.column('__row_kind__')

            # Validate data type
            if row_kind_col.type != pa.int32():
                raise ValueError(
                    f"'__row_kind__' column must be of type int32, got {row_kind_col.type}"
                )

            # Validate values are in valid RowKind range [0-3]
            for i in range(len(row_kind_col)):
                value = row_kind_col[i].as_py()
                if value is not None and value not in [0, 1, 2, 3]:
                    raise ValueError(
                        f"Invalid RowKind value: {value} at row {i}. "
                        f"Valid values are 0(INSERT), 1(UPDATE_BEFORE), 2(UPDATE_AFTER), 3(DELETE)"
                    )

            logger.debug("Using real RowKind values from '__row_kind__' column")
            return row_kind_col

        # Default to INSERT kind for all rows
        logger.debug("No '__row_kind__' column found, defaulting to INSERT kind")
        return pa.array([0] * num_rows, type=pa.int32())

    def _sort_by_primary_key(self, data: pa.RecordBatch) -> pa.RecordBatch:
        """Sort data by primary key fields and sequence number.

        Args:
            data: Record batch to sort

        Returns:
            Sorted record batch
        """
        sort_keys = [(key, 'ascending') for key in self.trimmed_primary_keys]
        if '_SEQUENCE_NUMBER' in data.column_names:
            sort_keys.append(('_SEQUENCE_NUMBER', 'ascending'))

        sorted_indices = pc.sort_indices(data, sort_keys=sort_keys)
        sorted_batch = data.take(sorted_indices)
        return sorted_batch
