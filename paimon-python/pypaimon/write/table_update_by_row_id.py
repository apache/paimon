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
import bisect
from typing import Dict, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.special_fields import SpecialFields
from pypaimon.write.file_store_write import FileStoreWrite


class TableUpdateByRowId:
    """
    Table update for partial column updates (data evolution).

    This update is designed for adding/updating specific columns in existing tables.
    Input data should contain _ROW_ID column.
    """

    FIRST_ROW_ID_COLUMN = '_FIRST_ROW_ID'

    def __init__(self, table, commit_user: str):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.commit_user = commit_user

        # Load existing first_row_ids and build partition map
        (self.first_row_ids,
         self.first_row_id_to_partition_map,
         self.first_row_id_to_row_count_map,
         self.total_row_count) = self._load_existing_files_info()

        # Collect commit messages
        self.commit_messages = []

    def _load_existing_files_info(self):
        """Load existing first_row_ids and build partition map for efficient lookup."""
        first_row_ids = []
        first_row_id_to_partition_map: Dict[int, GenericRow] = {}
        first_row_id_to_row_count_map: Dict[int, int] = {}

        read_builder = self.table.new_read_builder()
        scan = read_builder.new_scan()
        splits = scan.plan().splits()

        for split in splits:
            for file in split.files:
                if file.first_row_id is not None and not file.file_name.endswith('.blob'):
                    first_row_id = file.first_row_id
                    first_row_ids.append(first_row_id)
                    first_row_id_to_partition_map[first_row_id] = split.partition
                    first_row_id_to_row_count_map[first_row_id] = file.row_count

        total_row_count = sum(first_row_id_to_row_count_map.values())

        return sorted(list(set(first_row_ids))
                      ), first_row_id_to_partition_map, first_row_id_to_row_count_map, total_row_count

    def update_columns(self, data: pa.Table, column_names: List[str]) -> List:
        """
        Add or update columns in the table.

        Args:
            data: Input data containing row_id and columns to update
            column_names: Names of columns to update (excluding row_id)

        Returns:
            List of commit messages
        """

        # Validate column_names is not empty
        if not column_names:
            raise ValueError("column_names cannot be empty")

        # Validate input data has row_id column
        if SpecialFields.ROW_ID.name not in data.column_names:
            raise ValueError(f"Input data must contain {SpecialFields.ROW_ID.name} column")

        # Validate all update columns exist in the schema
        for col_name in column_names:
            if col_name not in self.table.field_names:
                raise ValueError(f"Column {col_name} not found in table schema")

        # Validate data row count matches total row count
        if data.num_rows != self.total_row_count:
            raise ValueError(
                f"Input data row count ({data.num_rows}) does not match table total row count ({self.total_row_count})")

        # Sort data by _ROW_ID column
        sorted_data = data.sort_by([(SpecialFields.ROW_ID.name, "ascending")])

        # Calculate first_row_id for each row
        data_with_first_row_id = self._calculate_first_row_id(sorted_data)

        # Group by first_row_id and write each group
        self._write_by_first_row_id(data_with_first_row_id, column_names)

        return self.commit_messages

    def _calculate_first_row_id(self, data: pa.Table) -> pa.Table:
        """Calculate _first_row_id for each row based on _ROW_ID."""
        row_ids = data[SpecialFields.ROW_ID.name].to_pylist()

        # Validate that row_ids are monotonically increasing starting from 0
        expected_row_ids = list(range(len(row_ids)))
        if row_ids != expected_row_ids:
            raise ValueError(f"Row IDs are not monotonically increasing starting from 0. "
                             f"Expected: {expected_row_ids}")

        # Calculate first_row_id for each row_id
        first_row_id_values = []
        for row_id in row_ids:
            first_row_id = self._floor_binary_search(self.first_row_ids, row_id)
            first_row_id_values.append(first_row_id)

        # Add first_row_id column to the table
        first_row_id_array = pa.array(first_row_id_values, type=pa.int64())
        return data.append_column(self.FIRST_ROW_ID_COLUMN, first_row_id_array)

    def _floor_binary_search(self, sorted_seq: List[int], value: int) -> int:
        """Binary search to find the floor value in sorted sequence."""
        if not sorted_seq:
            raise ValueError("The input sorted sequence is empty.")

        idx = bisect.bisect_right(sorted_seq, value) - 1
        if idx < 0:
            raise ValueError(f"Value {value} is less than the first element in the sorted sequence.")

        return sorted_seq[idx]

    def _write_by_first_row_id(self, data: pa.Table, column_names: List[str]):
        """Write data grouped by first_row_id."""
        # Extract unique first_row_id values
        first_row_id_array = data[self.FIRST_ROW_ID_COLUMN]
        unique_first_row_ids = pc.unique(first_row_id_array).to_pylist()

        for first_row_id in unique_first_row_ids:
            # Filter rows for this first_row_id
            mask = pc.equal(first_row_id_array, first_row_id)
            group_data = data.filter(mask)

            # Get partition for this first_row_id
            partition = self._find_partition_by_first_row_id(first_row_id)

            if partition is None:
                raise ValueError(f"No existing file found for first_row_id {first_row_id}")

            # Write this group
            self._write_group(partition, first_row_id, group_data, column_names)

    def _find_partition_by_first_row_id(self, first_row_id: int) -> Optional[GenericRow]:
        """Find the partition for a given first_row_id using pre-built partition map."""
        return self.first_row_id_to_partition_map.get(first_row_id)

    def _write_group(self, partition: GenericRow, first_row_id: int,
                     data: pa.Table, column_names: List[str]):
        """Write a group of data with the same first_row_id."""

        # Validate data row count matches the first_row_id's row count
        expected_row_count = self.first_row_id_to_row_count_map.get(first_row_id, 0)
        if data.num_rows != expected_row_count:
            raise ValueError(
                f"Data row count ({data.num_rows}) does not match expected row count ({expected_row_count}) "
                f"for first_row_id {first_row_id}")

        # Create a file store write for this partition
        file_store_write = FileStoreWrite(self.table, self.commit_user)

        # Set write columns to only update specific columns
        # Note: _ROW_ID is metadata column, not part of schema
        write_cols = column_names
        file_store_write.write_cols = write_cols

        # Convert partition to tuple for hashing
        partition_tuple = tuple(partition.values)

        # Write data - convert Table to RecordBatch
        data_to_write = data.select(write_cols)
        for batch in data_to_write.to_batches():
            file_store_write.write(partition_tuple, 0, batch)

        # Prepare commit and assign first_row_id
        commit_messages = file_store_write.prepare_commit(BATCH_COMMIT_IDENTIFIER)

        # Assign first_row_id to the new files
        for msg in commit_messages:
            for file in msg.new_files:
                # Assign the same first_row_id as the original file
                file.first_row_id = first_row_id
                file.write_cols = write_cols

        self.commit_messages.extend(commit_messages)

        # Close the writer
        file_store_write.close()
