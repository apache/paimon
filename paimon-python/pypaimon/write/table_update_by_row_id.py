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
from typing import Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.split import DataSplit
from pypaimon.read.table_read import TableRead
from pypaimon.utils.range import Range
from pypaimon.schema.data_types import DataField
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.special_fields import SpecialFields
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.file_store_write import FileStoreWrite


class TableUpdateByRowId:
    """
    Table update for partial column updates (data evolution).

    This update is designed for adding/updating specific columns in existing tables.
    Input data should contain _ROW_ID column.
    """

    FIRST_ROW_ID_COLUMN = '_FIRST_ROW_ID'

    def __init__(self, table, commit_user: str, commit_identifier: int):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.commit_user = commit_user
        self.commit_identifier = commit_identifier

        # Snapshot the current state once: a single ``first_row_id -> (split, files)``
        # map is enough to drive every downstream lookup (partition, row-count, read).
        (self.snapshot_id,
         self.first_row_ids,
         self._first_row_id_index,
         self.total_row_count) = self._load_existing_files_info()

        self.commit_messages: List[CommitMessage] = []

    def _load_existing_files_info(
            self,
    ) -> Tuple[int, List[int], Dict[int, Tuple[DataSplit, List[DataFileMeta]]], int]:
        """Scan the latest snapshot once and index files by ``first_row_id``.

        Returns:
            A 4-tuple of ``(snapshot_id, sorted_unique_first_row_ids, index, total_row_count)``
            where ``index`` maps each ``first_row_id`` to the owning split and
            the list of files with that id (a single id may belong to multiple
            files when data evolution has split a logical row range).
        """
        plan = self.table.new_read_builder().new_scan().plan()
        splits = plan.splits()

        index: Dict[int, Tuple[DataSplit, List[DataFileMeta]]] = {}
        row_id_ranges: List[Range] = []
        for split in splits:
            for file in split.files:
                if file.first_row_id is None or file.file_name.endswith('.blob'):
                    continue
                row_id_ranges.append(file.row_id_range())
                entry = index.get(file.first_row_id)
                if entry is None:
                    index[file.first_row_id] = (split, [file])
                else:
                    entry[1].append(file)

        # Multiple physical files may share the same first_row_id (data evolution);
        # summing row_count per file would over-count logical rows and widen
        # the _ROW_ID validation range incorrectly.
        if row_id_ranges:
            merged = Range.sort_and_merge_overlap(row_id_ranges, True, True)
            total_row_count = sum(r.count() for r in merged)
        else:
            total_row_count = 0

        snapshot_id = plan.snapshot_id if plan.snapshot_id is not None else -1
        return snapshot_id, sorted(index.keys()), index, total_row_count

    def update_columns(self, data: pa.Table, column_names: List[str]) -> List[CommitMessage]:
        """
        Add or update columns in the table.

        Args:
            data: Input data containing row_id and columns to update
            column_names: Names of columns to update (excluding row_id)

        Returns:
            List of commit messages
        """

        if not column_names:
            raise ValueError("column_names cannot be empty")

        if SpecialFields.ROW_ID.name not in data.column_names:
            raise ValueError(f"Input data must contain {SpecialFields.ROW_ID.name} column")

        for col_name in column_names:
            if col_name not in self.table.field_names:
                raise ValueError(f"Column {col_name} not found in table schema")

        sorted_data = data.sort_by([(SpecialFields.ROW_ID.name, "ascending")])
        data_with_first_row_id = self._calculate_first_row_id(sorted_data)
        self._write_by_first_row_id(data_with_first_row_id, column_names)

        return self.commit_messages

    def _calculate_first_row_id(self, data: pa.Table) -> pa.Table:
        """Append ``_FIRST_ROW_ID`` to *data* by looking up each ``_ROW_ID``.

        Validates that every input ``_ROW_ID`` is unique and falls in
        ``[0, total_row_count)``. Supports partial / non-consecutive updates.
        """
        row_id_arr = data[SpecialFields.ROW_ID.name]
        row_ids = row_id_arr.to_pylist()

        if len(row_ids) != len(set(row_ids)):
            raise ValueError("Input data contains duplicate _ROW_ID values")

        if not row_ids:
            return data.append_column(
                self.FIRST_ROW_ID_COLUMN, pa.array([], type=pa.int64()),
            )

        # Vectorised range check (avoids a Python-level per-row loop).
        min_id = pc.min(row_id_arr).as_py()
        max_id = pc.max(row_id_arr).as_py()
        if min_id < 0 or max_id >= self.total_row_count:
            offending = min_id if min_id < 0 else max_id
            raise ValueError(
                f"Row ID {offending} is out of valid range "
                f"[0, {self.total_row_count})"
            )

        if not self.first_row_ids:
            raise ValueError("The input sorted sequence is empty.")

        sorted_seq = self.first_row_ids
        bisect_right = bisect.bisect_right
        first_row_id_values = [
            sorted_seq[bisect_right(sorted_seq, row_id) - 1]
            for row_id in row_ids
        ]
        return data.append_column(
            self.FIRST_ROW_ID_COLUMN,
            pa.array(first_row_id_values, type=pa.int64()),
        )

    def _write_by_first_row_id(self, data: pa.Table, column_names: List[str]):
        """Write data grouped by first_row_id."""
        first_row_id_array = data[self.FIRST_ROW_ID_COLUMN]
        unique_first_row_ids = pc.unique(first_row_id_array).to_pylist()

        for first_row_id in unique_first_row_ids:
            entry = self._first_row_id_index.get(first_row_id)
            if entry is None:
                raise ValueError(f"No existing file found for first_row_id {first_row_id}")
            split, _files = entry

            group_data = data.filter(pc.equal(first_row_id_array, first_row_id))
            self._write_group(split.partition, first_row_id, group_data, column_names)

    def _read_original_file_data(self, first_row_id: int, column_names: List[str]) -> Optional[pa.Table]:
        """Read original file data for the given first_row_id.

        Only reads columns that exist in the original file and need to be updated.
        In Data Evolution mode, uses the table's read API to get the latest data
        for the specified columns, which handles merging multiple files correctly.

        Args:
            first_row_id: The first_row_id of the file to read
            column_names: The column names to update

        Returns:
            PyArrow Table containing the original data for columns that exist in the file,
            or None if no columns need to be read from the original file.
        """
        wanted = set(column_names)
        read_fields: List[DataField] = [
            field for field in self.table.fields if field.name in wanted
        ]
        if not read_fields:
            return None

        entry = self._first_row_id_index.get(first_row_id)
        if entry is None:
            raise ValueError(f"No file found for first_row_id {first_row_id}")
        owning_split, target_files = entry

        origin_split = DataSplit(
            files=target_files,
            partition=owning_split.partition,
            bucket=owning_split.bucket,
            raw_convertible=True,
        )
        table_read = TableRead(self.table, predicate=None, read_type=read_fields)
        return table_read.to_arrow([origin_split])

    def _merge_update_with_original(self, original_data: Optional[pa.Table], update_data: pa.Table,
                                    column_names: List[str], first_row_id: int) -> pa.Table:
        """Merge update data with original data, preserving row order.

        For rows that have updates, use the update values.
        For rows without updates, use the original values (if available).

        Args:
            original_data: Original data from the file (may be None if no columns need to be read)
            update_data: Update data (may contain only partial rows)
            column_names: Column names being updated
            first_row_id: The first_row_id of this file group

        Returns:
            Merged PyArrow Table with all rows
        """

        # Get the _ROW_ID values from update_data to determine which rows are being updated
        relative_indices = pc.subtract(
            update_data[SpecialFields.ROW_ID.name],
            pa.scalar(first_row_id, type=pa.int64())
        ).cast(pa.int64())

        # Build a boolean mask: True at positions that need to be updated
        all_indices = pa.array(range(original_data.num_rows), type=pa.int64())
        mask = pc.is_in(all_indices, relative_indices)

        # Build the merged table column by column
        merged_columns = {}
        for col_name in column_names:
            update_col = update_data[col_name].combine_chunks()
            original_col = original_data[col_name].combine_chunks()
            # replace_with_mask fills mask=True positions with update values in order
            merged_columns[col_name] = pc.replace_with_mask(
                original_col, mask, update_col.cast(original_col.type)
            )

        # Create the merged table
        merged_table = pa.table(merged_columns)

        return merged_table

    def _write_group(self, partition: GenericRow, first_row_id: int,
                     data: pa.Table, column_names: List[str]):
        """Write a group of data with the same first_row_id.

        Reads the original file data, merges in the update values, and
        writes a single output file (rolling disabled) for the group.
        """
        original_data = self._read_original_file_data(first_row_id, column_names)
        merged_data = self._merge_update_with_original(
            original_data, data, column_names, first_row_id,
        )

        file_store_write = FileStoreWrite(self.table, self.commit_user)
        try:
            file_store_write.disable_rolling()
            file_store_write.write_cols = column_names

            partition_tuple = tuple(partition.values)
            for batch in merged_data.to_batches():
                file_store_write.write(partition_tuple, 0, batch)

            new_messages = file_store_write.prepare_commit(self.commit_identifier)
            for msg in new_messages:
                msg.check_from_snapshot = self.snapshot_id
                for file in msg.new_files:
                    file.first_row_id = first_row_id
                    file.write_cols = column_names
            self.commit_messages.extend(new_messages)
        finally:
            file_store_write.close()
