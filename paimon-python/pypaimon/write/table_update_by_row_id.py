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

import bisect
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.split import DataSplit
from pypaimon.read.table_read import TableRead
from pypaimon.schema.data_types import DataField
from pypaimon.table.row.blob import Blob
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.special_fields import SpecialFields
from pypaimon.utils.range import Range
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.file_store_write import FileStoreWrite
from pypaimon.write.writer.blob_writer import BlobWriter


@dataclass(frozen=True)
class _FilesInfo:
    """Snapshot view of target data files keyed by first_row_id.

    Built once per merge by the driver and broadcast to workers so each task
    avoids re-scanning the manifest.
    """
    snapshot_id: int
    first_row_ids: List[int]
    first_row_id_index: Dict[int, Tuple[DataSplit, List[DataFileMeta]]] = (
        field(default_factory=dict)
    )
    valid_row_id_ranges: List[Range] = field(default_factory=list)


class TableUpdateByRowId:
    """
    Table update for partial column updates (data evolution).

    This update is designed for adding/updating specific columns in existing tables.
    Input data should contain _ROW_ID column.
    """

    FIRST_ROW_ID_COLUMN = '_FIRST_ROW_ID'

    def __init__(
            self, table, commit_user: str, commit_identifier: int,
            _precomputed_files_info: Optional[_FilesInfo] = None,
    ):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.commit_user = commit_user
        self.commit_identifier = commit_identifier

        info = _precomputed_files_info or self._load_existing_files_info()
        self.snapshot_id = info.snapshot_id
        self.first_row_ids = info.first_row_ids
        self._first_row_id_index = info.first_row_id_index
        self.valid_row_id_ranges = info.valid_row_id_ranges

        self.commit_messages: List[CommitMessage] = []

    def _snapshot_files_info(self) -> _FilesInfo:
        """Internal: return the current snapshot's file index for broadcast."""
        return _FilesInfo(
            snapshot_id=self.snapshot_id,
            first_row_ids=self.first_row_ids,
            first_row_id_index=self._first_row_id_index,
            valid_row_id_ranges=self.valid_row_id_ranges,
        )

    def _load_existing_files_info(self) -> _FilesInfo:
        """Scan the latest snapshot once and index files by ``first_row_id``.

        Returns a :class:`_FilesInfo` whose ``first_row_id_index`` maps each
        ``first_row_id`` to the owning split and the list of files with that
        id (a single id may belong to multiple files when data evolution has
        split a logical row range).
        """
        plan = self.table.new_read_builder().new_scan().plan()
        splits = plan.splits()

        index: Dict[int, Tuple[DataSplit, List[DataFileMeta]]] = {}
        row_id_ranges: List[Range] = []
        for split in splits:
            files_with_row_id = [
                file for file in split.files if file.first_row_id is not None
            ]
            data_files = [
                file for file in files_with_row_id
                if not DataFileMeta.is_blob_file(file.file_name)
            ]
            for file in split.files:
                if file.first_row_id is None or DataFileMeta.is_blob_file(file.file_name):
                    continue
                row_id_ranges.append(file.row_id_range())
            for file in data_files:
                target_files = [
                    target_file
                    for target_file in files_with_row_id
                    if self._overlaps(file.row_id_range(), target_file.row_id_range())
                ]

                entry = index.get(file.first_row_id)
                if entry is None:
                    index[file.first_row_id] = (split, target_files)
                else:
                    existing_files = entry[1]
                    existing_names = {existing.file_name for existing in existing_files}
                    existing_files.extend(
                        target_file
                        for target_file in target_files
                        if target_file.file_name not in existing_names
                    )

        if row_id_ranges:
            merged = Range.sort_and_merge_overlap(row_id_ranges, True, True)
        else:
            merged = []

        snapshot_id = plan.snapshot_id if plan.snapshot_id is not None else -1
        return _FilesInfo(
            snapshot_id=snapshot_id,
            first_row_ids=sorted(index.keys()),
            first_row_id_index=index,
            valid_row_id_ranges=merged,
        )

    @staticmethod
    def _overlaps(left: Range, right: Range) -> bool:
        return left.from_ <= right.to and right.from_ <= left.to

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

        Validates that every input ``_ROW_ID`` is unique and belongs to
        a valid row_id range. Supports partial / non-consecutive updates.
        """
        row_id_arr = data[SpecialFields.ROW_ID.name]
        row_ids = row_id_arr.to_pylist()

        if len(row_ids) != len(set(row_ids)):
            raise ValueError("Input data contains duplicate _ROW_ID values")

        if not row_ids:
            return data.append_column(
                self.FIRST_ROW_ID_COLUMN, pa.array([], type=pa.int64()),
            )

        for row_id in row_ids:
            if not any(r.contains(row_id) for r in self.valid_row_id_ranges):
                raise ValueError(
                    f"Row ID {row_id} does not belong to any valid range "
                    f"{[f'[{r.from_}, {r.to}]' for r in self.valid_row_id_ranges]}"
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
            table_field for table_field in self.table.fields if table_field.name in wanted
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

    def _merge_update_with_original(
            self,
            original_data: Optional[pa.Table],
            update_data: pa.Table,
            column_names: List[str],
            first_row_id: int) -> Tuple[Optional[pa.Table], Dict[str, List[object]]]:
        """Merge update data with original data, preserving row order.

        For rows that have updates, use the update values.
        For rows without updates, use the original values (if available).

        Args:
            original_data: Original data from the file (may be None if no columns need to be read)
            update_data: Update data (may contain only partial rows)
            column_names: Column names being updated
            first_row_id: The first_row_id of this file group

        Returns:
            Normal merged PyArrow Table and blob values to write row-by-row.
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
        blob_columns = {}
        update_by_col = {
            col_name: update_data[col_name].combine_chunks()
            for col_name in column_names
        }
        update_positions = {
            int(relative_index.as_py()): idx
            for idx, relative_index in enumerate(relative_indices)
        }
        for col_name in column_names:
            update_col = update_by_col[col_name]
            original_col = original_data[col_name].combine_chunks()
            if self._is_blob_column(col_name):
                blob_columns[col_name] = [
                    update_col[update_positions[i]].as_py()
                    if i in update_positions
                    else Blob.PLACE_HOLDER
                    for i in range(original_data.num_rows)
                ]
            else:
                # replace_with_mask fills mask=True positions with update values in order
                merged_columns[col_name] = pc.replace_with_mask(
                    original_col, mask, update_col.cast(original_col.type)
                )

        merged_table = pa.table(merged_columns) if merged_columns else None

        return merged_table, blob_columns

    def _is_blob_column(self, column_name: str) -> bool:
        for table_field in self.table.fields:
            if table_field.name == column_name:
                return getattr(table_field.type, 'type', None) == 'BLOB'
        return False

    def _write_group(self, partition: GenericRow, first_row_id: int,
                     data: pa.Table, column_names: List[str]):
        """Write a group of data with the same first_row_id.

        Reads the original file data, merges in the update values, and
        writes a single output file (rolling disabled) for the group.
        """
        original_data = self._read_original_file_data(first_row_id, column_names)
        merged_data, blob_columns = self._merge_update_with_original(
            original_data, data, column_names, first_row_id,
        )

        partition_tuple = tuple(partition.values)
        new_files = []
        file_store_write = None
        blob_writers = []
        try:
            if merged_data is not None:
                file_store_write = FileStoreWrite(self.table, self.commit_user)
                file_store_write.disable_rolling()
                file_store_write.write_cols = list(merged_data.column_names)
                for batch in merged_data.to_batches():
                    file_store_write.write(partition_tuple, 0, batch)
                new_messages = file_store_write.prepare_commit(self.commit_identifier)
                for msg in new_messages:
                    new_files.extend(msg.new_files)

            for column_name, values in blob_columns.items():
                blob_writer = BlobWriter(
                    self.table,
                    partition_tuple,
                    0,
                    0,
                    column_name,
                    self.table.options,
                )
                blob_writers.append(blob_writer)
                arrow_type = original_data.schema.field(column_name).type
                for value in values:
                    blob_writer.write_blob(value, arrow_type)
                new_files.extend(blob_writer.prepare_commit())

            if new_files:
                for file in new_files:
                    file.first_row_id = first_row_id
                    file.write_cols = file.write_cols or column_names
                self.commit_messages.append(
                    CommitMessage(
                        partition=partition_tuple,
                        bucket=0,
                        new_files=new_files,
                        check_from_snapshot=self.snapshot_id,
                    )
                )
        finally:
            if file_store_write is not None:
                file_store_write.close()
            for blob_writer in blob_writers:
                blob_writer.close()
