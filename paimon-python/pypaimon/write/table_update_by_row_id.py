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
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.read.scanner.data_evolution_split_generator import (
    DataEvolutionSplitGenerator,
)
from pypaimon.read.split import DataSplit
from pypaimon.read.table_read import TableRead
from pypaimon.schema.data_types import (
    DataField,
    PyarrowFieldParser,
    is_array_blob_type,
    is_blob_file_field,
    is_map_blob_type,
)
from pypaimon.table.row.blob import Blob
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.special_fields import SpecialFields
from pypaimon.utils.range import Range
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.file_store_write import FileStoreWrite
from pypaimon.write.row_utils import (
    require_columns,
    row_to_named_values,
    value_for_arrow,
)
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

    Python is the writer-side source of truth for update-by-row-id. The Java
    side ships the read path (``BlobFallbackRecordReader``) and pins the
    on-disk blob delta layout via ``BlobUpdateTest`` only; there is no Java
    writer. Changes to the blob delta layout here must stay compatible with
    that reader.
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
        """Return the already loaded snapshot file index for broadcast."""
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
        scan = self.table.new_read_builder().new_scan()
        plan = scan.plan_for_write()
        snapshot_id = plan.snapshot_id if plan.snapshot_id is not None else -1
        return self._files_info_from_splits(snapshot_id, plan.splits())

    @classmethod
    def _files_info_from_entries(
            cls,
            table,
            snapshot_id: int,
            entries: List[ManifestEntry],
    ) -> _FilesInfo:
        """Build a file index from an already resolved snapshot entry set."""
        splits = DataEvolutionSplitGenerator(
            table,
            table.options.source_split_target_size(),
            table.options.source_split_open_file_cost(),
        ).create_splits(entries)
        return cls._files_info_from_splits(snapshot_id, splits)

    @classmethod
    def _files_info_from_splits(
            cls, snapshot_id: int, splits: List[DataSplit]) -> _FilesInfo:
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
                if (
                        file.first_row_id is None
                        or DataFileMeta.is_blob_file(file.file_name)
                ):
                    continue
                row_id_ranges.append(file.row_id_range())
            for file in data_files:
                target_files = [
                    target_file
                    for target_file in files_with_row_id
                    if cls._overlaps(
                        file.row_id_range(), target_file.row_id_range()
                    )
                ]

                entry = index.get(file.first_row_id)
                if entry is None:
                    index[file.first_row_id] = (split, target_files)
                else:
                    existing_files = entry[1]
                    existing_names = {
                        existing.file_name for existing in existing_files
                    }
                    existing_files.extend(
                        target_file
                        for target_file in target_files
                        if target_file.file_name not in existing_names
                    )

        if row_id_ranges:
            merged = Range.sort_and_merge_overlap(row_id_ranges, True, True)
        else:
            merged = []

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

        data_with_first_row_id = self._calculate_first_row_id(data)
        self._write_by_first_row_id(data_with_first_row_id, column_names)

        return self.commit_messages

    def update_row_columns(
            self,
            row,
            row_ids: List[int],
            column_names: List[str],
    ) -> List[CommitMessage]:
        return self.update_rows_columns([row], [row_ids], column_names)

    def update_rows_columns(
            self,
            rows: List,
            row_ids_by_row: List[List[int]],
            column_names: List[str],
    ) -> List[CommitMessage]:
        if not column_names:
            raise ValueError("column_names cannot be empty")
        if len(rows) != len(row_ids_by_row):
            raise ValueError(
                "rows and row_ids_by_row must have the same length: "
                f"{len(rows)} != {len(row_ids_by_row)}"
            )

        values_by_row = [
            row_to_named_values(row, self.table.table_schema.fields)
            for row in rows
        ]
        for values_by_name in values_by_row:
            require_columns(values_by_name, column_names, "update_rows_columns")

        row_entries = []
        for values_by_name, row_ids in zip(values_by_row, row_ids_by_row):
            row_entries.extend((row_id, values_by_name) for row_id in row_ids)

        if not row_entries:
            return []

        row_entries.sort(key=lambda item: item[0])

        for col_name in column_names:
            if col_name not in self.table.field_names:
                raise ValueError(f"Column {col_name} not found in table schema")

        arrays = [
            pa.array([row_id for row_id, _ in row_entries], type=pa.int64())
        ]
        fields = [pa.field(SpecialFields.ROW_ID.name, pa.int64())]
        blob_object_columns: Dict[str, List[Any]] = {}

        for col_name in column_names:
            if self._is_blob_column(col_name):
                blob_object_columns[col_name] = [
                    values_by_name[col_name]
                    for _, values_by_name in row_entries
                ]
                continue

            table_field = self.table.field_dict[col_name]
            arrow_field = PyarrowFieldParser.from_paimon_field(table_field)
            arrays.append(
                pa.array(
                    [
                        value_for_arrow(values_by_name[col_name], table_field)
                        for _, values_by_name in row_entries
                    ],
                    type=arrow_field.type,
                )
            )
            fields.append(arrow_field)

        update_data = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
        data_with_first_row_id = self._calculate_first_row_id(update_data)
        self._write_by_first_row_id(
            data_with_first_row_id, column_names, blob_object_columns)

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

    def _write_by_first_row_id(
            self,
            data: pa.Table,
            column_names: List[str],
            blob_object_columns: Optional[Dict[str, List[Any]]] = None):
        """Write data grouped by first_row_id."""
        first_row_id_array = data[self.FIRST_ROW_ID_COLUMN]
        unique_first_row_ids = pc.unique(first_row_id_array).to_pylist()
        first_row_id_values = first_row_id_array.to_pylist()

        for first_row_id in unique_first_row_ids:
            entry = self._first_row_id_index.get(first_row_id)
            if entry is None:
                raise ValueError(f"No existing file found for first_row_id {first_row_id}")
            split, _files = entry

            group_data = data.filter(pc.equal(first_row_id_array, first_row_id))
            group_blob_object_columns = None
            if blob_object_columns:
                group_indices = [
                    i for i, value in enumerate(first_row_id_values)
                    if value == first_row_id
                ]
                group_blob_object_columns = {
                    col_name: [values[i] for i in group_indices]
                    for col_name, values in blob_object_columns.items()
                }
            self._write_group(
                split.partition,
                first_row_id,
                group_data,
                column_names,
                group_blob_object_columns,
            )

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
        # Keep _ROW_ID as a row-count anchor. If every requested column was
        # added after the original file was written, reading only those
        # missing columns can otherwise produce a zero-row table instead of
        # one null value per original row.
        table_read = TableRead(
            self.table,
            predicate=None,
            read_type=read_fields + [SpecialFields.ROW_ID],
        )
        original = table_read.to_arrow([origin_split])
        return original.select([field.name for field in read_fields])

    def _merge_update_with_original(
            self,
            original_data: Optional[pa.Table],
            update_data: pa.Table,
            column_names: List[str],
            first_row_id: int,
            blob_object_columns: Optional[Dict[str, List[Any]]] = None,
    ) -> Tuple[Optional[pa.Table], Dict[str, List[object]]]:
        """Merge update data with original data, preserving row order.

        For rows that have updates, use the update values.
        For rows without updates, use the original values (if available).

        Blob delta files cover ``[first_row_id, first_row_id + max_updated_pos]``
        — anchored at the original file's first_row_id, spanning up to and
        including the last updated row. The span is NOT shrunk at the head:
        ``BlobFallbackRecordReader`` resolves placeholders by relative offset
        from the delta file's ``first_row_id``, so anchoring anywhere other
        than the original ``first_row_id`` would misalign unchanged rows
        before ``min_updated_pos`` with the older blob file. This anchor is
        the same in every blob column being updated.

        Args:
            original_data: Original data from the file (may be None if no columns need to be read)
            update_data: Update data (may contain only partial rows)
            column_names: Column names being updated
            first_row_id: The first_row_id of this file group

        Returns:
            Normal merged PyArrow Table, and per-blob-column values list. All
            blob value lists have the same length (= ``max_updated_pos + 1``).
        """

        # Get the _ROW_ID values from update_data to determine which rows are being updated
        relative_indices = pc.subtract(
            update_data[SpecialFields.ROW_ID.name],
            pa.scalar(first_row_id, type=pa.int64())
        ).cast(pa.int64())

        # Build the merged table column by column
        merged_columns = {}
        blob_columns: Dict[str, List[object]] = {}
        update_by_col = {
            col_name: update_data[col_name]
            for col_name in column_names
            if col_name in update_data.column_names
        }
        update_positions = {
            int(relative_index.as_py()): idx
            for idx, relative_index in enumerate(relative_indices)
        }
        sorted_updates = None
        # Caller (_write_by_first_row_id) only enters this method with a
        # non-empty group, so update_positions is non-empty here.
        blob_row_count = max(update_positions) + 1
        for col_name in column_names:
            if self._is_blob_column(col_name):
                if blob_object_columns and col_name in blob_object_columns:
                    update_values = blob_object_columns[col_name]
                    placeholder = self._blob_placeholder(col_name)
                    blob_columns[col_name] = [
                        update_values[update_positions[i]]
                        if i in update_positions
                        else placeholder
                        for i in range(blob_row_count)
                    ]
                    continue
                update_col = update_by_col[col_name]
                placeholder = self._blob_placeholder(col_name)
                blob_columns[col_name] = [
                    update_col[update_positions[i]].as_py()
                    if i in update_positions
                    else placeholder
                    for i in range(blob_row_count)
                ]
                continue
            update_col = update_by_col[col_name]
            original_col = original_data[col_name]
            if sorted_updates is None:
                sorted_updates = sorted(update_positions.items())
                row_count = len(original_col)
                for position, _ in sorted_updates:
                    if position < 0 or position >= row_count:
                        raise IndexError(
                            f"Update position {position} is outside column "
                            f"range [0, {row_count})")
            merged_columns[col_name] = self._merge_chunked_column(
                original_col, update_col, sorted_updates)

        merged_table = pa.table(merged_columns) if merged_columns else None

        return merged_table, blob_columns

    @classmethod
    def _merge_chunked_column(
            cls,
            original_col: pa.ChunkedArray,
            update_col: pa.ChunkedArray,
            sorted_updates: List[Tuple[int, int]],
    ) -> pa.ChunkedArray:
        """Merge updates without flattening a column into one Arrow Array.

        ``binary``, ``string`` and ``list`` use signed 32-bit offsets, so a
        valid multi-chunk column can exceed 2 GiB while each individual Array
        remains below the limit. Keep those chunks independent. If merging an
        individual chunk still overflows (for example, the struct/list
        fallback temporarily concatenates original and replacement values),
        split that row range and retry.
        """
        update_chunk_offsets = cls._chunk_offsets(update_col)
        sorted_updates_idx = 0
        chunk_start_row = 0
        merged_chunks: List[pa.Array] = []

        for original_chunk in original_col.chunks:
            chunk_end = chunk_start_row + len(original_chunk)
            chunk_updates: List[Tuple[int, int]] = []
            while (
                    sorted_updates_idx < len(sorted_updates)
                    and sorted_updates[sorted_updates_idx][0] < chunk_end
            ):
                position, update_index = sorted_updates[sorted_updates_idx]
                chunk_updates.append((
                    position - chunk_start_row, update_index))
                sorted_updates_idx += 1

            merged_chunks.extend(cls._merge_chunk_with_updates(
                original_chunk,
                update_col,
                update_chunk_offsets,
                chunk_updates,
            ))
            chunk_start_row = chunk_end

        return pa.chunked_array(merged_chunks, type=original_col.type)

    @classmethod
    def _merge_chunk_with_updates(
            cls,
            original: pa.Array,
            update_col: pa.ChunkedArray,
            update_chunk_offsets: List[int],
            updates: List[Tuple[int, int]],
    ) -> List[pa.Array]:
        if not updates:
            return [original]

        try:
            replacements = cls._take_from_chunked_array(
                update_col,
                update_chunk_offsets,
                [update_index for _, update_index in updates],
            )
            if replacements.type != original.type:
                replacements = cls._coerce_column(
                    replacements, original.type)

            if len(updates) == len(original):
                return [replacements]

            mask_values = np.zeros(len(original), dtype=np.bool_)
            for position, _ in updates:
                mask_values[position] = True
            mask = pa.array(mask_values)

            try:
                return [pc.replace_with_mask(original, mask, replacements)]
            except pa.lib.ArrowNotImplementedError:
                combined = pa.concat_arrays([original, replacements])
                indices = np.arange(len(original), dtype=np.int64)
                replacement_offset = len(original)
                for replacement_index, (position, _) in enumerate(updates):
                    indices[position] = replacement_offset + replacement_index
                return [combined.take(pa.array(indices))]
        except (pa.lib.ArrowInvalid,
                pa.lib.ArrowCapacityError) as error:
            if not cls._is_offset_overflow(error) or len(original) <= 1:
                raise

            split_at = len(original) // 2
            left_updates = [
                update for update in updates if update[0] < split_at
            ]
            right_updates = [
                (position - split_at, update_index)
                for position, update_index in updates
                if position >= split_at
            ]
            return (
                cls._merge_chunk_with_updates(
                    original.slice(0, split_at),
                    update_col,
                    update_chunk_offsets,
                    left_updates,
                )
                + cls._merge_chunk_with_updates(
                    original.slice(split_at),
                    update_col,
                    update_chunk_offsets,
                    right_updates,
                )
            )

    @staticmethod
    def _chunk_offsets(column: pa.ChunkedArray) -> List[int]:
        offsets = [0]
        for chunk in column.chunks:
            offsets.append(offsets[-1] + len(chunk))
        return offsets

    @staticmethod
    def _take_from_chunked_array(
            column: pa.ChunkedArray,
            chunk_offsets: List[int],
            indices: List[int],
    ) -> pa.Array:
        """Take values without asking Arrow to combine unrelated chunks."""
        if not indices:
            return pa.array([], type=column.type)

        pieces: List[pa.Array] = []
        current_chunk_index = None
        current_local_indices: List[int] = []

        def append_piece(chunk_index: int, local_indices: List[int]):
            chunk = column.chunk(chunk_index)
            start = local_indices[0]
            if all(value == start + offset
                   for offset, value in enumerate(local_indices)):
                pieces.append(chunk.slice(start, len(local_indices)))
            else:
                pieces.append(chunk.take(pa.array(
                    local_indices, type=pa.int64())))

        for index in indices:
            if index < 0 or index >= len(column):
                raise IndexError(
                    f"Update index {index} is outside column range "
                    f"[0, {len(column)})")
            chunk_index = bisect.bisect_right(chunk_offsets, index) - 1
            local_index = index - chunk_offsets[chunk_index]
            if current_chunk_index is None:
                current_chunk_index = chunk_index
            elif chunk_index != current_chunk_index:
                append_piece(current_chunk_index, current_local_indices)
                current_chunk_index = chunk_index
                current_local_indices = []
            current_local_indices.append(local_index)

        append_piece(current_chunk_index, current_local_indices)
        if len(pieces) == 1:
            return pieces[0]
        return pa.concat_arrays(pieces)

    @staticmethod
    def _is_offset_overflow(error: pa.lib.ArrowException) -> bool:
        if isinstance(error, pa.lib.ArrowCapacityError):
            return True
        message = str(error).lower()
        return (
            "offset overflow" in message
            or "too large to convert" in message
        )

    @staticmethod
    def _coerce_column(col: pa.Array, target_type: pa.DataType) -> pa.Array:
        try:
            return col.cast(target_type)
        except pa.lib.ArrowInvalid as error:
            if TableUpdateByRowId._is_offset_overflow(error):
                raise
        except (pa.lib.ArrowNotImplementedError,
                pa.lib.ArrowTypeError):
            pass
        pylist = col.to_pylist()
        if pa.types.is_map(target_type):
            converted = []
            for row in pylist:
                if row is None:
                    converted.append(None)
                elif isinstance(row, dict):
                    if pa.types.is_struct(col.type) and any(
                            v is None for v in row.values()):
                        raise ValueError(
                            "Cannot coerce schema-less dict input with null "
                            "values to map type. PyArrow represents both "
                            "missing dict keys and explicit null map values "
                            "as None; pass an explicit map-typed array or "
                            "list-of-pairs instead.")
                    converted.append(list(row.items()))
                else:
                    converted.append(
                        [tuple(pair) for pair in row])
            pylist = converted
        return pa.array(pylist, type=target_type)

    def _is_blob_column(self, column_name: str) -> bool:
        for table_field in self.table.fields:
            if table_field.name == column_name:
                return is_blob_file_field(table_field)
        return False

    def _blob_placeholder(self, column_name: str):
        for table_field in self.table.fields:
            if table_field.name == column_name:
                if is_map_blob_type(table_field.type):
                    return Blob.MAP_PLACE_HOLDER
                return (
                    Blob.ARRAY_PLACE_HOLDER
                    if is_array_blob_type(table_field.type)
                    else Blob.PLACE_HOLDER
                )
        return Blob.PLACE_HOLDER

    def _write_group(
            self,
            partition: GenericRow,
            first_row_id: int,
            data: pa.Table,
            column_names: List[str],
            blob_object_columns: Optional[Dict[str, List[Any]]] = None):
        """Write a group of data with the same first_row_id.

        Reads the original file data, merges in the update values, and
        writes a single output file (rolling disabled) for the group.
        """
        original_data = self._read_original_file_data(first_row_id, column_names)
        merged_data, blob_columns = self._merge_update_with_original(
            original_data,
            data,
            column_names,
            first_row_id,
            blob_object_columns,
        )

        partition_tuple = tuple(partition.values)
        new_files = []
        file_store_write = None
        blob_writers = []
        success = False
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
                self._assign_update_file_metadata(
                    new_files, first_row_id, column_names, blob_columns)
                self.commit_messages.append(
                    CommitMessage(
                        partition=partition_tuple,
                        bucket=0,
                        new_files=new_files,
                        check_from_snapshot=self.snapshot_id,
                    )
                )
            success = True
        finally:
            if success:
                if file_store_write is not None:
                    file_store_write.close()
                for blob_writer in blob_writers:
                    blob_writer.close()
            else:
                if file_store_write is not None:
                    file_store_write.abort()
                for blob_writer in blob_writers:
                    blob_writer.abort()

    @staticmethod
    def _assign_update_file_metadata(new_files: List[DataFileMeta], first_row_id: int,
                                     column_names: List[str],
                                     blob_columns: Dict[str, List[object]]):
        # All blob columns share the same anchored span (see
        # _merge_update_with_original docstring), so any column's length is
        # the per-blob delta-file row count.
        blob_row_count = (
            len(next(iter(blob_columns.values()))) if blob_columns else 0
        )
        blob_end = first_row_id + blob_row_count
        blob_starts = {}
        # BlobWriter.prepare_commit preserves write/rolling order, which is required
        # for assigning continuous row-id ranges to rolled blob files.
        for file in new_files:
            file.write_cols = file.write_cols or column_names
            if DataFileMeta.is_blob_file(file.file_name):
                if len(file.write_cols) != 1:
                    raise RuntimeError(
                        f"Blob update file {file.file_name} should contain "
                        f"exactly one write column, got {file.write_cols}")
                blob_column = file.write_cols[0]
                blob_start = blob_starts.get(blob_column, first_row_id)
                next_blob_start = blob_start + file.row_count
                if next_blob_start > blob_end:
                    raise RuntimeError(
                        f"Blob update file {file.file_name} row-id range "
                        f"[{blob_start}, {next_blob_start - 1}] exceeds target range "
                        f"[{first_row_id}, {blob_end - 1}]")
                file.first_row_id = blob_start
                # Only update-by-row-id blob delta files use the 0/0 sentinel;
                # regular blob writes keep their per-row sequence range.
                file.min_sequence_number = 0
                file.max_sequence_number = 0
                blob_starts[blob_column] = next_blob_start
            else:
                file.first_row_id = first_row_id

        for blob_column, next_blob_start in blob_starts.items():
            if next_blob_start != blob_end:
                raise RuntimeError(
                    f"Blob update column {blob_column} covers row ids "
                    f"[{first_row_id}, {next_blob_start - 1}], expected "
                    f"[{first_row_id}, {blob_end - 1}]")
