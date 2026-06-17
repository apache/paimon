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

from collections import defaultdict
from typing import Dict, List, Set

import pyarrow as pa

from pypaimon.common.predicate import Predicate
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.split import DataSplit
from pypaimon.table.special_fields import SpecialFields
from pypaimon.utils.range import Range
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.file_store_write import FileStoreWrite
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId


class TableDeleteByFilter:
    """Delete rows from a data-evolution table by rewriting affected files."""

    def __init__(self, table, commit_user: str, commit_identifier: int):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.commit_user = commit_user
        self.commit_identifier = commit_identifier
        self._row_id_update = TableUpdateByRowId(
            table, commit_user, commit_identifier)
        self.snapshot_id = self._row_id_update.snapshot_id
        self._first_row_id_index = self._row_id_update._first_row_id_index
        self.valid_row_id_ranges = self._row_id_update.valid_row_id_ranges

    def delete_by_filter(self, predicate: Predicate) -> List[CommitMessage]:
        if predicate is None:
            raise ValueError("predicate cannot be None")
        if not self.table.options.row_tracking_enabled():
            raise ValueError(
                "delete_by_filter requires 'row-tracking.enabled=true'.")
        if not self.table.options.data_evolution_enabled():
            raise ValueError(
                "delete_by_filter requires 'data-evolution.enabled=true'.")
        if self.table.is_primary_key_table:
            raise NotImplementedError(
                "delete_by_filter is only supported for append-only "
                "data-evolution tables.")
        if self._has_dedicated_files():
            raise NotImplementedError(
                "delete_by_filter is not yet supported for tables with "
                "dedicated blob or vector files.")

        self._validate_row_id_predicate(predicate)
        row_ids = self._matched_row_ids(predicate)
        if not row_ids:
            return []
        self._validate_row_ids_exist(row_ids)
        return self._rewrite_affected_files(row_ids)

    def _validate_row_id_predicate(self, predicate: Predicate) -> None:
        requested = self._explicit_row_id_ranges(predicate)
        if requested:
            self._validate_row_id_ranges_exist(requested)

    def _explicit_row_id_ranges(self, predicate: Predicate) -> List[Range]:
        requested = []
        if predicate.method in ('and', 'or'):
            for child in predicate.literals or []:
                requested.extend(self._explicit_row_id_ranges(child))
            return requested

        ranges = self._row_id_ranges_from_leaf(predicate)
        return ranges if ranges is not None else []

    def _row_id_ranges_from_leaf(self, predicate: Predicate):
        if predicate.field != SpecialFields.ROW_ID.name:
            return None
        if predicate.method == 'equal':
            if not predicate.literals:
                return []
            value = int(predicate.literals[0])
            return [Range(value, value)]
        if predicate.method == 'in':
            if not predicate.literals:
                return []
            return Range.to_ranges([int(v) for v in predicate.literals])
        if predicate.method == 'between':
            if not predicate.literals or len(predicate.literals) < 2:
                return []
            return [Range(int(predicate.literals[0]), int(predicate.literals[1]))]
        return None

    def _validate_row_id_ranges_exist(self, row_id_ranges: List[Range]) -> None:
        for row_id_range in row_id_ranges:
            uncovered = row_id_range.exclude(self.valid_row_id_ranges)
            if uncovered:
                if row_id_range.from_ == row_id_range.to:
                    raise ValueError(
                        f"Row ID {row_id_range.from_} does not belong to "
                        f"any valid range "
                        f"{[f'[{r.from_}, {r.to}]' for r in self.valid_row_id_ranges]}"
                    )
                raise ValueError(
                    f"Row ID range [{row_id_range.from_}, {row_id_range.to}] "
                    f"does not belong to valid ranges "
                    f"{[f'[{r.from_}, {r.to}]' for r in self.valid_row_id_ranges]}"
                )

    def _matched_row_ids(self, predicate: Predicate) -> List[int]:
        projection = list(self._predicate_fields(predicate))
        unknown_fields = [
            field for field in projection
            if field not in self.table.field_names
            and field != SpecialFields.ROW_ID.name
        ]
        if unknown_fields:
            raise ValueError(
                f"Predicate references unknown fields: {unknown_fields}")
        if SpecialFields.ROW_ID.name not in projection:
            projection.append(SpecialFields.ROW_ID.name)
        read_builder = (
            self.table.new_read_builder()
            .with_projection(projection)
            .with_filter(predicate)
        )
        table = read_builder.new_read().to_arrow(
            read_builder.new_scan().plan().splits())
        if table is None or table.num_rows == 0:
            return []
        row_ids = table[SpecialFields.ROW_ID.name].to_pylist()
        return sorted(set(row_ids))

    def _predicate_fields(self, predicate: Predicate) -> Set[str]:
        if predicate.field is not None:
            return {predicate.field}
        fields = set()
        for child in predicate.literals or []:
            fields.update(self._predicate_fields(child))
        return fields

    def _validate_row_ids_exist(self, row_ids: List[int]) -> None:
        if len(row_ids) != len(set(row_ids)):
            raise ValueError("Input data contains duplicate _ROW_ID values")
        for row_id in row_ids:
            if not any(r.contains(row_id) for r in self.valid_row_id_ranges):
                raise ValueError(
                    f"Row ID {row_id} does not belong to any valid range "
                    f"{[f'[{r.from_}, {r.to}]' for r in self.valid_row_id_ranges]}"
                )

    def _rewrite_affected_files(self, row_ids: List[int]) -> List[CommitMessage]:
        row_ids_by_first: Dict[int, Set[int]] = defaultdict(set)
        for row_id in row_ids:
            first_row_id = self._first_row_id_for(row_id)
            row_ids_by_first[first_row_id].add(row_id)

        commit_messages: List[CommitMessage] = []
        for first_row_id, delete_row_ids in row_ids_by_first.items():
            split, files = self._first_row_id_index[first_row_id]
            data_files = [
                file for file in files
                if not DataFileMeta.is_blob_file(file.file_name)
                and not DataFileMeta.is_vector_file(file.file_name)
            ]
            if not data_files:
                raise ValueError(
                    f"No data files found for first_row_id {first_row_id}.")

            old_range = self._merged_data_range(data_files)
            if not all(old_range.contains(row_id) for row_id in delete_row_ids):
                raise ValueError(
                    f"Row IDs {sorted(delete_row_ids)} do not belong to "
                    f"file range [{old_range.from_}, {old_range.to}].")

            original_data = self._read_file_data(split, data_files)
            new_files = self._write_remaining_segments(
                split, old_range, original_data, delete_row_ids)
            commit_messages.append(CommitMessage(
                partition=tuple(split.partition.values),
                bucket=split.bucket,
                new_files=new_files,
                check_from_snapshot=self.snapshot_id,
                deleted_files=data_files,
            ))

        return commit_messages

    def _first_row_id_for(self, row_id: int) -> int:
        for first_row_id, (split, files) in self._first_row_id_index.items():
            for file in files:
                if DataFileMeta.is_blob_file(file.file_name):
                    continue
                file_range = file.row_id_range()
                if file_range is not None and file_range.contains(row_id):
                    return first_row_id
        raise ValueError(f"Row ID {row_id} does not belong to any valid file")

    @staticmethod
    def _merged_data_range(files: List[DataFileMeta]) -> Range:
        ranges = Range.sort_and_merge_overlap(
            [file.row_id_range() for file in files], True, True)
        if len(ranges) != 1:
            raise NotImplementedError(
                "delete_by_filter requires affected data files to cover one "
                f"continuous logical range, got {ranges}.")
        return ranges[0]

    def _read_file_data(self, split: DataSplit, files: List[DataFileMeta]) -> pa.Table:
        origin_split = DataSplit(
            files=files,
            partition=split.partition,
            bucket=split.bucket,
            raw_convertible=len(files) == 1,
        )
        return self.table.new_read_builder().new_read().to_arrow([origin_split])

    def _write_remaining_segments(
            self,
            split: DataSplit,
            old_range: Range,
            original_data: pa.Table,
            delete_row_ids: Set[int],
    ) -> List[DataFileMeta]:
        remaining_ranges = old_range.exclude(Range.to_ranges(list(delete_row_ids)))
        if not remaining_ranges:
            return []

        new_files: List[DataFileMeta] = []
        for remaining in remaining_ranges:
            offset = remaining.from_ - old_range.from_
            segment = original_data.slice(offset, remaining.count())
            new_files.extend(self._write_segment(split, segment, remaining.from_))
        return new_files

    def _write_segment(
            self, split: DataSplit, segment: pa.Table, first_row_id: int
    ) -> List[DataFileMeta]:
        file_store_write = FileStoreWrite(self.table, self.commit_user)
        file_store_write.disable_rolling()
        partition_tuple = tuple(split.partition.values)
        try:
            for batch in segment.to_batches():
                file_store_write.write(partition_tuple, split.bucket, batch)
            messages = file_store_write.prepare_commit(self.commit_identifier)
            new_files = [file for msg in messages for file in msg.new_files]
            for file in new_files:
                file.first_row_id = first_row_id
            return new_files
        finally:
            file_store_write.close()

    def _has_dedicated_files(self) -> bool:
        for field in self.table.fields:
            if getattr(field.type, 'type', None) == 'BLOB':
                return True
        for split, files in self._first_row_id_index.values():
            for file in files:
                if (DataFileMeta.is_blob_file(file.file_name)
                        or DataFileMeta.is_vector_file(file.file_name)):
                    return True
        return False
