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

"""Scan raw rows whose row ids are not covered by a global index query."""

from typing import Callable, List

from pypaimon.common.options.core_options import (
    CoreOptions,
    GlobalIndexSearchMode,
    StartupMode,
)
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.read.scanner.data_evolution_split_generator import (
    DataEvolutionSplitGenerator,
)
from pypaimon.table.special_fields import SpecialFields
from pypaimon.utils.range import Range
from pypaimon.utils.roaring_bitmap import RoaringBitmap64


class GlobalIndexUnindexedRowsScanner:

    def __init__(
        self,
        table,
        snapshot,
        manifest_files,
        partition_filter,
        predicate,
        target_split_size,
        open_file_cost,
        read_manifest_entries: Callable,
        deletion_files_map: Callable,
    ):
        self._table = table
        self._snapshot = snapshot
        self._manifest_files = manifest_files
        self._partition_filter = partition_filter
        self._predicate = predicate
        self._target_split_size = target_split_size
        self._open_file_cost = open_file_cost
        self._read_manifest_entries = read_manifest_entries
        self._deletion_files_map = deletion_files_map

    def with_unindexed_rows(self, indexed_result):
        entries = None
        data_ranges = []
        mode = self._table.options.global_index_search_mode()

        if mode == GlobalIndexSearchMode.DETAIL:
            entries = self._read_manifest_entries(self._manifest_files)
            for entry in entries:
                first_row_id = entry.file.first_row_id
                if first_row_id is not None:
                    data_ranges.append(entry.file.row_id_range())
        elif (self._snapshot is not None
              and self._snapshot.next_row_id is not None
              and self._snapshot.next_row_id > 0):
            data_ranges = [Range(0, int(self._snapshot.next_row_id) - 1)]

        unindexed_ranges = self._unindexed_ranges(data_ranges)

        if entries is None and unindexed_ranges:
            entries = self._read_manifest_entries(self._manifest_files)
        if entries is None:
            entries = []

        bitmap = RoaringBitmap64.or_(
            indexed_result.results(),
            self._matching_rows(entries, unindexed_ranges),
        )
        return GlobalIndexResult.create(bitmap)

    def _unindexed_ranges(self, data_ranges):
        if (self._snapshot is None
                or self._snapshot.next_row_id is None
                or self._snapshot.next_row_id <= 0):
            return []

        from pypaimon.globalindex.global_index_scanner import GlobalIndexScanner

        predicate_indexed_ranges = Range.sort_and_merge_overlap(
            GlobalIndexScanner.predicate_indexed_ranges(
                self._table,
                self._partition_filter,
                self._predicate,
                self._snapshot,
            ),
            merge=True,
            adjacent=True,
        )

        unindexed_ranges = []
        for data_range in Range.sort_and_merge_overlap(
                data_ranges, merge=True, adjacent=True):
            unindexed_ranges.extend(data_range.exclude(predicate_indexed_ranges))
        return Range.sort_and_merge_overlap(
            unindexed_ranges, merge=True, adjacent=True)

    def _matching_rows(self, entries, row_ranges):
        rows = RoaringBitmap64()
        if not row_ranges:
            return rows

        entries = _filter_manifest_entries_by_row_ranges(entries, row_ranges)
        if not entries:
            return rows

        split_generator = DataEvolutionSplitGenerator(
            self._table,
            self._target_split_size,
            self._open_file_cost,
            self._deletion_files_map(entries),
            row_ranges,
        )
        splits = split_generator.create_splits(entries)
        read_type = SpecialFields.row_type_with_row_id(self._table.fields)
        row_id_index = len(read_type) - 1
        reader = self._table.copy(self._snapshot_read_options()) \
            .new_read_builder().with_read_type(read_type) \
            .with_filter(self._predicate).new_read()
        for row in reader.to_iterator(splits):
            rows.add(int(row.get_field(row_id_index)))
        return rows

    def _snapshot_read_options(self):
        options = {
            CoreOptions.SCAN_MODE.key(): StartupMode.FROM_SNAPSHOT.value,
            CoreOptions.SCAN_SNAPSHOT_ID.key(): str(self._snapshot.id),
        }
        current_options = self._table.options.options.to_map()
        for option in (
            CoreOptions.SCAN_TAG_NAME,
            CoreOptions.SCAN_WATERMARK,
            CoreOptions.SCAN_TIMESTAMP,
            CoreOptions.SCAN_TIMESTAMP_MILLIS,
            CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS,
            CoreOptions.SCAN_CREATION_TIME_MILLIS,
        ):
            if option.key() in current_options:
                options[option.key()] = None
        return options


def _filter_manifest_entries_by_row_ranges(entries: List, row_ranges: List) -> List:
    filtered_entries = []
    for entry in entries:
        file = entry.file
        first_row_id = file.first_row_id
        if first_row_id is None:
            filtered_entries.append(entry)
            continue
        file_range = file.row_id_range()
        if any(file_range.overlaps(row_range) for row_range in row_ranges):
            filtered_entries.append(entry)
    return filtered_entries
