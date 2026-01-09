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

"""
Implementation of GlobalIndexScanBuilder for FileStoreTable.
"""

from typing import List

from pypaimon.globalindex.range import Range
from pypaimon.globalindex.global_index_scan_builder import (
    GlobalIndexScanBuilder,
    RowRangeGlobalIndexScanner
)


class GlobalIndexScanBuilderImpl(GlobalIndexScanBuilder):

    def __init__(
        self,
        options: dict,
        row_type: list,
        file_io: 'FileIO',
        index_path_factory: 'IndexPathFactory',
        snapshot_manager: 'SnapshotManager',
        index_file_handler: 'IndexFileHandler'
    ):
        self._options = options
        self._row_type = row_type
        self._file_io = file_io
        self._index_path_factory = index_path_factory
        self._snapshot_manager = snapshot_manager
        self._index_file_handler = index_file_handler

        self._snapshot = None
        self._partition_predicate = None
        self._row_range = None
        self._cached_entries = None

    def with_snapshot(self, snapshot_or_id) -> 'GlobalIndexScanBuilderImpl':
        """Set the snapshot to scan."""
        if isinstance(snapshot_or_id, int):
            self._snapshot = self._snapshot_manager.snapshot(snapshot_or_id)
        else:
            self._snapshot = snapshot_or_id
        return self

    def with_partition_predicate(self, partition_predicate) -> 'GlobalIndexScanBuilderImpl':
        """Set the partition predicate."""
        self._partition_predicate = partition_predicate
        return self

    def with_row_range(self, row_range: Range) -> 'GlobalIndexScanBuilderImpl':
        """Set the row range to scan."""
        self._row_range = row_range
        return self

    def shard_list(self) -> List[Range]:
        entries = self._scan()
        if not entries:
            return []

        # Build ranges from index entries grouped by index type
        index_ranges = {}
        for entry in entries:
            global_index_meta = entry.index_file.global_index_meta
            if global_index_meta is None:
                continue

            index_type = entry.index_file.index_type
            start = global_index_meta.row_range_start
            end = global_index_meta.row_range_end

            if index_type not in index_ranges:
                index_ranges[index_type] = []
            index_ranges[index_type].append(Range(start, end))

        if not index_ranges:
            return []

        # Merge all ranges
        all_ranges = []
        for ranges in index_ranges.values():
            all_ranges.extend(ranges)

        return Range.sort_and_merge_overlap(all_ranges)

    def build(self) -> RowRangeGlobalIndexScanner:
        """Build the scanner for the current row range."""
        if self._row_range is None:
            raise ValueError("rowRange must not be null")

        entries = self._scan()
        index_path = self._index_path_factory.index_path()

        # Convert entries to dict format for RowRangeGlobalIndexScanner
        index_entries = []
        for entry in entries:
            index_entries.append({
                'file_name': entry.index_file.file_name,
                'file_size': entry.index_file.file_size,
                'index_type': entry.index_file.index_type,
                'global_index_meta': entry.index_file.global_index_meta
            })

        return RowRangeGlobalIndexScanner(
            options=self._options,
            fields=self._row_type,
            file_io=self._file_io,
            index_path=index_path,
            row_range=self._row_range,
            index_entries=index_entries
        )

    def _scan(self) -> List['IndexManifestEntry']:
        """
        Scan index manifest entries.
        """
        if self._cached_entries is not None:
            return self._cached_entries

        def entry_filter(entry: 'IndexManifestEntry') -> bool:
            # Filter by partition predicate
            if self._partition_predicate is not None:
                if not self._partition_predicate.test(entry.partition):
                    return False

            # Filter by row range
            if self._row_range is not None:
                global_index_meta = entry.index_file.global_index_meta
                if global_index_meta is None:
                    return False

                entry_start = global_index_meta.row_range_start
                entry_end = global_index_meta.row_range_end

                if not Range.intersect(
                    entry_start, entry_end,
                    self._row_range.from_, self._row_range.to
                ):
                    return False

            return True

        snapshot = self._snapshot
        if snapshot is None:
            snapshot = self._snapshot_manager.get_latest_snapshot()

        self._cached_entries = self._index_file_handler.scan(snapshot, entry_filter)
        return self._cached_entries
