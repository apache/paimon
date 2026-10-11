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

"""Full-text scan to scan index files."""

from abc import ABC, abstractmethod
from collections import defaultdict
from typing import List

from pypaimon.globalindex.data_evolution_global_index_coverage import DataEvolutionGlobalIndexCoverage
from pypaimon.globalindex.full_text.native_full_text_global_index_reader import (
    FULL_TEXT_IDENTIFIER,
)
from pypaimon.table.source.full_text_search_split import (
    FullTextSearchSplit,
    IndexFullTextSearchSplit,
    RawFullTextSearchSplit,
)
from pypaimon.utils.range import Range


class FullTextScanPlan:
    """Plan of full-text scan."""

    def __init__(self, splits: List[FullTextSearchSplit], snapshot=None):
        self._splits = splits
        self._snapshot = snapshot

    def snapshot(self):
        return self._snapshot

    def splits(self) -> List[FullTextSearchSplit]:
        return self._splits


class FullTextScan(ABC):
    """Full-text scan to scan index files."""

    @abstractmethod
    def scan(self) -> FullTextScanPlan:
        pass


class DataEvolutionFullTextScan(FullTextScan):
    """Implementation for FullTextScan."""

    def __init__(
            self,
            table: 'FileStoreTable',
            text_columns,
            partition_filter=None,
            filter_=None):
        self._table = table
        self._text_columns = list(text_columns)
        self._partition_filter = partition_filter
        self._filter = filter_

    def scan(self) -> FullTextScanPlan:
        from pypaimon.index.index_file_handler import IndexFileHandler

        if not self._text_columns:
            return FullTextScanPlan([])

        text_column_ids = {field.id for field in self._text_columns}
        id_to_column = {field.id: field.name for field in self._text_columns}

        from pypaimon.snapshot.time_travel_util import TimeTravelUtil
        snapshot = TimeTravelUtil.resolve_snapshot(self._table)
        if snapshot is None:
            return FullTextScanPlan([])

        index_file_handler = IndexFileHandler(table=self._table)
        partition_filter = self._partition_filter

        def index_file_filter(entry):
            if partition_filter is not None:
                if not partition_filter.test(entry.partition):
                    return False
            global_index_meta = entry.index_file.global_index_meta
            if global_index_meta is None:
                return False
            return (
                global_index_meta.index_field_id in text_column_ids
                and _supports_full_text_search(entry.index_file.index_type)
            )

        from pypaimon.read.push_down_utils import _get_all_fields
        filter_names = _get_all_fields(self._filter) if self._filter is not None else set()
        filter_ids = {field.id for field in self._table.fields if field.name in filter_names}

        def selected(entry):
            if index_file_filter(entry):
                return True
            if partition_filter is not None and not partition_filter.test(entry.partition):
                return False
            file = entry.index_file
            meta = file.global_index_meta
            return (meta is not None and file.index_type in ('btree', 'bitmap')
                    and bool(filter_ids.intersection([meta.index_field_id] + list(meta.extra_field_ids or []))))

        entries = index_file_handler.scan(snapshot, selected)
        all_index_files = [entry.index_file for entry in entries if index_file_filter(entry)]
        scalar_files = [entry.index_file for entry in entries if entry.index_file.index_type in ('btree', 'bitmap')]
        # Java requires an existing full-text definition before admitting raw ranges.
        if not all_index_files:
            return FullTextScanPlan([], snapshot)

        # Group full-text index files by column and (rowRangeStart, rowRangeEnd).
        by_column_and_range = defaultdict(lambda: defaultdict(list))
        for index_file in all_index_files:
            meta = index_file.global_index_meta
            assert meta is not None
            range_key = Range(meta.row_range_start, meta.row_range_end)
            column_name = id_to_column[meta.index_field_id]
            by_column_and_range[column_name][range_key].append(index_file)

        splits = []
        for column_name, by_range in by_column_and_range.items():
            for range_key, files in by_range.items():
                splits.append(
                    IndexFullTextSearchSplit(
                        column_name, range_key.from_, range_key.to, files,
                        [file for file in scalar_files
                         if file.global_index_meta.row_range_start <= range_key.to
                         and file.global_index_meta.row_range_end >= range_key.from_]))

        raw_row_ranges = DataEvolutionGlobalIndexCoverage(
            self._table,
            snapshot,
            partition_filter,
            all_index_files,
        ).unindexed_ranges(
            list(text_column_ids),
            search_mode=self._table.options.full_text_index_search_mode(),
        )
        if raw_row_ranges:
            splits.append(RawFullTextSearchSplit(raw_row_ranges))

        return FullTextScanPlan(splits, snapshot)


def _supports_full_text_search(index_type):
    return index_type == FULL_TEXT_IDENTIFIER
