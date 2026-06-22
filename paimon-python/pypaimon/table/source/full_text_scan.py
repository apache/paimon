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

from pypaimon.globalindex.global_index_coverage import GlobalIndexCoverage
from pypaimon.globalindex.tantivy.tantivy_full_text_global_index_reader import (
    TANTIVY_FULLTEXT_IDENTIFIER,
)
from pypaimon.table.source.full_text_search_split import FullTextSearchSplit
from pypaimon.utils.range import Range


class FullTextScanPlan:
    """Plan of full-text scan."""

    def __init__(self, splits: List[FullTextSearchSplit]):
        self._splits = splits

    def splits(self) -> List[FullTextSearchSplit]:
        return self._splits


class FullTextScan(ABC):
    """Full-text scan to scan index files."""

    @abstractmethod
    def scan(self) -> FullTextScanPlan:
        pass


class FullTextScanImpl(FullTextScan):
    """Implementation for FullTextScan."""

    def __init__(
            self,
            table: 'FileStoreTable',
            text_columns,
            partition_filter=None):
        self._table = table
        self._text_columns = list(text_columns)
        self._partition_filter = partition_filter

    def scan(self) -> FullTextScanPlan:
        from pypaimon.index.index_file_handler import IndexFileHandler

        if not self._text_columns:
            return FullTextScanPlan([])

        text_column_ids = {field.id for field in self._text_columns}
        id_to_column = {field.id: field.name for field in self._text_columns}

        from pypaimon.snapshot.time_travel_util import TimeTravelUtil
        from pypaimon.common.options.options import Options
        snapshot = TimeTravelUtil.try_travel_to_snapshot(
            Options(self._table.table_schema.options),
            self._table.tag_manager(),
            self._table.snapshot_manager(),
        )
        if snapshot is None:
            snapshot = self._table.snapshot_manager().get_latest_snapshot()

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
                and entry.index_file.index_type == TANTIVY_FULLTEXT_IDENTIFIER
            )

        entries = index_file_handler.scan(snapshot, index_file_filter)
        all_index_files = [entry.index_file for entry in entries]

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
                    FullTextSearchSplit(
                        column_name, range_key.from_, range_key.to, files))

        if all_index_files:
            raw_row_ranges = GlobalIndexCoverage(
                self._table,
                snapshot,
                partition_filter,
                all_index_files,
            ).unindexed_ranges(list(text_column_ids))
            if raw_row_ranges:
                raise NotImplementedError(
                    "Python full-text search does not support "
                    "global-index.search-mode=full/detail for uncovered row "
                    "ranges yet. Raw full-text search requires rebuilding "
                    "temporary full-text indexes.")

        return FullTextScanPlan(splits)
