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

"""Row ranges covered and not covered by global index files."""

from typing import Collection, Dict, List, Optional, Union

from pypaimon.common.options.core_options import CoreOptions, GlobalIndexSearchMode
from pypaimon.common.options.options import Options
from pypaimon.common.predicate import Predicate
from pypaimon.read.push_down_utils import _get_all_fields
from pypaimon.schema.data_types import DataField
from pypaimon.utils.range import Range


class GlobalIndexCoverage:
    """Computes global-index coverage by field id."""

    def __init__(
        self,
        table,
        snapshot,
        partition_filter,
        index_files: Collection['IndexFileMeta'],
    ):
        self._table = table
        self._snapshot = snapshot
        self._partition_filter = partition_filter
        self._coverage_by_field: Dict[int, List[Range]] = {}
        for index_file in index_files:
            meta = index_file.global_index_meta
            if meta is None:
                continue
            row_range = Range(meta.row_range_start, meta.row_range_end)
            self._add_coverage(meta.index_field_id, row_range)
            if meta.extra_field_ids is not None:
                for extra_field_id in meta.extra_field_ids:
                    self._add_coverage(extra_field_id, row_range)

    def unindexed_ranges(
        self,
        fields_or_field_id: Union[List[DataField], int],
        predicate: Optional[Predicate] = None,
    ) -> List[Range]:
        if isinstance(fields_or_field_id, int):
            field_ids = {fields_or_field_id}
        else:
            field_by_name = {f.name: f for f in fields_or_field_id}
            field_ids = set()
            for name in _get_all_fields(predicate):
                field = field_by_name.get(name)
                if field is not None:
                    field_ids.add(field.id)
        return self._unindexed_ranges(field_ids)

    def _add_coverage(self, field_id: int, row_range: Range) -> None:
        self._coverage_by_field.setdefault(field_id, []).append(row_range)

    def _indexed_ranges(self, field_ids: Collection[int]) -> List[Range]:
        ranges = None
        for field_id in field_ids:
            field_ranges = self._coverage_by_field.get(field_id)
            if not field_ranges:
                return []
            field_ranges = Range.sort_and_merge_overlap(field_ranges, True)
            ranges = field_ranges if ranges is None else Range.and_(ranges, field_ranges)
        if ranges is None:
            return []
        return Range.sort_and_merge_overlap(ranges, True)

    def _unindexed_ranges(self, field_ids: Collection[int]) -> List[Range]:
        search_mode = _global_index_search_mode(self._table)
        if search_mode == GlobalIndexSearchMode.FAST:
            return []
        next_row_id = getattr(self._snapshot, "next_row_id", None)
        if self._snapshot is None or next_row_id is None:
            return []
        if next_row_id <= 0:
            return []

        if search_mode == GlobalIndexSearchMode.DETAIL:
            data_ranges = self._data_ranges_by_data_files()
        else:
            data_ranges = [Range(0, next_row_id - 1)]

        indexed_ranges = Range.sort_and_merge_overlap(
            self._indexed_ranges(field_ids), True)
        unindexed = []
        for data_range in Range.sort_and_merge_overlap(data_ranges, True):
            unindexed.extend(data_range.exclude(indexed_ranges))
        return Range.sort_and_merge_overlap(unindexed, True)

    def _data_ranges_by_data_files(self) -> List[Range]:
        if hasattr(self._table, "data_ranges_for_global_index_coverage"):
            return self._table.data_ranges_for_global_index_coverage(
                self._snapshot,
                self._partition_filter,
            )

        manifest_list_manager = getattr(self._table, "manifest_list_manager", None)
        if manifest_list_manager is None:
            from pypaimon.manifest.manifest_list_manager import ManifestListManager
            manifest_list_manager = ManifestListManager(self._table)
        manifest_files = manifest_list_manager.read_all(self._snapshot)
        from pypaimon.manifest.manifest_file_manager import ManifestFileManager
        manager = ManifestFileManager(self._table)
        entries = manager.read_entries_parallel(
            manifest_files,
            self._entry_matches_partition,
            max_workers=self._table.options.scan_manifest_parallelism(),
        )
        data_ranges = []
        for entry in entries:
            row_range = entry.file.row_id_range()
            if row_range is not None:
                data_ranges.append(row_range)
        return data_ranges

    def _entry_matches_partition(self, entry) -> bool:
        if self._partition_filter is None:
            return True
        return self._partition_filter.test(entry.partition)


def _global_index_search_mode(table):
    options = getattr(table, "options", None)
    if options is None:
        return GlobalIndexSearchMode.FAST
    if hasattr(options, "global_index_search_mode"):
        return options.global_index_search_mode()
    return CoreOptions(Options.from_none()).global_index_search_mode()
