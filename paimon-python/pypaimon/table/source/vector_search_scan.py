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

"""Vector search scan to scan index files."""

from abc import ABC, abstractmethod
from collections import defaultdict

from pypaimon.globalindex.global_index_coverage import GlobalIndexCoverage
from pypaimon.table.source.vector_search_split import (
    IndexVectorSearchSplit,
    RawVectorSearchSplit,
    VectorSearchSplit,
)
from pypaimon.utils.range import Range


class VectorSearchScanPlan:
    """Plan of vector search scan."""

    def __init__(self, splits):
        # type: (List[VectorSearchSplit]) -> None
        self._splits = splits

    def splits(self):
        # type: () -> List[VectorSearchSplit]
        return self._splits


class VectorSearchScan(ABC):
    """Vector search scan to scan index files."""

    @abstractmethod
    def scan(self):
        # type: () -> VectorSearchScanPlan
        pass


class VectorSearchScanImpl(VectorSearchScan):
    """Implementation for VectorSearchScan."""

    def __init__(
        self,
        table,
        vector_column,
        filter_=None,
        partition_filter=None,
        options=None,
    ):
        self._table = table
        self._vector_column = vector_column
        self._filter = filter_
        self._partition_filter = partition_filter
        self._options = dict(options or {})

    def scan(self):
        # type: () -> VectorSearchScanPlan
        from pypaimon.common.options.options import Options
        from pypaimon.index.index_file_handler import IndexFileHandler
        from pypaimon.read.push_down_utils import _get_all_fields
        from pypaimon.snapshot.time_travel_util import TimeTravelUtil

        vector_column = self._vector_column

        # Field IDs referenced by the scalar filter — used to pull in scalar
        # index files for pre-filtering.
        filter_field_ids = set()
        if self._filter is not None:
            referenced = _get_all_fields(self._filter)
            field_by_name = {f.name: f for f in self._table.fields}
            for name in referenced:
                field = field_by_name.get(name)
                if field is not None:
                    filter_field_ids.add(field.id)

        snapshot = TimeTravelUtil.try_travel_to_snapshot(
            Options(self._table.table_schema.options),
            self._table.tag_manager(),
            self._table.snapshot_manager(),
        )
        if snapshot is None:
            snapshot = self._table.snapshot_manager().get_latest_snapshot()

        index_file_handler = IndexFileHandler(table=self._table)

        partition_filter = self._partition_filter

        def contains_field(global_index_meta, field_id):
            if global_index_meta.index_field_id == field_id:
                return True
            if global_index_meta.extra_field_ids is not None:
                return field_id in global_index_meta.extra_field_ids
            return False

        def index_file_filter(entry):
            if partition_filter is not None:
                if not partition_filter.test(entry.partition):
                    return False
            global_index_meta = entry.index_file.global_index_meta
            if global_index_meta is None:
                return False
            field_id = global_index_meta.index_field_id
            if vector_column.id == field_id:
                return True
            for filter_field_id in filter_field_ids:
                if contains_field(global_index_meta, filter_field_id):
                    return True
            return False

        entries = index_file_handler.scan(snapshot, index_file_filter)
        all_index_files = [entry.index_file for entry in entries]

        # Group vector index files by (rowRangeStart, rowRangeEnd).
        vector_by_range = defaultdict(list)
        vector_index_files = []
        for index_file in all_index_files:
            meta = index_file.global_index_meta
            assert meta is not None
            if meta.index_field_id != vector_column.id:
                continue
            range_key = Range(meta.row_range_start, meta.row_range_end)
            vector_by_range[range_key].append(index_file)
            vector_index_files.append(index_file)

        vector_index_type = _vector_index_type(vector_column, all_index_files)
        if vector_index_type is None:
            vector_index_type = _configured_vector_index_type(
                self._table, vector_column, self._options)

        # For each vector range, attach matching scalar index files whose
        # row range intersects the vector range.
        splits = []
        for range_key, vector_files in vector_by_range.items():
            scalar_files = []
            for index_file in all_index_files:
                meta = index_file.global_index_meta
                assert meta is not None
                if meta.index_field_id == vector_column.id:
                    continue
                scalar_range = Range(meta.row_range_start, meta.row_range_end)
                if range_key.overlaps(scalar_range):
                    scalar_files.append(index_file)
            splits.append(
                IndexVectorSearchSplit(
                    range_key.from_,
                    range_key.to,
                    vector_files,
                    scalar_files,
                )
            )

        raw_row_ranges = GlobalIndexCoverage(
            self._table,
            snapshot,
            partition_filter,
            vector_index_files,
        ).unindexed_ranges(vector_column.id)
        scalar_index_files = [
            f for f in all_index_files
            if f.global_index_meta is not None
            and f.global_index_meta.index_field_id != vector_column.id
        ]
        if self._filter is not None:
            raw_row_ranges = Range.sort_and_merge_overlap(
                raw_row_ranges
                + GlobalIndexCoverage(
                    self._table,
                    snapshot,
                    partition_filter,
                    scalar_index_files,
                ).unindexed_ranges(self._table.fields, self._filter),
                True,
            )
        if raw_row_ranges:
            splits.append(
                RawVectorSearchSplit(
                    raw_row_ranges,
                    _scalar_index_files_for_ranges(
                        all_index_files,
                        raw_row_ranges,
                        vector_column.id,
                    ),
                    vector_index_type,
                )
            )

        return VectorSearchScanPlan(splits)


def _has_intersection(ranges, row_range):
    for r in ranges:
        if r.overlaps(row_range):
            return True
    return False


def _scalar_index_files_for_ranges(all_index_files, row_ranges, vector_field_id):
    scalar_files = []
    for index_file in all_index_files:
        meta = index_file.global_index_meta
        if meta is None or meta.index_field_id == vector_field_id:
            continue
        if _has_intersection(row_ranges, Range(meta.row_range_start, meta.row_range_end)):
            scalar_files.append(index_file)
    return scalar_files


def _vector_index_type(vector_column, index_files):
    index_type = None
    for index_file in index_files:
        meta = index_file.global_index_meta
        if meta is None or meta.index_field_id != vector_column.id:
            continue
        if index_type is None:
            index_type = index_file.index_type
        elif index_type != index_file.index_type:
            raise ValueError(
                "Vector column '%s' has multiple index types: %s and %s."
                % (vector_column.name, index_type, index_file.index_type)
            )
    return index_type


def _configured_vector_index_type(table, vector_column, options):
    keys = [
        "index_type",
        "index-type",
        "vector.index-type",
        "fields.%s.index-type" % vector_column.name,
    ]
    table_options = getattr(getattr(table, "options", None), "options", None)
    table_map = table_options.to_map() if table_options is not None else {}
    for key in keys:
        value = options.get(key) or table_map.get(key)
        if value is not None:
            return str(value).lower().strip()
    return None
