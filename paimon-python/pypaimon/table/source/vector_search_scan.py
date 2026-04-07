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

"""Vector search scan to scan index files."""

from abc import ABC, abstractmethod
from collections import defaultdict

from pypaimon.table.source.vector_search_split import VectorSearchSplit
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

    def __init__(self, table, vector_column):
        self._table = table
        self._vector_column = vector_column

    def scan(self):
        # type: () -> VectorSearchScanPlan
        from pypaimon.index.index_file_handler import IndexFileHandler
        from pypaimon.snapshot.snapshot_manager import SnapshotManager

        vector_column = self._vector_column

        from pypaimon.snapshot.time_travel_util import TimeTravelUtil
        from pypaimon.common.options.options import Options
        snapshot = TimeTravelUtil.try_travel_to_snapshot(
            Options(self._table.table_schema.options),
            self._table.tag_manager()
        )
        if snapshot is None:
            snapshot = SnapshotManager(self._table).get_latest_snapshot()

        index_file_handler = IndexFileHandler(table=self._table)

        def index_file_filter(entry):
            global_index_meta = entry.index_file.global_index_meta
            if global_index_meta is None:
                return False
            return vector_column.id == global_index_meta.index_field_id

        entries = index_file_handler.scan(snapshot, index_file_filter)
        all_index_files = [entry.index_file for entry in entries]

        # Group by (rowRangeStart, rowRangeEnd)
        by_range = defaultdict(list)
        for index_file in all_index_files:
            meta = index_file.global_index_meta
            assert meta is not None
            range_key = Range(meta.row_range_start, meta.row_range_end)
            by_range[range_key].append(index_file)

        splits = []
        for range_key, files in by_range.items():
            splits.append(VectorSearchSplit(range_key.from_, range_key.to, files))

        return VectorSearchScanPlan(splits)
