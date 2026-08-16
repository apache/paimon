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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.read.split import DataSplit
from pypaimon.table.source.global_index_split_result import GlobalIndexSplitResult
from pypaimon.utils.range import Range


LOG = logging.getLogger(__name__)


class PrimaryKeySortedIndexResult(GlobalIndexSplitResult):
    MAX_INDEXED_RANGES_PER_FILE = 4096

    def __init__(self, evaluated_plan):
        self._snapshot_id = evaluated_plan.snapshot_id
        converted = []
        preserved = set()
        for evaluated in evaluated_plan.files:
            file_plan = evaluated.file
            source = file_plan.source_split
            if not source.raw_convertible:
                if id(source) not in preserved:
                    preserved.add(id(source))
                    converted.append(source)
                continue
            single = _single_file_split(file_plan)
            if evaluated.result is None:
                converted.append(single)
                continue
            positions = evaluated.result.results()
            if positions.is_empty():
                continue
            ranges = self._ranges(positions, file_plan.data_file.row_count)
            if ranges is None:
                LOG.warning("Invalid primary-key sorted-index positions for %s; using raw scan.",
                            file_plan.data_file.file_name)
                converted.append(single)
            else:
                converted.append(IndexedSplit(single, ranges, None))
        self._splits = tuple(converted)

    @property
    def snapshot_id(self):
        return self._snapshot_id

    @property
    def splits(self):
        return self._splits

    @classmethod
    def _ranges(cls, positions, row_count):
        ranges = []
        start = end = None
        for position in positions:
            if position < 0 or position >= row_count or position > 2147483647:
                return None
            if start is None:
                start = position
            elif position != end + 1:
                if len(ranges) >= cls.MAX_INDEXED_RANGES_PER_FILE:
                    return None
                ranges.append(Range(start, end))
                start = position
            end = position
        if start is None or len(ranges) >= cls.MAX_INDEXED_RANGES_PER_FILE:
            return None
        ranges.append(Range(start, end))
        return ranges


def _single_file_split(file_plan):
    source = file_plan.source_split
    deletion_files = None
    if source.data_deletion_files is not None:
        deletion_files = [source.data_deletion_files[file_plan.file_index]]
    return DataSplit(
        files=[file_plan.data_file],
        partition=source.partition,
        bucket=source.bucket,
        raw_convertible=False,
        data_deletion_files=deletion_files,
    )
