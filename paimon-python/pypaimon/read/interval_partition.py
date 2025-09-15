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

import heapq
from dataclasses import dataclass
from functools import cmp_to_key
from typing import Callable, List

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.table.row.generic_row import GenericRow


@dataclass
class SortedRun:
    """
    A SortedRun is a list of files sorted by their keys.
    The key intervals [minKey, maxKey] of these files do not overlap.
    """
    files: List[DataFileMeta]


class IntervalPartition:
    """
    Algorithm to partition several data files into the minimum number of SortedRuns.
    """

    def __init__(self, input_files: List[DataFileMeta]):
        self.files = input_files.copy()
        self.key_comparator = default_key_comparator
        self.files.sort(key=cmp_to_key(self._compare_files))

    def partition(self) -> List[List[SortedRun]]:
        result = []
        section: List[DataFileMeta] = []
        bound = None

        for meta in self.files:
            if section and self.key_comparator(meta.min_key, bound) > 0:
                result.append(self._partition_section(section))
                section.clear()
                bound = None
            section.append(meta)
            if bound is None or self.key_comparator(meta.max_key, bound) > 0:
                bound = meta.max_key

        if section:
            result.append(self._partition_section(section))
        return result

    def _partition_section(self, metas: List[DataFileMeta]) -> List[SortedRun]:
        heap: List[HeapRun] = []
        first_run = [metas[0]]
        heapq.heappush(heap, HeapRun(first_run, self.key_comparator))
        for i in range(1, len(metas)):
            meta = metas[i]

            earliest_finishing_run = heap[0]
            last_max_key = earliest_finishing_run.run[-1].max_key
            if self.key_comparator(meta.min_key, last_max_key) > 0:
                top = heapq.heappop(heap)
                top.run.append(meta)
                heapq.heappush(heap, top)
            else:
                new_run = [meta]
                heapq.heappush(heap, HeapRun(new_run, self.key_comparator))

        return [SortedRun(files=h.run) for h in heap]

    def _compare_files(self, f1: DataFileMeta, f2: DataFileMeta) -> int:
        min_key_cmp = self.key_comparator(f1.min_key, f2.min_key)
        if min_key_cmp != 0:
            return min_key_cmp
        return self.key_comparator(f1.max_key, f2.max_key)


@dataclass
class HeapRun:
    run: List[DataFileMeta]
    comparator: Callable[[GenericRow, GenericRow], int]

    def __lt__(self, other) -> bool:
        my_last_max = self.run[-1].max_key
        other_last_max = other.run[-1].max_key
        return self.comparator(my_last_max, other_last_max) < 0


def default_key_comparator(key1: GenericRow, key2: GenericRow) -> int:
    if not key1 or not key1.values:
        if not key2 or not key2.values:
            return 0
        return -1
    if not key2 or not key2.values:
        return 1

    min_field_count = min(len(key1.values), len(key2.values))
    for i in range(min_field_count):
        val1 = key1.values[i]
        val2 = key2.values[i]
        if val1 is None and val2 is None:
            continue
        if val1 is None:
            return -1
        if val2 is None:
            return 1
        if val1 < val2:
            return -1
        elif val1 > val2:
            return 1

    if len(key1.values) < len(key2.values):
        return -1
    elif len(key1.values) > len(key2.values):
        return 1
    else:
        return 0
