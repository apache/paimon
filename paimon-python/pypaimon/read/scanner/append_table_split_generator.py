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
from typing import List

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.read.scanner.split_generator import AbstractSplitGenerator
from pypaimon.read.split import Split
from pypaimon.read.scan_distribution import slice_append_splits


class AppendTableSplitGenerator(AbstractSplitGenerator):
    """
    Split generator for append-only tables.
    """

    def create_splits(self, file_entries: List[ManifestEntry]) -> List[Split]:
        partitioned_files = defaultdict(list)
        for entry in file_entries:
            partitioned_files[(tuple(entry.partition.values), entry.bucket)].append(entry)

        def weight_func(f: DataFileMeta) -> int:
            return max(f.file_size, self.open_file_cost)

        splits = []
        for key, file_entries_list in partitioned_files.items():
            if not file_entries_list:
                continue

            data_files: List[DataFileMeta] = [e.file for e in file_entries_list]

            packed_files: List[List[DataFileMeta]] = self._pack_for_ordered(
                data_files, weight_func, self.target_split_size
            )
            splits += self._build_split_from_pack(
                packed_files, file_entries_list, False
            )

        if self.idx_of_this_subtask is not None:
            start, end = self._compute_shard_range(sum(split.row_count for split in splits))
            return slice_append_splits(splits, start, end)
        if self.start_pos_of_this_subtask is not None:
            return slice_append_splits(
                splits, self.start_pos_of_this_subtask, self.end_pos_of_this_subtask)
        return splits
