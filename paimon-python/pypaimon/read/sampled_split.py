#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
from typing import List, Dict

from pypaimon.read.split import Split


class SampledSplit(Split):
    """
    Wrapper for Split that adds file-level slicing information.

    This is used when a split needs to be further divided at the file level,
    storing the start and end row indices for each file in shard_file_idx_map.

    Maps file_name -> (start_row, end_row) where:
    - start_row: starting row index within the file (inclusive)
    - end_row: ending row index within the file (exclusive)
    - (-1, -1): file should be skipped entirely
    """

    def __init__(
            self,
            data_split: 'Split',
            sampled_file_idx_map: Dict[str, List[int]]
    ):
        self._data_split = data_split
        self._sampled_file_idx_map = sampled_file_idx_map

    def data_split(self) -> 'Split':
        return self._data_split

    def sampled_file_idx_map(self) -> Dict[str, List[int]]:
        return self._sampled_file_idx_map

    @property
    def files(self) -> List['DataFileMeta']:
        return self._data_split.files

    @property
    def partition(self) -> 'GenericRow':
        return self._data_split.partition

    @property
    def bucket(self) -> int:
        return self._data_split.bucket

    @property
    def row_count(self) -> int:
        if not self._sample_file_idx_map:
            return self._data_split.row_count

        total_rows = 0
        for file in self._data_split.files:
            positions = self._sample_file_idx_map[file.file_name]
            total_rows += len(positions)

        return total_rows

    @property
    def file_paths(self):
        return self._data_split.file_paths

    @property
    def file_size(self):
        return self._data_split.file_size

    @property
    def raw_convertible(self):
        return self._data_split.raw_convertible

    @property
    def data_deletion_files(self):
        return self._data_split.data_deletion_files

    def __eq__(self, other):
        if not isinstance(other, SlicedSplit):
            return False
        return (self._data_split == other._data_split and
                self._shard_file_idx_map == other._shard_file_idx_map)

    def __hash__(self):
        return hash((id(self._data_split), tuple(sorted(self._shard_file_idx_map.items()))))

    def __repr__(self):
        return (f"SlicedSplit(data_split={self._data_split}, "
                f"shard_file_idx_map={self._shard_file_idx_map})")
