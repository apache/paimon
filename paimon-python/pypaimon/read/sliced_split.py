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
SlicedSplit wraps a Split with file index ranges for shard/slice processing.
"""

from typing import List, Dict, Tuple

from pypaimon.read.split import Split


class SlicedSplit(Split):
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
        shard_file_idx_map: Dict[str, Tuple[int, int]]
    ):
        self._data_split = data_split
        self._shard_file_idx_map = shard_file_idx_map

    def data_split(self) -> 'Split':
        return self._data_split

    def shard_file_idx_map(self) -> Dict[str, Tuple[int, int]]:
        return self._shard_file_idx_map

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
        if not self._shard_file_idx_map:
            return self._data_split.row_count
        
        total_rows = 0
        for file in self._data_split.files:
            if file.file_name in self._shard_file_idx_map:
                start, end = self._shard_file_idx_map[file.file_name]
                if start != -1 and end != -1:
                    total_rows += (end - start)
            else:
                total_rows += file.row_count
        
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

    def _get_sliced_file_row_count(self, file: 'DataFileMeta') -> int:
        if file.file_name in self._shard_file_idx_map:
            start, end = self._shard_file_idx_map[file.file_name]
            return (end - start) if start != -1 and end != -1 else 0
        return file.row_count

    def merged_row_count(self):
        if not self._shard_file_idx_map:
            return self._data_split.merged_row_count()
        
        underlying_merged = self._data_split.merged_row_count()
        if underlying_merged is not None:
            original_row_count = self._data_split.row_count
            return int(underlying_merged * self.row_count / original_row_count) if original_row_count > 0 else 0
        
        from pypaimon.read.split import DataSplit
        
        if not isinstance(self._data_split, DataSplit):
            return None
        
        first_row_ids = [f.first_row_id for f in self._data_split.files if f.first_row_id is not None]
        if len(first_row_ids) == len(set(first_row_ids)):
            return self.row_count
        
        row_count_by_first_row_id = {}
        for file in self._data_split.files:
            if file.first_row_id is not None and file.first_row_id not in row_count_by_first_row_id:
                sliced_count = self._get_sliced_file_row_count(file)
                if not file.file_name.endswith('.blob'):
                    row_count_by_first_row_id[file.first_row_id] = sliced_count
        
        for file in self._data_split.files:
            if file.first_row_id is not None and file.first_row_id not in row_count_by_first_row_id:
                row_count_by_first_row_id[file.first_row_id] = self._get_sliced_file_row_count(file)
        
        actual_row_count = sum(row_count_by_first_row_id.values())
        
        if self._data_split.data_deletion_files is not None:
            if not all(f is None or f.cardinality is not None for f in self._data_split.data_deletion_files):
                return None
            
            for i, deletion_file in enumerate(self._data_split.data_deletion_files):
                if deletion_file is not None and deletion_file.cardinality is not None and i < len(self._data_split.files):
                    file = self._data_split.files[i]
                    if file.first_row_id is not None and file.first_row_id in row_count_by_first_row_id:
                        file_original_count = file.row_count
                        file_sliced_count = row_count_by_first_row_id[file.first_row_id]
                        if file_original_count > 0:
                            deletion_ratio = deletion_file.cardinality / file_original_count
                            actual_row_count -= int(file_sliced_count * deletion_ratio)
                        else:
                            actual_row_count -= deletion_file.cardinality
                        del row_count_by_first_row_id[file.first_row_id]
        
        return actual_row_count

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
