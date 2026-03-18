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

from abc import ABC, abstractmethod
from typing import List, Optional, Callable

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.source.deletion_file import DeletionFile
from pypaimon.utils.range import Range


class Split(ABC):
    """
    Base interface for Split following Java's org.apache.paimon.table.source.Split.

    All split implementations should inherit from this class.
    """

    @property
    @abstractmethod
    def row_count(self) -> int:
        """Return the total row count of this split."""
        pass

    @property
    @abstractmethod
    def files(self) -> List[DataFileMeta]:
        """Return the data files in this split."""
        pass

    @property
    @abstractmethod
    def partition(self) -> GenericRow:
        """Return the partition of this split."""
        pass

    @property
    @abstractmethod
    def bucket(self) -> int:
        """Return the bucket of this split."""
        pass

    def merged_row_count(self) -> Optional[int]:
        """
        Return the merged row count of data files. For example, when the delete vector is enabled in
        the primary key table, the number of rows that have been deleted will be subtracted from the
        returned result. In the Data Evolution mode of the Append table, the actual number of rows
        will be returned.
        """
        return None


class DataSplit(Split):
    """
    Implementation of Split for native Python reading.

    This is equivalent to Java's DataSplit.
    """

    def __init__(
        self,
        files: List[DataFileMeta],
        partition: GenericRow,
        bucket: int,
        raw_convertible: bool = False,
        data_deletion_files: Optional[List[DeletionFile]] = None
    ):
        self._files = files
        self._partition = partition
        self._bucket = bucket
        self.raw_convertible = raw_convertible
        self.data_deletion_files = data_deletion_files

    @property
    def files(self) -> List[DataFileMeta]:
        return self._files

    def filter_file(self, func: Callable[[DataFileMeta], bool]) -> Optional['DataSplit']:
        """
        Filter files based on a predicate function and create a new DataSplit.
        
        Args:
            func: A function that takes a DataFileMeta and returns True if the file should be kept
        
        Returns:
            A new DataSplit with filtered files, adjusted data_deletion_files
        """
        # Filter files based on the predicate
        filtered_files = [f for f in self._files if func(f)]
        
        # If no files match, return None
        if not filtered_files:
            return None
        
        # Find indices of filtered files to adjust data_deletion_files
        filtered_indices = [i for i, f in enumerate(self._files) if func(f)]
        
        # Filter data_deletion_files to match filtered files
        filtered_data_deletion_files = None
        if self.data_deletion_files is not None:
            filtered_data_deletion_files = [self.data_deletion_files[i] for i in filtered_indices]
        
        # Create new DataSplit with filtered data
        return DataSplit(
            files=filtered_files,
            partition=self._partition,
            bucket=self._bucket,
            raw_convertible=self.raw_convertible,
            data_deletion_files=filtered_data_deletion_files
        )

    @property
    def partition(self) -> GenericRow:
        return self._partition

    @property
    def bucket(self) -> int:
        return self._bucket

    @property
    def row_count(self) -> int:
        """Calculate total row count from all files."""
        return sum(f.row_count for f in self._files)

    @property
    def file_size(self) -> int:
        """Calculate total file size from all files."""
        return sum(f.file_size for f in self._files)

    @property
    def file_paths(self) -> List[str]:
        """Get file paths from all files."""
        return [f.file_path for f in self._files if f.file_path is not None]

    def merged_row_count(self) -> Optional[int]:
        """
        Return the merged row count of data files. For example, when the delete vector is enabled in
        the primary key table, the number of rows that have been deleted will be subtracted from the
        returned result. In the Data Evolution mode of the Append table, the actual number of rows
        will be returned.
        """
        if self._raw_merged_row_count_available():
            return self._raw_merged_row_count()
        if self._data_evolution_row_count_available():
            return self._data_evolution_merged_row_count()
        return None

    def _raw_merged_row_count_available(self) -> bool:
        return self.raw_convertible and (
            self.data_deletion_files is None
            or all(f is None or f.cardinality is not None for f in self.data_deletion_files)
        )

    def _raw_merged_row_count(self) -> int:
        sum_rows = 0
        for i, file in enumerate(self._files):
            deletion_file = None
            if self.data_deletion_files is not None and i < len(self.data_deletion_files):
                deletion_file = self.data_deletion_files[i]
            
            if deletion_file is None:
                sum_rows += file.row_count
            elif deletion_file.cardinality is not None:
                sum_rows += file.row_count - deletion_file.cardinality
        
        return sum_rows

    def _data_evolution_row_count_available(self) -> bool:
        for file in self._files:
            if file.first_row_id is None:
                return False
        return True

    def _data_evolution_merged_row_count(self) -> int:
        if not self._files:
            return 0
        
        file_ranges = []
        for file in self._files:
            file_ranges.append(file.row_id_range())
        
        if not file_ranges:
            return 0

        ranges = Range.sort_and_merge_overlap(file_ranges, True, True)
        return sum([r.count() for r in ranges])
