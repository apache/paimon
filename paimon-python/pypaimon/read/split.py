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
from typing import List, Optional

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.source.deletion_file import DeletionFile


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
        file_paths: List[str],
        row_count: int,
        file_size: int,
        raw_convertible: bool = False,
        data_deletion_files: Optional[List[DeletionFile]] = None
    ):
        self._files = files
        self._partition = partition
        self._bucket = bucket
        self._file_paths = file_paths
        self._row_count = row_count
        self._file_size = file_size
        self.raw_convertible = raw_convertible
        self.data_deletion_files = data_deletion_files

    @property
    def files(self) -> List[DataFileMeta]:
        return self._files

    @property
    def partition(self) -> GenericRow:
        return self._partition

    @property
    def bucket(self) -> int:
        return self._bucket

    @property
    def row_count(self) -> int:
        return self._row_count

    @property
    def file_size(self) -> int:
        return self._file_size

    @property
    def file_paths(self) -> List[str]:
        return self._file_paths

    def set_row_count(self, row_count: int) -> None:
        self._row_count = row_count

    def merged_row_count(self) -> Optional[int]:
        if not self.raw_convertible:
            return None
        
        # In data evolution scenario, files with the same first_row_id represent the same logical row.
        # We need to count each first_row_id only once to get the actual row count.
        if len(self._files) > 1:
            first_row_ids = [f.first_row_id for f in self._files if f.first_row_id is not None]
            if len(first_row_ids) != len(set(first_row_ids)):
                # Multiple files with the same first_row_id indicates data evolution scenario
                # Calculate actual row count by counting each first_row_id only once
                # Use the row_count from the first non-blob file for each first_row_id
                row_count_by_first_row_id = {}
                for file in self._files:
                    if file.first_row_id is not None:
                        if file.first_row_id not in row_count_by_first_row_id:
                            # Prefer non-blob file's row_count for accuracy
                            if not file.file_name.endswith('.blob'):
                                row_count_by_first_row_id[file.first_row_id] = file.row_count
                # Fill in any missing first_row_ids with the first file's row_count
                for file in self._files:
                    if file.first_row_id is not None and file.first_row_id not in row_count_by_first_row_id:
                        row_count_by_first_row_id[file.first_row_id] = file.row_count
                
                # Sum up row counts for each unique first_row_id
                actual_row_count = sum(row_count_by_first_row_id.values())
                
                # Account for deletion files if present
                # Note: deletion files in data evolution may need special handling
                if self.data_deletion_files is not None:
                    if not all(
                        f is None or f.cardinality is not None
                        for f in self.data_deletion_files
                    ):
                        return None
                    
                    # Subtract deletion counts
                    for i, deletion_file in enumerate(self.data_deletion_files):
                        if deletion_file is not None and deletion_file.cardinality is not None:
                            if i < len(self._files):
                                file = self._files[i]
                                if file.first_row_id is not None:
                                    # Only subtract once per first_row_id
                                    if file.first_row_id in row_count_by_first_row_id:
                                        actual_row_count -= deletion_file.cardinality
                                        # Remove from dict to avoid double subtraction
                                        del row_count_by_first_row_id[file.first_row_id]
                
                return actual_row_count
        
        if self.data_deletion_files is not None:
            if not all(
                f is None or f.cardinality is not None
                for f in self.data_deletion_files
            ):
                return None
        
        sum_rows = 0
        for i, file in enumerate(self._files):
            deletion_file = (
                None if self.data_deletion_files is None
                else self.data_deletion_files[i] if i < len(self.data_deletion_files)
                else None
            )
            
            if deletion_file is None:
                sum_rows += file.row_count
            elif deletion_file.cardinality is not None:
                sum_rows += file.row_count - deletion_file.cardinality
        
        return sum_rows
