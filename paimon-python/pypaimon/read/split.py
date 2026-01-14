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


class SplitBase(ABC):
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


class Split(SplitBase):
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
