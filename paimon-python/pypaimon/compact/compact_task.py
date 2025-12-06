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
from typing import List, Tuple, Any


class CompactTask(ABC):
    """Base class for compaction tasks."""

    @abstractmethod
    def get_partition(self) -> Tuple:
        """Get the partition key for this task."""
        pass

    @abstractmethod
    def get_bucket(self) -> int:
        """Get the bucket id for this task."""
        pass

    @abstractmethod
    def get_files(self) -> List[Any]:
        """Get the list of files to be compacted."""
        pass

    @abstractmethod
    def execute(self) -> None:
        """Execute the compaction task."""
        pass


class AppendCompactTask(CompactTask):
    """Compaction task for append-only tables."""

    def __init__(self, partition: Tuple, bucket: int, files: List[Any]):
        """
        Initialize an append-only compaction task.

        Args:
            partition: The partition key (tuple of partition field values)
            bucket: The bucket id
            files: List of DataFileMeta objects to be compacted
        """
        self.partition = partition
        self.bucket = bucket
        self.files = files or []

    def get_partition(self) -> Tuple:
        """Get the partition key for this task."""
        return self.partition

    def get_bucket(self) -> int:
        """Get the bucket id for this task."""
        return self.bucket

    def get_files(self) -> List[Any]:
        """Get the list of files to be compacted."""
        return self.files

    def execute(self) -> None:
        """Execute the compaction task."""
        if not self.files:
            raise ValueError(
                f"No files to compact for partition {self.partition}, "
                f"bucket {self.bucket}"
            )

    def __repr__(self) -> str:
        return (
            f"AppendCompactTask(partition={self.partition}, bucket={self.bucket}, "
            f"files_count={len(self.files)})"
        )


class KeyValueCompactTask(CompactTask):
    """Compaction task for primary key tables."""

    def __init__(
        self,
        partition: Tuple,
        bucket: int,
        files: List[Any],
        changelog_only: bool = False
    ):
        """
        Initialize a key-value compaction task.

        Args:
            partition: The partition key (tuple of partition field values)
            bucket: The bucket id
            files: List of DataFileMeta objects to be compacted
            changelog_only: Whether to only compact changelog files
        """
        self.partition = partition
        self.bucket = bucket
        self.files = files or []
        self.changelog_only = changelog_only

    def get_partition(self) -> Tuple:
        """Get the partition key for this task."""
        return self.partition

    def get_bucket(self) -> int:
        """Get the bucket id for this task."""
        return self.bucket

    def get_files(self) -> List[Any]:
        """Get the list of files to be compacted."""
        return self.files

    def is_changelog_only(self) -> bool:
        """Check if this task only compacts changelog files."""
        return self.changelog_only

    def execute(self) -> None:
        """Execute the compaction task."""
        if not self.files:
            raise ValueError(
                f"No files to compact for partition {self.partition}, "
                f"bucket {self.bucket}"
            )

    def __repr__(self) -> str:
        return (
            f"KeyValueCompactTask(partition={self.partition}, bucket={self.bucket}, "
            f"files_count={len(self.files)}, changelog_only={self.changelog_only})"
        )
