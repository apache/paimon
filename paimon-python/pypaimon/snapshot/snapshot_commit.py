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

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List

from pypaimon.common.json_util import json_field
from pypaimon.snapshot.snapshot import Snapshot


@dataclass
class PartitionStatistics:
    """
    Represents partition statistics for snapshot commits.

    This class matches the Java org.apache.paimon.partition.PartitionStatistics
    structure for proper JSON serialization in REST API calls.
    """

    spec: Dict[str, str] = json_field("spec", default_factory=dict)
    record_count: int = json_field("recordCount", default=0)
    file_size_in_bytes: int = json_field("fileSizeInBytes", default=0)
    file_count: int = json_field("fileCount", default=0)
    last_file_creation_time: int = json_field("lastFileCreationTime", default_factory=lambda: int(time.time() * 1000))
    total_buckets: int = json_field("totalBuckets", default=0)

    @classmethod
    def create(cls, partition_spec: Dict[str, str] = None, record_count: int = 0,
               file_count: int = 0, file_size_in_bytes: int = 0,
               last_file_creation_time: int = None, total_buckets: int = 0) -> 'PartitionStatistics':
        """
        Factory method to create PartitionStatistics with backward compatibility.

        Args:
            partition_spec: Partition specification dictionary
            record_count: Number of records
            file_count: Number of files
            file_size_in_bytes: Total file size in bytes
            last_file_creation_time: Last file creation time in milliseconds
            total_buckets: Total number of buckets in the partition

        Returns:
            PartitionStatistics instance
        """
        return cls(
            spec=partition_spec or {},
            record_count=record_count,
            file_count=file_count,
            file_size_in_bytes=file_size_in_bytes,
            last_file_creation_time=last_file_creation_time or int(time.time() * 1000),
            total_buckets=total_buckets
        )


class SnapshotCommit(ABC):
    """Interface to commit snapshot atomically."""

    @abstractmethod
    def commit(self, snapshot: Snapshot, branch: str, statistics: List[PartitionStatistics]) -> bool:
        """
        Commit the given snapshot.

        Args:
            snapshot: The snapshot to commit
            branch: The branch name to commit to
            statistics: List of partition statistics

        Returns:
            True if commit was successful, False otherwise

        Raises:
            Exception: If commit fails
        """
        pass

    @abstractmethod
    def close(self):
        """Close the snapshot commit and release any resources."""
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
