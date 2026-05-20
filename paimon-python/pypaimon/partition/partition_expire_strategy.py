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

"""
Partition expiration strategy implementations.

This mirrors the Java classes:
  - org.apache.paimon.partition.PartitionExpireStrategy
  - org.apache.paimon.partition.PartitionValuesTimeExpireStrategy
  - org.apache.paimon.partition.PartitionUpdateTimeExpireStrategy
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional

from pypaimon.partition.partition_time_extractor import PartitionTimeExtractor

logger = logging.getLogger(__name__)


class PartitionEntry:
    """
    Represents aggregated statistics for a single partition.

    Attributes:
        spec: Partition specification as {column_name: value_string}.
        record_count: Total number of records in the partition.
        file_count: Total number of files in the partition.
        file_size_in_bytes: Total file size in bytes.
        last_file_creation_time: Latest file creation time in epoch milliseconds.
    """

    def __init__(
        self,
        spec: Dict[str, str],
        record_count: int = 0,
        file_count: int = 0,
        file_size_in_bytes: int = 0,
        last_file_creation_time: int = 0,
    ):
        self.spec = spec
        self.record_count = record_count
        self.file_count = file_count
        self.file_size_in_bytes = file_size_in_bytes
        self.last_file_creation_time = last_file_creation_time

    def __repr__(self):
        spec_str = ",".join(f"{k}={v}" for k, v in self.spec.items())
        return f"PartitionEntry({spec_str}, records={self.record_count})"


class PartitionExpireStrategy(ABC):
    """
    Abstract strategy for determining which partitions are expired.
    """

    def __init__(self, partition_keys: List[str], partition_default_name: str = "__DEFAULT_PARTITION__"):
        self.partition_keys = partition_keys
        self.partition_default_name = partition_default_name

    @abstractmethod
    def select_expired_partitions(
        self, partition_entries: List[PartitionEntry], expiration_time: datetime
    ) -> List[PartitionEntry]:
        """
        Select partitions that have expired.

        Args:
            partition_entries: All current partition entries.
            expiration_time: The expiration threshold datetime. Partitions older
                than this should be considered expired.

        Returns:
            List of expired PartitionEntry objects.
        """
        pass

    def to_partition_string(self, entry: PartitionEntry) -> Dict[str, str]:
        """Convert a PartitionEntry to a partition spec dict suitable for drop_partitions."""
        return dict(entry.spec)


class PartitionValuesTimeExpireStrategy(PartitionExpireStrategy):
    """
    Expire partitions by parsing partition field values as timestamps.

    For example, a partition `dt=2024-01-01` would be parsed to extract
    the date 2024-01-01, and compared against the expiration threshold.
    """

    def __init__(
        self,
        partition_keys: List[str],
        partition_default_name: str = "__DEFAULT_PARTITION__",
        timestamp_pattern: Optional[str] = None,
        timestamp_formatter: Optional[str] = None,
    ):
        super().__init__(partition_keys, partition_default_name)
        self._time_extractor = PartitionTimeExtractor(
            pattern=timestamp_pattern, formatter=timestamp_formatter
        )

    def select_expired_partitions(
        self, partition_entries: List[PartitionEntry], expiration_time: datetime
    ) -> List[PartitionEntry]:
        expired = []
        for entry in partition_entries:
            try:
                partition_time = self._time_extractor.extract_from_spec(entry.spec)
                if expiration_time > partition_time:
                    expired.append(entry)
            except (ValueError, TypeError) as e:
                logger.warning(
                    "Cannot extract datetime from partition %s: %s. "
                    "Skipping this partition for expiration. "
                    "Consider using 'update-time' strategy for non-date formatted partitions.",
                    entry.spec, e,
                )
            except (IndexError, KeyError) as e:
                logger.warning(
                    "Partition %s has null or missing values, cannot expire: %s",
                    entry.spec, e,
                )
        return expired


class PartitionUpdateTimeExpireStrategy(PartitionExpireStrategy):
    """
    Expire partitions by comparing their last file creation time
    against the expiration threshold.
    """

    def select_expired_partitions(
        self, partition_entries: List[PartitionEntry], expiration_time: datetime
    ) -> List[PartitionEntry]:
        expiration_millis = int(expiration_time.timestamp() * 1000)
        return [
            entry for entry in partition_entries
            if expiration_millis > entry.last_file_creation_time
        ]
