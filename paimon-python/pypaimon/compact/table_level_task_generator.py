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

import logging
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime, timedelta

from pypaimon.compact.compact_task import CompactTask, AppendCompactTask
from pypaimon.compact.compact_strategy import CompactStrategy
from pypaimon.compact.table_level_file_scanner import (
    TableLevelFileScanner,
    PartitionFileStatistics,
)

logger = logging.getLogger(__name__)


class CompactionTaskGenerationResult:
    """Result of task generation."""

    def __init__(
        self,
        tasks: List[CompactTask],
        total_files_selected: int,
        partitions_selected: int,
    ):
        """
        Initialize the result.

        Args:
            tasks: Generated compaction tasks
            total_files_selected: Total files selected for compaction
            partitions_selected: Number of partitions selected
        """
        self.tasks = tasks
        self.total_files_selected = total_files_selected
        self.partitions_selected = partitions_selected

    def __repr__(self) -> str:
        return (
            f"CompactionTaskGenerationResult(tasks={len(self.tasks)}, "
            f"files={self.total_files_selected}, "
            f"partitions={self.partitions_selected})"
        )


class TableLevelTaskGenerator:
    """
    Generates compaction tasks at the table level.

    Takes file statistics from TableLevelFileScanner and generates
    CompactTask objects based on compaction strategy and configuration.
    """

    def __init__(
        self,
        table: Any,
        strategy: CompactStrategy,
        min_file_num: int = 5,
        delete_ratio_threshold: float = 0.2,
    ):
        """
        Initialize the task generator.

        Args:
            table: FileStoreTable instance
            strategy: Compaction strategy (Full or Minor)
            min_file_num: Minimum files to trigger compaction
            delete_ratio_threshold: Delete ratio threshold for triggering
        """
        self.table = table
        self.strategy = strategy
        self.min_file_num = min_file_num
        self.delete_ratio_threshold = delete_ratio_threshold
        self.options = table.options

        logger.debug(
            "TableLevelTaskGenerator initialized: min_files=%d, "
            "delete_threshold=%.2f%%",
            min_file_num,
            delete_ratio_threshold * 100,
        )

    def generate_tasks(
        self,
        file_scanner: TableLevelFileScanner,
        target_file_size: Optional[int] = None,
    ) -> CompactionTaskGenerationResult:
        """
        Generate compaction tasks from file statistics.

        Args:
            file_scanner: TableLevelFileScanner with scan results
            target_file_size: Target file size for filtering

        Returns:
            CompactionTaskGenerationResult with generated tasks
        """
        if target_file_size is None:
            target_file_size = self._get_target_file_size()

        logger.info(
            "Starting task generation for table: %s, "
            "partitions: %d",
            self.table.identifier,
            len(file_scanner.partition_stats),
        )

        tasks = []
        total_files_selected = 0
        partitions_selected = 0

        try:
            # Generate tasks for each partition
            for partition_key, partition_stats in (
                file_scanner.partition_stats.items()
            ):
                partition_tasks = self._generate_partition_tasks(
                    partition_key,
                    partition_stats,
                    target_file_size,
                )

                if partition_tasks:
                    tasks.extend(partition_tasks)
                    partitions_selected += 1
                    total_files_selected += sum(
                        len(task.get_files()) for task in partition_tasks
                    )

            logger.info(
                "Task generation completed: tasks=%d, files=%d, partitions=%d",
                len(tasks),
                total_files_selected,
                partitions_selected,
            )

            return CompactionTaskGenerationResult(
                tasks=tasks,
                total_files_selected=total_files_selected,
                partitions_selected=partitions_selected,
            )

        except Exception as e:
            logger.error("Error generating tasks: %s", str(e), exc_info=True)
            raise

    def _generate_partition_tasks(
        self,
        partition_key: Tuple,
        partition_stats: PartitionFileStatistics,
        target_file_size: int,
    ) -> List[CompactTask]:
        """
        Generate tasks for a specific partition.

        Args:
            partition_key: Partition key as tuple
            partition_stats: Statistics for the partition
            target_file_size: Target file size threshold

        Returns:
            List of CompactTask objects for this partition
        """
        # Check if partition meets compaction criteria
        if not self._should_compact_partition(
            partition_stats, target_file_size
        ):
            return []

        logger.debug(
            "Generating tasks for partition %s: files=%d, size=%d, deletes=%.2f%%",
            partition_key,
            partition_stats.file_count,
            partition_stats.total_size,
            partition_stats.delete_ratio * 100,
        )

        # Select files using strategy
        files_to_compact = self.strategy.select_files(partition_stats.files)

        if not files_to_compact:
            logger.debug(
                "No files selected by strategy for partition %s",
                partition_key,
            )
            return []

        if len(files_to_compact) < self.min_file_num:
            logger.debug(
                "Too few files for partition %s: %d < %d",
                partition_key,
                len(files_to_compact),
                self.min_file_num,
            )
            return []

        # Create compaction task
        # For append-only tables without buckets, bucket is typically 0
        task = AppendCompactTask(
            partition=partition_key,
            bucket=0,
            files=files_to_compact,
        )

        logger.debug(
            "Created compaction task for partition %s with %d files",
            partition_key,
            len(files_to_compact),
        )

        return [task]

    def _should_compact_partition(
        self,
        partition_stats: PartitionFileStatistics,
        target_file_size: int,
    ) -> bool:
        """
        Determine if partition should be compacted.

        Args:
            partition_stats: Partition statistics
            target_file_size: Target file size

        Returns:
            True if partition should be compacted
        """
        # Condition 1: Enough files
        if partition_stats.file_count >= self.min_file_num:
            return True

        # Condition 2: Average file size significantly smaller than target
        if (
            partition_stats.average_file_size > 0
            and partition_stats.average_file_size
            < target_file_size * 0.5
        ):
            return True

        # Condition 3: High delete ratio
        if partition_stats.delete_ratio >= self.delete_ratio_threshold:
            return True

        return False

    def _get_target_file_size(self) -> int:
        """Get target file size from options."""
        try:
            target_size_str = self.options.get(
                'target-file-size', '256mb'
            )
            return TableLevelFileScanner._parse_size(target_size_str)
        except Exception as e:
            logger.warning(
                "Error parsing target-file-size: %s, using default 256MB",
                str(e),
            )
            return 256 * 1024 * 1024

    def generate_selective_tasks(
        self,
        file_scanner: TableLevelFileScanner,
        partition_keys: Optional[List[Tuple]] = None,
        target_file_size: Optional[int] = None,
    ) -> CompactionTaskGenerationResult:
        """
        Generate tasks for specific partitions.

        Args:
            file_scanner: TableLevelFileScanner with scan results
            partition_keys: List of partition keys to compact. If None, use all
            target_file_size: Target file size threshold

        Returns:
            CompactionTaskGenerationResult
        """
        if target_file_size is None:
            target_file_size = self._get_target_file_size()

        if partition_keys is None:
            # Use all partitions
            partition_keys = list(file_scanner.partition_stats.keys())

        logger.info(
            "Generating selective tasks for %d partitions",
            len(partition_keys),
        )

        tasks = []
        total_files_selected = 0
        partitions_selected = 0

        for partition_key in partition_keys:
            partition_stats = file_scanner.get_partition_stats(
                partition_key
            )
            if partition_stats is None:
                logger.debug(
                    "Partition %s not found in scanner results",
                    partition_key,
                )
                continue

            partition_tasks = self._generate_partition_tasks(
                partition_key,
                partition_stats,
                target_file_size,
            )

            if partition_tasks:
                tasks.extend(partition_tasks)
                partitions_selected += 1
                total_files_selected += sum(
                    len(task.get_files()) for task in partition_tasks
                )

        return CompactionTaskGenerationResult(
            tasks=tasks,
            total_files_selected=total_files_selected,
            partitions_selected=partitions_selected,
        )

    def generate_idle_partition_tasks(
        self,
        file_scanner: TableLevelFileScanner,
        idle_time: timedelta,
        target_file_size: Optional[int] = None,
    ) -> CompactionTaskGenerationResult:
        """
        Generate tasks for idle partitions.

        Args:
            file_scanner: TableLevelFileScanner with scan results
            idle_time: Idle time threshold
            target_file_size: Target file size threshold

        Returns:
            CompactionTaskGenerationResult
        """
        if target_file_size is None:
            target_file_size = self._get_target_file_size()

        # Calculate idle cutoff time
        cutoff_time = datetime.now() - idle_time

        logger.info(
            "Generating tasks for idle partitions (> %s ago)",
            idle_time,
        )

        # Filter partitions by idle time
        idle_partitions = self._get_idle_partitions(
            file_scanner, cutoff_time
        )

        logger.debug(
            "Found %d idle partitions out of %d",
            len(idle_partitions),
            len(file_scanner.partition_stats),
        )

        # Generate tasks for idle partitions
        return self.generate_selective_tasks(
            file_scanner=file_scanner,
            partition_keys=idle_partitions,
            target_file_size=target_file_size,
        )

    def _get_idle_partitions(
        self, file_scanner: TableLevelFileScanner, cutoff_time: datetime
    ) -> List[Tuple]:
        """
        Get partitions that are idle (no recent updates).

        Args:
            file_scanner: File scanner with statistics
            cutoff_time: Cutoff time for idle detection

        Returns:
            List of idle partition keys
        """
        idle_partitions = []

        # In a real implementation, we would check actual file creation times
        # For now, we include all partitions in sorted order
        # This allows for deterministic selection of older partitions

        partition_keys = sorted(
            file_scanner.partition_stats.keys(),
            reverse=True,  # Most recent first
        )

        # Assume first half are recent, second half are idle
        # This is a simplified approach
        if len(partition_keys) > 2:
            idle_partitions = partition_keys[len(partition_keys) // 2:]

        return idle_partitions

    def summary(self, result: CompactionTaskGenerationResult) -> str:
        """
        Get a summary of task generation.

        Args:
            result: CompactionTaskGenerationResult

        Returns:
            Summary string
        """
        return (
            f"Task Generation Summary:\n"
            f"  Total tasks: {len(result.tasks)}\n"
            f"  Files selected: {result.total_files_selected}\n"
            f"  Partitions selected: {result.partitions_selected}\n"
            f"  Strategy: {self.__class__.__name__}"
        )
