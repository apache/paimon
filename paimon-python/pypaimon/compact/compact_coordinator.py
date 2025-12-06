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
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Any
from collections import defaultdict

from pypaimon.compact.compact_task import AppendCompactTask

logger = logging.getLogger(__name__)


class CompactCoordinator(ABC):
    """Base class for compaction coordinators."""

    @abstractmethod
    def scan_and_plan(self) -> List[Any]:
        """
        Scan the current table state and plan compaction tasks.

        Returns:
            List of CompactTask objects that can be executed in parallel
        """
        pass

    @abstractmethod
    def should_trigger_compaction(self) -> bool:
        """
        Determine if compaction should be triggered based on current table state.

        Returns:
            True if compaction should be triggered, False otherwise
        """
        pass


class AppendOnlyCompactCoordinator(CompactCoordinator):
    """
    Compaction coordinator for append-only tables.

    This coordinator scans snapshots and generates compaction tasks
    based on file count and size metrics.
    """

    # Batch size for scanning manifest entries
    FILES_BATCH = 100_000

    def __init__(self, table: Any, is_streaming: bool = True):
        """
        Initialize the append-only compaction coordinator.

        Args:
            table: FileStoreTable instance
            is_streaming: Whether the table is in streaming mode
        """
        self.table = table
        self.is_streaming = is_streaming
        self.snapshot_manager = table.snapshot_manager()
        self.options = table.options

        # Get configuration options
        self.target_file_size = self._get_target_file_size()
        self.min_file_num = self._get_min_file_num()
        self.delete_ratio_threshold = self._get_delete_ratio_threshold()

        # State tracking
        self.partition_files: Dict[Tuple, List[Any]] = {}
        self.file_count = 0
        self.total_file_size = 0

    def _get_target_file_size(self) -> int:
        """Get the target file size from options."""
        from pypaimon.common.core_options import CoreOptions
        size_str = self.options.get(
            CoreOptions.TARGET_FILE_SIZE,
            "256mb"  # Default for append-only tables
        )
        from pypaimon.common.memory_size import MemorySize
        return MemorySize.parse(size_str).get_bytes()

    def _get_min_file_num(self) -> int:
        """Get the minimum file number threshold from options."""
        from pypaimon.common.core_options import CoreOptions
        return int(self.options.get(
            CoreOptions.COMPACTION_MIN_FILE_NUM,
            5
        ))

    def _get_delete_ratio_threshold(self) -> float:
        """Get the delete ratio threshold from options."""
        from pypaimon.common.core_options import CoreOptions
        return float(self.options.get(
            CoreOptions.COMPACTION_DELETE_RATIO_THRESHOLD,
            0.2
        ))

    def scan_and_plan(self) -> List[AppendCompactTask]:
        """
        Scan the current table state and plan compaction tasks.

        Scans snapshots in batches to avoid memory issues with large tables.
        Organizes files by partition and generates compaction tasks.

        Returns:
            List of AppendCompactTask objects ready for execution
        """
        logger.info(
            "Starting scan and plan for append-only compaction. "
            "Streaming mode: %s", self.is_streaming
        )

        self.partition_files.clear()
        self.file_count = 0
        self.total_file_size = 0

        try:
            # Get snapshot reader
            snapshot_manager = self.table.snapshot_manager()
            latest_snapshot = snapshot_manager.latest_snapshot()

            if latest_snapshot is None:
                logger.warning("No snapshot found for table")
                return []

            # Scan files in batches
            all_files_by_partition = self._scan_files_batched(latest_snapshot)

            # Generate compaction tasks
            tasks = self._generate_tasks(all_files_by_partition)

            logger.info(
                "Scan and plan completed. Total files: %d, Total size: %d bytes, "
                "Generated tasks: %d",
                self.file_count,
                self.total_file_size,
                len(tasks)
            )

            return tasks

        except Exception as e:
            logger.error("Error during scan and plan: %s", str(e), exc_info=True)
            raise

    def _scan_files_batched(self, snapshot: Any) -> Dict[Tuple, List[Any]]:
        """
        Scan files from snapshot in batches to manage memory.

        Args:
            snapshot: The snapshot to scan

        Returns:
            Dictionary mapping partition tuples to lists of DataFileMeta objects
        """
        partition_files = defaultdict(list)

        try:
            # Try to get file iterator from snapshot
            # This is a simplified version; actual implementation may need adjustment
            # based on the actual snapshot reader API
            if hasattr(snapshot, 'file_iterator'):
                file_iterator = snapshot.file_iterator()
                batch_count = 0

                for file_entry in file_iterator:
                    partition = self._extract_partition(file_entry)
                    partition_files[partition].append(file_entry)

                    self.file_count += 1
                    if hasattr(file_entry, 'file_size'):
                        self.total_file_size += file_entry.file_size()

                    batch_count += 1
                    if batch_count % self.FILES_BATCH == 0:
                        logger.debug("Scanned %d files", batch_count)

            return dict(partition_files)

        except Exception as e:
            logger.error(
                "Error scanning files from snapshot: %s",
                str(e),
                exc_info=True
            )
            raise

    def _extract_partition(self, file_entry: Any) -> Tuple:
        """
        Extract partition key from file entry.

        Args:
            file_entry: File entry from snapshot

        Returns:
            Partition key as tuple
        """
        if hasattr(file_entry, 'partition'):
            partition = file_entry.partition()
            if partition is not None:
                if isinstance(partition, (list, tuple)):
                    return tuple(partition)
                elif isinstance(partition, dict):
                    # Extract partition values in order of partition keys
                    values = []
                    for key in self.table.partition_keys:
                        values.append(partition.get(key))
                    return tuple(values)
                else:
                    return (partition,)
        return ()

    def _generate_tasks(
        self,
        partition_files: Dict[Tuple, List[Any]]
    ) -> List[AppendCompactTask]:
        """
        Generate compaction tasks from partitioned files.

        Args:
            partition_files: Dictionary of files organized by partition

        Returns:
            List of compaction tasks
        """
        tasks = []

        for partition, files in partition_files.items():
            if not files:
                continue

            # Filter files that should be compacted
            files_to_compact = self._filter_compaction_files(files)

            if len(files_to_compact) >= self.min_file_num:
                logger.debug(
                    "Creating compaction task for partition %s with %d files",
                    partition,
                    len(files_to_compact)
                )
                # For append-only tables without buckets, bucket is typically 0
                task = AppendCompactTask(
                    partition=partition,
                    bucket=0,
                    files=files_to_compact
                )
                tasks.append(task)
            else:
                logger.debug(
                    "Skipping partition %s - only %d files (need at least %d)",
                    partition,
                    len(files_to_compact),
                    self.min_file_num
                )

        return tasks

    def _filter_compaction_files(self, files: List[Any]) -> List[Any]:
        """
        Filter files for compaction based on size and other criteria.

        Args:
            files: List of DataFileMeta objects

        Returns:
            Filtered list of files that should be compacted
        """
        result = []

        for file_meta in files:
            # Include files that are smaller than target size
            # or have high delete ratio
            if self._should_compact_file(file_meta):
                result.append(file_meta)

        return result

    def _should_compact_file(self, file_meta: Any) -> bool:
        """
        Determine if a file should be included in compaction.

        Args:
            file_meta: DataFileMeta object

        Returns:
            True if the file should be compacted, False otherwise
        """
        try:
            # Check file size
            if hasattr(file_meta, 'file_size'):
                file_size = file_meta.file_size()
                if file_size < self.target_file_size:
                    return True

            # Check delete ratio for files with deletion vectors
            if hasattr(file_meta, 'delete_rows_count'):
                delete_count = file_meta.delete_rows_count()
                if hasattr(file_meta, 'row_count'):
                    row_count = file_meta.row_count()
                    if row_count > 0:
                        delete_ratio = delete_count / row_count
                        if delete_ratio > self.delete_ratio_threshold:
                            return True

            return False

        except Exception as e:
            logger.warning(
                "Error checking if file should be compacted: %s",
                str(e)
            )
            return False

    def should_trigger_compaction(self) -> bool:
        """
        Determine if compaction should be triggered based on current state.

        Checks:
        1. Minimum file count threshold
        2. Total file size threshold
        3. Average file size compared to target

        Returns:
            True if compaction should be triggered, False otherwise
        """
        try:
            # Check minimum file count
            if self.file_count >= self.min_file_num:
                logger.debug(
                    "Should trigger compaction: file count %d >= threshold %d",
                    self.file_count,
                    self.min_file_num
                )
                return True

            # Check total file size
            avg_file_size = (
                self.total_file_size / self.file_count
                if self.file_count > 0
                else 0
            )
            if avg_file_size < self.target_file_size * 0.5:
                logger.debug(
                    "Should trigger compaction: avg file size %d is much smaller "
                    "than target %d",
                    avg_file_size,
                    self.target_file_size
                )
                return True

            return False

        except Exception as e:
            logger.error(
                "Error determining if compaction should be triggered: %s",
                str(e),
                exc_info=True
            )
            return False

    def close(self) -> None:
        """Cleanup resources."""
        self.partition_files.clear()
        logger.info("AppendOnlyCompactCoordinator closed")
