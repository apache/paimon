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

logger = logging.getLogger(__name__)


class FileStatistics:
    """Statistics for a single file in the table."""

    def __init__(
        self,
        file_name: str,
        file_size: int,
        row_count: int,
        delete_row_count: int = 0,
    ):
        """
        Initialize file statistics.

        Args:
            file_name: Name of the file
            file_size: Size of the file in bytes
            row_count: Number of rows in the file
            delete_row_count: Number of deleted rows
        """
        self.file_name = file_name
        self.file_size = file_size
        self.row_count = row_count
        self.delete_row_count = delete_row_count

    @property
    def delete_ratio(self) -> float:
        """Get the ratio of deleted rows to total rows."""
        if self.row_count == 0:
            return 0.0
        return float(self.delete_row_count) / float(self.row_count)

    def __repr__(self) -> str:
        return (
            f"FileStatistics(file={self.file_name}, "
            f"size={self.file_size}, rows={self.row_count}, "
            f"deletes={self.delete_row_count}, ratio={self.delete_ratio:.2%})"
        )


class PartitionFileStatistics:
    """Statistics for all files in a partition."""

    def __init__(self, partition_key: Tuple):
        """
        Initialize partition statistics.

        Args:
            partition_key: Partition key as a tuple (e.g., ('2024-01-01', '00'))
        """
        self.partition_key = partition_key
        self.files: List[FileStatistics] = []

    @property
    def file_count(self) -> int:
        """Get the number of files in this partition."""
        return len(self.files)

    @property
    def total_size(self) -> int:
        """Get total size of all files in this partition."""
        return sum(f.file_size for f in self.files)

    @property
    def total_row_count(self) -> int:
        """Get total row count in this partition."""
        return sum(f.row_count for f in self.files)

    @property
    def total_delete_count(self) -> int:
        """Get total delete count in this partition."""
        return sum(f.delete_row_count for f in self.files)

    @property
    def delete_ratio(self) -> float:
        """Get the overall delete ratio for this partition."""
        if self.total_row_count == 0:
            return 0.0
        return float(self.total_delete_count) / float(self.total_row_count)

    @property
    def average_file_size(self) -> float:
        """Get average file size in this partition."""
        if self.file_count == 0:
            return 0.0
        return float(self.total_size) / float(self.file_count)

    def add_file(self, file_stat: FileStatistics) -> None:
        """
        Add a file to this partition.

        Args:
            file_stat: FileStatistics object
        """
        self.files.append(file_stat)

    def __repr__(self) -> str:
        return (
            f"PartitionFileStatistics(partition={self.partition_key}, "
            f"files={self.file_count}, size={self.total_size}, "
            f"rows={self.total_row_count}, deletes={self.total_delete_count})"
        )


class TableLevelFileScanner:
    """
    Table-level file scanner for compaction planning.

    Scans all files in a table, organized by partition and bucket,
    collecting statistics to support compaction decision making.
    """

    # Batch size for scanning files to avoid memory issues
    SCAN_BATCH_SIZE = 100_000

    def __init__(self, table: Any, partition_filter: Optional[Any] = None):
        """
        Initialize the file scanner.

        Args:
            table: FileStoreTable instance
            partition_filter: Optional partition predicate for filtering
        """
        self.table = table
        self.partition_filter = partition_filter
        self.snapshot_manager = table.snapshot_manager()
        self.options = table.options

        # Statistics by partition
        self.partition_stats: Dict[Tuple, PartitionFileStatistics] = {}

        # Overall statistics
        self.total_file_count = 0
        self.total_size = 0
        self.total_row_count = 0
        self.total_delete_count = 0

        # Cache for target file size
        self._target_file_size = self._parse_target_file_size()

        logger.debug(
            "TableLevelFileScanner initialized for table: %s, "
            "target file size: %d bytes",
            table.identifier,
            self._target_file_size,
        )

    def _parse_target_file_size(self) -> int:
        """Parse target file size from options."""
        try:
            target_size_str = self.options.get(
                'target-file-size', '256mb'
            )
            return self._parse_size(target_size_str)
        except Exception as e:
            logger.warning(
                "Error parsing target-file-size: %s, using default 256MB",
                str(e),
            )
            return 256 * 1024 * 1024

    @staticmethod
    def _parse_size(size_str: str) -> int:
        """
        Parse size string (e.g., '256mb', '1gb') to bytes.

        Args:
            size_str: Size string

        Returns:
            Size in bytes
        """
        size_str = size_str.lower().strip()
        # Check larger units first to avoid matching 'b' in 'kb'
        multipliers = [
            ('tb', 1024 * 1024 * 1024 * 1024),
            ('gb', 1024 * 1024 * 1024),
            ('mb', 1024 * 1024),
            ('kb', 1024),
            ('b', 1),
        ]

        for suffix, multiplier in multipliers:
            if size_str.endswith(suffix):
                value_str = size_str[:-len(suffix)].strip()
                try:
                    value = float(value_str)
                    return int(value * multiplier)
                except ValueError:
                    raise ValueError(
                        f"Invalid size format: {size_str}"
                    )

        raise ValueError(f"Unknown size unit in: {size_str}")

    def scan(self) -> Dict[Tuple, PartitionFileStatistics]:
        """
        Scan all files in the table.

        Returns:
            Dictionary mapping partition key to PartitionFileStatistics
        """
        logger.info("Starting file scan for table: %s", self.table.identifier)

        try:
            # Get latest snapshot
            latest_snapshot = self.snapshot_manager.latest_snapshot()
            if latest_snapshot is None:
                logger.warning("No snapshot found for table")
                return {}

            # Scan files in batches
            self._scan_files_batched(latest_snapshot)

            logger.info(
                "File scan completed: total files=%d, size=%d bytes, "
                "rows=%d, deletes=%d, partitions=%d",
                self.total_file_count,
                self.total_size,
                self.total_row_count,
                self.total_delete_count,
                len(self.partition_stats),
            )

            return self.partition_stats

        except Exception as e:
            logger.error("Error during file scan: %s", str(e), exc_info=True)
            raise

    def _scan_files_batched(self, snapshot: Any) -> None:
        """
        Scan files in batches to manage memory efficiently.

        Args:
            snapshot: Snapshot to scan
        """
        batch_count = 0
        batch_files = []

        try:
            file_iter = snapshot.file_iterator()
            for file_entry in file_iter:
                batch_files.append(file_entry)

                if len(batch_files) >= self.SCAN_BATCH_SIZE:
                    self._process_file_batch(batch_files)
                    batch_count += 1
                    batch_files = []

            # Process remaining files
            if batch_files:
                self._process_file_batch(batch_files)
                batch_count += 1

            logger.debug(
                "Processed %d batches of files (batch size=%d)",
                batch_count,
                self.SCAN_BATCH_SIZE,
            )

        except Exception as e:
            logger.error(
                "Error scanning files in batches: %s", str(e), exc_info=True
            )
            raise

    def _process_file_batch(self, file_batch: List[Any]) -> None:
        """
        Process a batch of files.

        Args:
            file_batch: List of file entries
        """
        for file_entry in file_batch:
            try:
                partition_key = tuple(file_entry.partition())

                # Apply partition filter if provided
                if not self._should_include_partition(partition_key):
                    continue

                # Get or create partition statistics
                if partition_key not in self.partition_stats:
                    self.partition_stats[partition_key] = (
                        PartitionFileStatistics(partition_key)
                    )

                # Create file statistics
                file_stat = FileStatistics(
                    file_name=file_entry.fileName(),
                    file_size=file_entry.file_size(),
                    row_count=file_entry.row_count(),
                    delete_row_count=file_entry.delete_rows_count(),
                )

                # Add to partition
                self.partition_stats[partition_key].add_file(file_stat)

                # Update overall statistics
                self.total_file_count += 1
                self.total_size += file_stat.file_size
                self.total_row_count += file_stat.row_count
                self.total_delete_count += file_stat.delete_row_count

            except Exception as e:
                logger.warning(
                    "Error processing file entry: %s", str(e)
                )
                continue

    def _should_include_partition(self, partition_key: Tuple) -> bool:
        """
        Check if partition should be included based on filter.

        Args:
            partition_key: Partition key as tuple

        Returns:
            True if partition should be included
        """
        if self.partition_filter is None:
            return True

        # Convert partition tuple to dict for matching
        partition_dict = self._partition_tuple_to_dict(partition_key)
        return self.partition_filter.matches(partition_dict)

    def _partition_tuple_to_dict(self, partition_tuple: Tuple) -> Dict[str, str]:
        """
        Convert partition tuple to dictionary.

        Args:
            partition_tuple: Partition key as tuple

        Returns:
            Partition as dictionary
        """
        partition_keys = self.table.schema().partition_keys()
        if len(partition_tuple) != len(partition_keys):
            logger.warning(
                "Partition tuple size mismatch: expected %d, got %d",
                len(partition_keys),
                len(partition_tuple),
            )
            return {}

        return dict(zip(partition_keys, partition_tuple))

    def get_partition_stats(
        self, partition_key: Tuple
    ) -> Optional[PartitionFileStatistics]:
        """
        Get statistics for a specific partition.

        Args:
            partition_key: Partition key as tuple

        Returns:
            PartitionFileStatistics or None if not found
        """
        return self.partition_stats.get(partition_key)

    def get_small_file_partitions(
        self, target_size: Optional[int] = None
    ) -> List[Tuple]:
        """
        Get partitions with files smaller than target size.

        Args:
            target_size: Target file size threshold. If None, uses table default

        Returns:
            List of partition keys with small files
        """
        if target_size is None:
            target_size = self._target_file_size

        small_file_partitions = []

        for partition_key, stats in self.partition_stats.items():
            # Check if any file is significantly smaller than target
            for file_stat in stats.files:
                if file_stat.file_size < target_size * 0.5:
                    small_file_partitions.append(partition_key)
                    break

        logger.debug(
            "Found %d partitions with small files (< %.2f%% of target)",
            len(small_file_partitions),
            50.0,
        )

        return small_file_partitions

    def get_high_delete_partitions(
        self, delete_ratio_threshold: float = 0.2
    ) -> List[Tuple]:
        """
        Get partitions with high delete ratio.

        Args:
            delete_ratio_threshold: Delete ratio threshold

        Returns:
            List of partition keys with high delete ratio
        """
        high_delete_partitions = []

        for partition_key, stats in self.partition_stats.items():
            if stats.delete_ratio >= delete_ratio_threshold:
                high_delete_partitions.append(partition_key)

        logger.debug(
            "Found %d partitions with high delete ratio (>= %.2f%%)",
            len(high_delete_partitions),
            delete_ratio_threshold * 100,
        )

        return high_delete_partitions

    def summary(self) -> str:
        """
        Get a summary of scan results.

        Returns:
            Summary string
        """
        avg_file_size = (
            self.total_size // self.total_file_count
            if self.total_file_count > 0
            else 0
        )

        return (
            f"TableLevelFileScanner Summary:\n"
            f"  Total files: {self.total_file_count}\n"
            f"  Total size: {self._format_size(self.total_size)}\n"
            f"  Average file size: {self._format_size(avg_file_size)}\n"
            f"  Total rows: {self.total_row_count}\n"
            f"  Total deletes: {self.total_delete_count}\n"
            f"  Delete ratio: {(self.total_delete_count / self.total_row_count * 100) if self.total_row_count else 0:.2f}%\n"
            f"  Partitions: {len(self.partition_stats)}"
        )

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """Format bytes as human-readable string."""
        size_float = float(size_bytes)
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_float < 1024:
                return f"{size_float:.1f}{unit}"
            size_float /= 1024
        return f"{size_float:.1f}TB"
