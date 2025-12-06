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
from typing import Optional, Dict, List, Any
from datetime import timedelta

logger = logging.getLogger(__name__)


class CompactBuilder:
    """Builder for constructing and executing compaction operations."""

    # Compaction strategy constants
    FULL_COMPACTION = "full"
    MINOR_COMPACTION = "minor"

    def __init__(self, table: Any):
        """
        Initialize the compaction builder.

        Args:
            table: FileStoreTable instance to compact
        """
        self.table = table
        self._strategy = self.MINOR_COMPACTION
        self._partitions: Optional[List[Dict[str, str]]] = None
        self._where_sql: Optional[str] = None
        self._partition_idle_time: Optional[timedelta] = None
        self._options: Dict[str, str] = {}
        self._full_compaction = False

        logger.debug("CompactBuilder initialized for table: %s", table.identifier)

    def with_strategy(self, strategy: str) -> 'CompactBuilder':
        """
        Set the compaction strategy.

        Args:
            strategy: Either 'full' or 'minor' (default: 'minor')

        Returns:
            This builder for method chaining
        """
        if strategy not in (self.FULL_COMPACTION, self.MINOR_COMPACTION):
            raise ValueError(
                f"Invalid strategy: {strategy}. "
                f"Must be '{self.FULL_COMPACTION}' or '{self.MINOR_COMPACTION}'"
            )

        self._strategy = strategy
        self._full_compaction = strategy == self.FULL_COMPACTION
        logger.debug("Set compaction strategy to: %s", strategy)
        return self

    def with_partitions(
        self,
        partitions: List[Dict[str, str]]
    ) -> 'CompactBuilder':
        """
        Specify which partitions to compact.

        Partitions can be specified as complete partition specifications or
        partial specifications (for subset matching).

        Example:
            >>> builder.with_partitions([
            ...     {'dt': '2024-01-01', 'hour': '00'},
            ...     {'dt': '2024-01-01', 'hour': '01'}
            ... ])

        Args:
            partitions: List of partition dictionaries

        Returns:
            This builder for method chaining
        """
        if not partitions:
            raise ValueError("Partitions list cannot be empty")

        # Validate partition keys
        for partition in partitions:
            if not isinstance(partition, dict):
                raise TypeError(f"Each partition must be a dict, got {type(partition)}")
            if not partition:
                raise ValueError("Each partition dict cannot be empty")

        self._partitions = partitions
        self._where_sql = None  # Clear WHERE condition if partitions are set
        logger.debug("Set partitions for compaction: %s", partitions)
        return self

    def with_where(self, where_sql: str) -> 'CompactBuilder':
        """
        Specify partitions using a WHERE condition.

        The condition should only reference partition columns.

        Example:
            >>> builder.with_where("dt > '2024-01-01' and hour < '12'")

        Args:
            where_sql: WHERE clause for filtering partitions

        Returns:
            This builder for method chaining
        """
        if not where_sql or not isinstance(where_sql, str):
            raise ValueError("WHERE clause must be a non-empty string")

        self._where_sql = where_sql
        self._partitions = None  # Clear partitions if WHERE is set
        logger.debug("Set WHERE condition for compaction: %s", where_sql)
        return self

    def with_partition_idle_time(
        self,
        idle_time: Any  # Can be str like '1d' or timedelta
    ) -> 'CompactBuilder':
        """
        Only compact partitions that have been idle for a specified duration.

        This is useful for compacting historical partitions in batch mode.

        Example:
            >>> builder.with_partition_idle_time('1d')
            >>> # or
            >>> builder.with_partition_idle_time(timedelta(days=1))

        Args:
            idle_time: Idle duration as string (e.g., '1d', '2h') or timedelta

        Returns:
            This builder for method chaining
        """
        if isinstance(idle_time, str):
            # Parse string format like '1d', '2h', '30m'
            self._partition_idle_time = self._parse_duration(idle_time)
        elif isinstance(idle_time, timedelta):
            self._partition_idle_time = idle_time
        else:
            raise ValueError(
                f"partition_idle_time must be str or timedelta, got {type(idle_time)}"
            )

        logger.debug("Set partition idle time: %s", self._partition_idle_time)
        return self

    def with_options(self, options: Dict[str, str]) -> 'CompactBuilder':
        """
        Set additional options for compaction.

        Common options:
        - 'sink.parallelism': Number of parallel compaction tasks
        - 'compaction.min.file-num': Minimum files to trigger compaction

        Args:
            options: Dictionary of option key-value pairs

        Returns:
            This builder for method chaining
        """
        if not options:
            return self

        if not isinstance(options, dict):
            raise TypeError(f"Options must be a dict, got {type(options)}")

        self._options.update(options)
        logger.debug("Added options for compaction: %s", options)
        return self

    def build(self) -> None:
        """
        Execute the compaction operation.

        This method performs the actual compaction based on the configuration
        set through the builder methods.

        Raises:
            ValueError: If configuration is invalid
            RuntimeError: If compaction execution fails
        """
        try:
            logger.info(
                "Building and executing compaction for table: %s "
                "(strategy=%s, full=%s)",
                self.table.identifier,
                self._strategy,
                self._full_compaction
            )

            # Validate configuration
            self._validate_configuration()

            # Create a table copy with compaction options
            table_to_compact = self._prepare_table()

            # Create coordinator based on table type
            coordinator = self._create_coordinator(table_to_compact)

            # Scan and generate compaction tasks
            tasks = coordinator.scan_and_plan()

            if not tasks:
                logger.info("No compaction tasks generated")
                return

            logger.info("Generated %d compaction tasks", len(tasks))

            # Create executor and execute tasks
            executor = self._create_executor(table_to_compact)
            successful_tasks = 0

            for task in tasks:
                try:
                    if executor.execute(task):
                        successful_tasks += 1
                    else:
                        logger.warning(
                            "Failed to execute task: %s",
                            task
                        )
                except Exception as e:
                    logger.error(
                        "Error executing task: %s - %s",
                        task,
                        str(e),
                        exc_info=True
                    )

            logger.info(
                "Compaction completed: %d/%d tasks succeeded",
                successful_tasks,
                len(tasks)
            )

            # Cleanup
            coordinator.close()

        except Exception as e:
            logger.error(
                "Error during compaction: %s",
                str(e),
                exc_info=True
            )
            raise RuntimeError(f"Compaction failed: {str(e)}") from e

    def _validate_configuration(self) -> None:
        """Validate the builder configuration."""
        # Check that only one partition specification method is used
        partition_methods = sum([
            self._partitions is not None,
            self._where_sql is not None,
            self._partition_idle_time is not None
        ])

        if partition_methods > 1:
            raise ValueError(
                "Cannot specify multiple partition filtering methods. "
                "Use either partitions, where clause, or idle time, not multiple."
            )

    def _prepare_table(self) -> Any:
        """
        Prepare the table for compaction.

        Creates a copy of the table with compaction options applied.

        Returns:
            FileStoreTable instance configured for compaction
        """
        # Set write-only=false to enable compaction
        from pypaimon.common.core_options import CoreOptions

        compact_options = {str(CoreOptions.WRITE_ONLY): "false"}
        compact_options.update(self._options)

        return self.table.copy(compact_options)

    def _create_coordinator(self, table: Any) -> Any:
        """
        Create an appropriate compaction coordinator.

        Args:
            table: FileStoreTable instance

        Returns:
            CompactCoordinator instance
        """
        from pypaimon.compact.compact_coordinator import (
            AppendOnlyCompactCoordinator
        )
        from pypaimon.table.bucket_mode import BucketMode

        bucket_mode = table.bucket_mode()

        # For now, only support append-only tables
        if table.is_primary_key_table:
            logger.warning(
                "Compaction for primary key tables not yet fully implemented"
            )

        if bucket_mode == BucketMode.BUCKET_UNAWARE:
            logger.debug("Creating AppendOnlyCompactCoordinator")
            return AppendOnlyCompactCoordinator(table, is_streaming=False)
        else:
            raise NotImplementedError(
                f"Compaction not yet implemented for bucket mode: {bucket_mode}"
            )

    def _create_executor(self, table: Any) -> Any:
        """
        Create an appropriate compaction executor.

        Args:
            table: FileStoreTable instance

        Returns:
            CompactExecutor instance
        """
        from pypaimon.compact.compact_executor import CompactExecutorFactory

        return CompactExecutorFactory.create_executor(table, self._options)

    @staticmethod
    def _parse_duration(duration_str: str) -> timedelta:
        """
        Parse duration string to timedelta.

        Supported formats: '1d', '2h', '30m', '60s'

        Args:
            duration_str: Duration string

        Returns:
            timedelta object
        """
        if not duration_str:
            raise ValueError("Duration string cannot be empty")

        duration_str = duration_str.strip().lower()

        # Parse the numeric part and unit
        import re
        match = re.match(r'^(\d+)([dhms])$', duration_str)

        if not match:
            raise ValueError(
                f"Invalid duration format: {duration_str}. "
                f"Use format like '1d', '2h', '30m', '60s'"
            )

        value, unit = int(match.group(1)), match.group(2)

        if unit == 'd':
            return timedelta(days=value)
        elif unit == 'h':
            return timedelta(hours=value)
        elif unit == 'm':
            return timedelta(minutes=value)
        elif unit == 's':
            return timedelta(seconds=value)
        else:
            raise ValueError(f"Unknown time unit: {unit}")

    def __repr__(self) -> str:
        return (
            f"CompactBuilder(strategy={self._strategy}, "
            f"full={self._full_compaction}, "
            f"partitions={self._partitions}, "
            f"where={self._where_sql}, "
            f"idle_time={self._partition_idle_time})"
        )
