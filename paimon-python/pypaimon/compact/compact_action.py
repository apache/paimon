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
from typing import List, Tuple, Any, Optional, Dict

from pypaimon.compact.compact_builder import CompactBuilder
from pypaimon.compact.compact_executor import CompactExecutorFactory
from pypaimon.compact.table_level_file_scanner import TableLevelFileScanner
from pypaimon.compact.table_level_task_generator import (
    TableLevelTaskGenerator,
)
from pypaimon.compact.compact_strategy import CompactionStrategyFactory
from pypaimon.compact.partition_predicate import PartitionPredicateConverter

logger = logging.getLogger(__name__)


class CompactionActionResult:
    """Result of a compaction action execution."""

    def __init__(
        self,
        success: bool,
        tasks_generated: int = 0,
        files_compacted: int = 0,
        partitions_processed: int = 0,
        error_message: Optional[str] = None,
    ):
        """
        Initialize the result.

        Args:
            success: Whether the action was successful
            tasks_generated: Number of tasks generated
            files_compacted: Number of files compacted
            partitions_processed: Number of partitions processed
            error_message: Error message if failed
        """
        self.success = success
        self.tasks_generated = tasks_generated
        self.files_compacted = files_compacted
        self.partitions_processed = partitions_processed
        self.error_message = error_message

    def __repr__(self) -> str:
        return (
            f"CompactionActionResult(success={self.success}, "
            f"tasks={self.tasks_generated}, "
            f"files={self.files_compacted}, "
            f"partitions={self.partitions_processed})"
        )


class CompactAction:
    """
    Extended CompactAction for table-level compaction operations.

    Combines CompactBuilder API with table-level file scanning and
    task generation for comprehensive compaction control.
    """

    def __init__(self, table: Any):
        """
        Initialize the CompactAction.

        Args:
            table: FileStoreTable instance
        """
        self.table = table
        self.options = table.options
        self.identifier = table.identifier

        logger.debug("CompactAction initialized for table: %s", self.identifier)

    def execute_full_table_compaction(
        self, strategy: str = 'full'
    ) -> CompactionActionResult:
        """
        Execute compaction on the entire table.

        Args:
            strategy: Compaction strategy ('full' or 'minor')

        Returns:
            CompactionActionResult
        """
        logger.info(
            "Starting full table compaction for: %s, strategy: %s",
            self.identifier,
            strategy,
        )

        try:
            # Step 1: Scan files
            scanner = TableLevelFileScanner(self.table)
            scanner.scan()

            logger.info(
                "File scan completed:\n%s", scanner.summary()
            )

            # Step 2: Generate tasks
            compaction_strategy = CompactionStrategyFactory.create_strategy(
                strategy
            )
            generator = TableLevelTaskGenerator(
                self.table, compaction_strategy
            )
            result = generator.generate_tasks(scanner)

            logger.info(
                "Task generation completed:\n%s", generator.summary(result)
            )

            # Step 3: Execute tasks
            execution_result = self._execute_tasks(result.tasks)

            logger.info(
                "Full table compaction completed: %s", execution_result
            )

            return CompactionActionResult(
                success=True,
                tasks_generated=len(result.tasks),
                files_compacted=result.total_files_selected,
                partitions_processed=result.partitions_selected,
            )

        except Exception as e:
            error_msg = f"Full table compaction failed: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return CompactionActionResult(
                success=False, error_message=error_msg
            )

    def execute_partition_compaction(
        self,
        partitions: List[Dict[str, str]],
        strategy: str = 'minor',
    ) -> CompactionActionResult:
        """
        Execute compaction on specific partitions.

        Args:
            partitions: List of partition dictionaries
            strategy: Compaction strategy ('full' or 'minor')

        Returns:
            CompactionActionResult
        """
        logger.info(
            "Starting partition compaction for: %s, partitions: %d, strategy: %s",
            self.identifier,
            len(partitions),
            strategy,
        )

        try:
            # Validate partitions
            partition_keys = self._validate_partitions(partitions)

            # Step 1: Scan files
            scanner = TableLevelFileScanner(self.table)
            scanner.scan()

            # Step 2: Generate tasks for specific partitions
            compaction_strategy = CompactionStrategyFactory.create_strategy(
                strategy
            )
            generator = TableLevelTaskGenerator(
                self.table, compaction_strategy
            )
            result = generator.generate_selective_tasks(
                scanner, partition_keys
            )

            logger.info(
                "Partition task generation completed:\n%s",
                generator.summary(result),
            )

            # Step 3: Execute tasks
            execution_result = self._execute_tasks(result.tasks)

            logger.info(
                "Partition compaction completed: %s", execution_result
            )

            return CompactionActionResult(
                success=True,
                tasks_generated=len(result.tasks),
                files_compacted=result.total_files_selected,
                partitions_processed=result.partitions_selected,
            )

        except Exception as e:
            error_msg = f"Partition compaction failed: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return CompactionActionResult(
                success=False, error_message=error_msg
            )

    def execute_where_compaction(
        self,
        where_clause: str,
        strategy: str = 'full',
    ) -> CompactionActionResult:
        """
        Execute compaction on partitions matching WHERE condition.

        Args:
            where_clause: SQL WHERE condition for partition filtering
            strategy: Compaction strategy ('full' or 'minor')

        Returns:
            CompactionActionResult
        """
        logger.info(
            "Starting WHERE compaction for: %s, condition: %s, strategy: %s",
            self.identifier,
            where_clause,
            strategy,
        )

        try:
            # Parse WHERE clause to predicate
            partition_columns = self.table.schema().partition_keys()
            predicate = PartitionPredicateConverter.from_sql_where(
                where_clause, partition_columns
            )

            if predicate is None:
                error_msg = f"Invalid WHERE clause: {where_clause}"
                logger.error(error_msg)
                return CompactionActionResult(
                    success=False, error_message=error_msg
                )

            # Step 1: Scan files with partition filter
            scanner = TableLevelFileScanner(
                self.table, partition_filter=predicate
            )
            scanner.scan()

            # Step 2: Generate tasks
            compaction_strategy = CompactionStrategyFactory.create_strategy(
                strategy
            )
            generator = TableLevelTaskGenerator(
                self.table, compaction_strategy
            )
            result = generator.generate_tasks(scanner)

            logger.info(
                "WHERE clause task generation completed:\n%s",
                generator.summary(result),
            )

            # Step 3: Execute tasks
            execution_result = self._execute_tasks(result.tasks)

            logger.info(
                "WHERE compaction completed: %s", execution_result
            )

            return CompactionActionResult(
                success=True,
                tasks_generated=len(result.tasks),
                files_compacted=result.total_files_selected,
                partitions_processed=result.partitions_selected,
            )

        except Exception as e:
            error_msg = f"WHERE compaction failed: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return CompactionActionResult(
                success=False, error_message=error_msg
            )

    def execute_idle_partition_compaction(
        self,
        idle_time: str = '7d',
        strategy: str = 'full',
    ) -> CompactionActionResult:
        """
        Execute compaction on idle partitions.

        Args:
            idle_time: Idle time duration (e.g., '1d', '7d')
            strategy: Compaction strategy ('full' or 'minor')

        Returns:
            CompactionActionResult
        """
        logger.info(
            "Starting idle partition compaction for: %s, idle_time: %s, strategy: %s",
            self.identifier,
            idle_time,
            strategy,
        )

        try:
            # Parse idle time
            idle_timedelta = CompactBuilder._parse_duration(idle_time)

            # Step 1: Scan files
            scanner = TableLevelFileScanner(self.table)
            scanner.scan()

            # Step 2: Generate tasks for idle partitions
            compaction_strategy = CompactionStrategyFactory.create_strategy(
                strategy
            )
            generator = TableLevelTaskGenerator(
                self.table, compaction_strategy
            )
            result = generator.generate_idle_partition_tasks(
                scanner, idle_timedelta
            )

            logger.info(
                "Idle partition task generation completed:\n%s",
                generator.summary(result),
            )

            # Step 3: Execute tasks
            execution_result = self._execute_tasks(result.tasks)

            logger.info(
                "Idle partition compaction completed: %s",
                execution_result,
            )

            return CompactionActionResult(
                success=True,
                tasks_generated=len(result.tasks),
                files_compacted=result.total_files_selected,
                partitions_processed=result.partitions_selected,
            )

        except Exception as e:
            error_msg = f"Idle partition compaction failed: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return CompactionActionResult(
                success=False, error_message=error_msg
            )

    def _validate_partitions(
        self, partitions: List[Dict[str, str]]
    ) -> List[Tuple]:
        """
        Validate and convert partitions to tuples.

        Args:
            partitions: List of partition dicts

        Returns:
            List of partition key tuples

        Raises:
            ValueError: If partitions are invalid
        """
        if not partitions:
            raise ValueError("Partitions list cannot be empty")

        partition_keys = self.table.schema().partition_keys()
        partition_tuples = []

        for partition in partitions:
            if not isinstance(partition, dict):
                raise TypeError(
                    f"Each partition must be a dict, got {type(partition)}"
                )

            # Extract partition values in order
            values = []
            for key in partition_keys:
                if key not in partition:
                    raise ValueError(
                        f"Missing partition key: {key} in {partition}"
                    )
                values.append(str(partition[key]))

            partition_tuples.append(tuple(values))

        return partition_tuples

    def _execute_tasks(self, tasks: List[Any]) -> str:
        """
        Execute compaction tasks.

        Args:
            tasks: List of CompactTask objects

        Returns:
            Execution summary
        """
        logger.info("Executing %d compaction tasks", len(tasks))

        executed = 0
        failed = 0

        executor = CompactExecutorFactory.create_executor(self.table)

        for task in tasks:
            try:
                success = executor.execute(task)
                if success:
                    executed += 1
                else:
                    failed += 1
                    logger.warning("Task execution returned False: %s", task)
            except Exception as e:
                failed += 1
                logger.warning(
                    "Error executing task %s: %s", task, str(e)
                )

        summary = (
            f"Task Execution: {executed} executed, {failed} failed "
            f"(total: {len(tasks)})"
        )
        logger.info(summary)

        return summary

    def get_compaction_stats(self) -> Dict[str, Any]:
        """
        Get current compaction statistics for the table.

        Returns:
            Dictionary with table statistics
        """
        try:
            scanner = TableLevelFileScanner(self.table)
            scanner.scan()

            stats = {
                'table': self.identifier,
                'total_files': scanner.total_file_count,
                'total_size_bytes': scanner.total_size,
                'total_rows': scanner.total_row_count,
                'total_deletes': scanner.total_delete_count,
                'partitions': len(scanner.partition_stats),
                'average_file_size_bytes': (
                    scanner.total_size // scanner.total_file_count
                    if scanner.total_file_count > 0
                    else 0
                ),
                'delete_ratio': (
                    scanner.total_delete_count / scanner.total_row_count
                    if scanner.total_row_count > 0
                    else 0.0
                ),
            }

            return stats

        except Exception as e:
            logger.error(
                "Error getting compaction stats: %s", str(e), exc_info=True
            )
            raise
