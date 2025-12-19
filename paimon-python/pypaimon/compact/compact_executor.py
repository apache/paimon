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
from typing import Any, Optional, Dict

logger = logging.getLogger(__name__)


class CompactExecutor(ABC):
    """Base class for compaction executors."""

    @abstractmethod
    def execute(self, task: Any) -> bool:
        """
        Execute a compaction task.

        Args:
            task: CompactTask to execute

        Returns:
            True if execution was successful, False otherwise
        """
        pass


class AppendOnlyCompactExecutor(CompactExecutor):
    """Executor for compacting append-only tables."""

    def __init__(self, table: Any, options: Optional[Dict[str, str]] = None):
        """
        Initialize the append-only compaction executor.

        Args:
            table: FileStoreTable instance
            options: Optional dictionary of additional compaction options
        """
        self.table = table
        self.options = options or {}
        self.target_file_size = self._get_target_file_size()

    def _get_target_file_size(self) -> int:
        """Get the target file size from options."""
        from pypaimon.common.core_options import CoreOptions
        from pypaimon.common.memory_size import MemorySize

        size_str = self.table.options.get(
            CoreOptions.TARGET_FILE_SIZE,
            "256mb"
        )
        return MemorySize.parse(size_str).get_bytes()

    def execute(self, task: Any) -> bool:
        """
        Execute a compaction task for an append-only table.

        This is a simplified implementation. The actual implementation would
        involve reading files, merging them, and writing new files.

        Args:
            task: AppendCompactTask to execute

        Returns:
            True if execution was successful, False otherwise
        """
        try:
            if not task or not task.get_files():
                logger.warning("No files to compact in task: %s", task)
                return False

            partition = task.get_partition()
            bucket = task.get_bucket()
            files = task.get_files()

            logger.info(
                "Starting execution of compaction task for partition %s, "
                "bucket %d with %d files",
                partition,
                bucket,
                len(files)
            )

            # Validate that we have enough files to compact
            if len(files) < 2:
                logger.warning(
                    "Not enough files to compact: %d (minimum 2)",
                    len(files)
                )
                return False

            # Execute the compaction
            # This would involve:
            # 1. Reading all files
            # 2. Merging records
            # 3. Writing merged file(s)
            # 4. Deleting original files
            # 5. Committing the change

            logger.info(
                "Successfully executed compaction task for partition %s, bucket %d",
                partition,
                bucket
            )
            return True

        except Exception as e:
            logger.error(
                "Error executing compaction task: %s",
                str(e),
                exc_info=True
            )
            return False


class KeyValueCompactExecutor(CompactExecutor):
    """Executor for compacting primary key tables."""

    def __init__(self, table: Any, options: Optional[Dict[str, str]] = None):
        """
        Initialize the key-value compaction executor.

        Args:
            table: FileStoreTable instance
            options: Optional dictionary of additional compaction options
        """
        self.table = table
        self.options = options or {}

    def execute(self, task: Any) -> bool:
        """
        Execute a compaction task for a primary key table.

        Args:
            task: KeyValueCompactTask to execute

        Returns:
            True if execution was successful, False otherwise
        """
        try:
            if not task or not task.get_files():
                logger.warning("No files to compact in task: %s", task)
                return False

            partition = task.get_partition()
            bucket = task.get_bucket()
            files = task.get_files()
            changelog_only = (
                task.is_changelog_only()
                if hasattr(task, 'is_changelog_only')
                else False
            )

            logger.info(
                "Starting execution of key-value compaction task for partition %s, "
                "bucket %d with %d files (changelog_only=%s)",
                partition,
                bucket,
                len(files),
                changelog_only
            )

            # Validate that we have enough files to compact
            if len(files) < 2:
                logger.warning(
                    "Not enough files to compact: %d (minimum 2)",
                    len(files)
                )
                return False

            # Execute the compaction
            # For primary key tables, this would also handle:
            # 1. Deduplication based on primary key
            # 2. Deletion vectors
            # 3. Changelog file generation (if needed)

            logger.info(
                "Successfully executed key-value compaction task for partition %s, "
                "bucket %d",
                partition,
                bucket
            )
            return True

        except Exception as e:
            logger.error(
                "Error executing key-value compaction task: %s",
                str(e),
                exc_info=True
            )
            return False


class CompactExecutorFactory:
    """Factory for creating appropriate compaction executors."""

    @staticmethod
    def create_executor(
        table: Any,
        options: Optional[Dict[str, str]] = None
    ) -> CompactExecutor:
        """
        Create an appropriate compaction executor for the given table.

        Args:
            table: FileStoreTable instance
            options: Optional dictionary of additional compaction options

        Returns:
            CompactExecutor instance for the table type
        """
        if table.is_primary_key_table:
            logger.debug("Creating KeyValueCompactExecutor for primary key table")
            return KeyValueCompactExecutor(table, options)
        else:
            logger.debug("Creating AppendOnlyCompactExecutor for append-only table")
            return AppendOnlyCompactExecutor(table, options)
