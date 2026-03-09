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
#  Unless required by applicable law or agreed to in writing,
#  distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
################################################################################

import re
from datetime import datetime
from typing import Dict, List, Optional

from pypaimon.common.file_io import FileIO
from pypaimon.consumer.consumer import Consumer


class ConsumerManager:
    """Manage consumer groups."""

    CONSUMER_PREFIX = "consumer-"
    BRANCH_PREFIX = "branch-"
    DEFAULT_MAIN_BRANCH = "main"

    def __init__(self, file_io: FileIO, table_path: str, branch_name: Optional[str] = None):
        """
        Initialize ConsumerManager.

        Args:
            file_io: FileIO instance
            table_path: Path to the table
            branch_name: Branch name (default: "main")
        """
        self.file_io = file_io
        self.table_path = table_path
        self.branch = branch_name if branch_name else self.DEFAULT_MAIN_BRANCH

    def consumer(self, consumer_id: str) -> Optional[Consumer]:
        """
        Get consumer by ID.

        Args:
            consumer_id: Consumer ID

        Returns:
            Consumer if exists, None otherwise
        """
        return Consumer.from_path(self.file_io, self.consumer_path(consumer_id))

    def reset_consumer(self, consumer_id: str, consumer: Consumer):
        """
        Reset or create consumer.

        Args:
            consumer_id: Consumer ID
            consumer: Consumer instance
        """
        self.file_io.overwrite_file_utf8(self.consumer_path(consumer_id), consumer.to_json())

    def delete_consumer(self, consumer_id: str):
        """
        Delete consumer by ID.

        Args:
            consumer_id: Consumer ID
        """
        self.file_io.delete_quietly(self.consumer_path(consumer_id))

    def min_next_snapshot(self) -> Optional[int]:
        """
        Get minimum next snapshot across all consumers.

        Returns:
            Minimum next snapshot, or None if no consumers
        """
        try:
            consumer_ids = self.list_original_versioned_files(self.consumer_directory(), self.CONSUMER_PREFIX)
            next_snapshots = []
            for consumer_id in consumer_ids:
                consumer = self.consumer(consumer_id)
                if consumer:
                    next_snapshots.append(consumer.next_snapshot)

            return min(next_snapshots) if next_snapshots else None
        except Exception as e:
            raise RuntimeError(f"Failed to get min next snapshot: {e}")

    def expire(self, expire_datetime: datetime):
        """
        Expire consumers modified before given datetime.

        Args:
            expire_datetime: Expire datetime
        """
        try:
            file_statuses = self.list_versioned_file_status(self.consumer_directory(), self.CONSUMER_PREFIX)
            for status in file_statuses:
                mtime_ms = status.get('modification_time', 0)
                # Convert milliseconds to datetime
                modification_time = datetime.fromtimestamp(mtime_ms / 1000.0)
                if expire_datetime > modification_time:
                    self.file_io.delete_quietly(status['path'])
        except Exception as e:
            raise RuntimeError(f"Failed to expire consumers: {e}")

    def clear_consumers(self, including_pattern: str, excluding_pattern: Optional[str] = None):
        """
        Clear consumers matching pattern.

        Args:
            including_pattern: Regex pattern for consumers to include
            excluding_pattern: Optional regex pattern for consumers to exclude
        """
        try:
            file_statuses = self.list_versioned_file_status(self.consumer_directory(), self.CONSUMER_PREFIX)
            including_re = re.compile(including_pattern)
            excluding_re = re.compile(excluding_pattern) if excluding_pattern else None

            for status in file_statuses:
                consumer_name = status['path'].split('/')[-1][len(self.CONSUMER_PREFIX):]
                should_clear = including_re.match(consumer_name) is not None

                if excluding_re:
                    should_clear = should_clear and excluding_re.match(consumer_name) is None

                if should_clear:
                    self.file_io.delete_quietly(status['path'])
        except Exception as e:
            raise RuntimeError(f"Failed to clear consumers: {e}")

    def consumers(self) -> Dict[str, int]:
        """
        Get all consumers.

        Returns:
            Dictionary mapping consumer_id to next_snapshot
        """
        result = {}
        try:
            consumer_ids = self.list_original_versioned_files(self.consumer_directory(), self.CONSUMER_PREFIX)
            for consumer_id in consumer_ids:
                consumer = self.consumer(consumer_id)
                if consumer:
                    result[consumer_id] = consumer.next_snapshot
        except Exception as e:
            raise RuntimeError(f"Failed to get consumers: {e}")
        return result

    def list_all_ids(self) -> List[str]:
        """
        List all consumer IDs.

        Returns:
            List of consumer IDs
        """
        try:
            return self.list_original_versioned_files(self.consumer_directory(), self.CONSUMER_PREFIX)
        except Exception as e:
            raise RuntimeError(f"Failed to list consumer IDs: {e}")

    def consumer_directory(self) -> str:
        """Get consumer directory path."""
        return f"{self._branch_path(self.table_path, self.branch)}/consumer"

    def consumer_path(self, consumer_id: str) -> str:
        """Get consumer file path."""
        return f"{self._branch_path(self.table_path, self.branch)}/consumer/{self.CONSUMER_PREFIX}{consumer_id}"

    @staticmethod
    def _branch_path(table_path: str, branch: str) -> str:
        """
        Return the path string of a branch.

        Args:
            table_path: Table path
            branch: Branch name

        Returns:
            Branch path string
        """
        if branch == ConsumerManager.DEFAULT_MAIN_BRANCH:
            return table_path
        else:
            return f"{table_path}/branch/{ConsumerManager.BRANCH_PREFIX}{branch}"

    def list_original_versioned_files(self, directory: str, prefix: str) -> List[str]:
        """
        List files with given prefix in directory.

        Args:
            directory: Directory path
            prefix: File prefix

        Returns:
            List of file names (without prefix)
        """
        try:
            file_statuses = self.file_io.list_status(directory)
            result = []
            for status in file_statuses:
                file_name = status.path.split('/')[-1]
                if file_name.startswith(prefix):
                    result.append(file_name[len(prefix):])
            return result
        except Exception:
            # Directory may not exist
            return []

    def list_versioned_file_status(self, directory: str, prefix: str) -> List[dict]:
        """
        List file statuses with given prefix in directory.

        Args:
            directory: Directory path
            prefix: File prefix

        Returns:
            List of file status dictionaries with 'path' and 'modification_time'
        """
        try:
            file_statuses = self.file_io.list_status(directory)
            result = []
            for status in file_statuses:
                file_name = status.path.split('/')[-1]
                if file_name.startswith(prefix):
                    # Convert modification time to milliseconds
                    # status.mtime is in seconds (float), convert to milliseconds
                    mtime_ms = int(status.mtime * 1000)
                    result.append({
                        'path': status.path,
                        'modification_time': mtime_ms
                    })
            return result
        except Exception:
            # Directory may not exist
            return []
