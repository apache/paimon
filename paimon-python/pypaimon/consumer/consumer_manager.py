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
"""ConsumerManager for persisting streaming read progress."""

import re
from datetime import datetime
from typing import Dict, List, Optional

from pypaimon.consumer.consumer import Consumer

DEFAULT_MAIN_BRANCH = "main"
BRANCH_PREFIX = "branch-"


def _branch_path(table_path: str, branch: str) -> str:
    """Return the path string of a branch."""
    if _is_main_branch(branch):
        return table_path
    return f"{table_path}/branch/{BRANCH_PREFIX}{branch}"


def _is_main_branch(branch: str) -> bool:
    """Check if branch is main branch."""
    return branch == DEFAULT_MAIN_BRANCH


def _normalize_branch(branch: Optional[str]) -> str:
    """Normalize branch name, use DEFAULT_MAIN_BRANCH if None or empty."""
    if not branch or not branch.strip():
        return DEFAULT_MAIN_BRANCH
    return branch


class ConsumerManager:
    """Manages consumer state stored at {table_path}/consumer/consumer-{id}."""

    CONSUMER_PREFIX = "consumer-"

    def __init__(self, file_io, table_path: str,
                 branch: Optional[str] = None):
        self._file_io = file_io
        self._table_path = table_path
        self._branch = _normalize_branch(branch)

    @staticmethod
    def _validate_consumer_id(consumer_id: str) -> None:
        """Validate consumer_id to prevent path traversal."""
        if not consumer_id:
            raise ValueError("consumer_id cannot be empty")
        if '/' in consumer_id or '\\' in consumer_id:
            raise ValueError(
                f"consumer_id cannot contain path separators: {consumer_id}"
            )
        if consumer_id in ('.', '..'):
            raise ValueError(
                f"consumer_id cannot be a relative path component: {consumer_id}"
            )

    def _consumer_directory(self) -> str:
        """Return the path to consumer directory."""
        branch_table_path = _branch_path(self._table_path, self._branch)
        return f"{branch_table_path}/consumer"

    def _consumer_path(self, consumer_id: str) -> str:
        """Return the path to a consumer file."""
        self._validate_consumer_id(consumer_id)
        consumer_dir = self._consumer_directory()
        return f"{consumer_dir}/{self.CONSUMER_PREFIX}{consumer_id}"

    def _list_consumer_ids(self) -> List[str]:
        """List all consumer IDs."""
        consumer_dir = self._consumer_directory()
        if not self._file_io.exists(consumer_dir):
            return []

        consumer_ids = []
        for status in self._file_io.list_status(consumer_dir):
            if status.base_name and status.base_name.startswith(
                    self.CONSUMER_PREFIX):
                consumer_id = status.base_name[len(self.CONSUMER_PREFIX):]
                consumer_ids.append(consumer_id)
        return consumer_ids

    def consumer(self, consumer_id: str) -> Optional[Consumer]:
        """Get consumer state, or None if not found."""
        path = self._consumer_path(consumer_id)
        return Consumer.from_path(self._file_io, path)

    def reset_consumer(self, consumer_id: str, consumer: Consumer) -> None:
        """Write or update consumer state."""
        path = self._consumer_path(consumer_id)
        self._file_io.overwrite_file_utf8(path, consumer.to_json())

    def delete_consumer(self, consumer_id: str) -> None:
        """Delete a consumer."""
        path = self._consumer_path(consumer_id)
        self._file_io.delete_quietly(path)

    def min_next_snapshot(self) -> Optional[int]:
        """Get minimum next snapshot among all consumers."""
        consumer_ids = self._list_consumer_ids()
        if not consumer_ids:
            return None

        min_snapshot = None
        for consumer_id in consumer_ids:
            consumer = self.consumer(consumer_id)
            if consumer:
                if min_snapshot is None or consumer.next_snapshot < min_snapshot:
                    min_snapshot = consumer.next_snapshot
        return min_snapshot

    def expire(self, expire_datetime: datetime) -> None:
        """Delete consumers with modification time before expire_datetime."""
        consumer_dir = self._consumer_directory()
        if not self._file_io.exists(consumer_dir):
            return

        expire_timestamp = expire_datetime.timestamp()
        for status in self._file_io.list_status(consumer_dir):
            if status.base_name and status.base_name.startswith(
                    self.CONSUMER_PREFIX):
                consumer_path = f"{consumer_dir}/{status.base_name}"
                try:
                    if status.mtime and status.mtime < expire_timestamp:
                        self._file_io.delete_quietly(consumer_path)
                except Exception:
                    pass

    def list_all_ids(self) -> List[str]:
        """List all consumer IDs."""
        return self._list_consumer_ids()

    def consumers(self) -> Dict[str, int]:
        """Get all consumers as a dict mapping consumer_id to next_snapshot."""
        consumer_ids = self._list_consumer_ids()
        consumers = {}
        for consumer_id in consumer_ids:
            consumer = self.consumer(consumer_id)
            if consumer:
                consumers[consumer_id] = consumer.next_snapshot
        return consumers

    def clear_consumers(
            self, including_pattern: str, excluding_pattern: Optional[str] = None
    ) -> None:
        """
        Clear consumers matching inclusion pattern but not exclusion pattern.

        Args:
            including_pattern: Regex pattern for consumers to delete
            excluding_pattern: Optional regex pattern for consumers to keep
        """
        consumer_ids = self._list_consumer_ids()
        including_re = re.compile(including_pattern)
        excluding_re = (
            re.compile(excluding_pattern) if excluding_pattern else None
        )

        for consumer_id in consumer_ids:
            should_delete = including_re.match(consumer_id)
            if excluding_re:
                should_delete = should_delete and not excluding_re.match(
                    consumer_id
                )

            if should_delete:
                self.delete_consumer(consumer_id)
