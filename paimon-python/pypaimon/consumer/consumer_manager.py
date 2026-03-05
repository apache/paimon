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
"""
ConsumerManager for managing consumer progress.

A ConsumerManager reads and writes consumer state files in the table's
consumer directory. This enables tracking streaming read progress across
restarts and informs snapshot expiration which snapshots are still needed.
"""

import os
from typing import Optional

from pypaimon.consumer.consumer import Consumer


class ConsumerManager:
    """
    Manages consumer state for streaming reads.

    Consumer state is persisted to {table_path}/consumer/consumer-{consumer_id}.
    This is the Python equivalent of Java's ConsumerManager class.
    """

    CONSUMER_PREFIX = "consumer-"

    def __init__(self, file_io, table_path: str):
        """
        Create a ConsumerManager.

        Args:
            file_io: FileIO instance for reading/writing files
            table_path: Root path of the table
        """
        self._file_io = file_io
        self._table_path = table_path

    @staticmethod
    def _validate_consumer_id(consumer_id: str) -> None:
        """
        Validate consumer ID to prevent path traversal attacks.

        Args:
            consumer_id: The consumer identifier to validate

        Raises:
            ValueError: If consumer_id contains path separators or is empty
        """
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

    def _consumer_path(self, consumer_id: str) -> str:
        """
        Get the path to a consumer file.

        Args:
            consumer_id: The consumer identifier

        Returns:
            Path to the consumer file: {table_path}/consumer/consumer-{id}

        Raises:
            ValueError: If consumer_id is invalid
        """
        self._validate_consumer_id(consumer_id)
        return os.path.join(
            self._table_path,
            "consumer",
            f"{self.CONSUMER_PREFIX}{consumer_id}"
        )

    def consumer(self, consumer_id: str) -> Optional[Consumer]:
        """
        Get the consumer state for the given consumer ID.

        Args:
            consumer_id: The consumer identifier

        Returns:
            Consumer instance if exists, None otherwise
        """
        path = self._consumer_path(consumer_id)
        if not self._file_io.exists(path):
            return None

        json_str = self._file_io.read_file_utf8(path)
        return Consumer.from_json(json_str)

    def reset_consumer(self, consumer_id: str, consumer: Consumer) -> None:
        """
        Write or update consumer state.

        Args:
            consumer_id: The consumer identifier
            consumer: The consumer state to persist
        """
        path = self._consumer_path(consumer_id)
        self._file_io.overwrite_file_utf8(path, consumer.to_json())

    def delete_consumer(self, consumer_id: str) -> None:
        """
        Delete a consumer.

        Args:
            consumer_id: The consumer identifier to delete
        """
        path = self._consumer_path(consumer_id)
        self._file_io.delete_quietly(path)
