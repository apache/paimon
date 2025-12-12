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

from abc import ABC, abstractmethod
from typing import Optional
from dataclasses import dataclass


@dataclass
class CompactResult:
    """Result of a compaction operation."""

    success: bool
    files_compacted: int = 0
    new_files_count: int = 0
    compaction_time_ms: int = 0
    error_message: Optional[str] = None

    def is_successful(self) -> bool:
        """Check if the compaction was successful."""
        return self.success

    def get_error_message(self) -> Optional[str]:
        """Get the error message if the compaction failed."""
        return self.error_message


class CompactManager(ABC):
    """Manager interface for compaction operations."""

    @abstractmethod
    def should_trigger_compaction(self) -> bool:
        """
        Determine if a compaction should be triggered based on current state.

        Returns:
            True if compaction should be triggered, False otherwise
        """
        pass

    @abstractmethod
    def trigger_compaction(self, full_compaction: bool = False) -> bool:
        """
        Trigger a compaction task.

        Args:
            full_compaction: If True, perform a full compaction of all files.
                           If False, perform a minor compaction based on strategy.

        Returns:
            True if the compaction task was successfully triggered, False otherwise
        """
        pass

    @abstractmethod
    def get_compaction_result(self, blocking: bool = False) -> Optional[CompactResult]:
        """
        Get the result of a completed compaction task.

        Args:
            blocking: If True, wait for the compaction to complete.
                     If False, return immediately with None if no result is ready.

        Returns:
            CompactResult if a result is available, None otherwise
        """
        pass

    @abstractmethod
    def cancel_compaction(self) -> None:
        """Cancel any currently running compaction task."""
        pass

    @abstractmethod
    def compact_not_completed(self) -> bool:
        """
        Check if a compaction task is in progress or if a result remains to be fetched.

        Returns:
            True if a compaction is in progress or has pending results, False otherwise
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Close and cleanup resources."""
        pass
